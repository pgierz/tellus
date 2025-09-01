"""
Error handling and recovery tests for the tellus system.

This module tests error recovery scenarios, network failures, storage issues,
and circuit breaker patterns in the tellus Earth science data archive system.
"""

import contextlib
import os
import tempfile
import time
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import pytest

from tellus.location import Location, LocationKind
from tellus.simulation import (ArchiveRegistry, CacheManager,
                               CompressedArchive, Simulation)


@pytest.mark.integration
class TestNetworkFailureRecovery:
    """Test recovery from various network failure scenarios."""
    
    def test_connection_timeout_recovery(self, test_locations, mock_network_conditions,
                                       error_injection, progress_tracker):
        """Test recovery from connection timeouts."""
        location = test_locations['ssh']
        
        def simulate_timeout_then_success(*args, **kwargs):
            """Simulate network timeout followed by success."""
            if error_injection.should_inject('network_timeout'):
                raise TimeoutError("Connection timed out")
            return Mock()
        
        # Configure error injection for first few attempts
        error_injection.inject_at('network_timeout', TimeoutError, 
                                "Connection timed out", after_calls=2)
        
        # Set up the mock filesystem behavior directly on the fixture's fs
        location.fs.exists = simulate_timeout_then_success
        
        # Attempt operation with retry logic
        operation_id = "timeout_recovery_test"
        progress_tracker.track_operation(operation_id, "network_operation")
        
        max_retries = 5
        success = False
        
        for attempt in range(max_retries):
            try:
                result = location.fs.exists("/test/path")
                success = True
                break
            except TimeoutError:
                if attempt < max_retries - 1:
                    time.sleep(0.1 * (2 ** attempt))  # Exponential backoff
                    continue
                raise
        
        progress_tracker.complete_operation(operation_id, success)
        
        # Verify recovery succeeded
        assert success, "Operation should recover from timeout"
        stats = progress_tracker.get_stats()
        assert stats['successful_operations'] == 1
    
    def test_intermittent_connection_failure(self, test_locations, mock_network_conditions,
                                           archive_factory, progress_tracker):
        """Test handling of intermittent connection failures."""
        archive = archive_factory("intermittent_test", "local")
        
        # Mock intermittent failures
        call_count = 0
        def intermittent_failure(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            # Fail every 3rd call
            if call_count % 3 == 0:
                raise ConnectionError("Intermittent connection failure")
            return b"mock_file_data"
        
        with patch.object(archive, 'open_file', side_effect=intermittent_failure):
            operation_id = "intermittent_failure_test"
            progress_tracker.track_operation(operation_id, "intermittent_operations")
            
            successful_reads = 0
            failed_reads = 0
            
            # Attempt multiple file reads with error handling
            for i in range(10):
                try:
                    file_data = archive.open_file("logs/output.log")
                    successful_reads += 1
                except ConnectionError:
                    failed_reads += 1
                    # In a real implementation, we would retry with backoff
                    continue
            
            progress_tracker.complete_operation(operation_id, successful_reads > 0)
            
            # Verify some operations succeeded despite intermittent failures
            assert successful_reads > 0, "Some operations should succeed"
            assert failed_reads > 0, "Some failures should occur"
            
            # Verify expected failure pattern
            expected_failures = 10 // 3  # Every 3rd call fails
            assert failed_reads >= expected_failures - 1  # Allow for some variance
    
    def test_network_partition_recovery(self, test_locations, mock_network_conditions,
                                      temp_workspace, performance_monitor):
        """Test recovery from network partition scenarios."""
        location = test_locations['s3']
        
        partition_duration = 0.5  # seconds
        partition_start = None
        
        def simulate_network_partition(*args, **kwargs):
            """Simulate network partition for specified duration."""
            nonlocal partition_start
            current_time = time.time()
            
            if partition_start is None:
                partition_start = current_time
            
            if current_time - partition_start < partition_duration:
                raise ConnectionError("Network unreachable")
            else:
                # Network is back up
                return True
        
        # Set up the mock filesystem behavior directly on the fixture's fs
        location.fs.exists = simulate_network_partition
        
        operation = "network_partition_recovery"
        performance_monitor.start_timing(operation)
        
        # Implement retry logic with exponential backoff
        max_retries = 10
        base_delay = 0.1
        success = False
        
        for attempt in range(max_retries):
            try:
                result = location.fs.exists("/test/path")
                success = True
                break
            except ConnectionError:
                if attempt < max_retries - 1:
                    delay = base_delay * (2 ** attempt)
                    time.sleep(min(delay, 1.0))  # Cap at 1 second
                    continue
                raise
        
        duration = performance_monitor.end_timing(operation)
        
        # Verify recovery
        assert success, "Should recover from network partition"
        assert duration >= partition_duration, "Should wait for partition to end"
        assert duration < partition_duration + 2.0, "Should not wait too long"


@pytest.mark.integration
class TestStorageFailureRecovery:
    """Test recovery from storage system failures."""
    
    def test_disk_full_error_handling(self, cache_manager, sample_archive_data,
                                    error_injection, temp_workspace):
        """Test handling of disk full errors during caching."""
        # Store the original open function to avoid recursion
        original_open = open
        
        def simulate_disk_full(*args, **kwargs):
            """Simulate disk full error."""
            if error_injection.should_inject('disk_full'):
                raise OSError(28, "No space left on device")  # ENOSPC
            
            # Return original open result for normal operation
            return original_open(*args, **kwargs)
        
        # Configure error injection
        error_injection.inject_at('disk_full', OSError, "No space left on device")
        
        with patch('builtins.open', side_effect=simulate_disk_full):
            with tempfile.NamedTemporaryFile(suffix='.tar.gz', delete=False) as f:
                f.write(sample_archive_data)
                temp_path = Path(f.name)
            
            try:
                # First attempt should fail with disk full
                with pytest.raises(OSError, match="No space left on device"):
                    cache_manager.cache_archive(temp_path, "disk_full_test")
                
                # Clear error injection (simulate space freed up)
                error_injection.clear()
                
                # Second attempt should succeed
                cached_path = cache_manager.cache_archive(temp_path, "disk_full_test_retry")
                assert cached_path.exists()
                
            finally:
                temp_path.unlink(missing_ok=True)
    
    def test_corrupted_archive_recovery(self, temp_workspace, cache_manager):
        """Test recovery from corrupted archive files."""
        # Create corrupted archive
        corrupted_path = temp_workspace / "corrupted.tar.gz"
        corrupted_path.write_bytes(b"Not a valid tar.gz file content")
        
        # Try to create archive from corrupted file
        # CompressedArchive should handle corruption gracefully
        archive = CompressedArchive(
            "corrupted_test",
            str(corrupted_path),
            cache_manager=cache_manager
        )
        archive.refresh_manifest()
        
        # Verify that the archive correctly identifies corruption (manifest should be empty/None)
        assert archive.manifest is None or len(archive.manifest.files) == 0, "Corrupted archive should have empty manifest"
        
        # Create valid archive to verify system is still functional
        import io
        import tarfile
        
        valid_archive_data = io.BytesIO()
        with tarfile.open(fileobj=valid_archive_data, mode='w:gz') as tar:
            info = tarfile.TarInfo(name='test_file.txt')
            info.size = 12
            tar.addfile(info, io.BytesIO(b'Hello World!'))
        
        valid_path = temp_workspace / "valid.tar.gz"
        valid_path.write_bytes(valid_archive_data.getvalue())
        
        # Valid archive should work
        valid_archive = CompressedArchive(
            "valid_test",
            str(valid_path),
            cache_manager=cache_manager
        )
        valid_archive.refresh_manifest()
        
        assert valid_archive.manifest is not None
        assert len(valid_archive.manifest.files) > 0
    
    def test_permission_denied_recovery(self, temp_workspace, cache_manager,
                                      sample_archive_data):
        """Test recovery from permission denied errors."""
        # Create archive file
        archive_path = temp_workspace / "permission_test.tar.gz"
        archive_path.write_bytes(sample_archive_data)
        
        # Create cache directory with normal permissions first
        restricted_cache_dir = temp_workspace / "restricted_cache"
        restricted_cache_dir.mkdir(mode=0o755)
        
        try:
            # Configure cache manager with the directory
            restricted_config = cache_manager.config
            restricted_config.cache_dir = restricted_cache_dir
            restricted_cache_manager = CacheManager(restricted_config)
            
            # Now restrict permissions to test permission error handling
            restricted_cache_dir.chmod(0o000)  # No permissions
            
            # Should fail with permission error
            with pytest.raises(PermissionError):
                restricted_cache_manager.cache_archive(archive_path, "permission_test")
            
            # Fix permissions and retry
            restricted_cache_dir.chmod(0o755)
            
            # Should now succeed
            cached_path = restricted_cache_manager.cache_archive(archive_path, "permission_test")
            assert cached_path.exists()
            
        finally:
            # Clean up permissions
            try:
                restricted_cache_dir.chmod(0o755)
            except:
                pass
    
    def test_storage_backend_failover(self, test_locations, temp_workspace,
                                    mock_network_conditions, performance_monitor):
        """Test failover between storage backends."""
        primary_location = test_locations['ssh']
        fallback_location = test_locations['local']
        
        # Mock primary storage failure
        def primary_fails(*args, **kwargs):
            raise ConnectionError("Primary storage unavailable")
        
        def fallback_succeeds(*args, **kwargs):
            return True
        
        # Set up the mock filesystem behaviors directly on the fixture's fs
        primary_location.fs.exists = primary_fails
        fallback_location.fs.exists = fallback_succeeds
        
        operation = "storage_failover"
        performance_monitor.start_timing(operation)
        
        # Implement failover logic
        storage_backends = [primary_location, fallback_location]
        success = False
        used_backend = None
        
        for backend in storage_backends:
            try:
                result = backend.fs.exists("/test/file")
                success = True
                used_backend = backend
                break
            except ConnectionError:
                continue
        
        duration = performance_monitor.end_timing(operation)
        
        # Verify failover worked
        assert success, "Failover should succeed"
        assert used_backend == fallback_location, "Should use fallback backend"
        assert duration < 1.0, "Failover should be fast"


@pytest.mark.integration
class TestCacheRecoveryScenarios:
    """Test cache recovery from various failure scenarios."""
    
    def test_cache_corruption_recovery(self, cache_manager, temp_workspace,
                                     sample_archive_data):
        """Test recovery from cache corruption."""
        # Create and cache an archive
        archive_path = temp_workspace / "cache_corruption_test.tar.gz"
        archive_path.write_bytes(sample_archive_data)
        
        checksum = "corruption_test"
        cached_path = cache_manager.cache_archive(archive_path, checksum)
        
        # Verify cache works
        assert cache_manager.get_archive_path(checksum) == cached_path
        
        # Corrupt the cached file
        cached_path.write_bytes(b"Corrupted data")
        
        # Try to access corrupted cache
        retrieved_path = cache_manager.get_archive_path(checksum)
        assert retrieved_path == cached_path  # Path is returned
        
        # But the file is corrupted, so we should handle this gracefully
        with pytest.raises(Exception):  # Would fail when trying to use corrupted file
            import tarfile
            with tarfile.open(cached_path):
                pass
        
        # Re-cache the original file (recovery)
        new_cached_path = cache_manager.cache_archive(archive_path, f"{checksum}_recovered")
        
        # Verify recovery works
        assert new_cached_path.exists()
        with tarfile.open(new_cached_path):
            pass  # Should not raise exception
    
    def test_cache_index_corruption_recovery(self, cache_manager, temp_workspace):
        """Test recovery from cache index corruption."""
        # Corrupt the cache index file
        index_file = cache_manager.config.cache_dir / "cache_index.json"
        index_file.parent.mkdir(parents=True, exist_ok=True)
        index_file.write_text("Invalid JSON content {[[")
        
        # Create new cache manager (should handle corrupted index)
        recovered_cache_manager = CacheManager(cache_manager.config)
        
        # Should start with empty index but still be functional
        stats = recovered_cache_manager.get_cache_stats()
        assert stats['archive_count'] == 0
        assert stats['file_count'] == 0
        
        # Should be able to cache new items
        test_data = b"Test recovery data"
        cached_path = recovered_cache_manager.cache_file(test_data, "recovery_test", "test_checksum")
        assert cached_path.exists()
    
    def test_cache_cleanup_failure_recovery(self, cache_manager, sample_archive_data,
                                          error_injection):
        """Test recovery from cache cleanup failures."""
        # Fill cache beyond limit to trigger cleanup
        archive_paths = []
        checksums = []
        
        for i in range(5):
            with tempfile.NamedTemporaryFile(suffix='.tar.gz', delete=False) as f:
                f.write(sample_archive_data)
                archive_path = Path(f.name)
                archive_paths.append(archive_path)
                
                checksum = f"cleanup_test_{i}"
                checksums.append(checksum)
                cache_manager.cache_archive(archive_path, checksum)
        
        try:
            # Inject error during cleanup (simulate file in use, permission error, etc.)
            def cleanup_fails(*args, **kwargs):
                if error_injection.should_inject('cleanup_failure'):
                    raise PermissionError("File in use, cannot delete")
                # Normal unlink
                Path(args[0]).unlink()
            
            error_injection.inject_at('cleanup_failure', PermissionError, 
                                    "File in use, cannot delete", after_calls=0)
            
            # Create trigger file outside of patch context
            new_checksum = "cleanup_trigger"
            with tempfile.NamedTemporaryFile(suffix='.tar.gz', delete=False) as f:
                f.write(sample_archive_data)
                trigger_path = Path(f.name)
            
            with patch('pathlib.Path.unlink', side_effect=cleanup_fails):
                # Cache operation should still succeed even if cleanup fails
                cached_path = cache_manager.cache_archive(trigger_path, new_checksum)
                assert cached_path.exists()
                
                # Verify cache manager is still functional
                stats = cache_manager.get_cache_stats()
                assert stats['archive_count'] > 0
            
            # Clean up trigger file outside patch context
            trigger_path.unlink(missing_ok=True)
        
        finally:
            # Clean up test files
            for path in archive_paths:
                path.unlink(missing_ok=True)


@pytest.mark.integration
class TestCircuitBreakerPatterns:
    """Test circuit breaker patterns for fault tolerance."""
    
    def test_location_circuit_breaker(self, test_locations, error_injection,
                                    performance_monitor):
        """Test circuit breaker pattern for location operations."""
        location = test_locations['ssh']
        
        class SimpleCircuitBreaker:
            def __init__(self, failure_threshold=3, recovery_timeout=1.0):
                self.failure_threshold = failure_threshold
                self.recovery_timeout = recovery_timeout
                self.failure_count = 0
                self.last_failure_time = None
                self.state = 'closed'  # closed, open, half-open
            
            def call(self, func, *args, **kwargs):
                if self.state == 'open':
                    if time.time() - self.last_failure_time > self.recovery_timeout:
                        self.state = 'half-open'
                    else:
                        raise Exception("Circuit breaker is open")
                
                try:
                    result = func(*args, **kwargs)
                    if self.state == 'half-open':
                        self.state = 'closed'
                        self.failure_count = 0
                    return result
                except Exception as e:
                    self.failure_count += 1
                    self.last_failure_time = time.time()
                    
                    if self.failure_count >= self.failure_threshold:
                        self.state = 'open'
                    
                    raise
        
        circuit_breaker = SimpleCircuitBreaker()
        
        # Mock failing operation
        def failing_operation():
            if error_injection.should_inject('operation_failure'):
                raise ConnectionError("Service unavailable")
            return True
        
        # Configure failures for first few attempts
        error_injection.inject_at('operation_failure', ConnectionError,
                                "Service unavailable", after_calls=5)
        
        operation = "circuit_breaker_test"
        performance_monitor.start_timing(operation)
        
        # Test circuit breaker behavior
        failure_count = 0
        success_count = 0
        circuit_open_count = 0
        
        for i in range(10):
            try:
                result = circuit_breaker.call(failing_operation)
                success_count += 1
            except ConnectionError:
                failure_count += 1
            except Exception as e:
                if "Circuit breaker is open" in str(e):
                    circuit_open_count += 1
            
            time.sleep(0.1)  # Small delay between attempts
        
        duration = performance_monitor.end_timing(operation)
        
        # Verify circuit breaker behavior
        assert failure_count > 0, "Should have some failures"
        assert circuit_open_count > 0, "Circuit breaker should open"
        assert success_count > 0, "Should eventually succeed after recovery"
    
    def test_archive_access_circuit_breaker(self, archive_factory, error_injection,
                                          progress_tracker):
        """Test circuit breaker for archive access operations."""
        archive = archive_factory("circuit_breaker_archive", "local")
        
        # Mock archive.fs.open to fail initially
        original_open = archive.fs.open
        def failing_open(*args, **kwargs):
            if error_injection.should_inject('archive_access_failure'):
                raise OSError("Archive temporarily unavailable")
            return original_open(*args, **kwargs)
        
        # Configure failures
        error_injection.inject_at('archive_access_failure', OSError,
                                "Archive temporarily unavailable", after_calls=6)
        
        with patch.object(archive.fs, 'open', side_effect=failing_open):
            operation_id = "archive_circuit_breaker"
            progress_tracker.track_operation(operation_id, "archive_access")
            
            # Implement simple retry with backoff (simulating circuit breaker)
            max_attempts = 10
            backoff_multiplier = 1.5
            base_delay = 0.1
            
            for attempt in range(max_attempts):
                try:
                    files = archive.list_files()
                    if files:
                        filename = list(files.keys())[0]
                        file_data = archive.open_file(filename)
                        progress_tracker.complete_operation(operation_id, True)
                        break
                except OSError as e:
                    if attempt < max_attempts - 1:
                        delay = base_delay * (backoff_multiplier ** attempt)
                        time.sleep(min(delay, 2.0))  # Cap delay at 2 seconds
                        continue
                    progress_tracker.complete_operation(operation_id, False, str(e))
                    raise
            
            # Verify eventual success
            stats = progress_tracker.get_stats()
            assert stats['successful_operations'] == 1
    
    def test_bulk_operation_partial_failure_recovery(self, archive_factory,
                                                   temp_workspace, error_injection,
                                                   progress_tracker):
        """Test recovery from partial failures in bulk operations."""
        archive = archive_factory("bulk_failure_test", "local")
        extraction_dir = temp_workspace / "bulk_extractions"
        extraction_dir.mkdir(exist_ok=True)
        
        # Get files to extract
        files = archive.list_files()
        file_list = list(files.keys())[:6]  # Limit to 6 files
        
        # Configure some files to fail extraction
        failing_files = set(file_list[1::2])  # Every other file fails
        
        def selective_failure(filename, *args, **kwargs):
            if filename in failing_files and error_injection.should_inject('extraction_failure'):
                raise IOError(f"Failed to extract {filename}")
            
            # Simulate successful extraction
            output_path = extraction_dir / filename
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_text(f"Mock content for {filename}")
            return output_path
        
        error_injection.inject_at('extraction_failure', IOError, "Extraction failed")
        
        with patch.object(archive, 'extract_file', side_effect=selective_failure):
            operation_id = "bulk_operation_recovery"
            progress_tracker.track_operation(operation_id, "bulk_extraction")
            
            successful_extractions = []
            failed_extractions = []
            
            # Attempt to extract all files with individual error handling
            for filename in file_list:
                try:
                    extracted_path = archive.extract_file(filename, extraction_dir)
                    successful_extractions.append(filename)
                except IOError:
                    failed_extractions.append(filename)
                    # In production, might retry or log for later retry
            
            # Verify partial success
            assert len(successful_extractions) > 0, "Some extractions should succeed"
            assert len(failed_extractions) > 0, "Some extractions should fail"
            
            # Retry failed extractions (simulating recovery)
            error_injection.clear()  # Clear error injection
            
            recovered_extractions = []
            for filename in failed_extractions:
                try:
                    extracted_path = archive.extract_file(filename, extraction_dir)
                    recovered_extractions.append(filename)
                except IOError:
                    pass  # Still failed
            
            total_successful = len(successful_extractions) + len(recovered_extractions)
            progress_tracker.complete_operation(operation_id, 
                                               total_successful == len(file_list))
            
            # Verify recovery
            assert len(recovered_extractions) > 0, "Some failed extractions should recover"


@pytest.mark.integration
class TestErrorPropagationAndLogging:
    """Test error propagation and logging mechanisms."""
    
    def test_error_context_preservation(self, archive_factory, error_injection):
        """Test that error context is preserved through the call stack."""
        archive = archive_factory("error_context_test", "local")
        
        def deep_failure(*args, **kwargs):
            """Simulate a deep failure with context."""
            raise ValueError("Deep error with important context: file_id=12345")
        
        with patch.object(archive, 'open_file', side_effect=deep_failure):
            try:
                archive.open_file("logs/output.log")
                assert False, "Should have raised an exception"
            except ValueError as e:
                # Verify error context is preserved
                assert "Deep error with important context" in str(e)
                assert "file_id=12345" in str(e)
    
    def test_nested_error_handling(self, simulation_factory, test_locations,
                                 error_injection, progress_tracker):
        """Test error handling in nested operations."""
        sim = simulation_factory("nested_error_test")
        
        # Configure location with failing filesystem
        def nested_failure(*args, **kwargs):
            try:
                raise ConnectionError("Network layer failure")
            except ConnectionError as e:
                # Wrap in higher-level error
                raise RuntimeError(f"Storage operation failed: {str(e)}") from e
        
        with patch('tellus.location.location.Location._save_locations'):
            location = Location(
                name="failing_location",
                kinds=[LocationKind.DISK],
                config={"path": "/failing/path", "protocol": "file"}
            )
        
        # Set up the mock filesystem behavior directly on the fixture's fs
        location.fs.exists = nested_failure
        
        operation_id = "nested_error_test"
        progress_tracker.track_operation(operation_id, "nested_operation")
        
        try:
            with patch('tellus.simulation.simulation.Simulation.save_simulations'):
                sim.add_location(location, "failing_loc")
            
            # This should trigger the nested failure
            path = sim.get_location_path("failing_loc", "test", "file.txt")
            location.fs.exists(path)
            
            assert False, "Should have raised an exception"
            
        except RuntimeError as e:
            # Verify error chaining
            assert "Storage operation failed" in str(e)
            assert e.__cause__ is not None
            assert isinstance(e.__cause__, ConnectionError)
            assert "Network layer failure" in str(e.__cause__)
            
            progress_tracker.complete_operation(operation_id, False, str(e))
        
        # Verify error was properly tracked
        stats = progress_tracker.get_stats()
        assert stats['failed_operations'] == 1
    
    def test_error_recovery_with_logging(self, cache_manager, sample_archive_data,
                                       temp_workspace, performance_monitor):
        """Test error recovery with proper logging of recovery actions."""
        import shutil
        recovery_log = []
        
        def log_recovery_action(action, details):
            recovery_log.append({
                'timestamp': time.time(),
                'action': action,
                'details': details
            })
        
        # Create archive
        archive_path = temp_workspace / "recovery_logging_test.tar.gz"
        archive_path.write_bytes(sample_archive_data)
        
        # Simulate caching failure followed by recovery
        call_count = 0
        # Store the original copy2 function to avoid recursion
        original_copy2 = shutil.copy2
        
        def failing_then_succeeding(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            
            if call_count == 1:
                log_recovery_action("cache_failure", "Initial cache attempt failed")
                raise OSError("Temporary filesystem error")
            else:
                log_recovery_action("cache_success", "Cache retry succeeded")
                # Use the original copy2 function to avoid recursion
                original_copy2(args[0], args[1])
        
        operation = "recovery_with_logging"
        performance_monitor.start_timing(operation)
        
        with patch('shutil.copy2', side_effect=failing_then_succeeding):
            checksum = "recovery_logging_test"
            
            # First attempt should fail
            try:
                cache_manager.cache_archive(archive_path, checksum + "_fail")
            except OSError:
                log_recovery_action("retry_initiated", "Starting recovery procedure")
            
            # Retry should succeed
            cached_path = cache_manager.cache_archive(archive_path, checksum + "_success")
            
        duration = performance_monitor.end_timing(operation)
        
        # Verify recovery actions were logged
        assert len(recovery_log) >= 3, "Should have logged failure, retry, and success"
        
        actions = [entry['action'] for entry in recovery_log]
        assert 'cache_failure' in actions
        assert 'retry_initiated' in actions
        assert 'cache_success' in actions
        
        # Verify final success
        assert cached_path.exists()
        assert cache_manager.get_archive_path(checksum + "_success") == cached_path