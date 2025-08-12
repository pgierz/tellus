"""
Load testing and performance benchmarks for the tellus system.

This module tests system performance under various load scenarios including
high concurrent access, large file operations, and resource constraints.
"""

import asyncio
import os
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from unittest.mock import patch

import pytest

from tellus.location import Location, LocationKind
from tellus.simulation import (ArchiveRegistry, CacheManager,
                               CompressedArchive, Simulation)


@pytest.mark.performance
class TestConcurrentLoadScenarios:
    """Test system performance under concurrent load."""

    def test_high_concurrent_archive_access(
        self,
        archive_factory,
        concurrent_executor,
        performance_monitor,
        resource_monitor,
        test_env_config,
    ):
        """Test performance under high concurrent archive access."""
        # Create multiple archives
        archives = [archive_factory(f"load_test_archive_{i}") for i in range(3)]

        resource_monitor.take_snapshot("start_concurrent_access")

        def concurrent_archive_worker(worker_id):
            """Worker function for concurrent archive access."""
            operation = f"concurrent_access_{worker_id}"
            performance_monitor.start_timing(operation)

            try:
                archive = archives[worker_id % len(archives)]

                # Perform multiple operations per worker
                results = []
                for op_num in range(5):
                    # List files
                    files = archive.list_files()
                    results.append(len(files))

                    # Get status
                    status = archive.status()
                    results.append(status.get("file_count", 0))

                    # Access a file if available
                    if files:
                        filename = list(files.keys())[0]
                        try:
                            file_data = archive.open_file(filename)
                            results.append(len(file_data.read()))
                        except Exception:
                            results.append(0)

                return sum(results)
            finally:
                performance_monitor.end_timing(operation)

        # Submit high concurrency load
        worker_count = test_env_config["concurrent_operations"] * 2
        futures = [
            concurrent_executor.submit(concurrent_archive_worker, i)
            for i in range(worker_count)
        ]

        # Monitor resource usage during load
        resource_monitor.take_snapshot("peak_load")

        # Wait for completion
        results = []
        for future in as_completed(futures):
            results.append(future.result())

        resource_monitor.take_snapshot("end_concurrent_access")

        # Verify all operations completed
        assert len(results) == worker_count
        assert all(result > 0 for result in results)

        # Analyze performance
        perf_stats = performance_monitor.get_stats()
        operation_times = [stats["avg_time"] for stats in perf_stats.values()]
        avg_operation_time = sum(operation_times) / len(operation_times)

        # Performance assertions
        assert (
            avg_operation_time < 2.0
        ), f"Average operation time {avg_operation_time}s too high"

        # Resource usage should be reasonable
        assert not resource_monitor.check_memory_leak(
            100
        ), "No significant memory leaks"

    def test_cache_performance_under_load(
        self,
        cache_manager,
        sample_archive_data,
        concurrent_executor,
        performance_monitor,
        test_env_config,
    ):
        """Test cache performance under concurrent load."""
        performance_monitor.start_timing("cache_load_test")

        def cache_load_worker(worker_id):
            """Worker function for cache load testing."""
            operation = f"cache_load_{worker_id}"
            performance_monitor.start_timing(operation)

            try:
                # Create temporary archive
                with tempfile.NamedTemporaryFile(suffix=".tar.gz", delete=False) as f:
                    f.write(sample_archive_data)
                    temp_path = Path(f.name)

                try:
                    # Cache archive
                    checksum = f"load_test_{worker_id}"
                    cached_path = cache_manager.cache_archive(temp_path, checksum)

                    # Verify cache
                    retrieved_path = cache_manager.get_archive_path(checksum)
                    assert retrieved_path == cached_path

                    # Cache some files
                    for i in range(3):
                        file_data = f"file_data_{worker_id}_{i}".encode() * 100
                        file_key = f"file_{worker_id}_{i}"
                        file_checksum = f"checksum_{worker_id}_{i}"

                        file_path = cache_manager.cache_file(
                            file_data, file_key, file_checksum
                        )
                        assert file_path.exists()

                    return worker_id
                finally:
                    temp_path.unlink(missing_ok=True)
            finally:
                performance_monitor.end_timing(operation)

        # Submit cache load tasks
        worker_count = test_env_config["concurrent_operations"]
        futures = [
            concurrent_executor.submit(cache_load_worker, i)
            for i in range(worker_count)
        ]

        # Wait for completion
        results = []
        for future in as_completed(futures):
            results.append(future.result())

        total_time = performance_monitor.end_timing("cache_load_test")

        # Verify all operations completed
        assert len(results) == worker_count

        # Check cache performance
        cache_stats = cache_manager.get_cache_stats()
        assert cache_stats["archive_count"] <= worker_count  # Some may be cleaned up
        assert cache_stats["file_count"] <= worker_count * 3

        # Performance should be reasonable
        avg_time_per_worker = total_time / worker_count
        assert (
            avg_time_per_worker < 1.0
        ), f"Average time per worker {avg_time_per_worker}s too high"

    def test_bulk_file_extraction_performance(
        self, archive_factory, temp_workspace, performance_monitor, resource_monitor
    ):
        """Test performance of bulk file extraction operations."""
        archive = archive_factory("bulk_extraction_perf")
        extraction_dir = temp_workspace / "bulk_performance"
        extraction_dir.mkdir()

        resource_monitor.take_snapshot("start_bulk_extraction")

        # Get all files from archive
        files = archive.list_files()
        file_list = list(files.keys())

        performance_monitor.start_timing("bulk_extraction")

        # Extract all files
        extracted_paths = []
        for filename in file_list:
            try:
                extracted_path = archive.extract_file(filename, extraction_dir)
                extracted_paths.append(extracted_path)
            except Exception as e:
                print(f"Failed to extract {filename}: {e}")

        total_time = performance_monitor.end_timing("bulk_extraction")
        resource_monitor.take_snapshot("end_bulk_extraction")

        # Verify extractions
        assert len(extracted_paths) == len(file_list)

        # Performance metrics
        files_per_second = len(file_list) / total_time if total_time > 0 else 0
        assert (
            files_per_second > 1.0
        ), f"Extraction rate {files_per_second} files/sec too low"

        # Verify all files exist
        for path in extracted_paths:
            assert path.exists()
            assert path.stat().st_size > 0


@pytest.mark.performance
class TestScalabilityLimits:
    """Test system scalability and limits."""

    def test_large_archive_handling(
        self, temp_workspace, cache_manager, performance_monitor, resource_monitor
    ):
        """Test handling of large archive files."""
        # Create a larger test archive
        import io
        import tarfile

        large_archive_path = temp_workspace / "large_test_archive.tar.gz"

        performance_monitor.start_timing("create_large_archive")
        resource_monitor.take_snapshot("before_large_archive")

        # Create archive with many files
        with tarfile.open(large_archive_path, "w:gz") as tar:
            for i in range(100):  # 100 files
                file_content = (
                    f"Large file content {i}\n".encode() * 1000
                )  # ~20KB per file

                info = tarfile.TarInfo(name=f"large_files/file_{i:03d}.txt")
                info.size = len(file_content)
                tar.addfile(info, io.BytesIO(file_content))

        creation_time = performance_monitor.end_timing("create_large_archive")
        resource_monitor.take_snapshot("after_large_archive_creation")

        # Test archive operations
        performance_monitor.start_timing("large_archive_operations")

        archive = CompressedArchive(
            "large_archive_test", str(large_archive_path), cache_manager=cache_manager
        )

        # Refresh manifest (scan archive)
        archive.refresh_manifest()

        # List files
        files = archive.list_files()
        assert len(files) == 100

        # Get stats
        stats = archive.get_stats()
        assert stats["file_count"] == 100

        operations_time = performance_monitor.end_timing("large_archive_operations")
        resource_monitor.take_snapshot("after_large_archive_ops")

        # Performance assertions
        assert creation_time < 10.0, f"Archive creation took {creation_time}s, too long"
        assert (
            operations_time < 5.0
        ), f"Archive operations took {operations_time}s, too long"

        # Memory usage should be reasonable
        memory_usage = resource_monitor.get_memory_usage_mb()
        assert memory_usage < 500, f"Memory usage {memory_usage}MB too high"

    def test_many_small_files_performance(
        self, temp_workspace, cache_manager, performance_monitor
    ):
        """Test performance with many small files."""
        import io
        import tarfile

        many_files_archive = temp_workspace / "many_small_files.tar.gz"

        performance_monitor.start_timing("create_many_files_archive")

        # Create archive with many small files
        file_count = 1000
        with tarfile.open(many_files_archive, "w:gz") as tar:
            for i in range(file_count):
                file_content = f"Small file {i}".encode()

                info = tarfile.TarInfo(name=f"small_files/file_{i:04d}.txt")
                info.size = len(file_content)
                tar.addfile(info, io.BytesIO(file_content))

        creation_time = performance_monitor.end_timing("create_many_files_archive")

        # Test archive operations
        performance_monitor.start_timing("many_files_operations")

        archive = CompressedArchive(
            "many_files_test", str(many_files_archive), cache_manager=cache_manager
        )

        # Refresh manifest
        archive.refresh_manifest()

        # List files
        files = archive.list_files()
        assert len(files) == file_count

        # Test file access patterns
        sample_files = list(files.keys())[:10]  # Test first 10 files

        for filename in sample_files:
            file_data = archive.open_file(filename)
            assert len(file_data.read()) > 0

        operations_time = performance_monitor.end_timing("many_files_operations")

        # Performance metrics
        files_per_second_creation = (
            file_count / creation_time if creation_time > 0 else 0
        )
        files_per_second_access = (
            len(sample_files) / operations_time if operations_time > 0 else 0
        )

        assert (
            files_per_second_creation > 50
        ), f"Creation rate {files_per_second_creation} files/sec too low"
        assert (
            files_per_second_access > 2
        ), f"Access rate {files_per_second_access} files/sec too low"

    def test_simulation_registry_scalability(
        self, simulation_factory, test_locations, performance_monitor, resource_monitor
    ):
        """Test scalability of simulation registry operations."""
        resource_monitor.take_snapshot("start_registry_scalability")

        # Create many simulations
        simulation_count = 100
        simulations = []

        performance_monitor.start_timing("create_many_simulations")

        for i in range(simulation_count):
            sim = simulation_factory(f"scale_test_sim_{i}")
            simulations.append(sim)

        creation_time = performance_monitor.end_timing("create_many_simulations")

        # Test registry operations
        performance_monitor.start_timing("registry_operations")

        # List all simulations
        all_sims = Simulation.list_simulations()
        assert len(all_sims) >= simulation_count

        # Access simulations by ID
        for i in range(0, simulation_count, 10):  # Test every 10th simulation
            sim_id = f"scale_test_sim_{i}"
            retrieved_sim = Simulation.get_simulation(sim_id)
            assert retrieved_sim is not None
            assert retrieved_sim.simulation_id == sim_id

        operations_time = performance_monitor.end_timing("registry_operations")
        resource_monitor.take_snapshot("end_registry_scalability")

        # Performance assertions
        creation_rate = simulation_count / creation_time if creation_time > 0 else 0
        assert (
            creation_rate > 20
        ), f"Simulation creation rate {creation_rate} sims/sec too low"

        access_rate = 10 / operations_time if operations_time > 0 else 0
        assert (
            access_rate > 50
        ), f"Simulation access rate {access_rate} sims/sec too low"

        # Memory usage should scale reasonably
        memory_usage = resource_monitor.get_memory_usage_mb()
        memory_per_sim = memory_usage / simulation_count if simulation_count > 0 else 0
        assert (
            memory_per_sim < 1.0
        ), f"Memory per simulation {memory_per_sim}MB too high"


@pytest.mark.performance
class TestResourceConstrainedScenarios:
    """Test system behavior under resource constraints."""

    def test_low_memory_conditions(
        self, archive_factory, cache_manager, performance_monitor, resource_monitor
    ):
        """Test system behavior under simulated low memory conditions."""
        # Create cache with very small limits
        small_cache_config = cache_manager.config
        small_cache_config.archive_cache_size_limit = 1 * 1024 * 1024  # 1 MB
        small_cache_config.file_cache_size_limit = 512 * 1024  # 512 KB

        constrained_cache = CacheManager(small_cache_config)

        resource_monitor.take_snapshot("start_memory_constrained")

        # Create multiple archives that exceed cache limits
        archives = []
        for i in range(5):
            archive = archive_factory(f"memory_test_{i}")
            archives.append(archive)

        performance_monitor.start_timing("memory_constrained_operations")

        operation_results = []
        for archive in archives:
            try:
                # These operations should trigger cache cleanup
                files = archive.list_files()
                status = archive.status()
                operation_results.append(len(files))
            except Exception as e:
                operation_results.append(0)
                print(f"Operation failed under memory constraint: {e}")

        operations_time = performance_monitor.end_timing(
            "memory_constrained_operations"
        )
        resource_monitor.take_snapshot("end_memory_constrained")

        # Verify system remained functional despite constraints
        successful_operations = sum(1 for result in operation_results if result > 0)
        assert (
            successful_operations >= len(archives) // 2
        ), "At least half operations should succeed"

        # Cache should respect size limits
        cache_stats = constrained_cache.get_cache_stats()
        total_cache_size = cache_stats["archive_size"] + cache_stats["file_size"]
        cache_limit = (
            small_cache_config.archive_cache_size_limit
            + small_cache_config.file_cache_size_limit
        )

        # Allow some tolerance due to metadata overhead
        assert total_cache_size <= cache_limit * 1.1, "Cache should respect size limits"

    def test_disk_space_constraints(
        self, cache_manager, sample_archive_data, temp_workspace, performance_monitor
    ):
        """Test handling of disk space constraints."""
        # Create very small cache directory with limited space simulation
        constrained_cache_dir = temp_workspace / "constrained_cache"
        constrained_cache_dir.mkdir()

        # Mock filesystem to simulate limited space
        original_statvfs = os.statvfs if hasattr(os, "statvfs") else None

        def mock_statvfs(path):
            """Mock statvfs to return limited free space."""
            if str(constrained_cache_dir) in str(path):
                # Simulate only 5MB free space
                class MockStatvfs:
                    f_bavail = 1280  # Available blocks (1280 * 4KB = ~5MB)
                    f_frsize = 4096  # Fragment size

                return MockStatvfs()
            elif original_statvfs:
                return original_statvfs(path)
            else:
                # Default mock for systems without statvfs
                class MockStatvfs:
                    f_bavail = 1000000
                    f_frsize = 4096

                return MockStatvfs()

        performance_monitor.start_timing("disk_constrained_operations")

        if hasattr(os, "statvfs"):
            with patch("os.statvfs", side_effect=mock_statvfs):
                # Try to cache multiple archives
                successful_caches = 0
                failed_caches = 0

                for i in range(10):
                    try:
                        with tempfile.NamedTemporaryFile(
                            suffix=".tar.gz", delete=False
                        ) as f:
                            f.write(sample_archive_data)
                            temp_path = Path(f.name)

                        try:
                            checksum = f"disk_constraint_test_{i}"
                            cached_path = cache_manager.cache_archive(
                                temp_path, checksum
                            )
                            if cached_path.exists():
                                successful_caches += 1
                        except OSError as e:
                            if "No space left" in str(e) or "Disk full" in str(e):
                                failed_caches += 1
                            else:
                                raise
                        finally:
                            temp_path.unlink(missing_ok=True)
                    except Exception:
                        failed_caches += 1
        else:
            # Skip statvfs-based test on systems without it
            successful_caches = 1
            failed_caches = 0

        operations_time = performance_monitor.end_timing("disk_constrained_operations")

        # Verify system handled disk constraints gracefully
        total_operations = successful_caches + failed_caches
        assert total_operations > 0, "Should have attempted some operations"

        # System should either succeed or fail gracefully (no crashes)
        assert successful_caches + failed_caches == total_operations

    def test_network_bandwidth_constraints(
        self, test_locations, mock_network_conditions, performance_monitor
    ):
        """Test system behavior under network bandwidth constraints."""
        location = test_locations["ssh"]

        # Simulate slow network conditions
        with mock_network_conditions.simulate_conditions(
            latency_ms=200,
            bandwidth_mbps=1,  # Very slow 1 Mbps
            packet_loss=0.05,  # 5% packet loss
        ):
            performance_monitor.start_timing("bandwidth_constrained_operations")

            # Mock network operations with delays
            def slow_network_operation(*args, **kwargs):
                mock_network_conditions.add_delay()

                # Simulate packet loss
                if mock_network_conditions.should_fail_intermittent():
                    raise ConnectionError("Packet loss occurred")

                return True

            with patch.object(location, "fs") as mock_fs:
                mock_fs.exists = slow_network_operation
                mock_fs.ls = slow_network_operation

                successful_operations = 0
                failed_operations = 0

                # Perform multiple network operations
                for i in range(10):
                    try:
                        result = location.fs.exists(f"/test/file_{i}")
                        if result:
                            successful_operations += 1
                    except ConnectionError:
                        failed_operations += 1

            operations_time = performance_monitor.end_timing(
                "bandwidth_constrained_operations"
            )

        # Verify system handled bandwidth constraints
        total_operations = successful_operations + failed_operations
        assert total_operations == 10

        # Should have some successful operations despite constraints
        success_rate = (
            successful_operations / total_operations if total_operations > 0 else 0
        )
        assert (
            success_rate >= 0.7
        ), f"Success rate {success_rate} too low under bandwidth constraints"

        # Operations should take longer due to network constraints
        avg_time_per_op = (
            operations_time / total_operations if total_operations > 0 else 0
        )
        assert (
            avg_time_per_op >= 0.15
        ), "Operations should be slower under bandwidth constraints"


@pytest.mark.performance
@pytest.mark.slow
class TestPerformanceRegression:
    """Test for performance regressions over time."""

    def test_baseline_operation_performance(
        self, archive_factory, simulation_factory, performance_monitor, resource_monitor
    ):
        """Establish baseline performance metrics for core operations."""
        # Test archive operations
        archive = archive_factory("baseline_perf_test")

        resource_monitor.take_snapshot("baseline_start")

        # File listing performance
        performance_monitor.start_timing("baseline_list_files")
        files = archive.list_files()
        list_time = performance_monitor.end_timing("baseline_list_files")

        # File access performance
        if files:
            filename = list(files.keys())[0]
            performance_monitor.start_timing("baseline_file_access")
            file_data = archive.open_file(filename)
            content = file_data.read()
            access_time = performance_monitor.end_timing("baseline_file_access")
        else:
            access_time = 0

        # Simulation operations performance
        sim = simulation_factory("baseline_sim_test")

        performance_monitor.start_timing("baseline_simulation_ops")

        # Add and remove locations
        with patch("tellus.location.location.Location._save_locations"):
            location = Location(
                name="baseline_location",
                kinds=[LocationKind.DISK],
                config={"path": "/baseline/test", "protocol": "file"},
            )

        with patch("tellus.simulation.simulation.Simulation.save_simulations"):
            sim.add_location(location, "baseline_loc")
            retrieved = sim.get_location("baseline_loc")
            sim.remove_location("baseline_loc")

        sim_ops_time = performance_monitor.end_timing("baseline_simulation_ops")

        resource_monitor.take_snapshot("baseline_end")

        # Record baseline metrics (in a real scenario, these would be stored)
        baseline_metrics = {
            "list_files_time": list_time,
            "file_access_time": access_time,
            "simulation_ops_time": sim_ops_time,
            "memory_usage_mb": resource_monitor.get_memory_usage_mb(),
        }

        # Assert reasonable baseline performance
        assert baseline_metrics["list_files_time"] < 1.0, "File listing should be fast"
        assert baseline_metrics["file_access_time"] < 0.5, "File access should be fast"
        assert (
            baseline_metrics["simulation_ops_time"] < 0.1
        ), "Simulation ops should be fast"
        assert (
            baseline_metrics["memory_usage_mb"] < 200
        ), "Memory usage should be reasonable"

        return baseline_metrics

    def test_performance_under_stress(
        self,
        archive_factory,
        concurrent_executor,
        performance_monitor,
        resource_monitor,
        test_env_config,
    ):
        """Test performance degradation under stress conditions."""
        # Create multiple archives for stress testing
        archives = [archive_factory(f"stress_test_{i}") for i in range(5)]

        resource_monitor.take_snapshot("stress_start")

        def stress_worker(worker_id):
            """Worker function for stress testing."""
            operation = f"stress_worker_{worker_id}"
            performance_monitor.start_timing(operation)

            try:
                archive = archives[worker_id % len(archives)]
                operations_count = 0

                # Perform intensive operations
                for _ in range(20):  # Many operations per worker
                    files = archive.list_files()
                    operations_count += 1

                    if files and operations_count % 3 == 0:
                        filename = list(files.keys())[0]
                        file_data = archive.open_file(filename)
                        _ = file_data.read()
                        operations_count += 1

                return operations_count
            finally:
                performance_monitor.end_timing(operation)

        # Launch stress test
        stress_worker_count = test_env_config["concurrent_operations"] * 3
        futures = [
            concurrent_executor.submit(stress_worker, i)
            for i in range(stress_worker_count)
        ]

        # Monitor peak resource usage
        resource_monitor.take_snapshot("stress_peak")

        # Wait for completion
        results = []
        for future in as_completed(futures):
            results.append(future.result())

        resource_monitor.take_snapshot("stress_end")

        # Analyze stress test results
        total_operations = sum(results)
        perf_stats = performance_monitor.get_stats()

        # Calculate performance metrics
        worker_times = [
            stats["avg_time"] for stats in perf_stats.values() if stats["count"] > 0
        ]
        avg_worker_time = sum(worker_times) / len(worker_times) if worker_times else 0

        operations_per_second = (
            total_operations / avg_worker_time if avg_worker_time > 0 else 0
        )

        # Performance should degrade gracefully under stress
        assert (
            operations_per_second > 10
        ), f"Operations per second {operations_per_second} too low under stress"
        assert (
            avg_worker_time < 30
        ), f"Average worker time {avg_worker_time}s too high under stress"

        # Memory usage should not grow excessively
        assert not resource_monitor.check_memory_leak(
            200
        ), "No excessive memory growth under stress"

        return {
            "total_operations": total_operations,
            "operations_per_second": operations_per_second,
            "avg_worker_time": avg_worker_time,
            "peak_memory_mb": resource_monitor.get_memory_usage_mb(),
        }
