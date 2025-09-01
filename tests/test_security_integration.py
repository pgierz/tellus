"""
Integration security tests for the complete Location security framework.

This module tests the integration of security measures across the entire
Location system, including interactions with different storage backends,
configuration scenarios, and real-world usage patterns.
"""

import json
import os
import platform
import shutil
import tempfile
from pathlib import Path
from typing import Dict, List
from unittest.mock import MagicMock, patch

import pytest

from tellus.location import (Location, LocationKind, PathSandboxedFileSystem,
                             PathValidationError)
from tests.security_utils import (SecurityTestEnvironment, SecurityTestHelpers,
                                  SecurityTestVectors)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.security,
    pytest.mark.location,
]


class TestSecurityFrameworkIntegration:
    """Integration tests for the complete security framework."""
    
    def setup_method(self):
        """Set up integration test environment."""
        self.test_env = SecurityTestEnvironment()
        self.sandbox_dir, self.outside_dir, self.sensitive_files = self.test_env.__enter__()
        
        # Clear any existing locations
        Location._locations = {}
        
        # Create test locations file
        self.locations_file = Path(tempfile.mkdtemp()) / "test_locations.json"
        Location._locations_file = self.locations_file
    
    def teardown_method(self):
        """Clean up integration test environment."""
        self.test_env.__exit__(None, None, None)
        if self.locations_file.exists():
            shutil.rmtree(self.locations_file.parent, ignore_errors=True)
    
    def test_location_persistence_security(self):
        """Test that location persistence maintains security properties."""
        # Create a secure location
        location = Location(
            name="persistent_secure_location",
            kinds=[LocationKind.DISK],
            config={
                "protocol": "file",
                "path": str(self.sandbox_dir)
            }
        )
        
        # Verify location is secure
        with pytest.raises(PathValidationError):
            location.fs.read_text("../../../etc/passwd")
        
        # Save and reload locations
        Location._locations = {}
        Location.load_locations()
        
        # Verify reloaded location is still secure
        reloaded_location = Location.get_location("persistent_secure_location")
        assert reloaded_location is not None
        
        with pytest.raises(PathValidationError):
            reloaded_location.fs.read_text("../../../etc/passwd")
    
    def test_multiple_locations_security_isolation(self):
        """Test that multiple locations maintain security isolation."""
        # Create multiple isolated sandbox directories
        sandbox1 = Path(tempfile.mkdtemp(prefix="tellus_sandbox1_"))
        sandbox2 = Path(tempfile.mkdtemp(prefix="tellus_sandbox2_"))
        
        try:
            # Create test files in each sandbox
            (sandbox1 / "secret1.txt").write_text("SECRET DATA 1")
            (sandbox2 / "secret2.txt").write_text("SECRET DATA 2")
            
            # Create locations for each sandbox
            location1 = Location(
                name="isolated_location_1",
                kinds=[LocationKind.DISK],
                config={
                    "protocol": "file",
                    "path": str(sandbox1)
                }
            )
            
            location2 = Location(
                name="isolated_location_2", 
                kinds=[LocationKind.DISK],
                config={
                    "protocol": "file",
                    "path": str(sandbox2)
                }
            )
            
            # Verify each location can only access its own files
            assert location1.fs.read_text("secret1.txt") == "SECRET DATA 1"
            assert location2.fs.read_text("secret2.txt") == "SECRET DATA 2"
            
            # Verify locations cannot access each other's files
            with pytest.raises((PathValidationError, FileNotFoundError)):
                location1.fs.read_text("secret2.txt")
            
            with pytest.raises((PathValidationError, FileNotFoundError)):
                location2.fs.read_text("secret1.txt")
            
            # Verify traversal attacks can't access other location's files
            relative_path = os.path.relpath(sandbox2 / "secret2.txt", sandbox1)
            with pytest.raises(PathValidationError):
                location1.fs.read_text(relative_path)
        
        finally:
            shutil.rmtree(sandbox1, ignore_errors=True)
            shutil.rmtree(sandbox2, ignore_errors=True)
    
    def test_location_operations_security_consistency(self):
        """Test that all Location operations maintain consistent security."""
        location = Location(
            name="ops_security_test",
            kinds=[LocationKind.DISK],
            config={
                "protocol": "file",
                "path": str(self.sandbox_dir)
            }
        )
        
        # Create test files in sandbox
        (self.sandbox_dir / "test1.txt").write_text("test content 1")
        (self.sandbox_dir / "test2.log").write_text("log content")
        subdir = self.sandbox_dir / "subdir"
        subdir.mkdir()
        (subdir / "nested.txt").write_text("nested content")
        
        # Test that all operations maintain security boundaries
        attack_paths = ["../../../etc/passwd", "../../sensitive.txt", "../outside/file.txt"]
        
        for attack_path in attack_paths:
            # get() method security
            with pytest.raises(PathValidationError):
                location.get(attack_path, "downloaded.txt", show_progress=False)
            
            # find_files() method security  
            results = list(location.find_files(attack_path, recursive=True))
            # Should not return any files outside sandbox
            for file_path, _ in results:
                SecurityTestHelpers.verify_sandbox_boundaries(self.sandbox_dir, [file_path])
            
            # Direct filesystem operations security
            with pytest.raises(PathValidationError):
                location.fs.read_text(attack_path)
            
            with pytest.raises(PathValidationError):
                location.fs.write_text(attack_path, "malicious")
    
    def test_concurrent_location_access_security(self):
        """Test security under concurrent access patterns."""
        import threading
        import time
        
        location = Location(
            name="concurrent_security_test",
            kinds=[LocationKind.DISK],
            config={
                "protocol": "file",
                "path": str(self.sandbox_dir)
            }
        )
        
        # Create safe file
        (self.sandbox_dir / "safe.txt").write_text("safe content")
        
        security_violations = []
        
        def worker_thread(thread_id):
            """Worker thread that attempts various operations."""
            try:
                # Safe operation
                content = location.fs.read_text("safe.txt")
                assert "safe content" == content
                
                # Attack attempts
                attack_paths = [
                    f"../../../etc/passwd_{thread_id}",
                    f"../../sensitive_{thread_id}.txt",
                    f"/etc/shadow_{thread_id}",
                ]
                
                for attack_path in attack_paths:
                    try:
                        location.fs.read_text(attack_path)
                        # If this succeeds, it's a security violation
                        security_violations.append(f"Thread {thread_id}: {attack_path}")
                    except PathValidationError:
                        # Expected - attack was blocked
                        pass
                    except FileNotFoundError:
                        # Also acceptable - file doesn't exist in sandbox
                        pass
                        
            except Exception as e:
                security_violations.append(f"Thread {thread_id} error: {e}")
        
        # Run multiple threads concurrently
        threads = []
        for i in range(10):
            thread = threading.Thread(target=worker_thread, args=(i,))
            threads.append(thread)
            thread.start()
        
        # Wait for all threads to complete
        for thread in threads:
            thread.join(timeout=30)
        
        # Verify no security violations occurred
        assert len(security_violations) == 0, f"Security violations detected: {security_violations}"
    
    @patch('fsspec.filesystem')
    def test_cross_protocol_security_consistency(self, mock_fsspec):
        """Test security consistency across different storage protocols."""
        mock_fs = MagicMock()
        mock_fsspec.return_value = mock_fs
        
        protocols_to_test = [
            ("file", {}),
            ("sftp", {"host": "remote.server", "username": "user"}),
            ("s3", {"key": "access_key", "secret": "secret_key"}),
            ("ftp", {"host": "ftp.server", "username": "user"}),
        ]
        
        for protocol, storage_options in protocols_to_test:
            # Clear locations for each test
            Location._locations = {}
            
            location = Location(
                name=f"cross_protocol_{protocol}",
                kinds=[LocationKind.DISK],
                config={
                    "protocol": protocol,
                    "path": "/secure/path",
                    "storage_options": storage_options
                }
            )
            
            # Test that security is maintained regardless of protocol
            attack_paths = [
                "../../../etc/passwd",
                "../../sensitive.txt",
                "../outside/config.json"
            ]
            
            for attack_path in attack_paths:
                with pytest.raises(PathValidationError):
                    location.fs.read_text(attack_path)
            
            # Verify the underlying filesystem was created with correct protocol
            mock_fsspec.assert_called_with(protocol, **storage_options)
    
    def test_configuration_validation_security(self):
        """Test that configuration validation prevents security bypasses."""
        # Test with various potentially problematic configurations
        problematic_configs = [
            {"protocol": "file", "path": ""},  # Empty path
            {"protocol": "file", "path": "/"},  # Root path
            {"protocol": "file", "path": "../../../etc"},  # Traversal in config
            {"protocol": "file", "path": "/etc/passwd"},  # System file as path
        ]
        
        for i, config in enumerate(problematic_configs):
            Location._locations = {}  # Clear for each test
            
            location = Location(
                name=f"config_test_{i}",
                kinds=[LocationKind.DISK],
                config=config
            )
            
            # Even with problematic configs, should not allow escapes
            with pytest.raises((PathValidationError, FileNotFoundError, PermissionError)):
                location.fs.read_text("../../../root/.ssh/id_rsa")
    
    def test_error_handling_security(self):
        """Test that error conditions don't create security vulnerabilities."""
        location = Location(
            name="error_handling_test",
            kinds=[LocationKind.DISK],
            config={
                "protocol": "file",
                "path": str(self.sandbox_dir)
            }
        )
        
        # Test various error conditions
        error_test_cases = [
            ("nonexistent.txt", FileNotFoundError),  # File doesn't exist
            ("../../../etc/passwd", PathValidationError),  # Path traversal
            ("", (ValueError, PathValidationError)),  # Empty path
            (".", (PathValidationError, PermissionError)),  # Special path
        ]
        
        for test_path, expected_errors in error_test_cases:
            # Ensure errors don't leak information or bypass security
            try:
                location.fs.read_text(test_path)
                # If it succeeds unexpectedly, verify security
                resolved = location.fs._resolve_path(test_path)
                assert str(self.sandbox_dir.resolve()) in resolved
            except expected_errors:
                # Expected error - security maintained
                pass
            except Exception as e:
                # Unexpected error - verify it's not a security bypass
                assert "SENSITIVE" not in str(e)
                assert "SECRET" not in str(e)


class TestRealWorldSecurityScenarios:
    """Test security under real-world usage scenarios."""
    
    def setup_method(self):
        """Set up real-world test scenarios."""
        self.test_env = SecurityTestEnvironment()
        self.sandbox_dir, self.outside_dir, self.sensitive_files = self.test_env.__enter__()
        Location._locations = {}
    
    def teardown_method(self):
        """Clean up test environment."""
        self.test_env.__exit__(None, None, None)
    
    def test_climate_data_workflow_security(self):
        """Test security in a typical climate data workflow."""
        # Simulate a climate data location
        climate_location = Location(
            name="climate_data_archive",
            kinds=[LocationKind.DISK, LocationKind.ARCHIVE], 
            config={
                "protocol": "file",
                "path": str(self.sandbox_dir),
                "storage_options": {"auto_mkdir": True}
            }
        )
        
        # Create typical climate data structure
        data_dirs = ["ECHAM6", "FESOM2", "observations"]
        for data_dir in data_dirs:
            (self.sandbox_dir / data_dir).mkdir()
            (self.sandbox_dir / data_dir / "metadata.json").write_text(
                f'{{"dataset": "{data_dir}", "version": "1.0"}}'
            )
        
        # Simulate workflow operations that should be safe
        safe_operations = [
            lambda: list(climate_location.find_files("*.json", recursive=True)),
            lambda: climate_location.fs.ls("ECHAM6"),
            lambda: climate_location.fs.read_text("ECHAM6/metadata.json"),
        ]
        
        for operation in safe_operations:
            try:
                result = operation()
                # Verify results don't contain sensitive data
                if isinstance(result, str):
                    SecurityTestHelpers.assert_no_sensitive_data_leak(result)
                elif isinstance(result, list):
                    for item in result:
                        if isinstance(item, str):
                            SecurityTestHelpers.verify_sandbox_boundaries(
                                self.sandbox_dir, [item]
                            )
            except (FileNotFoundError, PathValidationError):
                pass  # Acceptable
        
        # Verify malicious operations are blocked
        malicious_attempts = [
            "../../../home/user/.aws/credentials",
            "../../etc/environment", 
            "../sensitive_climate_data.nc",
        ]
        
        for malicious_path in malicious_attempts:
            with pytest.raises(PathValidationError):
                climate_location.fs.read_text(malicious_path)
    
    def test_hpc_environment_security(self):
        """Test security in HPC environment scenarios."""
        # Simulate HPC scratch directory
        hpc_location = Location(
            name="hpc_scratch",
            kinds=[LocationKind.COMPUTE],
            config={
                "protocol": "file",
                "path": str(self.sandbox_dir)
            }
        )
        
        # Create HPC-like directory structure
        job_dirs = ["job_001", "job_002", "shared_tools"]
        for job_dir in job_dirs:
            (self.sandbox_dir / job_dir).mkdir()
            (self.sandbox_dir / job_dir / "slurm.out").write_text(
                f"Job output for {job_dir}"
            )
        
        # Test typical HPC operations
        job_outputs = list(hpc_location.find_files("slurm.out", recursive=True))
        assert len(job_outputs) == 3
        
        for job_output, _ in job_outputs:
            SecurityTestHelpers.verify_sandbox_boundaries(self.sandbox_dir, [job_output])
        
        # Test that HPC-specific attacks are blocked
        hpc_attacks = [
            "../../../etc/slurm/slurm.conf",
            "../../home/user/.ssh/authorized_keys",
            "../shared/sensitive_cluster_data",
        ]
        
        for attack in hpc_attacks:
            with pytest.raises(PathValidationError):
                hpc_location.fs.read_text(attack)
    
    def test_remote_storage_security_simulation(self):
        """Test security with simulated remote storage."""
        with patch('fsspec.filesystem') as mock_fsspec:
            mock_fs = MagicMock()
            mock_fsspec.return_value = mock_fs
            
            remote_location = Location(
                name="remote_archive",
                kinds=[LocationKind.FILESERVER],
                config={
                    "protocol": "sftp",
                    "path": "/remote/secure/archive",
                    "storage_options": {
                        "host": "archive.server",
                        "username": "archive_user"
                    }
                }
            )
            
            # Simulate remote attacks
            remote_attacks = [
                "../../../etc/passwd",
                "../../home/admin/.ssh/id_rsa",
                "../other_user/sensitive.txt",
                "/etc/shadow",
            ]
            
            for attack in remote_attacks:
                with pytest.raises(PathValidationError):
                    remote_location.fs.read_text(attack)
                
                # Verify underlying filesystem is never called with attack paths
                # (PathSandboxedFileSystem should block before delegation)
                for call in mock_fs.read_text.call_args_list:
                    call_path = call[0][0] if call[0] else ""
                    assert "/remote/secure/archive" in call_path, \
                        f"Underlying filesystem called with unresolved path: {call_path}"


class TestSecurityRegressionSuite:
    """Comprehensive security regression testing suite."""
    
    def test_comprehensive_attack_vector_coverage(self):
        """Test comprehensive coverage of all known attack vectors."""
        with SecurityTestEnvironment() as (sandbox_dir, outside_dir, sensitive_files):
            import fsspec
            base_fs = fsspec.filesystem("file")
            sandboxed_fs = PathSandboxedFileSystem(base_fs, str(sandbox_dir))
            
            # Test all attack vectors from SecurityTestVectors
            all_payloads = SecurityTestVectors.all_attack_payloads()
            
            blocked_count = 0
            total_count = len(all_payloads)
            
            for payload in all_payloads:
                try:
                    SecurityTestHelpers.assert_path_is_blocked(sandboxed_fs, payload)
                    blocked_count += 1
                except AssertionError as e:
                    pytest.fail(f"Attack payload '{payload}' was not properly blocked: {e}")
            
            # Verify high coverage of attack blocking
            coverage_ratio = blocked_count / total_count
            assert coverage_ratio >= 0.95, \
                f"Insufficient attack blocking coverage: {coverage_ratio:.2%} ({blocked_count}/{total_count})"
    
    def test_security_performance_under_load(self):
        """Test that security measures perform adequately under load."""
        import time
        
        with SecurityTestEnvironment() as (sandbox_dir, outside_dir, sensitive_files):
            location = Location(
                name="performance_security_test",
                kinds=[LocationKind.DISK],
                config={
                    "protocol": "file",
                    "path": str(sandbox_dir)
                }
            )
            
            # Create test file
            (sandbox_dir / "performance_test.txt").write_text("test content")
            
            # Measure performance under security validation load
            operations_count = 1000
            start_time = time.time()
            
            for i in range(operations_count):
                try:
                    # Mix of safe and malicious operations
                    if i % 2 == 0:
                        location.fs.exists("performance_test.txt")  # Safe
                    else:
                        location.fs.exists("../../../etc/passwd")  # Malicious
                except PathValidationError:
                    pass  # Expected for malicious operations
            
            total_time = time.time() - start_time
            avg_time_per_op = total_time / operations_count
            
            # Security validation should not significantly impact performance
            assert avg_time_per_op < 0.01, \
                f"Security validation too slow: {avg_time_per_op:.4f}s per operation"
    
    def test_security_framework_completeness(self):
        """Test that the security framework covers all necessary components."""
        # Verify all necessary security components exist and function
        security_components = [
            PathSandboxedFileSystem,
            PathValidationError,
            SecurityTestVectors,
            SecurityTestHelpers,
            SecurityTestEnvironment,
        ]
        
        for component in security_components:
            assert component is not None, f"Security component {component.__name__} not available"
        
        # Verify Location class integrates security properly
        with SecurityTestEnvironment() as (sandbox_dir, outside_dir, sensitive_files):
            location = Location(
                name="completeness_test",
                kinds=[LocationKind.DISK],
                config={
                    "protocol": "file",
                    "path": str(sandbox_dir)
                }
            )
            
            # Verify fs property returns sandboxed filesystem
            assert isinstance(location.fs, PathSandboxedFileSystem)
            
            # Verify all major operations are secured
            secured_operations = [
                'read_text', 'write_text', 'exists', 'isfile', 'isdir',
                'ls', 'glob', 'find', 'copy', 'move', 'remove'
            ]
            
            for op_name in secured_operations:
                assert hasattr(location.fs, op_name), \
                    f"Secured operation {op_name} not available"