"""
Configuration scenario tests for Location path handling and sandboxing.

This test suite verifies that Location configurations work correctly with different
path types, protocols, and storage backends while maintaining proper sandboxing.
Tests cover various real-world configuration scenarios and edge cases.
"""

import json
import shutil
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from tellus.application.dtos import CreateLocationDto
from tellus.application.services.location_service import \
    LocationApplicationService
from tellus.domain.entities.location import LocationEntity
from tellus.domain.entities.location import LocationKind as DomainLocationKind
from tellus.infrastructure.repositories.json_location_repository import \
    JsonLocationRepository
from tellus.location import Location, LocationKind, PathSandboxedFileSystem
from tellus.location.sandboxed_filesystem import PathValidationError


class TestRelativePathConfigurations:
    """Test Location configurations with relative paths."""
    
    def setup_method(self):
        """Set up test environment."""
        Location._locations = {}
        self.temp_root = Path(tempfile.mkdtemp())
        self.locations_file = self.temp_root / "locations.json"
        Location._locations_file = self.locations_file
        
        # Create nested directory structure
        self.data_dir = self.temp_root / "data"
        self.nested_dir = self.data_dir / "nested" / "deep"
        self.nested_dir.mkdir(parents=True)
        
        # Create test files
        (self.data_dir / "root_file.txt").write_text("root content")
        (self.nested_dir / "deep_file.txt").write_text("deep content")

    def teardown_method(self):
        """Clean up test environment."""
        shutil.rmtree(self.temp_root, ignore_errors=True)

    def test_relative_path_configuration(self):
        """Test Location with relative path configuration."""
        # Use relative path from temp_root
        relative_path = "data"
        
        # Create location with relative path
        location = Location(
            name="relative_path_test",
            kinds=[LocationKind.DISK],
            config={
                "protocol": "file",
                "path": relative_path
            }
        )
        
        # Change to temp_root directory for testing
        import os
        original_cwd = os.getcwd()
        try:
            os.chdir(str(self.temp_root))
            
            # Test filesystem operations
            fs = location.fs
            assert isinstance(fs, PathSandboxedFileSystem)
            
            # Should resolve relative to current working directory when location created
            # But sandboxed to the resolved absolute path
            assert fs.exists("root_file.txt")
            assert fs.exists("nested/deep/deep_file.txt")
            
            content = fs.read_text("root_file.txt")
            assert content == "root content"
            
            # Test sandboxing still works
            with pytest.raises(PathValidationError):
                fs.read_text("../outside.txt")
                
        finally:
            os.chdir(original_cwd)

    def test_dot_relative_path_configuration(self):
        """Test Location with dot (.) relative path configuration."""
        location = Location(
            name="dot_path_test", 
            kinds=[LocationKind.DISK],
            config={
                "protocol": "file",
                "path": "."
            }
        )
        
        fs = location.fs
        
        # Should work in current directory but sandboxed
        test_file = Path("temp_test_file.txt")
        try:
            fs.write_text("temp_test_file.txt", "temp content")
            assert test_file.exists()
            assert fs.read_text("temp_test_file.txt") == "temp content"
        finally:
            test_file.unlink(missing_ok=True)

    def test_nested_relative_path_configuration(self):
        """Test Location with nested relative path configuration."""
        location = Location(
            name="nested_relative_test",
            kinds=[LocationKind.DISK],  
            config={
                "protocol": "file",
                "path": str(self.nested_dir.relative_to(Path.cwd()))
            }
        )
        
        fs = location.fs
        
        # Should be sandboxed to the nested directory
        assert fs.exists("deep_file.txt")
        content = fs.read_text("deep_file.txt") 
        assert content == "deep content"
        
        # Should not be able to access parent directories
        with pytest.raises(PathValidationError):
            fs.read_text("../../../root_file.txt")


class TestAbsolutePathConfigurations:
    """Test Location configurations with absolute paths."""
    
    def setup_method(self):
        """Set up test environment."""
        Location._locations = {}
        self.temp_root = Path(tempfile.mkdtemp())
        self.locations_file = self.temp_root / "locations.json"
        Location._locations_file = self.locations_file
        
        # Create test directories
        self.abs_dir1 = self.temp_root / "absolute1"
        self.abs_dir2 = self.temp_root / "absolute2"
        self.abs_dir1.mkdir()
        self.abs_dir2.mkdir()
        
        # Create test files
        (self.abs_dir1 / "file1.txt").write_text("content1")
        (self.abs_dir2 / "file2.txt").write_text("content2")

    def teardown_method(self):
        """Clean up test environment."""
        shutil.rmtree(self.temp_root, ignore_errors=True)

    def test_absolute_path_configuration(self):
        """Test Location with absolute path configuration."""
        location = Location(
            name="absolute_path_test",
            kinds=[LocationKind.DISK],
            config={
                "protocol": "file",
                "path": str(self.abs_dir1.absolute())
            }
        )
        
        fs = location.fs
        assert isinstance(fs, PathSandboxedFileSystem)
        
        # Should be sandboxed to absolute directory
        assert fs.exists("file1.txt")
        assert fs.read_text("file1.txt") == "content1"
        
        # Should not access other absolute directories
        with pytest.raises(PathValidationError):
            fs.read_text(str(self.abs_dir2 / "file2.txt"))

    def test_multiple_absolute_path_locations(self):
        """Test multiple Locations with different absolute paths."""
        loc1 = Location(
            name="abs_location1",
            kinds=[LocationKind.DISK],
            config={
                "protocol": "file", 
                "path": str(self.abs_dir1.absolute())
            }
        )
        
        loc2 = Location(
            name="abs_location2",
            kinds=[LocationKind.DISK],
            config={
                "protocol": "file",
                "path": str(self.abs_dir2.absolute())
            }
        )
        
        # Each location should only see its own files
        fs1 = loc1.fs
        fs2 = loc2.fs
        
        assert fs1.exists("file1.txt")
        assert not fs1.exists("file2.txt")
        
        assert fs2.exists("file2.txt")
        assert not fs2.exists("file1.txt")
        
        # Verify persistence maintains isolation
        Location._locations = {}
        Location.load_locations()
        
        reloaded1 = Location.get_location("abs_location1")
        reloaded2 = Location.get_location("abs_location2")
        
        assert reloaded1.fs.exists("file1.txt")
        assert not reloaded1.fs.exists("file2.txt")
        assert reloaded2.fs.exists("file2.txt") 
        assert not reloaded2.fs.exists("file1.txt")

    def test_absolute_path_with_symlinks(self):
        """Test Location with absolute path containing symlinks."""
        # Create target directory
        target_dir = self.temp_root / "target"
        target_dir.mkdir()
        (target_dir / "target_file.txt").write_text("target content")
        
        # Create symlink
        symlink_dir = self.temp_root / "symlinked" 
        symlink_dir.symlink_to(target_dir)
        
        location = Location(
            name="symlink_test",
            kinds=[LocationKind.DISK],
            config={
                "protocol": "file",
                "path": str(symlink_dir.absolute())
            }
        )
        
        fs = location.fs
        
        # Should follow symlink but remain sandboxed
        assert fs.exists("target_file.txt")
        content = fs.read_text("target_file.txt")
        assert content == "target content"
        
        # Should still prevent escaping sandbox
        with pytest.raises(PathValidationError):
            fs.read_text("../../outside.txt")


class TestEmptyAndSpecialPathConfigurations:
    """Test Location configurations with empty, None, and special path values."""
    
    def setup_method(self):
        """Set up test environment."""
        Location._locations = {}
        self.temp_root = Path(tempfile.mkdtemp())
        self.locations_file = self.temp_root / "locations.json"
        Location._locations_file = self.locations_file

    def teardown_method(self):
        """Clean up test environment."""
        shutil.rmtree(self.temp_root, ignore_errors=True)

    def test_empty_path_configuration(self):
        """Test Location with empty path configuration."""
        location = Location(
            name="empty_path_test",
            kinds=[LocationKind.DISK],
            config={
                "protocol": "file",
                "path": ""
            }
        )
        
        fs = location.fs
        assert isinstance(fs, PathSandboxedFileSystem)
        assert fs.base_path == ""
        
        # Should work in current working directory
        test_file = Path("empty_path_test.txt")
        try:
            fs.write_text("empty_path_test.txt", "empty path content")
            assert test_file.exists()
            assert fs.read_text("empty_path_test.txt") == "empty path content"
        finally:
            test_file.unlink(missing_ok=True)

    def test_no_path_in_configuration(self):
        """Test Location without path key in configuration."""
        location = Location(
            name="no_path_test",
            kinds=[LocationKind.DISK],
            config={
                "protocol": "file",
                "storage_options": {"auto_mkdir": True}
            }
        )
        
        fs = location.fs
        assert isinstance(fs, PathSandboxedFileSystem)
        assert fs.base_path == ""  # Should default to empty
        
        # Should work but not be restricted to a specific directory
        test_file = Path("no_path_test.txt")
        try:
            fs.write_text("no_path_test.txt", "no path content")
            assert test_file.exists()
        finally:
            test_file.unlink(missing_ok=True)

    def test_root_path_configuration(self):
        """Test Location with root path configuration."""
        # Note: This test may need admin privileges on some systems
        # We'll test with a safe path that simulates root behavior
        location = Location(
            name="root_like_test",
            kinds=[LocationKind.DISK],
            config={
                "protocol": "file",
                "path": str(self.temp_root)  # Use temp_root as safe "root"
            }
        )
        
        fs = location.fs
        
        # Should work with root-like path
        assert isinstance(fs, PathSandboxedFileSystem)
        
        # Create file in "root" directory
        fs.write_text("root_file.txt", "root content")
        assert (self.temp_root / "root_file.txt").exists()
        
        # Should still prevent escaping
        with pytest.raises(PathValidationError):
            fs.read_text("../outside_root.txt")


class TestProtocolSpecificConfigurations:
    """Test Location configurations for different protocols."""
    
    def setup_method(self):
        """Set up test environment."""
        Location._locations = {}
        self.temp_root = Path(tempfile.mkdtemp())
        self.locations_file = self.temp_root / "locations.json"
        Location._locations_file = self.locations_file
        self.data_dir = self.temp_root / "protocol_data"
        self.data_dir.mkdir()

    def teardown_method(self):
        """Clean up test environment."""
        shutil.rmtree(self.temp_root, ignore_errors=True)

    def test_file_protocol_configuration(self):
        """Test Location with file protocol configuration."""
        location = Location(
            name="file_protocol_test",
            kinds=[LocationKind.DISK],
            config={
                "protocol": "file",
                "path": str(self.data_dir),
                "storage_options": {
                    "auto_mkdir": True,
                    "block_size": 8192
                }
            }
        )
        
        fs = location.fs
        assert isinstance(fs, PathSandboxedFileSystem)
        
        # Test file operations
        fs.write_text("protocol_test.txt", "file protocol content")
        assert fs.exists("protocol_test.txt")
        content = fs.read_text("protocol_test.txt")
        assert content == "file protocol content"

    @patch('fsspec.filesystem')
    def test_sftp_protocol_configuration(self, mock_fsspec):
        """Test Location with SFTP protocol configuration."""
        mock_fs = MagicMock()
        mock_fsspec.return_value = mock_fs
        
        location = Location(
            name="sftp_protocol_test", 
            kinds=[LocationKind.COMPUTE, LocationKind.FILESERVER],
            config={
                "protocol": "sftp",
                "path": "/remote/data",
                "storage_options": {
                    "host": "compute.example.com",
                    "username": "testuser",
                    "port": 22,
                    "key_filename": "/path/to/key"
                }
            }
        )
        
        # Verify fsspec called with correct parameters
        expected_options = {
            "host": "compute.example.com",
            "username": "testuser", 
            "port": 22,
            "key_filename": "/path/to/key"
        }
        mock_fsspec.assert_called_with("sftp", **expected_options)
        
        # Verify sandboxed filesystem created
        fs = location.fs
        assert isinstance(fs, PathSandboxedFileSystem)
        assert fs.base_path.rstrip("/") == "/remote/data"
        assert fs._fs == mock_fs

    @patch('fsspec.filesystem')
    def test_s3_protocol_configuration(self, mock_fsspec):
        """Test Location with S3 protocol configuration.""" 
        mock_fs = MagicMock()
        mock_fsspec.return_value = mock_fs
        
        location = Location(
            name="s3_protocol_test",
            kinds=[LocationKind.FILESERVER],
            config={
                "protocol": "s3",
                "path": "my-bucket/data",
                "storage_options": {
                    "endpoint_url": "https://s3.amazonaws.com",
                    "key": "access_key",
                    "secret": "secret_key",
                    "region": "us-east-1"
                }
            }
        )
        
        # Verify fsspec configuration
        expected_options = {
            "endpoint_url": "https://s3.amazonaws.com",
            "key": "access_key", 
            "secret": "secret_key",
            "region": "us-east-1",
            "host": "s3_protocol_test"  # Should add host from location name
        }
        mock_fsspec.assert_called_with("s3", **expected_options)
        
        # Verify sandboxing
        fs = location.fs
        assert isinstance(fs, PathSandboxedFileSystem)
        assert fs.base_path == "my-bucket/data"

    def test_protocol_configuration_persistence(self):
        """Test that protocol configurations persist correctly."""
        configs = [
            {
                "name": "local_config",
                "protocol": "file",
                "path": str(self.data_dir / "local"),
                "storage_options": {"auto_mkdir": True}
            },
            {
                "name": "remote_config", 
                "protocol": "sftp",
                "path": "/remote/path",
                "storage_options": {
                    "host": "remote.example.com",
                    "username": "user",
                    "timeout": 30
                }
            }
        ]
        
        # Create locations
        locations = []
        for config in configs:
            loc = Location(
                name=config["name"],
                kinds=[LocationKind.DISK],
                config={
                    "protocol": config["protocol"],
                    "path": config["path"], 
                    "storage_options": config["storage_options"]
                }
            )
            locations.append(loc)
        
        # Verify persistence
        assert self.locations_file.exists()
        
        with open(self.locations_file) as f:
            data = json.load(f)
        
        for config in configs:
            assert config["name"] in data
            loc_data = data[config["name"]]
            assert loc_data["config"]["protocol"] == config["protocol"]
            assert loc_data["config"]["path"] == config["path"]
            assert loc_data["config"]["storage_options"] == config["storage_options"]
        
        # Test reload
        Location._locations = {}
        Location.load_locations()
        
        for config in configs:
            reloaded = Location.get_location(config["name"])
            assert reloaded is not None
            assert reloaded.config["protocol"] == config["protocol"]
            assert reloaded.config["path"] == config["path"]
            assert reloaded.config["storage_options"] == config["storage_options"]


class TestTemplatedPathConfigurations:
    """Test Location configurations with templated paths for context resolution."""
    
    def setup_method(self):
        """Set up test environment."""
        Location._locations = {}
        self.temp_root = Path(tempfile.mkdtemp())
        self.locations_file = self.temp_root / "locations.json"
        Location._locations_file = self.locations_file
        
        # Create directory structure for template testing
        self.base_dir = self.temp_root / "templates"
        self.base_dir.mkdir()
        
        # Create template-like directory structure
        model_exp_path = self.base_dir / "CESM2" / "historical" / "r1i1p1f1"
        model_exp_path.mkdir(parents=True)
        (model_exp_path / "data.nc").write_text("model data")

    def teardown_method(self):
        """Clean up test environment."""
        shutil.rmtree(self.temp_root, ignore_errors=True)

    def test_location_with_resolved_template_paths(self):
        """Test Location works with paths that have been resolved from templates."""
        # This simulates what happens after template resolution by Simulation
        resolved_base_path = str(self.base_dir)
        
        location = Location(
            name="resolved_template_test",
            kinds=[LocationKind.DISK],
            config={
                "protocol": "file",
                "path": resolved_base_path,
                "template_context": "resolved"  # Metadata indicating this was templated
            }
        )
        
        fs = location.fs
        
        # Should access resolved template paths
        assert fs.exists("CESM2/historical/r1i1p1f1/data.nc")
        content = fs.read_text("CESM2/historical/r1i1p1f1/data.nc")
        assert content == "model data"
        
        # Test that path resolution persists
        Location._locations = {}
        Location.load_locations()
        
        reloaded = Location.get_location("resolved_template_test")
        assert reloaded.config["path"] == resolved_base_path
        assert reloaded.config["template_context"] == "resolved"
        
        # Verify filesystem still works after reload
        reloaded_fs = reloaded.fs
        assert reloaded_fs.exists("CESM2/historical/r1i1p1f1/data.nc")

    def test_location_configuration_with_context_metadata(self):
        """Test Location configurations that include context metadata."""
        # Simulate configuration that would come from Simulation+LocationContext
        config = {
            "protocol": "file",
            "path": str(self.base_dir),
            "context_metadata": {
                "model_id": "CESM2", 
                "experiment_id": "historical",
                "variant_label": "r1i1p1f1",
                "template_resolved": True,
                "original_template": "{base_path}/{model_id}/{experiment_id}/{variant_label}"
            },
            "access_patterns": ["*.nc", "*.txt"],
            "description": "Model output data with resolved template paths"
        }
        
        location = Location(
            name="context_metadata_test",
            kinds=[LocationKind.DISK, LocationKind.COMPUTE],
            config=config
        )
        
        # Test filesystem works
        fs = location.fs
        assert fs.exists("CESM2/historical/r1i1p1f1/data.nc")
        
        # Test persistence of complex configuration
        Location._locations = {}
        Location.load_locations()
        
        reloaded = Location.get_location("context_metadata_test")
        assert reloaded.config["context_metadata"]["model_id"] == "CESM2"
        assert reloaded.config["context_metadata"]["template_resolved"] is True
        assert reloaded.config["access_patterns"] == ["*.nc", "*.txt"]
        
        # Verify filesystem preservation
        reloaded_fs = reloaded.fs
        assert reloaded_fs.exists("CESM2/historical/r1i1p1f1/data.nc")


class TestNewArchitectureConfigurationScenarios:
    """Test configuration scenarios with the new domain-driven architecture."""
    
    def setup_method(self):
        """Set up test environment."""
        self.temp_root = Path(tempfile.mkdtemp())
        self.repo_file = self.temp_root / "locations_new.json"
        self.repository = JsonLocationRepository(self.repo_file)
        self.service = LocationApplicationService(self.repository)
        
        self.data_dir = self.temp_root / "new_arch_data"
        self.data_dir.mkdir()
        (self.data_dir / "test_file.txt").write_text("test content")

    def teardown_method(self):
        """Clean up test environment."""
        shutil.rmtree(self.temp_root, ignore_errors=True)

    def test_service_layer_path_configuration_validation(self):
        """Test that service layer validates path configurations correctly."""
        # Test valid configuration
        valid_dto = CreateLocationDto(
            name="valid_service_test",
            kinds=["DISK"],
            protocol="file",
            path=str(self.data_dir),
            storage_options={"auto_mkdir": True}
        )
        
        result = self.service.create_location(valid_dto)
        assert result.name == "valid_service_test"
        assert result.path == str(self.data_dir)
        
        # Test configuration with complex storage options
        complex_dto = CreateLocationDto(
            name="complex_service_test",
            kinds=["COMPUTE", "FILESERVER"],
            protocol="sftp",
            path="/remote/complex/path",
            storage_options={
                "host": "hpc.example.com",
                "username": "researcher", 
                "port": 22,
                "timeout": 30,
                "compression": True,
                "key_filename": "/home/user/.ssh/id_rsa"
            },
            additional_config={
                "max_retries": 3,
                "retry_delay": 5,
                "description": "HPC cluster storage"
            }
        )
        
        complex_result = self.service.create_location(complex_dto)
        assert complex_result.name == "complex_service_test"
        assert complex_result.path == "/remote/complex/path"
        assert complex_result.storage_options["host"] == "hpc.example.com"
        assert complex_result.additional_config["max_retries"] == 3

    def test_repository_handles_various_path_formats(self):
        """Test that repository correctly handles various path formats."""
        path_scenarios = [
            {
                "name": "absolute_unix_path",
                "path": "/absolute/unix/path",
                "protocol": "file"
            },
            {
                "name": "relative_path",  
                "path": "relative/path",
                "protocol": "file"
            },
            {
                "name": "s3_path",
                "path": "bucket-name/prefix/path", 
                "protocol": "s3"
            },
            {
                "name": "empty_path",
                "path": "",
                "protocol": "file"
            }
        ]
        
        # Create entities for each scenario
        entities = []
        for scenario in path_scenarios:
            entity = LocationEntity(
                name=scenario["name"],
                kinds=[DomainLocationKind.DISK],
                config={
                    "protocol": scenario["protocol"],
                    "path": scenario["path"]
                }
            )
            entities.append(entity)
            self.repository.save(entity)
        
        # Verify persistence
        all_locations = self.repository.list_all()
        assert len(all_locations) == len(path_scenarios)
        
        # Verify each scenario
        for scenario in path_scenarios:
            retrieved = self.repository.get_by_name(scenario["name"])
            assert retrieved is not None
            assert retrieved.get_base_path() == scenario["path"]
            assert retrieved.get_protocol() == scenario["protocol"]

    def test_configuration_edge_cases(self):
        """Test configuration edge cases and error conditions."""
        # Test with None path (should default to empty)
        entity_none_path = LocationEntity(
            name="none_path_test",
            kinds=[DomainLocationKind.DISK],
            config={
                "protocol": "file"
                # No path key
            }
        )
        
        self.repository.save(entity_none_path)
        retrieved = self.repository.get_by_name("none_path_test")
        assert retrieved.get_base_path() == ""
        
        # Test with complex nested configuration
        nested_config = {
            "protocol": "s3",
            "path": "complex-bucket/nested/path",
            "storage_options": {
                "endpoint_url": "https://minio.example.com",
                "access_key_id": "minioaccess",
                "secret_access_key": "miniosecret",
                "region_name": "us-east-1",
                "config": {
                    "signature_version": "s3v4",
                    "retries": {"max_attempts": 5}
                }
            },
            "custom_settings": {
                "cache_type": "memory",
                "cache_size": 100,
                "metadata": {
                    "project": "climate_research",
                    "department": "earth_sciences",
                    "tags": ["production", "critical"]
                }
            }
        }
        
        complex_entity = LocationEntity(
            name="complex_nested_test",
            kinds=[DomainLocationKind.FILESERVER, DomainLocationKind.TAPE],
            config=nested_config,
            optional=True
        )
        
        self.repository.save(complex_entity)
        
        # Verify complex configuration persists correctly
        retrieved_complex = self.repository.get_by_name("complex_nested_test")
        assert retrieved_complex.config == nested_config
        assert retrieved_complex.get_base_path() == "complex-bucket/nested/path"
        assert retrieved_complex.get_protocol() == "s3"
        assert retrieved_complex.optional is True
        
        # Verify nested structures preserved
        storage_opts = retrieved_complex.get_storage_options()
        assert storage_opts["config"]["retries"]["max_attempts"] == 5
        assert retrieved_complex.config["custom_settings"]["metadata"]["tags"] == ["production", "critical"]