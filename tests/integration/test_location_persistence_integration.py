"""
Integration tests for Location filesystem bug fix with persistence and configuration systems.

This test suite verifies that the PathSandboxedFileSystem fix integrates correctly 
with Location configuration, serialization, persistence, and context templating.
Tests ensure the security fix works end-to-end in realistic Tellus workflows.
"""

import json
import shutil
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from tellus.application.dtos import CreateLocationDto, UpdateLocationDto
from tellus.application.services.location_service import \
    LocationApplicationService
from tellus.domain.entities.location import LocationEntity
from tellus.domain.entities.location import LocationKind as DomainLocationKind
from tellus.infrastructure.repositories.json_location_repository import \
    JsonLocationRepository
# Test both legacy and new Location implementations
from tellus.location import Location as LegacyLocation
from tellus.location import LocationKind, PathSandboxedFileSystem
from tellus.simulation.context import LocationContext


class TestLegacyLocationPersistenceIntegration:
    """Test legacy Location system with PathSandboxedFileSystem integration."""
    
    def setup_method(self):
        """Set up test environment."""
        # Clear legacy location registry
        LegacyLocation._locations = {}
        
        # Create temporary directories
        self.temp_root = Path(tempfile.mkdtemp())
        self.location_base = self.temp_root / "location_data"
        self.location_base.mkdir()
        self.external_dir = self.temp_root / "external"
        self.external_dir.mkdir()
        
        # Set up temporary locations.json file
        self.locations_file = self.temp_root / "locations.json"
        LegacyLocation._locations_file = self.locations_file
        
        # Create test files in location
        self.test_file = self.location_base / "test.txt"
        self.test_file.write_text("test content")
        self.test_subdir = self.location_base / "subdir"
        self.test_subdir.mkdir()
        (self.test_subdir / "nested.txt").write_text("nested content")
        
        # Create external file (should be inaccessible)
        self.external_file = self.external_dir / "external.txt"
        self.external_file.write_text("external content")

    def teardown_method(self):
        """Clean up test environment."""
        shutil.rmtree(self.temp_root, ignore_errors=True)

    def test_location_creation_persists_with_sandboxed_fs(self):
        """Test that Location creation with path config persists correctly with sandboxed filesystem."""
        # Create location with path configuration
        config = {
            "protocol": "file",
            "path": str(self.location_base),
            "storage_options": {"auto_mkdir": True}
        }
        
        location = LegacyLocation(
            name="test_location",
            kinds=[LocationKind.DISK],
            config=config,
            optional=False
        )
        
        # Verify location was persisted
        assert self.locations_file.exists()
        
        # Verify JSON content
        with open(self.locations_file) as f:
            data = json.load(f)
        
        assert "test_location" in data
        location_data = data["test_location"]
        assert location_data["config"]["path"] == str(self.location_base)
        assert location_data["config"]["protocol"] == "file"
        assert location_data["kinds"] == ["DISK"]
        
        # Verify filesystem is properly sandboxed
        fs = location.fs
        assert isinstance(fs, PathSandboxedFileSystem)
        assert fs.base_path.rstrip("/") == str(self.location_base)
        
        # Test filesystem operations work within sandbox
        assert fs.exists("test.txt")
        assert fs.read_text("test.txt") == "test content"
        assert fs.exists("subdir/nested.txt")
        
        # Test filesystem prevents escaping sandbox
        from tellus.location.sandboxed_filesystem import PathValidationError
        with pytest.raises(PathValidationError):
            fs.read_text("../external.txt")

    def test_location_load_preserves_sandboxed_behavior(self):
        """Test that loaded Locations maintain sandboxed filesystem behavior."""
        # Create and save a location
        config = {
            "protocol": "file", 
            "path": str(self.location_base),
            "custom_setting": "test_value"
        }
        
        original = LegacyLocation(
            name="persistent_test",
            kinds=[LocationKind.DISK, LocationKind.COMPUTE],
            config=config,
            optional=True
        )
        
        # Clear in-memory locations and reload
        LegacyLocation._locations = {}
        LegacyLocation.load_locations()
        
        # Retrieve the loaded location
        loaded = LegacyLocation.get_location("persistent_test")
        assert loaded is not None
        
        # Verify configuration preserved
        assert loaded.config["path"] == str(self.location_base)
        assert loaded.config["protocol"] == "file"
        assert loaded.config["custom_setting"] == "test_value"
        assert loaded.optional is True
        assert len(loaded.kinds) == 2
        
        # Verify filesystem is still sandboxed after loading
        fs = loaded.fs
        assert isinstance(fs, PathSandboxedFileSystem)
        assert fs.base_path.rstrip("/") == str(self.location_base)
        
        # Test operations work correctly
        assert fs.exists("test.txt")
        content = fs.read_text("test.txt")
        assert content == "test content"
        
        # Test directory traversal is still prevented
        from tellus.location.sandboxed_filesystem import PathValidationError
        with pytest.raises(PathValidationError):
            fs.write_text("../malicious.txt", "bad content")

    def test_location_update_config_maintains_sandboxing(self):
        """Test that updating Location configuration maintains proper sandboxing."""
        # Create location
        initial_config = {
            "protocol": "file",
            "path": str(self.location_base),
            "initial_setting": "value1"
        }
        
        location = LegacyLocation(
            name="update_test",
            kinds=[LocationKind.DISK],
            config=initial_config
        )
        
        # Update configuration
        location.config["initial_setting"] = "updated_value"
        location.config["new_setting"] = "value2"
        
        # Save changes
        location._save_locations()
        
        # Reload and verify
        LegacyLocation._locations = {}
        LegacyLocation.load_locations()
        reloaded = LegacyLocation.get_location("update_test")
        
        assert reloaded.config["initial_setting"] == "updated_value"
        assert reloaded.config["new_setting"] == "value2"
        assert reloaded.config["path"] == str(self.location_base)
        
        # Verify filesystem still works correctly
        fs = reloaded.fs
        assert fs.exists("test.txt")
        assert fs.base_path.rstrip("/") == str(self.location_base)

    def test_multiple_locations_with_different_paths(self):
        """Test multiple locations with different base paths maintain separate sandboxing."""
        # Create second location directory
        location2_base = self.temp_root / "location2_data"
        location2_base.mkdir()
        (location2_base / "location2_file.txt").write_text("location2 content")
        
        # Create two locations with different paths
        loc1 = LegacyLocation(
            name="location1",
            kinds=[LocationKind.DISK],
            config={"protocol": "file", "path": str(self.location_base)}
        )
        
        loc2 = LegacyLocation(
            name="location2", 
            kinds=[LocationKind.DISK],
            config={"protocol": "file", "path": str(location2_base)}
        )
        
        # Verify each location sees only its own files
        fs1 = loc1.fs
        fs2 = loc2.fs
        
        assert fs1.exists("test.txt")
        assert not fs1.exists("location2_file.txt")
        
        assert fs2.exists("location2_file.txt")
        assert not fs2.exists("test.txt")
        
        # Verify persistence maintains separate configs
        LegacyLocation._locations = {}
        LegacyLocation.load_locations()
        
        reloaded1 = LegacyLocation.get_location("location1")
        reloaded2 = LegacyLocation.get_location("location2")
        
        assert reloaded1.config["path"] == str(self.location_base)
        assert reloaded2.config["path"] == str(location2_base)
        
        # Verify sandboxing still works after reload
        assert reloaded1.fs.exists("test.txt")
        assert not reloaded1.fs.exists("location2_file.txt")
        assert reloaded2.fs.exists("location2_file.txt") 
        assert not reloaded2.fs.exists("test.txt")

    def test_location_serialization_with_complex_config(self):
        """Test Location serialization/deserialization with complex configurations."""
        complex_config = {
            "protocol": "sftp",
            "path": "/remote/path/data",
            "storage_options": {
                "host": "example.com",
                "port": 2222,
                "username": "testuser",
                "key_filename": "/path/to/key",
                "timeout": 30
            },
            "retry_attempts": 3,
            "buffer_size": 8192,
            "metadata": {
                "description": "Production data location",
                "contact": "admin@example.com",
                "created": "2024-01-01"
            }
        }
        
        original = LegacyLocation(
            name="complex_location",
            kinds=[LocationKind.COMPUTE, LocationKind.FILESERVER],
            config=complex_config,
            optional=False
        )
        
        # Test to_dict serialization
        serialized = original.to_dict()
        expected = {
            "name": "complex_location",
            "kinds": ["COMPUTE", "FILESERVER"],
            "config": complex_config,
            "optional": False
        }
        assert serialized == expected
        
        # Test persistence and reload
        LegacyLocation._locations = {}
        LegacyLocation.load_locations()
        
        reloaded = LegacyLocation.get_location("complex_location")
        assert reloaded is not None
        assert reloaded.config == complex_config
        assert reloaded.kinds == [LocationKind.COMPUTE, LocationKind.FILESERVER]
        
        # Verify filesystem configuration
        fs = reloaded.fs
        assert fs.base_path.rstrip("/") == "/remote/path/data"

    def test_backward_compatibility_with_existing_locations_json(self):
        """Test that existing locations.json files work with the sandboxed filesystem."""
        # Create a locations.json file in the legacy format
        legacy_data = {
            "legacy_location": {
                "kinds": ["DISK"],
                "config": {
                    "protocol": "file",
                    "path": str(self.location_base),
                    "storage_options": {"auto_mkdir": True}
                },
                "optional": False
            },
            "remote_location": {
                "kinds": ["COMPUTE", "FILESERVER"],
                "config": {
                    "protocol": "sftp",
                    "path": "/remote/data",
                    "storage_options": {
                        "host": "compute.example.com",
                        "username": "user"
                    }
                },
                "optional": True
            }
        }
        
        with open(self.locations_file, 'w') as f:
            json.dump(legacy_data, f, indent=2)
        
        # Load locations and verify they work
        LegacyLocation.load_locations()
        
        # Test local location
        local_loc = LegacyLocation.get_location("legacy_location")
        assert local_loc is not None
        assert isinstance(local_loc.fs, PathSandboxedFileSystem)
        assert local_loc.fs.exists("test.txt")
        
        # Test remote location (filesystem creation)
        remote_loc = LegacyLocation.get_location("remote_location")
        assert remote_loc is not None
        assert isinstance(remote_loc.fs, PathSandboxedFileSystem)
        assert remote_loc.fs.base_path.rstrip("/") == "/remote/data"


class TestNewArchitecturePersistenceIntegration:
    """Test new domain-driven architecture with PathSandboxedFileSystem integration."""
    
    def setup_method(self):
        """Set up test environment."""
        self.temp_root = Path(tempfile.mkdtemp())
        self.location_base = self.temp_root / "location_data"
        self.location_base.mkdir()
        
        # Create test repository and service
        self.repo_file = self.temp_root / "locations_new.json"
        self.repository = JsonLocationRepository(self.repo_file)
        self.service = LocationApplicationService(self.repository)
        
        # Create test data
        self.test_file = self.location_base / "test.txt"
        self.test_file.write_text("test content")

    def teardown_method(self):
        """Clean up test environment."""
        shutil.rmtree(self.temp_root, ignore_errors=True)

    def test_location_service_create_and_persist(self):
        """Test LocationApplicationService creates and persists locations correctly."""
        dto = CreateLocationDto(
            name="service_test_location",
            kinds=["DISK", "COMPUTE"],
            protocol="file",
            path=str(self.location_base),
            storage_options={"auto_mkdir": True},
            optional=False
        )
        
        # Create location through service
        result_dto = self.service.create_location(dto)
        
        # Verify DTO result
        assert result_dto.name == "service_test_location"
        assert set(result_dto.kinds) == {"DISK", "COMPUTE"}
        assert result_dto.protocol == "file"
        assert result_dto.path == str(self.location_base)
        
        # Verify persistence
        assert self.repo_file.exists()
        
        # Load directly from repository to verify
        entity = self.repository.get_by_name("service_test_location")
        assert entity is not None
        assert entity.name == "service_test_location"
        assert entity.has_kind(DomainLocationKind.DISK)
        assert entity.has_kind(DomainLocationKind.COMPUTE)
        assert entity.get_base_path() == str(self.location_base)

    def test_location_service_update_maintains_integrity(self):
        """Test LocationApplicationService updates maintain data integrity."""
        # Create initial location
        create_dto = CreateLocationDto(
            name="update_test_location",
            kinds=["DISK"],
            protocol="file",
            path=str(self.location_base),
            optional=False
        )
        self.service.create_location(create_dto)
        
        # Update location
        update_dto = UpdateLocationDto(
            kinds=["DISK", "COMPUTE"],
            path=str(self.location_base / "subpath"),
            storage_options={"buffer_size": 8192},
            optional=True
        )
        
        updated_dto = self.service.update_location("update_test_location", update_dto)
        
        # Verify update results
        assert set(updated_dto.kinds) == {"DISK", "COMPUTE"}
        assert updated_dto.path == str(self.location_base / "subpath")
        assert updated_dto.storage_options["buffer_size"] == 8192
        assert updated_dto.optional is True
        
        # Verify persistence of updates
        entity = self.repository.get_by_name("update_test_location")
        assert entity is not None
        assert entity.has_kind(DomainLocationKind.DISK)
        assert entity.has_kind(DomainLocationKind.COMPUTE)
        assert entity.get_base_path() == str(self.location_base / "subpath")
        assert entity.optional is True

    def test_repository_atomic_operations(self):
        """Test that repository operations are atomic and handle failures correctly."""
        entity = LocationEntity(
            name="atomic_test",
            kinds=[DomainLocationKind.DISK],
            config={
                "protocol": "file",
                "path": str(self.location_base),
                "test_config": "value"
            }
        )
        
        # Save entity
        self.repository.save(entity)
        
        # Verify atomic write by checking temp file doesn't exist
        temp_file = self.repo_file.with_suffix('.tmp')
        assert not temp_file.exists()
        
        # Verify actual file exists and contains correct data
        assert self.repo_file.exists()
        
        with open(self.repo_file) as f:
            data = json.load(f)
        
        assert "atomic_test" in data
        assert data["atomic_test"]["config"]["test_config"] == "value"
        
        # Test that failure during save doesn't corrupt existing data
        with patch('json.dump', side_effect=Exception("Simulated failure")):
            with pytest.raises(Exception):
                invalid_entity = LocationEntity(
                    name="invalid_entity",
                    kinds=[DomainLocationKind.TAPE],
                    config={"protocol": "file"}
                )
                self.repository.save(invalid_entity)
        
        # Verify original data is still intact
        reloaded = self.repository.get_by_name("atomic_test")
        assert reloaded is not None
        assert reloaded.config["test_config"] == "value"

    def test_repository_migration_from_legacy(self):
        """Test migration from legacy location format."""
        # Create legacy format file
        legacy_file = self.temp_root / "legacy_locations.json"
        legacy_data = {
            "migrated_location": {
                "kinds": ["DISK", "COMPUTE"],
                "config": {
                    "protocol": "file",
                    "path": str(self.location_base),
                    "legacy_setting": "legacy_value"
                },
                "optional": True
            }
        }
        
        with open(legacy_file, 'w') as f:
            json.dump(legacy_data, f)
        
        # Perform migration
        self.repository.migrate_from_legacy_format(legacy_file)
        
        # Verify migration
        migrated = self.repository.get_by_name("migrated_location")
        assert migrated is not None
        assert migrated.has_kind(DomainLocationKind.DISK)
        assert migrated.has_kind(DomainLocationKind.COMPUTE)
        assert migrated.get_base_path() == str(self.location_base)
        assert migrated.config["legacy_setting"] == "legacy_value"
        assert migrated.optional is True


class TestLocationContextIntegration:
    """Test Location integration with Simulation context and templating."""
    
    def setup_method(self):
        """Set up test environment."""
        # Clear legacy locations
        LegacyLocation._locations = {}
        
        # Create temporary directories
        self.temp_root = Path(tempfile.mkdtemp())
        self.data_root = self.temp_root / "data"
        self.data_root.mkdir()
        
        # Set up locations file
        self.locations_file = self.temp_root / "locations.json"
        LegacyLocation._locations_file = self.locations_file
        
        # Create directory structure for templating
        self.model_data = self.data_root / "model1" / "experiment1"
        self.model_data.mkdir(parents=True)
        (self.model_data / "input.nc").write_text("input data")
        (self.model_data / "output.nc").write_text("output data")

    def teardown_method(self):
        """Clean up test environment."""
        shutil.rmtree(self.temp_root, ignore_errors=True)

    def test_location_with_context_templating(self):
        """Test Location works with context-based path templating."""
        # Create location with templated path
        base_config = {
            "protocol": "file",
            "path": str(self.data_root)
        }
        
        location = LegacyLocation(
            name="templated_location",
            kinds=[LocationKind.DISK],
            config=base_config
        )
        
        # Create context with path prefix
        context = LocationContext(
            path_prefix="model1/experiment1",
            metadata={"model": "climate_model", "experiment": "rcp85"}
        )
        
        # Test that filesystem can access files via context path
        fs = location.fs
        
        # Files should be accessible via the context path prefix
        assert fs.exists("model1/experiment1/input.nc")
        assert fs.exists("model1/experiment1/output.nc")
        
        # Test reading content
        content = fs.read_text("model1/experiment1/input.nc")
        assert content == "input data"
        
        # Verify context serialization/persistence
        context_dict = context.to_dict()
        assert context_dict["path_prefix"] == "model1/experiment1"
        assert context_dict["metadata"]["model"] == "climate_model"
        
        # Test context reconstruction
        restored_context = LocationContext.from_dict(context_dict)
        assert restored_context.path_prefix == "model1/experiment1"
        assert restored_context.metadata["experiment"] == "rcp85"

    def test_location_context_with_variable_substitution(self):
        """Test Location context with template variable substitution patterns."""
        # This tests the pattern used in Simulation path templating
        base_config = {
            "protocol": "file", 
            "path": str(self.data_root)
        }
        
        location = LegacyLocation(
            name="variable_location",
            kinds=[LocationKind.COMPUTE],
            config=base_config
        )
        
        # Test with template-like paths that would be resolved by Simulation
        # (This simulates what happens after template variable resolution)
        resolved_paths = [
            "model1/experiment1/input.nc",  # Resolved from "{model_id}/{experiment_id}/input.nc"
            "model1/experiment1/output.nc"  # Resolved from "{model_id}/{experiment_id}/output.nc"
        ]
        
        fs = location.fs
        
        for resolved_path in resolved_paths:
            assert fs.exists(resolved_path)
        
        # Test that sandbox prevents access outside resolved paths
        from tellus.location.sandboxed_filesystem import PathValidationError
        with pytest.raises(PathValidationError):
            fs.read_text("../../outside.txt")

    def test_location_persistence_with_context_metadata(self):
        """Test Location persistence works correctly with context metadata."""
        # Create location
        config = {
            "protocol": "file",
            "path": str(self.data_root),
            "context_aware": True,
            "default_patterns": ["input/*.nc", "output/*.nc"]
        }
        
        location = LegacyLocation(
            name="context_aware_location",
            kinds=[LocationKind.DISK, LocationKind.COMPUTE],
            config=config
        )
        
        # Verify persistence includes all configuration
        assert self.locations_file.exists()
        
        with open(self.locations_file) as f:
            data = json.load(f)
        
        loc_data = data["context_aware_location"]
        assert loc_data["config"]["context_aware"] is True
        assert loc_data["config"]["default_patterns"] == ["input/*.nc", "output/*.nc"]
        
        # Test reload maintains full configuration
        LegacyLocation._locations = {}
        LegacyLocation.load_locations()
        
        reloaded = LegacyLocation.get_location("context_aware_location")
        assert reloaded.config["context_aware"] is True
        assert reloaded.config["default_patterns"] == ["input/*.nc", "output/*.nc"]
        
        # Verify filesystem still works with complex config
        fs = reloaded.fs
        assert fs.exists("model1/experiment1/input.nc")


class TestEndToEndLocationWorkflows:
    """Test complete end-to-end workflows with Location filesystem operations."""
    
    def setup_method(self):
        """Set up complex test scenario."""
        # Clear locations
        LegacyLocation._locations = {}
        
        # Create comprehensive test environment
        self.temp_root = Path(tempfile.mkdtemp())
        
        # Create multiple location directories
        self.input_dir = self.temp_root / "input_data"
        self.output_dir = self.temp_root / "output_data" 
        self.archive_dir = self.temp_root / "archive_data"
        self.temp_dir = self.temp_root / "temp_data"
        
        for d in [self.input_dir, self.output_dir, self.archive_dir, self.temp_dir]:
            d.mkdir(parents=True)
        
        # Set up locations file
        self.locations_file = self.temp_root / "locations.json"
        LegacyLocation._locations_file = self.locations_file
        
        # Create realistic data structure
        experiment_path = "CESM2" / Path("historical") / "r1i1p1f1"
        for base_dir in [self.input_dir, self.output_dir]:
            exp_dir = base_dir / experiment_path
            exp_dir.mkdir(parents=True)
            (exp_dir / "atm_daily.nc").write_text("atmospheric data")
            (exp_dir / "ocn_monthly.nc").write_text("ocean data")
            (exp_dir / "ice_yearly.nc").write_text("ice data")

    def teardown_method(self):
        """Clean up test environment."""
        shutil.rmtree(self.temp_root, ignore_errors=True)

    def test_complete_location_workflow_with_sandboxing(self):
        """Test complete workflow: create → persist → reload → use filesystem → validate sandboxing."""
        # Step 1: Create multiple locations with different configurations
        locations = [
            {
                "name": "input_data",
                "kinds": [LocationKind.DISK],
                "config": {
                    "protocol": "file",
                    "path": str(self.input_dir),
                    "description": "Model input data"
                }
            },
            {
                "name": "output_data", 
                "kinds": [LocationKind.DISK, LocationKind.COMPUTE],
                "config": {
                    "protocol": "file",
                    "path": str(self.output_dir),
                    "description": "Model output data"
                }
            },
            {
                "name": "archive_storage",
                "kinds": [LocationKind.TAPE, LocationKind.FILESERVER],
                "config": {
                    "protocol": "file",
                    "path": str(self.archive_dir),
                    "description": "Long-term archive storage",
                    "retention_policy": "permanent"
                }
            }
        ]
        
        created_locations = []
        for loc_config in locations:
            location = LegacyLocation(
                name=loc_config["name"],
                kinds=loc_config["kinds"],
                config=loc_config["config"]
            )
            created_locations.append(location)
        
        # Step 2: Verify persistence
        assert self.locations_file.exists()
        
        with open(self.locations_file) as f:
            persisted_data = json.load(f)
        
        assert len(persisted_data) == 3
        assert all(name in persisted_data for name in ["input_data", "output_data", "archive_storage"])
        
        # Step 3: Clear memory and reload
        LegacyLocation._locations = {}
        LegacyLocation.load_locations()
        
        # Step 4: Verify all locations were reloaded correctly
        reloaded_locations = {
            name: LegacyLocation.get_location(name) 
            for name in ["input_data", "output_data", "archive_storage"]
        }
        
        assert all(loc is not None for loc in reloaded_locations.values())
        
        # Step 5: Test filesystem operations on each location
        for name, location in reloaded_locations.items():
            fs = location.fs
            assert isinstance(fs, PathSandboxedFileSystem)
            
            # Test basic file operations within sandbox
            if name in ["input_data", "output_data"]:
                # These should have the test data
                assert fs.exists("CESM2/historical/r1i1p1f1/atm_daily.nc")
                assert fs.exists("CESM2/historical/r1i1p1f1/ocn_monthly.nc") 
                assert fs.exists("CESM2/historical/r1i1p1f1/ice_yearly.nc")
                
                # Test reading content
                content = fs.read_text("CESM2/historical/r1i1p1f1/atm_daily.nc")
                assert content == "atmospheric data"
                
                # Test directory listing
                files = fs.glob("CESM2/historical/r1i1p1f1/*.nc")
                assert len(files) == 3
                
            # Test sandboxing prevents escaping
            from tellus.location.sandboxed_filesystem import \
                PathValidationError
            with pytest.raises(PathValidationError):
                fs.read_text("../../../etc/passwd")
            
            with pytest.raises(PathValidationError):
                fs.write_text("../../malicious.txt", "bad content")
        
        # Step 6: Test cross-location operations (should be isolated)
        input_fs = reloaded_locations["input_data"].fs
        output_fs = reloaded_locations["output_data"].fs
        archive_fs = reloaded_locations["archive_storage"].fs
        
        # Each location should only see its own files
        assert input_fs.exists("CESM2/historical/r1i1p1f1/atm_daily.nc")
        assert output_fs.exists("CESM2/historical/r1i1p1f1/atm_daily.nc")
        assert not archive_fs.exists("CESM2/historical/r1i1p1f1/atm_daily.nc")  # Archive is empty
        
        # Test creating files in archive location
        archive_fs.write_text("archived_experiment.tar", "archived data")
        
        # Verify file was created in correct location
        assert (self.archive_dir / "archived_experiment.tar").exists()
        assert not (self.input_dir / "archived_experiment.tar").exists()
        assert not (self.output_dir / "archived_experiment.tar").exists()

    def test_location_filesystem_operations_with_error_recovery(self):
        """Test that Location filesystem operations handle errors gracefully."""
        # Create location
        location = LegacyLocation(
            name="error_test_location",
            kinds=[LocationKind.DISK],
            config={
                "protocol": "file",
                "path": str(self.temp_dir)
            }
        )
        
        fs = location.fs
        
        # Test error handling for non-existent files
        assert not fs.exists("nonexistent.txt")
        
        with pytest.raises(FileNotFoundError):
            fs.read_text("nonexistent.txt")
        
        # Test creating file in non-existent subdirectory
        fs.write_text("subdir/new_file.txt", "new content")
        assert (self.temp_dir / "subdir" / "new_file.txt").exists()
        
        # Test that sandboxing errors are properly raised
        from tellus.location.sandboxed_filesystem import PathValidationError
        with pytest.raises(PathValidationError):
            fs.read_text("../outside_sandbox.txt")
        
        # Verify location is still functional after errors
        fs.write_text("recovery_test.txt", "recovered")
        assert fs.exists("recovery_test.txt")
        assert fs.read_text("recovery_test.txt") == "recovered"

    def test_location_concurrent_access_safety(self):
        """Test that Location persistence handles concurrent access safely."""
        # Create initial location
        location1 = LegacyLocation(
            name="concurrent_test",
            kinds=[LocationKind.DISK],
            config={"protocol": "file", "path": str(self.temp_dir)}
        )
        
        # Simulate concurrent modification
        import threading
        import time
        
        results = []
        errors = []
        
        def create_location(name, delay=0):
            try:
                if delay:
                    time.sleep(delay)
                
                loc = LegacyLocation(
                    name=f"concurrent_{name}",
                    kinds=[LocationKind.COMPUTE],
                    config={"protocol": "file", "path": str(self.temp_dir / name)}
                )
                results.append(loc.name)
            except Exception as e:
                errors.append(str(e))
        
        # Start multiple threads creating locations
        threads = []
        for i in range(5):
            thread = threading.Thread(target=create_location, args=(f"thread_{i}", i * 0.01))
            threads.append(thread)
            thread.start()
        
        # Wait for all threads
        for thread in threads:
            thread.join()
        
        # Verify all locations were created without errors
        assert len(errors) == 0, f"Concurrent errors: {errors}"
        assert len(results) == 5
        
        # Verify all locations exist in file
        LegacyLocation._locations = {}
        LegacyLocation.load_locations()
        
        for i in range(5):
            location_name = f"concurrent_thread_{i}"
            reloaded = LegacyLocation.get_location(location_name)
            assert reloaded is not None, f"Location {location_name} not found after concurrent creation"