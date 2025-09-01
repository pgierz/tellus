"""
Tests for JsonLocationRepository.

Tests the infrastructure layer for location persistence,
including JSON file operations, thread safety, and error handling.
"""

import json
import threading
import time
from pathlib import Path
from unittest.mock import mock_open, patch

import pytest

from tellus.domain.entities.location import LocationEntity, LocationKind
from tellus.domain.repositories.exceptions import (LocationExistsError,
                                                   LocationNotFoundError,
                                                   RepositoryError)
from tellus.infrastructure.repositories.json_location_repository import \
    JsonLocationRepository


@pytest.fixture
def temp_json_file(tmp_path):
    """Create a temporary JSON file for testing."""
    return tmp_path / "test_locations.json"


@pytest.fixture
def sample_location():
    """Create a sample location for testing."""
    return LocationEntity(
        name="test-storage",
        kinds=[LocationKind.DISK, LocationKind.FILESERVER],
        config={
            "protocol": "file",
            "path": "/data/storage",
            "max_size": "100GB"
        },
        optional=False
    )


@pytest.fixture
def sample_ssh_location():
    """Create a sample SSH location for testing."""
    return LocationEntity(
        name="remote-compute",
        kinds=[LocationKind.COMPUTE],
        config={
            "protocol": "ssh",
            "storage_options": {
                "host": "cluster.example.com",
                "port": 22,
                "username": "user"
            },
            "path": "/home/user/data"
        },
        optional=True
    )


class TestJsonLocationRepository:
    """Test suite for JsonLocationRepository."""
    
    def test_initialize_creates_empty_file(self, temp_json_file):
        """Test that initializing repository creates empty JSON file."""
        repo = JsonLocationRepository(temp_json_file)
        
        assert temp_json_file.exists()
        with open(temp_json_file) as f:
            data = json.load(f)
        assert data == {}
    
    def test_initialize_creates_parent_directory(self, tmp_path):
        """Test that initializing repository creates parent directories."""
        nested_path = tmp_path / "nested" / "dir" / "locations.json"
        repo = JsonLocationRepository(nested_path)
        
        assert nested_path.exists()
        assert nested_path.parent.exists()
    
    def test_save_and_get_location(self, temp_json_file, sample_location):
        """Test saving and retrieving a location."""
        repo = JsonLocationRepository(temp_json_file)
        
        # Save location
        repo.save(sample_location)
        
        # Retrieve location
        retrieved = repo.get_by_name("test-storage")
        
        assert retrieved is not None
        assert retrieved.name == "test-storage"
        assert LocationKind.DISK in retrieved.kinds
        assert LocationKind.FILESERVER in retrieved.kinds
        assert retrieved.config["protocol"] == "file"
        assert retrieved.config["path"] == "/data/storage"
        assert retrieved.optional is False
    
    def test_save_ssh_location(self, temp_json_file, sample_ssh_location):
        """Test saving and retrieving an SSH location with complex config."""
        repo = JsonLocationRepository(temp_json_file)
        
        # Save location
        repo.save(sample_ssh_location)
        
        # Retrieve location
        retrieved = repo.get_by_name("remote-compute")
        
        assert retrieved is not None
        assert retrieved.name == "remote-compute"
        assert LocationKind.COMPUTE in retrieved.kinds
        assert retrieved.config["protocol"] == "ssh"
        assert retrieved.config["storage_options"]["host"] == "cluster.example.com"
        assert retrieved.optional is True
    
    def test_get_nonexistent_location(self, temp_json_file):
        """Test retrieving a location that doesn't exist."""
        repo = JsonLocationRepository(temp_json_file)
        
        result = repo.get_by_name("nonexistent")
        assert result is None
    
    def test_update_existing_location(self, temp_json_file, sample_location):
        """Test updating an existing location."""
        repo = JsonLocationRepository(temp_json_file)
        
        # Save original location
        repo.save(sample_location)
        
        # Modify and save again
        sample_location.config["path"] = "/updated/path"
        sample_location.optional = True
        repo.save(sample_location)
        
        # Retrieve updated location
        retrieved = repo.get_by_name("test-storage")
        assert retrieved.config["path"] == "/updated/path"
        assert retrieved.optional is True
    
    def test_list_all_locations(self, temp_json_file, sample_location, sample_ssh_location):
        """Test listing all locations."""
        repo = JsonLocationRepository(temp_json_file)
        
        # Save multiple locations
        repo.save(sample_location)
        repo.save(sample_ssh_location)
        
        # List all
        locations = repo.list_all()
        
        assert len(locations) == 2
        location_names = [loc.name for loc in locations]
        assert "test-storage" in location_names
        assert "remote-compute" in location_names
    
    def test_list_empty_repository(self, temp_json_file):
        """Test listing locations in empty repository."""
        repo = JsonLocationRepository(temp_json_file)
        
        locations = repo.list_all()
        assert locations == []
    
    def test_exists_location(self, temp_json_file, sample_location):
        """Test checking if location exists."""
        repo = JsonLocationRepository(temp_json_file)
        
        # Initially doesn't exist
        assert not repo.exists("test-storage")
        
        # Save and check again
        repo.save(sample_location)
        assert repo.exists("test-storage")
        
        # Non-existent location still doesn't exist
        assert not repo.exists("nonexistent")
    
    def test_delete_location(self, temp_json_file, sample_location):
        """Test deleting a location."""
        repo = JsonLocationRepository(temp_json_file)
        
        # Save location
        repo.save(sample_location)
        assert repo.exists("test-storage")
        
        # Delete location
        result = repo.delete("test-storage")
        assert result is True
        assert not repo.exists("test-storage")
        
        # Retrieve returns None after deletion
        retrieved = repo.get_by_name("test-storage")
        assert retrieved is None
    
    def test_delete_nonexistent_location(self, temp_json_file):
        """Test deleting a location that doesn't exist."""
        repo = JsonLocationRepository(temp_json_file)
        
        result = repo.delete("nonexistent")
        assert result is False
    
    def test_find_by_kind(self, temp_json_file, sample_location, sample_ssh_location):
        """Test finding locations by kind."""
        repo = JsonLocationRepository(temp_json_file)
        
        # Save locations with different kinds
        repo.save(sample_location)  # DISK, FILESERVER
        repo.save(sample_ssh_location)  # COMPUTE
        
        # Find by DISK kind
        disk_locations = repo.find_by_kind(LocationKind.DISK)
        assert len(disk_locations) == 1
        assert disk_locations[0].name == "test-storage"
        
        # Find by COMPUTE kind
        compute_locations = repo.find_by_kind(LocationKind.COMPUTE)
        assert len(compute_locations) == 1
        assert compute_locations[0].name == "remote-compute"
        
        # Find by TAPE kind (none exist)
        tape_locations = repo.find_by_kind(LocationKind.TAPE)
        assert len(tape_locations) == 0
    
    def test_find_by_protocol(self, temp_json_file, sample_location, sample_ssh_location):
        """Test finding locations by protocol."""
        repo = JsonLocationRepository(temp_json_file)
        
        # Save locations with different protocols
        repo.save(sample_location)  # file protocol
        repo.save(sample_ssh_location)  # ssh protocol
        
        # Find by file protocol
        file_locations = repo.find_by_protocol("file")
        assert len(file_locations) == 1
        assert file_locations[0].name == "test-storage"
        
        # Find by ssh protocol
        ssh_locations = repo.find_by_protocol("ssh")
        assert len(ssh_locations) == 1
        assert ssh_locations[0].name == "remote-compute"
        
        # Find by non-existent protocol
        s3_locations = repo.find_by_protocol("s3")
        assert len(s3_locations) == 0


class TestConcurrencyAndThreadSafety:
    """Test thread safety and concurrent operations."""
    
    def test_concurrent_save_operations(self, temp_json_file):
        """Test concurrent save operations are thread-safe."""
        repo = JsonLocationRepository(temp_json_file)
        
        def save_location(location_id):
            location = LocationEntity(
                name=f"location-{location_id}",
                kinds=[LocationKind.DISK],
                config={"protocol": "file", "path": f"/data/{location_id}"}
            )
            repo.save(location)
        
        # Create multiple threads saving different locations
        threads = []
        for i in range(10):
            thread = threading.Thread(target=save_location, args=(i,))
            threads.append(thread)
            thread.start()
        
        # Wait for all threads to complete
        for thread in threads:
            thread.join()
        
        # Verify all locations were saved
        locations = repo.list_all()
        assert len(locations) == 10
        location_names = [loc.name for loc in locations]
        for i in range(10):
            assert f"location-{i}" in location_names
    
    def test_concurrent_read_write_operations(self, temp_json_file, sample_location):
        """Test concurrent read and write operations."""
        repo = JsonLocationRepository(temp_json_file)
        
        # Save initial location
        repo.save(sample_location)
        
        results = []
        
        def read_location():
            for _ in range(100):
                location = repo.get_by_name("test-storage")
                results.append(location is not None)
                time.sleep(0.001)  # Small delay to allow interleaving
        
        def write_location():
            for i in range(50):
                sample_location.config["counter"] = i
                repo.save(sample_location)
                time.sleep(0.002)  # Small delay
        
        # Start concurrent read and write threads
        read_thread = threading.Thread(target=read_location)
        write_thread = threading.Thread(target=write_location)
        
        read_thread.start()
        write_thread.start()
        
        read_thread.join()
        write_thread.join()
        
        # All reads should have succeeded
        assert all(results)
        
        # Final location should have the last counter value
        final_location = repo.get_by_name("test-storage")
        assert final_location.config["counter"] == 49


class TestErrorHandling:
    """Test error handling scenarios."""
    
    def test_save_with_corrupted_json_file(self, temp_json_file, sample_location):
        """Test saving when JSON file is corrupted."""
        # Create corrupted JSON file
        with open(temp_json_file, 'w') as f:
            f.write('{"invalid": json content}')
        
        repo = JsonLocationRepository(temp_json_file)
        
        # Should handle corrupted JSON gracefully
        with pytest.raises(RepositoryError) as exc_info:
            repo.save(sample_location)
        
        assert "Failed to save location" in str(exc_info.value)
    
    def test_get_with_file_permission_error(self, temp_json_file, sample_location):
        """Test getting location when file permissions are denied."""
        repo = JsonLocationRepository(temp_json_file)
        repo.save(sample_location)
        
        # Mock file permission error
        with patch('builtins.open', mock_open()) as mock_file:
            mock_file.side_effect = PermissionError("Permission denied")
            
            with pytest.raises(RepositoryError) as exc_info:
                repo.get_by_name("test-storage")
            
            assert "Failed to retrieve location" in str(exc_info.value)
    
    def test_list_with_json_decode_error(self, temp_json_file):
        """Test listing locations with JSON decode error."""
        # Create invalid JSON content
        with open(temp_json_file, 'w') as f:
            f.write('invalid json')
        
        repo = JsonLocationRepository(temp_json_file)
        
        with pytest.raises(RepositoryError) as exc_info:
            repo.list_all()
        
        assert "Failed to list locations" in str(exc_info.value)
    
    def test_delete_with_file_system_error(self, temp_json_file, sample_location):
        """Test deleting location with filesystem error."""
        repo = JsonLocationRepository(temp_json_file)
        repo.save(sample_location)
        
        # Mock filesystem error during save
        with patch.object(repo, '_save_data') as mock_save:
            mock_save.side_effect = OSError("Disk full")
            
            with pytest.raises(RepositoryError) as exc_info:
                repo.delete("test-storage")
            
            assert "Failed to delete location" in str(exc_info.value)


class TestDataFormatCompatibility:
    """Test compatibility with existing data formats."""
    
    def test_load_existing_locations_json_format(self, temp_json_file):
        """Test loading locations from existing JSON format."""
        # Create JSON data in the expected format
        existing_data = {
            "local-disk": {
                "kinds": ["DISK"],
                "config": {
                    "protocol": "file",
                    "path": "/data/local"
                },
                "optional": False
            },
            "remote-server": {
                "kinds": ["COMPUTE", "FILESERVER"],
                "config": {
                    "protocol": "ssh",
                    "storage_options": {
                        "host": "server.example.com"
                    }
                },
                "optional": True
            }
        }
        
        # Write to file
        with open(temp_json_file, 'w') as f:
            json.dump(existing_data, f)
        
        # Load with repository
        repo = JsonLocationRepository(temp_json_file)
        locations = repo.list_all()
        
        assert len(locations) == 2
        
        # Check local-disk location
        local_disk = repo.get_by_name("local-disk")
        assert local_disk is not None
        assert LocationKind.DISK in local_disk.kinds
        assert local_disk.config["protocol"] == "file"
        assert local_disk.optional is False
        
        # Check remote-server location
        remote_server = repo.get_by_name("remote-server")
        assert remote_server is not None
        assert LocationKind.COMPUTE in remote_server.kinds
        assert LocationKind.FILESERVER in remote_server.kinds
        assert remote_server.config["protocol"] == "ssh"
        assert remote_server.optional is True
    
    def test_save_maintains_json_format(self, temp_json_file, sample_location):
        """Test that saving maintains the expected JSON format."""
        repo = JsonLocationRepository(temp_json_file)
        repo.save(sample_location)
        
        # Read raw JSON and verify format
        with open(temp_json_file) as f:
            data = json.load(f)
        
        assert "test-storage" in data
        location_data = data["test-storage"]
        
        # Check structure
        assert "kinds" in location_data
        assert "config" in location_data
        assert "optional" in location_data
        
        # Check values
        assert location_data["kinds"] == ["DISK", "FILESERVER"]
        assert location_data["config"]["protocol"] == "file"
        assert location_data["optional"] is False
    
    def test_round_trip_data_integrity(self, temp_json_file, sample_location, sample_ssh_location):
        """Test that save/load cycles maintain data integrity."""
        repo = JsonLocationRepository(temp_json_file)
        
        # Save multiple locations
        original_locations = [sample_location, sample_ssh_location]
        for loc in original_locations:
            repo.save(loc)
        
        # Load all locations
        loaded_locations = repo.list_all()
        
        # Sort by name for consistent comparison
        original_locations.sort(key=lambda x: x.name)
        loaded_locations.sort(key=lambda x: x.name)
        
        assert len(loaded_locations) == len(original_locations)
        
        # Compare each location
        for orig, loaded in zip(original_locations, loaded_locations):
            assert orig.name == loaded.name
            assert orig.kinds == loaded.kinds
            assert orig.config == loaded.config
            assert orig.optional == loaded.optional


class TestEdgeCases:
    """Test edge cases and boundary conditions."""
    
    def test_location_with_minimal_config(self, temp_json_file):
        """Test location with minimal required configuration."""
        location = LocationEntity(
            name="minimal-location",
            kinds=[LocationKind.DISK],
            config={"protocol": "file"},  # Minimal required config
            optional=True
        )
        
        repo = JsonLocationRepository(temp_json_file)
        repo.save(location)
        
        retrieved = repo.get_by_name("minimal-location")
        assert retrieved is not None
        assert retrieved.config == {"protocol": "file"}
    
    def test_location_with_complex_nested_config(self, temp_json_file):
        """Test location with deeply nested configuration."""
        location = LocationEntity(
            name="complex-location",
            kinds=[LocationKind.COMPUTE],
            config={
                "protocol": "s3",
                "storage_options": {
                    "credentials": {
                        "access_key": "key123",
                        "secret_key": "secret456"
                    },
                    "region": "us-west-2",
                    "bucket": "my-bucket",
                    "metadata": {
                        "tags": ["production", "backup"],
                        "policies": {
                            "retention": "90days",
                            "versioning": True
                        }
                    }
                }
            },
            optional=False
        )
        
        repo = JsonLocationRepository(temp_json_file)
        repo.save(location)
        
        retrieved = repo.get_by_name("complex-location")
        assert retrieved is not None
        assert retrieved.config["storage_options"]["credentials"]["access_key"] == "key123"
        assert retrieved.config["storage_options"]["metadata"]["policies"]["versioning"] is True
    
    def test_location_with_special_characters_in_name(self, temp_json_file):
        """Test location with special characters in name."""
        location = LocationEntity(
            name="special-chars_location.test",
            kinds=[LocationKind.DISK],
            config={"protocol": "file", "path": "/data/special"},
            optional=False
        )
        
        repo = JsonLocationRepository(temp_json_file)
        repo.save(location)
        
        retrieved = repo.get_by_name("special-chars_location.test")
        assert retrieved is not None
        assert retrieved.name == "special-chars_location.test"
    
    def test_multiple_kinds_single_location(self, temp_json_file):
        """Test location with all possible kinds."""
        location = LocationEntity(
            name="all-kinds-location",
            kinds=[LocationKind.TAPE, LocationKind.COMPUTE, LocationKind.DISK, LocationKind.FILESERVER],
            config={"protocol": "hybrid", "supports": "all"},
            optional=False
        )
        
        repo = JsonLocationRepository(temp_json_file)
        repo.save(location)
        
        retrieved = repo.get_by_name("all-kinds-location")
        assert retrieved is not None
        assert len(retrieved.kinds) == 4
        assert all(kind in retrieved.kinds for kind in LocationKind)