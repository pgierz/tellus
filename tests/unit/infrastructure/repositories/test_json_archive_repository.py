"""
Tests for JsonArchiveRepository.

Tests the infrastructure layer for archive persistence,
including JSON file operations, thread safety, error handling, and search functionality.
"""

import json
import threading
import time
from pathlib import Path
from typing import Set
from unittest.mock import mock_open, patch

import pytest

from tellus.domain.entities.archive import (ArchiveId, ArchiveMetadata,
                                            ArchiveType, Checksum)
from tellus.domain.repositories.exceptions import RepositoryError
from tellus.infrastructure.repositories.json_archive_repository import \
    JsonArchiveRepository


@pytest.fixture
def temp_json_file(tmp_path):
    """Create a temporary JSON file for testing."""
    return tmp_path / "test_archives.json"


@pytest.fixture
def sample_checksum():
    """Create a sample checksum for testing."""
    return Checksum(value="abc123def4567890123456789012345678abc123def456789012345678901234", algorithm="sha256")


@pytest.fixture
def sample_archive(sample_checksum):
    """Create a sample archive for testing."""
    return ArchiveMetadata(
        archive_id=ArchiveId("test-archive-001"),
        location="local-storage",
        archive_type=ArchiveType.COMPRESSED,
        simulation_id="test-sim-001",
        checksum=sample_checksum,
        size=1048576,  # 1MB
        created_time=1672531200,  # 2023-01-01 00:00:00 UTC
        simulation_date="2023-01-01",
        version="1.0.0",
        description="Test archive for simulation data",
        tags={"test", "compressed"}
    )


@pytest.fixture
def sample_archive_no_checksum():
    """Create a sample archive without checksum for testing."""
    return ArchiveMetadata(
        archive_id=ArchiveId("test-archive-002"),
        location="remote-storage",
        archive_type=ArchiveType.ORGANIZED,
        simulation_id="test-sim-002",
        checksum=None,
        size=2097152,  # 2MB
        created_time=1672617600,  # 2023-01-02 00:00:00 UTC
        simulation_date="2023-01-02",
        version="2.0.0",
        description="Test organized archive",
        tags={"test", "organized"}
    )


class TestJsonArchiveRepository:
    """Test suite for JsonArchiveRepository."""
    
    def test_initialize_creates_empty_file(self, temp_json_file):
        """Test that initializing repository creates empty JSON file."""
        repo = JsonArchiveRepository(temp_json_file)
        
        assert temp_json_file.exists()
        with open(temp_json_file) as f:
            data = json.load(f)
        assert data == {}
    
    def test_initialize_creates_parent_directory(self, tmp_path):
        """Test that initializing repository creates parent directories."""
        nested_path = tmp_path / "nested" / "dir" / "archives.json"
        repo = JsonArchiveRepository(nested_path)
        
        assert nested_path.exists()
        assert nested_path.parent.exists()
    
    def test_save_and_get_archive(self, temp_json_file, sample_archive):
        """Test saving and retrieving an archive."""
        repo = JsonArchiveRepository(temp_json_file)
        
        # Save archive
        repo.save(sample_archive)
        
        # Retrieve archive
        retrieved = repo.get_by_id("test-archive-001")
        
        assert retrieved is not None
        assert retrieved.archive_id.value == "test-archive-001"
        assert retrieved.location == "local-storage"
        assert retrieved.archive_type == ArchiveType.COMPRESSED
        assert retrieved.simulation_id == "test-sim-001"
        assert retrieved.checksum.value == "abc123def4567890123456789012345678abc123def456789012345678901234"
        assert retrieved.checksum.algorithm == "sha256"
        assert retrieved.size == 1048576
        assert retrieved.created_time == 1672531200
        assert retrieved.simulation_date == "2023-01-01"
        assert retrieved.version == "1.0.0"
        assert retrieved.description == "Test archive for simulation data"
        assert retrieved.tags == {"test", "compressed"}
    
    def test_save_archive_without_checksum(self, temp_json_file, sample_archive_no_checksum):
        """Test saving and retrieving an archive without checksum."""
        repo = JsonArchiveRepository(temp_json_file)
        
        # Save archive
        repo.save(sample_archive_no_checksum)
        
        # Retrieve archive
        retrieved = repo.get_by_id("test-archive-002")
        
        assert retrieved is not None
        assert retrieved.archive_id.value == "test-archive-002"
        assert retrieved.location == "remote-storage"
        assert retrieved.archive_type == ArchiveType.ORGANIZED
        assert retrieved.checksum is None
        assert retrieved.size == 2097152
        assert retrieved.tags == {"test", "organized"}
    
    def test_get_nonexistent_archive(self, temp_json_file):
        """Test retrieving an archive that doesn't exist."""
        repo = JsonArchiveRepository(temp_json_file)
        
        result = repo.get_by_id("nonexistent")
        assert result is None
    
    def test_update_existing_archive(self, temp_json_file, sample_archive):
        """Test updating an existing archive."""
        repo = JsonArchiveRepository(temp_json_file)
        
        # Save original archive
        repo.save(sample_archive)
        
        # Modify and save again
        sample_archive.description = "Updated description"
        sample_archive.version = "1.1.0"
        sample_archive.tags.add("updated")
        repo.save(sample_archive)
        
        # Retrieve updated archive
        retrieved = repo.get_by_id("test-archive-001")
        assert retrieved.description == "Updated description"
        assert retrieved.version == "1.1.0"
        assert "updated" in retrieved.tags
    
    def test_list_all_archives(self, temp_json_file, sample_archive, sample_archive_no_checksum):
        """Test listing all archives."""
        repo = JsonArchiveRepository(temp_json_file)
        
        # Save multiple archives
        repo.save(sample_archive)
        repo.save(sample_archive_no_checksum)
        
        # List all
        archives = repo.list_all()
        
        assert len(archives) == 2
        archive_ids = [archive.archive_id.value for archive in archives]
        assert "test-archive-001" in archive_ids
        assert "test-archive-002" in archive_ids
    
    def test_list_empty_repository(self, temp_json_file):
        """Test listing archives in empty repository."""
        repo = JsonArchiveRepository(temp_json_file)
        
        archives = repo.list_all()
        assert archives == []
    
    def test_list_by_simulation(self, temp_json_file, sample_archive, sample_archive_no_checksum):
        """Test listing archives by simulation ID."""
        repo = JsonArchiveRepository(temp_json_file)
        
        # Save archives with different simulation IDs
        repo.save(sample_archive)  # test-sim-001
        repo.save(sample_archive_no_checksum)  # test-sim-002
        
        # Create another archive for same simulation
        archive3 = ArchiveMetadata(
            archive_id=ArchiveId("test-archive-003"),
            location="local-storage",
            archive_type=ArchiveType.COMPRESSED,
            simulation_id="test-sim-001",  # Same as first archive
            size=512000,
            tags=set()
        )
        repo.save(archive3)
        
        # List by simulation
        sim1_archives = repo.list_by_simulation("test-sim-001")
        sim2_archives = repo.list_by_simulation("test-sim-002")
        
        assert len(sim1_archives) == 2
        assert len(sim2_archives) == 1
        
        sim1_ids = [archive.archive_id.value for archive in sim1_archives]
        assert "test-archive-001" in sim1_ids
        assert "test-archive-003" in sim1_ids
        
        assert sim2_archives[0].archive_id.value == "test-archive-002"
    
    def test_exists_archive(self, temp_json_file, sample_archive):
        """Test checking if archive exists."""
        repo = JsonArchiveRepository(temp_json_file)
        
        # Initially doesn't exist
        assert not repo.exists("test-archive-001")
        
        # Save and check again
        repo.save(sample_archive)
        assert repo.exists("test-archive-001")
        
        # Non-existent archive still doesn't exist
        assert not repo.exists("nonexistent")
    
    def test_delete_archive(self, temp_json_file, sample_archive):
        """Test deleting an archive."""
        repo = JsonArchiveRepository(temp_json_file)
        
        # Save archive
        repo.save(sample_archive)
        assert repo.exists("test-archive-001")
        
        # Delete archive
        result = repo.delete("test-archive-001")
        assert result is True
        assert not repo.exists("test-archive-001")
        
        # Retrieve returns None after deletion
        retrieved = repo.get_by_id("test-archive-001")
        assert retrieved is None
    
    def test_delete_nonexistent_archive(self, temp_json_file):
        """Test deleting an archive that doesn't exist."""
        repo = JsonArchiveRepository(temp_json_file)
        
        result = repo.delete("nonexistent")
        assert result is False
    
    def test_find_by_tags_any_match(self, temp_json_file):
        """Test finding archives by tags (any match)."""
        repo = JsonArchiveRepository(temp_json_file)
        
        # Create archives with different tags
        archive1 = ArchiveMetadata(
            archive_id=ArchiveId("archive-1"),
            location="storage1",
            archive_type=ArchiveType.COMPRESSED,
            tags={"data", "simulation", "climate"}
        )
        
        archive2 = ArchiveMetadata(
            archive_id=ArchiveId("archive-2"),
            location="storage2", 
            archive_type=ArchiveType.ORGANIZED,
            tags={"output", "results", "analysis"}
        )
        
        archive3 = ArchiveMetadata(
            archive_id=ArchiveId("archive-3"),
            location="storage3",
            archive_type=ArchiveType.COMPRESSED,
            tags={"data", "backup", "processed"}
        )
        
        repo.save(archive1)
        repo.save(archive2)
        repo.save(archive3)
        
        # Find by single tag
        data_archives = repo.find_by_tags({"data"}, match_all=False)
        assert len(data_archives) == 2
        data_ids = [archive.archive_id.value for archive in data_archives]
        assert "archive-1" in data_ids
        assert "archive-3" in data_ids
        
        # Find by multiple tags (any match)
        analysis_or_climate = repo.find_by_tags({"analysis", "climate"}, match_all=False)
        assert len(analysis_or_climate) == 2
        
        # Find by non-existent tag
        none_found = repo.find_by_tags({"nonexistent"}, match_all=False)
        assert len(none_found) == 0
    
    def test_find_by_tags_all_match(self, temp_json_file):
        """Test finding archives by tags (all must match)."""
        repo = JsonArchiveRepository(temp_json_file)
        
        # Create archives with different tag combinations
        archive1 = ArchiveMetadata(
            archive_id=ArchiveId("archive-1"),
            location="storage1",
            archive_type=ArchiveType.COMPRESSED,
            tags={"data", "simulation", "climate", "processed"}
        )
        
        archive2 = ArchiveMetadata(
            archive_id=ArchiveId("archive-2"),
            location="storage2",
            archive_type=ArchiveType.ORGANIZED,
            tags={"data", "simulation", "raw"}
        )
        
        archive3 = ArchiveMetadata(
            archive_id=ArchiveId("archive-3"),
            location="storage3",
            archive_type=ArchiveType.COMPRESSED,
            tags={"data", "backup"}
        )
        
        repo.save(archive1)
        repo.save(archive2)
        repo.save(archive3)
        
        # Find archives with both "data" and "simulation" tags
        data_and_simulation = repo.find_by_tags({"data", "simulation"}, match_all=True)
        assert len(data_and_simulation) == 2
        found_ids = [archive.archive_id.value for archive in data_and_simulation]
        assert "archive-1" in found_ids
        assert "archive-2" in found_ids
        
        # Find archives with all three tags
        all_three = repo.find_by_tags({"data", "simulation", "climate"}, match_all=True)
        assert len(all_three) == 1
        assert all_three[0].archive_id.value == "archive-1"
        
        # Find archives with tags that don't all exist
        impossible = repo.find_by_tags({"data", "nonexistent"}, match_all=True)
        assert len(impossible) == 0


class TestThreadSafetyAndConcurrency:
    """Test thread safety and concurrent operations."""
    
    def test_concurrent_save_operations(self, temp_json_file):
        """Test concurrent save operations are thread-safe."""
        repo = JsonArchiveRepository(temp_json_file)
        
        def save_archive(archive_id):
            archive = ArchiveMetadata(
                archive_id=ArchiveId(f"archive-{archive_id}"),
                location=f"storage-{archive_id}",
                archive_type=ArchiveType.COMPRESSED,
                simulation_id=f"sim-{archive_id}",
                size=1024 * archive_id,
                tags={f"tag{archive_id}"}
            )
            repo.save(archive)
        
        # Create multiple threads saving different archives
        threads = []
        for i in range(10):
            thread = threading.Thread(target=save_archive, args=(i,))
            threads.append(thread)
            thread.start()
        
        # Wait for all threads to complete
        for thread in threads:
            thread.join()
        
        # Verify all archives were saved
        archives = repo.list_all()
        assert len(archives) == 10
        archive_ids = [archive.archive_id.value for archive in archives]
        for i in range(10):
            assert f"archive-{i}" in archive_ids
    
    def test_concurrent_read_write_operations(self, temp_json_file, sample_archive):
        """Test concurrent read and write operations."""
        repo = JsonArchiveRepository(temp_json_file)
        
        # Save initial archive
        repo.save(sample_archive)
        
        results = []
        
        def read_archive():
            for _ in range(100):
                archive = repo.get_by_id("test-archive-001")
                results.append(archive is not None)
                time.sleep(0.001)  # Small delay to allow interleaving
        
        def write_archive():
            for i in range(50):
                sample_archive.description = f"Updated description {i}"
                repo.save(sample_archive)
                time.sleep(0.002)  # Small delay
        
        # Start concurrent read and write threads
        read_thread = threading.Thread(target=read_archive)
        write_thread = threading.Thread(target=write_archive)
        
        read_thread.start()
        write_thread.start()
        
        read_thread.join()
        write_thread.join()
        
        # All reads should have succeeded
        assert all(results)
        
        # Final archive should have the last description
        final_archive = repo.get_by_id("test-archive-001")
        assert final_archive.description == "Updated description 49"


class TestErrorHandling:
    """Test error handling scenarios."""
    
    def test_save_with_corrupted_json_file(self, temp_json_file, sample_archive):
        """Test saving when JSON file is corrupted."""
        # Create corrupted JSON file
        with open(temp_json_file, 'w') as f:
            f.write('{"invalid": json content}')
        
        repo = JsonArchiveRepository(temp_json_file)
        
        # Should handle corrupted JSON gracefully
        with pytest.raises(RepositoryError) as exc_info:
            repo.save(sample_archive)
        
        assert "Failed to save archive" in str(exc_info.value)
    
    def test_get_with_file_permission_error(self, temp_json_file, sample_archive):
        """Test getting archive when file permissions are denied."""
        repo = JsonArchiveRepository(temp_json_file)
        repo.save(sample_archive)
        
        # Mock file permission error
        with patch('builtins.open', mock_open()) as mock_file:
            mock_file.side_effect = PermissionError("Permission denied")
            
            with pytest.raises(RepositoryError) as exc_info:
                repo.get_by_id("test-archive-001")
            
            assert "Failed to retrieve archive" in str(exc_info.value)
    
    def test_list_with_json_decode_error(self, temp_json_file):
        """Test listing archives with JSON decode error."""
        # Create invalid JSON content
        with open(temp_json_file, 'w') as f:
            f.write('invalid json')
        
        repo = JsonArchiveRepository(temp_json_file)
        
        with pytest.raises(RepositoryError) as exc_info:
            repo.list_all()
        
        assert "Failed to list archives" in str(exc_info.value)
    
    def test_invalid_archive_type_in_data(self, temp_json_file):
        """Test handling invalid archive type in JSON data."""
        # Create JSON with invalid archive type
        invalid_data = {
            "test-archive": {
                "archive_id": "test-archive",
                "location": "storage",
                "archive_type": "INVALID_TYPE",
                "simulation_id": "test-sim"
            }
        }
        
        with open(temp_json_file, 'w') as f:
            json.dump(invalid_data, f)
        
        repo = JsonArchiveRepository(temp_json_file)
        
        with pytest.raises(RepositoryError) as exc_info:
            repo.get_by_id("test-archive")
        
        assert "Failed to retrieve archive" in str(exc_info.value)


class TestDataFormatCompatibility:
    """Test compatibility with existing data formats."""
    
    def test_load_existing_archives_json_format(self, temp_json_file):
        """Test loading archives from existing JSON format."""
        # Create JSON data in the expected format
        existing_data = {
            "archive-1": {
                "archive_id": "archive-1",
                "location": "local-disk",
                "archive_type": "compressed",
                "simulation_id": "sim-001",
                "checksum": "sha256:abcdef123456789012345678901234567890abcdef1234567890123456789012",
                "size": 1048576,
                "created_time": 1672531200,
                "simulation_date": "2023-01-01",
                "version": "1.0.0",
                "description": "Test archive",
                "tags": ["test", "compressed"]
            },
            "archive-2": {
                "archive_id": "archive-2",
                "location": "remote-server",
                "archive_type": "organized",
                "simulation_id": "sim-002",
                "checksum": None,
                "size": 2097152,
                "created_time": 1672617600,
                "simulation_date": "2023-01-02",
                "version": "2.0.0",
                "description": "Test organized archive",
                "tags": ["test", "organized"]
            }
        }
        
        # Write to file
        with open(temp_json_file, 'w') as f:
            json.dump(existing_data, f)
        
        # Load with repository
        repo = JsonArchiveRepository(temp_json_file)
        archives = repo.list_all()
        
        assert len(archives) == 2
        
        # Check first archive
        archive1 = repo.get_by_id("archive-1")
        assert archive1 is not None
        assert archive1.archive_type == ArchiveType.COMPRESSED
        assert archive1.checksum.value == "abcdef123456789012345678901234567890abcdef1234567890123456789012"
        assert archive1.checksum.algorithm == "sha256"
        assert archive1.size == 1048576
        
        # Check second archive
        archive2 = repo.get_by_id("archive-2")
        assert archive2 is not None
        assert archive2.archive_type == ArchiveType.ORGANIZED
        assert archive2.checksum is None
        assert archive2.size == 2097152
    
    def test_legacy_checksum_format(self, temp_json_file):
        """Test handling legacy checksum format without algorithm prefix."""
        # Create archive with legacy checksum format
        legacy_data = {
            "legacy-archive": {
                "archive_id": "legacy-archive",
                "location": "legacy-storage",
                "archive_type": "compressed",
                "checksum": "abc123def45678901234567890123456",  # No algorithm prefix - 32 chars for MD5
                "size": 1024
            }
        }
        
        with open(temp_json_file, 'w') as f:
            json.dump(legacy_data, f)
        
        repo = JsonArchiveRepository(temp_json_file)
        archive = repo.get_by_id("legacy-archive")
        
        assert archive is not None
        assert archive.checksum.value == "abc123def45678901234567890123456"
        assert archive.checksum.algorithm == "md5"  # Default for legacy format
    
    def test_save_maintains_json_format(self, temp_json_file, sample_archive):
        """Test that saving maintains the expected JSON format."""
        repo = JsonArchiveRepository(temp_json_file)
        repo.save(sample_archive)
        
        # Read raw JSON and verify format
        with open(temp_json_file) as f:
            data = json.load(f)
        
        assert "test-archive-001" in data
        archive_data = data["test-archive-001"]
        
        # Check structure
        assert "archive_id" in archive_data
        assert "location" in archive_data
        assert "archive_type" in archive_data
        assert "simulation_id" in archive_data
        assert "checksum" in archive_data
        assert "size" in archive_data
        assert "tags" in archive_data
        
        # Check values
        assert archive_data["archive_id"] == "test-archive-001"
        assert archive_data["location"] == "local-storage"
        assert archive_data["archive_type"] == "compressed"
        assert archive_data["simulation_id"] == "test-sim-001"
        assert archive_data["checksum"] == "sha256:abc123def4567890123456789012345678abc123def456789012345678901234"
        assert archive_data["size"] == 1048576
        assert set(archive_data["tags"]) == {"test", "compressed"}
    
    def test_round_trip_data_integrity(self, temp_json_file, sample_archive, sample_archive_no_checksum):
        """Test that save/load cycles maintain data integrity."""
        repo = JsonArchiveRepository(temp_json_file)
        
        # Save multiple archives
        original_archives = [sample_archive, sample_archive_no_checksum]
        for archive in original_archives:
            repo.save(archive)
        
        # Load all archives
        loaded_archives = repo.list_all()
        
        # Sort by ID for consistent comparison
        original_archives.sort(key=lambda x: x.archive_id.value)
        loaded_archives.sort(key=lambda x: x.archive_id.value)
        
        assert len(loaded_archives) == len(original_archives)
        
        # Compare each archive
        for orig, loaded in zip(original_archives, loaded_archives):
            assert orig.archive_id.value == loaded.archive_id.value
            assert orig.location == loaded.location
            assert orig.archive_type == loaded.archive_type
            assert orig.simulation_id == loaded.simulation_id
            assert orig.size == loaded.size
            assert orig.created_time == loaded.created_time
            assert orig.simulation_date == loaded.simulation_date
            assert orig.version == loaded.version
            assert orig.description == loaded.description
            assert orig.tags == loaded.tags
            
            # Handle checksum comparison
            # Handle checksum comparison  
            if orig.checksum is None:
                assert loaded.checksum is None
            else:
                assert orig.checksum.value == loaded.checksum.value
                assert orig.checksum.algorithm == loaded.checksum.algorithm


class TestBackupAndRestore:
    """Test backup and restore functionality."""
    
    def test_backup_data_success(self, temp_json_file, tmp_path, sample_archive):
        """Test successful data backup."""
        repo = JsonArchiveRepository(temp_json_file)
        repo.save(sample_archive)
        
        backup_path = tmp_path / "backup.json"
        
        # Act
        repo.backup_data(backup_path)
        
        # Assert
        assert backup_path.exists()
        
        with open(backup_path) as f:
            backup_data = json.load(f)
        
        assert "test-archive-001" in backup_data
        assert backup_data["test-archive-001"]["archive_id"] == "test-archive-001"
    
    def test_restore_from_backup_success(self, temp_json_file, tmp_path, sample_archive):
        """Test successful restore from backup."""
        repo = JsonArchiveRepository(temp_json_file)
        
        # Create backup data
        backup_path = tmp_path / "backup.json"
        backup_data = {
            "restored-archive": {
                "archive_id": "restored-archive",
                "location": "restored-storage",
                "archive_type": "compressed",
                "simulation_id": "restored-sim",
                "checksum": "sha256:restored123456789012345678901234567890abcdef12345678901234567890",
                "size": 2048,
                "created_time": 1672617600,
                "simulation_date": "2023-01-02",
                "version": "2.0.0",
                "description": "Restored archive",
                "tags": ["restored", "backup"]
            }
        }
        
        with open(backup_path, 'w') as f:
            json.dump(backup_data, f)
        
        # Act
        repo.restore_from_backup(backup_path)
        
        # Assert
        restored = repo.get_by_id("restored-archive")
        assert restored is not None
        assert restored.location == "restored-storage"
        assert restored.description == "Restored archive"
        assert "restored" in restored.tags
    
    def test_restore_from_backup_validation_failure(self, temp_json_file, tmp_path):
        """Test restore failure due to validation errors."""
        repo = JsonArchiveRepository(temp_json_file)
        
        # Create invalid backup data
        backup_path = tmp_path / "invalid_backup.json"
        invalid_backup_data = {
            "invalid-archive": {
                "archive_id": "invalid-archive",
                # Missing required fields
                "archive_type": "INVALID_TYPE"
            }
        }
        
        with open(backup_path, 'w') as f:
            json.dump(invalid_backup_data, f)
        
        # Act & Assert
        with pytest.raises(RepositoryError) as exc_info:
            repo.restore_from_backup(backup_path)
        
        assert "Backup validation failed" in str(exc_info.value)
    
    def test_restore_from_nonexistent_backup(self, temp_json_file, tmp_path):
        """Test restore failure when backup file doesn't exist."""
        repo = JsonArchiveRepository(temp_json_file)
        
        nonexistent_path = tmp_path / "nonexistent.json"
        
        with pytest.raises(RepositoryError) as exc_info:
            repo.restore_from_backup(nonexistent_path)
        
        assert "Backup file not found" in str(exc_info.value)


class TestEdgeCases:
    """Test edge cases and boundary conditions."""
    
    def test_archive_with_empty_tags(self, temp_json_file):
        """Test archive with empty tags set."""
        repo = JsonArchiveRepository(temp_json_file)
        
        archive = ArchiveMetadata(
            archive_id=ArchiveId("no-tags-archive"),
            location="storage",
            archive_type=ArchiveType.COMPRESSED,
            tags=set()  # Empty tags
        )
        
        repo.save(archive)
        retrieved = repo.get_by_id("no-tags-archive")
        
        assert retrieved is not None
        assert retrieved.tags == set()
    
    def test_archive_with_large_tag_set(self, temp_json_file):
        """Test archive with large number of tags."""
        repo = JsonArchiveRepository(temp_json_file)
        
        # Create archive with many tags
        large_tag_set = {f"tag{i}" for i in range(100)}
        archive = ArchiveMetadata(
            archive_id=ArchiveId("many-tags-archive"),
            location="storage",
            archive_type=ArchiveType.ORGANIZED,
            tags=large_tag_set
        )
        
        repo.save(archive)
        retrieved = repo.get_by_id("many-tags-archive")
        
        assert retrieved is not None
        assert retrieved.tags == large_tag_set
    
    def test_archive_with_special_characters(self, temp_json_file):
        """Test archive with special characters in fields."""
        repo = JsonArchiveRepository(temp_json_file)
        
        archive = ArchiveMetadata(
            archive_id=ArchiveId("special-chars-archive"),
            location="storage/with spaces/and-dashes",
            archive_type=ArchiveType.COMPRESSED,
            simulation_id="sim_with_underscores",
            description="Description with émojis and ñ special chars 🚀",
            tags={"tag-with-dash", "tag_with_underscore", "tag with spaces"}
        )
        
        repo.save(archive)
        retrieved = repo.get_by_id("special-chars-archive")
        
        assert retrieved is not None
        assert retrieved.location == "storage/with spaces/and-dashes"
        assert retrieved.description == "Description with émojis and ñ special chars 🚀"
        assert "tag with spaces" in retrieved.tags
    
    def test_archive_with_very_large_size(self, temp_json_file):
        """Test archive with very large size value."""
        repo = JsonArchiveRepository(temp_json_file)
        
        # Test with size close to max int64
        large_size = 9223372036854775807  # Max int64
        archive = ArchiveMetadata(
            archive_id=ArchiveId("large-size-archive"),
            location="storage",
            archive_type=ArchiveType.ORGANIZED,
            size=large_size
        )
        
        repo.save(archive)
        retrieved = repo.get_by_id("large-size-archive")
        
        assert retrieved is not None
        assert retrieved.size == large_size