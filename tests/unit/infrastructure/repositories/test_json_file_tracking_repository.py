"""
Tests for JsonFileTrackingRepository.

Tests the infrastructure layer for file tracking repository operations,
including repository creation, state management, snapshot handling, and JSON persistence.
"""

import json
import uuid
from datetime import datetime
from pathlib import Path
from typing import Dict, List
from unittest.mock import mock_open, patch

import pytest

from tellus.domain.entities.file_tracking import (DVCConfiguration, FileChange,
                                                  FileChangeType, FileHash,
                                                  FileTrackingRepository,
                                                  RepositorySnapshot,
                                                  RepositoryState,
                                                  TrackedFileMetadata,
                                                  TrackingStatus)
from tellus.domain.repositories.exceptions import RepositoryError
from tellus.infrastructure.repositories.json_file_tracking_repository import \
    JsonFileTrackingRepository


@pytest.fixture
def temp_repo_path(tmp_path):
    """Create a temporary repository path for testing."""
    return tmp_path / "test_repo"


@pytest.fixture
def sample_file_hash():
    """Create a sample file hash for testing."""
    return FileHash(algorithm="sha256", value="abc123def456")


@pytest.fixture
def sample_tracked_file(sample_file_hash):
    """Create a sample tracked file metadata for testing."""
    return TrackedFileMetadata(
        path="data/sample.nc",
        size=1024,
        modification_time=datetime(2023, 1, 1, 12, 0, 0),
        content_hash=sample_file_hash,
        status=TrackingStatus.TRACKED,
        stage_hash=sample_file_hash,
        created_time=datetime(2023, 1, 1, 10, 0, 0)
    )


@pytest.fixture
def sample_file_change(sample_file_hash):
    """Create a sample file change for testing."""
    return FileChange(
        file_path="data/sample.nc",
        change_type=FileChangeType.MODIFIED,
        old_hash=sample_file_hash,
        new_hash=FileHash(algorithm="sha256", value="def456ghi789"),
        old_path=None,
        timestamp=datetime(2023, 1, 1, 15, 0, 0)
    )


@pytest.fixture
def sample_dvc_config():
    """Create a sample DVC configuration for testing."""
    return DVCConfiguration(
        enabled=True,
        remote_name="origin",
        remote_url="s3://my-bucket/dvc-cache",
        cache_dir=".dvc/cache",
        large_file_threshold=100 * 1024 * 1024  # 100MB
    )


@pytest.fixture
def sample_repository_state(sample_tracked_file):
    """Create a sample repository state for testing."""
    state = RepositoryState()
    state.tracked_files["data/sample.nc"] = sample_tracked_file
    state.ignore_patterns.extend(["*.tmp", "__pycache__/"])
    return state


@pytest.fixture
def sample_file_tracking_repo(temp_repo_path, sample_repository_state, sample_dvc_config):
    """Create a sample file tracking repository for testing."""
    return FileTrackingRepository(
        root_path=str(temp_repo_path),
        state=sample_repository_state,
        dvc_config=sample_dvc_config,
        snapshots=[]
    )


class TestJsonFileTrackingRepository:
    """Test suite for JsonFileTrackingRepository."""
    
    def test_create_repository_success(self, temp_repo_path):
        """Test successful repository creation."""
        repo = JsonFileTrackingRepository()
        
        # Ensure parent directory exists
        temp_repo_path.mkdir(parents=True, exist_ok=True)
        
        # Act
        result = repo.create_repository(str(temp_repo_path))
        
        # Assert
        assert isinstance(result, FileTrackingRepository)
        assert result.root_path == str(temp_repo_path.resolve())
        assert isinstance(result.state, RepositoryState)
        assert isinstance(result.dvc_config, DVCConfiguration)
        assert result.snapshots == []
        
        # Check directory structure
        tellus_dir = temp_repo_path / ".tellus"
        assert tellus_dir.exists()
        assert (tellus_dir / "data").exists()
        assert (tellus_dir / "snapshots").exists()
    
    def test_create_repository_creates_directories(self, temp_repo_path):
        """Test that repository creation creates necessary directories."""
        repo = JsonFileTrackingRepository()
        
        # Ensure directory doesn't exist initially, but create the root directory
        assert not temp_repo_path.exists()
        temp_repo_path.mkdir(parents=True, exist_ok=True)
        
        # Act
        repo.create_repository(str(temp_repo_path))
        
        # Assert
        tellus_dir = temp_repo_path / ".tellus"
        assert tellus_dir.exists()
        assert tellus_dir.is_dir()
        assert (tellus_dir / "data").exists()
        assert (tellus_dir / "snapshots").exists()
    
    def test_get_repository_success(self, temp_repo_path, sample_file_tracking_repo):
        """Test successful repository retrieval."""
        repo = JsonFileTrackingRepository()
        
        # Ensure parent directory exists
        temp_repo_path.mkdir(parents=True, exist_ok=True)
        
        # Save repository first
        repo.save_repository_state(sample_file_tracking_repo)
        
        # Act
        result = repo.get_repository(str(temp_repo_path))
        
        # Assert
        assert result is not None
        assert result.root_path == str(temp_repo_path.resolve())
        assert len(result.state.tracked_files) == 1
        assert "data/sample.nc" in result.state.tracked_files
        assert "*.tmp" in result.state.ignore_patterns
    
    def test_get_repository_not_exists(self, temp_repo_path):
        """Test retrieving repository that doesn't exist."""
        repo = JsonFileTrackingRepository()
        
        # Act
        result = repo.get_repository(str(temp_repo_path))
        
        # Assert
        assert result is None
    
    def test_save_repository_state_success(self, temp_repo_path, sample_file_tracking_repo):
        """Test successful repository state saving."""
        repo = JsonFileTrackingRepository()
        
        # Ensure parent directory exists
        temp_repo_path.mkdir(parents=True, exist_ok=True)
        
        # Act
        repo.save_repository_state(sample_file_tracking_repo)
        
        # Assert files exist
        tellus_dir = temp_repo_path / ".tellus"
        assert (tellus_dir / "tracked_files.json").exists()
        assert (tellus_dir / "ignore_patterns.json").exists()
        assert (tellus_dir / "dvc_config.json").exists()
        
        # Check tracked files content
        with open(tellus_dir / "tracked_files.json") as f:
            tracked_data = json.load(f)
        
        assert "data/sample.nc" in tracked_data
        file_data = tracked_data["data/sample.nc"]
        assert file_data["path"] == "data/sample.nc"
        assert file_data["size"] == 1024
        assert file_data["status"] == "tracked"
        assert file_data["content_hash"]["algorithm"] == "sha256"
        assert file_data["content_hash"]["value"] == "abc123def456"
    
    def test_load_repository_state_success(self, temp_repo_path, sample_file_tracking_repo):
        """Test successful repository state loading."""
        repo = JsonFileTrackingRepository()
        
        # Ensure parent directory exists
        temp_repo_path.mkdir(parents=True, exist_ok=True)
        
        # Save first
        repo.save_repository_state(sample_file_tracking_repo)
        
        # Act
        result = repo.load_repository_state(str(temp_repo_path))
        
        # Assert
        assert result is not None
        assert len(result.tracked_files) == 1
        assert "data/sample.nc" in result.tracked_files
        
        tracked_file = result.tracked_files["data/sample.nc"]
        assert tracked_file.path == "data/sample.nc"
        assert tracked_file.size == 1024
        assert tracked_file.status == TrackingStatus.TRACKED
        assert tracked_file.content_hash.algorithm == "sha256"
        assert tracked_file.content_hash.value == "abc123def456"
    
    def test_load_repository_state_not_exists(self, temp_repo_path):
        """Test loading repository state when directory doesn't exist."""
        repo = JsonFileTrackingRepository()
        
        # Act
        result = repo.load_repository_state(str(temp_repo_path))
        
        # Assert
        assert result is None
    
    def test_create_snapshot_success(self, temp_repo_path, sample_file_tracking_repo, sample_file_change):
        """Test successful snapshot creation."""
        repo = JsonFileTrackingRepository()
        
        # Ensure parent directory exists
        temp_repo_path.mkdir(parents=True, exist_ok=True)
        
        # Create repository first
        repo.save_repository_state(sample_file_tracking_repo)
        
        # Ensure snapshots directory exists
        (temp_repo_path / ".tellus" / "snapshots").mkdir(parents=True, exist_ok=True)
        
        # Act
        changes = [sample_file_change]
        result = repo.create_snapshot(
            sample_file_tracking_repo,
            "Test commit message",
            "Test Author <test@example.com>",
            changes
        )
        
        # Assert
        assert isinstance(result, RepositorySnapshot)
        assert result.message == "Test commit message"
        assert result.author == "Test Author <test@example.com>"
        assert len(result.changes) == 1
        assert result.changes[0].file_path == "data/sample.nc"
        assert result.changes[0].change_type == FileChangeType.MODIFIED
        assert result.parent_id is None  # No previous snapshots
        
        # Check snapshot file exists
        snapshot_file = temp_repo_path / ".tellus" / "snapshots" / f"{result.id}.json"
        assert snapshot_file.exists()
        
        # Verify snapshot was added to repository
        assert len(sample_file_tracking_repo.snapshots) == 1
        assert sample_file_tracking_repo.snapshots[0] == result
    
    def test_create_snapshot_with_parent(self, temp_repo_path, sample_file_tracking_repo, sample_file_change):
        """Test snapshot creation with parent snapshot."""
        repo = JsonFileTrackingRepository()
        
        # Ensure parent directory exists
        temp_repo_path.mkdir(parents=True, exist_ok=True)
        
        # Ensure snapshots directory exists
        (temp_repo_path / ".tellus" / "snapshots").mkdir(parents=True, exist_ok=True)
        
        # Create first snapshot
        first_changes = [sample_file_change]
        first_snapshot = repo.create_snapshot(
            sample_file_tracking_repo,
            "First commit",
            "Test Author",
            first_changes
        )
        
        # Create second snapshot
        second_change = FileChange(
            file_path="data/another.nc",
            change_type=FileChangeType.ADDED,
            old_hash=None,
            new_hash=FileHash("sha256", "new123hash"),
            timestamp=datetime(2023, 1, 2, 10, 0, 0)
        )
        
        # Act
        second_snapshot = repo.create_snapshot(
            sample_file_tracking_repo,
            "Second commit",
            "Test Author",
            [second_change]
        )
        
        # Assert
        assert second_snapshot.parent_id == first_snapshot.id
        assert len(sample_file_tracking_repo.snapshots) == 2
    
    def test_get_snapshots_success(self, temp_repo_path, sample_file_tracking_repo, sample_file_change):
        """Test successful snapshot retrieval."""
        repo = JsonFileTrackingRepository()
        
        # Ensure parent directory exists
        temp_repo_path.mkdir(parents=True, exist_ok=True)
        
        # Ensure snapshots directory exists
        (temp_repo_path / ".tellus" / "snapshots").mkdir(parents=True, exist_ok=True)
        
        # Create snapshots
        changes = [sample_file_change]
        repo.create_snapshot(sample_file_tracking_repo, "Test commit", "Test Author", changes)
        
        # Act
        result = repo.get_snapshots(sample_file_tracking_repo)
        
        # Assert
        assert len(result) == 1
        assert result[0].message == "Test commit"
        assert result[0].author == "Test Author"
    
    def test_get_snapshot_by_id_success(self, temp_repo_path, sample_file_tracking_repo, sample_file_change):
        """Test successful snapshot retrieval by ID."""
        repo = JsonFileTrackingRepository()
        
        # Ensure parent directory exists
        temp_repo_path.mkdir(parents=True, exist_ok=True)
        
        # Ensure snapshots directory exists
        (temp_repo_path / ".tellus" / "snapshots").mkdir(parents=True, exist_ok=True)
        
        # Create snapshot
        changes = [sample_file_change]
        snapshot = repo.create_snapshot(sample_file_tracking_repo, "Test commit", "Test Author", changes)
        
        # Act
        result = repo.get_snapshot(sample_file_tracking_repo, snapshot.id)
        
        # Assert
        assert result is not None
        assert result.id == snapshot.id
        assert result.message == "Test commit"
    
    def test_get_snapshot_by_id_not_found(self, sample_file_tracking_repo):
        """Test snapshot retrieval with non-existent ID."""
        repo = JsonFileTrackingRepository()
        
        # Act
        result = repo.get_snapshot(sample_file_tracking_repo, "nonexistent-id")
        
        # Assert
        assert result is None


class TestSnapshotPersistence:
    """Test snapshot persistence and loading."""
    
    def test_load_snapshots_success(self, temp_repo_path):
        """Test successful snapshot loading from disk."""
        repo = JsonFileTrackingRepository()
        
        # Create snapshot directory and file manually
        snapshots_dir = temp_repo_path / ".tellus" / "snapshots"
        snapshots_dir.mkdir(parents=True)
        
        snapshot_id = str(uuid.uuid4())
        snapshot_data = {
            "id": snapshot_id,
            "timestamp": "2023-01-01T12:00:00",
            "message": "Test snapshot",
            "author": "Test Author",
            "parent_id": None,
            "changes": [
                {
                    "file_path": "data/test.nc",
                    "change_type": "added",
                    "old_hash": None,
                    "new_hash": {
                        "algorithm": "sha256",
                        "value": "hash123"
                    },
                    "old_path": None,
                    "timestamp": "2023-01-01T12:00:00"
                }
            ]
        }
        
        with open(snapshots_dir / f"{snapshot_id}.json", 'w') as f:
            json.dump(snapshot_data, f)
        
        # Act
        result = repo._load_snapshots(temp_repo_path)
        
        # Assert
        assert len(result) == 1
        snapshot = result[0]
        assert snapshot.id == snapshot_id
        assert snapshot.message == "Test snapshot"
        assert snapshot.author == "Test Author"
        assert len(snapshot.changes) == 1
        assert snapshot.changes[0].file_path == "data/test.nc"
        assert snapshot.changes[0].change_type == FileChangeType.ADDED
    
    def test_load_snapshots_empty_directory(self, temp_repo_path):
        """Test loading snapshots from empty directory."""
        repo = JsonFileTrackingRepository()
        
        # Create empty snapshots directory
        snapshots_dir = temp_repo_path / ".tellus" / "snapshots"
        snapshots_dir.mkdir(parents=True)
        
        # Act
        result = repo._load_snapshots(temp_repo_path)
        
        # Assert
        assert result == []
    
    def test_load_snapshots_missing_directory(self, temp_repo_path):
        """Test loading snapshots when directory doesn't exist."""
        repo = JsonFileTrackingRepository()
        
        # Act
        result = repo._load_snapshots(temp_repo_path)
        
        # Assert
        assert result == []
    
    def test_load_snapshots_corrupted_file(self, temp_repo_path):
        """Test loading snapshots with corrupted JSON file."""
        repo = JsonFileTrackingRepository()
        
        # Create snapshot directory with corrupted file
        snapshots_dir = temp_repo_path / ".tellus" / "snapshots"
        snapshots_dir.mkdir(parents=True)
        
        with open(snapshots_dir / "corrupted.json", 'w') as f:
            f.write("{ invalid json")
        
        # Act - should skip corrupted files
        result = repo._load_snapshots(temp_repo_path)
        
        # Assert
        assert result == []  # Corrupted file skipped
    
    def test_snapshots_sorted_by_timestamp(self, temp_repo_path):
        """Test that snapshots are sorted by timestamp."""
        repo = JsonFileTrackingRepository()
        
        snapshots_dir = temp_repo_path / ".tellus" / "snapshots"
        snapshots_dir.mkdir(parents=True)
        
        # Create snapshots with different timestamps
        timestamps = [
            "2023-01-01T10:00:00",  # First chronologically
            "2023-01-01T15:00:00",  # Last chronologically  
            "2023-01-01T12:00:00"   # Middle chronologically
        ]
        
        for i, timestamp in enumerate(timestamps):
            snapshot_data = {
                "id": f"snapshot-{i}",
                "timestamp": timestamp,
                "message": f"Snapshot {i}",
                "author": "Test Author",
                "parent_id": None,
                "changes": []
            }
            
            with open(snapshots_dir / f"snapshot-{i}.json", 'w') as f:
                json.dump(snapshot_data, f)
        
        # Act
        result = repo._load_snapshots(temp_repo_path)
        
        # Assert - should be sorted by timestamp
        assert len(result) == 3
        assert result[0].id == "snapshot-0"  # 10:00
        assert result[1].id == "snapshot-2"  # 12:00  
        assert result[2].id == "snapshot-1"  # 15:00


class TestDVCConfiguration:
    """Test DVC configuration handling."""
    
    def test_save_and_load_dvc_config(self, temp_repo_path, sample_dvc_config):
        """Test saving and loading DVC configuration."""
        repo = JsonFileTrackingRepository()
        
        # Ensure parent directory exists
        temp_repo_path.mkdir(parents=True, exist_ok=True)
        
        # Create repository with DVC config
        tracking_repo = FileTrackingRepository(
            root_path=str(temp_repo_path),
            state=RepositoryState(),
            dvc_config=sample_dvc_config,
            snapshots=[]
        )
        
        # Act - save
        repo.save_repository_state(tracking_repo)
        
        # Act - load
        result = repo.get_repository(str(temp_repo_path))
        
        # Assert
        assert result is not None
        dvc_config = result.dvc_config
        assert dvc_config.enabled is True
        assert dvc_config.remote_name == "origin"
        assert dvc_config.remote_url == "s3://my-bucket/dvc-cache"
        assert dvc_config.cache_dir == ".dvc/cache"
        assert dvc_config.large_file_threshold == 100 * 1024 * 1024
    
    def test_load_dvc_config_missing_file(self, temp_repo_path):
        """Test loading DVC config when file doesn't exist."""
        repo = JsonFileTrackingRepository()
        
        # Create repository structure without DVC config
        tellus_dir = temp_repo_path / ".tellus"
        tellus_dir.mkdir(parents=True)
        
        # Create minimal state files
        with open(tellus_dir / "tracked_files.json", 'w') as f:
            json.dump({}, f)
        
        with open(tellus_dir / "ignore_patterns.json", 'w') as f:
            json.dump([], f)
        
        # Act
        result = repo.get_repository(str(temp_repo_path))
        
        # Assert - should use default DVC config
        assert result is not None
        assert isinstance(result.dvc_config, DVCConfiguration)
        assert result.dvc_config.enabled is False  # Default
    
    def test_load_dvc_config_corrupted_file(self, temp_repo_path):
        """Test loading DVC config with corrupted JSON."""
        repo = JsonFileTrackingRepository()
        
        # Create repository structure
        tellus_dir = temp_repo_path / ".tellus"
        tellus_dir.mkdir(parents=True)
        
        # Create corrupted DVC config
        with open(tellus_dir / "dvc_config.json", 'w') as f:
            f.write("{ invalid json")
        
        # Create minimal state files
        with open(tellus_dir / "tracked_files.json", 'w') as f:
            json.dump({}, f)
        
        with open(tellus_dir / "ignore_patterns.json", 'w') as f:
            json.dump([], f)
        
        # Act - should handle corrupted DVC config gracefully
        result = repo.get_repository(str(temp_repo_path))
        
        # Assert - should use default config
        assert result is not None
        assert isinstance(result.dvc_config, DVCConfiguration)
        assert result.dvc_config.enabled is False  # Default


class TestErrorHandling:
    """Test error handling scenarios."""
    
    def test_save_state_with_corrupted_tracked_files(self, temp_repo_path):
        """Test saving state when tracked files JSON is corrupted."""
        repo = JsonFileTrackingRepository()
        
        # Create repository structure with corrupted tracked files
        tellus_dir = temp_repo_path / ".tellus"
        tellus_dir.mkdir(parents=True)
        
        with open(tellus_dir / "tracked_files.json", 'w') as f:
            f.write("{ invalid json")
        
        # Create tracking repository
        tracking_repo = FileTrackingRepository(
            root_path=str(temp_repo_path),
            state=RepositoryState(),
            dvc_config=DVCConfiguration(),
            snapshots=[]
        )
        
        # Act & Assert - should handle gracefully
        repo.save_repository_state(tracking_repo)  # Should not raise exception
        
        # Verify files were overwritten correctly
        with open(tellus_dir / "tracked_files.json") as f:
            data = json.load(f)
        assert data == {}  # Empty state
    
    def test_load_state_with_corrupted_tracked_files(self, temp_repo_path):
        """Test loading state with corrupted tracked files JSON."""
        repo = JsonFileTrackingRepository()
        
        # Create repository structure with corrupted file
        tellus_dir = temp_repo_path / ".tellus"
        tellus_dir.mkdir(parents=True)
        
        with open(tellus_dir / "tracked_files.json", 'w') as f:
            f.write("{ invalid json")
        
        with open(tellus_dir / "ignore_patterns.json", 'w') as f:
            json.dump([], f)
        
        # Act
        result = repo.load_repository_state(str(temp_repo_path))
        
        # Assert - should return empty state
        assert result is not None
        assert len(result.tracked_files) == 0
        assert len(result.ignore_patterns) == 0  # Default empty
    
    def test_load_state_with_missing_hash_data(self, temp_repo_path, sample_tracked_file):
        """Test loading state with missing hash data in tracked files."""
        repo = JsonFileTrackingRepository()
        
        # Ensure parent directory exists
        temp_repo_path.mkdir(parents=True, exist_ok=True)
        
        # Create tracking repo and save normally first
        tracking_repo = FileTrackingRepository(
            root_path=str(temp_repo_path),
            state=RepositoryState(),
            dvc_config=DVCConfiguration(),
            snapshots=[]
        )
        tracking_repo.state.tracked_files["data/test.nc"] = sample_tracked_file
        repo.save_repository_state(tracking_repo)
        
        # Corrupt the tracked files JSON to remove hash data
        tellus_dir = temp_repo_path / ".tellus"
        tracked_files_path = tellus_dir / "tracked_files.json"
        
        with open(tracked_files_path) as f:
            data = json.load(f)
        
        # Remove content_hash to simulate corruption
        del data["data/test.nc"]["content_hash"]
        
        with open(tracked_files_path, 'w') as f:
            json.dump(data, f)
        
        # Act - should handle missing hash gracefully
        result = repo.load_repository_state(str(temp_repo_path))
        
        # Assert - should return empty state due to validation error
        assert result is not None
        assert len(result.tracked_files) == 0  # Corrupted entry skipped


class TestComplexScenarios:
    """Test complex scenarios and edge cases."""
    
    def test_multiple_file_types_tracking(self, temp_repo_path):
        """Test tracking multiple file types with different metadata."""
        repo = JsonFileTrackingRepository()
        
        # Ensure parent directory exists
        temp_repo_path.mkdir(parents=True, exist_ok=True)
        
        # Create repository with multiple tracked files
        state = RepositoryState()
        
        # Add different file types
        files = [
            TrackedFileMetadata(
                path="data/large_dataset.nc",
                size=100 * 1024 * 1024,  # 100MB
                modification_time=datetime(2023, 1, 1, 10, 0, 0),
                content_hash=FileHash("sha256", "large_file_hash"),
                status=TrackingStatus.TRACKED,
                created_time=datetime(2023, 1, 1, 9, 0, 0)
            ),
            TrackedFileMetadata(
                path="scripts/analysis.py",
                size=2048,
                modification_time=datetime(2023, 1, 1, 11, 0, 0),
                content_hash=FileHash("sha256", "script_hash"),
                status=TrackingStatus.MODIFIED,
                created_time=datetime(2023, 1, 1, 8, 0, 0)
            ),
            TrackedFileMetadata(
                path="docs/README.md",
                size=1024,
                modification_time=datetime(2023, 1, 1, 12, 0, 0),
                content_hash=FileHash("md5", "readme_hash"),
                status=TrackingStatus.UNTRACKED,
                created_time=datetime(2023, 1, 1, 7, 0, 0)
            )
        ]
        
        for file_meta in files:
            state.tracked_files[file_meta.path] = file_meta
        
        tracking_repo = FileTrackingRepository(
            root_path=str(temp_repo_path),
            state=state,
            dvc_config=DVCConfiguration(),
            snapshots=[]
        )
        
        # Act - save and reload
        repo.save_repository_state(tracking_repo)
        result = repo.get_repository(str(temp_repo_path))
        
        # Assert
        assert result is not None
        assert len(result.state.tracked_files) == 3
        
        # Check each file type
        large_file = result.state.tracked_files["data/large_dataset.nc"]
        assert large_file.size == 100 * 1024 * 1024
        assert large_file.status == TrackingStatus.TRACKED
        
        script_file = result.state.tracked_files["scripts/analysis.py"]
        assert script_file.size == 2048
        assert script_file.status == TrackingStatus.MODIFIED
        
        readme_file = result.state.tracked_files["docs/README.md"]
        assert readme_file.content_hash.algorithm == "md5"
        assert readme_file.status == TrackingStatus.UNTRACKED
    
    def test_large_number_of_snapshots(self, temp_repo_path, sample_file_tracking_repo):
        """Test handling large number of snapshots."""
        repo = JsonFileTrackingRepository()
        
        # Ensure parent directory exists
        temp_repo_path.mkdir(parents=True, exist_ok=True)
        
        # Ensure snapshots directory exists
        (temp_repo_path / ".tellus" / "snapshots").mkdir(parents=True, exist_ok=True)
        
        # Create many snapshots
        num_snapshots = 50
        for i in range(num_snapshots):
            change = FileChange(
                file_path=f"data/file_{i}.nc",
                change_type=FileChangeType.ADDED,
                old_hash=None,
                new_hash=FileHash("sha256", f"hash_{i}"),
                timestamp=datetime(2023, 1, 1, i % 24, i % 60, 0)  # Varied timestamps
            )
            
            repo.create_snapshot(
                sample_file_tracking_repo,
                f"Add file {i}",
                f"Author {i % 3}",  # Varied authors
                [change]
            )
        
        # Act
        snapshots = repo.get_snapshots(sample_file_tracking_repo)
        
        # Assert
        assert len(snapshots) == num_snapshots
        assert len(sample_file_tracking_repo.snapshots) == num_snapshots
        
        # Verify snapshots are sorted by timestamp
        for i in range(len(snapshots) - 1):
            assert snapshots[i].timestamp <= snapshots[i + 1].timestamp
        
        # Verify parent-child relationships
        for i in range(1, len(snapshots)):
            assert snapshots[i].parent_id == snapshots[i - 1].id
    
    def test_concurrent_operations_thread_safety(self, temp_repo_path):
        """Test thread safety of repository operations."""
        import threading
        import time

        # Ensure parent directory exists
        temp_repo_path.mkdir(parents=True, exist_ok=True)
        
        repo = JsonFileTrackingRepository()
        tracking_repo = repo.create_repository(str(temp_repo_path))
        
        errors = []
        results = []
        
        def create_snapshots(thread_id):
            try:
                for i in range(5):
                    change = FileChange(
                        file_path=f"thread_{thread_id}_file_{i}.nc",
                        change_type=FileChangeType.ADDED,
                        old_hash=None,
                        new_hash=FileHash("sha256", f"thread_{thread_id}_hash_{i}"),
                        timestamp=datetime.now()
                    )
                    
                    snapshot = repo.create_snapshot(
                        tracking_repo,
                        f"Thread {thread_id} commit {i}",
                        f"Thread {thread_id}",
                        [change]
                    )
                    results.append(snapshot.id)
                    time.sleep(0.01)  # Small delay to allow interleaving
                    
            except Exception as e:
                errors.append(e)
        
        # Create multiple threads
        threads = []
        for thread_id in range(3):
            thread = threading.Thread(target=create_snapshots, args=(thread_id,))
            threads.append(thread)
            thread.start()
        
        # Wait for all threads to complete
        for thread in threads:
            thread.join()
        
        # Assert
        assert len(errors) == 0
        assert len(results) == 15  # 3 threads * 5 snapshots each
        assert len(set(results)) == 15  # All unique snapshot IDs
        assert len(tracking_repo.snapshots) == 15