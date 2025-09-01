"""
Test suite for the Location class in tellus.location.location.

This test suite is designed to be independent of any existing tests and provides
comprehensive coverage of the Location class functionality.
"""

import json
import os
import shutil
import tempfile
from pathlib import Path
from unittest.mock import ANY, MagicMock, patch

import pytest

from tellus.location import Location, LocationExistsError, LocationKind


class TestLocationBasics:
    """Test basic Location class functionality and initialization."""

    def setup_method(self):
        """Clear any existing locations before each test."""
        Location._locations = {}

    def test_location_initialization(self):
        """Test that a Location can be initialized with valid parameters."""
        # Setup
        config = {"protocol": "file", "path": "/test/path"}

        # Exercise
        location = Location(
            name="test_loc", kinds=[LocationKind.DISK], config=config, optional=True
        )

        # Verify
        assert location.name == "test_loc"
        assert location.kinds == [LocationKind.DISK]
        assert location.config == config
        assert location.optional is True

    def test_invalid_kind_raises_error(self):
        """Test that invalid location kinds raise a ValueError."""
        with pytest.raises(ValueError, match="is not a valid LocationKind"):
            Location(name="invalid_kind", kinds=["INVALID"], config={})

    def test_duplicate_name_raises_error(self):
        """Test that duplicate location names raise LocationExistsError."""
        # Setup - create first location
        Location(name="dupe", kinds=[LocationKind.DISK], config={"protocol": "file"})

        # Exercise & Verify - try to create duplicate
        with pytest.raises(LocationExistsError):
            Location(
                name="dupe", kinds=[LocationKind.TAPE], config={"protocol": "file"}
            )

    def test_location_kinds_enum(self):
        """Test LocationKind enum functionality."""
        # Test enum values exist
        assert LocationKind.DISK
        assert LocationKind.TAPE
        assert LocationKind.COMPUTE
        
        # Test from_str method
        assert LocationKind.from_str("disk") == LocationKind.DISK
        assert LocationKind.from_str("TAPE") == LocationKind.TAPE
        
        with pytest.raises(ValueError):
            LocationKind.from_str("invalid")


class TestLocationFilesystemIntegration:
    """Test Location filesystem integration and properties."""

    def setup_method(self):
        """Clear any existing locations before each test."""
        Location._locations = {}

    @patch('fsspec.filesystem')
    def test_location_fs_property(self, mock_fsspec):
        """Test Location.fs property creates correct filesystem."""
        mock_fs = MagicMock()
        mock_fsspec.return_value = mock_fs
        
        config = {
            "protocol": "sftp",
            "storage_options": {"username": "test", "port": 22}
        }
        location = Location(
            name="fs_test",
            kinds=[LocationKind.COMPUTE],
            config=config
        )
        
        # Access fs property
        fs = location.fs
        
        # Verify fsspec.filesystem was called correctly
        expected_options = {"username": "test", "port": 22, "host": "fs_test"}
        mock_fsspec.assert_called_once_with("sftp", **expected_options)
        # fs is now a PathSandboxedFileSystem wrapping mock_fs
        assert fs._fs == mock_fs
        assert hasattr(fs, 'base_path')

    @patch('fsspec.filesystem')
    def test_location_get_method(self, mock_fsspec):
        """Test Location.get method for file downloads."""
        # Mock filesystem
        mock_fs = MagicMock()
        mock_fs.size.return_value = 1024
        mock_fs.get_file = MagicMock()
        mock_fsspec.return_value = mock_fs
        
        config = {"protocol": "file", "path": "/test"}
        location = Location(
            name="get_test",
            kinds=[LocationKind.DISK],
            config=config
        )
        
        # Test download
        with patch('pathlib.Path.exists', return_value=False), \
             patch('pathlib.Path.mkdir'), \
             patch('tellus.location.location.get_progress_callback') as mock_progress:
            
            # Create a mock that supports the context manager protocol
            mock_callback = MagicMock()
            mock_callback.__enter__ = MagicMock(return_value=mock_callback)
            mock_callback.__exit__ = MagicMock(return_value=None)
            mock_progress.return_value = mock_callback
            
            result = location.get("remote/file.txt", "local/file.txt", show_progress=True)
            
            # Verify filesystem calls - path gets resolved relative to config["path"]
            mock_fs.size.assert_called_once_with("/test/remote/file.txt")
            mock_progress.assert_called_once()
            mock_fs.get_file.assert_called_once()
            
            assert result == "local/file.txt"


class TestLocationSerialization:
    """Test serialization and deserialization of Location objects."""

    def setup_method(self):
        """Clear any existing locations before each test."""
        Location._locations = {}

    def test_to_dict(self):
        """Test converting a Location to a dictionary."""
        # Setup
        location = Location(
            name="test_serialize",
            kinds=[LocationKind.DISK, LocationKind.TAPE],
            config={"protocol": "memory"},
            optional=True,
        )

        # Exercise
        result = location.to_dict()

        # Verify
        assert result == {
            "name": "test_serialize",
            "kinds": ["DISK", "TAPE"],
            "config": {"protocol": "memory"},
            "optional": True,
        }

    def test_from_dict(self):
        """Test creating a Location from a dictionary."""
        # Setup
        data = {
            "name": "from_dict_test",
            "kinds": ["DISK", "COMPUTE"],
            "config": {"protocol": "s3"},
            "optional": False,
        }

        # Exercise
        location = Location.from_dict(data)

        # Verify
        assert location.name == "from_dict_test"
        assert location.kinds == [LocationKind.DISK, LocationKind.COMPUTE]
        assert location.config == {"protocol": "s3"}
        assert location.optional is False


class TestLocationPersistence:
    """Test persistence of Location objects to/from disk."""

    def setup_method(self):
        """Create a temporary directory for test files and clear existing locations."""
        self.temp_dir = tempfile.mkdtemp()
        self.locations_file = Path(self.temp_dir) / "locations.json"
        Location._locations_file = self.locations_file
        Location._locations = {}

    def teardown_method(self):
        """Clean up temporary directory."""
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_save_and_load_locations(self):
        """Test saving locations to disk and loading them back."""
        # Setup - create some locations
        loc1 = Location("loc1", [LocationKind.DISK], {"protocol": "file"})
        loc2 = Location("loc2", [LocationKind.TAPE], {"protocol": "s3"}, optional=True)

        # Save locations (happens automatically in __post_init__)
        assert self.locations_file.exists()

        # Clear in-memory locations
        Location._locations = {}

        # Exercise - load locations back
        Location.load_locations()

        # Verify
        assert len(Location._locations) == 2
        assert "loc1" in Location._locations
        assert "loc2" in Location._locations
        assert Location._locations["loc1"].kinds == [LocationKind.DISK]
        assert Location._locations["loc2"].optional is True

    def test_remove_location(self):
        """Test removing a location."""
        # Setup
        Location("to_remove", [LocationKind.DISK], {"protocol": "file"})
        assert "to_remove" in Location._locations

        # Exercise
        Location.remove_location("to_remove")

        # Verify
        assert "to_remove" not in Location._locations
        assert (
            not self.locations_file.exists()
        )  # File should be removed when last location is removed


class TestLocationFilesystemOperations:
    """Test filesystem operations using the Location class."""

    def setup_method(self):
        """Set up test environment."""
        # Create a temporary directory for this test
        self.temp_dir = Path(tempfile.mkdtemp())
        
        # Create a location with a unique name for this test
        test_id = str(id(self))
        self.location = Location(
            f"test_fs_{test_id}",  # Unique name for each test instance
            [LocationKind.DISK],
            {
                "protocol": "file",
                "storage_options": {
                    "auto_mkdir": True
                },
                "path": str(self.temp_dir.absolute())
            },
        )
        
        # Ensure the location directory exists
        self.temp_dir.mkdir(parents=True, exist_ok=True)

    def teardown_method(self):
        """Clean up test environment."""
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_get_file(self, tmp_path):
        """Test downloading a file from the location."""
        # Setup - create a test file in the location's directory structure
        test_content = "test content"
        test_file = self.temp_dir / "test_file.txt"
        test_file.parent.mkdir(parents=True, exist_ok=True)
        test_file.write_text(test_content)
        
        # Create a local path for the download
        local_path = Path(tmp_path) / "downloaded.txt"
        
        # Exercise - download the file using the relative path from the location root
        result = self.location.get("test_file.txt", str(local_path))
        
        # Verify the file was downloaded correctly
        assert result == str(local_path), "Returned path doesn't match expected"
        assert local_path.exists(), "Downloaded file was not created"
        assert local_path.read_text() == test_content, "File content doesn't match"

    @patch('fsspec.filesystem')
    def test_get_file_with_progress(self, mock_fs):
        """Test file download with progress tracking."""
        # Setup - mock the filesystem and file object
        mock_file = MagicMock()
        
        # Create a function that simulates fsspec callback behavior
        def mock_read_with_callback(size=None):
            # Get the next chunk from the side_effect
            if hasattr(mock_read_with_callback, 'call_count'):
                mock_read_with_callback.call_count += 1
            else:
                mock_read_with_callback.call_count = 1
                mock_read_with_callback.chunks = [b"chunk1", b"chunk2", b""]
            
            if mock_read_with_callback.call_count <= len(mock_read_with_callback.chunks):
                chunk = mock_read_with_callback.chunks[mock_read_with_callback.call_count - 1]
                # Simulate fsspec calling the callback during read
                if hasattr(mock_file, '_callback') and chunk:
                    mock_file._callback.relative_update(len(chunk))
                return chunk
            return b""
        
        mock_file.read.side_effect = mock_read_with_callback
        mock_file.close.return_value = None
        
        # Mock the filesystem's open method to store the callback
        def mock_open(path, mode, callback=None, **kwargs):
            mock_file._callback = callback  # Store callback on file object
            return mock_file
        
        # Configure the mock filesystem
        mock_fs.return_value.open.side_effect = mock_open
        mock_fs.return_value.size.return_value = 100

        # Create a test location with our mocked filesystem and unique name
        location = Location(
            f"mock_fs_{id(self)}",  # Unique name for this test
            [LocationKind.DISK],
            {"protocol": "mock"}
        )

        # Mock progress callback
        mock_callback = MagicMock()
        
        # Exercise - read the file in chunks
        with location.get_fileobj("test.txt", progress_callback=mock_callback) as (file_obj, size):
            # Read the file in chunks
            chunks = []
            while True:
                chunk = file_obj.read(1024)
                if not chunk:
                    break
                chunks.append(chunk)
            content = b''.join(chunks)

        # Verify the content and callbacks
        assert content == b"chunk1chunk2"
        assert size == 100
        mock_callback.set_size.assert_called_once_with(100)
        # Should be called twice - once for each chunk
        assert mock_callback.relative_update.call_count == 2

    def test_find_files(self):
        """Test finding files in the location."""
        # Setup - create some test files in the location's directory
        subdir = Path(self.temp_dir) / "subdir"
        subdir.mkdir()
        (Path(self.temp_dir) / "test1.txt").touch()
        (Path(self.temp_dir) / "test2.log").touch()
        (subdir / "test3.txt").touch()

        # Exercise - find all .txt files (non-recursive)
        result = list(self.location.find_files("*.txt"))
        
        # Verify - should only find files in the root directory
        assert len(result) == 1  # Only test1.txt in root
        assert any(r[0].endswith("test1.txt") for r in result)

        # Test recursive search
        result_recursive = list(self.location.find_files("*.txt", recursive=True))
        # Should find both test1.txt and subdir/test3.txt
        assert len(result_recursive) == 2
        # Convert paths to strings for easier checking
        paths = [r[0] for r in result_recursive]
        assert any("test1.txt" in p for p in paths)
        assert any("test3.txt" in p for p in paths)
