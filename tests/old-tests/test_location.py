import json
import pytest
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock

from tellus.location import (
    Location,
    LocationKind,
    LocationExistsError,
)


def test_location_initialization():
    """Test Location dataclass initialization with valid data"""
    config = {"path": "/some/path", "protocol": "file"}
    location = Location(
        name="test_loc", 
        kinds=[LocationKind.DISK], 
        config=config
    )

    assert location.name == "test_loc"
    assert location.kinds == [LocationKind.DISK]
    assert location.config == config
    assert location.optional is False


def test_location_invalid_kind():
    """Test that Location raises ValueError for invalid location kinds"""
    with pytest.raises(ValueError) as excinfo:
        Location(name="test_loc", kinds=["INVALID"], config={})

    assert "is not a valid LocationKind" in str(excinfo.value)


def test_location_kinds_enum():
    """Test LocationKind enum functionality"""
    # Test enum values exist
    assert LocationKind.DISK
    assert LocationKind.TAPE
    assert LocationKind.COMPUTE
    
    # Test from_str method
    assert LocationKind.from_str("disk") == LocationKind.DISK
    assert LocationKind.from_str("TAPE") == LocationKind.TAPE
    
    with pytest.raises(ValueError):
        LocationKind.from_str("invalid")


@patch('tellus.location.location.Location._save_locations')
def test_location_registry_management(mock_save):
    """Test location class registry management"""
    # Clear any existing locations first
    Location._locations.clear()
    
    config = {"path": "/test/path", "protocol": "file"}
    location = Location(
        name="test_registry",
        kinds=[LocationKind.DISK],
        config=config
    )
    
    # Check it was added to registry
    assert "test_registry" in Location._locations
    assert Location._locations["test_registry"] == location
    
    # Test duplicate name raises error
    with pytest.raises(LocationExistsError):
        Location(
            name="test_registry",
            kinds=[LocationKind.TAPE], 
            config=config
        )
    
    # Clean up
    Location._locations.clear()


def test_location_from_dict():
    """Test Location.from_dict class method"""
    data = {
        "name": "test_from_dict",
        "kinds": ["DISK", "COMPUTE"],
        "config": {"path": "/data", "protocol": "sftp"},
        "optional": True
    }
    
    # Clear locations first
    Location._locations.clear()
    
    location = Location.from_dict(data)
    
    assert location.name == "test_from_dict"
    assert location.kinds == [LocationKind.DISK, LocationKind.COMPUTE]
    assert location.config == {"path": "/data", "protocol": "sftp"}
    assert location.optional is True
    
    # Clean up
    Location._locations.clear()


def test_location_to_dict():
    """Test Location.to_dict method"""
    Location._locations.clear()
    
    config = {"path": "/test", "protocol": "file"}
    location = Location(
        name="test_to_dict",
        kinds=[LocationKind.DISK],
        config=config,
        optional=True
    )
    
    result = location.to_dict()
    expected = {
        "name": "test_to_dict",
        "kinds": ["DISK"],
        "config": config,
        "optional": True
    }
    
    assert result == expected
    Location._locations.clear()


@patch('tellus.location.location.Location._save_locations')
def test_location_class_methods(mock_save):
    """Test Location class methods for registry management"""
    Location._locations.clear()
    
    config = {"path": "/test", "protocol": "file"}
    location = Location(
        name="class_methods_test",
        kinds=[LocationKind.DISK],
        config=config
    )
    
    # Test get_location
    retrieved = Location.get_location("class_methods_test")
    assert retrieved == location
    assert Location.get_location("nonexistent") is None
    
    # Test list_locations
    locations = Location.list_locations()
    assert len(locations) == 1
    assert location in locations
    
    # Test remove_location
    Location.remove_location("class_methods_test")
    assert "class_methods_test" not in Location._locations
    assert Location.get_location("class_methods_test") is None


@patch('fsspec.filesystem')
def test_location_fs_property(mock_fsspec):
    """Test Location.fs property creates correct filesystem"""
    Location._locations.clear()
    
    mock_fs = Mock()
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
    assert fs == mock_fs
    
    Location._locations.clear()


@patch('tellus.location.location.Location._save_locations')
@patch('fsspec.filesystem')
def test_location_get_method(mock_fsspec, mock_save):
    """Test Location.get method for file downloads"""
    Location._locations.clear()
    
    # Mock filesystem
    mock_fs = Mock()
    mock_fs.size.return_value = 1024
    mock_fs.get_file = Mock()
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
        mock_callback.__enter__ = Mock(return_value=mock_callback)
        mock_callback.__exit__ = Mock(return_value=None)
        mock_progress.return_value = mock_callback
        
        result = location.get("remote/file.txt", "local/file.txt", show_progress=True)
        
        # Verify filesystem calls
        mock_fs.size.assert_called_once_with("remote/file.txt")
        mock_progress.assert_called_once()
        mock_fs.get_file.assert_called_once()
        
        assert result == "local/file.txt"
    
    Location._locations.clear()
