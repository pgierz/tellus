"""
Tests for FSSpec adapter.

Tests the infrastructure adapter for filesystem operations across multiple protocols,
including progress tracking, error handling, and file operations.
"""

import time
from pathlib import Path
from typing import Any, Dict, List, Tuple
from unittest.mock import MagicMock, Mock, call, patch

import fsspec
import pytest
from fsspec.callbacks import Callback
from fsspec.spec import AbstractFileSystem

from tellus.domain.entities.location import LocationEntity, LocationKind
from tellus.infrastructure.adapters.fsspec_adapter import (
    FSSpecAdapter, FSSpecProgressCallback, ProgressTracker)


@pytest.fixture
def sample_location():
    """Create a sample location entity for testing."""
    return LocationEntity(
        name="test-storage",
        protocol="file",
        kinds=[LocationKind.DISK],
        host=None,
        path="/tmp/test-storage",
        storage_options={}
    )


@pytest.fixture 
def sftp_location():
    """Create a sample SFTP location entity for testing."""
    return LocationEntity(
        name="remote-server",
        protocol="sftp",
        kinds=[LocationKind.COMPUTE],
        host="example.com",
        path="/data",
        storage_options={"username": "testuser", "port": 22}
    )


@pytest.fixture
def mock_filesystem():
    """Create a mock fsspec filesystem for testing."""
    mock_fs = Mock(spec=AbstractFileSystem)
    mock_fs.ls = Mock(return_value=["file1.txt", "file2.txt"])
    mock_fs.exists = Mock(return_value=True)
    mock_fs.isfile = Mock(return_value=True)
    mock_fs.isdir = Mock(return_value=False)
    mock_fs.size = Mock(return_value=1024)
    mock_fs.info = Mock(return_value={"name": "file.txt", "size": 1024, "type": "file"})
    mock_fs.glob = Mock(return_value=["file1.txt", "file2.txt"])
    mock_fs.walk = Mock(return_value=[("/root", ["dir1"], ["file1.txt"])])
    mock_fs.get_file = Mock()
    mock_fs.open = Mock()
    return mock_fs


class TestProgressTracker:
    """Test suite for ProgressTracker."""
    
    def test_init_with_defaults(self):
        """Test initialization with default values."""
        tracker = ProgressTracker("test_operation")
        
        assert tracker.operation == "test_operation"
        assert tracker.total_size is None
        assert tracker.total_files is None
        assert tracker.bytes_transferred == 0
        assert tracker.files_completed == 0
        assert tracker.callbacks == []
        assert isinstance(tracker.start_time, float)
    
    def test_init_with_custom_values(self):
        """Test initialization with custom values."""
        tracker = ProgressTracker(
            operation="custom_op",
            total_size=10240,
            total_files=5
        )
        
        assert tracker.operation == "custom_op"
        assert tracker.total_size == 10240
        assert tracker.total_files == 5
    
    def test_add_callback(self):
        """Test adding progress callbacks."""
        tracker = ProgressTracker("test_op")
        callback1 = Mock()
        callback2 = Mock()
        
        tracker.add_callback(callback1)
        tracker.add_callback(callback2)
        
        assert len(tracker.callbacks) == 2
        assert callback1 in tracker.callbacks
        assert callback2 in tracker.callbacks
    
    def test_update_bytes(self):
        """Test updating bytes transferred."""
        tracker = ProgressTracker("test_op")
        
        tracker.update_bytes(256)
        assert tracker.bytes_transferred == 256
        
        tracker.update_bytes(512)
        assert tracker.bytes_transferred == 768
    
    def test_update_bytes_with_callbacks(self):
        """Test updating bytes with callbacks."""
        tracker = ProgressTracker("test_op")
        
        callback1 = Mock()
        callback1.relative_update = Mock()
        callback2 = Mock()  # No relative_update method
        
        tracker.add_callback(callback1)
        tracker.add_callback(callback2)
        
        tracker.update_bytes(256)
        
        callback1.relative_update.assert_called_once_with(256)
        # callback2 should not be called since it doesn't have relative_update
    
    def test_update_files(self):
        """Test updating files completed."""
        tracker = ProgressTracker("test_op")
        
        tracker.update_files()  # Default increment of 1
        assert tracker.files_completed == 1
        
        tracker.update_files(3)
        assert tracker.files_completed == 4
    
    def test_get_progress_info(self):
        """Test getting progress information."""
        start_time = time.time()
        
        with patch('time.time', return_value=start_time):
            tracker = ProgressTracker("test_op", total_size=2048, total_files=4)
        
        tracker.update_bytes(1024)
        tracker.update_files(2)
        
        with patch('time.time', return_value=start_time + 10):
            info = tracker.get_progress_info()
        
        assert info["operation"] == "test_op"
        assert info["bytes_transferred"] == 1024
        assert info["files_completed"] == 2
        assert info["elapsed_seconds"] == 10
        assert info["total_size"] == 2048
        assert info["total_files"] == 4


class TestFSSpecProgressCallback:
    """Test suite for FSSpecProgressCallback."""
    
    def test_init(self):
        """Test initialization of progress callback."""
        tracker = ProgressTracker("test_op")
        callback = FSSpecProgressCallback(tracker)
        
        assert callback.tracker == tracker
        assert isinstance(callback, Callback)
    
    def test_relative_update(self):
        """Test relative progress updates."""
        tracker = ProgressTracker("test_op")
        callback = FSSpecProgressCallback(tracker)
        
        with patch.object(Callback, 'relative_update') as mock_super_update:
            callback.relative_update(128)
        
        assert tracker.bytes_transferred == 128
        mock_super_update.assert_called_once_with(128)
    
    def test_relative_update_default(self):
        """Test relative update with default increment."""
        tracker = ProgressTracker("test_op")
        callback = FSSpecProgressCallback(tracker)
        
        with patch.object(Callback, 'relative_update') as mock_super_update:
            callback.relative_update()
        
        assert tracker.bytes_transferred == 1
        mock_super_update.assert_called_once_with(1)
    
    def test_absolute_update_first_call(self):
        """Test absolute update on first call."""
        tracker = ProgressTracker("test_op")
        callback = FSSpecProgressCallback(tracker)
        
        with patch.object(Callback, 'absolute_update') as mock_super_update:
            callback.absolute_update(256)
        
        assert tracker.bytes_transferred == 256
        assert callback._last_val == 256
        mock_super_update.assert_called_once_with(256)
    
    def test_absolute_update_subsequent_call(self):
        """Test absolute update on subsequent calls."""
        tracker = ProgressTracker("test_op")
        callback = FSSpecProgressCallback(tracker)
        
        with patch.object(Callback, 'absolute_update') as mock_super_update:
            callback.absolute_update(256)
            callback.absolute_update(512)
        
        # Should have added 256 (initial) + 256 (increment from 256 to 512)
        assert tracker.bytes_transferred == 512
        assert callback._last_val == 512


class TestFSSpecAdapter:
    """Test suite for FSSpecAdapter."""
    
    def test_init(self, sample_location):
        """Test adapter initialization."""
        adapter = FSSpecAdapter(sample_location)
        
        assert adapter.location == sample_location
        assert adapter._fs is None
        assert adapter._connection_tested is False
    
    def test_fs_property_creates_filesystem(self, sample_location):
        """Test that fs property creates filesystem on first access."""
        adapter = FSSpecAdapter(sample_location)
        
        with patch('fsspec.filesystem') as mock_fsspec:
            mock_fs = Mock()
            mock_fsspec.return_value = mock_fs
            
            # First access should create filesystem
            result = adapter.fs
            
            assert result == mock_fs
            assert adapter._fs == mock_fs
            mock_fsspec.assert_called_once_with("file", path="/tmp/test-storage")
    
    def test_fs_property_reuses_filesystem(self, sample_location):
        """Test that fs property reuses existing filesystem."""
        adapter = FSSpecAdapter(sample_location)
        
        with patch('fsspec.filesystem') as mock_fsspec:
            mock_fs = Mock()
            mock_fsspec.return_value = mock_fs
            
            # Multiple accesses should reuse filesystem
            fs1 = adapter.fs
            fs2 = adapter.fs
            
            assert fs1 == fs2 == mock_fs
            mock_fsspec.assert_called_once()
    
    def test_fs_property_adds_host_for_sftp(self, sftp_location):
        """Test that fs property adds host for SFTP connections."""
        adapter = FSSpecAdapter(sftp_location)
        
        with patch('fsspec.filesystem') as mock_fsspec:
            mock_fs = Mock()
            mock_fsspec.return_value = mock_fs
            
            adapter.fs
            
            expected_options = {
                "username": "testuser",
                "port": 22,
                "host": "remote-server",
                "path": "/data"
            }
            mock_fsspec.assert_called_once_with("sftp", **expected_options)
    
    def test_test_connection_success(self, sample_location):
        """Test successful connection test."""
        adapter = FSSpecAdapter(sample_location)
        
        with patch.object(adapter, 'fs') as mock_fs:
            mock_fs.ls = Mock(return_value=["file1.txt"])
            
            start_time = time.time()
            with patch('time.time', side_effect=[start_time, start_time + 0.1]):
                result = adapter.test_connection()
        
        assert result["success"] is True
        assert result["protocol"] == "file"
        assert result["location_name"] == "test-storage"
        assert result["error"] is None
        assert result["response_time"] == 0.1
        assert adapter._connection_tested is True
    
    def test_test_connection_failure(self, sample_location):
        """Test failed connection test."""
        adapter = FSSpecAdapter(sample_location)
        
        with patch.object(adapter, 'fs') as mock_fs:
            mock_fs.ls = Mock(side_effect=Exception("Connection failed"))
            
            start_time = time.time()
            with patch('time.time', side_effect=[start_time, start_time + 1.0]):
                result = adapter.test_connection()
        
        assert result["success"] is False
        assert result["error"] == "Connection failed"
        assert result["response_time"] == 1.0
        assert adapter._connection_tested is False
    
    def test_get_file_success(self, sample_location, tmp_path):
        """Test successful file download."""
        adapter = FSSpecAdapter(sample_location)
        local_path = tmp_path / "downloaded_file.txt"
        
        with patch.object(adapter, 'fs') as mock_fs:
            mock_fs.size = Mock(return_value=1024)
            mock_fs.get_file = Mock()
            
            result = adapter.get_file("/remote/file.txt", local_path)
        
        assert result == str(local_path)
        mock_fs.get_file.assert_called_once()
        assert local_path.parent.exists()  # Directory created
    
    def test_get_file_with_progress_tracker(self, sample_location, tmp_path):
        """Test file download with progress tracking."""
        adapter = FSSpecAdapter(sample_location)
        local_path = tmp_path / "downloaded_file.txt"
        tracker = ProgressTracker("download")
        
        with patch.object(adapter, 'fs') as mock_fs:
            mock_fs.size = Mock(return_value=2048)
            mock_fs.get_file = Mock()
            
            adapter.get_file("/remote/file.txt", local_path, progress_tracker=tracker)
        
        assert tracker.total_size == 2048
        assert tracker.files_completed == 1
        
        # Check that callback was passed to get_file
        call_args = mock_fs.get_file.call_args
        assert call_args[1]["callback"] is not None
        assert isinstance(call_args[1]["callback"], FSSpecProgressCallback)
    
    def test_get_file_overwrite_false_file_exists(self, sample_location, tmp_path):
        """Test file download fails when file exists and overwrite=False."""
        adapter = FSSpecAdapter(sample_location)
        local_path = tmp_path / "existing_file.txt"
        local_path.write_text("existing content")
        
        with pytest.raises(FileExistsError) as exc_info:
            adapter.get_file("/remote/file.txt", local_path, overwrite=False)
        
        assert "File already exists" in str(exc_info.value)
    
    def test_get_file_overwrite_true_file_exists(self, sample_location, tmp_path):
        """Test file download succeeds when file exists and overwrite=True."""
        adapter = FSSpecAdapter(sample_location)
        local_path = tmp_path / "existing_file.txt"
        local_path.write_text("existing content")
        
        with patch.object(adapter, 'fs') as mock_fs:
            mock_fs.size = Mock(return_value=1024)
            mock_fs.get_file = Mock()
            
            result = adapter.get_file("/remote/file.txt", local_path, overwrite=True)
        
        assert result == str(local_path)
        mock_fs.get_file.assert_called_once()
    
    def test_get_file_failure_cleanup(self, sample_location, tmp_path):
        """Test that partial downloads are cleaned up on failure."""
        adapter = FSSpecAdapter(sample_location)
        local_path = tmp_path / "failed_download.txt"
        
        with patch.object(adapter, 'fs') as mock_fs:
            mock_fs.size = Mock(return_value=1024)
            mock_fs.get_file = Mock(side_effect=Exception("Download failed"))
            
            # Create partial file to simulate failed download
            local_path.write_text("partial content")
            
            with pytest.raises(RuntimeError) as exc_info:
                adapter.get_file("/remote/file.txt", local_path)
        
        assert "Failed to download" in str(exc_info.value)
        assert not local_path.exists()  # File should be cleaned up
    
    def test_get_files_success(self, sample_location, tmp_path):
        """Test successful multiple file download."""
        adapter = FSSpecAdapter(sample_location)
        
        # Mock find_files to return two files
        file_info = {"size": 512, "type": "file"}
        with patch.object(adapter, 'find_files') as mock_find:
            mock_find.return_value = [
                ("/remote/file1.txt", file_info),
                ("/remote/file2.txt", file_info)
            ]
            
            with patch.object(adapter, 'get_file') as mock_get_file:
                mock_get_file.side_effect = [
                    str(tmp_path / "file1.txt"),
                    str(tmp_path / "file2.txt")
                ]
                
                result = adapter.get_files("*.txt", tmp_path)
        
        assert len(result) == 2
        assert str(tmp_path / "file1.txt") in result
        assert str(tmp_path / "file2.txt") in result
    
    def test_get_files_with_progress_tracker(self, sample_location, tmp_path):
        """Test multiple file download with progress tracking."""
        adapter = FSSpecAdapter(sample_location)
        tracker = ProgressTracker("batch_download")
        
        file_info = {"size": 512, "type": "file"}
        with patch.object(adapter, 'find_files') as mock_find:
            mock_find.return_value = [
                ("/remote/file1.txt", file_info),
                ("/remote/file2.txt", file_info)
            ]
            
            with patch.object(adapter, 'get_file') as mock_get_file:
                mock_get_file.return_value = "downloaded"
                
                adapter.get_files("*.txt", tmp_path, progress_tracker=tracker)
        
        assert tracker.total_files == 2
        assert tracker.total_size == 1024  # 2 files * 512 bytes each
        assert tracker.files_completed == 2
    
    def test_get_files_skip_existing_no_overwrite(self, sample_location, tmp_path):
        """Test that existing files are skipped when overwrite=False."""
        adapter = FSSpecAdapter(sample_location)
        
        # Create existing file
        existing_file = tmp_path / "file1.txt"
        existing_file.write_text("existing")
        
        file_info = {"size": 512, "type": "file"}
        with patch.object(adapter, 'find_files') as mock_find:
            mock_find.return_value = [
                ("/remote/file1.txt", file_info),
                ("/remote/file2.txt", file_info)
            ]
            
            with patch.object(adapter, 'get_file') as mock_get_file:
                mock_get_file.return_value = str(tmp_path / "file2.txt")
                
                result = adapter.get_files("*.txt", tmp_path, overwrite=False)
        
        assert len(result) == 1  # Only file2.txt downloaded
        assert str(tmp_path / "file2.txt") in result
        # get_file should only be called once (for file2.txt)
        mock_get_file.assert_called_once()
    
    def test_find_files_non_recursive(self, sample_location):
        """Test finding files without recursion."""
        adapter = FSSpecAdapter(sample_location)
        
        with patch.object(adapter, 'fs') as mock_fs:
            mock_fs.glob = Mock(return_value=["/path/file1.txt", "/path/file2.txt"])
            mock_fs.isfile = Mock(return_value=True)
            mock_fs.info = Mock(return_value={"size": 1024, "type": "file"})
            
            files = list(adapter.find_files("*.txt"))
        
        assert len(files) == 2
        mock_fs.glob.assert_called_once_with("*.txt")
        assert mock_fs.info.call_count == 2
    
    def test_find_files_recursive(self, sample_location):
        """Test finding files with recursion."""
        adapter = FSSpecAdapter(sample_location)
        
        with patch.object(adapter, 'fs') as mock_fs:
            mock_fs.walk = Mock(return_value=[
                ("/root", ["dir1"], ["file1.txt"]),
                ("/root/dir1", [], ["file2.txt", "file3.py"])
            ])
            mock_fs.info = Mock(return_value={"size": 1024, "type": "file"})
            
            files = list(adapter.find_files("*.txt", recursive=True))
        
        assert len(files) == 2  # Only .txt files should match
        mock_fs.walk.assert_called_once()
    
    def test_find_files_with_base_path(self, sample_location):
        """Test finding files with base path."""
        adapter = FSSpecAdapter(sample_location)
        
        with patch.object(adapter, '_resolve_remote_path') as mock_resolve:
            mock_resolve.return_value = "/resolved/path"
            
            with patch.object(adapter, 'fs') as mock_fs:
                mock_fs.glob = Mock(return_value=[])
                
                list(adapter.find_files("*.txt", base_path="/custom/base"))
        
        mock_resolve.assert_called_once_with("/custom/base")
        mock_fs.glob.assert_called_once_with("/resolved/path/*.txt")
    
    def test_find_files_handles_errors(self, sample_location, capsys):
        """Test that find_files handles errors gracefully."""
        adapter = FSSpecAdapter(sample_location)
        
        with patch.object(adapter, 'fs') as mock_fs:
            mock_fs.glob = Mock(side_effect=Exception("Glob failed"))
            
            files = list(adapter.find_files("*.txt"))
        
        assert files == []
        captured = capsys.readouterr()
        assert "Warning: Failed to glob pattern" in captured.out
    
    def test_exists(self, sample_location):
        """Test path existence check."""
        adapter = FSSpecAdapter(sample_location)
        
        with patch.object(adapter, '_resolve_remote_path') as mock_resolve:
            mock_resolve.return_value = "/resolved/path.txt"
            
            with patch.object(adapter, 'fs') as mock_fs:
                mock_fs.exists = Mock(return_value=True)
                
                result = adapter.exists("path.txt")
        
        assert result is True
        mock_resolve.assert_called_once_with("path.txt")
        mock_fs.exists.assert_called_once_with("/resolved/path.txt")
    
    def test_isfile(self, sample_location):
        """Test file type check."""
        adapter = FSSpecAdapter(sample_location)
        
        with patch.object(adapter, '_resolve_remote_path') as mock_resolve:
            mock_resolve.return_value = "/resolved/file.txt"
            
            with patch.object(adapter, 'fs') as mock_fs:
                mock_fs.isfile = Mock(return_value=True)
                
                result = adapter.isfile("file.txt")
        
        assert result is True
        mock_resolve.assert_called_once_with("file.txt")
        mock_fs.isfile.assert_called_once_with("/resolved/file.txt")
    
    def test_isdir(self, sample_location):
        """Test directory type check."""
        adapter = FSSpecAdapter(sample_location)
        
        with patch.object(adapter, '_resolve_remote_path') as mock_resolve:
            mock_resolve.return_value = "/resolved/dir"
            
            with patch.object(adapter, 'fs') as mock_fs:
                mock_fs.isdir = Mock(return_value=True)
                
                result = adapter.isdir("dir")
        
        assert result is True
        mock_resolve.assert_called_once_with("dir")
        mock_fs.isdir.assert_called_once_with("/resolved/dir")
    
    def test_size(self, sample_location):
        """Test getting file size."""
        adapter = FSSpecAdapter(sample_location)
        
        with patch.object(adapter, '_resolve_remote_path') as mock_resolve:
            mock_resolve.return_value = "/resolved/file.txt"
            
            with patch.object(adapter, 'fs') as mock_fs:
                mock_fs.size = Mock(return_value=2048)
                
                result = adapter.size("file.txt")
        
        assert result == 2048
        mock_resolve.assert_called_once_with("file.txt")
        mock_fs.size.assert_called_once_with("/resolved/file.txt")
    
    def test_info(self, sample_location):
        """Test getting file information."""
        adapter = FSSpecAdapter(sample_location)
        file_info = {"name": "file.txt", "size": 1024, "type": "file"}
        
        with patch.object(adapter, '_resolve_remote_path') as mock_resolve:
            mock_resolve.return_value = "/resolved/file.txt"
            
            with patch.object(adapter, 'fs') as mock_fs:
                mock_fs.info = Mock(return_value=file_info)
                
                result = adapter.info("file.txt")
        
        assert result == file_info
        mock_resolve.assert_called_once_with("file.txt")
        mock_fs.info.assert_called_once_with("/resolved/file.txt")
    
    def test_open_file(self, sample_location):
        """Test opening a file."""
        adapter = FSSpecAdapter(sample_location)
        
        mock_file = Mock()
        with patch.object(adapter, '_resolve_remote_path') as mock_resolve:
            mock_resolve.return_value = "/resolved/file.txt"
            
            with patch.object(adapter, 'fs') as mock_fs:
                mock_fs.open = Mock(return_value=mock_file)
                mock_file.__enter__ = Mock(return_value=mock_file)
                mock_file.__exit__ = Mock(return_value=None)
                
                with adapter.open_file("file.txt", "r", encoding="utf-8") as f:
                    assert f == mock_file
        
        mock_resolve.assert_called_once_with("file.txt")
        mock_fs.open.assert_called_once_with("/resolved/file.txt", "r", encoding="utf-8")
    
    def test_get_filesystem_info(self, sample_location):
        """Test getting filesystem information."""
        adapter = FSSpecAdapter(sample_location)
        adapter._connection_tested = True
        
        with patch.object(adapter, 'fs') as mock_fs:
            mock_fs.__class__.__name__ = "LocalFileSystem"
            
            result = adapter.get_filesystem_info()
        
        assert result["protocol"] == "file"
        assert result["location_name"] == "test-storage"
        assert result["base_path"] == "/tmp/test-storage"
        assert result["connection_tested"] is True
        assert result["filesystem_class"] == "LocalFileSystem"
    
    def test_resolve_remote_path_empty_path(self, sample_location):
        """Test resolving empty path."""
        adapter = FSSpecAdapter(sample_location)
        
        result = adapter._resolve_remote_path("")
        
        assert result == "/tmp/test-storage"
    
    def test_resolve_remote_path_absolute(self, sample_location):
        """Test resolving absolute path."""
        adapter = FSSpecAdapter(sample_location)
        
        result = adapter._resolve_remote_path("/absolute/path.txt")
        
        assert result == "/absolute/path.txt"
    
    def test_resolve_remote_path_relative(self, sample_location):
        """Test resolving relative path."""
        adapter = FSSpecAdapter(sample_location)
        
        result = adapter._resolve_remote_path("relative/path.txt")
        
        assert result == "/tmp/test-storage/relative/path.txt"
    
    def test_resolve_remote_path_no_base_path(self):
        """Test resolving path when location has no base path."""
        location = LocationEntity(
            name="no-base",
            protocol="file",
            kinds=[LocationKind.DISK],
            path=None,
            storage_options={}
        )
        adapter = FSSpecAdapter(location)
        
        result = adapter._resolve_remote_path("some/path.txt")
        
        assert result == "some/path.txt"
    
    def test_close_with_close_method(self, sample_location):
        """Test closing filesystem that supports close method."""
        adapter = FSSpecAdapter(sample_location)
        
        mock_fs = Mock()
        mock_fs.close = Mock()
        adapter._fs = mock_fs
        adapter._connection_tested = True
        
        adapter.close()
        
        mock_fs.close.assert_called_once()
        assert adapter._fs is None
        assert adapter._connection_tested is False
    
    def test_close_without_close_method(self, sample_location):
        """Test closing filesystem that doesn't support close method."""
        adapter = FSSpecAdapter(sample_location)
        
        mock_fs = Mock()
        # Don't add close method to mock_fs
        adapter._fs = mock_fs
        adapter._connection_tested = True
        
        # Should not raise exception
        adapter.close()
        
        assert adapter._fs is None
        assert adapter._connection_tested is False
    
    def test_close_with_exception(self, sample_location):
        """Test closing filesystem when close method raises exception."""
        adapter = FSSpecAdapter(sample_location)
        
        mock_fs = Mock()
        mock_fs.close = Mock(side_effect=Exception("Close failed"))
        adapter._fs = mock_fs
        adapter._connection_tested = True
        
        # Should not raise exception
        adapter.close()
        
        assert adapter._fs is None
        assert adapter._connection_tested is False


class TestIntegrationScenarios:
    """Test integration scenarios and real-world usage patterns."""
    
    def test_large_file_download_with_progress(self, sample_location, tmp_path):
        """Test downloading large file with progress tracking."""
        adapter = FSSpecAdapter(sample_location)
        tracker = ProgressTracker("large_download", total_size=100*1024*1024)
        
        # Simulate callbacks being triggered during download
        def mock_get_file(remote, local, callback=None):
            if callback:
                # Simulate progress updates during download
                for i in range(0, 100*1024*1024, 10*1024*1024):
                    callback.absolute_update(i)
        
        with patch.object(adapter, 'fs') as mock_fs:
            mock_fs.size = Mock(return_value=100*1024*1024)
            mock_fs.get_file = Mock(side_effect=mock_get_file)
            
            result = adapter.get_file(
                "/remote/large_file.nc", 
                tmp_path / "large_file.nc",
                progress_tracker=tracker
            )
        
        assert result == str(tmp_path / "large_file.nc")
        assert tracker.total_size == 100*1024*1024
        assert tracker.files_completed == 1
    
    def test_batch_download_with_mixed_success(self, sample_location, tmp_path, capsys):
        """Test batch download where some files fail."""
        adapter = FSSpecAdapter(sample_location)
        
        file_info = {"size": 1024, "type": "file"}
        with patch.object(adapter, 'find_files') as mock_find:
            mock_find.return_value = [
                ("/remote/file1.txt", file_info),
                ("/remote/file2.txt", file_info),
                ("/remote/file3.txt", file_info)
            ]
            
            def mock_get_file(remote, local, overwrite=False):
                if "file2" in remote:
                    raise Exception("Download failed")
                return str(local)
            
            with patch.object(adapter, 'get_file') as mock_get_file_patch:
                mock_get_file_patch.side_effect = mock_get_file
                
                result = adapter.get_files("*.txt", tmp_path)
        
        # Should have downloaded 2 files (file2 failed)
        assert len(result) == 2
        
        # Check warning was printed
        captured = capsys.readouterr()
        assert "Warning: Failed to download" in captured.out
        assert "file2.txt" in captured.out
    
    def test_connection_test_with_different_protocols(self):
        """Test connection testing with different protocols."""
        # Test local filesystem
        local_location = LocationEntity(
            name="local",
            protocol="file",
            kinds=[LocationKind.DISK],
            path="/tmp",
            storage_options={}
        )
        
        local_adapter = FSSpecAdapter(local_location)
        
        with patch.object(local_adapter, 'fs') as mock_fs:
            mock_fs.ls = Mock(return_value=["file1", "file2"])
            
            result = local_adapter.test_connection()
            
            assert result["success"] is True
            assert result["protocol"] == "file"
    
    def test_recursive_file_search(self, sample_location):
        """Test recursive file searching with complex directory structure."""
        adapter = FSSpecAdapter(sample_location)
        
        # Mock complex directory structure
        walk_result = [
            ("/root", ["data", "output"], ["readme.txt"]),
            ("/root/data", ["2023", "2024"], ["config.yaml"]),
            ("/root/data/2023", [], ["jan.nc", "feb.nc", "summary.txt"]),
            ("/root/data/2024", [], ["jan.nc", "feb.nc"]),
            ("/root/output", [], ["results.nc", "plots.png"])
        ]
        
        with patch.object(adapter, 'fs') as mock_fs:
            mock_fs.walk = Mock(return_value=walk_result)
            mock_fs.info = Mock(return_value={"size": 1024, "type": "file"})
            
            # Find all .nc files recursively
            nc_files = list(adapter.find_files("*.nc", recursive=True))
        
        # Should find 4 .nc files
        assert len(nc_files) == 4
        paths = [path for path, _ in nc_files]
        assert any("jan.nc" in path for path in paths)
        assert any("feb.nc" in path for path in paths)
        assert any("results.nc" in path for path in paths)
    
    def test_filesystem_reuse_across_operations(self, sample_location):
        """Test that filesystem instance is reused across operations."""
        adapter = FSSpecAdapter(sample_location)
        
        with patch('fsspec.filesystem') as mock_fsspec:
            mock_fs = Mock()
            mock_fs.exists = Mock(return_value=True)
            mock_fs.size = Mock(return_value=1024)
            mock_fs.info = Mock(return_value={"size": 1024})
            mock_fsspec.return_value = mock_fs
            
            # Perform multiple operations
            adapter.exists("file1.txt")
            adapter.size("file2.txt")
            adapter.info("file3.txt")
            
            # fsspec.filesystem should only be called once
            mock_fsspec.assert_called_once()
    
    def test_error_recovery_during_batch_operations(self, sample_location, tmp_path):
        """Test error recovery during batch file operations."""
        adapter = FSSpecAdapter(sample_location)
        tracker = ProgressTracker("batch_with_errors")
        
        # Mock files with some causing errors during info retrieval
        def mock_find_files(pattern, recursive=False):
            yield ("/remote/good1.txt", {"size": 512})
            yield ("/remote/bad.txt", {"size": 1024})  # This will cause error
            yield ("/remote/good2.txt", {"size": 256})
        
        def mock_get_file(remote, local, overwrite=False):
            if "bad" in remote:
                raise Exception("Corrupted file")
            return str(local)
        
        with patch.object(adapter, 'find_files') as mock_find:
            mock_find.return_value = mock_find_files("*.txt")
            
            with patch.object(adapter, 'get_file') as mock_get:
                mock_get.side_effect = mock_get_file
                
                result = adapter.get_files("*.txt", tmp_path, progress_tracker=tracker)
        
        # Should have downloaded 2 good files despite error with bad file
        assert len(result) == 2
        assert tracker.files_completed == 2


class TestEdgeCases:
    """Test edge cases and boundary conditions."""
    
    def test_adapter_with_minimal_location(self):
        """Test adapter with minimal location configuration."""
        location = LocationEntity(
            name="minimal",
            protocol="file",
            kinds=[],
            storage_options={}
        )
        adapter = FSSpecAdapter(location)
        
        # Should still work with minimal configuration
        assert adapter.location == location
        assert adapter._resolve_remote_path("test.txt") == "test.txt"
    
    def test_progress_tracker_with_zero_values(self):
        """Test progress tracker with zero values."""
        tracker = ProgressTracker("test", total_size=0, total_files=0)
        
        assert tracker.total_size == 0
        assert tracker.total_files == 0
        
        info = tracker.get_progress_info()
        assert info["total_size"] == 0
        assert info["total_files"] == 0
    
    def test_find_files_with_empty_pattern(self, sample_location):
        """Test finding files with empty pattern."""
        adapter = FSSpecAdapter(sample_location)
        
        with patch.object(adapter, 'fs') as mock_fs:
            mock_fs.glob = Mock(return_value=[])
            
            files = list(adapter.find_files(""))
        
        assert files == []
    
    def test_get_files_empty_pattern_result(self, sample_location, tmp_path):
        """Test get_files when pattern matches no files."""
        adapter = FSSpecAdapter(sample_location)
        
        with patch.object(adapter, 'find_files') as mock_find:
            mock_find.return_value = []
            
            result = adapter.get_files("*.nonexistent", tmp_path)
        
        assert result == []
    
    def test_callback_without_relative_update_method(self):
        """Test progress tracker with callback lacking relative_update."""
        tracker = ProgressTracker("test")
        
        # Add callback without relative_update method
        callback_without_method = Mock()
        # Don't add relative_update attribute
        
        tracker.add_callback(callback_without_method)
        
        # Should not raise exception
        tracker.update_bytes(100)
        
        assert tracker.bytes_transferred == 100
    
    def test_connection_test_timeout_parameter(self, sample_location):
        """Test that connection test accepts timeout parameter."""
        adapter = FSSpecAdapter(sample_location)
        
        with patch.object(adapter, 'fs') as mock_fs:
            mock_fs.ls = Mock(return_value=[])
            
            # Should accept timeout parameter without error
            result = adapter.test_connection(timeout=60)
            
            assert result["success"] is True
    
    def test_filesystem_info_before_fs_creation(self, sample_location):
        """Test getting filesystem info before filesystem is created."""
        adapter = FSSpecAdapter(sample_location)
        
        # Don't access .fs property to trigger filesystem creation
        result = adapter.get_filesystem_info()
        
        assert result["protocol"] == "file"
        assert result["location_name"] == "test-storage"
        assert result["connection_tested"] is False