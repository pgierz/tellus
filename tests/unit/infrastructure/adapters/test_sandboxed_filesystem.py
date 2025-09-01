"""
Tests for PathSandboxedFileSystem.

Tests the infrastructure adapter for sandboxed filesystem operations,
including path validation, security constraints, and delegation to underlying filesystems.
"""

import os
import tempfile
from pathlib import Path
from typing import List
from unittest.mock import MagicMock, Mock, call, patch

import fsspec
import pytest
from fsspec.implementations.local import LocalFileSystem

from tellus.infrastructure.adapters.sandboxed_filesystem import (
    PathSandboxedFileSystem, PathValidationError)


@pytest.fixture
def temp_sandbox_dir(tmp_path):
    """Create a temporary directory for sandboxing tests."""
    sandbox_dir = tmp_path / "sandbox"
    sandbox_dir.mkdir(parents=True, exist_ok=True)
    
    # Create some test files and directories
    (sandbox_dir / "file1.txt").write_text("content1")
    (sandbox_dir / "file2.txt").write_text("content2")
    (sandbox_dir / "subdir").mkdir()
    (sandbox_dir / "subdir" / "nested.txt").write_text("nested content")
    
    return sandbox_dir


@pytest.fixture
def mock_filesystem():
    """Create a mock filesystem for testing delegation."""
    mock_fs = Mock(spec=LocalFileSystem)
    mock_fs.protocol = "file"
    
    # Configure common methods with reasonable defaults
    mock_fs.exists.return_value = True
    mock_fs.isfile.return_value = True
    mock_fs.isdir.return_value = False
    mock_fs.ls.return_value = []
    mock_fs.listdir.return_value = []
    mock_fs.glob.return_value = []
    mock_fs.walk.return_value = []
    mock_fs.find.return_value = []
    mock_fs.info.return_value = {"name": "test", "size": 100, "type": "file"}
    mock_fs.size.return_value = 100
    mock_fs.open.return_value = MagicMock()
    mock_fs.read_text.return_value = "test content"
    mock_fs.read_bytes.return_value = b"test content"
    
    return mock_fs


@pytest.fixture
def real_filesystem():
    """Create a real local filesystem for integration tests."""
    return LocalFileSystem()


class TestPathSandboxedFileSystemInitialization:
    """Test filesystem initialization and configuration."""
    
    def test_init_with_base_path(self, mock_filesystem, temp_sandbox_dir):
        """Test initialization with a base path."""
        fs = PathSandboxedFileSystem(mock_filesystem, str(temp_sandbox_dir))
        
        assert fs._fs == mock_filesystem
        assert fs._base_path.endswith(os.sep)
        assert str(temp_sandbox_dir) in fs._base_path
        assert fs.protocol == "file"
    
    def test_init_empty_base_path(self, mock_filesystem):
        """Test initialization with empty base path."""
        fs = PathSandboxedFileSystem(mock_filesystem, "")
        
        assert fs._fs == mock_filesystem
        assert fs._base_path == ""
        assert fs.protocol == "file"
    
    def test_init_no_base_path(self, mock_filesystem):
        """Test initialization with no base path (defaults to empty)."""
        fs = PathSandboxedFileSystem(mock_filesystem)
        
        assert fs._base_path == ""
    
    def test_protocol_property(self, mock_filesystem):
        """Test protocol property delegation."""
        mock_filesystem.protocol = "s3"
        fs = PathSandboxedFileSystem(mock_filesystem, "/test")
        
        assert fs.protocol == "s3"
    
    def test_base_path_property(self, mock_filesystem, temp_sandbox_dir):
        """Test base_path property access."""
        fs = PathSandboxedFileSystem(mock_filesystem, str(temp_sandbox_dir))
        
        assert fs.base_path.endswith(os.sep)
        assert str(temp_sandbox_dir) in fs.base_path
    
    def test_repr(self, mock_filesystem, temp_sandbox_dir):
        """Test string representation."""
        fs = PathSandboxedFileSystem(mock_filesystem, str(temp_sandbox_dir))
        
        repr_str = repr(fs)
        assert "PathSandboxedFileSystem" in repr_str
        assert str(temp_sandbox_dir) in repr_str
        assert "file" in repr_str


class TestPathResolution:
    """Test path resolution and validation logic."""
    
    def test_normalize_base_path_empty(self, mock_filesystem):
        """Test base path normalization with empty path."""
        fs = PathSandboxedFileSystem(mock_filesystem, "")
        
        assert fs._base_path == ""
    
    def test_normalize_base_path_absolute(self, mock_filesystem, temp_sandbox_dir):
        """Test base path normalization with absolute path."""
        fs = PathSandboxedFileSystem(mock_filesystem, str(temp_sandbox_dir))
        
        assert fs._base_path.endswith(os.sep)
        assert os.path.isabs(fs._base_path)
    
    def test_normalize_base_path_relative(self, mock_filesystem):
        """Test base path normalization with relative path."""
        fs = PathSandboxedFileSystem(mock_filesystem, "test/path")
        
        # Should resolve relative to current directory
        assert fs._base_path.endswith(os.sep)
        assert os.path.isabs(fs._base_path)
        assert "test" in fs._base_path and "path" in fs._base_path
    
    def test_resolve_path_no_sandbox(self, mock_filesystem):
        """Test path resolution without sandbox constraints."""
        fs = PathSandboxedFileSystem(mock_filesystem, "")
        
        result = fs._resolve_path("test/file.txt")
        assert result == "test/file.txt"
    
    def test_resolve_path_relative_within_sandbox(self, mock_filesystem, temp_sandbox_dir):
        """Test resolution of relative path within sandbox."""
        fs = PathSandboxedFileSystem(mock_filesystem, str(temp_sandbox_dir))
        
        result = fs._resolve_path("file.txt")
        assert result.startswith(str(temp_sandbox_dir))
        assert result.endswith("file.txt")
    
    def test_resolve_path_absolute_converted_to_relative(self, mock_filesystem, temp_sandbox_dir):
        """Test that absolute paths are treated as relative for security."""
        fs = PathSandboxedFileSystem(mock_filesystem, str(temp_sandbox_dir))
        
        result = fs._resolve_path("/etc/passwd")
        # Should be treated as relative to sandbox
        assert result.startswith(str(temp_sandbox_dir))
        assert "etc" in result and "passwd" in result
    
    def test_resolve_path_with_parent_references(self, mock_filesystem, temp_sandbox_dir):
        """Test resolution with parent directory references."""
        fs = PathSandboxedFileSystem(mock_filesystem, str(temp_sandbox_dir))
        
        # This should be allowed as it stays within sandbox
        result = fs._resolve_path("subdir/../file.txt")
        assert result.startswith(str(temp_sandbox_dir))
        assert result.endswith("file.txt")
    
    def test_resolve_path_escape_attempt(self, mock_filesystem, temp_sandbox_dir):
        """Test that path escape attempts are blocked."""
        fs = PathSandboxedFileSystem(mock_filesystem, str(temp_sandbox_dir))
        
        with pytest.raises(PathValidationError) as exc_info:
            fs._resolve_path("../../../etc/passwd")
        
        assert "outside the allowed base path" in str(exc_info.value)
    
    def test_resolve_path_already_within_sandbox(self, mock_filesystem, temp_sandbox_dir):
        """Test resolution of path already within sandbox (e.g., from glob results)."""
        fs = PathSandboxedFileSystem(mock_filesystem, str(temp_sandbox_dir))
        
        # Path already within sandbox should be returned as-is
        within_path = str(temp_sandbox_dir / "file.txt")
        result = fs._resolve_path(within_path)
        assert result == str(Path(within_path).resolve())
    
    def test_resolve_paths_list(self, mock_filesystem, temp_sandbox_dir):
        """Test resolution of multiple paths."""
        fs = PathSandboxedFileSystem(mock_filesystem, str(temp_sandbox_dir))
        
        paths = ["file1.txt", "file2.txt", "subdir/nested.txt"]
        results = fs._resolve_paths(paths)
        
        assert isinstance(results, list)
        assert len(results) == 3
        for result in results:
            assert result.startswith(str(temp_sandbox_dir))
    
    def test_resolve_paths_single(self, mock_filesystem, temp_sandbox_dir):
        """Test resolution of single path."""
        fs = PathSandboxedFileSystem(mock_filesystem, str(temp_sandbox_dir))
        
        result = fs._resolve_paths("file.txt")
        assert isinstance(result, str)
        assert result.startswith(str(temp_sandbox_dir))
    
    def test_is_within_base_path_valid(self, mock_filesystem, temp_sandbox_dir):
        """Test path validation for valid paths."""
        fs = PathSandboxedFileSystem(mock_filesystem, str(temp_sandbox_dir))
        
        valid_path = str(temp_sandbox_dir / "file.txt")
        assert fs._is_within_base_path(valid_path) is True
    
    def test_is_within_base_path_invalid(self, mock_filesystem, temp_sandbox_dir):
        """Test path validation for invalid paths."""
        fs = PathSandboxedFileSystem(mock_filesystem, str(temp_sandbox_dir))
        
        invalid_path = str(temp_sandbox_dir.parent / "outside.txt")
        assert fs._is_within_base_path(invalid_path) is False
    
    def test_is_within_base_path_no_sandbox(self, mock_filesystem):
        """Test path validation without sandbox constraints."""
        fs = PathSandboxedFileSystem(mock_filesystem, "")
        
        # Any path should be valid without sandbox
        assert fs._is_within_base_path("/any/path") is True


class TestFileOperations:
    """Test file operation methods with path sandboxing."""
    
    def test_exists(self, mock_filesystem, temp_sandbox_dir):
        """Test exists method with path resolution."""
        fs = PathSandboxedFileSystem(mock_filesystem, str(temp_sandbox_dir))
        
        fs.exists("test.txt")
        
        expected_path = fs._resolve_path("test.txt")
        mock_filesystem.exists.assert_called_once_with(expected_path)
    
    def test_isfile(self, mock_filesystem, temp_sandbox_dir):
        """Test isfile method with path resolution."""
        fs = PathSandboxedFileSystem(mock_filesystem, str(temp_sandbox_dir))
        
        fs.isfile("test.txt")
        
        expected_path = fs._resolve_path("test.txt")
        mock_filesystem.isfile.assert_called_once_with(expected_path)
    
    def test_isdir(self, mock_filesystem, temp_sandbox_dir):
        """Test isdir method with path resolution."""
        fs = PathSandboxedFileSystem(mock_filesystem, str(temp_sandbox_dir))
        
        fs.isdir("subdir")
        
        expected_path = fs._resolve_path("subdir")
        mock_filesystem.isdir.assert_called_once_with(expected_path)
    
    def test_ls_with_path(self, mock_filesystem, temp_sandbox_dir):
        """Test ls method with path resolution."""
        fs = PathSandboxedFileSystem(mock_filesystem, str(temp_sandbox_dir))
        
        fs.ls("subdir", detail=True)
        
        expected_path = fs._resolve_path("subdir")
        mock_filesystem.ls.assert_called_once_with(expected_path, detail=True)
    
    def test_ls_without_path(self, mock_filesystem, temp_sandbox_dir):
        """Test ls method without path (uses base path)."""
        fs = PathSandboxedFileSystem(mock_filesystem, str(temp_sandbox_dir))
        
        fs.ls("")
        
        mock_filesystem.ls.assert_called_once_with(fs._base_path, detail=True)
    
    def test_listdir(self, mock_filesystem, temp_sandbox_dir):
        """Test listdir method with path resolution."""
        fs = PathSandboxedFileSystem(mock_filesystem, str(temp_sandbox_dir))
        
        fs.listdir("subdir")
        
        expected_path = fs._resolve_path("subdir")
        mock_filesystem.listdir.assert_called_once_with(expected_path)
    
    def test_glob_relative_pattern(self, mock_filesystem, temp_sandbox_dir):
        """Test glob method with relative pattern."""
        fs = PathSandboxedFileSystem(mock_filesystem, str(temp_sandbox_dir))
        
        fs.glob("*.txt")
        
        expected_pattern = os.path.join(fs._base_path, "*.txt")
        mock_filesystem.glob.assert_called_once_with(expected_pattern)
    
    def test_glob_absolute_pattern(self, mock_filesystem, temp_sandbox_dir):
        """Test glob method with absolute pattern."""
        fs = PathSandboxedFileSystem(mock_filesystem, str(temp_sandbox_dir))
        
        pattern = "/absolute/*.txt"
        fs.glob(pattern)
        
        mock_filesystem.glob.assert_called_once_with(pattern)
    
    def test_walk(self, mock_filesystem, temp_sandbox_dir):
        """Test walk method with path resolution."""
        fs = PathSandboxedFileSystem(mock_filesystem, str(temp_sandbox_dir))
        
        fs.walk("subdir")
        
        expected_path = fs._resolve_path("subdir")
        mock_filesystem.walk.assert_called_once_with(expected_path)
    
    def test_find(self, mock_filesystem, temp_sandbox_dir):
        """Test find method with path resolution."""
        fs = PathSandboxedFileSystem(mock_filesystem, str(temp_sandbox_dir))
        
        fs.find("subdir", pattern="*.txt")
        
        expected_path = fs._resolve_path("subdir")
        mock_filesystem.find.assert_called_once_with(expected_path, pattern="*.txt")
    
    def test_info(self, mock_filesystem, temp_sandbox_dir):
        """Test info method with path resolution."""
        fs = PathSandboxedFileSystem(mock_filesystem, str(temp_sandbox_dir))
        
        fs.info("test.txt")
        
        expected_path = fs._resolve_path("test.txt")
        mock_filesystem.info.assert_called_once_with(expected_path)
    
    def test_size(self, mock_filesystem, temp_sandbox_dir):
        """Test size method with path resolution."""
        fs = PathSandboxedFileSystem(mock_filesystem, str(temp_sandbox_dir))
        
        fs.size("test.txt")
        
        expected_path = fs._resolve_path("test.txt")
        mock_filesystem.size.assert_called_once_with(expected_path)
    
    def test_open(self, mock_filesystem, temp_sandbox_dir):
        """Test open method with path resolution."""
        fs = PathSandboxedFileSystem(mock_filesystem, str(temp_sandbox_dir))
        
        fs.open("test.txt", mode="r", encoding="utf-8")
        
        expected_path = fs._resolve_path("test.txt")
        mock_filesystem.open.assert_called_once_with(expected_path, "r", encoding="utf-8")
    
    def test_read_text(self, mock_filesystem, temp_sandbox_dir):
        """Test read_text method with path resolution."""
        fs = PathSandboxedFileSystem(mock_filesystem, str(temp_sandbox_dir))
        
        fs.read_text("test.txt", encoding="utf-8")
        
        expected_path = fs._resolve_path("test.txt")
        mock_filesystem.read_text.assert_called_once_with(expected_path, encoding="utf-8")
    
    def test_read_bytes(self, mock_filesystem, temp_sandbox_dir):
        """Test read_bytes method with path resolution."""
        fs = PathSandboxedFileSystem(mock_filesystem, str(temp_sandbox_dir))
        
        fs.read_bytes("test.txt", buffer_size=1024)
        
        expected_path = fs._resolve_path("test.txt")
        mock_filesystem.read_bytes.assert_called_once_with(expected_path, buffer_size=1024)


class TestWriteOperations:
    """Test write operations with directory creation."""
    
    def test_write_text_with_makedirs(self, mock_filesystem, temp_sandbox_dir):
        """Test write_text method with automatic directory creation."""
        mock_filesystem.makedirs = Mock()
        fs = PathSandboxedFileSystem(mock_filesystem, str(temp_sandbox_dir))
        
        fs.write_text("subdir/new_file.txt", "content", encoding="utf-8")
        
        expected_path = fs._resolve_path("subdir/new_file.txt")
        expected_parent = str(Path(expected_path).parent)
        
        mock_filesystem.makedirs.assert_called_once_with(expected_parent, exist_ok=True)
        mock_filesystem.write_text.assert_called_once_with(expected_path, "content", encoding="utf-8")
    
    def test_write_text_without_makedirs(self, mock_filesystem, temp_sandbox_dir):
        """Test write_text method when filesystem doesn't support makedirs."""
        # Don't add makedirs method to mock_filesystem
        fs = PathSandboxedFileSystem(mock_filesystem, str(temp_sandbox_dir))
        
        fs.write_text("test.txt", "content")
        
        expected_path = fs._resolve_path("test.txt")
        mock_filesystem.write_text.assert_called_once_with(expected_path, "content", encoding="utf-8")
    
    def test_write_text_makedirs_fails(self, mock_filesystem, temp_sandbox_dir):
        """Test write_text method when makedirs fails."""
        mock_filesystem.makedirs = Mock(side_effect=Exception("Permission denied"))
        fs = PathSandboxedFileSystem(mock_filesystem, str(temp_sandbox_dir))
        
        # Should continue with write even if makedirs fails
        fs.write_text("test.txt", "content")
        
        expected_path = fs._resolve_path("test.txt")
        mock_filesystem.write_text.assert_called_once_with(expected_path, "content", encoding="utf-8")
    
    def test_write_bytes_with_makedirs(self, mock_filesystem, temp_sandbox_dir):
        """Test write_bytes method with automatic directory creation."""
        mock_filesystem.makedirs = Mock()
        fs = PathSandboxedFileSystem(mock_filesystem, str(temp_sandbox_dir))
        
        fs.write_bytes("subdir/new_file.bin", b"content")
        
        expected_path = fs._resolve_path("subdir/new_file.bin")
        expected_parent = str(Path(expected_path).parent)
        
        mock_filesystem.makedirs.assert_called_once_with(expected_parent, exist_ok=True)
        mock_filesystem.write_bytes.assert_called_once_with(expected_path, b"content")


class TestDirectoryOperations:
    """Test directory operation methods."""
    
    def test_mkdir_with_create_parents(self, mock_filesystem, temp_sandbox_dir):
        """Test mkdir method with parent creation."""
        mock_filesystem.makedirs = Mock()
        fs = PathSandboxedFileSystem(mock_filesystem, str(temp_sandbox_dir))
        
        fs.mkdir("new/nested/dir", create_parents=True, mode=0o755)
        
        expected_path = fs._resolve_path("new/nested/dir")
        mock_filesystem.makedirs.assert_called_once_with(expected_path, exist_ok=True, mode=0o755)
    
    def test_mkdir_without_create_parents(self, mock_filesystem, temp_sandbox_dir):
        """Test mkdir method without parent creation."""
        fs = PathSandboxedFileSystem(mock_filesystem, str(temp_sandbox_dir))
        
        fs.mkdir("newdir", create_parents=False, mode=0o755)
        
        expected_path = fs._resolve_path("newdir")
        mock_filesystem.mkdir.assert_called_once_with(expected_path, mode=0o755)
    
    def test_mkdir_no_makedirs_support(self, mock_filesystem, temp_sandbox_dir):
        """Test mkdir method when filesystem doesn't support makedirs."""
        # Don't add makedirs method
        fs = PathSandboxedFileSystem(mock_filesystem, str(temp_sandbox_dir))
        
        fs.mkdir("newdir", create_parents=True)
        
        expected_path = fs._resolve_path("newdir")
        mock_filesystem.mkdir.assert_called_once_with(expected_path)
    
    def test_makedirs(self, mock_filesystem, temp_sandbox_dir):
        """Test makedirs method."""
        fs = PathSandboxedFileSystem(mock_filesystem, str(temp_sandbox_dir))
        
        fs.makedirs("new/nested/dir", exist_ok=True, mode=0o755)
        
        expected_path = fs._resolve_path("new/nested/dir")
        mock_filesystem.makedirs.assert_called_once_with(expected_path, exist_ok=True, mode=0o755)
    
    def test_remove(self, mock_filesystem, temp_sandbox_dir):
        """Test remove method."""
        fs = PathSandboxedFileSystem(mock_filesystem, str(temp_sandbox_dir))
        
        fs.remove("file.txt")
        
        expected_path = fs._resolve_path("file.txt")
        mock_filesystem.rm_file.assert_called_once_with(expected_path)
    
    def test_rm(self, mock_filesystem, temp_sandbox_dir):
        """Test rm method."""
        fs = PathSandboxedFileSystem(mock_filesystem, str(temp_sandbox_dir))
        
        fs.rm("dir", recursive=True, force=True)
        
        expected_path = fs._resolve_path("dir")
        mock_filesystem.rm.assert_called_once_with(expected_path, recursive=True, force=True)
    
    def test_rmdir(self, mock_filesystem, temp_sandbox_dir):
        """Test rmdir method."""
        fs = PathSandboxedFileSystem(mock_filesystem, str(temp_sandbox_dir))
        
        fs.rmdir("emptydir")
        
        expected_path = fs._resolve_path("emptydir")
        mock_filesystem.rmdir.assert_called_once_with(expected_path)


class TestFileTransferOperations:
    """Test file transfer operation methods."""
    
    def test_copy(self, mock_filesystem, temp_sandbox_dir):
        """Test copy method."""
        fs = PathSandboxedFileSystem(mock_filesystem, str(temp_sandbox_dir))
        
        fs.copy("source.txt", "dest.txt", overwrite=True)
        
        expected_src = fs._resolve_path("source.txt")
        expected_dst = fs._resolve_path("dest.txt")
        mock_filesystem.copy.assert_called_once_with(expected_src, expected_dst, overwrite=True)
    
    def test_move(self, mock_filesystem, temp_sandbox_dir):
        """Test move method."""
        fs = PathSandboxedFileSystem(mock_filesystem, str(temp_sandbox_dir))
        
        fs.move("source.txt", "dest.txt", overwrite=True)
        
        expected_src = fs._resolve_path("source.txt")
        expected_dst = fs._resolve_path("dest.txt")
        mock_filesystem.move.assert_called_once_with(expected_src, expected_dst, overwrite=True)
    
    def test_get_file(self, mock_filesystem, temp_sandbox_dir):
        """Test get_file method (download)."""
        fs = PathSandboxedFileSystem(mock_filesystem, str(temp_sandbox_dir))
        
        fs.get_file("remote.txt", "/local/path.txt", callback=None)
        
        expected_remote = fs._resolve_path("remote.txt")
        mock_filesystem.get_file.assert_called_once_with(expected_remote, "/local/path.txt", callback=None)
    
    def test_put_file(self, mock_filesystem, temp_sandbox_dir):
        """Test put_file method (upload)."""
        fs = PathSandboxedFileSystem(mock_filesystem, str(temp_sandbox_dir))
        
        fs.put_file("/local/path.txt", "remote.txt", callback=None)
        
        expected_remote = fs._resolve_path("remote.txt")
        mock_filesystem.put_file.assert_called_once_with("/local/path.txt", expected_remote, callback=None)
    
    def test_touch(self, mock_filesystem, temp_sandbox_dir):
        """Test touch method with directory creation."""
        mock_filesystem.makedirs = Mock()
        fs = PathSandboxedFileSystem(mock_filesystem, str(temp_sandbox_dir))
        
        fs.touch("subdir/newfile.txt", truncate=True)
        
        expected_path = fs._resolve_path("subdir/newfile.txt")
        expected_parent = str(Path(expected_path).parent)
        
        mock_filesystem.makedirs.assert_called_once_with(expected_parent, exist_ok=True)
        mock_filesystem.touch.assert_called_once_with(expected_path, truncate=True)


class TestDelegationAndCompatibility:
    """Test delegation to underlying filesystem and compatibility."""
    
    def test_getattr_delegation(self, mock_filesystem, temp_sandbox_dir):
        """Test that unknown attributes are delegated to underlying filesystem."""
        mock_filesystem.custom_method = Mock(return_value="custom result")
        fs = PathSandboxedFileSystem(mock_filesystem, str(temp_sandbox_dir))
        
        result = fs.custom_method("arg1", kwarg="value")
        
        assert result == "custom result"
        mock_filesystem.custom_method.assert_called_once_with("arg1", kwarg="value")
    
    def test_protocol_delegation(self, mock_filesystem):
        """Test protocol property delegation."""
        mock_filesystem.protocol = ["s3", "s3a"]
        fs = PathSandboxedFileSystem(mock_filesystem, "/test")
        
        assert fs.protocol == ["s3", "s3a"]
    
    def test_no_protocol_fallback(self, temp_sandbox_dir):
        """Test fallback when underlying filesystem has no protocol."""
        mock_fs_no_protocol = Mock()
        # Explicitly don't set protocol attribute
        fs = PathSandboxedFileSystem(mock_fs_no_protocol, str(temp_sandbox_dir))
        
        assert fs.protocol == "file"


class TestIntegrationWithRealFilesystem:
    """Integration tests with real filesystem operations."""
    
    def test_real_file_operations(self, real_filesystem, temp_sandbox_dir):
        """Test actual file operations with real filesystem."""
        fs = PathSandboxedFileSystem(real_filesystem, str(temp_sandbox_dir))
        
        # Test file exists
        assert fs.exists("file1.txt") is True
        assert fs.exists("nonexistent.txt") is False
        
        # Test file properties
        assert fs.isfile("file1.txt") is True
        assert fs.isdir("file1.txt") is False
        assert fs.isdir("subdir") is True
        
        # Test reading file
        content = fs.read_text("file1.txt")
        assert content == "content1"
        
        # Test listing directory
        items = fs.listdir("")
        assert "file1.txt" in items
        assert "file2.txt" in items
        assert "subdir" in items
    
    def test_real_write_operations(self, real_filesystem, temp_sandbox_dir):
        """Test actual write operations with real filesystem."""
        fs = PathSandboxedFileSystem(real_filesystem, str(temp_sandbox_dir))
        
        # Write new file
        fs.write_text("new_file.txt", "new content")
        assert fs.exists("new_file.txt") is True
        assert fs.read_text("new_file.txt") == "new content"
        
        # Write to subdirectory (should create if needed)
        fs.write_text("new_subdir/nested_file.txt", "nested content")
        assert fs.exists("new_subdir/nested_file.txt") is True
        assert fs.read_text("new_subdir/nested_file.txt") == "nested content"
    
    def test_real_directory_operations(self, real_filesystem, temp_sandbox_dir):
        """Test actual directory operations with real filesystem."""
        fs = PathSandboxedFileSystem(real_filesystem, str(temp_sandbox_dir))
        
        # Create directory
        fs.mkdir("new_directory")
        assert fs.isdir("new_directory") is True
        
        # Create nested directories
        fs.makedirs("deep/nested/structure")
        assert fs.isdir("deep/nested/structure") is True
        
        # Touch file in nested directory
        fs.touch("deep/nested/test.txt")
        assert fs.exists("deep/nested/test.txt") is True
    
    def test_real_security_constraints(self, real_filesystem, temp_sandbox_dir):
        """Test security constraints with real filesystem."""
        fs = PathSandboxedFileSystem(real_filesystem, str(temp_sandbox_dir))
        
        # Test that we can't escape the sandbox
        with pytest.raises(PathValidationError):
            fs.exists("../../etc/passwd")
        
        with pytest.raises(PathValidationError):
            fs.write_text("../../../tmp/malicious.txt", "bad content")
        
        # Test that absolute paths are treated as relative
        fs.write_text("/safe_file.txt", "safe content")
        assert fs.exists("safe_file.txt") is True  # Should exist as relative path
        
        # Verify the file is actually in the sandbox
        full_path = temp_sandbox_dir / "safe_file.txt"
        assert full_path.exists()


class TestErrorHandling:
    """Test error handling scenarios."""
    
    def test_path_validation_error_details(self, mock_filesystem, temp_sandbox_dir):
        """Test detailed error messages for path validation failures."""
        fs = PathSandboxedFileSystem(mock_filesystem, str(temp_sandbox_dir))
        
        with pytest.raises(PathValidationError) as exc_info:
            fs._resolve_path("../../../etc/passwd")
        
        error_msg = str(exc_info.value)
        assert "outside the allowed base path" in error_msg
        assert str(temp_sandbox_dir) in error_msg
    
    def test_underlying_filesystem_errors(self, mock_filesystem, temp_sandbox_dir):
        """Test that underlying filesystem errors are propagated."""
        mock_filesystem.exists.side_effect = PermissionError("Access denied")
        fs = PathSandboxedFileSystem(mock_filesystem, str(temp_sandbox_dir))
        
        with pytest.raises(PermissionError) as exc_info:
            fs.exists("test.txt")
        
        assert "Access denied" in str(exc_info.value)
    
    def test_path_type_handling(self, mock_filesystem, temp_sandbox_dir):
        """Test handling of different path types (str, Path)."""
        fs = PathSandboxedFileSystem(mock_filesystem, str(temp_sandbox_dir))
        
        # Test with Path object
        path_obj = Path("test.txt")
        fs.exists(path_obj)
        
        expected_path = fs._resolve_path("test.txt")
        mock_filesystem.exists.assert_called_with(expected_path)
        
        # Test with string path
        fs.exists("test.txt")
        mock_filesystem.exists.assert_called_with(expected_path)


class TestEdgeCases:
    """Test edge cases and boundary conditions."""
    
    def test_empty_path_handling(self, mock_filesystem, temp_sandbox_dir):
        """Test handling of empty paths."""
        fs = PathSandboxedFileSystem(mock_filesystem, str(temp_sandbox_dir))
        
        # Empty path should use base path for ls and walk
        fs.ls("")
        mock_filesystem.ls.assert_called_with(fs._base_path, detail=True)
        
        fs.walk("")
        mock_filesystem.walk.assert_called_with(fs._base_path)
    
    def test_root_path_sandbox(self, mock_filesystem):
        """Test sandboxing with root path."""
        fs = PathSandboxedFileSystem(mock_filesystem, "/")
        
        # Should allow any path when base is root
        result = fs._resolve_path("any/path")
        assert result == str(Path("/any/path").resolve())
    
    def test_relative_base_path_resolution(self, mock_filesystem):
        """Test that relative base paths are resolved correctly."""
        with patch('os.getcwd', return_value="/current/dir"):
            fs = PathSandboxedFileSystem(mock_filesystem, "relative/path")
            
            # Base path should be resolved relative to current directory
            assert fs._base_path.startswith("/current/dir")
            assert "relative" in fs._base_path
            assert "path" in fs._base_path
    
    def test_symlink_handling(self, mock_filesystem, temp_sandbox_dir):
        """Test that symlinks are resolved through Path.resolve()."""
        fs = PathSandboxedFileSystem(mock_filesystem, str(temp_sandbox_dir))
        
        # The _resolve_path method uses Path.resolve() which handles symlinks
        # This test verifies the path resolution logic works correctly
        result = fs._resolve_path("./file.txt")
        expected = str(Path(temp_sandbox_dir / "file.txt").resolve())
        assert result == expected
    
    def test_case_sensitivity_handling(self, mock_filesystem, temp_sandbox_dir):
        """Test path handling on different case sensitivity systems."""
        fs = PathSandboxedFileSystem(mock_filesystem, str(temp_sandbox_dir))
        
        # Path resolution should work consistently regardless of case sensitivity
        result1 = fs._resolve_path("File.txt")
        result2 = fs._resolve_path("file.txt")
        
        # Both should resolve to valid paths within sandbox
        assert result1.startswith(str(temp_sandbox_dir))
        assert result2.startswith(str(temp_sandbox_dir))