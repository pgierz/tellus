"""
Comprehensive test suite for Location path sandboxing functionality.

This test suite verifies that the PathSandboxedFileSystem correctly enforces
path boundaries and prevents operations outside the configured location path.
"""

import shutil
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from tellus.location import (Location, LocationKind, PathSandboxedFileSystem,
                             PathValidationError)


class TestPathSandboxedFileSystem:
    """Test PathSandboxedFileSystem wrapper functionality."""

    def setup_method(self):
        """Set up test environment with temporary directories."""
        self.temp_dir = Path(tempfile.mkdtemp())
        self.outside_dir = Path(tempfile.mkdtemp())
        
        # Create test structure inside temp_dir
        self.test_file = self.temp_dir / "test.txt"
        self.test_subdir = self.temp_dir / "subdir"
        self.test_subdir.mkdir()
        self.test_subfile = self.test_subdir / "subfile.txt"
        
        # Create files
        self.test_file.write_text("test content")
        self.test_subfile.write_text("sub content")
        
        # Create test structure outside temp_dir (should be inaccessible)
        self.outside_file = self.outside_dir / "outside.txt"
        self.outside_file.write_text("outside content")

    def teardown_method(self):
        """Clean up temporary directories."""
        shutil.rmtree(self.temp_dir, ignore_errors=True)
        shutil.rmtree(self.outside_dir, ignore_errors=True)

    def test_sandboxed_filesystem_basic_operations(self):
        """Test basic file operations within sandbox."""
        import fsspec
        base_fs = fsspec.filesystem("file")
        sandboxed_fs = PathSandboxedFileSystem(base_fs, str(self.temp_dir))
        
        # Test exists
        assert sandboxed_fs.exists("test.txt")
        assert sandboxed_fs.exists("subdir")
        assert sandboxed_fs.exists("subdir/subfile.txt")
        assert not sandboxed_fs.exists("nonexistent.txt")
        
        # Test isfile/isdir
        assert sandboxed_fs.isfile("test.txt")
        assert not sandboxed_fs.isfile("subdir")
        assert sandboxed_fs.isdir("subdir")
        assert not sandboxed_fs.isdir("test.txt")
        
        # Test read operations
        assert sandboxed_fs.read_text("test.txt") == "test content"
        assert sandboxed_fs.read_text("subdir/subfile.txt") == "sub content"

    def test_sandboxed_filesystem_write_operations(self):
        """Test write operations within sandbox."""
        import fsspec
        base_fs = fsspec.filesystem("file")
        sandboxed_fs = PathSandboxedFileSystem(base_fs, str(self.temp_dir))
        
        # Test write_text
        sandboxed_fs.write_text("new_file.txt", "new content")
        assert (self.temp_dir / "new_file.txt").read_text() == "new content"
        
        # Test write in subdirectory (with auto-creation)
        sandboxed_fs.write_text("newdir/nested_file.txt", "nested content")
        assert (self.temp_dir / "newdir" / "nested_file.txt").read_text() == "nested content"

    def test_sandboxed_filesystem_directory_traversal_prevention(self):
        """Test that directory traversal attacks are prevented."""
        import fsspec
        base_fs = fsspec.filesystem("file")
        sandboxed_fs = PathSandboxedFileSystem(base_fs, str(self.temp_dir))
        
        # Test various directory traversal attempts
        with pytest.raises(PathValidationError):
            sandboxed_fs.read_text("../outside.txt")
        
        with pytest.raises(PathValidationError):
            sandboxed_fs.read_text("../../outside.txt")
        
        with pytest.raises(PathValidationError):
            sandboxed_fs.write_text("../malicious.txt", "bad content")
        
        with pytest.raises(PathValidationError):
            sandboxed_fs.exists("../outside.txt")
        
        # Test that the outside file still exists but is not accessible
        assert self.outside_file.exists()
        
        # Verify no malicious files were created
        assert not (self.temp_dir.parent / "malicious.txt").exists()

    def test_sandboxed_filesystem_absolute_path_handling(self):
        """Test handling of absolute paths (should be made relative to base path)."""
        import fsspec
        base_fs = fsspec.filesystem("file")
        sandboxed_fs = PathSandboxedFileSystem(base_fs, str(self.temp_dir))
        
        # Absolute paths should be treated as relative to base path
        sandboxed_fs.write_text("/absolute_looking.txt", "absolute content")
        assert (self.temp_dir / "absolute_looking.txt").read_text() == "absolute content"
        
        # Should not create file at root
        assert not Path("/absolute_looking.txt").exists()

    def test_sandboxed_filesystem_glob_operations(self):
        """Test glob operations within sandbox."""
        import fsspec
        base_fs = fsspec.filesystem("file")
        sandboxed_fs = PathSandboxedFileSystem(base_fs, str(self.temp_dir))
        
        # Create additional test files
        (self.temp_dir / "file1.log").touch()
        (self.temp_dir / "file2.log").touch()
        (self.temp_dir / "file3.txt").touch()
        
        # Test glob patterns
        txt_files = sandboxed_fs.glob("*.txt")
        assert len(txt_files) >= 2  # test.txt and file3.txt
        
        log_files = sandboxed_fs.glob("*.log")
        assert len(log_files) == 2
        
        all_files = sandboxed_fs.glob("*")
        assert len(all_files) >= 5  # Multiple files and subdirectories

    def test_sandboxed_filesystem_no_base_path(self):
        """Test sandboxed filesystem with empty base path (should work normally)."""
        import fsspec
        base_fs = fsspec.filesystem("file")
        sandboxed_fs = PathSandboxedFileSystem(base_fs, "")
        
        # Should delegate directly to underlying filesystem
        # Test with a temporary file in current directory
        temp_file = Path("temp_test.txt")
        try:
            sandboxed_fs.write_text("temp_test.txt", "temp content")
            assert temp_file.read_text() == "temp content"
        finally:
            temp_file.unlink(missing_ok=True)


class TestLocationPathSandboxing:
    """Test Location class path sandboxing integration."""

    def setup_method(self):
        """Set up test environment."""
        Location._locations = {}  # Clear locations
        self.temp_dir = Path(tempfile.mkdtemp())

    def teardown_method(self):
        """Clean up test environment."""
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_location_fs_sandboxing(self):
        """Test that Location.fs returns a properly sandboxed filesystem."""
        location = Location(
            name="sandboxed_location",
            kinds=[LocationKind.DISK],
            config={
                "protocol": "file",
                "path": str(self.temp_dir)
            }
        )
        
        # Verify fs is sandboxed
        assert isinstance(location.fs, PathSandboxedFileSystem)
        assert location.fs.base_path.rstrip("/") == str(self.temp_dir.resolve()).rstrip("/")

    def test_location_filesystem_operations_are_sandboxed(self):
        """Test that filesystem operations through Location.fs are sandboxed."""
        location = Location(
            name="test_sandboxed_ops",
            kinds=[LocationKind.DISK],
            config={
                "protocol": "file",
                "path": str(self.temp_dir)
            }
        )
        
        # Write a file using the sandboxed fs
        location.fs.write_text("sandboxed_file.txt", "sandboxed content")
        
        # Verify file was created in the correct location
        expected_file = self.temp_dir / "sandboxed_file.txt"
        assert expected_file.exists()
        assert expected_file.read_text() == "sandboxed content"
        
        # Verify file was NOT created in current working directory
        cwd_file = Path("sandboxed_file.txt")
        assert not cwd_file.exists()

    def test_location_directory_traversal_protection(self):
        """Test that Location protects against directory traversal."""
        location = Location(
            name="protected_location",
            kinds=[LocationKind.DISK],
            config={
                "protocol": "file",
                "path": str(self.temp_dir)
            }
        )
        
        # Attempt directory traversal attacks
        with pytest.raises(PathValidationError):
            location.fs.write_text("../malicious.txt", "bad content")
        
        with pytest.raises(PathValidationError):
            location.fs.read_text("../../etc/passwd")
        
        # Verify no malicious files were created
        assert not (self.temp_dir.parent / "malicious.txt").exists()

    def test_location_get_method_sandboxing(self):
        """Test that Location.get method works with sandboxed filesystem."""
        # Create a source file
        source_file = self.temp_dir / "source.txt"
        source_file.write_text("source content")
        
        location = Location(
            name="get_test_location",
            kinds=[LocationKind.DISK],
            config={
                "protocol": "file",
                "path": str(self.temp_dir)
            }
        )
        
        # Use get method to download the file
        with tempfile.TemporaryDirectory() as download_dir:
            local_path = Path(download_dir) / "downloaded.txt"
            result = location.get("source.txt", str(local_path), show_progress=False)
            
            assert result == str(local_path)
            assert local_path.exists()
            assert local_path.read_text() == "source content"

    def test_location_find_files_sandboxing(self):
        """Test that Location.find_files works with sandboxed filesystem."""
        # Create test files
        (self.temp_dir / "file1.txt").write_text("content1")
        (self.temp_dir / "file2.txt").write_text("content2")
        (self.temp_dir / "file3.log").write_text("content3")
        
        subdir = self.temp_dir / "subdir"
        subdir.mkdir()
        (subdir / "file4.txt").write_text("content4")
        
        location = Location(
            name="find_test_location",
            kinds=[LocationKind.DISK],
            config={
                "protocol": "file",
                "path": str(self.temp_dir)
            }
        )
        
        # Test non-recursive find
        txt_files = list(location.find_files("*.txt"))
        assert len(txt_files) == 2
        
        # Test recursive find
        txt_files_recursive = list(location.find_files("*.txt", recursive=True))
        assert len(txt_files_recursive) == 3
        
        # Test with base_path
        subdir_files = list(location.find_files("*.txt", base_path="subdir"))
        assert len(subdir_files) == 1


class TestLocationCompatibility:
    """Test that Location maintains compatibility with existing functionality."""

    def setup_method(self):
        """Set up test environment."""
        Location._locations = {}
        self.temp_dir = Path(tempfile.mkdtemp())

    def teardown_method(self):
        """Clean up test environment."""
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_location_without_path_config(self):
        """Test Location behavior when no path is configured (should work in CWD)."""
        location = Location(
            name="no_path_location",
            kinds=[LocationKind.DISK],
            config={"protocol": "file"}
        )
        
        # Should create a sandboxed filesystem with empty base path
        assert isinstance(location.fs, PathSandboxedFileSystem)
        assert location.fs.base_path == ""
        
        # Should work normally (delegating to underlying filesystem)
        temp_file = Path("temp_compatibility_test.txt")
        try:
            location.fs.write_text("temp_compatibility_test.txt", "test content")
            assert temp_file.read_text() == "test content"
        finally:
            temp_file.unlink(missing_ok=True)

    @patch('fsspec.filesystem')
    def test_location_with_different_protocols(self, mock_fsspec):
        """Test that sandboxing works with different fsspec protocols."""
        mock_fs = MagicMock()
        mock_fsspec.return_value = mock_fs
        
        # Test with different protocols
        protocols = ["file", "ftp", "sftp", "s3"]
        
        for protocol in protocols:
            location = Location(
                name=f"{protocol}_location",
                kinds=[LocationKind.DISK],
                config={
                    "protocol": protocol,
                    "path": "/test/path",
                    "storage_options": {"key": "value"}
                }
            )
            
            # Should create sandboxed filesystem
            fs = location.fs
            assert isinstance(fs, PathSandboxedFileSystem)
            assert fs._fs == mock_fs
            
            # Clear for next iteration
            Location._locations = {}

    def test_location_serialization_with_sandboxing(self):
        """Test that Location serialization still works with sandboxed filesystem."""
        location = Location(
            name="serialization_test",
            kinds=[LocationKind.DISK, LocationKind.COMPUTE],
            config={
                "protocol": "file",
                "path": str(self.temp_dir),
                "storage_options": {"auto_mkdir": True}
            },
            optional=True
        )
        
        # Test to_dict
        location_dict = location.to_dict()
        expected = {
            "name": "serialization_test",
            "kinds": ["DISK", "COMPUTE"],
            "config": {
                "protocol": "file",
                "path": str(self.temp_dir),
                "storage_options": {"auto_mkdir": True}
            },
            "optional": True
        }
        assert location_dict == expected
        
        # Test from_dict
        Location._locations = {}  # Clear
        restored_location = Location.from_dict(location_dict)
        assert restored_location.name == location.name
        assert restored_location.kinds == location.kinds
        assert restored_location.config == location.config
        assert restored_location.optional == location.optional
        
        # Verify filesystem is still sandboxed
        assert isinstance(restored_location.fs, PathSandboxedFileSystem)