"""
Filesystem abstraction implementations for testing.

This module provides concrete implementations of filesystem interfaces
for both real filesystem operations and test doubles.
"""

import io
import shutil
import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock

from .interfaces import FileSystemInterface


class RealFileSystem:
    """Real filesystem implementation for integration tests."""
    
    def exists(self, path: str) -> bool:
        """Check if a path exists."""
        return Path(path).exists()
    
    def read_text(self, path: str) -> str:
        """Read text content from a file."""
        return Path(path).read_text()
    
    def write_text(self, path: str, content: str) -> None:
        """Write text content to a file."""
        path_obj = Path(path)
        path_obj.parent.mkdir(parents=True, exist_ok=True)
        path_obj.write_text(content)
    
    def read_bytes(self, path: str) -> bytes:
        """Read binary content from a file."""
        return Path(path).read_bytes()
    
    def write_bytes(self, path: str, content: bytes) -> None:
        """Write binary content to a file."""
        path_obj = Path(path)
        path_obj.parent.mkdir(parents=True, exist_ok=True)
        path_obj.write_bytes(content)
    
    def mkdir(self, path: str, parents: bool = False, exist_ok: bool = False) -> None:
        """Create a directory."""
        Path(path).mkdir(parents=parents, exist_ok=exist_ok)
    
    def rmtree(self, path: str, ignore_errors: bool = False) -> None:
        """Remove directory tree."""
        shutil.rmtree(path, ignore_errors=ignore_errors)
    
    def list_dir(self, path: str) -> List[str]:
        """List directory contents."""
        return [item.name for item in Path(path).iterdir()]
    
    def size(self, path: str) -> int:
        """Get file size."""
        return Path(path).stat().st_size
    
    @contextmanager
    def open(self, path: str, mode: str = 'r'):
        """Open a file with context manager."""
        with open(path, mode) as f:
            yield f


class InMemoryFileSystem:
    """In-memory filesystem implementation for unit tests."""
    
    def __init__(self):
        """Initialize empty in-memory filesystem."""
        self._files: Dict[str, bytes] = {}
        self._directories: set = {"/"}
    
    def exists(self, path: str) -> bool:
        """Check if a path exists."""
        normalized = self._normalize_path(path)
        return normalized in self._files or normalized in self._directories
    
    def read_text(self, path: str) -> str:
        """Read text content from a file."""
        normalized = self._normalize_path(path)
        if normalized not in self._files:
            raise FileNotFoundError(f"File not found: {path}")
        return self._files[normalized].decode('utf-8')
    
    def write_text(self, path: str, content: str) -> None:
        """Write text content to a file."""
        self.write_bytes(path, content.encode('utf-8'))
    
    def read_bytes(self, path: str) -> bytes:
        """Read binary content from a file."""
        normalized = self._normalize_path(path)
        if normalized not in self._files:
            raise FileNotFoundError(f"File not found: {path}")
        return self._files[normalized]
    
    def write_bytes(self, path: str, content: bytes) -> None:
        """Write binary content to a file."""
        normalized = self._normalize_path(path)
        
        # Ensure parent directories exist
        parent_dir = str(Path(normalized).parent)
        self._ensure_directory_exists(parent_dir)
        
        self._files[normalized] = content
    
    def mkdir(self, path: str, parents: bool = False, exist_ok: bool = False) -> None:
        """Create a directory."""
        normalized = self._normalize_path(path)
        
        if normalized in self._directories:
            if not exist_ok:
                raise FileExistsError(f"Directory already exists: {path}")
            return
        
        if normalized in self._files:
            raise FileExistsError(f"File exists with same name: {path}")
        
        if parents:
            self._ensure_directory_exists(normalized)
        else:
            parent_dir = str(Path(normalized).parent)
            if parent_dir not in self._directories:
                raise FileNotFoundError(f"Parent directory does not exist: {parent_dir}")
            self._directories.add(normalized)
    
    def rmtree(self, path: str, ignore_errors: bool = False) -> None:
        """Remove directory tree."""
        normalized = self._normalize_path(path)
        
        try:
            if normalized not in self._directories:
                if not ignore_errors:
                    raise FileNotFoundError(f"Directory not found: {path}")
                return
            
            # Remove all files and subdirectories
            to_remove = []
            for file_path in self._files:
                if file_path.startswith(normalized + "/") or file_path == normalized:
                    to_remove.append(file_path)
            
            for file_path in to_remove:
                del self._files[file_path]
            
            dirs_to_remove = []
            for dir_path in self._directories:
                if dir_path.startswith(normalized + "/") or dir_path == normalized:
                    dirs_to_remove.append(dir_path)
            
            for dir_path in dirs_to_remove:
                self._directories.remove(dir_path)
                
        except Exception as e:
            if not ignore_errors:
                raise e
    
    def list_dir(self, path: str) -> List[str]:
        """List directory contents."""
        normalized = self._normalize_path(path)
        
        if normalized not in self._directories:
            raise FileNotFoundError(f"Directory not found: {path}")
        
        items = set()
        prefix = normalized + "/" if normalized != "/" else "/"
        
        # Find files in directory
        for file_path in self._files:
            if file_path.startswith(prefix):
                relative = file_path[len(prefix):]
                if "/" not in relative:  # Direct child
                    items.add(relative)
        
        # Find subdirectories
        for dir_path in self._directories:
            if dir_path.startswith(prefix) and dir_path != normalized:
                relative = dir_path[len(prefix):]
                if "/" not in relative:  # Direct child
                    items.add(relative)
        
        return sorted(list(items))
    
    def size(self, path: str) -> int:
        """Get file size."""
        normalized = self._normalize_path(path)
        if normalized not in self._files:
            raise FileNotFoundError(f"File not found: {path}")
        return len(self._files[normalized])
    
    @contextmanager
    def open(self, path: str, mode: str = 'r'):
        """Open a file with context manager."""
        if 'b' in mode:
            if 'r' in mode:
                content = self.read_bytes(path)
                yield io.BytesIO(content)
            elif 'w' in mode:
                buffer = io.BytesIO()
                yield buffer
                self.write_bytes(path, buffer.getvalue())
        else:
            if 'r' in mode:
                content = self.read_text(path)
                yield io.StringIO(content)
            elif 'w' in mode:
                buffer = io.StringIO()
                yield buffer
                self.write_text(path, buffer.getvalue())
    
    def _normalize_path(self, path: str) -> str:
        """Normalize path for consistent storage."""
        return str(Path(path).resolve())
    
    def _ensure_directory_exists(self, path: str) -> None:
        """Ensure directory and all parents exist."""
        path_obj = Path(path)
        parts = []
        
        current = path_obj
        while current != current.parent:
            parts.append(str(current))
            current = current.parent
        
        parts.reverse()
        
        for part in parts:
            if part not in self._directories:
                self._directories.add(part)
    
    def clear(self) -> None:
        """Clear all files and directories (for testing)."""
        self._files.clear()
        self._directories.clear()
        self._directories.add("/")


class MockFileSystem:
    """Mock filesystem for isolated unit tests."""
    
    def __init__(self):
        """Initialize mock filesystem."""
        self.exists = MagicMock(return_value=True)
        self.read_text = MagicMock(return_value="mock content")
        self.write_text = MagicMock()
        self.read_bytes = MagicMock(return_value=b"mock content")
        self.write_bytes = MagicMock()
        self.mkdir = MagicMock()
        self.rmtree = MagicMock()
        self.list_dir = MagicMock(return_value=["file1.txt", "file2.txt"])
        self.size = MagicMock(return_value=1024)
        self._open_mock = MagicMock()
    
    @contextmanager
    def open(self, path: str, mode: str = 'r'):
        """Mock file open with context manager."""
        mock_file = MagicMock()
        mock_file.read.return_value = "mock content" if 'b' not in mode else b"mock content"
        mock_file.write = MagicMock()
        
        self._open_mock(path, mode)
        yield mock_file
    
    def configure_exists(self, path_exists_mapping: Dict[str, bool]) -> None:
        """Configure exists behavior for specific paths."""
        self.exists.side_effect = lambda path: path_exists_mapping.get(path, True)
    
    def configure_read_text(self, path_content_mapping: Dict[str, str]) -> None:
        """Configure read_text behavior for specific paths."""
        self.read_text.side_effect = lambda path: path_content_mapping.get(path, "default content")
    
    def configure_list_dir(self, path_contents_mapping: Dict[str, List[str]]) -> None:
        """Configure list_dir behavior for specific paths."""
        self.list_dir.side_effect = lambda path: path_contents_mapping.get(path, [])


class TemporaryFileSystem:
    """Temporary filesystem for integration tests that auto-cleans."""
    
    def __init__(self):
        """Initialize temporary filesystem."""
        self._temp_dir = None
        self._real_fs = RealFileSystem()
    
    def __enter__(self):
        """Enter context manager."""
        self._temp_dir = Path(tempfile.mkdtemp())
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit context manager and cleanup."""
        if self._temp_dir and self._temp_dir.exists():
            shutil.rmtree(self._temp_dir, ignore_errors=True)
        self._temp_dir = None
    
    def get_temp_path(self, relative_path: str = "") -> str:
        """Get absolute path within temporary directory."""
        if not self._temp_dir:
            raise RuntimeError("TemporaryFileSystem not initialized")
        return str(self._temp_dir / relative_path)
    
    def exists(self, path: str) -> bool:
        """Check if a path exists."""
        return self._real_fs.exists(self._resolve_path(path))
    
    def read_text(self, path: str) -> str:
        """Read text content from a file."""
        return self._real_fs.read_text(self._resolve_path(path))
    
    def write_text(self, path: str, content: str) -> None:
        """Write text content to a file."""
        self._real_fs.write_text(self._resolve_path(path), content)
    
    def read_bytes(self, path: str) -> bytes:
        """Read binary content from a file."""
        return self._real_fs.read_bytes(self._resolve_path(path))
    
    def write_bytes(self, path: str, content: bytes) -> None:
        """Write binary content to a file."""
        self._real_fs.write_bytes(self._resolve_path(path), content)
    
    def mkdir(self, path: str, parents: bool = False, exist_ok: bool = False) -> None:
        """Create a directory."""
        self._real_fs.mkdir(self._resolve_path(path), parents, exist_ok)
    
    def rmtree(self, path: str, ignore_errors: bool = False) -> None:
        """Remove directory tree."""
        self._real_fs.rmtree(self._resolve_path(path), ignore_errors)
    
    def list_dir(self, path: str) -> List[str]:
        """List directory contents."""
        return self._real_fs.list_dir(self._resolve_path(path))
    
    def size(self, path: str) -> int:
        """Get file size."""
        return self._real_fs.size(self._resolve_path(path))
    
    @contextmanager
    def open(self, path: str, mode: str = 'r'):
        """Open a file with context manager."""
        with self._real_fs.open(self._resolve_path(path), mode) as f:
            yield f
    
    def _resolve_path(self, path: str) -> str:
        """Resolve path relative to temp directory."""
        if not self._temp_dir:
            raise RuntimeError("TemporaryFileSystem not initialized")
        
        if Path(path).is_absolute():
            return path
        else:
            return str(self._temp_dir / path)


class FileSystemFactory:
    """Factory for creating filesystem implementations."""
    
    @staticmethod
    def create_real() -> RealFileSystem:
        """Create real filesystem implementation."""
        return RealFileSystem()
    
    @staticmethod
    def create_in_memory() -> InMemoryFileSystem:
        """Create in-memory filesystem implementation."""
        return InMemoryFileSystem()
    
    @staticmethod
    def create_mock() -> MockFileSystem:
        """Create mock filesystem implementation."""
        return MockFileSystem()
    
    @staticmethod
    def create_temporary() -> TemporaryFileSystem:
        """Create temporary filesystem implementation."""
        return TemporaryFileSystem()