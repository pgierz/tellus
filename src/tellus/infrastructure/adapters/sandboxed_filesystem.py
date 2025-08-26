"""
PathSandboxedFileSystem: A secure wrapper around fsspec filesystems.

This module provides a filesystem wrapper that enforces path sandboxing,
ensuring all operations are constrained within a configured base path.
This prevents the filesystem from operating in unexpected directories
and provides security against directory traversal attacks.
"""

import os
from pathlib import Path, PurePosixPath
from typing import Any, Dict, List, Optional, Union
from urllib.parse import urljoin, urlparse

import fsspec
from fsspec.spec import AbstractFileSystem


class PathValidationError(Exception):
    """Raised when a path operation would escape the sandboxed directory."""
    pass


class PathSandboxedFileSystem:
    """
    A wrapper around fsspec.AbstractFileSystem that enforces path sandboxing.
    
    All file operations are constrained to operate within a configured base path.
    This provides security and ensures Location objects operate within their
    configured directory boundaries.
    
    Design Principles:
    - Dependency Inversion: Wraps any AbstractFileSystem implementation
    - Single Responsibility: Handles only path sandboxing concerns
    - Security First: Validates and normalizes all paths to prevent escapes
    - Transparent: Maintains the same interface as AbstractFileSystem
    """
    
    def __init__(self, base_filesystem: AbstractFileSystem, base_path: str = ""):
        """
        Initialize the sandboxed filesystem.
        
        Args:
            base_filesystem: The underlying fsspec filesystem to wrap
            base_path: The base path that constrains all operations
        """
        self._fs = base_filesystem
        self._base_path = self._normalize_base_path(base_path)
        self._protocol = getattr(base_filesystem, 'protocol', 'file')
    
    def _normalize_base_path(self, base_path: str) -> str:
        """
        Normalize the base path for consistent path resolution.
        
        Args:
            base_path: The base path to normalize
            
        Returns:
            Normalized base path
        """
        if not base_path:
            return ""
        
        # Convert to Path for normalization, then back to string
        # This handles things like "path/../other" -> "other"
        normalized = str(Path(base_path).resolve())
        
        # Ensure it ends with separator for consistent joining
        if not normalized.endswith(os.sep) and normalized != os.sep:
            normalized += os.sep
            
        return normalized
    
    def _resolve_path(self, path: Union[str, Path]) -> str:
        """
        Resolve a relative path against the base path and validate it's within bounds.
        
        Args:
            path: The path to resolve (relative or absolute)
            
        Returns:
            The fully resolved path within the sandboxed directory
            
        Raises:
            PathValidationError: If the resolved path would escape the sandbox
        """
        if not self._base_path:
            return str(path)
        
        # Convert to string for processing
        path_str = str(path)
        
        # Normalize the path for comparison
        normalized_path = str(Path(path_str).resolve())
        
        # Check if the path is already within our base path
        # (e.g., from a previous glob result)
        if self._is_within_base_path(normalized_path):
            return normalized_path
        
        # Handle absolute paths - they should be relative to base_path for safety
        if os.path.isabs(path_str):
            # For safety, treat absolute paths as relative to base_path
            # This prevents accidental access to system directories
            path_str = path_str.lstrip(os.sep)
        
        # Join with base path
        if self._base_path.endswith(os.sep):
            resolved = self._base_path + path_str
        else:
            resolved = os.path.join(self._base_path, path_str)
        
        # Normalize to handle ".." and "." components
        resolved = str(Path(resolved).resolve())
        
        # Validate that the resolved path is still within the base path
        if not self._is_within_base_path(resolved):
            raise PathValidationError(
                f"Path '{path}' resolves to '{resolved}' which is outside "
                f"the allowed base path '{self._base_path}'"
            )
        
        return resolved
    
    def _is_within_base_path(self, resolved_path: str) -> bool:
        """
        Check if a resolved path is within the base path boundaries.
        
        Args:
            resolved_path: The fully resolved path to check
            
        Returns:
            True if the path is within bounds, False otherwise
        """
        if not self._base_path:
            return True
        
        # Normalize both paths for comparison
        base_real = str(Path(self._base_path).resolve())
        path_real = str(Path(resolved_path).resolve())
        
        # Ensure base path ends with separator
        if not base_real.endswith(os.sep) and base_real != os.sep:
            base_real += os.sep
        
        # Check if the path starts with the base path
        return path_real.startswith(base_real) or path_real == base_real.rstrip(os.sep)
    
    def _resolve_paths(self, paths: Union[str, List[str]]) -> Union[str, List[str]]:
        """
        Resolve multiple paths, maintaining the input type.
        
        Args:
            paths: Single path or list of paths to resolve
            
        Returns:
            Resolved path(s) in the same format as input
        """
        if isinstance(paths, (list, tuple)):
            return [self._resolve_path(p) for p in paths]
        else:
            return self._resolve_path(paths)
    
    # File operation methods - all delegate to underlying filesystem with resolved paths
    
    def exists(self, path: Union[str, Path]) -> bool:
        """Check if a file or directory exists."""
        return self._fs.exists(self._resolve_path(path))
    
    def isfile(self, path: Union[str, Path]) -> bool:
        """Check if path is a file."""
        return self._fs.isfile(self._resolve_path(path))
    
    def isdir(self, path: Union[str, Path]) -> bool:
        """Check if path is a directory."""
        return self._fs.isdir(self._resolve_path(path))
    
    def ls(self, path: Union[str, Path] = "", detail: bool = True) -> List:
        """List directory contents."""
        resolved_path = self._resolve_path(path) if path else self._base_path
        return self._fs.ls(resolved_path, detail=detail)
    
    def listdir(self, path: Union[str, Path] = "") -> List[str]:
        """List directory contents (names only)."""
        resolved_path = self._resolve_path(path) if path else self._base_path
        return self._fs.listdir(resolved_path)
    
    def glob(self, pattern: str, **kwargs) -> List[str]:
        """Find files matching a pattern."""
        # Resolve pattern relative to base path
        if self._base_path and not os.path.isabs(pattern):
            if self._base_path.endswith(os.sep):
                resolved_pattern = self._base_path + pattern
            else:
                resolved_pattern = os.path.join(self._base_path, pattern)
        else:
            resolved_pattern = pattern
        return self._fs.glob(resolved_pattern, **kwargs)
    
    def walk(self, path: Union[str, Path] = "", **kwargs):
        """Walk directory tree."""
        resolved_path = self._resolve_path(path) if path else self._base_path
        return self._fs.walk(resolved_path, **kwargs)
    
    def find(self, path: Union[str, Path] = "", **kwargs) -> List[str]:
        """Find files in directory tree."""
        resolved_path = self._resolve_path(path) if path else self._base_path
        return self._fs.find(resolved_path, **kwargs)
    
    def info(self, path: Union[str, Path]) -> Dict:
        """Get file/directory information."""
        return self._fs.info(self._resolve_path(path))
    
    def size(self, path: Union[str, Path]) -> int:
        """Get file size in bytes."""
        return self._fs.size(self._resolve_path(path))
    
    def open(self, path: Union[str, Path], mode: str = "rb", **kwargs):
        """Open a file."""
        return self._fs.open(self._resolve_path(path), mode, **kwargs)
    
    def read_text(self, path: Union[str, Path], encoding: str = "utf-8", **kwargs) -> str:
        """Read file as text."""
        return self._fs.read_text(self._resolve_path(path), encoding=encoding, **kwargs)
    
    def read_bytes(self, path: Union[str, Path], **kwargs) -> bytes:
        """Read file as bytes."""
        return self._fs.read_bytes(self._resolve_path(path), **kwargs)
    
    def write_text(self, path: Union[str, Path], data: str, encoding: str = "utf-8", **kwargs) -> None:
        """Write text to file."""
        resolved_path = self._resolve_path(path)
        # Ensure parent directories exist if the filesystem supports it
        if hasattr(self._fs, 'makedirs'):
            parent_dir = str(Path(resolved_path).parent)
            try:
                self._fs.makedirs(parent_dir, exist_ok=True)
            except Exception:
                # Some filesystems might not support makedirs
                pass
        return self._fs.write_text(resolved_path, data, encoding=encoding, **kwargs)
    
    def write_bytes(self, path: Union[str, Path], data: bytes, **kwargs) -> None:
        """Write bytes to file."""
        resolved_path = self._resolve_path(path)
        # Ensure parent directories exist if the filesystem supports it
        if hasattr(self._fs, 'makedirs'):
            parent_dir = str(Path(resolved_path).parent)
            try:
                self._fs.makedirs(parent_dir, exist_ok=True)
            except Exception:
                pass
        return self._fs.write_bytes(resolved_path, data, **kwargs)
    
    def mkdir(self, path: Union[str, Path], create_parents: bool = True, **kwargs) -> None:
        """Create directory."""
        resolved_path = self._resolve_path(path)
        if create_parents and hasattr(self._fs, 'makedirs'):
            return self._fs.makedirs(resolved_path, exist_ok=True, **kwargs)
        else:
            return self._fs.mkdir(resolved_path, **kwargs)
    
    def makedirs(self, path: Union[str, Path], exist_ok: bool = True, **kwargs) -> None:
        """Create directory and parent directories."""
        return self._fs.makedirs(self._resolve_path(path), exist_ok=exist_ok, **kwargs)
    
    def remove(self, path: Union[str, Path]) -> None:
        """Remove file."""
        return self._fs.rm_file(self._resolve_path(path))
    
    def rm(self, path: Union[str, Path], recursive: bool = False, **kwargs) -> None:
        """Remove file or directory."""
        return self._fs.rm(self._resolve_path(path), recursive=recursive, **kwargs)
    
    def rmdir(self, path: Union[str, Path]) -> None:
        """Remove empty directory."""
        return self._fs.rmdir(self._resolve_path(path))
    
    def copy(self, src: Union[str, Path], dst: Union[str, Path], **kwargs) -> None:
        """Copy file or directory."""
        return self._fs.copy(self._resolve_path(src), self._resolve_path(dst), **kwargs)
    
    def move(self, src: Union[str, Path], dst: Union[str, Path], **kwargs) -> None:
        """Move file or directory."""
        return self._fs.move(self._resolve_path(src), self._resolve_path(dst), **kwargs)
    
    def get(self, remote_path: Union[str, Path], local_path: Union[str, Path], **kwargs) -> None:
        """Download file(s) from remote to local with path resolution."""
        return self._fs.get(self._resolve_path(remote_path), local_path, **kwargs)
    
    def get_file(self, remote_path: Union[str, Path], local_path: Union[str, Path], **kwargs) -> None:
        """Download file from remote to local."""
        return self._fs.get_file(self._resolve_path(remote_path), local_path, **kwargs)
    
    def put(self, local_path: Union[str, Path], remote_path: Union[str, Path], **kwargs) -> None:
        """Upload file(s) from local to remote with path resolution."""
        return self._fs.put(local_path, self._resolve_path(remote_path), **kwargs)
    
    def put_file(self, local_path: Union[str, Path], remote_path: Union[str, Path], **kwargs) -> None:
        """Upload file from local to remote."""
        return self._fs.put_file(local_path, self._resolve_path(remote_path), **kwargs)
    
    def touch(self, path: Union[str, Path], **kwargs) -> None:
        """Create empty file or update timestamp."""
        resolved_path = self._resolve_path(path)
        # Ensure parent directories exist
        if hasattr(self._fs, 'makedirs'):
            parent_dir = str(Path(resolved_path).parent)
            try:
                self._fs.makedirs(parent_dir, exist_ok=True)
            except Exception:
                pass
        return self._fs.touch(resolved_path, **kwargs)
    
    # Properties and methods for compatibility with fsspec interface
    
    @property
    def protocol(self) -> Union[str, List[str]]:
        """Return the protocol(s) supported by this filesystem."""
        return getattr(self._fs, 'protocol', 'file')
    
    @property
    def base_path(self) -> str:
        """Return the base path for this sandboxed filesystem."""
        return self._base_path
    
    def __getattr__(self, name: str) -> Any:
        """
        Delegate unknown attributes/methods to the underlying filesystem.
        
        This provides compatibility with filesystem-specific methods not
        explicitly wrapped above.
        """
        return getattr(self._fs, name)
    
    def __repr__(self) -> str:
        """String representation of the sandboxed filesystem."""
        return f"PathSandboxedFileSystem(base_path='{self._base_path}', protocol='{self.protocol}')"