"""Filesystem service implementation for file tracking."""

import fnmatch
import hashlib
import os
from datetime import datetime
from pathlib import Path
from typing import List, Optional

from ...domain.entities.file_tracking import (FileHash, TrackedFileMetadata,
                                              TrackingStatus)
from ...domain.repositories.file_tracking_repository import IFileSystemService


class FileSystemService(IFileSystemService):
    """Implementation of filesystem operations for file tracking."""

    def scan_directory(
        self, 
        path: Path, 
        ignore_patterns: List[str] = None
    ) -> List[Path]:
        """Scan directory for files, respecting ignore patterns."""
        if ignore_patterns is None:
            ignore_patterns = []
        
        files = []
        
        try:
            for root, dirs, filenames in os.walk(path):
                root_path = Path(root)
                
                # Filter out ignored directories
                dirs[:] = [
                    d for d in dirs 
                    if not self._should_ignore(root_path / d, ignore_patterns, path)
                ]
                
                # Add non-ignored files
                for filename in filenames:
                    file_path = root_path / filename
                    if not self._should_ignore(file_path, ignore_patterns, path):
                        files.append(file_path)
                        
        except (OSError, PermissionError):
            pass  # Skip directories we can't access
        
        return files

    def get_file_metadata(self, file_path: Path) -> TrackedFileMetadata:
        """Get metadata for a specific file."""
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")
        
        stat = file_path.stat()
        content_hash = self.calculate_file_hash(file_path)
        
        return TrackedFileMetadata(
            path=str(file_path),
            size=stat.st_size,
            modification_time=datetime.fromtimestamp(stat.st_mtime),
            content_hash=FileHash(algorithm="sha256", value=content_hash),
            status=TrackingStatus.UNTRACKED,
            created_time=datetime.fromtimestamp(stat.st_ctime)
        )

    def calculate_file_hash(self, file_path: Path, algorithm: str = "sha256") -> str:
        """Calculate hash for a file."""
        if not file_path.exists():
            return ""
        
        if algorithm not in {'sha256', 'md5', 'sha1'}:
            raise ValueError(f"Unsupported hash algorithm: {algorithm}")
        
        hash_obj = getattr(hashlib, algorithm)()
        
        try:
            with open(file_path, 'rb') as f:
                while chunk := f.read(8192):
                    hash_obj.update(chunk)
            return hash_obj.hexdigest()
        except (OSError, PermissionError):
            return ""

    def file_exists(self, file_path: Path) -> bool:
        """Check if file exists."""
        return file_path.exists() and file_path.is_file()

    def get_file_size(self, file_path: Path) -> int:
        """Get file size in bytes."""
        if not file_path.exists():
            return 0
        
        try:
            return file_path.stat().st_size
        except (OSError, PermissionError):
            return 0

    def _should_ignore(self, file_path: Path, ignore_patterns: List[str], base_path: Path) -> bool:
        """Check if a path should be ignored based on patterns."""
        try:
            rel_path = str(file_path.relative_to(base_path))
        except ValueError:
            return False  # Not under base path
        
        # Convert Windows paths to forward slashes for consistent matching
        rel_path = rel_path.replace('\\', '/')
        
        for pattern in ignore_patterns:
            # Handle directory patterns (ending with /)
            if pattern.endswith('/'):
                pattern = pattern.rstrip('/')
                if fnmatch.fnmatch(rel_path, pattern) or \
                   any(part.startswith(pattern + '/') for part in rel_path.split('/')):
                    return True
            # Regular file patterns
            elif fnmatch.fnmatch(rel_path, pattern) or \
                 fnmatch.fnmatch(os.path.basename(rel_path), pattern):
                return True
        
        return False