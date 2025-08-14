"""
FSSpec adapter for filesystem operations across multiple protocols.
"""

import contextlib
import time
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional, Tuple, Union

import fsspec
from fsspec.callbacks import Callback

from ...domain.entities.location import LocationEntity


class ProgressTracker:
    """Progress tracking for file operations."""

    def __init__(
        self,
        operation: str,
        total_size: Optional[int] = None,
        total_files: Optional[int] = None,
    ):
        self.operation = operation
        self.total_size = total_size
        self.total_files = total_files
        self.bytes_transferred = 0
        self.files_completed = 0
        self.start_time = time.time()
        self.callbacks = []

    def add_callback(self, callback: Any) -> None:
        """Add a progress callback."""
        self.callbacks.append(callback)

    def update_bytes(self, bytes_transferred: int) -> None:
        """Update bytes transferred."""
        self.bytes_transferred += bytes_transferred
        for callback in self.callbacks:
            if hasattr(callback, "relative_update"):
                callback.relative_update(bytes_transferred)

    def update_files(self, files_completed: int = 1) -> None:
        """Update files completed."""
        self.files_completed += files_completed

    def get_progress_info(self) -> Dict[str, Any]:
        """Get current progress information."""
        elapsed = time.time() - self.start_time
        return {
            "operation": self.operation,
            "bytes_transferred": self.bytes_transferred,
            "files_completed": self.files_completed,
            "elapsed_seconds": elapsed,
            "total_size": self.total_size,
            "total_files": self.total_files,
        }


class FSSpecProgressCallback(Callback):
    """Progress callback that integrates with FSSpec operations."""

    def __init__(self, tracker: ProgressTracker):
        self.tracker = tracker
        super().__init__()

    def relative_update(self, inc: int = 1) -> None:
        """Update progress by a relative amount."""
        self.tracker.update_bytes(inc)
        super().relative_update(inc)

    def absolute_update(self, val: int) -> None:
        """Set absolute progress value."""
        if hasattr(self, "_last_val"):
            inc = val - self._last_val
        else:
            inc = val
        self._last_val = val
        self.tracker.update_bytes(inc)
        super().absolute_update(val)


class FSSpecAdapter:
    """
    Adapter for fsspec filesystem operations.

    Provides a clean interface for file operations across different protocols
    while handling progress tracking, error recovery, and Earth science
    data patterns.
    """

    def __init__(self, location: LocationEntity):
        """
        Initialize the adapter with a location.

        Args:
            location: LocationEntity containing connection configuration
        """
        self.location = location
        self._fs = None
        self._connection_tested = False

    @property
    def fs(self) -> fsspec.AbstractFileSystem:
        """Get the filesystem instance, creating it if necessary."""
        if self._fs is None:
            protocol = self.location.get_protocol()
            storage_options = self.location.get_storage_options().copy()

            # Add location name as host if not specified
            if "host" not in storage_options and protocol in ("sftp", "ssh"):
                storage_options["host"] = self.location.name

            self._fs = fsspec.filesystem(protocol, **storage_options)

        return self._fs

    def test_connection(self, timeout: int = 30) -> Dict[str, Any]:
        """
        Test the connection to the location.

        Args:
            timeout: Connection timeout in seconds

        Returns:
            Dictionary with connection test results
        """
        result = {
            "success": False,
            "protocol": self.location.get_protocol(),
            "location_name": self.location.name,
            "error": None,
            "response_time": None,
        }

        start_time = time.time()

        try:
            # Test basic operations
            base_path = self.location.get_base_path() or "."

            # Try to list the base path
            self.fs.ls(base_path)

            result["success"] = True
            result["response_time"] = time.time() - start_time
            self._connection_tested = True

        except Exception as e:
            result["error"] = str(e)
            result["response_time"] = time.time() - start_time

        return result

    def get_file(
        self,
        remote_path: str,
        local_path: Union[str, Path],
        overwrite: bool = False,
        progress_tracker: Optional[ProgressTracker] = None,
    ) -> str:
        """
        Download a file from the remote location.

        Args:
            remote_path: Path to the file on the remote location
            local_path: Local path to save the file
            overwrite: Whether to overwrite existing files
            progress_tracker: Optional progress tracker

        Returns:
            Path to the downloaded file

        Raises:
            FileExistsError: If file exists and overwrite is False
            FileNotFoundError: If remote file doesn't exist
        """
        # Resolve paths
        remote_path = self._resolve_remote_path(remote_path)
        local_path = Path(local_path)

        # Check if local file exists
        if local_path.exists() and not overwrite:
            raise FileExistsError(f"File already exists: {local_path}")

        # Ensure parent directory exists
        local_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            # Get file size for progress tracking
            file_size = self.fs.size(remote_path)

            if progress_tracker:
                progress_tracker.total_size = file_size
                callback = FSSpecProgressCallback(progress_tracker)
            else:
                callback = None

            # Download the file
            self.fs.get_file(remote_path, str(local_path), callback=callback)

            if progress_tracker:
                progress_tracker.update_files(1)

            return str(local_path)

        except Exception as e:
            # Clean up partial download
            if local_path.exists():
                local_path.unlink()
            raise RuntimeError(f"Failed to download {remote_path}: {e}")

    def get_files(
        self,
        remote_pattern: str,
        local_dir: Union[str, Path],
        recursive: bool = False,
        overwrite: bool = False,
        progress_tracker: Optional[ProgressTracker] = None,
    ) -> List[str]:
        """
        Download multiple files matching a pattern.

        Args:
            remote_pattern: Pattern to match files
            local_dir: Local directory to save files
            recursive: Whether to search recursively
            overwrite: Whether to overwrite existing files
            progress_tracker: Optional progress tracker

        Returns:
            List of downloaded file paths
        """
        local_dir = Path(local_dir)
        local_dir.mkdir(parents=True, exist_ok=True)

        # Find matching files
        remote_files = list(self.find_files(remote_pattern, recursive=recursive))

        if progress_tracker:
            progress_tracker.total_files = len(remote_files)
            total_size = sum(info.get("size", 0) for _, info in remote_files)
            progress_tracker.total_size = total_size

        downloaded_files = []

        for remote_path, file_info in remote_files:
            try:
                # Determine local path
                rel_path = Path(remote_path).name  # Simple name for now
                local_path = local_dir / rel_path

                if local_path.exists() and not overwrite:
                    continue

                # Download file
                self.get_file(remote_path, local_path, overwrite=overwrite)
                downloaded_files.append(str(local_path))

                if progress_tracker:
                    progress_tracker.update_files(1)

            except Exception as e:
                print(f"Warning: Failed to download {remote_path}: {e}")

        return downloaded_files

    def find_files(
        self, pattern: str, base_path: str = "", recursive: bool = False
    ) -> Generator[Tuple[str, Dict], None, None]:
        """
        Find files matching a pattern.

        Args:
            pattern: Glob pattern to match
            base_path: Base path to start search from
            recursive: Whether to search recursively

        Yields:
            Tuples of (file_path, file_info)
        """
        import fnmatch
        import os

        # Resolve base path
        search_path = self._resolve_remote_path(base_path) if base_path else ""

        if recursive:
            for root, dirs, files in self.fs.walk(search_path):
                for file in files:
                    file_path = os.path.join(root, file)
                    if fnmatch.fnmatch(file_path, pattern) or fnmatch.fnmatch(
                        file, pattern
                    ):
                        try:
                            file_info = self.fs.info(file_path)
                            yield file_path, file_info
                        except Exception:
                            # Skip files we can't get info for
                            pass
        else:
            # Non-recursive search
            glob_pattern = (
                os.path.join(search_path, pattern) if search_path else pattern
            )

            try:
                for file_path in self.fs.glob(glob_pattern):
                    if self.fs.isfile(file_path):
                        try:
                            file_info = self.fs.info(file_path)
                            yield file_path, file_info
                        except Exception:
                            # Skip files we can't get info for
                            pass
            except Exception as e:
                print(f"Warning: Failed to glob pattern {glob_pattern}: {e}")

    def exists(self, path: str) -> bool:
        """Check if a path exists on the remote location."""
        remote_path = self._resolve_remote_path(path)
        return self.fs.exists(remote_path)

    def isfile(self, path: str) -> bool:
        """Check if a path is a file on the remote location."""
        remote_path = self._resolve_remote_path(path)
        return self.fs.isfile(remote_path)

    def isdir(self, path: str) -> bool:
        """Check if a path is a directory on the remote location."""
        remote_path = self._resolve_remote_path(path)
        return self.fs.isdir(remote_path)

    def size(self, path: str) -> int:
        """Get the size of a file on the remote location."""
        remote_path = self._resolve_remote_path(path)
        return self.fs.size(remote_path)

    def info(self, path: str) -> Dict[str, Any]:
        """Get information about a path on the remote location."""
        remote_path = self._resolve_remote_path(path)
        return self.fs.info(remote_path)

    @contextlib.contextmanager
    def open_file(self, path: str, mode: str = "rb", **kwargs):
        """
        Open a file on the remote location.

        Args:
            path: Path to the file
            mode: File opening mode
            **kwargs: Additional arguments for fsspec open

        Yields:
            File-like object
        """
        remote_path = self._resolve_remote_path(path)

        with self.fs.open(remote_path, mode, **kwargs) as f:
            yield f

    def get_filesystem_info(self) -> Dict[str, Any]:
        """Get information about the filesystem."""
        return {
            "protocol": self.location.get_protocol(),
            "location_name": self.location.name,
            "base_path": self.location.get_base_path(),
            "connection_tested": self._connection_tested,
            "filesystem_class": type(self.fs).__name__,
        }

    def _resolve_remote_path(self, path: str) -> str:
        """
        Resolve a path relative to the location's base path.

        Args:
            path: Relative or absolute path

        Returns:
            Resolved path on the remote filesystem
        """
        if not path:
            return self.location.get_base_path() or ""

        base_path = self.location.get_base_path()
        if base_path:
            if path.startswith("/"):
                # Absolute path - use as is
                return path
            else:
                # Relative path - join with base path
                return str(Path(base_path) / path)
        else:
            return path

    def close(self) -> None:
        """Close the filesystem connection if it supports it."""
        if self._fs and hasattr(self._fs, "close"):
            try:
                self._fs.close()
            except Exception:
                pass
        self._fs = None
        self._connection_tested = False
