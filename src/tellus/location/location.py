import contextlib
import fnmatch
import json
import os
import shutil
from dataclasses import asdict, dataclass, field
from enum import Enum, auto
from pathlib import Path
from typing import (ClassVar, Dict, Generator, List, Optional, Tuple, Type,
                    Union)

import fsspec
from rich.console import Console

from ..progress import get_default_progress, get_progress_callback
from ..progress import FSSpecProgressCallback
from .sandboxed_filesystem import PathSandboxedFileSystem


# Allowed location names
class LocationKind(Enum):
    TAPE = auto()
    COMPUTE = auto()
    DISK = auto()
    FILESERVER = auto()

    @classmethod
    def from_str(cls, s):
        try:
            return cls[s.upper()]
        except KeyError:
            raise ValueError(f"Invalid location kind: {s}")


class LocationExistsError(Exception):
    pass


@dataclass
class Location:
    # Class variable to track all location instances
    _locations: ClassVar[Dict[str, "Location"]] = {}
    _locations_file: ClassVar[Path] = (
        Path(__file__).parent.parent.parent.parent / "locations.json"
    )

    # Instance variables
    name: str
    kinds: list[LocationKind]
    config: dict
    optional: Optional[bool] = False
    _skip_registry: bool = field(default=False, repr=False)

    def __post_init__(self):
        # Validate location kinds
        for kind in self.kinds:
            if not isinstance(kind, LocationKind):
                raise ValueError(
                    f"Location kind '{kind}' is not a valid LocationKind. "
                    f"Allowed values: {', '.join(e.name for e in LocationKind)}"
                )

        # Add to class registry (skip if loading from disk to avoid conflicts)
        if not self._skip_registry:
            if self.name in self._locations:
                raise LocationExistsError(
                    f"Location with name '{self.name}' already exists"
                )
            self._locations[self.name] = self
            self._save_locations()
        else:
            # When loading from disk, just add to registry without validation
            self._locations[self.name] = self

    @classmethod
    def from_dict(cls, data, _skip_registry=False):
        # Create a new dictionary with the expected structure
        location_data = {
            "name": data.get("name", ""),  # This should be provided by the caller
            "kinds": [LocationKind[kind.upper()] for kind in data.get("kinds", [])],
            "config": data.get("config", {}),
            "optional": data.get("optional", False),
        }
        return cls(**location_data, _skip_registry=_skip_registry)

    @classmethod
    def load_locations(cls) -> None:
        """Load locations from the JSON file."""
        if not cls._locations_file.exists():
            cls._locations = {}
            return

        try:
            with open(cls._locations_file, "r") as f:
                locations_data = json.load(f)
                cls._locations = {}
                for name, data in locations_data.items():
                    # Ensure the name is included in the data
                    if "name" not in data:
                        data["name"] = name
                    # Create the location with registry skip to avoid conflicts
                    location = Location.from_dict(data, _skip_registry=True)
        except json.JSONDecodeError:
            # If the file is corrupted, reset to empty
            cls._locations = {}

    def _save_locations(self) -> None:
        """Save current locations to the JSON file."""
        # Ensure the parent directory exists
        self._locations_file.parent.mkdir(parents=True, exist_ok=True)

        # Convert locations to a serializable format
        locations_data = {
            name: {
                "kinds": [kind.name for kind in loc.kinds],
                "config": loc.config,
                "optional": loc.optional,
            }
            for name, loc in self._locations.items()
        }

        # Write to file atomically
        temp_file = self._locations_file.with_suffix(".tmp")
        with open(temp_file, "w") as f:
            json.dump(locations_data, f, indent=2)

        # On POSIX, this is an atomic operation
        temp_file.replace(self._locations_file)

    @classmethod
    def remove_location(cls, name: str) -> None:
        """Remove a location by name."""
        if name in cls._locations:
            del cls._locations[name]
            # Save the updated locations
            if cls._locations:  # If not empty, save the remaining locations
                cls._locations[next(iter(cls._locations))]._save_locations()
            else:  # If empty, remove the file
                try:
                    cls._locations_file.unlink(missing_ok=True)
                except Exception:
                    pass

    @classmethod
    def get_location(cls, name: str) -> Optional["Location"]:
        """Get a location by name."""
        return cls._locations.get(name)

    @classmethod
    def list_locations(cls) -> List["Location"]:
        """Get a list of all locations."""
        return list(cls._locations.values())

    def to_dict(self):
        return {
            "name": self.name,
            "kinds": [kind.name for kind in self.kinds],
            "config": self.config,
            "optional": self.optional,
        }

    @property
    def fs(self) -> PathSandboxedFileSystem:
        """
        Get a sandboxed filesystem for this location.
        
        Returns a PathSandboxedFileSystem that constrains all operations
        to the location's configured path. This prevents operations from
        accidentally occurring in the current working directory.
        
        Returns:
            PathSandboxedFileSystem: Sandboxed filesystem instance
        """
        storage_options = self.config.get("storage_options", {}).copy()
        protocol = self.config.get("protocol", "file")
        
        # Add host if not present for SSH-like protocols
        if "host" not in storage_options and protocol in ("sftp", "ssh", "scoutfs"):
            storage_options["host"] = self.name
        
        # Separate filesystem path from connection parameters
        # 'path' should not be passed to SSH connection, it's used for base path only
        fs_storage_options = storage_options.copy()
        base_path = fs_storage_options.pop("path", "")
        
        # Also check for path in the main config (fallback)
        if not base_path:
            base_path = self.config.get("path", "")
            
        # Create the underlying filesystem
        base_fs = fsspec.filesystem(protocol, **fs_storage_options)
        
        # Return sandboxed filesystem
        return PathSandboxedFileSystem(base_fs, base_path)

    def get(
        self,
        remote_path: str,
        local_path: Optional[str] = None,
        overwrite: bool = False,
        show_progress: bool = True,
        progress_callback: Optional[FSSpecProgressCallback] = None,
    ) -> str:
        """
        Download a file from the location.

        Args:
            remote_path: Path to the file in the location
            local_path: Local path to save the file (defaults to the remote filename in current dir)
            overwrite: If True, overwrite existing files
            show_progress: If True, show progress bar during download
            progress_callback: Optional callback for progress tracking

        Returns:
            Path to the downloaded file
        """
        # Path resolution is now handled by the sandboxed filesystem
        # Just use the path as-is - it will be resolved relative to location's base path
        local_path = Path(local_path) if local_path else Path(Path(remote_path).name)

        if local_path.exists() and not overwrite:
            raise FileExistsError(f"File already exists: {local_path}")

        # Ensure parent directory exists
        local_path.parent.mkdir(parents=True, exist_ok=True)

        # Use fsspec's get_file for the transfer
        if progress_callback:
            with self.get_fileobj(remote_path, progress_callback) as (
                remote_file,
                file_size,
            ):
                with open(local_path, "wb") as local_file:
                    while True:
                        chunk = remote_file.read(1024 * 1024)  # 1MB chunks
                        if not chunk:
                            break
                        local_file.write(chunk)
                        progress_callback.relative_update(len(chunk))
        elif show_progress:
            # Get file size for progress tracking
            try:
                file_size = self.fs.size(remote_path)
            except Exception:
                file_size = None
            
            # Use progress callback for download
            callback = get_progress_callback(
                description=f"Downloading {Path(remote_path).name}",
                size=file_size
            )
            
            with callback:
                self.fs.get_file(remote_path, str(local_path), callback=callback)
        else:
            # Use fsspec's get_file for the transfer without progress
            self.fs.get_file(remote_path, str(local_path))
        return str(local_path)

    @contextlib.contextmanager
    def get_fileobj(
        self,
        remote_path: str,
        progress_callback: Optional[FSSpecProgressCallback] = None,
        **kwargs,
    ):
        """
        Get a file-like object for reading from the remote location.

        Args:
            remote_path: Path to the file in the location
            progress_callback: Optional callback for progress tracking
            **kwargs: Additional arguments to pass to the filesystem's open method

        Yields:
            Tuple of (file-like object, file size in bytes)
        """
        # Path resolution is now handled by the sandboxed filesystem
        # Just use the path as-is - it will be resolved relative to location's base path
        file_obj = None
        try:
            # Use the callback if provided, otherwise use a no-op callback
            callback = progress_callback or fsspec.callbacks.NoOpCallback()

            # Open the file with the callback
            file_obj = self.fs.open(remote_path, "rb", callback=callback, **kwargs)

            # Get file size for progress tracking
            file_size = self.fs.size(remote_path)
            if progress_callback:
                progress_callback.set_size(file_size)

            yield file_obj, file_size

        finally:
            if file_obj is not None:
                file_obj.close()

    def find_files(
        self, pattern: str, base_path: str = "", recursive: bool = False
    ) -> Generator[Tuple[str, dict], None, None]:
        """
        Find files matching a pattern in the location.

        Args:
            pattern: Glob pattern to match files against
            base_path: Base path to start searching from
            recursive: Whether to search recursively

        Yields:
            Tuple of (file_path, file_info) for each matching file
        """
        # Path resolution is now handled by the sandboxed filesystem
        # Use base_path as-is - it will be resolved relative to location's base path
        resolved_base_path = base_path
        if recursive:
            for root, _, files in self.fs.walk(resolved_base_path):
                for file in files:
                    file_path = os.path.join(root, file)
                    if fnmatch.fnmatch(file_path, pattern):
                        yield file_path, self.fs.info(file_path)
        else:
            # With sandboxed filesystem, we can just use the pattern directly 
            # since path resolution is handled by the filesystem wrapper
            if resolved_base_path:
                glob_pattern = os.path.join(resolved_base_path, pattern)
            else:
                glob_pattern = pattern
            for file in self.fs.glob(glob_pattern):
                if self.fs.isfile(file):
                    yield file, self.fs.info(file)

    def mget(
        self,
        remote_pattern: str,
        local_dir: str,
        recursive: bool = False,
        overwrite: bool = False,
        base_path: str = "",
        show_progress: bool = True,
        console: Optional[Console] = None,
    ) -> List[str]:
        """
        Download multiple files matching a pattern using fsspec's get.

        Args:
            remote_pattern: Pattern to match files against
            local_dir: Local directory to save files to
            recursive: Whether to search recursively
            overwrite: Whether to overwrite existing files
            base_path: Base path to start searching from
            show_progress: Whether to show progress bars
            console: Optional console instance for output

        Returns:
            List of paths to downloaded files
        """
        if console is None:
            console = Console()
            
        local_dir = Path(local_dir)
        local_dir.mkdir(parents=True, exist_ok=True)

        downloaded_files = []

        # First, find all files to download
        files_to_download = list(self.find_files(remote_pattern, base_path, recursive))

        if not files_to_download:
            console.print(f"[yellow]No files found matching pattern: {remote_pattern}[/yellow]")
            return []

        if show_progress and len(files_to_download) > 1:
            # Use rich progress for multiple files
            with get_default_progress() as progress:
                overall_task = progress.add_task(
                    f"Downloading {len(files_to_download)} files", 
                    total=len(files_to_download)
                )
                
                for i, (remote_path, file_info) in enumerate(files_to_download):
                    rel_path = (
                        os.path.relpath(remote_path, base_path)
                        if base_path
                        else os.path.basename(remote_path)
                    )
                    local_path = local_dir / rel_path

                    if local_path.exists() and not overwrite:
                        console.print(f"[yellow]Skipping existing file: {local_path}[/yellow]")
                        progress.update(overall_task, advance=1)
                        continue

                    try:
                        local_path.parent.mkdir(parents=True, exist_ok=True)
                        
                        # Update progress description for current file
                        progress.update(overall_task, description=f"Downloading {Path(remote_path).name}")
                        
                        # Download with individual file progress
                        self.get(remote_path, str(local_path), overwrite=overwrite, show_progress=False)
                        downloaded_files.append(str(local_path))
                        console.print(f"✅ [green]Downloaded:[/green] {local_path}")
                        
                    except Exception as e:
                        console.print(f"[red]Error downloading {remote_path}: {str(e)}[/red]")
                    
                    progress.update(overall_task, advance=1)
                    
            # Show summary
            if downloaded_files:
                console.print(f"\n✅ [green]Successfully downloaded {len(downloaded_files)}/{len(files_to_download)} files to {local_dir}[/green]")
        else:
            # Download files without progress tracking or single file
            for remote_path, _ in files_to_download:
                rel_path = (
                    os.path.relpath(remote_path, base_path)
                    if base_path
                    else os.path.basename(remote_path)
                )
                local_path = local_dir / rel_path

                if local_path.exists() and not overwrite:
                    console.print(f"[yellow]Skipping existing file: {local_path}[/yellow]")
                    continue

                try:
                    local_path.parent.mkdir(parents=True, exist_ok=True)
                    self.get(remote_path, str(local_path), overwrite=overwrite, show_progress=show_progress)
                    downloaded_files.append(str(local_path))
                    console.print(f"✅ [green]Downloaded:[/green] {local_path}")
                except Exception as e:
                    console.print(f"[red]Error downloading {remote_path}: {str(e)}[/red]")

        return downloaded_files
