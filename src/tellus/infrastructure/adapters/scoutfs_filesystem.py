"""ScoutFS filesystem implementation for Tellus.

This module provides a filesystem implementation that works with ScoutFS, including
support for staging files from tape storage and progress tracking.
"""

import datetime
import time
import warnings
from contextlib import contextmanager
from typing import Any, Dict, Optional, Union

import fsspec
import fsspec.implementations.sftp
import requests
from fsspec.callbacks import Callback
from fsspec.registry import register_implementation
from loguru import logger
from rich.console import Console
from rich.text import Text

# Comprehensive warning suppression for ScoutFS
import urllib3
from urllib3.exceptions import InsecureRequestWarning

# Disable all urllib3 warnings globally
urllib3.disable_warnings()
urllib3.disable_warnings(InsecureRequestWarning)

# Also suppress via warnings module
warnings.filterwarnings('ignore', category=InsecureRequestWarning)

# Suppress requests SSL warnings
import requests.packages.urllib3 as requests_urllib3
requests_urllib3.disable_warnings()
requests_urllib3.disable_warnings(InsecureRequestWarning)


class ScoutFSFileSystem(fsspec.implementations.sftp.SFTPFileSystem):
    """Filesystem implementation for ScoutFS with tape staging support.

    This class extends SFTPFileSystem to add support for ScoutFS-specific features
    like file staging from tape storage and progress tracking.
    """

    protocol = "scoutfs", "sftp", "ssh"

    def __init__(self, host, **kwargs):
        """Initialize the ScoutFS filesystem.

        Args:
            host: The host to connect to
            **kwargs: Additional arguments passed to SFTPFileSystem
        """
        self._scoutfs_config = kwargs.pop("scoutfs_config", {})
        
        # Extract warning filter configuration (kept for compatibility)
        self._warning_filters = kwargs.pop("warning_filters", {})
        
        ssh_kwargs = kwargs
        
        # Initialize with global warning suppression already in effect
        super().__init__(host, **ssh_kwargs)

    @contextmanager
    def _filtered_warnings(self):
        """Context manager to apply warning filters for this filesystem instance.
        
        Since warnings are now suppressed globally, this is a no-op.
        """
        yield

    # --- ScoutFS API Methods ---

    def _scoutfs_generate_token(self):
        """Generate a new authentication token from the ScoutFS API."""
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
        # [FIXME] Use environment variables or some other secure method to
        #         store credentials!
        data = {
            "acct": "filestat",
            "pass": "filestat",
        }

        # Make request with warning filters applied
        with self._filtered_warnings():
            response = requests.post(
                f"{self._scoutfs_api_url}/security/login",
                headers=headers,
                json=data,
                verify=False,
            )
        response.raise_for_status()
        return response.json().get("response")

    @property
    def _scoutfs_token(self):
        """Get the current authentication token, generating a new one if needed."""
        if "token" not in self._scoutfs_config:
            self._scoutfs_config["token"] = self._scoutfs_generate_token()
        return self._scoutfs_config["token"]

    @property
    def _scoutfs_api_url(self):
        """Get the base URL for the ScoutFS API."""
        return self._scoutfs_config.get("api_url", "https://hsm.dmawi.de:8080/v1")

    def _scoutfs_get_filesystems(self):
        """Get information about all available filesystems from the ScoutFS API."""
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self._scoutfs_token}",
        }
        with self._filtered_warnings():
            response = requests.get(
                f"{self._scoutfs_api_url}/filesystems",
                headers=headers,
                verify=False,
            )
        response.raise_for_status()
        return response.json()

    def _get_fsid_for_path(self, path):
        """Get the filesystem ID for a given path."""
        fsid_response = self._scoutfs_get_filesystems()
        matching_fsids = []
        for fsid_info in fsid_response.get("fsids", []):
            if path.startswith(fsid_info["mount"]):
                matching_fsids.append(fsid_info)
        
        if len(matching_fsids) == 0:
            raise ValueError(f"No ScoutFS filesystem found for path '{path}'. "
                           f"Available mounts: {[f['mount'] for f in fsid_response.get('fsids', [])]}")
        elif len(matching_fsids) > 1:
            raise ValueError(f"Multiple ScoutFS filesystems match path '{path}': {matching_fsids}")
        
        return matching_fsids[0]["fsid"]

    def _scoutfs_file(self, path):
        """Get file information from the ScoutFS API."""
        fsid = self._get_fsid_for_path(path)
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self._scoutfs_token}",
        }
        with self._filtered_warnings():
            response = requests.get(
                f"{self._scoutfs_api_url}/file?fsid={fsid}&path={path}",
                headers=headers,
                verify=False,
            )
        response.raise_for_status()
        return response.json()

    def _scoutfs_request(self, command, path):
        """Make a request to the ScoutFS API."""
        fsid = self._get_fsid_for_path(path)
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self._scoutfs_token}",
        }
        with self._filtered_warnings():
            response = requests.post(
                f"{self._scoutfs_api_url}/request/{command}?fsid={fsid}&path={path}",
                headers=headers,
                json={"path": path},
                verify=False,
            )
        response.raise_for_status()
        return response.json()

    def _scoutfs_queues(self):
        """Get information about the staging queues."""
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self._scoutfs_token}",
        }
        with self._filtered_warnings():
            response = requests.get(
                f"{self._scoutfs_api_url}/queues",
                headers=headers,
                verify=False,
            )
        response.raise_for_status()
        return response.json()

    @property
    def queues(self):
        """Get information about the staging queues."""
        return self._scoutfs_queues()

    def stage(self, path):
        """Stage a file from tape to disk.

        Args:
            path: Path to the file to stage

        Returns:
            The API response from the staging request
        """
        return self._scoutfs_request("stage", path)

    def info(self, path, **kwargs):
        """Get information about a file or directory.

        This extends the base info() method to add ScoutFS-specific information.
        """
        robj = super().info(path, **kwargs)

        # Add ScoutFS-specific information
        try:
            scoutfs_file = self._scoutfs_file(path)
            robj["scoutfs_info"] = {
                "/file": scoutfs_file,
                "/batchfile": None,
            }
        except Exception as e:
            logger.error(f"Failed to get ScoutFS info for {path}: {e}")
            robj["scoutfs_info"] = {
                "/file": {"error": str(e)},
                "/batchfile": None,
            }

        return robj

    def is_online(self, path):
        """Check if a file is online (not on tape).

        Args:
            path: Path to the file to check

        Returns:
            bool: True if the file is online, False otherwise
        """
        try:
            info = self.info(path)
            scoutfs_info = info.get("scoutfs_info", {}).get("/file", {})
            online_blocks = scoutfs_info.get("onlineblocks", "")
            offline_blocks = scoutfs_info.get("offlineblocks", "")

            # Convert to int, defaulting to 0 for empty strings
            if online_blocks != "":
                online_blocks = int(online_blocks)
            else:
                online_blocks = 0
                
            if offline_blocks != "":
                offline_blocks = int(offline_blocks)
            else:
                offline_blocks = 0

            # [FIXME]: Partially online files might be mis-represented here?
            return online_blocks > 0 and offline_blocks == 0

        except Exception as e:
            logger.error(f"Error checking if {path} is online: {e}")
            # If we can't determine the status, assume the file is online
            # to avoid unnecessary staging attempts
            return True
    
    async def is_online_async(self, path):
        """Asynchronously check if a file is online (not on tape).
        
        Args:
            path: Path to the file to check
            
        Returns:
            bool: True if the file is online, False otherwise
        """
        import asyncio

        # Run the synchronous is_online method in a thread pool to avoid blocking
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self.is_online, path)
    
    async def check_online_status_batch(self, paths, max_concurrent=10):
        """Asynchronously check online status for multiple files concurrently.
        
        Args:
            paths: List of file paths to check
            max_concurrent: Maximum number of concurrent status checks
            
        Returns:
            dict: Dictionary mapping paths to their online status (True/False)
        """
        import asyncio

        # Create semaphore to limit concurrent operations
        semaphore = asyncio.Semaphore(max_concurrent)
        
        async def check_with_semaphore(path):
            async with semaphore:
                return path, await self.is_online_async(path)
        
        # Launch all status checks concurrently
        tasks = [check_with_semaphore(path) for path in paths]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Build result dictionary, handling exceptions
        status_dict = {}
        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Error in batch status check: {result}")
                continue
            path, is_online = result
            status_dict[path] = is_online
            
        return status_dict

    def _scoutfs_online_status(self, path):
        """Get a formatted string showing the online/offline status of a file."""
        try:
            info = self.info(path)
            scoutfs_info = info.get("scoutfs_info", {}).get("/file", {})
            online_blocks = scoutfs_info.get("onlineblocks", "")
            offline_blocks = scoutfs_info.get("offlineblocks", "")

            if online_blocks != "":
                online_blocks = int(online_blocks)
            if offline_blocks != "":
                offline_blocks = int(offline_blocks)

            return Text.from_markup(
                f"{path} [green]online_blocks: {online_blocks}[/green], [red]offline_blocks: {offline_blocks}[/red]"
            )
        except Exception as e:
            return f"{path} [red]Error: {e}[/red]"

    def open(
        self,
        path,
        mode="r",
        stage_before_opening=True,
        timeout=None,
        callback: Optional[Callback] = None,
        **kwargs,
    ):
        """Open a file, optionally staging it from tape first.

        Args:
            path: Path to the file to open
            mode: File mode ('r', 'w', etc.)
            stage_before_opening: If True, stage the file before opening
            timeout: Maximum time to wait for staging (in seconds)
            callback: Optional fsspec callback for progress tracking
            **kwargs: Additional arguments passed to the parent class

        Returns:
            A file-like object

        Raises:
            TimeoutError: If staging times out
            FileNotFoundError: If the file doesn't exist
        """
        if "w" in mode or not stage_before_opening:
            # Skip staging for write modes or if explicitly disabled
            return super().open(path, mode=mode, callback=callback, **kwargs)

        # Check if the file exists and get its info
        try:
            file_info = self.info(path)
        except FileNotFoundError:
            if "w" not in mode and "a" not in mode:
                raise
            # If the file doesn't exist but we're in write/append mode, that's fine
            return super().open(path, mode=mode, callback=callback, **kwargs)

        # If the file is already online, just open it
        if self.is_online(path):
            return super().open(path, mode=mode, callback=callback, **kwargs)

        # Otherwise, stage the file
        if callback:
            callback.set_description(f"Staging {path}")

        self.stage(path)

        # Wait for the file to become available
        timeout_dt = datetime.datetime.now() + datetime.timedelta(
            seconds=timeout if timeout is not None else 180  # Default 3 minutes
        )

        while True:
            if self.is_online(path):
                break

            if datetime.datetime.now() > timeout_dt:
                raise TimeoutError(
                    f"Timeout while waiting for file {path} to be staged"
                )

            if callback:
                callback.relative_update(0)  # Just to show we're still working

            time.sleep(1)

        # Now open the file using the parent class's open method
        return super().open(path, mode=mode, callback=callback, **kwargs)


# Register the implementation with fsspec
register_implementation("scoutfs", ScoutFSFileSystem, clobber=True)
