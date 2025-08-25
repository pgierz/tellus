"""DVC (Data Version Control) service implementation."""

import logging
import subprocess
import sys
from pathlib import Path
from typing import Dict, List, Optional

from ...domain.repositories.file_tracking_repository import IDVCService

logger = logging.getLogger(__name__)


class DVCService(IDVCService):
    """Implementation of DVC operations for large file management."""

    def __init__(self):
        """Initialize DVC service."""
        self._logger = logger

    def is_dvc_available(self) -> bool:
        """Check if DVC is available and configured."""
        try:
            result = subprocess.run(
                ["dvc", "--version"], 
                capture_output=True, 
                text=True, 
                timeout=10
            )
            return result.returncode == 0
        except (subprocess.TimeoutExpired, FileNotFoundError, subprocess.SubprocessError):
            return False

    def initialize_dvc(self, repository_path: Path) -> bool:
        """Initialize DVC in the given repository."""
        try:
            # Check if already initialized
            if (repository_path / ".dvc").exists():
                self._logger.info(f"DVC already initialized in {repository_path}")
                return True
            
            # Initialize DVC
            result = subprocess.run(
                ["dvc", "init"],
                cwd=repository_path,
                capture_output=True,
                text=True,
                timeout=30
            )
            
            if result.returncode == 0:
                self._logger.info(f"DVC initialized successfully in {repository_path}")
                return True
            else:
                self._logger.error(f"DVC initialization failed: {result.stderr}")
                return False
                
        except (subprocess.TimeoutExpired, subprocess.SubprocessError) as e:
            self._logger.error(f"Error initializing DVC: {e}")
            return False

    def add_to_dvc(self, file_path: Path) -> bool:
        """Add a file to DVC tracking."""
        try:
            result = subprocess.run(
                ["dvc", "add", str(file_path)],
                cwd=file_path.parent,
                capture_output=True,
                text=True,
                timeout=60
            )
            
            if result.returncode == 0:
                self._logger.info(f"Added {file_path} to DVC")
                return True
            else:
                self._logger.error(f"Failed to add {file_path} to DVC: {result.stderr}")
                return False
                
        except (subprocess.TimeoutExpired, subprocess.SubprocessError) as e:
            self._logger.error(f"Error adding file to DVC: {e}")
            return False

    def remove_from_dvc(self, file_path: Path) -> bool:
        """Remove a file from DVC tracking."""
        try:
            # Remove .dvc file if it exists
            dvc_file = file_path.with_suffix(file_path.suffix + ".dvc")
            if dvc_file.exists():
                dvc_file.unlink()
                self._logger.info(f"Removed DVC file: {dvc_file}")
            
            # Remove from DVC cache (if needed)
            result = subprocess.run(
                ["dvc", "remove", str(file_path)],
                cwd=file_path.parent,
                capture_output=True,
                text=True,
                timeout=30
            )
            
            return True  # Don't fail if dvc remove fails
            
        except (subprocess.TimeoutExpired, subprocess.SubprocessError) as e:
            self._logger.warning(f"Error removing file from DVC: {e}")
            return True  # Don't fail the operation

    def push_to_remote(self, file_path: Path, remote_name: Optional[str] = None) -> bool:
        """Push file to DVC remote storage."""
        try:
            cmd = ["dvc", "push"]
            if remote_name:
                cmd.extend(["-r", remote_name])
            cmd.append(str(file_path))
            
            result = subprocess.run(
                cmd,
                cwd=file_path.parent,
                capture_output=True,
                text=True,
                timeout=300  # 5 minutes for large files
            )
            
            if result.returncode == 0:
                self._logger.info(f"Pushed {file_path} to DVC remote")
                return True
            else:
                self._logger.error(f"Failed to push {file_path} to DVC remote: {result.stderr}")
                return False
                
        except (subprocess.TimeoutExpired, subprocess.SubprocessError) as e:
            self._logger.error(f"Error pushing to DVC remote: {e}")
            return False

    def pull_from_remote(self, file_path: Path, remote_name: Optional[str] = None) -> bool:
        """Pull file from DVC remote storage."""
        try:
            cmd = ["dvc", "pull"]
            if remote_name:
                cmd.extend(["-r", remote_name])
            cmd.append(str(file_path))
            
            result = subprocess.run(
                cmd,
                cwd=file_path.parent,
                capture_output=True,
                text=True,
                timeout=300  # 5 minutes for large files
            )
            
            if result.returncode == 0:
                self._logger.info(f"Pulled {file_path} from DVC remote")
                return True
            else:
                self._logger.error(f"Failed to pull {file_path} from DVC remote: {result.stderr}")
                return False
                
        except (subprocess.TimeoutExpired, subprocess.SubprocessError) as e:
            self._logger.error(f"Error pulling from DVC remote: {e}")
            return False

    def get_dvc_status(self, repository_path: Path) -> Dict[str, str]:
        """Get DVC status for files in repository."""
        try:
            result = subprocess.run(
                ["dvc", "status"],
                cwd=repository_path,
                capture_output=True,
                text=True,
                timeout=30
            )
            
            status = {}
            if result.returncode == 0:
                # Parse DVC status output
                lines = result.stdout.strip().split('\n')
                for line in lines:
                    if line.strip() and not line.startswith('Data and pipelines'):
                        # Simple parsing - this would need to be more robust
                        parts = line.split()
                        if len(parts) >= 2:
                            status[parts[-1]] = parts[0]  # file -> status
            
            return status
            
        except (subprocess.TimeoutExpired, subprocess.SubprocessError) as e:
            self._logger.error(f"Error getting DVC status: {e}")
            return {}

    def configure_remote(
        self, 
        repository_path: Path, 
        remote_name: str, 
        remote_url: str
    ) -> bool:
        """Configure a DVC remote."""
        try:
            # Add remote
            result = subprocess.run(
                ["dvc", "remote", "add", remote_name, remote_url],
                cwd=repository_path,
                capture_output=True,
                text=True,
                timeout=30
            )
            
            if result.returncode == 0:
                self._logger.info(f"Added DVC remote '{remote_name}': {remote_url}")
                
                # Set as default remote
                subprocess.run(
                    ["dvc", "remote", "default", remote_name],
                    cwd=repository_path,
                    capture_output=True,
                    text=True,
                    timeout=30
                )
                
                return True
            else:
                self._logger.error(f"Failed to add DVC remote: {result.stderr}")
                return False
                
        except (subprocess.TimeoutExpired, subprocess.SubprocessError) as e:
            self._logger.error(f"Error configuring DVC remote: {e}")
            return False