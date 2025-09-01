"""Repository interface for file tracking functionality."""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict, List, Optional

from ..entities.file_tracking import (DVCConfiguration, FileChange,
                                      FileTrackingRepository,
                                      RepositorySnapshot, RepositoryState,
                                      TrackedFileMetadata, TrackingStatus)


class IFileTrackingRepository(ABC):
    """Interface for file tracking repository operations."""
    
    @abstractmethod
    def create_repository(self, root_path: str) -> FileTrackingRepository:
        """Create a new file tracking repository."""
        pass
    
    @abstractmethod
    def get_repository(self, root_path: str) -> Optional[FileTrackingRepository]:
        """Get an existing repository by root path."""
        pass
    
    @abstractmethod
    def save_repository_state(self, repository: FileTrackingRepository) -> None:
        """Save repository state to persistent storage."""
        pass
    
    @abstractmethod
    def load_repository_state(self, root_path: str) -> Optional[RepositoryState]:
        """Load repository state from persistent storage."""
        pass
    
    @abstractmethod
    def create_snapshot(
        self, 
        repository: FileTrackingRepository, 
        message: str, 
        author: str,
        changes: List[FileChange]
    ) -> RepositorySnapshot:
        """Create a new repository snapshot."""
        pass
    
    @abstractmethod
    def get_snapshots(self, repository: FileTrackingRepository) -> List[RepositorySnapshot]:
        """Get all snapshots for a repository."""
        pass
    
    @abstractmethod
    def get_snapshot(self, repository: FileTrackingRepository, snapshot_id: str) -> Optional[RepositorySnapshot]:
        """Get a specific snapshot by ID."""
        pass


class IFileSystemService(ABC):
    """Interface for filesystem operations."""
    
    @abstractmethod
    def scan_directory(
        self, 
        path: Path, 
        ignore_patterns: List[str] = None
    ) -> List[Path]:
        """Scan directory for files, respecting ignore patterns."""
        pass
    
    @abstractmethod
    def get_file_metadata(self, file_path: Path) -> TrackedFileMetadata:
        """Get metadata for a specific file."""
        pass
    
    @abstractmethod
    def calculate_file_hash(self, file_path: Path, algorithm: str = "sha256") -> str:
        """Calculate hash for a file."""
        pass
    
    @abstractmethod
    def file_exists(self, file_path: Path) -> bool:
        """Check if file exists."""
        pass
    
    @abstractmethod
    def get_file_size(self, file_path: Path) -> int:
        """Get file size in bytes."""
        pass


class IDVCService(ABC):
    """Interface for DVC (Data Version Control) operations."""
    
    @abstractmethod
    def is_dvc_available(self) -> bool:
        """Check if DVC is available and configured."""
        pass
    
    @abstractmethod
    def initialize_dvc(self, repository_path: Path) -> bool:
        """Initialize DVC in the given repository."""
        pass
    
    @abstractmethod
    def add_to_dvc(self, file_path: Path) -> bool:
        """Add a file to DVC tracking."""
        pass
    
    @abstractmethod
    def remove_from_dvc(self, file_path: Path) -> bool:
        """Remove a file from DVC tracking."""
        pass
    
    @abstractmethod
    def push_to_remote(self, file_path: Path, remote_name: Optional[str] = None) -> bool:
        """Push file to DVC remote storage."""
        pass
    
    @abstractmethod
    def pull_from_remote(self, file_path: Path, remote_name: Optional[str] = None) -> bool:
        """Pull file from DVC remote storage."""
        pass
    
    @abstractmethod
    def get_dvc_status(self, repository_path: Path) -> Dict[str, str]:
        """Get DVC status for files in repository."""
        pass
    
    @abstractmethod
    def configure_remote(
        self, 
        repository_path: Path, 
        remote_name: str, 
        remote_url: str
    ) -> bool:
        """Configure a DVC remote."""
        pass