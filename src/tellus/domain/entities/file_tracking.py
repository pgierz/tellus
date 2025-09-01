"""Domain entities for file tracking functionality."""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional, Set


class TrackingStatus(Enum):
    """Status of a tracked file."""
    UNTRACKED = "untracked"
    STAGED = "staged"  
    TRACKED = "tracked"
    MODIFIED = "modified"
    DELETED = "deleted"
    IGNORED = "ignored"


class FileChangeType(Enum):
    """Type of change to a file."""
    ADDED = "added"
    MODIFIED = "modified"
    DELETED = "deleted"
    RENAMED = "renamed"
    COPIED = "copied"


@dataclass(frozen=True)
class FileHash:
    """Represents a file content hash."""
    algorithm: str
    value: str
    
    def __post_init__(self):
        if self.algorithm not in {'sha256', 'md5', 'sha1'}:
            raise ValueError(f"Unsupported hash algorithm: {self.algorithm}")
        if not self.value:
            raise ValueError("Hash value cannot be empty")


@dataclass
class TrackedFileMetadata:
    """Metadata for a tracked file."""
    path: str
    size: int
    modification_time: datetime
    content_hash: FileHash
    status: TrackingStatus = TrackingStatus.UNTRACKED
    stage_hash: Optional[FileHash] = None
    created_time: Optional[datetime] = None
    
    @property
    def is_modified(self) -> bool:
        """Check if file has been modified since last tracking."""
        return self.status == TrackingStatus.MODIFIED
    
    @property
    def is_staged(self) -> bool:
        """Check if file is staged for next commit."""
        return self.status == TrackingStatus.STAGED


@dataclass
class FileChange:
    """Represents a change to a file."""
    file_path: str
    change_type: FileChangeType
    old_hash: Optional[FileHash] = None
    new_hash: Optional[FileHash] = None
    old_path: Optional[str] = None  # For renames
    timestamp: datetime = field(default_factory=datetime.now)


@dataclass
class RepositoryState:
    """Current state of the file tracking repository."""
    tracked_files: Dict[str, TrackedFileMetadata] = field(default_factory=dict)
    staged_changes: List[FileChange] = field(default_factory=list)
    ignore_patterns: List[str] = field(default_factory=list)
    
    def get_file_status(self, file_path: str) -> TrackingStatus:
        """Get status of a specific file."""
        if file_path in self.tracked_files:
            return self.tracked_files[file_path].status
        return TrackingStatus.UNTRACKED
    
    def get_modified_files(self) -> List[str]:
        """Get list of modified file paths."""
        return [
            path for path, metadata in self.tracked_files.items()
            if metadata.is_modified
        ]
    
    def get_staged_files(self) -> List[str]:
        """Get list of staged file paths."""
        return [
            path for path, metadata in self.tracked_files.items()
            if metadata.is_staged
        ]


@dataclass
class RepositorySnapshot:
    """A snapshot of the repository at a point in time."""
    id: str
    timestamp: datetime
    message: str
    author: str
    changes: List[FileChange]
    parent_id: Optional[str] = None
    
    @property
    def short_id(self) -> str:
        """Get shortened snapshot ID."""
        return self.id[:8]


@dataclass
class DVCConfiguration:
    """Configuration for DVC integration."""
    enabled: bool = False
    remote_name: Optional[str] = None
    remote_url: Optional[str] = None
    cache_dir: Optional[str] = None
    large_file_threshold: int = 100 * 1024 * 1024  # 100MB default
    
    def should_use_dvc(self, file_size: int) -> bool:
        """Determine if a file should be managed by DVC."""
        return self.enabled and file_size >= self.large_file_threshold


@dataclass
class FileTrackingRepository:
    """Represents a file tracking repository."""
    root_path: str
    state: RepositoryState = field(default_factory=RepositoryState)
    dvc_config: DVCConfiguration = field(default_factory=DVCConfiguration)
    snapshots: List[RepositorySnapshot] = field(default_factory=list)
    
    @property
    def working_directory(self) -> Path:
        """Get the working directory path."""
        return Path(self.root_path)
    
    @property
    def tellus_directory(self) -> Path:
        """Get the .tellus directory path."""
        return self.working_directory / ".tellus"
    
    def is_file_ignored(self, file_path: str) -> bool:
        """Check if a file should be ignored."""
        # Implementation would check against ignore patterns
        return any(
            self._matches_pattern(file_path, pattern)
            for pattern in self.state.ignore_patterns
        )
    
    def _matches_pattern(self, file_path: str, pattern: str) -> bool:
        """Check if file path matches ignore pattern."""
        import fnmatch
        return fnmatch.fnmatch(file_path, pattern)
    
    def get_untracked_files(self, directory: Optional[str] = None) -> List[str]:
        """Get list of untracked files in the repository."""
        # Implementation would scan filesystem and return untracked files
        # This is a placeholder for the actual implementation
        return []