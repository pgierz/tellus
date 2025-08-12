"""
Archive-related domain entities and value objects.
"""

import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, Optional, Set


class ArchiveType(Enum):
    """Types of archives supported by the system."""
    COMPRESSED = "compressed"  # tar.gz, tgz, tar
    SPLIT_TARBALL = "split_tarball"  # Split tar archives
    ORGANIZED = "organized"  # Archives with complex internal organization
    

class CacheCleanupPolicy(Enum):
    """Cache cleanup policies."""
    LRU = "lru"  # Least Recently Used
    MANUAL = "manual"  # Manual cleanup only
    SIZE_ONLY = "size_only"  # Clean up largest files first


@dataclass(frozen=True)
class ArchiveId:
    """Value object for archive identification."""
    value: str
    
    def __post_init__(self):
        if not self.value or not isinstance(self.value, str):
            raise ValueError("Archive ID must be a non-empty string")
        
        # Validate ID format (alphanumeric, hyphens, underscores)
        if not self.value.replace('-', '').replace('_', '').isalnum():
            raise ValueError("Archive ID can only contain alphanumeric characters, hyphens, and underscores")
    
    def __str__(self) -> str:
        return self.value


@dataclass(frozen=True)
class Checksum:
    """Value object for file checksums."""
    value: str
    algorithm: str = "md5"
    
    def __post_init__(self):
        if not self.value or not isinstance(self.value, str):
            raise ValueError("Checksum value must be a non-empty string")
        
        if not isinstance(self.algorithm, str) or not self.algorithm:
            raise ValueError("Checksum algorithm must be a non-empty string")
        
        # Validate checksum format for common algorithms
        if self.algorithm == "md5" and len(self.value) != 32:
            raise ValueError("MD5 checksum must be 32 characters")
        elif self.algorithm == "sha256" and len(self.value) != 64:
            raise ValueError("SHA256 checksum must be 64 characters")
    
    def __str__(self) -> str:
        return f"{self.algorithm}:{self.value}"


@dataclass
class FileMetadata:
    """Value object for file metadata within archives."""
    path: str
    size: Optional[int] = None
    checksum: Optional[Checksum] = None
    modified_time: Optional[float] = None
    tags: Set[str] = field(default_factory=set)
    
    def __post_init__(self):
        if not self.path or not isinstance(self.path, str):
            raise ValueError("File path must be a non-empty string")
        
        if self.size is not None and (not isinstance(self.size, int) or self.size < 0):
            raise ValueError("File size must be a non-negative integer")
        
        if self.checksum is not None and not isinstance(self.checksum, Checksum):
            raise ValueError("Checksum must be a Checksum instance")
        
        if (self.modified_time is not None and 
            (not isinstance(self.modified_time, (int, float)) or self.modified_time < 0)):
            raise ValueError("Modified time must be a non-negative number")
        
        if not isinstance(self.tags, set):
            raise ValueError("Tags must be a set")
    
    def add_tag(self, tag: str) -> None:
        """Add a tag to the file."""
        if not isinstance(tag, str) or not tag:
            raise ValueError("Tag must be a non-empty string")
        self.tags.add(tag)
    
    def remove_tag(self, tag: str) -> bool:
        """Remove a tag from the file. Returns True if tag was present."""
        return tag in self.tags and (self.tags.discard(tag), True)[1]
    
    def has_tag(self, tag: str) -> bool:
        """Check if file has a specific tag."""
        return tag in self.tags
    
    def matches_any_tag(self, tags: Set[str]) -> bool:
        """Check if file matches any of the given tags."""
        return bool(self.tags.intersection(tags))
    
    def matches_all_tags(self, tags: Set[str]) -> bool:
        """Check if file matches all of the given tags."""
        return tags.issubset(self.tags)


@dataclass
class ArchiveMetadata:
    """Domain entity for archive metadata."""
    archive_id: ArchiveId
    location: str
    archive_type: ArchiveType
    checksum: Optional[Checksum] = None
    size: Optional[int] = None
    created_time: float = field(default_factory=time.time)
    simulation_date: Optional[str] = None
    version: Optional[str] = None
    description: Optional[str] = None
    tags: Set[str] = field(default_factory=set)
    
    def __post_init__(self):
        if not isinstance(self.archive_id, ArchiveId):
            raise ValueError("Archive ID must be an ArchiveId instance")
        
        if not self.location or not isinstance(self.location, str):
            raise ValueError("Location must be a non-empty string")
        
        if not isinstance(self.archive_type, ArchiveType):
            raise ValueError("Archive type must be an ArchiveType enum")
        
        if self.checksum is not None and not isinstance(self.checksum, Checksum):
            raise ValueError("Checksum must be a Checksum instance")
        
        if self.size is not None and (not isinstance(self.size, int) or self.size < 0):
            raise ValueError("Size must be a non-negative integer")
        
        if not isinstance(self.created_time, (int, float)) or self.created_time < 0:
            raise ValueError("Created time must be a non-negative number")
        
        if not isinstance(self.tags, set):
            raise ValueError("Tags must be a set")
    
    def add_tag(self, tag: str) -> None:
        """Add a tag to the archive."""
        if not isinstance(tag, str) or not tag:
            raise ValueError("Tag must be a non-empty string")
        self.tags.add(tag)
    
    def remove_tag(self, tag: str) -> bool:
        """Remove a tag from the archive. Returns True if tag was present."""
        return tag in self.tags and (self.tags.discard(tag), True)[1]


@dataclass
class CacheConfiguration:
    """Value object for cache configuration."""
    cache_directory: str
    archive_size_limit: int = 50 * 1024**3  # 50 GB
    file_size_limit: int = 10 * 1024**3  # 10 GB
    cleanup_policy: CacheCleanupPolicy = CacheCleanupPolicy.LRU
    unified_cache: bool = False
    
    def __post_init__(self):
        if not self.cache_directory or not isinstance(self.cache_directory, str):
            raise ValueError("Cache directory must be a non-empty string")
        
        if not isinstance(self.archive_size_limit, int) or self.archive_size_limit <= 0:
            raise ValueError("Archive size limit must be a positive integer")
        
        if not isinstance(self.file_size_limit, int) or self.file_size_limit <= 0:
            raise ValueError("File size limit must be a positive integer")
        
        if not isinstance(self.cleanup_policy, CacheCleanupPolicy):
            raise ValueError("Cleanup policy must be a CacheCleanupPolicy enum")
        
        if not isinstance(self.unified_cache, bool):
            raise ValueError("Unified cache must be a boolean")


@dataclass
class LocationContext:
    """Value object for location context information."""
    path_prefix: Optional[str] = None
    overrides: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        if self.path_prefix is not None and not isinstance(self.path_prefix, str):
            raise ValueError("Path prefix must be a string if provided")
        
        if not isinstance(self.overrides, dict):
            raise ValueError("Overrides must be a dictionary")
        
        if not isinstance(self.metadata, dict):
            raise ValueError("Metadata must be a dictionary")
    
    def render_path_prefix(self, variables: Dict[str, str]) -> str:
        """Render path prefix with template variables."""
        if not self.path_prefix:
            return ""
        
        result = self.path_prefix
        for var, value in variables.items():
            placeholder = f"{{{{{var}}}}}"
            result = result.replace(placeholder, str(value))
        
        return result