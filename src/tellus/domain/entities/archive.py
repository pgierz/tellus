"""
Archive-related domain entities and value objects.
"""

import time
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Set

if TYPE_CHECKING:
    from .simulation_file import FileInventory


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
        
        # Validate ID format (alphanumeric, hyphens, underscores, periods)
        # NOTE: Periods are allowed but may cause issues in future web applications:
        # - URL path segments with periods might be interpreted as file extensions
        # - Some web frameworks treat periods specially in routing
        # - Consider URL encoding or alternative representations for web APIs
        allowed_chars = self.value.replace('-', '').replace('_', '').replace('.', '')
        if not allowed_chars.isalnum():
            raise ValueError("Archive ID can only contain alphanumeric characters, hyphens, underscores, and periods")
    
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
    simulation_id: Optional[str] = None  # Which simulation this archive contains parts of
    archive_paths: Set[str] = field(default_factory=set)  # All paths where this archive exists
    checksum: Optional[Checksum] = None
    size: Optional[int] = None
    created_time: float = field(default_factory=time.time)
    simulation_date: Optional[str] = None
    version: Optional[str] = None
    description: Optional[str] = None
    tags: Set[str] = field(default_factory=set)
    
    # Path truncation for file listings/extraction
    path_prefix_to_strip: Optional[str] = None  # Prefix to remove from file paths when listing/extracting
    
    # File inventory for tracking archive contents
    file_inventory: Optional['FileInventory'] = None
    
    # Fragment information for multi-archive simulations
    fragment_info: Optional[Dict[str, Any]] = None  # Which parts of simulation this contains
    
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
        
        if self.file_inventory is not None:
            # Import here to avoid circular imports
            from .simulation_file import FileInventory
            if not isinstance(self.file_inventory, FileInventory):
                raise ValueError("File inventory must be a FileInventory instance")
        
        if self.fragment_info is not None and not isinstance(self.fragment_info, dict):
            raise ValueError("Fragment info must be a dictionary")
    
    def add_tag(self, tag: str) -> None:
        """Add a tag to the archive."""
        if not isinstance(tag, str) or not tag:
            raise ValueError("Tag must be a non-empty string")
        self.tags.add(tag)
    
    def remove_tag(self, tag: str) -> bool:
        """Remove a tag from the archive. Returns True if tag was present."""
        return tag in self.tags and (self.tags.discard(tag), True)[1]
    
    def has_file_inventory(self) -> bool:
        """Check if archive has a file inventory."""
        return self.file_inventory is not None
    
    def get_file_count(self) -> int:
        """Get the number of files in this archive."""
        if self.file_inventory:
            return self.file_inventory.file_count
        return 0
    
    def get_content_summary(self) -> Dict[str, int]:
        """Get summary of files by content type."""
        if self.file_inventory:
            return self.file_inventory.get_content_type_summary()
        return {}
    
    def add_path(self, path: str) -> None:
        """Add a path where this archive exists."""
        if not isinstance(path, str) or not path:
            raise ValueError("Path must be a non-empty string")
        self.archive_paths.add(path)
    
    def remove_path(self, path: str) -> bool:
        """Remove a path. Returns True if path was present."""
        return path in self.archive_paths and (self.archive_paths.discard(path), True)[1]
    
    def get_paths(self) -> Set[str]:
        """Get all paths where this archive exists."""
        return self.archive_paths.copy()
    
    @property
    def archive_path(self) -> Optional[str]:
        """Backward compatibility: return first path or None."""
        return next(iter(self.archive_paths)) if self.archive_paths else None
    
    @archive_path.setter
    def archive_path(self, path: Optional[str]) -> None:
        """Backward compatibility setter with deprecation warning."""
        import warnings
        warnings.warn(
            f"Setting 'archive_path' is deprecated. Use 'add_path()' method instead to manage multiple archive paths.",
            DeprecationWarning,
            stacklevel=2
        )
        if path:
            # Clear existing paths and set the single path
            self.archive_paths.clear()
            self.archive_paths.add(path)
        else:
            self.archive_paths.clear()
    
    def truncate_path(self, file_path: str) -> str:
        """Apply path truncation to a file path if prefix is configured.
        
        Args:
            file_path: The full file path from the archive
            
        Returns:
            The file path with prefix stripped, or original if no prefix configured
        """
        if not self.path_prefix_to_strip:
            return file_path
            
        # Normalize paths to handle different separators
        import os.path
        normalized_prefix = os.path.normpath(self.path_prefix_to_strip)
        normalized_path = os.path.normpath(file_path)
        
        # Remove prefix if present
        if normalized_path.startswith(normalized_prefix):
            # Strip prefix and any leading separator
            truncated = normalized_path[len(normalized_prefix):].lstrip(os.sep)
            return truncated if truncated else os.path.basename(file_path)
        
        return file_path
    
    def get_archivable_files_count(self) -> int:
        """Get count of files that should be archived."""
        if self.file_inventory:
            return len(self.file_inventory.get_archivable_files())
        return 0
    
    def is_fragment(self) -> bool:
        """Check if this archive represents a fragment of a larger simulation."""
        return self.fragment_info is not None
    
    def get_fragment_description(self) -> str:
        """Get description of what simulation parts this fragment contains."""
        if not self.fragment_info:
            return "Complete archive"
        
        parts = []
        if 'date_range' in self.fragment_info:
            parts.append(f"dates: {self.fragment_info['date_range']}")
        if 'content_types' in self.fragment_info:
            parts.append(f"content: {', '.join(self.fragment_info['content_types'])}")
        if 'directories' in self.fragment_info:
            parts.append(f"dirs: {', '.join(self.fragment_info['directories'])}")
        
        return f"Fragment ({'; '.join(parts)})" if parts else "Fragment"
    
    def set_fragment_info(
        self, 
        date_range: Optional[str] = None,
        content_types: Optional[List[str]] = None,
        directories: Optional[List[str]] = None,
        description: Optional[str] = None
    ) -> None:
        """Set fragment information for this archive."""
        self.fragment_info = {}
        
        if date_range:
            self.fragment_info['date_range'] = date_range
        if content_types:
            self.fragment_info['content_types'] = content_types
        if directories:
            self.fragment_info['directories'] = directories
        if description:
            self.fragment_info['description'] = description
    
    def estimate_extraction_complexity(self) -> str:
        """Estimate complexity of extracting this archive."""
        if not self.file_inventory:
            return "unknown"
        
        file_count = self.file_inventory.file_count
        total_size = self.file_inventory.total_size
        
        if file_count < 10 and total_size < 10 * 1024**2:  # < 10 files, < 10MB
            return "simple"
        elif file_count < 100 and total_size < 100 * 1024**2:  # < 100 files, < 100MB
            return "moderate"
        elif file_count < 1000 and total_size < 1024**3:  # < 1000 files, < 1GB
            return "complex"
        else:
            return "very_complex"


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