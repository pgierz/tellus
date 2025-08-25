"""
Simulation file domain entities and value objects.

This module provides the domain model for files within simulation contexts,
including content classification, temporal associations, and metadata tracking.
"""

import time
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Dict, Optional, Set

from .archive import Checksum


class FileContentType(Enum):
    """Types of content that simulation files can represent."""
    ANALYSIS = "analysis"        # Analysis results, statistical summaries
    INPUT = "input"              # Initial conditions, boundary data, parameters  
    CONFIG = "config"            # Configuration files, namelist files
    RESTART = "restart"          # Restart files, checkpoint data
    OUTDATA = "outdata"          # Primary model output data
    LOG = "log"                  # Log files, diagnostic output
    SCRIPTS = "scripts"          # Scripts, executables, workflow files
    VIZ = "viz"                  # Visualization files, plots, movies
    AUXILIARY = "auxiliary"      # Supporting files, documentation
    FORCING = "forcing"          # Forcing data, external input


class FileImportance(Enum):
    """Importance levels for archive decisions."""
    CRITICAL = "critical"        # Essential for simulation integrity
    IMPORTANT = "important"      # Valuable for analysis but not critical
    OPTIONAL = "optional"        # Nice to have, can be regenerated
    TEMPORARY = "temporary"      # Can be safely discarded


@dataclass(frozen=True)
class FilePattern:
    """Value object for file pattern matching and classification."""
    glob_pattern: str
    content_type: FileContentType
    importance: FileImportance
    description: str
    
    def __post_init__(self):
        if not self.glob_pattern or not isinstance(self.glob_pattern, str):
            raise ValueError("File pattern must be a non-empty string")
        
        if not isinstance(self.content_type, FileContentType):
            raise ValueError("Content type must be a FileContentType enum")
            
        if not isinstance(self.importance, FileImportance):
            raise ValueError("Importance must be a FileImportance enum")


@dataclass
class SimulationFile:
    """
    Domain entity representing a file within a simulation context.
    
    This entity provides semantic meaning to files beyond their raw filesystem
    properties, including their role in the simulation, temporal associations,
    and archive relationships.
    """
    
    # Core Identity
    relative_path: str                           # Path within simulation structure
    size: Optional[int] = None                   # File size in bytes
    checksum: Optional[Checksum] = None          # File integrity checksum
    
    # Semantic Classification
    content_type: FileContentType = FileContentType.OUTDATA
    importance: FileImportance = FileImportance.IMPORTANT
    file_role: Optional[str] = None              # Specific role: "parameters", "restart", etc.
    
    # Temporal Information
    simulation_date: Optional[datetime] = None    # What simulation date this represents
    created_time: Optional[float] = None          # When file was created (timestamp)
    modified_time: Optional[float] = None         # When file was modified (timestamp)
    
    # Archive Context
    source_archive: Optional[str] = None          # Which archive this came from
    extraction_time: Optional[float] = None      # When file was extracted
    
    # Metadata and Tags
    tags: Set[str] = field(default_factory=set)
    attributes: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        """Validate the simulation file entity."""
        if not self.relative_path or not isinstance(self.relative_path, str):
            raise ValueError("Relative path must be a non-empty string")
        
        # Normalize path separators
        object.__setattr__(self, 'relative_path', str(Path(self.relative_path).as_posix()))
        
        if self.size is not None and (not isinstance(self.size, int) or self.size < 0):
            raise ValueError("File size must be a non-negative integer")
        
        if self.checksum is not None and not isinstance(self.checksum, Checksum):
            raise ValueError("Checksum must be a Checksum instance")
        
        if not isinstance(self.content_type, FileContentType):
            raise ValueError("Content type must be a FileContentType enum")
            
        if not isinstance(self.importance, FileImportance):
            raise ValueError("Importance must be a FileImportance enum")
        
        # Validate timestamps
        for time_field in ['created_time', 'modified_time', 'extraction_time']:
            value = getattr(self, time_field)
            if value is not None and (not isinstance(value, (int, float)) or value < 0):
                raise ValueError(f"{time_field} must be a non-negative number")
        
        if not isinstance(self.tags, set):
            raise ValueError("Tags must be a set")
        
        if not isinstance(self.attributes, dict):
            raise ValueError("Attributes must be a dictionary")
    
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
    
    def get_file_extension(self) -> str:
        """Get the file extension (without the dot)."""
        return Path(self.relative_path).suffix.lstrip('.')
    
    def get_filename(self) -> str:
        """Get the filename without directory path."""
        return Path(self.relative_path).name
    
    def get_directory(self) -> str:
        """Get the directory path (parent directory)."""
        return str(Path(self.relative_path).parent)
    
    def is_in_directory(self, directory_path: str) -> bool:
        """Check if file is within a specific directory."""
        return Path(self.relative_path).is_relative_to(directory_path)
    
    def matches_pattern(self, pattern: str) -> bool:
        """Check if file path matches a glob pattern."""
        from fnmatch import fnmatch
        return fnmatch(self.relative_path, pattern)
    
    def get_simulation_date_string(self, format_str: str = "%Y-%m-%d") -> Optional[str]:
        """Get simulation date as formatted string."""
        if self.simulation_date:
            return self.simulation_date.strftime(format_str)
        return None
    
    def get_created_datetime(self) -> Optional[datetime]:
        """Get created time as datetime object."""
        if self.created_time:
            return datetime.fromtimestamp(self.created_time)
        return None
    
    def get_modified_datetime(self) -> Optional[datetime]:
        """Get modified time as datetime object."""
        if self.modified_time:
            return datetime.fromtimestamp(self.modified_time)
        return None
    
    def update_from_filesystem(self, file_path: Path) -> None:
        """Update file metadata from actual filesystem information."""
        if file_path.exists():
            stat_info = file_path.stat()
            self.size = stat_info.st_size
            self.created_time = stat_info.st_ctime
            self.modified_time = stat_info.st_mtime
    
    def is_archivable(self) -> bool:
        """Determine if this file should be included in archives."""
        # Don't archive temporary files by default
        if self.importance == FileImportance.TEMPORARY:
            return False
        
        # Don't archive certain system files
        filename = self.get_filename()
        if filename.startswith('.') or filename in ['Thumbs.db', '.DS_Store']:
            return False
        
        return True
    
    def estimate_archive_priority(self) -> int:
        """Estimate priority for archive inclusion (higher = more important)."""
        priority = 0
        
        # Base priority from importance level
        importance_priority = {
            FileImportance.CRITICAL: 100,
            FileImportance.IMPORTANT: 50,
            FileImportance.OPTIONAL: 20,
            FileImportance.TEMPORARY: 0
        }
        priority += importance_priority[self.importance]
        
        # Boost priority for certain content types
        content_boost = {
            FileContentType.INPUT: 20,
            FileContentType.OUTDATA: 15,
            FileContentType.CONFIG: 10,
            FileContentType.RESTART: 12,
            FileContentType.ANALYSIS: 8,
            FileContentType.LOG: 2,
            FileContentType.SCRIPTS: 5,
            FileContentType.VIZ: 3,
            FileContentType.FORCING: 10,
            FileContentType.AUXILIARY: 1
        }
        priority += content_boost.get(self.content_type, 0)
        
        # Small files get slight boost (easier to include)
        if self.size and self.size < 1024 * 1024:  # Less than 1MB
            priority += 5
        
        return priority
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation for serialization."""
        return {
            'relative_path': self.relative_path,
            'size': self.size,
            'checksum': str(self.checksum) if self.checksum else None,
            'content_type': self.content_type.value,
            'importance': self.importance.value,
            'file_role': self.file_role,
            'simulation_date': self.simulation_date.isoformat() if self.simulation_date else None,
            'created_time': self.created_time,
            'modified_time': self.modified_time,
            'source_archive': self.source_archive,
            'extraction_time': self.extraction_time,
            'tags': list(self.tags),
            'attributes': self.attributes.copy()
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'SimulationFile':
        """Create SimulationFile from dictionary representation."""
        # Parse checksum
        checksum = None
        if data.get('checksum'):
            checksum_str = data['checksum']
            if ':' in checksum_str:
                algorithm, value = checksum_str.split(':', 1)
                checksum = Checksum(value=value, algorithm=algorithm)
            else:
                checksum = Checksum(value=checksum_str, algorithm='md5')
        
        # Parse datetime
        simulation_date = None
        if data.get('simulation_date'):
            simulation_date = datetime.fromisoformat(data['simulation_date'])
        
        return cls(
            relative_path=data['relative_path'],
            size=data.get('size'),
            checksum=checksum,
            content_type=FileContentType(data.get('content_type', 'outdata')),
            importance=FileImportance(data.get('importance', 'important')),
            file_role=data.get('file_role'),
            simulation_date=simulation_date,
            created_time=data.get('created_time'),
            modified_time=data.get('modified_time'),
            source_archive=data.get('source_archive'),
            extraction_time=data.get('extraction_time'),
            tags=set(data.get('tags', [])),
            attributes=data.get('attributes', {})
        )


@dataclass
class FileInventory:
    """
    Collection of simulation files with metadata and organization capabilities.
    
    This class manages collections of SimulationFile objects and provides
    querying, filtering, and organization capabilities.
    """
    
    files: Dict[str, SimulationFile] = field(default_factory=dict)  # Key: relative_path
    total_size: int = 0
    file_count: int = 0
    created_time: float = field(default_factory=time.time)
    
    def add_file(self, file: SimulationFile) -> None:
        """Add a file to the inventory."""
        if not isinstance(file, SimulationFile):
            raise ValueError("Must provide a SimulationFile instance")
        
        # Update existing file or add new one
        old_file = self.files.get(file.relative_path)
        self.files[file.relative_path] = file
        
        # Update counters
        if old_file is None:
            self.file_count += 1
            if file.size:
                self.total_size += file.size
        else:
            # Update size difference
            old_size = old_file.size or 0
            new_size = file.size or 0
            self.total_size += (new_size - old_size)
    
    def remove_file(self, relative_path: str) -> bool:
        """Remove a file from the inventory. Returns True if file was present."""
        if relative_path in self.files:
            file = self.files[relative_path]
            del self.files[relative_path]
            
            self.file_count -= 1
            if file.size:
                self.total_size -= file.size
            return True
        return False
    
    def get_file(self, relative_path: str) -> Optional[SimulationFile]:
        """Get a file by its relative path."""
        return self.files.get(relative_path)
    
    def list_files(self) -> list[SimulationFile]:
        """Get list of all files."""
        return list(self.files.values())
    
    def filter_by_content_type(self, content_type: FileContentType) -> list[SimulationFile]:
        """Filter files by content type."""
        return [f for f in self.files.values() if f.content_type == content_type]
    
    def filter_by_importance(self, importance: FileImportance) -> list[SimulationFile]:
        """Filter files by importance level."""
        return [f for f in self.files.values() if f.importance == importance]
    
    def filter_by_tags(self, tags: Set[str], match_all: bool = False) -> list[SimulationFile]:
        """Filter files by tags."""
        if match_all:
            return [f for f in self.files.values() if f.matches_all_tags(tags)]
        else:
            return [f for f in self.files.values() if f.matches_any_tag(tags)]
    
    def filter_by_pattern(self, pattern: str) -> list[SimulationFile]:
        """Filter files by glob pattern."""
        return [f for f in self.files.values() if f.matches_pattern(pattern)]
    
    def filter_by_directory(self, directory: str) -> list[SimulationFile]:
        """Filter files within a specific directory."""
        return [f for f in self.files.values() if f.is_in_directory(directory)]
    
    def get_archivable_files(self) -> list[SimulationFile]:
        """Get files that should be included in archives."""
        return [f for f in self.files.values() if f.is_archivable()]
    
    def get_content_type_summary(self) -> Dict[str, int]:
        """Get summary of files by content type."""
        summary = {}
        for file in self.files.values():
            content_type = file.content_type.value
            summary[content_type] = summary.get(content_type, 0) + 1
        return summary
    
    def get_size_by_content_type(self) -> Dict[str, int]:
        """Get total size by content type."""
        size_summary = {}
        for file in self.files.values():
            content_type = file.content_type.value
            file_size = file.size or 0
            size_summary[content_type] = size_summary.get(content_type, 0) + file_size
        return size_summary
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert inventory to dictionary for serialization."""
        return {
            'files': {path: file.to_dict() for path, file in self.files.items()},
            'total_size': self.total_size,
            'file_count': self.file_count,
            'created_time': self.created_time
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'FileInventory':
        """Create FileInventory from dictionary representation."""
        inventory = cls(
            total_size=data.get('total_size', 0),
            file_count=data.get('file_count', 0),
            created_time=data.get('created_time', time.time())
        )
        
        # Add files
        for path, file_data in data.get('files', {}).items():
            file = SimulationFile.from_dict(file_data)
            inventory.files[path] = file
        
        return inventory