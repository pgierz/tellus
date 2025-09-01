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

# Checksum value object (formerly from archive entity)
@dataclass(frozen=True)
class Checksum:
    """File checksum with algorithm information."""
    value: str
    algorithm: str = "sha256"


class FileType(Enum):
    """Physical types of files in the system."""
    REGULAR = "regular"          # Regular files
    ARCHIVE = "archive"          # Archive files (tar.gz, zip, etc.)
    DIRECTORY = "directory"      # Directory entries
    SYMLINK = "symlink"          # Symbolic links
    

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
    
    This unified entity represents all types of files in the system:
    - Regular files (individual data files, scripts, logs, etc.)
    - Archive files (tar.gz, zip files containing other files)
    - Directories (collection containers)
    - Symbolic links (references to other files)
    
    Provides semantic meaning beyond raw filesystem properties, including
    simulation roles, temporal associations, hierarchical relationships,
    and archive-specific functionality.
    """
    
    # Core Identity
    relative_path: str                           # Path within simulation structure
    size: Optional[int] = None                   # File size in bytes
    checksum: Optional[Checksum] = None          # File integrity checksum
    
    # File Type Classification
    file_type: FileType = FileType.REGULAR      # Physical type (regular, archive, directory, symlink)
    content_type: FileContentType = FileContentType.OUTDATA  # Semantic content type
    importance: FileImportance = FileImportance.IMPORTANT
    file_role: Optional[str] = None              # Specific role: "parameters", "restart", etc.
    
    # Hierarchical Relationships (NEW)
    parent_file_id: Optional[str] = None         # ID of parent file (if extracted from archive)
    contained_file_ids: Set[str] = field(default_factory=set)  # IDs of contained files (if archive)
    
    # Temporal Information
    simulation_date: Optional[datetime] = None    # What simulation date this represents
    created_time: Optional[float] = None          # When file was created (timestamp)
    modified_time: Optional[float] = None         # When file was modified (timestamp)
    
    # Archive Context (ENHANCED)
    source_archive: Optional[str] = None          # Primary archive this came from (backward compatibility)
    source_archives: Set[str] = field(default_factory=set)  # All archives that contain this file
    extraction_time: Optional[float] = None      # When file was extracted
    
    # Archive-Specific Properties (NEW - for files with file_type=ARCHIVE)
    archive_format: Optional[str] = None          # Archive format: "tar.gz", "zip", "tar", etc.
    compression_type: Optional[str] = None        # Compression: "gzip", "bzip2", "xz", etc.
    path_prefix_to_strip: Optional[str] = None    # Path prefix to remove during extraction
    is_split_archive: bool = False                # Whether this is part of a split archive
    split_archive_parts: Set[str] = field(default_factory=set)  # Other parts if split
    
    # Location and Storage
    location_name: Optional[str] = None           # Primary location where file is stored
    available_locations: Set[str] = field(default_factory=set)  # All locations with copies
    
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
        
        if not isinstance(self.file_type, FileType):
            raise ValueError("File type must be a FileType enum")
            
        if not isinstance(self.content_type, FileContentType):
            raise ValueError("Content type must be a FileContentType enum")
            
        if not isinstance(self.importance, FileImportance):
            raise ValueError("Importance must be a FileImportance enum")
        
        # Validate hierarchical relationship fields
        if self.parent_file_id is not None and (not isinstance(self.parent_file_id, str) or not self.parent_file_id):
            raise ValueError("Parent file ID must be a non-empty string if provided")
        
        if not isinstance(self.contained_file_ids, set):
            raise ValueError("Contained file IDs must be a set")
        
        # Validate archive-specific fields
        if not isinstance(self.is_split_archive, bool):
            raise ValueError("is_split_archive must be a boolean")
            
        if not isinstance(self.split_archive_parts, set):
            raise ValueError("Split archive parts must be a set")
        
        if not isinstance(self.available_locations, set):
            raise ValueError("Available locations must be a set")
        
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
            # Core Identity
            'relative_path': self.relative_path,
            'size': self.size,
            'checksum': str(self.checksum) if self.checksum else None,
            
            # File Type Classification
            'file_type': self.file_type.value,
            'content_type': self.content_type.value,
            'importance': self.importance.value,
            'file_role': self.file_role,
            
            # Hierarchical Relationships
            'parent_file_id': self.parent_file_id,
            'contained_file_ids': list(self.contained_file_ids),
            
            # Temporal Information
            'simulation_date': self.simulation_date.isoformat() if self.simulation_date else None,
            'created_time': self.created_time,
            'modified_time': self.modified_time,
            
            # Archive Context
            'source_archive': self.source_archive,
            'source_archives': list(self.source_archives),
            'extraction_time': self.extraction_time,
            
            # Archive-Specific Properties
            'archive_format': self.archive_format,
            'compression_type': self.compression_type,
            'path_prefix_to_strip': self.path_prefix_to_strip,
            'is_split_archive': self.is_split_archive,
            'split_archive_parts': list(self.split_archive_parts),
            
            # Location and Storage
            'location_name': self.location_name,
            'available_locations': list(self.available_locations),
            
            # Metadata and Tags
            'tags': list(self.tags),
            'attributes': self.attributes.copy()
        }
    
    @classmethod
    def from_archive_metadata(cls, archive_metadata: 'Any') -> 'SimulationFile':
        """
        Create a SimulationFile from ArchiveMetadata entity.
        This is a migration helper for Phase 2 of the unified architecture.
        
        Args:
            archive_metadata: ArchiveMetadata instance to convert
            
        Returns:
            SimulationFile instance representing the archive
        """
        from .archive import ArchiveMetadata, ArchiveType
        
        if not isinstance(archive_metadata, ArchiveMetadata):
            raise ValueError("Must provide an ArchiveMetadata instance")
        
        # Determine archive format from archive type
        archive_format = None
        compression_type = None
        if archive_metadata.archive_type == ArchiveType.COMPRESSED:
            # Try to infer format from archive paths
            for path in archive_metadata.archive_paths:
                if path.endswith('.tar.gz') or path.endswith('.tgz'):
                    archive_format = "tar.gz"
                    compression_type = "gzip"
                    break
                elif path.endswith('.tar.bz2'):
                    archive_format = "tar.bz2"
                    compression_type = "bzip2"
                    break
                elif path.endswith('.tar.xz'):
                    archive_format = "tar.xz"
                    compression_type = "xz"
                    break
                elif path.endswith('.tar'):
                    archive_format = "tar"
                    break
                elif path.endswith('.zip'):
                    archive_format = "zip"
                    break
            
            # Default to tar.gz if we can't determine
            if not archive_format:
                archive_format = "tar.gz"
                compression_type = "gzip"
        elif archive_metadata.archive_type == ArchiveType.SPLIT_TARBALL:
            archive_format = "tar.gz"
            compression_type = "gzip"
        
        # Create the SimulationFile with archive type
        simulation_file = cls(
            relative_path=str(archive_metadata.archive_id),
            size=archive_metadata.size,
            checksum=archive_metadata.checksum,
            file_type=FileType.ARCHIVE,
            content_type=FileContentType.OUTDATA,  # Default, can be updated
            importance=FileImportance.IMPORTANT,   # Archives are generally important
            simulation_date=datetime.fromisoformat(archive_metadata.simulation_date) if archive_metadata.simulation_date else None,
            created_time=archive_metadata.created_time,
            archive_format=archive_format,
            compression_type=compression_type,
            path_prefix_to_strip=archive_metadata.path_prefix_to_strip,
            location_name=archive_metadata.location,
            available_locations={archive_metadata.location},
            tags=archive_metadata.tags.copy(),
            attributes={
                'version': archive_metadata.version,
                'description': archive_metadata.description,
                'fragment_info': archive_metadata.fragment_info,
                'archive_paths': list(archive_metadata.archive_paths),
                'archive_type_legacy': archive_metadata.archive_type.value,
                'simulation_id': archive_metadata.simulation_id  # Keep simulation association
            }
        )
        
        # Handle split archives
        if archive_metadata.archive_type == ArchiveType.SPLIT_TARBALL:
            simulation_file.is_split_archive = True
            # Archive paths become split parts
            for path in archive_metadata.archive_paths:
                simulation_file.add_split_archive_part(path)
        
        # If archive has file inventory, add contained files
        if archive_metadata.file_inventory:
            for file_path, file_obj in archive_metadata.file_inventory.files.items():
                # Create a synthetic file ID for contained files
                file_id = f"{archive_metadata.archive_id}:{file_path}"
                simulation_file.add_contained_file(file_id)
        
        return simulation_file
    
    def to_archive_metadata(self) -> 'Any':
        """
        Convert this SimulationFile back to ArchiveMetadata for backward compatibility.
        Only works if this file has file_type=ARCHIVE.
        
        Returns:
            ArchiveMetadata instance
            
        Raises:
            ValueError: If this file is not an archive type
        """
        import time
        from .archive import ArchiveMetadata, ArchiveId, ArchiveType
        
        if self.file_type != FileType.ARCHIVE:
            raise ValueError("Can only convert archive-type SimulationFiles to ArchiveMetadata")
        
        # Determine archive type from properties
        archive_type = ArchiveType.COMPRESSED
        if self.is_split_archive:
            archive_type = ArchiveType.SPLIT_TARBALL
        elif self.attributes.get('archive_type_legacy'):
            archive_type = ArchiveType(self.attributes['archive_type_legacy'])
        
        # Get archive paths from split parts or attributes
        archive_paths = set()
        if self.is_split_archive:
            archive_paths = self.split_archive_parts.copy()
        elif 'archive_paths' in self.attributes:
            archive_paths = set(self.attributes['archive_paths'])
        
        return ArchiveMetadata(
            archive_id=ArchiveId(self.relative_path),
            location=self.location_name or '',
            archive_type=archive_type,
            simulation_id=self.attributes.get('simulation_id'),
            archive_paths=archive_paths,
            checksum=self.checksum,
            size=self.size,
            created_time=self.created_time or time.time(),
            simulation_date=self.simulation_date.isoformat() if self.simulation_date else None,
            version=self.attributes.get('version'),
            description=self.attributes.get('description'),
            tags=self.tags.copy(),
            path_prefix_to_strip=self.path_prefix_to_strip,
            fragment_info=self.attributes.get('fragment_info')
        )
    
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
            # Core Identity
            relative_path=data['relative_path'],
            size=data.get('size'),
            checksum=checksum,
            
            # File Type Classification
            file_type=FileType(data.get('file_type', 'regular')),
            content_type=FileContentType(data.get('content_type', 'outdata')),
            importance=FileImportance(data.get('importance', 'important')),
            file_role=data.get('file_role'),
            
            # Hierarchical Relationships
            parent_file_id=data.get('parent_file_id'),
            contained_file_ids=set(data.get('contained_file_ids', [])),
            
            # Temporal Information
            simulation_date=simulation_date,
            created_time=data.get('created_time'),
            modified_time=data.get('modified_time'),
            
            # Archive Context
            source_archive=data.get('source_archive'),
            source_archives=set(data.get('source_archives', [])),
            extraction_time=data.get('extraction_time'),
            
            # Archive-Specific Properties
            archive_format=data.get('archive_format'),
            compression_type=data.get('compression_type'),
            path_prefix_to_strip=data.get('path_prefix_to_strip'),
            is_split_archive=data.get('is_split_archive', False),
            split_archive_parts=set(data.get('split_archive_parts', [])),
            
            # Location and Storage
            location_name=data.get('location_name'),
            available_locations=set(data.get('available_locations', [])),
            
            # Metadata and Tags
            tags=set(data.get('tags', [])),
            attributes=data.get('attributes', {})
        )
    
    # Archive Reference Management Methods
    
    def add_archive_reference(self, archive_id: str) -> None:
        """
        Add a reference to an archive that contains this file.
        
        Args:
            archive_id: ID of the archive to reference
        """
        if not isinstance(archive_id, str) or not archive_id:
            raise ValueError("Archive ID must be a non-empty string")
        
        self.source_archives.add(archive_id)
        
        # Maintain backward compatibility with source_archive
        if self.source_archive is None:
            self.source_archive = archive_id
    
    def remove_archive_reference(self, archive_id: str) -> bool:
        """
        Remove a reference to an archive.
        
        Args:
            archive_id: ID of the archive to remove reference to
            
        Returns:
            True if reference was removed, False if it didn't exist
        """
        if archive_id in self.source_archives:
            self.source_archives.remove(archive_id)
            
            # Update source_archive if we removed the primary reference
            if self.source_archive == archive_id:
                self.source_archive = next(iter(self.source_archives), None)
            
            return True
        return False
    
    def is_in_archive(self, archive_id: str) -> bool:
        """
        Check if this file is contained in a specific archive.
        
        Args:
            archive_id: ID of the archive to check
            
        Returns:
            True if file is in the archive, False otherwise
        """
        return archive_id in self.source_archives
    
    def get_archive_references(self) -> Set[str]:
        """
        Get all archive references for this file.
        
        Returns:
            Set of archive IDs that contain this file
        """
        return self.source_archives.copy()
    
    def has_archive_references(self) -> bool:
        """
        Check if this file has any archive references.
        
        Returns:
            True if file has archive references, False otherwise
        """
        return len(self.source_archives) > 0
    
    def get_primary_archive(self) -> Optional[str]:
        """
        Get the primary archive for this file (for backward compatibility).
        
        Returns:
            Primary archive ID, or None if no archives referenced
        """
        return self.source_archive
    
    # NEW: Hierarchical Relationship Management Methods
    
    def set_parent_file(self, parent_file_id: str) -> None:
        """
        Set the parent file for this file (e.g., archive it was extracted from).
        
        Args:
            parent_file_id: ID of the parent file
        """
        if not isinstance(parent_file_id, str) or not parent_file_id:
            raise ValueError("Parent file ID must be a non-empty string")
        self.parent_file_id = parent_file_id
    
    def clear_parent_file(self) -> None:
        """Remove parent file relationship."""
        self.parent_file_id = None
    
    def has_parent(self) -> bool:
        """Check if this file has a parent file."""
        return self.parent_file_id is not None
    
    def add_contained_file(self, file_id: str) -> None:
        """
        Add a file ID to the set of files contained by this file.
        Only valid for archive-type files.
        
        Args:
            file_id: ID of the contained file
        """
        if not isinstance(file_id, str) or not file_id:
            raise ValueError("File ID must be a non-empty string")
        self.contained_file_ids.add(file_id)
    
    def remove_contained_file(self, file_id: str) -> bool:
        """
        Remove a file from the set of contained files.
        
        Args:
            file_id: ID of the file to remove
            
        Returns:
            True if file was removed, False if it wasn't contained
        """
        if file_id in self.contained_file_ids:
            self.contained_file_ids.remove(file_id)
            return True
        return False
    
    def contains_file(self, file_id: str) -> bool:
        """Check if this file contains another file (for archives)."""
        return file_id in self.contained_file_ids
    
    def get_contained_files(self) -> Set[str]:
        """Get set of file IDs contained by this file."""
        return self.contained_file_ids.copy()
    
    def get_contained_file_count(self) -> int:
        """Get count of files contained by this file."""
        return len(self.contained_file_ids)
    
    # NEW: Archive-Specific Methods
    
    def is_archive(self) -> bool:
        """Check if this file is an archive."""
        return self.file_type == FileType.ARCHIVE
    
    def is_directory(self) -> bool:
        """Check if this file is a directory."""
        return self.file_type == FileType.DIRECTORY
    
    def is_regular_file(self) -> bool:
        """Check if this file is a regular file."""
        return self.file_type == FileType.REGULAR
    
    def set_archive_properties(self, archive_format: str, compression_type: Optional[str] = None,
                              path_prefix_to_strip: Optional[str] = None) -> None:
        """
        Set archive-specific properties. Should only be called on archive-type files.
        
        Args:
            archive_format: Format of the archive (e.g., "tar.gz", "zip")
            compression_type: Type of compression (e.g., "gzip", "bzip2")
            path_prefix_to_strip: Path prefix to remove during extraction
        """
        if self.file_type != FileType.ARCHIVE:
            raise ValueError("Archive properties can only be set on archive-type files")
        
        self.archive_format = archive_format
        self.compression_type = compression_type
        self.path_prefix_to_strip = path_prefix_to_strip
    
    def truncate_path(self, path: str) -> str:
        """
        Truncate a path by removing the configured prefix.
        Used for archive files to provide clean extraction paths.
        
        Args:
            path: Path to truncate
            
        Returns:
            Truncated path, or original path if no prefix configured
        """
        if not self.path_prefix_to_strip or not path.startswith(self.path_prefix_to_strip):
            return path
        
        truncated = path[len(self.path_prefix_to_strip):]
        # Remove leading slash if present
        return truncated.lstrip('/')
    
    def add_split_archive_part(self, part_id: str) -> None:
        """Add a part ID to split archive parts."""
        if not isinstance(part_id, str) or not part_id:
            raise ValueError("Part ID must be a non-empty string")
        self.split_archive_parts.add(part_id)
        self.is_split_archive = True
    
    def remove_split_archive_part(self, part_id: str) -> bool:
        """Remove a part from split archive parts."""
        if part_id in self.split_archive_parts:
            self.split_archive_parts.remove(part_id)
            # If no parts left, mark as not split
            if not self.split_archive_parts:
                self.is_split_archive = False
            return True
        return False
    
    # NEW: Location Management Methods
    
    def set_primary_location(self, location_name: str) -> None:
        """Set the primary location for this file."""
        if not isinstance(location_name, str) or not location_name:
            raise ValueError("Location name must be a non-empty string")
        self.location_name = location_name
        self.available_locations.add(location_name)
    
    def add_location(self, location_name: str) -> None:
        """Add a location where this file is available."""
        if not isinstance(location_name, str) or not location_name:
            raise ValueError("Location name must be a non-empty string")
        self.available_locations.add(location_name)
        
        # Set as primary if no primary location set
        if self.location_name is None:
            self.location_name = location_name
    
    def remove_location(self, location_name: str) -> bool:
        """Remove a location. Returns True if location was removed."""
        if location_name in self.available_locations:
            self.available_locations.remove(location_name)
            
            # Update primary location if we removed it
            if self.location_name == location_name:
                self.location_name = next(iter(self.available_locations), None)
            
            return True
        return False
    
    def is_available_at_location(self, location_name: str) -> bool:
        """Check if file is available at a specific location."""
        return location_name in self.available_locations
    
    def get_available_locations(self) -> Set[str]:
        """Get set of all locations where this file is available."""
        return self.available_locations.copy()


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