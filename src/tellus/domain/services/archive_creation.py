"""
Archive creation service for building tarballs from simulation directories.

This service orchestrates the creation of compressed archive tarballs from simulation
directories, including file scanning, filtering, compression, and metadata generation.
It integrates with the domain model to provide rich archive creation capabilities
with progress tracking and robust error handling.
"""

import hashlib
import logging
import os
import tarfile
import tempfile
import time
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Union

from ..entities.archive import (ArchiveId, ArchiveMetadata, ArchiveType,
                                Checksum, LocationContext)
from ..entities.location import LocationEntity
from ..entities.simulation_file import (FileContentType, FileImportance,
                                        FileInventory, SimulationFile)
from .file_scanner import FileScanner, FileScanResult
from .sidecar_metadata import SidecarMetadata

logger = logging.getLogger(__name__)


class CompressionLevel(Enum):
    """Compression levels for archive creation."""
    NONE = 0     # No compression (tar only)
    FAST = 1     # Fast compression
    BALANCED = 6 # Balanced speed/size
    BEST = 9     # Best compression


@dataclass
class ArchiveCreationFilter:
    """Configuration for filtering files during archive creation."""
    
    # Content type filtering
    include_content_types: Optional[Set[FileContentType]] = None
    exclude_content_types: Optional[Set[FileContentType]] = None
    
    # Importance level filtering
    min_importance: Optional[FileImportance] = None
    include_importance: Optional[Set[FileImportance]] = None
    exclude_importance: Optional[Set[FileImportance]] = None
    
    # Pattern-based filtering
    include_patterns: Optional[List[str]] = None
    exclude_patterns: Optional[List[str]] = None
    
    # Directory-based filtering
    include_directories: Optional[List[str]] = None
    exclude_directories: Optional[List[str]] = None
    
    # Size-based filtering
    max_file_size: Optional[int] = None  # Maximum file size in bytes
    min_file_size: Optional[int] = None  # Minimum file size in bytes
    
    # Tag-based filtering
    include_tags: Optional[Set[str]] = None
    exclude_tags: Optional[Set[str]] = None
    
    # Custom filter function
    custom_filter: Optional[Callable[[SimulationFile], bool]] = None
    
    def apply_filter(self, file: SimulationFile) -> bool:
        """
        Apply all filters to determine if file should be included.
        
        Returns:
            True if file passes all filters, False otherwise
        """
        # Content type filters
        if self.include_content_types and file.content_type not in self.include_content_types:
            return False
        if self.exclude_content_types and file.content_type in self.exclude_content_types:
            return False
        
        # Importance filters
        if self.min_importance:
            importance_order = {
                FileImportance.TEMPORARY: 0,
                FileImportance.OPTIONAL: 1,
                FileImportance.IMPORTANT: 2,
                FileImportance.CRITICAL: 3
            }
            if importance_order[file.importance] < importance_order[self.min_importance]:
                return False
        
        if self.include_importance and file.importance not in self.include_importance:
            return False
        if self.exclude_importance and file.importance in self.exclude_importance:
            return False
        
        # Pattern filters
        if self.include_patterns:
            if not any(file.matches_pattern(pattern) for pattern in self.include_patterns):
                return False
        if self.exclude_patterns:
            if any(file.matches_pattern(pattern) for pattern in self.exclude_patterns):
                return False
        
        # Directory filters
        if self.include_directories:
            if not any(file.is_in_directory(dir_path) for dir_path in self.include_directories):
                return False
        if self.exclude_directories:
            if any(file.is_in_directory(dir_path) for dir_path in self.exclude_directories):
                return False
        
        # Size filters
        if file.size is not None:
            if self.max_file_size and file.size > self.max_file_size:
                return False
            if self.min_file_size and file.size < self.min_file_size:
                return False
        
        # Tag filters
        if self.include_tags and not file.matches_any_tag(self.include_tags):
            return False
        if self.exclude_tags and file.matches_any_tag(self.exclude_tags):
            return False
        
        # Custom filter
        if self.custom_filter and not self.custom_filter(file):
            return False
        
        return True


@dataclass
class ArchiveCreationConfig:
    """Configuration for archive creation operations."""
    
    # Archive settings
    compression_level: CompressionLevel = CompressionLevel.BALANCED
    preserve_permissions: bool = True
    preserve_timestamps: bool = True
    follow_symlinks: bool = False
    
    # Size and performance limits
    max_archive_size: Optional[int] = None  # Maximum archive size in bytes
    chunk_size: int = 64 * 1024  # Read chunk size for streaming
    
    # Validation settings
    compute_checksums: bool = True
    verify_after_creation: bool = True
    
    # Progress tracking
    progress_update_interval: int = 100  # Update progress every N files
    
    # Atomic operation settings
    use_temp_file: bool = True
    temp_dir: Optional[Path] = None
    cleanup_temp_on_error: bool = True


class ArchiveCreationResult:
    """Result object for archive creation operations."""
    
    def __init__(self):
        self.success: bool = False
        self.archive_path: Optional[Path] = None
        self.sidecar_path: Optional[Path] = None
        self.archive_metadata: Optional[ArchiveMetadata] = None
        
        # Statistics
        self.files_processed: int = 0
        self.files_included: int = 0
        self.files_excluded: int = 0
        self.bytes_processed: int = 0
        self.compression_ratio: Optional[float] = None
        self.creation_time: float = 0.0
        
        # Error tracking
        self.errors: List[str] = []
        self.warnings: List[str] = []
        
        # Performance metrics
        self.scan_time: float = 0.0
        self.compression_time: float = 0.0
        self.verification_time: float = 0.0
        self.metadata_time: float = 0.0
    
    @property
    def has_errors(self) -> bool:
        """Check if creation had errors."""
        return len(self.errors) > 0
    
    @property
    def has_warnings(self) -> bool:
        """Check if creation had warnings."""
        return len(self.warnings) > 0
    
    def add_error(self, message: str) -> None:
        """Add an error message."""
        self.errors.append(message)
        logger.error(f"Archive creation error: {message}")
    
    def add_warning(self, message: str) -> None:
        """Add a warning message."""
        self.warnings.append(message)
        logger.warning(f"Archive creation warning: {message}")


class ArchiveCreationService:
    """
    Domain service for creating archive tarballs from simulation directories.
    
    This service handles the complex process of scanning directories, filtering files,
    creating compressed tarballs, and generating metadata. It provides selective
    archive creation capabilities with comprehensive error handling and progress tracking.
    """
    
    def __init__(self, file_scanner: Optional[FileScanner] = None):
        """
        Initialize the archive creation service.
        
        Args:
            file_scanner: Optional custom file scanner (creates default if None)
        """
        self._file_scanner = file_scanner or FileScanner()
        self._logger = logger
    
    def create_archive(
        self,
        source_directory: Path,
        archive_path: Path,
        archive_id: ArchiveId,
        location: LocationEntity,
        simulation_id: Optional[str] = None,
        simulation_date: Optional[str] = None,
        version: Optional[str] = None,
        description: Optional[str] = None,
        tags: Optional[Set[str]] = None,
        filter_config: Optional[ArchiveCreationFilter] = None,
        creation_config: Optional[ArchiveCreationConfig] = None,
        progress_callback: Optional[Callable[[int, int, str], None]] = None
    ) -> ArchiveCreationResult:
        """
        Create a compressed archive from a simulation directory.
        
        Args:
            source_directory: Directory to archive
            archive_path: Path where archive should be created
            archive_id: Unique identifier for the archive
            location: Location entity where archive will be stored
            simulation_id: Optional simulation identifier
            simulation_date: Optional simulation date
            version: Optional version string
            description: Optional description
            tags: Optional set of tags
            filter_config: Optional filtering configuration
            creation_config: Optional creation configuration
            progress_callback: Optional callback for progress updates (current, total, step)
            
        Returns:
            ArchiveCreationResult with creation details and metadata
        """
        result = ArchiveCreationResult()
        creation_config = creation_config or ArchiveCreationConfig()
        filter_config = filter_config or ArchiveCreationFilter()
        tags = tags or set()
        
        start_time = time.time()
        
        try:
            self._logger.info(f"Starting archive creation: {archive_id} from {source_directory}")
            
            # Validate inputs
            validation_errors = self._validate_inputs(
                source_directory, archive_path, archive_id, location
            )
            if validation_errors:
                for error in validation_errors:
                    result.add_error(error)
                return result
            
            # Step 1: Scan source directory
            if progress_callback:
                progress_callback(0, 100, "Scanning source directory")
            
            scan_start = time.time()
            scan_result = self._scan_source_directory(
                source_directory, creation_config, progress_callback
            )
            result.scan_time = time.time() - scan_start
            
            if not scan_result.success:
                result.errors.extend(scan_result.errors)
                result.warnings.extend(scan_result.warnings)
                return result
            
            result.files_processed = scan_result.files_processed
            
            # Step 2: Apply filters to create filtered inventory
            if progress_callback:
                progress_callback(20, 100, "Filtering files")
            
            filtered_inventory = self._apply_filters(
                scan_result.inventory, filter_config, result
            )
            
            if filtered_inventory.file_count == 0:
                result.add_warning("No files passed filtering criteria")
                return result
            
            result.files_included = filtered_inventory.file_count
            result.files_excluded = scan_result.inventory.file_count - filtered_inventory.file_count
            
            # Step 3: Validate archive size limits
            if creation_config.max_archive_size:
                if filtered_inventory.total_size > creation_config.max_archive_size:
                    result.add_error(
                        f"Filtered content size ({filtered_inventory.total_size} bytes) "
                        f"exceeds maximum archive size ({creation_config.max_archive_size} bytes)"
                    )
                    return result
            
            # Step 4: Create archive metadata
            archive_metadata = self._create_archive_metadata(
                archive_id, location, simulation_id, simulation_date,
                version, description, tags, filtered_inventory
            )
            
            # Step 5: Create the actual tarball
            if progress_callback:
                progress_callback(30, 100, "Creating tarball")
            
            compress_start = time.time()
            tarball_success = self._create_tarball(
                source_directory, archive_path, filtered_inventory,
                creation_config, progress_callback, result
            )
            result.compression_time = time.time() - compress_start
            
            if not tarball_success:
                return result
            
            # Step 6: Compute final checksums and update metadata
            if progress_callback:
                progress_callback(80, 100, "Computing checksums")
            
            self._finalize_archive_metadata(archive_path, archive_metadata, creation_config)
            
            # Step 7: Verify archive integrity if requested
            if creation_config.verify_after_creation:
                if progress_callback:
                    progress_callback(90, 100, "Verifying archive")
                
                verify_start = time.time()
                verification_success = self._verify_archive_integrity(
                    archive_path, filtered_inventory, result
                )
                result.verification_time = time.time() - verify_start
                
                if not verification_success:
                    return result
            
            # Step 8: Create sidecar metadata file
            if progress_callback:
                progress_callback(95, 100, "Creating metadata")
            
            metadata_start = time.time()
            sidecar_path = self._create_sidecar_metadata(
                archive_path, archive_metadata, result
            )
            result.metadata_time = time.time() - metadata_start
            
            # Finalize result
            result.success = True
            result.archive_path = archive_path
            result.sidecar_path = sidecar_path
            result.archive_metadata = archive_metadata
            result.bytes_processed = filtered_inventory.total_size
            
            # Calculate compression ratio
            if archive_path.exists():
                archive_size = archive_path.stat().st_size
                if filtered_inventory.total_size > 0:
                    result.compression_ratio = archive_size / filtered_inventory.total_size
            
            result.creation_time = time.time() - start_time
            
            if progress_callback:
                progress_callback(100, 100, "Complete")
            
            self._logger.info(
                f"Archive creation completed: {archive_id}, "
                f"{result.files_included} files, "
                f"{result.bytes_processed / (1024**2):.1f} MB in {result.creation_time:.1f}s"
            )
            
            return result
            
        except Exception as e:
            result.add_error(f"Archive creation failed with exception: {str(e)}")
            self._logger.exception("Archive creation failed")
            
            # Cleanup on error if requested
            if creation_config.cleanup_temp_on_error:
                self._cleanup_on_error(archive_path, result)
            
            return result
    
    def create_selective_archive(
        self,
        source_directory: Path,
        archive_path: Path,
        archive_id: ArchiveId,
        location: LocationEntity,
        file_list: List[str],
        simulation_id: Optional[str] = None,
        simulation_date: Optional[str] = None,
        version: Optional[str] = None,
        description: Optional[str] = None,
        tags: Optional[Set[str]] = None,
        creation_config: Optional[ArchiveCreationConfig] = None,
        progress_callback: Optional[Callable[[int, int, str], None]] = None
    ) -> ArchiveCreationResult:
        """
        Create an archive from a specific list of files.
        
        Args:
            source_directory: Base directory containing the files
            archive_path: Path where archive should be created
            archive_id: Unique identifier for the archive
            location: Location entity where archive will be stored
            file_list: List of relative file paths to include
            simulation_id: Optional simulation identifier
            simulation_date: Optional simulation date
            version: Optional version string
            description: Optional description
            tags: Optional set of tags
            creation_config: Optional creation configuration
            progress_callback: Optional callback for progress updates
            
        Returns:
            ArchiveCreationResult with creation details and metadata
        """
        result = ArchiveCreationResult()
        creation_config = creation_config or ArchiveCreationConfig()
        tags = tags or set()
        
        start_time = time.time()
        
        try:
            self._logger.info(f"Starting selective archive creation: {archive_id}")
            
            # Validate inputs
            validation_errors = self._validate_inputs(
                source_directory, archive_path, archive_id, location
            )
            if validation_errors:
                for error in validation_errors:
                    result.add_error(error)
                return result
            
            if not file_list:
                result.add_error("File list cannot be empty for selective archiving")
                return result
            
            # Step 1: Scan specific files
            if progress_callback:
                progress_callback(0, 100, "Scanning specified files")
            
            scan_start = time.time()
            scan_result = self._file_scanner.scan_file_list(
                source_directory, file_list, 
                simulation_context={'simulation_id': simulation_id},
                compute_checksums=creation_config.compute_checksums,
                progress_callback=lambda current, total: progress_callback(
                    int(10 + (current / total) * 10), 100, "Scanning files"
                ) if progress_callback else None
            )
            result.scan_time = time.time() - scan_start
            
            if not scan_result.success:
                result.errors.extend(scan_result.errors)
                result.warnings.extend(scan_result.warnings)
                return result
            
            result.files_processed = scan_result.files_processed
            result.files_included = scan_result.inventory.file_count
            result.files_excluded = len(file_list) - scan_result.inventory.file_count
            
            # Continue with standard archive creation process
            archive_metadata = self._create_archive_metadata(
                archive_id, location, simulation_id, simulation_date,
                version, description, tags, scan_result.inventory
            )
            
            # Create tarball, verify, and generate metadata (same as full archive)
            if progress_callback:
                progress_callback(30, 100, "Creating tarball")
            
            compress_start = time.time()
            tarball_success = self._create_tarball(
                source_directory, archive_path, scan_result.inventory,
                creation_config, progress_callback, result
            )
            result.compression_time = time.time() - compress_start
            
            if not tarball_success:
                return result
            
            # Finalize the same way as regular archives
            self._finalize_archive_metadata(archive_path, archive_metadata, creation_config)
            
            if creation_config.verify_after_creation:
                if progress_callback:
                    progress_callback(90, 100, "Verifying archive")
                
                verify_start = time.time()
                verification_success = self._verify_archive_integrity(
                    archive_path, scan_result.inventory, result
                )
                result.verification_time = time.time() - verify_start
                
                if not verification_success:
                    return result
            
            if progress_callback:
                progress_callback(95, 100, "Creating metadata")
            
            metadata_start = time.time()
            sidecar_path = self._create_sidecar_metadata(
                archive_path, archive_metadata, result
            )
            result.metadata_time = time.time() - metadata_start
            
            # Finalize result
            result.success = True
            result.archive_path = archive_path
            result.sidecar_path = sidecar_path
            result.archive_metadata = archive_metadata
            result.bytes_processed = scan_result.inventory.total_size
            result.creation_time = time.time() - start_time
            
            if archive_path.exists():
                archive_size = archive_path.stat().st_size
                if scan_result.inventory.total_size > 0:
                    result.compression_ratio = archive_size / scan_result.inventory.total_size
            
            if progress_callback:
                progress_callback(100, 100, "Complete")
            
            self._logger.info(f"Selective archive creation completed: {archive_id}")
            return result
            
        except Exception as e:
            result.add_error(f"Selective archive creation failed: {str(e)}")
            self._logger.exception("Selective archive creation failed")
            
            if creation_config.cleanup_temp_on_error:
                self._cleanup_on_error(archive_path, result)
            
            return result
    
    def estimate_archive_size(
        self,
        source_directory: Path,
        filter_config: Optional[ArchiveCreationFilter] = None,
        compression_level: CompressionLevel = CompressionLevel.BALANCED
    ) -> Dict[str, Any]:
        """
        Estimate the size and characteristics of a potential archive.
        
        Args:
            source_directory: Directory to analyze
            filter_config: Optional filtering configuration
            compression_level: Compression level for size estimation
            
        Returns:
            Dictionary with size estimates and file statistics
        """
        try:
            # Scan directory
            scan_result = self._file_scanner.scan_directory(
                source_directory, max_workers=2, compute_checksums=False
            )
            
            if not scan_result.success:
                return {
                    'success': False,
                    'errors': scan_result.errors,
                    'warnings': scan_result.warnings
                }
            
            # Apply filters if provided
            filtered_inventory = scan_result.inventory
            if filter_config:
                filtered_inventory = self._apply_filters(
                    scan_result.inventory, filter_config, ArchiveCreationResult()
                )
            
            # Estimate compression ratio based on file types
            estimated_ratio = self._estimate_compression_ratio(
                filtered_inventory, compression_level
            )
            
            # Calculate estimates
            uncompressed_size = filtered_inventory.total_size
            estimated_compressed_size = int(uncompressed_size * estimated_ratio)
            
            return {
                'success': True,
                'uncompressed_size': uncompressed_size,
                'estimated_compressed_size': estimated_compressed_size,
                'estimated_compression_ratio': estimated_ratio,
                'file_count': filtered_inventory.file_count,
                'files_excluded': scan_result.inventory.file_count - filtered_inventory.file_count,
                'content_summary': filtered_inventory.get_content_type_summary(),
                'size_by_content': filtered_inventory.get_size_by_content_type(),
                'scan_time': scan_result.scan_time
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': f"Size estimation failed: {str(e)}"
            }
    
    def _validate_inputs(
        self,
        source_directory: Path,
        archive_path: Path,
        archive_id: ArchiveId,
        location: LocationEntity
    ) -> List[str]:
        """Validate input parameters for archive creation."""
        errors = []
        
        if not source_directory.exists():
            errors.append(f"Source directory does not exist: {source_directory}")
        elif not source_directory.is_dir():
            errors.append(f"Source path is not a directory: {source_directory}")
        
        if archive_path.exists():
            errors.append(f"Archive path already exists: {archive_path}")
        
        parent_dir = archive_path.parent
        if not parent_dir.exists():
            try:
                parent_dir.mkdir(parents=True)
            except Exception as e:
                errors.append(f"Cannot create archive parent directory: {e}")
        
        if not isinstance(archive_id, ArchiveId):
            errors.append("Archive ID must be an ArchiveId instance")
        
        if not isinstance(location, LocationEntity):
            errors.append("Location must be a LocationEntity instance")
        
        return errors
    
    def _scan_source_directory(
        self,
        source_directory: Path,
        creation_config: ArchiveCreationConfig,
        progress_callback: Optional[Callable[[int, int, str], None]]
    ) -> FileScanResult:
        """Scan the source directory and build file inventory."""
        return self._file_scanner.scan_directory(
            source_directory,
            simulation_context=None,
            include_patterns=None,
            exclude_patterns=None,
            compute_checksums=creation_config.compute_checksums,
            max_workers=4,
            progress_callback=lambda current, total: progress_callback(
                int(5 + (current / total) * 15), 100, f"Scanning files ({current}/{total})"
            ) if progress_callback else None
        )
    
    def _apply_filters(
        self,
        inventory: FileInventory,
        filter_config: ArchiveCreationFilter,
        result: ArchiveCreationResult
    ) -> FileInventory:
        """Apply filtering configuration to create filtered inventory."""
        filtered_inventory = FileInventory()
        
        for file in inventory.list_files():
            if filter_config.apply_filter(file):
                filtered_inventory.add_file(file)
            else:
                # Could add more detailed exclusion logging here
                pass
        
        return filtered_inventory
    
    def _create_archive_metadata(
        self,
        archive_id: ArchiveId,
        location: LocationEntity,
        simulation_id: Optional[str],
        simulation_date: Optional[str],
        version: Optional[str],
        description: Optional[str],
        tags: Set[str],
        inventory: FileInventory
    ) -> ArchiveMetadata:
        """Create archive metadata entity."""
        return ArchiveMetadata(
            archive_id=archive_id,
            location=location.name,
            archive_type=ArchiveType.COMPRESSED,
            simulation_id=simulation_id,
            simulation_date=simulation_date,
            version=version,
            description=description,
            tags=tags,
            file_inventory=inventory
        )
    
    def _create_tarball(
        self,
        source_directory: Path,
        archive_path: Path,
        inventory: FileInventory,
        creation_config: ArchiveCreationConfig,
        progress_callback: Optional[Callable[[int, int, str], None]],
        result: ArchiveCreationResult
    ) -> bool:
        """Create the actual tarball file."""
        try:
            # Determine compression mode
            if creation_config.compression_level == CompressionLevel.NONE:
                mode = 'w'
                temp_suffix = '.tar'
            else:
                mode = f'w:gz'
                temp_suffix = '.tar.gz'
            
            # Use temporary file if requested
            if creation_config.use_temp_file:
                temp_dir = creation_config.temp_dir or archive_path.parent
                temp_fd, temp_path = tempfile.mkstemp(suffix=temp_suffix, dir=temp_dir)
                os.close(temp_fd)  # Close the file descriptor
                working_path = Path(temp_path)
            else:
                working_path = archive_path
            
            try:
                files_to_archive = inventory.list_files()
                total_files = len(files_to_archive)
                
                with tarfile.open(working_path, mode) as tar:
                    # Set compression level for gzip
                    if hasattr(tar, 'compresslevel') and creation_config.compression_level != CompressionLevel.NONE:
                        tar.compresslevel = creation_config.compression_level.value
                    
                    for i, sim_file in enumerate(files_to_archive):
                        try:
                            full_path = source_directory / sim_file.relative_path
                            
                            if not full_path.exists():
                                result.add_warning(f"File not found during archiving: {sim_file.relative_path}")
                                continue
                            
                            # Create tarinfo with custom settings
                            tarinfo = tar.gettarinfo(str(full_path), sim_file.relative_path)
                            
                            if not creation_config.preserve_permissions:
                                # Use default permissions
                                if tarinfo.isfile():
                                    tarinfo.mode = 0o644
                                elif tarinfo.isdir():
                                    tarinfo.mode = 0o755
                            
                            if not creation_config.preserve_timestamps:
                                # Use current time
                                tarinfo.mtime = int(time.time())
                            
                            # Handle symlinks
                            if tarinfo.islnk() or tarinfo.issym():
                                if not creation_config.follow_symlinks:
                                    tar.addfile(tarinfo)
                                    continue
                                else:
                                    # Follow the symlink
                                    if full_path.is_file():
                                        with open(full_path, 'rb') as f:
                                            tarinfo.size = full_path.stat().st_size
                                            tar.addfile(tarinfo, f)
                                    continue
                            
                            # Add regular files
                            if tarinfo.isfile():
                                with open(full_path, 'rb') as f:
                                    tar.addfile(tarinfo, f)
                            else:
                                tar.addfile(tarinfo)
                            
                            # Update progress
                            if progress_callback and (i + 1) % creation_config.progress_update_interval == 0:
                                progress_percent = int(30 + ((i + 1) / total_files) * 50)
                                progress_callback(
                                    progress_percent, 100,
                                    f"Archiving files ({i + 1}/{total_files})"
                                )
                                
                        except Exception as e:
                            result.add_error(f"Failed to archive file {sim_file.relative_path}: {str(e)}")
                
                # Move temp file to final location if using temp file
                if creation_config.use_temp_file:
                    working_path.rename(archive_path)
                
                return True
                
            except Exception as e:
                # Clean up temp file on error
                if creation_config.use_temp_file and working_path.exists():
                    working_path.unlink()
                raise e
                
        except Exception as e:
            result.add_error(f"Failed to create tarball: {str(e)}")
            return False
    
    def _finalize_archive_metadata(
        self,
        archive_path: Path,
        metadata: ArchiveMetadata,
        creation_config: ArchiveCreationConfig
    ) -> None:
        """Finalize archive metadata with size and checksum information."""
        if archive_path.exists():
            stat_info = archive_path.stat()
            metadata.size = stat_info.st_size
            
            # Compute archive checksum if requested
            if creation_config.compute_checksums:
                checksum = self._compute_file_checksum(archive_path)
                metadata.checksum = checksum
    
    def _verify_archive_integrity(
        self,
        archive_path: Path,
        inventory: FileInventory,
        result: ArchiveCreationResult
    ) -> bool:
        """Verify the integrity of the created archive."""
        try:
            with tarfile.open(archive_path, 'r:*') as tar:
                # Get list of files in archive
                archive_members = set(tar.getnames())
                inventory_files = set(f.relative_path for f in inventory.list_files())
                
                # Check that all expected files are present
                missing_files = inventory_files - archive_members
                if missing_files:
                    for missing in missing_files:
                        result.add_error(f"File missing from archive: {missing}")
                    return False
                
                # Check for unexpected files (shouldn't happen, but good to verify)
                unexpected_files = archive_members - inventory_files
                if unexpected_files:
                    for unexpected in unexpected_files:
                        result.add_warning(f"Unexpected file in archive: {unexpected}")
                
                # Try to read a few random files to verify they're not corrupted
                import random
                sample_size = min(10, len(archive_members))
                sample_files = random.sample(list(archive_members), sample_size)
                
                for member_name in sample_files:
                    try:
                        member = tar.getmember(member_name)
                        if member.isfile():
                            # Try to read the file
                            f = tar.extractfile(member)
                            if f:
                                f.read(1024)  # Read first 1KB
                                f.close()
                    except Exception as e:
                        result.add_error(f"Archive verification failed for {member_name}: {str(e)}")
                        return False
            
            return True
            
        except Exception as e:
            result.add_error(f"Archive verification failed: {str(e)}")
            return False
    
    def _create_sidecar_metadata(
        self,
        archive_path: Path,
        archive_metadata: ArchiveMetadata,
        result: ArchiveCreationResult
    ) -> Optional[Path]:
        """Create sidecar metadata file."""
        try:
            sidecar_path = SidecarMetadata.write_sidecar_file(archive_path, archive_metadata)
            return sidecar_path
            
        except Exception as e:
            result.add_error(f"Failed to create sidecar metadata: {str(e)}")
            return None
    
    def _compute_file_checksum(self, file_path: Path, algorithm: str = 'md5') -> Optional[Checksum]:
        """Compute checksum for a file."""
        try:
            hash_obj = hashlib.new(algorithm)
            
            with open(file_path, 'rb') as f:
                # Read file in chunks to handle large files efficiently
                for chunk in iter(lambda: f.read(64 * 1024), b''):
                    hash_obj.update(chunk)
            
            return Checksum(value=hash_obj.hexdigest(), algorithm=algorithm)
            
        except Exception as e:
            self._logger.error(f"Error computing checksum for {file_path}: {str(e)}")
            return None
    
    def _estimate_compression_ratio(
        self,
        inventory: FileInventory,
        compression_level: CompressionLevel
    ) -> float:
        """Estimate compression ratio based on file types and compression level."""
        if compression_level == CompressionLevel.NONE:
            return 1.0
        
        # Base compression ratios by file type (empirical estimates)
        base_ratios = {
            'output': 0.3,      # Scientific data compresses well
            'input': 0.6,       # Configuration files are typically small text
            'log': 0.4,         # Log files compress reasonably well
            'config': 0.7,      # Small text files
            'diagnostic': 0.4,  # Similar to output data
            'intermediate': 0.5, # Mixed content
            'metadata': 0.8     # Usually small files
        }
        
        # Adjust for compression level
        level_multipliers = {
            CompressionLevel.FAST: 1.2,
            CompressionLevel.BALANCED: 1.0,
            CompressionLevel.BEST: 0.8
        }
        
        total_size = 0
        weighted_ratio = 0.0
        
        size_by_content = inventory.get_size_by_content_type()
        
        for content_type, size in size_by_content.items():
            ratio = base_ratios.get(content_type, 0.6)  # Default ratio
            ratio *= level_multipliers[compression_level]
            
            weighted_ratio += size * ratio
            total_size += size
        
        if total_size == 0:
            return 0.6  # Default ratio if no files
        
        return min(weighted_ratio / total_size, 1.0)  # Cap at 1.0 (no expansion)
    
    def _cleanup_on_error(self, archive_path: Path, result: ArchiveCreationResult) -> None:
        """Clean up files on error if requested."""
        try:
            if archive_path.exists():
                archive_path.unlink()
                self._logger.info(f"Cleaned up failed archive: {archive_path}")
            
            # Also clean up sidecar if it exists
            sidecar_path = SidecarMetadata.create_sidecar_path(archive_path)
            if sidecar_path.exists():
                sidecar_path.unlink()
                self._logger.info(f"Cleaned up sidecar metadata: {sidecar_path}")
                
        except Exception as e:
            result.add_warning(f"Failed to cleanup files on error: {str(e)}")