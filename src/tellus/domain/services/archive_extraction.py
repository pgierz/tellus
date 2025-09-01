"""
Archive extraction service for extracting tarballs to any location.

This service orchestrates the extraction of archive tarballs to various storage locations,
including selective extraction with rich filtering capabilities. It integrates with the
domain model to provide intelligent extraction based on sidecar metadata and supports
fragment assembly for multi-archive simulations.
"""

import logging
import os
import re
import tarfile
import tempfile
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union

from ..entities.archive import ArchiveId, ArchiveMetadata, ArchiveType
from ..entities.location import LocationEntity
from ..entities.simulation_file import (FileContentType, FileImportance,
                                        FileInventory, SimulationFile)
from .sidecar_metadata import SidecarMetadata

logger = logging.getLogger(__name__)


class ConflictResolution(Enum):
    """Strategies for resolving file conflicts during extraction."""
    SKIP = "skip"              # Skip conflicting files
    OVERWRITE = "overwrite"    # Overwrite existing files
    NEWEST = "newest"          # Keep newest file based on timestamp
    LARGEST = "largest"        # Keep largest file
    MERGE = "merge"            # Attempt to merge (directories only)
    FAIL = "fail"              # Fail extraction on conflict


class ExtractionMode(Enum):
    """Modes for archive extraction."""
    FULL = "full"              # Extract entire archive
    SELECTIVE = "selective"    # Extract based on filters
    FRAGMENT = "fragment"      # Extract as part of fragment assembly


@dataclass
class DateRange:
    """Date range for temporal filtering."""
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    
    def contains_date(self, date: datetime) -> bool:
        """Check if date falls within this range."""
        if self.start_date and date < self.start_date:
            return False
        if self.end_date and date > self.end_date:
            return False
        return True
    
    @classmethod
    def from_string(cls, date_string: str) -> 'DateRange':
        """
        Create date range from string representations.
        
        Supported formats:
        - "2024-03-01" (single date)
        - "2024-03-01:2024-03-31" (date range)
        - "2024-03" (entire month)
        - "202403*" (month with wildcard)
        """
        if ':' in date_string:
            # Date range
            start_str, end_str = date_string.split(':', 1)
            start_date = cls._parse_date_component(start_str)
            end_date = cls._parse_date_component(end_str)
            return cls(start_date=start_date, end_date=end_date)
        else:
            # Single date or pattern
            date = cls._parse_date_component(date_string)
            if '*' in date_string:
                # Pattern like "202403*" - treat as month range
                if len(date_string) == 7 and date_string.endswith('*'):
                    # Month pattern
                    year = int(date_string[:4])
                    month = int(date_string[4:6])
                    start_date = datetime(year, month, 1)
                    # End of month
                    if month == 12:
                        end_date = datetime(year + 1, 1, 1) - timedelta(days=1)
                    else:
                        end_date = datetime(year, month + 1, 1) - timedelta(days=1)
                    return cls(start_date=start_date, end_date=end_date)
            return cls(start_date=date, end_date=date)
    
    @staticmethod
    def _parse_date_component(date_str: str) -> datetime:
        """Parse individual date component."""
        date_str = date_str.strip('*')
        
        # Try different date formats
        formats = [
            '%Y-%m-%d',     # 2024-03-15
            '%Y-%m',        # 2024-03 (first day of month)
            '%Y%m%d',       # 20240315
            '%Y%m',         # 202403 (first day of month)
            '%Y',           # 2024 (first day of year)
        ]
        
        for fmt in formats:
            try:
                return datetime.strptime(date_str, fmt)
            except ValueError:
                continue
        
        raise ValueError(f"Unable to parse date: {date_str}")


@dataclass
class ArchiveExtractionFilter:
    """Configuration for filtering files during archive extraction."""
    
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
    max_file_size: Optional[int] = None
    min_file_size: Optional[int] = None
    
    # Tag-based filtering
    include_tags: Optional[Set[str]] = None
    exclude_tags: Optional[Set[str]] = None
    
    # Temporal filtering
    date_range: Optional[DateRange] = None
    strftime_patterns: Optional[List[str]] = None  # From sidecar metadata
    
    # Custom filter function
    custom_filter: Optional[Callable[[SimulationFile], bool]] = None
    
    def apply_filter(self, file: SimulationFile, filename: str = None) -> bool:
        """
        Apply all filters to determine if file should be extracted.
        
        Args:
            file: SimulationFile entity to check
            filename: Optional filename for pattern matching
            
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
        
        # Temporal filtering
        if self.date_range and file.simulation_date:
            if not self.date_range.contains_date(file.simulation_date):
                return False
        
        # Pattern-based date filtering
        if self.date_range and self.strftime_patterns and filename:
            if not self._matches_date_patterns(filename):
                return False
        
        # Custom filter
        if self.custom_filter and not self.custom_filter(file):
            return False
        
        return True
    
    def _matches_date_patterns(self, filename: str) -> bool:
        """Check if filename matches any strftime patterns within date range."""
        if not self.strftime_patterns or not self.date_range:
            return True
        
        for pattern in self.strftime_patterns:
            dates = self._extract_dates_from_filename(filename, pattern)
            for date in dates:
                if self.date_range.contains_date(date):
                    return True
        
        return False
    
    def _extract_dates_from_filename(self, filename: str, strftime_pattern: str) -> List[datetime]:
        """Extract dates from filename using strftime pattern."""
        dates = []
        
        # Convert strftime pattern to regex
        regex_pattern = strftime_pattern
        replacements = {
            '%Y': r'(\d{4})',
            '%m': r'(\d{2})',
            '%d': r'(\d{2})',
            '%H': r'(\d{2})',
            '%M': r'(\d{2})',
            '%S': r'(\d{2})'
        }
        
        for strftime_code, regex in replacements.items():
            regex_pattern = regex_pattern.replace(strftime_code, regex)
        
        try:
            matches = re.finditer(regex_pattern, filename)
            for match in matches:
                try:
                    # Parse the matched date
                    matched_str = match.group(0)
                    date = datetime.strptime(matched_str, strftime_pattern)
                    dates.append(date)
                except ValueError:
                    continue
        except re.error:
            # Invalid regex pattern
            pass
        
        return dates


@dataclass
class ArchiveExtractionConfig:
    """Configuration for archive extraction operations."""
    
    # Extraction behavior
    preserve_permissions: bool = True
    preserve_timestamps: bool = True
    create_directories: bool = True
    conflict_resolution: ConflictResolution = ConflictResolution.NEWEST
    
    # Performance settings
    chunk_size: int = 64 * 1024
    max_concurrent_extractions: int = 4
    
    # Safety settings
    use_atomic_extraction: bool = True
    temp_dir: Optional[Path] = None
    verify_after_extraction: bool = True
    
    # Progress tracking
    progress_update_interval: int = 50
    
    # Fragment assembly
    enable_fragment_tracking: bool = True
    fragment_metadata_file: str = ".tellus_fragments.json"


@dataclass
class ExtractionResult:
    """Result of an archive extraction operation."""
    
    def __init__(self):
        self.success: bool = False
        self.extracted_files: List[str] = []
        self.skipped_files: List[str] = []
        self.failed_files: List[str] = []
        self.conflicts_resolved: List[str] = []
        
        # Statistics
        self.files_processed: int = 0
        self.files_extracted: int = 0
        self.bytes_extracted: int = 0
        self.extraction_time: float = 0.0
        
        # Error tracking
        self.errors: List[str] = []
        self.warnings: List[str] = []
        
        # Fragment tracking
        self.fragment_info: Optional[Dict[str, Any]] = None
    
    @property
    def has_errors(self) -> bool:
        """Check if extraction had errors."""
        return len(self.errors) > 0
    
    @property
    def has_warnings(self) -> bool:
        """Check if extraction had warnings."""
        return len(self.warnings) > 0
    
    def add_error(self, message: str) -> None:
        """Add an error message."""
        self.errors.append(message)
        logger.error(f"Archive extraction error: {message}")
    
    def add_warning(self, message: str) -> None:
        """Add a warning message."""
        self.warnings.append(message)
        logger.warning(f"Archive extraction warning: {message}")
    
    def get_extraction_summary(self) -> Dict[str, Any]:
        """Get a summary of the extraction operation."""
        return {
            'success': self.success,
            'files_extracted': self.files_extracted,
            'files_skipped': len(self.skipped_files),
            'files_failed': len(self.failed_files),
            'conflicts_resolved': len(self.conflicts_resolved),
            'bytes_extracted': self.bytes_extracted,
            'extraction_time': self.extraction_time,
            'has_errors': self.has_errors,
            'has_warnings': self.has_warnings
        }


class ArchiveExtractionService:
    """
    Domain service for extracting archive tarballs to various storage locations.
    
    This service handles the complex process of extracting archives with intelligent
    filtering based on sidecar metadata, handling conflicts, and supporting fragment
    assembly for multi-archive simulations.
    """
    
    def __init__(self):
        """Initialize the archive extraction service."""
        self._logger = logger
    
    def extract_archive(
        self,
        archive_path: Path,
        target_location: LocationEntity,
        target_path: Optional[str] = None,
        extraction_filter: Optional[ArchiveExtractionFilter] = None,
        extraction_config: Optional[ArchiveExtractionConfig] = None,
        progress_callback: Optional[Callable[[int, int, str], None]] = None
    ) -> ExtractionResult:
        """
        Extract an archive to a target location with optional filtering.
        
        Args:
            archive_path: Path to the archive file to extract
            target_location: Location entity where files should be extracted
            target_path: Optional specific path within the location
            extraction_filter: Optional filtering configuration
            extraction_config: Optional extraction configuration
            progress_callback: Optional callback for progress updates (current, total, step)
            
        Returns:
            ExtractionResult with extraction details and statistics
        """
        result = ExtractionResult()
        extraction_config = extraction_config or ArchiveExtractionConfig()
        extraction_filter = extraction_filter or ArchiveExtractionFilter()
        
        start_time = time.time()
        
        try:
            self._logger.info(f"Starting archive extraction: {archive_path}")
            
            # Validate inputs
            validation_errors = self._validate_extraction_inputs(
                archive_path, target_location, target_path
            )
            if validation_errors:
                for error in validation_errors:
                    result.add_error(error)
                return result
            
            # Read sidecar metadata if available
            if progress_callback:
                progress_callback(0, 100, "Reading metadata")
            
            sidecar_metadata = self._read_sidecar_metadata(archive_path)
            
            # Enhance filter with sidecar information
            self._enhance_filter_with_metadata(extraction_filter, sidecar_metadata)
            
            # Open and analyze archive
            if progress_callback:
                progress_callback(10, 100, "Analyzing archive")
            
            with tarfile.open(archive_path, 'r:*') as tar:
                members = tar.getmembers()
                result.files_processed = len(members)
                
                # Apply filtering to determine which files to extract
                if progress_callback:
                    progress_callback(20, 100, "Filtering files")
                
                files_to_extract = self._filter_archive_members(
                    members, extraction_filter, sidecar_metadata, result
                )
                
                if not files_to_extract:
                    result.add_warning("No files passed filtering criteria")
                    result.success = True
                    return result
                
                # Prepare target directory
                if progress_callback:
                    progress_callback(30, 100, "Preparing target")
                
                target_dir = self._prepare_target_directory(
                    target_location, target_path, extraction_config, result
                )
                if not target_dir:
                    return result
                
                # Extract files
                if progress_callback:
                    progress_callback(40, 100, "Extracting files")
                
                extraction_success = self._extract_files(
                    tar, files_to_extract, target_dir, extraction_config,
                    progress_callback, result
                )
                
                if not extraction_success and result.has_errors:
                    return result
            
            # Post-extraction verification
            if extraction_config.verify_after_extraction:
                if progress_callback:
                    progress_callback(90, 100, "Verifying extraction")
                
                self._verify_extraction(target_dir, result.extracted_files, result)
            
            # Update fragment tracking
            if extraction_config.enable_fragment_tracking:
                if progress_callback:
                    progress_callback(95, 100, "Updating fragments")
                
                self._update_fragment_tracking(
                    target_dir, archive_path, sidecar_metadata, extraction_config, result
                )
            
            # Finalize result
            result.success = True
            result.extraction_time = time.time() - start_time
            
            if progress_callback:
                progress_callback(100, 100, "Complete")
            
            self._logger.info(
                f"Archive extraction completed: {result.files_extracted} files extracted "
                f"in {result.extraction_time:.1f}s"
            )
            
            return result
            
        except Exception as e:
            result.add_error(f"Archive extraction failed with exception: {str(e)}")
            self._logger.exception("Archive extraction failed")
            return result
    
    def extract_multiple_archives(
        self,
        archive_paths: List[Path],
        target_location: LocationEntity,
        target_path: Optional[str] = None,
        extraction_filter: Optional[ArchiveExtractionFilter] = None,
        extraction_config: Optional[ArchiveExtractionConfig] = None,
        progress_callback: Optional[Callable[[int, int, str], None]] = None
    ) -> Dict[Path, ExtractionResult]:
        """
        Extract multiple archives to the same target location with fragment assembly.
        
        Args:
            archive_paths: List of archive files to extract
            target_location: Location entity where files should be extracted
            target_path: Optional specific path within the location
            extraction_filter: Optional filtering configuration
            extraction_config: Optional extraction configuration
            progress_callback: Optional callback for progress updates
            
        Returns:
            Dictionary mapping archive paths to their extraction results
        """
        results = {}
        extraction_config = extraction_config or ArchiveExtractionConfig()
        
        # Enable fragment tracking for multi-archive extraction
        extraction_config.enable_fragment_tracking = True
        
        total_archives = len(archive_paths)
        
        for i, archive_path in enumerate(archive_paths):
            self._logger.info(f"Extracting archive {i+1}/{total_archives}: {archive_path}")
            
            # Update progress for this archive
            archive_progress_callback = None
            if progress_callback:
                def archive_progress_callback(current, total, step):
                    # Map to overall progress
                    archive_progress = (current / total) * (100 / total_archives)
                    overall_progress = int((i / total_archives) * 100 + archive_progress)
                    progress_callback(overall_progress, 100, f"Archive {i+1}/{total_archives}: {step}")
            
            # Extract this archive
            result = self.extract_archive(
                archive_path, target_location, target_path,
                extraction_filter, extraction_config, archive_progress_callback
            )
            
            results[archive_path] = result
            
            # Stop on critical errors unless configured otherwise
            if result.has_errors and extraction_config.conflict_resolution == ConflictResolution.FAIL:
                break
        
        if progress_callback:
            progress_callback(100, 100, "All extractions complete")
        
        return results
    
    def extract_by_date_range(
        self,
        archive_path: Path,
        target_location: LocationEntity,
        date_range: str,
        target_path: Optional[str] = None,
        extraction_config: Optional[ArchiveExtractionConfig] = None,
        progress_callback: Optional[Callable[[int, int, str], None]] = None
    ) -> ExtractionResult:
        """
        Extract files from an archive based on date range filtering.
        
        Args:
            archive_path: Path to the archive file
            target_location: Location entity where files should be extracted
            date_range: Date range string (e.g., "2024-03-01:2024-03-31")
            target_path: Optional specific path within the location
            extraction_config: Optional extraction configuration
            progress_callback: Optional callback for progress updates
            
        Returns:
            ExtractionResult with extraction details
        """
        # Create filter with date range
        date_range_obj = DateRange.from_string(date_range)
        extraction_filter = ArchiveExtractionFilter(date_range=date_range_obj)
        
        return self.extract_archive(
            archive_path, target_location, target_path,
            extraction_filter, extraction_config, progress_callback
        )
    
    def extract_by_content_type(
        self,
        archive_path: Path,
        target_location: LocationEntity,
        content_types: List[str],
        target_path: Optional[str] = None,
        extraction_config: Optional[ArchiveExtractionConfig] = None,
        progress_callback: Optional[Callable[[int, int, str], None]] = None
    ) -> ExtractionResult:
        """
        Extract files from an archive based on content type filtering.
        
        Args:
            archive_path: Path to the archive file
            target_location: Location entity where files should be extracted
            content_types: List of content type strings to extract
            target_path: Optional specific path within the location
            extraction_config: Optional extraction configuration
            progress_callback: Optional callback for progress updates
            
        Returns:
            ExtractionResult with extraction details
        """
        # Convert strings to FileContentType enums
        include_types = set()
        for content_type in content_types:
            try:
                include_types.add(FileContentType(content_type))
            except ValueError:
                self._logger.warning(f"Unknown content type: {content_type}")
        
        extraction_filter = ArchiveExtractionFilter(include_content_types=include_types)
        
        return self.extract_archive(
            archive_path, target_location, target_path,
            extraction_filter, extraction_config, progress_callback
        )
    
    def _validate_extraction_inputs(
        self,
        archive_path: Path,
        target_location: LocationEntity,
        target_path: Optional[str]
    ) -> List[str]:
        """Validate input parameters for extraction."""
        errors = []
        
        if not archive_path.exists():
            errors.append(f"Archive file does not exist: {archive_path}")
        elif not archive_path.is_file():
            errors.append(f"Archive path is not a file: {archive_path}")
        
        if not isinstance(target_location, LocationEntity):
            errors.append("Target location must be a LocationEntity instance")
        
        # Try to open archive to validate format
        try:
            with tarfile.open(archive_path, 'r:*') as tar:
                pass  # Just test that we can open it
        except Exception as e:
            errors.append(f"Cannot open archive file: {str(e)}")
        
        return errors
    
    def _read_sidecar_metadata(self, archive_path: Path) -> Optional[Dict[str, Any]]:
        """Read sidecar metadata if available."""
        try:
            sidecar_path = SidecarMetadata.find_sidecar_for_archive(archive_path)
            if sidecar_path:
                return SidecarMetadata.read_sidecar_file(sidecar_path)
        except Exception as e:
            self._logger.warning(f"Could not read sidecar metadata: {str(e)}")
        
        return None
    
    def _enhance_filter_with_metadata(
        self,
        extraction_filter: ArchiveExtractionFilter,
        sidecar_metadata: Optional[Dict[str, Any]]
    ) -> None:
        """Enhance extraction filter with information from sidecar metadata."""
        if not sidecar_metadata:
            return
        
        # Add strftime patterns for date filtering
        if extraction_filter.date_range and not extraction_filter.strftime_patterns:
            date_patterns = SidecarMetadata.get_date_patterns(sidecar_metadata)
            if date_patterns:
                extraction_filter.strftime_patterns = date_patterns
    
    def _filter_archive_members(
        self,
        members: List[tarfile.TarInfo],
        extraction_filter: ArchiveExtractionFilter,
        sidecar_metadata: Optional[Dict[str, Any]],
        result: ExtractionResult
    ) -> List[tarfile.TarInfo]:
        """Filter archive members based on extraction criteria."""
        files_to_extract = []
        
        # Build file inventory from sidecar metadata if available
        file_lookup = {}
        if sidecar_metadata:
            try:
                archive_metadata = SidecarMetadata.reconstruct_archive_metadata(sidecar_metadata)
                if archive_metadata.file_inventory:
                    for sim_file in archive_metadata.file_inventory.list_files():
                        file_lookup[sim_file.relative_path] = sim_file
            except Exception as e:
                self._logger.warning(f"Could not reconstruct file inventory: {str(e)}")
        
        for member in members:
            # Skip directories unless specifically requested
            if member.isdir():
                continue
            
            # Check if we have metadata for this file
            sim_file = file_lookup.get(member.name)
            if sim_file:
                # Use metadata-based filtering
                if extraction_filter.apply_filter(sim_file, member.name):
                    files_to_extract.append(member)
                else:
                    result.skipped_files.append(member.name)
            else:
                # Fallback to basic pattern-based filtering
                if self._basic_member_filter(member, extraction_filter):
                    files_to_extract.append(member)
                else:
                    result.skipped_files.append(member.name)
        
        return files_to_extract
    
    def _basic_member_filter(
        self,
        member: tarfile.TarInfo,
        extraction_filter: ArchiveExtractionFilter
    ) -> bool:
        """Basic filtering for archive members without metadata."""
        # Pattern-based filtering
        if extraction_filter.include_patterns:
            if not any(self._matches_pattern(member.name, pattern) 
                      for pattern in extraction_filter.include_patterns):
                return False
        
        if extraction_filter.exclude_patterns:
            if any(self._matches_pattern(member.name, pattern) 
                  for pattern in extraction_filter.exclude_patterns):
                return False
        
        # Directory filtering
        if extraction_filter.include_directories:
            if not any(member.name.startswith(dir_path) 
                      for dir_path in extraction_filter.include_directories):
                return False
        
        if extraction_filter.exclude_directories:
            if any(member.name.startswith(dir_path) 
                  for dir_path in extraction_filter.exclude_directories):
                return False
        
        # Size filtering
        if extraction_filter.max_file_size and member.size > extraction_filter.max_file_size:
            return False
        if extraction_filter.min_file_size and member.size < extraction_filter.min_file_size:
            return False
        
        return True
    
    def _matches_pattern(self, filename: str, pattern: str) -> bool:
        """Check if filename matches a glob pattern."""
        import fnmatch
        return fnmatch.fnmatch(filename, pattern)
    
    def _prepare_target_directory(
        self,
        target_location: LocationEntity,
        target_path: Optional[str],
        extraction_config: ArchiveExtractionConfig,
        result: ExtractionResult
    ) -> Optional[Path]:
        """Prepare the target directory for extraction."""
        try:
            # For now, support local filesystem extraction
            # TODO: Integrate with Location abstraction layer for remote storage
            if target_location.get_protocol() != 'file':
                result.add_error(f"Remote extraction not yet implemented for protocol: {target_location.get_protocol()}")
                return None
            
            base_path = Path(target_location.config.get('path', '.'))
            if target_path:
                target_dir = base_path / target_path
            else:
                target_dir = base_path
            
            if extraction_config.create_directories:
                target_dir.mkdir(parents=True, exist_ok=True)
            
            if not target_dir.exists():
                result.add_error(f"Target directory does not exist: {target_dir}")
                return None
            
            return target_dir
            
        except Exception as e:
            result.add_error(f"Failed to prepare target directory: {str(e)}")
            return None
    
    def _extract_files(
        self,
        tar: tarfile.TarFile,
        files_to_extract: List[tarfile.TarInfo],
        target_dir: Path,
        extraction_config: ArchiveExtractionConfig,
        progress_callback: Optional[Callable[[int, int, str], None]],
        result: ExtractionResult
    ) -> bool:
        """Extract the filtered files to the target directory."""
        total_files = len(files_to_extract)
        extracted_count = 0
        
        for i, member in enumerate(files_to_extract):
            try:
                target_file_path = target_dir / member.name
                
                # Check for conflicts
                if target_file_path.exists():
                    if not self._handle_file_conflict(
                        member, target_file_path, extraction_config, result
                    ):
                        result.skipped_files.append(member.name)
                        continue
                
                # Create parent directories
                target_file_path.parent.mkdir(parents=True, exist_ok=True)
                
                # Extract the file
                if extraction_config.use_atomic_extraction:
                    self._extract_file_atomically(
                        tar, member, target_file_path, extraction_config, result
                    )
                else:
                    tar.extract(member, target_dir)
                    
                    # Apply permission and timestamp settings
                    self._apply_file_attributes(
                        target_file_path, member, extraction_config
                    )
                
                result.extracted_files.append(member.name)
                result.bytes_extracted += member.size
                extracted_count += 1
                
                # Update progress
                if progress_callback and (i + 1) % extraction_config.progress_update_interval == 0:
                    progress_percent = int(40 + ((i + 1) / total_files) * 40)
                    progress_callback(
                        progress_percent, 100,
                        f"Extracted {extracted_count}/{total_files} files"
                    )
                
            except Exception as e:
                error_msg = f"Failed to extract {member.name}: {str(e)}"
                result.add_error(error_msg)
                result.failed_files.append(member.name)
        
        result.files_extracted = extracted_count
        return True
    
    def _handle_file_conflict(
        self,
        member: tarfile.TarInfo,
        target_path: Path,
        extraction_config: ArchiveExtractionConfig,
        result: ExtractionResult
    ) -> bool:
        """
        Handle file conflicts based on conflict resolution strategy.
        
        Returns:
            True if extraction should proceed, False if file should be skipped
        """
        strategy = extraction_config.conflict_resolution
        
        if strategy == ConflictResolution.SKIP:
            return False
        elif strategy == ConflictResolution.OVERWRITE:
            return True
        elif strategy == ConflictResolution.FAIL:
            result.add_error(f"File conflict: {target_path} already exists")
            return False
        elif strategy == ConflictResolution.NEWEST:
            # Compare timestamps
            try:
                existing_mtime = target_path.stat().st_mtime
                archive_mtime = member.mtime
                should_overwrite = archive_mtime > existing_mtime
                if should_overwrite:
                    result.conflicts_resolved.append(str(target_path))
                return should_overwrite
            except Exception:
                return True  # Default to overwrite if we can't compare
        elif strategy == ConflictResolution.LARGEST:
            # Compare file sizes
            try:
                existing_size = target_path.stat().st_size
                archive_size = member.size
                should_overwrite = archive_size > existing_size
                if should_overwrite:
                    result.conflicts_resolved.append(str(target_path))
                return should_overwrite
            except Exception:
                return True  # Default to overwrite if we can't compare
        
        return True  # Default behavior
    
    def _extract_file_atomically(
        self,
        tar: tarfile.TarFile,
        member: tarfile.TarInfo,
        target_path: Path,
        extraction_config: ArchiveExtractionConfig,
        result: ExtractionResult
    ) -> None:
        """Extract a file atomically using a temporary file."""
        temp_dir = extraction_config.temp_dir or target_path.parent
        
        with tempfile.NamedTemporaryFile(
            dir=temp_dir, 
            prefix=f".{target_path.name}_",
            delete=False
        ) as temp_file:
            temp_path = Path(temp_file.name)
            
            try:
                # Extract to temporary file
                extracted_file = tar.extractfile(member)
                if extracted_file:
                    # Copy in chunks for memory efficiency
                    while True:
                        chunk = extracted_file.read(extraction_config.chunk_size)
                        if not chunk:
                            break
                        temp_file.write(chunk)
                    extracted_file.close()
                
                # Apply file attributes
                self._apply_file_attributes(temp_path, member, extraction_config)
                
                # Atomic move to final location
                temp_path.replace(target_path)
                
            except Exception as e:
                # Clean up temp file on error
                if temp_path.exists():
                    temp_path.unlink()
                raise e
    
    def _apply_file_attributes(
        self,
        file_path: Path,
        member: tarfile.TarInfo,
        extraction_config: ArchiveExtractionConfig
    ) -> None:
        """Apply file permissions and timestamps."""
        try:
            if extraction_config.preserve_permissions:
                os.chmod(file_path, member.mode)
            
            if extraction_config.preserve_timestamps:
                os.utime(file_path, (member.mtime, member.mtime))
        except Exception as e:
            # Non-critical error, just log it
            self._logger.warning(f"Could not apply file attributes to {file_path}: {str(e)}")
    
    def _verify_extraction(
        self,
        target_dir: Path,
        extracted_files: List[str],
        result: ExtractionResult
    ) -> None:
        """Verify that extracted files exist and have correct properties."""
        verification_errors = []
        
        for file_path in extracted_files:
            full_path = target_dir / file_path
            if not full_path.exists():
                verification_errors.append(f"Extracted file not found: {file_path}")
            elif not full_path.is_file():
                verification_errors.append(f"Extracted path is not a file: {file_path}")
        
        if verification_errors:
            for error in verification_errors:
                result.add_error(error)
    
    def _update_fragment_tracking(
        self,
        target_dir: Path,
        archive_path: Path,
        sidecar_metadata: Optional[Dict[str, Any]],
        extraction_config: ArchiveExtractionConfig,
        result: ExtractionResult
    ) -> None:
        """Update fragment tracking metadata for multi-archive assembly."""
        try:
            fragment_file = target_dir / extraction_config.fragment_metadata_file
            
            # Load existing fragment data
            fragment_data = {}
            if fragment_file.exists():
                import json
                with open(fragment_file, 'r') as f:
                    fragment_data = json.load(f)
            
            # Add this archive's information
            archive_info = {
                'archive_path': str(archive_path),
                'extraction_time': time.time(),
                'files_extracted': len(result.extracted_files),
                'archive_metadata': sidecar_metadata.get('archive', {}) if sidecar_metadata else {}
            }
            
            fragment_data[str(archive_path)] = archive_info
            result.fragment_info = fragment_data
            
            # Write updated fragment data
            with open(fragment_file, 'w') as f:
                import json
                json.dump(fragment_data, f, indent=2, default=str)
                
        except Exception as e:
            result.add_warning(f"Could not update fragment tracking: {str(e)}")