"""
File scanning service for simulation directories.

This service recursively scans simulation directories, classifies files,
and builds file inventories for archive operations.
"""

import hashlib
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set

from ..entities.simulation_file import Checksum, FileInventory, SimulationFile
from .file_classifier import FileClassifier

logger = logging.getLogger(__name__)


class FileScanResult:
    """Result object for file scanning operations."""
    
    def __init__(self):
        self.inventory = FileInventory()
        self.errors: List[str] = []
        self.warnings: List[str] = []
        self.scan_time: float = 0.0
        self.files_processed: int = 0
        self.bytes_processed: int = 0
        self.skipped_files: int = 0
    
    @property
    def success(self) -> bool:
        """Check if scan completed successfully."""
        return len(self.errors) == 0
    
    @property
    def has_warnings(self) -> bool:
        """Check if scan had warnings."""
        return len(self.warnings) > 0
    
    def add_error(self, message: str) -> None:
        """Add an error message."""
        self.errors.append(message)
        logger.error(f"File scan error: {message}")
    
    def add_warning(self, message: str) -> None:
        """Add a warning message."""
        self.warnings.append(message)
        logger.warning(f"File scan warning: {message}")


class FileScanner:
    """
    Domain service for scanning simulation directories and building file inventories.
    
    This service handles the complex task of recursively scanning directories,
    classifying files using domain knowledge, and building structured inventories
    suitable for archive operations.
    """
    
    def __init__(self, classifier: Optional[FileClassifier] = None):
        """
        Initialize the file scanner.
        
        Args:
            classifier: Optional custom file classifier (creates default if None)
        """
        self._classifier = classifier or FileClassifier()
        self._logger = logger
    
    def scan_directory(
        self,
        base_path: Path,
        simulation_context: Optional[Dict] = None,
        include_patterns: Optional[List[str]] = None,
        exclude_patterns: Optional[List[str]] = None,
        compute_checksums: bool = False,
        max_workers: int = 4,
        progress_callback: Optional[Callable[[int, int], None]] = None
    ) -> FileScanResult:
        """
        Scan a simulation directory and build a file inventory.
        
        Args:
            base_path: Base directory to scan
            simulation_context: Context information about the simulation
            include_patterns: Optional glob patterns for files to include
            exclude_patterns: Optional glob patterns for files to exclude
            compute_checksums: Whether to compute file checksums
            max_workers: Number of worker threads for parallel processing
            progress_callback: Optional callback for progress updates (processed, total)
            
        Returns:
            FileScanResult with inventory and scan metadata
        """
        result = FileScanResult()
        start_time = datetime.now()
        
        try:
            self._logger.info(f"Starting directory scan: {base_path}")
            
            if not base_path.exists():
                result.add_error(f"Directory does not exist: {base_path}")
                return result
            
            if not base_path.is_dir():
                result.add_error(f"Path is not a directory: {base_path}")
                return result
            
            # Find all files to process
            file_paths = self._find_files(
                base_path, include_patterns, exclude_patterns, result
            )
            
            if not file_paths:
                result.add_warning("No files found to scan")
                return result
            
            self._logger.info(f"Found {len(file_paths)} files to process")
            
            # Process files with parallel workers
            if max_workers > 1:
                self._process_files_parallel(
                    base_path, file_paths, simulation_context, compute_checksums,
                    max_workers, progress_callback, result
                )
            else:
                self._process_files_sequential(
                    base_path, file_paths, simulation_context, compute_checksums,
                    progress_callback, result
                )
            
            # Finalize result
            end_time = datetime.now()
            result.scan_time = (end_time - start_time).total_seconds()
            result.files_processed = len(result.inventory.files)
            result.bytes_processed = result.inventory.total_size
            
            self._logger.info(
                f"Directory scan completed: {result.files_processed} files, "
                f"{result.bytes_processed / (1024**2):.1f} MB in {result.scan_time:.1f}s"
            )
            
            return result
            
        except Exception as e:
            result.add_error(f"Scan failed with exception: {str(e)}")
            self._logger.exception("Directory scan failed")
            return result
    
    def scan_file_list(
        self,
        base_path: Path,
        file_paths: List[str],
        simulation_context: Optional[Dict] = None,
        compute_checksums: bool = False,
        progress_callback: Optional[Callable[[int, int], None]] = None
    ) -> FileScanResult:
        """
        Scan a specific list of files rather than a full directory.
        
        Args:
            base_path: Base directory for relative path resolution
            file_paths: List of file paths (relative to base_path)
            simulation_context: Context information about the simulation
            compute_checksums: Whether to compute file checksums
            progress_callback: Optional callback for progress updates
            
        Returns:
            FileScanResult with inventory and scan metadata
        """
        result = FileScanResult()
        start_time = datetime.now()
        
        try:
            self._logger.info(f"Starting file list scan: {len(file_paths)} files")
            
            # Filter to existing files
            existing_files = []
            for file_path in file_paths:
                full_path = base_path / file_path
                if full_path.exists() and full_path.is_file():
                    existing_files.append(file_path)
                else:
                    result.add_warning(f"File not found or not a file: {file_path}")
                    result.skipped_files += 1
            
            if not existing_files:
                result.add_warning("No valid files found in provided list")
                return result
            
            # Process files sequentially (file lists are typically smaller)
            self._process_files_sequential(
                base_path, existing_files, simulation_context, compute_checksums,
                progress_callback, result
            )
            
            # Finalize result
            end_time = datetime.now()
            result.scan_time = (end_time - start_time).total_seconds()
            result.files_processed = len(result.inventory.files)
            result.bytes_processed = result.inventory.total_size
            
            self._logger.info(f"File list scan completed: {result.files_processed} files")
            
            return result
            
        except Exception as e:
            result.add_error(f"File list scan failed: {str(e)}")
            self._logger.exception("File list scan failed")
            return result
    
    def _find_files(
        self,
        base_path: Path,
        include_patterns: Optional[List[str]],
        exclude_patterns: Optional[List[str]],
        result: FileScanResult
    ) -> List[str]:
        """Find all files to process based on patterns."""
        from fnmatch import fnmatch
        
        files = []
        
        try:
            # Recursively find all files
            for path in base_path.rglob('*'):
                if not path.is_file():
                    continue
                
                # Get relative path
                try:
                    rel_path = str(path.relative_to(base_path))
                except ValueError:
                    # Skip files outside base path
                    continue
                
                # Apply include patterns
                if include_patterns:
                    included = any(fnmatch(rel_path, pattern) for pattern in include_patterns)
                    if not included:
                        result.skipped_files += 1
                        continue
                
                # Apply exclude patterns
                if exclude_patterns:
                    excluded = any(fnmatch(rel_path, pattern) for pattern in exclude_patterns)
                    if excluded:
                        result.skipped_files += 1
                        continue
                
                files.append(rel_path)
        
        except Exception as e:
            result.add_error(f"Error finding files: {str(e)}")
        
        return files
    
    def _process_files_sequential(
        self,
        base_path: Path,
        file_paths: List[str],
        simulation_context: Optional[Dict],
        compute_checksums: bool,
        progress_callback: Optional[Callable[[int, int], None]],
        result: FileScanResult
    ) -> None:
        """Process files sequentially."""
        total_files = len(file_paths)
        
        for i, rel_path in enumerate(file_paths):
            try:
                simulation_file = self._process_single_file(
                    base_path, rel_path, simulation_context, compute_checksums
                )
                
                if simulation_file:
                    result.inventory.add_file(simulation_file)
                else:
                    result.skipped_files += 1
                
                # Report progress
                if progress_callback and (i + 1) % 10 == 0:
                    progress_callback(i + 1, total_files)
                    
            except Exception as e:
                result.add_error(f"Error processing file {rel_path}: {str(e)}")
        
        # Final progress update
        if progress_callback:
            progress_callback(total_files, total_files)
    
    def _process_files_parallel(
        self,
        base_path: Path,
        file_paths: List[str],
        simulation_context: Optional[Dict],
        compute_checksums: bool,
        max_workers: int,
        progress_callback: Optional[Callable[[int, int], None]],
        result: FileScanResult
    ) -> None:
        """Process files in parallel using thread pool."""
        total_files = len(file_paths)
        completed = 0
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            future_to_path = {
                executor.submit(
                    self._process_single_file,
                    base_path, rel_path, simulation_context, compute_checksums
                ): rel_path
                for rel_path in file_paths
            }
            
            # Process completed tasks
            for future in as_completed(future_to_path):
                rel_path = future_to_path[future]
                completed += 1
                
                try:
                    simulation_file = future.result()
                    if simulation_file:
                        result.inventory.add_file(simulation_file)
                    else:
                        result.skipped_files += 1
                        
                except Exception as e:
                    result.add_error(f"Error processing file {rel_path}: {str(e)}")
                
                # Report progress
                if progress_callback and completed % 10 == 0:
                    progress_callback(completed, total_files)
            
            # Final progress update
            if progress_callback:
                progress_callback(total_files, total_files)
    
    def _process_single_file(
        self,
        base_path: Path,
        rel_path: str,
        simulation_context: Optional[Dict],
        compute_checksums: bool
    ) -> Optional[SimulationFile]:
        """Process a single file and return SimulationFile entity."""
        try:
            full_path = base_path / rel_path
            
            if not full_path.exists() or not full_path.is_file():
                return None
            
            # Get basic file information
            stat_info = full_path.stat()
            size = stat_info.st_size
            created_time = stat_info.st_ctime
            modified_time = stat_info.st_mtime
            
            # Compute checksum if requested
            checksum = None
            if compute_checksums:
                checksum = self._compute_file_checksum(full_path)
            
            # Create SimulationFile with classification
            simulation_file = self._classifier.create_simulation_file(
                file_path=rel_path,
                simulation_context=simulation_context,
                size=size,
                checksum=checksum,
                created_time=created_time,
                modified_time=modified_time
            )
            
            return simulation_file
            
        except Exception as e:
            self._logger.error(f"Error processing file {rel_path}: {str(e)}")
            return None
    
    def _compute_file_checksum(self, file_path: Path, algorithm: str = 'md5') -> Optional[Checksum]:
        """Compute checksum for a file."""
        try:
            hash_obj = hashlib.new(algorithm)
            
            with open(file_path, 'rb') as f:
                # Read file in chunks to handle large files
                for chunk in iter(lambda: f.read(8192), b''):
                    hash_obj.update(chunk)
            
            return Checksum(value=hash_obj.hexdigest(), algorithm=algorithm)
            
        except Exception as e:
            self._logger.error(f"Error computing checksum for {file_path}: {str(e)}")
            return None
    
    def create_filtered_inventory(
        self,
        inventory: FileInventory,
        content_types: Optional[Set[str]] = None,
        importance_levels: Optional[Set[str]] = None,
        tags: Optional[Set[str]] = None,
        patterns: Optional[List[str]] = None
    ) -> FileInventory:
        """
        Create a filtered copy of an inventory based on criteria.
        
        Args:
            inventory: Source inventory to filter
            content_types: Filter by content types (e.g., {'output', 'input'})
            importance_levels: Filter by importance levels (e.g., {'critical', 'important'})
            tags: Filter files that match any of these tags
            patterns: Filter by glob patterns
            
        Returns:
            New FileInventory with filtered files
        """
        from fnmatch import fnmatch

        from ..entities.simulation_file import FileContentType, FileImportance
        
        filtered = FileInventory()
        
        for file in inventory.list_files():
            # Filter by content type
            if content_types and file.content_type.value not in content_types:
                continue
            
            # Filter by importance
            if importance_levels and file.importance.value not in importance_levels:
                continue
            
            # Filter by tags
            if tags and not file.matches_any_tag(tags):
                continue
            
            # Filter by patterns
            if patterns:
                matches_pattern = any(
                    fnmatch(file.relative_path, pattern) for pattern in patterns
                )
                if not matches_pattern:
                    continue
            
            # File passed all filters
            filtered.add_file(file)
        
        return filtered
    
    def analyze_inventory(self, inventory: FileInventory) -> Dict[str, Any]:
        """
        Analyze an inventory and provide summary statistics.
        
        Returns:
            Dictionary with analysis results
        """
        files = inventory.list_files()
        
        if not files:
            return {
                'total_files': 0,
                'total_size': 0,
                'content_summary': {},
                'importance_summary': {},
                'size_distribution': {},
                'archivable_files': 0,
                'extensions': {},
                'directories': {}
            }
        
        # Basic statistics
        total_files = len(files)
        total_size = sum(f.size or 0 for f in files)
        
        # Content type summary
        content_summary = {}
        importance_summary = {}
        size_by_content = {}
        extensions = {}
        directories = set()
        archivable_count = 0
        
        for file in files:
            # Content types
            ct = file.content_type.value
            content_summary[ct] = content_summary.get(ct, 0) + 1
            size_by_content[ct] = size_by_content.get(ct, 0) + (file.size or 0)
            
            # Importance levels
            imp = file.importance.value
            importance_summary[imp] = importance_summary.get(imp, 0) + 1
            
            # Extensions
            ext = file.get_file_extension()
            if ext:
                extensions[ext] = extensions.get(ext, 0) + 1
            
            # Directories
            directories.add(file.get_directory())
            
            # Archivable files
            if file.is_archivable():
                archivable_count += 1
        
        # Size distribution
        size_ranges = {
            'small (<1MB)': 0,
            'medium (1-100MB)': 0,
            'large (100MB-1GB)': 0,
            'huge (>1GB)': 0
        }
        
        for file in files:
            size = file.size or 0
            if size < 1024**2:
                size_ranges['small (<1MB)'] += 1
            elif size < 100 * 1024**2:
                size_ranges['medium (1-100MB)'] += 1
            elif size < 1024**3:
                size_ranges['large (100MB-1GB)'] += 1
            else:
                size_ranges['huge (>1GB)'] += 1
        
        return {
            'total_files': total_files,
            'total_size': total_size,
            'total_size_mb': total_size / (1024**2),
            'content_summary': content_summary,
            'importance_summary': importance_summary,
            'size_by_content_mb': {k: v / (1024**2) for k, v in size_by_content.items()},
            'size_distribution': size_ranges,
            'archivable_files': archivable_count,
            'archivable_percentage': (archivable_count / total_files * 100) if total_files > 0 else 0,
            'extensions': dict(sorted(extensions.items(), key=lambda x: x[1], reverse=True)),
            'directory_count': len(directories),
            'directories': sorted(directories)
        }