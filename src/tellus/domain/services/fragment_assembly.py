"""
Fragment assembly service for reconstructing complete simulations from multiple archive fragments.

This service orchestrates the assembly of multiple archive fragments into a complete simulation,
handling complex scenarios like temporal overlaps, content type merging, and sophisticated
conflict resolution. It coordinates with the ArchiveExtractionService for individual extractions
and provides comprehensive planning and validation capabilities.
"""

import json
import logging
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
from .archive_extraction import (ArchiveExtractionConfig,
                                 ArchiveExtractionFilter,
                                 ArchiveExtractionService, ConflictResolution,
                                 DateRange, ExtractionResult)
from .sidecar_metadata import SidecarMetadata

logger = logging.getLogger(__name__)


class FragmentConflictStrategy(Enum):
    """Strategies for resolving conflicts during fragment assembly."""
    NEWEST_WINS = "newest_wins"           # Keep file with latest modification time
    LARGEST_WINS = "largest_wins"         # Keep file with largest size
    FIRST_WINS = "first_wins"             # Keep first extracted file, skip conflicts
    MERGE_DIRECTORIES = "merge_directories"  # Allow directory merging but not file overwriting
    INTERACTIVE = "interactive"           # Callback for user decision on each conflict
    SKIP_CONFLICTS = "skip_conflicts"     # Skip any files that would conflict
    FAIL_ON_CONFLICT = "fail_on_conflict" # Fail assembly on any conflict


class AssemblyMode(Enum):
    """Modes for fragment assembly."""
    COMPLETE = "complete"              # Assemble complete simulation from all fragments
    TEMPORAL = "temporal"              # Assemble by date ranges
    CONTENT_TYPE = "content_type"      # Assemble by content types
    DIRECTORY = "directory"            # Assemble specific directories
    SELECTIVE = "selective"            # Custom selective assembly


class AssemblyComplexity(Enum):
    """Assembly complexity levels for estimation."""
    SIMPLE = "simple"          # Few fragments, minimal overlap
    MODERATE = "moderate"      # Some overlap, straightforward resolution
    COMPLEX = "complex"        # Significant overlap, complex conflicts
    VERY_COMPLEX = "very_complex"  # Many fragments, complex overlaps


@dataclass
class FragmentOverlap:
    """Information about overlapping content between fragments."""
    fragment1_id: str
    fragment2_id: str
    overlapping_files: List[str]
    overlap_type: str  # 'file', 'directory', 'temporal'
    conflict_potential: str  # 'low', 'medium', 'high'
    resolution_suggestion: Optional[str] = None


@dataclass
class AssemblyPlan:
    """Plan for assembling fragments into a complete simulation."""
    
    def __init__(self):
        self.assembly_id: str = f"assembly_{int(time.time())}"
        self.fragments: List[ArchiveMetadata] = []
        self.target_location: Optional[LocationEntity] = None
        self.target_path: Optional[str] = None
        
        # Assembly strategy
        self.assembly_mode: AssemblyMode = AssemblyMode.COMPLETE
        self.conflict_strategy: FragmentConflictStrategy = FragmentConflictStrategy.NEWEST_WINS
        
        # Planning results
        self.extraction_order: List[str] = []  # Archive IDs in extraction order
        self.overlaps: List[FragmentOverlap] = []
        self.conflicts_predicted: int = 0
        self.estimated_complexity: AssemblyComplexity = AssemblyComplexity.SIMPLE
        
        # Assembly filtering
        self.assembly_filter: Optional[ArchiveExtractionFilter] = None
        
        # Validation results
        self.is_valid: bool = False
        self.validation_errors: List[str] = []
        self.validation_warnings: List[str] = []
        
        # Estimates
        self.estimated_files: int = 0
        self.estimated_size: int = 0
        self.estimated_duration: float = 0.0
        
        # Dependencies
        self.fragment_dependencies: Dict[str, List[str]] = {}
        
    def add_fragment(self, fragment: ArchiveMetadata) -> None:
        """Add a fragment to the assembly plan."""
        self.fragments.append(fragment)
        self._update_estimates()
    
    def get_fragment_by_id(self, fragment_id: str) -> Optional[ArchiveMetadata]:
        """Get a fragment by its archive ID."""
        for fragment in self.fragments:
            if str(fragment.archive_id) == fragment_id:
                return fragment
        return None
    
    def get_fragment_count(self) -> int:
        """Get the number of fragments in the plan."""
        return len(self.fragments)
    
    def add_overlap(self, overlap: FragmentOverlap) -> None:
        """Add an overlap analysis result."""
        self.overlaps.append(overlap)
        if overlap.conflict_potential in ['medium', 'high']:
            self.conflicts_predicted += len(overlap.overlapping_files)
    
    def get_high_conflict_overlaps(self) -> List[FragmentOverlap]:
        """Get overlaps with high conflict potential."""
        return [o for o in self.overlaps if o.conflict_potential == 'high']
    
    def _update_estimates(self) -> None:
        """Update size and file count estimates."""
        total_files = 0
        total_size = 0
        
        for fragment in self.fragments:
            if fragment.file_inventory:
                total_files += fragment.file_inventory.file_count
                total_size += fragment.file_inventory.total_size
        
        # Account for overlaps (rough estimate)
        overlap_reduction = min(0.3, len(self.overlaps) * 0.05)
        self.estimated_files = int(total_files * (1 - overlap_reduction))
        self.estimated_size = int(total_size * (1 - overlap_reduction))
        
        # Estimate duration based on complexity
        base_duration = total_files * 0.01  # 0.01 seconds per file
        complexity_multiplier = {
            AssemblyComplexity.SIMPLE: 1.0,
            AssemblyComplexity.MODERATE: 1.5,
            AssemblyComplexity.COMPLEX: 2.5,
            AssemblyComplexity.VERY_COMPLEX: 4.0
        }
        self.estimated_duration = base_duration * complexity_multiplier.get(
            self.estimated_complexity, 1.0
        )


@dataclass
class AssemblyResult:
    """Result of a fragment assembly operation."""
    
    def __init__(self, assembly_id: str):
        self.assembly_id: str = assembly_id
        self.success: bool = False
        
        # Assembly statistics
        self.fragments_processed: int = 0
        self.fragments_successful: int = 0
        self.fragments_failed: int = 0
        
        # File statistics
        self.total_files_extracted: int = 0
        self.total_bytes_extracted: int = 0
        self.conflicts_resolved: int = 0
        self.files_skipped: int = 0
        
        # Individual extraction results
        self.extraction_results: Dict[str, ExtractionResult] = {}
        
        # Assembly metadata
        self.assembly_time: float = 0.0
        self.target_location: Optional[LocationEntity] = None
        self.target_path: Optional[str] = None
        
        # Error and warning tracking
        self.errors: List[str] = []
        self.warnings: List[str] = []
        
        # Fragment tracking
        self.fragment_assembly_log: List[Dict[str, Any]] = []
        
        # Assembly summary
        self.assembly_summary: Dict[str, Any] = {}
    
    def add_extraction_result(self, fragment_id: str, result: ExtractionResult) -> None:
        """Add an extraction result for a fragment."""
        self.extraction_results[fragment_id] = result
        self.fragments_processed += 1
        
        if result.success:
            self.fragments_successful += 1
            self.total_files_extracted += result.files_extracted
            self.total_bytes_extracted += result.bytes_extracted
            self.conflicts_resolved += len(result.conflicts_resolved)
        else:
            self.fragments_failed += 1
            self.errors.extend(result.errors)
            self.warnings.extend(result.warnings)
    
    def add_error(self, message: str) -> None:
        """Add an error message."""
        self.errors.append(message)
        logger.error(f"Fragment assembly error: {message}")
    
    def add_warning(self, message: str) -> None:
        """Add a warning message."""
        self.warnings.append(message)
        logger.warning(f"Fragment assembly warning: {message}")
    
    def log_fragment_action(self, fragment_id: str, action: str, details: Dict[str, Any]) -> None:
        """Log an action taken on a fragment."""
        log_entry = {
            'timestamp': time.time(),
            'fragment_id': fragment_id,
            'action': action,
            'details': details
        }
        self.fragment_assembly_log.append(log_entry)
    
    def generate_assembly_summary(self) -> Dict[str, Any]:
        """Generate a comprehensive assembly summary."""
        self.assembly_summary = {
            'assembly_id': self.assembly_id,
            'success': self.success,
            'fragments': {
                'total': self.fragments_processed,
                'successful': self.fragments_successful,
                'failed': self.fragments_failed
            },
            'files': {
                'extracted': self.total_files_extracted,
                'skipped': self.files_skipped,
                'conflicts_resolved': self.conflicts_resolved
            },
            'size': {
                'total_bytes': self.total_bytes_extracted,
                'total_mb': round(self.total_bytes_extracted / (1024 * 1024), 2)
            },
            'timing': {
                'assembly_time': self.assembly_time,
                'average_time_per_fragment': (
                    self.assembly_time / max(1, self.fragments_processed)
                )
            },
            'errors': len(self.errors),
            'warnings': len(self.warnings)
        }
        return self.assembly_summary


@dataclass
class ConflictResolutionCallback:
    """Callback configuration for interactive conflict resolution."""
    callback_function: Callable[[str, List[str], Dict[str, Any]], str]
    timeout_seconds: Optional[int] = 30
    default_action: str = "skip"  # Default if callback times out


class FragmentAssemblyService:
    """
    Domain service for assembling multiple archive fragments into complete simulations.
    
    This service orchestrates the complex process of multi-archive reconstruction,
    handling temporal overlaps, content type merging, dependency resolution, and
    sophisticated conflict resolution strategies.
    """
    
    def __init__(self, extraction_service: Optional[ArchiveExtractionService] = None):
        """Initialize the fragment assembly service."""
        self._logger = logger
        self._extraction_service = extraction_service or ArchiveExtractionService()
    
    def create_assembly_plan(
        self,
        fragments: List[ArchiveMetadata],
        target_location: LocationEntity,
        target_path: Optional[str] = None,
        assembly_mode: AssemblyMode = AssemblyMode.COMPLETE,
        conflict_strategy: FragmentConflictStrategy = FragmentConflictStrategy.NEWEST_WINS,
        assembly_filter: Optional[ArchiveExtractionFilter] = None
    ) -> AssemblyPlan:
        """
        Create a comprehensive assembly plan for the given fragments.
        
        Args:
            fragments: List of archive metadata for fragments to assemble
            target_location: Location where the assembled simulation will be stored
            target_path: Optional specific path within the target location
            assembly_mode: Strategy for assembly (complete, temporal, etc.)
            conflict_strategy: How to handle file conflicts
            assembly_filter: Optional filter for selective assembly
            
        Returns:
            AssemblyPlan with analysis and extraction strategy
        """
        plan = AssemblyPlan()
        plan.target_location = target_location
        plan.target_path = target_path
        plan.assembly_mode = assembly_mode
        plan.conflict_strategy = conflict_strategy
        plan.assembly_filter = assembly_filter
        
        # Add all fragments to the plan
        for fragment in fragments:
            plan.add_fragment(fragment)
        
        # Analyze fragment compatibility
        self._analyze_fragment_compatibility(plan)
        
        # Detect overlaps and conflicts
        self._detect_overlaps(plan)
        
        # Determine optimal extraction order
        self._determine_extraction_order(plan)
        
        # Resolve dependencies
        self._resolve_dependencies(plan)
        
        # Estimate complexity
        self._estimate_assembly_complexity(plan)
        
        # Validate the plan
        self._validate_assembly_plan(plan)
        
        self._logger.info(
            f"Created assembly plan for {len(fragments)} fragments: "
            f"{plan.estimated_files} files, {plan.estimated_complexity.value} complexity"
        )
        
        return plan
    
    def assemble_fragments(
        self,
        assembly_plan: AssemblyPlan,
        progress_callback: Optional[Callable[[int, int, str], None]] = None,
        conflict_callback: Optional[ConflictResolutionCallback] = None
    ) -> AssemblyResult:
        """
        Execute a fragment assembly plan to reconstruct a complete simulation.
        
        Args:
            assembly_plan: Pre-validated assembly plan
            progress_callback: Optional callback for progress updates
            conflict_callback: Optional callback for interactive conflict resolution
            
        Returns:
            AssemblyResult with detailed assembly information
        """
        result = AssemblyResult(assembly_plan.assembly_id)
        result.target_location = assembly_plan.target_location
        result.target_path = assembly_plan.target_path
        
        start_time = time.time()
        
        try:
            if not assembly_plan.is_valid:
                result.add_error("Assembly plan is not valid")
                return result
            
            self._logger.info(f"Starting fragment assembly: {assembly_plan.assembly_id}")
            
            if progress_callback:
                progress_callback(0, 100, "Preparing assembly")
            
            # Prepare target directory
            target_dir = self._prepare_assembly_target(assembly_plan, result)
            if not target_dir:
                return result
            
            # Setup assembly configuration
            assembly_config = self._create_assembly_config(
                assembly_plan, conflict_callback
            )
            
            # Execute assembly based on mode
            if assembly_plan.assembly_mode == AssemblyMode.TEMPORAL:
                assembly_success = self._assemble_temporal_fragments(
                    assembly_plan, assembly_config, progress_callback, result
                )
            elif assembly_plan.assembly_mode == AssemblyMode.CONTENT_TYPE:
                assembly_success = self._assemble_content_type_fragments(
                    assembly_plan, assembly_config, progress_callback, result
                )
            elif assembly_plan.assembly_mode == AssemblyMode.DIRECTORY:
                assembly_success = self._assemble_directory_fragments(
                    assembly_plan, assembly_config, progress_callback, result
                )
            else:
                # Default complete assembly
                assembly_success = self._assemble_complete_fragments(
                    assembly_plan, assembly_config, progress_callback, result
                )
            
            if not assembly_success:
                return result
            
            # Post-assembly validation
            if progress_callback:
                progress_callback(90, 100, "Validating assembly")
            
            self._validate_assembly_result(assembly_plan, target_dir, result)
            
            # Generate assembly metadata
            if progress_callback:
                progress_callback(95, 100, "Generating metadata")
            
            self._generate_assembly_metadata(assembly_plan, target_dir, result)
            
            # Finalize result
            result.success = True
            result.assembly_time = time.time() - start_time
            result.generate_assembly_summary()
            
            if progress_callback:
                progress_callback(100, 100, "Assembly complete")
            
            self._logger.info(
                f"Fragment assembly completed successfully: "
                f"{result.total_files_extracted} files in {result.assembly_time:.1f}s"
            )
            
        except Exception as e:
            result.add_error(f"Fragment assembly failed with exception: {str(e)}")
            self._logger.exception("Fragment assembly failed")
        
        return result
    
    def assemble_temporal_range(
        self,
        fragments: List[ArchiveMetadata],
        target_location: LocationEntity,
        date_range: str,
        target_path: Optional[str] = None,
        conflict_strategy: FragmentConflictStrategy = FragmentConflictStrategy.NEWEST_WINS,
        progress_callback: Optional[Callable[[int, int, str], None]] = None
    ) -> AssemblyResult:
        """
        Assemble fragments for a specific temporal range.
        
        Args:
            fragments: List of archive fragments
            target_location: Where to assemble the simulation
            date_range: Date range string (e.g., "2024-03-01:2024-03-31")
            target_path: Optional specific path within target location
            conflict_strategy: How to handle conflicts
            progress_callback: Optional progress callback
            
        Returns:
            AssemblyResult with assembly details
        """
        # Create temporal filter
        date_range_obj = DateRange.from_string(date_range)
        assembly_filter = ArchiveExtractionFilter(date_range=date_range_obj)
        
        # Create assembly plan
        plan = self.create_assembly_plan(
            fragments, target_location, target_path,
            AssemblyMode.TEMPORAL, conflict_strategy, assembly_filter
        )
        
        # Execute assembly
        return self.assemble_fragments(plan, progress_callback)
    
    def assemble_content_types(
        self,
        fragments: List[ArchiveMetadata],
        target_location: LocationEntity,
        content_types: List[str],
        target_path: Optional[str] = None,
        conflict_strategy: FragmentConflictStrategy = FragmentConflictStrategy.NEWEST_WINS,
        progress_callback: Optional[Callable[[int, int, str], None]] = None
    ) -> AssemblyResult:
        """
        Assemble fragments containing specific content types.
        
        Args:
            fragments: List of archive fragments
            target_location: Where to assemble the simulation
            content_types: List of content type strings to include
            target_path: Optional specific path within target location
            conflict_strategy: How to handle conflicts
            progress_callback: Optional progress callback
            
        Returns:
            AssemblyResult with assembly details
        """
        # Convert strings to FileContentType enums
        include_types = set()
        for content_type in content_types:
            try:
                include_types.add(FileContentType(content_type))
            except ValueError:
                self._logger.warning(f"Unknown content type: {content_type}")
        
        # Create content type filter
        assembly_filter = ArchiveExtractionFilter(include_content_types=include_types)
        
        # Create assembly plan
        plan = self.create_assembly_plan(
            fragments, target_location, target_path,
            AssemblyMode.CONTENT_TYPE, conflict_strategy, assembly_filter
        )
        
        # Execute assembly
        return self.assemble_fragments(plan, progress_callback)
    
    def validate_fragment_compatibility(
        self,
        fragments: List[ArchiveMetadata]
    ) -> Tuple[bool, List[str], List[str]]:
        """
        Validate that fragments can be assembled together.
        
        Args:
            fragments: List of archive fragments to validate
            
        Returns:
            Tuple of (is_compatible, errors, warnings)
        """
        errors = []
        warnings = []
        
        if not fragments:
            errors.append("No fragments provided for validation")
            return False, errors, warnings
        
        # Check simulation compatibility
        simulation_ids = set()
        for fragment in fragments:
            if fragment.simulation_id:
                simulation_ids.add(fragment.simulation_id)
        
        if len(simulation_ids) > 1:
            errors.append(
                f"Fragments belong to different simulations: {', '.join(simulation_ids)}"
            )
        
        # Check for missing critical fragments
        fragment_types = set()
        for fragment in fragments:
            if fragment.fragment_info:
                content_types = fragment.fragment_info.get('content_types', [])
                fragment_types.update(content_types)
        
        # Warn if we don't have inputs or outputs
        if fragment_types:
            if 'input' not in fragment_types:
                warnings.append("No input fragments found - assembly may be incomplete")
            if 'output' not in fragment_types:
                warnings.append("No output fragments found - assembly may be incomplete")
        
        # Check for temporal gaps
        date_ranges = []
        for fragment in fragments:
            if fragment.fragment_info and 'date_range' in fragment.fragment_info:
                try:
                    date_range = DateRange.from_string(fragment.fragment_info['date_range'])
                    date_ranges.append(date_range)
                except Exception:
                    warnings.append(f"Invalid date range in fragment {fragment.archive_id}")
        
        if len(date_ranges) > 1:
            # Check for gaps between date ranges
            date_ranges.sort(key=lambda dr: dr.start_date or datetime.min)
            for i in range(len(date_ranges) - 1):
                current_end = date_ranges[i].end_date
                next_start = date_ranges[i + 1].start_date
                if current_end and next_start and next_start > current_end + timedelta(days=1):
                    warnings.append(
                        f"Temporal gap detected between fragments: "
                        f"{current_end} to {next_start}"
                    )
        
        return len(errors) == 0, errors, warnings
    
    def estimate_assembly_time(self, assembly_plan: AssemblyPlan) -> float:
        """
        Estimate the time required to complete the assembly.
        
        Args:
            assembly_plan: The assembly plan to estimate
            
        Returns:
            Estimated time in seconds
        """
        return assembly_plan.estimated_duration
    
    def _analyze_fragment_compatibility(self, plan: AssemblyPlan) -> None:
        """Analyze compatibility of fragments in the plan."""
        is_compatible, errors, warnings = self.validate_fragment_compatibility(plan.fragments)
        
        plan.validation_errors.extend(errors)
        plan.validation_warnings.extend(warnings)
        
        if not is_compatible:
            plan.is_valid = False
    
    def _detect_overlaps(self, plan: AssemblyPlan) -> None:
        """Detect overlapping content between fragments."""
        fragments = plan.fragments
        
        for i, fragment1 in enumerate(fragments):
            for j, fragment2 in enumerate(fragments[i + 1:], i + 1):
                overlap = self._analyze_fragment_overlap(fragment1, fragment2)
                if overlap:
                    plan.add_overlap(overlap)
    
    def _analyze_fragment_overlap(
        self, 
        fragment1: ArchiveMetadata, 
        fragment2: ArchiveMetadata
    ) -> Optional[FragmentOverlap]:
        """Analyze overlap between two fragments."""
        # Check for file-level overlaps using file inventories
        if fragment1.file_inventory and fragment2.file_inventory:
            files1 = set(f.relative_path for f in fragment1.file_inventory.list_files())
            files2 = set(f.relative_path for f in fragment2.file_inventory.list_files())
            
            overlapping_files = list(files1.intersection(files2))
            
            if overlapping_files:
                # Determine conflict potential
                conflict_potential = "low"
                if len(overlapping_files) > 10:
                    conflict_potential = "medium"
                if len(overlapping_files) > 50:
                    conflict_potential = "high"
                
                # Suggest resolution strategy
                resolution = self._suggest_overlap_resolution(fragment1, fragment2, overlapping_files)
                
                return FragmentOverlap(
                    fragment1_id=str(fragment1.archive_id),
                    fragment2_id=str(fragment2.archive_id),
                    overlapping_files=overlapping_files,
                    overlap_type="file",
                    conflict_potential=conflict_potential,
                    resolution_suggestion=resolution
                )
        
        # Check for temporal overlaps
        if (fragment1.fragment_info and fragment2.fragment_info and
            'date_range' in fragment1.fragment_info and 'date_range' in fragment2.fragment_info):
            
            try:
                range1 = DateRange.from_string(fragment1.fragment_info['date_range'])
                range2 = DateRange.from_string(fragment2.fragment_info['date_range'])
                
                # Check if ranges overlap
                if self._date_ranges_overlap(range1, range2):
                    return FragmentOverlap(
                        fragment1_id=str(fragment1.archive_id),
                        fragment2_id=str(fragment2.archive_id),
                        overlapping_files=[],  # Will be determined during extraction
                        overlap_type="temporal",
                        conflict_potential="medium",
                        resolution_suggestion="Use newest_wins conflict resolution"
                    )
            except Exception:
                # Invalid date ranges
                pass
        
        return None
    
    def _suggest_overlap_resolution(
        self, 
        fragment1: ArchiveMetadata, 
        fragment2: ArchiveMetadata, 
        overlapping_files: List[str]
    ) -> str:
        """Suggest the best resolution strategy for an overlap."""
        # If one fragment is much newer, suggest newest_wins
        if fragment1.created_time and fragment2.created_time:
            time_diff = abs(fragment1.created_time - fragment2.created_time)
            if time_diff > 86400:  # More than a day difference
                return "newest_wins"
        
        # If fragments have different content types, suggest merge_directories
        if (fragment1.fragment_info and fragment2.fragment_info and
            fragment1.fragment_info.get('content_types') != fragment2.fragment_info.get('content_types')):
            return "merge_directories"
        
        # Default suggestion
        return "newest_wins"
    
    def _date_ranges_overlap(self, range1: DateRange, range2: DateRange) -> bool:
        """Check if two date ranges overlap."""
        if not range1.start_date or not range1.end_date or not range2.start_date or not range2.end_date:
            return False
        
        return (range1.start_date <= range2.end_date and range2.start_date <= range1.end_date)
    
    def _determine_extraction_order(self, plan: AssemblyPlan) -> None:
        """Determine optimal order for extracting fragments."""
        fragments = plan.fragments
        
        # Sort by creation time (oldest first for base layer)
        sorted_fragments = sorted(fragments, key=lambda f: f.created_time)
        
        # Adjust order based on dependencies and content types
        ordered_ids = []
        
        for fragment in sorted_fragments:
            # Prioritize input fragments first
            if (fragment.fragment_info and 
                'input' in fragment.fragment_info.get('content_types', [])):
                ordered_ids.insert(0, str(fragment.archive_id))
            else:
                ordered_ids.append(str(fragment.archive_id))
        
        plan.extraction_order = ordered_ids
    
    def _resolve_dependencies(self, plan: AssemblyPlan) -> None:
        """Resolve dependencies between fragments."""
        # For now, simple dependency resolution based on content types
        dependencies = {}
        
        for fragment in plan.fragments:
            fragment_id = str(fragment.archive_id)
            deps = []
            
            # Output fragments depend on input fragments
            if (fragment.fragment_info and 
                'output' in fragment.fragment_info.get('content_types', [])):
                
                for other_fragment in plan.fragments:
                    if (other_fragment.fragment_info and 
                        'input' in other_fragment.fragment_info.get('content_types', [])):
                        deps.append(str(other_fragment.archive_id))
            
            dependencies[fragment_id] = deps
        
        plan.fragment_dependencies = dependencies
    
    def _estimate_assembly_complexity(self, plan: AssemblyPlan) -> None:
        """Estimate the complexity of the assembly operation."""
        fragment_count = len(plan.fragments)
        overlap_count = len(plan.overlaps)
        conflict_count = plan.conflicts_predicted
        
        # Simple heuristic for complexity estimation
        if fragment_count <= 2 and overlap_count == 0:
            complexity = AssemblyComplexity.SIMPLE
        elif fragment_count <= 5 and overlap_count <= 2 and conflict_count <= 10:
            complexity = AssemblyComplexity.MODERATE
        elif fragment_count <= 10 and overlap_count <= 5 and conflict_count <= 50:
            complexity = AssemblyComplexity.COMPLEX
        else:
            complexity = AssemblyComplexity.VERY_COMPLEX
        
        plan.estimated_complexity = complexity
    
    def _validate_assembly_plan(self, plan: AssemblyPlan) -> None:
        """Validate the complete assembly plan."""
        # Check if we have any fragments
        if not plan.fragments:
            plan.validation_errors.append("No fragments in assembly plan")
            plan.is_valid = False
            return
        
        # Check target location
        if not plan.target_location:
            plan.validation_errors.append("No target location specified")
            plan.is_valid = False
            return
        
        # Check for high-conflict overlaps with incompatible strategy
        high_conflicts = plan.get_high_conflict_overlaps()
        if high_conflicts and plan.conflict_strategy == FragmentConflictStrategy.FAIL_ON_CONFLICT:
            plan.validation_errors.append(
                f"High-conflict overlaps detected but strategy is fail_on_conflict"
            )
            plan.is_valid = False
            return
        
        # If we get here, plan is valid
        plan.is_valid = True
    
    def _prepare_assembly_target(
        self, 
        plan: AssemblyPlan, 
        result: AssemblyResult
    ) -> Optional[Path]:
        """Prepare the target directory for assembly."""
        try:
            # For now, support local filesystem assembly
            # TODO: Integrate with Location abstraction for remote storage
            if plan.target_location.get_protocol() != 'file':
                result.add_error(
                    f"Remote assembly not yet implemented for protocol: "
                    f"{plan.target_location.get_protocol()}"
                )
                return None
            
            base_path = Path(plan.target_location.config.get('path', '.'))
            if plan.target_path:
                target_dir = base_path / plan.target_path
            else:
                target_dir = base_path
            
            target_dir.mkdir(parents=True, exist_ok=True)
            
            if not target_dir.exists():
                result.add_error(f"Target directory does not exist: {target_dir}")
                return None
            
            return target_dir
            
        except Exception as e:
            result.add_error(f"Failed to prepare assembly target: {str(e)}")
            return None
    
    def _create_assembly_config(
        self, 
        plan: AssemblyPlan, 
        conflict_callback: Optional[ConflictResolutionCallback]
    ) -> ArchiveExtractionConfig:
        """Create extraction configuration for the assembly."""
        # Map fragment conflict strategy to extraction conflict resolution
        conflict_mapping = {
            FragmentConflictStrategy.NEWEST_WINS: ConflictResolution.NEWEST,
            FragmentConflictStrategy.LARGEST_WINS: ConflictResolution.LARGEST,
            FragmentConflictStrategy.FIRST_WINS: ConflictResolution.SKIP,
            FragmentConflictStrategy.MERGE_DIRECTORIES: ConflictResolution.MERGE,
            FragmentConflictStrategy.SKIP_CONFLICTS: ConflictResolution.SKIP,
            FragmentConflictStrategy.FAIL_ON_CONFLICT: ConflictResolution.FAIL,
            FragmentConflictStrategy.INTERACTIVE: ConflictResolution.NEWEST  # Fallback
        }
        
        conflict_resolution = conflict_mapping.get(
            plan.conflict_strategy, ConflictResolution.NEWEST
        )
        
        return ArchiveExtractionConfig(
            conflict_resolution=conflict_resolution,
            enable_fragment_tracking=True,
            use_atomic_extraction=True,
            verify_after_extraction=True
        )
    
    def _assemble_complete_fragments(
        self,
        plan: AssemblyPlan,
        config: ArchiveExtractionConfig,
        progress_callback: Optional[Callable[[int, int, str], None]],
        result: AssemblyResult
    ) -> bool:
        """Assemble all fragments completely."""
        total_fragments = len(plan.extraction_order)
        
        for i, fragment_id in enumerate(plan.extraction_order):
            fragment = plan.get_fragment_by_id(fragment_id)
            if not fragment:
                result.add_error(f"Fragment not found: {fragment_id}")
                continue
            
            # Update progress
            if progress_callback:
                progress = int(10 + (i / total_fragments) * 70)
                progress_callback(
                    progress, 100, 
                    f"Extracting fragment {i+1}/{total_fragments}: {fragment_id}"
                )
            
            # Extract this fragment
            archive_path = Path(fragment.location)
            extract_result = self._extraction_service.extract_archive(
                archive_path,
                plan.target_location,
                plan.target_path,
                plan.assembly_filter,
                config
            )
            
            # Record result
            result.add_extraction_result(fragment_id, extract_result)
            result.log_fragment_action(
                fragment_id, "extract", 
                {"files_extracted": extract_result.files_extracted}
            )
            
            # Check for critical errors
            if extract_result.has_errors and config.conflict_resolution == ConflictResolution.FAIL:
                result.add_error(f"Fragment extraction failed: {fragment_id}")
                return False
        
        return True
    
    def _assemble_temporal_fragments(
        self,
        plan: AssemblyPlan,
        config: ArchiveExtractionConfig,
        progress_callback: Optional[Callable[[int, int, str], None]],
        result: AssemblyResult
    ) -> bool:
        """Assemble fragments with temporal ordering."""
        # Sort fragments by date range
        temporal_fragments = []
        for fragment in plan.fragments:
            if fragment.fragment_info and 'date_range' in fragment.fragment_info:
                try:
                    date_range = DateRange.from_string(fragment.fragment_info['date_range'])
                    temporal_fragments.append((fragment, date_range))
                except Exception:
                    result.add_warning(f"Invalid date range in fragment {fragment.archive_id}")
        
        # Sort by start date
        temporal_fragments.sort(key=lambda x: x[1].start_date or datetime.min)
        
        # Extract in temporal order
        total_fragments = len(temporal_fragments)
        for i, (fragment, date_range) in enumerate(temporal_fragments):
            # Update progress
            if progress_callback:
                progress = int(10 + (i / total_fragments) * 70)
                progress_callback(
                    progress, 100,
                    f"Extracting temporal fragment {i+1}/{total_fragments}: {fragment.archive_id}"
                )
            
            # Extract this fragment
            archive_path = Path(fragment.location)
            extract_result = self._extraction_service.extract_archive(
                archive_path,
                plan.target_location,
                plan.target_path,
                plan.assembly_filter,
                config
            )
            
            # Record result
            result.add_extraction_result(str(fragment.archive_id), extract_result)
            result.log_fragment_action(
                str(fragment.archive_id), "temporal_extract",
                {
                    "date_range": fragment.fragment_info['date_range'],
                    "files_extracted": extract_result.files_extracted
                }
            )
        
        return True
    
    def _assemble_content_type_fragments(
        self,
        plan: AssemblyPlan,
        config: ArchiveExtractionConfig,
        progress_callback: Optional[Callable[[int, int, str], None]],
        result: AssemblyResult
    ) -> bool:
        """Assemble fragments by content type priority."""
        # Group fragments by content type
        content_groups = {}
        for fragment in plan.fragments:
            if fragment.fragment_info and 'content_types' in fragment.fragment_info:
                for content_type in fragment.fragment_info['content_types']:
                    if content_type not in content_groups:
                        content_groups[content_type] = []
                    content_groups[content_type].append(fragment)
        
        # Extract in priority order: inputs first, then outputs
        priority_order = ['input', 'restart', 'output', 'log', 'other']
        extracted_count = 0
        total_fragments = len(plan.fragments)
        
        for content_type in priority_order:
            if content_type not in content_groups:
                continue
            
            for fragment in content_groups[content_type]:
                # Update progress
                if progress_callback:
                    progress = int(10 + (extracted_count / total_fragments) * 70)
                    progress_callback(
                        progress, 100,
                        f"Extracting {content_type} fragment: {fragment.archive_id}"
                    )
                
                # Extract this fragment
                archive_path = Path(fragment.location)
                extract_result = self._extraction_service.extract_archive(
                    archive_path,
                    plan.target_location,
                    plan.target_path,
                    plan.assembly_filter,
                    config
                )
                
                # Record result
                result.add_extraction_result(str(fragment.archive_id), extract_result)
                result.log_fragment_action(
                    str(fragment.archive_id), "content_type_extract",
                    {
                        "content_type": content_type,
                        "files_extracted": extract_result.files_extracted
                    }
                )
                
                extracted_count += 1
        
        return True
    
    def _assemble_directory_fragments(
        self,
        plan: AssemblyPlan,
        config: ArchiveExtractionConfig,
        progress_callback: Optional[Callable[[int, int, str], None]],
        result: AssemblyResult
    ) -> bool:
        """Assemble fragments with directory-based organization."""
        # This is similar to complete assembly but with directory awareness
        return self._assemble_complete_fragments(plan, config, progress_callback, result)
    
    def _validate_assembly_result(
        self,
        plan: AssemblyPlan,
        target_dir: Path,
        result: AssemblyResult
    ) -> None:
        """Validate the assembly result."""
        try:
            # Check if target directory has expected content
            if not target_dir.exists():
                result.add_error("Target directory does not exist after assembly")
                return
            
            # Count files in target directory
            file_count = sum(1 for f in target_dir.rglob('*') if f.is_file())
            
            if file_count == 0:
                result.add_warning("No files found in assembled directory")
            elif file_count < result.total_files_extracted * 0.8:
                result.add_warning(
                    f"Fewer files found ({file_count}) than expected "
                    f"({result.total_files_extracted})"
                )
            
        except Exception as e:
            result.add_warning(f"Could not validate assembly result: {str(e)}")
    
    def _generate_assembly_metadata(
        self,
        plan: AssemblyPlan,
        target_dir: Path,
        result: AssemblyResult
    ) -> None:
        """Generate metadata file for the assembled simulation."""
        try:
            metadata_file = target_dir / ".tellus_assembly.json"
            
            assembly_metadata = {
                'assembly_id': plan.assembly_id,
                'created_time': time.time(),
                'fragments': [
                    {
                        'archive_id': str(f.archive_id),
                        'location': f.location,
                        'fragment_info': f.fragment_info
                    }
                    for f in plan.fragments
                ],
                'assembly_mode': plan.assembly_mode.value,
                'conflict_strategy': plan.conflict_strategy.value,
                'extraction_order': plan.extraction_order,
                'overlaps_detected': len(plan.overlaps),
                'conflicts_resolved': result.conflicts_resolved,
                'total_files': result.total_files_extracted,
                'total_bytes': result.total_bytes_extracted,
                'assembly_time': result.assembly_time
            }
            
            with open(metadata_file, 'w') as f:
                json.dump(assembly_metadata, f, indent=2, default=str)
            
            self._logger.info(f"Generated assembly metadata: {metadata_file}")
            
        except Exception as e:
            result.add_warning(f"Could not generate assembly metadata: {str(e)}")