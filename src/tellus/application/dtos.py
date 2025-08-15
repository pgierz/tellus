"""
Data Transfer Objects (DTOs) for the application layer.

These objects define the contracts between the application layer and external clients,
providing a stable interface that can evolve independently of the domain model.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Set
from enum import Enum

from ..domain.entities.location import LocationKind
from ..domain.entities.archive import ArchiveType, CacheCleanupPolicy
from ..domain.entities.workflow import WorkflowStatus, WorkflowEngine, ExecutionEnvironment
from ..domain.entities.simulation_file import FileContentType, FileImportance


# Base DTOs

@dataclass
class PaginationInfo:
    """Pagination information for list operations."""
    page: int = 1
    page_size: int = 50
    total_count: Optional[int] = None
    has_next: bool = False
    has_previous: bool = False


@dataclass
class FilterOptions:
    """Common filtering options."""
    search_term: Optional[str] = None
    tags: Set[str] = field(default_factory=set)
    created_after: Optional[str] = None  # ISO format datetime
    created_before: Optional[str] = None
    modified_after: Optional[str] = None
    modified_before: Optional[str] = None


# Simulation DTOs

@dataclass
class CreateSimulationDto:
    """DTO for creating a new simulation."""
    simulation_id: str
    model_id: Optional[str] = None
    path: Optional[str] = None
    attrs: Dict[str, Any] = field(default_factory=dict)
    namelists: Dict[str, Any] = field(default_factory=dict)
    snakemakes: Dict[str, Any] = field(default_factory=dict)


@dataclass
class UpdateSimulationDto:
    """DTO for updating an existing simulation."""
    model_id: Optional[str] = None
    path: Optional[str] = None
    attrs: Optional[Dict[str, Any]] = None
    namelists: Optional[Dict[str, Any]] = None
    snakemakes: Optional[Dict[str, Any]] = None


@dataclass
class SimulationDto:
    """DTO for simulation data."""
    simulation_id: str
    uid: str
    model_id: Optional[str] = None
    path: Optional[str] = None
    attrs: Dict[str, Any] = field(default_factory=dict)
    namelists: Dict[str, Any] = field(default_factory=dict)
    snakemakes: Dict[str, Any] = field(default_factory=dict)
    
    # Type-based contexts system - extensible for different context types
    contexts: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    # Example structure:
    # {
    #   "LocationContext": {
    #     "hsm.dmawi.de": {"path_prefix": "/hs/projects/...", "overrides": {...}},
    #     "albedo": {"path_prefix": "/albedo/work/...", "metadata": {...}}
    #   },
    #   "ExecutionContext": {
    #     "slurm_cluster": {"partition": "compute", "time": "24:00:00"}
    #   }
    # }
    
    # Derived properties for backward compatibility
    @property
    def associated_locations(self) -> List[str]:
        """Get list of associated location names."""
        location_contexts = self.contexts.get("LocationContext", {})
        return list(location_contexts.keys())
    
    def get_location_context(self, location_name: str) -> Optional[Dict[str, Any]]:
        """Get context data for a specific location."""
        location_contexts = self.contexts.get("LocationContext", {})
        return location_contexts.get(location_name)
    
    def set_location_context(self, location_name: str, context_data: Dict[str, Any]):
        """Set context data for a specific location."""
        if "LocationContext" not in self.contexts:
            self.contexts["LocationContext"] = {}
        self.contexts["LocationContext"][location_name] = context_data
    
    def remove_location_context(self, location_name: str) -> bool:
        """Remove context data for a specific location. Returns True if removed."""
        location_contexts = self.contexts.get("LocationContext", {})
        if location_name in location_contexts:
            del location_contexts[location_name]
            return True
        return False


@dataclass
class SimulationListDto:
    """DTO for paginated simulation lists."""
    simulations: List[SimulationDto]
    pagination: PaginationInfo
    filters_applied: FilterOptions


@dataclass
class SimulationLocationAssociationDto:
    """DTO for associating simulations with locations."""
    simulation_id: str
    location_names: List[str]
    context_overrides: Dict[str, Any] = field(default_factory=dict)


# Location DTOs

@dataclass
class CreateLocationDto:
    """DTO for creating a new location."""
    name: str
    kinds: List[str]  # Will be converted to LocationKind enums
    protocol: str
    path: Optional[str] = None
    storage_options: Dict[str, Any] = field(default_factory=dict)
    optional: bool = False
    additional_config: Dict[str, Any] = field(default_factory=dict)


@dataclass
class UpdateLocationDto:
    """DTO for updating an existing location."""
    kinds: Optional[List[str]] = None
    protocol: Optional[str] = None
    path: Optional[str] = None
    storage_options: Optional[Dict[str, Any]] = None
    optional: Optional[bool] = None
    additional_config: Optional[Dict[str, Any]] = None


@dataclass
class LocationDto:
    """DTO for location data."""
    name: str
    kinds: List[str]
    protocol: str
    path: Optional[str] = None
    storage_options: Dict[str, Any] = field(default_factory=dict)
    optional: bool = False
    additional_config: Dict[str, Any] = field(default_factory=dict)
    is_remote: bool = False
    is_accessible: Optional[bool] = None
    last_verified: Optional[str] = None  # ISO format datetime


@dataclass
class LocationListDto:
    """DTO for paginated location lists."""
    locations: List[LocationDto]
    pagination: PaginationInfo
    filters_applied: FilterOptions


@dataclass
class LocationTestResult:
    """DTO for location connectivity test results."""
    location_name: str
    success: bool
    error_message: Optional[str] = None
    latency_ms: Optional[float] = None
    available_space: Optional[int] = None  # in bytes
    protocol_specific_info: Dict[str, Any] = field(default_factory=dict)


# Archive DTOs

@dataclass
class CreateArchiveDto:
    """DTO for creating a new archive."""
    archive_id: str
    location_name: str
    archive_type: str  # Will be converted to ArchiveType enum
    simulation_id: Optional[str] = None  # Which simulation this archive contains parts of
    simulation_date: Optional[str] = None
    version: Optional[str] = None
    description: Optional[str] = None
    tags: Set[str] = field(default_factory=set)


@dataclass
class UpdateArchiveDto:
    """DTO for updating archive metadata."""
    simulation_date: Optional[str] = None
    version: Optional[str] = None
    description: Optional[str] = None
    tags: Optional[Set[str]] = None


@dataclass
class ArchiveDto:
    """DTO for archive metadata."""
    archive_id: str
    location: str
    archive_type: str
    simulation_id: Optional[str] = None  # Which simulation this archive contains parts of
    checksum: Optional[str] = None
    checksum_algorithm: Optional[str] = None
    size: Optional[int] = None
    created_time: float = 0
    simulation_date: Optional[str] = None
    version: Optional[str] = None
    description: Optional[str] = None
    tags: Set[str] = field(default_factory=set)
    is_cached: bool = False
    cache_path: Optional[str] = None


@dataclass
class FileMetadataDto:
    """DTO for file metadata within archives."""
    path: str
    size: Optional[int] = None
    checksum: Optional[str] = None
    checksum_algorithm: Optional[str] = None
    modified_time: Optional[float] = None
    tags: Set[str] = field(default_factory=set)


@dataclass
class ArchiveContentsDto:
    """DTO for archive contents information."""
    archive_id: str
    files: List[FileMetadataDto]
    total_files: int
    total_size: Optional[int] = None
    directory_structure: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ArchiveListDto:
    """DTO for paginated archive lists."""
    archives: List[ArchiveDto]
    pagination: PaginationInfo
    filters_applied: FilterOptions


@dataclass
class ArchiveOperationDto:
    """DTO for archive operations (extract, compress, etc.)."""
    archive_id: str
    operation: str  # 'extract', 'compress', 'verify', etc.
    source_path: Optional[str] = None
    destination_path: Optional[str] = None
    include_patterns: List[str] = field(default_factory=list)
    exclude_patterns: List[str] = field(default_factory=list)
    overwrite: bool = False
    preserve_permissions: bool = True
    compression_level: int = 6


@dataclass
class ArchiveOperationResult:
    """DTO for archive operation results."""
    operation_id: str
    archive_id: str
    operation: str
    success: bool
    start_time: float
    end_time: Optional[float] = None
    files_processed: int = 0
    bytes_processed: int = 0
    error_message: Optional[str] = None
    warnings: List[str] = field(default_factory=list)


# Cache DTOs

@dataclass
class CacheConfigurationDto:
    """DTO for cache configuration."""
    cache_directory: str
    archive_size_limit: int = 50 * 1024**3  # 50 GB
    file_size_limit: int = 10 * 1024**3  # 10 GB
    cleanup_policy: str = "lru"  # Will be converted to CacheCleanupPolicy enum
    unified_cache: bool = False


@dataclass
class CacheStatusDto:
    """DTO for cache status information."""
    total_size: int
    used_size: int
    available_size: int
    entry_count: int
    archive_count: int
    file_count: int
    cleanup_policy: str
    last_cleanup: Optional[float] = None
    oldest_entry: Optional[float] = None
    newest_entry: Optional[float] = None


@dataclass
class CacheEntryDto:
    """DTO for individual cache entries."""
    key: str
    size: int
    created_time: float
    last_accessed: float
    access_count: int
    entry_type: str  # 'archive' or 'file'
    tags: Set[str] = field(default_factory=set)


@dataclass
class CacheOperationResult:
    """DTO for cache operation results."""
    operation: str
    success: bool
    entries_affected: int = 0
    bytes_affected: int = 0
    duration_ms: float = 0
    error_message: Optional[str] = None


# Workflow DTOs

@dataclass
class WorkflowExecutionDto:
    """DTO for long-running workflow operations."""
    workflow_id: str
    name: str
    status: str  # 'pending', 'running', 'completed', 'failed', 'cancelled'
    progress: float = 0.0  # 0.0 to 1.0
    start_time: Optional[float] = None
    end_time: Optional[float] = None
    current_step: Optional[str] = None
    total_steps: Optional[int] = None
    completed_steps: int = 0
    error_message: Optional[str] = None
    result_data: Dict[str, Any] = field(default_factory=dict)


@dataclass
class BatchOperationDto:
    """DTO for batch operations on multiple entities."""
    operation_id: str
    operation_type: str
    entity_type: str
    total_entities: int
    processed_entities: int = 0
    successful_entities: int = 0
    failed_entities: int = 0
    errors: List[str] = field(default_factory=list)
    start_time: Optional[float] = None
    estimated_completion: Optional[float] = None


# Extended Workflow DTOs

@dataclass
class ResourceRequirementDto:
    """DTO for workflow resource requirements."""
    cores: Optional[int] = None
    memory_gb: Optional[float] = None
    disk_gb: Optional[float] = None
    gpu_count: Optional[int] = None
    walltime_hours: Optional[float] = None
    queue_name: Optional[str] = None
    custom_requirements: Dict[str, Any] = field(default_factory=dict)


@dataclass
class WorkflowStepDto:
    """DTO for individual workflow steps."""
    step_id: str
    name: str
    command: Optional[str] = None
    script_path: Optional[str] = None
    input_files: List[str] = field(default_factory=list)
    output_files: List[str] = field(default_factory=list)
    parameters: Dict[str, Any] = field(default_factory=dict)
    dependencies: List[str] = field(default_factory=list)
    resource_requirements: Optional[ResourceRequirementDto] = None
    retry_count: int = 0
    max_retries: int = 3


@dataclass
class CreateWorkflowDto:
    """DTO for creating a new workflow."""
    workflow_id: str
    name: str
    description: Optional[str] = None
    engine: str = "snakemake"  # Will be converted to WorkflowEngine enum
    workflow_file: Optional[str] = None
    steps: List[WorkflowStepDto] = field(default_factory=list)
    global_parameters: Dict[str, Any] = field(default_factory=dict)
    input_schema: Dict[str, Any] = field(default_factory=dict)
    output_schema: Dict[str, Any] = field(default_factory=dict)
    tags: Set[str] = field(default_factory=set)
    version: str = "1.0"
    author: Optional[str] = None


@dataclass
class UpdateWorkflowDto:
    """DTO for updating an existing workflow."""
    name: Optional[str] = None
    description: Optional[str] = None
    engine: Optional[str] = None
    workflow_file: Optional[str] = None
    steps: Optional[List[WorkflowStepDto]] = None
    global_parameters: Optional[Dict[str, Any]] = None
    input_schema: Optional[Dict[str, Any]] = None
    output_schema: Optional[Dict[str, Any]] = None
    tags: Optional[Set[str]] = None
    version: Optional[str] = None
    author: Optional[str] = None


@dataclass
class WorkflowDto:
    """DTO for complete workflow information."""
    workflow_id: str
    uid: str
    name: str
    description: Optional[str] = None
    engine: str = "snakemake"
    workflow_file: Optional[str] = None
    steps: List[WorkflowStepDto] = field(default_factory=list)
    global_parameters: Dict[str, Any] = field(default_factory=dict)
    input_schema: Dict[str, Any] = field(default_factory=dict)
    output_schema: Dict[str, Any] = field(default_factory=dict)
    tags: Set[str] = field(default_factory=set)
    version: str = "1.0"
    author: Optional[str] = None
    created_at: Optional[str] = None  # ISO format datetime
    estimated_resources: Optional[ResourceRequirementDto] = None
    associated_locations: List[str] = field(default_factory=list)
    location_contexts: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    input_location_mapping: Dict[str, str] = field(default_factory=dict)  # step_id -> location_name
    output_location_mapping: Dict[str, str] = field(default_factory=dict)  # step_id -> location_name


@dataclass
class WorkflowListDto:
    """DTO for paginated workflow lists."""
    workflows: List[WorkflowDto]
    pagination: PaginationInfo
    filters_applied: FilterOptions


@dataclass
class CreateWorkflowRunDto:
    """DTO for creating a workflow run."""
    run_id: str
    workflow_id: str
    execution_environment: str = "local"  # Will be converted to ExecutionEnvironment enum
    input_parameters: Dict[str, Any] = field(default_factory=dict)
    location_context: Dict[str, str] = field(default_factory=dict)
    max_retries: int = 3


@dataclass
class WorkflowRunDto:
    """DTO for workflow run information."""
    run_id: str
    uid: str
    workflow_id: str
    status: str  # WorkflowStatus enum value
    execution_environment: str = "local"
    input_parameters: Dict[str, Any] = field(default_factory=dict)
    location_context: Dict[str, str] = field(default_factory=dict)
    
    # Timing
    submitted_at: Optional[str] = None  # ISO format datetime
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    execution_time_seconds: Optional[float] = None
    
    # Progress
    current_step: Optional[str] = None
    completed_steps: List[str] = field(default_factory=list)
    failed_steps: List[str] = field(default_factory=list)
    progress: float = 0.0  # 0.0 to 1.0
    
    # Results and errors
    step_results: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    error_message: Optional[str] = None
    retry_count: int = 0
    max_retries: int = 3
    
    # Resources and outputs
    resource_usage: Dict[str, Any] = field(default_factory=dict)
    output_files: List[str] = field(default_factory=list)
    output_locations: Dict[str, str] = field(default_factory=dict)


@dataclass
class WorkflowRunListDto:
    """DTO for paginated workflow run lists."""
    runs: List[WorkflowRunDto]
    pagination: PaginationInfo
    filters_applied: FilterOptions


@dataclass
class WorkflowExecutionRequestDto:
    """DTO for workflow execution requests."""
    workflow_id: str
    run_id: Optional[str] = None  # Auto-generated if not provided
    execution_environment: str = "local"
    input_parameters: Dict[str, Any] = field(default_factory=dict)
    location_context: Dict[str, str] = field(default_factory=dict)
    resource_overrides: Optional[ResourceRequirementDto] = None
    priority: int = 5  # 1-10, where 10 is highest priority
    dry_run: bool = False


@dataclass
class WorkflowExecutionResultDto:
    """DTO for workflow execution results."""
    run_id: str
    workflow_id: str
    success: bool
    start_time: str  # ISO format datetime
    end_time: Optional[str] = None
    execution_time_seconds: Optional[float] = None
    completed_steps: List[str] = field(default_factory=list)
    failed_steps: List[str] = field(default_factory=list)
    output_files: List[str] = field(default_factory=list)
    error_message: Optional[str] = None
    warnings: List[str] = field(default_factory=list)
    resource_usage: Dict[str, Any] = field(default_factory=dict)


@dataclass
class CreateWorkflowTemplateDto:
    """DTO for creating workflow templates."""
    template_id: str
    name: str
    description: Optional[str] = None
    category: Optional[str] = None
    template_parameters: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    workflow_template: Dict[str, Any] = field(default_factory=dict)
    version: str = "1.0"
    author: Optional[str] = None
    tags: Set[str] = field(default_factory=set)


@dataclass
class WorkflowTemplateDto:
    """DTO for workflow template information."""
    template_id: str
    uid: str
    name: str
    description: Optional[str] = None
    category: Optional[str] = None
    template_parameters: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    workflow_template: Dict[str, Any] = field(default_factory=dict)
    version: str = "1.0"
    author: Optional[str] = None
    tags: Set[str] = field(default_factory=set)
    usage_count: int = 0


@dataclass
class WorkflowTemplateListDto:
    """DTO for paginated workflow template lists."""
    templates: List[WorkflowTemplateDto]
    pagination: PaginationInfo
    filters_applied: FilterOptions


@dataclass
class WorkflowInstantiationDto:
    """DTO for instantiating workflows from templates."""
    template_id: str
    workflow_id: str
    parameters: Dict[str, Any] = field(default_factory=dict)
    override_name: Optional[str] = None
    override_description: Optional[str] = None
    additional_tags: Set[str] = field(default_factory=set)


@dataclass
class WorkflowProgressDto:
    """DTO for workflow progress updates."""
    run_id: str
    workflow_id: str
    status: str
    progress: float  # 0.0 to 1.0
    current_step: Optional[str] = None
    completed_steps: int = 0
    total_steps: int = 0
    estimated_completion: Optional[str] = None  # ISO format datetime
    recent_log_entries: List[str] = field(default_factory=list)


@dataclass
class WorkflowResourceUsageDto:
    """DTO for workflow resource usage tracking."""
    run_id: str
    cores_used: Optional[int] = None
    memory_gb_used: Optional[float] = None
    disk_gb_used: Optional[float] = None
    gpu_count_used: Optional[int] = None
    wall_time_seconds: Optional[float] = None
    cpu_time_seconds: Optional[float] = None
    network_io_gb: Optional[float] = None
    custom_metrics: Dict[str, Any] = field(default_factory=dict)


@dataclass
class WorkflowLocationAssociationDto:
    """DTO for associating workflows with storage locations."""
    workflow_id: str
    location_names: List[str]
    input_location_mapping: Dict[str, str] = field(default_factory=dict)  # step_id -> location_name
    output_location_mapping: Dict[str, str] = field(default_factory=dict)  # step_id -> location_name
    context_overrides: Dict[str, str] = field(default_factory=dict)


# SimulationFile DTOs

@dataclass
class SimulationFileDto:
    """DTO for simulation file metadata."""
    relative_path: str
    size: Optional[int] = None
    checksum: Optional[str] = None
    content_type: str = "output"
    importance: str = "important"
    file_role: Optional[str] = None
    simulation_date: Optional[str] = None  # ISO format
    created_time: Optional[float] = None
    modified_time: Optional[float] = None
    source_archive: Optional[str] = None
    extraction_time: Optional[float] = None
    tags: Set[str] = field(default_factory=set)
    attributes: Dict[str, Any] = field(default_factory=dict)


@dataclass
class FileInventoryDto:
    """DTO for collections of simulation files."""
    files: List[SimulationFileDto]
    total_size: int = 0
    file_count: int = 0
    created_time: float = 0.0
    content_type_summary: Dict[str, int] = field(default_factory=dict)
    size_by_content_type: Dict[str, int] = field(default_factory=dict)


@dataclass
class ArchiveFileListDto:
    """DTO for listing files within an archive."""
    archive_id: str
    files: List[SimulationFileDto]
    total_files: int = 0
    total_size: int = 0
    content_types: Dict[str, int] = field(default_factory=dict)
    pagination: Optional[PaginationInfo] = None
    filters_applied: Optional[FilterOptions] = None


@dataclass
class FileAssociationDto:
    """DTO for associating files with simulations."""
    archive_id: str
    simulation_id: str
    files_to_associate: List[str]  # relative paths
    content_type_filter: Optional[str] = None
    pattern_filter: Optional[str] = None
    dry_run: bool = False


@dataclass
class FileAssociationResultDto:
    """DTO for file association operation results."""
    archive_id: str
    simulation_id: str
    files_associated: List[str]
    files_skipped: List[str]
    success: bool = True
    error_message: Optional[str] = None


# Archive Operation DTOs

@dataclass
class ArchiveCopyOperationDto:
    """DTO for archive copy operations."""
    archive_id: str
    source_location: str
    destination_location: str
    simulation_id: Optional[str] = None  # For context resolution
    preserve_metadata: bool = True
    overwrite_existing: bool = False
    verify_integrity: bool = True
    progress_callback: Optional[str] = None  # Callback ID for progress updates


@dataclass
class ArchiveMoveOperationDto:
    """DTO for archive move operations."""
    archive_id: str
    source_location: str
    destination_location: str
    simulation_id: Optional[str] = None  # For context resolution
    preserve_metadata: bool = True
    cleanup_source: bool = True
    verify_integrity: bool = True
    progress_callback: Optional[str] = None


@dataclass
class ArchiveExtractionDto:
    """DTO for archive extraction operations."""
    archive_id: str
    destination_location: str
    simulation_id: Optional[str] = None  # For context resolution
    file_filters: Optional[List[str]] = None  # Specific files to extract
    content_type_filter: Optional[str] = None
    pattern_filter: Optional[str] = None
    preserve_directory_structure: bool = True
    overwrite_existing: bool = False
    create_manifest: bool = True  # Create extraction manifest
    progress_callback: Optional[str] = None


@dataclass
class LocationContextResolutionDto:
    """DTO for resolving location path templates with simulation context."""
    location_name: str
    simulation_id: str
    path_template: str
    resolved_path: Optional[str] = None
    context_variables: Dict[str, str] = field(default_factory=dict)
    resolution_errors: List[str] = field(default_factory=list)


@dataclass
class ArchiveOperationProgressDto:
    """DTO for tracking archive operation progress."""
    operation_id: str
    operation_type: str  # copy, move, extract
    archive_id: str
    status: str  # pending, running, completed, failed, cancelled
    progress_percentage: float = 0.0
    bytes_processed: int = 0
    total_bytes: Optional[int] = None
    files_processed: int = 0
    total_files: Optional[int] = None
    current_file: Optional[str] = None
    estimated_completion: Optional[str] = None  # ISO format datetime
    error_message: Optional[str] = None
    started_at: Optional[str] = None  # ISO format datetime
    completed_at: Optional[str] = None


@dataclass
class ArchiveOperationResultDto:
    """DTO for archive operation results."""
    operation_id: str
    operation_type: str
    archive_id: str
    success: bool
    destination_path: Optional[str] = None
    bytes_processed: int = 0
    files_processed: int = 0
    duration_seconds: float = 0.0
    checksum_verification: bool = False
    manifest_created: bool = False
    warnings: List[str] = field(default_factory=list)
    error_message: Optional[str] = None


@dataclass
class BulkArchiveOperationDto:
    """DTO for bulk archive operations."""
    operation_type: str  # bulk_copy, bulk_move, bulk_extract
    archive_ids: List[str]
    destination_location: str
    simulation_id: Optional[str] = None
    operation_parameters: Dict[str, Any] = field(default_factory=dict)
    parallel_operations: int = 3
    stop_on_error: bool = False
    progress_callback: Optional[str] = None


@dataclass
class BulkOperationResultDto:
    """DTO for bulk operation results."""
    operation_id: str
    operation_type: str
    total_archives: int
    successful_operations: List[str] = field(default_factory=list)
    failed_operations: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    total_duration_seconds: float = 0.0
    total_bytes_processed: int = 0


@dataclass
class ExtractionManifestDto:
    """DTO for archive extraction manifests."""
    archive_id: str
    extraction_path: str
    simulation_id: Optional[str] = None
    extracted_files: List[SimulationFileDto] = field(default_factory=list)
    extraction_timestamp: str = ""  # ISO format
    source_location: str = ""
    checksum_verification: Dict[str, bool] = field(default_factory=dict)
    extraction_options: Dict[str, Any] = field(default_factory=dict)

