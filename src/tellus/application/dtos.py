"""
Data Transfer Objects (DTOs) for the application layer.

These objects define the contracts between the application layer and external clients,
providing a stable interface that can evolve independently of the domain model.
"""

import json
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Set

# Base mixin for JSON serialization
class JsonSerializable:
    """Mixin class that provides JSON serialization for dataclasses."""
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert the dataclass to a dictionary."""
        def serialize_value(obj):
            """Custom serializer for special types."""
            if isinstance(obj, Enum):
                return obj.value
            elif isinstance(obj, datetime):
                return obj.isoformat()
            elif isinstance(obj, set):
                return list(obj)
            elif hasattr(obj, 'to_dict'):
                return obj.to_dict()
            return obj
            
        def convert_dict(d):
            """Recursively convert dictionary values."""
            if isinstance(d, dict):
                return {k: convert_dict(serialize_value(v)) for k, v in d.items()}
            elif isinstance(d, (list, tuple)):
                return [convert_dict(serialize_value(item)) for item in d]
            else:
                return serialize_value(d)
        
        return convert_dict(asdict(self))
    
    def to_json(self, indent: int = 2) -> str:
        """Convert the dataclass to JSON string."""
        return json.dumps(self.to_dict(), indent=indent)
    
    def pretty_json(self) -> str:
        """Convert the dataclass to pretty-printed JSON string."""
        return self.to_json(indent=2)


# Local enums for DTOs (formerly from archive entity)
class CacheCleanupPolicy(str, Enum):
    """Cache cleanup policies."""
    LRU = "lru"
    SIZE_ONLY = "size_only"
    MANUAL = "manual"
from ..domain.entities.file_tracking import FileChangeType, TrackingStatus
from ..domain.entities.location import LocationKind
from ..domain.entities.simulation_file import FileContentType, FileImportance
from ..domain.entities.workflow import (ExecutionEnvironment, WorkflowEngine,
                                        WorkflowStatus)

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
class CreateSimulationDto(JsonSerializable):
    """DTO for creating a new simulation."""
    simulation_id: str
    model_id: Optional[str] = None
    path: Optional[str] = None
    attrs: Dict[str, Any] = field(default_factory=dict)
    namelists: Dict[str, Any] = field(default_factory=dict)
    snakemakes: Dict[str, Any] = field(default_factory=dict)


@dataclass
class UpdateSimulationDto(JsonSerializable):
    """DTO for updating an existing simulation."""
    model_id: Optional[str] = None
    path: Optional[str] = None
    attrs: Optional[Dict[str, Any]] = None
    namelists: Optional[Dict[str, Any]] = None
    snakemakes: Optional[Dict[str, Any]] = None


@dataclass
class SimulationDto(JsonSerializable):
    """DTO for simulation data (new clean format)."""
    simulation_id: str
    uid: str
    attributes: Dict[str, Any] = field(default_factory=dict)
    locations: Dict[str, Dict[str, Any]] = field(default_factory=dict)  # Simplified from contexts
    namelists: Dict[str, Any] = field(default_factory=dict)
    workflows: Dict[str, Any] = field(default_factory=dict)  # Renamed from snakemakes
    
    # Derived properties for backward compatibility
    @property
    def associated_locations(self) -> List[str]:
        """Get list of associated location names."""
        return list(self.locations.keys())
    
    def get_location_context(self, location_name: str) -> Optional[Dict[str, Any]]:
        """Get location-specific context/configuration."""
        return self.locations.get(location_name)
    
    # Backward compatibility properties
    @property
    def attrs(self) -> Dict[str, Any]:
        """Attrs property - maps to attributes."""
        return self.attributes
        
    @property
    def snakemakes(self) -> Dict[str, Any]:
        """Snakemakes property - maps to workflows."""
        return self.workflows
        
    @property
    def contexts(self) -> Dict[str, Dict[str, Any]]:
        """Contexts property - maps locations to nested format."""
        return {"LocationContext": self.locations}
        
    @property
    def model_id(self) -> Optional[str]:
        """Model ID property - extracted from attributes."""
        return self.attributes.get("model")
        
    @property
    def path(self) -> Optional[str]:
        """Path property - not used in current format, returns None."""
        return None


# Archive DTOs

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
class CreateLocationDto(JsonSerializable):
    """DTO for creating a new location."""
    name: str
    kinds: List[str]  # Will be converted to LocationKind enums
    protocol: str
    path: Optional[str] = None
    storage_options: Dict[str, Any] = field(default_factory=dict)
    additional_config: Dict[str, Any] = field(default_factory=dict)


@dataclass
class UpdateLocationDto(JsonSerializable):
    """DTO for updating an existing location."""
    kinds: Optional[List[str]] = None
    protocol: Optional[str] = None
    path: Optional[str] = None
    storage_options: Optional[Dict[str, Any]] = None
    config: Optional[Dict[str, Any]] = None


@dataclass
class LocationDto(JsonSerializable):
    """DTO for location data."""
    name: str
    kinds: List[str]
    protocol: str
    path: Optional[str] = None
    storage_options: Dict[str, Any] = field(default_factory=dict)
    additional_config: Dict[str, Any] = field(default_factory=dict)
    is_remote: bool = False
    is_accessible: Optional[bool] = None
    last_verified: Optional[str] = None  # ISO format datetime


@dataclass
class LocationListDto(JsonSerializable):
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
    archive_type: str = "compressed"  # Will be converted to ArchiveType enum
    source_path: Optional[str] = None  # Source path to archive
    archive_path: Optional[str] = None  # Actual filename/path in location (format-agnostic)
    simulation_id: Optional[str] = None  # Which simulation this archive contains parts of
    simulation_date: Optional[str] = None
    version: Optional[str] = None
    description: Optional[str] = None
    tags: Set[str] = field(default_factory=set)


@dataclass
class UpdateArchiveDto:
    """DTO for updating archive metadata."""
    simulation_id: Optional[str] = None
    simulation_date: Optional[str] = None
    version: Optional[str] = None
    description: Optional[str] = None
    archive_path: Optional[str] = None
    tags: Optional[Set[str]] = None
    path_prefix_to_strip: Optional[str] = None


@dataclass
class ArchiveDto:
    """DTO for archive metadata."""
    archive_id: str
    location: str
    archive_type: str
    simulation_id: Optional[str] = None  # Which simulation this archive contains parts of
    archive_path: Optional[str] = None  # Actual filename/path in location (format-agnostic)
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
    path_prefix_to_strip: Optional[str] = None


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


# Network Topology DTOs

@dataclass  
class BandwidthMeasurementDto(JsonSerializable):
    """DTO for bandwidth measurements."""
    bytes_per_second: float
    measurement_time: str
    quality_score: float = 1.0
    utilization_percent: Optional[float] = None


@dataclass
class LatencyMeasurementDto(JsonSerializable):
    """DTO for latency measurements."""
    milliseconds: float
    measurement_time: str
    packet_loss_percent: float = 0.0
    jitter_ms: float = 0.0


@dataclass
class NetworkConnectionDto(JsonSerializable):
    """DTO for network connections."""
    source_location_id: str
    destination_location_id: str
    connection_type: str
    bandwidth: Optional[BandwidthMeasurementDto] = None
    latency: Optional[LatencyMeasurementDto] = None
    is_bidirectional: bool = True
    health_status: str = "UNKNOWN"


@dataclass
class NetworkPathDto(JsonSerializable):
    """DTO for network paths."""
    hops: List[str]
    total_cost: float
    estimated_bandwidth_bps: Optional[float] = None
    estimated_latency_ms: Optional[float] = None


@dataclass
class TopologyBenchmarkDto(JsonSerializable):
    """DTO for topology benchmark requests."""
    source_locations: List[str]
    destination_locations: List[str]
    test_size_mb: int = 100
    timeout_seconds: int = 300
    force_refresh: bool = False


@dataclass
class OptimalRouteRequestDto(JsonSerializable):
    """DTO for optimal route requests."""
    source_location_id: str
    destination_location_id: str
    optimization_criteria: str = "bandwidth"  # bandwidth, latency, cost
    constraints: Dict[str, Any] = field(default_factory=dict)


@dataclass
class OptimalRouteResponseDto(JsonSerializable):
    """DTO for optimal route responses."""
    path: NetworkPathDto
    reasoning: str
    alternatives: List[NetworkPathDto] = field(default_factory=list)
    estimated_transfer_time_seconds: Optional[float] = None


@dataclass
class CreateNetworkTopologyDto(JsonSerializable):
    """DTO for creating network topology."""
    topology_id: str
    connections: List[NetworkConnectionDto] = field(default_factory=list)
    auto_discover: bool = True


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


# File Transfer DTOs

@dataclass
class FileTransferOperationDto:
    """DTO for single file transfer operations."""
    source_location: str  # Location name or 'local'
    source_path: str     # Path within source location
    dest_location: str   # Location name
    dest_path: str       # Path within destination location
    operation_type: str = "file_transfer"  # For queue routing
    overwrite: bool = False
    verify_checksum: bool = True
    chunk_size: int = 8 * 1024 * 1024  # 8MB chunks
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass 
class BatchFileTransferOperationDto:
    """DTO for batch file transfer operations."""
    transfers: List[FileTransferOperationDto]
    operation_type: str = "batch_file_transfer"
    parallel_transfers: int = 3
    stop_on_error: bool = False
    verify_all_checksums: bool = True
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class DirectoryTransferOperationDto:
    """DTO for recursive directory transfer operations."""
    source_location: str
    source_path: str
    dest_location: str  
    dest_path: str
    operation_type: str = "directory_transfer"
    recursive: bool = True
    overwrite: bool = False
    verify_checksums: bool = True
    exclude_patterns: List[str] = field(default_factory=list)  # glob patterns
    include_patterns: List[str] = field(default_factory=list)  # glob patterns
    preserve_permissions: bool = False
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class FileTransferResultDto:
    """DTO for file transfer operation results."""
    operation_id: str
    operation_type: str
    success: bool
    source_location: str
    source_path: str
    dest_location: str
    dest_path: str
    bytes_transferred: int = 0
    files_transferred: int = 0
    duration_seconds: float = 0.0
    throughput_mbps: float = 0.0
    checksum_verified: bool = False
    error_message: Optional[str] = None
    retry_count: int = 0
    partial_transfer: bool = False  # True if transfer was resumed


@dataclass
class BatchFileTransferResultDto:
    """DTO for batch file transfer results."""
    operation_id: str
    operation_type: str
    total_files: int
    successful_transfers: List[FileTransferResultDto] = field(default_factory=list)
    failed_transfers: List[FileTransferResultDto] = field(default_factory=list)
    total_bytes_transferred: int = 0
    total_duration_seconds: float = 0.0
    average_throughput_mbps: float = 0.0


# Progress Tracking DTOs

@dataclass
class ProgressMetricsDto:
    """DTO for progress metrics."""
    percentage: float = 0.0  # 0.0 to 100.0
    current_value: int = 0
    total_value: Optional[int] = None
    bytes_processed: int = 0
    total_bytes: Optional[int] = None
    files_processed: int = 0
    total_files: Optional[int] = None
    operations_completed: int = 0
    total_operations: Optional[int] = None


@dataclass
class ThroughputMetricsDto:
    """DTO for throughput and timing metrics."""
    start_time: float
    current_time: Optional[float] = None
    bytes_per_second: float = 0.0
    files_per_second: float = 0.0
    operations_per_second: float = 0.0
    estimated_completion_time: Optional[float] = None
    estimated_remaining_seconds: Optional[float] = None
    elapsed_seconds: float = 0.0


@dataclass
class ProgressLogEntryDto:
    """DTO for progress log entries."""
    timestamp: float
    datetime: str  # ISO format
    message: str
    level: str  # INFO, WARN, ERROR, DEBUG
    metrics: Optional[ProgressMetricsDto] = None
    throughput: Optional[ThroughputMetricsDto] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class OperationContextDto:
    """DTO for operation context information."""
    user_id: Optional[str] = None
    session_id: Optional[str] = None
    parent_operation_id: Optional[str] = None
    simulation_id: Optional[str] = None
    location_name: Optional[str] = None
    workflow_id: Optional[str] = None
    tags: Set[str] = field(default_factory=set)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class CreateProgressTrackingDto:
    """DTO for creating a new progress tracking entity."""
    operation_id: str
    operation_type: str  # OperationType enum value
    operation_name: str
    priority: str = "normal"  # Priority enum value
    context: Optional[OperationContextDto] = None


@dataclass
class UpdateProgressDto:
    """DTO for updating progress."""
    operation_id: str
    metrics: ProgressMetricsDto
    message: Optional[str] = None
    throughput: Optional[ThroughputMetricsDto] = None


@dataclass
class ProgressTrackingDto:
    """DTO for complete progress tracking information."""
    operation_id: str
    operation_type: str
    operation_name: str
    priority: str
    status: str  # OperationStatus enum value
    context: OperationContextDto
    created_time: float
    started_time: Optional[float] = None
    completed_time: Optional[float] = None
    last_update_time: float = 0.0
    current_metrics: ProgressMetricsDto = field(default_factory=ProgressMetricsDto)
    current_throughput: Optional[ThroughputMetricsDto] = None
    error_message: Optional[str] = None
    warnings: List[str] = field(default_factory=list)
    cancellation_requested: bool = False
    sub_operations: List[str] = field(default_factory=list)
    duration_seconds: Optional[float] = None


@dataclass
class ProgressTrackingListDto:
    """DTO for paginated progress tracking lists."""
    operations: List[ProgressTrackingDto]
    pagination: PaginationInfo
    filters_applied: FilterOptions


@dataclass
class ProgressUpdateNotificationDto:
    """DTO for progress update notifications."""
    operation_id: str
    notification_type: str  # progress_update, status_change, completion, failure
    timestamp: float
    current_status: str
    previous_status: Optional[str] = None
    metrics: Optional[ProgressMetricsDto] = None
    message: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class OperationControlDto:
    """DTO for operation control commands."""
    operation_id: str
    command: str  # start, pause, resume, cancel, force_cancel
    reason: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class OperationControlResultDto:
    """DTO for operation control command results."""
    operation_id: str
    command: str
    success: bool
    previous_status: str
    new_status: str
    message: Optional[str] = None
    timestamp: float = field(default_factory=time.time)


@dataclass
class BulkProgressQueryDto:
    """DTO for querying multiple operations."""
    operation_ids: List[str]
    include_metrics: bool = True
    include_throughput: bool = True
    include_log_entries: bool = False
    log_entry_limit: int = 10


@dataclass
class BulkProgressResponseDto:
    """DTO for bulk progress query responses."""
    operations: Dict[str, ProgressTrackingDto] = field(default_factory=dict)
    not_found: List[str] = field(default_factory=list)
    query_timestamp: float = field(default_factory=time.time)


@dataclass
class ProgressCallbackRegistrationDto:
    """DTO for registering progress callbacks."""
    operation_id: str
    callback_id: str
    callback_type: str  # websocket, http_post, file_write, in_memory
    callback_config: Dict[str, Any] = field(default_factory=dict)
    filter_criteria: Dict[str, Any] = field(default_factory=dict)  # e.g., min_percentage_change
    active: bool = True


@dataclass
class ProgressSummaryDto:
    """DTO for progress summary statistics."""
    total_operations: int
    active_operations: int
    completed_operations: int
    failed_operations: int
    cancelled_operations: int
    operations_by_type: Dict[str, int] = field(default_factory=dict)
    operations_by_status: Dict[str, int] = field(default_factory=dict)
    operations_by_priority: Dict[str, int] = field(default_factory=dict)
    total_bytes_processed: int = 0
    average_completion_time: Optional[float] = None
    oldest_active_operation: Optional[str] = None


@dataclass
class NestedProgressDto:
    """DTO for nested operation progress tracking."""
    parent_operation_id: str
    child_operations: List[ProgressTrackingDto] = field(default_factory=list)
    aggregated_metrics: ProgressMetricsDto = field(default_factory=ProgressMetricsDto)
    overall_status: str = "pending"
    completion_order: List[str] = field(default_factory=list)


# File Tracking DTOs

@dataclass
class CreateFileTrackingRepositoryDto:
    """DTO for creating a file tracking repository."""
    root_path: str
    enable_dvc: bool = False
    dvc_remote_name: Optional[str] = None
    dvc_remote_url: Optional[str] = None
    large_file_threshold: int = 100 * 1024 * 1024  # 100MB


@dataclass
class FileTrackingRepositoryDto:
    """DTO for file tracking repository information."""
    root_path: str
    tracked_file_count: int
    modified_file_count: int
    staged_file_count: int
    untracked_file_count: int
    dvc_enabled: bool
    last_snapshot_id: Optional[str] = None
    last_snapshot_time: Optional[str] = None


@dataclass
class TrackedFileDto:
    """DTO for tracked file information."""
    path: str
    size: int
    modification_time: str  # ISO format
    content_hash: str
    hash_algorithm: str
    status: str  # TrackingStatus value
    stage_hash: Optional[str] = None
    created_time: Optional[str] = None
    is_dvc_tracked: bool = False


@dataclass
class AddFilesDto:
    """DTO for adding files to tracking."""
    file_paths: List[str]
    force_add: bool = False
    use_dvc_for_large_files: bool = True
    

@dataclass
class FileStatusDto:
    """DTO for file status information."""
    tracked_files: List[TrackedFileDto] = field(default_factory=list)
    modified_files: List[str] = field(default_factory=list)
    staged_files: List[str] = field(default_factory=list)
    untracked_files: List[str] = field(default_factory=list)
    deleted_files: List[str] = field(default_factory=list)
    ignored_files: List[str] = field(default_factory=list)


@dataclass
class CreateSnapshotDto:
    """DTO for creating a repository snapshot."""
    message: str
    author: str
    include_files: Optional[List[str]] = None  # If None, include all staged files


@dataclass
class RepositorySnapshotDto:
    """DTO for repository snapshot information."""
    id: str
    short_id: str
    timestamp: str  # ISO format
    message: str
    author: str
    parent_id: Optional[str] = None
    changed_files: List[str] = field(default_factory=list)
    change_types: Dict[str, str] = field(default_factory=dict)  # file_path -> change_type


@dataclass
class DVCConfigurationDto:
    """DTO for DVC configuration."""
    enabled: bool = False
    remote_name: Optional[str] = None
    remote_url: Optional[str] = None
    cache_dir: Optional[str] = None
    large_file_threshold: int = 100 * 1024 * 1024


@dataclass
class DVCStatusDto:
    """DTO for DVC status information."""
    is_available: bool
    repository_initialized: bool
    configured_remotes: List[str] = field(default_factory=list)
    tracked_files: List[str] = field(default_factory=list)
    pending_pushes: List[str] = field(default_factory=list)
    pending_pulls: List[str] = field(default_factory=list)


# File Registration DTOs

@dataclass
class FileRegistrationDto:
    """DTO for registering archive files to a simulation."""
    archive_id: str
    simulation_id: str
    overwrite_existing: bool = False
    content_type_filter: Optional[str] = None
    pattern_filter: Optional[str] = None
    preserve_archive_references: bool = True


@dataclass  
class FileRegistrationResultDto:
    """DTO for results of file registration operation."""
    archive_id: str
    simulation_id: str
    success: bool
    files_registered: int = 0
    files_updated: int = 0
    files_skipped: int = 0
    duplicate_files: List[str] = field(default_factory=list)
    error_message: Optional[str] = None
    warnings: List[str] = field(default_factory=list)
    processing_time: Optional[float] = None


@dataclass
class SyncResultDto:
    """DTO for results of simulation-archive file synchronization."""
    simulation_id: str
    success: bool
    archives_processed: int = 0
    files_synced: int = 0
    files_added: int = 0
    files_removed: int = 0
    files_updated: int = 0
    sync_conflicts: List[str] = field(default_factory=list)
    error_message: Optional[str] = None
    warnings: List[str] = field(default_factory=list)
    processing_time: Optional[float] = None

