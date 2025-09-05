"""Pydantic models for simulation REST API endpoints."""

from datetime import datetime
from typing import Dict, List, Optional, Any, Union
from enum import Enum
from pydantic import BaseModel, Field, ConfigDict


class SimulationStatus(str, Enum):
    """Status of a simulation."""
    CREATED = "created"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    ARCHIVED = "archived"


class LocationKind(str, Enum):
    """Types of storage locations."""
    TAPE = "TAPE"
    COMPUTE = "COMPUTE"
    DISK = "DISK"
    FILESERVER = "FILESERVER"


class SimulationBase(BaseModel):
    """Base model for simulation data."""
    model_config = ConfigDict(protected_namespaces=())
    
    expid: str = Field(..., description="Unique experiment identifier")
    model_id: Optional[str] = Field(None, description="Model identifier")
    description: Optional[str] = Field(None, description="Human-readable description")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Additional metadata")


class SimulationCreate(SimulationBase):
    """Model for creating a new simulation."""
    location: Optional[str] = Field(None, description="Primary storage location name")
    path: Optional[str] = Field(None, description="Base path for simulation data")
    
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "expid": "AWI-ESM-1-1_historical_r1i1p1f1",
                "model_id": "AWI-ESM-1-1",
                "description": "Historical simulation run 1",
                "location": "compute_cluster",
                "path": "/work/data/experiments",
                "metadata": {
                    "project": "CMIP6",
                    "institution": "AWI",
                    "experiment": "historical"
                }
            }
        }
    )


class SimulationUpdate(BaseModel):
    """Model for updating simulation parameters."""
    model_config = ConfigDict(
        protected_namespaces=(),
        json_schema_extra={
            "example": {
                "description": "Updated historical simulation description",
                "metadata": {
                    "status": "completed",
                    "end_date": "2023-12-31"
                }
            }
        }
    )
    
    model_id: Optional[str] = None
    description: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


class SimulationParameterUpdate(BaseModel):
    """Model for parameter-based simulation updates."""
    parameters: Dict[str, str] = Field(..., description="Parameters to update in key=value format")
    
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "parameters": {
                    "status": "completed",
                    "description": "Historical run completed successfully",
                    "metadata.end_date": "2023-12-31"
                }
            }
        }
    )


class SimulationResponse(SimulationBase):
    """Model for simulation response data."""
    id: str = Field(..., description="Internal simulation ID")
    status: SimulationStatus = Field(default=SimulationStatus.CREATED)
    created_at: datetime = Field(..., description="Creation timestamp")
    updated_at: Optional[datetime] = Field(None, description="Last update timestamp")
    locations: List[str] = Field(default_factory=list, description="Associated storage locations")
    
    model_config = ConfigDict(from_attributes=True)


class SimulationSummary(BaseModel):
    """Summary model for simulation listings."""
    model_config = ConfigDict(protected_namespaces=())
    
    id: str
    expid: str
    model_id: Optional[str] = None
    status: SimulationStatus
    description: Optional[str] = None
    created_at: datetime
    location_count: int = Field(0, description="Number of associated locations")
    file_count: int = Field(0, description="Number of associated files")
    archive_count: int = Field(0, description="Number of archives")


# File-related models
class FileTypeEnum(str, Enum):
    """Types of simulation files."""
    OUTPUT = "output"
    RESTART = "restart"
    FORCING = "forcing" 
    CONFIG = "config"
    LOG = "log"
    OTHER = "other"


class SimulationFileBase(BaseModel):
    """Base model for simulation files."""
    name: str = Field(..., description="File name")
    path: str = Field(..., description="Full file path")
    file_type: FileTypeEnum = Field(default=FileTypeEnum.OTHER)
    size_bytes: Optional[int] = Field(None, description="File size in bytes")
    checksum: Optional[str] = Field(None, description="File checksum (MD5/SHA256)")
    metadata: Dict[str, Any] = Field(default_factory=dict)


class SimulationFileCreate(SimulationFileBase):
    """Model for creating simulation files."""
    pass


class SimulationFileResponse(SimulationFileBase):
    """Model for simulation file responses."""
    id: str
    simulation_id: str
    created_at: datetime
    updated_at: Optional[datetime] = None
    
    model_config = ConfigDict(from_attributes=True)


# Archive-related models
class ArchiveType(str, Enum):
    """Types of archives."""
    TAR = "tar"
    TAR_GZ = "tar.gz"
    TAR_BZ2 = "tar.bz2"
    ZIP = "zip"
    SPLIT_TAR = "split_tar"


class ArchiveStatus(str, Enum):
    """Status of archives."""
    CREATED = "created"
    STAGING = "staging"
    STAGED = "staged"
    EXTRACTING = "extracting"
    EXTRACTED = "extracted"
    FAILED = "failed"


class SimulationArchiveBase(BaseModel):
    """Base model for simulation archives."""
    name: str = Field(..., description="Archive name")
    archive_type: ArchiveType = Field(default=ArchiveType.TAR)
    location: str = Field(..., description="Storage location")
    path: str = Field(..., description="Archive path")
    size_bytes: Optional[int] = Field(None, description="Archive size")
    metadata: Dict[str, Any] = Field(default_factory=dict)


class SimulationArchiveCreate(SimulationArchiveBase):
    """Model for creating simulation archives."""
    pass


class SimulationArchiveResponse(SimulationArchiveBase):
    """Model for simulation archive responses."""
    id: str
    simulation_id: str
    status: ArchiveStatus = Field(default=ArchiveStatus.CREATED)
    created_at: datetime
    updated_at: Optional[datetime] = None
    
    model_config = ConfigDict(from_attributes=True)


class ArchiveAddRequest(BaseModel):
    """Model for adding existing archive files to a simulation."""
    location: str = Field(..., description="Storage location name")
    pattern: str = Field(..., description="File pattern to match")
    split_parts: Optional[int] = Field(None, description="Number of split parts for split archives")
    archive_type: ArchiveType = Field(default=ArchiveType.TAR)
    
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "location": "tape_storage",
                "pattern": "experiment_*.tar",
                "split_parts": 31,
                "archive_type": "split_tar"
            }
        }
    )


class ArchiveStageRequest(BaseModel):
    """Model for archive staging requests."""
    from_location: str = Field(..., description="Source location")
    to_location: str = Field(..., description="Destination location") 
    reconstruct: bool = Field(False, description="Reconstruct split archives")
    optimize_route: bool = Field(True, description="Use network topology optimization")
    via: Optional[str] = Field(None, description="Force route through specific location")
    show_route: bool = Field(False, description="Show optimal route without staging")
    
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "from_location": "tape_storage",
                "to_location": "compute_cluster",
                "reconstruct": True,
                "optimize_route": True
            }
        }
    )


class ArchiveExtractRequest(BaseModel):
    """Model for archive extraction requests."""
    location: str = Field(..., description="Target location for extracted files")
    variables: Optional[List[str]] = Field(None, description="Specific variables to extract")
    output_format: Optional[str] = Field(None, description="Output format (netcdf, zarr, etc.)")
    output_path: Optional[str] = Field(None, description="Custom output path")
    
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "location": "compute_cluster",
                "variables": ["temp", "salt"],
                "output_format": "netcdf",
                "output_path": "/work/extracted_data"
            }
        }
    )


class ArchiveContentsFilter(BaseModel):
    """Model for filtering archive contents."""
    filter_pattern: Optional[str] = Field(None, description="Pattern to filter file names")
    grep_pattern: Optional[str] = Field(None, description="Pattern to grep file contents")
    
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "filter_pattern": "*.nc",
                "grep_pattern": "temperature"
            }
        }
    )


class ArchiveContentItem(BaseModel):
    """Model for individual archive content items."""
    path: str = Field(..., description="File path within archive")
    size_bytes: Optional[int] = Field(None, description="Uncompressed file size")
    modified_time: Optional[datetime] = Field(None, description="File modification time")
    file_type: Optional[str] = Field(None, description="Detected file type")


class ArchiveContentsResponse(BaseModel):
    """Model for archive contents listing."""
    archive_id: str
    total_files: int
    total_size_bytes: Optional[int] = None
    contents: List[ArchiveContentItem]
    
    model_config = ConfigDict(from_attributes=True)


# Response models
class OperationResult(BaseModel):
    """Generic operation result."""
    success: bool
    message: str
    details: Optional[Dict[str, Any]] = None


class PaginatedResponse(BaseModel):
    """Generic paginated response."""
    items: List[Any]
    total: int
    page: int = Field(1, ge=1)
    size: int = Field(20, ge=1, le=100)
    pages: int


class ErrorDetail(BaseModel):
    """Error detail model."""
    error: str
    detail: str
    type: str
    timestamp: datetime = Field(default_factory=datetime.now)