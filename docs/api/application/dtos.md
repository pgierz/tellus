# Data Transfer Objects (DTOs)

DTOs provide structured data containers for application service operations, ensuring type safety and validation for data flowing between layers.

## Core DTOs

### Simulation DTOs

```{eval-rst}
.. currentmodule:: tellus.application.dtos

.. autoclass:: CreateSimulationDto
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__

.. autoclass:: UpdateSimulationDto
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__

.. autoclass:: SimulationResponseDto
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__

.. autoclass:: SimulationListResponseDto
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__

.. autoclass:: SimulationLocationAssociationDto
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__
```

### Location DTOs

```{eval-rst}
.. autoclass:: CreateLocationDto
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__

.. autoclass:: UpdateLocationDto
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__

.. autoclass:: LocationResponseDto
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__

.. autoclass:: LocationListResponseDto
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__

.. autoclass:: LocationConnectionTestDto
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__
```

### Archive DTOs

```{eval-rst}
.. autoclass:: CreateArchiveDto
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__

.. autoclass:: ArchiveResponseDto
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__

.. autoclass:: ArchiveListResponseDto
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__

.. autoclass:: ArchiveFileListDto
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__

.. autoclass:: ArchiveExtractionDto
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__

.. autoclass:: ArchiveCopyDto
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__

.. autoclass:: ArchiveMoveDto
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__
```

### File Transfer DTOs

```{eval-rst}
.. autoclass:: FileTransferOperationDto
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__

.. autoclass:: BatchTransferOperationDto
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__

.. autoclass:: DirectoryTransferOperationDto
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__

.. autoclass:: TransferResultDto
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__

.. autoclass:: LocationSyncDto
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__
```

### Progress Tracking DTOs

```{eval-rst}
.. autoclass:: CreateProgressOperationDto
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__

.. autoclass:: ProgressOperationResponseDto
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__

.. autoclass:: ProgressOperationListDto
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__

.. autoclass:: UpdateProgressDto
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__
```

### File Tracking DTOs

```{eval-rst}
.. autoclass:: FileTrackingEntryDto
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__

.. autoclass:: FileTrackingQueryDto
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__

.. autoclass:: FileTrackingResultDto
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__
```

## Usage Examples

### Creating a Simulation

```python
from tellus.application.dtos import CreateSimulationDto

# Create CESM2 simulation DTO
cesm_dto = CreateSimulationDto(
    simulation_id="cesm2-historical-r1i1p1f1",
    model_id="CESM2.1",
    attrs={
        "experiment": "historical",
        "time_period": "1850-2014",
        "resolution": "f09_g17",
        "ensemble_member": "r1i1p1f1",
        "atmospheric_model": "CAM6",
        "ocean_model": "POP2",
        "land_model": "CLM5",
        "sea_ice_model": "CICE5",
        "output_frequency": "monthly",
        "variables": ["tas", "pr", "psl", "tos"],
        "institution": "NCAR",
        "contact": "researcher@ncar.ucar.edu"
    }
)

# Use with service
simulation_service = container.get_simulation_service()
simulation = simulation_service.create_simulation(cesm_dto)
```

### Configuring Storage Locations

```python
from tellus.application.dtos import CreateLocationDto
from tellus.domain.entities.location import LocationKind

# Local workspace
local_dto = CreateLocationDto(
    name="local-analysis",
    kinds=[LocationKind.DISK],
    protocol="file",
    path="/home/researcher/climate-data",
    description="Local workspace for data analysis",
    metadata={
        "purpose": "analysis",
        "capacity_gb": 1000,
        "backup": True
    }
)

# HPC scratch space
hpc_dto = CreateLocationDto(
    name="ncar-glade-scratch",
    kinds=[LocationKind.COMPUTE],
    protocol="ssh",
    config={
        "host": "glade.ucar.edu",
        "username": "researcher",
        "path": "/glade/scratch/researcher"
    },
    description="NCAR Glade scratch space",
    metadata={
        "institution": "NCAR",
        "scheduler": "PBS",
        "max_walltime": "12:00:00",
        "purge_policy": "30_days"
    }
)

# Cloud archive
s3_dto = CreateLocationDto(
    name="aws-climate-archive",
    kinds=[LocationKind.FILESERVER],
    protocol="s3",
    config={
        "bucket": "climate-data-archive",
        "region": "us-west-2",
        "prefix": "cesm2-data/"
    },
    description="AWS S3 long-term archive",
    metadata={
        "storage_class": "GLACIER",
        "retention_years": 10,
        "cost_per_gb_month": 0.004
    }
)
```

### File Transfer Operations

```python
from tellus.application.dtos import (
    FileTransferOperationDto,
    BatchTransferOperationDto,
    DirectoryTransferOperationDto
)

# Single file transfer
single_transfer = FileTransferOperationDto(
    source_location="ncar-glade-scratch",
    source_path="/glade/scratch/researcher/cesm2-output/tas_Amon_CESM2_historical_r1i1p1f1_185001-201412.nc",
    dest_location="local-analysis", 
    dest_path="cesm2-data/tas_monthly.nc",
    verify_checksum=True,
    overwrite=False,
    metadata={
        "variable": "tas",
        "frequency": "monthly",
        "simulation_id": "cesm2-historical-r1i1p1f1"
    }
)

# Batch transfer
batch_transfer = BatchTransferOperationDto(
    source_location="ncar-glade-scratch",
    dest_location="local-analysis",
    file_pairs=[
        ("/glade/scratch/researcher/cesm2-output/tas_*.nc", "cesm2-data/temperature/"),
        ("/glade/scratch/researcher/cesm2-output/pr_*.nc", "cesm2-data/precipitation/"),
        ("/glade/scratch/researcher/cesm2-output/psl_*.nc", "cesm2-data/pressure/")
    ],
    verify_checksums=True,
    max_concurrent=3,
    simulation_id="cesm2-historical-r1i1p1f1"
)

# Directory sync
directory_sync = DirectoryTransferOperationDto(
    source_location="ncar-glade-scratch",
    source_path="/glade/scratch/researcher/cesm2-output/",
    dest_location="aws-climate-archive",
    dest_path="cesm2-historical-r1i1p1f1/",
    recursive=True,
    exclude_patterns=["*.log", "*.tmp", "__pycache__"],
    include_patterns=["*.nc", "*.pdf"],
    preserve_timestamps=True,
    compression=True
)
```

### Archive Management

```python
from tellus.application.dtos import (
    CreateArchiveDto,
    ArchiveExtractionDto,
    ArchiveCopyDto
)

# Create archive
archive_dto = CreateArchiveDto(
    archive_id="cesm2-historical-processed-v2",
    location_name="aws-climate-archive",
    archive_type="compressed",
    simulation_id="cesm2-historical-r1i1p1f1",
    version="2.0",
    description="Processed CESM2 historical data with bias correction",
    tags={"cesm2", "historical", "processed", "bias_corrected", "v2"},
    metadata={
        "processing_date": "2024-03-15",
        "processing_method": "quantile_mapping",
        "reference_dataset": "ERA5",
        "variables": ["tas", "pr", "psl"],
        "spatial_resolution": "1_degree",
        "temporal_resolution": "monthly"
    }
)

# Extract specific files
extraction_dto = ArchiveExtractionDto(
    archive_id="cesm2-historical-processed-v2",
    destination_location="local-analysis",
    file_patterns=["**/tas_*.nc", "**/pr_*.nc"],
    content_type_filter="model_output",
    simulation_id="cesm2-historical-r1i1p1f1",
    preserve_structure=True,
    overwrite_existing=False
)

# Copy archive between locations
copy_dto = ArchiveCopyDto(
    archive_id="cesm2-historical-processed-v2",
    source_location="aws-climate-archive",
    dest_location="ncar-glade-work",
    simulation_id="cesm2-historical-r1i1p1f1",
    verify_integrity=True,
    preserve_metadata=True
)
```

### Progress Tracking

```python
from tellus.application.dtos import (
    CreateProgressOperationDto,
    UpdateProgressDto
)

# Create tracked operation
progress_dto = CreateProgressOperationDto(
    operation_id="cesm2-bias-correction",
    operation_type="data_processing",
    description="Applying bias correction to CESM2 historical simulation",
    estimated_duration=14400,  # 4 hours
    metadata={
        "simulation_id": "cesm2-historical-r1i1p1f1",
        "method": "quantile_mapping",
        "variables": ["tas", "pr"],
        "time_period": "1850-2014"
    }
)

# Update progress during operation
update_dto = UpdateProgressDto(
    operation_id="cesm2-bias-correction",
    current_step=25,
    total_steps=100,
    status_message="Processing temperature data for period 1851-1875",
    metadata={
        "current_variable": "tas",
        "current_period": "1851-1875",
        "files_processed": 25,
        "files_remaining": 75
    }
)
```

## Validation and Type Safety

All DTOs include comprehensive validation:

```python
from tellus.application.dtos import CreateSimulationDto
from tellus.application.exceptions import ValidationError

try:
    # This will raise ValidationError due to invalid simulation_id
    invalid_dto = CreateSimulationDto(
        simulation_id="",  # Empty string not allowed
        model_id="CESM2.1",
        attrs={}
    )
except ValidationError as e:
    print(f"Validation failed: {e}")

# Valid DTO with type checking
valid_dto = CreateSimulationDto(
    simulation_id="valid-simulation-id",
    model_id="CESM2.1",
    attrs={
        "experiment": "historical",  # str
        "ensemble_size": 5,          # int
        "output_enabled": True,      # bool
        "variables": ["tas", "pr"]   # List[str]
    }
)
```

## DTO Composition Patterns

DTOs can be composed for complex operations:

```python
# Multi-step workflow DTO composition
simulation_dto = CreateSimulationDto(...)
location_dto = CreateLocationDto(...)
association_dto = SimulationLocationAssociationDto(
    simulation_id=simulation_dto.simulation_id,
    location_names=[location_dto.name],
    context_overrides={
        location_dto.name: {
            "path_prefix": f"/data/{simulation_dto.simulation_id}",
            "file_pattern": "*.nc",
            "processing_stage": "raw"
        }
    }
)

# Use composed DTOs
simulation = simulation_service.create_simulation(simulation_dto)
location = location_service.create_location(location_dto)
simulation_service.associate_simulation_with_locations(association_dto)
```

## Serialization Support

DTOs support JSON serialization for API and persistence:

```python
import json
from tellus.application.dtos import CreateSimulationDto

# Create DTO
dto = CreateSimulationDto(
    simulation_id="cesm2-test",
    model_id="CESM2.1", 
    attrs={"experiment": "historical"}
)

# Serialize to JSON
json_data = dto.model_dump_json()
print(json_data)

# Deserialize from JSON
restored_dto = CreateSimulationDto.model_validate_json(json_data)
assert dto == restored_dto
```

## Performance Considerations

- DTOs use Pydantic for efficient validation and serialization
- Large metadata dictionaries are handled efficiently
- Validation is performed once at DTO creation
- JSON serialization is optimized for API performance
- Memory usage is minimal due to dataclass implementation