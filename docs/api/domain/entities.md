# Domain Entities

Domain entities represent the core business objects in Tellus, containing pure business logic without external dependencies.

## Simulation Entities

### SimulationEntity

```{eval-rst}
.. currentmodule:: tellus.domain.entities.simulation

.. autoclass:: SimulationEntity
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__
```

**Key Properties:**
- `simulation_id` - Unique simulation identifier
- `model_id` - Climate model identifier  
- `attrs` - Flexible metadata dictionary
- `associated_locations` - Set of associated location names
- `location_contexts` - Location-specific context data

**Key Methods:**
- `get_associated_locations()` - Get all associated locations
- `associate_location()` - Associate with a location
- `disassociate_location()` - Remove location association
- `get_location_context()` - Get context for specific location
- `is_location_associated()` - Check location association status

### SimulationFile

```{eval-rst}
.. autoclass:: tellus.domain.entities.simulation_file.SimulationFile
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__
```

## Location Entities

### LocationEntity

```{eval-rst}
.. currentmodule:: tellus.domain.entities.location

.. autoclass:: LocationEntity
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__

.. autoclass:: LocationKind
   :members:
   :undoc-members:
   :show-inheritance:
```

**Location Kinds:**
- `TAPE` - Tape archive systems (long-term storage)
- `COMPUTE` - HPC compute environments with scratch space
- `DISK` - Local or network-attached disk storage
- `FILESERVER` - Centralized file servers and cloud storage

**Key Properties:**
- `name` - Unique location identifier
- `kinds` - List of LocationKind values
- `protocol` - Storage protocol (file, ssh, s3, etc.)
- `config` - Protocol-specific configuration
- `metadata` - Additional metadata

### LocationContext

```{eval-rst}
.. autoclass:: LocationContext
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__
```

## Archive Entities

### ArchiveMetadata

```{eval-rst}
.. currentmodule:: tellus.domain.entities.archive

.. autoclass:: ArchiveMetadata
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__

.. autoclass:: ArchiveId
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__

.. autoclass:: ArchiveType
   :members:
   :undoc-members:
   :show-inheritance:
```

**Archive Types:**
- `COMPRESSED` - Compressed tar archives (.tar.gz, .tar.bz2)
- `UNCOMPRESSED` - Uncompressed tar archives (.tar)
- `DIRECTORY` - Directory-based archives

### ArchiveFile

```{eval-rst}
.. autoclass:: ArchiveFile
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__
```

## File Tracking Entities

### TrackedFile

```{eval-rst}
.. currentmodule:: tellus.domain.entities.file_tracking

.. autoclass:: TrackedFile
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__

.. autoclass:: FileStatus
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: ChecksumInfo
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__
```

**File Statuses:**
- `TRACKED` - File is being tracked
- `MODIFIED` - File has been modified since last check
- `MISSING` - File is missing from expected location
- `ARCHIVED` - File has been archived
- `DELETED` - File has been intentionally deleted

## File Type Configuration

### FileTypeConfig and FileContentType

```{eval-rst}
.. currentmodule:: tellus.domain.entities.file_type_config

.. autoclass:: FileTypeConfig
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__

.. autoclass:: FileContentType
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: FileRole
   :members:
   :undoc-members:
   :show-inheritance:
```

**Content Types:**
- `MODEL_OUTPUT` - Climate model output files (.nc, .grb)
- `INPUT_DATA` - Model input and boundary conditions
- `PROCESSED_DATA` - Post-processed analysis results
- `DOCUMENTATION` - Documentation and metadata files
- `SCRIPTS` - Analysis and processing scripts
- `CONFIGURATION` - Model and analysis configuration files
- `LOG` - Log and diagnostic files
- `UNKNOWN` - Files with unknown or unclassified content

**File Roles:**
- `PRIMARY` - Main data files for analysis
- `AUXILIARY` - Supporting files (metadata, logs)
- `INTERMEDIATE` - Temporary processing files
- `BACKUP` - Backup copies of important files

## Progress Tracking Entities

### ProgressOperation

```{eval-rst}
.. currentmodule:: tellus.domain.entities.progress_tracking

.. autoclass:: ProgressOperation
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__

.. autoclass:: OperationStatus
   :members:
   :undoc-members:
   :show-inheritance:
```

**Operation Statuses:**
- `PENDING` - Operation queued but not started
- `RUNNING` - Operation currently executing
- `PAUSED` - Operation temporarily paused
- `COMPLETED` - Operation finished successfully
- `FAILED` - Operation failed with errors
- `CANCELLED` - Operation cancelled by user

## Workflow Entities

### WorkflowDefinition

```{eval-rst}
.. currentmodule:: tellus.domain.entities.workflow

.. autoclass:: WorkflowDefinition
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__

.. autoclass:: WorkflowStep
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__

.. autoclass:: WorkflowExecution
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__
```

## Usage Examples

### Creating Simulations with Location Associations

```python
from tellus.domain.entities.simulation import SimulationEntity

# Create CESM2 simulation entity
simulation = SimulationEntity(
    simulation_id="cesm2-historical-r1i1p1f1",
    model_id="CESM2.1",
    attrs={
        "experiment": "historical",
        "time_period": "1850-2014",
        "resolution": "f09_g17",
        "ensemble_member": "r1i1p1f1",
        "atmospheric_model": "CAM6",
        "ocean_model": "POP2",
        "institution": "NCAR"
    }
)

# Associate with storage locations
simulation.associate_location("hpc-scratch", {
    "path_prefix": "/scratch/cesm2/{simulation_id}",
    "file_pattern": "*.nc",
    "stage": "processing"
})

simulation.associate_location("long-term-archive", {
    "path_prefix": "/archive/cesm2/{model_id}/{experiment}",
    "compression": "lz4",
    "retention": "10_years"
})

# Check associations
assert simulation.is_location_associated("hpc-scratch")
context = simulation.get_location_context("hpc-scratch")
print(f"Path prefix: {context.get('path_prefix')}")
```

### Configuring Storage Locations

```python
from tellus.domain.entities.location import LocationEntity, LocationKind

# Local analysis workspace
local_workspace = LocationEntity(
    name="local-analysis",
    kinds=[LocationKind.DISK],
    protocol="file",
    config={"path": "/home/researcher/climate-data"},
    metadata={
        "purpose": "analysis",
        "capacity_gb": 2000,
        "backup_enabled": True
    }
)

# HPC compute location
hpc_compute = LocationEntity(
    name="ncar-cheyenne",
    kinds=[LocationKind.COMPUTE, LocationKind.DISK],
    protocol="ssh",
    config={
        "host": "cheyenne.ucar.edu",
        "username": "researcher",
        "path": "/glade/scratch/researcher"
    },
    metadata={
        "institution": "NCAR",
        "scheduler": "PBS",
        "max_walltime": "12:00:00",
        "cores_per_node": 36,
        "max_nodes": 4608
    }
)

# Cloud archive location
cloud_archive = LocationEntity(
    name="aws-climate-archive", 
    kinds=[LocationKind.FILESERVER],
    protocol="s3",
    config={
        "bucket": "climate-data-archive",
        "region": "us-west-2",
        "prefix": "cesm2-data/"
    },
    metadata={
        "storage_class": "GLACIER",
        "cost_per_gb_month": 0.004,
        "retrieval_hours": 3-5
    }
)
```

### Managing Archives

```python
from tellus.domain.entities.archive import (
    ArchiveMetadata, ArchiveId, ArchiveType, ArchiveFile
)
from tellus.domain.entities.file_type_config import FileContentType, FileRole

# Create archive metadata
archive = ArchiveMetadata(
    archive_id=ArchiveId("cesm2-historical-processed-v2"),
    location="aws-climate-archive",
    archive_type=ArchiveType.COMPRESSED,
    simulation_id="cesm2-historical-r1i1p1f1",
    version="2.0",
    description="Processed CESM2 historical data with bias correction",
    tags={"cesm2", "historical", "processed", "bias_corrected"},
    metadata={
        "processing_date": "2024-03-15",
        "variables": ["tas", "pr", "psl"],
        "spatial_resolution": "1_degree",
        "reference_dataset": "ERA5"
    }
)

# Define archive files
temperature_file = ArchiveFile(
    relative_path="atmosphere/monthly/tas_Amon_CESM2_historical_r1i1p1f1_185001-201412.nc",
    size=1024 * 1024 * 500,  # 500 MB
    content_type=FileContentType.MODEL_OUTPUT,
    file_role=FileRole.PRIMARY,
    checksum="sha256:abc123...",
    metadata={
        "variable": "tas",
        "frequency": "monthly", 
        "time_range": "1850-2014",
        "units": "K"
    }
)

precipitation_file = ArchiveFile(
    relative_path="atmosphere/monthly/pr_Amon_CESM2_historical_r1i1p1f1_185001-201412.nc",
    size=1024 * 1024 * 750,  # 750 MB
    content_type=FileContentType.MODEL_OUTPUT,
    file_role=FileRole.PRIMARY,
    checksum="sha256:def456...",
    metadata={
        "variable": "pr",
        "frequency": "monthly",
        "time_range": "1850-2014", 
        "units": "kg m-2 s-1"
    }
)
```

### File Classification and Tracking

```python
from tellus.domain.entities.file_type_config import FileTypeConfig
from tellus.domain.entities.file_tracking import TrackedFile, FileStatus, ChecksumInfo

# Configure file type patterns
file_config = FileTypeConfig()

# Add NetCDF climate data pattern
file_config.add_pattern(
    pattern=r".*\.nc$",
    content_type=FileContentType.MODEL_OUTPUT,
    file_role=FileRole.PRIMARY,
    metadata_template={
        "format": "netcdf",
        "domain": "climate"
    }
)

# Add processing script pattern
file_config.add_pattern(
    pattern=r".*\.(py|sh|R)$",
    content_type=FileContentType.SCRIPTS,
    file_role=FileRole.AUXILIARY,
    metadata_template={
        "executable": True
    }
)

# Create tracked file
tracked_file = TrackedFile(
    file_path="/data/cesm2/tas_monthly.nc",
    content_type=FileContentType.MODEL_OUTPUT,
    file_role=FileRole.PRIMARY,
    status=FileStatus.TRACKED,
    checksum_info=ChecksumInfo(
        algorithm="sha256",
        value="abc123def456...",
        computed_at="2024-03-15T10:30:00Z"
    ),
    metadata={
        "variable": "tas",
        "simulation_id": "cesm2-historical-r1i1p1f1",
        "last_modified": "2024-03-15T10:00:00Z",
        "size_bytes": 524288000
    }
)

# Check if file needs update
if tracked_file.needs_checksum_update():
    # Recalculate checksum
    new_checksum = ChecksumInfo(
        algorithm="sha256",
        value="new_hash_value...",
        computed_at="2024-03-16T09:00:00Z"
    )
    tracked_file.update_checksum(new_checksum)
```

### Progress Tracking

```python
from tellus.domain.entities.progress_tracking import (
    ProgressOperation, OperationStatus
)
from datetime import datetime, timedelta

# Create progress operation
operation = ProgressOperation(
    operation_id="cesm2-bias-correction",
    operation_type="data_processing",
    description="Applying bias correction to CESM2 data",
    status=OperationStatus.PENDING,
    estimated_duration=timedelta(hours=4),
    metadata={
        "simulation_id": "cesm2-historical-r1i1p1f1",
        "method": "quantile_mapping",
        "variables": ["tas", "pr"]
    }
)

# Start operation
operation.start()
assert operation.status == OperationStatus.RUNNING
assert operation.started_at is not None

# Update progress
operation.update_progress(
    current_step=25,
    total_steps=100,
    status_message="Processing temperature data"
)

# Complete operation
operation.complete(
    result_summary="Bias correction completed successfully",
    result_metadata={
        "files_processed": 24,
        "output_size_gb": 12.5,
        "processing_time_hours": 3.2
    }
)
```

## Entity Relationships

Entities maintain relationships through IDs and references:

```python
# Simulation -> Location relationship through association
simulation = SimulationEntity(...)
simulation.associate_location("hpc-scratch", {...})

# Archive -> Simulation relationship through simulation_id
archive = ArchiveMetadata(
    simulation_id=simulation.simulation_id,
    ...
)

# File -> Archive relationship through archive membership
archive_file = ArchiveFile(
    relative_path="data/output.nc",
    ...
)

# Progress -> Operation relationship through operation_id
progress_op = ProgressOperation(
    operation_id="process-" + simulation.simulation_id,
    metadata={"simulation_id": simulation.simulation_id}
)
```

## Validation and Business Rules

Entities enforce business rules through validation:

```python
from tellus.domain.entities.simulation import SimulationEntity

# Invalid simulation ID raises validation error
try:
    invalid_sim = SimulationEntity(
        simulation_id="",  # Empty ID not allowed
        model_id="CESM2.1"
    )
except ValueError as e:
    print(f"Validation error: {e}")

# Location context validation
simulation = SimulationEntity("valid-id", "CESM2.1")

# Invalid context raises error
try:
    simulation.associate_location("", {})  # Empty location name
except ValueError as e:
    print(f"Association error: {e}")
```