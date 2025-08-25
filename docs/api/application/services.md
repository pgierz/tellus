# Application Services

Application services provide high-level orchestration of business operations, coordinating between domain entities and infrastructure concerns.

## Core Services

### Simulation Service

```{eval-rst}
.. currentmodule:: tellus.application.services.simulation_service

.. autoclass:: SimulationApplicationService
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__
```

**Key Methods:**
- `create_simulation()` - Register a new simulation
- `list_simulations()` - List all registered simulations  
- `get_simulation()` - Retrieve simulation by ID
- `update_simulation_attrs()` - Update simulation attributes
- `delete_simulation()` - Remove simulation record
- `associate_simulation_with_locations()` - Link simulation to storage locations

### Location Service

```{eval-rst}
.. currentmodule:: tellus.application.services.location_service

.. autoclass:: LocationApplicationService
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__
```

**Key Methods:**
- `create_location()` - Register a new storage location
- `list_locations()` - List all configured locations
- `get_location()` - Retrieve location by name
- `update_location()` - Update location configuration
- `delete_location()` - Remove location record
- `test_connection()` - Test location accessibility

### Archive Service

```{eval-rst}
.. currentmodule:: tellus.application.services.archive_service

.. autoclass:: ArchiveApplicationService
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__
```

**Key Methods:**
- `create_archive_metadata()` - Create archive metadata record
- `list_archives()` - List all archives
- `get_archive_files()` - List files in archive
- `extract_archive_to_location()` - Extract archive contents
- `copy_archive_between_locations()` - Copy archive between locations
- `move_archive_between_locations()` - Move archive between locations

### File Transfer Service

```{eval-rst}
.. currentmodule:: tellus.application.services.file_transfer_service

.. autoclass:: FileTransferApplicationService
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__
```

**Key Methods:**
- `transfer_file()` - Transfer single file between locations
- `transfer_batch()` - Transfer multiple files in batch
- `transfer_directory()` - Recursively transfer directory
- `sync_locations()` - Synchronize data between locations

### Progress Tracking Service

```{eval-rst}
.. currentmodule:: tellus.application.services.progress_tracking_service

.. autoclass:: ProgressTrackingApplicationService
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__
```

**Key Methods:**
- `create_operation()` - Create tracked operation
- `list_operations()` - List operations with filtering
- `get_operation()` - Get operation by ID
- `update_operation_progress()` - Update operation progress
- `complete_operation()` - Mark operation as completed
- `cancel_operation()` - Cancel running operation

## Support Services

### File Tracking Service

```{eval-rst}
.. currentmodule:: tellus.application.services.file_tracking_service

.. autoclass:: FileTrackingApplicationService
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__
```

### Operation Queue Service

```{eval-rst}
.. currentmodule:: tellus.application.services.operation_queue_service

.. autoclass:: OperationQueueService
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__
```

### Workflow Execution Service

```{eval-rst}
.. currentmodule:: tellus.application.services.workflow_execution_service

.. autoclass:: WorkflowExecutionApplicationService
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__
```

## Operation Handlers

### Operation Handler Interface

```{eval-rst}
.. currentmodule:: tellus.application.services.operation_handler

.. autoclass:: OperationHandler
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__
```

## Usage Examples

### Basic Service Usage

```python
from tellus.application.container import ServiceContainer

# Initialize service container
container = ServiceContainer()

# Get services
simulation_service = container.get_simulation_service()
location_service = container.get_location_service()
archive_service = container.get_archive_service()

# Services are ready to use
simulations = simulation_service.list_simulations()
```

### Creating a Complete Workflow

```python
from tellus.application.dtos import (
    CreateSimulationDto, CreateLocationDto, 
    FileTransferOperationDto
)

# Create simulation
sim_dto = CreateSimulationDto(
    simulation_id="cesm2-test",
    model_id="CESM2.1",
    attrs={"experiment": "historical"}
)
simulation = simulation_service.create_simulation(sim_dto)

# Create locations
local_dto = CreateLocationDto(
    name="local-workspace",
    kinds=[LocationKind.DISK],
    protocol="file",
    path="/home/user/climate-data"
)
local_location = location_service.create_location(local_dto)

# Transfer files
transfer_service = container.get_file_transfer_service()
transfer_dto = FileTransferOperationDto(
    source_location="archive",
    source_path="/archive/cesm2-data.tar.gz",
    dest_location="local-workspace",
    dest_path="cesm2-data.tar.gz"
)
result = await transfer_service.transfer_file(transfer_dto)
```

### Progress Tracking Integration

```python
# Create tracked operation
progress_service = container.get_progress_tracking_service()

operation = progress_service.create_operation(
    operation_id="cesm2-processing",
    operation_type="data_processing",
    description="Processing CESM2 historical simulation",
    estimated_duration=7200  # 2 hours
)

# Update progress during operation
for step in range(100):
    # Do processing work...
    progress_service.update_operation_progress(
        operation.operation_id,
        current_step=step,
        total_steps=100,
        status_message=f"Processing step {step}/100"
    )

# Complete operation
progress_service.complete_operation(
    operation.operation_id,
    result_summary="Processing completed successfully"
)
```

## Error Handling

All application services use structured exception handling:

```python
from tellus.application.exceptions import (
    TellusApplicationError,
    ValidationError,
    OperationError
)

try:
    simulation = simulation_service.create_simulation(invalid_dto)
except ValidationError as e:
    print(f"Validation failed: {e}")
except TellusApplicationError as e:
    print(f"Application error: {e}")
```

## Thread Safety

Application services are thread-safe and support concurrent operations:

- Repository access is synchronized
- Progress tracking supports concurrent updates
- File operations use proper locking mechanisms
- Service container provides singleton instances

## Performance Notes

- Services use lazy initialization for dependencies
- Database connections are pooled for efficiency  
- File operations stream data to minimize memory usage
- Progress callbacks are optimized for high-frequency updates