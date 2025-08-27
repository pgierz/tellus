# Application Services Architecture

This document describes the application services layer for the Tellus Earth System Model data management system, following clean architecture principles.

## Overview

The application services layer orchestrates domain operations and implements use cases, providing a stable interface between the domain model and external clients. It handles cross-cutting concerns like validation, error handling, and workflow coordination.

## Architecture

```
┌─────────────────────────────────────────┐
│              Application Layer           │
├─────────────────┬───────────────────────┤
│   Services      │   DTOs & Exceptions   │
├─────────────────┼───────────────────────┤
│ • SimulationApp │ • CreateSimulationDto │
│ • LocationApp   │ • LocationDto         │
│ • ArchiveApp    │ • ArchiveDto          │
│                 │ • ValidationError     │
└─────────────────┴───────────────────────┘
           ▼ Dependencies (Interfaces)
┌─────────────────────────────────────────┐
│              Domain Layer               │
├─────────────────┬───────────────────────┤
│   Entities      │    Repositories       │
├─────────────────┼───────────────────────┤
│ • SimulationEnt │ • ISimulationRepo     │
│ • LocationEnt   │ • ILocationRepo       │
│ • ArchiveEnt    │                       │
└─────────────────┴───────────────────────┘
```

## Services

### SimulationApplicationService

Manages simulation lifecycle and coordinates with storage locations.

**Key Features:**
- CRUD operations for simulations
- Location association and validation
- Context variable generation for templates
- Snakemake rule management
- Business rule enforcement

**Example Usage:**
```python
from tellus.application.services import SimulationApplicationService
from tellus.application.dtos import CreateSimulationDto

# Create simulation
dto = CreateSimulationDto(
    simulation_id="FESOM2-historical-001",
    model_id="FESOM2",
    attrs={
        "experiment": "historical",
        "resolution": "T127",
        "ensemble": "r1i1p1f1"
    }
)

simulation = service.create_simulation(dto)
print(f"Created: {simulation.simulation_id}")
```

### LocationApplicationService

Manages storage locations with protocol-specific validation and connectivity testing.

**Key Features:**
- Multi-protocol support (local, SFTP, S3, etc.)
- Protocol-specific configuration validation
- Connectivity testing and health checks
- Path validation and accessibility checks
- Location discovery by kind/protocol

**Example Usage:**
```python
from tellus.application.services import LocationApplicationService
from tellus.application.dtos import CreateLocationDto

# Create HPC cluster location
dto = CreateLocationDto(
    name="dkrz-mistral",
    kinds=["COMPUTE", "DISK"],
    protocol="sftp",
    storage_options={
        "host": "mistral.dkrz.de",
        "port": 22,
        "username": "researcher"
    },
    path="/work/ba1234/experiments"
)

location = service.create_location(dto)

# Test connectivity
result = service.test_location_connectivity("dkrz-mistral")
if result.success:
    print(f"Connection OK (latency: {result.latency_ms:.2f}ms)")
else:
    print(f"Connection failed: {result.error_message}")
```

### ArchiveApplicationService

Coordinates archive operations, caching, and large-scale data management workflows.

**Key Features:**
- Archive metadata management
- Asynchronous compression/extraction
- Multi-level caching with configurable policies
- Integrity verification and checksums
- Long-running operation tracking
- Cache cleanup and space management

**Example Usage:**
```python
from tellus.application.services import ArchiveApplicationService
from tellus.application.dtos import ArchiveOperationDto

# Start archive extraction
dto = ArchiveOperationDto(
    archive_id="fesom2-output-2020",
    operation="extract",
    destination_path="/tmp/extracted",
    include_patterns=["*.nc", "*.txt"],
    overwrite=True
)

operation_id = service.extract_archive(dto)
print(f"Started extraction: {operation_id}")

# Monitor progress
while True:
    status = service.get_operation_status(operation_id)
    print(f"Progress: {status.progress:.1%} - {status.current_step}")
    
    if status.status in ["completed", "failed", "cancelled"]:
        break
```

## Service Factory & Dependency Injection

The `ApplicationServiceFactory` manages service lifecycle and dependency injection:

```python
from tellus.application.service_factory import ApplicationServiceFactory

# Initialize with repositories
factory = ApplicationServiceFactory(
    simulation_repository=simulation_repo,
    location_repository=location_repo,
    cache_config=cache_config
)

# Get services (created lazily)
sim_service = factory.simulation_service
loc_service = factory.location_service
arch_service = factory.archive_service

# Create workflow coordinator
coordinator = factory.create_simulation_workflow_coordinator()
```

## Workflow Coordination

Complex workflows are handled by the `SimulationWorkflowCoordinator`:

### Simulation Environment Setup

```python
results = coordinator.setup_simulation_environment(
    simulation_id="ICON-amip-r1i1p1f1",
    model_id="ICON",
    location_names=["dkrz-mistral", "tape-archive", "local-scratch"]
)

if results["simulation_created"]:
    print(f"Environment ready with {len(results['locations_validated'])} locations")
    
if results["warnings"]:
    print(f"Warnings: {results['warnings']}")
```

### Data Migration

```python
results = coordinator.migrate_simulation_data(
    simulation_id="FESOM2-historical-001",
    source_location="hpc-cluster",
    target_location="tape-archive",
    archive_id="fesom2-hist-backup-2024"
)

if results["data_transferred"] and results["integrity_verified"]:
    print("Migration completed successfully")
```

## Data Transfer Objects (DTOs)

DTOs provide stable interfaces and validation:

### Core DTOs

- **CreateSimulationDto**: Simulation creation parameters
- **UpdateSimulationDto**: Simulation update parameters
- **SimulationDto**: Complete simulation data
- **LocationDto**: Location configuration and status
- **ArchiveDto**: Archive metadata and cache status

### Operation DTOs

- **ArchiveOperationDto**: Archive operation parameters
- **ArchiveOperationResult**: Operation results and progress
- **CacheStatusDto**: Cache utilization and statistics
- **WorkflowExecutionDto**: Long-running workflow status

### Utility DTOs

- **PaginationInfo**: List pagination metadata
- **FilterOptions**: Common filtering criteria
- **LocationTestResult**: Connectivity test results

## Error Handling

Comprehensive exception hierarchy for different error scenarios:

```python
from tellus.application.exceptions import (
    EntityNotFoundError,
    ValidationError,
    LocationAccessError,
    ArchiveOperationError,
    CacheOperationError
)

try:
    simulation = service.get_simulation("non-existent")
except EntityNotFoundError as e:
    print(f"Simulation not found: {e.identifier}")
    
try:
    service.create_location(invalid_dto)
except ValidationError as e:
    print(f"Validation failed: {e.errors}")
```

## Earth Science Domain Patterns

### Template-based Path Resolution

Simulations provide context variables for path templates:

```python
simulation = service.get_simulation("FESOM2-historical-001")
context = simulation.context_variables

# Context includes: simulation_id, model_id, experiment, resolution, etc.
# Used in location path templates: "{model}/{experiment}/{simulation_id}"
```

### Multi-Location Data Management

Simulations can be associated with multiple storage locations:

```python
# Associate with compute, archive, and local storage
service.associate_locations(SimulationLocationAssociationDto(
    simulation_id="climate-run-001",
    location_names=["hpc-compute", "tape-archive", "local-cache"],
    context_overrides={"priority": "high", "retention": "10years"}
))
```

### Large Dataset Handling

Archives support different types and compression levels:

```python
# Different archive types for different use cases
archive_types = {
    "COMPRESSED": "tar.gz for space efficiency",
    "SPLIT_TARBALL": "Multiple files for parallel processing",
    "ORGANIZED": "Complex internal structure preservation"
}
```

### Caching Strategies

Multiple cache policies for different storage patterns:

```python
cache_config = CacheConfigurationDto(
    cache_directory="/fast/cache",
    archive_size_limit=100 * 1024**3,  # 100 GB
    cleanup_policy="lru",  # or "size_only", "manual"
    unified_cache=True
)
```

## Testing

Comprehensive test suite demonstrates usage and validates behavior:

```bash
# Run application service tests
pixi run -e test pytest tests/application/ -v

# Run specific test categories
pixi run -e test pytest -m "integration" tests/application/
```

## Extension Points

The architecture supports extension through:

1. **New Storage Protocols**: Extend `LocationApplicationService` with protocol-specific validation
2. **Additional Archive Types**: Add new `ArchiveType` enum values and handling
3. **Custom Workflows**: Create new coordinators for domain-specific workflows
4. **Cache Policies**: Implement new `CacheCleanupPolicy` strategies
5. **Validation Rules**: Add domain-specific business rules in services

## Performance Considerations

- **Lazy Service Creation**: Services created on-demand via factory
- **Asynchronous Operations**: Long-running operations use async patterns
- **Caching**: Multi-level caching for frequently accessed data
- **Batch Operations**: Support for bulk operations where appropriate
- **Progress Tracking**: Real-time progress for long-running operations

## Security Considerations

- **Input Validation**: All DTOs validated before processing
- **Path Traversal Protection**: Safe path handling for all file operations
- **Credential Management**: Secure storage options handling
- **Access Control**: Ready for permission-based operation filtering
- **Audit Logging**: Comprehensive logging of all operations

This application services architecture provides a robust, testable, and extensible foundation for Earth System Model data management workflows while maintaining clean separation between business logic and infrastructure concerns.