# API Reference

This section provides comprehensive API documentation for all Tellus classes, functions, and modules, automatically generated from docstrings using Sphinx autodoc.

```{note}
All classes and functions include complete type hints and follow NumPy-style documentation standards. Cross-references link to related Earth System Model libraries like xarray, dask, and fsspec.
```

## Quick Reference

### Core Services
- {class}`~tellus.application.services.simulation_service.SimulationApplicationService` - Simulation management
- {class}`~tellus.application.services.location_service.LocationApplicationService` - Storage location management  
- {class}`~tellus.application.services.archive_service.ArchiveApplicationService` - Archive operations
- {class}`~tellus.application.services.file_transfer_service.FileTransferApplicationService` - File transfer operations
- {class}`~tellus.application.services.progress_tracking_service.ProgressTrackingApplicationService` - Progress tracking

### Domain Entities
- {class}`~tellus.domain.entities.simulation.SimulationEntity` - Simulation data model
- {class}`~tellus.domain.entities.location.LocationEntity` - Location data model
- {class}`~tellus.domain.entities.archive.ArchiveMetadata` - Archive metadata model
- {class}`~tellus.domain.entities.file_tracking.TrackedFile` - File tracking model

### Service Container
- {class}`~tellus.application.container.ServiceContainer` - Dependency injection container

## Application Layer

The application layer provides high-level orchestration of business operations through application services and DTOs (Data Transfer Objects).

```{toctree}
:maxdepth: 2

application/services
application/dtos
application/container
```

## Domain Layer  

The domain layer contains pure business logic, entities, and domain services without external dependencies.

```{toctree}
:maxdepth: 2

domain/entities
domain/services
domain/repositories
```

## Infrastructure Layer

The infrastructure layer implements technical concerns like data persistence, external service integration, and filesystem operations.

```{toctree}
:maxdepth: 2

infrastructure/repositories
infrastructure/adapters
infrastructure/services
```

## Interface Layer

The interface layer provides user-facing interfaces including CLI commands, TUI applications, and programmatic APIs.

```{toctree}
:maxdepth: 2

interfaces/cli
interfaces/tui
```

## Testing Utilities

Test fixtures, utilities, and plugins for testing Tellus applications and extensions.

```{toctree}
:maxdepth: 2

testing
```

## Type Definitions

Common type aliases and protocols used throughout Tellus:

```{eval-rst}
.. currentmodule:: tellus

.. autosummary::
   :toctree: generated/
   
   application.dtos
   domain.entities
```

### Path and Configuration Types

```python
from typing import Union, List, Dict, Optional, Protocol
from pathlib import Path

# Path types for filesystem operations
PathLike = Union[str, Path]
PathList = List[PathLike]

# Configuration and metadata types
ConfigDict = Dict[str, Any]
MetadataDict = Dict[str, Any]
AttrsDict = Dict[str, Any]

# Callback protocols for progress tracking
class ProgressCallback(Protocol):
    def __call__(self, current: int, total: int) -> None: ...

class ErrorCallback(Protocol):
    def __call__(self, error: Exception) -> None: ...
```

## Exception Hierarchy

Tellus uses a structured exception hierarchy for clear error handling:

```{eval-rst}
.. currentmodule:: tellus.application.exceptions

.. autosummary::
   :toctree: generated/
   
   TellusApplicationError
   ServiceNotFoundError
   ValidationError
   OperationError
```

### Repository Exceptions

```{eval-rst}
.. currentmodule:: tellus.domain.repositories.exceptions

.. autosummary::
   :toctree: generated/
   
   RepositoryError
   EntityNotFoundError
   DuplicateEntityError
   EntityValidationError
```

## Configuration Reference

### Service Container Configuration

The service container can be configured with custom repository implementations:

```python
from tellus.application.container import ServiceContainer
from tellus.infrastructure.repositories import JsonSimulationRepository

# Create container with custom repository
container = ServiceContainer()
container.register_simulation_repository(
    JsonSimulationRepository("custom_simulations.json")
)

# Use configured services
simulation_service = container.get_simulation_service()
```

### Environment Variables

Tellus recognizes several environment variables for configuration:

| Variable | Description | Default |
|----------|-------------|---------|
| `TELLUS_CONFIG_DIR` | Configuration directory | `~/.tellus` |
| `TELLUS_CACHE_DIR` | Cache directory | `~/.cache/tellus` |
| `TELLUS_LOG_LEVEL` | Logging level | `INFO` |
| `TELLUS_MAX_WORKERS` | Maximum concurrent operations | `4` |

## Integration Examples

### Earth System Model Integration

```python
from tellus.application.container import ServiceContainer
from tellus.application.dtos import CreateSimulationDto

# Initialize Tellus for climate model workflow
container = ServiceContainer()
sim_service = container.get_simulation_service()

# Register CESM2 simulation
cesm_sim = CreateSimulationDto(
    simulation_id="cesm2-historical-r1i1p1f1",
    model_id="CESM2.1",
    attrs={
        "experiment": "historical",
        "time_period": "1850-2014",
        "resolution": "f09_g17",
        "ensemble_member": "r1i1p1f1",
        "atmospheric_model": "CAM6",
        "ocean_model": "POP2",
        "variables": ["tas", "pr", "psl", "tos"]
    }
)

simulation = sim_service.create_simulation(cesm_sim)
```

### HPC Integration

```python
from tellus.application.dtos import CreateLocationDto
from tellus.domain.entities.location import LocationKind

# Configure HPC storage locations
hpc_scratch = CreateLocationDto(
    name="ncar-cheyenne-scratch",
    kinds=[LocationKind.COMPUTE],
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
        "max_nodes": 4608
    }
)

location_service = container.get_location_service()
hpc_location = location_service.create_location(hpc_scratch)
```

## Performance Considerations

### Memory Management

- Services use lazy loading and connection pooling
- Large file operations stream data to minimize memory usage
- Progress tracking uses efficient callback mechanisms

### Concurrency

- File transfer operations support configurable concurrency limits
- Archive operations use async/await for non-blocking I/O
- Progress tracking is thread-safe for concurrent operations

### Caching

- Location filesystem objects are cached for performance
- Archive metadata is cached to avoid repeated extraction
- File classification results are cached for efficiency

## See Also

- {doc}`../user-guide/index` - Comprehensive user guide with examples
- {doc}`../tutorials/index` - Step-by-step tutorials for common workflows  
- {doc}`../examples/index` - Real-world usage patterns and case studies
- {doc}`../development/index` - Development guide for contributors