# Domain Repositories

Domain repositories define abstract interfaces for data persistence, following the Repository pattern to decouple business logic from data storage concerns.

## Repository Interfaces

### Simulation Repository

```{eval-rst}
.. currentmodule:: tellus.domain.repositories.simulation_repository

.. autoclass:: SimulationRepository
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__
```

**Key Operations:**
- `save()` - Persist simulation entity
- `find_by_id()` - Retrieve by simulation ID
- `find_all()` - List all simulations
- `delete()` - Remove simulation record
- `exists()` - Check if simulation exists

### Location Repository

```{eval-rst}
.. currentmodule:: tellus.domain.repositories.location_repository

.. autoclass:: LocationRepository
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__
```

**Key Operations:**
- `save()` - Persist location entity
- `find_by_name()` - Retrieve by location name
- `find_all()` - List all locations
- `find_by_kind()` - Find locations by kind
- `delete()` - Remove location record

### Archive Repository

```{eval-rst}
.. currentmodule:: tellus.domain.repositories.archive_repository

.. autoclass:: ArchiveRepository
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__
```

**Key Operations:**
- `save()` - Persist archive metadata
- `find_by_id()` - Retrieve by archive ID
- `find_all()` - List all archives
- `find_by_simulation()` - Find archives for simulation
- `find_by_location()` - Find archives at location

### File Tracking Repository

```{eval-rst}
.. currentmodule:: tellus.domain.repositories.file_tracking_repository

.. autoclass:: FileTrackingRepository
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__
```

**Key Operations:**
- `save()` - Persist tracked file
- `find_by_path()` - Retrieve by file path
- `find_all()` - List all tracked files
- `find_by_status()` - Find files by status
- `find_modified_since()` - Find recently modified files

### Progress Tracking Repository

```{eval-rst}
.. currentmodule:: tellus.domain.repositories.progress_tracking_repository

.. autoclass:: ProgressTrackingRepository
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__
```

**Key Operations:**
- `save()` - Persist progress operation
- `find_by_id()` - Retrieve by operation ID
- `find_all()` - List all operations
- `find_by_status()` - Find operations by status
- `find_active()` - Find running operations

## Repository Exceptions

```{eval-rst}
.. currentmodule:: tellus.domain.repositories.exceptions

.. autoclass:: RepositoryError
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__

.. autoclass:: EntityNotFoundError
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__

.. autoclass:: DuplicateEntityError
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__

.. autoclass:: EntityValidationError
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__
```

## Usage Examples

### Basic Repository Operations

```python
from tellus.domain.repositories.simulation_repository import SimulationRepository
from tellus.domain.entities.simulation import SimulationEntity
from tellus.domain.repositories.exceptions import EntityNotFoundError

# Assuming we have a concrete implementation
simulation_repo: SimulationRepository = get_simulation_repository()

# Create and save simulation
simulation = SimulationEntity(
    simulation_id="cesm2-historical-r1i1p1f1",
    model_id="CESM2.1",
    attrs={
        "experiment": "historical",
        "time_period": "1850-2014"
    }
)

simulation_repo.save(simulation)

# Retrieve simulation
try:
    retrieved = simulation_repo.find_by_id("cesm2-historical-r1i1p1f1")
    print(f"Found simulation: {retrieved.simulation_id}")
except EntityNotFoundError:
    print("Simulation not found")

# List all simulations
all_simulations = simulation_repo.find_all()
print(f"Total simulations: {len(all_simulations)}")

# Check existence
exists = simulation_repo.exists("cesm2-historical-r1i1p1f1")
print(f"Simulation exists: {exists}")
```

### Location Repository with Filtering

```python
from tellus.domain.repositories.location_repository import LocationRepository
from tellus.domain.entities.location import LocationEntity, LocationKind

location_repo: LocationRepository = get_location_repository()

# Create different types of locations
locations = [
    LocationEntity(
        name="local-workspace",
        kinds=[LocationKind.DISK],
        protocol="file",
        config={"path": "/home/user/data"}
    ),
    LocationEntity(
        name="hpc-scratch",
        kinds=[LocationKind.COMPUTE],
        protocol="ssh",
        config={"host": "hpc.edu", "path": "/scratch"}
    ),
    LocationEntity(
        name="cloud-archive",
        kinds=[LocationKind.FILESERVER],
        protocol="s3",
        config={"bucket": "climate-data"}
    )
]

# Save all locations
for location in locations:
    location_repo.save(location)

# Find locations by kind
compute_locations = location_repo.find_by_kind(LocationKind.COMPUTE)
print(f"Compute locations: {[loc.name for loc in compute_locations]}")

disk_locations = location_repo.find_by_kind(LocationKind.DISK)
print(f"Disk locations: {[loc.name for loc in disk_locations]}")

# Find location by name
hpc_location = location_repo.find_by_name("hpc-scratch")
print(f"HPC location protocol: {hpc_location.protocol}")
```

### Archive Repository with Queries

```python
from tellus.domain.repositories.archive_repository import ArchiveRepository
from tellus.domain.entities.archive import ArchiveMetadata, ArchiveType

archive_repo: ArchiveRepository = get_archive_repository()

# Create archive metadata
archive1 = ArchiveMetadata(
    archive_id="cesm2-output-v1",
    location="cloud-archive",
    archive_type=ArchiveType.COMPRESSED,
    simulation_id="cesm2-historical-r1i1p1f1",
    description="CESM2 model output"
)

archive2 = ArchiveMetadata(
    archive_id="cesm2-processed-v1", 
    location="cloud-archive",
    archive_type=ArchiveType.COMPRESSED,
    simulation_id="cesm2-historical-r1i1p1f1",
    description="Processed analysis results"
)

# Save archives
archive_repo.save(archive1)
archive_repo.save(archive2)

# Find archives for a simulation
sim_archives = archive_repo.find_by_simulation("cesm2-historical-r1i1p1f1")
print(f"Archives for simulation: {[a.archive_id for a in sim_archives]}")

# Find archives at a location
location_archives = archive_repo.find_by_location("cloud-archive")
print(f"Archives at location: {[a.archive_id for a in location_archives]}")

# Find archive by ID
specific_archive = archive_repo.find_by_id("cesm2-output-v1")
print(f"Archive type: {specific_archive.archive_type}")
```

### File Tracking Repository

```python
from tellus.domain.repositories.file_tracking_repository import FileTrackingRepository
from tellus.domain.entities.file_tracking import TrackedFile, FileStatus
from tellus.domain.entities.file_type_config import FileContentType, FileRole
from datetime import datetime, timedelta

file_repo: FileTrackingRepository = get_file_tracking_repository()

# Create tracked files
files = [
    TrackedFile(
        file_path="/data/cesm2/tas_monthly.nc",
        content_type=FileContentType.MODEL_OUTPUT,
        file_role=FileRole.PRIMARY,
        status=FileStatus.TRACKED,
        metadata={"variable": "tas", "frequency": "monthly"}
    ),
    TrackedFile(
        file_path="/data/cesm2/processing.log",
        content_type=FileContentType.LOG,
        file_role=FileRole.AUXILIARY,
        status=FileStatus.TRACKED,
        metadata={"log_type": "processing"}
    )
]

# Save tracked files
for file in files:
    file_repo.save(file)

# Find files by status
tracked_files = file_repo.find_by_status(FileStatus.TRACKED)
print(f"Tracked files: {len(tracked_files)}")

# Find files modified in last 24 hours
since = datetime.now() - timedelta(hours=24)
recent_files = file_repo.find_modified_since(since)
print(f"Recently modified: {len(recent_files)}")

# Find specific file
tas_file = file_repo.find_by_path("/data/cesm2/tas_monthly.nc")
print(f"File content type: {tas_file.content_type}")
```

### Progress Tracking Repository

```python
from tellus.domain.repositories.progress_tracking_repository import ProgressTrackingRepository
from tellus.domain.entities.progress_tracking import ProgressOperation, OperationStatus
from datetime import datetime, timedelta

progress_repo: ProgressTrackingRepository = get_progress_tracking_repository()

# Create progress operations
operations = [
    ProgressOperation(
        operation_id="cesm2-processing-001",
        operation_type="data_processing",
        description="Processing CESM2 output",
        status=OperationStatus.RUNNING,
        estimated_duration=timedelta(hours=2)
    ),
    ProgressOperation(
        operation_id="archive-extraction-001",
        operation_type="archive_extraction",
        description="Extracting climate data",
        status=OperationStatus.COMPLETED,
        estimated_duration=timedelta(minutes=30)
    )
]

# Save operations
for op in operations:
    progress_repo.save(op)

# Find active operations
active_ops = progress_repo.find_active()
print(f"Active operations: {[op.operation_id for op in active_ops]}")

# Find operations by status
running_ops = progress_repo.find_by_status(OperationStatus.RUNNING)
completed_ops = progress_repo.find_by_status(OperationStatus.COMPLETED)

print(f"Running: {len(running_ops)}, Completed: {len(completed_ops)}")

# Find specific operation
processing_op = progress_repo.find_by_id("cesm2-processing-001")
print(f"Operation progress: {processing_op.progress_percentage}%")
```

## Repository Implementation Patterns

### Transaction Support

```python
from abc import abstractmethod
from typing import Protocol, ContextManager

class TransactionalRepository(Protocol):
    """Repository with transaction support."""
    
    @abstractmethod
    def begin_transaction(self) -> ContextManager:
        """Begin a transaction context."""
        pass

# Usage example
class AtomicSimulationOperations:
    def __init__(self, simulation_repo: TransactionalRepository):
        self.simulation_repo = simulation_repo
    
    def update_multiple_simulations(self, updates: dict):
        """Update multiple simulations atomically."""
        with self.simulation_repo.begin_transaction():
            for sim_id, attrs in updates.items():
                simulation = self.simulation_repo.find_by_id(sim_id)
                simulation.attrs.update(attrs)
                self.simulation_repo.save(simulation)
            # Transaction commits automatically on successful exit
            # Rolls back on exception
```

### Caching Repository Decorator

```python
from typing import Optional
from functools import wraps

class CachingRepositoryDecorator:
    """Add caching to any repository."""
    
    def __init__(self, repository, cache_ttl: int = 300):
        self.repository = repository
        self.cache = {}
        self.cache_ttl = cache_ttl
    
    def find_by_id(self, entity_id: str):
        # Check cache first
        cache_key = f"find_by_id:{entity_id}"
        if cache_key in self.cache:
            cached_time, entity = self.cache[cache_key]
            if datetime.now() - cached_time < timedelta(seconds=self.cache_ttl):
                return entity
        
        # Cache miss - fetch from repository
        entity = self.repository.find_by_id(entity_id)
        self.cache[cache_key] = (datetime.now(), entity)
        return entity
    
    def save(self, entity):
        # Save to repository
        result = self.repository.save(entity)
        
        # Invalidate cache for this entity
        cache_key = f"find_by_id:{entity.get_id()}"
        if cache_key in self.cache:
            del self.cache[cache_key]
        
        return result

# Usage
base_repo = JsonSimulationRepository("simulations.json")
cached_repo = CachingRepositoryDecorator(base_repo, cache_ttl=600)
```

### Repository Factory Pattern

```python
from abc import ABC, abstractmethod
from typing import Protocol

class RepositoryFactory(ABC):
    """Abstract factory for creating repositories."""
    
    @abstractmethod
    def create_simulation_repository(self) -> SimulationRepository:
        pass
    
    @abstractmethod
    def create_location_repository(self) -> LocationRepository:
        pass
    
    @abstractmethod
    def create_archive_repository(self) -> ArchiveRepository:
        pass

class JsonRepositoryFactory(RepositoryFactory):
    """Factory for JSON-based repositories."""
    
    def __init__(self, data_dir: str):
        self.data_dir = data_dir
    
    def create_simulation_repository(self) -> SimulationRepository:
        return JsonSimulationRepository(f"{self.data_dir}/simulations.json")
    
    def create_location_repository(self) -> LocationRepository:
        return JsonLocationRepository(f"{self.data_dir}/locations.json")
    
    def create_archive_repository(self) -> ArchiveRepository:
        return JsonArchiveRepository(f"{self.data_dir}/archives.json")

class DatabaseRepositoryFactory(RepositoryFactory):
    """Factory for database-based repositories."""
    
    def __init__(self, connection_string: str):
        self.connection_string = connection_string
    
    def create_simulation_repository(self) -> SimulationRepository:
        return SqlSimulationRepository(self.connection_string)
    
    def create_location_repository(self) -> LocationRepository:
        return SqlLocationRepository(self.connection_string)
    
    def create_archive_repository(self) -> ArchiveRepository:
        return SqlArchiveRepository(self.connection_string)

# Usage
# For development/testing
json_factory = JsonRepositoryFactory("./data")
simulation_repo = json_factory.create_simulation_repository()

# For production
db_factory = DatabaseRepositoryFactory("postgresql://...")
production_repo = db_factory.create_simulation_repository()
```

### Query Builder Pattern

```python
from typing import List, Optional, Any
from dataclasses import dataclass

@dataclass
class QueryFilter:
    field: str
    operator: str  # 'eq', 'ne', 'in', 'contains', etc.
    value: Any

class RepositoryQuery:
    """Build complex queries for repositories."""
    
    def __init__(self):
        self.filters: List[QueryFilter] = []
        self.order_by: Optional[str] = None
        self.limit: Optional[int] = None
        self.offset: Optional[int] = None
    
    def filter_by(self, field: str, value: Any, operator: str = 'eq'):
        self.filters.append(QueryFilter(field, operator, value))
        return self
    
    def order(self, field: str, ascending: bool = True):
        self.order_by = field if ascending else f"-{field}"
        return self
    
    def limit_to(self, count: int):
        self.limit = count
        return self
    
    def skip(self, count: int):
        self.offset = count
        return self

# Enhanced repository interface with query support
class QueryableSimulationRepository(SimulationRepository):
    @abstractmethod
    def find_by_query(self, query: RepositoryQuery) -> List[SimulationEntity]:
        pass

# Usage examples
query = (RepositoryQuery()
         .filter_by("attrs.experiment", "historical")
         .filter_by("model_id", ["CESM2.1", "GFDL-CM4"], operator='in')
         .order("simulation_id")
         .limit_to(10))

historical_simulations = simulation_repo.find_by_query(query)
```

## Error Handling Best Practices

```python
from tellus.domain.repositories.exceptions import (
    RepositoryError, EntityNotFoundError, DuplicateEntityError
)

def robust_repository_operations():
    """Demonstrate robust error handling with repositories."""
    
    simulation_repo = get_simulation_repository()
    
    # Handle not found errors
    try:
        simulation = simulation_repo.find_by_id("nonexistent")
    except EntityNotFoundError:
        print("Simulation not found - this is expected")
        simulation = None
    
    # Handle duplicate errors
    try:
        duplicate_sim = SimulationEntity("existing-id", "CESM2.1")
        simulation_repo.save(duplicate_sim)
    except DuplicateEntityError:
        print("Simulation already exists - update instead")
        existing = simulation_repo.find_by_id("existing-id")
        existing.attrs.update(duplicate_sim.attrs)
        simulation_repo.save(existing)
    
    # Handle general repository errors
    try:
        simulations = simulation_repo.find_all()
    except RepositoryError as e:
        print(f"Repository error: {e}")
        # Implement fallback strategy
        simulations = []
```

## Performance Considerations

- Repository interfaces support pagination for large datasets
- Bulk operations minimize database round-trips
- Lazy loading prevents unnecessary data fetching
- Query optimization through proper indexing
- Connection pooling for database repositories
- Async repository implementations for high-throughput scenarios