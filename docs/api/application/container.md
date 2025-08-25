# Service Container

The service container provides dependency injection and service lifecycle management for the Tellus application layer.

## ServiceContainer Class

```{eval-rst}
.. currentmodule:: tellus.application.container

.. autoclass:: ServiceContainer
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__
```

## Usage Patterns

### Basic Service Access

```python
from tellus.application.container import ServiceContainer

# Create container instance
container = ServiceContainer()

# Access application services
simulation_service = container.get_simulation_service()
location_service = container.get_location_service()
archive_service = container.get_archive_service()
transfer_service = container.get_file_transfer_service()
progress_service = container.get_progress_tracking_service()
```

### Custom Repository Registration

```python
from tellus.infrastructure.repositories import (
    JsonSimulationRepository,
    JsonLocationRepository,
    JsonArchiveRepository
)

# Create container with custom repositories
container = ServiceContainer()

# Register custom repositories
container.register_simulation_repository(
    JsonSimulationRepository("custom_simulations.json")
)
container.register_location_repository(
    JsonLocationRepository("custom_locations.json")
)
container.register_archive_repository(
    JsonArchiveRepository("custom_archives.json")
)

# Services will use custom repositories
simulation_service = container.get_simulation_service()
```

### Environment-Specific Configuration

```python
import os
from pathlib import Path

# Configure container for different environments
config_dir = Path(os.environ.get("TELLUS_CONFIG_DIR", "~/.tellus")).expanduser()

# Production configuration
if os.environ.get("TELLUS_ENV") == "production":
    container = ServiceContainer()
    container.register_simulation_repository(
        JsonSimulationRepository(config_dir / "prod_simulations.json")
    )
    container.register_location_repository(
        JsonLocationRepository(config_dir / "prod_locations.json")
    )

# Development configuration
elif os.environ.get("TELLUS_ENV") == "development":
    container = ServiceContainer()
    container.register_simulation_repository(
        JsonSimulationRepository(config_dir / "dev_simulations.json")
    )
    container.register_location_repository(
        JsonLocationRepository(config_dir / "dev_locations.json")
    )

# Testing configuration
else:
    container = ServiceContainer()
    # Use in-memory repositories for testing
    container.register_simulation_repository(
        JsonSimulationRepository(":memory:")
    )
```

### Service Factory Integration

```python
from tellus.application.service_factory import ServiceFactory

# Use service factory for advanced configuration
factory = ServiceFactory()
container = factory.create_container(
    simulation_repo_path="simulations.json",
    location_repo_path="locations.json", 
    archive_repo_path="archives.json",
    enable_caching=True,
    max_workers=8
)

# Container is configured and ready
services = {
    'simulation': container.get_simulation_service(),
    'location': container.get_location_service(),
    'archive': container.get_archive_service(),
    'transfer': container.get_file_transfer_service(),
    'progress': container.get_progress_tracking_service()
}
```

## Singleton Behavior

The service container ensures services are singletons within the container instance:

```python
container = ServiceContainer()

# Multiple calls return the same instance
service1 = container.get_simulation_service()
service2 = container.get_simulation_service()

assert service1 is service2  # Same instance
```

## Thread Safety

The service container is thread-safe and can be used across multiple threads:

```python
import threading
from tellus.application.container import ServiceContainer

# Shared container across threads
container = ServiceContainer()

def worker_thread():
    # Safe to access services from multiple threads
    simulation_service = container.get_simulation_service()
    simulations = simulation_service.list_simulations()
    
    # Each thread gets the same service instance
    # But service methods handle concurrent access safely

# Start multiple worker threads
threads = []
for i in range(4):
    thread = threading.Thread(target=worker_thread)
    threads.append(thread)
    thread.start()

# Wait for all threads to complete
for thread in threads:
    thread.join()
```

## Service Lifecycle

Services have defined lifecycle hooks for initialization and cleanup:

```python
class CustomServiceContainer(ServiceContainer):
    def __init__(self):
        super().__init__()
        self._initialized = False
    
    def initialize(self):
        """Initialize all services."""
        if not self._initialized:
            # Pre-load services to trigger initialization
            self.get_simulation_service()
            self.get_location_service()
            self.get_archive_service()
            self.get_file_transfer_service()
            self.get_progress_tracking_service()
            
            self._initialized = True
    
    def cleanup(self):
        """Cleanup all services."""
        # Services handle their own cleanup
        # Repository connections are closed
        # Background tasks are cancelled
        pass

# Usage with lifecycle management
container = CustomServiceContainer()
try:
    container.initialize()
    
    # Use services
    simulation_service = container.get_simulation_service()
    # ... application logic
    
finally:
    container.cleanup()
```

## Extension Points

The container supports extension through custom service registration:

```python
from tellus.application.services import FileTrackingApplicationService
from tellus.infrastructure.repositories import JsonFileTrackingRepository

# Extend container with additional services
class ExtendedServiceContainer(ServiceContainer):
    def __init__(self):
        super().__init__()
        self._file_tracking_service = None
    
    def get_file_tracking_service(self) -> FileTrackingApplicationService:
        if self._file_tracking_service is None:
            # Create custom repository
            repo = JsonFileTrackingRepository("file_tracking.json")
            
            # Create service with dependencies
            self._file_tracking_service = FileTrackingApplicationService(
                file_tracking_repository=repo,
                progress_service=self.get_progress_tracking_service()
            )
        
        return self._file_tracking_service

# Use extended container
container = ExtendedServiceContainer()
file_tracking_service = container.get_file_tracking_service()
```

## Testing Support

The container provides testing utilities:

```python
from tellus.testing.fixtures import create_test_container

# Create container for testing
test_container = create_test_container(
    use_memory_repositories=True,
    populate_test_data=True
)

# Test services have pre-loaded test data
simulation_service = test_container.get_simulation_service()
simulations = simulation_service.list_simulations()
assert len(simulations.simulations) > 0  # Test data available
```

## Performance Considerations

- Services are lazily instantiated on first access
- Repository connections are pooled and reused
- Service instances are cached as singletons
- Thread-safe access with minimal locking overhead
- Memory usage scales with active service count

## Error Handling

```python
from tellus.application.exceptions import ServiceNotFoundError

container = ServiceContainer()

try:
    # This would raise ServiceNotFoundError if service not available
    unknown_service = container.get_unknown_service()
except ServiceNotFoundError as e:
    print(f"Service not available: {e}")

# Services handle their own exceptions internally
simulation_service = container.get_simulation_service()
# Service methods raise appropriate application exceptions
```