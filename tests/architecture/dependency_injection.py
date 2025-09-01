"""
Dependency injection container for test environment.

This module provides a dependency injection container that manages
the creation and lifecycle of test dependencies, following SOLID principles.
"""

import tempfile
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Callable, Dict, Optional, Protocol, Type, TypeVar

from .cache import CacheFactory, InMemoryCache
from .configuration import SimpleConfiguration, TestConfiguration
from .filesystem import FileSystemFactory, InMemoryFileSystem
from .interfaces import (ArchiveHandler, CacheInterface,
                         ConfigurationInterface, EventPublisher,
                         FileSystemInterface, LocationRepository,
                         LoggerInterface, NetworkInterface, ProgressTracker,
                         SimulationRepository, TestDataProvider,
                         TestEnvironment)
from .network import FakeNetwork, NetworkFactory

T = TypeVar('T')


class ServiceFactory(Protocol):
    """Protocol for service factories."""
    def __call__(self, container: 'TestContainer') -> Any:
        ...


class TestContainer:
    """
    Dependency injection container for test environment.
    
    Manages the creation and lifecycle of test dependencies,
    providing proper isolation and configuration.
    """
    
    def __init__(self):
        """Initialize the test container."""
        self._singletons: Dict[Type, Any] = {}
        self._factories: Dict[Type, ServiceFactory] = {}
        self._configuration: Optional[ConfigurationInterface] = None
        self._test_environment: Optional[TestEnvironment] = None
    
    def register_singleton(self, interface: Type[T], instance: T) -> None:
        """Register a singleton instance."""
        self._singletons[interface] = instance
    
    def register_factory(self, interface: Type[T], factory: ServiceFactory) -> None:
        """Register a factory for creating instances."""
        self._factories[interface] = factory
    
    def get(self, interface: Type[T]) -> T:
        """Get an instance of the specified interface."""
        # Check singletons first
        if interface in self._singletons:
            return self._singletons[interface]
        
        # Check factories
        if interface in self._factories:
            instance = self._factories[interface](self)
            return instance
        
        # Try to create default implementation
        if hasattr(self, f'_create_default_{interface.__name__.lower()}'):
            factory_method = getattr(self, f'_create_default_{interface.__name__.lower()}')
            instance = factory_method()
            self._singletons[interface] = instance  # Cache as singleton
            return instance
        
        raise ValueError(f"No registration found for {interface}")
    
    def register_configuration(self, config: TestConfiguration) -> None:
        """Register test configuration."""
        if isinstance(config, ConfigurationInterface):
            self._configuration = config
        else:
            # Wrap in simple configuration
            config_dict = config.to_dict() if hasattr(config, 'to_dict') else {}
            self._configuration = SimpleConfiguration(config_dict)
    
    def get_configuration(self) -> ConfigurationInterface:
        """Get configuration interface."""
        if not self._configuration:
            self._configuration = SimpleConfiguration()
        return self._configuration
    
    def register_filesystem(self, filesystem: FileSystemInterface) -> None:
        """Register filesystem implementation."""
        self.register_singleton(FileSystemInterface, filesystem)
    
    def get_filesystem(self) -> FileSystemInterface:
        """Get filesystem interface."""
        return self.get(FileSystemInterface)
    
    def register_network(self, network: NetworkInterface) -> None:
        """Register network implementation."""
        self.register_singleton(NetworkInterface, network)
    
    def get_network(self) -> NetworkInterface:
        """Get network interface."""
        return self.get(NetworkInterface)
    
    def register_cache(self, cache: CacheInterface) -> None:
        """Register cache implementation."""
        self.register_singleton(CacheInterface, cache)
    
    def get_cache(self) -> CacheInterface:
        """Get cache interface."""
        return self.get(CacheInterface)
    
    def register_location_repository(self, repository: LocationRepository) -> None:
        """Register location repository."""
        self.register_singleton(LocationRepository, repository)
    
    def get_location_repository(self) -> LocationRepository:
        """Get location repository."""
        return self.get(LocationRepository)
    
    def register_simulation_repository(self, repository: SimulationRepository) -> None:
        """Register simulation repository."""
        self.register_singleton(SimulationRepository, repository)
    
    def get_simulation_repository(self) -> SimulationRepository:
        """Get simulation repository."""
        return self.get(SimulationRepository)
    
    def register_test_environment(self, environment: TestEnvironment) -> None:
        """Register test environment."""
        self._test_environment = environment
    
    def get_test_environment(self) -> TestEnvironment:
        """Get test environment."""
        if not self._test_environment:
            self._test_environment = DefaultTestEnvironment(self)
        return self._test_environment
    
    def clear(self) -> None:
        """Clear all registrations."""
        self._singletons.clear()
        self._factories.clear()
        self._configuration = None
        self._test_environment = None
    
    # Default factory methods
    def _create_default_filesysteminterface(self) -> FileSystemInterface:
        """Create default filesystem implementation."""
        return FileSystemFactory.create_in_memory()
    
    def _create_default_networkinterface(self) -> NetworkInterface:
        """Create default network implementation."""
        return NetworkFactory.create_fake()
    
    def _create_default_cacheinterface(self) -> CacheInterface:
        """Create default cache implementation."""
        return CacheFactory.create_in_memory()
    
    def _create_default_locationrepository(self) -> LocationRepository:
        """Create default location repository."""
        return InMemoryLocationRepository()
    
    def _create_default_simulationrepository(self) -> SimulationRepository:
        """Create default simulation repository."""
        return InMemorySimulationRepository()


class DefaultTestEnvironment:
    """Default test environment implementation."""
    
    def __init__(self, container: TestContainer):
        """Initialize with container."""
        self._container = container
        self._temp_dir: Optional[Path] = None
        self._setup_complete = False
    
    def setup(self) -> None:
        """Setup test environment."""
        if self._setup_complete:
            return
        
        # Create temporary directory
        self._temp_dir = Path(tempfile.mkdtemp(prefix="tellus_test_"))
        
        # Setup logging if configured
        config = self._container.get_configuration()
        if config.get("logging.capture_output", True):
            self._setup_logging_capture()
        
        self._setup_complete = True
    
    def teardown(self) -> None:
        """Teardown test environment."""
        if not self._setup_complete:
            return
        
        # Clean up temporary directory
        if self._temp_dir and self._temp_dir.exists():
            import shutil
            shutil.rmtree(self._temp_dir, ignore_errors=True)
        
        self._setup_complete = False
    
    def reset(self) -> None:
        """Reset environment to clean state."""
        # Clear caches
        try:
            cache = self._container.get_cache()
            cache.clear()
        except:
            pass
        
        # Clear repositories
        try:
            location_repo = self._container.get_location_repository()
            location_repo.clear()
        except:
            pass
        
        try:
            simulation_repo = self._container.get_simulation_repository()
            simulation_repo.clear()
        except:
            pass
    
    def get_temp_dir(self) -> Path:
        """Get temporary directory for test."""
        if not self._temp_dir:
            raise RuntimeError("Test environment not set up. Call setup() first.")
        return self._temp_dir
    
    def get_config(self) -> ConfigurationInterface:
        """Get test configuration."""
        return self._container.get_configuration()
    
    def _setup_logging_capture(self) -> None:
        """Set up logging capture."""
        # This would set up logging capture based on configuration
        pass


class InMemoryLocationRepository(LocationRepository):
    """In-memory location repository for testing."""
    
    def __init__(self):
        """Initialize empty repository."""
        self._locations: Dict[str, Any] = {}
    
    def save(self, location: Any) -> None:
        """Save a location entity."""
        if hasattr(location, 'name'):
            self._locations[location.name] = location
        else:
            raise ValueError("Location must have a 'name' attribute")
    
    def find_by_name(self, name: str) -> Optional[Any]:
        """Find location by name."""
        return self._locations.get(name)
    
    def find_all(self) -> list[Any]:
        """Find all locations."""
        return list(self._locations.values())
    
    def delete(self, name: str) -> bool:
        """Delete location by name."""
        if name in self._locations:
            del self._locations[name]
            return True
        return False
    
    def exists(self, name: str) -> bool:
        """Check if location exists."""
        return name in self._locations
    
    def clear(self) -> None:
        """Clear all locations."""
        self._locations.clear()


class InMemorySimulationRepository(SimulationRepository):
    """In-memory simulation repository for testing."""
    
    def __init__(self):
        """Initialize empty repository."""
        self._simulations: Dict[str, Any] = {}
    
    def save(self, simulation: Any) -> None:
        """Save a simulation entity."""
        if hasattr(simulation, 'simulation_id'):
            self._simulations[simulation.simulation_id] = simulation
        else:
            raise ValueError("Simulation must have a 'simulation_id' attribute")
    
    def find_by_id(self, simulation_id: str) -> Optional[Any]:
        """Find simulation by ID."""
        return self._simulations.get(simulation_id)
    
    def find_all(self) -> list[Any]:
        """Find all simulations."""
        return list(self._simulations.values())
    
    def delete(self, simulation_id: str) -> bool:
        """Delete simulation by ID."""
        if simulation_id in self._simulations:
            del self._simulations[simulation_id]
            return True
        return False
    
    def exists(self, simulation_id: str) -> bool:
        """Check if simulation exists."""
        return simulation_id in self._simulations
    
    def clear(self) -> None:
        """Clear all simulations."""
        self._simulations.clear()


class TestContainerBuilder:
    """Builder for creating configured test containers."""
    
    def __init__(self):
        """Initialize builder."""
        self._container = TestContainer()
    
    def with_configuration(self, config: TestConfiguration) -> 'TestContainerBuilder':
        """Add configuration."""
        self._container.register_configuration(config)
        return self
    
    def with_in_memory_filesystem(self) -> 'TestContainerBuilder':
        """Add in-memory filesystem."""
        self._container.register_filesystem(FileSystemFactory.create_in_memory())
        return self
    
    def with_real_filesystem(self) -> 'TestContainerBuilder':
        """Add real filesystem."""
        self._container.register_filesystem(FileSystemFactory.create_real())
        return self
    
    def with_fake_network(self) -> 'TestContainerBuilder':
        """Add fake network."""
        self._container.register_network(NetworkFactory.create_fake())
        return self
    
    def with_mock_network(self) -> 'TestContainerBuilder':
        """Add mock network."""
        self._container.register_network(NetworkFactory.create_mock())
        return self
    
    def with_in_memory_cache(self, max_size: Optional[int] = None) -> 'TestContainerBuilder':
        """Add in-memory cache."""
        self._container.register_cache(CacheFactory.create_in_memory(max_size))
        return self
    
    def with_file_cache(self, cache_dir: Path, max_size: Optional[int] = None) -> 'TestContainerBuilder':
        """Add file cache."""
        self._container.register_cache(CacheFactory.create_file_cache(cache_dir, max_size))
        return self
    
    def with_in_memory_repositories(self) -> 'TestContainerBuilder':
        """Add in-memory repositories."""
        self._container.register_location_repository(InMemoryLocationRepository())
        self._container.register_simulation_repository(InMemorySimulationRepository())
        return self
    
    def build(self) -> TestContainer:
        """Build the configured container."""
        return self._container


class ContainerFactory:
    """Factory for creating pre-configured containers for different test types."""
    
    @staticmethod
    def create_unit_test_container(config: Optional[TestConfiguration] = None) -> TestContainer:
        """Create container configured for unit tests."""
        if not config:
            from .configuration import TestConfigurationProvider
            config = TestConfigurationProvider.get_unit_test_config()
        
        return (TestContainerBuilder()
                .with_configuration(config)
                .with_in_memory_filesystem()
                .with_mock_network()
                .with_in_memory_cache(1024 * 1024)  # 1MB
                .with_in_memory_repositories()
                .build())
    
    @staticmethod
    def create_integration_test_container(config: Optional[TestConfiguration] = None) -> TestContainer:
        """Create container configured for integration tests."""
        if not config:
            from .configuration import TestConfigurationProvider
            config = TestConfigurationProvider.get_integration_test_config()
        
        temp_dir = Path(tempfile.mkdtemp(prefix="tellus_integration_"))
        
        return (TestContainerBuilder()
                .with_configuration(config)
                .with_in_memory_filesystem()  # Use in-memory for faster tests
                .with_fake_network()
                .with_file_cache(temp_dir / "cache", 10 * 1024 * 1024)  # 10MB
                .with_in_memory_repositories()
                .build())
    
    @staticmethod
    def create_e2e_test_container(config: Optional[TestConfiguration] = None) -> TestContainer:
        """Create container configured for end-to-end tests."""
        if not config:
            from .configuration import TestConfigurationProvider
            config = TestConfigurationProvider.get_e2e_test_config()
        
        temp_dir = Path(tempfile.mkdtemp(prefix="tellus_e2e_"))
        
        return (TestContainerBuilder()
                .with_configuration(config)
                .with_real_filesystem()
                .with_fake_network()  # Still use fake for controlled testing
                .with_file_cache(temp_dir / "cache", 100 * 1024 * 1024)  # 100MB
                .with_in_memory_repositories()
                .build())
    
    @staticmethod
    def create_performance_test_container(config: Optional[TestConfiguration] = None) -> TestContainer:
        """Create container configured for performance tests."""
        if not config:
            from .configuration import TestConfigurationProvider
            config = TestConfigurationProvider.get_performance_test_config()
        
        return (TestContainerBuilder()
                .with_configuration(config)
                .with_in_memory_filesystem()
                .with_fake_network()
                .with_in_memory_cache(500 * 1024 * 1024)  # 500MB
                .with_in_memory_repositories()
                .build())