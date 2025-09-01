"""
Base test classes following Clean Architecture principles.

These classes provide the foundation for different types of tests,
with proper separation of concerns and dependency injection.
"""

import shutil
import tempfile
import unittest
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict, List, Optional, Type
from unittest.mock import Mock, patch

from .cache import CacheFactory
from .configuration import (ConfigurationManager, TestConfiguration,
                            TestEnvironmentType)
from .dependency_injection import TestContainer
from .filesystem import FileSystemFactory
from .interfaces import (CacheInterface, ConfigurationInterface,
                         FileSystemInterface, NetworkInterface,
                         TestDataProvider, TestEnvironment)
from .network import NetworkFactory


class BaseTest(unittest.TestCase, ABC):
    """
    Abstract base class for all tests following Clean Architecture principles.
    
    Provides:
    - Dependency injection setup
    - Common test utilities
    - Environment isolation
    - Proper cleanup
    """
    
    def __init__(self, methodName: str = "runTest"):
        """Initialize base test."""
        super().__init__(methodName)
        self._container: Optional[TestContainer] = None
        self._test_environment: Optional[TestEnvironment] = None
        self._temp_paths: List[Path] = []
    
    def setUp(self) -> None:
        """Set up test environment with dependency injection."""
        super().setUp()
        
        # Create dependency injection container
        self._container = TestContainer()
        
        # Configure container for this test type
        self._configure_container()
        
        # Set up test environment
        self._test_environment = self._container.get_test_environment()
        self._test_environment.setup()
        
        # Perform test-specific setup
        self._setup_test_specific()
    
    def tearDown(self) -> None:
        """Clean up test environment."""
        try:
            # Perform test-specific cleanup
            self._cleanup_test_specific()
            
            # Clean up test environment
            if self._test_environment:
                self._test_environment.teardown()
            
            # Clean up temporary paths
            self._cleanup_temp_paths()
            
        finally:
            super().tearDown()
    
    @abstractmethod
    def _configure_container(self) -> None:
        """Configure the dependency injection container for this test type."""
        pass
    
    def _setup_test_specific(self) -> None:
        """Override for test-specific setup logic."""
        pass
    
    def _cleanup_test_specific(self) -> None:
        """Override for test-specific cleanup logic."""
        pass
    
    def get_container(self) -> TestContainer:
        """Get the dependency injection container."""
        if not self._container:
            raise RuntimeError("Container not initialized. Call setUp() first.")
        return self._container
    
    def get_filesystem(self) -> FileSystemInterface:
        """Get filesystem interface from container."""
        return self.get_container().get_filesystem()
    
    def get_network(self) -> NetworkInterface:
        """Get network interface from container."""
        return self.get_container().get_network()
    
    def get_cache(self) -> CacheInterface:
        """Get cache interface from container."""
        return self.get_container().get_cache()
    
    def get_config(self) -> ConfigurationInterface:
        """Get configuration interface from container."""
        return self.get_container().get_configuration()
    
    def create_temp_path(self, suffix: str = "", prefix: str = "test_") -> Path:
        """Create a temporary path that will be cleaned up automatically."""
        temp_path = Path(tempfile.mkdtemp(suffix=suffix, prefix=prefix))
        self._temp_paths.append(temp_path)
        return temp_path
    
    def create_temp_file(self, content: str = "", suffix: str = ".txt") -> Path:
        """Create a temporary file with content."""
        temp_dir = self.create_temp_path()
        temp_file = temp_dir / f"test_file{suffix}"
        temp_file.write_text(content)
        return temp_file
    
    def _cleanup_temp_paths(self) -> None:
        """Clean up all temporary paths."""
        for path in self._temp_paths:
            if path.exists():
                if path.is_file():
                    path.unlink()
                else:
                    shutil.rmtree(path, ignore_errors=True)
        self._temp_paths.clear()


class UnitTest(BaseTest):
    """
    Base class for unit tests.
    
    Unit tests should:
    - Test single components in isolation
    - Use mocks for all external dependencies
    - Be fast and deterministic
    - Not perform I/O operations
    """
    
    def _configure_container(self) -> None:
        """Configure container for unit tests."""
        config = TestConfiguration(environment=TestEnvironmentType.UNIT)
        
        # Use mock implementations for all external dependencies
        self._container.register_configuration(config)
        self._container.register_filesystem(FileSystemFactory.create_in_memory())
        self._container.register_network(NetworkFactory.create_mock())
        self._container.register_cache(CacheFactory.create_in_memory(max_size=1024*1024))
    
    def mock_location_repository(self) -> Mock:
        """Get a mock location repository."""
        return self.get_container().get_location_repository()
    
    def mock_simulation_repository(self) -> Mock:
        """Get a mock simulation repository."""
        return self.get_container().get_simulation_repository()
    
    def assert_no_filesystem_calls(self) -> None:
        """Assert that no filesystem operations were performed."""
        fs = self.get_filesystem()
        if hasattr(fs, 'exists'):
            self.assertFalse(fs.exists.called, "Filesystem operations should not be called in unit tests")


class IntegrationTest(BaseTest):
    """
    Base class for integration tests.
    
    Integration tests should:
    - Test interaction between multiple components
    - Use fake implementations for external dependencies
    - Test data flow and component integration
    - Be reasonably fast but may perform some I/O
    """
    
    def _configure_container(self) -> None:
        """Configure container for integration tests."""
        config = TestConfiguration(environment=TestEnvironmentType.INTEGRATION)
        
        # Use fake/temporary implementations
        self._container.register_configuration(config)
        self._container.register_filesystem(FileSystemFactory.create_temporary())
        self._container.register_network(NetworkFactory.create_fake())
        self._container.register_cache(CacheFactory.create_in_memory(max_size=10*1024*1024))
    
    def setup_fake_remote_data(self, url: str, content: bytes) -> None:
        """Set up fake remote data for testing."""
        network = self.get_network()
        if hasattr(network, 'add_remote_file'):
            network.add_remote_file(url, content)
    
    def setup_fake_filesystem_data(self, path: str, content: str) -> None:
        """Set up fake filesystem data for testing."""
        fs = self.get_filesystem()
        fs.write_text(path, content)
    
    def assert_cache_used(self, expected_calls: int = None) -> None:
        """Assert that cache was used during the test."""
        cache = self.get_cache()
        if hasattr(cache, 'stats'):
            stats = cache.stats()
            if expected_calls:
                # This would need specific implementation based on cache type
                pass


class EndToEndTest(BaseTest):
    """
    Base class for end-to-end tests.
    
    E2E tests should:
    - Test complete user workflows
    - Use real implementations where possible
    - Test system behavior from user perspective
    - May be slower and less deterministic
    """
    
    def _configure_container(self) -> None:
        """Configure container for end-to-end tests."""
        config = TestConfiguration(environment=TestEnvironmentType.END_TO_END)
        
        # Use real implementations for comprehensive testing
        self._container.register_configuration(config)
        self._container.register_filesystem(FileSystemFactory.create_real())
        self._container.register_network(NetworkFactory.create_real())
        self._container.register_cache(CacheFactory.create_file_cache(
            self.create_temp_path("cache"), max_size=100*1024*1024
        ))
    
    def setup_test_data_directory(self) -> Path:
        """Set up a directory with test data."""
        test_data_dir = self.create_temp_path("test_data")
        
        # Create sample files
        (test_data_dir / "sample1.txt").write_text("Sample file 1 content")
        (test_data_dir / "sample2.txt").write_text("Sample file 2 content")
        
        # Create subdirectory
        subdir = test_data_dir / "subdir"
        subdir.mkdir()
        (subdir / "nested.txt").write_text("Nested file content")
        
        return test_data_dir
    
    def assert_files_exist(self, *file_paths: str) -> None:
        """Assert that specified files exist."""
        fs = self.get_filesystem()
        for file_path in file_paths:
            self.assertTrue(fs.exists(file_path), f"File should exist: {file_path}")


class PerformanceTest(BaseTest):
    """
    Base class for performance tests.
    
    Performance tests should:
    - Measure execution time and resource usage
    - Test scalability and efficiency
    - Use optimized implementations
    - Generate performance reports
    """
    
    def _configure_container(self) -> None:
        """Configure container for performance tests."""
        config = TestConfiguration(environment=TestEnvironmentType.PERFORMANCE)
        
        # Use optimized implementations
        self._container.register_configuration(config)
        self._container.register_filesystem(FileSystemFactory.create_in_memory())
        self._container.register_network(NetworkFactory.create_fake())
        self._container.register_cache(CacheFactory.create_in_memory(max_size=500*1024*1024))
    
    def measure_execution_time(self, func, *args, **kwargs) -> tuple[Any, float]:
        """Measure execution time of a function."""
        import time
        start_time = time.perf_counter()
        result = func(*args, **kwargs)
        end_time = time.perf_counter()
        execution_time = end_time - start_time
        return result, execution_time
    
    def assert_execution_time_under(self, max_time: float, func, *args, **kwargs) -> Any:
        """Assert that function executes within specified time."""
        result, execution_time = self.measure_execution_time(func, *args, **kwargs)
        self.assertLess(
            execution_time, max_time,
            f"Function took {execution_time:.3f}s, expected under {max_time:.3f}s"
        )
        return result


class RepositoryTest(UnitTest):
    """
    Base class for testing repository implementations.
    
    Provides common test patterns for repository classes:
    - CRUD operations
    - Query methods
    - Error handling
    - Data consistency
    """
    
    @abstractmethod
    def create_repository(self) -> Any:
        """Create the repository under test."""
        pass
    
    @abstractmethod
    def create_sample_entity(self) -> Any:
        """Create a sample entity for testing."""
        pass
    
    def setUp(self) -> None:
        """Set up repository test."""
        super().setUp()
        self.repository = self.create_repository()
        self.sample_entity = self.create_sample_entity()
    
    def test_save_and_find(self) -> None:
        """Test basic save and find operations."""
        # This would be implemented by concrete repository tests
        pass
    
    def test_delete(self) -> None:
        """Test delete operations."""
        # This would be implemented by concrete repository tests
        pass
    
    def test_find_all(self) -> None:
        """Test find all operations."""
        # This would be implemented by concrete repository tests
        pass


class ServiceTest(IntegrationTest):
    """
    Base class for testing service layer components.
    
    Service tests should:
    - Test business logic
    - Test service interactions
    - Use repository mocks/fakes
    - Test error handling and validation
    """
    
    def setUp(self) -> None:
        """Set up service test with repository mocks."""
        super().setUp()
        self._setup_repository_mocks()
    
    def _setup_repository_mocks(self) -> None:
        """Set up mock repositories."""
        # Override in concrete service tests
        pass


class ComponentTest(IntegrationTest):
    """
    Base class for testing individual components with their dependencies.
    
    Component tests should:
    - Test component interfaces
    - Test component collaboration
    - Use dependency injection
    - Test configuration and setup
    """
    
    @abstractmethod
    def create_component(self) -> Any:
        """Create the component under test."""
        pass
    
    def setUp(self) -> None:
        """Set up component test."""
        super().setUp()
        self.component = self.create_component()


class TestSuite:
    """
    Test suite builder that follows Clean Architecture principles.
    
    Provides utilities for organizing tests by type and running them
    with appropriate configurations.
    """
    
    def __init__(self):
        """Initialize test suite builder."""
        self._unit_tests: List[Type[UnitTest]] = []
        self._integration_tests: List[Type[IntegrationTest]] = []
        self._e2e_tests: List[Type[EndToEndTest]] = []
        self._performance_tests: List[Type[PerformanceTest]] = []
    
    def add_unit_tests(self, *test_classes: Type[UnitTest]) -> 'TestSuite':
        """Add unit test classes."""
        self._unit_tests.extend(test_classes)
        return self
    
    def add_integration_tests(self, *test_classes: Type[IntegrationTest]) -> 'TestSuite':
        """Add integration test classes."""
        self._integration_tests.extend(test_classes)
        return self
    
    def add_e2e_tests(self, *test_classes: Type[EndToEndTest]) -> 'TestSuite':
        """Add end-to-end test classes."""
        self._e2e_tests.extend(test_classes)
        return self
    
    def add_performance_tests(self, *test_classes: Type[PerformanceTest]) -> 'TestSuite':
        """Add performance test classes."""
        self._performance_tests.extend(test_classes)
        return self
    
    def build_unit_suite(self) -> unittest.TestSuite:
        """Build test suite for unit tests only."""
        suite = unittest.TestSuite()
        for test_class in self._unit_tests:
            suite.addTest(unittest.TestLoader().loadTestsFromTestCase(test_class))
        return suite
    
    def build_integration_suite(self) -> unittest.TestSuite:
        """Build test suite for integration tests only."""
        suite = unittest.TestSuite()
        for test_class in self._integration_tests:
            suite.addTest(unittest.TestLoader().loadTestsFromTestCase(test_class))
        return suite
    
    def build_e2e_suite(self) -> unittest.TestSuite:
        """Build test suite for end-to-end tests only."""
        suite = unittest.TestSuite()
        for test_class in self._e2e_tests:
            suite.addTest(unittest.TestLoader().loadTestsFromTestCase(test_class))
        return suite
    
    def build_performance_suite(self) -> unittest.TestSuite:
        """Build test suite for performance tests only."""
        suite = unittest.TestSuite()
        for test_class in self._performance_tests:
            suite.addTest(unittest.TestLoader().loadTestsFromTestCase(test_class))
        return suite
    
    def build_all_suite(self) -> unittest.TestSuite:
        """Build complete test suite with all test types."""
        suite = unittest.TestSuite()
        suite.addTest(self.build_unit_suite())
        suite.addTest(self.build_integration_suite())
        suite.addTest(self.build_e2e_suite())
        suite.addTest(self.build_performance_suite())
        return suite