"""
Clean Test Architecture for Tellus

This package provides a complete test architecture following Clean Architecture
principles and SOLID design patterns.

Quick Start:
-----------

For Unit Tests:
>>> from .base_tests import UnitTest
>>> from .factories import LocationBuilder
>>> 
>>> class MyUnitTest(UnitTest):
>>>     def test_something(self):
>>>         location = LocationBuilder().with_name("test").build()
>>>         # Fast, isolated test logic here

For Integration Tests:
>>> from .base_tests import IntegrationTest
>>> from .factories import SimulationFactory
>>> 
>>> class MyIntegrationTest(IntegrationTest):
>>>     def test_something(self):
>>>         simulation = SimulationFactory().create_with_locations()
>>>         # Test component interaction with fakes

For Test Utilities:
>>> from .utilities import test_environment, quick_temp_dir
>>> 
>>> with test_environment() as env:
>>>     temp_dir = env["path_manager"].create_temp_dir()
>>>     # Use utilities for common test operations

Architecture Benefits:
---------------------
- Clear separation between unit, integration, and E2E tests
- Proper dependency injection and test doubles
- Fast, reliable, maintainable test suite
- Clean abstractions that follow SOLID principles
- Extensive utilities following single responsibility principle
"""

# Base test classes
from .base_tests import (BaseTest, ComponentTest, EndToEndTest,
                         IntegrationTest, PerformanceTest, RepositoryTest,
                         ServiceTest, TestSuite, UnitTest)
# Configuration and dependency injection
from .configuration import (ConfigurationManager, TestConfiguration,
                            TestConfigurationProvider, TestEnvironmentType)
from .dependency_injection import (ContainerFactory, TestContainer,
                                   TestContainerBuilder)
# Test factories and builders
from .factories import (ArchiveBuilder, ArchiveFactory, LocationBuilder,
                        LocationFactory, SimulationBuilder, SimulationFactory,
                        TestDataFactory, archive, location, simulation)
# Abstract interfaces (for advanced usage)
from .interfaces import (CacheInterface, ConfigurationInterface,
                         FileSystemInterface, LocationRepository,
                         NetworkInterface, SimulationRepository)
# Test doubles
from .test_doubles import (TestDoubleFactory, TestDoubleRegistry,
                           TestDoubleType, create_integration_test_doubles,
                           create_unit_test_doubles)
# Test organization
from .test_organization import (TestCategorizer, TestLayerRegistry,
                                TestSuiteOrganizer, create_layered_test_runner,
                                organize_test_classes)
# Utilities
from .utilities import (ArchiveTestHelper, AssertionHelper, ChecksumHelper,
                        FileSystemTestHelper, NetworkTestHelper,
                        TemporaryPathManager, TestDataGenerator,
                        TestResourceManager, TimeTestHelper, quick_archive,
                        quick_temp_dir, quick_temp_file, test_environment)

__version__ = "1.0.0"
__author__ = "Claude (Anthropic)"
__email__ = "noreply@anthropic.com"

__all__ = [
    # Base classes
    "BaseTest", "UnitTest", "IntegrationTest", "EndToEndTest", "PerformanceTest",
    "RepositoryTest", "ServiceTest", "ComponentTest", "TestSuite",
    
    # Factories
    "LocationBuilder", "SimulationBuilder", "ArchiveBuilder",
    "LocationFactory", "SimulationFactory", "ArchiveFactory",
    "TestDataFactory", "location", "simulation", "archive",
    
    # Test doubles
    "TestDoubleType", "TestDoubleFactory", "TestDoubleRegistry",
    "create_unit_test_doubles", "create_integration_test_doubles",
    
    # Configuration
    "TestConfiguration", "TestEnvironmentType", "ConfigurationManager",
    "TestConfigurationProvider",
    
    # Dependency injection
    "TestContainer", "TestContainerBuilder", "ContainerFactory",
    
    # Organization
    "TestLayerRegistry", "TestCategorizer", "TestSuiteOrganizer",
    "organize_test_classes", "create_layered_test_runner",
    
    # Utilities
    "TemporaryPathManager", "TestDataGenerator", "FileSystemTestHelper",
    "ArchiveTestHelper", "NetworkTestHelper", "TimeTestHelper", "ChecksumHelper",
    "AssertionHelper", "TestResourceManager", "test_environment",
    "quick_temp_dir", "quick_temp_file", "quick_archive",
    
    # Interfaces
    "FileSystemInterface", "NetworkInterface", "CacheInterface",
    "ConfigurationInterface", "LocationRepository", "SimulationRepository",
]