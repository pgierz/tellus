# Clean Test Architecture for Tellus

This directory contains a complete redesign of the test architecture following Clean Architecture principles and SOLID design patterns.

## Overview

The existing test suite had several architectural issues:
- Tight coupling between tests and implementation details
- Direct filesystem and network dependencies in unit tests
- Poor separation between different types of tests
- Inconsistent mocking strategies
- Violation of Single Responsibility Principle

This new architecture addresses these issues with:
- **Dependency Inversion**: All external dependencies are abstracted behind interfaces
- **Single Responsibility**: Each component has one clear purpose
- **Open/Closed Principle**: Easy to extend without modifying existing code
- **Interface Segregation**: Small, focused interfaces
- **Clear boundaries** between unit, integration, and end-to-end tests

## Architecture Components

### 1. Interfaces (`interfaces.py`)
Defines abstract contracts for all testable components:
- `FileSystemInterface` - Abstract filesystem operations
- `NetworkInterface` - Abstract network operations  
- `CacheInterface` - Abstract cache operations
- `LocationRepository` - Abstract location persistence
- `SimulationRepository` - Abstract simulation persistence
- Plus logging, progress tracking, and configuration interfaces

### 2. Implementation Layers

#### Filesystem Abstraction (`filesystem.py`)
- `RealFileSystem` - Real filesystem for E2E tests
- `InMemoryFileSystem` - Fast in-memory filesystem for unit tests
- `TemporaryFileSystem` - Controlled temporary filesystem for integration tests
- `MockFileSystem` - Mock filesystem for isolated unit tests

#### Network Abstraction (`network.py`)
- `FakeNetwork` - Controllable fake network for integration tests
- `MockNetwork` - Mock network for unit tests
- `SlowNetwork` - Wrapper to simulate network delays
- `NetworkRecorder` - Records network operations for verification

#### Cache Abstraction (`cache.py`)
- `InMemoryCache` - Fast in-memory cache
- `FileCache` - File-based cache for integration tests
- `MockCache` - Mock cache for unit tests
- `NoOpCache` - Cache that does nothing

### 3. Test Organization

#### Base Test Classes (`base_tests.py`)
- `UnitTest` - Base for unit tests (fast, isolated, no I/O)
- `IntegrationTest` - Base for integration tests (fake dependencies)
- `EndToEndTest` - Base for E2E tests (real implementations)
- `PerformanceTest` - Base for performance tests
- `RepositoryTest` - Base for repository testing patterns
- `ServiceTest` - Base for service layer testing

#### Test Organization (`test_organization.py`)
- `TestLayer` - Abstract test layer definition
- `TestLayerRegistry` - Manages test layers and boundaries
- `TestCategorizer` - Automatically categorizes tests
- `TestSuiteOrganizer` - Organizes tests by type
- `LayeredTestRunner` - Executes tests respecting boundaries

### 4. Test Data and Factories

#### Factories (`factories.py`)
Builder pattern implementations for clean test data:
- `LocationBuilder` - Fluent interface for creating test locations
- `SimulationBuilder` - Fluent interface for creating test simulations
- `ArchiveBuilder` - Fluent interface for creating test archives
- Factory classes for common object creation patterns

#### Test Doubles (`test_doubles.py`)
Clear separation of test double types:
- **Dummies** - Objects that do nothing but satisfy interfaces
- **Stubs** - Objects that return predetermined responses
- **Fakes** - Objects that have working implementations but simplified
- **Mocks** - Objects that verify interactions
- **Spies** - Objects that record method calls for verification

### 5. Configuration and Dependency Injection

#### Configuration (`configuration.py`)
- Environment-specific configurations (unit, integration, E2E)
- Component-specific settings (database, cache, network, filesystem)
- Environment variable loading
- Configuration validation

#### Dependency Injection (`dependency_injection.py`)
- `TestContainer` - IoC container for managing dependencies
- `TestContainerBuilder` - Builder for container configuration
- `ContainerFactory` - Pre-configured containers for test types
- Repository implementations for testing

### 6. Utilities (`utilities.py`)
Single-purpose utility classes:
- `TemporaryPathManager` - Manages temporary paths with cleanup
- `TestDataGenerator` - Generates test data with various patterns
- `FileSystemTestHelper` - Filesystem testing operations
- `ArchiveTestHelper` - Archive creation and manipulation
- `TimeTestHelper` - Time freezing and measurement
- `AssertionHelper` - Custom assertion methods

## Usage Examples

### Unit Test Example
```python
from .base_tests import UnitTest
from .factories import LocationBuilder

class TestLocationUnit(UnitTest):
    def test_location_creation(self):
        # Fast, deterministic, no I/O
        location = (LocationBuilder()
                   .with_name("test_location")
                   .with_kinds("DISK")
                   .as_s3_location("test-bucket")
                   .build())
        
        self.assertEqual(location.name, "test_location")
        # Tests business logic only
```

### Integration Test Example
```python
from .base_tests import IntegrationTest
from .factories import SimulationFactory

class TestSimulationIntegration(IntegrationTest):
    def test_simulation_persistence(self):
        # Uses fake filesystem and in-memory storage
        simulation = SimulationFactory().create_with_locations()
        
        # Test component interaction
        self.simulation_repository.save(simulation)
        loaded = self.simulation_repository.find_by_id(simulation.simulation_id)
        
        self.assertEqual(loaded.simulation_id, simulation.simulation_id)
```

### End-to-End Test Example
```python
from .base_tests import EndToEndTest

class TestCompleteWorkflow(EndToEndTest):
    def test_full_archive_workflow(self):
        # Uses real implementations in controlled environment
        archive_dir = self.setup_test_data_directory()
        
        # Test complete user workflow
        result = self.run_archive_command(archive_dir)
        
        self.assert_files_exist(result.output_path)
```

## Migration Guide

### From Old Tests to New Architecture

1. **Identify Test Type**:
   - Pure logic → `UnitTest`
   - Component interaction → `IntegrationTest`
   - Full workflow → `EndToEndTest`

2. **Replace Direct Dependencies**:
   ```python
   # Old: Direct file operations
   with open("test_file.txt", "w") as f:
       f.write("content")
   
   # New: Use filesystem interface
   filesystem = self.get_filesystem()
   filesystem.write_text("test_file.txt", "content")
   ```

3. **Use Builders Instead of Direct Construction**:
   ```python
   # Old: Direct object creation
   location = Location("test", ["DISK"], {"protocol": "file"})
   
   # New: Builder pattern
   location = (LocationBuilder()
              .with_name("test")
              .with_kinds("DISK")
              .with_protocol("file")
              .build())
   ```

4. **Replace Manual Mocking with Test Doubles**:
   ```python
   # Old: Manual mock setup
   with patch('tellus.location.Location._save_locations'):
       # test code
   
   # New: Proper test doubles
   def _configure_container(self):
       doubles = create_unit_test_doubles()
       self.get_container().register_location_repository(
           doubles.get("location_repository")
       )
   ```

## Benefits

### 1. **Speed**
- Unit tests run in milliseconds (no I/O)
- Clear separation prevents slow tests from affecting fast feedback

### 2. **Reliability**
- Deterministic test data via builders
- Proper isolation eliminates test interdependencies
- Controlled environments prevent flaky tests

### 3. **Maintainability**
- Clear boundaries and single responsibilities
- Easy to extend without breaking existing tests
- Consistent patterns across all test types

### 4. **Scalability**
- Architecture supports large test suites
- Parallel execution for isolated tests
- Proper resource management prevents memory leaks

### 5. **Developer Experience**
- Clear test categorization
- Rich builder APIs for test data
- Comprehensive utilities for common operations
- Automatic cleanup and resource management

## Directory Structure

```
tests/architecture/
├── README.md                 # This file
├── interfaces.py            # Abstract interfaces
├── filesystem.py            # Filesystem implementations
├── network.py               # Network implementations  
├── cache.py                 # Cache implementations
├── configuration.py         # Test configuration management
├── dependency_injection.py  # IoC container
├── base_tests.py            # Base test classes
├── factories.py             # Test data builders
├── test_doubles.py          # Test double implementations
├── test_organization.py     # Test layer organization
├── utilities.py             # Test utilities
└── example_refactored_test.py # Usage examples
```

## Getting Started

1. **For New Tests**: Inherit from appropriate base class (`UnitTest`, `IntegrationTest`, `EndToEndTest`)

2. **For Existing Tests**: Use the migration guide to gradually refactor

3. **For Test Data**: Use builders from `factories.py` instead of direct construction

4. **For External Dependencies**: Use interfaces and test doubles from appropriate modules

5. **For Utilities**: Use focused helpers from `utilities.py` instead of custom implementations

This architecture provides a solid foundation for maintainable, scalable testing that grows with your system while maintaining clean boundaries and fast feedback cycles.