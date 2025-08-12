# Location Filesystem Integration Tests

This directory contains comprehensive integration tests for the Location filesystem bug fix, specifically validating that the PathSandboxedFileSystem correctly integrates with Tellus's data persistence and configuration systems.

## Overview

The PathSandboxedFileSystem fix addresses a security vulnerability where Location filesystem operations could occur in the current working directory instead of the configured location path. These integration tests ensure the fix works correctly across all Tellus components and workflows.

## Test Files

### `test_location_persistence_integration.py`
**Purpose**: Tests Location data flow from creation through persistence and filesystem operations.

**Key Test Classes**:
- `TestLegacyLocationPersistenceIntegration`: Legacy Location system with PathSandboxedFileSystem
- `TestNewArchitecturePersistenceIntegration`: Domain-driven architecture integration
- `TestLocationContextIntegration`: Simulation context and path templating
- `TestEndToEndLocationWorkflows`: Complete realistic workflows

**Critical Tests**:
- Location creation persists with sandboxed filesystem
- Location load/reload preserves path behavior
- Multiple locations maintain separate sandboxing
- Configuration updates maintain sandboxing
- Context templating works with security constraints

### `test_location_configuration_scenarios.py`
**Purpose**: Tests various Location configuration scenarios with different path types and protocols.

**Key Test Classes**:
- `TestRelativePathConfigurations`: Relative path handling and sandboxing
- `TestAbsolutePathConfigurations`: Absolute path configurations
- `TestEmptyAndSpecialPathConfigurations`: Edge cases (empty, None, root paths)
- `TestProtocolSpecificConfigurations`: Different protocols (file, SFTP, S3)
- `TestTemplatedPathConfigurations`: Template resolution with sandboxing
- `TestNewArchitectureConfigurationScenarios`: Service layer validation

**Critical Tests**:
- Relative paths resolve correctly but remain sandboxed
- Absolute paths maintain isolation between locations
- Protocol-specific configurations work with sandboxing
- Complex nested configurations persist correctly
- Edge cases don't break security

### `test_location_filesystem_adapter_integration.py`
**Purpose**: Tests FSSpecAdapter integration with PathSandboxedFileSystem.

**Key Test Classes**:
- `TestFSSpecAdapterIntegration`: Adapter with sandboxed filesystem
- `TestLegacyLocationWithFSSpecPatterns`: Legacy system compatibility
- `TestIntegratedWorkflowScenarios`: Multi-component workflows

**Critical Tests**:
- FSSpecAdapter operations work within sandbox
- Progress tracking works with sandboxed operations
- File finding respects sandbox boundaries
- Bulk operations maintain security
- Earth science workflows complete successfully

### `test_location_sandboxing_integration_suite.py`
**Purpose**: Master test suite providing comprehensive validation of the entire fix.

**Key Features**:
- Complete critical path testing
- Cross-architecture compatibility validation
- Security regression prevention
- Performance impact assessment
- Comprehensive validation summary

## Running the Tests

### Run All Integration Tests
```bash
# Run all location integration tests
pixi run -e test pytest tests/integration/test_location_*.py -v

# Run with coverage
pixi run -e test pytest tests/integration/test_location_*.py --cov=tellus.location --cov-report=html
```

### Run Specific Test Suites
```bash
# Run persistence tests
pixi run -e test pytest tests/integration/test_location_persistence_integration.py -v

# Run configuration scenarios
pixi run -e test pytest tests/integration/test_location_configuration_scenarios.py -v

# Run filesystem adapter tests
pixi run -e test pytest tests/integration/test_location_filesystem_adapter_integration.py -v

# Run comprehensive validation suite
pixi run -e test pytest tests/integration/test_location_sandboxing_integration_suite.py -v
```

### Quick Validation
```bash
# Run the master validation suite for quick verification
python tests/integration/test_location_sandboxing_integration_suite.py
```

## Test Categories and Markers

The integration tests use pytest markers to categorize tests:

- `@pytest.mark.integration`: All integration tests
- `@pytest.mark.location`: Location-specific tests
- `@pytest.mark.security`: Security-related tests
- `@pytest.mark.earth_science`: Earth science workflow tests

Run specific categories:
```bash
# Run only security tests
pixi run -e test pytest -m security tests/integration/

# Run only earth science tests
pixi run -e test pytest -m earth_science tests/integration/
```

## Data Flow Validation

The tests validate the complete data flow:

1. **Configuration Phase**:
   - Location configuration with path settings
   - Validation of path types (relative, absolute, templated)
   - Protocol-specific configuration handling

2. **Creation Phase**:
   - Location entity creation
   - PathSandboxedFileSystem instantiation
   - Configuration validation and storage

3. **Persistence Phase**:
   - JSON serialization of location data
   - Atomic file operations
   - Backup and recovery procedures

4. **Loading Phase**:
   - JSON deserialization
   - Location entity reconstruction
   - PathSandboxedFileSystem recreation

5. **Operation Phase**:
   - Filesystem operations within sandbox
   - Security constraint enforcement
   - Cross-location isolation

6. **Integration Phase**:
   - Simulation context templating
   - FSSpec adapter compatibility
   - Earth science workflow execution

## Security Validation

The tests specifically validate security aspects:

### Directory Traversal Prevention
- `../` path attempts blocked
- `../../` and deeper traversals blocked
- Windows-style `..\\` traversals blocked
- Mixed traversal patterns blocked

### Path Resolution Security
- Absolute paths made relative to base path
- Symlink following within sandbox only
- Template resolution maintains constraints
- Context path prefixes respect boundaries

### Cross-Location Isolation
- Multiple locations can't access each other's files
- Configuration changes don't affect other locations
- Persistence maintains separation
- Filesystem operations respect boundaries

## Earth Science Workflow Validation

Tests include realistic Earth science scenarios:

### Multi-Model Data Management
- CESM2, GFDL, UKESM model outputs
- Hierarchical directory structures
- Model/experiment/variant organization

### Data Processing Pipelines
- Input data → processing → output → archive
- Cross-stage data transfers
- Metadata preservation
- Quality control workflows

### Context Templating
- `{model_id}/{experiment_id}` path patterns
- Dynamic path resolution
- Metadata-driven organization
- Template variable substitution

## Performance Validation

Tests ensure sandboxing doesn't impact performance:

- Bulk file operations (100+ files)
- Glob pattern matching
- Recursive directory traversal
- File existence checks
- Large file transfers

Performance thresholds:
- Glob 100 files: < 1 second
- Exists checks: < 1 second
- File transfers: No significant overhead

## Architecture Integration

The tests validate integration across Tellus architectures:

### Legacy System Integration
- `Location` class with class registry
- Direct JSON file persistence
- Integrated PathSandboxedFileSystem

### New Domain-Driven Architecture
- `LocationEntity` domain objects
- `ILocationRepository` interface
- `JsonLocationRepository` implementation
- `LocationApplicationService` use cases
- `FSSpecAdapter` infrastructure

### Cross-Architecture Compatibility
- Both systems work with same data
- Persistence formats compatible
- Security constraints consistent
- Migration pathways validated

## Troubleshooting

### Common Issues

**Test Environment Setup**:
```bash
# Ensure test dependencies installed
pixi install --feature test

# Clear any cached bytecode
find . -name "*.pyc" -delete
find . -name "__pycache__" -type d -delete
```

**Temporary Directory Cleanup**:
```bash
# If tests fail with permission errors
sudo rm -rf /tmp/tellus_test_*
```

**Path Resolution Issues**:
- Verify absolute paths used in tests
- Check that temporary directories exist
- Validate file permissions

### Debugging Failed Tests

1. **Run with verbose output**:
   ```bash
   pixi run -e test pytest tests/integration/test_location_*.py -v -s
   ```

2. **Check specific test method**:
   ```bash
   pixi run -e test pytest tests/integration/test_location_persistence_integration.py::TestLegacyLocationPersistenceIntegration::test_location_creation_persists_with_sandboxed_fs -v -s
   ```

3. **Enable debug logging**:
   ```bash
   TELLUS_LOG_LEVEL=DEBUG pixi run -e test pytest tests/integration/test_location_*.py -v
   ```

## Contributing

When adding new integration tests:

1. **Follow naming conventions**:
   - Test files: `test_location_*_integration.py`
   - Test classes: `Test*Integration`
   - Test methods: `test_*`

2. **Include proper setup/teardown**:
   - Use temporary directories
   - Clean up resources
   - Clear Location registries

3. **Test both architectures**:
   - Legacy Location system
   - New domain-driven system
   - Cross-compatibility

4. **Validate security**:
   - Include directory traversal tests
   - Test sandbox boundaries
   - Verify isolation

5. **Document test purpose**:
   - Clear docstrings
   - Explain test scenarios
   - Document expected outcomes

## Success Criteria

The integration tests pass when:

✅ **Location Creation**: Locations create with PathSandboxedFileSystem
✅ **Configuration Persistence**: All config types persist correctly
✅ **Security Enforcement**: Directory traversal attacks blocked
✅ **Cross-Location Isolation**: Multiple locations remain isolated
✅ **Context Integration**: Simulation context works with sandboxing
✅ **Adapter Compatibility**: FSSpecAdapter works with sandboxed filesystem
✅ **Workflow Execution**: Earth science workflows complete successfully
✅ **Performance Acceptable**: No significant performance degradation
✅ **Architecture Integration**: Both legacy and new systems work
✅ **Regression Prevention**: Security vulnerabilities remain fixed

When all these criteria are met, the PathSandboxedFileSystem fix is validated as working correctly within the broader Tellus ecosystem.