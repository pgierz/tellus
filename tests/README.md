# Tellus Test Framework for Earth Science Workflows

This directory contains a comprehensive test framework specifically designed for Earth System Model data archiving and management workflows. The framework provides robust testing capabilities tailored to the unique requirements of Earth science research environments.

## Overview

The test framework is organized into several components:

- **Unit Tests**: Core component testing with Earth science domain knowledge
- **Integration Tests**: Complete workflow testing for typical research scenarios  
- **Performance Tests**: Large dataset handling and scalability testing
- **Property-Based Tests**: Data integrity verification using property-based testing
- **Earth Science Fixtures**: Realistic test data and scenarios
- **Domain-Specific Utilities**: Tools for validating Earth science data formats and conventions

## Test Organization

### Core Test Modules

- `test_archive_system.py` - Unit tests for archive system components (CompressedArchive, ArchiveManifest, CacheManager)
- `test_integration_workflows.py` - Integration tests for complete research workflows
- `test_performance.py` - Performance tests for large dataset handling
- `test_property_based.py` - Property-based tests for data integrity
- `test_simulation.py` - Existing simulation object tests (preserved)
- `test_location.py` - Existing location management tests (preserved)

### Fixtures and Utilities

- `fixtures/earth_science.py` - Earth science specific test fixtures
- `utils/earth_science_helpers.py` - Domain-specific validation and testing utilities
- `conftest.py` - Global test configuration and fixture registration

## Test Markers

The framework uses pytest markers to categorize and organize tests:

### Primary Categories
- `@pytest.mark.unit` - Unit tests for individual components
- `@pytest.mark.integration` - Integration tests for component interactions
- `@pytest.mark.performance` - Performance tests for large datasets
- `@pytest.mark.property` - Property-based tests for data integrity

### Domain-Specific Markers
- `@pytest.mark.earth_science` - Tests specific to Earth science workflows
- `@pytest.mark.archive` - Tests for archive system components
- `@pytest.mark.cache` - Tests for caching functionality
- `@pytest.mark.location` - Tests for multi-location functionality

### Resource Markers
- `@pytest.mark.slow` - Tests that take more than 30 seconds
- `@pytest.mark.network` - Tests requiring network access
- `@pytest.mark.large_data` - Tests working with large datasets (>100MB)
- `@pytest.mark.hpc` - Tests for HPC environment scenarios

## Running Tests

### Basic Test Execution

```bash
# Run all tests
pytest

# Run only unit tests
pytest -m unit

# Run Earth science specific tests
pytest -m earth_science

# Run integration tests (excluding slow tests)
pytest -m "integration and not slow"

# Run performance tests
pytest -m performance
```

### Focused Test Execution

```bash
# Run only archive system tests
pytest tests/test_archive_system.py

# Run cache-related tests
pytest -m cache

# Run tests for specific functionality
pytest -k "test_cache" 

# Run property-based tests
pytest -m property
```

### Running Tests with Coverage

```bash
# Generate coverage report
pytest --cov=tellus --cov-report=html

# Coverage with specific markers
pytest -m "unit or integration" --cov=tellus --cov-report=term-missing
```

### Parallel Test Execution

```bash
# Run tests in parallel (requires pytest-xdist)
pytest -n auto

# Run slow tests in parallel
pytest -m slow -n 4
```

## Test Configuration

The test framework is configured through `pyproject.toml`:

### Key Configuration Options

- **Test Discovery**: Tests are discovered in the `tests/` directory
- **Coverage**: Configured to track coverage of the `tellus` package
- **Timeouts**: Tests timeout after 5 minutes by default
- **Markers**: All markers are strictly enforced
- **Warnings**: Filters for handling common warnings in Earth science libraries

### Dependencies

The test framework requires additional dependencies for comprehensive testing:

#### Core Testing Dependencies
- `pytest` - Main testing framework
- `pytest-cov` - Coverage reporting
- `pytest-timeout` - Test timeouts
- `pytest-mock` - Mocking utilities
- `pytest-xdist` - Parallel test execution

#### Property-Based Testing
- `hypothesis` - Property-based test generation
- `faker` - Fake data generation
- `factory-boy` - Test data factories

#### Earth Science Libraries
- `numpy` - Numerical computing
- `netcdf4` - NetCDF file handling
- `xarray` - Labeled multi-dimensional arrays
- `zarr` - Chunked array storage
- `dask` - Parallel computing

#### Performance Testing
- `pytest-benchmark` - Performance benchmarking
- `psutil` - System resource monitoring

## Earth Science Test Fixtures

### Climate Data Fixtures

The framework provides realistic climate model data fixtures:

```python
def test_netcdf_processing(create_netcdf_file):
    # Create a realistic climate NetCDF file
    netcdf_path = create_netcdf_file("temperature_2020.nc")
    # Test processing...
```

### Archive Structure Fixtures

Model archive structures following Earth science conventions:

```python
def test_model_archive(create_model_archive):
    # Create realistic model experiment archive
    archive_dir = create_model_archive("ECHAM6_piControl_r1i1p1f1")
    # Test archive processing...
```

### Multi-Location Setup

HPC and research environment simulation:

```python
def test_hpc_workflow(multi_location_setup):
    locations = multi_location_setup
    # locations['hpc_scratch'], locations['hpc_work'], etc.
    # Test multi-location data management...
```

## Domain-Specific Utilities

### File Validation

```python
from tests.utils.earth_science_helpers import EarthScienceFileValidator

validator = EarthScienceFileValidator()

# Validate NetCDF files
assert validator.is_netcdf_file("temperature_2020.nc")

# Parse CMIP6 filenames
file_info = validator.parse_cmip6_filename("tas_Amon_GFDL-CM4_historical_r1i1p1f1_gn_185001-201412.nc")
```

### Archive Analysis

```python
from tests.utils.earth_science_helpers import EarthScienceArchiveAnalyzer

analyzer = EarthScienceArchiveAnalyzer(archive_path)
structure = analyzer.analyze_archive_structure()

# Get detected models, variables, time coverage, etc.
print(structure['models_detected'])
print(structure['time_coverage'])
```

### Test Assertions

```python
from tests.utils.earth_science_helpers import EarthScienceTestAssertions

# Assert NetCDF file validity
EarthScienceTestAssertions.assert_valid_netcdf_file(
    netcdf_path, 
    required_variables=['temperature', 'pressure']
)

# Assert archive completeness
EarthScienceTestAssertions.assert_archive_completeness(archive_path)

# Assert filename conventions
EarthScienceTestAssertions.assert_filename_follows_convention(
    "tas_Amon_GFDL-CM4_historical_r1i1p1f1_gn_185001-201412.nc",
    convention="cmip6"
)
```

## Writing Earth Science Tests

### Unit Test Example

```python
@pytest.mark.unit
@pytest.mark.archive
def test_compressed_archive_netcdf_handling(create_compressed_archive, cache_manager):
    """Test CompressedArchive handles NetCDF files correctly."""
    # Create archive with NetCDF files
    archive_path = create_compressed_archive("climate_model_output")
    
    # Create CompressedArchive instance
    archive = CompressedArchive(
        archive_id="climate_test",
        archive_location=str(archive_path),
        cache_manager=cache_manager
    )
    
    # Test NetCDF-specific operations
    netcdf_files = archive.list_files(pattern="*.nc")
    assert len(netcdf_files) > 0
    
    # Test opening NetCDF files
    for filename in netcdf_files:
        file_obj = archive.open_file(filename)
        assert file_obj is not None
```

### Integration Test Example

```python
@pytest.mark.integration
@pytest.mark.earth_science
@pytest.mark.hpc
def test_complete_hpc_archive_workflow(multi_location_setup, create_model_archive):
    """Test complete HPC archiving workflow."""
    locations = multi_location_setup
    
    # Create simulation with HPC locations
    sim = Simulation("CESM2_coupled_run")
    sim.add_location(locations['hpc_scratch'], "scratch")
    sim.add_location(locations['hpc_work'], "work") 
    sim.add_location(locations['archive_tape'], "archive")
    
    # Create model output
    model_dir = create_model_archive("CESM2_output")
    
    # Test archival workflow: scratch -> work -> tape
    # ... detailed workflow testing
```

### Property-Based Test Example

```python
@given(
    models=st.lists(earth_science_models, min_size=1, max_size=5),
    experiments=st.lists(climate_experiments, min_size=1, max_size=3)
)
@settings(max_examples=20)
def test_model_ensemble_consistency(models, experiments):
    """Test ensemble organization remains consistent across model combinations."""
    for model in models:
        for experiment in experiments:
            archive_id = f"{model}_{experiment}_r1i1p1f1"
            # Verify archive ID follows conventions
            assert model in archive_id
            assert experiment in archive_id
```

## Performance Testing

### Benchmarking

```python
@pytest.mark.performance
@pytest.mark.benchmark
def test_cache_performance_benchmark(benchmark, large_dataset):
    """Benchmark cache operations."""
    def cache_operation():
        return cache_manager.cache_archive(large_dataset)
    
    result = benchmark(cache_operation)
    # Performance assertions...
```

### Memory Monitoring

```python
@pytest.mark.performance
@pytest.mark.large_data
def test_memory_efficient_processing(earth_science_temp_dir):
    """Test memory efficiency with large datasets."""
    initial_memory = get_memory_usage()
    
    # Process large archive
    process_large_archive(archive_path)
    
    peak_memory = get_memory_usage()
    memory_delta = peak_memory - initial_memory
    
    # Memory should not exceed reasonable bounds
    assert memory_delta < archive_size_mb * 2
```

## Continuous Integration

### Test Suites for CI

```bash
# Fast test suite (CI basic check)
pytest -m "unit and not slow" --maxfail=5

# Comprehensive test suite (nightly builds)
pytest -m "not large_data" --cov=tellus

# Performance regression testing
pytest -m performance --benchmark-only
```

### Environment-Specific Testing

```bash
# HPC environment tests
pytest -m hpc

# Network-dependent tests
pytest -m network

# Tests requiring Earth science libraries
pytest -m earth_science
```

## Best Practices

### 1. Test Data Management
- Use fixtures for creating realistic test data
- Clean up temporary files automatically
- Use appropriate file sizes for test performance

### 2. Earth Science Domain Knowledge
- Follow CMIP6 and other community conventions
- Test with realistic variable names and file structures
- Validate metadata and file format compliance

### 3. Performance Considerations
- Mark slow tests appropriately
- Use parallel execution for independent tests
- Monitor memory usage in large data tests

### 4. Error Handling
- Test network failures and timeouts
- Validate error messages are helpful
- Test edge cases specific to Earth science data

### 5. Reproducibility
- Use fixed random seeds where appropriate
- Document test data requirements
- Ensure tests work across different environments

## Troubleshooting

### Common Issues

1. **Missing Dependencies**: Install optional dependencies with `pixi install`
2. **Slow Tests**: Use `-m "not slow"` to skip time-consuming tests
3. **Memory Issues**: Run large data tests individually or with more memory
4. **NetCDF Issues**: Ensure netCDF4 and underlying libraries are properly installed

### Debug Mode

```bash
# Run with verbose output
pytest -v -s

# Debug specific test
pytest --pdb tests/test_archive_system.py::TestCacheManager::test_large_file_caching

# Show fixtures
pytest --fixtures
```

## Contributing

When adding new tests:

1. Use appropriate markers to categorize tests
2. Follow Earth science naming conventions
3. Add realistic test data scenarios
4. Document domain-specific requirements
5. Consider performance implications
6. Ensure tests work with optional dependencies

The test framework is designed to grow with the project while maintaining focus on Earth science research workflows and requirements.