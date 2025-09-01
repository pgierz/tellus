"""
Global test configuration and fixtures for tellus.

This module provides shared fixtures and configuration for all tests,
with special focus on Earth science workflows and data scenarios.
"""

import shutil
import tempfile
from functools import wraps
from pathlib import Path

import pytest

# Import core fixtures
from tellus.testing.fixtures import sample_simulation_awi_locations_with_laptop

# Import Earth science specific fixtures (with error handling for numpy/netCDF4 compatibility)
try:
    from .fixtures.earth_science import (create_compressed_archive,
                                         create_model_archive,
                                         create_netcdf_file,
                                         earth_science_file_patterns,
                                         earth_science_temp_dir,
                                         hpc_environment_config,
                                         multi_location_setup,
                                         realistic_file_sizes,
                                         sample_climate_netcdf_data,
                                         sample_model_archive_structure)
    EARTH_SCIENCE_AVAILABLE = True
except (ImportError, RuntimeWarning) as e:
    # Handle netCDF4/NumPy compatibility issues
    print(f"Warning: Earth science fixtures unavailable due to: {e}")
    EARTH_SCIENCE_AVAILABLE = False
    # Create placeholder fixtures
    earth_science_temp_dir = None
    sample_climate_netcdf_data = None
    create_netcdf_file = None
    sample_model_archive_structure = None
    create_model_archive = None
    create_compressed_archive = None
    multi_location_setup = None
    earth_science_file_patterns = None
    realistic_file_sizes = None
    hpc_environment_config = None

# Import test utilities (with error handling)
try:
    from .utils.earth_science_helpers import (EarthScienceArchiveAnalyzer,
                                              EarthScienceFileValidator,
                                              EarthScienceTestAssertions)
except (ImportError, RuntimeWarning):
    # Create placeholder classes if imports fail
    class EarthScienceFileValidator:
        pass
    class EarthScienceArchiveAnalyzer:
        pass  
    class EarthScienceTestAssertions:
        pass

# Error recovery fixtures are provided by integration/conftest.py

# Re-export fixtures to make them available to all tests
__all__ = [
    "sample_simulation_awi_locations_with_laptop",
    "earth_science_temp_dir",
    "sample_climate_netcdf_data",
    "create_netcdf_file",
    "create_model_archive",
    "create_compressed_archive",
    "multi_location_setup",
    "earth_science_file_patterns",
    "realistic_file_sizes",
    "hpc_environment_config",
]


def trio_test(func):
    """Decorator to run async tests with Trio."""

    @wraps(func)
    def wrapper(*args, **kwargs):
        return trio.run(func, *args, **kwargs)

    return wrapper


@pytest.fixture(scope="session")
def earth_science_validator():
    """Provide Earth science file validator for tests."""
    return EarthScienceFileValidator()


@pytest.fixture(scope="session")
def earth_science_assertions():
    """Provide Earth science test assertions."""
    return EarthScienceTestAssertions()


@pytest.fixture
def archive_analyzer():
    """Create archive analyzer for a test."""

    def _create_analyzer(archive_path: Path):
        return EarthScienceArchiveAnalyzer(archive_path)

    return _create_analyzer


# Test markers configuration
def pytest_configure(config):
    """Configure custom pytest markers."""
    config.addinivalue_line("markers", "unit: Unit tests for individual components")
    config.addinivalue_line(
        "markers", "integration: Integration tests for component interactions"
    )
    config.addinivalue_line(
        "markers", "performance: Performance tests for large datasets"
    )
    config.addinivalue_line(
        "markers", "property: Property-based tests for data integrity"
    )
    config.addinivalue_line(
        "markers", "earth_science: Tests specific to Earth science workflows"
    )
    config.addinivalue_line("markers", "archive: Tests for archive system components")
    config.addinivalue_line("markers", "cache: Tests for caching functionality")
    config.addinivalue_line(
        "markers", "location: Tests for multi-location functionality"
    )
    config.addinivalue_line("markers", "slow: Tests that take more than 30 seconds")
    config.addinivalue_line("markers", "network: Tests that require network access")
    config.addinivalue_line(
        "markers", "large_data: Tests that work with large datasets (>100MB)"
    )
    config.addinivalue_line("markers", "hpc: Tests for HPC environment scenarios")


def pytest_collection_modifyitems(config, items):
    """Modify test collection to add automatic markers."""
    for item in items:
        # Auto-mark slow tests
        if "slow" in item.keywords or "performance" in item.keywords:
            item.add_marker(pytest.mark.slow)

        # Auto-mark tests that use large data
        if "large_data" in item.keywords or any(
            "large" in fixture for fixture in item.fixturenames
        ):
            item.add_marker(pytest.mark.large_data)

        # Auto-mark Earth science tests based on filename or fixtures
        if (
            "earth_science" in str(item.fspath)
            or any("earth_science" in fixture for fixture in item.fixturenames)
            or any("climate" in fixture for fixture in item.fixturenames)
            or any("netcdf" in fixture for fixture in item.fixturenames)
        ):
            item.add_marker(pytest.mark.earth_science)


# Cleanup fixture for temporary files
@pytest.fixture(autouse=True)
def cleanup_temp_files():
    """Automatically cleanup temporary files after each test."""
    temp_dirs = []

    def track_temp_dir(temp_dir):
        temp_dirs.append(temp_dir)
        return temp_dir

    yield track_temp_dir

    # Cleanup after test
    for temp_dir in temp_dirs:
        if isinstance(temp_dir, (str, Path)) and Path(temp_dir).exists():
            try:
                shutil.rmtree(temp_dir, ignore_errors=True)
            except Exception:
                pass  # Best effort cleanup


# Skip conditions for optional dependencies
def pytest_runtest_setup(item):
    """Setup hook to skip tests based on missing dependencies."""
    # Skip NetCDF tests if netCDF4 not available
    if item.get_closest_marker("netcdf"):
        try:
            import netCDF4
        except ImportError:
            pytest.skip("netCDF4 not available")

    # Skip hypothesis tests if hypothesis not available
    if item.get_closest_marker("property"):
        try:
            import hypothesis
        except ImportError:
            pytest.skip("hypothesis not available")

    # Skip xarray tests if xarray not available
    if item.get_closest_marker("xarray"):
        try:
            import xarray
        except ImportError:
            pytest.skip("xarray not available")
