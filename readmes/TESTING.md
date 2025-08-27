# Testing Guide

## Quick Start

Run the standard test suite (recommended for development):

```bash
pixi run test
```

This runs 135 tests and skips 19 performance tests for faster development cycles.

## Test Categories

### Regular Tests (Default)
- **Unit tests**: Individual component tests
- **Integration tests**: Component interaction tests  
- **CLI tests**: Command-line interface tests
- **Location tests**: Multi-location functionality
- **Archive tests**: Archive system functionality
- **Simulation tests**: Simulation management tests

### Performance Tests (Optional)
Performance tests create large datasets (up to 7GB) and can take 30+ minutes to complete:

```bash
# Run only performance tests
pixi run test-performance

# Run all tests including performance tests
pixi run test-all
```

## Test Configuration

Performance tests are **skipped by default** to keep development cycles fast. This is configured in `pyproject.toml`:

```toml
[tool.pytest.ini_options]
addopts = [
    "-m", "not performance"  # Skip performance tests by default
]

markers = [
    "performance: Performance tests for large datasets (skipped by default - use -m performance to run)",
    # ... other markers
]
```

## Manual Test Selection

You can also manually select test categories:

```bash
# Run only unit tests
pixi run test -m unit

# Run integration tests only  
pixi run test -m integration

# Run specific test categories
pixi run test -m "unit or integration"

# Run everything except performance
pixi run test -m "not performance"  # (this is the default)

# Run performance tests only
pixi run test -m performance
```

## Performance Test Details

Performance tests include:
- **Large archive caching**: Tests with ~700MB datasets
- **Concurrent cache access**: Multi-threaded cache operations
- **Archive scanning**: Processing 1000+ file archives (~7GB)
- **Memory usage patterns**: Testing memory efficiency
- **Benchmark operations**: Timing critical operations

These tests ensure the system works efficiently with realistic Earth science datasets but are too slow for regular development workflows.

## Tips

- Use `pixi run test` for regular development
- Run `pixi run test-performance` before releases
- Use `pixi run test-all` for comprehensive testing
- Performance tests require significant disk space and time