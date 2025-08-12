# Tellus Integration Testing Suite

This directory contains comprehensive integration tests for the tellus Earth science data archive system. The tests are designed to validate cross-system integration, concurrency handling, error recovery, performance, and system reliability in research computing environments.

## Overview

The tellus system operates in complex research environments with:
- Multiple storage backends (local filesystem, SSH, S3, tape archives)
- Concurrent scientific workflows
- Large datasets and intermittent network connectivity
- HPC/cluster computing requirements
- Caching and progress tracking systems

This integration testing suite validates these scenarios comprehensively.

## Test Structure

### Core Test Modules

#### `conftest.py`
Central configuration and fixtures for all integration tests:
- **Test environment setup** with temporary workspaces
- **Mock network conditions** for simulating connectivity issues
- **Storage backend mocks** for different protocols
- **Performance and resource monitoring** utilities
- **Concurrent execution** support with thread pool management
- **Error injection** framework for fault testing

#### `test_concurrency_races.py`
Tests for concurrent operations and race conditions:
- **Concurrent location operations** - simultaneous location creation/access
- **Cache thread safety** - concurrent cache reads/writes/cleanup
- **Archive access concurrency** - parallel archive file operations
- **Simulation registry operations** - concurrent simulation management
- **High concurrency scenarios** - stress testing with many simultaneous operations

#### `test_error_recovery.py`
Error handling and recovery scenarios:
- **Network failure recovery** - timeouts, intermittent failures, partitions
- **Storage failure recovery** - disk full, permissions, corruption
- **Cache recovery** - corruption detection, index repair, cleanup failures
- **Circuit breaker patterns** - fault tolerance and graceful degradation
- **Error propagation** - context preservation through call stacks

#### `test_load_performance.py`
Performance testing under various load conditions:
- **Concurrent load scenarios** - high simultaneous access patterns
- **Scalability limits** - large archives, many files, system limits
- **Resource constraints** - low memory, disk space, bandwidth limitations
- **Performance regression** - baseline metrics and stress testing

#### `test_system_reliability.py`
System reliability and monitoring:
- **Health checks** - location accessibility, cache status, archive integrity
- **Monitoring and observability** - operation tracing, metrics collection, alerting
- **Recovery scenarios** - automatic repair, graceful degradation, leak detection

## Test Categories and Markers

Tests are organized using pytest markers:

- `@pytest.mark.integration` - All integration tests
- `@pytest.mark.concurrency` - Concurrency and thread-safety tests
- `@pytest.mark.performance` - Performance and load tests
- `@pytest.mark.slow` - Long-running tests (typically skipped in CI)
- `@pytest.mark.network` - Tests requiring network simulation

## Running Tests

### Basic Integration Tests
```bash
# Run all integration tests (excluding slow tests)
pytest tests/integration/ -m "not slow"

# Run specific test categories
pytest tests/integration/ -m "concurrency"
pytest tests/integration/ -m "performance and not slow"
```

### Extended Testing
```bash
# Run all tests including slow ones
pytest tests/integration/ 

# Run with specific concurrency limits
pytest tests/integration/ --maxfail=5 -n 4

# Run with custom timeouts
pytest tests/integration/ --timeout=300
```

### Environment-Specific Testing
```bash
# Set environment configuration
export TELLUS_TEST_CONFIG=hpc_cluster
export TELLUS_TIMEOUT_MULTIPLIER=3
export TELLUS_CONCURRENT_LIMIT=2

pytest tests/integration/ --env-config=hpc_cluster
```

## Test Configuration

### Environment Variables

- `TELLUS_TEST_CONFIG` - Test environment configuration (local, ci, hpc_cluster)
- `TELLUS_TIMEOUT_MULTIPLIER` - Timeout multiplier for slow environments
- `TELLUS_CONCURRENT_LIMIT` - Maximum concurrent operations
- `TELLUS_CACHE_DIR` - Test cache directory override
- `TELLUS_CONFIG_DIR` - Test configuration directory override

### Test Data Management

The test suite uses generated test data to avoid large file dependencies:

```python
# Sample usage in tests
def test_with_sample_data(sample_archive_data, temp_workspace):
    # sample_archive_data is a fixture providing realistic archive content
    archive_path = temp_workspace / "test.tar.gz"
    archive_path.write_bytes(sample_archive_data)
    
    # Test archive operations
    archive = CompressedArchive("test", str(archive_path))
    files = archive.list_files()
    assert len(files) > 0
```

## Key Testing Patterns

### 1. Concurrent Operation Testing

```python
def test_concurrent_operations(concurrent_executor, progress_tracker):
    def worker(worker_id):
        # Perform operations that might conflict
        operation_id = f"worker_{worker_id}"
        progress_tracker.track_operation(operation_id, "test_operation")
        
        try:
            # Your concurrent operation here
            result = perform_operation()
            progress_tracker.complete_operation(operation_id, True)
            return result
        except Exception as e:
            progress_tracker.complete_operation(operation_id, False, str(e))
            raise
    
    # Submit concurrent tasks
    futures = [concurrent_executor.submit(worker, i) for i in range(10)]
    results = [future.result() for future in futures]
    
    # Verify all operations completed successfully
    stats = progress_tracker.get_stats()
    assert stats['successful_operations'] == 10
```

### 2. Error Injection and Recovery

```python
def test_error_recovery(error_injection, archive):
    # Configure error injection
    error_injection.inject_at('operation_target', ConnectionError, 
                            "Network unavailable", after_calls=2)
    
    # Test operation with retry logic
    max_retries = 5
    for attempt in range(max_retries):
        try:
            result = archive.some_operation()
            break  # Success
        except ConnectionError:
            if attempt < max_retries - 1:
                time.sleep(0.1 * (2 ** attempt))  # Exponential backoff
                continue
            raise
    
    # Verify eventual success
    assert result is not None
```

### 3. Performance Monitoring

```python
def test_performance_monitoring(performance_monitor, resource_monitor):
    resource_monitor.take_snapshot("start")
    
    with performance_monitor.time_operation("test_operation"):
        # Perform operation to be timed
        perform_expensive_operation()
    
    resource_monitor.take_snapshot("end")
    
    # Analyze performance
    stats = performance_monitor.get_stats("test_operation")
    assert stats['avg_time'] < acceptable_threshold
    
    # Check for resource leaks
    assert not resource_monitor.check_memory_leak(50)  # 50MB threshold
```

### 4. System Health Validation

```python
def test_system_health(cache_manager, test_locations):
    # Check component health
    cache_stats = cache_manager.get_cache_stats()
    assert cache_stats['cache_accessible']
    
    # Validate location connectivity
    for name, location in test_locations.items():
        try:
            accessible = location.fs.exists("/")
            assert accessible, f"Location {name} should be accessible"
        except Exception as e:
            pytest.fail(f"Location {name} health check failed: {e}")
```

## CI/CD Integration

See `ci_cd_strategies.md` for detailed CI/CD pipeline integration including:

- Multi-environment testing configurations
- Distributed test execution for HPC environments
- Performance regression detection
- Test result monitoring and alerting

## Troubleshooting

### Common Issues

1. **Timeout Errors in Slow Environments**
   ```bash
   # Increase timeout multiplier
   export TELLUS_TIMEOUT_MULTIPLIER=5
   pytest tests/integration/ --timeout=600
   ```

2. **Resource Constraints**
   ```bash
   # Reduce concurrent operations
   export TELLUS_CONCURRENT_LIMIT=2
   pytest tests/integration/ -m "not performance"
   ```

3. **Network Simulation Issues**
   ```bash
   # Skip network-dependent tests
   pytest tests/integration/ -m "not network"
   ```

4. **Large File Test Failures**
   ```bash
   # Skip slow/large file tests
   pytest tests/integration/ -m "not slow"
   ```

### Debug Mode

Enable verbose output for troubleshooting:

```bash
# Verbose output with detailed tracebacks
pytest tests/integration/ -v --tb=long

# Show test execution details
pytest tests/integration/ -s --log-cli-level=DEBUG

# Capture performance metrics
pytest tests/integration/ --benchmark-only
```

## Contributing

When adding new integration tests:

1. **Use appropriate markers** to categorize tests
2. **Follow naming conventions** (test_*, descriptive names)
3. **Include docstrings** explaining test purpose and scenarios
4. **Use provided fixtures** for consistency
5. **Add performance assertions** where relevant
6. **Test error conditions** alongside happy paths
7. **Document complex test scenarios** in comments

### Example Test Template

```python
@pytest.mark.integration
@pytest.mark.concurrency
def test_new_concurrent_feature(concurrent_executor, progress_tracker, 
                               performance_monitor):
    """Test description explaining what is being validated.
    
    This test validates [specific scenario] under [conditions].
    Expected behavior: [description]
    """
    # Test implementation
    performance_monitor.start_timing("new_feature_test")
    
    def worker(worker_id):
        operation_id = f"new_feature_{worker_id}"
        progress_tracker.track_operation(operation_id, "new_feature")
        
        try:
            # Test logic here
            result = test_new_feature()
            progress_tracker.complete_operation(operation_id, True)
            return result
        except Exception as e:
            progress_tracker.complete_operation(operation_id, False, str(e))
            raise
    
    # Execute test
    futures = [concurrent_executor.submit(worker, i) for i in range(5)]
    results = [future.result() for future in futures]
    
    # Verify results
    assert len(results) == 5
    stats = progress_tracker.get_stats()
    assert stats['successful_operations'] == 5
    
    # Performance validation
    duration = performance_monitor.end_timing("new_feature_test")
    assert duration < expected_threshold
```

This integration testing suite provides comprehensive validation of the tellus system's behavior in realistic research computing scenarios, ensuring reliability and performance under various operational conditions.