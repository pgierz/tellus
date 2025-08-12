# PathSandboxedFileSystem Performance Testing Suite

## Overview

This comprehensive performance testing suite validates that the PathSandboxedFileSystem security wrapper maintains acceptable performance for real-world HPC climate science workloads. The suite ensures the security fix doesn't impact typical Earth System Model data operations.

## Performance Requirements

- **< 5% overhead** for individual file operations
- **< 10% overhead** for bulk operations  
- **< 15% overhead** for concurrent access
- **Sub-second response** for metadata queries
- **Memory usage proportional** to working set, not total dataset size
- **Linear scaling** up to 8 concurrent processes/threads

## Test Suite Components

### 1. HPC Climate Performance Tests (`test_hpc_climate_performance.py`)

**Purpose**: Core performance validation for realistic climate data scenarios

**Key Features**:
- CMIP6, reanalysis, and ensemble data structure simulation
- MPI-style distributed processing simulation  
- Interactive analysis performance validation
- Time series extraction workflow testing

**Test Categories**:
- `TestHPCClimatePerformance`: Core climate science workflows
- `TestScalabilityLimits`: Extreme condition testing
- `TestMemoryStabilityUnderLoad`: Memory leak detection

**Usage**:
```bash
# Run HPC climate performance tests
pixi run -e test pytest -m "hpc and performance" tests/test_hpc_climate_performance.py

# Run with specific concurrency levels
pixi run -e test pytest -k "ensemble_simulation" --timeout=300
```

### 2. Parallel Stress Testing (`test_parallel_stress_performance.py`)

**Purpose**: Intensive stress testing for parallel file operations

**Key Features**:
- Thread pool scaling validation
- Process pool MPI simulation
- Mixed read/write contention testing
- Fault tolerance under stress
- Sustained load stability testing

**Test Categories**:
- `TestParallelStressPerformance`: Core parallel stress scenarios  
- `TestMemoryStabilityUnderLoad`: Memory stability validation

**Usage**:
```bash
# Run parallel stress tests
pixi run -e test pytest -m "hpc and benchmark" tests/test_parallel_stress_performance.py

# Test specific concurrency patterns
pixi run -e test pytest -k "thread_pool" --timeout=180
```

### 3. Memory & CPU Profiling (`test_memory_cpu_profiling.py`)

**Purpose**: Detailed memory allocation and CPU performance profiling

**Key Features**:
- Advanced memory allocation tracking
- CPU instruction efficiency analysis
- Real NetCDF/Zarr dataset processing
- NUMA locality assessment
- Cache behavior analysis

**Test Categories**:
- `TestMemoryCPUProfiling`: Core memory/CPU analysis

**Usage**:
```bash
# Run memory/CPU profiling tests
pixi run -e test pytest -m "benchmark and large_data" tests/test_memory_cpu_profiling.py

# Profile specific operations
pixi run -e test pytest -k "netcdf_operation" --timeout=300
```

### 4. Regression Testing Framework (`test_performance_regression_framework.py`)

**Purpose**: Statistical regression detection and baseline management

**Key Features**:
- Performance baseline establishment
- Statistical significance testing (Welch's t-test, Mann-Whitney U)
- Trend analysis and regression detection
- Confidence interval calculations
- CI/CD integration hooks

**Test Categories**:
- `TestPerformanceRegressionFramework`: Framework validation
- `TestPathSandboxedFileSystemRegressionSuite`: Comprehensive regression testing

**Usage**:
```bash
# Run regression framework tests
pixi run -e test pytest -m "benchmark and hpc" tests/test_performance_regression_framework.py

# Establish new baselines
pixi run -e test pytest -k "baseline_establishment"
```

### 5. Optimization Recommendations (`test_optimization_recommendations.py`)

**Purpose**: Performance analysis and actionable optimization recommendations

**Key Features**:
- Bottleneck identification and analysis
- Path resolution optimization opportunities
- Memory management improvement suggestions
- Concurrency scaling recommendations
- Performance budget analysis

**Test Categories**:
- `TestPerformanceOptimizationAnalysis`: Comprehensive optimization analysis

**Usage**:
```bash
# Run optimization analysis
pixi run -e test pytest -m "benchmark and performance" tests/test_optimization_recommendations.py
```

## Running the Complete Suite

### Quick Performance Validation
```bash
# Essential performance tests (< 10 minutes)
pixi run -e test pytest -m "performance and not slow" tests/test_*performance*.py
```

### Comprehensive Performance Testing
```bash
# Full performance suite (30+ minutes)
pixi run test-performance
```

### CI/CD Integration
```bash
# Performance regression detection
pixi run -e test pytest -m "benchmark" --timeout=600 tests/test_performance_regression_framework.py
```

## Test Data Generation

The suite includes realistic climate data generators:

- **CMIP6 Structure**: Multi-institutional model data
- **Reanalysis Data**: ERA5/MERRA-2 style datasets  
- **Ensemble Data**: Weather/climate ensemble simulations
- **NetCDF Files**: Realistic file sizes and structures
- **Directory Hierarchies**: Deep nested structures typical in climate science

## Performance Metrics Tracked

### Core Metrics
- **Execution Time**: Wall-clock time for operations
- **Throughput**: Operations/files processed per second
- **Memory Usage**: Peak memory, allocation patterns, fragmentation
- **CPU Efficiency**: CPU time vs wall time, instruction efficiency
- **Concurrency Scaling**: Efficiency at different thread/process counts
- **Error Rates**: Operation failure rates under load

### HPC-Specific Metrics
- **NUMA Locality**: Memory access patterns
- **Cache Efficiency**: L1/L2/L3 cache hit rates
- **Network I/O**: For distributed filesystems
- **GC Pressure**: Garbage collection frequency and impact
- **Context Switches**: System-level efficiency metrics

## Expected Performance Characteristics

### Baseline Performance (Single Thread)
- **File existence checks**: > 100 ops/sec
- **File info queries**: > 50 ops/sec  
- **Directory listings**: < 0.1s for 100s of files
- **Pattern matching (glob)**: < 2.0s for complex patterns
- **Path resolution**: < 2ms per path

### Concurrent Performance (4-8 threads)
- **Scaling efficiency**: > 70% for 4 threads, > 50% for 8 threads
- **Error rate**: < 5% under normal load
- **Memory overhead**: < 100MB additional for 8 threads
- **Context switch rate**: < 100/sec per thread

### Memory Characteristics
- **Allocation efficiency**: > 80% useful memory
- **Fragmentation index**: < 0.3
- **Memory growth**: < 50MB per 1000 operations
- **GC collections**: < 5 per 100 operations

## Troubleshooting Performance Issues

### Common Performance Problems

1. **High Path Resolution Overhead**
   - **Symptoms**: High CPU usage, slow file operations
   - **Investigation**: Run path analysis tests
   - **Solutions**: Path caching, pattern optimization

2. **Memory Growth/Leaks**
   - **Symptoms**: Increasing memory usage over time
   - **Investigation**: Run memory profiling tests
   - **Solutions**: Object pooling, explicit cleanup

3. **Poor Concurrency Scaling**
   - **Symptoms**: No improvement with more threads
   - **Investigation**: Run parallel stress tests
   - **Solutions**: Lock-free algorithms, fine-grained locking

4. **High Error Rates**
   - **Symptoms**: Operation failures under load
   - **Investigation**: Run fault tolerance tests
   - **Solutions**: Retry mechanisms, better error handling

### Performance Debugging Commands

```bash
# Identify bottlenecks
pixi run -e test pytest -k "path_resolution_analysis" -v -s

# Check memory patterns  
pixi run -e test pytest -k "memory_allocation_patterns" -v -s

# Analyze concurrency issues
pixi run -e test pytest -k "concurrent_access" -v -s

# Generate optimization report
pixi run -e test pytest -k "optimization_recommendations" -v -s
```

## Performance Baselines

Performance baselines are automatically managed by the regression testing framework:

- **Storage**: `~/.tellus/performance_baselines/`
- **Format**: JSON with statistical properties
- **Versioning**: Semantic versioning for baseline compatibility
- **Updates**: Automatic updates when performance improves consistently

### Managing Baselines

```bash
# List existing baselines
python -c "from test_performance_regression_framework import BaselineManager; print(BaselineManager().list_baselines())"

# Reset baselines (after major optimizations)
rm -rf ~/.tellus/performance_baselines/
```

## Continuous Integration

The performance suite integrates with CI/CD pipelines:

### GitHub Actions Integration
```yaml
- name: Run Performance Tests
  run: pixi run test-performance

- name: Performance Regression Check
  run: pixi run -e test pytest -m benchmark --timeout=600
  continue-on-error: true  # Don't fail builds on performance issues

- name: Generate Performance Report
  run: |
    pixi run -e test pytest -k optimization_recommendations -v -s > performance_report.txt
    cat performance_report.txt
```

### Performance Budgets
The suite enforces performance budgets:
- **Critical Regressions**: > 25% performance degradation
- **Major Regressions**: 15-25% performance degradation  
- **Minor Regressions**: 5-15% performance degradation

## Contributing Performance Tests

### Adding New Test Cases

1. **Identify Performance Scenario**: What specific HPC/climate workflow?
2. **Choose Test Category**: Which test module fits best?
3. **Create Realistic Data**: Use ClimateDataGenerator for test data
4. **Measure Both Direct and Sandboxed**: Always compare performance
5. **Set Appropriate Thresholds**: Based on real-world requirements
6. **Add Performance Markers**: Use `@pytest.mark.performance`

### Example New Test
```python
@pytest.mark.performance
@pytest.mark.hpc
def test_zarr_array_access_performance(self, profiler, zarr_dataset):
    """Test Zarr array access performance for large climate datasets."""
    base_path, zarr_stores = zarr_dataset
    
    direct_fs = fsspec.filesystem('file')
    sandboxed_fs = PathSandboxedFileSystem(direct_fs, str(base_path))
    
    # Your test implementation...
    
    assert overhead_percentage < 10.0, "Zarr access overhead too high"
```

### Performance Test Guidelines

1. **Realistic Workloads**: Test patterns actually used in climate science
2. **Statistical Rigor**: Multiple measurements, proper statistical analysis
3. **Clear Assertions**: Specific performance requirements
4. **Comprehensive Coverage**: Memory, CPU, concurrency, I/O patterns
5. **Maintainable Tests**: Clear, documented, not overly complex

## Related Documentation

- [`SECURITY_TEST_SUMMARY.md`](SECURITY_TEST_SUMMARY.md): Security testing overview
- [`CLAUDE.md`](../CLAUDE.md): General development guidance  
- [`HACKING.md`](../HACKING.md): Development environment setup
- Project documentation: `docs/performance/`

## Support

For performance testing questions:
1. Check existing test output and reports
2. Review performance baselines and trends  
3. Run optimization analysis for specific recommendations
4. Consult HPC performance best practices documentation