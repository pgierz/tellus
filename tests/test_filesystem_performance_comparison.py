"""
Before/after performance comparison framework for PathSandboxedFileSystem.

This module provides comprehensive before/after performance comparison to quantify
the exact performance impact of the PathSandboxedFileSystem wrapper. It includes
detailed memory profiling, CPU analysis, and regression detection.

Key Features:
- Direct fsspec vs PathSandboxedFileSystem comparison
- Memory usage profiling with detailed allocation tracking  
- CPU overhead analysis
- I/O pattern performance impact assessment
- Statistical analysis of performance differences
- Performance regression detection
- Detailed reporting and visualization
"""

import cProfile
import gc
import io
import os
import pstats
import tempfile
import time
import tracemalloc
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from statistics import mean, median, stdev
from typing import Any, Callable, Dict, List, Optional, Tuple
from unittest.mock import Mock, patch

import fsspec
import psutil
import pytest

from tellus.location.sandboxed_filesystem import PathSandboxedFileSystem


@dataclass
class PerformanceMetrics:
    """Container for performance measurement results."""
    operation_name: str
    execution_time: float
    memory_usage_mb: float
    cpu_percent: float
    operations_per_second: float
    memory_allocations: int
    peak_memory_mb: float
    success_count: int
    error_count: int
    additional_stats: Dict[str, Any]


class DetailedProfiler:
    """Advanced profiler with memory tracking and CPU analysis."""
    
    def __init__(self):
        self.reset()
        
    def reset(self):
        """Reset profiler state."""
        self.measurements = {}
        self.memory_snapshots = []
        self.cpu_samples = []
        self.start_memory = self._get_memory_usage()
        self.process = psutil.Process(os.getpid())
        
    def _get_memory_usage(self) -> float:
        """Get current memory usage in MB."""
        return self.process.memory_info().rss / (1024 * 1024)
    
    def start_profiling(self, operation_name: str):
        """Start profiling an operation with memory tracking."""
        # Start memory tracing
        tracemalloc.start()
        
        # Record initial state
        self.measurements[operation_name] = {
            'start_time': time.perf_counter(),
            'start_memory': self._get_memory_usage(),
            'start_cpu': self.process.cpu_percent(),
            'success_count': 0,
            'error_count': 0,
            'memory_samples': [],
            'cpu_samples': [],
        }
        
    def sample_resources(self, operation_name: str):
        """Take a resource usage sample during operation."""
        if operation_name in self.measurements:
            self.measurements[operation_name]['memory_samples'].append(self._get_memory_usage())
            self.measurements[operation_name]['cpu_samples'].append(self.process.cpu_percent())
            
    def record_operation_result(self, operation_name: str, success: bool = True):
        """Record the result of an individual operation."""
        if operation_name in self.measurements:
            if success:
                self.measurements[operation_name]['success_count'] += 1
            else:
                self.measurements[operation_name]['error_count'] += 1
    
    def end_profiling(self, operation_name: str) -> PerformanceMetrics:
        """End profiling and return comprehensive metrics."""
        if operation_name not in self.measurements:
            raise ValueError(f"No profiling started for operation: {operation_name}")
        
        measurement = self.measurements[operation_name]
        
        # Calculate timing
        end_time = time.perf_counter()
        execution_time = end_time - measurement['start_time']
        
        # Get memory statistics
        end_memory = self._get_memory_usage()
        memory_delta = end_memory - measurement['start_memory']
        
        memory_samples = measurement['memory_samples']
        if memory_samples:
            peak_memory = max(memory_samples)
            avg_memory = mean(memory_samples)
        else:
            peak_memory = end_memory
            avg_memory = end_memory
        
        # Get CPU statistics
        end_cpu = self.process.cpu_percent()
        cpu_samples = measurement['cpu_samples']
        if cpu_samples:
            avg_cpu = mean(cpu_samples)
        else:
            avg_cpu = end_cpu
        
        # Get memory allocation statistics
        if tracemalloc.is_tracing():
            current_trace, peak_trace = tracemalloc.get_traced_memory()
            tracemalloc.stop()
            memory_allocations = peak_trace
        else:
            memory_allocations = 0
        
        # Calculate operations per second
        total_operations = measurement['success_count'] + measurement['error_count']
        ops_per_second = total_operations / execution_time if execution_time > 0 else 0
        
        return PerformanceMetrics(
            operation_name=operation_name,
            execution_time=execution_time,
            memory_usage_mb=memory_delta,
            cpu_percent=avg_cpu,
            operations_per_second=ops_per_second,
            memory_allocations=memory_allocations,
            peak_memory_mb=peak_memory,
            success_count=measurement['success_count'],
            error_count=measurement['error_count'],
            additional_stats={
                'avg_memory_mb': avg_memory,
                'end_memory_mb': end_memory,
                'memory_samples': len(memory_samples),
                'cpu_samples': len(cpu_samples),
                'peak_memory_trace': peak_trace if tracemalloc.is_tracing() else 0,
            }
        )


class PerformanceComparator:
    """Compare performance between direct fsspec and PathSandboxedFileSystem."""
    
    def __init__(self):
        self.profiler = DetailedProfiler()
        self.test_results = {}
        
    def create_test_environment(self, tmp_path: Path, file_count: int = 500) -> Tuple[Path, List[Path]]:
        """Create standardized test environment for comparison."""
        files_created = []
        
        # Create realistic file structure
        components = ['atm', 'ocn', 'lnd', 'ice']
        variables = ['temp', 'precip', 'wind', 'psl']
        frequencies = ['daily', 'monthly']
        years = range(2000, 2011)  # 11 years
        
        file_idx = 0
        for component in components:
            if file_idx >= file_count:
                break
                
            comp_dir = tmp_path / "model_data" / component
            comp_dir.mkdir(parents=True, exist_ok=True)
            
            for variable in variables:
                if file_idx >= file_count:
                    break
                    
                for frequency in frequencies:
                    if file_idx >= file_count:
                        break
                        
                    for year in years:
                        if file_idx >= file_count:
                            break
                        
                        filename = f"{variable}_{frequency}_{year}.nc"
                        filepath = comp_dir / filename
                        
                        # Create realistic file sizes (1-10KB for testing speed)
                        file_size = 1024 * (1 + (file_idx % 10))
                        filepath.write_bytes(b'climate_data' * (file_size // 12))
                        
                        files_created.append(filepath)
                        file_idx += 1
        
        return tmp_path, files_created
    
    def benchmark_operation(self, operation_name: str, operation_func: Callable, 
                          iterations: int = 100) -> PerformanceMetrics:
        """Benchmark a specific operation with detailed profiling."""
        self.profiler.reset()
        self.profiler.start_profiling(operation_name)
        
        # Perform operation multiple times for statistical significance
        for i in range(iterations):
            try:
                operation_func()
                self.profiler.record_operation_result(operation_name, success=True)
            except Exception as e:
                self.profiler.record_operation_result(operation_name, success=False)
            
            # Sample resources periodically
            if i % 10 == 0:
                self.profiler.sample_resources(operation_name)
        
        return self.profiler.end_profiling(operation_name)
    
    def compare_filesystem_operations(self, base_path: Path, test_files: List[Path]) -> Dict[str, Tuple[PerformanceMetrics, PerformanceMetrics]]:
        """Compare operations between direct fsspec and PathSandboxedFileSystem."""
        direct_fs = fsspec.filesystem('file')
        sandboxed_fs = PathSandboxedFileSystem(direct_fs, str(base_path))
        
        test_paths = [str(f.relative_to(base_path)) for f in test_files[:100]]  # Limit for performance
        comparisons = {}
        
        # Test 1: File existence checks
        def direct_exists():
            for path in test_paths[:20]:  # Subset for speed
                direct_fs.exists(str(base_path / path))
        
        def sandboxed_exists():
            for path in test_paths[:20]:
                sandboxed_fs.exists(path)
        
        direct_metrics = self.benchmark_operation("direct_exists", direct_exists, iterations=50)
        sandboxed_metrics = self.benchmark_operation("sandboxed_exists", sandboxed_exists, iterations=50)
        comparisons["exists"] = (direct_metrics, sandboxed_metrics)
        
        # Test 2: File info operations
        def direct_info():
            for path in test_paths[:10]:  # Smaller subset
                try:
                    direct_fs.info(str(base_path / path))
                except Exception:
                    pass
        
        def sandboxed_info():
            for path in test_paths[:10]:
                try:
                    sandboxed_fs.info(path)
                except Exception:
                    pass
        
        direct_metrics = self.benchmark_operation("direct_info", direct_info, iterations=30)
        sandboxed_metrics = self.benchmark_operation("sandboxed_info", sandboxed_info, iterations=30)
        comparisons["info"] = (direct_metrics, sandboxed_metrics)
        
        # Test 3: Directory listing
        def direct_ls():
            direct_fs.ls(str(base_path / "model_data"), detail=False)
        
        def sandboxed_ls():
            sandboxed_fs.ls("model_data", detail=False)
        
        direct_metrics = self.benchmark_operation("direct_ls", direct_ls, iterations=100)
        sandboxed_metrics = self.benchmark_operation("sandboxed_ls", sandboxed_ls, iterations=100)
        comparisons["ls"] = (direct_metrics, sandboxed_metrics)
        
        # Test 4: Pattern matching
        def direct_glob():
            direct_fs.glob(str(base_path / "model_data/**/*.nc"))
        
        def sandboxed_glob():
            sandboxed_fs.glob("model_data/**/*.nc")
        
        direct_metrics = self.benchmark_operation("direct_glob", direct_glob, iterations=20)
        sandboxed_metrics = self.benchmark_operation("sandboxed_glob", sandboxed_glob, iterations=20)
        comparisons["glob"] = (direct_metrics, sandboxed_metrics)
        
        # Test 5: File reading
        test_file_path = test_paths[0] if test_paths else "model_data/atm/temp_daily_2000.nc"
        
        def direct_read():
            try:
                direct_fs.read_bytes(str(base_path / test_file_path))
            except Exception:
                pass
        
        def sandboxed_read():
            try:
                sandboxed_fs.read_bytes(test_file_path)
            except Exception:
                pass
        
        direct_metrics = self.benchmark_operation("direct_read", direct_read, iterations=50)
        sandboxed_metrics = self.benchmark_operation("sandboxed_read", sandboxed_read, iterations=50)
        comparisons["read"] = (direct_metrics, sandboxed_metrics)
        
        return comparisons
    
    def analyze_performance_impact(self, comparisons: Dict[str, Tuple[PerformanceMetrics, PerformanceMetrics]]) -> Dict[str, Dict[str, float]]:
        """Analyze performance impact and calculate overhead percentages."""
        analysis = {}
        
        for operation, (direct_metrics, sandboxed_metrics) in comparisons.items():
            # Calculate overheads
            time_overhead = ((sandboxed_metrics.execution_time - direct_metrics.execution_time) / 
                            direct_metrics.execution_time) * 100 if direct_metrics.execution_time > 0 else 0
            
            memory_overhead = sandboxed_metrics.memory_usage_mb - direct_metrics.memory_usage_mb
            
            cpu_overhead = sandboxed_metrics.cpu_percent - direct_metrics.cpu_percent
            
            ops_impact = ((direct_metrics.operations_per_second - sandboxed_metrics.operations_per_second) / 
                         direct_metrics.operations_per_second) * 100 if direct_metrics.operations_per_second > 0 else 0
            
            # Statistical significance (simplified)
            performance_ratio = sandboxed_metrics.execution_time / direct_metrics.execution_time if direct_metrics.execution_time > 0 else 1
            
            analysis[operation] = {
                'time_overhead_percent': time_overhead,
                'memory_overhead_mb': memory_overhead,
                'cpu_overhead_percent': cpu_overhead,
                'throughput_impact_percent': ops_impact,
                'performance_ratio': performance_ratio,
                'direct_ops_per_sec': direct_metrics.operations_per_second,
                'sandboxed_ops_per_sec': sandboxed_metrics.operations_per_second,
                'direct_memory_mb': direct_metrics.memory_usage_mb,
                'sandboxed_memory_mb': sandboxed_metrics.memory_usage_mb,
                'success_rate_direct': direct_metrics.success_count / max(1, direct_metrics.success_count + direct_metrics.error_count),
                'success_rate_sandboxed': sandboxed_metrics.success_count / max(1, sandboxed_metrics.success_count + sandboxed_metrics.error_count),
            }
        
        return analysis


@pytest.fixture
def performance_comparator():
    """Provide performance comparator instance."""
    return PerformanceComparator()


@pytest.fixture
def standard_test_environment(tmp_path):
    """Provide standardized test environment for comparison."""
    comparator = PerformanceComparator()
    return comparator.create_test_environment(tmp_path, file_count=300)


@pytest.mark.performance
@pytest.mark.benchmark
class TestPerformanceComparison:
    """Comprehensive before/after performance comparison tests."""
    
    def test_comprehensive_operation_comparison(self, performance_comparator, standard_test_environment):
        """Compare all major filesystem operations comprehensively."""
        base_path, test_files = standard_test_environment
        
        # Run comprehensive comparison
        comparisons = performance_comparator.compare_filesystem_operations(base_path, test_files)
        
        # Analyze performance impact
        analysis = performance_comparator.analyze_performance_impact(comparisons)
        
        # Performance assertions
        for operation, metrics in analysis.items():
            time_overhead = metrics['time_overhead_percent']
            memory_overhead = metrics['memory_overhead_mb']
            throughput_impact = metrics['throughput_impact_percent']
            
            # Core performance requirements
            assert time_overhead < 20.0, f"{operation} time overhead {time_overhead:.2f}% exceeds 20%"
            assert memory_overhead < 50.0, f"{operation} memory overhead {memory_overhead:.1f}MB exceeds 50MB"
            assert throughput_impact < 25.0, f"{operation} throughput impact {throughput_impact:.2f}% exceeds 25%"
            
            # Success rates should be equivalent
            success_diff = abs(metrics['success_rate_direct'] - metrics['success_rate_sandboxed'])
            assert success_diff < 0.05, f"{operation} success rates differ by {success_diff:.2f}"
        
        # Overall performance summary
        avg_time_overhead = mean([m['time_overhead_percent'] for m in analysis.values()])
        avg_memory_overhead = mean([m['memory_overhead_mb'] for m in analysis.values()])
        max_time_overhead = max([m['time_overhead_percent'] for m in analysis.values()])
        
        print(f"\nComprehensive Performance Comparison:")
        print(f"  Average time overhead: {avg_time_overhead:.2f}%")
        print(f"  Maximum time overhead: {max_time_overhead:.2f}%")
        print(f"  Average memory overhead: {avg_memory_overhead:.1f}MB")
        
        for operation, metrics in analysis.items():
            print(f"  {operation}:")
            print(f"    Time overhead: {metrics['time_overhead_percent']:.2f}%")
            print(f"    Memory overhead: {metrics['memory_overhead_mb']:.1f}MB")
            print(f"    Throughput: {metrics['direct_ops_per_sec']:.1f} -> {metrics['sandboxed_ops_per_sec']:.1f} ops/sec")
        
        # Global performance requirement
        assert avg_time_overhead < 10.0, f"Average time overhead {avg_time_overhead:.2f}% exceeds 10% target"
        
        return analysis
    
    def test_concurrent_performance_comparison(self, performance_comparator, standard_test_environment):
        """Compare performance under concurrent load."""
        base_path, test_files = standard_test_environment
        
        direct_fs = fsspec.filesystem('file')
        sandboxed_fs = PathSandboxedFileSystem(direct_fs, str(base_path))
        
        test_paths = [str(f.relative_to(base_path)) for f in test_files[:50]]
        
        def concurrent_direct_worker():
            for path in test_paths:
                direct_fs.exists(str(base_path / path))
        
        def concurrent_sandboxed_worker():
            for path in test_paths:
                sandboxed_fs.exists(path)
        
        # Benchmark concurrent operations
        with ThreadPoolExecutor(max_workers=4) as executor:
            direct_metrics = performance_comparator.benchmark_operation(
                "concurrent_direct", 
                lambda: list(executor.map(lambda _: concurrent_direct_worker(), range(4))),
                iterations=10
            )
        
        with ThreadPoolExecutor(max_workers=4) as executor:
            sandboxed_metrics = performance_comparator.benchmark_operation(
                "concurrent_sandboxed",
                lambda: list(executor.map(lambda _: concurrent_sandboxed_worker(), range(4))),
                iterations=10
            )
        
        # Analyze concurrent performance
        time_overhead = ((sandboxed_metrics.execution_time - direct_metrics.execution_time) / 
                        direct_metrics.execution_time) * 100
        
        memory_overhead = sandboxed_metrics.memory_usage_mb - direct_metrics.memory_usage_mb
        
        # Concurrent performance assertions
        assert time_overhead < 30.0, f"Concurrent time overhead {time_overhead:.2f}% exceeds 30%"
        assert memory_overhead < 100.0, f"Concurrent memory overhead {memory_overhead:.1f}MB exceeds 100MB"
        
        print(f"\nConcurrent Performance Comparison:")
        print(f"  Direct concurrent time: {direct_metrics.execution_time:.3f}s")
        print(f"  Sandboxed concurrent time: {sandboxed_metrics.execution_time:.3f}s")
        print(f"  Concurrent time overhead: {time_overhead:.2f}%")
        print(f"  Concurrent memory overhead: {memory_overhead:.1f}MB")
    
    def test_memory_allocation_analysis(self, performance_comparator, standard_test_environment):
        """Detailed memory allocation analysis."""
        base_path, test_files = standard_test_environment
        
        direct_fs = fsspec.filesystem('file')
        sandboxed_fs = PathSandboxedFileSystem(direct_fs, str(base_path))
        
        test_paths = [str(f.relative_to(base_path)) for f in test_files[:30]]
        
        # Memory allocation comparison for different operations
        def direct_memory_test():
            for path in test_paths:
                direct_fs.exists(str(base_path / path))
                direct_fs.isfile(str(base_path / path))
        
        def sandboxed_memory_test():
            for path in test_paths:
                sandboxed_fs.exists(path)
                sandboxed_fs.isfile(path)
        
        direct_metrics = performance_comparator.benchmark_operation("direct_memory", direct_memory_test, iterations=20)
        sandboxed_metrics = performance_comparator.benchmark_operation("sandboxed_memory", sandboxed_memory_test, iterations=20)
        
        # Memory allocation analysis
        direct_allocs = direct_metrics.memory_allocations
        sandboxed_allocs = sandboxed_metrics.memory_allocations
        
        allocation_overhead = sandboxed_allocs - direct_allocs
        allocation_ratio = sandboxed_allocs / max(1, direct_allocs)
        
        # Memory allocation assertions
        assert allocation_ratio < 2.0, f"Memory allocation ratio {allocation_ratio:.2f} too high"
        
        peak_memory_diff = sandboxed_metrics.peak_memory_mb - direct_metrics.peak_memory_mb
        assert peak_memory_diff < 50.0, f"Peak memory difference {peak_memory_diff:.1f}MB too high"
        
        print(f"\nMemory Allocation Analysis:")
        print(f"  Direct allocations: {direct_allocs:,} bytes")
        print(f"  Sandboxed allocations: {sandboxed_allocs:,} bytes")
        print(f"  Allocation overhead: {allocation_overhead:,} bytes")
        print(f"  Allocation ratio: {allocation_ratio:.2f}x")
        print(f"  Peak memory difference: {peak_memory_diff:.1f}MB")
    
    def test_statistical_performance_analysis(self, performance_comparator, standard_test_environment):
        """Statistical analysis of performance differences over multiple runs."""
        base_path, test_files = standard_test_environment
        
        direct_fs = fsspec.filesystem('file')
        sandboxed_fs = PathSandboxedFileSystem(direct_fs, str(base_path))
        
        test_paths = [str(f.relative_to(base_path)) for f in test_files[:20]]
        
        # Collect multiple performance samples
        direct_times = []
        sandboxed_times = []
        
        for run in range(20):  # 20 independent runs
            # Direct filesystem
            start_time = time.perf_counter()
            for path in test_paths:
                direct_fs.exists(str(base_path / path))
            direct_times.append(time.perf_counter() - start_time)
            
            # Sandboxed filesystem
            start_time = time.perf_counter()
            for path in test_paths:
                sandboxed_fs.exists(path)
            sandboxed_times.append(time.perf_counter() - start_time)
        
        # Statistical analysis
        direct_mean = mean(direct_times)
        sandboxed_mean = mean(sandboxed_times)
        
        direct_stdev = stdev(direct_times) if len(direct_times) > 1 else 0
        sandboxed_stdev = stdev(sandboxed_times) if len(sandboxed_times) > 1 else 0
        
        direct_median = median(direct_times)
        sandboxed_median = median(sandboxed_times)
        
        # Calculate confidence intervals (simplified)
        mean_overhead = ((sandboxed_mean - direct_mean) / direct_mean) * 100
        median_overhead = ((sandboxed_median - direct_median) / direct_median) * 100
        
        # Statistical assertions
        assert mean_overhead < 15.0, f"Mean overhead {mean_overhead:.2f}% exceeds 15%"
        assert median_overhead < 15.0, f"Median overhead {median_overhead:.2f}% exceeds 15%"
        
        # Consistency check (low variance indicates consistent performance)
        cv_direct = (direct_stdev / direct_mean) * 100 if direct_mean > 0 else 0
        cv_sandboxed = (sandboxed_stdev / sandboxed_mean) * 100 if sandboxed_mean > 0 else 0
        
        assert cv_sandboxed < 50.0, f"Sandboxed performance variance {cv_sandboxed:.1f}% too high"
        
        print(f"\nStatistical Performance Analysis ({len(direct_times)} runs):")
        print(f"  Direct mean: {direct_mean:.4f}s (±{direct_stdev:.4f})")
        print(f"  Sandboxed mean: {sandboxed_mean:.4f}s (±{sandboxed_stdev:.4f})")
        print(f"  Mean overhead: {mean_overhead:.2f}%")
        print(f"  Median overhead: {median_overhead:.2f}%")
        print(f"  Direct CV: {cv_direct:.1f}%")
        print(f"  Sandboxed CV: {cv_sandboxed:.1f}%")


@pytest.mark.performance
class TestRegressionDetection:
    """Performance regression detection and baseline establishment."""
    
    def test_performance_baseline_establishment(self, performance_comparator, standard_test_environment):
        """Establish performance baselines for regression testing."""
        base_path, test_files = standard_test_environment
        
        # Run comprehensive comparison to establish baselines
        comparisons = performance_comparator.compare_filesystem_operations(base_path, test_files)
        analysis = performance_comparator.analyze_performance_impact(comparisons)
        
        # Define baseline performance targets (these would be stored/versioned in real use)
        baseline_targets = {
            'exists': {'max_time_overhead': 10.0, 'max_memory_overhead': 10.0},
            'info': {'max_time_overhead': 15.0, 'max_memory_overhead': 15.0},
            'ls': {'max_time_overhead': 5.0, 'max_memory_overhead': 5.0},
            'glob': {'max_time_overhead': 20.0, 'max_memory_overhead': 20.0},
            'read': {'max_time_overhead': 8.0, 'max_memory_overhead': 8.0},
        }
        
        # Check against baselines
        baseline_report = {}
        regression_detected = False
        
        for operation, targets in baseline_targets.items():
            if operation in analysis:
                metrics = analysis[operation]
                time_overhead = metrics['time_overhead_percent']
                memory_overhead = metrics['memory_overhead_mb']
                
                time_regression = time_overhead > targets['max_time_overhead']
                memory_regression = memory_overhead > targets['max_memory_overhead']
                
                baseline_report[operation] = {
                    'time_overhead': time_overhead,
                    'memory_overhead': memory_overhead,
                    'time_target': targets['max_time_overhead'],
                    'memory_target': targets['max_memory_overhead'],
                    'time_regression': time_regression,
                    'memory_regression': memory_regression,
                }
                
                if time_regression or memory_regression:
                    regression_detected = True
        
        # Report baseline comparison
        print(f"\nPerformance Baseline Comparison:")
        print(f"  Regression detected: {regression_detected}")
        
        for operation, report in baseline_report.items():
            status = "PASS"
            if report['time_regression'] or report['memory_regression']:
                status = "FAIL"
            
            print(f"  {operation}: {status}")
            print(f"    Time: {report['time_overhead']:.2f}% (target: {report['time_target']:.2f}%)")
            print(f"    Memory: {report['memory_overhead']:.1f}MB (target: {report['memory_target']:.1f}MB)")
        
        # Assert no significant regressions
        critical_operations = ['exists', 'ls']  # Most frequently used operations
        for op in critical_operations:
            if op in baseline_report:
                assert not baseline_report[op]['time_regression'], f"Critical regression in {op} time performance"
                assert not baseline_report[op]['memory_regression'], f"Critical regression in {op} memory usage"
        
        return baseline_report


def generate_performance_comparison_report(analysis: Dict[str, Dict[str, float]]) -> str:
    """Generate comprehensive performance comparison report."""
    report_lines = [
        "PathSandboxedFileSystem Performance Comparison Report",
        "=" * 60,
        "",
        "EXECUTIVE SUMMARY:",
    ]
    
    # Calculate summary statistics
    overheads = [m['time_overhead_percent'] for m in analysis.values()]
    avg_overhead = mean(overheads)
    max_overhead = max(overheads)
    
    memory_overheads = [m['memory_overhead_mb'] for m in analysis.values()]
    avg_memory_overhead = mean(memory_overheads)
    max_memory_overhead = max(memory_overheads)
    
    report_lines.extend([
        f"  Average time overhead: {avg_overhead:.2f}%",
        f"  Maximum time overhead: {max_overhead:.2f}%",
        f"  Average memory overhead: {avg_memory_overhead:.1f}MB",
        f"  Maximum memory overhead: {max_memory_overhead:.1f}MB",
        "",
        "DETAILED ANALYSIS:",
    ])
    
    for operation, metrics in analysis.items():
        report_lines.extend([
            f"  {operation.upper()}:",
            f"    Time overhead: {metrics['time_overhead_percent']:.2f}%",
            f"    Memory overhead: {metrics['memory_overhead_mb']:.1f}MB",
            f"    Direct throughput: {metrics['direct_ops_per_sec']:.1f} ops/sec", 
            f"    Sandboxed throughput: {metrics['sandboxed_ops_per_sec']:.1f} ops/sec",
            f"    Performance ratio: {metrics['performance_ratio']:.2f}x",
            "",
        ])
    
    # Performance assessment
    report_lines.extend([
        "PERFORMANCE ASSESSMENT:",
        "✓ PASS" if avg_overhead < 10 else "✗ FAIL - Average overhead too high",
        "✓ PASS" if max_overhead < 20 else "✗ FAIL - Maximum overhead too high", 
        "✓ PASS" if avg_memory_overhead < 50 else "✗ FAIL - Memory overhead too high",
        "",
        "RECOMMENDATIONS:",
        "1. Monitor long-running operations for memory leaks",
        "2. Consider path caching for repeated operations",
        "3. Optimize critical path validation logic",
        "4. Implement lazy validation where safe",
    ])
    
    return "\n".join(report_lines)


if __name__ == "__main__":
    print("PathSandboxedFileSystem Performance Comparison Suite")
    print("Run with: pytest -m benchmark tests/test_filesystem_performance_comparison.py")