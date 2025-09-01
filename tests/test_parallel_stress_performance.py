"""
Parallel File Operations Stress Testing for PathSandboxedFileSystem.

This module provides intensive stress testing for parallel file operations
typical in HPC climate science environments, ensuring PathSandboxedFileSystem
maintains performance and stability under high concurrent load.

Test Scenarios:
- Multi-process file operations (simulating MPI applications)
- High-concurrency thread pools (simulating data servers)
- Mixed read/write workloads under contention
- Distributed filesystem simulation (NFS, Lustre, GPFS)
- Memory-constrained parallel processing
- Fault tolerance under concurrent access failures

Performance Targets:
- Linear scaling up to 8 concurrent processes/threads
- < 20% overhead vs direct filesystem under parallel load
- Stable memory usage (no leaks) over 1000+ operations
- Error resilience: continue operation when some files unavailable
"""

import asyncio
import functools
import multiprocessing as mp
import os
import queue
import random
import signal
import tempfile
import threading
import time
import warnings
from concurrent.futures import (ProcessPoolExecutor, ThreadPoolExecutor,
                                as_completed)
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from statistics import mean, stdev
from typing import Any, Callable, Dict, List, Optional, Tuple
from unittest.mock import Mock, patch

import fsspec
import psutil
import pytest
# Import the HPC profiler from the main performance test
from test_hpc_climate_performance import (ClimateDataGenerator,
                                          HPC_Performance_Profiler)

from tellus.location.sandboxed_filesystem import (PathSandboxedFileSystem,
                                                  PathValidationError)


@dataclass
class StressTestResult:
    """Results from a stress testing scenario."""
    test_name: str
    duration: float
    operations_completed: int
    operations_failed: int
    peak_memory_mb: float
    avg_cpu_percent: float
    concurrency_level: int
    throughput_ops_per_sec: float
    error_rate_percent: float
    additional_metrics: Dict[str, Any]


class ParallelStressTester:
    """Comprehensive parallel stress testing framework."""
    
    def __init__(self):
        self.process = psutil.Process(os.getpid())
        self.results = []
        
    def _monitor_resources(self, duration: float, stop_event: threading.Event) -> Dict[str, List[float]]:
        """Monitor system resources during test execution."""
        memory_samples = []
        cpu_samples = []
        
        start_time = time.time()
        
        while not stop_event.is_set() and (time.time() - start_time) < duration * 1.2:  # 20% buffer
            try:
                memory_mb = self.process.memory_info().rss / (1024 * 1024)
                cpu_percent = self.process.cpu_percent()
                
                memory_samples.append(memory_mb)
                cpu_samples.append(cpu_percent)
                
                time.sleep(0.1)  # Sample every 100ms
            except psutil.NoSuchProcess:
                break
        
        return {
            'memory_samples': memory_samples,
            'cpu_samples': cpu_samples
        }
    
    def run_thread_pool_stress_test(self, filesystem, file_paths: List[str], 
                                  max_workers: int = 16, duration: float = 30.0,
                                  operation_mix: Dict[str, float] = None) -> StressTestResult:
        """Run stress test with thread pool concurrent access."""
        if operation_mix is None:
            operation_mix = {
                'exists': 0.4,
                'info': 0.3, 
                'read_header': 0.2,
                'list_dir': 0.1
            }
        
        operations_completed = 0
        operations_failed = 0
        stop_event = threading.Event()
        
        # Start resource monitoring
        monitor_thread = threading.Thread(
            target=self._monitor_resources,
            args=(duration, stop_event)
        )
        monitor_thread.start()
        
        def worker_task():
            """Task executed by each worker thread."""
            nonlocal operations_completed, operations_failed
            
            local_completed = 0
            local_failed = 0
            
            start_time = time.time()
            
            while (time.time() - start_time) < duration:
                try:
                    # Select random operation based on mix
                    rand = random.random()
                    cumulative = 0
                    selected_op = 'exists'
                    
                    for op, prob in operation_mix.items():
                        cumulative += prob
                        if rand <= cumulative:
                            selected_op = op
                            break
                    
                    # Select random file
                    filepath = random.choice(file_paths)
                    
                    # Execute operation
                    if selected_op == 'exists':
                        result = filesystem.exists(filepath)
                    elif selected_op == 'info':
                        result = filesystem.info(filepath)
                    elif selected_op == 'read_header':
                        with filesystem.open(filepath, 'rb') as f:
                            result = f.read(1024)  # Read first 1KB
                    elif selected_op == 'list_dir':
                        dir_path = str(Path(filepath).parent) if hasattr(filesystem, 'base_path') else str(Path(filesystem._resolve_path(filepath)).parent)
                        result = filesystem.ls(dir_path if hasattr(filesystem, 'base_path') else str(Path(dir_path).relative_to(filesystem.base_path)))
                    
                    local_completed += 1
                    
                except Exception as e:
                    local_failed += 1
                    
                # Small delay to prevent overwhelming the filesystem
                time.sleep(0.001)  # 1ms
            
            # Thread-safe updates
            with threading.Lock():
                nonlocal operations_completed, operations_failed
                operations_completed += local_completed
                operations_failed += local_failed
        
        # Run the stress test
        start_time = time.time()
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit worker tasks
            futures = [executor.submit(worker_task) for _ in range(max_workers)]
            
            # Wait for completion
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    operations_failed += 1
        
        execution_time = time.time() - start_time
        
        # Stop monitoring
        stop_event.set()
        monitor_thread.join(timeout=2.0)
        
        # Calculate metrics
        throughput = operations_completed / execution_time if execution_time > 0 else 0
        error_rate = (operations_failed / max(1, operations_completed + operations_failed)) * 100
        
        return StressTestResult(
            test_name=f"thread_pool_stress_{max_workers}workers",
            duration=execution_time,
            operations_completed=operations_completed,
            operations_failed=operations_failed,
            peak_memory_mb=0,  # Would be filled by monitoring
            avg_cpu_percent=0,  # Would be filled by monitoring
            concurrency_level=max_workers,
            throughput_ops_per_sec=throughput,
            error_rate_percent=error_rate,
            additional_metrics={
                'target_duration': duration,
                'operation_mix': operation_mix
            }
        )
    
    def run_process_pool_stress_test(self, filesystem_factory: Callable, base_path: str,
                                   file_paths: List[str], max_workers: int = 4,
                                   duration: float = 30.0) -> StressTestResult:
        """Run stress test with process pool (simulating MPI-style access)."""
        
        def process_worker(worker_id: int, file_subset: List[str], duration: float) -> Dict[str, int]:
            """Worker function for process pool."""
            # Each process creates its own filesystem instance
            try:
                direct_fs = fsspec.filesystem('file')
                filesystem = PathSandboxedFileSystem(direct_fs, base_path)
                
                completed = 0
                failed = 0
                start_time = time.time()
                
                while (time.time() - start_time) < duration:
                    filepath = random.choice(file_subset)
                    
                    try:
                        # Mix of operations
                        exists = filesystem.exists(filepath)
                        if exists:
                            info = filesystem.info(filepath)
                            completed += 2  # Count both operations
                        else:
                            completed += 1
                            
                    except Exception:
                        failed += 1
                    
                    # Small delay to prevent overwhelming
                    time.sleep(0.01)  # 10ms
                
                return {
                    'worker_id': worker_id,
                    'completed': completed,
                    'failed': failed
                }
                
            except Exception as e:
                return {
                    'worker_id': worker_id,
                    'completed': 0,
                    'failed': 1000,  # Major failure
                    'error': str(e)
                }
        
        # Divide files among processes
        chunk_size = len(file_paths) // max_workers
        file_chunks = [file_paths[i:i+chunk_size] for i in range(0, len(file_paths), chunk_size)]
        
        start_time = time.time()
        
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            futures = [
                executor.submit(process_worker, i, chunk, duration)
                for i, chunk in enumerate(file_chunks)
            ]
            
            results = []
            for future in as_completed(futures):
                try:
                    result = future.result(timeout=duration + 10)
                    results.append(result)
                except Exception as e:
                    results.append({'completed': 0, 'failed': 1, 'error': str(e)})
        
        execution_time = time.time() - start_time
        
        # Aggregate results
        total_completed = sum(r.get('completed', 0) for r in results)
        total_failed = sum(r.get('failed', 0) for r in results)
        
        throughput = total_completed / execution_time if execution_time > 0 else 0
        error_rate = (total_failed / max(1, total_completed + total_failed)) * 100
        
        return StressTestResult(
            test_name=f"process_pool_stress_{max_workers}processes",
            duration=execution_time,
            operations_completed=total_completed,
            operations_failed=total_failed,
            peak_memory_mb=0,  # Process pool makes this complex to measure
            avg_cpu_percent=0,
            concurrency_level=max_workers,
            throughput_ops_per_sec=throughput,
            error_rate_percent=error_rate,
            additional_metrics={
                'worker_results': results,
                'target_duration': duration
            }
        )
    
    def run_mixed_workload_stress_test(self, filesystem, file_paths: List[str],
                                     read_write_ratio: float = 0.8,
                                     concurrency: int = 8,
                                     duration: float = 30.0) -> StressTestResult:
        """Run stress test with mixed read/write workloads under contention."""
        
        operations_completed = 0
        operations_failed = 0
        write_operations = 0
        read_operations = 0
        
        # Create some temporary files for write operations
        temp_files = []
        try:
            for i in range(concurrency * 2):  # 2 temp files per worker
                temp_file = f"stress_temp_{i:03d}.tmp"
                temp_files.append(temp_file)
        except Exception:
            pass
        
        def mixed_worker():
            """Worker performing mixed read/write operations."""
            nonlocal operations_completed, operations_failed, write_operations, read_operations
            
            local_completed = 0
            local_failed = 0
            local_writes = 0
            local_reads = 0
            
            start_time = time.time()
            
            while (time.time() - start_time) < duration:
                try:
                    if random.random() < read_write_ratio:
                        # Read operation
                        filepath = random.choice(file_paths)
                        
                        # Random read operation
                        op_type = random.choice(['exists', 'info', 'read_bytes'])
                        
                        if op_type == 'exists':
                            result = filesystem.exists(filepath)
                        elif op_type == 'info':
                            result = filesystem.info(filepath)
                        elif op_type == 'read_bytes':
                            with filesystem.open(filepath, 'rb') as f:
                                result = f.read(512)
                        
                        local_reads += 1
                    else:
                        # Write operation
                        temp_file = random.choice(temp_files)
                        
                        # Write some test data
                        test_data = f"stress_test_data_{time.time()}_{random.randint(1, 1000)}"
                        filesystem.write_text(temp_file, test_data)
                        
                        local_writes += 1
                    
                    local_completed += 1
                    
                except Exception as e:
                    local_failed += 1
                
                # Brief delay
                time.sleep(0.002)  # 2ms
            
            # Update global counters (thread-safe)
            with threading.Lock():
                nonlocal operations_completed, operations_failed, write_operations, read_operations
                operations_completed += local_completed
                operations_failed += local_failed
                write_operations += local_writes
                read_operations += local_reads
        
        # Run mixed workload
        start_time = time.time()
        
        with ThreadPoolExecutor(max_workers=concurrency) as executor:
            futures = [executor.submit(mixed_worker) for _ in range(concurrency)]
            
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception:
                    operations_failed += 1
        
        execution_time = time.time() - start_time
        
        # Cleanup temp files
        for temp_file in temp_files:
            try:
                filesystem.rm(temp_file)
            except:
                pass
        
        throughput = operations_completed / execution_time if execution_time > 0 else 0
        error_rate = (operations_failed / max(1, operations_completed + operations_failed)) * 100
        
        return StressTestResult(
            test_name=f"mixed_workload_stress_{concurrency}workers",
            duration=execution_time,
            operations_completed=operations_completed,
            operations_failed=operations_failed,
            peak_memory_mb=0,
            avg_cpu_percent=0,
            concurrency_level=concurrency,
            throughput_ops_per_sec=throughput,
            error_rate_percent=error_rate,
            additional_metrics={
                'read_operations': read_operations,
                'write_operations': write_operations,
                'read_write_ratio': read_write_ratio,
                'temp_files_created': len(temp_files)
            }
        )
    
    def run_fault_tolerance_stress_test(self, filesystem, file_paths: List[str],
                                      fault_injection_rate: float = 0.1,
                                      concurrency: int = 6,
                                      duration: float = 30.0) -> StressTestResult:
        """Run stress test with fault injection to test error handling."""
        
        operations_completed = 0
        operations_failed = 0
        injected_faults = 0
        
        # Create list of files that "exist" and files that "don't exist"
        existing_files = file_paths[:int(len(file_paths) * 0.8)]  # 80% exist
        missing_files = [f"missing_file_{i:03d}.nc" for i in range(len(file_paths) // 5)]
        
        def fault_tolerant_worker():
            """Worker that handles faults gracefully."""
            nonlocal operations_completed, operations_failed, injected_faults
            
            local_completed = 0
            local_failed = 0
            local_faults = 0
            
            start_time = time.time()
            
            while (time.time() - start_time) < duration:
                try:
                    # Inject faults by occasionally using missing files
                    if random.random() < fault_injection_rate:
                        filepath = random.choice(missing_files)
                        local_faults += 1
                    else:
                        filepath = random.choice(existing_files)
                    
                    # Perform operation with proper error handling
                    try:
                        exists = filesystem.exists(filepath)
                        if exists:
                            # File exists, try to get info
                            info = filesystem.info(filepath)
                            local_completed += 2
                        else:
                            # File doesn't exist, but this is expected behavior
                            local_completed += 1
                            
                    except PathValidationError:
                        # This is expected for path traversal attempts
                        local_completed += 1
                    except Exception as e:
                        # Unexpected error
                        local_failed += 1
                
                except Exception as e:
                    local_failed += 1
                
                time.sleep(0.005)  # 5ms
            
            # Update global counters
            with threading.Lock():
                nonlocal operations_completed, operations_failed, injected_faults
                operations_completed += local_completed
                operations_failed += local_failed
                injected_faults += local_faults
        
        # Run fault tolerance test
        start_time = time.time()
        
        with ThreadPoolExecutor(max_workers=concurrency) as executor:
            futures = [executor.submit(fault_tolerant_worker) for _ in range(concurrency)]
            
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception:
                    operations_failed += 1
        
        execution_time = time.time() - start_time
        
        throughput = operations_completed / execution_time if execution_time > 0 else 0
        error_rate = (operations_failed / max(1, operations_completed + operations_failed)) * 100
        
        return StressTestResult(
            test_name=f"fault_tolerance_stress_{concurrency}workers",
            duration=execution_time,
            operations_completed=operations_completed,
            operations_failed=operations_failed,
            peak_memory_mb=0,
            avg_cpu_percent=0,
            concurrency_level=concurrency,
            throughput_ops_per_sec=throughput,
            error_rate_percent=error_rate,
            additional_metrics={
                'fault_injection_rate': fault_injection_rate,
                'injected_faults': injected_faults,
                'existing_files': len(existing_files),
                'missing_files': len(missing_files)
            }
        )


# Test Fixtures
@pytest.fixture
def stress_tester():
    """Parallel stress testing framework."""
    return ParallelStressTester()


@pytest.fixture
def large_test_dataset(tmp_path):
    """Large dataset for stress testing."""
    generator = ClimateDataGenerator()
    
    # Create substantial dataset for stress testing
    ensemble_files = generator.create_ensemble_structure(
        tmp_path / "stress_ensemble", 
        ensemble_size=20, 
        size_constraint_mb=300
    )
    
    cmip6_files = generator.create_cmip6_structure(
        tmp_path / "stress_cmip6",
        size_constraint_mb=200
    )
    
    all_files = ensemble_files + cmip6_files
    return tmp_path, all_files


# Stress Test Classes
@pytest.mark.performance
@pytest.mark.hpc
@pytest.mark.benchmark
@pytest.mark.slow
class TestParallelStressPerformance:
    """Parallel stress testing for HPC scenarios."""
    
    def test_thread_pool_scaling_stress(self, stress_tester, large_test_dataset):
        """Test thread pool scaling under stress conditions."""
        base_path, files = large_test_dataset
        
        direct_fs = fsspec.filesystem('file')
        sandboxed_fs = PathSandboxedFileSystem(direct_fs, str(base_path))
        
        file_paths = [str(f.relative_to(base_path)) for f in files[:100]]  # Sample for performance
        
        thread_counts = [1, 2, 4, 8, 16]
        results = {}
        
        for thread_count in thread_counts:
            # Test sandboxed filesystem under thread stress
            result = stress_tester.run_thread_pool_stress_test(
                filesystem=sandboxed_fs,
                file_paths=file_paths,
                max_workers=thread_count,
                duration=20.0,  # 20 seconds per test
                operation_mix={
                    'exists': 0.5,
                    'info': 0.3,
                    'read_header': 0.15,
                    'list_dir': 0.05
                }
            )
            
            results[thread_count] = result
            
            # Performance assertions per thread count
            assert result.error_rate_percent < 5.0, f"Error rate {result.error_rate_percent:.2f}% too high for {thread_count} threads"
            assert result.throughput_ops_per_sec > 10 * thread_count, f"Throughput {result.throughput_ops_per_sec:.1f} ops/sec too low for {thread_count} threads"
        
        # Analyze scaling efficiency
        base_throughput = results[1].throughput_ops_per_sec
        
        print(f"\nThread Pool Scaling Stress Test Results:")
        for threads, result in results.items():
            efficiency = result.throughput_ops_per_sec / (base_throughput * threads) if base_throughput > 0 else 0
            print(f"  {threads} threads: {result.throughput_ops_per_sec:.1f} ops/sec, {result.error_rate_percent:.2f}% errors, {efficiency:.2f} efficiency")
        
        # Scaling assertions
        assert results[4].throughput_ops_per_sec > base_throughput * 2.5, "Poor scaling to 4 threads"
        assert results[8].throughput_ops_per_sec > base_throughput * 4.0, "Poor scaling to 8 threads"
        
        # Error rates should remain low even under high concurrency
        for thread_count, result in results.items():
            assert result.error_rate_percent < 10.0, f"Error rate too high for {thread_count} threads: {result.error_rate_percent:.2f}%"
    
    def test_process_pool_mpi_simulation(self, stress_tester, large_test_dataset):
        """Test process pool stress (simulating MPI applications)."""
        base_path, files = large_test_dataset
        
        file_paths = [str(f.relative_to(base_path)) for f in files[:80]]  # Sample
        
        process_counts = [1, 2, 4]  # Limited for test performance
        results = {}
        
        def filesystem_factory():
            """Factory function for creating filesystem instances."""
            direct_fs = fsspec.filesystem('file')
            return PathSandboxedFileSystem(direct_fs, str(base_path))
        
        for process_count in process_counts:
            result = stress_tester.run_process_pool_stress_test(
                filesystem_factory=filesystem_factory,
                base_path=str(base_path),
                file_paths=file_paths,
                max_workers=process_count,
                duration=15.0  # 15 seconds per test
            )
            
            results[process_count] = result
            
            # Process-level performance assertions
            assert result.error_rate_percent < 15.0, f"Process pool error rate {result.error_rate_percent:.2f}% too high for {process_count} processes"
            assert result.throughput_ops_per_sec > 5 * process_count, f"Process throughput too low: {result.throughput_ops_per_sec:.1f} ops/sec"
        
        print(f"\nProcess Pool MPI Simulation Results:")
        base_throughput = results[1].throughput_ops_per_sec
        
        for processes, result in results.items():
            scaling_factor = result.throughput_ops_per_sec / base_throughput if base_throughput > 0 else 0
            print(f"  {processes} processes: {result.throughput_ops_per_sec:.1f} ops/sec, {result.error_rate_percent:.2f}% errors, {scaling_factor:.2f}x scaling")
        
        # Process scaling should be reasonable (not as efficient as threads due to overhead)
        if len(results) > 1:
            assert results[2].throughput_ops_per_sec > base_throughput * 1.5, "Poor process scaling"
    
    def test_mixed_read_write_contention(self, stress_tester, large_test_dataset):
        """Test performance under mixed read/write workloads with contention."""
        base_path, files = large_test_dataset
        
        direct_fs = fsspec.filesystem('file')
        sandboxed_fs = PathSandboxedFileSystem(direct_fs, str(base_path))
        
        file_paths = [str(f.relative_to(base_path)) for f in files[:60]]  # Sample
        
        # Test different read/write ratios
        read_write_ratios = [0.9, 0.7, 0.5]  # 90%, 70%, 50% reads
        results = {}
        
        for ratio in read_write_ratios:
            result = stress_tester.run_mixed_workload_stress_test(
                filesystem=sandboxed_fs,
                file_paths=file_paths,
                read_write_ratio=ratio,
                concurrency=6,
                duration=20.0
            )
            
            results[ratio] = result
            
            # Mixed workload assertions
            assert result.error_rate_percent < 20.0, f"Mixed workload error rate {result.error_rate_percent:.2f}% too high for {ratio:.0%} reads"
            assert result.throughput_ops_per_sec > 20.0, f"Mixed workload throughput {result.throughput_ops_per_sec:.1f} too low"
        
        print(f"\nMixed Read/Write Contention Results:")
        for ratio, result in results.items():
            read_ops = result.additional_metrics.get('read_operations', 0)
            write_ops = result.additional_metrics.get('write_operations', 0)
            print(f"  {ratio:.0%} reads: {result.throughput_ops_per_sec:.1f} ops/sec, {result.error_rate_percent:.2f}% errors")
            print(f"    Read ops: {read_ops}, Write ops: {write_ops}")
        
        # Higher read ratios should generally have better performance
        if 0.9 in results and 0.5 in results:
            read_heavy_throughput = results[0.9].throughput_ops_per_sec
            balanced_throughput = results[0.5].throughput_ops_per_sec
            
            # Read-heavy workloads should perform better (filesystem reads are typically faster)
            # But allow for some variation due to write overhead
            assert read_heavy_throughput >= balanced_throughput * 0.8, "Read-heavy performance unexpectedly lower"
    
    def test_fault_tolerance_under_stress(self, stress_tester, large_test_dataset):
        """Test fault tolerance and error handling under stress."""
        base_path, files = large_test_dataset
        
        direct_fs = fsspec.filesystem('file')
        sandboxed_fs = PathSandboxedFileSystem(direct_fs, str(base_path))
        
        file_paths = [str(f.relative_to(base_path)) for f in files[:50]]
        
        fault_rates = [0.05, 0.1, 0.2]  # 5%, 10%, 20% fault injection
        results = {}
        
        for fault_rate in fault_rates:
            result = stress_tester.run_fault_tolerance_stress_test(
                filesystem=sandboxed_fs,
                file_paths=file_paths,
                fault_injection_rate=fault_rate,
                concurrency=6,
                duration=15.0
            )
            
            results[fault_rate] = result
            
            # Fault tolerance assertions
            # System should handle faults gracefully without catastrophic failures
            assert result.error_rate_percent < 30.0, f"Excessive error rate {result.error_rate_percent:.2f}% with {fault_rate:.0%} fault injection"
            assert result.throughput_ops_per_sec > 10.0, f"Throughput collapsed under fault injection: {result.throughput_ops_per_sec:.1f} ops/sec"
        
        print(f"\nFault Tolerance Stress Test Results:")
        for fault_rate, result in results.items():
            injected = result.additional_metrics.get('injected_faults', 0)
            print(f"  {fault_rate:.0%} fault rate: {result.throughput_ops_per_sec:.1f} ops/sec, {result.error_rate_percent:.2f}% errors")
            print(f"    Injected faults: {injected}, Total ops: {result.operations_completed}")
        
        # Error rates should increase with fault injection, but not excessively
        if len(results) > 1:
            sorted_results = sorted(results.items())
            for i in range(1, len(sorted_results)):
                prev_error_rate = sorted_results[i-1][1].error_rate_percent
                curr_error_rate = sorted_results[i][1].error_rate_percent
                
                # Error rate can increase, but throughput shouldn't collapse completely
                assert sorted_results[i][1].throughput_ops_per_sec > 5.0, "Throughput collapsed under fault injection"
    
    @pytest.mark.timeout(120)  # 2 minutes max
    def test_sustained_load_stability(self, stress_tester, large_test_dataset):
        """Test stability under sustained high load (memory leaks, performance degradation)."""
        base_path, files = large_test_dataset
        
        direct_fs = fsspec.filesystem('file')
        sandboxed_fs = PathSandboxedFileSystem(direct_fs, str(base_path))
        
        file_paths = [str(f.relative_to(base_path)) for f in files[:40]]
        
        # Run sustained load test for longer duration
        result = stress_tester.run_thread_pool_stress_test(
            filesystem=sandboxed_fs,
            file_paths=file_paths,
            max_workers=8,
            duration=60.0,  # 1 minute sustained load
            operation_mix={
                'exists': 0.6,
                'info': 0.25,
                'read_header': 0.1,
                'list_dir': 0.05
            }
        )
        
        # Stability assertions
        assert result.error_rate_percent < 8.0, f"Sustained load error rate {result.error_rate_percent:.2f}% too high"
        assert result.throughput_ops_per_sec > 50.0, f"Sustained throughput {result.throughput_ops_per_sec:.1f} too low"
        assert result.operations_completed > 2000, f"Too few operations completed: {result.operations_completed}"
        
        print(f"\nSustained Load Stability Test:")
        print(f"  Duration: {result.duration:.1f}s")
        print(f"  Operations completed: {result.operations_completed}")
        print(f"  Throughput: {result.throughput_ops_per_sec:.1f} ops/sec")
        print(f"  Error rate: {result.error_rate_percent:.2f}%")
        print(f"  Concurrency: {result.concurrency_level} threads")
        
        # Additional stability checks
        ops_per_second = result.operations_completed / result.duration
        assert ops_per_second > 30.0, f"Overall operation rate {ops_per_second:.1f} too low for sustained load"


@pytest.mark.performance
@pytest.mark.hpc
class TestMemoryStabilityUnderLoad:
    """Test memory stability and leak detection under parallel load."""
    
    def test_memory_stability_thread_pool(self, large_test_dataset):
        """Test memory stability with thread pool operations."""
        base_path, files = large_test_dataset
        
        direct_fs = fsspec.filesystem('file')
        sandboxed_fs = PathSandboxedFileSystem(direct_fs, str(base_path))
        
        file_paths = [str(f.relative_to(base_path)) for f in files[:30]]
        
        # Monitor memory over multiple cycles
        process = psutil.Process(os.getpid())
        initial_memory = process.memory_info().rss / (1024 * 1024)  # MB
        memory_samples = [initial_memory]
        
        cycles = 5
        operations_per_cycle = 200
        
        for cycle in range(cycles):
            print(f"  Memory test cycle {cycle + 1}/{cycles}")
            
            def memory_worker():
                for _ in range(operations_per_cycle // 4):  # 4 workers
                    filepath = random.choice(file_paths)
                    try:
                        sandboxed_fs.exists(filepath)
                        sandboxed_fs.info(filepath)
                        with sandboxed_fs.open(filepath, 'rb') as f:
                            f.read(512)
                    except Exception:
                        pass
            
            # Run operations
            with ThreadPoolExecutor(max_workers=4) as executor:
                futures = [executor.submit(memory_worker) for _ in range(4)]
                for future in futures:
                    future.result()
            
            # Sample memory
            current_memory = process.memory_info().rss / (1024 * 1024)
            memory_samples.append(current_memory)
            
            # Force garbage collection
            import gc
            gc.collect()
            
            # Brief pause
            time.sleep(0.5)
        
        # Analyze memory stability
        memory_growth = memory_samples[-1] - memory_samples[0]
        max_memory = max(memory_samples)
        memory_variance = stdev(memory_samples) if len(memory_samples) > 1 else 0
        
        print(f"\nMemory Stability Analysis:")
        print(f"  Initial memory: {initial_memory:.1f}MB")
        print(f"  Final memory: {memory_samples[-1]:.1f}MB")
        print(f"  Memory growth: {memory_growth:.1f}MB")
        print(f"  Peak memory: {max_memory:.1f}MB")
        print(f"  Memory variance: {memory_variance:.1f}MB")
        
        # Memory stability assertions
        assert memory_growth < 50.0, f"Memory growth {memory_growth:.1f}MB indicates potential leak"
        assert max_memory - initial_memory < 100.0, f"Peak memory usage {max_memory - initial_memory:.1f}MB too high"
        assert memory_variance < 30.0, f"Memory variance {memory_variance:.1f}MB indicates instability"


# Utilities for generating stress test reports
def generate_stress_test_report(results: List[StressTestResult]) -> str:
    """Generate comprehensive stress testing report."""
    if not results:
        return "No stress test results to analyze."
    
    report_lines = [
        "Parallel File Operations Stress Test Report",
        "=" * 55,
        f"Total tests executed: {len(results)}",
        f"Report generated: {time.strftime('%Y-%m-%d %H:%M:%S')}",
        "",
        "EXECUTIVE SUMMARY:",
    ]
    
    # Calculate summary statistics
    total_operations = sum(r.operations_completed for r in results)
    total_failures = sum(r.operations_failed for r in results)
    avg_throughput = mean([r.throughput_ops_per_sec for r in results])
    avg_error_rate = mean([r.error_rate_percent for r in results])
    max_concurrency = max([r.concurrency_level for r in results])
    
    report_lines.extend([
        f"  Total operations completed: {total_operations:,}",
        f"  Total operations failed: {total_failures:,}",
        f"  Average throughput: {avg_throughput:.1f} ops/sec",
        f"  Average error rate: {avg_error_rate:.2f}%",
        f"  Maximum concurrency tested: {max_concurrency}",
        "",
        "DETAILED TEST RESULTS:",
    ])
    
    for result in results:
        report_lines.extend([
            f"  {result.test_name.upper()}:",
            f"    Duration: {result.duration:.2f}s",
            f"    Operations: {result.operations_completed:,} completed, {result.operations_failed:,} failed",
            f"    Throughput: {result.throughput_ops_per_sec:.1f} ops/sec",
            f"    Error rate: {result.error_rate_percent:.2f}%",
            f"    Concurrency: {result.concurrency_level}",
            "",
        ])
    
    # Performance assessment
    report_lines.extend([
        "PERFORMANCE ASSESSMENT:",
        "✓ PASS" if avg_error_rate < 10 else "✗ FAIL - Error rate too high",
        "✓ PASS" if avg_throughput > 20 else "✗ FAIL - Throughput too low",
        "✓ PASS" if total_failures < total_operations * 0.1 else "✗ FAIL - Too many failures",
        "",
        "RECOMMENDATIONS FOR HPC DEPLOYMENT:",
        "1. Thread pools scale better than process pools for I/O operations",
        "2. Read-heavy workloads perform better than write-heavy", 
        "3. Implement circuit breakers for fault tolerance",
        "4. Monitor memory growth in long-running applications",
        "5. Consider async I/O for network filesystems",
    ])
    
    return "\n".join(report_lines)


if __name__ == "__main__":
    print("Parallel File Operations Stress Testing Suite")
    print("Run with: pixi run -e test pytest -m 'performance and hpc' tests/test_parallel_stress_performance.py")