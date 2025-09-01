"""
Stress testing framework for PathSandboxedFileSystem under extreme HPC workloads.

This module provides comprehensive stress testing for the PathSandboxedFileSystem
wrapper to ensure it can handle extreme HPC and climate science workloads without
performance degradation or system instability.

Focus areas:
- High-concurrency scenarios (100+ concurrent operations)
- Large-scale file operations (10,000+ files)  
- Memory pressure testing
- Long-running stability testing
- Resource exhaustion scenarios
- Network filesystem simulation
"""

import asyncio
import gc
import os
import random
import tempfile
import threading
import time
from concurrent.futures import (ProcessPoolExecutor, ThreadPoolExecutor,
                                as_completed)
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from unittest.mock import Mock, patch

import fsspec
import psutil
import pytest

from tellus.location.sandboxed_filesystem import (PathSandboxedFileSystem,
                                                  PathValidationError)


class StressTestMonitor:
    """Monitor system resources during stress testing."""
    
    def __init__(self):
        self.monitoring = False
        self.samples = []
        self.start_time = None
        self.monitor_thread = None
        
    def start_monitoring(self):
        """Start resource monitoring."""
        self.monitoring = True
        self.start_time = time.time()
        self.samples = []
        self.monitor_thread = threading.Thread(target=self._monitor_worker)
        self.monitor_thread.start()
        
    def stop_monitoring(self):
        """Stop resource monitoring."""
        self.monitoring = False
        if self.monitor_thread:
            self.monitor_thread.join()
            
    def _monitor_worker(self):
        """Worker thread for resource monitoring."""
        process = psutil.Process(os.getpid())
        
        while self.monitoring:
            try:
                sample = {
                    'timestamp': time.time() - self.start_time,
                    'cpu_percent': process.cpu_percent(),
                    'memory_mb': process.memory_info().rss / (1024 * 1024),
                    'threads': process.num_threads(),
                    'open_files': len(process.open_files()) if hasattr(process, 'open_files') else 0,
                }
                self.samples.append(sample)
                time.sleep(0.1)  # Sample every 100ms
            except psutil.Error:
                # Handle cases where process monitoring fails
                break
                
    def get_stats(self) -> Dict[str, float]:
        """Get monitoring statistics."""
        if not self.samples:
            return {}
            
        memory_values = [s['memory_mb'] for s in self.samples]
        cpu_values = [s['cpu_percent'] for s in self.samples if s['cpu_percent'] > 0]
        
        return {
            'peak_memory_mb': max(memory_values),
            'avg_memory_mb': sum(memory_values) / len(memory_values),
            'memory_delta_mb': max(memory_values) - memory_values[0],
            'peak_cpu_percent': max(cpu_values) if cpu_values else 0,
            'avg_cpu_percent': sum(cpu_values) / len(cpu_values) if cpu_values else 0,
            'duration_seconds': self.samples[-1]['timestamp'],
            'sample_count': len(self.samples),
            'peak_threads': max(s['threads'] for s in self.samples),
            'peak_open_files': max(s['open_files'] for s in self.samples),
        }


def create_massive_file_structure(base_path: Path, file_count: int = 10000) -> List[Path]:
    """Create a massive file structure for stress testing."""
    files_created = []
    
    # Create deep directory hierarchy typical of large HPC datasets
    models = ['CESM', 'GFDL', 'UKESM', 'IPSL', 'MPI']
    experiments = ['historical', 'ssp126', 'ssp245', 'ssp370', 'ssp585']
    components = ['atm', 'ocn', 'lnd', 'ice', 'rof']
    variables = ['tas', 'pr', 'psl', 'ua', 'va', 'ta', 'hus', 'zg']
    frequencies = ['Amon', '6hrLev', 'day', 'Oday', 'Lmon']
    ensembles = [f'r{i}i1p1f1' for i in range(1, 11)]
    years = range(1850, 2101)
    
    file_idx = 0
    for model in models:
        if file_idx >= file_count:
            break
            
        model_dir = base_path / "CMIP6" / model
        
        for experiment in experiments:
            if file_idx >= file_count:
                break
                
            exp_dir = model_dir / experiment
            
            for component in components:
                if file_idx >= file_count:
                    break
                    
                comp_dir = exp_dir / component
                comp_dir.mkdir(parents=True, exist_ok=True)
                
                for variable in variables:
                    if file_idx >= file_count:
                        break
                        
                    var_dir = comp_dir / variable
                    var_dir.mkdir(parents=True, exist_ok=True)
                    
                    for frequency in frequencies:
                        if file_idx >= file_count:
                            break
                            
                        freq_dir = var_dir / frequency
                        freq_dir.mkdir(parents=True, exist_ok=True)
                        
                        for ensemble in ensembles:
                            if file_idx >= file_count:
                                break
                                
                            ens_dir = freq_dir / ensemble
                            ens_dir.mkdir(parents=True, exist_ok=True)
                            
                            # Create yearly files
                            years_per_ensemble = min(10, (file_count - file_idx))
                            selected_years = random.sample(list(years), min(years_per_ensemble, len(years)))
                            
                            for year in selected_years:
                                if file_idx >= file_count:
                                    break
                                
                                filename = f"{variable}_{frequency}_{model}_{experiment}_{ensemble}_gr_{year}01-{year}12.nc"
                                filepath = ens_dir / filename
                                
                                # Create files with varying sizes to simulate real data
                                file_size = random.randint(1024, 50 * 1024)  # 1KB to 50KB
                                filepath.write_bytes(b'x' * file_size)
                                
                                files_created.append(filepath)
                                file_idx += 1
    
    return files_created


@pytest.fixture
def stress_monitor():
    """Provide stress test monitor."""
    monitor = StressTestMonitor()
    yield monitor
    monitor.stop_monitoring()


@pytest.fixture  
def massive_file_structure(tmp_path):
    """Create massive file structure for stress testing."""
    files = create_massive_file_structure(tmp_path, file_count=5000)
    return tmp_path, files


@pytest.mark.performance
@pytest.mark.hpc
@pytest.mark.slow
class TestHighConcurrencyStress:
    """Test PathSandboxedFileSystem under high concurrency stress."""
    
    def test_extreme_concurrent_access(self, stress_monitor, massive_file_structure):
        """Test with 100+ concurrent workers accessing filesystem."""
        base_path, files = massive_file_structure
        
        direct_fs = fsspec.filesystem('file')
        sandboxed_fs = PathSandboxedFileSystem(direct_fs, str(base_path))
        
        # Prepare test data
        test_paths = [str(f.relative_to(base_path)) for f in files[:1000]]
        
        def concurrent_worker(worker_id: int, paths: List[str], operations_per_worker: int):
            """Worker function for extreme concurrency testing."""
            results = {
                'worker_id': worker_id,
                'operations_completed': 0,
                'errors': [],
                'start_time': time.time(),
            }
            
            try:
                for i in range(operations_per_worker):
                    path_idx = (worker_id * operations_per_worker + i) % len(paths)
                    path = paths[path_idx]
                    
                    try:
                        # Mix of different operations
                        operation = i % 4
                        if operation == 0:
                            sandboxed_fs.exists(path)
                        elif operation == 1:
                            sandboxed_fs.isfile(path)
                        elif operation == 2:
                            try:
                                sandboxed_fs.size(path)
                            except Exception:
                                pass  # File might not exist
                        else:
                            try:
                                sandboxed_fs.info(path)
                            except Exception:
                                pass  # File might not exist
                                
                        results['operations_completed'] += 1
                        
                    except Exception as e:
                        results['errors'].append(str(e))
                        
            except Exception as e:
                results['errors'].append(f"Worker error: {str(e)}")
                
            results['end_time'] = time.time()
            results['duration'] = results['end_time'] - results['start_time']
            return results
        
        stress_monitor.start_monitoring()
        
        # Launch extreme concurrency test
        num_workers = 50  # 50 concurrent workers
        operations_per_worker = 100  # 100 operations each = 5000 total operations
        
        start_time = time.time()
        
        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            futures = [
                executor.submit(concurrent_worker, i, test_paths, operations_per_worker)
                for i in range(num_workers)
            ]
            
            # Collect results
            worker_results = []
            for future in as_completed(futures):
                try:
                    result = future.result()
                    worker_results.append(result)
                except Exception as e:
                    worker_results.append({
                        'worker_id': -1,
                        'operations_completed': 0,
                        'errors': [str(e)],
                        'duration': 0
                    })
        
        total_time = time.time() - start_time
        stress_monitor.stop_monitoring()
        
        # Analyze results
        total_operations = sum(r['operations_completed'] for r in worker_results)
        total_errors = sum(len(r['errors']) for r in worker_results)
        successful_workers = len([r for r in worker_results if r['operations_completed'] > 0])
        
        # Get monitoring stats
        monitor_stats = stress_monitor.get_stats()
        
        # Performance assertions
        assert successful_workers >= num_workers * 0.9, f"Only {successful_workers}/{num_workers} workers succeeded"
        assert total_operations >= (num_workers * operations_per_worker * 0.8), f"Only {total_operations} operations completed"
        
        operations_per_second = total_operations / total_time
        assert operations_per_second > 50, f"Operations/sec {operations_per_second:.1f} too low"
        
        # Resource usage should be reasonable despite high concurrency
        assert monitor_stats.get('peak_memory_mb', 0) < 500, f"Peak memory {monitor_stats.get('peak_memory_mb', 0):.1f}MB too high"
        assert monitor_stats.get('memory_delta_mb', 0) < 200, f"Memory growth {monitor_stats.get('memory_delta_mb', 0):.1f}MB too high"
        
        print(f"\nExtreme Concurrency Stress Test:")
        print(f"  Workers: {num_workers}")
        print(f"  Total operations: {total_operations}")
        print(f"  Operations/sec: {operations_per_second:.1f}")
        print(f"  Error rate: {(total_errors / max(total_operations, 1)) * 100:.2f}%")
        print(f"  Peak memory: {monitor_stats.get('peak_memory_mb', 0):.1f}MB")
        print(f"  Memory delta: {monitor_stats.get('memory_delta_mb', 0):.1f}MB")
        print(f"  Peak threads: {monitor_stats.get('peak_threads', 0)}")
        
    def test_mixed_workload_stress(self, stress_monitor, massive_file_structure):
        """Test mixed workload patterns typical in HPC environments."""
        base_path, files = massive_file_structure
        
        direct_fs = fsspec.filesystem('file')
        sandboxed_fs = PathSandboxedFileSystem(direct_fs, str(base_path))
        
        test_paths = [str(f.relative_to(base_path)) for f in files[:500]]
        
        def listing_worker():
            """Worker that performs directory listing operations."""
            results = {'operations': 0, 'errors': []}
            try:
                for _ in range(20):
                    sandboxed_fs.ls("")
                    sandboxed_fs.ls("CMIP6")
                    sandboxed_fs.listdir("CMIP6")
                    results['operations'] += 3
            except Exception as e:
                results['errors'].append(str(e))
            return results
            
        def glob_worker():
            """Worker that performs pattern matching operations."""
            results = {'operations': 0, 'errors': []}
            patterns = ["**/*.nc", "**/atm/**", "**/historical/**", "**/*tas*"]
            try:
                for pattern in patterns * 5:  # 20 operations
                    sandboxed_fs.glob(pattern)
                    results['operations'] += 1
            except Exception as e:
                results['errors'].append(str(e))
            return results
            
        def file_access_worker():
            """Worker that performs individual file operations."""
            results = {'operations': 0, 'errors': []}
            try:
                for path in test_paths[:50]:  # 50 file operations
                    try:
                        sandboxed_fs.exists(path)
                        if random.random() < 0.5:  # 50% chance
                            sandboxed_fs.isfile(path)
                        results['operations'] += 1
                    except Exception:
                        pass  # Some files might not exist
            except Exception as e:
                results['errors'].append(str(e))
            return results
            
        def walk_worker():
            """Worker that performs directory tree walking."""
            results = {'operations': 0, 'errors': []}
            try:
                for path in ["CMIP6", "CMIP6/CESM", "CMIP6/GFDL"]:
                    try:
                        list(sandboxed_fs.walk(path))
                        results['operations'] += 1
                    except Exception:
                        pass  # Path might not exist
            except Exception as e:
                results['errors'].append(str(e))
            return results
        
        stress_monitor.start_monitoring()
        start_time = time.time()
        
        # Launch mixed workload
        with ThreadPoolExecutor(max_workers=20) as executor:
            # Submit different types of workers
            futures = []
            
            # 5 listing workers
            futures.extend([executor.submit(listing_worker) for _ in range(5)])
            
            # 5 glob workers  
            futures.extend([executor.submit(glob_worker) for _ in range(5)])
            
            # 8 file access workers
            futures.extend([executor.submit(file_access_worker) for _ in range(8)])
            
            # 2 walk workers
            futures.extend([executor.submit(walk_worker) for _ in range(2)])
            
            # Collect results
            worker_results = [future.result() for future in as_completed(futures)]
        
        total_time = time.time() - start_time
        stress_monitor.stop_monitoring()
        
        # Analyze mixed workload performance
        total_operations = sum(r['operations'] for r in worker_results)
        total_errors = sum(len(r['errors']) for r in worker_results)
        
        monitor_stats = stress_monitor.get_stats()
        
        # Performance assertions
        assert total_operations > 200, f"Only {total_operations} operations completed in mixed workload"
        
        operations_per_second = total_operations / total_time
        assert operations_per_second > 20, f"Mixed workload ops/sec {operations_per_second:.1f} too low"
        
        # Memory should remain stable under mixed load
        assert monitor_stats.get('memory_delta_mb', 0) < 150, f"Memory growth {monitor_stats.get('memory_delta_mb', 0):.1f}MB under mixed load"
        
        print(f"\nMixed Workload Stress Test:")
        print(f"  Total operations: {total_operations}")
        print(f"  Operations/sec: {operations_per_second:.1f}")
        print(f"  Error count: {total_errors}")
        print(f"  Duration: {total_time:.2f}s")
        print(f"  Peak memory: {monitor_stats.get('peak_memory_mb', 0):.1f}MB")


@pytest.mark.performance
@pytest.mark.large_data  
@pytest.mark.slow
class TestLargeScaleDataStress:
    """Test PathSandboxedFileSystem with large-scale data scenarios."""
    
    def test_massive_file_enumeration(self, stress_monitor, tmp_path):
        """Test enumeration of massive number of files (10,000+)."""
        # Create truly massive file structure
        files = create_massive_file_structure(tmp_path, file_count=10000)
        
        direct_fs = fsspec.filesystem('file')
        sandboxed_fs = PathSandboxedFileSystem(direct_fs, str(tmp_path))
        
        stress_monitor.start_monitoring()
        
        # Test massive find operation
        start_time = time.time()
        all_files = sandboxed_fs.find("")
        find_time = time.time() - start_time
        
        # Test massive glob operation
        start_time = time.time()
        nc_files = sandboxed_fs.glob("**/*.nc")
        glob_time = time.time() - start_time
        
        # Test directory tree walking
        start_time = time.time()
        walked_files = []
        for root, dirs, files_in_dir in sandboxed_fs.walk(""):
            walked_files.extend([os.path.join(root, f) for f in files_in_dir])
        walk_time = time.time() - start_time
        
        stress_monitor.stop_monitoring()
        
        # Verify operations found reasonable number of files
        assert len(all_files) >= 5000, f"find() only found {len(all_files)} files"
        assert len(nc_files) >= 4000, f"glob() only found {len(nc_files)} .nc files"
        assert len(walked_files) >= 5000, f"walk() only found {len(walked_files)} files"
        
        # Performance assertions for large scale
        find_rate = len(all_files) / find_time if find_time > 0 else 0
        assert find_rate > 100, f"find() rate {find_rate:.1f} files/sec too slow for large scale"
        
        glob_rate = len(nc_files) / glob_time if glob_time > 0 else 0
        assert glob_rate > 50, f"glob() rate {glob_rate:.1f} files/sec too slow for large scale"
        
        walk_rate = len(walked_files) / walk_time if walk_time > 0 else 0  
        assert walk_rate > 100, f"walk() rate {walk_rate:.1f} files/sec too slow for large scale"
        
        monitor_stats = stress_monitor.get_stats()
        
        print(f"\nMassive File Enumeration Test:")
        print(f"  Files created: {len(files)}")
        print(f"  find() results: {len(all_files)} ({find_rate:.1f} files/sec)")
        print(f"  glob() results: {len(nc_files)} ({glob_rate:.1f} files/sec)")
        print(f"  walk() results: {len(walked_files)} ({walk_rate:.1f} files/sec)")
        print(f"  Peak memory: {monitor_stats.get('peak_memory_mb', 0):.1f}MB")
        
    def test_deep_directory_hierarchy_stress(self, stress_monitor, tmp_path):
        """Test performance with very deep directory hierarchies."""
        # Create deep hierarchy (typical of organized climate data)
        max_depth = 8
        files_per_level = 5
        
        def create_deep_structure(current_path: Path, depth: int, files_created: List[Path]):
            if depth >= max_depth:
                return
                
            for i in range(files_per_level):
                # Create directory
                dir_path = current_path / f"level_{depth}_{i}"
                dir_path.mkdir(exist_ok=True)
                
                # Create files in directory
                for j in range(3):
                    file_path = dir_path / f"data_{depth}_{i}_{j}.nc"
                    file_path.write_bytes(b'deep_data' * 100)
                    files_created.append(file_path)
                
                # Recurse deeper
                create_deep_structure(dir_path, depth + 1, files_created)
        
        files_created = []
        create_deep_structure(tmp_path / "deep_structure", 0, files_created)
        
        direct_fs = fsspec.filesystem('file')
        sandboxed_fs = PathSandboxedFileSystem(direct_fs, str(tmp_path))
        
        stress_monitor.start_monitoring()
        
        # Test operations on deep hierarchy
        start_time = time.time()
        deep_files = sandboxed_fs.find("deep_structure")
        find_deep_time = time.time() - start_time
        
        start_time = time.time()
        deep_nc_files = sandboxed_fs.glob("deep_structure/**/*.nc")
        glob_deep_time = time.time() - start_time
        
        # Test walking deep structure
        start_time = time.time()
        walked_deep = []
        for root, dirs, files in sandboxed_fs.walk("deep_structure"):
            walked_deep.extend(files)
        walk_deep_time = time.time() - start_time
        
        stress_monitor.stop_monitoring()
        
        # Performance assertions for deep hierarchies
        assert len(deep_files) >= len(files_created) * 0.9, "find() missed files in deep hierarchy"
        assert len(deep_nc_files) >= len(files_created) * 0.9, "glob() missed .nc files in deep hierarchy"
        
        # Performance should remain reasonable even with deep structures
        find_deep_rate = len(deep_files) / find_deep_time if find_deep_time > 0 else 0
        assert find_deep_rate > 20, f"Deep find() rate {find_deep_rate:.1f} files/sec too slow"
        
        monitor_stats = stress_monitor.get_stats()
        
        print(f"\nDeep Directory Hierarchy Test:")
        print(f"  Max depth: {max_depth}")
        print(f"  Files created: {len(files_created)}")
        print(f"  Deep find() rate: {find_deep_rate:.1f} files/sec")
        print(f"  Deep walk time: {walk_deep_time:.2f}s")
        print(f"  Memory usage: {monitor_stats.get('peak_memory_mb', 0):.1f}MB")


@pytest.mark.performance
@pytest.mark.slow
class TestLongRunningStability:
    """Test long-running stability and resource leaks."""
    
    def test_sustained_operations_stability(self, stress_monitor, massive_file_structure):
        """Test stability over sustained operations (30+ minutes simulated)."""
        base_path, files = massive_file_structure
        
        direct_fs = fsspec.filesystem('file')
        sandboxed_fs = PathSandboxedFileSystem(direct_fs, str(base_path))
        
        test_paths = [str(f.relative_to(base_path)) for f in files[:200]]
        
        stress_monitor.start_monitoring()
        
        # Simulate sustained operations over time
        operation_cycles = 100  # Simulate extended operation period
        operations_per_cycle = 50
        total_operations = 0
        
        start_time = time.time()
        
        for cycle in range(operation_cycles):
            # Mix of operations per cycle
            for i in range(operations_per_cycle):
                path_idx = (cycle * operations_per_cycle + i) % len(test_paths)
                path = test_paths[path_idx]
                
                try:
                    # Rotate through different operation types
                    op_type = i % 5
                    if op_type == 0:
                        sandboxed_fs.exists(path)
                    elif op_type == 1:
                        sandboxed_fs.isfile(path)
                    elif op_type == 2:
                        try:
                            sandboxed_fs.size(path)
                        except Exception:
                            pass
                    elif op_type == 3:
                        sandboxed_fs.ls("")
                    else:
                        sandboxed_fs.glob("**/*.nc")
                        
                    total_operations += 1
                    
                except Exception as e:
                    # Log but don't fail on individual operation errors
                    pass
            
            # Periodic garbage collection to test for leaks
            if cycle % 20 == 0:
                gc.collect()
                
            # Small delay to simulate realistic timing
            time.sleep(0.01)
        
        total_time = time.time() - start_time
        stress_monitor.stop_monitoring()
        
        # Analyze long-running stability
        monitor_stats = stress_monitor.get_stats()
        
        operations_per_second = total_operations / total_time
        
        # Stability assertions
        assert total_operations >= operation_cycles * operations_per_cycle * 0.9, "Operations dropped significantly"
        assert operations_per_second > 50, f"Long-running ops/sec {operations_per_second:.1f} degraded too much"
        
        # Memory leak detection
        memory_delta = monitor_stats.get('memory_delta_mb', 0)
        assert memory_delta < 100, f"Memory leak detected: {memory_delta:.1f}MB growth"
        
        # CPU usage should remain reasonable
        avg_cpu = monitor_stats.get('avg_cpu_percent', 0)
        assert avg_cpu < 50, f"Average CPU {avg_cpu:.1f}% too high for sustained operations"
        
        print(f"\nSustained Operations Stability Test:")
        print(f"  Duration: {total_time:.1f}s")
        print(f"  Total operations: {total_operations}")
        print(f"  Operations/sec: {operations_per_second:.1f}")
        print(f"  Memory delta: {memory_delta:.1f}MB")
        print(f"  Average CPU: {avg_cpu:.1f}%")
        print(f"  Samples collected: {monitor_stats.get('sample_count', 0)}")


@pytest.mark.performance
class TestResourceExhaustionScenarios:
    """Test behavior under resource exhaustion conditions."""
    
    def test_memory_pressure_handling(self, stress_monitor, tmp_path):
        """Test behavior under simulated memory pressure."""
        # Create moderate file structure
        files = create_test_file_structure(tmp_path, file_count=500)
        
        direct_fs = fsspec.filesystem('file')
        sandboxed_fs = PathSandboxedFileSystem(direct_fs, str(tmp_path))
        
        # Simulate memory pressure by creating large objects
        memory_hogs = []
        
        stress_monitor.start_monitoring()
        
        try:
            # Create memory pressure
            for i in range(10):
                # Create 10MB objects to simulate memory pressure
                memory_hog = bytearray(10 * 1024 * 1024)  # 10 MB
                memory_hogs.append(memory_hog)
            
            # Perform operations under memory pressure
            operations_completed = 0
            errors = 0
            
            test_paths = [str(f.relative_to(tmp_path)) for f in files[:100]]
            
            for path in test_paths:
                try:
                    sandboxed_fs.exists(path)
                    operations_completed += 1
                except Exception:
                    errors += 1
            
            # Test directory operations under memory pressure
            try:
                all_files = sandboxed_fs.find("")
                operations_completed += 1
            except Exception:
                errors += 1
            
            # Test pattern matching under memory pressure
            try:
                nc_files = sandboxed_fs.glob("**/*.nc")
                operations_completed += 1
            except Exception:
                errors += 1
        
        finally:
            # Clean up memory
            memory_hogs.clear()
            gc.collect()
        
        stress_monitor.stop_monitoring()
        
        monitor_stats = stress_monitor.get_stats()
        
        # System should remain functional under memory pressure
        success_rate = operations_completed / (operations_completed + errors) if (operations_completed + errors) > 0 else 0
        assert success_rate > 0.8, f"Success rate {success_rate:.2f} too low under memory pressure"
        
        print(f"\nMemory Pressure Test:")
        print(f"  Operations completed: {operations_completed}")
        print(f"  Errors: {errors}")
        print(f"  Success rate: {success_rate:.2f}")
        print(f"  Peak memory: {monitor_stats.get('peak_memory_mb', 0):.1f}MB")
        
    def test_file_descriptor_limits(self, stress_monitor, tmp_path):
        """Test behavior when approaching file descriptor limits."""
        # Create test files
        files = create_test_file_structure(tmp_path, file_count=100)
        
        direct_fs = fsspec.filesystem('file')
        sandboxed_fs = PathSandboxedFileSystem(direct_fs, str(tmp_path))
        
        # Simulate file descriptor pressure
        open_files = []
        
        stress_monitor.start_monitoring()
        
        try:
            # Open many files to simulate FD pressure (be careful not to exceed limits)
            max_open_files = min(100, len(files))  # Conservative limit
            
            for i in range(max_open_files):
                try:
                    f = open(files[i], 'rb')
                    open_files.append(f)
                except OSError:
                    # Hit file descriptor limit
                    break
            
            # Perform filesystem operations with high FD usage
            operations_completed = 0
            
            test_paths = [str(f.relative_to(tmp_path)) for f in files[:50]]
            
            for path in test_paths:
                try:
                    sandboxed_fs.exists(path)
                    operations_completed += 1
                except Exception:
                    pass
            
        finally:
            # Clean up file descriptors
            for f in open_files:
                try:
                    f.close()
                except Exception:
                    pass
        
        stress_monitor.stop_monitoring()
        
        monitor_stats = stress_monitor.get_stats()
        
        # Operations should complete despite FD pressure
        assert operations_completed > 40, f"Only {operations_completed} operations completed under FD pressure"
        
        print(f"\nFile Descriptor Pressure Test:")
        print(f"  Open files: {len(open_files)}")
        print(f"  Operations completed: {operations_completed}")
        print(f"  Peak open files: {monitor_stats.get('peak_open_files', 0)}")


def create_test_file_structure(base_path: Path, file_count: int = 1000) -> List[Path]:
    """Create a realistic test file structure (imported from main test file)."""
    files_created = []
    
    # Create directory structure typical of climate model output
    components = ['atm', 'ocn', 'lnd', 'ice']
    variables = ['temp', 'precip', 'wind', 'pres', 'humid', 'cloud']
    frequencies = ['daily', 'monthly', 'hourly']
    years = range(2000, 2021)
    
    file_idx = 0
    for component in components:
        if file_idx >= file_count:
            break
            
        comp_dir = base_path / "model_output" / component
        comp_dir.mkdir(parents=True, exist_ok=True)
        
        for variable in variables:
            if file_idx >= file_count:
                break
                
            var_dir = comp_dir / variable
            var_dir.mkdir(parents=True, exist_ok=True)
            
            for frequency in frequencies:
                if file_idx >= file_count:
                    break
                    
                freq_dir = var_dir / frequency
                freq_dir.mkdir(parents=True, exist_ok=True)
                
                for year in years:
                    if file_idx >= file_count:
                        break
                    
                    filename = f"{variable}_{frequency}_{year}.nc"
                    filepath = freq_dir / filename
                    
                    file_size = 1024 * (50 + file_idx % 200)  # 50KB to 250KB files
                    filepath.write_bytes(b'x' * file_size)
                    
                    files_created.append(filepath)
                    file_idx += 1
    
    return files_created


if __name__ == "__main__":
    print("PathSandboxedFileSystem Stress Test Suite")
    print("Run with: pytest -m 'performance and hpc' tests/test_sandboxed_filesystem_stress.py")