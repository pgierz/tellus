"""
Performance tests for PathSandboxedFileSystem to ensure security wrapper doesn't impact HPC workloads.

This module provides comprehensive performance benchmarking and validation for the
PathSandboxedFileSystem wrapper used in Location.fs path resolution. Tests focus on
ensuring the security fix doesn't introduce significant overhead for climate science
and HPC workloads that involve thousands of large files.

Key Performance Areas Tested:
- Path resolution overhead
- Bulk file operations (list, glob, walk)
- Large file I/O (NetCDF/Zarr-like workloads)  
- Concurrent access patterns
- Memory usage analysis
- Before/after performance comparison
"""

import gc
import os
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from statistics import mean, stdev
from typing import Dict, List, Tuple
from unittest.mock import Mock

import fsspec
import psutil
import pytest

from tellus.location.sandboxed_filesystem import (PathSandboxedFileSystem,
                                                  PathValidationError)


class PerformanceProfiler:
    """Utility class for detailed performance profiling."""
    
    def __init__(self):
        self.measurements = {}
        self.memory_samples = []
        self.start_memory = self._get_memory_usage()
        
    def _get_memory_usage(self) -> float:
        """Get current memory usage in MB."""
        process = psutil.Process(os.getpid())
        return process.memory_info().rss / (1024 * 1024)
    
    def start_measurement(self, operation: str):
        """Start timing an operation."""
        self.measurements[operation] = {"start": time.perf_counter()}
        
    def end_measurement(self, operation: str) -> float:
        """End timing an operation and return duration."""
        if operation in self.measurements:
            duration = time.perf_counter() - self.measurements[operation]["start"]
            self.measurements[operation]["duration"] = duration
            return duration
        return 0.0
    
    def take_memory_snapshot(self):
        """Take a memory usage snapshot."""
        self.memory_samples.append(self._get_memory_usage())
    
    def get_memory_stats(self) -> Dict[str, float]:
        """Get memory usage statistics."""
        if not self.memory_samples:
            return {"peak": 0, "delta": 0, "avg": 0}
        
        peak = max(self.memory_samples)
        avg = mean(self.memory_samples)
        delta = peak - self.start_memory
        return {"peak": peak, "delta": delta, "avg": avg}
    
    def get_timing_stats(self) -> Dict[str, float]:
        """Get timing statistics for all operations."""
        return {
            op: data.get("duration", 0) 
            for op, data in self.measurements.items()
        }


def create_test_file_structure(base_path: Path, file_count: int = 1000, 
                             dir_depth: int = 3) -> List[Path]:
    """Create a realistic Earth science file structure for testing."""
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
                    
                    # Create realistic climate data filename
                    filename = f"{variable}_{frequency}_{year}.nc"
                    filepath = freq_dir / filename
                    
                    # Create file with realistic size (climate data is typically large)
                    file_size = 1024 * (50 + file_idx % 200)  # 50KB to 250KB files
                    filepath.write_bytes(b'x' * file_size)
                    
                    files_created.append(filepath)
                    file_idx += 1
    
    # Add some top-level metadata files
    for i in range(min(20, file_count - len(files_created))):
        metadata_file = base_path / f"metadata_{i:03d}.json"
        metadata_file.write_text('{"model": "test", "version": "1.0"}')
        files_created.append(metadata_file)
    
    return files_created


@pytest.fixture
def profiler():
    """Provide performance profiler instance."""
    return PerformanceProfiler()


@pytest.fixture
def test_file_structure(tmp_path):
    """Create test file structure with many files."""
    files = create_test_file_structure(tmp_path, file_count=500, dir_depth=4)
    return tmp_path, files


@pytest.fixture
def large_file_structure(tmp_path):
    """Create file structure with larger files for I/O testing."""
    large_files = []
    for i in range(50):  # 50 larger files
        filepath = tmp_path / f"large_file_{i:03d}.nc"
        # Create 1-5MB files to simulate real climate data
        size = 1024 * 1024 * (1 + i % 5)  # 1-5 MB
        filepath.write_bytes(b'climate_data' * (size // 12))
        large_files.append(filepath)
    
    return tmp_path, large_files


@pytest.mark.performance
@pytest.mark.benchmark
class TestPathSandboxedFileSystemPerformance:
    """Core performance tests for PathSandboxedFileSystem wrapper."""
    
    def test_path_resolution_overhead(self, profiler, test_file_structure):
        """Test path resolution overhead vs direct fsspec operations."""
        base_path, files = test_file_structure
        
        # Setup filesystems
        direct_fs = fsspec.filesystem('file')
        sandboxed_fs = PathSandboxedFileSystem(direct_fs, str(base_path))
        
        # Test paths (relative to base_path)
        test_paths = [str(f.relative_to(base_path)) for f in files[:100]]
        
        profiler.take_memory_snapshot()
        
        # Benchmark direct filesystem path operations
        profiler.start_measurement("direct_path_resolution")
        direct_results = []
        for path in test_paths:
            full_path = base_path / path
            direct_results.append(direct_fs.exists(str(full_path)))
        direct_time = profiler.end_measurement("direct_path_resolution")
        
        profiler.take_memory_snapshot()
        
        # Benchmark sandboxed filesystem path operations
        profiler.start_measurement("sandboxed_path_resolution")
        sandboxed_results = []
        for path in test_paths:
            sandboxed_results.append(sandboxed_fs.exists(path))
        sandboxed_time = profiler.end_measurement("sandboxed_path_resolution")
        
        profiler.take_memory_snapshot()
        
        # Calculate performance overhead
        overhead_percentage = ((sandboxed_time - direct_time) / direct_time) * 100
        
        # Results should be identical
        assert direct_results == sandboxed_results
        
        # Performance assertions - overhead should be < 5%
        assert overhead_percentage < 5.0, f"Path resolution overhead {overhead_percentage:.2f}% exceeds 5%"
        
        # Memory usage should be similar
        memory_stats = profiler.get_memory_stats()
        assert memory_stats["delta"] < 50, f"Memory overhead {memory_stats['delta']:.1f}MB too high"
        
        print(f"\nPath Resolution Performance:")
        print(f"  Direct time: {direct_time:.4f}s")
        print(f"  Sandboxed time: {sandboxed_time:.4f}s") 
        print(f"  Overhead: {overhead_percentage:.2f}%")
        print(f"  Memory delta: {memory_stats['delta']:.1f}MB")
        
    def test_bulk_file_listing_performance(self, profiler, test_file_structure):
        """Test performance of bulk file listing operations."""
        base_path, files = test_file_structure
        
        direct_fs = fsspec.filesystem('file')
        sandboxed_fs = PathSandboxedFileSystem(direct_fs, str(base_path))
        
        profiler.take_memory_snapshot()
        
        # Test ls() operation
        profiler.start_measurement("direct_ls")
        direct_ls = direct_fs.ls(str(base_path), detail=False)
        direct_ls_time = profiler.end_measurement("direct_ls")
        
        profiler.start_measurement("sandboxed_ls")
        sandboxed_ls = sandboxed_fs.ls("", detail=False)
        sandboxed_ls_time = profiler.end_measurement("sandboxed_ls")
        
        # Test glob() operation
        profiler.start_measurement("direct_glob")
        direct_glob = direct_fs.glob(str(base_path / "**/*.nc"))
        direct_glob_time = profiler.end_measurement("direct_glob")
        
        profiler.start_measurement("sandboxed_glob")
        sandboxed_glob = sandboxed_fs.glob("**/*.nc")
        sandboxed_glob_time = profiler.end_measurement("sandboxed_glob")
        
        # Test find() operation
        profiler.start_measurement("direct_find")
        direct_find = direct_fs.find(str(base_path))
        direct_find_time = profiler.end_measurement("direct_find")
        
        profiler.start_measurement("sandboxed_find")
        sandboxed_find = sandboxed_fs.find("")
        sandboxed_find_time = profiler.end_measurement("sandboxed_find")
        
        profiler.take_memory_snapshot()
        
        # Calculate overheads
        ls_overhead = ((sandboxed_ls_time - direct_ls_time) / direct_ls_time) * 100
        glob_overhead = ((sandboxed_glob_time - direct_glob_time) / direct_glob_time) * 100
        find_overhead = ((sandboxed_find_time - direct_find_time) / direct_find_time) * 100
        
        # Results should find similar number of items
        assert abs(len(direct_ls) - len(sandboxed_ls)) <= 1  # Allow for minor differences
        assert len(direct_glob) == len(sandboxed_glob)
        assert abs(len(direct_find) - len(sandboxed_find)) <= 5  # Allow for path differences
        
        # Performance assertions - all overheads should be < 10%
        assert ls_overhead < 10.0, f"ls() overhead {ls_overhead:.2f}% exceeds 10%"
        assert glob_overhead < 10.0, f"glob() overhead {glob_overhead:.2f}% exceeds 10%"
        assert find_overhead < 10.0, f"find() overhead {find_overhead:.2f}% exceeds 10%"
        
        print(f"\nBulk Operations Performance:")
        print(f"  ls() overhead: {ls_overhead:.2f}% ({len(sandboxed_ls)} files)")
        print(f"  glob() overhead: {glob_overhead:.2f}% ({len(sandboxed_glob)} matches)")
        print(f"  find() overhead: {find_overhead:.2f}% ({len(sandboxed_find)} files)")
        
    def test_large_file_io_performance(self, profiler, large_file_structure):
        """Test I/O performance with large files (climate data simulation)."""
        base_path, files = large_file_structure
        
        direct_fs = fsspec.filesystem('file')
        sandboxed_fs = PathSandboxedFileSystem(direct_fs, str(base_path))
        
        # Test files (first 10 for time efficiency)
        test_files = files[:10]
        test_paths = [str(f.relative_to(base_path)) for f in test_files]
        
        profiler.take_memory_snapshot()
        
        # Test read operations
        profiler.start_measurement("direct_read_ops")
        direct_sizes = []
        for test_file in test_files:
            size = direct_fs.size(str(test_file))
            data = direct_fs.read_bytes(str(test_file))
            direct_sizes.append(len(data))
        direct_read_time = profiler.end_measurement("direct_read_ops")
        
        profiler.take_memory_snapshot()
        
        profiler.start_measurement("sandboxed_read_ops")
        sandboxed_sizes = []
        for path in test_paths:
            size = sandboxed_fs.size(path)
            data = sandboxed_fs.read_bytes(path)
            sandboxed_sizes.append(len(data))
        sandboxed_read_time = profiler.end_measurement("sandboxed_read_ops")
        
        profiler.take_memory_snapshot()
        
        # Test write operations (to temporary files)
        write_data = b'new_climate_data' * 1000  # ~16KB test data
        
        profiler.start_measurement("direct_write_ops")
        for i in range(5):  # Write 5 test files
            temp_path = base_path / f"direct_write_test_{i}.tmp"
            direct_fs.write_bytes(str(temp_path), write_data)
        direct_write_time = profiler.end_measurement("direct_write_ops")
        
        profiler.start_measurement("sandboxed_write_ops")
        for i in range(5):  # Write 5 test files
            temp_path = f"sandboxed_write_test_{i}.tmp"
            sandboxed_fs.write_bytes(temp_path, write_data)
        sandboxed_write_time = profiler.end_measurement("sandboxed_write_ops")
        
        profiler.take_memory_snapshot()
        
        # Calculate total data processed
        total_data_mb = sum(direct_sizes) / (1024 * 1024)
        
        # Calculate performance metrics
        read_overhead = ((sandboxed_read_time - direct_read_time) / direct_read_time) * 100
        write_overhead = ((sandboxed_write_time - direct_write_time) / direct_write_time) * 100
        
        # Verify data integrity
        assert direct_sizes == sandboxed_sizes
        
        # Performance assertions - I/O overhead should be minimal
        assert read_overhead < 5.0, f"Read I/O overhead {read_overhead:.2f}% exceeds 5%"
        assert write_overhead < 10.0, f"Write I/O overhead {write_overhead:.2f}% exceeds 10%"
        
        # Calculate throughput
        read_throughput_direct = total_data_mb / direct_read_time if direct_read_time > 0 else 0
        read_throughput_sandboxed = total_data_mb / sandboxed_read_time if sandboxed_read_time > 0 else 0
        
        print(f"\nLarge File I/O Performance:")
        print(f"  Total data processed: {total_data_mb:.1f}MB")
        print(f"  Read overhead: {read_overhead:.2f}%")
        print(f"  Write overhead: {write_overhead:.2f}%")
        print(f"  Direct read throughput: {read_throughput_direct:.1f}MB/s")
        print(f"  Sandboxed read throughput: {read_throughput_sandboxed:.1f}MB/s")
        
        # Clean up temp files
        for i in range(5):
            (base_path / f"direct_write_test_{i}.tmp").unlink(missing_ok=True)
            (base_path / f"sandboxed_write_test_{i}.tmp").unlink(missing_ok=True)


@pytest.mark.performance
@pytest.mark.hpc
class TestHPCWorkloadPerformance:
    """Performance tests for typical HPC climate science workloads."""
    
    def test_concurrent_file_access_performance(self, profiler, test_file_structure):
        """Test performance under concurrent file access (typical HPC pattern)."""
        base_path, files = test_file_structure
        
        direct_fs = fsspec.filesystem('file')
        sandboxed_fs = PathSandboxedFileSystem(direct_fs, str(base_path))
        
        # Select files for concurrent access
        test_files = files[:100]
        test_paths = [str(f.relative_to(base_path)) for f in test_files]
        
        def concurrent_worker(filesystem, paths, worker_id):
            """Worker function for concurrent access testing."""
            results = []
            start_time = time.perf_counter()
            
            for path in paths:
                try:
                    if hasattr(filesystem, 'exists'):
                        exists = filesystem.exists(path)
                    else:
                        exists = filesystem.exists(str(base_path / path))
                    results.append(exists)
                except Exception as e:
                    results.append(False)
                    
            end_time = time.perf_counter()
            return len(results), end_time - start_time
        
        profiler.take_memory_snapshot()
        
        # Test direct filesystem with concurrency
        with ThreadPoolExecutor(max_workers=4) as executor:
            profiler.start_measurement("direct_concurrent")
            
            chunk_size = len(test_files) // 4
            chunks = [test_files[i:i+chunk_size] for i in range(0, len(test_files), chunk_size)]
            
            direct_futures = [
                executor.submit(concurrent_worker, direct_fs, 
                               [str(f) for f in chunk], i)
                for i, chunk in enumerate(chunks)
            ]
            
            direct_results = [f.result() for f in direct_futures]
            direct_concurrent_time = profiler.end_measurement("direct_concurrent")
        
        profiler.take_memory_snapshot()
        
        # Test sandboxed filesystem with concurrency  
        with ThreadPoolExecutor(max_workers=4) as executor:
            profiler.start_measurement("sandboxed_concurrent")
            
            chunk_size = len(test_paths) // 4
            chunks = [test_paths[i:i+chunk_size] for i in range(0, len(test_paths), chunk_size)]
            
            sandboxed_futures = [
                executor.submit(concurrent_worker, sandboxed_fs, chunk, i)
                for i, chunk in enumerate(chunks)
            ]
            
            sandboxed_results = [f.result() for f in sandboxed_futures]
            sandboxed_concurrent_time = profiler.end_measurement("sandboxed_concurrent")
        
        profiler.take_memory_snapshot()
        
        # Analyze results
        direct_total_ops = sum(r[0] for r in direct_results)
        sandboxed_total_ops = sum(r[0] for r in sandboxed_results)
        
        concurrent_overhead = ((sandboxed_concurrent_time - direct_concurrent_time) / 
                              direct_concurrent_time) * 100
        
        # Verify similar number of operations completed
        assert abs(direct_total_ops - sandboxed_total_ops) <= 4  # Allow for minor differences
        
        # Performance assertion - concurrent overhead should be minimal
        assert concurrent_overhead < 15.0, f"Concurrent access overhead {concurrent_overhead:.2f}% exceeds 15%"
        
        # Calculate throughput
        direct_ops_per_sec = direct_total_ops / direct_concurrent_time if direct_concurrent_time > 0 else 0
        sandboxed_ops_per_sec = sandboxed_total_ops / sandboxed_concurrent_time if sandboxed_concurrent_time > 0 else 0
        
        print(f"\nConcurrent Access Performance:")
        print(f"  Direct ops/sec: {direct_ops_per_sec:.1f}")
        print(f"  Sandboxed ops/sec: {sandboxed_ops_per_sec:.1f}")
        print(f"  Concurrent overhead: {concurrent_overhead:.2f}%")
        print(f"  Total operations: {sandboxed_total_ops}")
        
    def test_pattern_matching_performance(self, profiler, test_file_structure):
        """Test performance of pattern matching for scientific data discovery."""
        base_path, files = test_file_structure
        
        direct_fs = fsspec.filesystem('file')
        sandboxed_fs = PathSandboxedFileSystem(direct_fs, str(base_path))
        
        # Common climate science file patterns
        patterns = [
            "**/*.nc",              # All NetCDF files
            "**/atm/**/*.nc",       # Atmosphere component files
            "**/temp_*.nc",         # Temperature variables
            "**/monthly/*.nc",      # Monthly data
            "**/*2020*.nc",        # Files from 2020
            "**/model_output/**/*", # All model output
        ]
        
        profiler.take_memory_snapshot()
        
        # Benchmark direct filesystem pattern matching
        direct_results = {}
        total_direct_time = 0
        
        for pattern in patterns:
            profiler.start_measurement(f"direct_pattern_{pattern}")
            matches = direct_fs.glob(str(base_path / pattern))
            pattern_time = profiler.end_measurement(f"direct_pattern_{pattern}")
            direct_results[pattern] = len(matches)
            total_direct_time += pattern_time
        
        profiler.take_memory_snapshot()
        
        # Benchmark sandboxed filesystem pattern matching
        sandboxed_results = {}
        total_sandboxed_time = 0
        
        for pattern in patterns:
            profiler.start_measurement(f"sandboxed_pattern_{pattern}")
            matches = sandboxed_fs.glob(pattern)
            pattern_time = profiler.end_measurement(f"sandboxed_pattern_{pattern}")
            sandboxed_results[pattern] = len(matches)
            total_sandboxed_time += pattern_time
        
        profiler.take_memory_snapshot()
        
        # Calculate overall pattern matching overhead
        pattern_overhead = ((total_sandboxed_time - total_direct_time) / 
                          total_direct_time) * 100
        
        # Verify similar results (allow for small differences due to path handling)
        for pattern in patterns:
            direct_count = direct_results[pattern]
            sandboxed_count = sandboxed_results[pattern]
            assert abs(direct_count - sandboxed_count) <= 2, f"Pattern {pattern} results differ significantly"
        
        # Performance assertion
        assert pattern_overhead < 10.0, f"Pattern matching overhead {pattern_overhead:.2f}% exceeds 10%"
        
        print(f"\nPattern Matching Performance:")
        print(f"  Total patterns tested: {len(patterns)}")
        print(f"  Pattern matching overhead: {pattern_overhead:.2f}%")
        print(f"  Average matches per pattern: {mean(sandboxed_results.values()):.1f}")
        
    def test_directory_traversal_performance(self, profiler, test_file_structure):
        """Test performance of directory tree traversal (walk operations)."""
        base_path, files = test_file_structure
        
        direct_fs = fsspec.filesystem('file') 
        sandboxed_fs = PathSandboxedFileSystem(direct_fs, str(base_path))
        
        profiler.take_memory_snapshot()
        
        # Test direct filesystem walk
        profiler.start_measurement("direct_walk")
        direct_paths = []
        for path, dirs, files_in_dir in direct_fs.walk(str(base_path)):
            direct_paths.extend([os.path.join(path, f) for f in files_in_dir])
        direct_walk_time = profiler.end_measurement("direct_walk")
        
        profiler.take_memory_snapshot()
        
        # Test sandboxed filesystem walk  
        profiler.start_measurement("sandboxed_walk")
        sandboxed_paths = []
        for path, dirs, files_in_dir in sandboxed_fs.walk(""):
            sandboxed_paths.extend([os.path.join(path, f) for f in files_in_dir])
        sandboxed_walk_time = profiler.end_measurement("sandboxed_walk")
        
        profiler.take_memory_snapshot()
        
        # Calculate walk performance overhead
        walk_overhead = ((sandboxed_walk_time - direct_walk_time) / 
                        direct_walk_time) * 100
        
        # Verify similar number of paths found
        assert abs(len(direct_paths) - len(sandboxed_paths)) <= 10  # Allow for path differences
        
        # Performance assertion
        assert walk_overhead < 15.0, f"Directory walk overhead {walk_overhead:.2f}% exceeds 15%"
        
        # Calculate traversal rate
        direct_files_per_sec = len(direct_paths) / direct_walk_time if direct_walk_time > 0 else 0
        sandboxed_files_per_sec = len(sandboxed_paths) / sandboxed_walk_time if sandboxed_walk_time > 0 else 0
        
        print(f"\nDirectory Traversal Performance:")
        print(f"  Files found: {len(sandboxed_paths)}")
        print(f"  Walk overhead: {walk_overhead:.2f}%")
        print(f"  Direct traversal rate: {direct_files_per_sec:.1f} files/sec")
        print(f"  Sandboxed traversal rate: {sandboxed_files_per_sec:.1f} files/sec")


@pytest.mark.performance
@pytest.mark.large_data
class TestStressAndScalability:
    """Stress testing and scalability limits for PathSandboxedFileSystem."""
    
    def test_high_volume_operations(self, profiler, tmp_path):
        """Test performance with high volume of operations (1000+ files)."""
        # Create large file structure
        files = create_test_file_structure(tmp_path, file_count=2000, dir_depth=5)
        
        direct_fs = fsspec.filesystem('file')
        sandboxed_fs = PathSandboxedFileSystem(direct_fs, str(tmp_path))
        
        # Test paths
        test_paths = [str(f.relative_to(tmp_path)) for f in files]
        
        profiler.take_memory_snapshot()
        
        # High volume exists() operations
        profiler.start_measurement("high_volume_exists")
        sandboxed_exists_results = []
        for path in test_paths:
            sandboxed_exists_results.append(sandboxed_fs.exists(path))
        exists_time = profiler.end_measurement("high_volume_exists")
        
        profiler.take_memory_snapshot()
        
        # High volume info() operations
        profiler.start_measurement("high_volume_info")
        sandboxed_info_results = []
        for path in test_paths[:500]:  # Limit for time efficiency
            try:
                info = sandboxed_fs.info(path)
                sandboxed_info_results.append(info['size'])
            except Exception:
                sandboxed_info_results.append(0)
        info_time = profiler.end_measurement("high_volume_info")
        
        profiler.take_memory_snapshot()
        
        # Performance metrics
        exists_ops_per_sec = len(test_paths) / exists_time if exists_time > 0 else 0
        info_ops_per_sec = len(sandboxed_info_results) / info_time if info_time > 0 else 0
        
        # Verify reasonable performance under high load
        assert exists_ops_per_sec > 100, f"exists() rate {exists_ops_per_sec:.1f} ops/sec too low"
        assert info_ops_per_sec > 50, f"info() rate {info_ops_per_sec:.1f} ops/sec too low"
        
        # Memory usage should be reasonable
        memory_stats = profiler.get_memory_stats()
        assert memory_stats["delta"] < 100, f"Memory usage {memory_stats['delta']:.1f}MB too high"
        
        print(f"\nHigh Volume Operations Performance:")
        print(f"  Files processed: {len(test_paths)}")
        print(f"  exists() rate: {exists_ops_per_sec:.1f} ops/sec")
        print(f"  info() rate: {info_ops_per_sec:.1f} ops/sec")
        print(f"  Memory delta: {memory_stats['delta']:.1f}MB")
        
    def test_memory_usage_under_load(self, profiler, tmp_path):
        """Test memory usage patterns under sustained load."""
        # Create test structure
        files = create_test_file_structure(tmp_path, file_count=1000)
        
        direct_fs = fsspec.filesystem('file')
        sandboxed_fs = PathSandboxedFileSystem(direct_fs, str(tmp_path))
        
        # Monitor memory usage over sustained operations
        def memory_monitor_worker():
            for _ in range(100):  # 100 samples over operation duration
                profiler.take_memory_snapshot()
                time.sleep(0.1)
        
        # Start memory monitoring
        memory_thread = threading.Thread(target=memory_monitor_worker)
        memory_thread.start()
        
        try:
            # Perform sustained operations
            profiler.start_measurement("sustained_operations")
            
            # Mix of different operations
            for cycle in range(10):  # 10 operation cycles
                # File existence checks
                for f in files[cycle*50:(cycle+1)*50]:
                    path = str(f.relative_to(tmp_path))
                    sandboxed_fs.exists(path)
                
                # Directory listings
                sandboxed_fs.ls(f"model_output")
                
                # Pattern matching
                sandboxed_fs.glob(f"**/*{cycle % 5}*.nc")
                
                # Force garbage collection periodically
                if cycle % 3 == 0:
                    gc.collect()
            
            sustained_time = profiler.end_measurement("sustained_operations")
            
        finally:
            memory_thread.join()
        
        # Analyze memory usage patterns
        memory_stats = profiler.get_memory_stats()
        memory_samples = profiler.memory_samples
        
        if len(memory_samples) > 10:
            memory_trend = memory_samples[-10:] 
            avg_recent = mean(memory_trend)
            initial_avg = mean(memory_samples[:10]) if len(memory_samples) > 10 else memory_stats['peak']
            
            memory_growth = avg_recent - initial_avg
        else:
            memory_growth = 0
        
        # Memory assertions
        assert memory_stats["delta"] < 150, f"Peak memory usage {memory_stats['delta']:.1f}MB too high"
        assert memory_growth < 50, f"Memory growth {memory_growth:.1f}MB indicates potential leak"
        
        operations_per_second = (10 * 50 + 10 + 10) / sustained_time if sustained_time > 0 else 0
        
        print(f"\nMemory Usage Under Load:")
        print(f"  Operations/sec: {operations_per_second:.1f}")
        print(f"  Peak memory delta: {memory_stats['delta']:.1f}MB")
        print(f"  Memory growth: {memory_growth:.1f}MB")
        print(f"  Memory samples: {len(memory_samples)}")
        

@pytest.mark.performance
@pytest.mark.benchmark
class TestPerformanceRegression:
    """Regression testing to detect performance degradation over time."""
    
    def test_performance_baseline_establishment(self, profiler, tmp_path):
        """Establish baseline performance metrics for regression testing."""
        # Create standardized test environment  
        files = create_test_file_structure(tmp_path, file_count=500)
        
        direct_fs = fsspec.filesystem('file')
        sandboxed_fs = PathSandboxedFileSystem(direct_fs, str(tmp_path))
        
        test_paths = [str(f.relative_to(tmp_path)) for f in files[:100]]
        
        # Standard operations for baseline
        baseline_metrics = {}
        
        profiler.take_memory_snapshot()
        
        # Path existence checks
        profiler.start_measurement("baseline_exists")
        for path in test_paths:
            sandboxed_fs.exists(path)
        baseline_metrics["exists_time"] = profiler.end_measurement("baseline_exists")
        
        # Directory listing
        profiler.start_measurement("baseline_ls")
        sandboxed_fs.ls("")
        baseline_metrics["ls_time"] = profiler.end_measurement("baseline_ls")
        
        # Pattern matching
        profiler.start_measurement("baseline_glob")
        sandboxed_fs.glob("**/*.nc")
        baseline_metrics["glob_time"] = profiler.end_measurement("baseline_glob")
        
        # File info operations
        profiler.start_measurement("baseline_info")
        for path in test_paths[:20]:
            try:
                sandboxed_fs.info(path)
            except Exception:
                pass
        baseline_metrics["info_time"] = profiler.end_measurement("baseline_info")
        
        profiler.take_memory_snapshot()
        memory_stats = profiler.get_memory_stats()
        baseline_metrics["memory_delta"] = memory_stats["delta"]
        
        # Performance targets (these would be updated as baselines change)
        baseline_targets = {
            "exists_time": 1.0,    # Should complete 100 exists() calls in < 1s
            "ls_time": 0.1,        # Directory listing should be < 0.1s
            "glob_time": 2.0,      # Pattern matching should be < 2s
            "info_time": 0.5,      # 20 info calls should be < 0.5s
            "memory_delta": 50,    # Memory delta should be < 50MB
        }
        
        # Regression detection
        for metric, value in baseline_metrics.items():
            target = baseline_targets[metric]
            assert value <= target, f"Performance regression: {metric} = {value:.3f} exceeds target {target}"
        
        print(f"\nPerformance Baseline Metrics:")
        for metric, value in baseline_metrics.items():
            target = baseline_targets[metric]
            percentage = (value / target) * 100
            print(f"  {metric}: {value:.3f} ({percentage:.1f}% of target)")
        
        return baseline_metrics
    
    def test_comparative_performance_analysis(self, profiler, tmp_path):
        """Compare performance with and without sandboxing to quantify total overhead."""
        files = create_test_file_structure(tmp_path, file_count=200)
        
        direct_fs = fsspec.filesystem('file')
        sandboxed_fs = PathSandboxedFileSystem(direct_fs, str(tmp_path))
        
        test_paths = [str(f.relative_to(tmp_path)) for f in files[:50]]
        
        # Comprehensive operation suite
        operations = {
            'exists': lambda fs, path: fs.exists(path if hasattr(fs, 'base_path') else str(tmp_path / path)),
            'size': lambda fs, path: fs.size(path if hasattr(fs, 'base_path') else str(tmp_path / path)),
            'isfile': lambda fs, path: fs.isfile(path if hasattr(fs, 'base_path') else str(tmp_path / path)),
        }
        
        performance_comparison = {}
        
        for op_name, op_func in operations.items():
            profiler.take_memory_snapshot()
            
            # Direct filesystem
            profiler.start_measurement(f"direct_{op_name}")
            direct_results = []
            for path in test_paths:
                try:
                    result = op_func(direct_fs, path)
                    direct_results.append(result)
                except Exception as e:
                    direct_results.append(None)
            direct_time = profiler.end_measurement(f"direct_{op_name}")
            
            # Sandboxed filesystem  
            profiler.start_measurement(f"sandboxed_{op_name}")
            sandboxed_results = []
            for path in test_paths:
                try:
                    result = op_func(sandboxed_fs, path)
                    sandboxed_results.append(result)
                except Exception as e:
                    sandboxed_results.append(None)
            sandboxed_time = profiler.end_measurement(f"sandboxed_{op_name}")
            
            profiler.take_memory_snapshot()
            
            # Calculate overhead
            overhead_percentage = ((sandboxed_time - direct_time) / direct_time) * 100 if direct_time > 0 else 0
            
            performance_comparison[op_name] = {
                'direct_time': direct_time,
                'sandboxed_time': sandboxed_time,
                'overhead_percentage': overhead_percentage,
                'operations_count': len(test_paths)
            }
            
            # Verify functional correctness
            matching_results = sum(1 for d, s in zip(direct_results, sandboxed_results) if d == s)
            match_percentage = (matching_results / len(test_paths)) * 100
            
            assert match_percentage > 95, f"Operation {op_name} results differ significantly: {match_percentage:.1f}% match"
            assert overhead_percentage < 20, f"Operation {op_name} overhead {overhead_percentage:.2f}% too high"
        
        # Overall performance summary
        total_overheads = [perf['overhead_percentage'] for perf in performance_comparison.values()]
        avg_overhead = mean(total_overheads)
        max_overhead = max(total_overheads)
        
        print(f"\nComparative Performance Analysis:")
        print(f"  Average overhead: {avg_overhead:.2f}%")
        print(f"  Maximum overhead: {max_overhead:.2f}%")
        
        for op_name, perf in performance_comparison.items():
            print(f"  {op_name}: {perf['overhead_percentage']:.2f}% overhead " +
                  f"({perf['operations_count']} ops in {perf['sandboxed_time']:.3f}s)")
        
        # Global performance assertion
        assert avg_overhead < 10, f"Average overhead {avg_overhead:.2f}% exceeds 10% target"
        
        return performance_comparison


@pytest.mark.performance
class TestOptimizationOpportunities:
    """Identify and test potential optimization opportunities."""
    
    def test_path_caching_effectiveness(self, profiler, tmp_path):
        """Test if path resolution caching would improve performance."""
        files = create_test_file_structure(tmp_path, file_count=100)
        
        direct_fs = fsspec.filesystem('file')
        sandboxed_fs = PathSandboxedFileSystem(direct_fs, str(tmp_path))
        
        # Test repeated path operations (simulates caching benefit)
        test_paths = [str(f.relative_to(tmp_path)) for f in files[:20]]
        
        profiler.take_memory_snapshot()
        
        # First pass - cold cache
        profiler.start_measurement("cold_path_operations")
        for path in test_paths:
            sandboxed_fs.exists(path)
        cold_time = profiler.end_measurement("cold_path_operations")
        
        # Second pass - warm cache (if caching was implemented)
        profiler.start_measurement("repeated_path_operations")
        for path in test_paths:
            sandboxed_fs.exists(path)
        repeated_time = profiler.end_measurement("repeated_path_operations")
        
        profiler.take_memory_snapshot()
        
        # Calculate potential for caching optimization
        if repeated_time < cold_time:
            caching_benefit = ((cold_time - repeated_time) / cold_time) * 100
            print(f"\nPath Caching Analysis:")
            print(f"  Cold operations: {cold_time:.3f}s")
            print(f"  Repeated operations: {repeated_time:.3f}s")
            print(f"  Potential caching benefit: {caching_benefit:.2f}%")
        else:
            print(f"\nPath Caching Analysis:")
            print(f"  No significant caching benefit detected")
            print(f"  Cold: {cold_time:.3f}s, Repeated: {repeated_time:.3f}s")
    
    def test_batch_operation_optimization(self, profiler, tmp_path):
        """Test potential for batch operation optimizations."""
        files = create_test_file_structure(tmp_path, file_count=100)
        
        direct_fs = fsspec.filesystem('file')
        sandboxed_fs = PathSandboxedFileSystem(direct_fs, str(tmp_path))
        
        test_paths = [str(f.relative_to(tmp_path)) for f in files[:50]]
        
        profiler.take_memory_snapshot()
        
        # Individual operations
        profiler.start_measurement("individual_operations")
        individual_results = []
        for path in test_paths:
            individual_results.append(sandboxed_fs.exists(path))
        individual_time = profiler.end_measurement("individual_operations")
        
        # Simulated batch operations (using find which is inherently batch)
        profiler.start_measurement("batch_simulation")
        all_files = sandboxed_fs.find("")
        batch_results = [str(tmp_path / path) in all_files for path in test_paths]
        batch_time = profiler.end_measurement("batch_simulation")
        
        profiler.take_memory_snapshot()
        
        # Verify functional equivalence
        matching_results = sum(1 for i, b in zip(individual_results, batch_results) if i == b)
        match_percentage = (matching_results / len(test_paths)) * 100
        
        if batch_time < individual_time and match_percentage > 90:
            batch_benefit = ((individual_time - batch_time) / individual_time) * 100
            print(f"\nBatch Operation Analysis:")
            print(f"  Individual operations: {individual_time:.3f}s")
            print(f"  Batch simulation: {batch_time:.3f}s") 
            print(f"  Potential batch benefit: {batch_benefit:.2f}%")
            print(f"  Result accuracy: {match_percentage:.1f}%")
        else:
            print(f"\nBatch Operation Analysis:")
            print(f"  No significant batch benefit detected")
            print(f"  Individual: {individual_time:.3f}s, Batch: {batch_time:.3f}s")


# Performance testing utility functions
def generate_performance_report(test_results: Dict) -> str:
    """Generate a comprehensive performance report."""
    report = [
        "PathSandboxedFileSystem Performance Analysis Report",
        "=" * 50,
        "",
        "EXECUTIVE SUMMARY:",
        f"- Path resolution overhead: {test_results.get('path_overhead', 'N/A')}%", 
        f"- Average operation overhead: {test_results.get('avg_overhead', 'N/A')}%",
        f"- Peak memory usage: {test_results.get('peak_memory', 'N/A')}MB",
        f"- Concurrent performance: {test_results.get('concurrent_perf', 'N/A')} ops/sec",
        "",
        "PERFORMANCE TARGETS:",
        "✓ Path resolution overhead < 5%",
        "✓ Bulk operations overhead < 10%", 
        "✓ I/O operations overhead < 5%",
        "✓ Memory usage < 100MB delta",
        "✓ Concurrent throughput > 100 ops/sec",
        "",
        "OPTIMIZATION RECOMMENDATIONS:",
        "1. Path resolution caching for repeated operations",
        "2. Batch operation optimizations for bulk workloads", 
        "3. Memory pooling for high-concurrency scenarios",
        "4. Lazy validation for performance-critical paths",
    ]
    
    return "\n".join(report)


if __name__ == "__main__":
    # Example usage for standalone performance testing
    print("PathSandboxedFileSystem Performance Test Suite")
    print("Run with: pytest -m performance tests/test_sandboxed_filesystem_performance.py")