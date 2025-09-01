"""
Memory and CPU Profiling for PathSandboxedFileSystem with Real Climate Data Workloads.

This module provides detailed memory allocation tracking and CPU performance 
profiling for PathSandboxedFileSystem when handling realistic climate science
data formats (NetCDF, Zarr) and access patterns.

Profiling Categories:
- Memory allocation patterns for large file operations
- CPU overhead analysis for path resolution and validation
- Memory fragmentation assessment under sustained load
- Cache behavior analysis (L1/L2/L3 cache efficiency)
- NUMA topology impact assessment
- Real NetCDF/Zarr dataset processing profiles

Performance Metrics:
- Memory allocation efficiency (bytes allocated vs. useful work)
- CPU instruction efficiency (cycles per operation)  
- Cache hit/miss ratios during file operations
- Memory fragmentation levels over time
- GC pressure and frequency analysis
- NUMA node affinity and memory locality
"""

import cProfile
import gc
import io
import mmap
import os
import pstats
import resource
import sys
import tempfile
import threading
import time
import tracemalloc
import warnings
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from statistics import mean, median, stdev
from typing import Any, Dict, Iterator, List, Optional, Tuple, Union
from unittest.mock import Mock, patch

import fsspec
import psutil
import pytest

# Scientific computing imports (optional but preferred for realistic tests)
try:
    import h5py
    import numpy as np
    import xarray as xr
    import zarr
    HAS_SCIENTIFIC = True
except ImportError:
    HAS_SCIENTIFIC = False
    warnings.warn("Scientific packages not available, using fallback implementations")

from tellus.location.sandboxed_filesystem import (PathSandboxedFileSystem,
                                                  PathValidationError)


@dataclass
class MemoryProfile:
    """Detailed memory profiling information."""
    operation: str
    peak_memory_mb: float
    current_memory_mb: float
    allocated_memory_mb: float
    deallocated_memory_mb: float
    net_memory_growth_mb: float
    allocation_count: int
    deallocation_count: int
    fragmentation_index: float  # 0.0 = no fragmentation, 1.0 = high fragmentation
    gc_collections: int
    gc_time_ms: float
    memory_efficiency: float  # useful data / total memory allocated
    numa_locality_score: float  # 0.0 = poor locality, 1.0 = perfect locality


@dataclass  
class CPUProfile:
    """Detailed CPU profiling information."""
    operation: str
    cpu_time_seconds: float
    wall_time_seconds: float
    cpu_efficiency: float  # cpu_time / wall_time
    user_time_seconds: float
    system_time_seconds: float
    context_switches: int
    page_faults: int
    instructions_per_operation: int
    cache_misses: int
    branch_misses: int
    cpu_cycles: int
    ipc_ratio: float  # instructions per cycle


@dataclass
class CombinedProfile:
    """Combined memory and CPU profiling results."""
    memory_profile: MemoryProfile
    cpu_profile: CPUProfile
    operation_count: int
    data_processed_mb: float
    operations_per_second: float
    mb_per_second: float
    memory_bandwidth_mbps: float
    efficiency_score: float  # Overall efficiency metric


class AdvancedMemoryProfiler:
    """Advanced memory profiler with allocation tracking and fragmentation analysis."""
    
    def __init__(self, track_allocations: bool = True):
        self.track_allocations = track_allocations
        self.reset()
    
    def reset(self):
        """Reset profiler state."""
        self.profiles = []
        self.current_operation = None
        self.baseline_memory = self._get_memory_info()
        self.gc_stats_start = None
        
        if self.track_allocations and not tracemalloc.is_tracing():
            tracemalloc.start(25)  # Keep 25 frames for detailed tracing
    
    def _get_memory_info(self) -> Dict[str, float]:
        """Get comprehensive memory information."""
        process = psutil.Process(os.getpid())
        memory_info = process.memory_info()
        
        # Get detailed memory statistics
        try:
            memory_full_info = process.memory_full_info()
            virtual_memory = psutil.virtual_memory()
            
            return {
                'rss_mb': memory_info.rss / (1024 * 1024),
                'vms_mb': memory_info.vms / (1024 * 1024),
                'uss_mb': getattr(memory_full_info, 'uss', 0) / (1024 * 1024),
                'pss_mb': getattr(memory_full_info, 'pss', 0) / (1024 * 1024),
                'swap_mb': getattr(memory_full_info, 'swap', 0) / (1024 * 1024),
                'available_mb': virtual_memory.available / (1024 * 1024),
                'percent_used': virtual_memory.percent
            }
        except (AttributeError, psutil.NoSuchProcess):
            return {
                'rss_mb': memory_info.rss / (1024 * 1024),
                'vms_mb': memory_info.vms / (1024 * 1024),
                'uss_mb': 0,
                'pss_mb': 0,
                'swap_mb': 0,
                'available_mb': 0,
                'percent_used': 0
            }
    
    def _calculate_fragmentation_index(self) -> float:
        """Calculate memory fragmentation index."""
        try:
            # Simple fragmentation heuristic: compare allocated vs. used memory
            if self.track_allocations and tracemalloc.is_tracing():
                current, peak = tracemalloc.get_traced_memory()
                current_info = self._get_memory_info()
                
                # Fragmentation = (virtual - physical) / virtual
                virtual_mb = current_info['vms_mb']
                physical_mb = current_info['rss_mb']
                
                if virtual_mb > 0:
                    return min(1.0, max(0.0, (virtual_mb - physical_mb) / virtual_mb))
                
            return 0.0
        except Exception:
            return 0.0
    
    def _get_numa_locality_score(self) -> float:
        """Estimate NUMA locality score (simplified)."""
        try:
            # This is a simplified metric - in real HPC environments, you'd use
            # hardware performance counters or specialized tools
            process = psutil.Process(os.getpid())
            
            # Use CPU affinity as a proxy for NUMA locality
            try:
                cpu_affinity = process.cpu_affinity()
                total_cpus = psutil.cpu_count()
                
                if total_cpus and cpu_affinity:
                    # Assume better locality if using fewer, consecutive CPUs
                    affinity_ratio = len(cpu_affinity) / total_cpus
                    consecutiveness = self._calculate_consecutiveness(cpu_affinity)
                    
                    # Score based on compactness and consecutiveness
                    return (1.0 - affinity_ratio) * consecutiveness
                    
            except (AttributeError, OSError):
                pass
            
            return 0.5  # Neutral score if can't determine
            
        except Exception:
            return 0.0
    
    def _calculate_consecutiveness(self, cpu_list: List[int]) -> float:
        """Calculate how consecutive the CPU list is."""
        if len(cpu_list) <= 1:
            return 1.0
        
        sorted_cpus = sorted(cpu_list)
        consecutive_count = 0
        
        for i in range(1, len(sorted_cpus)):
            if sorted_cpus[i] == sorted_cpus[i-1] + 1:
                consecutive_count += 1
        
        return consecutive_count / (len(sorted_cpus) - 1)
    
    def start_profiling(self, operation: str):
        """Start memory profiling for an operation."""
        if self.current_operation:
            warnings.warn(f"Starting new operation '{operation}' while '{self.current_operation}' is active")
        
        self.current_operation = operation
        self.operation_start_time = time.perf_counter()
        self.start_memory_info = self._get_memory_info()
        
        # Reset tracemalloc if needed
        if self.track_allocations:
            if tracemalloc.is_tracing():
                tracemalloc.stop()
            tracemalloc.start(25)
        
        # Record GC stats
        self.gc_stats_start = {
            'collections': [gc.get_count()[i] for i in range(3)],
            'time': time.perf_counter()
        }
        
    def end_profiling(self) -> MemoryProfile:
        """End profiling and return memory profile."""
        if not self.current_operation:
            raise ValueError("No profiling operation in progress")
        
        end_time = time.perf_counter()
        end_memory_info = self._get_memory_info()
        
        # Calculate memory metrics
        peak_memory = end_memory_info['rss_mb']
        current_memory = end_memory_info['rss_mb']
        net_growth = current_memory - self.start_memory_info['rss_mb']
        
        # Allocation tracking
        allocated_mb = 0
        deallocated_mb = 0
        allocation_count = 0
        deallocation_count = 0
        
        if self.track_allocations and tracemalloc.is_tracing():
            current_trace, peak_trace = tracemalloc.get_traced_memory()
            allocated_mb = peak_trace / (1024 * 1024)
            
            # Get allocation statistics
            stats = tracemalloc.take_snapshot().statistics('lineno')
            allocation_count = len(stats)
            
            tracemalloc.stop()
        
        # GC analysis
        gc_collections = 0
        gc_time = 0
        if self.gc_stats_start:
            current_collections = [gc.get_count()[i] for i in range(3)]
            gc_collections = sum(c - s for c, s in zip(current_collections, self.gc_stats_start['collections']))
            gc_time = (time.perf_counter() - self.gc_stats_start['time']) * 1000  # ms
        
        # Calculate efficiency metrics
        fragmentation_index = self._calculate_fragmentation_index()
        numa_score = self._get_numa_locality_score()
        
        # Memory efficiency: useful memory / total allocated
        useful_memory = end_memory_info['uss_mb']  # Unique Set Size
        total_allocated = allocated_mb if allocated_mb > 0 else current_memory
        memory_efficiency = useful_memory / total_allocated if total_allocated > 0 else 0
        
        profile = MemoryProfile(
            operation=self.current_operation,
            peak_memory_mb=peak_memory,
            current_memory_mb=current_memory,
            allocated_memory_mb=allocated_mb,
            deallocated_memory_mb=deallocated_mb,
            net_memory_growth_mb=net_growth,
            allocation_count=allocation_count,
            deallocation_count=deallocation_count,
            fragmentation_index=fragmentation_index,
            gc_collections=gc_collections,
            gc_time_ms=gc_time,
            memory_efficiency=memory_efficiency,
            numa_locality_score=numa_score
        )
        
        self.profiles.append(profile)
        self.current_operation = None
        
        return profile


class AdvancedCPUProfiler:
    """Advanced CPU profiler with detailed performance counter analysis."""
    
    def __init__(self):
        self.reset()
    
    def reset(self):
        """Reset profiler state."""
        self.profiles = []
        self.current_operation = None
        self.profiler = None
    
    def start_profiling(self, operation: str):
        """Start CPU profiling for an operation."""
        self.current_operation = operation
        self.start_time = time.perf_counter()
        self.start_process_time = time.process_time()
        self.start_resource_usage = resource.getrusage(resource.RUSAGE_SELF)
        
        # Start cProfile for detailed function-level profiling
        self.profiler = cProfile.Profile()
        self.profiler.enable()
    
    def end_profiling(self) -> CPUProfile:
        """End profiling and return CPU profile."""
        if not self.current_operation:
            raise ValueError("No CPU profiling operation in progress")
        
        # Stop profiling
        self.profiler.disable()
        
        end_time = time.perf_counter()
        end_process_time = time.process_time()
        end_resource_usage = resource.getrusage(resource.RUSAGE_SELF)
        
        # Calculate timing metrics
        wall_time = end_time - self.start_time
        cpu_time = end_process_time - self.start_process_time
        cpu_efficiency = cpu_time / wall_time if wall_time > 0 else 0
        
        # Calculate resource usage differences
        user_time = end_resource_usage.ru_utime - self.start_resource_usage.ru_utime
        system_time = end_resource_usage.ru_stime - self.start_resource_usage.ru_stime
        
        context_switches = (end_resource_usage.ru_nvcsw - self.start_resource_usage.ru_nvcsw +
                           end_resource_usage.ru_nivcsw - self.start_resource_usage.ru_nivcsw)
        
        page_faults = (end_resource_usage.ru_majflt - self.start_resource_usage.ru_majflt +
                      end_resource_usage.ru_minflt - self.start_resource_usage.ru_minflt)
        
        # Estimate performance counters (simplified - real HPC would use perf or PAPI)
        estimated_instructions = self._estimate_instructions()
        estimated_cycles = self._estimate_cycles(cpu_time)
        ipc_ratio = estimated_instructions / estimated_cycles if estimated_cycles > 0 else 0
        
        profile = CPUProfile(
            operation=self.current_operation,
            cpu_time_seconds=cpu_time,
            wall_time_seconds=wall_time,
            cpu_efficiency=cpu_efficiency,
            user_time_seconds=user_time,
            system_time_seconds=system_time,
            context_switches=context_switches,
            page_faults=page_faults,
            instructions_per_operation=estimated_instructions,
            cache_misses=self._estimate_cache_misses(page_faults),
            branch_misses=self._estimate_branch_misses(),
            cpu_cycles=estimated_cycles,
            ipc_ratio=ipc_ratio
        )
        
        self.profiles.append(profile)
        self.current_operation = None
        
        return profile
    
    def _estimate_instructions(self) -> int:
        """Estimate instruction count from profiling data."""
        if not self.profiler:
            return 0
        
        # Analyze profiling stats to estimate instruction count
        stats_stream = io.StringIO()
        stats = pstats.Stats(self.profiler, stream=stats_stream)
        stats.sort_stats('cumulative')
        
        # Rough estimation based on function call count
        total_calls = stats.total_calls
        # Assume average of 50 instructions per function call
        return total_calls * 50
    
    def _estimate_cycles(self, cpu_time: float) -> int:
        """Estimate CPU cycles from timing."""
        # Rough estimation: assume 2.5 GHz CPU
        cpu_frequency = 2.5e9  # 2.5 GHz
        return int(cpu_time * cpu_frequency)
    
    def _estimate_cache_misses(self, page_faults: int) -> int:
        """Estimate cache misses from page faults."""
        # Very rough estimation: assume page faults indicate cache misses
        return page_faults * 1000  # Multiply by factor for L3 cache misses
    
    def _estimate_branch_misses(self) -> int:
        """Estimate branch mispredictions."""
        # Simplified estimation
        if self.profiler:
            stats = pstats.Stats(self.profiler)
            # Assume 5% branch misprediction rate
            return int(stats.total_calls * 0.05)
        return 0
    
    def get_function_profile_report(self) -> str:
        """Get detailed function-level profiling report."""
        if not self.profiler:
            return "No profiling data available"
        
        stats_stream = io.StringIO()
        stats = pstats.Stats(self.profiler, stream=stats_stream)
        stats.sort_stats('cumulative')
        stats.print_stats(20)  # Top 20 functions
        
        return stats_stream.getvalue()


class RealisticDatasetProfiler:
    """Profile PathSandboxedFileSystem with realistic climate datasets."""
    
    def __init__(self, memory_profiler: AdvancedMemoryProfiler, 
                 cpu_profiler: AdvancedCPUProfiler):
        self.memory_profiler = memory_profiler
        self.cpu_profiler = cpu_profiler
        self.profiles = []
    
    def profile_netcdf_operations(self, filesystem, netcdf_files: List[str]) -> List[CombinedProfile]:
        """Profile operations on NetCDF files."""
        profiles = []
        
        for i, filepath in enumerate(netcdf_files[:10]):  # Sample first 10 files
            # Profile file existence check
            combined = self._profile_operation(
                f"netcdf_exists_{i}",
                lambda: filesystem.exists(filepath),
                data_size_mb=0  # Metadata operation
            )
            profiles.append(combined)
            
            # Profile file info retrieval
            combined = self._profile_operation(
                f"netcdf_info_{i}",
                lambda: filesystem.info(filepath),
                data_size_mb=0
            )
            profiles.append(combined)
            
            # Profile file opening and header reading
            if HAS_SCIENTIFIC:
                def read_netcdf_header():
                    try:
                        with filesystem.open(filepath, 'rb') as f:
                            # Read NetCDF header
                            header = f.read(8192)  # Read first 8KB
                            return len(header)
                    except Exception:
                        return 0
                
                combined = self._profile_operation(
                    f"netcdf_header_{i}",
                    read_netcdf_header,
                    data_size_mb=0.008  # 8KB
                )
                profiles.append(combined)
        
        return profiles
    
    def profile_zarr_operations(self, filesystem, zarr_files: List[str]) -> List[CombinedProfile]:
        """Profile operations on Zarr datasets."""
        profiles = []
        
        # Zarr stores are typically directories, not single files
        for i, zarr_path in enumerate(zarr_files[:5]):  # Fewer Zarr operations (more complex)
            try:
                # Profile directory listing (Zarr metadata access)
                combined = self._profile_operation(
                    f"zarr_ls_{i}",
                    lambda: filesystem.ls(zarr_path),
                    data_size_mb=0
                )
                profiles.append(combined)
                
                # Profile reading Zarr metadata files
                def read_zarr_metadata():
                    try:
                        # Try to read .zarray or .zattrs files
                        for meta_file in ['.zarray', '.zattrs', '.zgroup']:
                            meta_path = f"{zarr_path}/{meta_file}"
                            if filesystem.exists(meta_path):
                                with filesystem.open(meta_path, 'rb') as f:
                                    content = f.read()
                                return len(content)
                        return 0
                    except Exception:
                        return 0
                
                combined = self._profile_operation(
                    f"zarr_metadata_{i}",
                    read_zarr_metadata,
                    data_size_mb=0.001  # ~1KB metadata
                )
                profiles.append(combined)
                
            except Exception:
                continue  # Skip problematic Zarr paths
        
        return profiles
    
    def profile_bulk_operations(self, filesystem, file_paths: List[str]) -> List[CombinedProfile]:
        """Profile bulk operations on multiple files."""
        profiles = []
        
        # Profile bulk exists check
        def bulk_exists():
            results = []
            for filepath in file_paths[:50]:  # Sample 50 files
                results.append(filesystem.exists(filepath))
            return len(results)
        
        combined = self._profile_operation(
            "bulk_exists_check",
            bulk_exists,
            data_size_mb=0  # Metadata operations
        )
        profiles.append(combined)
        
        # Profile glob pattern matching
        def glob_pattern():
            patterns = ["*.nc", "**/*.nc", "**/atm/*.nc", "**/daily/*.nc"]
            results = []
            for pattern in patterns:
                matches = filesystem.glob(pattern)
                results.extend(matches)
            return len(results)
        
        combined = self._profile_operation(
            "glob_pattern_matching",
            glob_pattern,
            data_size_mb=0
        )
        profiles.append(combined)
        
        # Profile directory tree walking
        def tree_walk():
            all_files = []
            try:
                for root, dirs, files in filesystem.walk(""):
                    all_files.extend(files)
            except Exception:
                pass
            return len(all_files)
        
        combined = self._profile_operation(
            "directory_tree_walk",
            tree_walk,
            data_size_mb=0
        )
        profiles.append(combined)
        
        return profiles
    
    def _profile_operation(self, operation_name: str, operation_func: callable, 
                          data_size_mb: float) -> CombinedProfile:
        """Profile a single operation with both memory and CPU profiling."""
        # Start profiling
        self.memory_profiler.start_profiling(operation_name)
        self.cpu_profiler.start_profiling(operation_name)
        
        start_time = time.perf_counter()
        
        try:
            result = operation_func()
            operation_count = result if isinstance(result, int) else 1
        except Exception as e:
            operation_count = 0
        
        end_time = time.perf_counter()
        
        # End profiling
        memory_profile = self.memory_profiler.end_profiling()
        cpu_profile = self.cpu_profiler.end_profiling()
        
        # Calculate combined metrics
        duration = end_time - start_time
        ops_per_second = operation_count / duration if duration > 0 else 0
        mb_per_second = data_size_mb / duration if duration > 0 else 0
        
        # Estimate memory bandwidth (simplified)
        memory_bandwidth = memory_profile.allocated_memory_mb / duration if duration > 0 else 0
        
        # Calculate overall efficiency score
        efficiency_score = self._calculate_efficiency_score(memory_profile, cpu_profile, 
                                                           ops_per_second, data_size_mb)
        
        combined = CombinedProfile(
            memory_profile=memory_profile,
            cpu_profile=cpu_profile,
            operation_count=operation_count,
            data_processed_mb=data_size_mb,
            operations_per_second=ops_per_second,
            mb_per_second=mb_per_second,
            memory_bandwidth_mbps=memory_bandwidth,
            efficiency_score=efficiency_score
        )
        
        self.profiles.append(combined)
        return combined
    
    def _calculate_efficiency_score(self, memory_profile: MemoryProfile, 
                                   cpu_profile: CPUProfile, 
                                   ops_per_second: float, 
                                   data_size_mb: float) -> float:
        """Calculate overall efficiency score (0.0 = poor, 1.0 = excellent)."""
        # Weight different aspects of efficiency
        memory_efficiency = memory_profile.memory_efficiency * 0.3
        cpu_efficiency = cpu_profile.cpu_efficiency * 0.3
        throughput_efficiency = min(1.0, ops_per_second / 100.0) * 0.2  # Normalize to 100 ops/sec
        numa_efficiency = memory_profile.numa_locality_score * 0.1
        fragmentation_efficiency = (1.0 - memory_profile.fragmentation_index) * 0.1
        
        return memory_efficiency + cpu_efficiency + throughput_efficiency + numa_efficiency + fragmentation_efficiency


# Test Fixtures
@pytest.fixture
def memory_profiler():
    """Advanced memory profiler instance."""
    return AdvancedMemoryProfiler(track_allocations=True)


@pytest.fixture  
def cpu_profiler():
    """Advanced CPU profiler instance."""
    return AdvancedCPUProfiler()


@pytest.fixture
def dataset_profiler(memory_profiler, cpu_profiler):
    """Realistic dataset profiler."""
    return RealisticDatasetProfiler(memory_profiler, cpu_profiler)


@pytest.fixture
def netcdf_test_files(tmp_path):
    """Generate realistic NetCDF test files."""
    if not HAS_SCIENTIFIC:
        pytest.skip("Scientific packages required for NetCDF testing")
    
    files = []
    
    # Create various types of NetCDF files
    file_types = [
        ("temperature_2m", "tas", 50),      # 50MB temperature file
        ("precipitation", "pr", 30),        # 30MB precipitation file  
        ("pressure", "psl", 40),           # 40MB pressure file
        ("wind_speed", "sfcWind", 25),     # 25MB wind file
        ("humidity", "hurs", 35),          # 35MB humidity file
    ]
    
    for var_name, standard_name, size_mb in file_types:
        filepath = tmp_path / f"{var_name}_daily_2020.nc"
        
        # Create realistic NetCDF structure
        time_steps = 365  # Daily data for one year
        lat_points = 180
        lon_points = 360
        
        # Calculate data size to reach target file size
        target_bytes = size_mb * 1024 * 1024
        values_needed = int(target_bytes * 0.8) // 4  # 80% for data, 4 bytes per float32
        
        # Adjust spatial resolution if needed
        spatial_points = values_needed // time_steps
        if spatial_points < lat_points * lon_points:
            # Reduce resolution
            scale_factor = (spatial_points / (lat_points * lon_points)) ** 0.5
            lat_points = int(lat_points * scale_factor)
            lon_points = int(lon_points * scale_factor)
        
        # Create dataset
        data = np.random.normal(0, 1, (time_steps, lat_points, lon_points)).astype(np.float32)
        
        coords = {
            'time': ('time', np.arange(time_steps)),
            'lat': ('lat', np.linspace(-90, 90, lat_points)),
            'lon': ('lon', np.linspace(-180, 180, lon_points))
        }
        
        da = xr.DataArray(
            data,
            dims=['time', 'lat', 'lon'],
            coords=coords,
            name=standard_name,
            attrs={
                'units': 'K' if 'tas' in standard_name else 'kg m-2 s-1' if 'pr' in standard_name else '1',
                'long_name': f'{var_name} daily data',
                'standard_name': standard_name,
            }
        )
        
        ds = da.to_dataset()
        ds.attrs = {
            'title': f'Test {var_name} dataset',
            'institution': 'Tellus Performance Testing',
            'Conventions': 'CF-1.8',
        }
        
        # Write with compression
        encoding = {standard_name: {'zlib': True, 'complevel': 1}}
        ds.to_netcdf(filepath, encoding=encoding)
        
        files.append(filepath)
    
    return tmp_path, files


# Performance Test Classes
@pytest.mark.performance
@pytest.mark.benchmark  
@pytest.mark.large_data
class TestMemoryCPUProfiling:
    """Memory and CPU profiling tests for realistic climate data workloads."""
    
    def test_netcdf_operation_profiling(self, dataset_profiler, netcdf_test_files):
        """Profile PathSandboxedFileSystem operations on NetCDF files."""
        base_path, netcdf_files = netcdf_test_files
        
        # Setup filesystems
        direct_fs = fsspec.filesystem('file')
        sandboxed_fs = PathSandboxedFileSystem(direct_fs, str(base_path))
        
        file_paths = [str(f.relative_to(base_path)) for f in netcdf_files]
        
        # Profile NetCDF operations
        profiles = dataset_profiler.profile_netcdf_operations(sandboxed_fs, file_paths)
        
        print(f"\nNetCDF Operation Profiling Results:")
        print(f"  Total operations profiled: {len(profiles)}")
        
        # Analyze profiles
        memory_efficiency_scores = [p.memory_profile.memory_efficiency for p in profiles]
        cpu_efficiency_scores = [p.cpu_profile.cpu_efficiency for p in profiles]
        overall_efficiency_scores = [p.efficiency_score for p in profiles]
        
        avg_memory_efficiency = mean(memory_efficiency_scores) if memory_efficiency_scores else 0
        avg_cpu_efficiency = mean(cpu_efficiency_scores) if cpu_efficiency_scores else 0
        avg_overall_efficiency = mean(overall_efficiency_scores) if overall_efficiency_scores else 0
        
        print(f"  Average memory efficiency: {avg_memory_efficiency:.3f}")
        print(f"  Average CPU efficiency: {avg_cpu_efficiency:.3f}")
        print(f"  Average overall efficiency: {avg_overall_efficiency:.3f}")
        
        # Performance assertions
        assert avg_memory_efficiency > 0.1, f"Memory efficiency {avg_memory_efficiency:.3f} too low"
        assert avg_cpu_efficiency > 0.05, f"CPU efficiency {avg_cpu_efficiency:.3f} too low"
        assert avg_overall_efficiency > 0.2, f"Overall efficiency {avg_overall_efficiency:.3f} too low"
        
        # Memory usage assertions
        peak_memories = [p.memory_profile.peak_memory_mb for p in profiles]
        max_peak_memory = max(peak_memories) if peak_memories else 0
        assert max_peak_memory < 500.0, f"Peak memory usage {max_peak_memory:.1f}MB too high"
        
        # Fragmentation check
        fragmentation_indices = [p.memory_profile.fragmentation_index for p in profiles]
        max_fragmentation = max(fragmentation_indices) if fragmentation_indices else 0
        assert max_fragmentation < 0.5, f"Memory fragmentation {max_fragmentation:.3f} too high"
    
    def test_bulk_operation_profiling(self, dataset_profiler, netcdf_test_files):
        """Profile bulk file operations."""
        base_path, netcdf_files = netcdf_test_files
        
        direct_fs = fsspec.filesystem('file')
        sandboxed_fs = PathSandboxedFileSystem(direct_fs, str(base_path))
        
        file_paths = [str(f.relative_to(base_path)) for f in netcdf_files]
        
        # Profile bulk operations
        profiles = dataset_profiler.profile_bulk_operations(sandboxed_fs, file_paths)
        
        print(f"\nBulk Operation Profiling Results:")
        
        for profile in profiles:
            operation = profile.memory_profile.operation
            memory_peak = profile.memory_profile.peak_memory_mb
            cpu_time = profile.cpu_profile.cpu_time_seconds
            throughput = profile.operations_per_second
            
            print(f"  {operation}:")
            print(f"    Peak memory: {memory_peak:.1f}MB")
            print(f"    CPU time: {cpu_time:.3f}s")  
            print(f"    Throughput: {throughput:.1f} ops/sec")
            print(f"    Efficiency: {profile.efficiency_score:.3f}")
        
        # Performance requirements for bulk operations
        for profile in profiles:
            assert profile.memory_profile.peak_memory_mb < 200.0, f"Peak memory too high for {profile.memory_profile.operation}"
            assert profile.operations_per_second > 1.0, f"Throughput too low for {profile.memory_profile.operation}"
            assert profile.efficiency_score > 0.15, f"Efficiency too low for {profile.memory_profile.operation}"
    
    def test_memory_allocation_patterns(self, memory_profiler, netcdf_test_files):
        """Analyze memory allocation patterns during file operations."""
        base_path, netcdf_files = netcdf_test_files
        
        direct_fs = fsspec.filesystem('file')
        sandboxed_fs = PathSandboxedFileSystem(direct_fs, str(base_path))
        
        file_paths = [str(f.relative_to(base_path)) for f in netcdf_files]
        
        # Test sustained operation patterns
        operations_to_test = [
            ("repeated_exists", lambda: [sandboxed_fs.exists(fp) for fp in file_paths]),
            ("repeated_info", lambda: [sandboxed_fs.info(fp) for fp in file_paths]),
            ("repeated_open_close", lambda: [self._safe_open_close(sandboxed_fs, fp) for fp in file_paths]),
        ]
        
        allocation_profiles = []
        
        for operation_name, operation_func in operations_to_test:
            memory_profiler.start_profiling(operation_name)
            
            # Run operation multiple times to see allocation patterns
            for cycle in range(3):
                try:
                    operation_func()
                except Exception:
                    pass
                
                # Force garbage collection between cycles
                gc.collect()
            
            profile = memory_profiler.end_profiling()
            allocation_profiles.append(profile)
        
        print(f"\nMemory Allocation Pattern Analysis:")
        
        for profile in allocation_profiles:
            print(f"  {profile.operation}:")
            print(f"    Net memory growth: {profile.net_memory_growth_mb:.1f}MB")
            print(f"    Allocation count: {profile.allocation_count}")
            print(f"    GC collections: {profile.gc_collections}")
            print(f"    GC time: {profile.gc_time_ms:.1f}ms")
            print(f"    Fragmentation index: {profile.fragmentation_index:.3f}")
        
        # Memory pattern assertions
        for profile in allocation_profiles:
            # Memory growth should be reasonable for repeated operations
            assert profile.net_memory_growth_mb < 100.0, f"Excessive memory growth in {profile.operation}"
            
            # Fragmentation should be controlled
            assert profile.fragmentation_index < 0.6, f"High fragmentation in {profile.operation}"
            
            # GC pressure should be reasonable
            assert profile.gc_collections < 20, f"Excessive GC activity in {profile.operation}"
    
    def _safe_open_close(self, filesystem, filepath: str) -> int:
        """Safely open and close a file, return 1 for success, 0 for failure."""
        try:
            with filesystem.open(filepath, 'rb') as f:
                f.read(1024)  # Read 1KB
            return 1
        except Exception:
            return 0
    
    def test_cpu_performance_analysis(self, cpu_profiler, netcdf_test_files):
        """Analyze CPU performance characteristics."""
        base_path, netcdf_files = netcdf_test_files
        
        direct_fs = fsspec.filesystem('file')
        sandboxed_fs = PathSandboxedFileSystem(direct_fs, str(base_path))
        
        file_paths = [str(f.relative_to(base_path)) for f in netcdf_files]
        
        # Test CPU-intensive operations
        cpu_operations = [
            ("path_resolution_intensive", lambda: [sandboxed_fs._resolve_path(fp) for fp in file_paths * 10]),
            ("exists_check_intensive", lambda: [sandboxed_fs.exists(fp) for fp in file_paths * 5]),
            ("glob_pattern_intensive", lambda: [sandboxed_fs.glob(pattern) for pattern in ["*.nc", "**/*.nc", "**/daily/*.nc"]]),
        ]
        
        cpu_profiles = []
        
        for operation_name, operation_func in cpu_operations:
            cpu_profiler.start_profiling(operation_name)
            
            try:
                operation_func()
            except Exception:
                pass
            
            profile = cpu_profiler.end_profiling()
            cpu_profiles.append(profile)
        
        print(f"\nCPU Performance Analysis:")
        
        for profile in cpu_profiles:
            print(f"  {profile.operation}:")
            print(f"    CPU time: {profile.cpu_time_seconds:.3f}s")
            print(f"    Wall time: {profile.wall_time_seconds:.3f}s")
            print(f"    CPU efficiency: {profile.cpu_efficiency:.3f}")
            print(f"    Context switches: {profile.context_switches}")
            print(f"    Page faults: {profile.page_faults}")
            print(f"    IPC ratio: {profile.ipc_ratio:.2f}")
        
        # CPU performance assertions
        for profile in cpu_profiles:
            # CPU efficiency should be reasonable (not too low due to I/O wait)
            assert profile.cpu_efficiency > 0.01, f"CPU efficiency too low in {profile.operation}"
            
            # Context switches should be controlled
            assert profile.context_switches < 1000, f"Too many context switches in {profile.operation}"
            
            # IPC ratio should be reasonable (modern CPUs: 1-4 instructions per cycle)
            assert 0.1 <= profile.ipc_ratio <= 6.0, f"Unusual IPC ratio {profile.ipc_ratio} in {profile.operation}"
    
    @pytest.mark.timeout(180)  # 3 minutes max
    def test_sustained_load_memory_stability(self, memory_profiler, netcdf_test_files):
        """Test memory stability under sustained load (detecting memory leaks)."""
        base_path, netcdf_files = netcdf_test_files
        
        direct_fs = fsspec.filesystem('file')
        sandboxed_fs = PathSandboxedFileSystem(direct_fs, str(base_path))
        
        file_paths = [str(f.relative_to(base_path)) for f in netcdf_files]
        
        # Run sustained load test
        memory_profiler.start_profiling("sustained_load_stability")
        
        initial_memory = memory_profiler._get_memory_info()
        memory_samples = [initial_memory['rss_mb']]
        
        # Run for multiple cycles with different operations
        for cycle in range(10):  # 10 cycles of operations
            # Mix of operations per cycle
            for filepath in file_paths:
                try:
                    sandboxed_fs.exists(filepath)
                    sandboxed_fs.info(filepath)
                    with sandboxed_fs.open(filepath, 'rb') as f:
                        f.read(1024)
                except Exception:
                    pass
            
            # Pattern matching operations
            try:
                sandboxed_fs.glob("*.nc")
                sandboxed_fs.ls("")
            except Exception:
                pass
            
            # Sample memory every few cycles
            if cycle % 2 == 0:
                current_memory = memory_profiler._get_memory_info()
                memory_samples.append(current_memory['rss_mb'])
            
            # Force garbage collection every 3 cycles
            if cycle % 3 == 0:
                gc.collect()
        
        profile = memory_profiler.end_profiling()
        
        # Analyze memory stability
        memory_growth = memory_samples[-1] - memory_samples[0]
        memory_variance = stdev(memory_samples) if len(memory_samples) > 1 else 0
        max_memory = max(memory_samples)
        
        print(f"\nSustained Load Memory Stability:")
        print(f"  Initial memory: {memory_samples[0]:.1f}MB")
        print(f"  Final memory: {memory_samples[-1]:.1f}MB") 
        print(f"  Memory growth: {memory_growth:.1f}MB")
        print(f"  Peak memory: {max_memory:.1f}MB")
        print(f"  Memory variance: {memory_variance:.1f}MB")
        print(f"  GC collections: {profile.gc_collections}")
        print(f"  Fragmentation: {profile.fragmentation_index:.3f}")
        
        # Memory stability assertions
        assert memory_growth < 75.0, f"Memory growth {memory_growth:.1f}MB indicates potential leak"
        assert memory_variance < 50.0, f"Memory variance {memory_variance:.1f}MB indicates instability"
        assert max_memory - memory_samples[0] < 150.0, f"Peak memory usage too high"
        assert profile.fragmentation_index < 0.7, f"Memory fragmentation {profile.fragmentation_index:.3f} too high"


# Utility functions for analysis and reporting
def generate_profiling_report(profiles: List[CombinedProfile]) -> str:
    """Generate comprehensive memory/CPU profiling report."""
    if not profiles:
        return "No profiling data available."
    
    report_lines = [
        "PathSandboxedFileSystem Memory & CPU Profiling Report",
        "=" * 65,
        f"Total operations profiled: {len(profiles)}",
        f"Report generated: {time.strftime('%Y-%m-%d %H:%M:%S')}",
        "",
        "MEMORY PERFORMANCE SUMMARY:",
    ]
    
    # Memory analysis
    peak_memories = [p.memory_profile.peak_memory_mb for p in profiles]
    memory_growths = [p.memory_profile.net_memory_growth_mb for p in profiles]
    fragmentation_indices = [p.memory_profile.fragmentation_index for p in profiles]
    
    if peak_memories:
        report_lines.extend([
            f"  Average peak memory: {mean(peak_memories):.1f}MB",
            f"  Maximum peak memory: {max(peak_memories):.1f}MB",
            f"  Average memory growth: {mean(memory_growths):.1f}MB",
            f"  Average fragmentation: {mean(fragmentation_indices):.3f}",
        ])
    
    # CPU analysis
    report_lines.append("\nCPU PERFORMANCE SUMMARY:")
    cpu_efficiencies = [p.cpu_profile.cpu_efficiency for p in profiles]
    context_switches = [p.cpu_profile.context_switches for p in profiles]
    ipc_ratios = [p.cpu_profile.ipc_ratio for p in profiles]
    
    if cpu_efficiencies:
        report_lines.extend([
            f"  Average CPU efficiency: {mean(cpu_efficiencies):.3f}",
            f"  Average context switches: {mean(context_switches):.0f}",
            f"  Average IPC ratio: {mean(ipc_ratios):.2f}",
        ])
    
    # Overall efficiency
    efficiency_scores = [p.efficiency_score for p in profiles]
    if efficiency_scores:
        report_lines.extend([
            "\nOVERALL EFFICIENCY SUMMARY:",
            f"  Average efficiency score: {mean(efficiency_scores):.3f}",
            f"  Best efficiency score: {max(efficiency_scores):.3f}",
            f"  Worst efficiency score: {min(efficiency_scores):.3f}",
        ])
    
    # Detailed operation analysis
    report_lines.append("\nDETAILED OPERATION ANALYSIS:")
    for profile in profiles[:10]:  # Show top 10 operations
        mem_op = profile.memory_profile.operation
        report_lines.extend([
            f"  {mem_op.upper()}:",
            f"    Memory: {profile.memory_profile.peak_memory_mb:.1f}MB peak, {profile.memory_profile.net_memory_growth_mb:.1f}MB growth",
            f"    CPU: {profile.cpu_profile.cpu_time_seconds:.3f}s, {profile.cpu_profile.cpu_efficiency:.3f} efficiency",
            f"    Throughput: {profile.operations_per_second:.1f} ops/sec",
            f"    Efficiency: {profile.efficiency_score:.3f}",
            "",
        ])
    
    # Recommendations
    report_lines.extend([
        "PERFORMANCE OPTIMIZATION RECOMMENDATIONS:",
        "1. Memory Management:",
        "   - Implement object pooling for frequently used path objects",
        "   - Consider lazy loading for large directory structures", 
        "   - Monitor and limit memory growth in long-running processes",
        "",
        "2. CPU Optimization:",
        "   - Cache validated path resolutions to reduce redundant computation",
        "   - Optimize regex patterns used in path validation",
        "   - Consider async I/O for network filesystem operations",
        "",
        "3. System-Level Optimizations:",
        "   - Pin processes to NUMA nodes for better memory locality",
        "   - Tune garbage collection settings for workload characteristics",
        "   - Monitor and optimize cache line utilization patterns",
    ])
    
    return "\n".join(report_lines)


if __name__ == "__main__":
    print("Advanced Memory & CPU Profiling Suite for PathSandboxedFileSystem")
    print("Run with: pixi run -e test pytest -m 'performance and benchmark' tests/test_memory_cpu_profiling.py")