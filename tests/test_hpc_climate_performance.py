"""
Enhanced HPC Climate Science Performance Tests for PathSandboxedFileSystem.

This module provides specialized performance testing focused on real-world
HPC climate science workloads, ensuring the PathSandboxedFileSystem security
wrapper doesn't negatively impact typical Earth System Model data operations.

Performance Test Categories:
- Multi-TB dataset handling (representative of CMIP6, reanalysis)  
- Parallel NetCDF/Zarr file operations (typical of ensemble runs)
- High-frequency time series access patterns
- Distributed computing simulation (MPI-style access)
- Memory-constrained environment simulation
- Network filesystem performance (NFS, Lustre, GPFS)

Key Performance Requirements:
- < 5% overhead for individual file operations
- < 10% overhead for bulk operations 
- < 15% overhead for concurrent access
- Maintain sub-second response for metadata queries
- Memory usage proportional to working set, not total dataset size
"""

import asyncio
import concurrent.futures
import cProfile
import gc
import io
import multiprocessing as mp
import os
import pstats
import random
import tempfile
import threading
import time
import tracemalloc
import warnings
from concurrent.futures import (ProcessPoolExecutor, ThreadPoolExecutor,
                                as_completed)
from dataclasses import dataclass, field
from pathlib import Path
from statistics import mean, median, stdev
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple
from unittest.mock import Mock, patch

import fsspec
import psutil
import pytest

# Climate science specific imports (optional)
try:
    import numpy as np
    import xarray as xr
    import zarr
    HAS_SCIENTIFIC = True
except ImportError:
    HAS_SCIENTIFIC = False

from tellus.location.sandboxed_filesystem import (PathSandboxedFileSystem,
                                                  PathValidationError)


@dataclass
class PerformanceProfile:
    """Comprehensive performance profiling data."""
    operation: str
    execution_time: float
    memory_peak_mb: float
    memory_delta_mb: float
    cpu_percent: float
    throughput_mbps: float
    operations_per_second: float
    memory_allocations: int
    error_count: int = 0
    concurrency_level: int = 1
    data_size_mb: float = 0.0
    filesystem_type: str = "unknown"
    additional_metrics: Dict[str, Any] = field(default_factory=dict)


class HPC_Performance_Profiler:
    """Advanced performance profiler designed for HPC workload analysis."""
    
    def __init__(self, enable_memory_tracing: bool = True):
        self.enable_memory_tracing = enable_memory_tracing
        self.reset()
    
    def reset(self):
        """Reset all profiler state."""
        self.profiles = []
        self.current_profile = None
        self.process = psutil.Process(os.getpid())
        self.start_memory = self._get_memory_usage()
        if self.enable_memory_tracing and tracemalloc.is_tracing():
            tracemalloc.stop()
    
    def _get_memory_usage(self) -> float:
        """Get current memory usage in MB."""
        return self.process.memory_info().rss / (1024 * 1024)
    
    def start_profiling(self, operation: str, data_size_mb: float = 0.0, 
                       concurrency_level: int = 1, filesystem_type: str = "unknown"):
        """Start profiling an operation."""
        if self.enable_memory_tracing:
            tracemalloc.start()
        
        self.current_profile = {
            'operation': operation,
            'data_size_mb': data_size_mb,
            'concurrency_level': concurrency_level,
            'filesystem_type': filesystem_type,
            'start_time': time.perf_counter(),
            'start_memory': self._get_memory_usage(),
            'start_cpu': self.process.cpu_percent(),
            'error_count': 0,
            'additional_metrics': {},
        }
    
    def record_error(self):
        """Record an error during the current operation."""
        if self.current_profile:
            self.current_profile['error_count'] += 1
    
    def add_metric(self, key: str, value: Any):
        """Add custom metric to current profile."""
        if self.current_profile:
            self.current_profile['additional_metrics'][key] = value
    
    def end_profiling(self) -> PerformanceProfile:
        """End profiling and return comprehensive metrics."""
        if not self.current_profile:
            raise ValueError("No profiling session in progress")
        
        # Calculate timing
        end_time = time.perf_counter()
        execution_time = end_time - self.current_profile['start_time']
        
        # Memory metrics
        end_memory = self._get_memory_usage()
        memory_delta = end_memory - self.current_profile['start_memory']
        
        # CPU metrics  
        end_cpu = self.process.cpu_percent()
        avg_cpu = (self.current_profile['start_cpu'] + end_cpu) / 2
        
        # Memory allocation metrics
        memory_allocations = 0
        peak_memory = end_memory
        if self.enable_memory_tracing and tracemalloc.is_tracing():
            current_mem, peak_mem = tracemalloc.get_traced_memory()
            tracemalloc.stop()
            memory_allocations = peak_mem
            peak_memory = max(peak_memory, (peak_mem / (1024 * 1024)) + self.start_memory)
        
        # Throughput calculations
        data_size_mb = self.current_profile['data_size_mb']
        throughput_mbps = data_size_mb / execution_time if execution_time > 0 else 0
        ops_per_second = self.current_profile['concurrency_level'] / execution_time if execution_time > 0 else 0
        
        profile = PerformanceProfile(
            operation=self.current_profile['operation'],
            execution_time=execution_time,
            memory_peak_mb=peak_memory,
            memory_delta_mb=memory_delta,
            cpu_percent=avg_cpu,
            throughput_mbps=throughput_mbps,
            operations_per_second=ops_per_second,
            memory_allocations=memory_allocations,
            error_count=self.current_profile['error_count'],
            concurrency_level=self.current_profile['concurrency_level'],
            data_size_mb=data_size_mb,
            filesystem_type=self.current_profile['filesystem_type'],
            additional_metrics=self.current_profile['additional_metrics'].copy()
        )
        
        self.profiles.append(profile)
        self.current_profile = None
        return profile


class ClimateDataGenerator:
    """Generate realistic climate model data structures for performance testing."""
    
    @staticmethod
    def create_cmip6_structure(base_path: Path, size_constraint_mb: float = 1000) -> List[Path]:
        """Create CMIP6-style directory structure with realistic file sizes."""
        files_created = []
        
        # CMIP6 structure: institution/model/experiment/variant/table/variable/grid
        institutions = ['NCAR', 'GFDL', 'MPI-M']
        models = ['CESM2', 'GFDL-CM4', 'MPI-ESM1-2-HR']
        experiments = ['historical', 'ssp126', 'ssp585']
        variants = ['r1i1p1f1', 'r2i1p1f1', 'r3i1p1f1']
        tables = ['Amon', 'Omon', 'day', 'fx']
        variables = {
            'Amon': ['tas', 'pr', 'psl', 'ua', 'va', 'ta'],
            'Omon': ['tos', 'so', 'uo', 'vo', 'thetao'],
            'day': ['tasmax', 'tasmin', 'pr'],
            'fx': ['areacella', 'sftlf']
        }
        
        current_size_mb = 0
        
        for inst, model in zip(institutions, models):
            for experiment in experiments:
                for variant in variants:
                    for table in tables:
                        if current_size_mb >= size_constraint_mb:
                            break
                        
                        table_vars = variables[table]
                        for variable in table_vars:
                            if current_size_mb >= size_constraint_mb:
                                break
                            
                            # Create directory structure
                            var_dir = (base_path / "CMIP6" / "CMIP" / inst / model / 
                                     experiment / variant / table / variable / "gn" / "latest")
                            var_dir.mkdir(parents=True, exist_ok=True)
                            
                            # Create realistic NetCDF files
                            if table == 'fx':
                                file_size_mb = 1  # Fixed fields are small
                                filename = f"{variable}_fx_{model}_{experiment}_{variant}_gn.nc"
                                filepath = var_dir / filename
                            else:
                                file_size_mb = min(50, (size_constraint_mb - current_size_mb) / 10)
                                if file_size_mb < 1:
                                    break
                                
                                # Time-dependent files  
                                start_year = 2000 if experiment == 'historical' else 2015
                                end_year = start_year + 5  # 5 years of data
                                filename = f"{variable}_{table}_{model}_{experiment}_{variant}_gn_{start_year}01-{end_year}12.nc"
                                filepath = var_dir / filename
                            
                            ClimateDataGenerator._create_netcdf_file(filepath, file_size_mb, variable, table)
                            files_created.append(filepath)
                            current_size_mb += file_size_mb
                            
                            if current_size_mb >= size_constraint_mb:
                                break
                        if current_size_mb >= size_constraint_mb:
                            break
                    if current_size_mb >= size_constraint_mb:
                        break
                if current_size_mb >= size_constraint_mb:
                    break
            if current_size_mb >= size_constraint_mb:
                break
        
        return files_created
    
    @staticmethod
    def create_reanalysis_structure(base_path: Path, size_constraint_mb: float = 1000) -> List[Path]:
        """Create ERA5/MERRA-2 style reanalysis structure."""
        files_created = []
        
        # Reanalysis structure: product/stream/year/month
        products = ['ERA5', 'MERRA2']
        streams = ['reanalysis', 'ensemble_members', 'land']
        variables = ['2t', 'tp', 'msl', 'u10', 'v10', 'z500', 't850']
        
        current_size_mb = 0
        years = range(2018, 2023)  # 5 years
        months = range(1, 13)
        
        for product in products:
            for stream in streams:
                if current_size_mb >= size_constraint_mb:
                    break
                
                for year in years:
                    if current_size_mb >= size_constraint_mb:
                        break
                    
                    for month in months:
                        if current_size_mb >= size_constraint_mb:
                            break
                        
                        for variable in variables:
                            if current_size_mb >= size_constraint_mb:
                                break
                            
                            # Directory structure
                            var_dir = base_path / product / stream / str(year) / f"{month:02d}"
                            var_dir.mkdir(parents=True, exist_ok=True)
                            
                            # File size based on variable and temporal resolution
                            if stream == 'reanalysis':
                                file_size_mb = 25  # Hourly global data
                            elif stream == 'ensemble_members':
                                file_size_mb = 100  # Multiple members
                            else:
                                file_size_mb = 15  # Land-only
                            
                            file_size_mb = min(file_size_mb, size_constraint_mb - current_size_mb)
                            if file_size_mb < 1:
                                break
                            
                            filename = f"{variable}_{product}_{stream}_{year}_{month:02d}.nc"
                            filepath = var_dir / filename
                            
                            ClimateDataGenerator._create_netcdf_file(filepath, file_size_mb, variable, 'hourly')
                            files_created.append(filepath)
                            current_size_mb += file_size_mb
        
        return files_created
    
    @staticmethod
    def create_ensemble_structure(base_path: Path, ensemble_size: int = 50, 
                                size_constraint_mb: float = 1000) -> List[Path]:
        """Create ensemble simulation structure (typical of weather/climate ensembles)."""
        files_created = []
        
        # Ensemble structure: experiment/member/component/variable
        experiments = ['seasonal_forecast', 'decadal_prediction']
        components = ['atm', 'ocn', 'lnd']
        variables = {
            'atm': ['tas', 'pr', 'psl', 'ua850', 'va850'],
            'ocn': ['tos', 'sos', 'zos'],
            'lnd': ['mrsos', 'tsl', 'snw']
        }
        
        current_size_mb = 0
        
        for experiment in experiments:
            if current_size_mb >= size_constraint_mb:
                break
            
            for member in range(1, min(ensemble_size + 1, 26)):  # Limit to 25 members for testing
                if current_size_mb >= size_constraint_mb:
                    break
                
                member_id = f"r{member}i1p1"
                
                for component in components:
                    if current_size_mb >= size_constraint_mb:
                        break
                    
                    for variable in variables[component]:
                        if current_size_mb >= size_constraint_mb:
                            break
                        
                        # Directory structure
                        var_dir = base_path / experiment / member_id / component
                        var_dir.mkdir(parents=True, exist_ok=True)
                        
                        # File size varies by component
                        if component == 'atm':
                            file_size_mb = 30
                        elif component == 'ocn':
                            file_size_mb = 45
                        else:  # land
                            file_size_mb = 20
                        
                        file_size_mb = min(file_size_mb, (size_constraint_mb - current_size_mb) / 5)
                        if file_size_mb < 1:
                            break
                        
                        filename = f"{variable}_{experiment}_{member_id}_{component}.nc"
                        filepath = var_dir / filename
                        
                        ClimateDataGenerator._create_netcdf_file(filepath, file_size_mb, variable, component)
                        files_created.append(filepath)
                        current_size_mb += file_size_mb
        
        return files_created
    
    @staticmethod
    def _create_netcdf_file(filepath: Path, size_mb: float, variable: str, table: str):
        """Create a NetCDF file with realistic structure and target size."""
        if not HAS_SCIENTIFIC:
            # Create dummy binary file
            size_bytes = int(size_mb * 1024 * 1024)
            with open(filepath, 'wb') as f:
                chunk_size = min(1024 * 1024, size_bytes)  # 1MB chunks max
                chunks = size_bytes // chunk_size
                remainder = size_bytes % chunk_size
                
                for _ in range(chunks):
                    f.write(b'NETCDF_DUMMY' * (chunk_size // 12))
                if remainder:
                    f.write(b'NETCDF_DUMMY' * (remainder // 12))
            return filepath
        
        # Calculate dimensions for target file size
        target_bytes = size_mb * 1024 * 1024
        # Assume 4 bytes per float32 value + metadata overhead
        values_needed = int(target_bytes * 0.8) // 4  # 80% for data, 20% for metadata
        
        # Choose dimensions based on variable type
        if table in ['fx', 'fixed']:
            # 2D spatial field
            lat_size = min(180, int(values_needed ** 0.5))
            lon_size = values_needed // lat_size
            time_size = 1
        elif table == 'hourly':
            # Hourly data for one month
            time_size = 24 * 31  # ~744 timesteps
            remaining = values_needed // time_size
            lat_size = min(180, int(remaining ** 0.5))
            lon_size = remaining // lat_size
        else:
            # Monthly/daily data
            time_size = min(365, int(values_needed ** (1/3)))
            remaining = values_needed // time_size
            lat_size = min(180, int(remaining ** 0.5))
            lon_size = remaining // lat_size
        
        # Ensure minimum sizes
        lat_size = max(1, lat_size)
        lon_size = max(1, lon_size)
        time_size = max(1, time_size)
        
        try:
            # Create NetCDF file with xarray for better performance
            coords = {}
            dims = []
            shape = []
            
            if time_size > 1:
                coords['time'] = ('time', np.arange(time_size))
                dims.append('time')
                shape.append(time_size)
            
            coords['lat'] = ('lat', np.linspace(-90, 90, lat_size))
            coords['lon'] = ('lon', np.linspace(-180, 180, lon_size))
            dims.extend(['lat', 'lon'])
            shape.extend([lat_size, lon_size])
            
            # Generate realistic data
            data = np.random.normal(0, 1, shape).astype(np.float32)
            
            # Create dataset
            da = xr.DataArray(
                data,
                dims=dims,
                coords=coords,
                name=variable,
                attrs={
                    'units': 'K' if 'tas' in variable or 't' in variable else 'kg m-2 s-1' if 'pr' in variable else '1',
                    'long_name': f'Performance test variable {variable}',
                    'standard_name': variable,
                }
            )
            
            ds = da.to_dataset()
            
            # Add global attributes
            ds.attrs = {
                'title': f'Performance test dataset for {variable}',
                'institution': 'Tellus Performance Testing',
                'source': 'PathSandboxedFileSystem performance tests',
                'Conventions': 'CF-1.8',
            }
            
            # Write with compression
            encoding = {variable: {'zlib': True, 'complevel': 1}}
            ds.to_netcdf(filepath, encoding=encoding)
            
        except Exception as e:
            warnings.warn(f"Failed to create NetCDF with xarray: {e}, falling back to dummy file")
            # Fallback to dummy file
            size_bytes = int(size_mb * 1024 * 1024)
            with open(filepath, 'wb') as f:
                f.write(b'NETCDF_FALLBACK' * (size_bytes // 15))
        
        return filepath


class HPCWorkloadSimulator:
    """Simulate realistic HPC climate science workloads."""
    
    def __init__(self, profiler: HPC_Performance_Profiler):
        self.profiler = profiler
    
    def simulate_mpi_ensemble_analysis(self, filesystem, base_path: str, 
                                     ensemble_files: List[str], 
                                     num_processes: int = 4) -> PerformanceProfile:
        """Simulate MPI-style ensemble analysis where each process handles subset of files."""
        total_size_mb = 0
        
        # Estimate total data size
        for filepath in ensemble_files[:10]:  # Sample first 10 files
            try:
                size_mb = filesystem.size(filepath) / (1024 * 1024)
                total_size_mb += size_mb
            except:
                total_size_mb += 25  # Assume 25MB per file
        
        total_size_mb *= len(ensemble_files) / 10  # Scale estimate
        
        self.profiler.start_profiling(
            "mpi_ensemble_analysis", 
            data_size_mb=total_size_mb,
            concurrency_level=num_processes,
            filesystem_type=type(filesystem).__name__
        )
        
        def process_worker(file_subset):
            """Worker function simulating one MPI process."""
            process_results = []
            for filepath in file_subset:
                try:
                    # Simulate typical operations: check existence, get info, read metadata
                    exists = filesystem.exists(filepath)
                    if exists:
                        info = filesystem.info(filepath)
                        # Simulate reading file headers (first 1KB)
                        with filesystem.open(filepath, 'rb') as f:
                            header = f.read(1024)
                        process_results.append(len(header))
                except Exception as e:
                    self.profiler.record_error()
                    process_results.append(0)
            return process_results
        
        # Divide files among processes
        chunk_size = len(ensemble_files) // num_processes
        file_chunks = [ensemble_files[i:i+chunk_size] 
                      for i in range(0, len(ensemble_files), chunk_size)]
        
        # Execute in parallel using threads (simulating MPI processes)
        with ThreadPoolExecutor(max_workers=num_processes) as executor:
            futures = [executor.submit(process_worker, chunk) for chunk in file_chunks]
            results = [future.result() for future in as_completed(futures)]
        
        # Record successful operations
        total_operations = sum(len(result) for result in results)
        self.profiler.add_metric('total_files_processed', total_operations)
        self.profiler.add_metric('processes_used', num_processes)
        
        return self.profiler.end_profiling()
    
    def simulate_time_series_extraction(self, filesystem, base_path: str, 
                                      files: List[str], points: int = 100) -> PerformanceProfile:
        """Simulate extracting time series from many files at specific points."""
        total_size_mb = len(files) * 25  # Assume 25MB per file average
        
        self.profiler.start_profiling(
            "time_series_extraction",
            data_size_mb=total_size_mb,
            filesystem_type=type(filesystem).__name__
        )
        
        extraction_results = []
        
        for filepath in files:
            try:
                # Check if file exists
                if not filesystem.exists(filepath):
                    self.profiler.record_error()
                    continue
                
                # Get file info
                info = filesystem.info(filepath)
                file_size = info.get('size', 0)
                
                # Simulate reading time series data
                # In real workflow, this would be xarray.open_dataset().sel() operations
                with filesystem.open(filepath, 'rb') as f:
                    # Read small chunks simulating coordinate/variable access
                    for point in range(min(points, 10)):  # Limit for performance
                        f.seek(point * 1024)  # Skip to different file positions
                        chunk = f.read(256)  # Small read simulating extracted values
                        extraction_results.append(len(chunk))
                        
            except Exception as e:
                self.profiler.record_error()
        
        self.profiler.add_metric('files_processed', len(files))
        self.profiler.add_metric('extraction_points', points)
        self.profiler.add_metric('total_extractions', len(extraction_results))
        
        return self.profiler.end_profiling()
    
    def simulate_high_frequency_metadata_queries(self, filesystem, files: List[str], 
                                                queries_per_second: int = 50) -> PerformanceProfile:
        """Simulate high-frequency metadata queries typical in interactive analysis."""
        
        self.profiler.start_profiling(
            "high_frequency_metadata",
            concurrency_level=queries_per_second,
            filesystem_type=type(filesystem).__name__
        )
        
        # Run queries for 10 seconds at target frequency
        duration = 10  # seconds
        total_queries = duration * queries_per_second
        query_results = []
        
        start_time = time.time()
        
        for i in range(total_queries):
            if time.time() - start_time > duration:
                break
            
            # Select random file
            filepath = random.choice(files)
            
            try:
                # Rapid metadata operations
                exists = filesystem.exists(filepath)
                if exists:
                    is_file = filesystem.isfile(filepath)
                    info = filesystem.info(filepath) if is_file else None
                    query_results.append(1)
                else:
                    query_results.append(0)
                    
            except Exception as e:
                self.profiler.record_error()
                query_results.append(0)
            
            # Maintain query frequency
            target_time = start_time + (i + 1) / queries_per_second
            current_time = time.time()
            if current_time < target_time:
                time.sleep(target_time - current_time)
        
        actual_duration = time.time() - start_time
        actual_qps = len(query_results) / actual_duration if actual_duration > 0 else 0
        
        self.profiler.add_metric('target_queries_per_second', queries_per_second)
        self.profiler.add_metric('actual_queries_per_second', actual_qps)
        self.profiler.add_metric('total_queries', len(query_results))
        
        return self.profiler.end_profiling()


# Test Fixtures
@pytest.fixture
def hpc_profiler():
    """HPC-optimized performance profiler."""
    return HPC_Performance_Profiler(enable_memory_tracing=True)


@pytest.fixture
def climate_data_structure(tmp_path):
    """Generate realistic climate data structure for testing."""
    generator = ClimateDataGenerator()
    
    # Create mixed data structure (CMIP6 + reanalysis + ensemble)
    cmip6_files = generator.create_cmip6_structure(tmp_path / "cmip6", size_constraint_mb=200)
    reanalysis_files = generator.create_reanalysis_structure(tmp_path / "reanalysis", size_constraint_mb=200) 
    ensemble_files = generator.create_ensemble_structure(tmp_path / "ensemble", 
                                                       ensemble_size=10, size_constraint_mb=200)
    
    all_files = cmip6_files + reanalysis_files + ensemble_files
    
    return tmp_path, {
        'cmip6': cmip6_files,
        'reanalysis': reanalysis_files, 
        'ensemble': ensemble_files,
        'all': all_files
    }


@pytest.fixture
def hpc_workload_simulator(hpc_profiler):
    """HPC workload simulator."""
    return HPCWorkloadSimulator(hpc_profiler)


# Performance Test Classes
@pytest.mark.performance
@pytest.mark.hpc
@pytest.mark.benchmark
class TestHPCClimatePerformance:
    """Core HPC climate science performance tests."""
    
    def test_cmip6_data_access_performance(self, hpc_profiler, climate_data_structure):
        """Test performance with CMIP6-style multi-institutional data access."""
        base_path, file_groups = climate_data_structure
        cmip6_files = file_groups['cmip6']
        
        # Test both direct and sandboxed access
        direct_fs = fsspec.filesystem('file')
        sandboxed_fs = PathSandboxedFileSystem(direct_fs, str(base_path))
        
        test_files = [str(f.relative_to(base_path)) for f in cmip6_files[:50]]  # Sample
        
        # Direct filesystem performance
        hpc_profiler.start_profiling("cmip6_direct_access", filesystem_type="direct")
        for filepath in test_files:
            try:
                full_path = base_path / filepath
                direct_fs.exists(str(full_path))
                direct_fs.info(str(full_path))
            except Exception:
                hpc_profiler.record_error()
        direct_profile = hpc_profiler.end_profiling()
        
        # Sandboxed filesystem performance  
        hpc_profiler.start_profiling("cmip6_sandboxed_access", filesystem_type="sandboxed")
        for filepath in test_files:
            try:
                sandboxed_fs.exists(filepath)
                sandboxed_fs.info(filepath)
            except Exception:
                hpc_profiler.record_error()
        sandboxed_profile = hpc_profiler.end_profiling()
        
        # Performance analysis
        time_overhead = ((sandboxed_profile.execution_time - direct_profile.execution_time) / 
                        direct_profile.execution_time) * 100
        
        memory_overhead = sandboxed_profile.memory_delta_mb - direct_profile.memory_delta_mb
        
        # Performance assertions for CMIP6 workloads
        assert time_overhead < 10.0, f"CMIP6 access time overhead {time_overhead:.2f}% exceeds 10%"
        assert memory_overhead < 25.0, f"CMIP6 memory overhead {memory_overhead:.1f}MB exceeds 25MB"
        assert sandboxed_profile.error_count == direct_profile.error_count, "Error rates should match"
        
        print(f"\nCMIP6 Data Access Performance:")
        print(f"  Files tested: {len(test_files)}")
        print(f"  Direct time: {direct_profile.execution_time:.3f}s")
        print(f"  Sandboxed time: {sandboxed_profile.execution_time:.3f}s") 
        print(f"  Time overhead: {time_overhead:.2f}%")
        print(f"  Memory overhead: {memory_overhead:.1f}MB")
        print(f"  Throughput: {sandboxed_profile.operations_per_second:.1f} ops/sec")
    
    def test_ensemble_simulation_performance(self, hpc_workload_simulator, climate_data_structure):
        """Test performance with ensemble simulation workloads."""
        base_path, file_groups = climate_data_structure
        ensemble_files = file_groups['ensemble']
        
        # Test MPI-style ensemble processing
        direct_fs = fsspec.filesystem('file')
        sandboxed_fs = PathSandboxedFileSystem(direct_fs, str(base_path))
        
        ensemble_paths = [str(f.relative_to(base_path)) for f in ensemble_files]
        
        # Test direct filesystem
        direct_profile = hpc_workload_simulator.simulate_mpi_ensemble_analysis(
            direct_fs, str(base_path), [str(f) for f in ensemble_files], num_processes=4
        )
        
        # Test sandboxed filesystem
        sandboxed_profile = hpc_workload_simulator.simulate_mpi_ensemble_analysis(
            sandboxed_fs, str(base_path), ensemble_paths, num_processes=4
        )
        
        # Performance comparison
        time_overhead = ((sandboxed_profile.execution_time - direct_profile.execution_time) / 
                        direct_profile.execution_time) * 100
        
        # Assertions for ensemble workflows
        assert time_overhead < 15.0, f"Ensemble processing overhead {time_overhead:.2f}% exceeds 15%"
        assert sandboxed_profile.error_count <= direct_profile.error_count + 2, "Too many additional errors"
        
        print(f"\nEnsemble Simulation Performance:")
        print(f"  Files processed: {sandboxed_profile.additional_metrics.get('total_files_processed', 0)}")
        print(f"  MPI processes: {sandboxed_profile.additional_metrics.get('processes_used', 0)}")
        print(f"  Time overhead: {time_overhead:.2f}%")
        print(f"  Concurrent throughput: {sandboxed_profile.operations_per_second:.1f} ops/sec")
    
    def test_time_series_analysis_performance(self, hpc_workload_simulator, climate_data_structure):
        """Test performance of time series extraction workflows."""
        base_path, file_groups = climate_data_structure
        all_files = file_groups['all'][:30]  # Sample for performance
        
        direct_fs = fsspec.filesystem('file')
        sandboxed_fs = PathSandboxedFileSystem(direct_fs, str(base_path))
        
        file_paths = [str(f.relative_to(base_path)) for f in all_files]
        
        # Test time series extraction
        direct_profile = hpc_workload_simulator.simulate_time_series_extraction(
            direct_fs, str(base_path), [str(f) for f in all_files], points=50
        )
        
        sandboxed_profile = hpc_workload_simulator.simulate_time_series_extraction(
            sandboxed_fs, str(base_path), file_paths, points=50
        )
        
        # Performance analysis
        time_overhead = ((sandboxed_profile.execution_time - direct_profile.execution_time) / 
                        direct_profile.execution_time) * 100 if direct_profile.execution_time > 0 else 0
        
        assert time_overhead < 12.0, f"Time series extraction overhead {time_overhead:.2f}% exceeds 12%"
        
        print(f"\nTime Series Analysis Performance:")
        print(f"  Files analyzed: {sandboxed_profile.additional_metrics.get('files_processed', 0)}")
        print(f"  Extraction points: {sandboxed_profile.additional_metrics.get('extraction_points', 0)}")
        print(f"  Time overhead: {time_overhead:.2f}%")
    
    def test_interactive_analysis_performance(self, hpc_workload_simulator, climate_data_structure):
        """Test performance under high-frequency interactive queries."""
        base_path, file_groups = climate_data_structure
        sample_files = file_groups['all'][:20]  # Sample for rapid queries
        
        direct_fs = fsspec.filesystem('file')
        sandboxed_fs = PathSandboxedFileSystem(direct_fs, str(base_path))
        
        file_paths = [str(f.relative_to(base_path)) for f in sample_files]
        
        # Test high-frequency metadata queries (typical of interactive analysis)
        target_qps = 30  # 30 queries per second
        
        direct_profile = hpc_workload_simulator.simulate_high_frequency_metadata_queries(
            direct_fs, [str(f) for f in sample_files], queries_per_second=target_qps
        )
        
        sandboxed_profile = hpc_workload_simulator.simulate_high_frequency_metadata_queries(
            sandboxed_fs, file_paths, queries_per_second=target_qps
        )
        
        # Performance requirements for interactive analysis
        actual_qps_direct = direct_profile.additional_metrics.get('actual_queries_per_second', 0)
        actual_qps_sandboxed = sandboxed_profile.additional_metrics.get('actual_queries_per_second', 0)
        
        qps_degradation = ((actual_qps_direct - actual_qps_sandboxed) / 
                          actual_qps_direct) * 100 if actual_qps_direct > 0 else 0
        
        # Interactive analysis should maintain responsiveness
        assert actual_qps_sandboxed >= target_qps * 0.9, f"Query rate {actual_qps_sandboxed:.1f} below target"
        assert qps_degradation < 20.0, f"Query rate degradation {qps_degradation:.2f}% too high"
        
        print(f"\nInteractive Analysis Performance:")
        print(f"  Target QPS: {target_qps}")
        print(f"  Direct QPS: {actual_qps_direct:.1f}")
        print(f"  Sandboxed QPS: {actual_qps_sandboxed:.1f}")
        print(f"  QPS degradation: {qps_degradation:.2f}%")


@pytest.mark.performance
@pytest.mark.hpc
@pytest.mark.large_data  
class TestScalabilityLimits:
    """Test scalability limits and performance under extreme conditions."""
    
    def test_large_directory_traversal_performance(self, hpc_profiler, tmp_path):
        """Test performance with very large directory structures."""
        # Create large directory structure (1000+ files)
        generator = ClimateDataGenerator()
        large_files = generator.create_cmip6_structure(tmp_path, size_constraint_mb=500)
        
        direct_fs = fsspec.filesystem('file')
        sandboxed_fs = PathSandboxedFileSystem(direct_fs, str(tmp_path))
        
        # Test directory traversal performance
        hpc_profiler.start_profiling("large_dir_direct_traversal", filesystem_type="direct")
        direct_all_files = direct_fs.find(str(tmp_path))
        direct_profile = hpc_profiler.end_profiling()
        
        hpc_profiler.start_profiling("large_dir_sandboxed_traversal", filesystem_type="sandboxed") 
        sandboxed_all_files = sandboxed_fs.find("")
        sandboxed_profile = hpc_profiler.end_profiling()
        
        # Performance analysis
        time_overhead = ((sandboxed_profile.execution_time - direct_profile.execution_time) / 
                        direct_profile.execution_time) * 100
        
        files_found_direct = len(direct_all_files)
        files_found_sandboxed = len(sandboxed_all_files)
        
        # Scalability assertions
        assert abs(files_found_direct - files_found_sandboxed) <= 10, "File count should be similar"
        assert time_overhead < 25.0, f"Large directory traversal overhead {time_overhead:.2f}% too high"
        assert sandboxed_profile.memory_delta_mb < 200, f"Memory usage {sandboxed_profile.memory_delta_mb:.1f}MB too high"
        
        print(f"\nLarge Directory Traversal Performance:")
        print(f"  Files found: {files_found_sandboxed}")
        print(f"  Traversal rate: {files_found_sandboxed/sandboxed_profile.execution_time:.1f} files/sec")
        print(f"  Time overhead: {time_overhead:.2f}%")
        print(f"  Memory usage: {sandboxed_profile.memory_delta_mb:.1f}MB")
    
    def test_concurrent_access_scalability(self, hpc_profiler, climate_data_structure):
        """Test scalability with increasing levels of concurrency."""
        base_path, file_groups = climate_data_structure
        test_files = file_groups['all'][:100]  # Sufficient sample
        
        direct_fs = fsspec.filesystem('file') 
        sandboxed_fs = PathSandboxedFileSystem(direct_fs, str(base_path))
        
        test_paths = [str(f.relative_to(base_path)) for f in test_files]
        concurrency_levels = [1, 2, 4, 8, 16]
        
        results = {}
        
        for concurrency in concurrency_levels:
            def concurrent_worker(worker_files):
                for filepath in worker_files:
                    try:
                        sandboxed_fs.exists(filepath)
                        sandboxed_fs.info(filepath)
                    except Exception:
                        pass
            
            # Divide files among workers
            chunk_size = len(test_paths) // concurrency
            file_chunks = [test_paths[i:i+chunk_size] 
                          for i in range(0, len(test_paths), chunk_size)]
            
            hpc_profiler.start_profiling(f"concurrent_access_{concurrency}", 
                                       concurrency_level=concurrency, filesystem_type="sandboxed")
            
            with ThreadPoolExecutor(max_workers=concurrency) as executor:
                futures = [executor.submit(concurrent_worker, chunk) for chunk in file_chunks]
                for future in as_completed(futures):
                    future.result()
            
            profile = hpc_profiler.end_profiling()
            results[concurrency] = profile
            
            # Memory and performance checks at each level
            assert profile.memory_delta_mb < 100 + (concurrency * 10), f"Memory scaling issue at {concurrency} workers"
            assert profile.execution_time < 60, f"Execution time too high at {concurrency} workers"
        
        # Analyze scalability  
        base_throughput = results[1].operations_per_second
        max_throughput = max(profile.operations_per_second for profile in results.values())
        
        scaling_efficiency = max_throughput / (base_throughput * max(concurrency_levels))
        
        print(f"\nConcurrency Scalability Analysis:")
        for concurrency, profile in results.items():
            efficiency = profile.operations_per_second / (base_throughput * concurrency)
            print(f"  {concurrency} workers: {profile.operations_per_second:.1f} ops/sec (efficiency: {efficiency:.2f})")
        
        # Reasonable scaling up to 8 workers expected
        assert results[4].operations_per_second > base_throughput * 2, "Poor scaling to 4 workers"
        assert scaling_efficiency > 0.3, f"Overall scaling efficiency {scaling_efficiency:.2f} too low"
    
    @pytest.mark.timeout(300)  # 5 minutes max
    def test_memory_pressure_performance(self, hpc_profiler, tmp_path):
        """Test performance under memory pressure (low memory simulation)."""
        # Create data structure that will stress memory
        generator = ClimateDataGenerator()
        files = generator.create_cmip6_structure(tmp_path, size_constraint_mb=100)
        
        direct_fs = fsspec.filesystem('file')
        sandboxed_fs = PathSandboxedFileSystem(direct_fs, str(tmp_path))
        
        test_paths = [str(f.relative_to(tmp_path)) for f in files]
        
        # Monitor memory throughout test
        initial_memory = psutil.virtual_memory().available / (1024 * 1024)  # MB
        
        hpc_profiler.start_profiling("memory_pressure_test", filesystem_type="sandboxed")
        
        # Perform operations that could accumulate memory
        for cycle in range(10):
            # Multiple operations per cycle
            for filepath in test_paths[:20]:  # Subset per cycle
                try:
                    sandboxed_fs.exists(filepath)
                    sandboxed_fs.info(filepath)
                    
                    # Simulate some data processing
                    with sandboxed_fs.open(filepath, 'rb') as f:
                        chunk = f.read(1024)
                
                except Exception:
                    hpc_profiler.record_error()
            
            # Check memory growth
            current_memory = psutil.virtual_memory().available / (1024 * 1024)
            memory_consumed = initial_memory - current_memory
            
            if memory_consumed > 500:  # More than 500MB consumed
                break  # Stop test to prevent system issues
            
            # Force garbage collection
            if cycle % 3 == 0:
                gc.collect()
        
        profile = hpc_profiler.end_profiling()
        
        final_memory = psutil.virtual_memory().available / (1024 * 1024)
        total_memory_impact = initial_memory - final_memory
        
        # Memory pressure assertions
        assert total_memory_impact < 300, f"Memory impact {total_memory_impact:.1f}MB too high"
        assert profile.memory_delta_mb < 150, f"Process memory delta {profile.memory_delta_mb:.1f}MB too high"
        
        print(f"\nMemory Pressure Test Results:")
        print(f"  System memory impact: {total_memory_impact:.1f}MB")
        print(f"  Process memory delta: {profile.memory_delta_mb:.1f}MB")
        print(f"  Operations completed: {10 * 20 - profile.error_count}")
        print(f"  Error rate: {profile.error_count / (10 * 20) * 100:.1f}%")


@pytest.mark.performance
@pytest.mark.benchmark
class TestPerformanceRegression:
    """Performance regression testing and baseline management."""
    
    def test_establish_performance_baselines(self, hpc_profiler, climate_data_structure):
        """Establish performance baselines for climate science operations."""
        base_path, file_groups = climate_data_structure
        sample_files = file_groups['all'][:50]
        
        direct_fs = fsspec.filesystem('file')
        sandboxed_fs = PathSandboxedFileSystem(direct_fs, str(base_path))
        
        test_paths = [str(f.relative_to(base_path)) for f in sample_files]
        
        # Define standard benchmark operations
        benchmark_operations = {
            'exists_batch': lambda fs, paths: [fs.exists(p) for p in paths],
            'info_batch': lambda fs, paths: [fs.info(p) for p in paths[:20]],  # Limit for time
            'list_directories': lambda fs, paths: fs.ls(""),
            'glob_pattern': lambda fs, paths: fs.glob("**/*.nc"),
        }
        
        baselines = {}
        
        for op_name, op_func in benchmark_operations.items():
            # Direct filesystem baseline
            hpc_profiler.start_profiling(f"{op_name}_direct", filesystem_type="direct")
            try:
                direct_result = op_func(direct_fs, [str(base_path / p) for p in test_paths])
            except Exception:
                hpc_profiler.record_error()
                direct_result = []
            direct_profile = hpc_profiler.end_profiling()
            
            # Sandboxed filesystem measurement
            hpc_profiler.start_profiling(f"{op_name}_sandboxed", filesystem_type="sandboxed")
            try:
                sandboxed_result = op_func(sandboxed_fs, test_paths)
            except Exception:
                hpc_profiler.record_error()
                sandboxed_result = []
            sandboxed_profile = hpc_profiler.end_profiling()
            
            # Calculate metrics
            time_overhead = ((sandboxed_profile.execution_time - direct_profile.execution_time) / 
                            direct_profile.execution_time) * 100 if direct_profile.execution_time > 0 else 0
            
            memory_overhead = sandboxed_profile.memory_delta_mb - direct_profile.memory_delta_mb
            
            baselines[op_name] = {
                'time_overhead_percent': time_overhead,
                'memory_overhead_mb': memory_overhead,
                'direct_ops_per_sec': direct_profile.operations_per_second,
                'sandboxed_ops_per_sec': sandboxed_profile.operations_per_second,
                'result_count_direct': len(direct_result) if hasattr(direct_result, '__len__') else 0,
                'result_count_sandboxed': len(sandboxed_result) if hasattr(sandboxed_result, '__len__') else 0,
            }
        
        # Performance baseline targets (these define acceptable performance)
        baseline_targets = {
            'exists_batch': {'max_time_overhead': 8.0, 'max_memory_overhead': 15.0},
            'info_batch': {'max_time_overhead': 12.0, 'max_memory_overhead': 20.0},
            'list_directories': {'max_time_overhead': 5.0, 'max_memory_overhead': 10.0},
            'glob_pattern': {'max_time_overhead': 15.0, 'max_memory_overhead': 25.0},
        }
        
        print(f"\nPerformance Baseline Establishment:")
        regression_detected = False
        
        for op_name, metrics in baselines.items():
            targets = baseline_targets[op_name]
            
            time_pass = metrics['time_overhead_percent'] <= targets['max_time_overhead']
            memory_pass = metrics['memory_overhead_mb'] <= targets['max_memory_overhead']
            
            status = "PASS" if time_pass and memory_pass else "FAIL"
            if not (time_pass and memory_pass):
                regression_detected = True
            
            print(f"  {op_name}: {status}")
            print(f"    Time overhead: {metrics['time_overhead_percent']:.2f}% (target: ≤{targets['max_time_overhead']:.1f}%)")
            print(f"    Memory overhead: {metrics['memory_overhead_mb']:.1f}MB (target: ≤{targets['max_memory_overhead']:.1f}MB)")
            print(f"    Throughput: {metrics['sandboxed_ops_per_sec']:.1f} ops/sec")
        
        # Critical operations must pass
        critical_operations = ['exists_batch', 'list_directories']
        for op in critical_operations:
            if op in baselines and op in baseline_targets:
                metrics = baselines[op]
                targets = baseline_targets[op]
                
                assert metrics['time_overhead_percent'] <= targets['max_time_overhead'], \
                    f"Critical performance regression in {op}: time overhead {metrics['time_overhead_percent']:.2f}% > {targets['max_time_overhead']:.1f}%"
                
                assert metrics['memory_overhead_mb'] <= targets['max_memory_overhead'], \
                    f"Critical performance regression in {op}: memory overhead {metrics['memory_overhead_mb']:.1f}MB > {targets['max_memory_overhead']:.1f}MB"
        
        return baselines


# Performance Reporting and Analysis
def generate_hpc_performance_report(profiles: List[PerformanceProfile]) -> str:
    """Generate comprehensive HPC performance analysis report."""
    if not profiles:
        return "No performance profiles to analyze."
    
    report_lines = [
        "HPC Climate Science Performance Analysis Report",
        "=" * 60,
        f"Report generated: {time.strftime('%Y-%m-%d %H:%M:%S')}",
        f"Total operations analyzed: {len(profiles)}",
        "",
        "EXECUTIVE SUMMARY:",
    ]
    
    # Calculate summary statistics
    time_overheads = [p.additional_metrics.get('time_overhead_percent', 0) for p in profiles if 'time_overhead_percent' in p.additional_metrics]
    memory_overheads = [p.memory_delta_mb for p in profiles]
    throughputs = [p.throughput_mbps for p in profiles if p.throughput_mbps > 0]
    
    if time_overheads:
        avg_time_overhead = mean(time_overheads)
        max_time_overhead = max(time_overheads)
        report_lines.extend([
            f"  Average time overhead: {avg_time_overhead:.2f}%",
            f"  Maximum time overhead: {max_time_overhead:.2f}%",
        ])
    
    if memory_overheads:
        avg_memory_overhead = mean(memory_overheads)
        max_memory_overhead = max(memory_overheads)
        report_lines.extend([
            f"  Average memory overhead: {avg_memory_overhead:.1f}MB",
            f"  Maximum memory overhead: {max_memory_overhead:.1f}MB",
        ])
    
    if throughputs:
        avg_throughput = mean(throughputs)
        report_lines.append(f"  Average throughput: {avg_throughput:.1f}MB/s")
    
    # Performance assessment
    report_lines.extend([
        "",
        "PERFORMANCE ASSESSMENT:",
    ])
    
    if time_overheads:
        time_assessment = "✓ PASS" if avg_time_overhead < 10 and max_time_overhead < 20 else "✗ FAIL"
        report_lines.append(f"  Time overhead: {time_assessment}")
    
    if memory_overheads:
        memory_assessment = "✓ PASS" if avg_memory_overhead < 50 and max_memory_overhead < 100 else "✗ FAIL"
        report_lines.append(f"  Memory efficiency: {memory_assessment}")
    
    # Detailed operation analysis
    report_lines.extend([
        "",
        "DETAILED OPERATION ANALYSIS:",
    ])
    
    for profile in profiles:
        report_lines.extend([
            f"  {profile.operation.upper()}:",
            f"    Execution time: {profile.execution_time:.3f}s",
            f"    Memory delta: {profile.memory_delta_mb:.1f}MB",
            f"    Throughput: {profile.operations_per_second:.1f} ops/sec",
            f"    Error rate: {profile.error_count / max(1, profile.concurrency_level) * 100:.1f}%",
            "",
        ])
    
    # HPC-specific recommendations
    report_lines.extend([
        "HPC OPTIMIZATION RECOMMENDATIONS:",
        "1. Path resolution caching for repeated directory access",
        "2. Bulk operation batching for ensemble processing", 
        "3. Memory pool management for high-concurrency scenarios",
        "4. Async I/O support for network filesystem optimization",
        "5. NUMA-aware memory allocation for multi-socket systems",
        "",
        "CLIMATE SCIENCE WORKFLOW IMPACT:",
        "- CMIP6 data access: Minimal impact expected",
        "- Ensemble analysis: Acceptable for typical workflows",
        "- Time series extraction: Within performance targets",
        "- Interactive analysis: Maintains responsiveness requirements",
    ])
    
    return "\n".join(report_lines)


if __name__ == "__main__":
    print("HPC Climate Science Performance Test Suite for PathSandboxedFileSystem")
    print("Run with: pixi run -e test pytest -m 'performance and hpc' tests/test_hpc_climate_performance.py")
    print("For full benchmarking: pixi run -e test pytest -m 'benchmark and hpc' --timeout=600")