"""
Performance tests for large Earth science dataset handling.

This module tests the performance characteristics of the archive system
when dealing with realistic large datasets typical in Earth science research.
"""

import gc
import os
import tarfile
import tempfile
import threading
import time
from pathlib import Path
from unittest.mock import Mock, patch

import psutil
import pytest

try:
    import netCDF4 as nc
    import numpy as np
    HAS_NETCDF = True
except ImportError:
    HAS_NETCDF = False

from tellus.location import Location, LocationKind
from tellus.simulation.simulation import (ArchiveManifest, CacheConfig,
                                          CacheManager, CompressedArchive)

from .fixtures.earth_science import *


def get_memory_usage():
    """Get current memory usage in MB."""
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / 1024 / 1024  # MB


def create_large_netcdf_file(filepath: Path, size_mb: int):
    """Create a large NetCDF file for performance testing."""
    if not HAS_NETCDF:
        # Create a dummy binary file instead
        with open(filepath, 'wb') as f:
            chunk_size = 1024 * 1024  # 1 MB chunks
            chunks = size_mb
            for _ in range(chunks):
                f.write(b'x' * chunk_size)
        return filepath
    
    # Calculate dimensions for approximately the target size
    # Assume 4 bytes per float32 value
    target_bytes = size_mb * 1024 * 1024
    values_needed = target_bytes // 4
    
    # Use reasonable Earth science dimensions
    time_steps = min(365, int(values_needed**(1/4)))
    levels = min(50, int((values_needed / time_steps)**(1/3)))
    lat_points = min(180, int((values_needed / (time_steps * levels))**(1/2)))
    lon_points = values_needed // (time_steps * levels * lat_points)
    
    with nc.Dataset(filepath, 'w', format='NETCDF4') as ds:
        # Create dimensions
        ds.createDimension('time', time_steps)
        ds.createDimension('lev', levels)
        ds.createDimension('lat', lat_points)
        ds.createDimension('lon', lon_points)
        
        # Create coordinate variables
        time_var = ds.createVariable('time', 'f8', ('time',))
        time_var[:] = np.arange(time_steps)
        
        lev_var = ds.createVariable('lev', 'f4', ('lev',))
        lev_var[:] = np.logspace(np.log10(1000), np.log10(1), levels)
        
        lat_var = ds.createVariable('lat', 'f4', ('lat',))
        lat_var[:] = np.linspace(-90, 90, lat_points)
        
        lon_var = ds.createVariable('lon', 'f4', ('lon',))
        lon_var[:] = np.linspace(-180, 180, lon_points)
        
        # Create large data variable with compression
        temp_var = ds.createVariable('temperature', 'f4', 
                                   ('time', 'lev', 'lat', 'lon'),
                                   chunksizes=(min(30, time_steps), 
                                             min(10, levels),
                                             min(45, lat_points), 
                                             min(90, lon_points)),
                                   zlib=True, complevel=1)
        
        # Fill with realistic-looking data in chunks to manage memory
        chunk_time = min(30, time_steps)
        for t_start in range(0, time_steps, chunk_time):
            t_end = min(t_start + chunk_time, time_steps)
            chunk_data = np.random.normal(273.15, 20, 
                                        (t_end - t_start, levels, lat_points, lon_points)).astype(np.float32)
            temp_var[t_start:t_end] = chunk_data
            
        # Add attributes
        temp_var.units = 'K'
        temp_var.long_name = 'air temperature'
        ds.title = f'Large test dataset (~{size_mb} MB)'
    
    return filepath


@pytest.mark.performance
@pytest.mark.slow
class TestCachePerformance:
    """Test cache performance with large datasets."""
    
    @pytest.fixture
    def performance_cache_manager(self, earth_science_temp_dir):
        """Create cache manager optimized for performance testing."""
        config = CacheConfig(
            cache_dir=earth_science_temp_dir / "perf_cache",
            archive_cache_size_limit=10 * 1024**3,  # 10 GB
            file_cache_size_limit=5 * 1024**3,      # 5 GB
            unified_cache=False
        )
        return CacheManager(config)
    
    @pytest.mark.large_data
    @pytest.mark.timeout(300)  # 5 minutes max
    def test_large_archive_caching_performance(self, performance_cache_manager, earth_science_temp_dir):
        """Test caching performance with large archives (>1GB)."""
        # Create large archive directory structure
        large_archive_dir = earth_science_temp_dir / "large_climate_archive"
        large_archive_dir.mkdir()
        
        # Create multiple large NetCDF files
        sizes_mb = [100, 150, 200, 250, 300]  # Total: ~1GB
        netcdf_files = []
        
        creation_start = time.time()
        for i, size_mb in enumerate(sizes_mb):
            netcdf_file = large_archive_dir / f"climate_data_{i:02d}.nc"
            create_large_netcdf_file(netcdf_file, size_mb)
            netcdf_files.append(netcdf_file)
        creation_time = time.time() - creation_start
        
        # Create compressed archive
        archive_path = earth_science_temp_dir / "large_climate_archive.tar.gz"
        compression_start = time.time()
        with tarfile.open(archive_path, "w:gz") as tar:
            tar.add(large_archive_dir, arcname="large_climate_archive")
        compression_time = time.time() - compression_start
        
        archive_size_mb = archive_path.stat().st_size / (1024 * 1024)
        
        # Test caching performance
        initial_memory = get_memory_usage()
        cache_start = time.time()
        
        checksum = performance_cache_manager._calculate_checksum(archive_path)
        cached_path = performance_cache_manager.cache_archive(archive_path, checksum)
        
        cache_time = time.time() - cache_start
        peak_memory = get_memory_usage()
        memory_delta = peak_memory - initial_memory
        
        # Test cache retrieval performance
        retrieval_start = time.time()
        retrieved_path = performance_cache_manager.get_archive_path(checksum)
        retrieval_time = time.time() - retrieval_start
        
        # Performance assertions and reporting
        print(f"\nLarge Archive Performance Metrics:")
        print(f"  Archive size: {archive_size_mb:.1f} MB")
        print(f"  Creation time: {creation_time:.2f} seconds")
        print(f"  Compression time: {compression_time:.2f} seconds")
        print(f"  Cache time: {cache_time:.2f} seconds")
        print(f"  Retrieval time: {retrieval_time:.4f} seconds")
        print(f"  Memory delta: {memory_delta:.1f} MB")
        print(f"  Cache throughput: {archive_size_mb/cache_time:.1f} MB/s")
        
        # Performance requirements for large datasets
        assert cached_path.exists()
        assert retrieved_path == cached_path
        assert cache_time < 60.0  # Should cache 1GB in under 1 minute
        assert retrieval_time < 0.1  # Cache lookup should be very fast
        assert memory_delta < 500  # Should not use excessive memory
    
    def test_concurrent_cache_access(self, performance_cache_manager, earth_science_temp_dir):
        """Test cache performance under concurrent access."""
        # Create multiple small archives for concurrent access
        archives = []
        checksums = []
        
        for i in range(5):
            archive_dir = earth_science_temp_dir / f"concurrent_test_{i}"
            archive_dir.mkdir()
            
            # Create small test file
            test_file = archive_dir / f"data_{i}.nc"
            create_large_netcdf_file(test_file, 10)  # 10 MB each
            
            # Create archive
            archive_path = earth_science_temp_dir / f"concurrent_test_{i}.tar.gz"
            with tarfile.open(archive_path, "w:gz") as tar:
                tar.add(archive_dir, arcname=f"concurrent_test_{i}")
            
            archives.append(archive_path)
            checksums.append(performance_cache_manager._calculate_checksum(archive_path))
        
        # Test concurrent caching
        results = {}
        errors = []
        
        def cache_archive_worker(idx, archive_path, checksum):
            try:
                start_time = time.time()
                cached_path = performance_cache_manager.cache_archive(archive_path, checksum)
                end_time = time.time()
                results[idx] = {
                    'cached_path': cached_path,
                    'duration': end_time - start_time,
                    'success': True
                }
            except Exception as e:
                errors.append((idx, str(e)))
                results[idx] = {'success': False, 'error': str(e)}
        
        # Start concurrent caching operations
        threads = []
        start_time = time.time()
        
        for i, (archive_path, checksum) in enumerate(zip(archives, checksums)):
            thread = threading.Thread(
                target=cache_archive_worker,
                args=(i, archive_path, checksum)
            )
            threads.append(thread)
            thread.start()
        
        # Wait for all threads to complete
        for thread in threads:
            thread.join()
        
        total_time = time.time() - start_time
        
        # Verify results
        print(f"\nConcurrent Cache Performance:")
        print(f"  Total time: {total_time:.2f} seconds")
        print(f"  Errors: {len(errors)}")
        
        assert len(errors) == 0, f"Concurrent caching errors: {errors}"
        assert all(result['success'] for result in results.values())
        assert total_time < 30.0  # Should complete all operations quickly


@pytest.mark.performance
@pytest.mark.slow
class TestArchivePerformance:
    """Test archive system performance with large datasets."""
    
    @pytest.fixture
    def performance_location(self, earth_science_temp_dir):
        """Create location for performance testing."""
        with patch('tellus.location.location.Location._save_locations'):
            return Location(
                name="performance_test_location",
                kinds=[LocationKind.DISK],
                config={
                    "protocol": "file",
                    "path": str(earth_science_temp_dir),
                    "storage_options": {"auto_mkdir": True}
                }
            )
    
    @pytest.mark.large_data
    @pytest.mark.timeout(600)  # 10 minutes max
    def test_large_archive_scanning_performance(self, performance_location, earth_science_temp_dir):
        """Test performance of scanning large archives for file manifest."""
        # Create large archive with many files
        archive_dir = earth_science_temp_dir / "large_scan_test"
        archive_dir.mkdir()
        
        # Create directory structure with many files
        file_count = 1000
        files_created = []
        
        creation_start = time.time()
        
        # Create realistic Earth science directory structure
        components = ['atm', 'ocn', 'lnd', 'ice']
        variables = ['temp', 'precip', 'wind', 'pres']
        years = range(2000, 2020)  # 20 years
        
        file_idx = 0
        for component in components:
            comp_dir = archive_dir / "model_output" / component
            comp_dir.mkdir(parents=True)
            
            for variable in variables:
                for year in years:
                    if file_idx >= file_count:
                        break
                    
                    filename = f"{variable}_daily_{year}.nc"
                    filepath = comp_dir / filename
                    
                    # Create small dummy NetCDF file
                    if HAS_NETCDF:
                        with nc.Dataset(filepath, 'w') as ds:
                            ds.createDimension('time', 365)
                            ds.createDimension('lat', 96)
                            ds.createDimension('lon', 192)
                            
                            var = ds.createVariable(variable, 'f4', ('time', 'lat', 'lon'))
                            var[:] = np.random.random((365, 96, 192)).astype(np.float32)
                    else:
                        # Create dummy file
                        filepath.write_bytes(b'dummy netcdf data' * 1000)
                    
                    files_created.append(filepath)
                    file_idx += 1
                    
                    if file_idx >= file_count:
                        break
                if file_idx >= file_count:
                    break
            if file_idx >= file_count:
                break
        
        creation_time = time.time() - creation_start
        
        # Create compressed archive
        archive_path = earth_science_temp_dir / "large_scan_test.tar.gz"
        compression_start = time.time()
        
        with tarfile.open(archive_path, "w:gz") as tar:
            tar.add(archive_dir, arcname="large_scan_test")
        
        compression_time = time.time() - compression_start
        archive_size_mb = archive_path.stat().st_size / (1024 * 1024)
        
        # Test archive scanning performance
        cache_config = CacheConfig(cache_dir=earth_science_temp_dir / "scan_cache")
        cache_manager = CacheManager(cache_config)
        
        initial_memory = get_memory_usage()
        scan_start = time.time()
        
        compressed_archive = CompressedArchive(
            archive_id="large_scan_performance_test",
            archive_location=str(archive_path),
            location=performance_location,
            cache_manager=cache_manager
        )
        
        # Force manifest creation/scanning
        with patch.object(compressed_archive, 'refresh_manifest') as mock_refresh:
            # Simulate manifest creation
            mock_manifest = Mock()
            mock_files = {}
            
            # Simulate scanning all files
            for i, filepath in enumerate(files_created):
                rel_path = filepath.relative_to(archive_dir)
                mock_files[str(rel_path)] = Mock()
            
            mock_manifest.files = mock_files
            compressed_archive.manifest = mock_manifest
            
            # Test file listing performance
            all_files = compressed_archive.list_files()
            netcdf_files = compressed_archive.list_files(pattern="*.nc")
            atm_files = compressed_archive.list_files(pattern="model_output/atm/*")
        
        scan_time = time.time() - scan_start
        peak_memory = get_memory_usage()
        memory_delta = peak_memory - initial_memory
        
        # Performance reporting
        print(f"\nLarge Archive Scanning Performance:")
        print(f"  Files created: {len(files_created)}")
        print(f"  Archive size: {archive_size_mb:.1f} MB")
        print(f"  Creation time: {creation_time:.2f} seconds")
        print(f"  Compression time: {compression_time:.2f} seconds")
        print(f"  Scan time: {scan_time:.2f} seconds")
        print(f"  Memory delta: {memory_delta:.1f} MB")
        print(f"  Files/second: {len(files_created)/scan_time:.1f}")
        
        # Performance assertions
        assert len(mock_files) == len(files_created)
        assert scan_time < 30.0  # Should scan 1000 files in under 30 seconds
        assert memory_delta < 200  # Should not use excessive memory
    
    def test_archive_file_access_patterns(self, performance_location, earth_science_temp_dir):
        """Test performance of different file access patterns."""
        # Create archive with structured data
        archive_dir = earth_science_temp_dir / "access_pattern_test"
        archive_dir.mkdir()
        
        # Create files with different access patterns
        patterns = {
            'sequential': [],  # Files accessed in order
            'random': [],     # Files accessed randomly
            'spatial': [],    # Files grouped spatially
            'temporal': []    # Files grouped temporally
        }
        
        # Create files for each pattern
        for pattern_name in patterns.keys():
            pattern_dir = archive_dir / pattern_name
            pattern_dir.mkdir()
            
            for i in range(20):  # 20 files per pattern
                filepath = pattern_dir / f"data_{i:03d}.nc"
                if HAS_NETCDF:
                    create_large_netcdf_file(filepath, 5)  # 5 MB each
                else:
                    filepath.write_bytes(b'test data' * 1000)
                patterns[pattern_name].append(filepath)
        
        # Create archive
        archive_path = earth_science_temp_dir / "access_pattern_test.tar.gz"
        with tarfile.open(archive_path, "w:gz") as tar:
            tar.add(archive_dir, arcname="access_pattern_test")
        
        # Test access patterns
        cache_config = CacheConfig(cache_dir=earth_science_temp_dir / "pattern_cache")
        cache_manager = CacheManager(cache_config)
        
        compressed_archive = CompressedArchive(
            archive_id="access_pattern_test",
            archive_location=str(archive_path),
            location=performance_location,
            cache_manager=cache_manager
        )
        
        # Mock file access for different patterns
        access_times = {}
        
        for pattern_name, files in patterns.items():
            start_time = time.time()
            
            # Simulate accessing files according to pattern
            if pattern_name == 'sequential':
                # Access files in order
                for filepath in files:
                    rel_path = filepath.relative_to(archive_dir)
                    # Simulate file opening
                    with patch.object(compressed_archive, 'open_file') as mock_open:
                        mock_open.return_value = Mock()
                        compressed_archive.open_file(str(rel_path))
            
            elif pattern_name == 'random':
                # Access files in random order
                import random
                random_files = files.copy()
                random.shuffle(random_files)
                for filepath in random_files:
                    rel_path = filepath.relative_to(archive_dir)
                    with patch.object(compressed_archive, 'open_file') as mock_open:
                        mock_open.return_value = Mock()
                        compressed_archive.open_file(str(rel_path))
            
            access_times[pattern_name] = time.time() - start_time
        
        # Performance analysis
        print(f"\nFile Access Pattern Performance:")
        for pattern_name, access_time in access_times.items():
            files_per_second = len(patterns[pattern_name]) / access_time
            print(f"  {pattern_name}: {access_time:.3f}s ({files_per_second:.1f} files/s)")
        
        # Sequential access should be fastest for compressed archives
        assert access_times['sequential'] <= access_times['random'] * 1.5


@pytest.mark.performance
@pytest.mark.slow 
class TestMemoryUsagePatterns:
    """Test memory usage patterns with large Earth science datasets."""
    
    def test_memory_efficient_archive_processing(self, earth_science_temp_dir):
        """Test memory efficiency when processing large archives."""
        # Create large archive
        archive_dir = earth_science_temp_dir / "memory_test"
        archive_dir.mkdir()
        
        # Create large files
        large_files = []
        for i in range(3):  # 3 large files
            filepath = archive_dir / f"large_climate_data_{i}.nc"
            create_large_netcdf_file(filepath, 100)  # 100 MB each
            large_files.append(filepath)
        
        # Create archive
        archive_path = earth_science_temp_dir / "memory_test.tar.gz"
        with tarfile.open(archive_path, "w:gz") as tar:
            tar.add(archive_dir, arcname="memory_test")
        
        # Monitor memory usage during operations
        initial_memory = get_memory_usage()
        memory_samples = [initial_memory]
        
        def memory_monitor():
            while getattr(memory_monitor, 'running', True):
                memory_samples.append(get_memory_usage())
                time.sleep(0.1)
        
        # Start memory monitoring
        memory_monitor.running = True
        monitor_thread = threading.Thread(target=memory_monitor)
        monitor_thread.start()
        
        try:
            # Create cache manager
            cache_config = CacheConfig(
                cache_dir=earth_science_temp_dir / "memory_cache",
                archive_cache_size_limit=1024**3,  # 1 GB limit
                file_cache_size_limit=512*1024**2  # 512 MB limit
            )
            cache_manager = CacheManager(cache_config)
            
            # Create location
            with patch('tellus.location.location.Location._save_locations'):
                location = Location(
                    name="memory_test_location",
                    kinds=[LocationKind.DISK],
                    config={
                        "protocol": "file",
                        "path": str(earth_science_temp_dir),
                        "storage_options": {"auto_mkdir": True}
                    }
                )
            
            # Process archive
            compressed_archive = CompressedArchive(
                archive_id="memory_efficiency_test",
                archive_location=str(archive_path),
                location=location,
                cache_manager=cache_manager
            )
            
            # Simulate various operations
            checksum = cache_manager._calculate_checksum(archive_path)
            cached_path = cache_manager.cache_archive(archive_path, checksum)
            
            # Force garbage collection
            gc.collect()
            
        finally:
            # Stop memory monitoring
            memory_monitor.running = False
            monitor_thread.join()
        
        # Analyze memory usage
        peak_memory = max(memory_samples)
        final_memory = get_memory_usage()
        max_delta = peak_memory - initial_memory
        final_delta = final_memory - initial_memory
        
        print(f"\nMemory Usage Analysis:")
        print(f"  Initial memory: {initial_memory:.1f} MB")
        print(f"  Peak memory: {peak_memory:.1f} MB")
        print(f"  Final memory: {final_memory:.1f} MB")
        print(f"  Max delta: {max_delta:.1f} MB")
        print(f"  Final delta: {final_delta:.1f} MB")
        print(f"  Memory samples: {len(memory_samples)}")
        
        # Memory efficiency requirements
        archive_size_mb = archive_path.stat().st_size / (1024 * 1024)
        
        # Should not use more than 2x the archive size in memory
        assert max_delta < archive_size_mb * 2
        
        # Should release memory after operations
        assert final_delta < max_delta * 0.5


@pytest.mark.performance
@pytest.mark.benchmark
class TestBenchmarkOperations:
    """Benchmark critical operations for performance regression testing."""
    
    def test_checksum_calculation_benchmark(self, benchmark, earth_science_temp_dir):
        """Benchmark checksum calculation for various file sizes."""
        # Create test files of different sizes
        test_files = {}
        sizes = [1, 10, 100]  # MB
        
        for size in sizes:
            filepath = earth_science_temp_dir / f"checksum_test_{size}mb.dat"
            with open(filepath, 'wb') as f:
                chunk = b'x' * (1024 * 1024)  # 1 MB chunk
                for _ in range(size):
                    f.write(chunk)
            test_files[size] = filepath
        
        # Benchmark checksum calculation
        cache_manager = CacheManager(CacheConfig(cache_dir=earth_science_temp_dir / "bench_cache"))
        
        def checksum_benchmark():
            checksums = {}
            for size, filepath in test_files.items():
                checksums[size] = cache_manager._calculate_checksum(filepath)
            return checksums
        
        result = benchmark(checksum_benchmark)
        
        # Verify checksums were calculated
        assert len(result) == len(sizes)
        print(f"\nChecksum benchmark completed for files: {list(result.keys())} MB")
    
    def test_archive_creation_benchmark(self, benchmark, earth_science_temp_dir, create_model_archive):
        """Benchmark archive creation performance."""
        # Create model archive directory
        model_dir = create_model_archive("benchmark_model")
        
        def archive_creation_benchmark():
            archive_path = earth_science_temp_dir / "benchmark_archive.tar.gz"
            with tarfile.open(archive_path, "w:gz") as tar:
                tar.add(model_dir, arcname="benchmark_model")
            return archive_path.stat().st_size
        
        archive_size = benchmark(archive_creation_benchmark)
        
        print(f"\nArchive creation benchmark: {archive_size / (1024*1024):.1f} MB created")
        assert archive_size > 0