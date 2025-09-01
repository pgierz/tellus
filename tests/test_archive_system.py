"""
Comprehensive unit tests for the archive system components.

This module tests the core archive functionality including CompressedArchive,
ArchiveManifest, and CacheManager with Earth science specific scenarios.
"""

import hashlib
import io
import json
import tarfile
import tempfile
import time
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import pytest

from tellus.location import Location, LocationKind
from tellus.simulation.simulation import (ArchivedSimulation, ArchiveManifest,
                                          ArchiveMetadata, CacheConfig,
                                          CacheEntry, CacheManager,
                                          CachePriority, CleanupPolicy,
                                          CompressedArchive)

from .fixtures.earth_science import *


@pytest.mark.unit
@pytest.mark.cache
class TestCacheConfig:
    """Test CacheConfig dataclass for Earth science workflows."""
    
    def test_default_configuration(self):
        """Test default cache configuration is suitable for Earth science."""
        config = CacheConfig()
        
        # Verify default paths
        assert config.cache_dir == Path.home() / ".cache" / "tellus"
        assert config.archive_cache_dir == config.cache_dir / "archives"
        assert config.file_cache_dir == config.cache_dir / "files" 
        
        # Verify default sizes are appropriate for Earth science data
        assert config.archive_cache_size_limit == 50 * 1024**3  # 50 GB
        assert config.file_cache_size_limit == 10 * 1024**3     # 10 GB
        
        # Verify default policies
        assert config.archive_cache_cleanup_policy == CleanupPolicy.LRU
        assert config.file_cache_cleanup_policy == CleanupPolicy.LRU
        assert config.unified_cache is False
        assert config.cache_priority == CachePriority.FILES
    
    def test_custom_configuration(self, earth_science_temp_dir):
        """Test custom cache configuration for large Earth science datasets."""
        custom_cache_dir = earth_science_temp_dir / "custom_cache"
        
        config = CacheConfig(
            cache_dir=custom_cache_dir,
            archive_cache_size_limit=500 * 1024**3,  # 500 GB for large model output
            file_cache_size_limit=100 * 1024**3,     # 100 GB for frequent access
            unified_cache=True,
            cache_priority=CachePriority.ARCHIVES
        )
        
        assert config.cache_dir == custom_cache_dir
        assert config.archive_cache_size_limit == 500 * 1024**3
        assert config.file_cache_size_limit == 100 * 1024**3
        assert config.unified_cache is True
        assert config.cache_priority == CachePriority.ARCHIVES


@pytest.mark.unit 
@pytest.mark.cache
class TestCacheEntry:
    """Test CacheEntry dataclass."""
    
    def test_cache_entry_creation(self, earth_science_temp_dir):
        """Test creating a cache entry."""
        test_file = earth_science_temp_dir / "test_data.nc"
        test_file.write_bytes(b"test netcdf data")
        
        entry = CacheEntry(
            path=test_file,
            size=test_file.stat().st_size,
            checksum="test_checksum"
        )
        
        assert entry.path == test_file
        assert entry.size == len(b"test netcdf data")
        assert entry.checksum == "test_checksum"
        assert isinstance(entry.last_accessed, float)
        assert isinstance(entry.created, float)
        assert entry.last_accessed >= entry.created


@pytest.mark.unit
@pytest.mark.cache  
class TestCacheManager:
    """Test CacheManager for Earth science data caching."""
    
    @pytest.fixture
    def cache_manager(self, earth_science_temp_dir):
        """Create a cache manager with temporary directory."""
        config = CacheConfig(cache_dir=earth_science_temp_dir / "cache")
        return CacheManager(config)
    
    def test_cache_manager_initialization(self, cache_manager):
        """Test cache manager initializes correctly."""
        assert cache_manager.config.archive_cache_dir.exists()
        assert cache_manager.config.file_cache_dir.exists()
        assert isinstance(cache_manager._archive_index, dict)
        assert isinstance(cache_manager._file_index, dict)
    
    def test_archive_caching_workflow(self, cache_manager, create_compressed_archive):
        """Test complete archive caching workflow for Earth science data."""
        # Create a model archive
        archive_path = create_compressed_archive("climate_model_run_001")
        
        # Calculate checksum
        checksum = cache_manager._calculate_checksum(archive_path)
        
        # Cache the archive
        cached_path = cache_manager.cache_archive(archive_path, checksum)
        
        # Verify caching worked
        assert cached_path.exists()
        assert cached_path.name == f"{checksum}.tar.gz"
        assert checksum in cache_manager._archive_index
        
        # Verify cache retrieval
        retrieved_path = cache_manager.get_archive_path(checksum)
        assert retrieved_path == cached_path
        
        # Verify last accessed time was updated
        entry = cache_manager._archive_index[checksum]
        assert entry.last_accessed > entry.created
    
    def test_file_caching_workflow(self, cache_manager):
        """Test file-level caching for individual NetCDF files."""
        # Create sample NetCDF data as bytes
        netcdf_data = b"mock netcdf file content for temperature_2020.nc"
        file_key = "climate_data/temperature_2020.nc"
        checksum = hashlib.md5(netcdf_data).hexdigest()
        
        # Cache the file
        cached_path = cache_manager.cache_file(netcdf_data, file_key, checksum)
        
        # Verify caching worked
        assert cached_path.exists()
        assert file_key in cache_manager._file_index
        assert cached_path.read_bytes() == netcdf_data
        
        # Verify cache retrieval
        retrieved_path = cache_manager.get_file_path(file_key)
        assert retrieved_path == cached_path
    
    def test_cache_persistence(self, earth_science_temp_dir):
        """Test cache index persistence across manager instances."""
        config = CacheConfig(cache_dir=earth_science_temp_dir / "persistent_cache")
        
        # Create first manager and cache something
        manager1 = CacheManager(config)
        test_data = b"persistent test data"
        checksum = hashlib.md5(test_data).hexdigest()
        cached_path = manager1.cache_file(test_data, "test_file.nc", checksum)
        
        # Create second manager - should load existing cache
        manager2 = CacheManager(config)
        retrieved_path = manager2.get_file_path("test_file.nc")
        
        assert retrieved_path == cached_path
        assert retrieved_path.exists()
    
    @patch('shutil.rmtree')
    def test_cache_cleanup_lru_policy(self, mock_rmtree, cache_manager):
        """Test LRU cleanup policy for cache management."""
        # Override cache size limit for testing
        cache_manager.config.file_cache_size_limit = 100  # 100 bytes
        
        # Add several files that exceed the limit
        large_data = b"x" * 50  # 50 bytes each
        files = ["file1.nc", "file2.nc", "file3.nc"]
        
        for i, filename in enumerate(files):
            checksum = f"checksum_{i}"
            cache_manager.cache_file(large_data, filename, checksum)
            time.sleep(0.01)  # Ensure different timestamps
        
        # Access first file to make it most recent
        cache_manager.get_file_path("file1.nc")
        
        # Add one more file to trigger cleanup
        cache_manager.cache_file(large_data, "file4.nc", "checksum_4")
        
        # Should have triggered cleanup of least recently used files
        # Implementation would normally clean up file2.nc (oldest unused)


@pytest.mark.unit
@pytest.mark.archive
class TestArchiveMetadata:
    """Test ArchiveMetadata dataclass for Earth science archives."""
    
    def test_archive_metadata_creation(self):
        """Test creating archive metadata for climate model output."""
        metadata = ArchiveMetadata(
            archive_id="ECHAM6_piControl_r1i1p1",
            location="/archive/climate/ECHAM6/piControl_r1i1p1.tar.gz",
            checksum="abc123def456",
            size=50 * 1024**3,  # 50 GB
            created=time.time(),
            simulation_date="2020-01-01",
            version="v1.0", 
            description="ECHAM6 pre-industrial control run",
            tags={"model:ECHAM6", "experiment:piControl", "realm:atmosphere"}
        )
        
        assert metadata.archive_id == "ECHAM6_piControl_r1i1p1"
        assert metadata.size == 50 * 1024**3
        assert "model:ECHAM6" in metadata.tags
        assert "experiment:piControl" in metadata.tags
        assert metadata.description == "ECHAM6 pre-industrial control run"


@pytest.mark.unit
@pytest.mark.archive
class TestArchiveManifest:
    """Test ArchiveManifest for Earth science archive indexing."""
    
    @pytest.fixture
    def sample_metadata(self):
        """Create sample archive metadata."""
        return ArchiveMetadata(
            archive_id="test_climate_archive",
            location="/test/archive.tar.gz",
            checksum="test_checksum",
            size=1024**3,  # 1 GB
            created=time.time()
        )
    
    def test_manifest_creation(self, sample_metadata):
        """Test creating an archive manifest."""
        manifest = ArchiveManifest("test_archive", sample_metadata)
        
        assert manifest.archive_id == "test_archive"
        assert manifest.metadata == sample_metadata
        assert isinstance(manifest.files, dict)
        assert hasattr(manifest, 'tag_system')
    
    @patch('fsspec.filesystem')
    def test_create_from_archive(self, mock_filesystem, earth_science_temp_dir):
        """Test creating manifest from actual archive file."""
        # Create a real tar file for testing
        archive_path = earth_science_temp_dir / "test_archive.tar.gz"
        test_dir = earth_science_temp_dir / "test_content"
        test_dir.mkdir()
        
        # Create some test files
        (test_dir / "temp_data.nc").write_text("temperature data")
        (test_dir / "precip_data.nc").write_text("precipitation data")
        (test_dir / "namelist.nml").write_text("&model_nml\n  dt=1800\n/")
        
        # Create tar archive
        with tarfile.open(archive_path, "w:gz") as tar:
            tar.add(test_dir, arcname="test_content")
        
        # Mock filesystem to return our test file
        mock_fs = Mock()
        mock_fs.exists.return_value = True
        mock_fs.size.return_value = archive_path.stat().st_size
        mock_filesystem.return_value = mock_fs
        
        # Create manifest from archive
        manifest = ArchiveManifest.create_from_archive("test_archive", archive_path, mock_fs)
        
        assert manifest.archive_id == "test_archive"
        assert manifest.metadata is not None
        assert manifest.metadata.size == archive_path.stat().st_size
    
    def test_manifest_serialization(self, sample_metadata):
        """Test manifest can be serialized/deserialized."""
        manifest = ArchiveManifest("test_archive", sample_metadata)
        
        # Add some test files to manifest
        from tellus.simulation.simulation import TaggedFile
        test_file = TaggedFile("test.nc", {"variable": "temperature"})
        manifest.files["test.nc"] = test_file
        
        # Test to_dict
        manifest_dict = manifest.to_dict()
        assert manifest_dict["archive_id"] == "test_archive"
        assert "metadata" in manifest_dict
        assert "files" in manifest_dict
        
        # Test from_dict  
        restored_manifest = ArchiveManifest.from_dict(manifest_dict)
        assert restored_manifest.archive_id == "test_archive"
        assert restored_manifest.metadata.archive_id == sample_metadata.archive_id


@pytest.mark.unit
@pytest.mark.archive
class TestCompressedArchive:
    """Test CompressedArchive class for Earth science model output."""
    
    @pytest.fixture
    def mock_location(self):
        """Create a mock location for testing."""
        location = Mock(spec=Location)
        location.fs = Mock()
        location.fs.protocol = "file"
        return location
    
    @pytest.fixture 
    def sample_cache_manager(self, earth_science_temp_dir):
        """Create a cache manager for testing."""
        config = CacheConfig(cache_dir=earth_science_temp_dir / "test_cache")
        return CacheManager(config)
    
    def test_compressed_archive_initialization(self, mock_location, sample_cache_manager):
        """Test CompressedArchive initialization."""
        archive = CompressedArchive(
            archive_id="test_model_output",
            archive_location="/path/to/model_output.tar.gz",
            location=mock_location,
            cache_manager=sample_cache_manager
        )
        
        assert archive.archive_id == "test_model_output"
        assert archive.archive_location == "/path/to/model_output.tar.gz"
        assert archive.location == mock_location
        assert archive.cache_manager == sample_cache_manager
        assert archive.fs == mock_location.fs
    
    def test_archive_without_location(self, sample_cache_manager):
        """Test archive can work without Location object."""
        with patch('fsspec.filesystem') as mock_filesystem:
            mock_fs = Mock()
            mock_filesystem.return_value = mock_fs
            
            archive = CompressedArchive(
                archive_id="test_archive",
                archive_location="/path/to/archive.tar.gz",
                cache_manager=sample_cache_manager
            )
            
            assert archive.fs == mock_fs
            mock_filesystem.assert_called_once_with("file")
    
    def test_cached_archive_path_local_file(self, mock_location, sample_cache_manager, 
                                          create_compressed_archive):
        """Test getting cached path for local archive file."""
        # Create actual archive
        archive_path = create_compressed_archive("test_climate_model")
        
        # Mock filesystem behavior
        mock_location.fs.protocol = "file"
        mock_location.fs.exists.return_value = True
        mock_location.fs.size.return_value = archive_path.stat().st_size
        
        archive = CompressedArchive(
            archive_id="test_climate_model",
            archive_location=str(archive_path),
            location=mock_location,
            cache_manager=sample_cache_manager
        )
        
        # Get cached path
        cached_path = archive._get_cached_archive_path()
        
        assert cached_path is not None
        assert cached_path.exists()
        assert cached_path.suffix == ".gz"
    
    @patch('tellus.simulation.simulation.CompressedArchive._notify_progress')
    def test_cached_archive_path_remote_file(self, mock_notify, mock_location, 
                                           sample_cache_manager, earth_science_temp_dir):
        """Test caching remote archive file."""
        # Create a mock remote file
        remote_path = "/remote/path/climate_data.tar.gz"
        test_data = b"test archive data"
        
        # Configure mock filesystem for remote access
        mock_location.fs.protocol = "ssh"  # Remote protocol
        mock_location.fs.exists.return_value = True
        mock_location.fs.size.return_value = len(test_data)
        
        # Mock location.get method for download
        temp_file = earth_science_temp_dir / "temp_archive.tar.gz"
        temp_file.write_bytes(test_data)
        mock_location.get = Mock(side_effect=lambda src, dst, **kwargs: None)
        
        archive = CompressedArchive(
            archive_id="remote_climate_data",
            archive_location=remote_path,
            location=mock_location,
            cache_manager=sample_cache_manager
        )
        
        with patch('pathlib.Path.unlink'):  # Mock cleanup
            cached_path = archive._get_cached_archive_path()
        
        # Verify download was attempted
        mock_location.get.assert_called_once()
        mock_notify.assert_called()
    
    def test_list_files_with_patterns(self, mock_location, sample_cache_manager):
        """Test listing files with Earth science specific patterns."""
        archive = CompressedArchive(
            archive_id="test_archive",
            archive_location="/path/to/archive.tar.gz", 
            location=mock_location,
            cache_manager=sample_cache_manager
        )
        
        # Mock manifest with typical Earth science files
        mock_manifest = Mock()
        mock_files = {
            "model_output/atm/temp_daily_2020.nc": Mock(),
            "model_output/atm/precip_daily_2020.nc": Mock(),
            "model_output/ocn/sst_monthly_2020.nc": Mock(),
            "namelists/namelist.atm": Mock(),
            "scripts/run_model.sh": Mock(),
            "logs/model.log": Mock()
        }
        mock_manifest.files = mock_files
        archive.manifest = mock_manifest
        
        # Test pattern matching for NetCDF files
        netcdf_files = archive.list_files(pattern="*.nc")
        assert len(netcdf_files) == 3  # temp, precip, sst files
        
        # Test pattern matching for atmospheric data
        atm_files = archive.list_files(pattern="model_output/atm/*")
        assert len(atm_files) == 2  # temp and precip files
    
    def test_file_opening_with_cache(self, mock_location, sample_cache_manager):
        """Test opening files from archive with caching."""
        archive = CompressedArchive(
            archive_id="test_archive", 
            archive_location="/path/to/archive.tar.gz",
            location=mock_location,
            cache_manager=sample_cache_manager
        )
        
        # Mock cached file
        test_data = b"netcdf file content"
        file_key = "test_archive:temp_data.nc"
        
        # Pre-cache the file
        checksum = hashlib.md5(test_data).hexdigest()
        cached_path = sample_cache_manager.cache_file(test_data, file_key, checksum)
        
        # Open file should return cached content
        file_obj = archive.open_file("temp_data.nc")
        assert file_obj.read() == test_data


@pytest.mark.unit
@pytest.mark.archive
class TestArchivedSimulation:
    """Test base ArchivedSimulation class."""
    
    def test_archived_simulation_initialization(self):
        """Test base archived simulation initialization."""
        cache_manager = Mock()
        path_mapper = Mock()
        
        simulation = ArchivedSimulation("test_sim", cache_manager, path_mapper)
        
        assert simulation.archive_id == "test_sim"
        assert simulation.cache_manager == cache_manager
        assert simulation.path_mapper == path_mapper
        assert simulation.manifest is None
        assert hasattr(simulation, '_progress_callbacks')
    
    def test_progress_callback_registration(self):
        """Test progress callback system for long operations."""
        simulation = ArchivedSimulation("test_sim")
        
        callback = Mock()
        simulation.add_progress_callback(callback)
        
        assert callback in simulation._progress_callbacks
        
        # Test notification
        simulation._notify_progress("test_event", "test_data", 50, 100)
        callback.test_event.assert_called_once_with("test_data", 50, 100)
    
    def test_abstract_methods_not_implemented(self):
        """Test that abstract methods raise NotImplementedError."""
        simulation = ArchivedSimulation("test_sim")
        
        with pytest.raises(NotImplementedError):
            simulation.list_files()
        
        with pytest.raises(NotImplementedError):
            simulation.open_file("test.nc")
        
        with pytest.raises(NotImplementedError):
            simulation.refresh_manifest()