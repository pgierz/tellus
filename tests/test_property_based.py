"""
Property-based tests for data integrity in Earth science workflows.

This module uses Hypothesis to generate test cases that verify data integrity
properties hold across a wide range of inputs, ensuring robustness of the
archive system for Earth science data.
"""

import hashlib
import json
import tarfile
import tempfile
import time
from pathlib import Path
from typing import Any, Dict, List
from unittest.mock import Mock, patch

# Import hypothesis at module level
import hypothesis
import pytest
from hypothesis import HealthCheck, assume, given, settings
from hypothesis import strategies as st
from hypothesis.stateful import Bundle, RuleBasedStateMachine, initialize, rule

HAS_HYPOTHESIS = True

from tellus import Simulation
from tellus.location import Location, LocationKind
from tellus.simulation.simulation import (ArchiveManifest, ArchiveMetadata,
                                          CacheConfig, CacheEntry,
                                          CacheManager, CompressedArchive)

from .fixtures.earth_science import *

pytestmark = pytest.mark.skipif(not HAS_HYPOTHESIS, reason="Hypothesis not available")


# Hypothesis strategies for Earth science data
earth_science_filenames = st.one_of(
    st.text(min_size=1, max_size=50).filter(lambda x: x.isascii() and '/' not in x and '\\' not in x),
    st.sampled_from([
        "temp_daily_2020.nc", "precip_monthly_2021.nc", "wind_hourly_2019.nc",
        "sst_annual_2018.nc", "soil_temp_daily_2020.nc", "namelist.atm",
        "run_model.sh", "model.log", "restart_20200101.nc"
    ])
)

earth_science_variables = st.sampled_from([
    "temperature", "precipitation", "wind_speed", "pressure", "humidity",
    "sea_surface_temperature", "soil_moisture", "snow_depth", "sea_ice_concentration"
])

earth_science_models = st.sampled_from([
    "ECHAM6", "CESM2", "GFDL-CM4", "MPI-ESM", "UKESM1", "IPSL-CM6A",
    "ACCESS-CM2", "CanESM5", "MIROC6", "NorESM2"
])

climate_experiments = st.sampled_from([
    "piControl", "historical", "ssp126", "ssp245", "ssp585", "1pctCO2",
    "abrupt4xCO2", "amip", "omip1", "omip2"
])

file_sizes = st.integers(min_value=1024, max_value=10*1024**3)  # 1KB to 10GB

archive_tags = st.sets(
    st.text(alphabet=st.characters(whitelist_categories=("Lu", "Ll", "Nd", "Pc")), 
           min_size=1, max_size=20),
    min_size=0, max_size=10
)


@pytest.mark.property
@pytest.mark.unit
class TestCacheIntegrityProperties:
    """Property-based tests for cache integrity."""
    
    @given(data=st.binary(min_size=1, max_size=1024*1024))  # Up to 1MB
    @settings(max_examples=50, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_cache_entry_roundtrip_integrity(self, data, earth_science_temp_dir):
        """Test that cached data maintains integrity through roundtrip operations."""
        import uuid
        unique_dir = earth_science_temp_dir / f"property_cache_{uuid.uuid4().hex[:8]}"
        cache_config = CacheConfig(cache_dir=unique_dir)
        cache_manager = CacheManager(cache_config)
        
        # Calculate original checksum
        original_checksum = hashlib.md5(data).hexdigest()
        file_key = f"test_file_{len(data)}.nc"
        
        # Cache the data
        cached_path = cache_manager.cache_file(data, file_key, original_checksum)
        
        # Retrieve and verify
        retrieved_path = cache_manager.get_file_path(file_key)
        assert retrieved_path == cached_path
        
        # Verify data integrity
        cached_data = retrieved_path.read_bytes()
        assert cached_data == data
        
        # Verify checksum integrity
        retrieved_checksum = hashlib.md5(cached_data).hexdigest()
        assert retrieved_checksum == original_checksum
    
    @given(
        checksums=st.lists(
            st.text(alphabet="0123456789abcdef", min_size=32, max_size=32),
            min_size=1, max_size=100,
            unique=True
        )
    )
    @settings(max_examples=20, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_cache_index_consistency(self, checksums, earth_science_temp_dir):
        """Test that cache index remains consistent across operations."""
        import uuid
        unique_dir = earth_science_temp_dir / f"index_cache_{uuid.uuid4().hex[:8]}"
        cache_config = CacheConfig(cache_dir=unique_dir)
        cache_manager = CacheManager(cache_config)
        
        # Cache multiple files
        cached_files = {}
        for i, checksum in enumerate(checksums):
            data = f"test data {i}".encode()
            file_key = f"file_{i}.nc"
            cached_path = cache_manager.cache_file(data, file_key, checksum)
            cached_files[file_key] = (cached_path, checksum, data)
        
        # Verify all files are in index
        assert len(cache_manager._file_index) == len(checksums)
        
        # Create new cache manager (should load from saved index)
        cache_manager2 = CacheManager(cache_config)
        
        # Verify index consistency
        assert len(cache_manager2._file_index) == len(checksums)
        
        # Verify all cached files are still accessible
        for file_key, (original_path, checksum, original_data) in cached_files.items():
            retrieved_path = cache_manager2.get_file_path(file_key)
            assert retrieved_path == original_path
            assert retrieved_path.exists()
            assert retrieved_path.read_bytes() == original_data


@pytest.mark.property
@pytest.mark.unit
class TestArchiveMetadataProperties:
    """Property-based tests for archive metadata integrity."""
    
    @given(
        archive_id=st.text(min_size=1, max_size=100),
        location=st.text(min_size=1, max_size=200),
        checksum=st.text(alphabet="0123456789abcdef", min_size=32, max_size=64),
        size=file_sizes,
        tags=archive_tags
    )
    @settings(max_examples=50, suppress_health_check=[HealthCheck.filter_too_much])
    def test_archive_metadata_serialization_roundtrip(self, archive_id, location, 
                                                     checksum, size, tags):
        """Test that archive metadata serializes and deserializes correctly."""
        # Filter out problematic characters that might cause issues
        assume(archive_id.isascii() and location.isascii())
        assume(all(tag.isascii() for tag in tags))
        
        # Create metadata
        metadata = ArchiveMetadata(
            archive_id=archive_id,
            location=location,
            checksum=checksum,
            size=size,
            created=1234567890.0,
            tags=tags
        )
        
        # Serialize to dict
        metadata_dict = {
            "archive_id": metadata.archive_id,
            "location": metadata.location,
            "checksum": metadata.checksum,
            "size": metadata.size,
            "created": metadata.created,
            "simulation_date": metadata.simulation_date,
            "version": metadata.version,
            "description": metadata.description,
            "tags": list(metadata.tags)
        }
        
        # Deserialize from dict
        restored_metadata = ArchiveMetadata(
            archive_id=metadata_dict["archive_id"],
            location=metadata_dict["location"],
            checksum=metadata_dict["checksum"],
            size=metadata_dict["size"],
            created=metadata_dict["created"],
            simulation_date=metadata_dict["simulation_date"],
            version=metadata_dict["version"],
            description=metadata_dict["description"],
            tags=set(metadata_dict["tags"])
        )
        
        # Verify roundtrip integrity
        assert restored_metadata.archive_id == metadata.archive_id
        assert restored_metadata.location == metadata.location
        assert restored_metadata.checksum == metadata.checksum
        assert restored_metadata.size == metadata.size
        assert restored_metadata.created == metadata.created
        assert restored_metadata.tags == metadata.tags
    
    @given(
        model=earth_science_models,
        experiment=climate_experiments,
        variable=earth_science_variables,
        year_start=st.integers(min_value=1850, max_value=2020),
        year_end=st.integers(min_value=1851, max_value=2100)
    )
    @settings(max_examples=30)
    def test_earth_science_metadata_consistency(self, model, experiment, variable, 
                                               year_start, year_end):
        """Test metadata consistency for Earth science specific attributes."""
        assume(year_end > year_start)
        assume(year_end - year_start <= 200)  # Reasonable simulation length
        
        # Create Earth science specific metadata
        archive_id = f"{model}_{experiment}_{variable}_{year_start}_{year_end}"
        
        metadata = ArchiveMetadata(
            archive_id=archive_id,
            location=f"/archive/{model}/{experiment}/{archive_id}.tar.gz",
            checksum="a" * 32,  # Dummy checksum
            size=1024**3,  # 1 GB
            created=1234567890.0,
            simulation_date=f"{year_start}-{year_end}",
            version="v1.0",
            description=f"{model} {experiment} simulation with {variable} output",
            tags={f"model:{model}", f"experiment:{experiment}", f"variable:{variable}"}
        )
        
        # Verify Earth science conventions
        assert model in metadata.archive_id
        assert experiment in metadata.archive_id
        assert f"model:{model}" in metadata.tags
        assert f"experiment:{experiment}" in metadata.tags
        assert str(year_start) in metadata.simulation_date
        assert str(year_end) in metadata.simulation_date


@pytest.mark.property
@pytest.mark.integration
class TestSimulationStateProperties:
    """Property-based tests for simulation state consistency."""
    
    @given(
        simulation_id=st.text(min_size=1, max_size=50).filter(
            lambda x: x.isascii() and x.replace('_', '').replace('-', '').isalnum()
        ),
        model_id=earth_science_models,
        path=st.text(min_size=1, max_size=100).filter(lambda x: x.isascii())
    )
    @settings(max_examples=20, suppress_health_check=[HealthCheck.filter_too_much])
    def test_simulation_serialization_integrity(self, simulation_id, model_id, path):
        """Test that simulation state is preserved through serialization."""
        # Clean registries for this test
        original_sims = Simulation._simulations.copy()
        Simulation._simulations.clear()
        
        try:
            with patch('tellus.simulation.simulation.Simulation.save_simulations'):
                # Create simulation
                sim = Simulation(
                    simulation_id=simulation_id,
                    path=path,
                    model_id=model_id
                )
                
                # Add some attributes
                sim.attrs["test_attr"] = "test_value"
                sim.attrs["creation_time"] = 1234567890.0
                
                # Serialize to dict
                sim_dict = sim.to_dict()
                
                # Clear and restore from dict
                Simulation._simulations.clear()
                restored_sim = Simulation.from_dict(sim_dict)
                
                # Verify integrity
                assert restored_sim.simulation_id == simulation_id
                assert restored_sim.model_id == model_id
                assert restored_sim.path == path
                assert restored_sim.attrs["test_attr"] == "test_value"
                assert restored_sim.attrs["creation_time"] == 1234567890.0
                
        finally:
            Simulation._simulations = original_sims
    
    @given(
        location_names=st.lists(
            st.text(min_size=1, max_size=20).filter(
                lambda x: x.isascii() and x.isalnum()
            ),
            min_size=1, max_size=5,
            unique=True
        )
    )
    @settings(max_examples=15)
    def test_simulation_location_consistency(self, location_names):
        """Test consistency of simulation location management."""
        original_sims = Simulation._simulations.copy()
        original_locs = Location._locations.copy()
        Simulation._simulations.clear()
        Location._locations.clear()
        
        try:
            with patch('tellus.simulation.simulation.Simulation.save_simulations'):
                with patch('tellus.location.location.Location._save_locations'):
                    # Create simulation
                    sim = Simulation(
                        simulation_id="test_sim",
                        path="/test/path",
                        model_id="TEST_MODEL"
                    )
                    
                    # Create and add locations
                    added_locations = {}
                    for loc_name in location_names:
                        location = Location(
                            name=f"location_{loc_name}",
                            kinds=[LocationKind.DISK],
                            config={"protocol": "file", "path": f"/path/{loc_name}"}
                        )
                        sim.add_location(location, loc_name)
                        added_locations[loc_name] = location
                    
                    # Verify all locations were added
                    assert len(sim.list_locations()) == len(location_names)
                    
                    # Verify each location can be retrieved
                    for loc_name in location_names:
                        retrieved_location = sim.get_location(loc_name)
                        assert retrieved_location == added_locations[loc_name]
                    
                    # Test serialization preserves locations
                    sim_dict = sim.to_dict()
                    assert len(sim_dict["locations"]) == len(location_names)
                    
        finally:
            Simulation._simulations = original_sims
            Location._locations = original_locs


@pytest.mark.property
@pytest.mark.integration
class TestArchiveStateMachine(RuleBasedStateMachine):
    """Stateful property-based testing for archive operations."""
    
    def __init__(self):
        super().__init__()
        self.temp_dir = Path(tempfile.mkdtemp(prefix="tellus_stateful_"))
        self.cache_config = CacheConfig(cache_dir=self.temp_dir / "cache")
        self.cache_manager = CacheManager(self.cache_config)
        self.archives = {}
        self.files = {}
    
    archives = Bundle('archives')
    files = Bundle('files')
    
    @initialize()
    def setup_environment(self):
        """Initialize the test environment."""
        self.temp_dir.mkdir(exist_ok=True)
    
    @rule(target=archives, 
          archive_id=st.text(alphabet=st.characters(min_codepoint=65, max_codepoint=122, blacklist_categories=('Cc', 'Cs')), min_size=1, max_size=30).filter(lambda x: '/' not in x and '\\' not in x and x.strip()),
          size_kb=st.integers(min_value=1, max_value=1000))
    def create_archive(self, archive_id, size_kb):
        """Create a new archive."""
        assume(archive_id not in self.archives)
        assume(len(archive_id.strip()) > 0)
        
        # Create archive directory
        archive_dir = self.temp_dir / f"archive_{archive_id}"
        archive_dir.mkdir(exist_ok=True)
        
        # Create some files in the archive
        test_data = b"test data" * (size_kb // 10)
        test_file = archive_dir / "data.nc"
        test_file.write_bytes(test_data)
        
        # Create tar archive
        archive_path = self.temp_dir / f"{archive_id}.tar.gz"
        with tarfile.open(archive_path, "w:gz") as tar:
            tar.add(archive_dir, arcname=archive_id)
        
        self.archives[archive_id] = {
            'path': archive_path,
            'size': archive_path.stat().st_size,
            'checksum': self.cache_manager._calculate_checksum(archive_path)
        }
        
        return archive_id
    
    @rule(archive_id=archives)
    def cache_archive(self, archive_id):
        """Cache an existing archive."""
        archive_info = self.archives[archive_id]
        cached_path = self.cache_manager.cache_archive(
            archive_info['path'], 
            archive_info['checksum']
        )
        
        # Verify caching worked
        assert cached_path.exists()
        assert archive_info['checksum'] in self.cache_manager._archive_index
        
        # Update archive info
        archive_info['cached_path'] = cached_path
    
    @rule(archive_id=archives)
    def retrieve_cached_archive(self, archive_id):
        """Retrieve a cached archive."""
        archive_info = self.archives[archive_id]
        
        if 'cached_path' in archive_info:
            retrieved_path = self.cache_manager.get_archive_path(archive_info['checksum'])
            assert retrieved_path == archive_info['cached_path']
            assert retrieved_path.exists()
    
    @rule(target=files,
          archive_id=archives,
          filename=earth_science_filenames,
          data=st.binary(min_size=1, max_size=10000))
    def add_file_to_cache(self, archive_id, filename, data):
        """Add a file to the file cache."""
        assume(f"{archive_id}:{filename}" not in self.files)
        
        file_key = f"{archive_id}:{filename}"
        checksum = hashlib.md5(data).hexdigest()
        
        cached_path = self.cache_manager.cache_file(data, file_key, checksum)
        
        self.files[file_key] = {
            'path': cached_path,
            'data': data,
            'checksum': checksum
        }
        
        return file_key
    
    @rule(file_key=files)
    def retrieve_cached_file(self, file_key):
        """Retrieve a cached file."""
        file_info = self.files[file_key]
        
        retrieved_path = self.cache_manager.get_file_path(file_key)
        assert retrieved_path == file_info['path']
        assert retrieved_path.exists()
        
        # Verify data integrity
        cached_data = retrieved_path.read_bytes()
        assert cached_data == file_info['data']
        
        # Verify checksum
        actual_checksum = hashlib.md5(cached_data).hexdigest()
        assert actual_checksum == file_info['checksum']
    
    def teardown(self):
        """Clean up after testing."""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)


# Run the stateful test
if HAS_HYPOTHESIS:
    TestArchiveStateMachineTest = TestArchiveStateMachine.TestCase
    TestArchiveStateMachineTest.settings = settings(
        max_examples=20, 
        stateful_step_count=10, 
        suppress_health_check=[HealthCheck.filter_too_much]
    )


@pytest.mark.property 
@pytest.mark.earth_science
class TestEarthScienceDataProperties:
    """Property-based tests specific to Earth science data patterns."""
    
    @given(
        years=st.lists(st.integers(min_value=1850, max_value=2100), 
                      min_size=1, max_size=50, unique=True),
        frequency=st.sampled_from(['daily', 'monthly', 'yearly', '6hourly']),
        variable=earth_science_variables
    )
    @settings(max_examples=25)
    def test_temporal_data_consistency(self, years, frequency, variable):
        """Test consistency of temporal Earth science data organization."""
        # Sort years for realistic time series
        years.sort()
        
        # Generate filenames following Earth science conventions
        filenames = []
        for year in years:
            if frequency == 'daily':
                filename = f"{variable}_daily_{year}.nc"
            elif frequency == 'monthly':
                filename = f"{variable}_monthly_{year}.nc"
            elif frequency == 'yearly':
                filename = f"{variable}_annual_{year}.nc"
            elif frequency == '6hourly':
                filename = f"{variable}_6hourly_{year}.nc"
            else:
                filename = f"{variable}_{frequency}_{year}.nc"
            
            filenames.append(filename)
        
        # Test filename parsing consistency
        parsed_years = []
        for filename in filenames:
            # Extract year from filename - look for 4-digit years
            parts = filename.split('_')
            for part in parts:
                # Remove file extension first
                part_no_ext = part.replace('.nc', '')
                if part_no_ext.isdigit() and len(part_no_ext) == 4 and 1850 <= int(part_no_ext) <= 2100:
                    parsed_years.append(int(part_no_ext))
                    break
        
        # Verify all years are preserved in filenames
        assert set(parsed_years) == set(years)
        
        # Verify frequency consistency (handle yearly -> annual conversion)
        for filename in filenames:
            expected_freq = 'annual' if frequency == 'yearly' else frequency
            assert expected_freq in filename
            assert variable in filename
            assert filename.endswith('.nc')
    
    @given(
        models=st.lists(earth_science_models, min_size=1, max_size=10, unique=True),
        experiments=st.lists(climate_experiments, min_size=1, max_size=5, unique=True)
    )
    @settings(max_examples=15) 
    def test_model_ensemble_organization(self, models, experiments):
        """Test consistency of multi-model ensemble organization."""
        # Create archive structure for ensemble
        ensemble_archives = []
        
        for model in models:
            for experiment in experiments:
                archive_id = f"{model}_{experiment}_r1i1p1f1"
                
                # Test archive ID follows CMIP conventions
                assert model in archive_id
                assert experiment in archive_id
                assert "r1i1p1f1" in archive_id  # CMIP realization notation
                
                ensemble_archives.append(archive_id)
        
        # Verify ensemble completeness
        expected_count = len(models) * len(experiments)
        assert len(ensemble_archives) == expected_count
        
        # Verify unique combinations
        assert len(set(ensemble_archives)) == expected_count
        
        # Test grouping by model
        model_groups = {}
        for archive_id in ensemble_archives:
            for model in models:
                if archive_id.startswith(model):
                    if model not in model_groups:
                        model_groups[model] = []
                    model_groups[model].append(archive_id)
                    break
        
        # Each model should have archives for all experiments
        for model in models:
            assert len(model_groups[model]) == len(experiments)


@pytest.mark.property
@pytest.mark.performance
class TestPerformanceProperties:
    """Property-based tests for performance characteristics."""
    
    @given(
        file_sizes=st.lists(
            st.integers(min_value=1024, max_value=100*1024**2),  # 1KB to 100MB
            min_size=1, max_size=20
        )
    )
    @settings(max_examples=10, deadline=30000)  # 30 second deadline
    def test_cache_scaling_properties(self, file_sizes, earth_science_temp_dir):
        """Test that cache operations scale reasonably with data size."""
        import uuid
        unique_dir = earth_science_temp_dir / f"scaling_cache_{uuid.uuid4().hex[:8]}"
        cache_config = CacheConfig(cache_dir=unique_dir)
        cache_manager = CacheManager(cache_config)
        
        cache_times = []
        retrieval_times = []
        
        for i, size in enumerate(file_sizes):
            # Create test data
            data = b"x" * size
            file_key = f"scaling_test_{i}.nc"
            checksum = hashlib.md5(data).hexdigest()
            
            # Measure cache time
            start_time = time.time()
            cached_path = cache_manager.cache_file(data, file_key, checksum)
            cache_time = time.time() - start_time
            cache_times.append(cache_time)
            
            # Measure retrieval time
            start_time = time.time()
            retrieved_path = cache_manager.get_file_path(file_key)
            retrieval_time = time.time() - start_time
            retrieval_times.append(retrieval_time)
            
            # Verify operation succeeded
            assert retrieved_path == cached_path
            assert retrieved_path.exists()
        
        # Performance properties
        if len(file_sizes) > 1:
            # Cache time should generally increase with file size
            # (allowing for some variability due to system factors)
            max_size = max(file_sizes)
            min_size = min(file_sizes)
            max_time = max(cache_times)
            min_time = min(cache_times)
            
            # Cache time shouldn't grow unreasonably fast
            # This is a loose bound to catch major performance regressions
            size_ratio = max_size / min_size
            time_ratio = max_time / min_time if min_time > 0 else 1
            
            # Time ratio shouldn't be much larger than size ratio
            # (allowing for overhead and system variability)
            assert time_ratio <= size_ratio * 10
            
            # Retrieval times should be consistently fast regardless of file size
            max_retrieval_time = max(retrieval_times)
            assert max_retrieval_time < 1.0  # Should retrieve any cached file in under 1 second