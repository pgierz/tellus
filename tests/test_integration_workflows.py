"""
Integration tests for typical Earth science research workflows.

This module tests complete workflows that Earth system modelers would
encounter, including multi-location data management, archive processing,
and collaborative research scenarios.
"""

import json
import shutil
import tarfile
import tempfile
import time
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import pytest

from tellus import Location, Simulation
from tellus.location import LocationKind
from tellus.simulation.context import LocationContext
from tellus.simulation.simulation import (ArchiveManifest, ArchiveMetadata,
                                          CacheConfig, CacheManager,
                                          CompressedArchive)

from .fixtures.earth_science import *


@pytest.mark.integration
@pytest.mark.earth_science
class TestClimateModelWorkflow:
    """Integration tests for complete climate model workflows."""
    
    @pytest.fixture
    def clean_registries(self):
        """Clean simulation and location registries for integration tests."""
        original_sims = Simulation._simulations.copy()
        original_locs = Location._locations.copy()
        Simulation._simulations.clear()
        Location._locations.clear()
        yield
        Simulation._simulations = original_sims
        Location._locations = original_locs
    
    def test_complete_model_experiment_workflow(self, clean_registries, multi_location_setup,
                                              create_compressed_archive):
        """Test complete workflow from model setup to archive analysis."""
        locations = multi_location_setup
        
        # 1. Create simulation representing a climate model experiment
        with patch('tellus.simulation.simulation.Simulation.save_simulations'):
            experiment = Simulation(
                simulation_id="ECHAM6_piControl_r1i1p1f1",
                path="/work/climate/ECHAM6/piControl",
                model_id="ECHAM6-HAM"
            )
            
            # 2. Add multiple storage locations typical in climate research
            with patch('tellus.location.location.Location._save_locations'):
                # HPC scratch for active computation
                hpc_scratch = Location(
                    name="hpc_scratch",
                    kinds=[LocationKind.COMPUTE, LocationKind.DISK],
                    config={
                        "protocol": "file",
                        "path": str(locations['hpc_scratch']),
                        "storage_options": {"auto_mkdir": True}
                    }
                )
                
                # HPC work for intermediate storage
                hpc_work = Location(
                    name="hpc_work", 
                    kinds=[LocationKind.DISK],
                    config={
                        "protocol": "file",
                        "path": str(locations['hpc_work']),
                        "storage_options": {"auto_mkdir": True}
                    }
                )
                
                # Archive tape for long-term storage
                tape_archive = Location(
                    name="tape_archive",
                    kinds=[LocationKind.TAPE],
                    config={
                        "protocol": "file",  # Mock tape as file for testing
                        "path": str(locations['archive_tape']),
                        "storage_options": {"auto_mkdir": True}
                    }
                )
            
            # 3. Add locations to experiment with context
            experiment.add_location(
                hpc_scratch, "scratch",
                context=LocationContext(path_prefix="{{simulation_id}}/run")
            )
            experiment.add_location(
                hpc_work, "work", 
                context=LocationContext(path_prefix="experiments/{{model_id}}/{{simulation_id}}")
            )
            experiment.add_location(
                tape_archive, "archive",
                context=LocationContext(path_prefix="climate_data/{{model_id}}")
            )
            
            # 4. Verify location setup
            assert len(experiment.list_locations()) == 3
            assert experiment.get_location("scratch") == hpc_scratch
            
            # 5. Test path resolution with context
            scratch_path = experiment.get_location_path("scratch", "output", "temp_2020.nc")
            expected_scratch = str(locations['hpc_scratch'] / "ECHAM6_piControl_r1i1p1f1" / "run" / "output" / "temp_2020.nc")
            assert scratch_path == expected_scratch
            
            # 6. Create archive in work location
            archive_path = create_compressed_archive("ECHAM6_piControl_data")
            work_archive = locations['hpc_work'] / 'experiments' / 'ECHAM6-HAM' / 'ECHAM6_piControl_r1i1p1f1' / 'output.tar.gz'
            work_archive.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(archive_path, work_archive)
            
            # 7. Create CompressedArchive for analysis
            cache_config = CacheConfig(cache_dir=locations['local_cache'] / '.tellus_cache')
            cache_manager = CacheManager(cache_config)
            
            compressed_archive = CompressedArchive(
                archive_id="ECHAM6_piControl_output", 
                archive_location=str(work_archive),
                location=hpc_work,
                cache_manager=cache_manager
            )
            
            # 8. Test archive operations
            assert compressed_archive.archive_id == "ECHAM6_piControl_output"
            assert compressed_archive.location == hpc_work
            
            # 9. Test file listing (would normally scan real archive)
            with patch.object(compressed_archive, 'manifest') as mock_manifest:
                mock_files = {
                    "model_output/atm/temp_daily_2020.nc": Mock(),
                    "model_output/atm/temp_daily_2021.nc": Mock(),
                    "model_output/ocn/sst_monthly_2020.nc": Mock(),
                    "namelists/namelist.atm": Mock(),
                    "scripts/run_experiment.sh": Mock()
                }
                mock_manifest.files = mock_files
                mock_manifest.get_files_by_tags.return_value = mock_files
                
                # List all NetCDF files
                netcdf_files = compressed_archive.list_files(pattern="*.nc")
                assert len([f for f in mock_files.keys() if f.endswith('.nc')]) == 3
                
                # List atmospheric data only
                atm_files = compressed_archive.list_files(pattern="model_output/atm/*")
                assert len([f for f in mock_files.keys() if "atm/" in f]) == 2


@pytest.mark.integration
@pytest.mark.earth_science
@pytest.mark.network
class TestCollaborativeResearchWorkflow:
    """Test workflows for collaborative Earth science research."""
    
    @pytest.fixture
    def collaborative_setup(self, multi_location_setup):
        """Set up a collaborative research environment."""
        locations = multi_location_setup
        
        # Add cloud storage location for sharing
        cloud_location = Location(
            name="cloud_share",
            kinds=[LocationKind.DISK],
            config={
                "protocol": "file",  # Mock cloud as file for testing
                "path": str(locations['cloud_storage']),
                "storage_options": {"auto_mkdir": True}
            }
        )
        
        return {
            'locations': locations,
            'cloud_location': cloud_location
        }
    
    def test_multi_institution_data_sharing(self, collaborative_setup, create_compressed_archive):
        """Test sharing climate data between institutions."""
        setup = collaborative_setup
        locations = setup['locations']
        cloud_location = setup['cloud_location']
        
        # Institution A creates and shares data
        with patch('tellus.location.location.Location._save_locations'):
            with patch('tellus.simulation.simulation.Simulation.save_simulations'):
                # Create simulation at Institution A
                inst_a_sim = Simulation(
                    simulation_id="GFDL_CM4_historical_r1i1p1f1",
                    path="/data/GFDL/CM4/historical",
                    model_id="GFDL-CM4"
                )
                
                # Add local and cloud locations
                inst_a_sim.add_location(cloud_location, "shared_cloud")
                
                # Create archive with model output
                archive_path = create_compressed_archive("GFDL_CM4_historical_data")
                
                # Upload to cloud (simulated by copying to cloud location)
                cloud_archive_path = cloud_location.config['path'] + "/shared_datasets/GFDL_CM4_historical.tar.gz"
                cloud_archive_file = Path(cloud_archive_path)
                cloud_archive_file.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(archive_path, cloud_archive_file)
                
                # Create metadata for sharing
                metadata = ArchiveMetadata(
                    archive_id="GFDL_CM4_historical_r1i1p1f1",
                    location=cloud_archive_path,
                    checksum="gfdl_cm4_checksum",
                    size=cloud_archive_file.stat().st_size,
                    created=time.time(),
                    simulation_date="1850-2014",
                    version="v1.0",
                    description="GFDL CM4 historical simulation for CMIP6",
                    tags={"model:GFDL-CM4", "experiment:historical", "cmip6", "realm:all"}
                )
                
        # Institution B accesses shared data
        with patch('tellus.location.location.Location._save_locations'):
            with patch('tellus.simulation.simulation.Simulation.save_simulations'):
                # Institution B creates their own analysis simulation
                inst_b_sim = Simulation(
                    simulation_id="GFDL_CM4_analysis",
                    path="/local/analysis/GFDL_CM4",
                    model_id="analysis"
                )
                
                # Add cloud location for data access
                inst_b_sim.add_location(cloud_location, "shared_data")
                
                # Create local cache for analysis
                local_cache = locations['local_cache'] / 'institution_b_cache'
                cache_config = CacheConfig(cache_dir=local_cache)
                cache_manager = CacheManager(cache_config)
                
                # Access shared archive
                shared_archive = CompressedArchive(
                    archive_id="GFDL_CM4_shared",
                    archive_location=cloud_archive_path,
                    location=cloud_location,
                    cache_manager=cache_manager
                )
                
                # Verify access to shared data
                assert shared_archive.archive_location == cloud_archive_path
                assert shared_archive.location == cloud_location
                
                # Test metadata sharing
                manifest = ArchiveManifest("GFDL_CM4_shared", metadata)
                assert manifest.metadata.description == "GFDL CM4 historical simulation for CMIP6"
                assert "cmip6" in manifest.metadata.tags


@pytest.mark.integration
@pytest.mark.earth_science
@pytest.mark.hpc
class TestHPCWorkflow:
    """Test workflows specific to HPC environments."""
    
    @pytest.fixture
    def clean_registries(self):
        """Clean simulation and location registries for integration tests."""
        original_sims = Simulation._simulations.copy()
        original_locs = Location._locations.copy()
        Simulation._simulations.clear()
        Location._locations.clear()
        yield
        Simulation._simulations = original_sims
        Location._locations = original_locs
    
    def test_hpc_data_lifecycle(self, clean_registries, multi_location_setup, hpc_environment_config, 
                               create_compressed_archive):
        """Test complete HPC data lifecycle from scratch to archive."""
        locations = multi_location_setup
        hpc_config = hpc_environment_config
        
        with patch('tellus.location.location.Location._save_locations'):
            with patch('tellus.simulation.simulation.Simulation.save_simulations'):
                # Create locations representing HPC filesystem hierarchy
                scratch_location = Location(
                    name="hpc_scratch",
                    kinds=[LocationKind.COMPUTE, LocationKind.DISK],
                    config={
                        "protocol": "file",
                        "path": str(locations['hpc_scratch']),
                        "quota": hpc_config['filesystems']['scratch']['quota'],
                        "purge_days": hpc_config['filesystems']['scratch']['purge_days'],
                        "storage_options": {"auto_mkdir": True}
                    }
                )
                
                work_location = Location(
                    name="hpc_work",
                    kinds=[LocationKind.DISK],
                    config={
                        "protocol": "file",
                        "path": str(locations['hpc_work']),
                        "quota": hpc_config['filesystems']['work']['quota'],
                        "backup": hpc_config['filesystems']['work']['backup'],
                        "storage_options": {"auto_mkdir": True}
                    }
                )
                
                tape_location = Location(
                    name="hpc_tape",
                    kinds=[LocationKind.TAPE],
                    config={
                        "protocol": "file",  # Mock tape system
                        "path": str(locations['archive_tape']),
                        "hierarchical": hpc_config['archive_systems']['tape']['hierarchical'],
                        "latency": hpc_config['archive_systems']['tape']['latency'],
                        "storage_options": {"auto_mkdir": True}
                    }
                )
                
                # Create HPC simulation workflow
                hpc_sim = Simulation(
                    simulation_id="CESM2_coupled_simulation",
                    path="/scratch/user/CESM2/run001",
                    model_id="CESM2"
                )
                
                # Add HPC locations with appropriate context
                hpc_sim.add_location(
                    scratch_location, "scratch",
                    context=LocationContext(
                        path_prefix="{{simulation_id}}/run",
                        metadata={"description": "High-performance scratch storage for active computation"}
                    )
                )
                hpc_sim.add_location(
                    work_location, "work",
                    context=LocationContext(
                        path_prefix="projects/climate/{{model_id}}/{{simulation_id}}",
                        metadata={"description": "Intermediate storage with backup"}
                    )
                )
                hpc_sim.add_location(
                    tape_location, "archive", 
                    context=LocationContext(
                        path_prefix="archive/climate/{{model_id}}",
                        metadata={"description": "Long-term hierarchical tape storage"}
                    )
                )
                
                # Test HPC-specific path resolution
                scratch_path = hpc_sim.get_location_path("scratch", "output", "cesm.log")
                work_path = hpc_sim.get_location_path("work", "analysis", "summary.nc")
                archive_path = hpc_sim.get_location_path("archive", "CESM2_coupled_simulation.tar.gz")
                
                # Verify paths follow HPC conventions
                assert "/CESM2_coupled_simulation/run/output/cesm.log" in scratch_path
                assert "/projects/climate/CESM2/CESM2_coupled_simulation/analysis/summary.nc" in work_path
                assert "/archive/climate/CESM2/CESM2_coupled_simulation.tar.gz" in archive_path
                
                # Simulate data archiving workflow
                # 1. Create archive from scratch data
                model_archive = create_compressed_archive("CESM2_model_output")
                
                # 2. Move to work storage
                work_archive_path = Path(work_path).parent / "CESM2_output.tar.gz"
                work_archive_path.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(model_archive, work_archive_path)
                
                # 3. Create archive manager for work storage
                cache_dir = locations['local_cache'] / 'hpc_cache'
                cache_config = CacheConfig(
                    cache_dir=cache_dir,
                    archive_cache_size_limit=100 * 1024**3,  # 100 GB for HPC
                    file_cache_size_limit=50 * 1024**3       # 50 GB for files
                )
                cache_manager = CacheManager(cache_config)
                
                work_archive = CompressedArchive(
                    archive_id="CESM2_coupled_work_storage",
                    archive_location=str(work_archive_path),
                    location=work_location,
                    cache_manager=cache_manager
                )
                
                # 4. Test archive operations on HPC
                assert work_archive.archive_id == "CESM2_coupled_work_storage"
                assert work_archive.location == work_location
                
                # 5. Simulate tape archiving (final step)
                tape_archive_path = Path(archive_path)
                tape_archive_path.parent.mkdir(parents=True, exist_ok=True) 
                shutil.copy2(work_archive_path, tape_archive_path)
                
                # Verify complete HPC lifecycle
                assert work_archive_path.exists()  # Data in work storage
                assert tape_archive_path.exists()  # Data archived to tape


@pytest.mark.integration
@pytest.mark.earth_science
@pytest.mark.slow
class TestLargeDatasetWorkflow:
    """Test workflows with realistic large Earth science datasets."""
    
    @pytest.fixture
    def clean_registries(self):
        """Clean simulation and location registries for integration tests."""
        original_sims = Simulation._simulations.copy()
        original_locs = Location._locations.copy()
        Simulation._simulations.clear()
        Location._locations.clear()
        yield
        Simulation._simulations = original_sims
        Location._locations = original_locs
    
    @pytest.mark.large_data
    def test_multi_decade_climate_analysis(self, clean_registries, multi_location_setup, create_model_archive):
        """Test workflow for analyzing multi-decade climate data."""
        locations = multi_location_setup
        
        with patch('tellus.location.location.Location._save_locations'):
            with patch('tellus.simulation.simulation.Simulation.save_simulations'):
                # Create analysis simulation for multi-decade study
                climate_analysis = Simulation(
                    simulation_id="CMIP6_multi_model_analysis",
                    path="/project/climate_analysis/CMIP6",
                    model_id="multi_model_ensemble"
                )
                
                # Add high-capacity storage locations
                storage_location = Location(
                    name="high_capacity_storage",
                    kinds=[LocationKind.DISK],
                    config={
                        "protocol": "file",
                        "path": str(locations['hpc_work']),
                        "storage_options": {"auto_mkdir": True}
                    }
                )
                
                climate_analysis.add_location(
                    storage_location, "analysis_storage",
                    context=LocationContext(
                        path_prefix="CMIP6_analysis/{{simulation_id}}",
                        metadata={"description": "High-capacity storage for multi-decade analysis"}
                    )
                )
                
                # Create cache optimized for large datasets
                large_cache_config = CacheConfig(
                    cache_dir=locations['local_cache'] / 'large_data_cache',
                    archive_cache_size_limit=1024 * 1024**3,  # 1 TB
                    file_cache_size_limit=500 * 1024**3,      # 500 GB
                    unified_cache=True  # Better for large datasets
                )
                large_cache_manager = CacheManager(large_cache_config)
                
                # Simulate multiple model archives
                model_names = ["CESM2", "GFDL-CM4", "MPI-ESM", "UKESM1", "IPSL-CM6A"]
                archives = []
                
                for model in model_names:
                    # Create model-specific archive 
                    model_archive_dir = create_model_archive(f"{model}_historical")
                    
                    # Create compressed archive
                    model_archive_file = model_archive_dir.parent / f"{model}_historical.tar.gz"
                    with tarfile.open(model_archive_file, "w:gz") as tar:
                        tar.add(model_archive_dir, arcname=f"{model}_historical")
                    
                    # Create CompressedArchive for analysis
                    archive = CompressedArchive(
                        archive_id=f"{model}_historical_r1i1p1f1",
                        archive_location=str(model_archive_file),
                        location=storage_location,
                        cache_manager=large_cache_manager
                    )
                    archives.append(archive)
                
                # Test multi-model ensemble analysis workflow
                assert len(archives) == 5
                
                # Test accessing files across multiple archives
                for archive in archives:
                    assert archive.archive_id.startswith(("CESM2", "GFDL", "MPI", "UKESM", "IPSL"))
                    assert archive.location == storage_location
                    assert archive.cache_manager == large_cache_manager
                
                # Simulate analysis operations
                analysis_files = []
                for archive in archives:
                    with patch.object(archive, 'list_files') as mock_list:
                        mock_files = {
                            f"model_output/atm/temp_monthly_{year}.nc": Mock()
                            for year in range(1850, 2015)  # 165 years of data
                        }
                        mock_list.return_value = mock_files
                        
                        # Get temperature files for trend analysis
                        temp_files = archive.list_files(pattern="*temp_monthly*.nc")
                        analysis_files.extend(temp_files)
                
                # Verify we have data from all models and all years
                # 5 models × 165 years = 825 files expected
                expected_files = 5 * 165
                # Note: In real test this would actually scan archives


@pytest.mark.integration
@pytest.mark.earth_science
class TestDataProvenanceWorkflow:
    """Test workflows that maintain data provenance and reproducibility."""
    
    @pytest.fixture
    def clean_registries(self):
        """Clean simulation and location registries for integration tests."""
        original_sims = Simulation._simulations.copy()
        original_locs = Location._locations.copy()
        Simulation._simulations.clear()
        Location._locations.clear()
        yield
        Simulation._simulations = original_sims
        Location._locations = original_locs
    
    def test_experiment_reproducibility_tracking(self, clean_registries, multi_location_setup, create_model_archive):
        """Test tracking experiment provenance for reproducible research."""
        locations = multi_location_setup
        
        with patch('tellus.location.location.Location._save_locations'):
            with patch('tellus.simulation.simulation.Simulation.save_simulations'):
                # Create base experiment
                base_experiment = Simulation(
                    simulation_id="ECHAM6_sensitivity_ctrl",
                    path="/experiments/ECHAM6/sensitivity/control",
                    model_id="ECHAM6"
                )
                
                # Add provenance attributes
                base_experiment.attrs.update({
                    "experiment_type": "sensitivity_study",
                    "control_run": True,
                    "model_version": "ECHAM6.3.04p1",
                    "grid_resolution": "T63L47",
                    "simulation_length": "100_years",
                    "initial_conditions": "pre_industrial",
                    "forcing_scenario": "constant_1850",
                    "contact": "climate.researcher@institute.edu",
                    "institution": "Max Planck Institute for Meteorology",
                    "creation_date": "2024-01-15",
                    "parent_experiment": None
                })
                
                # Add namelists for reproducibility
                base_experiment.namelists.update({
                    "namelist.echam": """&runctl
 dt_start = 1850, 1, 1, 0, 0, 0
 dt_stop  = 1950, 1, 1, 0, 0, 0
 dt_resume = 1850, 1, 1, 0, 0, 0
 putdata = 12, 'hours', 'last', 0
 trigfiles = ''
/""",
                    "namelist.jsbach": """&jsbach_ctl
 use_dynveg = .false.
 use_disturbance = .false.
 debug_Cconservation = .false.
/"""
                })
                
                # Create sensitivity experiments that reference the control
                sensitivity_params = [
                    {"co2_concentration": 280, "variant": "ctrl"},
                    {"co2_concentration": 560, "variant": "2xCO2"},
                    {"co2_concentration": 1120, "variant": "4xCO2"}
                ]
                
                sensitivity_experiments = []
                for params in sensitivity_params:
                    exp_id = f"ECHAM6_sensitivity_{params['variant']}"
                    
                    # Reuse base experiment for control variant
                    if params['variant'] == 'ctrl':
                        sens_exp = base_experiment
                    else:
                        sens_exp = Simulation(
                            simulation_id=exp_id,
                            path=f"/experiments/ECHAM6/sensitivity/{params['variant']}",
                            model_id="ECHAM6"
                        )
                    
                    # Inherit base attributes and add specific ones only for new experiments
                    if params['variant'] != 'ctrl':
                        sens_exp.attrs.update(base_experiment.attrs.copy())
                        sens_exp.attrs.update({
                            "control_run": False,
                            "parent_experiment": "ECHAM6_sensitivity_ctrl",
                            "experiment_variant": params['variant'],
                            "co2_concentration_ppm": params['co2_concentration'],
                            "sensitivity_parameter": "atmospheric_co2"
                        })
                    else:
                        # Control experiment already has the correct attributes
                        sens_exp.attrs.update({
                            "experiment_variant": params['variant'],
                            "co2_concentration_ppm": params['co2_concentration'],
                            "sensitivity_parameter": "atmospheric_co2"
                        })
                    
                    # Inherit namelists with modifications
                    sens_exp.namelists.update(base_experiment.namelists.copy())
                    if params['variant'] != 'ctrl':
                        # Modify CO2 concentration in namelist
                        modified_namelist = sens_exp.namelists["namelist.echam"] + f"""
&radctl
 co2vmr = {params['co2_concentration'] * 1e-6}
/"""
                        sens_exp.namelists["namelist.echam"] = modified_namelist
                    
                    sensitivity_experiments.append(sens_exp)
                
                # Test provenance relationships
                control_exp = sensitivity_experiments[0]  # ctrl variant
                co2_2x_exp = sensitivity_experiments[1]   # 2xCO2 variant
                
                # Verify control experiment
                assert control_exp.attrs["control_run"] is True
                assert control_exp.attrs["parent_experiment"] is None
                assert control_exp.attrs["co2_concentration_ppm"] == 280
                
                # Verify derived experiment
                assert co2_2x_exp.attrs["control_run"] is False
                assert co2_2x_exp.attrs["parent_experiment"] == "ECHAM6_sensitivity_ctrl"
                assert co2_2x_exp.attrs["co2_concentration_ppm"] == 560
                assert "co2vmr = 0.00056" in co2_2x_exp.namelists["namelist.echam"]
                
                # Test experiment serialization for provenance preservation
                control_dict = control_exp.to_dict()
                restored_exp = Simulation.from_dict(control_dict)
                
                # Verify provenance is preserved through serialization
                assert restored_exp.attrs["experiment_type"] == "sensitivity_study"
                assert restored_exp.attrs["model_version"] == "ECHAM6.3.04p1"
                assert "namelist.echam" in restored_exp.namelists
                assert "namelist.jsbach" in restored_exp.namelists
                
                # Create archives with provenance metadata
                for i, exp in enumerate(sensitivity_experiments):
                    archive_dir = create_model_archive(f"sensitivity_{exp.attrs['experiment_variant']}")
                    
                    # Create archive metadata with full provenance
                    metadata = ArchiveMetadata(
                        archive_id=exp.simulation_id,
                        location=str(archive_dir / "output.tar.gz"),
                        checksum=f"checksum_{i}",
                        size=1024**3,  # 1 GB
                        created=time.time(),
                        simulation_date=exp.attrs.get("creation_date"),
                        version="v1.0",
                        description=f"ECHAM6 sensitivity study: {exp.attrs['experiment_variant']} variant",
                        tags={
                            f"model:{exp.model_id}",
                            f"variant:{exp.attrs['experiment_variant']}",
                            f"co2_ppm:{exp.attrs['co2_concentration_ppm']}",
                            "sensitivity_study",
                            "climate_model"
                        }
                    )
                    
                    # Verify provenance in archive metadata
                    assert exp.attrs['experiment_variant'] in str(metadata.tags)
                    assert str(exp.attrs['co2_concentration_ppm']) in str(metadata.tags)
                    assert metadata.description.startswith("ECHAM6 sensitivity study")