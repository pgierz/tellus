"""
Earth Science specific test fixtures for tellus.

This module provides fixtures that create realistic Earth science data
scenarios for testing the archive system.
"""

import io
import json
import tarfile
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np
import pytest

try:
    import netCDF4 as nc
    import xarray as xr
    HAS_NETCDF = True
except ImportError:
    HAS_NETCDF = False

try:
    import zarr
    HAS_ZARR = True
except ImportError:
    HAS_ZARR = False


@pytest.fixture
def earth_science_temp_dir():
    """Create a temporary directory for Earth science test data."""
    with tempfile.TemporaryDirectory(prefix="tellus_es_test_") as temp_dir:
        yield Path(temp_dir)


@pytest.fixture
def sample_climate_netcdf_data():
    """Generate sample climate NetCDF data similar to model output."""
    if not HAS_NETCDF:
        pytest.skip("netCDF4 not available")
    
    # Create minimal climate model dimensions for fast testing
    time_steps = 12   # Monthly data for one year (faster)
    lat_points = 8    # Small grid for testing
    lon_points = 16   # Small grid for testing
    levels = 4        # Few levels for testing
    
    # Generate sample data
    np.random.seed(42)  # Reproducible data
    
    # Time dimension (days since reference)
    time = np.arange(time_steps, dtype=np.float64)
    
    # Spatial dimensions
    lat = np.linspace(-90, 90, lat_points, dtype=np.float32)
    lon = np.linspace(-180, 180, lon_points, dtype=np.float32)
    lev = np.logspace(np.log10(1000), np.log10(1), levels, dtype=np.float32)  # Pressure levels
    
    # Sample variables with realistic Earth science patterns
    temp_base = 273.15 + 15  # Global mean temperature
    temp_seasonal = 10 * np.sin(2 * np.pi * time / 365)  # Seasonal cycle
    temp_latitudinal = -30 * np.cos(np.pi * lat / 90)    # Latitudinal gradient
    
    # 4D temperature field with realistic structure
    temp_4d = np.zeros((time_steps, levels, lat_points, lon_points), dtype=np.float32)
    for t in range(time_steps):
        for k in range(levels):
            # Temperature decreases with height, varies with season and latitude
            temp_4d[t, k] = (temp_base + temp_seasonal[t] + 
                           temp_latitudinal[:, np.newaxis] * (1 - k/levels) +
                           np.random.normal(0, 2, (lat_points, lon_points)))
    
    # Surface pressure (2D)
    pres_surf = 101325 + 1000 * np.sin(np.pi * lat / 90)[:, np.newaxis] + \
                np.random.normal(0, 500, (lat_points, lon_points))
    pres_surf = np.broadcast_to(pres_surf, (time_steps, lat_points, lon_points)).astype(np.float32)
    
    # Precipitation (3D with time)
    precip = np.abs(np.random.normal(2, 1, (time_steps, lat_points, lon_points))).astype(np.float32)
    # Add tropical precipitation band
    tropical_mask = np.abs(lat) < 30
    precip[:, tropical_mask] *= 2
    
    return {
        'dimensions': {
            'time': time_steps,
            'lat': lat_points, 
            'lon': lon_points,
            'lev': levels
        },
        'coordinates': {
            'time': time,
            'lat': lat,
            'lon': lon,
            'lev': lev
        },
        'variables': {
            'temp': temp_4d,
            'pres_surf': pres_surf,
            'precip': precip
        },
        'attributes': {
            'title': 'Sample Earth System Model Output',
            'model': 'ECHAM6',
            'experiment': 'piControl',
            'institution': 'Max Planck Institute for Meteorology',
            'creation_date': '2024-01-01',
            'contact': 'test@example.com'
        }
    }


@pytest.fixture
def create_netcdf_file(earth_science_temp_dir, sample_climate_netcdf_data):
    """Create a realistic NetCDF file from sample climate data."""
    if not HAS_NETCDF:
        pytest.skip("netCDF4 not available")
    
    def _create_netcdf(filename: str, data: Optional[Dict] = None) -> Path:
        if data is None:
            data = sample_climate_netcdf_data
            
        file_path = earth_science_temp_dir / filename
        
        with nc.Dataset(file_path, 'w', format='NETCDF4') as ds:
            # Create dimensions
            for dim_name, dim_size in data['dimensions'].items():
                ds.createDimension(dim_name, dim_size)
            
            # Create coordinate variables
            for coord_name, coord_data in data['coordinates'].items():
                var = ds.createVariable(coord_name, coord_data.dtype, (coord_name,))
                var[:] = coord_data
                
                # Add coordinate attributes
                if coord_name == 'time':
                    var.units = 'days since 1850-01-01'
                    var.calendar = 'gregorian'
                elif coord_name == 'lat':
                    var.units = 'degrees_north'
                    var.long_name = 'latitude'
                elif coord_name == 'lon':
                    var.units = 'degrees_east'
                    var.long_name = 'longitude'
                elif coord_name == 'lev':
                    var.units = 'hPa'
                    var.long_name = 'pressure level'
                    var.positive = 'down'
            
            # Create data variables
            for var_name, var_data in data['variables'].items():
                if var_name == 'temp':
                    var = ds.createVariable(var_name, 'f4', ('time', 'lev', 'lat', 'lon'),
                                          chunksizes=(30, 10, 24, 48), zlib=True, complevel=6)
                    var.units = 'K'
                    var.long_name = 'temperature'
                    var.standard_name = 'air_temperature'
                elif var_name == 'pres_surf':
                    var = ds.createVariable(var_name, 'f4', ('time', 'lat', 'lon'),
                                          chunksizes=(50, 48, 96), zlib=True, complevel=6)
                    var.units = 'Pa'
                    var.long_name = 'surface pressure'
                    var.standard_name = 'surface_air_pressure'
                elif var_name == 'precip':
                    var = ds.createVariable(var_name, 'f4', ('time', 'lat', 'lon'),
                                          chunksizes=(50, 48, 96), zlib=True, complevel=6)
                    var.units = 'mm/day'
                    var.long_name = 'precipitation'
                    var.standard_name = 'precipitation_flux'
                
                var[:] = var_data
            
            # Add global attributes
            for attr_name, attr_value in data['attributes'].items():
                setattr(ds, attr_name, attr_value)
        
        return file_path
    
    return _create_netcdf


@pytest.fixture
def sample_model_archive_structure():
    """Create a realistic Earth system model archive structure."""
    return {
        'model_output': {
            'atm': [
                'temp_daily_2020.nc',
                'temp_daily_2021.nc', 
                'precip_daily_2020.nc',
                'precip_daily_2021.nc',
                'wind_daily_2020.nc',
                'wind_daily_2021.nc'
            ],
            'ocn': [
                'sst_monthly_2020.nc',
                'sst_monthly_2021.nc',
                'salt_monthly_2020.nc', 
                'salt_monthly_2021.nc',
                'currents_monthly_2020.nc',
                'currents_monthly_2021.nc'
            ],
            'lnd': [
                'soil_temp_daily_2020.nc',
                'soil_temp_daily_2021.nc',
                'vegetation_monthly_2020.nc',
                'vegetation_monthly_2021.nc'
            ],
            'ice': [
                'sea_ice_daily_2020.nc',
                'sea_ice_daily_2021.nc'
            ]
        },
        'namelists': {
            'atm': ['namelist.atm'],
            'ocn': ['namelist.ocn'], 
            'lnd': ['namelist.lnd'],
            'ice': ['namelist.ice'],
            'coupler': ['namelist.cpl']
        },
        'scripts': [
            'run_experiment.sh',
            'postprocess.py',
            'analysis_workflow.py'
        ],
        'logs': [
            'model.log',
            'atm.log',
            'ocn.log',
            'timing.log'
        ],
        'restart': {
            '20200101': [
                'restart_atm_20200101.nc',
                'restart_ocn_20200101.nc',
                'restart_lnd_20200101.nc',
                'restart_ice_20200101.nc'
            ],
            '20210101': [
                'restart_atm_20210101.nc',
                'restart_ocn_20210101.nc', 
                'restart_lnd_20210101.nc',
                'restart_ice_20210101.nc'
            ]
        }
    }


@pytest.fixture
def create_model_archive(earth_science_temp_dir, sample_model_archive_structure, create_netcdf_file):
    """Create a realistic model archive directory structure with files."""
    
    def _create_archive(archive_name: str = "model_experiment_001") -> Path:
        archive_path = earth_science_temp_dir / archive_name
        archive_path.mkdir(exist_ok=True)
        
        structure = sample_model_archive_structure
        
        # Create model output directories and files
        for component, files in structure['model_output'].items():
            comp_dir = archive_path / 'model_output' / component
            comp_dir.mkdir(parents=True, exist_ok=True)
            
            for filename in files:
                if filename.endswith('.nc'):
                    # Create empty placeholder NetCDF files for integration tests (faster)
                    (comp_dir / filename).touch()
                else:
                    # Create empty placeholder files
                    (comp_dir / filename).touch()
        
        # Create namelist files
        for component, files in structure['namelists'].items():
            comp_dir = archive_path / 'namelists' / component
            comp_dir.mkdir(parents=True, exist_ok=True)
            
            for filename in files:
                namelist_file = comp_dir / filename
                # Create sample namelist content
                namelist_content = f"""! Sample namelist for {component}
&{component}_nml
  dt = 1800
  nsteps = 48
  output_freq = 86400
/
"""
                namelist_file.write_text(namelist_content)
        
        # Create script files
        scripts_dir = archive_path / 'scripts'
        scripts_dir.mkdir(exist_ok=True)
        for filename in structure['scripts']:
            script_file = scripts_dir / filename
            if filename.endswith('.sh'):
                script_file.write_text("#!/bin/bash\necho 'Sample script'\n")
                script_file.chmod(0o755)
            elif filename.endswith('.py'):
                script_file.write_text("#!/usr/bin/env python3\nprint('Sample Python script')\n")
        
        # Create log files
        logs_dir = archive_path / 'logs'
        logs_dir.mkdir(exist_ok=True)
        for filename in structure['logs']:
            log_file = logs_dir / filename
            log_content = f"""Sample log file: {filename}
Simulation started: 2020-01-01 00:00:00
Model components initialized successfully
Timestep: 0001 completed
Timestep: 0002 completed
...
Simulation completed: 2021-12-31 23:59:59
"""
            log_file.write_text(log_content)
        
        # Create restart files
        for date, files in structure['restart'].items():
            restart_dir = archive_path / 'restart' / date
            restart_dir.mkdir(parents=True, exist_ok=True)
            
            for filename in files:
                if filename.endswith('.nc'):
                    # Create empty placeholder files for integration tests
                    (restart_dir / filename).touch()
        
        return archive_path
    
    return _create_archive


@pytest.fixture 
def create_compressed_archive(create_model_archive):
    """Create a compressed tar.gz archive from model output."""
    
    def _create_tar_archive(archive_name: str = "model_experiment_001", 
                           compression: str = "gz") -> Path:
        # Create the model archive first
        archive_dir = create_model_archive(archive_name)
        
        # Create compressed archive
        archive_file = archive_dir.parent / f"{archive_name}.tar.{compression}"
        
        with tarfile.open(archive_file, f"w:{compression}") as tar:
            tar.add(archive_dir, arcname=archive_name)
        
        return archive_file
    
    return _create_tar_archive


@pytest.fixture
def multi_location_setup(earth_science_temp_dir):
    """Create a multi-location setup typical for Earth science research."""
    
    # Create different "locations" as subdirectories
    locations = {
        'hpc_scratch': earth_science_temp_dir / 'hpc_scratch',
        'hpc_work': earth_science_temp_dir / 'hpc_work', 
        'archive_tape': earth_science_temp_dir / 'archive_tape',
        'local_cache': earth_science_temp_dir / 'local_cache',
        'cloud_storage': earth_science_temp_dir / 'cloud_storage'
    }
    
    for name, path in locations.items():
        path.mkdir(exist_ok=True)
    
    # Create sample data in different locations
    # HPC scratch - active computation area
    (locations['hpc_scratch'] / 'current_run').mkdir()
    (locations['hpc_scratch'] / 'current_run' / 'model_output.nc').touch()
    
    # HPC work - longer term storage
    (locations['hpc_work'] / 'experiments').mkdir()
    (locations['hpc_work'] / 'experiments' / 'exp001.tar.gz').touch()
    
    # Archive tape - long term storage  
    (locations['archive_tape'] / 'archived_experiments').mkdir()
    (locations['archive_tape'] / 'archived_experiments' / 'historical_runs.tar.gz').touch()
    
    # Local cache - frequently accessed data
    (locations['local_cache'] / 'frequently_used').mkdir()
    (locations['local_cache'] / 'frequently_used' / 'analysis_data.nc').touch()
    
    # Cloud storage - collaborative sharing
    (locations['cloud_storage'] / 'shared_datasets').mkdir()
    (locations['cloud_storage'] / 'shared_datasets' / 'reference_data.nc').touch()
    
    return locations


@pytest.fixture
def earth_science_file_patterns():
    """Common file patterns in Earth science archives."""
    return {
        'netcdf_files': ['*.nc', '*.nc4', '*.netcdf'],
        'archive_files': ['*.tar.gz', '*.tgz', '*.tar.bz2', '*.zip'],
        'restart_files': ['restart_*.nc', 'rpointer.*', '*.rs', '*.res'],
        'namelist_files': ['namelist.*', '*.nml', 'input.nml'],
        'log_files': ['*.log', '*.out', '*.err', 'timing.*'],
        'script_files': ['*.sh', '*.py', '*.R', '*.ncl', '*.csh'],
        'documentation': ['README*', '*.md', '*.txt', '*.pdf'],
        'metadata': ['*.json', '*.yaml', '*.yml', '*.xml'],
        'model_output': [
            # Atmospheric variables
            'temp_*.nc', 'precip_*.nc', 'wind_*.nc', 'pres_*.nc',
            # Ocean variables  
            'sst_*.nc', 'salt_*.nc', 'currents_*.nc', 'ssh_*.nc',
            # Land variables
            'soil_*.nc', 'vegetation_*.nc', 'runoff_*.nc',
            # Ice variables
            'sea_ice_*.nc', 'ice_thick_*.nc'
        ]
    }


@pytest.fixture
def realistic_file_sizes():
    """Realistic file sizes for different types of Earth science data."""
    return {
        'daily_global_2d': 50 * 1024**2,      # 50 MB - daily global 2D field
        'daily_global_3d': 500 * 1024**2,     # 500 MB - daily global 3D field  
        'monthly_global_2d': 200 * 1024**2,   # 200 MB - monthly global 2D
        'monthly_global_3d': 2 * 1024**3,     # 2 GB - monthly global 3D
        'yearly_timeseries': 10 * 1024**3,    # 10 GB - yearly high-res timeseries
        'restart_file': 1 * 1024**3,          # 1 GB - model restart file
        'full_experiment': 100 * 1024**3,     # 100 GB - complete experiment
        'namelist': 10 * 1024,                # 10 KB - namelist file
        'log_file': 1 * 1024**2,              # 1 MB - log file
        'script': 5 * 1024,                   # 5 KB - script file
    }


@pytest.fixture
def hpc_environment_config():
    """Configuration for typical HPC environment testing."""
    return {
        'filesystems': {
            'scratch': {
                'path': '/scratch/user', 
                'quota': 100 * 1024**3,  # 100 TB
                'purge_days': 30,
                'high_performance': True
            },
            'work': {
                'path': '/work/user',
                'quota': 10 * 1024**3,   # 10 TB  
                'backup': True,
                'long_term': True
            },
            'home': {
                'path': '/home/user',
                'quota': 100 * 1024**2,  # 100 GB
                'backup': True,
                'small_files': True
            }
        },
        'archive_systems': {
            'tape': {
                'hierarchical': True,
                'latency': 'high',
                'capacity': 'unlimited',
                'cost': 'low'
            },
            'disk_cache': {
                'hierarchical': False,
                'latency': 'low', 
                'capacity': 'limited',
                'cost': 'medium'
            }
        },
        'compute_nodes': {
            'login': {'cores': 2, 'memory': 8},
            'compute': {'cores': 48, 'memory': 192},
            'gpu': {'cores': 24, 'memory': 384, 'gpus': 4}
        }
    }