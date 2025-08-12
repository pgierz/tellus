"""Test fixtures for tellus Earth science workflows."""

from .earth_science import *

__all__ = [
    "earth_science_temp_dir",
    "sample_climate_netcdf_data",
    "create_netcdf_file", 
    "sample_model_archive_structure",
    "create_model_archive",
    "create_compressed_archive",
    "multi_location_setup",
    "earth_science_file_patterns",
    "realistic_file_sizes",
    "hpc_environment_config"
]