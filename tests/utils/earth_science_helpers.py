"""
Earth science domain-specific test utilities and helpers.

This module provides utilities specifically designed for testing Earth science
workflows, including data validation, file format checking, and domain-specific
assertions.
"""

import re
import tarfile
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

try:
    import netCDF4 as nc
    import numpy as np
    HAS_NETCDF = True
except ImportError:
    HAS_NETCDF = False

try:
    import xarray as xr
    HAS_XARRAY = True
except ImportError:
    HAS_XARRAY = False


class DataFrequency(Enum):
    """Earth science data frequency types."""
    HOURLY = "hourly"
    THREE_HOURLY = "3hourly"
    SIX_HOURLY = "6hourly"
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"
    SEASONAL = "seasonal"
    ANNUAL = "annual"
    DECADAL = "decadal"


class ModelRealm(Enum):
    """Earth system model realms."""
    ATMOSPHERE = "atm"
    OCEAN = "ocn"
    LAND = "lnd"
    SEA_ICE = "ice"
    LAND_ICE = "landice"
    COUPLER = "cpl"
    BIOGEOCHEMISTRY = "bgc"


@dataclass
class EarthScienceFileInfo:
    """Information about an Earth science data file."""
    filename: str
    variable: Optional[str] = None
    realm: Optional[ModelRealm] = None
    frequency: Optional[DataFrequency] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    model: Optional[str] = None
    experiment: Optional[str] = None
    ensemble_member: Optional[str] = None
    version: Optional[str] = None
    file_type: Optional[str] = None


class EarthScienceFileValidator:
    """Validator for Earth science file naming conventions and content."""
    
    # Standard Earth science file patterns
    NETCDF_PATTERN = re.compile(r'.*\.nc[4]?$', re.IGNORECASE)
    RESTART_PATTERN = re.compile(r'.*restart.*\.nc$', re.IGNORECASE)
    NAMELIST_PATTERN = re.compile(r'namelist\.[a-zA-Z]+$|.*\.nml$', re.IGNORECASE)
    LOG_PATTERN = re.compile(r'.*\.log$|.*\.out$|.*\.err$', re.IGNORECASE)
    SCRIPT_PATTERN = re.compile(r'.*\.(sh|py|R|ncl|csh)$')
    
    # CMIP6 filename pattern
    CMIP6_PATTERN = re.compile(
        r'(?P<variable>[a-zA-Z0-9]+)_'
        r'(?P<table>[a-zA-Z0-9]+)_'
        r'(?P<model>[a-zA-Z0-9\-]+)_'
        r'(?P<experiment>[a-zA-Z0-9\-]+)_'
        r'(?P<ensemble>r\d+i\d+p\d+f\d+)_'
        r'(?P<grid>[a-zA-Z0-9\-]+)_'
        r'(?P<time_range>\d+-\d+)\.nc$'
    )
    
    # Generic Earth science filename pattern
    GENERIC_PATTERN = re.compile(
        r'(?P<variable>[a-zA-Z0-9_]+)_'
        r'(?P<frequency>hourly|3hourly|6hourly|daily|monthly|annual)_'
        r'(?P<date>\d{4}|\d{6}|\d{8}).*\.nc$'
    )
    
    @classmethod
    def is_netcdf_file(cls, filename: str) -> bool:
        """Check if file is a NetCDF file."""
        return bool(cls.NETCDF_PATTERN.match(filename))
    
    @classmethod
    def is_restart_file(cls, filename: str) -> bool:
        """Check if file is a model restart file."""
        return bool(cls.RESTART_PATTERN.match(filename))
    
    @classmethod
    def is_namelist_file(cls, filename: str) -> bool:
        """Check if file is a namelist file."""
        return bool(cls.NAMELIST_PATTERN.match(filename))
    
    @classmethod
    def is_log_file(cls, filename: str) -> bool:
        """Check if file is a log file."""
        return bool(cls.LOG_PATTERN.match(filename))
    
    @classmethod
    def is_script_file(cls, filename: str) -> bool:
        """Check if file is a script file."""
        return bool(cls.SCRIPT_PATTERN.match(filename))
    
    @classmethod
    def parse_cmip6_filename(cls, filename: str) -> Optional[EarthScienceFileInfo]:
        """Parse CMIP6 filename convention."""
        match = cls.CMIP6_PATTERN.match(filename)
        if not match:
            return None
        
        groups = match.groupdict()
        time_parts = groups['time_range'].split('-')
        
        return EarthScienceFileInfo(
            filename=filename,
            variable=groups['variable'],
            model=groups['model'],
            experiment=groups['experiment'],
            ensemble_member=groups['ensemble'],
            start_date=time_parts[0] if len(time_parts) > 0 else None,
            end_date=time_parts[1] if len(time_parts) > 1 else None,
            file_type="netcdf"
        )
    
    @classmethod
    def parse_generic_filename(cls, filename: str) -> Optional[EarthScienceFileInfo]:
        """Parse generic Earth science filename."""
        match = cls.GENERIC_PATTERN.match(filename)
        if not match:
            return None
        
        groups = match.groupdict()
        
        # Map frequency string to enum
        freq_map = {
            'hourly': DataFrequency.HOURLY,
            '3hourly': DataFrequency.THREE_HOURLY,
            '6hourly': DataFrequency.SIX_HOURLY,
            'daily': DataFrequency.DAILY,
            'monthly': DataFrequency.MONTHLY,
            'annual': DataFrequency.ANNUAL
        }
        
        return EarthScienceFileInfo(
            filename=filename,
            variable=groups['variable'],
            frequency=freq_map.get(groups['frequency']),
            start_date=groups['date'],
            file_type="netcdf"
        )
    
    @classmethod
    def validate_netcdf_structure(cls, filepath: Path) -> Dict[str, Any]:
        """Validate NetCDF file structure and return metadata."""
        if not HAS_NETCDF:
            return {"error": "netCDF4 not available"}
        
        try:
            with nc.Dataset(filepath, 'r') as ds:
                info = {
                    "dimensions": dict(ds.dimensions.items()),
                    "variables": list(ds.variables.keys()),
                    "global_attributes": {attr: getattr(ds, attr) for attr in ds.ncattrs()},
                    "data_variables": [],
                    "coordinate_variables": [],
                    "valid": True
                }
                
                # Categorize variables
                for var_name, var in ds.variables.items():
                    var_info = {
                        "name": var_name,
                        "dimensions": var.dimensions,
                        "dtype": str(var.dtype),
                        "shape": var.shape,
                        "attributes": {attr: getattr(var, attr) for attr in var.ncattrs()}
                    }
                    
                    # Check if it's a coordinate variable
                    if len(var.dimensions) == 1 and var.dimensions[0] == var_name:
                        info["coordinate_variables"].append(var_info)
                    else:
                        info["data_variables"].append(var_info)
                
                return info
                
        except Exception as e:
            return {"valid": False, "error": str(e)}


class EarthScienceArchiveAnalyzer:
    """Analyzer for Earth science archive contents."""
    
    def __init__(self, archive_path: Path):
        self.archive_path = archive_path
        self.validator = EarthScienceFileValidator()
    
    def analyze_archive_structure(self) -> Dict[str, Any]:
        """Analyze the structure of an Earth science archive."""
        if not self.archive_path.exists():
            return {"error": f"Archive not found: {self.archive_path}"}
        
        try:
            structure = {
                "total_files": 0,
                "file_types": {
                    "netcdf": [],
                    "restart": [],
                    "namelist": [],
                    "scripts": [],
                    "logs": [],
                    "other": []
                },
                "realms_detected": set(),
                "frequencies_detected": set(),
                "variables_detected": set(),
                "models_detected": set(),
                "experiments_detected": set(),
                "time_coverage": {"earliest": None, "latest": None},
                "directory_structure": {},
                "size_distribution": {}
            }
            
            with tarfile.open(self.archive_path, 'r:*') as tar:
                for member in tar.getmembers():
                    if member.isfile():
                        structure["total_files"] += 1
                        filename = Path(member.name).name
                        file_size = member.size
                        
                        # Categorize file type
                        if self.validator.is_netcdf_file(filename):
                            structure["file_types"]["netcdf"].append(filename)
                            
                            # Try to parse filename for metadata
                            file_info = (self.validator.parse_cmip6_filename(filename) or
                                       self.validator.parse_generic_filename(filename))
                            
                            if file_info:
                                if file_info.variable:
                                    structure["variables_detected"].add(file_info.variable)
                                if file_info.model:
                                    structure["models_detected"].add(file_info.model)
                                if file_info.experiment:
                                    structure["experiments_detected"].add(file_info.experiment)
                                if file_info.frequency:
                                    structure["frequencies_detected"].add(file_info.frequency.value)
                                
                                # Track time coverage
                                if file_info.start_date:
                                    if (structure["time_coverage"]["earliest"] is None or
                                        file_info.start_date < structure["time_coverage"]["earliest"]):
                                        structure["time_coverage"]["earliest"] = file_info.start_date
                                
                                if file_info.end_date:
                                    if (structure["time_coverage"]["latest"] is None or
                                        file_info.end_date > structure["time_coverage"]["latest"]):
                                        structure["time_coverage"]["latest"] = file_info.end_date
                            
                        elif self.validator.is_restart_file(filename):
                            structure["file_types"]["restart"].append(filename)
                        elif self.validator.is_namelist_file(filename):
                            structure["file_types"]["namelist"].append(filename)
                        elif self.validator.is_script_file(filename):
                            structure["file_types"]["scripts"].append(filename)
                        elif self.validator.is_log_file(filename):
                            structure["file_types"]["logs"].append(filename)
                        else:
                            structure["file_types"]["other"].append(filename)
                        
                        # Analyze directory structure
                        path_parts = Path(member.name).parts[:-1]  # Exclude filename
                        if path_parts:
                            current_level = structure["directory_structure"]
                            for part in path_parts:
                                if part not in current_level:
                                    current_level[part] = {}
                                current_level = current_level[part]
                        
                        # Size distribution
                        size_category = self._categorize_file_size(file_size)
                        if size_category not in structure["size_distribution"]:
                            structure["size_distribution"][size_category] = 0
                        structure["size_distribution"][size_category] += 1
                        
                        # Detect model realms from directory structure
                        for part in Path(member.name).parts:
                            if part.lower() in ['atm', 'atmosphere']:
                                structure["realms_detected"].add(ModelRealm.ATMOSPHERE.value)
                            elif part.lower() in ['ocn', 'ocean']:
                                structure["realms_detected"].add(ModelRealm.OCEAN.value)
                            elif part.lower() in ['lnd', 'land']:
                                structure["realms_detected"].add(ModelRealm.LAND.value)
                            elif part.lower() in ['ice', 'seaice']:
                                structure["realms_detected"].add(ModelRealm.SEA_ICE.value)
            
            # Convert sets to lists for JSON serialization
            structure["realms_detected"] = list(structure["realms_detected"])
            structure["frequencies_detected"] = list(structure["frequencies_detected"])
            structure["variables_detected"] = list(structure["variables_detected"])
            structure["models_detected"] = list(structure["models_detected"])
            structure["experiments_detected"] = list(structure["experiments_detected"])
            
            return structure
            
        except Exception as e:
            return {"error": f"Failed to analyze archive: {str(e)}"}
    
    def _categorize_file_size(self, size_bytes: int) -> str:
        """Categorize file size into ranges."""
        if size_bytes < 1024**2:  # < 1 MB
            return "small"
        elif size_bytes < 100 * 1024**2:  # < 100 MB
            return "medium"
        elif size_bytes < 1024**3:  # < 1 GB
            return "large"
        else:
            return "very_large"
    
    def validate_archive_completeness(self) -> Dict[str, Any]:
        """Validate that archive contains expected Earth science components."""
        structure = self.analyze_archive_structure()
        
        if "error" in structure:
            return structure
        
        validation = {
            "is_complete": True,
            "missing_components": [],
            "warnings": [],
            "recommendations": []
        }
        
        # Check for essential components
        if not structure["file_types"]["netcdf"]:
            validation["is_complete"] = False
            validation["missing_components"].append("No NetCDF data files found")
        
        if not structure["realms_detected"]:
            validation["warnings"].append("No model realms detected from directory structure")
        
        if not structure["variables_detected"]:
            validation["warnings"].append("No variables detected from filenames")
        
        # Check for documentation
        if (not structure["file_types"]["namelist"] and 
            not any("readme" in f.lower() for f in structure["file_types"]["other"])):
            validation["recommendations"].append("Consider adding namelists or documentation")
        
        # Check for restart files if this looks like a model run
        if (structure["file_types"]["netcdf"] and 
            not structure["file_types"]["restart"] and
            any("restart" not in f.lower() for f in structure["file_types"]["netcdf"])):
            validation["recommendations"].append("Consider including restart files for reproducibility")
        
        return validation


class EarthScienceTestAssertions:
    """Domain-specific assertions for Earth science testing."""
    
    @staticmethod
    def assert_valid_netcdf_file(filepath: Path, required_variables: Optional[List[str]] = None):
        """Assert that a file is a valid NetCDF file with expected structure."""
        assert filepath.exists(), f"NetCDF file not found: {filepath}"
        
        validator = EarthScienceFileValidator()
        assert validator.is_netcdf_file(str(filepath)), f"File is not a NetCDF file: {filepath}"
        
        if HAS_NETCDF:
            metadata = validator.validate_netcdf_structure(filepath)
            assert metadata.get("valid", False), f"Invalid NetCDF structure: {metadata.get('error', 'Unknown error')}"
            
            if required_variables:
                variables = metadata.get("variables", [])
                for var in required_variables:
                    assert var in variables, f"Required variable '{var}' not found in {filepath}"
    
    @staticmethod
    def assert_earth_science_archive_structure(archive_path: Path, 
                                             expected_realms: Optional[List[str]] = None,
                                             min_netcdf_files: int = 1):
        """Assert that archive has valid Earth science structure."""
        assert archive_path.exists(), f"Archive not found: {archive_path}"
        
        analyzer = EarthScienceArchiveAnalyzer(archive_path)
        structure = analyzer.analyze_archive_structure()
        
        assert "error" not in structure, f"Failed to analyze archive: {structure.get('error')}"
        
        # Check minimum NetCDF files
        netcdf_count = len(structure["file_types"]["netcdf"])
        assert netcdf_count >= min_netcdf_files, f"Expected at least {min_netcdf_files} NetCDF files, found {netcdf_count}"
        
        # Check expected realms if specified
        if expected_realms:
            detected_realms = set(structure["realms_detected"])
            expected_set = set(expected_realms)
            missing_realms = expected_set - detected_realms
            assert not missing_realms, f"Missing expected realms: {missing_realms}"
    
    @staticmethod
    def assert_filename_follows_convention(filename: str, convention: str = "generic"):
        """Assert that filename follows Earth science naming conventions."""
        validator = EarthScienceFileValidator()
        
        if convention == "cmip6":
            file_info = validator.parse_cmip6_filename(filename)
            assert file_info is not None, f"Filename '{filename}' does not follow CMIP6 convention"
        elif convention == "generic":
            file_info = validator.parse_generic_filename(filename)
            assert file_info is not None, f"Filename '{filename}' does not follow generic Earth science convention"
        else:
            raise ValueError(f"Unknown convention: {convention}")
    
    @staticmethod
    def assert_temporal_consistency(filenames: List[str], expected_frequency: Optional[str] = None):
        """Assert temporal consistency in a set of Earth science files."""
        validator = EarthScienceFileValidator()
        file_infos = []
        
        for filename in filenames:
            file_info = (validator.parse_cmip6_filename(filename) or
                        validator.parse_generic_filename(filename))
            if file_info:
                file_infos.append(file_info)
        
        assert file_infos, "No parseable Earth science files found"
        
        # Check frequency consistency
        if expected_frequency:
            for file_info in file_infos:
                if file_info.frequency:
                    assert file_info.frequency.value == expected_frequency, \
                        f"File {file_info.filename} has frequency {file_info.frequency.value}, expected {expected_frequency}"
        
        # Check for temporal gaps (basic check)
        dates = []
        for file_info in file_infos:
            if file_info.start_date:
                dates.append(file_info.start_date)
        
        if len(dates) > 1:
            dates.sort()
            # Basic check that we have a reasonable temporal sequence
            assert len(set(dates)) == len(dates), "Duplicate dates found in file sequence"
    
    @staticmethod
    def assert_archive_completeness(archive_path: Path, strict: bool = False):
        """Assert that archive contains a complete Earth science dataset."""
        analyzer = EarthScienceArchiveAnalyzer(archive_path)
        validation = analyzer.validate_archive_completeness()
        
        assert "error" not in validation, f"Archive validation failed: {validation.get('error')}"
        
        if strict:
            assert validation["is_complete"], f"Archive incomplete: {validation['missing_components']}"
            assert not validation["missing_components"], f"Missing components: {validation['missing_components']}"
        
        # Always check for critical issues
        critical_warnings = [w for w in validation.get("warnings", []) if "netcdf" in w.lower()]
        assert not critical_warnings, f"Critical issues found: {critical_warnings}"


# Convenience functions for common Earth science test patterns
def create_cmip6_filename(variable: str, table: str, model: str, experiment: str, 
                         ensemble: str = "r1i1p1f1", grid: str = "gn", 
                         start_year: int = 2000, end_year: int = 2009) -> str:
    """Create a CMIP6-compliant filename."""
    time_range = f"{start_year}01-{end_year}12"
    return f"{variable}_{table}_{model}_{experiment}_{ensemble}_{grid}_{time_range}.nc"


def create_generic_earth_science_filename(variable: str, frequency: str, 
                                        year: int, model: str = None) -> str:
    """Create a generic Earth science filename."""
    if model:
        return f"{variable}_{frequency}_{year}_{model}.nc"
    else:
        return f"{variable}_{frequency}_{year}.nc"


def extract_model_metadata_from_archive(archive_path: Path) -> Dict[str, Any]:
    """Extract model metadata from an Earth science archive."""
    analyzer = EarthScienceArchiveAnalyzer(archive_path)
    structure = analyzer.analyze_archive_structure()
    
    return {
        "models": structure.get("models_detected", []),
        "experiments": structure.get("experiments_detected", []),
        "variables": structure.get("variables_detected", []),
        "realms": structure.get("realms_detected", []),
        "frequencies": structure.get("frequencies_detected", []),
        "time_coverage": structure.get("time_coverage", {}),
        "total_files": structure.get("total_files", 0)
    }