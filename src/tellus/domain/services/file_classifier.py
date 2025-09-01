"""
File classification service for Earth System Model simulations.

This service automatically classifies simulation files based on patterns,
content analysis, and Earth science domain knowledge.
"""

import re
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

from ..entities.simulation_file import (FileContentType, FileImportance,
                                        FilePattern, SimulationFile)


class FileClassifier:
    """
    Domain service for classifying simulation files based on Earth science patterns.
    
    This classifier uses domain knowledge about Earth System Models to automatically
    determine content types, importance levels, and roles for simulation files.
    """
    
    def __init__(self):
        """Initialize the classifier with Earth science file patterns."""
        self._patterns = self._build_classification_patterns()
        self._model_specific_patterns = self._build_model_specific_patterns()
    
    def classify_file(self, file_path: str, simulation_context: Optional[Dict] = None) -> Tuple[FileContentType, FileImportance, Optional[str]]:
        """
        Classify a file and return its content type, importance, and role.
        
        Args:
            file_path: Relative path of the file within simulation
            simulation_context: Optional context about the simulation (model type, etc.)
            
        Returns:
            Tuple of (content_type, importance, file_role)
        """
        path = Path(file_path)
        filename = path.name.lower()
        directory = str(path.parent).lower()
        extension = path.suffix.lower()
        
        # Check model-specific patterns first
        if simulation_context:
            model_type = simulation_context.get('model_id', '').lower()
            if model_type in self._model_specific_patterns:
                result = self._match_patterns(file_path, self._model_specific_patterns[model_type])
                if result:
                    return result
        
        # Check general patterns
        result = self._match_patterns(file_path, self._patterns)
        if result:
            return result
        
        # Fallback classification based on location and extension
        return self._fallback_classification(file_path, directory, extension)
    
    def classify_files(self, file_paths: List[str], simulation_context: Optional[Dict] = None) -> Dict[str, Tuple[FileContentType, FileImportance, Optional[str]]]:
        """Classify multiple files at once."""
        results = {}
        for file_path in file_paths:
            results[file_path] = self.classify_file(file_path, simulation_context)
        return results
    
    def create_simulation_file(self, file_path: str, simulation_context: Optional[Dict] = None, **kwargs) -> SimulationFile:
        """
        Create a SimulationFile entity with automatic classification.
        
        Args:
            file_path: Relative path of the file within simulation
            simulation_context: Optional simulation context for classification
            **kwargs: Additional SimulationFile parameters
            
        Returns:
            Classified SimulationFile entity
        """
        content_type, importance, file_role = self.classify_file(file_path, simulation_context)
        
        # Merge with any explicitly provided values
        final_content_type = kwargs.pop('content_type', content_type)
        final_importance = kwargs.pop('importance', importance)
        final_file_role = kwargs.pop('file_role', file_role)
        
        # Add automatic tags based on classification
        tags = set(kwargs.pop('tags', set()))
        tags.update(self._generate_automatic_tags(file_path, final_content_type, final_importance))
        
        return SimulationFile(
            relative_path=file_path,
            content_type=final_content_type,
            importance=final_importance,
            file_role=final_file_role,
            tags=tags,
            **kwargs
        )
    
    def _match_patterns(self, file_path: str, patterns: List[FilePattern]) -> Optional[Tuple[FileContentType, FileImportance, Optional[str]]]:
        """Match file path against a list of patterns."""
        from fnmatch import fnmatch
        
        for pattern in patterns:
            if fnmatch(file_path.lower(), pattern.glob_pattern.lower()):
                role = self._extract_file_role(file_path, pattern)
                return (pattern.content_type, pattern.importance, role)
        return None
    
    def _extract_file_role(self, file_path: str, pattern: FilePattern) -> Optional[str]:
        """Extract specific file role from path based on pattern context."""
        filename = Path(file_path).stem.lower()
        
        # Common role patterns in Earth science
        role_patterns = {
            'parameters': r'param|config|setting|namelist',
            'restart': r'restart|checkpoint|resume|init',
            'diagnostic': r'diag|analysis|stat|summary',
            'log': r'log|output|stdout|stderr|debug',
            'metadata': r'meta|catalog|index|manifest',
            'forcing': r'forcing|boundary|driver|input',
            'output': r'output|result|data'
        }
        
        for role, regex_pattern in role_patterns.items():
            if re.search(regex_pattern, filename):
                return role
        
        return None
    
    def _build_classification_patterns(self) -> List[FilePattern]:
        """Build general file classification patterns for Earth science simulations."""
        return [
            # Configuration and Input Files
            FilePattern("*.nml", FileContentType.INPUT, FileImportance.CRITICAL, "Namelist configuration files"),
            FilePattern("*namelist*", FileContentType.INPUT, FileImportance.CRITICAL, "Namelist configuration"),
            FilePattern("*.cfg", FileContentType.CONFIG, FileImportance.IMPORTANT, "Configuration files"),
            FilePattern("*.conf", FileContentType.CONFIG, FileImportance.IMPORTANT, "Configuration files"),
            FilePattern("*.xml", FileContentType.CONFIG, FileImportance.IMPORTANT, "XML configuration"),
            FilePattern("*.yaml", FileContentType.CONFIG, FileImportance.IMPORTANT, "YAML configuration"),
            FilePattern("*.yml", FileContentType.CONFIG, FileImportance.IMPORTANT, "YAML configuration"),
            FilePattern("*.json", FileContentType.CONFIG, FileImportance.IMPORTANT, "JSON configuration"),
            
            # NetCDF Data Files (primary Earth science format)
            FilePattern("*.nc", FileContentType.OUTPUT, FileImportance.IMPORTANT, "NetCDF data files"),
            FilePattern("*.nc4", FileContentType.OUTPUT, FileImportance.IMPORTANT, "NetCDF4 data files"),
            FilePattern("*.cdf", FileContentType.OUTPUT, FileImportance.IMPORTANT, "NetCDF data files"),
            
            # Other Data Formats
            FilePattern("*.hdf", FileContentType.OUTPUT, FileImportance.IMPORTANT, "HDF data files"),
            FilePattern("*.hdf5", FileContentType.OUTPUT, FileImportance.IMPORTANT, "HDF5 data files"),
            FilePattern("*.h5", FileContentType.OUTPUT, FileImportance.IMPORTANT, "HDF5 data files"),
            FilePattern("*.grib", FileContentType.OUTPUT, FileImportance.IMPORTANT, "GRIB meteorological data"),
            FilePattern("*.grib2", FileContentType.OUTPUT, FileImportance.IMPORTANT, "GRIB2 meteorological data"),
            FilePattern("*.grb", FileContentType.OUTPUT, FileImportance.IMPORTANT, "GRIB meteorological data"),
            
            # Log and Output Files
            FilePattern("*.log", FileContentType.LOG, FileImportance.OPTIONAL, "Log files"),
            FilePattern("*.out", FileContentType.LOG, FileImportance.OPTIONAL, "Output logs"),
            FilePattern("*.err", FileContentType.LOG, FileImportance.OPTIONAL, "Error logs"),
            FilePattern("stdout*", FileContentType.LOG, FileImportance.OPTIONAL, "Standard output"),
            FilePattern("stderr*", FileContentType.LOG, FileImportance.OPTIONAL, "Standard error"),
            FilePattern("slurm-*.out", FileContentType.LOG, FileImportance.OPTIONAL, "SLURM job output"),
            
            # Restart and Checkpoint Files
            FilePattern("*restart*", FileContentType.INTERMEDIATE, FileImportance.CRITICAL, "Restart files"),
            FilePattern("*checkpoint*", FileContentType.INTERMEDIATE, FileImportance.CRITICAL, "Checkpoint files"),
            FilePattern("*.res", FileContentType.INTERMEDIATE, FileImportance.CRITICAL, "Restart files"),
            FilePattern("*.rst", FileContentType.INTERMEDIATE, FileImportance.CRITICAL, "Restart files"),
            
            # Analysis and Diagnostic Files
            FilePattern("*diag*", FileContentType.DIAGNOSTIC, FileImportance.IMPORTANT, "Diagnostic output"),
            FilePattern("*analysis*", FileContentType.DIAGNOSTIC, FileImportance.IMPORTANT, "Analysis output"),
            FilePattern("*stat*", FileContentType.DIAGNOSTIC, FileImportance.IMPORTANT, "Statistical output"),
            FilePattern("*summary*", FileContentType.DIAGNOSTIC, FileImportance.IMPORTANT, "Summary output"),
            
            # Input Data and Forcing
            FilePattern("input/*", FileContentType.INPUT, FileImportance.IMPORTANT, "Input data files"),
            FilePattern("forcing/*", FileContentType.INPUT, FileImportance.IMPORTANT, "Forcing data files"),
            FilePattern("boundary/*", FileContentType.INPUT, FileImportance.IMPORTANT, "Boundary condition files"),
            FilePattern("initial/*", FileContentType.INPUT, FileImportance.IMPORTANT, "Initial condition files"),
            
            # Scripts and Code
            FilePattern("*.sh", FileContentType.CONFIG, FileImportance.IMPORTANT, "Shell scripts"),
            FilePattern("*.py", FileContentType.CONFIG, FileImportance.IMPORTANT, "Python scripts"),
            FilePattern("*.r", FileContentType.CONFIG, FileImportance.IMPORTANT, "R scripts"),
            FilePattern("*.ncl", FileContentType.CONFIG, FileImportance.IMPORTANT, "NCL scripts"),
            FilePattern("*.m", FileContentType.CONFIG, FileImportance.IMPORTANT, "MATLAB scripts"),
            
            # Metadata and Documentation
            FilePattern("*.txt", FileContentType.METADATA, FileImportance.OPTIONAL, "Text documentation"),
            FilePattern("*.md", FileContentType.METADATA, FileImportance.OPTIONAL, "Markdown documentation"),
            FilePattern("README*", FileContentType.METADATA, FileImportance.IMPORTANT, "README files"),
            FilePattern("*metadata*", FileContentType.METADATA, FileImportance.IMPORTANT, "Metadata files"),
            FilePattern("*catalog*", FileContentType.METADATA, FileImportance.IMPORTANT, "Data catalog files"),
            
            # Temporary and System Files
            FilePattern("*.tmp", FileContentType.INTERMEDIATE, FileImportance.TEMPORARY, "Temporary files"),
            FilePattern("*.temp", FileContentType.INTERMEDIATE, FileImportance.TEMPORARY, "Temporary files"),
            FilePattern("*.swp", FileContentType.INTERMEDIATE, FileImportance.TEMPORARY, "Vim swap files"),
            FilePattern("*.bak", FileContentType.INTERMEDIATE, FileImportance.TEMPORARY, "Backup files"),
            FilePattern(".*", FileContentType.INTERMEDIATE, FileImportance.TEMPORARY, "Hidden system files"),
            FilePattern("core.*", FileContentType.INTERMEDIATE, FileImportance.TEMPORARY, "Core dump files"),
            
            # Archive Files
            FilePattern("*.tar", FileContentType.INTERMEDIATE, FileImportance.OPTIONAL, "Archive files"),
            FilePattern("*.gz", FileContentType.INTERMEDIATE, FileImportance.OPTIONAL, "Compressed files"),
            FilePattern("*.zip", FileContentType.INTERMEDIATE, FileImportance.OPTIONAL, "ZIP archives"),
            FilePattern("*.tar.gz", FileContentType.INTERMEDIATE, FileImportance.OPTIONAL, "Compressed archives"),
            FilePattern("*.tgz", FileContentType.INTERMEDIATE, FileImportance.OPTIONAL, "Compressed archives"),
        ]
    
    def _build_model_specific_patterns(self) -> Dict[str, List[FilePattern]]:
        """Build model-specific classification patterns."""
        return {
            'cesm': [
                FilePattern("*cam*", FileContentType.OUTPUT, FileImportance.IMPORTANT, "CAM atmospheric model output"),
                FilePattern("*clm*", FileContentType.OUTPUT, FileImportance.IMPORTANT, "CLM land model output"),
                FilePattern("*pop*", FileContentType.OUTPUT, FileImportance.IMPORTANT, "POP ocean model output"),
                FilePattern("*cice*", FileContentType.OUTPUT, FileImportance.IMPORTANT, "CICE sea ice model output"),
                FilePattern("user_nl_*", FileContentType.INPUT, FileImportance.CRITICAL, "User namelist files"),
                FilePattern("env_*.xml", FileContentType.CONFIG, FileImportance.CRITICAL, "Environment configuration"),
            ],
            'echam': [
                FilePattern("*BOT*", FileContentType.OUTPUT, FileImportance.IMPORTANT, "ECHAM surface output"),
                FilePattern("*ATM*", FileContentType.OUTPUT, FileImportance.IMPORTANT, "ECHAM atmosphere output"),
                FilePattern("*LOG*", FileContentType.LOG, FileImportance.OPTIONAL, "ECHAM log output"),
                FilePattern("namelist.echam", FileContentType.INPUT, FileImportance.CRITICAL, "ECHAM namelist"),
            ],
            'icon': [
                FilePattern("*atm_*", FileContentType.OUTPUT, FileImportance.IMPORTANT, "ICON atmosphere output"),
                FilePattern("*oce_*", FileContentType.OUTPUT, FileImportance.IMPORTANT, "ICON ocean output"),
                FilePattern("*lnd_*", FileContentType.OUTPUT, FileImportance.IMPORTANT, "ICON land output"),
                FilePattern("icon_master.namelist", FileContentType.INPUT, FileImportance.CRITICAL, "ICON master namelist"),
            ],
            'wrf': [
                FilePattern("wrfout_*", FileContentType.OUTPUT, FileImportance.IMPORTANT, "WRF model output"),
                FilePattern("wrfrst_*", FileContentType.INTERMEDIATE, FileImportance.CRITICAL, "WRF restart files"),
                FilePattern("wrfbdy_*", FileContentType.INPUT, FileImportance.IMPORTANT, "WRF boundary files"),
                FilePattern("namelist.input", FileContentType.INPUT, FileImportance.CRITICAL, "WRF namelist"),
            ],
            'fesom': [
                FilePattern("*.fesom.*", FileContentType.OUTPUT, FileImportance.IMPORTANT, "FESOM ocean model output"),
                FilePattern("namelist.config", FileContentType.INPUT, FileImportance.CRITICAL, "FESOM configuration"),
                FilePattern("forcing/*", FileContentType.INPUT, FileImportance.IMPORTANT, "FESOM forcing data"),
            ]
        }
    
    def _fallback_classification(self, file_path: str, directory: str, extension: str) -> Tuple[FileContentType, FileImportance, Optional[str]]:
        """Provide fallback classification when no patterns match."""
        path = Path(file_path)
        
        # Classification by directory structure
        if any(dir_part in directory for dir_part in ['input', 'forcing', 'boundary']):
            return (FileContentType.INPUT, FileImportance.IMPORTANT, 'input_data')
        
        if any(dir_part in directory for dir_part in ['output', 'results', 'data']):
            return (FileContentType.OUTPUT, FileImportance.IMPORTANT, 'model_output')
        
        if any(dir_part in directory for dir_part in ['log', 'logs']):
            return (FileContentType.LOG, FileImportance.OPTIONAL, 'general_log')
        
        if any(dir_part in directory for dir_part in ['config', 'setup', 'run']):
            return (FileContentType.CONFIG, FileImportance.IMPORTANT, 'configuration')
        
        if any(dir_part in directory for dir_part in ['restart', 'checkpoint']):
            return (FileContentType.INTERMEDIATE, FileImportance.CRITICAL, 'restart_data')
        
        # Classification by file extension
        if extension in ['.dat', '.txt', '.csv']:
            return (FileContentType.OUTPUT, FileImportance.OPTIONAL, 'text_data')
        
        if extension in ['.png', '.jpg', '.jpeg', '.pdf', '.eps']:
            return (FileContentType.DIAGNOSTIC, FileImportance.OPTIONAL, 'visualization')
        
        if extension in ['.exe', '.x']:
            return (FileContentType.CONFIG, FileImportance.IMPORTANT, 'executable')
        
        # Default fallback
        return (FileContentType.OUTPUT, FileImportance.OPTIONAL, 'unknown')
    
    def _generate_automatic_tags(self, file_path: str, content_type: FileContentType, importance: FileImportance) -> Set[str]:
        """Generate automatic tags based on file classification."""
        tags = set()
        
        # Add content type tag
        tags.add(content_type.value)
        
        # Add importance tag
        tags.add(importance.value)
        
        # Add file extension tag
        extension = Path(file_path).suffix.lstrip('.')
        if extension:
            tags.add(f"ext_{extension}")
        
        # Add directory-based tags
        path_parts = Path(file_path).parts
        for part in path_parts[:-1]:  # Exclude filename
            if part and not part.startswith('.'):
                tags.add(f"dir_{part.lower()}")
        
        # Add temporal tags if filename suggests temporal data
        filename = Path(file_path).stem.lower()
        temporal_patterns = {
            r'\b(\d{4})\b': 'year',
            r'\b(\d{4})-(\d{2})\b': 'monthly',
            r'\b(\d{4})-(\d{2})-(\d{2})\b': 'daily',
            r'\b(\d{8})\b': 'daily',
            r'\b(\d{10})\b': 'hourly'
        }
        
        for pattern, tag in temporal_patterns.items():
            if re.search(pattern, filename):
                tags.add(tag)
                break
        
        return tags
    
    def get_classification_summary(self, files: List[str], simulation_context: Optional[Dict] = None) -> Dict[str, Dict]:
        """Get a summary of file classification results."""
        classifications = self.classify_files(files, simulation_context)
        
        content_type_counts = {}
        importance_counts = {}
        role_counts = {}
        
        for file_path, (content_type, importance, role) in classifications.items():
            # Count content types
            ct_name = content_type.value
            content_type_counts[ct_name] = content_type_counts.get(ct_name, 0) + 1
            
            # Count importance levels
            imp_name = importance.value
            importance_counts[imp_name] = importance_counts.get(imp_name, 0) + 1
            
            # Count roles
            if role:
                role_counts[role] = role_counts.get(role, 0) + 1
        
        return {
            'total_files': len(files),
            'content_types': content_type_counts,
            'importance_levels': importance_counts,
            'file_roles': role_counts
        }