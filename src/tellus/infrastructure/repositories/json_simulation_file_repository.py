"""
JSON-based implementation of the unified SimulationFile repository.

This implementation provides file and archive management through a single
JSON-based persistence layer, supporting the unified file architecture.
"""

import json
import logging
import os
import time
from fnmatch import fnmatch
from pathlib import Path
from typing import Dict, List, Optional, Set

from ...domain.entities.simulation_file import FileType, SimulationFile
from ...domain.repositories.exceptions import RepositoryError
from ...domain.repositories.simulation_file_repository import ISimulationFileRepository

logger = logging.getLogger(__name__)


class JsonSimulationFileRepository(ISimulationFileRepository):
    """
    JSON-based implementation of the unified SimulationFile repository.
    
    Stores files per-simulation in separate JSON files with pattern:
    simulation-files-{simulation_id}.json
    
    This provides better scaling and cleaner organization than a single large file.
    """
    
    def __init__(self, storage_dir: str = None, auto_create_dirs: bool = True):
        """
        Initialize the JSON repository.
        
        Args:
            storage_dir: Directory containing simulation file JSON files
            auto_create_dirs: Whether to auto-create parent directories
        """
        if storage_dir is None:
            # Default to project-local storage directory
            storage_dir = ".tellus"
        
        self.storage_dir = Path(storage_dir)
        self.auto_create_dirs = auto_create_dirs
        self._simulation_data: Dict[str, Dict[str, Dict]] = {}  # simulation_id -> file_path -> file_data
        self._last_modified: Dict[str, float] = {}  # simulation_id -> timestamp
        
        logger.debug(f"Initialized JSON SimulationFile repository in: {self.storage_dir}")
        
        if self.auto_create_dirs:
            self.storage_dir.mkdir(parents=True, exist_ok=True)
    
    def _get_simulation_file_path(self, simulation_id: str) -> Path:
        """Get the file path for a specific simulation's files."""
        return self.storage_dir / f"simulation-files-{simulation_id}.json"
    
    def _load_simulation_data(self, simulation_id: str) -> None:
        """Load data for a specific simulation if it exists."""
        if simulation_id in self._simulation_data:
            # Check if we need to reload
            file_path = self._get_simulation_file_path(simulation_id)
            if file_path.exists():
                stat = file_path.stat()
                last_modified = self._last_modified.get(simulation_id, 0)
                if stat.st_mtime <= last_modified:
                    return  # Data is up to date
        
        file_path = self._get_simulation_file_path(simulation_id)
        try:
            if file_path.exists():
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                self._simulation_data[simulation_id] = data
                self._last_modified[simulation_id] = file_path.stat().st_mtime
                logger.debug(f"Loaded {len(data)} files for simulation {simulation_id}")
            else:
                self._simulation_data[simulation_id] = {}
                logger.debug(f"No existing file data for simulation {simulation_id}")
                
        except (json.JSONDecodeError, IOError) as e:
            logger.error(f"Error loading files for simulation {simulation_id}: {e}")
            raise RepositoryError(f"Failed to load simulation files: {e}") from e
    
    def _save_simulation_data(self, simulation_id: str) -> None:
        """Save data for a specific simulation."""
        try:
            file_path = self._get_simulation_file_path(simulation_id)
            data = self._simulation_data.get(simulation_id, {})
            
            # Write to temporary file first for atomic operation
            temp_path = file_path.with_suffix('.tmp')
            with open(temp_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            
            # Atomic move
            temp_path.replace(file_path)
            self._last_modified[simulation_id] = time.time()
            
            logger.debug(f"Saved {len(data)} files for simulation {simulation_id}")
            
        except (IOError, OSError) as e:
            logger.error(f"Error saving files for simulation {simulation_id}: {e}")
            raise RepositoryError(f"Failed to save simulation files: {e}") from e
    
    def _extract_simulation_id_from_file(self, file: SimulationFile) -> Optional[str]:
        """Extract simulation_id from a SimulationFile's attributes."""
        return file.attributes.get('simulation_id') if file.attributes else None
    
    def _discover_simulation_files(self) -> List[str]:
        """Discover all simulation file JSON files in the storage directory."""
        try:
            if not self.storage_dir.exists():
                return []
            
            simulation_ids = []
            for file_path in self.storage_dir.glob("simulation-files-*.json"):
                # Extract simulation_id from filename
                filename = file_path.name
                if filename.startswith("simulation-files-") and filename.endswith(".json"):
                    simulation_id = filename[len("simulation-files-"):-len(".json")]
                    simulation_ids.append(simulation_id)
            
            return simulation_ids
            
        except Exception as e:
            logger.error(f"Error discovering simulation files: {e}")
            return []
    
    def _load_all_simulations(self) -> None:
        """Load data for all discovered simulations."""
        simulation_ids = self._discover_simulation_files()
        for simulation_id in simulation_ids:
            self._load_simulation_data(simulation_id)
    
    def _get_file_key(self, file: SimulationFile) -> str:
        """
        Get the storage key for a file.
        For regular files, use relative_path.
        For archives (backward compatibility), use relative_path as ID.
        """
        return file.relative_path
    
    def save(self, file: SimulationFile) -> None:
        """Save a simulation file entity."""
        simulation_id = self._extract_simulation_id_from_file(file)
        if not simulation_id:
            raise RepositoryError("Cannot save file without simulation_id in attributes")
        
        self._load_simulation_data(simulation_id)
        
        try:
            key = self._get_file_key(file)
            if simulation_id not in self._simulation_data:
                self._simulation_data[simulation_id] = {}
            
            self._simulation_data[simulation_id][key] = file.to_dict()
            self._save_simulation_data(simulation_id)
            
            logger.info(f"Saved simulation file: {key} (type: {file.file_type.value}) for simulation {simulation_id}")
            
        except Exception as e:
            logger.error(f"Failed to save simulation file {file.relative_path}: {e}")
            raise RepositoryError(f"Failed to save simulation file: {e}") from e
    
    def get_by_path(self, relative_path: str) -> Optional[SimulationFile]:
        """Retrieve a file by its relative path."""
        # Need to search across all simulations since we don't know which one contains this file
        self._load_all_simulations()
        
        try:
            # Search through all loaded simulation data
            for simulation_id, simulation_files in self._simulation_data.items():
                file_data = simulation_files.get(relative_path)
                if file_data:
                    return SimulationFile.from_dict(file_data)
            
            return None
            
        except Exception as e:
            logger.error(f"Failed to retrieve file by path {relative_path}: {e}")
            raise RepositoryError(f"Failed to retrieve file: {e}") from e
    
    def get_by_id(self, file_id: str) -> Optional[SimulationFile]:
        """Retrieve a file by its ID (same as path for now)."""
        return self.get_by_path(file_id)
    
    def list_all(self) -> List[SimulationFile]:
        """Retrieve all simulation files."""
        self._load_all_simulations()
        
        try:
            files = []
            # Aggregate files from all simulations
            for simulation_id, simulation_files in self._simulation_data.items():
                for file_data in simulation_files.values():
                    files.append(SimulationFile.from_dict(file_data))
            
            return files
            
        except Exception as e:
            logger.error(f"Failed to list all files: {e}")
            raise RepositoryError(f"Failed to list files: {e}") from e
    
    def list_by_type(self, file_type: FileType) -> List[SimulationFile]:
        """Retrieve files by their type."""
        self._load_all_simulations()
        
        try:
            files = []
            # Search through all simulations for files of the specified type
            for simulation_id, simulation_files in self._simulation_data.items():
                for file_data in simulation_files.values():
                    if file_data.get('file_type', 'regular') == file_type.value:
                        files.append(SimulationFile.from_dict(file_data))
            
            return files
            
        except Exception as e:
            logger.error(f"Failed to list files by type {file_type}: {e}")
            raise RepositoryError(f"Failed to list files by type: {e}") from e
    
    def list_by_simulation(self, simulation_id: str) -> List[SimulationFile]:
        """Retrieve files associated with a simulation."""
        # Only load data for the specific simulation
        self._load_simulation_data(simulation_id)
        
        try:
            files = []
            simulation_files = self._simulation_data.get(simulation_id, {})
            
            # All files in this simulation's file should be associated with it
            for file_data in simulation_files.values():
                files.append(SimulationFile.from_dict(file_data))
            
            return files
            
        except Exception as e:
            logger.error(f"Failed to list files by simulation {simulation_id}: {e}")
            raise RepositoryError(f"Failed to list files by simulation: {e}") from e
    
    def list_by_location(self, location_name: str) -> List[SimulationFile]:
        """Retrieve files available at a specific location."""
        self._load_all_simulations()
        
        try:
            files = []
            # Search through all simulations for files at this location
            for simulation_id, simulation_files in self._simulation_data.items():
                for file_data in simulation_files.values():
                    available_locations = file_data.get('available_locations', [])
                    if location_name in available_locations:
                        files.append(SimulationFile.from_dict(file_data))
            
            return files
            
        except Exception as e:
            logger.error(f"Failed to list files by location {location_name}: {e}")
            raise RepositoryError(f"Failed to list files by location: {e}") from e
    
    def list_by_parent(self, parent_file_id: str) -> List[SimulationFile]:
        """Retrieve files that have a specific parent file."""
        self._load_all_simulations()
        
        try:
            files = []
            # Search through all simulations for files with this parent
            for simulation_id, simulation_files in self._simulation_data.items():
                for file_data in simulation_files.values():
                    if file_data.get('parent_file_id') == parent_file_id:
                        files.append(SimulationFile.from_dict(file_data))
            
            return files
            
        except Exception as e:
            logger.error(f"Failed to list files by parent {parent_file_id}: {e}")
            raise RepositoryError(f"Failed to list files by parent: {e}") from e
    
    def get_children(self, file_id: str) -> List[SimulationFile]:
        """Get all files contained by a specific file."""
        self._load_all_simulations()
        
        try:
            # First find the parent file
            parent_file_data = None
            parent_simulation_id = None
            
            for simulation_id, simulation_files in self._simulation_data.items():
                if file_id in simulation_files:
                    parent_file_data = simulation_files[file_id]
                    parent_simulation_id = simulation_id
                    break
            
            if not parent_file_data:
                return []
            
            contained_file_ids = parent_file_data.get('contained_file_ids', [])
            files = []
            
            # Find files by contained IDs - search through all simulations
            for contained_id in contained_file_ids:
                for simulation_id, simulation_files in self._simulation_data.items():
                    for file_data in simulation_files.values():
                        if (file_data.get('relative_path') == contained_id or 
                            file_data.get('parent_file_id') == file_id):
                            files.append(SimulationFile.from_dict(file_data))
            
            return files
            
        except Exception as e:
            logger.error(f"Failed to get children of {file_id}: {e}")
            raise RepositoryError(f"Failed to get children: {e}") from e
    
    def delete(self, file_path_or_id: str) -> bool:
        """Delete a simulation file."""
        self._load_all_simulations()
        
        try:
            # Find which simulation contains this file
            for simulation_id, simulation_files in self._simulation_data.items():
                if file_path_or_id in simulation_files:
                    del simulation_files[file_path_or_id]
                    self._save_simulation_data(simulation_id)
                    logger.info(f"Deleted simulation file: {file_path_or_id} from simulation {simulation_id}")
                    return True
            
            return False
            
        except Exception as e:
            logger.error(f"Failed to delete file {file_path_or_id}: {e}")
            raise RepositoryError(f"Failed to delete file: {e}") from e
    
    def exists(self, file_path_or_id: str) -> bool:
        """Check if a file exists."""
        self._load_all_simulations()
        
        # Check if file exists in any simulation
        for simulation_id, simulation_files in self._simulation_data.items():
            if file_path_or_id in simulation_files:
                return True
        
        return False
    
    def count_by_type(self) -> Dict[FileType, int]:
        """Get count of files by type."""
        self._load_all_simulations()
        
        try:
            counts = {file_type: 0 for file_type in FileType}
            
            # Count files across all simulations
            for simulation_id, simulation_files in self._simulation_data.items():
                for file_data in simulation_files.values():
                    file_type_str = file_data.get('file_type', 'regular')
                    file_type = FileType(file_type_str)
                    counts[file_type] += 1
            
            return counts
            
        except Exception as e:
            logger.error(f"Failed to count files by type: {e}")
            raise RepositoryError(f"Failed to count files by type: {e}") from e
    
    def search(self, pattern: str, file_type: Optional[FileType] = None) -> List[SimulationFile]:
        """Search for files matching a pattern."""
        self._load_all_simulations()
        
        try:
            files = []
            # Search through all simulations
            for simulation_id, simulation_files in self._simulation_data.items():
                for file_data in simulation_files.values():
                    # Check file type filter
                    if file_type:
                        file_type_str = file_data.get('file_type', 'regular')
                        if FileType(file_type_str) != file_type:
                            continue
                    
                    # Check pattern match
                    relative_path = file_data.get('relative_path', '')
                    if fnmatch(relative_path, pattern):
                        files.append(SimulationFile.from_dict(file_data))
            
            return files
            
        except Exception as e:
            logger.error(f"Failed to search files with pattern {pattern}: {e}")
            raise RepositoryError(f"Failed to search files: {e}") from e
    
    # Migration helper methods
    
    def migrate_from_archive_repository(self, archive_repo: 'IArchiveRepository') -> int:
        """
        Migrate archives from old archive repository to unified repository.
        
        Args:
            archive_repo: The old archive repository to migrate from
            
        Returns:
            Number of archives migrated
        """
        try:
            archives = archive_repo.list_all()
            migrated_count = 0
            
            for archive_metadata in archives:
                # Convert to SimulationFile
                simulation_file = SimulationFile.from_archive_metadata(archive_metadata)
                
                # Save to unified repository
                self.save(simulation_file)
                migrated_count += 1
                
                logger.info(f"Migrated archive: {archive_metadata.archive_id}")
            
            logger.info(f"Successfully migrated {migrated_count} archives")
            return migrated_count
            
        except Exception as e:
            logger.error(f"Failed to migrate from archive repository: {e}")
            raise RepositoryError(f"Archive migration failed: {e}") from e