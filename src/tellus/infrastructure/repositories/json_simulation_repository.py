"""
JSON-based simulation repository implementation.
"""

import json
import os
import threading
from pathlib import Path
from typing import List, Optional

from ...domain.entities.simulation import SimulationEntity
from ...domain.repositories.simulation_repository import ISimulationRepository
from ...domain.repositories.exceptions import (
    RepositoryError, 
    SimulationExistsError, 
    SimulationNotFoundError
)


class JsonSimulationRepository(ISimulationRepository):
    """
    JSON file-based implementation of simulation repository.
    
    Provides atomic file operations and thread-safe access to simulation data
    stored in JSON format, compatible with the existing simulations.json format.
    """
    
    def __init__(self, file_path: Path):
        """
        Initialize the repository with a JSON file path.
        
        Args:
            file_path: Path to the JSON file for persistence
        """
        self._file_path = Path(file_path)
        self._lock = threading.RLock()
        
        # Ensure parent directory exists
        self._file_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Initialize file if it doesn't exist
        if not self._file_path.exists():
            self._save_data({})
    
    def save(self, simulation: SimulationEntity) -> None:
        """Save a simulation entity to the JSON file."""
        with self._lock:
            try:
                data = self._load_data()
                
                # Convert entity to dictionary format
                simulation_dict = {
                    "simulation_id": simulation.simulation_id,
                    "uid": simulation.uid,
                    "path": simulation.path,
                    "model_id": simulation.model_id,
                    "attrs": simulation.attrs,
                    "namelists": simulation.namelists,
                    "snakemakes": simulation.snakemakes,
                    "associated_locations": list(simulation.associated_locations),
                    "location_contexts": simulation.location_contexts
                }
                
                data[simulation.simulation_id] = simulation_dict
                self._save_data(data)
                
            except Exception as e:
                raise RepositoryError(f"Failed to save simulation '{simulation.simulation_id}': {e}")
    
    def get_by_id(self, simulation_id: str) -> Optional[SimulationEntity]:
        """Retrieve a simulation by its ID."""
        with self._lock:
            try:
                data = self._load_data()
                
                if simulation_id not in data:
                    return None
                
                sim_data = data[simulation_id]
                return self._dict_to_entity(sim_data)
                
            except Exception as e:
                raise RepositoryError(f"Failed to retrieve simulation '{simulation_id}': {e}")
    
    def list_all(self) -> List[SimulationEntity]:
        """List all simulations."""
        with self._lock:
            try:
                data = self._load_data()
                return [self._dict_to_entity(sim_data) for sim_data in data.values()]
                
            except Exception as e:
                raise RepositoryError(f"Failed to list simulations: {e}")
    
    def delete(self, simulation_id: str) -> bool:
        """Delete a simulation by its ID."""
        with self._lock:
            try:
                data = self._load_data()
                
                if simulation_id not in data:
                    return False
                
                del data[simulation_id]
                self._save_data(data)
                return True
                
            except Exception as e:
                raise RepositoryError(f"Failed to delete simulation '{simulation_id}': {e}")
    
    def exists(self, simulation_id: str) -> bool:
        """Check if a simulation exists."""
        with self._lock:
            try:
                data = self._load_data()
                return simulation_id in data
                
            except Exception as e:
                raise RepositoryError(f"Failed to check simulation existence '{simulation_id}': {e}")
    
    def count(self) -> int:
        """Get the total number of simulations."""
        with self._lock:
            try:
                data = self._load_data()
                return len(data)
                
            except Exception as e:
                raise RepositoryError(f"Failed to count simulations: {e}")
    
    def _load_data(self) -> dict:
        """Load data from the JSON file."""
        try:
            if not self._file_path.exists():
                return {}
            
            # Check if file is empty
            if self._file_path.stat().st_size == 0:
                return {}
            
            with open(self._file_path, 'r', encoding='utf-8') as f:
                content = f.read().strip()
                if not content:
                    return {}
                return json.loads(content)
                
        except json.JSONDecodeError as e:
            raise RepositoryError(f"Invalid JSON in {self._file_path}: {e}")
        except Exception as e:
            raise RepositoryError(f"Failed to load data from {self._file_path}: {e}")
    
    def _save_data(self, data: dict) -> None:
        """Save data to the JSON file atomically."""
        try:
            # Write to temporary file first
            temp_file = self._file_path.with_suffix('.tmp')
            
            with open(temp_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, default=str, ensure_ascii=False)
            
            # Atomic replace (POSIX systems)
            if hasattr(os, 'replace'):
                os.replace(temp_file, self._file_path)
            else:
                # Fallback for older systems
                if self._file_path.exists():
                    backup_file = self._file_path.with_suffix('.bak')
                    self._file_path.rename(backup_file)
                
                temp_file.rename(self._file_path)
                
                if backup_file.exists():
                    backup_file.unlink()
                
        except Exception as e:
            # Clean up temp file if it exists
            temp_file = self._file_path.with_suffix('.tmp')
            if temp_file.exists():
                temp_file.unlink()
            raise RepositoryError(f"Failed to save data to {self._file_path}: {e}")
    
    def _dict_to_entity(self, data: dict) -> SimulationEntity:
        """Convert dictionary data to SimulationEntity."""
        try:
            # Create entity with required fields
            entity = SimulationEntity(
                simulation_id=data["simulation_id"],
                model_id=data.get("model_id"),
                path=data.get("path"),
                attrs=data.get("attrs", {}),
                namelists=data.get("namelists", {}),
                snakemakes=data.get("snakemakes", {}),
                associated_locations=set(data.get("associated_locations", [])),
                location_contexts=data.get("location_contexts", {})
            )
            
            # Set the internal UID if it exists
            if "uid" in data:
                entity._uid = data["uid"]
            
            return entity
            
        except Exception as e:
            raise RepositoryError(f"Failed to convert data to SimulationEntity: {e}")
    
    def migrate_from_legacy_format(self, legacy_file: Path) -> None:
        """
        Migrate data from the legacy simulation format.
        
        Args:
            legacy_file: Path to the legacy simulations.json file
        """
        with self._lock:
            try:
                if not legacy_file.exists():
                    return
                
                with open(legacy_file, 'r', encoding='utf-8') as f:
                    legacy_data = json.load(f)
                
                migrated_count = 0
                for sim_id, sim_data in legacy_data.items():
                    try:
                        # Convert legacy format to entity
                        entity = self._dict_to_entity(sim_data)
                        self.save(entity)
                        migrated_count += 1
                    except Exception as e:
                        print(f"Warning: Failed to migrate simulation '{sim_id}': {e}")
                
                print(f"Migrated {migrated_count} simulations from legacy format")
                
            except Exception as e:
                raise RepositoryError(f"Failed to migrate from legacy format: {e}")