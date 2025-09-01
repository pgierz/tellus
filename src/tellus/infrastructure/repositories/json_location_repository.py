"""
JSON-based location repository implementation.
"""

import json
import os
import threading
from pathlib import Path
from typing import List, Optional

from ...domain.entities.location import (LocationEntity, LocationKind,
                                         PathTemplate)
from ...domain.repositories.exceptions import (LocationExistsError,
                                               LocationNotFoundError,
                                               RepositoryError)
from ...domain.repositories.location_repository import ILocationRepository


class JsonLocationRepository(ILocationRepository):
    """
    JSON file-based implementation of location repository.
    
    Provides atomic file operations and thread-safe access to location data
    stored in JSON format, compatible with the existing locations.json format.
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
    
    def save(self, location: LocationEntity) -> None:
        """Save a location entity to the JSON file."""
        with self._lock:
            try:
                data = self._load_data()
                
                # Convert entity to dictionary format
                path_templates_data = []
                for template in location.path_templates:
                    template_dict = {
                        "name": template.name,
                        "pattern": template.pattern,
                        "description": template.description,
                        "required_attributes": template.required_attributes
                    }
                    path_templates_data.append(template_dict)
                
                location_dict = {
                    "kinds": [kind.name for kind in location.kinds],
                    "config": location.config,
                    "path_templates": path_templates_data
                }
                
                data[location.name] = location_dict
                self._save_data(data)
                
            except Exception as e:
                raise RepositoryError(f"Failed to save location '{location.name}': {e}")
    
    def get_by_name(self, name: str) -> Optional[LocationEntity]:
        """Retrieve a location by its name."""
        with self._lock:
            try:
                data = self._load_data()
                
                if name not in data:
                    return None
                
                location_data = data[name]
                return self._dict_to_entity(name, location_data)
                
            except Exception as e:
                raise RepositoryError(f"Failed to retrieve location '{name}': {e}")
    
    def list_all(self) -> List[LocationEntity]:
        """List all locations."""
        with self._lock:
            try:
                data = self._load_data()
                return [
                    self._dict_to_entity(name, loc_data) 
                    for name, loc_data in data.items()
                ]
                
            except Exception as e:
                raise RepositoryError(f"Failed to list locations: {e}")
    
    def delete(self, name: str) -> bool:
        """Delete a location by its name."""
        with self._lock:
            try:
                data = self._load_data()
                
                if name not in data:
                    return False
                
                del data[name]
                self._save_data(data)
                return True
                
            except Exception as e:
                raise RepositoryError(f"Failed to delete location '{name}': {e}")
    
    def exists(self, name: str) -> bool:
        """Check if a location exists."""
        with self._lock:
            try:
                data = self._load_data()
                return name in data
                
            except Exception as e:
                raise RepositoryError(f"Failed to check location existence '{name}': {e}")
    
    def find_by_kind(self, kind: LocationKind) -> List[LocationEntity]:
        """Find all locations that have a specific kind."""
        with self._lock:
            try:
                data = self._load_data()
                matching_locations = []
                
                for name, loc_data in data.items():
                    location = self._dict_to_entity(name, loc_data)
                    if location.has_kind(kind):
                        matching_locations.append(location)
                
                return matching_locations
                
            except Exception as e:
                raise RepositoryError(f"Failed to find locations by kind '{kind}': {e}")
    
    def find_by_protocol(self, protocol: str) -> List[LocationEntity]:
        """Find all locations that use a specific protocol."""
        with self._lock:
            try:
                data = self._load_data()
                matching_locations = []
                
                for name, loc_data in data.items():
                    location = self._dict_to_entity(name, loc_data)
                    if location.get_protocol() == protocol:
                        matching_locations.append(location)
                
                return matching_locations
                
            except Exception as e:
                raise RepositoryError(f"Failed to find locations by protocol '{protocol}': {e}")
    
    def count(self) -> int:
        """Get the total number of locations."""
        with self._lock:
            try:
                data = self._load_data()
                return len(data)
                
            except Exception as e:
                raise RepositoryError(f"Failed to count locations: {e}")
    
    def _load_data(self) -> dict:
        """Load data from the JSON file."""
        try:
            if not self._file_path.exists():
                return {}
            
            with open(self._file_path, 'r', encoding='utf-8') as f:
                return json.load(f)
                
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
    
    def _dict_to_entity(self, name: str, data: dict) -> LocationEntity:
        """Convert dictionary data to LocationEntity."""
        try:
            # Parse location kinds
            kinds = []
            for kind_str in data.get("kinds", []):
                try:
                    kinds.append(LocationKind.from_str(kind_str))
                except ValueError:
                    # Skip invalid kinds but log warning
                    print(f"Warning: Invalid location kind '{kind_str}' for location '{name}'")
            
            # Parse path templates
            path_templates = []
            for template_data in data.get("path_templates", []):
                try:
                    path_template = PathTemplate(
                        name=template_data["name"],
                        pattern=template_data["pattern"],
                        description=template_data.get("description", ""),
                        required_attributes=template_data.get("required_attributes", [])
                    )
                    path_templates.append(path_template)
                except (KeyError, ValueError) as e:
                    print(f"Warning: Invalid path template in location '{name}': {e}")
            
            # Create entity
            entity = LocationEntity(
                name=name,
                kinds=kinds,
                config=data.get("config", {}),
                path_templates=path_templates
            )
            
            return entity
            
        except Exception as e:
            raise RepositoryError(f"Failed to convert data to LocationEntity: {e}")
    
    
    def backup_data(self, backup_path: Path) -> None:
        """
        Create a backup of the current location data.
        
        Args:
            backup_path: Path where to save the backup
        """
        with self._lock:
            try:
                data = self._load_data()
                
                backup_path.parent.mkdir(parents=True, exist_ok=True)
                with open(backup_path, 'w', encoding='utf-8') as f:
                    json.dump(data, f, indent=2, default=str, ensure_ascii=False)
                
                print(f"Location data backed up to {backup_path}")
                
            except Exception as e:
                raise RepositoryError(f"Failed to backup data: {e}")
    
    def restore_from_backup(self, backup_path: Path) -> None:
        """
        Restore location data from a backup.
        
        Args:
            backup_path: Path to the backup file
        """
        with self._lock:
            try:
                if not backup_path.exists():
                    raise RepositoryError(f"Backup file not found: {backup_path}")
                
                with open(backup_path, 'r', encoding='utf-8') as f:
                    backup_data = json.load(f)
                
                # Validate all locations before restoring
                validation_errors = []
                for name, loc_data in backup_data.items():
                    try:
                        self._dict_to_entity(name, loc_data)
                    except Exception as e:
                        validation_errors.append(f"Location '{name}': {e}")
                
                if validation_errors:
                    raise RepositoryError(f"Backup validation failed: {'; '.join(validation_errors)}")
                
                # If validation passes, restore the data
                self._save_data(backup_data)
                print(f"Location data restored from {backup_path}")
                
            except Exception as e:
                raise RepositoryError(f"Failed to restore from backup: {e}")