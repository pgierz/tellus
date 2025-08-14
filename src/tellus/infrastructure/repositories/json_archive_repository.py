"""
JSON-based archive repository implementation.
"""

import json
import os
import threading
from pathlib import Path
from typing import List, Optional, Set

from ...domain.entities.archive import ArchiveMetadata, ArchiveId, ArchiveType, Checksum
from ...domain.repositories.archive_repository import IArchiveRepository
from ...domain.repositories.exceptions import (
    RepositoryError
)


class JsonArchiveRepository(IArchiveRepository):
    """
    JSON file-based implementation of archive repository.
    
    Provides atomic file operations and thread-safe access to archive metadata
    stored in JSON format. Each archive is stored as an entry in archives.json.
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
    
    def save(self, archive: ArchiveMetadata) -> None:
        """Save an archive metadata entity to the JSON file."""
        with self._lock:
            try:
                data = self._load_data()
                
                archive_id_str = str(archive.archive_id)
                
                # Convert entity to dictionary format
                archive_dict = {
                    "archive_id": archive_id_str,
                    "location": archive.location,
                    "archive_type": archive.archive_type.value,
                    "simulation_id": archive.simulation_id,
                    "checksum": str(archive.checksum) if archive.checksum else None,
                    "size": archive.size,
                    "created_time": archive.created_time,
                    "simulation_date": archive.simulation_date,
                    "version": archive.version,
                    "description": archive.description,
                    "tags": list(archive.tags)
                }
                
                data[archive_id_str] = archive_dict
                self._save_data(data)
                
            except Exception as e:
                raise RepositoryError(f"Failed to save archive '{archive.archive_id}': {e}")
    
    def get_by_id(self, archive_id: str) -> Optional[ArchiveMetadata]:
        """Retrieve an archive by its ID."""
        with self._lock:
            try:
                data = self._load_data()
                
                if archive_id not in data:
                    return None
                
                archive_data = data[archive_id]
                return self._dict_to_entity(archive_data)
                
            except Exception as e:
                raise RepositoryError(f"Failed to retrieve archive '{archive_id}': {e}")
    
    def list_all(self) -> List[ArchiveMetadata]:
        """Retrieve all archives."""
        with self._lock:
            try:
                data = self._load_data()
                return [
                    self._dict_to_entity(archive_data) 
                    for archive_data in data.values()
                ]
                
            except Exception as e:
                raise RepositoryError(f"Failed to list archives: {e}")
    
    def list_by_simulation(self, simulation_id: str) -> List[ArchiveMetadata]:
        """Retrieve all archives associated with a specific simulation."""
        with self._lock:
            try:
                data = self._load_data()
                matching_archives = []
                
                for archive_data in data.values():
                    if archive_data.get("simulation_id") == simulation_id:
                        matching_archives.append(self._dict_to_entity(archive_data))
                
                return matching_archives
                
            except Exception as e:
                raise RepositoryError(f"Failed to list archives for simulation '{simulation_id}': {e}")
    
    def exists(self, archive_id: str) -> bool:
        """Check if an archive exists."""
        with self._lock:
            try:
                data = self._load_data()
                return archive_id in data
                
            except Exception as e:
                raise RepositoryError(f"Failed to check archive existence '{archive_id}': {e}")
    
    def delete(self, archive_id: str) -> bool:
        """Delete an archive by its ID."""
        with self._lock:
            try:
                data = self._load_data()
                
                if archive_id not in data:
                    return False
                
                del data[archive_id]
                self._save_data(data)
                return True
                
            except Exception as e:
                raise RepositoryError(f"Failed to delete archive '{archive_id}': {e}")
    
    def find_by_tags(self, tags: Set[str], match_all: bool = False) -> List[ArchiveMetadata]:
        """Find archives by tags."""
        with self._lock:
            try:
                data = self._load_data()
                matching_archives = []
                
                for archive_data in data.values():
                    archive_tags = set(archive_data.get("tags", []))
                    
                    if match_all:
                        # All required tags must be present
                        if tags.issubset(archive_tags):
                            matching_archives.append(self._dict_to_entity(archive_data))
                    else:
                        # Any tag matches
                        if tags.intersection(archive_tags):
                            matching_archives.append(self._dict_to_entity(archive_data))
                
                return matching_archives
                
            except Exception as e:
                raise RepositoryError(f"Failed to find archives by tags: {e}")
    
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
    
    def _dict_to_entity(self, data: dict) -> ArchiveMetadata:
        """Convert dictionary data to ArchiveMetadata entity."""
        try:
            # Parse archive type
            archive_type = ArchiveType(data["archive_type"])
            
            # Parse checksum if present
            checksum = None
            if data.get("checksum"):
                checksum_str = data["checksum"]
                if ":" in checksum_str:
                    algorithm, value = checksum_str.split(":", 1)
                    checksum = Checksum(value=value, algorithm=algorithm)
                else:
                    # Assume md5 for backward compatibility
                    checksum = Checksum(value=checksum_str, algorithm="md5")
            
            # Create entity
            entity = ArchiveMetadata(
                archive_id=ArchiveId(data["archive_id"]),
                location=data["location"],
                archive_type=archive_type,
                simulation_id=data.get("simulation_id"),
                checksum=checksum,
                size=data.get("size"),
                created_time=data.get("created_time", 0),
                simulation_date=data.get("simulation_date"),
                version=data.get("version"),
                description=data.get("description"),
                tags=set(data.get("tags", []))
            )
            
            return entity
            
        except Exception as e:
            raise RepositoryError(f"Failed to convert data to ArchiveMetadata: {e}")
    
    def backup_data(self, backup_path: Path) -> None:
        """
        Create a backup of the current archive data.
        
        Args:
            backup_path: Path where to save the backup
        """
        with self._lock:
            try:
                data = self._load_data()
                
                backup_path.parent.mkdir(parents=True, exist_ok=True)
                with open(backup_path, 'w', encoding='utf-8') as f:
                    json.dump(data, f, indent=2, default=str, ensure_ascii=False)
                
            except Exception as e:
                raise RepositoryError(f"Failed to backup data: {e}")
    
    def restore_from_backup(self, backup_path: Path) -> None:
        """
        Restore archive data from a backup.
        
        Args:
            backup_path: Path to the backup file
        """
        with self._lock:
            try:
                if not backup_path.exists():
                    raise RepositoryError(f"Backup file not found: {backup_path}")
                
                with open(backup_path, 'r', encoding='utf-8') as f:
                    backup_data = json.load(f)
                
                # Validate all archives before restoring
                validation_errors = []
                for archive_id, archive_data in backup_data.items():
                    try:
                        self._dict_to_entity(archive_data)
                    except Exception as e:
                        validation_errors.append(f"Archive '{archive_id}': {e}")
                
                if validation_errors:
                    raise RepositoryError(f"Backup validation failed: {'; '.join(validation_errors)}")
                
                # If validation passes, restore the data
                self._save_data(backup_data)
                
            except Exception as e:
                raise RepositoryError(f"Failed to restore from backup: {e}")