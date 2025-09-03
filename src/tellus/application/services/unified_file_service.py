"""
Unified File Application Service.

This service provides unified file operations,
providing a single interface for all file operations in the unified architecture.
"""

import logging
import time
from typing import Dict, List, Optional, Set, Union

from ...domain.entities.simulation_file import (FileContentType, FileImportance,
                                                FileType, SimulationFile)
from ...domain.repositories.simulation_file_repository import ISimulationFileRepository
from ..dtos import (CreateArchiveDto, FileRegistrationDto, FileRegistrationResultDto)
from ..exceptions import EntityNotFoundError, ValidationError

logger = logging.getLogger(__name__)


class UnifiedFileService:
    """
    Unified application service for all file operations.
    
    This service handles:
    - Regular file management
    - Archive operations (create, extract, list)
    - File registration and association
    - Hierarchical file relationships
    - Location management for files
    """
    
    def __init__(self, file_repository: ISimulationFileRepository):
        """
        Initialize the unified file service.
        
        Args:
            file_repository: Repository for file persistence
        """
        self.file_repository = file_repository
        self._logger = logger
    
    # === File Management Operations ===
    
    def create_file_from_entity(self, file: SimulationFile) -> None:
        """
        Create a file from an existing SimulationFile entity.
        
        Args:
            file: The simulation file entity to save
        """
        self.file_repository.save(file)
        self._logger.info(f"Created file from entity: {file.relative_path} (type: {file.file_type.value})")
    
    def create_file(self, relative_path: str, file_type: FileType = FileType.REGULAR,
                   content_type: FileContentType = FileContentType.OUTDATA,
                   simulation_id: Optional[str] = None,
                   location_name: Optional[str] = None,
                   **kwargs) -> SimulationFile:
        """
        Create a new simulation file.
        
        Args:
            relative_path: Path of the file
            file_type: Type of the file
            content_type: Content classification
            simulation_id: Associated simulation ID
            location_name: Primary location
            **kwargs: Additional file properties
            
        Returns:
            Created SimulationFile instance
        """
        try:
            # Create the simulation file
            file_obj = SimulationFile(
                relative_path=relative_path,
                file_type=file_type,
                content_type=content_type,
                **kwargs
            )
            
            # Set simulation association
            if simulation_id:
                if 'attributes' not in kwargs:
                    file_obj.attributes = {}
                file_obj.attributes['simulation_id'] = simulation_id
            
            # Set location
            if location_name:
                file_obj.set_primary_location(location_name)
            
            # Save to repository
            self.file_repository.save(file_obj)
            
            self._logger.info(f"Created file: {relative_path} (type: {file_type.value})")
            return file_obj
            
        except Exception as e:
            self._logger.error(f"Failed to create file {relative_path}: {e}")
            raise ValidationError(f"Failed to create file: {e}") from e
    
    def get_file(self, file_path_or_id: str) -> Optional[SimulationFile]:
        """
        Get a file by path or ID.
        
        Args:
            file_path_or_id: File path or ID to retrieve
            
        Returns:
            SimulationFile if found, None otherwise
        """
        return self.file_repository.get_by_path(file_path_or_id)
    
    def list_files(self, file_type: Optional[FileType] = None,
                   simulation_id: Optional[str] = None,
                   location_name: Optional[str] = None) -> List[SimulationFile]:
        """
        List files with optional filtering.
        
        Args:
            file_type: Optional file type filter
            simulation_id: Optional simulation ID filter
            location_name: Optional location filter
            
        Returns:
            List of matching files
        """
        if simulation_id:
            files = self.file_repository.list_by_simulation(simulation_id)
        elif location_name:
            files = self.file_repository.list_by_location(location_name)
        elif file_type:
            files = self.file_repository.list_by_type(file_type)
        else:
            files = self.file_repository.list_all()
        
        # Apply additional filters
        if file_type and not simulation_id and not location_name:
            # Already filtered by type
            pass
        elif file_type:
            files = [f for f in files if f.file_type == file_type]
        
        if location_name and not location_name:
            # Already filtered by location
            pass
        elif location_name:
            files = [f for f in files if f.is_available_at_location(location_name)]
        
        return files
    
    def delete_file(self, file_path_or_id: str) -> bool:
        """
        Delete a file.
        
        Args:
            file_path_or_id: File path or ID to delete
            
        Returns:
            True if deleted, False if not found
        """
        success = self.file_repository.delete(file_path_or_id)
        if success:
            self._logger.info(f"Deleted file: {file_path_or_id}")
        return success
    
    def update_file(self, file_path_or_id: str, **updates) -> Optional[SimulationFile]:
        """
        Update file properties.
        
        Args:
            file_path_or_id: File to update
            **updates: Properties to update
            
        Returns:
            Updated file if found, None otherwise
        """
        file_obj = self.file_repository.get_by_path(file_path_or_id)
        if not file_obj:
            return None
        
        # Update properties
        for key, value in updates.items():
            if hasattr(file_obj, key):
                setattr(file_obj, key, value)
        
        # Save changes
        self.file_repository.save(file_obj)
        
        self._logger.info(f"Updated file: {file_path_or_id}")
        return file_obj
    
    # === Archive Operations (Backward Compatibility) ===
    
    def create_archive(self, creation_dto: CreateArchiveDto) -> SimulationFile:
        """
        Create an archive file.
        
        Args:
            creation_dto: Archive creation parameters
            
        Returns:
            Created archive file
        """
        # Determine archive format from file extension
        archive_format = "tar.gz"  # Default
        archive_path = creation_dto.archive_path or f"{creation_dto.archive_id}.tar.gz"
        if archive_path.endswith('.tar.gz') or archive_path.endswith('.tgz'):
            archive_format = "tar.gz"
        elif archive_path.endswith('.tar.bz2'):
            archive_format = "tar.bz2"
        elif archive_path.endswith('.zip'):
            archive_format = "zip"
        elif archive_path.endswith('.tar'):
            archive_format = "tar"
        
        # Create archive file
        archive_file = self.create_file(
            relative_path=creation_dto.archive_id,
            file_type=FileType.ARCHIVE,
            content_type=FileContentType.OUTDATA,
            simulation_id=creation_dto.simulation_id,
            location_name=creation_dto.location_name,
            size=getattr(creation_dto, 'size', None),
            archive_format=archive_format,
            compression_type='gzip' if archive_format == 'tar.gz' else None,
            created_time=time.time(),
            tags=creation_dto.tags,
            attributes={
                'description': creation_dto.description,
                'version': creation_dto.version,
                'archive_paths': [archive_path],
                'source_path': creation_dto.source_path
            }
        )
        
        return archive_file
    
    def list_archives(self, location_name: Optional[str] = None,
                     simulation_id: Optional[str] = None) -> List[SimulationFile]:
        """
        List archive files.
        
        Args:
            location_name: Optional location filter
            simulation_id: Optional simulation filter
            
        Returns:
            List of archive files
        """
        return self.list_files(
            file_type=FileType.ARCHIVE,
            simulation_id=simulation_id,
            location_name=location_name
        )
    
    def get_archive(self, archive_id: str) -> Optional[SimulationFile]:
        """
        Get an archive file by ID.
        
        Args:
            archive_id: Archive ID to retrieve
            
        Returns:
            Archive file if found, None otherwise
        """
        archive = self.get_file(archive_id)
        return archive if archive and archive.is_archive() else None
    
    # === Hierarchical Relationships ===
    
    def set_file_parent(self, file_path: str, parent_file_path: str) -> bool:
        """
        Set parent-child relationship between files.
        
        Args:
            file_path: Child file path
            parent_file_path: Parent file path
            
        Returns:
            True if successful, False if files not found
        """
        child_file = self.get_file(file_path)
        parent_file = self.get_file(parent_file_path)
        
        if not child_file or not parent_file:
            return False
        
        # Update relationships
        child_file.set_parent_file(parent_file_path)
        parent_file.add_contained_file(file_path)
        
        # Save both files
        self.file_repository.save(child_file)
        self.file_repository.save(parent_file)
        
        self._logger.info(f"Set parent relationship: {file_path} -> {parent_file_path}")
        return True
    
    def get_file_children(self, file_path: str) -> List[SimulationFile]:
        """
        Get all child files of a file.
        
        Args:
            file_path: Parent file path
            
        Returns:
            List of child files
        """
        return self.file_repository.get_children(file_path)
    
    def get_file_parent(self, file_path: str) -> Optional[SimulationFile]:
        """
        Get the parent file of a file.
        
        Args:
            file_path: Child file path
            
        Returns:
            Parent file if exists, None otherwise
        """
        child_file = self.get_file(file_path)
        if child_file and child_file.parent_file_id:
            return self.get_file(child_file.parent_file_id)
        return None
    
    # === File Registration (Simulation Association) ===
    
    def register_files_to_simulation(self, registration_dto: FileRegistrationDto) -> FileRegistrationResultDto:
        """
        Register files to a simulation (replaces archive file registration).
        
        Args:
            registration_dto: Registration parameters
            
        Returns:
            Registration results
        """
        try:
            # Get archive file
            archive_file = self.get_archive(registration_dto.archive_id)
            if not archive_file:
                raise EntityNotFoundError("Archive", registration_dto.archive_id)
            
            # Get files contained in archive
            contained_files = self.get_file_children(registration_dto.archive_id)
            
            # Filter files if needed
            filtered_files = contained_files
            if registration_dto.content_type_filter:
                content_type = FileContentType(registration_dto.content_type_filter)
                filtered_files = [f for f in filtered_files if f.content_type == content_type]
            
            if registration_dto.pattern_filter:
                import fnmatch
                filtered_files = [f for f in filtered_files if fnmatch.fnmatch(f.relative_path, registration_dto.pattern_filter)]
            
            # Register files to simulation
            registered_count = 0
            updated_count = 0
            skipped_count = 0
            
            for file_obj in filtered_files:
                # Check if already registered to this simulation
                current_sim_id = file_obj.attributes.get('simulation_id')
                if current_sim_id == registration_dto.simulation_id:
                    skipped_count += 1
                    continue
                
                # Update simulation association
                if current_sim_id and not registration_dto.overwrite_existing:
                    skipped_count += 1
                    continue
                
                file_obj.attributes['simulation_id'] = registration_dto.simulation_id
                self.file_repository.save(file_obj)
                
                if current_sim_id:
                    updated_count += 1
                else:
                    registered_count += 1
            
            return FileRegistrationResultDto(
                archive_id=registration_dto.archive_id,
                simulation_id=registration_dto.simulation_id,
                success=True,
                files_registered=registered_count,
                files_updated=updated_count,
                files_skipped=skipped_count,
                error_message=None
            )
            
        except Exception as e:
            self._logger.error(f"Failed to register files: {e}")
            return FileRegistrationResultDto(
                archive_id=registration_dto.archive_id,
                simulation_id=registration_dto.simulation_id,
                success=False,
                files_registered=0,
                files_updated=0,
                files_skipped=0,
                error_message=str(e)
            )
    
    def get_simulation_files(self, simulation_id: str) -> List[SimulationFile]:
        """
        Get all files associated with a simulation.
        
        Args:
            simulation_id: Simulation ID
            
        Returns:
            List of associated files
        """
        return self.list_files(simulation_id=simulation_id)
    
    def get_simulation_archives(self, simulation_id: str) -> List[SimulationFile]:
        """
        Get archive files associated with a simulation.
        
        Args:
            simulation_id: Simulation ID
            
        Returns:
            List of associated archive files
        """
        return self.list_files(
            file_type=FileType.ARCHIVE,
            simulation_id=simulation_id
        )
    
    def list_simulation_archives(self, simulation_id: str) -> List[SimulationFile]:
        """
        List archive files associated with a simulation (alias for get_simulation_archives).
        
        Args:
            simulation_id: Simulation ID
            
        Returns:
            List of associated archive files
        """
        return self.get_simulation_archives(simulation_id)
    
    # === Statistics and Summary ===
    
    def get_file_statistics(self) -> Dict[str, Union[int, Dict[str, int]]]:
        """
        Get file statistics.
        
        Returns:
            Dictionary with file counts and statistics
        """
        counts_by_type = self.file_repository.count_by_type()
        all_files = self.file_repository.list_all()
        
        total_size = sum(f.size or 0 for f in all_files)
        archive_files = [f for f in all_files if f.is_archive()]
        regular_files = [f for f in all_files if f.is_regular_file()]
        
        return {
            'total_files': len(all_files),
            'total_size': total_size,
            'counts_by_type': {ft.value: count for ft, count in counts_by_type.items()},
            'archive_count': len(archive_files),
            'regular_file_count': len(regular_files),
            'files_with_parents': len([f for f in all_files if f.has_parent()]),
            'archive_files_with_children': len([f for f in archive_files if f.get_contained_file_count() > 0])
        }
    
    # === Migration Support ===
    
