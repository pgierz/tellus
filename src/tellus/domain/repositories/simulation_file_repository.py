"""
Unified repository interface for SimulationFile entities.

This repository handles all file types in the system including regular files,
archives, directories, and symbolic links through a single interface.
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Set

from ..entities.simulation_file import FileType, SimulationFile


class ISimulationFileRepository(ABC):
    """
    Unified repository interface for all SimulationFile entities.
    
    This interface replaces separate archive and file repositories,
    providing a single interface for all file management operations
    in the unified architecture.
    """
    
    @abstractmethod
    def save(self, file: SimulationFile) -> None:
        """
        Save a simulation file entity.
        
        Args:
            file: The simulation file entity to save
            
        Raises:
            RepositoryError: If the save operation fails
        """
        pass
    
    @abstractmethod
    def get_by_path(self, relative_path: str) -> Optional[SimulationFile]:
        """
        Retrieve a file by its relative path.
        
        Args:
            relative_path: The relative path of the file to retrieve
            
        Returns:
            The simulation file entity if found, None otherwise
            
        Raises:
            RepositoryError: If the retrieval operation fails
        """
        pass
    
    @abstractmethod
    def get_by_id(self, file_id: str) -> Optional[SimulationFile]:
        """
        Retrieve a file by its ID.
        For backward compatibility with archive systems.
        
        Args:
            file_id: The ID of the file to retrieve
            
        Returns:
            The simulation file entity if found, None otherwise
            
        Raises:
            RepositoryError: If the retrieval operation fails
        """
        pass
    
    @abstractmethod
    def list_all(self) -> List[SimulationFile]:
        """
        Retrieve all simulation files.
        
        Returns:
            List of all simulation file entities
            
        Raises:
            RepositoryError: If the retrieval operation fails
        """
        pass
    
    @abstractmethod
    def list_by_type(self, file_type: FileType) -> List[SimulationFile]:
        """
        Retrieve files by their type.
        
        Args:
            file_type: The type of files to retrieve
            
        Returns:
            List of simulation files of the specified type
            
        Raises:
            RepositoryError: If the retrieval operation fails
        """
        pass
    
    @abstractmethod
    def list_by_simulation(self, simulation_id: str) -> List[SimulationFile]:
        """
        Retrieve files associated with a simulation.
        
        Args:
            simulation_id: The ID of the simulation
            
        Returns:
            List of simulation files associated with the simulation
            
        Raises:
            RepositoryError: If the retrieval operation fails
        """
        pass
    
    @abstractmethod
    def list_by_location(self, location_name: str) -> List[SimulationFile]:
        """
        Retrieve files available at a specific location.
        
        Args:
            location_name: The name of the location
            
        Returns:
            List of simulation files available at the location
            
        Raises:
            RepositoryError: If the retrieval operation fails
        """
        pass
    
    @abstractmethod
    def list_by_parent(self, parent_file_id: str) -> List[SimulationFile]:
        """
        Retrieve files that have a specific parent file.
        Used for hierarchical relationships (e.g., files extracted from archives).
        
        Args:
            parent_file_id: The ID of the parent file
            
        Returns:
            List of child files
            
        Raises:
            RepositoryError: If the retrieval operation fails
        """
        pass
    
    @abstractmethod
    def get_children(self, file_id: str) -> List[SimulationFile]:
        """
        Get all files contained by a specific file (for archives/directories).
        
        Args:
            file_id: The ID of the container file
            
        Returns:
            List of contained files
            
        Raises:
            RepositoryError: If the retrieval operation fails
        """
        pass
    
    @abstractmethod
    def delete(self, file_path_or_id: str) -> bool:
        """
        Delete a simulation file.
        
        Args:
            file_path_or_id: Either relative path or ID of the file to delete
            
        Returns:
            True if file was deleted, False if file was not found
            
        Raises:
            RepositoryError: If the deletion operation fails
        """
        pass
    
    @abstractmethod
    def exists(self, file_path_or_id: str) -> bool:
        """
        Check if a file exists.
        
        Args:
            file_path_or_id: Either relative path or ID of the file to check
            
        Returns:
            True if file exists, False otherwise
            
        Raises:
            RepositoryError: If the check operation fails
        """
        pass
    
    @abstractmethod
    def count_by_type(self) -> Dict[FileType, int]:
        """
        Get count of files by type.
        
        Returns:
            Dictionary mapping file types to counts
            
        Raises:
            RepositoryError: If the count operation fails
        """
        pass
    
    @abstractmethod
    def search(self, pattern: str, file_type: Optional[FileType] = None) -> List[SimulationFile]:
        """
        Search for files matching a pattern.
        
        Args:
            pattern: Glob pattern to search for
            file_type: Optional file type filter
            
        Returns:
            List of matching simulation files
            
        Raises:
            RepositoryError: If the search operation fails
        """
        pass
    
    # Archive-Specific Convenience Methods (for backward compatibility)
    
    def get_archive_by_id(self, archive_id: str) -> Optional[SimulationFile]:
        """
        Get an archive file by ID.
        Convenience method for backward compatibility.
        
        Args:
            archive_id: The archive ID
            
        Returns:
            Archive file if found and is of type ARCHIVE, None otherwise
        """
        file = self.get_by_id(archive_id)
        return file if file and file.is_archive() else None
    
    def list_archives(self) -> List[SimulationFile]:
        """
        List all archive files.
        Convenience method for backward compatibility.
        
        Returns:
            List of all archive-type files
        """
        return self.list_by_type(FileType.ARCHIVE)
    
    def list_archives_by_simulation(self, simulation_id: str) -> List[SimulationFile]:
        """
        List archives associated with a simulation.
        Convenience method for backward compatibility.
        
        Args:
            simulation_id: The simulation ID
            
        Returns:
            List of archive files associated with the simulation
        """
        all_files = self.list_by_simulation(simulation_id)
        return [f for f in all_files if f.is_archive()]
    
    # Regular File Convenience Methods
    
    def list_regular_files(self) -> List[SimulationFile]:
        """
        List all regular files.
        
        Returns:
            List of all regular files
        """
        return self.list_by_type(FileType.REGULAR)
    
    def list_directories(self) -> List[SimulationFile]:
        """
        List all directories.
        
        Returns:
            List of all directory entries
        """
        return self.list_by_type(FileType.DIRECTORY)