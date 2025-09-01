"""
Repository interface for archive persistence.
"""

from abc import ABC, abstractmethod
from typing import List, Optional, Set

from ..entities.archive import ArchiveId, ArchiveMetadata


class IArchiveRepository(ABC):
    """
    Abstract repository interface for archive persistence.
    
    This interface defines the contract for archive data access,
    allowing different storage implementations without affecting
    the domain logic.
    """
    
    @abstractmethod
    def save(self, archive: ArchiveMetadata) -> None:
        """
        Save an archive metadata entity.
        
        Args:
            archive: The archive metadata entity to save
            
        Raises:
            RepositoryError: If the save operation fails
        """
        pass
    
    @abstractmethod
    def get_by_id(self, archive_id: str) -> Optional[ArchiveMetadata]:
        """
        Retrieve an archive by its ID.
        
        Args:
            archive_id: The ID of the archive to retrieve
            
        Returns:
            The archive metadata entity if found, None otherwise
            
        Raises:
            RepositoryError: If the retrieval operation fails
        """
        pass
    
    @abstractmethod
    def list_all(self) -> List[ArchiveMetadata]:
        """
        Retrieve all archives.
        
        Returns:
            List of all archive metadata entities
            
        Raises:
            RepositoryError: If the retrieval operation fails
        """
        pass
    
    @abstractmethod
    def list_by_simulation(self, simulation_id: str) -> List[ArchiveMetadata]:
        """
        Retrieve all archives associated with a specific simulation.
        
        Args:
            simulation_id: The ID of the simulation
            
        Returns:
            List of archive metadata entities for the simulation
            
        Raises:
            RepositoryError: If the retrieval operation fails
        """
        pass
    
    @abstractmethod
    def exists(self, archive_id: str) -> bool:
        """
        Check if an archive exists.
        
        Args:
            archive_id: The ID of the archive to check
            
        Returns:
            True if the archive exists, False otherwise
            
        Raises:
            RepositoryError: If the check operation fails
        """
        pass
    
    @abstractmethod
    def delete(self, archive_id: str) -> bool:
        """
        Delete an archive by its ID.
        
        Args:
            archive_id: The ID of the archive to delete
            
        Returns:
            True if the archive was deleted, False if it didn't exist
            
        Raises:
            RepositoryError: If the delete operation fails
        """
        pass
    
    @abstractmethod
    def find_by_tags(self, tags: Set[str], match_all: bool = False) -> List[ArchiveMetadata]:
        """
        Find archives by tags.
        
        Args:
            tags: Set of tags to search for
            match_all: If True, archives must have all tags. If False, any tag matches.
            
        Returns:
            List of archive metadata entities matching the tag criteria
            
        Raises:
            RepositoryError: If the search operation fails
        """
        pass