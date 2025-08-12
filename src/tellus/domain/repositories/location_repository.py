"""
Repository interface for location persistence.
"""

from abc import ABC, abstractmethod
from typing import List, Optional

from ..entities.location import LocationEntity, LocationKind


class ILocationRepository(ABC):
    """
    Abstract repository interface for location persistence.
    
    This interface defines the contract for location data access,
    allowing different storage implementations without affecting
    the domain logic.
    """
    
    @abstractmethod
    def save(self, location: LocationEntity) -> None:
        """
        Save a location entity.
        
        Args:
            location: The location entity to save
            
        Raises:
            RepositoryError: If the save operation fails
        """
        pass
    
    @abstractmethod
    def get_by_name(self, name: str) -> Optional[LocationEntity]:
        """
        Retrieve a location by its name.
        
        Args:
            name: The name of the location to retrieve
            
        Returns:
            The location entity if found, None otherwise
            
        Raises:
            RepositoryError: If the retrieval operation fails
        """
        pass
    
    @abstractmethod
    def list_all(self) -> List[LocationEntity]:
        """
        List all locations.
        
        Returns:
            List of all location entities
            
        Raises:
            RepositoryError: If the list operation fails
        """
        pass
    
    @abstractmethod
    def delete(self, name: str) -> bool:
        """
        Delete a location by its name.
        
        Args:
            name: The name of the location to delete
            
        Returns:
            True if the location was deleted, False if it didn't exist
            
        Raises:
            RepositoryError: If the delete operation fails
        """
        pass
    
    @abstractmethod
    def exists(self, name: str) -> bool:
        """
        Check if a location exists.
        
        Args:
            name: The name of the location to check
            
        Returns:
            True if the location exists, False otherwise
            
        Raises:
            RepositoryError: If the check operation fails
        """
        pass
    
    @abstractmethod
    def find_by_kind(self, kind: LocationKind) -> List[LocationEntity]:
        """
        Find all locations that have a specific kind.
        
        Args:
            kind: The location kind to search for
            
        Returns:
            List of locations with the specified kind
            
        Raises:
            RepositoryError: If the search operation fails
        """
        pass
    
    @abstractmethod
    def find_by_protocol(self, protocol: str) -> List[LocationEntity]:
        """
        Find all locations that use a specific protocol.
        
        Args:
            protocol: The protocol to search for (e.g., 'sftp', 'file')
            
        Returns:
            List of locations with the specified protocol
            
        Raises:
            RepositoryError: If the search operation fails
        """
        pass
    
    @abstractmethod
    def count(self) -> int:
        """
        Get the total number of locations.
        
        Returns:
            The number of locations
            
        Raises:
            RepositoryError: If the count operation fails
        """
        pass