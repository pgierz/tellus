"""
Repository interface for network topology persistence.

Defines the contract for storing and retrieving network topology data
following clean architecture principles.
"""

from abc import ABC, abstractmethod
from typing import List, Optional

from ..entities.network_topology import NetworkTopology


class INetworkTopologyRepository(ABC):
    """
    Repository interface for network topology persistence.
    
    Provides abstraction for storing and retrieving NetworkTopology entities
    without coupling to specific storage implementations.
    """
    
    @abstractmethod
    def save_topology(self, topology: NetworkTopology) -> None:
        """
        Save a network topology.
        
        Args:
            topology: NetworkTopology entity to save
            
        Raises:
            RepositoryError: If save operation fails
        """
        pass
    
    @abstractmethod
    def get_topology(self, name: str) -> Optional[NetworkTopology]:
        """
        Retrieve topology by name.
        
        Args:
            name: Name of topology to retrieve
            
        Returns:
            NetworkTopology entity if found, None otherwise
            
        Raises:
            RepositoryError: If retrieval operation fails
        """
        pass
    
    @abstractmethod
    def list_topologies(self) -> List[NetworkTopology]:
        """
        List all topologies.
        
        Returns:
            List of all NetworkTopology entities
            
        Raises:
            RepositoryError: If list operation fails
        """
        pass
    
    @abstractmethod
    def delete_topology(self, name: str) -> bool:
        """
        Delete topology by name.
        
        Args:
            name: Name of topology to delete
            
        Returns:
            True if topology was deleted, False if not found
            
        Raises:
            RepositoryError: If delete operation fails
        """
        pass
    
    @abstractmethod
    def topology_exists(self, name: str) -> bool:
        """
        Check if topology exists.
        
        Args:
            name: Name of topology to check
            
        Returns:
            True if topology exists, False otherwise
            
        Raises:
            RepositoryError: If check operation fails
        """
        pass