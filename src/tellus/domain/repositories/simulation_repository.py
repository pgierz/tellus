"""
Repository interface for simulation persistence.
"""

from abc import ABC, abstractmethod
from typing import List, Optional

from ..entities.simulation import SimulationEntity


class ISimulationRepository(ABC):
    """
    Abstract repository interface for simulation persistence.
    
    This interface defines the contract for simulation data access,
    allowing different storage implementations (JSON, database, etc.)
    without affecting the domain logic.
    """
    
    @abstractmethod
    def save(self, simulation: SimulationEntity) -> None:
        """
        Save a simulation entity.
        
        Args:
            simulation: The simulation entity to save
            
        Raises:
            RepositoryError: If the save operation fails
        """
        pass
    
    @abstractmethod
    def get_by_id(self, simulation_id: str) -> Optional[SimulationEntity]:
        """
        Retrieve a simulation by its ID.
        
        Args:
            simulation_id: The ID of the simulation to retrieve
            
        Returns:
            The simulation entity if found, None otherwise
            
        Raises:
            RepositoryError: If the retrieval operation fails
        """
        pass
    
    @abstractmethod
    def list_all(self) -> List[SimulationEntity]:
        """
        List all simulations.
        
        Returns:
            List of all simulation entities
            
        Raises:
            RepositoryError: If the list operation fails
        """
        pass
    
    @abstractmethod
    def delete(self, simulation_id: str) -> bool:
        """
        Delete a simulation by its ID.
        
        Args:
            simulation_id: The ID of the simulation to delete
            
        Returns:
            True if the simulation was deleted, False if it didn't exist
            
        Raises:
            RepositoryError: If the delete operation fails
        """
        pass
    
    @abstractmethod
    def exists(self, simulation_id: str) -> bool:
        """
        Check if a simulation exists.
        
        Args:
            simulation_id: The ID of the simulation to check
            
        Returns:
            True if the simulation exists, False otherwise
            
        Raises:
            RepositoryError: If the check operation fails
        """
        pass
    
    @abstractmethod
    def count(self) -> int:
        """
        Get the total number of simulations.
        
        Returns:
            The number of simulations
            
        Raises:
            RepositoryError: If the count operation fails
        """
        pass