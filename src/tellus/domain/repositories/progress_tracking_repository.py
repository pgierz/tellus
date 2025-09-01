"""
Repository interface for progress tracking persistence.

This module defines the abstract repository interface for persisting and retrieving
progress tracking entities, following clean architecture principles.
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Set

from ..entities.progress_tracking import (OperationStatus, OperationType,
                                          Priority, ProgressTrackingEntity)


class ProgressTrackingRepositoryError(Exception):
    """Base exception for progress tracking repository errors."""
    pass


class OperationNotFoundError(ProgressTrackingRepositoryError):
    """Raised when an operation is not found in the repository."""
    pass


class OperationAlreadyExistsError(ProgressTrackingRepositoryError):
    """Raised when attempting to create an operation that already exists."""
    pass


class IProgressTrackingRepository(ABC):
    """
    Abstract repository interface for progress tracking entities.
    
    This interface defines the contract for persisting and retrieving progress
    tracking information without coupling to specific storage implementations.
    """
    
    @abstractmethod
    async def create(self, entity: ProgressTrackingEntity) -> None:
        """
        Create a new progress tracking entity.
        
        Args:
            entity: The progress tracking entity to create
            
        Raises:
            OperationAlreadyExistsError: If operation with the same ID already exists
            ProgressTrackingRepositoryError: If creation fails
        """
        pass
    
    @abstractmethod
    async def get_by_id(self, operation_id: str) -> Optional[ProgressTrackingEntity]:
        """
        Retrieve a progress tracking entity by its operation ID.
        
        Args:
            operation_id: The unique operation identifier
            
        Returns:
            The progress tracking entity or None if not found
            
        Raises:
            ProgressTrackingRepositoryError: If retrieval fails
        """
        pass
    
    @abstractmethod
    async def update(self, entity: ProgressTrackingEntity) -> None:
        """
        Update an existing progress tracking entity.
        
        Args:
            entity: The progress tracking entity to update
            
        Raises:
            OperationNotFoundError: If operation does not exist
            ProgressTrackingRepositoryError: If update fails
        """
        pass
    
    @abstractmethod
    async def delete(self, operation_id: str) -> bool:
        """
        Delete a progress tracking entity.
        
        Args:
            operation_id: The unique operation identifier
            
        Returns:
            True if the entity was deleted, False if it didn't exist
            
        Raises:
            ProgressTrackingRepositoryError: If deletion fails
        """
        pass
    
    @abstractmethod
    async def list_operations(
        self,
        status_filter: Optional[Set[OperationStatus]] = None,
        operation_type_filter: Optional[Set[OperationType]] = None,
        priority_filter: Optional[Set[Priority]] = None,
        user_id_filter: Optional[str] = None,
        parent_operation_filter: Optional[str] = None,
        tag_filter: Optional[Set[str]] = None,
        limit: Optional[int] = None,
        offset: int = 0,
        order_by: str = "created_time",
        ascending: bool = False
    ) -> List[ProgressTrackingEntity]:
        """
        List progress tracking entities with optional filtering and pagination.
        
        Args:
            status_filter: Filter by operation status
            operation_type_filter: Filter by operation type
            priority_filter: Filter by priority level
            user_id_filter: Filter by user ID
            parent_operation_filter: Filter by parent operation ID
            tag_filter: Filter by tags (must contain at least one tag)
            limit: Maximum number of results to return
            offset: Number of results to skip
            order_by: Field to order by (created_time, last_update_time, priority)
            ascending: Sort order (True for ascending, False for descending)
            
        Returns:
            List of matching progress tracking entities
            
        Raises:
            ProgressTrackingRepositoryError: If listing fails
        """
        pass
    
    @abstractmethod
    async def get_active_operations(self) -> List[ProgressTrackingEntity]:
        """
        Get all operations that are currently active (not in terminal states).
        
        Returns:
            List of active progress tracking entities
            
        Raises:
            ProgressTrackingRepositoryError: If retrieval fails
        """
        pass
    
    @abstractmethod
    async def get_operations_by_parent(
        self, 
        parent_operation_id: str
    ) -> List[ProgressTrackingEntity]:
        """
        Get all sub-operations for a given parent operation.
        
        Args:
            parent_operation_id: The parent operation identifier
            
        Returns:
            List of child progress tracking entities
            
        Raises:
            ProgressTrackingRepositoryError: If retrieval fails
        """
        pass
    
    @abstractmethod
    async def get_operations_by_user(
        self, 
        user_id: str, 
        include_completed: bool = False
    ) -> List[ProgressTrackingEntity]:
        """
        Get all operations for a specific user.
        
        Args:
            user_id: The user identifier
            include_completed: Whether to include completed operations
            
        Returns:
            List of user's progress tracking entities
            
        Raises:
            ProgressTrackingRepositoryError: If retrieval fails
        """
        pass
    
    @abstractmethod
    async def get_operations_by_context(
        self,
        simulation_id: Optional[str] = None,
        location_name: Optional[str] = None,
        workflow_id: Optional[str] = None,
        session_id: Optional[str] = None
    ) -> List[ProgressTrackingEntity]:
        """
        Get operations by context criteria.
        
        Args:
            simulation_id: Filter by simulation ID
            location_name: Filter by location name
            workflow_id: Filter by workflow ID
            session_id: Filter by session ID
            
        Returns:
            List of matching progress tracking entities
            
        Raises:
            ProgressTrackingRepositoryError: If retrieval fails
        """
        pass
    
    @abstractmethod
    async def count_operations(
        self,
        status_filter: Optional[Set[OperationStatus]] = None,
        operation_type_filter: Optional[Set[OperationType]] = None,
        user_id_filter: Optional[str] = None
    ) -> int:
        """
        Count operations matching the given criteria.
        
        Args:
            status_filter: Filter by operation status
            operation_type_filter: Filter by operation type
            user_id_filter: Filter by user ID
            
        Returns:
            Number of matching operations
            
        Raises:
            ProgressTrackingRepositoryError: If count fails
        """
        pass
    
    @abstractmethod
    async def cleanup_completed_operations(
        self, 
        older_than_seconds: float,
        preserve_failed: bool = True
    ) -> int:
        """
        Clean up completed operations older than the specified time.
        
        Args:
            older_than_seconds: Remove operations completed more than this many seconds ago
            preserve_failed: Whether to preserve failed operations from cleanup
            
        Returns:
            Number of operations cleaned up
            
        Raises:
            ProgressTrackingRepositoryError: If cleanup fails
        """
        pass
    
    @abstractmethod
    async def get_statistics(self) -> Dict[str, any]:
        """
        Get repository statistics.
        
        Returns:
            Dictionary containing repository statistics
            
        Raises:
            ProgressTrackingRepositoryError: If statistics retrieval fails
        """
        pass
    
    @abstractmethod
    async def bulk_update_status(
        self,
        operation_ids: List[str],
        new_status: OperationStatus,
        reason: Optional[str] = None
    ) -> List[str]:
        """
        Update status for multiple operations in bulk.
        
        Args:
            operation_ids: List of operation IDs to update
            new_status: New status to set
            reason: Optional reason for the status change
            
        Returns:
            List of operation IDs that were successfully updated
            
        Raises:
            ProgressTrackingRepositoryError: If bulk update fails
        """
        pass
    
    @abstractmethod
    async def exists(self, operation_id: str) -> bool:
        """
        Check if an operation exists in the repository.
        
        Args:
            operation_id: The unique operation identifier
            
        Returns:
            True if the operation exists, False otherwise
            
        Raises:
            ProgressTrackingRepositoryError: If existence check fails
        """
        pass
    
    @abstractmethod
    async def get_recent_log_entries(
        self,
        operation_id: str,
        limit: int = 100
    ) -> List[Dict[str, any]]:
        """
        Get recent log entries for an operation.
        
        Args:
            operation_id: The unique operation identifier
            limit: Maximum number of log entries to return
            
        Returns:
            List of log entry dictionaries
            
        Raises:
            OperationNotFoundError: If operation does not exist
            ProgressTrackingRepositoryError: If retrieval fails
        """
        pass


class IProgressTrackingRepositoryFactory(ABC):
    """Factory interface for creating progress tracking repositories."""
    
    @abstractmethod
    def create_repository(self, **config) -> IProgressTrackingRepository:
        """
        Create a progress tracking repository instance.
        
        Args:
            **config: Repository-specific configuration
            
        Returns:
            Progress tracking repository instance
        """
        pass