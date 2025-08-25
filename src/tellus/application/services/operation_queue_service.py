"""
Operation Queue Service - Manages the generic operation queue with handlers.

Provides a high-level interface for managing queued operations including
archive operations and file transfers with progress tracking.
"""

import logging
from typing import Optional, Set, Callable, Dict, Any

from .bulk_operation_queue import GenericOperationQueue, QueuePriority, QueueStatus
from .operation_handler import (
    OperationRouter, 
    ArchiveOperationHandler, 
    FileTransferOperationHandler,
    OperationDto,
    OperationResultDto
)
from .archive_service import ArchiveApplicationService
from .file_transfer_service import FileTransferApplicationService

logger = logging.getLogger(__name__)


class OperationQueueService:
    """
    High-level service for managing the operation queue.
    
    Configures handlers for different operation types and provides
    a unified interface for queue management across the application.
    """
    
    def __init__(
        self,
        archive_service: ArchiveApplicationService,
        file_transfer_service: FileTransferApplicationService,
        max_concurrent: int = 3,
        default_priority: QueuePriority = QueuePriority.NORMAL
    ):
        """
        Initialize the operation queue service.
        
        Args:
            archive_service: Service for archive operations
            file_transfer_service: Service for file transfer operations  
            max_concurrent: Maximum concurrent operations
            default_priority: Default priority for new operations
        """
        self._archive_service = archive_service
        self._file_transfer_service = file_transfer_service
        
        # Create operation router and register handlers
        self._router = OperationRouter()
        self._router.register_handler(ArchiveOperationHandler(archive_service))
        self._router.register_handler(FileTransferOperationHandler(file_transfer_service))
        
        # Create the queue
        self._queue = GenericOperationQueue(
            operation_router=self._router,
            max_concurrent=max_concurrent,
            default_priority=default_priority
        )
        
        self._logger = logging.getLogger(__name__)
        self._logger.info(f"Operation queue service initialized with {max_concurrent} max concurrent operations")
    
    async def add_operation(
        self,
        operation_dto: OperationDto,
        priority: Optional[QueuePriority] = None,
        tags: Optional[Set[str]] = None,
        user_id: Optional[str] = None,
        progress_callback: Optional[Callable[[str, Dict[str, Any]], None]] = None
    ) -> str:
        """
        Add an operation to the queue.
        
        Args:
            operation_dto: Operation details (archive or file transfer)
            priority: Operation priority (defaults to service default)
            tags: Tags for categorizing the operation
            user_id: User identifier for the operation
            progress_callback: Callback for progress updates
            
        Returns:
            Operation ID for tracking
        """
        operation_id = await self._queue.add_operation(
            operation_dto=operation_dto,
            priority=priority,
            tags=tags,
            user_id=user_id,
            progress_callback=progress_callback
        )
        
        operation_type = getattr(operation_dto, 'operation_type', 'unknown')
        self._logger.info(f"Added {operation_type} operation {operation_id} to queue")
        
        return operation_id
    
    def get_operation_status(self, operation_id: str):
        """Get status of a specific operation."""
        return self._queue.get_operation_status(operation_id)
    
    def list_operations(
        self,
        status_filter: Optional[QueueStatus] = None,
        user_filter: Optional[str] = None,
        tag_filter: Optional[Set[str]] = None
    ):
        """List operations with optional filters."""
        return self._queue.list_operations(
            status_filter=status_filter,
            user_filter=user_filter,
            tag_filter=tag_filter
        )
    
    def get_queue_stats(self) -> Dict[str, Any]:
        """Get queue statistics."""
        return self._queue.get_queue_stats()
    
    def pause_queue(self) -> None:
        """Pause queue processing."""
        self._queue.pause_queue()
        self._logger.info("Queue processing paused")
    
    def resume_queue(self) -> None:
        """Resume queue processing."""
        self._queue.resume_queue()
        self._logger.info("Queue processing resumed")
    
    def stop_queue(self) -> None:
        """Stop queue processing."""
        self._queue.stop_queue()
        self._logger.info("Queue processing stopped")
    
    def cancel_operation(self, operation_id: str) -> bool:
        """Cancel a queued or running operation."""
        result = self._queue.cancel_operation(operation_id)
        if result:
            self._logger.info(f"Cancelled operation {operation_id}")
        else:
            self._logger.warning(f"Failed to cancel operation {operation_id}")
        return result
    
    def clear_completed(self) -> int:
        """Clear completed and failed operations from memory."""
        cleared_count = self._queue.clear_completed()
        self._logger.info(f"Cleared {cleared_count} completed operations")
        return cleared_count
    
    @property
    def is_processing(self) -> bool:
        """Check if queue is currently processing operations."""
        stats = self.get_queue_stats()
        return stats.get('is_processing', False)
    
    @property
    def is_paused(self) -> bool:
        """Check if queue is paused."""
        stats = self.get_queue_stats()
        return stats.get('is_paused', False)
    
    @property
    def queue_length(self) -> int:
        """Get current queue length."""
        stats = self.get_queue_stats()
        return stats.get('queue_length', 0)
    
    @property
    def running_operations(self) -> int:
        """Get number of currently running operations."""
        stats = self.get_queue_stats()
        return stats.get('running', 0)