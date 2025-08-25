"""
Generic Operation Queue Management Service.

Manages queued operations for archive management and file transfers, providing
progress tracking, priority management, and concurrent execution.
"""

import asyncio
import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Callable, Union
import uuid

from ..dtos import (
    BulkArchiveOperationDto, 
    BulkOperationResultDto,
    FileTransferOperationDto,
    BatchFileTransferOperationDto,
    DirectoryTransferOperationDto,
    FileTransferResultDto,
    BatchFileTransferResultDto,
)
from .operation_handler import OperationDto, OperationResultDto, OperationRouter

logger = logging.getLogger(__name__)


class QueueStatus(Enum):
    """Status of a queue operation."""
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    PAUSED = "paused"


class QueuePriority(Enum):
    """Priority levels for queue operations."""
    LOW = 1
    NORMAL = 2
    HIGH = 3
    URGENT = 4


@dataclass
class QueuedOperation:
    """Represents a queued operation (archive or file transfer)."""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    operation_dto: OperationDto = None
    priority: QueuePriority = QueuePriority.NORMAL
    status: QueueStatus = QueueStatus.QUEUED
    created_time: float = field(default_factory=time.time)
    started_time: Optional[float] = None
    completed_time: Optional[float] = None
    result: Optional[OperationResultDto] = None
    error_message: Optional[str] = None
    progress_callback: Optional[Callable[[str, Dict[str, Any]], None]] = None
    tags: Set[str] = field(default_factory=set)
    user_id: Optional[str] = None
    
    @property
    def duration(self) -> Optional[float]:
        """Calculate operation duration."""
        if self.started_time and self.completed_time:
            return self.completed_time - self.started_time
        elif self.started_time:
            return time.time() - self.started_time
        return None


class GenericOperationQueue:
    """Manages a queue of operations (archive and file transfer) with priority and concurrency control."""
    
    def __init__(self, 
                 operation_router: Optional[OperationRouter] = None,
                 max_concurrent: int = 3,
                 default_priority: QueuePriority = QueuePriority.NORMAL):
        self.operation_router = operation_router or OperationRouter()
        self.max_concurrent = max_concurrent
        self.default_priority = default_priority
        
        # Queue state
        self._operations: Dict[str, QueuedOperation] = {}
        self._queue_order: List[str] = []  # Order of operations in queue
        self._running_operations: Set[str] = set()
        self._completed_operations: List[str] = []
        self._failed_operations: List[str] = []
        
        # Control flags
        self._is_processing = False
        self._is_paused = False
        self._should_stop = False
        
        # Statistics
        self._total_processed = 0
        self._total_failed = 0
        self._total_bytes_processed = 0
        
    async def add_operation(self, 
                           operation_dto: OperationDto,
                           priority: QueuePriority = None,
                           tags: Set[str] = None,
                           user_id: Optional[str] = None,
                           progress_callback: Optional[Callable[[str, Dict[str, Any]], None]] = None) -> str:
        """Add an operation to the queue."""
        queued_op = QueuedOperation(
            operation_dto=operation_dto,
            priority=priority or self.default_priority,
            tags=tags or set(),
            user_id=user_id,
            progress_callback=progress_callback
        )
        
        self._operations[queued_op.id] = queued_op
        self._insert_by_priority(queued_op.id)
        
        operation_type = getattr(operation_dto, 'operation_type', 'unknown')
        logger.info(f"Added {operation_type} operation {queued_op.id} to queue with priority {queued_op.priority.name}")
        
        # Start processing if not already running
        if not self._is_processing and not self._is_paused:
            asyncio.create_task(self.process_queue())
            
        return queued_op.id
    
    def _insert_by_priority(self, operation_id: str) -> None:
        """Insert operation into queue maintaining priority order."""
        operation = self._operations[operation_id]
        
        # Find insertion point based on priority
        insert_index = len(self._queue_order)
        for i, existing_id in enumerate(self._queue_order):
            existing_op = self._operations[existing_id]
            if operation.priority.value > existing_op.priority.value:
                insert_index = i
                break
                
        self._queue_order.insert(insert_index, operation_id)
    
    async def process_queue(self) -> None:
        """Main queue processing loop."""
        if self._is_processing:
            return
            
        self._is_processing = True
        logger.info("Starting bulk operation queue processing")
        
        try:
            while not self._should_stop and self._queue_order:
                if self._is_paused:
                    await asyncio.sleep(1)
                    continue
                    
                # Check if we can start more operations
                if len(self._running_operations) >= self.max_concurrent:
                    await asyncio.sleep(0.5)
                    continue
                
                # Get next operation to process
                operation_id = self._get_next_operation()
                if not operation_id:
                    break
                    
                # Start processing the operation
                asyncio.create_task(self._process_single_operation(operation_id))
                await asyncio.sleep(0.1)  # Brief pause to prevent overwhelming
                
        except Exception as e:
            logger.error(f"Queue processing error: {e}")
        finally:
            # Wait for all running operations to complete
            while self._running_operations:
                await asyncio.sleep(0.5)
                
            self._is_processing = False
            logger.info("Queue processing stopped")
    
    def _get_next_operation(self) -> Optional[str]:
        """Get the next operation to process from the queue."""
        for operation_id in self._queue_order:
            operation = self._operations[operation_id]
            if operation.status == QueueStatus.QUEUED:
                return operation_id
        return None
    
    async def _process_single_operation(self, operation_id: str) -> None:
        """Process a single bulk operation."""
        operation = self._operations[operation_id]
        self._running_operations.add(operation_id)
        
        try:
            # Update status
            operation.status = QueueStatus.RUNNING
            operation.started_time = time.time()
            
            operation_type = getattr(operation.operation_dto, 'operation_type', 'unknown')
            logger.info(f"Starting {operation_type} operation {operation_id}")
            
            # Notify progress callback
            if operation.progress_callback:
                callback_data = {
                    "status": "started",
                    "operation_type": operation_type,
                }
                
                # Add operation-specific data
                if hasattr(operation.operation_dto, 'archive_ids'):
                    callback_data["archive_count"] = len(operation.operation_dto.archive_ids)
                elif hasattr(operation.operation_dto, 'transfers'):
                    callback_data["transfer_count"] = len(operation.operation_dto.transfers)
                
                operation.progress_callback(operation_id, callback_data)
            
            # Execute the operation using the router
            result = await self.operation_router.execute_operation(operation.operation_dto)
            operation.result = result
            
            # Update statistics based on result type
            if hasattr(result, 'total_bytes_processed'):
                self._total_bytes_processed += result.total_bytes_processed
            elif hasattr(result, 'total_bytes_transferred'):
                self._total_bytes_processed += result.total_bytes_transferred
            elif hasattr(result, 'bytes_transferred'):
                self._total_bytes_processed += result.bytes_transferred
            
            # Determine success/failure
            operation_failed = False
            if hasattr(result, 'failed_operations') and result.failed_operations:
                operation_failed = True
                operation.error_message = f"Failed operations: {len(result.failed_operations)}"
            elif hasattr(result, 'failed_transfers') and result.failed_transfers:
                operation_failed = True
                operation.error_message = f"Failed transfers: {len(result.failed_transfers)}"
            elif hasattr(result, 'success') and not result.success:
                operation_failed = True
                operation.error_message = getattr(result, 'error_message', 'Operation failed')
            
            if operation_failed:
                operation.status = QueueStatus.FAILED
                self._failed_operations.append(operation_id)
                self._total_failed += 1
            else:
                operation.status = QueueStatus.COMPLETED
                self._completed_operations.append(operation_id)
                self._total_processed += 1
                
            logger.info(f"{operation_type} operation {operation_id} completed successfully")
                
        except Exception as e:
            operation.status = QueueStatus.FAILED
            operation.error_message = str(e)
            self._failed_operations.append(operation_id)
            self._total_failed += 1
            logger.error(f"Bulk operation {operation_id} failed: {e}")
            
        finally:
            operation.completed_time = time.time()
            self._running_operations.remove(operation_id)
            
            # Remove from queue order
            if operation_id in self._queue_order:
                self._queue_order.remove(operation_id)
            
            # Notify progress callback
            if operation.progress_callback:
                operation.progress_callback(operation_id, {
                    "status": operation.status.value,
                    "result": operation.result,
                    "error": operation.error_message,
                    "duration": operation.duration
                })
    
    def pause_queue(self) -> None:
        """Pause queue processing."""
        self._is_paused = True
        logger.info("Queue processing paused")
    
    def resume_queue(self) -> None:
        """Resume queue processing."""
        self._is_paused = False
        logger.info("Queue processing resumed")
        
        # Restart processing if needed
        if not self._is_processing and self._queue_order:
            asyncio.create_task(self.process_queue())
    
    def stop_queue(self) -> None:
        """Stop queue processing."""
        self._should_stop = True
        logger.info("Queue processing stopping")
    
    def cancel_operation(self, operation_id: str) -> bool:
        """Cancel a queued or running operation."""
        if operation_id not in self._operations:
            return False
            
        operation = self._operations[operation_id]
        
        if operation.status == QueueStatus.QUEUED:
            operation.status = QueueStatus.CANCELLED
            if operation_id in self._queue_order:
                self._queue_order.remove(operation_id)
            logger.info(f"Cancelled queued operation {operation_id}")
            return True
        elif operation.status == QueueStatus.RUNNING:
            # Mark for cancellation (actual cancellation depends on operation support)
            operation.status = QueueStatus.CANCELLED
            logger.info(f"Marked running operation {operation_id} for cancellation")
            return True
        
        return False
    
    def get_operation_status(self, operation_id: str) -> Optional[QueuedOperation]:
        """Get status of a specific operation."""
        return self._operations.get(operation_id)
    
    def list_operations(self, 
                       status_filter: Optional[QueueStatus] = None,
                       user_filter: Optional[str] = None,
                       tag_filter: Optional[Set[str]] = None) -> List[QueuedOperation]:
        """List operations with optional filters."""
        operations = list(self._operations.values())
        
        if status_filter:
            operations = [op for op in operations if op.status == status_filter]
        if user_filter:
            operations = [op for op in operations if op.user_id == user_filter]
        if tag_filter:
            operations = [op for op in operations if tag_filter.intersection(op.tags)]
            
        return sorted(operations, key=lambda x: x.created_time, reverse=True)
    
    def get_queue_stats(self) -> Dict[str, Any]:
        """Get queue statistics."""
        return {
            "total_operations": len(self._operations),
            "queued": len([op for op in self._operations.values() if op.status == QueueStatus.QUEUED]),
            "running": len(self._running_operations),
            "completed": len(self._completed_operations),
            "failed": len(self._failed_operations),
            "cancelled": len([op for op in self._operations.values() if op.status == QueueStatus.CANCELLED]),
            "total_processed": self._total_processed,
            "total_failed": self._total_failed,
            "total_bytes_processed": self._total_bytes_processed,
            "is_processing": self._is_processing,
            "is_paused": self._is_paused,
            "max_concurrent": self.max_concurrent,
            "queue_length": len(self._queue_order)
        }
    
    def clear_completed(self) -> int:
        """Clear completed and failed operations from memory."""
        to_remove = []
        for op_id, operation in self._operations.items():
            if operation.status in [QueueStatus.COMPLETED, QueueStatus.FAILED, QueueStatus.CANCELLED]:
                to_remove.append(op_id)
        
        for op_id in to_remove:
            del self._operations[op_id]
            if op_id in self._completed_operations:
                self._completed_operations.remove(op_id)
            if op_id in self._failed_operations:
                self._failed_operations.remove(op_id)
                
        logger.info(f"Cleared {len(to_remove)} completed operations from queue")
        return len(to_remove)


# Backward compatibility alias
BulkOperationQueue = GenericOperationQueue