"""Advanced operation queue management for the Tellus TUI."""

import asyncio
from typing import Dict, List, Optional, Any, Callable, Set
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
import threading
import queue
import json
from pathlib import Path

from .managers import OperationStatus, OperationInfo, OperationManager


class QueuePriority(Enum):
    """Queue priority modes."""
    FIFO = "fifo"  # First In, First Out
    LIFO = "lifo"  # Last In, First Out
    SIZE_ASC = "size_asc"  # Smallest first
    SIZE_DESC = "size_desc"  # Largest first
    PRIORITY = "priority"  # User-defined priority


@dataclass
class QueuedOperation:
    """Represents an operation in the queue."""
    operation_info: OperationInfo
    priority: int = 0
    dependencies: Set[str] = field(default_factory=set)
    retry_count: int = 0
    max_retries: int = 3
    scheduled_time: Optional[datetime] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


class AdvancedOperationQueue:
    """Advanced operation queue with scheduling, dependencies, and batch processing."""
    
    def __init__(self, max_concurrent: int = 3):
        """Initialize the operation queue."""
        self.max_concurrent = max_concurrent
        self.queued_operations: Dict[str, QueuedOperation] = {}
        self.active_operations: Dict[str, QueuedOperation] = {}
        self.completed_operations: Dict[str, QueuedOperation] = {}
        
        self.priority_mode = QueuePriority.FIFO
        self.auto_retry = True
        self.continue_on_error = True
        
        self.operation_manager = OperationManager()
        self.operation_manager.max_concurrent = max_concurrent
        
        # Queue control
        self._queue_active = False
        self._queue_paused = False
        self._scheduler_thread: Optional[threading.Thread] = None
        self._stop_scheduler = False
        
        # Statistics
        self.stats = {
            'total_queued': 0,
            'total_completed': 0,
            'total_failed': 0,
            'total_cancelled': 0,
            'queue_start_time': None,
            'average_completion_time': 0.0
        }
        
        # Callbacks
        self.status_callbacks: List[Callable[[str, QueuedOperation], None]] = []
        
    def add_status_callback(self, callback: Callable[[str, QueuedOperation], None]) -> None:
        """Add a callback for queue status updates."""
        self.status_callbacks.append(callback)
        
    def _notify_status_change(self, operation_id: str, queued_op: QueuedOperation) -> None:
        """Notify callbacks of status changes."""
        for callback in self.status_callbacks:
            try:
                callback(operation_id, queued_op)
            except Exception:
                pass
                
    def queue_operation(self, 
                       operation_type: str,
                       archive_id: str,
                       source_location: str,
                       destination_location: str,
                       priority: int = 0,
                       dependencies: Optional[Set[str]] = None,
                       scheduled_time: Optional[datetime] = None,
                       **kwargs) -> str:
        """Queue an operation with advanced options."""
        
        operation_id = f"{operation_type}_{archive_id}_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}"
        
        operation_info = OperationInfo(
            id=operation_id,
            type=operation_type,
            archive_id=archive_id,
            source_location=source_location,
            destination_location=destination_location,
            status=OperationStatus.PENDING
        )
        
        queued_op = QueuedOperation(
            operation_info=operation_info,
            priority=priority,
            dependencies=dependencies or set(),
            scheduled_time=scheduled_time,
            metadata=kwargs
        )
        
        self.queued_operations[operation_id] = queued_op
        self.stats['total_queued'] += 1
        
        self._notify_status_change(operation_id, queued_op)
        
        # Start scheduler if not running
        if not self._scheduler_thread or not self._scheduler_thread.is_alive():
            self.start_queue_processing()
            
        return operation_id
        
    def queue_bulk_operations(self, operations: List[Dict[str, Any]]) -> List[str]:
        """Queue multiple operations as a batch."""
        operation_ids = []
        
        for op_config in operations:
            op_id = self.queue_operation(**op_config)
            operation_ids.append(op_id)
            
        return operation_ids
        
    def add_dependency(self, operation_id: str, dependency_id: str) -> bool:
        """Add a dependency between operations."""
        if operation_id in self.queued_operations:
            self.queued_operations[operation_id].dependencies.add(dependency_id)
            return True
        return False
        
    def remove_dependency(self, operation_id: str, dependency_id: str) -> bool:
        """Remove a dependency between operations."""
        if operation_id in self.queued_operations:
            self.queued_operations[operation_id].dependencies.discard(dependency_id)
            return True
        return False
        
    def set_priority_mode(self, mode: QueuePriority) -> None:
        """Set the queue priority mode."""
        self.priority_mode = mode
        
    def start_queue_processing(self) -> None:
        """Start processing the queue."""
        if self._scheduler_thread and self._scheduler_thread.is_alive():
            return
            
        self._queue_active = True
        self._queue_paused = False
        self._stop_scheduler = False
        
        self.stats['queue_start_time'] = datetime.now()
        
        self._scheduler_thread = threading.Thread(target=self._queue_scheduler, daemon=True)
        self._scheduler_thread.start()
        
        # Also start the operation manager
        self.operation_manager.start_monitoring()
        
    def pause_queue_processing(self) -> None:
        """Pause queue processing."""
        self._queue_paused = True
        
    def resume_queue_processing(self) -> None:
        """Resume queue processing."""
        self._queue_paused = False
        
    def stop_queue_processing(self) -> None:
        """Stop queue processing."""
        self._queue_active = False
        self._stop_scheduler = True
        
        if self._scheduler_thread:
            self._scheduler_thread.join(timeout=5.0)
            
        self.operation_manager.stop_monitoring()
        
    def _queue_scheduler(self) -> None:
        """Main queue scheduler loop."""
        while self._queue_active and not self._stop_scheduler:
            try:
                if not self._queue_paused:
                    self._process_ready_operations()
                    self._check_completed_operations()
                    self._handle_failed_operations()
                    
                threading.Event().wait(1.0)
                
            except Exception:
                threading.Event().wait(5.0)
                
    def _process_ready_operations(self) -> None:
        """Process operations that are ready to run."""
        if len(self.active_operations) >= self.max_concurrent:
            return
            
        ready_ops = self._get_ready_operations()
        
        for operation_id in ready_ops:
            if len(self.active_operations) >= self.max_concurrent:
                break
                
            queued_op = self.queued_operations.pop(operation_id)
            self._start_operation(operation_id, queued_op)
            
    def _get_ready_operations(self) -> List[str]:
        """Get operations that are ready to run."""
        ready_ops = []
        
        for operation_id, queued_op in self.queued_operations.items():
            # Check if dependencies are satisfied
            if not self._dependencies_satisfied(queued_op):
                continue
                
            # Check if scheduled time has passed
            if queued_op.scheduled_time and datetime.now() < queued_op.scheduled_time:
                continue
                
            ready_ops.append(operation_id)
            
        # Sort by priority mode
        ready_ops.sort(key=lambda op_id: self._get_sort_key(self.queued_operations[op_id]))
        
        return ready_ops
        
    def _dependencies_satisfied(self, queued_op: QueuedOperation) -> bool:
        """Check if all dependencies for an operation are satisfied."""
        for dep_id in queued_op.dependencies:
            # Dependency is satisfied if it's completed successfully
            if dep_id not in self.completed_operations:
                return False
            if self.completed_operations[dep_id].operation_info.status != OperationStatus.COMPLETED:
                return False
        return True
        
    def _get_sort_key(self, queued_op: QueuedOperation) -> Any:
        """Get sort key based on priority mode."""
        if self.priority_mode == QueuePriority.FIFO:
            return queued_op.operation_info.start_time or datetime.now()
        elif self.priority_mode == QueuePriority.LIFO:
            return -(queued_op.operation_info.start_time or datetime.now()).timestamp()
        elif self.priority_mode == QueuePriority.SIZE_ASC:
            return queued_op.operation_info.total_bytes
        elif self.priority_mode == QueuePriority.SIZE_DESC:
            return -queued_op.operation_info.total_bytes
        elif self.priority_mode == QueuePriority.PRIORITY:
            return -queued_op.priority  # Higher priority first
        else:
            return 0
            
    def _start_operation(self, operation_id: str, queued_op: QueuedOperation) -> None:
        """Start an operation."""
        # Move to active operations
        self.active_operations[operation_id] = queued_op
        
        # Update status
        queued_op.operation_info.status = OperationStatus.RUNNING
        queued_op.operation_info.start_time = datetime.now()
        
        # Start via operation manager
        self.operation_manager.operations[operation_id] = queued_op.operation_info
        self.operation_manager.active_operations.append(operation_id)
        
        # Set up callback for completion
        def operation_completed(op_id: str, op_info: OperationInfo) -> None:
            if op_id == operation_id:
                self._handle_operation_completion(op_id, op_info)
                
        self.operation_manager.add_status_callback(operation_completed)
        
        self._notify_status_change(operation_id, queued_op)
        
        # Start the actual operation
        operation_thread = threading.Thread(
            target=self._execute_operation_wrapper,
            args=(operation_id, queued_op),
            daemon=True
        )
        operation_thread.start()
        
    def _execute_operation_wrapper(self, operation_id: str, queued_op: QueuedOperation) -> None:
        """Wrapper for operation execution."""
        try:
            # Execute the operation
            self.operation_manager._execute_operation(operation_id, queued_op.metadata)
        except Exception as e:
            # Handle operation errors
            queued_op.operation_info.status = OperationStatus.FAILED
            queued_op.operation_info.error_message = str(e)
            queued_op.operation_info.end_time = datetime.now()
            
            self._handle_operation_completion(operation_id, queued_op.operation_info)
            
    def _handle_operation_completion(self, operation_id: str, operation_info: OperationInfo) -> None:
        """Handle completion of an operation."""
        if operation_id not in self.active_operations:
            return
            
        queued_op = self.active_operations.pop(operation_id)
        queued_op.operation_info = operation_info
        
        # Move to completed operations
        self.completed_operations[operation_id] = queued_op
        
        # Update statistics
        if operation_info.status == OperationStatus.COMPLETED:
            self.stats['total_completed'] += 1
            self._update_completion_time_stats(queued_op)
        elif operation_info.status == OperationStatus.FAILED:
            self.stats['total_failed'] += 1
        elif operation_info.status == OperationStatus.CANCELLED:
            self.stats['total_cancelled'] += 1
            
        self._notify_status_change(operation_id, queued_op)
        
    def _update_completion_time_stats(self, queued_op: QueuedOperation) -> None:
        """Update average completion time statistics."""
        if queued_op.operation_info.start_time and queued_op.operation_info.end_time:
            duration = (queued_op.operation_info.end_time - queued_op.operation_info.start_time).total_seconds()
            
            # Update running average
            total_completed = self.stats['total_completed']
            current_avg = self.stats['average_completion_time']
            new_avg = ((current_avg * (total_completed - 1)) + duration) / total_completed
            self.stats['average_completion_time'] = new_avg
            
    def _check_completed_operations(self) -> None:
        """Check for operations that have completed."""
        # This is handled by the operation completion callback
        pass
        
    def _handle_failed_operations(self) -> None:
        """Handle failed operations with retry logic."""
        if not self.auto_retry:
            return
            
        failed_ops = [
            (op_id, queued_op) for op_id, queued_op in self.completed_operations.items()
            if (queued_op.operation_info.status == OperationStatus.FAILED and 
                queued_op.retry_count < queued_op.max_retries)
        ]
        
        for operation_id, queued_op in failed_ops:
            # Remove from completed and re-queue
            del self.completed_operations[operation_id]
            
            # Reset status and increment retry count
            queued_op.operation_info.status = OperationStatus.PENDING
            queued_op.operation_info.start_time = None
            queued_op.operation_info.end_time = None
            queued_op.operation_info.error_message = ""
            queued_op.retry_count += 1
            
            # Add delay before retry
            retry_delay = min(60 * (2 ** queued_op.retry_count), 600)  # Exponential backoff
            queued_op.scheduled_time = datetime.now() + timedelta(seconds=retry_delay)
            
            self.queued_operations[operation_id] = queued_op
            self._notify_status_change(operation_id, queued_op)
            
    def cancel_operation(self, operation_id: str) -> bool:
        """Cancel an operation."""
        # Check if it's queued
        if operation_id in self.queued_operations:
            queued_op = self.queued_operations.pop(operation_id)
            queued_op.operation_info.status = OperationStatus.CANCELLED
            queued_op.operation_info.end_time = datetime.now()
            self.completed_operations[operation_id] = queued_op
            self.stats['total_cancelled'] += 1
            self._notify_status_change(operation_id, queued_op)
            return True
            
        # Check if it's active
        if operation_id in self.active_operations:
            return self.operation_manager.cancel_operation(operation_id)
            
        return False
        
    def cancel_all_operations(self) -> int:
        """Cancel all queued and active operations."""
        cancelled_count = 0
        
        # Cancel queued operations
        queued_ids = list(self.queued_operations.keys())
        for operation_id in queued_ids:
            if self.cancel_operation(operation_id):
                cancelled_count += 1
                
        # Cancel active operations
        active_ids = list(self.active_operations.keys())
        for operation_id in active_ids:
            if self.cancel_operation(operation_id):
                cancelled_count += 1
                
        return cancelled_count
        
    def get_queue_status(self) -> Dict[str, Any]:
        """Get current queue status."""
        return {
            'queue_active': self._queue_active,
            'queue_paused': self._queue_paused,
            'max_concurrent': self.max_concurrent,
            'priority_mode': self.priority_mode.value,
            'queued_count': len(self.queued_operations),
            'active_count': len(self.active_operations),
            'completed_count': len(self.completed_operations),
            'statistics': self.stats.copy()
        }
        
    def get_all_operations(self) -> Dict[str, QueuedOperation]:
        """Get all operations in the queue system."""
        all_ops = {}
        all_ops.update(self.queued_operations)
        all_ops.update(self.active_operations)
        all_ops.update(self.completed_operations)
        return all_ops
        
    def clear_completed_operations(self) -> int:
        """Clear completed operations from memory."""
        count = len(self.completed_operations)
        self.completed_operations.clear()
        return count
        
    def save_queue_state(self, filepath: Path) -> bool:
        """Save current queue state to file."""
        try:
            state = {
                'timestamp': datetime.now().isoformat(),
                'max_concurrent': self.max_concurrent,
                'priority_mode': self.priority_mode.value,
                'auto_retry': self.auto_retry,
                'continue_on_error': self.continue_on_error,
                'statistics': self.stats,
                'operations': {}
            }
            
            # Serialize operations
            for op_id, queued_op in self.get_all_operations().items():
                state['operations'][op_id] = {
                    'operation_info': {
                        'id': queued_op.operation_info.id,
                        'type': queued_op.operation_info.type,
                        'archive_id': queued_op.operation_info.archive_id,
                        'source_location': queued_op.operation_info.source_location,
                        'destination_location': queued_op.operation_info.destination_location,
                        'status': queued_op.operation_info.status.value,
                        'progress': queued_op.operation_info.progress,
                        'current_step': queued_op.operation_info.current_step,
                        'error_message': queued_op.operation_info.error_message,
                        'files_processed': queued_op.operation_info.files_processed,
                        'bytes_processed': queued_op.operation_info.bytes_processed,
                        'total_files': queued_op.operation_info.total_files,
                        'total_bytes': queued_op.operation_info.total_bytes,
                        'transfer_rate': queued_op.operation_info.transfer_rate,
                    },
                    'priority': queued_op.priority,
                    'dependencies': list(queued_op.dependencies),
                    'retry_count': queued_op.retry_count,
                    'max_retries': queued_op.max_retries,
                    'metadata': queued_op.metadata
                }
                
                # Handle datetime serialization
                if queued_op.operation_info.start_time:
                    state['operations'][op_id]['operation_info']['start_time'] = \
                        queued_op.operation_info.start_time.isoformat()
                if queued_op.operation_info.end_time:
                    state['operations'][op_id]['operation_info']['end_time'] = \
                        queued_op.operation_info.end_time.isoformat()
                if queued_op.scheduled_time:
                    state['operations'][op_id]['scheduled_time'] = \
                        queued_op.scheduled_time.isoformat()
            
            with open(filepath, 'w') as f:
                json.dump(state, f, indent=2)
                
            return True
            
        except Exception:
            return False
            
    def load_queue_state(self, filepath: Path) -> bool:
        """Load queue state from file."""
        try:
            if not filepath.exists():
                return False
                
            with open(filepath, 'r') as f:
                state = json.load(f)
                
            # Restore configuration
            self.max_concurrent = state.get('max_concurrent', 3)
            self.priority_mode = QueuePriority(state.get('priority_mode', 'fifo'))
            self.auto_retry = state.get('auto_retry', True)
            self.continue_on_error = state.get('continue_on_error', True)
            self.stats = state.get('statistics', {})
            
            # Clear existing operations
            self.queued_operations.clear()
            self.active_operations.clear()
            self.completed_operations.clear()
            
            # Restore operations
            for op_id, op_data in state.get('operations', {}).items():
                # Recreate operation info
                op_info_data = op_data['operation_info']
                operation_info = OperationInfo(
                    id=op_info_data['id'],
                    type=op_info_data['type'],
                    archive_id=op_info_data['archive_id'],
                    source_location=op_info_data['source_location'],
                    destination_location=op_info_data['destination_location'],
                    status=OperationStatus(op_info_data['status']),
                    progress=op_info_data.get('progress', 0.0),
                    current_step=op_info_data.get('current_step', ''),
                    error_message=op_info_data.get('error_message', ''),
                    files_processed=op_info_data.get('files_processed', 0),
                    bytes_processed=op_info_data.get('bytes_processed', 0),
                    total_files=op_info_data.get('total_files', 0),
                    total_bytes=op_info_data.get('total_bytes', 0),
                    transfer_rate=op_info_data.get('transfer_rate', 0.0),
                )
                
                # Handle datetime deserialization
                if 'start_time' in op_info_data and op_info_data['start_time']:
                    operation_info.start_time = datetime.fromisoformat(op_info_data['start_time'])
                if 'end_time' in op_info_data and op_info_data['end_time']:
                    operation_info.end_time = datetime.fromisoformat(op_info_data['end_time'])
                
                # Recreate queued operation
                scheduled_time = None
                if 'scheduled_time' in op_data and op_data['scheduled_time']:
                    scheduled_time = datetime.fromisoformat(op_data['scheduled_time'])
                    
                queued_op = QueuedOperation(
                    operation_info=operation_info,
                    priority=op_data.get('priority', 0),
                    dependencies=set(op_data.get('dependencies', [])),
                    retry_count=op_data.get('retry_count', 0),
                    max_retries=op_data.get('max_retries', 3),
                    scheduled_time=scheduled_time,
                    metadata=op_data.get('metadata', {})
                )
                
                # Place in appropriate collection based on status
                if operation_info.status == OperationStatus.PENDING:
                    self.queued_operations[op_id] = queued_op
                elif operation_info.status == OperationStatus.RUNNING:
                    # Running operations should be reset to pending on load
                    operation_info.status = OperationStatus.PENDING
                    self.queued_operations[op_id] = queued_op
                else:
                    self.completed_operations[op_id] = queued_op
                    
            return True
            
        except Exception:
            return False