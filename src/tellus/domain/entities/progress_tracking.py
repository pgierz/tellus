"""
Domain entities for progress tracking.

This module defines pure business objects for tracking progress of long-running operations
in the Tellus system, following clean architecture principles.
"""

import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional, Set


class OperationStatus(Enum):
    """Status of a tracked operation."""
    PENDING = "pending"
    INITIALIZING = "initializing"
    RUNNING = "running"
    PAUSED = "paused"
    CANCELLING = "cancelling"
    CANCELLED = "cancelled"
    COMPLETED = "completed"
    FAILED = "failed"
    
    def is_active(self) -> bool:
        """Check if the operation is in an active state."""
        return self in {OperationStatus.PENDING, OperationStatus.INITIALIZING, 
                       OperationStatus.RUNNING, OperationStatus.PAUSED}
    
    def is_terminal(self) -> bool:
        """Check if the operation is in a terminal state."""
        return self in {OperationStatus.CANCELLED, OperationStatus.COMPLETED, 
                       OperationStatus.FAILED}
    
    def can_cancel(self) -> bool:
        """Check if the operation can be cancelled."""
        return self in {OperationStatus.PENDING, OperationStatus.INITIALIZING, 
                       OperationStatus.RUNNING, OperationStatus.PAUSED}
    
    def can_pause(self) -> bool:
        """Check if the operation can be paused."""
        return self == OperationStatus.RUNNING
    
    def can_resume(self) -> bool:
        """Check if the operation can be resumed."""
        return self == OperationStatus.PAUSED


class OperationType(Enum):
    """Types of operations that can be tracked."""
    ARCHIVE_COPY = "archive_copy"
    ARCHIVE_MOVE = "archive_move" 
    ARCHIVE_EXTRACT = "archive_extract"
    ARCHIVE_CREATE = "archive_create"
    ARCHIVE_COMPRESS = "archive_compress"
    BULK_ARCHIVE_OPERATION = "bulk_archive_operation"
    FILE_TRANSFER = "file_transfer"
    FILE_HASH = "file_hash"
    DVC_OPERATION = "dvc_operation"
    WORKFLOW_EXECUTION = "workflow_execution"
    SIMULATION_ANALYSIS = "simulation_analysis"
    DATA_VALIDATION = "data_validation"
    CUSTOM = "custom"


class Priority(Enum):
    """Priority levels for operations."""
    LOW = 1
    NORMAL = 2
    HIGH = 3
    URGENT = 4
    CRITICAL = 5
    
    def __lt__(self, other):
        if isinstance(other, Priority):
            return self.value < other.value
        return NotImplemented


@dataclass(frozen=True)
class ProgressMetrics:
    """Value object for progress metrics."""
    percentage: float = 0.0  # 0.0 to 100.0
    current_value: int = 0
    total_value: Optional[int] = None
    bytes_processed: int = 0
    total_bytes: Optional[int] = None
    files_processed: int = 0
    total_files: Optional[int] = None
    operations_completed: int = 0
    total_operations: Optional[int] = None
    
    def __post_init__(self):
        """Validate progress metrics."""
        if not 0.0 <= self.percentage <= 100.0:
            raise ValueError(f"Percentage must be between 0.0 and 100.0, got {self.percentage}")
        if self.current_value < 0:
            raise ValueError(f"Current value must be non-negative, got {self.current_value}")
        if self.total_value is not None and self.current_value > self.total_value:
            raise ValueError(f"Current value {self.current_value} exceeds total {self.total_value}")
    
    @property
    def is_complete(self) -> bool:
        """Check if progress indicates completion."""
        return self.percentage >= 100.0
    
    @property
    def bytes_remaining(self) -> Optional[int]:
        """Calculate remaining bytes to process."""
        if self.total_bytes is None:
            return None
        return max(0, self.total_bytes - self.bytes_processed)
    
    @property
    def completion_ratio(self) -> float:
        """Get completion ratio as 0.0 to 1.0."""
        return self.percentage / 100.0


@dataclass(frozen=True)
class ThroughputMetrics:
    """Value object for throughput and timing metrics."""
    start_time: float
    current_time: Optional[float] = None
    bytes_per_second: float = 0.0
    files_per_second: float = 0.0
    operations_per_second: float = 0.0
    estimated_completion_time: Optional[float] = None
    estimated_remaining_seconds: Optional[float] = None
    
    @property
    def elapsed_seconds(self) -> float:
        """Calculate elapsed time in seconds."""
        current = self.current_time or time.time()
        return max(0.0, current - self.start_time)
    
    @property
    def estimated_total_duration(self) -> Optional[float]:
        """Calculate estimated total duration in seconds."""
        if self.estimated_completion_time is None:
            return None
        return self.estimated_completion_time - self.start_time
    
    def calculate_eta(self, progress_metrics: ProgressMetrics) -> Optional[float]:
        """Calculate ETA based on current progress."""
        if progress_metrics.percentage <= 0.0:
            return None
        
        elapsed = self.elapsed_seconds
        if elapsed <= 0.0:
            return None
        
        completion_ratio = progress_metrics.completion_ratio
        if completion_ratio <= 0.0:
            return None
        
        total_estimated = elapsed / completion_ratio
        return self.start_time + total_estimated


@dataclass
class ProgressLogEntry:
    """Individual progress log entry."""
    timestamp: float = field(default_factory=time.time)
    message: str = ""
    level: str = "INFO"  # INFO, WARN, ERROR, DEBUG
    metrics: Optional[ProgressMetrics] = None
    throughput: Optional[ThroughputMetrics] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    @property
    def datetime_utc(self) -> datetime:
        """Get timestamp as UTC datetime."""
        return datetime.fromtimestamp(self.timestamp, tz=timezone.utc)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            "timestamp": self.timestamp,
            "datetime": self.datetime_utc.isoformat(),
            "message": self.message,
            "level": self.level,
            "metrics": self.metrics,
            "throughput": self.throughput,
            "metadata": self.metadata
        }


@dataclass
class OperationContext:
    """Context information for an operation."""
    user_id: Optional[str] = None
    session_id: Optional[str] = None
    parent_operation_id: Optional[str] = None
    simulation_id: Optional[str] = None
    location_name: Optional[str] = None
    workflow_id: Optional[str] = None
    tags: Set[str] = field(default_factory=set)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def add_tag(self, tag: str) -> None:
        """Add a tag to the context."""
        self.tags.add(tag)
    
    def remove_tag(self, tag: str) -> None:
        """Remove a tag from the context."""
        self.tags.discard(tag)
    
    def has_tag(self, tag: str) -> bool:
        """Check if context has a specific tag."""
        return tag in self.tags


class ProgressTrackingEntity:
    """
    Domain entity for tracking progress of long-running operations.
    
    This entity encapsulates all business logic related to progress tracking
    without any infrastructure dependencies.
    """
    
    def __init__(
        self,
        operation_id: str,
        operation_type: OperationType,
        operation_name: str,
        priority: Priority = Priority.NORMAL,
        context: Optional[OperationContext] = None
    ):
        """Initialize a new progress tracking entity."""
        self._operation_id = operation_id
        self._operation_type = operation_type
        self._operation_name = operation_name
        self._priority = priority
        self._context = context or OperationContext()
        
        # State
        self._status = OperationStatus.PENDING
        self._created_time = time.time()
        self._started_time: Optional[float] = None
        self._completed_time: Optional[float] = None
        self._last_update_time = self._created_time
        
        # Progress tracking
        self._current_metrics = ProgressMetrics()
        self._current_throughput: Optional[ThroughputMetrics] = None
        self._log_entries: List[ProgressLogEntry] = []
        self._max_log_entries = 1000
        
        # Sub-operations for nested tracking
        self._sub_operations: Dict[str, "ProgressTrackingEntity"] = {}
        
        # Cancellation and error handling
        self._cancellation_requested = False
        self._error_message: Optional[str] = None
        self._warnings: List[str] = []
    
    # Properties
    @property
    def operation_id(self) -> str:
        return self._operation_id
    
    @property
    def operation_type(self) -> OperationType:
        return self._operation_type
    
    @property
    def operation_name(self) -> str:
        return self._operation_name
    
    @property
    def priority(self) -> Priority:
        return self._priority
    
    @property
    def status(self) -> OperationStatus:
        return self._status
    
    @property
    def context(self) -> OperationContext:
        return self._context
    
    @property
    def created_time(self) -> float:
        return self._created_time
    
    @property
    def started_time(self) -> Optional[float]:
        return self._started_time
    
    @property
    def completed_time(self) -> Optional[float]:
        return self._completed_time
    
    @property
    def last_update_time(self) -> float:
        return self._last_update_time
    
    @property
    def current_metrics(self) -> ProgressMetrics:
        return self._current_metrics
    
    @property
    def current_throughput(self) -> Optional[ThroughputMetrics]:
        return self._current_throughput
    
    @property
    def error_message(self) -> Optional[str]:
        return self._error_message
    
    @property
    def warnings(self) -> List[str]:
        return self._warnings.copy()
    
    @property
    def cancellation_requested(self) -> bool:
        return self._cancellation_requested
    
    @property
    def sub_operations(self) -> Dict[str, "ProgressTrackingEntity"]:
        return self._sub_operations.copy()
    
    # Business logic methods
    def start_operation(self) -> None:
        """Start the operation."""
        if self._status != OperationStatus.PENDING:
            raise ValueError(f"Cannot start operation in status {self._status}")
        
        self._status = OperationStatus.RUNNING
        self._started_time = time.time()
        self._last_update_time = self._started_time
        self._current_throughput = ThroughputMetrics(start_time=self._started_time)
        
        self._add_log_entry("Operation started", "INFO")
    
    def update_progress(
        self,
        metrics: ProgressMetrics,
        message: Optional[str] = None,
        throughput: Optional[ThroughputMetrics] = None
    ) -> None:
        """Update progress metrics."""
        if not self._status.is_active():
            raise ValueError(f"Cannot update progress for operation in status {self._status}")
        
        self._current_metrics = metrics
        self._last_update_time = time.time()
        
        if throughput:
            self._current_throughput = throughput
        
        if message:
            self._add_log_entry(message, "INFO", metrics, throughput)
        
        # Auto-complete if metrics indicate completion
        if metrics.is_complete and self._status == OperationStatus.RUNNING:
            self.complete_operation()
    
    def complete_operation(self, message: Optional[str] = None) -> None:
        """Mark the operation as completed."""
        if not self._status.is_active():
            raise ValueError(f"Cannot complete operation in status {self._status}")
        
        self._status = OperationStatus.COMPLETED
        self._completed_time = time.time()
        self._last_update_time = self._completed_time
        
        # Ensure metrics show 100% completion
        if self._current_metrics.percentage < 100.0:
            self._current_metrics = ProgressMetrics(
                percentage=100.0,
                current_value=self._current_metrics.total_value or self._current_metrics.current_value,
                total_value=self._current_metrics.total_value,
                bytes_processed=self._current_metrics.total_bytes or self._current_metrics.bytes_processed,
                total_bytes=self._current_metrics.total_bytes,
                files_processed=self._current_metrics.total_files or self._current_metrics.files_processed,
                total_files=self._current_metrics.total_files,
                operations_completed=self._current_metrics.total_operations or self._current_metrics.operations_completed,
                total_operations=self._current_metrics.total_operations
            )
        
        self._add_log_entry(message or "Operation completed successfully", "INFO")
    
    def fail_operation(self, error_message: str) -> None:
        """Mark the operation as failed."""
        if self._status.is_terminal():
            raise ValueError(f"Cannot fail operation in status {self._status}")
        
        self._status = OperationStatus.FAILED
        self._error_message = error_message
        self._completed_time = time.time()
        self._last_update_time = self._completed_time
        
        self._add_log_entry(f"Operation failed: {error_message}", "ERROR")
    
    def cancel_operation(self, message: Optional[str] = None) -> None:
        """Cancel the operation."""
        if not self._status.can_cancel():
            raise ValueError(f"Cannot cancel operation in status {self._status}")
        
        self._cancellation_requested = True
        
        if self._status == OperationStatus.RUNNING:
            self._status = OperationStatus.CANCELLING
        else:
            self._status = OperationStatus.CANCELLED
            self._completed_time = time.time()
        
        self._last_update_time = time.time()
        self._add_log_entry(message or "Operation cancellation requested", "WARN")
    
    def confirm_cancellation(self) -> None:
        """Confirm that the operation has been cancelled."""
        if self._status != OperationStatus.CANCELLING:
            raise ValueError(f"Cannot confirm cancellation for operation in status {self._status}")
        
        self._status = OperationStatus.CANCELLED
        self._completed_time = time.time()
        self._last_update_time = self._completed_time
        
        self._add_log_entry("Operation cancelled", "WARN")
    
    def pause_operation(self) -> None:
        """Pause the operation."""
        if not self._status.can_pause():
            raise ValueError(f"Cannot pause operation in status {self._status}")
        
        self._status = OperationStatus.PAUSED
        self._last_update_time = time.time()
        
        self._add_log_entry("Operation paused", "INFO")
    
    def resume_operation(self) -> None:
        """Resume the operation."""
        if not self._status.can_resume():
            raise ValueError(f"Cannot resume operation in status {self._status}")
        
        self._status = OperationStatus.RUNNING
        self._last_update_time = time.time()
        
        self._add_log_entry("Operation resumed", "INFO")
    
    def add_warning(self, warning: str) -> None:
        """Add a warning message."""
        self._warnings.append(warning)
        self._add_log_entry(f"Warning: {warning}", "WARN")
    
    def add_sub_operation(self, sub_operation: "ProgressTrackingEntity") -> None:
        """Add a sub-operation for nested tracking."""
        if sub_operation.operation_id in self._sub_operations:
            raise ValueError(f"Sub-operation {sub_operation.operation_id} already exists")
        
        self._sub_operations[sub_operation.operation_id] = sub_operation
        
        # Set parent context
        sub_operation.context.parent_operation_id = self._operation_id
    
    def remove_sub_operation(self, operation_id: str) -> None:
        """Remove a sub-operation."""
        if operation_id in self._sub_operations:
            del self._sub_operations[operation_id]
    
    def get_aggregated_metrics(self) -> ProgressMetrics:
        """Get aggregated metrics including sub-operations."""
        if not self._sub_operations:
            return self._current_metrics
        
        # Aggregate metrics from sub-operations
        total_bytes = 0
        bytes_processed = 0
        total_files = 0
        files_processed = 0
        total_ops = len(self._sub_operations)
        completed_ops = 0
        
        for sub_op in self._sub_operations.values():
            sub_metrics = sub_op.get_aggregated_metrics()
            
            if sub_metrics.total_bytes:
                total_bytes += sub_metrics.total_bytes
            bytes_processed += sub_metrics.bytes_processed
            
            if sub_metrics.total_files:
                total_files += sub_metrics.total_files
            files_processed += sub_metrics.files_processed
            
            if sub_op.status.is_terminal():
                completed_ops += 1
        
        # Calculate overall percentage
        percentage = (completed_ops / total_ops * 100.0) if total_ops > 0 else self._current_metrics.percentage
        
        return ProgressMetrics(
            percentage=percentage,
            current_value=completed_ops,
            total_value=total_ops,
            bytes_processed=bytes_processed,
            total_bytes=total_bytes or None,
            files_processed=files_processed,
            total_files=total_files or None,
            operations_completed=completed_ops,
            total_operations=total_ops
        )
    
    def get_log_entries(self, limit: Optional[int] = None) -> List[ProgressLogEntry]:
        """Get log entries, optionally limited to recent entries."""
        entries = self._log_entries
        if limit:
            entries = entries[-limit:]
        return entries.copy()
    
    def calculate_duration(self) -> Optional[float]:
        """Calculate operation duration in seconds."""
        if self._started_time is None:
            return None
        
        end_time = self._completed_time or time.time()
        return end_time - self._started_time
    
    def _add_log_entry(
        self,
        message: str,
        level: str,
        metrics: Optional[ProgressMetrics] = None,
        throughput: Optional[ThroughputMetrics] = None
    ) -> None:
        """Add a log entry."""
        entry = ProgressLogEntry(
            message=message,
            level=level,
            metrics=metrics or self._current_metrics,
            throughput=throughput or self._current_throughput
        )
        
        self._log_entries.append(entry)
        
        # Trim old entries if we exceed the limit
        if len(self._log_entries) > self._max_log_entries:
            self._log_entries = self._log_entries[-self._max_log_entries:]
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            "operation_id": self._operation_id,
            "operation_type": self._operation_type.value,
            "operation_name": self._operation_name,
            "priority": self._priority.value,
            "status": self._status.value,
            "context": {
                "user_id": self._context.user_id,
                "session_id": self._context.session_id,
                "parent_operation_id": self._context.parent_operation_id,
                "simulation_id": self._context.simulation_id,
                "location_name": self._context.location_name,
                "workflow_id": self._context.workflow_id,
                "tags": list(self._context.tags),
                "metadata": self._context.metadata
            },
            "created_time": self._created_time,
            "started_time": self._started_time,
            "completed_time": self._completed_time,
            "last_update_time": self._last_update_time,
            "current_metrics": {
                "percentage": self._current_metrics.percentage,
                "current_value": self._current_metrics.current_value,
                "total_value": self._current_metrics.total_value,
                "bytes_processed": self._current_metrics.bytes_processed,
                "total_bytes": self._current_metrics.total_bytes,
                "files_processed": self._current_metrics.files_processed,
                "total_files": self._current_metrics.total_files,
                "operations_completed": self._current_metrics.operations_completed,
                "total_operations": self._current_metrics.total_operations
            },
            "current_throughput": {
                "start_time": self._current_throughput.start_time if self._current_throughput else None,
                "current_time": self._current_throughput.current_time if self._current_throughput else None,
                "bytes_per_second": self._current_throughput.bytes_per_second if self._current_throughput else 0.0,
                "files_per_second": self._current_throughput.files_per_second if self._current_throughput else 0.0,
                "operations_per_second": self._current_throughput.operations_per_second if self._current_throughput else 0.0,
                "estimated_completion_time": self._current_throughput.estimated_completion_time if self._current_throughput else None,
                "estimated_remaining_seconds": self._current_throughput.estimated_remaining_seconds if self._current_throughput else None
            } if self._current_throughput else None,
            "error_message": self._error_message,
            "warnings": self._warnings,
            "cancellation_requested": self._cancellation_requested,
            "sub_operations": [sub_op.operation_id for sub_op in self._sub_operations.values()]
        }