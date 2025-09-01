"""
Thread-safe concurrent progress tracking system.

This module provides a comprehensive, thread-safe progress tracking system
that supports concurrent operations, worker pools, and distributed progress updates.
"""

import asyncio
import logging
import threading
import time
import uuid
import weakref
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from dataclasses import dataclass, field
from threading import Event, Lock, RLock
from typing import Any, Callable, Dict, List, Optional, Set, Union

from ...domain.entities.progress_tracking import (OperationContext,
                                                  OperationStatus,
                                                  OperationType, Priority,
                                                  ProgressMetrics,
                                                  ProgressTrackingEntity,
                                                  ThroughputMetrics)
from ..dtos import (CreateProgressTrackingDto, ProgressMetricsDto,
                    ProgressUpdateNotificationDto, ThroughputMetricsDto,
                    UpdateProgressDto)
from .progress_tracking_service import IProgressTrackingService

logger = logging.getLogger(__name__)


@dataclass
class WorkerProgress:
    """Progress tracking for individual workers."""
    worker_id: str
    operation_id: str
    start_time: float
    last_update: float
    bytes_processed: int = 0
    files_processed: int = 0
    current_task: Optional[str] = None
    error_count: int = 0
    is_active: bool = True
    
    def update(self, bytes_delta: int = 0, files_delta: int = 0, task: Optional[str] = None):
        """Update worker progress."""
        self.bytes_processed += bytes_delta
        self.files_processed += files_delta
        self.last_update = time.time()
        if task:
            self.current_task = task


@dataclass
class ConcurrentOperationConfig:
    """Configuration for concurrent operations."""
    max_workers: int = 4
    chunk_size: int = 1024 * 1024  # 1MB chunks
    progress_update_interval: float = 1.0  # seconds
    enable_worker_tracking: bool = True
    enable_throughput_calculation: bool = True
    timeout_seconds: Optional[float] = None
    retry_on_failure: bool = True
    max_retries: int = 3


class ProgressAggregator:
    """Aggregates progress from multiple concurrent workers."""
    
    def __init__(self, operation_id: str):
        self.operation_id = operation_id
        self.workers: Dict[str, WorkerProgress] = {}
        self.lock = RLock()
        self.total_bytes_target: Optional[int] = None
        self.total_files_target: Optional[int] = None
        self.start_time = time.time()
        
    def add_worker(self, worker_id: str) -> None:
        """Add a new worker to track."""
        with self.lock:
            self.workers[worker_id] = WorkerProgress(
                worker_id=worker_id,
                operation_id=self.operation_id,
                start_time=time.time(),
                last_update=time.time()
            )
    
    def remove_worker(self, worker_id: str) -> None:
        """Remove a worker from tracking."""
        with self.lock:
            if worker_id in self.workers:
                self.workers[worker_id].is_active = False
                # Keep worker data for final aggregation
    
    def update_worker_progress(
        self, 
        worker_id: str, 
        bytes_delta: int = 0, 
        files_delta: int = 0,
        task: Optional[str] = None
    ) -> None:
        """Update progress for a specific worker."""
        with self.lock:
            if worker_id in self.workers:
                self.workers[worker_id].update(bytes_delta, files_delta, task)
    
    def get_aggregated_metrics(self) -> ProgressMetrics:
        """Get aggregated progress metrics from all workers."""
        with self.lock:
            total_bytes = sum(w.bytes_processed for w in self.workers.values())
            total_files = sum(w.files_processed for w in self.workers.values())
            
            # Calculate percentage
            percentage = 0.0
            if self.total_bytes_target and self.total_bytes_target > 0:
                percentage = min(100.0, (total_bytes / self.total_bytes_target) * 100.0)
            elif self.total_files_target and self.total_files_target > 0:
                percentage = min(100.0, (total_files / self.total_files_target) * 100.0)
            
            return ProgressMetrics(
                percentage=percentage,
                current_value=total_files,
                total_value=self.total_files_target,
                bytes_processed=total_bytes,
                total_bytes=self.total_bytes_target,
                files_processed=total_files,
                total_files=self.total_files_target,
                operations_completed=len([w for w in self.workers.values() if not w.is_active]),
                total_operations=len(self.workers)
            )
    
    def get_throughput_metrics(self) -> ThroughputMetrics:
        """Calculate throughput metrics."""
        with self.lock:
            current_time = time.time()
            elapsed = current_time - self.start_time
            
            if elapsed <= 0:
                return ThroughputMetrics(start_time=self.start_time)
            
            total_bytes = sum(w.bytes_processed for w in self.workers.values())
            total_files = sum(w.files_processed for w in self.workers.values())
            
            bytes_per_second = total_bytes / elapsed
            files_per_second = total_files / elapsed
            
            # Estimate completion time
            estimated_completion = None
            if self.total_bytes_target and bytes_per_second > 0:
                remaining_bytes = max(0, self.total_bytes_target - total_bytes)
                estimated_remaining = remaining_bytes / bytes_per_second
                estimated_completion = current_time + estimated_remaining
            
            return ThroughputMetrics(
                start_time=self.start_time,
                current_time=current_time,
                bytes_per_second=bytes_per_second,
                files_per_second=files_per_second,
                operations_per_second=len(self.workers) / elapsed if elapsed > 0 else 0,
                estimated_completion_time=estimated_completion,
                estimated_remaining_seconds=estimated_remaining if estimated_completion else None
            )
    
    def set_targets(self, total_bytes: Optional[int] = None, total_files: Optional[int] = None):
        """Set target values for progress calculation."""
        with self.lock:
            if total_bytes is not None:
                self.total_bytes_target = total_bytes
            if total_files is not None:
                self.total_files_target = total_files


class ConcurrentProgressTracker:
    """
    Thread-safe progress tracker for concurrent operations.
    
    This class provides comprehensive progress tracking for operations that
    involve multiple concurrent workers, with support for aggregation,
    real-time updates, and worker health monitoring.
    """
    
    def __init__(
        self,
        progress_service: IProgressTrackingService,
        update_interval: float = 1.0,
        max_aggregators: int = 1000
    ):
        """Initialize the concurrent progress tracker."""
        self.progress_service = progress_service
        self.update_interval = update_interval
        self.max_aggregators = max_aggregators
        
        # Thread-safe data structures
        self.aggregators: Dict[str, ProgressAggregator] = {}
        self.aggregators_lock = RLock()
        self.update_tasks: Dict[str, asyncio.Task] = {}
        self.update_tasks_lock = asyncio.Lock()
        
        # Background update system
        self.update_executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="progress-updater")
        self.running = False
        self.update_events: Dict[str, Event] = {}
        self.update_events_lock = threading.Lock()
        
        # Cleanup tracking
        self.cleanup_interval = 300  # 5 minutes
        self.last_cleanup = time.time()
    
    async def start(self) -> None:
        """Start the concurrent progress tracker."""
        if self.running:
            return
        
        self.running = True
        logger.info("Concurrent progress tracker started")
    
    async def stop(self) -> None:
        """Stop the concurrent progress tracker and cleanup resources."""
        if not self.running:
            return
        
        self.running = False
        
        # Cancel all update tasks
        async with self.update_tasks_lock:
            for task in self.update_tasks.values():
                task.cancel()
            
            # Wait for tasks to complete
            if self.update_tasks:
                await asyncio.gather(*self.update_tasks.values(), return_exceptions=True)
            
            self.update_tasks.clear()
        
        # Shutdown executor
        self.update_executor.shutdown(wait=True)
        
        # Clear aggregators
        with self.aggregators_lock:
            self.aggregators.clear()
        
        logger.info("Concurrent progress tracker stopped")
    
    @contextmanager
    def track_operation(
        self,
        operation_id: str,
        config: Optional[ConcurrentOperationConfig] = None
    ):
        """
        Context manager for tracking a concurrent operation.
        
        Usage:
            with tracker.track_operation("op-123") as aggregator:
                # Add workers and track progress
                aggregator.add_worker("worker-1")
                # ... perform work ...
                aggregator.update_worker_progress("worker-1", bytes_delta=1024)
        """
        config = config or ConcurrentOperationConfig()
        
        # Create aggregator
        aggregator = self._create_aggregator(operation_id)
        
        # Start background updates
        update_task = None
        if self.running:
            update_task = asyncio.create_task(self._background_update_loop(operation_id))
        
        try:
            yield aggregator
        finally:
            # Stop background updates
            if update_task:
                update_task.cancel()
                try:
                    asyncio.create_task(update_task)
                except asyncio.CancelledError:
                    pass
            
            # Send final update
            if self.running:
                asyncio.create_task(self._send_final_update(operation_id))
            
            # Cleanup aggregator
            self._cleanup_aggregator(operation_id)
    
    def create_concurrent_operation(
        self,
        operation_name: str,
        operation_type: OperationType,
        config: Optional[ConcurrentOperationConfig] = None,
        context: Optional[OperationContext] = None
    ) -> str:
        """Create a new concurrent operation and return its ID."""
        operation_id = str(uuid.uuid4())
        config = config or ConcurrentOperationConfig()
        
        # Create operation via service
        create_dto = CreateProgressTrackingDto(
            operation_id=operation_id,
            operation_type=operation_type.value,
            operation_name=operation_name,
            priority="normal",
            context=None  # Convert context if needed
        )
        
        # Note: This would need to be called from an async context
        # For now, we'll store the operation for later creation
        self._create_aggregator(operation_id)
        
        return operation_id
    
    def add_worker(self, operation_id: str, worker_id: Optional[str] = None) -> str:
        """Add a worker to an operation."""
        if worker_id is None:
            worker_id = str(uuid.uuid4())
        
        with self.aggregators_lock:
            if operation_id in self.aggregators:
                self.aggregators[operation_id].add_worker(worker_id)
            else:
                raise ValueError(f"Operation {operation_id} not found")
        
        return worker_id
    
    def remove_worker(self, operation_id: str, worker_id: str) -> None:
        """Remove a worker from an operation."""
        with self.aggregators_lock:
            if operation_id in self.aggregators:
                self.aggregators[operation_id].remove_worker(worker_id)
    
    def update_worker_progress(
        self,
        operation_id: str,
        worker_id: str,
        bytes_delta: int = 0,
        files_delta: int = 0,
        current_task: Optional[str] = None
    ) -> None:
        """Update progress for a specific worker."""
        with self.aggregators_lock:
            if operation_id in self.aggregators:
                self.aggregators[operation_id].update_worker_progress(
                    worker_id, bytes_delta, files_delta, current_task
                )
                
                # Trigger immediate update if significant progress
                if bytes_delta > 1024 * 1024 or files_delta > 100:  # 1MB or 100 files
                    self._trigger_update(operation_id)
    
    def set_operation_targets(
        self,
        operation_id: str,
        total_bytes: Optional[int] = None,
        total_files: Optional[int] = None
    ) -> None:
        """Set target values for an operation."""
        with self.aggregators_lock:
            if operation_id in self.aggregators:
                self.aggregators[operation_id].set_targets(total_bytes, total_files)
    
    def get_operation_progress(self, operation_id: str) -> Optional[ProgressMetrics]:
        """Get current aggregated progress for an operation."""
        with self.aggregators_lock:
            if operation_id in self.aggregators:
                return self.aggregators[operation_id].get_aggregated_metrics()
        return None
    
    def get_worker_status(self, operation_id: str) -> Dict[str, Dict[str, Any]]:
        """Get status of all workers for an operation."""
        with self.aggregators_lock:
            if operation_id not in self.aggregators:
                return {}
            
            aggregator = self.aggregators[operation_id]
            return {
                worker_id: {
                    "is_active": worker.is_active,
                    "bytes_processed": worker.bytes_processed,
                    "files_processed": worker.files_processed,
                    "current_task": worker.current_task,
                    "error_count": worker.error_count,
                    "last_update": worker.last_update,
                    "duration": time.time() - worker.start_time
                }
                for worker_id, worker in aggregator.workers.items()
            }
    
    async def execute_concurrent_operation(
        self,
        operation_name: str,
        operation_type: OperationType,
        work_items: List[Any],
        work_function: Callable[[Any, str, str], Any],  # work_item, operation_id, worker_id -> result
        config: Optional[ConcurrentOperationConfig] = None,
        context: Optional[OperationContext] = None
    ) -> List[Any]:
        """
        Execute a concurrent operation with automatic progress tracking.
        
        Args:
            operation_name: Name of the operation
            operation_type: Type of operation
            work_items: List of work items to process
            work_function: Function to process each work item
            config: Configuration for the operation
            context: Operation context
            
        Returns:
            List of results from processing work items
        """
        config = config or ConcurrentOperationConfig()
        operation_id = self.create_concurrent_operation(
            operation_name, operation_type, config, context
        )
        
        try:
            with self.track_operation(operation_id, config) as aggregator:
                # Set targets
                aggregator.set_targets(total_files=len(work_items))
                
                # Create workers
                worker_ids = []
                for i in range(min(config.max_workers, len(work_items))):
                    worker_id = self.add_worker(operation_id)
                    worker_ids.append(worker_id)
                
                # Execute work items concurrently
                results = []
                with ThreadPoolExecutor(max_workers=config.max_workers) as executor:
                    # Submit tasks
                    future_to_item = {}
                    for i, work_item in enumerate(work_items):
                        worker_id = worker_ids[i % len(worker_ids)]
                        future = executor.submit(
                            self._execute_work_item,
                            work_function,
                            work_item,
                            operation_id,
                            worker_id
                        )
                        future_to_item[future] = (work_item, worker_id)
                    
                    # Collect results
                    for future in as_completed(future_to_item):
                        work_item, worker_id = future_to_item[future]
                        try:
                            result = future.result(timeout=config.timeout_seconds)
                            results.append(result)
                            
                            # Update progress
                            self.update_worker_progress(
                                operation_id, worker_id, files_delta=1
                            )
                            
                        except Exception as e:
                            logger.error(f"Work item failed: {e}")
                            results.append(None)
                            
                            if not config.retry_on_failure:
                                raise
                
                return results
                
        except Exception as e:
            logger.error(f"Concurrent operation {operation_id} failed: {e}")
            raise
    
    def _create_aggregator(self, operation_id: str) -> ProgressAggregator:
        """Create a new progress aggregator."""
        with self.aggregators_lock:
            if operation_id in self.aggregators:
                return self.aggregators[operation_id]
            
            # Check if we need to cleanup old aggregators
            if len(self.aggregators) >= self.max_aggregators:
                self._cleanup_old_aggregators()
            
            aggregator = ProgressAggregator(operation_id)
            self.aggregators[operation_id] = aggregator
            
            # Create update event
            with self.update_events_lock:
                self.update_events[operation_id] = Event()
            
            return aggregator
    
    def _cleanup_aggregator(self, operation_id: str) -> None:
        """Cleanup an aggregator and its resources."""
        with self.aggregators_lock:
            if operation_id in self.aggregators:
                del self.aggregators[operation_id]
        
        with self.update_events_lock:
            if operation_id in self.update_events:
                del self.update_events[operation_id]
    
    def _cleanup_old_aggregators(self) -> None:
        """Cleanup old, inactive aggregators."""
        current_time = time.time()
        to_remove = []
        
        for operation_id, aggregator in self.aggregators.items():
            # Remove aggregators older than 1 hour with no active workers
            if (current_time - aggregator.start_time > 3600 and
                not any(w.is_active for w in aggregator.workers.values())):
                to_remove.append(operation_id)
        
        for operation_id in to_remove:
            self._cleanup_aggregator(operation_id)
        
        logger.info(f"Cleaned up {len(to_remove)} old aggregators")
    
    def _trigger_update(self, operation_id: str) -> None:
        """Trigger an immediate update for an operation."""
        with self.update_events_lock:
            if operation_id in self.update_events:
                self.update_events[operation_id].set()
    
    async def _background_update_loop(self, operation_id: str) -> None:
        """Background loop for sending progress updates."""
        try:
            while self.running:
                # Wait for update interval or trigger event
                with self.update_events_lock:
                    event = self.update_events.get(operation_id)
                
                if event:
                    # Wait for either timeout or event trigger
                    await asyncio.sleep(self.update_interval)
                    if event.is_set():
                        event.clear()
                
                # Send progress update
                await self._send_progress_update(operation_id)
                
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"Background update loop failed for {operation_id}: {e}")
    
    async def _send_progress_update(self, operation_id: str) -> None:
        """Send a progress update to the progress service."""
        try:
            with self.aggregators_lock:
                if operation_id not in self.aggregators:
                    return
                
                aggregator = self.aggregators[operation_id]
                metrics = aggregator.get_aggregated_metrics()
                throughput = aggregator.get_throughput_metrics()
            
            # Convert to DTOs
            metrics_dto = ProgressMetricsDto(
                percentage=metrics.percentage,
                current_value=metrics.current_value,
                total_value=metrics.total_value,
                bytes_processed=metrics.bytes_processed,
                total_bytes=metrics.total_bytes,
                files_processed=metrics.files_processed,
                total_files=metrics.total_files,
                operations_completed=metrics.operations_completed,
                total_operations=metrics.total_operations
            )
            
            throughput_dto = ThroughputMetricsDto(
                start_time=throughput.start_time,
                current_time=throughput.current_time,
                bytes_per_second=throughput.bytes_per_second,
                files_per_second=throughput.files_per_second,
                operations_per_second=throughput.operations_per_second,
                estimated_completion_time=throughput.estimated_completion_time,
                estimated_remaining_seconds=throughput.estimated_remaining_seconds,
                elapsed_seconds=throughput.elapsed_seconds
            )
            
            update_dto = UpdateProgressDto(
                operation_id=operation_id,
                metrics=metrics_dto,
                throughput=throughput_dto
            )
            
            # Send update
            await self.progress_service.update_progress(update_dto)
            
        except Exception as e:
            logger.error(f"Failed to send progress update for {operation_id}: {e}")
    
    async def _send_final_update(self, operation_id: str) -> None:
        """Send final progress update when operation completes."""
        await self._send_progress_update(operation_id)
    
    def _execute_work_item(
        self,
        work_function: Callable[[Any, str, str], Any],
        work_item: Any,
        operation_id: str,
        worker_id: str
    ) -> Any:
        """Execute a single work item with progress tracking."""
        try:
            # Update worker task
            self.update_worker_progress(
                operation_id, worker_id, current_task=str(work_item)
            )
            
            # Execute work function
            result = work_function(work_item, operation_id, worker_id)
            
            return result
            
        except Exception as e:
            # Track error
            with self.aggregators_lock:
                if (operation_id in self.aggregators and 
                    worker_id in self.aggregators[operation_id].workers):
                    self.aggregators[operation_id].workers[worker_id].error_count += 1
            raise