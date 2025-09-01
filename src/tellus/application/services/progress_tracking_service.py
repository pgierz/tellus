"""
Progress tracking application service.

This module provides the application service layer for progress tracking operations,
orchestrating domain entities and repository interactions.
"""

import asyncio
import logging
import threading
import time
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict
from typing import Any, Callable, Dict, List, Optional, Set, Union

from ...domain.entities.progress_tracking import (OperationContext,
                                                  OperationStatus,
                                                  OperationType, Priority,
                                                  ProgressMetrics,
                                                  ProgressTrackingEntity,
                                                  ThroughputMetrics)
from ...domain.repositories.progress_tracking_repository import (
    IProgressTrackingRepository, OperationAlreadyExistsError,
    OperationNotFoundError)
from ..dtos import (BulkProgressQueryDto, BulkProgressResponseDto,
                    CreateProgressTrackingDto, FilterOptions,
                    NestedProgressDto, OperationContextDto,
                    OperationControlDto, OperationControlResultDto,
                    PaginationInfo, ProgressCallbackRegistrationDto,
                    ProgressLogEntryDto, ProgressMetricsDto,
                    ProgressSummaryDto, ProgressTrackingDto,
                    ProgressTrackingListDto, ProgressUpdateNotificationDto,
                    ThroughputMetricsDto, UpdateProgressDto)
from ..exceptions import ApplicationError

logger = logging.getLogger(__name__)


class ProgressTrackingServiceError(ApplicationError):
    """Base exception for progress tracking service errors."""
    pass


class OperationNotFoundServiceError(ProgressTrackingServiceError):
    """Raised when an operation is not found."""
    pass


class OperationAlreadyExistsServiceError(ProgressTrackingServiceError):
    """Raised when attempting to create an operation that already exists."""
    pass


class InvalidOperationError(ProgressTrackingServiceError):
    """Raised when an invalid operation is attempted."""
    pass


class ProgressCallback:
    """Represents a registered progress callback."""
    
    def __init__(
        self,
        callback_id: str,
        callback_type: str,
        callback_func: Callable[[ProgressUpdateNotificationDto], None],
        filter_criteria: Optional[Dict[str, Any]] = None,
        active: bool = True
    ):
        self.callback_id = callback_id
        self.callback_type = callback_type
        self.callback_func = callback_func
        self.filter_criteria = filter_criteria or {}
        self.active = active
        self.registration_time = time.time()
        self.call_count = 0
        self.last_called = None
    
    def should_notify(self, notification: ProgressUpdateNotificationDto) -> bool:
        """Check if this callback should be notified for the given update."""
        if not self.active:
            return False
        
        # Check minimum percentage change filter
        min_change = self.filter_criteria.get('min_percentage_change', 0.0)
        if min_change > 0.0 and notification.metrics:
            # Implementation would need to track previous percentage
            pass
        
        # Check notification type filter
        allowed_types = self.filter_criteria.get('notification_types')
        if allowed_types and notification.notification_type not in allowed_types:
            return False
        
        return True
    
    async def notify(self, notification: ProgressUpdateNotificationDto) -> None:
        """Send notification to the callback."""
        if not self.should_notify(notification):
            return
        
        try:
            if asyncio.iscoroutinefunction(self.callback_func):
                await self.callback_func(notification)
            else:
                self.callback_func(notification)
            
            self.call_count += 1
            self.last_called = time.time()
            
        except Exception as e:
            logger.error(f"Error in progress callback {self.callback_id}: {e}")


class IProgressTrackingService(ABC):
    """Abstract interface for progress tracking service."""
    
    @abstractmethod
    async def create_operation(self, create_dto: CreateProgressTrackingDto) -> ProgressTrackingDto:
        """Create a new progress tracking operation."""
        pass
    
    @abstractmethod
    async def get_operation(self, operation_id: str) -> Optional[ProgressTrackingDto]:
        """Get a progress tracking operation by ID."""
        pass
    
    @abstractmethod
    async def update_progress(self, update_dto: UpdateProgressDto) -> ProgressTrackingDto:
        """Update progress for an operation."""
        pass
    
    @abstractmethod
    async def control_operation(self, control_dto: OperationControlDto) -> OperationControlResultDto:
        """Control operation (start, pause, resume, cancel)."""
        pass
    
    @abstractmethod
    async def list_operations(
        self,
        filters: FilterOptions,
        pagination: PaginationInfo
    ) -> ProgressTrackingListDto:
        """List operations with filtering and pagination."""
        pass
    
    @abstractmethod
    async def get_summary(self) -> ProgressSummaryDto:
        """Get progress tracking summary statistics."""
        pass
    
    @abstractmethod
    async def register_callback(
        self,
        registration_dto: ProgressCallbackRegistrationDto,
        callback_func: Callable[[ProgressUpdateNotificationDto], None]
    ) -> str:
        """Register a progress callback."""
        pass
    
    @abstractmethod
    async def unregister_callback(self, callback_id: str) -> bool:
        """Unregister a progress callback."""
        pass


class ProgressTrackingService(IProgressTrackingService):
    """
    Implementation of progress tracking service.
    
    This service orchestrates progress tracking operations, manages callbacks,
    and provides thread-safe access to progress information.
    """
    
    def __init__(
        self,
        repository: IProgressTrackingRepository,
        max_workers: int = 4,
        notification_queue_size: int = 1000
    ):
        """Initialize the progress tracking service."""
        self._repository = repository
        self._callbacks: Dict[str, ProgressCallback] = {}
        self._operation_locks: Dict[str, threading.RLock] = {}
        self._callbacks_lock = threading.RLock()
        self._locks_lock = threading.RLock()
        
        # Thread pool for async operations
        self._executor = ThreadPoolExecutor(max_workers=max_workers)
        
        # Notification queue for batch processing
        self._notification_queue: asyncio.Queue = asyncio.Queue(maxsize=notification_queue_size)
        self._notification_task: Optional[asyncio.Task] = None
        self._running = False
    
    async def start(self) -> None:
        """Start the progress tracking service."""
        if self._running:
            return
        
        self._running = True
        self._notification_task = asyncio.create_task(self._process_notifications())
        logger.info("Progress tracking service started")
    
    async def stop(self) -> None:
        """Stop the progress tracking service."""
        if not self._running:
            return
        
        self._running = False
        
        if self._notification_task:
            self._notification_task.cancel()
            try:
                await self._notification_task
            except asyncio.CancelledError:
                pass
        
        self._executor.shutdown(wait=True)
        logger.info("Progress tracking service stopped")
    
    def _get_operation_lock(self, operation_id: str) -> threading.RLock:
        """Get or create a lock for the given operation."""
        with self._locks_lock:
            if operation_id not in self._operation_locks:
                self._operation_locks[operation_id] = threading.RLock()
            return self._operation_locks[operation_id]
    
    def _cleanup_operation_lock(self, operation_id: str) -> None:
        """Clean up the lock for a completed operation."""
        with self._locks_lock:
            if operation_id in self._operation_locks:
                del self._operation_locks[operation_id]
    
    async def create_operation(self, create_dto: CreateProgressTrackingDto) -> ProgressTrackingDto:
        """Create a new progress tracking operation."""
        try:
            # Convert DTO to domain entity
            context = None
            if create_dto.context:
                context = OperationContext(
                    user_id=create_dto.context.user_id,
                    session_id=create_dto.context.session_id,
                    parent_operation_id=create_dto.context.parent_operation_id,
                    simulation_id=create_dto.context.simulation_id,
                    location_name=create_dto.context.location_name,
                    workflow_id=create_dto.context.workflow_id,
                    tags=create_dto.context.tags,
                    metadata=create_dto.context.metadata
                )
            
            entity = ProgressTrackingEntity(
                operation_id=create_dto.operation_id,
                operation_type=OperationType(create_dto.operation_type),
                operation_name=create_dto.operation_name,
                priority=Priority[create_dto.priority.upper()],
                context=context
            )
            
            # Create in repository
            await self._repository.create(entity)
            
            # Send notification
            await self._send_notification(
                entity,
                "operation_created",
                message="Operation created"
            )
            
            return self._entity_to_dto(entity)
            
        except OperationAlreadyExistsError as e:
            raise OperationAlreadyExistsServiceError(str(e))
        except Exception as e:
            logger.error(f"Error creating operation {create_dto.operation_id}: {e}")
            raise ProgressTrackingServiceError(f"Failed to create operation: {e}")
    
    async def get_operation(self, operation_id: str) -> Optional[ProgressTrackingDto]:
        """Get a progress tracking operation by ID."""
        try:
            entity = await self._repository.get_by_id(operation_id)
            if entity is None:
                return None
            
            return self._entity_to_dto(entity)
            
        except Exception as e:
            logger.error(f"Error getting operation {operation_id}: {e}")
            raise ProgressTrackingServiceError(f"Failed to get operation: {e}")
    
    async def update_progress(self, update_dto: UpdateProgressDto) -> ProgressTrackingDto:
        """Update progress for an operation."""
        operation_lock = self._get_operation_lock(update_dto.operation_id)
        
        with operation_lock:
            try:
                # Get current entity
                entity = await self._repository.get_by_id(update_dto.operation_id)
                if entity is None:
                    raise OperationNotFoundServiceError(f"Operation {update_dto.operation_id} not found")
                
                # Convert DTO metrics to domain objects
                metrics = ProgressMetrics(
                    percentage=update_dto.metrics.percentage,
                    current_value=update_dto.metrics.current_value,
                    total_value=update_dto.metrics.total_value,
                    bytes_processed=update_dto.metrics.bytes_processed,
                    total_bytes=update_dto.metrics.total_bytes,
                    files_processed=update_dto.metrics.files_processed,
                    total_files=update_dto.metrics.total_files,
                    operations_completed=update_dto.metrics.operations_completed,
                    total_operations=update_dto.metrics.total_operations
                )
                
                throughput = None
                if update_dto.throughput:
                    throughput = ThroughputMetrics(
                        start_time=update_dto.throughput.start_time,
                        current_time=update_dto.throughput.current_time,
                        bytes_per_second=update_dto.throughput.bytes_per_second,
                        files_per_second=update_dto.throughput.files_per_second,
                        operations_per_second=update_dto.throughput.operations_per_second,
                        estimated_completion_time=update_dto.throughput.estimated_completion_time,
                        estimated_remaining_seconds=update_dto.throughput.estimated_remaining_seconds
                    )
                
                # Update entity
                previous_status = entity.status
                entity.update_progress(metrics, update_dto.message, throughput)
                
                # Save to repository
                await self._repository.update(entity)
                
                # Send notifications
                await self._send_notification(
                    entity,
                    "progress_update",
                    message=update_dto.message,
                    previous_status=previous_status
                )
                
                # If operation completed, clean up lock
                if entity.status.is_terminal():
                    self._cleanup_operation_lock(update_dto.operation_id)
                
                return self._entity_to_dto(entity)
                
            except OperationNotFoundError as e:
                raise OperationNotFoundServiceError(str(e))
            except Exception as e:
                logger.error(f"Error updating progress for {update_dto.operation_id}: {e}")
                raise ProgressTrackingServiceError(f"Failed to update progress: {e}")
    
    async def control_operation(self, control_dto: OperationControlDto) -> OperationControlResultDto:
        """Control operation (start, pause, resume, cancel)."""
        operation_lock = self._get_operation_lock(control_dto.operation_id)
        
        with operation_lock:
            try:
                # Get current entity
                entity = await self._repository.get_by_id(control_dto.operation_id)
                if entity is None:
                    raise OperationNotFoundServiceError(f"Operation {control_dto.operation_id} not found")
                
                previous_status = entity.status
                
                # Execute command
                if control_dto.command == "start":
                    entity.start_operation()
                elif control_dto.command == "pause":
                    entity.pause_operation()
                elif control_dto.command == "resume":
                    entity.resume_operation()
                elif control_dto.command == "cancel":
                    entity.cancel_operation(control_dto.reason)
                elif control_dto.command == "force_cancel":
                    entity.cancel_operation(control_dto.reason)
                    entity.confirm_cancellation()
                else:
                    raise InvalidOperationError(f"Unknown command: {control_dto.command}")
                
                # Save to repository
                await self._repository.update(entity)
                
                # Send notification
                await self._send_notification(
                    entity,
                    "status_change",
                    message=f"Operation {control_dto.command}",
                    previous_status=previous_status
                )
                
                # If operation completed, clean up lock
                if entity.status.is_terminal():
                    self._cleanup_operation_lock(control_dto.operation_id)
                
                return OperationControlResultDto(
                    operation_id=control_dto.operation_id,
                    command=control_dto.command,
                    success=True,
                    previous_status=previous_status.value,
                    new_status=entity.status.value,
                    message=f"Operation {control_dto.command} successful"
                )
                
            except (OperationNotFoundError, ValueError) as e:
                return OperationControlResultDto(
                    operation_id=control_dto.operation_id,
                    command=control_dto.command,
                    success=False,
                    previous_status="unknown",
                    new_status="unknown",
                    message=str(e)
                )
            except Exception as e:
                logger.error(f"Error controlling operation {control_dto.operation_id}: {e}")
                raise ProgressTrackingServiceError(f"Failed to control operation: {e}")
    
    async def list_operations(
        self,
        filters: FilterOptions,
        pagination: PaginationInfo
    ) -> ProgressTrackingListDto:
        """List operations with filtering and pagination."""
        try:
            # Convert filters to repository parameters
            status_filter = None
            if filters.search_term:
                # Could implement status filtering by search term
                pass
            
            operations = await self._repository.list_operations(
                limit=pagination.page_size,
                offset=(pagination.page - 1) * pagination.page_size,
                order_by="last_update_time",
                ascending=False
            )
            
            # Convert entities to DTOs
            operation_dtos = [self._entity_to_dto(op) for op in operations]
            
            # Count total for pagination
            total_count = await self._repository.count_operations()
            
            pagination_info = PaginationInfo(
                page=pagination.page,
                page_size=pagination.page_size,
                total_count=total_count,
                has_next=(pagination.page * pagination.page_size) < total_count,
                has_previous=pagination.page > 1
            )
            
            return ProgressTrackingListDto(
                operations=operation_dtos,
                pagination=pagination_info,
                filters_applied=filters
            )
            
        except Exception as e:
            logger.error(f"Error listing operations: {e}")
            raise ProgressTrackingServiceError(f"Failed to list operations: {e}")
    
    async def get_summary(self) -> ProgressSummaryDto:
        """Get progress tracking summary statistics."""
        try:
            stats = await self._repository.get_statistics()
            
            return ProgressSummaryDto(
                total_operations=stats.get('total_operations', 0),
                active_operations=stats.get('active_operations', 0),
                completed_operations=stats.get('completed_operations', 0),
                failed_operations=stats.get('failed_operations', 0),
                cancelled_operations=stats.get('cancelled_operations', 0),
                operations_by_type=stats.get('operations_by_type', {}),
                operations_by_status=stats.get('operations_by_status', {}),
                operations_by_priority=stats.get('operations_by_priority', {}),
                total_bytes_processed=stats.get('total_bytes_processed', 0),
                average_completion_time=stats.get('average_completion_time'),
                oldest_active_operation=stats.get('oldest_active_operation')
            )
            
        except Exception as e:
            logger.error(f"Error getting summary: {e}")
            raise ProgressTrackingServiceError(f"Failed to get summary: {e}")
    
    async def register_callback(
        self,
        registration_dto: ProgressCallbackRegistrationDto,
        callback_func: Callable[[ProgressUpdateNotificationDto], None]
    ) -> str:
        """Register a progress callback."""
        with self._callbacks_lock:
            if registration_dto.callback_id in self._callbacks:
                raise ProgressTrackingServiceError(f"Callback {registration_dto.callback_id} already exists")
            
            callback = ProgressCallback(
                callback_id=registration_dto.callback_id,
                callback_type=registration_dto.callback_type,
                callback_func=callback_func,
                filter_criteria=registration_dto.filter_criteria,
                active=registration_dto.active
            )
            
            self._callbacks[registration_dto.callback_id] = callback
            
            logger.info(f"Registered progress callback {registration_dto.callback_id}")
            return registration_dto.callback_id
    
    async def unregister_callback(self, callback_id: str) -> bool:
        """Unregister a progress callback."""
        with self._callbacks_lock:
            if callback_id in self._callbacks:
                del self._callbacks[callback_id]
                logger.info(f"Unregistered progress callback {callback_id}")
                return True
            return False
    
    async def _send_notification(
        self,
        entity: ProgressTrackingEntity,
        notification_type: str,
        message: Optional[str] = None,
        previous_status: Optional[OperationStatus] = None
    ) -> None:
        """Send a progress update notification."""
        if not self._running:
            return
        
        try:
            # Convert metrics to DTO
            metrics_dto = None
            if entity.current_metrics:
                metrics_dto = ProgressMetricsDto(
                    percentage=entity.current_metrics.percentage,
                    current_value=entity.current_metrics.current_value,
                    total_value=entity.current_metrics.total_value,
                    bytes_processed=entity.current_metrics.bytes_processed,
                    total_bytes=entity.current_metrics.total_bytes,
                    files_processed=entity.current_metrics.files_processed,
                    total_files=entity.current_metrics.total_files,
                    operations_completed=entity.current_metrics.operations_completed,
                    total_operations=entity.current_metrics.total_operations
                )
            
            notification = ProgressUpdateNotificationDto(
                operation_id=entity.operation_id,
                notification_type=notification_type,
                timestamp=time.time(),
                current_status=entity.status.value,
                previous_status=previous_status.value if previous_status else None,
                metrics=metrics_dto,
                message=message
            )
            
            # Queue notification for processing
            try:
                self._notification_queue.put_nowait(notification)
            except asyncio.QueueFull:
                logger.warning("Notification queue full, dropping notification")
                
        except Exception as e:
            logger.error(f"Error sending notification: {e}")
    
    async def _process_notifications(self) -> None:
        """Process notification queue and send to callbacks."""
        while self._running:
            try:
                # Get notification from queue with timeout
                try:
                    notification = await asyncio.wait_for(
                        self._notification_queue.get(), 
                        timeout=1.0
                    )
                except asyncio.TimeoutError:
                    continue
                
                # Send to all registered callbacks
                with self._callbacks_lock:
                    callback_tasks = []
                    for callback in self._callbacks.values():
                        if callback.active:
                            task = asyncio.create_task(callback.notify(notification))
                            callback_tasks.append(task)
                    
                    # Wait for all callbacks to complete
                    if callback_tasks:
                        await asyncio.gather(*callback_tasks, return_exceptions=True)
                
                # Mark notification as done
                self._notification_queue.task_done()
                
            except Exception as e:
                logger.error(f"Error processing notifications: {e}")
    
    def _entity_to_dto(self, entity: ProgressTrackingEntity) -> ProgressTrackingDto:
        """Convert a domain entity to a DTO."""
        context_dto = OperationContextDto(
            user_id=entity.context.user_id,
            session_id=entity.context.session_id,
            parent_operation_id=entity.context.parent_operation_id,
            simulation_id=entity.context.simulation_id,
            location_name=entity.context.location_name,
            workflow_id=entity.context.workflow_id,
            tags=entity.context.tags,
            metadata=entity.context.metadata
        )
        
        metrics_dto = ProgressMetricsDto(
            percentage=entity.current_metrics.percentage,
            current_value=entity.current_metrics.current_value,
            total_value=entity.current_metrics.total_value,
            bytes_processed=entity.current_metrics.bytes_processed,
            total_bytes=entity.current_metrics.total_bytes,
            files_processed=entity.current_metrics.files_processed,
            total_files=entity.current_metrics.total_files,
            operations_completed=entity.current_metrics.operations_completed,
            total_operations=entity.current_metrics.total_operations
        )
        
        throughput_dto = None
        if entity.current_throughput:
            throughput_dto = ThroughputMetricsDto(
                start_time=entity.current_throughput.start_time,
                current_time=entity.current_throughput.current_time,
                bytes_per_second=entity.current_throughput.bytes_per_second,
                files_per_second=entity.current_throughput.files_per_second,
                operations_per_second=entity.current_throughput.operations_per_second,
                estimated_completion_time=entity.current_throughput.estimated_completion_time,
                estimated_remaining_seconds=entity.current_throughput.estimated_remaining_seconds,
                elapsed_seconds=entity.current_throughput.elapsed_seconds
            )
        
        return ProgressTrackingDto(
            operation_id=entity.operation_id,
            operation_type=entity.operation_type.value,
            operation_name=entity.operation_name,
            priority=entity.priority.name.lower(),
            status=entity.status.value,
            context=context_dto,
            created_time=entity.created_time,
            started_time=entity.started_time,
            completed_time=entity.completed_time,
            last_update_time=entity.last_update_time,
            current_metrics=metrics_dto,
            current_throughput=throughput_dto,
            error_message=entity.error_message,
            warnings=entity.warnings,
            cancellation_requested=entity.cancellation_requested,
            sub_operations=list(entity.sub_operations.keys()),
            duration_seconds=entity.calculate_duration()
        )