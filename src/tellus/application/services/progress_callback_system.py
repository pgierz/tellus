"""
Progress callback and observer system for real-time updates.

This module provides a comprehensive callback and observer system for real-time
progress tracking updates, supporting multiple notification channels and filtering.
"""

import asyncio
import json
import logging
import threading
import time
import uuid
import weakref
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Union
from urllib.parse import urlparse

from ..dtos import ProgressMetricsDto, ProgressUpdateNotificationDto

logger = logging.getLogger(__name__)


class CallbackType(Enum):
    """Types of progress callbacks."""
    IN_MEMORY = "in_memory"
    WEBSOCKET = "websocket"
    HTTP_POST = "http_post"
    FILE_WRITE = "file_write"
    QUEUE = "queue"
    CUSTOM = "custom"


class NotificationPriority(Enum):
    """Priority levels for notifications."""
    LOW = 1
    NORMAL = 2
    HIGH = 3
    URGENT = 4


@dataclass
class CallbackFilter:
    """Filtering criteria for progress callbacks."""
    operation_ids: Optional[Set[str]] = None
    operation_types: Optional[Set[str]] = None
    status_changes_only: bool = False
    min_percentage_change: float = 0.0
    notification_types: Optional[Set[str]] = None
    user_ids: Optional[Set[str]] = None
    tag_filters: Optional[Set[str]] = None
    custom_filters: Dict[str, Any] = field(default_factory=dict)
    
    def matches(self, notification: ProgressUpdateNotificationDto) -> bool:
        """Check if a notification matches this filter."""
        # Check operation ID filter
        if self.operation_ids and notification.operation_id not in self.operation_ids:
            return False
        
        # Check notification type filter
        if self.notification_types and notification.notification_type not in self.notification_types:
            return False
        
        # Check status changes only
        if self.status_changes_only and notification.notification_type != "status_change":
            return False
        
        # Additional custom filter logic can be added here
        return True


@dataclass
class CallbackStats:
    """Statistics for a callback."""
    registration_time: float
    total_notifications: int = 0
    successful_deliveries: int = 0
    failed_deliveries: int = 0
    last_delivery_time: Optional[float] = None
    last_error: Optional[str] = None
    last_error_time: Optional[float] = None
    average_delivery_time: float = 0.0
    
    def record_success(self, delivery_time: float) -> None:
        """Record a successful delivery."""
        self.total_notifications += 1
        self.successful_deliveries += 1
        self.last_delivery_time = time.time()
        
        # Update average delivery time
        if self.average_delivery_time == 0.0:
            self.average_delivery_time = delivery_time
        else:
            self.average_delivery_time = (self.average_delivery_time + delivery_time) / 2
    
    def record_failure(self, error: str) -> None:
        """Record a failed delivery."""
        self.total_notifications += 1
        self.failed_deliveries += 1
        self.last_error = error
        self.last_error_time = time.time()
    
    @property
    def success_rate(self) -> float:
        """Calculate success rate."""
        if self.total_notifications == 0:
            return 1.0
        return self.successful_deliveries / self.total_notifications


class IProgressCallback(ABC):
    """Abstract interface for progress callbacks."""
    
    @abstractmethod
    async def deliver(self, notification: ProgressUpdateNotificationDto) -> bool:
        """
        Deliver a notification.
        
        Args:
            notification: The notification to deliver
            
        Returns:
            True if delivery was successful, False otherwise
        """
        pass
    
    @abstractmethod
    def is_healthy(self) -> bool:
        """Check if the callback is healthy and can receive notifications."""
        pass
    
    @abstractmethod
    async def close(self) -> None:
        """Close the callback and clean up resources."""
        pass


class InMemoryCallback(IProgressCallback):
    """In-memory callback that stores notifications in a queue."""
    
    def __init__(self, max_size: int = 1000):
        self.notifications: asyncio.Queue = asyncio.Queue(maxsize=max_size)
        self._closed = False
    
    async def deliver(self, notification: ProgressUpdateNotificationDto) -> bool:
        """Deliver notification to in-memory queue."""
        if self._closed:
            return False
        
        try:
            self.notifications.put_nowait(notification)
            return True
        except asyncio.QueueFull:
            logger.warning("In-memory callback queue full, dropping notification")
            return False
    
    def is_healthy(self) -> bool:
        """Check if callback is healthy."""
        return not self._closed
    
    async def close(self) -> None:
        """Close the callback."""
        self._closed = True
    
    async def get_notification(self, timeout: Optional[float] = None) -> Optional[ProgressUpdateNotificationDto]:
        """Get a notification from the queue."""
        if self._closed:
            return None
        
        try:
            if timeout:
                return await asyncio.wait_for(self.notifications.get(), timeout=timeout)
            else:
                return await self.notifications.get()
        except (asyncio.TimeoutError, asyncio.QueueEmpty):
            return None


class WebSocketCallback(IProgressCallback):
    """WebSocket callback for real-time web updates."""
    
    def __init__(self, websocket):
        self.websocket = websocket
        self._closed = False
    
    async def deliver(self, notification: ProgressUpdateNotificationDto) -> bool:
        """Deliver notification via WebSocket."""
        if self._closed or self.websocket.closed:
            return False
        
        try:
            # Convert notification to JSON
            notification_dict = {
                "operation_id": notification.operation_id,
                "notification_type": notification.notification_type,
                "timestamp": notification.timestamp,
                "current_status": notification.current_status,
                "previous_status": notification.previous_status,
                "message": notification.message,
                "metadata": notification.metadata
            }
            
            if notification.metrics:
                notification_dict["metrics"] = {
                    "percentage": notification.metrics.percentage,
                    "current_value": notification.metrics.current_value,
                    "total_value": notification.metrics.total_value,
                    "bytes_processed": notification.metrics.bytes_processed,
                    "total_bytes": notification.metrics.total_bytes,
                    "files_processed": notification.metrics.files_processed,
                    "total_files": notification.metrics.total_files
                }
            
            await self.websocket.send(json.dumps(notification_dict))
            return True
            
        except Exception as e:
            logger.error(f"WebSocket delivery failed: {e}")
            return False
    
    def is_healthy(self) -> bool:
        """Check if WebSocket is healthy."""
        return not self._closed and not self.websocket.closed
    
    async def close(self) -> None:
        """Close the WebSocket callback."""
        self._closed = True
        if not self.websocket.closed:
            await self.websocket.close()


class HTTPPostCallback(IProgressCallback):
    """HTTP POST callback for webhook-style notifications."""
    
    def __init__(self, url: str, headers: Optional[Dict[str, str]] = None):
        self.url = url
        self.headers = headers or {}
        self._session = None
        self._closed = False
    
    async def _get_session(self):
        """Get or create HTTP session."""
        if self._session is None:
            import aiohttp
            self._session = aiohttp.ClientSession()
        return self._session
    
    async def deliver(self, notification: ProgressUpdateNotificationDto) -> bool:
        """Deliver notification via HTTP POST."""
        if self._closed:
            return False
        
        try:
            session = await self._get_session()
            
            # Prepare payload
            payload = {
                "operation_id": notification.operation_id,
                "notification_type": notification.notification_type,
                "timestamp": notification.timestamp,
                "current_status": notification.current_status,
                "previous_status": notification.previous_status,
                "message": notification.message,
                "metadata": notification.metadata
            }
            
            if notification.metrics:
                payload["metrics"] = {
                    "percentage": notification.metrics.percentage,
                    "current_value": notification.metrics.current_value,
                    "total_value": notification.metrics.total_value,
                    "bytes_processed": notification.metrics.bytes_processed,
                    "total_bytes": notification.metrics.total_bytes,
                    "files_processed": notification.metrics.files_processed,
                    "total_files": notification.metrics.total_files
                }
            
            async with session.post(
                self.url,
                json=payload,
                headers=self.headers,
                timeout=10
            ) as response:
                return response.status < 400
                
        except Exception as e:
            logger.error(f"HTTP POST delivery failed: {e}")
            return False
    
    def is_healthy(self) -> bool:
        """Check if HTTP callback is healthy."""
        return not self._closed
    
    async def close(self) -> None:
        """Close the HTTP callback."""
        self._closed = True
        if self._session:
            await self._session.close()


class FileWriteCallback(IProgressCallback):
    """File write callback for logging notifications to files."""
    
    def __init__(self, file_path: str, max_file_size: int = 100 * 1024 * 1024):
        self.file_path = file_path
        self.max_file_size = max_file_size
        self._lock = asyncio.Lock()
        self._closed = False
    
    async def deliver(self, notification: ProgressUpdateNotificationDto) -> bool:
        """Deliver notification by writing to file."""
        if self._closed:
            return False
        
        async with self._lock:
            try:
                # Check file size and rotate if needed
                import os
                if os.path.exists(self.file_path) and os.path.getsize(self.file_path) > self.max_file_size:
                    # Rotate file
                    backup_path = f"{self.file_path}.{int(time.time())}"
                    os.rename(self.file_path, backup_path)
                
                # Prepare log entry
                log_entry = {
                    "timestamp": notification.timestamp,
                    "operation_id": notification.operation_id,
                    "notification_type": notification.notification_type,
                    "current_status": notification.current_status,
                    "previous_status": notification.previous_status,
                    "message": notification.message,
                    "metadata": notification.metadata
                }
                
                if notification.metrics:
                    log_entry["metrics"] = {
                        "percentage": notification.metrics.percentage,
                        "bytes_processed": notification.metrics.bytes_processed,
                        "files_processed": notification.metrics.files_processed
                    }
                
                # Write to file
                with open(self.file_path, 'a') as f:
                    f.write(json.dumps(log_entry) + '\n')
                
                return True
                
            except Exception as e:
                logger.error(f"File write delivery failed: {e}")
                return False
    
    def is_healthy(self) -> bool:
        """Check if file callback is healthy."""
        return not self._closed
    
    async def close(self) -> None:
        """Close the file callback."""
        self._closed = True


class FunctionCallback(IProgressCallback):
    """Function callback for custom notification handling."""
    
    def __init__(self, callback_func: Callable[[ProgressUpdateNotificationDto], Union[bool, None]]):
        self.callback_func = callback_func
        self._closed = False
    
    async def deliver(self, notification: ProgressUpdateNotificationDto) -> bool:
        """Deliver notification by calling the function."""
        if self._closed:
            return False
        
        try:
            if asyncio.iscoroutinefunction(self.callback_func):
                result = await self.callback_func(notification)
            else:
                result = self.callback_func(notification)
            
            return result is not False  # None or True is considered success
            
        except Exception as e:
            logger.error(f"Function callback delivery failed: {e}")
            return False
    
    def is_healthy(self) -> bool:
        """Check if function callback is healthy."""
        return not self._closed and self.callback_func is not None
    
    async def close(self) -> None:
        """Close the function callback."""
        self._closed = True


@dataclass
class RegisteredCallback:
    """Represents a registered callback with metadata."""
    callback_id: str
    operation_id: str
    callback: IProgressCallback
    callback_type: CallbackType
    priority: NotificationPriority
    filters: CallbackFilter
    stats: CallbackStats
    metadata: Dict[str, Any] = field(default_factory=dict)
    active: bool = True
    max_retries: int = 3
    retry_delay: float = 1.0
    
    async def deliver_with_retry(self, notification: ProgressUpdateNotificationDto) -> bool:
        """Deliver notification with retry logic."""
        if not self.active or not self.callback.is_healthy():
            return False
        
        # Check if notification matches filters
        if not self.filters.matches(notification):
            return True  # Filtered out, but not an error
        
        start_time = time.time()
        
        for attempt in range(self.max_retries + 1):
            try:
                success = await self.callback.deliver(notification)
                
                if success:
                    delivery_time = time.time() - start_time
                    self.stats.record_success(delivery_time)
                    return True
                
                # If this was the last attempt, record failure
                if attempt == self.max_retries:
                    self.stats.record_failure("Max retries exceeded")
                    return False
                
                # Wait before retry
                if attempt < self.max_retries:
                    await asyncio.sleep(self.retry_delay * (2 ** attempt))  # Exponential backoff
                    
            except Exception as e:
                error_msg = f"Attempt {attempt + 1} failed: {e}"
                
                if attempt == self.max_retries:
                    self.stats.record_failure(error_msg)
                    return False
                
                logger.warning(f"Callback {self.callback_id} delivery failed, retrying: {e}")
                await asyncio.sleep(self.retry_delay * (2 ** attempt))
        
        return False


class ProgressCallbackManager:
    """
    Manages progress callbacks and notifications.
    
    This class provides a centralized system for managing progress callbacks,
    including registration, filtering, delivery, and health monitoring.
    """
    
    def __init__(self, max_concurrent_deliveries: int = 50):
        self._callbacks: Dict[str, RegisteredCallback] = {}
        self._callbacks_by_operation: Dict[str, Set[str]] = {}
        self._lock = asyncio.RLock()
        self._delivery_semaphore = asyncio.Semaphore(max_concurrent_deliveries)
        self._notification_queue: asyncio.Queue = asyncio.Queue()
        self._delivery_task: Optional[asyncio.Task] = None
        self._running = False
        self._stats = {
            "total_notifications": 0,
            "successful_deliveries": 0,
            "failed_deliveries": 0,
            "active_callbacks": 0
        }
    
    async def start(self) -> None:
        """Start the callback manager."""
        if self._running:
            return
        
        self._running = True
        self._delivery_task = asyncio.create_task(self._process_delivery_queue())
        logger.info("Progress callback manager started")
    
    async def stop(self) -> None:
        """Stop the callback manager."""
        if not self._running:
            return
        
        self._running = False
        
        if self._delivery_task:
            self._delivery_task.cancel()
            try:
                await self._delivery_task
            except asyncio.CancelledError:
                pass
        
        # Close all callbacks
        async with self._lock:
            for callback_info in self._callbacks.values():
                try:
                    await callback_info.callback.close()
                except Exception as e:
                    logger.error(f"Error closing callback {callback_info.callback_id}: {e}")
        
        logger.info("Progress callback manager stopped")
    
    async def register_callback(
        self,
        operation_id: str,
        callback: IProgressCallback,
        callback_type: CallbackType,
        filters: Optional[CallbackFilter] = None,
        priority: NotificationPriority = NotificationPriority.NORMAL,
        callback_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        max_retries: int = 3
    ) -> str:
        """Register a new progress callback."""
        if callback_id is None:
            callback_id = str(uuid.uuid4())
        
        async with self._lock:
            if callback_id in self._callbacks:
                raise ValueError(f"Callback {callback_id} already registered")
            
            registered_callback = RegisteredCallback(
                callback_id=callback_id,
                operation_id=operation_id,
                callback=callback,
                callback_type=callback_type,
                priority=priority,
                filters=filters or CallbackFilter(),
                stats=CallbackStats(registration_time=time.time()),
                metadata=metadata or {},
                max_retries=max_retries
            )
            
            self._callbacks[callback_id] = registered_callback
            
            # Add to operation mapping
            if operation_id not in self._callbacks_by_operation:
                self._callbacks_by_operation[operation_id] = set()
            self._callbacks_by_operation[operation_id].add(callback_id)
            
            self._stats["active_callbacks"] = len(self._callbacks)
            
            logger.info(f"Registered callback {callback_id} for operation {operation_id}")
            return callback_id
    
    async def unregister_callback(self, callback_id: str) -> bool:
        """Unregister a progress callback."""
        async with self._lock:
            if callback_id not in self._callbacks:
                return False
            
            callback_info = self._callbacks[callback_id]
            
            # Remove from operation mapping
            operation_callbacks = self._callbacks_by_operation.get(callback_info.operation_id, set())
            operation_callbacks.discard(callback_id)
            if not operation_callbacks:
                del self._callbacks_by_operation[callback_info.operation_id]
            
            # Close callback
            try:
                await callback_info.callback.close()
            except Exception as e:
                logger.error(f"Error closing callback {callback_id}: {e}")
            
            del self._callbacks[callback_id]
            self._stats["active_callbacks"] = len(self._callbacks)
            
            logger.info(f"Unregistered callback {callback_id}")
            return True
    
    async def send_notification(self, notification: ProgressUpdateNotificationDto) -> None:
        """Send a notification to all applicable callbacks."""
        if not self._running:
            return
        
        try:
            await self._notification_queue.put(notification)
            self._stats["total_notifications"] += 1
        except Exception as e:
            logger.error(f"Error queuing notification: {e}")
    
    async def _process_delivery_queue(self) -> None:
        """Process the notification delivery queue."""
        while self._running:
            try:
                # Get notification with timeout
                try:
                    notification = await asyncio.wait_for(
                        self._notification_queue.get(),
                        timeout=1.0
                    )
                except asyncio.TimeoutError:
                    continue
                
                # Find applicable callbacks
                async with self._lock:
                    applicable_callbacks = []
                    
                    # Get callbacks for this specific operation
                    operation_callbacks = self._callbacks_by_operation.get(notification.operation_id, set())
                    for callback_id in operation_callbacks:
                        if callback_id in self._callbacks:
                            applicable_callbacks.append(self._callbacks[callback_id])
                    
                    # Also check for global callbacks (those with no specific operation filter)
                    for callback_info in self._callbacks.values():
                        if (callback_info.operation_id == "*" and 
                            callback_info not in applicable_callbacks):
                            applicable_callbacks.append(callback_info)
                
                # Deliver to applicable callbacks
                if applicable_callbacks:
                    await self._deliver_to_callbacks(notification, applicable_callbacks)
                
                # Mark notification as done
                self._notification_queue.task_done()
                
            except Exception as e:
                logger.error(f"Error processing delivery queue: {e}")
    
    async def _deliver_to_callbacks(
        self,
        notification: ProgressUpdateNotificationDto,
        callbacks: List[RegisteredCallback]
    ) -> None:
        """Deliver notification to a list of callbacks."""
        # Sort callbacks by priority
        callbacks.sort(key=lambda cb: cb.priority.value, reverse=True)
        
        # Create delivery tasks
        delivery_tasks = []
        for callback_info in callbacks:
            if callback_info.active and callback_info.callback.is_healthy():
                task = asyncio.create_task(
                    self._deliver_with_semaphore(notification, callback_info)
                )
                delivery_tasks.append(task)
        
        # Wait for all deliveries to complete
        if delivery_tasks:
            results = await asyncio.gather(*delivery_tasks, return_exceptions=True)
            
            # Update statistics
            for result in results:
                if isinstance(result, bool):
                    if result:
                        self._stats["successful_deliveries"] += 1
                    else:
                        self._stats["failed_deliveries"] += 1
                elif isinstance(result, Exception):
                    self._stats["failed_deliveries"] += 1
                    logger.error(f"Delivery task failed: {result}")
    
    async def _deliver_with_semaphore(
        self,
        notification: ProgressUpdateNotificationDto,
        callback_info: RegisteredCallback
    ) -> bool:
        """Deliver notification with semaphore control."""
        async with self._delivery_semaphore:
            return await callback_info.deliver_with_retry(notification)
    
    def get_callback_stats(self, callback_id: str) -> Optional[CallbackStats]:
        """Get statistics for a specific callback."""
        callback_info = self._callbacks.get(callback_id)
        return callback_info.stats if callback_info else None
    
    def get_manager_stats(self) -> Dict[str, Any]:
        """Get manager-level statistics."""
        return {
            **self._stats,
            "queue_size": self._notification_queue.qsize(),
            "callbacks_by_type": self._get_callbacks_by_type(),
            "callbacks_by_priority": self._get_callbacks_by_priority()
        }
    
    def _get_callbacks_by_type(self) -> Dict[str, int]:
        """Get callback count by type."""
        counts = {}
        for callback_info in self._callbacks.values():
            callback_type = callback_info.callback_type.value
            counts[callback_type] = counts.get(callback_type, 0) + 1
        return counts
    
    def _get_callbacks_by_priority(self) -> Dict[str, int]:
        """Get callback count by priority."""
        counts = {}
        for callback_info in self._callbacks.values():
            priority = callback_info.priority.name
            counts[priority] = counts.get(priority, 0) + 1
        return counts
    
    async def health_check(self) -> Dict[str, Any]:
        """Perform health check on all callbacks."""
        async with self._lock:
            healthy_callbacks = 0
            unhealthy_callbacks = []
            
            for callback_id, callback_info in self._callbacks.items():
                if callback_info.callback.is_healthy():
                    healthy_callbacks += 1
                else:
                    unhealthy_callbacks.append({
                        "callback_id": callback_id,
                        "operation_id": callback_info.operation_id,
                        "type": callback_info.callback_type.value,
                        "stats": callback_info.stats
                    })
            
            return {
                "total_callbacks": len(self._callbacks),
                "healthy_callbacks": healthy_callbacks,
                "unhealthy_callbacks": unhealthy_callbacks,
                "manager_running": self._running,
                "queue_size": self._notification_queue.qsize()
            }