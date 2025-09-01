"""
JSON-based implementation of progress tracking repository.

This module provides a file-based implementation of the progress tracking repository
using JSON for persistence, suitable for single-user scenarios and development.
"""

import asyncio
import json
import logging
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

from ...domain.entities.progress_tracking import (OperationContext,
                                                  OperationStatus,
                                                  OperationType, Priority,
                                                  ProgressLogEntry,
                                                  ProgressMetrics,
                                                  ProgressTrackingEntity,
                                                  ThroughputMetrics)
from ...domain.repositories.progress_tracking_repository import (
    IProgressTrackingRepository, OperationAlreadyExistsError,
    OperationNotFoundError, ProgressTrackingRepositoryError)

logger = logging.getLogger(__name__)


class JsonProgressTrackingRepository(IProgressTrackingRepository):
    """
    JSON file-based implementation of progress tracking repository.
    
    This implementation stores progress tracking data in JSON files with
    thread-safe access and periodic persistence.
    """
    
    def __init__(
        self,
        storage_path: str = "~/.tellus/progress_tracking.json",
        backup_count: int = 5,
        auto_save_interval: float = 30.0
    ):
        """Initialize the JSON progress tracking repository."""
        self.storage_path = Path(storage_path).expanduser()
        self.backup_count = backup_count
        self.auto_save_interval = auto_save_interval
        
        # Thread safety
        self._lock = threading.RLock()
        self._dirty = False
        self._last_save = 0.0
        
        # In-memory storage
        self._operations: Dict[str, Dict[str, Any]] = {}
        
        # Auto-save timer
        self._auto_save_timer = None
        self._shutdown = False
        
        # Ensure storage directory exists
        self.storage_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Load existing data
        self._load_data()
        
        # Start auto-save timer
        self._start_auto_save()
    
    async def create(self, entity: ProgressTrackingEntity) -> None:
        """Create a new progress tracking entity."""
        with self._lock:
            if entity.operation_id in self._operations:
                raise OperationAlreadyExistsError(
                    f"Operation {entity.operation_id} already exists"
                )
            
            self._operations[entity.operation_id] = entity.to_dict()
            self._mark_dirty()
            
            logger.debug(f"Created progress tracking for operation {entity.operation_id}")
    
    async def get_by_id(self, operation_id: str) -> Optional[ProgressTrackingEntity]:
        """Retrieve a progress tracking entity by its operation ID."""
        with self._lock:
            if operation_id not in self._operations:
                return None
            
            data = self._operations[operation_id].copy()
            return self._dict_to_entity(data)
    
    async def update(self, entity: ProgressTrackingEntity) -> None:
        """Update an existing progress tracking entity."""
        with self._lock:
            if entity.operation_id not in self._operations:
                raise OperationNotFoundError(
                    f"Operation {entity.operation_id} not found"
                )
            
            self._operations[entity.operation_id] = entity.to_dict()
            self._mark_dirty()
            
            logger.debug(f"Updated progress tracking for operation {entity.operation_id}")
    
    async def delete(self, operation_id: str) -> bool:
        """Delete a progress tracking entity."""
        with self._lock:
            if operation_id in self._operations:
                del self._operations[operation_id]
                self._mark_dirty()
                logger.debug(f"Deleted progress tracking for operation {operation_id}")
                return True
            return False
    
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
        """List progress tracking entities with optional filtering and pagination."""
        with self._lock:
            # Convert all operations to entities for filtering
            all_operations = []
            for data in self._operations.values():
                try:
                    entity = self._dict_to_entity(data)
                    all_operations.append(entity)
                except Exception as e:
                    logger.warning(f"Failed to convert operation data to entity: {e}")
                    continue
            
            # Apply filters
            filtered_operations = []
            for op in all_operations:
                # Status filter
                if status_filter and op.status not in status_filter:
                    continue
                
                # Operation type filter
                if operation_type_filter and op.operation_type not in operation_type_filter:
                    continue
                
                # Priority filter
                if priority_filter and op.priority not in priority_filter:
                    continue
                
                # User ID filter
                if user_id_filter and op.context.user_id != user_id_filter:
                    continue
                
                # Parent operation filter
                if parent_operation_filter and op.context.parent_operation_id != parent_operation_filter:
                    continue
                
                # Tag filter (must contain at least one tag)
                if tag_filter and not tag_filter.intersection(op.context.tags):
                    continue
                
                filtered_operations.append(op)
            
            # Sort operations
            if order_by == "created_time":
                filtered_operations.sort(key=lambda x: x.created_time, reverse=not ascending)
            elif order_by == "last_update_time":
                filtered_operations.sort(key=lambda x: x.last_update_time, reverse=not ascending)
            elif order_by == "priority":
                filtered_operations.sort(key=lambda x: x.priority, reverse=not ascending)
            
            # Apply pagination
            start_idx = offset
            end_idx = offset + limit if limit else None
            
            return filtered_operations[start_idx:end_idx]
    
    async def get_active_operations(self) -> List[ProgressTrackingEntity]:
        """Get all operations that are currently active."""
        active_statuses = {OperationStatus.PENDING, OperationStatus.INITIALIZING,
                          OperationStatus.RUNNING, OperationStatus.PAUSED}
        return await self.list_operations(status_filter=active_statuses)
    
    async def get_operations_by_parent(
        self, 
        parent_operation_id: str
    ) -> List[ProgressTrackingEntity]:
        """Get all sub-operations for a given parent operation."""
        return await self.list_operations(parent_operation_filter=parent_operation_id)
    
    async def get_operations_by_user(
        self, 
        user_id: str, 
        include_completed: bool = False
    ) -> List[ProgressTrackingEntity]:
        """Get all operations for a specific user."""
        status_filter = None
        if not include_completed:
            status_filter = {OperationStatus.PENDING, OperationStatus.INITIALIZING,
                           OperationStatus.RUNNING, OperationStatus.PAUSED}
        
        return await self.list_operations(
            user_id_filter=user_id,
            status_filter=status_filter
        )
    
    async def get_operations_by_context(
        self,
        simulation_id: Optional[str] = None,
        location_name: Optional[str] = None,
        workflow_id: Optional[str] = None,
        session_id: Optional[str] = None
    ) -> List[ProgressTrackingEntity]:
        """Get operations by context criteria."""
        with self._lock:
            matching_operations = []
            
            for data in self._operations.values():
                try:
                    entity = self._dict_to_entity(data)
                    
                    # Check context filters
                    if (simulation_id and entity.context.simulation_id != simulation_id):
                        continue
                    if (location_name and entity.context.location_name != location_name):
                        continue
                    if (workflow_id and entity.context.workflow_id != workflow_id):
                        continue
                    if (session_id and entity.context.session_id != session_id):
                        continue
                    
                    matching_operations.append(entity)
                    
                except Exception as e:
                    logger.warning(f"Failed to convert operation data to entity: {e}")
                    continue
            
            return matching_operations
    
    async def count_operations(
        self,
        status_filter: Optional[Set[OperationStatus]] = None,
        operation_type_filter: Optional[Set[OperationType]] = None,
        user_id_filter: Optional[str] = None
    ) -> int:
        """Count operations matching the given criteria."""
        operations = await self.list_operations(
            status_filter=status_filter,
            operation_type_filter=operation_type_filter,
            user_id_filter=user_id_filter
        )
        return len(operations)
    
    async def cleanup_completed_operations(
        self, 
        older_than_seconds: float,
        preserve_failed: bool = True
    ) -> int:
        """Clean up completed operations older than the specified time."""
        with self._lock:
            current_time = time.time()
            cutoff_time = current_time - older_than_seconds
            
            to_remove = []
            
            for operation_id, data in self._operations.items():
                try:
                    entity = self._dict_to_entity(data)
                    
                    # Only cleanup terminal operations
                    if not entity.status.is_terminal():
                        continue
                    
                    # Check age
                    completion_time = entity.completed_time or entity.last_update_time
                    if completion_time > cutoff_time:
                        continue
                    
                    # Preserve failed operations if requested
                    if preserve_failed and entity.status == OperationStatus.FAILED:
                        continue
                    
                    to_remove.append(operation_id)
                    
                except Exception as e:
                    logger.warning(f"Failed to process operation {operation_id} for cleanup: {e}")
                    continue
            
            # Remove operations
            for operation_id in to_remove:
                del self._operations[operation_id]
            
            if to_remove:
                self._mark_dirty()
                logger.info(f"Cleaned up {len(to_remove)} completed operations")
            
            return len(to_remove)
    
    async def get_statistics(self) -> Dict[str, Any]:
        """Get repository statistics."""
        with self._lock:
            stats = {
                'total_operations': len(self._operations),
                'active_operations': 0,
                'completed_operations': 0,
                'failed_operations': 0,
                'cancelled_operations': 0,
                'operations_by_type': {},
                'operations_by_status': {},
                'operations_by_priority': {},
                'total_bytes_processed': 0,
                'average_completion_time': None,
                'oldest_active_operation': None
            }
            
            completion_times = []
            oldest_active_time = None
            
            for data in self._operations.values():
                try:
                    entity = self._dict_to_entity(data)
                    
                    # Count by status
                    status_key = entity.status.value
                    stats['operations_by_status'][status_key] = \
                        stats['operations_by_status'].get(status_key, 0) + 1
                    
                    if entity.status.is_active():
                        stats['active_operations'] += 1
                        if oldest_active_time is None or entity.created_time < oldest_active_time:
                            oldest_active_time = entity.created_time
                    elif entity.status == OperationStatus.COMPLETED:
                        stats['completed_operations'] += 1
                        duration = entity.calculate_duration()
                        if duration:
                            completion_times.append(duration)
                    elif entity.status == OperationStatus.FAILED:
                        stats['failed_operations'] += 1
                    elif entity.status == OperationStatus.CANCELLED:
                        stats['cancelled_operations'] += 1
                    
                    # Count by type
                    type_key = entity.operation_type.value
                    stats['operations_by_type'][type_key] = \
                        stats['operations_by_type'].get(type_key, 0) + 1
                    
                    # Count by priority
                    priority_key = entity.priority.name.lower()
                    stats['operations_by_priority'][priority_key] = \
                        stats['operations_by_priority'].get(priority_key, 0) + 1
                    
                    # Sum bytes processed
                    stats['total_bytes_processed'] += entity.current_metrics.bytes_processed
                    
                except Exception as e:
                    logger.warning(f"Failed to process operation for statistics: {e}")
                    continue
            
            # Calculate average completion time
            if completion_times:
                stats['average_completion_time'] = sum(completion_times) / len(completion_times)
            
            # Set oldest active operation timestamp
            if oldest_active_time:
                stats['oldest_active_operation'] = oldest_active_time
            
            return stats
    
    async def bulk_update_status(
        self,
        operation_ids: List[str],
        new_status: OperationStatus,
        reason: Optional[str] = None
    ) -> List[str]:
        """Update status for multiple operations in bulk."""
        with self._lock:
            updated_ids = []
            
            for operation_id in operation_ids:
                if operation_id in self._operations:
                    try:
                        entity = self._dict_to_entity(self._operations[operation_id])
                        
                        # Update status based on the new status
                        if new_status == OperationStatus.CANCELLED:
                            entity.cancel_operation(reason)
                        elif new_status == OperationStatus.FAILED:
                            entity.fail_operation(reason or "Bulk status update")
                        # Add other status updates as needed
                        
                        self._operations[operation_id] = entity.to_dict()
                        updated_ids.append(operation_id)
                        
                    except Exception as e:
                        logger.warning(f"Failed to update operation {operation_id}: {e}")
                        continue
            
            if updated_ids:
                self._mark_dirty()
                logger.info(f"Bulk updated {len(updated_ids)} operations to status {new_status.value}")
            
            return updated_ids
    
    async def exists(self, operation_id: str) -> bool:
        """Check if an operation exists in the repository."""
        with self._lock:
            return operation_id in self._operations
    
    async def get_recent_log_entries(
        self,
        operation_id: str,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Get recent log entries for an operation."""
        with self._lock:
            if operation_id not in self._operations:
                raise OperationNotFoundError(f"Operation {operation_id} not found")
            
            try:
                entity = self._dict_to_entity(self._operations[operation_id])
                log_entries = entity.get_log_entries(limit=limit)
                
                return [entry.to_dict() for entry in log_entries]
                
            except Exception as e:
                logger.error(f"Failed to get log entries for {operation_id}: {e}")
                raise ProgressTrackingRepositoryError(f"Failed to get log entries: {e}")
    
    def _dict_to_entity(self, data: Dict[str, Any]) -> ProgressTrackingEntity:
        """Convert dictionary data to a ProgressTrackingEntity."""
        # Create context
        context_data = data.get('context', {})
        context = OperationContext(
            user_id=context_data.get('user_id'),
            session_id=context_data.get('session_id'),
            parent_operation_id=context_data.get('parent_operation_id'),
            simulation_id=context_data.get('simulation_id'),
            location_name=context_data.get('location_name'),
            workflow_id=context_data.get('workflow_id'),
            tags=set(context_data.get('tags', [])),
            metadata=context_data.get('metadata', {})
        )
        
        # Create entity
        entity = ProgressTrackingEntity(
            operation_id=data['operation_id'],
            operation_type=OperationType(data['operation_type']),
            operation_name=data['operation_name'],
            priority=Priority(data['priority']),
            context=context
        )
        
        # Restore state
        entity._status = OperationStatus(data['status'])
        entity._created_time = data['created_time']
        entity._started_time = data.get('started_time')
        entity._completed_time = data.get('completed_time')
        entity._last_update_time = data['last_update_time']
        
        # Restore metrics
        metrics_data = data.get('current_metrics', {})
        entity._current_metrics = ProgressMetrics(
            percentage=metrics_data.get('percentage', 0.0),
            current_value=metrics_data.get('current_value', 0),
            total_value=metrics_data.get('total_value'),
            bytes_processed=metrics_data.get('bytes_processed', 0),
            total_bytes=metrics_data.get('total_bytes'),
            files_processed=metrics_data.get('files_processed', 0),
            total_files=metrics_data.get('total_files'),
            operations_completed=metrics_data.get('operations_completed', 0),
            total_operations=metrics_data.get('total_operations')
        )
        
        # Restore throughput
        throughput_data = data.get('current_throughput')
        if throughput_data:
            entity._current_throughput = ThroughputMetrics(
                start_time=throughput_data['start_time'],
                current_time=throughput_data.get('current_time'),
                bytes_per_second=throughput_data.get('bytes_per_second', 0.0),
                files_per_second=throughput_data.get('files_per_second', 0.0),
                operations_per_second=throughput_data.get('operations_per_second', 0.0),
                estimated_completion_time=throughput_data.get('estimated_completion_time'),
                estimated_remaining_seconds=throughput_data.get('estimated_remaining_seconds')
            )
        
        # Restore other state
        entity._error_message = data.get('error_message')
        entity._warnings = data.get('warnings', [])
        entity._cancellation_requested = data.get('cancellation_requested', False)
        
        return entity
    
    def _mark_dirty(self) -> None:
        """Mark the repository as dirty (needing save)."""
        self._dirty = True
    
    def _load_data(self) -> None:
        """Load data from the JSON file."""
        try:
            if self.storage_path.exists():
                with open(self.storage_path, 'r') as f:
                    data = json.load(f)
                    self._operations = data.get('operations', {})
                    logger.info(f"Loaded {len(self._operations)} progress tracking operations")
            else:
                self._operations = {}
                logger.info("No existing progress tracking data found")
                
        except Exception as e:
            logger.error(f"Failed to load progress tracking data: {e}")
            self._operations = {}
    
    def _save_data(self) -> None:
        """Save data to the JSON file."""
        if not self._dirty:
            return
        
        try:
            # Create backup if file exists
            if self.storage_path.exists():
                self._create_backup()
            
            # Save data
            data = {
                'operations': self._operations,
                'metadata': {
                    'last_saved': time.time(),
                    'version': '1.0'
                }
            }
            
            # Write to temporary file first, then rename for atomicity
            temp_path = self.storage_path.with_suffix('.tmp')
            with open(temp_path, 'w') as f:
                json.dump(data, f, indent=2, default=str)
            
            temp_path.rename(self.storage_path)
            
            self._dirty = False
            self._last_save = time.time()
            logger.debug(f"Saved progress tracking data ({len(self._operations)} operations)")
            
        except Exception as e:
            logger.error(f"Failed to save progress tracking data: {e}")
    
    def _create_backup(self) -> None:
        """Create a backup of the current data file."""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_path = self.storage_path.with_suffix(f'.{timestamp}.bak')
            
            # Copy current file to backup
            import shutil
            shutil.copy2(self.storage_path, backup_path)
            
            # Clean up old backups
            self._cleanup_old_backups()
            
        except Exception as e:
            logger.warning(f"Failed to create backup: {e}")
    
    def _cleanup_old_backups(self) -> None:
        """Remove old backup files beyond the backup count."""
        try:
            backup_pattern = f"{self.storage_path.stem}.*.bak"
            backup_files = list(self.storage_path.parent.glob(backup_pattern))
            
            if len(backup_files) > self.backup_count:
                # Sort by modification time and remove oldest
                backup_files.sort(key=lambda x: x.stat().st_mtime)
                for old_backup in backup_files[:-self.backup_count]:
                    old_backup.unlink()
                    
        except Exception as e:
            logger.warning(f"Failed to cleanup old backups: {e}")
    
    def _start_auto_save(self) -> None:
        """Start the auto-save timer."""
        if self._auto_save_timer:
            self._auto_save_timer.cancel()
        
        self._auto_save_timer = threading.Timer(self.auto_save_interval, self._auto_save_callback)
        self._auto_save_timer.daemon = True
        self._auto_save_timer.start()
    
    def _auto_save_callback(self) -> None:
        """Auto-save callback function."""
        if not self._shutdown:
            with self._lock:
                self._save_data()
            
            # Schedule next auto-save
            self._start_auto_save()
    
    def shutdown(self) -> None:
        """Shutdown the repository and save any pending data."""
        self._shutdown = True
        
        if self._auto_save_timer:
            self._auto_save_timer.cancel()
        
        with self._lock:
            self._save_data()
        
        logger.info("Progress tracking repository shutdown complete")