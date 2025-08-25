"""Specialized managers for TUI operations."""

import asyncio
from typing import Dict, List, Optional, Any, Callable
from datetime import datetime
import threading
import queue
from dataclasses import dataclass
from enum import Enum

# Feature flags and legacy bridge removed - using new architecture directly
from ...application.container import get_service_container


class OperationStatus(Enum):
    """Operation status enumeration."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    PAUSED = "paused"


@dataclass
class OperationInfo:
    """Information about an archive operation."""
    id: str
    type: str  # copy, move, extract, create
    archive_id: str
    source_location: str
    destination_location: str
    status: OperationStatus
    progress: float = 0.0
    current_step: str = ""
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    error_message: str = ""
    files_processed: int = 0
    bytes_processed: int = 0
    total_files: int = 0
    total_bytes: int = 0
    transfer_rate: float = 0.0  # MB/s


class OperationManager:
    """Manager for archive operations with real-time monitoring."""
    
    def __init__(self):
        """Initialize the operation manager."""
        self.operations: Dict[str, OperationInfo] = {}
        self.active_operations: List[str] = []
        self.max_concurrent = 3
        self.operation_queue = queue.Queue()
        self.status_callbacks: List[Callable[[str, OperationInfo], None]] = []
        self.archive_service = self._get_archive_service()
        self._stop_monitoring = False
        self._monitor_thread = None
        
    def _get_archive_service(self):
        """Get archive service using new architecture."""
        service_container = get_service_container()
        return service_container.service_factory.archive_service
        
    def add_status_callback(self, callback: Callable[[str, OperationInfo], None]) -> None:
        """Add a callback for operation status updates."""
        self.status_callbacks.append(callback)
        
    def remove_status_callback(self, callback: Callable[[str, OperationInfo], None]) -> None:
        """Remove a status update callback."""
        if callback in self.status_callbacks:
            self.status_callbacks.remove(callback)
            
    def _notify_status_change(self, operation_id: str, operation_info: OperationInfo) -> None:
        """Notify all callbacks of status changes."""
        for callback in self.status_callbacks:
            try:
                callback(operation_id, operation_info)
            except Exception:
                # Ignore callback errors to prevent breaking the manager
                pass
                
    def queue_operation(self, 
                       operation_type: str,
                       archive_id: str,
                       source_location: str,
                       destination_location: str,
                       **kwargs) -> str:
        """Queue a new operation for execution."""
        operation_id = f"{operation_type}_{archive_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        operation_info = OperationInfo(
            id=operation_id,
            type=operation_type,
            archive_id=archive_id,
            source_location=source_location,
            destination_location=destination_location,
            status=OperationStatus.PENDING
        )
        
        self.operations[operation_id] = operation_info
        self.operation_queue.put((operation_id, kwargs))
        
        self._notify_status_change(operation_id, operation_info)
        
        # Start monitor thread if not running
        if self._monitor_thread is None or not self._monitor_thread.is_alive():
            self.start_monitoring()
            
        return operation_id
        
    def start_monitoring(self) -> None:
        """Start the operation monitoring thread."""
        if self._monitor_thread and self._monitor_thread.is_alive():
            return
            
        self._stop_monitoring = False
        self._monitor_thread = threading.Thread(target=self._monitor_operations, daemon=True)
        self._monitor_thread.start()
        
    def stop_monitoring(self) -> None:
        """Stop the operation monitoring thread."""
        self._stop_monitoring = True
        if self._monitor_thread:
            self._monitor_thread.join(timeout=5.0)
            
    def _monitor_operations(self) -> None:
        """Monitor operations in a separate thread."""
        while not self._stop_monitoring:
            try:
                # Process pending operations
                self._process_pending_operations()
                
                # Update active operations
                self._update_active_operations()
                
                # Sleep briefly to avoid busy waiting
                threading.Event().wait(2.0)
                
            except Exception:
                # Continue monitoring even if there are errors
                threading.Event().wait(5.0)
                
    def _process_pending_operations(self) -> None:
        """Process pending operations from the queue."""
        while len(self.active_operations) < self.max_concurrent:
            try:
                operation_id, kwargs = self.operation_queue.get_nowait()
                if operation_id in self.operations:
                    operation_info = self.operations[operation_id]
                    if operation_info.status == OperationStatus.PENDING:
                        self._start_operation(operation_id, kwargs)
            except queue.Empty:
                break
                
    def _start_operation(self, operation_id: str, kwargs: Dict[str, Any]) -> None:
        """Start executing an operation."""
        operation_info = self.operations[operation_id]
        operation_info.status = OperationStatus.RUNNING
        operation_info.start_time = datetime.now()
        
        self.active_operations.append(operation_id)
        self._notify_status_change(operation_id, operation_info)
        
        # Start operation in a separate thread
        operation_thread = threading.Thread(
            target=self._execute_operation,
            args=(operation_id, kwargs),
            daemon=True
        )
        operation_thread.start()
        
    def _execute_operation(self, operation_id: str, kwargs: Dict[str, Any]) -> None:
        """Execute an operation (runs in separate thread)."""
        operation_info = self.operations[operation_id]
        
        try:
            if not self.archive_service:
                raise Exception("Archive service not available")
                
            # Execute based on operation type
            if operation_info.type == "copy":
                self._execute_copy_operation(operation_info, kwargs)
            elif operation_info.type == "move":
                self._execute_move_operation(operation_info, kwargs)
            elif operation_info.type == "extract":
                self._execute_extract_operation(operation_info, kwargs)
            else:
                raise Exception(f"Unknown operation type: {operation_info.type}")
                
            # Mark as completed
            operation_info.status = OperationStatus.COMPLETED
            operation_info.end_time = datetime.now()
            operation_info.progress = 100.0
            
        except Exception as e:
            operation_info.status = OperationStatus.FAILED
            operation_info.error_message = str(e)
            operation_info.end_time = datetime.now()
            
        finally:
            # Remove from active operations
            if operation_id in self.active_operations:
                self.active_operations.remove(operation_id)
            self._notify_status_change(operation_id, operation_info)
            
    def _execute_copy_operation(self, operation_info: OperationInfo, kwargs: Dict[str, Any]) -> None:
        """Execute a copy operation."""
        # Simulate copy operation with progress updates
        for i in range(0, 101, 10):
            if self._stop_monitoring:
                operation_info.status = OperationStatus.CANCELLED
                return
                
            operation_info.progress = float(i)
            operation_info.current_step = f"Copying files... {i}%"
            operation_info.files_processed = i // 10
            operation_info.bytes_processed = i * 1024 * 1024  # Simulate bytes
            operation_info.transfer_rate = 15.5  # Simulate transfer rate
            
            self._notify_status_change(operation_info.id, operation_info)
            threading.Event().wait(1.0)  # Simulate work
            
    def _execute_move_operation(self, operation_info: OperationInfo, kwargs: Dict[str, Any]) -> None:
        """Execute a move operation."""
        # Similar to copy but with different steps
        steps = ["Copying files", "Verifying integrity", "Cleaning up source"]
        
        for step_idx, step in enumerate(steps):
            for i in range(0, 34):  # 33% per step
                if self._stop_monitoring:
                    operation_info.status = OperationStatus.CANCELLED
                    return
                    
                progress = (step_idx * 33) + i
                operation_info.progress = float(progress)
                operation_info.current_step = f"{step}... {progress}%"
                
                self._notify_status_change(operation_info.id, operation_info)
                threading.Event().wait(0.5)
                
    def _execute_extract_operation(self, operation_info: OperationInfo, kwargs: Dict[str, Any]) -> None:
        """Execute an extract operation."""
        # Simulate extract with file-by-file progress
        files = ["input.nc", "config.yaml", "output.nc", "results.nc", "logs.txt"]
        
        for idx, filename in enumerate(files):
            if self._stop_monitoring:
                operation_info.status = OperationStatus.CANCELLED
                return
                
            progress = (idx + 1) / len(files) * 100
            operation_info.progress = progress
            operation_info.current_step = f"Extracting {filename}"
            operation_info.files_processed = idx + 1
            operation_info.total_files = len(files)
            
            self._notify_status_change(operation_info.id, operation_info)
            threading.Event().wait(2.0)  # Simulate extraction time
            
    def _update_active_operations(self) -> None:
        """Update status of active operations."""
        # This would typically poll the actual service for status updates
        # For now, we rely on the operation threads to update status
        pass
        
    def cancel_operation(self, operation_id: str) -> bool:
        """Cancel an operation."""
        if operation_id in self.operations:
            operation_info = self.operations[operation_id]
            if operation_info.status in (OperationStatus.PENDING, OperationStatus.RUNNING):
                operation_info.status = OperationStatus.CANCELLED
                operation_info.end_time = datetime.now()
                
                if operation_id in self.active_operations:
                    self.active_operations.remove(operation_id)
                    
                self._notify_status_change(operation_id, operation_info)
                return True
        return False
        
    def pause_operation(self, operation_id: str) -> bool:
        """Pause an operation."""
        if operation_id in self.operations:
            operation_info = self.operations[operation_id]
            if operation_info.status == OperationStatus.RUNNING:
                operation_info.status = OperationStatus.PAUSED
                self._notify_status_change(operation_id, operation_info)
                return True
        return False
        
    def resume_operation(self, operation_id: str) -> bool:
        """Resume a paused operation."""
        if operation_id in self.operations:
            operation_info = self.operations[operation_id]
            if operation_info.status == OperationStatus.PAUSED:
                operation_info.status = OperationStatus.RUNNING
                self._notify_status_change(operation_id, operation_info)
                return True
        return False
        
    def get_operation_status(self, operation_id: str) -> Optional[OperationInfo]:
        """Get current status of an operation."""
        return self.operations.get(operation_id)
        
    def get_all_operations(self) -> List[OperationInfo]:
        """Get all operations."""
        return list(self.operations.values())
        
    def get_active_operations(self) -> List[OperationInfo]:
        """Get currently active operations."""
        return [self.operations[op_id] for op_id in self.active_operations 
                if op_id in self.operations]
                
    def clear_completed_operations(self) -> None:
        """Clear completed and failed operations."""
        to_remove = [
            op_id for op_id, op_info in self.operations.items()
            if op_info.status in (OperationStatus.COMPLETED, OperationStatus.FAILED, OperationStatus.CANCELLED)
        ]
        
        for op_id in to_remove:
            del self.operations[op_id]


class LocationManager:
    """Manager for storage locations and path templates."""
    
    def __init__(self):
        """Initialize the location manager."""
        self.locations_cache = {}
        self.last_refresh = None
        
    def get_locations(self, refresh: bool = False) -> List[Dict[str, Any]]:
        """Get all storage locations."""
        if refresh or not self.locations_cache:
            self._refresh_locations()
        return list(self.locations_cache.values())
        
    def _refresh_locations(self) -> None:
        """Refresh locations from the service."""
        try:
            # Use new architecture
            service_container = get_service_container()
            location_service = service_container.service_factory.location_service
            
            result = location_service.list_locations()
            self.locations_cache = {}
            
            if result.success:
                for loc in result.locations:
                    self.locations_cache[loc.name] = {
                        'name': loc.name,
                        'protocol': loc.protocol or 'local',
                        'host': loc.config.get('host', '') if loc.config else '',
                        'port': loc.config.get('port', None) if loc.config else None,
                        'path_prefix': loc.path or '',
                        'kinds': [k if isinstance(k, str) else k.name for k in loc.kinds],
                        'connected': True  # Would need to implement connection testing
                    }
            self.last_refresh = datetime.now()
            
        except Exception:
            # If refresh fails, keep existing cache
            pass
            
    def _test_connection(self, location) -> bool:
        """Test connection to a location."""
        try:
            # This would perform an actual connection test
            return True
        except Exception:
            return False
            
    def create_location(self, config: Dict[str, Any]) -> bool:
        """Create a new location."""
        try:
            # Use new architecture
            service_container = get_service_container()
            location_service = service_container.service_factory.location_service
            
            from ...application.dtos import CreateLocationDto
            
            # Create location using DTO
            dto = CreateLocationDto(
                name=config['name'],
                protocol=config.get('protocol', 'local'),
                path=config.get('path_prefix', ''),
                kinds=config.get('kinds', []),
                config={
                    'host': config.get('host', ''),
                    'port': config.get('port'),
                    **{k: v for k, v in config.items() if k not in ['name', 'protocol', 'path_prefix', 'kinds']}
                }
            )
            
            result = location_service.create_location(dto)
            
            if result.success:
                # Update cache
                self.locations_cache[config['name']] = config
                return True
            else:
                return False
            
        except Exception:
            return False
            
    def update_location(self, name: str, config: Dict[str, Any]) -> bool:
        """Update an existing location."""
        try:
            # This would update the location using the actual service
            self.locations_cache[name] = config
            return True
        except Exception:
            return False
            
    def delete_location(self, name: str) -> bool:
        """Delete a location."""
        try:
            # This would delete the location using the actual service
            if name in self.locations_cache:
                del self.locations_cache[name]
            return True
        except Exception:
            return False
            
    def test_location_connection(self, name: str) -> Dict[str, Any]:
        """Test connection to a location."""
        if name not in self.locations_cache:
            return {'success': False, 'error': 'Location not found'}
            
        try:
            # This would perform actual connection test
            return {
                'success': True,
                'response_time': 0.123,
                'available_space': '1.2 TB',
                'permissions': 'read/write'
            }
        except Exception as e:
            return {'success': False, 'error': str(e)}
            
    def resolve_path_template(self, location_name: str, simulation_context: Dict[str, Any]) -> str:
        """Resolve a path template with simulation context."""
        if location_name not in self.locations_cache:
            return ""
            
        location = self.locations_cache[location_name]
        template = location.get('path_prefix', '')
        
        if not template:
            return ""
            
        try:
            return template.format(**simulation_context)
        except KeyError:
            # Return template with unresolved variables if context is incomplete
            return template


class ArchiveManager:
    """Manager for archive operations and metadata."""
    
    def __init__(self):
        """Initialize the archive manager."""
        self.archives_cache = {}
        self.archive_service = self._get_archive_service()
        
    def _get_archive_service(self):
        """Get archive service using new architecture."""
        service_container = get_service_container()
        return service_container.service_factory.archive_service
        
    def get_archives(self, simulation_id: str = "", refresh: bool = False) -> List[Dict[str, Any]]:
        """Get archives, optionally filtered by simulation."""
        cache_key = simulation_id or "all"
        
        if refresh or cache_key not in self.archives_cache:
            self._refresh_archives(simulation_id)
            
        return self.archives_cache.get(cache_key, [])
        
    def _refresh_archives(self, simulation_id: str = "") -> None:
        """Refresh archives from the service."""
        try:
            # Always use new architecture service
            from ...application.dtos import ListArchivesDto
            dto = ListArchivesDto(simulation_id=simulation_id or None)
            result = self.archive_service.list_archives(dto)
            
            archives = []
            if result.success:
                for archive in result.archives:
                    archives.append({
                        'archive_id': archive.archive_id,
                        'name': archive.name or archive.archive_id,
                        'simulation': archive.simulation_id or 'Unknown',
                        'size': archive.size or 0,
                        'cached': False,  # Would need cache status check
                        'location': archive.location_name or 'Unknown',
                        'archive_type': 'Archive'
                    })
            
            cache_key = simulation_id or "all"
            self.archives_cache[cache_key] = archives
            
        except Exception:
            # Keep existing cache on error
            pass
            
    def _get_legacy_archives(self, simulation_id: str = "") -> List[Dict[str, Any]]:
        """Get archives using new architecture (legacy method name kept for compatibility)."""
        # This method is now redirected to use the new architecture
        try:
            from ...application.dtos import ListArchivesDto
            dto = ListArchivesDto(simulation_id=simulation_id or None)
            result = self.archive_service.list_archives(dto)
            
            archives = []
            if result.success:
                for archive in result.archives:
                    archives.append({
                        'archive_id': archive.archive_id,
                        'name': archive.name or archive.archive_id,
                        'simulation': archive.simulation_id or 'Unknown',
                        'size': archive.size or 0,
                        'cached': False,  # Would need cache status check
                        'location': archive.location_name or 'Unknown',
                        'archive_type': 'Archive'  # Simplified for now
                    })
            
            return archives
        except Exception:
            return []
        
    def get_archive_files(self, archive_id: str, 
                         content_type_filter: str = "",
                         pattern_filter: str = "") -> List[Dict[str, Any]]:
        """Get files in an archive with optional filtering."""
        try:
            # Use new architecture service
            from ...application.dtos import ListArchiveFilesDto
            dto = ListArchiveFilesDto(
                archive_id=archive_id,
                content_type_filter=content_type_filter or None,
                pattern_filter=pattern_filter or None
            )
            result = self.archive_service.list_archive_files(dto)
            return result.files if result.success else []
        except Exception:
            return []
            
    def create_archive(self, config: Dict[str, Any]) -> bool:
        """Create a new archive."""
        try:
            # Use new architecture service
            from ...application.dtos import CreateArchiveDto
            dto = CreateArchiveDto(
                archive_id=config['archive_id'],
                simulation_id=config.get('simulation_id', ''),
                archive_path=config['archive_path'],
                location_name=config.get('location'),
                name=config.get('name'),
                tags=config.get('tags', [])
            )
            result = self.archive_service.create_archive(dto)
            return result.success
        except Exception:
            return False