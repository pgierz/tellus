"""Reusable widgets for the Tellus TUI."""

from typing import Optional, List, Dict, Any, Callable
import asyncio
import time
from datetime import datetime

from textual.widget import Widget
from textual.containers import Container, Horizontal, Vertical
from textual.widgets import (
    Static, DataTable, Tree, ProgressBar, Log,
    Button, Input, Label, Select
)
from textual.reactive import reactive, var
from textual.message import Message
from textual import on

# Feature flags removed - using new architecture directly
from ...application.container import get_service_container


class ArchiveList(Widget):
    """Widget for displaying and managing a list of archives."""
    
    class ArchiveSelected(Message):
        """Message sent when an archive is selected."""
        
        def __init__(self, archive_id: str, archive_data: Dict[str, Any]):
            super().__init__()
            self.archive_id = archive_id
            self.archive_data = archive_data

    selected_archive = reactive("")
    
    def __init__(self, **kwargs):
        """Initialize the archive list."""
        super().__init__(**kwargs)
        self.archives = []

    def compose(self):
        """Create the archive list layout."""
        with Container():
            yield DataTable(id="archives-table")

    def on_mount(self) -> None:
        """Initialize the widget when mounted."""
        table = self.query_one("#archives-table")
        table.add_columns("ID", "Name", "Simulation", "Size", "Location", "Cached")
        table.cursor_type = "row"

    def update_data(self, archives: List[Dict[str, Any]]) -> None:
        """Update the archive list data."""
        self.archives = archives
        table = self.query_one("#archives-table")
        table.clear()
        
        for archive in archives:
            size_str = self._format_size(archive.get('size', 0))
            cached = "✓" if archive.get('cached', False) else "✗"
            
            table.add_row(
                archive.get('id', ''),
                archive.get('name', ''),
                archive.get('simulation', ''),
                size_str,
                archive.get('location', ''),
                cached,
                key=archive.get('id', '')
            )

    @on(DataTable.RowSelected, "#archives-table")
    def on_archive_selected(self, event: DataTable.RowSelected) -> None:
        """Handle archive selection."""
        if event.row_key:
            self.selected_archive = str(event.row_key.value)
            archive_data = next(
                (arch for arch in self.archives if arch.get('id') == self.selected_archive),
                {}
            )
            self.post_message(self.ArchiveSelected(self.selected_archive, archive_data))

    def _format_size(self, bytes_val: int) -> str:
        """Format bytes as human readable size."""
        if bytes_val is None or bytes_val == 0:
            return "0 B"
        
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if bytes_val < 1024.0:
                return f"{bytes_val:.1f} {unit}"
            bytes_val /= 1024.0
        return f"{bytes_val:.1f} PB"


class LocationList(Widget):
    """Widget for displaying and managing storage locations."""
    
    class LocationSelected(Message):
        """Message sent when a location is selected."""
        
        def __init__(self, location_name: str, location_data: Dict[str, Any]):
            super().__init__()
            self.location_name = location_name
            self.location_data = location_data

    selected_location = reactive("")
    
    def __init__(self, **kwargs):
        """Initialize the location list."""
        super().__init__(**kwargs)
        self.locations = []

    def compose(self):
        """Create the location list layout."""
        with Container():
            yield DataTable(id="locations-table")

    def on_mount(self) -> None:
        """Initialize the widget when mounted."""
        table = self.query_one("#locations-table")
        table.add_columns("Name", "Type", "Protocol", "Status")
        table.cursor_type = "row"

    def update_data(self, locations: List[Dict[str, Any]]) -> None:
        """Update the location list data."""
        self.locations = locations
        table = self.query_one("#locations-table")
        table.clear()
        
        for location in locations:
            kinds = ', '.join(location.get('kinds', []))
            status = "🟢 Connected" if location.get('connected', False) else "🔴 Disconnected"
            
            table.add_row(
                location.get('name', ''),
                kinds,
                location.get('protocol', ''),
                status,
                key=location.get('name', '')
            )

    @on(DataTable.RowSelected, "#locations-table")
    def on_location_selected(self, event: DataTable.RowSelected) -> None:
        """Handle location selection."""
        if event.row_key:
            self.selected_location = str(event.row_key.value)
            location_data = next(
                (loc for loc in self.locations if loc.get('name') == self.selected_location),
                {}
            )
            self.post_message(self.LocationSelected(self.selected_location, location_data))


class OperationList(Widget):
    """Widget for displaying and managing archive operations."""
    
    class OperationSelected(Message):
        """Message sent when an operation is selected."""
        
        def __init__(self, operation_id: str, operation_data: Dict[str, Any]):
            super().__init__()
            self.operation_id = operation_id
            self.operation_data = operation_data

    selected_operation = reactive("")
    
    def __init__(self, **kwargs):
        """Initialize the operation list."""
        super().__init__(**kwargs)
        self.operations = []

    def compose(self):
        """Create the operation list layout."""
        with Container():
            yield DataTable(id="operations-table")

    def on_mount(self) -> None:
        """Initialize the widget when mounted."""
        table = self.query_one("#operations-table")
        table.add_columns("ID", "Type", "Archive", "Status", "Progress", "Rate")
        table.cursor_type = "row"

    def update_data(self, operations: List[Dict[str, Any]]) -> None:
        """Update the operation list data."""
        self.operations = operations
        table = self.query_one("#operations-table")
        table.clear()
        
        for operation in operations:
            progress = f"{operation.get('progress', 0):.1f}%"
            rate = operation.get('rate', '')
            if rate:
                rate = f"{rate:.1f} MB/s"
            
            status_icon = {
                'pending': '⏳',
                'running': '🔄',
                'completed': '✅',
                'failed': '❌',
                'cancelled': '⏹️'
            }.get(operation.get('status', 'unknown'), '❓')
            
            table.add_row(
                operation.get('id', ''),
                operation.get('type', ''),
                operation.get('archive', ''),
                f"{status_icon} {operation.get('status', '').title()}",
                progress,
                rate,
                key=operation.get('id', '')
            )

    @on(DataTable.RowSelected, "#operations-table")
    def on_operation_selected(self, event: DataTable.RowSelected) -> None:
        """Handle operation selection."""
        if event.row_key:
            self.selected_operation = str(event.row_key.value)
            operation_data = next(
                (op for op in self.operations if op.get('id') == self.selected_operation),
                {}
            )
            self.post_message(self.OperationSelected(self.selected_operation, operation_data))


class StatusBar(Widget):
    """Status bar widget for displaying system messages."""
    
    current_message = reactive("")
    message_type = reactive("info")  # info, warning, error, success
    
    def __init__(self, **kwargs):
        """Initialize the status bar."""
        super().__init__(**kwargs)

    def compose(self):
        """Create the status bar layout."""
        with Horizontal():
            yield Static("Ready", id="status-message")
            yield Static("", id="status-details")
            # Always using new architecture now
            yield Static("✨ Clean Architecture", id="service-indicator")

    def update_message(self, message: str, error: bool = False, warning: bool = False, success: bool = False) -> None:
        """Update the status message."""
        self.current_message = message
        
        if error:
            self.message_type = "error"
            icon = "❌"
        elif warning:
            self.message_type = "warning"
            icon = "⚠️"
        elif success:
            self.message_type = "success"
            icon = "✅"
        else:
            self.message_type = "info"
            icon = "ℹ️"
        
        status_widget = self.query_one("#status-message")
        status_widget.update(f"{icon} {message}")
        
        # Auto-clear success/info messages after 5 seconds
        if not error and not warning:
            self.set_timer(5.0, self.clear_message)

    def clear_message(self) -> None:
        """Clear the current status message."""
        status_widget = self.query_one("#status-message")
        status_widget.update("Ready")


class LogViewer(Widget):
    """Widget for displaying system logs."""
    
    def __init__(self, **kwargs):
        """Initialize the log viewer."""
        super().__init__(**kwargs)
        self.log_entries = []

    def compose(self):
        """Create the log viewer layout."""
        with Container():
            yield Log(id="log-content")

    def add_log(self, message: str, level: str = "INFO") -> None:
        """Add a log entry."""
        timestamp = datetime.now().strftime("%H:%M:%S")
        log_entry = f"[{timestamp}] [{level}] {message}"
        self.log_entries.append(log_entry)
        
        log_widget = self.query_one("#log-content")
        log_widget.write_line(log_entry)

    def clear(self) -> None:
        """Clear all log entries."""
        self.log_entries.clear()
        log_widget = self.query_one("#log-content")
        log_widget.clear()


class ArchiveProgress(Widget):
    """Widget for displaying archive operation progress."""
    
    progress_value = reactive(0.0)
    total_value = reactive(100.0)
    current_file = reactive("")
    transfer_rate = reactive("")
    
    def __init__(self, operation_id: str = "", **kwargs):
        """Initialize the progress widget."""
        super().__init__(**kwargs)
        self.operation_id = operation_id

    def compose(self):
        """Create the progress widget layout."""
        with Container():
            yield Label(f"Operation: {self.operation_id}", id="operation-label")
            yield ProgressBar(total=100, id="progress-bar")
            yield Static("0% complete", id="progress-text")
            yield Static("", id="current-file-text")
            yield Static("", id="transfer-rate-text")

    def update_progress(self, 
                       progress: float, 
                       total: float = 100.0,
                       current_file: str = "",
                       transfer_rate: str = "") -> None:
        """Update the progress display."""
        self.progress_value = progress
        self.total_value = total
        self.current_file = current_file
        self.transfer_rate = transfer_rate
        
        # Update progress bar
        progress_bar = self.query_one("#progress-bar")
        progress_bar.total = total
        progress_bar.progress = progress
        
        # Update text displays
        percentage = (progress / total * 100) if total > 0 else 0
        self.query_one("#progress-text").update(f"{percentage:.1f}% complete")
        
        if current_file:
            self.query_one("#current-file-text").update(f"Current: {current_file}")
        
        if transfer_rate:
            self.query_one("#transfer-rate-text").update(f"Rate: {transfer_rate}")


class FileTree(Widget):
    """Widget for displaying archive file trees."""
    
    class FileSelected(Message):
        """Message sent when a file is selected."""
        
        def __init__(self, file_path: str, file_data: Dict[str, Any]):
            super().__init__()
            self.file_path = file_path
            self.file_data = file_data

    selected_file = reactive("")
    
    def __init__(self, **kwargs):
        """Initialize the file tree widget."""
        super().__init__(**kwargs)
        self.files_data = []

    def compose(self):
        """Create the file tree layout."""
        with Container():
            yield Tree("Files", id="file-tree")

    def update_files(self, files: List[Dict[str, Any]]) -> None:
        """Update the file tree with new data."""
        self.files_data = files
        tree = self.query_one("#file-tree")
        tree.clear()
        
        # Build tree structure from file paths
        root = tree.root
        directories = {}
        
        for file_info in files:
            path_parts = file_info['path'].split('/')
            current_node = root
            
            # Build directory structure
            for i, part in enumerate(path_parts[:-1]):
                dir_path = '/'.join(path_parts[:i+1])
                if dir_path not in directories:
                    current_node = current_node.add(f"{part}/", data={"type": "directory", "path": dir_path})
                    directories[dir_path] = current_node
                else:
                    current_node = directories[dir_path]
            
            # Add file
            file_name = path_parts[-1]
            file_node = current_node.add_leaf(file_name, data=file_info)

    @on(Tree.NodeSelected, "#file-tree")
    def on_file_selected(self, event: Tree.NodeSelected) -> None:
        """Handle file selection in the tree."""
        if event.node.data and event.node.data.get('type') != 'directory':
            self.selected_file = event.node.data['path']
            self.post_message(self.FileSelected(self.selected_file, event.node.data))


class OperationMonitor(Widget):
    """Widget for monitoring real-time operation status."""
    
    def __init__(self, operation_id: str = "", **kwargs):
        """Initialize the operation monitor."""
        super().__init__(**kwargs)
        self.operation_id = operation_id
        self.is_monitoring = False

    def compose(self):
        """Create the operation monitor layout."""
        with Container():
            yield Static(f"Monitoring: {self.operation_id}", classes="section-header")
            yield ArchiveProgress(self.operation_id, id="operation-progress")
            
            with Container():
                yield Static("Operation Details:", classes="section-header")
                yield Static("", id="operation-details")
            
            with Container():
                yield Static("Recent Activity:", classes="section-header")
                yield Log(id="operation-log")

    def start_monitoring(self) -> None:
        """Start monitoring the operation."""
        self.is_monitoring = True
        self.set_interval(2.0, self.update_status)

    def stop_monitoring(self) -> None:
        """Stop monitoring the operation."""
        self.is_monitoring = False

    async def update_status(self) -> None:
        """Update operation status from the service."""
        if not self.is_monitoring:
            return
        
        # This would fetch actual operation status from the service
        # For now, we'll simulate progress
        progress_widget = self.query_one("#operation-progress")
        
        # Simulate progress update
        current_progress = getattr(progress_widget, 'progress_value', 0)
        new_progress = min(current_progress + 5, 100)
        
        progress_widget.update_progress(
            progress=new_progress,
            current_file="simulation_output.nc",
            transfer_rate="15.2 MB/s"
        )
        
        # Add log entry
        log_widget = self.query_one("#operation-log")
        log_widget.write_line(f"Progress: {new_progress}% - Transferred file: simulation_output.nc")


class BulkOperationPanel(Widget):
    """Widget for configuring and managing bulk operations."""
    
    def __init__(self, **kwargs):
        """Initialize the bulk operation panel."""
        super().__init__(**kwargs)

    def compose(self):
        """Create the bulk operation panel layout."""
        with Container():
            yield Static("Bulk Operation Configuration", classes="section-header")
            
            with Vertical():
                with Horizontal():
                    yield Label("Operation Type:")
                    yield Select([
                        ("Copy", "copy"),
                        ("Move", "move"),
                        ("Extract", "extract"),
                    ], id="bulk-operation-type")
                
                with Horizontal():
                    yield Label("Source Location:")
                    yield Select([], id="bulk-source-location")
                
                with Horizontal():
                    yield Label("Destination Location:")
                    yield Select([], id="bulk-dest-location")
                
                with Horizontal():
                    yield Label("Max Concurrent:")
                    yield Select([
                        ("1", "1"),
                        ("2", "2"),
                        ("3", "3"),
                        ("5", "5"),
                    ], id="bulk-max-concurrent")
                
                with Horizontal():
                    yield Label("Archive Selection:")
                    yield DataTable(id="bulk-archive-selection")
                
                with Horizontal():
                    yield Button("Select All", id="select-all-archives")
                    yield Button("Select None", id="select-none-archives")
                    yield Button("Start Bulk Operation", id="start-bulk-operation", variant="primary")


class ProgressTrackingWidget(Widget):
    """Widget for displaying real-time progress tracking of operations."""
    
    def __init__(self, **kwargs):
        """Initialize the progress tracking widget."""
        super().__init__(**kwargs)
        self.operations_data = []
        self.queue_stats = {}
        
        # Initialize services
        service_container = get_service_container()
        self.queue_service = service_container.service_factory.operation_queue_service
        self.progress_service = service_container.service_factory.progress_tracking_service
        
    def compose(self):
        """Create the progress tracking layout."""
        with Container():
            yield Static("Operation Progress", classes="section-header")
            
            # Live Operations Table
            with Vertical():
                yield Static("Active Operations", classes="subsection-header")
                yield DataTable(id="progress-operations", cursor_type="row")
                
                # Queue Statistics
                yield Static("Queue Statistics", classes="subsection-header")
                with Horizontal(classes="stats-row"):
                    yield Static("Running: 0", id="stat-running", classes="stat-item")
                    yield Static("Queued: 0", id="stat-queued", classes="stat-item")
                    yield Static("Completed: 0", id="stat-completed", classes="stat-item")
                    yield Static("Failed: 0", id="stat-failed", classes="stat-item")
                
                # Current Operation Detail
                yield Static("Current Operation Details", classes="subsection-header")
                with Container(classes="current-op-details"):
                    yield Static("No operation selected", id="current-op-info")
                    yield ProgressBar(total=100, id="current-op-progress")
                    yield Static("Speed: N/A | ETA: N/A", id="current-op-stats")

    def on_mount(self) -> None:
        """Initialize the widget when mounted."""
        self.setup_tables()
        self.load_operations_data()
        
        # Set up periodic refresh
        self.set_interval(2.0, self.refresh_data)
    
    def setup_tables(self) -> None:
        """Set up data table columns."""
        ops_table = self.query_one("#progress-operations", DataTable)
        ops_table.add_columns("ID", "Type", "Status", "Progress", "Speed", "ETA")
    
    def load_operations_data(self) -> None:
        """Load current operations data."""
        try:
            operations = self.queue_service.list_operations()
            self.update_operations_display(operations)
            
            # Update queue statistics
            stats = self.queue_service.get_queue_stats()
            self.update_queue_stats(stats)
            
        except Exception as e:
            # Handle errors gracefully
            self.query_one("#current-op-info").update(f"Error loading operations: {str(e)}")
    
    def update_operations_display(self, operations: List[Any]) -> None:
        """Update the operations table display."""
        ops_table = self.query_one("#progress-operations", DataTable)
        ops_table.clear()
        
        for op in operations[:10]:  # Show latest 10 operations
            operation_type = getattr(op.operation_dto, 'operation_type', 'unknown')
            
            # Extract progress information
            progress_text = "0%"
            speed_text = "N/A"
            eta_text = "N/A"
            
            if op.result:
                if hasattr(op.result, 'bytes_transferred') and op.result.bytes_transferred > 0:
                    # Format progress for file transfers
                    if hasattr(op.result, 'total_bytes') and op.result.total_bytes > 0:
                        pct = (op.result.bytes_transferred / op.result.total_bytes) * 100
                        progress_text = f"{pct:.1f}%"
                    else:
                        progress_text = self._format_bytes(op.result.bytes_transferred)
                    
                    if hasattr(op.result, 'throughput_mbps'):
                        speed_text = f"{op.result.throughput_mbps:.1f} MB/s"
                    
                    # Calculate ETA if we have throughput and remaining bytes
                    if hasattr(op.result, 'throughput_mbps') and op.result.throughput_mbps > 0:
                        if hasattr(op.result, 'total_bytes') and op.result.total_bytes > 0:
                            remaining_bytes = op.result.total_bytes - op.result.bytes_transferred
                            remaining_mb = remaining_bytes / (1024 * 1024)
                            eta_seconds = remaining_mb / op.result.throughput_mbps
                            eta_text = self._format_time(eta_seconds)
            
            # Status formatting
            status_text = op.status.value.title()
            if op.status.value == "completed":
                status_text = f"✓ {status_text}"
            elif op.status.value == "failed":
                status_text = f"✗ {status_text}"
            elif op.status.value == "running":
                status_text = f"⚡ {status_text}"
            elif op.status.value == "queued":
                status_text = f"⏳ {status_text}"
            
            ops_table.add_row(
                op.id[:8],  # Short ID
                operation_type.replace('_', ' ').title(),
                status_text,
                progress_text,
                speed_text,
                eta_text
            )
    
    def update_queue_stats(self, stats: Dict[str, Any]) -> None:
        """Update queue statistics display."""
        try:
            self.query_one("#stat-running").update(f"Running: {stats.get('running', 0)}")
            self.query_one("#stat-queued").update(f"Queued: {stats.get('queued', 0)}")
            self.query_one("#stat-completed").update(f"Completed: {stats.get('completed', 0)}")
            self.query_one("#stat-failed").update(f"Failed: {stats.get('failed', 0)}")
        except Exception:
            pass  # Ignore update errors
    
    @on(DataTable.RowSelected, "#progress-operations")
    def on_operation_selected(self, event: DataTable.RowSelected) -> None:
        """Handle operation selection for detailed view."""
        if event.row_key is not None:
            ops_table = self.query_one("#progress-operations", DataTable)
            row_data = ops_table.get_row(event.row_key)
            if row_data:
                operation_id = row_data[0]  # Full ID from first column
                self.show_operation_details(operation_id)
    
    def show_operation_details(self, operation_id: str) -> None:
        """Show detailed information for a specific operation."""
        try:
            # Find the operation in our data
            operation = None
            for op in self.operations_data:
                if op.id.startswith(operation_id):
                    operation = op
                    break
            
            if operation:
                # Update operation info
                op_type = getattr(operation.operation_dto, 'operation_type', 'Unknown')
                source = getattr(operation.operation_dto, 'source_location', 'N/A')
                dest = getattr(operation.operation_dto, 'dest_location', 'N/A')
                
                info_text = f"Operation: {op_type}\nStatus: {operation.status.value}\n"
                info_text += f"Source: {source}\nDestination: {dest}"
                
                if hasattr(operation.operation_dto, 'source_path'):
                    info_text += f"\nSource Path: {operation.operation_dto.source_path}"
                if hasattr(operation.operation_dto, 'dest_path'):
                    info_text += f"\nDest Path: {operation.operation_dto.dest_path}"
                
                self.query_one("#current-op-info").update(info_text)
                
                # Update progress bar
                progress_bar = self.query_one("#current-op-progress", ProgressBar)
                if operation.result and hasattr(operation.result, 'bytes_transferred'):
                    if hasattr(operation.result, 'total_bytes') and operation.result.total_bytes > 0:
                        progress_pct = (operation.result.bytes_transferred / operation.result.total_bytes) * 100
                        progress_bar.update(progress=progress_pct)
                    else:
                        progress_bar.update(progress=0)
                
                # Update stats
                stats_text = "Speed: N/A | ETA: N/A"
                if operation.result:
                    if hasattr(operation.result, 'throughput_mbps'):
                        stats_text = f"Speed: {operation.result.throughput_mbps:.1f} MB/s"
                    if hasattr(operation.result, 'duration_seconds'):
                        stats_text += f" | Duration: {operation.result.duration_seconds:.1f}s"
                
                self.query_one("#current-op-stats").update(stats_text)
            else:
                self.query_one("#current-op-info").update(f"Operation {operation_id} not found")
                
        except Exception as e:
            self.query_one("#current-op-info").update(f"Error loading operation details: {str(e)}")
    
    def refresh_data(self) -> None:
        """Refresh operations data periodically."""
        self.load_operations_data()
    
    def _format_bytes(self, bytes_count: int) -> str:
        """Format bytes in human-readable format."""
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if bytes_count < 1024.0:
                return f"{bytes_count:.1f} {unit}"
            bytes_count /= 1024.0
        return f"{bytes_count:.1f} PB"
    
    def _format_time(self, seconds: float) -> str:
        """Format time duration in human-readable format."""
        if seconds < 60:
            return f"{seconds:.0f}s"
        elif seconds < 3600:
            minutes = seconds / 60
            return f"{minutes:.1f}m"
        else:
            hours = seconds / 3600
            return f"{hours:.1f}h"