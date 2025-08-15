"""Screen components for the Tellus TUI."""

from typing import Optional, List, Dict, Any, Callable
import asyncio
from pathlib import Path

from textual.screen import Screen, ModalScreen
from textual.containers import Container, Horizontal, Vertical, VerticalScroll, Grid
from textual.widgets import (
    Static, Button, Input, Label, DataTable, Tree, ProgressBar,
    Select, Switch, TextArea, Checkbox, RadioSet, RadioButton
)
from textual.binding import Binding
from textual import on
from textual.app import ComposeResult
from textual.reactive import reactive

from ..core.feature_flags import feature_flags, FeatureFlag
from ..core.service_container import get_service_container
from ..core.legacy_bridge import ArchiveBridge


class ArchiveBrowserScreen(Screen):
    """Interactive archive browser with file tree navigation."""
    
    BINDINGS = [
        Binding("escape", "close", "Close"),
        Binding("f", "filter", "Filter"),
        Binding("r", "refresh", "Refresh"),
        Binding("enter", "select_file", "Select File"),
        Binding("space", "preview_file", "Preview"),
    ]
    
    selected_archive = reactive("")
    selected_file = reactive("")
    
    def __init__(self, archive_id: str = "", **kwargs):
        """Initialize the archive browser."""
        super().__init__(**kwargs)
        self.archive_id = archive_id
        self.files_data = []
        self.current_filter = ""
        
    def compose(self) -> ComposeResult:
        """Create the archive browser layout."""
        with Container():
            with Horizontal():
                # Archive selector
                with Vertical(classes="sidebar"):
                    yield Static("Archive Selection", classes="section-header")
                    yield Select([], id="archive-selector")
                    
                    yield Static("Filters", classes="section-header")
                    yield Input(placeholder="Filter files...", id="file-filter")
                    
                    with Container(classes="filter-options"):
                        yield Label("Content Type:")
                        yield Select([
                            ("All", "all"),
                            ("Input", "input"),
                            ("Output", "output"),
                            ("Config", "config"),
                            ("Log", "log"),
                        ], id="content-type-filter")
                        
                        yield Label("File Extensions:")
                        yield Checkbox("NetCDF (.nc)", id="filter-nc")
                        yield Checkbox("Text (.txt, .log)", id="filter-txt")
                        yield Checkbox("Binary", id="filter-bin")
                
                # File tree
                with Vertical(classes="main-content"):
                    yield Static("Archive Contents", classes="section-header")
                    yield Tree("Archive Files", id="file-tree", classes="archive-browser-tree")
                    
                    with Horizontal(classes="button-row"):
                        yield Button("Extract Selected", id="extract-selected", variant="primary")
                        yield Button("Download", id="download-file")
                        yield Button("View Metadata", id="view-metadata")
                        yield Button("Copy Path", id="copy-path")
                
                # File preview
                with Vertical(classes="sidebar"):
                    yield Static("File Preview", classes="section-header")
                    yield Static("Select a file to preview", id="file-preview", classes="file-preview")
                    
                    yield Static("File Details", classes="section-header")
                    yield Static("", id="file-details", classes="file-preview")

    def on_mount(self) -> None:
        """Initialize the browser when mounted."""
        self.load_archives()
        if self.archive_id:
            self.selected_archive = self.archive_id
            self.load_archive_files()

    def load_archives(self) -> None:
        """Load available archives into the selector."""
        # This would load archives from the service
        archive_selector = self.query_one("#archive-selector")
        # Placeholder data
        archive_selector.set_options([
            ("Archive 1", "arch1"),
            ("Archive 2", "arch2"),
        ])

    def load_archive_files(self) -> None:
        """Load files for the selected archive."""
        if not self.selected_archive:
            return
            
        tree = self.query_one("#file-tree")
        tree.clear()
        
        # This would load actual file tree from the archive service
        # For now, we'll create a sample structure
        root = tree.root
        
        # Sample file structure
        input_node = root.add("input/")
        input_node.add_leaf("forcing.nc")
        input_node.add_leaf("config.yaml")
        
        output_node = root.add("output/")
        output_node.add_leaf("results.nc")
        output_node.add_leaf("diagnostics.nc")
        
        logs_node = root.add("logs/")
        logs_node.add_leaf("model.log")
        logs_node.add_leaf("error.log")

    @on(Select.Changed, "#archive-selector")
    def on_archive_selected(self, event: Select.Changed) -> None:
        """Handle archive selection."""
        if event.value != Select.BLANK:
            self.selected_archive = str(event.value)
            self.load_archive_files()

    @on(Tree.NodeSelected, "#file-tree")
    def on_file_selected(self, event: Tree.NodeSelected) -> None:
        """Handle file selection in the tree."""
        if event.node.data:
            self.selected_file = str(event.node.data)
            self.update_file_preview()
            self.update_file_details()

    def update_file_preview(self) -> None:
        """Update the file preview pane."""
        preview = self.query_one("#file-preview")
        if self.selected_file:
            # This would load actual file preview
            preview.update(f"Preview of {self.selected_file}\n\nContent would be shown here...")
        else:
            preview.update("Select a file to preview")

    def update_file_details(self) -> None:
        """Update the file details pane."""
        details = self.query_one("#file-details")
        if self.selected_file:
            # This would load actual file metadata
            details.update(f"""File: {self.selected_file}
Size: 1.2 MB
Type: NetCDF
Created: 2024-01-15
Compressed: Yes
Checksum: abc123...""")
        else:
            details.update("")

    @on(Input.Changed, "#file-filter")
    def on_filter_changed(self, event: Input.Changed) -> None:
        """Handle filter text changes."""
        self.current_filter = event.value
        self.apply_filters()

    def apply_filters(self) -> None:
        """Apply current filters to the file tree."""
        # This would filter the tree based on current filter settings
        pass

    def action_close(self) -> None:
        """Close the browser screen."""
        self.dismiss()

    def action_filter(self) -> None:
        """Focus the filter input."""
        self.query_one("#file-filter").focus()

    def action_refresh(self) -> None:
        """Refresh the archive contents."""
        self.load_archive_files()

    def action_select_file(self) -> None:
        """Select the current file."""
        if self.selected_file:
            # This would trigger file selection action
            pass

    def action_preview_file(self) -> None:
        """Toggle file preview."""
        self.update_file_preview()


class LocationManagerScreen(ModalScreen):
    """Screen for managing storage locations and path templates."""
    
    BINDINGS = [
        Binding("escape", "close", "Close"),
        Binding("ctrl+s", "save_location", "Save"),
    ]
    
    def __init__(self, location_name: str = "", **kwargs):
        """Initialize the location manager."""
        super().__init__(**kwargs)
        self.location_name = location_name
        self.is_editing = bool(location_name)

    def compose(self) -> ComposeResult:
        """Create the location manager layout."""
        with Container(classes="location-form"):
            yield Static("Location Configuration", classes="section-header")
            
            with Vertical():
                with Horizontal(classes="form-row"):
                    yield Label("Name:")
                    yield Input(placeholder="Location name", id="location-name")
                
                with Horizontal(classes="form-row"):
                    yield Label("Type:")
                    yield Select([
                        ("Local Filesystem", "local"),
                        ("SSH/SFTP", "ssh"),
                        ("S3 Compatible", "s3"),
                        ("Google Cloud", "gcs"),
                        ("Azure Blob", "azure"),
                    ], id="location-type")
                
                with Horizontal(classes="form-row"):
                    yield Label("Protocol:")
                    yield Input(placeholder="file://, ssh://, s3://, etc.", id="location-protocol")
                
                with Horizontal(classes="form-row"):
                    yield Label("Host:")
                    yield Input(placeholder="hostname or IP", id="location-host")
                
                with Horizontal(classes="form-row"):
                    yield Label("Port:")
                    yield Input(placeholder="port number", id="location-port")
                
                with Horizontal(classes="form-row"):
                    yield Label("Username:")
                    yield Input(placeholder="username", id="location-username")
                
                with Horizontal(classes="form-row"):
                    yield Label("Base Path:")
                    yield Input(placeholder="/path/to/data", id="location-path")
                
                with Horizontal(classes="form-row"):
                    yield Label("Path Template:")
                    yield Input(placeholder="{model}/{experiment}/{resolution}", id="path-template")
                
                yield Static("Location Kinds:", classes="section-header")
                with Container():
                    yield Checkbox("Tape Storage", id="kind-tape")
                    yield Checkbox("Compute Storage", id="kind-compute")
                    yield Checkbox("Disk Storage", id="kind-disk")
                    yield Checkbox("File Server", id="kind-fileserver")
                
                yield Static("Advanced Settings:", classes="section-header")
                with Container():
                    yield Checkbox("Enable Compression", id="enable-compression")
                    yield Checkbox("Verify Checksums", id="verify-checksums")
                    yield Checkbox("Use Connection Pool", id="use-pool")
                
                with Horizontal(classes="action-buttons"):
                    yield Button("Save", id="save-location", variant="primary")
                    yield Button("Test Connection", id="test-connection")
                    yield Button("Cancel", id="cancel-location")

    def on_mount(self) -> None:
        """Initialize the location manager when mounted."""
        if self.is_editing:
            self.load_location_data()

    def load_location_data(self) -> None:
        """Load existing location data for editing."""
        # This would load location data from the service
        pass

    @on(Button.Pressed, "#save-location")
    def on_save_location(self) -> None:
        """Handle save location button press."""
        self.action_save_location()

    @on(Button.Pressed, "#test-connection")
    def on_test_connection(self) -> None:
        """Handle test connection button press."""
        # This would test the connection with current settings
        pass

    @on(Button.Pressed, "#cancel-location")
    def on_cancel_location(self) -> None:
        """Handle cancel button press."""
        self.action_close()

    def action_close(self) -> None:
        """Close the location manager."""
        self.dismiss()

    def action_save_location(self) -> None:
        """Save the location configuration."""
        # This would save the location using the service
        self.dismiss()


class OperationDashboardScreen(Screen):
    """Real-time operation progress monitoring dashboard."""
    
    BINDINGS = [
        Binding("escape", "close", "Close"),
        Binding("r", "refresh", "Refresh"),
        Binding("c", "cancel_operation", "Cancel"),
    ]
    
    operation_id = reactive("")
    
    def __init__(self, operation_id: str = "", **kwargs):
        """Initialize the operation dashboard."""
        super().__init__(**kwargs)
        self.operation_id = operation_id

    def compose(self) -> ComposeResult:
        """Create the operation dashboard layout."""
        with Container():
            yield Static("Operation Dashboard", classes="section-header")
            
            with Grid():
                # Operation overview
                with Container(classes="operation-progress"):
                    yield Label("Overall Progress")
                    yield ProgressBar(total=100, id="overall-progress")
                    yield Static("0% Complete", id="progress-text")
                
                # Current step
                with Container(classes="operation-progress"):
                    yield Label("Current Step")
                    yield ProgressBar(total=100, id="step-progress")
                    yield Static("Initializing...", id="step-text")
                
                # Transfer rate
                with Container(classes="operation-progress"):
                    yield Label("Transfer Rate")
                    yield Static("0 MB/s", id="rate-text")
                
                # Time remaining
                with Container(classes="operation-progress"):
                    yield Label("Time Remaining")
                    yield Static("Calculating...", id="time-remaining")
            
            # Detailed operation information
            with Container(classes="operation-details"):
                yield Static("Operation Details", classes="section-header")
                yield Static("", id="operation-info")
            
            # File progress table
            with Container():
                yield Static("File Progress", classes="section-header")
                yield DataTable(id="file-progress-table")
            
            # Control buttons
            with Horizontal(classes="action-buttons"):
                yield Button("Pause", id="pause-operation")
                yield Button("Resume", id="resume-operation", disabled=True)
                yield Button("Cancel", id="cancel-operation", variant="error")
                yield Button("View Logs", id="view-logs")

    def on_mount(self) -> None:
        """Initialize the dashboard when mounted."""
        self.setup_file_progress_table()
        self.load_operation_data()
        self.set_interval(2.0, self.refresh_operation_data)

    def setup_file_progress_table(self) -> None:
        """Set up the file progress table."""
        table = self.query_one("#file-progress-table")
        table.add_columns("File", "Status", "Progress", "Size", "Rate")

    def load_operation_data(self) -> None:
        """Load operation data from the service."""
        # This would load actual operation data
        pass

    async def refresh_operation_data(self) -> None:
        """Refresh operation data periodically."""
        # This would refresh operation progress from the service
        pass

    def action_close(self) -> None:
        """Close the dashboard."""
        self.dismiss()

    def action_refresh(self) -> None:
        """Refresh operation data."""
        self.load_operation_data()

    def action_cancel_operation(self) -> None:
        """Cancel the current operation."""
        # This would cancel the operation via the service
        pass


class OperationQueueScreen(ModalScreen):
    """Screen for managing multiple concurrent archive operations."""
    
    BINDINGS = [
        Binding("escape", "close", "Close"),
        Binding("ctrl+a", "add_operation", "Add Operation"),
    ]

    def compose(self) -> ComposeResult:
        """Create the operation queue layout."""
        with Container():
            yield Static("Operation Queue", classes="section-header")
            
            with Horizontal():
                # Queue list
                with Vertical(classes="sidebar"):
                    yield Static("Queued Operations", classes="section-header")
                    yield DataTable(id="operation-queue")
                    
                    with Horizontal(classes="button-row"):
                        yield Button("Add", id="add-operation", variant="primary")
                        yield Button("Remove", id="remove-operation")
                        yield Button("Clear All", id="clear-queue", variant="error")
                
                # Queue configuration
                with Vertical(classes="main-content"):
                    yield Static("Queue Settings", classes="section-header")
                    
                    with Container():
                        with Horizontal(classes="form-row"):
                            yield Label("Max Concurrent:")
                            yield Select([
                                ("1", "1"),
                                ("2", "2"),
                                ("3", "3"),
                                ("5", "5"),
                                ("10", "10"),
                            ], id="max-concurrent")
                        
                        with Horizontal(classes="form-row"):
                            yield Label("Priority Mode:")
                            yield Select([
                                ("First In, First Out", "fifo"),
                                ("Last In, First Out", "lifo"),
                                ("Size (Smallest First)", "size-asc"),
                                ("Size (Largest First)", "size-desc"),
                            ], id="priority-mode")
                        
                        yield Checkbox("Continue on Error", id="continue-on-error")
                        yield Checkbox("Auto-retry Failed", id="auto-retry")
                    
                    with Horizontal(classes="action-buttons"):
                        yield Button("Start Queue", id="start-queue", variant="primary")
                        yield Button("Pause Queue", id="pause-queue")
                        yield Button("Stop All", id="stop-queue", variant="error")

    def on_mount(self) -> None:
        """Initialize the queue screen when mounted."""
        self.setup_queue_table()

    def setup_queue_table(self) -> None:
        """Set up the operation queue table."""
        table = self.query_one("#operation-queue")
        table.add_columns("Archive", "Operation", "Source", "Destination", "Status", "Priority")

    def action_close(self) -> None:
        """Close the queue screen."""
        self.dismiss()

    def action_add_operation(self) -> None:
        """Add a new operation to the queue."""
        # This would show a dialog to add new operations
        pass


class ArchiveDetailsScreen(ModalScreen):
    """Detailed view of a specific archive."""
    
    def __init__(self, archive_id: str, **kwargs):
        """Initialize the archive details screen."""
        super().__init__(**kwargs)
        self.archive_id = archive_id

    def compose(self) -> ComposeResult:
        """Create the archive details layout."""
        with Container():
            yield Static(f"Archive Details: {self.archive_id}", classes="section-header")
            
            # Archive metadata
            with Container():
                yield Static("Metadata", classes="section-header")
                yield Static("", id="archive-metadata")
            
            # File statistics
            with Container():
                yield Static("File Statistics", classes="section-header")
                yield DataTable(id="file-stats")
            
            # Location information
            with Container():
                yield Static("Storage Locations", classes="section-header")
                yield DataTable(id="location-info")
            
            with Horizontal(classes="action-buttons"):
                yield Button("Browse Files", id="browse-files", variant="primary")
                yield Button("Export Manifest", id="export-manifest")
                yield Button("Close", id="close-details")

    def action_close(self) -> None:
        """Close the details screen."""
        self.dismiss()


class CreateArchiveScreen(ModalScreen):
    """Screen for creating new archives."""
    
    BINDINGS = [
        Binding("escape", "close", "Close"),
        Binding("ctrl+s", "create_archive", "Create"),
    ]

    def compose(self) -> ComposeResult:
        """Create the archive creation layout."""
        with Container(classes="location-form"):
            yield Static("Create New Archive", classes="section-header")
            
            with Vertical():
                with Horizontal(classes="form-row"):
                    yield Label("Archive ID:")
                    yield Input(placeholder="unique-archive-id", id="archive-id")
                
                with Horizontal(classes="form-row"):
                    yield Label("Archive Name:")
                    yield Input(placeholder="Human-readable name", id="archive-name")
                
                with Horizontal(classes="form-row"):
                    yield Label("Source Path:")
                    yield Input(placeholder="/path/to/archive.tar.gz", id="archive-path")
                
                with Horizontal(classes="form-row"):
                    yield Label("Simulation:")
                    yield Select([], id="simulation-select")
                
                with Horizontal(classes="form-row"):
                    yield Label("Location:")
                    yield Select([], id="location-select")
                
                with Horizontal(classes="form-row"):
                    yield Label("Tags:")
                    yield Input(placeholder="tag1, tag2, tag3", id="archive-tags")
                
                with Horizontal(classes="form-row"):
                    yield Label("Description:")
                    yield TextArea(placeholder="Archive description...", id="archive-description")
                
                with Horizontal(classes="action-buttons"):
                    yield Button("Create", id="create-archive", variant="primary")
                    yield Button("Cancel", id="cancel-create")

    def action_close(self) -> None:
        """Close the create archive screen."""
        self.dismiss()

    def action_create_archive(self) -> None:
        """Create the archive with current settings."""
        # This would create the archive using the service
        self.dismiss()