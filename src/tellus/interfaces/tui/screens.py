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

# Feature flags and legacy bridge removed - using new architecture directly
from ...application.container import get_service_container


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


class SimulationManagerScreen(ModalScreen):
    """Screen for managing Earth System Model simulations."""
    
    BINDINGS = [
        Binding("escape", "close", "Close"),
        Binding("ctrl+s", "save_simulation", "Save"),
        Binding("ctrl+n", "new_simulation", "New"),
        Binding("ctrl+d", "delete_simulation", "Delete"),
        Binding("enter", "edit_simulation", "Edit"),
    ]
    
    def __init__(self, simulation_id: str = "", **kwargs):
        """Initialize the simulation manager."""
        super().__init__(**kwargs)
        self.simulation_id = simulation_id
        self.is_editing = bool(simulation_id)
        self.simulations_data = []
        
        # Initialize services
        service_container = get_service_container()
        self.simulation_service = service_container.service_factory.simulation_service
        self.location_service = service_container.service_factory.location_service
    
    def compose(self) -> ComposeResult:
        """Create the simulation manager layout."""
        with Container(classes="simulation-manager"):
            # Header
            yield Static("Simulation Management", classes="section-header")
            
            with Horizontal():
                # Left Panel: Simulation List
                with Vertical(classes="sidebar"):
                    yield Static("Simulations", classes="subsection-header") 
                    yield DataTable(id="simulation-list", cursor_type="row")
                    
                    with Horizontal(classes="button-row"):
                        yield Button("New", id="new-simulation", variant="primary")
                        yield Button("Delete", id="delete-simulation", variant="error")
                        yield Button("Refresh", id="refresh-simulations")
                
                # Right Panel: Simulation Details/Editor
                with Vertical(classes="main-content"):
                    yield Static("Simulation Details", classes="subsection-header")
                    
                    with VerticalScroll(classes="form-container"):
                        # Basic Info Section
                        yield Static("Basic Information", classes="form-section-header")
                        
                        with Container(classes="form-group"):
                            with Horizontal(classes="form-row"):
                                yield Label("Simulation ID:", classes="form-label")
                                yield Input(placeholder="simulation-id", id="sim-id", disabled=True)
                            
                            with Horizontal(classes="form-row"):
                                yield Label("Model ID:", classes="form-label")
                                yield Input(placeholder="model-id", id="model-id")
                            
                            with Horizontal(classes="form-row"):
                                yield Label("Path:", classes="form-label")
                                yield Input(placeholder="/path/to/simulation", id="sim-path")
                        
                        # Location Associations Section
                        yield Static("Location Associations", classes="form-section-header")
                        
                        with Container(classes="form-group"):
                            yield DataTable(id="location-associations", cursor_type="row")
                            
                            with Horizontal(classes="button-row"):
                                yield Button("Add Location", id="add-location", variant="primary")
                                yield Button("Remove Location", id="remove-location", variant="error")
                                yield Button("Set Context", id="set-context")
                        
                        # Attributes Section
                        yield Static("Simulation Attributes", classes="form-section-header")
                        
                        with Container(classes="form-group"):
                            yield TextArea(placeholder="# YAML attributes\nmodel: example\nexperiment: test", 
                                          id="sim-attributes")
                        
                        # Actions
                        with Horizontal(classes="action-buttons"):
                            yield Button("Save", id="save-simulation", variant="success")
                            yield Button("Cancel", id="cancel-edit")
                            yield Button("Export", id="export-simulation")

    def on_mount(self) -> None:
        """Initialize the simulation manager when mounted."""
        self.load_simulations()
        self.setup_tables()
        
        if self.simulation_id:
            self.load_simulation_details(self.simulation_id)
    
    def setup_tables(self) -> None:
        """Set up data table columns."""
        # Simulation list table
        sim_table = self.query_one("#simulation-list", DataTable)
        sim_table.add_columns("ID", "Model", "Path", "Locations")
        
        # Location associations table
        loc_table = self.query_one("#location-associations", DataTable)
        loc_table.add_columns("Location", "Context", "Optional")
    
    def load_simulations(self) -> None:
        """Load simulations data from the service."""
        try:
            result = self.simulation_service.list_simulations()
            sim_table = self.query_one("#simulation-list", DataTable)
            sim_table.clear()
            
            for sim in result.simulations:
                locations_str = ", ".join(sim.associated_locations) if sim.associated_locations else "None"
                sim_table.add_row(
                    sim.simulation_id,
                    sim.model_id,
                    sim.path or "Not set",
                    locations_str
                )
                
            self.simulations_data = result.simulations
            
        except Exception as e:
            self.app.sub_title = f"Error loading simulations: {str(e)}"
    
    def load_simulation_details(self, simulation_id: str) -> None:
        """Load details for a specific simulation."""
        try:
            # Find simulation in loaded data
            simulation = None
            for sim in self.simulations_data:
                if sim.simulation_id == simulation_id:
                    simulation = sim
                    break
            
            if not simulation:
                self.app.sub_title = f"Simulation '{simulation_id}' not found"
                return
            
            # Populate form fields
            self.query_one("#sim-id", Input).value = simulation.simulation_id
            self.query_one("#model-id", Input).value = simulation.model_id
            self.query_one("#sim-path", Input).value = simulation.path or ""
            
            # Populate attributes
            if simulation.attrs:
                import yaml
                attrs_text = yaml.dump(simulation.attrs, default_flow_style=False)
                self.query_one("#sim-attributes", TextArea).text = attrs_text
            
            # Populate location associations
            self.load_location_associations(simulation)
            
        except Exception as e:
            self.app.sub_title = f"Error loading simulation details: {str(e)}"
    
    def load_location_associations(self, simulation) -> None:
        """Load location associations for a simulation."""
        try:
            loc_table = self.query_one("#location-associations", DataTable)
            loc_table.clear()
            
            # Get all available locations for context lookup
            locations_result = self.location_service.list_locations()
            location_lookup = {loc.name: loc for loc in locations_result.locations}
            
            for loc_name in simulation.associated_locations:
                location = location_lookup.get(loc_name)
                context = "Default"
                optional = "No"
                
                if location:
                    optional = "Yes" if location.optional else "No"
                    # Get context information if available
                    contexts = getattr(simulation, 'location_contexts', {})
                    if loc_name in contexts:
                        context = str(contexts[loc_name])
                
                loc_table.add_row(loc_name, context, optional)
                
        except Exception as e:
            self.app.sub_title = f"Error loading location associations: {str(e)}"
    
    @on(DataTable.RowSelected, "#simulation-list")
    def on_simulation_selected(self, event: DataTable.RowSelected) -> None:
        """Handle simulation selection from the list."""
        if event.row_key is not None:
            sim_table = self.query_one("#simulation-list", DataTable)
            row_data = sim_table.get_row(event.row_key)
            if row_data:
                simulation_id = row_data[0]
                self.simulation_id = simulation_id
                self.load_simulation_details(simulation_id)
    
    @on(Button.Pressed, "#new-simulation")
    def on_new_simulation(self) -> None:
        """Create a new simulation."""
        # Clear form for new simulation
        self.simulation_id = ""
        self.query_one("#sim-id", Input).value = ""
        self.query_one("#model-id", Input).value = ""
        self.query_one("#sim-path", Input).value = ""
        self.query_one("#sim-attributes", TextArea).text = ""
        
        # Clear location associations
        loc_table = self.query_one("#location-associations", DataTable)
        loc_table.clear()
        
        # Enable simulation ID field for new entries
        self.query_one("#sim-id", Input).disabled = False
        self.is_editing = False
    
    @on(Button.Pressed, "#save-simulation")
    def on_save_simulation(self) -> None:
        """Save the current simulation."""
        try:
            from ...application.dtos import CreateSimulationDto, UpdateSimulationDto
            
            sim_id = self.query_one("#sim-id", Input).value
            model_id = self.query_one("#model-id", Input).value
            path = self.query_one("#sim-path", Input).value
            attrs_text = self.query_one("#sim-attributes", TextArea).text
            
            if not sim_id:
                self.app.sub_title = "Error: Simulation ID is required"
                return
            
            # Parse attributes
            attrs = {}
            if attrs_text.strip():
                import yaml
                try:
                    attrs = yaml.safe_load(attrs_text)
                except yaml.YAMLError as e:
                    self.app.sub_title = f"Error: Invalid YAML in attributes: {str(e)}"
                    return
            
            if self.is_editing:
                # Update existing simulation
                dto = UpdateSimulationDto(
                    simulation_id=sim_id,
                    model_id=model_id,
                    path=path,
                    attrs=attrs
                )
                result = self.simulation_service.update_simulation(dto)
            else:
                # Create new simulation
                dto = CreateSimulationDto(
                    simulation_id=sim_id,
                    model_id=model_id,
                    path=path,
                    attrs=attrs
                )
                result = self.simulation_service.create_simulation(dto)
            
            if result.success:
                self.app.sub_title = f"Simulation '{sim_id}' saved successfully"
                self.load_simulations()
                self.is_editing = True
                self.query_one("#sim-id", Input).disabled = True
            else:
                self.app.sub_title = f"Error saving simulation: {result.error_message}"
                
        except Exception as e:
            self.app.sub_title = f"Error saving simulation: {str(e)}"
    
    @on(Button.Pressed, "#delete-simulation")
    def on_delete_simulation(self) -> None:
        """Delete the selected simulation."""
        if not self.simulation_id:
            self.app.sub_title = "No simulation selected for deletion"
            return
        
        try:
            from ...application.dtos import DeleteSimulationDto
            dto = DeleteSimulationDto(simulation_id=self.simulation_id)
            result = self.simulation_service.delete_simulation(dto)
            
            if result.success:
                self.app.sub_title = f"Simulation '{self.simulation_id}' deleted successfully"
                self.load_simulations()
                self.on_new_simulation()  # Clear form
            else:
                self.app.sub_title = f"Error deleting simulation: {result.error_message}"
                
        except Exception as e:
            self.app.sub_title = f"Error deleting simulation: {str(e)}"
    
    @on(Button.Pressed, "#add-location")
    def on_add_location(self) -> None:
        """Add a location association to the simulation."""
        if not self.simulation_id:
            self.app.sub_title = "Save simulation first before adding locations"
            return
        
        # This would open a location selection dialog
        self.app.sub_title = "Location association dialog not yet implemented"
    
    @on(Button.Pressed, "#refresh-simulations")
    def on_refresh_simulations(self) -> None:
        """Refresh the simulations list."""
        self.load_simulations()
    
    def action_close(self) -> None:
        """Close the simulation manager screen."""
        self.dismiss()
    
    def action_save_simulation(self) -> None:
        """Save simulation via keyboard shortcut."""
        self.on_save_simulation()
    
    def action_new_simulation(self) -> None:
        """New simulation via keyboard shortcut."""
        self.on_new_simulation()
    
    def action_delete_simulation(self) -> None:
        """Delete simulation via keyboard shortcut."""
        self.on_delete_simulation()


class FileTransferManagerScreen(ModalScreen):
    """Screen for managing file transfers with progress tracking."""
    
    BINDINGS = [
        Binding("escape", "close", "Close"),
        Binding("ctrl+t", "new_transfer", "New Transfer"),
        Binding("ctrl+p", "pause_queue", "Pause Queue"),
        Binding("ctrl+r", "resume_queue", "Resume Queue"),
        Binding("enter", "transfer_details", "Details"),
    ]
    
    def __init__(self, **kwargs):
        """Initialize the file transfer manager."""
        super().__init__(**kwargs)
        
        # Initialize services
        service_container = get_service_container()
        self.transfer_service = service_container.service_factory.file_transfer_service
        self.queue_service = service_container.service_factory.operation_queue_service
        self.location_service = service_container.service_factory.location_service
        
        self.transfers_data = []
        self.queue_stats = {}
    
    def compose(self) -> ComposeResult:
        """Create the file transfer manager layout."""
        with Container(classes="transfer-manager"):
            # Header
            yield Static("File Transfer Management", classes="section-header")
            
            with Horizontal():
                # Left Panel: Transfer Queue
                with Vertical(classes="sidebar"):
                    yield Static("Transfer Queue", classes="subsection-header")
                    yield DataTable(id="transfer-queue", cursor_type="row")
                    
                    with Horizontal(classes="button-row"):
                        yield Button("New Transfer", id="new-transfer", variant="primary")
                        yield Button("Cancel", id="cancel-transfer", variant="error")
                        yield Button("Refresh", id="refresh-queue")
                    
                    # Queue Controls
                    yield Static("Queue Controls", classes="subsection-header")
                    with Container(classes="queue-controls"):
                        with Horizontal(classes="control-row"):
                            yield Button("Pause", id="pause-queue", variant="warning")
                            yield Button("Resume", id="resume-queue", variant="success")
                            yield Button("Clear", id="clear-completed")
                        
                        # Queue Statistics
                        with Container(classes="queue-stats"):
                            yield Static("Queue Stats", classes="stats-header")
                            yield Static("Total: 0", id="stat-total")
                            yield Static("Running: 0", id="stat-running")
                            yield Static("Queued: 0", id="stat-queued")
                            yield Static("Completed: 0", id="stat-completed")
                            yield Static("Failed: 0", id="stat-failed")
                
                # Right Panel: Transfer Form/Details
                with Vertical(classes="main-content"):
                    yield Static("Transfer Configuration", classes="subsection-header")
                    
                    with VerticalScroll(classes="form-container"):
                        # Transfer Type Section
                        yield Static("Transfer Type", classes="form-section-header")
                        
                        with Container(classes="form-group"):
                            yield RadioSet(
                                "Single File",
                                "Batch Files", 
                                "Directory",
                                id="transfer-type"
                            )
                        
                        # Source Configuration
                        yield Static("Source Configuration", classes="form-section-header")
                        
                        with Container(classes="form-group"):
                            with Horizontal(classes="form-row"):
                                yield Label("Source Location:", classes="form-label")
                                yield Select([("Local", "local")], id="source-location")
                            
                            with Horizontal(classes="form-row"):
                                yield Label("Source Path:", classes="form-label")
                                yield Input(placeholder="/path/to/source", id="source-path")
                        
                        # Destination Configuration
                        yield Static("Destination Configuration", classes="form-section-header")
                        
                        with Container(classes="form-group"):
                            with Horizontal(classes="form-row"):
                                yield Label("Destination Location:", classes="form-label")
                                yield Select([], id="dest-location")
                            
                            with Horizontal(classes="form-row"):
                                yield Label("Destination Path:", classes="form-label")
                                yield Input(placeholder="/path/to/destination", id="dest-path")
                        
                        # Transfer Options
                        yield Static("Transfer Options", classes="form-section-header")
                        
                        with Container(classes="form-group"):
                            with Horizontal(classes="form-row"):
                                yield Checkbox("Verify checksums", id="verify-checksums", value=True)
                                yield Checkbox("Overwrite existing files", id="overwrite-files")
                            
                            with Horizontal(classes="form-row"):
                                yield Label("Parallel transfers:", classes="form-label")
                                yield Select([("1", 1), ("3", 3), ("5", 5), ("10", 10)], 
                                           id="parallel-count", value=3)
                        
                        # Batch/Directory Options (conditionally shown)
                        with Container(classes="form-group", id="batch-options"):
                            yield Static("Batch Options", classes="form-section-header")
                            
                            with Horizontal(classes="form-row"):
                                yield Label("Include patterns:", classes="form-label")
                                yield Input(placeholder="*.nc, *.log", id="include-patterns")
                            
                            with Horizontal(classes="form-row"):
                                yield Label("Exclude patterns:", classes="form-label")
                                yield Input(placeholder="*.tmp, temp/*", id="exclude-patterns")
                            
                            with Horizontal(classes="form-row"):
                                yield Checkbox("Stop on first error", id="stop-on-error")
                        
                        # Actions
                        with Horizontal(classes="action-buttons"):
                            yield Button("Start Transfer", id="start-transfer", variant="success")
                            yield Button("Add to Queue", id="queue-transfer", variant="primary")
                            yield Button("Clear Form", id="clear-form")

    def on_mount(self) -> None:
        """Initialize the transfer manager when mounted."""
        self.load_locations()
        self.load_transfer_queue()
        self.setup_tables()
        self.update_queue_stats()
        
        # Hide batch options initially
        self.query_one("#batch-options").visible = False
    
    def setup_tables(self) -> None:
        """Set up data table columns."""
        queue_table = self.query_one("#transfer-queue", DataTable)
        queue_table.add_columns("ID", "Type", "Status", "Progress", "Source", "Destination")
    
    def load_locations(self) -> None:
        """Load available locations for selection."""
        try:
            result = self.location_service.list_locations()
            
            # Populate location selectors
            dest_select = self.query_one("#dest-location", Select)
            dest_select.set_options([(loc.name, loc.name) for loc in result.locations])
            
        except Exception as e:
            self.app.sub_title = f"Error loading locations: {str(e)}"
    
    def load_transfer_queue(self) -> None:
        """Load current transfer queue."""
        try:
            operations = self.queue_service.list_operations()
            queue_table = self.query_one("#transfer-queue", DataTable)
            queue_table.clear()
            
            for op in operations[:20]:  # Show recent 20 operations
                operation_type = getattr(op.operation_dto, 'operation_type', 'unknown')
                if 'transfer' not in operation_type:
                    continue  # Skip non-transfer operations
                
                # Extract transfer details
                source = getattr(op.operation_dto, 'source_location', 'Unknown')
                dest = getattr(op.operation_dto, 'dest_location', 'Unknown')
                
                if hasattr(op.operation_dto, 'source_path'):
                    source += f":{op.operation_dto.source_path}"
                if hasattr(op.operation_dto, 'dest_path'):
                    dest += f":{op.operation_dto.dest_path}"
                
                # Progress indication
                progress = "0%"
                if op.result and hasattr(op.result, 'bytes_transferred'):
                    if hasattr(op.result, 'total_bytes') and op.result.total_bytes > 0:
                        pct = (op.result.bytes_transferred / op.result.total_bytes) * 100
                        progress = f"{pct:.1f}%"
                    else:
                        progress = f"{op.result.bytes_transferred:,} bytes"
                
                queue_table.add_row(
                    op.id[:8],
                    operation_type.replace('_', ' ').title(),
                    op.status.value.title(),
                    progress,
                    source,
                    dest
                )
                
        except Exception as e:
            self.app.sub_title = f"Error loading transfer queue: {str(e)}"
    
    def update_queue_stats(self) -> None:
        """Update queue statistics display."""
        try:
            stats = self.queue_service.get_queue_stats()
            
            self.query_one("#stat-total").update(f"Total: {stats['total_operations']}")
            self.query_one("#stat-running").update(f"Running: {stats['running']}")
            self.query_one("#stat-queued").update(f"Queued: {stats['queued']}")
            self.query_one("#stat-completed").update(f"Completed: {stats['completed']}")
            self.query_one("#stat-failed").update(f"Failed: {stats['failed']}")
            
        except Exception as e:
            self.app.sub_title = f"Error updating queue stats: {str(e)}"
    
    @on(RadioSet.Changed, "#transfer-type")
    def on_transfer_type_changed(self, event: RadioSet.Changed) -> None:
        """Handle transfer type selection."""
        batch_options = self.query_one("#batch-options")
        
        if event.pressed.label in ["Batch Files", "Directory"]:
            batch_options.visible = True
        else:
            batch_options.visible = False
    
    @on(Button.Pressed, "#start-transfer")
    def on_start_transfer(self) -> None:
        """Start the configured transfer immediately."""
        try:
            transfer_dto = self._build_transfer_dto()
            if not transfer_dto:
                return
            
            # Start transfer immediately (not queued)
            self.app.sub_title = "Starting immediate transfer..."
            
            # This would start the transfer directly
            # For now, just show a message
            self.app.sub_title = "Direct transfer execution not yet implemented"
            
        except Exception as e:
            self.app.sub_title = f"Error starting transfer: {str(e)}"
    
    @on(Button.Pressed, "#queue-transfer")
    def on_queue_transfer(self) -> None:
        """Add the configured transfer to the queue."""
        try:
            transfer_dto = self._build_transfer_dto()
            if not transfer_dto:
                return
            
            # Add to queue
            operation_id = self.queue_service.add_operation(transfer_dto)
            self.app.sub_title = f"Transfer queued with ID: {operation_id[:8]}"
            
            # Refresh queue display
            self.load_transfer_queue()
            self.update_queue_stats()
            
        except Exception as e:
            self.app.sub_title = f"Error queueing transfer: {str(e)}"
    
    def _build_transfer_dto(self):
        """Build transfer DTO from form data."""
        from ...application.dtos import (
            FileTransferOperationDto, 
            BatchFileTransferOperationDto, 
            DirectoryTransferOperationDto
        )
        
        # Get form values
        transfer_type = self.query_one("#transfer-type", RadioSet).pressed.label
        source_location = self.query_one("#source-location", Select).value
        source_path = self.query_one("#source-path", Input).value
        dest_location = self.query_one("#dest-location", Select).value
        dest_path = self.query_one("#dest-path", Input).value
        verify_checksums = self.query_one("#verify-checksums", Checkbox).value
        overwrite = self.query_one("#overwrite-files", Checkbox).value
        
        # Validation
        if not source_path or not dest_path or not dest_location:
            self.app.sub_title = "Error: Source path, destination path, and location are required"
            return None
        
        if transfer_type == "Single File":
            return FileTransferOperationDto(
                source_location=source_location,
                source_path=source_path,
                dest_location=dest_location,
                dest_path=dest_path,
                overwrite=overwrite,
                verify_checksum=verify_checksums
            )
        elif transfer_type == "Directory":
            include_patterns = self.query_one("#include-patterns", Input).value
            exclude_patterns = self.query_one("#exclude-patterns", Input).value
            
            return DirectoryTransferOperationDto(
                source_location=source_location,
                source_path=source_path,
                dest_location=dest_location,
                dest_path=dest_path,
                recursive=True,
                overwrite=overwrite,
                verify_checksums=verify_checksums,
                include_patterns=include_patterns.split(',') if include_patterns else [],
                exclude_patterns=exclude_patterns.split(',') if exclude_patterns else []
            )
        else:
            self.app.sub_title = "Batch transfers not yet implemented"
            return None
    
    @on(Button.Pressed, "#pause-queue")
    def on_pause_queue(self) -> None:
        """Pause the transfer queue."""
        try:
            self.queue_service.pause_queue()
            self.app.sub_title = "Transfer queue paused"
            self.update_queue_stats()
        except Exception as e:
            self.app.sub_title = f"Error pausing queue: {str(e)}"
    
    @on(Button.Pressed, "#resume-queue")
    def on_resume_queue(self) -> None:
        """Resume the transfer queue."""
        try:
            self.queue_service.resume_queue()
            self.app.sub_title = "Transfer queue resumed"
            self.update_queue_stats()
        except Exception as e:
            self.app.sub_title = f"Error resuming queue: {str(e)}"
    
    @on(Button.Pressed, "#refresh-queue")
    def on_refresh_queue(self) -> None:
        """Refresh the transfer queue display."""
        self.load_transfer_queue()
        self.update_queue_stats()
    
    @on(Button.Pressed, "#clear-form")
    def on_clear_form(self) -> None:
        """Clear the transfer form."""
        self.query_one("#source-path", Input).value = ""
        self.query_one("#dest-path", Input).value = ""
        self.query_one("#include-patterns", Input).value = ""
        self.query_one("#exclude-patterns", Input).value = ""
        self.query_one("#verify-checksums", Checkbox).value = True
        self.query_one("#overwrite-files", Checkbox).value = False
        self.query_one("#stop-on-error", Checkbox).value = False
    
    def action_close(self) -> None:
        """Close the file transfer manager screen."""
        self.dismiss()
    
    def action_new_transfer(self) -> None:
        """New transfer via keyboard shortcut."""
        self.on_clear_form()
    
    def action_pause_queue(self) -> None:
        """Pause queue via keyboard shortcut."""
        self.on_pause_queue()
    
    def action_resume_queue(self) -> None:
        """Resume queue via keyboard shortcut."""
        self.on_resume_queue()