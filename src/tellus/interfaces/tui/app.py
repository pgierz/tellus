"""Main Textual TUI application for Tellus archive management."""

from typing import Optional, List, Dict, Any

from textual.app import App, ComposeResult
from textual.containers import Container, Horizontal, Vertical, VerticalScroll
from textual.widgets import (
    Button,
    DataTable,
    Footer,
    Header,
    Input,
    Markdown,
    Static,
    Tree,
)
from textual.binding import Binding
from textual.reactive import reactive
from textual import on

from ...application.container import get_service_container

# Screen imports will be added as needed when implementing those features
from .widgets import StatusBar


class TellusTUIApp(App):
    """Main Tellus TUI application."""
    
    CSS_PATH = "app.tcss"
    TITLE = "Tellus Archive Management"
    SUB_TITLE = "Earth System Model Data Management"
    
    BINDINGS = [
        # Vim-like navigation
        Binding("h", "nav_left", "Left panel", show=False),
        Binding("j", "nav_down", "Down", show=False),
        Binding("k", "nav_up", "Up", show=False),
        Binding("l", "nav_right", "Right panel", show=False),
        
        # Arrow key navigation
        Binding("left", "nav_left", "Left panel", show=False),
        Binding("down", "nav_down", "Down", show=False),
        Binding("up", "nav_up", "Up", show=False),
        Binding("right", "nav_right", "Right panel", show=False),
        Binding("g,g", "nav_top", "Top", show=False),
        Binding("G", "nav_bottom", "Bottom", show=False),
        Binding("/", "search", "Search", show=True),
        Binding("n", "search_next", "Next match", show=False),
        Binding("N", "search_prev", "Prev match", show=False),
        Binding("v", "visual_mode", "Visual mode", show=False),
        Binding("escape", "normal_mode", "Normal mode", show=False),
        
        # Main actions
        Binding("q", "quit", "Quit"),
        Binding("ctrl+c", "quit", "Quit"),
        Binding("?", "show_help", "Help"),
        Binding("r", "refresh", "Refresh"),
        Binding("enter", "select_item", "Select"),
        Binding("space", "preview_item", "Preview"),
        
        # Operations
        Binding("c", "copy_archive", "Copy"),
        Binding("m", "move_archive", "Move"),
        Binding("x", "extract_archive", "Extract"),
        Binding("d", "delete_item", "Delete"),
        Binding("y", "yank_item", "Yank"),
        Binding("p", "paste_item", "Paste"),
        Binding("t", "test_location", "Test Location"),
        
        # View modes
        Binding("1", "show_simulations", "Simulations"),
        Binding("2", "show_archives", "Archives"),
        Binding("3", "show_location_manager", "Location Manager (Not Implemented)"),
        Binding("4", "show_operations", "Operations"),
        
        # Bulk operations
        Binding("B", "bulk_operations", "Bulk Ops"),
        Binding("ctrl+p", "pause_queue", "Pause Queue"),
        Binding("ctrl+r", "resume_queue", "Resume Queue"),
        Binding("ctrl+k", "cancel_operation", "Cancel Op"),
        
        # Create new items
        Binding("A", "new_archive", "New Archive"),
        Binding("L", "new_location", "New Location"),
        Binding("S", "new_simulation", "New Simulation"),
        
        # Transfer management
        Binding("T", "file_transfer_manager", "File Transfers"),
        Binding("ctrl+t", "new_transfer", "New Transfer"),
    ]
    
    # Reactive variables for state management
    current_simulation = reactive("")
    selected_archive = reactive("")
    selected_location = reactive("")
    operation_count = reactive(0)
    
    # Vim-like navigation state
    current_panel = reactive("left")  # left, center, right
    visual_mode = reactive(False)
    search_term = reactive("")
    search_results = reactive([])
    search_index = reactive(0)
    
    # Yank buffer for copy/paste operations
    yank_buffer = reactive("")
    yank_type = reactive("")  # archive, simulation, location
    
    def __init__(self, **kwargs):
        """Initialize the TUI app."""
        super().__init__(**kwargs)
        
        # Initialize services using new architecture
        service_container = get_service_container()
        self.archive_service = service_container.service_factory.archive_service
        self.simulation_service = service_container.service_factory.simulation_service
        self.location_service = service_container.service_factory.location_service
        
        # Remove legacy bridge references
        self.archive_bridge = None
        
        # Data storage
        self.simulations_data = []
        self.archives_data = []
        self.locations_data = []
        self.operations_data = []
        self.simulation_files_data = {}  # simulation_id -> list of files
        

    def compose(self) -> ComposeResult:
        """Create ranger-style 3-panel layout."""
        with Container(id="main-container"):
            yield Header(show_clock=True)
            
            # Status bar showing current mode and context
            yield StatusBar(id="status-bar")
            
            # Main 3-panel ranger-style layout
            with Horizontal(id="ranger-layout"):
                # Left Panel: Navigation tree (Simulations/Archives/Locations)
                with Vertical(id="left-panel", classes="panel"):
                    yield Static("Navigation", classes="panel-header active")
                    yield Tree("Tellus Data", id="nav-tree")
                    with Horizontal(classes="panel-actions"):
                        yield Static("1:Sims 2:Archives 3:Locations 4:Ops", classes="panel-help")
                
                # Center Panel: File browser for selected item
                with Vertical(id="center-panel", classes="panel"):
                    yield Static("Files", classes="panel-header")
                    yield DataTable(id="file-browser", cursor_type="row")
                    with Horizontal(classes="panel-actions"):
                        yield Static("Enter:Select Space:Preview c:Copy m:Move x:Extract t:Test", classes="panel-help")
                
                # Right Panel: Preview/Details
                with Vertical(id="right-panel", classes="panel"):
                    yield Static("Details", classes="panel-header")
                    with VerticalScroll(id="details-view"):
                        yield Markdown("Select an item to view details", id="item-details", classes="-default")
                    with Horizontal(classes="panel-actions"):
                        yield Static("/:Search n:Next N:Prev", classes="panel-help")
            
            # Search/Command bar (initially hidden)
            yield Input(placeholder="Search...", id="search-input", classes="hidden")
            
            yield Footer()

    def on_mount(self) -> None:
        """Initialize the app when mounted."""
        self.title = "Tellus Archive Management"
        self.sub_title = "Earth System Model Data Management"
        
        # Initialize current view state
        self._current_view = "simulations"
        
        # Initialize data structures
        self.archives_data = []
        self.locations_data = []
        self.simulations_data = []
        self.operations_data = []
        self.simulation_files_data = {}
        self.operation_count = 0
        self.selected_archive = ""
        self.selected_location = ""
        self.current_simulation = ""
        
        # Services are already initialized in __init__
        
        # Load initial data (use call_later for async methods)
        self.call_later(self._load_initial_data)
        
        # Initialize navigation after data is loaded
        self.call_later(self._init_navigation)
        
        # Set up periodic refresh for operations (reduced frequency)
        self.set_interval(10.0, self._refresh_operations_sync)
    
    def _load_initial_data(self) -> None:
        """Load initial data synchronously."""
        try:
            # Load data synchronously to avoid async issues
            self._load_archives_sync()
            self._load_locations_sync()
            self._load_simulations_sync()
        except Exception as e:
            self.sub_title = f"Error loading data: {str(e)}"
    
    def _refresh_operations_sync(self) -> None:
        """Refresh operations data synchronously."""
        # Only refresh if we're currently showing operations view
        if hasattr(self, '_current_view') and self._current_view == 'operations':
            try:
                # For now, operations are not implemented in new architecture
                self.operations_data = []
                self.queue_stats = {}
                self.operation_count = 0
                
                # Update navigation tree if available
                try:
                    nav_tree = self.query_one("#nav-tree", Tree)
                    self._populate_operations_tree(nav_tree)
                except Exception:
                    pass  # Tree might not be ready yet
            except Exception as e:
                # Log error but don't disrupt the UI
                pass
    
    def _load_archives_sync(self) -> None:
        """Load archives data synchronously."""
        try:
            # Use new architecture service
            from ...application.dtos import ListArchivesDto
            dto = ListArchivesDto()
            result = self.archive_service.list_archives(dto)
            
            self.archives_data = []
            if result.success:
                for archive in result.archives:
                    self.archives_data.append({
                        'id': archive.archive_id,
                        'name': archive.name or archive.archive_id,
                        'simulation': archive.simulation_id or 'Unknown',
                        'size': archive.size or 0,
                        'cached': False,  # Would need to check cache status
                        'location': archive.location_name or 'Unknown'
                    })
        except Exception:
            # Fallback to empty list on error
            self.archives_data = []
    
    def _load_locations_sync(self) -> None:
        """Load locations data synchronously."""
        try:
            # Use new architecture service
            result = self.location_service.list_locations()
            self.locations_data = []
            if result.success:
                for loc in result.locations:
                    self.locations_data.append({
                        'name': loc.name,
                        'kinds': [k if isinstance(k, str) else k.name for k in loc.kinds],
                        'protocol': loc.protocol or 'local',
                        'path_prefix': loc.path or '',
                        'connected': True  # Simplified for now
                    })
        except Exception:
            self.locations_data = []
    
    def _load_simulations_sync(self) -> None:
        """Load simulations data synchronously."""
        try:
            # Use new architecture service
            result = self.simulation_service.list_simulations()
            # Convert entities to dict format for TUI compatibility
            self.simulations_data = []
            for sim in result.simulations:
                sim_dict = {
                    'simulation_id': sim.simulation_id,
                    'model_id': sim.model_id,
                    'path': sim.path,
                    'attrs': sim.attrs,
                    'locations': list(sim.associated_locations)
                }
                self.simulations_data.append(sim_dict)
        except Exception:
            self.simulations_data = []
    
    def _init_navigation(self) -> None:
        """Initialize navigation tree with default view."""
        self._switch_nav_view("simulations")

    async def load_archives_data(self) -> None:
        """Load archives data from the service."""
        try:
            # Use new architecture service
            from ...application.dtos import ListArchivesDto
            dto = ListArchivesDto()
            result = self.archive_service.list_archives(dto)
            
            self.archives_data = []
            if result.success:
                for archive in result.archives:
                    self.archives_data.append({
                        'id': archive.archive_id,
                        'name': archive.name or archive.archive_id,
                        'simulation': archive.simulation_id or 'Unknown',
                        'size': archive.size or 0,
                        'cached': False,  # Would need to check cache status
                        'location': archive.location_name or 'Unknown'
                    })
                self._update_status("Archives loaded successfully")
            else:
                self._update_status("Failed to load archives", error=True)
            
            # Update navigation tree if currently showing archives
            if hasattr(self, '_current_view') and self._current_view == 'archives':
                self._populate_archives_tree(self.query_one("#nav-tree", Tree))
        except Exception as e:
            self._update_status(f"Error loading archives: {str(e)}", error=True)

    async def _load_archives_async(self) -> List[Dict[str, Any]]:
        """Load archives data asynchronously using new service."""
        # This would be implemented to call the bridge asynchronously
        # For now, we'll simulate async behavior
        return []

    async def load_locations_data(self) -> None:
        """Load locations data from the service."""
        try:
            # Use new architecture service
            result = self.location_service.list_locations()
            self.locations_data = []
            for loc in result.locations:
                self.locations_data.append({
                    'name': loc.name,
                    'kinds': [k if isinstance(k, str) else k.name for k in loc.kinds],
                    'protocol': loc.protocol or 'local',
                    'path_prefix': loc.path or '',
                    'connected': True  # Would check actual connection status
                })
            self._update_status("Locations loaded successfully")
            
            # Update navigation tree if currently showing locations
            if hasattr(self, '_current_view') and self._current_view == 'locations':
                self._populate_locations_tree(self.query_one("#nav-tree", Tree))
        except Exception as e:
            self._update_status(f"Error loading locations: {str(e)}", error=True)

    async def load_operations_data(self) -> None:
        """Load operations data from the service."""
        try:
            # Load real bulk operation data from new architecture
            if self.archive_service:
                # For now, bulk operations are not implemented in the new service
                # This would be implemented when bulk operation queue is added
                queue_status = {'success': False}
                operations_list = {'success': False}
                
                if queue_status.get('success') and operations_list.get('success'):
                    self.operations_data = operations_list.get('operations', [])
                    
                    # Add queue statistics to operations data
                    queue_stats = queue_status.get('queue_stats', {})
                    self.queue_stats = queue_stats
                    
                    self.operation_count = len(self.operations_data)
                    self._update_status(f"Loaded {self.operation_count} operations from queue")
                else:
                    self.operations_data = []
                    self.queue_stats = {}
                    self.operation_count = 0
                    error_msg = queue_status.get('error_message', 'Unknown error')
                    self._update_status(f"Error loading operations: {error_msg}", error=True)
            else:
                # Fallback when service is not available
                self.operations_data = []
                self.queue_stats = {}
                self.operation_count = 0
                self._update_status("Archive service not available")
            
            # Update navigation tree if currently showing operations
            if hasattr(self, '_current_view') and self._current_view == 'operations':
                self._populate_operations_tree(self.query_one("#nav-tree", Tree))
        except Exception as e:
            self._update_status(f"Error loading operations: {str(e)}", error=True)

    async def refresh_operations_data(self) -> None:
        """Refresh operations data periodically."""
        await self.load_operations_data()

    def _update_status(self, message: str, error: bool = False) -> None:
        """Update the status bar with a message."""
        status_bar = self.query_one("#status-bar")
        status_bar.update_message(message, error=error)

    # Event handlers for buttons
    @on(Button.Pressed, "#create-archive")
    def show_create_archive(self) -> None:
        """Show create archive screen."""
        self._update_status("Create archive functionality not yet implemented")

    @on(Button.Pressed, "#import-archive")
    def on_import_archive(self) -> None:
        """Handle import archive button press."""
        self._update_status("Import archive functionality not yet implemented")

    @on(Button.Pressed, "#refresh-archives")
    def on_refresh_archives(self) -> None:
        """Handle refresh archives button press."""
        self.call_later(self.load_archives_data)

    @on(Button.Pressed, "#add-location")
    def show_location_manager(self) -> None:
        """Show location manager screen."""
        from .screens import LocationManagerScreen
        self.push_screen(LocationManagerScreen())

    @on(Button.Pressed, "#test-location")
    def on_test_location(self) -> None:
        """Handle test location button press."""
        if self.selected_location:
            self._update_status(f"Testing connection to {self.selected_location}...")
            # Add actual connection test logic here
        else:
            self._update_status("Please select a location to test", error=True)

    @on(Button.Pressed, "#refresh-locations")
    def on_refresh_locations(self) -> None:
        """Handle refresh locations button press."""
        self.call_later(self.load_locations_data)

    @on(Button.Pressed, "#start-bulk")
    def show_operation_queue(self) -> None:
        """Show operation queue screen."""
        self._update_status("Operation queue functionality not yet implemented")

    @on(Button.Pressed, "#refresh-operations")
    def on_refresh_operations(self) -> None:
        """Handle refresh operations button press."""
        self.call_later(self.load_operations_data)

    @on(Button.Pressed, "#clear-logs")
    def on_clear_logs(self) -> None:
        """Handle clear logs button press."""
        log_viewer = self.query_one("#log-viewer")
        log_viewer.clear()
        self._update_status("Logs cleared")

    # Vim-like navigation actions
    def action_nav_left(self) -> None:
        """Move to left panel."""
        self.current_panel = "left"
        self._update_panel_focus()
        
    def action_nav_right(self) -> None:
        """Move to right panel.""" 
        if self.current_panel == "left":
            self.current_panel = "center"
        elif self.current_panel == "center":
            self.current_panel = "right"
        self._update_panel_focus()
    
    def action_nav_up(self) -> None:
        """Move up in current panel."""
        if self.current_panel == "left":
            nav_tree = self.query_one("#nav-tree", Tree)
            nav_tree.action_cursor_up()
        elif self.current_panel == "center":
            file_browser = self.query_one("#file-browser", DataTable)
            file_browser.action_cursor_up()
    
    def action_nav_down(self) -> None:
        """Move down in current panel."""
        if self.current_panel == "left":
            nav_tree = self.query_one("#nav-tree", Tree)
            nav_tree.action_cursor_down()
        elif self.current_panel == "center":
            file_browser = self.query_one("#file-browser", DataTable)
            file_browser.action_cursor_down()
    
    def action_nav_top(self) -> None:
        """Go to top of current panel."""
        if self.current_panel == "left":
            nav_tree = self.query_one("#nav-tree", Tree)
            nav_tree.scroll_home()
        elif self.current_panel == "center":
            file_browser = self.query_one("#file-browser", DataTable)
            file_browser.scroll_home()
    
    def action_nav_bottom(self) -> None:
        """Go to bottom of current panel."""
        if self.current_panel == "left":
            nav_tree = self.query_one("#nav-tree", Tree)
            nav_tree.scroll_end()
        elif self.current_panel == "center":
            file_browser = self.query_one("#file-browser", DataTable)
            file_browser.scroll_end()
    
    def action_search(self) -> None:
        """Enter search mode."""
        search_input = self.query_one("#search-input", Input)
        search_input.remove_class("hidden")
        search_input.focus()
    
    def action_search_next(self) -> None:
        """Go to next search result."""
        if self.search_results and self.search_index < len(self.search_results) - 1:
            self.search_index += 1
            self._highlight_search_result()
    
    def action_search_prev(self) -> None:
        """Go to previous search result."""
        if self.search_results and self.search_index > 0:
            self.search_index -= 1
            self._highlight_search_result()
    
    def action_visual_mode(self) -> None:
        """Enter visual selection mode."""
        self.visual_mode = not self.visual_mode
        status = "VISUAL" if self.visual_mode else "NORMAL"
        self._update_status(f"Mode: {status}")
    
    def action_normal_mode(self) -> None:
        """Enter normal mode."""
        self.visual_mode = False
        search_input = self.query_one("#search-input", Input)
        search_input.add_class("hidden")
        self._update_status("Mode: NORMAL")
    
    def action_select_item(self) -> None:
        """Select item in current panel."""
        if self.current_panel == "left":
            self._select_nav_item()
        elif self.current_panel == "center":
            self._select_file_item()
    
    def action_preview_item(self) -> None:
        """Preview item in right panel."""
        self._update_details_view()
    
    # Archive operations
    def action_copy_archive(self) -> None:
        """Copy selected archive."""
        if self.selected_archive:
            self.yank_buffer = self.selected_archive
            self.yank_type = "archive"
            self._update_status(f"Yanked archive: {self.selected_archive}")
    
    def action_move_archive(self) -> None:
        """Move selected archive."""
        self._update_status("Move operation not yet implemented")
    
    def action_extract_archive(self) -> None:
        """Extract selected archive."""
        self._update_status("Extract operation not yet implemented")
    
    def action_delete_item(self) -> None:
        """Delete selected item."""
        self._update_status("Delete operation not yet implemented")
    
    def action_yank_item(self) -> None:
        """Yank (copy) selected item."""
        if self.current_panel == "left":
            # Yank selected simulation/archive/location
            self.action_copy_archive()
        elif self.current_panel == "center":
            # Yank selected file
            self._update_status("File yank not yet implemented")
    
    def action_paste_item(self) -> None:
        """Paste yanked item."""
        if self.yank_buffer:
            self._update_status(f"Paste {self.yank_type}: {self.yank_buffer} (not yet implemented)")
        else:
            self._update_status("Nothing to paste")
    
    # View mode switching
    def action_show_simulations(self) -> None:
        """Show simulations view."""
        self._switch_nav_view("simulations")
    
    def action_show_archives(self) -> None:
        """Show archives view."""
        self._switch_nav_view("archives")
    
    def action_show_locations(self) -> None:
        """Show locations view."""
        self._switch_nav_view("locations")
    
    def action_show_location_manager(self) -> None:
        """Show location manager screen."""
        from .screens import LocationManagerScreen
        self.push_screen(LocationManagerScreen())
    
    def action_show_operations(self) -> None:
        """Show operations view."""
        self._switch_nav_view("operations")
    
    def action_new_simulation(self) -> None:
        """Create new simulation."""
        from .screens import SimulationManagerScreen
        self.push_screen(SimulationManagerScreen())

    # Action handlers
    def action_quit(self) -> None:
        """Quit the application."""
        self.exit()

    def action_show_help(self) -> None:
        """Show help screen."""
        help_text = """## Tellus TUI Help

### Navigation
- **h/←, j/↓, k/↑, l/→**: Vim-style navigation
- **gg/G**: Go to top/bottom
- **Enter**: Select item
- **Space**: Preview item
- **/** : Search
- **n/N**: Next/Previous search result
- **v**: Visual mode
- **Esc**: Normal mode

### Views
- **1**: Simulations view
- **2**: Archives view  
- **3**: Location Manager
- **4**: Operations/Queue view

### Operations
- **c**: Copy archive
- **m**: Move archive
- **x**: Extract archive
- **d**: Delete item
- **y**: Yank (copy to buffer)
- **p**: Paste from buffer
- **t**: Test location connection

### Bulk Operations
- **B**: Show bulk operations menu
- **Ctrl+P**: Pause operation queue
- **Ctrl+R**: Resume operation queue
- **Ctrl+K**: Cancel selected operation

### Create New
- **A**: New archive
- **L**: New location
- **S**: New simulation

### Other
- **r**: Refresh current view
- **?**: Show this help
- **q/Ctrl+C**: Quit
"""
        self._update_status("Help: Navigation h/j/k/l, Views 1-4, Bulk Ops B, Pause/Resume Ctrl+P/R")

    def action_refresh(self) -> None:
        """Refresh current view data."""
        if self._current_view == "simulations":
            self.call_later(self.load_simulations_data)
        elif self._current_view == "archives":
            self.call_later(self.load_archives_data)
        elif self._current_view == "locations":
            self.call_later(self.load_locations_data)
        elif self._current_view == "operations":
            self.call_later(self.load_operations_data)

    def action_new_archive(self) -> None:
        """Create new archive."""
        self._update_status("Create archive functionality not yet implemented")

    def action_new_location(self) -> None:
        """Create new location."""
        from .screens import LocationManagerScreen
        self.push_screen(LocationManagerScreen())
    
    def action_file_transfer_manager(self) -> None:
        """Open file transfer manager."""
        from .screens import FileTransferManagerScreen
        self.push_screen(FileTransferManagerScreen())
    
    def action_new_transfer(self) -> None:
        """Open file transfer manager for new transfer."""
        from .screens import FileTransferManagerScreen
        self.push_screen(FileTransferManagerScreen())
    
    def action_test_location(self) -> None:
        """Test connection to selected location."""
        if self.selected_location:
            self._test_location_connection(self.selected_location)
        else:
            self._update_status("No location selected for testing", error=True)
    
    # Bulk operation action handlers
    def action_bulk_operations(self) -> None:
        """Show bulk operations menu."""
        self._show_bulk_operations_menu()
    
    def action_pause_queue(self) -> None:
        """Pause the bulk operation queue."""
        self._update_status("Queue operations not yet implemented in new architecture", error=True)
    
    def action_resume_queue(self) -> None:
        """Resume the bulk operation queue."""
        self._update_status("Queue operations not yet implemented in new architecture", error=True)
    
    def action_cancel_operation(self) -> None:
        """Cancel the selected operation."""
        # Get selected operation from navigation tree
        nav_tree = self.query_one("#nav-tree", Tree)
        if nav_tree.cursor_node and nav_tree.cursor_node.label:
            node_label = str(nav_tree.cursor_node.label)
            
            # Extract operation ID from the label
            if "⚡" in node_label or "⏳" in node_label:
                # Try to extract operation ID (first 8 characters after the icon)
                parts = node_label.split(" ")
                if len(parts) >= 2:
                    op_id = parts[1].split("-")[0]  # Get ID before dash
                    self._cancel_operation(op_id)
                else:
                    self._update_status("Could not determine operation ID", error=True)
            else:
                self._update_status("Please select a running or queued operation to cancel", error=True)
        else:
            self._update_status("No operation selected for cancellation", error=True)
    
    def _test_location_connection(self, location_name: str) -> None:
        """Test connection to a specific location."""
        try:
            self._update_status(f"Testing connection to {location_name}...")
            
            # Get location using new architecture
            result = self.location_service.list_locations()
            location = None
            
            if result.success:
                for loc in result.locations:
                    if loc.name == location_name:
                        location = loc
                        break
            
            if not location:
                self._update_status(f"Location '{location_name}' not found", error=True)
                return
            
            # Connection testing would need to be implemented in the location service
            # For now, just indicate that the location is configured
            self._update_status(f"✓ Location {location_name} is configured (connection test not implemented)")
                
        except Exception as e:
            error_msg = f"Error testing location: {str(e)}"
            self._update_status(error_msg, error=True)
    
    # Helper methods for vim-like navigation
    def _update_panel_focus(self) -> None:
        """Update visual focus indicators for panels."""
        # Remove active class from all panels
        for panel_id in ["left-panel", "center-panel", "right-panel"]:
            try:
                panel = self.query_one(f"#{panel_id}")
                header = panel.query_one(".panel-header")
                header.remove_class("active")
            except Exception as e:
                error_msg = f"Error updating panel focus: {str(e)}"
                self._update_status(error_msg, error=True)
        
        # Add active class to current panel
        try:
            current_panel_id = f"{self.current_panel}-panel"
            panel = self.query_one(f"#{current_panel_id}")
            header = panel.query_one(".panel-header")
            header.add_class("active")
        except Exception as e:
            error_msg = f"Error updating panel focus: {str(e)}"
            self._update_status(error_msg, error=True)
    
    def _switch_nav_view(self, view_type: str) -> None:
        """Switch navigation tree to different view."""
        nav_tree = self.query_one("#nav-tree", Tree)
        nav_tree.clear()
        
        self._current_view = view_type
        
        if view_type == "simulations":
            self._populate_simulations_tree(nav_tree)
        elif view_type == "archives":
            self._populate_archives_tree(nav_tree)
        elif view_type == "locations":
            self._populate_locations_tree(nav_tree)
        elif view_type == "operations":
            self._populate_operations_tree(nav_tree)
    
    def _populate_simulations_tree(self, tree: Tree) -> None:
        """Populate tree with simulations."""
        tree.root.set_label("Simulations")
        
        # Add simulations
        if self.simulations_data:
            for sim in self.simulations_data:
                # Handle both dict and object formats
                if isinstance(sim, dict):
                    sim_id = sim.get('simulation_id', 'Unknown')
                    file_count = sim.get('file_count')
                else:
                    # It's a Simulation object
                    sim_id = getattr(sim, 'simulation_id', 'Unknown')
                    file_count = None  # Will calculate below
                
                sim_node = tree.root.add(f"📊 {sim_id}")
                
                # Try to get file count from simulation object
                if not file_count and hasattr(sim, '_file_registry'):
                    try:
                        file_count = len(sim._file_registry.files) if sim._file_registry.files else 0
                    except Exception as e:
                        error_msg = f"Error getting file count: {str(e)}"
                        self._update_status(error_msg, error=True)
                        file_count = 0
                
                # Add file count if available
                if file_count:
                    sim_node.add_leaf(f"📁 {file_count} files")
        else:
            tree.root.add_leaf("No simulations found")
    
    async def load_simulations_data(self) -> None:
        """Load simulations data from the service."""
        try:
            # Use new architecture service
            result = self.simulation_service.list_simulations()
            # Convert entities to dict format for TUI compatibility
            self.simulations_data = []
            for sim in result.simulations:
                sim_dict = {
                    'simulation_id': sim.simulation_id,
                    'model_id': sim.model_id,
                    'path': sim.path,
                    'attrs': sim.attrs,
                    'locations': list(sim.associated_locations)
                }
                self.simulations_data.append(sim_dict)
            self._update_status(f"Loaded {len(self.simulations_data)} simulations")
            
            # Update navigation tree if currently showing simulations
            if hasattr(self, '_current_view') and self._current_view == 'simulations':
                self._populate_simulations_tree(self.query_one("#nav-tree", Tree))
        except Exception as e:
            error_msg = f"Error loading simulations: {str(e)}"
            self._update_status(error_msg, error=True)
            self.log(error_msg)
            raise
    
    def _populate_archives_tree(self, tree: Tree) -> None:
        """Populate tree with archives."""
        tree.root.set_label("Archives")
        
        if self.archives_data:
            for archive in self.archives_data:
                archive_node = tree.root.add(f"📦 {archive.get('archive_id', 'Unknown')}")
                location = archive.get('location', 'Unknown')
                archive_node.add_leaf(f"📍 {location}")
        else:
            tree.root.add_leaf("No archives found")
    
    def _populate_locations_tree(self, tree: Tree) -> None:
        """Populate tree with locations."""
        tree.root.set_label("Locations")
        
        if self.locations_data:
            for location in self.locations_data:
                loc_node = tree.root.add(f"🌐 {location.get('name', 'Unknown')}")
                kinds = location.get('kinds', [])
                if kinds:
                    loc_node.add_leaf(f"📋 {', '.join(kinds)}")
        else:
            tree.root.add_leaf("No locations found")
    
    def _populate_operations_tree(self, tree: Tree) -> None:
        """Populate tree with operations."""
        tree.clear()
        tree.root.set_label("Bulk Operations")
        
        # Add queue statistics section
        if hasattr(self, 'queue_stats') and self.queue_stats:
            stats_node = tree.root.add("📊 Queue Statistics")
            stats = self.queue_stats
            stats_node.add_leaf(f"Total: {stats.get('total_operations', 0)}")
            stats_node.add_leaf(f"Queued: {stats.get('queued', 0)}")
            stats_node.add_leaf(f"Running: {stats.get('running', 0)}")
            stats_node.add_leaf(f"Completed: {stats.get('completed', 0)}")
            stats_node.add_leaf(f"Failed: {stats.get('failed', 0)}")
            stats_node.add_leaf(f"Max Concurrent: {stats.get('max_concurrent', 0)}")
            
            # Queue status indicators
            is_processing = stats.get('is_processing', False)
            is_paused = stats.get('is_paused', False)
            status_icon = "▶️" if is_processing and not is_paused else "⏸️" if is_paused else "⏹️"
            stats_node.add_leaf(f"Status: {status_icon} {'Processing' if is_processing and not is_paused else 'Paused' if is_paused else 'Stopped'}")
        
        # Add operations section
        if self.operations_data:
            ops_node = tree.root.add("🔄 Active Operations")
            
            # Group operations by status
            status_groups = {}
            for op in self.operations_data:
                status = op.get('status', 'unknown')
                if status not in status_groups:
                    status_groups[status] = []
                status_groups[status].append(op)
            
            # Add operations grouped by status
            for status, ops in status_groups.items():
                status_icons = {
                    'queued': '⏳',
                    'running': '⚡',
                    'completed': '✅',
                    'failed': '❌',
                    'cancelled': '🚫'
                }
                status_icon = status_icons.get(status, '❓')
                
                if len(ops) == 1:
                    op = ops[0]
                    op_id = op.get('id', 'Unknown')[:8]  # Truncate ID
                    op_type = op.get('operation_type', 'unknown').replace('bulk_', '')
                    archive_count = op.get('archive_count', 0)
                    progress = f" ({op.get('progress', 0):.0f}%)" if status == 'running' else ""
                    ops_node.add_leaf(f"{status_icon} {op_id} - {op_type} ({archive_count} archives){progress}")
                else:
                    # Group multiple operations of same status
                    status_group_node = ops_node.add(f"{status_icon} {status.title()} ({len(ops)})")
                    for op in ops[:5]:  # Show first 5
                        op_id = op.get('id', 'Unknown')[:8]
                        op_type = op.get('operation_type', 'unknown').replace('bulk_', '')
                        archive_count = op.get('archive_count', 0)
                        progress = f" ({op.get('progress', 0):.0f}%)" if status == 'running' else ""
                        status_group_node.add_leaf(f"{op_id} - {op_type} ({archive_count}){progress}")
                    if len(ops) > 5:
                        status_group_node.add_leaf(f"... and {len(ops) - 5} more")
        else:
            tree.root.add_leaf("No operations in queue")
    
    def _select_nav_item(self) -> None:
        """Handle selection of navigation tree item."""
        nav_tree = self.query_one("#nav-tree", Tree)
        if nav_tree.cursor_node:
            node_label = str(nav_tree.cursor_node.label)
            if "📊" in node_label:  # Simulation
                sim_id = node_label.replace("📊 ", "")
                self._load_simulation_files(sim_id)
            elif "📦" in node_label:  # Archive
                archive_id = node_label.replace("📦 ", "")
                self.selected_archive = archive_id
                self._load_archive_files(archive_id)
            elif "🌐" in node_label:  # Location
                location_name = node_label.replace("🌐 ", "")
                self.selected_location = location_name
                self._load_location_info(location_name)
    
    def _select_file_item(self) -> None:
        """Handle selection of file browser item."""
        file_browser = self.query_one("#file-browser", DataTable)
        # Get selected row and update details
        self._update_details_view()
        
        # Optional: perform file-specific actions
        try:
            if file_browser.cursor_row is not None and hasattr(file_browser, 'get_row'):
                row_data = file_browser.get_row(file_browser.cursor_row)
                if row_data and len(row_data) > 0:
                    file_path = row_data[0]
                    self._update_status(f"Selected file: {file_path}")
        except Exception as e:
            error_msg = f"Error updating file details: {str(e)}"
            self._update_status(error_msg, error=True)
    
    @on(DataTable.RowHighlighted, "#file-browser")
    def on_file_highlighted(self, event: DataTable.RowHighlighted) -> None:
        """Handle file browser row highlighting (cursor movement)."""
        # Update details view when cursor moves
        self._update_details_view()
    
    @on(Tree.NodeHighlighted, "#nav-tree")  
    def on_nav_highlighted(self, event: Tree.NodeHighlighted) -> None:
        """Handle navigation tree highlighting (cursor movement)."""
        # Update details view when nav cursor moves
        self._update_details_view()
    
    def _load_simulation_files(self, simulation_id: str) -> None:
        """Load files for a simulation."""
        self.current_simulation = simulation_id
        file_browser = self.query_one("#file-browser", DataTable)
        
        # Clear and set up columns
        file_browser.clear(columns=True)
        file_browser.add_columns("Path", "Size", "Type", "Archive", "Location")
        
        try:
            # Load simulation file data using new architecture
            # For now, use placeholder data since file listing is not implemented
            # in the new service yet
            
            # Check if simulation exists
            result = self.simulation_service.list_simulations()
            simulation_exists = any(sim.simulation_id == simulation_id for sim in result.simulations)
            
            if not simulation_exists:
                file_browser.add_row(f"Simulation '{simulation_id}' not found", "", "", "", "")
                return
            
            files_added = 0
            
            # For now, add placeholder data since file registry is not implemented
            # in the new service architecture yet
            file_browser.add_row(
                "Simulation file listing not yet implemented",
                "N/A",
                "Info",
                "None",
                "New Architecture"
            )
            files_added = 1
            
            # TODO: Implement file listing in the new architecture
            # This would use the simulation service to get associated files
            
            if files_added == 0:
                file_browser.add_row("No files found", "", "", "", "")
            else:
                self._update_status(f"Loaded {files_added} files for simulation {simulation_id}")
                
        except Exception as e:
            error_msg = f"Error loading simulation files: {str(e)}"
            self._update_status(error_msg, error=True)
            file_browser.add_row("Error", f"Failed to load simulation files: {str(e)}", "Exception occurred")
    
    def _load_archive_files(self, archive_id: str) -> None:
        """Load files for an archive."""
        file_browser = self.query_one("#file-browser", DataTable)
        
        # Clear and set up columns
        file_browser.clear(columns=True)
        file_browser.add_columns("Path", "Size", "Type", "Role")
        
        # Try to load archive files using new architecture
        try:
            from ...application.dtos import ListArchiveFilesDto
            dto = ListArchiveFilesDto(archive_id=archive_id)
            result = self.archive_service.list_archive_files(dto)
            
            if result.success:
                for file_info in result.files:
                    file_browser.add_row(
                        file_info.get('relative_path', ''),
                        self._format_size(file_info.get('size', 0)),
                        file_info.get('content_type', ''),
                        file_info.get('file_role', '')
                    )
            else:
                file_browser.add_row("Failed to load files", "", "", "")
        except Exception as e:
            error_msg = f"Error loading archive files: {str(e)}"
            self._update_status(error_msg, error=True)
            file_browser.add_row("Error", f"Failed to load archive files: {str(e)}", "Exception occurred")
    
    def _load_location_info(self, location_name: str) -> None:
        """Load information for a location."""
        file_browser = self.query_one("#file-browser", DataTable)
        
        # Clear and set up columns for location info
        file_browser.clear(columns=True)
        file_browser.add_columns("Property", "Value", "Details")
        
        try:
            # Load location data using new architecture
            result = self.location_service.list_locations()
            location = None
            
            if result.success:
                for loc in result.locations:
                    if loc.name == location_name:
                        location = loc
                        break
            
            if location:
                # Add basic information
                file_browser.add_row("Name", location.name, "Location identifier")
                file_browser.add_row("Protocol", location.protocol or 'file', "Access protocol")
                
                # Add configuration details
                if location.config:
                    file_browser.add_row("Host", location.config.get('host', 'localhost'), "Server hostname")
                    file_browser.add_row("Path", location.config.get('path', '/'), "Base path")
                    file_browser.add_row("Port", str(location.config.get('port', 'default')), "Connection port")
                    
                    # Add authentication info (safely)
                    if location.config.get('username'):
                        file_browser.add_row("Username", location.config.get('username'), "Authentication user")
                    
                    # Add other config options
                    for key, value in location.config.items():
                        if key not in ['host', 'path', 'port', 'username', 'password', 'key']:
                            file_browser.add_row(f"Config: {key}", str(value), "Configuration option")
                
                # Add location kinds
                if location.kinds:
                    kinds_str = ', '.join([k if isinstance(k, str) else k.name for k in location.kinds])
                    file_browser.add_row("Kinds", kinds_str, "Storage types")
                
                # Add path template if available
                if location.path:
                    file_browser.add_row("Path Template", location.path, "Path template for simulations")
                
                # Connection status would need to be implemented in the service
                file_browser.add_row("Status", "Available", "Location configured in new architecture")
                
                # Add usage stats if available
                file_browser.add_row("Optional", str(location.optional or False), "Required for operations")
                
                self._update_status(f"Loaded location info for {location_name}")
                
            else:
                file_browser.add_row("Error", "Location not found", f"No location named '{location_name}'")
                file_browser.add_row("Available", f"{len(self.locations_data)} locations", "Use 'l' key to navigate")
                
        except Exception as e:
            error_msg = f"Error loading location info: {str(e)}"
            self._update_status(error_msg, error=True)
            file_browser.add_row("Error", f"Failed to load location: {str(e)}", "Exception occurred")
    
    def _update_details_view(self) -> None:
        """Update the details panel with current selection."""
        details_placeholder = self.query_one("#item-details", Markdown)
        
        if self.current_panel == "left":
            # Show details about selected nav item
            nav_tree = self.query_one("#nav-tree", Tree)
            if nav_tree.cursor_node:
                node_label = str(nav_tree.cursor_node.label)
                if "📊" in node_label:  # Simulation
                    sim_id = node_label.replace("📊 ", "")
                    details_placeholder.renderable = f"""[bold]Simulation Details[/bold]

ID: {sim_id}
Type: Earth System Model Simulation
Status: Active

Files: Check center panel for file listing
Archives: Available in simulation registry
Locations: Various storage locations

Press [cyan]Enter[/cyan] to load files in center panel"""
                elif "📦" in node_label:  # Archive
                    archive_id = node_label.replace("📦 ", "")
                    details_placeholder.update(f"""[bold]Archive Details[/bold]

ID: {archive_id}
Type: Compressed Archive
Format: TAR/GZ

Contains simulation data files
Press [cyan]Enter[/cyan] to browse files""")
                elif "🌐" in node_label:  # Location
                    location_name = node_label.replace("🌐 ", "")
                    details_placeholder.update(f"""[bold]Location Details[/bold]

Name: {location_name}
Type: Storage Location

Configuration and properties shown in center panel
Press [cyan]Enter[/cyan] to view details""")
                else:
                    details_placeholder.update("Navigation item details")
            else:
                details_placeholder.update("Select a navigation item")
        elif self.current_panel == "center":
            # Show details about selected file
            file_browser = self.query_one("#file-browser", DataTable)
            try:
                if file_browser.cursor_row is not None:
                    # Get the current row data
                    row_key = file_browser.cursor_row
                    if hasattr(file_browser, 'get_row'):
                        row_data = file_browser.get_row(row_key)
                        if len(row_data) >= 5:  # Simulation file format
                            path, size, file_type, archive, location = row_data[:5]
                            details_placeholder.update(f"""[bold]File Details[/bold]

Path: {path}
Size: {size}
Type: {file_type}
Archive: {archive}
Location: {location}

[dim]This file belongs to simulation:[/dim] {self.current_simulation}

Operations available:
• [cyan]c[/cyan] - Copy file
• [cyan]x[/cyan] - Extract from archive
• [cyan]d[/cyan] - Download to local""")
                        elif len(row_data) >= 4:  # Archive file format
                            path, size, file_type, role = row_data[:4]
                            details_placeholder.update(f"""[bold]Archive File Details[/bold]

Path: {path}
Size: {size}
Type: {file_type}
Role: {role}

[dim]From archive:[/dim] {self.selected_archive}

Operations available:
• [cyan]x[/cyan] - Extract this file
• [cyan]d[/cyan] - Download to local
• [cyan]Space[/cyan] - Preview content""")
                        else:
                            details_placeholder.update(f"""[bold]Selected Item[/bold]

{row_data[0] if row_data else 'Unknown'}

Use arrow keys to navigate
Press [cyan]Enter[/cyan] to select""")
                    else:
                        details_placeholder.update("File selected - details unavailable")
                else:
                    details_placeholder.update("Select a file to view details")
            except Exception as e:
                error_msg = f"Error loading file details: {str(e)}"
                self._update_status(error_msg, error=True)
                details_placeholder.update(error_msg)
    
    def _highlight_search_result(self) -> None:
        """Highlight current search result."""
        # Placeholder for search result highlighting
        if self.search_results and self.search_index < len(self.search_results):
            result = self.search_results[self.search_index]
            self._update_status(f"Search result {self.search_index + 1}/{len(self.search_results)}: {result}")
    
    def _format_size(self, size_bytes: int) -> str:
        """Format file size in human readable format."""
        if size_bytes == 0:
            return "0 B"
        
        size_names = ["B", "KB", "MB", "GB", "TB"]
        i = 0
        size = float(size_bytes)
        
        while size >= 1024.0 and i < len(size_names) - 1:
            size /= 1024.0
            i += 1
        
        return f"{size:.1f} {size_names[i]}"
    
    def _update_status(self, message: str, error: bool = False) -> None:
        """Update status bar with message."""
        try:
            # For now, just update the subtitle since StatusBar widget may not exist
            self.sub_title = message
        except Exception as e:
            # Log any errors that occur during status update
            self.log(f"Status update failed: {e}")
            # Try a basic fallback to ensure the app remains responsive
            try:
                self.sub_title = "Error: See logs for details"
            except Exception as fallback_error:
                self.log(f"Fallback status update also failed: {fallback_error}")
    
    # Bulk operation helper methods
    
    def _show_bulk_operations_menu(self) -> None:
        """Show a menu for bulk operation options."""
        # For now, show a simple status message
        # In the future, this could open a modal dialog with options
        if hasattr(self, 'queue_stats') and self.queue_stats:
            stats = self.queue_stats
            total = stats.get('total_operations', 0)
            running = stats.get('running', 0)
            queued = stats.get('queued', 0)
            completed = stats.get('completed', 0)
            failed = stats.get('failed', 0)
            
            status_msg = f"Queue: {total} total, {running} running, {queued} queued, {completed} completed, {failed} failed"
            self._update_status(status_msg)
            
            # Switch to operations view to show details
            self._switch_nav_view("operations")
        else:
            self._update_status("Bulk operations queue not available - service initialization failed")
    
    def _cancel_operation(self, operation_id: str) -> None:
        """Cancel a specific operation."""
        self._update_status(f"Operation cancellation not yet implemented: {operation_id}", error=True)
    
    def _start_bulk_copy(self, archive_ids: List[str], destination_location: str, 
                        source_location: str = None, simulation_context: str = None) -> None:
        """Start a bulk copy operation."""
        self._update_status(f"Bulk copy not yet implemented ({len(archive_ids)} archives)", error=True)
    
    def _start_bulk_move(self, archive_ids: List[str], destination_location: str, 
                        source_location: str = None, simulation_context: str = None) -> None:
        """Start a bulk move operation."""
        self._update_status(f"Bulk move not yet implemented ({len(archive_ids)} archives)", error=True)
    
    def _start_bulk_extract(self, archive_ids: List[str], destination_location: str, 
                           source_location: str = None, simulation_context: str = None) -> None:
        """Start a bulk extract operation."""
        self._update_status(f"Bulk extract not yet implemented ({len(archive_ids)} archives)", error=True)