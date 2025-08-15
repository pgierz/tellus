"""Main Textual TUI application for Tellus archive management."""

from typing import Optional, List, Dict, Any
import asyncio
from pathlib import Path

from textual.app import App, ComposeResult
from textual.containers import Container, Horizontal, Vertical, VerticalScroll
from textual.widgets import (
    Header, Footer, Static, Button, Input, Label, 
    DataTable, Tree, ProgressBar, Log, Placeholder,
    TabbedContent, TabPane, Select, Switch
)
from textual.binding import Binding
from textual.screen import Screen, ModalScreen
from textual.reactive import reactive, var
from textual.message import Message
from textual import on

from ..core.feature_flags import feature_flags, FeatureFlag
from ..core.service_container import get_service_container
from ..core.legacy_bridge import ArchiveBridge, SimulationBridge, LocationBridge
from .screens import (
    ArchiveBrowserScreen, LocationManagerScreen, 
    OperationDashboardScreen, OperationQueueScreen,
    ArchiveDetailsScreen, CreateArchiveScreen
)
from .widgets import (
    ArchiveList, LocationList, OperationList,
    StatusBar, LogViewer
)


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
        Binding("3", "show_locations", "Locations"),
        Binding("4", "show_operations", "Operations"),
        
        # Create new items
        Binding("A", "new_archive", "New Archive"),
        Binding("L", "new_location", "New Location"),
        Binding("S", "new_simulation", "New Simulation"),
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
        self.archive_bridge = self._get_archive_bridge()
        self.simulation_bridge = self._get_simulation_bridge()
        self.location_bridge = self._get_location_bridge()
        
        # Data storage
        self.simulations_data = []
        self.archives_data = []
        self.locations_data = []
        self.operations_data = []
        self.simulation_files_data = {}  # simulation_id -> list of files
        
    def _get_archive_bridge(self) -> Optional[ArchiveBridge]:
        """Get archive bridge if new architecture is enabled."""
        if feature_flags.is_enabled(FeatureFlag.USE_NEW_ARCHIVE_SERVICE):
            service_container = get_service_container()
            return ArchiveBridge(service_container.service_factory)
        return None
    
    def _get_simulation_bridge(self) -> Optional[SimulationBridge]:
        """Get simulation bridge if new architecture is enabled."""
        if feature_flags.is_enabled(FeatureFlag.USE_NEW_SIMULATION_SERVICE):
            service_container = get_service_container()
            return SimulationBridge(service_container.service_factory)
        return None
    
    def _get_location_bridge(self) -> Optional[LocationBridge]:
        """Get location bridge if new architecture is enabled."""
        if feature_flags.is_enabled(FeatureFlag.USE_NEW_LOCATION_SERVICE):
            service_container = get_service_container()
            return LocationBridge(service_container.service_factory)
        return None

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
                        yield Placeholder("Select an item to view details", id="item-details")
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
        
        # Load initial data
        self.call_later(self.load_archives_data)
        self.call_later(self.load_locations_data) 
        self.call_later(self.load_operations_data)
        
        # Initialize navigation with simulations view
        self.call_later(self._init_navigation)
        
        # Set up periodic refresh for operations
        self.set_interval(5.0, self.refresh_operations_data)
    
    def _init_navigation(self) -> None:
        """Initialize navigation tree with default view."""
        self._switch_nav_view("simulations")

    async def load_archives_data(self) -> None:
        """Load archives data from the service."""
        try:
            if self.archive_bridge:
                # Use new architecture
                self.archives_data = await self._load_archives_async()
                self._update_status("Archives loaded successfully")
            else:
                # Use legacy architecture
                from ..simulation.simulation import Simulation
                simulations = Simulation.list_simulations()
                self.archives_data = []
                for sim in simulations:
                    sim_obj = Simulation.get_simulation(sim['simulation_id'])
                    if hasattr(sim_obj, '_archive_registry') and sim_obj._archive_registry.archives:
                        for name, archive in sim_obj._archive_registry.archives.items():
                            status = archive.status()
                            self.archives_data.append({
                                'id': archive.archive_id,
                                'name': name,
                                'simulation': sim['simulation_id'],
                                'size': status.get('size', 0),
                                'cached': status.get('cached', False),
                                'location': status.get('location', 'Unknown')
                            })
                self._update_status("Archives loaded (legacy mode)")
            
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
            from ..location.location import Location
            locations = Location.list_locations()
            self.locations_data = []
            for loc in locations:
                self.locations_data.append({
                    'name': loc.name,
                    'kinds': [k.name for k in loc.kinds],
                    'protocol': getattr(loc, 'protocol', 'local'),
                    'path_prefix': getattr(loc, 'path_prefix', ''),
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
            # For now, simulate some operations data
            self.operations_data = []
            self.operation_count = len(self.operations_data)
            
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
    def on_create_archive(self) -> None:
        """Handle create archive button press."""
        self.push_screen(CreateArchiveScreen())

    @on(Button.Pressed, "#import-archive")
    def on_import_archive(self) -> None:
        """Handle import archive button press."""
        self._update_status("Import archive functionality not yet implemented")

    @on(Button.Pressed, "#refresh-archives")
    def on_refresh_archives(self) -> None:
        """Handle refresh archives button press."""
        self.call_later(self.load_archives_data)

    @on(Button.Pressed, "#add-location")
    def on_add_location(self) -> None:
        """Handle add location button press."""
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
    def on_start_bulk(self) -> None:
        """Handle start bulk operation button press."""
        self.push_screen(OperationQueueScreen())

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
    
    def action_show_operations(self) -> None:
        """Show operations view."""
        self._switch_nav_view("operations")
    
    def action_new_simulation(self) -> None:
        """Create new simulation."""
        self._update_status("New simulation creation not yet implemented")

    # Action handlers
    def action_quit(self) -> None:
        """Quit the application."""
        self.exit()

    def action_show_help(self) -> None:
        """Show help screen."""
        self._update_status("Help functionality not yet implemented")

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
        self.push_screen(CreateArchiveScreen())

    def action_new_location(self) -> None:
        """Create new location."""
        self.push_screen(LocationManagerScreen())
    
    def action_test_location(self) -> None:
        """Test connection to selected location."""
        if self.selected_location:
            self._test_location_connection(self.selected_location)
        else:
            self._update_status("No location selected for testing", error=True)
    
    def _test_location_connection(self, location_name: str) -> None:
        """Test connection to a specific location."""
        try:
            self._update_status(f"Testing connection to {location_name}...")
            
            from ..location.location import Location
            
            # Get location object
            location = None
            locations = Location.list_locations()
            for loc in locations:
                if loc.name == location_name:
                    location = loc
                    break
            
            if not location:
                self._update_status(f"Location '{location_name}' not found", error=True)
                return
            
            # Test filesystem access
            try:
                fs = location.fs
                if fs:
                    # Try to list the base directory
                    base_path = getattr(location, 'path', '/')
                    if hasattr(fs, 'ls'):
                        fs.ls(base_path, detail=False)
                        self._update_status(f"✓ Connection to {location_name} successful")
                    elif hasattr(fs, 'exists'):
                        if fs.exists(base_path):
                            self._update_status(f"✓ Connection to {location_name} successful")
                        else:
                            self._update_status(f"⚠ Connected to {location_name} but path {base_path} not accessible")
                    else:
                        self._update_status(f"✓ Filesystem created for {location_name}")
                else:
                    self._update_status(f"✗ Cannot create filesystem for {location_name}", error=True)
            except Exception as e:
                self._update_status(f"✗ Connection test failed: {str(e)}", error=True)
                
        except Exception as e:
            self._update_status(f"Error testing location: {str(e)}", error=True)
    
    # Helper methods for vim-like navigation
    def _update_panel_focus(self) -> None:
        """Update visual focus indicators for panels."""
        # Remove active class from all panels
        for panel_id in ["left-panel", "center-panel", "right-panel"]:
            try:
                panel = self.query_one(f"#{panel_id}")
                header = panel.query_one(".panel-header")
                header.remove_class("active")
            except:
                pass
        
        # Add active class to current panel
        try:
            current_panel_id = f"{self.current_panel}-panel"
            panel = self.query_one(f"#{current_panel_id}")
            header = panel.query_one(".panel-header")
            header.add_class("active")
        except:
            pass
    
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
        
        # Load simulations if not already loaded
        if not self.simulations_data:
            self.call_later(self.load_simulations_data)
        
        # Add simulations (placeholder for now)
        if self.simulations_data:
            for sim in self.simulations_data:
                sim_node = tree.root.add(f"📊 {sim.get('simulation_id', 'Unknown')}")
                # Add file count if available
                if sim.get('file_count'):
                    sim_node.add_leaf(f"📁 {sim['file_count']} files")
        else:
            tree.root.add_leaf("No simulations found")
    
    async def load_simulations_data(self) -> None:
        """Load simulations data from the service."""
        try:
            if self.simulation_bridge:
                # Use new architecture - placeholder for now
                self.simulations_data = []
                self._update_status("Simulations loaded (new service)")
            else:
                # Use legacy architecture
                from ..simulation.simulation import Simulation
                simulations = Simulation.list_simulations()
                self.simulations_data = simulations
                self._update_status("Simulations loaded (legacy mode)")
            
            # Update navigation tree if currently showing simulations
            if hasattr(self, '_current_view') and self._current_view == 'simulations':
                self._populate_simulations_tree(self.query_one("#nav-tree", Tree))
        except Exception as e:
            self._update_status(f"Error loading simulations: {str(e)}", error=True)
    
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
        tree.root.set_label("Operations")
        
        if self.operations_data:
            for op in self.operations_data:
                status = op.get('status', 'unknown')
                op_node = tree.root.add(f"⚡ {op.get('operation_id', 'Unknown')} ({status})")
        else:
            tree.root.add_leaf("No operations running")
    
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
        except Exception:
            pass  # Ignore errors in optional status update
    
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
            # Load real simulation file data
            from ..simulation.simulation import Simulation
            
            # Get simulation object
            simulation = Simulation.get_simulation(simulation_id)
            if not simulation:
                file_browser.add_row(f"Simulation '{simulation_id}' not found", "", "", "", "")
                return
            
            files_added = 0
            
            # Load files from simulation's file registry if available
            if hasattr(simulation, '_file_registry') and simulation._file_registry:
                for file_path, file_obj in simulation._file_registry.files.items():
                    # Get file information
                    size_str = self._format_size(getattr(file_obj, 'size', 0)) if hasattr(file_obj, 'size') else "Unknown"
                    file_type = getattr(file_obj, 'content_type', 'Unknown') if hasattr(file_obj, 'content_type') else "Unknown"
                    archive_name = getattr(file_obj, 'archive_id', 'None') if hasattr(file_obj, 'archive_id') else "None"
                    location_name = getattr(file_obj, 'location', 'Unknown') if hasattr(file_obj, 'location') else "Unknown"
                    
                    file_browser.add_row(
                        file_path,
                        size_str,
                        str(file_type),
                        archive_name,
                        location_name
                    )
                    files_added += 1
            
            # Load files from archives associated with this simulation
            if hasattr(simulation, '_archive_registry') and simulation._archive_registry.archives:
                for archive_name, archive in simulation._archive_registry.archives.items():
                    try:
                        # Try to get archive files via bridge if available
                        if self.archive_bridge:
                            files_result = self.archive_bridge.list_archive_files(archive.archive_id)
                            if files_result.get('success', False):
                                archive_files = files_result.get('files', [])
                                for file_info in archive_files:
                                    file_browser.add_row(
                                        file_info.get('relative_path', ''),
                                        self._format_size(file_info.get('size', 0)),
                                        file_info.get('content_type', 'Unknown'),
                                        archive_name,
                                        archive.location if hasattr(archive, 'location') else 'Unknown'
                                    )
                                    files_added += 1
                        else:
                            # Fallback: add archive entry
                            archive_status = archive.status() if hasattr(archive, 'status') else {}
                            file_browser.add_row(
                                f"{archive_name} (archive)",
                                self._format_size(archive_status.get('size', 0)),
                                "Archive",
                                archive_name,
                                archive_status.get('location', 'Unknown')
                            )
                            files_added += 1
                    except Exception as e:
                        # Add error row for this archive
                        file_browser.add_row(
                            f"{archive_name} (error loading)",
                            "",
                            "Error",
                            archive_name,
                            str(e)[:30] + "..." if len(str(e)) > 30 else str(e)
                        )
                        files_added += 1
            
            if files_added == 0:
                file_browser.add_row("No files found", "", "", "", "")
            else:
                self._update_status(f"Loaded {files_added} files for simulation {simulation_id}")
                
        except Exception as e:
            file_browser.add_row(f"Error loading simulation: {str(e)}", "", "", "", "")
            self._update_status(f"Error loading simulation files: {str(e)}", error=True)
    
    def _load_archive_files(self, archive_id: str) -> None:
        """Load files for an archive."""
        file_browser = self.query_one("#file-browser", DataTable)
        
        # Clear and set up columns
        file_browser.clear(columns=True)
        file_browser.add_columns("Path", "Size", "Type", "Role")
        
        # Try to load archive files via bridge
        if self.archive_bridge:
            try:
                # This would call the actual archive file listing
                files_result = self.archive_bridge.list_archive_files(archive_id)
                if files_result.get('success', False):
                    files = files_result.get('files', [])
                    for file_info in files:
                        file_browser.add_row(
                            file_info.get('relative_path', ''),
                            self._format_size(file_info.get('size', 0)),
                            file_info.get('content_type', ''),
                            file_info.get('file_role', '')
                        )
                else:
                    file_browser.add_row("Failed to load files", "", "", "")
            except Exception as e:
                file_browser.add_row(f"Error: {str(e)}", "", "", "")
        else:
            file_browser.add_row("Archive service not available", "", "", "")
    
    def _load_location_info(self, location_name: str) -> None:
        """Load information for a location."""
        file_browser = self.query_one("#file-browser", DataTable)
        
        # Clear and set up columns for location info
        file_browser.clear(columns=True)
        file_browser.add_columns("Property", "Value", "Details")
        
        try:
            # Load real location data
            from ..location.location import Location
            
            # Get location object
            try:
                location = Location.from_name(location_name)
            except:
                # Fallback: find from location list
                location = None
                locations = Location.list_locations()
                for loc in locations:
                    if loc.name == location_name:
                        location = loc
                        break
            
            if location:
                # Add basic information
                file_browser.add_row("Name", location.name, "Location identifier")
                file_browser.add_row("Protocol", getattr(location, 'protocol', 'file'), "Access protocol")
                
                # Add configuration details
                if hasattr(location, 'config') and location.config:
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
                if hasattr(location, 'kinds') and location.kinds:
                    kinds_str = ', '.join([k.name if hasattr(k, 'name') else str(k) for k in location.kinds])
                    file_browser.add_row("Kinds", kinds_str, "Storage types")
                
                # Add path prefix if available
                if hasattr(location, 'path_prefix') and location.path_prefix:
                    file_browser.add_row("Path Template", location.path_prefix, "Path template for simulations")
                
                # Add connection status
                try:
                    # Try to check if filesystem is accessible
                    fs = location.fs
                    if fs:
                        file_browser.add_row("Filesystem", "Available", "Filesystem access ready")
                        
                        # Try to check if base path exists
                        base_path = getattr(location, 'path', '/')
                        if hasattr(fs, 'exists') and fs.exists(base_path):
                            file_browser.add_row("Base Path Status", "Exists", f"Path {base_path} is accessible")
                        else:
                            file_browser.add_row("Base Path Status", "Not accessible", f"Cannot access {base_path}")
                    else:
                        file_browser.add_row("Filesystem", "Not available", "Cannot create filesystem connection")
                except Exception as e:
                    file_browser.add_row("Connection Error", str(e)[:50], "Filesystem connection failed")
                
                # Add usage stats if available
                file_browser.add_row("Optional", str(getattr(location, 'optional', False)), "Required for operations")
                
                self._update_status(f"Loaded location info for {location_name}")
                
            else:
                file_browser.add_row("Error", "Location not found", f"No location named '{location_name}'")
                file_browser.add_row("Available", f"{len(self.locations_data)} locations", "Use 'l' key to navigate")
                
        except Exception as e:
            file_browser.add_row("Error", f"Failed to load location: {str(e)}", "Exception occurred")
            self._update_status(f"Error loading location info: {str(e)}", error=True)
    
    def _update_details_view(self) -> None:
        """Update the details panel with current selection."""
        details_placeholder = self.query_one("#item-details", Placeholder)
        
        if self.current_panel == "left":
            # Show details about selected nav item
            nav_tree = self.query_one("#nav-tree", Tree)
            if nav_tree.cursor_node:
                node_label = str(nav_tree.cursor_node.label)
                if "📊" in node_label:  # Simulation
                    sim_id = node_label.replace("📊 ", "")
                    details_placeholder.update(f"""[bold]Simulation Details[/bold]

ID: {sim_id}
Type: Earth System Model Simulation
Status: Active

Files: Check center panel for file listing
Archives: Available in simulation registry
Locations: Various storage locations

Press [cyan]Enter[/cyan] to load files in center panel""")
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
                details_placeholder.update(f"Error loading file details: {str(e)}")
        else:
            details_placeholder.update("Select an item to view details")
    
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
            status_bar = self.query_one("#status-bar", StatusBar)
            status_bar.update_message(message, error=error)
        except:
            # Fallback to updating sub_title if status bar not found
            self.sub_title = message