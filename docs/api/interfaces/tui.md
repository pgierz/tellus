# Text User Interface (TUI)

The TUI provides an interactive, terminal-based interface for managing Tellus operations with vim-style navigation and a ranger-like file browser layout.

## TUI Application

### Main TUI Application

```{eval-rst}
.. currentmodule:: tellus.interfaces.tui.app

.. autoclass:: TellusApp
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__

.. autoclass:: MainScreen
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__
```

### TUI CLI Integration

```{eval-rst}
.. currentmodule:: tellus.interfaces.tui.cli

.. autofunction:: tui
.. autofunction:: launch_tui
```

## Screen Components

### Main Screens

```{eval-rst}
.. currentmodule:: tellus.interfaces.tui.screens

.. autoclass:: SimulationScreen
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__

.. autoclass:: LocationScreen
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__

.. autoclass:: ArchiveScreen
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__

.. autoclass:: TransferScreen
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__

.. autoclass:: ProgressScreen
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__
```

## UI Widgets

### Navigation and Display Widgets

```{eval-rst}
.. currentmodule:: tellus.interfaces.tui.widgets

.. autoclass:: NavigationTree
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__

.. autoclass:: DataTable
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__

.. autoclass:: DetailPanel
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__

.. autoclass:: StatusBar
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__

.. autoclass:: ProgressWidget
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__
```

### File Browser Widgets

```{eval-rst}
.. autoclass:: FileBrowser
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__

.. autoclass:: FileTree
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__

.. autoclass:: FileDetails
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__
```

## Data Managers

### TUI Data Management

```{eval-rst}
.. currentmodule:: tellus.interfaces.tui.managers

.. autoclass:: SimulationManager
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__

.. autoclass:: LocationManager
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__

.. autoclass:: ArchiveManager
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__

.. autoclass:: ProgressManager
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__
```

## Operation Queue

### Queue Management

```{eval-rst}
.. currentmodule:: tellus.interfaces.tui.operation_queue

.. autoclass:: OperationQueueWidget
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__

.. autoclass:: QueuedOperation
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__
```

## TUI Layout and Navigation

### Three-Panel Layout

The TUI uses a ranger-inspired three-panel layout:

```
┌─────────────────┬─────────────────┬─────────────────┐
│   Navigation    │   Main View     │   Details       │
│   (Left Panel)  │  (Center Panel) │  (Right Panel)  │
├─────────────────┼─────────────────┼─────────────────┤
│ • Simulations   │ Data Browser    │ Metadata &      │
│   - cesm2-hist  │ ┌─────────────┐ │ Properties      │
│   - gfdl-ssp585 │ │Path    Size │ │                 │  
│ • Locations     │ │file1.nc 1GB│ │ Selected Item:  │
│   - hpc-scratch │ │file2.nc 2GB│ │ file1.nc        │
│   - cloud-arch  │ │dir1/     --│ │                 │
│ • Archives      │ └─────────────┘ │ Size: 1.2 GB    │
│ • Operations    │                 │ Type: NetCDF    │
│ • Queue         │                 │ Modified: Today │
└─────────────────┴─────────────────┴─────────────────┘
```

### Vim-Style Navigation

```
Movement Keys:
  h, j, k, l    - Move left, down, up, right
  gg            - Go to top of current panel
  G             - Go to bottom of current panel
  /             - Search within current view
  n, N          - Next/previous search result

Panel Navigation:
  Tab           - Switch to next panel
  Shift+Tab     - Switch to previous panel
  
Actions:
  Enter         - Select/expand item
  Space         - Toggle item selection
  r             - Refresh current view
  q             - Quit current view or application
  ?             - Show help overlay

File Operations:
  y             - Yank (copy) selected items
  p             - Paste to current location
  d             - Delete selected items
  c             - Create new item
  e             - Edit item properties
  
Location Specific:
  t             - Test location connection
  m             - Mount/unmount location
  s             - Show location statistics

Progress Operations:
  p             - Pause operation
  r             - Resume operation  
  c             - Cancel operation
  v             - View operation details
```

## Usage Examples

### Launching the TUI

```bash
# Launch full TUI application
tellus tui

# Launch with specific screen
tellus tui --screen simulations
tellus tui --screen locations
tellus tui --screen archives

# Launch in development mode with debug info
tellus tui --debug --log-file tui.log
```

### TUI Configuration

```python
from tellus.interfaces.tui.app import TellusApp

# Configure TUI appearance
app = TellusApp()

app.configure_theme({
    "primary": "#00d4aa",
    "secondary": "#0178d4", 
    "accent": "#f0f0f0",
    "background": "#1e1e1e",
    "surface": "#2d2d30"
})

app.configure_keybindings({
    "quit": ["q", "ctrl+c"],
    "help": ["?", "h"],
    "refresh": ["r", "f5"],
    "search": ["/", "ctrl+f"]
})

app.configure_panels({
    "navigation_width": 20,
    "details_width": 25,
    "show_status_bar": True,
    "show_help_footer": True
})
```

### Custom TUI Screens

```python
from textual.screen import Screen
from textual.widgets import Header, Footer, DataTable
from tellus.interfaces.tui.widgets import DetailPanel

class CustomAnalysisScreen(Screen):
    """Custom screen for climate data analysis."""
    
    BINDINGS = [
        ("r", "refresh", "Refresh"),
        ("a", "analyze", "Analyze"),
        ("e", "export", "Export"),
        ("q", "quit", "Quit")
    ]
    
    def compose(self):
        yield Header()
        
        with Horizontal():
            # Analysis results table
            yield DataTable(id="analysis_results")
            
            # Analysis details
            yield DetailPanel(id="analysis_details")
        
        yield Footer()
    
    def on_mount(self):
        """Initialize screen when mounted."""
        self.title = "Climate Data Analysis"
        
        # Setup analysis results table
        table = self.query_one("#analysis_results", DataTable)
        table.add_columns("Variable", "Mean", "Std Dev", "Trend")
        
        # Load analysis data
        self.refresh_analysis_data()
    
    def action_refresh(self):
        """Refresh analysis data."""
        self.refresh_analysis_data()
    
    def action_analyze(self):
        """Run new analysis."""
        # Implementation for running analysis
        pass
    
    def action_export(self):
        """Export analysis results."""
        # Implementation for exporting results
        pass
    
    def refresh_analysis_data(self):
        """Refresh the analysis data display."""
        # Implementation for loading and displaying data
        pass
```

### TUI Event Handling

```python
from textual import on
from textual.message import Message

class SimulationSelectedMessage(Message):
    """Message sent when a simulation is selected."""
    
    def __init__(self, simulation_id: str):
        super().__init__()
        self.simulation_id = simulation_id

class EnhancedSimulationScreen(SimulationScreen):
    """Enhanced simulation screen with custom event handling."""
    
    @on(DataTable.RowSelected, "#simulation_table")
    def on_simulation_selected(self, event):
        """Handle simulation selection."""
        row_key = event.row_key
        simulation_data = self.get_simulation_data(row_key)
        
        # Update details panel
        details_panel = self.query_one("#simulation_details", DetailPanel)
        details_panel.update_content(simulation_data)
        
        # Post message for other components
        self.post_message(SimulationSelectedMessage(simulation_data["simulation_id"]))
    
    @on(SimulationSelectedMessage)
    def handle_simulation_selection(self, message):
        """Handle simulation selection message."""
        simulation_id = message.simulation_id
        
        # Update file browser with simulation files
        file_browser = self.query_one("#file_browser", FileBrowser)
        file_browser.load_simulation_files(simulation_id)
        
        # Update status bar
        status_bar = self.query_one("#status_bar", StatusBar)
        status_bar.update_status(f"Selected: {simulation_id}")
```

### TUI Data Binding

```python
from textual.reactive import reactive
from tellus.application.container import ServiceContainer

class ReactiveSimulationManager:
    """Reactive simulation manager for TUI."""
    
    def __init__(self):
        self.container = ServiceContainer()
        self.simulation_service = self.container.get_simulation_service()
        
        # Reactive properties
        self.simulations = reactive([])
        self.selected_simulation = reactive(None)
        self.filter_text = reactive("")
    
    def watch_filter_text(self, filter_text: str):
        """React to filter text changes."""
        self.refresh_simulations()
    
    def watch_selected_simulation(self, simulation):
        """React to simulation selection changes."""
        if simulation:
            self.load_simulation_details(simulation.simulation_id)
    
    def refresh_simulations(self):
        """Refresh simulation list with current filter."""
        all_simulations = self.simulation_service.list_simulations()
        
        if self.filter_text:
            # Apply filter
            filtered = [
                sim for sim in all_simulations.simulations
                if self.filter_text.lower() in sim.simulation_id.lower()
                or self.filter_text.lower() in sim.model_id.lower()
            ]
            self.simulations = filtered
        else:
            self.simulations = all_simulations.simulations
    
    def load_simulation_details(self, simulation_id: str):
        """Load detailed information for simulation."""
        # Implementation for loading simulation details
        pass

# Use reactive manager in TUI screen
class ReactiveSimulationScreen(Screen):
    def __init__(self):
        super().__init__()
        self.manager = ReactiveSimulationManager()
        
        # Bind manager updates to UI updates
        self.manager.watch(self.manager, "simulations", self.update_simulation_table)
        self.manager.watch(self.manager, "selected_simulation", self.update_details_panel)
    
    def update_simulation_table(self, simulations):
        """Update simulation table with new data."""
        table = self.query_one("#simulation_table", DataTable)
        table.clear()
        
        for sim in simulations:
            table.add_row(
                sim.simulation_id,
                sim.model_id,
                sim.attrs.get("experiment", "unknown"),
                sim.attrs.get("status", "unknown")
            )
    
    def update_details_panel(self, simulation):
        """Update details panel with selected simulation."""
        if simulation:
            details = self.query_one("#details_panel", DetailPanel)
            details.update_simulation_details(simulation)
```

### TUI Progress Integration

```python
from tellus.interfaces.tui.widgets import ProgressWidget
from tellus.domain.entities.progress_tracking import ProgressOperation

class TUIProgressIntegration:
    """Integration between TUI and progress tracking."""
    
    def __init__(self, app):
        self.app = app
        self.container = ServiceContainer()
        self.progress_service = self.container.get_progress_tracking_service()
        
        # Monitor progress updates
        self.setup_progress_monitoring()
    
    def setup_progress_monitoring(self):
        """Set up automatic progress monitoring."""
        self.app.set_interval(1.0, self.update_progress_displays)
    
    def update_progress_displays(self):
        """Update all progress displays in the TUI."""
        # Get active operations
        operations = self.progress_service.list_operations(
            status_filter="running"
        )
        
        # Update progress widgets
        for operation in operations.operations:
            self.update_operation_progress(operation)
    
    def update_operation_progress(self, operation: ProgressOperation):
        """Update progress display for specific operation."""
        # Find progress widget for this operation
        progress_widget = self.app.query_one(
            f"#progress_{operation.operation_id}", 
            ProgressWidget
        )
        
        if progress_widget:
            progress_widget.update_progress(
                current=operation.current_step or 0,
                total=operation.total_steps or 100,
                description=operation.description,
                status=operation.status_message or ""
            )
    
    def create_operation_progress_widget(self, operation: ProgressOperation):
        """Create new progress widget for operation."""
        progress_widget = ProgressWidget(
            id=f"progress_{operation.operation_id}",
            operation_id=operation.operation_id,
            description=operation.description
        )
        
        # Add to progress screen
        progress_screen = self.app.get_screen("progress")
        progress_screen.mount(progress_widget)
        
        return progress_widget
```

### TUI Customization and Theming

```css
/* TUI CSS styling (app.tcss) */

/* Main layout */
#main_container {
    layout: horizontal;
    height: 100%;
}

#navigation_panel {
    width: 20%;
    border-right: solid $accent;
}

#content_panel {
    width: 55%;
    border-right: solid $accent;
}

#details_panel {
    width: 25%;
}

/* Navigation tree styling */
NavigationTree {
    background: $surface;
    color: $text;
}

NavigationTree > .navigation-node {
    padding: 0 1;
}

NavigationTree > .navigation-node--selected {
    background: $primary;
    color: $background;
}

/* Data table styling */
DataTable {
    background: $background;
}

DataTable > .datatable--header {
    background: $surface;
    color: $accent;
    text-style: bold;
}

DataTable > .datatable--selected {
    background: $secondary;
    color: $background;
}

/* Progress widgets */
ProgressWidget {
    height: 3;
    margin: 1 0;
    border: solid $accent;
}

ProgressWidget > .progress-bar {
    background: $primary;
}

ProgressWidget > .progress-text {
    text-align: center;
    color: $text;
}

/* Status bar */
StatusBar {
    dock: bottom;
    height: 1;
    background: $surface;
    color: $text;
}

/* File browser */
FileBrowser {
    background: $background;
}

FileBrowser > .file-entry {
    padding: 0 1;
}

FileBrowser > .file-entry--directory {
    color: $primary;
    text-style: bold;
}

FileBrowser > .file-entry--file {
    color: $text;
}

FileBrowser > .file-entry--selected {
    background: $secondary;
}
```

### TUI Testing and Development

```python
import pytest
from textual.testing import AppTest
from tellus.interfaces.tui.app import TellusApp

def test_tui_navigation():
    """Test TUI navigation functionality."""
    
    with AppTest(TellusApp) as pilot:
        # Test initial screen
        assert pilot.app.screen.title == "Tellus Climate Data Manager"
        
        # Test navigation to simulations
        pilot.press("tab")  # Focus navigation panel
        pilot.press("j")    # Move down to simulations
        pilot.press("enter") # Enter simulations screen
        
        # Verify simulation screen is displayed
        assert "simulations" in pilot.app.screen.id.lower()

def test_tui_simulation_selection():
    """Test simulation selection in TUI."""
    
    with AppTest(TellusApp) as pilot:
        # Navigate to simulations
        pilot.press("tab", "j", "enter")
        
        # Select first simulation
        pilot.press("j")     # Move to first simulation
        pilot.press("enter") # Select simulation
        
        # Verify details panel is updated
        details_panel = pilot.app.query_one("#details_panel")
        assert details_panel.has_content()

def test_tui_progress_updates():
    """Test progress display updates."""
    
    with AppTest(TellusApp) as pilot:
        # Create mock operation
        operation = ProgressOperation(
            operation_id="test-op",
            operation_type="test",
            description="Test operation"
        )
        
        # Add progress widget
        progress_widget = pilot.app.query_one("#progress_test-op")
        
        # Simulate progress update
        progress_widget.update_progress(50, 100, "Halfway complete")
        
        # Verify progress display
        assert "50%" in progress_widget.render()
```

## Performance Optimization

The TUI is optimized for responsiveness and low resource usage:

### Lazy Loading

```python
# TUI components use lazy loading for better performance
class LazySimulationTable(DataTable):
    """Simulation table with lazy loading."""
    
    def __init__(self):
        super().__init__()
        self._data_loaded = False
        self._visible_range = (0, 50)  # Load only visible items
    
    def on_show(self):
        """Load data when table becomes visible."""
        if not self._data_loaded:
            self.load_visible_data()
            self._data_loaded = True
    
    def on_scroll(self, event):
        """Load additional data as user scrolls."""
        if self.needs_more_data():
            self.load_additional_data()
```

### Event Debouncing

```python
from textual.timer import Timer

class DebouncedSearchWidget(Input):
    """Search widget with debounced input handling."""
    
    def __init__(self):
        super().__init__()
        self._search_timer: Timer | None = None
    
    def on_input_changed(self, event):
        """Handle input changes with debouncing."""
        # Cancel previous timer
        if self._search_timer:
            self._search_timer.stop()
        
        # Set new timer for delayed search
        self._search_timer = self.set_timer(
            0.5,  # 500ms delay
            lambda: self.perform_search(event.value)
        )
    
    def perform_search(self, query: str):
        """Perform the actual search operation."""
        # Implementation for search
        pass
```

### Memory Management

```python
# TUI implements memory management for large datasets
class MemoryManagedTUI:
    """TUI with memory management for large datasets."""
    
    def __init__(self):
        self._data_cache = {}
        self._cache_size_limit = 100  # MB
        self._cache_ttl = 300  # 5 minutes
    
    def cache_data(self, key: str, data):
        """Cache data with size and TTL limits."""
        # Implementation for caching with limits
        pass
    
    def cleanup_cache(self):
        """Clean up expired or excess cached data."""
        # Implementation for cache cleanup
        pass
    
    def get_memory_usage(self):
        """Get current memory usage statistics."""
        # Implementation for memory monitoring
        pass
```