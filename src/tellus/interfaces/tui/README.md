# Tellus TUI - Terminal User Interface

A comprehensive Terminal User Interface (TUI) for the Tellus Earth System Model archive management system, built with [Textual](https://textual.textualize.io/).

## Features

### 🗂️ Interactive Archive Browser
- **File Tree Navigation**: Browse archive contents with intuitive tree structure
- **Content Filtering**: Filter files by content type, patterns, and file-specific operations
- **File Preview**: Preview file metadata and content directly in the interface
- **Earth Science Organization**: Automatically organize files by realm (atmosphere, ocean, land, ice)

### 📊 Operation Dashboard
- **Real-time Progress Monitoring**: Track copy/move/extract operations with live progress bars
- **Transfer Rate Display**: Monitor data transfer rates and estimated completion times
- **Operation Details**: View detailed information about running operations
- **Error Handling**: Display and manage operation errors with retry capabilities

### 📍 Location Manager
- **Visual Location Management**: Create, edit, and test storage locations through forms
- **Path Template Editor**: Design and test location path templates with simulation context
- **Connection Testing**: Verify connectivity to remote storage locations
- **Protocol Support**: Support for local, SSH/SFTP, S3, and cloud storage protocols

### 🔄 Operation Queue
- **Bulk Operations**: Queue multiple archive operations for batch processing
- **Priority Management**: Set operation priorities and dependencies
- **Concurrent Processing**: Configure maximum concurrent operations
- **Queue Persistence**: Save and restore operation queues across sessions

### 🌍 Earth Science Features
- **NetCDF File Analysis**: Specialized widgets for NetCDF metadata and structure
- **Model Output Classification**: Automatic classification of Earth System Model outputs
- **Standards Verification**: Check files against CF conventions and CMIP standards
- **Dataset Summaries**: Generate comprehensive summaries of Earth science datasets

## Installation

The TUI is part of the Tellus package and requires the Textual framework:

```bash
# Install Textual if not already available
pip install textual

# Check TUI dependencies
tellus tui check
```

## Usage

### Basic Commands

```bash
# Launch the full TUI application
tellus tui app

# Launch with specific focus
tellus tui app --archive my-archive-id
tellus tui app --simulation my-sim-id

# Launch specific components
tellus tui browser              # Archive browser only
tellus tui monitor             # Operation monitor
tellus tui queue               # Operation queue manager

# Development and testing
tellus tui demo                # Launch with demo data
tellus tui app --debug         # Enable debug mode
```

### Navigation

#### Global Keyboard Shortcuts
- `F1` - Show help
- `F2` - Switch to Archives tab
- `F3` - Switch to Locations tab
- `F4` - Switch to Operations tab
- `F5` - Refresh current view
- `Ctrl+N` - Create new archive
- `Ctrl+L` - Create new location
- `Q` or `Ctrl+C` - Quit application

#### Archive Browser
- `Enter` - Select file/directory
- `Space` - Preview file
- `F` - Focus filter input
- `R` - Refresh archive contents

#### Operation Queue
- `Ctrl+A` - Add new operation
- `Ctrl+S` - Start queue processing
- `Ctrl+P` - Pause/resume queue

### Screen Overview

#### Main Application Tabs

1. **Archives Tab**
   - Left sidebar: List of available archives with metadata
   - Main content: Archive details and file browser
   - Action buttons: Copy, Move, Extract, Delete operations

2. **Locations Tab**
   - Left sidebar: List of configured storage locations
   - Main content: Location details and connection status
   - Action buttons: Add, Edit, Test, Remove locations

3. **Operations Tab**
   - Left sidebar: List of active and queued operations
   - Main content: Operation monitoring and progress tracking
   - Action buttons: Start bulk operations, cancel operations

4. **Browser Tab**
   - Interactive file browser with Earth science organization
   - File metadata preview and content analysis
   - Specialized NetCDF file viewer

5. **Logs Tab**
   - System logs and operation history
   - Searchable and exportable log viewer

## Architecture Integration

### Service Layer Integration
The TUI integrates with Tellus's clean architecture:

- **Application Services**: Uses `ArchiveService`, `LocationService`, `SimulationService`
- **Domain Entities**: Works with `Archive`, `Location`, `Simulation` entities
- **Infrastructure**: Leverages `ArchiveBridge` for legacy compatibility

### Feature Flags
Automatically adapts based on enabled feature flags:

```python
# New architecture (when enabled)
TELLUS_USE_NEW_ARCHIVE_SERVICE=true

# Legacy mode (fallback)
Uses existing Simulation-based archive management
```

### Configuration
Respects existing Tellus configuration:

- Location definitions from `locations.json`
- Simulation metadata from `simulations.json`
- Cache settings and directory structure
- Authentication and credential management

## Earth Science Workflow Patterns

### Typical Workflow
1. **Browse Archives**: Navigate to find datasets of interest
2. **Classify Content**: Automatically organize by Earth science categories
3. **Queue Operations**: Set up bulk transfers or extractions
4. **Monitor Progress**: Track operations in real-time
5. **Verify Results**: Check transferred data integrity and organization

### Model Output Organization
Files are automatically organized by:
- **Realm**: Atmosphere, Ocean, Land, Ice, Biogeochemistry
- **Frequency**: Daily, Monthly, Yearly, Instantaneous
- **Purpose**: History, Restart, Diagnostic, Forcing files

### NetCDF Integration
Specialized support for NetCDF files:
- Dimension and variable analysis
- CF convention checking
- Metadata extraction and preview
- Data subset creation

## Advanced Features

### Operation Dependencies
Set up complex operation workflows:

```python
# Example: Extract after successful copy
copy_op = queue.queue_operation('copy', archive_id, src, dst)
extract_op = queue.queue_operation('extract', archive_id, dst, local)
queue.add_dependency(extract_op, copy_op)
```

### Bulk Processing
Process multiple archives efficiently:

```python
# Queue multiple operations with shared settings
operations = [
    {'operation_type': 'copy', 'archive_id': 'arch1', ...},
    {'operation_type': 'copy', 'archive_id': 'arch2', ...},
]
queue.queue_bulk_operations(operations)
```

### Custom Filters
Create domain-specific file filters:

```python
# Filter for atmospheric daily data
filter_config = {
    'realm': 'atmosphere',
    'frequency': 'daily',
    'content_type': 'output'
}
```

## Performance Considerations

### Large Datasets
- **Pagination**: File lists are paginated for archives with thousands of files
- **Lazy Loading**: Metadata loaded on-demand to reduce memory usage
- **Background Processing**: Operations run in separate threads

### Memory Management
- **Cache Management**: Automatic cleanup of cached metadata
- **Connection Pooling**: Reuse connections for remote operations
- **Resource Cleanup**: Proper cleanup of threads and network connections

### Network Optimization
- **Compression**: Use compression for remote transfers when available
- **Checksums**: Verify data integrity with configurable checksum algorithms
- **Retry Logic**: Automatic retry with exponential backoff for network errors

## Troubleshooting

### Common Issues

#### TUI Won't Start
```bash
# Check dependencies
tellus tui check

# Run in debug mode
tellus tui app --debug
```

#### Slow Performance
- Check network connectivity to remote locations
- Verify cache directory has sufficient space
- Reduce concurrent operations if system is overloaded

#### Operation Failures
- Check logs in the Logs tab
- Verify location credentials and connectivity
- Ensure sufficient disk space at destination

### Debug Mode
Enable debug mode for development:

```bash
tellus tui app --debug
```

Debug mode provides:
- Detailed error messages and stack traces
- Performance timing information
- Network request/response logging
- Memory usage statistics

## Development

### Adding Custom Widgets
Extend the TUI with custom widgets:

```python
from tellus.interfaces.tui.widgets import Widget

class CustomWidget(Widget):
    def compose(self):
        # Define widget layout
        pass
    
    def on_mount(self):
        # Initialize widget
        pass
```

### Earth Science Extensions
Add domain-specific functionality:

```python
from tellus.interfaces.tui.earth_science import EarthSystemArchiveBrowser

class CustomEarthWidget(EarthSystemArchiveBrowser):
    def _classify_single_file(self, file_info):
        # Custom classification logic
        pass
```

### Styling
Customize appearance with CSS:

```css
/* Custom styles in app.tcss */
.custom-widget {
    background: $primary;
    border: solid $accent;
}
```

## Contributing

When contributing to the TUI:

1. Follow existing widget patterns and architecture
2. Add appropriate keyboard shortcuts and accessibility features
3. Include Earth science domain expertise where relevant
4. Test with both new and legacy service architectures
5. Update documentation for new features

## Dependencies

- **Textual**: Modern TUI framework
- **Rich**: Text formatting and display
- **Tellus Core**: Archive and location services
- **Threading**: Background operation processing
- **JSON**: Configuration and state persistence

## License

Part of the Tellus project - see main project license.