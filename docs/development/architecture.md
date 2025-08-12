# Architecture Overview

This document provides an overview of Tellus's architecture, design patterns, and key components. Understanding this architecture will help you contribute effectively and make informed decisions about extending the system.

## High-Level Architecture

Tellus follows a modular design with clear separation of concerns:

```{mermaid}
graph TD
    A[CLI Interface] --> B[Core Application]
    B --> C[Simulation Management]
    B --> D[Location Management]
    C --> E[Context & Templating]
    D --> F[Storage Abstraction]
    F --> G[Local Filesystem]
    F --> H[SSH/SFTP]
    F --> I[Cloud Storage]
    E --> J[Path Resolution]
    C --> K[Metadata & Persistence]
    D --> K
```

## Core Components

### 1. CLI Layer (`tellus.core.cli`)

The command-line interface provides the primary user interaction:

- **Entry Point**: `cli.py` defines the main command group
- **Rich Integration**: Uses rich-click for beautiful, interactive CLI
- **Subcommand Structure**: Organized by domain (simulation, location)
- **Interactive Wizards**: Questionary-based wizards for complex operations

```python
# CLI structure
@click.group(name=\"tellus\")
def cli():
    \"\"\"Tellus - A distributed data management system.\"\"\"

# Subcommands are added from domain modules
from tellus.simulation.cli import simulation
from tellus.location.cli import location
cli.add_command(simulation)
cli.add_command(location)
```

### 2. Simulation Management (`tellus.simulation`)

Manages computational experiments and their metadata:

#### Core Classes

**`Simulation`**: Central entity representing a computational experiment
```python
class Simulation:
    simulation_id: str          # Unique identifier
    path: Optional[str]         # Base filesystem path
    attrs: Dict[str, Any]       # Metadata attributes
    locations: Dict[str, Any]   # Attached storage locations
```

**Key Features:**
- **Persistence**: JSON-based storage in `simulations.json`
- **Location Management**: Attach/detach storage locations with context
- **Path Resolution**: Dynamic path generation using templates
- **Metadata**: Flexible attribute system for experiment tracking

#### Context System (`tellus.simulation.context`)

Provides path templating and metadata for location usage:

```python
class LocationContext:
    path_prefix: Optional[str]      # Template like \"{model}/{experiment}\"
    overrides: Dict[str, Any]       # Override simulation attributes
    metadata: Dict[str, Any]        # Context-specific metadata
```

**Template Resolution:**
- Uses simulation attributes as template variables
- Supports nested path structures
- Enables flexible data organization patterns

### 3. Location Management (`tellus.location`)

Abstracts storage backends and provides unified access:

#### Core Classes

**`Location`**: Represents a storage backend
```python
class Location:
    name: str                       # Human-readable identifier
    kinds: List[LocationKind]       # Storage characteristics
    config: Dict[str, Any]          # Backend configuration
    optional: bool                  # Whether location is required
```

**`LocationKind`**: Enum defining storage characteristics
```python
class LocationKind(Enum):
    DISK = \"disk\"                  # Local or mounted filesystem
    ARCHIVE = \"archive\"            # Long-term storage (tape, etc.)
    CACHE = \"cache\"                # Temporary/fast storage
    REMOTE = \"remote\"              # Network-accessible storage
```

#### Storage Abstraction

Uses **fsspec** for unified filesystem interface:

```python
# Location creates filesystem instances
self.fs = fsspec.filesystem(
    protocol=self.config[\"protocol\"],
    **self.config.get(\"storage_options\", {})
)
```

**Supported Protocols:**
- `file`: Local filesystem
- `ssh`: SSH/SFTP servers  
- `s3`: Amazon S3
- `gcs`: Google Cloud Storage
- `ftp`: FTP servers
- Custom protocols via fsspec plugins

### 4. Progress Tracking (`tellus.progress`)

Provides beautiful progress bars for file operations:

#### Key Classes

**`FSSpecProgressCallback`**: Bridges fsspec and Rich progress bars
```python
class FSSpecProgressCallback(Callback):
    def __init__(self, description: str, size: Optional[int] = None):
        # Initialize Rich progress bar
        
    def call(self, *args, **kwargs):
        # Update progress bar based on fsspec events
```

**Features:**
- Real-time progress updates
- Transfer speed calculation
- Time remaining estimation
- Beautiful terminal output via Rich

### 5. File Operations

File transfer operations with progress tracking:

```python
# Download with progress
with location.get_fileobj(remote_path, progress=progress) as (f, size):
    # Stream file with progress updates
    with open(local_path, \"wb\") as local_f:
        shutil.copyfileobj(f, local_f)
```

## Design Patterns

### 1. Configuration Management

**Pattern**: JSON-based persistence with in-memory objects

```python
# Simulations persist to simulations.json
class Simulation:
    @classmethod
    def load_simulations(cls):
        # Load from JSON file
        
    @classmethod  
    def save_simulations(cls):
        # Save to JSON file
```

**Benefits:**
- Simple, human-readable storage
- Easy backup and version control
- No database dependencies

### 2. Plugin Architecture

**Pattern**: fsspec-based storage plugins

```python
# Locations use fsspec's plugin system
fs = fsspec.filesystem(protocol, **options)
```

**Benefits:**
- Easy to add new storage backends
- Leverage existing fsspec ecosystem
- Consistent interface across all storage types

### 3. Template-Based Path Resolution

**Pattern**: String template substitution with simulation attributes

```python
# Context provides path templating
def resolve_path(self, simulation: Simulation) -> str:
    template_vars = {**simulation.attrs, **self.overrides}
    return self.path_prefix.format(**template_vars)
```

**Benefits:**
- Flexible data organization
- Dynamic path generation
- Consistent naming patterns

### 4. CLI Command Organization

**Pattern**: Domain-based command grouping with shared utilities

```python
# Each domain has its own CLI module
@cli.group()
def simulation():
    \"\"\"Manage simulations\"\"\"

@simulation.command()
def create(sim_id: str):
    # Implementation
```

**Benefits:**
- Clear separation of concerns
- Easy to extend with new commands
- Consistent user experience

## Data Flow

### Typical User Workflow

1. **Create Simulation**: User creates simulation with metadata
2. **Configure Locations**: User adds storage locations to simulation
3. **Apply Context**: System resolves paths using templates
4. **Transfer Files**: User downloads/uploads files with progress tracking

```{mermaid}
sequenceDiagram
    participant User
    participant CLI
    participant Simulation
    participant Location
    participant Storage

    User->>CLI: tellus simulation create
    CLI->>Simulation: Create simulation object
    Simulation->>Simulation: Save to JSON
    
    User->>CLI: tellus simulation location add
    CLI->>Location: Load location config
    CLI->>Simulation: Add location with context
    
    User->>CLI: tellus simulation location get
    CLI->>Simulation: Resolve path with context
    CLI->>Location: Access storage backend
    Location->>Storage: Transfer file with progress
```

### Path Resolution Flow

```python
# 1. User specifies template in context
context = LocationContext(path_prefix=\"{model}/{experiment}/run_{run_id}\")

# 2. Simulation provides attributes
simulation.attrs = {\"model\": \"CESM2\", \"experiment\": \"ssp585\", \"run_id\": \"001\"}

# 3. System resolves path
resolved_path = context.resolve_path(simulation)
# Result: \"CESM2/ssp585/run_001\"

# 4. Combine with location base path
full_path = location.base_path + \"/\" + resolved_path
```

## Extension Points

### Adding New Storage Backends

1. **Use fsspec protocol**: Most storage systems already have fsspec support
2. **Custom authentication**: Extend location configuration for new auth methods
3. **Protocol-specific features**: Add specialized methods for unique capabilities

```python
# Example: Adding WebDAV support
class WebDAVLocation(Location):
    def __init__(self, name: str, webdav_url: str, **kwargs):
        config = {
            \"protocol\": \"webdav\",
            \"storage_options\": {\"base_url\": webdav_url}
        }
        super().__init__(name, config=config, **kwargs)
```

### Adding New CLI Commands

1. **Create command function**: Use rich-click decorators
2. **Add to command group**: Register with appropriate parent
3. **Follow patterns**: Use existing error handling and output formatting

```python
@simulation.command()
@click.argument(\"sim_id\")
def export(sim_id: str):
    \"\"\"Export simulation data to archive format.\"\"\"
    sim = get_simulation_or_exit(sim_id)
    # Implementation
```

### Adding New Context Features

1. **Extend LocationContext**: Add new fields and methods
2. **Update path resolution**: Modify template processing
3. **Maintain backwards compatibility**: Support existing templates

## Performance Considerations

### Memory Usage
- **Lazy loading**: Load simulations/locations only when needed
- **Streaming transfers**: Use file streams for large files
- **Progress chunking**: Update progress in reasonable intervals

### Network Efficiency
- **Connection reuse**: fsspec handles connection pooling
- **Parallel transfers**: Support concurrent file operations
- **Resume capability**: Handle interrupted transfers gracefully

### Storage Efficiency
- **Metadata caching**: Cache location filesystem metadata
- **Path optimization**: Minimize path resolution overhead
- **Batch operations**: Group multiple file operations

## Security Considerations

### Authentication
- **Credential storage**: Use secure credential management
- **Session management**: Handle authentication tokens properly
- **Access control**: Respect storage backend permissions

### Data Protection
- **Path validation**: Prevent directory traversal attacks
- **Input sanitization**: Validate all user inputs
- **Secure transfers**: Use encrypted protocols when available

## Testing Strategy

### Unit Tests
- **Component isolation**: Test individual classes and functions
- **Mock external dependencies**: Use mocks for network/filesystem
- **Edge case coverage**: Test error conditions and boundary cases

### Integration Tests
- **CLI testing**: Test complete command workflows
- **Storage testing**: Test with real storage backends
- **End-to-end scenarios**: Test typical user workflows

### Documentation Tests
- **Example validation**: Ensure all examples work
- **Link checking**: Verify all documentation links
- **API consistency**: Check that docs match implementation

This architecture provides a solid foundation for managing distributed scientific data while remaining extensible and maintainable.