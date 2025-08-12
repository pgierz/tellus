# API Reference

This section provides comprehensive API documentation for all Tellus classes, functions, and modules.

```{note}
This is a draft API reference structure. The actual API documentation will be auto-generated from docstrings using Sphinx autodoc.
```

## Core Classes

### Simulation

The main entry point for Tellus operations.

```{eval-rst}
.. currentmodule:: tellus

.. autoclass:: Simulation
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__
```

**Key Methods:**
- `add_location()` - Add a new storage location
- `get_location()` - Retrieve an existing location
- `list_locations()` - List all configured locations
- `save()` - Save simulation configuration
- `load()` - Load existing simulation

### Location Classes

#### Location

Base class for all storage location types.

```{eval-rst}
.. autoclass:: Location
   :members:
   :undoc-members:
   :show-inheritance:
```

**Note**: Specific location types (SSH, S3, etc.) will be documented here once implemented in the codebase.

## Additional Classes

```{note}
Additional utility classes, configuration classes, exceptions, and functions will be documented here as they are implemented in the codebase. The current API reference shows the structure that will be auto-generated from docstrings.
```

### Planned Classes
- **FileInfo** - Information about files and directories
- **TransferProgress** - Progress tracking for file transfers  
- **Cache** - Caching system for remote data
- **SimulationConfig** - Configuration management for simulations
- **LocationConfig** - Configuration for storage locations

### Planned Exception Classes
- **TellusError** - Base exception class for all Tellus errors
- **ConnectionError** - Raised when connection to remote storage fails
- **TransferError** - Raised when file transfers fail
- **PermissionError** - Raised when file permission errors occur
- **ConfigurationError** - Raised when configuration is invalid

### Planned Module Functions
- **list_simulations()** - List all available simulations
- **configure()** - Configure global settings
- **get_version()** - Get Tellus version information
- **set_log_level()** - Set logging level

## Archive System API

```{note}
The archive system provides intelligent file archiving and caching capabilities. Documentation will be auto-generated once implemented.
```

### Planned Archive Classes
- **ArchiveSystem** - Main archive system class
- **Archive** - Individual archive representation  
- **ArchiveEntry** - Individual file/directory within an archive

## Interactive Features

```{note}
Interactive wizards provide guided workflows for complex operations. Documentation will be auto-generated once implemented.
```

### Planned Interactive Classes
- **Wizard** - Base class for interactive wizards
- **SimulationWizard** - Interactive simulation creation wizard
- **LocationWizard** - Interactive location configuration wizard

## CLI Interface

```{note}
Command-line interface functions provide programmatic access to CLI operations. Documentation will be auto-generated once implemented.
```

## Type Definitions

Common type aliases used throughout Tellus:

```python
from typing import Union, List, Dict, Optional, Callable, Any
from pathlib import Path

# Path types
PathLike = Union[str, Path]
PathList = List[PathLike]

# Configuration types
ConfigDict = Dict[str, Any]
MetadataDict = Dict[str, Any]

# Callback types
ProgressCallback = Callable[[int, int], None]
ErrorCallback = Callable[[Exception], None]

# File patterns
FilePattern = Union[str, List[str]]
```

## Configuration Reference

### Global Configuration Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `default_cache_dir` | str | `"~/.tellus/cache"` | Default cache directory |
| `max_parallel_transfers` | int | `4` | Maximum parallel transfers |
| `default_timeout` | int | `300` | Default connection timeout (seconds) |
| `chunk_size` | str | `"10MB"` | Default transfer chunk size |
| `progress_bar_style` | str | `"rich"` | Progress bar style |
| `log_level` | str | `"INFO"` | Default logging level |
| `verify_checksums` | bool | `True` | Verify file checksums |
| `enable_compression` | bool | `False` | Enable transfer compression |

### Location-Specific Configuration

#### SSH Location Options

| Option | Type | Required | Description |
|--------|------|----------|-------------|
| `host` | str | Yes | SSH hostname |
| `username` | str | Yes | SSH username |
| `port` | int | No | SSH port (default: 22) |
| `key_file` | str | No | SSH private key file |
| `password` | str | No | SSH password (interactive if not provided) |
| `timeout` | int | No | Connection timeout |
| `compression` | bool | No | Enable SSH compression |

#### S3 Location Options

| Option | Type | Required | Description |
|--------|------|----------|-------------|
| `bucket` | str | Yes | S3 bucket name |
| `prefix` | str | No | Key prefix |
| `region` | str | No | AWS region |
| `aws_access_key_id` | str | No | AWS access key |
| `aws_secret_access_key` | str | No | AWS secret key |
| `aws_profile` | str | No | AWS profile name |
| `endpoint_url` | str | No | Custom S3 endpoint |

#### Local Location Options

| Option | Type | Required | Description |
|--------|------|----------|-------------|
| `path` | str | Yes | Local filesystem path |
| `create_dirs` | bool | No | Create directories if missing |
| `permissions` | str | No | Default file permissions |

## Development API

### Plugin System

```{eval-rst}
.. autoclass:: Plugin
   :members:
   :undoc-members:
   :show-inheritance:

.. autofunction:: register_plugin

.. autofunction:: load_plugins

.. autofunction:: get_plugins
```

### Testing Utilities

```{eval-rst}
.. autofunction:: create_test_simulation

.. autofunction:: create_mock_location

.. autofunction:: generate_test_data

.. autoclass:: TellusTestCase
   :members:
   :undoc-members:
   :show-inheritance:
```

## Examples

### Basic Usage

```python
import tellus

# Create simulation
sim = tellus.Simulation("my-experiment")

# Add locations
local = sim.add_location("local", type="local", path="./data")
remote = sim.add_location("hpc", type="ssh", host="hpc.edu", path="/scratch")

# Transfer files
remote.get("*.nc", "./data/")
local.put("results.pdf", "analysis/")
```

### Advanced Configuration

```python
# Custom location with all options
location = sim.add_location(
    "advanced-ssh",
    type="ssh",
    host="secure.server.edu",
    port=2222,
    username="researcher",
    key_file="~/.ssh/research_key",
    timeout=600,
    compression=True,
    max_retries=5,
    retry_delay=2.0,
    cache_dir="./cache/",
    cache_expires="1d"
)

# Parallel transfer with progress tracking
def progress_callback(current, total):
    percent = (current / total) * 100
    print(f"Progress: {percent:.1f}%")

location.get(
    ["file1.nc", "file2.nc", "file3.nc"],
    "./downloads/",
    max_workers=4,
    progress_callback=progress_callback,
    verify_checksum=True
)
```

### Error Handling

```python
try:
    location.get("important_file.nc", "./")
except tellus.ConnectionError as e:
    print(f"Connection failed: {e}")
    # Retry with different settings
except tellus.PermissionError as e:
    print(f"Permission denied: {e}")
    # Check credentials
except tellus.TransferError as e:
    print(f"Transfer failed: {e}")
    # Handle partial transfer
```

## Migration Guide

### Upgrading from v1.x to v2.x

Key changes in v2.x:

1. **New Archive System**: Added comprehensive archive management
2. **Enhanced Location Types**: Support for more cloud providers
3. **Improved Configuration**: YAML-based configuration files
4. **Breaking Changes**: Some method signatures have changed

```python
# v1.x syntax
sim = tellus.Simulation("name", config_file="sim.json")
location = sim.add_ssh_location("name", host="server", user="user")

# v2.x syntax  
sim = tellus.Simulation.from_config("sim.yaml")
location = sim.add_location("name", type="ssh", host="server", username="user")
```

See the {doc}`../changelog` for complete migration details.

## See Also

- {doc}`../user-guide/index` - Comprehensive user guide
- {doc}`../examples/index` - Practical examples
- {doc}`../development/index` - Development and contributing guide