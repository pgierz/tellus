# User Guide

Welcome to the comprehensive Tellus user guide. This section provides detailed documentation on all aspects of using Tellus for distributed data management in scientific computing workflows.

## Core Concepts

Tellus is built around several key concepts that work together to provide a unified data management experience:

### Simulations

A **Simulation** is the top-level organizational unit in Tellus. It represents a computational experiment, research project, or data collection effort. Simulations contain metadata and manage multiple data locations.

```python
import tellus

# Create a new simulation
sim = tellus.Simulation("climate-experiment-2024")

# Load an existing simulation
sim = tellus.Simulation.load("climate-experiment-2024")
```

### Locations

**Locations** represent different storage backends where your data can live. Each location has a type (local, SSH, S3, etc.) and specific configuration for accessing that storage.

```python
# Add various location types
local_storage = sim.add_location("local-data", type="local", path="/data/local")
remote_server = sim.add_location("hpc-storage", type="ssh", host="hpc.edu", path="/scratch")
cloud_storage = sim.add_location("s3-backup", type="s3", bucket="research-data")
```

### File Operations

Tellus provides consistent file operations across all location types:

- `list()` - List files and directories
- `get()` - Download/copy files to local storage
- `put()` - Upload/copy files to remote storage  
- `exists()` - Check if files exist
- `remove()` - Delete files

```python
# List files matching a pattern
files = remote_server.list("*.nc")

# Download files with progress tracking
remote_server.get("model_output_*.nc", "./local_data/")

# Upload results
cloud_storage.put("./results/analysis.pdf", "experiments/exp-001/")
```

## Working with Simulations

### Creating and Managing Simulations

```python
# Create with metadata
sim = tellus.Simulation(
    "ocean-circulation-study",
    description="Global ocean circulation analysis 2024",
    tags=["oceanography", "climate", "modeling"],
    metadata={
        "model": "MOM6",
        "resolution": "0.25deg",
        "start_date": "2024-01-01"
    }
)

# Save simulation configuration
sim.save()

# List all simulations
available_sims = tellus.list_simulations()
```

### Simulation Metadata

```python
# Add and update metadata
sim.metadata["grid_size"] = "1440x1080"
sim.tags.append("high-resolution")

# Access metadata
print(f"Model type: {sim.metadata['model']}")
print(f"Tags: {', '.join(sim.tags)}")
```

### Simulation Templates

```python
# Create reusable simulation templates
template = tellus.SimulationTemplate(
    "climate-model-template",
    locations=[
        {"name": "input-data", "type": "ssh", "host": "data.server.edu"},
        {"name": "results", "type": "s3", "bucket": "climate-results"},
        {"name": "scratch", "type": "local", "path": "./temp"}
    ],
    metadata_schema={
        "model_version": str,
        "experiment_id": str,
        "start_date": str
    }
)

# Create simulation from template
sim = template.create_simulation(
    "climate-exp-001",
    model_version="v2.1",
    experiment_id="CMIP6-001",
    start_date="2024-01-01"
)
```

## Location Types and Configuration

### Local Storage

```python
local = sim.add_location(
    "local-workspace",
    type="local",
    path="/home/user/data",
    create_dirs=True  # Create directories if they don't exist
)
```

### SSH/SFTP Storage

```python
# Basic SSH configuration
ssh_server = sim.add_location(
    "compute-cluster",
    type="ssh",
    host="hpc.university.edu",
    username="researcher",
    path="/scratch/data"
)

# Advanced SSH configuration
ssh_secure = sim.add_location(
    "secure-server",
    type="ssh",
    host="secure.gov.server",
    port=2222,
    username="scientist",
    key_file="~/.ssh/gov_id_rsa",
    passphrase_file="~/.ssh/passphrase",
    path="/classified/data",
    timeout=300,
    compression=True
)
```

### Cloud Storage

#### Amazon S3

```python
s3_location = sim.add_location(
    "aws-storage",
    type="s3",
    bucket="research-datasets",
    prefix="climate/experiments/",
    region="us-west-2",
    aws_access_key_id="ACCESS_KEY",
    aws_secret_access_key="SECRET_KEY",
    # Or use AWS profiles
    aws_profile="research"
)
```

#### Google Cloud Storage

```python
gcs_location = sim.add_location(
    "google-storage", 
    type="gcs",
    bucket="climate-research-data",
    prefix="models/",
    credentials_file="/path/to/service-account.json"
)
```

#### Azure Blob Storage

```python
azure_location = sim.add_location(
    "azure-storage",
    type="azure",
    account_name="researchdata",
    container="climate-models",
    account_key="ACCOUNT_KEY"
)
```

### Custom Storage Backends

```python
# Register custom storage backend
@tellus.register_location_type("custom")
class CustomLocation(tellus.BaseLocation):
    def __init__(self, **config):
        super().__init__(**config)
        # Custom initialization
    
    def list(self, pattern="*"):
        # Custom list implementation
        pass
    
    def get(self, files, destination):
        # Custom download implementation  
        pass

# Use custom location
custom = sim.add_location("my-custom", type="custom", custom_param="value")
```

## File Operations

### Listing Files

```python
# Basic listing
files = location.list()

# Pattern matching
nc_files = location.list("*.nc")
temp_files = location.list("temperature_*.nc")

# Recursive listing
all_files = location.list("**/*", recursive=True)

# Get detailed file information
file_info = location.list_detailed("*.nc")
for info in file_info:
    print(f"{info.path}: {info.size} bytes, modified {info.mtime}")
```

### Downloading Files

```python
# Download single file
location.get("data.nc", "./local_data/")

# Download multiple files
location.get(["file1.nc", "file2.nc"], "./downloads/")

# Download with patterns
location.get("temperature_*.nc", "./climate_data/")

# Download with renaming
location.get("remote_file.nc", "./local_file.nc")

# Parallel downloads
location.get(file_list, "./data/", max_workers=4)

# Resume interrupted downloads
location.get("large_file.nc", "./", resume=True)
```

### Uploading Files

```python
# Upload single file
location.put("./local_data.nc", "remote_data.nc")

# Upload directory
location.put("./results/", "experiment_001/")

# Upload with compression
location.put("./large_file.nc", "compressed.nc.gz", compress=True)

# Upload with progress tracking
def progress_callback(bytes_sent, total_bytes):
    percent = (bytes_sent / total_bytes) * 100
    print(f"Upload progress: {percent:.1f}%")

location.put("file.nc", "remote.nc", progress_callback=progress_callback)
```

### File Synchronization

```python
# Sync directories
location.sync("./local_dir/", "remote_dir/", 
              delete=True,      # Delete remote files not in local
              dry_run=False,    # Actually perform sync
              checksum=True)    # Verify with checksums

# Bidirectional sync
location.bidirectional_sync("./local/", "remote/")
```

## Advanced Features

### Caching

```python
# Enable automatic caching
cached_location = sim.add_location(
    "cached-remote",
    type="ssh",
    host="slow.server.edu",
    path="/data",
    cache_dir="./cache/",
    cache_expires="1d",      # Cache expires after 1 day
    cache_size_limit="10GB"  # Limit cache size
)

# Manual cache management
cached_location.cache.clear()
cached_location.cache.info()
```

### Progress Tracking

```python
from rich.progress import Progress

with Progress() as progress:
    task = progress.add_task("Downloading...", total=100)
    
    def update_progress(current, total):
        percent = (current / total) * 100
        progress.update(task, completed=percent)
    
    location.get("large_file.nc", "./", progress_callback=update_progress)
```

### Error Handling and Retries

```python
# Configure retry behavior
robust_location = sim.add_location(
    "unreliable-server",
    type="ssh",
    host="unreliable.server.edu",
    max_retries=5,
    retry_delay=2.0,        # Seconds between retries
    backoff_factor=2.0,     # Exponential backoff
    timeout=300
)

# Handle specific errors
try:
    location.get("important_file.nc", "./")
except tellus.ConnectionError:
    print("Cannot connect to server")
except tellus.PermissionError:
    print("Access denied")
except tellus.TransferError as e:
    print(f"Transfer failed: {e}")
```

### Parallel Operations

```python
# Configure parallel transfers
location.get(
    file_list,
    "./downloads/",
    max_workers=8,           # Number of parallel transfers
    chunk_size="50MB",       # Size of transfer chunks
    connection_pool_size=4   # SSH connection pool size
)
```

## Configuration Management

### Configuration Files

```python
# Load from configuration file
sim = tellus.Simulation.from_config("simulation_config.yaml")
```

```yaml
# simulation_config.yaml
name: "climate-experiment"
description: "Climate model analysis"
tags: ["climate", "ocean", "atmosphere"]

metadata:
  model: "CESM"
  version: "2.1"
  resolution: "1deg"

locations:
  - name: "input-data"
    type: "ssh"
    host: "data.server.edu"
    username: "researcher"
    path: "/data/input"
    key_file: "~/.ssh/research_key"
  
  - name: "results"
    type: "s3"
    bucket: "climate-results"
    prefix: "experiments/"
    aws_profile: "research"
  
  - name: "scratch"
    type: "local"
    path: "./workspace"
    create_dirs: true
```

### Environment Variables

```bash
# Set global configuration via environment
export TELLUS_DEFAULT_CACHE_DIR="./cache"
export TELLUS_SSH_KEY_FILE="~/.ssh/id_rsa"
export TELLUS_MAX_PARALLEL_TRANSFERS="4"
export TELLUS_DEFAULT_TIMEOUT="300"
```

### Global Settings

```python
# Configure global defaults
tellus.configure(
    default_cache_dir="./cache",
    max_parallel_transfers=4,
    default_timeout=300,
    progress_bar_style="rich"
)
```

## Integration Patterns

### With Jupyter Notebooks

```python
# Notebook-friendly progress display
%load_ext tellus_magic

# Magic command for quick operations
%tellus_get remote-server "*.nc" ./data/

# Interactive file browser
tellus.browse_location("remote-server")
```

### With Command Line Tools

```bash
# CLI integration
tellus simulation create climate-exp --type climate-model
tellus location add climate-exp hpc-data --type ssh --host hpc.edu
tellus get climate-exp hpc-data "*.nc" ./data/
```

### With Workflow Engines

```python
# Snakemake integration
def get_input_files(wildcards):
    location = SIM.get_location("input-data")
    return location.list(f"*{wildcards.year}.nc")

rule download_data:
    output: "data/{year}.nc"
    run:
        INPUT_LOCATION.get(f"{wildcards.year}.nc", output[0])
```

## Best Practices

### 1. Organization

- Use descriptive simulation names
- Group related locations logically
- Tag simulations for easy filtering
- Document metadata thoroughly

### 2. Security

- Use SSH keys instead of passwords
- Store credentials in secure configuration files
- Use appropriate file permissions
- Enable connection encryption

### 3. Performance

- Use parallel transfers for multiple files
- Enable caching for frequently accessed data
- Configure appropriate timeouts
- Monitor transfer progress for large datasets

### 4. Reliability

- Enable retry logic for unstable connections
- Use checksums to verify transfers
- Implement proper error handling
- Keep backups of important data

### 5. Maintenance

- Regularly clean up cached data
- Monitor storage usage
- Update credentials periodically
- Keep configurations under version control

## Troubleshooting

### Common Issues

#### Connection Problems
```python
# Test connectivity
location.test_connection()

# Check credentials
location.verify_credentials()

# Debug connection details
tellus.set_log_level("DEBUG")
```

#### Permission Errors
```python
# Check file permissions
info = location.list_detailed("problematic_file.nc")
print(f"Permissions: {info[0].permissions}")

# Test write access
location.test_write_access()
```

#### Transfer Failures
```python
# Enable detailed logging
import logging
logging.getLogger("tellus").setLevel(logging.DEBUG)

# Use verbose error reporting
try:
    location.get("file.nc", "./")
except Exception as e:
    tellus.diagnose_error(e)
```

#### Performance Issues
```python
# Profile transfer performance
with tellus.profile_transfers():
    location.get(large_file_list, "./")

# Monitor resource usage
tellus.monitor_transfers(location)
```

## Next Steps

- Explore {doc}`../examples/index` for practical use cases
- See {doc}`../api/index` for complete API reference
- Check {doc}`../development/index` for extending Tellus
- Review {doc}`../interactive-wizards` for guided workflows