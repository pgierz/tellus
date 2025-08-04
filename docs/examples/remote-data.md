# Remote Data Access

This example demonstrates how to access data from remote locations including SSH servers, cloud storage, and other remote backends using Tellus.

```{note}
This example shows various remote data access patterns. For a complete interactive notebook version, see the planned `remote-data.ipynb` file.
```

## Overview

Tellus provides unified access to remote data sources through its location system. You can:

- Connect to SSH/SFTP servers with authentication
- Access cloud storage (S3, Google Cloud Storage, Azure)
- Download files with progress tracking and resumable transfers
- Cache remote data locally for efficient access

## SSH/SFTP Access

### Basic Setup

```python
import tellus

# Create simulation and add SSH location
sim = tellus.Simulation("my-experiment")
location = sim.add_location(
    "remote-server",
    type="ssh",
    host="data.example.com",
    username="myuser",
    path="/data/experiments"
)
```

### Authentication Options

```python
# SSH key authentication (recommended)
location = sim.add_location(
    "secure-server", 
    type="ssh",
    host="secure.example.com",
    username="scientist",
    key_file="~/.ssh/id_rsa",
    path="/scratch/data"
)

# Password authentication (interactive prompt)
location = sim.add_location(
    "password-server",
    type="ssh", 
    host="old-server.edu",
    username="myuser",
    # password will be prompted securely
    path="/home/myuser/data"
)
```

### File Operations

```python
# List remote files
files = location.list("*.nc")
print(f"Found {len(files)} NetCDF files")

# Download with progress tracking
location.get("model_output_*.nc", "./local_data/")

# Download specific files
location.get(["results.dat", "metadata.json"], "./downloads/")

# Resumable downloads for large files
location.get("large_dataset.tar.gz", "./", resume=True)
```

## Cloud Storage Access

### AWS S3

```python
# S3 access with credentials
s3_location = sim.add_location(
    "s3-bucket",
    type="s3",
    bucket="my-data-bucket",
    prefix="experiments/exp001/",
    aws_access_key_id="YOUR_ACCESS_KEY",
    aws_secret_access_key="YOUR_SECRET_KEY"
)

# Download from S3
s3_location.get("*.nc", "./s3_data/")
```

### Google Cloud Storage

```python
# GCS access
gcs_location = sim.add_location(
    "gcs-bucket",
    type="gcs", 
    bucket="research-data",
    prefix="climate-models/",
    credentials_file="path/to/service-account.json"
)

# List and download
files = gcs_location.list("temperature_*.nc")
gcs_location.get(files[:5], "./gcs_data/")
```

## Advanced Features

### Progress Tracking

All remote operations show detailed progress information:

```python
# Progress is shown automatically
location.get("large_file.nc", "./")
# Output:
# Downloading large_file.nc: 45%|████▌     | 450MB/1.0GB [00:30<00:40, 15.2MB/s]
```

### Parallel Downloads

```python
# Download multiple files in parallel
location.get(
    ["file1.nc", "file2.nc", "file3.nc"], 
    "./parallel_data/",
    max_workers=4
)
```

### Caching

```python
# Enable local caching for repeated access
location = sim.add_location(
    "cached-remote",
    type="ssh",
    host="data.server.com", 
    username="user",
    path="/data",
    cache_dir="./remote_cache/",
    cache_expires="1d"  # Cache for 1 day
)

# First access downloads and caches
data = location.get("dataset.nc", cache=True)

# Subsequent access uses cache
data = location.get("dataset.nc", cache=True)  # Fast!
```

### Error Handling and Retries

```python
# Robust downloads with retries
try:
    location.get(
        "unstable_connection_file.nc",
        "./",
        max_retries=3,
        retry_delay=5  # seconds
    )
except tellus.TransferError as e:
    print(f"Download failed after retries: {e}")
```

## Integration Examples

### With Jupyter Notebooks

```python
# Perfect for interactive data exploration
import tellus
import xarray as xr

# Connect to remote data
sim = tellus.Simulation("analysis")
remote = sim.add_location("hpc-storage", type="ssh", ...)

# Download and analyze in one workflow
remote.get("*.nc", "./temp_data/")
ds = xr.open_mfdataset("./temp_data/*.nc")
ds.plot()
```

### With Scripts

```python
#!/usr/bin/env python3
"""Download and process remote climate data."""

import tellus
from pathlib import Path

def main():
    # Setup
    sim = tellus.Simulation("climate-analysis")
    remote = sim.add_location("climate-server", type="ssh", ...)
    
    # Download latest data
    output_dir = Path("./climate_data")
    output_dir.mkdir(exist_ok=True)
    
    files = remote.list("temperature_*_latest.nc") 
    remote.get(files, output_dir)
    
    print(f"Downloaded {len(files)} files to {output_dir}")

if __name__ == "__main__":
    main()
```

## Best Practices

1. **Use SSH keys** instead of passwords for automated workflows
2. **Enable caching** for frequently accessed remote data
3. **Use glob patterns** to download multiple related files efficiently
4. **Set appropriate timeouts** for unstable connections
5. **Monitor transfer progress** for large datasets
6. **Handle errors gracefully** with retries and fallbacks

## Troubleshooting

### Connection Issues
```python
# Test connection before large transfers
try:
    location.test_connection()
    print("Connection successful!")
except tellus.ConnectionError as e:
    print(f"Connection failed: {e}")
```

### Permission Problems
```python
# Check available files and permissions
try:
    files = location.list("*", show_hidden=True)
    print(f"Accessible files: {len(files)}")
except tellus.PermissionError as e:
    print(f"Permission denied: {e}")
```

### Network Timeouts
```python
# Adjust timeouts for slow connections  
location = sim.add_location(
    "slow-server",
    type="ssh",
    host="slow.server.edu",
    timeout=300,  # 5 minutes
    connect_timeout=60  # 1 minute connection timeout
)
```

## Next Steps

- Explore {doc}`workflow-integration` to use remote data in automated pipelines
- See {doc}`../user-guide/index` for comprehensive location management
- Check {doc}`../api/index` for complete API reference