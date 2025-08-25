# Infrastructure Adapters

Infrastructure adapters provide concrete implementations for external system integration, including filesystems, workflow engines, and progress tracking systems.

## Filesystem Adapters

### Sandboxed Filesystem

```{eval-rst}
.. currentmodule:: tellus.infrastructure.adapters.sandboxed_filesystem

.. autoclass:: SandboxedFileSystem
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__

.. autoclass:: PathSandboxedFileSystem
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__
```

**Security Features:**
- Path traversal protection
- Access control within sandbox boundaries
- Whitelist-based path validation
- Safe symlink handling
- Resource usage monitoring

### ScoutFS Filesystem Adapter

```{eval-rst}
.. currentmodule:: tellus.infrastructure.adapters.scoutfs_filesystem

.. autoclass:: ScoutFSFileSystem
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__
```

**ScoutFS Features:**
- High-performance parallel filesystem access
- Metadata optimization for scientific workflows
- Large file handling with optimal chunking
- Distributed file operations
- Integration with HPC storage systems

### FSSpec Integration Adapter

```{eval-rst}
.. currentmodule:: tellus.infrastructure.adapters.fsspec_adapter

.. autoclass:: FSSpecAdapter
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__
```

**Supported Protocols:**
- Local filesystem (`file://`)
- SSH/SFTP (`ssh://`, `sftp://`)
- Amazon S3 (`s3://`)
- Google Cloud Storage (`gs://`)
- Azure Blob Storage (`az://`)
- HTTP/HTTPS (`http://`, `https://`)

## Progress Tracking Adapters

### Progress Tracking Adapter

```{eval-rst}
.. currentmodule:: tellus.infrastructure.adapters.progress_tracking

.. autoclass:: ProgressTrackingAdapter
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__

.. autoclass:: ConsoleProgressTracker
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__

.. autoclass:: FileProgressTracker
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__
```

**Progress Tracking Features:**
- Real-time progress updates
- Multiple output formats (console, file, JSON)
- Progress aggregation across multiple operations
- Custom progress callback support
- Integration with Rich progress bars

## Workflow Engine Adapters

### Workflow Engine Adapter

```{eval-rst}
.. currentmodule:: tellus.infrastructure.adapters.workflow_engines

.. autoclass:: WorkflowEngineAdapter
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__

.. autoclass:: SnakemakeAdapter
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__

.. autoclass:: NextflowAdapter
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__

.. autoclass:: CWLAdapter
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__
```

**Supported Workflow Engines:**
- Snakemake - Python-based workflow management
- Nextflow - Data-driven computational pipelines
- Common Workflow Language (CWL) - Portable workflow standard
- Custom workflow executors

## Usage Examples

### Sandboxed Filesystem Operations

```python
from tellus.infrastructure.adapters.sandboxed_filesystem import (
    PathSandboxedFileSystem
)
from pathlib import Path

# Create sandboxed filesystem with restricted access
sandbox_root = Path("/data/climate-sandbox")
sandbox_fs = PathSandboxedFileSystem(
    base_path=sandbox_root,
    auto_mkdir=True,
    writable=True
)

# All operations are restricted to sandbox
try:
    # Safe operation within sandbox
    sandbox_fs.makedirs("experiments/cesm2", exist_ok=True)
    
    # Write file within sandbox
    with sandbox_fs.open("experiments/cesm2/config.yaml", "w") as f:
        f.write("model: CESM2.1\nexperiment: historical\n")
    
    # List contents
    files = sandbox_fs.ls("experiments/cesm2/")
    print(f"Files in sandbox: {files}")
    
    # This would raise SecurityError - path traversal attempt
    # sandbox_fs.open("../../../etc/passwd", "r")
    
except SecurityError as e:
    print(f"Security violation prevented: {e}")
```

### Multi-Protocol File Operations

```python
from tellus.infrastructure.adapters.fsspec_adapter import FSSpecAdapter

# Initialize adapter for different protocols
adapter = FSSpecAdapter()

# Local filesystem operations
local_fs = adapter.get_filesystem("file")
local_files = local_fs.ls("/data/local/")

# SSH/SFTP operations  
ssh_fs = adapter.get_filesystem("ssh", {
    "host": "hpc.university.edu",
    "username": "researcher",
    "key_filename": "~/.ssh/hpc_key"
})

# List remote files
remote_files = ssh_fs.ls("/scratch/researcher/")

# S3 operations
s3_fs = adapter.get_filesystem("s3", {
    "aws_access_key_id": "ACCESS_KEY",
    "aws_secret_access_key": "SECRET_KEY",
    "region_name": "us-west-2"
})

# List S3 bucket contents
s3_objects = s3_fs.ls("s3://climate-data-archive/cesm2/")

# Copy between protocols
with local_fs.open("/data/local/analysis.nc", "rb") as src:
    with s3_fs.open("s3://climate-data-archive/results/analysis.nc", "wb") as dst:
        dst.write(src.read())

print("File copied from local to S3")
```

### Progress Tracking Integration

```python
from tellus.infrastructure.adapters.progress_tracking import (
    ConsoleProgressTracker, FileProgressTracker
)
from tellus.domain.entities.progress_tracking import ProgressOperation
import time

# Create progress trackers
console_tracker = ConsoleProgressTracker(
    use_rich=True,
    show_rate=True,
    show_eta=True
)

file_tracker = FileProgressTracker(
    log_file="/tmp/progress.log",
    json_output=True
)

# Simulate long-running operation with progress
operation = ProgressOperation(
    operation_id="file-processing-001",
    operation_type="data_processing", 
    description="Processing climate data files"
)

# Start tracking
console_tracker.start_operation(operation)
file_tracker.start_operation(operation)

# Simulate progress updates
total_files = 100
for current_file in range(total_files):
    # Update progress
    progress_data = {
        "current_step": current_file,
        "total_steps": total_files,
        "status_message": f"Processing file {current_file + 1}/{total_files}",
        "metadata": {
            "current_file": f"data_{current_file:03d}.nc",
            "processing_rate": "2.5 files/sec"
        }
    }
    
    console_tracker.update_progress(operation.operation_id, progress_data)
    file_tracker.update_progress(operation.operation_id, progress_data)
    
    # Simulate processing time
    time.sleep(0.1)

# Complete operation
result_data = {
    "files_processed": total_files,
    "total_time_seconds": 10.0,
    "average_rate": "10 files/sec"
}

console_tracker.complete_operation(operation.operation_id, result_data)
file_tracker.complete_operation(operation.operation_id, result_data)
```

### Snakemake Workflow Integration

```python
from tellus.infrastructure.adapters.workflow_engines import SnakemakeAdapter
from tellus.domain.entities.workflow import WorkflowDefinition, WorkflowStep

# Create Snakemake adapter
snakemake_adapter = SnakemakeAdapter(
    working_directory="/data/workflows",
    conda_env_path="/opt/conda/envs/climate"
)

# Define climate data processing workflow
workflow_steps = [
    WorkflowStep(
        name="download_data",
        description="Download CMIP6 data from ESGF",
        command="python download_cmip6.py {params.model} {params.experiment}",
        inputs=[],
        outputs=["raw_data/{model}_{experiment}.nc"],
        params={
            "model": "CESM2",
            "experiment": "historical"
        }
    ),
    
    WorkflowStep(
        name="regrid_data", 
        description="Regrid data to common grid",
        command="cdo remapbil,target_grid.txt {input} {output}",
        inputs=["raw_data/{model}_{experiment}.nc"],
        outputs=["processed_data/{model}_{experiment}_regridded.nc"],
        dependencies=["download_data"]
    ),
    
    WorkflowStep(
        name="calculate_climatology",
        description="Calculate climatological means",
        command="cdo timmean {input} {output}",
        inputs=["processed_data/{model}_{experiment}_regridded.nc"],
        outputs=["climatology/{model}_{experiment}_climatology.nc"],
        dependencies=["regrid_data"]
    )
]

# Create workflow definition
workflow = WorkflowDefinition(
    name="cmip6_processing",
    description="CMIP6 data processing pipeline",
    steps=workflow_steps,
    metadata={
        "model": "CESM2",
        "experiment": "historical",
        "variables": ["tas", "pr"]
    }
)

# Execute workflow
execution_result = snakemake_adapter.execute_workflow(
    workflow,
    target_rule="calculate_climatology",
    cores=4,
    dryrun=False
)

if execution_result.success:
    print(f"Workflow completed successfully")
    print(f"Output files: {execution_result.output_files}")
else:
    print(f"Workflow failed: {execution_result.error_message}")
```

### Nextflow Pipeline Integration

```python
from tellus.infrastructure.adapters.workflow_engines import NextflowAdapter
from pathlib import Path

# Create Nextflow adapter
nextflow_adapter = NextflowAdapter(
    work_dir="/tmp/nextflow-work",
    conda_enabled=True
)

# Define Nextflow pipeline script
pipeline_script = """
#!/usr/bin/env nextflow

params.input_dir = "/data/cesm2/raw"
params.output_dir = "/data/cesm2/processed"
params.variables = ["tas", "pr", "psl"]

process EXTRACT_VARIABLE {
    conda 'cdo nco'
    
    input:
    path input_file
    each variable from params.variables
    
    output:
    path "${variable}_${input_file.baseName}.nc"
    
    script:
    """
    cdo selvar,${variable} ${input_file} ${variable}_${input_file.baseName}.nc
    """
}

process CALCULATE_STATS {
    conda 'cdo nco'
    
    input:
    path input_file
    
    output:
    path "*_stats.nc"
    
    script:
    """
    cdo timstd ${input_file} ${input_file.baseName}_timstd.nc
    cdo timmean ${input_file} ${input_file.baseName}_timmean.nc
    """
}

workflow {
    input_files = Channel.fromPath("${params.input_dir}/*.nc")
    
    EXTRACT_VARIABLE(input_files)
    CALCULATE_STATS(EXTRACT_VARIABLE.out)
    
    CALCULATE_STATS.out.view { "Generated: $it" }
}
"""

# Save pipeline script
pipeline_path = Path("/tmp/climate_analysis.nf")
pipeline_path.write_text(pipeline_script)

# Execute Nextflow pipeline
execution_result = nextflow_adapter.execute_pipeline(
    script_path=str(pipeline_path),
    params={
        "input_dir": "/data/cesm2/raw",
        "output_dir": "/data/cesm2/processed",
        "variables": ["tas", "pr", "psl"]
    },
    profile="conda",
    resume=True
)

if execution_result.success:
    print("Nextflow pipeline completed successfully")
    print(f"Execution time: {execution_result.execution_time}")
else:
    print(f"Pipeline failed: {execution_result.error_message}")
```

### ScoutFS High-Performance Operations

```python
from tellus.infrastructure.adapters.scoutfs_filesystem import ScoutFSFileSystem
import asyncio

# Initialize ScoutFS filesystem for large-scale operations
scoutfs = ScoutFSFileSystem(
    mount_point="/mnt/scoutfs-climate",
    enable_parallel_io=True,
    chunk_size_mb=64,
    max_concurrent_ops=16
)

async def process_large_dataset():
    """Process large climate dataset using ScoutFS."""
    
    # List files in parallel
    file_pattern = "/mnt/scoutfs-climate/cesm2/output/*.nc"
    files = await scoutfs.glob_async(file_pattern)
    
    print(f"Found {len(files)} NetCDF files")
    
    # Process files in parallel batches
    batch_size = 10
    for i in range(0, len(files), batch_size):
        batch = files[i:i + batch_size]
        
        # Create processing tasks
        tasks = []
        for file_path in batch:
            task = scoutfs.process_file_async(
                file_path, 
                operation="checksum",
                algorithm="sha256"
            )
            tasks.append(task)
        
        # Execute batch in parallel
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Process results
        for file_path, result in zip(batch, results):
            if isinstance(result, Exception):
                print(f"Error processing {file_path}: {result}")
            else:
                print(f"Processed {file_path}: {result['checksum'][:16]}...")

# Run async processing
asyncio.run(process_large_dataset())
```

### Custom Progress Callback System

```python
from tellus.infrastructure.adapters.progress_tracking import ProgressTrackingAdapter
from typing import Callable, Dict, Any

class CustomProgressAdapter(ProgressTrackingAdapter):
    """Custom progress adapter with webhook notifications."""
    
    def __init__(self, webhook_url: str = None):
        super().__init__()
        self.webhook_url = webhook_url
        self.callbacks: Dict[str, Callable] = {}
    
    def register_callback(self, operation_id: str, callback: Callable):
        """Register custom callback for operation."""
        self.callbacks[operation_id] = callback
    
    def update_progress(self, operation_id: str, progress_data: Dict[str, Any]):
        """Update progress with custom notifications."""
        
        # Standard progress update
        super().update_progress(operation_id, progress_data)
        
        # Custom callback
        if operation_id in self.callbacks:
            try:
                self.callbacks[operation_id](operation_id, progress_data)
            except Exception as e:
                print(f"Callback error for {operation_id}: {e}")
        
        # Webhook notification for major milestones
        progress_percent = progress_data.get("progress_percentage", 0)
        if progress_percent % 25 == 0 and self.webhook_url:
            self._send_webhook_notification(operation_id, progress_data)
    
    def _send_webhook_notification(self, operation_id: str, progress_data: Dict[str, Any]):
        """Send webhook notification."""
        import requests
        
        try:
            payload = {
                "operation_id": operation_id,
                "progress": progress_data,
                "timestamp": datetime.now().isoformat()
            }
            
            response = requests.post(
                self.webhook_url,
                json=payload,
                timeout=5
            )
            response.raise_for_status()
            
        except Exception as e:
            print(f"Webhook notification failed: {e}")

# Usage with custom callbacks
progress_adapter = CustomProgressAdapter(
    webhook_url="https://api.example.com/webhooks/progress"
)

def custom_progress_callback(operation_id: str, progress_data: Dict[str, Any]):
    """Custom progress callback function."""
    progress = progress_data.get("progress_percentage", 0)
    
    if progress >= 50:
        print(f"🎉 Operation {operation_id} is halfway complete!")
    
    if progress >= 100:
        print(f"✅ Operation {operation_id} finished!")

# Register callback
progress_adapter.register_callback("data-processing-001", custom_progress_callback)

# Use adapter for tracking
operation = ProgressOperation(
    operation_id="data-processing-001",
    operation_type="data_processing",
    description="Processing climate model output"
)

progress_adapter.start_operation(operation)

# Progress updates trigger callbacks and webhooks
progress_adapter.update_progress("data-processing-001", {
    "progress_percentage": 25,
    "status_message": "Processing temperature data"
})

progress_adapter.update_progress("data-processing-001", {
    "progress_percentage": 50, 
    "status_message": "Processing precipitation data"
})

progress_adapter.update_progress("data-processing-001", {
    "progress_percentage": 100,
    "status_message": "Processing complete"
})
```

## Security Considerations

Infrastructure adapters implement several security measures:

### Path Traversal Protection

```python
from tellus.infrastructure.adapters.sandboxed_filesystem import SecurityError

# Path validation prevents traversal attacks
try:
    # These operations would be blocked
    sandbox_fs.open("../../../etc/passwd", "r")  # Path traversal
    sandbox_fs.makedirs("/tmp/malicious", exist_ok=True)  # Absolute path escape
    
except SecurityError as e:
    print(f"Security violation prevented: {e}")
```

### Resource Usage Monitoring

```python
# Filesystem adapters monitor resource usage
fs_stats = scoutfs.get_usage_stats()
print(f"Operations performed: {fs_stats['operation_count']}")
print(f"Data transferred: {fs_stats['bytes_transferred']}")
print(f"Average operation time: {fs_stats['avg_operation_time']}s")

# Resource limits can be configured
scoutfs.set_resource_limits(
    max_operations_per_minute=1000,
    max_concurrent_operations=20,
    max_memory_usage_gb=8
)
```

### Access Control Integration

```python
# Filesystem adapters support access control
fs.set_access_policy("read_only_policy", {
    "allowed_paths": ["/data/public/*"],
    "denied_paths": ["/data/private/*"],
    "allowed_operations": ["read", "list"],
    "denied_operations": ["write", "delete"]
})

# Apply policy to operations
fs.apply_policy("read_only_policy")
```

## Performance Monitoring

Adapters provide built-in performance monitoring:

```python
# Enable performance monitoring
adapter.enable_performance_monitoring(
    metrics_interval=30,  # seconds
    log_slow_operations=True,
    slow_operation_threshold=5.0  # seconds
)

# Access performance metrics
metrics = adapter.get_performance_metrics()
print(f"Average operation latency: {metrics['avg_latency']}ms")
print(f"Throughput: {metrics['operations_per_second']} ops/sec")
print(f"Error rate: {metrics['error_rate']}%")

# Export metrics for external monitoring
adapter.export_metrics_to_prometheus("/metrics")
adapter.export_metrics_to_json("/tmp/adapter_metrics.json")
```