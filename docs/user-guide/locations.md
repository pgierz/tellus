# Location Management

Tellus provides powerful location management capabilities that allow you to work with data across multiple storage systems - from local filesystems to HPC clusters, tape archives, and cloud storage.

## Overview

Locations in Tellus represent different storage systems where your Earth System Model data can reside. Each location has:

- **Name**: A unique identifier (e.g., `compute-cluster`, `tape-archive`)
- **Kind**: The type of storage (`COMPUTE`, `DISK`, `TAPE`, `FILESERVER`) 
- **Configuration**: Connection details and protocols
- **Context**: Templates for path generation and data organization

## Location Types

```{list-table} Location Types
:header-rows: 1
:widths: 20 30 50

* - Kind
  - Description
  - Example Use Cases
* - `COMPUTE`
  - High-performance compute nodes
  - Active simulation data, temporary processing
* - `DISK` 
  - Standard disk storage
  - Working datasets, analysis results
* - `TAPE`
  - Long-term archival storage
  - Historical simulations, backup data
* - `FILESERVER`
  - Network file systems
  - Shared datasets, collaborative storage
```

## Command Line Interface

### Creating Locations

Create a new storage location:

```bash
# Create a local disk location
tellus location create local-storage \
    --kind DISK \
    --protocol file \
    --path /data/tellus

# Create an SSH-based compute cluster location
tellus location create hpc-cluster \
    --kind COMPUTE \
    --protocol ssh \
    --host cluster.university.edu \
    --username $USER \
    --path /scratch/$USER

# Create a tape archive location
tellus location create tape-archive \
    --kind TAPE \
    --protocol sftp \
    --host archive.facility.gov \
    --path /archive/project123
```

### Listing and Managing Locations

```bash
# List all configured locations
tellus location list

# Get detailed information about a location
tellus location show hpc-cluster

# Test connectivity to a location
tellus location test tape-archive

# Update location configuration
tellus location update hpc-cluster --path /new/scratch/path

# Remove a location (with confirmation)
tellus location delete old-location
```

### Location Configuration Examples

#### Local Filesystem
```bash
tellus location create local-data \
    --kind DISK \
    --protocol file \
    --path /home/user/earth-science-data \
    --description "Local development data storage"
```

#### SSH/SFTP Remote Server
```bash
tellus location create remote-server \
    --kind FILESERVER \
    --protocol sftp \
    --host data.institution.edu \
    --username researcher \
    --path /shared/climate-data \
    --key-file ~/.ssh/id_rsa
```

#### S3-Compatible Cloud Storage
```bash
tellus location create cloud-storage \
    --kind DISK \
    --protocol s3 \
    --endpoint https://s3.amazonaws.com \
    --bucket earth-model-data \
    --region us-east-1
```

## Working with Simulations and Locations

### Associating Simulations with Locations

```bash
# Create a simulation
tellus simulation create fesom2-historical-001 \
    --model-id FESOM2 \
    --description "Historical FESOM2 simulation"

# Associate the simulation with storage locations
tellus simulation location add fesom2-historical-001 hpc-cluster \
    --context path_prefix="/scratch/fesom2/{simulation_id}" \
    --context work_dir="/tmp/{simulation_id}"

tellus simulation location add fesom2-historical-001 tape-archive \
    --context archive_path="/archive/fesom2/{model_id}/{simulation_id}"

# List locations associated with a simulation
tellus simulation location list fesom2-historical-001

# Remove a location association
tellus simulation location remove fesom2-historical-001 old-location
```

### Context Templates

Location contexts support template variables that are automatically resolved:

```bash
# Available variables in templates:
# {simulation_id} - The simulation identifier  
# {model_id} - The Earth system model name
# {uid} - Unique internal identifier
# Plus any custom attributes from the simulation

# Example context with templates
tellus simulation location add climate-run-2024 hpc-cluster \
    --context data_dir="/scratch/{model_id}/{simulation_id}/data" \
    --context output_dir="/scratch/{model_id}/{simulation_id}/output" \
    --context log_dir="/scratch/{model_id}/{simulation_id}/logs"
```

## Working with Workflows and Locations

### Creating Location-Aware Workflows

```bash
# Create a workflow
tellus workflow create ocean-data-processing \
    --name "Ocean Model Data Processing" \
    --type DATA_PREPROCESSING \
    --description "Process FESOM2 ocean model output"

# Associate workflow with locations
tellus workflow location add ocean-data-processing hpc-cluster \
    --context scratch_dir="/scratch/{workflow_id}" \
    --context tmp_dir="/tmp/{workflow_id}"

tellus workflow location add ocean-data-processing tape-archive \
    --context input_path="/archive/{simulation_id}/raw" \
    --context output_path="/archive/{simulation_id}/processed"
```

### Step-Specific Location Mappings

```bash
# Map workflow steps to specific locations
tellus workflow location map-input ocean-data-processing extract-data tape-archive
tellus workflow location map-output ocean-data-processing extract-data hpc-cluster

tellus workflow location map-input ocean-data-processing process-data hpc-cluster  
tellus workflow location map-output ocean-data-processing process-data local-storage

# View step location mappings
tellus workflow location mappings ocean-data-processing
```

## Data Transfer Operations

### Manual Data Transfer

```bash
# Transfer files between locations
tellus data transfer \
    --from tape-archive:/archive/sim001/data.nc \
    --to hpc-cluster:/scratch/sim001/data.nc \
    --simulation sim001

# Sync directories between locations
tellus data sync \
    --from local-storage:/home/user/results \
    --to tape-archive:/archive/project/results \
    --simulation climate-analysis
```

### Archive Operations

```bash
# Archive simulation data to long-term storage
tellus simulation archive fesom2-historical-001 \
    --location tape-archive \
    --include "output/*.nc" \
    --include "logs/*.log" \
    --exclude "tmp/*" \
    --compression gzip

# Extract archived data for analysis
tellus simulation extract fesom2-historical-001 \
    --location hpc-cluster \
    --pattern "output/temperature_*.nc" \
    --target-dir /scratch/analysis

# List archived files
tellus simulation archive list fesom2-historical-001 --location tape-archive
```

## Location Status and Monitoring

### Health Checks

```bash
# Test all location connectivity
tellus location test-all

# Check location storage usage
tellus location status tape-archive

# Monitor data transfer progress
tellus data status --watch
```

### Troubleshooting

```bash
# Debug connection issues
tellus location test hpc-cluster --verbose --debug

# Check location configuration
tellus location show hpc-cluster --config

# Validate location setup
tellus location validate remote-server
```

## Best Practices

### Location Naming

- Use descriptive names: `hpc-levante`, `tape-dkrz`, `cloud-aws-us-east`
- Include institution or system: `levante-compute`, `mistral-scratch`
- Indicate purpose: `analysis-storage`, `backup-tape`

### Security

```bash
# Use SSH keys instead of passwords
tellus location create secure-cluster \
    --protocol ssh \
    --host cluster.edu \
    --key-file ~/.ssh/tellus_key \
    --no-password

# Set appropriate permissions for configuration files
chmod 600 ~/.config/tellus/locations.json
```

### Performance

- Use local locations for active data processing
- Configure tape locations for long-term archival  
- Set up staging areas for data transfer workflows
- Use compression for network transfers

### Organization

```bash
# Use consistent path templates
--context data_dir="/data/{model_id}/{experiment}/{simulation_id}"
--context work_dir="/work/{username}/{simulation_id}" 
--context archive_path="/archive/{institution}/{project}/{simulation_id}"
```

## Next Steps

- [Simulation Management](simulations.md) - Learn about managing Earth System Model simulations
- [Workflow Integration](workflows.md) - Discover workflow automation capabilities  
- [API Reference](../api/location.md) - Detailed Python API documentation
- [Examples](../examples/remote-data.md) - Complete examples and use cases