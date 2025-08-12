# Tellus CLI Cookbook

A comprehensive command-line reference for climate scientists working with Earth System Model data. This cookbook provides practical, step-by-step workflows for managing distributed climate data using Tellus.

## Quick Reference

### Essential Commands
```bash
# List all available commands
pixi run tellus --help

# Simulation management
pixi run tellus simulation list              # List all simulations
pixi run tellus simulation create           # Interactive simulation wizard
pixi run tellus simulation show <sim_id>    # Show simulation details

# Location management  
pixi run tellus location list               # List all locations
pixi run tellus location create             # Interactive location wizard
pixi run tellus location show <name>        # Show location details

# Data operations
pixi run tellus simulation location get <sim_id> <location> <file> <local_path>
pixi run tellus simulation location mget <sim_id> <location> <pattern>
pixi run tellus simulation location ls <sim_id> <location>
pixi run tellus simulation location browse  # Interactive file browser
```

### Common Flags
```bash
--force, -f          # Force overwrite existing files
--recursive, -r      # Process directories recursively  
--detail, -l         # Show detailed file information
--override           # Override existing configurations
```

### Quick Status Check
```bash
# Check system status
pixi run tellus simulation list
pixi run tellus location list

# Test location connectivity
pixi run tellus simulation location ls <sim_id> <location>
```

---

## Getting Started Scenarios

### 1. First-Time Setup

**Scenario**: Setting up Tellus for a new climate modeling project.

```bash
# Initialize a new simulation for CESM2 experiment
pixi run tellus simulation create

# Follow the interactive wizard:
# - Simulation ID: cesm2_historical_001
# - Path: /work/climate/cesm2/historical/run001
# - Attributes: 
#   - model: CESM2
#   - experiment: historical
#   - resolution: f19_g17
#   - run_id: 001
```

**Expected Output**:
```
✅ Created simulation: cesm2_historical_001
Path: /work/climate/cesm2/historical/run001
Attributes: model=CESM2, experiment=historical, resolution=f19_g17, run_id=001
```

### 2. Configure Storage Locations

**Scenario**: Setting up local, HPC, and archive storage locations.

```bash
# Create local scratch location
pixi run tellus location create

# Interactive wizard for local storage:
# - Name: local_scratch
# - Protocol: file
# - Kind: disk  
# - Path: /scratch/username/cesm_data
```

```bash
# Create HPC remote location
pixi run tellus location create

# Interactive wizard for HPC:
# - Name: hpc_storage
# - Protocol: sftp
# - Kind: compute
# - Host: cluster.university.edu
# - Username: your_username
# - Path: /home/username/cesm_runs
```

**Pro Tip**: Store credentials in environment variables:
```bash
export TELLUS_HPC_PASSWORD="your_password"
export TELLUS_S3_ACCESS_KEY="your_key"
```

### 3. Add Locations to Simulation

**Scenario**: Link storage locations to your simulation with context-aware paths.

```bash
# Add local location with template path
pixi run tellus simulation location add cesm2_historical_001 local_scratch \
  --path-prefix "/scratch/username/{model}/{experiment}/{run_id}"

# Add HPC location  
pixi run tellus simulation location add cesm2_historical_001 hpc_storage \
  --path-prefix "/home/username/runs/{model}/{experiment}"

# Add archive location
pixi run tellus simulation location add cesm2_historical_001 tape_archive \
  --path-prefix "/archive/climate/{model}/{experiment}/{run_id}"
```

**Expected Output**:
```
✅ Added location to simulation cesm2_historical_001: local_scratch
Path Prefix: /scratch/username/{model}/{experiment}/{run_id}
Resolved Path: /scratch/username/CESM2/historical/001
```

---

## Data Management Workflows

### 1. Downloading CMIP6 Data

**Scenario**: Download CMIP6 model output from a remote data portal.

```bash
# Create simulation for CMIP6 analysis
pixi run tellus simulation create --attr model CESM2 --attr experiment historical \
  --attr variable tas --attr frequency monthly

# Add ESGF data portal location
pixi run tellus location create
# - Name: esgf_portal
# - Protocol: https  
# - Host: esgf-node.llnl.gov

# Browse available files interactively
pixi run tellus simulation location browse cmip6_analysis esgf_portal

# Download specific variable files
pixi run tellus simulation location mget cmip6_analysis esgf_portal \
  "tas_Amon_CESM2_historical_*.nc" ./data/
```

### 2. Organizing Model Output

**Scenario**: Organizing large model runs with multiple output streams.

```bash
# Create simulation with comprehensive metadata
pixi run tellus simulation create cesm2_long_run \
  --path /work/cesm2/long_control \
  --attr model CESM2 \
  --attr experiment control \
  --attr years "0001-0500" \
  --attr components "atm,ocn,ice,lnd"

# Add locations for different output streams
pixi run tellus simulation location add cesm2_long_run archive_atm \
  --path-prefix "/archive/{model}/{experiment}/atm/hist"

pixi run tellus simulation location add cesm2_long_run archive_ocn \
  --path-prefix "/archive/{model}/{experiment}/ocn/hist" 

# List organized output files
pixi run tellus simulation location ls cesm2_long_run archive_atm --recursive
```

**Expected File Organization**:
```
/archive/CESM2/control/atm/hist/
├── cesm2.control.cam.h0.0001-01.nc
├── cesm2.control.cam.h0.0001-02.nc
└── cesm2.control.cam.h1.0001-01.nc
```

### 3. Archive Creation and Extraction

**Scenario**: Creating compressed archives for long-term storage.

```bash
# Create archive from simulation data
pixi run tellus simulation archive create cesm2_historical_001 \
  --include-location local_scratch \
  --output cesm2_historical_001_complete.tar.gz \
  --compression gzip

# Extract archive to new location  
pixi run tellus simulation archive extract cesm2_historical_001_complete.tar.gz \
  --destination /restore/cesm2_data \
  --create-locations

# Verify extracted data
pixi run tellus simulation location ls restored_simulation local_restore
```

### 4. Data Transfer Between Locations

**Scenario**: Moving data from HPC scratch to long-term archive.

```bash
# Bulk transfer with pattern matching
pixi run tellus simulation location mget cesm2_historical_001 hpc_scratch \
  "*.cam.h0.*.nc" /archive/transfer/ --recursive

# Transfer specific time periods
pixi run tellus simulation location mget cesm2_historical_001 hpc_scratch \
  "*185[0-9]-*.nc" ./nineteenth_century/ 

# Monitor transfer progress (shown automatically)
```

**Expected Progress Output**:
```
Downloading files... ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 15/20 files
✅ Downloaded: cesm2.control.cam.h0.1850-01.nc (2.3 GB)
✅ Downloaded: cesm2.control.cam.h0.1850-02.nc (2.1 GB)
```

---

## Interactive Wizards

### 1. Simulation Creation Wizard

**When to Use**: First time creating a simulation or when you need guided setup.

```bash
pixi run tellus simulation create
```

**Wizard Flow**:
1. **Simulation ID**: Enter unique identifier or leave blank for UUID
2. **Filesystem Path**: Base directory for simulation data (tab completion available)
3. **Attributes**: Key-value metadata (model, experiment, resolution, etc.)
4. **Confirmation**: Review summary before creation

**Tips**:
- Use descriptive simulation IDs: `model_experiment_resolution_run`
- Store paths consistently: `/work/{project}/{model}/{experiment}`
- Add comprehensive attributes for searchability

### 2. Location Configuration Wizard

```bash
pixi run tellus location create
```

**Protocol-Specific Configuration**:

**Local Filesystem** (`file`):
```
✓ Name: local_data
✓ Protocol: file  
✓ Base path: /data/climate (tab completion)
✓ Optional: No
```

**SSH/SFTP** (`sftp`):
```
✓ Name: hpc_cluster
✓ Protocol: sftp
✓ Host: login.hpc.university.edu
✓ Port: 22 (default)  
✓ Username: your_username
✓ Password: [hidden]
✓ Remote path: /home/username/climate
```

**Amazon S3** (`s3`):
```
✓ Name: aws_archive
✓ Protocol: s3
✓ Bucket: climate-model-archive
Note: Configure AWS credentials via environment or ~/.aws/
```

### 3. File Browser Wizard

**Most Interactive Experience**:
```bash
pixi run tellus simulation location browse
```

**Navigation**:
- `📁 directory/` - Enter directory
- `📄 filename.nc` - Download single file  
- `📁 .. (go up)` - Return to parent directory
- `⬇️ Download selected files` - Multi-select download
- `🔍 Search in current directory` - Pattern search

**Search Examples**:
- `*.nc` - All NetCDF files
- `*2020*.nc` - Files containing "2020"
- `tas_*.nc` - Temperature files
- `**/*.log` - All log files recursively

---

## Advanced Scenarios

### 1. Batch Operations and Scripting

**Scenario**: Processing multiple simulations programmatically.

```bash
# Create multiple related simulations
for run in {001..010}; do
  pixi run tellus simulation create "cesm2_ensemble_${run}" \
    --path "/work/ensemble/run${run}" \
    --attr model CESM2 \
    --attr experiment historical \
    --attr ensemble_member ${run}
done

# Batch add locations
for sim in $(pixi run tellus simulation list --format ids); do
  pixi run tellus simulation location add ${sim} hpc_archive \
    --path-prefix "/archive/cesm2/{experiment}/{simulation_id}"
done

# Batch download with error handling
#!/bin/bash
SIMS=(cesm2_ensemble_001 cesm2_ensemble_002 cesm2_ensemble_003)
for sim in "${SIMS[@]}"; do
  echo "Processing ${sim}..."
  if ! pixi run tellus simulation location mget ${sim} hpc_archive "*.nc" ./output/${sim}/; then
    echo "Error downloading ${sim}, continuing..."
  fi
done
```

### 2. HPC Job Scheduler Integration

**SLURM Script Example**:
```bash
#!/bin/bash
#SBATCH --job-name=tellus_download
#SBATCH --time=04:00:00  
#SBATCH --nodes=1
#SBATCH --ntasks=1

# Load environment
module load pixi
cd /work/climate/tellus_project

# Download large dataset in parallel
pixi run tellus simulation location mget cesm2_long_run tape_archive \
  "*.cam.h0.*.nc" /scratch/username/downloads/ \
  --recursive --force

# Create archive after download
pixi run tellus simulation archive create cesm2_long_run \
  --include-location local_scratch \
  --output cesm2_complete_${SLURM_JOB_ID}.tar.gz
```

**PBS Script Example**:
```bash
#!/bin/bash
#PBS -N tellus_transfer
#PBS -l walltime=06:00:00
#PBS -l nodes=1:ppn=1

cd $PBS_O_WORKDIR
source activate tellus_env

# Transfer multiple datasets
DATASETS=("historical" "rcp85" "control")
for dataset in "${DATASETS[@]}"; do
  pixi run tellus simulation location mget "cesm2_${dataset}" remote_archive \
    "*.nc" "/scratch/climate/${dataset}/" --recursive
done
```

### 3. Monitoring Long-Running Operations

**Progress Tracking**:
All file operations show real-time progress automatically:

```
Downloading files... ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 47/100 files
Current: cesm2.control.cam.h0.1987-03.nc (2.1 GB) 
Rate: 45.2 MB/s | ETA: 00:23:45
```

**Background Monitoring**:
```bash
# Run in background with logging
pixi run tellus simulation location mget large_simulation tape_archive \
  "*.nc" ./large_download/ > download_log.txt 2>&1 &

# Monitor progress
tail -f download_log.txt

# Check status  
jobs
fg %1  # bring to foreground
```

### 4. Error Recovery and Debugging

**Common Recovery Scenarios**:

```bash
# Resume interrupted download
pixi run tellus simulation location mget cesm2_historical tape_archive \
  "*.nc" ./downloads/ --force  # overwrites partial files

# Test connectivity before large transfers
pixi run tellus simulation location ls cesm2_historical remote_hpc

# Validate downloaded files
pixi run tellus simulation location ls cesm2_historical local_scratch --detail
```

---

## Troubleshooting Section

### Common Error Messages

**`Simulation with ID 'xyz' not found`**
```bash
# Check existing simulations
pixi run tellus simulation list

# Create missing simulation
pixi run tellus simulation create xyz
```

**`Location 'name' not found in simulation`**  
```bash
# List simulation locations
pixi run tellus simulation location list <sim_id>

# Add missing location
pixi run tellus simulation location add <sim_id> <location_name>
```

**`Permission denied` or `Authentication failed`**
```bash
# Check location configuration
pixi run tellus location show <location_name>

# Update credentials
pixi run tellus location update <location_name> --username new_user --password

# Test connection
pixi run tellus simulation location ls <sim_id> <location_name>
```

### Network Connectivity Issues

**Timeout errors**:
```bash
# Test basic connectivity
ping hostname.edu

# Check specific ports  
telnet hostname.edu 22    # SSH/SFTP
telnet hostname.edu 21    # FTP

# Verify credentials
ssh username@hostname.edu
```

**DNS resolution problems**:
```bash
# Test name resolution
nslookup hostname.edu
dig hostname.edu

# Use IP address temporarily
pixi run tellus location update remote_site --host 192.168.1.100
```

### Performance Optimization

**Slow transfers**:
```bash
# Use pattern matching to reduce file count
pixi run tellus simulation location mget sim remote "*.h0.*.nc"  # monthly only
# Instead of: "*.nc"  # all frequencies

# Transfer in smaller batches
pixi run tellus simulation location mget sim remote "*200[0-5]*.nc" ./batch1/
pixi run tellus simulation location mget sim remote "*200[6-9]*.nc" ./batch2/
```

**Large file handling**:
```bash
# Check available disk space first
df -h .

# Use selective downloads
pixi run tellus simulation location browse  # interactive selection
# Instead of blind bulk downloads
```

**Memory usage with large directories**:
```bash
# Avoid recursive listing of huge directories
pixi run tellus simulation location ls sim remote --recursive  # slow
pixi run tellus simulation location ls sim remote subdir/     # faster
```

### Permission and Authentication Problems

**SSH key authentication**:
```bash
# Set up SSH keys (more secure than passwords)
ssh-keygen -t rsa -b 4096
ssh-copy-id username@hostname.edu

# Test key-based login
ssh username@hostname.edu

# Remove password from location config
pixi run tellus location update remote_hpc --remove-option password
```

**File permission errors**:
```bash
# Check remote permissions
pixi run tellus simulation location ls sim remote --detail

# Fix local permissions
chmod 755 ./download_directory/
```

**S3 authentication**:
```bash
# Configure AWS credentials
aws configure
# Or use environment variables:
export AWS_ACCESS_KEY_ID="your_key"
export AWS_SECRET_ACCESS_KEY="your_secret"
export AWS_DEFAULT_REGION="us-west-2"
```

---

## Integration Examples

### Working with Jupyter Notebooks

```python
# In Jupyter notebook
import subprocess
import json

# Get simulation data programmatically
result = subprocess.run(['pixi', 'run', 'tellus', 'simulation', 'show', 'my_sim'], 
                       capture_output=True, text=True)

# Download data for analysis  
subprocess.run(['pixi', 'run', 'tellus', 'simulation', 'location', 'get', 
               'my_sim', 'remote', 'temperature_data.nc', './data/'])

# Now analyze with xarray
import xarray as xr
ds = xr.open_dataset('./data/temperature_data.nc')
```

### Integration with Data Analysis Pipelines

```bash
# Snakemake workflow integration
rule download_forcing_data:
    output: "data/forcing/{simulation}_forcing.nc"  
    shell: """
    pixi run tellus simulation location get {wildcards.simulation} \
      forcing_data "forcing.nc" {output}
    """

rule download_output_data:
    output: "data/output/{simulation}_monthly.nc"
    shell: """  
    pixi run tellus simulation location mget {wildcards.simulation} \
      model_output "*.h0.*.nc" data/output/
    """
```

### Environment Setup Scripts

```bash
#!/bin/bash
# setup_tellus_env.sh - Project initialization script

echo "Setting up Tellus environment for climate project..."

# Create project simulations
pixi run tellus simulation create control_run --attr experiment control
pixi run tellus simulation create warming_run --attr experiment 4xCO2  

# Add standard locations
for sim in control_run warming_run; do
  pixi run tellus simulation location add ${sim} hpc_data \
    --path-prefix "/work/climate/{simulation_id}"
  pixi run tellus simulation location add ${sim} analysis_results \
    --path-prefix "/results/{experiment}"  
done

echo "Environment setup complete!"
echo "Available simulations:"
pixi run tellus simulation list
```

---

## Tips and Best Practices

### Naming Conventions
- **Simulations**: `{model}_{experiment}_{resolution}_{run}`
  - Example: `cesm2_historical_f19_001`
- **Locations**: `{type}_{purpose}` 
  - Example: `hpc_scratch`, `tape_archive`, `local_analysis`

### Path Templates
Use consistent templating for reproducible organization:
```bash
# Good templates
--path-prefix "/work/{model}/{experiment}/{run_id}"
--path-prefix "/archive/{project}/{model}/{experiment}"
--path-prefix "{base_path}/{component}/{frequency}"

# Avoid hardcoded paths  
--path-prefix "/work/cesm2/historical/001"  # inflexible
```

### Metadata Strategy
Add comprehensive attributes for discoverability:
```bash
--attr model CESM2 \
--attr component atmosphere \  
--attr experiment historical \
--attr resolution f19_g17 \
--attr time_period "1850-2014" \
--attr ensemble_member r1i1p1f1 \
--attr institution NCAR
```

### Backup and Version Control
```bash
# Export configurations for backup
pixi run tellus simulation export my_important_sim backup/sim_config.json

# Version control simulation definitions
git add simulations.json locations.json
git commit -m "Add CESM2 historical simulation configuration"

# Restore from backup
pixi run tellus simulation import backup/sim_config.json
```

This cookbook provides comprehensive coverage of Tellus CLI usage for climate scientists. Each section builds from basic concepts to advanced workflows, with practical examples you'll encounter in real research scenarios. Keep this reference handy as you develop your climate data management workflows!