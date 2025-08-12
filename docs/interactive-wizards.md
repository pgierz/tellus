# Interactive Wizards Guide

Tellus features comprehensive interactive wizards that transform complex CLI operations into guided, user-friendly workflows. This guide covers all available wizards and their capabilities.

## 🎯 Overview of Wizards

| Wizard | Command | Purpose | Key Features |
|--------|---------|---------|--------------|
| [Simulation Creation](#simulation-creation-wizard) | `tellus simulation create` | Create new simulations | ID generation, path completion, dynamic attributes |
| [Location Creation](#location-creation-wizard) | `tellus location create` | Set up storage locations | Protocol selection, smart configuration, validation |
| [File Browser](#interactive-file-browser) | `tellus simulation location browse` | Navigate and download files | Directory browsing, multi-select, search |
| [Bulk Download](#bulk-download-wizard) | `tellus simulation location mget` | Pattern-based downloads | Pattern suggestions, live preview, batch operations |
| [Export/Import](#exportimport-wizards) | `tellus simulation export/import` | Share configurations | Batch operations, conflict resolution, validation |
| [Template Setup](#template-based-wizard) | `tellus simulation template` | Quick simulation setup | Pre-configured templates, customization |

## 🚀 Simulation Creation Wizard

### Basic Usage

```bash
# Launch the wizard (no arguments needed)
tellus simulation create
```

### Wizard Flow

1. **Simulation ID Input**
   - Enter custom ID or leave empty for auto-generated UUID
   - Validation checks for uniqueness and valid characters

2. **Path Selection**
   - Tab completion for directory navigation
   - Optional - can be skipped for metadata-only simulations

3. **Attribute Collection**
   - Dynamic key-value pair addition
   - Validation for keys and values
   - Continue adding until complete

4. **Confirmation Summary**
   - Review all settings before creation
   - Final confirmation prompt

### Example Session

```
🚀 Simulation Creation Wizard
Let's create a new simulation. Press Ctrl+C to cancel at any time.

Enter simulation ID (leave empty for auto-generated UUID): climate-2024

Enter filesystem path for simulation data (press Tab to complete, Enter to skip): /data/climate/runs

Attributes (optional)
Add key-value attributes to describe your simulation.
? Would you like to add an attribute? Yes
? Attribute key: model
? Value for 'model': CESM2.1
  ✓ Added: model = CESM2.1

? Add another attribute? Yes
? Attribute key: resolution
? Value for 'resolution': f19_g17
  ✓ Added: resolution = f19_g17

? Add another attribute? No

Summary:
  ID: climate-2024
  Path: /data/climate/runs
  Attributes:
    • model: CESM2.1
    • resolution: f19_g17

? Create this simulation? Yes

✅ Created simulation: climate-2024
```

## 📍 Location Creation Wizard

### Basic Usage

```bash
# Launch the location creation wizard
tellus location create
```

### Protocol-Specific Configuration

#### Local Filesystem
- **Path selection**: Tab completion for directory paths
- **Validation**: Checks directory existence and permissions

#### SFTP/SSH
- **Hostname**: Server address or IP
- **Port**: Default 22, customizable
- **Credentials**: Username/password (password masked)
- **Remote path**: Base directory on remote server

#### Cloud Storage (S3, GCS, Azure)
- **Bucket/Container**: Storage bucket name
- **Region**: Cloud region selection
- **Credentials**: Environment variable guidance
- **Path prefixes**: Bucket subdirectory structure

### Example: SFTP Location

```
📍 Location Creation Wizard
Let's create a new storage location. Press Ctrl+C to cancel at any time.

? Enter a name for this location: hpc-cluster

? Select storage protocol: sftp - SSH File Transfer Protocol

? Select location type: disk - DISK

? Is this location optional? No

? Enter hostname: cluster.university.edu
? Enter port (default: 22): 
? Enter username (optional): myuser
? Enter password (optional): [hidden]
? Enter remote base path: /scratch/data

Summary:
  Name: hpc-cluster
  Protocol: sftp
  Kind: disk
  Optional: False
  Configuration:
    • host: cluster.university.edu
    • username: myuser
    • password: *****
    • path: /scratch/data

? Create this location? Yes

✅ Created location: hpc-cluster
```

## 🗂️ Interactive File Browser

### Basic Usage

```bash
# Launch the file browser
tellus simulation location browse
```

### Navigation Features

- **Directory traversal**: Navigate up/down through directory tree
- **Visual indicators**: 📁 for directories, 📄 for files
- **File information**: Shows file sizes where available
- **Action menu**: Download, search, multi-select options

### Available Actions

1. **Single file download**: Select file → specify local name
2. **Multi-select download**: Checkbox selection → batch download
3. **Directory search**: Pattern matching within current directory
4. **Navigate directories**: Enter subdirectories, go up levels

### Example Session

```
🗂️ Interactive File Browser
Browse and download files from simulation locations. Press Ctrl+C to cancel at any time.

? Select a simulation: climate-2024 (2 locations)
? Select a location to browse: hpc-cluster (sftp)

Current path: /scratch/data/climate-2024

? Select an item:
❯ 📁 input/
  📁 output/
  📁 logs/
  📄 run_config.json (1.2KB)
  📄 job_script.sh (856 bytes)
  ⬇️  Download selected files
  🔍 Search in current directory
  ❌ Exit browser

[Navigate to input/ directory...]
Current path: /scratch/data/climate-2024/input

? Select an item:
❯ 📁 ../ (go up)
  📄 forcing_data.nc (2.3GB)
  📄 initial_conditions.nc (500MB)
  📄 boundary_conditions.nc (100MB)
  ⬇️  Download selected files

[Select "Download selected files"...]
? Select files to download:
❯ ◯ forcing_data.nc
  ◉ initial_conditions.nc
  ◉ boundary_conditions.nc

? Enter local directory to save files: ./input_data

✅ Downloaded: ./input_data/initial_conditions.nc
✅ Downloaded: ./input_data/boundary_conditions.nc
```

## 📦 Bulk Download Wizard

### Basic Usage

```bash
# Launch the bulk download wizard
tellus simulation location mget
```

### Pattern Selection

**Pre-defined patterns**:
- `*.txt` - All text files
- `*.nc` - NetCDF files  
- `*.csv` - CSV files
- `*.json` - JSON files
- `*.log` - Log files
- `data*` - Files starting with 'data'
- `*output*` - Files containing 'output'
- Custom pattern - Enter your own

### Live Preview

Before downloading, the wizard shows:
- **Match count**: Number of files matching pattern
- **File list**: First 10 matching files with sizes
- **Total size**: Estimated download size
- **Options**: Recursive, overwrite, destination settings

### Example Session

```
📦 Bulk Download Wizard
Download multiple files with pattern matching. Press Ctrl+C to cancel at any time.

? Select a simulation: climate-2024 (2 locations)
? Select a location: hpc-cluster (sftp)

? Select a file pattern: *.nc - NetCDF files

Previewing files matching '*.nc'...

✅ Found 15 matching files:
  📄 temperature_daily.nc (1.2GB)
  📄 precipitation_daily.nc (890MB)
  📄 wind_speed_daily.nc (750MB)
  📄 sea_ice_monthly.nc (450MB)
  📄 ocean_temp_monthly.nc (680MB)
  📄 surface_flux_monthly.nc (320MB)
  📄 atmosphere_3d_monthly.nc (2.1GB)
  📄 land_carbon_annual.nc (125MB)
  📄 ocean_carbon_annual.nc (180MB)
  📄 ice_sheet_annual.nc (95MB)
  ... and 5 more files

? Download recursively (include subdirectories)? Yes
? Overwrite existing files? No
? Enter local directory to save files (press Tab to complete): ./model_output

Download Summary:
  Pattern: *.nc
  Files found: 15
  Local directory: ./model_output
  Recursive: True
  Overwrite: False

? Proceed with download? Yes

[Progress bars and download status...]
✅ Downloaded 15 files successfully
```

## 📤📥 Export/Import Wizards

### Export Wizard

```bash
# Launch export wizard
tellus simulation export
```

**Export options**:
- **Single simulation**: Export one simulation with all locations and contexts
- **Multiple simulations**: Multi-select from available simulations
- **All simulations**: Complete backup of all configurations

**Export format**: JSON with version info, timestamps, and complete configuration data

### Import Wizard

```bash
# Launch import wizard  
tellus simulation import
```

**Import features**:
- **File validation**: Checks JSON format and version compatibility
- **Conflict detection**: Identifies existing simulations
- **Preview**: Shows what will be imported before proceeding
- **Selective import**: Option to skip conflicting items
- **Location creation**: Creates missing locations automatically

### Example Export Session

```
📤 Simulation Export Wizard
Export simulation configurations for sharing or backup. Press Ctrl+C to cancel at any time.

? What would you like to export?
❯ Single simulation
  Multiple simulations  
  All simulations

? Select a simulation to export: climate-2024 (2 locations) - /data/climate/runs

? Enter export file path (press Tab to complete): climate-2024-config.json

✅ Exported 1 simulation(s) to: climate-2024-config.json
File size: 2,847 bytes
```

### Example Import Session

```
📥 Simulation Import Wizard
Import simulation configurations from exported files. Press Ctrl+C to cancel at any time.

? Enter path to import file (press Tab to complete): shared-config.json

Found 3 simulation(s) to import:
  • climate-2024 - /data/climate/runs (2 locations)
  • ocean-analysis - /data/ocean (1 locations)  
  • land-model - No path (3 locations)

Warning: The following simulations already exist:
  • climate-2024

? Overwrite existing simulations? Yes

? Import 3 simulation(s)? Yes

✅ Imported simulation: ocean-analysis
✅ Imported simulation: land-model  
✅ Imported simulation: climate-2024 (overwritten)

✅ Import completed!
Imported: 3 simulations
Skipped: 0 simulations
```

## 🏗️ Template-based Wizard

### Basic Usage

```bash
# Launch template wizard
tellus simulation template
```

### Available Templates

#### 1. Climate Model Simulation
- **Use case**: Earth System Model simulations
- **Attributes**: model, type, resolution
- **Locations**: input_data, output_data, restart_files
- **Path templates**: `input/{model_id}/{simulation_id}`

#### 2. Data Processing Pipeline  
- **Use case**: Data analysis and processing workflows
- **Attributes**: type, stage
- **Locations**: raw_data, processed_data, results
- **Path templates**: `raw/{project_id}`, `processed/{project_id}`

#### 3. HPC Remote Storage
- **Use case**: High-performance computing workflows
- **Attributes**: compute, storage  
- **Locations**: hpc_scratch, archive_storage
- **Path templates**: `scratch/{username}/{job_id}`

#### 4. Cloud-based Workflow
- **Use case**: Cloud storage for distributed computing
- **Attributes**: platform, scalable
- **Locations**: s3_input, s3_output
- **Path templates**: `s3://bucket/input/{project_id}`

### Template Customization

For each template, you can:
- **Modify attributes**: Change default values or add custom ones
- **Customize locations**: Rename, reconfigure, or skip suggested locations  
- **Adjust path templates**: Modify path prefix patterns
- **Select protocols**: Change storage protocols for locations

### Example Template Session

```
🏗️ Template-based Simulation Wizard
Create simulations from predefined templates. Press Ctrl+C to cancel at any time.

? Select a template:
❯ Climate Model Simulation - Standard setup for Earth System Model simulations
  Data Processing Pipeline - Setup for data analysis and processing workflows
  HPC Remote Storage - High-performance computing with remote storage
  Cloud-based Workflow - Cloud storage for distributed computing

Using template: Climate Model Simulation
Description: Standard setup for Earth System Model simulations

? Enter simulation ID (leave empty for auto-generated UUID): cesm-ssp585-2024

? Enter base filesystem path for simulation (press Tab to complete, Enter to skip): /data/cesm/runs

Template attributes:
? Enter value for 'model': CESM2.1
? Enter value for 'type': climate  
? Enter value for 'resolution': f19_g17

? Add custom attribute? Yes
? Attribute key: experiment
? Value for 'experiment': ssp585

? Add custom attribute? No

Suggested locations for this template:

1. input_data
   Description: Model input files (forcing data, initial conditions)
   Suggested path: input/{model_id}/{simulation_id}
   Protocol: file

? Include 'input_data' location? Yes
? Location name: input_data
? Path prefix template: input/{model_id}/{simulation_id}
? Storage protocol: file - Local filesystem

2. output_data
   Description: Model output files (NetCDF, logs)  
   Suggested path: output/{model_id}/{simulation_id}
   Protocol: file

? Include 'output_data' location? Yes
? Location name: output_data
? Path prefix template: output/{model_id}/{simulation_id}
? Storage protocol: sftp - SSH File Transfer Protocol

3. restart_files
   Description: Model restart/checkpoint files
   Suggested path: restart/{model_id}/{simulation_id}
   Protocol: file

? Include 'restart_files' location? No

Final Summary:
  Simulation ID: cesm-ssp585-2024
  Locations to create: 2
    • input_data (file)
    • output_data (sftp)

? Create simulation with template? Yes

📍 Created location: input_data
📍 Created location: output_data

✅ Created template-based simulation: cesm-ssp585-2024
Template: Climate Model Simulation
Locations: 2
Path: /data/cesm/runs

Next steps:
  1. Configure location credentials if needed
  2. Test connectivity with 'tellus simulation location ls'
  3. Start transferring data with 'tellus simulation location get/mget'
```

## 💡 Tips and Best Practices

### General Wizard Usage

1. **Use Tab completion**: All wizards support tab completion for file paths
2. **Ctrl+C to cancel**: Exit any wizard at any time without changes
3. **Review summaries**: Always check the confirmation summary before proceeding
4. **Start with wizards**: Use interactive mode first, then learn CLI arguments for automation

### Simulation Management

1. **Use descriptive IDs**: Choose meaningful simulation identifiers
2. **Add metadata**: Use attributes to document your simulations thoroughly  
3. **Organize paths**: Use consistent directory structures across simulations
4. **Regular exports**: Export configurations for backup and sharing

### Location Configuration

1. **Test connections**: Verify location access after creation
2. **Use path templates**: Leverage template variables for dynamic paths
3. **Security first**: Use environment variables for credentials when possible
4. **Document protocols**: Add clear names and descriptions for team locations

### File Operations

1. **Preview first**: Use bulk download preview to verify patterns
2. **Start small**: Test with small file sets before large downloads
3. **Use patterns**: Leverage glob patterns for efficient bulk operations
4. **Monitor progress**: Watch progress bars for large transfers

This comprehensive wizard system makes Tellus accessible to users of all experience levels while maintaining the power and flexibility needed for complex data management workflows.