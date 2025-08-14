# Tellus Archive System - Usage Summary

## Overview

The Tellus Archive System provides comprehensive archive management for Earth System Model simulations with intelligent file classification, selective extraction, and multi-archive reconstruction capabilities.

## Key Features

- **🔍 Intelligent Classification**: Automatic detection of Earth science file types (NetCDF, GRIB, namelists)
- **📦 Rich Archive Creation**: Compressed tarballs with detailed metadata sidecars
- **🎯 Selective Extraction**: Filter by content type, patterns, dates, and tags
- **🧩 Fragment Assembly**: Reconstruct simulations from multiple archive fragments
- **📅 DateTime Support**: Advanced temporal filtering using strftime patterns
- **🚀 Production Ready**: Atomic operations, progress tracking, error recovery

## Basic Usage

### Creating Archives

```bash
# Create archive from simulation directory
tellus archive create my_simulation_2024 /path/to/simulation --simulation sim_001

# Create selective archive (only output files)
tellus archive create outputs_only /path/to/simulation \
  --simulation sim_001 \
  --content-types output \
  --patterns "*.nc"

# List all archives
tellus archive list

# Show archive details
tellus archive show my_simulation_2024
```

### Extracting Archives

```bash
# Extract complete archive to local directory
tellus archive extract my_simulation_2024 --location localhost --path /tmp/restored

# Selective extraction with patterns
tellus archive extract my_simulation_2024 \
  --location localhost \
  --patterns "*.nc" \
  --content-types output

# DateTime-based extraction
tellus archive extract my_simulation_2024 \
  --location localhost \
  --date-pattern "%Y-%m-%d" \
  --date-range "2024-01-01:2024-03-31"

# Extract to remote location
tellus archive extract my_simulation_2024 \
  --location compute_cluster \
  --path /scratch/restored
```

### Fragment Assembly

```bash
# Assemble simulation from multiple fragments
tellus archive assemble complete_sim_2024 \
  --fragments monthly_jan_2024,monthly_feb_2024,monthly_mar_2024 \
  --location localhost \
  --path /tmp/assembled \
  --conflict-strategy newest_wins

# Temporal assembly (reconstruct by date range)
tellus archive assemble q1_2024 \
  --fragments yearly_2024 \
  --assembly-mode temporal \
  --date-range "2024-01-01:2024-03-31"

# Content-type assembly (combine different content types)
tellus archive assemble full_experiment \
  --fragments inputs_2024,outputs_2024,diagnostics_2024 \
  --assembly-mode content_type
```

## Advanced Features

### Content Classification

The system automatically classifies files into categories:

- **INPUT**: Configuration files, parameters, initial conditions
- **OUTPUT**: Primary model output data (NetCDF, GRIB)
- **LOG**: Log files, diagnostic output
- **INTERMEDIATE**: Restart files, checkpoints
- **CONFIG**: Runtime configuration, namelist files
- **DIAGNOSTIC**: Analysis output, derived quantities
- **METADATA**: Documentation, catalogs, index files

### Importance Levels

Files are classified by importance for selective archiving:

- **CRITICAL**: Essential for simulation integrity (restart files, configurations)
- **IMPORTANT**: Valuable for analysis (primary outputs)
- **OPTIONAL**: Nice to have, can be regenerated (diagnostics)
- **TEMPORARY**: Can be safely discarded (temp files, logs)

### DateTime Filtering

Support for flexible date-based extraction:

```bash
# Extract files from specific date
tellus archive extract sim_2024 --date-pattern "%Y-%m-%d" --date "2024-03-15"

# Extract files from specific month
tellus archive extract sim_2024 --date-pattern "%Y-%m" --date "2024-03"

# Extract files matching pattern in filename
tellus archive extract sim_2024 --patterns "*%Y%m%d*.nc" --date-range "2024-01-01:2024-12-31"

# Complex temporal extraction
tellus archive extract sim_2024 \
  --patterns "output_*.nc" \
  --date-pattern "%Y%m%d" \
  --date-range "20240101:20240331" \
  --content-types "output"
```

### Fragment Management

#### Fragment Types
- **Temporal**: Archives covering specific time periods
- **Content**: Archives containing specific content types
- **Directory**: Archives of specific directory structures
- **Spatial**: Archives covering specific geographic regions

#### Conflict Resolution Strategies
- **newest_wins**: Keep file with latest modification time
- **largest_wins**: Keep file with largest size
- **first_wins**: Keep first extracted file, skip conflicts
- **merge_directories**: Allow directory merging but not file overwriting
- **skip_conflicts**: Skip any files that would conflict
- **interactive**: Prompt user for each conflict

## Archive Metadata

Each archive includes rich metadata stored in sidecar files:

### Sidecar Format (`archive_name.metadata.json`)

```json
{
  "metadata_version": "1.0",
  "generated_at": 1755083471.456814,
  "archive": {
    "archive_id": "my_simulation_2024",
    "location": "localhost",
    "archive_type": "compressed",
    "created_time": 1755083471.456814
  },
  "simulation": {
    "simulation_id": "sim_001",
    "simulation_date": "2024-03-15"
  },
  "fragment": {
    "date_range": "2024-01-01:2024-03-31",
    "content_types": ["output", "diagnostic"],
    "directories": ["output", "analysis"]
  },
  "inventory": {
    "total_files": 150,
    "total_size": 2147483648,
    "content_summary": {
      "output": 120,
      "input": 15,
      "log": 10,
      "config": 5
    },
    "files": [...]
  },
  "extraction": {
    "complexity": "moderate",
    "available_patterns": ["*.nc", "output/*", "*.log"],
    "content_types": ["output", "input", "log", "config"],
    "date_patterns": ["%Y-%m-%d", "%Y%m%d"]
  }
}
```

## Model-Specific Support

The system includes specialized patterns for major Earth System Models:

### CESM (Community Earth System Model)
- CAM atmospheric output (`*cam*`)
- CLM land model output (`*clm*`)
- POP ocean output (`*pop*`)
- CICE sea ice output (`*cice*`)
- User namelists (`user_nl_*`)

### ECHAM
- Surface output (`*BOT*`)
- Atmosphere output (`*ATM*`)
- ECHAM namelists (`namelist.echam`)

### ICON
- Atmosphere output (`*atm_*`)
- Ocean output (`*oce_*`)
- Land output (`*lnd_*`)
- Master namelists (`icon_master.namelist`)

### WRF
- Model output (`wrfout_*`)
- Restart files (`wrfrst_*`)
- Boundary files (`wrfbdy_*`)
- WRF namelists (`namelist.input`)

### FESOM
- Ocean output (`*.fesom.*`)
- Configuration (`namelist.config`)
- Forcing data (`forcing/*`)

## Integration with Storage Locations

The archive system works seamlessly with Tellus location management:

```bash
# List available locations
tellus location list

# Create archive on remote storage
tellus archive create remote_archive /local/simulation \
  --location tape_storage \
  --simulation sim_001

# Extract to different location type
tellus archive extract remote_archive \
  --location compute_cluster \
  --path /scratch/workspace
```

## Performance and Scalability

### Parallel Processing
- Multi-threaded file scanning for large directories
- Parallel compression for improved performance
- Streaming I/O for memory-efficient handling of large files

### Progress Tracking
```bash
# All operations support progress callbacks
tellus archive create large_simulation /huge/dataset \
  --progress \
  --simulation big_sim
```

### Resource Management
- Configurable memory limits for large archives
- Atomic operations with rollback on failure
- Comprehensive error handling and recovery

## Best Practices

### Archive Organization
```bash
# Create temporal fragments for long simulations
tellus archive create sim_2024_q1 /simulation/2024 \
  --patterns "*2024-0[1-3]*" \
  --fragment-info "Q1 2024 outputs"

tellus archive create sim_2024_q2 /simulation/2024 \
  --patterns "*2024-0[4-6]*" \
  --fragment-info "Q2 2024 outputs"
```

### Content-Type Separation
```bash
# Separate critical configs from outputs
tellus archive create configs_2024 /simulation \
  --content-types input,config \
  --importance critical

tellus archive create outputs_2024 /simulation \
  --content-types output \
  --exclude-patterns "*.log"
```

### Storage Optimization
```bash
# Archive only important files to expensive storage
tellus archive create essential_data /simulation \
  --location tape_storage \
  --importance critical,important \
  --exclude-content-types log,temporary
```

## Error Handling and Recovery

The system provides robust error handling:

- **Validation**: Pre-operation checks for space, permissions, conflicts
- **Atomic Operations**: All-or-nothing operations with rollback
- **Partial Success**: Graceful handling of partial failures
- **Detailed Logging**: Comprehensive error messages and warnings
- **Recovery**: Automatic cleanup of temporary files on failure

## Summary

The Tellus Archive System provides a complete solution for Earth System Model data archiving with:

- **Intelligent automation** reducing manual classification effort
- **Flexible extraction** supporting complex scientific workflows  
- **Fragment management** enabling efficient storage strategies
- **Production reliability** with comprehensive error handling
- **Extensive metadata** supporting data discovery and management
- **Model-aware patterns** optimized for Earth science use cases

Whether archiving single experiments or managing complex multi-year simulation campaigns, the system adapts to your needs while maintaining data integrity and providing rich metadata for future access.