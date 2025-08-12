# Quick Start

This guide will get you up and running with Tellus in just a few minutes. We'll cover the basic concepts and show you how to create your first simulation and storage location using the new **interactive wizards**.

## Basic Concepts

Before we start, let's understand the two main concepts in Tellus:

- **Simulations**: Represent computational experiments or datasets with associated metadata and file locations
- **Locations**: Define storage backends (local disk, remote servers, cloud storage) that can be attached to simulations

## 🚀 Interactive Wizards vs CLI Commands

Tellus now features **interactive wizards** that guide you through complex operations step-by-step. You can use either approach:

- **Interactive wizards**: Run commands without arguments for guided workflows (recommended for beginners)
- **Direct CLI**: Provide all arguments for automation and scripting

## Your First Simulation

### Option 1: Interactive Wizard (Recommended)

```bash
# Launch the simulation creation wizard
tellus simulation create
```

The wizard will guide you through:
1. **Simulation ID**: Enter a custom ID or auto-generate a UUID
2. **Path selection**: Use tab completion to select filesystem paths
3. **Attributes**: Add key-value metadata dynamically
4. **Confirmation**: Review all settings before creation

### Option 2: Direct CLI

```bash
# Create simulation with direct arguments
tellus simulation create my-first-sim --path /data/my-experiment --attr model CESM --attr resolution T42

# View the simulation
tellus simulation show my-first-sim
```

You should see output similar to:

```
┌─ Simulation: my-first-sim ─┐
│ ID: my-first-sim           │
│ Path: /data/my-experiment  │
│ Attributes:                │
│   model: CESM              │
│   resolution: T42          │
└────────────────────────────┘
```

## Adding Storage Locations

### Option 1: Interactive Location Wizard

```bash
# Launch the location creation wizard
tellus location create
```

The wizard provides:
1. **Protocol selection**: Choose from file, SFTP, S3, Google Cloud, Azure, FTP
2. **Smart configuration**: Protocol-specific prompts (credentials, bucket names, etc.)
3. **Path completion**: Tab-based navigation for local paths
4. **Validation**: Conflict detection and input validation

### Option 2: Direct CLI

```bash
# Create a local storage location
tellus location create local-data --protocol file --path /home/user/data

# Create an SFTP location
tellus location create remote-hpc --protocol sftp --host cluster.edu --username myuser --path /scratch/data

# View all locations
tellus location list
```

### Connecting Locations to Simulations

```bash
# Interactive wizard (recommended)
tellus simulation location add

# Or with direct arguments
tellus simulation location add my-first-sim local-data --path-prefix "output/{simulation_id}"
```

## Working with Files

Once you have a simulation with attached locations, you can start working with files:

### 🗂️ Interactive File Browser

```bash
# Launch the interactive file browser
tellus simulation location browse
```

The file browser provides:
- **Directory navigation**: Browse remote file systems with up/down movement
- **Visual indicators**: File/folder icons with size information
- **Multiple actions**: Single downloads, bulk selection, pattern search
- **Live preview**: See files before downloading

### 📦 Bulk Download Wizard

```bash
# Launch the bulk download wizard with pattern matching
tellus simulation location mget
```

Features include:
- **Pattern suggestions**: Common patterns like `*.nc`, `*.txt`, `data*`
- **Live preview**: Shows matching files with counts and sizes before download
- **Custom patterns**: Support for complex glob patterns
- **Download options**: Recursive, overwrite, destination directory selection

### Direct File Operations

```bash
# List files in a location
tellus simulation location ls my-first-sim local-data

# Download a single file
tellus simulation location get my-first-sim local-data "data.txt" ./

# Download multiple files with pattern
tellus simulation location mget my-first-sim local-data "*.nc" ./results/ --recursive
```

## 🏗️ Template-based Quick Setup

For common scenarios, use the template wizard to create pre-configured simulations:

```bash
# Launch the template wizard
tellus simulation template
```

Available templates:
- **Climate Model Simulation**: Standard Earth System Model setup
- **Data Processing Pipeline**: Analysis and processing workflows  
- **HPC Remote Storage**: High-performance computing with remote storage
- **Cloud-based Workflow**: Cloud storage for distributed computing

Each template provides:
- **Pre-configured attributes**: Relevant metadata for the use case
- **Suggested locations**: Appropriate storage locations with path templates
- **Customization options**: Modify templates to fit your specific needs

### Example: Climate Model Template

```bash
# Use climate model template directly
tellus simulation template climate_model
```

This creates a simulation with:
- Input data location: `input/{model_id}/{simulation_id}`
- Output data location: `output/{model_id}/{simulation_id}` 
- Restart files location: `restart/{model_id}/{simulation_id}`

## 📤📥 Sharing Configurations

### Export Simulations

```bash
# Interactive export wizard
tellus simulation export

# Export specific simulation
tellus simulation export my-simulation config.json

# Export all simulations
tellus simulation export --all-simulations backup.json
```

### Import Simulations

```bash
# Interactive import wizard
tellus simulation import

# Import from file
tellus simulation import config.json --overwrite
```

This enables:
- **Team collaboration**: Share simulation setups across team members
- **Environment migration**: Move configurations between development/production
- **Backup/restore**: Save and restore simulation configurations

## Remote Storage Example

Tellus really shines when working with remote storage. Let's set up an SSH location:

```bash
# Interactive wizard (recommended)
tellus location create

# Or create SFTP location directly
tellus location create remote-server \
  --protocol sftp \
  --host example.com \
  --username myuser \
  --path /data/remote

# Add to simulation with context
tellus simulation location add my-first-sim remote-server \
  --path-prefix "/experiments/{simulation_id}"
```

The path prefix uses template variables - `{simulation_id}` will be replaced with `my-first-sim` when accessing files.

## Configuration File

For repeated use, you can create a configuration file at `~/.config/tellus/config.yaml`:

```yaml
default_locations:
  - name: "hpc-cluster"
    protocol: "ssh"
    host: "cluster.university.edu"
    username: "myuser"
    path: "/scratch/data"
    
  - name: "s3-bucket"  
    protocol: "s3"
    bucket: "my-research-data"
    region: "us-east-1"

simulations:
  template_attrs:
    - model_id
    - experiment_name
    - run_date
```

## Next Steps

Now that you have the basics down, explore more advanced features:

- {doc}`user-guide/simulations`: Learn about simulation metadata and organization
- {doc}`user-guide/locations`: Explore different storage backends and authentication
- {doc}`user-guide/workflows`: Integrate with Snakemake and other workflow tools
- {doc}`examples/index`: See real-world examples and use cases

## Common Patterns

### Scientific Workflow Pattern

```bash
# 1. Create simulation for experiment
tellus simulation create climate-run-2024 \
  --attr model_id=CESM2 \
  --attr experiment=ssp585

# 2. Add input data location  
tellus simulation location add climate-run-2024 input-data \
  --path-prefix "/inputs/{{model_id}}/{{experiment}}"

# 3. Add output location
tellus simulation location add climate-run-2024 hpc-output \
  --path-prefix "/scratch/{{model_id}}/{{experiment}}/{{simulation_id}}"

# 4. Download results
tellus simulation location mget climate-run-2024 hpc-output \
  "*.nc" ./results/ --recursive
```

### Data Archive Pattern

```bash
# Archive completed simulation data
tellus location create long-term-storage \
  --protocol s3 \
  --bucket research-archive

tellus simulation location add my-simulation long-term-storage \
  --path-prefix "archive/{{model_id}}/{{year}}/{{simulation_id}}"
```

You're now ready to use Tellus for your data management needs!