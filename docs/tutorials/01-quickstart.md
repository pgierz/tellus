# Tellus Quickstart Tutorial

## Introduction

Welcome to Tellus, the comprehensive data management system for Earth System Model research! This quickstart guide will get you up and running with Tellus in 15 minutes, covering the essential concepts and basic operations you need to manage your climate simulation data effectively.

## What You'll Learn

By the end of this tutorial, you'll be able to:
- Set up your first Tellus workspace
- Create and manage storage locations
- Register and track climate simulations
- Perform basic file operations and transfers
- Monitor data processing workflows

## Prerequisites

- Python 3.9+ installed
- Basic familiarity with command-line interfaces
- Access to climate model data (or use our sample data)

## Installation

Install Tellus using pixi (recommended) or pip:

```bash
# Using pixi (recommended for reproducible environments)
curl -fsSL https://pixi.sh/install.sh | bash
cd your-project-directory
pixi add tellus

# Or using pip
pip install tellus
```

## Step 1: Initialize Your Workspace

Let's start by creating your first Tellus workspace and understanding the basic concepts.

```python
# Import the main Tellus components
from tellus.application.container import ServiceContainer
from tellus.application.dtos import CreateLocationDto, CreateSimulationDto
from tellus.domain.entities.location import LocationKind

# Initialize the Tellus service container
container = ServiceContainer()
location_service = container.get_location_service()
simulation_service = container.get_simulation_service()

print("🌍 Welcome to Tellus!")
print("Your Earth System Model data management system is ready.")
```

## Step 2: Create Your First Storage Location

Storage locations in Tellus represent where your data is stored - this could be your local filesystem, a remote server, or cloud storage.

```python
# Create a local workspace location
local_workspace = CreateLocationDto(
    name="my-local-workspace",
    kinds=[LocationKind.DISK],
    protocol="file",
    path="/home/username/climate-data",  # Update this path for your system
    description="My local workspace for climate data analysis",
    metadata={
        "purpose": "development_and_analysis",
        "capacity_gb": 500,
        "backup": False
    }
)

# Register the location with Tellus
location = location_service.create_location(local_workspace)
print(f"✅ Created location: {location.name}")
print(f"   Path: {location.path}")
print(f"   Purpose: {location.metadata.get('purpose', 'general')}")
```

Let's also create a location for archived data:

```python
# Create an archive location (this could be a remote server or cloud storage)
archive_location = CreateLocationDto(
    name="climate-archive",
    kinds=[LocationKind.FILESERVER],
    protocol="file",  # In practice, this might be "ssh" or "s3"
    path="/archive/climate-data",  # Update for your archive system
    description="Long-term archive for processed climate data",
    metadata={
        "purpose": "long_term_storage",
        "retention_years": 10,
        "compressed": True
    }
)

archive = location_service.create_location(archive_location)
print(f"✅ Created archive: {archive.name}")

# List all your locations
locations = location_service.list_locations()
print(f"\n📁 Your storage locations ({len(locations.locations)}):")
for loc in locations.locations:
    print(f"   • {loc.name}: {loc.path}")
```

## Step 3: Register Your First Climate Simulation

Now let's register a climate simulation with Tellus. This creates a record that tracks metadata, file locations, and processing history.

```python
# Register a CESM2 historical simulation
cesm_simulation = CreateSimulationDto(
    simulation_id="cesm2-historical-tutorial",
    model_id="CESM2.1",
    attrs={
        # Basic simulation information
        "experiment": "historical",
        "time_period": "1850-2014",
        "resolution": "f09_g17",  # ~1 degree atmosphere, ~1 degree ocean
        "ensemble_member": "r1i1p1f1",
        
        # Scientific configuration
        "atmospheric_model": "CAM6",
        "ocean_model": "POP2",
        "land_model": "CLM5",
        "sea_ice_model": "CICE5",
        
        # Data characteristics
        "output_frequency": "monthly",
        "variables": ["tas", "pr", "psl", "tos"],  # Temperature, precipitation, pressure, SST
        "file_format": "netcdf4",
        "compression": "lz4",
        
        # Metadata for tracking
        "institution": "NCAR",
        "contact": "your.email@institution.edu",
        "status": "completed",
        "creation_date": "2024-01-15"
    }
)

# Register the simulation
simulation = simulation_service.create_simulation(cesm_simulation)
print(f"🌡️  Registered simulation: {simulation.simulation_id}")
print(f"   Model: {simulation.attrs['atmospheric_model']} + {simulation.attrs['ocean_model']}")
print(f"   Experiment: {simulation.attrs['experiment']} ({simulation.attrs['time_period']})")
print(f"   Status: {simulation.attrs['status']}")
```

## Step 4: Associate Data with Your Simulation

Link your simulation data to storage locations so Tellus knows where to find your files.

```python
from tellus.application.dtos import SimulationLocationAssociationDto

# Associate the simulation with your local workspace
association = SimulationLocationAssociationDto(
    simulation_id=simulation.simulation_id,
    location_names=["my-local-workspace"],
    context_overrides={
        "my-local-workspace": {
            "path_prefix": "/home/username/climate-data/cesm2-historical",
            "data_structure": "component_separated",
            "file_pattern": "*.nc",
            "processing_stage": "analysis_ready"
        }
    }
)

# Create the association
simulation_service.associate_simulation_with_locations(association)
print(f"🔗 Associated simulation with local workspace")
print(f"   Data path: {association.context_overrides['my-local-workspace']['path_prefix']}")
```

## Step 5: Basic File Operations

Let's demonstrate basic file transfer operations between locations.

```python
from tellus.application.dtos import FileTransferOperationDto

# Get the file transfer service
transfer_service = container.get_file_transfer_service()

# Example: Transfer a processed file to archive
# Note: In a real scenario, make sure these files exist
transfer_operation = FileTransferOperationDto(
    source_location="my-local-workspace",
    source_path="/home/username/climate-data/cesm2-historical/processed_tas_annual_mean.nc",
    dest_location="climate-archive", 
    dest_path="/archive/climate-data/cesm2-historical/tas_annual_mean.nc",
    verify_checksum=True,  # Ensure data integrity
    overwrite=False,  # Don't overwrite existing files
    metadata={
        "transfer_purpose": "archival",
        "data_type": "processed_analysis",
        "simulation_id": simulation.simulation_id
    }
)

# Note: Commented out as files may not exist in tutorial
# result = await transfer_service.transfer_file(transfer_operation)
print("📦 File transfer configured (would transfer processed data to archive)")
print(f"   Source: {transfer_operation.source_location}:{transfer_operation.source_path}")
print(f"   Destination: {transfer_operation.dest_location}:{transfer_operation.dest_path}")
print(f"   Checksum verification: {transfer_operation.verify_checksum}")
```

## Step 6: Monitor Your Data and Operations

Tellus provides tools to monitor your data and track operations.

```python
# List all your registered simulations
all_simulations = simulation_service.list_simulations()
print(f"\n🔬 Your registered simulations ({len(all_simulations.simulations)}):")
for sim in all_simulations.simulations:
    status = sim.attrs.get('status', 'unknown')
    model = sim.attrs.get('atmospheric_model', sim.model_id)
    experiment = sim.attrs.get('experiment', 'unknown')
    print(f"   • {sim.simulation_id}")
    print(f"     Model: {model}, Experiment: {experiment}, Status: {status}")

# Check location usage and status
print(f"\n💾 Storage location status:")
for loc in locations.locations:
    purpose = loc.metadata.get('purpose', 'general')
    capacity = loc.metadata.get('capacity_gb', 'unknown')
    print(f"   • {loc.name}: {purpose} ({capacity} GB capacity)")
```

## Step 7: Working with Archives

Create an archive to bundle and compress your simulation data.

```python
from tellus.application.dtos import CreateArchiveDto

# Get the archive service
archive_service = container.get_archive_service()

# Create an archive for your processed data
archive_metadata = CreateArchiveDto(
    archive_id="cesm2-tutorial-processed-v1",
    location_name="climate-archive",
    archive_type="compressed",  # Creates a compressed tar archive
    simulation_id=simulation.simulation_id,
    version="1.0",
    description="Processed CESM2 tutorial data - annual means and trends",
    tags={"tutorial", "cesm2", "processed", "v1"}
)

# Create the archive metadata (actual archiving would require real files)
archive = archive_service.create_archive_metadata(archive_metadata)
print(f"📚 Created archive: {archive.archive_id}")
print(f"   Location: {archive.location}")
print(f"   Type: {archive.archive_type}")
print(f"   Version: {archive.version}")
print(f"   Tags: {', '.join(archive.tags)}")
```

## Step 8: Quick Data Discovery

Find your data quickly using Tellus's discovery features.

```python
# Search for simulations by attributes
historical_sims = [
    sim for sim in all_simulations.simulations 
    if sim.attrs.get('experiment') == 'historical'
]
print(f"\n🔍 Found {len(historical_sims)} historical simulations:")
for sim in historical_sims:
    period = sim.attrs.get('time_period', 'unknown period')
    print(f"   • {sim.simulation_id} ({period})")

# List archives
all_archives = archive_service.list_archives()
print(f"\n📦 Your archives ({len(all_archives.archives)}):")
for archive in all_archives.archives:
    print(f"   • {archive.archive_id} (v{archive.version})")
    if archive.description:
        print(f"     {archive.description}")
```

## Next Steps

Congratulations! You've completed the Tellus quickstart tutorial. You now know how to:

✅ **Set up locations** - Configure storage locations for your data  
✅ **Register simulations** - Track your climate model runs with rich metadata  
✅ **Associate data** - Link simulations with their storage locations  
✅ **Transfer files** - Move data between locations safely  
✅ **Create archives** - Bundle and compress data for long-term storage  
✅ **Discover data** - Find your simulations and archives quickly

## What's Next?

Continue your Tellus journey with these tutorials:

1. **[Storage Setup Guide](02-storage-setup.md)** - Learn to configure complex storage hierarchies including HPC systems, cloud storage, and tape archives
2. **[Automation Workflows](03-automation-workflows.md)** - Set up automated data processing pipelines and quality control
3. **[Advanced Interfaces](04-advanced-interfaces.md)** - Master the TUI, advanced CLI features, and integration with other tools

## Common Next Steps

### For Individual Researchers
- Set up automated backups to institutional storage
- Create workflows for routine data processing
- Integrate with your analysis notebooks and scripts

### For Research Groups
- Configure shared storage locations for collaboration
- Set up automated quality control and validation
- Create standardized simulation metadata templates

### For Data Centers
- Configure hierarchical storage management
- Set up automated archival and retrieval workflows
- Implement monitoring and alerting systems

## Getting Help

- **Documentation**: Full documentation at `docs/`
- **Examples**: More examples in `docs/user-stories/`
- **Issues**: Report bugs and request features on GitHub
- **Community**: Join the Earth System Model data management community

Happy data managing! 🌍📊