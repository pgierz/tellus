# Tellus Python SDK Documentation

The Tellus Python SDK provides domain entities and services for working with climate model simulations programmatically. This guide covers installation, core concepts, API reference, and best practices for integrating Tellus into your scientific workflows.

## Table of Contents

1. [Installation](#installation)
2. [Core Concepts](#core-concepts)
3. [Quick Start](#quick-start)
4. [Core Entities](#core-entities)
5. [Application Services](#application-services)
6. [Working with Simulations](#working-with-simulations)
7. [Managing Storage Locations](#managing-storage-locations)
8. [File Management](#file-management)
9. [Advanced Topics](#advanced-topics)
10. [Best Practices](#best-practices)
11. [Error Handling](#error-handling)
12. [API Reference](#api-reference)

---

## Installation

### Requirements

- Python 3.9+ (HPC compatibility until Python 3.9 EOL in October 2025)
- SSH access for remote storage locations (optional)

### Install with pip

```bash
pip install tellus
```

### Install for Development

```bash
git clone https://github.com/pgierz/tellus.git
cd tellus
pip install -e .
```

### Install with pixi (Recommended)

```bash
pixi install
```

---

## Core Concepts

### Domain-Driven Design Architecture

Tellus follows clean architecture principles with clear separation of concerns:

- **Domain Entities**: Pure business logic (simulations, locations, files)
- **Application Services**: Orchestrate use cases and business workflows
- **Infrastructure**: Implementation details (storage protocols, databases)
- **Interfaces**: External-facing APIs (CLI, REST API, Python SDK)

### Key Abstractions

1. **Simulation**: Represents a climate model run with metadata, provenance, and file tracking
2. **Location**: Multi-protocol storage abstraction (local, SSH, SFTP, cloud, tape)
3. **SimulationFile**: Semantic file classification with content types and importance levels
4. **Service Container**: Dependency injection for managing application services

---

## Quick Start

```python
from tellus import (
    Simulation,
    Location,
    LocationKind,
    SimulationService,
    LocationService,
    get_service_container
)

# Initialize service container
container = get_service_container()
service_factory = container.service_factory

# Get application services
sim_service = service_factory.get_simulation_service()
loc_service = service_factory.get_location_service()

# Create a storage location
from tellus.application.dtos import CreateLocationDto

location_dto = CreateLocationDto(
    name="levante_scratch",
    kinds=["COMPUTE", "DISK"],
    protocol="sftp",
    path="/scratch/users/username/experiments",
    storage_options={
        "host": "levante.dkrz.de",
        "username": "username",
        "key_filename": "/home/username/.ssh/id_rsa"
    }
)

location = loc_service.create_location(location_dto)
print(f"Created location: {location.name}")

# Create a simulation
from tellus.application.dtos import CreateSimulationDto

sim_dto = CreateSimulationDto(
    simulation_id="CMIP6_historical_r1i1p1f1",
    model_id="AWI-CM-1-1-MR",
    attrs={
        "experiment": "historical",
        "domain": "Arctic",
        "resolution": "T127_CORE2"
    }
)

simulation = sim_service.create_simulation(sim_dto)
print(f"Created simulation: {simulation.simulation_id}")

# Associate simulation with location
from tellus.application.dtos import SimulationLocationAssociationDto

assoc_dto = SimulationLocationAssociationDto(
    simulation_id="CMIP6_historical_r1i1p1f1",
    location_names=["levante_scratch"],
    context_overrides={
        "levante_scratch": {
            "path_prefix": "{model}/{experiment}"
        }
    }
)

sim_service.associate_locations(assoc_dto)
print("Simulation associated with location")
```

---

## Core Entities

### Simulation Entity

The `Simulation` (alias for `SimulationEntity`) represents a climate model run with rich metadata and file management capabilities.

#### Creating Simulations

```python
from tellus import Simulation

# Create with minimal information
sim = Simulation(
    simulation_id="arctic_run_001",
    model_id="FESOM2"
)

# Create with detailed attributes
sim = Simulation(
    simulation_id="CMIP6_historical_r1i1p1f1",
    model_id="AWI-CM-1-1-MR",
    path="/path/to/simulation",
    attrs={
        "experiment": "historical",
        "ensemble_member": "r1i1p1f1",
        "start_year": 1850,
        "end_year": 2014,
        "domain": "Arctic",
        "resolution": "T127_CORE2"
    },
    namelists={
        "fesom": {"timestep": 3600},
        "echam": {"radiation_scheme": "rrtm"}
    },
    snakemakes={
        "postprocess": "workflow/postprocess.smk"
    }
)

print(f"Simulation UID: {sim.uid}")
print(f"Simulation ID: {sim.simulation_id}")
```

#### Simulation Attributes

```python
# Add attributes
sim.add_attribute("sea_ice_model", "FESOM")
sim.add_attribute("forcing", "ERA5")

# Remove attributes
sim.remove_attribute("temporary_flag")
removed_value = sim.pop_attribute("old_key")

# Access attributes
experiment = sim.attrs.get("experiment")
all_attrs = sim.attrs  # Dictionary of all attributes
```

#### Context Variables for Templates

```python
# Get context variables for path templates
context = sim.get_context_variables()
# Returns: {
#   'simulation_id': 'CMIP6_historical_r1i1p1f1',
#   'model_id': 'AWI-CM-1-1-MR',
#   'uid': '<unique-id>',
#   'experiment': 'historical',
#   'domain': 'Arctic',
#   ...all other attributes...
# }
```

#### Location Management

```python
# Associate with locations
sim.associate_location("levante_scratch")
sim.associate_location(
    "levante_archive",
    context={"archive_type": "tape", "priority": "high"}
)

# Check associations
if sim.is_location_associated("levante_scratch"):
    print("Simulation is on Levante scratch")

# Get all associated locations
locations = sim.get_associated_locations()  # Returns sorted list

# Disassociate from location
sim.disassociate_location("old_location")

# Get location context
context = sim.get_location_context("levante_scratch")

# Update location context
sim.update_location_context(
    "levante_archive",
    {"archive_type": "tape", "backup_date": "2025-01-15"}
)
```

#### Snakemake Rules

```python
# Add Snakemake workflow rules
sim.add_snakemake_rule("preprocess", "workflow/preprocess.smk")
sim.add_snakemake_rule("analysis", "workflow/analysis.smk")

# Remove rules
sim.remove_snakemake_rule("old_workflow")

# Access rules
workflows = sim.snakemakes  # Dictionary of rule_name -> file_path
```

#### Validation

```python
# Validate simulation entity
errors = sim.validate()
if errors:
    print(f"Validation errors: {errors}")
else:
    print("Simulation is valid")
```

### Location Entity

The `Location` (alias for `LocationEntity`) abstracts storage backends with protocol-specific configuration.

#### Location Kinds

```python
from tellus import LocationKind

# Available location kinds
LocationKind.TAPE        # Tape archive storage
LocationKind.COMPUTE     # HPC compute scratch space
LocationKind.DISK        # Persistent disk storage
LocationKind.FILESERVER  # Network file server

# Convert from string
kind = LocationKind.from_str("compute")  # Returns LocationKind.COMPUTE
```

#### Creating Locations

```python
from tellus import Location, LocationKind

# Local filesystem location
local_loc = Location(
    name="local_data",
    kinds=[LocationKind.DISK],
    config={
        "protocol": "file",
        "path": "/data/simulations"
    }
)

# SSH/SFTP location
remote_loc = Location(
    name="levante_scratch",
    kinds=[LocationKind.COMPUTE, LocationKind.DISK],
    config={
        "protocol": "sftp",
        "path": "/scratch/users/username",
        "storage_options": {
            "host": "levante.dkrz.de",
            "username": "username",
            "key_filename": "~/.ssh/id_rsa",
            "port": 22
        }
    }
)

# ScoutFS location (optimized for HPC)
scoutfs_loc = Location(
    name="levante_archive",
    kinds=[LocationKind.TAPE, LocationKind.DISK],
    config={
        "protocol": "scoutfs",
        "path": "/work/archive",
        "storage_options": {
            "host": "levante.dkrz.de",
            "username": "username",
            "key_filename": "~/.ssh/id_rsa"
        },
        "warning_filters": {
            "stripe_warnings": False,
            "release_warnings": False
        }
    }
)
```

#### Location Properties

```python
# Get protocol and paths
protocol = location.get_protocol()  # 'sftp', 'file', 'ssh', 'scoutfs'
base_path = location.get_base_path()
storage_opts = location.get_storage_options()

# Check location characteristics
is_remote = location.is_remote()  # True for sftp, ssh, scoutfs
is_tape = location.is_tape_storage()  # True if LocationKind.TAPE
is_compute = location.is_compute_location()  # True if LocationKind.COMPUTE

# Manage location kinds
location.add_kind(LocationKind.FILESERVER)
location.remove_kind(LocationKind.COMPUTE)
has_disk = location.has_kind(LocationKind.DISK)
```

#### Location Configuration

```python
# Update configuration
location.update_config("path", "/new/base/path")
location.update_config("storage_options", {
    "host": "new-host.example.com",
    "username": "newuser"
})

# Validation happens automatically
try:
    location.update_config("protocol", "")  # Will raise ValueError
except ValueError as e:
    print(f"Invalid config: {e}")
```

#### Path Templates

Path templates enable dynamic path generation based on simulation attributes.

```python
from tellus.domain.entities.location import PathTemplate

# Create path templates
simple_template = PathTemplate(
    name="simple",
    pattern="{simulation_id}",
    description="Flat structure by simulation ID"
)

hierarchical_template = PathTemplate(
    name="hierarchical",
    pattern="{model}/{experiment}/{simulation_id}",
    description="Organized by model and experiment"
)

# Add templates to location
location.add_path_template(simple_template)
location.add_path_template(hierarchical_template)

# Get template suggestions
simulation_attrs = {
    "simulation_id": "arctic_001",
    "model": "FESOM2",
    "experiment": "historical"
}

# Auto-select best template
best_template = location.suggest_path_template(simulation_attrs)
suggested_path = location.suggest_path(simulation_attrs)
# Returns: "FESOM2/historical/arctic_001"

# Get all compatible templates
suggestions = location.get_template_suggestions(simulation_attrs)
# Returns list of dicts with template_name, resolved_path, etc.

# Use specific template
specific_path = location.suggest_path(simulation_attrs, template_name="simple")
# Returns: "arctic_001"
```

#### Default Templates

```python
# Create default templates based on location kind
location.create_default_templates()

# For COMPUTE locations: "simple", "model_experiment", "detailed"
# For DISK/FILESERVER: "project_organized", "timestamped"
# For TAPE: "archive_basic", "archive_dated"
```

### SimulationFile Entity

The `SimulationFile` entity represents files within simulation contexts with semantic classification.

#### File Content Types

```python
from tellus.domain.entities.simulation_file import FileContentType

FileContentType.INPUT       # Initial conditions, boundary data
FileContentType.OUTPUT      # Primary model output (renamed to OUTDATA)
FileContentType.OUTDATA     # Primary model output data
FileContentType.CONFIG      # Configuration files, namelists
FileContentType.RESTART     # Restart files, checkpoint data
FileContentType.LOG         # Log files, diagnostic output
FileContentType.SCRIPTS     # Scripts, executables, workflow files
FileContentType.VIZ         # Visualization files, plots
FileContentType.FORCING     # Forcing data, external input
FileContentType.ANALYSIS    # Analysis results, statistics
FileContentType.AUXILIARY   # Supporting files, documentation
```

#### File Importance Levels

```python
from tellus.domain.entities.simulation_file import FileImportance

FileImportance.CRITICAL    # Essential for simulation integrity
FileImportance.IMPORTANT   # Valuable but not critical
FileImportance.OPTIONAL    # Nice to have, can be regenerated
FileImportance.TEMPORARY   # Can be safely discarded
```

#### Creating Files

```python
from tellus.domain.entities.simulation_file import (
    SimulationFile,
    FileContentType,
    FileImportance,
    Checksum
)
from datetime import datetime

# Create a simulation file
sim_file = SimulationFile(
    relative_path="output/fesom/temp.nc",
    size=1024 * 1024 * 100,  # 100 MB
    checksum=Checksum(value="abc123", algorithm="sha256"),
    content_type=FileContentType.OUTDATA,
    importance=FileImportance.IMPORTANT,
    file_role="ocean_temperature",
    simulation_date=datetime(2025, 1, 1),
    location_name="levante_scratch",
    tags={"netcdf", "ocean", "monthly"}
)
```

#### File Inventory

```python
from tellus.domain.entities.simulation_file import FileInventory

# Create file inventory
inventory = FileInventory()

# Add files
inventory.add_file(sim_file)

# Query files
all_files = inventory.list_files()
output_files = inventory.filter_by_content_type(FileContentType.OUTDATA)
critical_files = inventory.filter_by_importance(FileImportance.CRITICAL)
netcdf_files = inventory.filter_by_pattern("*.nc")
tagged_files = inventory.filter_by_tags({"ocean"}, match_all=False)

# Get summaries
content_summary = inventory.get_content_type_summary()
# Returns: {'outdata': 45, 'restart': 12, 'config': 3, ...}

size_summary = inventory.get_size_by_content_type()
# Returns: {'outdata': 450000000, 'restart': 120000000, ...}

# Statistics
total_files = inventory.file_count
total_size = inventory.total_size
```

---

## Application Services

Application services orchestrate business workflows and use cases. They sit between the domain layer and infrastructure, handling validation, coordination, and data transformation.

### Service Container

The service container provides centralized dependency management.

```python
from tellus import get_service_container

# Get global service container
container = get_service_container()

# Access service factory
service_factory = container.service_factory

# Get specific services
sim_service = service_factory.get_simulation_service()
loc_service = service_factory.get_location_service()
file_service = service_factory.get_simulation_file_service()
archive_service = service_factory.get_archive_service()

# Access data paths
global_data = container.global_data_path  # ~/.tellus/
project_data = container.project_data_path  # <project>/.tellus/
project_root = container.project_path
```

### SimulationService

Handles simulation CRUD operations and business workflows.

#### Creating Simulations

```python
from tellus.application.dtos import CreateSimulationDto

sim_service = service_factory.get_simulation_service()

# Create simulation
dto = CreateSimulationDto(
    simulation_id="arctic_run_001",
    model_id="FESOM2",
    path="/scratch/username/arctic_run_001",
    attrs={
        "experiment": "sensitivity",
        "parameter": "sea_ice_albedo",
        "value": 0.75
    },
    namelists={
        "fesom": {"timestep": 3600}
    },
    snakemakes={
        "postprocess": "workflow/postprocess.smk"
    }
)

result = sim_service.create_simulation(dto)
print(f"Created: {result.simulation_id}")
print(f"UID: {result.uid}")
```

#### Retrieving Simulations

```python
# Get single simulation
simulation = sim_service.get_simulation("arctic_run_001")

if simulation:
    print(f"Model: {simulation.attributes.get('experiment')}")
    print(f"Locations: {simulation.locations}")

# List simulations with pagination
from tellus.application.dtos import FilterOptions

filters = FilterOptions(search_term="arctic")
result = sim_service.list_simulations(
    page=1,
    page_size=50,
    filters=filters
)

print(f"Total simulations: {result.pagination.total_count}")
for sim in result.simulations:
    print(f"- {sim.simulation_id}: {sim.attributes}")
```

#### Updating Simulations

```python
from tellus.application.dtos import UpdateSimulationDto

update_dto = UpdateSimulationDto(
    model_id="FESOM2.1",  # Update model version
    attrs={
        "experiment": "sensitivity",
        "updated": "2025-01-15",
        "notes": "Increased resolution"
    }
)

updated = sim_service.update_simulation("arctic_run_001", update_dto)
```

#### Deleting Simulations

```python
# Delete simulation
success = sim_service.delete_simulation("old_simulation")
if success:
    print("Simulation deleted")
```

#### Managing Attributes

```python
# Add single attribute
sim_service.add_simulation_attribute(
    "arctic_run_001",
    "ice_model_version",
    "2.1"
)

# Add Snakemake rule
sim_service.add_snakemake_rule(
    "arctic_run_001",
    "visualization",
    "workflow/viz.smk"
)
```

#### Location Associations

```python
from tellus.application.dtos import SimulationLocationAssociationDto

# Associate with multiple locations
assoc_dto = SimulationLocationAssociationDto(
    simulation_id="arctic_run_001",
    location_names=["levante_scratch", "levante_archive"],
    context_overrides={
        "levante_scratch": {
            "path_prefix": "{model}/{experiment}"
        },
        "levante_archive": {
            "path_prefix": "archive/{model}/{experiment}",
            "archive_priority": "high"
        }
    }
)

sim_service.associate_locations(assoc_dto)

# Disassociate from location
updated_sim = sim_service.disassociate_simulation_from_location(
    "arctic_run_001",
    "levante_scratch"
)

# Update location context
sim_service.update_simulation_location_context(
    "arctic_run_001",
    "levante_archive",
    {"backup_date": "2025-01-20", "tape_id": "T12345"}
)
```

#### Getting Context

```python
# Get context variables for path templates
context = sim_service.get_simulation_context("arctic_run_001")
# Returns dict with simulation_id, model_id, and all attributes
```

### LocationService

Manages storage location operations with protocol-specific validation.

#### Creating Locations

```python
from tellus.application.dtos import CreateLocationDto

loc_service = service_factory.get_location_service()

# Create local location
local_dto = CreateLocationDto(
    name="local_scratch",
    kinds=["COMPUTE", "DISK"],
    protocol="file",
    path="/scratch/local/simulations",
    additional_config={}
)

location = loc_service.create_location(local_dto)

# Create SSH location
ssh_dto = CreateLocationDto(
    name="hpc_cluster",
    kinds=["COMPUTE"],
    protocol="ssh",
    path="/work/username",
    storage_options={
        "host": "cluster.hpc.edu",
        "username": "username",
        "key_filename": "~/.ssh/id_rsa",
        "port": 22
    }
)

ssh_location = loc_service.create_location(ssh_dto)
```

#### Retrieving Locations

```python
# Get single location
location = loc_service.get_location("levante_scratch")
print(f"Protocol: {location.protocol}")
print(f"Kinds: {location.kinds}")
print(f"Remote: {location.is_remote}")

# List all locations
result = loc_service.list_locations(page=1, page_size=50)
for loc in result.locations:
    print(f"{loc.name}: {loc.protocol} - {loc.kinds}")

# Find by criteria
compute_locations = loc_service.find_by_kind("COMPUTE")
sftp_locations = loc_service.find_by_protocol("sftp")
```

#### Updating Locations

```python
from tellus.application.dtos import UpdateLocationDto

update_dto = UpdateLocationDto(
    kinds=["COMPUTE", "DISK", "FILESERVER"],
    path="/new/scratch/path",
    storage_options={
        "host": "new-host.example.com",
        "username": "newuser",
        "key_filename": "~/.ssh/new_key"
    }
)

updated = loc_service.update_location("levante_scratch", update_dto)
```

#### Testing Connectivity

```python
# Test location connectivity
result = loc_service.test_location_connectivity("levante_scratch", timeout_seconds=30)

if result.success:
    print(f"Connected successfully!")
    print(f"Latency: {result.latency_ms:.2f} ms")
    if result.available_space:
        print(f"Available space: {result.available_space / (1024**3):.2f} GB")
    print(f"Protocol info: {result.protocol_specific_info}")
else:
    print(f"Connection failed: {result.error_message}")
```

#### Validating Paths

```python
# Validate path accessibility
try:
    is_valid = loc_service.validate_location_path(
        "levante_scratch",
        "/scratch/users/username/experiment_001"
    )
    print("Path is accessible")
except LocationAccessError as e:
    print(f"Path not accessible: {e}")
```

### SimulationFileService

Manages simulation file operations and file inventories.

```python
file_service = service_factory.get_simulation_file_service()

# Get files for a simulation
files = sim_service.get_simulation_files("arctic_run_001")

for file in files:
    print(f"{file.relative_path}: {file.content_type} ({file.size} bytes)")
```

---

## Working with Simulations

### Complete Simulation Workflow

```python
from tellus import get_service_container
from tellus.application.dtos import (
    CreateSimulationDto,
    CreateLocationDto,
    SimulationLocationAssociationDto
)

# Initialize services
container = get_service_container()
service_factory = container.service_factory
sim_service = service_factory.get_simulation_service()
loc_service = service_factory.get_location_service()

# 1. Create storage locations
scratch_dto = CreateLocationDto(
    name="scratch",
    kinds=["COMPUTE"],
    protocol="file",
    path="/scratch/experiments"
)
scratch_loc = loc_service.create_location(scratch_dto)

archive_dto = CreateLocationDto(
    name="archive",
    kinds=["TAPE", "DISK"],
    protocol="file",
    path="/archive/experiments"
)
archive_loc = loc_service.create_location(archive_dto)

# 2. Create simulation
sim_dto = CreateSimulationDto(
    simulation_id="CMIP6_piControl_r1i1p1f1",
    model_id="AWI-CM-1-1-MR",
    attrs={
        "experiment": "piControl",
        "ensemble": "r1i1p1f1",
        "start_year": 1850,
        "simulation_years": 500,
        "components": ["ECHAM", "FESOM", "JSBACH"]
    },
    namelists={
        "echam": {
            "timestep": 450,
            "radiation": "rrtmg"
        },
        "fesom": {
            "timestep": 3600,
            "mixing_scheme": "KPP"
        }
    }
)

simulation = sim_service.create_simulation(sim_dto)
print(f"Created simulation: {simulation.simulation_id}")

# 3. Associate with locations
assoc_dto = SimulationLocationAssociationDto(
    simulation_id="CMIP6_piControl_r1i1p1f1",
    location_names=["scratch", "archive"],
    context_overrides={
        "scratch": {
            "path_prefix": "active/{model}/{experiment}/{simulation_id}"
        },
        "archive": {
            "path_prefix": "longterm/{model}/{experiment}"
        }
    }
)

sim_service.associate_locations(assoc_dto)
print("Locations associated")

# 4. Add metadata over time
sim_service.add_simulation_attribute(
    "CMIP6_piControl_r1i1p1f1",
    "status",
    "running"
)

sim_service.add_snakemake_rule(
    "CMIP6_piControl_r1i1p1f1",
    "daily_monitoring",
    "workflow/monitor.smk"
)

# 5. Retrieve and inspect
sim_info = sim_service.get_simulation("CMIP6_piControl_r1i1p1f1")
print(f"Simulation attributes: {sim_info.attributes}")
print(f"Locations: {list(sim_info.locations.keys())}")

# 6. List simulations with filters
from tellus.application.dtos import FilterOptions

filters = FilterOptions(search_term="CMIP6")
result = sim_service.list_simulations(page=1, page_size=10, filters=filters)

print(f"Found {result.pagination.total_count} CMIP6 simulations")
```

### Batch Operations

```python
# Create multiple simulations in a batch
ensemble_members = ["r1i1p1f1", "r2i1p1f1", "r3i1p1f1"]

for member in ensemble_members:
    sim_id = f"CMIP6_historical_{member}"

    dto = CreateSimulationDto(
        simulation_id=sim_id,
        model_id="AWI-CM-1-1-MR",
        attrs={
            "experiment": "historical",
            "ensemble_member": member,
            "start_year": 1850,
            "end_year": 2014
        }
    )

    sim = sim_service.create_simulation(dto)
    print(f"Created: {sim.simulation_id}")

    # Associate with locations
    assoc_dto = SimulationLocationAssociationDto(
        simulation_id=sim_id,
        location_names=["scratch", "archive"]
    )
    sim_service.associate_locations(assoc_dto)
```

---

## Managing Storage Locations

### Multi-Protocol Configuration

```python
from tellus.application.dtos import CreateLocationDto

loc_service = service_factory.get_location_service()

# Local filesystem
local = CreateLocationDto(
    name="local_data",
    kinds=["DISK"],
    protocol="file",
    path="/data/climate"
)

# SSH with key authentication
ssh_key = CreateLocationDto(
    name="remote_cluster",
    kinds=["COMPUTE"],
    protocol="ssh",
    path="/work/username",
    storage_options={
        "host": "cluster.example.com",
        "username": "username",
        "key_filename": "~/.ssh/id_rsa",
        "port": 22
    }
)

# SSH with password (not recommended)
ssh_pass = CreateLocationDto(
    name="remote_backup",
    kinds=["DISK"],
    protocol="ssh",
    path="/backup",
    storage_options={
        "host": "backup.example.com",
        "username": "username",
        "password": "secret"  # Use key authentication instead
    }
)

# SFTP (similar to SSH)
sftp = CreateLocationDto(
    name="data_server",
    kinds=["FILESERVER"],
    protocol="sftp",
    path="/data/shared",
    storage_options={
        "host": "data.example.com",
        "username": "datauser",
        "key_filename": "~/.ssh/data_key"
    }
)

# ScoutFS (HPC-optimized)
scoutfs = CreateLocationDto(
    name="hpc_archive",
    kinds=["TAPE", "DISK"],
    protocol="scoutfs",
    path="/work/archive",
    storage_options={
        "host": "hpc.example.com",
        "username": "username",
        "key_filename": "~/.ssh/hpc_key"
    },
    additional_config={
        "warning_filters": {
            "stripe_warnings": False,
            "release_warnings": False
        }
    }
)

# Create all locations
for dto in [local, ssh_key, sftp, scoutfs]:
    location = loc_service.create_location(dto)
    print(f"Created: {location.name} ({location.protocol})")
```

### Location Discovery and Management

```python
# Find compute locations
compute_locs = loc_service.find_by_kind("COMPUTE")
print(f"Compute locations: {[loc.name for loc in compute_locs]}")

# Find tape storage
tape_locs = loc_service.find_by_kind("TAPE")
print(f"Archive locations: {[loc.name for loc in tape_locs]}")

# Find by protocol
sftp_locs = loc_service.find_by_protocol("sftp")

# Test all locations
all_locs = loc_service.list_locations(page=1, page_size=100)
for loc in all_locs.locations:
    result = loc_service.test_location_connectivity(loc.name, timeout_seconds=10)
    status = "OK" if result.success else f"FAILED: {result.error_message}"
    print(f"{loc.name}: {status}")
```

---

## File Management

### Working with Simulation Files

```python
from tellus.domain.entities.simulation_file import (
    SimulationFile,
    FileContentType,
    FileImportance,
    Checksum
)

# Create simulation entity
sim = Simulation(
    simulation_id="ocean_test_001",
    model_id="FESOM2"
)

# Add files to simulation
restart_file = SimulationFile(
    relative_path="restart/fesom.restart.2025.nc",
    size=500 * 1024 * 1024,  # 500 MB
    content_type=FileContentType.RESTART,
    importance=FileImportance.CRITICAL,
    checksum=Checksum(value="abc123", algorithm="sha256")
)

output_file = SimulationFile(
    relative_path="output/fesom.temp.nc",
    size=2 * 1024 * 1024 * 1024,  # 2 GB
    content_type=FileContentType.OUTDATA,
    importance=FileImportance.IMPORTANT
)

log_file = SimulationFile(
    relative_path="logs/fesom.log",
    size=10 * 1024,  # 10 KB
    content_type=FileContentType.LOG,
    importance=FileImportance.OPTIONAL
)

# Add to simulation
sim.add_file(restart_file)
sim.add_file(output_file)
sim.add_file(log_file)

# Query files
all_files = sim.get_files()
print(f"Total files: {len(all_files)}")

restart_files = sim.get_files_by_content_type(FileContentType.RESTART)
critical_files = sim.get_files_by_importance(FileImportance.CRITICAL)

# Get summary
summary = sim.get_content_type_summary()
# {'restart': 1, 'outdata': 1, 'log': 1}

# File count
file_count = sim.get_file_count()
```

### File Tagging and Classification

```python
# Add tags to files
output_file.add_tag("ocean")
output_file.add_tag("monthly")
output_file.add_tag("temperature")

# Check tags
if output_file.has_tag("ocean"):
    print("This is ocean data")

# Match tags
ocean_tags = {"ocean", "salinity"}
if output_file.matches_any_tag(ocean_tags):
    print("Contains ocean-related data")

# File properties
filename = output_file.get_filename()  # "fesom.temp.nc"
extension = output_file.get_file_extension()  # "nc"
directory = output_file.get_directory()  # "output"

# Pattern matching
if output_file.matches_pattern("output/*.nc"):
    print("NetCDF output file")
```

### File Locations

```python
# Set file locations
output_file.set_primary_location("levante_scratch")
output_file.add_location("levante_archive")
output_file.add_location("local_backup")

# Check availability
if output_file.is_available_at_location("levante_scratch"):
    print("File is on Levante scratch")

# Get all locations
locations = output_file.get_available_locations()
# Returns: {'levante_scratch', 'levante_archive', 'local_backup'}

# Remove from location
output_file.remove_location("local_backup")
```

---

## Advanced Topics

### Custom Path Templates

```python
from tellus.domain.entities.location import PathTemplate

# Create custom templates
cmip_template = PathTemplate(
    name="cmip6_structure",
    pattern="{activity}/{institution}/{model}/{experiment}/{variant}/{table}",
    description="CMIP6 DRS directory structure"
)

date_based = PathTemplate(
    name="date_organized",
    pattern="{year}/{month:02d}/{day:02d}/{simulation_id}",
    description="Date-based organization"
)

# Add to location
location.add_path_template(cmip_template)
location.add_path_template(date_based)

# Use with simulation attributes
sim_attrs = {
    "activity": "CMIP",
    "institution": "AWI",
    "model": "AWI-CM-1-1-MR",
    "experiment": "historical",
    "variant": "r1i1p1f1",
    "table": "Amon",
    "simulation_id": "historical_001"
}

path = location.suggest_path(sim_attrs, template_name="cmip6_structure")
# Returns: "CMIP/AWI/AWI-CM-1-1-MR/historical/r1i1p1f1/Amon"
```

### Context Overrides

Location contexts allow per-simulation customization of paths and settings.

```python
# Associate simulation with context
sim.associate_location(
    "levante_archive",
    context={
        "path_prefix": "archive/{year}/{model}",
        "archive_policy": "compress",
        "retention_days": 730,
        "priority": "high"
    }
)

# Update context later
sim.update_location_context(
    "levante_archive",
    {
        "path_prefix": "archive/{year}/{month}/{model}",
        "backup_completed": "2025-01-20",
        "tape_id": "T54321"
    }
)

# Retrieve context
context = sim.get_location_context("levante_archive")
print(f"Archive policy: {context.get('archive_policy')}")
```

### Working with Archives

```python
from tellus.domain.entities.simulation_file import FileType

# Create archive file
archive = SimulationFile(
    relative_path="archives/output_2025.tar.gz",
    size=10 * 1024 * 1024 * 1024,  # 10 GB
    file_type=FileType.ARCHIVE,
    content_type=FileContentType.OUTDATA,
    importance=FileImportance.IMPORTANT
)

# Set archive properties
archive.set_archive_properties(
    archive_format="tar.gz",
    compression_type="gzip",
    path_prefix_to_strip="simulation_001/"
)

# Add contained files
for i in range(100):
    file_id = f"output_file_{i:03d}.nc"
    archive.add_contained_file(file_id)

# Check archive properties
is_archive = archive.is_archive()  # True
contained_count = archive.get_contained_file_count()  # 100

# Truncate paths during extraction
full_path = "simulation_001/output/temp.nc"
clean_path = archive.truncate_path(full_path)  # "output/temp.nc"
```

### Repository Pattern Usage

For advanced users who need direct repository access:

```python
from tellus import get_service_container

container = get_service_container()
factory = container.service_factory

# Get repositories directly
sim_repo = factory.get_simulation_repository()
loc_repo = factory.get_location_repository()

# Use entity objects directly
from tellus import Simulation

sim = Simulation(
    simulation_id="direct_access_001",
    model_id="FESOM2"
)

# Save directly
sim_repo.save(sim)

# Query directly
all_sims = sim_repo.list_all()
sim_by_id = sim_repo.get_by_id("direct_access_001")

# Exists check
exists = sim_repo.exists("direct_access_001")
```

---

## Best Practices

### When to Use Entities vs Services

**Use Entities directly when:**
- Performing in-memory operations
- Building complex object graphs
- Testing business logic
- Working with temporary data

**Use Services when:**
- Persisting data to storage
- Orchestrating multi-step workflows
- Validating against external systems
- Need transaction-like behavior

### Configuration Management

```python
# Recommended: Use environment variables for sensitive data
import os

location_dto = CreateLocationDto(
    name="secure_location",
    kinds=["COMPUTE"],
    protocol="ssh",
    storage_options={
        "host": os.environ["HPC_HOST"],
        "username": os.environ["HPC_USER"],
        "key_filename": os.path.expanduser("~/.ssh/id_rsa")
    }
)

# Avoid: Hardcoding credentials
# location_dto = CreateLocationDto(
#     storage_options={"password": "secret123"}  # Bad!
# )
```

### Path Template Design

```python
# Good: Hierarchical, human-readable
pattern = "{institution}/{model}/{experiment}/{year}"

# Good: Machine-friendly, sortable
pattern = "{year}/{month:02d}/{simulation_id}"

# Avoid: Flat structures for large datasets
pattern = "{simulation_id}"  # Hard to navigate with 1000s of runs

# Avoid: Too deep nesting
pattern = "{a}/{b}/{c}/{d}/{e}/{f}/{g}/{h}"  # Excessive
```

### Error Handling

```python
from tellus.application.exceptions import (
    EntityNotFoundError,
    EntityAlreadyExistsError,
    ValidationError,
    LocationAccessError
)

try:
    # Create simulation
    sim = sim_service.create_simulation(dto)

    # Associate location
    assoc_dto = SimulationLocationAssociationDto(
        simulation_id=sim.simulation_id,
        location_names=["nonexistent_location"]
    )
    sim_service.associate_locations(assoc_dto)

except EntityAlreadyExistsError as e:
    print(f"Simulation already exists: {e}")
except EntityNotFoundError as e:
    print(f"Entity not found: {e}")
except ValidationError as e:
    print(f"Validation failed: {e}")
    if hasattr(e, 'errors'):
        for error in e.errors:
            print(f"  - {error}")
except LocationAccessError as e:
    print(f"Cannot access location: {e}")
```

### Performance Optimization

```python
# Batch queries when possible
result = sim_service.list_simulations(page=1, page_size=100)
simulations = result.simulations

# Avoid N+1 queries
for sim in simulations:
    # Don't query for each simulation
    # files = sim_service.get_simulation_files(sim.simulation_id)
    pass

# Instead, get file information upfront or use bulk operations

# Use pagination for large result sets
page = 1
while True:
    result = sim_service.list_simulations(page=page, page_size=50)
    if not result.simulations:
        break

    for sim in result.simulations:
        process_simulation(sim)

    if not result.pagination.has_next:
        break
    page += 1
```

---

## Error Handling

### Common Exceptions

```python
from tellus.application.exceptions import (
    EntityNotFoundError,           # Entity doesn't exist
    EntityAlreadyExistsError,      # Entity already exists (unique constraint)
    ValidationError,               # Invalid input data
    BusinessRuleViolationError,    # Business logic violation
    LocationAccessError,           # Cannot access storage location
    ConfigurationError,            # Invalid configuration
    ExternalServiceError           # External service failure
)
```

### Exception Handling Patterns

```python
from tellus.application.exceptions import (
    EntityNotFoundError,
    ValidationError,
    LocationAccessError
)

def safe_simulation_operation(sim_id: str):
    """Example of comprehensive error handling."""
    try:
        # Attempt operation
        sim = sim_service.get_simulation(sim_id)

        if sim is None:
            print(f"Simulation {sim_id} not found")
            return None

        # Test location connectivity
        for location_name in sim.locations.keys():
            result = loc_service.test_location_connectivity(location_name)
            if not result.success:
                raise LocationAccessError(
                    location_name,
                    result.error_message
                )

        return sim

    except EntityNotFoundError as e:
        print(f"Entity not found: {e.entity_type} '{e.entity_id}'")
        return None

    except ValidationError as e:
        print(f"Validation error: {e.message}")
        if hasattr(e, 'errors') and e.errors:
            for error in e.errors:
                print(f"  - {error}")
        return None

    except LocationAccessError as e:
        print(f"Location access error: {e}")
        # Could implement retry logic here
        return None

    except Exception as e:
        print(f"Unexpected error: {e}")
        raise  # Re-raise unexpected errors
```

---

## API Reference

### Main Exports

```python
from tellus import (
    # Entities (domain objects)
    Simulation,              # Alias for SimulationEntity
    SimulationEntity,        # Full name
    Location,                # Alias for LocationEntity
    LocationEntity,          # Full name
    LocationKind,            # Enum for location types

    # Services
    SimulationService,       # Alias for SimulationApplicationService
    SimulationApplicationService,  # Full name
    LocationService,         # Alias for LocationApplicationService
    LocationApplicationService,    # Full name

    # Container
    get_service_container,   # Get global service container

    # Infrastructure
    ScoutFSFileSystem        # ScoutFS filesystem adapter
)
```

### LocationKind Enum

```python
from tellus import LocationKind

LocationKind.TAPE        # Tape archive storage
LocationKind.COMPUTE     # HPC compute scratch space
LocationKind.DISK        # Persistent disk storage
LocationKind.FILESERVER  # Network file server

# Convert from string
kind = LocationKind.from_str("compute")
```

### DTOs (Data Transfer Objects)

```python
from tellus.application.dtos import (
    # Simulation DTOs
    CreateSimulationDto,
    UpdateSimulationDto,
    SimulationDto,
    SimulationListDto,

    # Location DTOs
    CreateLocationDto,
    UpdateLocationDto,
    LocationDto,
    LocationListDto,
    LocationTestResult,

    # Association DTOs
    SimulationLocationAssociationDto,

    # File DTOs
    SimulationFileDto,
    FileRegistrationDto,

    # Utility DTOs
    FilterOptions,
    PaginationInfo,
    CacheConfigurationDto
)
```

### Service Factory Methods

```python
service_factory = get_service_container().service_factory

# Get services
sim_service = service_factory.get_simulation_service()
loc_service = service_factory.get_location_service()
file_service = service_factory.get_simulation_file_service()
archive_service = service_factory.get_archive_service()
workflow_service = service_factory.get_workflow_service()

# Get repositories (for advanced usage)
sim_repo = service_factory.get_simulation_repository()
loc_repo = service_factory.get_location_repository()
```

---

## Complete Example: CMIP6 Workflow

```python
#!/usr/bin/env python3
"""
Example: CMIP6-compliant simulation management workflow.
"""

from tellus import get_service_container
from tellus.application.dtos import (
    CreateSimulationDto,
    CreateLocationDto,
    SimulationLocationAssociationDto
)

def main():
    # Initialize
    container = get_service_container()
    factory = container.service_factory
    sim_service = factory.get_simulation_service()
    loc_service = factory.get_location_service()

    # Define CMIP6 metadata
    cmip6_attrs = {
        "activity_id": "CMIP",
        "institution_id": "AWI",
        "source_id": "AWI-CM-1-1-MR",
        "experiment_id": "historical",
        "variant_label": "r1i1p1f1",
        "grid_label": "gn",
        "start_year": 1850,
        "end_year": 2014,
        "realm": "atmos ocean seaIce",
        "frequency": "mon",
        "cmip6_version": "v20191120"
    }

    # Create storage locations
    locations = [
        CreateLocationDto(
            name="compute_scratch",
            kinds=["COMPUTE"],
            protocol="file",
            path="/scratch/climate/cmip6"
        ),
        CreateLocationDto(
            name="longterm_archive",
            kinds=["TAPE", "DISK"],
            protocol="file",
            path="/archive/cmip6"
        ),
        CreateLocationDto(
            name="publication_server",
            kinds=["FILESERVER"],
            protocol="sftp",
            path="/esgf/data/cmip6",
            storage_options={
                "host": "esgf.example.com",
                "username": "cmip_user",
                "key_filename": "~/.ssh/cmip_key"
            }
        )
    ]

    for loc_dto in locations:
        try:
            loc = loc_service.create_location(loc_dto)
            print(f"Created location: {loc.name}")
        except Exception as e:
            print(f"Location {loc_dto.name} already exists or error: {e}")

    # Create simulation
    sim_id = f"{cmip6_attrs['source_id']}_{cmip6_attrs['experiment_id']}_{cmip6_attrs['variant_label']}"

    sim_dto = CreateSimulationDto(
        simulation_id=sim_id,
        model_id=cmip6_attrs['source_id'],
        attrs=cmip6_attrs,
        namelists={
            "echam": {
                "timestep": 450,
                "radiation_scheme": "rrtmg"
            },
            "fesom": {
                "timestep": 3600,
                "mixing_scheme": "KPP"
            }
        }
    )

    try:
        sim = sim_service.create_simulation(sim_dto)
        print(f"Created simulation: {sim.simulation_id}")
    except Exception as e:
        print(f"Simulation creation failed: {e}")
        return

    # Associate with locations using CMIP6 DRS
    assoc_dto = SimulationLocationAssociationDto(
        simulation_id=sim_id,
        location_names=["compute_scratch", "longterm_archive", "publication_server"],
        context_overrides={
            "compute_scratch": {
                "path_prefix": "work/{source_id}/{experiment_id}/{variant_label}"
            },
            "longterm_archive": {
                "path_prefix": "cmip6/{activity_id}/{institution_id}/{source_id}/{experiment_id}"
            },
            "publication_server": {
                "path_prefix": "{activity_id}/{institution_id}/{source_id}/{experiment_id}/{variant_label}/{grid_label}"
            }
        }
    )

    sim_service.associate_locations(assoc_dto)
    print("Locations associated with CMIP6 DRS structure")

    # Add workflow tracking
    sim_service.add_snakemake_rule(
        sim_id,
        "cmip6_postprocess",
        "workflow/cmip6_postprocess.smk"
    )

    # Retrieve and display
    final_sim = sim_service.get_simulation(sim_id)
    print(f"\nSimulation Configuration:")
    print(f"  ID: {final_sim.simulation_id}")
    print(f"  Model: {final_sim.attributes.get('source_id')}")
    print(f"  Experiment: {final_sim.attributes.get('experiment_id')}")
    print(f"  Variant: {final_sim.attributes.get('variant_label')}")
    print(f"\nLocations:")
    for loc_name, loc_context in final_sim.locations.items():
        print(f"  - {loc_name}: {loc_context}")

if __name__ == "__main__":
    main()
```

---

## Further Reading

- **User Guide**: `/docs/user-guide/` - Comprehensive user documentation
- **Architecture**: `/docs/development/architecture.md` - System design and patterns
- **CLI Documentation**: `/docs/interactive-wizards.md` - Command-line interface
- **API Documentation**: `/docs/api/` - Detailed API reference
- **Examples**: `/docs/examples/` - Additional code examples
- **GitHub Repository**: https://github.com/pgierz/tellus

---

## Support and Community

- **Issues**: [GitHub Issues](https://github.com/pgierz/tellus/issues)
- **Discussions**: [GitHub Discussions](https://github.com/pgierz/tellus/discussions)
- **Email**: pgierz@awi.de

---

## License

Tellus is released under the MIT License. See LICENSE file for details.

---

*This documentation covers Tellus SDK version 0.1.0+. For the latest updates, see the [changelog](/docs/changelog.md).*
