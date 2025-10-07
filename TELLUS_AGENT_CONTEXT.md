# Tellus Agent Context

This document provides comprehensive context for AI agents working with the Tellus distributed data management system for Earth System Model simulations.

## What is Tellus?

Tellus is a command-line tool for managing Earth System Model simulation data across distributed storage systems. It provides a unified interface for organizing, discovering, and accessing simulation data stored on various backends (local filesystems, SSH/SFTP servers, cloud storage, tape archives).

## Core Architecture

### Domain Structure
- **Simulations**: Computational experiments with metadata (model, experiment ID, attributes)
- **Locations**: Storage backends with different characteristics (DISK, TAPE, COMPUTE, FILESERVER)
- **Templates**: Patterns for creating simulation series with variable substitution
- **Files**: Individual data files associated with simulations

### Key Design Patterns
- **Clean Architecture**: Domain entities, application services, infrastructure adapters
- **Template-based Path Resolution**: Locations use templates like `{model}/{experiment}` for dynamic paths
- **Storage Abstraction**: Unified interface over fsspec for local/remote/cloud storage
- **Context System**: Per-location path customization for simulation data organization

### Technology Stack
- **CLI**: rich-click for interactive commands with rich formatting
- **Storage**: fsspec for unified filesystem interface (local, SSH, S3, etc.)
- **Database**: SQLite (primary) with PostgreSQL fallback support
- **Package Management**: Pixi for reproducible environments
- **Architecture**: Service layer with DTOs, repositories, and clean separation

## Core Workflows

### Basic Simulation Management
```bash
# Create simulation
tellus simulation create <sim_id>

# List all simulations
tellus simulation list

# Show simulation details
tellus simulation show <sim_id>

# Remove simulation
tellus simulation remove <sim_id>
```

### Location Management
```bash
# Create location
tellus location create <name> --kind <DISK|TAPE|COMPUTE> --path <path>

# For SSH locations
tellus location create remote-hpc --kind COMPUTE --ssh-host cluster.example.com --path /scratch/user

# List locations
tellus location list

# Show location details
tellus location show <name>
```

### Location Association
```bash
# Associate simulation with location
tellus simulation add_location <sim_id> <location_name>

# With custom path context
tellus simulation add_location <sim_id> <location_name> --context path_prefix=/custom/path

# List files at location
tellus simulation location ls <sim_id> <location_name>
tellus simulation location ls -lhT <sim_id> <location_name>  # Detailed view
```

### Template System (Advanced)
```bash
# Create template for simulation series
tellus simulation template create eem-series \
  --pattern "Eem{time_period}-S2" \
  --variables time_period:int:120-130 \
  --description "Eemian simulations"

# Scan location for matching simulations
tellus simulation location scan <location_name> <path> --template <template_name>

# Auto-import discovered simulations
tellus simulation location scan <location_name> <path> --template <template_name> --auto-import
```

## Path Template System

### Variable Expansion
Locations can use template variables in their path configurations:
- `{expid}` - Simulation/experiment ID
- `{model}` - Model name
- `{user}` - Username
- Custom attributes from simulation metadata

### Context Overrides
Per-location customization:
```bash
# Override path for specific simulation-location pair
tellus simulation add_location sim-001 hsm --context path_prefix=/archive/special/path
```

### Template Patterns
For simulation series:
- `Eem{time_period}-S2` matches `Eem125-S2`, `Eem130-S2`
- `{model}_{experiment}_{year}` matches `FESOM_PI_2020`
- Variables can have type constraints (int, str) and ranges

## Storage Backend Support

### Local Filesystem
```bash
tellus location create local-data --kind DISK --path /data/simulations
```

### SSH/SFTP
```bash
tellus location create hpc-cluster --kind COMPUTE \
  --ssh-host cluster.example.com \
  --ssh-user username \
  --path /scratch/simulations
```

### Tape Archives (HSM)
```bash
tellus location create tape-archive --kind TAPE \
  --ssh-host archive.example.com \
  --path /hs/projects/archive
```

## Common Use Cases

### 1. Organizing Existing Data
```bash
# Create location for existing data
tellus location create existing-data --kind DISK --path /path/to/data

# Create simulations for existing experiments
tellus simulation create experiment-001
tellus simulation add_location experiment-001 existing-data

# Browse data
tellus simulation location ls experiment-001 existing-data
```

### 2. Bulk Import from Templates
```bash
# Create template for series
tellus simulation template create climate-series \
  --pattern "{model}_{scenario}_{period}" \
  --variables model:str,scenario:str,period:int

# Scan and import
tellus simulation location scan data-location /path/to/experiments \
  --template climate-series --auto-import
```

### 3. Multi-Location Workflows
```bash
# Associate simulation with multiple locations
tellus simulation add_location sim-001 compute-cluster
tellus simulation add_location sim-001 tape-archive
tellus simulation add_location sim-001 local-analysis

# Different path contexts per location
tellus simulation add_location sim-001 archive \
  --context path_prefix=/long-term/storage/sim-001
```

## Development Commands

### Testing
```bash
pixi run test                    # Full test suite
pixi run -e test pytest -m unit # Unit tests only
pixi run -e test pytest -k "test_name"  # Specific tests
```

### Documentation
```bash
pixi run docs-build             # Build docs
pixi run docs-serve             # Serve locally
```

## Troubleshooting

### Common Issues

1. **Template variables not expanding**
   - Check simulation has required attributes
   - Verify location path template syntax
   - Use `tellus simulation show <id>` to check context

2. **SSH connection failures**
   - Verify SSH key authentication
   - Check host connectivity
   - Test with `ssh user@host` manually

3. **Path resolution errors**
   - Check absolute vs relative paths
   - Verify location base_path configuration
   - Use `tellus location show <name>` to debug

### Debug Mode
```bash
# Enable database debugging
export TELLUS_DEBUG_DATABASE=true

# Enable verbose logging
export TELLUS_LOG_LEVEL=DEBUG
```

## Recent Architectural Improvements

### Fixed Issues
- Template variable expansion in path resolution
- Absolute path handling in location listings
- Auto-import with proper location associations
- PostgreSQL connection noise reduction
- Path context using discovered simulation paths

### New Features
- Location-based scanning for remote filesystems
- Template pattern matching with variable extraction
- Bulk import from scan results with location association
- Context override system for flexible path management

## CLI Styling Standards

When working with CLI code:
- Always use `import rich_click as click` (not plain click)
- Import `console` from `..core.cli` for consistent output
- Use `console.print()` instead of `click.echo()`
- Rich markup: `[red]Error:[/red]`, `[green]Success:[/green]`, `[dim]...[/dim]`
- Use `Table` for tabular data, `Panel` for detailed info
- Show feature flag indicators: "✨ Using new [service] service"

## Agent Usage Guidelines

When helping users with Tellus:

1. **Start Simple**: Begin with basic simulation/location workflows
2. **Progressive Complexity**: Introduce templates/scanning for advanced use cases
3. **Verify Setup**: Check location configuration before complex operations
4. **Context Awareness**: Consider path templates and variable expansion
5. **Error Diagnosis**: Use show/list commands to debug issues
6. **Best Practices**: Encourage consistent naming conventions

Users typically start with manual simulation creation and location association, then progress to template-based bulk operations as their data organization needs grow.