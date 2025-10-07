# Tellus Core Working Features

## ✅ Basic Simulation Management
- **Create Simulation**: `tellus simulation create <sim_id>`
- **List Simulations**: `tellus simulation list`
- **Show Simulation Details**: `tellus simulation show <sim_id>`
- **Remove Simulation**: `tellus simulation remove <sim_id>`

## ✅ Location Management
- **Create Location**: `tellus location create <name> --kind <DISK|TAPE|COMPUTE> --path <path>`
- **List Locations**: `tellus location list`
- **Show Location Details**: `tellus location show <name>`

## ✅ Location Association
- **Associate Location with Simulation**: `tellus simulation add_location <sim_id> <location_name>`
- **Associate with Path Context**: Can specify custom path with `--context path_prefix=/custom/path`
- **Multiple Locations**: Simulations can be associated with multiple locations

## ✅ Remote Filesystem Access
- **SSH/SFTP Support**: Locations with SSH configuration work properly
  - Example: `hsm` location with SSH to remote server
- **List Remote Files**: `tellus simulation location ls <sim_id> <location_name>`
  - Works with SSH locations
  - Properly resolves template variables in paths
  - Shows file sizes, permissions, modification times

## ✅ Path Template Resolution
- **Variable Expansion**: Paths with `{expid}`, `{model}`, `{user}` variables expand correctly
- **Absolute Paths**: Paths starting with `/` are handled correctly
- **Context Overrides**: Can override path context per location association

## 📝 Basic Commands That Work
```bash
# Create a simulation
tellus simulation create Eem130-S2

# Create a location
tellus location create hsm --kind TAPE --path /hs/projects

# Associate location with simulation
tellus simulation add_location Eem130-S2 hsm

# List files on remote location
tellus simulation location ls Eem130-S2 hsm
tellus simulation location ls -lhT Eem130-S2 hsm  # Detailed listing

# Show simulation with locations
tellus simulation show Eem130-S2
```