# Quick Start

This guide will get you up and running with Tellus in just a few minutes. We'll cover the basic concepts and show you how to create your first simulation and storage location.

## Basic Concepts

Before we start, let's understand the two main concepts in Tellus:

- **Simulations**: Represent computational experiments or datasets with associated metadata and file locations
- **Locations**: Define storage backends (local disk, remote servers, cloud storage) that can be attached to simulations

## Your First Simulation

Let's create a simple simulation to manage some data:

```bash
# Create a new simulation
tellus simulation create my-first-sim --path /data/my-experiment

# View the simulation
tellus simulation show my-first-sim
```

You should see output similar to:

```
┌─ Simulation: my-first-sim ─┐
│ ID: my-first-sim           │
│ Path: /data/my-experiment  │
└────────────────────────────┘
```

## Adding Storage Locations

Next, let's create a storage location. We'll start with a local directory:

```bash
# Create a local storage location
tellus location create local-data --protocol file --path /home/user/data

# View all locations
tellus location ls
```

Now attach this location to your simulation:

```bash
# Add location to simulation (interactive wizard)
tellus simulation location add
```

The wizard will guide you through:
1. Selecting your simulation (`my-first-sim`)
2. Choosing the location (`local-data`)
3. Optionally setting a path prefix

## Working with Files

Once you have a simulation with attached locations, you can start working with files:

```bash
# List files in a location
tellus simulation location ls my-first-sim local-data

# Download a file (if it exists)
tellus simulation location get my-first-sim local-data "data.txt" ./

# Download multiple files matching a pattern
tellus simulation location mget my-first-sim local-data "*.nc" ./results/
```

## Remote Storage Example

Tellus really shines when working with remote storage. Let's set up an SSH location:

```bash
# Create SSH location
tellus location create remote-server \
  --protocol ssh \
  --host example.com \
  --username myuser \
  --path /data/remote

# Add to simulation with context
tellus simulation location add my-first-sim remote-server \
  --path-prefix "/experiments/{{simulation_id}}"
```

The path prefix uses template variables - `{{simulation_id}}` will be replaced with `my-first-sim` when accessing files.

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