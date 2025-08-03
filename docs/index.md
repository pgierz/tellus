# Tellus Documentation

Welcome to **Tellus**, a distributed data management system designed for scientific computing workflows. Tellus helps you manage simulations, configure storage locations, and efficiently transfer data across different storage backends.

```{note}
Tellus is currently in active development. Features and APIs may change between versions.
```

## What is Tellus?

Tellus provides a unified interface for:

- **Simulation Management**: Organize and track computational experiments with metadata and file locations
- **Storage Abstraction**: Work with local filesystems, remote servers, and cloud storage through a single interface  
- **Data Transfer**: Download files and datasets with progress tracking and resumable transfers
- **Workflow Integration**: Seamlessly integrate with Snakemake and other workflow engines

## Key Features

::::{grid} 2
:::{grid-item-card} 🎯 Unified Data Access
:class-header: text-center

Access data from local disks, SSH/SFTP servers, and cloud storage (S3, Google Cloud) through a single interface.
:::

:::{grid-item-card} 📊 Progress Tracking  
:class-header: text-center

Beautiful progress bars and transfer statistics for all file operations using Rich and fsspec.
:::

:::{grid-item-card} 🔧 Interactive CLI
:class-header: text-center

Rich command-line interface with interactive wizards and tab completion for easy configuration.
:::

:::{grid-item-card} 🔗 Workflow Integration
:class-header: text-center

Native integration with Snakemake workflows and support for computational pipelines.
:::
::::

## Quick Example

Here's a simple example of using Tellus to manage a simulation and download data:

```bash
# Create a new simulation
tellus simulation create my-experiment --path /data/experiments/exp1

# Add a remote storage location
tellus simulation location add my-experiment remote-storage

# Download files with progress tracking
tellus simulation location get my-experiment remote-storage "results/*.nc" ./local-data/
```

## Getting Started

::::{grid} 1 2 2 3
:::{grid-item-card} {octicon}`rocket` Installation
:link: installation
:link-type: doc

Install Tellus and set up your environment
:::

:::{grid-item-card} {octicon}`zap` Quick Start
:link: quickstart  
:link-type: doc

Get up and running with Tellus in minutes
:::

:::{grid-item-card} {octicon}`book` User Guide
:link: user-guide/index
:link-type: doc

Learn about simulations, locations, and workflows
:::

:::{grid-item-card} {octicon}`code` Examples
:link: examples/index
:link-type: doc

Notebook examples and real-world use cases
:::

:::{grid-item-card} {octicon}`gear` API Reference
:link: api/index
:link-type: doc

Complete API documentation
:::

:::{grid-item-card} {octicon}`people` Development
:link: development/index
:link-type: doc

Contributing and architecture guides
:::
::::

```{toctree}
:hidden:
:maxdepth: 2

installation
quickstart
user-guide/index
examples/index
api/index
development/index
changelog
references
```