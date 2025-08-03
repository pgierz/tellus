# Development Guide

Welcome to the Tellus development guide! This section contains information for contributors, maintainers, and anyone interested in understanding how Tellus works under the hood.

## Quick Links

::::{grid} 2
:::{grid-item-card} {octicon}`rocket` Getting Started
:link: getting-started
:link-type: doc

Set up your development environment and build documentation
:::

:::{grid-item-card} {octicon}`git-branch` Contributing
:link: contributing
:link-type: doc

Guidelines for contributing code, documentation, and examples
:::

:::{grid-item-card} {octicon}`package` Architecture
:link: architecture
:link-type: doc

Understanding Tellus's design and internal structure
:::

:::{grid-item-card} {octicon}`tools` Testing
:link: testing
:link-type: doc

Running tests and ensuring code quality
:::
::::

## Development Workflow

Tellus uses a modern Python development workflow with:

- **[Pixi](https://pixi.sh)** for environment and dependency management
- **[Pytest](https://pytest.org)** for testing
- **[Jupyter-Book](https://jupyterbook.org)** for documentation
- **[Rich](https://rich.readthedocs.io/)** for beautiful CLI interfaces
- **[fsspec](https://filesystem-spec.readthedocs.io/)** for filesystem abstraction

## Key Concepts for Developers

Understanding these core concepts will help you contribute effectively:

### Simulations
- Represent computational experiments or datasets
- Store metadata and file location mappings
- Persist to JSON files for state management

### Locations  
- Abstract storage backends (local, SSH, cloud)
- Use fsspec for unified filesystem interface
- Support authentication and configuration

### Context
- Provides path templating and metadata for location usage
- Enables dynamic path resolution based on simulation attributes
- Allows flexible data organization patterns

## Code Organization

```
src/tellus/
├── core/           # CLI entry points and main application logic
├── simulation/     # Simulation management and CLI commands  
├── location/       # Storage location management and CLI commands
├── progress.py     # Progress tracking utilities
└── scoutfs.py      # ScoutFS-specific utilities
```

```{toctree}
:maxdepth: 2

getting-started
contributing
architecture
testing
```