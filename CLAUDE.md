# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Github Issue Workflow

The user may occassionally want to work in "github issue flow" mode. The workflow is as follows:

* Work has (more-or-less) stabilized on a branch (e.g. `wip`). Regular commits are occuring, and the tool is in a usable form.
* User creates a new issue with `gh issue` (or `gh issue create`)
* The AI will be asked to "work on gh-issues" or "work on github issues" or "work on issues". The AI will then look at the issues like this:
```
$ gh issue list
$ gh issue view <issue_number>
```
* The AI can work on one issue until it would normally report a summary back to the user.
* The AI will ensure an atomic commit of its work is made, which includes a reference to the issue number.
* The AI will add the summary of the work for that issue to a global summary.
* The AI will examine it's credit token usage, and if it is close to the limit, it will report the global summary to the user. If it is not close, it will go to the next issue.
* The limit is close if the token usage is 90% of the limit.
* The AI will then ask the user to "close issue <issue_number>" or "close issues <issue_number1>,<issue_number2>,..."


## Development Commands

This project uses **Pixi** as the package and environment manager. All development commands should use `pixi run`.

### Testing
- `pixi run test` - Run the full test suite
- `pixi run -e test pytest tests/test_specific.py` - Run specific test file
- `pixi run -e test pytest -m unit` - Run tests with specific markers
- `pixi run -e test pytest -k "test_name"` - Run tests matching pattern

Available test markers: unit, integration, performance, property, earth_science, archive, cache, location, slow, network, large_data, hpc, timeout, xarray, benchmark, trio

### Documentation
- `pixi run docs-build` - Build documentation with Jupyter Book
- `pixi run docs-clean` - Clean documentation build
- `pixi run docs-serve` - Serve docs locally on port 8000

### CLI
- `pixi run tellus` - Run the main CLI
- `pixi run sandbox` - Run sandbox scripts in sandbox/ directory

### Adding Dependencies
- `pixi add <package>` - Add to main dependencies
- `pixi add --feature test <package>` - Add to test environment
- See HACKING.md for specialized environments

## Architecture Overview

Tellus is a distributed data management system for Earth System Model simulations with a modular architecture:

### Core Components

**CLI Layer (`tellus.core.cli`)**
- Rich-click based CLI with subcommands organized by domain
- Interactive wizards using questionary for complex operations
- Main entry point: `src/tellus/core/cli.py`

**CLI Styling Standards**
- ALWAYS use `import rich_click as click` (not plain `click`)
- ALWAYS import `console` from `..core.cli` for output
- ALWAYS use `console.print()` instead of `click.echo()`
- Use rich markup: `[red]Error:[/red]`, `[green]Success:[/green]`, `[dim]...[/dim]`
- Use `Table` for tabular data, `Panel` for detailed info
- Show "✨ Using new [service] service" indicators when feature flags are enabled
- Path completion uses `SmartPathCompleter` for location-aware filesystem completion

**Simulation Management (`tellus.simulation`)**
- Central `Simulation` class representing computational experiments
- JSON-based persistence in `simulations.json` at project root
- Context system for path templating using simulation attributes
- Archive system with tar-based storage and extraction

**Location Management (`tellus.location`)**
- `Location` class abstracting storage backends (local, SSH/SFTP, cloud)
- LocationKind enum: TAPE, COMPUTE, DISK, FILESERVER
- fsspec-based unified storage interface
- JSON-based persistence in `locations.json`

### Key Design Patterns

**Template-based Path Resolution**
- Location contexts use `path_prefix` templates like `"{model}/{experiment}"`
- Templates resolved using simulation attributes
- Supports nested directory structures for data organization

**Storage Abstraction**
- Unified interface over local filesystem, SSH, and cloud storage
- fsspec backend provides consistent API across storage types
- Progress tracking for file operations

**Cache Management** 
- Two-level caching: archive-level (50GB default) and file-level (10GB default)
- Configurable cleanup policies: LRU, manual, size-only
- Cache directory: `~/.cache/tellus`

## Testing Strategy

The project uses pytest with comprehensive test organization:

- `tests/` - Main test directory
- `tests/architecture/` - Architectural patterns and utilities  
- `tests/fixtures/` - Shared test fixtures
- `tests/integration/` - Integration and system tests
- `conftest.py` - Global pytest configuration and fixtures

Test configuration in `pyproject.toml` includes extensive markers for categorizing tests by type (unit, integration), domain (earth_science, archive), and requirements (network, large_data).

## Important Files

- `simulations.json` - Simulation metadata persistence
- `locations.json` - Storage location definitions  
- `config/` - Configuration files and templates
- `workflow/` - Snakemake workflow definitions
- `sandbox/` - Development and testing scripts

## Package Structure

```
src/tellus/
├── __init__.py           # Main exports
├── cli/                  # New CLI structure  
├── core/                 # Core CLI functionality
├── simulation/           # Simulation management
├── location/             # Storage location management
├── testing/              # Test utilities and fixtures
├── progress.py           # Progress tracking
└── scoutfs.py           # ScoutFS filesystem support
```

The codebase is transitioning from `tellus.core.cli` to `tellus.cli` for CLI organization.

## Architecture Guidelines

**Entity vs Infrastructure Classes**
- ALWAYS prefer the new clean architecture when available:
  - Domain entities: `tellus.domain.entities.*` (pure business logic)
  - Application services: `tellus.application.services.*` (orchestration)
  - Infrastructure: `tellus.location.location.Location` (filesystem operations)
  
**Location Abstractions**
- Use `LocationEntity` for pure domain logic
- Use `Location.fs` property for filesystem operations (returns `PathSandboxedFileSystem`)
- NEVER create fsspec filesystems directly - use the provided abstractions

**Service Layer Pattern**
- New architecture uses service layer with DTOs
- Enable with feature flags: `TELLUS_USE_NEW_*_SERVICE=true`
- Legacy bridge handles compatibility between old and new systems

## ⚠️ Known Architectural Issues

**Location Association Gap**: The new simulation service architecture does not yet handle location associations (simulation-location relationships). Currently, the CLI falls back to using the legacy `Simulation.get_simulation()` object for `add_location` operations. This creates a mixed architecture where:

- Simulation listing/creation uses new service (with feature flags)
- Location associations still use legacy objects
- Need to implement location association support in new SimulationService

This should be addressed before the new architecture becomes the default.
- You should be consistently using questionary for interactive prompts in the wizards. Do not use rich or click based interactivity
- Remember that you do not need to use these TELLUS_USE_NEW variables ever again.
- Can you please forget about the old structure entirely before we had the "new" and "old" architecture? Anything that is old is wrong, by definition.

- You do not need to set TELLUS_USE_NEW_ARCHIVE_SERVICE
- When changing internals and need to modify something on the objects, make a deprecation warning to be easy to remove this later on.