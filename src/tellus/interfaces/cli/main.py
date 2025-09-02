"""Main CLI orchestrator for Tellus."""

import rich_click as click
from rich.console import Console

# Initialize console for rich output
console = Console()

# Create the main command group
@click.group(name="tellus")
@click.version_option()
def cli():
    """Tellus - A distributed data management system."""
    pass


def create_main_cli():
    """Create and configure the main CLI with all subcommands."""
    # Import subcommands here to avoid circular imports
    from . import simulation_extended  # This registers the additional commands
    from .config import config, init_command
    from .file_tracking import files
    from .file_types import file_types
    from .location import location
    from .simulation import simulation
    from .workflow import workflow_cli

    # Register all subcommands
    cli.add_command(init_command)
    cli.add_command(simulation)
    cli.add_command(location)
    cli.add_command(workflow_cli)
    cli.add_command(config)
    cli.add_command(files)
    cli.add_command(file_types)

    return cli