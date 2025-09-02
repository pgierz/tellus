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
    # TUI functionality now fixed - .
    # Import extended simulation commands
    from . import simulation_extended  # This registers the additional commands
    # Configuration management
    from .config import config, init_command
    # File tracking - .
    from .file_tracking import files
    # File type configuration - .
    from .file_types import file_types
    from .location import location
    from .simulation import simulation
    from .workflow import workflow_cli

    # Add working subcommands
    cli.add_command(init_command)
    cli.add_command(simulation)
    cli.add_command(location)
    cli.add_command(workflow_cli)
    cli.add_command(config)
    cli.add_command(files)  # Add file tracking commands
    cli.add_command(file_types)  # Add file type configuration commands

    return cli