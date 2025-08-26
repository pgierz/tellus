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
    from .simulation import simulation
    # Import extended simulation commands
    from . import simulation_extended  # This registers the additional commands
    from .location import location
    from .archive import archive
    from .workflow import workflow_cli
    # TUI functionality now fixed - using clean architecture
    from ..tui.main import create_tui_commands
    
    # File tracking - using clean architecture
    from .file_tracking import files
    
    # Progress tracking - using clean architecture
    from .progress import progress
    
    # File type configuration - using clean architecture
    from .file_types import file_types
    
    # File transfer - using clean architecture
    from .transfer import transfer, queue_group
    
    # Configuration management
    from .config import config

    # Add working subcommands
    cli.add_command(simulation)
    cli.add_command(location)
    cli.add_command(archive)
    cli.add_command(workflow_cli)
    cli.add_command(config)
    # TUI functionality now fixed and re-enabled
    tui_commands = create_tui_commands()
    cli.add_command(tui_commands)
    cli.add_command(files)  # Add file tracking commands
    cli.add_command(progress)  # Add progress tracking commands
    cli.add_command(file_types)  # Add file type configuration commands
    cli.add_command(transfer)  # Add file transfer commands
    cli.add_command(queue_group)  # Add queue management commands

    return cli