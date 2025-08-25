"""Main CLI entry point for Tellus."""

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


def main():
    """Entry point for the application script."""
    # Import subcommands here to avoid circular imports
    from tellus.simulation.cli import simulation
    from tellus.location.cli import location
    from tellus.workflow.cli import workflow_cli
    from tellus.archive.cli import archive
    from tellus.tui.cli import tui

    # Add subcommands
    cli.add_command(simulation)
    cli.add_command(location)
    cli.add_command(workflow_cli)
    cli.add_command(archive)
    cli.add_command(tui)

    # Run the CLI
    cli()


if __name__ == "__main__":
    main()
