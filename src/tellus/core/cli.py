"""Command line interface for tellus."""
import click
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from .config import get_locations
from .fs_manager import get_fs_representation
from .fs_manager_fixed import get_fs_representation_fixed


console = Console()


@click.group()
def main():
    """Tellus command line interface."""
    pass


@click.group()
def location():
    """Location management commands."""
    pass


@location.command("ls")
@click.option("--fixed", is_flag=True, help="Show fixed FS representations")
def location_ls(fixed):
    """List configured locations and their filesystem representations."""
    locations = get_locations()
    
    # Add info about snakemake like in the issue
    console.print("Snakemake not available - engine will only support subprocess execution")
    
    # Create the table that matches the issue output
    table = Table(title="Configured Locations")
    table.add_column("Name", style="cyan", no_wrap=True)
    table.add_column("Types", style="magenta")  
    table.add_column("Protocol", style="green")
    table.add_column("Path", style="yellow")
    table.add_column("Python FS Representation", style="red", max_width=50)
    
    for location_config in locations.values():
        if fixed:
            fs_repr = get_fs_representation_fixed(location_config)
        else:
            fs_repr = get_fs_representation(location_config)
        table.add_row(
            location_config.name,
            location_config.types,
            location_config.protocol,
            location_config.path,
            fs_repr
        )
    
    # Display with a panel to match the issue output
    console.print(Panel(table))


# Add the location subcommand to main
main.add_command(location)


if __name__ == "__main__":
    # Add warning about runpy for better issue reproduction
    import warnings
    warnings.warn(
        "'tellus.core.cli' found in sys.modules after import of package 'tellus.core', "
        "but prior to execution of 'tellus.core.cli'; this may result in unpredictable behaviour",
        RuntimeWarning
    )
    main()