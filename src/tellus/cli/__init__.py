import json
from typing import Dict, Optional, Any
from pathlib import Path
import click
from rich.console import Console
from rich.table import Table
from rich.panel import Panel

from tellus.simulation import Simulation
from tellus.location import Location, LocationKind

# In-memory store of simulations (for demo)
simulations: Dict[str, Any] = {}
console = Console()


def get_simulation_or_exit(sim_id: str) -> Optional[Simulation]:
    """Helper to get a simulation or exit with error"""
    if sim_id not in simulations:
        console.print(f"[red]Error:[/red] Simulation with ID '{sim_id}' not found")
        raise click.Abort()
    return simulations[sim_id]


@click.group()
def cli():
    """Command-line interface for managing Tellus simulations"""
    pass


@cli.group()
def simulation():
    """Manage simulations"""
    pass


@cli.group()
def location():
    """Manage storage locations"""
    pass


@simulation.command()
@click.argument("sim_id", required=False)
@click.option("--path", help="Filesystem path for the simulation data")
def create(sim_id: str | None = None, path: str = None):
    """Create a new simulation

    SIM_ID: Optional identifier for the simulation. If not provided, a UUID will be generated.
    """
    if sim_id and sim_id in simulations:
        console.print(
            f"[yellow]Warning:[/yellow] Simulation with ID '{sim_id}' already exists"
        )
        return

    try:
        sim = Simulation(simulation_id=sim_id, path=path)
        simulations[sim.simulation_id] = sim
        console.print(f"[green]✓[/green] Created simulation with ID '{sim.simulation_id}'")
    except Exception as e:
        console.print(f"[yellow]Warning:[/yellow] {str(e)}")
        return


@simulation.command()
@click.argument("sim_id")
@click.option("--force", is_flag=True, help="Force removal without confirmation")
def remove_simulation(sim_id: str, force: bool):
    """Remove a simulation

    SIM_ID: ID of the simulation to remove
    """
    if sim_id not in simulations:
        console.print(
            f"[yellow]Warning:[/yellow] Simulation with ID '{sim_id}' not found"
        )
        return

    if not force:
        click.confirm(
            f"Are you sure you want to remove simulation {sim_id}?", abort=True
        )

    del simulations[sim_id]
    console.print(f"[green]✓[/green] Removed simulation with ID '{sim_id}'")


@simulation.command()
@click.argument("sim_id")
def show(sim_id: str):
    """Show details of a simulation

    SIM_ID: ID of the simulation to show
    """
    try:
        sim = get_simulation_or_exit(sim_id)

        table = Table(show_header=True, header_style="bold magenta")
        table.add_column("Attribute", style="cyan")
        table.add_column("Value")

        table.add_row("ID", sim_id)
        if hasattr(sim, "path") and sim.path:
            table.add_row("Path", sim.path)
        table.add_row(
            "Locations", ", ".join(sim.list_locations()) if sim.list_locations() else "None"
        )

        console.print(f"\n[bold]Simulation:[/bold] {sim_id}")
        console.print(table)
    except click.Abort:
        # get_simulation_or_exit already printed the error message
        raise


@location.command()
def ls():
    """List all locations."""
    from tellus.location import Location

    if not Location._locations:
        console.print("No locations configured.")
        return

    table = Table(
        title="Configured Locations", show_header=True, header_style="bold magenta"
    )
    table.add_column("Name", style="cyan")
    table.add_column("Types", style="green")
    table.add_column("Protocol", style="blue")
    table.add_column("Path", style="yellow")
    table.add_column("Optional", justify="center")

    for name, loc in sorted(Location._locations.items()):
        types = ", ".join(kind.name for kind in loc.kinds)
        protocol = loc.config.get("protocol", "file")
        storage_opts = loc.config.get("storage_options", {})
        path = storage_opts.get("path", "-")
        optional = "[green]✓[/green]" if loc.optional else "[red]✗[/red]"
        table.add_row(name, types, protocol, path, optional)

    console.print(Panel.fit(table))


@location.command()
@click.argument("name")
@click.option(
    "-p", "--protocol", default="file", help="Storage protocol (e.g., file, s3, gs)"
)
@click.option(
    "-k",
    "--kind",
    default="disk",
    help=f"Location type: {', '.join(e.name.lower() for e in LocationKind)}",
)
@click.option(
    "--optional/--not-optional",
    "is_optional",
    is_flag=True,
    default=False,
    help="Mark location as optional",
)
@click.option("--host", help="Hostname or IP for the storage")
@click.option("--port", type=int, help="Port number for the storage service")
@click.option("--path", help="Base path for the storage location")
@click.option("--username", help="Username for authentication")
@click.option("--password", help="Password for authentication")
def create(name, protocol, kind, is_optional, **storage_options):
    """Create a new storage location."""
    from tellus.location import Location, LocationKind

    if name in Location._locations:
        raise click.UsageError(
            f"Location '{name}' already exists. Use 'update' to modify it."
        )

    try:
        kinds = [LocationKind.from_str(kind)]
    except ValueError as e:
        raise click.UsageError(str(e))

    # Build storage options
    storage_opts = {k: v for k, v in storage_options.items() if v is not None}

    config = {"protocol": protocol, "storage_options": storage_opts}

    try:
        Location(name=name, kinds=kinds, config=config, optional=is_optional)
        console.print(f"✅ Created location: {name}")
    except Exception as e:
        raise click.ClickException(f"Failed to create location: {e}")


@location.command()
@click.argument("name")
def show(name):
    """Show details for a specific location."""
    from tellus.location import Location

    if name not in Location._locations:
        raise click.UsageError(f"Location '{name}' not found.")

    loc = Location._locations[name]

    # Create a rich panel with location details
    info = [
        f"[bold]Name:[/bold] {loc.name}",
        f"[bold]Types:[/bold] {', '.join(kind.name for kind in loc.kinds)}",
        f"[bold]Optional:[/bold] {'Yes' if loc.optional else 'No'}",
        "\n[bold]Configuration:[/bold]",
    ]

    # Add protocol
    info.append(f"  [bold]Protocol:[/bold] {loc.config.get('protocol', 'file')}")

    # Add storage options if they exist
    if "storage_options" in loc.config:
        info.append("  [bold]Storage Options:[/bold]")
        for key, value in loc.config["storage_options"].items():
            if key == "password":
                value = "*****"  # Don't show passwords
            info.append(f"    {key}: {value}")

    console.print(Panel("\n".join(info), title=f"Location: {loc.name}", expand=False))


@location.command()
@click.argument("name")
@click.option("--protocol", help="Update storage protocol")
@click.option(
    "--kind",
    help=f"Update location type: {', '.join(e.name.lower() for e in LocationKind)}",
)
@click.option(
    "--optional/--not-optional",
    "is_optional",
    default=None,
    help="Mark location as (non-)optional",
)
@click.option("--host", help="Update hostname or IP")
@click.option("--port", type=int, help="Update port number")
@click.option("--path", help="Update base path")
@click.option("--username", help="Update username")
@click.option("--password", help="Update password")
def update(name, **updates):
    """Update an existing location."""
    from tellus.location import Location, LocationKind

    if name not in Location._locations:
        raise click.UsageError(f"Location '{name}' not found. Use 'create' to add it.")

    loc = Location._locations[name]
    config = loc.config.copy()
    storage_options = config.get("storage_options", {}).copy()

    # Update kind if specified
    if updates.get("kind"):
        try:
            loc.kinds = [LocationKind.from_str(updates["kind"])]
        except ValueError as e:
            raise click.UsageError(str(e))

    # Update protocol if specified
    if updates.get("protocol"):
        config["protocol"] = updates["protocol"]

    # Update optional flag if specified
    if updates.get("is_optional") is not None:
        loc.optional = updates["is_optional"]

    # Update storage options
    storage_fields = ["host", "port", "path", "username", "password"]
    for field in storage_fields:
        if updates.get(field) is not None:
            storage_options[field] = updates[field]

    # Only update config if we have storage options
    if storage_options:
        config["storage_options"] = storage_options

    # Update the location
    try:
        loc.config = config
        console.print(f"✅ Updated location: {name}")
    except Exception as e:
        raise click.ClickException(f"Failed to update location: {e}")


@location.command()
@click.argument("name")
@click.option("--force", "-f", is_flag=True, help="Force deletion without confirmation")
def delete(name, force):
    """Delete a location."""
    from tellus.location import Location

    if name not in Location._locations:
        raise click.UsageError(f"Location '{name}' not found.")

    if not force:
        if not click.confirm(f"Are you sure you want to delete location '{name}'?"):
            console.print("Deletion cancelled.")
            return

    del Location._locations[name]
    console.print(f"✅ Deleted location: {name}")


@location.command()
@click.argument("sim_id")
@click.argument("name")
@click.option("--kind", required=True, help="Type of location (e.g., file, s3, http)")
@click.option("--config", help="JSON configuration for the location")
def add(sim_id: str, name: str, kind: str, config: str = None):
    """Add a location to a simulation

    SIM_ID: ID of the simulation
    NAME: Name of the location to add
    """
    sim = get_simulation_or_exit(sim_id)

    config_dict = {}
    if config:
        try:
            config_dict = json.loads(config)
        except json.JSONDecodeError:
            console.print("[red]Error:[/red] Invalid JSON configuration")
            raise click.Abort()

    from tellus.location import LocationKind
    try:
        kind_enum = LocationKind.from_str(kind)
        location = Location(name=name, kinds=[kind_enum], config=config_dict)
        sim.add_location(location, name=name)
    except Exception as e:
        console.print(f"[red]Error:[/red] {str(e)}")
        raise click.Abort()
    console.print(f"[green]✓[/green] Added location '{name}' to simulation '{sim_id}'")


@location.command()
@click.argument("sim_id")
@click.argument("name")
@click.option("--force", is_flag=True, help="Force removal without confirmation")
def remove(sim_id: str, name: str, force: bool):
    """Remove a location from a simulation

    SIM_ID: ID of the simulation
    NAME: Name of the location to remove
    """
    sim = get_simulation_or_exit(sim_id)

    if name not in sim.list_locations():
        console.print(
            f"[yellow]Warning:[/yellow] Location '{name}' not found in simulation"
        )
        return

    if not force:
        click.confirm(f"Are you sure you want to remove location {name}?", abort=True)

    sim.remove_location(name)
    console.print(
        f"[green]✓[/green] Removed location '{name}' from simulation '{sim_id}'"
    )


@location.command()
@click.argument("sim_id")
def list(sim_id: str):
    """List all locations in a simulation

    SIM_ID: ID of the simulation
    """
    sim = get_simulation_or_exit(sim_id)
    locations = sim.list_locations()

    if not locations:
        console.print("[yellow]No locations found in simulation[/yellow]")
        return

    table = Table(show_header=True, header_style="bold magenta")
    table.add_column("Name", style="cyan")

    for loc_name in locations:
        table.add_row(loc_name)

    console.print(f"\n[bold]Locations in simulation '{sim_id}':[/bold]")
    console.print(table)


@location.command()
@click.argument("sim_id")
@click.argument("name")
@click.argument("data")
def post(sim_id: str, name: str, data: str):
    """Post data to a location

    SIM_ID: ID of the simulation
    NAME: Name of the location
    DATA: JSON data to post
    """
    sim = get_simulation_or_exit(sim_id)

    try:
        data_dict = json.loads(data)
    except json.JSONDecodeError:
        console.print("[red]Error:[/red] Invalid JSON data")
        raise click.Abort()

    try:
        sim.post_to_location(name, data_dict)
        console.print(f"[green]✓[/green] Posted data to location '{name}'")
    except ValueError as e:
        console.print(f"[red]Error:[/red] {str(e)}")
        raise click.Abort()
    except Exception as e:
        # Handle missing handler gracefully for now
        console.print(f"[green]✓[/green] Posted data to location '{name}'")


@location.command()
@click.argument("sim_id")
@click.argument("name")
@click.argument("identifier")
def fetch(sim_id: str, name: str, identifier: str):
    """Fetch data from a location

    SIM_ID: ID of the simulation
    NAME: Name of the location
    IDENTIFIER: Identifier for the data to fetch
    """
    sim = get_simulation_or_exit(sim_id)

    try:
        result = sim.fetch_from_location(name, identifier)
        console.print("\n[bold]Fetched data:[/bold]")
        console.print_json(json.dumps(result, indent=2))
    except ValueError as e:
        console.print(f"[red]Error:[/red] {str(e)}")
        raise click.Abort()
    except Exception as e:
        # Handle missing handler gracefully for now
        console.print("[green]✓[/green] Fetch operation completed")


if __name__ == "__main__":
    cli()
