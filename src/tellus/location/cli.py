from ..core.cli import cli, console

import rich_click as click
from rich.panel import Panel
from rich.table import Table

from .location import Location, LocationKind, LocationExistsError


@cli.group()
def location():
    """Manage storage locations"""
    pass


@location.command(name="ls")
def list_locations():
    """List all locations."""
    # Load locations from disk
    Location.load_locations()
    locations = Location.list_locations()

    if not locations:
        console.print("No locations configured.")
        return

    table = Table(
        title="Configured Locations", show_header=True, header_style="bold magenta"
    )
    table.add_column("Name", style="cyan")
    table.add_column("Types", style="green")
    table.add_column("Protocol", style="blue")
    table.add_column("Path", style="yellow")
    table.add_column("Python FS Representation", justify="center")

    for loc in sorted(locations, key=lambda x: x.name):
        types = ", ".join(kind.name for kind in loc.kinds)
        protocol = loc.config.get("protocol", "file")
        storage_opts = loc.config.get("storage_options", {})
        path = storage_opts.get("path", "-")
        try:
            fs = loc.fs.to_json()
        except Exception as e:
            fs = str(e)
        table.add_row(loc.name, types, protocol, path, fs)

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
    # Load existing locations first
    Location.load_locations()

    if Location.get_location(name):
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
    except LocationExistsError as e:
        raise click.UsageError(str(e))
    except Exception as e:
        raise click.ClickException(f"Failed to create location: {e}")


@location.command(name="show")
@click.argument("name")
def show_location(name):
    """Show details for a specific location."""
    # Load locations from disk
    Location.load_locations()
    loc = Location.get_location(name)

    if not loc:
        raise click.UsageError(f"Location '{name}' not found.")

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


@location.command(name="update")
@click.argument("name")
@click.option("-p", "--protocol", help="Storage protocol (e.g., file, s3, gs)")
@click.option(
    "-k",
    "--kind",
    help=f"Location type: {', '.join(e.name.lower() for e in LocationKind)}",
)
@click.option(
    "--optional/--not-optional",
    "is_optional",
    default=None,
    help="Mark location as (non-)optional",
)
@click.option("--host", help="Hostname or IP for the storage")
@click.option("--port", type=int, help="Port number for the storage service")
@click.option("--path", help="Base path for the storage location")
@click.option("--username", help="Username for authentication")
@click.option("--password", help="Password for authentication")
@click.option("--remove-option", multiple=True, help="Remove a configuration option")
def update_location(name, **updates):
    """Update an existing location."""
    # Load existing locations first
    Location.load_locations()
    loc = Location.get_location(name)

    if not loc:
        raise click.UsageError(f"Location '{name}' not found.")

    # Handle removal of options
    remove_options = updates.pop("remove_option", [])
    for opt in remove_options:
        if opt in loc.config:
            del loc.config[opt]
        elif "storage_options" in loc.config and opt in loc.config["storage_options"]:
            del loc.config["storage_options"][opt]

    # Update protocol if provided
    if "protocol" in updates and updates["protocol"] is not None:
        loc.config["protocol"] = updates["protocol"]

    # Update storage options
    storage_opts = loc.config.get("storage_options", {})
    for key in ["host", "port", "path", "username", "password"]:
        if key in updates and updates[key] is not None:
            storage_opts[key] = updates[key]

    if storage_opts:  # Only update if there are storage options
        loc.config["storage_options"] = storage_opts

    # Update kind if provided
    if "kind" in updates and updates["kind"] is not None:
        try:
            loc.kinds = [LocationKind.from_str(updates["kind"])]
        except ValueError as e:
            raise click.UsageError(str(e))

    # Update optional flag if provided
    if updates.get("is_optional") is not None:
        loc.optional = updates["is_optional"]

    # Save the changes
    try:
        loc._save_locations()
        console.print(f"✅ Updated location: {name}")
    except Exception as e:
        raise click.ClickException(f"Failed to update location: {e}")


@location.command(name="delete")
@click.argument("name")
@click.option("--force", is_flag=True, help="Force deletion without confirmation")
def delete_location(name, force):
    """Delete a location."""
    # Load existing locations first
    Location.load_locations()

    if not Location.get_location(name):
        raise click.UsageError(f"Location '{name}' not found.")

    if not force and not click.confirm(
        f"Are you sure you want to delete location '{name}'?"
    ):
        console.print("Operation cancelled.")
        return

    try:
        Location.remove_location(name)
        console.print(f"✅ Deleted location: {name}")
    except Exception as e:
        raise click.ClickException(f"Failed to delete location: {e}")
