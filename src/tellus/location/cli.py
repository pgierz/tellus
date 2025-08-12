from ..core.cli import cli, console

import rich_click as click
from rich.panel import Panel
from rich.table import Table
import paramiko
import fsspec

from .location import Location, LocationKind, LocationExistsError


def _get_improved_fs_representation(loc: Location) -> str:
    """Get improved filesystem representation that avoids common SSH/JSON errors."""
    protocol = loc.config.get("protocol", "file")
    
    try:
        if protocol == "sftp":
            return _get_sftp_representation_fixed(loc)
        elif protocol == "file":
            return _get_file_representation_fixed(loc)
        else:
            return f"Protocol: {protocol} (untested)"
    except Exception as e:
        return f"Error: {str(e)}"


def _get_sftp_representation_fixed(loc: Location) -> str:
    """Get improved SFTP filesystem representation with proper error handling."""
    try:
        storage_opts = loc.config.get("storage_options", {})
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        
        # Support password or key-based authentication
        connect_kwargs = {
            "hostname": storage_opts.get("host", storage_opts.get("hostname", loc.name)),
            "username": storage_opts.get("username", ""),
            "timeout": 5,
        }
        
        password = storage_opts.get("password")
        key_filename = storage_opts.get("key_filename")
        if password:
            connect_kwargs["password"] = password
        elif key_filename:
            connect_kwargs["key_filename"] = key_filename
        else:
            # Try passwordless connection (common in HPC environments)
            pass
            
        client.connect(**connect_kwargs)
        
        # Test the connection using SFTP protocol
        sftp = client.open_sftp()
        current_path = sftp.getcwd()
        sftp.close()
        client.close()
        
        return f"SFTP connection successful. Current path: {current_path}"
        
    except paramiko.AuthenticationException:
        return "Authentication failed. Check credentials or SSH keys."
    except paramiko.SSHException as e:
        return f"SSH connection error: {str(e)}"
    except Exception as e:
        return f"Connection error: {str(e)}"


def _get_file_representation_fixed(loc: Location) -> str:
    """Get improved file filesystem representation."""
    try:
        storage_opts = loc.config.get("storage_options", {})
        host = storage_opts.get("host", "localhost")
        path = storage_opts.get("path", loc.config.get("path", ""))
        
        # Return a user-friendly representation instead of raw JSON
        if path:
            return f"Local filesystem: {host}:{path}"
        else:
            return f"Local filesystem: {host} (no path specified)"
            
    except Exception as e:
        return f"Error creating filesystem: {str(e)}"


@cli.group()
def location():
    """Manage storage locations"""
    pass


@location.command(name="list")
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
        
        # Use improved representation that avoids common errors
        fs = _get_improved_fs_representation(loc)
        table.add_row(loc.name, types, protocol, path, fs)

    console.print(Panel.fit(table))


@location.command()
@click.argument("name", required=False)
@click.option(
    "-p", "--protocol", default=None, help="Storage protocol (e.g., file, s3, gs)"
)
@click.option(
    "-k",
    "--kind",
    default=None,
    help=f"Location type: {', '.join(e.name.lower() for e in LocationKind)}",
)
@click.option(
    "--optional/--not-optional",
    "is_optional",
    is_flag=True,
    default=None,
    help="Mark location as optional",
)
@click.option("--host", help="Hostname or IP for the storage")
@click.option("--port", type=int, help="Port number for the storage service")
@click.option("--path", help="Base path for the storage location")
@click.option("--username", help="Username for authentication")
@click.option("--password", help="Password for authentication")
def create(name, protocol, kind, is_optional, **storage_options):
    """Create a new storage location.
    
    If no arguments are provided, an interactive wizard will guide you through the process.
    """
    # Load existing locations first
    Location.load_locations()

    # Interactive wizard when no name is provided
    if name is None:
        console.print("\n[bold blue]📍 Location Creation Wizard[/bold blue]")
        console.print(
            "Let's create a new storage location. Press Ctrl+C to cancel at any time.\n"
        )

        import questionary

        # Get location name
        name = questionary.text(
            "Enter a name for this location:",
            validate=lambda x: len(x.strip()) > 0 or "Name cannot be empty"
        ).ask()
        
        if not name:
            console.print("\nOperation cancelled.")
            raise click.Abort(1)

        # Check if location already exists
        if Location.get_location(name):
            override = questionary.confirm(
                f"Location '{name}' already exists. Override?",
                default=False
            ).ask()
            if not override:
                console.print("Operation cancelled.")
                return

        # Get protocol
        protocol_choices = [
            {"name": "file - Local filesystem", "value": "file"},
            {"name": "sftp - SSH File Transfer Protocol", "value": "sftp"},
            {"name": "scoutfs - ScoutFS with tape staging", "value": "scoutfs"},
            {"name": "s3 - Amazon S3", "value": "s3"},
            {"name": "gs - Google Cloud Storage", "value": "gs"},
            {"name": "azure - Azure Blob Storage", "value": "azure"},
            {"name": "ftp - File Transfer Protocol", "value": "ftp"},
        ]

        protocol = questionary.select(
            "Select storage protocol:",
            choices=protocol_choices,
        ).ask()

        if not protocol:
            console.print("\nOperation cancelled.")
            raise click.Abort(1)

        # Get location kind
        kind_choices = [
            {"name": f"{kind.name.lower()} - {kind.name}", "value": kind.name.lower()}
            for kind in LocationKind
        ]

        kind = questionary.select(
            "Select location type:",
            choices=kind_choices,
        ).ask()

        if not kind:
            console.print("\nOperation cancelled.")
            raise click.Abort(1)

        # Get optional flag
        is_optional = questionary.confirm(
            "Is this location optional?",
            default=False
        ).ask()

        if is_optional is None:
            console.print("\nOperation cancelled.")
            raise click.Abort(1)

        # Protocol-specific configuration
        storage_opts = {}
        
        if protocol == "file":
            from prompt_toolkit import prompt
            from prompt_toolkit.completion import PathCompleter

            path_completer = PathCompleter(expanduser=True, only_directories=True)
            path = prompt(
                "Enter base path (press Tab to complete): ",
                completer=path_completer,
                complete_while_typing=True,
            )
            if path:
                storage_opts["path"] = path

        elif protocol in ["sftp", "scoutfs", "ftp"]:
            host = questionary.text("Enter hostname:").ask()
            if host:
                storage_opts["host"] = host
            
            port_str = questionary.text(
                f"Enter port (default: {22 if protocol in ['sftp', 'scoutfs'] else 21}):",
                default=""
            ).ask()
            if port_str:
                try:
                    storage_opts["port"] = int(port_str)
                except ValueError:
                    console.print("[yellow]Invalid port, using default[/yellow]")

            username = questionary.text("Enter username (optional):").ask()
            if username:
                storage_opts["username"] = username

            password = questionary.password("Enter password (optional):").ask()
            if password:
                storage_opts["password"] = password

            from prompt_toolkit import prompt
            from prompt_toolkit.completion import PathCompleter

            path_completer = PathCompleter(expanduser=True, only_directories=True)
            path = prompt(
                "Enter remote base path: ",
                completer=path_completer,
                complete_while_typing=True,
            )
            if path:
                storage_opts["path"] = path

        elif protocol in ["s3", "gs", "azure"]:
            console.print(f"\n[bold]{protocol.upper()} Configuration[/bold]")
            console.print("Note: Credentials should be configured via environment variables or cloud SDK")
            
            if protocol == "s3":
                bucket = questionary.text("Enter S3 bucket name:").ask()
                if bucket:
                    storage_opts["path"] = f"s3://{bucket}"
                    
            elif protocol == "gs":
                bucket = questionary.text("Enter GCS bucket name:").ask()
                if bucket:
                    storage_opts["path"] = f"gs://{bucket}"
                    
            elif protocol == "azure":
                container = questionary.text("Enter Azure container name:").ask()
                account = questionary.text("Enter Azure storage account name:").ask()
                if container and account:
                    storage_opts["path"] = f"az://{container}"
                    storage_opts["account_name"] = account

        # Show summary
        console.print("\n[bold]Summary:[/bold]")
        console.print(f"  [bold]Name:[/bold] {name}")
        console.print(f"  [bold]Protocol:[/bold] {protocol}")
        console.print(f"  [bold]Kind:[/bold] {kind}")
        console.print(f"  [bold]Optional:[/bold] {is_optional}")
        if storage_opts:
            console.print(f"  [bold]Configuration:[/bold]")
            for key, value in storage_opts.items():
                display_value = "*****" if key == "password" else value
                console.print(f"    • {key}: {display_value}")

        confirm = questionary.confirm(
            "\nCreate this location?",
            default=True,
        ).ask()
        
        if not confirm:
            console.print("Operation cancelled.")
            return

        # Update storage_options with collected values
        storage_options.update({k: v for k, v in storage_opts.items()})

    # Validate name
    if Location.get_location(name) and not questionary.confirm(f"Location '{name}' exists. Override?", default=False).ask():
        raise click.UsageError(f"Location '{name}' already exists. Use 'update' to modify it.")

    # Set defaults if not provided
    if protocol is None:
        protocol = "file"
    if kind is None:
        kind = "disk"
    if is_optional is None:
        is_optional = False

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
