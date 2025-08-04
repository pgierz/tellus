"""Command-line interface for Tellus simulation management."""

import os
from pathlib import Path
from typing import Optional, List, Union, Tuple, Dict, Any

import rich_click as click
from rich.console import Console
from rich.text import Text
from rich.table import Table
from rich.panel import Panel
from rich.prompt import Prompt, Confirm
from rich import print as rprint

from .simulation import Simulation, SimulationExistsError
from ..core.cli import cli, console

import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Load simulations at module import
Simulation.load_simulations()


def get_simulation_or_exit(sim_id: str) -> Simulation:
    """Helper to get a simulation or exit with error"""
    sim = Simulation.get_simulation(sim_id)
    if not sim:
        console.print(f"[red]Error:[/red] Simulation with ID '{sim_id}' not found")
        raise click.Abort(1)
    return sim


@cli.group()
def simulation():
    """Manage simulations"""
    pass


@simulation.command(name="list")
def list_simulations():
    """List all simulations."""
    simulations = Simulation.list_simulations()
    if not simulations:
        console.print("No simulations found.")
        return

    table = Table(
        title="Available Simulations", show_header=True, header_style="bold magenta"
    )
    table.add_column("ID", style="cyan")
    table.add_column("Path", style="green")
    table.add_column("# Locations", style="blue")
    table.add_column("Attributes", style="yellow")

    for sim in sorted(simulations, key=lambda s: s.simulation_id):
        path = str(sim.path) if sim.path else "-"
        num_locations = len(sim.locations)
        attrs = ", ".join(sim.attrs.keys()) if sim.attrs else "-"
        table.add_row(sim.simulation_id, path, str(num_locations), attrs)

    console.print(Panel.fit(table))


@simulation.command()
@click.argument("sim_id", required=False)
@click.option(
    "--path",
    type=click.Path(file_okay=False, dir_okay=True, resolve_path=True),
    help="Filesystem path for the simulation data",
)
@click.option(
    "--attr",
    multiple=True,
    nargs=2,
    type=(str, str),
    help="Additional attributes as key=value pairs",
)
def create(
    sim_id: Optional[str] = None,
    path: Optional[str] = None,
    attr: Optional[List[tuple]] = None,
):
    """Create a new simulation.

    SIM_ID: Optional identifier for the simulation. If not provided, a UUID will be generated.
    """
    try:
        # Convert path to Path object if provided
        path_obj = Path(path).resolve() if path else None

        # Create simulation (this will add it to the Simulation._simulations dict)
        sim = Simulation(simulation_id=sim_id, path=str(path_obj) if path_obj else None)

        # Add attributes if provided
        if attr:
            for key, value in attr:
                sim.attrs[key] = value

        # Save all simulations to persist the new one
        Simulation.save_simulations()

        console.print(
            Panel.fit(
                f"✅ [bold green]Created simulation:[/bold green] {sim.simulation_id}\n"
                f"[bold]Path:[/bold] {path_obj or 'Not specified'}\n"
                f"[bold]Attributes:[/bold] {', '.join(f'{k}={v}' for k, v in sim.attrs.items()) if sim.attrs else 'None'}",
                title="Simulation Created",
            )
        )
    except SimulationExistsError as e:
        console.print(f"[red]Error:[/red] {str(e)}")
        raise click.Abort(1)
    except Exception as e:
        console.print(f"[red]Error creating simulation:[/red] {str(e)}")
        raise click.Abort(1)


@simulation.command()
@click.argument("sim_id")
def show(sim_id: str):
    """Show details of a simulation.

    SIM_ID: ID of the simulation to show
    """
    sim = get_simulation_or_exit(sim_id)

    # Basic info
    info = [
        f"[bold]ID:[/bold] {sim.simulation_id}",
        f"[bold]Path:[/bold] {sim.path or 'Not specified'}",
    ]

    # Attributes
    if sim.attrs:
        attrs = "\n  ".join(f"{k}: {v}" for k, v in sim.attrs.items())
        info.append(f"\n[bold]Attributes:[/bold]\n  {attrs}")

    # Locations
    if sim.locations:
        locations = "\n  ".join(
            f"{name}: {loc.get('type', 'unknown')}"
            for name, loc in sim.locations.items()
        )
        info.append(f"\n[bold]Locations:[/bold]\n  {locations}")

    console.print(
        Panel(
            "\n".join(info),
            title=f"Simulation: {sim_id}",
            border_style="blue",
            expand=False,
        )
    )


@simulation.command()
@click.argument("sim_id")
@click.option(
    "--path",
    type=click.Path(file_okay=False, dir_okay=True, resolve_path=True),
    help="Update the simulation path",
)
@click.option(
    "--attr",
    multiple=True,
    nargs=2,
    type=(str, str),
    help="Add or update attributes (key value)",
)
@click.option("--remove-attr", multiple=True, help="Remove an attribute")
def update(
    sim_id: str,
    path: Optional[str] = None,
    attr: Optional[List[tuple]] = None,
    remove_attr: Optional[List[str]] = None,
):
    """Update an existing simulation.

    SIM_ID: ID of the simulation to update
    """
    sim = get_simulation_or_exit(sim_id)

    updates = []

    # Update path if provided
    if path is not None:
        path_obj = Path(path).resolve()
        sim.path = str(path_obj)
        updates.append(f"Path: {path_obj}")

    # Update attributes
    if attr:
        for key, value in attr:
            sim.attrs[key] = value
            updates.append(f"Added/Updated attribute: {key}={value}")

    # Remove attributes
    if remove_attr:
        for key in remove_attr:
            if key in sim.attrs:
                del sim.attrs[key]
                updates.append(f"Removed attribute: {key}")

    if not updates:
        console.print(
            "[yellow]No updates specified. Use --path, --attr, or --remove-attr to make changes.[/yellow]"
        )
        return

    # Save the updated simulation
    Simulation.save_simulations()

    # Show what was updated
    console.print(
        Panel.fit(
            f"✅ [bold green]Updated simulation:[/bold green] {sim_id}\n"
            + "\n".join(f"• {update}" for update in updates),
            title="Simulation Updated",
        )
    )


@simulation.group()
def location():
    """Manage locations in a simulation"""
    pass


@location.command(name="add")
@click.argument("sim_id", required=False)
@click.argument("location_name", required=False)
@click.option(
    "--path-prefix",
    help="Path prefix with template variables (e.g., '/path/{{model_id}}/{{simulation_id}}')",
)
@click.option("--override", is_flag=True, help="Override if location already exists")
def add_location(
    sim_id: Optional[str] = None,
    location_name: Optional[str] = None,
    path_prefix: Optional[str] = None,
    override: bool = False,
):
    """Add a location to a simulation with optional context.

    If no arguments are provided, an interactive wizard will guide you through the process.
    """
    # Interactive wizard when no arguments are provided
    if sim_id is None:
        console.print("\n[bold blue]📝 Location Wizard[/bold blue]")
        console.print(
            "Let's add a location to a simulation. Press Ctrl+C to cancel at any time.\n"
        )

        # List available simulations
        simulations = Simulation.list_simulations()
        if not simulations:
            console.print(
                "[red]Error:[/red] No simulations found. Please create a simulation first."
            )
            raise click.Abort(1)

        # Create choices for questionary
        choices = [
            {
                "name": f"{sim.simulation_id} ({sim.path or 'No path'})",
                "value": sim.simulation_id,
            }
            for sim in simulations
        ]

        # Let user select simulation using questionary
        import questionary

        sim_id = questionary.select(
            "Select a simulation:",
            choices=choices,
            # style=lambda x: "",  # Disable questionary's built-in styling
        ).ask()

        if not sim_id:  # User pressed Ctrl+C or entered empty input
            console.print("\nOperation cancelled.")
            raise click.Abort(1)

        # Get location name with option to select from existing or create new
        from ..location.location import Location as LocationModel

        # Get all existing locations
        existing_locations = LocationModel.list_locations()
        location_choices = [
            {
                "name": f"{loc.name} (Base Path: {loc.config.get('path') or 'No path'})",
                "value": loc.name,
            }
            for loc in existing_locations
        ]

        # Add option to create a new location
        location_choices.append(
            {
                "name": "Create new location...",
                "value": "__new__",
            }
        )

        # Let user select or create a location
        import questionary
        from rich.markup import escape

        # Configure questionary to use rich markup
        questionary.text.ask = lambda text, **kwargs: Prompt.ask(escape(text), **kwargs)
        questionary.confirm.ask = lambda text, **kwargs: Confirm.ask(
            escape(text), **kwargs
        )
        questionary.select.ask = lambda text, **kwargs: Select.ask(
            escape(text), **kwargs
        )

        location_choice = questionary.select(
            "\nSelect a location or create a new one:",
            choices=location_choices,
            style=questionary.Style(
                [
                    ("selected", "fg:#00FF00 bg:#000000"),
                    ("highlighted", "fg:#ffffff bg:#000000"),
                ]
            ),
        ).ask()

        if not location_choice:  # User cancelled
            console.print("\nOperation cancelled.")
            raise click.Abort(1)

        if location_choice == "__new__":
            # Get new location name
            location_name = Prompt.ask("\nEnter name for new location")
            if not location_name:
                console.print("\nOperation cancelled.")
                raise click.Abort(1)

            # Get location path
            from prompt_toolkit import prompt
            from prompt_toolkit.completion import PathCompleter

            path_completer = PathCompleter(
                expanduser=True,
            )
            location_path = prompt(
                "Enter path for the new location (press Tab to complete): ",
                completer=path_completer,
                complete_while_typing=True,
            )

            if not location_path:
                console.print("\nOperation cancelled.")
                raise click.Abort(1)

            # Create the new location
            try:
                location = LocationModel(name=location_name, path=location_path)
                location.save()
                console.print(f"\n✅ Created new location: {location_name}")
            except Exception as e:
                console.print(f"[red]Error creating location:[/red] {str(e)}")
                raise click.Abort(1)
        else:
            location_name = location_choice

        # Get path prefix (optional) with tab completion
        from prompt_toolkit import prompt
        from prompt_toolkit.completion import PathCompleter

        path_completer = PathCompleter(
            expanduser=True,
            only_directories=True,
        )
        path_prefix = (
            prompt(
                "\nEnter path prefix (press Tab to complete, Enter to skip): ",
                completer=path_completer,
                complete_while_typing=True,
            )
            or None
        )

        # Check if location already exists in simulation
        sim = Simulation.get_simulation(sim_id)
        if sim and location_name in sim.locations:
            override = Confirm.ask(
                f"[yellow]Location '{location_name}' already exists in this simulation. Override?[/yellow]"
            )
            if not override:
                console.print("Operation cancelled.")
                return

    # Validate required arguments
    if not sim_id or not location_name:
        raise click.UsageError("sim_id and location_name are required")

    # Get the simulation or exit if not found
    sim = get_simulation_or_exit(sim_id)

    try:
        # Get the location (should exist at this point)
        from ..location.location import Location

        location = Location.get_location(location_name)
        if not location:
            raise click.UsageError(
                f"Location '{location_name}' not found. This should not happen."
            )

        # Create context with path prefix if provided
        context = None
        if path_prefix:
            from .context import LocationContext

            context = LocationContext(path_prefix=path_prefix)

        # Add to simulation
        sim.add_location(location, context=context, override=override)
        Simulation.save_simulations()

        # Show success message
        console.print(
            Panel.fit(
                f"✅ [bold green]Added location to simulation {sim_id}:[/bold green] {location_name}\n"
                f"[bold]Path Prefix:[/bold] {path_prefix or 'None'}",
                title="Location Added",
            )
        )

    except Exception as e:
        console.print(f"[red]Error adding location:[/red] {str(e)}")
        raise click.Abort(1)


@location.command(name="list")
@click.argument("sim_id")
def list_locations(sim_id: str):
    """List all locations in a simulation with their context.

    SIM_ID: ID of the simulation
    """
    sim = get_simulation_or_exit(sim_id)

    if not sim.locations:
        console.print("No locations found in this simulation.")
        return

    table = Table(
        title=f"Locations in Simulation: {sim_id}",
        show_header=True,
        header_style="bold magenta",
    )
    table.add_column("Name", style="cyan")
    table.add_column("Resolved Path", style="green")
    table.add_column("Kinds", style="blue")
    table.add_column("Metadata", style="blue")

    for name, loc_data in sim.locations.items():
        location = loc_data["location"]
        context = loc_data.get("context", {})

        # Get the resolved path using get_location_path
        resolved_path = sim.get_location_path(name)
        metadata = ", ".join(
            f"{k}={v}" for k, v in getattr(context, "metadata", {}).items()
        )

        table.add_row(
            name,
            resolved_path,
            ", ".join(kind.name for kind in location.kinds),
            metadata or "",
        )

    console.print(Panel.fit(table))


def _get_remote_file(
    location,
    remote_path: str,
    local_path: Union[str, Path],
    force: bool = False,
    progress: Optional[Any] = None,
    task_id: Optional[str] = None,
) -> str:
    """Helper function to download a single file with progress tracking.

    Args:
        location: Location instance to download from
        remote_path: Path to the remote file
        local_path: Local path to save the file
        force: If True, overwrite existing files
        progress: Rich Progress instance for tracking
        task_id: Task ID for the progress bar

    Returns:
        Path to the downloaded file
    """
    local_path = Path(local_path)

    # Create a progress callback if we have a progress bar
    progress_callback = None
    if progress is not None and task_id is not None:
        # Create a callback that updates our progress bar
        from tellus.progress import FSSpecProgressCallback

        progress_callback = FSSpecProgressCallback(progress=progress, task_id=task_id)
        progress_callback.set_description(os.path.basename(remote_path))

    try:
        return location.get(
            remote_path=remote_path,
            local_path=local_path,
            overwrite=force,
            progress_callback=progress_callback,
        )
    except Exception as e:
        if progress is not None and task_id is not None:
            progress.print(f"[red]Error downloading {remote_path}: {str(e)}")
        raise


def _download_files(
    location,
    files: List[Tuple[str, Dict[str, Any]]],
    force: bool = False,
) -> Tuple[int, int]:
    """Download multiple files with progress tracking.

    Args:
        location: Location instance to download from
        files: List of (path, info) tuples to download
        force: If True, overwrite existing files

    Returns:
        Tuple of (success_count, total_count)
    """
    from rich.progress import Progress, SpinnerColumn, TextColumn
    from tellus.progress import FSSpecProgressCallback

    success_count = 0
    total_count = len(files)

    if not files:
        return 0, 0

    # Create a progress bar for the overall operation
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        transient=True,
    ) as progress:
        task = progress.add_task("Downloading files...", total=total_count)

        for file_path, file_info in files:
            local_path = Path(file_path).name
            file_size = file_info.get("size", 0)

            # Update the task description to show the current file
            progress.update(task, description=f"Downloading {file_path}")

            try:
                # Create a progress callback for this file
                progress_callback = FSSpecProgressCallback(
                    description=os.path.basename(file_path),
                    size=file_size,
                    progress=progress,
                )

                # Download the file with progress
                local_path = location.get(
                    remote_path=file_path,
                    local_path=local_path,
                    overwrite=force,
                    progress_callback=progress_callback,
                )

                success_count += 1
                console.print(f"✅ [green]Downloaded:[/green] {local_path}")

            except Exception as e:
                console.print(f"❌ [red]Error downloading {file_path}:[/red] {str(e)}")
                continue
            finally:
                # Update the overall progress
                progress.update(task, advance=1)

    return success_count, total_count


@location.command(name="get")
@click.argument("sim_id")
@click.argument("location_name")
@click.argument("remote_path")
@click.argument("local_path", required=False, default=".")
@click.option("--force", "-f", is_flag=True, help="Overwrite existing files")
def get_file(
    sim_id: str, location_name: str, remote_path: str, local_path: str, force: bool
):
    """Download a file from a location to the local filesystem.

    SIM_ID: ID of the simulation
    LOCATION_NAME: Name of the location to get the file from
    REMOTE_PATH: Path to the file on the remote location
    LOCAL_PATH: Local path to save the file (default: current directory)
    """
    sim = get_simulation_or_exit(sim_id)

    if location_name not in sim.locations:
        console.print(
            f"[red]Error:[/red] Location '{location_name}' not found in simulation"
        )
        raise click.Abort(1)

    loc_data = sim.locations[location_name]
    location = loc_data["location"]

    # Resolve paths
    base_path = sim.get_location_path(location_name)
    full_remote_path = (
        f"{base_path}/{remote_path}"
        if not remote_path.startswith(base_path)
        else remote_path
    )
    local_path = Path(local_path)

    # If local_path is a directory, use the remote filename
    if local_path.is_dir() or local_path == Path("."):
        local_path = local_path / Path(remote_path).name

    try:
        # Use the Location's get method with built-in progress tracking
        downloaded_path = location.get(
            full_remote_path, str(local_path), overwrite=force, show_progress=True
        )
        console.print(f"✅ [green]Downloaded:[/green] {downloaded_path}")
        return downloaded_path

    except Exception as e:
        console.print(f"[red]Error downloading file: {str(e)}[/red]")
        raise click.Abort(1)


@location.command(name="mget")
@click.argument("sim_id")
@click.argument("location_name")
@click.argument("pattern")
@click.argument("local_dir", required=False, default=".")
@click.option(
    "--recursive", "-r", is_flag=True, help="Download directories recursively"
)
@click.option("--force", "-f", is_flag=True, help="Overwrite existing files")
def get_multiple_files(
    sim_id: str,
    location_name: str,
    pattern: str,
    local_dir: str,
    recursive: bool,
    force: bool,
):
    """Download multiple files matching a pattern from a location.

    SIM_ID: ID of the simulation
    LOCATION_NAME: Name of the location to get files from
    PATTERN: Pattern to match files (e.g., '*.txt' or 'data/*.nc')
    LOCAL_DIR: Local directory to save files (default: current directory)
    """
    sim = get_simulation_or_exit(sim_id)

    if location_name not in sim.locations:
        console.print(
            f"[red]Error:[/red] Location '{location_name}' not found in simulation"
        )
        raise click.Abort(1)

    loc_data = sim.locations[location_name]
    location = loc_data["location"]

    # Resolve base path
    base_path = sim.get_location_path(location_name)

    try:
        # Use the Location's mget method with built-in progress tracking
        downloaded_files = location.mget(
            remote_pattern=pattern,
            local_dir=local_dir,
            recursive=recursive,
            overwrite=force,
            base_path=base_path,
            show_progress=True,
            console=console,
        )
        
        return len(downloaded_files), len(downloaded_files)

    except Exception as e:
        console.print(f"[red]Error during download: {str(e)}[/red]")
        raise click.Abort(1)

@location.command(name="ls")
@click.argument("sim_id")
@click.argument("location_name")
@click.argument("path", required=False, default="")
@click.option("--recursive", "-r", is_flag=True, help="List recursively")
@click.option("--detail", "-l", is_flag=True, help="Show detailed information")
def list_files(
    sim_id: str, location_name: str, path: str, recursive: bool, detail: bool
):
    """List files in a location with context-aware path resolution.

    SIM_ID: ID of the simulation
    LOCATION_NAME: Name of the location to list files from
    PATH: Optional path relative to the location's base path
    """
    sim = get_simulation_or_exit(sim_id)

    if location_name not in sim.locations:
        console.print(
            f"[red]Error:[/red] Location '{location_name}' not found in simulation"
        )
        raise click.Abort(1)

    loc_data = sim.locations[location_name]
    location = loc_data["location"]

    # Get the base path with context applied
    base_path = sim.get_location_path(location_name)
    full_path = f"{base_path}/{path}" if path else base_path

    try:
        if not hasattr(location, "fs"):
            console.print(
                f"[red]Error:[/red] Location '{location_name}' does not support file operations"
            )
            raise click.Abort(1)

        # List files
        if recursive:
            files = []
            for root, _, filenames in location.fs.walk(full_path):
                for filename in filenames:
                    files.append(
                        f"{root}/{filename}" if root != full_path else filename
                    )
        else:
            files = location.fs.ls(full_path, detail=detail)

        # Display results
        console.print(f"[bold]Listing files in:[/bold] {full_path}\n")

        if detail and files:
            table = Table(show_header=True, header_style="bold magenta")
            table.add_column("Name", style="cyan")
            table.add_column("Type", style="green")
            table.add_column("Size", style="blue")
            table.add_column("Modified", style="yellow")
            table.add_column("Staged", style="blue")

            for file_info in files:
                name = file_info["name"] if "name" in file_info else file_info
                file_type = (
                    "directory" if file_info.get("type") == "directory" else "file"
                )
                size = file_info.get("size", "")
                modified = file_info.get("mtime", "")
                staged = file_info.get("staged", "")

                table.add_row(
                    name,
                    file_type,
                    str(size) if size else "",
                    str(modified) if modified else "",
                    "[green]✓[/green]" if staged else "[red]✗[/red]",
                )
            console.print(table)
        else:
            for file in files:
                console.print(f"- {file}")

    except Exception as e:
        console.print(f"[red]Error listing files:[/red] {str(e)}")
        raise click.Abort(1)


@location.command(name="show")
@click.argument("sim_id")
@click.argument("location_name")
def show_location(sim_id: str, location_name: str):
    """Show details of a location in a simulation.

    SIM_ID: ID of the simulation
    LOCATION_NAME: Name of the location to show
    """
    sim = get_simulation_or_exit(sim_id)

    if location_name not in sim.locations:
        console.print(
            f"[red]Error:[/red] Location '{location_name}' not found in simulation"
        )
        raise click.Abort(1)

    loc_data = sim.locations[location_name]
    location = loc_data["location"]
    context = loc_data.get("context", {})

    # Basic info
    info = [
        f"[bold]Name:[/bold] {location_name}",
        f"[bold]Path:[/bold] {location.config.get('path', 'Not specified')}",
        f"[bold]Type:[/bold] {location.kinds[0].name if location.kinds else 'Unknown'}",
        f"[bold]Protocol:[/bold] {location.config.get('protocol', 'Not specified')}",
    ]

    # Context info
    if context:
        info.append("\n[bold]Context:[/bold]")
        if context.path_prefix:
            resolved_path = sim.get_location_path(location_name)
            info.append(f"  [bold]Path Prefix:[/bold] {context.path_prefix}")
            info.append(f"  [bold]Resolved Path:[/bold] {resolved_path}")
        if context.overrides:
            overrides = "\n    ".join(f"{k}: {v}" for k, v in context.overrides.items())
            info.append(f"  [bold]Overrides:[/bold]\n    {overrides}")
        if context.metadata:
            metadata = "\n    ".join(f"{k}: {v}" for k, v in context.metadata.items())
            info.append(f"  [bold]Metadata:[/bold]\n    {metadata}")

    console.print(
        Panel(
            "\n".join(info),
            title=f"Location: {location_name}",
            border_style="green",
            expand=False,
        )
    )


@simulation.command()
@click.argument("sim_id")
@click.option(
    "--force",
    is_flag=True,
    help="Force removal without confirmation",
)
def delete(sim_id: str, force: bool):
    """Delete a simulation.

    SIM_ID: ID of the simulation to delete
    """
    # This will raise an error if the simulation doesn't exist
    get_simulation_or_exit(sim_id)

    if not force:
        # Show simulation details before deletion
        console.print("\n[bold]Simulation to be deleted:[/bold]")
        show.callback(sim_id)
        console.print("\n")

        click.confirm(
            f"[red]Are you sure you want to delete simulation '{sim_id}'?[/red]",
            abort=True,
        )

    try:
        # Delete the simulation
        Simulation.delete_simulation(sim_id)

        console.print(
            Panel.fit(
                f"✅ [bold green]Deleted simulation:[/bold green] {sim_id}",
                title="Simulation Deleted",
            )
        )
    except Exception as e:
        console.print(f"[red]Error deleting simulation:[/red] {str(e)}")
        raise click.Abort(1)
