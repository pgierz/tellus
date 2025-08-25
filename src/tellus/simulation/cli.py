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
from ..domain.entities.simulation import SimulationEntity
from ..core.cli import cli, console
from ..core.feature_flags import feature_flags, FeatureFlag
from ..core.service_container import get_service_container
from ..core.legacy_bridge import SimulationBridge
from ..application.exceptions import (
    EntityNotFoundError,
    EntityAlreadyExistsError,
    ValidationError,
    ApplicationError,
)

import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Load simulations at module import (legacy path)
Simulation.load_simulations()


def _get_simulation_bridge() -> Optional[SimulationBridge]:
    """Get simulation bridge if new architecture is enabled."""
    if feature_flags.is_enabled(FeatureFlag.USE_NEW_SIMULATION_SERVICE):
        service_container = get_service_container()
        return SimulationBridge(service_container.service_factory)
    return None


def get_simulation_or_exit(sim_id: str) -> Union[Simulation, SimulationEntity]:
    """Helper to get a simulation or exit with error"""
    bridge = _get_simulation_bridge()

    if bridge:
        # Use new architecture - returns SimulationEntity
        sim = bridge.get_simulation(sim_id)
        if not sim:
            console.print(f"[red]Error:[/red] Simulation with ID '{sim_id}' not found")
            raise click.Abort(1)
        return sim
    else:
        # Use legacy architecture - returns Simulation
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
    bridge = _get_simulation_bridge()

    if bridge:
        # Use new architecture
        try:
            simulations = bridge.list_simulations_legacy_format()
            if not simulations:
                console.print("No simulations found.")
                if feature_flags.is_enabled(FeatureFlag.USE_NEW_SIMULATION_SERVICE):
                    console.print("[dim]Using new simulation service[/dim]")
                return
        except ApplicationError as e:
            console.print(f"[red]Error:[/red] {str(e)}")
            return
    else:
        # Use legacy architecture
        simulations = Simulation.list_simulations()
        if not simulations:
            console.print("No simulations found.")
            return
        # Convert legacy objects to dict format for consistent processing
        simulations = [
            {
                "simulation_id": sim.simulation_id,
                "path": str(sim.path) if sim.path else None,
                "attrs": sim.attrs or {},
                "locations": sim.locations or {},
            }
            for sim in simulations
        ]

    table = Table(
        title="Available Simulations", show_header=True, header_style="bold magenta"
    )
    table.add_column("ID", style="cyan")
    table.add_column("Path", style="green")
    table.add_column("# Locations", style="blue")
    table.add_column("Attributes", style="yellow")

    for sim in sorted(simulations, key=lambda s: s["simulation_id"]):
        path = sim["path"] if sim["path"] else "-"
        num_locations = len(sim.get("locations", {}))
        attrs = ", ".join(sim.get("attrs", {}).keys()) if sim.get("attrs") else "-"
        table.add_row(sim["simulation_id"], path, str(num_locations), attrs)

    console.print(Panel.fit(table))

    # Show which architecture is being used
    if feature_flags.is_enabled(FeatureFlag.USE_NEW_SIMULATION_SERVICE):
        console.print("[dim]✨ Using new simulation service[/dim]")


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

    If no arguments are provided, an interactive wizard will guide you through the process.

    SIM_ID: Optional identifier for the simulation. If not provided, a UUID will be generated.
    """
    # Interactive wizard when no arguments are provided
    if sim_id is None and path is None and not attr:
        console.print("\n[bold blue]🚀 Simulation Creation Wizard[/bold blue]")
        console.print(
            "Let's create a new simulation. Press Ctrl+C to cancel at any time.\n"
        )

        # Get simulation ID
        import questionary

        sim_id = questionary.text(
            "Enter simulation ID (leave empty for auto-generated UUID):",
            default="",
        ).ask()

        if sim_id is None:  # User cancelled
            console.print("\nOperation cancelled.")
            raise click.Abort(1)

        # If empty, let Simulation class generate UUID
        if not sim_id.strip():
            sim_id = None

        # Get simulation path with tab completion
        from prompt_toolkit import prompt
        from prompt_toolkit.completion import PathCompleter

        path_completer = PathCompleter(
            expanduser=True,
            only_directories=True,
        )

        path_input = prompt(
            "Enter filesystem path for simulation data (press Tab to complete, Enter to skip): ",
            completer=path_completer,
            complete_while_typing=True,
        )

        path = path_input.strip() if path_input else None

        # Get attributes interactively
        attrs = []
        console.print("\n[bold]Attributes (optional)[/bold]")
        console.print("Add key-value attributes to describe your simulation.")

        while True:
            add_more = questionary.confirm(
                "Would you like to add an attribute?"
                if not attrs
                else "Add another attribute?",
                default=False if attrs else True,
            ).ask()

            if not add_more:
                break

            key = questionary.text("Attribute key:").ask()
            if not key:
                break

            value = questionary.text(f"Value for '{key}':").ask()
            if value is not None:
                attrs.append((key, value))
                console.print(f"  ✓ Added: {key} = {value}")

        attr = attrs if attrs else None

        # Confirmation summary
        console.print("\n[bold]Summary:[/bold]")
        console.print(f"  [bold]ID:[/bold] {sim_id or 'Auto-generated UUID'}")
        console.print(f"  [bold]Path:[/bold] {path or 'Not specified'}")
        if attr:
            console.print(f"  [bold]Attributes:[/bold]")
            for key, value in attr:
                console.print(f"    • {key}: {value}")
        else:
            console.print(f"  [bold]Attributes:[/bold] None")

        confirm = questionary.confirm(
            "\nCreate this simulation?",
            default=True,
        ).ask()

        if not confirm:
            console.print("Operation cancelled.")
            return

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

    # Locations - handle both SimulationEntity and legacy Simulation
    if hasattr(sim, "associated_locations") and sim.associated_locations:
        # SimulationEntity has associated_locations (set)
        locations = "\n  ".join(str(loc) for loc in sim.associated_locations)
        info.append(f"\n[bold]Locations:[/bold]\n  {locations}")
    elif hasattr(sim, "locations") and sim.locations:
        # Legacy Simulation has locations (dict)
        locations = "\n  ".join(
            f"{name}: {loc.get('type', 'unknown') if isinstance(loc, dict) else str(loc)}"
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


@simulation.command(name="show-attrs")
@click.argument("sim_id")
@click.option(
    "--render",
    "render_str",
    type=str,
    default=None,
    help="Optionally render a Jinja2 template using this simulation's attrs context.",
)
def show_attrs(sim_id: str, render_str: Optional[str] = None):
    """Show the attrs of a simulation (used for template variables).

    SIM_ID: ID of the simulation to inspect
    """
    sim = get_simulation_or_exit(sim_id)

    attrs = dict(getattr(sim, "attrs", {}) or {})

    # Display attrs in a table
    table = Table(
        title=f"Attributes for {sim_id}", show_header=True, header_style="bold magenta"
    )
    table.add_column("Key", style="cyan")
    table.add_column("Value", style="green")

    if attrs:
        for k in sorted(attrs.keys()):
            table.add_row(str(k), str(attrs[k]))
    else:
        table.add_row("-", "-")

    console.print(Panel.fit(table))

    # Optional rendering of a provided template string
    if render_str:
        from ..core.template import render_template

        console.print("\n[bold]Render test:[/bold]")
        console.print(f"Template: [dim]{render_str}[/dim]")
        try:
            rendered = render_template(render_str, attrs)
            console.print(f"Result  : [bold green]{rendered}[/bold green]")
        except Exception as e:
            console.print(f"[red]Render error:[/red] {e}")


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

    # Persist updates depending on architecture
    bridge = _get_simulation_bridge()
    if bridge:
        # Use new application service via bridge
        persisted = bridge.update_simulation(
            sim_id,
            path=str(sim.path) if path is not None else None,
            attributes=dict(getattr(sim, "attrs", {}) or {}),
        )
        if not persisted:
            console.print(
                "[red]Failed to persist simulation updates via new service[/red]"
            )
            raise click.Abort(1)
    else:
        # Legacy persistence
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
        # Use location-aware completion for better UX
        from prompt_toolkit import prompt
        from ..core.completion import SmartPathCompleter

        # Get the selected location for smart completion
        try:
            location_obj = (
                LocationModel.get_location(location_name)
                if location_name != "__new__"
                else None
            )
        except:
            location_obj = None

        path_completer = SmartPathCompleter(
            location=location_obj,
            expanduser=True,
            only_directories=True,
        )

        prompt_text = "\nEnter path prefix (press Tab to complete, Enter to skip): "
        if location_obj and location_obj.config.get("protocol") != "file":
            protocol = location_obj.config.get("protocol", "unknown")
            prompt_text = f"\nEnter path prefix for {protocol}://{location_obj.name} (press Tab to complete, Enter to skip): "

        path_prefix = (
            prompt(
                prompt_text,
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

        # Ensure legacy locations registry is loaded for resolution
        try:
            Location.load_locations()
        except Exception:
            pass

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

        # Add to simulation using the bridge if available, fallback to legacy
        bridge = _get_simulation_bridge()
        if bridge:
            # Use new architecture through bridge
            context_data = {}
            if context:
                context_data = {
                    "path_prefix": getattr(context, "path_prefix", ""),
                    "overrides": getattr(context, "overrides", {}),
                    "metadata": getattr(context, "metadata", {}),
                }

            success = bridge.associate_location_to_simulation(
                simulation_id=sim_id,
                location_name=location_name,
                context_data=context_data,
            )

            if not success:
                console.print(
                    f"[red]Error:[/red] Failed to associate location with simulation"
                )
                raise click.Abort(1)
        else:
            # Fallback to legacy architecture
            actual_sim = Simulation.get_simulation(sim_id)
            if not actual_sim:
                console.print(
                    f"[red]Error:[/red] Could not find simulation object for '{sim_id}'"
                )
                raise click.Abort(1)

            actual_sim.add_location(location, context=context, override=override)
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

    table = Table(
        title=f"Locations in Simulation: {sim_id}",
        show_header=True,
        header_style="bold magenta",
    )
    table.add_column("Name", style="cyan")
    table.add_column("Resolved Path", style="green")
    table.add_column("Kinds", style="blue")
    table.add_column("Metadata", style="blue")

    # New architecture: SimulationEntity
    if hasattr(sim, "associated_locations"):
        location_names = list(sim.associated_locations) or []
        if not location_names:
            # Fallback: use legacy simulation mapping if present
            legacy_sim = Simulation.get_simulation(sim_id)
            if legacy_sim and getattr(legacy_sim, "locations", None):
                location_names = sorted(list(legacy_sim.locations.keys()))
            else:
                console.print("No locations found in this simulation.")
                return

        # Use attrs-only context with Jinja2 renderer for template resolution
        from ..core.template import render_template

        attrs_ctx = {}
        try:
            attrs_ctx = dict(getattr(sim, "attrs", {}) or {})
        except Exception:
            attrs_ctx = {}

        from ..location.location import Location

        for name in sorted(location_names):
            # Prefer new-service context, fall back to legacy entry
            context = {}
            if hasattr(sim, "location_contexts") and name in getattr(
                sim, "location_contexts", {}
            ):
                context = sim.location_contexts.get(name, {})
            else:
                legacy_sim = (
                    "legacy_sim" in locals()
                    and legacy_sim
                    or Simulation.get_simulation(sim_id)
                )
                if legacy_sim and name in getattr(legacy_sim, "locations", {}):
                    entry = legacy_sim.locations.get(name, {})
                    context = (
                        entry.get("context", {}) if isinstance(entry, dict) else {}
                    )
            loc_obj = Location.get_location(name)

            # Determine kinds
            kinds = []
            if loc_obj is not None and getattr(loc_obj, "kinds", None):
                try:
                    kinds = [k.name for k in loc_obj.kinds]
                except Exception:
                    kinds = [str(k) for k in loc_obj.kinds]

            # Resolved path: combine location base path with rendered context path_prefix
            raw_prefix = (
                context.get("path_prefix")
                if isinstance(context, dict)
                else getattr(context, "path_prefix", "")
            )
            # Determine base path from location config
            base_path = ""
            try:
                if loc_obj is not None:
                    storage_opts = loc_obj.config.get("storage_options", {})
                    base_path = (
                        storage_opts.get("path", loc_obj.config.get("path", "")) or ""
                    )
            except Exception:
                base_path = ""

            rendered_suffix = ""
            if raw_prefix:
                try:
                    rendered_suffix = render_template(raw_prefix, attrs_ctx)
                except Exception:
                    rendered_suffix = raw_prefix

            # Build final resolved path
            parts = []
            if base_path:
                parts.append(str(base_path).rstrip("/"))
            if rendered_suffix:
                parts.append(str(rendered_suffix).lstrip("/"))
            resolved_path = "/".join(parts) if parts else "-"

            metadata = ", ".join(
                f"{k}={v}"
                for k, v in (
                    context.get("metadata", {})
                    if isinstance(context, dict)
                    else getattr(context, "metadata", {})
                ).items()
            )

            table.add_row(name, resolved_path, ", ".join(kinds) or "-", metadata or "")

        console.print(Panel.fit(table))
        return

    # Legacy architecture: Simulation
    if not getattr(sim, "locations", {}):
        console.print("No locations found in this simulation.")
        return

    # Use attrs-only context with Jinja2 renderer for template resolution (legacy)
    from ..core.template import render_template

    legacy_attrs_ctx = {}
    try:
        legacy_attrs_ctx = dict(getattr(sim, "attrs", {}) or {})
    except Exception:
        legacy_attrs_ctx = {}

    for name, loc_data in getattr(sim, "locations").items():
        location = loc_data["location"]
        context = loc_data.get("context", {})

        # Resolved path: render context path_prefix with attrs-only context
        raw_prefix = (
            context.get("path_prefix")
            if isinstance(context, dict)
            else getattr(context, "path_prefix", "")
        )
        if raw_prefix:
            try:
                resolved_path = render_template(raw_prefix, legacy_attrs_ctx)
            except Exception:
                resolved_path = raw_prefix
        else:
            resolved_path = "-"

        metadata = ", ".join(
            f"{k}={v}"
            for k, v in (
                context.get("metadata", {})
                if isinstance(context, dict)
                else getattr(context, "metadata", {})
            ).items()
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


@location.command(name="browse")
@click.argument("sim_id", required=False)
@click.argument("location_name", required=False)
def browse_files(sim_id: Optional[str] = None, location_name: Optional[str] = None):
    """Interactive file browser for downloading files from locations.

    If no arguments are provided, an interactive wizard will guide you through browsing and downloading files.
    """
    if sim_id is None:
        console.print("\n[bold blue]🗂️  Interactive File Browser[/bold blue]")
        console.print(
            "Browse and download files from simulation locations. Press Ctrl+C to cancel at any time.\n"
        )

        # Select simulation
        simulations = Simulation.list_simulations()
        if not simulations:
            console.print(
                "[red]Error:[/red] No simulations found. Please create a simulation first."
            )
            raise click.Abort(1)

        import questionary

        sim_choices = []
        for sim in simulations:
            try:
                if hasattr(sim, "locations") and isinstance(
                    getattr(sim, "locations", None), dict
                ):
                    loc_count = len(sim.locations)
                else:
                    loc_count = len(
                        list(getattr(sim, "associated_locations", []) or [])
                    )
            except Exception:
                loc_count = 0
            sim_choices.append(
                {
                    "name": f"{sim.simulation_id} ({loc_count} locations)",
                    "value": sim.simulation_id,
                }
            )

        sim_id = questionary.select(
            "Select a simulation:",
            choices=sim_choices,
        ).ask()

        if not sim_id:
            console.print("\nOperation cancelled.")
            raise click.Abort(1)

    sim = get_simulation_or_exit(sim_id)

    if location_name is None:
        # Determine available location names across architectures
        if (
            hasattr(sim, "locations")
            and isinstance(getattr(sim, "locations", None), dict)
            and sim.locations
        ):
            location_names = sorted(list(sim.locations.keys()))
        else:
            location_names = sorted(
                list(getattr(sim, "associated_locations", []) or [])
            )
            if not location_names:
                # Fallback to legacy simulation mapping if needed
                legacy_sim = Simulation.get_simulation(sim_id)
                if legacy_sim and getattr(legacy_sim, "locations", None):
                    location_names = sorted(list(legacy_sim.locations.keys()))

        if not location_names:
            console.print(
                f"[red]Error:[/red] No locations found in simulation '{sim_id}'"
            )
            raise click.Abort(1)

        # Build location choices by resolving protocol from Location registry when possible
        from ..location.location import Location

        try:
            Location.load_locations()
        except Exception:
            pass

        location_choices = []
        for name in location_names:
            proto = "unknown"
            loc_obj = Location.get_location(name)
            if loc_obj:
                proto = loc_obj.config.get("protocol", proto)
            elif hasattr(sim, "locations") and name in getattr(sim, "locations", {}):
                # legacy path retains protocol in loc_data
                try:
                    proto = sim.locations[name]["location"].config.get(
                        "protocol", proto
                    )
                except Exception:
                    pass
            location_choices.append({"name": f"{name} ({proto})", "value": name})

        location_name = questionary.select(
            "Select a location to browse:",
            choices=location_choices,
        ).ask()

        if not location_name:
            console.print("\nOperation cancelled.")
            raise click.Abort(1)

    # Start browsing from the base path
    current_path = ""
    from ..location.location import Location
    from ..core.template import render_template

    # Resolve Location object
    try:
        Location.load_locations()
    except Exception:
        pass

    location = None
    base_path = ""
    if hasattr(sim, "locations") and location_name in getattr(sim, "locations", {}):
        loc_data = sim.locations[location_name]
        location = loc_data["location"]
        base_path = sim.get_location_path(location_name)
    else:
        location = Location.get_location(location_name)
        if not location:
            console.print(f"[red]Error:[/red] Location '{location_name}' not found")
            raise click.Abort(1)

        # Build attrs-only context
        attrs_ctx = dict(getattr(sim, "attrs", {}) or {})
        context = {}
        if hasattr(sim, "location_contexts") and location_name in getattr(
            sim, "location_contexts", {}
        ):
            context = sim.location_contexts.get(location_name, {})

        storage_opts = location.config.get("storage_options", {})
        base_prefix = storage_opts.get("path", location.config.get("path", "")) or ""

        raw_prefix = (
            context.get("path_prefix")
            if isinstance(context, dict)
            else getattr(context, "path_prefix", "")
        )
        rendered_suffix = ""
        if raw_prefix:
            try:
                rendered_suffix = render_template(raw_prefix, attrs_ctx)
            except Exception:
                rendered_suffix = raw_prefix

        parts = []
        if base_prefix:
            parts.append(str(base_prefix).rstrip("/"))
        if rendered_suffix:
            parts.append(str(rendered_suffix).lstrip("/"))
        base_path = "/".join(parts) if parts else base_prefix

    while True:
        full_path = f"{base_path}/{current_path}" if current_path else base_path

        try:
            # List files in current directory
            files = location.fs.ls(full_path, detail=True)

            # Build menu choices
            choices = []

            # Add parent directory option if not at root
            if current_path:
                choices.append({"name": "📁 .. (go up)", "value": "__parent__"})

            # Add directories first
            dirs = [f for f in files if f.get("type") == "directory"]
            for file_info in sorted(dirs, key=lambda x: x["name"]):
                relative_name = file_info["name"]
                if relative_name.startswith(base_path):
                    relative_name = relative_name[len(base_path) :].lstrip("/")
                choices.append(
                    {"name": f"📁 {relative_name}/", "value": f"dir:{relative_name}"}
                )

            # Add files
            files_list = [f for f in files if f.get("type") != "directory"]
            for file_info in sorted(files_list, key=lambda x: x["name"]):
                relative_name = file_info["name"]
                if relative_name.startswith(base_path):
                    relative_name = relative_name[len(base_path) :].lstrip("/")
                size = file_info.get("size", 0)
                size_str = f" ({size} bytes)" if size else ""
                choices.append(
                    {
                        "name": f"📄 {relative_name}{size_str}",
                        "value": f"file:{relative_name}",
                    }
                )

            # Add action options
            choices.extend(
                [
                    {"name": "⬇️  Download selected files", "value": "__download__"},
                    {"name": "🔍 Search in current directory", "value": "__search__"},
                    {"name": "❌ Exit browser", "value": "__exit__"},
                ]
            )

            console.print(f"\n[bold]Current path:[/bold] {full_path}")

            selection = questionary.select(
                "Select an item:",
                choices=choices,
            ).ask()

            if not selection or selection == "__exit__":
                break
            elif selection == "__parent__":
                # Go up one directory
                if "/" in current_path:
                    current_path = "/".join(current_path.split("/")[:-1])
                else:
                    current_path = ""
            elif selection == "__download__":
                # Multi-select files for download
                file_choices = [
                    {
                        "name": f"📄 {f['name'][len(base_path) :].lstrip('/') if f['name'].startswith(base_path) else f['name']}",
                        "value": f["name"],
                    }
                    for f in files_list
                ]

                if not file_choices:
                    console.print("[yellow]No files in current directory[/yellow]")
                    continue

                selected_files = questionary.checkbox(
                    "Select files to download:",
                    choices=file_choices,
                ).ask()

                if selected_files:
                    local_dir = questionary.text(
                        "Enter local directory to save files:", default="."
                    ).ask()

                    if local_dir:
                        for file_path in selected_files:
                            try:
                                local_path = Path(local_dir) / Path(file_path).name
                                location.get(
                                    file_path,
                                    str(local_path),
                                    overwrite=True,
                                    show_progress=True,
                                )
                                console.print(
                                    f"✅ [green]Downloaded:[/green] {local_path}"
                                )
                            except Exception as e:
                                console.print(
                                    f"❌ [red]Error downloading {file_path}:[/red] {str(e)}"
                                )

                        if questionary.confirm(
                            "Continue browsing?", default=True
                        ).ask():
                            continue
                        else:
                            break

            elif selection == "__search__":
                pattern = questionary.text(
                    "Enter search pattern (e.g., '*.txt', '*data*'):"
                ).ask()

                if pattern:
                    try:
                        import fnmatch

                        matching_files = []
                        for file_info in files_list:
                            filename = file_info["name"]
                            if filename.startswith(base_path):
                                filename = filename[len(base_path) :].lstrip("/")
                            if fnmatch.fnmatch(filename, pattern):
                                matching_files.append(file_info)

                        if matching_files:
                            console.print(
                                f"\n[bold]Found {len(matching_files)} matching files:[/bold]"
                            )
                            for file_info in matching_files:
                                filename = file_info["name"]
                                if filename.startswith(base_path):
                                    filename = filename[len(base_path) :].lstrip("/")
                                console.print(f"  📄 {filename}")
                        else:
                            console.print(
                                f"[yellow]No files matching '{pattern}' found[/yellow]"
                            )
                    except Exception as e:
                        console.print(f"[red]Search error:[/red] {str(e)}")

            elif selection.startswith("dir:"):
                # Navigate into directory
                dir_name = selection[4:]  # Remove "dir:" prefix
                current_path = (
                    f"{current_path}/{dir_name}" if current_path else dir_name
                )
            elif selection.startswith("file:"):
                # Download single file
                file_name = selection[5:]  # Remove "file:" prefix
                file_path = (
                    f"{base_path}/{current_path}/{file_name}"
                    if current_path
                    else f"{base_path}/{file_name}"
                )

                local_path = questionary.text(
                    f"Save '{file_name}' as:", default=file_name
                ).ask()

                if local_path:
                    try:
                        location.get(
                            file_path, local_path, overwrite=True, show_progress=True
                        )
                        console.print(f"✅ [green]Downloaded:[/green] {local_path}")
                    except Exception as e:
                        console.print(f"❌ [red]Error downloading:[/red] {str(e)}")

        except Exception as e:
            console.print(f"[red]Error browsing directory:[/red] {str(e)}")
            break


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
@click.argument("sim_id", required=False)
@click.argument("location_name", required=False)
@click.argument("pattern", required=False)
@click.argument("local_dir", required=False, default=".")
@click.option(
    "--recursive", "-r", is_flag=True, help="Download directories recursively"
)
@click.option("--force", "-f", is_flag=True, help="Overwrite existing files")
def get_multiple_files(
    sim_id: Optional[str] = None,
    location_name: Optional[str] = None,
    pattern: Optional[str] = None,
    local_dir: str = ".",
    recursive: bool = False,
    force: bool = False,
):
    """Download multiple files matching a pattern from a location.

    If no arguments are provided, an interactive wizard will guide you through pattern selection and preview.

    SIM_ID: ID of the simulation
    LOCATION_NAME: Name of the location to get files from
    PATTERN: Pattern to match files (e.g., '*.txt' or 'data/*.nc')
    LOCAL_DIR: Local directory to save files (default: current directory)
    """
    # Interactive wizard when no arguments are provided
    if sim_id is None:
        console.print("\n[bold blue]📦 Bulk Download Wizard[/bold blue]")
        console.print(
            "Download multiple files with pattern matching. Press Ctrl+C to cancel at any time.\n"
        )

        # Select simulation
        simulations = Simulation.list_simulations()
        if not simulations:
            console.print(
                "[red]Error:[/red] No simulations found. Please create a simulation first."
            )
            raise click.Abort(1)

        import questionary

        sim_choices = [
            {
                "name": f"{sim.simulation_id} ({len(sim.locations)} locations)",
                "value": sim.simulation_id,
            }
            for sim in simulations
        ]

        sim_id = questionary.select(
            "Select a simulation:",
            choices=sim_choices,
        ).ask()

        if not sim_id:
            console.print("\nOperation cancelled.")
            raise click.Abort(1)

    sim = get_simulation_or_exit(sim_id)

    if location_name is None:
        if not sim.locations:
            console.print(
                f"[red]Error:[/red] No locations found in simulation '{sim_id}'"
            )
            raise click.Abort(1)

        # Select location
        location_choices = [
            {
                "name": f"{name} ({loc_data['location'].config.get('protocol', 'unknown')})",
                "value": name,
            }
            for name, loc_data in sim.locations.items()
        ]

        location_name = questionary.select(
            "Select a location:",
            choices=location_choices,
        ).ask()

        if not location_name:
            console.print("\nOperation cancelled.")
            raise click.Abort(1)

    if location_name not in sim.locations:
        console.print(
            f"[red]Error:[/red] Location '{location_name}' not found in simulation"
        )
        raise click.Abort(1)

    loc_data = sim.locations[location_name]
    location = loc_data["location"]
    base_path = sim.get_location_path(location_name)

    # Interactive pattern selection and preview
    if pattern is None:
        import questionary

        # Suggest common patterns
        pattern_choices = [
            {"name": "*.txt - All text files", "value": "*.txt"},
            {"name": "*.nc - NetCDF files", "value": "*.nc"},
            {"name": "*.csv - CSV files", "value": "*.csv"},
            {"name": "*.json - JSON files", "value": "*.json"},
            {"name": "*.log - Log files", "value": "*.log"},
            {"name": "data* - Files starting with 'data'", "value": "data*"},
            {"name": "*output* - Files containing 'output'", "value": "*output*"},
            {"name": "Custom pattern...", "value": "__custom__"},
        ]

        pattern_choice = questionary.select(
            "Select a file pattern:",
            choices=pattern_choices,
        ).ask()

        if not pattern_choice:
            console.print("\nOperation cancelled.")
            raise click.Abort(1)

        if pattern_choice == "__custom__":
            pattern = questionary.text(
                "Enter custom pattern (e.g., '*.txt', 'data/*.nc', '**/*.log'):",
                validate=lambda x: len(x.strip()) > 0 or "Pattern cannot be empty",
            ).ask()
        else:
            pattern = pattern_choice

        if not pattern:
            console.print("\nOperation cancelled.")
            raise click.Abort(1)

        # Preview matching files
        console.print(f"\n[bold]Previewing files matching '{pattern}'...[/bold]")

        try:
            import glob
            import fnmatch

            # List all files in the location
            try:
                all_files = location.fs.ls(base_path, detail=True)
                if recursive:
                    # Get files recursively
                    recursive_files = []
                    for root, _, filenames in location.fs.walk(base_path):
                        for filename in filenames:
                            file_path = (
                                f"{root}/{filename}" if root != base_path else filename
                            )
                            try:
                                file_info = location.fs.info(file_path)
                                recursive_files.append(file_info)
                            except:
                                # If we can't get info, create basic entry
                                recursive_files.append(
                                    {"name": file_path, "type": "file"}
                                )
                    all_files.extend(recursive_files)
            except Exception as e:
                console.print(f"[red]Error listing files:[/red] {str(e)}")
                raise click.Abort(1)

            # Normalize entries to dicts to avoid AttributeError when FS returns strings
            normalized_files = []
            for entry in all_files:
                if isinstance(entry, dict):
                    normalized_files.append(entry)
                else:
                    try:
                        info = location.fs.info(entry)
                        normalized_files.append(info)
                    except Exception:
                        normalized_files.append({"name": str(entry), "type": "file"})
            all_files = normalized_files

            # Filter files by pattern
            matching_files = []
            for file_info in all_files:
                if file_info.get("type") == "directory":
                    continue

                filename = file_info["name"]
                if filename.startswith(base_path):
                    relative_name = filename[len(base_path) :].lstrip("/")
                else:
                    relative_name = filename

                if fnmatch.fnmatch(relative_name, pattern):
                    matching_files.append(file_info)

            if not matching_files:
                console.print(
                    f"[yellow]No files matching pattern '{pattern}' found[/yellow]"
                )

                if questionary.confirm("Try a different pattern?", default=True).ask():
                    return get_multiple_files(
                        sim_id, location_name, None, local_dir, recursive, force
                    )
                else:
                    return

            # Show preview
            console.print(
                f"\n[bold green]Found {len(matching_files)} matching files:[/bold green]"
            )

            # Show first 10 files as preview
            preview_files = matching_files[:10]
            for file_info in preview_files:
                filename = file_info["name"]
                if filename.startswith(base_path):
                    filename = filename[len(base_path) :].lstrip("/")
                size = file_info.get("size", 0)
                size_str = f" ({size} bytes)" if size else ""
                console.print(f"  📄 {filename}{size_str}")

            if len(matching_files) > 10:
                console.print(f"  ... and {len(matching_files) - 10} more files")

            # Get download options
            recursive = questionary.confirm(
                "Download recursively (include subdirectories)?", default=recursive
            ).ask()

            force = questionary.confirm(
                "Overwrite existing files?", default=force
            ).ask()

            from prompt_toolkit import prompt
            from prompt_toolkit.completion import PathCompleter

            path_completer = PathCompleter(expanduser=True, only_directories=True)
            local_dir = prompt(
                f"Enter local directory to save files (press Tab to complete): ",
                default=local_dir,
                completer=path_completer,
                complete_while_typing=True,
            )

            # Final confirmation
            console.print(f"\n[bold]Download Summary:[/bold]")
            console.print(f"  [bold]Pattern:[/bold] {pattern}")
            console.print(f"  [bold]Files found:[/bold] {len(matching_files)}")
            console.print(f"  [bold]Local directory:[/bold] {local_dir}")
            console.print(f"  [bold]Recursive:[/bold] {recursive}")
            console.print(f"  [bold]Overwrite:[/bold] {force}")

            if not questionary.confirm("\nProceed with download?", default=True).ask():
                console.print("Operation cancelled.")
                return

        except Exception as e:
            console.print(f"[red]Error during preview:[/red] {str(e)}")
            if not questionary.confirm(
                "Continue with download anyway?", default=False
            ).ask():
                return

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

    # Legacy Simulation path
    if hasattr(sim, "locations"):
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
    else:
        # New SimulationEntity path
        from ..location.location import Location
        from ..core.template import render_template

        # Ensure registry is loaded
        try:
            Location.load_locations()
        except Exception:
            pass

        location = Location.get_location(location_name)
        if not location:
            console.print(f"[red]Error:[/red] Location '{location_name}' not found")
            raise click.Abort(1)

        # Build attrs-only context
        attrs_ctx = dict(getattr(sim, "attrs", {}) or {})

        # Determine context for this location
        context = {}
        if hasattr(sim, "location_contexts") and location_name in getattr(
            sim, "location_contexts", {}
        ):
            context = sim.location_contexts.get(location_name, {})

        # Base path from Location config
        storage_opts = location.config.get("storage_options", {})
        base_prefix = storage_opts.get("path", location.config.get("path", "")) or ""

        # Render path_prefix from context with attrs
        raw_prefix = (
            context.get("path_prefix")
            if isinstance(context, dict)
            else getattr(context, "path_prefix", "")
        )
        rendered_suffix = ""
        if raw_prefix:
            try:
                rendered_suffix = render_template(raw_prefix, attrs_ctx)
            except Exception:
                rendered_suffix = raw_prefix

        # Compose the base path
        parts = []
        if base_prefix:
            parts.append(str(base_prefix).rstrip("/"))
        if rendered_suffix:
            parts.append(str(rendered_suffix).lstrip("/"))
        base_path = "/".join(parts) if parts else base_prefix

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
                # Strip the base path prefix to show relative paths
                if isinstance(name, str) and name.startswith(base_path):
                    # Remove base path and leading slash
                    relative_name = name[len(base_path) :].lstrip("/")
                    name = relative_name if relative_name else name

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
                # Strip the base path prefix to show relative paths
                if isinstance(file, str) and file.startswith(base_path):
                    # Remove base path and leading slash
                    relative_file = file[len(base_path) :].lstrip("/")
                    file = relative_file if relative_file else file
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

    # Handle both SimulationEntity and legacy Simulation
    if hasattr(sim, "associated_locations"):
        # SimulationEntity (from bridge)
        if location_name not in sim.associated_locations:
            console.print(
                f"[red]Error:[/red] Location '{location_name}' not found in simulation"
            )
            raise click.Abort(1)

        # Get location context from SimulationEntity
        context = sim.location_contexts.get(location_name, {})

        # Get location details from location service
        from ..location.location import Location

        location_obj = Location.get_location(location_name)
        if not location_obj:
            console.print(
                f"[red]Error:[/red] Location '{location_name}' not found in location registry"
            )
            raise click.Abort(1)

        # Convert to expected format
        location = {
            "name": location_obj.name,
            "kinds": [kind.name for kind in location_obj.kinds],
            "protocol": location_obj.config.get("protocol", "unknown"),
            "config": location_obj.config,
            "optional": location_obj.optional,
        }

    else:
        # Legacy Simulation
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
        f"[bold]Path:[/bold] {location.get('config', {}).get('path', 'Not specified')}",
        f"[bold]Type:[/bold] {location['kinds'][0] if location.get('kinds') else 'Unknown'}",
        f"[bold]Protocol:[/bold] {location.get('protocol', 'Not specified')}",
    ]

    # Context info - handle both dict and object formats
    if context:
        info.append("\n[bold]Context:[/bold]")

        # Handle path_prefix
        path_prefix = (
            context.get("path_prefix")
            if isinstance(context, dict)
            else getattr(context, "path_prefix", None)
        )
        if path_prefix:
            info.append(f"  [bold]Path Prefix:[/bold] {path_prefix}")
            # Try to get resolved path if method exists
            if hasattr(sim, "get_location_path"):
                try:
                    resolved_path = sim.get_location_path(location_name)
                    info.append(f"  [bold]Resolved Path:[/bold] {resolved_path}")
                except:
                    pass  # Skip if method fails

        # Handle overrides
        overrides = (
            context.get("overrides", {})
            if isinstance(context, dict)
            else getattr(context, "overrides", {})
        )
        if overrides:
            overrides_str = "\n    ".join(f"{k}: {v}" for k, v in overrides.items())
            info.append(f"  [bold]Overrides:[/bold]\n    {overrides_str}")

        # Handle metadata
        metadata = (
            context.get("metadata", {})
            if isinstance(context, dict)
            else getattr(context, "metadata", {})
        )
        if metadata:
            metadata_str = "\n    ".join(f"{k}: {v}" for k, v in metadata.items())
            info.append(f"  [bold]Metadata:[/bold]\n    {metadata_str}")

    console.print(
        Panel(
            "\n".join(info),
            title=f"Location: {location_name}",
            border_style="green",
            expand=False,
        )
    )


@simulation.command()
@click.argument("sim_id", required=False)
@click.argument("export_path", required=False)
def export(sim_id: Optional[str] = None, export_path: Optional[str] = None):
    """Export simulation configuration to a file.

    If no arguments are provided, an interactive wizard will guide you through the export process.

    SIM_ID: ID of the simulation to export
    EXPORT_PATH: Path to save the exported configuration
    """
    if sim_id is None:
        console.print("\n[bold blue]📤 Simulation Export Wizard[/bold blue]")
        console.print(
            "Export simulation configurations for sharing or backup. Press Ctrl+C to cancel at any time.\n"
        )

        # Select simulation
        simulations = Simulation.list_simulations()
        if not simulations:
            console.print(
                "[red]Error:[/red] No simulations found. Please create a simulation first."
            )
            raise click.Abort(1)

        import questionary

        sim_choices = [
            {
                "name": f"{sim.simulation_id} ({len(sim.locations)} locations) - {sim.path or 'No path'}",
                "value": sim.simulation_id,
            }
            for sim in simulations
        ]

        # Allow multiple selection for batch export
        export_type = questionary.select(
            "What would you like to export?",
            choices=[
                {"name": "Single simulation", "value": "single"},
                {"name": "Multiple simulations", "value": "multiple"},
                {"name": "All simulations", "value": "all"},
            ],
        ).ask()

        if not export_type:
            console.print("\nOperation cancelled.")
            raise click.Abort(1)

        if export_type == "single":
            sim_ids = [
                questionary.select(
                    "Select a simulation to export:",
                    choices=sim_choices,
                ).ask()
            ]
        elif export_type == "multiple":
            sim_ids = questionary.checkbox(
                "Select simulations to export:",
                choices=sim_choices,
            ).ask()
        else:  # all
            sim_ids = [sim.simulation_id for sim in simulations]

        if not sim_ids or not any(sim_ids):
            console.print("\nOperation cancelled.")
            raise click.Abort(1)

        # Get export path
        if export_path is None:
            from prompt_toolkit import prompt
            from prompt_toolkit.completion import PathCompleter

            path_completer = PathCompleter(expanduser=True)
            default_name = (
                f"tellus_export_{sim_ids[0] if len(sim_ids) == 1 else 'multiple'}.json"
            )

            export_path = prompt(
                "Enter export file path (press Tab to complete): ",
                default=default_name,
                completer=path_completer,
                complete_while_typing=True,
            )

        if not export_path:
            console.print("\nOperation cancelled.")
            raise click.Abort(1)

        # Export multiple simulations
        export_data = {
            "tellus_export_version": "1.0",
            "export_timestamp": __import__("datetime").datetime.now().isoformat(),
            "simulations": [],
        }

        for sim_id in sim_ids:
            sim = Simulation.get_simulation(sim_id)
            if sim:
                sim_data = {
                    "simulation_id": sim.simulation_id,
                    "path": sim.path,
                    "attrs": sim.attrs,
                    "locations": {},
                }

                # Export location configurations (not the location objects themselves)
                for loc_name, loc_data in sim.locations.items():
                    location = loc_data["location"]
                    context = loc_data.get("context")

                    sim_data["locations"][loc_name] = {
                        "name": location.name,
                        "kinds": [kind.name for kind in location.kinds],
                        "config": location.config,
                        "optional": location.optional,
                        "context": {
                            "path_prefix": context.path_prefix if context else None,
                            "overrides": context.overrides if context else {},
                            "metadata": context.metadata if context else {},
                        }
                        if context
                        else None,
                    }

                export_data["simulations"].append(sim_data)

    else:
        # Single simulation export (CLI mode)
        sim = get_simulation_or_exit(sim_id)

        if export_path is None:
            export_path = f"{sim_id}_export.json"

        export_data = {
            "tellus_export_version": "1.0",
            "export_timestamp": __import__("datetime").datetime.now().isoformat(),
            "simulations": [
                {
                    "simulation_id": sim.simulation_id,
                    "path": sim.path,
                    "attrs": sim.attrs,
                    "locations": {},
                }
            ],
        }

        # Export location configurations
        for loc_name, loc_data in sim.locations.items():
            location = loc_data["location"]
            context = loc_data.get("context")

            export_data["simulations"][0]["locations"][loc_name] = {
                "name": location.name,
                "kinds": [kind.name for kind in location.kinds],
                "config": location.config,
                "optional": location.optional,
                "context": {
                    "path_prefix": context.path_prefix if context else None,
                    "overrides": context.overrides if context else {},
                    "metadata": context.metadata if context else {},
                }
                if context
                else None,
            }

    # Write export file
    try:
        export_path_obj = Path(export_path)
        export_path_obj.parent.mkdir(parents=True, exist_ok=True)

        with open(export_path_obj, "w") as f:
            json.dump(export_data, f, indent=2)

        console.print(
            Panel.fit(
                f"✅ [bold green]Exported {len(export_data['simulations'])} simulation(s) to:[/bold green] {export_path}\n"
                f"[bold]File size:[/bold] {export_path_obj.stat().st_size} bytes",
                title="Export Complete",
            )
        )
    except Exception as e:
        console.print(f"[red]Error writing export file:[/red] {str(e)}")
        raise click.Abort(1)


@simulation.command()
@click.argument("import_path", required=False)
@click.option("--overwrite", is_flag=True, help="Overwrite existing simulations")
def import_(import_path: Optional[str] = None, overwrite: bool = False):
    """Import simulation configurations from a file.

    If no path is provided, an interactive wizard will guide you through the import process.

    IMPORT_PATH: Path to the exported configuration file
    """
    if import_path is None:
        console.print("\n[bold blue]📥 Simulation Import Wizard[/bold blue]")
        console.print(
            "Import simulation configurations from exported files. Press Ctrl+C to cancel at any time.\n"
        )

        import questionary
        from prompt_toolkit import prompt
        from prompt_toolkit.completion import PathCompleter

        path_completer = PathCompleter(expanduser=True)

        import_path = prompt(
            "Enter path to import file (press Tab to complete): ",
            completer=path_completer,
            complete_while_typing=True,
        )

        if not import_path:
            console.print("\nOperation cancelled.")
            raise click.Abort(1)

    # Read and validate import file
    try:
        import_path_obj = Path(import_path)
        if not import_path_obj.exists():
            console.print(f"[red]Error:[/red] Import file '{import_path}' not found.")
            raise click.Abort(1)

        with open(import_path_obj, "r") as f:
            import_data = json.load(f)

        # Validate format
        if not isinstance(import_data, dict) or "simulations" not in import_data:
            console.print("[red]Error:[/red] Invalid import file format.")
            raise click.Abort(1)

        simulations_to_import = import_data["simulations"]
        if not simulations_to_import:
            console.print("[yellow]No simulations found in import file.[/yellow]")
            return

        console.print(
            f"\n[bold]Found {len(simulations_to_import)} simulation(s) to import:[/bold]"
        )

        # Show preview
        for sim_data in simulations_to_import:
            sim_id = sim_data.get("simulation_id", "Unknown")
            path = sim_data.get("path", "No path")
            num_locations = len(sim_data.get("locations", {}))
            console.print(f"  • {sim_id} - {path} ({num_locations} locations)")

        # Check for conflicts
        conflicts = []
        for sim_data in simulations_to_import:
            sim_id = sim_data.get("simulation_id")
            if sim_id and Simulation.get_simulation(sim_id):
                conflicts.append(sim_id)

        if conflicts and not overwrite:
            console.print(
                f"\n[yellow]Warning: The following simulations already exist:[/yellow]"
            )
            for conflict in conflicts:
                console.print(f"  • {conflict}")

            import questionary

            overwrite = questionary.confirm(
                "Overwrite existing simulations?", default=False
            ).ask()

            if overwrite is None:
                console.print("\nOperation cancelled.")
                raise click.Abort(1)

        # Final confirmation
        if import_path is None:  # Interactive mode
            if not questionary.confirm(
                f"\nImport {len(simulations_to_import)} simulation(s)?", default=True
            ).ask():
                console.print("Operation cancelled.")
                return

    except json.JSONDecodeError as e:
        console.print(f"[red]Error parsing import file:[/red] {str(e)}")
        raise click.Abort(1)
    except Exception as e:
        console.print(f"[red]Error reading import file:[/red] {str(e)}")
        raise click.Abort(1)

    # Import simulations
    imported_count = 0
    skipped_count = 0

    for sim_data in simulations_to_import:
        try:
            sim_id = sim_data.get("simulation_id")
            if not sim_id:
                console.print("[yellow]Skipping simulation with no ID[/yellow]")
                skipped_count += 1
                continue

            # Check if simulation exists
            if Simulation.get_simulation(sim_id) and not overwrite:
                console.print(
                    f"[yellow]Skipping existing simulation:[/yellow] {sim_id}"
                )
                skipped_count += 1
                continue

            # Create or update simulation
            if overwrite and Simulation.get_simulation(sim_id):
                Simulation.delete_simulation(sim_id)

            sim = Simulation(simulation_id=sim_id, path=sim_data.get("path"))

            # Add attributes
            attrs = sim_data.get("attrs", {})
            for key, value in attrs.items():
                sim.attrs[key] = value

            # Import locations
            locations_data = sim_data.get("locations", {})
            for loc_name, loc_info in locations_data.items():
                # First ensure the location exists
                from ..location.location import Location, LocationKind

                existing_location = Location.get_location(loc_info["name"])
                if not existing_location:
                    # Create the location
                    kinds = [
                        LocationKind[kind_name]
                        for kind_name in loc_info.get("kinds", ["DISK"])
                    ]
                    location = Location(
                        name=loc_info["name"],
                        kinds=kinds,
                        config=loc_info.get("config", {}),
                        optional=loc_info.get("optional", False),
                    )
                else:
                    location = existing_location

                # Add location to simulation with context
                context = None
                if loc_info.get("context"):
                    from .context import LocationContext

                    context_data = loc_info["context"]
                    if context_data.get("path_prefix"):
                        context = LocationContext(
                            path_prefix=context_data["path_prefix"],
                            overrides=context_data.get("overrides", {}),
                            metadata=context_data.get("metadata", {}),
                        )

                sim.add_location(location, context=context, override=True)

            imported_count += 1
            console.print(f"✅ [green]Imported simulation:[/green] {sim_id}")

        except Exception as e:
            console.print(
                f"❌ [red]Error importing simulation {sim_data.get('simulation_id', 'Unknown')}:[/red] {str(e)}"
            )
            skipped_count += 1

    # Save all simulations
    Simulation.save_simulations()

    # Show summary
    console.print(
        Panel.fit(
            f"✅ [bold green]Import completed![/bold green]\n"
            f"[bold]Imported:[/bold] {imported_count} simulations\n"
            f"[bold]Skipped:[/bold] {skipped_count} simulations",
            title="Import Summary",
        )
    )


@simulation.command()
@click.argument("template_name", required=False)
def template(template_name: Optional[str] = None):
    """Create simulations from predefined templates.

    If no template is specified, an interactive wizard will show available templates.

    TEMPLATE_NAME: Name of the template to use
    """
    # Define predefined templates
    templates = {
        "climate_model": {
            "name": "Climate Model Simulation",
            "description": "Standard setup for Earth System Model simulations",
            "attrs": {"model": "ESM", "type": "climate", "resolution": "standard"},
            "suggested_locations": [
                {
                    "name": "input_data",
                    "description": "Model input files (forcing data, initial conditions)",
                    "path_prefix": "input/{model_id}/{simulation_id}",
                    "protocol": "file",
                },
                {
                    "name": "output_data",
                    "description": "Model output files (NetCDF, logs)",
                    "path_prefix": "output/{model_id}/{simulation_id}",
                    "protocol": "file",
                },
                {
                    "name": "restart_files",
                    "description": "Model restart/checkpoint files",
                    "path_prefix": "restart/{model_id}/{simulation_id}",
                    "protocol": "file",
                },
            ],
        },
        "data_processing": {
            "name": "Data Processing Pipeline",
            "description": "Setup for data analysis and processing workflows",
            "attrs": {"type": "processing", "stage": "analysis"},
            "suggested_locations": [
                {
                    "name": "raw_data",
                    "description": "Raw input data files",
                    "path_prefix": "raw/{project_id}",
                    "protocol": "file",
                },
                {
                    "name": "processed_data",
                    "description": "Processed/cleaned data files",
                    "path_prefix": "processed/{project_id}",
                    "protocol": "file",
                },
                {
                    "name": "results",
                    "description": "Analysis results and outputs",
                    "path_prefix": "results/{project_id}",
                    "protocol": "file",
                },
            ],
        },
        "remote_hpc": {
            "name": "HPC Remote Storage",
            "description": "High-performance computing with remote storage",
            "attrs": {"compute": "hpc", "storage": "remote"},
            "suggested_locations": [
                {
                    "name": "hpc_scratch",
                    "description": "HPC scratch space for temporary files",
                    "path_prefix": "scratch/{username}/{job_id}",
                    "protocol": "sftp",
                },
                {
                    "name": "archive_storage",
                    "description": "Long-term archive storage",
                    "path_prefix": "archive/{project_id}/{simulation_id}",
                    "protocol": "sftp",
                },
            ],
        },
        "cloud_storage": {
            "name": "Cloud-based Workflow",
            "description": "Cloud storage for distributed computing",
            "attrs": {"platform": "cloud", "scalable": "true"},
            "suggested_locations": [
                {
                    "name": "s3_input",
                    "description": "S3 bucket for input data",
                    "path_prefix": "s3://my-bucket/input/{project_id}",
                    "protocol": "s3",
                },
                {
                    "name": "s3_output",
                    "description": "S3 bucket for output data",
                    "path_prefix": "s3://my-bucket/output/{project_id}",
                    "protocol": "s3",
                },
            ],
        },
    }

    if template_name is None:
        console.print("\n[bold blue]🏗️  Template-based Simulation Wizard[/bold blue]")
        console.print(
            "Create simulations from predefined templates. Press Ctrl+C to cancel at any time.\n"
        )

        import questionary

        # Show available templates
        template_choices = [
            {"name": f"{template['name']} - {template['description']}", "value": key}
            for key, template in templates.items()
        ]

        template_name = questionary.select(
            "Select a template:",
            choices=template_choices,
        ).ask()

        if not template_name:
            console.print("\nOperation cancelled.")
            raise click.Abort(1)

    if template_name not in templates:
        console.print(f"[red]Error:[/red] Template '{template_name}' not found.")
        available = ", ".join(templates.keys())
        console.print(f"Available templates: {available}")
        raise click.Abort(1)

    template = templates[template_name]

    console.print(f"\n[bold]Using template:[/bold] {template['name']}")
    console.print(f"[bold]Description:[/bold] {template['description']}")

    import questionary

    # Get simulation details
    sim_id = questionary.text(
        "Enter simulation ID (leave empty for auto-generated UUID):",
        default="",
    ).ask()

    if sim_id is None:
        console.print("\nOperation cancelled.")
        raise click.Abort(1)

    if not sim_id.strip():
        sim_id = None

    # Get simulation path with tab completion
    from prompt_toolkit import prompt
    from prompt_toolkit.completion import PathCompleter

    path_completer = PathCompleter(expanduser=True, only_directories=True)

    path_input = prompt(
        "Enter base filesystem path for simulation (press Tab to complete, Enter to skip): ",
        completer=path_completer,
        complete_while_typing=True,
    )

    path = path_input.strip() if path_input else None

    # Customize template attributes
    console.print(f"\n[bold]Template attributes:[/bold]")
    attrs = {}
    for key, default_value in template["attrs"].items():
        value = questionary.text(
            f"Enter value for '{key}':", default=str(default_value)
        ).ask()
        if value is not None:
            attrs[key] = value

    # Allow additional custom attributes
    while True:
        add_more = questionary.confirm("Add custom attribute?", default=False).ask()

        if not add_more:
            break

        key = questionary.text("Attribute key:").ask()
        if not key:
            break

        value = questionary.text(f"Value for '{key}':").ask()
        if value is not None:
            attrs[key] = value

    # Show simulation summary
    console.print(f"\n[bold]Simulation Summary:[/bold]")
    console.print(f"  [bold]Template:[/bold] {template['name']}")
    console.print(f"  [bold]ID:[/bold] {sim_id or 'Auto-generated UUID'}")
    console.print(f"  [bold]Path:[/bold] {path or 'Not specified'}")
    console.print(f"  [bold]Attributes:[/bold]")
    for key, value in attrs.items():
        console.print(f"    • {key}: {value}")

    # Configure locations
    console.print(f"\n[bold]Suggested locations for this template:[/bold]")
    locations_to_create = []

    for i, loc_template in enumerate(template["suggested_locations"], 1):
        console.print(f"\n[bold]{i}. {loc_template['name']}[/bold]")
        console.print(f"   Description: {loc_template['description']}")
        console.print(f"   Suggested path: {loc_template['path_prefix']}")
        console.print(f"   Protocol: {loc_template['protocol']}")

        include = questionary.confirm(
            f"Include '{loc_template['name']}' location?", default=True
        ).ask()

        if include:
            # Customize location
            location_name = questionary.text(
                "Location name:", default=loc_template["name"]
            ).ask()

            path_prefix = questionary.text(
                "Path prefix template:", default=loc_template["path_prefix"]
            ).ask()

            protocol = questionary.select(
                "Storage protocol:",
                choices=[
                    {"name": "file - Local filesystem", "value": "file"},
                    {"name": "sftp - SSH File Transfer Protocol", "value": "sftp"},
                    {"name": "s3 - Amazon S3", "value": "s3"},
                    {"name": "gs - Google Cloud Storage", "value": "gs"},
                    {"name": "azure - Azure Blob Storage", "value": "azure"},
                ],
                default=loc_template["protocol"],
            ).ask()

            locations_to_create.append(
                {
                    "name": location_name,
                    "path_prefix": path_prefix,
                    "protocol": protocol,
                    "description": loc_template["description"],
                }
            )

    # Final confirmation
    console.print(f"\n[bold]Final Summary:[/bold]")
    console.print(f"  [bold]Simulation ID:[/bold] {sim_id or 'Auto-generated UUID'}")
    console.print(f"  [bold]Locations to create:[/bold] {len(locations_to_create)}")
    for loc in locations_to_create:
        console.print(f"    • {loc['name']} ({loc['protocol']})")

    if not questionary.confirm(
        "\nCreate simulation with template?", default=True
    ).ask():
        console.print("Operation cancelled.")
        return

    # Create the simulation
    try:
        sim = Simulation(simulation_id=sim_id, path=path)

        # Add attributes
        for key, value in attrs.items():
            sim.attrs[key] = value

        # Create and add locations
        for loc_config in locations_to_create:
            # Check if location already exists
            from ..location.location import Location, LocationKind

            existing_location = Location.get_location(loc_config["name"])
            if existing_location:
                location = existing_location
                console.print(
                    f"📍 [yellow]Using existing location:[/yellow] {loc_config['name']}"
                )
            else:
                # Create new location with minimal config
                config = {"protocol": loc_config["protocol"], "storage_options": {}}
                location = Location(
                    name=loc_config["name"],
                    kinds=[LocationKind.DISK],  # Default kind
                    config=config,
                    optional=False,
                )
                console.print(
                    f"📍 [green]Created location:[/green] {loc_config['name']}"
                )

            # Add location to simulation with context
            from .context import LocationContext

            context = LocationContext(path_prefix=loc_config["path_prefix"])
            sim.add_location(location, context=context, override=True)

        # Save simulation
        Simulation.save_simulations()

        console.print(
            Panel.fit(
                f"✅ [bold green]Created template-based simulation:[/bold green] {sim.simulation_id}\n"
                f"[bold]Template:[/bold] {template['name']}\n"
                f"[bold]Locations:[/bold] {len(locations_to_create)}\n"
                f"[bold]Path:[/bold] {path or 'Not specified'}",
                title="Template Simulation Created",
            )
        )

        # Show next steps
        console.print(f"\n[bold]Next steps:[/bold]")
        console.print("  1. Configure location credentials if needed")
        console.print("  2. Test connectivity with 'tellus simulation location ls'")
        console.print(
            "  3. Start transferring data with 'tellus simulation location get/mget'"
        )

    except Exception as e:
        console.print(f"[red]Error creating simulation:[/red] {str(e)}")
        raise click.Abort(1)


@simulation.command()
@click.argument("sim_id", required=False)
@click.option(
    "--force",
    is_flag=True,
    help="Force removal without confirmation",
)
def delete(sim_id: Optional[str] = None, force: bool = False):
    """Delete a simulation.

    If no SIM_ID is provided, an interactive wizard will guide you through the selection process.

    SIM_ID: ID of the simulation to delete
    """
    # Interactive wizard when no sim_id is provided
    if sim_id is None:
        console.print("\n[bold blue]🗑️  Simulation Deletion Wizard[/bold blue]")
        console.print(
            "Select a simulation to delete. Press Ctrl+C to cancel at any time.\n"
        )

        # List available simulations
        bridge = _get_simulation_bridge()

        if bridge:
            # Use new architecture
            try:
                simulations = bridge.list_simulations_legacy_format()
                if not simulations:
                    console.print("No simulations found.")
                    return
            except ApplicationError as e:
                console.print(f"[red]Error:[/red] {str(e)}")
                return
        else:
            # Use legacy architecture
            simulations = Simulation.list_simulations()
            if not simulations:
                console.print("No simulations found.")
                return
            # Convert legacy objects to dict format for consistent processing
            simulations = [
                {
                    "simulation_id": sim.simulation_id,
                    "path": str(sim.path) if sim.path else None,
                    "attrs": sim.attrs or {},
                    "locations": sim.locations or {},
                }
                for sim in simulations
            ]

        # Create choices for questionary
        import questionary

        choices = [
            {
                "name": f"{sim['simulation_id']} - {sim['path'] or 'No path'} ({len(sim.get('locations', {}))} locations)",
                "value": sim["simulation_id"],
            }
            for sim in sorted(simulations, key=lambda s: s["simulation_id"])
        ]

        # Let user select simulation to delete
        sim_id = questionary.select(
            "Select a simulation to delete:",
            choices=choices,
        ).ask()

        if not sim_id:  # User cancelled
            console.print("\nOperation cancelled.")
            return

    # This will raise an error if the simulation doesn't exist
    get_simulation_or_exit(sim_id)

    if not force:
        # Show simulation details before deletion
        console.print("\n[bold]Simulation to be deleted:[/bold]")
        show.callback(sim_id)
        console.print("\n")

        # Use questionary for all confirmation prompts
        import questionary

        confirmed = questionary.confirm(
            f"Are you sure you want to delete simulation '{sim_id}'?",
            default=False,
        ).ask()

        if not confirmed:
            console.print("Operation cancelled.")
            return

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
