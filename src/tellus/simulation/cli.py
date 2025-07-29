"""Command-line interface for Tellus simulation management."""

from typing import Optional, List
from pathlib import Path

import rich_click as click
from rich.table import Table
from rich.panel import Panel

from .simulation import Simulation, SimulationExistsError
from ..core.cli import cli, console

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
        title="Available Simulations", 
        show_header=True, 
        header_style="bold magenta"
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
@click.option("--path", type=click.Path(file_okay=False, dir_okay=True, resolve_path=True),
              help="Filesystem path for the simulation data")
@click.option("--attr", multiple=True, nargs=2, type=(str, str),
             help="Additional attributes as key=value pairs")
def create(sim_id: Optional[str] = None, path: Optional[str] = None, attr: Optional[List[tuple]] = None):
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
        
        console.print(Panel.fit(
            f"✅ [bold green]Created simulation:[/bold green] {sim.simulation_id}\n"
            f"[bold]Path:[/bold] {path_obj or 'Not specified'}\n"
            f"[bold]Attributes:[/bold] {', '.join(f'{k}={v}' for k, v in sim.attrs.items()) if sim.attrs else 'None'}",
            title="Simulation Created"
        ))
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
    
    console.print(Panel(
        "\n".join(info),
        title=f"Simulation: {sim_id}",
        border_style="blue",
        expand=False
    ))


@simulation.command()
@click.argument("sim_id")
@click.option("--path", type=click.Path(file_okay=False, dir_okay=True, resolve_path=True),
             help="Update the simulation path")
@click.option("--attr", multiple=True, nargs=2, type=(str, str),
             help="Add or update attributes (key value)")
@click.option("--remove-attr", multiple=True, help="Remove an attribute")
def update(sim_id: str, path: Optional[str] = None, 
          attr: Optional[List[tuple]] = None, remove_attr: Optional[List[str]] = None):
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
        console.print("[yellow]No updates specified. Use --path, --attr, or --remove-attr to make changes.[/yellow]")
        return
    
    # Save the updated simulation
    Simulation.save_simulations()
    
    # Show what was updated
    console.print(Panel.fit(
        f"✅ [bold green]Updated simulation:[/bold green] {sim_id}\n" + 
        "\n".join(f"• {update}" for update in updates),
        title="Simulation Updated"
    ))


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
            abort=True
        )
    
    try:
        # Delete the simulation
        Simulation.delete_simulation(sim_id)
        
        console.print(Panel.fit(
            f"✅ [bold green]Deleted simulation:[/bold green] {sim_id}",
            title="Simulation Deleted"
        ))
    except Exception as e:
        console.print(f"[red]Error deleting simulation:[/red] {str(e)}")
        raise click.Abort(1)
