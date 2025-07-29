"""Command-line interface for Tellus simulation management."""

import json
from typing import Dict, Any, Optional
import rich_click as click
from rich.console import Console
from rich.table import Table
from rich.panel import Panel

from .simulation import Simulation

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


@simulation.command()
@click.argument("sim_id", required=False)
@click.option("--path", help="Filesystem path for the simulation data")
def create(sim_id: str | None = None, path: str = None):
    """Create a new simulation

    SIM_ID: Optional identifier for the simulation. If not provided, a UUID will be generated.
    """
    sim = Simulation(simulation_id=sim_id, path=path)
    simulations[sim.simulation_id] = sim
    console.print(f"✅ Created simulation with ID: {sim.simulation_id}")
    if path:
        console.print(f"  Path: {path}")


@simulation.command()
@click.argument("sim_id")
@click.option("--force", "-f", is_flag=True, help="Force removal without confirmation")
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
            f"Are you sure you want to delete simulation '{sim_id}'?",
            abort=True,
        )

    del simulations[sim_id]
    console.print(f"✅ Removed simulation: {sim_id}")


@simulation.command()
@click.argument("sim_id")
def show(sim_id: str):
    """Show details of a simulation

    SIM_ID: ID of the simulation to show
    """
    sim = get_simulation_or_exit(sim_id)

    info = [
        f"[bold]ID:[/bold] {sim.simulation_id}",
        f"[bold]Path:[/bold] {sim.path or 'Not specified'}",
    ]

    # List locations associated with this simulation
    locations = sim.list_locations()
    if locations:
        info.append("\n[bold]Locations:[/bold]")
        for loc_name in locations:
            info.append(f"  - {loc_name}")

    console.print(Panel("\n".join(info), title=f"Simulation: {sim_id}", expand=False))
