"""Clean architecture CLI for simulation management."""

import rich_click as click
from rich.console import Console
from rich.table import Table
from rich.panel import Panel

from .main import cli, console
from ...application.container import get_service_container
from ...application.dtos import CreateSimulationDto, UpdateSimulationDto, SimulationLocationAssociationDto

def _get_simulation_service():
    """
    Get simulation service from the service container.
    
    Returns
    -------
    SimulationApplicationService
        Configured simulation service instance with repository dependencies.
        
    Examples
    --------
    >>> service = _get_simulation_service()
    >>> service is not None
    True
    """
    service_container = get_service_container()
    return service_container.service_factory.simulation_service


@cli.group()
def simulation():
    """
    Manage Earth System Model simulations.
    
    Provides commands for creating, updating, and managing simulation
    configurations including metadata, location associations, and
    workflow integration.
    
    Examples
    --------
    >>> # List all simulations
    >>> # tellus simulation list
    >>> 
    >>> # Create a new simulation
    >>> # tellus simulation create climate-run-001 --model CESM2 --attr experiment=historical
    >>> 
    >>> # Show detailed information
    >>> # tellus simulation show climate-run-001
    >>> 
    >>> # Associate with storage location
    >>> # tellus simulation add-location climate-run-001 hpc-storage
    
    Notes
    -----
    Simulations represent computational experiments or datasets with:
    - Unique identifiers for tracking and reference
    - Model and experiment metadata
    - Storage location associations
    - Path templating for organized data layout
    - Integration with workflow systems
    """
    pass


@simulation.command(name="list")
def list_simulations():
    """
    List all configured simulations with summary information.
    
    Displays a formatted table of simulations showing their ID, path,
    number of associated locations, and available attributes. Useful
    for getting an overview of all simulation configurations.
    
    Examples
    --------
    >>> # Command line usage:
    >>> # tellus simulation list
    >>> # 
    >>> # ┏━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
    >>> # ┃                ┃                                                                ┃                                                                ┃                                                                ┃
    >>> # ┃ ID             ┃ Path                                                           ┃ # Locations                                                    ┃ Attributes                                                     ┃
    >>> # ┃                ┃                                                                ┃                                                                ┃                                                                ┃
    >>> # ┡━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
    >>> # │ climate-run-01 │ /data/cesm/historical                                          │ 2                                                              │ model, experiment, years                                       │
    >>> # │ ocean-analysis │ /projects/ocean/mpiom                                          │ 1                                                              │ model, resolution                                              │
    >>> # └────────────────┴────────────────────────────────────────────────────────────────┴────────────────────────────────────────────────────────────────┴────────────────────────────────────────────────────────────────┘
    
    Notes
    -----
    - Shows summary information only; use 'show' command for detailed view
    - Simulations are sorted alphabetically by ID
    - Location count includes all associated storage locations
    - Attributes column shows attribute keys, not values
    
    See Also
    --------
    tellus simulation show : Get detailed simulation information
    tellus simulation create : Create a new simulation
    """
    try:
        service = _get_simulation_service()
        result = service.list_simulations()
        simulations = result.simulations
        
        if not simulations:
            console.print("No simulations found.")
            return
            
        table = Table(
            title="Available Simulations", show_header=True, header_style="bold magenta"
        )
        table.add_column("ID", style="cyan")
        table.add_column("# Locations", style="blue")
        table.add_column("# Attributes", style="yellow")
        table.add_column("# Workflows", style="green")
        table.add_column("# Files", style="red")

        for sim in sorted(simulations, key=lambda s: s.simulation_id):
            num_locations = len(sim.associated_locations)
            num_attributes = len(sim.attrs)
            num_workflows = len(sim.snakemakes) if hasattr(sim, 'snakemakes') and sim.snakemakes else 0
            num_files = 0  # File tracking not yet implemented
            table.add_row(
                sim.simulation_id, 
                str(num_locations), 
                str(num_attributes), 
                str(num_workflows), 
                str(num_files)
            )

        console.print(Panel.fit(table))
        
    except Exception as e:
        console.print(f"[red]Error:[/red] {str(e)}")


@simulation.command(name="create")
@click.argument("sim_id")
@click.option("--model-id", help="Model identifier")
@click.option("--path", help="Simulation path")
def create_simulation(sim_id: str, model_id: str = None, path: str = None):
    """Create a new simulation."""
    try:
        service = _get_simulation_service()
        
        dto = CreateSimulationDto(
            simulation_id=sim_id,
            model_id=model_id,
            path=path
        )
        
        result = service.create_simulation(dto)
        console.print(f"[green]✓[/green] Created simulation: {result.simulation_id}")
        
    except Exception as e:
        console.print(f"[red]Error:[/red] {str(e)}")


@simulation.command(name="show")
@click.argument("sim_id")
def show_simulation(sim_id: str):
    """Show details for a simulation."""
    try:
        service = _get_simulation_service()
        sim = service.get_simulation(sim_id)
        
        table = Table(title=f"Simulation: {sim_id}")
        table.add_column("Property", style="cyan")
        table.add_column("Value", style="green")
        
        table.add_row("ID", sim.simulation_id)
        table.add_row("Model ID", sim.model_id or "-")
        table.add_row("Path", sim.path or "-")
        table.add_row("Locations", ", ".join(sim.associated_locations) if sim.associated_locations else "-")
        table.add_row("Attributes", str(len(sim.attrs)) + " items" if sim.attrs else "-")
        
        console.print(Panel.fit(table))
        
    except Exception as e:
        console.print(f"[red]Error:[/red] {str(e)}")