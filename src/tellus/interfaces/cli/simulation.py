"""CLI for simulation management."""

import rich_click as click
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from ...application.container import get_service_container
from ...application.dtos import (CreateSimulationDto,
                                 SimulationLocationAssociationDto,
                                 UpdateSimulationDto)
from .main import cli, console


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


def _get_unified_file_service():
    """
    Get unified file service from the service container.
    
    Returns
    -------
    UnifiedFileService
        Configured unified file service for file operations.
    """
    service_container = get_service_container()
    return service_container.service_factory.unified_file_service


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

        # Get unified file service for file counting
        unified_file_service = _get_unified_file_service()
        
        for sim in sorted(simulations, key=lambda s: s.simulation_id):
            num_locations = len(sim.associated_locations)
            num_attributes = len(sim.attrs)
            num_workflows = len(sim.snakemakes) if hasattr(sim, 'snakemakes') and sim.snakemakes else 0
            
            # Get actual file count from unified system
            try:
                simulation_files = unified_file_service.get_simulation_files(sim.simulation_id)
                num_files = len(simulation_files)
            except Exception:
                num_files = 0  # Fallback to 0 if service fails
            
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
        table.add_row("Locations", ", ".join(sim.associated_locations) if sim.associated_locations else "-")
        
        # Show actual attributes with their values
        if sim.attributes:
            for key, value in sim.attributes.items():
                table.add_row(f"  {key}", str(value))
        else:
            table.add_row("Attributes", "-")
            
        # Show workflows if any
        if sim.workflows:
            table.add_row("Workflows", f"{len(sim.workflows)} defined")
        
        # Show namelists if any  
        if sim.namelists:
            table.add_row("Namelists", f"{len(sim.namelists)} files")
        
        console.print(Panel.fit(table))
        
    except Exception as e:
        console.print(f"[red]Error:[/red] {str(e)}")


@simulation.command(name="edit")
@click.argument("sim_id", required=False)
@click.option("--dry-run", is_flag=True, help="Show metadata JSON without opening editor")
def edit_simulation(sim_id: str = None, dry_run: bool = False):
    """Edit simulation metadata in vim.
    
    Opens the simulation metadata in your default editor (vim) for direct editing.
    The metadata is presented in JSON format with editable fields clearly separated
    from readonly fields.
    
    If no simulation ID is provided, launches an interactive simulation selection.
    
    Examples
    --------
    # Edit specific simulation
    tellus simulation edit my-sim-001
    
    # Interactive selection and editing  
    tellus simulation edit
    
    # Preview metadata format without editing
    tellus simulation edit my-sim-001 --dry-run
    """
    import json
    import subprocess
    import tempfile
    from pathlib import Path
    
    try:
        service = _get_simulation_service()
        
        # If no sim_id provided, launch interactive selection
        if not sim_id:
            import questionary
            
            # Get all simulations for selection
            simulations = service.list_simulations()
            if not simulations.simulations:
                console.print("[yellow]No simulations found[/yellow]")
                return
                
            sim_choices = [f"{sim.simulation_id}" + (f" - {sim.model_id}" if hasattr(sim, 'model_id') and sim.model_id else '') 
                          for sim in simulations.simulations]
            
            selected = questionary.select(
                "Select simulation to edit:",
                choices=sim_choices,
                style=questionary.Style([
                    ('question', 'bold'),
                    ('selected', 'fg:#cc5454'),
                    ('pointer', 'fg:#ff0066 bold'),
                ])
            ).ask()
            
            if not selected:
                console.print("[yellow]No simulation selected[/yellow]")
                return
                
            # Extract sim_id from selection
            sim_id = selected.split(" - ")[0]
        
        # Get simulation metadata
        try:
            metadata_result = service.get_simulation(sim_id)
        except Exception as e:
            console.print(f"[red]Error:[/red] Simulation '{sim_id}' not found: {e}")
            return
        
        # Create editable JSON structure - now matches storage format!
        editable_data = {
            "simulation_id": metadata_result.simulation_id,
            "attributes": metadata_result.attributes,
            "locations": metadata_result.locations,
            "_readonly": {
                "uid": metadata_result.uid
            }
        }
        
        # Only add optional sections if they contain data
        if metadata_result.namelists:
            editable_data["namelists"] = metadata_result.namelists
            
        if metadata_result.workflows:
            editable_data["workflows"] = metadata_result.workflows
        
        # Format JSON nicely
        json_content = json.dumps(editable_data, indent=2, default=str)
        
        if dry_run:
            console.print(f"Simulation metadata for '{sim_id}' (editable format):\n")
            console.print(json_content)
            return
        
        # Create temporary file for editing
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as temp_file:
            temp_file.write(json_content)
            temp_file_path = temp_file.name
        
        try:
            # Open in vim (or fall back to $EDITOR)
            editor = 'vim'  # Could be made configurable
            result = subprocess.run([editor, temp_file_path], check=True)
            
            # Read modified content
            with open(temp_file_path, 'r') as f:
                modified_content = f.read()
            
            # Parse and validate JSON
            try:
                modified_data = json.loads(modified_content)
            except json.JSONDecodeError as e:
                console.print(f"[red]Error:[/red] Invalid JSON: {e}")
                console.print("[yellow]Changes not saved[/yellow]")
                return
            
            # Check if simulation_id was changed (not allowed)
            if modified_data["simulation_id"] != metadata_result.simulation_id:
                console.print("[red]Error:[/red] Simulation ID cannot be changed")
                console.print("[yellow]Changes not saved[/yellow]")
                return
            
            # The structure now matches storage format - much simpler!
            update_dto = UpdateSimulationDto(
                attrs=modified_data.get("attributes"),
                namelists=modified_data.get("namelists"),
                snakemakes=modified_data.get("workflows")  # Still need to map back for service compatibility
            )
            
            # Call update service  
            updated_sim = service.update_simulation(sim_id, update_dto)
            
            # Handle location updates if modified
            if "locations" in modified_data:
                original_locations = metadata_result.locations
                if modified_data["locations"] != original_locations:
                    console.print("[yellow]Note:[/yellow] Location updates require separate commands")
                    console.print("Use: tellus simulation location for location context management")
            
            console.print(f"[green]✓[/green] Successfully updated simulation '{sim_id}'")
            
        finally:
            # Clean up temp file
            Path(temp_file_path).unlink(missing_ok=True)
            
    except subprocess.CalledProcessError:
        console.print("[yellow]Editor closed without saving[/yellow]")
    except Exception as e:
        console.print(f"[red]Error:[/red] {str(e)}")


# Note: File management commands moved to simulation_extended.py under the files subgroup