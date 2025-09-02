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
@click.option("--location", help="Filter simulations by location (supports regex patterns)")
@click.option("--json", "output_json", is_flag=True, help="Output in JSON format")
def list_simulations(location: str = None, output_json: bool = False):
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
        
        # Apply location filter if provided (supports regex)
        if location:
            import re
            try:
                # Try to compile the pattern as regex
                location_pattern = re.compile(location, re.IGNORECASE)
                filtered_simulations = []
                for sim in simulations:
                    # Check if simulation has locations matching the pattern
                    if hasattr(sim, 'associated_locations') and sim.associated_locations:
                        for loc_name in sim.associated_locations.keys():
                            if location_pattern.search(loc_name):
                                filtered_simulations.append(sim)
                                break
                simulations = filtered_simulations
            except re.error as e:
                console.print(f"[red]Error:[/red] Invalid regex pattern '{location}': {e}")
                return
        
        if not simulations:
            if location:
                console.print(f"No simulations found matching location pattern '{location}'.")
            else:
                console.print("No simulations found.")
            return
            
        # JSON output
        if output_json:
            import json
            # Output a simple dict with simulations list
            output = {
                "simulations": [
                    json.loads(sim.to_json()) if hasattr(sim, 'to_json') else sim.__dict__
                    for sim in simulations
                ]
            }
            console.print(json.dumps(output, indent=2))
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
@click.argument("expid", required=False)
@click.option("--location", help="Location where simulation will be stored")
@click.option("--model-id", help="Model identifier")
@click.option("--path", help="Simulation path")
@click.option("--json", "output_json", is_flag=True, help="Output in JSON format")
def create_simulation(expid: str = None, location: str = None, model_id: str = None, path: str = None, output_json: bool = False):
    """Create a new simulation.
    
    If no expid is provided, launches an interactive wizard to gather information.
    """
    try:
        service = _get_simulation_service()
        
        # If no expid provided, launch interactive wizard
        if not expid:
            import questionary
            
            expid = questionary.text(
                "Simulation ID (expid):",
                validate=lambda text: True if text.strip() else "Simulation ID is required"
            ).ask()
            
            if not expid:
                console.print("[dim]Operation cancelled[/dim]")
                return
                
        # If no location provided and interactive mode
        if not location:
            import questionary
            
            # Get available locations
            location_service = get_service_container().service_factory.location_service
            locations_result = location_service.list_locations()
            
            if locations_result.locations:
                location_choices = [loc.name for loc in locations_result.locations]
                location = questionary.select(
                    "Select location for simulation:",
                    choices=location_choices
                ).ask()
        
        dto = CreateSimulationDto(
            simulation_id=expid,
            model_id=model_id,
            path=path
        )
        
        result = service.create_simulation(dto)
        
        if output_json:
            console.print(result.pretty_json())
        else:
            console.print(f"[green]✓[/green] Created simulation: {result.simulation_id}")
            if location:
                console.print(f"[dim]Location: {location}[/dim]")
        
    except Exception as e:
        console.print(f"[red]Error:[/red] {str(e)}")


@simulation.command(name="show")
@click.argument("expid", required=False)
@click.option("--json", "output_json", is_flag=True, help="Output in JSON format")
def show_simulation(expid: str = None, output_json: bool = False):
    """Show details for a simulation."""
    try:
        # If no expid provided, launch interactive selection
        if not expid:
            import questionary
            service = _get_simulation_service()
            simulations_result = service.list_simulations()
            
            if not simulations_result.simulations:
                console.print("[yellow]No simulations found[/yellow]")
                return
            
            sim_choices = [sim.simulation_id for sim in simulations_result.simulations]
            expid = questionary.select(
                "Select simulation to show:",
                choices=sim_choices
            ).ask()
            
            if not expid:
                console.print("[dim]Operation cancelled[/dim]")
                return
        
        service = _get_simulation_service()
        sim = service.get_simulation(expid)
        
        if not sim:
            console.print(f"[red]Error:[/red] Simulation '{expid}' not found")
            return
        
        # JSON output
        if output_json:
            console.print(sim.pretty_json() if hasattr(sim, 'pretty_json') else '{}')
            return
        
        table = Table(title=f"Simulation: {expid}")
        table.add_column("Property", style="cyan")
        table.add_column("Value", style="green")
        
        table.add_row("ID", sim.simulation_id)
        table.add_row("Locations", ", ".join(sim.associated_locations) if sim.associated_locations else "-")
        
        # Show actual attributes with their values
        if sim.attrs:
            for key, value in sim.attrs.items():
                table.add_row(f"  {key}", str(value))
        else:
            table.add_row("Attributes", "-")
            
        # Show workflows if any
        if hasattr(sim, 'workflows') and sim.workflows:
            table.add_row("Workflows", f"{len(sim.workflows)} defined")
        
        # Show namelists if any  
        if hasattr(sim, 'namelists') and sim.namelists:
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


@simulation.command(name="update")
@click.argument("expid", required=False)
@click.option("--json", "output_json", is_flag=True, help="Output in JSON format")
@click.option("--param", multiple=True, help="Update parameter in key=value format")
def update_simulation(expid: str = None, output_json: bool = False, param: tuple = ()):
    """Update simulation parameters programmatically.
    
    If no expid is provided, launches an interactive wizard to select a simulation.
    Use --param multiple times to update multiple parameters.
    
    Examples:
        tellus simulation update exp001 --param model=FESOM2 --param years=100
        tellus simulation update  # Interactive selection
    """
    try:
        service = _get_simulation_service()
        
        # If no expid provided, launch interactive selection  
        if not expid:
            import questionary
            
            simulations = service.list_simulations()
            if not simulations.simulations:
                console.print("No simulations found.")
                return
                
            choices = [f"{sim.simulation_id}" + (f" - {sim.model_id}" if hasattr(sim, 'model_id') and sim.model_id else '') 
                      for sim in simulations.simulations]
            
            selected = questionary.select("Select simulation to update:", choices=choices).ask()
            if not selected:
                console.print("[dim]No simulation selected[/dim]")
                return
                
            expid = selected.split(" -")[0].strip()
            
        # Get existing simulation
        try:
            existing_sim = service.get_simulation(expid)
        except Exception:
            console.print(f"[red]Error:[/red] Simulation '{expid}' not found")
            return
            
        # Parse parameters
        updates = {}
        for p in param:
            if "=" not in p:
                console.print(f"[red]Error:[/red] Invalid parameter format: {p}. Use key=value")
                return
            key, value = p.split("=", 1)
            updates[key.strip()] = value.strip()
            
        if not updates and not param:
            console.print("[yellow]No parameters to update. Use --param key=value[/yellow]")
            return
            
        # Show what will be updated
        console.print(f"[dim]Updating simulation '{expid}':[/dim]")
        for key, value in updates.items():
            console.print(f"  {key} → {value}")
            
        # Perform update
        update_dto = UpdateSimulationDto(**updates)
        result = service.update_simulation(expid, update_dto)
        
        if output_json:
            console.print(result.pretty_json())
        else:
            console.print(f"[green]✓[/green] Updated simulation: {result.simulation_id}")
            for key, value in updates.items():
                console.print(f"[dim]  {key}: {value}[/dim]")
                
    except Exception as e:
        console.print(f"[red]Error:[/red] {str(e)}")


@simulation.command(name="delete")
@click.argument("expid", required=False)  
@click.option("--force", "-f", is_flag=True, help="Skip confirmation prompt")
@click.option("--json", "output_json", is_flag=True, help="Output in JSON format")
def delete_simulation(expid: str = None, force: bool = False, output_json: bool = False):
    """Delete a simulation.
    
    If no expid is provided, launches an interactive wizard to select a simulation.
    
    Examples:
        tellus simulation delete exp001
        tellus simulation delete --force exp001  # Skip confirmation
        tellus simulation delete  # Interactive selection
    """
    try:
        service = _get_simulation_service()
        
        # If no expid provided, launch interactive selection
        if not expid:
            import questionary
            
            simulations = service.list_simulations()
            if not simulations.simulations:
                console.print("No simulations found.")
                return
                
            choices = [f"{sim.simulation_id}" + (f" - {sim.model_id}" if hasattr(sim, 'model_id') and sim.model_id else '') 
                      for sim in simulations.simulations]
            
            selected = questionary.select("Select simulation to delete:", choices=choices).ask()
            if not selected:
                console.print("[dim]No simulation selected[/dim]")
                return
                
            expid = selected.split(" -")[0].strip()
            
        # Check if simulation exists
        try:
            existing_sim = service.get_simulation(expid)
        except Exception:
            console.print(f"[red]Error:[/red] Simulation '{expid}' not found")
            return
            
        # Confirmation prompt unless forced
        if not force:
            import questionary
            
            if not questionary.confirm(f"Are you sure you want to delete simulation '{expid}'?").ask():
                console.print("[dim]Operation cancelled[/dim]")
                return
                
        # Perform deletion
        service.delete_simulation(expid)
        
        if output_json:
            import json
            delete_result = {"simulation_id": expid, "status": "deleted"}
            console.print(json.dumps(delete_result, indent=2))
        else:
            console.print(f"[green]✓[/green] Deleted simulation: {expid}")
            
    except Exception as e:
        console.print(f"[red]Error:[/red] {str(e)}")


# Note: Simulation Location subcommands are implemented in simulation_extended.py
# to avoid conflicts with existing implementation


# Simulation File subcommands (per CLI specification)

@simulation.group()
def file():
    """Manage files associated with simulations."""
    pass


@file.command(name="create")
@click.argument("simulation_id", required=False)
@click.argument("file_path", required=False)
@click.option("--json", "output_json", is_flag=True, help="Output in JSON format")
def create_simulation_file(simulation_id: str = None, file_path: str = None, output_json: bool = False):
    """Attach a file to a simulation.
    
    If arguments are not provided, launches an interactive wizard.
    
    Examples:
        tellus simulation file create exp001 /path/to/output.nc
        tellus simulation file create  # Interactive mode
    """
    try:
        service = _get_unified_file_service()
        
        # Interactive mode if arguments missing
        if not simulation_id or not file_path:
            import questionary
            
            if not simulation_id:
                sim_service = _get_simulation_service()
                simulations = sim_service.list_simulations()
                if not simulations.simulations:
                    console.print("No simulations found.")
                    return
                    
                choices = [f"{sim.simulation_id}" for sim in simulations.simulations]
                simulation_id = questionary.select("Select simulation:", choices=choices).ask()
                
                if not simulation_id:
                    console.print("[dim]No simulation selected[/dim]")
                    return
                    
            if not file_path:
                file_path = questionary.text("File path:").ask()
                
                if not file_path:
                    console.print("[dim]No file path provided[/dim]")
                    return
        
        # Register file with simulation using unified file service
        from ...application.dtos import FileRegistrationDto
        from ...domain.entities.simulation_file import FileContentType, FileImportance
        
        registration_dto = FileRegistrationDto(
            simulation_id=simulation_id,
            file_path=file_path,
            content_type=FileContentType.OUTPUT,  # Default, could be made configurable
            importance=FileImportance.NORMAL,     # Default, could be made configurable
            description=f"File registered via CLI: {file_path}"
        )
        
        result = service.register_file(registration_dto)
        
        if output_json:
            console.print(result.pretty_json() if hasattr(result, 'pretty_json') else '{"status": "registered"}')
        else:
            console.print(f"[green]✓[/green] Registered file '{file_path}' with simulation '{simulation_id}'")
            
    except Exception as e:
        console.print(f"[red]Error:[/red] {str(e)}")


@file.command(name="show")
@click.argument("file_id", required=False)
@click.option("--json", "output_json", is_flag=True, help="Output in JSON format")
def show_simulation_file(file_id: str = None, output_json: bool = False):
    """Display details of a file.
    
    If no file-id is provided, launches interactive selection.
    """
    try:
        service = _get_unified_file_service()
        
        if not file_id:
            import questionary
            console.print("Interactive file selection not yet fully implemented.")
            console.print("Please specify a file-id")
            return
            
        # Get file details from unified file service
        file_details = service.get_file_details(file_id)
        
        if output_json:
            console.print(file_details.pretty_json() if hasattr(file_details, 'pretty_json') else '{}')
        else:
            console.print(f"File ID: {file_id}")
            # Display file details in table format
            
    except Exception as e:
        console.print(f"[red]Error:[/red] {str(e)}")


@file.command(name="list")
@click.argument("simulation_id", required=False)
@click.option("--json", "output_json", is_flag=True, help="Output in JSON format")
def list_simulation_files(simulation_id: str = None, output_json: bool = False):
    """List files associated with a simulation.
    
    If no simulation-id is provided, launches interactive selection.
    """
    try:
        service = _get_unified_file_service()
        
        if not simulation_id:
            import questionary
            
            sim_service = _get_simulation_service()
            simulations = sim_service.list_simulations()
            if not simulations.simulations:
                console.print("No simulations found.")
                return
                
            choices = [f"{sim.simulation_id}" for sim in simulations.simulations]
            simulation_id = questionary.select("Select simulation:", choices=choices).ask()
            
            if not simulation_id:
                console.print("[dim]No simulation selected[/dim]")
                return
                
        # List files for simulation using unified file service
        files = service.list_simulation_files(simulation_id)
        
        if output_json:
            console.print(files.pretty_json() if hasattr(files, 'pretty_json') else '[]')
        else:
            if not files:
                console.print(f"No files registered for simulation '{simulation_id}'")
            else:
                console.print(f"Files for simulation '{simulation_id}':")
                # Display files in table format
                
    except Exception as e:
        console.print(f"[red]Error:[/red] {str(e)}")


@file.command(name="edit")
@click.argument("file_id", required=False)
@click.option("--json", "output_json", is_flag=True, help="Output in JSON format")
def edit_simulation_file(file_id: str = None, output_json: bool = False):
    """Edit file metadata.
    
    If no file-id is provided, launches interactive selection.
    """
    try:
        console.print("Edit functionality for simulation files not yet implemented.")
        console.print("This would open an editor for file metadata.")
        
    except Exception as e:
        console.print(f"[red]Error:[/red] {str(e)}")


@file.command(name="update")
@click.argument("file_id", required=False)
@click.option("--json", "output_json", is_flag=True, help="Output in JSON format")
def update_simulation_file(file_id: str = None, output_json: bool = False):
    """Update file metadata programmatically.
    
    If no file-id is provided, launches interactive selection.
    """
    try:
        console.print("Update functionality for simulation files not yet implemented.")
        console.print("This would allow programmatic updates to file metadata.")
        
    except Exception as e:
        console.print(f"[red]Error:[/red] {str(e)}")


@file.command(name="delete")
@click.argument("file_id", required=False)
@click.option("--force", "-f", is_flag=True, help="Skip confirmation prompt")
@click.option("--json", "output_json", is_flag=True, help="Output in JSON format")
def delete_simulation_file(file_id: str = None, force: bool = False, output_json: bool = False):
    """Remove a file from a simulation.
    
    If no file-id is provided, launches interactive selection.
    """
    try:
        service = _get_unified_file_service()
        
        if not file_id:
            import questionary
            console.print("Interactive file selection for deletion not yet implemented.")
            console.print("Please specify a file-id")
            return
            
        # Confirmation unless forced
        if not force:
            import questionary
            
            if not questionary.confirm(f"Are you sure you want to remove file '{file_id}' from the simulation?").ask():
                console.print("[dim]Operation cancelled[/dim]")
                return
                
        # Remove file using unified file service
        service.remove_file(file_id)
        
        if output_json:
            import json
            result = {"file_id": file_id, "status": "removed"}
            console.print(json.dumps(result, indent=2))
        else:
            console.print(f"[green]✓[/green] Removed file: {file_id}")
            
    except Exception as e:
        console.print(f"[red]Error:[/red] {str(e)}")


# Simulation Archive subcommands (per CLI specification)

@simulation.group()
def archive():
    """Manage archived outputs for simulations (special case of files)."""
    pass


@archive.command(name="create")
@click.argument("simulation_id", required=False)
@click.argument("archive_name", required=False)
@click.option("--json", "output_json", is_flag=True, help="Output in JSON format")
def create_simulation_archive(simulation_id: str = None, archive_name: str = None, output_json: bool = False):
    """Create a new archive for a simulation.
    
    If arguments are not provided, launches an interactive wizard.
    
    Examples:
        tellus simulation archive create exp001 results_archive
        tellus simulation archive create  # Interactive mode
    """
    try:
        service = _get_unified_file_service()
        
        # Interactive mode if arguments missing
        if not simulation_id or not archive_name:
            import questionary
            
            if not simulation_id:
                sim_service = _get_simulation_service()
                simulations = sim_service.list_simulations()
                if not simulations.simulations:
                    console.print("No simulations found.")
                    return
                    
                choices = [f"{sim.simulation_id}" for sim in simulations.simulations]
                simulation_id = questionary.select("Select simulation:", choices=choices).ask()
                
                if not simulation_id:
                    console.print("[dim]No simulation selected[/dim]")
                    return
                    
            if not archive_name:
                archive_name = questionary.text("Archive name:").ask()
                
                if not archive_name:
                    console.print("[dim]No archive name provided[/dim]")
                    return
        
        # Create archive using unified file service (archives are SimulationFiles with file_type=ARCHIVE)
        from ...application.dtos import FileRegistrationDto
        from ...domain.entities.simulation_file import FileContentType, FileImportance
        
        registration_dto = FileRegistrationDto(
            simulation_id=simulation_id,
            file_path=f"archives/{archive_name}.tar.gz",  # Default archive path
            content_type=FileContentType.ARCHIVE,
            importance=FileImportance.HIGH,  # Archives are typically important
            description=f"Archive created via CLI: {archive_name}"
        )
        
        result = service.register_file(registration_dto)
        
        if output_json:
            console.print(result.pretty_json() if hasattr(result, 'pretty_json') else '{"status": "created"}')
        else:
            console.print(f"[green]✓[/green] Created archive '{archive_name}' for simulation '{simulation_id}'")
            
    except Exception as e:
        console.print(f"[red]Error:[/red] {str(e)}")


@archive.command(name="show")
@click.argument("archive_id", required=False)
@click.option("--json", "output_json", is_flag=True, help="Output in JSON format")
def show_simulation_archive(archive_id: str = None, output_json: bool = False):
    """Show details of an archive.
    
    If no archive-id is provided, launches interactive selection.
    """
    try:
        service = _get_unified_file_service()
        
        if not archive_id:
            import questionary
            console.print("Interactive archive selection not yet fully implemented.")
            console.print("Please specify an archive-id")
            return
            
        # Get archive details (archives are SimulationFiles with file_type=ARCHIVE)
        archive_details = service.get_file_details(archive_id)
        
        if output_json:
            console.print(archive_details.pretty_json() if hasattr(archive_details, 'pretty_json') else '{}')
        else:
            console.print(f"Archive ID: {archive_id}")
            # Display archive details in table format
            
    except Exception as e:
        console.print(f"[red]Error:[/red] {str(e)}")


@archive.command(name="list")
@click.argument("simulation_id", required=False)
@click.option("--json", "output_json", is_flag=True, help="Output in JSON format")
def list_simulation_archives(simulation_id: str = None, output_json: bool = False):
    """List all archives for a simulation.
    
    If no simulation-id is provided, launches interactive selection.
    """
    try:
        service = _get_unified_file_service()
        
        if not simulation_id:
            import questionary
            
            sim_service = _get_simulation_service()
            simulations = sim_service.list_simulations()
            if not simulations.simulations:
                console.print("No simulations found.")
                return
                
            choices = [f"{sim.simulation_id}" for sim in simulations.simulations]
            simulation_id = questionary.select("Select simulation:", choices=choices).ask()
            
            if not simulation_id:
                console.print("[dim]No simulation selected[/dim]")
                return
                
        # List archives for simulation (filter SimulationFiles by file_type=ARCHIVE)
        archives = service.list_simulation_archives(simulation_id)
        
        if output_json:
            console.print(archives.pretty_json() if hasattr(archives, 'pretty_json') else '[]')
        else:
            if not archives:
                console.print(f"No archives found for simulation '{simulation_id}'")
            else:
                console.print(f"Archives for simulation '{simulation_id}':")
                # Display archives in table format
                
    except Exception as e:
        console.print(f"[red]Error:[/red] {str(e)}")


@archive.command(name="edit")
@click.argument("archive_id", required=False)
@click.option("--json", "output_json", is_flag=True, help="Output in JSON format")
def edit_simulation_archive(archive_id: str = None, output_json: bool = False):
    """Edit archive metadata.
    
    If no archive-id is provided, launches interactive selection.
    """
    try:
        console.print("Edit functionality for simulation archives not yet implemented.")
        console.print("This would open an editor for archive metadata.")
        
    except Exception as e:
        console.print(f"[red]Error:[/red] {str(e)}")


@archive.command(name="update")
@click.argument("archive_id", required=False)
@click.option("--json", "output_json", is_flag=True, help="Output in JSON format")
def update_simulation_archive(archive_id: str = None, output_json: bool = False):
    """Update archive metadata programmatically.
    
    If no archive-id is provided, launches interactive selection.
    """
    try:
        console.print("Update functionality for simulation archives not yet implemented.")
        console.print("This would allow programmatic updates to archive metadata.")
        
    except Exception as e:
        console.print(f"[red]Error:[/red] {str(e)}")


@archive.command(name="delete")
@click.argument("archive_id", required=False)
@click.option("--force", "-f", is_flag=True, help="Skip confirmation prompt")
@click.option("--json", "output_json", is_flag=True, help="Output in JSON format")
def delete_simulation_archive(archive_id: str = None, force: bool = False, output_json: bool = False):
    """Delete an archive.
    
    If no archive-id is provided, launches interactive selection.
    """
    try:
        service = _get_unified_file_service()
        
        if not archive_id:
            import questionary
            console.print("Interactive archive selection for deletion not yet implemented.")
            console.print("Please specify an archive-id")
            return
            
        # Confirmation unless forced
        if not force:
            import questionary
            
            if not questionary.confirm(f"Are you sure you want to delete archive '{archive_id}'? This action cannot be undone.").ask():
                console.print("[dim]Operation cancelled[/dim]")
                return
                
        # Delete archive using unified file service (archives are SimulationFiles)
        service.remove_file(archive_id)
        
        if output_json:
            import json
            result = {"archive_id": archive_id, "status": "deleted"}
            console.print(json.dumps(result, indent=2))
        else:
            console.print(f"[green]✓[/green] Deleted archive: {archive_id}")
            console.print("[yellow]Warning:[/yellow] Archive data has been permanently removed.")
            
    except Exception as e:
        console.print(f"[red]Error:[/red] {str(e)}")