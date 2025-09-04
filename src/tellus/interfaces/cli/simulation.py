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


def _get_location_service():
    """
    Get location service from the service container.
    
    Returns
    -------
    LocationApplicationService
        Configured location service for location operations.
    """
    service_container = get_service_container()
    return service_container.service_factory.location_service


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
@click.pass_context
def list_simulations(ctx, location: str = None):
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
    output_json = ctx.obj.get('output_json', False) if ctx.obj else False
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
@click.pass_context
def create_simulation(ctx, expid: str = None, location: str = None, model_id: str = None, path: str = None):
    """Create a new simulation.
    
    If no expid is provided, launches an interactive wizard to gather information.
    """
    output_json = ctx.obj.get('output_json', False) if ctx.obj else False
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
@click.pass_context
def show_simulation(ctx, expid: str = None):
    """Show details for a simulation."""
    output_json = ctx.obj.get('output_json', False) if ctx.obj else False
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
@click.option("--param", multiple=True, help="Update parameter in key=value format")
@click.pass_context
def update_simulation(ctx, expid: str = None, param: tuple = ()):
    """Update simulation parameters programmatically.
    
    If no expid is provided, launches an interactive wizard to select a simulation.
    Use --param multiple times to update multiple parameters.
    
    Examples:
        tellus simulation update exp001 --param model=FESOM2 --param years=100
        tellus simulation update  # Interactive selection
    """
    output_json = ctx.obj.get('output_json', False) if ctx.obj else False
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
@click.pass_context
def delete_simulation(ctx, expid: str = None, force: bool = False):
    """Delete a simulation.
    
    If no expid is provided, launches an interactive wizard to select a simulation.
    
    Examples:
        tellus simulation delete exp001
        tellus simulation delete --force exp001  # Skip confirmation
        tellus simulation delete  # Interactive selection
    """
    output_json = ctx.obj.get('output_json', False) if ctx.obj else False
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
@click.pass_context
def create_simulation_file(ctx, simulation_id: str = None, file_path: str = None):
    """Attach a file to a simulation.
    
    If arguments are not provided, launches an interactive wizard.
    
    Examples:
        tellus simulation file create exp001 /path/to/output.nc
        tellus simulation file create  # Interactive mode
    """
    output_json = ctx.obj.get('output_json', False) if ctx.obj else False
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
@click.pass_context
def show_simulation_file(ctx, file_id: str = None):
    """Display details of a file.
    
    If no file-id is provided, launches interactive selection.
    """
    output_json = ctx.obj.get('output_json', False) if ctx.obj else False
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
@click.pass_context
def list_simulation_files(ctx, simulation_id: str = None):
    """List files associated with a simulation.
    
    If no simulation-id is provided, launches interactive selection.
    """
    output_json = ctx.obj.get('output_json', False) if ctx.obj else False
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
@click.pass_context
def edit_simulation_file(ctx, file_id: str = None):
    """Edit file metadata.
    
    If no file-id is provided, launches interactive selection.
    """
    output_json = ctx.obj.get('output_json', False) if ctx.obj else False
    try:
        console.print("Edit functionality for simulation files not yet implemented.")
        console.print("This would open an editor for file metadata.")
        
    except Exception as e:
        console.print(f"[red]Error:[/red] {str(e)}")


@file.command(name="update")
@click.argument("file_id", required=False)
@click.pass_context
def update_simulation_file(ctx, file_id: str = None):
    """Update file metadata programmatically.
    
    If no file-id is provided, launches interactive selection.
    """
    output_json = ctx.obj.get('output_json', False) if ctx.obj else False
    try:
        console.print("Update functionality for simulation files not yet implemented.")
        console.print("This would allow programmatic updates to file metadata.")
        
    except Exception as e:
        console.print(f"[red]Error:[/red] {str(e)}")


@file.command(name="delete")
@click.argument("file_id", required=False)
@click.option("--force", "-f", is_flag=True, help="Skip confirmation prompt")
@click.pass_context
def delete_simulation_file(ctx, file_id: str = None, force: bool = False):
    """Remove a file from a simulation.
    
    If no file-id is provided, launches interactive selection.
    """
    output_json = ctx.obj.get('output_json', False) if ctx.obj else False
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
@click.pass_context
def create_simulation_archive(ctx, simulation_id: str = None, archive_name: str = None):
    """Create a new archive for a simulation.
    
    If arguments are not provided, launches an interactive wizard.
    
    Examples:
        tellus simulation archive create exp001 results_archive
        tellus simulation archive create  # Interactive mode
    """
    output_json = ctx.obj.get('output_json', False) if ctx.obj else False
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
@click.pass_context
def show_simulation_archive(ctx, archive_id: str = None):
    """Show details of an archive.
    
    If no archive-id is provided, launches interactive selection.
    """
    output_json = ctx.obj.get('output_json', False) if ctx.obj else False
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
@click.pass_context
def list_simulation_archives(ctx, simulation_id: str = None):
    """List all archives for a simulation.
    
    If no simulation-id is provided, launches interactive selection.
    """
    output_json = ctx.obj.get('output_json', False) if ctx.obj else False
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
            import json
            archive_data = []
            for archive in archives:
                archive_data.append({
                    "archive_id": archive.relative_path,
                    "location": archive.attributes.get('location', ''),
                    "pattern": archive.attributes.get('pattern', ''),
                    "split_parts": archive.attributes.get('split_parts'),
                    "type": archive.attributes.get('archive_type', ''),
                    "format": getattr(archive, 'archive_format', '')
                })
            console.print(json.dumps(archive_data, indent=2))
        else:
            if not archives:
                console.print(f"No archives found for simulation '{simulation_id}'")
            else:
                console.print(f"Archives for simulation '{simulation_id}':")
                
                from rich.table import Table
                table = Table()
                table.add_column("Archive ID")
                table.add_column("Location")
                table.add_column("Pattern")
                table.add_column("Split Parts")
                table.add_column("Type")
                
                for archive in archives:
                    table.add_row(
                        archive.relative_path,
                        archive.attributes.get('location', ''),
                        archive.attributes.get('pattern', ''),
                        str(archive.attributes.get('split_parts', '')) if archive.attributes.get('split_parts') else '',
                        archive.attributes.get('archive_type', '')
                    )
                
                console.print(table)
                
    except Exception as e:
        console.print(f"[red]Error:[/red] {str(e)}")


@archive.command(name="edit")
@click.argument("archive_id", required=False)
@click.pass_context
def edit_simulation_archive(ctx, archive_id: str = None):
    """Edit archive metadata.
    
    If no archive-id is provided, launches interactive selection.
    """
    output_json = ctx.obj.get('output_json', False) if ctx.obj else False
    try:
        console.print("Edit functionality for simulation archives not yet implemented.")
        console.print("This would open an editor for archive metadata.")
        
    except Exception as e:
        console.print(f"[red]Error:[/red] {str(e)}")


@archive.command(name="update")
@click.argument("archive_id", required=False)
@click.pass_context
def update_simulation_archive(ctx, archive_id: str = None):
    """Update archive metadata programmatically.
    
    If no archive-id is provided, launches interactive selection.
    """
    output_json = ctx.obj.get('output_json', False) if ctx.obj else False
    try:
        console.print("Update functionality for simulation archives not yet implemented.")
        console.print("This would allow programmatic updates to archive metadata.")
        
    except Exception as e:
        console.print(f"[red]Error:[/red] {str(e)}")


@archive.command(name="delete")
@click.argument("archive_id", required=False)
@click.option("--force", "-f", is_flag=True, help="Skip confirmation prompt")
@click.pass_context
def delete_simulation_archive(ctx, archive_id: str = None, force: bool = False):
    """Delete an archive.
    
    If no archive-id is provided, launches interactive selection.
    """
    output_json = ctx.obj.get('output_json', False) if ctx.obj else False
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


@archive.command(name="add")
@click.argument("simulation_id", required=True)
@click.option("--location", required=True, help="Location where archive files exist")
@click.option("--pattern", help="File pattern for split archives (e.g., 'archive.tar.gz_*')")
@click.option("--split-parts", type=int, help="Number of split parts (for split archives)")
@click.option("--type", "archive_type", type=click.Choice(["single", "split-tar"]), default="single", help="Archive type")
@click.pass_context
def add_simulation_archive(ctx, simulation_id: str, location: str, pattern: str = None, split_parts: int = None, archive_type: str = "single"):
    """Add existing archive files to a simulation.
    
    Tracks existing archives (including split archives) for a simulation.
    
    Examples:
        # Single archive
        tellus simulation archive add Eem125-S2 --location hsm.dmawi.de --pattern "data.tar.gz"
        
        # Split archive with 31 parts
        tellus simulation archive add Eem125-S2 --location hsm.dmawi.de \
            --pattern "Eem125-S2.tar.gz_*" --split-parts 31 --type split-tar
    """
    output_json = ctx.obj.get('output_json', False) if ctx.obj else False
    
    try:
        # Use UnifiedFileService to create archive entry
        service = _get_unified_file_service()
        
        # Create archive file entry
        from ...domain.entities.simulation_file import FileType
        
        # Generate archive ID from pattern/simulation
        archive_id = f"{simulation_id}_{pattern.replace('*', 'archive').replace('.', '_')}" if pattern else f"{simulation_id}_archive"
        
        # Create archive file with metadata
        archive_file = service.create_file(
            relative_path=archive_id,
            file_type=FileType.ARCHIVE,
            simulation_id=simulation_id,
            location_name=location,
            archive_format="tar.gz" if archive_type in ["single", "split-tar"] else "unknown",
            attributes={
                "pattern": pattern or "*",
                "split_parts": split_parts,
                "archive_type": archive_type,
                "location": location,
                "indexed": False,  # Track if contents have been indexed
                "index_timestamp": None,
                "file_count": None,
                "content_summary": {}
            }
        )
        
        archive_info = {
            "simulation_id": simulation_id,
            "archive_id": archive_file.relative_path,
            "location": location,
            "pattern": pattern or "*",
            "split_parts": split_parts,
            "type": archive_type
        }
        
        if output_json:
            import json
            archive_info["status"] = "added"
            console.print(json.dumps(archive_info, indent=2))
        else:
            console.print(f"[green]✓[/green] Added archive for simulation: {simulation_id}")
            console.print(f"  Archive ID: {archive_file.relative_path}")
            console.print(f"  Location: {location}")
            console.print(f"  Pattern: {pattern or '*'}")
            if split_parts:
                console.print(f"  Split parts: {split_parts}")
            console.print(f"  Type: {archive_type}")
            
    except Exception as e:
        console.print(f"[red]Error:[/red] {str(e)}")


@archive.command(name="list-contents")
@click.argument("simulation_id", required=True)
@click.argument("archive_id", required=False)
@click.option("--filter", "file_filter", help="Filter files by pattern (e.g., '*.nc')")
@click.option("--grep", help="Grep for specific text in filenames")
@click.pass_context
def list_archive_contents(ctx, simulation_id: str, archive_id: str = None, file_filter: str = None, grep: str = None):
    """List contents of an archive without extraction.
    
    Uses tar -t to list archive contents. For split archives, handles
    concatenation automatically.
    
    Examples:
        # List all files in archive
        tellus simulation archive list-contents Eem125-S2
        
        # Filter for NetCDF files containing 'mpiom'
        tellus simulation archive list-contents Eem125-S2 --filter "*.nc" --grep mpiom
    """
    output_json = ctx.obj.get('output_json', False) if ctx.obj else False
    
    try:
        # Get services
        file_service = _get_unified_file_service()
        location_service = _get_location_service()
        
        # Get archives for this simulation
        archives = file_service.list_simulation_archives(simulation_id)
        if not archives:
            error_msg = f"No archives found for simulation '{simulation_id}'"
            if output_json:
                import json
                console.print(json.dumps({"error": error_msg}, indent=2))
            else:
                console.print(f"[red]Error:[/red] {error_msg}")
            return
            
        # If specific archive_id provided, find it
        target_archive = None
        if archive_id:
            target_archive = next((a for a in archives if a.relative_path == archive_id), None)
            if not target_archive:
                error_msg = f"Archive '{archive_id}' not found for simulation '{simulation_id}'"
                if output_json:
                    import json
                    console.print(json.dumps({"error": error_msg}, indent=2))
                else:
                    console.print(f"[red]Error:[/red] {error_msg}")
                return
            archives = [target_archive]
        
        all_contents = []
        
        for archive in archives:
            archive_name = archive.relative_path
            pattern = archive.attributes.get('pattern', '*')
            split_parts = archive.attributes.get('split_parts')
            archive_type = archive.attributes.get('archive_type', 'single')
            location_name = archive.attributes.get('location')
            
            if not output_json:
                console.print(f"[cyan]Listing contents of archive: {archive_name}[/cyan]")
            
            try:
                # Get location and create filesystem adapter
                source_location_entity = location_service.get_location_filesystem(location_name)
                from ...infrastructure.adapters.fsspec_adapter import FSSpecAdapter
                source_location = FSSpecAdapter(source_location_entity)
                
                # List archive contents
                contents = _list_archive_files(
                    source_location, pattern, split_parts, archive_type,
                    file_filter, grep
                )
                
                archive_contents = {
                    "archive_id": archive_name,
                    "location": location_name,
                    "type": archive_type,
                    "split_parts": split_parts,
                    "files": contents
                }
                all_contents.append(archive_contents)
                
                if not output_json:
                    _display_archive_contents(archive_contents, file_filter, grep)
                    
            except Exception as e:
                error_msg = f"Failed to list contents of archive {archive_name}: {str(e)}"
                if output_json:
                    all_contents.append({
                        "archive_id": archive_name,
                        "error": error_msg
                    })
                else:
                    console.print(f"[red]Error:[/red] {error_msg}")
                continue
        
        if output_json:
            import json
            console.print(json.dumps({"simulation_id": simulation_id, "archives": all_contents}, indent=2))
            
    except Exception as e:
        console.print(f"[red]Error:[/red] {str(e)}")


def _list_archive_files(source_location, pattern: str, split_parts: int, archive_type: str, 
                       file_filter: str = None, grep: str = None) -> list:
    """List files in an archive."""
    import tempfile
    import subprocess
    import os
    import fnmatch
    
    try:
        source_fs = source_location.fs
        source_base_path = source_location.location.get_base_path() or ""
        
        if archive_type == 'split-tar' and split_parts:
            # Handle split archive by reconstructing temporarily and listing
            with tempfile.TemporaryDirectory() as temp_dir:
                # Download and concatenate parts
                temp_archive = os.path.join(temp_dir, "temp_archive.tar.gz")
                
                with open(temp_archive, 'wb') as output:
                    for part_num in range(split_parts):
                        part_pattern = pattern.replace('*', f'{part_num:04d}')
                        source_part_path = os.path.join(source_base_path, part_pattern) if source_base_path else part_pattern
                        
                        # Find the actual part file
                        matching_parts = list(source_fs.glob(source_part_path))
                        if not matching_parts:
                            continue
                        
                        source_part = matching_parts[0]
                        
                        # Stream part content to concatenated archive
                        with source_fs.open(source_part, 'rb') as src:
                            while True:
                                chunk = src.read(8192)
                                if not chunk:
                                    break
                                output.write(chunk)
                
                # List contents of concatenated archive
                result = subprocess.run(['tar', '-tzf', temp_archive], 
                                      capture_output=True, text=True, check=True)
                files = result.stdout.strip().split('\n')
                
        else:
            # Handle single archive
            source_pattern = os.path.join(source_base_path, pattern) if source_base_path else pattern
            matching_files = list(source_fs.glob(source_pattern))
            
            if not matching_files:
                return []
            
            # For single archive, download temporarily and list
            source_file = matching_files[0]
            
            with tempfile.TemporaryDirectory() as temp_dir:
                temp_archive = os.path.join(temp_dir, "temp_archive")
                
                # Download archive
                with source_fs.open(source_file, 'rb') as src:
                    with open(temp_archive, 'wb') as dst:
                        while True:
                            chunk = src.read(8192)
                            if not chunk:
                                break
                            dst.write(chunk)
                
                # Determine archive format and list contents
                if source_file.endswith(('.tar.gz', '.tgz')):
                    cmd = ['tar', '-tzf', temp_archive]
                elif source_file.endswith('.tar.bz2'):
                    cmd = ['tar', '-tjf', temp_archive]
                elif source_file.endswith('.tar'):
                    cmd = ['tar', '-tf', temp_archive]
                elif source_file.endswith('.zip'):
                    cmd = ['unzip', '-l', temp_archive]
                else:
                    # Try tar as default
                    cmd = ['tar', '-tzf', temp_archive]
                
                result = subprocess.run(cmd, capture_output=True, text=True, check=True)
                
                if source_file.endswith('.zip'):
                    # Parse zip output format
                    lines = result.stdout.strip().split('\n')[3:-2]  # Skip header/footer
                    files = [line.split()[-1] for line in lines if line.strip()]
                else:
                    files = result.stdout.strip().split('\n')
        
        # Filter files if needed
        if files and files != ['']:
            if file_filter:
                files = [f for f in files if fnmatch.fnmatch(f, file_filter)]
            
            if grep:
                files = [f for f in files if grep.lower() in f.lower()]
        else:
            files = []
            
        return files
        
    except subprocess.CalledProcessError as e:
        raise Exception(f"Failed to list archive contents: {e.stderr}")
    except Exception as e:
        raise Exception(f"Archive listing error: {str(e)}")


def _display_archive_contents(archive_contents: dict, file_filter: str = None, grep: str = None):
    """Display archive contents in a formatted way."""
    files = archive_contents['files']
    
    if not files:
        console.print("  [dim]No files found[/dim]")
        return
    
    # Show summary
    total_files = len(files)
    console.print(f"  [dim]Found {total_files} file(s)[/dim]")
    
    if file_filter:
        console.print(f"  [dim]Filtered by: {file_filter}[/dim]")
    if grep:
        console.print(f"  [dim]Grep: {grep}[/dim]")
    
    console.print()
    
    # Display files in columns if many files
    if total_files > 20:
        console.print("  [dim]Showing first 20 files (use --json for complete list):[/dim]")
        files = files[:20]
    
    for file_path in files:
        console.print(f"    {file_path}")
    
    if total_files > 20:
        console.print(f"    [dim]... and {total_files - 20} more files[/dim]")
    
    console.print()


@archive.command(name="index")
@click.argument("simulation_id", required=True)
@click.argument("archive_id", required=False)
@click.option("--force", is_flag=True, help="Force re-indexing even if already indexed")
@click.pass_context
def index_archive_contents(ctx, simulation_id: str, archive_id: str = None, force: bool = False):
    """Create content index for archives.
    
    Analyzes archive contents and stores metadata for fast querying without
    needing to download/extract archives.
    
    Examples:
        # Index all archives for a simulation
        tellus simulation archive index Eem125-S2
        
        # Index specific archive
        tellus simulation archive index Eem125-S2 specific_archive_id
        
        # Force re-indexing
        tellus simulation archive index Eem125-S2 --force
    """
    output_json = ctx.obj.get('output_json', False) if ctx.obj else False
    
    try:
        # Get services
        file_service = _get_unified_file_service()
        location_service = _get_location_service()
        
        # Get archives for this simulation
        archives = file_service.list_simulation_archives(simulation_id)
        if not archives:
            error_msg = f"No archives found for simulation '{simulation_id}'"
            if output_json:
                import json
                console.print(json.dumps({"error": error_msg}, indent=2))
            else:
                console.print(f"[red]Error:[/red] {error_msg}")
            return
            
        # Filter to specific archive if requested
        if archive_id:
            target_archive = next((a for a in archives if a.relative_path == archive_id), None)
            if not target_archive:
                error_msg = f"Archive '{archive_id}' not found for simulation '{simulation_id}'"
                if output_json:
                    import json
                    console.print(json.dumps({"error": error_msg}, indent=2))
                else:
                    console.print(f"[red]Error:[/red] {error_msg}")
                return
            archives = [target_archive]
        
        indexed_archives = []
        skipped_archives = []
        
        if not output_json:
            console.print(f"[cyan]Indexing {len(archives)} archive(s) for simulation: {simulation_id}[/cyan]")
        
        from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, DownloadColumn, TransferSpeedColumn, TimeRemainingColumn
        
        # Create progress with transfer rate and size columns
        progress_columns = [
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            DownloadColumn(),
            TransferSpeedColumn(),
            TimeRemainingColumn(),
        ]
        
        with Progress(*progress_columns, console=console, disable=output_json, refresh_per_second=4) as progress:
            main_task = progress.add_task("Indexing archives...", total=len(archives))
            
            for archive in archives:
                archive_name = archive.relative_path
                
                # Check if already indexed and not forcing
                if not force and archive.attributes.get('indexed', False):
                    skipped_archives.append(archive_name)
                    if not output_json:
                        console.print(f"  [dim]Skipping {archive_name} (already indexed, use --force to re-index)[/dim]")
                    progress.advance(main_task)
                    continue
                
                progress.update(main_task, description=f"Indexing {archive_name}")
                
                try:
                    # Get location and create filesystem adapter
                    location_name = archive.attributes.get('location')
                    source_location_entity = location_service.get_location_filesystem(location_name)
                    from ...infrastructure.adapters.fsspec_adapter import FSSpecAdapter
                    source_location = FSSpecAdapter(source_location_entity)
                    
                    # Create index
                    index_data = _create_archive_index(
                        source_location, 
                        archive.attributes.get('pattern', '*'),
                        archive.attributes.get('split_parts'),
                        archive.attributes.get('archive_type', 'single')
                    )
                    
                    # Update archive metadata with index
                    archive.attributes.update({
                        "indexed": True,
                        "index_timestamp": _get_current_timestamp(),
                        "file_count": len(index_data['files']),
                        "content_summary": index_data['summary']
                    })
                    
                    # Save updated archive
                    file_service.create_file_from_entity(archive)
                    
                    indexed_archives.append({
                        "archive_id": archive_name,
                        "file_count": len(index_data['files']),
                        "content_summary": index_data['summary']
                    })
                    
                    if not output_json:
                        console.print(f"  [green]✓[/green] Indexed {archive_name}: {len(index_data['files'])} files")
                        for file_type, count in index_data['summary'].items():
                            console.print(f"    {file_type}: {count}")
                    
                except Exception as e:
                    if not output_json:
                        console.print(f"  [red]✗[/red] Failed to index {archive_name}: {str(e)}")
                    continue
                
                progress.advance(main_task)
        
        # Prepare results
        index_info = {
            "simulation_id": simulation_id,
            "indexed_archives": indexed_archives,
            "skipped_archives": skipped_archives,
            "status": "completed" if indexed_archives else "no_new_indexes"
        }
        
        if output_json:
            import json
            console.print(json.dumps(index_info, indent=2))
        else:
            console.print(f"\n[green]✓[/green] Indexing complete:")
            console.print(f"  Indexed: {len(indexed_archives)} archives")
            console.print(f"  Skipped: {len(skipped_archives)} archives")
            
    except Exception as e:
        console.print(f"[red]Error:[/red] {str(e)}")


def _create_archive_index(source_location, pattern: str, split_parts: int, archive_type: str) -> dict:
    """Create content index for an archive."""
    import tempfile
    import subprocess
    import os
    from collections import defaultdict
    
    try:
        source_fs = source_location.fs
        source_base_path = source_location.location.get_base_path() or ""
        
        # Get archive contents (reuse from list-contents functionality)
        files = _list_archive_files(source_location, pattern, split_parts, archive_type)
        
        # Analyze file types and create summary
        content_summary = defaultdict(int)
        file_details = []
        
        for file_path in files:
            # Categorize by file extension and path patterns
            if file_path.endswith(('.grb', '.grib', '.grib2')):
                content_summary['grib_files'] += 1
            elif file_path.endswith(('.nc', '.netcdf')):
                content_summary['netcdf_files'] += 1
            elif file_path.endswith('.txt'):
                content_summary['text_files'] += 1
            elif '/outdata/' in file_path:
                content_summary['outdata_files'] += 1
            elif '/analysis/' in file_path:
                content_summary['analysis_files'] += 1
            elif file_path.endswith('/'):
                content_summary['directories'] += 1
            else:
                content_summary['other_files'] += 1
            
            # Store file details (could be expanded with size, timestamp, etc.)
            file_details.append({
                "path": file_path,
                "type": _classify_file_type(file_path)
            })
        
        return {
            "files": file_details,
            "summary": dict(content_summary),
            "total_files": len(files)
        }
        
    except Exception as e:
        raise Exception(f"Index creation error: {str(e)}")


def _classify_file_type(file_path: str) -> str:
    """Classify a file by its path and extension."""
    if file_path.endswith(('.grb', '.grib', '.grib2')):
        return 'grib'
    elif file_path.endswith(('.nc', '.netcdf')):
        return 'netcdf'
    elif file_path.endswith('.txt'):
        return 'text'
    elif '/outdata/' in file_path:
        return 'outdata'
    elif '/analysis/' in file_path:
        return 'analysis'
    elif file_path.endswith('/'):
        return 'directory'
    else:
        return 'other'


def _get_current_timestamp() -> str:
    """Get current timestamp in ISO format."""
    import datetime
    return datetime.datetime.now().isoformat()


@archive.command(name="stage")
@click.argument("simulation_id", required=True)
@click.option("--from-location", required=True, help="Source location")
@click.option("--to-location", required=True, help="Destination location for staging")
@click.option("--reconstruct", is_flag=True, help="Reconstruct split archives")
@click.option("--optimize-route", is_flag=True, help="Use network topology to optimize transfer routing")
@click.option("--via", help="Force transfer through specific intermediate location")
@click.option("--show-route", is_flag=True, help="Display planned transfer route with performance estimates")
@click.option("--optimize-for", 
              type=click.Choice(['bandwidth', 'latency', 'cost', 'reliability']),
              default='bandwidth',
              help="Optimization criteria for route selection")
@click.pass_context
def stage_simulation_archive(ctx, simulation_id: str, from_location: str, to_location: str, reconstruct: bool = False, 
                           optimize_route: bool = False, via: str = None, show_route: bool = False, 
                           optimize_for: str = 'bandwidth'):
    """Stage archives from remote to local location.
    
    Downloads archives from remote locations and optionally reconstructs
    split archives into single files. Supports network topology optimization
    for efficient routing through intermediate locations.
    
    Examples:
        # Stage split archive and reconstruct
        tellus simulation archive stage Eem125-S2 \
            --from-location hsm.dmawi.de \
            --to-location local-scratch \
            --reconstruct
            
        # Stage with network topology optimization
        tellus simulation archive stage Eem125-S2 \
            --from-location hsm.dmawi.de \
            --to-location isibhv \
            --optimize-route \
            --show-route
            
        # Force routing through specific intermediate location
        tellus simulation archive stage Eem125-S2 \
            --from-location hsm.dmawi.de \
            --to-location isibhv \
            --via albedo0.dmawi.de \
            --show-route
            
        # Optimize for latency instead of bandwidth
        tellus simulation archive stage Eem125-S2 \
            --from-location hsm.dmawi.de \
            --to-location isibhv \
            --optimize-route \
            --optimize-for latency
    """
    import asyncio
    return asyncio.run(_stage_simulation_archive_async(
        ctx, simulation_id, from_location, to_location, reconstruct,
        optimize_route, via, show_route, optimize_for
    ))


async def _stage_simulation_archive_async(ctx, simulation_id: str, from_location: str, to_location: str, reconstruct: bool = False, 
                                        optimize_route: bool = False, via: str = None, show_route: bool = False, 
                                        optimize_for: str = 'bandwidth'):
    """Async implementation of stage_simulation_archive."""
    output_json = ctx.obj.get('output_json', False) if ctx.obj else False
    
    try:
        # Get services
        file_service = _get_unified_file_service()
        location_service = _get_location_service()
        
        # Get archives for this simulation
        archives = file_service.list_simulation_archives(simulation_id)
        if not archives:
            error_msg = f"No archives found for simulation '{simulation_id}'"
            if output_json:
                import json
                console.print(json.dumps({"error": error_msg}, indent=2))
            else:
                console.print(f"[red]Error:[/red] {error_msg}")
            return
            
        # Get location objects
        try:
            source_location_entity = location_service.get_location_filesystem(from_location)
            dest_location_entity = location_service.get_location_filesystem(to_location)
            
            # Create filesystem adapters
            from ...infrastructure.adapters.fsspec_adapter import FSSpecAdapter
            source_location = FSSpecAdapter(source_location_entity)
            dest_location = FSSpecAdapter(dest_location_entity)
            
        except Exception as e:
            error_msg = f"Location not found: {str(e)}"
            if output_json:
                import json
                console.print(json.dumps({"error": error_msg}, indent=2))
            else:
                console.print(f"[red]Error:[/red] {error_msg}")
            return
        
        staged_files = []
        total_archives = len(archives)
        
        # Network optimization setup
        network_route_info = None
        if optimize_route or via or show_route:
            network_route_info = await _get_network_route_info(
                from_location, to_location, via, optimize_for, show_route, output_json
            )
            if show_route:
                _display_route_information(network_route_info, output_json)
                if not optimize_route:
                    # User only wanted to see the route, not use it
                    return
        
        if not output_json:
            console.print(f"[cyan]Staging {total_archives} archive(s) for simulation: {simulation_id}[/cyan]")
            console.print(f"  From: {from_location}")
            console.print(f"  To: {to_location}")
            if reconstruct:
                console.print(f"  [yellow]Will reconstruct split archives[/yellow]")
            if optimize_route:
                route_desc = network_route_info.get('route_description', 'optimized route') if network_route_info else 'optimized route'
                console.print(f"  [green]Using {route_desc}[/green]")
        
        from rich.progress import Progress, TaskID, SpinnerColumn, TextColumn, BarColumn, DownloadColumn, TransferSpeedColumn, TimeRemainingColumn
        from pathlib import Path
        import tempfile
        import os
        
        # Create progress with transfer rate and size columns
        progress_columns = [
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            DownloadColumn(),
            TransferSpeedColumn(),
            TimeRemainingColumn(),
        ]
        
        with Progress(*progress_columns, console=console, disable=output_json, refresh_per_second=4) as progress:
            main_task = progress.add_task(f"Staging archives...", total=total_archives)
            
            for archive in archives:
                archive_name = archive.relative_path
                pattern = archive.attributes.get('pattern', '*')
                split_parts = archive.attributes.get('split_parts')
                archive_type = archive.attributes.get('archive_type', 'single')
                
                progress.update(main_task, description=f"Staging {archive_name}")
                
                try:
                    if archive_type == 'split-tar' and split_parts:
                        # Handle split archive
                        staged_file = await _stage_split_archive(
                            source_location, dest_location, pattern, split_parts, 
                            reconstruct, progress, network_route_info
                        )
                    else:
                        # Handle single archive
                        staged_file = await _stage_single_archive(
                            source_location, dest_location, pattern, progress, network_route_info
                        )
                    
                    if staged_file:
                        staged_files.append(staged_file)
                        
                except Exception as e:
                    if not output_json:
                        console.print(f"[red]Failed to stage {archive_name}:[/red] {str(e)}")
                    continue
                    
                progress.advance(main_task)
        
        # Prepare results
        staging_info = {
            "simulation_id": simulation_id,
            "from_location": from_location,
            "to_location": to_location,
            "reconstruct": reconstruct,
            "staged_files": staged_files,
            "status": "completed" if staged_files else "failed"
        }
        
        if output_json:
            import json
            console.print(json.dumps(staging_info, indent=2))
        else:
            if staged_files:
                console.print(f"[green]✓[/green] Successfully staged {len(staged_files)} file(s)")
                for file_path in staged_files:
                    console.print(f"  → {file_path}")
            else:
                console.print(f"[red]✗[/red] No files were staged")
            
    except Exception as e:
        console.print(f"[red]Error:[/red] {str(e)}")


async def _stage_single_archive(source_location, dest_location, pattern: str, progress, network_route_info=None) -> str:
    """Stage a single archive file."""
    from pathlib import Path
    import os
    
    try:
        # Get filesystem objects from locations
        source_fs = source_location.fs
        dest_fs = dest_location.fs
        
        # Find files matching the pattern on source location
        source_base_path = source_location.location.get_base_path() or ""
        source_pattern = os.path.join(source_base_path, pattern) if source_base_path else pattern
        
        # Find matching files
        matching_files = list(source_fs.glob(source_pattern))
        
        if not matching_files:
            console.print(f"[yellow]Warning:[/yellow] No files found matching pattern: {pattern}")
            return None
        
        # For single archive, take the first match
        source_file = matching_files[0]
        dest_base_path = dest_location.location.get_base_path() or ""
        dest_file = os.path.join(dest_base_path, os.path.basename(source_file)) if dest_base_path else os.path.basename(source_file)
        
        # Get file size for progress
        file_size = source_fs.size(source_file)
        
        # Check if network optimization should be used
        if network_route_info and network_route_info.get('optimization_used') and len(network_route_info.get('intermediate_hops', [])) > 0:
            # Use network-aware transfer for multi-hop routes
            transfer_time_est = _estimate_transfer_time(file_size, network_route_info)
            if transfer_time_est.get('estimated_time_seconds'):
                est_min = transfer_time_est['estimated_time_seconds'] / 60
                console.print(f"[dim]Using optimized route, estimated transfer time: {est_min:.1f} minutes[/dim]")
            
            # Note: Multi-hop transfers would require the network-aware transfer service
            # For now, we'll use direct transfer but with network performance estimates
            console.print(f"[yellow]Note:[/yellow] Multi-hop optimization not yet fully implemented, using direct transfer")
        
        # Create progress tracker
        from ...infrastructure.adapters.fsspec_adapter import ProgressTracker, FSSpecProgressCallback
        tracker = ProgressTracker("download", total_size=file_size, total_files=1)
        callback = FSSpecProgressCallback(tracker)
        
        # Transfer the file
        route_desc = ""
        if network_route_info and network_route_info.get('route_description'):
            route_desc = f" via {network_route_info['route_description']}"
        
        console.print(f"[dim]Copying {source_file} to {dest_file} ({file_size} bytes){route_desc}[/dim]")
        
        # Create progress task for this file
        file_task = progress.add_task(f"Transferring {os.path.basename(source_file)}", total=file_size)
        bytes_transferred = 0
        
        # Use get_file if available, otherwise try copy
        if hasattr(source_fs, 'get_file'):
            # Create local temp file first, then upload to destination
            import tempfile
            with tempfile.NamedTemporaryFile() as tmp_file:
                # Download with progress tracking
                with source_fs.open(source_file, 'rb') as src:
                    while True:
                        chunk = src.read(8192)
                        if not chunk:
                            break
                        tmp_file.write(chunk)
                        bytes_transferred += len(chunk)
                        progress.update(file_task, completed=bytes_transferred)
                
                tmp_file.flush()
                # Upload to destination (could add progress here too if needed)
                dest_fs.put_file(tmp_file.name, dest_file)
        else:
            # Direct copy between filesystems with progress
            with source_fs.open(source_file, 'rb') as src:
                with dest_fs.open(dest_file, 'wb') as dst:
                    while True:
                        chunk = src.read(8192)  # 8KB chunks
                        if not chunk:
                            break
                        dst.write(chunk)
                        bytes_transferred += len(chunk)
                        progress.update(file_task, completed=bytes_transferred)
        
        progress.remove_task(file_task)
        
        return dest_file
        
    except Exception as e:
        console.print(f"[red]Error staging single archive:[/red] {str(e)}")
        return None


async def _stage_split_archive(source_location, dest_location, pattern: str, split_parts: int, reconstruct: bool, progress, network_route_info=None) -> str:
    """Stage and optionally reconstruct a split archive."""
    from pathlib import Path
    import tempfile
    import os
    import subprocess
    
    try:
        # Get filesystem objects
        source_fs = source_location.fs  
        dest_fs = dest_location.fs
        
        # Get base paths
        source_base_path = source_location.location.get_base_path() or ""
        dest_base_path = dest_location.location.get_base_path() or ""
        
        from ...infrastructure.adapters.fsspec_adapter import ProgressTracker, FSSpecProgressCallback
        
        if reconstruct:
            # Stage all parts and reconstruct into single archive
            base_name = pattern.replace('_*', '').replace('*', 'archive')
            base_name = base_name.rstrip('._')
            if not base_name.endswith(('.tar.gz', '.tgz', '.tar')):
                base_name += '.tar.gz'
            
            dest_file = os.path.join(dest_base_path, base_name) if dest_base_path else base_name
            
            # Display network optimization information for split archives
            if network_route_info and network_route_info.get('optimization_used'):
                route_desc = network_route_info.get('route_description', 'optimized route')
                console.print(f"[dim]Using {route_desc} for split archive reconstruction[/dim]")
                if len(network_route_info.get('intermediate_hops', [])) > 0:
                    console.print(f"[yellow]Note:[/yellow] Multi-hop optimization for split archives not fully implemented")
            
            # Calculate total size for all parts
            total_archive_size = 0
            part_sizes = []
            for part_num in range(split_parts):
                part_pattern = pattern.replace('*', f'{part_num:04d}')
                source_part_path = os.path.join(source_base_path, part_pattern) if source_base_path else part_pattern
                matching_parts = list(source_fs.glob(source_part_path))
                if matching_parts:
                    part_size = source_fs.size(matching_parts[0])
                    part_sizes.append(part_size)
                    total_archive_size += part_size
                else:
                    part_sizes.append(0)
            
            # Create subtask for reconstruction with total bytes
            subtask = progress.add_task(f"Reconstructing {base_name}...", total=total_archive_size)
            
            # Download all parts to temporary directory and reconstruct
            with tempfile.TemporaryDirectory() as temp_dir:
                temp_parts = []
                total_size = 0
                
                # Step 1: Download all parts in parallel
                from concurrent.futures import ThreadPoolExecutor, as_completed
                import threading
                
                # Thread lock for progress updates
                progress_lock = threading.Lock()
                
                # Pre-create progress tasks for all parts (but only show active ones)
                part_tasks = {}
                
                def download_part(part_info):
                    """Download a single part with progress tracking."""
                    part_num, part_pattern, source_part_path, part_size = part_info
                    
                    if part_size == 0:
                        return None, 0, part_num
                    
                    # Find the actual part file
                    matching_parts = list(source_fs.glob(source_part_path))
                    if not matching_parts:
                        with progress_lock:
                            console.print(f"[yellow]Warning:[/yellow] Part {part_num + 1} not found: {part_pattern}")
                        return None, 0, part_num
                    
                    source_part = matching_parts[0]
                    temp_part_path = os.path.join(temp_dir, f"part_{part_num:04d}")
                    
                    # Create task for this download
                    with progress_lock:
                        part_task = progress.add_task(
                            f"Part {part_num + 1}/{split_parts}", 
                            total=part_size
                        )
                        part_tasks[part_num] = part_task
                    
                    bytes_transferred = 0
                    
                    try:
                        with source_fs.open(source_part, 'rb') as src:
                            with open(temp_part_path, 'wb') as dst:
                                while True:
                                    chunk = src.read(8192)
                                    if not chunk:
                                        break
                                    dst.write(chunk)
                                    bytes_transferred += len(chunk)
                                    # Update progress
                                    with progress_lock:
                                        progress.update(part_task, completed=bytes_transferred)
                        
                        # Mark as complete
                        with progress_lock:
                            progress.update(part_task, completed=part_size)
                        return temp_part_path, part_size, part_num
                    except Exception as e:
                        with progress_lock:
                            console.print(f"[red]Error downloading part {part_num + 1}:[/red] {str(e)}")
                        return None, 0, part_num
                
                # Prepare download tasks
                download_tasks = []
                subtask_bytes_completed = 0
                
                for part_num in range(split_parts):
                    part_pattern = pattern.replace('*', f'{part_num:04d}')
                    source_part_path = os.path.join(source_base_path, part_pattern) if source_base_path else part_pattern
                    part_size = part_sizes[part_num]
                    
                    if part_size > 0:
                        download_tasks.append((part_num, part_pattern, source_part_path, part_size))
                        total_size += part_size
                
                # Execute downloads in parallel (limit concurrent downloads to avoid overwhelming the server)
                max_workers = min(8, len(download_tasks))  # Max 8 concurrent downloads
                completed_parts = {}
                
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    # Submit all download tasks
                    future_to_part = {executor.submit(download_part, task): task[0] for task in download_tasks}
                    
                    # Process completed downloads
                    for future in as_completed(future_to_part):
                        part_num = future_to_part[future]
                        try:
                            temp_part_path, part_size_actual, part_num_result = future.result()
                            if temp_part_path:
                                completed_parts[part_num_result] = temp_part_path
                                subtask_bytes_completed += part_size_actual
                                progress.update(subtask, completed=subtask_bytes_completed)
                                console.print(f"[dim]  Downloaded part {part_num_result + 1}/{split_parts} ({part_size_actual} bytes)[/dim]")
                        except Exception as e:
                            console.print(f"[red]Error downloading part {part_num + 1}:[/red] {str(e)}")
                
                # Sort parts by number and create temp_parts list
                temp_parts = []
                for part_num in sorted(completed_parts.keys()):
                    temp_parts.append(completed_parts[part_num])
                
                # Step 2: Reconstruct by concatenating parts
                console.print(f"[dim]  Concatenating {len(temp_parts)} parts into {dest_file}[/dim]")
                reconstructed_path = os.path.join(temp_dir, "reconstructed.tar.gz")
                
                with open(reconstructed_path, 'wb') as output:
                    for part_path in temp_parts:
                        with open(part_path, 'rb') as part:
                            while True:
                                chunk = part.read(8192)
                                if not chunk:
                                    break
                                output.write(chunk)
                
                # Step 3: Upload reconstructed file to destination
                upload_task = progress.add_task(f"Uploading reconstructed archive", total=total_size)
                bytes_uploaded = 0
                
                with open(reconstructed_path, 'rb') as src:
                    with dest_fs.open(dest_file, 'wb') as dst:
                        while True:
                            chunk = src.read(8192)
                            if not chunk:
                                break
                            dst.write(chunk)
                            bytes_uploaded += len(chunk)
                            progress.update(upload_task, completed=bytes_uploaded)
                
                progress.remove_task(upload_task)
                progress.remove_task(subtask)
                
                console.print(f"[dim]Reconstructed archive: {dest_file} ({total_size} bytes total)[/dim]")
                return dest_file
            
        else:
            # Stage all parts separately without reconstruction
            parts_dir = f"{pattern}_parts"
            dest_parts_path = os.path.join(dest_base_path, parts_dir) if dest_base_path else parts_dir
            
            # Ensure parts directory exists
            try:
                dest_fs.makedirs(dest_parts_path, exist_ok=True)
            except:
                pass  # Some filesystems don't support makedirs
            
            # Calculate total size for progress tracking
            total_parts_size = 0
            parts_info = []
            for part_num in range(split_parts):
                part_pattern = pattern.replace('*', f'{part_num:04d}')
                source_part_path = os.path.join(source_base_path, part_pattern) if source_base_path else part_pattern
                matching_parts = list(source_fs.glob(source_part_path))
                if matching_parts:
                    part_size = source_fs.size(matching_parts[0])
                    parts_info.append((matching_parts[0], part_size))
                    total_parts_size += part_size
                else:
                    parts_info.append((None, 0))
            
            subtask = progress.add_task(f"Downloading parts...", total=total_parts_size)
            downloaded_parts = []
            subtask_bytes_completed = 0
            
            # Thread-safe parallel download for parts staging
            from concurrent.futures import ThreadPoolExecutor, as_completed
            import threading
            
            # Thread lock for progress updates
            progress_lock = threading.Lock()
            
            def download_part_to_dest(part_info):
                """Download a part directly to destination with progress tracking."""
                part_num, source_part, part_size = part_info
                
                if source_part is None or part_size == 0:
                    return None, 0, part_num
                
                dest_part = os.path.join(dest_parts_path, os.path.basename(source_part))
                
                # Create task for this download
                with progress_lock:
                    part_task = progress.add_task(
                        f"Part {part_num + 1}/{split_parts}",
                        total=part_size
                    )
                
                bytes_transferred = 0
                
                try:
                    with source_fs.open(source_part, 'rb') as src:
                        with dest_fs.open(dest_part, 'wb') as dst:
                            while True:
                                chunk = src.read(8192)
                                if not chunk:
                                    break
                                dst.write(chunk)
                                bytes_transferred += len(chunk)
                                # Update progress
                                with progress_lock:
                                    progress.update(part_task, completed=bytes_transferred)
                    
                    # Mark as complete
                    with progress_lock:
                        progress.update(part_task, completed=part_size)
                    return dest_part, part_size, part_num
                except Exception as e:
                    with progress_lock:
                        console.print(f"[red]Error downloading part {part_num + 1}:[/red] {str(e)}")
                    return None, 0, part_num
            
            # Prepare download tasks for parts that exist
            parts_tasks = []
            for part_num, (source_part, part_size) in enumerate(parts_info):
                if source_part is not None and part_size > 0:
                    parts_tasks.append((part_num, source_part, part_size))
                elif source_part is None:
                    part_pattern = pattern.replace('*', f'{part_num:04d}')
                    console.print(f"[yellow]Warning:[/yellow] Part {part_num + 1} not found: {part_pattern}")
            
            # Execute downloads in parallel
            max_workers = min(8, len(parts_tasks))  # Max 8 concurrent downloads
            completed_downloads = {}
            
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Submit all download tasks
                future_to_part = {executor.submit(download_part_to_dest, task): task[0] for task in parts_tasks}
                
                # Process completed downloads
                for future in as_completed(future_to_part):
                    part_num = future_to_part[future]
                    try:
                        dest_part, part_size_actual, part_num_result = future.result()
                        if dest_part:
                            completed_downloads[part_num_result] = dest_part
                            subtask_bytes_completed += part_size_actual
                            progress.update(subtask, completed=subtask_bytes_completed)
                            console.print(f"[dim]  Downloaded part {part_num_result + 1}/{split_parts}: {os.path.basename(dest_part)} ({part_size_actual} bytes)[/dim]")
                    except Exception as e:
                        console.print(f"[red]Error downloading part {part_num + 1}:[/red] {str(e)}")
            
            # Create downloaded_parts list in order
            for part_num in sorted(completed_downloads.keys()):
                downloaded_parts.append(completed_downloads[part_num])
            
            progress.remove_task(subtask)
            console.print(f"[dim]Downloaded {len(downloaded_parts)} parts to {dest_parts_path}[/dim]")
            return dest_parts_path
            
    except Exception as e:
        console.print(f"[red]Error staging split archive:[/red] {str(e)}")
        import traceback
        console.print(f"[red]Traceback:[/red] {traceback.format_exc()}")
        return None


@archive.command(name="extract")
@click.argument("simulation_id", required=True)
@click.option("--location", required=True, help="Location where archive exists")
@click.option("--variables", help="Variables to extract (for NetCDF)")
@click.option("--output-format", type=click.Choice(["netcdf", "zarr", "csv"]), default="netcdf")
@click.option("--output-path", help="Output path for extracted data")
@click.pass_context
def extract_from_archive(ctx, simulation_id: str, location: str, variables: str = None, output_format: str = "netcdf", output_path: str = None):
    """Extract specific data from archives.
    
    Extracts files or variables from archives, with special support for
    scientific data formats like NetCDF.
    
    Examples:
        # Extract specific MPIOM ocean variables (temperature/salinity)
        tellus simulation archive extract Eem125-S2 \
            --location local-scratch \
            --variables THO,SAO \
            --output-format netcdf
    """
    output_json = ctx.obj.get('output_json', False) if ctx.obj else False
    
    try:
        # Get services
        file_service = _get_unified_file_service()
        location_service = _get_location_service()
        
        # Get archives for this simulation
        archives = file_service.list_simulation_archives(simulation_id)
        if not archives:
            error_msg = f"No archives found for simulation '{simulation_id}'"
            if output_json:
                import json
                console.print(json.dumps({"error": error_msg}, indent=2))
            else:
                console.print(f"[red]Error:[/red] {error_msg}")
            return
        
        # Get location for staged files
        try:
            dest_location_entity = location_service.get_location_filesystem(location)
            from ...infrastructure.adapters.fsspec_adapter import FSSpecAdapter
            dest_location = FSSpecAdapter(dest_location_entity)
        except Exception as e:
            error_msg = f"Location '{location}' not found: {str(e)}"
            if output_json:
                import json
                console.print(json.dumps({"error": error_msg}, indent=2))
            else:
                console.print(f"[red]Error:[/red] {error_msg}")
            return
        
        # Parse variables
        var_list = variables.split(",") if variables else None
        if var_list:
            var_list = [v.strip() for v in var_list]
        
        # Set up output path
        if not output_path:
            dest_base_path = dest_location.location.get_base_path() or ""
            output_path = f"{simulation_id}_extracted"
            if dest_base_path:
                output_path = f"{dest_base_path}/{output_path}"
        
        extracted_files = []
        total_archives = len(archives)
        
        if not output_json:
            console.print(f"[cyan]Extracting from {total_archives} archive(s) for simulation: {simulation_id}[/cyan]")
            console.print(f"  Location: {location}")
            if var_list:
                console.print(f"  Variables: {', '.join(var_list)}")
            console.print(f"  Output format: {output_format}")
            console.print(f"  Output path: {output_path}")
        
        from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, DownloadColumn, TransferSpeedColumn, TimeRemainingColumn
        
        # Create progress with transfer rate and size columns  
        progress_columns = [
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            DownloadColumn(),
            TransferSpeedColumn(),
            TimeRemainingColumn(),
        ]
        
        with Progress(*progress_columns, console=console, disable=output_json) as progress:
            main_task = progress.add_task("Extracting archives...", total=total_archives)
            
            for archive in archives:
                archive_name = archive.relative_path
                pattern = archive.attributes.get('pattern', '*')
                split_parts = archive.attributes.get('split_parts')
                archive_type = archive.attributes.get('archive_type', 'single')
                source_location_name = archive.attributes.get('location')
                
                progress.update(main_task, description=f"Extracting {archive_name}")
                
                try:
                    # Get source location
                    source_location_entity = location_service.get_location_filesystem(source_location_name)
                    source_location = FSSpecAdapter(source_location_entity)
                    
                    # Extract files from this archive
                    archive_extracted_files = _extract_archive_files(
                        source_location, dest_location, pattern, split_parts, archive_type,
                        output_path, var_list, output_format, progress
                    )
                    
                    extracted_files.extend(archive_extracted_files)
                    
                except Exception as e:
                    if not output_json:
                        console.print(f"[red]Failed to extract from {archive_name}:[/red] {str(e)}")
                    continue
                
                progress.advance(main_task)
        
        # Prepare results
        extraction_info = {
            "simulation_id": simulation_id,
            "location": location,
            "variables": var_list,
            "output_format": output_format,
            "output_path": output_path,
            "extracted_files": extracted_files,
            "status": "completed" if extracted_files else "failed"
        }
        
        if output_json:
            import json
            console.print(json.dumps(extraction_info, indent=2))
        else:
            if extracted_files:
                console.print(f"[green]✓[/green] Successfully extracted {len(extracted_files)} file(s)")
                for file_path in extracted_files:
                    console.print(f"  → {file_path}")
            else:
                console.print(f"[red]✗[/red] No files were extracted")
            
    except Exception as e:
        console.print(f"[red]Error:[/red] {str(e)}")


def _extract_archive_files(source_location, dest_location, pattern: str, split_parts: int, 
                          archive_type: str, output_path: str, variables: list = None,
                          output_format: str = "netcdf", progress = None) -> list:
    """Extract files from an archive."""
    import tempfile
    import subprocess
    import os
    from pathlib import Path
    
    try:
        source_fs = source_location.fs
        dest_fs = dest_location.fs
        source_base_path = source_location.location.get_base_path() or ""
        
        extracted_files = []
        
        # Ensure output directory exists
        try:
            dest_fs.makedirs(output_path, exist_ok=True)
        except:
            pass  # Some filesystems don't support makedirs
        
        if archive_type == 'split-tar' and split_parts:
            # Handle split archive by reconstructing and extracting
            with tempfile.TemporaryDirectory() as temp_dir:
                # Download and concatenate parts
                temp_archive = os.path.join(temp_dir, "temp_archive.tar.gz")
                extraction_dir = os.path.join(temp_dir, "extracted")
                os.makedirs(extraction_dir)
                
                # Reconstruct archive
                with open(temp_archive, 'wb') as output:
                    for part_num in range(split_parts):
                        part_pattern = pattern.replace('*', f'{part_num:04d}')
                        source_part_path = os.path.join(source_base_path, part_pattern) if source_base_path else part_pattern
                        
                        matching_parts = list(source_fs.glob(source_part_path))
                        if not matching_parts:
                            continue
                        
                        source_part = matching_parts[0]
                        with source_fs.open(source_part, 'rb') as src:
                            while True:
                                chunk = src.read(8192)
                                if not chunk:
                                    break
                                output.write(chunk)
                
                # Extract archive contents
                result = subprocess.run(['tar', '-xzf', temp_archive, '-C', extraction_dir], 
                                      capture_output=True, text=True, check=True)
                
                # Process extracted files
                extracted_files = _process_extracted_files(
                    extraction_dir, dest_fs, output_path, variables, output_format
                )
                
        else:
            # Handle single archive
            source_pattern = os.path.join(source_base_path, pattern) if source_base_path else pattern
            matching_files = list(source_fs.glob(source_pattern))
            
            if not matching_files:
                return []
            
            source_file = matching_files[0]
            
            with tempfile.TemporaryDirectory() as temp_dir:
                temp_archive = os.path.join(temp_dir, "temp_archive")
                extraction_dir = os.path.join(temp_dir, "extracted")
                os.makedirs(extraction_dir)
                
                # Download archive
                with source_fs.open(source_file, 'rb') as src:
                    with open(temp_archive, 'wb') as dst:
                        while True:
                            chunk = src.read(8192)
                            if not chunk:
                                break
                            dst.write(chunk)
                
                # Extract based on archive format
                if source_file.endswith(('.tar.gz', '.tgz')):
                    cmd = ['tar', '-xzf', temp_archive, '-C', extraction_dir]
                elif source_file.endswith('.tar.bz2'):
                    cmd = ['tar', '-xjf', temp_archive, '-C', extraction_dir]
                elif source_file.endswith('.tar'):
                    cmd = ['tar', '-xf', temp_archive, '-C', extraction_dir]
                elif source_file.endswith('.zip'):
                    cmd = ['unzip', '-q', temp_archive, '-d', extraction_dir]
                else:
                    # Try tar.gz as default
                    cmd = ['tar', '-xzf', temp_archive, '-C', extraction_dir]
                
                result = subprocess.run(cmd, capture_output=True, text=True, check=True)
                
                # Process extracted files
                extracted_files = _process_extracted_files(
                    extraction_dir, dest_fs, output_path, variables, output_format
                )
        
        return extracted_files
        
    except subprocess.CalledProcessError as e:
        raise Exception(f"Failed to extract archive: {e.stderr}")
    except Exception as e:
        raise Exception(f"Extraction error: {str(e)}")


def _process_extracted_files(extraction_dir: str, dest_fs, output_path: str, 
                           variables: list = None, output_format: str = "netcdf") -> list:
    """Process and upload extracted files to destination."""
    import os
    import shutil
    from pathlib import Path
    
    extracted_files = []
    
    try:
        # Walk through extracted files
        for root, dirs, files in os.walk(extraction_dir):
            for file in files:
                local_file_path = os.path.join(root, file)
                relative_path = os.path.relpath(local_file_path, extraction_dir)
                
                # Apply variable filtering for scientific data
                if variables and file.endswith(('.grb', '.nc', '.grib', '.grib2')):
                    # For GRIB files, check if any variables are needed
                    # This is a simplified check - real implementation would inspect the file
                    include_file = any(var.lower() in file.lower() for var in variables)
                    if not include_file:
                        # Skip files that don't contain requested variables
                        continue
                
                # Determine destination path
                dest_file_path = os.path.join(output_path, relative_path)
                
                # Create destination directory structure
                dest_dir = os.path.dirname(dest_file_path)
                if dest_dir:
                    try:
                        dest_fs.makedirs(dest_dir, exist_ok=True)
                    except:
                        pass
                
                # Upload file to destination
                with open(local_file_path, 'rb') as src:
                    with dest_fs.open(dest_file_path, 'wb') as dst:
                        shutil.copyfileobj(src, dst)
                
                extracted_files.append(dest_file_path)
        
        return extracted_files
        
    except Exception as e:
        raise Exception(f"File processing error: {str(e)}")


# Network-aware transfer helper functions

async def _get_network_route_info(source_location: str, dest_location: str, via: str = None, 
                                optimize_for: str = 'bandwidth', show_route: bool = False, 
                                output_json: bool = False) -> dict:
    """Get network route information for optimal transfer routing."""
    try:
        from ...application.container import get_service_container
        container = get_service_container()
        
        # Get network topology service
        network_service = container.network_topology_service()
        
        if via:
            # Manual route specified - validate that intermediate location exists
            location_service = container.location_service()
            try:
                location_service.get_location(via)
                return {
                    'route_type': 'manual',
                    'route_description': f'manual route via {via}',
                    'intermediate_location': via,
                    'path': [source_location, via, dest_location],
                    'optimization_used': False,
                    'warning': 'Manual routing may not be optimal'
                }
            except Exception:
                return {
                    'route_type': 'error',
                    'error': f'Intermediate location "{via}" not found',
                    'fallback_to_direct': True
                }
        
        # Get optimal route from network topology service
        from ...application.services.network_topology_service import OptimalRouteRequestDto
        
        route_request = OptimalRouteRequestDto(
            source_location=source_location,
            destination_location=dest_location,
            optimize_for=optimize_for,
            avoid_bottlenecks=True
        )
        
        route_response = await network_service.find_optimal_route(route_request)
        
        if route_response and route_response.primary_path:
            path = route_response.primary_path
            return {
                'route_type': 'optimized',
                'route_description': f'{optimize_for}-optimized route',
                'path': path.full_path,
                'intermediate_hops': path.intermediate_hops,
                'estimated_bandwidth_mbps': path.estimated_bandwidth_mbps,
                'estimated_latency_ms': path.estimated_latency_ms,
                'bottleneck_location': path.bottleneck_location,
                'path_quality': route_response.path_analysis.get('path_quality', 'unknown'),
                'recommendation': route_response.recommendation,
                'alternative_count': len(route_response.alternative_paths),
                'optimization_used': True
            }
        else:
            return {
                'route_type': 'direct',
                'route_description': 'direct transfer (no topology data available)',
                'path': [source_location, dest_location],
                'optimization_used': False,
                'message': 'Network topology not available, using direct transfer'
            }
            
    except Exception as e:
        if not output_json:
            console.print(f"[yellow]Warning:[/yellow] Network optimization failed: {str(e)}")
        return {
            'route_type': 'error',
            'route_description': 'direct transfer (optimization failed)',
            'path': [source_location, dest_location],
            'optimization_used': False,
            'error': str(e),
            'fallback_to_direct': True
        }


def _display_route_information(route_info: dict, output_json: bool = False):
    """Display network route information to the user."""
    if output_json:
        import json
        console.print(json.dumps(route_info, indent=2))
        return
    
    if route_info.get('route_type') == 'error':
        console.print(f"[red]Route Error:[/red] {route_info.get('error', 'Unknown error')}")
        if route_info.get('fallback_to_direct'):
            console.print("[yellow]Falling back to direct transfer[/yellow]")
        return
    
    from rich.table import Table
    from rich.panel import Panel
    
    route_table = Table(title="Planned Transfer Route", show_header=True, header_style="bold cyan")
    route_table.add_column("Step", style="cyan", width=6)
    route_table.add_column("Location", style="green")
    route_table.add_column("Notes", style="dim")
    
    path = route_info.get('path', [])
    for i, location in enumerate(path):
        step = str(i + 1)
        notes = ""
        
        if i == 0:
            notes = "Source"
        elif i == len(path) - 1:
            notes = "Destination"
        elif location == route_info.get('bottleneck_location'):
            notes = "⚠️ Bottleneck"
        else:
            notes = "Intermediate"
            
        route_table.add_row(step, location, notes)
    
    console.print(route_table)
    
    # Performance information
    if route_info.get('optimization_used'):
        perf_info = []
        
        if route_info.get('estimated_bandwidth_mbps'):
            bandwidth = route_info['estimated_bandwidth_mbps']
            perf_info.append(f"Estimated Bandwidth: {bandwidth:.1f} Mbps")
            
        if route_info.get('estimated_latency_ms'):
            latency = route_info['estimated_latency_ms']
            perf_info.append(f"Estimated Latency: {latency:.1f} ms")
            
        path_quality = route_info.get('path_quality', 'unknown')
        perf_info.append(f"Path Quality: {path_quality.title()}")
        
        if route_info.get('alternative_count', 0) > 0:
            alt_count = route_info['alternative_count']
            perf_info.append(f"Alternative Routes Available: {alt_count}")
        
        if perf_info:
            perf_text = "\n".join(perf_info)
            console.print(Panel(perf_text, title="Performance Estimates", border_style="blue"))
    
    # Recommendations
    if route_info.get('recommendation'):
        console.print(Panel(
            route_info['recommendation'], 
            title="Network Recommendation", 
            border_style="green"
        ))
    
    # Warnings
    if route_info.get('warning'):
        console.print(f"[yellow]Warning:[/yellow] {route_info['warning']}")


def _estimate_transfer_time(file_size_bytes: int, route_info: dict) -> dict:
    """Estimate transfer time based on route information."""
    if not route_info.get('estimated_bandwidth_mbps'):
        return {'estimated_time_seconds': None, 'transfer_rate_estimate': 'unknown'}
    
    bandwidth_mbps = route_info['estimated_bandwidth_mbps']
    # Convert to bytes per second (Mbps to MBps, assuming 8 bits per byte)
    bandwidth_mbps_bytes = bandwidth_mbps / 8 * 1024 * 1024  # MBps in bytes
    
    # Add 25% overhead for protocol overhead, latency, etc.
    effective_bandwidth = bandwidth_mbps_bytes * 0.75
    
    estimated_seconds = file_size_bytes / effective_bandwidth if effective_bandwidth > 0 else None
    
    # Format transfer rate description
    if bandwidth_mbps > 100:
        rate_desc = "high-speed"
    elif bandwidth_mbps > 50:
        rate_desc = "good"
    elif bandwidth_mbps > 10:
        rate_desc = "moderate"
    else:
        rate_desc = "slow"
    
    return {
        'estimated_time_seconds': estimated_seconds,
        'transfer_rate_estimate': rate_desc,
        'effective_bandwidth_mbps': bandwidth_mbps * 0.75
    }

