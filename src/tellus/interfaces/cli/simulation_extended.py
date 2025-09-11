"""Extended simulation CLI commands."""

import rich_click as click
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from ...application.dtos import (FileRegistrationDto,
                                 SimulationLocationAssociationDto,
                                 UpdateSimulationDto)
from ...application.exceptions import EntityNotFoundError
from ...application.container import get_service_container
from .main import console
from .simulation import _get_simulation_service, simulation


def _complete_simulation_id(ctx, param, incomplete):
    """Shell completion for simulation IDs."""
    try:
        service = _get_simulation_service()
        simulations = service.list_simulations()
        sim_ids = [sim.simulation_id for sim in simulations.simulations if sim.simulation_id.startswith(incomplete)]
        return sim_ids
    except Exception:
        return []


def _complete_location_name(ctx, param, incomplete):
    """Shell completion for location names."""
    try:
        container = get_service_container()
        location_service = container.service_factory.location_service
        locations = location_service.list_locations()
        loc_names = [loc.name for loc in locations.locations if loc.name.startswith(incomplete)]
        return loc_names
    except Exception:
        return []


def _handle_simulation_not_found(sim_id: str, service):
    """Handle simulation not found error with fuzzy matching suggestions."""
    console.print(f"[red]Error:[/red] Simulation '{sim_id}' not found")
    
    # Get all simulations for fuzzy matching
    try:
        all_simulations = service.list_simulations()
        if all_simulations.simulations:
            # Use rapidfuzz for better fuzzy matching
            try:
                from rapidfuzz import fuzz
                
                def similarity_score(a, b):
                    """Calculate similarity score using rapidfuzz."""
                    # Use ratio for general similarity
                    base_score = fuzz.ratio(a.lower(), b.lower()) / 100.0
                    
                    # Bonus for prefix matches
                    if b.lower().startswith(a.lower()):
                        base_score = min(base_score + 0.3, 1.0)
                    
                    return base_score
                    
            except ImportError:
                # Fallback to difflib if rapidfuzz not available
                import difflib
                
                def similarity_score(a, b):
                    """Calculate similarity score using difflib fallback."""
                    base_score = difflib.SequenceMatcher(None, a.lower(), b.lower()).ratio()
                    
                    # Bonus for prefix matches
                    if b.lower().startswith(a.lower()):
                        base_score = min(base_score + 0.3, 1.0)
                    
                    return base_score
            
            # Find best matches
            matches = [(s.simulation_id, similarity_score(sim_id, s.simulation_id)) 
                      for s in all_simulations.simulations]
            matches.sort(key=lambda x: x[1], reverse=True)
            
            # Show suggestion if we have a good match
            best_match, score = matches[0]
            if score > 0.6:  # Threshold for "did you mean"
                console.print(f"[yellow]Did you mean:[/yellow] {best_match}")
    except Exception:
        # If fuzzy matching fails, just continue
        pass
        
    console.print("[dim]Use 'tellus simulation list' to see available simulations[/dim]")


@simulation.command(name="update")
@click.argument("expid")
@click.option("--model-id", help="Update model identifier")
@click.option("--path", help="Update simulation path")
@click.option("--description", help="Update simulation description")
@click.pass_context
def update_simulation(ctx, expid: str, model_id: str = None, path: str = None, description: str = None):
    """Update an existing simulation."""
    output_json = ctx.obj.get('output_json', False) if ctx.obj else False
    try:
        service = _get_simulation_service()
        
        dto = UpdateSimulationDto(
            model_id=model_id,
            path=path,
            description=description
        )
        
        result = service.update_simulation(expid, dto)
        
        if output_json:
            console.print(result.pretty_json() if hasattr(result, 'pretty_json') else '{"status": "updated"}')
        else:
            console.print(f"[green]✓[/green] Updated simulation: {result.simulation_id}")
        
    except Exception as e:
        console.print(f"[red]Error:[/red] {str(e)}")


@simulation.command(name="delete")
@click.argument("expid")
@click.option("--force", is_flag=True, help="Force deletion without confirmation")
@click.pass_context
def delete_simulation(ctx, expid: str, force: bool = False):
    """Delete a simulation."""
    output_json = ctx.obj.get('output_json', False) if ctx.obj else False
    try:
        if not force:
            if not click.confirm(f"Are you sure you want to delete simulation '{expid}'?"):
                console.print("[yellow]Deletion cancelled.[/yellow]")
                return
        
        service = _get_simulation_service()
        success = service.delete_simulation(expid)
        
        if output_json:
            import json
            result = {"simulation_id": expid, "status": "deleted" if success else "failed"}
            console.print(json.dumps(result, indent=2))
        else:
            if success:
                console.print(f"[green]✓[/green] Deleted simulation: {expid}")
            else:
                console.print(f"[red]Error:[/red] Could not delete simulation: {expid}")
        
    except Exception as e:
        console.print(f"[red]Error:[/red] {str(e)}")


@simulation.group(name="location")
def simulation_location():
    """Manage simulation-location associations."""
    pass


@simulation_location.command(name="add")
@click.argument("sim_id", required=False, shell_complete=_complete_simulation_id)
@click.argument("location_name", required=False, shell_complete=_complete_location_name)
@click.option("--context", help="JSON string with location context data")
def add_location(sim_id: str = None, location_name: str = None, context: str = None):
    """Associate a location with a simulation.
    
    If no arguments are provided, launches an interactive selection wizard
    with tab completion for path templates.
    
    Examples:
        tellus simulation location add                           # Interactive wizard
        tellus simulation location add MySim tmp                 # Direct association
        tellus simulation location add MySim tmp --context '{}'  # With custom context
    """
    try:
        import json
        
        service = _get_simulation_service()
        
        # Interactive mode when no arguments provided
        if not sim_id or not location_name:
            import questionary
            import sys
            
            # Check if we can use interactive mode
            if not sys.stdin.isatty():
                console.print("[red]Error:[/red] Interactive mode requires arguments when not run in a terminal")
                console.print("Usage: tellus simulation location add <sim_id> <location_name>")
                return

            # Get simulation ID if not provided
            if not sim_id:
                simulations = service.list_simulations()
                if not simulations.simulations:
                    console.print("[yellow]No simulations found[/yellow]")
                    return
                    
                sim_choices = [sim.simulation_id for sim in simulations.simulations]
                sim_id = questionary.select(
                    "Select simulation:",
                    choices=sim_choices,
                    style=questionary.Style([
                        ('question', 'bold'),
                        ('selected', 'fg:#cc5454'),
                        ('pointer', 'fg:#ff0066 bold'),
                    ])
                ).ask()
                
                if not sim_id:
                    console.print("[yellow]No simulation selected[/yellow]")
                    return
            
            # Get location name if not provided  
            if not location_name:
                from ...application.container import get_service_container
                container = get_service_container()
                location_service = container.service_factory.location_service
                locations = location_service.list_locations()
                
                if not locations.locations:
                    console.print("[yellow]No locations found[/yellow]")
                    return
                    
                loc_choices = [loc.name for loc in locations.locations]
                location_name = questionary.select(
                    "Select location:",
                    choices=loc_choices,
                    style=questionary.Style([
                        ('question', 'bold'),
                        ('selected', 'fg:#cc5454'),
                        ('pointer', 'fg:#ff0066 bold'),
                    ])
                ).ask()
                
                if not location_name:
                    console.print("[yellow]No location selected[/yellow]")
                    return
            
            # Interactive context configuration if no context provided
            if not context:
                console.print(f"\n[dim]Configuring context for location '{location_name}'[/dim]")
                
                # Get location info to show base path context
                container = get_service_container()
                location_service = container.service_factory.location_service
                location_info = location_service.get_location(location_name)
                location_base_path = location_info.path if location_info else "/"
                
                console.print(f"[dim]Location base path: {location_base_path}[/dim]")
                console.print(f"[dim]Your path prefix will be relative to the base path[/dim]")
                
                # Check if this location already has a context for this simulation
                existing_sim = service.get_simulation(sim_id)
                existing_context = {}
                if existing_sim and hasattr(existing_sim, 'location_contexts') and location_name in existing_sim.location_contexts:
                    existing_context = existing_sim.location_contexts[location_name]
                
                # Path prefix configuration with location-aware tab completion
                current_path_prefix = existing_context.get('path_prefix', '')
                
                # If no current prefix, start with the location's base path
                if not current_path_prefix:
                    current_path_prefix = location_base_path
                
                # Try to set up standard path completion (starting from base path)
                completer = None
                try:
                    from .completion import SmartPathCompleter
                    # Use get_location_filesystem to get the domain entity
                    location_entity = location_service.get_location_filesystem(location_name)
                    completer = SmartPathCompleter(location_entity, only_directories=True)
                except Exception as e:
                    # If completion setup fails, continue without it
                    console.print(f"[dim]Note: Tab completion not available ({str(e)})[/dim]")
                    pass
                
                # Show different prompts based on whether tab completion is available
                if completer:
                    prompt_text = f"Path prefix (Tab completion available, starts at {location_base_path}):"
                else:
                    prompt_text = f"Path prefix (base: {location_base_path}):"
                
                path_prefix = questionary.text(
                    prompt_text,
                    default=current_path_prefix,
                    completer=completer,
                    style=questionary.Style([
                        ('question', 'bold'),
                        ('answer', 'fg:#cc5454'),
                    ])
                ).ask()
                
                if path_prefix is None:  # User cancelled
                    console.print("[yellow]Operation cancelled[/yellow]")
                    return
                
                # Build context
                location_context = {
                    'path_prefix': path_prefix,
                    'overrides': existing_context.get('overrides', {}),
                    'metadata': existing_context.get('metadata', {})
                }
                
                # Ask if user wants to add custom metadata
                if questionary.confirm("Add custom metadata?", default=False).ask():
                    while True:
                        key = questionary.text("Metadata key (empty to finish):").ask()
                        if not key or not key.strip():
                            break
                        value = questionary.text(f"Value for '{key}':").ask()
                        if value is not None:
                            location_context['metadata'][key] = value
        else:
            # Handle provided context string
            location_context = {}
            if context:
                try:
                    location_context = json.loads(context)
                except json.JSONDecodeError:
                    console.print(f"[red]Error:[/red] Invalid JSON in context: {context}")
                    return
        
        dto = SimulationLocationAssociationDto(
            simulation_id=sim_id,
            location_names=[location_name],
            context_overrides={location_name: location_context} if location_context else {}
        )
        
        service.associate_locations(dto)
        console.print(f"[green]✓[/green] Associated location '{location_name}' with simulation '{sim_id}'")
        
    except Exception as e:
        console.print(f"[red]Error:[/red] {str(e)}")


@simulation_location.command(name="remove")
@click.argument("sim_id", shell_complete=_complete_simulation_id)
@click.argument("location_name", shell_complete=_complete_location_name)
def remove_location(sim_id: str, location_name: str):
    """Remove a location association from a simulation."""
    try:
        service = _get_simulation_service()
        
        # Get the simulation and remove the location association
        simulation = service.get_simulation(sim_id)
        if simulation is None:
            console.print(f"[red]Error:[/red] Simulation '{sim_id}' not found")
            return
            
        if location_name not in simulation.associated_locations:
            console.print(f"[yellow]Warning:[/yellow] Location '{location_name}' is not associated with simulation '{sim_id}'")
            return
        
        # Use the entity method to disassociate
        simulation_entity = service._simulation_repo.get_by_id(sim_id)
        simulation_entity.disassociate_location(location_name)
        service._simulation_repo.save(simulation_entity)
        
        console.print(f"[green]✓[/green] Removed location '{location_name}' from simulation '{sim_id}'")
        
    except Exception as e:
        console.print(f"[red]Error:[/red] {str(e)}")


@simulation_location.command(name="rm")
@click.argument("sim_id", shell_complete=_complete_simulation_id)
@click.argument("location_name", shell_complete=_complete_location_name)  
def rm_location(sim_id: str, location_name: str):
    """Remove a location association from a simulation (alias for remove)."""
    # Call the remove_location function directly
    remove_location.callback(sim_id, location_name)


@simulation_location.command(name="edit")
@click.argument("sim_id", required=False, shell_complete=_complete_simulation_id)
@click.argument("location_name", required=False, shell_complete=_complete_location_name)
@click.option("--path-prefix", help="Set path prefix template for this simulation-location")
@click.option("--context", help="JSON string with location context data")
def edit_location_association(sim_id: str = None, location_name: str = None, 
                             path_prefix: str = None, context: str = None):
    """Edit simulation-location association settings (alias for update)."""
    # Call the update function directly
    update_location_association.callback(sim_id, location_name, path_prefix, context)


@simulation_location.command(name="update")
@click.argument("sim_id", required=False)
@click.argument("location_name", required=False) 
@click.option("--path-prefix", help="Set path prefix template for this simulation-location")
@click.option("--context", help="JSON string with location context data")
def update_location_association(sim_id: str = None, location_name: str = None, 
                               path_prefix: str = None, context: str = None):
    """Update simulation-location association settings.
    
    Update location context and path prefix templates for a specific 
    simulation-location association. If no arguments provided, launches 
    interactive selection.
    
    Examples:
        tellus simulation location update MIS11.3-B tellus_hsm --path-prefix "/data/{model}/{experiment}"
        tellus simulation location update                 # Interactive mode
    """
    try:
        import json
        
        service = _get_simulation_service()
        
        # Interactive mode when arguments missing
        if not sim_id or not location_name:
            import questionary
            import sys
            
            # Check if we can run interactively
            if not sys.stdin.isatty():
                console.print("[red]Error:[/red] Interactive mode requires simulation ID and location name when not run in a terminal")
                console.print(f"[dim]Usage: tellus simulation location edit <simulation_id> <location_name>[/dim]")
                console.print(f"[dim]Example: tellus simulation location edit MIS11.3-B tellus_hsm[/dim]")
                return

            # Get simulation ID if not provided
            if not sim_id:
                simulations = service.list_simulations()
                if not simulations.simulations:
                    console.print("[yellow]No simulations found[/yellow]")
                    return
                
                sim_choices = [sim.simulation_id for sim in simulations.simulations]
                sim_id = questionary.select(
                    "Select simulation:",
                    choices=sim_choices,
                    style=questionary.Style([
                        ('question', 'bold'),
                        ('selected', 'fg:#cc5454'),
                        ('pointer', 'fg:#ff0066 bold'),
                    ])
                ).ask()
                
                if not sim_id:
                    console.print("[yellow]No simulation selected[/yellow]")
                    return
            
            # Get location name if not provided
            if not location_name:
                # Get simulation to check associated locations
                sim = service.get_simulation(sim_id)
                if sim is None:
                    console.print(f"[red]Error:[/red] Simulation '{sim_id}' not found")
                    return
                
                if not sim.associated_locations:
                    console.print(f"[yellow]No locations associated with simulation '{sim_id}'[/yellow]")
                    return
                
                location_name = questionary.select(
                    "Select location to update:",
                    choices=sorted(sim.associated_locations),
                    style=questionary.Style([
                        ('question', 'bold'),
                        ('selected', 'fg:#cc5454'),
                        ('pointer', 'fg:#ff0066 bold'),
                    ])
                ).ask()
                
                if not location_name:
                    console.print("[yellow]No location selected[/yellow]")
                    return
        
        # Get current simulation entity and verify location association  
        sim_entity = service._simulation_repo.get_by_id(sim_id)
        if sim_entity is None:
            console.print(f"[red]Error:[/red] Simulation '{sim_id}' not found")
            return
        
        if not sim_entity.is_location_associated(location_name):
            console.print(f"[red]Error:[/red] Location '{location_name}' is not associated with simulation '{sim_id}'")
            return
        
        # Show current context
        current_context = sim_entity.get_location_context(location_name)
        console.print(f"\n[cyan]Current context for {sim_id} -> {location_name}:[/cyan]")
        if current_context:
            for key, value in current_context.items():
                console.print(f"  {key}: {value}")
        else:
            console.print("  (no context set)")
        
        # Build updated context
        updated_context = current_context.copy() if current_context else {}
        
        # Handle path prefix
        if path_prefix is not None:
            updated_context['path_prefix'] = path_prefix
        
        # Handle JSON context
        if context:
            try:
                context_data = json.loads(context)
                updated_context.update(context_data)
            except json.JSONDecodeError:
                console.print(f"[red]Error:[/red] Invalid JSON in context: {context}")
                return
        
        # Interactive updates if no options provided
        if not any([path_prefix, context]):
            import questionary
            import sys
            
            # Check if we can run interactively
            if not sys.stdin.isatty():
                console.print("[yellow]No update options provided and not running in interactive terminal.[/yellow]")
                console.print("[dim]Use --path-prefix or --context options to specify updates.[/dim]")
                console.print(f"[dim]Example: tellus simulation location edit {sim_id} {location_name} --path-prefix '{{model}}/{{experiment}}'[/dim]")
                return
            
            console.print("\n[cyan]Select what you'd like to update:[/cyan]")
            
            try:
                # Ask what to update with multiple select calls (avoiding VSplit bug)
                update_options = []
                
                update_path_prefix = questionary.confirm(
                    "Update Path Prefix Template?",
                    default=False
                ).ask()
                
                if update_path_prefix:
                    update_options.append("Path Prefix Template")
                    
                update_context = questionary.confirm(
                    "Update Custom Context Data?",
                    default=False
                ).ask()
                
                if update_context:
                    update_options.append("Custom Context Data")
                
                if not update_options:
                    console.print("[yellow]No updates selected[/yellow]")
                    return
            except Exception as e:
                console.print(f"[red]Error in interactive mode:[/red] {e}")
                console.print("[dim]Try using --path-prefix or --context options instead[/dim]")
                return
            
            # Handle path prefix update
            if "Path Prefix Template" in update_options:
                current_prefix = updated_context.get('path_prefix', '')
                new_prefix = questionary.text(
                    "Path prefix template:",
                    default=current_prefix,
                    instruction="Use {model}, {experiment}, etc. for templates"
                ).ask()
                
                if new_prefix is not None:
                    updated_context['path_prefix'] = new_prefix
            
            # Handle custom context
            if "Custom Context Data" in update_options:
                context_json = questionary.text(
                    "Additional context (JSON):",
                    default="{}",
                    instruction="e.g., {\"custom_field\": \"value\"}"
                ).ask()
                
                if context_json:
                    try:
                        context_data = json.loads(context_json)
                        updated_context.update(context_data)
                    except json.JSONDecodeError:
                        console.print("[red]Error:[/red] Invalid JSON format")
                        return
        
        # Apply the updates
        if updated_context != current_context:
            sim_entity.update_location_context(location_name, updated_context)
            
            # Save the updated simulation entity
            service._simulation_repo.save(sim_entity)
            
            console.print(f"[green]✓[/green] Updated location context for '{location_name}' in simulation '{sim_id}'")
            
            # Show updated context
            console.print("\n[cyan]New context:[/cyan]")
            for key, value in updated_context.items():
                console.print(f"  {key}: {value}")
        else:
            console.print("[yellow]No changes to apply[/yellow]")
            
    except Exception as e:
        console.print(f"[red]Error:[/red] {str(e)}")


@simulation_location.command(name="list")
@click.argument("sim_id")
def list_locations(sim_id: str):
    """List all locations associated with a simulation."""
    try:
        service = _get_simulation_service()
        sim = service.get_simulation(sim_id)
        
        if sim is None:
            _handle_simulation_not_found(sim_id, service)
            return
        
        if not sim.associated_locations:
            console.print(f"No locations associated with simulation '{sim_id}'")
            return
        
        table = Table(title=f"Locations for Simulation: {sim_id}")
        table.add_column("Location", style="cyan")
        table.add_column("Context", style="green")
        
        for location in sorted(sim.associated_locations):
            context = sim.get_location_context(location) if hasattr(sim, 'get_location_context') else {}
            context_str = str(context) if context else "-"
            table.add_row(location, context_str)
        
        console.print(Panel.fit(table))
        
    except Exception as e:
        console.print(f"[red]Error:[/red] {str(e)}")


@simulation_location.command(name="ls")
@click.argument("sim_id")
@click.argument("location_name")
@click.argument("path", required=False, default=".")
@click.option("-l", "--long", is_flag=True, help="Use long listing format")
@click.option("-a", "--all", is_flag=True, help="Show hidden files")
@click.option("-h", "--human-readable", is_flag=True, help="Human readable file sizes")
@click.option("-t", "--time", is_flag=True, help="Sort by modification time")
@click.option("-S", "--size", is_flag=True, help="Sort by file size")
@click.option("-r", "--reverse", is_flag=True, help="Reverse sort order")
@click.option("-R", "--recursive", is_flag=True, help="List subdirectories recursively")
@click.option("--color", is_flag=True, default=True, help="Colorize output")
@click.option("-T", "--tape-status", is_flag=True, help="Show tape staging status (ScoutFS only)")
@click.option("--async-status", is_flag=True, help="Use async batch status checking for better performance")
def ls_location(sim_id: str, location_name: str, path: str = ".", 
                long: bool = False, all: bool = False, human_readable: bool = False,
                time: bool = False, size: bool = False, reverse: bool = False,
                recursive: bool = False, color: bool = True, tape_status: bool = False,
                async_status: bool = False):
    """List directory contents at a simulation location.
    
    Performs remote directory listing similar to Unix ls command.
    Supports standard ls flags for formatting and sorting.
    
    Examples:
        tellus simulation location ls MIS11.3-B tellus_hsm
        tellus simulation location ls MIS11.3-B tellus_hsm /data/output -l
        tellus simulation location ls MIS11.3-B tellus_localhost . -lah
    """
    try:
        # Get the simulation entity to verify location association and resolve templates
        service = _get_simulation_service()
        sim_entity = service._simulation_repo.get_by_id(sim_id)
        
        if sim_entity is None:
            _handle_simulation_not_found(sim_id, service)
            return
        
        if not sim_entity.is_location_associated(location_name):
            console.print(f"[red]Error:[/red] Location '{location_name}' is not associated with simulation '{sim_id}'")
            available_locations = list(sim_entity.get_associated_locations())
            console.print(f"Available locations: {', '.join(available_locations)}")
            return
        
        # Get location service to access the filesystem
        from ...application.container import get_service_container
        container = get_service_container()
        location_service = container.service_factory.location_service
        
        location = None
        try:
            location = location_service.get_location_filesystem(location_name)
        except Exception as e:
            console.print(f"[yellow]Warning:[/yellow] Location '{location_name}' not found in registry: {str(e)}")
            console.print(f"[yellow]Note:[/yellow] This appears to be a simulation-specific location context only.")
            console.print(f"The simulation knows about this location, but it's not registered in the global location registry.")
        
        # Use PathResolutionService for clean template resolution
        path_service = container.service_factory.path_resolution_service
        
        try:
            # Resolve the full path using the path resolution service
            if path.startswith('/'):
                # Absolute path - use as-is
                full_path = path
                console.print(f"[dim]Listing: {location_name}:{full_path}[/dim]")
                resolved_path = path  # Keep for filesystem access
            else:
                # Use path resolution service for template-based paths
                full_path = path_service.resolve_simulation_location_path(sim_id, location_name, path)
                console.print(f"[dim]Listing: {location_name}:{full_path}[/dim]")
                
                # Calculate the relative path for filesystem access
                if location is not None:
                    base_path = location.get_base_path().rstrip('/')
                    if full_path.startswith(base_path):
                        resolved_path = full_path[len(base_path):].lstrip('/')
                    else:
                        resolved_path = full_path
                else:
                    resolved_path = path
            
        except Exception as e:
            console.print(f"[yellow]Warning:[/yellow] Path resolution failed: {str(e)}")
            # Fallback to simple path
            resolved_path = path
            console.print(f"[dim]Listing: {location_name}:{resolved_path}[/dim]")
        
        # Try to get actual filesystem access
        if location is not None:
            try:
                # Use the clean architecture approach like location test does
                fs = location_service._create_location_filesystem(location)
                
                # Handle async execution with progress tracking for async mode
                import asyncio
                progress_tracker = AsyncProgressTracker() if async_status else None
                
                if async_status:
                    console.print("[dim]🚀 Starting async operation...[/dim]")
                
                asyncio.run(_perform_real_listing(fs, resolved_path, long, all, human_readable, 
                                                time, size, reverse, recursive, color, tape_status, location, async_status, progress_tracker))
            except Exception as e:
                console.print(f"[yellow]Warning:[/yellow] Could not access registered location filesystem: {str(e)}")
                console.print(f"[dim]Location '{location_name}' is registered but filesystem access failed[/dim]")
                # Fall back to showing what would happen
                _show_filesystem_listing(location_name, resolved_path, long, all, human_readable, 
                                        time, size, reverse, recursive, color)
        else:
            # Location not in registry - try simple file system access if it looks like a local path
            if path_prefix and not path_prefix.startswith(('http://', 'https://', 's3://', 'ssh://')):
                console.print(f"[dim]Location '{location_name}' not in global registry[/dim]")
                console.print(f"[dim]This appears to be a simulation-specific location context[/dim]")
                console.print(f"[dim]Attempting direct filesystem access to: {resolved_path}[/dim]")
                try:
                    _try_simple_filesystem_access(resolved_path, long, all, human_readable, 
                                                 time, size, reverse, recursive, color)
                except Exception as e:
                    console.print(f"[yellow]Could not access path:[/yellow] {str(e)}")
                    console.print(f"[dim]The simulation context path '{resolved_path}' may not exist on this system[/dim]")
                    console.print(f"[dim]This could mean:[/dim]")
                    console.print(f"[dim]  • The simulation data hasn't been created yet[/dim]")
                    console.print(f"[dim]  • The path is meant for a different system[/dim]") 
                    console.print(f"[dim]  • The location should be registered globally for proper access[/dim]")
                    _show_filesystem_listing(location_name, resolved_path, long, all, human_readable, 
                                            time, size, reverse, recursive, color)
            else:
                console.print(f"[yellow]Cannot access location '{location_name}'[/yellow]")
                console.print(f"[dim]Location is not registered globally and path context suggests remote access[/dim]")
                console.print(f"[dim]Consider registering this location with: tellus location create {location_name}[/dim]")
                _show_filesystem_listing(location_name, resolved_path, long, all, human_readable, 
                                        time, size, reverse, recursive, color)
        
    except Exception as e:
        console.print(f"[red]Error:[/red] {str(e)}")


def _try_simple_filesystem_access(path: str, long_format: bool, show_all: bool, 
                                 human_readable: bool, sort_time: bool, sort_size: bool, 
                                 reverse_sort: bool, recursive: bool, use_color: bool):
    """Try simple local filesystem access."""
    import os
    import stat
    from datetime import datetime
    
    try:
        if not os.path.exists(path):
            raise FileNotFoundError(f"Path does not exist: {path}")
            
        if not os.path.isdir(path):
            raise NotADirectoryError(f"Not a directory: {path}")
        
        # Get directory entries
        entries = []
        for name in os.listdir(path):
            if not show_all and name.startswith('.'):
                continue
                
            full_path = os.path.join(path, name)
            try:
                stat_info = os.stat(full_path)
                entry = {
                    'name': name,
                    'full_path': full_path,
                    'type': 'directory' if os.path.isdir(full_path) else 'file',
                    'size': stat_info.st_size,
                    'mtime': stat_info.st_mtime,
                    'mode': stat_info.st_mode
                }
                entries.append(entry)
            except (OSError, IOError):
                # Skip files we can't stat
                continue
        
        # Sort entries
        if sort_time:
            entries.sort(key=lambda x: x['mtime'], reverse=reverse_sort)
        elif sort_size:
            entries.sort(key=lambda x: x['size'], reverse=reverse_sort)
        else:
            entries.sort(key=lambda x: x['name'], reverse=reverse_sort)
        
        if not entries:
            console.print("[dim]Empty directory[/dim]")
            return
        
        if long_format:
            _show_simple_long_format(entries, human_readable, use_color)
        else:
            _show_simple_short_format(entries, use_color)
            
        if recursive:
            for entry in entries:
                if entry['type'] == 'directory':
                    console.print(f"\n[bold]{entry['name']}:[/bold]")
                    _try_simple_filesystem_access(entry['full_path'], long_format, show_all,
                                                human_readable, sort_time, sort_size, 
                                                reverse_sort, False, use_color)
                    
    except Exception as e:
        console.print(f"[red]Error accessing directory:[/red] {str(e)}")
        raise


def _show_simple_long_format(entries, human_readable: bool, use_color: bool):
    """Show entries in long format using simple file info."""
    import stat
    from datetime import datetime
    
    table = Table()
    table.add_column("Permissions", style="cyan")
    table.add_column("Size", style="green", justify="right")
    table.add_column("Modified", style="yellow") 
    table.add_column("Name", style="blue" if use_color else "white")
    
    for entry in entries:
        # File permissions
        mode = entry['mode']
        perms = stat.filemode(mode)
        
        # File size
        size = entry['size']
        if human_readable and size > 0:
            size_str = _human_readable_size(size)
        else:
            size_str = str(size)
        
        # Modification time
        mod_time = datetime.fromtimestamp(entry['mtime']).strftime("%Y-%m-%d %H:%M")
        
        # File name with proper styling
        name = entry['name']
        if entry['type'] == 'directory':
            name = f"{name}/"
        
        table.add_row(perms, size_str, mod_time, name)
    
    console.print(table)


def _show_simple_short_format(entries, use_color: bool):
    """Show entries in simple format using simple file info."""
    items = []
    for entry in entries:
        name = entry['name']
        if entry['type'] == 'directory':
            if use_color:
                items.append(f"[bold blue]{name}/[/bold blue]")
            else:
                items.append(f"{name}/")
        else:
            items.append(name)
    
    console.print("  ".join(items))


def _get_filesystem_for_location(location):
    """Get filesystem access for a location."""
    # Import here to avoid circular imports
    import fsspec

    from ...infrastructure.adapters.sandboxed_filesystem import \
        PathSandboxedFileSystem

    # Get location configuration
    config = location.config if hasattr(location, 'config') else {}
    protocol = location.protocol if hasattr(location, 'protocol') else config.get('protocol', 'file')
    
    if protocol == 'file' or protocol == 'local':
        # Local filesystem
        base_path = config.get('path', '.')
        fs = fsspec.filesystem('file')
        return PathSandboxedFileSystem(fs, base_path)
    
    elif protocol == 'ssh':
        # SSH filesystem
        ssh_config = {
            'host': config.get('host'),
            'username': config.get('username'),
            'port': config.get('port', 22),
        }
        # Add other SSH config as needed
        if config.get('key_filename'):
            ssh_config['client_keys'] = [config['key_filename']]
        
        fs = fsspec.filesystem('ssh', **ssh_config)
        base_path = config.get('path', '/')
        return PathSandboxedFileSystem(fs, base_path)
    
    elif protocol == 's3':
        # S3 filesystem
        s3_config = {}
        if config.get('aws_access_key_id'):
            s3_config['key'] = config['aws_access_key_id']
        if config.get('aws_secret_access_key'):
            s3_config['secret'] = config['aws_secret_access_key']
        if config.get('region'):
            s3_config['client_kwargs'] = {'region_name': config['region']}
        
        fs = fsspec.filesystem('s3', **s3_config)
        bucket = config.get('bucket', '')
        prefix = config.get('prefix', '')
        base_path = f"{bucket}/{prefix}".rstrip('/')
        return PathSandboxedFileSystem(fs, base_path)
    
    elif protocol == 'scoutfs':
        # ScoutFS filesystem
        storage_options = config.get('storage_options', {})
        scoutfs_config = {
            'host': storage_options.get('host'),
            'username': storage_options.get('username'),
            'port': storage_options.get('port', 22),
        }
        
        # Add ScoutFS-specific configuration
        if storage_options.get('scoutfs_config'):
            scoutfs_config['scoutfs_config'] = storage_options['scoutfs_config']
            
        # Add other SSH config as needed (ScoutFS extends SSH)
        if storage_options.get('key_filename'):
            scoutfs_config['client_keys'] = [storage_options['key_filename']]
        
        # Import and use ScoutFS filesystem
        from ...infrastructure.adapters.scoutfs_filesystem import \
            ScoutFSFileSystem
        fs = ScoutFSFileSystem(**scoutfs_config)
        base_path = config.get('path', '/')
        return PathSandboxedFileSystem(fs, base_path)
    
    else:
        raise ValueError(f"Unsupported protocol: {protocol}")


class AsyncProgressTracker:
    """Simple progress tracker for async operations."""
    
    def __init__(self):
        self.last_message = None
        self.last_timestamp = None
    
    def log(self, message: str):
        """Add a progress message."""
        import time
        timestamp = time.strftime("%H:%M:%S")
        
        # Only print if message is different or if 2+ seconds have passed
        current_time = time.time()
        if (self.last_message != message and 
            (self.last_timestamp is None or current_time - self.last_timestamp >= 1)):
            full_message = f"[dim]{timestamp}[/dim] {message}"
            console.print(full_message)
            self.last_message = message
            self.last_timestamp = current_time
    
    def clear(self):
        """Clear the progress display."""
        # Just add a newline for clean separation
        console.print()

async def _perform_real_listing(fs, path: str, long_format: bool, show_all: bool, 
                        human_readable: bool, sort_time: bool, sort_size: bool, 
                        reverse_sort: bool, recursive: bool, use_color: bool, 
                        tape_status: bool = False, location=None, async_status: bool = False, 
                        progress_tracker: AsyncProgressTracker = None):
    """Perform actual filesystem listing."""
    if progress_tracker is None:
        progress_tracker = AsyncProgressTracker()
    
    try:
        progress_tracker.log(f"🔍 Accessing path: {path}")
        # Check if path is a file or directory
        try:
            progress_tracker.log("📋 Getting path info...")
            path_info = fs.info(path)
            if path_info.get('type') == 'file':
                progress_tracker.log("📄 Single file detected")
                # Show single file info instead of trying to list it
                entries = [path_info]
                # For single files, we don't need recursion or directory-specific sorting
                if long_format:
                    await _show_long_format(entries, human_readable, use_color, tape_status, fs)
                else:
                    await _show_simple_format(entries, use_color, tape_status, fs)
                progress_tracker.clear()
                return
        except Exception:
            # If info() fails, fallback to trying ls() - might be a directory
            progress_tracker.log("⚠️ Path info failed, trying directory listing...")
            pass
        
        # Get directory contents
        progress_tracker.log("📁 Listing directory contents...")
        entries = fs.ls(path, detail=True)
        progress_tracker.log(f"✅ Found {len(entries)} items")
        
        # Filter hidden files if not showing all
        if not show_all:
            entries = [e for e in entries if not e['name'].split('/')[-1].startswith('.')]
        
        # Sort entries
        if sort_time:
            entries.sort(key=lambda x: x.get('mtime', 0), reverse=reverse_sort)
        elif sort_size:
            entries.sort(key=lambda x: x.get('size', 0), reverse=reverse_sort)
        else:
            entries.sort(key=lambda x: x['name'], reverse=reverse_sort)
        
        if not entries:
            console.print("[dim]Empty directory[/dim]")
            return
        
        # Get tape status in batch if async is enabled
        batch_status = {}
        if tape_status and async_status and hasattr(fs, 'check_online_status_batch'):
            progress_tracker.log("🎯 Starting batch tape status check...")
            batch_status = await _get_tape_status_batch(fs, entries, show_progress=False)  # Use our tracker instead
            progress_tracker.log(f"✅ Batch status completed ({len(batch_status)} items)")
        
        progress_tracker.log("🖨️ Formatting output...")
        if long_format:
            await _show_long_format(entries, human_readable, use_color, tape_status, fs, async_status, batch_status)
        else:
            await _show_simple_format(entries, use_color, tape_status, fs, async_status, batch_status)
        progress_tracker.log("✅ Output complete")
            
        if recursive:
            # Recursively list subdirectories
            dirs = [e for e in entries if e['type'] == 'directory']
            progress_tracker.log(f"🔄 Recursing into {len(dirs)} subdirectories...")
            for i, entry in enumerate(dirs):
                progress_tracker.log(f"📂 [{i+1}/{len(dirs)}] Entering {entry['name'].split('/')[-1]}")
                console.print(f"\n[bold]{entry['name']}:[/bold]")
                await _perform_real_listing(fs, entry['name'], long_format, show_all,
                                          human_readable, sort_time, sort_size, 
                                          reverse_sort, False, use_color, tape_status, location, async_status, progress_tracker)
    
    except Exception as e:
        progress_tracker.log(f"❌ Error: {str(e)}")
        progress_tracker.clear()
        console.print(f"[red]Error accessing filesystem:[/red] {str(e)}")
        raise
    finally:
        # Always clear progress display when done
        if progress_tracker:
            progress_tracker.clear()


async def _show_long_format(entries, human_readable: bool, use_color: bool, tape_status: bool = False, fs=None, async_status: bool = False, batch_status: dict = None):
    """Show entries in long format."""
    from datetime import datetime
    
    table = Table()
    table.add_column("Permissions", style="cyan")
    table.add_column("Size", style="green", justify="right")
    table.add_column("Modified", style="yellow")
    if tape_status:
        table.add_column("Tape", style="magenta", justify="center")
    table.add_column("Name", style="blue" if use_color else "white")
    
    for entry in entries:
        # File permissions (simplified)
        if entry['type'] == 'directory':
            perms = "drwxr-xr-x"
            name_style = "bold blue" if use_color else "bold"
            name = f"{entry['name'].split('/')[-1]}/"
        else:
            perms = "-rw-r--r--"
            name_style = "white"
            name = entry['name'].split('/')[-1]
        
        # File size
        size = entry.get('size', 0)
        if human_readable and size > 0:
            size_str = _human_readable_size(size)
        else:
            size_str = str(size)
        
        # Modification time
        mtime = entry.get('mtime', 0)
        if mtime:
            if isinstance(mtime, datetime):
                # Already a datetime object
                mod_time = mtime.strftime("%Y-%m-%d %H:%M")
            else:
                # Assume it's a timestamp
                mod_time = datetime.fromtimestamp(mtime).strftime("%Y-%m-%d %H:%M")
        else:
            mod_time = "-"
        
        # Tape status (ScoutFS only)
        if tape_status:
            if async_status and batch_status and entry['name'] in batch_status:
                tape_status_str = batch_status[entry['name']]
            else:
                tape_status_str = _get_tape_status(fs, entry['name'], entry['type'])
            table.add_row(perms, size_str, mod_time, tape_status_str, name)
        else:
            table.add_row(perms, size_str, mod_time, name)
    
    # Show performance note for large directories
    if tape_status and fs and hasattr(fs, 'check_online_status_batch') and len(entries) > 5:
        console.print("[dim]Note: For better performance with large directories, use --async-status flag[/dim]")
    
    console.print(table)


async def _show_simple_format(entries, use_color: bool, tape_status: bool = False, fs=None, async_status: bool = False, batch_status: dict = None):
    """Show entries in simple format."""
    items = []
    for entry in entries:
        name = entry['name'].split('/')[-1]
        if entry['type'] == 'directory':
            if use_color:
                item = f"[bold blue]{name}/[/bold blue]"
            else:
                item = f"{name}/"
        else:
            item = name
            
        # Add tape status indicator for files
        if tape_status and entry['type'] != 'directory':
            if async_status and batch_status and entry['name'] in batch_status:
                status = batch_status[entry['name']]
            else:
                status = _get_tape_status(fs, entry['name'], entry['type'])
            item = f"{item} {status}"
            
        items.append(item)
    
    # Print items in columns (simplified)
    console.print("  ".join(items))


def _human_readable_size(size: int) -> str:
    """Convert size to human readable format."""
    for unit in ['B', 'K', 'M', 'G', 'T']:
        if size < 1024.0:
            if unit == 'B':
                return f"{int(size)}{unit}"
            else:
                return f"{size:.1f}{unit}"
        size /= 1024.0
    return f"{size:.1f}P"


def _get_tape_status(fs, path: str, file_type: str) -> str:
    """Get tape staging status for ScoutFS filesystems."""
    # Skip directories
    if file_type == 'directory':
        return "-"
    
    # Check if this is a ScoutFS filesystem
    if not hasattr(fs, 'is_online'):
        return "-"
    
    try:
        # Use ScoutFS methods to check staging status
        is_online = fs.is_online(path)
        if is_online:
            return "[green]●[/green]"  # Online (green filled circle)
        else:
            return "[red]○[/red]"      # Offline (red empty circle)
    except ValueError as e:
        # Filesystem detection errors (no ScoutFS mount for path)
        if "No ScoutFS filesystem found" in str(e):
            return "-"  # Not a ScoutFS path, no status available
        return "[yellow]?[/yellow]"  # Other filesystem errors
    except Exception as e:
        # Other errors in staging status determination
        return "[yellow]?[/yellow]"


async def _get_tape_status_async(fs, path: str, file_type: str) -> str:
    """Asynchronously get tape staging status for ScoutFS filesystems."""
    # Skip directories
    if file_type == 'directory':
        return "-"
    
    # Check if this is a ScoutFS filesystem
    if not hasattr(fs, 'is_online_async'):
        return "-"
    
    try:
        # Use async ScoutFS methods to check staging status
        is_online = await fs.is_online_async(path)
        if is_online:
            return "[green]●[/green]"  # Online (green filled circle)
        else:
            return "[red]○[/red]"      # Offline (red empty circle)
    except ValueError as e:
        # Filesystem detection errors (no ScoutFS mount for path)
        if "No ScoutFS filesystem found" in str(e):
            return "-"  # Not a ScoutFS path, no status available
        return "[yellow]?[/yellow]"  # Other filesystem errors
    except Exception as e:
        # Other errors in staging status determination
        return "[yellow]?[/yellow]"


async def _get_tape_status_batch(fs, entries, show_progress=True) -> dict:
    """Batch check tape staging status for multiple files asynchronously.
    
    Args:
        fs: Filesystem instance
        entries: List of file entries with 'name' and 'type' keys
        show_progress: Whether to show progress during checking
        
    Returns:
        dict: Dictionary mapping file paths to their status strings
    """
    # Filter to only files that need status checking
    files_to_check = [
        entry['name'] for entry in entries 
        if entry['type'] != 'directory' and hasattr(fs, 'check_online_status_batch')
    ]
    
    if not files_to_check:
        # No files to check, return empty dict
        return {}
    
    # Show progress if requested
    if show_progress:
        from rich.progress import Progress, SpinnerColumn, TextColumn
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console
        ) as progress:
            task = progress.add_task(f"Checking tape status for {len(files_to_check)} files...", total=None)
            
            try:
                # Use the batch status checking method
                status_results = await fs.check_online_status_batch(files_to_check)
            except Exception as e:
                console.print(f"[yellow]Warning: Batch status check failed, using fallback: {e}[/yellow]")
                status_results = {}
    else:
        try:
            status_results = await fs.check_online_status_batch(files_to_check)
        except Exception as e:
            status_results = {}
    
    # Convert boolean status to display strings
    display_status = {}
    for path, is_online in status_results.items():
        if is_online:
            display_status[path] = "[green]●[/green]"
        else:
            display_status[path] = "[red]○[/red]"
    
    # Handle files that weren't in the batch result (errors, etc.)
    for path in files_to_check:
        if path not in display_status:
            display_status[path] = "[yellow]?[/yellow]"
    
    return display_status


def _show_filesystem_listing(location_name: str, path: str, long_format: bool, 
                           show_all: bool, human_readable: bool, sort_time: bool,
                           sort_size: bool, reverse_sort: bool, recursive: bool, 
                           use_color: bool):
    """Show what a filesystem listing would look like (fallback when access fails)."""
    console.print(f"[dim]Unable to access filesystem for location '{location_name}' at path '{path}'[/dim]")
    console.print("[dim]This command would list directory contents if the path were accessible.[/dim]")


def _perform_simple_listing(path: str, long_format: bool, show_all: bool,
                          human_readable: bool, sort_time: bool, sort_size: bool,
                          reverse_sort: bool, recursive: bool, use_color: bool):
    """Perform simple filesystem listing using os.listdir for local paths."""
    import os
    import stat
    from datetime import datetime
    from pathlib import Path
    
    try:
        base_path = Path(path)
        
        # Get directory entries
        entries = []
        for item in os.listdir(path):
            if not show_all and item.startswith('.'):
                continue
            
            item_path = base_path / item
            try:
                stat_info = item_path.stat()
                entry = {
                    'name': item,
                    'type': 'directory' if item_path.is_dir() else 'file',
                    'size': stat_info.st_size,
                    'mtime': stat_info.st_mtime
                }
                entries.append(entry)
            except (OSError, PermissionError):
                # Skip items we can't stat
                continue
        
        # Sort entries
        if sort_time:
            entries.sort(key=lambda x: x.get('mtime', 0), reverse=not reverse_sort)
        elif sort_size:
            entries.sort(key=lambda x: x.get('size', 0), reverse=not reverse_sort)
        else:
            entries.sort(key=lambda x: x['name'], reverse=reverse_sort)
        
        if not entries:
            console.print("[dim]Empty directory[/dim]")
            return
        
        if long_format:
            import asyncio
            asyncio.run(_show_long_format(entries, human_readable, use_color, False, None))  # No tape status for simple listing
        else:
            import asyncio
            asyncio.run(_show_simple_format(entries, use_color, False, None))  # No tape status for simple listing
            
        if recursive:
            # Recursively list subdirectories
            for entry in entries:
                if entry['type'] == 'directory':
                    subdir_path = base_path / entry['name']
                    console.print(f"\n[bold]{subdir_path}:[/bold]")
                    _perform_simple_listing(str(subdir_path), long_format, show_all,
                                          human_readable, sort_time, sort_size,
                                          reverse_sort, False, use_color)
    
    except PermissionError:
        console.print(f"[red]Error:[/red] Permission denied accessing: {path}")
    except FileNotFoundError:
        console.print(f"[red]Error:[/red] Directory not found: {path}")
    except Exception as e:
        console.print(f"[red]Error:[/red] {str(e)}")


@simulation_location.command(name="put")
@click.argument("sim_id")
@click.argument("location_name") 
@click.argument("local_file", required=False)
@click.argument("remote_path", required=False)
@click.option("--glob", "-g", help="Use glob pattern to select multiple files")
@click.option("--progress", "-p", is_flag=True, default=True, help="Show progress bar")
@click.option("--overwrite", "-f", is_flag=True, help="Force overwrite existing files")
def put_file(sim_id: str, location_name: str, local_file: str = None, remote_path: str = None,
            glob: str = None, progress: bool = True, overwrite: bool = False):
    """Upload a file to simulation location.
    
    Upload local files to a remote simulation location. Supports interactive
    selection, glob patterns, and progress tracking.
    
    Examples:
        tellus simulation location put MIS11.3-B tellus_hsm ./data.nc
        tellus simulation location put MIS11.3-B tellus_hsm ./data.nc /remote/path/
        tellus simulation location put MIS11.3-B tellus_hsm --glob "*.nc" /remote/output/
        tellus simulation location put MIS11.3-B tellus_hsm --interactive
    """
    import glob as glob_module
    import os
    from pathlib import Path
    
    try:
        # Get the simulation to verify location association
        service = _get_simulation_service()
        sim = service.get_simulation(sim_id)
        
        if sim is None:
            _handle_simulation_not_found(sim_id, service)
            return
            
        if location_name not in sim.associated_locations:
            console.print(f"[red]Error:[/red] Location '{location_name}' is not associated with simulation '{sim_id}'")
            console.print(f"Available locations: {', '.join(sim.associated_locations)}")
            return
            
        # Get location service to access the filesystem
        from ...application.container import get_service_container
        container = get_service_container()
        location_service = container.service_factory.location_service
        
        location = None
        try:
            location = location_service.get_location_filesystem(location_name)
            fs = location_service._create_location_filesystem(location)
        except Exception as e:
            console.print(f"[red]Error:[/red] Could not access location '{location_name}': {str(e)}")
            return
            
        # Use PathResolutionService for consistent path resolution
        path_service = container.service_factory.path_resolution_service
        
        # Resolve the base path for this simulation-location
        base_resolved_path = path_service.resolve_simulation_location_path(sim_id, location_name, ".")
        
        # Calculate relative path from base path for filesystem access
        if location is not None:
            base_path = location.get_base_path().rstrip('/')
            if base_resolved_path.startswith(base_path):
                resolved_path = base_resolved_path[len(base_path):].lstrip('/')
            else:
                resolved_path = base_resolved_path
        else:
            resolved_path = "."
        
        # Handle different input modes
        files_to_upload = []
        interactive = not local_file and not glob  # Auto-interactive when no args
        
        if interactive:
            # Interactive file selection using questionary
            from pathlib import Path

            import questionary

            # Get current directory files
            current_dir = Path.cwd()
            all_files = [f for f in current_dir.rglob('*') if f.is_file()]
            file_choices = [str(f.relative_to(current_dir)) for f in all_files]
            
            if not file_choices:
                console.print("[yellow]No files found in current directory[/yellow]")
                return
                
            selected_file = questionary.select(
                "Select a file to upload:",
                choices=file_choices,
                style=questionary.Style([
                    ('question', 'bold'),
                    ('selected', 'fg:#cc5454'),
                    ('pointer', 'fg:#ff0066 bold'),
                ])
            ).ask()
            
            if not selected_file:
                console.print("[yellow]No file selected[/yellow]")
                return
                
            files_to_upload = [(selected_file, selected_file)]  # (local_path, remote_name)
            
        elif glob:
            # Glob pattern selection
            matching_files = glob_module.glob(glob, recursive=True)
            if not matching_files:
                console.print(f"[yellow]No files match pattern '{glob}'[/yellow]")
                return
                
            files_to_upload = [(f, os.path.basename(f)) for f in matching_files if os.path.isfile(f)]
            
        else:
            # Single file upload
            if not os.path.exists(local_file):
                console.print(f"[red]Error:[/red] File '{local_file}' not found")
                return
                
            if os.path.isdir(local_file):
                console.print(f"[red]Error:[/red] '{local_file}' is a directory. Use mput for directories or specify a file.")
                return
                
            remote_name = remote_path or os.path.basename(local_file)
            files_to_upload = [(local_file, remote_name)]
            
        if not files_to_upload:
            console.print("[yellow]No files to upload[/yellow]")
            return
            
        console.print(f"[dim]Uploading {len(files_to_upload)} file(s) to {location_name}[/dim]")
        
        # Upload files
        for local_path, remote_name in files_to_upload:
            # Resolve remote path using PathResolutionService logic
            if remote_path and remote_path.startswith('/'):
                # Absolute path - need to compute relative path for filesystem
                if location is not None:
                    base_path = location.get_base_path().rstrip('/')
                    if remote_path.startswith(base_path):
                        resolved_remote = remote_path[len(base_path):].lstrip('/')
                    else:
                        resolved_remote = remote_path
                else:
                    resolved_remote = remote_path
            else:
                # Relative path - combine with resolved base path
                resolved_remote = f"{resolved_path}/{remote_name.lstrip('/')}" if resolved_path != "." else remote_name
                
            try:
                # Check if file exists and handle overwrite
                if fs.exists(resolved_remote) and not overwrite:
                    console.print(f"[yellow]Skipping '{remote_name}' - already exists (use --overwrite to force)[/yellow]")
                    continue
                    
                # Create progress callback if requested
                if progress:
                    from rich.progress import (BarColumn, Progress, SpinnerColumn, 
                                               TextColumn, DownloadColumn, TransferSpeedColumn)
                    
                    file_size = os.path.getsize(local_path)
                    
                    with Progress(
                        SpinnerColumn(),
                        TextColumn("[progress.description]{task.description}"),
                        BarColumn(),
                        DownloadColumn(),
                        TransferSpeedColumn(),
                        console=console
                    ) as prog:
                        task = prog.add_task(f"Uploading {os.path.basename(local_path)}", total=file_size)
                        
                        # Create proper fsspec callback
                        from fsspec.callbacks import Callback
                        
                        class ProgressCallback(Callback):
                            def __init__(self, prog_instance, task_id, size=None):
                                super().__init__(size=size)
                                self.prog = prog_instance
                                self.task = task_id
                                
                            def set_size(self, size):
                                """Called when file size is determined"""
                                super().set_size(size)
                                self.prog.update(self.task, total=size)
                                
                            def absolute_update(self, value):
                                """Called with absolute bytes transferred"""
                                super().absolute_update(value)
                                self.prog.update(self.task, completed=value)
                                
                            def relative_update(self, inc=1):
                                """Called with incremental bytes transferred"""
                                super().relative_update(inc)
                                self.prog.advance(self.task, inc)
                        
                        callback = ProgressCallback(prog, task, file_size)
                        
                        # Upload with progress callback
                        fs.put(local_path, resolved_remote, callback=callback)
                        
                else:
                    # Upload without progress
                    fs.put(local_path, resolved_remote)
                    
                console.print(f"[green]✓[/green] Uploaded '{local_path}' → '{resolved_remote}'")
                
            except Exception as e:
                console.print(f"[red]✗[/red] Failed to upload '{local_path}': {str(e)}")
                
        console.print(f"[green]Upload completed[/green]")
        
    except Exception as e:
        console.print(f"[red]Error:[/red] {str(e)}")


@simulation_location.command(name="get")
@click.argument("sim_id")
@click.argument("location_name")
@click.argument("remote_file", required=False)
@click.argument("local_path", required=False)
@click.option("--glob", "-g", help="Use glob pattern to select multiple remote files")
@click.option("--progress", "-p", is_flag=True, default=True, help="Show progress bar")
@click.option("--overwrite", "-f", is_flag=True, help="Force overwrite existing files")
def get_file(sim_id: str, location_name: str, remote_file: str = None, local_path: str = None,
            glob: str = None, progress: bool = True, overwrite: bool = False):
    """Download a file from simulation location.
    
    Download files from a remote simulation location to local filesystem.
    Supports interactive selection, glob patterns, and progress tracking.
    
    Examples:
        tellus simulation location get MIS11.3-B tellus_hsm data.nc
        tellus simulation location get MIS11.3-B tellus_hsm data.nc ./local_data.nc
        tellus simulation location get MIS11.3-B tellus_hsm --glob "*.nc" ./output/
        tellus simulation location get MIS11.3-B tellus_hsm --interactive
    """
    import os
    from pathlib import Path
    
    try:
        # Get the simulation to verify location association
        service = _get_simulation_service()
        sim = service.get_simulation(sim_id)
        
        if sim is None:
            _handle_simulation_not_found(sim_id, service)
            return
            
        if location_name not in sim.associated_locations:
            console.print(f"[red]Error:[/red] Location '{location_name}' is not associated with simulation '{sim_id}'")
            console.print(f"Available locations: {', '.join(sim.associated_locations)}")
            return
            
        # Get location service to access the filesystem
        from ...application.container import get_service_container
        container = get_service_container()
        location_service = container.service_factory.location_service
        
        location = None
        try:
            location = location_service.get_location_filesystem(location_name)
            fs = location_service._create_location_filesystem(location)
        except Exception as e:
            console.print(f"[red]Error:[/red] Could not access location '{location_name}': {str(e)}")
            return
            
        # Use PathResolutionService for consistent path resolution
        path_service = container.service_factory.path_resolution_service
        
        # Resolve the base path for this simulation-location
        base_resolved_path = path_service.resolve_simulation_location_path(sim_id, location_name, ".")
        
        # Calculate relative path from base path for filesystem access
        if location is not None:
            base_path = location.get_base_path().rstrip('/')
            if base_resolved_path.startswith(base_path):
                resolved_path = base_resolved_path[len(base_path):].lstrip('/')
            else:
                resolved_path = base_resolved_path
        else:
            resolved_path = "."
        
        # Handle different input modes
        files_to_download = []
        interactive = not remote_file and not glob  # Auto-interactive when no args
        
        if interactive:
            # Interactive file selection using questionary
            import questionary

            # Get remote directory listing  
            try:
                console.print(f"[dim]Listing files at: {resolved_path}[/dim]")
                entries = fs.ls(resolved_path, detail=True)
                file_choices = [entry['name'].split('/')[-1] for entry in entries if entry['type'] != 'directory']
                
                if not file_choices:
                    console.print(f"[yellow]No files found in remote location '{resolved_path}'[/yellow]")
                    return
                    
                selected_file = questionary.select(
                    "Select a file to download:",
                    choices=file_choices,
                    style=questionary.Style([
                        ('question', 'bold'),
                        ('selected', 'fg:#cc5454'),
                        ('pointer', 'fg:#ff0066 bold'),
                    ])
                ).ask()
                
                if not selected_file:
                    console.print("[yellow]No file selected[/yellow]")
                    return
                    
                files_to_download = [(selected_file, selected_file)]  # (remote_name, local_name)
                
            except Exception as e:
                console.print(f"[red]Error:[/red] Could not list remote files: {str(e)}")
                return
                
        elif glob:
            # Glob pattern selection on remote
            try:
                import fnmatch
                entries = fs.ls(resolved_path, detail=True)
                matching_files = []
                
                for entry in entries:
                    if entry['type'] != 'directory':
                        filename = entry['name'].split('/')[-1]
                        if fnmatch.fnmatch(filename, glob):
                            matching_files.append(filename)
                            
                if not matching_files:
                    console.print(f"[yellow]No remote files match pattern '{glob}'[/yellow]")
                    return
                    
                files_to_download = [(f, f) for f in matching_files]
                
            except Exception as e:
                console.print(f"[red]Error:[/red] Could not search remote files: {str(e)}")
                return
                
        else:
            # Single file download
            local_name = local_path or remote_file
            files_to_download = [(remote_file, local_name)]
            
        if not files_to_download:
            console.print("[yellow]No files to download[/yellow]")
            return
            
        console.print(f"[dim]Downloading {len(files_to_download)} file(s) from {location_name}[/dim]")
        
        # Download files
        for remote_name, local_name in files_to_download:
            # Resolve remote path using PathResolutionService
            if remote_name.startswith('/'):
                # Absolute path - need to compute relative path for filesystem
                if location is not None:
                    base_path = location.get_base_path().rstrip('/')
                    if remote_name.startswith(base_path):
                        resolved_remote = remote_name[len(base_path):].lstrip('/')
                    else:
                        resolved_remote = remote_name
                else:
                    resolved_remote = remote_name
            else:
                # Relative path - combine with resolved base path
                resolved_remote = f"{resolved_path}/{remote_name.lstrip('/')}" if resolved_path != "." else remote_name
                
            try:
                # Check if local file exists and handle overwrite
                if os.path.exists(local_name) and not overwrite:
                    console.print(f"[yellow]Skipping '{local_name}' - already exists (use --overwrite to force)[/yellow]")
                    continue
                    
                # Create local directory if needed
                local_dir = os.path.dirname(local_name)
                if local_dir:
                    Path(local_dir).mkdir(parents=True, exist_ok=True)
                    
                # Create progress callback if requested
                if progress:
                    from rich.progress import (BarColumn, Progress, SpinnerColumn, 
                                               TextColumn, DownloadColumn, TransferSpeedColumn)
                    
                    try:
                        file_info = fs.info(resolved_remote)
                        file_size = file_info.get('size', 0)
                        console.print(f"[dim]Debug: file_info = {file_info}[/dim]")
                        console.print(f"[dim]Debug: extracted file_size = {file_size}[/dim]")
                    except Exception as e:
                        console.print(f"[dim]Debug: fs.info() failed: {e}[/dim]")
                        file_size = 0
                        
                    with Progress(
                        SpinnerColumn(),
                        TextColumn("[progress.description]{task.description}"),
                        BarColumn(),
                        DownloadColumn(),
                        TransferSpeedColumn(),
                        console=console
                    ) as prog:
                        task = prog.add_task(f"Downloading {remote_name}", total=file_size)
                        
                        # Create proper fsspec callback with debugging
                        from fsspec.callbacks import Callback
                        
                        class DownloadProgressCallback(Callback):
                            def __init__(self, prog_instance, task_id, size=None):
                                console.print(f"[dim]Debug: DownloadProgressCallback init with size={size}[/dim]")
                                super().__init__(size=size)
                                self.prog = prog_instance
                                self.task = task_id
                                
                            def set_size(self, size):
                                """Called when file size is determined"""
                                console.print(f"[dim]Debug: set_size called with size={size}, current size={self.size}[/dim]")
                                # Only update if we don't have a size or the new size is larger
                                if self.size is None or self.size == 0 or size > self.size:
                                    super().set_size(size)
                                    self.prog.update(self.task, total=size)
                                else:
                                    console.print(f"[dim]Debug: ignoring set_size({size}) because we have better size={self.size}[/dim]")
                                
                            def absolute_update(self, value):
                                """Called with absolute bytes transferred"""
                                console.print(f"[dim]Debug: absolute_update called with value={value}[/dim]")
                                super().absolute_update(value)
                                self.prog.update(self.task, completed=value)
                                
                            def relative_update(self, inc=1):
                                """Called with incremental bytes transferred"""
                                console.print(f"[dim]Debug: relative_update called with inc={inc}[/dim]")
                                super().relative_update(inc)
                                self.prog.advance(self.task, inc)
                        
                        callback = DownloadProgressCallback(prog, task, file_size)
                        
                        # Download with progress callback
                        fs.get(resolved_remote, local_name, callback=callback)
                        
                else:
                    # Download without progress
                    fs.get(resolved_remote, local_name)
                    
                console.print(f"[green]✓[/green] Downloaded '{resolved_remote}' → '{local_name}'")
                
            except Exception as e:
                console.print(f"[red]✗[/red] Failed to download '{remote_name}': {str(e)}")
                
        console.print(f"[green]Download completed[/green]")
        
    except Exception as e:
        console.print(f"[red]Error:[/red] {str(e)}")


@simulation_location.command(name="mput")
@click.argument("sim_id")
@click.argument("location_name")
@click.argument("pattern", required=False)
@click.option("--recursive", "-r", is_flag=True, help="Upload directories recursively")
@click.option("--progress", "-p", is_flag=True, default=True, help="Show progress bar")
@click.option("--overwrite", "-f", is_flag=True, help="Force overwrite existing files")
@click.option("--exclude", help="Exclude pattern (can be used multiple times)", multiple=True)
def mput_files(sim_id: str, location_name: str, pattern: str = None,
               recursive: bool = False, progress: bool = True,
               overwrite: bool = False, exclude: tuple = ()):
    """Upload multiple files/directories to simulation location.
    
    Upload multiple files and directories using glob patterns or interactive selection.
    Supports recursive directory uploads and file exclusion patterns.
    
    Examples:
        tellus simulation location mput MIS11.3-B tellus_hsm "*.nc"
        tellus simulation location mput MIS11.3-B tellus_hsm --interactive
        tellus simulation location mput MIS11.3-B tellus_hsm "data/*" --recursive
        tellus simulation location mput MIS11.3-B tellus_hsm "*.txt" --exclude "*.tmp"
    """
    import fnmatch
    import glob as glob_module
    import os
    from pathlib import Path
    
    try:
        # Get the simulation to verify location association
        service = _get_simulation_service()
        sim = service.get_simulation(sim_id)
        
        if sim is None:
            _handle_simulation_not_found(sim_id, service)
            return
            
        if location_name not in sim.associated_locations:
            console.print(f"[red]Error:[/red] Location '{location_name}' is not associated with simulation '{sim_id}'")
            console.print(f"Available locations: {', '.join(sim.associated_locations)}")
            return
            
        # Get location service to access the filesystem
        from ...application.container import get_service_container
        container = get_service_container()
        location_service = container.service_factory.location_service
        
        location = None
        try:
            location = location_service.get_location_filesystem(location_name)
            fs = location_service._create_location_filesystem(location)
        except Exception as e:
            console.print(f"[red]Error:[/red] Could not access location '{location_name}': {str(e)}")
            return
            
        # Use PathResolutionService for consistent path resolution
        path_service = container.service_factory.path_resolution_service
        
        # Resolve the base path for this simulation-location
        base_resolved_path = path_service.resolve_simulation_location_path(sim_id, location_name, ".")
        
        # Calculate relative path from base path for filesystem access
        if location is not None:
            base_path = location.get_base_path().rstrip('/')
            if base_resolved_path.startswith(base_path):
                resolved_path = base_resolved_path[len(base_path):].lstrip('/')
            else:
                resolved_path = base_resolved_path
        else:
            resolved_path = "."
        
        # Handle different input modes
        files_to_upload = []
        interactive = not pattern  # Auto-interactive when no pattern
        
        if interactive:
            # Interactive selection using questionary
            import questionary
            
            current_dir = Path.cwd()
            
            # Get all files and directories
            all_items = []
            for item in current_dir.rglob('*'):
                relative_path = str(item.relative_to(current_dir))
                if item.is_file():
                    all_items.append(f"📄 {relative_path}")
                elif item.is_dir() and any(item.iterdir()):  # Non-empty directories
                    all_items.append(f"📁 {relative_path}/")
                    
            if not all_items:
                console.print("[yellow]No files found in current directory[/yellow]")
                return
                
            selected_items = questionary.checkbox(
                "Select files and directories to upload:",
                choices=sorted(all_items),
                style=questionary.Style([
                    ('question', 'bold'),
                    ('checkbox', 'fg:#ff0066'),
                    ('selected', 'fg:#cc5454'),
                    ('pointer', 'fg:#ff0066 bold'),
                ])
            ).ask()
            
            if not selected_items:
                console.print("[yellow]No items selected[/yellow]")
                return
                
            # Process selected items
            for item_display in selected_items:
                item_path = item_display[2:].rstrip('/')  # Remove emoji and trailing slash
                full_path = current_dir / item_path
                
                if full_path.is_file():
                    files_to_upload.append((str(full_path), item_path))
                elif full_path.is_dir() and recursive:
                    # Add all files in directory recursively
                    for file_path in full_path.rglob('*'):
                        if file_path.is_file():
                            rel_path = str(file_path.relative_to(current_dir))
                            files_to_upload.append((str(file_path), rel_path))
                elif full_path.is_dir():
                    console.print(f"[yellow]Skipping directory '{item_path}' (use --recursive to include)[/yellow]")
                    
        else:
            # Pattern-based selection
            matching_files = glob_module.glob(pattern, recursive=recursive)
            if not matching_files:
                console.print(f"[yellow]No files match pattern '{pattern}'[/yellow]")
                return
                
            # Process each match
            for match in matching_files:
                match_path = Path(match)
                
                # Check exclusion patterns
                excluded = False
                for exclude_pattern in exclude:
                    if fnmatch.fnmatch(match, exclude_pattern):
                        console.print(f"[dim]Excluding '{match}' (matches {exclude_pattern})[/dim]")
                        excluded = True
                        break
                        
                if excluded:
                    continue
                    
                if match_path.is_file():
                    files_to_upload.append((match, os.path.basename(match)))
                elif match_path.is_dir() and recursive:
                    # Add all files in directory recursively
                    for file_path in match_path.rglob('*'):
                        if file_path.is_file():
                            rel_path = str(file_path.relative_to(Path.cwd()))
                            
                            # Check exclusion on individual files too
                            excluded_file = False
                            for exclude_pattern in exclude:
                                if fnmatch.fnmatch(str(file_path), exclude_pattern):
                                    excluded_file = True
                                    break
                                    
                            if not excluded_file:
                                files_to_upload.append((str(file_path), rel_path))
                elif match_path.is_dir():
                    console.print(f"[yellow]Skipping directory '{match}' (use --recursive to include)[/yellow]")
                    
        if not files_to_upload:
            console.print("[yellow]No files to upload[/yellow]")
            return
            
        console.print(f"[dim]Uploading {len(files_to_upload)} file(s) to {location_name}[/dim]")
        
        # Upload files with progress
        successful_uploads = 0
        failed_uploads = 0
        
        if progress and len(files_to_upload) > 1:
            from rich.progress import (BarColumn, MofNCompleteColumn, Progress,
                                       SpinnerColumn, TextColumn)
            
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                MofNCompleteColumn(),
                TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
                console=console
            ) as prog:
                overall_task = prog.add_task("Uploading files...", total=len(files_to_upload))
                
                for local_path, remote_name in files_to_upload:
                    # Resolve remote path using PathResolutionService logic
                    if remote_name.startswith('/'):
                        # Absolute path - need to compute relative path for filesystem
                        if location is not None:
                            base_path = location.get_base_path().rstrip('/')
                            if remote_name.startswith(base_path):
                                resolved_remote = remote_name[len(base_path):].lstrip('/')
                            else:
                                resolved_remote = remote_name
                        else:
                            resolved_remote = remote_name
                    else:
                        # Relative path - combine with resolved base path
                        resolved_remote = f"{resolved_path}/{remote_name.lstrip('/')}" if resolved_path != "." else remote_name
                        
                    # Create remote directory if needed
                    remote_dir = os.path.dirname(resolved_remote)
                    if remote_dir:
                        try:
                            fs.makedirs(remote_dir, exist_ok=True)
                        except:
                            pass  # Directory might already exist
                            
                    try:
                        # Check if file exists and handle overwrite
                        if fs.exists(resolved_remote) and not overwrite:
                            console.print(f"[yellow]Skipping '{remote_name}' - already exists[/yellow]")
                            prog.advance(overall_task)
                            continue
                            
                        # Upload file
                        fs.put(local_path, resolved_remote)
                        successful_uploads += 1
                        console.print(f"[green]✓[/green] {remote_name}")
                        
                    except Exception as e:
                        console.print(f"[red]✗[/red] Failed to upload '{remote_name}': {str(e)}")
                        failed_uploads += 1
                        
                    prog.advance(overall_task)
                    
        else:
            # Upload without overall progress
            for local_path, remote_name in files_to_upload:
                # Resolve remote path using PathResolutionService logic
                if remote_name.startswith('/'):
                    # Absolute path - need to compute relative path for filesystem
                    if location is not None:
                        base_path = location.get_base_path().rstrip('/')
                        if remote_name.startswith(base_path):
                            resolved_remote = remote_name[len(base_path):].lstrip('/')
                        else:
                            resolved_remote = remote_name
                    else:
                        resolved_remote = remote_name
                else:
                    # Relative path - combine with resolved base path
                    resolved_remote = f"{resolved_path}/{remote_name.lstrip('/')}" if resolved_path != "." else remote_name
                    
                # Create remote directory if needed
                remote_dir = os.path.dirname(resolved_remote)
                if remote_dir:
                    try:
                        fs.makedirs(remote_dir, exist_ok=True)
                    except:
                        pass  # Directory might already exist
                        
                try:
                    # Check if file exists and handle overwrite
                    if fs.exists(resolved_remote) and not overwrite:
                        console.print(f"[yellow]Skipping '{remote_name}' - already exists (use --overwrite to force)[/yellow]")
                        continue
                        
                    # Upload file
                    fs.put(local_path, resolved_remote)
                    successful_uploads += 1
                    console.print(f"[green]✓[/green] Uploaded '{local_path}' → '{resolved_remote}'")
                    
                except Exception as e:
                    console.print(f"[red]✗[/red] Failed to upload '{remote_name}': {str(e)}")
                    failed_uploads += 1
                    
        # Summary
        console.print(f"\n[green]Upload summary:[/green] {successful_uploads} successful, {failed_uploads} failed")
        
    except Exception as e:
        console.print(f"[red]Error:[/red] {str(e)}")


@simulation_location.command(name="mget")
@click.argument("sim_id")
@click.argument("location_name")
@click.argument("pattern", required=False)
@click.option("--recursive", "-r", is_flag=True, help="Download directories recursively")
@click.option("--progress", "-p", is_flag=True, default=True, help="Show progress bar")
@click.option("--overwrite", "-f", is_flag=True, help="Force overwrite existing files")
@click.option("--exclude", help="Exclude pattern (can be used multiple times)", multiple=True)
@click.option("--output-dir", "-o", help="Output directory (default: current directory)")
def mget_files(sim_id: str, location_name: str, pattern: str = None,
               recursive: bool = False, progress: bool = True,
               overwrite: bool = False, exclude: tuple = (), output_dir: str = None):
    """Download multiple files/directories from simulation location.
    
    Download multiple files and directories using glob patterns or interactive selection.
    Supports recursive directory downloads and file exclusion patterns.
    
    Examples:
        tellus simulation location mget MIS11.3-B tellus_hsm "*.nc"
        tellus simulation location mget MIS11.3-B tellus_hsm --interactive
        tellus simulation location mget MIS11.3-B tellus_hsm "*.txt" --output-dir ./downloads/
        tellus simulation location mget MIS11.3-B tellus_hsm "*" --exclude "*.tmp" --recursive
    """
    import fnmatch
    import os
    from pathlib import Path
    
    try:
        # Get the simulation to verify location association
        service = _get_simulation_service()
        sim = service.get_simulation(sim_id)
        
        if sim is None:
            _handle_simulation_not_found(sim_id, service)
            return
            
        if location_name not in sim.associated_locations:
            console.print(f"[red]Error:[/red] Location '{location_name}' is not associated with simulation '{sim_id}'")
            console.print(f"Available locations: {', '.join(sim.associated_locations)}")
            return
            
        # Get location service to access the filesystem
        from ...application.container import get_service_container
        container = get_service_container()
        location_service = container.service_factory.location_service
        
        location = None
        try:
            location = location_service.get_location_filesystem(location_name)
            fs = location_service._create_location_filesystem(location)
        except Exception as e:
            console.print(f"[red]Error:[/red] Could not access location '{location_name}': {str(e)}")
            return
            
        # Use PathResolutionService for consistent path resolution
        path_service = container.service_factory.path_resolution_service
        
        # Resolve the base path for this simulation-location
        base_resolved_path = path_service.resolve_simulation_location_path(sim_id, location_name, ".")
        
        # Calculate relative path from base path for filesystem access
        if location is not None:
            base_path = location.get_base_path().rstrip('/')
            if base_resolved_path.startswith(base_path):
                resolved_path = base_resolved_path[len(base_path):].lstrip('/')
            else:
                resolved_path = base_resolved_path
        else:
            resolved_path = "."
        
        # Set up output directory
        output_path = Path(output_dir) if output_dir else Path.cwd()
        output_path.mkdir(parents=True, exist_ok=True)
        
        # Handle different input modes  
        files_to_download = []
        interactive = not pattern  # Auto-interactive when no pattern
        
        if interactive:
            # Interactive selection using questionary
            import questionary
            
            try:
                base_path = resolved_path
                
                def get_remote_items(remote_path, prefix=""):
                    """Recursively get remote items for display."""
                    items = []
                    try:
                        entries = fs.ls(remote_path, detail=True)
                        for entry in entries:
                            item_name = entry['name'].split('/')[-1]
                            full_prefix = f"{prefix}{item_name}" if prefix else item_name
                            
                            if entry['type'] == 'directory':
                                items.append(f"📁 {full_prefix}/")
                                if recursive:
                                    # Add subdirectory items
                                    sub_items = get_remote_items(entry['name'], f"{full_prefix}/")
                                    items.extend(sub_items)
                            else:
                                items.append(f"📄 {full_prefix}")
                    except:
                        pass
                    return items
                
                all_remote_items = get_remote_items(base_path)
                
                if not all_remote_items:
                    console.print(f"[yellow]No files found in remote location[/yellow]")
                    return
                    
                selected_items = questionary.checkbox(
                    "Select files to download:",
                    choices=sorted(all_remote_items),
                    style=questionary.Style([
                        ('question', 'bold'),
                        ('checkbox', 'fg:#ff0066'),
                        ('selected', 'fg:#cc5454'),
                        ('pointer', 'fg:#ff0066 bold'),
                    ])
                ).ask()
                
                if not selected_items:
                    console.print("[yellow]No files selected[/yellow]")
                    return
                    
                # Process selected items
                for item_display in selected_items:
                    if item_display.startswith("📄 "):  # File
                        remote_name = item_display[2:]  # Remove emoji
                        local_name = os.path.join(str(output_path), remote_name)
                        files_to_download.append((remote_name, local_name))
                        
            except Exception as e:
                console.print(f"[red]Error:[/red] Could not list remote files: {str(e)}")
                return
                
        else:
            # Pattern-based selection
            try:
                base_path = resolved_path
                
                def find_matching_files(remote_path, current_pattern):
                    """Find files matching pattern recursively."""
                    matches = []
                    try:
                        entries = fs.ls(remote_path, detail=True)
                        for entry in entries:
                            item_name = entry['name'].split('/')[-1]
                            
                            if entry['type'] == 'directory' and recursive:
                                # Recursively search subdirectories
                                sub_matches = find_matching_files(entry['name'], current_pattern)
                                matches.extend(sub_matches)
                            elif entry['type'] != 'directory':
                                # Check if file matches pattern
                                if fnmatch.fnmatch(item_name, current_pattern):
                                    # Check exclusion patterns
                                    excluded = False
                                    for exclude_pattern in exclude:
                                        if fnmatch.fnmatch(item_name, exclude_pattern):
                                            excluded = True
                                            break
                                    
                                    if not excluded:
                                        # Calculate relative path from base
                                        if remote_path == base_path:
                                            relative_remote = item_name
                                        else:
                                            relative_remote = entry['name'].replace(f"{base_path}/", "")
                                        
                                        local_name = os.path.join(str(output_path), relative_remote)
                                        matches.append((relative_remote, local_name))
                                        
                    except Exception as e:
                        console.print(f"[yellow]Warning:[/yellow] Could not search {remote_path}: {str(e)}")
                        
                    return matches
                
                files_to_download = find_matching_files(base_path, pattern)
                
            except Exception as e:
                console.print(f"[red]Error:[/red] Could not search remote files: {str(e)}")
                return
                
        if not files_to_download:
            console.print("[yellow]No files to download[/yellow]")
            return
            
        console.print(f"[dim]Downloading {len(files_to_download)} file(s) from {location_name}[/dim]")
        
        # Download files with progress
        successful_downloads = 0
        failed_downloads = 0
        
        if progress and len(files_to_download) > 1:
            from rich.progress import (BarColumn, MofNCompleteColumn, Progress,
                                       SpinnerColumn, TextColumn)
            
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                MofNCompleteColumn(),
                TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
                console=console
            ) as prog:
                overall_task = prog.add_task("Downloading files...", total=len(files_to_download))
                
                for remote_name, local_name in files_to_download:
                    # Resolve remote path using PathResolutionService logic
                    if remote_name.startswith('/'):
                        # Absolute path - need to compute relative path for filesystem
                        if location is not None:
                            base_path = location.get_base_path().rstrip('/')
                            if remote_name.startswith(base_path):
                                resolved_remote = remote_name[len(base_path):].lstrip('/')
                            else:
                                resolved_remote = remote_name
                        else:
                            resolved_remote = remote_name
                    else:
                        # Relative path - combine with resolved base path
                        resolved_remote = f"{resolved_path}/{remote_name.lstrip('/')}" if resolved_path != "." else remote_name
                        
                    try:
                        # Check if local file exists and handle overwrite
                        if os.path.exists(local_name) and not overwrite:
                            console.print(f"[yellow]Skipping '{remote_name}' - already exists[/yellow]")
                            prog.advance(overall_task)
                            continue
                            
                        # Create local directory if needed
                        local_dir = os.path.dirname(local_name)
                        if local_dir:
                            Path(local_dir).mkdir(parents=True, exist_ok=True)
                            
                        # Download file
                        fs.get(resolved_remote, local_name)
                        successful_downloads += 1
                        console.print(f"[green]✓[/green] {remote_name}")
                        
                    except Exception as e:
                        console.print(f"[red]✗[/red] Failed to download '{remote_name}': {str(e)}")
                        failed_downloads += 1
                        
                    prog.advance(overall_task)
                    
        else:
            # Download without overall progress
            for remote_name, local_name in files_to_download:
                # Resolve remote path using PathResolutionService logic
                if remote_name.startswith('/'):
                    # Absolute path - need to compute relative path for filesystem
                    if location is not None:
                        base_path = location.get_base_path().rstrip('/')
                        if remote_name.startswith(base_path):
                            resolved_remote = remote_name[len(base_path):].lstrip('/')
                        else:
                            resolved_remote = remote_name
                    else:
                        resolved_remote = remote_name
                else:
                    # Relative path - combine with resolved base path
                    resolved_remote = f"{resolved_path}/{remote_name.lstrip('/')}" if resolved_path != "." else remote_name
                    
                try:
                    # Check if local file exists and handle overwrite
                    if os.path.exists(local_name) and not overwrite:
                        console.print(f"[yellow]Skipping '{remote_name}' - already exists (use --overwrite to force)[/yellow]")
                        continue
                        
                    # Create local directory if needed
                    local_dir = os.path.dirname(local_name)
                    if local_dir:
                        Path(local_dir).mkdir(parents=True, exist_ok=True)
                        
                    # Download file
                    fs.get(resolved_remote, local_name)
                    successful_downloads += 1
                    console.print(f"[green]✓[/green] Downloaded '{resolved_remote}' → '{local_name}'")
                    
                except Exception as e:
                    console.print(f"[red]✗[/red] Failed to download '{remote_name}': {str(e)}")
                    failed_downloads += 1
                    
        # Summary
        console.print(f"\n[green]Download summary:[/green] {successful_downloads} successful, {failed_downloads} failed")
        
    except Exception as e:
        console.print(f"[red]Error:[/red] {str(e)}")


@simulation.group(name="files")
def simulation_files():
    """Manage simulation files."""
    pass


@simulation_files.command(name="list")
@click.argument("sim_id")
@click.option("--location", help="Filter by location")
@click.option("--content-type", help="Filter by content type")
@click.option("--type", help="Filter by file type (regular, archive, directory)")
def list_files(sim_id: str, location: str = None, content_type: str = None, type: str = None):
    """List files associated with a simulation."""
    try:
        container = get_service_container()
        unified_service = container.service_factory.unified_file_service
        simulation_service = container.service_factory.simulation_service
        
        # Verify simulation exists
        sim = simulation_service.get_simulation(sim_id)
        if not sim:
            _handle_simulation_not_found(sim_id, simulation_service)
            return
        
        # Get files associated with this simulation
        files = unified_service.get_simulation_files(sim_id)
        
        # Apply filters
        if location:
            files = [f for f in files if f.is_available_at_location(location)]
        if content_type:
            from ...domain.entities.simulation_file import FileContentType
            try:
                content_type_enum = FileContentType(content_type.lower())
                files = [f for f in files if f.content_type == content_type_enum]
            except ValueError:
                console.print(f"[red]Error:[/red] Invalid content type: {content_type}")
                console.print(f"Valid types: {', '.join([ct.value for ct in FileContentType])}")
                return
        if type:
            from ...domain.entities.simulation_file import FileType
            try:
                file_type_enum = FileType(type.lower())
                files = [f for f in files if f.file_type == file_type_enum]
            except ValueError:
                console.print(f"[red]Error:[/red] Invalid file type: {type}")
                console.print(f"Valid types: {', '.join([ft.value for ft in FileType])}")
                return
        
        if not files:
            console.print(f"[yellow]No files found for simulation '{sim_id}'[/yellow]")
            if location or content_type or type:
                console.print("[dim]Try removing filters to see if files exist[/dim]")
            else:
                console.print("[dim]Use 'tellus simulation files add' to register files from archives[/dim]")
            return
        
        # Show summary info
        console.print(f"[dim]Simulation locations: {', '.join(sim.associated_locations) if sim.associated_locations else 'none'}[/dim]")
        if location:
            console.print(f"[dim]Filtering by location: {location}[/dim]")
        if content_type:
            console.print(f"[dim]Filtering by content type: {content_type}[/dim]")
        if type:
            console.print(f"[dim]Filtering by file type: {type}[/dim]")
        
        # Create and populate table
        table = Table(title=f"Files for Simulation: {sim_id} ({len(files)} files)")
        table.add_column("Path", style="cyan", no_wrap=False)
        table.add_column("Size", style="green", justify="right")
        table.add_column("Content Type", style="yellow")
        table.add_column("File Type", style="magenta")
        table.add_column("Location", style="blue")
        table.add_column("Source", style="dim")
        
        # Sort files by path for consistent display
        files.sort(key=lambda f: f.relative_path)
        
        for file in files:
            # Format file size
            if file.size:
                if file.size > 1024**3:  # GB
                    size_str = f"{file.size / (1024**3):.1f} GB"
                elif file.size > 1024**2:  # MB
                    size_str = f"{file.size / (1024**2):.1f} MB"
                elif file.size > 1024:  # KB
                    size_str = f"{file.size / 1024:.1f} KB"
                else:
                    size_str = f"{file.size} B"
            else:
                size_str = "Unknown"
            
            # Determine source (archive or direct)
            if file.source_archives:
                source = f"Archive: {', '.join(list(file.source_archives)[:2])}"
                if len(file.source_archives) > 2:
                    source += f" +{len(file.source_archives) - 2}"
            else:
                source = "Direct"
            
            table.add_row(
                file.relative_path,
                size_str,
                file.content_type.value,
                file.file_type.value,
                file.location_name or "Unknown",
                source
            )
        
        console.print(Panel.fit(table))
        
        # Show summary statistics
        total_size = sum(f.size or 0 for f in files)
        archive_files = [f for f in files if f.file_type.value == 'archive']
        regular_files = [f for f in files if f.file_type.value == 'regular']
        
        console.print(f"[dim]Summary: {len(files)} total files, {len(regular_files)} regular, {len(archive_files)} archives")
        if total_size > 0:
            if total_size > 1024**3:  # GB
                console.print(f"[dim]Total size: {total_size / (1024**3):.1f} GB[/dim]")
            else:
                console.print(f"[dim]Total size: {total_size / (1024**2):.1f} MB[/dim]")
        
    except EntityNotFoundError:
        _handle_simulation_not_found(sim_id, simulation_service)
    except Exception as e:
        console.print(f"[red]Error:[/red] {str(e)}")


@simulation_files.command(name="add")
@click.argument("sim_id")
@click.option("--from-archive", required=True, help="Archive ID to register files from")
@click.option("--content-type", help="Filter files by content type (input, output, log, config)")
@click.option("--pattern", help="Filter files by pattern (glob)")
@click.option("--overwrite", is_flag=True, help="Overwrite existing file registrations")
@click.option("--dry-run", is_flag=True, help="Show what would be registered without making changes")
def add_files(sim_id: str, from_archive: str, content_type: str = None, pattern: str = None, 
              overwrite: bool = False, dry_run: bool = False):
    """Add (register) files from an archive to this simulation.
    
    This is similar to 'git add' - it registers files from an archive as being
    associated with this simulation for tracking and provenance.
    
    Examples:
    
        # Register all files from an archive
        tellus simulation files add my-sim --from-archive my-archive
        
        # Register only output files  
        tellus simulation files add my-sim --from-archive my-archive --content-type output
        
        # Register files matching a pattern
        tellus simulation files add my-sim --from-archive my-archive --pattern "*.nc"
        
        # Preview what would be registered
        tellus simulation files add my-sim --from-archive my-archive --dry-run
    """
    try:
        container = get_service_container()
        unified_service = container.service_factory.unified_file_service
        simulation_service = container.service_factory.simulation_service
        
        registration_dto = FileRegistrationDto(
            archive_id=from_archive,
            simulation_id=sim_id,
            overwrite_existing=overwrite,
            content_type_filter=content_type,
            pattern_filter=pattern,
            preserve_archive_references=True
        )
        
        if dry_run:
            # Get files that would be registered  
            # For dry-run, we need to get files from archive to preview
            # The unified service doesn't have this exact method yet, so use simulation service for preview
            try:
                archive_service = container.service_factory.archive_service
                archive_files = archive_service.get_archive_files_for_simulation(
                    from_archive, sim_id, content_type, pattern
                )
            except Exception:
                # Fallback if archive service fails
                archive_files = []
            
            console.print(f"[yellow]Dry run:[/yellow] Would register {len(archive_files)} files from archive '{from_archive}'")
            
            if archive_files:
                table = Table(title="Files to be registered")
                table.add_column("Path", style="cyan")
                table.add_column("Size", style="green") 
                table.add_column("Type", style="yellow")
                table.add_column("Archive", style="blue")
                
                for file in archive_files[:10]:  # Show first 10
                    table.add_row(
                        file.relative_path,
                        f"{file.size:,} bytes" if file.size else "Unknown",
                        file.content_type.value if file.content_type else "Unknown",
                        from_archive
                    )
                
                console.print(Panel.fit(table))
                if len(archive_files) > 10:
                    console.print(f"[dim]... and {len(archive_files) - 10} more files[/dim]")
            
            console.print("[dim]Use without --dry-run to register these files[/dim]")
            return
        
        result = unified_service.register_files_to_simulation(registration_dto)
        
        if result.success:
            console.print(f"[green]✓[/green] Successfully registered files from archive '{from_archive}':")
            console.print(f"  • {result.files_registered} files registered")
            console.print(f"  • {result.files_updated} files updated") 
            console.print(f"  • {result.files_skipped} files skipped")
        else:
            console.print(f"[red]Error:[/red] {result.error_message}")
            
    except EntityNotFoundError as e:
        if "simulation" in str(e).lower():
            _handle_simulation_not_found(sim_id, simulation_service)
        else:
            console.print(f"[red]Error:[/red] Archive '{from_archive}' not found")
    except Exception as e:
        console.print(f"[red]Error:[/red] {str(e)}")


@simulation_files.command(name="rm")
@click.argument("sim_id")
@click.option("--from-archive", required=True, help="Archive ID to unregister files from")
@click.option("--content-type", help="Filter files by content type")
@click.option("--pattern", help="Filter files by pattern (glob)")
@click.option("--force", is_flag=True, help="Force removal without confirmation")
@click.option("--dry-run", is_flag=True, help="Show what would be removed without making changes")
def remove_files(sim_id: str, from_archive: str, content_type: str = None, pattern: str = None,
                force: bool = False, dry_run: bool = False):
    """Remove (unregister) files from this simulation.
    
    This is similar to 'git rm' - it removes the association between files and this
    simulation, but doesn't delete the actual files from archives.
    
    Examples:
    
        # Remove all files from a specific archive
        tellus simulation files rm my-sim --from-archive my-archive
        
        # Remove only output files
        tellus simulation files rm my-sim --from-archive my-archive --content-type output
        
        # Preview what would be removed
        tellus simulation files rm my-sim --from-archive my-archive --dry-run
    """
    try:
        container = get_service_container()
        simulation_service = container.service_factory.simulation_service
        
        if dry_run:
            # Get current files to show what would be removed
            files = simulation_service.get_simulation_files(sim_id)
            matching_files = [
                f for f in files 
                if from_archive in f.source_archives and
                (not content_type or f.content_type.value == content_type) and
                (not pattern or f.matches_pattern(pattern))
            ]
            
            console.print(f"[yellow]Dry run:[/yellow] Would remove {len(matching_files)} files")
            
            if matching_files:
                table = Table(title="Files to be removed")
                table.add_column("Path", style="cyan")
                table.add_column("Type", style="yellow")
                table.add_column("Archive", style="blue")
                
                for file in matching_files[:10]:  # Show first 10
                    table.add_row(
                        file.relative_path,
                        file.content_type.value if file.content_type else "Unknown", 
                        from_archive
                    )
                
                console.print(Panel.fit(table))
                if len(matching_files) > 10:
                    console.print(f"[dim]... and {len(matching_files) - 10} more files[/dim]")
            
            console.print("[dim]Use without --dry-run to remove these files[/dim]")
            return
        
        if not force:
            if not click.confirm(f"Remove file associations from archive '{from_archive}' for simulation '{sim_id}'?"):
                console.print("[yellow]Removal cancelled.[/yellow]")
                return
        
        result = simulation_service.unregister_archive_files(sim_id, from_archive, content_type, pattern)
        
        if result.success:
            console.print(f"[green]✓[/green] Successfully removed file associations:")
            console.print(f"  • {result.files_removed} files unregistered")
        else:
            console.print(f"[red]Error:[/red] {result.error_message}")
            
    except EntityNotFoundError as e:
        if "simulation" in str(e).lower():
            _handle_simulation_not_found(sim_id, simulation_service)
        else:
            console.print(f"[red]Error:[/red] Archive '{from_archive}' not found")
    except Exception as e:
        console.print(f"[red]Error:[/red] {str(e)}")


@simulation_files.command(name="status")  
@click.argument("sim_id")
@click.option("--show-archives", is_flag=True, help="Show which archives contain each file")
@click.option("--content-type", help="Filter by content type")
def status_files(sim_id: str, show_archives: bool = False, content_type: str = None):
    """Show file status and archive associations.
    
    This is similar to 'git status' - it shows the current state of files
    associated with this simulation and their archive sources.
    
    Examples:
    
        # Show file summary
        tellus simulation files status my-sim
        
        # Show detailed archive information
        tellus simulation files status my-sim --show-archives
        
        # Filter by content type
        tellus simulation files status my-sim --content-type output
    """
    try:
        container = get_service_container()
        simulation_service = container.service_factory.simulation_service
        
        files = simulation_service.get_simulation_files(sim_id)
        
        if content_type:
            files = [f for f in files if f.content_type and f.content_type.value == content_type]
        
        if not files:
            console.print(f"[yellow]No files registered for simulation '{sim_id}'[/yellow]")
            console.print("[dim]Use 'tellus simulation files add' to register files from archives[/dim]")
            return
        
        # Summary statistics
        total_files = len(files)
        archives_used = set()
        content_types = {}
        
        for file in files:
            archives_used.update(file.source_archives)
            if file.content_type:
                content_type_name = file.content_type.value
                content_types[content_type_name] = content_types.get(content_type_name, 0) + 1
        
        # Summary table
        summary_table = Table(title=f"File Status for Simulation: {sim_id}")
        summary_table.add_column("Metric", style="cyan")
        summary_table.add_column("Value", style="green")
        
        summary_table.add_row("Total Files", str(total_files))
        summary_table.add_row("Source Archives", str(len(archives_used)))
        summary_table.add_row("Content Types", str(len(content_types)))
        
        console.print(Panel.fit(summary_table))
        
        # Content type breakdown
        if content_types:
            ct_table = Table(title="Files by Content Type")
            ct_table.add_column("Type", style="yellow")
            ct_table.add_column("Count", style="green")
            
            for ct, count in sorted(content_types.items()):
                ct_table.add_row(ct, str(count))
            
            console.print(Panel.fit(ct_table))
        
        # Archive sources
        if archives_used:
            console.print(f"[cyan]Source Archives:[/cyan] {', '.join(sorted(archives_used))}")
        
        # Detailed file listing if requested
        if show_archives:
            file_table = Table(title="Files with Archive Sources")
            file_table.add_column("Path", style="cyan")
            file_table.add_column("Type", style="yellow")
            file_table.add_column("Archives", style="blue")
            
            for file in sorted(files, key=lambda f: f.relative_path):
                file_table.add_row(
                    file.relative_path,
                    file.content_type.value if file.content_type else "Unknown",
                    ", ".join(sorted(file.source_archives))
                )
            
            console.print(Panel.fit(file_table))
            
    except EntityNotFoundError:
        _handle_simulation_not_found(sim_id, simulation_service)
    except Exception as e:
        console.print(f"[red]Error:[/red] {str(e)}")


@simulation.group(name="workflow")
def simulation_workflow():
    """Manage simulation workflows."""
    pass


@simulation_workflow.command(name="list")
@click.argument("sim_id")
def list_workflows(sim_id: str):
    """List workflows associated with a simulation."""
    try:
        # This would integrate with the workflow service
        # For now, show a placeholder
        console.print(f"[yellow]Note:[/yellow] Workflow listing for simulation '{sim_id}' is not yet implemented.")
        console.print("This would integrate with the workflow execution service.")
        
    except Exception as e:
        console.print(f"[red]Error:[/red] {str(e)}")


@simulation.command(name="attrs")
@click.argument("sim_id")
@click.option("--set", "set_attr", nargs=2, metavar="KEY VALUE", help="Set an attribute")
@click.option("--get", "get_attr", help="Get an attribute value")
@click.option("--list-all", "list_all", is_flag=True, help="List all attributes")
def manage_attributes(sim_id: str, set_attr: tuple = None, get_attr: str = None, list_all: bool = False):
    """Manage simulation attributes."""
    try:
        service = _get_simulation_service()
        
        if set_attr:
            key, value = set_attr
            service.add_simulation_attribute(sim_id, key, value)
            console.print(f"[green]✓[/green] Set attribute '{key}' = '{value}' for simulation '{sim_id}'")
            
        elif get_attr:
            sim = service.get_simulation(sim_id)
            if sim.attrs and get_attr in sim.attrs:
                console.print(f"{get_attr}: {sim.attrs[get_attr]}")
            else:
                console.print(f"[yellow]Attribute '{get_attr}' not found for simulation '{sim_id}'[/yellow]")
                
        elif list_all:
            sim = service.get_simulation(sim_id)
            if not sim.attrs:
                console.print(f"No attributes set for simulation '{sim_id}'")
                return
                
            table = Table(title=f"Attributes for Simulation: {sim_id}")
            table.add_column("Key", style="cyan")
            table.add_column("Value", style="green")
            
            for key, value in sorted(sim.attrs.items()):
                table.add_row(key, str(value))
            
            console.print(Panel.fit(table))
        else:
            # Default behavior: list all attributes
            sim = service.get_simulation(sim_id)
            if not sim.attrs:
                console.print(f"No attributes set for simulation '{sim_id}'")
                return
                
            table = Table(title=f"Attributes for Simulation: {sim_id}")
            table.add_column("Key", style="cyan")
            table.add_column("Value", style="green")
            
            for key, value in sorted(sim.attrs.items()):
                table.add_row(key, str(value))
            
            console.print(Panel.fit(table))
            
    except Exception as e:
        console.print(f"[red]Error:[/red] {str(e)}")