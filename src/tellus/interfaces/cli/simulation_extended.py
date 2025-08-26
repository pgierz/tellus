"""Extended simulation CLI commands."""

import rich_click as click
from rich.console import Console
from rich.table import Table
from rich.panel import Panel

from .main import console
from .simulation import simulation, _get_simulation_service
from ...application.dtos import UpdateSimulationDto, SimulationLocationAssociationDto


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
@click.argument("sim_id")
@click.option("--model-id", help="Update model identifier")
@click.option("--path", help="Update simulation path")
@click.option("--description", help="Update simulation description")
def update_simulation(sim_id: str, model_id: str = None, path: str = None, description: str = None):
    """Update an existing simulation."""
    try:
        service = _get_simulation_service()
        
        dto = UpdateSimulationDto(
            model_id=model_id,
            path=path,
            description=description
        )
        
        result = service.update_simulation(sim_id, dto)
        console.print(f"[green]✓[/green] Updated simulation: {result.simulation_id}")
        
    except Exception as e:
        console.print(f"[red]Error:[/red] {str(e)}")


@simulation.command(name="delete")
@click.argument("sim_id")
@click.option("--force", is_flag=True, help="Force deletion without confirmation")
def delete_simulation(sim_id: str, force: bool = False):
    """Delete a simulation."""
    try:
        if not force:
            if not click.confirm(f"Are you sure you want to delete simulation '{sim_id}'?"):
                console.print("[yellow]Deletion cancelled.[/yellow]")
                return
        
        service = _get_simulation_service()
        success = service.delete_simulation(sim_id)
        
        if success:
            console.print(f"[green]✓[/green] Deleted simulation: {sim_id}")
        else:
            console.print(f"[red]Error:[/red] Could not delete simulation: {sim_id}")
        
    except Exception as e:
        console.print(f"[red]Error:[/red] {str(e)}")


@simulation.group(name="location")
def simulation_location():
    """Manage simulation-location associations."""
    pass


@simulation_location.command(name="add")
@click.argument("sim_id", required=False)
@click.argument("location_name", required=False)
@click.option("--context", help="JSON string with location context data")
def add_location(sim_id: str = None, location_name: str = None, context: str = None):
    """Associate a location with a simulation.
    
    If no arguments are provided, launches an interactive selection wizard.
    """
    try:
        import json
        
        service = _get_simulation_service()
        
        # Interactive mode when no arguments provided
        if not sim_id or not location_name:
            import questionary
            
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
@click.argument("sim_id")
@click.argument("location_name")
def remove_location(sim_id: str, location_name: str):
    """Remove a location association from a simulation."""
    try:
        service = _get_simulation_service()
        result = service.disassociate_simulation_from_location(sim_id, location_name)
        console.print(f"[green]✓[/green] Removed location '{location_name}' from simulation '{sim_id}'")
        
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
def ls_location(sim_id: str, location_name: str, path: str = ".", 
                long: bool = False, all: bool = False, human_readable: bool = False,
                time: bool = False, size: bool = False, reverse: bool = False,
                recursive: bool = False, color: bool = True, tape_status: bool = False):
    """List directory contents at a simulation location.
    
    Performs remote directory listing similar to Unix ls command.
    Supports standard ls flags for formatting and sorting.
    
    Examples:
        tellus simulation location ls MIS11.3-B tellus_hsm
        tellus simulation location ls MIS11.3-B tellus_hsm /data/output -l
        tellus simulation location ls MIS11.3-B tellus_localhost . -lah
    """
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
        except Exception as e:
            console.print(f"[yellow]Warning:[/yellow] Location '{location_name}' not found in registry: {str(e)}")
            console.print(f"[yellow]Note:[/yellow] This appears to be a simulation-specific location context only.")
            console.print(f"The simulation knows about this location, but it's not registered in the global location registry.")
        
        # Get the location context for path resolution
        context = sim.get_location_context(location_name) if hasattr(sim, 'get_location_context') else {}
        path_prefix = context.get('path_prefix', '') if context else ''
        
        # Resolve the actual path
        if path.startswith('/'):
            # Absolute path - use as-is, ignore path_prefix
            resolved_path = path
        elif path_prefix and path == ".":
            # Use the path prefix as the base path
            resolved_path = path_prefix.rstrip('/ ')
        elif path_prefix:
            # Combine path prefix with requested path
            resolved_path = f"{path_prefix.rstrip('/ ')}/{path.lstrip('/')}"
        else:
            resolved_path = path
        
        # Show full path context if we have location information
        if location is not None and hasattr(location, 'get_base_path'):
            base_path = location.get_base_path() or "/"
            full_path = f"{base_path.rstrip('/')}/{resolved_path.lstrip('/')}" if resolved_path else base_path
            console.print(f"[dim]Listing: {location_name}:{full_path}[/dim]")
        else:
            console.print(f"[dim]Listing: {location_name}:{resolved_path}[/dim]")
        
        # Try to get actual filesystem access
        if location is not None:
            try:
                # Use the clean architecture approach like location test does
                fs = location_service._create_location_filesystem(location)
                _perform_real_listing(fs, resolved_path, long, all, human_readable, 
                                    time, size, reverse, recursive, color, tape_status, location)
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
    from ...infrastructure.adapters.sandboxed_filesystem import PathSandboxedFileSystem
    import fsspec
    
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
        from ...infrastructure.adapters.scoutfs_filesystem import ScoutFSFileSystem
        fs = ScoutFSFileSystem(**scoutfs_config)
        base_path = config.get('path', '/')
        return PathSandboxedFileSystem(fs, base_path)
    
    else:
        raise ValueError(f"Unsupported protocol: {protocol}")


def _perform_real_listing(fs, path: str, long_format: bool, show_all: bool, 
                        human_readable: bool, sort_time: bool, sort_size: bool, 
                        reverse_sort: bool, recursive: bool, use_color: bool, 
                        tape_status: bool = False, location=None):
    """Perform actual filesystem listing."""
    try:
        # Get directory contents
        entries = fs.ls(path, detail=True)
        
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
        
        if long_format:
            _show_long_format(entries, human_readable, use_color, tape_status, fs)
        else:
            _show_simple_format(entries, use_color, tape_status, fs)
            
        if recursive:
            # Recursively list subdirectories
            for entry in entries:
                if entry['type'] == 'directory':
                    console.print(f"\n[bold]{entry['name']}:[/bold]")
                    _perform_real_listing(fs, entry['name'], long_format, show_all,
                                        human_readable, sort_time, sort_size, 
                                        reverse_sort, False, use_color, tape_status, location)
    
    except Exception as e:
        console.print(f"[red]Error accessing filesystem:[/red] {str(e)}")
        raise


def _show_long_format(entries, human_readable: bool, use_color: bool, tape_status: bool = False, fs=None):
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
            tape_status_str = _get_tape_status(fs, entry['name'], entry['type'])
            table.add_row(perms, size_str, mod_time, tape_status_str, name)
        else:
            table.add_row(perms, size_str, mod_time, name)
    
    console.print(table)


def _show_simple_format(entries, use_color: bool, tape_status: bool = False, fs=None):
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
            _show_long_format(entries, human_readable, use_color, tape_status, fs)
        else:
            _show_simple_format(entries, use_color, tape_status, fs)
            
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
    import os
    import glob as glob_module
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
            
        # Get location context for path resolution
        context = sim.get_location_context(location_name) if hasattr(sim, 'get_location_context') else {}
        path_prefix = context.get('path_prefix', '') if context else ''
        
        # Handle different input modes
        files_to_upload = []
        interactive = not local_file and not glob  # Auto-interactive when no args
        
        if interactive:
            # Interactive file selection using questionary
            import questionary
            from pathlib import Path
            
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
            # Resolve remote path with context
            if remote_path and remote_path.startswith('/'):
                # Absolute path
                resolved_remote = remote_path
            elif path_prefix:
                # Use path prefix
                resolved_remote = f"{path_prefix.rstrip('/')}/{remote_name.lstrip('/')}"
            else:
                resolved_remote = remote_name
                
            try:
                # Check if file exists and handle overwrite
                if fs.exists(resolved_remote) and not overwrite:
                    console.print(f"[yellow]Skipping '{remote_name}' - already exists (use --overwrite to force)[/yellow]")
                    continue
                    
                # Create progress callback if requested
                if progress:
                    from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn
                    
                    file_size = os.path.getsize(local_path)
                    with Progress(
                        SpinnerColumn(),
                        TextColumn("[progress.description]{task.description}"),
                        BarColumn(),
                        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
                        console=console
                    ) as prog:
                        task = prog.add_task(f"Uploading {os.path.basename(local_path)}", total=file_size)
                        
                        # Create proper fsspec callback for upload progress
                        from fsspec.callbacks import Callback
                        
                        class ProgressCallback(Callback):
                            def __init__(self, progress_obj, task_id):
                                super().__init__()
                                self.prog = progress_obj
                                self.task = task_id
                                
                            def set_size(self, size):
                                """Called when file size is known"""
                                self.prog.update(self.task, total=size)
                                
                            def absolute_update(self, value):
                                """Called with absolute bytes transferred"""
                                self.prog.update(self.task, completed=value)
                        
                        callback = ProgressCallback(prog, task)
                        
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
            
        # Get location context for path resolution
        context = sim.get_location_context(location_name) if hasattr(sim, 'get_location_context') else {}
        path_prefix = context.get('path_prefix', '') if context else ''
        
        # Handle different input modes
        files_to_download = []
        interactive = not remote_file and not glob  # Auto-interactive when no args
        
        if interactive:
            # Interactive file selection using questionary
            import questionary
            
            # Get remote directory listing  
            try:
                # Use the same path resolution logic as ls command
                base_path = path_prefix.rstrip('/ ') if path_prefix else '.'
                console.print(f"[dim]Listing files at: {base_path}[/dim]")
                entries = fs.ls(base_path, detail=True)
                file_choices = [entry['name'].split('/')[-1] for entry in entries if entry['type'] != 'directory']
                
                if not file_choices:
                    console.print(f"[yellow]No files found in remote location '{base_path}'[/yellow]")
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
                base_path = path_prefix.rstrip('/') if path_prefix else '.'
                entries = fs.ls(base_path, detail=True)
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
            # Resolve remote path with context
            if remote_name.startswith('/'):
                # Absolute path
                resolved_remote = remote_name
            elif path_prefix:
                # Use path prefix
                resolved_remote = f"{path_prefix.rstrip('/')}/{remote_name.lstrip('/')}"
            else:
                resolved_remote = remote_name
                
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
                    from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn
                    
                    try:
                        file_info = fs.info(resolved_remote)
                        file_size = file_info.get('size', 0)
                    except:
                        file_size = 0
                        
                    with Progress(
                        SpinnerColumn(),
                        TextColumn("[progress.description]{task.description}"),
                        BarColumn(),
                        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
                        console=console
                    ) as prog:
                        task = prog.add_task(f"Downloading {remote_name}", total=file_size)
                        
                        # Create proper fsspec callback for download progress
                        from fsspec.callbacks import Callback
                        
                        class ProgressCallback(Callback):
                            def __init__(self, progress_obj, task_id):
                                super().__init__()
                                self.prog = progress_obj
                                self.task = task_id
                                
                            def set_size(self, size):
                                """Called when file size is known"""
                                self.prog.update(self.task, total=size)
                                
                            def absolute_update(self, value):
                                """Called with absolute bytes transferred"""
                                self.prog.update(self.task, completed=value)
                        
                        callback = ProgressCallback(prog, task)
                        
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
    import os
    import glob as glob_module
    from pathlib import Path
    import fnmatch
    
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
            
        # Get location context for path resolution
        context = sim.get_location_context(location_name) if hasattr(sim, 'get_location_context') else {}
        path_prefix = context.get('path_prefix', '') if context else ''
        
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
            from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, MofNCompleteColumn
            
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
                    # Resolve remote path with context
                    if path_prefix:
                        resolved_remote = f"{path_prefix.rstrip('/')}/{remote_name.lstrip('/')}"
                    else:
                        resolved_remote = remote_name
                        
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
                # Resolve remote path with context
                if path_prefix:
                    resolved_remote = f"{path_prefix.rstrip('/')}/{remote_name.lstrip('/')}"
                else:
                    resolved_remote = remote_name
                    
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
    import os
    from pathlib import Path
    import fnmatch
    
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
            
        # Get location context for path resolution
        context = sim.get_location_context(location_name) if hasattr(sim, 'get_location_context') else {}
        path_prefix = context.get('path_prefix', '') if context else ''
        
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
                base_path = path_prefix.rstrip('/') if path_prefix else '.'
                
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
                base_path = path_prefix.rstrip('/') if path_prefix else '.'
                
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
            from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, MofNCompleteColumn
            
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
                    # Resolve remote path with context
                    if path_prefix:
                        resolved_remote = f"{path_prefix.rstrip('/')}/{remote_name.lstrip('/')}"
                    else:
                        resolved_remote = remote_name
                        
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
                # Resolve remote path with context
                if path_prefix:
                    resolved_remote = f"{path_prefix.rstrip('/')}/{remote_name.lstrip('/')}"
                else:
                    resolved_remote = remote_name
                    
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
def list_files(sim_id: str, location: str = None, content_type: str = None):
    """List files associated with a simulation."""
    try:
        service = _get_simulation_service()
        sim = service.get_simulation(sim_id)
        
        # For now, provide basic information about what would be shown
        table = Table(title=f"Files for Simulation: {sim_id}")
        table.add_column("Path", style="cyan")
        table.add_column("Size", style="green")
        table.add_column("Type", style="yellow")
        table.add_column("Location", style="blue")
        table.add_column("Last Modified", style="magenta")
        
        # Show example/placeholder files that would be discovered
        # This would normally come from the file tracking service
        console.print(f"[dim]Simulation locations: {', '.join(sim.associated_locations) if sim.associated_locations else 'none'}[/dim]")
        
        if location:
            console.print(f"[dim]Filtering by location: {location}[/dim]")
        if content_type:
            console.print(f"[dim]Filtering by content type: {content_type}[/dim]")
        
        # Show placeholder message
        console.print(Panel.fit(table))
        console.print("[yellow]Note:[/yellow] File discovery and tracking is not yet fully implemented.")
        console.print("This command would show:")
        console.print("• All files associated with this simulation")
        console.print("• Files discovered in associated locations") 
        console.print("• File metadata (size, type, modification time)")
        console.print("• Content type classification (model output, logs, etc.)")
        
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