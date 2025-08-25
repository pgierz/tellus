"""Clean architecture CLI for location management."""

import rich_click as click
from rich.console import Console
from rich.table import Table
from rich.panel import Panel

from .main import cli, console
from ...application.container import get_service_container
from ...application.dtos import CreateLocationDto
from ...domain.entities.location import LocationKind

def _complete_location_path(ctx, param, incomplete):
    """Shell completion for location path - provides tab completion based on the location being updated."""
    try:
        # Get the location name from the command arguments
        location_name = ctx.params.get('name')
        if not location_name:
            # Can't provide completion without knowing which location
            return []
        
        # Get filesystem access through the application service (clean architecture)
        service = _get_location_service()
        filesystem_wrapper = service.get_location_filesystem(location_name)
        
        # Use the SmartPathCompleter with the filesystem wrapper
        from .completion import SmartPathCompleter
        completer = SmartPathCompleter(filesystem_wrapper, only_directories=True)
        
        # Get completions
        completions = completer.get_completions(None, incomplete)
        return [completion.text for completion in completions]
        
    except Exception:
        # If completion fails, return empty list
        return []

def _get_location_service():
    """Get location service from the service container."""
    service_container = get_service_container()
    return service_container.service_factory.location_service


@cli.group()
def location():
    """Manage locations using clean architecture."""
    pass


@location.command(name="list")
def list_locations():
    """List all locations."""
    try:
        service = _get_location_service()
        result = service.list_locations()
        locations = result.locations
        
        if not locations:
            console.print("No locations found.")
            return
            
        table = Table(
            title="Available Locations", show_header=True, header_style="bold magenta"
        )
        table.add_column("Name", style="cyan")
        table.add_column("Kind", style="green")
        table.add_column("Protocol", style="blue")
        table.add_column("Host", style="yellow")

        for loc in sorted(locations, key=lambda l: l.name):
            kinds = ", ".join([k.lower() if isinstance(k, str) else k.name.lower() for k in loc.kinds]) if loc.kinds else "-"
            protocol = loc.protocol or "-"
            host = loc.storage_options.get("host", "-") if loc.storage_options else "-"
            table.add_row(loc.name, kinds, protocol, host)

        console.print(Panel.fit(table))
        
    except Exception as e:
        console.print(f"[red]Error:[/red] {str(e)}")


@location.command(name="delete")
@click.argument("names", nargs=-1)
@click.option("--force", "-f", is_flag=True, help="Skip confirmation prompt")
def delete_location(names: tuple = (), force: bool = False):
    """Delete one or more locations.
    
    Can specify multiple location names as arguments, or launch interactive wizard if none provided.
    
    Examples:
        tellus location delete loc1 loc2 loc3
        tellus location delete --force loc1 loc2
        tellus location delete  # Interactive wizard
    """
    try:
        service = _get_location_service()
        
        # If no names provided, launch interactive wizard
        if not names:
            console.print("[cyan]Delete locations...[/cyan]")
            
            # Get all locations
            result = service.list_locations()
            locations = result.locations
            
            if not locations:
                console.print("No locations found to delete.")
                return
            
            # Use questionary for multi-select
            import questionary
            
            choices = [f"{loc.name} ({loc.protocol})" for loc in sorted(locations, key=lambda l: l.name)]
            selected = questionary.checkbox(
                "Select locations to delete:",
                choices=choices
            ).ask()
            
            if not selected:
                console.print("[dim]No locations selected[/dim]")
                return
                
            # Extract location names from selections
            location_names = []
            for selection in selected:
                location_name = selection.split(' (')[0]  # Extract name before protocol
                location_names.append(location_name)
            
            console.print(f"\n[yellow]You selected {len(location_names)} location(s) for deletion:[/yellow]")
            for location_name in location_names:
                console.print(f"  • {location_name}")
                
        else:
            # Location names specified on command line
            location_names = list(names)
            
            # Check if all locations exist
            missing_locations = []
            for location_name in location_names:
                try:
                    service.get_location(location_name)
                except Exception:
                    missing_locations.append(location_name)
            
            if missing_locations:
                console.print(f"[red]Error:[/red] Locations not found: {', '.join(missing_locations)}")
                return
        
        # Confirmation prompt unless forced
        if not force:
            import questionary
            if len(location_names) == 1:
                question = f"Are you sure you want to delete location '{location_names[0]}'?"
            else:
                question = f"Are you sure you want to delete these {len(location_names)} locations?"
                
            if not questionary.confirm(question).ask():
                console.print("[dim]Operation cancelled[/dim]")
                return
        
        # Delete the locations
        deleted_count = 0
        errors = []
        
        for location_name in location_names:
            try:
                service.delete_location(location_name)
                console.print(f"[green]✓[/green] Deleted location: {location_name}")
                deleted_count += 1
            except Exception as e:
                errors.append(f"{location_name}: {str(e)}")
                console.print(f"[red]✗[/red] Failed to delete {location_name}: {str(e)}")
        
        # Summary
        if deleted_count > 0:
            console.print(f"\n[green]Successfully deleted {deleted_count} location(s)[/green]")
        if errors:
            console.print(f"[yellow]Failed to delete {len(errors)} location(s)[/yellow]")
        
    except Exception as e:
        console.print(f"[red]Error:[/red] {str(e)}")


@location.command(name="update")
@click.argument("name", required=False)
@click.option("--protocol", help="Update protocol (file, ssh, etc.)")
@click.option("--add-kind", multiple=True, type=click.Choice([k.name.lower() for k in LocationKind]), help="Add location kind")
@click.option("--remove-kind", multiple=True, type=click.Choice([k.name.lower() for k in LocationKind]), help="Remove location kind")
@click.option("--host", help="Update host (for remote locations)")
@click.option("--path", help="Update path on the location", shell_complete=_complete_location_path)
@click.option("--set-optional", is_flag=True, help="Mark location as optional")
@click.option("--set-required", is_flag=True, help="Mark location as required (not optional)")
def update_location(name: str = None, protocol: str = None, add_kind: tuple = (), remove_kind: tuple = (), 
                   host: str = None, path: str = None, set_optional: bool = False, set_required: bool = False):
    """Update an existing location.
    
    If no name is provided, launches an interactive wizard to select and update a location.
    """
    try:
        service = _get_location_service()
        
        # If no name provided, launch interactive wizard
        if not name:
            console.print("[cyan]Update a location...[/cyan]")
            
            # Get all locations
            result = service.list_locations()
            locations = result.locations
            
            if not locations:
                console.print("No locations found to update.")
                return
            
            # Use questionary to select location
            import questionary
            
            choices = [f"{loc.name} ({loc.protocol})" for loc in sorted(locations, key=lambda l: l.name)]
            selected = questionary.select(
                "Select location to update:",
                choices=choices
            ).ask()
            
            if not selected:
                console.print("[dim]No location selected[/dim]")
                return
                
            # Extract location name
            name = selected.split(' (')[0]
            
        # Get existing location
        try:
            existing_loc = service.get_location(name)
        except Exception:
            console.print(f"[red]Error:[/red] Location '{name}' not found")
            return
        
        # Show current values
        console.print(f"\n[dim]Current configuration for '{name}':[/dim]")
        console.print(f"  Protocol: {existing_loc.protocol}")
        console.print(f"  Kinds: {', '.join(existing_loc.kinds) if existing_loc.kinds else 'None'}")
        console.print(f"  Path: {existing_loc.path or 'None'}")
        if existing_loc.storage_options:
            for key, value in existing_loc.storage_options.items():
                console.print(f"  Storage.{key}: {value}")
        console.print(f"  Optional: {'Yes' if existing_loc.optional else 'No'}")
        
        # Interactive updates if no options provided
        if not any([protocol, add_kind, remove_kind, host, path, set_optional, set_required]):
            import questionary
            
            console.print("\n[cyan]Select what you'd like to update:[/cyan]")
            
            # Ask what to update
            update_options = questionary.checkbox(
                "What would you like to update?",
                choices=[
                    "Protocol",
                    "Kinds",
                    "Path", 
                    "Host (for remote protocols)",
                    "Optional flag"
                ]
            ).ask()
            
            if not update_options:
                console.print("[dim]No updates selected[/dim]")
                return
            
            # Collect updates based on selections
            if "Protocol" in update_options:
                protocol = questionary.select(
                    "Select new protocol:",
                    choices=[
                        questionary.Choice(title="file (local filesystem)", value="file"),
                        questionary.Choice(title="ssh (SSH/SFTP)", value="ssh"),
                        questionary.Choice(title="sftp (SFTP)", value="sftp"),
                        questionary.Choice(title="s3 (Amazon S3)", value="s3"),
                        questionary.Choice(title="scoutfs (ScoutFS with tape staging)", value="scoutfs")
                    ],
                    default=existing_loc.protocol
                ).ask()
                
            if "Kinds" in update_options:
                available_kinds = [k.name.lower() for k in LocationKind]
                current_kinds = [k.name.lower() if hasattr(k, 'name') else str(k).lower() for k in (existing_loc.kinds or [])]
                
                selected_kinds = questionary.checkbox(
                    "Select kinds for this location:",
                    choices=available_kinds,
                    default=current_kinds
                ).ask()
                
                # Convert to add/remove operations
                current_set = set(current_kinds)
                new_set = set(selected_kinds)
                add_kind = tuple(new_set - current_set)
                remove_kind = tuple(current_set - new_set)
                
            if "Path" in update_options:
                # Use smart tab completion with the existing location
                from ...interfaces.cli.completion import SmartPathCompleter
                
                try:
                    # Get filesystem access through the application service (clean architecture)
                    filesystem_wrapper = service.get_location_filesystem(existing_loc.name)
                    completer = SmartPathCompleter(filesystem_wrapper, only_directories=True)
                    
                    path = questionary.text(
                        "Enter new path:",
                        default=existing_loc.path or "",
                        completer=completer
                    ).ask()
                except Exception:
                    # Fallback to simple path input if completer fails
                    path = questionary.text(
                        "Enter new path:",
                        default=existing_loc.path or ""
                    ).ask()
                
            if "Host (for remote protocols)" in update_options:
                current_host = existing_loc.storage_options.get('host', '') if existing_loc.storage_options else ''
                host = questionary.text(
                    "Enter host:",
                    default=current_host
                ).ask()
                
            if "Optional flag" in update_options:
                set_optional = questionary.confirm(
                    "Mark as optional?",
                    default=existing_loc.optional
                ).ask()
                if not set_optional:
                    set_required = True
        
        # Prepare update data
        updates = {}
        
        if protocol and protocol != existing_loc.protocol:
            updates['protocol'] = protocol
            
        if path and path != existing_loc.path:
            updates['path'] = path
            
        if host:
            if 'storage_options' not in updates:
                updates['storage_options'] = existing_loc.storage_options or {}
            updates['storage_options']['host'] = host
        
        # Handle kinds
        current_kinds = set(k.name.lower() if hasattr(k, 'name') else str(k).lower() for k in (existing_loc.kinds or []))
        
        if add_kind:
            current_kinds.update(k.upper() for k in add_kind)
            
        if remove_kind:
            current_kinds.difference_update(k.upper() for k in remove_kind)
            
        if add_kind or remove_kind:
            updates['kinds'] = list(current_kinds)
        
        # Handle optional flag
        if set_optional and set_required:
            console.print("[red]Error:[/red] Cannot set both --set-optional and --set-required")
            return
        elif set_optional and not existing_loc.optional:
            updates['optional'] = True
        elif set_required and existing_loc.optional:
            updates['optional'] = False
        
        if not updates:
            console.print("[yellow]No changes to make.[/yellow]")
            return
        
        # Show what will change
        console.print(f"\n[dim]Updating location '{name}':[/dim]")
        for key, value in updates.items():
            console.print(f"  {key} → {value}")
        
        # Perform update
        from ...application.dtos import UpdateLocationDto
        update_dto = UpdateLocationDto(**updates)
        result = service.update_location(name, update_dto)
        console.print(f"\n[green]✓[/green] Updated location: {result.name}")
        
        # Show what was updated
        changes = []
        if protocol:
            changes.append(f"protocol → {protocol}")
        if path:
            changes.append(f"path → {path}")
        if host:
            changes.append(f"host → {host}")
        if add_kind:
            changes.append(f"added kinds: {', '.join(add_kind)}")
        if remove_kind:
            changes.append(f"removed kinds: {', '.join(remove_kind)}")
        if set_optional:
            changes.append("marked as optional")
        if set_required:
            changes.append("marked as required")
            
        if changes:
            console.print(f"[dim]Changes: {'; '.join(changes)}[/dim]")
        
    except Exception as e:
        console.print(f"[red]Error:[/red] {str(e)}")


@location.command(name="create")
@click.argument("name", required=False)
@click.option("--protocol", help="Protocol (file, ssh, etc.)")
@click.option("--kind", multiple=True, type=click.Choice([k.name.lower() for k in LocationKind]), help="Location kind")
@click.option("--host", help="Host (for remote locations)")
@click.option("--path", help="Path on the location")
@click.option("--optional", is_flag=True, help="Mark location as optional")
def create_location(name: str = None, protocol: str = None, kind: tuple = (), host: str = None, path: str = None, optional: bool = False):
    """Create a new location.
    
    If no name is provided, launches an interactive wizard to gather all required information.
    """
    try:
        service = _get_location_service()
        
        # If no name provided, launch interactive wizard
        if not name:
            console.print("[cyan]Creating a new location...[/cyan]")
            
            import questionary
            
            # Get name
            name = questionary.text(
                "Location name:",
                validate=lambda text: True if text.strip() else "Location name is required"
            ).ask()
            
            if not name:
                console.print("[dim]Operation cancelled[/dim]")
                return
            
            # Get protocol
            if not protocol:
                protocol = questionary.select(
                    "Select protocol:",
                    choices=[
                        questionary.Choice(title="file (local filesystem)", value="file"),
                        questionary.Choice(title="ssh (SSH/SFTP)", value="ssh"),
                        questionary.Choice(title="sftp (SFTP)", value="sftp"),
                        questionary.Choice(title="s3 (Amazon S3)", value="s3"),
                        questionary.Choice(title="scoutfs (ScoutFS with tape staging)", value="scoutfs")
                    ],
                    default="file"
                ).ask()
            
            # Get kinds
            if not kind:
                available_kinds = [k.name.lower() for k in LocationKind]
                selected_kinds = questionary.checkbox(
                    "Select location kinds (at least one required):",
                    choices=available_kinds,
                    validate=lambda x: True if x else "At least one location kind is required"
                ).ask()
                
                if selected_kinds:
                    kind = tuple(selected_kinds)
                else:
                    console.print("[red]Error:[/red] At least one location kind is required")
                    return
            
            # Get host (for remote protocols)
            if protocol in ['ssh', 'sftp', 's3', 'scoutfs'] and not host:
                host = questionary.text(
                    f"Host (for {protocol} protocol):",
                    validate=lambda text: True if text.strip() else f"Host is required for {protocol} protocol"
                ).ask()
                
                if not host:
                    console.print("[dim]Operation cancelled[/dim]")
                    return
            
            # Get ScoutFS-specific configuration
            scoutfs_api_url = None
            if protocol == 'scoutfs':
                use_custom_api = questionary.confirm(
                    "Use custom ScoutFS API URL?",
                    default=False
                ).ask()
                
                if use_custom_api:
                    scoutfs_api_url = questionary.text(
                        "ScoutFS API URL:",
                        default="https://hsm.dmawi.de:8080/v1"
                    ).ask()
            
            # Get path with smart tab completion  
            if not path:
                # For remote protocols, we'll use a simple text input since we don't have the location created yet
                # The SmartPathCompleter needs an existing Location object
                if protocol in ['ssh', 'sftp', 'scoutfs', 's3']:
                    path = questionary.text(
                        "Path on the location:",
                        default="/"
                    ).ask()
                else:
                    # For local filesystem, use path completion
                    path = questionary.path(
                        "Path on the location:",
                        only_directories=True
                    ).ask()
                
                if path is None:
                    path = ""
            
            # Optional flag
            if not optional:
                optional = questionary.confirm(
                    "Is this location optional?",
                    default=False
                ).ask()
            
            console.print(f"\n[dim]Creating location '{name}' with protocol '{protocol}'...[/dim]")
        else:
            # Initialize scoutfs_api_url for non-interactive usage
            scoutfs_api_url = None
        
        # Validate required fields
        if not protocol:
            console.print("[red]Error:[/red] Protocol is required")
            return
        
        # Prepare storage options
        storage_options = {}
        if host:
            storage_options["host"] = host
        
        # Handle protocol-specific options
        if protocol in ['ssh', 'sftp']:
            # Could add more SSH-specific options here
            pass
        elif protocol == 's3':
            # Could add S3-specific options here
            pass
        elif protocol == 'scoutfs':
            # Add ScoutFS-specific configuration
            if scoutfs_api_url:
                storage_options["scoutfs_config"] = {
                    "api_url": scoutfs_api_url
                }
            
        kinds_str = [k.upper() for k in kind] if kind else []
        
        dto = CreateLocationDto(
            name=name,
            kinds=kinds_str,
            protocol=protocol,
            path=path,
            storage_options=storage_options,
            optional=optional
        )
        
        result = service.create_location(dto)
        console.print(f"[green]✓[/green] Created location: {result.name}")
        
        # Show summary
        table = Table(title=f"Created Location: {name}")
        table.add_column("Property", style="cyan")
        table.add_column("Value", style="green")
        
        table.add_row("Name", result.name)
        table.add_row("Protocol", result.protocol)
        table.add_row("Kinds", ", ".join(result.kinds) if result.kinds else "-")
        table.add_row("Path", result.path or "-")
        if result.storage_options:
            for key, value in result.storage_options.items():
                table.add_row(f"Storage.{key}", str(value))
        table.add_row("Optional", "Yes" if result.optional else "No")
        
        console.print(Panel.fit(table))
        
    except Exception as e:
        console.print(f"[red]Error:[/red] {str(e)}")


@location.command(name="show")
@click.argument("name", required=False)
def show_location(name: str = None):
    """Show details for a location.
    
    If no name is provided, launches an interactive wizard to select a location to show.
    """
    try:
        service = _get_location_service()
        
        # If no name provided, launch interactive wizard
        if not name:
            console.print("[cyan]Show location details...[/cyan]")
            
            # Get all locations
            result = service.list_locations()
            locations = result.locations
            
            if not locations:
                console.print("No locations found.")
                return
            
            # Use questionary to select location
            import questionary
            
            choices = [f"{loc.name} ({loc.protocol})" for loc in sorted(locations, key=lambda l: l.name)]
            selected = questionary.select(
                "Select location to show:",
                choices=choices
            ).ask()
            
            if not selected:
                console.print("[dim]No location selected[/dim]")
                return
                
            # Extract location name
            name = selected.split(' (')[0]
        
        loc = service.get_location(name)
        
        table = Table(title=f"Location: {name}")
        table.add_column("Property", style="cyan")
        table.add_column("Value", style="green")
        
        table.add_row("Name", loc.name)
        table.add_row("Kinds", ", ".join([k.lower() if isinstance(k, str) else k.name.lower() for k in loc.kinds]) if loc.kinds else "-")
        table.add_row("Protocol", loc.protocol or "-")
        table.add_row("Path", loc.path or "-")
        table.add_row("Optional", "Yes" if loc.optional else "No")
        
        if loc.storage_options:
            for key, value in loc.storage_options.items():
                table.add_row(f"Storage.{key}", str(value))
                
        if loc.additional_config:
            for key, value in loc.additional_config.items():
                table.add_row(f"Config.{key}", str(value))
        
        console.print(Panel.fit(table))
        
    except Exception as e:
        console.print(f"[red]Error:[/red] {str(e)}")


@location.command(name="test")
@click.argument("name", required=False)
@click.option("--timeout", "-t", default=10, help="Connection timeout in seconds")
@click.option("--all", "test_all", is_flag=True, help="Test all locations")
def test_location(name: str = None, timeout: int = 10, test_all: bool = False):
    """Test connection to one or more locations.
    
    If no name is provided, launches an interactive wizard to select locations to test.
    """
    try:
        service = _get_location_service()
        
        # Determine which locations to test
        locations_to_test = []
        
        if test_all:
            # Test all locations
            result = service.list_locations()
            locations_to_test = [loc.name for loc in result.locations]
            console.print(f"[cyan]Testing all {len(locations_to_test)} locations...[/cyan]")
            
        elif not name:
            # Interactive wizard
            console.print("[cyan]Test location connections...[/cyan]")
            
            # Get all locations
            result = service.list_locations()
            locations = result.locations
            
            if not locations:
                console.print("No locations found to test.")
                return
            
            # Use questionary for multi-select
            import questionary
            
            choices = [f"{loc.name} ({loc.protocol})" for loc in sorted(locations, key=lambda l: l.name)]
            choices.append("All locations")
            
            selected = questionary.checkbox(
                "Select locations to test:",
                choices=choices
            ).ask()
            
            if not selected:
                console.print("[dim]No locations selected[/dim]")
                return
            
            if "All locations" in selected:
                locations_to_test = [loc.name for loc in locations]
            else:
                # Extract location names from selections
                for selection in selected:
                    if selection != "All locations":
                        location_name = selection.split(' (')[0]  # Extract name before protocol
                        locations_to_test.append(location_name)
        else:
            # Single location specified
            locations_to_test = [name]
            
            # Check if location exists
            try:
                loc = service.get_location(name)
            except Exception:
                console.print(f"[red]Error:[/red] Location '{name}' not found")
                return
        
        # Test each location
        successful_tests = 0
        failed_tests = 0
        
        for location_name in locations_to_test:
            console.print(f"\n[dim]Testing connection to '{location_name}'...[/dim]")
            
            try:
                # Perform connection test
                result = service.test_location_connectivity(location_name, timeout_seconds=timeout)
                
                if result.success:
                    console.print(f"[green]✓[/green] Connection to '{location_name}' successful")
                    
                    if result.latency_ms is not None:
                        console.print(f"[dim]Response time: {result.latency_ms:.1f}ms[/dim]")
                        
                    if result.available_space is not None:
                        # Convert bytes to human readable
                        if result.available_space > 1024**3:
                            space_str = f"{result.available_space / (1024**3):.1f}GB"
                        elif result.available_space > 1024**2:
                            space_str = f"{result.available_space / (1024**2):.1f}MB"
                        else:
                            space_str = f"{result.available_space / 1024:.1f}KB"
                        console.print(f"[dim]Available space: {space_str}[/dim]")
                        
                    if result.protocol_specific_info:
                        for key, value in result.protocol_specific_info.items():
                            console.print(f"[dim]{key}: {value}[/dim]")
                    
                    successful_tests += 1
                else:
                    console.print(f"[red]✗[/red] Connection to '{location_name}' failed")
                    if result.error_message:
                        console.print(f"[red]Error:[/red] {result.error_message}")
                    failed_tests += 1
                    
            except Exception as e:
                console.print(f"[red]✗[/red] Connection test for '{location_name}' failed: {str(e)}")
                failed_tests += 1
        
        # Summary for multiple locations
        if len(locations_to_test) > 1:
            console.print(f"\n[dim]Test Summary:[/dim]")
            console.print(f"[green]✓ {successful_tests} successful[/green]")
            console.print(f"[red]✗ {failed_tests} failed[/red]")
            
    except Exception as e:
        console.print(f"[red]Error:[/red] {str(e)}")