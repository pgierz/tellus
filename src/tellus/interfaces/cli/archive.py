"""Clean architecture CLI for archive management."""

from pathlib import Path
from typing import Optional

import rich_click as click
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from ...application.container import get_service_container
from ...application.dtos import (ArchiveCopyOperationDto, ArchiveExtractionDto,
                                 ArchiveOperationDto, CreateArchiveDto)
from .main import cli, console


def _get_archive_service():
    """Get archive service from the service container."""
    service_container = get_service_container()
    return service_container.service_factory.archive_service


@cli.group()
def archive():
    """Manage archives using clean architecture."""
    pass


@archive.command(name="list")
def list_archives():
    """List all archives."""
    try:
        service = _get_archive_service()
        result = service.list_archives()
        archives = result.archives
        
        if not archives:
            console.print("No archives found.")
            return
            
        table = Table(
            title="Available Archives", show_header=True, header_style="bold magenta"
        )
        table.add_column("Archive ID", style="cyan")
        table.add_column("Location", style="green")
        table.add_column("Type", style="blue")
        table.add_column("Simulation", style="yellow")

        for archive in sorted(archives, key=lambda a: a.archive_id):
            archive_id = archive.archive_id
            location = archive.location
            archive_type = archive.archive_type
            simulation = archive.simulation_id or "-"
            table.add_row(archive_id, location, archive_type, simulation)

        console.print(Panel.fit(table))
        
    except Exception as e:
        console.print(f"[red]Error:[/red] {str(e)}")


@archive.command(name="create")
@click.argument("archive_id")
@click.argument("source_path")
@click.option("--simulation", help="Associated simulation ID")
@click.option("--type", "archive_type", default="compressed", help="Archive type (compressed, uncompressed)")
@click.option("--from-location", "from_location", help="Explicitly specify source location (overrides location:path format)")
def create_archive(archive_id: str, source_path: str, simulation: str = None, archive_type: str = "compressed", from_location: str = None):
    """Create a new archive from local or remote sources.
    
    Examples:
        tellus archive create my_archive.tgz /local/path                           # Auto-creates localhost
        tellus archive create my_archive.tgz location_name:/remote/path            # Remote location:path format
        tellus archive create my_archive.tgz /remote/path --from-location hsm      # Explicit --from-location
    """
    import asyncio
    import os

    # Determine location and path based on arguments
    if from_location:
        # --from-location specified - use it explicitly
        actual_location = from_location
        actual_source_path = source_path
        remote_mode = True
    elif ':' in source_path:
        # location:path format detected
        parsed_location, parsed_path = source_path.split(':', 1)
        actual_location = parsed_location
        actual_source_path = parsed_path
        remote_mode = True
    else:
        # No location specified - ensure localhost exists
        actual_source_path = source_path
        remote_mode = False
        
        # For localhost, check if path exists
        if not os.path.exists(source_path):
            console.print(f"[red]Error:[/red] Local source path '{source_path}' does not exist")
            return
        
        # Ensure localhost location exists
        from ...application.container import get_service_container
        service_container = get_service_container()
        location_service = service_container.service_factory.location_service
        
        try:
            actual_location = location_service.ensure_localhost_location()
        except Exception as e:
            console.print(f"[red]Error:[/red] Could not setup localhost location: {e}")
            return
    
    async def _create_archive():
        try:
            service = _get_archive_service()
            
            if remote_mode:
                console.print(f"🗜️  Creating archive [cyan]{archive_id}[/cyan] from [blue]{actual_location}:{actual_source_path}[/blue]...")
            else:
                console.print(f"🗜️  Creating archive [cyan]{archive_id}[/cyan] from [blue]{actual_location}:{actual_source_path}[/blue]...")
            
            dto = CreateArchiveDto(
                archive_id=archive_id,
                location_name=actual_location,
                archive_type=archive_type,
                source_path=actual_source_path,
                simulation_id=simulation
            )
            
            result = await service.create_archive(dto)
            
            if result.success:
                console.print(f"[green]✓[/green] Created archive: [cyan]{result.archive_id}[/cyan]")
                if result.destination_path:
                    console.print(f"   Archive stored at: [dim]{result.destination_path}[/dim]")
                console.print(f"   Files processed: [yellow]{result.files_processed}[/yellow]")
                console.print(f"   Bytes processed: [yellow]{result.bytes_processed:,}[/yellow] bytes")
            else:
                console.print(f"[red]✗[/red] Failed to create archive")
                
        except Exception as e:
            console.print(f"[red]Error:[/red] {str(e)}")
    
    try:
        # Run the async function
        asyncio.run(_create_archive())
    except KeyboardInterrupt:
        console.print("\n[yellow]Archive creation cancelled by user[/yellow]")


@archive.command(name="show")
@click.argument("archive_id", required=False)
def show_archive(archive_id: str = None):
    """Show details for an archive.
    
    If no archive_id is provided, launches an interactive archive selection.
    """
    try:
        service = _get_archive_service()
        
        # If no archive_id provided, launch interactive selection
        if not archive_id:
            import questionary

            # Get all archives for selection
            archives_result = service.list_archives()
            if not archives_result.archives:
                console.print("[yellow]No archives found[/yellow]")
                return
                
            archive_choices = [f"{archive.archive_id} (location: {archive.location})" 
                             for archive in archives_result.archives]
            
            selected = questionary.select(
                "Select archive to show details:",
                choices=archive_choices,
                style=questionary.Style([
                    ('question', 'bold'),
                    ('selected', 'fg:#cc5454'),
                    ('pointer', 'fg:#ff0066 bold'),
                ])
            ).ask()
            
            if not selected:
                console.print("[yellow]No archive selected[/yellow]")
                return
                
            # Extract archive_id from selection
            archive_id = selected.split(" (location:")[0]
        
        archive = service.get_archive_metadata(archive_id)
        
        table = Table(title=f"Archive: {archive_id}")
        table.add_column("Property", style="cyan")
        table.add_column("Value", style="green")
        
        table.add_row("Archive ID", archive.archive_id)
        table.add_row("Location", archive.location)
        table.add_row("Type", archive.archive_type)
        table.add_row("Simulation", archive.simulation_id or "-")
        if hasattr(archive, 'archive_path') and archive.archive_path:
            table.add_row("Archive Path", archive.archive_path)
        
        # Additional metadata if available
        if hasattr(archive, 'size') and archive.size:
            size_mb = archive.size / (1024 * 1024)
            table.add_row("Size", f"{size_mb:.1f} MB")
        if hasattr(archive, 'created_time') and archive.created_time:
            from datetime import datetime
            created_date = datetime.fromtimestamp(archive.created_time).strftime("%Y-%m-%d %H:%M")
            table.add_row("Created", created_date)
        if hasattr(archive, 'description') and archive.description:
            table.add_row("Description", archive.description)
        if hasattr(archive, 'version') and archive.version:
            table.add_row("Version", archive.version)
        if hasattr(archive, 'tags') and archive.tags:
            table.add_row("Tags", ", ".join(archive.tags))
        
        console.print(Panel.fit(table))
        
    except Exception as e:
        console.print(f"[red]Error:[/red] {str(e)}")


@archive.command(name="files")
@click.argument("archive_id", required=False)
@click.option("--content-type", help="Filter by content type")
@click.option("--pattern", help="Filter by filename pattern")
def list_files(archive_id: str = None, content_type: str = None, pattern: str = None):
    """List files in an archive.
    
    If no archive_id is provided, launches an interactive archive selection.
    """
    try:
        service = _get_archive_service()
        
        # If no archive_id provided, launch interactive selection
        if not archive_id:
            import questionary

            # Get all archives for selection
            archives_result = service.list_archives()
            if not archives_result.archives:
                console.print("[yellow]No archives found[/yellow]")
                return
                
            archive_choices = [f"{archive.archive_id} (location: {archive.location})" 
                             for archive in archives_result.archives]
            
            selected = questionary.select(
                "Select archive to list files:",
                choices=archive_choices,
                style=questionary.Style([
                    ('question', 'bold'),
                    ('selected', 'fg:#cc5454'),
                    ('pointer', 'fg:#ff0066 bold'),
                ])
            ).ask()
            
            if not selected:
                console.print("[yellow]No archive selected[/yellow]")
                return
                
            # Extract archive_id from selection
            archive_id = selected.split(" (location:")[0]
        
        file_list_result = service.list_archive_files(
            archive_id=archive_id,
            content_type=content_type,
            pattern=pattern
        )
        
        if not file_list_result.files:
            console.print("No files found in archive.")
            return
            
        table = Table(
            title=f"Files in Archive: {archive_id}", 
            show_header=True, 
            header_style="bold magenta"
        )
        table.add_column("Path", style="cyan")
        table.add_column("Size", style="green")
        table.add_column("Type", style="blue")
        table.add_column("Role", style="yellow")

        for file in file_list_result.files:
            size_str = f"{file.size}" if file.size else "-"
            table.add_row(
                file.relative_path, 
                size_str, 
                file.content_type if file.content_type else "-",
                file.file_role or "-"
            )

        console.print(Panel.fit(table))
        
    except Exception as e:
        console.print(f"[red]Error:[/red] {str(e)}")


@archive.command(name="copy")
@click.argument("archive_id")
@click.argument("source_location")
@click.argument("destination_location")
@click.option("--simulation", help="Associated simulation ID")
def copy_archive(archive_id: str, source_location: str, destination_location: str, simulation: str = None):
    """Copy an archive between locations."""
    try:
        service = _get_archive_service()
        
        dto = ArchiveCopyOperationDto(
            archive_id=archive_id,
            source_location=source_location,
            destination_location=destination_location,
            simulation_id=simulation
        )
        
        result = service.copy_archive_to_location(dto)
        console.print(f"[green]✓[/green] Archive copy initiated: {result.operation_id}")
        
    except Exception as e:
        console.print(f"[red]Error:[/red] {str(e)}")


@archive.command(name="extract")
@click.argument("archive_id") 
@click.argument("destination_location")
@click.option("--simulation", help="Associated simulation ID")
@click.option("--content-type", help="Filter by content type")
def extract_archive(archive_id: str, destination_location: str, simulation: str = None, content_type: str = None):
    """Extract an archive to a location."""
    try:
        service = _get_archive_service()
        
        dto = ArchiveExtractionDto(
            archive_id=archive_id,
            destination_location=destination_location,
            simulation_id=simulation,
            content_type_filter=content_type
        )
        
        result = service.extract_archive_to_location(dto)
        if result.success:
            console.print(f"[green]✓[/green] Extracted {result.files_processed} files to {result.destination_path}")
        else:
            console.print(f"[red]✗[/red] Extraction failed: {result.error_message}")
        
    except Exception as e:
        console.print(f"[red]Error:[/red] {str(e)}")


@archive.command(name="update")
@click.argument("archive_id", required=False)
@click.option("--simulation-date", help="Update simulation date")  
@click.option("--version", help="Update version")
@click.option("--description", help="Update description")
@click.option("--archive-path", help="Update archive path/filename in location")
@click.option("--path-prefix-to-strip", help="Set path prefix to strip from file paths when listing/extracting")
@click.option("--add-tag", multiple=True, help="Add tags to archive")
@click.option("--remove-tag", multiple=True, help="Remove tags from archive")
def update_archive(archive_id: str = None, simulation_date: str = None, version: str = None, 
                  description: str = None, archive_path: str = None, path_prefix_to_strip: str = None, add_tag: tuple = (), remove_tag: tuple = ()):
    """Update an existing archive's metadata.
    
    If no archive ID is provided, launches an interactive wizard to select and update an archive.
    """
    try:
        service = _get_archive_service()
        
        # If no archive_id provided, launch interactive wizard
        if not archive_id:
            import questionary

            # Get all archives for selection
            archives_result = service.list_archives()
            if not archives_result.archives:
                console.print("[yellow]No archives found[/yellow]")
                return
                
            archive_choices = [f"{archive.archive_id} (location: {archive.location})" 
                             for archive in archives_result.archives]
            
            selected = questionary.select(
                "Select archive to update:",
                choices=archive_choices,
                style=questionary.Style([
                    ('question', 'bold'),
                    ('selected', 'fg:#cc5454'),
                    ('pointer', 'fg:#ff0066 bold'),
                ])
            ).ask()
            
            if not selected:
                console.print("[yellow]No archive selected[/yellow]")
                return
                
            # Extract archive_id from selection
            archive_id = selected.split(" (location:")[0]
            
            # Get current archive info
            current_archive = None
            for archive in archives_result.archives:
                if archive.archive_id == archive_id:
                    current_archive = archive
                    break
                    
            if not current_archive:
                console.print(f"[red]Error:[/red] Archive '{archive_id}' not found")
                return
            
            # Interactive questionary for updates
            console.print(f"\n[bold]Current archive details:[/bold]")
            console.print(f"Archive ID: {current_archive.archive_id}")
            console.print(f"Location: {current_archive.location}")
            console.print(f"Archive Path: {current_archive.archive_path or 'Not set'}")
            console.print(f"Simulation Date: {current_archive.simulation_date or 'Not set'}")
            console.print(f"Version: {current_archive.version or 'Not set'}")
            console.print(f"Description: {current_archive.description or 'Not set'}")
            console.print(f"Tags: {', '.join(current_archive.tags) if current_archive.tags else 'None'}")
            
            # Ask what to update
            update_fields = questionary.checkbox(
                "What would you like to update?",
                choices=[
                    "Simulation Date",
                    "Version", 
                    "Description",
                    "Archive Path",
                    "Tags"
                ],
                style=questionary.Style([
                    ('question', 'bold'),
                    ('checkbox', 'fg:#ff0066'),
                    ('selected', 'fg:#cc5454'),
                    ('pointer', 'fg:#ff0066 bold'),
                ])
            ).ask()
            
            if not update_fields:
                console.print("[yellow]No fields selected for update[/yellow]")
                return
            
            # Collect new values
            if "Simulation Date" in update_fields:
                simulation_date = questionary.text(
                    "New simulation date:",
                    default=current_archive.simulation_date or ""
                ).ask()
                
            if "Version" in update_fields:
                version = questionary.text(
                    "New version:",
                    default=current_archive.version or ""
                ).ask()
                
            if "Description" in update_fields:
                description = questionary.text(
                    "New description:",
                    default=current_archive.description or ""
                ).ask()
                
            if "Archive Path" in update_fields:
                # Get location service and filesystem for tab completion
                from ...application.container import get_service_container
                from .completion import SmartPathCompleter
                
                try:
                    service_container = get_service_container()
                    location_service = service_container.service_factory.location_service
                    filesystem_wrapper = location_service.get_location_filesystem(current_archive.location)
                    completer = SmartPathCompleter(filesystem_wrapper, only_directories=False)
                    
                    archive_path = questionary.text(
                        f"Archive path on {current_archive.location}:",
                        default=current_archive.archive_path or "",
                        completer=completer
                    ).ask()
                except Exception as e:
                    # Fallback to simple text input if completion fails
                    console.print(f"[yellow]Warning:[/yellow] Tab completion unavailable: {e}")
                    archive_path = questionary.text(
                        f"Archive path on {current_archive.location}:",
                        default=current_archive.archive_path or ""
                    ).ask()
                
            if "Tags" in update_fields:
                current_tags_str = ", ".join(current_archive.tags) if current_archive.tags else ""
                new_tags_str = questionary.text(
                    "Tags (comma-separated):",
                    default=current_tags_str
                ).ask()
                
                if new_tags_str:
                    new_tags = {tag.strip() for tag in new_tags_str.split(",") if tag.strip()}
                else:
                    new_tags = set()
        
        # Apply command line arguments if no interactive mode
        else:
            # For command line mode, handle tags differently
            if add_tag or remove_tag:
                # Get current archive to modify tags
                archives_result = service.list_archives()
                current_archive = None
                for archive in archives_result.archives:
                    if archive.archive_id == archive_id:
                        current_archive = archive
                        break
                        
                if current_archive:
                    new_tags = set(current_archive.tags) if current_archive.tags else set()
                    new_tags.update(add_tag)
                    new_tags.difference_update(remove_tag)
                else:
                    new_tags = set(add_tag) if add_tag else None
            else:
                new_tags = None
        
        # Create update DTO
        from ...application.dtos import UpdateArchiveDto
        
        update_dto = UpdateArchiveDto(
            simulation_date=simulation_date if simulation_date else None,
            version=version if version else None,
            description=description if description else None,
            archive_path=archive_path if archive_path else None,
            path_prefix_to_strip=path_prefix_to_strip if path_prefix_to_strip else None,
            tags=new_tags if new_tags is not None else None
        )
        
        # Call service method (this may not exist yet)
        try:
            result = service.update_archive(archive_id, update_dto)
            if result:
                console.print(f"[green]✓[/green] Successfully updated archive '{archive_id}'")
            else:
                console.print(f"[red]✗[/red] Failed to update archive '{archive_id}'")
        except AttributeError:
            console.print(f"[red]Error:[/red] Archive update service method not yet implemented")
        except Exception as e:
            console.print(f"[red]Error:[/red] {str(e)}")
        
    except Exception as e:
        console.print(f"[red]Error:[/red] {str(e)}")


@archive.command(name="delete")
@click.argument("archive_ids", nargs=-1)
@click.option("--force", is_flag=True, help="Skip confirmation prompts")
def delete_archives(archive_ids: tuple, force: bool = False):
    """Delete one or more archives.
    
    Examples:
        tellus archive delete                           # Interactive selection
        tellus archive delete archive1 archive2         # Delete specific archives
        tellus archive delete archive1 --force          # Skip confirmation
    """
    try:
        service = _get_archive_service()
        archives_to_delete = []
        
        # If no archive_ids provided, launch interactive selection
        if not archive_ids:
            import questionary

            # Get all archives for selection
            archives_result = service.list_archives()
            if not archives_result.archives:
                console.print("[yellow]No archives found[/yellow]")
                return
                
            archive_choices = [f"{archive.archive_id} (location: {archive.location})" 
                             for archive in archives_result.archives]
            
            selected = questionary.checkbox(
                "Select archives to delete:",
                choices=archive_choices,
                style=questionary.Style([
                    ('question', 'bold'),
                    ('checkbox', 'fg:#ff0066'),
                    ('selected', 'fg:#cc5454'),
                    ('pointer', 'fg:#ff0066 bold'),
                ])
            ).ask()
            
            if not selected:
                console.print("[yellow]No archives selected[/yellow]")
                return
                
            # Extract archive_ids from selections
            archives_to_delete = [selection.split(" (location:")[0] for selection in selected]
        else:
            archives_to_delete = list(archive_ids)
        
        if not archives_to_delete:
            console.print("[yellow]No archives to delete[/yellow]")
            return
            
        # Show what will be deleted
        console.print(f"\n[bold]Archives to delete:[/bold]")
        for archive_id in archives_to_delete:
            console.print(f"  • {archive_id}")
        
        # Confirmation prompt (unless --force used)
        if not force:
            import questionary
            
            confirm = questionary.confirm(
                f"Are you sure you want to delete {len(archives_to_delete)} archive(s)? This action cannot be undone.",
                default=False
            ).ask()
            
            if not confirm:
                console.print("[yellow]Deletion cancelled[/yellow]")
                return
        
        # Delete archives
        deleted_count = 0
        failed_count = 0
        
        for archive_id in archives_to_delete:
            try:
                success = service.delete_archive(archive_id)
                if success:
                    console.print(f"[green]✓[/green] Deleted archive: [cyan]{archive_id}[/cyan]")
                    deleted_count += 1
                else:
                    console.print(f"[red]✗[/red] Failed to delete archive: [cyan]{archive_id}[/cyan]")
                    failed_count += 1
            except Exception as e:
                console.print(f"[red]✗[/red] Error deleting [cyan]{archive_id}[/cyan]: {str(e)}")
                failed_count += 1
        
        # Summary
        console.print(f"\n[bold]Summary:[/bold]")
        console.print(f"  [green]Deleted:[/green] {deleted_count}")
        if failed_count > 0:
            console.print(f"  [red]Failed:[/red] {failed_count}")
        
    except Exception as e:
        console.print(f"[red]Error:[/red] {str(e)}")


@archive.command(name="add")
@click.argument("archive_id")
@click.argument("remote_path")
@click.option("--simulation", help="Associated simulation ID")
@click.option("--type", "archive_type", default="compressed", help="Archive type (compressed, uncompressed)")
@click.option("--description", help="Archive description")
def add_archive(archive_id: str, remote_path: str, simulation: str = None, archive_type: str = "compressed", description: str = None):
    """Register an existing remote archive.
    
    This command registers metadata for an archive that already exists on a remote location
    without attempting to access or create the archive file.
    
    Examples:
        tellus archive add my_archive.tgz tellus_hsm:/path/to/archive.tgz --simulation my-sim
        tellus archive add data_archive.tar.gz remote_storage:/archives/data.tar.gz --description "Climate data archive"
    """
    try:
        # Parse remote_path to extract location and path
        if ':' not in remote_path:
            console.print("[red]Error:[/red] Remote path must be in format location:path")
            console.print("Example: tellus_hsm:/path/to/archive.tgz")
            return
            
        location_name, archive_path = remote_path.split(':', 1)
        
        # Display what will be registered
        console.print(f"📝  Registering archive [cyan]{archive_id}[/cyan] at [blue]{remote_path}[/blue]...")
        
        service = _get_archive_service()
        
        # Create DTO for metadata-only archive registration
        from ...application.dtos import CreateArchiveDto
        
        dto = CreateArchiveDto(
            archive_id=archive_id,
            location_name=location_name,
            archive_type=archive_type,
            source_path=None,  # No source path for metadata-only
            archive_path=archive_path,  # Store the actual filename from remote_path
            simulation_id=simulation,
            description=description
        )
        
        # Use the metadata-only creation method
        result = service.create_archive_metadata(dto)
        
        console.print(f"[green]✓[/green] Successfully registered archive: [cyan]{archive_id}[/cyan]")
        console.print(f"   Location: [dim]{location_name}[/dim]")
        console.print(f"   Type: [dim]{archive_type}[/dim]")
        if simulation:
            console.print(f"   Simulation: [dim]{simulation}[/dim]")
        if description:
            console.print(f"   Description: [dim]{description}[/dim]")
            
    except Exception as e:
        console.print(f"[red]Error:[/red] {str(e)}")


@archive.command(name="edit")
@click.argument("archive_id", required=False)
@click.option("--dry-run", is_flag=True, help="Show metadata JSON without opening editor")
def edit_archive(archive_id: str = None, dry_run: bool = False):
    """Edit archive metadata in vim.
    
    Opens the archive metadata in vim for direct editing. Supports all metadata fields
    including description, version, tags, simulation_id, and archive_path.
    
    If no archive_id is provided, launches an interactive archive selection.
    
    Examples:
        tellus archive edit my-archive          # Edit specific archive
        tellus archive edit                     # Interactive selection
    """
    import json
    import os
    import subprocess
    import tempfile
    from pathlib import Path
    
    try:
        service = _get_archive_service()
        
        # If no archive_id provided, launch interactive selection
        if not archive_id:
            import questionary

            # Get all archives for selection
            archives_result = service.list_archives()
            if not archives_result.archives:
                console.print("[yellow]No archives found[/yellow]")
                return

            archive_choices = [
                f"{archive.archive_id} ({archive.location}) - {archive.description or 'No description'}"
                for archive in archives_result.archives
            ]

            selected = questionary.select(
                "Select archive to edit:",
                choices=archive_choices,
                style=questionary.Style([
                    ('question', 'bold'),
                    ('selected', 'fg:#cc5454'),
                    ('pointer', 'fg:#ff0066 bold'),
                ])
            ).ask()

            if not selected:
                console.print("[yellow]No archive selected[/yellow]")
                return

            # Extract archive_id from selection
            archive_id = selected.split(' ')[0]
        
        # Get current archive metadata
        try:
            metadata_result = service.get_archive_metadata(archive_id)
        except Exception as e:
            console.print(f"[red]Error:[/red] Archive '{archive_id}' not found: {str(e)}")
            return
        
        # Convert metadata to editable format
        editable_data = {
            "archive_id": metadata_result.archive_id,
            "location": metadata_result.location,
            "archive_type": metadata_result.archive_type,
            "simulation_id": metadata_result.simulation_id,
            "archive_path": metadata_result.archive_path,
            "description": metadata_result.description,
            "version": metadata_result.version,
            "tags": list(metadata_result.tags) if metadata_result.tags else [],
            "simulation_date": metadata_result.simulation_date,
            "path_prefix_to_strip": metadata_result.path_prefix_to_strip,
            # Read-only fields for reference
            "_readonly": {
                "created_time": metadata_result.created_time,
                "size": metadata_result.size
            }
        }
        
        # Handle dry-run mode
        if dry_run:
            console.print(f"[dim]Archive metadata for '{archive_id}' (editable format):[/dim]\n")
            console.print(json.dumps(editable_data, indent=2, default=str))
            return
        
        # Create temporary file with JSON content
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as tmp_file:
            json.dump(editable_data, tmp_file, indent=2, default=str)
            tmp_file_path = tmp_file.name
        
        try:
            # Get editor from environment or default to vim
            editor = os.environ.get('EDITOR', 'vim')
            
            console.print(f"[dim]Opening {archive_id} metadata in {editor}...[/dim]")
            console.print("[dim]Edit the metadata and save/quit to apply changes[/dim]")
            
            # Open editor
            result = subprocess.run([editor, tmp_file_path])
            
            if result.returncode != 0:
                console.print(f"[yellow]Editor exited with code {result.returncode}, changes not saved[/yellow]")
                return
            
            # Read back the modified content
            with open(tmp_file_path, 'r') as tmp_file:
                try:
                    modified_data = json.load(tmp_file)
                except json.JSONDecodeError as e:
                    console.print(f"[red]Error:[/red] Invalid JSON format: {str(e)}")
                    console.print("[yellow]Changes not saved[/yellow]")
                    return
            
            # Validate that required fields haven't been removed
            required_fields = ["archive_id", "location", "archive_type"]
            missing_fields = [field for field in required_fields if field not in modified_data]
            if missing_fields:
                console.print(f"[red]Error:[/red] Required fields missing: {', '.join(missing_fields)}")
                console.print("[yellow]Changes not saved[/yellow]")
                return
            
            # Check if archive_id was changed (not allowed)
            if modified_data["archive_id"] != metadata_result.archive_id:
                console.print("[red]Error:[/red] Archive ID cannot be changed")
                console.print("[yellow]Changes not saved[/yellow]")
                return
                
            # Check if location was changed (not allowed)
            if modified_data["location"] != metadata_result.location:
                console.print("[red]Error:[/red] Location cannot be changed")
                console.print("[yellow]Changes not saved[/yellow]")
                return
            
            # Apply updates using the update service
            from ...application.dtos import UpdateArchiveDto
            
            update_dto = UpdateArchiveDto(
                simulation_id=modified_data.get("simulation_id"),
                simulation_date=modified_data.get("simulation_date"),
                version=modified_data.get("version"),
                description=modified_data.get("description"),
                archive_path=modified_data.get("archive_path"),
                path_prefix_to_strip=modified_data.get("path_prefix_to_strip"),
                tags=set(modified_data.get("tags", [])) if modified_data.get("tags") else None
            )
            
            # Update the archive
            service.update_archive(archive_id, update_dto)
            
            console.print(f"[green]✓[/green] Successfully updated archive: [cyan]{archive_id}[/cyan]")
            
        finally:
            # Clean up temporary file
            try:
                os.unlink(tmp_file_path)
            except:
                pass
                
    except KeyboardInterrupt:
        console.print("\n[yellow]Edit cancelled by user[/yellow]")
    except Exception as e:
        console.print(f"[red]Error:[/red] {str(e)}")