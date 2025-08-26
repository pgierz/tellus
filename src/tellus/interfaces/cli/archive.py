"""Clean architecture CLI for archive management."""

import rich_click as click
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from pathlib import Path
from typing import Optional

from .main import cli, console
from ...application.container import get_service_container
from ...application.dtos import (
    CreateArchiveDto, 
    ArchiveOperationDto,
    ArchiveExtractionDto,
    ArchiveCopyOperationDto
)

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
def create_archive(archive_id: str, source_path: str, simulation: str = None, archive_type: str = "compressed"):
    """Create a new archive from location:path format.
    
    Examples:
        tellus archive create my_archive.tgz /local/path                    # Auto-creates localhost
        tellus archive create my_archive.tgz location_name:/remote/path     # Remote location
    """
    import asyncio
    import os
    
    # Parse source_path to detect location:path format
    if ':' in source_path:
        # Explicit location:path format
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
@click.option("--add-tag", multiple=True, help="Add tags to archive")
@click.option("--remove-tag", multiple=True, help="Remove tags from archive")
def update_archive(archive_id: str = None, simulation_date: str = None, version: str = None, 
                  description: str = None, add_tag: tuple = (), remove_tag: tuple = ()):
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