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
@click.argument("source_path", type=click.Path(exists=True))
@click.option("--location", required=True, help="Storage location name")
@click.option("--simulation", help="Associated simulation ID")
@click.option("--type", "archive_type", default="compressed", help="Archive type (compressed, uncompressed)")
def create_archive(archive_id: str, source_path: str, location: str, simulation: str = None, archive_type: str = "compressed"):
    """Create a new archive."""
    import asyncio
    
    async def _create_archive():
        try:
            service = _get_archive_service()
            
            console.print(f"🗜️  Creating archive [cyan]{archive_id}[/cyan] from [blue]{source_path}[/blue]...")
            
            dto = CreateArchiveDto(
                archive_id=archive_id,
                location_name=location,
                archive_type=archive_type,
                source_path=source_path,
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
@click.argument("archive_id")
def show_archive(archive_id: str):
    """Show details for an archive."""
    try:
        service = _get_archive_service()
        archive = service.get_archive(archive_id)
        
        table = Table(title=f"Archive: {archive_id}")
        table.add_column("Property", style="cyan")
        table.add_column("Value", style="green")
        
        table.add_row("Archive ID", archive.archive_id)
        table.add_row("Location", archive.location)
        table.add_row("Type", archive.archive_type)
        table.add_row("Simulation", archive.simulation_id or "-")
        
        console.print(Panel.fit(table))
        
    except Exception as e:
        console.print(f"[red]Error:[/red] {str(e)}")


@archive.command(name="files")
@click.argument("archive_id")
@click.option("--content-type", help="Filter by content type")
@click.option("--pattern", help="Filter by filename pattern")
def list_files(archive_id: str, content_type: str = None, pattern: str = None):
    """List files in an archive."""
    try:
        service = _get_archive_service()
        files = service.list_archive_files(
            archive_id=archive_id,
            content_type_filter=content_type,
            pattern_filter=pattern
        )
        
        if not files:
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

        for file in files:
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