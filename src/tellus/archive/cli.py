#!/usr/bin/env python3
"""CLI commands for archive management in Tellus."""

from ..core.cli import cli, console

import rich_click as click
from pathlib import Path
from typing import Optional, List
import sys
from rich.panel import Panel
from rich.table import Table

# Import legacy classes for backward compatibility  
from ..simulation.simulation import (
    Simulation, CacheManager, CacheConfig, ArchiveRegistry, 
    CompressedArchive, CLIProgressCallback, PathMapping
)
from ..core.feature_flags import feature_flags, FeatureFlag
from ..core.service_container import get_service_container
from ..core.legacy_bridge import ArchiveBridge
from ..application.exceptions import (
    EntityNotFoundError, EntityAlreadyExistsError, 
    ValidationError, ApplicationError
)


# Helper functions
def _get_archive_bridge() -> Optional[ArchiveBridge]:
    """Get archive bridge if new architecture is enabled."""
    if feature_flags.is_enabled(FeatureFlag.USE_NEW_ARCHIVE_SERVICE):
        service_container = get_service_container()
        return ArchiveBridge(service_container.service_factory)
    return None


def get_simulation(sim_id: str) -> Simulation:
    """Get simulation by ID, exit if not found"""
    sim = Simulation.get_simulation(sim_id)
    if not sim:
        console.print(f"[red]Error:[/red] Simulation '{sim_id}' not found")
        sys.exit(1)
    return sim


def format_size(bytes_val: int) -> str:
    """Format bytes as human readable size"""
    if bytes_val is None:
        return "Unknown"
    
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_val < 1024.0:
            return f"{bytes_val:.1f} {unit}"
        bytes_val /= 1024.0
    return f"{bytes_val:.1f} PB"


def format_time_ago(timestamp: float) -> str:
    """Format timestamp as time ago string"""
    import time
    
    if timestamp is None:
        return "Never"
    
    diff = time.time() - timestamp
    
    if diff < 60:
        return "Just now"
    elif diff < 3600:
        return f"{int(diff // 60)} minutes ago"
    elif diff < 86400:
        return f"{int(diff // 3600)} hours ago"
    elif diff < 604800:
        return f"{int(diff // 86400)} days ago"
    else:
        return f"{int(diff // 604800)} weeks ago"


# Main archive CLI group
@click.group()
def archive():
    """Manage data archives"""
    pass


@archive.command()
@click.argument('archive_id')
@click.argument('archive_path')
@click.option('--simulation', help='Associate with simulation ID')
@click.option('--name', help='Human-friendly name for the archive')
@click.option('--location', help='Location name where the archive is stored')
@click.option('--tags', help='Comma-separated custom tags for the archive')
def create(archive_id: str, archive_path: str, simulation: Optional[str],
           name: Optional[str], location: Optional[str], tags: Optional[str]):
    """Create a new archive"""
    
    # Validate simulation if provided
    sim = None
    if simulation:
        sim = get_simulation(simulation)
    
    # Validate location if provided  
    location_obj = None
    if location:
        from ..location import Location
        location_obj = Location.get_location(location)
        if not location_obj:
            console.print(f"[red]Error:[/red] Location '{location}' not found")
            console.print("Available locations:")
            for loc in Location.list_locations():
                console.print(f"  - {loc.name} ({', '.join(k.name for k in loc.kinds)})")
            sys.exit(1)
    
    # For local archives, verify the file exists
    if not location and not Path(archive_path).exists():
        console.print(f"[red]Error:[/red] Archive file not found: {archive_path}")
        sys.exit(1)
    
    console.print(f"Creating archive [cyan]{archive_id}[/cyan]...")
    if location:
        console.print(f"  Location: {location} ({', '.join(k.name for k in location_obj.kinds)})")
    if simulation:
        console.print(f"  Associated with simulation: {simulation}")
    
    bridge = _get_archive_bridge()
    
    if bridge:
        # Use new architecture
        try:
            # Parse tags if provided
            tag_list = []
            if tags:
                tag_list = [tag.strip() for tag in tags.split(',')]
            
            result = bridge.create_archive_from_legacy_data(
                archive_id=archive_id,
                simulation_id=simulation or "",
                archive_path=archive_path,
                location_name=location,
                name=name,
                tags=tag_list
            )
            
            if result:
                console.print(f"[green]✓[/green] Archive located: {archive_path} ({format_size(result.get('size', 0))})")
                if location:
                    console.print(f"  Storage: {result.get('archive_type', 'unknown')} via location '{location}'")
                
                console.print(f"[green]✓[/green] Archive '[cyan]{name or archive_id}[/cyan]' created successfully")
                if feature_flags.is_enabled(FeatureFlag.USE_NEW_ARCHIVE_SERVICE):
                    console.print("[dim]✨ Using new archive service[/dim]")
            else:
                console.print(f"[red]Error:[/red] Archive '{archive_id}' already exists or creation failed")
                sys.exit(1)
                
        except ApplicationError as e:
            console.print(f"[red]Error:[/red] {str(e)}")
            sys.exit(1)
    else:
        # Use legacy architecture - this requires simulation context
        if not simulation:
            console.print("[red]Error:[/red] Simulation ID is required when using legacy archive system")
            sys.exit(1)
            
        try:
            # Set up progress callback
            progress = CLIProgressCallback(verbose=True)
            
            archive = CompressedArchive(
                archive_id=archive_id,
                archive_location=archive_path,
                location=location_obj
            )
            archive.add_progress_callback(progress)
            
            # Get or create archive registry for simulation
            if not hasattr(sim, '_archive_registry'):
                sim._archive_registry = ArchiveRegistry(simulation)
            
            archive_name = name or Path(archive_path).stem
            sim._archive_registry.add_archive(archive, archive_name)
            
            # Show results
            status = archive.status()
            file_count = status.get('file_count', 0)
            total_size = status.get('total_size', 0)
            tags_info = status.get('tags', {})
            
            console.print(f"✓ Archive located: {archive_path} ({format_size(status.get('size', 0))})")
            if location:
                protocol = status.get('storage_protocol', 'unknown')
                console.print(f"  Storage: {protocol} protocol via location '{location}'")
            console.print(f"✓ Found {file_count} files")
            
            if tags_info:
                tag_summary = ", ".join([f"{tag} ({count})" for tag, count in tags_info.items()])
                console.print(f"✓ Tagged files: {tag_summary}")
            
            console.print(f"✓ Archive '{archive_name}' created and added to simulation {simulation}")
            
            # Save simulation
            sim.save_simulations()
            
        except Exception as e:
            console.print(f"[red]Error creating archive:[/red] {e}")
            sys.exit(1)


@archive.command()
@click.option('--simulation', help='Filter by simulation ID')
@click.option('--verbose', is_flag=True, help='Show detailed information')
@click.option('--cached-only', is_flag=True, help='Only show cached archives')
def list(simulation: Optional[str], verbose: bool, cached_only: bool):
    """List all archives"""
    
    bridge = _get_archive_bridge()
    
    if bridge:
        # Use new architecture
        try:
            if simulation:
                archives_data = bridge.list_archives_for_simulation_legacy_format(simulation, cached_only)
            else:
                # List all archives - may need service enhancement for this
                archives_data = bridge.list_archives_for_simulation_legacy_format("", cached_only)
            
            if not archives_data:
                if cached_only:
                    console.print("No cached archives found")
                else:
                    console.print("No archives found")
                if feature_flags.is_enabled(FeatureFlag.USE_NEW_ARCHIVE_SERVICE):
                    console.print("[dim]✨ Using new archive service[/dim]")
                return
            
            # Create rich table for archives
            table = Table(title="Archives" if not simulation else f"Archives for simulation {simulation}")
            table.add_column("ID", style="cyan", no_wrap=True)
            table.add_column("Location", style="blue")
            table.add_column("Type", style="yellow")
            table.add_column("Size", justify="right", style="green")
            table.add_column("Cached", justify="center")
            
            if verbose:
                table.add_column("Description", style="dim")
                table.add_column("Tags", style="magenta")
            
            for archive_data in archives_data:
                size_str = format_size(archive_data.get('size', 0))
                cached = "✓" if archive_data.get('is_cached', False) else "✗"
                
                row = [
                    archive_data['archive_id'],
                    archive_data.get('location', 'Unknown'),
                    archive_data.get('archive_type', 'Unknown'),
                    size_str,
                    cached
                ]
                
                if verbose:
                    description = archive_data.get('description', '')
                    tags = ", ".join(archive_data.get('tags', [])) if archive_data.get('tags') else ''
                    row.extend([description, tags])
                
                table.add_row(*row)
            
            console.print(table)
            
            if feature_flags.is_enabled(FeatureFlag.USE_NEW_ARCHIVE_SERVICE):
                console.print("[dim]✨ Using new archive service[/dim]")
                
        except ApplicationError as e:
            console.print(f"[red]Error:[/red] {str(e)}")
            return
    else:
        # Use legacy architecture - requires simulation context
        if not simulation:
            console.print("[red]Error:[/red] Simulation ID is required when using legacy archive system")
            console.print("Available simulations:")
            for sim in Simulation.list_simulations():
                console.print(f"  - {sim['simulation_id']}")
            sys.exit(1)
            
        sim = get_simulation(simulation)
        
        if not hasattr(sim, '_archive_registry') or not sim._archive_registry.archives:
            console.print(f"No archives found for simulation {simulation}")
            return
        
        registry = sim._archive_registry
        archives = registry.archives
        
        if cached_only:
            archives = {name: archive for name, archive in archives.items() 
                       if archive.status().get('cached', False)}
        
        if not archives:
            if cached_only:
                console.print(f"No cached archives found for simulation {simulation}")
            else:
                console.print(f"No archives found for simulation {simulation}")
            return
        
        if verbose:
            console.print(f"Archives for simulation {simulation}:\n")
            
            for name, archive in archives.items():
                status = archive.status()
                console.print(f"📦 {name} ({archive.archive_id})")
                console.print(f"   Location: {status.get('location', 'Unknown')}")
                
                # Show location info if available
                if status.get('location_name'):
                    location_kinds = ", ".join(status.get('location_kinds', []))
                    protocol = status.get('storage_protocol', 'unknown')
                    console.print(f"   Storage: {status['location_name']} ({location_kinds}) - {protocol} protocol")
                elif status.get('storage_protocol'):
                    console.print(f"   Storage: {status['storage_protocol']} protocol")
                
                size_str = format_size(status.get('size', 0))
                file_count = status.get('file_count', 0)
                cached = "✓" if status.get('cached', False) else "✗"
                
                console.print(f"   Size: {size_str} | Files: {file_count} | Cached: {cached}")
                
                tags_info = status.get('tags', {})
                if tags_info:
                    tag_summary = ", ".join([f"{tag} ({count})" for tag, count in tags_info.items()])
                    console.print(f"   Tags: {tag_summary}")
                    
                if status.get('created'):
                    created_str = format_time_ago(status['created'])
                    console.print(f"   Created: {created_str}")
                
                console.print()
        else:
            console.print(f"Archives for simulation {simulation}:")
            for name, archive in archives.items():
                status = archive.status()
                size_str = format_size(status.get('size', 0))
                cached = "🗂️" if status.get('cached', False) else "📦"
                console.print(f"  {cached} {name} ({archive.archive_id}) - {size_str}")


@archive.command()
@click.argument('archive_id')
def show(archive_id: str):
    """Show detailed information about an archive"""
    
    bridge = _get_archive_bridge()
    
    if bridge:
        # Use new architecture
        try:
            archive_data = bridge.get_archive_legacy_format(archive_id)
            if not archive_data:
                console.print(f"[red]Error:[/red] Archive '{archive_id}' not found")
                sys.exit(1)
            
            # Create rich panel for archive details
            size_str = format_size(archive_data.get('size', 0))
            cached = "[green]Yes[/green]" if archive_data.get('is_cached', False) else "[red]No[/red]"
            
            content = f"""[cyan]Location:[/cyan] {archive_data.get('location', 'Unknown')}
[cyan]Type:[/cyan] {archive_data.get('archive_type', 'Unknown')}
[cyan]Size:[/cyan] {size_str}
[cyan]Cached:[/cyan] {cached}"""
            
            if archive_data.get('cache_path'):
                content += f"\n[cyan]Cache Path:[/cyan] {archive_data['cache_path']}"
            
            if archive_data.get('description'):
                content += f"\n[cyan]Description:[/cyan] {archive_data['description']}"
            
            if archive_data.get('tags'):
                tags_str = ", ".join(archive_data['tags'])
                content += f"\n[cyan]Tags:[/cyan] [magenta]{tags_str}[/magenta]"
            
            if archive_data.get('created'):
                created_str = format_time_ago(archive_data['created'])
                content += f"\n[cyan]Created:[/cyan] {created_str}"
            
            panel = Panel(content, title=f"Archive: [yellow]{archive_id}[/yellow]", border_style="blue")
            console.print(panel)
            
            if feature_flags.is_enabled(FeatureFlag.USE_NEW_ARCHIVE_SERVICE):
                console.print("[dim]✨ Using new archive service[/dim]")
                
        except ApplicationError as e:
            console.print(f"[red]Error:[/red] {str(e)}")
            sys.exit(1)
    else:
        # Legacy system doesn't have a direct archive lookup by ID
        # This would require searching through all simulations
        console.print("[red]Error:[/red] Archive lookup by ID not supported in legacy system")
        console.print("Please specify a simulation ID and use 'tellus archive list --simulation SIM_ID'")
        sys.exit(1)


@archive.command()
@click.argument('archive_id')
@click.option('--force', is_flag=True, help='Force deletion without confirmation')
def delete(archive_id: str, force: bool):
    """Delete an archive"""
    
    bridge = _get_archive_bridge()
    
    if bridge:
        # Use new architecture
        try:
            # Check if archive exists
            archive_data = bridge.get_archive_legacy_format(archive_id)
            if not archive_data:
                console.print(f"Archive '{archive_id}' not found", err=True)
                sys.exit(1)

            if not force and not click.confirm(
                f"Are you sure you want to delete archive '{archive_id}'?"
            ):
                console.print("Operation cancelled.")
                return

            success = bridge.delete_archive(archive_id)
            if success:
                console.print(f"✓ Deleted archive: {archive_id}")
                if feature_flags.is_enabled(FeatureFlag.USE_NEW_ARCHIVE_SERVICE):
                    console.print("✨ Using new archive service")
            else:
                console.print(f"[red]Error:[/red] Could not delete archive '{archive_id}'")
                sys.exit(1)
                
        except ApplicationError as e:
            console.print(f"[red]Error:[/red] {str(e)}")
            sys.exit(1)
    else:
        # Legacy system doesn't support direct archive deletion by ID
        console.print("[red]Error:[/red] Archive deletion by ID not supported in legacy system")
        console.print("Please use simulation-specific archive management commands")
        sys.exit(1)


@archive.command(name='files')
@click.argument('archive_id')
@click.option('--content-type', help='Filter by content type (input, output, config, log, etc.)')
@click.option('--pattern', help='Filter files by name pattern (glob syntax)')
@click.option('--limit', type=int, default=50, help='Maximum number of files to show')
def list_files(archive_id: str, content_type: Optional[str], pattern: Optional[str], limit: int):
    """List files in an archive"""
    
    bridge = _get_archive_bridge()
    
    if bridge:
        console.print(f"📋 Files in archive: [bold]{archive_id}[/bold]")
        if feature_flags.is_enabled(FeatureFlag.USE_NEW_ARCHIVE_SERVICE):
            console.print("✨ Using new archive service")
        
        try:
            # Get file list from archive
            files = bridge.list_archive_files(
                archive_id, 
                content_type=content_type,
                pattern=pattern,
                limit=limit
            )
            
            if not files:
                console.print(f"No files found in archive '{archive_id}'")
                return
            
            # Create rich table for files
            table = Table(title=f"Files in Archive: {archive_id}")
            table.add_column("Path", style="cyan", no_wrap=False)
            table.add_column("Type", style="yellow")
            table.add_column("Size", justify="right", style="green") 
            table.add_column("Role", style="blue")
            
            for file_info in files:
                size_str = format_size(file_info.get('size', 0))
                content_type_display = file_info.get('content_type', 'unknown')
                file_role = file_info.get('file_role', '')
                
                table.add_row(
                    file_info['relative_path'],
                    content_type_display,
                    size_str,
                    file_role or ''
                )
            
            console.print(table)
            console.print(f"[dim]Total files: {len(files)}[/dim]")
            
        except Exception as e:
            console.print(f"[red]Error:[/red] {e}")
            
    else:
        # Legacy archive file listing
        console.print(f"📋 Files in archive: [bold]{archive_id}[/bold]")
        
        # This would need to be implemented in legacy system
        console.print("[yellow]Info:[/yellow] Legacy archive file listing not implemented")


@archive.command(name='associate-files')
@click.argument('archive_id')
@click.argument('simulation_id')
@click.option('--content-type', help='Only associate files of this content type')
@click.option('--pattern', help='Only associate files matching this pattern')
@click.option('--dry-run', is_flag=True, help='Show what would be associated without doing it')
def associate_files(archive_id: str, simulation_id: str, content_type: Optional[str], 
                   pattern: Optional[str], dry_run: bool):
    """Associate archive files with a simulation"""
    
    bridge = _get_archive_bridge()
    
    if bridge:
        if feature_flags.is_enabled(FeatureFlag.USE_NEW_ARCHIVE_SERVICE):
            console.print("✨ Using new archive service")
        
        console.print(f"🔗 Associating files from archive [bold]{archive_id}[/bold] with simulation [bold]{simulation_id}[/bold]")
        
        if dry_run:
            console.print("[yellow]DRY RUN:[/yellow] No changes will be made")
            
        try:
            # Associate files using the bridge
            result = bridge.associate_files_with_simulation(
                archive_id=archive_id,
                simulation_id=simulation_id,
                content_type_filter=content_type,
                pattern_filter=pattern,
                dry_run=dry_run
            )
            
            if result.get('success', False):
                files_associated = result.get('files_associated', [])
                files_skipped = result.get('files_skipped', [])
                
                if files_associated:
                    console.print(f"[green]✓[/green] Associated {len(files_associated)} files with simulation")
                    if not dry_run:
                        for file_path in files_associated[:5]:  # Show first 5
                            console.print(f"  - {file_path}")
                        if len(files_associated) > 5:
                            console.print(f"  ... and {len(files_associated) - 5} more")
                
                if files_skipped:
                    console.print(f"[yellow]![/yellow] Skipped {len(files_skipped)} files")
                    
            else:
                error_msg = result.get('error_message', 'Unknown error')
                console.print(f"[red]Error:[/red] {error_msg}")
            
        except Exception as e:
            console.print(f"[red]Error:[/red] {e}")
            
    else:
        # Legacy file association
        console.print(f"🔗 Associating files from archive [bold]{archive_id}[/bold] with simulation [bold]{simulation_id}[/bold]")
        
        if dry_run:
            console.print("[yellow]DRY RUN:[/yellow] No changes will be made")
            
        console.print("[yellow]Info:[/yellow] Legacy file association not implemented")


@archive.command(name='copy')
@click.argument('archive_id')
@click.argument('source_location')
@click.argument('destination_location')
@click.option('--simulation', help='Simulation ID for context resolution')
@click.option('--verify-integrity', is_flag=True, default=True, help='Verify file integrity after copy')
@click.option('--overwrite', is_flag=True, help='Overwrite existing files at destination')
def copy_archive(archive_id: str, source_location: str, destination_location: str,
                simulation: Optional[str], verify_integrity: bool, overwrite: bool):
    """Copy archive to a different location with path resolution"""
    
    bridge = _get_archive_bridge()
    
    if bridge:
        console.print(f"🔄 Copying archive [bold]{archive_id}[/bold]")
        console.print(f"From: {source_location} → To: {destination_location}")
        
        if feature_flags.is_enabled(FeatureFlag.USE_NEW_ARCHIVE_SERVICE):
            console.print("✨ Using new archive service")
        
        if simulation:
            console.print(f"Using simulation context: {simulation}")
            
        try:
            result = bridge.copy_archive(
                archive_id=archive_id,
                source_location=source_location,
                destination_location=destination_location,
                simulation_id=simulation,
                verify_integrity=verify_integrity,
                overwrite_existing=overwrite
            )
            
            if result.get('success', False):
                destination_path = result.get('destination_path', 'Unknown path')
                bytes_processed = format_size(result.get('bytes_processed', 0))
                duration = result.get('duration_seconds', 0)
                
                console.print(f"[green]✓[/green] Archive copied successfully")
                console.print(f"  Destination: {destination_path}")
                console.print(f"  Size: {bytes_processed}")
                console.print(f"  Duration: {duration:.1f}s")
                
                if result.get('checksum_verified', False):
                    console.print(f"  [green]✓[/green] Integrity verified")
                    
                warnings = result.get('warnings', [])
                if warnings:
                    for warning in warnings:
                        console.print(f"  [yellow]![/yellow] {warning}")
                        
            else:
                error_msg = result.get('error_message', 'Unknown error')
                console.print(f"[red]Error:[/red] {error_msg}")
                
        except Exception as e:
            console.print(f"[red]Error:[/red] {e}")
    else:
        console.print("[red]Error:[/red] Archive copy requires new archive service")
        console.print("Please enable the feature flag: TELLUS_USE_NEW_ARCHIVE_SERVICE=true")


@archive.command(name='move')
@click.argument('archive_id')
@click.argument('source_location')
@click.argument('destination_location')
@click.option('--simulation', help='Simulation ID for context resolution')
@click.option('--no-cleanup', is_flag=True, help='Do not cleanup source after move')
@click.option('--verify-integrity', is_flag=True, default=True, help='Verify file integrity')
def move_archive(archive_id: str, source_location: str, destination_location: str,
                simulation: Optional[str], no_cleanup: bool, verify_integrity: bool):
    """Move archive to a different location with path resolution"""
    
    bridge = _get_archive_bridge()
    
    if bridge:
        console.print(f"📦 Moving archive [bold]{archive_id}[/bold]")
        console.print(f"From: {source_location} → To: {destination_location}")
        
        if feature_flags.is_enabled(FeatureFlag.USE_NEW_ARCHIVE_SERVICE):
            console.print("✨ Using new archive service")
        
        if simulation:
            console.print(f"Using simulation context: {simulation}")
            
        if no_cleanup:
            console.print("[yellow]Warning:[/yellow] Source will not be cleaned up after move")
            
        try:
            result = bridge.move_archive(
                archive_id=archive_id,
                source_location=source_location,
                destination_location=destination_location,
                simulation_id=simulation,
                cleanup_source=not no_cleanup,
                verify_integrity=verify_integrity
            )
            
            if result.get('success', False):
                destination_path = result.get('destination_path', 'Unknown path')
                bytes_processed = format_size(result.get('bytes_processed', 0))
                duration = result.get('duration_seconds', 0)
                
                console.print(f"[green]✓[/green] Archive moved successfully")
                console.print(f"  Destination: {destination_path}")
                console.print(f"  Size: {bytes_processed}")
                console.print(f"  Duration: {duration:.1f}s")
                
                if result.get('checksum_verified', False):
                    console.print(f"  [green]✓[/green] Integrity verified")
                    
                warnings = result.get('warnings', [])
                if warnings:
                    for warning in warnings:
                        console.print(f"  [green]✓[/green] {warning}")
                        
            else:
                error_msg = result.get('error_message', 'Unknown error')
                console.print(f"[red]Error:[/red] {error_msg}")
                
        except Exception as e:
            console.print(f"[red]Error:[/red] {e}")
    else:
        console.print("[red]Error:[/red] Archive move requires new archive service")
        console.print("Please enable the feature flag: TELLUS_USE_NEW_ARCHIVE_SERVICE=true")


@archive.command(name='extract')
@click.argument('archive_id')
@click.argument('destination_location')
@click.option('--simulation', help='Simulation ID for context resolution')
@click.option('--content-type', help='Only extract files of this content type')
@click.option('--pattern', help='Only extract files matching this pattern')
@click.option('--files', help='Comma-separated list of specific files to extract')
@click.option('--no-manifest', is_flag=True, help='Do not create extraction manifest')
@click.option('--overwrite', is_flag=True, help='Overwrite existing files')
@click.option('--flat', is_flag=True, help='Extract files to flat structure (ignore directories)')
def extract_archive(archive_id: str, destination_location: str, simulation: Optional[str],
                   content_type: Optional[str], pattern: Optional[str], files: Optional[str],
                   no_manifest: bool, overwrite: bool, flat: bool):
    """Extract archive contents to a location with path resolution"""
    
    bridge = _get_archive_bridge()
    
    if bridge:
        console.print(f"📂 Extracting archive [bold]{archive_id}[/bold]")
        console.print(f"To: {destination_location}")
        
        if feature_flags.is_enabled(FeatureFlag.USE_NEW_ARCHIVE_SERVICE):
            console.print("✨ Using new archive service")
        
        if simulation:
            console.print(f"Using simulation context: {simulation}")
            
        # Parse file filters
        file_filters = None
        if files:
            file_filters = [f.strip() for f in files.split(',')]
            console.print(f"Extracting specific files: {len(file_filters)} files")
            
        if content_type:
            console.print(f"Filtering by content type: {content_type}")
            
        if pattern:
            console.print(f"Filtering by pattern: {pattern}")
            
        try:
            result = bridge.extract_archive_to_location(
                archive_id=archive_id,
                destination_location=destination_location,
                simulation_id=simulation,
                file_filters=file_filters,
                content_type_filter=content_type,
                pattern_filter=pattern,
                preserve_directory_structure=not flat,
                overwrite_existing=overwrite,
                create_manifest=not no_manifest
            )
            
            if result.get('success', False):
                destination_path = result.get('destination_path', 'Unknown path')
                files_processed = result.get('files_processed', 0)
                bytes_processed = format_size(result.get('bytes_processed', 0))
                duration = result.get('duration_seconds', 0)
                
                console.print(f"[green]✓[/green] Archive extracted successfully")
                console.print(f"  Destination: {destination_path}")
                console.print(f"  Files: {files_processed}")
                console.print(f"  Size: {bytes_processed}")
                console.print(f"  Duration: {duration:.1f}s")
                
                if result.get('manifest_created', False):
                    console.print(f"  [green]✓[/green] Extraction manifest created")
                    
                warnings = result.get('warnings', [])
                if warnings:
                    for warning in warnings:
                        console.print(f"  [yellow]![/yellow] {warning}")
                        
            else:
                error_msg = result.get('error_message', 'Unknown error')
                console.print(f"[red]Error:[/red] {error_msg}")
                
        except Exception as e:
            console.print(f"[red]Error:[/red] {e}")
    else:
        console.print("[red]Error:[/red] Archive extraction requires new archive service")
        console.print("Please enable the feature flag: TELLUS_USE_NEW_ARCHIVE_SERVICE=true")


@archive.command(name='resolve-path')
@click.argument('location_name')
@click.argument('simulation_id')
@click.option('--template', help='Custom path template to resolve')
def resolve_location_path(location_name: str, simulation_id: str, template: Optional[str]):
    """Resolve location path template with simulation context"""
    
    bridge = _get_archive_bridge()
    
    if bridge:
        console.print(f"🔍 Resolving path for location [bold]{location_name}[/bold]")
        console.print(f"Simulation: {simulation_id}")
        
        if template:
            console.print(f"Template: {template}")
        
        if feature_flags.is_enabled(FeatureFlag.USE_NEW_ARCHIVE_SERVICE):
            console.print("✨ Using new archive service")
        
        try:
            result = bridge.resolve_location_path(
                location_name=location_name,
                simulation_id=simulation_id,
                path_template=template
            )
            
            if result.get('success', False):
                resolved_path = result.get('resolved_path', 'Failed to resolve')
                context_vars = result.get('context_variables', {})
                
                console.print(f"[green]✓[/green] Path resolved successfully")
                console.print(f"  Resolved path: [cyan]{resolved_path}[/cyan]")
                
                if context_vars:
                    console.print("  Context variables:")
                    for key, value in context_vars.items():
                        console.print(f"    {key}: {value}")
                        
            else:
                errors = result.get('resolution_errors', ['Unknown error'])
                console.print(f"[red]Error:[/red] Path resolution failed")
                for error in errors:
                    console.print(f"  - {error}")
                    
        except Exception as e:
            console.print(f"[red]Error:[/red] {e}")
    else:
        console.print("[red]Error:[/red] Path resolution requires new archive service")
        console.print("Please enable the feature flag: TELLUS_USE_NEW_ARCHIVE_SERVICE=true")


# Cache management commands  
@archive.group()
def cache():
    """Manage archive cache"""
    pass


@cache.command(name='status')
@click.option('--detailed', is_flag=True, help='Show detailed cache information')
def cache_status(detailed: bool):
    """Show cache status"""
    
    bridge = _get_archive_bridge()
    
    if bridge:
        console.print("Cache status (new architecture):")
        if feature_flags.is_enabled(FeatureFlag.USE_NEW_ARCHIVE_SERVICE):
            console.print("✨ Using new archive service")
        # Cache status would need to be implemented in the bridge/service
        console.print("Cache status not yet implemented in new service")
    else:
        # Legacy cache management
        try:
            cache_manager = CacheManager()
            status = cache_manager.status()
            
            console.print("Cache Status:")
            console.print(f"  Total size: {format_size(status.get('total_size', 0))}")
            console.print(f"  Archive cache: {format_size(status.get('archive_cache_size', 0))}")
            console.print(f"  File cache: {format_size(status.get('file_cache_size', 0))}")
            console.print(f"  Cached archives: {status.get('cached_archives', 0)}")
            console.print(f"  Cached files: {status.get('cached_files', 0)}")
            
            if detailed:
                console.print("\nCache configuration:")
                config = cache_manager.config
                console.print(f"  Archive cache limit: {format_size(config.archive_cache_size_limit)}")
                console.print(f"  File cache limit: {format_size(config.file_cache_size_limit)}")
                console.print(f"  Cache directory: {config.cache_dir}")
                
        except Exception as e:
            console.print(f"[red]Error getting cache status:[/red] {e}")


# Archive transfer and extraction commands

@archive.command()
@click.argument('archive_id')
@click.argument('source_location')
@click.argument('destination_location')
@click.option('--operation', type=click.Choice(['copy', 'move']), default='copy', help='Copy or move the archive')
@click.option('--simulation', help='Simulation ID for path template resolution')
@click.option('--overwrite', is_flag=True, help='Overwrite existing files at destination')
@click.option('--no-verify', is_flag=True, help='Skip integrity verification')
@click.option('--watch', is_flag=True, help='Watch transfer progress in real-time')
def transfer(archive_id: str, source_location: str, destination_location: str, 
             operation: str, simulation: Optional[str], overwrite: bool, no_verify: bool, watch: bool):
    """Transfer an archive between locations"""
    
    bridge = _get_archive_bridge()
    
    if not bridge:
        console.print("[red]Error:[/red] Archive transfer requires new architecture")
        console.print("Enable with: export TELLUS_USE_NEW_ARCHIVE_SERVICE=true")
        sys.exit(1)
    
    console.print(f"🚛 {operation.title()}ing archive [bold]{archive_id}[/bold]")
    console.print(f"  From: [blue]{source_location}[/blue]")
    console.print(f"  To: [green]{destination_location}[/green]")
    
    if simulation:
        console.print(f"  Simulation context: {simulation}")
    
    if feature_flags.is_enabled(FeatureFlag.USE_NEW_ARCHIVE_SERVICE):
        console.print("✨ Using new archive service")
    
    try:
        # Start the transfer
        result = bridge.transfer_archive(
            archive_id=archive_id,
            source_location=source_location,
            destination_location=destination_location,
            operation_type=operation,
            simulation_id=simulation,
            overwrite=overwrite,
            verify_integrity=not no_verify
        )
        
        if not result.get('success', False):
            console.print(f"[red]Error:[/red] {result.get('error_message', 'Unknown error')}")
            sys.exit(1)
        
        operation_id = result['operation_id']
        console.print(f"Transfer started with ID: [cyan]{operation_id}[/cyan]")
        
        # Watch progress if requested
        if watch:
            console.print("\nWatching transfer progress (press Ctrl+C to stop watching):")
            _watch_operation_progress(bridge, operation_id)
        else:
            console.print(f"Use '[cyan]tellus archive status {operation_id}[/cyan]' to check progress")
            
    except ApplicationError as e:
        console.print(f"[red]Error:[/red] {str(e)}")
        sys.exit(1)


@archive.command()
@click.argument('archive_id')
@click.argument('source_location')
@click.argument('destination_location')
@click.option('--simulation', help='Simulation ID for path template resolution')
@click.option('--include', multiple=True, help='Include files matching pattern (can be used multiple times)')
@click.option('--exclude', multiple=True, help='Exclude files matching pattern (can be used multiple times)')
@click.option('--content-type', help='Only extract files of this content type')
@click.option('--overwrite', is_flag=True, help='Overwrite existing files at destination')
@click.option('--watch', is_flag=True, help='Watch extraction progress in real-time')
def extract(archive_id: str, source_location: str, destination_location: str,
            simulation: Optional[str], include: tuple, exclude: tuple, 
            content_type: Optional[str], overwrite: bool, watch: bool):
    """Extract an archive to a specific location"""
    
    bridge = _get_archive_bridge()
    
    if not bridge:
        console.print("[red]Error:[/red] Archive extraction requires new architecture")
        console.print("Enable with: export TELLUS_USE_NEW_ARCHIVE_SERVICE=true")
        sys.exit(1)
    
    console.print(f"📦 Extracting archive [bold]{archive_id}[/bold]")
    console.print(f"  From: [blue]{source_location}[/blue]")
    console.print(f"  To: [green]{destination_location}[/green]")
    
    if simulation:
        console.print(f"  Simulation context: {simulation}")
        
    if include:
        console.print(f"  Include patterns: {', '.join(include)}")
        
    if exclude:
        console.print(f"  Exclude patterns: {', '.join(exclude)}")
        
    if content_type:
        console.print(f"  Content type filter: {content_type}")
    
    if feature_flags.is_enabled(FeatureFlag.USE_NEW_ARCHIVE_SERVICE):
        console.print("✨ Using new archive service")
    
    try:
        # Start the extraction
        result = bridge.extract_archive_to_location(
            archive_id=archive_id,
            source_location=source_location,
            destination_location=destination_location,
            simulation_id=simulation,
            extract_all=not include and not content_type,  # Extract all if no specific filters
            include_patterns=list(include),
            exclude_patterns=list(exclude),
            overwrite=overwrite
        )
        
        if not result.get('success', False):
            console.print(f"[red]Error:[/red] {result.get('error_message', 'Unknown error')}")
            sys.exit(1)
        
        operation_id = result['operation_id']
        console.print(f"Extraction started with ID: [cyan]{operation_id}[/cyan]")
        
        # Watch progress if requested
        if watch:
            console.print("\nWatching extraction progress (press Ctrl+C to stop watching):")
            _watch_operation_progress(bridge, operation_id)
        else:
            console.print(f"Use '[cyan]tellus archive status {operation_id}[/cyan]' to check progress")
            
    except ApplicationError as e:
        console.print(f"[red]Error:[/red] {str(e)}")
        sys.exit(1)


@archive.command()
@click.argument('operation_id')
@click.option('--watch', is_flag=True, help='Watch operation progress in real-time')
def status(operation_id: str, watch: bool):
    """Check the status of an archive operation"""
    
    bridge = _get_archive_bridge()
    
    if not bridge:
        console.print("[red]Error:[/red] Operation status requires new architecture")
        console.print("Enable with: export TELLUS_USE_NEW_ARCHIVE_SERVICE=true")
        sys.exit(1)
    
    if watch:
        console.print(f"Watching operation [cyan]{operation_id}[/cyan] (press Ctrl+C to stop):")
        _watch_operation_progress(bridge, operation_id)
    else:
        _show_operation_status(bridge, operation_id)


@archive.command(name='bulk-copy')
@click.argument('destination_location')
@click.option('--source-location', help='Source location (if different for all archives)')
@click.option('--archives', help='Comma-separated list of archive IDs')
@click.option('--simulation', help='Simulation ID to copy archives for (alternative to --archives)')
@click.option('--simulation-context', help='Simulation ID for path template resolution')
@click.option('--parallel', type=int, default=3, help='Number of parallel operations')
@click.option('--continue-on-error', is_flag=True, help='Continue processing other archives on individual failures')
@click.option('--watch', is_flag=True, help='Watch bulk operation progress')
def bulk_copy(destination_location: str, source_location: Optional[str], 
              archives: Optional[str], simulation: Optional[str], 
              simulation_context: Optional[str], parallel: int, 
              continue_on_error: bool, watch: bool):
    """Copy multiple archives to a destination location"""
    
    _run_bulk_operation(
        operation_type="bulk_copy",
        destination_location=destination_location,
        source_location=source_location,
        archives=archives,
        simulation=simulation,
        simulation_context=simulation_context,
        parallel=parallel,
        continue_on_error=continue_on_error,
        watch=watch
    )


@archive.command(name='bulk-move')
@click.argument('destination_location')
@click.option('--source-location', help='Source location (if different for all archives)')
@click.option('--archives', help='Comma-separated list of archive IDs')
@click.option('--simulation', help='Simulation ID to move archives for (alternative to --archives)')
@click.option('--simulation-context', help='Simulation ID for path template resolution')
@click.option('--parallel', type=int, default=3, help='Number of parallel operations')
@click.option('--continue-on-error', is_flag=True, help='Continue processing other archives on individual failures')
@click.option('--watch', is_flag=True, help='Watch bulk operation progress')
def bulk_move(destination_location: str, source_location: Optional[str], 
              archives: Optional[str], simulation: Optional[str], 
              simulation_context: Optional[str], parallel: int, 
              continue_on_error: bool, watch: bool):
    """Move multiple archives to a destination location"""
    
    _run_bulk_operation(
        operation_type="bulk_move",
        destination_location=destination_location,
        source_location=source_location,
        archives=archives,
        simulation=simulation,
        simulation_context=simulation_context,
        parallel=parallel,
        continue_on_error=continue_on_error,
        watch=watch
    )


@archive.command(name='bulk-extract')
@click.argument('destination_location')
@click.option('--source-location', help='Source location (if different for all archives)')
@click.option('--archives', help='Comma-separated list of archive IDs')
@click.option('--simulation', help='Simulation ID to extract archives for (alternative to --archives)')
@click.option('--simulation-context', help='Simulation ID for path template resolution')
@click.option('--parallel', type=int, default=3, help='Number of parallel operations')
@click.option('--continue-on-error', is_flag=True, help='Continue processing other archives on individual failures')
@click.option('--watch', is_flag=True, help='Watch bulk operation progress')
def bulk_extract(destination_location: str, source_location: Optional[str], 
                 archives: Optional[str], simulation: Optional[str], 
                 simulation_context: Optional[str], parallel: int, 
                 continue_on_error: bool, watch: bool):
    """Extract multiple archives to a destination location"""
    
    _run_bulk_operation(
        operation_type="bulk_extract",
        destination_location=destination_location,
        source_location=source_location,
        archives=archives,
        simulation=simulation,
        simulation_context=simulation_context,
        parallel=parallel,
        continue_on_error=continue_on_error,
        watch=watch
    )


@archive.command(name='resolve-path')
@click.argument('location_name')
@click.argument('simulation_id')
@click.option('--template', help='Override location path template')
def resolve_path(location_name: str, simulation_id: str, template: Optional[str]):
    """Resolve location path template using simulation context"""
    
    bridge = _get_archive_bridge()
    
    if not bridge:
        console.print("[red]Error:[/red] Path resolution requires new architecture")
        console.print("Enable with: export TELLUS_USE_NEW_ARCHIVE_SERVICE=true")
        sys.exit(1)
    
    console.print(f"🔍 Resolving path for location [bold]{location_name}[/bold] with simulation [bold]{simulation_id}[/bold]")
    
    if template:
        console.print(f"Using template override: {template}")
    
    if feature_flags.is_enabled(FeatureFlag.USE_NEW_ARCHIVE_SERVICE):
        console.print("✨ Using new archive service")
    
    try:
        result = bridge.resolve_location_path(
            location_name=location_name,
            simulation_id=simulation_id,
            path_template=template
        )
        
        if not result.get('success', False):
            console.print(f"[red]Error:[/red] {result.get('error_message', 'Unknown error')}")
            sys.exit(1)
        
        # Create rich panel for resolution results
        content = f"""[cyan]Original Template:[/cyan] {result['original_template']}
[cyan]Resolved Path:[/cyan] [green]{result['resolved_path']}[/green]
[cyan]Simulation Context:[/cyan] {result['simulation_context']}"""
        
        if result.get('overrides_applied'):
            content += f"\n[cyan]Overrides Applied:[/cyan] {result['overrides_applied']}"
        
        panel = Panel(content, title=f"Path Resolution: [yellow]{location_name}[/yellow]", border_style="blue")
        console.print(panel)
        
    except ApplicationError as e:
        console.print(f"[red]Error:[/red] {str(e)}")
        sys.exit(1)


# Helper functions for new commands

def _run_bulk_operation(operation_type: str, destination_location: str, 
                       source_location: Optional[str], archives: Optional[str], 
                       simulation: Optional[str], simulation_context: Optional[str],
                       parallel: int, continue_on_error: bool, watch: bool):
    """Helper function to run bulk operations"""
    
    bridge = _get_archive_bridge()
    
    if not bridge:
        console.print(f"[red]Error:[/red] {operation_type.replace('_', ' ').title()} requires new architecture")
        console.print("Enable with: export TELLUS_USE_NEW_ARCHIVE_SERVICE=true")
        sys.exit(1)
    
    # Determine archive IDs to process
    archive_ids = []
    
    if archives:
        # Use provided archive list
        archive_ids = [aid.strip() for aid in archives.split(',')]
    elif simulation:
        # Get archives for simulation
        try:
            archives_data = bridge.list_archives_for_simulation_legacy_format(simulation, cached_only=False)
            archive_ids = [arch['archive_id'] for arch in archives_data]
        except Exception as e:
            console.print(f"[red]Error getting archives for simulation {simulation}:[/red] {e}")
            sys.exit(1)
    else:
        console.print("[red]Error:[/red] Must specify either --archives or --simulation")
        sys.exit(1)
    
    if not archive_ids:
        console.print("No archives found to process")
        return
    
    operation_display = operation_type.replace('bulk_', '').replace('_', ' ')
    console.print(f"🚀 Starting bulk {operation_display} of {len(archive_ids)} archives")
    console.print(f"  Destination: [green]{destination_location}[/green]")
    
    if source_location:
        console.print(f"  Source: [blue]{source_location}[/blue]")
    
    if simulation_context:
        console.print(f"  Simulation context: {simulation_context}")
    
    console.print(f"  Parallel operations: {parallel}")
    console.print(f"  Continue on error: {continue_on_error}")
    
    if feature_flags.is_enabled(FeatureFlag.USE_NEW_ARCHIVE_SERVICE):
        console.print("✨ Using new archive service")
    
    # Show archive list preview
    console.print(f"\nArchives to process:")
    for i, aid in enumerate(archive_ids[:5]):  # Show first 5
        console.print(f"  - {aid}")
    if len(archive_ids) > 5:
        console.print(f"  ... and {len(archive_ids) - 5} more")
    
    try:
        # Start the bulk operation
        result = bridge.start_bulk_operation(
            operation_type=operation_type,
            archive_ids=archive_ids,
            destination_location=destination_location,
            source_location=source_location,
            simulation_context=simulation_context,
            parallel_operations=parallel,
            continue_on_error=continue_on_error
        )
        
        if not result.get('success', False):
            console.print(f"[red]Error:[/red] {result.get('error_message', 'Unknown error')}")
            sys.exit(1)
        
        operation_id = result['operation_id']
        console.print(f"\nBulk operation started with ID: [cyan]{operation_id}[/cyan]")
        
        # Watch progress if requested
        if watch:
            console.print(f"\nWatching bulk {operation_display} progress (press Ctrl+C to stop watching):")
            _watch_operation_progress(bridge, operation_id)
        else:
            console.print(f"Use '[cyan]tellus archive status {operation_id}[/cyan]' to check progress")
            
    except ApplicationError as e:
        console.print(f"[red]Error:[/red] {str(e)}")
        sys.exit(1)


def _show_operation_status(bridge: ArchiveBridge, operation_id: str):
    """Show current status of an operation"""
    
    try:
        result = bridge.get_operation_progress(operation_id)
        
        if not result.get('success', False):
            console.print(f"[red]Error:[/red] {result.get('error_message', 'Unknown error')}")
            return
        
        # Create rich table for status
        table = Table(title=f"Operation Status: {operation_id}")
        table.add_column("Property", style="cyan", no_wrap=True)
        table.add_column("Value", style="white")
        
        # Basic status information
        status = result.get('status', 'unknown')
        status_color = {
            'pending': 'yellow',
            'running': 'blue', 
            'completed': 'green',
            'failed': 'red',
            'cancelled': 'dim'
        }.get(status, 'white')
        
        table.add_row("Status", f"[{status_color}]{status.title()}[/{status_color}]")
        table.add_row("Archive ID", result.get('archive_id', 'N/A'))
        table.add_row("Operation Type", result.get('operation_type', 'N/A'))
        
        # Progress information
        progress = result.get('progress_percentage', 0)
        table.add_row("Progress", f"{progress:.1f}%")
        table.add_row("Current Step", result.get('current_step', 'N/A'))
        table.add_row("Steps", f"{result.get('completed_steps', 0)}/{result.get('total_steps', 0)}")
        
        # Timing information
        if result.get('start_time'):
            table.add_row("Started", result['start_time'])
        if result.get('last_update'):
            table.add_row("Last Update", result['last_update'])
        
        # Performance information
        if result.get('files_processed'):
            table.add_row("Files Processed", str(result['files_processed']))
        if result.get('bytes_processed'):
            table.add_row("Data Processed", format_size(result['bytes_processed']))
        if result.get('processing_rate_mbps'):
            table.add_row("Rate", f"{result['processing_rate_mbps']:.1f} MB/s")
        
        # Error information
        if result.get('errors_encountered'):
            table.add_row("Errors", f"[red]{result['errors_encountered']}[/red]")
        if result.get('last_error'):
            table.add_row("Last Error", f"[red]{result['last_error']}[/red]")
        
        console.print(table)
        
        # Show estimation
        if result.get('estimated_completion'):
            console.print(f"\n[dim]Estimated completion: {result['estimated_completion']}[/dim]")
        
    except Exception as e:
        console.print(f"[red]Error getting operation status:[/red] {e}")


def _watch_operation_progress(bridge: ArchiveBridge, operation_id: str):
    """Watch operation progress in real-time"""
    import time
    from rich.live import Live
    from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TimeElapsedColumn
    
    try:
        with Live(refresh_per_second=2) as live:
            while True:
                try:
                    result = bridge.get_operation_progress(operation_id)
                    
                    if not result.get('success', False):
                        live.update(f"[red]Error:[/red] {result.get('error_message', 'Unknown error')}")
                        break
                    
                    status = result.get('status', 'unknown')
                    progress_pct = result.get('progress_percentage', 0)
                    current_step = result.get('current_step', 'Working...')
                    
                    # Create progress display
                    progress = Progress(
                        SpinnerColumn(),
                        TextColumn("[progress.description]{task.description}"),
                        BarColumn(),
                        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
                        TimeElapsedColumn(),
                    )
                    
                    task = progress.add_task(current_step, total=100, completed=progress_pct)
                    
                    # Add status information
                    status_colors = {'pending': 'yellow', 'running': 'blue', 'completed': 'green', 'failed': 'red'}
                    status_color = status_colors.get(status, 'white')
                    status_text = f"Status: [{status_color}]{status.title()}[/]"
                    
                    if result.get('current_file'):
                        status_text += f"\nCurrent file: {result['current_file']}"
                    
                    if result.get('processing_rate_mbps'):
                        status_text += f"\nRate: {result['processing_rate_mbps']:.1f} MB/s"
                    
                    # Update display
                    live.update(Panel(progress, title=status_text, border_style="blue"))
                    
                    # Check if operation is complete
                    if status in ('completed', 'failed', 'cancelled'):
                        if status == 'completed':
                            live.update(f"[green]✓[/green] Operation completed successfully!")
                        elif status == 'failed':
                            error_msg = result.get('last_error', 'Unknown error')
                            live.update(f"[red]✗[/red] Operation failed: {error_msg}")
                        else:
                            live.update(f"[yellow]![/yellow] Operation was cancelled")
                        break
                    
                    time.sleep(2)
                    
                except KeyboardInterrupt:
                    live.update("Stopped watching (operation continues in background)")
                    break
                except Exception as e:
                    live.update(f"[red]Error watching progress:[/red] {e}")
                    break
                    
    except Exception as e:
        console.print(f"[red]Error setting up progress watch:[/red] {e}")