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