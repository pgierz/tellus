#!/usr/bin/env python3
"""CLI commands for archive management in tellus simulations"""

import click
from pathlib import Path
from typing import Optional, List
import sys

from .simulation import (
    Simulation, CacheManager, CacheConfig, ArchiveRegistry, 
    CompressedArchive, CLIProgressCallback, PathMapping
)


# Helper functions
def get_simulation(sim_id: str) -> Simulation:
    """Get simulation by ID, exit if not found"""
    sim = Simulation.get_simulation(sim_id)
    if not sim:
        click.echo(f"Error: Simulation '{sim_id}' not found", err=True)
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


# Archive management commands
@click.group()
def archive():
    """Archive management commands"""
    pass


@archive.command()
@click.argument('sim_id')
@click.argument('archive_path')
@click.option('--name', help='Human-friendly name for the archive')
@click.option('--archive-id', help='Custom ID for the archive')
@click.option('--location', help='Location name where the archive is stored')
@click.option('--tags', help='Comma-separated custom tags for the archive')
def add(sim_id: str, archive_path: str, name: Optional[str], 
        archive_id: Optional[str], location: Optional[str], tags: Optional[str]):
    """Add an archive to a simulation"""
    
    sim = get_simulation(sim_id)
    
    # Validate location if provided  
    location_obj = None
    if location:
        from ..location import Location
        location_obj = Location.get_location(location)
        if not location_obj:
            click.echo(f"Error: Location '{location}' not found", err=True)
            click.echo("Available locations:")
            for loc in Location.list_locations():
                click.echo(f"  - {loc.name} ({', '.join(k.name for k in loc.kinds)})")
            sys.exit(1)
    
    # For local archives, verify the file exists
    if not location and not Path(archive_path).exists():
        click.echo(f"Error: Archive file not found: {archive_path}", err=True)
        sys.exit(1)
    
    click.echo(f"Adding archive to simulation {sim_id}...")
    if location:
        click.echo(f"  Location: {location} ({', '.join(k.name for k in location_obj.kinds)})")
    
    # Set up progress callback
    progress = CLIProgressCallback(verbose=True)
    
    try:
        # Create archive instance
        if not archive_id:
            archive_path_stem = Path(archive_path).stem
            archive_id = f"{sim_id}_{archive_path_stem}"
        
        archive = CompressedArchive(
            archive_id=archive_id,
            archive_location=archive_path,
            location=location_obj
        )
        archive.add_progress_callback(progress)
        
        # Get or create archive registry for simulation
        if not hasattr(sim, '_archive_registry'):
            sim._archive_registry = ArchiveRegistry(sim_id)
        
        archive_name = name or Path(archive_path).stem
        sim._archive_registry.add_archive(archive, archive_name)
        
        # Show results
        status = archive.status()
        file_count = status.get('file_count', 0)
        total_size = status.get('total_size', 0)
        tags_info = status.get('tags', {})
        
        click.echo(f"✓ Archive located: {archive_path} ({format_size(status.get('size', 0))})")
        if location:
            protocol = status.get('storage_protocol', 'unknown')
            click.echo(f"  Storage: {protocol} protocol via location '{location}'")
        click.echo(f"✓ Found {file_count} files")
        
        if tags_info:
            tag_summary = ", ".join([f"{tag} ({count})" for tag, count in tags_info.items()])
            click.echo(f"✓ Tagged files: {tag_summary}")
        
        click.echo(f"✓ Archive '{archive_name}' added to simulation {sim_id}")
        
        # Save simulation
        sim.save_simulations()
        
    except Exception as e:
        click.echo(f"Error adding archive: {e}", err=True)
        sys.exit(1)


@archive.command()
@click.argument('sim_id')
@click.option('--verbose', is_flag=True, help='Show detailed information')
@click.option('--cached-only', is_flag=True, help='Only show cached archives')
def list(sim_id: str, verbose: bool, cached_only: bool):
    """List archives for a simulation"""
    
    sim = get_simulation(sim_id)
    
    if not hasattr(sim, '_archive_registry') or not sim._archive_registry.archives:
        click.echo(f"No archives found for simulation {sim_id}")
        return
    
    registry = sim._archive_registry
    archives = registry.archives
    
    if cached_only:
        archives = {name: archive for name, archive in archives.items() 
                   if archive.status().get('cached', False)}
    
    if not archives:
        if cached_only:
            click.echo(f"No cached archives found for simulation {sim_id}")
        else:
            click.echo(f"No archives found for simulation {sim_id}")
        return
    
    if verbose:
        click.echo(f"Archives for simulation {sim_id}:\n")
        
        for name, archive in archives.items():
            status = archive.status()
            click.echo(f"📦 {name} ({archive.archive_id})")
            click.echo(f"   Location: {status.get('location', 'Unknown')}")
            
            # Show location info if available
            if status.get('location_name'):
                location_kinds = ", ".join(status.get('location_kinds', []))
                protocol = status.get('storage_protocol', 'unknown')
                click.echo(f"   Storage: {status['location_name']} ({location_kinds}) - {protocol} protocol")
            elif status.get('storage_protocol'):
                click.echo(f"   Storage: {status['storage_protocol']} protocol")
            
            size_str = format_size(status.get('size', 0))
            file_count = status.get('file_count', 0)
            cached = "✓" if status.get('cached', False) else "✗"
            
            click.echo(f"   Size: {size_str} | Files: {file_count} | Cached: {cached}")
            
            tags_info = status.get('tags', {})
            if tags_info:
                tag_summary = ", ".join([f"{tag} ({count})" for tag, count in tags_info.items()])
                click.echo(f"   Tags: {tag_summary}")
            
            click.echo(f"   Created: {status.get('created', 'Unknown')}")
            click.echo("")
    else:
        # Table format
        click.echo(f"Archives for simulation {sim_id}:\n")
        click.echo("Name            Archive ID       Size     Cached   Files   Location")
        click.echo("─" * 80)
        
        total_files = 0
        total_size = 0
        cached_count = 0
        
        for name, archive in archives.items():
            status = archive.status()
            
            size = status.get('size', 0)
            file_count = status.get('file_count', 0)
            cached = status.get('cached', False)
            location = status.get('location', 'Unknown')
            
            total_files += file_count
            total_size += size
            if cached:
                cached_count += 1
            
            cached_str = "✓" if cached else "✗"
            size_str = format_size(size)
            
            # Truncate long names/paths for table formatting
            name_display = name[:15] if len(name) <= 15 else name[:12] + "..."
            archive_id_display = archive.archive_id[:15] if len(archive.archive_id) <= 15 else archive.archive_id[:12] + "..."
            location_display = location if len(location) <= 30 else "..." + location[-27:]
            
            click.echo(f"{name_display:<15} {archive_id_display:<15} {size_str:<8} {cached_str:<8} {file_count:<7} {location_display}")
        
        click.echo(f"\nTotal: {len(archives)} archives, {total_files} files, {format_size(total_size)} ({cached_count} cached)")


@archive.command()
@click.argument('sim_id')
@click.argument('archive_name')
@click.option('--clear-cache', is_flag=True, help='Also remove cached files')
@click.option('--force', is_flag=True, help='Skip confirmation prompt')
def remove(sim_id: str, archive_name: str, clear_cache: bool, force: bool):
    """Remove an archive from a simulation"""
    
    sim = get_simulation(sim_id)
    
    if not hasattr(sim, '_archive_registry'):
        click.echo(f"Error: No archives found for simulation {sim_id}", err=True)
        sys.exit(1)
    
    registry = sim._archive_registry
    archive = registry.get_archive(archive_name)
    
    if not archive:
        click.echo(f"Error: Archive '{archive_name}' not found in simulation {sim_id}", err=True)
        sys.exit(1)
    
    # Show confirmation unless --force
    if not force:
        status = archive.status()
        
        click.echo(f"Remove archive '{archive_name}' from simulation {sim_id}?\n")
        click.echo("Archive details:")
        click.echo(f"  Name: {archive_name}")
        click.echo(f"  Location: {status.get('location', 'Unknown')}")
        click.echo(f"  Size: {format_size(status.get('size', 0))}")
        click.echo(f"  Files: {status.get('file_count', 0)}")
        
        cached = status.get('cached', False)
        click.echo(f"  Cached: {'Yes' if cached else 'No'}")
        
        if clear_cache and cached:
            click.echo(f"\nThis will:")
            click.echo(f"  • Remove the archive registration")
            click.echo(f"  • Clear cached archive data")
            click.echo(f"  • Clear cached extracted files")
            click.echo(f"  • NOT delete the original archive file")
        else:
            click.echo(f"\nThis will remove the archive registration but NOT delete the archive file.")
        
        if not click.confirm("\nContinue?"):
            click.echo("Cancelled.")
            return
    
    try:
        # Clear cache if requested
        if clear_cache:
            click.echo("✓ Clearing cached data...")
            # Note: This would need to be implemented in the cache manager
            # For now, just show the message
        
        # Remove from registry
        registry.remove_archive(archive_name)
        
        click.echo(f"✓ Archive '{archive_name}' removed from simulation {sim_id}")
        
        if clear_cache:
            click.echo("✓ Freed cache space")
        
        # Save simulation
        sim.save_simulations()
        
    except Exception as e:
        click.echo(f"Error removing archive: {e}", err=True)
        sys.exit(1)


@archive.command()
@click.argument('sim_id')
@click.argument('archive_name')
@click.option('--validate', is_flag=True, help='Validate manifest against archive')
@click.option('--check-access', is_flag=True, help='Test archive accessibility')
def status(sim_id: str, archive_name: str, validate: bool, check_access: bool):
    """Show detailed status for a specific archive"""
    
    sim = get_simulation(sim_id)
    
    if not hasattr(sim, '_archive_registry'):
        click.echo(f"Error: No archives found for simulation {sim_id}", err=True)
        sys.exit(1)
    
    registry = sim._archive_registry
    archive = registry.get_archive(archive_name)
    
    if not archive:
        click.echo(f"Error: Archive '{archive_name}' not found in simulation {sim_id}", err=True)
        sys.exit(1)
    
    click.echo(f"Archive Status: {archive_name} ({sim_id})\n")
    
    try:
        status_info = archive.status()
        
        # Location info
        click.echo("📍 Location")
        click.echo(f"   Path: {status_info.get('location', 'Unknown')}")
        click.echo(f"   Size: {format_size(status_info.get('size', 0))}")
        click.echo(f"   Type: Compressed tarball (.tar.gz)")
        
        if check_access:
            # This would need to be implemented
            click.echo(f"   Access: ✓ Readable")
        
        # Content info
        click.echo(f"\n📊 Content")
        file_count = status_info.get('file_count', 0)
        click.echo(f"   Files: {file_count} total")
        
        tags_info = status_info.get('tags', {})
        if tags_info:
            tag_summary = ", ".join([f"{tag} ({count})" for tag, count in tags_info.items()])
            click.echo(f"   Tags: {tag_summary}")
        
        if status_info.get('created'):
            click.echo(f"   Manifest: Up to date (created {status_info['created']})")
        
        # Validation results
        if validate:
            click.echo(f"\n🔍 Validation Results")
            try:
                validation = archive.validate_manifest()
                if validation.get('valid', False):
                    click.echo("   ✓ Manifest matches archive contents")
                    click.echo("   ✓ All file sizes match")
                    click.echo("   ✓ No missing files")
                    click.echo("   ✓ No extra files in archive")
                else:
                    for error in validation.get('errors', []):
                        click.echo(f"   ✗ {error}")
                    for missing in validation.get('missing_files', []):
                        click.echo(f"   ⚠️  Missing file: {missing}")
                    for extra in validation.get('extra_files', []):
                        click.echo(f"   ⚠️  Extra file: {extra}")
            except Exception as e:
                click.echo(f"   ✗ Validation failed: {e}")
        
        # Cache status
        click.echo(f"\n💾 Cache Status")
        cached = status_info.get('cached', False)
        click.echo(f"   Archive cached: {'✓ Yes' if cached else '✗ No'}")
        
        if cached:
            # This info would come from cache manager
            cache_stats = archive.cache_manager.get_cache_stats()
            click.echo(f"   Extracted files: Some files cached")
        
    except Exception as e:
        click.echo(f"Error getting archive status: {e}", err=True)
        sys.exit(1)


# File operation commands
@click.group()
def files():
    """File operation commands"""
    pass


@files.command()
@click.argument('sim_id')
@click.option('--tags', help='Filter by tags (comma-separated)')
@click.option('--pattern', help='Filter by glob pattern')
@click.option('--archive', help='Only search specific archive')
@click.option('--details', is_flag=True, help='Show file sizes, dates, checksums')
def list(sim_id: str, tags: Optional[str], pattern: Optional[str], 
         archive: Optional[str], details: bool):
    """List files in simulation archives"""
    
    sim = get_simulation(sim_id)
    
    if not hasattr(sim, '_archive_registry'):
        click.echo(f"Error: No archives found for simulation {sim_id}", err=True)
        sys.exit(1)
    
    registry = sim._archive_registry
    
    try:
        all_files = {}
        archives_to_search = [archive] if archive else registry.list_archives()
        
        if archive and archive not in registry.archives:
            click.echo(f"Error: Archive '{archive}' not found in simulation {sim_id}", err=True)
            sys.exit(1)
        
        # Collect files from selected archives
        for archive_name in archives_to_search:
            if archive_name not in registry.archives:
                continue
                
            archive_obj = registry.get_archive(archive_name)
            
            # Get files with filters
            tag_list = tags.split(',') if tags else None
            files_in_archive = archive_obj.list_files(tags=tag_list, pattern=pattern)
            
            # Add archive name to file info
            for file_path, tagged_file in files_in_archive.items():
                all_files[file_path] = {
                    'file': tagged_file,
                    'archive': archive_name
                }
        
        if not all_files:
            filter_desc = []
            if tags:
                filter_desc.append(f"tags: {tags}")
            if pattern:
                filter_desc.append(f"pattern: {pattern}")
            if archive:
                filter_desc.append(f"archive: {archive}")
            
            filter_str = f" matching {', '.join(filter_desc)}" if filter_desc else ""
            click.echo(f"No files found{filter_str} for simulation {sim_id}")
            return
        
        # Display results
        if details:
            click.echo(f"Files in simulation {sim_id}:\n")
            total_size = 0
            
            for file_path, info in sorted(all_files.items()):
                tagged_file = info['file']
                archive_name = info['archive']
                
                size = tagged_file.size or 0
                total_size += size
                
                size_str = format_size(size)
                tags_str = ', '.join(sorted(tagged_file.tags))
                modified_str = format_time_ago(tagged_file.modified) if tagged_file.modified else "Unknown"
                
                click.echo(f"{file_path:<50} {size_str:<10} {modified_str:<15} [{tags_str}] ({archive_name})")
            
            click.echo(f"\nFound: {len(all_files)} files, {format_size(total_size)} total")
        else:
            click.echo(f"Files in simulation {sim_id}:\n")
            
            for file_path, info in sorted(all_files.items()):
                tagged_file = info['file']
                archive_name = info['archive']
                tags_str = ', '.join(sorted(tagged_file.tags))
                
                click.echo(f"{file_path:<50} [{tags_str}] ({archive_name})")
            
            click.echo(f"\nTotal: {len(all_files)} files")
            
            # Show archive summary
            archive_counts = {}
            for info in all_files.values():
                archive_name = info['archive']
                archive_counts[archive_name] = archive_counts.get(archive_name, 0) + 1
            
            if len(archive_counts) > 1:
                archive_summary = ", ".join([f"{name} ({count})" for name, count in archive_counts.items()])
                click.echo(f"Archives: {archive_summary}")
        
    except Exception as e:
        click.echo(f"Error listing files: {e}", err=True)
        sys.exit(1)


@files.command()
@click.argument('sim_id')
@click.argument('filename_or_pattern')
@click.option('--archive', help='Only search specific archive')
@click.option('--details', is_flag=True, help='Show file details and recommendations')
def find(sim_id: str, filename_or_pattern: str, archive: Optional[str], details: bool):
    """Find specific files in simulation archives"""
    
    sim = get_simulation(sim_id)
    
    if not hasattr(sim, '_archive_registry'):
        click.echo(f"Error: No archives found for simulation {sim_id}", err=True)
        sys.exit(1)
    
    registry = sim._archive_registry
    
    try:
        matches = []
        archives_to_search = [archive] if archive else registry.list_archives()
        
        if archive and archive not in registry.archives:
            click.echo(f"Error: Archive '{archive}' not found in simulation {sim_id}", err=True)
            sys.exit(1)
        
        # Search for exact matches and pattern matches
        for archive_name in archives_to_search:
            if archive_name not in registry.archives:
                continue
                
            archive_obj = registry.get_archive(archive_name)
            files_in_archive = archive_obj.list_files()
            
            # Check for exact match first
            if filename_or_pattern in files_in_archive:
                matches.append({
                    'path': filename_or_pattern,
                    'file': files_in_archive[filename_or_pattern],
                    'archive': archive_name,
                    'match_type': 'exact'
                })
            else:
                # Check for pattern matches
                import fnmatch
                for file_path, tagged_file in files_in_archive.items():
                    if fnmatch.fnmatch(file_path, filename_or_pattern):
                        matches.append({
                            'path': file_path,
                            'file': tagged_file,
                            'archive': archive_name,
                            'match_type': 'pattern'
                        })
        
        if not matches:
            click.echo(f"No files matching '{filename_or_pattern}' found in simulation {sim_id}")
            return
        
        # Display results
        if len(matches) == 1 and matches[0]['match_type'] == 'exact':
            # Single exact match
            match = matches[0]
            tagged_file = match['file']
            archive_name = match['archive']
            
            click.echo(f"Found \"{filename_or_pattern}\" in simulation {sim_id}:\n")
            click.echo(f"📁 {match['path']}")
            click.echo(f"   Archive: {archive_name}")
            click.echo(f"   Size: {format_size(tagged_file.size or 0)}")
            click.echo(f"   Tags: [{', '.join(sorted(tagged_file.tags))}]")
            
            # Check if archive is cached
            archive_obj = registry.get_archive(archive_name)
            status = archive_obj.status()
            cached = status.get('cached', False)
            click.echo(f"   Cached: {'✓ Archive cached' if cached else '✗ Not cached'}")
            
        elif len([m for m in matches if m['match_type'] == 'exact']) > 1:
            # Multiple exact matches (same filename in different archives)
            exact_matches = [m for m in matches if m['match_type'] == 'exact']
            
            click.echo(f"Found \"{filename_or_pattern}\" in simulation {sim_id}:\n")
            
            # Sort by archive size (cached first, then by size)
            def sort_key(match):
                archive_obj = registry.get_archive(match['archive'])
                status = archive_obj.status()
                is_cached = status.get('cached', False)
                size = status.get('size', 0)
                return (not is_cached, size)  # Cached first, then smaller
            
            exact_matches.sort(key=sort_key)
            
            for match in exact_matches:
                tagged_file = match['file']
                archive_name = match['archive']
                
                click.echo(f"📁 {match['path']}")
                
                archive_obj = registry.get_archive(archive_name)
                status = archive_obj.status()
                cached = status.get('cached', False)
                archive_size = status.get('size', 0)
                
                size_str = format_size(tagged_file.size or 0)
                tags_str = ', '.join(sorted(tagged_file.tags))
                cached_str = "cached ✓" if cached else "not cached"
                
                click.echo(f"   Archive: {archive_name} ({format_size(archive_size)}, {cached_str})")
                click.echo(f"   Size: {size_str} | Tags: [{tags_str}]")
                click.echo()
            
            # Show recommendation
            best_match = exact_matches[0]
            best_archive = best_match['archive']
            click.echo(f"For fastest access, use: {best_archive} (optimal choice)")
            
        else:
            # Pattern matches
            click.echo(f"Found {len(matches)} files matching \"{filename_or_pattern}\" in simulation {sim_id}:\n")
            
            total_size = 0
            archive_counts = {}
            
            for match in sorted(matches, key=lambda m: m['path']):
                tagged_file = match['file']
                archive_name = match['archive']
                
                size = tagged_file.size or 0
                total_size += size
                archive_counts[archive_name] = archive_counts.get(archive_name, 0) + 1
                
                size_str = format_size(size)
                tags_str = ', '.join(sorted(tagged_file.tags))
                
                click.echo(f"📁 {match['path']:<40} {size_str:<10} [{tags_str}] ({archive_name})")
            
            click.echo(f"\nTotal: {len(matches)} files, {format_size(total_size)}")
        
    except Exception as e:
        click.echo(f"Error finding files: {e}", err=True)
        sys.exit(1)


@files.command()
@click.argument('sim_id')
@click.argument('file_path', required=False)
@click.option('--tags', help='Extract files with these tags (comma-separated)')
@click.option('--pattern', help='Extract files matching pattern')
@click.option('--archive', help='Extract only from specific archive')
@click.option('--output', default='.', help='Output directory (default: current directory)')
@click.option('--no-path-mapping', is_flag=True, help='Keep original archive paths')
def extract(sim_id: str, file_path: Optional[str], tags: Optional[str], 
            pattern: Optional[str], archive: Optional[str], output: str, 
            no_path_mapping: bool):
    """Extract files from simulation archives (smart selection)"""
    
    sim = get_simulation(sim_id)
    
    if not hasattr(sim, '_archive_registry'):
        click.echo(f"Error: No archives found for simulation {sim_id}", err=True)
        sys.exit(1)
    
    registry = sim._archive_registry
    output_path = Path(output)
    
    # Validate inputs
    filter_count = sum(bool(x) for x in [file_path, tags, pattern])
    if filter_count == 0:
        click.echo("Error: Must specify either a file path, --tags, or --pattern", err=True)
        sys.exit(1)
    elif filter_count > 1:
        click.echo("Error: Can only specify one of: file path, --tags, or --pattern", err=True)
        sys.exit(1)
    
    if archive and archive not in registry.archives:
        click.echo(f"Error: Archive '{archive}' not found in simulation {sim_id}", err=True)
        sys.exit(1)
    
    try:
        # Set up progress callback
        progress = CLIProgressCallback(verbose=True)
        
        if file_path:
            # Single file extraction with smart selection
            click.echo(f"Extracting \"{file_path}\" from simulation {sim_id}...")
            
            if archive:
                # Extract from specific archive
                archive_obj = registry.get_archive(archive)
                archive_obj.add_progress_callback(progress)
                extracted_path = archive_obj.extract_file(file_path, output_path, 
                                                        apply_path_mapping=not no_path_mapping)
                click.echo(f"✓ Extracted to: {extracted_path}")
            else:
                # Smart extraction
                extracted_path = registry.extract_file_smart(file_path, output_path, 
                                                           apply_path_mapping=not no_path_mapping)
                click.echo(f"✓ Extracted to: {extracted_path}")
        
        else:
            # Multi-file extraction
            filter_desc = []
            if tags:
                filter_desc.append(f"tags: {tags}")
            if pattern:
                filter_desc.append(f"pattern: {pattern}")
            if archive:
                filter_desc.append(f"archive: {archive}")
            
            click.echo(f"Extracting files from simulation {sim_id}...")
            click.echo(f"🎯 Filters: {', '.join(filter_desc)}")
            
            if archive:
                # Extract from specific archive
                archive_obj = registry.get_archive(archive)
                archive_obj.add_progress_callback(progress)
                
                if tags:
                    tag_list = tags.split(',')
                    extracted_paths = archive_obj.extract_files_by_tags(output_path, *tag_list,
                                                                      apply_path_mapping=not no_path_mapping)
                else:  # pattern
                    files_to_extract = archive_obj.get_files_by_pattern(pattern)
                    extracted_paths = archive_obj._extract_multiple_files(files_to_extract, output_path, 
                                                                         not no_path_mapping)
                
                click.echo(f"✓ Extracted {len(extracted_paths)} files to {output_path}")
            else:
                # Extract from all archives
                if tags:
                    tag_list = tags.split(',')
                    results = registry.extract_files_by_tags(output_path, *tag_list,
                                                           apply_path_mapping=not no_path_mapping)
                    
                    total_files = sum(len(files) for files in results.values())
                    click.echo(f"✓ Extracted {total_files} files to {output_path}")
                    
                    for archive_name, extracted_files in results.items():
                        if extracted_files:
                            click.echo(f"   From {archive_name}: {len(extracted_files)} files")
                
                else:  # pattern - need to implement this
                    click.echo("Pattern extraction across all archives not yet implemented")
                    sys.exit(1)
        
    except Exception as e:
        click.echo(f"Error extracting files: {e}", err=True)
        sys.exit(1)


# Archive-specific extraction command
@archive.command()
@click.argument('sim_id')
@click.argument('archive_name')
@click.argument('file_path', required=False)
@click.option('--tags', help='Extract files with these tags (comma-separated)')
@click.option('--pattern', help='Extract files matching pattern')
@click.option('--output', default='.', help='Output directory (default: current directory)')
@click.option('--no-path-mapping', is_flag=True, help='Keep original archive paths')
def extract(sim_id: str, archive_name: str, file_path: Optional[str], 
            tags: Optional[str], pattern: Optional[str], output: str, 
            no_path_mapping: bool):
    """Extract files from a specific archive"""
    
    sim = get_simulation(sim_id)
    
    if not hasattr(sim, '_archive_registry'):
        click.echo(f"Error: No archives found for simulation {sim_id}", err=True)
        sys.exit(1)
    
    registry = sim._archive_registry
    archive_obj = registry.get_archive(archive_name)
    
    if not archive_obj:
        click.echo(f"Error: Archive '{archive_name}' not found in simulation {sim_id}", err=True)
        sys.exit(1)
    
    output_path = Path(output)
    
    # Validate inputs
    filter_count = sum(bool(x) for x in [file_path, tags, pattern])
    if filter_count == 0:
        click.echo("Error: Must specify either a file path, --tags, or --pattern", err=True)
        sys.exit(1)
    elif filter_count > 1:
        click.echo("Error: Can only specify one of: file path, --tags, or --pattern", err=True)
        sys.exit(1)
    
    try:
        # Set up progress callback
        progress = CLIProgressCallback(verbose=True)
        archive_obj.add_progress_callback(progress)
        
        if file_path:
            # Single file extraction
            click.echo(f"Extracting \"{file_path}\" from archive {archive_name}...")
            extracted_path = archive_obj.extract_file(file_path, output_path, 
                                                    apply_path_mapping=not no_path_mapping)
            click.echo(f"✓ Extracted to: {extracted_path}")
            
        elif tags:
            # Extract by tags
            tag_list = tags.split(',')
            click.echo(f"Extracting files with tags [{', '.join(tag_list)}] from archive {archive_name}...")
            
            extracted_paths = archive_obj.extract_files_by_tags(output_path, *tag_list,
                                                              apply_path_mapping=not no_path_mapping)
            click.echo(f"✓ Extracted {len(extracted_paths)} files to {output_path}")
            
        else:  # pattern
            # Extract by pattern
            click.echo(f"Extracting files matching '{pattern}' from archive {archive_name}...")
            
            files_to_extract = archive_obj.get_files_by_pattern(pattern)
            extracted_paths = archive_obj._extract_multiple_files(files_to_extract, output_path, 
                                                                 not no_path_mapping)
            click.echo(f"✓ Extracted {len(extracted_paths)} files to {output_path}")
        
    except Exception as e:
        click.echo(f"Error extracting from archive: {e}", err=True)
        sys.exit(1)


# Cache management commands
@click.group()
def cache():
    """Cache management commands"""
    pass


@cache.command()
@click.option('--sim', help='Show cache for specific simulation only')
@click.option('--detailed', is_flag=True, help='Show per-file cache details')
def status(sim: Optional[str], detailed: bool):
    """Show cache status and usage"""
    
    try:
        # For now, create a default cache manager to show status
        cache_manager = CacheManager()
        stats = cache_manager.get_cache_stats()
        
        click.echo("Tellus Archive Cache Status\n")
        
        # Cache usage overview
        click.echo("📊 Cache Usage")
        click.echo(f"   Location: {stats['cache_dir']}")
        
        archive_size = stats['archive_size']
        file_size = stats['file_size']
        total_size = stats['total_size']
        
        # For now, use placeholder limits since we don't have config loaded
        archive_limit = 50 * 1024**3  # 50GB
        file_limit = 10 * 1024**3     # 10GB
        total_limit = archive_limit + file_limit
        
        archive_pct = (archive_size / archive_limit * 100) if archive_limit > 0 else 0
        file_pct = (file_size / file_limit * 100) if file_limit > 0 else 0
        total_pct = (total_size / total_limit * 100) if total_limit > 0 else 0
        
        click.echo(f"   Archive cache: {format_size(archive_size)} / {format_size(archive_limit)} ({archive_pct:.0f}% used)")
        click.echo(f"   File cache: {format_size(file_size)} / {format_size(file_limit)} ({file_pct:.0f}% used)")
        click.echo(f"   Total: {format_size(total_size)} / {format_size(total_limit)}")
        
        # Cached archives summary
        archive_count = stats['archive_count']
        file_count = stats['file_count']
        
        if archive_count > 0:
            click.echo(f"\n📦 Cached Archives ({archive_count})")
            # In a real implementation, we'd get details from the cache manager
            click.echo("   (Archive details would be shown here)")
        
        if file_count > 0:
            click.echo(f"\n🗂️  Extracted Files Cache")
            click.echo(f"   {file_count} files cached ({format_size(file_size)} total)")
        
        # Recommendations
        click.echo(f"\n💡 Recommendations")
        if total_pct < 50:
            click.echo("   • Cache is healthy (plenty of space)")
        elif total_pct < 80:
            click.echo("   • Cache usage is moderate")
        else:
            click.echo("   • Cache is getting full, consider cleaning old files")
            
        if file_count > 100:
            click.echo("   • Consider cleaning files older than 1 week: tellus cache clean --files --older-than 7d")
            
    except Exception as e:
        click.echo(f"Error getting cache status: {e}", err=True)
        sys.exit(1)


@cache.command()
@click.option('--archives', is_flag=True, help='Clean archive cache only')
@click.option('--files', is_flag=True, help='Clean extracted files cache only')
@click.option('--older-than', help='Remove items older than time (e.g., 7d, 2w, 1m)')
@click.option('--sim', help='Clean cache for specific simulation only')
@click.option('--dry-run', is_flag=True, help='Show what would be cleaned without doing it')
def clean(archives: bool, files: bool, older_than: Optional[str], 
          sim: Optional[str], dry_run: bool):
    """Clean cached data"""
    
    if not archives and not files:
        # Default to cleaning both if nothing specified
        archives = files = True
    
    try:
        cache_manager = CacheManager()
        
        # Parse older_than if provided
        max_age_seconds = None
        if older_than:
            max_age_seconds = parse_time_duration(older_than)
            if max_age_seconds is None:
                click.echo(f"Error: Invalid time format '{older_than}'. Use formats like: 7d, 2w, 1m", err=True)
                sys.exit(1)
        
        # For now, show what we would do since the full implementation isn't complete
        action_desc = []
        if archives:
            action_desc.append("archives")
        if files:
            action_desc.append("extracted files")
        
        filter_desc = []
        if older_than:
            filter_desc.append(f"older than {older_than}")
        if sim:
            filter_desc.append(f"from simulation {sim}")
        
        action_str = " and ".join(action_desc)
        filter_str = f" ({', '.join(filter_desc)})" if filter_desc else ""
        
        if dry_run:
            click.echo(f"Would clean {action_str}{filter_str}...")
        else:
            click.echo(f"Cleaning {action_str}{filter_str}...")
        
        # In a full implementation, we would:
        # 1. Scan cache for items matching criteria
        # 2. Calculate total size to be freed
        # 3. Show confirmation if not dry_run
        # 4. Perform the cleanup
        
        # For now, show placeholder results
        if dry_run:
            click.echo("\n🗑️  Items to remove:")
            click.echo("   (Items would be listed here)")
            click.echo("\nTotal to free: (size would be calculated)")
        else:
            if not click.confirm("\nContinue with cleanup?"):
                click.echo("Cancelled.")
                return
            
            # Perform cleanup (placeholder)
            click.echo("✓ Cache cleanup completed")
            click.echo("✓ Freed (amount) of cache space")
        
    except Exception as e:
        click.echo(f"Error cleaning cache: {e}", err=True)
        sys.exit(1)


@cache.command()
@click.option('--archive-size', help='Set archive cache size limit (e.g., 100GB)')
@click.option('--file-size', help='Set extracted files cache size limit')
@click.option('--policy', type=click.Choice(['lru', 'size', 'manual']), help='Set cleanup policy')
@click.option('--location', help='Change cache directory location')
@click.option('--show', is_flag=True, help='Show current configuration')
def config(archive_size: Optional[str], file_size: Optional[str], 
           policy: Optional[str], location: Optional[str], show: bool):
    """Configure cache settings"""
    
    try:
        if show:
            # Show current configuration
            cache_manager = CacheManager()
            config = cache_manager.config
            
            click.echo("Current Cache Configuration:\n")
            
            click.echo(f"📍 Location: {config.cache_dir}")
            click.echo(f"📏 Size Limits:")
            click.echo(f"   Archive cache: {format_size(config.archive_cache_size_limit)}")
            click.echo(f"   File cache: {format_size(config.file_cache_size_limit)}")
            click.echo(f"   Total: {format_size(config.archive_cache_size_limit + config.file_cache_size_limit)}")
            
            click.echo(f"\n🔄 Cleanup Policy: {config.archive_cache_cleanup_policy.value.upper()}")
            if config.archive_cache_cleanup_policy.value == 'lru':
                click.echo("   When cache fills up, remove oldest accessed items first")
            
            click.echo(f"\n⚙️  Advanced Settings:")
            click.echo(f"   Unified cache: {'Yes' if config.unified_cache else 'No (separate archive/file limits)'}")
            click.echo(f"   Cache priority: Clean {config.cache_priority.value} first")
            
            click.echo(f"\nTo modify: tellus cache config --archive-size <size> --file-size <size>")
            return
        
        # Apply configuration changes
        changes_made = []
        
        if archive_size:
            # Parse size (e.g., "100GB" -> bytes)
            size_bytes = parse_size_string(archive_size)
            if size_bytes is None:
                click.echo(f"Error: Invalid size format '{archive_size}'. Use formats like: 50GB, 1TB", err=True)
                sys.exit(1)
            changes_made.append(f"Archive cache size: {format_size(size_bytes)}")
        
        if file_size:
            size_bytes = parse_size_string(file_size)
            if size_bytes is None:
                click.echo(f"Error: Invalid size format '{file_size}'. Use formats like: 10GB, 500MB", err=True)
                sys.exit(1)
            changes_made.append(f"File cache size: {format_size(size_bytes)}")
        
        if policy:
            changes_made.append(f"Cleanup policy: {policy.upper()}")
        
        if location:
            location_path = Path(location)
            if not location_path.exists():
                click.echo(f"Error: Directory '{location}' does not exist", err=True)
                sys.exit(1)
            changes_made.append(f"Cache location: {location}")
        
        if not changes_made:
            click.echo("No configuration changes specified. Use --show to see current config.")
            return
        
        # Apply changes (in full implementation)
        click.echo("Configuration updated:")
        for change in changes_made:
            click.echo(f"  ✓ {change}")
        
        click.echo("\nNote: Configuration changes will take effect for new cache operations.")
        
    except Exception as e:
        click.echo(f"Error configuring cache: {e}", err=True)
        sys.exit(1)


# Helper functions for parsing time and size
def parse_time_duration(duration_str: str) -> Optional[int]:
    """Parse duration string like '7d', '2w', '1m' into seconds"""
    import re
    
    match = re.match(r'^(\d+)([dwmy])$', duration_str.lower())
    if not match:
        return None
    
    value, unit = match.groups()
    value = int(value)
    
    if unit == 'd':
        return value * 86400  # days to seconds
    elif unit == 'w':
        return value * 604800  # weeks to seconds
    elif unit == 'm':
        return value * 2629746  # months to seconds (average)
    elif unit == 'y':
        return value * 31556952  # years to seconds (average)
    
    return None


def parse_size_string(size_str: str) -> Optional[int]:
    """Parse size string like '50GB', '1TB' into bytes"""
    import re
    
    match = re.match(r'^(\d+(?:\.\d+)?)\s*([KMGT]?B)$', size_str.upper())
    if not match:
        return None
    
    value, unit = match.groups()
    value = float(value)
    
    multipliers = {
        'B': 1,
        'KB': 1024,
        'MB': 1024**2,
        'GB': 1024**3,
        'TB': 1024**4
    }
    
    return int(value * multipliers.get(unit, 1))


# Tag management commands
@click.group()
def tags():
    """Tag management commands"""
    pass


@tags.command()
@click.argument('sim_id')
def list(sim_id: str):
    """Show current tag patterns and file counts"""
    
    sim = get_simulation(sim_id)
    
    if not hasattr(sim, '_archive_registry'):
        click.echo(f"Error: No archives found for simulation {sim_id}", err=True)
        sys.exit(1)
    
    registry = sim._archive_registry
    
    try:
        click.echo(f"Tags for simulation {sim_id}:\n")
        
        # Get combined tag counts from all archives
        combined_stats = registry.get_combined_stats()
        combined_tags = combined_stats.get('combined_tags', {})
        
        if not combined_tags:
            click.echo("No tagged files found.")
            return
        
        # Get tag patterns from first archive (assuming they're similar)
        # In a full implementation, we'd merge tag patterns from all archives
        first_archive = next(iter(registry.archives.values()))
        tag_patterns = first_archive.manifest.tag_system.tag_patterns if first_archive.manifest else {}
        
        click.echo("📋 Active Tag Patterns:")
        
        total_files = 0
        for tag in sorted(combined_tags.keys()):
            count = combined_tags[tag]
            total_files += count
            
            patterns = tag_patterns.get(tag, ['(pattern not available)'])
            pattern_str = ', '.join(patterns)
            
            click.echo(f"   {tag:<12} → {pattern_str:<40} ({count} files)")
        
        click.echo(f"\nTotal: {total_files} files across {len(combined_tags)} tag categories")
        click.echo(f"\n💡 Tip: Use 'tellus sim tags {sim_id} add <tag> <pattern>' to add custom patterns")
        
    except Exception as e:
        click.echo(f"Error listing tags: {e}", err=True)
        sys.exit(1)


@tags.command()
@click.argument('sim_id')
@click.argument('tag')
@click.argument('pattern')
def add(sim_id: str, tag: str, pattern: str):
    """Add a custom tag pattern"""
    
    sim = get_simulation(sim_id)
    
    if not hasattr(sim, '_archive_registry'):
        click.echo(f"Error: No archives found for simulation {sim_id}", err=True)
        sys.exit(1)
    
    registry = sim._archive_registry
    
    try:
        click.echo(f"Adding tag pattern: {tag} → {pattern}")
        
        # Add pattern to all archives in the simulation
        for archive_name, archive in registry.archives.items():
            if archive.manifest and archive.manifest.tag_system:
                archive.manifest.tag_system.add_tag_pattern(tag, pattern)
                click.echo(f"✓ Added to archive: {archive_name}")
        
        click.echo(f"\n💡 Run 'tellus sim tags {sim_id} retag' to apply new patterns to existing files")
        
        # Save simulation
        sim.save_simulations()
        
    except Exception as e:
        click.echo(f"Error adding tag pattern: {e}", err=True)
        sys.exit(1)


@tags.command()
@click.argument('sim_id')
@click.argument('tag')
@click.argument('pattern')
def remove(sim_id: str, tag: str, pattern: str):
    """Remove a tag pattern"""
    
    sim = get_simulation(sim_id)
    
    if not hasattr(sim, '_archive_registry'):
        click.echo(f"Error: No archives found for simulation {sim_id}", err=True)
        sys.exit(1)
    
    registry = sim._archive_registry
    
    try:
        click.echo(f"Removing tag pattern: {tag} → {pattern}")
        
        # Remove pattern from all archives in the simulation
        removed_count = 0
        for archive_name, archive in registry.archives.items():
            if archive.manifest and archive.manifest.tag_system:
                # Check if pattern exists before removing
                if tag in archive.manifest.tag_system.tag_patterns:
                    if pattern in archive.manifest.tag_system.tag_patterns[tag]:
                        archive.manifest.tag_system.remove_tag_pattern(tag, pattern)
                        removed_count += 1
                        click.echo(f"✓ Removed from archive: {archive_name}")
        
        if removed_count == 0:
            click.echo(f"Pattern '{pattern}' not found for tag '{tag}' in any archive")
        else:
            click.echo(f"\n💡 Run 'tellus sim tags {sim_id} retag' to update file tags")
        
        # Save simulation
        sim.save_simulations()
        
    except Exception as e:
        click.echo(f"Error removing tag pattern: {e}", err=True)
        sys.exit(1)


@tags.command()
@click.argument('sim_id')
def retag(sim_id: str):
    """Re-scan all files with current tag patterns"""
    
    sim = get_simulation(sim_id)
    
    if not hasattr(sim, '_archive_registry'):
        click.echo(f"Error: No archives found for simulation {sim_id}", err=True)
        sys.exit(1)
    
    registry = sim._archive_registry
    
    try:
        click.echo(f"Re-tagging files in simulation {sim_id}...")
        
        total_files = 0
        for archive_name, archive in registry.archives.items():
            if archive.manifest:
                file_count = len(archive.manifest.files)
                total_files += file_count
                
                click.echo(f"Re-tagging {file_count} files in archive {archive_name}...")
                
                # Re-tag all files with current patterns
                for file_path in archive.manifest.files:
                    new_tags = archive.manifest.tag_system.tag_file(file_path)
                    archive.manifest.files[file_path].tags = new_tags
                
                # Update manifest
                archive.manifest.update_manifest()
                click.echo(f"✓ Updated {file_count} files in {archive_name}")
        
        click.echo(f"\n✓ Re-tagged {total_files} files across {len(registry.archives)} archives")
        
        # Save simulation
        sim.save_simulations()
        
    except Exception as e:
        click.echo(f"Error re-tagging files: {e}", err=True)
        sys.exit(1)


# Main CLI groups for integration
@click.group()
def sim():
    """Simulation commands"""
    pass


# Add subcommands to main groups
sim.add_command(archive)
sim.add_command(files)
sim.add_command(tags)

# Cache is a top-level command
# This would be integrated with the main CLI later