"""Configuration management CLI commands for Tellus."""

import shutil
from pathlib import Path
from typing import List, Tuple

import rich_click as click
from rich.console import Console
from rich.table import Table
from rich.panel import Panel

from ...application.container import ServiceContainer

console = Console()


@click.group(name="config")
def config():
    """Configuration management and data migration commands."""
    pass


@config.command(name="info")
def config_info():
    """Show current configuration and data paths."""
    try:
        from ...application.container import get_service_container
        
        container = get_service_container()
        
        # Create info table
        table = Table(title="Tellus Configuration")
        table.add_column("Setting", style="cyan", no_wrap=True)
        table.add_column("Value", style="green")
        
        table.add_row("Project Directory", str(container.project_path))
        table.add_row("Global Data Directory", str(container.global_data_path))
        table.add_row("Project Data Directory", str(container.project_data_path))
        
        console.print(table)
        
        # Show file locations
        global_files = list(container.global_data_path.glob("*.json"))
        project_files = list(container.project_data_path.glob("*.json"))
        
        if global_files:
            console.print("\n[bold]Global Data Files:[/bold]")
            for file in global_files:
                size = file.stat().st_size if file.exists() else 0
                console.print(f"  • {file.name} ({size} bytes)")
        
        if project_files:
            console.print("\n[bold]Project Data Files:[/bold]")
            for file in project_files:
                size = file.stat().st_size if file.exists() else 0
                console.print(f"  • {file.name} ({size} bytes)")
                
    except Exception as e:
        console.print(f"[red]Error:[/red] {str(e)}")


@config.command(name="migrate")
@click.option("--source", type=click.Path(exists=True, path_type=Path), 
              help="Source directory with old JSON files (defaults to current directory)")
@click.option("--dry-run", is_flag=True, help="Show what would be migrated without making changes")
@click.option("--force", is_flag=True, help="Overwrite existing files in destination")
def migrate_data(source: Path = None, dry_run: bool = False, force: bool = False):
    """Migrate existing Tellus data files to the new hybrid persistence structure."""
    
    if source is None:
        source = Path.cwd()
    
    console.print(f"[dim]Scanning for Tellus data files in: {source}[/dim]")
    
    # Define file mappings: (source_file, destination_type, description)
    file_mappings = [
        ("simulations.json", "project", "Simulation definitions"),
        ("locations.json", "global", "Location registry"),
        ("archives.json", "global", "Archive registry"),
        ("workflows.json", "project", "Workflow definitions"),
        ("workflow_runs.json", "project", "Workflow execution history"),
        ("workflow_templates.json", "project", "Workflow templates"),
        ("file_types.json", "global", "File type configurations"),
        ("progress_tracking.json", "global", "Progress tracking data"),
    ]
    
    # Detect available files
    migrations: List[Tuple[Path, Path, str]] = []
    container = ServiceContainer(project_path=source)
    
    for filename, dest_type, description in file_mappings:
        source_file = source / filename
        if source_file.exists():
            if dest_type == "global":
                dest_file = container.global_data_path / filename
            else:  # project
                dest_file = container.project_data_path / filename
            
            migrations.append((source_file, dest_file, description))
    
    if not migrations:
        console.print("[yellow]No Tellus data files found to migrate.[/yellow]")
        return
    
    # Show migration plan
    console.print("\n[bold]Migration Plan:[/bold]")
    table = Table()
    table.add_column("File", style="cyan")
    table.add_column("Source", style="dim")
    table.add_column("Destination", style="green")
    table.add_column("Description", style="blue")
    table.add_column("Status", style="yellow")
    
    for source_file, dest_file, description in migrations:
        status = "✓ Ready"
        if dest_file.exists():
            if force:
                status = "⚠ Will overwrite"
            else:
                status = "✗ Exists (use --force)"
        
        table.add_row(
            source_file.name,
            str(source_file.parent),
            str(dest_file.parent),
            description,
            status
        )
    
    console.print(table)
    
    if dry_run:
        console.print("\n[dim]Dry run - no files were actually migrated.[/dim]")
        return
    
    # Check for conflicts
    conflicts = [(s, d, desc) for s, d, desc in migrations if d.exists() and not force]
    if conflicts:
        console.print(f"\n[red]Error:[/red] {len(conflicts)} files already exist at destination.")
        console.print("Use --force to overwrite existing files.")
        return
    
    # Perform migration
    console.print(f"\n[bold]Migrating {len(migrations)} files...[/bold]")
    
    successful = 0
    for source_file, dest_file, description in migrations:
        try:
            # Ensure destination directory exists
            dest_file.parent.mkdir(parents=True, exist_ok=True)
            
            # Copy file
            shutil.copy2(source_file, dest_file)
            console.print(f"✓ Migrated {source_file.name}")
            successful += 1
            
        except Exception as e:
            console.print(f"✗ Failed to migrate {source_file.name}: {str(e)}")
    
    if successful == len(migrations):
        console.print(f"\n[green]✓ Successfully migrated all {successful} files![/green]")
        
        # Show post-migration info
        console.print(Panel(
            f"""[bold]Migration Complete![/bold]

Your Tellus data has been migrated to the new structure:
• Global data: [green]{container.global_data_path}[/green]
• Project data: [green]{container.project_data_path}[/green]

You can now safely delete the old JSON files from the source directory.
Use [cyan]tellus config info[/cyan] to view your current configuration.""",
            title="Migration Success",
            border_style="green"
        ))
    else:
        console.print(f"\n[yellow]Migration completed with errors: {successful}/{len(migrations)} files migrated.[/yellow]")


@config.command(name="reset")
@click.option("--global-data", is_flag=True, help="Reset global data directory")
@click.option("--project-data", is_flag=True, help="Reset project data directory")
@click.option("--confirm", is_flag=True, help="Skip confirmation prompt")
def reset_data(global_data: bool = False, project_data: bool = False, confirm: bool = False):
    """Reset Tellus data directories (DANGEROUS - this will delete data)."""
    
    if not global_data and not project_data:
        console.print("[red]Error:[/red] Must specify --global-data or --project-data")
        return
    
    from ...application.container import get_service_container
    container = get_service_container()
    
    targets = []
    if global_data:
        targets.append(("Global data", container.global_data_path))
    if project_data:
        targets.append(("Project data", container.project_data_path))
    
    console.print("[red][bold]WARNING: This will permanently delete data![/bold][/red]")
    for name, path in targets:
        console.print(f"• {name}: {path}")
    
    if not confirm:
        if not click.confirm("Are you sure you want to proceed?"):
            console.print("Operation cancelled.")
            return
    
    for name, path in targets:
        try:
            if path.exists():
                shutil.rmtree(path)
                console.print(f"✓ Deleted {name} directory")
            else:
                console.print(f"• {name} directory did not exist")
        except Exception as e:
            console.print(f"✗ Failed to delete {name}: {str(e)}")
    
    console.print("[green]Reset completed.[/green]")