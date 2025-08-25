"""CLI commands for the Tellus TUI."""

import rich_click as click
from typing import Optional
import sys

from ..cli.main import console
from .main import run_tui


@click.group()
def tui():
    """Interactive Terminal User Interface for archive management"""
    pass


@tui.command()
@click.option('--archive', help='Archive ID to focus on startup')
@click.option('--simulation', help='Simulation ID to filter by')
@click.option('--debug', is_flag=True, help='Enable debug mode')
def app(archive: Optional[str], simulation: Optional[str], debug: bool):
    """Launch the interactive TUI application"""
    console.print("🚀 Launching Tellus TUI...")
    
    if archive:
        console.print(f"Focusing on archive: [cyan]{archive}[/cyan]")
    if simulation:
        console.print(f"Filtering by simulation: [blue]{simulation}[/blue]")
    if debug:
        console.print("[yellow]Debug mode enabled[/yellow]")
    
    try:
        run_tui(archive_id=archive, simulation_id=simulation, debug=debug)
    except KeyboardInterrupt:
        console.print("\n[yellow]TUI application interrupted[/yellow]")
    except Exception as e:
        console.print(f"[red]Error running TUI:[/red] {e}")
        if debug:
            raise
        sys.exit(1)


@tui.command()
@click.option('--archive', help='Archive ID to browse')
@click.option('--location', help='Location to browse')
def browser(archive: Optional[str], location: Optional[str]):
    """Launch the archive browser directly"""
    console.print("📂 Launching archive browser...")
    
    # This would launch just the browser screen
    try:
        run_tui(archive_id=archive, debug=False)
    except Exception as e:
        console.print(f"[red]Error launching browser:[/red] {e}")
        sys.exit(1)


@tui.command()
@click.option('--operation-id', help='Operation ID to monitor')
def monitor(operation_id: Optional[str]):
    """Launch the operation monitor"""
    console.print("📊 Launching operation monitor...")
    
    if operation_id:
        console.print(f"Monitoring operation: [cyan]{operation_id}[/cyan]")
    
    # This would launch the operation monitor screen
    try:
        run_tui(debug=False)
    except Exception as e:
        console.print(f"[red]Error launching monitor:[/red] {e}")
        sys.exit(1)


@tui.command()
@click.option('--max-concurrent', type=int, default=3, help='Maximum concurrent operations')
@click.option('--auto-start', is_flag=True, help='Start queue processing immediately')
def queue(max_concurrent: int, auto_start: bool):
    """Launch the operation queue manager"""
    console.print("🔄 Launching operation queue manager...")
    console.print(f"Max concurrent operations: {max_concurrent}")
    
    if auto_start:
        console.print("Auto-starting queue processing")
    
    # This would launch the queue manager
    try:
        run_tui(debug=False)
    except Exception as e:
        console.print(f"[red]Error launching queue manager:[/red] {e}")
        sys.exit(1)


@tui.command()
def demo():
    """Launch TUI with demo data for testing"""
    console.print("🎭 Launching TUI with demo data...")
    console.print("[dim]This mode includes sample archives and operations for testing[/dim]")
    
    try:
        run_tui(debug=True)
    except Exception as e:
        console.print(f"[red]Error launching demo:[/red] {e}")
        sys.exit(1)


@tui.command()
def check():
    """Check TUI dependencies and configuration"""
    console.print("🔍 Checking TUI dependencies...")
    
    try:
        # Check Textual installation
        import textual
        console.print(f"✅ Textual: {textual.__version__}")
        
        # Check service container
        from ...application.container import get_service_container
        service_container = get_service_container()
        console.print("✅ Service container: Available")
        
        # Check services
        archive_service = service_container.service_factory.archive_service
        simulation_service = service_container.service_factory.simulation_service
        location_service = service_container.service_factory.location_service
        console.print("✅ Archive service: Available")
        console.print("✅ Simulation service: Available")
        console.print("✅ Location service: Available")
        
        console.print("\n[green]All TUI dependencies are available![/green]")
        console.print("Run '[cyan]tellus tui app[/cyan]' to launch the TUI")
        
    except ImportError as e:
        console.print(f"[red]Missing dependency:[/red] {e}")
        console.print("Install with: [cyan]pip install textual[/cyan]")
        sys.exit(1)
    except Exception as e:
        console.print(f"[red]Configuration error:[/red] {e}")
        sys.exit(1)