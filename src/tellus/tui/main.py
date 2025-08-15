"""Main entry point for the Tellus TUI."""

import asyncio
import sys
from typing import Optional

from .app import TellusTUIApp


def run_tui(archive_id: Optional[str] = None, 
           simulation_id: Optional[str] = None,
           debug: bool = False) -> None:
    """Run the Tellus TUI application.
    
    Args:
        archive_id: Optional archive ID to focus on startup
        simulation_id: Optional simulation ID to filter by
        debug: Enable debug mode for development
    """
    try:
        app = TellusTUIApp()
        
        # Set initial context if provided
        if archive_id:
            app.selected_archive = archive_id
        if simulation_id:
            app.current_simulation = simulation_id
            
        # Configure debug mode
        if debug:
            app.title += " [DEBUG]"
        
        app.run()
        
    except KeyboardInterrupt:
        print("\nTUI application interrupted by user.")
        sys.exit(0)
    except Exception as e:
        print(f"Error running TUI application: {e}")
        if debug:
            raise
        sys.exit(1)


async def run_tui_async(archive_id: Optional[str] = None,
                       simulation_id: Optional[str] = None,
                       debug: bool = False) -> None:
    """Run the TUI application asynchronously.
    
    This is useful for integration with async applications.
    """
    try:
        app = TellusTUIApp()
        
        if archive_id:
            app.selected_archive = archive_id
        if simulation_id:
            app.current_simulation = simulation_id
            
        if debug:
            app.title += " [DEBUG]"
        
        await app.run_async()
        
    except KeyboardInterrupt:
        print("\nTUI application interrupted by user.")
    except Exception as e:
        print(f"Error running TUI application: {e}")
        if debug:
            raise


if __name__ == "__main__":
    # Simple CLI for testing
    import argparse
    
    parser = argparse.ArgumentParser(description="Tellus TUI Application")
    parser.add_argument("--archive", help="Archive ID to focus on")
    parser.add_argument("--simulation", help="Simulation ID to filter by")
    parser.add_argument("--debug", action="store_true", help="Enable debug mode")
    
    args = parser.parse_args()
    
    run_tui(
        archive_id=args.archive,
        simulation_id=args.simulation,
        debug=args.debug
    )