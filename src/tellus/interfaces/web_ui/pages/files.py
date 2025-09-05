"""
Files page for the Tellus Web UI.

This page provides file discovery, browsing, and management
across distributed storage locations.
"""

import reflex as rx
from ..components.layout import base_layout
from ..state import AppState


def file_browser_item(name: str, size: str, modified: str, file_type: str) -> rx.Component:
    """Individual file browser item."""
    
    icon_map = {
        "folder": "folder",
        "netcdf": "file-bar-chart",
        "text": "file-text",
        "archive": "archive", 
        "binary": "file"
    }
    icon = icon_map.get(file_type, "file")
    
    return rx.hstack(
        rx.checkbox(),
        rx.icon(icon, size=18, color="blue.9"),
        rx.text(name, size="2", weight="medium"),
        rx.spacer(),
        rx.text(size, size="1", color="gray.9", width="80px"),
        rx.text(modified, size="1", color="gray.9", width="120px"),
        rx.button(
            rx.icon("more-horizontal", size=16),
            size="1",
            variant="ghost"
        ),
        align="center",
        padding="0.5rem 1rem",
        width="100%",
        border_bottom="1px solid var(--gray-3)",
        _hover={"background": "var(--gray-2)"}
    )


def file_browser() -> rx.Component:
    """File browser component."""
    return rx.card(
        rx.vstack(
            rx.hstack(
                rx.breadcrumb(
                    rx.breadcrumb_item("Home", href="/"),
                    rx.breadcrumb_item("Simulations", href="/simulations"),
                    rx.breadcrumb_item("fesom-test-001"),
                    rx.breadcrumb_item("output", is_current_page=True)
                ),
                rx.spacer(),
                rx.hstack(
                    rx.button(
                        rx.icon("grid-3x3", size=16),
                        variant="ghost",
                        size="2"
                    ),
                    rx.button(
                        rx.icon("list", size=16),
                        variant="ghost", 
                        size="2"
                    ),
                    spacing="1"
                ),
                align="center",
                width="100%"
            ),
            
            rx.hstack(
                rx.text("Name", size="2", weight="bold"),
                rx.spacer(),
                rx.text("Size", size="2", weight="bold", width="80px"),
                rx.text("Modified", size="2", weight="bold", width="120px"),
                rx.box(width="40px"),  # For actions menu
                align="center",
                padding="0.5rem 1rem",
                border_bottom="2px solid var(--gray-4)",
                width="100%"
            ),
            
            rx.vstack(
                file_browser_item("ocean", "—", "2025-09-01", "folder"),
                file_browser_item("atmosphere", "—", "2025-09-01", "folder"), 
                file_browser_item("ice", "—", "2025-09-01", "folder"),
                file_browser_item("fesom.ocean.mean.1850.nc", "2.4 GB", "2025-09-01 15:30", "netcdf"),
                file_browser_item("fesom.ice.daily.1850.nc", "890 MB", "2025-09-01 15:28", "netcdf"),
                file_browser_item("restart.fesom.1850.tar.gz", "15 GB", "2025-09-01 14:45", "archive"),
                file_browser_item("run.log", "45 KB", "2025-09-01 16:22", "text"),
                spacing="0",
                width="100%"
            ),
            
            align="start",
            spacing="0",
            width="100%"
        ),
        padding="0",
        width="100%"
    )


def file_operations_panel() -> rx.Component:
    """File operations and metadata panel."""
    return rx.vstack(
        rx.card(
            rx.vstack(
                rx.heading("File Operations", size="4"),
                rx.vstack(
                    rx.button(
                        rx.icon("download", size=16),
                        "Download Selected",
                        width="100%",
                        variant="outline"
                    ),
                    rx.button(
                        rx.icon("copy", size=16), 
                        "Copy to Location",
                        width="100%",
                        variant="outline"
                    ),
                    rx.button(
                        rx.icon("archive", size=16),
                        "Create Archive", 
                        width="100%",
                        variant="outline"
                    ),
                    rx.button(
                        rx.icon("trash-2", size=16),
                        "Delete Selected",
                        width="100%",
                        variant="outline",
                        color_scheme="red"
                    ),
                    spacing="2",
                    width="100%"
                ),
                align="start",
                spacing="3",
                width="100%"
            ),
            padding="1.5rem",
            width="100%"
        ),
        
        rx.card(
            rx.vstack(
                rx.heading("Quick Filters", size="4"),
                rx.vstack(
                    rx.checkbox("NetCDF files (.nc)", checked=True),
                    rx.checkbox("Archive files (.tar.gz)"),
                    rx.checkbox("Log files (.log)"),
                    rx.checkbox("Restart files"),
                    rx.checkbox("Large files (>1GB)"),
                    align="start",
                    spacing="2"
                ),
                align="start",
                spacing="3",
                width="100%"
            ),
            padding="1.5rem",
            width="100%"
        ),
        
        spacing="1rem",
        width="100%"
    )


@rx.page(route="/files", title="Files - Tellus")
def page() -> rx.Component:
    """File discovery and management page."""
    
    return base_layout(
        rx.vstack(
            rx.hstack(
                rx.heading("File Browser", size="8"),
                rx.spacer(),
                rx.hstack(
                    rx.select(
                        ["All Locations", "mistral", "levante", "local-cache"],
                        value="mistral",
                        size="2"
                    ),
                    rx.input(
                        placeholder="Search files...",
                        size="2",
                        width="250px"
                    ),
                    rx.button(
                        rx.icon("search", size=16),
                        "Search",
                        variant="outline"
                    ),
                    spacing="2"
                ),
                align="center",
                width="100%"
            ),
            
            rx.text(
                "Browsing: mistral:/work/ab0246/a270124/fesom-test-001/output",
                size="2",
                color="gray.11",
                font_family="mono"
            ),
            
            rx.grid(
                file_browser(),
                file_operations_panel(),
                columns="1fr 300px",
                gap="2rem",
                width="100%",
                align="start"
            ),
            
            align="start",
            spacing="4",
            width="100%"
        )
    )