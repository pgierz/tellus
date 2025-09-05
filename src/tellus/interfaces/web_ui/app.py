"""
Main Reflex application for Tellus Web UI.

This provides a modern, responsive web interface for managing climate simulation
data, locations, and workflows.
"""

import reflex as rx
from .pages import (
    index_page,
    simulations_page,
    locations_page,
    files_page
)
from .components.layout import base_layout


def create_app():
    """Create and configure the Reflex app."""
    
    app = rx.App(
        title="Tellus - Climate Data Management",
        description="Modern web interface for the Tellus climate data management system",
        theme=rx.theme(
            appearance="dark",
            has_background=True,
            radius="medium",
            scaling="100%"
        ),
        stylesheets=[
            "https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css"
        ]
    )
    
    # Add routes
    app.add_page(index_page, route="/", title="Dashboard")
    app.add_page(simulations_page, route="/simulations", title="Simulations")
    app.add_page(locations_page, route="/locations", title="Locations") 
    app.add_page(files_page, route="/files", title="Files")
    
    return app


# Create the app instance
app = create_app()