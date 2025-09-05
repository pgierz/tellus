"""
Locations page for the Tellus Web UI.

This page provides management of storage locations including
local filesystems, remote servers, and cloud storage.
"""

import reflex as rx
from ..components.layout import base_layout
from ..components.location_card import location_card
from ..state import AppState


def locations_grid() -> rx.Component:
    """Grid of location cards."""
    return rx.cond(
        AppState.loading,
        rx.center(
            rx.spinner(size="3"),
            min_height="400px"
        ),
        rx.cond(
            AppState.filtered_locations.length() == 0,
            rx.center(
                rx.vstack(
                    rx.icon("map-pin", size=48, color="gray.6"),
                    rx.heading("No locations found", size="5", color="gray.9"),
                    rx.text(
                        "Try adjusting your search or add a new storage location.",
                        size="3",
                        color="gray.7",
                        text_align="center"
                    ),
                    rx.button(
                        rx.icon("plus", size=16),
                        "Add New Location",
                        size="3"
                    ),
                    align="center",
                    spacing="4"
                ),
                min_height="400px"
            ),
            rx.grid(
                rx.foreach(
                    AppState.filtered_locations,
                    location_card
                ),
                columns="repeat(auto-fit, minmax(400px, 1fr))",
                gap="1rem",
                width="100%"
            )
        )
    )


def location_type_filter() -> rx.Component:
    """Filter by location types."""
    return rx.card(
        rx.vstack(
            rx.heading("Location Types", size="4"),
            rx.vstack(
                rx.checkbox("All", checked=True),
                rx.checkbox("Compute Clusters"), 
                rx.checkbox("Storage Systems"),
                rx.checkbox("File Servers"),
                rx.checkbox("Cloud Storage"),
                rx.checkbox("Local Storage"),
                align="start",
                spacing="2"
            ),
            align="start",
            spacing="3",
            width="100%"
        ),
        padding="1.5rem",
        width="100%"
    )


@rx.page(route="/locations", title="Locations - Tellus")
def page() -> rx.Component:
    """Storage locations management page."""
    
    return base_layout(
        rx.vstack(
            rx.hstack(
                rx.heading("Storage Locations", size="8"),
                rx.spacer(), 
                rx.hstack(
                    rx.button(
                        rx.icon("refresh-cw", size=16),
                        "Refresh",
                        on_click=AppState.load_locations,
                        loading=AppState.loading,
                        variant="outline"
                    ),
                    rx.button(
                        rx.icon("wifi", size=16),
                        "Test All",
                        variant="outline"
                    ),
                    rx.button(
                        rx.icon("plus", size=16),
                        "Add Location",
                        variant="solid"
                    ),
                    spacing="2"
                ),
                align="center",
                width="100%"
            ),
            
            rx.hstack(
                rx.input(
                    placeholder="Search locations...",
                    value=AppState.location_search,
                    on_change=AppState.set_location_search,
                    size="3",
                    width="300px"
                ),
                rx.spacer(),
                rx.text(
                    f"Showing {AppState.filtered_locations.length()} location(s)",
                    size="2", 
                    color="gray.11"
                ),
                align="center",
                width="100%"
            ),
            
            rx.grid(
                location_type_filter(),
                locations_grid(),
                columns="250px 1fr",
                gap="2rem",
                width="100%",
                align="start"
            ),
            
            align="start",
            spacing="6",
            width="100%"
        ),
        on_mount=AppState.load_locations
    )