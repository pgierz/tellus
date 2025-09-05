"""
Simulations page for the Tellus Web UI.

This page provides a comprehensive view of all climate simulations,
with search, filtering, and management capabilities.
"""

import reflex as rx
from ..components.layout import base_layout
from ..components.simulation_card import simulation_card
from ..state import AppState


def simulation_filters() -> rx.Component:
    """Filters panel for simulations."""
    return rx.card(
        rx.vstack(
            rx.hstack(
                rx.icon("filter", size=20),
                rx.heading("Filters", size="4"),
                align="center",
                spacing="2"
            ),
            rx.vstack(
                rx.vstack(
                    rx.text("Status", size="2", weight="medium"),
                    rx.select(
                        ["all", "running", "completed", "failed", "pending"],
                        value=AppState.simulation_status_filter,
                        on_change=AppState.set_simulation_status_filter,
                        size="2",
                        width="100%"
                    ),
                    align="start",
                    spacing="1",
                    width="100%"
                ),
                rx.vstack(
                    rx.text("Model", size="2", weight="medium"),
                    rx.select(
                        ["all", "FESOM2", "ICON-ESM", "MPI-ESM", "AWI-CM"],
                        size="2",
                        width="100%"
                    ),
                    align="start",
                    spacing="1",
                    width="100%"
                ),
                rx.vstack(
                    rx.text("Experiment", size="2", weight="medium"), 
                    rx.select(
                        ["all", "historical", "ssp585", "ssp245", "piControl"],
                        size="2",
                        width="100%"
                    ),
                    align="start",
                    spacing="1",
                    width="100%"
                ),
                rx.button(
                    "Clear Filters",
                    variant="outline",
                    size="2",
                    width="100%"
                ),
                align="start",
                spacing="3",
                width="100%"
            ),
            align="start",
            spacing="4",
            width="100%"
        ),
        padding="1.5rem",
        width="100%"
    )


def simulations_grid() -> rx.Component:
    """Grid of simulation cards.""" 
    return rx.cond(
        AppState.loading,
        rx.center(
            rx.spinner(size="3"),
            min_height="400px"
        ),
        rx.cond(
            AppState.filtered_simulations.length() == 0,
            rx.center(
                rx.vstack(
                    rx.icon("database", size=48, color="gray.6"),
                    rx.heading("No simulations found", size="5", color="gray.9"),
                    rx.text(
                        "Try adjusting your search or filters, or create a new simulation.",
                        size="3",
                        color="gray.7",
                        text_align="center"
                    ),
                    rx.button(
                        rx.icon("plus", size=16),
                        "Create New Simulation",
                        size="3"
                    ),
                    align="center",
                    spacing="4"
                ),
                min_height="400px"
            ),
            rx.grid(
                rx.foreach(
                    AppState.filtered_simulations,
                    simulation_card
                ),
                columns="1",
                gap="1rem", 
                width="100%"
            )
        )
    )


@rx.page(route="/simulations", title="Simulations - Tellus")
def page() -> rx.Component:
    """Simulations management page."""
    
    return base_layout(
        rx.vstack(
            rx.hstack(
                rx.heading("Simulations", size="8"),
                rx.spacer(),
                rx.hstack(
                    rx.button(
                        rx.icon("refresh-cw", size=16),
                        "Refresh",
                        on_click=AppState.load_simulations,
                        loading=AppState.loading,
                        variant="outline"
                    ),
                    rx.button(
                        rx.icon("plus", size=16),
                        "New Simulation",
                        variant="solid"
                    ),
                    spacing="2"
                ),
                align="center",
                width="100%"
            ),
            
            rx.hstack(
                rx.input(
                    placeholder="Search simulations...",
                    value=AppState.simulation_search,
                    on_change=AppState.set_simulation_search,
                    size="3",
                    width="300px"
                ),
                rx.spacer(),
                rx.text(
                    f"Showing {AppState.filtered_simulations.length()} simulation(s)",
                    size="2",
                    color="gray.11"
                ),
                align="center",
                width="100%"
            ),
            
            rx.grid(
                simulation_filters(),
                simulations_grid(),
                columns="300px 1fr",
                gap="2rem",
                width="100%",
                align="start"
            ),
            
            align="start",
            spacing="6",
            width="100%"
        ),
        on_mount=AppState.load_simulations
    )