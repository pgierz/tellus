"""
Dashboard page for the Tellus Web UI.

This provides an overview of the system status, recent activity,
and quick access to key functionality.
"""

import reflex as rx
from ..components.layout import base_layout
from ..state import AppState


def stats_card(title: str, value: str, icon: str, color: str = "blue") -> rx.Component:
    """Statistics card component."""
    return rx.card(
        rx.vstack(
            rx.hstack(
                rx.icon(icon, size=24, color=f"{color}.9"),
                rx.spacer(),
                align="center",
                width="100%"
            ),
            rx.vstack(
                rx.text(value, size="6", weight="bold"),
                rx.text(title, size="2", color="gray.11"),
                align="start",
                spacing="1"
            ),
            align="start",
            spacing="3",
            width="100%"
        ),
        padding="1.5rem",
        width="100%"
    )


def recent_activity_item(icon: str, title: str, description: str, time: str) -> rx.Component:
    """Recent activity item."""
    return rx.hstack(
        rx.icon(icon, size=20, color="blue.9"),
        rx.vstack(
            rx.text(title, size="3", weight="medium"),
            rx.text(description, size="2", color="gray.11"),
            align="start",
            spacing="0"
        ),
        rx.spacer(),
        rx.text(time, size="1", color="gray.9"),
        align="center",
        spacing="3",
        width="100%",
        padding="1rem",
        border_bottom="1px solid var(--gray-4)"
    )


def quick_action_button(icon: str, title: str, description: str, route: str) -> rx.Component:
    """Quick action button."""
    return rx.link(
        rx.card(
            rx.vstack(
                rx.icon(icon, size=32, color="blue.9"),
                rx.vstack(
                    rx.text(title, size="4", weight="bold", text_align="center"),
                    rx.text(description, size="2", color="gray.11", text_align="center"),
                    align="center",
                    spacing="1"
                ),
                align="center",
                spacing="3",
                width="100%"
            ),
            padding="2rem",
            width="100%",
            _hover={"box_shadow": "var(--shadow-3)", "transform": "translateY(-2px)"},
            transition="all 0.2s ease",
            cursor="pointer"
        ),
        href=route,
        text_decoration="none"
    )


@rx.page(route="/", title="Tellus - Dashboard")
def page() -> rx.Component:
    """Main dashboard page."""
    
    return base_layout(
        rx.vstack(
            rx.hstack(
                rx.heading("Dashboard", size="8"),
                rx.spacer(),
                rx.button(
                    rx.icon("refresh-cw", size=16),
                    "Refresh",
                    on_click=lambda: [AppState.load_simulations(), AppState.load_locations()],
                    loading=AppState.loading,
                    variant="outline"
                ),
                align="center",
                width="100%"
            ),
            
            # Statistics Cards
            rx.grid(
                stats_card("Active Simulations", "2", "database", "blue"),
                stats_card("Storage Locations", "3", "map-pin", "green"),
                stats_card("Running Workflows", "1", "workflow", "orange"),
                stats_card("Cached Files", "156", "folder", "purple"),
                columns="4",
                gap="1rem",
                width="100%"
            ),
            
            rx.grid(
                # Recent Activity
                rx.card(
                    rx.vstack(
                        rx.heading("Recent Activity", size="5"),
                        rx.vstack(
                            recent_activity_item(
                                "play-circle", 
                                "Workflow Started", 
                                "FESOM preprocessing workflow initiated",
                                "2 hours ago"
                            ),
                            recent_activity_item(
                                "check-circle", 
                                "Simulation Completed", 
                                "ICON-ESM-LR historical run finished",
                                "5 hours ago"
                            ),
                            recent_activity_item(
                                "upload", 
                                "Files Archived", 
                                "Output files moved to long-term storage",
                                "1 day ago"
                            ),
                            recent_activity_item(
                                "server", 
                                "Location Added", 
                                "New compute cluster 'mistral' connected",
                                "2 days ago"
                            ),
                            spacing="0",
                            width="100%"
                        ),
                        align="start",
                        spacing="3",
                        width="100%"
                    ),
                    padding="1.5rem",
                    width="100%"
                ),
                
                # Quick Actions
                rx.card(
                    rx.vstack(
                        rx.heading("Quick Actions", size="5"),
                        rx.grid(
                            quick_action_button(
                                "plus-circle", 
                                "New Simulation", 
                                "Create a new climate simulation",
                                "/simulations/new"
                            ),
                            quick_action_button(
                                "map-pin", 
                                "Add Location", 
                                "Connect a new storage location",
                                "/locations/new"
                            ),
                            quick_action_button(
                                "play", 
                                "Run Workflow", 
                                "Execute analysis workflows",
                                "/workflows"
                            ),
                            quick_action_button(
                                "message-circle", 
                                "Chat Assistant", 
                                "Get help with natural language",
                                "/chat"
                            ),
                            columns="2",
                            gap="1rem",
                            width="100%"
                        ),
                        align="start",
                        spacing="4",
                        width="100%"
                    ),
                    padding="1.5rem",
                    width="100%"
                ),
                
                columns="2",
                gap="1rem", 
                width="100%"
            ),
            
            align="start",
            spacing="6",
            width="100%"
        ),
        on_mount=[AppState.load_simulations, AppState.load_locations]
    )