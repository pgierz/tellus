"""
Location card component for displaying location information.
"""

import reflex as rx
from ...application.dtos import LocationDto
from datetime import datetime


def connection_status(is_accessible: bool, is_remote: bool) -> rx.Component:
    """Connection status indicator."""
    if is_accessible:
        return rx.hstack(
            rx.icon("check-circle", size=16, color="green.9"),
            rx.text("Connected", size="1", color="green.9"),
            align="center",
            spacing="1"
        )
    else:
        return rx.hstack(
            rx.icon("x-circle", size=16, color="red.9"), 
            rx.text("Disconnected", size="1", color="red.9"),
            align="center",
            spacing="1"
        )


def protocol_badge(protocol: str) -> rx.Component:
    """Protocol badge with appropriate styling."""
    color_map = {
        "ssh": "blue",
        "sftp": "blue", 
        "file": "green",
        "s3": "orange",
        "gcs": "orange"
    }
    color = color_map.get(protocol.lower(), "gray")
    
    return rx.badge(
        protocol.upper(),
        variant="outline", 
        color_scheme=color,
        size="1"
    )


def location_card(location: LocationDto) -> rx.Component:
    """Card component for displaying location information."""
    
    # Format last verified time
    last_verified = "Never"
    if location.last_verified:
        try:
            dt = datetime.fromisoformat(location.last_verified.replace('Z', '+00:00'))
            last_verified = dt.strftime("%Y-%m-%d %H:%M")
        except:
            last_verified = "Unknown"
    
    return rx.card(
        rx.vstack(
            rx.hstack(
                rx.hstack(
                    rx.icon("server" if location.is_remote else "folder", size=20),
                    rx.heading(location.name, size="4", weight="bold"),
                    align="center",
                    spacing="2"
                ),
                connection_status(location.is_accessible or False, location.is_remote),
                justify="between",
                align="center",
                width="100%"
            ),
            rx.vstack(
                rx.hstack(
                    protocol_badge(location.protocol),
                    *[rx.badge(kind, variant="soft", size="1") for kind in location.kinds],
                    spacing="2",
                    wrap="wrap"
                ),
                rx.text(
                    f"Path: {location.path or 'Not specified'}",
                    size="2",
                    color="gray.11", 
                    font_family="mono"
                ),
                rx.cond(
                    location.storage_options.get("host"),
                    rx.text(
                        f"Host: {location.storage_options.get('host', '')}",
                        size="2",
                        color="gray.11"
                    )
                ),
                align="start",
                spacing="1",
                width="100%"
            ),
            rx.text(
                f"Last verified: {last_verified}",
                size="1",
                color="gray.9"
            ),
            rx.hstack(
                rx.button(
                    rx.icon("eye", size=16),
                    "View Details", 
                    size="2",
                    variant="soft"
                ),
                rx.button(
                    rx.icon("refresh-cw", size=16),
                    "Test Connection",
                    size="2",
                    variant="outline"
                ),
                rx.button(
                    rx.icon("settings", size=16),
                    size="2",
                    variant="ghost"
                ),
                justify="start",
                spacing="2",
                width="100%"
            ),
            align="start",
            spacing="3",
            width="100%"
        ),
        padding="1rem",
        width="100%",
        _hover={"box_shadow": "var(--shadow-3)"},
        transition="box-shadow 0.2s ease"
    )