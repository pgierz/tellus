"""
Layout components for the Tellus Web UI.

This module provides the main layout structure including navigation,
sidebar, and page containers.
"""

import reflex as rx
from ..state import AppState


def navbar() -> rx.Component:
    """Top navigation bar."""
    return rx.hstack(
        rx.hstack(
            rx.icon("menu", size=24, on_click=AppState.toggle_sidebar),
            rx.heading("Tellus", size="6", color="blue.9"),
            rx.badge("Climate Data Management", variant="soft", color="gray"),
            align="center",
            spacing="3"
        ),
        rx.hstack(
            rx.icon("bell", size=20),
            rx.icon("settings", size=20),
            rx.avatar(fallback="PG", size="2"),
            align="center",
            spacing="3"
        ),
        justify="between",
        align="center",
        width="100%",
        padding="1rem",
        border_bottom="1px solid var(--gray-6)",
        background="var(--color-panel)"
    )


def sidebar_item(icon: str, label: str, route: str, is_active: bool = False) -> rx.Component:
    """Individual sidebar navigation item."""
    return rx.link(
        rx.hstack(
            rx.icon(icon, size=18),
            rx.text(label, size="2", weight="medium"),
            align="center",
            spacing="3",
            padding="0.75rem 1rem",
            border_radius="6px",
            background=rx.cond(is_active, "var(--blue-9)", "transparent"),
            color=rx.cond(is_active, "white", "var(--gray-11)"),
            _hover={"background": rx.cond(is_active, "var(--blue-9)", "var(--gray-3)")},
            width="100%"
        ),
        href=route,
        text_decoration="none",
        width="100%"
    )


def sidebar() -> rx.Component:
    """Main sidebar navigation."""
    return rx.vstack(
        rx.vstack(
            sidebar_item("home", "Dashboard", "/", True),
            sidebar_item("database", "Simulations", "/simulations"),
            sidebar_item("map-pin", "Locations", "/locations"),  
            sidebar_item("folder", "Files", "/files"),
            sidebar_item("workflow", "Workflows", "/workflows"),
            spacing="1",
            width="100%"
        ),
        rx.spacer(),
        rx.vstack(
            rx.divider(),
            sidebar_item("message-circle", "Chat Assistant", "/chat"),
            sidebar_item("help-circle", "Help", "/help"),
            spacing="1",
            width="100%"
        ),
        padding="1rem",
        width="250px",
        height="100vh",
        border_right="1px solid var(--gray-6)",
        background="var(--color-panel)",
        position="fixed",
        left="0",
        top="60px",
        overflow_y="auto",
        display=rx.cond(AppState.sidebar_collapsed, "none", "flex")
    )


def main_content(content: rx.Component) -> rx.Component:
    """Main content area wrapper."""
    return rx.box(
        content,
        margin_left=rx.cond(AppState.sidebar_collapsed, "0", "250px"),
        padding="2rem",
        min_height="calc(100vh - 60px)",
        background="var(--color-background)",
        transition="margin-left 0.2s ease"
    )


def notification_toast() -> rx.Component:
    """Notification toast for success/error messages."""
    return rx.cond(
        AppState.error_message != "",
        rx.alert(
            rx.alert_icon(),
            rx.alert_title("Error"),
            rx.alert_description(AppState.error_message),
            status="error",
            position="fixed",
            top="80px",
            right="20px",
            z_index="1000",
            width="400px"
        ),
        rx.cond(
            AppState.success_message != "",
            rx.alert(
                rx.alert_icon(),
                rx.alert_title("Success"),
                rx.alert_description(AppState.success_message),
                status="success",
                position="fixed",
                top="80px", 
                right="20px",
                z_index="1000",
                width="400px"
            )
        )
    )


def base_layout(page_content: rx.Component) -> rx.Component:
    """Base layout wrapper for all pages."""
    return rx.box(
        navbar(),
        sidebar(),
        main_content(page_content),
        notification_toast(),
        min_height="100vh",
        background="var(--color-background)"
    )