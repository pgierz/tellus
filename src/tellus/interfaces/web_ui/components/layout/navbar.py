"""
Navigation bar component for the Tellus web UI.

Provides main navigation, search, and user actions.
"""

# Uncomment when Reflex is available
# import reflex as rx
from typing import List, Dict, Any


def navbar():
    """
    Main navigation bar component.
    
    When Reflex is available, this will return a proper Reflex component.
    For now, this serves as a blueprint.
    """
    
    # This is the structure that would be implemented with Reflex:
    component_structure = {
        "type": "navbar",
        "props": {
            "className": "bg-white border-b border-gray-200 px-4 py-3",
            "style": {"position": "sticky", "top": 0, "z-index": 50}
        },
        "children": [
            {
                "type": "container",
                "props": {"className": "flex items-center justify-between"},
                "children": [
                    # Logo and brand
                    {
                        "type": "brand",
                        "children": [
                            {
                                "type": "logo",
                                "props": {"src": "/logo.svg", "alt": "Tellus"}
                            },
                            {
                                "type": "title", 
                                "props": {"className": "text-xl font-semibold text-gray-900"},
                                "text": "Tellus"
                            }
                        ]
                    },
                    
                    # Navigation links
                    {
                        "type": "nav_links",
                        "props": {"className": "hidden md:flex space-x-8"},
                        "children": [
                            {"type": "link", "props": {"href": "/"}, "text": "Dashboard"},
                            {"type": "link", "props": {"href": "/simulations"}, "text": "Simulations"},
                            {"type": "link", "props": {"href": "/locations"}, "text": "Locations"},
                            {"type": "link", "props": {"href": "/files"}, "text": "Files"},
                            {"type": "link", "props": {"href": "/workflows"}, "text": "Workflows"}
                        ]
                    },
                    
                    # Search bar
                    {
                        "type": "search",
                        "props": {
                            "className": "hidden md:block w-96",
                            "placeholder": "Search simulations, files, locations..."
                        }
                    },
                    
                    # User actions
                    {
                        "type": "actions",
                        "props": {"className": "flex items-center space-x-4"},
                        "children": [
                            # Chat toggle button
                            {
                                "type": "chat_toggle",
                                "props": {
                                    "className": "p-2 text-gray-500 hover:text-blue-600 transition-colors",
                                    "title": "AI Assistant"
                                },
                                "icon": "💬"
                            },
                            
                            # Notifications
                            {
                                "type": "notifications",
                                "props": {
                                    "className": "p-2 text-gray-500 hover:text-blue-600 transition-colors",
                                    "title": "Notifications"
                                },
                                "icon": "🔔"
                            },
                            
                            # Settings
                            {
                                "type": "settings",
                                "props": {
                                    "className": "p-2 text-gray-500 hover:text-blue-600 transition-colors",
                                    "title": "Settings"
                                },
                                "icon": "⚙️"
                            },
                            
                            # User menu
                            {
                                "type": "user_menu",
                                "props": {"className": "relative"},
                                "children": [
                                    {
                                        "type": "avatar",
                                        "props": {
                                            "className": "w-8 h-8 rounded-full bg-blue-600 text-white flex items-center justify-center"
                                        },
                                        "text": "U"
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }
        ]
    }
    
    return component_structure


def mobile_nav():
    """Mobile navigation component."""
    return {
        "type": "mobile_nav",
        "props": {
            "className": "md:hidden bg-white border-t border-gray-200 px-4 py-3"
        },
        "children": [
            {
                "type": "nav_links",
                "props": {"className": "flex justify-around"},
                "children": [
                    {"type": "link", "props": {"href": "/"}, "text": "🏠", "title": "Dashboard"},
                    {"type": "link", "props": {"href": "/simulations"}, "text": "🔬", "title": "Simulations"},
                    {"type": "link", "props": {"href": "/locations"}, "text": "📍", "title": "Locations"},
                    {"type": "link", "props": {"href": "/files"}, "text": "📁", "title": "Files"},
                    {"type": "link", "props": {"href": "/workflows"}, "text": "⚙️", "title": "Workflows"}
                ]
            }
        ]
    }


def breadcrumb_nav(items: List[Dict[str, str]]):
    """Breadcrumb navigation component."""
    return {
        "type": "breadcrumb",
        "props": {
            "className": "flex items-center space-x-2 text-sm text-gray-600 mb-4"
        },
        "children": [
            {
                "type": "breadcrumb_item",
                "props": {"key": i},
                "children": [
                    {
                        "type": "link" if item.get("href") else "span",
                        "props": {
                            "href": item.get("href"),
                            "className": "hover:text-blue-600" if item.get("href") else "text-gray-900 font-medium"
                        },
                        "text": item["label"]
                    },
                    {"type": "separator", "text": "/"} if i < len(items) - 1 else None
                ]
            }
            for i, item in enumerate(items)
        ]
    }


# When Reflex is available, these will be proper components:
"""
import reflex as rx

def navbar() -> rx.Component:
    return rx.box(
        rx.hstack(
            # Logo and brand
            rx.hstack(
                rx.image(src="/logo.svg", width="32px", height="32px"),
                rx.heading("Tellus", size="lg", color="gray.900"),
                spacing="2"
            ),
            
            # Navigation links
            rx.hstack(
                rx.link("Dashboard", href="/", color="gray.600", _hover={"color": "blue.600"}),
                rx.link("Simulations", href="/simulations", color="gray.600", _hover={"color": "blue.600"}), 
                rx.link("Locations", href="/locations", color="gray.600", _hover={"color": "blue.600"}),
                rx.link("Files", href="/files", color="gray.600", _hover={"color": "blue.600"}),
                rx.link("Workflows", href="/workflows", color="gray.600", _hover={"color": "blue.600"}),
                spacing="6",
                display=["none", "none", "flex"]
            ),
            
            # Search bar
            rx.input(
                placeholder="Search simulations, files, locations...",
                width="300px",
                display=["none", "none", "block"]
            ),
            
            # Actions
            rx.hstack(
                rx.icon_button(icon="chat", aria_label="AI Assistant"),
                rx.icon_button(icon="bell", aria_label="Notifications"),
                rx.icon_button(icon="settings", aria_label="Settings"),
                rx.avatar(name="User", size="sm"),
                spacing="2"
            ),
            
            justify="between",
            align="center",
            width="100%"
        ),
        bg="white",
        border_bottom="1px solid",
        border_color="gray.200",
        px="4",
        py="3",
        position="sticky",
        top="0",
        z_index="50"
    )
"""