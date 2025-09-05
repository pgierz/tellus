"""
Simulation card component for displaying simulation information in lists and grids.
"""

# Uncomment when Reflex is available
# import reflex as rx
from typing import Dict, Any, List
from datetime import datetime


def simulation_card(simulation: Dict[str, Any]):
    """
    Card component for displaying a single simulation.
    
    When Reflex is available, this will return a proper Reflex component.
    For now, this serves as a blueprint.
    """
    
    # Extract key information
    sim_id = simulation.get("simulation_id", "Unknown")
    model = simulation.get("model_id", "Unknown Model")
    status = simulation.get("status", "unknown")
    attrs = simulation.get("attributes", {})
    locations = simulation.get("locations", {})
    
    # Format attributes for display
    experiment = attrs.get("experiment", "N/A")
    resolution = attrs.get("resolution", "N/A")
    start_year = attrs.get("start_year")
    end_year = attrs.get("end_year")
    
    date_range = "N/A"
    if start_year and end_year:
        date_range = f"{start_year}-{end_year}"
    
    # Status styling
    status_colors = {
        "completed": {"bg": "green.100", "text": "green.800", "icon": "✅"},
        "running": {"bg": "blue.100", "text": "blue.800", "icon": "🔄"},
        "failed": {"bg": "red.100", "text": "red.800", "icon": "❌"},
        "paused": {"bg": "yellow.100", "text": "yellow.800", "icon": "⏸️"},
        "created": {"bg": "gray.100", "text": "gray.800", "icon": "📝"},
        "unknown": {"bg": "gray.100", "text": "gray.500", "icon": "❓"}
    }
    
    status_style = status_colors.get(status, status_colors["unknown"])
    
    component_structure = {
        "type": "simulation_card",
        "props": {
            "className": "bg-white rounded-lg border border-gray-200 p-6 hover:shadow-md transition-shadow cursor-pointer",
            "onClick": f"selectSimulation('{sim_id}')"
        },
        "children": [
            # Header with simulation ID and status
            {
                "type": "header",
                "props": {"className": "flex items-center justify-between mb-3"},
                "children": [
                    {
                        "type": "title",
                        "props": {"className": "text-lg font-semibold text-gray-900"},
                        "text": sim_id
                    },
                    {
                        "type": "status_badge", 
                        "props": {
                            "className": f"px-2 py-1 rounded-full text-xs font-medium flex items-center space-x-1",
                            "style": {
                                "backgroundColor": status_style["bg"],
                                "color": status_style["text"]
                            }
                        },
                        "children": [
                            {"type": "icon", "text": status_style["icon"]},
                            {"type": "text", "text": status.title()}
                        ]
                    }
                ]
            },
            
            # Model and experiment info
            {
                "type": "info_grid",
                "props": {"className": "grid grid-cols-2 gap-4 mb-4"},
                "children": [
                    {
                        "type": "info_item",
                        "children": [
                            {"type": "label", "props": {"className": "text-sm text-gray-500"}, "text": "Model"},
                            {"type": "value", "props": {"className": "text-sm font-medium text-gray-900"}, "text": model}
                        ]
                    },
                    {
                        "type": "info_item", 
                        "children": [
                            {"type": "label", "props": {"className": "text-sm text-gray-500"}, "text": "Experiment"},
                            {"type": "value", "props": {"className": "text-sm font-medium text-gray-900"}, "text": experiment}
                        ]
                    },
                    {
                        "type": "info_item",
                        "children": [
                            {"type": "label", "props": {"className": "text-sm text-gray-500"}, "text": "Period"},
                            {"type": "value", "props": {"className": "text-sm font-medium text-gray-900"}, "text": date_range}
                        ]
                    },
                    {
                        "type": "info_item",
                        "children": [
                            {"type": "label", "props": {"className": "text-sm text-gray-500"}, "text": "Resolution"},
                            {"type": "value", "props": {"className": "text-sm font-medium text-gray-900"}, "text": resolution}
                        ]
                    }
                ]
            },
            
            # Location badges
            {
                "type": "locations",
                "props": {"className": "mb-4"},
                "children": [
                    {"type": "label", "props": {"className": "text-sm text-gray-500 mb-2 block"}, "text": "Locations:"},
                    {
                        "type": "location_badges",
                        "props": {"className": "flex flex-wrap gap-2"},
                        "children": [
                            {
                                "type": "location_badge",
                                "props": {
                                    "key": loc_name,
                                    "className": "px-2 py-1 bg-blue-100 text-blue-800 text-xs rounded-full"
                                },
                                "text": loc_name
                            }
                            for loc_name in locations.keys()
                        ] if locations else [
                            {"type": "no_locations", "props": {"className": "text-xs text-gray-400"}, "text": "No locations configured"}
                        ]
                    }
                ] if locations else []
            },
            
            # Actions
            {
                "type": "actions",
                "props": {"className": "flex items-center justify-between pt-4 border-t border-gray-100"},
                "children": [
                    {
                        "type": "metadata",
                        "props": {"className": "text-xs text-gray-400"},
                        "text": f"UID: {simulation.get('uid', 'Unknown')[:8]}..."
                    },
                    {
                        "type": "action_buttons",
                        "props": {"className": "flex space-x-2"},
                        "children": [
                            {
                                "type": "view_button",
                                "props": {
                                    "className": "text-blue-600 hover:text-blue-800 text-sm font-medium",
                                    "onClick": f"viewSimulation('{sim_id}')"
                                },
                                "text": "View"
                            },
                            {
                                "type": "edit_button",
                                "props": {
                                    "className": "text-gray-600 hover:text-gray-800 text-sm font-medium",
                                    "onClick": f"editSimulation('{sim_id}')"
                                },
                                "text": "Edit"
                            }
                        ]
                    }
                ]
            }
        ]
    }
    
    return component_structure


def simulation_list_item(simulation: Dict[str, Any], compact: bool = False):
    """List item variant of simulation display."""
    
    sim_id = simulation.get("simulation_id", "Unknown")
    model = simulation.get("model_id", "Unknown Model")
    status = simulation.get("status", "unknown")
    attrs = simulation.get("attributes", {})
    
    status_icons = {
        "completed": "✅",
        "running": "🔄", 
        "failed": "❌",
        "paused": "⏸️",
        "created": "📝",
        "unknown": "❓"
    }
    
    return {
        "type": "simulation_list_item",
        "props": {
            "className": "flex items-center justify-between py-3 px-4 border-b border-gray-100 hover:bg-gray-50 cursor-pointer",
            "onClick": f"selectSimulation('{sim_id}')"
        },
        "children": [
            {
                "type": "content",
                "props": {"className": "flex items-center space-x-4"},
                "children": [
                    {
                        "type": "status_icon",
                        "props": {"className": "text-lg"},
                        "text": status_icons.get(status, "❓")
                    },
                    {
                        "type": "info",
                        "children": [
                            {
                                "type": "title",
                                "props": {"className": "font-medium text-gray-900"},
                                "text": sim_id
                            },
                            {
                                "type": "subtitle",
                                "props": {"className": "text-sm text-gray-500"},
                                "text": f"{model} • {attrs.get('experiment', 'N/A')}"
                            } if not compact else None
                        ]
                    }
                ]
            },
            {
                "type": "actions",
                "props": {"className": "flex items-center space-x-2"},
                "children": [
                    {
                        "type": "status",
                        "props": {"className": "text-sm text-gray-500"},
                        "text": status.title()
                    },
                    {
                        "type": "arrow",
                        "props": {"className": "text-gray-400"},
                        "text": "→"
                    }
                ]
            }
        ]
    }


# When Reflex is available, these will be proper components:
"""
import reflex as rx

def simulation_card(simulation: Dict[str, Any]) -> rx.Component:
    sim_id = simulation.get("simulation_id", "Unknown")
    model = simulation.get("model_id", "Unknown Model") 
    status = simulation.get("status", "unknown")
    attrs = simulation.get("attributes", {})
    
    return rx.box(
        rx.vstack(
            # Header
            rx.hstack(
                rx.heading(sim_id, size="md", color="gray.900"),
                rx.badge(
                    status.title(),
                    variant="subtle", 
                    color_scheme=get_status_color(status)
                ),
                justify="between",
                width="100%"
            ),
            
            # Info grid
            rx.grid(
                rx.vstack(
                    rx.text("Model", font_size="sm", color="gray.500"),
                    rx.text(model, font_size="sm", font_weight="medium"),
                    spacing="1", align="start"
                ),
                rx.vstack(
                    rx.text("Experiment", font_size="sm", color="gray.500"),
                    rx.text(attrs.get("experiment", "N/A"), font_size="sm", font_weight="medium"),
                    spacing="1", align="start"
                ),
                columns="2",
                spacing="4",
                width="100%"
            ),
            
            # Actions
            rx.hstack(
                rx.button("View", size="sm", variant="ghost", color_scheme="blue"),
                rx.button("Edit", size="sm", variant="ghost"),
                spacing="2"
            ),
            
            spacing="4",
            align="start",
            width="100%"
        ),
        p="6",
        border="1px solid",
        border_color="gray.200", 
        border_radius="lg",
        bg="white",
        _hover={"shadow": "md"},
        cursor="pointer"
    )
"""