"""
Main dashboard page for the Tellus web UI.

This page provides an overview of simulations, recent activity, and system status.
"""

# Uncomment when Reflex is available
# import reflex as rx
from typing import Dict, Any, List


def index_page():
    """
    Main dashboard page.
    
    When Reflex is available, this will return a proper Reflex page component.
    For now, this serves as a blueprint.
    """
    
    page_structure = {
        "type": "dashboard_page",
        "props": {"className": "min-h-screen bg-gray-50"},
        "children": [
            # Page header
            {
                "type": "page_header",
                "props": {"className": "bg-white border-b border-gray-200 px-6 py-4"},
                "children": [
                    {
                        "type": "header_content",
                        "props": {"className": "flex items-center justify-between"},
                        "children": [
                            {
                                "type": "title_section",
                                "children": [
                                    {
                                        "type": "title",
                                        "props": {"className": "text-2xl font-bold text-gray-900"},
                                        "text": "Dashboard"
                                    },
                                    {
                                        "type": "subtitle",
                                        "props": {"className": "text-gray-600 mt-1"},
                                        "text": "Overview of your climate simulations and data"
                                    }
                                ]
                            },
                            {
                                "type": "actions",
                                "props": {"className": "flex space-x-3"},
                                "children": [
                                    {
                                        "type": "new_simulation_button",
                                        "props": {
                                            "className": "bg-blue-600 text-white px-4 py-2 rounded-lg hover:bg-blue-700 transition-colors"
                                        },
                                        "text": "➕ New Simulation"
                                    },
                                    {
                                        "type": "refresh_button", 
                                        "props": {
                                            "className": "bg-gray-100 text-gray-700 px-4 py-2 rounded-lg hover:bg-gray-200 transition-colors"
                                        },
                                        "text": "🔄 Refresh"
                                    }
                                ]
                            }
                        ]
                    }
                ]
            },
            
            # Main content
            {
                "type": "main_content",
                "props": {"className": "p-6"},
                "children": [
                    # Stats cards
                    {
                        "type": "stats_section",
                        "props": {"className": "grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6 mb-8"},
                        "children": [
                            {
                                "type": "stat_card",
                                "props": {"key": "total_simulations"},
                                "children": [
                                    {
                                        "type": "stat_content",
                                        "props": {"className": "bg-white p-6 rounded-lg border border-gray-200"},
                                        "children": [
                                            {"type": "stat_icon", "text": "🔬", "props": {"className": "text-2xl mb-2"}},
                                            {"type": "stat_value", "text": "12", "props": {"className": "text-3xl font-bold text-gray-900"}},
                                            {"type": "stat_label", "text": "Total Simulations", "props": {"className": "text-gray-600"}}
                                        ]
                                    }
                                ]
                            },
                            {
                                "type": "stat_card",
                                "props": {"key": "running"},
                                "children": [
                                    {
                                        "type": "stat_content",
                                        "props": {"className": "bg-white p-6 rounded-lg border border-gray-200"},
                                        "children": [
                                            {"type": "stat_icon", "text": "🔄", "props": {"className": "text-2xl mb-2"}},
                                            {"type": "stat_value", "text": "3", "props": {"className": "text-3xl font-bold text-blue-600"}},
                                            {"type": "stat_label", "text": "Running", "props": {"className": "text-gray-600"}}
                                        ]
                                    }
                                ]
                            },
                            {
                                "type": "stat_card",
                                "props": {"key": "completed"},
                                "children": [
                                    {
                                        "type": "stat_content",
                                        "props": {"className": "bg-white p-6 rounded-lg border border-gray-200"},
                                        "children": [
                                            {"type": "stat_icon", "text": "✅", "props": {"className": "text-2xl mb-2"}},
                                            {"type": "stat_value", "text": "8", "props": {"className": "text-3xl font-bold text-green-600"}},
                                            {"type": "stat_label", "text": "Completed", "props": {"className": "text-gray-600"}}
                                        ]
                                    }
                                ]
                            },
                            {
                                "type": "stat_card",
                                "props": {"key": "storage"},
                                "children": [
                                    {
                                        "type": "stat_content",
                                        "props": {"className": "bg-white p-6 rounded-lg border border-gray-200"},
                                        "children": [
                                            {"type": "stat_icon", "text": "💾", "props": {"className": "text-2xl mb-2"}},
                                            {"type": "stat_value", "text": "2.4TB", "props": {"className": "text-3xl font-bold text-purple-600"}},
                                            {"type": "stat_label", "text": "Total Storage", "props": {"className": "text-gray-600"}}
                                        ]
                                    }
                                ]
                            }
                        ]
                    },
                    
                    # Content grid
                    {
                        "type": "content_grid",
                        "props": {"className": "grid grid-cols-1 lg:grid-cols-2 gap-6"},
                        "children": [
                            # Recent simulations
                            {
                                "type": "recent_simulations",
                                "props": {"className": "bg-white rounded-lg border border-gray-200 p-6"},
                                "children": [
                                    {
                                        "type": "section_header",
                                        "props": {"className": "flex items-center justify-between mb-4"},
                                        "children": [
                                            {"type": "title", "text": "Recent Simulations", "props": {"className": "text-lg font-semibold text-gray-900"}},
                                            {"type": "view_all", "text": "View All →", "props": {"className": "text-blue-600 hover:text-blue-800 text-sm"}}
                                        ]
                                    },
                                    {
                                        "type": "simulation_list",
                                        "props": {"className": "space-y-3"},
                                        "children": [
                                            # Mock recent simulations
                                            {
                                                "type": "simulation_item",
                                                "props": {"className": "flex items-center justify-between p-3 hover:bg-gray-50 rounded"},
                                                "children": [
                                                    {
                                                        "type": "sim_info",
                                                        "children": [
                                                            {"type": "name", "text": "CESM2_ssp585_001", "props": {"className": "font-medium text-gray-900"}},
                                                            {"type": "details", "text": "SSP5-8.5 scenario • Started 2 hours ago", "props": {"className": "text-sm text-gray-500"}}
                                                        ]
                                                    },
                                                    {"type": "status", "text": "🔄", "props": {"className": "text-lg"}}
                                                ]
                                            }
                                        ]
                                    }
                                ]
                            },
                            
                            # System status
                            {
                                "type": "system_status",
                                "props": {"className": "bg-white rounded-lg border border-gray-200 p-6"},
                                "children": [
                                    {
                                        "type": "section_header",
                                        "props": {"className": "flex items-center justify-between mb-4"},
                                        "children": [
                                            {"type": "title", "text": "System Status", "props": {"className": "text-lg font-semibold text-gray-900"}},
                                            {"type": "status_indicator", "text": "🟢 All Systems Operational", "props": {"className": "text-green-600 text-sm"}}
                                        ]
                                    },
                                    {
                                        "type": "status_list",
                                        "props": {"className": "space-y-3"},
                                        "children": [
                                            {
                                                "type": "status_item",
                                                "props": {"className": "flex items-center justify-between"},
                                                "children": [
                                                    {"type": "service", "text": "HPC Storage", "props": {"className": "text-gray-700"}},
                                                    {"type": "status", "text": "🟢 Online", "props": {"className": "text-green-600 text-sm"}}
                                                ]
                                            },
                                            {
                                                "type": "status_item", 
                                                "props": {"className": "flex items-center justify-between"},
                                                "children": [
                                                    {"type": "service", "text": "Archive System", "props": {"className": "text-gray-700"}},
                                                    {"type": "status", "text": "🟢 Online", "props": {"className": "text-green-600 text-sm"}}
                                                ]
                                            },
                                            {
                                                "type": "status_item",
                                                "props": {"className": "flex items-center justify-between"},
                                                "children": [
                                                    {"type": "service", "text": "AI Assistant", "props": {"className": "text-gray-700"}},
                                                    {"type": "status", "text": "🟢 Online", "props": {"className": "text-green-600 text-sm"}}
                                                ]
                                            }
                                        ]
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }
        ]
    }
    
    return page_structure


def quick_actions_panel():
    """Quick actions floating panel."""
    return {
        "type": "quick_actions",
        "props": {
            "className": "fixed bottom-6 right-6 bg-white rounded-lg shadow-lg border border-gray-200 p-4",
            "style": {"z_index": 40}
        },
        "children": [
            {
                "type": "actions_title",
                "props": {"className": "text-sm font-medium text-gray-900 mb-3"},
                "text": "Quick Actions"
            },
            {
                "type": "action_buttons",
                "props": {"className": "space-y-2"},
                "children": [
                    {
                        "type": "action_button",
                        "props": {"className": "w-full text-left px-3 py-2 text-sm text-gray-700 hover:bg-gray-100 rounded"},
                        "text": "📊 Create Simulation"
                    },
                    {
                        "type": "action_button",
                        "props": {"className": "w-full text-left px-3 py-2 text-sm text-gray-700 hover:bg-gray-100 rounded"},
                        "text": "📍 Add Location"
                    },
                    {
                        "type": "action_button",
                        "props": {"className": "w-full text-left px-3 py-2 text-sm text-gray-700 hover:bg-gray-100 rounded"},
                        "text": "💬 Ask AI Assistant"
                    }
                ]
            }
        ]
    }


# When Reflex is available, this will be a proper page:
"""
import reflex as rx

def index_page() -> rx.Component:
    return rx.box(
        rx.vstack(
            # Page header
            rx.box(
                rx.hstack(
                    rx.vstack(
                        rx.heading("Dashboard", size="xl", color="gray.900"),
                        rx.text("Overview of your climate simulations and data", color="gray.600"),
                        align="start", spacing="1"
                    ),
                    rx.hstack(
                        rx.button("New Simulation", left_icon="add", color_scheme="blue"),
                        rx.button("Refresh", left_icon="repeat", variant="outline"),
                        spacing="3"
                    ),
                    justify="between", align="center", width="100%"
                ),
                bg="white", border_bottom="1px", border_color="gray.200", p="6"
            ),
            
            # Stats cards
            rx.grid(
                rx.stat(
                    rx.stat_label("Total Simulations"),
                    rx.stat_number("12"),
                    rx.stat_help_text("🔬")
                ),
                rx.stat(
                    rx.stat_label("Running"),  
                    rx.stat_number("3", color="blue.600"),
                    rx.stat_help_text("🔄")
                ),
                rx.stat(
                    rx.stat_label("Completed"),
                    rx.stat_number("8", color="green.600"),
                    rx.stat_help_text("✅")
                ),
                rx.stat(
                    rx.stat_label("Total Storage"),
                    rx.stat_number("2.4TB", color="purple.600"),
                    rx.stat_help_text("💾")
                ),
                columns=[1, 2, 4], spacing="6", width="100%"
            ),
            
            # Content sections
            rx.grid(
                # Recent simulations
                rx.box(
                    rx.vstack(
                        rx.hstack(
                            rx.heading("Recent Simulations", size="md"),
                            rx.link("View All →", color="blue.600"),
                            justify="between", width="100%"
                        ),
                        # Simulation list would go here
                        spacing="4", align="start", width="100%"
                    ),
                    bg="white", p="6", border_radius="lg", border="1px", border_color="gray.200"
                ),
                
                # System status  
                rx.box(
                    rx.vstack(
                        rx.hstack(
                            rx.heading("System Status", size="md"),
                            rx.text("🟢 All Systems Operational", color="green.600", font_size="sm"),
                            justify="between", width="100%"
                        ),
                        # Status list would go here
                        spacing="4", align="start", width="100%"
                    ),
                    bg="white", p="6", border_radius="lg", border="1px", border_color="gray.200"
                ),
                
                columns=[1, 2], spacing="6", width="100%"
            ),
            
            spacing="6", align="start", width="100%"
        ),
        min_h="100vh", bg="gray.50", p="6"
    )
"""