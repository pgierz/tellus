"""
Simulation card component for displaying simulation information.
"""

try:
    import reflex as rx
except ImportError:
    print("Warning: Reflex not installed. Install with: pip install reflex")

try:
    from ...application.dtos import SimulationDto
except ImportError:
    # Use mock class if DTOs not available
    from dataclasses import dataclass, field
    from typing import Dict, Any, List
    
    @dataclass
    class SimulationDto:
        simulation_id: str
        uid: str
        attributes: Dict[str, Any] = field(default_factory=dict)
        locations: Dict[str, Dict[str, Any]] = field(default_factory=dict)
        workflows: Dict[str, Any] = field(default_factory=dict)
        
        @property
        def associated_locations(self) -> List[str]:
            return list(self.locations.keys())


def status_badge(status: str) -> rx.Component:
    """Status badge with appropriate color."""
    color_map = {
        "completed": "green",
        "running": "blue", 
        "failed": "red",
        "pending": "orange",
        "unknown": "gray"
    }
    color = color_map.get(status.lower(), "gray")
    
    return rx.badge(
        status.title(),
        variant="soft",
        color_scheme=color
    )


def simulation_card(simulation: SimulationDto) -> rx.Component:
    """Card component for displaying simulation information."""
    
    # Extract status from workflows (simplified)
    workflow_statuses = [
        wf.get("status", "unknown") 
        for wf in simulation.workflows.values()
    ] if simulation.workflows else ["unknown"]
    
    primary_status = workflow_statuses[0] if workflow_statuses else "unknown"
    
    return rx.card(
        rx.vstack(
            rx.hstack(
                rx.heading(simulation.simulation_id, size="4", weight="bold"),
                status_badge(primary_status),
                justify="between",
                align="center",
                width="100%"
            ),
            rx.vstack(
                rx.text(
                    f"Model: {simulation.attributes.get('model', 'Unknown')}", 
                    size="2",
                    color="gray.11"
                ),
                rx.text(
                    f"Experiment: {simulation.attributes.get('experiment', 'Unknown')}", 
                    size="2", 
                    color="gray.11"
                ),
                rx.text(
                    f"Resolution: {simulation.attributes.get('resolution', 'Unknown')}", 
                    size="2",
                    color="gray.11"
                ),
                align="start",
                spacing="1",
                width="100%"
            ),
            rx.hstack(
                rx.hstack(
                    rx.icon("map-pin", size=16),
                    rx.text(
                        f"{len(simulation.associated_locations)} location(s)",
                        size="1",
                        color="gray.9"
                    ),
                    align="center",
                    spacing="1"
                ),
                rx.hstack(
                    rx.icon("workflow", size=16),
                    rx.text(
                        f"{len(simulation.workflows)} workflow(s)",
                        size="1", 
                        color="gray.9"
                    ),
                    align="center",
                    spacing="1"
                ),
                justify="between",
                width="100%"
            ),
            rx.hstack(
                rx.button(
                    rx.icon("eye", size=16),
                    "View Details",
                    size="2",
                    variant="soft"
                ),
                rx.button(
                    rx.icon("play", size=16), 
                    "Run Workflow",
                    size="2",
                    variant="outline"
                ),
                rx.button(
                    rx.icon("more-horizontal", size=16),
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