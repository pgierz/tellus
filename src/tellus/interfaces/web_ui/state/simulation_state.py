"""
Simulation state management for the Tellus web UI.

This module manages the state for simulation-related data and operations.
"""

# Uncomment when Reflex is available
# import reflex as rx
from typing import List, Dict, Any, Optional
from dataclasses import asdict

# Import Tellus DTOs
from ....application.dtos import SimulationDto, CreateSimulationDto, FilterOptions


class SimulationState:
    """
    State management for simulations in the web UI.
    
    When Reflex is available, this will inherit from rx.State.
    For now, it serves as a blueprint.
    """
    
    def __init__(self):
        # Simulation data
        self.simulations: List[Dict[str, Any]] = []
        self.selected_simulation: Optional[Dict[str, Any]] = None
        self.loading: bool = False
        self.error_message: str = ""
        
        # Filters and search
        self.search_term: str = ""
        self.status_filter: str = "all"
        self.model_filter: str = "all"
        
        # Pagination
        self.current_page: int = 1
        self.page_size: int = 20
        self.total_count: int = 0
        
        # Form data for creating/editing
        self.form_simulation_id: str = ""
        self.form_model_id: str = ""
        self.form_path: str = ""
        self.form_attributes: Dict[str, Any] = {}
    
    def load_simulations(self):
        """Load simulations from the backend API."""
        self.loading = True
        self.error_message = ""
        
        # TODO: Implement API call
        # For now, return mock data
        mock_simulations = [
            {
                "simulation_id": "CESM2_hist_001",
                "uid": "sim_001_uid",
                "model_id": "CESM2",
                "attributes": {
                    "experiment": "historical",
                    "variant": "r1i1p1f1",
                    "resolution": "f09_g17",
                    "start_year": 1850,
                    "end_year": 2014
                },
                "locations": {
                    "hpc_storage": {"path_prefix": "{model}/{experiment}"},
                    "archive_tape": {"path_prefix": "archives/{model}"}
                },
                "status": "completed"
            },
            {
                "simulation_id": "CESM2_ssp585_001", 
                "uid": "sim_002_uid",
                "model_id": "CESM2",
                "attributes": {
                    "experiment": "ssp585",
                    "variant": "r1i1p1f1", 
                    "resolution": "f09_g17",
                    "start_year": 2015,
                    "end_year": 2100
                },
                "locations": {
                    "hpc_storage": {"path_prefix": "{model}/{experiment}"}
                },
                "status": "running"
            }
        ]
        
        self.simulations = mock_simulations
        self.total_count = len(mock_simulations)
        self.loading = False
    
    def select_simulation(self, simulation_id: str):
        """Select a simulation for detailed view."""
        for sim in self.simulations:
            if sim["simulation_id"] == simulation_id:
                self.selected_simulation = sim
                break
    
    def create_simulation(self):
        """Create a new simulation."""
        if not self.form_simulation_id:
            self.error_message = "Simulation ID is required"
            return
        
        # TODO: Implement API call to create simulation
        new_simulation = {
            "simulation_id": self.form_simulation_id,
            "uid": f"sim_{len(self.simulations)+1}_uid",
            "model_id": self.form_model_id,
            "attributes": dict(self.form_attributes),
            "locations": {},
            "status": "created"
        }
        
        self.simulations.append(new_simulation)
        self.clear_form()
    
    def delete_simulation(self, simulation_id: str):
        """Delete a simulation."""
        # TODO: Implement API call
        self.simulations = [
            sim for sim in self.simulations 
            if sim["simulation_id"] != simulation_id
        ]
        
        if self.selected_simulation and self.selected_simulation["simulation_id"] == simulation_id:
            self.selected_simulation = None
    
    def clear_form(self):
        """Clear the simulation form."""
        self.form_simulation_id = ""
        self.form_model_id = ""
        self.form_path = ""
        self.form_attributes = {}
    
    def apply_filters(self):
        """Apply current filters to simulation list."""
        # TODO: Implement filtering logic
        # This would typically trigger a new API call with filter parameters
        self.load_simulations()
    
    def get_filtered_simulations(self) -> List[Dict[str, Any]]:
        """Get simulations filtered by current criteria."""
        filtered = self.simulations
        
        # Apply search filter
        if self.search_term:
            filtered = [
                sim for sim in filtered
                if self.search_term.lower() in sim["simulation_id"].lower()
                or self.search_term.lower() in sim.get("model_id", "").lower()
            ]
        
        # Apply status filter
        if self.status_filter != "all":
            filtered = [
                sim for sim in filtered
                if sim.get("status") == self.status_filter
            ]
        
        # Apply model filter
        if self.model_filter != "all":
            filtered = [
                sim for sim in filtered
                if sim.get("model_id") == self.model_filter
            ]
        
        return filtered
    
    def get_status_options(self) -> List[str]:
        """Get available status options."""
        return ["all", "created", "running", "completed", "failed", "paused"]
    
    def get_model_options(self) -> List[str]:
        """Get available model options."""
        models = set()
        for sim in self.simulations:
            if sim.get("model_id"):
                models.add(sim["model_id"])
        return ["all"] + sorted(models)


# When Reflex is available, this will be a proper state class:
"""
class SimulationState(rx.State):
    # All the methods above, but with proper Reflex decorators and async support
    
    @rx.var
    def filtered_simulations(self) -> List[Dict[str, Any]]:
        return self.get_filtered_simulations()
    
    @rx.var 
    def status_options(self) -> List[str]:
        return self.get_status_options()
        
    @rx.var
    def model_options(self) -> List[str]:
        return self.get_model_options()
"""