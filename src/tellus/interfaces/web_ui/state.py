"""
Global state management for the Tellus Web UI.

This module provides centralized state management for the web interface,
including authentication, simulation data, and UI state.
"""

try:
    import reflex as rx
except ImportError:
    print("Warning: Reflex not installed. Install with: pip install reflex")
    
from typing import List, Optional, Dict, Any
from datetime import datetime

try:
    from ...application.dtos import (
        SimulationDto,
        LocationDto, 
        ArchiveDto,
        SimulationFileDto
    )
except ImportError:
    # Fallback for development - create minimal mock classes
    from dataclasses import dataclass, field
    
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
    
    @dataclass  
    class LocationDto:
        name: str
        kinds: List[str] = field(default_factory=list)
        protocol: str = "file"
        path: Optional[str] = None
        storage_options: Dict[str, Any] = field(default_factory=dict)
        additional_config: Dict[str, Any] = field(default_factory=dict)
        is_remote: bool = False
        is_accessible: Optional[bool] = None
        last_verified: Optional[str] = None
    
    @dataclass
    class ArchiveDto:
        archive_id: str
        location: str
        archive_type: str
    
    @dataclass
    class SimulationFileDto:
        relative_path: str
        size: Optional[int] = None

from .services.api_client import TellusAPIClient


class AppState(rx.State):
    """Main application state for the Tellus Web UI."""
    
    # Core data
    simulations: List[SimulationDto] = []
    locations: List[LocationDto] = []
    selected_simulation: Optional[SimulationDto] = None
    selected_location: Optional[LocationDto] = None
    
    # UI State
    loading: bool = False
    error_message: str = ""
    success_message: str = ""
    current_page: str = "dashboard"
    sidebar_collapsed: bool = False
    
    # Filters and search
    simulation_search: str = ""
    location_search: str = ""
    simulation_status_filter: str = "all"
    
    def __init__(self):
        super().__init__()
        self.api_client = TellusAPIClient()
    
    @rx.background
    async def load_simulations(self):
        """Load simulations from the backend."""
        async with self:
            self.loading = True
            self.error_message = ""
        
        try:
            # For now, create mock data since we don't have the full REST API yet
            mock_simulations = [
                SimulationDto(
                    simulation_id="fesom-test-001",
                    uid="sim-001",
                    attributes={
                        "model": "FESOM2", 
                        "experiment": "historical",
                        "resolution": "HR",
                        "ensemble": "r1i1p1f1"
                    },
                    locations={
                        "mistral": {"path_prefix": "/work/ab0246/a270124"},
                        "levante": {"path_prefix": "/work/ab0246/a270124"}  
                    },
                    workflows={"preprocess": {"status": "completed"}}
                ),
                SimulationDto(
                    simulation_id="icon-esm-lr",
                    uid="sim-002", 
                    attributes={
                        "model": "ICON-ESM",
                        "experiment": "ssp585",
                        "resolution": "LR",
                        "ensemble": "r1i1p1f1"
                    },
                    locations={
                        "levante": {"path_prefix": "/work/ba1243/a270094"}
                    },
                    workflows={"postprocess": {"status": "running"}}
                )
            ]
            
            async with self:
                self.simulations = mock_simulations
                self.success_message = f"Loaded {len(mock_simulations)} simulations"
                
        except Exception as e:
            async with self:
                self.error_message = f"Failed to load simulations: {str(e)}"
        finally:
            async with self:
                self.loading = False
    
    @rx.background 
    async def load_locations(self):
        """Load locations from the backend."""
        async with self:
            self.loading = True
            self.error_message = ""
            
        try:
            # Mock location data
            mock_locations = [
                LocationDto(
                    name="mistral",
                    kinds=["COMPUTE", "DISK"],
                    protocol="ssh", 
                    path="/work/ab0246",
                    storage_options={
                        "host": "mistral.dkrz.de",
                        "username": "a270124"
                    },
                    is_remote=True,
                    is_accessible=True,
                    last_verified="2025-09-05T09:00:00Z"
                ),
                LocationDto(
                    name="levante", 
                    kinds=["COMPUTE", "DISK"],
                    protocol="ssh",
                    path="/work/ba1243",
                    storage_options={
                        "host": "levante.dkrz.de",
                        "username": "a270094"
                    },
                    is_remote=True,
                    is_accessible=True,
                    last_verified="2025-09-05T08:30:00Z"
                ),
                LocationDto(
                    name="local-cache",
                    kinds=["DISK"],
                    protocol="file",
                    path="~/.cache/tellus",
                    storage_options={},
                    is_remote=False,
                    is_accessible=True,
                    last_verified="2025-09-05T10:00:00Z"
                )
            ]
            
            async with self:
                self.locations = mock_locations
                self.success_message = f"Loaded {len(mock_locations)} locations"
                
        except Exception as e:
            async with self:
                self.error_message = f"Failed to load locations: {str(e)}"
        finally:
            async with self:
                self.loading = False
    
    def select_simulation(self, simulation_id: str):
        """Select a simulation for detailed view."""
        for sim in self.simulations:
            if sim.simulation_id == simulation_id:
                self.selected_simulation = sim
                break
    
    def select_location(self, location_name: str):
        """Select a location for detailed view."""
        for loc in self.locations:
            if loc.name == location_name:
                self.selected_location = loc
                break
    
    def clear_messages(self):
        """Clear error and success messages."""
        self.error_message = ""
        self.success_message = ""
    
    def toggle_sidebar(self):
        """Toggle sidebar collapsed state."""
        self.sidebar_collapsed = not self.sidebar_collapsed
    
    def set_page(self, page: str):
        """Set the current page."""
        self.current_page = page
        self.clear_messages()
    
    @property
    def filtered_simulations(self) -> List[SimulationDto]:
        """Get filtered simulations based on search and filters."""
        filtered = self.simulations
        
        if self.simulation_search:
            filtered = [
                sim for sim in filtered
                if self.simulation_search.lower() in sim.simulation_id.lower()
                or self.simulation_search.lower() in str(sim.attributes.get("model", "")).lower()
            ]
        
        return filtered
    
    @property 
    def filtered_locations(self) -> List[LocationDto]:
        """Get filtered locations based on search."""
        if not self.location_search:
            return self.locations
            
        return [
            loc for loc in self.locations
            if self.location_search.lower() in loc.name.lower()
            or self.location_search.lower() in loc.protocol.lower()
        ]