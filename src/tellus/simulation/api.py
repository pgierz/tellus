"""
REST API for managing Tellus simulations and locations.

This module provides FastAPI endpoints for managing simulations and their associated
storage locations programmatically.
"""

from typing import Dict, List, Optional

from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel

from ..location import Location, LocationKind
from .simulation import Simulation

# Create FastAPI app
app = FastAPI()

# In-memory store of simulations (for demo)
simulations: Dict[str, Simulation] = {}

# Pydantic models for request/response validation
class LocationCreate(BaseModel):
    """Model for creating a new location."""
    name: str
    kinds: List[str]  # List of location kinds (TAPE, COMPUTE, DISK)
    config: dict
    optional: bool = False


class LocationResponse(LocationCreate):
    """Response model for location data."""
    name: str
    kinds: List[str]
    config: dict
    optional: bool


class SimulationModel(BaseModel):
    """Model for simulation data."""
    simulation_id: Optional[str] = None
    path: Optional[str] = None


# Simulation endpoints
@app.post("/simulations/", status_code=status.HTTP_201_CREATED)
async def create_simulation(sim: SimulationModel):
    """Create a new simulation."""
    simulation = Simulation(simulation_id=sim.simulation_id, path=sim.path)
    simulations[simulation.simulation_id] = simulation
    return {"simulation_id": simulation.simulation_id}


# Location endpoints
@app.get("/locations/", response_model=List[str])
async def list_locations():
    """List all locations."""
    return list(Location._locations.keys())


@app.post("/locations/", response_model=LocationResponse, status_code=status.HTTP_201_CREATED)
async def create_location(location: LocationCreate):
    """Create a new storage location."""
    if location.name in Location._locations:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Location '{location.name}' already exists"
        )
    
    try:
        # Convert string kinds to LocationKind enums
        kinds = [LocationKind.from_str(kind) for kind in location.kinds]
        
        # Create and store the location
        Location(
            name=location.name,
            kinds=kinds,
            config=location.config,
            optional=location.optional
        )
        
        # Return the created location
        return {
            "name": location.name,
            "kinds": location.kinds,
            "config": location.config,
            "optional": location.optional
        }
        
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )


@app.get("/locations/{name}", response_model=LocationResponse)
async def get_location(name: str):
    """Get details of a specific location."""
    if name not in Location._locations:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Location '{name}' not found"
        )
    
    loc = Location._locations[name]
    return {
        "name": loc.name,
        "kinds": [kind.name for kind in loc.kinds],
        "config": loc.config,
        "optional": loc.optional
    }


@app.put("/locations/{name}", response_model=LocationResponse)
async def update_location(name: str, location: LocationCreate):
    """Update an existing location."""
    if name not in Location._locations:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Location '{name}' not found"
        )
    
    try:
        # Convert string kinds to LocationKind enums
        kinds = [LocationKind.from_str(kind) for kind in location.kinds]
        
        # Update the location
        loc = Location._locations[name]
        loc.kinds = kinds
        loc.config = location.config
        loc.optional = location.optional
        
        # If the name changed, update the key in the _locations dict
        if name != location.name:
            Location._locations[location.name] = loc
            del Location._locations[name]
        
        # Return the updated location
        return {
            "name": loc.name,
            "kinds": [kind.name for kind in loc.kinds],
            "config": loc.config,
            "optional": loc.optional
        }
        
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )


@app.delete("/locations/{name}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_location(name: str):
    """Delete a location."""
    if name not in Location._locations:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Location '{name}' not found"
        )
    
    del Location._locations[name]
    return None


# Simulation-specific location endpoints
@app.get("/simulations/{simulation_id}/locations/", response_model=List[str])
async def list_simulation_locations(simulation_id: str):
    """List all locations for a specific simulation."""
    if simulation_id not in simulations:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Simulation '{simulation_id}' not found"
        )
    return simulations[simulation_id].list_locations()


@app.post("/simulations/{simulation_id}/locations/{location_name}/")
async def post_to_location(simulation_id: str, location_name: str, data: dict):
    """Post data to a specific location in a simulation."""
    if simulation_id not in simulations:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Simulation '{simulation_id}' not found"
        )
    
    try:
        data_id = simulations[simulation_id].post_to_location(location_name, data)
        return {"id": data_id}
    except KeyError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e)
        )


@app.get("/simulations/{simulation_id}/locations/{location_name}/{identifier}")
async def fetch_from_location(simulation_id: str, location_name: str, identifier: str):
    """Fetch data from a specific location in a simulation."""
    if simulation_id not in simulations:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Simulation '{simulation_id}' not found"
        )
    
    try:
        return simulations[simulation_id].fetch_from_location(location_name, identifier)
    except (KeyError, ValueError) as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e)
        )
