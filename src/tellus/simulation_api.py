from typing import Dict, Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from simulation import (  # assume your Simulation/Location classes live here
    Location,
    Simulation,
)

app = FastAPI()

# In-memory store of simulations (for demo)
simulations: Dict[str, Simulation] = {}


class LocationModel(BaseModel):
    name: str
    kind: str
    config: dict


class SimulationModel(BaseModel):
    simulation_id: Optional[str] = None
    path: Optional[str] = None


@app.post("/simulations/", response_model=dict)
async def create_simulation(sim: SimulationModel):
    if sim.simulation_id and sim.simulation_id in simulations:
        raise HTTPException(
            status_code=400, detail="Simulation with this ID already exists"
        )
    s = Simulation(simulation_id=sim.simulation_id, path=sim.path)
    simulations[s.simulation_id] = s
    return {"status": "created", "simulation_id": s.simulation_id}


@app.post("/simulations/{simulation_id}/locations/", response_model=dict)
async def add_location(simulation_id: str, location: LocationModel):
    if simulation_id not in simulations:
        raise HTTPException(status_code=404, detail="Simulation not found")
    loc = Location(**location.dict())
    simulations[simulation_id].set_location(loc)
    return {
        "status": "location added",
        "simulation_id": simulation_id,
        "location": location.name,
    }


@app.get("/simulations/{simulation_id}/locations/", response_model=dict)
async def list_locations(simulation_id: str):
    if simulation_id not in simulations:
        raise HTTPException(status_code=404, detail="Simulation not found")
    locs = simulations[simulation_id].list_locations()
    return {"simulation_id": simulation_id, "locations": locs}


@app.post(
    "/simulations/{simulation_id}/locations/{location_name}/post/", response_model=dict
)
async def post_to_location(simulation_id: str, location_name: str, data: dict):
    if simulation_id not in simulations:
        raise HTTPException(status_code=404, detail="Simulation not found")
    # In a real app, this could be an async call to a handler
    await simulations[simulation_id].post_to_location(location_name, data)
    return {
        "status": "posted",
        "simulation_id": simulation_id,
        "location": location_name,
    }


@app.get(
    "/simulations/{simulation_id}/locations/{location_name}/fetch/", response_model=dict
)
async def fetch_from_location(simulation_id: str, location_name: str, identifier: str):
    if simulation_id not in simulations:
        raise HTTPException(status_code=404, detail="Simulation not found")
    result = await simulations[simulation_id].fetch_from_location(
        location_name, identifier
    )
    return {"simulation_id": simulation_id, "location": location_name, "data": result}
