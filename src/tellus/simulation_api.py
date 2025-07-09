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
    path: str


@app.post("/simulations/", response_model=dict)
def create_simulation(sim: SimulationModel):
    if sim.path in simulations:
        raise HTTPException(status_code=400, detail="Simulation already exists")
    s = Simulation(path=sim.path)
    simulations[sim.path] = s
    return {"status": "created", "simulation": sim.path}


@app.post("/simulations/{sim_path}/locations/", response_model=dict)
def add_location(sim_path: str, location: LocationModel):
    if sim_path not in simulations:
        raise HTTPException(status_code=404, detail="Simulation not found")
    loc = Location(**location.dict())
    simulations[sim_path].set_location(loc)
    return {"status": "location added", "location": location.name}


@app.get("/simulations/{sim_path}/locations/", response_model=dict)
def list_locations(sim_path: str):
    if sim_path not in simulations:
        raise HTTPException(status_code=404, detail="Simulation not found")
    locs = simulations[sim_path].list_locations()
    return {"locations": locs}


@app.post(
    "/simulations/{sim_path}/locations/{location_name}/post/", response_model=dict
)
async def post_to_location(sim_path: str, location_name: str, data: dict):
    if sim_path not in simulations:
        raise HTTPException(status_code=404, detail="Simulation not found")
    # In a real app, this could be an async call to a handler
    await simulations[sim_path].post_to_location(location_name, data)
    return {"status": "posted", "location": location_name}


@app.get(
    "/simulations/{sim_path}/locations/{location_name}/fetch/", response_model=dict
)
async def fetch_from_location(sim_path: str, location_name: str, identifier: str):
    if sim_path not in simulations:
        raise HTTPException(status_code=404, detail="Simulation not found")
    result = await simulations[sim_path].fetch_from_location(location_name, identifier)
    return {"data": result}
