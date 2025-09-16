"""
Simulation management endpoints for the Tellus API.

Provides CRUD operations for climate simulations including:
- Listing and searching simulations
- Creating new simulations
- Getting simulation details
- Updating simulation metadata
- Managing simulation-location associations
"""

from typing import List, Optional
from fastapi import APIRouter, HTTPException, Depends, Query, Request
from fastapi import status

from ....application.dtos import (
    SimulationDto, CreateSimulationDto, UpdateSimulationDto,
    SimulationListDto, PaginationInfo, FilterOptions
)
from ....application.services.simulation_service import SimulationApplicationService
from ..dependencies import get_simulation_service

router = APIRouter()


@router.get("/", response_model=SimulationListDto)
async def list_simulations(
    request: Request,
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(50, ge=1, le=100, description="Number of items per page"),
    search: Optional[str] = Query(None, description="Search term for simulation IDs"),
    simulation_service: SimulationApplicationService = Depends(get_simulation_service)
):
    """
    List all simulations with pagination and optional filtering.
    
    Args:
        page: Page number (1-based)
        page_size: Number of simulations per page (1-100)
        search: Optional search term to filter simulation IDs
        
    Returns:
        Paginated list of simulations with metadata
    """
    try:
        # Create filter options
        filters = FilterOptions(search_term=search) if search else None
        
        # Get simulations using the service (it handles pagination and filtering)
        result = simulation_service.list_simulations(
            page=page,
            page_size=page_size,
            filters=filters
        )
        
        return result
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to list simulations: {str(e)}"
        )


@router.post("/", response_model=SimulationDto, status_code=status.HTTP_201_CREATED)
async def create_simulation(
    simulation_data: CreateSimulationDto,
    simulation_service: SimulationApplicationService = Depends(get_simulation_service)
):
    """
    Create a new simulation.
    
    Args:
        simulation_data: Simulation creation data
        
    Returns:
        Created simulation with generated UID
        
    Raises:
        400: If simulation ID already exists
        422: If validation fails
    """
    try:
        # Create the simulation (service will check for duplicates)
        created_simulation = simulation_service.create_simulation(simulation_data)
        return created_simulation
        
    except HTTPException:
        raise
    except ValueError as e:
        # Validation error
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=str(e)
        )
    except Exception as e:
        # Check if this is a "already exists" error
        if "already exists" in str(e).lower():
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=str(e)
            )
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to create simulation: {str(e)}"
            )


@router.get("/{simulation_id}", response_model=SimulationDto)
async def get_simulation(
    simulation_id: str,
    simulation_service: SimulationApplicationService = Depends(get_simulation_service)
):
    """
    Get details of a specific simulation.
    
    Args:
        simulation_id: The simulation identifier
        
    Returns:
        Simulation details including metadata and associations
        
    Raises:
        404: If simulation is not found
    """
    try:
        simulation = simulation_service.get_simulation(simulation_id)
        return simulation
        
    except HTTPException:
        raise
    except Exception as e:
        # Check if this is a "not found" error from the service layer
        if "not found" in str(e).lower():
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Simulation '{simulation_id}' not found"
            )
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to get simulation: {str(e)}"
            )


@router.put("/{simulation_id}", response_model=SimulationDto)
async def update_simulation(
    simulation_id: str,
    update_data: UpdateSimulationDto,
    simulation_service: SimulationApplicationService = Depends(get_simulation_service)
):
    """
    Update an existing simulation.
    
    Args:
        simulation_id: The simulation identifier
        update_data: Fields to update
        
    Returns:
        Updated simulation data
        
    Raises:
        404: If simulation is not found
        422: If validation fails
    """
    try:
        # Update the simulation (service handles existence check)
        updated_simulation = simulation_service.update_simulation(simulation_id, update_data)
        return updated_simulation
        
    except HTTPException:
        raise
    except Exception as e:
        # Check if this is a "not found" error from the service layer
        if "not found" in str(e).lower():
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Simulation '{simulation_id}' not found"
            )
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to update simulation: {str(e)}"
            )


@router.delete("/{simulation_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_simulation(
    simulation_id: str,
    simulation_service: SimulationApplicationService = Depends(get_simulation_service)
):
    """
    Delete a simulation.
    
    Args:
        simulation_id: The simulation identifier
        
    Raises:
        404: If simulation is not found
    """
    try:
        # Delete the simulation (service handles existence check)
        simulation_service.delete_simulation(simulation_id)
        
    except HTTPException:
        raise
    except Exception as e:
        # Check if this is a "not found" error from the service layer
        if "not found" in str(e).lower():
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Simulation '{simulation_id}' not found"
            )
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to delete simulation: {str(e)}"
            )