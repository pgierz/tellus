"""
Simulation management endpoints for the Tellus API.

Provides CRUD operations for climate simulations including:
- Listing and searching simulations
- Creating new simulations
- Getting simulation details
- Updating simulation metadata
- Managing simulation-location associations
"""

from typing import List, Optional, Dict, Any
from fastapi import APIRouter, HTTPException, Depends, Query, Request
from fastapi import status
from pydantic import BaseModel, Field

from ....application.dtos import (
    SimulationDto, CreateSimulationDto, UpdateSimulationDto,
    SimulationListDto, PaginationInfo, FilterOptions,
    SimulationLocationAssociationDto
)
from ....application.services.simulation_service import SimulationApplicationService
from ..dependencies import get_simulation_service

router = APIRouter()


# Pydantic models for attributes API
class AttributeRequest(BaseModel):
    """Request model for setting an attribute."""
    key: str = Field(..., description="The attribute key")
    value: str = Field(..., description="The attribute value")


class AttributeResponse(BaseModel):
    """Response model for an attribute."""
    key: str = Field(..., description="The attribute key")
    value: str = Field(..., description="The attribute value")


class AttributesResponse(BaseModel):
    """Response model for all attributes of a simulation."""
    simulation_id: str = Field(..., description="The simulation identifier")
    attributes: Dict[str, Any] = Field(..., description="All simulation attributes")


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


@router.get("/{simulation_id}/attributes", response_model=AttributesResponse)
async def get_simulation_attributes(
    simulation_id: str,
    simulation_service: SimulationApplicationService = Depends(get_simulation_service)
):
    """
    Get all attributes of a simulation.
    
    Args:
        simulation_id: The simulation identifier
        
    Returns:
        All simulation attributes as key-value pairs
        
    Raises:
        404: If simulation is not found
    """
    try:
        simulation = simulation_service.get_simulation(simulation_id)
        return AttributesResponse(
            simulation_id=simulation_id,
            attributes=simulation.attrs or {}
        )
        
    except HTTPException:
        raise
    except Exception as e:
        if "not found" in str(e).lower():
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Simulation '{simulation_id}' not found"
            )
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to get simulation attributes: {str(e)}"
            )


@router.get("/{simulation_id}/attributes/{attribute_key}", response_model=AttributeResponse)
async def get_simulation_attribute(
    simulation_id: str,
    attribute_key: str,
    simulation_service: SimulationApplicationService = Depends(get_simulation_service)
):
    """
    Get a specific attribute of a simulation.
    
    Args:
        simulation_id: The simulation identifier
        attribute_key: The attribute key to retrieve
        
    Returns:
        The requested attribute key-value pair
        
    Raises:
        404: If simulation or attribute is not found
    """
    try:
        simulation = simulation_service.get_simulation(simulation_id)
        
        if not simulation.attrs or attribute_key not in simulation.attrs:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Attribute '{attribute_key}' not found for simulation '{simulation_id}'"
            )
            
        return AttributeResponse(
            key=attribute_key,
            value=str(simulation.attrs[attribute_key])
        )
        
    except HTTPException:
        raise
    except Exception as e:
        if "not found" in str(e).lower():
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Simulation '{simulation_id}' not found"
            )
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to get simulation attribute: {str(e)}"
            )


@router.put("/{simulation_id}/attributes/{attribute_key}", response_model=AttributeResponse)
async def set_simulation_attribute(
    simulation_id: str,
    attribute_key: str,
    attribute_data: AttributeRequest,
    simulation_service: SimulationApplicationService = Depends(get_simulation_service)
):
    """
    Set a specific attribute of a simulation.
    
    Args:
        simulation_id: The simulation identifier
        attribute_key: The attribute key to set
        attribute_data: The attribute data containing key and value
        
    Returns:
        The updated attribute key-value pair
        
    Raises:
        400: If attribute key in URL doesn't match request body
        404: If simulation is not found
        422: If validation fails
    """
    try:
        # Validate that the key in URL matches the key in the request body
        if attribute_key != attribute_data.key:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Attribute key in URL ('{attribute_key}') must match key in request body ('{attribute_data.key}')"
            )
            
        # Set the attribute using the service
        simulation_service.add_simulation_attribute(
            simulation_id, 
            attribute_data.key, 
            attribute_data.value
        )
        
        return AttributeResponse(
            key=attribute_data.key,
            value=attribute_data.value
        )
        
    except HTTPException:
        raise
    except Exception as e:
        if "not found" in str(e).lower():
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Simulation '{simulation_id}' not found"
            )
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to set simulation attribute: {str(e)}"
            )


@router.post("/{simulation_id}/attributes", response_model=AttributeResponse, status_code=status.HTTP_201_CREATED)
async def add_simulation_attribute(
    simulation_id: str,
    attribute_data: AttributeRequest,
    simulation_service: SimulationApplicationService = Depends(get_simulation_service)
):
    """
    Add a new attribute to a simulation.
    
    Args:
        simulation_id: The simulation identifier
        attribute_data: The attribute data containing key and value
        
    Returns:
        The created attribute key-value pair
        
    Raises:
        404: If simulation is not found
        422: If validation fails
    """
    try:
        # Add the attribute using the service
        simulation_service.add_simulation_attribute(
            simulation_id, 
            attribute_data.key, 
            attribute_data.value
        )
        
        return AttributeResponse(
            key=attribute_data.key,
            value=attribute_data.value
        )
        
    except HTTPException:
        raise
    except Exception as e:
        if "not found" in str(e).lower():
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Simulation '{simulation_id}' not found"
            )
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to add simulation attribute: {str(e)}"
            )


# Location association endpoints

@router.post("/{simulation_id}/locations", response_model=SimulationDto)
async def associate_simulation_locations(
    simulation_id: str,
    association_data: SimulationLocationAssociationDto,
    simulation_service: SimulationApplicationService = Depends(get_simulation_service)
):
    """
    Associate a simulation with one or more locations.
    
    Args:
        simulation_id: The simulation identifier
        association_data: Location association data
        
    Returns:
        Updated simulation with new location associations
        
    Raises:
        400: If simulation_id in URL doesn't match request body
        404: If simulation is not found
        422: If validation fails
    """
    try:
        # Validate that the simulation_id in URL matches the one in the request body
        if simulation_id != association_data.simulation_id:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Simulation ID in URL ('{simulation_id}') must match ID in request body ('{association_data.simulation_id}')"
            )
            
        # Associate the locations using the service
        simulation_service.associate_locations(association_data)
        
        # Return the updated simulation
        return simulation_service.get_simulation(simulation_id)
        
    except HTTPException:
        raise
    except Exception as e:
        if "not found" in str(e).lower():
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Simulation '{simulation_id}' not found"
            )
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to associate locations: {str(e)}"
            )


@router.delete("/{simulation_id}/locations/{location_name}", response_model=SimulationDto)
async def disassociate_simulation_location(
    simulation_id: str,
    location_name: str,
    simulation_service: SimulationApplicationService = Depends(get_simulation_service)
):
    """
    Remove a location association from a simulation.
    
    Args:
        simulation_id: The simulation identifier
        location_name: The location name to disassociate
        
    Returns:
        Updated simulation without the location association
        
    Raises:
        404: If simulation is not found
    """
    try:
        # Disassociate the location using the service
        updated_simulation = simulation_service.disassociate_simulation_from_location(
            simulation_id, location_name
        )
        
        return updated_simulation
        
    except HTTPException:
        raise
    except Exception as e:
        if "not found" in str(e).lower():
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Simulation '{simulation_id}' not found"
            )
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to disassociate location: {str(e)}"
            )


class UpdateLocationContextRequest(BaseModel):
    """Request model for updating location context."""
    context_overrides: Dict[str, Any] = Field(..., description="Context overrides to apply")


@router.put("/{simulation_id}/locations/{location_name}/context", response_model=SimulationDto)
async def update_simulation_location_context(
    simulation_id: str,
    location_name: str,
    context_data: UpdateLocationContextRequest,
    simulation_service: SimulationApplicationService = Depends(get_simulation_service)
):
    """
    Update the context for a specific location association.
    
    Args:
        simulation_id: The simulation identifier
        location_name: The location name
        context_data: Context overrides to apply
        
    Returns:
        Updated simulation with modified location context
        
    Raises:
        404: If simulation is not found
    """
    try:
        # Update the location context using the service
        updated_simulation = simulation_service.update_simulation_location_context(
            simulation_id=simulation_id,
            location_name=location_name,
            context_overrides=context_data.context_overrides
        )
        
        return updated_simulation
        
    except HTTPException:
        raise
    except Exception as e:
        if "not found" in str(e).lower():
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Simulation '{simulation_id}' or location '{location_name}' not found"
            )
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to update location context: {str(e)}"
            )


# Files endpoints

class SimulationFilesResponse(BaseModel):
    """Response model for simulation files."""
    simulation_id: str = Field(..., description="The simulation identifier")
    files: List[Dict[str, Any]] = Field(..., description="List of files")


@router.get("/{simulation_id}/files", response_model=SimulationFilesResponse)
async def get_simulation_files(
    simulation_id: str,
    simulation_service: SimulationApplicationService = Depends(get_simulation_service)
):
    """
    Get files associated with a simulation.
    
    Args:
        simulation_id: The simulation identifier
        
    Returns:
        List of files associated with the simulation
        
    Raises:
        404: If simulation is not found
    """
    try:
        # Get files using the service
        files = simulation_service.get_simulation_files(simulation_id)
        
        # Convert to dict format for response
        files_data = [file_dto.model_dump() for file_dto in files]
        
        return SimulationFilesResponse(
            simulation_id=simulation_id,
            files=files_data
        )
        
    except HTTPException:
        raise
    except Exception as e:
        if "not found" in str(e).lower():
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Simulation '{simulation_id}' not found"
            )
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to get simulation files: {str(e)}"
            )