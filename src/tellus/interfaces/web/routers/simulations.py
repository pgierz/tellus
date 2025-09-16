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
from datetime import datetime
from fastapi import APIRouter, HTTPException, Depends, Query, Request
from fastapi import status
from pydantic import BaseModel, Field

from ....application.dtos import (
    SimulationDto, CreateSimulationDto, UpdateSimulationDto,
    SimulationListDto, PaginationInfo, FilterOptions,
    SimulationLocationAssociationDto
)
from ....application.services.simulation_service import SimulationApplicationService
from ....application.services.unified_file_service import UnifiedFileService
from ..dependencies import get_simulation_service, get_unified_file_service

router = APIRouter()


# Pydantic models for attributes API
class AttributeRequest(BaseModel):
    """Request model for setting an attribute."""
    key: str = Field(..., min_length=1, description="The attribute key")
    value: str = Field(..., description="The attribute value")


class AttributeResponse(BaseModel):
    """Response model for an attribute."""
    key: str = Field(..., description="The attribute key")
    value: str = Field(..., description="The attribute value")


class AttributesResponse(BaseModel):
    """Response model for all attributes of a simulation."""
    simulation_id: str = Field(..., description="The simulation identifier")
    attributes: Dict[str, Any] = Field(..., description="All simulation attributes")


# Pydantic models for archive API
class CreateArchiveRequest(BaseModel):
    """Request model for creating an archive."""
    archive_name: str = Field(..., description="Name of the archive")
    description: Optional[str] = Field(None, description="Optional archive description")
    location: Optional[str] = Field(None, description="Location where archive files exist")
    pattern: Optional[str] = Field(None, description="File pattern for archive files")
    split_parts: Optional[int] = Field(None, description="Number of split parts for split archives")
    archive_type: str = Field("single", description="Archive type (single, split-tar)")


class ArchiveResponse(BaseModel):
    """Response model for an archive."""
    archive_id: str = Field(..., description="The archive identifier")
    archive_name: str = Field(..., description="The archive name")
    simulation_id: str = Field(..., description="Associated simulation ID")
    location: Optional[str] = Field(None, description="Archive location")
    pattern: Optional[str] = Field(None, description="File pattern")
    split_parts: Optional[int] = Field(None, description="Number of split parts")
    archive_type: str = Field(..., description="Archive type")
    description: Optional[str] = Field(None, description="Archive description")
    created_at: Optional[str] = Field(None, description="Creation timestamp")


class ArchiveListResponse(BaseModel):
    """Response model for listing archives."""
    simulation_id: str = Field(..., description="The simulation identifier")
    archives: List[ArchiveResponse] = Field(..., description="List of archives")


class ArchiveDeleteResponse(BaseModel):
    """Response model for archive deletion."""
    archive_id: str = Field(..., description="The deleted archive identifier")
    status: str = Field(..., description="Deletion status")


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


# Archive management endpoints
@router.post("/{simulation_id}/archives", response_model=ArchiveResponse, status_code=status.HTTP_201_CREATED)
async def create_archive(
    simulation_id: str,
    request: CreateArchiveRequest,
    file_service: UnifiedFileService = Depends(get_unified_file_service)
):
    """
    Create a new archive for a simulation.
    
    Args:
        simulation_id: The simulation identifier
        request: Archive creation parameters
        
    Returns:
        Created archive information
        
    Raises:
        404: If simulation is not found
        409: If archive already exists
    """
    try:
        from ....application.dtos import CreateArchiveDto
        from ....domain.entities.simulation_file import FileContentType, FileImportance
        
        # Create archive using the unified file service
        create_dto = CreateArchiveDto(
            simulation_id=simulation_id,
            archive_name=request.archive_name,
            archive_description=request.description,
            location=request.location,
            file_pattern=request.pattern,
            split_parts=request.split_parts,
            archive_type=request.archive_type
        )
        
        archive = file_service.create_archive(create_dto)
        
        return ArchiveResponse(
            archive_id=archive.relative_path,
            archive_name=request.archive_name,
            simulation_id=simulation_id,
            location=request.location,
            pattern=request.pattern,
            split_parts=request.split_parts,
            archive_type=request.archive_type,
            description=request.description,
            created_at=datetime.fromtimestamp(archive.created_time).isoformat() if archive.created_time else None
        )
        
    except Exception as e:
        if "already exists" in str(e).lower():
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Archive '{request.archive_name}' already exists for simulation '{simulation_id}'"
            )
        elif "not found" in str(e).lower():
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Simulation '{simulation_id}' not found"
            )
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to create archive: {str(e)}"
            )


@router.get("/{simulation_id}/archives", response_model=ArchiveListResponse)
async def list_archives(
    simulation_id: str,
    file_service: UnifiedFileService = Depends(get_unified_file_service)
):
    """
    List all archives for a simulation.
    
    Args:
        simulation_id: The simulation identifier
        
    Returns:
        List of archives for the simulation
        
    Raises:
        404: If simulation is not found
    """
    try:
        archives = file_service.list_simulation_archives(simulation_id)
        
        archive_responses = []
        for archive in archives:
            archive_responses.append(ArchiveResponse(
                archive_id=archive.relative_path,
                archive_name=archive.attributes.get('archive_name', archive.relative_path),
                simulation_id=simulation_id,
                location=archive.attributes.get('location'),
                pattern=archive.attributes.get('pattern'),
                split_parts=archive.attributes.get('split_parts'),
                archive_type=archive.attributes.get('archive_type', 'single'),
                description=archive.attributes.get('description'),
                created_at=datetime.fromtimestamp(archive.created_time).isoformat() if archive.created_time else None
            ))
        
        return ArchiveListResponse(
            simulation_id=simulation_id,
            archives=archive_responses
        )
        
    except Exception as e:
        if "not found" in str(e).lower():
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Simulation '{simulation_id}' not found"
            )
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to list archives: {str(e)}"
            )


@router.get("/{simulation_id}/archives/{archive_id}", response_model=ArchiveResponse)
async def get_archive(
    simulation_id: str,
    archive_id: str,
    file_service: UnifiedFileService = Depends(get_unified_file_service)
):
    """
    Get details of a specific archive.
    
    Args:
        simulation_id: The simulation identifier
        archive_id: The archive identifier
        
    Returns:
        Archive details
        
    Raises:
        404: If simulation or archive is not found
    """
    try:
        archive = file_service.get_archive(archive_id)
        
        if not archive:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Archive '{archive_id}' not found"
            )
        
        return ArchiveResponse(
            archive_id=archive.relative_path,
            archive_name=archive.attributes.get('archive_name', archive.relative_path),
            simulation_id=simulation_id,
            location=archive.attributes.get('location'),
            pattern=archive.attributes.get('pattern'),
            split_parts=archive.attributes.get('split_parts'),
            archive_type=archive.attributes.get('archive_type', 'single'),
            description=archive.attributes.get('description'),
            created_at=datetime.fromtimestamp(archive.created_time).isoformat() if archive.created_time else None
        )
        
    except HTTPException:
        raise
    except Exception as e:
        if "not found" in str(e).lower():
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Archive '{archive_id}' not found"
            )
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to get archive: {str(e)}"
            )


@router.delete("/{simulation_id}/archives/{archive_id}", response_model=ArchiveDeleteResponse)
async def delete_archive(
    simulation_id: str,
    archive_id: str,
    file_service: UnifiedFileService = Depends(get_unified_file_service)
):
    """
    Delete an archive.
    
    Args:
        simulation_id: The simulation identifier
        archive_id: The archive identifier
        
    Returns:
        Deletion confirmation
        
    Raises:
        404: If simulation or archive is not found
    """
    try:
        # Check if archive exists first
        archive = file_service.get_archive(archive_id)
        if not archive:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Archive '{archive_id}' not found"
            )
        
        # Delete the archive
        file_service.remove_file(archive_id)
        
        return ArchiveDeleteResponse(
            archive_id=archive_id,
            status="deleted"
        )
        
    except HTTPException:
        raise
    except Exception as e:
        if "not found" in str(e).lower():
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Archive '{archive_id}' not found"
            )
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to delete archive: {str(e)}"
            )


# === Archive Content Management ===

class ArchiveContentResponse(BaseModel):
    """Response model for archive content listing."""
    archive_id: str = Field(..., description="The archive identifier")
    files: List[Dict[str, Any]] = Field(..., description="List of files in the archive")
    total_files: int = Field(..., description="Total number of files")


@router.get(
    "/{simulation_id}/archives/{archive_id}/contents",
    response_model=ArchiveContentResponse,
    status_code=status.HTTP_200_OK
)
async def list_archive_contents(
    simulation_id: str,
    archive_id: str,
    file_service: UnifiedFileService = Depends(get_unified_file_service),
    file_filter: Optional[str] = Query(None, description="Filter files by pattern (e.g., '*.nc')"),
    content_type_filter: Optional[str] = Query(None, description="Filter by content type")
):
    """
    List contents of an archive without extraction.
    
    Args:
        simulation_id: The simulation identifier
        archive_id: The archive identifier
        file_filter: Optional file pattern filter
        content_type_filter: Optional content type filter
        
    Returns:
        Archive content listing
        
    Raises:
        404: If archive is not found
    """
    try:
        # Check if archive exists
        archive = file_service.get_archive(archive_id)
        if not archive:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Archive '{archive_id}' not found"
            )
        
        # Get archive contents (child files)
        child_files = file_service.get_file_children(archive_id)
        
        # Apply filters
        if file_filter:
            import fnmatch
            child_files = [f for f in child_files if fnmatch.fnmatch(f.relative_path, file_filter)]
        
        if content_type_filter:
            from ....domain.entities.simulation_file import FileContentType
            try:
                content_type = FileContentType(content_type_filter)
                child_files = [f for f in child_files if f.content_type == content_type]
            except ValueError:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Invalid content type: {content_type_filter}"
                )
        
        # Convert to response format
        file_list = []
        for file_obj in child_files:
            file_dict = {
                "file_path": file_obj.relative_path,
                "size_bytes": file_obj.size_bytes,
                "content_type": file_obj.content_type.value if file_obj.content_type else None,
                "file_type": file_obj.file_type.value if file_obj.file_type else None,
                "created_at": file_obj.created_at.isoformat() if file_obj.created_at else None,
                "attributes": file_obj.attributes
            }
            file_list.append(file_dict)
        
        return ArchiveContentResponse(
            archive_id=archive_id,
            files=file_list,
            total_files=len(file_list)
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to list archive contents: {str(e)}"
        )


class IndexArchiveRequest(BaseModel):
    """Request model for indexing an archive."""
    force: bool = Field(False, description="Force re-indexing even if already indexed")


class IndexArchiveResponse(BaseModel):
    """Response model for archive indexing."""
    archive_id: str = Field(..., description="The archive identifier")
    status: str = Field(..., description="Indexing status")
    files_indexed: int = Field(..., description="Number of files indexed")


@router.post(
    "/{simulation_id}/archives/{archive_id}/index",
    response_model=IndexArchiveResponse,
    status_code=status.HTTP_200_OK
)
async def index_archive_contents(
    simulation_id: str,
    archive_id: str,
    request: IndexArchiveRequest,
    file_service: UnifiedFileService = Depends(get_unified_file_service)
):
    """
    Create content index for archives.
    
    Analyzes archive contents and stores metadata for fast querying without
    needing to download/extract archives.
    
    Args:
        simulation_id: The simulation identifier
        archive_id: The archive identifier
        request: Indexing request parameters
        
    Returns:
        Indexing results
        
    Raises:
        404: If archive is not found
    """
    try:
        # Check if archive exists
        archive = file_service.get_archive(archive_id)
        if not archive:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Archive '{archive_id}' not found"
            )
        
        # Get current child files count
        existing_children = file_service.get_file_children(archive_id)
        
        # For now, indexing means ensuring archive metadata is up-to-date
        # In a full implementation, this would analyze archive contents and create file entries
        if existing_children and not request.force:
            # Archive already indexed
            return IndexArchiveResponse(
                archive_id=archive_id,
                status="already_indexed",
                files_indexed=len(existing_children)
            )
        
        # TODO: Implement actual archive content analysis
        # This would involve:
        # 1. Reading archive headers/metadata
        # 2. Creating SimulationFile entries for each contained file
        # 3. Setting up parent-child relationships
        
        # For now, return current state
        return IndexArchiveResponse(
            archive_id=archive_id,
            status="indexed" if request.force else "already_indexed",
            files_indexed=len(existing_children)
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to index archive: {str(e)}"
        )


# === File Management Operations ===

class FileListResponse(BaseModel):
    """Response model for file listing."""
    simulation_id: str = Field(..., description="The simulation identifier")
    files: List[Dict[str, Any]] = Field(..., description="List of files")
    total_files: int = Field(..., description="Total number of files")


@router.get(
    "/{simulation_id}/files",
    response_model=FileListResponse,
    status_code=status.HTTP_200_OK
)
async def list_simulation_files(
    simulation_id: str,
    file_service: UnifiedFileService = Depends(get_unified_file_service),
    location: Optional[str] = Query(None, description="Filter by location"),
    content_type: Optional[str] = Query(None, description="Filter by content type"),
    file_type: Optional[str] = Query(None, description="Filter by file type (regular, archive, directory)")
):
    """
    List files associated with a simulation.
    
    Args:
        simulation_id: The simulation identifier
        location: Optional location filter
        content_type: Optional content type filter
        file_type: Optional file type filter
        
    Returns:
        List of files associated with the simulation
    """
    try:
        # Get simulation files
        files = file_service.get_simulation_files(simulation_id)
        
        # Apply filters
        if location:
            files = [f for f in files if f.location_name == location]
        
        if content_type:
            from ....domain.entities.simulation_file import FileContentType
            try:
                ct = FileContentType(content_type)
                files = [f for f in files if f.content_type == ct]
            except ValueError:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Invalid content type: {content_type}"
                )
        
        if file_type:
            from ....domain.entities.simulation_file import FileType
            try:
                ft = FileType(file_type)
                files = [f for f in files if f.file_type == ft]
            except ValueError:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Invalid file type: {file_type}"
                )
        
        # Convert to response format
        file_list = []
        for file_obj in files:
            file_dict = {
                "file_path": file_obj.relative_path,
                "location": file_obj.location_name,
                "size_bytes": file_obj.size_bytes,
                "content_type": file_obj.content_type.value if file_obj.content_type else None,
                "file_type": file_obj.file_type.value if file_obj.file_type else None,
                "created_at": file_obj.created_at.isoformat() if file_obj.created_at else None,
                "parent_file": file_obj.parent_file_id,
                "attributes": file_obj.attributes
            }
            file_list.append(file_dict)
        
        return FileListResponse(
            simulation_id=simulation_id,
            files=file_list,
            total_files=len(file_list)
        )
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to list files: {str(e)}"
        )


class RegisterFilesRequest(BaseModel):
    """Request model for registering files to a simulation."""
    archive_id: str = Field(..., description="Archive ID to register files from")
    content_type_filter: Optional[str] = Field(None, description="Filter files by content type")
    pattern_filter: Optional[str] = Field(None, description="Filter files by pattern (glob)")
    overwrite_existing: bool = Field(False, description="Overwrite existing file registrations")


class RegisterFilesResponse(BaseModel):
    """Response model for file registration."""
    simulation_id: str = Field(..., description="The simulation identifier")
    archive_id: str = Field(..., description="The archive identifier")
    registered_count: int = Field(..., description="Number of files registered")
    updated_count: int = Field(..., description="Number of existing files updated")
    skipped_count: int = Field(..., description="Number of files skipped")
    status: str = Field(..., description="Operation status")


@router.post(
    "/{simulation_id}/files/register",
    response_model=RegisterFilesResponse,
    status_code=status.HTTP_201_CREATED
)
async def register_files_to_simulation(
    simulation_id: str,
    request: RegisterFilesRequest,
    file_service: UnifiedFileService = Depends(get_unified_file_service)
):
    """
    Register files from an archive to a simulation.
    
    Args:
        simulation_id: The simulation identifier
        request: File registration parameters
        
    Returns:
        Registration results
        
    Raises:
        404: If archive is not found
        400: If validation fails
    """
    try:
        from ....application.dtos import FileRegistrationDto
        
        # Create registration DTO
        registration_dto = FileRegistrationDto(
            simulation_id=simulation_id,
            archive_id=request.archive_id,
            content_type_filter=request.content_type_filter,
            pattern_filter=request.pattern_filter,
            overwrite_existing=request.overwrite_existing
        )
        
        # Register files
        result = file_service.register_files_to_simulation(registration_dto)
        
        return RegisterFilesResponse(
            simulation_id=simulation_id,
            archive_id=request.archive_id,
            registered_count=result.registered_count,
            updated_count=result.updated_count,
            skipped_count=result.skipped_count,
            status="completed"
        )
        
    except Exception as e:
        if "not found" in str(e).lower():
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=str(e)
            )
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to register files: {str(e)}"
            )


class UnregisterFilesRequest(BaseModel):
    """Request model for unregistering files from a simulation."""
    archive_id: str = Field(..., description="Archive ID to unregister files from")
    content_type_filter: Optional[str] = Field(None, description="Filter files by content type")
    pattern_filter: Optional[str] = Field(None, description="Filter files by pattern (glob)")


class UnregisterFilesResponse(BaseModel):
    """Response model for file unregistration."""
    simulation_id: str = Field(..., description="The simulation identifier")
    archive_id: str = Field(..., description="The archive identifier")
    unregistered_count: int = Field(..., description="Number of files unregistered")
    status: str = Field(..., description="Operation status")


@router.delete(
    "/{simulation_id}/files/unregister",
    response_model=UnregisterFilesResponse,
    status_code=status.HTTP_200_OK
)
async def unregister_files_from_simulation(
    simulation_id: str,
    request: UnregisterFilesRequest,
    file_service: UnifiedFileService = Depends(get_unified_file_service)
):
    """
    Unregister files from a simulation.
    
    Args:
        simulation_id: The simulation identifier
        request: File unregistration parameters
        
    Returns:
        Unregistration results
        
    Raises:
        404: If archive is not found
    """
    try:
        # Get files to unregister
        simulation_files = file_service.get_simulation_files(simulation_id)
        
        # Filter by archive
        archive_files = [f for f in simulation_files if f.parent_file_id == request.archive_id]
        
        # Apply additional filters
        if request.content_type_filter:
            from ....domain.entities.simulation_file import FileContentType
            try:
                ct = FileContentType(request.content_type_filter)
                archive_files = [f for f in archive_files if f.content_type == ct]
            except ValueError:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Invalid content type: {request.content_type_filter}"
                )
        
        if request.pattern_filter:
            import fnmatch
            archive_files = [f for f in archive_files if fnmatch.fnmatch(f.relative_path, request.pattern_filter)]
        
        # Unregister files
        unregistered_count = 0
        for file_obj in archive_files:
            if 'simulation_id' in file_obj.attributes:
                del file_obj.attributes['simulation_id']
                file_service.file_repository.save(file_obj)
                unregistered_count += 1
        
        return UnregisterFilesResponse(
            simulation_id=simulation_id,
            archive_id=request.archive_id,
            unregistered_count=unregistered_count,
            status="completed"
        )
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to unregister files: {str(e)}"
        )


class FileStatusResponse(BaseModel):
    """Response model for file status."""
    simulation_id: str = Field(..., description="The simulation identifier")
    total_files: int = Field(..., description="Total number of registered files")
    files_by_archive: Dict[str, int] = Field(..., description="File count by archive")
    files_by_content_type: Dict[str, int] = Field(..., description="File count by content type")
    files_by_location: Dict[str, int] = Field(..., description="File count by location")


@router.get(
    "/{simulation_id}/files/status",
    response_model=FileStatusResponse,
    status_code=status.HTTP_200_OK
)
async def get_simulation_files_status(
    simulation_id: str,
    file_service: UnifiedFileService = Depends(get_unified_file_service)
):
    """
    Show file status and archive associations.
    
    Similar to 'git status' - shows the current state of files
    associated with this simulation and their archive sources.
    
    Args:
        simulation_id: The simulation identifier
        
    Returns:
        File status summary
    """
    try:
        # Get simulation files
        files = file_service.get_simulation_files(simulation_id)
        
        # Calculate statistics
        files_by_archive = {}
        files_by_content_type = {}
        files_by_location = {}
        
        for file_obj in files:
            # Count by archive
            archive_key = file_obj.parent_file_id or "no_archive"
            files_by_archive[archive_key] = files_by_archive.get(archive_key, 0) + 1
            
            # Count by content type
            ct_key = file_obj.content_type.value if file_obj.content_type else "unknown"
            files_by_content_type[ct_key] = files_by_content_type.get(ct_key, 0) + 1
            
            # Count by location
            loc_key = file_obj.location_name or "no_location"
            files_by_location[loc_key] = files_by_location.get(loc_key, 0) + 1
        
        return FileStatusResponse(
            simulation_id=simulation_id,
            total_files=len(files),
            files_by_archive=files_by_archive,
            files_by_content_type=files_by_content_type,
            files_by_location=files_by_location
        )
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get file status: {str(e)}"
        )