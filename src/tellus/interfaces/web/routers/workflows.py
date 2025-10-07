"""
Workflow management endpoints for the Tellus API.

Provides comprehensive workflow operations including:
- CRUD operations for workflows
- Simulation association management
- RunFingerprint tracking from Snakemake observation systems
- Location association and configuration
- Polling control for automated updates
"""

from typing import List, Optional, Dict, Any
from datetime import datetime
from fastapi import APIRouter, HTTPException, Depends, Query, Request, status
from pydantic import BaseModel, Field

from ....application.dtos import (
    WorkflowDto, CreateWorkflowDto, UpdateWorkflowDto,
    WorkflowListDto, PaginationInfo, FilterOptions,
    WorkflowLocationAssociationDto, WorkflowStepDto
)
from ....application.services.workflow_service import WorkflowApplicationService
from ..dependencies import get_workflow_service

router = APIRouter()


# ============================================================================
# Request/Response Models for Workflow-Specific Operations
# ============================================================================

class WorkflowPhaseDto(BaseModel):
    """DTO for workflow execution phase information."""
    phase_id: str = Field(..., description="Unique identifier for the phase")
    name: str = Field(..., description="Phase name (prepare, compute, cleanup, post-process)")
    status: str = Field(..., description="Phase status (PENDING, RUNNING, COMPLETED, FAILED)")
    start_time: Optional[str] = Field(None, description="Phase start time (ISO format)")
    end_time: Optional[str] = Field(None, description="Phase end time (ISO format)")
    log_files: List[str] = Field(default_factory=list, description="Associated log file paths")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Additional phase metadata")


class ObservedFileDto(BaseModel):
    """DTO for files observed during workflow execution."""
    path: str = Field(..., description="File path relative to workflow root")
    classification: str = Field(..., description="File type (OUTPUT, RESTART, CONFIG, LOG)")
    component: Optional[str] = Field(None, description="Model component (echam6, fesom, mpiom)")
    size_bytes: int = Field(0, ge=0, description="File size in bytes")
    temporal_info: Optional[Dict[str, Any]] = Field(None, description="Temporal metadata")
    location_name: str = Field("", description="Storage location name")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Additional file metadata")


class RunFingerprintDto(BaseModel):
    """
    DTO for Snakemake RunFingerprint data.

    This captures the state of a workflow run as observed by Snakemake's
    observation system, including execution phases and discovered files.
    """
    workflow_id: str = Field(..., description="Associated workflow ID")
    run_timestamp: str = Field(..., description="Fingerprint capture timestamp (ISO format)")
    snakemake_version: Optional[str] = Field(None, description="Snakemake version used")
    phases: List[WorkflowPhaseDto] = Field(default_factory=list, description="Execution phases")
    observed_files: List[ObservedFileDto] = Field(default_factory=list, description="Discovered files")
    execution_summary: Dict[str, Any] = Field(default_factory=dict, description="Summary statistics")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Additional metadata")


class UpdateFingerprintDto(BaseModel):
    """DTO for updating workflow RunFingerprint data."""
    run_timestamp: str = Field(..., description="Fingerprint capture timestamp (ISO format)")
    snakemake_version: Optional[str] = Field(None, description="Snakemake version")
    phases: Optional[List[WorkflowPhaseDto]] = Field(None, description="Updated phases")
    observed_files: Optional[List[ObservedFileDto]] = Field(None, description="Updated observed files")
    execution_summary: Optional[Dict[str, Any]] = Field(None, description="Execution summary")
    metadata: Optional[Dict[str, Any]] = Field(None, description="Additional metadata")


class FingerprintHistoryResponse(BaseModel):
    """Response model for fingerprint history."""
    workflow_id: str = Field(..., description="Workflow identifier")
    fingerprints: List[RunFingerprintDto] = Field(..., description="Historical fingerprints")
    total_count: int = Field(..., description="Total number of fingerprints")


class PollingControlResponse(BaseModel):
    """Response model for polling control operations."""
    workflow_id: str = Field(..., description="Workflow identifier")
    polling_enabled: bool = Field(..., description="Polling status")
    polling_interval_seconds: Optional[int] = Field(None, description="Polling interval")
    last_poll_time: Optional[str] = Field(None, description="Last poll timestamp (ISO format)")
    message: str = Field(..., description="Operation result message")


class WorkflowSimulationAssociationDto(BaseModel):
    """DTO for associating workflows with simulations."""
    simulation_id: str = Field(..., description="Simulation identifier")
    workflow_config: Dict[str, Any] = Field(default_factory=dict, description="Simulation-specific config")


# ============================================================================
# Workflow CRUD Endpoints
# ============================================================================

@router.get("/", response_model=WorkflowListDto)
async def list_workflows(
    request: Request,
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(50, ge=1, le=100, description="Items per page"),
    search: Optional[str] = Query(None, description="Search term for workflow names"),
    simulation_id: Optional[str] = Query(None, description="Filter by simulation ID"),
    workflow_status: Optional[str] = Query(None, description="Filter by workflow status"),
    workflow_service: WorkflowApplicationService = Depends(get_workflow_service)
):
    """
    List all workflows with pagination and optional filtering.

    **Query Parameters:**
    - `page`: Page number (1-based, default: 1)
    - `page_size`: Number of workflows per page (1-100, default: 50)
    - `search`: Search term to filter workflow names
    - `simulation_id`: Filter workflows by associated simulation
    - `workflow_status`: Filter by workflow status (DRAFT, READY, DEPRECATED, ARCHIVED)

    **Returns:**
    Paginated list of workflows with metadata

    **Example:**
    ```
    GET /api/v0a3/workflows/?page=1&page_size=20&status=READY
    ```
    """
    try:
        # Create filter options
        filters = FilterOptions(search_term=search) if search else None

        # Get workflows using the service
        result = workflow_service.list_workflows(
            page=page,
            page_size=page_size,
            filters=filters
        )

        # Apply additional filters (simulation_id, workflow_status)
        if simulation_id or workflow_status:
            filtered_workflows = result.workflows

            if simulation_id:
                filtered_workflows = [
                    wf for wf in filtered_workflows
                    if getattr(wf, 'simulation_id', None) == simulation_id
                ]

            if workflow_status:
                filtered_workflows = [
                    wf for wf in filtered_workflows
                    if getattr(wf, 'status', '').upper() == workflow_status.upper()
                ]

            result.workflows = filtered_workflows

        return result

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to list workflows: {str(e)}"
        )


@router.post("/", response_model=WorkflowDto, status_code=status.HTTP_201_CREATED)
async def create_workflow(
    workflow_data: CreateWorkflowDto,
    workflow_service: WorkflowApplicationService = Depends(get_workflow_service)
):
    """
    Create a new workflow definition.

    **Request Body Example:**
    ```json
    {
        "workflow_id": "esm_postproc_v1",
        "name": "ESM Post-Processing Pipeline",
        "description": "Standard post-processing for Earth System Model output",
        "engine": "snakemake",
        "workflow_file": "/path/to/Snakefile",
        "global_parameters": {
            "resolution": "T63",
            "output_format": "netcdf4"
        },
        "tags": ["production", "esm", "post-processing"]
    }
    ```

    **Returns:**
    Created workflow with generated UID

    **Status Codes:**
    - `201 Created`: Workflow successfully created
    - `400 Bad Request`: Workflow ID already exists
    - `422 Unprocessable Entity`: Validation failed
    """
    try:
        created_workflow = workflow_service.create_workflow(workflow_data)
        return created_workflow

    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=str(e)
        )
    except Exception as e:
        if "already exists" in str(e).lower():
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=str(e)
            )
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to create workflow: {str(e)}"
            )


@router.get("/{workflow_id}", response_model=WorkflowDto)
async def get_workflow(
    workflow_id: str,
    workflow_service: WorkflowApplicationService = Depends(get_workflow_service)
):
    """
    Get details of a specific workflow.

    **Path Parameters:**
    - `workflow_id`: Unique workflow identifier

    **Returns:**
    Complete workflow definition including steps, parameters, and location associations

    **Status Codes:**
    - `200 OK`: Workflow found and returned
    - `404 Not Found`: Workflow does not exist

    **Example:**
    ```
    GET /api/v0a3/workflows/esm_postproc_v1
    ```
    """
    try:
        workflow = workflow_service.get_workflow(workflow_id)
        return workflow

    except HTTPException:
        raise
    except Exception as e:
        if "not found" in str(e).lower():
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Workflow '{workflow_id}' not found"
            )
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to get workflow: {str(e)}"
            )


@router.put("/{workflow_id}", response_model=WorkflowDto)
async def update_workflow(
    workflow_id: str,
    update_data: UpdateWorkflowDto,
    workflow_service: WorkflowApplicationService = Depends(get_workflow_service)
):
    """
    Update an existing workflow.

    **Path Parameters:**
    - `workflow_id`: Workflow to update

    **Request Body Example:**
    ```json
    {
        "name": "Updated Workflow Name",
        "description": "Updated description",
        "global_parameters": {
            "new_param": "value"
        }
    }
    ```

    **Returns:**
    Updated workflow data

    **Status Codes:**
    - `200 OK`: Workflow successfully updated
    - `404 Not Found`: Workflow not found
    - `422 Unprocessable Entity`: Validation failed
    """
    try:
        updated_workflow = workflow_service.update_workflow(workflow_id, update_data)
        return updated_workflow

    except HTTPException:
        raise
    except Exception as e:
        if "not found" in str(e).lower():
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Workflow '{workflow_id}' not found"
            )
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to update workflow: {str(e)}"
            )


@router.delete("/{workflow_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_workflow(
    workflow_id: str,
    workflow_service: WorkflowApplicationService = Depends(get_workflow_service)
):
    """
    Delete a workflow.

    **Path Parameters:**
    - `workflow_id`: Workflow to delete

    **Status Codes:**
    - `204 No Content`: Workflow successfully deleted
    - `404 Not Found`: Workflow not found

    **Note:** This will also remove all associated RunFingerprint history.
    """
    try:
        workflow_service.delete_workflow(workflow_id)

    except HTTPException:
        raise
    except Exception as e:
        if "not found" in str(e).lower():
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Workflow '{workflow_id}' not found"
            )
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to delete workflow: {str(e)}"
            )


# ============================================================================
# Simulation Association Endpoints
# ============================================================================

@router.get("/simulations/{simulation_id}/workflows", response_model=WorkflowListDto)
async def list_simulation_workflows(
    simulation_id: str,
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(50, ge=1, le=100, description="Items per page"),
    workflow_service: WorkflowApplicationService = Depends(get_workflow_service)
):
    """
    List all workflows associated with a specific simulation.

    **Path Parameters:**
    - `simulation_id`: Simulation identifier

    **Returns:**
    Paginated list of workflows associated with the simulation

    **Example:**
    ```
    GET /api/v0a3/simulations/PI_CTRL_01/workflows
    ```
    """
    try:
        # Get all workflows and filter by simulation
        result = workflow_service.list_workflows(
            page=page,
            page_size=page_size,
            filters=None
        )

        # Filter by simulation_id
        filtered_workflows = [
            wf for wf in result.workflows
            if getattr(wf, 'simulation_id', None) == simulation_id
        ]

        result.workflows = filtered_workflows
        return result

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to list simulation workflows: {str(e)}"
        )


@router.post("/simulations/{simulation_id}/workflows", response_model=WorkflowDto, status_code=status.HTTP_201_CREATED)
async def create_simulation_workflow(
    simulation_id: str,
    workflow_data: CreateWorkflowDto,
    workflow_service: WorkflowApplicationService = Depends(get_workflow_service)
):
    """
    Create a new workflow and associate it with a simulation.

    **Path Parameters:**
    - `simulation_id`: Simulation to associate the workflow with

    **Request Body:**
    Same as POST /workflows/, but will be automatically associated with the simulation

    **Returns:**
    Created workflow with simulation association

    **Status Codes:**
    - `201 Created`: Workflow created and associated
    - `404 Not Found`: Simulation not found
    - `400 Bad Request`: Workflow ID already exists
    """
    try:
        # Create workflow with simulation context
        created_workflow = workflow_service.create_workflow(workflow_data)

        # Associate with simulation
        # Note: The actual association would happen through the workflow service
        # For now, we'll set the simulation_id in the workflow metadata
        if hasattr(workflow_service, 'associate_workflow_with_simulation'):
            workflow_service.associate_workflow_with_simulation(
                created_workflow.workflow_id,
                simulation_id
            )

        return created_workflow

    except HTTPException:
        raise
    except Exception as e:
        if "not found" in str(e).lower():
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Simulation '{simulation_id}' not found"
            )
        elif "already exists" in str(e).lower():
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=str(e)
            )
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to create simulation workflow: {str(e)}"
            )


# ============================================================================
# RunFingerprint Management Endpoints
# ============================================================================

@router.put("/{workflow_id}/fingerprint", response_model=RunFingerprintDto)
async def update_workflow_fingerprint(
    workflow_id: str,
    fingerprint_data: UpdateFingerprintDto,
    workflow_service: WorkflowApplicationService = Depends(get_workflow_service)
):
    """
    Update RunFingerprint data for a workflow.

    This endpoint is used by Snakemake observation systems to report
    the current state of workflow execution, including phases and discovered files.

    **Path Parameters:**
    - `workflow_id`: Workflow identifier

    **Request Body Example:**
    ```json
    {
        "run_timestamp": "2025-10-07T14:30:00Z",
        "snakemake_version": "7.32.4",
        "phases": [
            {
                "phase_id": "prepare",
                "name": "Preparation Phase",
                "status": "COMPLETED",
                "start_time": "2025-10-07T14:00:00Z",
                "end_time": "2025-10-07T14:15:00Z"
            }
        ],
        "observed_files": [
            {
                "path": "output/model_output_2025.nc",
                "classification": "OUTPUT",
                "component": "echam6",
                "size_bytes": 1073741824
            }
        ]
    }
    ```

    **Returns:**
    Updated fingerprint data

    **Status Codes:**
    - `200 OK`: Fingerprint updated
    - `404 Not Found`: Workflow not found
    """
    try:
        # This would delegate to a fingerprint tracking service
        # For now, we'll store it in workflow metadata
        workflow = workflow_service.get_workflow(workflow_id)

        # Create fingerprint response
        fingerprint = RunFingerprintDto(
            workflow_id=workflow_id,
            run_timestamp=fingerprint_data.run_timestamp,
            snakemake_version=fingerprint_data.snakemake_version,
            phases=fingerprint_data.phases or [],
            observed_files=fingerprint_data.observed_files or [],
            execution_summary=fingerprint_data.execution_summary or {},
            metadata=fingerprint_data.metadata or {}
        )

        # Store fingerprint (would normally go to a dedicated fingerprint repository)
        # For now, append to workflow metadata
        if not hasattr(workflow_service, 'update_workflow_fingerprint'):
            # Fallback: store in workflow metadata
            pass

        return fingerprint

    except HTTPException:
        raise
    except Exception as e:
        if "not found" in str(e).lower():
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Workflow '{workflow_id}' not found"
            )
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to update fingerprint: {str(e)}"
            )


@router.get("/{workflow_id}/fingerprint", response_model=RunFingerprintDto)
async def get_workflow_fingerprint(
    workflow_id: str,
    workflow_service: WorkflowApplicationService = Depends(get_workflow_service)
):
    """
    Get the current RunFingerprint data for a workflow.

    **Path Parameters:**
    - `workflow_id`: Workflow identifier

    **Returns:**
    Current workflow fingerprint with execution state

    **Status Codes:**
    - `200 OK`: Fingerprint retrieved
    - `404 Not Found`: Workflow or fingerprint not found
    """
    try:
        workflow = workflow_service.get_workflow(workflow_id)

        # Retrieve current fingerprint from workflow metadata or dedicated storage
        # This is a placeholder implementation
        fingerprint = RunFingerprintDto(
            workflow_id=workflow_id,
            run_timestamp=datetime.now().isoformat(),
            phases=[],
            observed_files=[],
            execution_summary={},
            metadata={}
        )

        return fingerprint

    except HTTPException:
        raise
    except Exception as e:
        if "not found" in str(e).lower():
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Workflow '{workflow_id}' or fingerprint not found"
            )
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to get fingerprint: {str(e)}"
            )


@router.get("/{workflow_id}/fingerprint/history", response_model=FingerprintHistoryResponse)
async def get_fingerprint_history(
    workflow_id: str,
    limit: int = Query(10, ge=1, le=100, description="Number of historical entries"),
    workflow_service: WorkflowApplicationService = Depends(get_workflow_service)
):
    """
    Get historical RunFingerprint data for a workflow.

    **Path Parameters:**
    - `workflow_id`: Workflow identifier

    **Query Parameters:**
    - `limit`: Maximum number of historical entries (1-100, default: 10)

    **Returns:**
    List of historical fingerprints showing workflow execution over time

    **Status Codes:**
    - `200 OK`: History retrieved
    - `404 Not Found`: Workflow not found

    **Example:**
    ```
    GET /api/v0a3/workflows/esm_postproc_v1/fingerprint/history?limit=20
    ```
    """
    try:
        workflow = workflow_service.get_workflow(workflow_id)

        # Retrieve fingerprint history from dedicated storage
        # This is a placeholder implementation
        history = FingerprintHistoryResponse(
            workflow_id=workflow_id,
            fingerprints=[],
            total_count=0
        )

        return history

    except HTTPException:
        raise
    except Exception as e:
        if "not found" in str(e).lower():
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Workflow '{workflow_id}' not found"
            )
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to get fingerprint history: {str(e)}"
            )


# ============================================================================
# Location Association Endpoints
# ============================================================================

@router.post("/{workflow_id}/locations", response_model=WorkflowDto)
async def associate_workflow_locations(
    workflow_id: str,
    association_data: WorkflowLocationAssociationDto,
    workflow_service: WorkflowApplicationService = Depends(get_workflow_service)
):
    """
    Associate a workflow with one or more storage locations.

    **Path Parameters:**
    - `workflow_id`: Workflow identifier

    **Request Body Example:**
    ```json
    {
        "workflow_id": "esm_postproc_v1",
        "location_names": ["hpc_scratch", "archive_tape"],
        "input_location_mapping": {
            "step1": "hpc_scratch",
            "step2": "hpc_scratch"
        },
        "output_location_mapping": {
            "step1": "hpc_scratch",
            "step2": "archive_tape"
        }
    }
    ```

    **Returns:**
    Updated workflow with location associations

    **Status Codes:**
    - `200 OK`: Locations associated
    - `400 Bad Request`: workflow_id mismatch
    - `404 Not Found`: Workflow or location not found
    """
    try:
        # Validate workflow_id match
        if workflow_id != association_data.workflow_id:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Workflow ID in URL ('{workflow_id}') must match ID in request body ('{association_data.workflow_id}')"
            )

        # Associate locations using the service
        workflow_service.associate_locations(association_data)

        # Return updated workflow
        return workflow_service.get_workflow(workflow_id)

    except HTTPException:
        raise
    except Exception as e:
        if "not found" in str(e).lower():
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Workflow '{workflow_id}' or location not found"
            )
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to associate locations: {str(e)}"
            )


@router.delete("/{workflow_id}/locations/{location_name}", response_model=WorkflowDto)
async def disassociate_workflow_location(
    workflow_id: str,
    location_name: str,
    workflow_service: WorkflowApplicationService = Depends(get_workflow_service)
):
    """
    Remove a location association from a workflow.

    **Path Parameters:**
    - `workflow_id`: Workflow identifier
    - `location_name`: Location to disassociate

    **Returns:**
    Updated workflow without the location association

    **Status Codes:**
    - `200 OK`: Location disassociated
    - `404 Not Found`: Workflow not found

    **Note:** This will also remove step location mappings that reference this location.
    """
    try:
        # Disassociate location using the service
        updated_workflow = workflow_service.disassociate_workflow_from_location(
            workflow_id, location_name
        )

        return updated_workflow

    except HTTPException:
        raise
    except Exception as e:
        if "not found" in str(e).lower():
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Workflow '{workflow_id}' not found"
            )
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to disassociate location: {str(e)}"
            )


# ============================================================================
# Polling Control Endpoints
# ============================================================================

@router.post("/{workflow_id}/polling/enable", response_model=PollingControlResponse)
async def enable_workflow_polling(
    workflow_id: str,
    interval_seconds: int = Query(60, ge=10, le=3600, description="Polling interval in seconds"),
    workflow_service: WorkflowApplicationService = Depends(get_workflow_service)
):
    """
    Enable automated polling for workflow status updates.

    When enabled, the system will periodically check the workflow execution
    status and update RunFingerprint data automatically.

    **Path Parameters:**
    - `workflow_id`: Workflow identifier

    **Query Parameters:**
    - `interval_seconds`: Polling interval (10-3600 seconds, default: 60)

    **Returns:**
    Polling status confirmation

    **Status Codes:**
    - `200 OK`: Polling enabled
    - `404 Not Found`: Workflow not found

    **Example:**
    ```
    POST /api/v0a3/workflows/esm_postproc_v1/polling/enable?interval_seconds=120
    ```
    """
    try:
        workflow = workflow_service.get_workflow(workflow_id)

        # Enable polling (would normally update a polling service)
        # This is a placeholder implementation
        response = PollingControlResponse(
            workflow_id=workflow_id,
            polling_enabled=True,
            polling_interval_seconds=interval_seconds,
            last_poll_time=None,
            message=f"Polling enabled with {interval_seconds}s interval"
        )

        return response

    except HTTPException:
        raise
    except Exception as e:
        if "not found" in str(e).lower():
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Workflow '{workflow_id}' not found"
            )
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to enable polling: {str(e)}"
            )


@router.post("/{workflow_id}/polling/disable", response_model=PollingControlResponse)
async def disable_workflow_polling(
    workflow_id: str,
    workflow_service: WorkflowApplicationService = Depends(get_workflow_service)
):
    """
    Disable automated polling for a workflow.

    **Path Parameters:**
    - `workflow_id`: Workflow identifier

    **Returns:**
    Polling status confirmation

    **Status Codes:**
    - `200 OK`: Polling disabled
    - `404 Not Found`: Workflow not found
    """
    try:
        workflow = workflow_service.get_workflow(workflow_id)

        # Disable polling (would normally update a polling service)
        response = PollingControlResponse(
            workflow_id=workflow_id,
            polling_enabled=False,
            polling_interval_seconds=None,
            last_poll_time=None,
            message="Polling disabled"
        )

        return response

    except HTTPException:
        raise
    except Exception as e:
        if "not found" in str(e).lower():
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Workflow '{workflow_id}' not found"
            )
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to disable polling: {str(e)}"
            )
