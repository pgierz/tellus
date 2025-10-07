"""
Integration tests for the workflow REST API endpoints.

These tests verify the complete HTTP request/response cycle for all workflow
API endpoints including CRUD operations, fingerprint management, location
associations, simulation associations, and polling control.
"""

import pytest
from unittest.mock import Mock, MagicMock
from datetime import datetime
from fastapi.testclient import TestClient
from fastapi import FastAPI

from tellus.interfaces.web.routers.workflows import router
from tellus.application.services.workflow_service import WorkflowApplicationService
from tellus.application.dtos import (
    WorkflowDto, CreateWorkflowDto, UpdateWorkflowDto,
    WorkflowListDto, PaginationInfo, FilterOptions,
    WorkflowLocationAssociationDto, WorkflowStepDto,
    ResourceRequirementDto
)
from tellus.application.exceptions import (
    EntityNotFoundError, EntityAlreadyExistsError, ValidationError
)


# ============================================================================
# Test Fixtures
# ============================================================================

@pytest.fixture
def mock_workflow_service():
    """Mock workflow application service."""
    service = Mock(spec=WorkflowApplicationService)
    return service


@pytest.fixture
def app(mock_workflow_service):
    """Create FastAPI test application with mocked dependencies."""
    test_app = FastAPI()

    # Override the dependency
    from tellus.interfaces.web.dependencies import get_workflow_service

    def override_get_workflow_service():
        return mock_workflow_service

    test_app.dependency_overrides[get_workflow_service] = override_get_workflow_service
    test_app.include_router(router, prefix="/api/v0a3/workflows")

    return test_app


@pytest.fixture
def client(app):
    """Create test client."""
    return TestClient(app)


@pytest.fixture
def sample_workflow_dto():
    """Sample workflow DTO for testing."""
    return WorkflowDto(
        workflow_id="test-workflow-1",
        uid="wf-12345-abcde",
        name="Test Workflow",
        description="A test workflow for climate data processing",
        engine="snakemake",
        workflow_file="/path/to/Snakefile",
        steps=[
            WorkflowStepDto(
                step_id="step1",
                name="Preprocessing",
                command="python preprocess.py",
                input_files=["input.nc"],
                output_files=["preprocessed.nc"],
                parameters={"resolution": "T63"},
                dependencies=[],
                resource_requirements=ResourceRequirementDto(
                    cores=4,
                    memory_gb=16.0
                )
            )
        ],
        global_parameters={"model": "ECHAM6", "resolution": "T63"},
        input_schema={},
        output_schema={},
        tags={"climate", "preprocessing"},
        version="1.0",
        author="test_user",
        created_at="2025-10-07T10:00:00Z",
        estimated_resources=ResourceRequirementDto(
            cores=4,
            memory_gb=16.0,
            disk_gb=100.0
        ),
        associated_locations=["hpc_scratch"],
        location_contexts={"hpc_scratch": {"path": "/scratch/workflows"}},
        input_location_mapping={"step1": "hpc_scratch"},
        output_location_mapping={"step1": "hpc_scratch"}
    )


@pytest.fixture
def sample_create_workflow_dto():
    """Sample create workflow DTO."""
    return CreateWorkflowDto(
        workflow_id="new-workflow",
        name="New Workflow",
        description="A new workflow",
        engine="snakemake",
        workflow_file="/path/to/Snakefile",
        steps=[],
        global_parameters={},
        tags=set(),
        version="1.0"
    )


@pytest.fixture
def sample_workflow_list_dto(sample_workflow_dto):
    """Sample workflow list DTO."""
    return WorkflowListDto(
        workflows=[sample_workflow_dto],
        pagination=PaginationInfo(
            page=1,
            page_size=50,
            total_count=1,
            has_next=False,
            has_previous=False
        ),
        filters_applied=FilterOptions()
    )


# ============================================================================
# Workflow CRUD Endpoint Tests
# ============================================================================

class TestListWorkflows:
    """Tests for GET /api/v0a3/workflows/"""

    def test_list_workflows_success(self, client, mock_workflow_service, sample_workflow_list_dto):
        """Test successful workflow listing with default pagination."""
        mock_workflow_service.list_workflows.return_value = sample_workflow_list_dto

        response = client.get("/api/v0a3/workflows/")

        assert response.status_code == 200
        data = response.json()
        assert "workflows" in data
        assert "pagination" in data
        assert len(data["workflows"]) == 1
        assert data["workflows"][0]["workflow_id"] == "test-workflow-1"

        # Verify service was called with correct defaults
        mock_workflow_service.list_workflows.assert_called_once()
        call_kwargs = mock_workflow_service.list_workflows.call_args.kwargs
        assert call_kwargs["page"] == 1
        assert call_kwargs["page_size"] == 50

    def test_list_workflows_with_pagination(self, client, mock_workflow_service, sample_workflow_list_dto):
        """Test workflow listing with custom pagination."""
        mock_workflow_service.list_workflows.return_value = sample_workflow_list_dto

        response = client.get("/api/v0a3/workflows/?page=2&page_size=25")

        assert response.status_code == 200
        mock_workflow_service.list_workflows.assert_called_once()
        call_kwargs = mock_workflow_service.list_workflows.call_args.kwargs
        assert call_kwargs["page"] == 2
        assert call_kwargs["page_size"] == 25

    def test_list_workflows_with_search(self, client, mock_workflow_service, sample_workflow_list_dto):
        """Test workflow listing with search filter."""
        mock_workflow_service.list_workflows.return_value = sample_workflow_list_dto

        response = client.get("/api/v0a3/workflows/?search=climate")

        assert response.status_code == 200
        mock_workflow_service.list_workflows.assert_called_once()
        call_kwargs = mock_workflow_service.list_workflows.call_args.kwargs
        assert call_kwargs["filters"] is not None
        assert call_kwargs["filters"].search_term == "climate"

    def test_list_workflows_with_simulation_filter(self, client, mock_workflow_service, sample_workflow_list_dto):
        """Test workflow listing filtered by simulation ID."""
        # Create a new workflow DTO with simulation_id
        workflow_data = sample_workflow_list_dto.workflows[0].model_dump()
        workflow_data["simulation_id"] = "sim-123"

        # Create fresh WorkflowListDto with updated workflow
        filtered_list = WorkflowListDto(
            workflows=[WorkflowDto(**workflow_data)],
            pagination=sample_workflow_list_dto.pagination,
            filters_applied=sample_workflow_list_dto.filters_applied
        )

        mock_workflow_service.list_workflows.return_value = filtered_list

        response = client.get("/api/v0a3/workflows/?simulation_id=sim-123")

        assert response.status_code == 200
        data = response.json()
        # The filtering happens in the router, so it may filter out workflows without matching simulation_id
        assert "workflows" in data

    def test_list_workflows_with_status_filter(self, client, mock_workflow_service, sample_workflow_list_dto):
        """Test workflow listing filtered by status."""
        # Create workflow with status field
        workflow_data = sample_workflow_list_dto.workflows[0].model_dump()
        workflow_data["status"] = "READY"

        filtered_list = WorkflowListDto(
            workflows=[WorkflowDto(**workflow_data)],
            pagination=sample_workflow_list_dto.pagination,
            filters_applied=sample_workflow_list_dto.filters_applied
        )

        mock_workflow_service.list_workflows.return_value = filtered_list

        response = client.get("/api/v0a3/workflows/?workflow_status=READY")

        assert response.status_code == 200
        assert "workflows" in response.json()

    def test_list_workflows_service_error(self, client, mock_workflow_service):
        """Test workflow listing when service raises an error."""
        mock_workflow_service.list_workflows.side_effect = RuntimeError("Database connection failed")

        response = client.get("/api/v0a3/workflows/")

        # Should return 500 error
        assert response.status_code == 500
        response_data = response.json()
        assert "detail" in response_data
        # The error message should mention the failure
        assert "fail" in response_data["detail"].lower() or "error" in response_data["detail"].lower()


class TestCreateWorkflow:
    """Tests for POST /api/v0a3/workflows/"""

    def test_create_workflow_success(self, client, mock_workflow_service, sample_workflow_dto):
        """Test successful workflow creation."""
        mock_workflow_service.create_workflow.return_value = sample_workflow_dto

        payload = {
            "workflow_id": "new-workflow",
            "name": "New Workflow",
            "description": "Test description",
            "engine": "snakemake",
            "workflow_file": "/path/to/Snakefile",
            "steps": [],
            "global_parameters": {},
            "tags": [],
            "version": "1.0"
        }

        response = client.post("/api/v0a3/workflows/", json=payload)

        assert response.status_code == 201
        data = response.json()
        assert data["workflow_id"] == "test-workflow-1"
        assert data["uid"] == "wf-12345-abcde"
        mock_workflow_service.create_workflow.assert_called_once()

    def test_create_workflow_duplicate_id(self, client, mock_workflow_service):
        """Test workflow creation with duplicate ID returns 400."""
        mock_workflow_service.create_workflow.side_effect = Exception("Workflow already exists")

        payload = {
            "workflow_id": "duplicate-id",
            "name": "Duplicate",
            "engine": "snakemake"
        }

        response = client.post("/api/v0a3/workflows/", json=payload)

        assert response.status_code == 400
        assert "already exists" in response.json()["detail"].lower()

    def test_create_workflow_validation_error(self, client, mock_workflow_service):
        """Test workflow creation with validation error returns 422."""
        mock_workflow_service.create_workflow.side_effect = ValueError("Invalid engine type")

        payload = {
            "workflow_id": "test",
            "name": "Test",
            "engine": "invalid_engine"
        }

        response = client.post("/api/v0a3/workflows/", json=payload)

        assert response.status_code == 422

    def test_create_workflow_missing_required_fields(self, client, mock_workflow_service):
        """Test workflow creation with missing required fields."""
        payload = {
            "name": "Incomplete Workflow"
            # Missing workflow_id and engine
        }

        response = client.post("/api/v0a3/workflows/", json=payload)

        assert response.status_code == 422  # Pydantic validation error


class TestGetWorkflow:
    """Tests for GET /api/v0a3/workflows/{workflow_id}"""

    def test_get_workflow_success(self, client, mock_workflow_service, sample_workflow_dto):
        """Test successful workflow retrieval."""
        mock_workflow_service.get_workflow.return_value = sample_workflow_dto

        response = client.get("/api/v0a3/workflows/test-workflow-1")

        assert response.status_code == 200
        data = response.json()
        assert data["workflow_id"] == "test-workflow-1"
        assert data["name"] == "Test Workflow"
        assert data["engine"] == "snakemake"
        mock_workflow_service.get_workflow.assert_called_once_with("test-workflow-1")

    def test_get_workflow_not_found(self, client, mock_workflow_service):
        """Test getting non-existent workflow returns 404."""
        mock_workflow_service.get_workflow.side_effect = Exception("Workflow not found")

        response = client.get("/api/v0a3/workflows/nonexistent")

        assert response.status_code == 404
        assert "not found" in response.json()["detail"].lower()

    def test_get_workflow_service_error(self, client, mock_workflow_service):
        """Test getting workflow when service error occurs."""
        mock_workflow_service.get_workflow.side_effect = Exception("Database error")

        response = client.get("/api/v0a3/workflows/test-id")

        assert response.status_code == 500


class TestUpdateWorkflow:
    """Tests for PUT /api/v0a3/workflows/{workflow_id}"""

    def test_update_workflow_success(self, client, mock_workflow_service, sample_workflow_dto):
        """Test successful workflow update."""
        updated_dto = sample_workflow_dto.model_copy(deep=True)
        updated_dto.name = "Updated Workflow Name"
        mock_workflow_service.update_workflow.return_value = updated_dto

        payload = {
            "name": "Updated Workflow Name",
            "description": "Updated description"
        }

        response = client.put("/api/v0a3/workflows/test-workflow-1", json=payload)

        assert response.status_code == 200
        data = response.json()
        assert data["name"] == "Updated Workflow Name"
        mock_workflow_service.update_workflow.assert_called_once()

    def test_update_workflow_not_found(self, client, mock_workflow_service):
        """Test updating non-existent workflow returns 404."""
        mock_workflow_service.update_workflow.side_effect = Exception("Workflow not found")

        payload = {"name": "Updated Name"}
        response = client.put("/api/v0a3/workflows/nonexistent", json=payload)

        assert response.status_code == 404

    def test_update_workflow_validation_error(self, client, mock_workflow_service):
        """Test workflow update with invalid data."""
        mock_workflow_service.update_workflow.side_effect = ValueError("Invalid update data")

        payload = {"name": "Updated Name"}
        response = client.put("/api/v0a3/workflows/test-id", json=payload)

        # Should return 500 for service-level errors
        # The router catches ValueError and wraps it in HTTPException with 500
        assert response.status_code == 500


class TestDeleteWorkflow:
    """Tests for DELETE /api/v0a3/workflows/{workflow_id}"""

    def test_delete_workflow_success(self, client, mock_workflow_service):
        """Test successful workflow deletion."""
        mock_workflow_service.delete_workflow.return_value = True

        response = client.delete("/api/v0a3/workflows/test-workflow-1")

        assert response.status_code == 204
        assert response.content == b''
        mock_workflow_service.delete_workflow.assert_called_once_with("test-workflow-1")

    def test_delete_workflow_not_found(self, client, mock_workflow_service):
        """Test deleting non-existent workflow returns 404."""
        mock_workflow_service.delete_workflow.side_effect = Exception("Workflow not found")

        response = client.delete("/api/v0a3/workflows/nonexistent")

        assert response.status_code == 404

    def test_delete_workflow_service_error(self, client, mock_workflow_service):
        """Test deletion when service error occurs."""
        mock_workflow_service.delete_workflow.side_effect = Exception("Cannot delete")

        response = client.delete("/api/v0a3/workflows/test-id")

        assert response.status_code == 500


# ============================================================================
# Simulation Association Endpoint Tests
# ============================================================================

class TestSimulationWorkflowEndpoints:
    """Tests for simulation-workflow association endpoints."""

    def test_list_simulation_workflows(self, client, mock_workflow_service, sample_workflow_list_dto):
        """Test GET /api/v0a3/workflows/simulations/{simulation_id}/workflows"""
        # Create workflow with simulation_id
        workflow_data = sample_workflow_list_dto.workflows[0].model_dump()
        workflow_data["simulation_id"] = "sim-123"

        filtered_list = WorkflowListDto(
            workflows=[WorkflowDto(**workflow_data)],
            pagination=sample_workflow_list_dto.pagination,
            filters_applied=sample_workflow_list_dto.filters_applied
        )

        mock_workflow_service.list_workflows.return_value = filtered_list

        response = client.get("/api/v0a3/workflows/simulations/sim-123/workflows")

        assert response.status_code == 200
        data = response.json()
        assert "workflows" in data
        mock_workflow_service.list_workflows.assert_called_once()

    def test_create_simulation_workflow_success(self, client, mock_workflow_service, sample_workflow_dto):
        """Test POST /api/v0a3/simulations/{simulation_id}/workflows"""
        mock_workflow_service.create_workflow.return_value = sample_workflow_dto

        payload = {
            "workflow_id": "sim-workflow-1",
            "name": "Simulation Workflow",
            "engine": "snakemake"
        }

        response = client.post("/api/v0a3/workflows/simulations/sim-123/workflows", json=payload)

        assert response.status_code == 201
        data = response.json()
        assert data["workflow_id"] == "test-workflow-1"

    def test_create_simulation_workflow_simulation_not_found(self, client, mock_workflow_service):
        """Test creating workflow for non-existent simulation."""
        mock_workflow_service.create_workflow.side_effect = Exception("Simulation not found")

        payload = {
            "workflow_id": "test",
            "name": "Test",
            "engine": "snakemake"
        }

        response = client.post("/api/v0a3/workflows/simulations/nonexistent/workflows", json=payload)

        assert response.status_code == 404


# ============================================================================
# Fingerprint Endpoint Tests
# ============================================================================

class TestFingerprintEndpoints:
    """Tests for workflow fingerprint management endpoints."""

    def test_update_fingerprint_success(self, client, mock_workflow_service):
        """Test PUT /api/v0a3/workflows/{workflow_id}/fingerprint"""
        from tellus.interfaces.web.routers.workflows import RunFingerprintDto

        fingerprint_response = RunFingerprintDto(
            workflow_id="test-workflow-1",
            run_timestamp="2025-10-07T14:30:00Z",
            snakemake_version="7.32.4",
            phases=[],
            observed_files=[],
            execution_summary={},
            metadata={}
        )

        # Mock the get_workflow call
        sample_workflow = Mock()
        sample_workflow.workflow_id = "test-workflow-1"
        mock_workflow_service.get_workflow.return_value = sample_workflow

        payload = {
            "run_timestamp": "2025-10-07T14:30:00Z",
            "snakemake_version": "7.32.4",
            "phases": [],
            "observed_files": []
        }

        response = client.put("/api/v0a3/workflows/test-workflow-1/fingerprint", json=payload)

        assert response.status_code == 200
        data = response.json()
        assert data["workflow_id"] == "test-workflow-1"
        assert data["run_timestamp"] == "2025-10-07T14:30:00Z"

    def test_update_fingerprint_workflow_not_found(self, client, mock_workflow_service):
        """Test updating fingerprint for non-existent workflow."""
        mock_workflow_service.get_workflow.side_effect = Exception("Workflow not found")

        payload = {
            "run_timestamp": "2025-10-07T14:30:00Z"
        }

        response = client.put("/api/v0a3/workflows/nonexistent/fingerprint", json=payload)

        assert response.status_code == 404

    def test_get_fingerprint_success(self, client, mock_workflow_service):
        """Test GET /api/v0a3/workflows/{workflow_id}/fingerprint"""
        from tellus.interfaces.web.routers.workflows import RunFingerprintDto

        sample_workflow = Mock()
        sample_workflow.workflow_id = "test-workflow-1"
        mock_workflow_service.get_workflow.return_value = sample_workflow

        response = client.get("/api/v0a3/workflows/test-workflow-1/fingerprint")

        assert response.status_code == 200
        data = response.json()
        assert data["workflow_id"] == "test-workflow-1"
        assert "run_timestamp" in data

    def test_get_fingerprint_workflow_not_found(self, client, mock_workflow_service):
        """Test getting fingerprint for non-existent workflow."""
        mock_workflow_service.get_workflow.side_effect = Exception("Workflow not found")

        response = client.get("/api/v0a3/workflows/nonexistent/fingerprint")

        assert response.status_code == 404

    def test_get_fingerprint_history_success(self, client, mock_workflow_service):
        """Test GET /api/v0a3/workflows/{workflow_id}/fingerprint/history"""
        from tellus.interfaces.web.routers.workflows import FingerprintHistoryResponse

        sample_workflow = Mock()
        sample_workflow.workflow_id = "test-workflow-1"
        mock_workflow_service.get_workflow.return_value = sample_workflow

        response = client.get("/api/v0a3/workflows/test-workflow-1/fingerprint/history")

        assert response.status_code == 200
        data = response.json()
        assert data["workflow_id"] == "test-workflow-1"
        assert "fingerprints" in data
        assert "total_count" in data

    def test_get_fingerprint_history_with_limit(self, client, mock_workflow_service):
        """Test fingerprint history with custom limit."""
        sample_workflow = Mock()
        mock_workflow_service.get_workflow.return_value = sample_workflow

        response = client.get("/api/v0a3/workflows/test-workflow-1/fingerprint/history?limit=20")

        assert response.status_code == 200


# ============================================================================
# Location Association Endpoint Tests
# ============================================================================

class TestLocationAssociationEndpoints:
    """Tests for workflow-location association endpoints."""

    def test_associate_locations_success(self, client, mock_workflow_service, sample_workflow_dto):
        """Test POST /api/v0a3/workflows/{workflow_id}/locations"""
        mock_workflow_service.associate_locations.return_value = None
        mock_workflow_service.get_workflow.return_value = sample_workflow_dto

        payload = {
            "workflow_id": "test-workflow-1",
            "location_names": ["hpc_scratch", "archive_tape"],
            "input_location_mapping": {"step1": "hpc_scratch"},
            "output_location_mapping": {"step1": "archive_tape"}
        }

        response = client.post("/api/v0a3/workflows/test-workflow-1/locations", json=payload)

        assert response.status_code == 200
        data = response.json()
        assert data["workflow_id"] == "test-workflow-1"
        mock_workflow_service.associate_locations.assert_called_once()

    def test_associate_locations_workflow_id_mismatch(self, client, mock_workflow_service):
        """Test location association with mismatched workflow IDs."""
        payload = {
            "workflow_id": "different-id",
            "location_names": ["hpc_scratch"]
        }

        response = client.post("/api/v0a3/workflows/test-workflow-1/locations", json=payload)

        assert response.status_code == 400
        assert "must match" in response.json()["detail"]

    def test_associate_locations_workflow_not_found(self, client, mock_workflow_service):
        """Test associating locations with non-existent workflow."""
        mock_workflow_service.associate_locations.side_effect = Exception("Workflow not found")

        payload = {
            "workflow_id": "nonexistent",
            "location_names": ["hpc_scratch"]
        }

        response = client.post("/api/v0a3/workflows/nonexistent/locations", json=payload)

        assert response.status_code == 404

    def test_associate_locations_location_not_found(self, client, mock_workflow_service):
        """Test associating non-existent location."""
        mock_workflow_service.associate_locations.side_effect = Exception("Location not found")

        payload = {
            "workflow_id": "test-workflow-1",
            "location_names": ["nonexistent_location"]
        }

        response = client.post("/api/v0a3/workflows/test-workflow-1/locations", json=payload)

        assert response.status_code == 404

    def test_disassociate_location_success(self, client, mock_workflow_service, sample_workflow_dto):
        """Test DELETE /api/v0a3/workflows/{workflow_id}/locations/{location_name}"""
        mock_workflow_service.disassociate_workflow_from_location.return_value = sample_workflow_dto

        response = client.delete("/api/v0a3/workflows/test-workflow-1/locations/hpc_scratch")

        assert response.status_code == 200
        data = response.json()
        assert data["workflow_id"] == "test-workflow-1"
        mock_workflow_service.disassociate_workflow_from_location.assert_called_once_with(
            "test-workflow-1", "hpc_scratch"
        )

    def test_disassociate_location_workflow_not_found(self, client, mock_workflow_service):
        """Test disassociating location from non-existent workflow."""
        mock_workflow_service.disassociate_workflow_from_location.side_effect = Exception(
            "Workflow not found"
        )

        response = client.delete("/api/v0a3/workflows/nonexistent/locations/hpc_scratch")

        assert response.status_code == 404


# ============================================================================
# Polling Control Endpoint Tests
# ============================================================================

class TestPollingControlEndpoints:
    """Tests for workflow polling control endpoints."""

    def test_enable_polling_success(self, client, mock_workflow_service):
        """Test POST /api/v0a3/workflows/{workflow_id}/polling/enable"""
        from tellus.interfaces.web.routers.workflows import PollingControlResponse

        sample_workflow = Mock()
        sample_workflow.workflow_id = "test-workflow-1"
        mock_workflow_service.get_workflow.return_value = sample_workflow

        response = client.post("/api/v0a3/workflows/test-workflow-1/polling/enable?interval_seconds=120")

        assert response.status_code == 200
        data = response.json()
        assert data["workflow_id"] == "test-workflow-1"
        assert data["polling_enabled"] is True
        assert data["polling_interval_seconds"] == 120
        assert "message" in data

    def test_enable_polling_default_interval(self, client, mock_workflow_service):
        """Test enabling polling with default interval."""
        sample_workflow = Mock()
        mock_workflow_service.get_workflow.return_value = sample_workflow

        response = client.post("/api/v0a3/workflows/test-workflow-1/polling/enable")

        assert response.status_code == 200
        data = response.json()
        assert data["polling_interval_seconds"] == 60  # Default

    def test_enable_polling_workflow_not_found(self, client, mock_workflow_service):
        """Test enabling polling for non-existent workflow."""
        mock_workflow_service.get_workflow.side_effect = Exception("Workflow not found")

        response = client.post("/api/v0a3/workflows/nonexistent/polling/enable")

        assert response.status_code == 404

    def test_enable_polling_invalid_interval(self, client, mock_workflow_service):
        """Test enabling polling with invalid interval."""
        # FastAPI should validate this before it reaches the service
        response = client.post("/api/v0a3/workflows/test-workflow-1/polling/enable?interval_seconds=5")

        # Should fail validation (min is 10)
        assert response.status_code == 422

    def test_disable_polling_success(self, client, mock_workflow_service):
        """Test POST /api/v0a3/workflows/{workflow_id}/polling/disable"""
        from tellus.interfaces.web.routers.workflows import PollingControlResponse

        sample_workflow = Mock()
        sample_workflow.workflow_id = "test-workflow-1"
        mock_workflow_service.get_workflow.return_value = sample_workflow

        response = client.post("/api/v0a3/workflows/test-workflow-1/polling/disable")

        assert response.status_code == 200
        data = response.json()
        assert data["workflow_id"] == "test-workflow-1"
        assert data["polling_enabled"] is False
        assert data["message"] == "Polling disabled"

    def test_disable_polling_workflow_not_found(self, client, mock_workflow_service):
        """Test disabling polling for non-existent workflow."""
        mock_workflow_service.get_workflow.side_effect = Exception("Workflow not found")

        response = client.post("/api/v0a3/workflows/nonexistent/polling/disable")

        assert response.status_code == 404


# ============================================================================
# HTTP Status Code and DTO Validation Tests
# ============================================================================

class TestHTTPStatusCodes:
    """Tests for correct HTTP status code usage."""

    def test_create_returns_201(self, client, mock_workflow_service, sample_workflow_dto):
        """Verify POST operations return 201 Created."""
        mock_workflow_service.create_workflow.return_value = sample_workflow_dto

        payload = {
            "workflow_id": "test",
            "name": "Test",
            "engine": "snakemake"
        }

        response = client.post("/api/v0a3/workflows/", json=payload)
        assert response.status_code == 201

    def test_get_returns_200(self, client, mock_workflow_service, sample_workflow_dto):
        """Verify GET operations return 200 OK."""
        mock_workflow_service.get_workflow.return_value = sample_workflow_dto

        response = client.get("/api/v0a3/workflows/test-workflow-1")
        assert response.status_code == 200

    def test_update_returns_200(self, client, mock_workflow_service, sample_workflow_dto):
        """Verify PUT operations return 200 OK."""
        mock_workflow_service.update_workflow.return_value = sample_workflow_dto

        payload = {"name": "Updated"}
        response = client.put("/api/v0a3/workflows/test-workflow-1", json=payload)
        assert response.status_code == 200

    def test_delete_returns_204(self, client, mock_workflow_service):
        """Verify DELETE operations return 204 No Content."""
        mock_workflow_service.delete_workflow.return_value = True

        response = client.delete("/api/v0a3/workflows/test-workflow-1")
        assert response.status_code == 204

    def test_not_found_returns_404(self, client, mock_workflow_service):
        """Verify 404 Not Found for missing resources."""
        mock_workflow_service.get_workflow.side_effect = Exception("not found")

        response = client.get("/api/v0a3/workflows/nonexistent")
        assert response.status_code == 404

    def test_validation_error_returns_422(self, client):
        """Verify 422 Unprocessable Entity for validation errors."""
        payload = {
            "name": "Missing required fields"
            # Missing workflow_id and engine
        }

        response = client.post("/api/v0a3/workflows/", json=payload)
        assert response.status_code == 422


class TestDTOValidation:
    """Tests for request/response DTO validation."""

    def test_create_workflow_dto_validation(self, client, mock_workflow_service):
        """Test CreateWorkflowDto validation."""
        # Missing required field: workflow_id
        payload = {
            "name": "Test",
            "engine": "snakemake"
        }

        response = client.post("/api/v0a3/workflows/", json=payload)
        assert response.status_code == 422
        assert "workflow_id" in str(response.json()["detail"]).lower()

    def test_update_workflow_dto_accepts_partial(self, client, mock_workflow_service, sample_workflow_dto):
        """Test UpdateWorkflowDto accepts partial updates."""
        mock_workflow_service.update_workflow.return_value = sample_workflow_dto

        # Only updating name
        payload = {"name": "New Name"}

        response = client.put("/api/v0a3/workflows/test-workflow-1", json=payload)
        assert response.status_code == 200

    def test_workflow_location_association_dto_validation(self, client):
        """Test WorkflowLocationAssociationDto validation."""
        # Missing required fields
        payload = {
            "workflow_id": "test"
            # Missing location_names
        }

        response = client.post("/api/v0a3/workflows/test/locations", json=payload)
        assert response.status_code == 422

    def test_response_serialization(self, client, mock_workflow_service, sample_workflow_dto):
        """Test response DTO serialization includes all fields."""
        mock_workflow_service.get_workflow.return_value = sample_workflow_dto

        response = client.get("/api/v0a3/workflows/test-workflow-1")

        assert response.status_code == 200
        data = response.json()

        # Verify all key fields are present
        assert "workflow_id" in data
        assert "uid" in data
        assert "name" in data
        assert "engine" in data
        assert "steps" in data
        assert "global_parameters" in data
        assert "tags" in data
        assert "associated_locations" in data


# ============================================================================
# Error Response Format Tests
# ============================================================================

class TestErrorResponses:
    """Tests for consistent error response formatting."""

    def test_404_error_format(self, client, mock_workflow_service):
        """Test 404 error response structure."""
        mock_workflow_service.get_workflow.side_effect = Exception("Workflow not found")

        response = client.get("/api/v0a3/workflows/nonexistent")

        assert response.status_code == 404
        data = response.json()
        assert "detail" in data
        assert isinstance(data["detail"], str)

    def test_400_error_format(self, client, mock_workflow_service):
        """Test 400 error response structure."""
        mock_workflow_service.create_workflow.side_effect = Exception("Workflow already exists")

        payload = {
            "workflow_id": "duplicate",
            "name": "Test",
            "engine": "snakemake"
        }

        response = client.post("/api/v0a3/workflows/", json=payload)

        assert response.status_code == 400
        data = response.json()
        assert "detail" in data

    def test_422_error_format(self, client):
        """Test 422 validation error response structure."""
        payload = {
            "name": "Missing workflow_id"
        }

        response = client.post("/api/v0a3/workflows/", json=payload)

        assert response.status_code == 422
        data = response.json()
        assert "detail" in data

    def test_500_error_format(self, client, mock_workflow_service):
        """Test 500 error response structure."""
        mock_workflow_service.list_workflows.side_effect = RuntimeError("Database connection failed")

        response = client.get("/api/v0a3/workflows/")

        assert response.status_code == 500
        data = response.json()
        assert "detail" in data
        # Check that error message is present
        assert isinstance(data["detail"], str)
        assert len(data["detail"]) > 0
