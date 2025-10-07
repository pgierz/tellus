"""
Comprehensive unit tests for WorkflowApplicationService.

Tests the service layer orchestration logic for workflow management including:
- Simulation associations
- Location associations
- Fingerprint management
- Polling control
- Business rule validation

Following clean architecture testing principles by mocking all infrastructure dependencies.
"""

import pytest
from datetime import datetime
from unittest.mock import Mock, MagicMock
from typing import List, Optional, Dict, Any

from tellus.application.services.workflow_service import (
    WorkflowApplicationService,
    IWorkflowRepository,
    IWorkflowTemplateRepository
)
from tellus.application.exceptions import (
    ValidationError,
    EntityNotFoundError,
    EntityAlreadyExistsError,
    BusinessRuleViolationError
)
from tellus.domain.entities.workflow import (
    WorkflowEntity,
    WorkflowEngine,
    WorkflowType,
    WorkflowStep,
    WorkflowPhase,
    ResourceRequirement
)
from tellus.domain.entities.location import LocationEntity, LocationKind
from tellus.domain.repositories.location_repository import ILocationRepository


# Test Fixtures

@pytest.fixture
def mock_workflow_repo():
    """Mock workflow repository."""
    repo = Mock(spec=IWorkflowRepository)
    repo.get_by_id = Mock(return_value=None)
    repo.exists = Mock(return_value=False)
    repo.save = Mock()
    repo.delete = Mock(return_value=True)
    repo.list_all = Mock(return_value=[])
    return repo


@pytest.fixture
def mock_template_repo():
    """Mock workflow template repository."""
    repo = Mock(spec=IWorkflowTemplateRepository)
    repo.get_by_id = Mock(return_value=None)
    repo.exists = Mock(return_value=False)
    repo.save = Mock()
    repo.delete = Mock(return_value=True)
    repo.list_all = Mock(return_value=[])
    return repo


@pytest.fixture
def mock_location_repo():
    """Mock location repository."""
    repo = Mock(spec=ILocationRepository)
    repo.get_by_name = Mock(return_value=None)
    repo.exists = Mock(return_value=False)
    repo.list_all = Mock(return_value=[])
    return repo


@pytest.fixture
def service(mock_workflow_repo, mock_template_repo, mock_location_repo, monkeypatch):
    """Create service instance with mocked dependencies."""
    svc = WorkflowApplicationService(
        workflow_repository=mock_workflow_repo,
        template_repository=mock_template_repo,
        location_repository=mock_location_repo
    )

    # Mock the _entity_to_dto method to avoid entity/DTO structure mismatches
    # This allows us to test business logic without worrying about DTO conversion
    def mock_entity_to_dto(workflow):
        mock_dto = Mock()
        mock_dto.workflow_id = workflow.workflow_id
        mock_dto.name = workflow.name
        mock_dto.simulation_id = getattr(workflow, 'simulation_id', None)
        return mock_dto

    monkeypatch.setattr(svc, "_entity_to_dto", mock_entity_to_dto)

    return svc


@pytest.fixture
def sample_workflow():
    """Create a sample workflow entity for testing."""
    step = WorkflowStep(
        step_id="step1",
        name="Process Data",
        command="python process.py",
        dependencies=[],
        resource_requirements=ResourceRequirement(
            cpu_cores=4,
            memory_gb=16,
            disk_space_gb=100
        )
    )

    return WorkflowEntity(
        workflow_id="test-workflow-001",
        name="Test Workflow",
        workflow_type=WorkflowType.DATA_PREPROCESSING,
        steps=[step],
        description="A test workflow for unit testing",
        version="1.0.0",
        author="test_author",
        tags={"climate", "preprocessing"},
        parameters={"global_param": "value"},
        workflow_system="snakemake"
    )


@pytest.fixture
def sample_location():
    """Create a sample location entity for testing."""
    return LocationEntity(
        name="test-storage",
        kinds=[LocationKind.FILESERVER],
        config={
            "protocol": "sftp",
            "path": "/data/storage",
            "storage_options": {"host": "storage.example.com"},
            "host": "storage.example.com",
            "username": "user"
        }
    )


# Simulation Association Tests

class TestSimulationAssociation:
    """Test workflow-simulation association functionality."""

    def test_associate_with_simulation_success(self, service, mock_workflow_repo, sample_workflow):
        """Test successfully associating a workflow with a simulation."""
        # Setup
        mock_workflow_repo.get_by_id.return_value = sample_workflow

        # Execute
        result = service.associate_with_simulation("test-workflow-001", "sim-123")

        # Verify
        mock_workflow_repo.get_by_id.assert_called_once_with("test-workflow-001")
        assert sample_workflow.simulation_id == "sim-123"
        mock_workflow_repo.save.assert_called_once_with(sample_workflow)
        assert result.workflow_id == "test-workflow-001"

    def test_associate_with_simulation_workflow_not_found(self, service, mock_workflow_repo):
        """Test associating with simulation when workflow doesn't exist."""
        # Setup
        mock_workflow_repo.get_by_id.return_value = None

        # Execute & Verify
        with pytest.raises(EntityNotFoundError) as exc_info:
            service.associate_with_simulation("nonexistent", "sim-123")

        assert "Workflow" in str(exc_info.value)
        assert "nonexistent" in str(exc_info.value)

    def test_associate_with_simulation_invalid_simulation_id(self, service, mock_workflow_repo, sample_workflow):
        """Test associating with invalid simulation ID."""
        # Setup
        mock_workflow_repo.get_by_id.return_value = sample_workflow

        # Execute & Verify - empty string
        with pytest.raises(ValidationError) as exc_info:
            service.associate_with_simulation("test-workflow-001", "")
        assert "Simulation ID must be a non-empty string" in str(exc_info.value)

        # Execute & Verify - None
        with pytest.raises(ValidationError) as exc_info:
            service.associate_with_simulation("test-workflow-001", None)
        assert "Simulation ID must be a non-empty string" in str(exc_info.value)

    def test_get_simulation_workflows_success(self, service, mock_workflow_repo):
        """Test retrieving workflows by simulation ID."""
        # Setup
        workflow1 = WorkflowEntity(
            workflow_id="wf1",
            name="Workflow 1",
            workflow_type=WorkflowType.DATA_PREPROCESSING,
            description="Test",
            simulation_id="sim-123"
        )
        workflow2 = WorkflowEntity(
            workflow_id="wf2",
            name="Workflow 2",
            workflow_type=WorkflowType.POST_PROCESSING,
            description="Test",
            simulation_id="sim-123"
        )
        workflow3 = WorkflowEntity(
            workflow_id="wf3",
            name="Workflow 3",
            workflow_type=WorkflowType.DATA_ANALYSIS,
            description="Test",
            simulation_id="sim-456"
        )

        mock_workflow_repo.list_all.return_value = [workflow1, workflow2, workflow3]

        # Execute
        result = service.get_simulation_workflows("sim-123")

        # Verify
        assert len(result) == 2
        assert result[0].workflow_id == "wf1"
        assert result[1].workflow_id == "wf2"

    def test_get_simulation_workflows_no_matches(self, service, mock_workflow_repo):
        """Test retrieving workflows when simulation has no workflows."""
        # Setup
        mock_workflow_repo.list_all.return_value = []

        # Execute
        result = service.get_simulation_workflows("sim-nonexistent")

        # Verify
        assert result == []


# Location Management Tests

class TestLocationManagement:
    """Test workflow location association functionality."""

    def test_associate_location_success(self, service, mock_workflow_repo, mock_location_repo, sample_workflow):
        """Test successfully associating a location with a workflow."""
        # Setup
        mock_workflow_repo.get_by_id.return_value = sample_workflow
        mock_location_repo.exists.return_value = True

        # Execute
        result = service.associate_location("test-workflow-001", "test-storage")

        # Verify
        mock_workflow_repo.get_by_id.assert_called_once_with("test-workflow-001")
        mock_location_repo.exists.assert_called_once_with("test-storage")
        assert "test-storage" in sample_workflow.associated_locations
        mock_workflow_repo.save.assert_called_once_with(sample_workflow)

    def test_associate_location_with_context(self, service, mock_workflow_repo, mock_location_repo, sample_workflow):
        """Test associating a location with custom context."""
        # Setup
        mock_workflow_repo.get_by_id.return_value = sample_workflow
        mock_location_repo.exists.return_value = True
        context = {"mount_point": "/mnt/data", "priority": "high"}

        # Execute
        result = service.associate_location("test-workflow-001", "test-storage", context)

        # Verify
        assert "test-storage" in sample_workflow.associated_locations
        assert sample_workflow.location_contexts.get("test-storage") == context
        mock_workflow_repo.save.assert_called_once_with(sample_workflow)

    def test_associate_location_workflow_not_found(self, service, mock_workflow_repo):
        """Test associating location when workflow doesn't exist."""
        # Setup
        mock_workflow_repo.get_by_id.return_value = None

        # Execute & Verify
        with pytest.raises(EntityNotFoundError) as exc_info:
            service.associate_location("nonexistent", "test-storage")

        assert "Workflow" in str(exc_info.value)

    def test_associate_location_location_not_found(self, service, mock_workflow_repo, mock_location_repo, sample_workflow):
        """Test associating non-existent location."""
        # Setup
        mock_workflow_repo.get_by_id.return_value = sample_workflow
        mock_location_repo.exists.return_value = False

        # Execute & Verify
        with pytest.raises(EntityNotFoundError) as exc_info:
            service.associate_location("test-workflow-001", "nonexistent-location")

        assert "Location" in str(exc_info.value)

    def test_dissociate_location_success(self, service, mock_workflow_repo, sample_workflow):
        """Test successfully dissociating a location from a workflow."""
        # Setup
        sample_workflow.associate_location("test-storage")
        mock_workflow_repo.get_by_id.return_value = sample_workflow

        # Execute
        result = service.dissociate_location("test-workflow-001", "test-storage")

        # Verify
        assert "test-storage" not in sample_workflow.associated_locations
        mock_workflow_repo.save.assert_called_once_with(sample_workflow)

    def test_dissociate_location_not_associated(self, service, mock_workflow_repo, sample_workflow):
        """Test dissociating a location that isn't associated."""
        # Setup
        mock_workflow_repo.get_by_id.return_value = sample_workflow

        # Execute & Verify
        with pytest.raises(ValidationError) as exc_info:
            service.dissociate_location("test-workflow-001", "unassociated-location")

        assert "not associated" in str(exc_info.value)

    def test_dissociate_location_workflow_not_found(self, service, mock_workflow_repo):
        """Test dissociating location when workflow doesn't exist."""
        # Setup
        mock_workflow_repo.get_by_id.return_value = None

        # Execute & Verify
        with pytest.raises(EntityNotFoundError) as exc_info:
            service.dissociate_location("nonexistent", "test-storage")

        assert "Workflow" in str(exc_info.value)


# Fingerprint Management Tests

class TestFingerprintManagement:
    """Test workflow fingerprint update functionality."""

    def test_update_fingerprint_success(self, service, mock_workflow_repo, sample_workflow):
        """Test successfully updating workflow fingerprint."""
        # Setup
        mock_workflow_repo.get_by_id.return_value = sample_workflow
        fingerprint = {
            "phases": [
                {
                    "phase_id": "prep",
                    "name": "Preparation",
                    "status": "COMPLETED",
                    "start_time": "2025-01-01T10:00:00",
                    "end_time": "2025-01-01T10:15:00",
                    "log_files": ["/logs/prep.log"],
                    "metadata": {"step_count": 3}
                }
            ],
            "observed_files": [
                {"path": "/data/output1.nc", "size": 1024},
                {"path": "/data/output2.nc", "size": 2048}
            ],
            "status": "running",
            "timestamp": "2025-01-01T10:30:00"
        }

        # Execute
        result = service.update_fingerprint("test-workflow-001", fingerprint)

        # Verify
        assert sample_workflow.current_fingerprint == fingerprint
        assert len(sample_workflow.current_phases) == 1
        assert sample_workflow.current_phases[0].phase_id == "prep"
        assert sample_workflow.current_phases[0].status == "COMPLETED"
        assert sample_workflow.observed_file_count == 2
        assert sample_workflow.current_status == "running"
        assert sample_workflow.latest_observation_time is not None
        mock_workflow_repo.save.assert_called_once_with(sample_workflow)

    def test_update_fingerprint_history_accumulation(self, service, mock_workflow_repo, sample_workflow):
        """Test that fingerprint updates are accumulated in history."""
        # Setup
        mock_workflow_repo.get_by_id.return_value = sample_workflow

        fingerprint1 = {
            "phases": [],
            "observed_files": [],
            "status": "pending",
            "timestamp": "2025-01-01T10:00:00"
        }

        fingerprint2 = {
            "phases": [],
            "observed_files": [],
            "status": "running",
            "timestamp": "2025-01-01T10:15:00"
        }

        # Execute
        service.update_fingerprint("test-workflow-001", fingerprint1)
        service.update_fingerprint("test-workflow-001", fingerprint2)

        # Verify
        assert len(sample_workflow.fingerprint_history) == 1
        assert sample_workflow.fingerprint_history[0] == fingerprint1
        assert sample_workflow.current_fingerprint == fingerprint2

    def test_update_fingerprint_workflow_not_found(self, service, mock_workflow_repo):
        """Test updating fingerprint when workflow doesn't exist."""
        # Setup
        mock_workflow_repo.get_by_id.return_value = None
        fingerprint = {"status": "running"}

        # Execute & Verify
        with pytest.raises(EntityNotFoundError) as exc_info:
            service.update_fingerprint("nonexistent", fingerprint)

        assert "Workflow" in str(exc_info.value)

    def test_update_fingerprint_invalid_data(self, service, mock_workflow_repo, sample_workflow):
        """Test updating fingerprint with invalid data."""
        # Setup
        mock_workflow_repo.get_by_id.return_value = sample_workflow

        # Execute & Verify - non-dict fingerprint
        with pytest.raises(ValidationError) as exc_info:
            service.update_fingerprint("test-workflow-001", "invalid")

        assert "must be a dictionary" in str(exc_info.value)

    def test_update_fingerprint_invalid_phase_data(self, service, mock_workflow_repo, sample_workflow):
        """Test updating fingerprint with invalid phase data."""
        # Setup
        mock_workflow_repo.get_by_id.return_value = sample_workflow
        fingerprint = {
            "phases": [
                {
                    "phase_id": "",  # Invalid: empty phase_id
                    "name": "Test",
                    "status": "PENDING"
                }
            ],
            "status": "running",
            "timestamp": "2025-01-01T10:00:00"
        }

        # Execute - should log warning but not fail
        result = service.update_fingerprint("test-workflow-001", fingerprint)

        # Verify - fingerprint stored but invalid phase skipped
        assert sample_workflow.current_fingerprint == fingerprint
        assert len(sample_workflow.current_phases) == 0  # Invalid phase skipped

    def test_update_fingerprint_phase_extraction(self, service, mock_workflow_repo, sample_workflow):
        """Test correct extraction of phase data from fingerprint."""
        # Setup
        mock_workflow_repo.get_by_id.return_value = sample_workflow
        fingerprint = {
            "phases": [
                {
                    "phase_id": "phase1",
                    "name": "Phase 1",
                    "status": "COMPLETED",
                    "start_time": "2025-01-01T10:00:00",
                    "end_time": "2025-01-01T11:00:00",
                    "log_files": ["/logs/phase1.log"],
                    "metadata": {"key": "value"}
                },
                {
                    "phase_id": "phase2",
                    "name": "Phase 2",
                    "status": "RUNNING",
                    "start_time": "2025-01-01T11:00:00",
                    "log_files": [],
                    "metadata": {}
                }
            ],
            "observed_files": [],
            "status": "running",
            "timestamp": "2025-01-01T11:30:00"
        }

        # Execute
        result = service.update_fingerprint("test-workflow-001", fingerprint)

        # Verify
        assert len(sample_workflow.current_phases) == 2
        assert sample_workflow.current_phases[0].phase_id == "phase1"
        assert sample_workflow.current_phases[0].status == "COMPLETED"
        assert sample_workflow.current_phases[1].phase_id == "phase2"
        assert sample_workflow.current_phases[1].status == "RUNNING"

    def test_get_latest_fingerprint_success(self, service, mock_workflow_repo, sample_workflow):
        """Test retrieving the latest fingerprint."""
        # Setup
        fingerprint = {"status": "running", "timestamp": "2025-01-01T10:00:00"}
        sample_workflow.current_fingerprint = fingerprint
        mock_workflow_repo.get_by_id.return_value = sample_workflow

        # Execute
        result = service.get_latest_fingerprint("test-workflow-001")

        # Verify
        assert result == fingerprint
        assert result is not sample_workflow.current_fingerprint  # Should be a copy

    def test_get_latest_fingerprint_none(self, service, mock_workflow_repo, sample_workflow):
        """Test retrieving fingerprint when none exists."""
        # Setup
        mock_workflow_repo.get_by_id.return_value = sample_workflow

        # Execute
        result = service.get_latest_fingerprint("test-workflow-001")

        # Verify
        assert result is None

    def test_get_latest_fingerprint_workflow_not_found(self, service, mock_workflow_repo):
        """Test retrieving fingerprint when workflow doesn't exist."""
        # Setup
        mock_workflow_repo.get_by_id.return_value = None

        # Execute & Verify
        with pytest.raises(EntityNotFoundError) as exc_info:
            service.get_latest_fingerprint("nonexistent")

        assert "Workflow" in str(exc_info.value)

    def test_get_fingerprint_history_success(self, service, mock_workflow_repo, sample_workflow):
        """Test retrieving fingerprint history."""
        # Setup
        history = [
            {"status": "pending", "timestamp": "2025-01-01T10:00:00"},
            {"status": "running", "timestamp": "2025-01-01T10:15:00"}
        ]
        sample_workflow.fingerprint_history = history
        mock_workflow_repo.get_by_id.return_value = sample_workflow

        # Execute
        result = service.get_fingerprint_history("test-workflow-001")

        # Verify
        assert len(result) == 2
        assert result[0]["status"] == "pending"
        assert result[1]["status"] == "running"
        # Should be copies, not originals
        assert result[0] is not history[0]

    def test_get_fingerprint_history_empty(self, service, mock_workflow_repo, sample_workflow):
        """Test retrieving fingerprint history when empty."""
        # Setup
        mock_workflow_repo.get_by_id.return_value = sample_workflow

        # Execute
        result = service.get_fingerprint_history("test-workflow-001")

        # Verify
        assert result == []

    def test_get_fingerprint_history_workflow_not_found(self, service, mock_workflow_repo):
        """Test retrieving fingerprint history when workflow doesn't exist."""
        # Setup
        mock_workflow_repo.get_by_id.return_value = None

        # Execute & Verify
        with pytest.raises(EntityNotFoundError) as exc_info:
            service.get_fingerprint_history("nonexistent")

        assert "Workflow" in str(exc_info.value)


# Polling Control Tests

class TestPollingControl:
    """Test workflow polling control functionality."""

    def test_enable_polling_success(self, service, mock_workflow_repo, sample_workflow):
        """Test successfully enabling polling."""
        # Setup
        mock_workflow_repo.get_by_id.return_value = sample_workflow

        # Execute
        result = service.enable_polling("test-workflow-001", interval_minutes=30)

        # Verify
        assert sample_workflow.polling_enabled is True
        assert sample_workflow.polling_interval_minutes == 30
        mock_workflow_repo.save.assert_called_once_with(sample_workflow)

    def test_enable_polling_default_interval(self, service, mock_workflow_repo, sample_workflow):
        """Test enabling polling with default interval."""
        # Setup
        mock_workflow_repo.get_by_id.return_value = sample_workflow

        # Execute
        result = service.enable_polling("test-workflow-001")

        # Verify
        assert sample_workflow.polling_enabled is True
        assert sample_workflow.polling_interval_minutes == 15  # default

    def test_enable_polling_interval_validation_too_small(self, service, mock_workflow_repo, sample_workflow):
        """Test enabling polling with interval less than 1 minute."""
        # Setup
        mock_workflow_repo.get_by_id.return_value = sample_workflow

        # Execute & Verify
        with pytest.raises(ValidationError) as exc_info:
            service.enable_polling("test-workflow-001", interval_minutes=0)

        assert "at least 1 minute" in str(exc_info.value)

    def test_enable_polling_interval_validation_too_large(self, service, mock_workflow_repo, sample_workflow):
        """Test enabling polling with interval greater than 24 hours."""
        # Setup
        mock_workflow_repo.get_by_id.return_value = sample_workflow

        # Execute & Verify
        with pytest.raises(ValidationError) as exc_info:
            service.enable_polling("test-workflow-001", interval_minutes=1441)

        assert "cannot exceed 1440 minutes" in str(exc_info.value)

    def test_enable_polling_workflow_not_found(self, service, mock_workflow_repo):
        """Test enabling polling when workflow doesn't exist."""
        # Setup
        mock_workflow_repo.get_by_id.return_value = None

        # Execute & Verify
        with pytest.raises(EntityNotFoundError) as exc_info:
            service.enable_polling("nonexistent")

        assert "Workflow" in str(exc_info.value)

    def test_disable_polling_success(self, service, mock_workflow_repo, sample_workflow):
        """Test successfully disabling polling."""
        # Setup
        sample_workflow.polling_enabled = True
        sample_workflow.polling_interval_minutes = 30
        mock_workflow_repo.get_by_id.return_value = sample_workflow

        # Execute
        result = service.disable_polling("test-workflow-001")

        # Verify
        assert sample_workflow.polling_enabled is False
        mock_workflow_repo.save.assert_called_once_with(sample_workflow)

    def test_disable_polling_workflow_not_found(self, service, mock_workflow_repo):
        """Test disabling polling when workflow doesn't exist."""
        # Setup
        mock_workflow_repo.get_by_id.return_value = None

        # Execute & Verify
        with pytest.raises(EntityNotFoundError) as exc_info:
            service.disable_polling("nonexistent")

        assert "Workflow" in str(exc_info.value)


# Integration Scenario Tests

class TestIntegrationScenarios:
    """Test complete workflow scenarios combining multiple operations."""

    def test_complete_workflow_lifecycle(self, service, mock_workflow_repo, mock_location_repo, sample_workflow):
        """Test complete workflow: create → associate simulation → associate location → update fingerprint → enable polling."""
        # Setup
        mock_workflow_repo.get_by_id.return_value = sample_workflow
        mock_location_repo.exists.return_value = True

        # 1. Associate with simulation
        result = service.associate_with_simulation("test-workflow-001", "sim-123")
        assert sample_workflow.simulation_id == "sim-123"

        # 2. Associate location
        result = service.associate_location("test-workflow-001", "test-storage")
        assert "test-storage" in sample_workflow.associated_locations

        # 3. Update fingerprint
        fingerprint = {
            "phases": [{"phase_id": "p1", "name": "Phase 1", "status": "RUNNING"}],
            "observed_files": [{"path": "/data/file1.nc"}],
            "status": "running",
            "timestamp": "2025-01-01T10:00:00"
        }
        result = service.update_fingerprint("test-workflow-001", fingerprint)
        assert sample_workflow.current_fingerprint is not None

        # 4. Enable polling
        result = service.enable_polling("test-workflow-001", interval_minutes=20)
        assert sample_workflow.polling_enabled is True

        # Verify save was called for each operation
        assert mock_workflow_repo.save.call_count == 4

    def test_multiple_fingerprint_updates(self, service, mock_workflow_repo, sample_workflow):
        """Test multiple fingerprint updates and history tracking."""
        # Setup
        mock_workflow_repo.get_by_id.return_value = sample_workflow

        # Update 1
        fp1 = {"status": "pending", "timestamp": "2025-01-01T10:00:00"}
        service.update_fingerprint("test-workflow-001", fp1)

        # Update 2
        fp2 = {"status": "running", "timestamp": "2025-01-01T10:15:00"}
        service.update_fingerprint("test-workflow-001", fp2)

        # Update 3
        fp3 = {"status": "completed", "timestamp": "2025-01-01T10:30:00"}
        service.update_fingerprint("test-workflow-001", fp3)

        # Verify
        assert sample_workflow.current_fingerprint == fp3
        assert len(sample_workflow.fingerprint_history) == 2
        assert sample_workflow.fingerprint_history[0] == fp1
        assert sample_workflow.fingerprint_history[1] == fp2

        # Verify history retrieval
        history = service.get_fingerprint_history("test-workflow-001")
        assert len(history) == 2
        assert history[0]["status"] == "pending"
        assert history[1]["status"] == "running"

    def test_workflow_with_multiple_locations(self, service, mock_workflow_repo, mock_location_repo, sample_workflow):
        """Test workflow with multiple location associations."""
        # Setup
        mock_workflow_repo.get_by_id.return_value = sample_workflow
        mock_location_repo.exists.return_value = True

        # Associate multiple locations
        locations = ["storage1", "storage2", "storage3"]
        for location in locations:
            service.associate_location("test-workflow-001", location)

        # Verify
        for location in locations:
            assert location in sample_workflow.associated_locations

        # Dissociate one location
        service.dissociate_location("test-workflow-001", "storage2")

        # Verify
        assert "storage1" in sample_workflow.associated_locations
        assert "storage2" not in sample_workflow.associated_locations
        assert "storage3" in sample_workflow.associated_locations

    def test_workflow_location_context_management(self, service, mock_workflow_repo, mock_location_repo, sample_workflow):
        """Test location association with different contexts."""
        # Setup
        mock_workflow_repo.get_by_id.return_value = sample_workflow
        mock_location_repo.exists.return_value = True

        # Associate locations with different contexts
        service.associate_location(
            "test-workflow-001",
            "input-storage",
            {"role": "input", "priority": "high"}
        )

        service.associate_location(
            "test-workflow-001",
            "output-storage",
            {"role": "output", "priority": "medium"}
        )

        service.associate_location(
            "test-workflow-001",
            "scratch-storage",
            {"role": "temporary", "cleanup": True}
        )

        # Verify
        assert len(sample_workflow.associated_locations) == 3
        assert sample_workflow.location_contexts["input-storage"]["role"] == "input"
        assert sample_workflow.location_contexts["output-storage"]["role"] == "output"
        assert sample_workflow.location_contexts["scratch-storage"]["cleanup"] is True


# Edge Case Tests

class TestEdgeCases:
    """Test edge cases and error paths."""

    def test_associate_location_empty_name(self, service, mock_workflow_repo, mock_location_repo, sample_workflow):
        """Test associating location with empty name."""
        # Setup
        mock_workflow_repo.get_by_id.return_value = sample_workflow
        mock_location_repo.exists.return_value = False

        # Execute & Verify - should raise EntityNotFoundError for empty location
        with pytest.raises(EntityNotFoundError):
            service.associate_location("test-workflow-001", "")

    def test_fingerprint_with_missing_timestamp(self, service, mock_workflow_repo, sample_workflow):
        """Test fingerprint update without timestamp (should use current time)."""
        # Setup
        mock_workflow_repo.get_by_id.return_value = sample_workflow
        fingerprint = {
            "phases": [],
            "observed_files": [],
            "status": "running"
            # No timestamp
        }

        # Execute
        before = datetime.now()
        result = service.update_fingerprint("test-workflow-001", fingerprint)
        after = datetime.now()

        # Verify - should have set current time
        assert sample_workflow.latest_observation_time is not None
        assert before <= sample_workflow.latest_observation_time <= after

    def test_polling_interval_boundary_values(self, service, mock_workflow_repo, sample_workflow):
        """Test polling interval at boundary values."""
        # Setup
        mock_workflow_repo.get_by_id.return_value = sample_workflow

        # Test minimum valid value
        result = service.enable_polling("test-workflow-001", interval_minutes=1)
        assert sample_workflow.polling_interval_minutes == 1

        # Test maximum valid value
        result = service.enable_polling("test-workflow-001", interval_minutes=1440)
        assert sample_workflow.polling_interval_minutes == 1440

    def test_dissociate_location_removes_step_mappings(self, service, mock_workflow_repo, sample_workflow):
        """Test that dissociating location also removes related step mappings."""
        # Setup
        sample_workflow.associate_location("test-storage")
        sample_workflow.set_step_input_location("step1", "test-storage")
        sample_workflow.set_step_output_location("step1", "test-storage")
        mock_workflow_repo.get_by_id.return_value = sample_workflow

        # Verify setup
        assert "test-storage" in sample_workflow.input_location_mapping.values()
        assert "test-storage" in sample_workflow.output_location_mapping.values()

        # Execute
        result = service.dissociate_location("test-workflow-001", "test-storage")

        # Verify location and mappings removed
        assert "test-storage" not in sample_workflow.associated_locations
        assert "test-storage" not in sample_workflow.input_location_mapping.values()
        assert "test-storage" not in sample_workflow.output_location_mapping.values()

    def test_repository_exception_handling(self, service, mock_workflow_repo, sample_workflow):
        """Test handling of repository exceptions."""
        # Setup
        mock_workflow_repo.get_by_id.return_value = sample_workflow
        mock_workflow_repo.save.side_effect = Exception("Database connection failed")

        # Execute & Verify
        with pytest.raises(Exception) as exc_info:
            service.associate_with_simulation("test-workflow-001", "sim-123")

        assert "Database connection failed" in str(exc_info.value)

    def test_get_simulation_workflows_filters_correctly(self, service, mock_workflow_repo):
        """Test that get_simulation_workflows filters by exact simulation_id."""
        # Setup
        wf1 = WorkflowEntity(
            workflow_id="wf1",
            name="Workflow 1",
            workflow_type=WorkflowType.DATA_PREPROCESSING,
            description="Test",
            simulation_id="sim-123"
        )
        wf2 = WorkflowEntity(
            workflow_id="wf2",
            name="Workflow 2",
            workflow_type=WorkflowType.POST_PROCESSING,
            description="Test",
            simulation_id="sim-1234"  # Similar but not exact match
        )

        mock_workflow_repo.list_all.return_value = [wf1, wf2]

        # Execute
        result = service.get_simulation_workflows("sim-123")

        # Verify - should only match exact ID
        assert len(result) == 1
        assert result[0].workflow_id == "wf1"
