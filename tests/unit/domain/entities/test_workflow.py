"""
Comprehensive unit tests for workflow domain entities.

Tests WorkflowPhase, ObservedFile, and WorkflowEntity including:
- Validation rules
- Fingerprint updates and history tracking
- Phase extraction and status determination
- Datetime parsing
"""

import copy
import pytest
from datetime import datetime, timedelta
from typing import Dict, Any

from tellus.domain.entities.workflow import (
    WorkflowPhase,
    ObservedFile,
    WorkflowEntity,
    WorkflowStep,
    WorkflowType,
    WorkflowStatus,
    ResourceRequirement,
)


# ============================================================================
# WorkflowPhase Tests
# ============================================================================

class TestWorkflowPhase:
    """Tests for WorkflowPhase entity."""

    def test_valid_phase_creation(self):
        """Test creating a valid workflow phase."""
        phase = WorkflowPhase(
            phase_id="prepare",
            name="Preparation",
            status="PENDING"
        )

        assert phase.phase_id == "prepare"
        assert phase.name == "Preparation"
        assert phase.status == "PENDING"
        assert phase.start_time is None
        assert phase.end_time is None
        assert phase.log_files == []
        assert phase.metadata == {}

    def test_phase_with_all_fields(self):
        """Test creating a phase with all optional fields."""
        start = datetime(2025, 1, 6, 10, 0, 0)
        end = datetime(2025, 1, 6, 10, 15, 0)

        phase = WorkflowPhase(
            phase_id="compute",
            name="Computation",
            status="COMPLETED",
            start_time=start,
            end_time=end,
            log_files=["compute.log", "debug.log"],
            metadata={"cpu_cores": 128, "memory_gb": 256}
        )

        assert phase.phase_id == "compute"
        assert phase.name == "Computation"
        assert phase.status == "COMPLETED"
        assert phase.start_time == start
        assert phase.end_time == end
        assert phase.log_files == ["compute.log", "debug.log"]
        assert phase.metadata == {"cpu_cores": 128, "memory_gb": 256}

    def test_empty_phase_id_raises_error(self):
        """Test that empty phase_id raises ValueError."""
        with pytest.raises(ValueError, match="Phase ID cannot be empty"):
            WorkflowPhase(
                phase_id="",
                name="Test",
                status="PENDING"
            )

    def test_empty_name_raises_error(self):
        """Test that empty name raises ValueError."""
        with pytest.raises(ValueError, match="Phase name cannot be empty"):
            WorkflowPhase(
                phase_id="test",
                name="",
                status="PENDING"
            )

    def test_invalid_status_raises_error(self):
        """Test that invalid status raises ValueError."""
        with pytest.raises(ValueError, match="Invalid phase status: INVALID"):
            WorkflowPhase(
                phase_id="test",
                name="Test Phase",
                status="INVALID"
            )

    def test_valid_statuses(self):
        """Test all valid phase statuses."""
        valid_statuses = ["PENDING", "RUNNING", "COMPLETED", "FAILED"]

        for status in valid_statuses:
            phase = WorkflowPhase(
                phase_id="test",
                name="Test Phase",
                status=status
            )
            assert phase.status == status

    def test_get_duration_with_both_times(self):
        """Test get_duration() with both start and end times set."""
        start = datetime(2025, 1, 6, 10, 0, 0)
        end = datetime(2025, 1, 6, 10, 15, 30)

        phase = WorkflowPhase(
            phase_id="test",
            name="Test Phase",
            status="COMPLETED",
            start_time=start,
            end_time=end
        )

        duration = phase.get_duration()
        assert duration == timedelta(minutes=15, seconds=30)

    def test_get_duration_with_start_only(self):
        """Test get_duration() returns None when only start time is set."""
        phase = WorkflowPhase(
            phase_id="test",
            name="Test Phase",
            status="RUNNING",
            start_time=datetime(2025, 1, 6, 10, 0, 0)
        )

        duration = phase.get_duration()
        assert duration is None

    def test_get_duration_with_end_only(self):
        """Test get_duration() returns None when only end time is set."""
        phase = WorkflowPhase(
            phase_id="test",
            name="Test Phase",
            status="PENDING",
            end_time=datetime(2025, 1, 6, 10, 0, 0)
        )

        duration = phase.get_duration()
        assert duration is None

    def test_get_duration_with_no_times(self):
        """Test get_duration() returns None when no times are set."""
        phase = WorkflowPhase(
            phase_id="test",
            name="Test Phase",
            status="PENDING"
        )

        duration = phase.get_duration()
        assert duration is None


# ============================================================================
# ObservedFile Tests
# ============================================================================

class TestObservedFile:
    """Tests for ObservedFile entity."""

    def test_valid_file_creation(self):
        """Test creating a valid observed file."""
        file = ObservedFile(
            path="output/data.nc",
            classification="OUTPUT"
        )

        assert file.path == "output/data.nc"
        assert file.classification == "OUTPUT"
        assert file.component is None
        assert file.size_bytes == 0
        assert file.temporal_info is None
        assert file.location_name == ""
        assert file.metadata == {}

    def test_file_with_all_fields(self):
        """Test creating a file with all optional fields."""
        file = ObservedFile(
            path="restart/echam6.nc",
            classification="RESTART",
            component="echam6",
            size_bytes=1024000,
            temporal_info={"year": 2025, "month": 1, "day": 6},
            location_name="hpc-scratch",
            metadata={"compression": "gzip", "level": 9}
        )

        assert file.path == "restart/echam6.nc"
        assert file.classification == "RESTART"
        assert file.component == "echam6"
        assert file.size_bytes == 1024000
        assert file.temporal_info == {"year": 2025, "month": 1, "day": 6}
        assert file.location_name == "hpc-scratch"
        assert file.metadata == {"compression": "gzip", "level": 9}

    def test_empty_path_raises_error(self):
        """Test that empty path raises ValueError."""
        with pytest.raises(ValueError, match="File path cannot be empty"):
            ObservedFile(
                path="",
                classification="OUTPUT"
            )

    def test_empty_classification_raises_error(self):
        """Test that empty classification raises ValueError."""
        with pytest.raises(ValueError, match="File classification cannot be empty"):
            ObservedFile(
                path="test.nc",
                classification=""
            )

    def test_negative_size_raises_error(self):
        """Test that negative size raises ValueError."""
        with pytest.raises(ValueError, match="File size cannot be negative"):
            ObservedFile(
                path="test.nc",
                classification="OUTPUT",
                size_bytes=-100
            )

    def test_zero_size_allowed(self):
        """Test that zero size is allowed."""
        file = ObservedFile(
            path="empty.txt",
            classification="LOG",
            size_bytes=0
        )
        assert file.size_bytes == 0

    def test_valid_classifications(self):
        """Test creating files with various classifications."""
        classifications = ["OUTPUT", "RESTART", "CONFIG", "LOG"]

        for classification in classifications:
            file = ObservedFile(
                path=f"test_{classification.lower()}.nc",
                classification=classification
            )
            assert file.classification == classification


# ============================================================================
# WorkflowEntity Fingerprint Tests
# ============================================================================

class TestWorkflowEntityFingerprint:
    """Tests for WorkflowEntity fingerprint update functionality."""

    def test_update_from_valid_fingerprint(self):
        """Test updating workflow from a valid RunFingerprint."""
        workflow = WorkflowEntity(
            workflow_id="wf-001",
            name="Test Workflow",
            workflow_type=WorkflowType.MODEL_EXECUTION
        )

        fingerprint = {
            "run_id": "test_run_123",
            "workflow_system": "esm-tools",
            "phases": [
                {
                    "phase_id": "prepare",
                    "name": "Preparation",
                    "status": "COMPLETED",
                    "start_time": "2025-01-06T10:00:00",
                    "end_time": "2025-01-06T10:15:00",
                    "log_files": ["prep.log"]
                }
            ],
            "observed_files": [
                {"path": "output.nc", "classification": "OUTPUT"},
                {"path": "restart.nc", "classification": "RESTART"}
            ]
        }

        workflow.update_from_fingerprint(fingerprint)

        # Check that phases were extracted
        assert len(workflow.current_phases) == 1
        assert workflow.current_phases[0].phase_id == "prepare"
        assert workflow.current_phases[0].name == "Preparation"
        assert workflow.current_phases[0].status == "COMPLETED"

        # Check file count
        assert workflow.observed_file_count == 2

        # Check status
        assert workflow.current_status == "completed"

        # Check fingerprint storage
        assert workflow.current_fingerprint == fingerprint
        assert len(workflow.fingerprint_history) == 1

    def test_fingerprint_phase_extraction(self):
        """Test phase extraction from fingerprint data."""
        workflow = WorkflowEntity(
            workflow_id="wf-002",
            name="Multi-Phase Workflow",
            workflow_type=WorkflowType.MODEL_EXECUTION
        )

        fingerprint = {
            "run_id": "run_456",
            "workflow_system": "snakemake",
            "phases": [
                {
                    "phase_id": "prepare",
                    "name": "Preparation",
                    "status": "COMPLETED",
                    "start_time": "2025-01-06T10:00:00",
                    "end_time": "2025-01-06T10:15:00",
                    "log_files": ["prep.log"],
                    "metadata": {"cores": 4}
                },
                {
                    "phase_id": "compute",
                    "name": "Computation",
                    "status": "RUNNING",
                    "start_time": "2025-01-06T10:20:00",
                    "log_files": ["compute.log"]
                },
                {
                    "phase_id": "cleanup",
                    "name": "Cleanup",
                    "status": "PENDING"
                }
            ],
            "observed_files": []
        }

        workflow.update_from_fingerprint(fingerprint)

        assert len(workflow.current_phases) == 3

        # Check first phase
        assert workflow.current_phases[0].phase_id == "prepare"
        assert workflow.current_phases[0].status == "COMPLETED"
        assert workflow.current_phases[0].start_time == datetime(2025, 1, 6, 10, 0, 0)
        assert workflow.current_phases[0].end_time == datetime(2025, 1, 6, 10, 15, 0)
        assert workflow.current_phases[0].log_files == ["prep.log"]
        assert workflow.current_phases[0].metadata == {"cores": 4}

        # Check second phase
        assert workflow.current_phases[1].phase_id == "compute"
        assert workflow.current_phases[1].status == "RUNNING"
        assert workflow.current_phases[1].start_time == datetime(2025, 1, 6, 10, 20, 0)
        assert workflow.current_phases[1].end_time is None

        # Check third phase
        assert workflow.current_phases[2].phase_id == "cleanup"
        assert workflow.current_phases[2].status == "PENDING"
        assert workflow.current_phases[2].start_time is None
        assert workflow.current_phases[2].end_time is None

    def test_file_count_extraction(self):
        """Test observed file count extraction from fingerprint."""
        workflow = WorkflowEntity(
            workflow_id="wf-003",
            name="File Count Test",
            workflow_type=WorkflowType.POST_PROCESSING
        )

        fingerprint = {
            "run_id": "run_789",
            "phases": [],
            "observed_files": [
                {"path": "file1.nc", "classification": "OUTPUT"},
                {"path": "file2.nc", "classification": "OUTPUT"},
                {"path": "file3.nc", "classification": "RESTART"},
                {"path": "log.txt", "classification": "LOG"}
            ]
        }

        workflow.update_from_fingerprint(fingerprint)

        assert workflow.observed_file_count == 4

    def test_status_determination_failed(self):
        """Test status determination when any phase has FAILED status."""
        workflow = WorkflowEntity(
            workflow_id="wf-004",
            name="Failed Workflow",
            workflow_type=WorkflowType.MODEL_EXECUTION
        )

        fingerprint = {
            "run_id": "run_fail",
            "phases": [
                {"phase_id": "prepare", "name": "Preparation", "status": "COMPLETED"},
                {"phase_id": "compute", "name": "Computation", "status": "FAILED"},
                {"phase_id": "cleanup", "name": "Cleanup", "status": "PENDING"}
            ],
            "observed_files": []
        }

        workflow.update_from_fingerprint(fingerprint)

        assert workflow.current_status == "failed"

    def test_status_determination_running(self):
        """Test status determination when any phase is RUNNING."""
        workflow = WorkflowEntity(
            workflow_id="wf-005",
            name="Running Workflow",
            workflow_type=WorkflowType.MODEL_EXECUTION
        )

        fingerprint = {
            "run_id": "run_active",
            "phases": [
                {"phase_id": "prepare", "name": "Preparation", "status": "COMPLETED"},
                {"phase_id": "compute", "name": "Computation", "status": "RUNNING"}
            ],
            "observed_files": []
        }

        workflow.update_from_fingerprint(fingerprint)

        assert workflow.current_status == "running"

    def test_status_determination_completed(self):
        """Test status determination when all phases are COMPLETED."""
        workflow = WorkflowEntity(
            workflow_id="wf-006",
            name="Completed Workflow",
            workflow_type=WorkflowType.MODEL_EXECUTION
        )

        fingerprint = {
            "run_id": "run_complete",
            "phases": [
                {"phase_id": "prepare", "name": "Preparation", "status": "COMPLETED"},
                {"phase_id": "compute", "name": "Computation", "status": "COMPLETED"},
                {"phase_id": "cleanup", "name": "Cleanup", "status": "COMPLETED"}
            ],
            "observed_files": []
        }

        workflow.update_from_fingerprint(fingerprint)

        assert workflow.current_status == "completed"

    def test_status_determination_pending(self):
        """Test status determination when phases are PENDING."""
        workflow = WorkflowEntity(
            workflow_id="wf-007",
            name="Pending Workflow",
            workflow_type=WorkflowType.MODEL_EXECUTION
        )

        fingerprint = {
            "run_id": "run_pending",
            "phases": [
                {"phase_id": "prepare", "name": "Preparation", "status": "PENDING"},
                {"phase_id": "compute", "name": "Computation", "status": "PENDING"}
            ],
            "observed_files": []
        }

        workflow.update_from_fingerprint(fingerprint)

        assert workflow.current_status == "pending"

    def test_status_determination_no_phases(self):
        """Test status determination with no phases (remains unknown)."""
        workflow = WorkflowEntity(
            workflow_id="wf-008",
            name="No Phases Workflow",
            workflow_type=WorkflowType.MODEL_EXECUTION
        )

        fingerprint = {
            "run_id": "run_empty",
            "phases": [],
            "observed_files": []
        }

        workflow.update_from_fingerprint(fingerprint)

        # When all phases are completed but there are no phases, status should remain unknown
        assert workflow.current_status == "unknown"

    def test_fingerprint_history_tracking(self):
        """Test that fingerprint history is tracked correctly."""
        workflow = WorkflowEntity(
            workflow_id="wf-009",
            name="History Test",
            workflow_type=WorkflowType.MODEL_EXECUTION
        )

        fingerprint1 = {
            "run_id": "run_1",
            "phases": [{"phase_id": "prepare", "name": "Preparation", "status": "RUNNING"}],
            "observed_files": []
        }

        fingerprint2 = {
            "run_id": "run_2",
            "phases": [{"phase_id": "prepare", "name": "Preparation", "status": "COMPLETED"}],
            "observed_files": []
        }

        # Update with first fingerprint
        workflow.update_from_fingerprint(fingerprint1)
        assert len(workflow.fingerprint_history) == 1
        assert workflow.fingerprint_history[0]["fingerprint"] == fingerprint1

        # Update with second fingerprint
        workflow.update_from_fingerprint(fingerprint2)
        assert len(workflow.fingerprint_history) == 2
        assert workflow.fingerprint_history[1]["fingerprint"] == fingerprint2

        # Check current fingerprint is the latest
        assert workflow.current_fingerprint == fingerprint2

    def test_fingerprint_invalid_type_raises_error(self):
        """Test that non-dict fingerprint raises ValueError."""
        workflow = WorkflowEntity(
            workflow_id="wf-010",
            name="Invalid Fingerprint Test",
            workflow_type=WorkflowType.MODEL_EXECUTION
        )

        with pytest.raises(ValueError, match="Fingerprint must be a dictionary"):
            workflow.update_from_fingerprint("not a dict")

    def test_fingerprint_missing_fields(self):
        """Test handling of fingerprint with missing optional fields."""
        workflow = WorkflowEntity(
            workflow_id="wf-011",
            name="Missing Fields Test",
            workflow_type=WorkflowType.MODEL_EXECUTION
        )

        # Minimal fingerprint with missing phases and files
        fingerprint = {
            "run_id": "run_minimal"
        }

        workflow.update_from_fingerprint(fingerprint)

        assert workflow.current_phases == []
        assert workflow.observed_file_count == 0
        assert workflow.current_fingerprint == fingerprint

    def test_parse_datetime_iso_format(self):
        """Test datetime parsing with ISO format strings."""
        workflow = WorkflowEntity(
            workflow_id="wf-012",
            name="Datetime Parse Test",
            workflow_type=WorkflowType.MODEL_EXECUTION
        )

        # Test various ISO format strings
        dt1 = workflow._parse_datetime("2025-01-06T10:00:00")
        assert dt1 == datetime(2025, 1, 6, 10, 0, 0)

        dt2 = workflow._parse_datetime("2025-01-06T10:30:45")
        assert dt2 == datetime(2025, 1, 6, 10, 30, 45)

    def test_parse_datetime_with_z_suffix(self):
        """Test datetime parsing with Z suffix (UTC)."""
        workflow = WorkflowEntity(
            workflow_id="wf-013",
            name="Datetime Z Test",
            workflow_type=WorkflowType.MODEL_EXECUTION
        )

        # Test with Z suffix
        dt = workflow._parse_datetime("2025-01-06T10:00:00Z")
        assert dt is not None
        assert dt.year == 2025
        assert dt.month == 1
        assert dt.day == 6
        assert dt.hour == 10

    def test_parse_datetime_none_returns_none(self):
        """Test that None datetime string returns None."""
        workflow = WorkflowEntity(
            workflow_id="wf-014",
            name="Datetime None Test",
            workflow_type=WorkflowType.MODEL_EXECUTION
        )

        dt = workflow._parse_datetime(None)
        assert dt is None

    def test_parse_datetime_empty_string_returns_none(self):
        """Test that empty string returns None."""
        workflow = WorkflowEntity(
            workflow_id="wf-015",
            name="Datetime Empty Test",
            workflow_type=WorkflowType.MODEL_EXECUTION
        )

        dt = workflow._parse_datetime("")
        assert dt is None

    def test_parse_datetime_invalid_format_returns_none(self):
        """Test that invalid datetime format returns None."""
        workflow = WorkflowEntity(
            workflow_id="wf-016",
            name="Datetime Invalid Test",
            workflow_type=WorkflowType.MODEL_EXECUTION
        )

        dt = workflow._parse_datetime("not a datetime")
        assert dt is None

    def test_latest_observation_time_updated(self):
        """Test that latest_observation_time is updated on fingerprint update."""
        workflow = WorkflowEntity(
            workflow_id="wf-017",
            name="Observation Time Test",
            workflow_type=WorkflowType.MODEL_EXECUTION
        )

        assert workflow.latest_observation_time is None

        before_update = datetime.now()
        workflow.update_from_fingerprint({"run_id": "test", "phases": [], "observed_files": []})
        after_update = datetime.now()

        assert workflow.latest_observation_time is not None
        assert before_update <= workflow.latest_observation_time <= after_update


# ============================================================================
# WorkflowEntity Validation Tests
# ============================================================================

class TestWorkflowEntityValidation:
    """Tests for WorkflowEntity validation including new fields."""

    def test_valid_workflow_creation(self):
        """Test creating a valid workflow entity."""
        workflow = WorkflowEntity(
            workflow_id="wf-valid",
            name="Valid Workflow",
            workflow_type=WorkflowType.MODEL_EXECUTION
        )

        assert workflow.workflow_id == "wf-valid"
        assert workflow.name == "Valid Workflow"
        assert workflow.workflow_type == WorkflowType.MODEL_EXECUTION

    def test_empty_workflow_id_raises_error(self):
        """Test that empty workflow_id raises ValueError."""
        with pytest.raises(ValueError, match="Workflow ID cannot be empty"):
            WorkflowEntity(
                workflow_id="",
                name="Test",
                workflow_type=WorkflowType.MODEL_EXECUTION
            )

    def test_empty_name_raises_error(self):
        """Test that empty name raises ValueError."""
        with pytest.raises(ValueError, match="Workflow name cannot be empty"):
            WorkflowEntity(
                workflow_id="wf-test",
                name="",
                workflow_type=WorkflowType.MODEL_EXECUTION
            )

    def test_invalid_workflow_type_raises_error(self):
        """Test that invalid workflow_type raises ValueError."""
        with pytest.raises(ValueError, match="Invalid workflow type"):
            WorkflowEntity(
                workflow_id="wf-test",
                name="Test",
                workflow_type="not_an_enum"
            )

    def test_workflow_system_validation(self):
        """Test workflow_system field validation."""
        workflow = WorkflowEntity(
            workflow_id="wf-sys",
            name="System Test",
            workflow_type=WorkflowType.MODEL_EXECUTION,
            workflow_system="esm-tools"
        )

        assert workflow.workflow_system == "esm-tools"

    def test_workflow_system_must_be_string(self):
        """Test that workflow_system must be a string."""
        with pytest.raises(ValueError, match="Workflow system must be a string"):
            WorkflowEntity(
                workflow_id="wf-sys",
                name="System Test",
                workflow_type=WorkflowType.MODEL_EXECUTION,
                workflow_system=123
            )

    def test_current_phases_validation(self):
        """Test current_phases field validation."""
        phase = WorkflowPhase(
            phase_id="test",
            name="Test Phase",
            status="PENDING"
        )

        workflow = WorkflowEntity(
            workflow_id="wf-phases",
            name="Phases Test",
            workflow_type=WorkflowType.MODEL_EXECUTION,
            current_phases=[phase]
        )

        assert len(workflow.current_phases) == 1
        assert workflow.current_phases[0] == phase

    def test_current_phases_must_be_list(self):
        """Test that current_phases must be a list."""
        with pytest.raises(ValueError, match="Current phases must be a list"):
            WorkflowEntity(
                workflow_id="wf-phases",
                name="Phases Test",
                workflow_type=WorkflowType.MODEL_EXECUTION,
                current_phases="not a list"
            )

    def test_observed_file_count_validation(self):
        """Test observed_file_count field validation."""
        workflow = WorkflowEntity(
            workflow_id="wf-count",
            name="File Count Test",
            workflow_type=WorkflowType.MODEL_EXECUTION,
            observed_file_count=42
        )

        assert workflow.observed_file_count == 42

    def test_negative_file_count_raises_error(self):
        """Test that negative observed_file_count raises ValueError."""
        with pytest.raises(ValueError, match="Observed file count cannot be negative"):
            WorkflowEntity(
                workflow_id="wf-count",
                name="File Count Test",
                workflow_type=WorkflowType.MODEL_EXECUTION,
                observed_file_count=-5
            )

    def test_current_status_validation(self):
        """Test current_status field validation."""
        valid_statuses = ["unknown", "pending", "running", "completed", "failed"]

        for status in valid_statuses:
            workflow = WorkflowEntity(
                workflow_id=f"wf-status-{status}",
                name=f"Status {status}",
                workflow_type=WorkflowType.MODEL_EXECUTION,
                current_status=status
            )
            assert workflow.current_status == status

    def test_invalid_current_status_raises_error(self):
        """Test that invalid current_status raises ValueError."""
        with pytest.raises(ValueError, match="Invalid current status: invalid"):
            WorkflowEntity(
                workflow_id="wf-status",
                name="Status Test",
                workflow_type=WorkflowType.MODEL_EXECUTION,
                current_status="invalid"
            )

    def test_associated_locations_must_be_set(self):
        """Test that associated_locations must be a set."""
        with pytest.raises(ValueError, match="Associated locations must be a set"):
            WorkflowEntity(
                workflow_id="wf-loc",
                name="Location Test",
                workflow_type=WorkflowType.MODEL_EXECUTION,
                associated_locations=["loc1", "loc2"]  # List instead of set
            )

    def test_location_contexts_must_be_dict(self):
        """Test that location_contexts must be a dictionary."""
        with pytest.raises(ValueError, match="Location contexts must be a dictionary"):
            WorkflowEntity(
                workflow_id="wf-ctx",
                name="Context Test",
                workflow_type=WorkflowType.MODEL_EXECUTION,
                location_contexts="not a dict"
            )

    def test_input_location_mapping_must_be_dict(self):
        """Test that input_location_mapping must be a dictionary."""
        with pytest.raises(ValueError, match="Input location mapping must be a dictionary"):
            WorkflowEntity(
                workflow_id="wf-map",
                name="Mapping Test",
                workflow_type=WorkflowType.MODEL_EXECUTION,
                input_location_mapping=["not", "a", "dict"]
            )

    def test_output_location_mapping_must_be_dict(self):
        """Test that output_location_mapping must be a dictionary."""
        # Test with valid dict (should succeed)
        workflow = WorkflowEntity(
            workflow_id="wf-map",
            name="Mapping Test",
            workflow_type=WorkflowType.MODEL_EXECUTION,
            output_location_mapping={"step": "location"}
        )
        assert workflow.output_location_mapping == {"step": "location"}

        # Now test with invalid type (should raise error)
        with pytest.raises(ValueError, match="Output location mapping must be a dictionary"):
            WorkflowEntity(
                workflow_id="wf-map2",
                name="Mapping Test 2",
                workflow_type=WorkflowType.MODEL_EXECUTION,
                output_location_mapping="not a dict"
            )


# ============================================================================
# Integration Tests
# ============================================================================

class TestWorkflowEntityIntegration:
    """Integration tests for complete workflow lifecycle."""

    def test_complete_workflow_lifecycle_with_fingerprints(self):
        """Test complete workflow lifecycle with multiple fingerprint updates."""
        # Create workflow
        workflow = WorkflowEntity(
            workflow_id="wf-lifecycle",
            name="Lifecycle Test Workflow",
            workflow_type=WorkflowType.MODEL_EXECUTION,
            workflow_system="esm-tools"
        )

        # Initial state
        assert workflow.current_status == "unknown"
        assert workflow.observed_file_count == 0
        assert len(workflow.current_phases) == 0
        assert len(workflow.fingerprint_history) == 0

        # Update 1: Workflow starts, preparation phase begins
        fingerprint1 = {
            "run_id": "lifecycle_run",
            "workflow_system": "esm-tools",
            "phases": [
                {
                    "phase_id": "prepare",
                    "name": "Preparation",
                    "status": "RUNNING",
                    "start_time": "2025-01-06T10:00:00",
                    "log_files": ["prep.log"]
                }
            ],
            "observed_files": []
        }
        workflow.update_from_fingerprint(fingerprint1)

        assert workflow.current_status == "running"
        assert len(workflow.current_phases) == 1
        assert workflow.current_phases[0].status == "RUNNING"
        assert len(workflow.fingerprint_history) == 1

        # Update 2: Preparation completes, computation starts
        fingerprint2 = {
            "run_id": "lifecycle_run",
            "workflow_system": "esm-tools",
            "phases": [
                {
                    "phase_id": "prepare",
                    "name": "Preparation",
                    "status": "COMPLETED",
                    "start_time": "2025-01-06T10:00:00",
                    "end_time": "2025-01-06T10:15:00",
                    "log_files": ["prep.log"]
                },
                {
                    "phase_id": "compute",
                    "name": "Computation",
                    "status": "RUNNING",
                    "start_time": "2025-01-06T10:20:00",
                    "log_files": ["compute.log"]
                }
            ],
            "observed_files": [
                {"path": "config.yaml", "classification": "CONFIG"}
            ]
        }
        workflow.update_from_fingerprint(fingerprint2)

        assert workflow.current_status == "running"
        assert len(workflow.current_phases) == 2
        assert workflow.current_phases[0].status == "COMPLETED"
        assert workflow.current_phases[1].status == "RUNNING"
        assert workflow.observed_file_count == 1
        assert len(workflow.fingerprint_history) == 2

        # Update 3: Everything completes
        fingerprint3 = {
            "run_id": "lifecycle_run",
            "workflow_system": "esm-tools",
            "phases": [
                {
                    "phase_id": "prepare",
                    "name": "Preparation",
                    "status": "COMPLETED",
                    "start_time": "2025-01-06T10:00:00",
                    "end_time": "2025-01-06T10:15:00",
                    "log_files": ["prep.log"]
                },
                {
                    "phase_id": "compute",
                    "name": "Computation",
                    "status": "COMPLETED",
                    "start_time": "2025-01-06T10:20:00",
                    "end_time": "2025-01-06T11:30:00",
                    "log_files": ["compute.log"]
                },
                {
                    "phase_id": "cleanup",
                    "name": "Cleanup",
                    "status": "COMPLETED",
                    "start_time": "2025-01-06T11:35:00",
                    "end_time": "2025-01-06T11:40:00",
                    "log_files": ["cleanup.log"]
                }
            ],
            "observed_files": [
                {"path": "config.yaml", "classification": "CONFIG"},
                {"path": "output/data.nc", "classification": "OUTPUT"},
                {"path": "restart/state.nc", "classification": "RESTART"}
            ]
        }
        workflow.update_from_fingerprint(fingerprint3)

        assert workflow.current_status == "completed"
        assert len(workflow.current_phases) == 3
        assert all(p.status == "COMPLETED" for p in workflow.current_phases)
        assert workflow.observed_file_count == 3
        assert len(workflow.fingerprint_history) == 3

        # Verify history contains all updates
        assert workflow.fingerprint_history[0]["fingerprint"] == fingerprint1
        assert workflow.fingerprint_history[1]["fingerprint"] == fingerprint2
        assert workflow.fingerprint_history[2]["fingerprint"] == fingerprint3

    def test_workflow_with_failure_scenario(self):
        """Test workflow that fails during execution."""
        workflow = WorkflowEntity(
            workflow_id="wf-failure",
            name="Failure Test",
            workflow_type=WorkflowType.MODEL_EXECUTION
        )

        # Start normally
        fingerprint1 = {
            "run_id": "fail_run",
            "phases": [
                {"phase_id": "prepare", "name": "Preparation", "status": "COMPLETED"}
            ],
            "observed_files": []
        }
        workflow.update_from_fingerprint(fingerprint1)
        assert workflow.current_status == "completed"

        # Computation fails
        fingerprint2 = {
            "run_id": "fail_run",
            "phases": [
                {"phase_id": "prepare", "name": "Preparation", "status": "COMPLETED"},
                {"phase_id": "compute", "name": "Computation", "status": "FAILED"}
            ],
            "observed_files": []
        }
        workflow.update_from_fingerprint(fingerprint2)
        assert workflow.current_status == "failed"
        assert workflow.current_phases[1].status == "FAILED"

    def test_multiple_fingerprint_updates_build_history(self):
        """Test that multiple updates correctly build fingerprint history."""
        workflow = WorkflowEntity(
            workflow_id="wf-history",
            name="History Building Test",
            workflow_type=WorkflowType.DATA_PREPROCESSING
        )

        num_updates = 5
        for i in range(num_updates):
            fingerprint = {
                "run_id": f"run_{i}",
                "phases": [
                    {
                        "phase_id": f"phase_{i}",
                        "name": f"Phase {i}",
                        "status": "COMPLETED"
                    }
                ],
                "observed_files": [
                    {"path": f"file_{j}.nc", "classification": "OUTPUT"}
                    for j in range(i + 1)
                ]
            }
            workflow.update_from_fingerprint(fingerprint)

        # Verify history
        assert len(workflow.fingerprint_history) == num_updates
        assert workflow.observed_file_count == num_updates  # Last update has 5 files

        # Verify each history entry has a timestamp
        for entry in workflow.fingerprint_history:
            assert "timestamp" in entry
            assert "fingerprint" in entry
            datetime.fromisoformat(entry["timestamp"])  # Should not raise

    def test_workflow_step_validation_with_fingerprint_fields(self):
        """Test that workflow validation works with fingerprint fields."""
        step = WorkflowStep(
            step_id="step1",
            name="Test Step",
            command="echo test"
        )

        workflow = WorkflowEntity(
            workflow_id="wf-val",
            name="Validation Test",
            workflow_type=WorkflowType.CUSTOM,
            steps=[step],
            workflow_system="custom",
            current_phases=[],
            observed_file_count=0,
            current_status="pending"
        )

        errors = workflow.validate()
        assert len(errors) == 0  # No validation errors
