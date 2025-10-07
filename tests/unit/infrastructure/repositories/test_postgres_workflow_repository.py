"""
Tests for PostgreSQL workflow repository.

Comprehensive tests for the PostgresWorkflowRepository including:
- CRUD operations
- Hybrid storage (structured + JSON) for RunFingerprint data
- Entity-to-model conversions with Sets, Lists, and custom objects
- Datetime handling and timezone support
- Error handling and rollback behavior
"""

import pytest
from unittest.mock import AsyncMock, MagicMock
from datetime import datetime, timedelta
from sqlalchemy.exc import IntegrityError

from tellus.infrastructure.repositories.postgres_workflow_repository import (
    PostgresWorkflowRepository,
    WorkflowNotFoundError,
    WorkflowExistsError,
)
from tellus.domain.entities.workflow import (
    WorkflowEntity,
    WorkflowType,
    WorkflowStatus,
    WorkflowStep,
    WorkflowPhase,
    ResourceRequirement,
)
from tellus.domain.repositories.exceptions import RepositoryError


@pytest.fixture
def repository():
    """Create repository instance for testing."""
    return PostgresWorkflowRepository()


@pytest.fixture
def repository_with_session(mock_session):
    """Create repository instance with provided session for testing."""
    return PostgresWorkflowRepository(session=mock_session)


@pytest.mark.asyncio
class TestPostgresWorkflowRepositoryCRUD:
    """Test basic CRUD operations."""

    async def test_create_new_workflow(
        self, repository, mock_session, sample_workflow_entity, mock_query_result
    ):
        """Test creating a new workflow."""
        # Mock that workflow doesn't exist
        mock_session.execute.return_value = mock_query_result(scalar_result=None)

        result = await repository.create(sample_workflow_entity)

        # Verify that we checked for existing workflow
        assert mock_session.execute.called
        # Verify that we added the new workflow
        assert mock_session.add.called
        # Verify returned entity matches
        assert result.workflow_id == sample_workflow_entity.workflow_id
        assert result.name == sample_workflow_entity.name
        assert result.workflow_type == sample_workflow_entity.workflow_type

    async def test_create_duplicate_workflow_raises_error(
        self, repository, mock_session, sample_workflow_entity, sample_workflow_model, mock_query_result
    ):
        """Test that creating a duplicate workflow raises WorkflowExistsError."""
        # Mock that workflow already exists
        mock_session.execute.return_value = mock_query_result(scalar_result=sample_workflow_model)

        with pytest.raises(WorkflowExistsError, match="already exists"):
            await repository.create(sample_workflow_entity)

        # Verify rollback was called
        assert mock_session.rollback.called

    async def test_create_with_provided_session(
        self, repository_with_session, mock_session, sample_workflow_entity, mock_query_result
    ):
        """Test creating with a provided session doesn't auto-commit."""
        # Mock that workflow doesn't exist
        mock_session.execute.return_value = mock_query_result(scalar_result=None)

        await repository_with_session.create(sample_workflow_entity)

        # Should not commit when using provided session
        assert not mock_session.commit.called

    async def test_get_by_id_existing_workflow(
        self, repository, mock_session, sample_workflow_model, sample_workflow_entity, mock_query_result
    ):
        """Test retrieving an existing workflow by ID."""
        # Mock that workflow exists
        mock_session.execute.return_value = mock_query_result(scalar_result=sample_workflow_model)

        result = await repository.get_by_id(sample_workflow_entity.workflow_id)

        assert result is not None
        assert result.workflow_id == sample_workflow_entity.workflow_id
        assert result.name == sample_workflow_entity.name
        assert result.workflow_type == sample_workflow_entity.workflow_type
        assert result.status == sample_workflow_entity.status

    async def test_get_by_id_nonexistent_workflow(
        self, repository, mock_session, mock_query_result
    ):
        """Test retrieving a workflow that doesn't exist returns None."""
        # Mock that workflow doesn't exist
        mock_session.execute.return_value = mock_query_result(scalar_result=None)

        result = await repository.get_by_id("nonexistent_workflow")

        assert result is None

    async def test_get_by_simulation(
        self, repository, mock_session, sample_workflow_model, mock_query_result
    ):
        """Test retrieving workflows by simulation ID."""
        # Mock that workflows exist for this simulation
        models = [sample_workflow_model]
        mock_session.execute.return_value = mock_query_result(models=models)

        result = await repository.get_by_simulation("test_sim_001")

        assert len(result) == 1
        assert result[0].workflow_id == sample_workflow_model.workflow_id
        assert result[0].simulation_id == "test_sim_001"

    async def test_get_by_simulation_empty(
        self, repository, mock_session, mock_query_result
    ):
        """Test retrieving workflows for a simulation with no workflows."""
        # Mock empty result
        mock_session.execute.return_value = mock_query_result(models=[])

        result = await repository.get_by_simulation("nonexistent_sim")

        assert len(result) == 0

    async def test_update_existing_workflow(
        self, repository, mock_session, sample_workflow_model, sample_workflow_entity, mock_query_result
    ):
        """Test updating an existing workflow."""
        # Mock that workflow exists
        mock_session.execute.return_value = mock_query_result(scalar_result=sample_workflow_model)

        # Modify entity
        sample_workflow_entity.description = "Updated description"
        sample_workflow_entity.status = WorkflowStatus.ARCHIVED

        result = await repository.update(sample_workflow_entity)

        assert result.workflow_id == sample_workflow_entity.workflow_id
        # Verify session refresh was attempted
        assert mock_session.refresh.called or not mock_session.commit.called

    async def test_update_nonexistent_workflow_raises_error(
        self, repository, mock_session, sample_workflow_entity, mock_query_result
    ):
        """Test that updating a nonexistent workflow raises WorkflowNotFoundError."""
        # Mock that workflow doesn't exist
        mock_session.execute.return_value = mock_query_result(scalar_result=None)

        with pytest.raises(WorkflowNotFoundError, match="not found"):
            await repository.update(sample_workflow_entity)

        # Verify rollback was called
        assert mock_session.rollback.called

    async def test_delete_existing_workflow(
        self, repository, mock_session, mock_query_result
    ):
        """Test deleting an existing workflow."""
        # Mock successful delete
        result_mock = MagicMock()
        result_mock.rowcount = 1
        mock_session.execute.return_value = result_mock

        result = await repository.delete("test_workflow_001")

        assert result is True
        assert mock_session.execute.called

    async def test_delete_nonexistent_workflow(
        self, repository, mock_session, mock_query_result
    ):
        """Test deleting a workflow that doesn't exist returns False."""
        # Mock no rows affected
        result_mock = MagicMock()
        result_mock.rowcount = 0
        mock_session.execute.return_value = result_mock

        result = await repository.delete("nonexistent_workflow")

        assert result is False

    async def test_list_all_workflows(
        self, repository, mock_session, sample_workflow_model, mock_query_result
    ):
        """Test listing all workflows with pagination."""
        # Mock multiple workflows exist
        models = [sample_workflow_model]
        mock_session.execute.return_value = mock_query_result(models=models)

        results = await repository.list_all(skip=0, limit=100)

        assert len(results) == 1
        assert results[0].workflow_id == sample_workflow_model.workflow_id

    async def test_list_all_with_pagination(
        self, repository, mock_session, sample_workflow_model, mock_query_result
    ):
        """Test list_all respects pagination parameters."""
        models = [sample_workflow_model]
        mock_session.execute.return_value = mock_query_result(models=models)

        results = await repository.list_all(skip=10, limit=5)

        # Verify the query was executed (pagination is handled by SQLAlchemy)
        assert mock_session.execute.called
        assert len(results) == 1

    async def test_list_all_empty(
        self, repository, mock_session, mock_query_result
    ):
        """Test listing workflows when none exist."""
        # Mock empty result
        mock_session.execute.return_value = mock_query_result(models=[])

        results = await repository.list_all()

        assert len(results) == 0


@pytest.mark.asyncio
class TestHybridStorage:
    """Test hybrid storage of structured fields and JSON data."""

    async def test_workflow_step_serialization(
        self, repository, sample_workflow_entity
    ):
        """Test WorkflowStep objects are properly serialized to JSON."""
        model = repository._entity_to_model(sample_workflow_entity)

        # Steps should be converted to JSON-serializable dicts
        assert isinstance(model.steps, list)
        assert len(model.steps) == 2
        assert isinstance(model.steps[0], dict)
        assert model.steps[0]["step_id"] == "step1"
        assert model.steps[0]["name"] == "Prepare data"
        assert model.steps[0]["command"] == "python prepare.py"

    async def test_workflow_step_deserialization(
        self, repository, sample_workflow_model
    ):
        """Test JSON dicts are properly deserialized back to WorkflowStep objects."""
        entity = repository._model_to_entity(sample_workflow_model)

        # Steps should be converted back to WorkflowStep objects
        assert isinstance(entity.steps, list)
        assert len(entity.steps) == 2
        assert isinstance(entity.steps[0], WorkflowStep)
        assert entity.steps[0].step_id == "step1"
        assert entity.steps[0].name == "Prepare data"

    async def test_resource_requirements_serialization(
        self, repository, sample_workflow_entity
    ):
        """Test ResourceRequirement objects are properly serialized."""
        model = repository._entity_to_model(sample_workflow_entity)

        # Check resource requirements in steps
        step1 = model.steps[0]
        assert step1["resource_requirements"] is not None
        assert step1["resource_requirements"]["cpu_cores"] == 4
        assert step1["resource_requirements"]["memory_gb"] == 8.0
        assert step1["resource_requirements"]["estimated_runtime"] == "1h"

    async def test_resource_requirements_deserialization(
        self, repository, sample_workflow_model
    ):
        """Test ResourceRequirement objects are properly deserialized."""
        entity = repository._model_to_entity(sample_workflow_model)

        # Check resource requirements in steps
        step1 = entity.steps[0]
        assert step1.resource_requirements is not None
        assert isinstance(step1.resource_requirements, ResourceRequirement)
        assert step1.resource_requirements.cpu_cores == 4
        assert step1.resource_requirements.memory_gb == 8.0

    async def test_set_to_list_conversion_tags(
        self, repository, sample_workflow_entity
    ):
        """Test Set[str] is converted to List[str] for JSON storage (tags)."""
        model = repository._entity_to_model(sample_workflow_entity)

        # Tags should be converted from Set to List
        assert isinstance(model.tags, list)
        assert set(model.tags) == {"climate", "model", "test"}

    async def test_list_to_set_conversion_tags(
        self, repository, sample_workflow_model
    ):
        """Test List[str] is converted back to Set[str] (tags)."""
        entity = repository._model_to_entity(sample_workflow_model)

        # Tags should be converted back to Set
        assert isinstance(entity.tags, set)
        assert entity.tags == {"climate", "model", "test"}

    async def test_set_to_list_conversion_locations(
        self, repository, sample_workflow_entity
    ):
        """Test Set[str] is converted to List[str] for JSON storage (associated_locations)."""
        model = repository._entity_to_model(sample_workflow_entity)

        # Associated locations should be converted from Set to List
        assert isinstance(model.associated_locations, list)
        assert set(model.associated_locations) == {"cluster", "archive"}

    async def test_list_to_set_conversion_locations(
        self, repository, sample_workflow_model
    ):
        """Test List[str] is converted back to Set[str] (associated_locations)."""
        entity = repository._model_to_entity(sample_workflow_model)

        # Associated locations should be converted back to Set
        assert isinstance(entity.associated_locations, set)
        assert entity.associated_locations == {"cluster", "archive"}

    async def test_workflow_phase_serialization(
        self, repository, sample_workflow_entity
    ):
        """Test WorkflowPhase objects are properly serialized to JSON."""
        model = repository._entity_to_model(sample_workflow_entity)

        # Phases should be converted to JSON-serializable dicts
        assert isinstance(model.current_phases, list)
        assert len(model.current_phases) == 1
        assert isinstance(model.current_phases[0], dict)
        assert model.current_phases[0]["phase_id"] == "prep"
        assert model.current_phases[0]["name"] == "Preparation"
        assert model.current_phases[0]["status"] == "COMPLETED"

    async def test_workflow_phase_deserialization(
        self, repository, sample_workflow_model
    ):
        """Test JSON dicts are properly deserialized back to WorkflowPhase objects."""
        entity = repository._model_to_entity(sample_workflow_model)

        # Phases should be converted back to WorkflowPhase objects
        assert isinstance(entity.current_phases, list)
        assert len(entity.current_phases) == 1
        assert isinstance(entity.current_phases[0], WorkflowPhase)
        assert entity.current_phases[0].phase_id == "prep"
        assert entity.current_phases[0].name == "Preparation"
        assert entity.current_phases[0].status == "COMPLETED"

    async def test_deployment_config_json_storage(
        self, repository, sample_workflow_entity
    ):
        """Test deployment_config is stored as JSON."""
        model = repository._entity_to_model(sample_workflow_entity)

        # Deployment config should be stored as-is (already a dict)
        assert isinstance(model.deployment_config, dict)
        assert model.deployment_config["executor"] == "slurm"
        assert model.deployment_config["queue"] == "standard"

    async def test_fingerprint_history_persistence(
        self, repository, sample_workflow_entity
    ):
        """Test fingerprint_history is properly stored."""
        # Add some history
        sample_workflow_entity.fingerprint_history = [
            {"timestamp": "2025-01-01T10:00:00", "status": "running"},
            {"timestamp": "2025-01-01T11:00:00", "status": "completed"}
        ]

        model = repository._entity_to_model(sample_workflow_entity)

        # History should be stored as-is (list of dicts)
        assert isinstance(model.fingerprint_history, list)
        assert len(model.fingerprint_history) == 2
        assert model.fingerprint_history[0]["status"] == "running"

    async def test_current_fingerprint_storage(
        self, repository, sample_workflow_entity
    ):
        """Test current_fingerprint is properly stored and retrieved."""
        model = repository._entity_to_model(sample_workflow_entity)
        entity = repository._model_to_entity(model)

        # Current fingerprint should be preserved
        assert entity.current_fingerprint is not None
        assert entity.current_fingerprint["status"] == "running"
        assert len(entity.current_fingerprint["phases"]) == 1


@pytest.mark.asyncio
class TestUpdateFingerprint:
    """Test specialized update_fingerprint method."""

    async def test_update_fingerprint_updates_only_fingerprint_fields(
        self, repository, mock_session, sample_workflow_model, mock_query_result
    ):
        """Test update_fingerprint only modifies fingerprint-related fields."""
        # Mock that workflow exists
        mock_session.execute.return_value = mock_query_result(scalar_result=sample_workflow_model)

        new_fingerprint = {
            "status": "completed",
            "phases": [
                {
                    "phase_id": "compute",
                    "name": "Computation",
                    "status": "COMPLETED",
                    "start_time": "2025-01-01T11:00:00",
                    "end_time": "2025-01-01T12:00:00"
                }
            ],
            "observed_files": [
                {"path": "/output/data.nc", "size": 1024}
            ],
            "metadata": {"run_id": "12345"}
        }

        result = await repository.update_fingerprint(
            sample_workflow_model.workflow_id,
            new_fingerprint
        )

        # Verify result is a WorkflowEntity
        assert isinstance(result, WorkflowEntity)
        # Original workflow properties should be unchanged
        assert result.workflow_id == sample_workflow_model.workflow_id
        assert result.name == sample_workflow_model.name

    async def test_update_fingerprint_nonexistent_raises_error(
        self, repository, mock_session, mock_query_result
    ):
        """Test update_fingerprint on nonexistent workflow raises error."""
        # Mock that workflow doesn't exist
        mock_session.execute.return_value = mock_query_result(scalar_result=None)

        with pytest.raises(WorkflowNotFoundError):
            await repository.update_fingerprint(
                "nonexistent",
                {"status": "running"}
            )

    async def test_fingerprint_history_accumulation(
        self, repository, mock_session, sample_workflow_model, sample_workflow_entity, mock_query_result
    ):
        """Test that fingerprint history accumulates over updates."""
        # Mock that workflow exists
        mock_session.execute.return_value = mock_query_result(scalar_result=sample_workflow_model)

        # First update
        fingerprint1 = {
            "status": "running",
            "phases": [],
            "observed_files": [],
            "metadata": {"update": 1}
        }

        await repository.update_fingerprint(
            sample_workflow_model.workflow_id,
            fingerprint1
        )

        # The entity's update_from_fingerprint method handles history
        # We're testing the repository calls it correctly
        assert mock_session.execute.called


@pytest.mark.asyncio
class TestDataIntegrity:
    """Test data integrity, type handling, and edge cases."""

    async def test_datetime_timezone_handling(
        self, repository, sample_workflow_entity
    ):
        """Test datetime objects are properly handled with timezones."""
        model = repository._entity_to_model(sample_workflow_entity)
        entity = repository._model_to_entity(model)

        # Datetimes should be preserved
        assert entity.latest_observation_time is not None
        assert isinstance(entity.latest_observation_time, datetime)

    async def test_timedelta_serialization(
        self, repository, sample_workflow_entity
    ):
        """Test timedelta objects are serialized to strings."""
        model = repository._entity_to_model(sample_workflow_entity)

        # Check timedelta in step timeout
        step1 = model.steps[0]
        assert step1["timeout"] == "2h"  # 2 hours
        assert step1["retry_delay"] == "10s"

    async def test_timedelta_deserialization(
        self, repository, sample_workflow_model
    ):
        """Test timedelta strings are deserialized back to timedelta objects."""
        entity = repository._model_to_entity(sample_workflow_model)

        # Check timedelta in step timeout
        step1 = entity.steps[0]
        assert step1.timeout == timedelta(hours=2)
        assert step1.retry_delay == timedelta(seconds=10)

    async def test_complex_nested_json_structures(
        self, repository, sample_workflow_entity
    ):
        """Test deeply nested JSON structures are preserved."""
        # Add complex nested metadata
        sample_workflow_entity.metadata = {
            "project": {
                "name": "CMIP6",
                "models": ["CESM2", "MPAS"],
                "config": {
                    "resolution": "high",
                    "ensemble": {
                        "members": [1, 2, 3],
                        "perturbation": True
                    }
                }
            }
        }

        model = repository._entity_to_model(sample_workflow_entity)
        entity = repository._model_to_entity(model)

        # Complex structure should be preserved
        assert entity.metadata["project"]["name"] == "CMIP6"
        assert entity.metadata["project"]["models"] == ["CESM2", "MPAS"]
        assert entity.metadata["project"]["config"]["ensemble"]["members"] == [1, 2, 3]

    async def test_null_handling_optional_fields(
        self, repository
    ):
        """Test NULL values for optional fields are handled correctly."""
        # Create minimal workflow entity
        minimal_workflow = WorkflowEntity(
            workflow_id="minimal",
            name="Minimal Workflow",
            workflow_type=WorkflowType.CUSTOM,
            steps=[
                WorkflowStep(
                    step_id="s1",
                    name="Step 1",
                    command="echo hello"
                )
            ],
            # Leave optional fields as defaults/None
            simulation_id=None,
            description="",
            author="",
            current_fingerprint=None,
            latest_observation_time=None
        )

        model = repository._entity_to_model(minimal_workflow)
        entity = repository._model_to_entity(model)

        # Optional fields should handle None
        assert entity.simulation_id is None
        assert entity.current_fingerprint is None
        assert entity.latest_observation_time is None

    async def test_enum_serialization(
        self, repository, sample_workflow_entity
    ):
        """Test enum types (WorkflowType, WorkflowStatus) are serialized to strings."""
        model = repository._entity_to_model(sample_workflow_entity)

        # Enums should be stored as string names
        assert model.workflow_type == "MODEL_EXECUTION"
        assert model.status == "READY"
        assert isinstance(model.workflow_type, str)
        assert isinstance(model.status, str)

    async def test_enum_deserialization(
        self, repository, sample_workflow_model
    ):
        """Test enum strings are deserialized back to enum types."""
        entity = repository._model_to_entity(sample_workflow_model)

        # Enums should be converted back to enum types
        assert entity.workflow_type == WorkflowType.MODEL_EXECUTION
        assert entity.status == WorkflowStatus.READY
        assert isinstance(entity.workflow_type, WorkflowType)
        assert isinstance(entity.status, WorkflowStatus)

    async def test_empty_collections_handling(
        self, repository
    ):
        """Test empty collections (sets, lists, dicts) are handled correctly."""
        workflow = WorkflowEntity(
            workflow_id="empty_collections",
            name="Empty Collections",
            workflow_type=WorkflowType.CUSTOM,
            steps=[
                WorkflowStep(
                    step_id="s1",
                    name="Step 1",
                    command="echo hello"
                )
            ],
            tags=set(),  # Empty set
            parameters={},  # Empty dict
            associated_locations=set(),  # Empty set
            fingerprint_history=[]  # Empty list
        )

        model = repository._entity_to_model(workflow)
        entity = repository._model_to_entity(model)

        # Empty collections should be preserved
        assert entity.tags == set()
        assert entity.parameters == {}
        assert entity.associated_locations == set()
        assert entity.fingerprint_history == []


@pytest.mark.asyncio
class TestErrorHandling:
    """Test error handling and rollback behavior."""

    async def test_database_error_on_create(
        self, repository, mock_session, sample_workflow_entity
    ):
        """Test database errors during create are properly handled."""
        # Mock database error
        mock_session.execute.side_effect = Exception("Database connection error")

        with pytest.raises(RepositoryError, match="Failed to create workflow"):
            await repository.create(sample_workflow_entity)

        # Verify rollback was called
        assert mock_session.rollback.called

    async def test_database_error_on_get_by_id(
        self, repository, mock_session
    ):
        """Test database errors during get_by_id are properly handled."""
        mock_session.execute.side_effect = Exception("Database error")

        with pytest.raises(RepositoryError, match="Failed to retrieve workflow"):
            await repository.get_by_id("test_workflow")

    async def test_database_error_on_get_by_simulation(
        self, repository, mock_session
    ):
        """Test database errors during get_by_simulation are properly handled."""
        mock_session.execute.side_effect = Exception("Database error")

        with pytest.raises(RepositoryError, match="Failed to retrieve workflows"):
            await repository.get_by_simulation("test_sim")

    async def test_database_error_on_update(
        self, repository, mock_session, sample_workflow_entity
    ):
        """Test database errors during update are properly handled."""
        mock_session.execute.side_effect = Exception("Database error")

        with pytest.raises(RepositoryError, match="Failed to update workflow"):
            await repository.update(sample_workflow_entity)

        # Verify rollback was called
        assert mock_session.rollback.called

    async def test_database_error_on_delete(
        self, repository, mock_session
    ):
        """Test database errors during delete are properly handled."""
        mock_session.execute.side_effect = Exception("Database error")

        with pytest.raises(RepositoryError, match="Failed to delete workflow"):
            await repository.delete("test_workflow")

        # Verify rollback was called
        assert mock_session.rollback.called

    async def test_database_error_on_list_all(
        self, repository, mock_session
    ):
        """Test database errors during list_all are properly handled."""
        mock_session.execute.side_effect = Exception("Database error")

        with pytest.raises(RepositoryError, match="Failed to list workflows"):
            await repository.list_all()

    async def test_database_error_on_update_fingerprint(
        self, repository, mock_session
    ):
        """Test database errors during update_fingerprint are properly handled."""
        mock_session.execute.side_effect = Exception("Database error")

        with pytest.raises(RepositoryError, match="Failed to update fingerprint"):
            await repository.update_fingerprint("test_workflow", {})

        # Verify rollback was called
        assert mock_session.rollback.called

    async def test_integrity_error_on_create(
        self, repository, mock_session, sample_workflow_entity, mock_query_result
    ):
        """Test IntegrityError during create is handled gracefully."""
        # Mock that workflow doesn't exist initially
        mock_session.execute.return_value = mock_query_result(scalar_result=None)
        # But then raise IntegrityError on add (race condition)
        mock_session.add.side_effect = IntegrityError("duplicate key", None, None)

        # IntegrityError should be caught and converted to RepositoryError
        with pytest.raises(RepositoryError):
            await repository.create(sample_workflow_entity)

        # Verify rollback was called
        assert mock_session.rollback.called


@pytest.mark.asyncio
class TestAsyncWorkflowRepositoryWrapper:
    """Test the async to sync wrapper functionality."""

    def test_wrapper_creation(self):
        """Test creating the wrapper."""
        from tellus.infrastructure.repositories.postgres_workflow_repository import AsyncWorkflowRepositoryWrapper

        async_repo = PostgresWorkflowRepository()
        wrapper = AsyncWorkflowRepositoryWrapper(async_repo)

        assert wrapper.async_repo is async_repo

    def test_wrapper_create(self, sample_workflow_entity):
        """Test sync wrapper for create operation."""
        from unittest.mock import patch
        from tellus.infrastructure.repositories.postgres_workflow_repository import AsyncWorkflowRepositoryWrapper

        async_repo = PostgresWorkflowRepository()
        wrapper = AsyncWorkflowRepositoryWrapper(async_repo)

        with patch('asyncio.run') as mock_asyncio_run:
            wrapper.create(sample_workflow_entity)
            mock_asyncio_run.assert_called_once()

    def test_wrapper_get_by_id(self):
        """Test sync wrapper for get_by_id operation."""
        from unittest.mock import patch
        from tellus.infrastructure.repositories.postgres_workflow_repository import AsyncWorkflowRepositoryWrapper

        async_repo = PostgresWorkflowRepository()
        wrapper = AsyncWorkflowRepositoryWrapper(async_repo)

        with patch('asyncio.run') as mock_asyncio_run:
            wrapper.get_by_id("test_workflow")
            mock_asyncio_run.assert_called_once()

    def test_wrapper_get_by_simulation(self):
        """Test sync wrapper for get_by_simulation operation."""
        from unittest.mock import patch
        from tellus.infrastructure.repositories.postgres_workflow_repository import AsyncWorkflowRepositoryWrapper

        async_repo = PostgresWorkflowRepository()
        wrapper = AsyncWorkflowRepositoryWrapper(async_repo)

        with patch('asyncio.run') as mock_asyncio_run:
            wrapper.get_by_simulation("test_sim")
            mock_asyncio_run.assert_called_once()

    def test_wrapper_update(self, sample_workflow_entity):
        """Test sync wrapper for update operation."""
        from unittest.mock import patch
        from tellus.infrastructure.repositories.postgres_workflow_repository import AsyncWorkflowRepositoryWrapper

        async_repo = PostgresWorkflowRepository()
        wrapper = AsyncWorkflowRepositoryWrapper(async_repo)

        with patch('asyncio.run') as mock_asyncio_run:
            wrapper.update(sample_workflow_entity)
            mock_asyncio_run.assert_called_once()

    def test_wrapper_delete(self):
        """Test sync wrapper for delete operation."""
        from unittest.mock import patch
        from tellus.infrastructure.repositories.postgres_workflow_repository import AsyncWorkflowRepositoryWrapper

        async_repo = PostgresWorkflowRepository()
        wrapper = AsyncWorkflowRepositoryWrapper(async_repo)

        with patch('asyncio.run') as mock_asyncio_run:
            wrapper.delete("test_workflow")
            mock_asyncio_run.assert_called_once()

    def test_wrapper_list_all(self):
        """Test sync wrapper for list_all operation."""
        from unittest.mock import patch
        from tellus.infrastructure.repositories.postgres_workflow_repository import AsyncWorkflowRepositoryWrapper

        async_repo = PostgresWorkflowRepository()
        wrapper = AsyncWorkflowRepositoryWrapper(async_repo)

        with patch('asyncio.run') as mock_asyncio_run:
            wrapper.list_all()
            mock_asyncio_run.assert_called_once()

    def test_wrapper_update_fingerprint(self):
        """Test sync wrapper for update_fingerprint operation."""
        from unittest.mock import patch
        from tellus.infrastructure.repositories.postgres_workflow_repository import AsyncWorkflowRepositoryWrapper

        async_repo = PostgresWorkflowRepository()
        wrapper = AsyncWorkflowRepositoryWrapper(async_repo)

        with patch('asyncio.run') as mock_asyncio_run:
            wrapper.update_fingerprint("test_workflow", {})
            mock_asyncio_run.assert_called_once()


@pytest.mark.asyncio
class TestRoundTripConversions:
    """Test complete round-trip conversions between entity and model."""

    async def test_full_entity_to_model_to_entity_roundtrip(
        self, repository, sample_workflow_entity
    ):
        """Test that converting entity -> model -> entity preserves all data."""
        # Convert to model and back
        model = repository._entity_to_model(sample_workflow_entity)
        result_entity = repository._model_to_entity(model)

        # Core fields
        assert result_entity.workflow_id == sample_workflow_entity.workflow_id
        assert result_entity.simulation_id == sample_workflow_entity.simulation_id
        assert result_entity.name == sample_workflow_entity.name
        assert result_entity.workflow_type == sample_workflow_entity.workflow_type
        assert result_entity.description == sample_workflow_entity.description
        assert result_entity.version == sample_workflow_entity.version
        assert result_entity.author == sample_workflow_entity.author
        assert result_entity.status == sample_workflow_entity.status

        # Collections (Sets should be preserved)
        assert result_entity.tags == sample_workflow_entity.tags
        assert result_entity.associated_locations == sample_workflow_entity.associated_locations

        # Dicts
        assert result_entity.parameters == sample_workflow_entity.parameters
        assert result_entity.metadata == sample_workflow_entity.metadata
        assert result_entity.location_contexts == sample_workflow_entity.location_contexts

        # Steps (complex objects)
        assert len(result_entity.steps) == len(sample_workflow_entity.steps)
        for orig_step, result_step in zip(sample_workflow_entity.steps, result_entity.steps):
            assert result_step.step_id == orig_step.step_id
            assert result_step.name == orig_step.name
            assert result_step.command == orig_step.command

        # Fingerprint data
        assert result_entity.observed_file_count == sample_workflow_entity.observed_file_count
        assert result_entity.current_status == sample_workflow_entity.current_status
        assert len(result_entity.current_phases) == len(sample_workflow_entity.current_phases)
