"""
Fixtures and configuration for repository tests.

This conftest provides mocked database infrastructure for unit testing
repositories without requiring a real database connection.
"""

import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from typing import AsyncGenerator, List, Optional, Dict, Any

from tellus.infrastructure.database.config import DatabaseConfig, DatabaseManager
from tellus.infrastructure.database.models import SimulationModel, LocationModel, SimulationLocationContextModel, WorkflowModel
from tellus.domain.entities.simulation import SimulationEntity
from tellus.domain.entities.location import LocationEntity, LocationKind
from tellus.domain.entities.workflow import (
    WorkflowEntity,
    WorkflowType,
    WorkflowStatus,
    WorkflowStep,
    WorkflowPhase,
    ResourceRequirement,
)
from datetime import datetime, timedelta


@pytest.fixture(scope="session", autouse=True)
def event_loop():
    """Create an instance of the default event loop for the test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
def mock_session():
    """Create a mock AsyncSession for testing."""
    session = AsyncMock()
    session.commit = AsyncMock()
    session.rollback = AsyncMock()
    session.close = AsyncMock()
    session.execute = AsyncMock()
    session.add = MagicMock()
    session.scalar_one_or_none = AsyncMock()
    session.scalars = MagicMock()
    return session


@pytest.fixture
def mock_db_manager(mock_session):
    """Create a mock DatabaseManager for testing."""
    manager = AsyncMock(spec=DatabaseManager)

    # Make get_session return a context manager that yields our mock session
    def get_session_context():
        return MockSessionContext(mock_session)

    manager.get_session = get_session_context
    manager.create_tables = AsyncMock()
    manager.close = AsyncMock()

    return manager


class MockSessionContext:
    """Mock context manager for database session."""

    def __init__(self, session):
        self.session = session

    async def __aenter__(self):
        return self.session

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            await self.session.rollback()
        else:
            await self.session.commit()
        await self.session.close()


@pytest.fixture
def mock_query_result():
    """Factory fixture for creating mock query results."""
    def _create_result(models: List = None, scalar_result = None):
        result = MagicMock()

        if scalar_result is not None:
            result.scalar_one_or_none.return_value = scalar_result
        elif models is not None:
            scalars = MagicMock()
            scalars.all.return_value = models
            result.scalars.return_value = scalars
            result.scalar_one_or_none.return_value = models[0] if models else None
        else:
            result.scalar_one_or_none.return_value = None
            scalars = MagicMock()
            scalars.all.return_value = []
            result.scalars.return_value = scalars

        result.rowcount = len(models) if models else 0
        return result

    return _create_result


@pytest.fixture
def sample_simulation_entity():
    """Create a sample simulation entity for testing."""
    return SimulationEntity(
        simulation_id="test_sim_001",
        model_id="FESOM2",
        path="/path/to/simulation",
        attrs={
            "experiment": "test_experiment",
            "model": "fesom",
            "resolution": "low"
        },
        namelists={
            "namelist.config": {"param1": "value1"}
        },
        snakemakes={
            "workflow": {"rule": "all"}
        },
        associated_locations={"cluster", "archive"},
        location_contexts={
            "cluster": {"path_prefix": "/work/data"},
            "archive": {"path_prefix": "/archive/data"}
        }
    )


@pytest.fixture
def sample_location_entity():
    """Create a sample location entity for testing."""
    from tellus.domain.entities.location import PathTemplate

    return LocationEntity(
        name="test_cluster",
        kinds=[LocationKind.COMPUTE, LocationKind.DISK],
        config={
            "protocol": "sftp",
            "path": "/work/data",
            "storage_options": {
                "host": "cluster.example.com",
                "username": "testuser"
            },
            "additional_config": {
                "queue_system": "slurm",
                "max_jobs": 100
            },
            "is_remote": True,
            "is_accessible": True,
        },
        path_templates=[
            PathTemplate(
                name="experiment_template",
                pattern="{model}/{experiment}",
                description="Standard experiment path",
                required_attributes=["model", "experiment"]
            )
        ]
    )


@pytest.fixture
def sample_simulation_model(sample_simulation_entity):
    """Create a sample SimulationModel for testing."""
    return SimulationModel(
        simulation_id=sample_simulation_entity.simulation_id,
        uid=sample_simulation_entity.uid,
        model_id=sample_simulation_entity.model_id,
        path=sample_simulation_entity.path,
        attrs=sample_simulation_entity.attrs,
        namelists=sample_simulation_entity.namelists,
        workflows=sample_simulation_entity.snakemakes,
    )


@pytest.fixture
def sample_location_model(sample_location_entity):
    """Create a sample LocationModel for testing."""
    kind_strings = [kind.name for kind in sample_location_entity.kinds]

    # Create a mock object instead of a real model to avoid DB dependencies
    from unittest.mock import MagicMock
    model = MagicMock()
    model.name = sample_location_entity.name
    model.kinds = kind_strings
    model.protocol = sample_location_entity.config["protocol"]
    model.path = sample_location_entity.config["path"]
    model.storage_options = sample_location_entity.config["storage_options"]
    model.additional_config = sample_location_entity.config["additional_config"]
    model.is_remote = sample_location_entity.config["is_remote"]
    model.is_accessible = sample_location_entity.config["is_accessible"]
    model.last_verified = None
    model.path_templates = {}  # Simplified for testing

    return model


@pytest.fixture
def sample_workflow_entity():
    """Create a sample workflow entity for testing."""
    return WorkflowEntity(
        workflow_id="test_workflow_001",
        simulation_id="test_sim_001",
        name="Test Climate Model Workflow",
        workflow_type=WorkflowType.MODEL_EXECUTION,
        description="Test workflow for climate model execution",
        version="1.0.0",
        author="test_user",
        status=WorkflowStatus.READY,
        steps=[
            WorkflowStep(
                step_id="step1",
                name="Prepare data",
                command="python prepare.py",
                dependencies=[],
                resource_requirements=ResourceRequirement(
                    cpu_cores=4,
                    memory_gb=8.0,
                    disk_space_gb=100.0,
                    estimated_runtime=timedelta(hours=1)
                ),
                environment={"ENV_VAR": "value"},
                working_directory="/work/dir",
                timeout=timedelta(hours=2),
                retry_count=3,
                metadata={"priority": "high"}
            ),
            WorkflowStep(
                step_id="step2",
                name="Run model",
                command="./model.exe",
                dependencies=["step1"],
                resource_requirements=ResourceRequirement(
                    cpu_cores=128,
                    memory_gb=256.0,
                    disk_space_gb=1000.0,
                    gpu_count=4,
                    estimated_runtime=timedelta(hours=24)
                )
            )
        ],
        tags={"climate", "model", "test"},
        parameters={
            "resolution": "T63",
            "timestep": 1800,
            "output_frequency": "daily"
        },
        metadata={"project": "test_project", "funding": "test_grant"},
        simulation_context={
            "experiment_id": "piControl",
            "ensemble_member": "r1i1p1f1"
        },
        associated_locations={"cluster", "archive"},
        location_contexts={
            "cluster": {"base_path": "/work/climate"},
            "archive": {"base_path": "/archive/data"}
        },
        input_location_mapping={"step1": "cluster"},
        output_location_mapping={"step2": "archive"},
        workflow_system="snakemake",
        deployment_path="/work/workflows/test",
        deployment_config={"executor": "slurm", "queue": "standard"},
        polling_enabled=True,
        polling_interval_minutes=15,
        current_phases=[
            WorkflowPhase(
                phase_id="prep",
                name="Preparation",
                status="COMPLETED",
                start_time=datetime(2025, 1, 1, 10, 0, 0),
                end_time=datetime(2025, 1, 1, 10, 30, 0),
                log_files=["/logs/prep.log"],
                metadata={"files_prepared": 10}
            )
        ],
        observed_file_count=42,
        latest_observation_time=datetime(2025, 1, 1, 12, 0, 0),
        current_status="running",
        current_fingerprint={
            "status": "running",
            "phases": [
                {
                    "phase_id": "prep",
                    "name": "Preparation",
                    "status": "COMPLETED",
                    "start_time": "2025-01-01T10:00:00",
                    "end_time": "2025-01-01T10:30:00"
                }
            ],
            "observed_files": [],
            "metadata": {}
        },
        fingerprint_history=[]
    )


@pytest.fixture
def sample_workflow_model(sample_workflow_entity):
    """Create a sample WorkflowModel for testing."""
    from unittest.mock import MagicMock

    # Create a mock model with all the necessary attributes
    # This avoids SQLAlchemy session initialization issues in unit tests
    model = MagicMock(spec=WorkflowModel)
    model.workflow_id = sample_workflow_entity.workflow_id
    model.simulation_id = sample_workflow_entity.simulation_id
    model.name = sample_workflow_entity.name
    model.workflow_type = sample_workflow_entity.workflow_type.name
    model.description = sample_workflow_entity.description
    model.version = sample_workflow_entity.version
    model.author = sample_workflow_entity.author
    model.status = sample_workflow_entity.status.name

    # Convert steps to JSON format
    model.steps = [
        {
            "step_id": "step1",
            "name": "Prepare data",
            "command": "python prepare.py",
            "dependencies": [],
            "resource_requirements": {
                "cpu_cores": 4,
                "memory_gb": 8.0,
                "disk_space_gb": 100.0,
                "gpu_count": 0,
                "estimated_runtime": "1h",
                "special_requirements": {}
            },
            "environment": {"ENV_VAR": "value"},
            "working_directory": "/work/dir",
            "timeout": "2h",
            "retry_count": 3,
            "retry_delay": "10s",
            "metadata": {"priority": "high"}
        },
        {
            "step_id": "step2",
            "name": "Run model",
            "command": "./model.exe",
            "dependencies": ["step1"],
            "resource_requirements": {
                "cpu_cores": 128,
                "memory_gb": 256.0,
                "disk_space_gb": 1000.0,
                "gpu_count": 4,
                "estimated_runtime": "24h",
                "special_requirements": {}
            },
            "environment": {},
            "working_directory": None,
            "timeout": None,
            "retry_count": 0,
            "retry_delay": "10s",
            "metadata": {}
        }
    ]

    # Convert sets to lists for JSON
    model.tags = list(sample_workflow_entity.tags)
    model.parameters = sample_workflow_entity.parameters
    model.workflow_metadata = sample_workflow_entity.metadata
    model.simulation_context = sample_workflow_entity.simulation_context
    model.associated_locations = list(sample_workflow_entity.associated_locations)
    model.location_contexts = sample_workflow_entity.location_contexts
    model.input_location_mapping = sample_workflow_entity.input_location_mapping
    model.output_location_mapping = sample_workflow_entity.output_location_mapping
    model.workflow_system = sample_workflow_entity.workflow_system
    model.deployment_path = sample_workflow_entity.deployment_path
    model.deployment_config = sample_workflow_entity.deployment_config
    model.polling_enabled = sample_workflow_entity.polling_enabled
    model.polling_interval_minutes = sample_workflow_entity.polling_interval_minutes

    # Convert phases to JSON format
    model.current_phases = [
        {
            "phase_id": "prep",
            "name": "Preparation",
            "status": "COMPLETED",
            "start_time": "2025-01-01T10:00:00",
            "end_time": "2025-01-01T10:30:00",
            "log_files": ["/logs/prep.log"],
            "metadata": {"files_prepared": 10}
        }
    ]

    model.observed_file_count = sample_workflow_entity.observed_file_count
    model.latest_observation_time = sample_workflow_entity.latest_observation_time
    model.current_status = sample_workflow_entity.current_status
    model.current_fingerprint = sample_workflow_entity.current_fingerprint
    model.fingerprint_history = sample_workflow_entity.fingerprint_history
    model.created_at = sample_workflow_entity.created_at
    model.updated_at = sample_workflow_entity.updated_at

    return model


@pytest.fixture(autouse=True)
def mock_database_manager(mock_db_manager, mock_session):
    """Automatically mock the database manager for all tests."""
    # Fix the mock manager to return proper context manager
    def get_session_context():
        return MockSessionContext(mock_session)

    mock_db_manager.get_session = get_session_context

    with patch('tellus.infrastructure.database.config.get_database_manager', return_value=mock_db_manager):
        yield mock_db_manager


# Add markers for unit tests
pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.unit,  # These are unit tests with mocked database
]