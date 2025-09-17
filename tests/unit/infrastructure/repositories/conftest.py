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
from tellus.infrastructure.database.models import SimulationModel, LocationModel, SimulationLocationContextModel
from tellus.domain.entities.simulation import SimulationEntity
from tellus.domain.entities.location import LocationEntity, LocationKind


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