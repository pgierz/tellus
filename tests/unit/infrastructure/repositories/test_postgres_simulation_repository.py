"""
Tests for PostgreSQL simulation repository.

Uses testcontainers to spin up a real PostgreSQL instance for integration testing.
"""

import pytest
import asyncio
from typing import AsyncGenerator
from testcontainers.postgres import PostgresContainer

from tellus.infrastructure.database.config import DatabaseConfig, DatabaseManager
from tellus.infrastructure.database.models import Base
from tellus.infrastructure.repositories.postgres_simulation_repository import PostgresSimulationRepository
from tellus.domain.entities.simulation import SimulationEntity
from tellus.domain.repositories.exceptions import SimulationExistsError, RepositoryError


@pytest.fixture(scope="module")
def postgres_container():
    """Start a PostgreSQL container for testing."""
    with PostgresContainer("postgres:15") as postgres:
        yield postgres


@pytest.fixture(scope="module")
async def db_manager(postgres_container):
    """Create database manager for testing."""
    db_url = postgres_container.get_connection_url()
    # Convert psycopg2 URL to asyncpg URL
    async_db_url = db_url.replace("postgresql://", "postgresql+asyncpg://")

    config = DatabaseConfig.from_url(async_db_url)
    manager = DatabaseManager(config)

    # Create tables
    await manager.create_tables()

    yield manager

    await manager.close()


@pytest.fixture
async def repository(db_manager):
    """Create repository instance for testing."""
    return PostgresSimulationRepository()


@pytest.fixture
async def sample_simulation():
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


@pytest.mark.asyncio
class TestPostgresSimulationRepository:
    """Test PostgreSQL simulation repository functionality."""

    async def test_save_and_get_simulation(self, repository, sample_simulation):
        """Test saving and retrieving a simulation."""
        # Save simulation
        await repository.save(sample_simulation)

        # Retrieve simulation
        retrieved = await repository.get_by_id(sample_simulation.simulation_id)

        assert retrieved is not None
        assert retrieved.simulation_id == sample_simulation.simulation_id
        assert retrieved.model_id == sample_simulation.model_id
        assert retrieved.path == sample_simulation.path
        assert retrieved.attrs == sample_simulation.attrs
        assert retrieved.namelists == sample_simulation.namelists
        assert retrieved.snakemakes == sample_simulation.snakemakes
        assert retrieved.associated_locations == sample_simulation.associated_locations
        assert retrieved.location_contexts == sample_simulation.location_contexts

    async def test_save_update_simulation(self, repository, sample_simulation):
        """Test updating an existing simulation."""
        # Save original
        await repository.save(sample_simulation)

        # Update simulation
        sample_simulation.model_id = "UPDATED_MODEL"
        sample_simulation.attrs["new_attr"] = "new_value"

        # Save update
        await repository.save(sample_simulation)

        # Verify update
        retrieved = await repository.get_by_id(sample_simulation.simulation_id)
        assert retrieved.model_id == "UPDATED_MODEL"
        assert retrieved.attrs["new_attr"] == "new_value"

    async def test_get_nonexistent_simulation(self, repository):
        """Test getting a simulation that doesn't exist."""
        result = await repository.get_by_id("nonexistent")
        assert result is None

    async def test_list_all_simulations(self, repository):
        """Test listing all simulations."""
        # Create multiple simulations
        sims = [
            SimulationEntity(simulation_id="sim1", model_id="model1"),
            SimulationEntity(simulation_id="sim2", model_id="model2"),
            SimulationEntity(simulation_id="sim3", model_id="model3"),
        ]

        # Save all
        for sim in sims:
            await repository.save(sim)

        # List all
        all_sims = await repository.list_all()
        sim_ids = {sim.simulation_id for sim in all_sims}

        # Should include all our simulations (and possibly others from other tests)
        assert {"sim1", "sim2", "sim3"}.issubset(sim_ids)

    async def test_delete_simulation(self, repository, sample_simulation):
        """Test deleting a simulation."""
        # Save simulation
        await repository.save(sample_simulation)

        # Verify it exists
        assert await repository.exists(sample_simulation.simulation_id)

        # Delete simulation
        deleted = await repository.delete(sample_simulation.simulation_id)
        assert deleted is True

        # Verify it's gone
        assert not await repository.exists(sample_simulation.simulation_id)
        assert await repository.get_by_id(sample_simulation.simulation_id) is None

    async def test_delete_nonexistent_simulation(self, repository):
        """Test deleting a simulation that doesn't exist."""
        deleted = await repository.delete("nonexistent")
        assert deleted is False

    async def test_exists_simulation(self, repository, sample_simulation):
        """Test checking if simulation exists."""
        # Should not exist initially
        assert not await repository.exists(sample_simulation.simulation_id)

        # Save simulation
        await repository.save(sample_simulation)

        # Should exist now
        assert await repository.exists(sample_simulation.simulation_id)

    async def test_count_simulations(self, repository):
        """Test counting simulations."""
        initial_count = await repository.count()

        # Add simulations
        sims = [
            SimulationEntity(simulation_id=f"count_sim_{i}")
            for i in range(3)
        ]

        for sim in sims:
            await repository.save(sim)

        final_count = await repository.count()
        assert final_count >= initial_count + 3

    async def test_location_contexts_persistence(self, repository):
        """Test that location contexts are properly saved and retrieved."""
        sim = SimulationEntity(
            simulation_id="context_test_sim",
            location_contexts={
                "location1": {"key1": "value1", "nested": {"key2": "value2"}},
                "location2": {"key3": "value3"}
            },
            associated_locations={"location1", "location2"}
        )

        await repository.save(sim)
        retrieved = await repository.get_by_id(sim.simulation_id)

        assert retrieved.location_contexts == sim.location_contexts
        assert retrieved.associated_locations == sim.associated_locations

    async def test_complex_attributes_persistence(self, repository):
        """Test that complex attribute structures are preserved."""
        complex_attrs = {
            "nested_dict": {
                "level1": {
                    "level2": ["item1", "item2"]
                }
            },
            "list_of_dicts": [
                {"id": 1, "name": "first"},
                {"id": 2, "name": "second"}
            ],
            "numeric_values": {
                "int": 42,
                "float": 3.14159
            }
        }

        sim = SimulationEntity(
            simulation_id="complex_attrs_sim",
            attrs=complex_attrs
        )

        await repository.save(sim)
        retrieved = await repository.get_by_id(sim.simulation_id)

        assert retrieved.attrs == complex_attrs

    async def test_concurrent_operations(self, repository):
        """Test concurrent repository operations."""
        async def save_simulation(sim_id: str):
            sim = SimulationEntity(simulation_id=sim_id)
            await repository.save(sim)
            return await repository.get_by_id(sim_id)

        # Run multiple operations concurrently
        tasks = [
            save_simulation(f"concurrent_sim_{i}")
            for i in range(10)
        ]

        results = await asyncio.gather(*tasks)

        # All should succeed
        assert len(results) == 10
        assert all(result is not None for result in results)
        assert len(set(result.simulation_id for result in results)) == 10


@pytest.mark.asyncio
class TestPostgresSimulationRepositoryErrorHandling:
    """Test error handling in PostgreSQL simulation repository."""

    async def test_database_connection_error(self):
        """Test handling of database connection errors."""
        # Create repository with invalid connection
        from tellus.infrastructure.database.config import DatabaseConfig
        invalid_config = DatabaseConfig(
            host="nonexistent-host",
            database="nonexistent-db"
        )

        from tellus.infrastructure.database.config import DatabaseManager
        invalid_manager = DatabaseManager(invalid_config)

        repository = PostgresSimulationRepository()
        # This should raise a connection error when trying to operate
        with pytest.raises(RepositoryError):
            await repository.get_by_id("test_sim")