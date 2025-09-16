"""
Tests for PostgreSQL location repository.

Uses testcontainers to spin up a real PostgreSQL instance for integration testing.
"""

import pytest
import asyncio
from datetime import datetime
from typing import AsyncGenerator
from testcontainers.postgres import PostgresContainer

from tellus.infrastructure.database.config import DatabaseConfig, DatabaseManager
from tellus.infrastructure.database.models import Base
from tellus.infrastructure.repositories.postgres_location_repository import PostgresLocationRepository
from tellus.domain.entities.location import LocationEntity, LocationKind, PathTemplate
from tellus.domain.repositories.exceptions import LocationExistsError, RepositoryError


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
    return PostgresLocationRepository()


@pytest.fixture
async def sample_location():
    """Create a sample location entity for testing."""
    return LocationEntity(
        name="test_cluster",
        kinds=[LocationKind.COMPUTE, LocationKind.DISK],
        protocol="sftp",
        path="/work/data",
        storage_options={
            "host": "cluster.example.com",
            "username": "testuser"
        },
        additional_config={
            "queue_system": "slurm",
            "max_jobs": 100
        },
        is_remote=True,
        is_accessible=True,
        path_templates=[
            PathTemplate(
                name="experiment_template",
                pattern="{model}/{experiment}",
                description="Standard experiment path",
                required_attributes=["model", "experiment"]
            )
        ]
    )


@pytest.mark.asyncio
class TestPostgresLocationRepository:
    """Test PostgreSQL location repository functionality."""

    async def test_save_and_get_location(self, repository, sample_location):
        """Test saving and retrieving a location."""
        # Save location
        await repository.save(sample_location)

        # Retrieve location
        retrieved = await repository.get_by_name(sample_location.name)

        assert retrieved is not None
        assert retrieved.name == sample_location.name
        assert retrieved.kinds == sample_location.kinds
        assert retrieved.protocol == sample_location.protocol
        assert retrieved.path == sample_location.path
        assert retrieved.storage_options == sample_location.storage_options
        assert retrieved.additional_config == sample_location.additional_config
        assert retrieved.is_remote == sample_location.is_remote
        assert retrieved.is_accessible == sample_location.is_accessible

    async def test_save_update_location(self, repository, sample_location):
        """Test updating an existing location."""
        # Save original
        await repository.save(sample_location)

        # Update location
        sample_location.path = "/updated/path"
        sample_location.storage_options["port"] = 2222
        sample_location.additional_config["updated"] = True

        # Save update
        await repository.save(sample_location)

        # Verify update
        retrieved = await repository.get_by_name(sample_location.name)
        assert retrieved.path == "/updated/path"
        assert retrieved.storage_options["port"] == 2222
        assert retrieved.additional_config["updated"] is True

    async def test_get_nonexistent_location(self, repository):
        """Test getting a location that doesn't exist."""
        result = await repository.get_by_name("nonexistent")
        assert result is None

    async def test_list_all_locations(self, repository):
        """Test listing all locations."""
        # Create multiple locations
        locations = [
            LocationEntity(name="loc1", kinds=[LocationKind.DISK], protocol="file"),
            LocationEntity(name="loc2", kinds=[LocationKind.COMPUTE], protocol="ssh"),
            LocationEntity(name="loc3", kinds=[LocationKind.TAPE], protocol="file"),
        ]

        # Save all
        for loc in locations:
            await repository.save(loc)

        # List all
        all_locs = await repository.list_all()
        loc_names = {loc.name for loc in all_locs}

        # Should include all our locations (and possibly others from other tests)
        assert {"loc1", "loc2", "loc3"}.issubset(loc_names)

    async def test_delete_location(self, repository, sample_location):
        """Test deleting a location."""
        # Save location
        await repository.save(sample_location)

        # Verify it exists
        assert await repository.exists(sample_location.name)

        # Delete location
        deleted = await repository.delete(sample_location.name)
        assert deleted is True

        # Verify it's gone
        assert not await repository.exists(sample_location.name)
        assert await repository.get_by_name(sample_location.name) is None

    async def test_delete_nonexistent_location(self, repository):
        """Test deleting a location that doesn't exist."""
        deleted = await repository.delete("nonexistent")
        assert deleted is False

    async def test_exists_location(self, repository, sample_location):
        """Test checking if location exists."""
        # Should not exist initially
        assert not await repository.exists(sample_location.name)

        # Save location
        await repository.save(sample_location)

        # Should exist now
        assert await repository.exists(sample_location.name)

    async def test_find_by_kind(self, repository):
        """Test finding locations by kind."""
        # Create locations with different kinds
        locations = [
            LocationEntity(name="compute1", kinds=[LocationKind.COMPUTE], protocol="ssh"),
            LocationEntity(name="compute2", kinds=[LocationKind.COMPUTE, LocationKind.DISK], protocol="sftp"),
            LocationEntity(name="disk1", kinds=[LocationKind.DISK], protocol="file"),
            LocationEntity(name="tape1", kinds=[LocationKind.TAPE], protocol="file"),
        ]

        for loc in locations:
            await repository.save(loc)

        # Find compute locations
        compute_locs = await repository.find_by_kind(LocationKind.COMPUTE)
        compute_names = {loc.name for loc in compute_locs}
        assert {"compute1", "compute2"}.issubset(compute_names)

        # Find disk locations
        disk_locs = await repository.find_by_kind(LocationKind.DISK)
        disk_names = {loc.name for loc in disk_locs}
        assert {"compute2", "disk1"}.issubset(disk_names)

        # Find tape locations
        tape_locs = await repository.find_by_kind(LocationKind.TAPE)
        tape_names = {loc.name for loc in tape_locs}
        assert "tape1" in tape_names

    async def test_find_by_protocol(self, repository):
        """Test finding locations by protocol."""
        # Create locations with different protocols
        locations = [
            LocationEntity(name="ssh1", kinds=[LocationKind.COMPUTE], protocol="ssh"),
            LocationEntity(name="ssh2", kinds=[LocationKind.COMPUTE], protocol="ssh"),
            LocationEntity(name="sftp1", kinds=[LocationKind.DISK], protocol="sftp"),
            LocationEntity(name="file1", kinds=[LocationKind.DISK], protocol="file"),
        ]

        for loc in locations:
            await repository.save(loc)

        # Find SSH locations
        ssh_locs = await repository.find_by_protocol("ssh")
        ssh_names = {loc.name for loc in ssh_locs}
        assert {"ssh1", "ssh2"}.issubset(ssh_names)

        # Find SFTP locations
        sftp_locs = await repository.find_by_protocol("sftp")
        sftp_names = {loc.name for loc in sftp_locs}
        assert "sftp1" in sftp_names

        # Find file locations
        file_locs = await repository.find_by_protocol("file")
        file_names = {loc.name for loc in file_locs}
        assert "file1" in file_names

    async def test_count_locations(self, repository):
        """Test counting locations."""
        initial_count = await repository.count()

        # Add locations
        locs = [
            LocationEntity(name=f"count_loc_{i}", kinds=[LocationKind.DISK], protocol="file")
            for i in range(3)
        ]

        for loc in locs:
            await repository.save(loc)

        final_count = await repository.count()
        assert final_count >= initial_count + 3

    async def test_path_templates_persistence(self, repository):
        """Test that path templates are properly saved and retrieved."""
        templates = [
            PathTemplate(
                name="template1",
                pattern="{model}/{experiment}",
                description="Model experiment path",
                required_attributes=["model", "experiment"]
            ),
            PathTemplate(
                name="template2",
                pattern="{year}/{month}/{day}",
                description="Date-based path",
                required_attributes=["year", "month", "day"]
            )
        ]

        loc = LocationEntity(
            name="template_test_loc",
            kinds=[LocationKind.DISK],
            protocol="file",
            path_templates=templates
        )

        await repository.save(loc)
        retrieved = await repository.get_by_name(loc.name)

        assert len(retrieved.path_templates) == 2

        # Check templates by name
        retrieved_templates = {t.name: t for t in retrieved.path_templates}

        assert "template1" in retrieved_templates
        assert retrieved_templates["template1"].pattern == "{model}/{experiment}"
        assert retrieved_templates["template1"].required_attributes == ["model", "experiment"]

        assert "template2" in retrieved_templates
        assert retrieved_templates["template2"].pattern == "{year}/{month}/{day}"
        assert retrieved_templates["template2"].required_attributes == ["year", "month", "day"]

    async def test_last_verified_timestamp(self, repository):
        """Test that last_verified timestamps are preserved."""
        now = datetime.now()

        loc = LocationEntity(
            name="timestamp_test_loc",
            kinds=[LocationKind.DISK],
            protocol="file",
            last_verified=now
        )

        await repository.save(loc)
        retrieved = await repository.get_by_name(loc.name)

        # Note: Database might have slightly different precision
        assert retrieved.last_verified is not None
        assert abs((retrieved.last_verified - now).total_seconds()) < 1

    async def test_complex_storage_options(self, repository):
        """Test that complex storage options are preserved."""
        complex_options = {
            "host": "example.com",
            "port": 2222,
            "credentials": {
                "username": "testuser",
                "key_file": "/path/to/key"
            },
            "advanced": {
                "timeout": 30,
                "retry_count": 3,
                "features": ["compression", "keep_alive"]
            }
        }

        loc = LocationEntity(
            name="complex_options_loc",
            kinds=[LocationKind.COMPUTE],
            protocol="sftp",
            storage_options=complex_options
        )

        await repository.save(loc)
        retrieved = await repository.get_by_name(loc.name)

        assert retrieved.storage_options == complex_options

    async def test_concurrent_operations(self, repository):
        """Test concurrent repository operations."""
        async def save_location(loc_name: str):
            loc = LocationEntity(
                name=loc_name,
                kinds=[LocationKind.DISK],
                protocol="file"
            )
            await repository.save(loc)
            return await repository.get_by_name(loc_name)

        # Run multiple operations concurrently
        tasks = [
            save_location(f"concurrent_loc_{i}")
            for i in range(10)
        ]

        results = await asyncio.gather(*tasks)

        # All should succeed
        assert len(results) == 10
        assert all(result is not None for result in results)
        assert len(set(result.name for result in results)) == 10


@pytest.mark.asyncio
class TestPostgresLocationRepositoryErrorHandling:
    """Test error handling in PostgreSQL location repository."""

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

        repository = PostgresLocationRepository()
        # This should raise a connection error when trying to operate
        with pytest.raises(RepositoryError):
            await repository.get_by_name("test_location")