"""
Fixtures and configuration for repository tests.
"""

import pytest
import asyncio
from testcontainers.postgres import PostgresContainer

from tellus.infrastructure.database.config import DatabaseConfig, DatabaseManager, set_database_manager


@pytest.fixture(scope="session", autouse=True)
def event_loop():
    """Create an instance of the default event loop for the test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="session")
def postgres_container_session():
    """Start a PostgreSQL container for the entire test session."""
    with PostgresContainer("postgres:15") as postgres:
        yield postgres


@pytest.fixture(scope="session", autouse=True)
async def setup_test_database(postgres_container_session):
    """Set up test database configuration for all repository tests."""
    db_url = postgres_container_session.get_connection_url()
    # Convert psycopg2 URL to asyncpg URL
    async_db_url = db_url.replace("postgresql://", "postgresql+asyncpg://")

    config = DatabaseConfig.from_url(async_db_url)
    manager = DatabaseManager(config)

    # Set as global manager for tests
    set_database_manager(manager)

    # Create tables
    await manager.create_tables()

    yield manager

    await manager.close()


@pytest.fixture(autouse=True)
async def cleanup_database_between_tests(setup_test_database):
    """Clean up database between tests."""
    manager = setup_test_database

    # Clean up after each test
    yield

    # Truncate all tables but keep schema
    async with manager.get_session() as session:
        # Delete in order to respect foreign keys
        await session.execute("TRUNCATE simulation_location_contexts CASCADE")
        await session.execute("TRUNCATE simulation_location_associations CASCADE")
        await session.execute("TRUNCATE simulations CASCADE")
        await session.execute("TRUNCATE locations CASCADE")
        await session.commit()


# Add markers for database tests
pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.integration,  # These are integration tests with real database
]