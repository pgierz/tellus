"""
Service factory for creating database-backed services.

Provides dependency injection and service configuration for the application layer
using PostgreSQL repositories.
"""

from typing import Optional
from sqlalchemy.ext.asyncio import AsyncSession

from ...application.services.simulation_service import SimulationApplicationService
from ...application.services.location_service import LocationApplicationService
from ...domain.repositories.simulation_repository import ISimulationRepository
from ...domain.repositories.location_repository import ILocationRepository

from .config import DatabaseManager, get_database_manager
from ..repositories.postgres_simulation_repository import (
    PostgresSimulationRepository,
    AsyncSimulationRepositoryWrapper
)
from ..repositories.postgres_location_repository import (
    PostgresLocationRepository,
    AsyncLocationRepositoryWrapper
)


class DatabaseServiceFactory:
    """
    Factory for creating application services backed by PostgreSQL repositories.

    This factory handles dependency injection and provides both async and sync
    versions of repositories and services.
    """

    def __init__(self, db_manager: Optional[DatabaseManager] = None):
        """
        Initialize service factory.

        Args:
            db_manager: Optional database manager. If not provided, uses global manager.
        """
        self.db_manager = db_manager or get_database_manager()

    def create_simulation_service(
        self, session: Optional[AsyncSession] = None
    ) -> SimulationApplicationService:
        """
        Create a simulation service with PostgreSQL repository.

        Args:
            session: Optional async session. If not provided, will use per-operation sessions.

        Returns:
            Configured simulation application service
        """
        # Create async repositories
        async_sim_repo = PostgresSimulationRepository(session)
        async_loc_repo = PostgresLocationRepository(session)

        # Wrap in sync adapters for current service interface
        sim_repo = AsyncSimulationRepositoryWrapper(async_sim_repo)
        loc_repo = AsyncLocationRepositoryWrapper(async_loc_repo)

        return SimulationApplicationService(
            simulation_repository=sim_repo,
            location_repository=loc_repo
        )

    def create_location_service(
        self, session: Optional[AsyncSession] = None
    ) -> LocationApplicationService:
        """
        Create a location service with PostgreSQL repository.

        Args:
            session: Optional async session. If not provided, will use per-operation sessions.

        Returns:
            Configured location application service
        """
        # Create async repository
        async_loc_repo = PostgresLocationRepository(session)

        # Wrap in sync adapter for current service interface
        loc_repo = AsyncLocationRepositoryWrapper(async_loc_repo)

        return LocationApplicationService(location_repository=loc_repo)

    async def create_async_simulation_service(
        self, session: Optional[AsyncSession] = None
    ) -> "AsyncSimulationApplicationService":
        """
        Create an async simulation service with PostgreSQL repository.

        Args:
            session: Optional async session. If not provided, will use per-operation sessions.

        Returns:
            Configured async simulation application service
        """
        sim_repo = PostgresSimulationRepository(session)
        loc_repo = PostgresLocationRepository(session)

        # This would require creating an async version of the service
        # For now, this is a placeholder for future async service implementation
        raise NotImplementedError("Async services not yet implemented")

    async def create_async_location_service(
        self, session: Optional[AsyncSession] = None
    ) -> "AsyncLocationApplicationService":
        """
        Create an async location service with PostgreSQL repository.

        Args:
            session: Optional async session. If not provided, will use per-operation sessions.

        Returns:
            Configured async location application service
        """
        loc_repo = PostgresLocationRepository(session)

        # This would require creating an async version of the service
        # For now, this is a placeholder for future async service implementation
        raise NotImplementedError("Async services not yet implemented")


# Global service factory instance
_service_factory: Optional[DatabaseServiceFactory] = None


def get_service_factory(db_manager: Optional[DatabaseManager] = None) -> DatabaseServiceFactory:
    """Get the global service factory instance."""
    global _service_factory

    if _service_factory is None:
        _service_factory = DatabaseServiceFactory(db_manager)

    return _service_factory


def set_service_factory(factory: DatabaseServiceFactory):
    """Set the global service factory instance."""
    global _service_factory
    _service_factory = factory


# Convenience functions for creating services
def create_simulation_service(session: Optional[AsyncSession] = None) -> SimulationApplicationService:
    """Create a simulation service using the global factory."""
    factory = get_service_factory()
    return factory.create_simulation_service(session)


def create_location_service(session: Optional[AsyncSession] = None) -> LocationApplicationService:
    """Create a location service using the global factory."""
    factory = get_service_factory()
    return factory.create_location_service(session)