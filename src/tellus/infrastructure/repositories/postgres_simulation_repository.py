"""
PostgreSQL-based simulation repository implementation.
"""

from typing import List, Optional
from sqlalchemy import select, delete
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from ...domain.entities.simulation import SimulationEntity
from ...domain.repositories.exceptions import (
    RepositoryError,
    SimulationExistsError,
    SimulationNotFoundError,
)
from ...domain.repositories.simulation_repository import ISimulationRepository
from ..database.models import SimulationModel, SimulationLocationContextModel
from ..database.config import get_session


class PostgresSimulationRepository(ISimulationRepository):
    """
    PostgreSQL-based implementation of simulation repository.

    Uses SQLAlchemy async patterns with proper transaction management
    and entity-to-model mapping.
    """

    def __init__(self, session: Optional[AsyncSession] = None):
        """
        Initialize repository with optional session.

        Args:
            session: Optional async session. If not provided, will use global session factory.
        """
        self._session = session
        self._owns_session = session is None

    def _get_db_manager(self):
        """Get database manager for session creation."""
        from ..database.config import get_database_manager
        return get_database_manager()

    async def _get_session(self) -> AsyncSession:
        """Get an async database session."""
        if self._session:
            return self._session
        db_manager = self._get_db_manager()
        return db_manager.get_session()

    async def save(self, simulation: SimulationEntity) -> None:
        """Save a simulation entity to the database."""
        if self._session:
            await self._save_with_session(self._session, simulation)
        else:
            db_manager = self._get_db_manager()
            async with db_manager.get_session() as session:
                await self._save_with_session(session, simulation)

    async def _save_with_session(self, session: AsyncSession, simulation: SimulationEntity) -> None:
        """Save with provided session."""
        try:
            # Check if simulation already exists
            stmt = select(SimulationModel).where(
                SimulationModel.simulation_id == simulation.simulation_id
            )
            existing = await session.execute(stmt)
            existing_sim = existing.scalar_one_or_none()

            if existing_sim:
                # Update existing simulation
                self._update_model_from_entity(existing_sim, simulation)
            else:
                # Create new simulation
                sim_model = self._entity_to_model(simulation)
                session.add(sim_model)

            # Handle location contexts separately
            await self._update_location_contexts(session, simulation)

            if self._owns_session:
                await session.commit()

        except IntegrityError as e:
            if self._owns_session:
                await session.rollback()
            raise SimulationExistsError(
                f"Simulation '{simulation.simulation_id}' already exists"
            ) from e
        except Exception as e:
            if self._owns_session:
                await session.rollback()
            raise RepositoryError(f"Failed to save simulation '{simulation.simulation_id}': {e}") from e

    async def get_by_id(self, simulation_id: str) -> Optional[SimulationEntity]:
        """Retrieve a simulation by its ID."""
        if self._session:
            return await self._get_by_id_with_session(self._session, simulation_id)
        else:
            db_manager = self._get_db_manager()
            async with db_manager.get_session() as session:
                return await self._get_by_id_with_session(session, simulation_id)

    async def _get_by_id_with_session(self, session: AsyncSession, simulation_id: str) -> Optional[SimulationEntity]:
        """Get by ID with provided session."""
        try:
            stmt = select(SimulationModel).where(
                SimulationModel.simulation_id == simulation_id
            )
            result = await session.execute(stmt)
            sim_model = result.scalar_one_or_none()

            if not sim_model:
                return None

            # Get location contexts
            location_contexts = await self._get_location_contexts(session, simulation_id)

            return self._model_to_entity(sim_model, location_contexts)

        except Exception as e:
            raise RepositoryError(f"Failed to retrieve simulation '{simulation_id}': {e}") from e

    async def list_all(self) -> List[SimulationEntity]:
        """List all simulations."""
        if self._session:
            return await self._list_all_with_session(self._session)
        else:
            db_manager = self._get_db_manager()
            async with db_manager.get_session() as session:
                return await self._list_all_with_session(session)

    async def _list_all_with_session(self, session: AsyncSession) -> List[SimulationEntity]:
        """List all with provided session."""
        try:
            stmt = select(SimulationModel)
            result = await session.execute(stmt)
            sim_models = result.scalars().all()

            entities = []
            for sim_model in sim_models:
                location_contexts = await self._get_location_contexts(
                    session, sim_model.simulation_id
                )
                entity = self._model_to_entity(sim_model, location_contexts)
                entities.append(entity)

            return entities

        except Exception as e:
            raise RepositoryError(f"Failed to list simulations: {e}") from e

    async def delete(self, simulation_id: str) -> bool:
        """Delete a simulation by its ID."""
        if self._session:
            return await self._delete_with_session(self._session, simulation_id)
        else:
            db_manager = self._get_db_manager()
            async with db_manager.get_session() as session:
                return await self._delete_with_session(session, simulation_id)

    async def _delete_with_session(self, session: AsyncSession, simulation_id: str) -> bool:
        """Delete with provided session."""
        try:
            # First delete location contexts (cascading should handle this, but let's be explicit)
            await session.execute(
                delete(SimulationLocationContextModel).where(
                    SimulationLocationContextModel.simulation_id == simulation_id
                )
            )

            # Delete the simulation
            stmt = delete(SimulationModel).where(
                SimulationModel.simulation_id == simulation_id
            )
            result = await session.execute(stmt)

            if self._owns_session:
                await session.commit()

            return result.rowcount > 0

        except Exception as e:
            if self._owns_session:
                await session.rollback()
            raise RepositoryError(f"Failed to delete simulation '{simulation_id}': {e}") from e

    async def exists(self, simulation_id: str) -> bool:
        """Check if a simulation exists."""
        if self._session:
            return await self._exists_with_session(self._session, simulation_id)
        else:
            db_manager = self._get_db_manager()
            async with db_manager.get_session() as session:
                return await self._exists_with_session(session, simulation_id)

    async def _exists_with_session(self, session: AsyncSession, simulation_id: str) -> bool:
        """Check existence with provided session."""
        try:
            stmt = select(SimulationModel.simulation_id).where(
                SimulationModel.simulation_id == simulation_id
            )
            result = await session.execute(stmt)
            return result.scalar_one_or_none() is not None

        except Exception as e:
            raise RepositoryError(f"Failed to check simulation existence '{simulation_id}': {e}") from e

    async def count(self) -> int:
        """Get the total number of simulations."""
        if self._session:
            return await self._count_with_session(self._session)
        else:
            db_manager = self._get_db_manager()
            async with db_manager.get_session() as session:
                return await self._count_with_session(session)

    async def _count_with_session(self, session: AsyncSession) -> int:
        """Count with provided session."""
        try:
            stmt = select(SimulationModel)
            result = await session.execute(stmt)
            return len(result.scalars().all())

        except Exception as e:
            raise RepositoryError(f"Failed to count simulations: {e}") from e

    def _entity_to_model(self, entity: SimulationEntity) -> SimulationModel:
        """Convert SimulationEntity to SimulationModel."""
        return SimulationModel(
            simulation_id=entity.simulation_id,
            uid=entity.uid,
            model_id=entity.model_id,
            attrs=entity.attributes,  # Use new 'attributes' field
            namelists=entity.attributes.get('namelists', {}),  # Extract from attributes
            workflows=entity.workflows,  # Use new 'workflows' field
        )

    def _update_model_from_entity(self, model: SimulationModel, entity: SimulationEntity) -> None:
        """Update existing model with entity data."""
        model.model_id = entity.model_id
        model.attrs = entity.attributes  # Use new 'attributes' field
        model.namelists = entity.attributes.get('namelists', {})  # Extract from attributes
        model.workflows = entity.workflows  # Use new 'workflows' field

    def _model_to_entity(
        self,
        model: SimulationModel,
        location_contexts: Optional[dict] = None
    ) -> SimulationEntity:
        """Convert SimulationModel to SimulationEntity."""
        if location_contexts is None:
            location_contexts = {}

        # Merge namelists into attributes for the new structure
        attributes = model.attrs.copy() if model.attrs else {}
        if model.namelists:
            attributes['namelists'] = model.namelists

        return SimulationEntity(
            simulation_id=model.simulation_id,
            model_id=model.model_id,
            attributes=attributes,  # Use new 'attributes' field
            workflows=model.workflows,  # Use new 'workflows' field
            associated_locations=set(location_contexts.keys()),
            location_contexts=location_contexts,
        )

    async def _get_location_contexts(
        self, session: AsyncSession, simulation_id: str
    ) -> dict:
        """Get location contexts for a simulation."""
        stmt = select(SimulationLocationContextModel).where(
            SimulationLocationContextModel.simulation_id == simulation_id
        )
        result = await session.execute(stmt)
        contexts = result.scalars().all()

        return {
            ctx.location_name: ctx.context_data
            for ctx in contexts
        }

    async def _update_location_contexts(
        self, session: AsyncSession, entity: SimulationEntity
    ) -> None:
        """Update location contexts for a simulation."""
        # Delete existing contexts
        await session.execute(
            delete(SimulationLocationContextModel).where(
                SimulationLocationContextModel.simulation_id == entity.simulation_id
            )
        )

        # Add new contexts
        for location_name, context_data in entity.location_contexts.items():
            context_model = SimulationLocationContextModel(
                simulation_id=entity.simulation_id,
                location_name=location_name,
                context_data=context_data,
            )
            session.add(context_model)


class AsyncSimulationRepositoryWrapper:
    """
    Wrapper to adapt the async repository to sync interface.

    This allows gradual migration from sync to async patterns.
    Detects if already in an async event loop (like FastAPI) and adapts accordingly.
    """

    def __init__(self, async_repo: PostgresSimulationRepository):
        self.async_repo = async_repo

    def _run_async(self, coro):
        """Run async coroutine, detecting if we're already in an event loop."""
        import asyncio
        try:
            # Check if there's already a running event loop
            loop = asyncio.get_running_loop()
            # We're in an async context (like FastAPI or pytest), run in separate thread
            import concurrent.futures

            def run_in_new_loop(coroutine):
                """Run coroutine in a new event loop in the thread."""
                new_loop = asyncio.new_event_loop()
                asyncio.set_event_loop(new_loop)
                try:
                    return new_loop.run_until_complete(coroutine)
                finally:
                    new_loop.close()
                    asyncio.set_event_loop(None)

            # Run in a separate thread with its own event loop
            with concurrent.futures.ThreadPoolExecutor() as pool:
                future = pool.submit(run_in_new_loop, coro)
                return future.result()
        except RuntimeError:
            # No event loop running, just use asyncio.run() directly
            # Reset the database manager to ensure fresh engine for new event loop
            from ..database.config import reset_database_manager
            reset_database_manager()
            return asyncio.run(coro)

    def save(self, simulation: SimulationEntity) -> None:
        """Sync wrapper for save operation."""
        self._run_async(self.async_repo.save(simulation))

    def get_by_id(self, simulation_id: str) -> Optional[SimulationEntity]:
        """Sync wrapper for get_by_id operation."""
        return self._run_async(self.async_repo.get_by_id(simulation_id))

    def list_all(self) -> List[SimulationEntity]:
        """Sync wrapper for list_all operation."""
        return self._run_async(self.async_repo.list_all())

    def delete(self, simulation_id: str) -> bool:
        """Sync wrapper for delete operation."""
        return self._run_async(self.async_repo.delete(simulation_id))

    def exists(self, simulation_id: str) -> bool:
        """Sync wrapper for exists operation."""
        return self._run_async(self.async_repo.exists(simulation_id))

    def count(self) -> int:
        """Sync wrapper for count operation."""
        return self._run_async(self.async_repo.count())