"""
PostgreSQL-based location repository implementation.
"""

from typing import List, Optional
from sqlalchemy import select, delete
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from ...domain.entities.location import LocationEntity, LocationKind
from ...domain.repositories.exceptions import (
    RepositoryError,
    LocationExistsError,
    LocationNotFoundError,
)
from ...domain.repositories.location_repository import ILocationRepository
from ..database.models import LocationModel


class PostgresLocationRepository(ILocationRepository):
    """
    PostgreSQL-based implementation of location repository.

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

    async def _get_session(self) -> AsyncSession:
        """Get async session for database operations."""
        if self._session:
            return self._session

        # This would need to be implemented properly with dependency injection
        raise RuntimeError(
            "No session provided and global session factory not available. "
            "Please provide a session or configure the database manager."
        )

    async def save(self, location: LocationEntity) -> None:
        """Save a location entity to the database."""
        session = await self._get_session()

        try:
            # Check if location already exists
            stmt = select(LocationModel).where(LocationModel.name == location.name)
            existing = await session.execute(stmt)
            existing_loc = existing.scalar_one_or_none()

            if existing_loc:
                # Update existing location
                self._update_model_from_entity(existing_loc, location)
            else:
                # Create new location
                loc_model = self._entity_to_model(location)
                session.add(loc_model)

            if self._owns_session:
                await session.commit()

        except IntegrityError as e:
            if self._owns_session:
                await session.rollback()
            raise LocationExistsError(f"Location '{location.name}' already exists") from e
        except Exception as e:
            if self._owns_session:
                await session.rollback()
            raise RepositoryError(f"Failed to save location '{location.name}': {e}") from e

    async def get_by_name(self, name: str) -> Optional[LocationEntity]:
        """Retrieve a location by its name."""
        session = await self._get_session()

        try:
            stmt = select(LocationModel).where(LocationModel.name == name)
            result = await session.execute(stmt)
            loc_model = result.scalar_one_or_none()

            if not loc_model:
                return None

            return self._model_to_entity(loc_model)

        except Exception as e:
            raise RepositoryError(f"Failed to retrieve location '{name}': {e}") from e

    async def list_all(self) -> List[LocationEntity]:
        """List all locations."""
        session = await self._get_session()

        try:
            stmt = select(LocationModel)
            result = await session.execute(stmt)
            loc_models = result.scalars().all()

            return [self._model_to_entity(model) for model in loc_models]

        except Exception as e:
            raise RepositoryError(f"Failed to list locations: {e}") from e

    async def delete(self, name: str) -> bool:
        """Delete a location by its name."""
        session = await self._get_session()

        try:
            stmt = delete(LocationModel).where(LocationModel.name == name)
            result = await session.execute(stmt)

            if self._owns_session:
                await session.commit()

            return result.rowcount > 0

        except Exception as e:
            if self._owns_session:
                await session.rollback()
            raise RepositoryError(f"Failed to delete location '{name}': {e}") from e

    async def exists(self, name: str) -> bool:
        """Check if a location exists."""
        session = await self._get_session()

        try:
            stmt = select(LocationModel.name).where(LocationModel.name == name)
            result = await session.execute(stmt)
            return result.scalar_one_or_none() is not None

        except Exception as e:
            raise RepositoryError(f"Failed to check location existence '{name}': {e}") from e

    async def find_by_kind(self, kind: LocationKind) -> List[LocationEntity]:
        """Find all locations that have a specific kind."""
        session = await self._get_session()

        try:
            # Use PostgreSQL array operations to find locations with the specified kind
            stmt = select(LocationModel).where(LocationModel.kinds.any(kind.name))
            result = await session.execute(stmt)
            loc_models = result.scalars().all()

            return [self._model_to_entity(model) for model in loc_models]

        except Exception as e:
            raise RepositoryError(f"Failed to find locations by kind '{kind}': {e}") from e

    async def find_by_protocol(self, protocol: str) -> List[LocationEntity]:
        """Find all locations that use a specific protocol."""
        session = await self._get_session()

        try:
            stmt = select(LocationModel).where(LocationModel.protocol == protocol)
            result = await session.execute(stmt)
            loc_models = result.scalars().all()

            return [self._model_to_entity(model) for model in loc_models]

        except Exception as e:
            raise RepositoryError(f"Failed to find locations by protocol '{protocol}': {e}") from e

    async def count(self) -> int:
        """Get the total number of locations."""
        session = await self._get_session()

        try:
            stmt = select(LocationModel)
            result = await session.execute(stmt)
            return len(result.scalars().all())

        except Exception as e:
            raise RepositoryError(f"Failed to count locations: {e}") from e

    def _entity_to_model(self, entity: LocationEntity) -> LocationModel:
        """Convert LocationEntity to LocationModel."""
        # Convert enum kinds to string list
        kind_strings = [kind.name for kind in entity.kinds]

        return LocationModel(
            name=entity.name,
            kinds=kind_strings,
            protocol=entity.protocol,
            path=entity.path,
            storage_options=entity.storage_options,
            additional_config=entity.additional_config,
            is_remote=entity.is_remote,
            is_accessible=entity.is_accessible,
            last_verified=entity.last_verified,
            path_templates=self._path_templates_to_dict(entity.path_templates),
        )

    def _update_model_from_entity(self, model: LocationModel, entity: LocationEntity) -> None:
        """Update existing model with entity data."""
        kind_strings = [kind.name for kind in entity.kinds]

        model.kinds = kind_strings
        model.protocol = entity.protocol
        model.path = entity.path
        model.storage_options = entity.storage_options
        model.additional_config = entity.additional_config
        model.is_remote = entity.is_remote
        model.is_accessible = entity.is_accessible
        model.last_verified = entity.last_verified
        model.path_templates = self._path_templates_to_dict(entity.path_templates)

    def _model_to_entity(self, model: LocationModel) -> LocationEntity:
        """Convert LocationModel to LocationEntity."""
        # Convert string list back to enum kinds
        kinds = [LocationKind.from_str(kind_str) for kind_str in model.kinds]

        return LocationEntity(
            name=model.name,
            kinds=kinds,
            protocol=model.protocol,
            path=model.path,
            storage_options=model.storage_options,
            additional_config=model.additional_config,
            is_remote=model.is_remote,
            is_accessible=model.is_accessible,
            last_verified=model.last_verified,
            path_templates=self._dict_to_path_templates(model.path_templates),
        )

    def _path_templates_to_dict(self, path_templates: List) -> dict:
        """Convert path templates list to dictionary for JSON storage."""
        if not path_templates:
            return {}

        result = {}
        for template in path_templates:
            result[template.name] = {
                "pattern": template.pattern,
                "description": template.description,
                "required_attributes": template.required_attributes,
            }
        return result

    def _dict_to_path_templates(self, templates_dict: dict) -> List:
        """Convert dictionary back to path templates list."""
        if not templates_dict:
            return []

        from ...domain.entities.location import PathTemplate

        templates = []
        for name, data in templates_dict.items():
            template = PathTemplate(
                name=name,
                pattern=data["pattern"],
                description=data["description"],
                required_attributes=data.get("required_attributes", []),
            )
            templates.append(template)
        return templates


class AsyncLocationRepositoryWrapper:
    """
    Wrapper to adapt the async repository to sync interface.

    This allows gradual migration from sync to async patterns.
    """

    def __init__(self, async_repo: PostgresLocationRepository):
        self.async_repo = async_repo

    def save(self, location: LocationEntity) -> None:
        """Sync wrapper for save operation."""
        import asyncio
        asyncio.run(self.async_repo.save(location))

    def get_by_name(self, name: str) -> Optional[LocationEntity]:
        """Sync wrapper for get_by_name operation."""
        import asyncio
        return asyncio.run(self.async_repo.get_by_name(name))

    def list_all(self) -> List[LocationEntity]:
        """Sync wrapper for list_all operation."""
        import asyncio
        return asyncio.run(self.async_repo.list_all())

    def delete(self, name: str) -> bool:
        """Sync wrapper for delete operation."""
        import asyncio
        return asyncio.run(self.async_repo.delete(name))

    def exists(self, name: str) -> bool:
        """Sync wrapper for exists operation."""
        import asyncio
        return asyncio.run(self.async_repo.exists(name))

    def find_by_kind(self, kind: LocationKind) -> List[LocationEntity]:
        """Sync wrapper for find_by_kind operation."""
        import asyncio
        return asyncio.run(self.async_repo.find_by_kind(kind))

    def find_by_protocol(self, protocol: str) -> List[LocationEntity]:
        """Sync wrapper for find_by_protocol operation."""
        import asyncio
        return asyncio.run(self.async_repo.find_by_protocol(protocol))

    def count(self) -> int:
        """Sync wrapper for count operation."""
        import asyncio
        return asyncio.run(self.async_repo.count())