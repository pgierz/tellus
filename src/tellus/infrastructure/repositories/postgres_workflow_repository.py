"""
PostgreSQL-based workflow repository implementation with hybrid storage.

This repository handles WorkflowEntity persistence using a hybrid approach:
- Structured fields for common queries (workflow_system, current_status, etc.)
- JSON storage for complex nested data (fingerprints, steps, configurations)
"""

from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
from sqlalchemy import select, delete
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from ...domain.entities.workflow import (
    WorkflowEntity,
    WorkflowType,
    WorkflowStatus,
    WorkflowStep,
    WorkflowPhase,
    ResourceRequirement,
)
from ...domain.repositories.exceptions import (
    RepositoryError,
)
from ..database.models import WorkflowModel
from ..database.config import get_session


class WorkflowNotFoundError(RepositoryError):
    """Raised when a workflow is not found."""
    pass


class WorkflowExistsError(RepositoryError):
    """Raised when attempting to create a workflow that already exists."""
    pass


class PostgresWorkflowRepository:
    """
    PostgreSQL-based implementation of workflow repository.

    Uses SQLAlchemy async patterns with hybrid storage:
    - Structured columns for queryable fields
    - JSON columns for complex nested data
    - Proper entity-to-model conversion handling Sets, Lists, and custom objects
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

    async def create(self, workflow: WorkflowEntity) -> WorkflowEntity:
        """
        Create a new workflow.

        Args:
            workflow: WorkflowEntity to create

        Returns:
            Created WorkflowEntity

        Raises:
            WorkflowExistsError: If workflow already exists
            RepositoryError: If creation fails
        """
        if self._session:
            return await self._create_with_session(self._session, workflow)
        else:
            db_manager = self._get_db_manager()
            async with db_manager.get_session() as session:
                return await self._create_with_session(session, workflow)

    async def _create_with_session(
        self, session: AsyncSession, workflow: WorkflowEntity
    ) -> WorkflowEntity:
        """Create with provided session."""
        try:
            # Check if workflow already exists
            stmt = select(WorkflowModel).where(
                WorkflowModel.workflow_id == workflow.workflow_id
            )
            existing = await session.execute(stmt)
            if existing.scalar_one_or_none():
                raise WorkflowExistsError(
                    f"Workflow '{workflow.workflow_id}' already exists"
                )

            # Create new workflow
            workflow_model = self._entity_to_model(workflow)
            session.add(workflow_model)

            if self._owns_session:
                await session.commit()
                await session.refresh(workflow_model)

            return self._model_to_entity(workflow_model)

        except WorkflowExistsError:
            if self._owns_session:
                await session.rollback()
            raise
        except Exception as e:
            if self._owns_session:
                await session.rollback()
            raise RepositoryError(
                f"Failed to create workflow '{workflow.workflow_id}': {e}"
            ) from e

    async def get_by_id(self, workflow_id: str) -> Optional[WorkflowEntity]:
        """
        Retrieve a workflow by its ID.

        Args:
            workflow_id: ID of the workflow to retrieve

        Returns:
            WorkflowEntity if found, None otherwise

        Raises:
            RepositoryError: If retrieval fails
        """
        if self._session:
            return await self._get_by_id_with_session(self._session, workflow_id)
        else:
            db_manager = self._get_db_manager()
            async with db_manager.get_session() as session:
                return await self._get_by_id_with_session(session, workflow_id)

    async def _get_by_id_with_session(
        self, session: AsyncSession, workflow_id: str
    ) -> Optional[WorkflowEntity]:
        """Get by ID with provided session."""
        try:
            stmt = select(WorkflowModel).where(
                WorkflowModel.workflow_id == workflow_id
            )
            result = await session.execute(stmt)
            workflow_model = result.scalar_one_or_none()

            if not workflow_model:
                return None

            return self._model_to_entity(workflow_model)

        except Exception as e:
            raise RepositoryError(
                f"Failed to retrieve workflow '{workflow_id}': {e}"
            ) from e

    async def get_by_simulation(self, simulation_id: str) -> List[WorkflowEntity]:
        """
        Get all workflows associated with a simulation.

        Args:
            simulation_id: ID of the simulation

        Returns:
            List of WorkflowEntity objects

        Raises:
            RepositoryError: If retrieval fails
        """
        if self._session:
            return await self._get_by_simulation_with_session(
                self._session, simulation_id
            )
        else:
            db_manager = self._get_db_manager()
            async with db_manager.get_session() as session:
                return await self._get_by_simulation_with_session(session, simulation_id)

    async def _get_by_simulation_with_session(
        self, session: AsyncSession, simulation_id: str
    ) -> List[WorkflowEntity]:
        """Get by simulation with provided session."""
        try:
            stmt = select(WorkflowModel).where(
                WorkflowModel.simulation_id == simulation_id
            )
            result = await session.execute(stmt)
            workflow_models = result.scalars().all()

            return [self._model_to_entity(model) for model in workflow_models]

        except Exception as e:
            raise RepositoryError(
                f"Failed to retrieve workflows for simulation '{simulation_id}': {e}"
            ) from e

    async def update(self, workflow: WorkflowEntity) -> WorkflowEntity:
        """
        Update an existing workflow.

        Args:
            workflow: WorkflowEntity with updated data

        Returns:
            Updated WorkflowEntity

        Raises:
            WorkflowNotFoundError: If workflow doesn't exist
            RepositoryError: If update fails
        """
        if self._session:
            return await self._update_with_session(self._session, workflow)
        else:
            db_manager = self._get_db_manager()
            async with db_manager.get_session() as session:
                return await self._update_with_session(session, workflow)

    async def _update_with_session(
        self, session: AsyncSession, workflow: WorkflowEntity
    ) -> WorkflowEntity:
        """Update with provided session."""
        try:
            stmt = select(WorkflowModel).where(
                WorkflowModel.workflow_id == workflow.workflow_id
            )
            result = await session.execute(stmt)
            existing_model = result.scalar_one_or_none()

            if not existing_model:
                raise WorkflowNotFoundError(
                    f"Workflow '{workflow.workflow_id}' not found"
                )

            # Update model from entity
            self._update_model_from_entity(existing_model, workflow)

            if self._owns_session:
                await session.commit()
                await session.refresh(existing_model)

            return self._model_to_entity(existing_model)

        except WorkflowNotFoundError:
            if self._owns_session:
                await session.rollback()
            raise
        except Exception as e:
            if self._owns_session:
                await session.rollback()
            raise RepositoryError(
                f"Failed to update workflow '{workflow.workflow_id}': {e}"
            ) from e

    async def delete(self, workflow_id: str) -> bool:
        """
        Delete a workflow by its ID.

        Args:
            workflow_id: ID of the workflow to delete

        Returns:
            True if deleted, False if not found

        Raises:
            RepositoryError: If deletion fails
        """
        if self._session:
            return await self._delete_with_session(self._session, workflow_id)
        else:
            db_manager = self._get_db_manager()
            async with db_manager.get_session() as session:
                return await self._delete_with_session(session, workflow_id)

    async def _delete_with_session(
        self, session: AsyncSession, workflow_id: str
    ) -> bool:
        """Delete with provided session."""
        try:
            stmt = delete(WorkflowModel).where(
                WorkflowModel.workflow_id == workflow_id
            )
            result = await session.execute(stmt)

            if self._owns_session:
                await session.commit()

            return result.rowcount > 0

        except Exception as e:
            if self._owns_session:
                await session.rollback()
            raise RepositoryError(
                f"Failed to delete workflow '{workflow_id}': {e}"
            ) from e

    async def list_all(
        self, skip: int = 0, limit: int = 100
    ) -> List[WorkflowEntity]:
        """
        List all workflows with pagination.

        Args:
            skip: Number of records to skip
            limit: Maximum number of records to return

        Returns:
            List of WorkflowEntity objects

        Raises:
            RepositoryError: If listing fails
        """
        if self._session:
            return await self._list_all_with_session(self._session, skip, limit)
        else:
            db_manager = self._get_db_manager()
            async with db_manager.get_session() as session:
                return await self._list_all_with_session(session, skip, limit)

    async def _list_all_with_session(
        self, session: AsyncSession, skip: int, limit: int
    ) -> List[WorkflowEntity]:
        """List all with provided session."""
        try:
            stmt = select(WorkflowModel).offset(skip).limit(limit)
            result = await session.execute(stmt)
            workflow_models = result.scalars().all()

            return [self._model_to_entity(model) for model in workflow_models]

        except Exception as e:
            raise RepositoryError(f"Failed to list workflows: {e}") from e

    async def update_fingerprint(
        self, workflow_id: str, fingerprint: Dict[str, Any]
    ) -> WorkflowEntity:
        """
        Update workflow fingerprint data.

        This is a specialized method for updating just the fingerprint-related
        fields without touching other workflow configuration.

        Args:
            workflow_id: ID of the workflow
            fingerprint: RunFingerprint dictionary to update

        Returns:
            Updated WorkflowEntity

        Raises:
            WorkflowNotFoundError: If workflow doesn't exist
            RepositoryError: If update fails
        """
        if self._session:
            return await self._update_fingerprint_with_session(
                self._session, workflow_id, fingerprint
            )
        else:
            db_manager = self._get_db_manager()
            async with db_manager.get_session() as session:
                return await self._update_fingerprint_with_session(
                    session, workflow_id, fingerprint
                )

    async def _update_fingerprint_with_session(
        self, session: AsyncSession, workflow_id: str, fingerprint: Dict[str, Any]
    ) -> WorkflowEntity:
        """Update fingerprint with provided session."""
        try:
            stmt = select(WorkflowModel).where(
                WorkflowModel.workflow_id == workflow_id
            )
            result = await session.execute(stmt)
            workflow_model = result.scalar_one_or_none()

            if not workflow_model:
                raise WorkflowNotFoundError(f"Workflow '{workflow_id}' not found")

            # Convert to entity, update fingerprint, convert back
            workflow_entity = self._model_to_entity(workflow_model)
            workflow_entity.update_from_fingerprint(fingerprint)
            self._update_model_from_entity(workflow_model, workflow_entity)

            if self._owns_session:
                await session.commit()
                await session.refresh(workflow_model)

            return self._model_to_entity(workflow_model)

        except WorkflowNotFoundError:
            if self._owns_session:
                await session.rollback()
            raise
        except Exception as e:
            if self._owns_session:
                await session.rollback()
            raise RepositoryError(
                f"Failed to update fingerprint for workflow '{workflow_id}': {e}"
            ) from e

    def _entity_to_model(self, entity: WorkflowEntity) -> WorkflowModel:
        """
        Convert WorkflowEntity to WorkflowModel.

        Handles conversion of:
        - Enums to string names
        - Sets to Lists for JSON storage
        - Custom objects (WorkflowStep, WorkflowPhase) to dicts
        - datetime objects to timezone-aware datetimes
        """
        return WorkflowModel(
            workflow_id=entity.workflow_id,
            simulation_id=entity.simulation_id,
            name=entity.name,
            workflow_type=entity.workflow_type.name,
            description=entity.description,
            version=entity.version,
            author=entity.author,
            status=entity.status.name,
            steps=self._steps_to_json(entity.steps),
            tags=list(entity.tags),  # Set -> List
            parameters=entity.parameters,
            workflow_metadata=entity.metadata,  # Map entity.metadata to workflow_metadata column
            simulation_context=entity.simulation_context,
            associated_locations=list(entity.associated_locations),  # Set -> List
            location_contexts=entity.location_contexts,
            input_location_mapping=entity.input_location_mapping,
            output_location_mapping=entity.output_location_mapping,
            workflow_system=entity.workflow_system,
            deployment_path=entity.deployment_path,
            deployment_config=entity.deployment_config,
            polling_enabled=entity.polling_enabled,
            polling_interval_minutes=entity.polling_interval_minutes,
            current_phases=self._phases_to_json(entity.current_phases),
            observed_file_count=entity.observed_file_count,
            latest_observation_time=entity.latest_observation_time,
            current_status=entity.current_status,
            current_fingerprint=entity.current_fingerprint,
            fingerprint_history=entity.fingerprint_history,
            created_at=entity.created_at,
            updated_at=entity.updated_at,
        )

    def _update_model_from_entity(
        self, model: WorkflowModel, entity: WorkflowEntity
    ) -> None:
        """Update existing model with entity data."""
        model.simulation_id = entity.simulation_id
        model.name = entity.name
        model.workflow_type = entity.workflow_type.name
        model.description = entity.description
        model.version = entity.version
        model.author = entity.author
        model.status = entity.status.name
        model.steps = self._steps_to_json(entity.steps)
        model.tags = list(entity.tags)
        model.parameters = entity.parameters
        model.workflow_metadata = entity.metadata  # Map entity.metadata to workflow_metadata column
        model.simulation_context = entity.simulation_context
        model.associated_locations = list(entity.associated_locations)
        model.location_contexts = entity.location_contexts
        model.input_location_mapping = entity.input_location_mapping
        model.output_location_mapping = entity.output_location_mapping
        model.workflow_system = entity.workflow_system
        model.deployment_path = entity.deployment_path
        model.deployment_config = entity.deployment_config
        model.polling_enabled = entity.polling_enabled
        model.polling_interval_minutes = entity.polling_interval_minutes
        model.current_phases = self._phases_to_json(entity.current_phases)
        model.observed_file_count = entity.observed_file_count
        model.latest_observation_time = entity.latest_observation_time
        model.current_status = entity.current_status
        model.current_fingerprint = entity.current_fingerprint
        model.fingerprint_history = entity.fingerprint_history
        model.updated_at = entity.updated_at

    def _model_to_entity(self, model: WorkflowModel) -> WorkflowEntity:
        """
        Convert WorkflowModel to WorkflowEntity.

        Handles conversion of:
        - String names to Enums
        - Lists to Sets where appropriate
        - Dicts to custom objects (WorkflowStep, WorkflowPhase)
        - Timezone handling for datetimes
        """
        return WorkflowEntity(
            workflow_id=model.workflow_id,
            simulation_id=model.simulation_id,
            name=model.name,
            workflow_type=WorkflowType[model.workflow_type],
            description=model.description,
            version=model.version,
            author=model.author,
            status=WorkflowStatus[model.status],
            steps=self._json_to_steps(model.steps),
            tags=set(model.tags),  # List -> Set
            parameters=model.parameters,
            metadata=model.workflow_metadata,  # Map workflow_metadata column to entity.metadata
            simulation_context=model.simulation_context,
            associated_locations=set(model.associated_locations),  # List -> Set
            location_contexts=model.location_contexts,
            input_location_mapping=model.input_location_mapping,
            output_location_mapping=model.output_location_mapping,
            workflow_system=model.workflow_system,
            deployment_path=model.deployment_path,
            deployment_config=model.deployment_config,
            polling_enabled=model.polling_enabled,
            polling_interval_minutes=model.polling_interval_minutes,
            current_phases=self._json_to_phases(model.current_phases),
            observed_file_count=model.observed_file_count,
            latest_observation_time=model.latest_observation_time,
            current_status=model.current_status,
            current_fingerprint=model.current_fingerprint,
            fingerprint_history=model.fingerprint_history,
            created_at=model.created_at,
            updated_at=model.updated_at,
        )

    def _steps_to_json(self, steps: List[WorkflowStep]) -> List[Dict[str, Any]]:
        """Convert WorkflowStep objects to JSON-serializable dicts."""
        result = []
        for step in steps:
            step_dict = {
                "step_id": step.step_id,
                "name": step.name,
                "command": step.command,
                "dependencies": step.dependencies,
                "environment": step.environment,
                "working_directory": step.working_directory,
                "timeout": self._timedelta_to_str(step.timeout),
                "retry_count": step.retry_count,
                "retry_delay": self._timedelta_to_str(step.retry_delay),
                "metadata": step.metadata,
            }

            if step.resource_requirements:
                step_dict["resource_requirements"] = {
                    "cpu_cores": step.resource_requirements.cpu_cores,
                    "memory_gb": step.resource_requirements.memory_gb,
                    "disk_space_gb": step.resource_requirements.disk_space_gb,
                    "gpu_count": step.resource_requirements.gpu_count,
                    "estimated_runtime": self._timedelta_to_str(
                        step.resource_requirements.estimated_runtime
                    ),
                    "special_requirements": step.resource_requirements.special_requirements,
                }
            else:
                step_dict["resource_requirements"] = None

            result.append(step_dict)
        return result

    def _json_to_steps(self, steps_json: List[Dict[str, Any]]) -> List[WorkflowStep]:
        """Convert JSON dicts back to WorkflowStep objects."""
        steps = []
        for step_dict in steps_json:
            resource_req = None
            if step_dict.get("resource_requirements"):
                req_dict = step_dict["resource_requirements"]
                resource_req = ResourceRequirement(
                    cpu_cores=req_dict.get("cpu_cores", 1),
                    memory_gb=req_dict.get("memory_gb", 1.0),
                    disk_space_gb=req_dict.get("disk_space_gb", 1.0),
                    gpu_count=req_dict.get("gpu_count", 0),
                    estimated_runtime=self._str_to_timedelta(
                        req_dict.get("estimated_runtime")
                    ),
                    special_requirements=req_dict.get("special_requirements", {}),
                )

            step = WorkflowStep(
                step_id=step_dict["step_id"],
                name=step_dict["name"],
                command=step_dict["command"],
                dependencies=step_dict.get("dependencies", []),
                resource_requirements=resource_req,
                environment=step_dict.get("environment", {}),
                working_directory=step_dict.get("working_directory"),
                timeout=self._str_to_timedelta(step_dict.get("timeout")),
                retry_count=step_dict.get("retry_count", 0),
                retry_delay=self._str_to_timedelta(
                    step_dict.get("retry_delay", "10s")
                ),
                metadata=step_dict.get("metadata", {}),
            )
            steps.append(step)
        return steps

    def _phases_to_json(self, phases: List[WorkflowPhase]) -> List[Dict[str, Any]]:
        """Convert WorkflowPhase objects to JSON-serializable dicts."""
        result = []
        for phase in phases:
            phase_dict = {
                "phase_id": phase.phase_id,
                "name": phase.name,
                "status": phase.status,
                "start_time": phase.start_time.isoformat() if phase.start_time else None,
                "end_time": phase.end_time.isoformat() if phase.end_time else None,
                "log_files": phase.log_files,
                "metadata": phase.metadata,
            }
            result.append(phase_dict)
        return result

    def _json_to_phases(self, phases_json: List[Dict[str, Any]]) -> List[WorkflowPhase]:
        """Convert JSON dicts back to WorkflowPhase objects."""
        phases = []
        for phase_dict in phases_json:
            phase = WorkflowPhase(
                phase_id=phase_dict["phase_id"],
                name=phase_dict["name"],
                status=phase_dict["status"],
                start_time=self._parse_datetime(phase_dict.get("start_time")),
                end_time=self._parse_datetime(phase_dict.get("end_time")),
                log_files=phase_dict.get("log_files", []),
                metadata=phase_dict.get("metadata", {}),
            )
            phases.append(phase)
        return phases

    def _timedelta_to_str(self, td: Optional[timedelta]) -> Optional[str]:
        """Convert timedelta to string representation."""
        if not td:
            return None
        total_seconds = int(td.total_seconds())
        hours = total_seconds // 3600
        minutes = (total_seconds % 3600) // 60
        seconds = total_seconds % 60

        parts = []
        if hours > 0:
            parts.append(f"{hours}h")
        if minutes > 0:
            parts.append(f"{minutes}m")
        if seconds > 0 or not parts:
            parts.append(f"{seconds}s")

        return "".join(parts)

    def _str_to_timedelta(self, td_str: Optional[str]) -> Optional[timedelta]:
        """Convert string to timedelta."""
        if not td_str:
            return None

        import re
        pattern = r"(?:(\d+)h)?(?:(\d+)m)?(?:(\d+)s)?"
        match = re.match(pattern, td_str.lower())

        if not match:
            return None

        hours = int(match.group(1) or 0)
        minutes = int(match.group(2) or 0)
        seconds = int(match.group(3) or 0)

        return timedelta(hours=hours, minutes=minutes, seconds=seconds)

    def _parse_datetime(self, dt_str: Optional[str]) -> Optional[datetime]:
        """Parse datetime string to datetime object."""
        if not dt_str:
            return None
        try:
            return datetime.fromisoformat(dt_str.replace('Z', '+00:00'))
        except (ValueError, AttributeError):
            return None


class AsyncWorkflowRepositoryWrapper:
    """
    Wrapper to adapt the async repository to sync interface.

    This allows gradual migration from sync to async patterns.
    """

    def __init__(self, async_repo: PostgresWorkflowRepository):
        self.async_repo = async_repo

    def create(self, workflow: WorkflowEntity) -> WorkflowEntity:
        """Sync wrapper for create operation."""
        import asyncio
        return asyncio.run(self.async_repo.create(workflow))

    def get_by_id(self, workflow_id: str) -> Optional[WorkflowEntity]:
        """Sync wrapper for get_by_id operation."""
        import asyncio
        return asyncio.run(self.async_repo.get_by_id(workflow_id))

    def get_by_simulation(self, simulation_id: str) -> List[WorkflowEntity]:
        """Sync wrapper for get_by_simulation operation."""
        import asyncio
        return asyncio.run(self.async_repo.get_by_simulation(simulation_id))

    def update(self, workflow: WorkflowEntity) -> WorkflowEntity:
        """Sync wrapper for update operation."""
        import asyncio
        return asyncio.run(self.async_repo.update(workflow))

    def delete(self, workflow_id: str) -> bool:
        """Sync wrapper for delete operation."""
        import asyncio
        return asyncio.run(self.async_repo.delete(workflow_id))

    def list_all(self, skip: int = 0, limit: int = 100) -> List[WorkflowEntity]:
        """Sync wrapper for list_all operation."""
        import asyncio
        return asyncio.run(self.async_repo.list_all(skip, limit))

    def update_fingerprint(
        self, workflow_id: str, fingerprint: Dict[str, Any]
    ) -> WorkflowEntity:
        """Sync wrapper for update_fingerprint operation."""
        import asyncio
        return asyncio.run(self.async_repo.update_fingerprint(workflow_id, fingerprint))
