"""
Workflow Execution Application Service - Orchestrates workflow execution.

This service coordinates the execution of workflows across different engines,
manages workflow runs, tracks progress, and handles long-running operations
with proper error handling and retry logic.
"""

import asyncio
import logging
import uuid
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional

from ...domain.entities.location import LocationEntity
from ...domain.entities.workflow import (ExecutionEnvironment, WorkflowEngine,
                                         WorkflowEntity, WorkflowRunEntity,
                                         WorkflowStatus)
from ...domain.repositories.location_repository import ILocationRepository
from ...infrastructure.adapters.progress_tracking import ProgressTracker
from ..dtos import (CreateWorkflowRunDto, FilterOptions, PaginationInfo,
                    WorkflowExecutionRequestDto, WorkflowExecutionResultDto,
                    WorkflowProgressDto, WorkflowResourceUsageDto,
                    WorkflowRunDto, WorkflowRunListDto)
from ..exceptions import (BusinessRuleViolationError, EntityNotFoundError,
                          OperationNotAllowedError, ValidationError,
                          WorkflowExecutionError)
from .workflow_service import IWorkflowRepository

logger = logging.getLogger(__name__)


class IWorkflowRunRepository:
    """Interface for workflow run repository operations."""
    
    def save(self, run: WorkflowRunEntity) -> None:
        """Save a workflow run entity."""
        raise NotImplementedError
    
    def get_by_id(self, run_id: str) -> Optional[WorkflowRunEntity]:
        """Get workflow run by ID."""
        raise NotImplementedError
    
    def exists(self, run_id: str) -> bool:
        """Check if workflow run exists."""
        raise NotImplementedError
    
    def delete(self, run_id: str) -> bool:
        """Delete a workflow run."""
        raise NotImplementedError
    
    def list_all(self) -> List[WorkflowRunEntity]:
        """List all workflow runs."""
        raise NotImplementedError
    
    def list_by_workflow(self, workflow_id: str) -> List[WorkflowRunEntity]:
        """List runs for a specific workflow."""
        raise NotImplementedError
    
    def list_by_status(self, status: WorkflowStatus) -> List[WorkflowRunEntity]:
        """List runs by status."""
        raise NotImplementedError


class IWorkflowEngine:
    """Interface for workflow execution engines."""
    
    def execute(
        self,
        workflow: WorkflowEntity,
        run: WorkflowRunEntity,
        progress_callback: Optional[Callable[[str, float, str], None]] = None
    ) -> WorkflowExecutionResultDto:
        """Execute a workflow and return results."""
        raise NotImplementedError
    
    def validate_workflow(self, workflow: WorkflowEntity) -> List[str]:
        """Validate workflow for this engine."""
        raise NotImplementedError
    
    def estimate_resources(self, workflow: WorkflowEntity) -> Dict[str, Any]:
        """Estimate resource requirements."""
        raise NotImplementedError
    
    def cancel_execution(self, run_id: str) -> bool:
        """Cancel a running workflow."""
        raise NotImplementedError
    
    def get_execution_logs(self, run_id: str) -> List[str]:
        """Get execution logs for a run."""
        raise NotImplementedError


class WorkflowExecutionService:
    """
    Application service for workflow execution management.
    
    Coordinates workflow execution across different engines, manages run lifecycle,
    tracks progress and resources, and provides monitoring capabilities.
    """
    
    def __init__(
        self,
        workflow_repository: IWorkflowRepository,
        run_repository: IWorkflowRunRepository,
        location_repository: ILocationRepository,
        workflow_engines: Dict[WorkflowEngine, IWorkflowEngine],
        progress_tracker: ProgressTracker,
        executor: Optional[ThreadPoolExecutor] = None
    ):
        """
        Initialize the workflow execution service.
        
        Args:
            workflow_repository: Repository for workflow access
            run_repository: Repository for workflow run persistence
            location_repository: Repository for location data access
            workflow_engines: Map of workflow engines by type
            progress_tracker: Progress tracking system
            executor: Thread pool for async execution
        """
        self._workflow_repo = workflow_repository
        self._run_repo = run_repository
        self._location_repo = location_repository
        self._engines = workflow_engines
        self._progress_tracker = progress_tracker
        self._executor = executor or ThreadPoolExecutor(max_workers=4)
        self._active_runs: Dict[str, Future] = {}
        self._logger = logger
    
    def submit_workflow_execution(
        self, dto: WorkflowExecutionRequestDto
    ) -> WorkflowRunDto:
        """
        Submit a workflow for execution.
        
        Args:
            dto: Workflow execution request
            
        Returns:
            Created workflow run DTO
            
        Raises:
            EntityNotFoundError: If workflow not found
            ValidationError: If execution parameters invalid
            BusinessRuleViolationError: If execution violates business rules
        """
        self._logger.info(f"Submitting workflow execution: {dto.workflow_id}")
        
        try:
            # Get workflow
            workflow = self._workflow_repo.get_by_id(dto.workflow_id)
            if workflow is None:
                raise EntityNotFoundError("Workflow", dto.workflow_id)
            
            # Generate run ID if not provided
            run_id = dto.run_id or f"{dto.workflow_id}-{uuid.uuid4().hex[:8]}"
            
            # Validate run doesn't already exist
            if self._run_repo.exists(run_id):
                raise ValidationError(f"Workflow run already exists: {run_id}")
            
            # Create workflow run entity
            run = WorkflowRunEntity(
                run_id=run_id,
                workflow_id=dto.workflow_id,
                status=WorkflowStatus.DRAFT,
                execution_environment=ExecutionEnvironment(dto.execution_environment),
                input_parameters=dto.input_parameters.copy(),
                location_context=dto.location_context.copy(),
                max_retries=3  # Default, could come from workflow or request
            )
            
            # Set total steps for progress tracking
            run.set_total_steps(len(workflow.steps))
            
            # Validate execution parameters
            self._validate_execution_request(workflow, run, dto)
            
            # Validate locations
            self._validate_execution_locations(run.location_context)
            
            # Dry run validation if requested
            if dto.dry_run:
                engine = self._get_workflow_engine(workflow.engine)
                validation_errors = engine.validate_workflow(workflow)
                if validation_errors:
                    raise ValidationError(f"Workflow validation failed: {validation_errors}")
                
                # Return without actual execution
                run.status = WorkflowStatus.COMPLETED
                return self._run_entity_to_dto(run)
            
            # Queue for execution
            run.status = WorkflowStatus.QUEUED
            run.submitted_at = datetime.now()
            
            # Persist the run
            self._run_repo.save(run)
            
            # Submit for async execution
            future = self._executor.submit(
                self._execute_workflow_async,
                workflow,
                run,
                dto.priority
            )
            self._active_runs[run_id] = future
            
            self._logger.info(f"Successfully submitted workflow run: {run_id}")
            return self._run_entity_to_dto(run)
            
        except Exception as e:
            self._logger.error(f"Error submitting workflow execution: {str(e)}")
            raise
    
    def get_workflow_run(self, run_id: str) -> WorkflowRunDto:
        """
        Get a workflow run by ID.
        
        Args:
            run_id: The ID of the workflow run
            
        Returns:
            Workflow run DTO
            
        Raises:
            EntityNotFoundError: If run not found
        """
        self._logger.debug(f"Retrieving workflow run: {run_id}")
        
        run = self._run_repo.get_by_id(run_id)
        if run is None:
            raise EntityNotFoundError("WorkflowRun", run_id)
        
        return self._run_entity_to_dto(run)
    
    def list_workflow_runs(
        self,
        workflow_id: Optional[str] = None,
        status: Optional[WorkflowStatus] = None,
        page: int = 1,
        page_size: int = 50,
        filters: Optional[FilterOptions] = None
    ) -> WorkflowRunListDto:
        """
        List workflow runs with pagination and filtering.
        
        Args:
            workflow_id: Optional workflow ID to filter by
            status: Optional status to filter by
            page: Page number (1-based)
            page_size: Number of runs per page
            filters: Optional additional filtering criteria
            
        Returns:
            Paginated list of workflow runs
        """
        self._logger.debug(f"Listing workflow runs (workflow: {workflow_id}, status: {status})")
        
        try:
            # Get runs based on filters
            if workflow_id:
                all_runs = self._run_repo.list_by_workflow(workflow_id)
            elif status:
                all_runs = self._run_repo.list_by_status(status)
            else:
                all_runs = self._run_repo.list_all()
            
            # Apply additional filters if provided
            if filters:
                all_runs = self._apply_run_filters(all_runs, filters)
            
            # Calculate pagination
            total_count = len(all_runs)
            start_idx = (page - 1) * page_size
            end_idx = start_idx + page_size
            
            runs_page = all_runs[start_idx:end_idx]
            
            # Convert to DTOs
            run_dtos = [self._run_entity_to_dto(run) for run in runs_page]
            
            # Create pagination info
            pagination = PaginationInfo(
                page=page,
                page_size=page_size,
                total_count=total_count,
                has_next=end_idx < total_count,
                has_previous=page > 1
            )
            
            return WorkflowRunListDto(
                runs=run_dtos,
                pagination=pagination,
                filters_applied=filters or FilterOptions()
            )
            
        except Exception as e:
            self._logger.error(f"Error listing workflow runs: {str(e)}")
            raise
    
    def cancel_workflow_run(self, run_id: str) -> bool:
        """
        Cancel a running workflow.
        
        Args:
            run_id: The ID of the workflow run to cancel
            
        Returns:
            True if cancellation was successful
            
        Raises:
            EntityNotFoundError: If run not found
            OperationNotAllowedError: If run cannot be cancelled
        """
        self._logger.info(f"Cancelling workflow run: {run_id}")
        
        try:
            # Get workflow run
            run = self._run_repo.get_by_id(run_id)
            if run is None:
                raise EntityNotFoundError("WorkflowRun", run_id)
            
            # Check if run can be cancelled
            if run.is_terminal_status():
                raise OperationNotAllowedError(
                    f"Cannot cancel workflow run in status: {run.status.value}"
                )
            
            # Cancel active execution if running
            if run_id in self._active_runs:
                future = self._active_runs[run_id]
                future.cancel()
                del self._active_runs[run_id]
            
            # Try to cancel via engine
            workflow = self._workflow_repo.get_by_id(run.workflow_id)
            if workflow:
                engine = self._get_workflow_engine(workflow.engine)
                engine.cancel_execution(run_id)
            
            # Update run status
            run.cancel_execution()
            self._run_repo.save(run)
            
            self._logger.info(f"Successfully cancelled workflow run: {run_id}")
            return True
            
        except Exception as e:
            self._logger.error(f"Error cancelling workflow run: {str(e)}")
            raise
    
    def retry_workflow_run(self, run_id: str) -> WorkflowRunDto:
        """
        Retry a failed workflow run.
        
        Args:
            run_id: The ID of the workflow run to retry
            
        Returns:
            Updated workflow run DTO
            
        Raises:
            EntityNotFoundError: If run not found
            OperationNotAllowedError: If run cannot be retried
        """
        self._logger.info(f"Retrying workflow run: {run_id}")
        
        try:
            # Get workflow run
            run = self._run_repo.get_by_id(run_id)
            if run is None:
                raise EntityNotFoundError("WorkflowRun", run_id)
            
            # Check if run can be retried
            if not run.can_retry():
                raise OperationNotAllowedError(
                    f"Cannot retry workflow run: status={run.status.value}, "
                    f"retries={run.retry_count}/{run.max_retries}"
                )
            
            # Get workflow
            workflow = self._workflow_repo.get_by_id(run.workflow_id)
            if workflow is None:
                raise EntityNotFoundError("Workflow", run.workflow_id)
            
            # Retry the run
            run.retry_execution()
            self._run_repo.save(run)
            
            # Submit for async execution
            future = self._executor.submit(
                self._execute_workflow_async,
                workflow,
                run,
                priority=5  # Default priority for retries
            )
            self._active_runs[run_id] = future
            
            self._logger.info(f"Successfully queued retry for workflow run: {run_id}")
            return self._run_entity_to_dto(run)
            
        except Exception as e:
            self._logger.error(f"Error retrying workflow run: {str(e)}")
            raise
    
    def get_workflow_progress(self, run_id: str) -> WorkflowProgressDto:
        """
        Get current progress for a workflow run.
        
        Args:
            run_id: The ID of the workflow run
            
        Returns:
            Current progress information
            
        Raises:
            EntityNotFoundError: If run not found
        """
        run = self._run_repo.get_by_id(run_id)
        if run is None:
            raise EntityNotFoundError("WorkflowRun", run_id)
        
        # Get recent logs from progress tracker
        recent_logs = self._progress_tracker.get_recent_log_entries(run_id, limit=10)
        
        # Estimate completion time
        estimated_completion = None
        if run.get_progress() > 0 and run.status == WorkflowStatus.RUNNING:
            # Simple estimation based on current progress and elapsed time
            if run.started_at:
                elapsed = (datetime.now() - run.started_at).total_seconds()
                if run.get_progress() > 0:
                    total_estimated = elapsed / run.get_progress()
                    remaining = total_estimated - elapsed
                    estimated_completion = (datetime.now().timestamp() + remaining)
        
        return WorkflowProgressDto(
            run_id=run.run_id,
            workflow_id=run.workflow_id,
            status=run.status.value,
            progress=run.get_progress(),
            current_step=run.current_step,
            completed_steps=len(run.completed_steps),
            total_steps=getattr(run, '_total_steps', 0),
            estimated_completion=datetime.fromtimestamp(estimated_completion).isoformat() if estimated_completion else None,
            recent_log_entries=recent_logs
        )
    
    def get_resource_usage(self, run_id: str) -> WorkflowResourceUsageDto:
        """
        Get resource usage for a workflow run.
        
        Args:
            run_id: The ID of the workflow run
            
        Returns:
            Resource usage information
            
        Raises:
            EntityNotFoundError: If run not found
        """
        run = self._run_repo.get_by_id(run_id)
        if run is None:
            raise EntityNotFoundError("WorkflowRun", run_id)
        
        usage = run.resource_usage
        execution_time = run.get_execution_time()
        
        return WorkflowResourceUsageDto(
            run_id=run.run_id,
            cores_used=usage.get("cores_used"),
            memory_gb_used=usage.get("memory_gb_used"),
            disk_gb_used=usage.get("disk_gb_used"),
            gpu_count_used=usage.get("gpu_count_used"),
            wall_time_seconds=execution_time,
            cpu_time_seconds=usage.get("cpu_time_seconds"),
            network_io_gb=usage.get("network_io_gb"),
            custom_metrics=usage.get("custom_metrics", {})
        )
    
    # Private methods
    
    def _execute_workflow_async(
        self, workflow: WorkflowEntity, run: WorkflowRunEntity, priority: int
    ) -> WorkflowExecutionResultDto:
        """
        Execute workflow asynchronously.
        
        This method runs in a separate thread and handles the full execution lifecycle.
        """
        try:
            self._logger.info(f"Starting execution of workflow run: {run.run_id}")
            
            # Update status to running
            run.start_execution()
            self._run_repo.save(run)
            
            # Get execution engine
            engine = self._get_workflow_engine(workflow.engine)
            
            # Create progress callback
            def progress_callback(step_id: str, progress: float, message: str):
                self._update_progress(run, step_id, progress, message)
            
            # Execute workflow
            result = engine.execute(workflow, run, progress_callback)
            
            # Update run with results
            if result.success:
                run.complete_execution()
                run.output_files = result.output_files
                if result.resource_usage:
                    run.resource_usage.update(result.resource_usage)
            else:
                run.fail_execution(result.error_message or "Execution failed")
            
            self._run_repo.save(run)
            
            # Clean up active runs tracking
            if run.run_id in self._active_runs:
                del self._active_runs[run.run_id]
            
            self._logger.info(f"Completed execution of workflow run: {run.run_id} (success: {result.success})")
            return result
            
        except Exception as e:
            self._logger.error(f"Error executing workflow run {run.run_id}: {str(e)}")
            
            # Update run status on error
            run.fail_execution(str(e))
            self._run_repo.save(run)
            
            # Clean up active runs tracking
            if run.run_id in self._active_runs:
                del self._active_runs[run.run_id]
            
            raise WorkflowExecutionError(
                f"Workflow execution failed: {str(e)}",
                run_id=run.run_id,
                cause=e
            )
    
    def _update_progress(
        self, run: WorkflowRunEntity, step_id: str, progress: float, message: str
    ) -> None:
        """Update progress for a workflow run."""
        try:
            run.current_step = step_id
            
            # Log progress
            self._progress_tracker.log_progress(
                run.run_id,
                progress,
                message,
                {"step_id": step_id, "workflow_id": run.workflow_id}
            )
            
            # Save updated run
            self._run_repo.save(run)
            
        except Exception as e:
            self._logger.warning(f"Failed to update progress for run {run.run_id}: {str(e)}")
    
    def _get_workflow_engine(self, engine_type: WorkflowEngine) -> IWorkflowEngine:
        """Get the appropriate workflow engine."""
        if engine_type not in self._engines:
            raise ValidationError(f"Unsupported workflow engine: {engine_type.value}")
        
        return self._engines[engine_type]
    
    def _validate_execution_request(
        self,
        workflow: WorkflowEntity,
        run: WorkflowRunEntity,
        dto: WorkflowExecutionRequestDto
    ) -> None:
        """Validate workflow execution request."""
        # Validate input parameters against workflow schema
        if workflow.input_schema:
            missing_required = []
            for param_name, param_config in workflow.input_schema.items():
                if param_config.get("required", False) and param_name not in dto.input_parameters:
                    missing_required.append(param_name)
            
            if missing_required:
                raise ValidationError(f"Missing required input parameters: {missing_required}")
        
        # Validate workflow can be executed with specified engine
        engine = self._get_workflow_engine(workflow.engine)
        validation_errors = engine.validate_workflow(workflow)
        if validation_errors:
            raise ValidationError(f"Workflow validation failed: {validation_errors}")
    
    def _validate_execution_locations(self, location_context: Dict[str, str]) -> None:
        """Validate that all specified locations exist and are accessible."""
        missing_locations = []
        
        for location_name in location_context.values():
            location = self._location_repo.get_by_name(location_name)
            if location is None:
                missing_locations.append(location_name)
        
        if missing_locations:
            raise EntityNotFoundError(
                "Location(s)",
                ", ".join(missing_locations)
            )
    
    def _apply_run_filters(
        self,
        runs: List[WorkflowRunEntity],
        filters: FilterOptions
    ) -> List[WorkflowRunEntity]:
        """Apply filtering to workflow run list."""
        filtered = runs
        
        if filters.search_term:
            search_term = filters.search_term.lower()
            filtered = [
                run for run in filtered
                if (search_term in run.run_id.lower() or
                    search_term in run.workflow_id.lower() or
                    (run.error_message and search_term in run.error_message.lower()))
            ]
        
        # Date-based filtering would require extending the domain entity
        # with proper timestamp fields
        
        return filtered
    
    def _run_entity_to_dto(self, run: WorkflowRunEntity) -> WorkflowRunDto:
        """Convert workflow run entity to DTO."""
        return WorkflowRunDto(
            run_id=run.run_id,
            uid=run.uid,
            workflow_id=run.workflow_id,
            status=run.status.value,
            execution_environment=run.execution_environment.value,
            input_parameters=run.input_parameters.copy(),
            location_context=run.location_context.copy(),
            submitted_at=run.submitted_at.isoformat() if run.submitted_at else None,
            started_at=run.started_at.isoformat() if run.started_at else None,
            completed_at=run.completed_at.isoformat() if run.completed_at else None,
            execution_time_seconds=run.get_execution_time(),
            current_step=run.current_step,
            completed_steps=run.completed_steps.copy(),
            failed_steps=run.failed_steps.copy(),
            progress=run.get_progress(),
            step_results=run.step_results.copy(),
            error_message=run.error_message,
            retry_count=run.retry_count,
            max_retries=run.max_retries,
            resource_usage=run.resource_usage.copy(),
            output_files=run.output_files.copy(),
            output_locations=run.output_locations.copy()
        )
    
    def shutdown(self) -> None:
        """Shutdown the execution service and clean up resources."""
        self._logger.info("Shutting down workflow execution service")
        
        # Cancel all active runs
        for run_id, future in self._active_runs.items():
            self._logger.info(f"Cancelling active run: {run_id}")
            future.cancel()
        
        # Shutdown thread pool
        self._executor.shutdown(wait=True, timeout=30)
        
        self._logger.info("Workflow execution service shutdown complete")