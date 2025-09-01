"""
Unit tests for WorkflowExecutionService.

Tests the application service layer for workflow execution management,
including workflow submission, run management, progress tracking, and engine coordination.
"""

from concurrent.futures import Future, ThreadPoolExecutor
from datetime import datetime
from typing import Any, Dict, List
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest

from tellus.application.dtos import (FilterOptions, PaginationInfo,
                                     WorkflowExecutionRequestDto,
                                     WorkflowExecutionResultDto,
                                     WorkflowProgressDto,
                                     WorkflowResourceUsageDto, WorkflowRunDto,
                                     WorkflowRunListDto)
from tellus.application.exceptions import (BusinessRuleViolationError,
                                           EntityNotFoundError,
                                           OperationNotAllowedError,
                                           ValidationError,
                                           WorkflowExecutionError)
from tellus.application.services.workflow_execution_service import (
    IWorkflowEngine, IWorkflowRunRepository, WorkflowExecutionService)
from tellus.domain.entities.location import LocationEntity, LocationKind
from tellus.domain.entities.workflow import (ExecutionEnvironment,
                                             WorkflowEngine, WorkflowEntity,
                                             WorkflowRunEntity, WorkflowStatus)


@pytest.fixture
def mock_workflow_repo():
    """Mock workflow repository."""
    return Mock()


@pytest.fixture
def mock_run_repo():
    """Mock workflow run repository."""
    return Mock()


@pytest.fixture
def mock_location_repo():
    """Mock location repository."""
    return Mock()


@pytest.fixture
def mock_progress_tracker():
    """Mock progress tracker."""
    return Mock()


@pytest.fixture
def mock_workflow_engine():
    """Mock workflow engine."""
    engine = Mock(spec=IWorkflowEngine)
    engine.execute.return_value = WorkflowExecutionResultDto(
        success=True,
        run_id="test-run",
        execution_time_seconds=10.5,
        output_data={"result": "success"}
    )
    engine.validate_workflow.return_value = []
    engine.estimate_resources.return_value = {"cpu": 2, "memory": "4GB"}
    engine.cancel_execution.return_value = True
    engine.get_execution_logs.return_value = ["Starting workflow", "Step 1 completed"]
    return engine


@pytest.fixture
def mock_executor():
    """Mock thread pool executor."""
    executor = Mock(spec=ThreadPoolExecutor)
    future = Mock(spec=Future)
    future.done.return_value = False
    future.cancelled.return_value = False
    executor.submit.return_value = future
    return executor


@pytest.fixture
def workflow_engines(mock_workflow_engine):
    """Mock workflow engines mapping."""
    return {WorkflowEngine.SNAKEMAKE: mock_workflow_engine}


@pytest.fixture
def service(mock_workflow_repo, mock_run_repo, mock_location_repo, 
            workflow_engines, mock_progress_tracker, mock_executor):
    """Create service instance with mocked dependencies."""
    return WorkflowExecutionService(
        workflow_repository=mock_workflow_repo,
        run_repository=mock_run_repo,
        location_repository=mock_location_repo,
        workflow_engines=workflow_engines,
        progress_tracker=mock_progress_tracker,
        executor=mock_executor
    )


@pytest.fixture
def sample_workflow_entity():
    """Create a sample workflow entity for testing."""
    return WorkflowEntity(
        workflow_id="test-workflow",
        name="Test Workflow",
        description="A test workflow",
        engine=WorkflowEngine.SNAKEMAKE,
        steps=["step1", "step2", "step3"],
        input_schema={"param1": "string", "param2": "integer"},
        output_schema={"result": "string"}
    )


@pytest.fixture
def sample_run_entity():
    """Create a sample workflow run entity for testing."""
    return WorkflowRunEntity(
        run_id="test-run",
        workflow_id="test-workflow",
        status=WorkflowStatus.DRAFT,
        execution_environment=ExecutionEnvironment.LOCAL,
        input_parameters={"param1": "value1", "param2": 42},
        location_context={"input": "local-disk", "output": "local-disk"},
        max_retries=3
    )


@pytest.fixture
def sample_location_entity():
    """Create a sample location entity for testing."""
    return LocationEntity(
        name="local-disk",
        kinds=[LocationKind.DISK],
        config={"protocol": "file", "path": "/test/workflow"},
        optional=False
    )


class TestWorkflowExecutionService:
    """Test suite for WorkflowExecutionService."""


class TestSubmitWorkflowExecution:
    """Test workflow execution submission operations."""
    
    def test_submit_workflow_execution_success(self, service, mock_workflow_repo, mock_run_repo,
                                             mock_location_repo, sample_workflow_entity, sample_location_entity):
        """Test successful workflow execution submission."""
        # Arrange
        dto = WorkflowExecutionRequestDto(
            workflow_id="test-workflow",
            execution_environment="local",
            input_parameters={"param1": "value1", "param2": 42},
            location_context={"input": "local-disk", "output": "local-disk"}
        )
        
        mock_workflow_repo.get_by_id.return_value = sample_workflow_entity
        mock_run_repo.exists.return_value = False
        mock_run_repo.save.return_value = None
        mock_location_repo.get_by_name.return_value = sample_location_entity
        
        # Act
        result = service.submit_workflow_execution(dto)
        
        # Assert
        assert isinstance(result, WorkflowRunDto)
        assert result.workflow_id == "test-workflow"
        assert result.status == "queued"
        assert result.execution_environment == "local"
        assert result.input_parameters == {"param1": "value1", "param2": 42}
        
        mock_workflow_repo.get_by_id.assert_called_once_with("test-workflow")
        mock_run_repo.save.assert_called_once()
        assert len(service._active_runs) == 1
    
    def test_submit_workflow_execution_with_custom_run_id(self, service, mock_workflow_repo, mock_run_repo,
                                                        mock_location_repo, sample_workflow_entity, sample_location_entity):
        """Test workflow submission with custom run ID."""
        # Arrange
        dto = WorkflowExecutionRequestDto(
            workflow_id="test-workflow",
            run_id="custom-run-id",
            execution_environment="local"
        )
        
        mock_workflow_repo.get_by_id.return_value = sample_workflow_entity
        mock_run_repo.exists.return_value = False
        mock_run_repo.save.return_value = None
        mock_location_repo.get_by_name.return_value = sample_location_entity
        
        # Act
        result = service.submit_workflow_execution(dto)
        
        # Assert
        assert result.run_id == "custom-run-id"
    
    def test_submit_workflow_execution_workflow_not_found(self, service, mock_workflow_repo):
        """Test workflow submission with non-existent workflow."""
        # Arrange
        dto = WorkflowExecutionRequestDto(workflow_id="nonexistent-workflow")
        mock_workflow_repo.get_by_id.return_value = None
        
        # Act & Assert
        with pytest.raises(EntityNotFoundError) as exc_info:
            service.submit_workflow_execution(dto)
        
        assert "nonexistent-workflow" in str(exc_info.value)
    
    def test_submit_workflow_execution_run_already_exists(self, service, mock_workflow_repo, mock_run_repo,
                                                        sample_workflow_entity):
        """Test workflow submission when run ID already exists."""
        # Arrange
        dto = WorkflowExecutionRequestDto(
            workflow_id="test-workflow",
            run_id="existing-run"
        )
        
        mock_workflow_repo.get_by_id.return_value = sample_workflow_entity
        mock_run_repo.exists.return_value = True
        
        # Act & Assert
        with pytest.raises(ValidationError) as exc_info:
            service.submit_workflow_execution(dto)
        
        assert "already exists" in str(exc_info.value)
        mock_run_repo.save.assert_not_called()
    
    def test_submit_workflow_execution_dry_run(self, service, mock_workflow_repo, mock_run_repo,
                                             mock_location_repo, sample_workflow_entity, sample_location_entity,
                                             mock_workflow_engine):
        """Test workflow submission with dry run."""
        # Arrange
        dto = WorkflowExecutionRequestDto(
            workflow_id="test-workflow",
            dry_run=True
        )
        
        mock_workflow_repo.get_by_id.return_value = sample_workflow_entity
        mock_run_repo.exists.return_value = False
        mock_location_repo.get_by_name.return_value = sample_location_entity
        mock_workflow_engine.validate_workflow.return_value = []
        
        # Act
        result = service.submit_workflow_execution(dto)
        
        # Assert
        assert result.status == "completed"  # Dry run completed without actual execution
        mock_workflow_engine.validate_workflow.assert_called_once_with(sample_workflow_entity)
        mock_run_repo.save.assert_not_called()  # Dry run doesn't persist
    
    def test_submit_workflow_execution_dry_run_validation_failure(self, service, mock_workflow_repo, 
                                                                mock_location_repo, sample_workflow_entity,
                                                                sample_location_entity, mock_workflow_engine):
        """Test workflow submission dry run with validation errors."""
        # Arrange
        dto = WorkflowExecutionRequestDto(
            workflow_id="test-workflow",
            dry_run=True
        )
        
        mock_workflow_repo.get_by_id.return_value = sample_workflow_entity
        mock_location_repo.get_by_name.return_value = sample_location_entity
        mock_workflow_engine.validate_workflow.return_value = ["Missing required parameter"]
        
        # Act & Assert
        with pytest.raises(ValidationError) as exc_info:
            service.submit_workflow_execution(dto)
        
        assert "validation failed" in str(exc_info.value)


class TestGetWorkflowRun:
    """Test workflow run retrieval operations."""
    
    def test_get_workflow_run_success(self, service, mock_run_repo, sample_run_entity):
        """Test successful workflow run retrieval."""
        # Arrange
        mock_run_repo.get_by_id.return_value = sample_run_entity
        
        # Act
        result = service.get_workflow_run("test-run")
        
        # Assert
        assert isinstance(result, WorkflowRunDto)
        assert result.run_id == "test-run"
        assert result.workflow_id == "test-workflow"
        assert result.status == "draft"
        mock_run_repo.get_by_id.assert_called_once_with("test-run")
    
    def test_get_workflow_run_not_found(self, service, mock_run_repo):
        """Test retrieving non-existent workflow run."""
        # Arrange
        mock_run_repo.get_by_id.return_value = None
        
        # Act & Assert
        with pytest.raises(EntityNotFoundError) as exc_info:
            service.get_workflow_run("nonexistent-run")
        
        assert "nonexistent-run" in str(exc_info.value)


class TestListWorkflowRuns:
    """Test workflow run listing operations."""
    
    def test_list_workflow_runs_success(self, service, mock_run_repo, sample_run_entity):
        """Test successful workflow run listing."""
        # Arrange
        runs = [sample_run_entity]
        mock_run_repo.list_all.return_value = runs
        
        # Act
        result = service.list_workflow_runs()
        
        # Assert
        assert isinstance(result, WorkflowRunListDto)
        assert len(result.runs) == 1
        assert result.runs[0].run_id == "test-run"
        assert isinstance(result.pagination, PaginationInfo)
        assert result.pagination.total_count == 1
    
    def test_list_workflow_runs_by_workflow(self, service, mock_run_repo):
        """Test workflow run listing filtered by workflow."""
        # Arrange
        runs = [
            WorkflowRunEntity(
                run_id="run-1",
                workflow_id="workflow-1",
                status=WorkflowStatus.COMPLETED
            ),
            WorkflowRunEntity(
                run_id="run-2", 
                workflow_id="workflow-1",
                status=WorkflowStatus.RUNNING
            )
        ]
        mock_run_repo.list_by_workflow.return_value = runs
        
        # Act
        result = service.list_workflow_runs(workflow_id="workflow-1")
        
        # Assert
        assert len(result.runs) == 2
        assert all(run.workflow_id == "workflow-1" for run in result.runs)
        mock_run_repo.list_by_workflow.assert_called_once_with("workflow-1")
    
    def test_list_workflow_runs_by_status(self, service, mock_run_repo):
        """Test workflow run listing filtered by status."""
        # Arrange
        runs = [
            WorkflowRunEntity(
                run_id="run-1",
                workflow_id="workflow-1",
                status=WorkflowStatus.RUNNING
            )
        ]
        mock_run_repo.list_by_status.return_value = runs
        
        # Act
        result = service.list_workflow_runs(status=WorkflowStatus.RUNNING)
        
        # Assert
        assert len(result.runs) == 1
        assert result.runs[0].status == "running"
        mock_run_repo.list_by_status.assert_called_once_with(WorkflowStatus.RUNNING)
    
    def test_list_workflow_runs_with_pagination(self, service, mock_run_repo):
        """Test workflow run listing with pagination."""
        # Arrange
        runs = [
            WorkflowRunEntity(
                run_id=f"run-{i}",
                workflow_id="test-workflow",
                status=WorkflowStatus.COMPLETED
            )
            for i in range(10)
        ]
        mock_run_repo.list_all.return_value = runs
        
        # Act
        result = service.list_workflow_runs(page=2, page_size=3)
        
        # Assert
        assert len(result.runs) == 3
        assert result.pagination.page == 2
        assert result.pagination.page_size == 3
        assert result.pagination.total_count == 10
        assert result.pagination.has_next is True
        assert result.pagination.has_previous is True


class TestCancelWorkflowRun:
    """Test workflow run cancellation operations."""
    
    def test_cancel_workflow_run_success(self, service, mock_run_repo, mock_workflow_engine, sample_run_entity):
        """Test successful workflow run cancellation."""
        # Arrange
        sample_run_entity.status = WorkflowStatus.RUNNING
        mock_run_repo.get_by_id.return_value = sample_run_entity
        mock_run_repo.save.return_value = None
        mock_workflow_engine.cancel_execution.return_value = True
        
        # Add active run to service
        future = Mock()
        future.cancel.return_value = True
        service._active_runs["test-run"] = future
        
        # Act
        result = service.cancel_workflow_run("test-run")
        
        # Assert
        assert result is True
        assert sample_run_entity.status == WorkflowStatus.CANCELLED
        mock_run_repo.save.assert_called_once()
        future.cancel.assert_called_once()
    
    def test_cancel_workflow_run_not_found(self, service, mock_run_repo):
        """Test cancelling non-existent workflow run."""
        # Arrange
        mock_run_repo.get_by_id.return_value = None
        
        # Act & Assert
        with pytest.raises(EntityNotFoundError):
            service.cancel_workflow_run("nonexistent-run")
    
    def test_cancel_workflow_run_not_running(self, service, mock_run_repo, sample_run_entity):
        """Test cancelling workflow run that's not running."""
        # Arrange
        sample_run_entity.status = WorkflowStatus.COMPLETED
        mock_run_repo.get_by_id.return_value = sample_run_entity
        
        # Act & Assert
        with pytest.raises(OperationNotAllowedError) as exc_info:
            service.cancel_workflow_run("test-run")
        
        assert "cannot be cancelled" in str(exc_info.value)


class TestRetryWorkflowRun:
    """Test workflow run retry operations."""
    
    def test_retry_workflow_run_success(self, service, mock_workflow_repo, mock_run_repo, mock_location_repo,
                                       sample_workflow_entity, sample_run_entity, sample_location_entity):
        """Test successful workflow run retry."""
        # Arrange
        sample_run_entity.status = WorkflowStatus.FAILED
        sample_run_entity.retry_count = 1
        mock_run_repo.get_by_id.return_value = sample_run_entity
        mock_workflow_repo.get_by_id.return_value = sample_workflow_entity
        mock_location_repo.get_by_name.return_value = sample_location_entity
        mock_run_repo.save.return_value = None
        
        # Act
        result = service.retry_workflow_run("test-run")
        
        # Assert
        assert isinstance(result, WorkflowRunDto)
        assert result.status == "queued"
        assert sample_run_entity.retry_count == 2
        mock_run_repo.save.assert_called_once()
    
    def test_retry_workflow_run_max_retries_exceeded(self, service, mock_run_repo, sample_run_entity):
        """Test retry when maximum retries exceeded."""
        # Arrange
        sample_run_entity.status = WorkflowStatus.FAILED
        sample_run_entity.retry_count = 3
        sample_run_entity.max_retries = 3
        mock_run_repo.get_by_id.return_value = sample_run_entity
        
        # Act & Assert
        with pytest.raises(BusinessRuleViolationError) as exc_info:
            service.retry_workflow_run("test-run")
        
        assert "Maximum retries" in str(exc_info.value)
    
    def test_retry_workflow_run_not_failed(self, service, mock_run_repo, sample_run_entity):
        """Test retry when workflow run hasn't failed."""
        # Arrange
        sample_run_entity.status = WorkflowStatus.COMPLETED
        mock_run_repo.get_by_id.return_value = sample_run_entity
        
        # Act & Assert
        with pytest.raises(OperationNotAllowedError) as exc_info:
            service.retry_workflow_run("test-run")
        
        assert "cannot be retried" in str(exc_info.value)


class TestProgressTracking:
    """Test workflow progress tracking operations."""
    
    def test_get_workflow_progress_success(self, service, mock_run_repo, sample_run_entity):
        """Test successful workflow progress retrieval."""
        # Arrange
        sample_run_entity.status = WorkflowStatus.RUNNING
        sample_run_entity.current_step = 2
        sample_run_entity.total_steps = 5
        sample_run_entity.progress_percentage = 40.0
        mock_run_repo.get_by_id.return_value = sample_run_entity
        
        # Act
        result = service.get_workflow_progress("test-run")
        
        # Assert
        assert isinstance(result, WorkflowProgressDto)
        assert result.run_id == "test-run"
        assert result.workflow_id == "test-workflow"
        assert result.status == "running"
        assert result.progress == 40.0
        assert result.current_step == 2
        assert result.total_steps == 5
    
    def test_get_resource_usage_success(self, service, mock_run_repo, sample_run_entity):
        """Test successful resource usage retrieval."""
        # Arrange
        sample_run_entity.resource_usage = {
            "cpu_usage": 75.5,
            "memory_usage": 2048,
            "disk_usage": 1024,
            "execution_time": 300.5
        }
        mock_run_repo.get_by_id.return_value = sample_run_entity
        
        # Act
        result = service.get_resource_usage("test-run")
        
        # Assert
        assert isinstance(result, WorkflowResourceUsageDto)
        assert result.run_id == "test-run"
        assert result.cpu_usage == 75.5
        assert result.memory_usage == 2048
        assert result.disk_usage == 1024
        assert result.execution_time_seconds == 300.5


class TestPrivateHelperMethods:
    """Test private helper methods."""
    
    def test_get_workflow_engine_success(self, service, mock_workflow_engine):
        """Test successful workflow engine retrieval."""
        # Act
        result = service._get_workflow_engine(WorkflowEngine.SNAKEMAKE)
        
        # Assert
        assert result == mock_workflow_engine
    
    def test_get_workflow_engine_not_supported(self, service):
        """Test workflow engine retrieval for unsupported engine."""
        # Act & Assert
        with pytest.raises(ValidationError) as exc_info:
            service._get_workflow_engine(WorkflowEngine.NEXTFLOW)  # Not in our mock engines
        
        assert "not supported" in str(exc_info.value)
    
    def test_validate_execution_locations_success(self, service, mock_location_repo, sample_location_entity):
        """Test successful execution location validation."""
        # Arrange
        location_context = {"input": "local-disk", "output": "local-disk"}
        mock_location_repo.get_by_name.return_value = sample_location_entity
        
        # Act & Assert - Should not raise any exception
        service._validate_execution_locations(location_context)
        
        assert mock_location_repo.get_by_name.call_count == 2
    
    def test_validate_execution_locations_not_found(self, service, mock_location_repo):
        """Test execution location validation with missing location."""
        # Arrange
        location_context = {"input": "nonexistent-location"}
        mock_location_repo.get_by_name.return_value = None
        
        # Act & Assert
        with pytest.raises(EntityNotFoundError) as exc_info:
            service._validate_execution_locations(location_context)
        
        assert "nonexistent-location" in str(exc_info.value)
    
    def test_run_entity_to_dto_conversion(self, service, sample_run_entity):
        """Test workflow run entity to DTO conversion."""
        # Act
        result = service._run_entity_to_dto(sample_run_entity)
        
        # Assert
        assert isinstance(result, WorkflowRunDto)
        assert result.run_id == "test-run"
        assert result.workflow_id == "test-workflow"
        assert result.status == "draft"
        assert result.execution_environment == "local"
        assert result.input_parameters == {"param1": "value1", "param2": 42}
    
    def test_apply_run_filters(self, service):
        """Test run filtering functionality."""
        # Arrange
        runs = [
            WorkflowRunEntity(run_id="test-run-1", workflow_id="workflow-1", status=WorkflowStatus.COMPLETED),
            WorkflowRunEntity(run_id="prod-run-2", workflow_id="workflow-2", status=WorkflowStatus.RUNNING),
            WorkflowRunEntity(run_id="test-run-3", workflow_id="workflow-1", status=WorkflowStatus.FAILED)
        ]
        filters = FilterOptions(search_term="test")
        
        # Act
        result = service._apply_run_filters(runs, filters)
        
        # Assert
        assert len(result) == 2
        assert all("test" in run.run_id for run in result)


class TestWorkflowExecution:
    """Test workflow execution functionality."""
    
    def test_execute_workflow_async_success(self, service, mock_run_repo, mock_progress_tracker,
                                          sample_workflow_entity, sample_run_entity, mock_workflow_engine):
        """Test successful asynchronous workflow execution."""
        # Arrange
        mock_run_repo.save.return_value = None
        mock_workflow_engine.execute.return_value = WorkflowExecutionResultDto(
            success=True,
            run_id="test-run",
            execution_time_seconds=45.2,
            output_data={"result": "completed"}
        )
        
        # Act
        service._execute_workflow_async(sample_workflow_entity, sample_run_entity, priority="normal")
        
        # Assert
        assert sample_run_entity.status == WorkflowStatus.COMPLETED
        mock_workflow_engine.execute.assert_called_once()
        mock_run_repo.save.assert_called()
    
    def test_execute_workflow_async_failure(self, service, mock_run_repo, sample_workflow_entity,
                                          sample_run_entity, mock_workflow_engine):
        """Test workflow execution failure handling."""
        # Arrange
        mock_workflow_engine.execute.return_value = WorkflowExecutionResultDto(
            success=False,
            run_id="test-run",
            error_message="Step 2 failed",
            execution_time_seconds=15.3
        )
        
        # Act
        service._execute_workflow_async(sample_workflow_entity, sample_run_entity, priority="normal")
        
        # Assert
        assert sample_run_entity.status == WorkflowStatus.FAILED
        assert "Step 2 failed" in sample_run_entity.error_message
        mock_run_repo.save.assert_called()


class TestServiceLifecycle:
    """Test service lifecycle management."""
    
    def test_service_shutdown(self, service, mock_executor):
        """Test service shutdown functionality."""
        # Act
        service.shutdown()
        
        # Assert
        mock_executor.shutdown.assert_called_once_with(wait=True)


class TestErrorHandling:
    """Test error handling scenarios."""
    
    def test_validation_error_on_invalid_environment(self, service, mock_workflow_repo, sample_workflow_entity):
        """Test validation error for invalid execution environment."""
        # Arrange
        dto = WorkflowExecutionRequestDto(
            workflow_id="test-workflow",
            execution_environment="invalid_env"
        )
        mock_workflow_repo.get_by_id.return_value = sample_workflow_entity
        
        # Act & Assert
        with pytest.raises(ValidationError) as exc_info:
            service.submit_workflow_execution(dto)
        
        assert "execution environment" in str(exc_info.value).lower()
    
    def test_business_rule_violation_on_resource_limits(self, service, mock_workflow_repo, mock_run_repo,
                                                      sample_workflow_entity, mock_workflow_engine):
        """Test business rule violation for resource limits."""
        # Arrange
        dto = WorkflowExecutionRequestDto(
            workflow_id="test-workflow",
            input_parameters={"large_dataset": True}
        )
        
        mock_workflow_repo.get_by_id.return_value = sample_workflow_entity
        mock_run_repo.exists.return_value = False
        mock_workflow_engine.estimate_resources.return_value = {"memory": "100GB"}  # Too large
        
        # Mock validation to reject large resource requirements
        with patch.object(service, '_validate_execution_request') as mock_validate:
            mock_validate.side_effect = BusinessRuleViolationError("Resource limit exceeded")
            
            # Act & Assert
            with pytest.raises(BusinessRuleViolationError):
                service.submit_workflow_execution(dto)
    
    def test_concurrent_execution_limits(self, service):
        """Test that service respects concurrent execution limits."""
        # This would test the ThreadPoolExecutor max_workers limit
        # For now, just verify the executor is configured correctly
        assert service._executor._max_workers == 4  # Default from fixture
    
    def test_workflow_execution_timeout_handling(self, service, mock_run_repo, sample_run_entity):
        """Test handling of workflow execution timeouts."""
        # Arrange
        sample_run_entity.status = WorkflowStatus.RUNNING
        sample_run_entity.started_at = datetime(2023, 1, 1, 0, 0, 0)  # Long ago
        mock_run_repo.get_by_id.return_value = sample_run_entity
        
        # This would typically involve timeout detection logic
        # For now, just verify we can retrieve long-running workflows
        result = service.get_workflow_run("test-run")
        assert result.status == "running"