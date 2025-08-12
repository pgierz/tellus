"""
Integration tests for workflow execution system.
"""

import asyncio
import pytest
from pathlib import Path
from unittest.mock import Mock, patch, AsyncMock

from src.tellus.domain.entities.workflow import (
    WorkflowEntity, WorkflowStep, WorkflowType, WorkflowStatus, 
    WorkflowRunEntity, RunStatus, ResourceRequirement
)
from src.tellus.application.services.workflow_execution_service import WorkflowExecutionService
from src.tellus.infrastructure.adapters.workflow_engines import PythonWorkflowEngine
from src.tellus.infrastructure.repositories.json_workflow_repository import JsonWorkflowRepository


@pytest.fixture
def temp_workflow_files(tmp_path):
    """Create temporary files for workflow persistence."""
    return {
        'workflows': tmp_path / "workflows.json",
        'runs': tmp_path / "workflow_runs.json",
        'templates': tmp_path / "workflow_templates.json"
    }


@pytest.fixture
def workflow_repository(temp_workflow_files):
    """Create workflow repository for testing."""
    return JsonWorkflowRepository(
        workflows_file=temp_workflow_files['workflows'],
        runs_file=temp_workflow_files['runs'],
        templates_file=temp_workflow_files['templates']
    )


@pytest.fixture
def python_engine():
    """Create Python workflow engine for testing."""
    return PythonWorkflowEngine()


@pytest.fixture
def execution_service(workflow_repository, python_engine):
    """Create workflow execution service."""
    return WorkflowExecutionService(
        workflow_repository=workflow_repository,
        engines={'python': python_engine}
    )


@pytest.fixture
def sample_workflow():
    """Create a sample workflow for testing."""
    step1 = WorkflowStep(
        step_id="step1",
        name="First Step",
        command="print('Starting workflow')",
        dependencies=[]
    )
    
    step2 = WorkflowStep(
        step_id="step2", 
        name="Second Step",
        command="print('Processing data')",
        dependencies=["step1"]
    )
    
    step3 = WorkflowStep(
        step_id="step3",
        name="Final Step",
        command="print('Workflow complete')",
        dependencies=["step2"]
    )
    
    return WorkflowEntity(
        workflow_id="test-integration-workflow",
        name="Integration Test Workflow",
        workflow_type=WorkflowType.DATA_PREPROCESSING,
        steps=[step1, step2, step3]
    )


class TestWorkflowExecutionIntegration:
    """Integration tests for workflow execution."""
    
    @pytest.mark.asyncio
    async def test_complete_workflow_execution_lifecycle(self, execution_service, sample_workflow):
        """Test complete workflow execution from start to finish."""
        # Save workflow to repository
        await execution_service.workflow_repository.save_workflow(sample_workflow)
        
        # Start workflow execution
        run_id = await execution_service.start_workflow_execution(
            workflow_id="test-integration-workflow",
            engine_type="python",
            parameters={"test_param": "test_value"}
        )
        
        assert run_id is not None
        
        # Check initial run status
        run_status = await execution_service.get_execution_status(run_id)
        assert run_status is not None
        assert run_status.run_id == run_id
        assert run_status.status in [RunStatus.QUEUED, RunStatus.RUNNING]
        
        # Wait for execution to complete (with timeout)
        max_wait = 10  # seconds
        wait_interval = 0.1
        elapsed = 0
        
        while elapsed < max_wait:
            run_status = await execution_service.get_execution_status(run_id)
            if run_status.status in [RunStatus.COMPLETED, RunStatus.FAILED, RunStatus.CANCELLED]:
                break
            await asyncio.sleep(wait_interval)
            elapsed += wait_interval
        
        # Verify final status
        final_status = await execution_service.get_execution_status(run_id)
        assert final_status.status == RunStatus.COMPLETED
        assert final_status.progress == 100.0
        assert final_status.end_time is not None
        assert len(final_status.step_results) == 3  # All steps completed
    
    @pytest.mark.asyncio
    async def test_workflow_execution_with_resource_monitoring(self, execution_service):
        """Test workflow execution with resource monitoring."""
        # Create workflow with resource requirements
        requirements = ResourceRequirement(
            cpu_cores=2,
            memory_gb=4,
            disk_space_gb=10
        )
        
        step = WorkflowStep(
            step_id="resource-step",
            name="Resource Monitored Step",
            command="import time; import psutil; time.sleep(0.1); print(f'CPU: {psutil.cpu_percent()}')",
            dependencies=[],
            resource_requirements=requirements
        )
        
        workflow = WorkflowEntity(
            workflow_id="resource-workflow",
            name="Resource Monitoring Workflow",
            workflow_type=WorkflowType.MODEL_EXECUTION,
            steps=[step]
        )
        
        await execution_service.workflow_repository.save_workflow(workflow)
        
        # Execute workflow
        run_id = await execution_service.start_workflow_execution(
            workflow_id="resource-workflow",
            engine_type="python"
        )
        
        # Wait for completion
        await asyncio.sleep(1)
        
        # Check resource usage was monitored
        run_status = await execution_service.get_execution_status(run_id)
        assert run_status.resource_usage is not None
        assert 'cpu_usage_percent' in run_status.resource_usage
        assert 'memory_usage_mb' in run_status.resource_usage
    
    @pytest.mark.asyncio
    async def test_workflow_execution_failure_handling(self, execution_service):
        """Test workflow execution failure handling and recovery."""
        # Create workflow with a failing step
        failing_step = WorkflowStep(
            step_id="failing-step",
            name="Failing Step",
            command="raise Exception('Intentional test failure')",
            dependencies=[]
        )
        
        recovery_step = WorkflowStep(
            step_id="recovery-step",
            name="Recovery Step",
            command="print('This should not execute')",
            dependencies=["failing-step"]
        )
        
        workflow = WorkflowEntity(
            workflow_id="failing-workflow",
            name="Failing Workflow",
            workflow_type=WorkflowType.DATA_PREPROCESSING,
            steps=[failing_step, recovery_step]
        )
        
        await execution_service.workflow_repository.save_workflow(workflow)
        
        # Execute workflow
        run_id = await execution_service.start_workflow_execution(
            workflow_id="failing-workflow",
            engine_type="python"
        )
        
        # Wait for failure
        await asyncio.sleep(1)
        
        # Check failure was handled properly
        run_status = await execution_service.get_execution_status(run_id)
        assert run_status.status == RunStatus.FAILED
        assert run_status.error_message is not None
        assert "Intentional test failure" in run_status.error_message
        
        # Verify only the failing step was executed
        executed_steps = [result['step_id'] for result in run_status.step_results]
        assert "failing-step" in executed_steps
        assert "recovery-step" not in executed_steps
    
    @pytest.mark.asyncio
    async def test_workflow_cancellation(self, execution_service):
        """Test workflow cancellation during execution."""
        # Create workflow with a long-running step
        long_step = WorkflowStep(
            step_id="long-step",
            name="Long Running Step",
            command="import time; time.sleep(5); print('Should be cancelled')",
            dependencies=[]
        )
        
        workflow = WorkflowEntity(
            workflow_id="long-workflow",
            name="Long Running Workflow",
            workflow_type=WorkflowType.MODEL_EXECUTION,
            steps=[long_step]
        )
        
        await execution_service.workflow_repository.save_workflow(workflow)
        
        # Start execution
        run_id = await execution_service.start_workflow_execution(
            workflow_id="long-workflow",
            engine_type="python"
        )
        
        # Let it start
        await asyncio.sleep(0.5)
        
        # Cancel workflow
        success = await execution_service.cancel_workflow_execution(run_id)
        assert success is True
        
        # Wait a moment for cancellation to take effect
        await asyncio.sleep(0.5)
        
        # Check status
        run_status = await execution_service.get_execution_status(run_id)
        assert run_status.status == RunStatus.CANCELLED
    
    @pytest.mark.asyncio
    async def test_concurrent_workflow_executions(self, execution_service):
        """Test multiple workflows executing concurrently."""
        workflows = []
        
        # Create multiple simple workflows
        for i in range(3):
            step = WorkflowStep(
                step_id=f"step-{i}",
                name=f"Step {i}",
                command=f"import time; time.sleep(0.2); print('Workflow {i} complete')",
                dependencies=[]
            )
            
            workflow = WorkflowEntity(
                workflow_id=f"concurrent-workflow-{i}",
                name=f"Concurrent Workflow {i}",
                workflow_type=WorkflowType.DATA_PREPROCESSING,
                steps=[step]
            )
            
            workflows.append(workflow)
            await execution_service.workflow_repository.save_workflow(workflow)
        
        # Start all workflows concurrently
        run_ids = []
        for workflow in workflows:
            run_id = await execution_service.start_workflow_execution(
                workflow_id=workflow.workflow_id,
                engine_type="python"
            )
            run_ids.append(run_id)
        
        # Wait for all to complete
        await asyncio.sleep(2)
        
        # Check all completed successfully
        for run_id in run_ids:
            run_status = await execution_service.get_execution_status(run_id)
            assert run_status.status == RunStatus.COMPLETED
            assert run_status.progress == 100.0
    
    @pytest.mark.asyncio
    async def test_workflow_persistence_across_service_restart(self, temp_workflow_files, python_engine):
        """Test that workflow runs persist across service restarts."""
        # Create first service instance
        repo1 = JsonWorkflowRepository(
            workflows_file=temp_workflow_files['workflows'],
            runs_file=temp_workflow_files['runs'],
            templates_file=temp_workflow_files['templates']
        )
        service1 = WorkflowExecutionService(
            workflow_repository=repo1,
            engines={'python': python_engine}
        )
        
        # Create and save workflow
        step = WorkflowStep(
            step_id="persist-step",
            name="Persistence Test Step",
            command="print('Testing persistence')",
            dependencies=[]
        )
        
        workflow = WorkflowEntity(
            workflow_id="persistence-workflow",
            name="Persistence Test Workflow",
            workflow_type=WorkflowType.DATA_PREPROCESSING,
            steps=[step]
        )
        
        await service1.workflow_repository.save_workflow(workflow)
        
        # Start execution and let it complete
        run_id = await service1.start_workflow_execution(
            workflow_id="persistence-workflow",
            engine_type="python"
        )
        
        await asyncio.sleep(1)  # Let it complete
        
        # Create second service instance (simulating restart)
        repo2 = JsonWorkflowRepository(
            workflows_file=temp_workflow_files['workflows'],
            runs_file=temp_workflow_files['runs'],
            templates_file=temp_workflow_files['templates']
        )
        service2 = WorkflowExecutionService(
            workflow_repository=repo2,
            engines={'python': python_engine}
        )
        
        # Check that the run is still accessible
        run_status = await service2.get_execution_status(run_id)
        assert run_status is not None
        assert run_status.run_id == run_id
        assert run_status.status == RunStatus.COMPLETED
    
    @pytest.mark.asyncio
    async def test_workflow_progress_tracking(self, execution_service):
        """Test detailed progress tracking during workflow execution."""
        # Create workflow with multiple steps
        steps = []
        for i in range(5):
            step = WorkflowStep(
                step_id=f"step-{i}",
                name=f"Step {i}",
                command=f"import time; time.sleep(0.1); print('Step {i} complete')",
                dependencies=[f"step-{i-1}"] if i > 0 else []
            )
            steps.append(step)
        
        workflow = WorkflowEntity(
            workflow_id="progress-workflow",
            name="Progress Tracking Workflow",
            workflow_type=WorkflowType.DATA_PREPROCESSING,
            steps=steps
        )
        
        await execution_service.workflow_repository.save_workflow(workflow)
        
        # Start execution
        run_id = await execution_service.start_workflow_execution(
            workflow_id="progress-workflow",
            engine_type="python"
        )
        
        # Monitor progress
        progress_history = []
        for _ in range(10):  # Check progress multiple times
            await asyncio.sleep(0.1)
            run_status = await execution_service.get_execution_status(run_id)
            progress_history.append(run_status.progress)
            if run_status.status == RunStatus.COMPLETED:
                break
        
        # Verify progress increased over time
        assert len(progress_history) > 1
        assert progress_history[0] < progress_history[-1]
        assert progress_history[-1] == 100.0
        
        # Verify all steps completed
        final_status = await execution_service.get_execution_status(run_id)
        assert len(final_status.step_results) == 5
    
    @pytest.mark.asyncio
    async def test_workflow_parameter_substitution(self, execution_service):
        """Test parameter substitution in workflow commands."""
        step = WorkflowStep(
            step_id="param-step",
            name="Parameter Step",
            command="print('Input: {input_file}, Output: {output_file}')",
            dependencies=[]
        )
        
        workflow = WorkflowEntity(
            workflow_id="param-workflow",
            name="Parameter Workflow",
            workflow_type=WorkflowType.DATA_PREPROCESSING,
            steps=[step],
            parameters={"input_file": "default.nc", "output_file": "default_out.nc"}
        )
        
        await execution_service.workflow_repository.save_workflow(workflow)
        
        # Execute with custom parameters
        run_id = await execution_service.start_workflow_execution(
            workflow_id="param-workflow",
            engine_type="python",
            parameters={"input_file": "custom.nc", "output_file": "custom_out.nc"}
        )
        
        await asyncio.sleep(1)  # Let it complete
        
        # Check that parameters were substituted
        run_status = await execution_service.get_execution_status(run_id)
        assert run_status.status == RunStatus.COMPLETED
        
        # Parameters should be recorded in the run
        assert run_status.parameters["input_file"] == "custom.nc"
        assert run_status.parameters["output_file"] == "custom_out.nc"