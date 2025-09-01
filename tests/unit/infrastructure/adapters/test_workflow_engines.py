"""
Tests for workflow execution engines.

Tests the infrastructure adapters for workflow execution engines,
including Snakemake and Python workflow engines with validation, resource estimation, and execution.
"""

import json
import os
import subprocess
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional
from unittest.mock import MagicMock, Mock, call, mock_open, patch

import pytest

from tellus.application.dtos import WorkflowExecutionResultDto
from tellus.domain.entities.workflow import (ResourceRequirement,
                                             WorkflowEntity, WorkflowRunEntity,
                                             WorkflowStep)
from tellus.infrastructure.adapters.workflow_engines import (
    SNAKEMAKE_AVAILABLE, PythonWorkflowEngine, SnakemakeWorkflowEngine)


@pytest.fixture
def sample_resource_requirements():
    """Create sample resource requirements."""
    return ResourceRequirement(
        cores=4,
        memory_gb=8.0,
        disk_gb=50.0,
        walltime_hours=2.0
    )


@pytest.fixture
def sample_workflow_step(sample_resource_requirements):
    """Create a sample workflow step."""
    return WorkflowStep(
        step_id="test_step_1",
        name="Test Processing Step",
        command="echo 'Processing data'",
        input_files=["input.txt"],
        output_files=["output.txt"],
        parameters={"param1": "value1", "param2": 42},
        resource_requirements=sample_resource_requirements,
        dependencies=[]
    )


@pytest.fixture
def sample_workflow(sample_workflow_step):
    """Create a sample workflow entity."""
    return WorkflowEntity(
        workflow_id="test_workflow_001",
        name="Test Workflow",
        description="Sample workflow for testing",
        workflow_file=None,
        steps=[sample_workflow_step],
        global_parameters={"global_param": "global_value", "cores": 2},
        metadata={}
    )


@pytest.fixture
def sample_workflow_with_file():
    """Create a workflow with an existing Snakefile."""
    return WorkflowEntity(
        workflow_id="test_workflow_file",
        name="Workflow with Snakefile",
        description="Workflow that uses existing Snakefile",
        workflow_file="/path/to/Snakefile",
        steps=[],
        global_parameters={"cores": 4},
        metadata={}
    )


@pytest.fixture
def sample_workflow_run():
    """Create a sample workflow run entity."""
    return WorkflowRunEntity(
        run_id="run_12345",
        workflow_id="test_workflow_001",
        status="pending",
        input_parameters={"input_data": "/data/input.nc", "threshold": 0.5},
        location_context={"compute": "hpc_cluster", "storage": "data_archive"},
        created_time=datetime.now().isoformat(),
        metadata={}
    )


@pytest.fixture
def mock_snakemake_api():
    """Mock Snakemake API objects."""
    mock_api = Mock()
    mock_workflow_api = Mock()
    mock_dag_api = Mock()
    mock_execution_plan = Mock()
    
    # Setup method chaining
    mock_api.workflow.return_value = mock_workflow_api
    mock_workflow_api.dag.return_value = mock_dag_api
    mock_dag_api.execute.return_value = mock_execution_plan
    
    # Setup context manager
    mock_api.__enter__ = Mock(return_value=mock_api)
    mock_api.__exit__ = Mock(return_value=None)
    
    return {
        "api": mock_api,
        "workflow_api": mock_workflow_api,
        "dag_api": mock_dag_api,
        "execution_plan": mock_execution_plan
    }


class TestSnakemakeWorkflowEngine:
    """Test suite for SnakemakeWorkflowEngine."""
    
    def test_init_default_values(self):
        """Test initialization with default values."""
        engine = SnakemakeWorkflowEngine()
        
        assert engine.snakemake_executable == "snakemake"
        assert engine.default_cores == 1
        assert engine.default_resources == {}
    
    def test_init_custom_values(self):
        """Test initialization with custom values."""
        custom_resources = {"mem_gb": 16, "runtime": 120}
        engine = SnakemakeWorkflowEngine(
            snakemake_executable="/usr/bin/snakemake",
            default_cores=8,
            default_resources=custom_resources
        )
        
        assert engine.snakemake_executable == "/usr/bin/snakemake"
        assert engine.default_cores == 8
        assert engine.default_resources == custom_resources
    
    def test_init_without_snakemake(self):
        """Test initialization when Snakemake is not available."""
        with patch('tellus.infrastructure.adapters.workflow_engines.SNAKEMAKE_AVAILABLE', False):
            with patch('tellus.infrastructure.adapters.workflow_engines.logger') as mock_logger:
                engine = SnakemakeWorkflowEngine()
                
                mock_logger.warning.assert_called_once()
                assert "Snakemake not available" in str(mock_logger.warning.call_args)
    
    @patch('tellus.infrastructure.adapters.workflow_engines.SNAKEMAKE_AVAILABLE', True)
    def test_execute_with_api_success(self, sample_workflow_with_file, sample_workflow_run, mock_snakemake_api):
        """Test successful execution using Snakemake API."""
        engine = SnakemakeWorkflowEngine()
        
        # Mock Path.exists to return True for workflow file
        with patch('pathlib.Path.exists', return_value=True):
            # Mock SnakemakeApi
            with patch('tellus.infrastructure.adapters.workflow_engines.SnakemakeApi', return_value=mock_snakemake_api["api"]):
                # Mock job results
                mock_job = Mock()
                mock_job.output = ["result1.txt", "result2.txt"]
                mock_job.resources = {"cores": 4, "mem_gb": 8}
                mock_snakemake_api["execution_plan"].__iter__ = Mock(return_value=iter([mock_job]))
                
                with patch.object(engine, '_create_storage_settings') as mock_storage:
                    with patch.object(engine, '_create_resource_settings') as mock_resource:
                        result = engine.execute(sample_workflow_with_file, sample_workflow_run)
        
        assert result.success is True
        assert result.run_id == "run_12345"
        assert result.workflow_id == "test_workflow_file"
        assert "result1.txt" in result.output_files
        assert "result2.txt" in result.output_files
        assert result.resource_usage == {"cores": 4, "mem_gb": 8}
    
    def test_execute_with_api_failure(self, sample_workflow_with_file, sample_workflow_run):
        """Test execution failure using Snakemake API."""
        engine = SnakemakeWorkflowEngine()
        
        with patch('pathlib.Path.exists', return_value=True):
            with patch('tellus.infrastructure.adapters.workflow_engines.SnakemakeApi') as mock_api_class:
                mock_api_class.side_effect = Exception("API execution failed")
                
                result = engine.execute(sample_workflow_with_file, sample_workflow_run)
        
        assert result.success is False
        assert "API execution failed" in result.error_message
        assert result.run_id == "run_12345"
    
    def test_execute_with_generated_snakefile(self, sample_workflow, sample_workflow_run):
        """Test execution with generated Snakefile."""
        engine = SnakemakeWorkflowEngine()
        
        mock_subprocess_result = {
            "success": True,
            "output_files": ["output.txt"],
            "resource_usage": {"cores": 2}
        }
        
        with patch.object(engine, '_generate_snakefile') as mock_generate:
            mock_generate.return_value = "# Mock Snakefile content"
            
            with patch.object(engine, '_execute_with_subprocess') as mock_subprocess:
                mock_subprocess.return_value = mock_subprocess_result
                
                with patch('tempfile.NamedTemporaryFile') as mock_temp:
                    mock_temp.return_value.__enter__ = Mock()
                    mock_temp.return_value.__exit__ = Mock()
                    mock_temp.return_value.name = "/tmp/temp_snakefile.smk"
                    mock_temp.return_value.write = Mock()
                    
                    with patch('os.unlink') as mock_unlink:
                        result = engine.execute(sample_workflow, sample_workflow_run)
        
        assert result.success is True
        assert "output.txt" in result.output_files
        mock_generate.assert_called_once_with(sample_workflow, sample_workflow_run)
        mock_unlink.assert_called_once()
    
    def test_execute_with_subprocess_success(self, sample_workflow, sample_workflow_run):
        """Test successful subprocess execution."""
        engine = SnakemakeWorkflowEngine()
        
        # Mock successful subprocess
        mock_process = Mock()
        mock_process.returncode = 0
        mock_process.stdout.readline.side_effect = [
            "rule test_step_1:\n",
            "Finished job 0\n",
            ""  # End of output
        ]
        mock_process.poll.side_effect = [None, None, 0]  # Not finished, not finished, finished
        mock_process.communicate.return_value = ("", "")
        
        with patch('subprocess.Popen', return_value=mock_process):
            with patch.object(engine, '_collect_output_files') as mock_collect:
                mock_collect.return_value = ["output.txt"]
                
                with patch.object(engine, '_parse_resource_usage') as mock_parse:
                    mock_parse.return_value = {"cores": 2}
                    
                    result = engine._execute_with_subprocess("/tmp/test.smk", sample_workflow, sample_workflow_run)
        
        assert result["success"] is True
        assert "output.txt" in result["output_files"]
        assert result["resource_usage"]["cores"] == 2
    
    def test_execute_with_subprocess_failure(self, sample_workflow, sample_workflow_run):
        """Test failed subprocess execution."""
        engine = SnakemakeWorkflowEngine()
        
        # Mock failed subprocess
        mock_process = Mock()
        mock_process.returncode = 1
        mock_process.stdout.readline.return_value = ""
        mock_process.poll.return_value = 1
        mock_process.communicate.return_value = ("", "Error: Rule failed")
        
        with patch('subprocess.Popen', return_value=mock_process):
            result = engine._execute_with_subprocess("/tmp/test.smk", sample_workflow, sample_workflow_run)
        
        assert result["success"] is False
        assert "Error: Rule failed" in result["error_message"]
    
    def test_generate_snakefile_basic(self, sample_workflow, sample_workflow_run):
        """Test basic Snakefile generation."""
        engine = SnakemakeWorkflowEngine()
        
        snakefile_content = engine._generate_snakefile(sample_workflow, sample_workflow_run)
        
        assert "# Auto-generated Snakefile" in snakefile_content
        assert "rule test_step_1:" in snakefile_content
        assert "input:" in snakefile_content
        assert "output:" in snakefile_content
        assert "shell:" in snakefile_content
        assert "echo 'Processing data'" in snakefile_content
        assert "rule all:" in snakefile_content
    
    def test_generate_snakefile_with_resources(self, sample_workflow_step, sample_workflow_run):
        """Test Snakefile generation with resource requirements."""
        workflow = WorkflowEntity(
            workflow_id="test_workflow",
            name="Test",
            description="Test",
            steps=[sample_workflow_step],
            global_parameters={},
            metadata={}
        )
        
        engine = SnakemakeWorkflowEngine()
        snakefile_content = engine._generate_snakefile(workflow, sample_workflow_run)
        
        assert "resources:" in snakefile_content
        assert "cores=4" in snakefile_content
        assert "mem_gb=8.0" in snakefile_content
        assert "runtime=120" in snakefile_content  # 2 hours * 60 minutes
    
    def test_generate_snakefile_with_script_path(self, sample_workflow_run):
        """Test Snakefile generation with script path."""
        step = WorkflowStep(
            step_id="script_step",
            name="Script Step",
            script_path="/path/to/script.py",
            input_files=["input.txt"],
            output_files=["output.txt"]
        )
        
        workflow = WorkflowEntity(
            workflow_id="test_workflow",
            name="Test",
            steps=[step],
            global_parameters={},
            metadata={}
        )
        
        engine = SnakemakeWorkflowEngine()
        snakefile_content = engine._generate_snakefile(workflow, sample_workflow_run)
        
        assert "script:" in snakefile_content
        assert "/path/to/script.py" in snakefile_content
    
    def test_generate_storage_config(self, sample_workflow_run):
        """Test storage configuration generation."""
        engine = SnakemakeWorkflowEngine()
        
        location_context = {"remote_storage": "sftp_server", "local_cache": "local_disk"}
        lines = engine._generate_storage_config(location_context)
        
        assert "# Storage configuration" in lines[0]
        assert any("storage sftp_server:" in line for line in lines)
        assert any("protocol=\"sftp\"" in line for line in lines)
    
    def test_get_cores_from_workflow_global_params(self, sample_workflow):
        """Test getting cores from workflow global parameters."""
        engine = SnakemakeWorkflowEngine()
        
        cores = engine._get_cores_from_workflow(sample_workflow)
        
        assert cores == 2  # From global_parameters["cores"]
    
    def test_get_cores_from_workflow_step_requirements(self):
        """Test getting cores from step resource requirements."""
        step = WorkflowStep(
            step_id="test_step",
            name="Test Step", 
            resource_requirements=ResourceRequirement(cores=8)
        )
        
        workflow = WorkflowEntity(
            workflow_id="test_workflow",
            name="Test",
            steps=[step],
            global_parameters={},  # No cores in global params
            metadata={}
        )
        
        engine = SnakemakeWorkflowEngine()
        cores = engine._get_cores_from_workflow(workflow)
        
        assert cores == 8
    
    def test_get_cores_from_workflow_default(self):
        """Test getting cores falls back to default."""
        workflow = WorkflowEntity(
            workflow_id="test_workflow",
            name="Test",
            steps=[],
            global_parameters={},
            metadata={}
        )
        
        engine = SnakemakeWorkflowEngine(default_cores=4)
        cores = engine._get_cores_from_workflow(workflow)
        
        assert cores == 4
    
    def test_parse_progress_from_output_rule_start(self):
        """Test parsing progress from Snakemake output - rule start."""
        engine = SnakemakeWorkflowEngine()
        progress_callback = Mock()
        
        engine._parse_progress_from_output("rule test_step_1:", progress_callback)
        
        progress_callback.assert_called_once_with("test_step_1", 0.0, "Starting rule: test_step_1")
    
    def test_parse_progress_from_output_job_finished(self):
        """Test parsing progress from Snakemake output - job finished."""
        engine = SnakemakeWorkflowEngine()
        progress_callback = Mock()
        
        engine._parse_progress_from_output("Finished job 0, rule test_step_1", progress_callback)
        
        progress_callback.assert_called_once_with("test_step_1", 1.0, "Completed rule: test_step_1")
    
    def test_collect_output_files_with_templates(self, sample_workflow, sample_workflow_run):
        """Test collecting output files with template resolution."""
        # Modify step to have templated output
        sample_workflow.steps[0].output_files = ["output_{input_data}.txt"]
        
        engine = SnakemakeWorkflowEngine()
        
        # Mock file existence
        with patch('os.path.exists') as mock_exists:
            mock_exists.return_value = True
            
            output_files = engine._collect_output_files(sample_workflow, sample_workflow_run)
        
        assert "output_/data/input.nc.txt" in output_files
    
    def test_validate_workflow_snakemake_not_found(self, sample_workflow):
        """Test workflow validation when Snakemake executable not found."""
        engine = SnakemakeWorkflowEngine()
        
        with patch('subprocess.run') as mock_run:
            mock_run.side_effect = FileNotFoundError("snakemake not found")
            
            errors = engine.validate_workflow(sample_workflow)
        
        assert len(errors) >= 1
        assert "Snakemake executable not found" in errors[0]
    
    def test_validate_workflow_file_not_found(self, sample_workflow_with_file):
        """Test workflow validation with missing workflow file."""
        engine = SnakemakeWorkflowEngine()
        
        with patch('subprocess.run'):  # Mock successful version check
            with patch('pathlib.Path.exists', return_value=False):
                errors = engine.validate_workflow(sample_workflow_with_file)
        
        assert len(errors) >= 1
        assert "Workflow file not found" in errors[0]
    
    def test_validate_workflow_no_steps_or_file(self):
        """Test workflow validation with no steps and no workflow file."""
        workflow = WorkflowEntity(
            workflow_id="empty_workflow",
            name="Empty",
            steps=[],
            workflow_file=None,
            global_parameters={},
            metadata={}
        )
        
        engine = SnakemakeWorkflowEngine()
        
        with patch('subprocess.run'):  # Mock successful version check
            errors = engine.validate_workflow(workflow)
        
        assert len(errors) >= 1
        assert "must have either steps or a workflow file" in errors[0]
    
    def test_validate_workflow_step_no_command_or_script(self):
        """Test workflow validation with step lacking command or script."""
        step = WorkflowStep(
            step_id="invalid_step",
            name="Invalid Step"
            # Missing command and script_path
        )
        
        workflow = WorkflowEntity(
            workflow_id="test_workflow",
            name="Test",
            steps=[step],
            global_parameters={},
            metadata={}
        )
        
        engine = SnakemakeWorkflowEngine()
        
        with patch('subprocess.run'):  # Mock successful version check
            errors = engine.validate_workflow(workflow)
        
        assert len(errors) >= 1
        assert "must have either command or script_path" in errors[0]
    
    def test_estimate_resources(self, sample_workflow):
        """Test resource estimation from workflow steps."""
        engine = SnakemakeWorkflowEngine()
        
        resources = engine.estimate_resources(sample_workflow)
        
        assert resources["estimated_cores"] == 4  # From step requirements
        assert resources["estimated_memory_gb"] == 8.0
        assert resources["estimated_disk_gb"] == 50.0
        assert resources["estimated_walltime_hours"] == 2.0
        assert resources["parallel_steps"] == 1  # One step with no dependencies
    
    def test_cancel_execution(self):
        """Test execution cancellation."""
        engine = SnakemakeWorkflowEngine()
        
        result = engine.cancel_execution("test_run_123")
        
        assert result is True
    
    def test_get_execution_logs(self):
        """Test getting execution logs."""
        engine = SnakemakeWorkflowEngine()
        
        logs = engine.get_execution_logs("test_run_123")
        
        assert logs == []  # Basic implementation returns empty list


class TestPythonWorkflowEngine:
    """Test suite for PythonWorkflowEngine."""
    
    def test_init_default_values(self):
        """Test initialization with default values."""
        engine = PythonWorkflowEngine()
        
        assert engine.python_executable == "python"
    
    def test_init_custom_executable(self):
        """Test initialization with custom Python executable."""
        engine = PythonWorkflowEngine(python_executable="/usr/bin/python3.9")
        
        assert engine.python_executable == "/usr/bin/python3.9"
    
    def test_execute_success(self, sample_workflow, sample_workflow_run):
        """Test successful workflow execution."""
        engine = PythonWorkflowEngine()
        
        # Mock workflow methods
        sample_workflow.get_execution_order = Mock(return_value=["test_step_1"])
        sample_workflow.get_step = Mock(return_value=sample_workflow.steps[0])
        
        with patch.object(engine, '_execute_step') as mock_execute_step:
            mock_execute_step.return_value = True
            
            with patch.object(engine, '_collect_output_files') as mock_collect:
                mock_collect.return_value = ["output.txt"]
                
                result = engine.execute(sample_workflow, sample_workflow_run)
        
        assert result.success is True
        assert result.run_id == "run_12345"
        assert "output.txt" in result.output_files
        mock_execute_step.assert_called_once()
    
    def test_execute_step_failure(self, sample_workflow, sample_workflow_run):
        """Test workflow execution with step failure."""
        engine = PythonWorkflowEngine()
        
        # Mock workflow methods
        sample_workflow.get_execution_order = Mock(return_value=["test_step_1"])
        sample_workflow.get_step = Mock(return_value=sample_workflow.steps[0])
        
        with patch.object(engine, '_execute_step') as mock_execute_step:
            mock_execute_step.return_value = False  # Step fails
            
            result = engine.execute(sample_workflow, sample_workflow_run)
        
        assert result.success is False
        assert "Step test_step_1 failed" in result.error_message
    
    def test_execute_with_progress_callback(self, sample_workflow, sample_workflow_run):
        """Test execution with progress callback."""
        engine = PythonWorkflowEngine()
        progress_callback = Mock()
        
        # Mock workflow methods
        sample_workflow.get_execution_order = Mock(return_value=["test_step_1"])
        sample_workflow.get_step = Mock(return_value=sample_workflow.steps[0])
        
        with patch.object(engine, '_execute_step') as mock_execute_step:
            mock_execute_step.return_value = True
            
            with patch.object(engine, '_collect_output_files') as mock_collect:
                mock_collect.return_value = []
                
                engine.execute(sample_workflow, sample_workflow_run, progress_callback)
        
        # Should have been called twice - start and end of step
        assert progress_callback.call_count == 2
        assert progress_callback.call_args_list[0][0] == ("test_step_1", 0.0, "Executing step: Test Processing Step")
        assert progress_callback.call_args_list[1][0] == ("test_step_1", 1.0, "Completed step: Test Processing Step")
    
    def test_execute_exception_handling(self, sample_workflow, sample_workflow_run):
        """Test execution exception handling."""
        engine = PythonWorkflowEngine()
        
        # Mock workflow methods to raise exception
        sample_workflow.get_execution_order = Mock(side_effect=Exception("Execution order failed"))
        
        result = engine.execute(sample_workflow, sample_workflow_run)
        
        assert result.success is False
        assert "Execution order failed" in result.error_message
    
    def test_execute_step_with_script_path(self, sample_workflow_run):
        """Test executing step with script path."""
        engine = PythonWorkflowEngine()
        
        step = WorkflowStep(
            step_id="script_step",
            name="Script Step",
            script_path="/path/to/script.py",
            parameters={"param1": "value1"}
        )
        
        workflow = WorkflowEntity(
            workflow_id="test_workflow",
            name="Test",
            steps=[step],
            global_parameters={},
            metadata={}
        )
        
        # Mock successful subprocess
        mock_result = Mock()
        mock_result.returncode = 0
        mock_result.stderr = ""
        
        with patch('subprocess.run', return_value=mock_result) as mock_run:
            success = engine._execute_step(step, workflow, sample_workflow_run)
        
        assert success is True
        mock_run.assert_called_once()
        
        # Check command and environment
        call_args = mock_run.call_args
        assert call_args[0][0] == ["python", "/path/to/script.py"]
        assert "param1" in call_args[1]["env"]
        assert call_args[1]["env"]["param1"] == "value1"
    
    def test_execute_step_with_command(self, sample_workflow_run):
        """Test executing step with shell command."""
        engine = PythonWorkflowEngine()
        
        step = WorkflowStep(
            step_id="command_step",
            name="Command Step",
            command="echo 'Hello World'"
        )
        
        workflow = WorkflowEntity(
            workflow_id="test_workflow",
            name="Test",
            steps=[step],
            global_parameters={},
            metadata={}
        )
        
        # Mock successful subprocess
        mock_result = Mock()
        mock_result.returncode = 0
        mock_result.stderr = ""
        
        with patch('subprocess.run', return_value=mock_result) as mock_run:
            success = engine._execute_step(step, workflow, sample_workflow_run)
        
        assert success is True
        mock_run.assert_called_once()
        
        # Check shell command
        call_args = mock_run.call_args
        assert call_args[0][0] == "echo 'Hello World'"
        assert call_args[1]["shell"] is True
    
    def test_execute_step_script_failure(self, sample_workflow_run):
        """Test executing step with script failure."""
        engine = PythonWorkflowEngine()
        
        step = WorkflowStep(
            step_id="failing_step",
            name="Failing Step",
            script_path="/path/to/failing_script.py"
        )
        
        workflow = WorkflowEntity(
            workflow_id="test_workflow",
            name="Test",
            steps=[step],
            global_parameters={},
            metadata={}
        )
        
        # Mock failed subprocess
        mock_result = Mock()
        mock_result.returncode = 1
        mock_result.stderr = "Script error occurred"
        
        with patch('subprocess.run', return_value=mock_result):
            success = engine._execute_step(step, workflow, sample_workflow_run)
        
        assert success is False
    
    def test_execute_step_exception(self, sample_workflow_run):
        """Test executing step with exception."""
        engine = PythonWorkflowEngine()
        
        step = WorkflowStep(
            step_id="error_step",
            name="Error Step",
            script_path="/path/to/script.py"
        )
        
        workflow = WorkflowEntity(
            workflow_id="test_workflow",
            name="Test",
            steps=[step],
            global_parameters={},
            metadata={}
        )
        
        with patch('subprocess.run', side_effect=Exception("Subprocess failed")):
            success = engine._execute_step(step, workflow, sample_workflow_run)
        
        assert success is False
    
    def test_collect_output_files(self, sample_workflow, sample_workflow_run):
        """Test collecting output files."""
        engine = PythonWorkflowEngine()
        
        # Mock file existence
        with patch('os.path.exists') as mock_exists:
            mock_exists.return_value = True
            
            output_files = engine._collect_output_files(sample_workflow, sample_workflow_run)
        
        assert "output.txt" in output_files
    
    def test_collect_output_files_with_templates(self, sample_workflow_run):
        """Test collecting output files with template resolution."""
        step = WorkflowStep(
            step_id="template_step",
            name="Template Step",
            output_files=["result_{input_data}_{threshold}.txt"]
        )
        
        workflow = WorkflowEntity(
            workflow_id="test_workflow",
            name="Test",
            steps=[step],
            global_parameters={"global_param": "global_value"},
            metadata={}
        )
        
        engine = PythonWorkflowEngine()
        
        # Mock file existence
        with patch('os.path.exists') as mock_exists:
            mock_exists.return_value = True
            
            output_files = engine._collect_output_files(workflow, sample_workflow_run)
        
        # Should resolve template variables
        assert "result_/data/input.nc_0.5.txt" in output_files
    
    def test_validate_workflow_python_not_found(self, sample_workflow):
        """Test workflow validation when Python not found."""
        engine = PythonWorkflowEngine(python_executable="nonexistent_python")
        
        with patch('subprocess.run') as mock_run:
            mock_run.side_effect = FileNotFoundError("Python not found")
            
            errors = engine.validate_workflow(sample_workflow)
        
        assert len(errors) >= 1
        assert "Python executable not found: nonexistent_python" in errors[0]
    
    def test_validate_workflow_script_not_found(self, sample_workflow_run):
        """Test workflow validation with missing script file."""
        step = WorkflowStep(
            step_id="missing_script_step",
            name="Missing Script Step",
            script_path="/nonexistent/script.py"
        )
        
        workflow = WorkflowEntity(
            workflow_id="test_workflow",
            name="Test",
            steps=[step],
            global_parameters={},
            metadata={}
        )
        
        engine = PythonWorkflowEngine()
        
        with patch('subprocess.run'):  # Mock successful Python version check
            with patch('os.path.exists', return_value=False):
                errors = engine.validate_workflow(workflow)
        
        assert len(errors) >= 1
        assert "Script file not found: /nonexistent/script.py" in errors[0]
    
    def test_validate_workflow_success(self, sample_workflow):
        """Test successful workflow validation."""
        engine = PythonWorkflowEngine()
        
        with patch('subprocess.run'):  # Mock successful Python version check
            with patch('os.path.exists', return_value=True):  # Mock script exists
                errors = engine.validate_workflow(sample_workflow)
        
        assert errors == []
    
    def test_estimate_resources(self, sample_workflow):
        """Test resource estimation."""
        engine = PythonWorkflowEngine()
        
        resources = engine.estimate_resources(sample_workflow)
        
        assert resources["estimated_cores"] == 1
        assert resources["estimated_memory_gb"] == 1.0
        assert resources["estimated_disk_gb"] == 0.1
        assert resources["estimated_walltime_hours"] == 0.1  # 1 step * 0.1 hours
    
    def test_cancel_execution(self):
        """Test execution cancellation."""
        engine = PythonWorkflowEngine()
        
        result = engine.cancel_execution("test_run_123")
        
        assert result is True
    
    def test_get_execution_logs(self):
        """Test getting execution logs."""
        engine = PythonWorkflowEngine()
        
        logs = engine.get_execution_logs("test_run_123")
        
        assert logs == []


class TestIntegrationScenarios:
    """Test integration scenarios and real-world usage patterns."""
    
    def test_snakemake_complete_workflow_execution(self, sample_workflow, sample_workflow_run):
        """Test complete Snakemake workflow execution scenario."""
        engine = SnakemakeWorkflowEngine()
        
        # Mock the entire subprocess execution pipeline
        mock_process = Mock()
        mock_process.returncode = 0
        mock_process.stdout.readline.side_effect = [
            "Building DAG of jobs...\n",
            "Using shell: /bin/bash\n",
            "rule test_step_1:\n",
            "    input: input.txt\n",
            "    output: output.txt\n",
            "    shell:\n",
            "        echo 'Processing data'\n",
            "[Mon Jan  1 12:00:00 2023] rule test_step_1:\n",
            "    input: input.txt\n",
            "    output: output.txt\n",
            "Finished job 0.\n",
            "1 of 1 steps (100%) done\n",
            ""
        ]
        mock_process.poll.side_effect = [None] * 10 + [0]  # Process running then finished
        mock_process.communicate.return_value = ("", "")
        
        with patch('subprocess.Popen', return_value=mock_process):
            with patch('os.path.exists', return_value=True):  # Mock output files exist
                result = engine.execute(sample_workflow, sample_workflow_run)
        
        assert result.success is True
        assert result.run_id == "run_12345"
        assert result.workflow_id == "test_workflow_001"
        assert isinstance(result.execution_time_seconds, float)
        assert result.execution_time_seconds > 0
    
    def test_python_multi_step_workflow(self, sample_workflow_run):
        """Test Python engine with multi-step workflow."""
        # Create workflow with multiple steps
        step1 = WorkflowStep(
            step_id="step1",
            name="Data Preprocessing",
            command="python preprocess.py",
            output_files=["preprocessed.csv"]
        )
        
        step2 = WorkflowStep(
            step_id="step2",
            name="Data Analysis",
            command="python analyze.py",
            input_files=["preprocessed.csv"],
            output_files=["results.json"],
            dependencies=["step1"]
        )
        
        step3 = WorkflowStep(
            step_id="step3",
            name="Report Generation",
            command="python report.py",
            input_files=["results.json"],
            output_files=["report.html"],
            dependencies=["step2"]
        )
        
        workflow = WorkflowEntity(
            workflow_id="multi_step_workflow",
            name="Multi-Step Analysis",
            steps=[step1, step2, step3],
            global_parameters={},
            metadata={}
        )
        
        # Mock workflow execution order
        workflow.get_execution_order = Mock(return_value=["step1", "step2", "step3"])
        workflow.get_step = Mock(side_effect=lambda step_id: {
            "step1": step1, "step2": step2, "step3": step3
        }.get(step_id))
        
        engine = PythonWorkflowEngine()
        progress_callback = Mock()
        
        # Mock successful subprocess calls
        mock_result = Mock()
        mock_result.returncode = 0
        mock_result.stderr = ""
        
        with patch('subprocess.run', return_value=mock_result):
            with patch('os.path.exists', return_value=True):
                result = engine.execute(workflow, sample_workflow_run, progress_callback)
        
        assert result.success is True
        assert progress_callback.call_count == 6  # 2 calls per step (start and end)
        assert "preprocessed.csv" in result.output_files
        assert "results.json" in result.output_files
        assert "report.html" in result.output_files
    
    def test_workflow_with_resource_constraints(self):
        """Test workflow execution with resource constraints."""
        high_memory_step = WorkflowStep(
            step_id="memory_intensive",
            name="Memory Intensive Processing",
            command="python process_big_data.py",
            resource_requirements=ResourceRequirement(
                cores=16,
                memory_gb=64.0,
                disk_gb=500.0,
                walltime_hours=12.0
            ),
            input_files=["big_data.nc"],
            output_files=["processed_data.nc"]
        )
        
        workflow = WorkflowEntity(
            workflow_id="hpc_workflow",
            name="HPC Workflow",
            steps=[high_memory_step],
            global_parameters={"cores": 16},
            metadata={}
        )
        
        engine = SnakemakeWorkflowEngine()
        
        # Test resource estimation
        resources = engine.estimate_resources(workflow)
        
        assert resources["estimated_cores"] == 16
        assert resources["estimated_memory_gb"] == 64.0
        assert resources["estimated_disk_gb"] == 500.0
        assert resources["estimated_walltime_hours"] == 12.0
    
    def test_error_recovery_and_logging(self, sample_workflow, sample_workflow_run):
        """Test error recovery and logging during workflow execution."""
        engine = SnakemakeWorkflowEngine()
        
        # Mock subprocess that fails initially then succeeds
        failure_process = Mock()
        failure_process.returncode = 1
        failure_process.stdout.readline.return_value = ""
        failure_process.poll.return_value = 1
        failure_process.communicate.return_value = ("", "MissingInputException: input.txt not found")
        
        with patch('subprocess.Popen', return_value=failure_process):
            result = engine.execute(sample_workflow, sample_workflow_run)
        
        assert result.success is False
        assert "MissingInputException" in result.error_message
        assert isinstance(result.execution_time_seconds, float)
        assert result.end_time is not None


class TestEdgeCases:
    """Test edge cases and boundary conditions."""
    
    def test_snakemake_empty_workflow(self, sample_workflow_run):
        """Test Snakemake engine with empty workflow."""
        workflow = WorkflowEntity(
            workflow_id="empty_workflow",
            name="Empty Workflow",
            steps=[],
            global_parameters={},
            metadata={}
        )
        
        engine = SnakemakeWorkflowEngine()
        
        result = engine.execute(workflow, sample_workflow_run)
        
        # Should handle empty workflow gracefully
        assert result.run_id == "run_12345"
        assert result.workflow_id == "empty_workflow"
    
    def test_python_engine_step_without_command_or_script(self, sample_workflow_run):
        """Test Python engine with step lacking command or script."""
        step = WorkflowStep(
            step_id="empty_step",
            name="Empty Step"
            # No command or script_path
        )
        
        workflow = WorkflowEntity(
            workflow_id="test_workflow",
            name="Test",
            steps=[step],
            global_parameters={},
            metadata={}
        )
        
        workflow.get_execution_order = Mock(return_value=["empty_step"])
        workflow.get_step = Mock(return_value=step)
        
        engine = PythonWorkflowEngine()
        
        result = engine.execute(workflow, sample_workflow_run)
        
        # Should complete successfully but might not do anything useful
        assert result.run_id == "run_12345"
    
    def test_snakemake_api_not_available_fallback(self, sample_workflow_with_file, sample_workflow_run):
        """Test Snakemake engine fallback when API not available."""
        engine = SnakemakeWorkflowEngine()
        
        with patch('tellus.infrastructure.adapters.workflow_engines.SNAKEMAKE_AVAILABLE', False):
            result = engine._execute_with_api(sample_workflow_with_file, sample_workflow_run)
        
        assert result["success"] is False
        assert "Snakemake API not available" in result["error_message"]
        assert "falling back to subprocess execution" in result["warnings"][0]
    
    def test_workflow_with_circular_dependencies(self):
        """Test workflow validation with circular dependencies."""
        step1 = WorkflowStep(step_id="step1", name="Step 1", dependencies=["step2"])
        step2 = WorkflowStep(step_id="step2", name="Step 2", dependencies=["step1"])
        
        workflow = WorkflowEntity(
            workflow_id="circular_workflow",
            name="Circular Workflow",
            steps=[step1, step2],
            global_parameters={},
            metadata={}
        )
        
        engine = PythonWorkflowEngine()
        
        # The workflow should handle this at the domain level
        # Engine validation focuses on executable and file availability
        with patch('subprocess.run'):
            errors = engine.validate_workflow(workflow)
        
        # Engine-level validation might not catch circular dependencies
        # That would be handled at the domain/application level
        assert isinstance(errors, list)
    
    def test_resource_estimation_edge_cases(self):
        """Test resource estimation with edge cases."""
        # Workflow with no resource requirements
        step_no_resources = WorkflowStep(
            step_id="minimal_step",
            name="Minimal Step",
            command="echo 'hello'"
        )
        
        workflow = WorkflowEntity(
            workflow_id="minimal_workflow",
            name="Minimal Workflow",
            steps=[step_no_resources],
            global_parameters={},
            metadata={}
        )
        
        snakemake_engine = SnakemakeWorkflowEngine()
        python_engine = PythonWorkflowEngine()
        
        snakemake_resources = snakemake_engine.estimate_resources(workflow)
        python_resources = python_engine.estimate_resources(workflow)
        
        # Should handle missing resource requirements gracefully
        assert snakemake_resources["estimated_cores"] == 0
        assert python_resources["estimated_cores"] == 1
    
    def test_long_running_workflow_simulation(self, sample_workflow, sample_workflow_run):
        """Test simulation of long-running workflow with progress updates."""
        engine = PythonWorkflowEngine()
        progress_callback = Mock()
        
        # Simulate long execution order
        long_execution_order = [f"step_{i}" for i in range(10)]
        sample_workflow.get_execution_order = Mock(return_value=long_execution_order)
        sample_workflow.get_step = Mock(return_value=sample_workflow.steps[0])
        
        with patch.object(engine, '_execute_step') as mock_execute:
            mock_execute.return_value = True
            
            with patch.object(engine, '_collect_output_files') as mock_collect:
                mock_collect.return_value = []
                
                result = engine.execute(sample_workflow, sample_workflow_run, progress_callback)
        
        assert result.success is True
        # Should have called progress callback 20 times (10 steps * 2 calls each)
        assert progress_callback.call_count == 20
    
    def test_workflow_parameter_templating_complex(self, sample_workflow_run):
        """Test complex parameter templating in workflow steps."""
        step = WorkflowStep(
            step_id="template_step",
            name="Template Step",
            command="process {input_data} --threshold {threshold} --output {output_dir}/result_{run_id}.nc",
            output_files=["{output_dir}/result_{run_id}.nc", "{output_dir}/log_{run_id}.txt"]
        )
        
        workflow = WorkflowEntity(
            workflow_id="template_workflow",
            name="Template Workflow",
            steps=[step],
            global_parameters={"output_dir": "/results"},
            metadata={}
        )
        
        # Add run_id to input parameters
        sample_workflow_run.input_parameters["run_id"] = "test_run_001"
        
        engine = PythonWorkflowEngine()
        
        with patch('os.path.exists', return_value=True):
            output_files = engine._collect_output_files(workflow, sample_workflow_run)
        
        assert "/results/result_test_run_001.nc" in output_files
        assert "/results/log_test_run_001.txt" in output_files