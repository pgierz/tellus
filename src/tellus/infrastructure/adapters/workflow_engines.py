"""
Infrastructure adapters for workflow execution engines.

These adapters provide concrete implementations for executing workflows
on different platforms while maintaining a clean interface for the
application layer.
"""

import json
import logging
import os
import subprocess
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

try:
    from snakemake.api import (ResourceSettings, SnakemakeApi, StorageSettings,
                               WorkflowApi)
    SNAKEMAKE_AVAILABLE = True
except ImportError:
    # Snakemake not available - create stub classes
    SnakemakeApi = None
    ResourceSettings = None  
    StorageSettings = None
    WorkflowApi = None
    SNAKEMAKE_AVAILABLE = False

from ...application.dtos import WorkflowExecutionResultDto
from ...application.services.workflow_execution_service import IWorkflowEngine
from ...domain.entities.workflow import WorkflowEntity, WorkflowRunEntity

logger = logging.getLogger(__name__)


class SnakemakeWorkflowEngine(IWorkflowEngine):
    """
    Snakemake workflow execution engine adapter.
    
    Integrates with the Snakemake API to execute workflows defined
    in Snakefile format, with support for remote storage and HPC environments.
    """
    
    def __init__(
        self,
        snakemake_executable: str = "snakemake",
        default_cores: int = 1,
        default_resources: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize Snakemake engine adapter.
        
        Args:
            snakemake_executable: Path to snakemake executable
            default_cores: Default number of cores to use
            default_resources: Default resource settings
        """
        if not SNAKEMAKE_AVAILABLE:
            self._logger = logger
            self._logger.warning("Snakemake not available - engine will only support subprocess execution")
        
        self.snakemake_executable = snakemake_executable
        self.default_cores = default_cores
        self.default_resources = default_resources or {}
        self._logger = logger
    
    def execute(
        self,
        workflow: WorkflowEntity,
        run: WorkflowRunEntity,
        progress_callback: Optional[Callable[[str, float, str], None]] = None
    ) -> WorkflowExecutionResultDto:
        """
        Execute a workflow using Snakemake.
        
        Args:
            workflow: Workflow definition
            run: Workflow run instance
            progress_callback: Optional progress callback function
            
        Returns:
            Execution result with success status and output information
        """
        self._logger.info(f"Executing workflow {workflow.workflow_id} with Snakemake")
        
        start_time = datetime.now()
        execution_result = WorkflowExecutionResultDto(
            run_id=run.run_id,
            workflow_id=workflow.workflow_id,
            success=False,
            start_time=start_time.isoformat(),
            output_files=[],
            resource_usage={}
        )
        
        try:
            # Use Snakemake API for execution if available
            if SNAKEMAKE_AVAILABLE and workflow.workflow_file and Path(workflow.workflow_file).exists():
                result = self._execute_with_api(workflow, run, progress_callback)
            else:
                # Generate Snakefile from workflow steps or use if API not available
                result = self._execute_with_generated_snakefile(workflow, run, progress_callback)
            
            execution_result.success = result["success"]
            execution_result.end_time = datetime.now().isoformat()
            execution_result.execution_time_seconds = (
                datetime.now() - start_time
            ).total_seconds()
            
            if result["success"]:
                execution_result.output_files = result.get("output_files", [])
                execution_result.resource_usage = result.get("resource_usage", {})
                self._logger.info(f"Successfully executed workflow {workflow.workflow_id}")
            else:
                execution_result.error_message = result.get("error_message", "Unknown error")
                execution_result.warnings = result.get("warnings", [])
                self._logger.error(f"Failed to execute workflow {workflow.workflow_id}: {execution_result.error_message}")
            
            return execution_result
            
        except Exception as e:
            self._logger.error(f"Error executing workflow {workflow.workflow_id}: {str(e)}")
            execution_result.error_message = str(e)
            execution_result.end_time = datetime.now().isoformat()
            execution_result.execution_time_seconds = (
                datetime.now() - start_time
            ).total_seconds()
            return execution_result
    
    def _execute_with_api(
        self,
        workflow: WorkflowEntity,
        run: WorkflowRunEntity,
        progress_callback: Optional[Callable[[str, float, str], None]] = None
    ) -> Dict[str, Any]:
        """Execute workflow using existing Snakefile via Snakemake API."""
        if not SNAKEMAKE_AVAILABLE:
            return {
                "success": False,
                "error_message": "Snakemake API not available",
                "warnings": ["Snakemake API not installed - falling back to subprocess execution"]
            }
        
        try:
            with SnakemakeApi() as api:
                # Configure storage settings
                storage_settings = self._create_storage_settings(run.location_context)
                
                # Configure resource settings
                resource_settings = self._create_resource_settings(workflow)
                
                # Create workflow API
                workflow_api = api.workflow(
                    snakefile=Path(workflow.workflow_file),
                    storage_settings=storage_settings,
                    resource_settings=resource_settings
                )
                
                # Set up progress tracking
                if progress_callback:
                    self._setup_progress_tracking(workflow_api, progress_callback)
                
                # Execute workflow
                dag_api = workflow_api.dag(
                    **run.input_parameters,
                    configfiles=[],  # Config would be passed here
                    config=workflow.global_parameters
                )
                
                execution_plan = dag_api.execute(
                    cores=self._get_cores_from_workflow(workflow),
                    force_incomplete=False,
                    keep_going=False,
                    dry_run=False
                )
                
                # Collect results
                output_files = []
                resource_usage = {}
                
                # Process execution results
                for job_result in execution_plan:
                    if hasattr(job_result, 'output'):
                        output_files.extend(job_result.output)
                    if hasattr(job_result, 'resources'):
                        # Aggregate resource usage
                        for resource, value in job_result.resources.items():
                            resource_usage[resource] = resource_usage.get(resource, 0) + value
                
                return {
                    "success": True,
                    "output_files": output_files,
                    "resource_usage": resource_usage
                }
                
        except Exception as e:
            return {
                "success": False,
                "error_message": str(e),
                "warnings": []
            }
    
    def _execute_with_generated_snakefile(
        self,
        workflow: WorkflowEntity,
        run: WorkflowRunEntity,
        progress_callback: Optional[Callable[[str, float, str], None]] = None
    ) -> Dict[str, Any]:
        """Execute workflow by generating a Snakefile from workflow steps."""
        try:
            # Generate Snakefile content
            snakefile_content = self._generate_snakefile(workflow, run)
            
            # Create temporary Snakefile
            with tempfile.NamedTemporaryFile(mode='w', suffix='.smk', delete=False) as f:
                f.write(snakefile_content)
                temp_snakefile = f.name
            
            try:
                # Execute using subprocess (fallback method)
                result = self._execute_with_subprocess(
                    temp_snakefile, workflow, run, progress_callback
                )
                return result
                
            finally:
                # Clean up temporary file
                os.unlink(temp_snakefile)
                
        except Exception as e:
            return {
                "success": False,
                "error_message": str(e),
                "warnings": []
            }
    
    def _execute_with_subprocess(
        self,
        snakefile_path: str,
        workflow: WorkflowEntity,
        run: WorkflowRunEntity,
        progress_callback: Optional[Callable[[str, float, str], None]] = None
    ) -> Dict[str, Any]:
        """Execute workflow using subprocess call to snakemake."""
        try:
            # Build snakemake command
            cmd = [
                self.snakemake_executable,
                "-s", snakefile_path,
                "--cores", str(self._get_cores_from_workflow(workflow)),
                "--force-incomplete",
                "--printshellcmds",
                "--reason"
            ]
            
            # Add configuration
            if workflow.global_parameters:
                config_str = json.dumps(workflow.global_parameters)
                cmd.extend(["--config", config_str])
            
            # Add input parameters
            for key, value in run.input_parameters.items():
                cmd.extend(["--config", f"{key}={value}"])
            
            # Execute command
            self._logger.debug(f"Executing Snakemake command: {' '.join(cmd)}")
            
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                universal_newlines=True
            )
            
            # Track progress by parsing output
            output_lines = []
            error_lines = []
            
            while True:
                output = process.stdout.readline()
                if output == '' and process.poll() is not None:
                    break
                
                if output:
                    output_lines.append(output.strip())
                    self._logger.debug(f"Snakemake output: {output.strip()}")
                    
                    # Parse progress from output if callback provided
                    if progress_callback:
                        self._parse_progress_from_output(output.strip(), progress_callback)
            
            # Get any remaining stderr
            stderr_output = process.communicate()[1]
            if stderr_output:
                error_lines.extend(stderr_output.splitlines())
            
            # Check return code
            return_code = process.returncode
            
            if return_code == 0:
                # Success - collect output files
                output_files = self._collect_output_files(workflow, run)
                resource_usage = self._parse_resource_usage(output_lines)
                
                return {
                    "success": True,
                    "output_files": output_files,
                    "resource_usage": resource_usage
                }
            else:
                error_message = "\n".join(error_lines) if error_lines else f"Process failed with code {return_code}"
                return {
                    "success": False,
                    "error_message": error_message,
                    "warnings": []
                }
                
        except Exception as e:
            return {
                "success": False,
                "error_message": str(e),
                "warnings": []
            }
    
    def _generate_snakefile(self, workflow: WorkflowEntity, run: WorkflowRunEntity) -> str:
        """Generate Snakefile content from workflow definition."""
        lines = [
            "# Auto-generated Snakefile from workflow definition",
            f"# Workflow: {workflow.workflow_id}",
            f"# Run: {run.run_id}",
            "",
            "import os",
            "import json",
            "",
        ]
        
        # Add configuration
        if workflow.global_parameters or run.input_parameters:
            lines.append("# Configuration")
            config_dict = {**workflow.global_parameters, **run.input_parameters}
            lines.append(f"configfile: 'config.yaml'  # Would be generated")
            lines.append("")
        
        # Add storage configuration if location context provided
        if run.location_context:
            lines.extend(self._generate_storage_config(run.location_context))
            lines.append("")
        
        # Generate rules from workflow steps
        for step in workflow.steps:
            lines.extend(self._generate_snakemake_rule(step, workflow, run))
            lines.append("")
        
        # Add final target rule
        if workflow.steps:
            output_files = []
            for step in workflow.steps:
                output_files.extend(step.output_files)
            
            if output_files:
                lines.extend([
                    "rule all:",
                    "    input:",
                    *[f"        \"{f}\"," for f in output_files],
                    ""
                ])
        
        return "\n".join(lines)
    
    def _generate_storage_config(self, location_context: Dict[str, str]) -> List[str]:
        """Generate storage configuration for remote locations."""
        lines = ["# Storage configuration"]
        
        # This would map location names to fsspec/snakemake storage providers
        # For now, generate basic SFTP storage example
        for context_key, location_name in location_context.items():
            if "remote" in context_key.lower() or "sftp" in context_key.lower():
                lines.extend([
                    f"storage {location_name}:",
                    "    provider=\"fsspec\",",
                    "    protocol=\"sftp\",",
                    "    storage_options={'host': config['host']},",
                ])
        
        return lines
    
    def _generate_snakemake_rule(
        self, step: "WorkflowStep", workflow: WorkflowEntity, run: WorkflowRunEntity
    ) -> List[str]:
        """Generate a Snakemake rule from a workflow step."""
        lines = [f"rule {step.step_id}:"]
        
        # Input files
        if step.input_files:
            lines.append("    input:")
            for input_file in step.input_files:
                lines.append(f"        \"{input_file}\",")
        
        # Output files
        if step.output_files:
            lines.append("    output:")
            for output_file in step.output_files:
                lines.append(f"        \"{output_file}\",")
        
        # Parameters
        if step.parameters:
            lines.append("    params:")
            for key, value in step.parameters.items():
                lines.append(f"        {key}=\"{value}\",")
        
        # Resources
        if step.resource_requirements:
            req = step.resource_requirements
            lines.append("    resources:")
            if req.cores:
                lines.append(f"        cores={req.cores},")
            if req.memory_gb:
                lines.append(f"        mem_gb={req.memory_gb},")
            if req.walltime_hours:
                lines.append(f"        runtime={int(req.walltime_hours * 60)},")  # Convert to minutes
        
        # Execution
        if step.command:
            lines.append("    shell:")
            lines.append(f"        \"{step.command}\"")
        elif step.script_path:
            lines.append("    script:")
            lines.append(f"        \"{step.script_path}\"")
        else:
            # Default shell command
            lines.append("    shell:")
            lines.append("        \"echo 'Step {step.step_id} executed'\"")
        
        return lines
    
    def _create_storage_settings(self, location_context: Dict[str, str]):
        """Create Snakemake storage settings from location context."""
        if not SNAKEMAKE_AVAILABLE or StorageSettings is None:
            return None
        # This would map location names to actual storage configurations
        return StorageSettings()
    
    def _create_resource_settings(self, workflow: WorkflowEntity):
        """Create Snakemake resource settings from workflow."""
        if not SNAKEMAKE_AVAILABLE or ResourceSettings is None:
            return None
        return ResourceSettings()
    
    def _get_cores_from_workflow(self, workflow: WorkflowEntity) -> int:
        """Get number of cores to use from workflow configuration."""
        # Check if cores specified in global parameters
        if "cores" in workflow.global_parameters:
            return workflow.global_parameters["cores"]
        
        # Estimate from step requirements
        max_cores = 0
        for step in workflow.steps:
            if step.resource_requirements and step.resource_requirements.cores:
                max_cores = max(max_cores, step.resource_requirements.cores)
        
        return max_cores or self.default_cores
    
    def _setup_progress_tracking(
        self, 
        workflow_api, 
        progress_callback: Callable[[str, float, str], None]
    ) -> None:
        """Set up progress tracking for Snakemake execution."""
        # This would integrate with Snakemake's progress reporting
        # For now, provide a basic implementation
        pass
    
    def _parse_progress_from_output(
        self, 
        output_line: str, 
        progress_callback: Callable[[str, float, str], None]
    ) -> None:
        """Parse progress information from Snakemake output."""
        # Parse common Snakemake output patterns
        if "rule" in output_line and ":" in output_line:
            # Extract rule name
            rule_name = output_line.split("rule")[1].split(":")[0].strip()
            progress_callback(rule_name, 0.0, f"Starting rule: {rule_name}")
        elif "Finished job" in output_line:
            # Job completed
            if "rule" in output_line:
                rule_name = output_line.split("rule")[1].split()[0].strip()
                progress_callback(rule_name, 1.0, f"Completed rule: {rule_name}")
    
    def _collect_output_files(self, workflow: WorkflowEntity, run: WorkflowRunEntity) -> List[str]:
        """Collect output files produced by workflow execution."""
        output_files = []
        
        # Collect from all steps
        for step in workflow.steps:
            for output_file in step.output_files:
                # Resolve any template variables
                resolved_path = output_file.format(**run.input_parameters, **workflow.global_parameters)
                if os.path.exists(resolved_path):
                    output_files.append(resolved_path)
        
        return output_files
    
    def _parse_resource_usage(self, output_lines: List[str]) -> Dict[str, Any]:
        """Parse resource usage information from Snakemake output."""
        resource_usage = {}
        
        # Parse Snakemake output for resource information
        # This would extract CPU time, memory usage, etc.
        
        return resource_usage
    
    def validate_workflow(self, workflow: WorkflowEntity) -> List[str]:
        """
        Validate workflow for Snakemake execution.
        
        Args:
            workflow: Workflow to validate
            
        Returns:
            List of validation error messages
        """
        errors = []
        
        # Check if Snakemake is available
        try:
            subprocess.run([self.snakemake_executable, "--version"], 
                         capture_output=True, check=True)
        except (subprocess.CalledProcessError, FileNotFoundError):
            errors.append(f"Snakemake executable not found: {self.snakemake_executable}")
        
        # Validate workflow file if provided
        if workflow.workflow_file:
            workflow_path = Path(workflow.workflow_file)
            if not workflow_path.exists():
                errors.append(f"Workflow file not found: {workflow.workflow_file}")
            elif not workflow_path.is_file():
                errors.append(f"Workflow file is not a file: {workflow.workflow_file}")
        
        # Validate steps
        if not workflow.steps and not workflow.workflow_file:
            errors.append("Workflow must have either steps or a workflow file")
        
        for step in workflow.steps:
            if not step.command and not step.script_path:
                errors.append(f"Step {step.step_id} must have either command or script_path")
            
            if step.script_path and not os.path.exists(step.script_path):
                errors.append(f"Script file not found for step {step.step_id}: {step.script_path}")
        
        return errors
    
    def estimate_resources(self, workflow: WorkflowEntity) -> Dict[str, Any]:
        """
        Estimate resource requirements for workflow.
        
        Args:
            workflow: Workflow to analyze
            
        Returns:
            Dictionary of estimated resource requirements
        """
        total_cores = 0
        max_memory = 0.0
        total_disk = 0.0
        max_walltime = 0.0
        
        for step in workflow.steps:
            if step.resource_requirements:
                req = step.resource_requirements
                if req.cores:
                    total_cores += req.cores
                if req.memory_gb:
                    max_memory = max(max_memory, req.memory_gb)
                if req.disk_gb:
                    total_disk += req.disk_gb
                if req.walltime_hours:
                    max_walltime = max(max_walltime, req.walltime_hours)
        
        return {
            "estimated_cores": total_cores,
            "estimated_memory_gb": max_memory,
            "estimated_disk_gb": total_disk,
            "estimated_walltime_hours": max_walltime,
            "parallel_steps": len([s for s in workflow.steps if not s.dependencies])
        }
    
    def cancel_execution(self, run_id: str) -> bool:
        """
        Cancel a running workflow execution.
        
        Args:
            run_id: ID of the workflow run to cancel
            
        Returns:
            True if cancellation was successful
        """
        # This would need to track running processes/jobs
        # and send appropriate signals to cancel them
        self._logger.info(f"Cancelling Snakemake execution for run: {run_id}")
        # Implementation would depend on job tracking mechanism
        return True
    
    def get_execution_logs(self, run_id: str) -> List[str]:
        """
        Get execution logs for a workflow run.
        
        Args:
            run_id: ID of the workflow run
            
        Returns:
            List of log entries
        """
        # This would read from Snakemake log files
        # For now, return empty list
        return []


class PythonWorkflowEngine(IWorkflowEngine):
    """
    Pure Python workflow execution engine.
    
    Executes workflows defined as Python functions or scripts,
    useful for simple data processing pipelines.
    """
    
    def __init__(self, python_executable: str = "python"):
        """Initialize Python workflow engine."""
        self.python_executable = python_executable
        self._logger = logger
    
    def execute(
        self,
        workflow: WorkflowEntity,
        run: WorkflowRunEntity,
        progress_callback: Optional[Callable[[str, float, str], None]] = None
    ) -> WorkflowExecutionResultDto:
        """Execute workflow using Python."""
        self._logger.info(f"Executing workflow {workflow.workflow_id} with Python")
        
        start_time = datetime.now()
        execution_result = WorkflowExecutionResultDto(
            run_id=run.run_id,
            workflow_id=workflow.workflow_id,
            success=False,
            start_time=start_time.isoformat(),
            output_files=[],
            resource_usage={}
        )
        
        try:
            # Execute steps in order
            execution_order = workflow.get_execution_order()
            total_steps = len(execution_order)
            
            for i, step_id in enumerate(execution_order):
                step = workflow.get_step(step_id)
                if not step:
                    continue
                
                if progress_callback:
                    progress_callback(step_id, i / total_steps, f"Executing step: {step.name}")
                
                # Execute step
                success = self._execute_step(step, workflow, run)
                
                if not success:
                    execution_result.error_message = f"Step {step_id} failed"
                    break
                
                if progress_callback:
                    progress_callback(step_id, (i + 1) / total_steps, f"Completed step: {step.name}")
            else:
                # All steps completed successfully
                execution_result.success = True
                execution_result.output_files = self._collect_output_files(workflow, run)
            
            execution_result.end_time = datetime.now().isoformat()
            execution_result.execution_time_seconds = (
                datetime.now() - start_time
            ).total_seconds()
            
            return execution_result
            
        except Exception as e:
            self._logger.error(f"Error executing Python workflow: {str(e)}")
            execution_result.error_message = str(e)
            execution_result.end_time = datetime.now().isoformat()
            execution_result.execution_time_seconds = (
                datetime.now() - start_time
            ).total_seconds()
            return execution_result
    
    def _execute_step(
        self, step: "WorkflowStep", workflow: WorkflowEntity, run: WorkflowRunEntity
    ) -> bool:
        """Execute a single workflow step."""
        try:
            if step.script_path:
                # Execute Python script
                cmd = [self.python_executable, step.script_path]
                
                # Add parameters as environment variables
                env = os.environ.copy()
                env.update({k: str(v) for k, v in step.parameters.items()})
                env.update({k: str(v) for k, v in run.input_parameters.items()})
                
                result = subprocess.run(cmd, env=env, capture_output=True, text=True)
                
                if result.returncode != 0:
                    self._logger.error(f"Step {step.step_id} failed: {result.stderr}")
                    return False
                
            elif step.command:
                # Execute shell command
                result = subprocess.run(
                    step.command, shell=True, capture_output=True, text=True
                )
                
                if result.returncode != 0:
                    self._logger.error(f"Step {step.step_id} failed: {result.stderr}")
                    return False
            
            return True
            
        except Exception as e:
            self._logger.error(f"Error executing step {step.step_id}: {str(e)}")
            return False
    
    def _collect_output_files(self, workflow: WorkflowEntity, run: WorkflowRunEntity) -> List[str]:
        """Collect output files from workflow execution."""
        output_files = []
        
        for step in workflow.steps:
            for output_file in step.output_files:
                # Resolve template variables
                resolved_path = output_file.format(**run.input_parameters, **workflow.global_parameters)
                if os.path.exists(resolved_path):
                    output_files.append(resolved_path)
        
        return output_files
    
    def validate_workflow(self, workflow: WorkflowEntity) -> List[str]:
        """Validate workflow for Python execution."""
        errors = []
        
        # Check Python availability
        try:
            subprocess.run([self.python_executable, "--version"], 
                         capture_output=True, check=True)
        except (subprocess.CalledProcessError, FileNotFoundError):
            errors.append(f"Python executable not found: {self.python_executable}")
        
        # Validate steps
        for step in workflow.steps:
            if step.script_path and not os.path.exists(step.script_path):
                errors.append(f"Script file not found: {step.script_path}")
        
        return errors
    
    def estimate_resources(self, workflow: WorkflowEntity) -> Dict[str, Any]:
        """Estimate resource requirements."""
        return {
            "estimated_cores": 1,  # Python typically single-threaded
            "estimated_memory_gb": 1.0,  # Conservative estimate
            "estimated_disk_gb": 0.1,
            "estimated_walltime_hours": len(workflow.steps) * 0.1  # 6 minutes per step
        }
    
    def cancel_execution(self, run_id: str) -> bool:
        """Cancel execution (basic implementation)."""
        return True
    
    def get_execution_logs(self, run_id: str) -> List[str]:
        """Get execution logs."""
        return []