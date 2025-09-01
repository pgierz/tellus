"""
JSON-based repository implementations for workflow entities.

These repositories provide persistence for workflow definitions, runs, and templates
using JSON files, following the same pattern as other Tellus repositories.
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from ...application.services.workflow_execution_service import \
    IWorkflowRunRepository
from ...application.services.workflow_service import (
    IWorkflowRepository, IWorkflowTemplateRepository)
from ...domain.entities.workflow import (ExecutionEnvironment,
                                         ResourceRequirement, WorkflowEngine,
                                         WorkflowEntity, WorkflowRunEntity,
                                         WorkflowStatus, WorkflowStep,
                                         WorkflowTemplateEntity)
from ...domain.repositories.exceptions import RepositoryError

logger = logging.getLogger(__name__)


class JsonWorkflowRepository(IWorkflowRepository):
    """JSON-based workflow repository implementation."""
    
    def __init__(self, workflows_file: str = "workflows.json"):
        """
        Initialize JSON workflow repository.
        
        Args:
            workflows_file: Path to JSON file storing workflow data
        """
        self.workflows_file = Path(workflows_file)
        self._logger = logger
        self._ensure_file_exists()
    
    def _ensure_file_exists(self) -> None:
        """Ensure the workflows JSON file exists."""
        if not self.workflows_file.exists():
            self.workflows_file.write_text("{}")
    
    def _load_workflows(self) -> Dict[str, Dict[str, Any]]:
        """Load workflows from JSON file."""
        try:
            with open(self.workflows_file, 'r') as f:
                return json.load(f)
        except json.JSONDecodeError as e:
            self._logger.error(f"Error loading workflows JSON: {e}")
            return {}
        except Exception as e:
            self._logger.error(f"Error reading workflows file: {e}")
            raise RepositoryError(f"Failed to load workflows: {str(e)}")
    
    def _save_workflows(self, workflows: Dict[str, Dict[str, Any]]) -> None:
        """Save workflows to JSON file."""
        try:
            with open(self.workflows_file, 'w') as f:
                json.dump(workflows, f, indent=2, default=str)
        except Exception as e:
            self._logger.error(f"Error saving workflows: {e}")
            raise RepositoryError(f"Failed to save workflows: {str(e)}")
    
    def save(self, workflow: WorkflowEntity) -> None:
        """Save a workflow entity."""
        try:
            workflows = self._load_workflows()
            workflows[workflow.workflow_id] = self._entity_to_dict(workflow)
            self._save_workflows(workflows)
            self._logger.debug(f"Saved workflow: {workflow.workflow_id}")
        except Exception as e:
            self._logger.error(f"Error saving workflow {workflow.workflow_id}: {e}")
            raise RepositoryError(f"Failed to save workflow: {str(e)}")
    
    def get_by_id(self, workflow_id: str) -> Optional[WorkflowEntity]:
        """Get workflow by ID."""
        try:
            workflows = self._load_workflows()
            workflow_data = workflows.get(workflow_id)
            
            if workflow_data is None:
                return None
            
            return self._dict_to_entity(workflow_data)
        except Exception as e:
            self._logger.error(f"Error getting workflow {workflow_id}: {e}")
            raise RepositoryError(f"Failed to get workflow: {str(e)}")
    
    def exists(self, workflow_id: str) -> bool:
        """Check if workflow exists."""
        workflows = self._load_workflows()
        return workflow_id in workflows
    
    def delete(self, workflow_id: str) -> bool:
        """Delete a workflow."""
        try:
            workflows = self._load_workflows()
            
            if workflow_id not in workflows:
                return False
            
            del workflows[workflow_id]
            self._save_workflows(workflows)
            self._logger.debug(f"Deleted workflow: {workflow_id}")
            return True
            
        except Exception as e:
            self._logger.error(f"Error deleting workflow {workflow_id}: {e}")
            raise RepositoryError(f"Failed to delete workflow: {str(e)}")
    
    def list_all(self) -> List[WorkflowEntity]:
        """List all workflows."""
        try:
            workflows = self._load_workflows()
            return [
                self._dict_to_entity(workflow_data)
                for workflow_data in workflows.values()
            ]
        except Exception as e:
            self._logger.error(f"Error listing workflows: {e}")
            raise RepositoryError(f"Failed to list workflows: {str(e)}")
    
    def _entity_to_dict(self, workflow: WorkflowEntity) -> Dict[str, Any]:
        """Convert workflow entity to dictionary."""
        return {
            "workflow_id": workflow.workflow_id,
            "name": workflow.name,
            "description": workflow.description,
            "engine": workflow.engine.value,
            "workflow_file": workflow.workflow_file,
            "steps": [self._step_to_dict(step) for step in workflow.steps],
            "global_parameters": workflow.global_parameters,
            "input_schema": workflow.input_schema,
            "output_schema": workflow.output_schema,
            "tags": list(workflow.tags),
            "version": workflow.version,
            "author": workflow.author,
            "created_at": workflow.created_at.isoformat() if workflow.created_at else None,
            "_uid": workflow._uid
        }
    
    def _dict_to_entity(self, data: Dict[str, Any]) -> WorkflowEntity:
        """Convert dictionary to workflow entity."""
        steps = [self._dict_to_step(step_data) for step_data in data.get("steps", [])]
        
        workflow = WorkflowEntity(
            workflow_id=data["workflow_id"],
            name=data["name"],
            description=data.get("description"),
            engine=WorkflowEngine(data.get("engine", "snakemake")),
            workflow_file=data.get("workflow_file"),
            steps=steps,
            global_parameters=data.get("global_parameters", {}),
            input_schema=data.get("input_schema", {}),
            output_schema=data.get("output_schema", {}),
            tags=set(data.get("tags", [])),
            version=data.get("version", "1.0"),
            author=data.get("author"),
            created_at=datetime.fromisoformat(data["created_at"]) if data.get("created_at") else datetime.now()
        )
        
        # Set internal UID if present
        if "_uid" in data:
            workflow._uid = data["_uid"]
        
        return workflow
    
    def _step_to_dict(self, step: WorkflowStep) -> Dict[str, Any]:
        """Convert workflow step to dictionary."""
        step_dict = {
            "step_id": step.step_id,
            "name": step.name,
            "command": step.command,
            "script_path": step.script_path,
            "input_files": step.input_files,
            "output_files": step.output_files,
            "parameters": step.parameters,
            "dependencies": step.dependencies,
            "retry_count": step.retry_count,
            "max_retries": step.max_retries
        }
        
        if step.resource_requirements:
            step_dict["resource_requirements"] = self._resource_req_to_dict(step.resource_requirements)
        
        return step_dict
    
    def _dict_to_step(self, data: Dict[str, Any]) -> WorkflowStep:
        """Convert dictionary to workflow step."""
        resource_req = None
        if "resource_requirements" in data:
            resource_req = self._dict_to_resource_req(data["resource_requirements"])
        
        return WorkflowStep(
            step_id=data["step_id"],
            name=data["name"],
            command=data.get("command"),
            script_path=data.get("script_path"),
            input_files=data.get("input_files", []),
            output_files=data.get("output_files", []),
            parameters=data.get("parameters", {}),
            dependencies=data.get("dependencies", []),
            resource_requirements=resource_req,
            retry_count=data.get("retry_count", 0),
            max_retries=data.get("max_retries", 3)
        )
    
    def _resource_req_to_dict(self, req: ResourceRequirement) -> Dict[str, Any]:
        """Convert resource requirement to dictionary."""
        return {
            "cores": req.cores,
            "memory_gb": req.memory_gb,
            "disk_gb": req.disk_gb,
            "gpu_count": req.gpu_count,
            "walltime_hours": req.walltime_hours,
            "queue_name": req.queue_name,
            "custom_requirements": req.custom_requirements
        }
    
    def _dict_to_resource_req(self, data: Dict[str, Any]) -> ResourceRequirement:
        """Convert dictionary to resource requirement."""
        return ResourceRequirement(
            cores=data.get("cores"),
            memory_gb=data.get("memory_gb"),
            disk_gb=data.get("disk_gb"),
            gpu_count=data.get("gpu_count"),
            walltime_hours=data.get("walltime_hours"),
            queue_name=data.get("queue_name"),
            custom_requirements=data.get("custom_requirements", {})
        )


class JsonWorkflowRunRepository(IWorkflowRunRepository):
    """JSON-based workflow run repository implementation."""
    
    def __init__(self, runs_file: str = "workflow_runs.json"):
        """
        Initialize JSON workflow run repository.
        
        Args:
            runs_file: Path to JSON file storing workflow run data
        """
        self.runs_file = Path(runs_file)
        self._logger = logger
        self._ensure_file_exists()
    
    def _ensure_file_exists(self) -> None:
        """Ensure the workflow runs JSON file exists."""
        if not self.runs_file.exists():
            self.runs_file.write_text("{}")
    
    def _load_runs(self) -> Dict[str, Dict[str, Any]]:
        """Load workflow runs from JSON file."""
        try:
            with open(self.runs_file, 'r') as f:
                return json.load(f)
        except json.JSONDecodeError as e:
            self._logger.error(f"Error loading workflow runs JSON: {e}")
            return {}
        except Exception as e:
            self._logger.error(f"Error reading workflow runs file: {e}")
            raise RepositoryError(f"Failed to load workflow runs: {str(e)}")
    
    def _save_runs(self, runs: Dict[str, Dict[str, Any]]) -> None:
        """Save workflow runs to JSON file."""
        try:
            with open(self.runs_file, 'w') as f:
                json.dump(runs, f, indent=2, default=str)
        except Exception as e:
            self._logger.error(f"Error saving workflow runs: {e}")
            raise RepositoryError(f"Failed to save workflow runs: {str(e)}")
    
    def save(self, run: WorkflowRunEntity) -> None:
        """Save a workflow run entity."""
        try:
            runs = self._load_runs()
            runs[run.run_id] = self._entity_to_dict(run)
            self._save_runs(runs)
            self._logger.debug(f"Saved workflow run: {run.run_id}")
        except Exception as e:
            self._logger.error(f"Error saving workflow run {run.run_id}: {e}")
            raise RepositoryError(f"Failed to save workflow run: {str(e)}")
    
    def get_by_id(self, run_id: str) -> Optional[WorkflowRunEntity]:
        """Get workflow run by ID."""
        try:
            runs = self._load_runs()
            run_data = runs.get(run_id)
            
            if run_data is None:
                return None
            
            return self._dict_to_entity(run_data)
        except Exception as e:
            self._logger.error(f"Error getting workflow run {run_id}: {e}")
            raise RepositoryError(f"Failed to get workflow run: {str(e)}")
    
    def exists(self, run_id: str) -> bool:
        """Check if workflow run exists."""
        runs = self._load_runs()
        return run_id in runs
    
    def delete(self, run_id: str) -> bool:
        """Delete a workflow run."""
        try:
            runs = self._load_runs()
            
            if run_id not in runs:
                return False
            
            del runs[run_id]
            self._save_runs(runs)
            self._logger.debug(f"Deleted workflow run: {run_id}")
            return True
            
        except Exception as e:
            self._logger.error(f"Error deleting workflow run {run_id}: {e}")
            raise RepositoryError(f"Failed to delete workflow run: {str(e)}")
    
    def list_all(self) -> List[WorkflowRunEntity]:
        """List all workflow runs."""
        try:
            runs = self._load_runs()
            return [
                self._dict_to_entity(run_data)
                for run_data in runs.values()
            ]
        except Exception as e:
            self._logger.error(f"Error listing workflow runs: {e}")
            raise RepositoryError(f"Failed to list workflow runs: {str(e)}")
    
    def list_by_workflow(self, workflow_id: str) -> List[WorkflowRunEntity]:
        """List runs for a specific workflow."""
        try:
            all_runs = self.list_all()
            return [run for run in all_runs if run.workflow_id == workflow_id]
        except Exception as e:
            self._logger.error(f"Error listing runs for workflow {workflow_id}: {e}")
            raise RepositoryError(f"Failed to list workflow runs: {str(e)}")
    
    def list_by_status(self, status: WorkflowStatus) -> List[WorkflowRunEntity]:
        """List runs by status."""
        try:
            all_runs = self.list_all()
            return [run for run in all_runs if run.status == status]
        except Exception as e:
            self._logger.error(f"Error listing runs by status {status}: {e}")
            raise RepositoryError(f"Failed to list workflow runs: {str(e)}")
    
    def _entity_to_dict(self, run: WorkflowRunEntity) -> Dict[str, Any]:
        """Convert workflow run entity to dictionary."""
        return {
            "run_id": run.run_id,
            "workflow_id": run.workflow_id,
            "status": run.status.value,
            "execution_environment": run.execution_environment.value,
            "input_parameters": run.input_parameters,
            "location_context": run.location_context,
            "submitted_at": run.submitted_at.isoformat() if run.submitted_at else None,
            "started_at": run.started_at.isoformat() if run.started_at else None,
            "completed_at": run.completed_at.isoformat() if run.completed_at else None,
            "current_step": run.current_step,
            "completed_steps": run.completed_steps,
            "failed_steps": run.failed_steps,
            "step_results": run.step_results,
            "error_message": run.error_message,
            "retry_count": run.retry_count,
            "max_retries": run.max_retries,
            "resource_usage": run.resource_usage,
            "output_files": run.output_files,
            "output_locations": run.output_locations,
            "_uid": run._uid,
            "_total_steps": getattr(run, '_total_steps', 0)
        }
    
    def _dict_to_entity(self, data: Dict[str, Any]) -> WorkflowRunEntity:
        """Convert dictionary to workflow run entity."""
        run = WorkflowRunEntity(
            run_id=data["run_id"],
            workflow_id=data["workflow_id"],
            status=WorkflowStatus(data.get("status", "draft")),
            execution_environment=ExecutionEnvironment(data.get("execution_environment", "local")),
            input_parameters=data.get("input_parameters", {}),
            location_context=data.get("location_context", {}),
            submitted_at=datetime.fromisoformat(data["submitted_at"]) if data.get("submitted_at") else None,
            started_at=datetime.fromisoformat(data["started_at"]) if data.get("started_at") else None,
            completed_at=datetime.fromisoformat(data["completed_at"]) if data.get("completed_at") else None,
            current_step=data.get("current_step"),
            completed_steps=data.get("completed_steps", []),
            failed_steps=data.get("failed_steps", []),
            step_results=data.get("step_results", {}),
            error_message=data.get("error_message"),
            retry_count=data.get("retry_count", 0),
            max_retries=data.get("max_retries", 3),
            resource_usage=data.get("resource_usage", {}),
            output_files=data.get("output_files", []),
            output_locations=data.get("output_locations", {})
        )
        
        # Set internal fields if present
        if "_uid" in data:
            run._uid = data["_uid"]
        if "_total_steps" in data:
            run.set_total_steps(data["_total_steps"])
        
        return run


class JsonWorkflowTemplateRepository(IWorkflowTemplateRepository):
    """JSON-based workflow template repository implementation."""
    
    def __init__(self, templates_file: str = "workflow_templates.json"):
        """
        Initialize JSON workflow template repository.
        
        Args:
            templates_file: Path to JSON file storing template data
        """
        self.templates_file = Path(templates_file)
        self._logger = logger
        self._ensure_file_exists()
    
    def _ensure_file_exists(self) -> None:
        """Ensure the templates JSON file exists."""
        if not self.templates_file.exists():
            self.templates_file.write_text("{}")
    
    def _load_templates(self) -> Dict[str, Dict[str, Any]]:
        """Load templates from JSON file."""
        try:
            with open(self.templates_file, 'r') as f:
                return json.load(f)
        except json.JSONDecodeError as e:
            self._logger.error(f"Error loading templates JSON: {e}")
            return {}
        except Exception as e:
            self._logger.error(f"Error reading templates file: {e}")
            raise RepositoryError(f"Failed to load templates: {str(e)}")
    
    def _save_templates(self, templates: Dict[str, Dict[str, Any]]) -> None:
        """Save templates to JSON file."""
        try:
            with open(self.templates_file, 'w') as f:
                json.dump(templates, f, indent=2, default=str)
        except Exception as e:
            self._logger.error(f"Error saving templates: {e}")
            raise RepositoryError(f"Failed to save templates: {str(e)}")
    
    def save(self, template: WorkflowTemplateEntity) -> None:
        """Save a workflow template entity."""
        try:
            templates = self._load_templates()
            templates[template.template_id] = self._entity_to_dict(template)
            self._save_templates(templates)
            self._logger.debug(f"Saved workflow template: {template.template_id}")
        except Exception as e:
            self._logger.error(f"Error saving template {template.template_id}: {e}")
            raise RepositoryError(f"Failed to save template: {str(e)}")
    
    def get_by_id(self, template_id: str) -> Optional[WorkflowTemplateEntity]:
        """Get template by ID."""
        try:
            templates = self._load_templates()
            template_data = templates.get(template_id)
            
            if template_data is None:
                return None
            
            return self._dict_to_entity(template_data)
        except Exception as e:
            self._logger.error(f"Error getting template {template_id}: {e}")
            raise RepositoryError(f"Failed to get template: {str(e)}")
    
    def exists(self, template_id: str) -> bool:
        """Check if template exists."""
        templates = self._load_templates()
        return template_id in templates
    
    def delete(self, template_id: str) -> bool:
        """Delete a template."""
        try:
            templates = self._load_templates()
            
            if template_id not in templates:
                return False
            
            del templates[template_id]
            self._save_templates(templates)
            self._logger.debug(f"Deleted template: {template_id}")
            return True
            
        except Exception as e:
            self._logger.error(f"Error deleting template {template_id}: {e}")
            raise RepositoryError(f"Failed to delete template: {str(e)}")
    
    def list_all(self) -> List[WorkflowTemplateEntity]:
        """List all templates."""
        try:
            templates = self._load_templates()
            return [
                self._dict_to_entity(template_data)
                for template_data in templates.values()
            ]
        except Exception as e:
            self._logger.error(f"Error listing templates: {e}")
            raise RepositoryError(f"Failed to list templates: {str(e)}")
    
    def list_by_category(self, category: str) -> List[WorkflowTemplateEntity]:
        """List templates by category."""
        try:
            all_templates = self.list_all()
            return [tmpl for tmpl in all_templates if tmpl.category == category]
        except Exception as e:
            self._logger.error(f"Error listing templates by category {category}: {e}")
            raise RepositoryError(f"Failed to list templates: {str(e)}")
    
    def _entity_to_dict(self, template: WorkflowTemplateEntity) -> Dict[str, Any]:
        """Convert template entity to dictionary."""
        return {
            "template_id": template.template_id,
            "name": template.name,
            "description": template.description,
            "category": template.category,
            "template_parameters": template.template_parameters,
            "workflow_template": template.workflow_template,
            "version": template.version,
            "author": template.author,
            "tags": list(template.tags),
            "usage_count": template.usage_count,
            "_uid": template._uid
        }
    
    def _dict_to_entity(self, data: Dict[str, Any]) -> WorkflowTemplateEntity:
        """Convert dictionary to template entity."""
        template = WorkflowTemplateEntity(
            template_id=data["template_id"],
            name=data["name"],
            description=data.get("description"),
            category=data.get("category"),
            template_parameters=data.get("template_parameters", {}),
            workflow_template=data.get("workflow_template", {}),
            version=data.get("version", "1.0"),
            author=data.get("author"),
            tags=set(data.get("tags", [])),
            usage_count=data.get("usage_count", 0)
        )
        
        # Set internal UID if present
        if "_uid" in data:
            template._uid = data["_uid"]
        
        return template