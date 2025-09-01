"""
Workflow domain entities for Earth System Model data processing.
"""

import copy
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum, auto
from typing import Any, Dict, List, Optional, Set


class WorkflowType(Enum):
    """Types of Earth science workflows."""

    DATA_PREPROCESSING = auto()
    MODEL_EXECUTION = auto()
    POST_PROCESSING = auto()
    DATA_ANALYSIS = auto()
    QUALITY_CONTROL = auto()
    ARCHIVAL = auto()
    VISUALIZATION = auto()
    CUSTOM = auto()


class WorkflowStatus(Enum):
    """Workflow definition status."""

    DRAFT = auto()
    READY = auto()
    DEPRECATED = auto()
    ARCHIVED = auto()


class RunStatus(Enum):
    """Workflow execution status."""

    QUEUED = auto()
    RUNNING = auto()
    COMPLETED = auto()
    FAILED = auto()
    CANCELLED = auto()
    PAUSED = auto()


class WorkflowEngine(Enum):
    """Workflow execution engines."""

    SNAKEMAKE = auto()
    NEXTFLOW = auto()
    AIRFLOW = auto()
    SLURM = auto()
    BASH = auto()
    PYTHON = auto()


class ExecutionEnvironment(Enum):
    """Execution environments for workflows."""

    LOCAL = auto()
    CLUSTER = auto()
    CLOUD = auto()
    CONTAINER = auto()
    SINGULARITY = auto()
    CONDA = auto()


class WorkflowValidationError(Exception):
    """Raised when workflow validation fails."""

    pass


@dataclass
class ResourceRequirement:
    """Resource requirements for workflow execution."""

    cpu_cores: int = 1
    memory_gb: float = 1.0
    disk_space_gb: float = 1.0
    gpu_count: int = 0
    estimated_runtime: Optional[timedelta] = None
    special_requirements: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        if self.cpu_cores < 1:
            raise ValueError("CPU cores must be at least 1")
        if self.memory_gb <= 0:
            raise ValueError("Memory must be positive")
        if self.disk_space_gb <= 0:
            raise ValueError("Disk space must be positive")
        if self.gpu_count < 0:
            raise ValueError("GPU count cannot be negative")


@dataclass
class WorkflowStep:
    """Individual step within a workflow."""

    step_id: str
    name: str
    command: str
    dependencies: List[str] = field(default_factory=list)
    resource_requirements: Optional[ResourceRequirement] = None
    environment: Dict[str, str] = field(default_factory=dict)
    working_directory: Optional[str] = None
    timeout: Optional[timedelta] = None
    retry_count: int = 0
    retry_delay: timedelta = timedelta(seconds=10)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        if not self.step_id:
            raise ValueError("Step ID cannot be empty")
        if not self.name:
            raise ValueError("Step name cannot be empty")
        if not self.command:
            raise ValueError("Step command cannot be empty")
        if self.retry_count < 0:
            raise ValueError("Retry count cannot be negative")


@dataclass
class WorkflowEntity:
    """Core workflow entity representing a complete workflow definition."""

    workflow_id: str
    name: str
    workflow_type: WorkflowType
    steps: List[WorkflowStep] = field(default_factory=list)
    description: str = ""
    version: str = "1.0"
    author: str = ""
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)
    status: WorkflowStatus = WorkflowStatus.DRAFT
    tags: Set[str] = field(default_factory=set)
    parameters: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)

    # Simulation context integration
    simulation_id: Optional[str] = None
    simulation_context: Dict[str, Any] = field(default_factory=dict)
    
    # Location associations - tracks which locations this workflow can use
    associated_locations: Set[str] = field(default_factory=set)
    
    # Location-specific contexts and configurations
    location_contexts: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    
    # Step-specific location mappings
    input_location_mapping: Dict[str, str] = field(default_factory=dict)  # step_id -> location_name
    output_location_mapping: Dict[str, str] = field(default_factory=dict)  # step_id -> location_name

    def __post_init__(self):
        if not self.workflow_id:
            raise ValueError("Workflow ID cannot be empty")
        if not self.name:
            raise ValueError("Workflow name cannot be empty")
        if not isinstance(self.workflow_type, WorkflowType):
            raise ValueError("Invalid workflow type")
        
        # Validate location data
        if not isinstance(self.associated_locations, set):
            raise ValueError("Associated locations must be a set")
        if not isinstance(self.location_contexts, dict):
            raise ValueError("Location contexts must be a dictionary")
        if not isinstance(self.input_location_mapping, dict):
            raise ValueError("Input location mapping must be a dictionary")
        if not isinstance(self.output_location_mapping, dict):
            raise ValueError("Output location mapping must be a dictionary")

    def validate(self) -> List[str]:
        """
        Validate the workflow definition.

        Returns:
            List of validation error messages (empty if valid)
        """
        errors = []

        if not self.steps:
            errors.append("Workflow must have at least one step")
            return errors

        # Check for duplicate step IDs
        step_ids = [step.step_id for step in self.steps]
        if len(step_ids) != len(set(step_ids)):
            duplicates = [sid for sid in step_ids if step_ids.count(sid) > 1]
            errors.append(f"Duplicate step IDs found: {set(duplicates)}")

        # Validate dependencies
        for step in self.steps:
            for dep in step.dependencies:
                if dep not in step_ids:
                    errors.append(
                        f"Step '{step.step_id}' references unknown dependency '{dep}'"
                    )

        # Check for circular dependencies
        if self._has_circular_dependencies():
            errors.append("Circular dependency detected in workflow")
        
        # Validate location mappings reference existing steps
        for step_id in self.input_location_mapping:
            if step_id not in step_ids:
                errors.append(f"Input location mapping references unknown step '{step_id}'")
        
        for step_id in self.output_location_mapping:
            if step_id not in step_ids:
                errors.append(f"Output location mapping references unknown step '{step_id}'")
        
        # Validate location mappings reference associated locations
        for step_id, location_name in self.input_location_mapping.items():
            if location_name not in self.associated_locations:
                errors.append(f"Input location mapping for step '{step_id}' references unassociated location '{location_name}'")
        
        for step_id, location_name in self.output_location_mapping.items():
            if location_name not in self.associated_locations:
                errors.append(f"Output location mapping for step '{step_id}' references unassociated location '{location_name}'")

        return errors

    def _has_circular_dependencies(self) -> bool:
        """Check if the workflow has circular dependencies using DFS."""

        def has_cycle_util(
            step_id: str, visited: Set[str], rec_stack: Set[str]
        ) -> bool:
            visited.add(step_id)
            rec_stack.add(step_id)

            step = self.get_step(step_id)
            if step:
                for dep in step.dependencies:
                    if dep not in visited:
                        if has_cycle_util(dep, visited, rec_stack):
                            return True
                    elif dep in rec_stack:
                        return True

            rec_stack.remove(step_id)
            return False

        visited = set()
        rec_stack = set()

        for step in self.steps:
            if step.step_id not in visited:
                if has_cycle_util(step.step_id, visited, rec_stack):
                    return True

        return False

    def get_step(self, step_id: str) -> Optional[WorkflowStep]:
        """Get a step by its ID."""
        for step in self.steps:
            if step.step_id == step_id:
                return step
        return None

    def add_step(self, step: WorkflowStep) -> None:
        """Add a step to the workflow."""
        if self.get_step(step.step_id):
            raise ValueError(f"Step with ID '{step.step_id}' already exists")
        self.steps.append(step)
        self.updated_at = datetime.now()

    def remove_step(self, step_id: str) -> bool:
        """Remove a step from the workflow."""
        for i, step in enumerate(self.steps):
            if step.step_id == step_id:
                del self.steps[i]
                self.updated_at = datetime.now()
                return True
        return False

    def get_root_steps(self) -> List[WorkflowStep]:
        """Get steps with no dependencies (can run first)."""
        return [step for step in self.steps if not step.dependencies]

    def add_tag(self, tag: str) -> None:
        """Add a tag to the workflow."""
        self.tags.add(tag)
        self.updated_at = datetime.now()

    def remove_tag(self, tag: str) -> bool:
        """Remove a tag from the workflow."""
        if tag in self.tags:
            self.tags.remove(tag)
            self.updated_at = datetime.now()
            return True
        return False

    def resolve_template_variables(self, template: str) -> str:
        """Resolve template variables in a string using workflow parameters."""
        result = template
        for key, value in self.parameters.items():
            placeholder = f"{{{key}}}"
            result = result.replace(placeholder, str(value))
        return result

    def set_simulation_context(self, simulation_context: Dict[str, Any]) -> None:
        """Set simulation context for template resolution."""
        self.simulation_context = simulation_context.copy()
        self.updated_at = datetime.now()

    def associate_location(self, location_name: str, context: Optional[Dict[str, Any]] = None) -> None:
        """
        Associate this workflow with a location.
        
        Args:
            location_name: Name of the location to associate
            context: Optional location-specific context/configuration
            
        Raises:
            ValueError: If location_name is invalid
        """
        if not isinstance(location_name, str) or not location_name.strip():
            raise ValueError("Location name must be a non-empty string")
            
        self.associated_locations.add(location_name)
        
        if context is not None:
            if not isinstance(context, dict):
                raise ValueError("Location context must be a dictionary")
            self.location_contexts[location_name] = copy.deepcopy(context)
        
        self.updated_at = datetime.now()
    
    def disassociate_location(self, location_name: str) -> bool:
        """
        Remove association with a location.
        
        Args:
            location_name: Name of the location to disassociate
            
        Returns:
            True if location was disassociated, False if it wasn't associated
        """
        removed = location_name in self.associated_locations
        self.associated_locations.discard(location_name)
        self.location_contexts.pop(location_name, None)
        
        # Also remove from step mappings
        self.input_location_mapping = {k: v for k, v in self.input_location_mapping.items() if v != location_name}
        self.output_location_mapping = {k: v for k, v in self.output_location_mapping.items() if v != location_name}
        
        if removed:
            self.updated_at = datetime.now()
        return removed
    
    def get_location_context(self, location_name: str) -> Optional[Dict[str, Any]]:
        """
        Get location-specific context/configuration.
        
        Args:
            location_name: Name of the location
            
        Returns:
            Location context dictionary or None if no context is set
        """
        return self.location_contexts.get(location_name)
    
    def update_location_context(self, location_name: str, context: Dict[str, Any]) -> None:
        """
        Update location-specific context/configuration.
        
        Args:
            location_name: Name of the location
            context: New context dictionary
            
        Raises:
            ValueError: If location is not associated or context is invalid
        """
        if location_name not in self.associated_locations:
            raise ValueError(f"Location '{location_name}' is not associated with this workflow")
            
        if not isinstance(context, dict):
            raise ValueError("Location context must be a dictionary")
            
        self.location_contexts[location_name] = copy.deepcopy(context)
        self.updated_at = datetime.now()
    
    def is_location_associated(self, location_name: str) -> bool:
        """
        Check if a location is associated with this workflow.
        
        Args:
            location_name: Name of the location to check
            
        Returns:
            True if location is associated, False otherwise
        """
        return location_name in self.associated_locations
    
    def get_associated_locations(self) -> List[str]:
        """
        Get list of associated location names.
        
        Returns:
            Sorted list of location names
        """
        return sorted(self.associated_locations)
    
    def set_step_input_location(self, step_id: str, location_name: str) -> None:
        """
        Set the input location for a specific step.
        
        Args:
            step_id: ID of the workflow step
            location_name: Name of the location for input data
            
        Raises:
            ValueError: If step doesn't exist or location isn't associated
        """
        if not self.get_step(step_id):
            raise ValueError(f"Step '{step_id}' not found in workflow")
        
        if location_name not in self.associated_locations:
            raise ValueError(f"Location '{location_name}' is not associated with this workflow")
        
        self.input_location_mapping[step_id] = location_name
        self.updated_at = datetime.now()
    
    def set_step_output_location(self, step_id: str, location_name: str) -> None:
        """
        Set the output location for a specific step.
        
        Args:
            step_id: ID of the workflow step
            location_name: Name of the location for output data
            
        Raises:
            ValueError: If step doesn't exist or location isn't associated
        """
        if not self.get_step(step_id):
            raise ValueError(f"Step '{step_id}' not found in workflow")
        
        if location_name not in self.associated_locations:
            raise ValueError(f"Location '{location_name}' is not associated with this workflow")
        
        self.output_location_mapping[step_id] = location_name
        self.updated_at = datetime.now()
    
    def get_step_input_location(self, step_id: str) -> Optional[str]:
        """Get the input location for a step."""
        return self.input_location_mapping.get(step_id)
    
    def get_step_output_location(self, step_id: str) -> Optional[str]:
        """Get the output location for a step."""
        return self.output_location_mapping.get(step_id)

    def resolve_context_variables(self, template: str, location_name: Optional[str] = None) -> str:
        """
        Resolve template variables using simulation and location context.

        Variables are resolved in this order of precedence:
        1. Workflow parameters (highest precedence)
        2. Simulation context (medium precedence)
        3. Location-specific context (lowest precedence)
        
        Args:
            template: Template string with placeholder variables
            location_name: Optional specific location to use context from
        """
        result = template

        # Apply location context first (lowest precedence)
        if location_name and location_name in self.location_contexts:
            for key, value in self.location_contexts[location_name].items():
                placeholder = f"{{{key}}}"
                result = result.replace(placeholder, str(value))
        else:
            # Apply all location contexts if no specific location is provided
            for location_context in self.location_contexts.values():
                for key, value in location_context.items():
                    placeholder = f"{{{key}}}"
                    result = result.replace(placeholder, str(value))

        # Apply simulation context (medium precedence)
        for key, value in self.simulation_context.items():
            placeholder = f"{{{key}}}"
            result = result.replace(placeholder, str(value))

        # Apply workflow parameters (highest precedence)
        for key, value in self.parameters.items():
            placeholder = f"{{{key}}}"
            result = result.replace(placeholder, str(value))

        return result

    def get_resolved_command(self, step_id: str, location_name: Optional[str] = None) -> Optional[str]:
        """Get a step's command with all template variables resolved."""
        step = self.get_step(step_id)
        if not step:
            return None
        
        # Use step's input location if no location specified
        if not location_name:
            location_name = self.get_step_input_location(step_id)
        
        return self.resolve_context_variables(step.command, location_name)

    def get_context_variables(self, location_name: Optional[str] = None) -> Dict[str, Any]:
        """Get all available context variables for template resolution."""
        context = {}
        
        # Add location context
        if location_name and location_name in self.location_contexts:
            context.update(self.location_contexts[location_name])
        else:
            # Merge all location contexts (later ones override earlier ones)
            for location_context in self.location_contexts.values():
                context.update(location_context)
        
        context.update(self.simulation_context)
        context.update(self.parameters)
        return context

    def __eq__(self, other) -> bool:
        """Equality based on workflow ID."""
        if not isinstance(other, WorkflowEntity):
            return False
        return self.workflow_id == other.workflow_id

    def __hash__(self) -> int:
        """Hash based on workflow ID."""
        return hash(self.workflow_id)

    def __str__(self) -> str:
        """String representation."""
        return f"Workflow[{self.workflow_id}]: {self.name} ({self.status.name})"

    def __repr__(self) -> str:
        """Detailed representation."""
        return (
            f"WorkflowEntity(workflow_id='{self.workflow_id}', "
            f"name='{self.name}', type={self.workflow_type.name}, "
            f"steps={len(self.steps)}, status={self.status.name})"
        )


@dataclass
class WorkflowRunEntity:
    """Entity representing a workflow execution instance."""

    run_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    workflow_id: str = ""
    status: RunStatus = RunStatus.QUEUED
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    progress: float = 0.0
    current_step: Optional[str] = None
    parameters: Dict[str, Any] = field(default_factory=dict)
    environment: Dict[str, str] = field(default_factory=dict)
    resource_usage: Dict[str, Any] = field(default_factory=dict)
    step_results: List[Dict[str, Any]] = field(default_factory=list)
    error_message: Optional[str] = None
    logs: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        if not self.workflow_id:
            raise ValueError("Workflow ID cannot be empty")
        if not (0 <= self.progress <= 100):
            raise ValueError("Progress must be between 0 and 100")

    def add_log_entry(self, message: str) -> None:
        """Add a log entry with timestamp."""
        timestamp = datetime.now().isoformat()
        self.logs.append(f"[{timestamp}] {message}")

    def update_progress(
        self, progress: float, current_step: Optional[str] = None
    ) -> None:
        """Update execution progress."""
        if not (0 <= progress <= 100):
            raise ValueError("Progress must be between 0 and 100")

        self.progress = progress
        if current_step:
            self.current_step = current_step

        # Auto-update status based on progress
        if progress == 0 and self.status == RunStatus.QUEUED:
            self.status = RunStatus.RUNNING
            self.start_time = datetime.now()
        elif progress == 100 and self.status == RunStatus.RUNNING:
            self.status = RunStatus.COMPLETED
            self.end_time = datetime.now()

    def mark_failed(self, error_message: str) -> None:
        """Mark the run as failed."""
        self.status = RunStatus.FAILED
        self.error_message = error_message
        self.end_time = datetime.now()
        self.add_log_entry(f"Run failed: {error_message}")

    def mark_cancelled(self) -> None:
        """Mark the run as cancelled."""
        self.status = RunStatus.CANCELLED
        self.end_time = datetime.now()
        self.add_log_entry("Run cancelled by user")

    def get_duration(self) -> Optional[timedelta]:
        """Get execution duration if available."""
        if self.start_time and self.end_time:
            return self.end_time - self.start_time
        elif self.start_time:
            return datetime.now() - self.start_time
        return None

    def __eq__(self, other) -> bool:
        """Equality based on run ID."""
        if not isinstance(other, WorkflowRunEntity):
            return False
        return self.run_id == other.run_id

    def __hash__(self) -> int:
        """Hash based on run ID."""
        return hash(self.run_id)

    def __str__(self) -> str:
        """String representation."""
        duration_str = ""
        duration = self.get_duration()
        if duration:
            duration_str = f" ({duration})"

        return f"WorkflowRun[{self.run_id}]: {self.workflow_id} - {self.status.name} ({self.progress}%){duration_str}"

    def __repr__(self) -> str:
        """Detailed representation."""
        return (
            f"WorkflowRunEntity(run_id='{self.run_id}', "
            f"workflow_id='{self.workflow_id}', status={self.status.name}, "
            f"progress={self.progress}%)"
        )


@dataclass
class WorkflowTemplateEntity:
    """Entity representing a reusable workflow template."""

    template_id: str
    name: str
    description: str
    workflow_type: WorkflowType
    template_steps: List[Dict[str, Any]] = field(default_factory=list)
    default_parameters: Dict[str, Any] = field(default_factory=dict)
    parameter_schema: Dict[str, Any] = field(default_factory=dict)
    version: str = "1.0"
    author: str = ""
    created_at: datetime = field(default_factory=datetime.now)
    tags: Set[str] = field(default_factory=set)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        if not self.template_id:
            raise ValueError("Template ID cannot be empty")
        if not self.name:
            raise ValueError("Template name cannot be empty")

    def instantiate(
        self, workflow_id: str, parameters: Optional[Dict[str, Any]] = None
    ) -> WorkflowEntity:
        """Create a workflow instance from this template."""
        # Merge default parameters with provided parameters
        final_params = self.default_parameters.copy()
        if parameters:
            final_params.update(parameters)

        # Create workflow steps from template
        steps = []
        for step_template in self.template_steps:
            # Replace parameters in step template
            step_dict = self._substitute_parameters(step_template, final_params)

            # Create WorkflowStep
            step = WorkflowStep(
                step_id=step_dict["step_id"],
                name=step_dict["name"],
                command=step_dict["command"],
                dependencies=step_dict.get("dependencies", []),
                resource_requirements=self._parse_resource_requirements(
                    step_dict.get("resource_requirements")
                ),
                environment=step_dict.get("environment", {}),
                working_directory=step_dict.get("working_directory"),
                timeout=self._parse_timedelta(step_dict.get("timeout")),
                retry_count=step_dict.get("retry_count", 0),
                retry_delay=self._parse_timedelta(step_dict.get("retry_delay", "10s")),
                metadata=step_dict.get("metadata", {}),
            )
            steps.append(step)

        # Create workflow instance
        workflow = WorkflowEntity(
            workflow_id=workflow_id,
            name=f"{self.name} - {workflow_id}",
            workflow_type=self.workflow_type,
            steps=steps,
            description=f"Generated from template: {self.template_id}",
            parameters=final_params,
            tags=self.tags.copy(),
            metadata={
                "template_id": self.template_id,
                "template_version": self.version,
            },
        )

        return workflow

    def _substitute_parameters(
        self, template_dict: Dict[str, Any], parameters: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Recursively substitute parameters in a template dictionary."""
        if isinstance(template_dict, dict):
            return {
                k: self._substitute_parameters(v, parameters)
                for k, v in template_dict.items()
            }
        elif isinstance(template_dict, list):
            return [
                self._substitute_parameters(item, parameters) for item in template_dict
            ]
        elif isinstance(template_dict, str):
            # Replace parameter placeholders
            result = template_dict
            for key, value in parameters.items():
                placeholder = f"{{{{{key}}}}}"
                result = result.replace(placeholder, str(value))
            return result
        else:
            return template_dict

    def _parse_resource_requirements(
        self, req_dict: Optional[Dict[str, Any]]
    ) -> Optional[ResourceRequirement]:
        """Parse resource requirements from dictionary."""
        if not req_dict:
            return None

        return ResourceRequirement(
            cpu_cores=req_dict.get("cpu_cores", 1),
            memory_gb=req_dict.get("memory_gb", 1.0),
            disk_space_gb=req_dict.get("disk_space_gb", 1.0),
            gpu_count=req_dict.get("gpu_count", 0),
            estimated_runtime=self._parse_timedelta(req_dict.get("estimated_runtime")),
            special_requirements=req_dict.get("special_requirements", {}),
        )

    def _parse_timedelta(self, time_str: Optional[str]) -> Optional[timedelta]:
        """Parse timedelta from string (e.g., '1h', '30m', '45s')."""
        if not time_str:
            return None

        import re

        # Parse time string like "1h30m45s", "2h", "30m", "120s"
        pattern = r"(?:(\d+)h)?(?:(\d+)m)?(?:(\d+)s)?"
        match = re.match(pattern, time_str.lower())

        if not match:
            return None

        hours = int(match.group(1) or 0)
        minutes = int(match.group(2) or 0)
        seconds = int(match.group(3) or 0)

        return timedelta(hours=hours, minutes=minutes, seconds=seconds)

    def validate_parameters(self, parameters: Dict[str, Any]) -> List[str]:
        """Validate parameters against the schema."""
        errors = []

        # Check required parameters
        for param, schema in self.parameter_schema.items():
            if schema.get("required", False) and param not in parameters:
                errors.append(f"Required parameter '{param}' is missing")

        # Validate parameter types and values
        for param, value in parameters.items():
            if param in self.parameter_schema:
                schema = self.parameter_schema[param]
                param_type = schema.get("type")

                if param_type and not isinstance(value, eval(param_type)):
                    errors.append(f"Parameter '{param}' must be of type {param_type}")

                # Check allowed values
                allowed_values = schema.get("allowed_values")
                if allowed_values and value not in allowed_values:
                    errors.append(
                        f"Parameter '{param}' must be one of {allowed_values}"
                    )

        return errors

    def __eq__(self, other) -> bool:
        """Equality based on template ID."""
        if not isinstance(other, WorkflowTemplateEntity):
            return False
        return self.template_id == other.template_id

    def __hash__(self) -> int:
        """Hash based on template ID."""
        return hash(self.template_id)

    def __str__(self) -> str:
        """String representation."""
        return f"Template[{self.template_id}]: {self.name} v{self.version}"

    def __repr__(self) -> str:
        """Detailed representation."""
        return (
            f"WorkflowTemplateEntity(template_id='{self.template_id}', "
            f"name='{self.name}', type={self.workflow_type.name}, "
            f"version='{self.version}')"
        )
