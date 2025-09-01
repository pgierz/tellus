"""
Simulation-aware Workflow Service - Integrates workflows with simulation context.

This service extends workflow functionality to automatically integrate with
simulation and location context for Earth science workflows.
"""

import logging
from typing import Any, Dict, List, Optional

from ...domain.entities.location import LocationEntity
from ...domain.entities.simulation import SimulationEntity
from ...domain.entities.workflow import WorkflowEntity
from ...infrastructure.repositories.json_location_repository import \
    ILocationRepository
from ...infrastructure.repositories.json_simulation_repository import \
    ISimulationRepository
from .workflow_service import WorkflowApplicationService

logger = logging.getLogger(__name__)


class SimulationWorkflowService:
    """Service for managing simulation-aware workflows."""
    
    def __init__(
        self,
        workflow_service: WorkflowApplicationService,
        simulation_repository: ISimulationRepository,
        location_repository: ILocationRepository
    ):
        self.workflow_service = workflow_service
        self.simulation_repository = simulation_repository
        self.location_repository = location_repository
    
    async def create_workflow_for_simulation(
        self,
        workflow: WorkflowEntity,
        simulation_id: str,
        location_ids: Optional[List[str]] = None
    ) -> WorkflowEntity:
        """
        Create a workflow with simulation and location context.
        
        Args:
            workflow: The workflow entity to create
            simulation_id: ID of the simulation to associate with
            location_ids: Optional list of location IDs to include in context
        
        Returns:
            The created workflow with context applied
        """
        # Get simulation entity
        simulation = await self.simulation_repository.get_by_id(simulation_id)
        if not simulation:
            raise ValueError(f"Simulation '{simulation_id}' not found")
        
        # Build simulation context
        simulation_context = self._build_simulation_context(simulation)
        
        # Build location context if location IDs provided
        location_context = {}
        if location_ids:
            location_context = await self._build_location_context(location_ids)
        
        # Apply context to workflow
        workflow.simulation_id = simulation_id
        workflow.set_simulation_context(simulation_context)
        workflow.set_location_context(location_context)
        
        # Create workflow through the base service
        return await self.workflow_service.create_workflow(workflow)
    
    async def get_simulation_workflows(self, simulation_id: str) -> List[WorkflowEntity]:
        """Get all workflows associated with a simulation."""
        workflows = await self.workflow_service.list_workflows()
        return [w for w in workflows if w.simulation_id == simulation_id]
    
    async def update_workflow_simulation_context(
        self,
        workflow_id: str,
        simulation_id: Optional[str] = None,
        location_ids: Optional[List[str]] = None
    ) -> WorkflowEntity:
        """Update the simulation and location context for an existing workflow."""
        workflow = await self.workflow_service.get_workflow(workflow_id)
        if not workflow:
            raise ValueError(f"Workflow '{workflow_id}' not found")
        
        # Update simulation context if provided
        if simulation_id:
            simulation = await self.simulation_repository.get_by_id(simulation_id)
            if not simulation:
                raise ValueError(f"Simulation '{simulation_id}' not found")
            
            simulation_context = self._build_simulation_context(simulation)
            workflow.simulation_id = simulation_id
            workflow.set_simulation_context(simulation_context)
        
        # Update location context if provided
        if location_ids:
            location_context = await self._build_location_context(location_ids)
            workflow.set_location_context(location_context)
        
        # Save updated workflow
        return await self.workflow_service.update_workflow(workflow_id, workflow)
    
    async def resolve_workflow_paths(self, workflow_id: str) -> Dict[str, str]:
        """
        Resolve all template variables in workflow step commands.
        
        Returns a mapping of step_id -> resolved_command
        """
        workflow = await self.workflow_service.get_workflow(workflow_id)
        if not workflow:
            raise ValueError(f"Workflow '{workflow_id}' not found")
        
        resolved_commands = {}
        for step in workflow.steps:
            resolved_command = workflow.get_resolved_command(step.step_id)
            resolved_commands[step.step_id] = resolved_command
        
        return resolved_commands
    
    async def validate_workflow_context(self, workflow_id: str) -> List[str]:
        """
        Validate that all template variables in the workflow can be resolved.
        
        Returns list of validation errors.
        """
        workflow = await self.workflow_service.get_workflow(workflow_id)
        if not workflow:
            raise ValueError(f"Workflow '{workflow_id}' not found")
        
        errors = []
        available_vars = set(workflow.get_context_variables().keys())
        
        # Check each step's command for unresolved variables
        for step in workflow.steps:
            command = step.command
            
            # Find all template variables in the command
            import re
            variables = re.findall(r'\{([^}]+)\}', command)
            
            for var in variables:
                if var not in available_vars:
                    errors.append(f"Step '{step.step_id}': Unresolved variable '{var}' in command")
        
        return errors
    
    def _build_simulation_context(self, simulation: SimulationEntity) -> Dict[str, Any]:
        """Build context dictionary from simulation entity."""
        context = {
            'simulation_id': simulation.simulation_id,
            'model_id': simulation.model_id,
            'simulation_path': simulation.path,
        }
        
        # Add simulation attributes
        for key, value in simulation.attrs.items():
            context[f'sim_{key}'] = value
        
        # Add common Earth science context variables
        if 'experiment' in simulation.attrs:
            context['experiment'] = simulation.attrs['experiment']
        if 'model' in simulation.attrs:
            context['model'] = simulation.attrs['model']
        if 'resolution' in simulation.attrs:
            context['resolution'] = simulation.attrs['resolution']
        if 'forcing' in simulation.attrs:
            context['forcing'] = simulation.attrs['forcing']
        
        return context
    
    async def _build_location_context(self, location_ids: List[str]) -> Dict[str, Any]:
        """Build context dictionary from location entities."""
        context = {}
        
        for location_id in location_ids:
            location = await self.location_repository.get_by_id(location_id)
            if location:
                prefix = f"loc_{location_id}"
                context[f"{prefix}_path"] = location.get_resolved_path({})
                context[f"{prefix}_kind"] = location.kind.name.lower()
                context[f"{prefix}_protocol"] = location.protocol
                
                # Add location-specific attributes
                for key, value in location.metadata.items():
                    context[f"{prefix}_{key}"] = value
        
        return context