"""
Workflow Application Service - Orchestrates workflow-related use cases.

This service coordinates workflow definition, validation, and basic management
while delegating execution to the WorkflowExecutionService.
"""

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Set

from ...domain.entities.location import LocationEntity
from ...domain.entities.workflow import (ExecutionEnvironment,
                                         ResourceRequirement, WorkflowEngine,
                                         WorkflowEntity, WorkflowStep,
                                         WorkflowTemplateEntity)
from ...domain.repositories.location_repository import ILocationRepository
from ..dtos import (CreateWorkflowDto, CreateWorkflowTemplateDto,
                    FilterOptions, PaginationInfo, ResourceRequirementDto,
                    UpdateWorkflowDto, WorkflowDto, WorkflowInstantiationDto,
                    WorkflowListDto, WorkflowLocationAssociationDto,
                    WorkflowStepDto, WorkflowTemplateDto,
                    WorkflowTemplateListDto)
from ..exceptions import (BusinessRuleViolationError, EntityAlreadyExistsError,
                          EntityNotFoundError, OperationNotAllowedError,
                          ValidationError)

logger = logging.getLogger(__name__)


class IWorkflowRepository:
    """Interface for workflow repository operations."""
    
    def save(self, workflow: WorkflowEntity) -> None:
        """Save a workflow entity."""
        raise NotImplementedError
    
    def get_by_id(self, workflow_id: str) -> Optional[WorkflowEntity]:
        """Get workflow by ID."""
        raise NotImplementedError
    
    def exists(self, workflow_id: str) -> bool:
        """Check if workflow exists."""
        raise NotImplementedError
    
    def delete(self, workflow_id: str) -> bool:
        """Delete a workflow."""
        raise NotImplementedError
    
    def list_all(self) -> List[WorkflowEntity]:
        """List all workflows."""
        raise NotImplementedError


class IWorkflowTemplateRepository:
    """Interface for workflow template repository operations."""
    
    def save(self, template: WorkflowTemplateEntity) -> None:
        """Save a workflow template entity."""
        raise NotImplementedError
    
    def get_by_id(self, template_id: str) -> Optional[WorkflowTemplateEntity]:
        """Get template by ID."""
        raise NotImplementedError
    
    def exists(self, template_id: str) -> bool:
        """Check if template exists."""
        raise NotImplementedError
    
    def delete(self, template_id: str) -> bool:
        """Delete a template."""
        raise NotImplementedError
    
    def list_all(self) -> List[WorkflowTemplateEntity]:
        """List all templates."""
        raise NotImplementedError
    
    def list_by_category(self, category: str) -> List[WorkflowTemplateEntity]:
        """List templates by category."""
        raise NotImplementedError


class WorkflowApplicationService:
    """
    Application service for workflow management.
    
    Handles workflow CRUD operations, template management, validation,
    and integration with location services for Earth science workflows.
    """
    
    def __init__(
        self,
        workflow_repository: IWorkflowRepository,
        template_repository: IWorkflowTemplateRepository,
        location_repository: ILocationRepository
    ):
        """
        Initialize the workflow service.
        
        Args:
            workflow_repository: Repository for workflow persistence
            template_repository: Repository for workflow templates
            location_repository: Repository for location data access
        """
        self._workflow_repo = workflow_repository
        self._template_repo = template_repository
        self._location_repo = location_repository
        self._logger = logger
    
    # Workflow CRUD Operations
    
    def create_workflow(self, dto: CreateWorkflowDto) -> WorkflowDto:
        """
        Create a new workflow.
        
        Args:
            dto: Data transfer object with workflow creation data
            
        Returns:
            Created workflow DTO
            
        Raises:
            EntityAlreadyExistsError: If workflow already exists
            ValidationError: If validation fails
        """
        self._logger.info(f"Creating workflow: {dto.workflow_id}")
        
        try:
            # Check if workflow already exists
            if self._workflow_repo.exists(dto.workflow_id):
                raise EntityAlreadyExistsError("Workflow", dto.workflow_id)
            
            # Convert DTOs to domain objects
            steps = [self._step_dto_to_entity(step_dto) for step_dto in dto.steps]
            
            # Create domain entity
            workflow = WorkflowEntity(
                workflow_id=dto.workflow_id,
                name=dto.name,
                description=dto.description,
                engine=WorkflowEngine(dto.engine),
                workflow_file=dto.workflow_file,
                steps=steps,
                global_parameters=dto.global_parameters.copy(),
                input_schema=dto.input_schema.copy(),
                output_schema=dto.output_schema.copy(),
                tags=dto.tags.copy(),
                version=dto.version,
                author=dto.author
            )
            
            # Persist the workflow
            self._workflow_repo.save(workflow)
            
            self._logger.info(f"Successfully created workflow: {dto.workflow_id}")
            return self._entity_to_dto(workflow)
            
        except ValueError as e:
            raise ValidationError(f"Invalid workflow data: {str(e)}")
        except Exception as e:
            self._logger.error(f"Unexpected error creating workflow: {str(e)}")
            raise
    
    def get_workflow(self, workflow_id: str) -> WorkflowDto:
        """
        Get a workflow by its ID.
        
        Args:
            workflow_id: The ID of the workflow to retrieve
            
        Returns:
            Workflow DTO
            
        Raises:
            EntityNotFoundError: If workflow not found
        """
        self._logger.debug(f"Retrieving workflow: {workflow_id}")
        
        workflow = self._workflow_repo.get_by_id(workflow_id)
        if workflow is None:
            raise EntityNotFoundError("Workflow", workflow_id)
        
        return self._entity_to_dto(workflow)
    
    def update_workflow(self, workflow_id: str, dto: UpdateWorkflowDto) -> WorkflowDto:
        """
        Update an existing workflow.
        
        Args:
            workflow_id: The ID of the workflow to update
            dto: Data transfer object with update data
            
        Returns:
            Updated workflow DTO
            
        Raises:
            EntityNotFoundError: If workflow not found
            ValidationError: If validation fails
        """
        self._logger.info(f"Updating workflow: {workflow_id}")
        
        try:
            # Get existing workflow
            workflow = self._workflow_repo.get_by_id(workflow_id)
            if workflow is None:
                raise EntityNotFoundError("Workflow", workflow_id)
            
            # Apply updates
            if dto.name is not None:
                workflow.name = dto.name
            
            if dto.description is not None:
                workflow.description = dto.description
            
            if dto.engine is not None:
                workflow.engine = WorkflowEngine(dto.engine)
            
            if dto.workflow_file is not None:
                workflow.workflow_file = dto.workflow_file
            
            if dto.steps is not None:
                workflow.steps = [self._step_dto_to_entity(step_dto) for step_dto in dto.steps]
            
            if dto.global_parameters is not None:
                workflow.global_parameters = dto.global_parameters.copy()
            
            if dto.input_schema is not None:
                workflow.input_schema = dto.input_schema.copy()
            
            if dto.output_schema is not None:
                workflow.output_schema = dto.output_schema.copy()
            
            if dto.tags is not None:
                workflow.tags = dto.tags.copy()
            
            if dto.version is not None:
                workflow.version = dto.version
            
            if dto.author is not None:
                workflow.author = dto.author
            
            # Validate the updated entity
            validation_errors = workflow.validate()
            if validation_errors:
                raise ValidationError("Workflow validation failed", errors=validation_errors)
            
            # Persist the changes
            self._workflow_repo.save(workflow)
            
            self._logger.info(f"Successfully updated workflow: {workflow_id}")
            return self._entity_to_dto(workflow)
            
        except ValueError as e:
            raise ValidationError(f"Invalid update data: {str(e)}")
        except Exception as e:
            self._logger.error(f"Repository error updating workflow: {str(e)}")
            raise
    
    def delete_workflow(self, workflow_id: str) -> bool:
        """
        Delete a workflow.
        
        Args:
            workflow_id: The ID of the workflow to delete
            
        Returns:
            True if workflow was deleted, False if it didn't exist
            
        Raises:
            OperationNotAllowedError: If workflow has active runs
        """
        self._logger.info(f"Deleting workflow: {workflow_id}")
        
        # Check if workflow exists
        if not self._workflow_repo.exists(workflow_id):
            self._logger.warning(f"Workflow not found for deletion: {workflow_id}")
            return False
        
        # Business rule: Check for active runs (would need WorkflowRunRepository)
        # For now, we'll implement a simple check
        
        # Delete the workflow
        success = self._workflow_repo.delete(workflow_id)
        
        if success:
            self._logger.info(f"Successfully deleted workflow: {workflow_id}")
        else:
            self._logger.warning(f"Failed to delete workflow: {workflow_id}")
        
        return success
    
    def list_workflows(
        self,
        page: int = 1,
        page_size: int = 50,
        filters: Optional[FilterOptions] = None
    ) -> WorkflowListDto:
        """
        List workflows with pagination and filtering.
        
        Args:
            page: Page number (1-based)
            page_size: Number of workflows per page
            filters: Optional filtering criteria
            
        Returns:
            Paginated list of workflows
        """
        self._logger.debug(f"Listing workflows (page {page}, size {page_size})")
        
        try:
            # Get all workflows (repository would typically handle pagination)
            all_workflows = self._workflow_repo.list_all()
            
            # Apply filters if provided
            if filters:
                all_workflows = self._apply_workflow_filters(all_workflows, filters)
            
            # Calculate pagination
            total_count = len(all_workflows)
            start_idx = (page - 1) * page_size
            end_idx = start_idx + page_size
            
            workflows_page = all_workflows[start_idx:end_idx]
            
            # Convert to DTOs
            workflow_dtos = [self._entity_to_dto(wf) for wf in workflows_page]
            
            # Create pagination info
            pagination = PaginationInfo(
                page=page,
                page_size=page_size,
                total_count=total_count,
                has_next=end_idx < total_count,
                has_previous=page > 1
            )
            
            return WorkflowListDto(
                workflows=workflow_dtos,
                pagination=pagination,
                filters_applied=filters or FilterOptions()
            )
            
        except Exception as e:
            self._logger.error(f"Repository error listing workflows: {str(e)}")
            raise
    
    # Template Operations
    
    def create_template(self, dto: CreateWorkflowTemplateDto) -> WorkflowTemplateDto:
        """
        Create a new workflow template.
        
        Args:
            dto: Template creation data
            
        Returns:
            Created template DTO
            
        Raises:
            EntityAlreadyExistsError: If template already exists
            ValidationError: If validation fails
        """
        self._logger.info(f"Creating workflow template: {dto.template_id}")
        
        try:
            # Check if template already exists
            if self._template_repo.exists(dto.template_id):
                raise EntityAlreadyExistsError("WorkflowTemplate", dto.template_id)
            
            # Create domain entity
            template = WorkflowTemplateEntity(
                template_id=dto.template_id,
                name=dto.name,
                description=dto.description,
                category=dto.category,
                template_parameters=dto.template_parameters.copy(),
                workflow_template=dto.workflow_template.copy(),
                version=dto.version,
                author=dto.author,
                tags=dto.tags.copy()
            )
            
            # Persist the template
            self._template_repo.save(template)
            
            self._logger.info(f"Successfully created workflow template: {dto.template_id}")
            return self._template_entity_to_dto(template)
            
        except ValueError as e:
            raise ValidationError(f"Invalid template data: {str(e)}")
        except Exception as e:
            self._logger.error(f"Unexpected error creating template: {str(e)}")
            raise
    
    def instantiate_workflow_from_template(
        self, dto: WorkflowInstantiationDto
    ) -> WorkflowDto:
        """
        Create a concrete workflow from a template.
        
        Args:
            dto: Instantiation parameters
            
        Returns:
            Created workflow DTO
            
        Raises:
            EntityNotFoundError: If template not found
            EntityAlreadyExistsError: If workflow already exists
            ValidationError: If template parameters invalid
        """
        self._logger.info(f"Instantiating workflow {dto.workflow_id} from template {dto.template_id}")
        
        try:
            # Get template
            template = self._template_repo.get_by_id(dto.template_id)
            if template is None:
                raise EntityNotFoundError("WorkflowTemplate", dto.template_id)
            
            # Check if workflow already exists
            if self._workflow_repo.exists(dto.workflow_id):
                raise EntityAlreadyExistsError("Workflow", dto.workflow_id)
            
            # Instantiate workflow from template
            workflow = template.instantiate_workflow(dto.workflow_id, dto.parameters)
            
            # Apply overrides
            if dto.override_name:
                workflow.name = dto.override_name
            
            if dto.override_description:
                workflow.description = dto.override_description
            
            if dto.additional_tags:
                workflow.tags.update(dto.additional_tags)
            
            # Persist the workflow
            self._workflow_repo.save(workflow)
            
            # Update template usage count
            self._template_repo.save(template)
            
            self._logger.info(f"Successfully instantiated workflow: {dto.workflow_id}")
            return self._entity_to_dto(workflow)
            
        except ValueError as e:
            raise ValidationError(f"Template instantiation failed: {str(e)}")
        except Exception as e:
            self._logger.error(f"Unexpected error instantiating workflow: {str(e)}")
            raise
    
    def get_template(self, template_id: str) -> WorkflowTemplateDto:
        """Get a workflow template by ID."""
        template = self._template_repo.get_by_id(template_id)
        if template is None:
            raise EntityNotFoundError("WorkflowTemplate", template_id)
        
        return self._template_entity_to_dto(template)
    
    def list_templates(
        self,
        page: int = 1,
        page_size: int = 50,
        category: Optional[str] = None,
        filters: Optional[FilterOptions] = None
    ) -> WorkflowTemplateListDto:
        """
        List workflow templates with pagination and filtering.
        """
        self._logger.debug(f"Listing workflow templates (page {page}, size {page_size})")
        
        try:
            # Get templates (by category if specified)
            if category:
                all_templates = self._template_repo.list_by_category(category)
            else:
                all_templates = self._template_repo.list_all()
            
            # Apply filters if provided
            if filters:
                all_templates = self._apply_template_filters(all_templates, filters)
            
            # Calculate pagination
            total_count = len(all_templates)
            start_idx = (page - 1) * page_size
            end_idx = start_idx + page_size
            
            templates_page = all_templates[start_idx:end_idx]
            
            # Convert to DTOs
            template_dtos = [self._template_entity_to_dto(tmpl) for tmpl in templates_page]
            
            # Create pagination info
            pagination = PaginationInfo(
                page=page,
                page_size=page_size,
                total_count=total_count,
                has_next=end_idx < total_count,
                has_previous=page > 1
            )
            
            return WorkflowTemplateListDto(
                templates=template_dtos,
                pagination=pagination,
                filters_applied=filters or FilterOptions()
            )
            
        except Exception as e:
            self._logger.error(f"Repository error listing templates: {str(e)}")
            raise
    
    # Location Integration
    
    def associate_locations(self, dto: WorkflowLocationAssociationDto) -> None:
        """
        Associate a workflow with storage locations.
        
        Args:
            dto: Association data with workflow and location information
            
        Raises:
            EntityNotFoundError: If workflow or location not found
            BusinessRuleViolationError: If association violates business rules
        """
        self._logger.info(f"Associating workflow {dto.workflow_id} with locations: {dto.location_names}")
        
        try:
            # Verify workflow exists
            workflow = self._workflow_repo.get_by_id(dto.workflow_id)
            if workflow is None:
                raise EntityNotFoundError("Workflow", dto.workflow_id)
            
            # Verify all locations exist
            missing_locations = []
            valid_locations = []
            
            for location_name in dto.location_names:
                location = self._location_repo.get_by_name(location_name)
                if location is None:
                    missing_locations.append(location_name)
                else:
                    valid_locations.append(location)
            
            if missing_locations:
                raise EntityNotFoundError(
                    "Location(s)", 
                    ", ".join(missing_locations)
                )
            
            # Business rule validation
            self._validate_workflow_location_associations(workflow, valid_locations)
            
            # Store associations in workflow metadata
            workflow.global_parameters["associated_locations"] = dto.location_names
            workflow.global_parameters["input_location_mapping"] = dto.input_location_mapping
            workflow.global_parameters["output_location_mapping"] = dto.output_location_mapping
            
            if dto.context_overrides:
                workflow.global_parameters["location_context_overrides"] = dto.context_overrides
            
            self._workflow_repo.save(workflow)
            
            self._logger.info(f"Successfully associated workflow {dto.workflow_id} with locations")
            
        except Exception as e:
            self._logger.error(f"Error in workflow location association: {str(e)}")
            raise
    
    def get_workflow_resource_estimates(self, workflow_id: str) -> ResourceRequirementDto:
        """Get estimated resource requirements for a workflow."""
        workflow = self._workflow_repo.get_by_id(workflow_id)
        if workflow is None:
            raise EntityNotFoundError("Workflow", workflow_id)
        
        estimated = workflow.estimate_resources()
        return ResourceRequirementDto(
            cores=estimated.cores,
            memory_gb=estimated.memory_gb,
            disk_gb=estimated.disk_gb,
            gpu_count=estimated.gpu_count,
            walltime_hours=estimated.walltime_hours,
            queue_name=estimated.queue_name,
            custom_requirements=estimated.custom_requirements
        )
    
    def associate_workflow_with_locations(self, dto: WorkflowLocationAssociationDto) -> WorkflowDto:
        """
        Associate a workflow with one or more locations.
        
        Args:
            dto: Data transfer object containing workflow ID, location names, and mappings
            
        Returns:
            Updated workflow DTO
            
        Raises:
            EntityNotFoundError: If workflow or any location doesn't exist
            ValidationError: If association data is invalid
        """
        self._logger.info(f"Associating workflow {dto.workflow_id} with locations: {dto.location_names}")
        
        try:
            # Get the workflow
            workflow = self._workflow_repo.get_by_id(dto.workflow_id)
            if not workflow:
                raise EntityNotFoundError("Workflow", dto.workflow_id)
            
            # Validate that all locations exist
            for location_name in dto.location_names:
                if not self._location_repo.exists(location_name):
                    raise EntityNotFoundError("Location", location_name)
            
            # Associate locations with optional context
            for location_name in dto.location_names:
                context = dto.context_overrides.get(location_name)
                workflow.associate_location(location_name, context)
            
            # Apply step location mappings
            for step_id, location_name in dto.input_location_mapping.items():
                workflow.set_step_input_location(step_id, location_name)
            
            for step_id, location_name in dto.output_location_mapping.items():
                workflow.set_step_output_location(step_id, location_name)
            
            # Save the updated workflow
            self._workflow_repo.save(workflow)
            
            self._logger.info(f"Successfully associated workflow with {len(dto.location_names)} locations")
            return self._entity_to_dto(workflow)
            
        except ValueError as e:
            raise ValidationError(f"Invalid association data: {str(e)}")
        except Exception as e:
            self._logger.error(f"Error during location association: {str(e)}")
            raise
    
    def disassociate_workflow_from_location(self, workflow_id: str, location_name: str) -> WorkflowDto:
        """
        Remove association between a workflow and a location.
        
        Args:
            workflow_id: The ID of the workflow
            location_name: The name of the location to disassociate
            
        Returns:
            Updated workflow DTO
            
        Raises:
            EntityNotFoundError: If workflow doesn't exist
            ValidationError: If location is not associated
        """
        self._logger.info(f"Disassociating workflow {workflow_id} from location {location_name}")
        
        try:
            # Get the workflow
            workflow = self._workflow_repo.get_by_id(workflow_id)
            if not workflow:
                raise EntityNotFoundError("Workflow", workflow_id)
            
            # Check if location is associated
            if not workflow.is_location_associated(location_name):
                raise ValidationError(f"Location '{location_name}' is not associated with workflow '{workflow_id}'")
            
            # Remove the association (this also removes step mappings)
            workflow.disassociate_location(location_name)
            
            # Save the updated workflow
            self._workflow_repo.save(workflow)
            
            self._logger.info(f"Successfully disassociated workflow from location {location_name}")
            return self._entity_to_dto(workflow)
            
        except ValueError as e:
            raise ValidationError(f"Invalid disassociation request: {str(e)}")
        except Exception as e:
            self._logger.error(f"Error during location disassociation: {str(e)}")
            raise

    # Private helper methods
    
    def _step_dto_to_entity(self, step_dto: WorkflowStepDto) -> WorkflowStep:
        """Convert WorkflowStepDto to WorkflowStep entity."""
        resource_req = None
        if step_dto.resource_requirements:
            req_dto = step_dto.resource_requirements
            resource_req = ResourceRequirement(
                cores=req_dto.cores,
                memory_gb=req_dto.memory_gb,
                disk_gb=req_dto.disk_gb,
                gpu_count=req_dto.gpu_count,
                walltime_hours=req_dto.walltime_hours,
                queue_name=req_dto.queue_name,
                custom_requirements=req_dto.custom_requirements.copy()
            )
        
        return WorkflowStep(
            step_id=step_dto.step_id,
            name=step_dto.name,
            command=step_dto.command,
            script_path=step_dto.script_path,
            input_files=step_dto.input_files.copy(),
            output_files=step_dto.output_files.copy(),
            parameters=step_dto.parameters.copy(),
            dependencies=step_dto.dependencies.copy(),
            resource_requirements=resource_req,
            retry_count=step_dto.retry_count,
            max_retries=step_dto.max_retries
        )
    
    def _entity_to_dto(self, workflow: WorkflowEntity) -> WorkflowDto:
        """Convert workflow entity to DTO."""
        steps_dto = []
        for step in workflow.steps:
            resource_dto = None
            if step.resource_requirements:
                req = step.resource_requirements
                resource_dto = ResourceRequirementDto(
                    cores=req.cores,
                    memory_gb=req.memory_gb,
                    disk_gb=req.disk_gb,
                    gpu_count=req.gpu_count,
                    walltime_hours=req.walltime_hours,
                    queue_name=req.queue_name,
                    custom_requirements=req.custom_requirements.copy()
                )
            
            steps_dto.append(WorkflowStepDto(
                step_id=step.step_id,
                name=step.name,
                command=step.command,
                script_path=step.script_path,
                input_files=step.input_files.copy(),
                output_files=step.output_files.copy(),
                parameters=step.parameters.copy(),
                dependencies=step.dependencies.copy(),
                resource_requirements=resource_dto,
                retry_count=step.retry_count,
                max_retries=step.max_retries
            ))
        
        estimated = workflow.estimate_resources()
        estimated_dto = ResourceRequirementDto(
            cores=estimated.cores,
            memory_gb=estimated.memory_gb,
            disk_gb=estimated.disk_gb,
            gpu_count=estimated.gpu_count,
            walltime_hours=estimated.walltime_hours,
            queue_name=estimated.queue_name,
            custom_requirements=estimated.custom_requirements
        )
        
        return WorkflowDto(
            workflow_id=workflow.workflow_id,
            uid=workflow.uid,
            name=workflow.name,
            description=workflow.description,
            engine=workflow.engine.value,
            workflow_file=workflow.workflow_file,
            steps=steps_dto,
            global_parameters=workflow.global_parameters.copy(),
            input_schema=workflow.input_schema.copy(),
            output_schema=workflow.output_schema.copy(),
            tags=workflow.tags.copy(),
            version=workflow.version,
            author=workflow.author,
            created_at=workflow.created_at.isoformat() if workflow.created_at else None,
            estimated_resources=estimated_dto,
            associated_locations=workflow.get_associated_locations(),
            location_contexts=workflow.location_contexts.copy(),
            input_location_mapping=workflow.input_location_mapping.copy(),
            output_location_mapping=workflow.output_location_mapping.copy()
        )
    
    def _template_entity_to_dto(self, template: WorkflowTemplateEntity) -> WorkflowTemplateDto:
        """Convert template entity to DTO."""
        return WorkflowTemplateDto(
            template_id=template.template_id,
            uid=template.uid,
            name=template.name,
            description=template.description,
            category=template.category,
            template_parameters=template.template_parameters.copy(),
            workflow_template=template.workflow_template.copy(),
            version=template.version,
            author=template.author,
            tags=template.tags.copy(),
            usage_count=template.usage_count
        )
    
    def _apply_workflow_filters(
        self,
        workflows: List[WorkflowEntity],
        filters: FilterOptions
    ) -> List[WorkflowEntity]:
        """Apply filtering to workflow list."""
        filtered = workflows
        
        if filters.search_term:
            search_term = filters.search_term.lower()
            filtered = [
                wf for wf in filtered
                if (search_term in wf.workflow_id.lower() or
                    search_term in wf.name.lower() or
                    (wf.description and search_term in wf.description.lower()) or
                    any(search_term in str(v).lower() for v in wf.global_parameters.values()))
            ]
        
        if filters.tags:
            filtered = [
                wf for wf in filtered
                if filters.tags.intersection(wf.tags)
            ]
        
        return filtered
    
    def _apply_template_filters(
        self,
        templates: List[WorkflowTemplateEntity],
        filters: FilterOptions
    ) -> List[WorkflowTemplateEntity]:
        """Apply filtering to template list."""
        filtered = templates
        
        if filters.search_term:
            search_term = filters.search_term.lower()
            filtered = [
                tmpl for tmpl in filtered
                if (search_term in tmpl.template_id.lower() or
                    search_term in tmpl.name.lower() or
                    (tmpl.description and search_term in tmpl.description.lower()) or
                    (tmpl.category and search_term in tmpl.category.lower()))
            ]
        
        if filters.tags:
            filtered = [
                tmpl for tmpl in filtered
                if filters.tags.intersection(tmpl.tags)
            ]
        
        return filtered
    
    def _validate_workflow_location_associations(
        self,
        workflow: WorkflowEntity,
        locations: List[LocationEntity]
    ) -> None:
        """
        Validate business rules for workflow location associations.
        
        Raises:
            BusinessRuleViolationError: If validation fails
        """
        # Business rule: Must have at least one location for input/output
        if not locations:
            raise BusinessRuleViolationError(
                "no_locations",
                "At least one location is required for workflow execution"
            )
        
        # Business rule: For distributed workflows, validate compatible locations
        location_kinds = set()
        for location in locations:
            location_kinds.update(location.kinds)
        
        # Additional validation could include:
        # - Checking that required location kinds are present
        # - Validating storage capacity for expected outputs
        # - Ensuring compute locations can access data locations
        # - Validating network connectivity between locations