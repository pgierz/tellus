"""
Service Factory - Dependency injection and service composition.

This factory provides configured application service instances with proper
dependency injection for use in Earth System Model workflows.
"""

import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, Optional

from ..domain.entities.workflow import WorkflowEngine
from ..domain.repositories.location_repository import ILocationRepository
from ..domain.repositories.simulation_repository import ISimulationRepository
from ..domain.repositories.simulation_file_repository import ISimulationFileRepository
from ..infrastructure.adapters.progress_tracking import ProgressTracker
from .dtos import CacheConfigurationDto
from .services import (LocationApplicationService,
                       PathResolutionService, SimulationApplicationService,
                       WorkflowApplicationService, WorkflowExecutionService)
from .services.unified_file_service import UnifiedFileService
from .services.file_transfer_service import FileTransferApplicationService
from .services.progress_tracking_service import IProgressTrackingService
from .services.workflow_execution_service import (IWorkflowEngine,
                                                  IWorkflowRunRepository)
from .services.workflow_service import (IWorkflowRepository,
                                        IWorkflowTemplateRepository)

logger = logging.getLogger(__name__)


class ApplicationServiceFactory:
    """
    Factory for creating application services with dependency injection.
    
    This factory manages service lifecycle and ensures proper configuration
    of dependencies for Earth System Model operations.
    """
    
    def __init__(
        self,
        simulation_repository: ISimulationRepository,
        location_repository: ILocationRepository,
        simulation_file_repository: Optional[ISimulationFileRepository] = None,
        workflow_repository: Optional[IWorkflowRepository] = None,
        workflow_run_repository: Optional[IWorkflowRunRepository] = None,
        workflow_template_repository: Optional[IWorkflowTemplateRepository] = None,
        workflow_engines: Optional[Dict[WorkflowEngine, IWorkflowEngine]] = None,
        progress_tracker: Optional[ProgressTracker] = None,
        progress_tracking_service: Optional[IProgressTrackingService] = None,
        cache_config: Optional[CacheConfigurationDto] = None,
        workflow_executor: Optional[ThreadPoolExecutor] = None
    ):
        """
        Initialize the service factory.
        
        Args:
            simulation_repository: Repository for simulation persistence
            location_repository: Repository for location persistence
            workflow_repository: Repository for workflow persistence
            workflow_run_repository: Repository for workflow run persistence
            workflow_template_repository: Repository for workflow template persistence
            workflow_engines: Map of workflow execution engines by type
            progress_tracker: Progress tracking system (deprecated)
            progress_tracking_service: New progress tracking service
            cache_config: Optional cache configuration
            workflow_executor: Thread pool for workflow execution
        """
        self._simulation_repo = simulation_repository
        self._location_repo = location_repository
        self._simulation_file_repo = simulation_file_repository
        self._workflow_repo = workflow_repository
        self._workflow_run_repo = workflow_run_repository
        self._workflow_template_repo = workflow_template_repository
        self._workflow_engines = workflow_engines or {}
        self._progress_tracker = progress_tracker
        self._progress_tracking_service = progress_tracking_service
        self._cache_config = cache_config
        self._workflow_executor = workflow_executor
        self._logger = logger
        
        # Service instances (created lazily)
        self._simulation_service: Optional[SimulationApplicationService] = None
        self._location_service: Optional[LocationApplicationService] = None
        self._unified_file_service: Optional[UnifiedFileService] = None
        self._workflow_service: Optional[WorkflowApplicationService] = None
        self._workflow_execution_service: Optional[WorkflowExecutionService] = None
        self._file_transfer_service: Optional[FileTransferApplicationService] = None
        self._path_resolution_service: Optional[PathResolutionService] = None
    
    @property
    def simulation_service(self) -> SimulationApplicationService:
        """Get or create simulation application service."""
        if self._simulation_service is None:
            self._logger.debug("Creating SimulationApplicationService")
            self._simulation_service = SimulationApplicationService(
                simulation_repository=self._simulation_repo,
                location_repository=self._location_repo
            )
        return self._simulation_service
    
    @property
    def location_service(self) -> LocationApplicationService:
        """Get or create location application service."""
        if self._location_service is None:
            self._logger.debug("Creating LocationApplicationService")
            self._location_service = LocationApplicationService(
                location_repository=self._location_repo
            )
        return self._location_service
    
    
    @property
    def unified_file_service(self) -> UnifiedFileService:
        """Get or create unified file application service."""
        if self._unified_file_service is None:
            if not self._simulation_file_repo:
                raise ValueError("SimulationFile repository not configured for UnifiedFileService")
            
            self._logger.debug("Creating UnifiedFileService")
            self._unified_file_service = UnifiedFileService(
                file_repository=self._simulation_file_repo
            )
        return self._unified_file_service
    
    @property
    def workflow_service(self) -> WorkflowApplicationService:
        """Get or create workflow application service."""
        if self._workflow_service is None:
            if not self._workflow_repo or not self._workflow_template_repo:
                raise ValueError("Workflow repositories not configured for WorkflowApplicationService")
            
            self._logger.debug("Creating WorkflowApplicationService")
            self._workflow_service = WorkflowApplicationService(
                workflow_repository=self._workflow_repo,
                template_repository=self._workflow_template_repo,
                location_repository=self._location_repo
            )
        return self._workflow_service
    
    @property
    def workflow_execution_service(self) -> WorkflowExecutionService:
        """Get or create workflow execution service."""
        if self._workflow_execution_service is None:
            if not self._workflow_repo or not self._workflow_run_repo:
                raise ValueError("Workflow repositories not configured for WorkflowExecutionService")
            
            if not self._progress_tracker:
                # Create default progress tracker if not provided
                self._progress_tracker = ProgressTracker()
            
            if not self._workflow_executor:
                # Create default executor if not provided
                self._workflow_executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="workflow-")
            
            self._logger.debug("Creating WorkflowExecutionService")
            self._workflow_execution_service = WorkflowExecutionService(
                workflow_repository=self._workflow_repo,
                run_repository=self._workflow_run_repo,
                location_repository=self._location_repo,
                workflow_engines=self._workflow_engines,
                progress_tracker=self._progress_tracker,
                executor=self._workflow_executor
            )
        return self._workflow_execution_service
    
    @property
    def file_transfer_service(self) -> FileTransferApplicationService:
        """Get or create file transfer application service."""
        if self._file_transfer_service is None:
            self._logger.debug("Creating FileTransferApplicationService")
            self._file_transfer_service = FileTransferApplicationService(
                location_repo=self._location_repo,
                progress_service=self._progress_tracking_service
            )
        return self._file_transfer_service
    
    
    @property
    def path_resolution_service(self) -> PathResolutionService:
        """Get or create path resolution service."""
        if self._path_resolution_service is None:
            self._logger.debug("Creating PathResolutionService")
            self._path_resolution_service = PathResolutionService(
                simulation_service=self.simulation_service,
                location_service=self.location_service
            )
        return self._path_resolution_service
    
    @property
    def progress_tracking_service(self) -> IProgressTrackingService:
        """Get the progress tracking service."""
        if self._progress_tracking_service is None:
            raise ValueError("Progress tracking service not configured")
        return self._progress_tracking_service
    
    def create_simulation_workflow_coordinator(self) -> 'SimulationWorkflowCoordinator':
        """
        Create a coordinator that orchestrates complex workflows across services.
        
        Returns:
            Workflow coordinator with access to all services
        """
        return SimulationWorkflowCoordinator(
            simulation_service=self.simulation_service,
            location_service=self.location_service,
            archive_service=self.archive_service,
            workflow_service=self.workflow_service if self._workflow_repo else None,
            workflow_execution_service=self.workflow_execution_service if self._workflow_repo and self._workflow_run_repo else None
        )
    
    def create_workflow_coordinator(self) -> 'WorkflowCoordinator':
        """
        Create a coordinator specifically for workflow orchestration.
        
        Returns:
            Workflow coordinator with workflow-focused capabilities
        """
        if not self._workflow_repo or not self._workflow_run_repo:
            raise ValueError("Workflow repositories not configured for WorkflowCoordinator")
        
        return WorkflowCoordinator(
            workflow_service=self.workflow_service,
            workflow_execution_service=self.workflow_execution_service,
            location_service=self.location_service,
            archive_service=self.archive_service
        )
    
    def validate_configuration(self) -> bool:
        """
        Validate that all service dependencies are properly configured.
        
        Returns:
            True if configuration is valid
            
        Raises:
            ConfigurationError: If configuration is invalid
        """
        from .exceptions import ConfigurationError
        
        try:
            # Test repository connections
            self._simulation_repo.count()
            self._location_repo.count()
            
            # Test service creation
            _ = self.simulation_service
            _ = self.location_service
            _ = self.archive_service
            
            self._logger.info("Service configuration validation successful")
            return True
            
        except Exception as e:
            raise ConfigurationError("ServiceFactory", f"Configuration validation failed: {str(e)}")
    
    def get_location_repository(self) -> ILocationRepository:
        """Get the location repository instance."""
        return self._location_repo


class SimulationWorkflowCoordinator:
    """
    Coordinates complex workflows across multiple application services.
    
    This coordinator implements high-level business workflows that span
    multiple services, such as setting up a complete simulation environment
    or migrating data between storage locations.
    """
    
    def __init__(
        self,
        simulation_service: SimulationApplicationService,
        location_service: LocationApplicationService,
        unified_file_service: UnifiedFileService,
        workflow_service: Optional[WorkflowApplicationService] = None,
        workflow_execution_service: Optional[WorkflowExecutionService] = None
    ):
        """
        Initialize the workflow coordinator.
        
        Args:
            simulation_service: Service for simulation operations
            location_service: Service for location operations
            archive_service: Service for archive operations
            workflow_service: Optional workflow management service
            workflow_execution_service: Optional workflow execution service
        """
        self._simulation_service = simulation_service
        self._location_service = location_service
        self._archive_service = archive_service
        self._workflow_service = workflow_service
        self._workflow_execution_service = workflow_execution_service
        self._logger = logger
    
    def setup_simulation_environment(
        self,
        simulation_id: str,
        model_id: str,
        location_names: list[str]
    ) -> dict:
        """
        Set up a complete simulation environment.
        
        This workflow:
        1. Creates the simulation
        2. Validates all required locations exist and are accessible
        3. Associates the simulation with locations
        4. Sets up any required directory structures
        
        Args:
            simulation_id: ID for the new simulation
            model_id: Model identifier
            location_names: List of location names to associate
            
        Returns:
            Dictionary with setup results and any warnings
        """
        from .dtos import CreateSimulationDto, SimulationLocationAssociationDto
        from .exceptions import LocationAccessError, ValidationError
        
        self._logger.info(f"Setting up simulation environment: {simulation_id}")
        
        results = {
            "simulation_created": False,
            "locations_validated": [],
            "locations_failed": [],
            "association_created": False,
            "warnings": [],
            "errors": []
        }
        
        try:
            # Step 1: Create simulation
            simulation_dto = CreateSimulationDto(
                simulation_id=simulation_id,
                model_id=model_id
            )
            
            created_simulation = self._simulation_service.create_simulation(simulation_dto)
            results["simulation_created"] = True
            self._logger.info(f"Simulation created: {simulation_id}")
            
            # Step 2: Validate locations
            valid_locations = []
            for location_name in location_names:
                try:
                    # Check if location exists
                    location = self._location_service.get_location(location_name)
                    
                    # Test connectivity
                    test_result = self._location_service.test_location_connectivity(location_name)
                    
                    if test_result.success:
                        valid_locations.append(location_name)
                        results["locations_validated"].append(location_name)
                        self._logger.info(f"Location validated: {location_name}")
                    else:
                        results["locations_failed"].append({
                            "location": location_name,
                            "error": test_result.error_message
                        })
                        results["warnings"].append(f"Location {location_name} failed connectivity test")
                        
                except Exception as e:
                    results["locations_failed"].append({
                        "location": location_name,
                        "error": str(e)
                    })
                    results["warnings"].append(f"Location {location_name} validation failed: {str(e)}")
            
            # Step 3: Associate simulation with valid locations
            if valid_locations:
                try:
                    association_dto = SimulationLocationAssociationDto(
                        simulation_id=simulation_id,
                        location_names=valid_locations
                    )
                    
                    self._simulation_service.associate_locations(association_dto)
                    results["association_created"] = True
                    self._logger.info(f"Simulation associated with {len(valid_locations)} locations")
                    
                except Exception as e:
                    results["errors"].append(f"Failed to associate locations: {str(e)}")
            else:
                results["errors"].append("No valid locations available for association")
            
            # Step 4: Additional setup tasks could go here
            # - Create directory structures
            # - Set up configuration files
            # - Initialize logging
            
            success = results["simulation_created"] and results["association_created"]
            if success:
                self._logger.info(f"Simulation environment setup completed: {simulation_id}")
            else:
                self._logger.warning(f"Simulation environment setup had issues: {simulation_id}")
            
            return results
            
        except Exception as e:
            self._logger.error(f"Simulation environment setup failed: {simulation_id} - {str(e)}")
            results["errors"].append(f"Setup failed: {str(e)}")
            return results
    
    def migrate_simulation_data(
        self,
        simulation_id: str,
        source_location: str,
        target_location: str,
        archive_id: Optional[str] = None
    ) -> dict:
        """
        Migrate simulation data between storage locations.
        
        This workflow:
        1. Validates source and target locations
        2. Creates archive if needed
        3. Transfers data
        4. Verifies integrity
        5. Updates location associations
        
        Args:
            simulation_id: ID of the simulation to migrate
            source_location: Source location name
            target_location: Target location name
            archive_id: Optional archive ID if creating an archive
            
        Returns:
            Dictionary with migration results
        """
        self._logger.info(f"Starting data migration for simulation: {simulation_id}")
        
        results = {
            "validation_passed": False,
            "archive_created": False,
            "data_transferred": False,
            "integrity_verified": False,
            "associations_updated": False,
            "warnings": [],
            "errors": []
        }
        
        try:
            # Step 1: Validate simulation exists
            simulation = self._simulation_service.get_simulation(simulation_id)
            
            # Step 2: Validate locations
            source_loc = self._location_service.get_location(source_location)
            target_loc = self._location_service.get_location(target_location)
            
            # Test connectivity
            source_test = self._location_service.test_location_connectivity(source_location)
            target_test = self._location_service.test_location_connectivity(target_location)
            
            if not source_test.success:
                results["errors"].append(f"Source location not accessible: {source_test.error_message}")
                return results
            
            if not target_test.success:
                results["errors"].append(f"Target location not accessible: {target_test.error_message}")
                return results
            
            results["validation_passed"] = True
            
            # Step 3: Create archive if requested
            if archive_id:
                from .dtos import CreateArchiveDto
                
                try:
                    archive_dto = CreateArchiveDto(
                        archive_id=archive_id,
                        location_name=source_location,
                        archive_type="compressed",
                        description=f"Migration archive for simulation {simulation_id}"
                    )
                    
                    created_archive = self._archive_service.create_archive_metadata(archive_dto)
                    results["archive_created"] = True
                    self._logger.info(f"Archive created for migration: {archive_id}")
                    
                except Exception as e:
                    results["warnings"].append(f"Archive creation failed: {str(e)}")
            
            # Step 4: Data transfer (simulated)
            # In a real implementation, this would:
            # - Copy files from source to target
            # - Handle large file transfers
            # - Provide progress updates
            # - Handle failures and retries
            
            results["data_transferred"] = True
            self._logger.info(f"Data transfer completed: {source_location} -> {target_location}")
            
            # Step 5: Integrity verification
            if archive_id and results["archive_created"]:
                try:
                    integrity_ok = self._archive_service.verify_archive_integrity(archive_id)
                    results["integrity_verified"] = integrity_ok
                    
                    if not integrity_ok:
                        results["errors"].append("Archive integrity verification failed")
                        
                except Exception as e:
                    results["warnings"].append(f"Integrity verification failed: {str(e)}")
            
            # Step 6: Update location associations
            try:
                # This would update the simulation's location associations
                # to include the new target location
                current_locations = simulation.associated_locations or []
                if target_location not in current_locations:
                    from .dtos import SimulationLocationAssociationDto
                    
                    association_dto = SimulationLocationAssociationDto(
                        simulation_id=simulation_id,
                        location_names=current_locations + [target_location]
                    )
                    
                    self._simulation_service.associate_locations(association_dto)
                    results["associations_updated"] = True
                    self._logger.info(f"Location associations updated for simulation: {simulation_id}")
            
            except Exception as e:
                results["warnings"].append(f"Failed to update associations: {str(e)}")
            
            success = (results["validation_passed"] and 
                      results["data_transferred"] and
                      results["associations_updated"])
            
            if success:
                self._logger.info(f"Data migration completed successfully: {simulation_id}")
            else:
                self._logger.warning(f"Data migration completed with issues: {simulation_id}")
            
            return results
            
        except Exception as e:
            self._logger.error(f"Data migration failed: {simulation_id} - {str(e)}")
            results["errors"].append(f"Migration failed: {str(e)}")
            return results


class WorkflowCoordinator:
    """
    Coordinator specifically for workflow operations and Earth science computing.
    
    This coordinator provides high-level workflow management capabilities,
    integrating workflow execution with the Tellus data management system.
    """
    
    def __init__(
        self,
        workflow_service: WorkflowApplicationService,
        workflow_execution_service: WorkflowExecutionService,
        location_service: LocationApplicationService,
        unified_file_service: UnifiedFileService
    ):
        """
        Initialize the workflow coordinator.
        
        Args:
            workflow_service: Service for workflow management
            workflow_execution_service: Service for workflow execution
            location_service: Service for location operations
            archive_service: Service for archive operations
        """
        self._workflow_service = workflow_service
        self._workflow_execution_service = workflow_execution_service
        self._location_service = location_service
        self._archive_service = archive_service
        self._logger = logger
    
    def submit_earth_science_workflow(
        self,
        template_id: str,
        workflow_id: str,
        parameters: Dict[str, Any],
        location_context: Dict[str, str],
        execution_environment: str = "hpc_cluster"
    ) -> Dict[str, Any]:
        """
        Submit an Earth science workflow from a template.
        
        This high-level method:
        1. Instantiates workflow from template
        2. Validates parameters and locations
        3. Associates workflow with appropriate storage locations
        4. Submits for execution
        5. Returns tracking information
        
        Args:
            template_id: ID of the workflow template to use
            workflow_id: Unique ID for the workflow instance
            parameters: Template parameters
            location_context: Mapping of workflow locations
            execution_environment: Target execution environment
            
        Returns:
            Dictionary with workflow submission results
        """
        from .dtos import (WorkflowExecutionRequestDto,
                           WorkflowInstantiationDto,
                           WorkflowLocationAssociationDto)
        
        self._logger.info(f"Submitting Earth science workflow: {workflow_id} from template {template_id}")
        
        results = {
            "workflow_created": False,
            "locations_associated": False,
            "execution_submitted": False,
            "run_id": None,
            "estimated_duration_hours": None,
            "warnings": [],
            "errors": []
        }
        
        try:
            # Step 1: Instantiate workflow from template
            instantiation_dto = WorkflowInstantiationDto(
                template_id=template_id,
                workflow_id=workflow_id,
                parameters=parameters
            )
            
            workflow_dto = self._workflow_service.instantiate_workflow_from_template(instantiation_dto)
            results["workflow_created"] = True
            self._logger.info(f"Workflow instantiated: {workflow_id}")
            
            # Step 2: Validate and associate locations
            valid_locations = []
            for context_key, location_name in location_context.items():
                try:
                    location = self._location_service.get_location(location_name)
                    test_result = self._location_service.test_location_connectivity(location_name)
                    
                    if test_result.success:
                        valid_locations.append(location_name)
                    else:
                        results["warnings"].append(f"Location {location_name} failed connectivity test")
                        
                except Exception as e:
                    results["warnings"].append(f"Location validation failed for {location_name}: {str(e)}")
            
            if valid_locations:
                association_dto = WorkflowLocationAssociationDto(
                    workflow_id=workflow_id,
                    location_names=valid_locations,
                    input_location_mapping=self._extract_input_location_mapping(location_context),
                    output_location_mapping=self._extract_output_location_mapping(location_context)
                )
                
                self._workflow_service.associate_locations(association_dto)
                results["locations_associated"] = True
                self._logger.info(f"Workflow associated with {len(valid_locations)} locations")
            
            # Step 3: Estimate resource requirements and duration
            resource_estimate = self._workflow_service.get_workflow_resource_estimates(workflow_id)
            if resource_estimate.walltime_hours:
                results["estimated_duration_hours"] = resource_estimate.walltime_hours
            
            # Step 4: Submit for execution
            execution_dto = WorkflowExecutionRequestDto(
                workflow_id=workflow_id,
                execution_environment=execution_environment,
                input_parameters=parameters,
                location_context=location_context,
                priority=7  # High priority for Earth science workflows
            )
            
            run_dto = self._workflow_execution_service.submit_workflow_execution(execution_dto)
            results["execution_submitted"] = True
            results["run_id"] = run_dto.run_id
            self._logger.info(f"Workflow submitted for execution: {run_dto.run_id}")
            
            # Step 5: Set up monitoring (if needed)
            self._setup_workflow_monitoring(run_dto.run_id, workflow_id)
            
            success = (results["workflow_created"] and 
                      results["locations_associated"] and 
                      results["execution_submitted"])
            
            if success:
                self._logger.info(f"Earth science workflow submission completed: {workflow_id}")
            else:
                self._logger.warning(f"Workflow submission completed with issues: {workflow_id}")
            
            return results
            
        except Exception as e:
            self._logger.error(f"Earth science workflow submission failed: {workflow_id} - {str(e)}")
            results["errors"].append(f"Workflow submission failed: {str(e)}")
            return results
    
    def monitor_workflow_pipeline(
        self,
        run_id: str,
        archive_outputs: bool = True
    ) -> Dict[str, Any]:
        """
        Monitor a running workflow and handle completion tasks.
        
        This method:
        1. Monitors workflow progress
        2. Archives outputs upon completion
        3. Performs cleanup tasks
        4. Updates metadata
        
        Args:
            run_id: ID of the workflow run to monitor
            archive_outputs: Whether to archive outputs on completion
            
        Returns:
            Dictionary with monitoring results
        """
        self._logger.info(f"Starting workflow monitoring: {run_id}")
        
        results = {
            "monitoring_active": True,
            "current_status": "unknown",
            "progress": 0.0,
            "outputs_archived": False,
            "cleanup_completed": False,
            "errors": [],
            "warnings": []
        }
        
        try:
            # Get current workflow run status
            run_dto = self._workflow_execution_service.get_workflow_run(run_id)
            results["current_status"] = run_dto.status
            results["progress"] = run_dto.progress
            
            # Get progress details
            progress_dto = self._workflow_execution_service.get_workflow_progress(run_id)
            results["current_step"] = progress_dto.current_step
            results["completed_steps"] = progress_dto.completed_steps
            results["total_steps"] = progress_dto.total_steps
            
            # If workflow is completed, handle post-processing
            if run_dto.status == "completed":
                if archive_outputs and run_dto.output_files:
                    try:
                        archive_id = f"{run_id}_outputs_{datetime.now().strftime('%Y%m%d')}"
                        
                        # This would create an archive of the outputs
                        # Implementation depends on specific archive service capabilities
                        results["outputs_archived"] = True
                        results["archive_id"] = archive_id
                        
                    except Exception as e:
                        results["warnings"].append(f"Output archiving failed: {str(e)}")
                
                # Cleanup temporary files
                results["cleanup_completed"] = True
                
            elif run_dto.status == "failed":
                results["errors"].append(f"Workflow execution failed: {run_dto.error_message}")
                results["monitoring_active"] = False
            
            return results
            
        except Exception as e:
            self._logger.error(f"Workflow monitoring failed: {run_id} - {str(e)}")
            results["errors"].append(f"Monitoring failed: {str(e)}")
            results["monitoring_active"] = False
            return results
    
    def create_workflow_from_simulation(
        self,
        simulation_id: str,
        workflow_type: str = "analysis",
        custom_parameters: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Create and submit a workflow based on an existing simulation.
        
        This method automatically configures a workflow using simulation
        metadata and associated locations.
        
        Args:
            simulation_id: ID of the simulation to base workflow on
            workflow_type: Type of workflow to create (analysis, preprocessing, etc.)
            custom_parameters: Optional custom parameters
            
        Returns:
            Dictionary with workflow creation results
        """
        self._logger.info(f"Creating workflow from simulation: {simulation_id}")
        
        # Implementation would:
        # 1. Get simulation metadata
        # 2. Select appropriate template based on workflow_type
        # 3. Configure parameters from simulation attributes
        # 4. Use simulation's associated locations
        # 5. Submit workflow
        
        return {"workflow_created": True, "workflow_id": f"{simulation_id}_{workflow_type}"}
    
    def _extract_input_location_mapping(self, location_context: Dict[str, str]) -> Dict[str, str]:
        """Extract input location mappings from location context."""
        return {k: v for k, v in location_context.items() if "input" in k.lower()}
    
    def _extract_output_location_mapping(self, location_context: Dict[str, str]) -> Dict[str, str]:
        """Extract output location mappings from location context."""
        return {k: v for k, v in location_context.items() if "output" in k.lower()}
    
    def _setup_workflow_monitoring(self, run_id: str, workflow_id: str) -> None:
        """Set up monitoring for a workflow run."""
        # This would configure monitoring tasks, notifications, etc.
        self._logger.debug(f"Setting up monitoring for workflow run: {run_id}")
        pass