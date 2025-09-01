"""
Simulation Application Service - Orchestrates simulation-related use cases.

This service coordinates between the domain layer and infrastructure layer,
implementing business workflows and ensuring data consistency.
"""

import logging
import time
from dataclasses import asdict
from typing import Any, Dict, List, Optional, Set

from ...domain.entities.location import LocationEntity
from ...domain.entities.simulation import SimulationEntity
from ...domain.repositories.exceptions import (LocationNotFoundError,
                                               RepositoryError,
                                               SimulationExistsError,
                                               SimulationNotFoundError)
from ...domain.repositories.location_repository import ILocationRepository
from ...domain.repositories.simulation_repository import ISimulationRepository
from ..dtos import (CreateSimulationDto, FileRegistrationDto, FileRegistrationResultDto,
                    FilterOptions, PaginationInfo, SimulationDto, SimulationFileDto,
                    SimulationListDto, SimulationLocationAssociationDto, UpdateSimulationDto)
from ..exceptions import (BusinessRuleViolationError, EntityAlreadyExistsError,
                          EntityNotFoundError, OperationNotAllowedError,
                          ValidationError)

logger = logging.getLogger(__name__)


class SimulationApplicationService:
    """
    Application service for simulation management.
    
    Handles simulation CRUD operations, location associations, validation,
    and complex business workflows in Earth System Model contexts.
    """
    
    def __init__(
        self,
        simulation_repository: ISimulationRepository,
        location_repository: ILocationRepository
    ) -> None:
        """
        Initialize the simulation application service.
        
        Sets up the service with required repositories for data persistence
        and business logic orchestration.
        
        Parameters
        ----------
        simulation_repository : ISimulationRepository
            Repository interface for simulation entity persistence operations.
            Must implement create, read, update, delete operations.
        location_repository : ILocationRepository
            Repository interface for location entity data access.
            Used to validate location associations.
            
        Examples
        --------
        >>> from tellus.infrastructure.repositories import JsonSimulationRepository
        >>> from tellus.infrastructure.repositories import JsonLocationRepository
        >>> sim_repo = JsonSimulationRepository("/tmp/simulations.json")
        >>> loc_repo = JsonLocationRepository("/tmp/locations.json")
        >>> service = SimulationApplicationService(sim_repo, loc_repo)
        >>> service._simulation_repo is not None
        True
        >>> service._location_repo is not None
        True
        
        See Also
        --------
        create_simulation : Create a new simulation
        get_simulation : Retrieve simulation by ID
        """
        self._simulation_repo = simulation_repository
        self._location_repo = location_repository
        self._logger = logger
    
    def create_simulation(self, dto: CreateSimulationDto) -> SimulationDto:
        """
        Create a new simulation with metadata and configuration.
        
        This method validates the input data, creates a simulation entity,
        and persists it to the repository. It handles conflicts with existing
        simulation IDs and enforces business rules.
        
        Parameters
        ----------
        dto : CreateSimulationDto
            Data transfer object containing simulation ID, model information,
            path configuration, and metadata attributes. The simulation_id
            must be unique across all simulations.
            
        Returns
        -------
        SimulationDto
            Complete simulation data including generated UID, timestamps,
            and all provided metadata.
            
        Raises
        ------
        EntityAlreadyExistsError
            If a simulation with the same simulation_id already exists
            in the repository.
        ValidationError
            If the DTO contains invalid data, missing required fields,
            or violates domain constraints.
        RepositoryError
            If there's an error persisting the simulation to storage.
            
        Examples
        --------
        >>> from tellus.application.dtos import CreateSimulationDto
        >>> dto = CreateSimulationDto(
        ...     simulation_id="climate-run-001",
        ...     model_id="CESM2",
        ...     attrs={"experiment": "historical", "years": "1850-2014"}
        ... )
        >>> # service = SimulationApplicationService(repo, location_repo)
        >>> # result = service.create_simulation(dto)
        >>> # result.simulation_id
        >>> # 'climate-run-001'
        >>> # result.model_id  
        >>> # 'CESM2'
        >>> # len(result.attrs)
        >>> # 2
        
        Notes
        -----
        The simulation_id should follow naming conventions appropriate
        for your organization. Common patterns include experiment codes,
        date-based identifiers, or hierarchical naming schemes.
        
        See Also
        --------
        get_simulation : Retrieve an existing simulation
        update_simulation : Modify simulation metadata
        delete_simulation : Remove a simulation
        """
        self._logger.info(f"Creating simulation: {dto.simulation_id}")
        
        try:
            # Check if simulation already exists
            if self._simulation_repo.exists(dto.simulation_id):
                raise EntityAlreadyExistsError("Simulation", dto.simulation_id)
            
            # Create domain entity
            simulation = SimulationEntity(
                simulation_id=dto.simulation_id,
                model_id=dto.model_id,
                path=dto.path,
                attrs=dto.attrs.copy(),
                namelists=dto.namelists.copy(),
                snakemakes=dto.snakemakes.copy()
            )
            
            # Persist the simulation
            self._simulation_repo.save(simulation)
            
            self._logger.info(f"Successfully created simulation: {dto.simulation_id}")
            return self._entity_to_dto(simulation)
            
        except SimulationExistsError as e:
            raise EntityAlreadyExistsError("Simulation", e.simulation_id)
        except ValueError as e:
            raise ValidationError(f"Invalid simulation data: {str(e)}")
        except RepositoryError as e:
            self._logger.error(f"Repository error creating simulation: {str(e)}")
            raise
        except Exception as e:
            self._logger.error(f"Unexpected error creating simulation: {str(e)}")
            raise
    
    def get_simulation(self, simulation_id: str) -> SimulationDto:
        """
        Get a simulation by its ID.
        
        Args:
            simulation_id: The ID of the simulation to retrieve
            
        Returns:
            Simulation DTO
            
        Raises:
            EntityNotFoundError: If simulation not found
        """
        self._logger.debug(f"Retrieving simulation: {simulation_id}")
        
        try:
            simulation = self._simulation_repo.get_by_id(simulation_id)
            if simulation is None:
                return None
            
            return self._entity_to_dto(simulation)
            
        except SimulationNotFoundError as e:
            return None
        except RepositoryError as e:
            self._logger.error(f"Repository error retrieving simulation: {str(e)}")
            raise
    
    def update_simulation(self, simulation_id: str, dto: UpdateSimulationDto) -> SimulationDto:
        """
        Update an existing simulation.
        
        Args:
            simulation_id: The ID of the simulation to update
            dto: Data transfer object with update data
            
        Returns:
            Updated simulation DTO
            
        Raises:
            EntityNotFoundError: If simulation not found
            ValidationError: If validation fails
        """
        self._logger.info(f"Updating simulation: {simulation_id}")
        
        try:
            # Get existing simulation
            simulation = self._simulation_repo.get_by_id(simulation_id)
            if simulation is None:
                raise EntityNotFoundError("Simulation", simulation_id)
            
            # Apply updates
            if dto.model_id is not None:
                simulation.model_id = dto.model_id
            
            if dto.path is not None:
                simulation.path = dto.path
            
            if dto.attrs is not None:
                simulation.attrs.clear()
                simulation.attrs.update(dto.attrs)
            
            if dto.namelists is not None:
                simulation.namelists.clear()
                simulation.namelists.update(dto.namelists)
            
            if dto.snakemakes is not None:
                simulation.snakemakes.clear()
                simulation.snakemakes.update(dto.snakemakes)
            
            # Validate the updated entity
            validation_errors = simulation.validate()
            if validation_errors:
                raise ValidationError("Simulation validation failed", errors=validation_errors)
            
            # Persist the changes
            self._simulation_repo.save(simulation)
            
            self._logger.info(f"Successfully updated simulation: {simulation_id}")
            return self._entity_to_dto(simulation)
            
        except SimulationNotFoundError as e:
            raise EntityNotFoundError("Simulation", e.simulation_id)
        except ValueError as e:
            raise ValidationError(f"Invalid update data: {str(e)}")
        except RepositoryError as e:
            self._logger.error(f"Repository error updating simulation: {str(e)}")
            raise
    
    def delete_simulation(self, simulation_id: str) -> bool:
        """
        Delete a simulation.
        
        Args:
            simulation_id: The ID of the simulation to delete
            
        Returns:
            True if simulation was deleted, False if it didn't exist
            
        Raises:
            OperationNotAllowedError: If simulation has active operations
        """
        self._logger.info(f"Deleting simulation: {simulation_id}")
        
        try:
            # Check if simulation exists
            if not self._simulation_repo.exists(simulation_id):
                self._logger.warning(f"Simulation not found for deletion: {simulation_id}")
                return False
            
            # Business rule: Check for active operations or dependencies
            # This would typically check for running workflows, active archives, etc.
            # For now, we'll implement a simple check
            
            # Delete the simulation
            success = self._simulation_repo.delete(simulation_id)
            
            if success:
                self._logger.info(f"Successfully deleted simulation: {simulation_id}")
            else:
                self._logger.warning(f"Failed to delete simulation: {simulation_id}")
            
            return success
            
        except RepositoryError as e:
            self._logger.error(f"Repository error deleting simulation: {str(e)}")
            raise
    
    def list_simulations(
        self,
        page: int = 1,
        page_size: int = 50,
        filters: Optional[FilterOptions] = None
    ) -> SimulationListDto:
        """
        List simulations with pagination and filtering.
        
        Args:
            page: Page number (1-based)
            page_size: Number of simulations per page
            filters: Optional filtering criteria
            
        Returns:
            Paginated list of simulations
        """
        self._logger.debug(f"Listing simulations (page {page}, size {page_size})")
        
        try:
            # Get all simulations (repository would typically handle pagination)
            all_simulations = self._simulation_repo.list_all()
            
            # Apply filters if provided
            if filters:
                all_simulations = self._apply_filters(all_simulations, filters)
            
            # Calculate pagination
            total_count = len(all_simulations)
            start_idx = (page - 1) * page_size
            end_idx = start_idx + page_size
            
            simulations_page = all_simulations[start_idx:end_idx]
            
            # Convert to DTOs
            simulation_dtos = [self._entity_to_dto(sim) for sim in simulations_page]
            
            # Create pagination info
            pagination = PaginationInfo(
                page=page,
                page_size=page_size,
                total_count=total_count,
                has_next=end_idx < total_count,
                has_previous=page > 1
            )
            
            return SimulationListDto(
                simulations=simulation_dtos,
                pagination=pagination,
                filters_applied=filters or FilterOptions()
            )
            
        except RepositoryError as e:
            self._logger.error(f"Repository error listing simulations: {str(e)}")
            raise
    
    def associate_locations(self, dto: SimulationLocationAssociationDto) -> None:
        """
        Associate a simulation with storage locations.
        
        Args:
            dto: Association data with simulation and location information
            
        Raises:
            EntityNotFoundError: If simulation or location not found
            BusinessRuleViolationError: If association violates business rules
        """
        self._logger.info(f"Associating simulation {dto.simulation_id} with locations: {dto.location_names}")
        
        try:
            # Verify simulation exists
            simulation = self._simulation_repo.get_by_id(dto.simulation_id)
            if simulation is None:
                raise EntityNotFoundError("Simulation", dto.simulation_id)
            
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
            self._validate_location_associations(simulation, valid_locations)
            
            # Store the association using the proper entity methods
            for location_name in dto.location_names:
                # Get context for this location if provided
                location_context = dto.context_overrides.get(location_name, {}) if dto.context_overrides else {}
                
                # Use the entity's associate_location method
                simulation.associate_location(location_name, location_context if location_context else None)
            
            self._simulation_repo.save(simulation)
            
            self._logger.info(f"Successfully associated simulation {dto.simulation_id} with locations")
            
        except (SimulationNotFoundError, LocationNotFoundError) as e:
            if isinstance(e, SimulationNotFoundError):
                raise EntityNotFoundError("Simulation", e.simulation_id)
            else:
                raise EntityNotFoundError("Location", e.name)
        except RepositoryError as e:
            self._logger.error(f"Repository error in location association: {str(e)}")
            raise
    
    def get_simulation_context(self, simulation_id: str) -> Dict[str, str]:
        """
        Get context variables for a simulation (for template rendering).
        
        Args:
            simulation_id: The ID of the simulation
            
        Returns:
            Dictionary of context variables
            
        Raises:
            EntityNotFoundError: If simulation not found
        """
        self._logger.debug(f"Getting context for simulation: {simulation_id}")
        
        try:
            simulation = self._simulation_repo.get_by_id(simulation_id)
            if simulation is None:
                raise EntityNotFoundError("Simulation", simulation_id)

            # Only attributes participate in templating context
            attrs = getattr(simulation, "attrs", {}) or {}
            return {str(k): str(v) for k, v in attrs.items()}

        except SimulationNotFoundError as e:
            raise EntityNotFoundError("Simulation", e.simulation_id)
        except RepositoryError as e:
            self._logger.error(f"Repository error getting simulation context: {str(e)}")
            raise
    
    def add_simulation_attribute(self, simulation_id: str, key: str, value: Any) -> None:
        """
        Add or update a simulation attribute.
        
        Args:
            simulation_id: The ID of the simulation
            key: Attribute key
            value: Attribute value
            
        Raises:
            EntityNotFoundError: If simulation not found
            ValidationError: If attribute is invalid
        """
        self._logger.debug(f"Adding attribute to simulation {simulation_id}: {key}")
        
        try:
            simulation = self._simulation_repo.get_by_id(simulation_id)
            if simulation is None:
                raise EntityNotFoundError("Simulation", simulation_id)
            
            simulation.add_attribute(key, value)
            self._simulation_repo.save(simulation)
            
            self._logger.debug(f"Successfully added attribute {key} to simulation {simulation_id}")
            
        except SimulationNotFoundError as e:
            raise EntityNotFoundError("Simulation", e.simulation_id)
        except ValueError as e:
            raise ValidationError(f"Invalid attribute: {str(e)}")
        except RepositoryError as e:
            self._logger.error(f"Repository error adding simulation attribute: {str(e)}")
            raise
    
    def add_snakemake_rule(self, simulation_id: str, rule_name: str, smk_file: str) -> None:
        """
        Add a Snakemake rule to a simulation.
        
        Args:
            simulation_id: The ID of the simulation
            rule_name: Name of the Snakemake rule
            smk_file: Path to the Snakemake file
            
        Raises:
            EntityNotFoundError: If simulation not found
            ValidationError: If rule is invalid
            BusinessRuleViolationError: If rule already exists
        """
        self._logger.info(f"Adding Snakemake rule to simulation {simulation_id}: {rule_name}")
        
        try:
            simulation = self._simulation_repo.get_by_id(simulation_id)
            if simulation is None:
                raise EntityNotFoundError("Simulation", simulation_id)
            
            simulation.add_snakemake_rule(rule_name, smk_file)
            self._simulation_repo.save(simulation)
            
            self._logger.info(f"Successfully added Snakemake rule {rule_name} to simulation {simulation_id}")
            
        except SimulationNotFoundError as e:
            raise EntityNotFoundError("Simulation", e.simulation_id)
        except ValueError as e:
            if "already exists" in str(e):
                raise BusinessRuleViolationError(
                    "duplicate_snakemake_rule",
                    f"Rule '{rule_name}' already exists for simulation '{simulation_id}'"
                )
            else:
                raise ValidationError(f"Invalid Snakemake rule: {str(e)}")
        except RepositoryError as e:
            self._logger.error(f"Repository error adding Snakemake rule: {str(e)}")
            raise
    
    def associate_simulation_with_locations(self, dto: SimulationLocationAssociationDto) -> SimulationDto:
        """
        Associate a simulation with one or more locations.
        
        Args:
            dto: Data transfer object containing simulation ID, location names, and context
            
        Returns:
            Updated simulation DTO
            
        Raises:
            EntityNotFoundError: If simulation or any location doesn't exist
            ValidationError: If association data is invalid
        """
        self._logger.info(f"Associating simulation {dto.simulation_id} with locations: {dto.location_names}")
        
        try:
            # Get the simulation
            simulation = self._simulation_repo.get_by_id(dto.simulation_id)
            if not simulation:
                raise EntityNotFoundError("Simulation", dto.simulation_id)
            
            # Validate that all locations exist
            for location_name in dto.location_names:
                if not self._location_repo.exists(location_name):
                    raise EntityNotFoundError("Location", location_name)
            
            # Associate locations with optional context
            for location_name in dto.location_names:
                context = dto.context_overrides.get(location_name)
                simulation.associate_location(location_name, context)
            
            # Save the updated simulation
            self._simulation_repo.save(simulation)
            
            self._logger.info(f"Successfully associated simulation with {len(dto.location_names)} locations")
            return self._entity_to_dto(simulation)
            
        except SimulationNotFoundError as e:
            raise EntityNotFoundError("Simulation", e.simulation_id)
        except LocationNotFoundError as e:
            raise EntityNotFoundError("Location", e.location_name)
        except ValueError as e:
            raise ValidationError(f"Invalid association data: {str(e)}")
        except RepositoryError as e:
            self._logger.error(f"Repository error during location association: {str(e)}")
            raise
    
    def disassociate_simulation_from_location(self, simulation_id: str, location_name: str) -> SimulationDto:
        """
        Remove association between a simulation and a location.
        
        Args:
            simulation_id: The ID of the simulation
            location_name: The name of the location to disassociate
            
        Returns:
            Updated simulation DTO
            
        Raises:
            EntityNotFoundError: If simulation doesn't exist
            ValidationError: If location is not associated
        """
        self._logger.info(f"Disassociating simulation {simulation_id} from location {location_name}")
        
        try:
            # Get the simulation
            simulation = self._simulation_repo.get_by_id(simulation_id)
            if not simulation:
                raise EntityNotFoundError("Simulation", simulation_id)
            
            # Check if location is associated
            if not simulation.is_location_associated(location_name):
                raise ValidationError(f"Location '{location_name}' is not associated with simulation '{simulation_id}'")
            
            # Remove the association
            simulation.disassociate_location(location_name)
            
            # Save the updated simulation
            self._simulation_repo.save(simulation)
            
            self._logger.info(f"Successfully disassociated simulation from location {location_name}")
            return self._entity_to_dto(simulation)
            
        except SimulationNotFoundError as e:
            raise EntityNotFoundError("Simulation", e.simulation_id)
        except ValueError as e:
            raise ValidationError(f"Invalid disassociation request: {str(e)}")
        except RepositoryError as e:
            self._logger.error(f"Repository error during location disassociation: {str(e)}")
            raise
    
    def update_simulation_location_context(
        self, 
        simulation_id: str, 
        location_name: str, 
        context: Dict[str, Any]
    ) -> SimulationDto:
        """
        Update location-specific context for a simulation.
        
        Args:
            simulation_id: The ID of the simulation
            location_name: The name of the location
            context: New context dictionary
            
        Returns:
            Updated simulation DTO
            
        Raises:
            EntityNotFoundError: If simulation doesn't exist
            ValidationError: If location is not associated or context is invalid
        """
        self._logger.info(f"Updating location context for simulation {simulation_id}, location {location_name}")
        
        try:
            # Get the simulation
            simulation = self._simulation_repo.get_by_id(simulation_id)
            if not simulation:
                raise EntityNotFoundError("Simulation", simulation_id)
            
            # Update the location context
            simulation.update_location_context(location_name, context)
            
            # Save the updated simulation
            self._simulation_repo.save(simulation)
            
            self._logger.info(f"Successfully updated location context")
            return self._entity_to_dto(simulation)
            
        except SimulationNotFoundError as e:
            raise EntityNotFoundError("Simulation", e.simulation_id)
        except ValueError as e:
            raise ValidationError(f"Invalid context update: {str(e)}")
        except RepositoryError as e:
            self._logger.error(f"Repository error during context update: {str(e)}")
            raise

    # Private helper methods
    
    def _entity_to_dto(self, simulation: SimulationEntity) -> SimulationDto:
        """Convert domain entity to DTO."""
        # Clean user attributes (filter out system-managed data)
        clean_attributes = {k: v for k, v in simulation.attrs.items() 
                           if k != "associated_locations"}
        
        # Build locations from entity location contexts
        locations = {}
        associated_locations = simulation.get_associated_locations()
        if associated_locations:
            for location_name in associated_locations:
                context_data = simulation.get_location_context(location_name)
                if context_data:
                    # Convert LocationContext to dict
                    if hasattr(context_data, 'to_dict'):
                        locations[location_name] = context_data.to_dict()
                    else:
                        locations[location_name] = context_data
                else:
                    # Default empty context
                    locations[location_name] = {"path_prefix": "", "overrides": {}, "metadata": {}}
        
        return SimulationDto(
            simulation_id=simulation.simulation_id,
            uid=simulation.uid,
            attributes=clean_attributes,  # New format: clean user attributes
            locations=locations,  # New format: simplified locations
            namelists=simulation.namelists.copy(),
            workflows=simulation.snakemakes.copy()  # New format: renamed to workflows
        )
    
    def _apply_filters(
        self,
        simulations: List[SimulationEntity],
        filters: FilterOptions
    ) -> List[SimulationEntity]:
        """Apply filtering to simulation list."""
        filtered = simulations
        
        if filters.search_term:
            search_term = filters.search_term.lower()
            filtered = [
                sim for sim in filtered
                if (search_term in sim.simulation_id.lower() or
                    (sim.model_id and search_term in sim.model_id.lower()) or
                    any(search_term in str(v).lower() for v in sim.attrs.values()))
            ]
        
        if filters.tags:
            # Assuming tags are stored as an attribute
            filtered = [
                sim for sim in filtered
                if filters.tags.intersection(set(sim.attrs.get("tags", [])))
            ]
        
        # Additional filtering by creation/modification dates would require
        # extending the domain entity with timestamp fields
        
        return filtered
    
    def _validate_location_associations(
        self,
        simulation: SimulationEntity,
        locations: List[LocationEntity]
    ) -> None:
        """
        Validate business rules for location associations.
        
        Raises:
            BusinessRuleViolationError: If validation fails
        """
        # Business rule: Must have at least one location
        if not locations:
            raise BusinessRuleViolationError(
                "required_location",
                "At least one location is required"
            )
        
        # Business rule: Cannot have conflicting protocols for same location kind
        kind_protocols = {}
        for location in locations:
            protocol = location.get_protocol()
            for kind in location.kinds:
                if kind in kind_protocols and kind_protocols[kind] != protocol:
                    raise BusinessRuleViolationError(
                        "conflicting_protocols",
                        f"Conflicting protocols for location kind {kind.name}: "
                        f"{kind_protocols[kind]} vs {protocol}"
                    )
                kind_protocols[kind] = protocol
        
        # Additional business rules could be added here
        # e.g., validating that required location types are present,
        # checking quotas, validating paths, etc.

    # File Management Methods
    
    def get_simulation_files(self, simulation_id: str) -> List[SimulationFileDto]:
        """
        Get all files associated with a simulation.
        
        Args:
            simulation_id: The ID of the simulation
            
        Returns:
            List of SimulationFileDto objects
            
        Raises:
            EntityNotFoundError: If simulation not found
        """
        from ..dtos import SimulationFileDto
        
        self._logger.debug(f"Getting files for simulation: {simulation_id}")
        
        # Get simulation entity
        entity = self._simulation_repo.get_by_id(simulation_id)
        if entity is None:
            raise EntityNotFoundError("Simulation", simulation_id)
        
        if not entity.has_file_inventory():
            return []
        
        # Convert SimulationFile entities to DTOs
        file_dtos = []
        for simulation_file in entity.get_files():
            file_dto = SimulationFileDto(
                relative_path=simulation_file.relative_path,
                size=simulation_file.size,
                checksum=str(simulation_file.checksum) if simulation_file.checksum else None,
                content_type=simulation_file.content_type.value,
                importance=simulation_file.importance.value,
                file_role=simulation_file.file_role,
                simulation_date=simulation_file.get_simulation_date_string(),
                created_time=simulation_file.created_time,
                modified_time=simulation_file.modified_time,
                source_archive=simulation_file.source_archive,
                extraction_time=simulation_file.extraction_time,
                tags=list(simulation_file.tags),
                attributes=simulation_file.attributes.copy()
            )
            file_dtos.append(file_dto)
        
        self._logger.info(f"Retrieved {len(file_dtos)} files for simulation {simulation_id}")
        return file_dtos
    
    
