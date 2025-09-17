"""
Application service for simulation template management.

Provides business logic for creating, managing, and applying simulation templates
to enable efficient creation of simulation series with consistent patterns.
"""

import re
import os
from typing import Dict, List, Optional, Any, Set
from pathlib import Path
from loguru import logger

from ..dtos import (CreateSimulationTemplateDto, SimulationTemplateDto, TemplateListDto,
                   ScanResultDto, ApplyTemplateDto, CreateSimulationDto, SimulationLocationAssociationDto)
from ..exceptions import EntityNotFoundError, EntityAlreadyExistsError, ValidationError
from ...domain.entities.simulation_template import SimulationTemplate
from ...infrastructure.repositories.template_repository import TemplateRepository


class TemplateApplicationService:
    """
    Application service for template operations.

    Coordinates template creation, pattern matching, scanning, and simulation generation
    while maintaining clean separation between domain logic and infrastructure concerns.
    """

    def __init__(
        self,
        template_repo: TemplateRepository,
        simulation_service: 'SimulationApplicationService'
    ) -> None:
        """
        Initialize template service.

        Args:
            template_repo: Repository for template persistence
            simulation_service: Service for creating simulations from templates
        """
        self._template_repo = template_repo
        self._simulation_service = simulation_service
        self._logger = logger.bind(service="TemplateService")

    def create_template(self, dto: CreateSimulationTemplateDto) -> SimulationTemplateDto:
        """
        Create a new simulation template.

        Args:
            dto: Template creation data

        Returns:
            Created template DTO

        Raises:
            EntityAlreadyExistsError: If template name already exists
            ValidationError: If template data is invalid
        """
        self._logger.info(f"Creating template: {dto.name}")

        try:
            # Check if template already exists
            if self._template_repo.exists_by_name(dto.name):
                raise EntityAlreadyExistsError("Template", dto.name)

            # Create domain entity
            template = SimulationTemplate(
                name=dto.name,
                description=dto.description,
                pattern=dto.pattern,
                variables=dto.variables,
                default_attrs=dto.default_attrs,
                default_model_id=dto.default_model_id,
                location_associations=dto.location_associations,
                tags=dto.tags
            )

            # Persist template
            saved_template = self._template_repo.save(template)

            self._logger.info(f"Successfully created template: {dto.name}")
            return self._entity_to_dto(saved_template)

        except ValueError as e:
            raise ValidationError(f"Invalid template data: {str(e)}")
        except Exception as e:
            self._logger.error(f"Error creating template: {str(e)}")
            raise

    def create_template_from_simulation(
        self,
        simulation_id: str,
        template_name: str,
        pattern: str,
        variables: Optional[Dict[str, Dict[str, Any]]] = None,
        description: Optional[str] = None
    ) -> SimulationTemplateDto:
        """
        Create a template from an existing simulation.

        Args:
            simulation_id: ID of simulation to use as template source
            template_name: Name for the new template
            pattern: Naming pattern with variable placeholders
            variables: Variable definitions
            description: Template description

        Returns:
            Created template DTO

        Examples:
            >>> service.create_template_from_simulation(
            ...     "Eem125-S2",
            ...     "eem-series",
            ...     "Eem{time_period}-S2",
            ...     {"time_period": {"type": "int", "range": [120, 130]}}
            ... )
        """
        self._logger.info(f"Creating template '{template_name}' from simulation '{simulation_id}'")

        # Get source simulation
        try:
            simulation = self._simulation_service.get_simulation(simulation_id)
        except EntityNotFoundError:
            raise EntityNotFoundError("Simulation", simulation_id)

        # Extract variable values from the simulation ID using the pattern
        temp_template = SimulationTemplate(
            name=template_name,
            pattern=pattern,
            variables=variables or {}
        )

        extracted_vars = temp_template.extract_variables_from_string(simulation_id)
        if not extracted_vars:
            raise ValidationError(f"Pattern '{pattern}' does not match simulation ID '{simulation_id}'")

        # Create template DTO with simulation data
        template_dto = CreateSimulationTemplateDto(
            name=template_name,
            description=description or f"Template created from simulation {simulation_id}",
            pattern=pattern,
            variables=variables or {},
            default_attrs=simulation.attributes.copy(),
            default_model_id=getattr(simulation, 'model_id', None),
            location_associations=getattr(simulation, 'locations', {})
        )

        return self.create_template(template_dto)

    def list_templates(self, tags: Optional[Set[str]] = None) -> TemplateListDto:
        """
        List all templates with optional tag filtering.

        Args:
            tags: Optional set of tags to filter by

        Returns:
            List of templates
        """
        self._logger.debug("Listing templates")

        templates = self._template_repo.list_all()

        # Filter by tags if provided
        if tags:
            templates = [t for t in templates if tags.intersection(t.tags)]

        template_dtos = [self._entity_to_dto(t) for t in templates]

        return TemplateListDto(
            templates=template_dtos,
            total_count=len(template_dtos)
        )

    def get_template(self, template_name: str) -> SimulationTemplateDto:
        """
        Get a template by name.

        Args:
            template_name: Name of template to retrieve

        Returns:
            Template DTO

        Raises:
            EntityNotFoundError: If template not found
        """
        template = self._template_repo.get_by_name(template_name)
        if not template:
            raise EntityNotFoundError("Template", template_name)

        return self._entity_to_dto(template)

    def apply_template(self, dto: ApplyTemplateDto) -> str:
        """
        Apply a template to create a new simulation.

        Args:
            dto: Template application data

        Returns:
            ID of created simulation

        Raises:
            EntityNotFoundError: If template not found
            ValidationError: If variable values are invalid
        """
        self._logger.info(f"Applying template '{dto.template_name}'")

        # Get template
        template = self._template_repo.get_by_name(dto.template_name)
        if not template:
            raise EntityNotFoundError("Template", dto.template_name)

        # Validate variable values
        errors = template.validate_variable_values(dto.variable_values)
        if errors:
            raise ValidationError(f"Invalid variable values: {'; '.join(errors)}")

        # Generate simulation ID
        simulation_id = template.generate_simulation_id(dto.variable_values)

        # Create simulation attributes
        attrs = template.create_simulation_attrs(dto.variable_values)
        attrs.update(dto.override_attrs)

        # Create simulation
        create_sim_dto = CreateSimulationDto(
            simulation_id=simulation_id,
            model_id=template.default_model_id,
            attrs=attrs
        )

        created_simulation = self._simulation_service.create_simulation(create_sim_dto)

        # Associate locations if template has them
        if template.location_associations:
            location_names = list(template.location_associations.keys())

            # Apply location overrides
            context_overrides = {}
            for loc_name, loc_config in template.location_associations.items():
                # Start with template configuration
                context_overrides[loc_name] = loc_config.copy()

                # Apply any overrides from DTO
                if loc_name in dto.location_overrides:
                    context_overrides[loc_name].update(dto.location_overrides[loc_name])

            # Create location association
            assoc_dto = SimulationLocationAssociationDto(
                simulation_id=simulation_id,
                location_names=location_names,
                context_overrides=context_overrides
            )

            self._simulation_service.associate_locations(assoc_dto)

        self._logger.info(f"Successfully created simulation '{simulation_id}' from template")
        return simulation_id

    def scan_for_simulations(
        self,
        scan_path: str,
        template_names: Optional[List[str]] = None,
        pattern: Optional[str] = None
    ) -> ScanResultDto:
        """
        Scan a filesystem path for potential simulations.

        Args:
            scan_path: Path to scan for simulations
            template_names: Optional list of template names to match against
            pattern: Optional pattern to match directory names

        Returns:
            Scan results with discovered simulations and template matches

        Examples:
            >>> # Scan with specific templates
            >>> results = service.scan_for_simulations(
            ...     "/path/to/simulations",
            ...     template_names=["eem-series"]
            ... )

            >>> # Scan with custom pattern
            >>> results = service.scan_for_simulations(
            ...     "/path/to/simulations",
            ...     pattern="Eem*-S2"
            ... )
        """
        self._logger.info(f"Scanning path: {scan_path}")

        discovered = []
        template_matches = {}
        warnings = []

        try:
            scan_path_obj = Path(scan_path)
            if not scan_path_obj.exists():
                warnings.append(f"Scan path does not exist: {scan_path}")
                return ScanResultDto(
                    path=scan_path,
                    discovered_simulations=[],
                    template_matches={},
                    scan_summary={"total_found": 0, "template_matches": 0},
                    warnings=warnings
                )

            # Get templates to match against
            templates = []
            if template_names:
                for name in template_names:
                    try:
                        template = self._template_repo.get_by_name(name)
                        if template:
                            templates.append(template)
                        else:
                            warnings.append(f"Template not found: {name}")
                    except Exception as e:
                        warnings.append(f"Error loading template {name}: {str(e)}")
            else:
                # Use all templates if none specified
                templates = self._template_repo.list_all()

            # Scan directory entries
            try:
                for entry in scan_path_obj.iterdir():
                    if entry.is_dir():
                        dir_name = entry.name

                        # Check if matches custom pattern
                        pattern_match = True
                        if pattern:
                            # Convert shell pattern to regex
                            regex_pattern = pattern.replace('*', '.*').replace('?', '.')
                            pattern_match = re.match(f"^{regex_pattern}$", dir_name) is not None

                        if pattern_match:
                            discovered.append({
                                "name": dir_name,
                                "path": str(entry),
                                "is_directory": True
                            })

                            # Check against templates
                            for template in templates:
                                extracted_vars = template.extract_variables_from_string(dir_name)
                                if extracted_vars:
                                    if template.name not in template_matches:
                                        template_matches[template.name] = []

                                    template_matches[template.name].append({
                                        "simulation_name": dir_name,
                                        "path": str(entry),
                                        "variables": extracted_vars,
                                        "template_pattern": template.pattern
                                    })

            except PermissionError:
                warnings.append(f"Permission denied accessing: {scan_path}")
            except Exception as e:
                warnings.append(f"Error scanning directory: {str(e)}")

        except Exception as e:
            warnings.append(f"Error during scan: {str(e)}")

        # Create summary
        total_template_matches = sum(len(matches) for matches in template_matches.values())
        summary = {
            "total_found": len(discovered),
            "template_matches": total_template_matches,
            "templates_used": len(template_matches),
            "scan_path": scan_path
        }

        self._logger.info(f"Scan complete: {len(discovered)} items found, {total_template_matches} template matches")

        return ScanResultDto(
            path=scan_path,
            discovered_simulations=discovered,
            template_matches=template_matches,
            scan_summary=summary,
            warnings=warnings
        )

    def bulk_import_from_scan(
        self,
        scan_result: ScanResultDto,
        template_name: str,
        auto_create: bool = False
    ) -> List[str]:
        """
        Bulk import simulations from scan results using a template.

        Args:
            scan_result: Results from a previous scan operation
            template_name: Template to use for import
            auto_create: Whether to automatically create simulations

        Returns:
            List of created simulation IDs

        Raises:
            EntityNotFoundError: If template not found
        """
        self._logger.info(f"Bulk importing from scan using template '{template_name}'")

        # Get template
        template = self._template_repo.get_by_name(template_name)
        if not template:
            raise EntityNotFoundError("Template", template_name)

        created_ids = []

        if template_name in scan_result.template_matches:
            matches = scan_result.template_matches[template_name]

            for match in matches:
                try:
                    if auto_create:
                        # Apply template with extracted variables
                        apply_dto = ApplyTemplateDto(
                            template_name=template_name,
                            variable_values=match["variables"]
                        )

                        simulation_id = self.apply_template(apply_dto)
                        created_ids.append(simulation_id)
                        self._logger.info(f"Created simulation: {simulation_id}")

                except Exception as e:
                    self._logger.warning(f"Failed to create simulation for {match['simulation_name']}: {str(e)}")

        self._logger.info(f"Bulk import complete: {len(created_ids)} simulations created")
        return created_ids

    def _entity_to_dto(self, template: SimulationTemplate) -> SimulationTemplateDto:
        """Convert template entity to DTO."""
        return SimulationTemplateDto(
            template_id=template.template_id,
            name=template.name,
            description=template.description,
            pattern=template.pattern,
            variables=template.variables,
            default_attrs=template.default_attrs,
            default_model_id=template.default_model_id,
            location_associations=template.location_associations,
            created_by=template.created_by,
            tags=template.tags
        )