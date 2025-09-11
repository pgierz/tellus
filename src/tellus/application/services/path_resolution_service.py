"""
Path Resolution Application Service - Coordinates path resolution between domains.

This service orchestrates path resolution workflows by coordinating between
simulation and location domains without coupling them directly. It handles
template-based path resolution, base path concatenation, and full absolute
path generation for filesystem operations.
"""

import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

from ..exceptions import (EntityNotFoundError, PathResolutionError,
                          ValidationError)
from .location_service import LocationApplicationService
from .simulation_service import SimulationApplicationService

logger = logging.getLogger(__name__)


class PathResolutionService:
    """
    Application service for coordinating path resolution between simulation and location domains.
    
    This service maintains clean separation of concerns by:
    - Using simulation service to get context data
    - Using location service to get path templates and base paths
    - Orchestrating the resolution process without coupling the domains
    - Providing a unified interface for path resolution operations
    """
    
    def __init__(
        self,
        simulation_service: SimulationApplicationService,
        location_service: LocationApplicationService
    ) -> None:
        """
        Initialize the path resolution service.
        
        Args:
            simulation_service: Service for simulation-related operations
            location_service: Service for location-related operations
        """
        self._simulation_service = simulation_service
        self._location_service = location_service
        self._logger = logger
    
    def resolve_simulation_location_path(
        self,
        simulation_id: str,
        location_name: str,
        requested_path: str = "."
    ) -> str:
        """
        Resolve a complete absolute filesystem path for a simulation at a location.
        
        This method combines:
        1. Location base path
        2. Best matching path template (resolved with simulation context)
        3. Requested relative path
        
        Args:
            simulation_id: ID of the simulation
            location_name: Name of the storage location
            requested_path: Relative path within the resolved location (default ".")
            
        Returns:
            Absolute filesystem path ready for operations
            
        Raises:
            EntityNotFoundError: If simulation or location not found
            PathResolutionError: If path resolution fails
            ValidationError: If inputs are invalid
            
        Examples:
            >>> # Simple case with auto-selected template
            >>> path = service.resolve_simulation_location_path(
            ...     "climate-run-001", 
            ...     "compute-cluster",
            ...     "output/monthly"
            ... )
            >>> path
            '/scratch/projects/CESM2/historical/climate-run-001/output/monthly'
            
            >>> # Just the resolved base location
            >>> base = service.resolve_simulation_location_path(
            ...     "climate-run-001",
            ...     "archive-storage"
            ... )
            >>> base  
            '/tape/archives/CESM2/historical'
        """
        self._logger.debug(
            f"Resolving path for simulation {simulation_id} at location {location_name}, "
            f"requested_path: {requested_path}"
        )
        
        try:
            # Get simulation context for template resolution
            simulation_context = self._simulation_service.get_simulation_context(simulation_id)
            
            # Get location entity for path operations
            location_entity = self._location_service.get_location_filesystem(location_name)
            
            # Get base path from location
            base_path = location_entity.get_base_path()
            
            # Check if simulation has a direct path_prefix for this location
            simulation_entity = self._simulation_service._simulation_repo.get_by_id(simulation_id)
            template_path = None
            resolved_base_path = base_path  # Default to location base
            
            if (simulation_entity and 
                location_name in simulation_entity.location_contexts and
                'path_prefix' in simulation_entity.location_contexts[location_name]):
                
                # Use direct path_prefix from simulation-location association
                configured_prefix = simulation_entity.location_contexts[location_name]['path_prefix']
                
                if configured_prefix:
                    # Resolve template variables in the configured prefix
                    resolved_prefix = configured_prefix
                    
                    # Combine simulation context with location-specific context
                    combined_context = simulation_context.copy()
                    location_context = simulation_entity.location_contexts[location_name]
                    combined_context.update(location_context)
                    
                    for attr_name, attr_value in combined_context.items():
                        placeholder = f"{{{attr_name}}}"
                        resolved_prefix = resolved_prefix.replace(placeholder, str(attr_value))
                    
                    # If the prefix is an absolute path, use it as the complete base
                    if resolved_prefix.startswith('/'):
                        resolved_base_path = resolved_prefix
                        template_path = None  # No additional template needed
                    else:
                        # Relative prefix - use as resolved template with location base
                        template_path = resolved_prefix
            else:
                # Fall back to location path templates
                template_path = location_entity.suggest_path(simulation_context)
            
            # Rebuild path components logic
            path_components = []
            
            # Use resolved base path (either location base or simulation's absolute prefix)
            if resolved_base_path:
                path_components.append(resolved_base_path)
            
            # Add resolved template path if available (for relative prefixes)
            if template_path:
                path_components.append(template_path)
            
            # Add requested path if not just current directory
            if requested_path and requested_path != ".":
                path_components.append(requested_path)
            
            # Combine all components
            if path_components:
                resolved_path = str(Path(*path_components))
            else:
                # Fallback to current directory
                resolved_path = "."
            
            # Ensure absolute path
            if not Path(resolved_path).is_absolute():
                resolved_path = str(Path.cwd() / resolved_path)
            
            self._logger.debug(
                f"Resolved path: {resolved_path} "
                f"(base: {base_path}, template: {template_path}, requested: {requested_path})"
            )
            
            return resolved_path
            
        except EntityNotFoundError:
            # Re-raise domain errors directly
            raise
        except Exception as e:
            error_msg = (
                f"Failed to resolve path for simulation {simulation_id} "
                f"at location {location_name}: {str(e)}"
            )
            self._logger.error(error_msg)
            raise PathResolutionError(error_msg) from e
    
    def resolve_path_with_template(
        self,
        simulation_id: str,
        location_name: str,
        template_name: str,
        requested_path: str = "."
    ) -> str:
        """
        Resolve a path using a specific template instead of auto-selection.
        
        Args:
            simulation_id: ID of the simulation
            location_name: Name of the storage location
            template_name: Specific template name to use
            requested_path: Relative path within the resolved location
            
        Returns:
            Absolute filesystem path
            
        Raises:
            EntityNotFoundError: If simulation, location, or template not found
            PathResolutionError: If path resolution fails
            ValidationError: If template cannot be resolved with simulation context
        """
        self._logger.debug(
            f"Resolving path using template {template_name} for simulation {simulation_id} "
            f"at location {location_name}"
        )
        
        try:
            # Get simulation context
            simulation_context = self._simulation_service.get_simulation_context(simulation_id)
            
            # Get location entity  
            location_entity = self._location_service.get_location_filesystem(location_name)
            
            # Get the specific template
            template = location_entity.get_path_template(template_name)
            if not template:
                raise EntityNotFoundError("Template", template_name)
            
            # Check if template can be resolved with available context
            if not template.has_all_required_attributes(simulation_context):
                missing_attrs = [
                    attr for attr in template.required_attributes 
                    if attr not in simulation_context
                ]
                raise ValidationError(
                    f"Template '{template_name}' requires attributes not available in simulation: "
                    f"{', '.join(missing_attrs)}"
                )
            
            # Resolve template with simulation context
            template_path = location_entity.suggest_path(simulation_context, template_name)
            if not template_path:
                raise PathResolutionError(f"Failed to resolve template '{template_name}'")
            
            # Get base path and combine components
            base_path = location_entity.get_base_path()
            
            path_components = []
            if base_path:
                path_components.append(base_path)
            path_components.append(template_path)
            if requested_path and requested_path != ".":
                path_components.append(requested_path)
            
            resolved_path = str(Path(*path_components))
            
            # Ensure absolute path
            if not Path(resolved_path).is_absolute():
                resolved_path = str(Path.cwd() / resolved_path)
            
            self._logger.debug(f"Resolved path with template {template_name}: {resolved_path}")
            return resolved_path
            
        except (EntityNotFoundError, ValidationError):
            # Re-raise these directly
            raise
        except Exception as e:
            error_msg = (
                f"Failed to resolve path with template {template_name} for "
                f"simulation {simulation_id} at location {location_name}: {str(e)}"
            )
            self._logger.error(error_msg)
            raise PathResolutionError(error_msg) from e
    
    def get_available_templates(
        self,
        simulation_id: str,
        location_name: str
    ) -> List[Dict[str, Any]]:
        """
        Get all available templates for a simulation at a location.
        
        Returns template information including which ones can be resolved
        with the current simulation context.
        
        Args:
            simulation_id: ID of the simulation
            location_name: Name of the storage location
            
        Returns:
            List of dictionaries containing template information:
            - template_name: Name of the template
            - template_pattern: Template pattern string  
            - description: Human-readable description
            - can_resolve: Whether template can be resolved with current context
            - resolved_path: Resolved path if can_resolve is True
            - missing_attributes: List of missing attributes if can_resolve is False
            - complexity_score: Template complexity for sorting
            
        Raises:
            EntityNotFoundError: If simulation or location not found
        """
        self._logger.debug(
            f"Getting available templates for simulation {simulation_id} at location {location_name}"
        )
        
        try:
            # Get simulation context
            simulation_context = self._simulation_service.get_simulation_context(simulation_id)
            
            # Get location entity
            location_entity = self._location_service.get_location_filesystem(location_name)
            
            # Get template suggestions from location
            template_suggestions = location_entity.get_template_suggestions(simulation_context)
            
            # Enhance with resolution information
            enhanced_templates = []
            
            for suggestion in template_suggestions:
                enhanced_template = {
                    'template_name': suggestion['template_name'],
                    'template_pattern': suggestion['template_pattern'],
                    'description': suggestion['description'],
                    'complexity_score': suggestion['complexity_score'],
                    'can_resolve': True,
                    'resolved_path': suggestion['resolved_path'],
                    'missing_attributes': []
                }
                enhanced_templates.append(enhanced_template)
            
            # Also check templates that cannot be resolved
            all_templates = location_entity.list_path_templates()
            
            for template in all_templates:
                # Skip if already included in suggestions
                if any(t['template_name'] == template.name for t in enhanced_templates):
                    continue
                
                # This template couldn't be resolved - find missing attributes
                missing_attrs = [
                    attr for attr in template.required_attributes
                    if attr not in simulation_context
                ]
                
                enhanced_template = {
                    'template_name': template.name,
                    'template_pattern': template.pattern,
                    'description': template.description,
                    'complexity_score': template.get_complexity_score(),
                    'can_resolve': False,
                    'resolved_path': None,
                    'missing_attributes': missing_attrs
                }
                enhanced_templates.append(enhanced_template)
            
            # Sort by can_resolve (True first), then by complexity
            enhanced_templates.sort(key=lambda t: (not t['can_resolve'], t['complexity_score']))
            
            self._logger.debug(
                f"Found {len(enhanced_templates)} templates "
                f"({len(template_suggestions)} can be resolved)"
            )
            
            return enhanced_templates
            
        except EntityNotFoundError:
            raise
        except Exception as e:
            error_msg = (
                f"Failed to get available templates for simulation {simulation_id} "
                f"at location {location_name}: {str(e)}"
            )
            self._logger.error(error_msg)
            raise PathResolutionError(error_msg) from e
    
    def get_simulation_context_preview(self, simulation_id: str) -> Dict[str, str]:
        """
        Get simulation context for preview/debugging purposes.
        
        Args:
            simulation_id: ID of the simulation
            
        Returns:
            Dictionary of context variables available for template resolution
            
        Raises:
            EntityNotFoundError: If simulation not found
        """
        self._logger.debug(f"Getting context preview for simulation {simulation_id}")
        
        try:
            return self._simulation_service.get_simulation_context(simulation_id)
        except EntityNotFoundError:
            raise
        except Exception as e:
            error_msg = f"Failed to get context for simulation {simulation_id}: {str(e)}"
            self._logger.error(error_msg)
            raise PathResolutionError(error_msg) from e
    
    def validate_path_resolution(
        self,
        simulation_id: str,
        location_name: str,
        template_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Validate that a path can be resolved and provide detailed information.
        
        This method performs a dry-run of path resolution and returns detailed
        information about the process, useful for debugging and validation.
        
        Args:
            simulation_id: ID of the simulation
            location_name: Name of the storage location  
            template_name: Optional specific template name
            
        Returns:
            Dictionary with validation results:
            - can_resolve: Whether path can be resolved
            - resolved_path: The resolved path if successful
            - base_path: Location base path
            - template_used: Template that was used (if any)
            - template_path: Resolved template portion
            - simulation_context: Available simulation context
            - errors: List of error messages
            - warnings: List of warning messages
        """
        self._logger.debug(
            f"Validating path resolution for simulation {simulation_id} "
            f"at location {location_name}" + 
            (f" with template {template_name}" if template_name else "")
        )
        
        validation_result = {
            'can_resolve': False,
            'resolved_path': None,
            'base_path': None,
            'template_used': None,
            'template_path': None,
            'simulation_context': {},
            'errors': [],
            'warnings': []
        }
        
        try:
            # Get simulation context
            try:
                simulation_context = self._simulation_service.get_simulation_context(simulation_id)
                validation_result['simulation_context'] = simulation_context
            except EntityNotFoundError as e:
                validation_result['errors'].append(f"Simulation not found: {simulation_id}")
                return validation_result
            
            # Get location
            try:
                location_entity = self._location_service.get_location_filesystem(location_name)
                base_path = location_entity.get_base_path()
                validation_result['base_path'] = base_path
            except EntityNotFoundError as e:
                validation_result['errors'].append(f"Location not found: {location_name}")
                return validation_result
            
            # Try to resolve template
            if template_name:
                # Use specific template
                template = location_entity.get_path_template(template_name)
                if not template:
                    validation_result['errors'].append(f"Template not found: {template_name}")
                    return validation_result
                
                if not template.has_all_required_attributes(simulation_context):
                    missing = [attr for attr in template.required_attributes 
                             if attr not in simulation_context]
                    validation_result['errors'].append(
                        f"Template requires missing attributes: {', '.join(missing)}"
                    )
                    return validation_result
                
                template_path = location_entity.suggest_path(simulation_context, template_name)
                validation_result['template_used'] = template_name
            else:
                # Auto-select template
                template = location_entity.suggest_path_template(simulation_context)
                if template:
                    template_path = location_entity.suggest_path(simulation_context, template.name)
                    validation_result['template_used'] = template.name
                else:
                    template_path = None
                    validation_result['warnings'].append("No compatible templates found")
            
            validation_result['template_path'] = template_path
            
            # Build final path
            try:
                if template_name:
                    resolved_path = self.resolve_path_with_template(
                        simulation_id, location_name, template_name
                    )
                else:
                    resolved_path = self.resolve_simulation_location_path(
                        simulation_id, location_name
                    )
                
                validation_result['can_resolve'] = True
                validation_result['resolved_path'] = resolved_path
                
            except Exception as e:
                validation_result['errors'].append(f"Path resolution failed: {str(e)}")
            
            return validation_result
            
        except Exception as e:
            validation_result['errors'].append(f"Validation failed: {str(e)}")
            return validation_result