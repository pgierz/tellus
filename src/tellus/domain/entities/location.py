"""
Core Location domain entity - pure business logic without infrastructure dependencies.
"""

from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Dict, List, Optional


class LocationKind(Enum):
    """Types of storage locations in Earth System Model workflows."""
    TAPE = auto()
    COMPUTE = auto()
    DISK = auto()
    FILESERVER = auto()

    @classmethod
    def from_str(cls, s: str) -> 'LocationKind':
        """Create LocationKind from string representation."""
        try:
            return cls[s.upper()]
        except KeyError:
            valid_kinds = ', '.join(e.name for e in cls)
            raise ValueError(f"Invalid location kind: {s}. Valid kinds: {valid_kinds}")


@dataclass
class PathTemplate:
    """
    Value object representing a path template pattern for a location.
    """
    name: str
    pattern: str
    description: str
    required_attributes: List[str] = field(default_factory=list)
    
    def __post_init__(self):
        """Extract required attributes from pattern."""
        if not self.required_attributes:
            self.required_attributes = self._extract_attributes()
    
    def _extract_attributes(self) -> List[str]:
        """Extract template variable names from the pattern."""
        import re

        # Find all {variable_name} patterns
        matches = re.findall(r'\{([a-zA-Z_][a-zA-Z0-9_]*)\}', self.pattern)
        return list(set(matches))
    
    def has_all_required_attributes(self, available_attributes: Dict[str, str]) -> bool:
        """Check if all required attributes are available."""
        return all(attr in available_attributes for attr in self.required_attributes)
    
    def get_complexity_score(self) -> int:
        """Get complexity score for template selection (lower is simpler)."""
        return len(self.required_attributes)


@dataclass
class LocationEntity:
    """
    Pure domain entity representing a storage location.
    
    This entity contains only the core business data and validation logic,
    without any infrastructure concerns like filesystem operations or persistence.
    """
    name: str
    kinds: List[LocationKind]
    config: Dict[str, Any]
    path_templates: List[PathTemplate] = field(default_factory=list)
    optional: bool = False
    
    def __post_init__(self):
        """Validate the entity after initialization."""
        validation_errors = self.validate()
        if validation_errors:
            raise ValueError(f"Invalid location data: {', '.join(validation_errors)}")
    
    def validate(self) -> List[str]:
        """
        Validate business rules for the location entity.
        
        Returns:
            List of validation error messages (empty if valid)
        """
        errors = []
        
        if not self.name:
            errors.append("Location name is required")
        
        if not isinstance(self.name, str):
            errors.append("Location name must be a string")
        
        if not self.kinds:
            errors.append("At least one location kind is required")
        
        if not isinstance(self.kinds, list):
            errors.append("Location kinds must be a list")
        
        for kind in self.kinds:
            if not isinstance(kind, LocationKind):
                errors.append(f"Invalid location kind: {kind}. Must be LocationKind enum")
        
        if not isinstance(self.config, dict):
            errors.append("Config must be a dictionary")
        
        if not isinstance(self.path_templates, list):
            errors.append("Path templates must be a list")
        
        for template in self.path_templates:
            if not isinstance(template, PathTemplate):
                errors.append(f"Invalid path template: {template}. Must be PathTemplate instance")
        
        # Validate required config fields based on location kinds
        config_errors = self._validate_config()
        errors.extend(config_errors)
        
        return errors
    
    def _validate_config(self) -> List[str]:
        """
        Validate configuration based on location kinds and protocols.
        
        Returns:
            List of configuration validation errors
        """
        errors = []
        
        # Check for required protocol
        if 'protocol' not in self.config:
            errors.append("Protocol is required in config")
        else:
            protocol = self.config['protocol']
            if not isinstance(protocol, str):
                errors.append("Protocol must be a string")
        
        # Validate protocol-specific requirements
        protocol = self.config.get('protocol', '')
        
        if protocol in ('sftp', 'ssh'):
            if 'storage_options' not in self.config:
                errors.append(f"storage_options required for {protocol} protocol")
            else:
                storage_options = self.config['storage_options']
                if not isinstance(storage_options, dict):
                    errors.append("storage_options must be a dictionary")
        
        # Validate path if present
        if 'path' in self.config:
            path = self.config['path']
            if not isinstance(path, str):
                errors.append("Path must be a string if provided")
        
        return errors
    
    def has_kind(self, kind: LocationKind) -> bool:
        """Check if location has a specific kind."""
        return kind in self.kinds
    
    def add_kind(self, kind: LocationKind) -> None:
        """Add a location kind if not already present."""
        if not isinstance(kind, LocationKind):
            raise ValueError(f"Invalid location kind: {kind}")
        
        if kind not in self.kinds:
            self.kinds.append(kind)
    
    def remove_kind(self, kind: LocationKind) -> bool:
        """
        Remove a location kind.
        
        Returns:
            True if kind was removed, False if it wasn't present
        
        Raises:
            ValueError: If trying to remove the last kind
        """
        if kind in self.kinds:
            if len(self.kinds) == 1:
                raise ValueError("Cannot remove the last location kind")
            self.kinds.remove(kind)
            return True
        return False
    
    def get_protocol(self) -> str:
        """Get the storage protocol for this location."""
        return self.config.get('protocol', 'file')
    
    def get_base_path(self) -> str:
        """Get the base path for this location."""
        return self.config.get('path', '')
    
    def get_storage_options(self) -> Dict[str, Any]:
        """Get storage options for this location."""
        return self.config.get('storage_options', {})
    
    def update_config(self, key: str, value: Any) -> None:
        """
        Update a configuration value.
        
        Args:
            key: Configuration key to update
            value: New value
        
        Raises:
            ValueError: If the update would make the config invalid
        """
        if not isinstance(key, str):
            raise ValueError("Config key must be a string")
        
        # Store old value for rollback
        old_value = self.config.get(key)
        self.config[key] = value
        
        # Validate the change
        try:
            validation_errors = self._validate_config()
            if validation_errors:
                # Rollback the change
                if old_value is not None:
                    self.config[key] = old_value
                else:
                    del self.config[key]
                raise ValueError(f"Invalid config update: {', '.join(validation_errors)}")
        except Exception:
            # Rollback on any error
            if old_value is not None:
                self.config[key] = old_value
            else:
                self.config.pop(key, None)
            raise
    
    def is_remote(self) -> bool:
        """Check if this is a remote location (not local filesystem)."""
        protocol = self.get_protocol()
        return protocol not in ('file', 'local')
    
    def is_tape_storage(self) -> bool:
        """Check if this location includes tape storage."""
        return self.has_kind(LocationKind.TAPE)
    
    def is_compute_location(self) -> bool:
        """Check if this is a compute location."""
        return self.has_kind(LocationKind.COMPUTE)
    
    def __eq__(self, other) -> bool:
        """Check equality based on name."""
        if not isinstance(other, LocationEntity):
            return False
        return self.name == other.name
    
    def __hash__(self) -> int:
        """Hash based on name."""
        return hash(self.name)
    
    def __str__(self) -> str:
        """String representation of the location."""
        kinds_str = ', '.join(kind.name for kind in self.kinds)
        protocol = self.get_protocol()
        return f"Location[{self.name}] ({protocol}, {kinds_str})"
    
    def __repr__(self) -> str:
        """Detailed representation for debugging."""
        return (f"LocationEntity(name='{self.name}', "
                f"kinds={[k.name for k in self.kinds]}, "
                f"protocol='{self.get_protocol()}')")
    
    # Path Template Management Methods
    
    def add_path_template(self, template: PathTemplate) -> None:
        """
        Add a path template to this location.
        
        Args:
            template: PathTemplate to add
            
        Raises:
            ValueError: If template name already exists
        """
        if not isinstance(template, PathTemplate):
            raise ValueError("Template must be a PathTemplate instance")
            
        if any(t.name == template.name for t in self.path_templates):
            raise ValueError(f"Template '{template.name}' already exists")
            
        self.path_templates.append(template)
    
    def remove_path_template(self, template_name: str) -> bool:
        """
        Remove a path template by name.
        
        Args:
            template_name: Name of template to remove
            
        Returns:
            True if template was removed, False if not found
        """
        for i, template in enumerate(self.path_templates):
            if template.name == template_name:
                del self.path_templates[i]
                return True
        return False
    
    def get_path_template(self, template_name: str) -> Optional[PathTemplate]:
        """
        Get a path template by name.
        
        Args:
            template_name: Name of template to retrieve
            
        Returns:
            PathTemplate if found, None otherwise
        """
        for template in self.path_templates:
            if template.name == template_name:
                return template
        return None
    
    def list_path_templates(self) -> List[PathTemplate]:
        """
        Get all path templates for this location.
        
        Returns:
            List of PathTemplate objects
        """
        return self.path_templates.copy()
    
    # Path Suggestion Methods
    
    def suggest_path_template(self, simulation_attributes: Dict[str, str]) -> Optional[PathTemplate]:
        """
        Suggest the best path template based on available simulation attributes.
        
        Selects the template that:
        1. Has all required attributes available
        2. Uses the most attributes (most specific)
        3. Has lowest complexity score for ties
        
        Args:
            simulation_attributes: Dictionary of simulation attribute key-value pairs
            
        Returns:
            Best matching PathTemplate, or None if no templates match
        """
        if not self.path_templates:
            return None
        
        # Filter to templates that have all required attributes
        compatible_templates = [
            template for template in self.path_templates
            if template.has_all_required_attributes(simulation_attributes)
        ]
        
        if not compatible_templates:
            return None
        
        # Sort by number of attributes used (descending) then by complexity (ascending)
        compatible_templates.sort(
            key=lambda t: (-len(t.required_attributes), t.get_complexity_score())
        )
        
        return compatible_templates[0]
    
    def suggest_path(self, simulation_attributes: Dict[str, str], template_name: Optional[str] = None) -> Optional[str]:
        """
        Suggest a complete path based on simulation attributes.
        
        Args:
            simulation_attributes: Dictionary of simulation attribute key-value pairs
            template_name: Specific template to use, or None for auto-selection
            
        Returns:
            Resolved path string, or None if no suitable template found
        """
        if template_name:
            template = self.get_path_template(template_name)
            if not template:
                return None
            if not template.has_all_required_attributes(simulation_attributes):
                return None
        else:
            template = self.suggest_path_template(simulation_attributes)
            if not template:
                return None
        
        # Resolve the template pattern
        resolved_path = template.pattern
        for attr_name, attr_value in simulation_attributes.items():
            placeholder = f"{{{attr_name}}}"
            resolved_path = resolved_path.replace(placeholder, str(attr_value))
        
        return resolved_path
    
    def get_template_suggestions(self, simulation_attributes: Dict[str, str]) -> List[Dict[str, Any]]:
        """
        Get all compatible templates with their resolved paths as suggestions.
        
        Args:
            simulation_attributes: Dictionary of simulation attribute key-value pairs
            
        Returns:
            List of dictionaries containing template info and resolved paths
        """
        suggestions = []
        
        for template in self.path_templates:
            if template.has_all_required_attributes(simulation_attributes):
                resolved_path = self.suggest_path(simulation_attributes, template.name)
                suggestions.append({
                    'template_name': template.name,
                    'template_pattern': template.pattern,
                    'description': template.description,
                    'resolved_path': resolved_path,
                    'complexity_score': template.get_complexity_score(),
                    'required_attributes': template.required_attributes.copy()
                })
        
        # Sort by complexity score (simpler first)
        suggestions.sort(key=lambda s: s['complexity_score'])
        
        return suggestions
    
    def create_default_templates(self) -> None:
        """
        Create default path templates based on location kind and common patterns.
        
        This method creates sensible defaults for Earth System Model workflows
        based on the location's kinds and intended usage patterns.
        """
        if self.has_kind(LocationKind.COMPUTE):
            self.path_templates.extend([
                PathTemplate(
                    name="simple",
                    pattern="{simulation_id}",
                    description="Simple simulation ID only"
                ),
                PathTemplate(
                    name="model_experiment",
                    pattern="{model}/{experiment}",
                    description="Model and experiment grouping"
                ),
                PathTemplate(
                    name="detailed",
                    pattern="{model}/{experiment}/{simulation_id}",
                    description="Hierarchical with model, experiment, and simulation"
                )
            ])
        
        if self.has_kind(LocationKind.DISK) or self.has_kind(LocationKind.FILESERVER):
            self.path_templates.extend([
                PathTemplate(
                    name="project_organized",
                    pattern="{project}/{model}/{experiment}",
                    description="Project-based organization"
                ),
                PathTemplate(
                    name="timestamped",
                    pattern="{model}/{experiment}/{run_date}",
                    description="Date-based organization for runs"
                )
            ])
        
        if self.has_kind(LocationKind.TAPE):
            self.path_templates.extend([
                PathTemplate(
                    name="archive_basic",
                    pattern="archives/{model}/{experiment}",
                    description="Basic archive organization"
                ),
                PathTemplate(
                    name="archive_dated",
                    pattern="archives/{model}/{experiment}/{year}",
                    description="Year-based archive organization"
                )
            ])
        
        # Remove duplicates by name
        seen_names = set()
        unique_templates = []
        for template in self.path_templates:
            if template.name not in seen_names:
                unique_templates.append(template)
                seen_names.add(template.name)
        self.path_templates = unique_templates