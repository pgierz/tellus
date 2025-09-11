"""
Core Location domain entity - pure business logic without infrastructure dependencies.
"""

from enum import Enum, auto
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field, ConfigDict, field_validator


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


class PathTemplate(BaseModel):
    """
    Value object representing a path template pattern for a location.
    """
    
    model_config = ConfigDict(
        extra='forbid',
        validate_assignment=True
    )
    
    name: str
    pattern: str
    description: str
    required_attributes: List[str] = Field(default_factory=list)
    
    def model_post_init(self, __context) -> None:
        """Extract required attributes from pattern after model initialization."""
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


class LocationEntity(BaseModel):
    """
    Pure domain entity representing a storage location.
    
    This entity contains only the core business data and validation logic,
    without any infrastructure concerns like filesystem operations or persistence.
    """
    
    model_config = ConfigDict(
        extra='forbid',
        validate_assignment=True,
        arbitrary_types_allowed=True
    )
    
    name: str
    kinds: List[LocationKind]
    config: Dict[str, Any]
    path_templates: List[PathTemplate] = Field(default_factory=list)
    optional: bool = False
    
    @field_validator('name')
    @classmethod
    def validate_name(cls, v):
        """Validate location name."""
        if not v:
            raise ValueError('Location name is required')
        if not isinstance(v, str):
            raise ValueError('Location name must be a string')
        return v
    
    @field_validator('kinds')
    @classmethod
    def validate_kinds(cls, v):
        """Validate location kinds."""
        if not v:
            raise ValueError('At least one location kind is required')
        if not isinstance(v, list):
            raise ValueError('Location kinds must be a list')
        for kind in v:
            if not isinstance(kind, LocationKind):
                raise ValueError(f'Invalid location kind: {kind}. Must be LocationKind enum')
        return v
    
    @field_validator('config')
    @classmethod
    def validate_config(cls, v):
        """Validate config dictionary."""
        if not isinstance(v, dict):
            raise ValueError('Config must be a dictionary')
        # Check for required protocol
        if 'protocol' not in v:
            raise ValueError('Protocol is required in config')
        protocol = v['protocol']
        if not isinstance(protocol, str):
            raise ValueError('Protocol must be a string')
        
        # Validate protocol-specific requirements
        if protocol in ('sftp', 'ssh'):
            if 'storage_options' not in v:
                raise ValueError(f'storage_options required for {protocol} protocol')
            storage_options = v['storage_options']
            if not isinstance(storage_options, dict):
                raise ValueError('storage_options must be a dictionary')
        
        # Validate path if present
        if 'path' in v:
            path = v['path']
            if not isinstance(path, str):
                raise ValueError('Path must be a string if provided')
        
        return v
    
    @field_validator('path_templates')
    @classmethod
    def validate_path_templates(cls, v):
        """Validate path templates."""
        if not isinstance(v, list):
            raise ValueError('Path templates must be a list')
        for template in v:
            if not isinstance(template, PathTemplate):
                raise ValueError(f'Invalid path template: {template}. Must be PathTemplate instance')
        return v
    
    def validate(self) -> List[str]:
        """
        Validate business rules for the location entity.
        
        Note: With pydantic, most validation is handled automatically.
        This method is kept for backward compatibility and custom business rules.
        
        Returns:
            List of validation error messages (empty if valid)
        """
        errors = []
        
        # Additional custom validation can go here if needed
        
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
        new_config = self.config.copy()
        new_config[key] = value
        
        # Validate the change by trying to update the model
        try:
            # This will trigger validation
            self.config = new_config
        except Exception as e:
            # Rollback on any error
            if old_value is not None:
                self.config[key] = old_value
            else:
                self.config.pop(key, None)
            raise ValueError(f"Invalid config update: {e}") from e
    
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