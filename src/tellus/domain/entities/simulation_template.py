"""
Domain entity for simulation templates.

Templates capture common patterns for creating series of related simulations,
enabling efficient management of simulation families like paleoclimate time series.
"""

import re
from typing import Dict, List, Optional, Any, Set
from pydantic import BaseModel, Field, field_validator
from uuid import uuid4


class SimulationTemplate(BaseModel):
    """
    Template for creating simulation series with pattern-based variable substitution.

    Templates define reusable patterns for simulation creation, including:
    - Naming patterns with variable placeholders
    - Default attributes and metadata
    - Location associations and path templates
    - Variable definitions and constraints

    Examples:
        >>> # Paleoclimate time series template
        >>> template = SimulationTemplate(
        ...     name="eem-series",
        ...     description="Eemian paleoclimate time series",
        ...     pattern="Eem{time_period}-S2",
        ...     variables={
        ...         "time_period": {"type": "int", "range": [120, 130]},
        ...         "model": {"type": "str", "default": "cosmos-aso-wiso"},
        ...         "user": {"type": "str", "default": "pgierz"}
        ...     },
        ...     default_attrs={
        ...         "domain": "paleoclimate",
        ...         "experiment_type": "time_slice"
        ...     }
        ... )

        >>> # Generate simulation ID from template
        >>> sim_id = template.generate_simulation_id({"time_period": 125})
        >>> sim_id
        'Eem125-S2'
    """

    # Template identification
    name: str = Field(..., description="Unique template name")
    description: Optional[str] = Field(None, description="Human-readable description")

    # Pattern definition
    pattern: str = Field(..., description="Naming pattern with {variable} placeholders")
    variables: Dict[str, Dict[str, Any]] = Field(
        default_factory=dict,
        description="Variable definitions with types, defaults, constraints"
    )

    # Template content
    default_attrs: Dict[str, Any] = Field(
        default_factory=dict,
        description="Default simulation attributes"
    )
    default_model_id: Optional[str] = Field(None, description="Default model identifier")

    # Location associations
    location_associations: Dict[str, Dict[str, Any]] = Field(
        default_factory=dict,
        description="Default location associations and path templates"
    )

    # Metadata
    template_id: str = Field(default_factory=lambda: str(uuid4()), description="Unique template ID")
    created_by: Optional[str] = Field(None, description="Template creator")
    tags: Set[str] = Field(default_factory=set, description="Template tags for organization")

    @field_validator('pattern')
    @classmethod
    def validate_pattern(cls, v: str) -> str:
        """Validate that pattern contains at least one variable placeholder."""
        if not re.search(r'\{[^}]+\}', v):
            raise ValueError("Pattern must contain at least one variable placeholder like {variable}")
        return v

    @field_validator('name')
    @classmethod
    def validate_name(cls, v: str) -> str:
        """Validate template name format."""
        if not v or not v.replace('-', '').replace('_', '').isalnum():
            raise ValueError("Template name must be alphanumeric (with - or _ allowed)")
        return v

    def extract_variables_from_string(self, text: str) -> Optional[Dict[str, str]]:
        """
        Extract variable values from a string using the template pattern.

        Args:
            text: String to extract variables from (e.g., simulation ID, path)

        Returns:
            Dictionary of variable values, or None if pattern doesn't match

        Examples:
            >>> template = SimulationTemplate(pattern="Eem{time_period}-S2", ...)
            >>> template.extract_variables_from_string("Eem125-S2")
            {'time_period': '125'}
        """
        # Convert pattern to regex by replacing {var} with named groups
        regex_pattern = self.pattern

        # Find all variable placeholders
        variables = re.findall(r'\{([^}]+)\}', self.pattern)

        # Replace each placeholder with a named capture group
        for var in variables:
            placeholder = f"{{{var}}}"
            # Use word characters, numbers, and common symbols for variable values
            regex_pattern = regex_pattern.replace(placeholder, f"(?P<{var}>[\\w.-]+)")

        # Anchor the pattern to match the entire string
        regex_pattern = f"^{regex_pattern}$"

        match = re.match(regex_pattern, text)
        if match:
            return match.groupdict()
        return None

    def generate_simulation_id(self, variable_values: Dict[str, Any]) -> str:
        """
        Generate a simulation ID from the template pattern and variable values.

        Args:
            variable_values: Dictionary mapping variable names to values

        Returns:
            Generated simulation ID

        Raises:
            ValueError: If required variables are missing

        Examples:
            >>> template = SimulationTemplate(pattern="Eem{time_period}-S2", ...)
            >>> template.generate_simulation_id({"time_period": 125})
            'Eem125-S2'
        """
        result = self.pattern

        # Get all variables from pattern
        pattern_vars = set(re.findall(r'\{([^}]+)\}', self.pattern))

        # Check for missing variables
        missing_vars = pattern_vars - set(variable_values.keys())
        if missing_vars:
            raise ValueError(f"Missing required variables: {missing_vars}")

        # Substitute variables
        for var, value in variable_values.items():
            placeholder = f"{{{var}}}"
            result = result.replace(placeholder, str(value))

        return result

    def validate_variable_values(self, variable_values: Dict[str, Any]) -> List[str]:
        """
        Validate variable values against template constraints.

        Args:
            variable_values: Dictionary of variable values to validate

        Returns:
            List of validation error messages (empty if valid)
        """
        errors = []

        for var_name, value in variable_values.items():
            if var_name not in self.variables:
                continue

            var_def = self.variables[var_name]
            var_type = var_def.get("type", "str")

            # Type validation
            if var_type == "int":
                try:
                    int_value = int(value)
                    # Range validation
                    if "range" in var_def:
                        min_val, max_val = var_def["range"]
                        if not (min_val <= int_value <= max_val):
                            errors.append(f"{var_name} must be between {min_val} and {max_val}")
                except ValueError:
                    errors.append(f"{var_name} must be an integer")

            elif var_type == "str":
                if "choices" in var_def:
                    if str(value) not in var_def["choices"]:
                        errors.append(f"{var_name} must be one of: {var_def['choices']}")

        return errors

    def get_default_variable_values(self) -> Dict[str, Any]:
        """Get default values for all template variables."""
        defaults = {}
        for var_name, var_def in self.variables.items():
            if "default" in var_def:
                defaults[var_name] = var_def["default"]
        return defaults

    def create_simulation_attrs(self, variable_values: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create simulation attributes by merging template defaults with variable values.

        Args:
            variable_values: Variable values for this simulation instance

        Returns:
            Complete attributes dictionary for simulation creation
        """
        attrs = self.default_attrs.copy()

        # Add variable values to attributes
        attrs.update(variable_values)

        # Add template metadata
        attrs["template_name"] = self.name
        attrs["template_id"] = self.template_id

        return attrs