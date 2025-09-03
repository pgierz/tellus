"""
Core Simulation domain entity - pure business logic without infrastructure dependencies.
"""

import copy
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set


@dataclass
class SimulationEntity:
    """
    Pure domain entity representing a simulation in the Earth System Model context.

    This entity contains only the core business data and validation logic,
    without any infrastructure concerns like persistence or file system operations.
    """

    simulation_id: str
    model_id: Optional[str] = None
    path: Optional[str] = None
    attrs: Dict[str, Any] = field(default_factory=dict)
    namelists: Dict[str, Any] = field(default_factory=dict)
    snakemakes: Dict[str, Any] = field(default_factory=dict)
    
    # Location associations - tracks which locations this simulation knows about
    associated_locations: Set[str] = field(default_factory=set)
    
    # Location-specific contexts and configurations
    location_contexts: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    
    # File management - tracks files associated with this simulation
    file_inventory: Optional['FileInventory'] = None

    # Internal identifier (different from user-facing simulation_id)
    _uid: str = field(default_factory=lambda: str(uuid.uuid4()))

    def __post_init__(self):
        """Validate the entity after initialization."""
        validation_errors = self.validate()
        if validation_errors:
            raise ValueError(f"Invalid simulation data: {', '.join(validation_errors)}")

    def validate(self) -> List[str]:
        """
        Validate business rules for the simulation entity.

        Returns:
            List of validation error messages (empty if valid)
        """
        errors = []

        if not self.simulation_id:
            errors.append("Simulation ID is required")

        if not isinstance(self.simulation_id, str):
            errors.append("Simulation ID must be a string")

        if self.model_id is not None and not isinstance(self.model_id, str):
            errors.append("Model ID must be a string if provided")

        if self.path is not None and not isinstance(self.path, str):
            errors.append("Path must be a string if provided")

        if not isinstance(self.attrs, dict):
            errors.append("Attributes must be a dictionary")

        if not isinstance(self.namelists, dict):
            errors.append("Namelists must be a dictionary")

        if not isinstance(self.snakemakes, dict):
            errors.append("Snakemakes must be a dictionary")
        
        if not isinstance(self.associated_locations, set):
            errors.append("Associated locations must be a set")
            
        if not isinstance(self.location_contexts, dict):
            errors.append("Location contexts must be a dictionary")
            
        if self.file_inventory is not None:
            # Import here to avoid circular imports
            from .simulation_file import FileInventory
            if not isinstance(self.file_inventory, FileInventory):
                errors.append("File inventory must be a FileInventory instance")

        return errors

    @property
    def uid(self) -> str:
        """Get the internal unique identifier."""
        return self._uid

    def add_attribute(self, key: str, value: Any) -> None:
        """Add or update a simulation attribute."""
        if not isinstance(key, str):
            raise ValueError("Attribute key must be a string")
        self.attrs[key] = value

    def pop_attribute(self, key: str) -> Any:
        """
        Remove and return a simulation attribute.

        Returns:
            The removed attribute value
        """
        return self.attrs.pop(key)

    def remove_attribute(self, key: str) -> bool:
        """
        Remove a simulation attribute.

        Returns:
            True if attribute was removed, False if it didn't exist
        """
        if key in self.attrs:
            del self.attrs[key]
            return True
        return False

    def add_namelist(self, name: str, namelist_data: Any) -> None:
        """Add or update a namelist."""
        if not isinstance(name, str):
            raise ValueError("Namelist name must be a string")
        self.namelists[name] = namelist_data

    def add_snakemake_rule(self, rule_name: str, smk_file: str) -> None:
        """
        Add a snakemake rule to the simulation.

        Args:
            rule_name: The name of the snakemake rule
            smk_file: The path to the snakemake file

        Raises:
            ValueError: If rule already exists or if parameters are invalid
        """
        if not isinstance(rule_name, str) or not rule_name:
            raise ValueError("Rule name must be a non-empty string")

        if not isinstance(smk_file, str) or not smk_file:
            raise ValueError("Snakemake file path must be a non-empty string")

        if rule_name in self.snakemakes:
            raise ValueError(f"Snakemake rule '{rule_name}' already exists")

        self.snakemakes[rule_name] = smk_file

    def remove_snakemake_rule(self, rule_name: str) -> bool:
        """
        Remove a snakemake rule.

        Returns:
            True if rule was removed, False if it didn't exist
        """
        if rule_name in self.snakemakes:
            del self.snakemakes[rule_name]
            return True
        return False

    def get_context_variables(self) -> Dict[str, str]:
        """
        Get context variables for template rendering.

        Returns:
            Dictionary of variables that can be used in path templates
        """
        return {
            "simulation_id": str(self.simulation_id),
            "model_id": str(self.model_id or ""),
            "uid": str(self._uid),
            **{k: str(v) for k, v in self.attrs.items()},
        }

    # Location management methods
    
    def associate_location(self, location_name: str, context: Optional[Dict[str, Any]] = None) -> None:
        """
        Associate this simulation with a location.
        
        Args:
            location_name: Name of the location to associate
            context: Optional location-specific context/configuration
            
        Raises:
            ValueError: If location_name is invalid
        """
        if not isinstance(location_name, str) or not location_name.strip():
            raise ValueError("Location name must be a non-empty string")
            
        self.associated_locations.add(location_name)
        
        # Always ensure location is in location_contexts for persistence
        # Use empty dict if no context provided
        if context is not None:
            if not isinstance(context, dict):
                raise ValueError("Location context must be a dictionary")
            self.location_contexts[location_name] = copy.deepcopy(context)
        else:
            # Ensure location exists in contexts even with empty context
            if location_name not in self.location_contexts:
                self.location_contexts[location_name] = {}
    
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
            raise ValueError(f"Location '{location_name}' is not associated with this simulation")
            
        if not isinstance(context, dict):
            raise ValueError("Location context must be a dictionary")
            
        self.location_contexts[location_name] = copy.deepcopy(context)
    
    def is_location_associated(self, location_name: str) -> bool:
        """
        Check if a location is associated with this simulation.
        
        Args:
            location_name: Name of the location to check
            
        Returns:
            True if location is associated, False otherwise
        """
        return location_name in self.associated_locations

    # File Management Methods
    
    def has_file_inventory(self) -> bool:
        """Check if simulation has a file inventory."""
        return self.file_inventory is not None
    
    def get_file_count(self) -> int:
        """Get the number of files associated with this simulation."""
        if self.file_inventory:
            return self.file_inventory.file_count
        return 0
    
    def add_file(self, file: 'SimulationFile') -> None:
        """
        Add a file to the simulation's file inventory.
        
        Args:
            file: SimulationFile to add
        """
        # Import here to avoid circular imports
        from .simulation_file import FileInventory, SimulationFile
        
        if not isinstance(file, SimulationFile):
            raise ValueError("File must be a SimulationFile instance")
        
        if self.file_inventory is None:
            self.file_inventory = FileInventory()
        
        self.file_inventory.add_file(file)
    
    def remove_file(self, relative_path: str) -> bool:
        """
        Remove a file from the simulation's file inventory.
        
        Args:
            relative_path: Path of the file to remove
            
        Returns:
            True if file was removed, False if it didn't exist
        """
        if self.file_inventory is None:
            return False
        
        return self.file_inventory.remove_file(relative_path)
    
    def get_file(self, relative_path: str) -> Optional['SimulationFile']:
        """
        Get a specific file from the simulation's file inventory.
        
        Args:
            relative_path: Path of the file to get
            
        Returns:
            SimulationFile if found, None otherwise
        """
        if self.file_inventory is None:
            return None
        
        return self.file_inventory.get_file(relative_path)
    
    def get_files(self) -> List['SimulationFile']:
        """
        Get all files in the simulation's file inventory.
        
        Returns:
            List of all SimulationFile instances
        """
        if self.file_inventory is None:
            return []
        
        return self.file_inventory.get_all_files()
    
    def get_files_by_content_type(self, content_type: 'FileContentType') -> List['SimulationFile']:
        """
        Get files filtered by content type.
        
        Args:
            content_type: Content type to filter by
            
        Returns:
            List of SimulationFile instances matching the content type
        """
        if self.file_inventory is None:
            return []
        
        return self.file_inventory.get_files_by_content_type(content_type)
    
    def get_files_by_importance(self, importance: 'FileImportance') -> List['SimulationFile']:
        """
        Get files filtered by importance level.
        
        Args:
            importance: Importance level to filter by
            
        Returns:
            List of SimulationFile instances matching the importance level
        """
        if self.file_inventory is None:
            return []
        
        return self.file_inventory.get_files_by_importance(importance)
    
    def get_content_type_summary(self) -> Dict[str, int]:
        """
        Get summary of files by content type.
        
        Returns:
            Dictionary mapping content type names to counts
        """
        if self.file_inventory is None:
            return {}
        
        return self.file_inventory.get_content_type_summary()
    
    def clear_files(self) -> int:
        """
        Remove all files from the simulation's file inventory.
        
        Returns:
            Number of files that were removed
        """
        if self.file_inventory is None:
            return 0
        
        count = self.file_inventory.file_count
        self.file_inventory = None
        return count
    
    def get_associated_locations(self) -> List[str]:
        """
        Get list of associated location names.
        
        Returns:
            Sorted list of location names
        """
        return sorted(self.associated_locations)

    def __eq__(self, other) -> bool:
        """Check equality based on simulation_id."""
        if not isinstance(other, SimulationEntity):
            return False
        return self.simulation_id == other.simulation_id

    def __hash__(self) -> int:
        """Hash based on simulation_id."""
        return hash(self.simulation_id)

    def __str__(self) -> str:
        """String representation of the simulation."""
        model_info = f" (model: {self.model_id})" if self.model_id else ""
        return f"Simulation[{self.simulation_id}]{model_info}"

    def __repr__(self) -> str:
        """Detailed representation for debugging."""
        return (
            f"SimulationEntity(simulation_id='{self.simulation_id}', "
            f"model_id='{self.model_id}', path='{self.path}', "
            f"uid='{self._uid}')"
        )
