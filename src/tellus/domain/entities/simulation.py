"""
Core Simulation domain entity - pure business logic without infrastructure dependencies.
"""

import copy
import uuid
from typing import Any, Dict, List, Optional, Set
from pydantic import BaseModel, Field, ConfigDict, field_validator, computed_field


class SimulationEntity(BaseModel):
    """
    Pure domain entity representing a simulation as an RO-Crate container.

    A Simulation combines:
    - RO-Crate for packaging & provenance
    - STAC-style assets for geospatial file cataloging
    - Icechunk/Zarr for versioned array data
    - Multi-location storage references (HPC, tape, cloud)

    This entity contains only the core business data and validation logic,
    without any infrastructure concerns like persistence or file system operations.
    """

    model_config = ConfigDict(
        # Allow field validation for private fields
        extra='forbid',
        # Keep the same behavior as dataclass __init__
        validate_assignment=True,
        # Make it compatible with existing serialization
        arbitrary_types_allowed=True
    )

    # Core identity
    simulation_id: str
    model_id: Optional[str] = None

    # Flexible metadata (replaces 'attrs', 'namelists')
    attributes: Dict[str, Any] = Field(default_factory=dict)

    # Workflows (replaces 'snakemakes')
    workflows: Dict[str, Any] = Field(default_factory=dict)

    # === RO-Crate Integration ===
    ro_crate_path: Optional[str] = None  # Path to ro-crate-metadata.json
    ro_crate_id: Optional[str] = None    # @id within RO-Crate

    # === Data Storage ===
    # Primary data store (Icechunk, Zarr, NetCDF, etc.)
    data_store: Dict[str, Any] = Field(default_factory=dict)
    # Example: {"type": "icechunk", "uri": "s3://bucket/sim.ichk", "snapshot_id": "abc123"}

    # === File Assets (STAC-like) ===
    assets: Dict[str, Dict[str, Any]] = Field(default_factory=dict)
    # Example: {
    #   "restart_files": {"href": "s3://bucket/restarts/", "type": "application/x-netcdf", "roles": ["restart"]},
    #   "output_data": {"href": "file:///scratch/outputs/", "type": "application/x-zarr", "roles": ["data"]}
    # }

    # === Multi-Location Storage ===
    # Location associations - tracks which locations this simulation knows about
    associated_locations: Set[str] = Field(default_factory=set)

    # Location-specific contexts and configurations
    location_contexts: Dict[str, Dict[str, Any]] = Field(default_factory=dict)

    # === Provenance & Versioning ===
    provenance: List[Dict[str, Any]] = Field(default_factory=list)
    # Analysis accretion history

    # === STAC Catalog Reference (optional) ===
    stac_catalog_url: Optional[str] = None  # Link to STAC catalog for this simulation

    # File management - tracks files associated with this simulation
    file_inventory: Optional['FileInventory'] = None

    # Internal identifier (different from user-facing simulation_id)
    uid_: str = Field(default_factory=lambda: str(uuid.uuid4()), alias='_uid')

    # === DEPRECATED fields for backward compatibility ===
    # These will be removed in a future version - use 'attributes' instead
    @property
    def attrs(self) -> Dict[str, Any]:
        """DEPRECATED: Use 'attributes' instead."""
        import warnings
        warnings.warn(
            "SimulationEntity.attrs is deprecated, use .attributes instead",
            DeprecationWarning,
            stacklevel=2
        )
        return self.attributes

    @property
    def path(self) -> Optional[str]:
        """DEPRECATED: Path is now stored in data_store or assets."""
        import warnings
        warnings.warn(
            "SimulationEntity.path is deprecated, use .data_store['uri'] or .assets instead",
            DeprecationWarning,
            stacklevel=2
        )
        return self.data_store.get('uri') if self.data_store else None

    @property
    def namelists(self) -> Dict[str, Any]:
        """DEPRECATED: Namelists are now stored in 'attributes'."""
        import warnings
        warnings.warn(
            "SimulationEntity.namelists is deprecated, store in .attributes instead",
            DeprecationWarning,
            stacklevel=2
        )
        return self.attributes.get('namelists', {})

    @property
    def snakemakes(self) -> Dict[str, Any]:
        """DEPRECATED: Use 'workflows' instead."""
        import warnings
        warnings.warn(
            "SimulationEntity.snakemakes is deprecated, use .workflows instead",
            DeprecationWarning,
            stacklevel=2
        )
        return self.workflows

    @field_validator('simulation_id')
    @classmethod
    def validate_simulation_id(cls, v):
        """Validate simulation_id field."""
        if not v:
            raise ValueError('Simulation ID is required')
        if not isinstance(v, str):
            raise ValueError('Simulation ID must be a string')
        return v

    @field_validator('model_id')
    @classmethod
    def validate_model_id(cls, v):
        """Validate model_id field."""
        if v is not None and not isinstance(v, str):
            raise ValueError('Model ID must be a string if provided')
        return v

    def validate(self) -> List[str]:
        """
        Validate business rules for the simulation entity.
        
        Note: With pydantic, most validation is handled automatically.
        This method is kept for backward compatibility and custom business rules.

        Returns:
            List of validation error messages (empty if valid)
        """
        errors = []
            
        if self.file_inventory is not None:
            # Import here to avoid circular imports
            from .simulation_file import FileInventory
            if not isinstance(self.file_inventory, FileInventory):
                errors.append("File inventory must be a FileInventory instance")

        return errors

    @computed_field  # type: ignore
    @property
    def uid(self) -> str:
        """Get the internal unique identifier."""
        return self.uid_

    def add_attribute(self, key: str, value: Any) -> None:
        """Add or update a simulation attribute."""
        if not isinstance(key, str):
            raise ValueError("Attribute key must be a string")
        self.attributes[key] = value

    def pop_attribute(self, key: str) -> Any:
        """
        Remove and return a simulation attribute.

        Returns:
            The removed attribute value
        """
        return self.attributes.pop(key)

    def remove_attribute(self, key: str) -> bool:
        """
        Remove a simulation attribute.

        Returns:
            True if attribute was removed, False if it didn't exist
        """
        if key in self.attributes:
            del self.attributes[key]
            return True
        return False

    def add_namelist(self, name: str, namelist_data: Any) -> None:
        """
        DEPRECATED: Add or update a namelist.

        Namelists should now be stored in attributes['namelists'].
        """
        import warnings
        warnings.warn(
            "add_namelist is deprecated, store namelists in attributes['namelists'] instead",
            DeprecationWarning,
            stacklevel=2
        )
        if not isinstance(name, str):
            raise ValueError("Namelist name must be a string")
        if 'namelists' not in self.attributes:
            self.attributes['namelists'] = {}
        self.attributes['namelists'][name] = namelist_data

    def add_workflow(self, workflow_name: str, workflow_data: Any) -> None:
        """
        Add or update a workflow definition.

        Args:
            workflow_name: The name of the workflow (e.g., "preprocessing", "analysis")
            workflow_data: Workflow definition (Snakemake, CWL, script path, etc.)

        Raises:
            ValueError: If workflow already exists or if parameters are invalid
        """
        if not isinstance(workflow_name, str) or not workflow_name:
            raise ValueError("Workflow name must be a non-empty string")

        if workflow_name in self.workflows:
            raise ValueError(f"Workflow '{workflow_name}' already exists")

        self.workflows[workflow_name] = workflow_data

    def remove_workflow(self, workflow_name: str) -> bool:
        """
        Remove a workflow.

        Returns:
            True if workflow was removed, False if it didn't exist
        """
        if workflow_name in self.workflows:
            del self.workflows[workflow_name]
            return True
        return False

    # DEPRECATED methods - kept for backward compatibility
    def add_snakemake_rule(self, rule_name: str, smk_file: str) -> None:
        """DEPRECATED: Use add_workflow instead."""
        import warnings
        warnings.warn(
            "add_snakemake_rule is deprecated, use add_workflow instead",
            DeprecationWarning,
            stacklevel=2
        )
        self.add_workflow(rule_name, smk_file)

    def remove_snakemake_rule(self, rule_name: str) -> bool:
        """DEPRECATED: Use remove_workflow instead."""
        import warnings
        warnings.warn(
            "remove_snakemake_rule is deprecated, use remove_workflow instead",
            DeprecationWarning,
            stacklevel=2
        )
        return self.remove_workflow(rule_name)

    def get_context_variables(self) -> Dict[str, str]:
        """
        Get context variables for template rendering.

        Returns:
            Dictionary of variables that can be used in path templates
        """
        return {
            "simulation_id": str(self.simulation_id),
            "model_id": str(self.model_id or ""),
            "uid": str(self.uid_),
            **{k: str(v) for k, v in self.attributes.items()},
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
            f"uid='{self.uid}')"
        )


# Import FileInventory and rebuild model to resolve forward references
def _rebuild_model():
    """Rebuild the SimulationEntity model after FileInventory is defined."""
    try:
        from .simulation_file import FileInventory  # noqa: F401
        SimulationEntity.model_rebuild()
    except ImportError:
        # FileInventory might not exist yet, that's okay
        pass

# Try to rebuild now, but don't fail if FileInventory doesn't exist
_rebuild_model()
