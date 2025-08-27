"""
Core Location domain entity - pure business logic without infrastructure dependencies.
"""

from dataclasses import dataclass
from enum import Enum, auto
from typing import Any, Dict, List


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
class LocationEntity:
    """
    Pure domain entity representing a storage location.
    
    This entity contains only the core business data and validation logic,
    without any infrastructure concerns like filesystem operations or persistence.
    """
    name: str
    kinds: List[LocationKind]
    config: Dict[str, Any]
    
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