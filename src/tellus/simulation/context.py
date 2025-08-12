"""Location context for experiment-specific configurations."""
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Optional


@dataclass
class LocationContext:
    """Holds experiment-specific context for a location.
    
    Attributes:
        path_prefix: Path prefix to prepend to location paths
        overrides: Dictionary of configuration overrides for the location
        metadata: Additional metadata for the location in this context
    """
    path_prefix: str = ""
    overrides: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> dict:
        """Convert the context to a dictionary for serialization."""
        return {
            "path_prefix": self.path_prefix,
            "overrides": self.overrides,
            "metadata": self.metadata
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> 'LocationContext':
        """Create a LocationContext from a dictionary."""
        return cls(
            path_prefix=data.get("path_prefix", ""),
            overrides=data.get("overrides", {}),
            metadata=data.get("metadata", {})
        )
