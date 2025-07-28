from dataclasses import dataclass, asdict
from enum import Enum, auto
import json
import os
from pathlib import Path
from typing import ClassVar, Dict, Optional, Type, List

import fsspec
from .. import scoutfs  # noqa: F401


# Allowed location names
class LocationKind(Enum):
    TAPE = auto()
    COMPUTE = auto()
    DISK = auto()

    @classmethod
    def from_str(cls, s):
        try:
            return cls[s.upper()]
        except KeyError:
            raise ValueError(f"Invalid location kind: {s}")


class LocationExistsError(Exception):
    pass


@dataclass
class Location:
    # Class variable to track all location instances
    _locations: ClassVar[Dict[str, "Location"]] = {}
    _locations_file: ClassVar[Path] = (
        Path(__file__).parent.parent.parent.parent / "locations.json"
    )

    # Instance variables
    name: str
    kinds: list[LocationKind]
    config: dict
    optional: Optional[bool] = False

    def __post_init__(self):
        # Validate location kinds
        for kind in self.kinds:
            if not isinstance(kind, LocationKind):
                raise ValueError(
                    f"Location kind '{kind}' is not a valid LocationKind. "
                    f"Allowed values: {', '.join(e.name for e in LocationKind)}"
                )

        # Add to class registry
        if self.name in self._locations:
            raise LocationExistsError(
                f"Location with name '{self.name}' already exists"
            )
        self._locations[self.name] = self
        self._save_locations()

    @classmethod
    def from_dict(cls, data):
        # Create a new dictionary with the expected structure
        location_data = {
            "name": data.get("name", ""),  # This should be provided by the caller
            "kinds": [LocationKind[kind.upper()] for kind in data.get("kinds", [])],
            "config": data.get("config", {}),
            "optional": data.get("optional", False),
        }
        return cls(**location_data)

    @classmethod
    def load_locations(cls) -> None:
        """Load locations from the JSON file."""
        if not cls._locations_file.exists():
            cls._locations = {}
            return

        try:
            with open(cls._locations_file, "r") as f:
                locations_data = json.load(f)
                cls._locations = {}
                for name, data in locations_data.items():
                    # Ensure the name is included in the data
                    if "name" not in data:
                        data["name"] = name
                    # Create the location and add it to the dictionary
                    location = Location.from_dict(data)
                    cls._locations[name] = location
        except json.JSONDecodeError:
            # If the file is corrupted, reset to empty
            cls._locations = {}

    def _save_locations(self) -> None:
        """Save current locations to the JSON file."""
        # Ensure the parent directory exists
        self._locations_file.parent.mkdir(parents=True, exist_ok=True)

        # Convert locations to a serializable format
        locations_data = {
            name: {
                "kinds": [kind.name for kind in loc.kinds],
                "config": loc.config,
                "optional": loc.optional,
            }
            for name, loc in self._locations.items()
        }

        # Write to file atomically
        temp_file = self._locations_file.with_suffix(".tmp")
        with open(temp_file, "w") as f:
            json.dump(locations_data, f, indent=2)

        # On POSIX, this is an atomic operation
        temp_file.replace(self._locations_file)

    @classmethod
    def remove_location(cls, name: str) -> None:
        """Remove a location by name."""
        if name in cls._locations:
            del cls._locations[name]
            # Save the updated locations
            if cls._locations:  # If not empty, save the remaining locations
                cls._locations[next(iter(cls._locations))]._save_locations()
            else:  # If empty, remove the file
                try:
                    cls._locations_file.unlink(missing_ok=True)
                except Exception:
                    pass

    @classmethod
    def get_location(cls, name: str) -> Optional["Location"]:
        """Get a location by name."""
        return cls._locations.get(name)

    @classmethod
    def list_locations(cls) -> List["Location"]:
        """Get a list of all locations."""
        return list(cls._locations.values())

    def to_dict(self):
        return {
            "name": self.name,
            "kinds": [kind.name for kind in self.kinds],
            "config": self.config,
            "optional": self.optional,
        }

    @property
    def fs(self) -> fsspec.AbstractFileSystem:
        storage_options = self.config.get("storage_options", {})
        if "host" not in storage_options:
            storage_options["host"] = self.name
        fs = fsspec.filesystem(
            self.config.get("protocol", "file"),
            **storage_options,
        )  # [NOTE] Local filesystem

        return fs


# Handlers for each location type
class BaseLocationHandler:
    def post(self, data):
        raise NotImplementedError

    def get(self, identifier):
        # Separate method for possible direct access
        raise NotImplementedError

    def fetch(self, identifier):
        # Separate method for possible "non-local" access
        raise NotImplementedError


class HSMHandler(BaseLocationHandler):
    def post(self, data):
        print("Storing to HSM...")

    def get(self, identifier):
        print("Getting from HSM...")

    def fetch(self, identifier):
        # Separate method for possible "non-local" access
        print("Fetching from HSM...")


class HPCHandler(BaseLocationHandler):
    def post(self, data):
        print("Storing to HPC...")

    def get(self, identifier):
        print("Getting from HPC...")

    def fetch(self, identifier):
        # Separate method for possible "non-local" access
        print("Fetching from HPC...")


class FileServerHandler(BaseLocationHandler):
    def post(self, data):
        print("Storing to FileServer...")

    def get(self, identifier):
        print("Getting from FileServer...")

    def fetch(self, identifier):
        # Separate method for possible "non-local" access
        print("Fetching from FileServer...")


# The registry mapping allowed names to handler classes
LOCATION_REGISTRY: Dict[LocationKind, Type[BaseLocationHandler]] = {
    LocationKind.TAPE: HSMHandler,
    LocationKind.COMPUTE: HPCHandler,
    LocationKind.DISK: FileServerHandler,
}


def create_location_handlers(location: Location) -> list[BaseLocationHandler]:
    handler_clses = [LOCATION_REGISTRY.get(kind) for kind in location.kinds]
    if not all(handler_clses):
        raise ValueError(f"No handler registered for location: {location.kinds}")
    return [cls() for cls in handler_clses]
