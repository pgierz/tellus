from dataclasses import dataclass
from enum import Enum, auto
from typing import Dict, Optional, Type

import fsspec


# Allowed location names
class LocationKind(Enum):
    TAPE = auto()
    COMPUTE = auto()
    DISK = auto()


class LocationExistsError(Exception):
    pass


@dataclass
class Location:
    name: str
    kinds: list[LocationKind]
    config: dict
    optional: Optional[bool] = False

    def __post_init__(self):
        for kind in self.kinds:
            if not isinstance(kind, LocationKind):
                raise ValueError(
                    f"Location kind '{kind}' is not a valid LocationKind. "
                    f"Allowed values: {', '.join(e.name for e in LocationKind)}"
                )

    @classmethod
    def from_dict(cls, data):
        data["kinds"] = [LocationKind[kind.upper()] for kind in data["kinds"]]
        return cls(**data)

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
