from dataclasses import dataclass
from typing import Dict, Type

# Allowed location names
ALLOWED_LOCATIONS = {"HSM", "HPC", "FileServer"}


@dataclass
class Location:
    name: str
    kind: str
    config: dict

    def __post_init__(self):
        if self.kind not in ALLOWED_LOCATIONS:
            raise ValueError(
                f"Location kind '{self.kind}' is not allowed. Allowed: {ALLOWED_LOCATIONS}"
            )


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
LOCATION_REGISTRY: Dict[str, Type[BaseLocationHandler]] = {
    "HSM": HSMHandler,
    "HPC": HPCHandler,
    "FileServer": FileServerHandler,
}


def create_location_handler(location: Location) -> BaseLocationHandler:
    handler_cls = LOCATION_REGISTRY.get(location.kind)
    if not handler_cls:
        raise ValueError(f"No handler registered for location: {location.kind}")
    return handler_cls()
