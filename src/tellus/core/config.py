"""Configuration management for tellus locations."""
from dataclasses import dataclass
from typing import Dict, Any, Optional
import json
import os
from pathlib import Path


@dataclass
class LocationConfig:
    name: str
    types: str
    protocol: str  
    path: str
    config: Dict[str, Any]


# Default configuration with the locations from the issue
DEFAULT_LOCATIONS = {
    "albedo": LocationConfig(
        name="albedo",
        types="COMPUTE", 
        protocol="sftp",
        path="/albedo/work/user/pgierz",
        config={
            "hostname": "albedo.awi.de",
            "username": "pgierz",
            "path": "/albedo/work/user/pgierz"  # This will cause the SSH error
        }
    ),
    "hsm.dmawi.de": LocationConfig(
        name="hsm.dmawi.de",
        types="TAPE",
        protocol="sftp", 
        path="-",
        config={
            "hostname": "hsm.dmawi.de",
            "username": "pgierz"
        }
    ),
    "localhost": LocationConfig(
        name="localhost",
        types="DISK",
        protocol="file",
        path="-", 
        config={
            "protocol": "file",
            "host": "localhost"
        }
    )
}


def get_locations() -> Dict[str, LocationConfig]:
    """Get configured locations."""
    # For now, return the default locations that match the issue
    return DEFAULT_LOCATIONS