#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Simulation objects for Earth System Models"""

import io
import json
from typing import Any, Dict, Optional, Union

# import itertools
import tarfile
import uuid

import fsspec

from .location import Location, LocationExistsError, create_location_handlers

# from snakemake.workflow import Rules, Workflow


class Simulation:
    """A Earth System Model Simulation

    This class represents a simulation in the Tellus system. A simulation can have
    multiple locations associated with it, which are used to store and retrieve data.

    Args:
        simulation_id: Optional unique identifier for the simulation. If not provided,
                     a UUID will be generated automatically.
        path: Optional filesystem path for the simulation data. If not provided,
              the simulation will be in-memory only.
    """

    def __init__(self, simulation_id: str | None = None, path: str | None = None):
        """Initialize a new simulation.

        Args:
            simulation_id: Optional unique identifier for the simulation.
            path: Optional filesystem path for the simulation data.
        """
        _uid = str(uuid.uuid4())
        if simulation_id:
            self.simulation_id = simulation_id
        else:
            self.simulation_id = _uid
        self._uid = _uid
        self.path = path
        self.attrs = {}
        self.data = None
        self.namelists = {}
        self.locations: dict[str, dict[str, object]] = {}
        self.snakemakes = {}
        """dict: A collection of snakemake rules this simulation knows about"""

    @property
    def uid(self):
        return self._uid

    def add_snakemake(self, rule_name, smk_file):
        """Add a snakemake rule to the simulation.

        Args:
            rule_name: The name of the snakemake rule.
            smk_file: The path to the snakemake file.
        """
        if rule_name in self.snakemakes:
            raise ValueError(f"Snakemake rule {rule_name} already exists.")
        self.snakemakes[rule_name] = smk_file

    def run_snakemake(self, rule_name):
        """Run a snakemake rule associated with this simulation."""
        if rule_name not in self.snakemakes:
            raise ValueError(f"Snakemake rule {rule_name} not found.")
        smk_file = self.snakemakes[rule_name]
        # Here you would implement the logic to run the snakemake file
        # For now, we just print a message
        print(f"Running snakemake rule {rule_name} from file {smk_file}")

    def add_location(self, location: Location, name: str = None, override=False):
        if isinstance(location, dict):
            location = Location.from_dict(location)
        if name is None:
            name = location.name
        handlers = create_location_handlers(location)
        if name in self.locations and not override:
            raise LocationExistsError(
                f"{location.name} already registered. Will not override with ``force=True``!"
            )
        self.locations[name] = {
            "location": location,
            "handlers": handlers,
        }

    set_location = add_location

    def get_location(self, name: str) -> Location | None:
        entry = self.locations.get(name)
        return entry["location"] if entry else None

    def post_to_location(self, name: str, data):
        entry = self.locations.get(name)
        if not entry:
            raise ValueError(f"Location {name} not set.")
        entry["handler"].post(data)

    def fetch_from_location(self, name: str, identifier):
        entry = self.locations.get(name)
        if not entry:
            raise ValueError(f"Location {name} not set.")
        return entry["handler"].fetch(identifier)

    def list_locations(self):
        return list(self.locations.keys())

    def remove_location(self, name: str):
        if name in self.locations:
            del self.locations[name]

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Simulation":
        """Create a Simulation instance from a dictionary.

        Args:
            data: Dictionary containing simulation data.
                Expected keys: simulation_id, uid, path, attrs, data,
                             namelists, locations, snakemakes

        Returns:
            Simulation: A new Simulation instance
        """
        # Create a new instance without calling __init__ directly
        sim = cls.__new__(cls)

        # Set basic attributes
        sim.simulation_id = data.get("simulation_id")
        sim._uid = data.get("uid", str(uuid.uuid4()))
        sim.path = data.get("path")

        # Set dictionaries with default empty dicts if not present
        sim.attrs = data.get("attrs", {})
        sim.namelists = data.get("namelists", {})
        sim.snakemakes = data.get("snakemakes", {})

        # Handle locations - convert dicts back to Location objects
        sim.locations = {}
        for name, loc_data in data.get("locations", {}).items():
            if isinstance(loc_data, dict):
                location = Location.from_dict(loc_data)
                sim.locations[name] = {
                    "location": location,
                    "handlers": create_location_handlers(location),
                }

        # Set data last as it might depend on other attributes
        sim.data = data.get("data")

        return sim

    def to_dict(self) -> Dict[str, Any]:
        """Convert the Simulation instance to a dictionary.

        Returns:
            Dict containing all serializable attributes of the Simulation
        """
        return {
            "simulation_id": self.simulation_id,
            "uid": self.uid,
            "path": self.path,
            "attrs": self.attrs,
            "data": self.data,
            "namelists": self.namelists,
            "locations": {
                name: loc["location"].to_dict() for name, loc in self.locations.items()
            },
            "snakemakes": self.snakemakes,
        }

    @classmethod
    def from_json(cls, json_str: str) -> "Simulation":
        """Create a Simulation instance from a JSON string.

        Args:
            json_str: JSON string containing simulation data

        Returns:
            Simulation: A new Simulation instance
        """
        data = json.loads(json_str)
        return cls.from_dict(data)

    def to_json(self, indent: Optional[int] = None) -> str:
        """Convert the Simulation instance to a JSON string.

        Args:
            indent: If specified, will pretty-print with the given indent level

        Returns:
            JSON string representation of the Simulation
        """
        return json.dumps(self.to_dict(), indent=indent, default=str)

    def save(self, filepath: str, indent: Optional[int] = 2) -> None:
        """Save the Simulation to a JSON file.

        Args:
            filepath: Path to save the JSON file
            indent: If specified, will pretty-print with the given indent level
        """
        with open(filepath, "w") as f:
            json.dump(self.to_dict(), f, indent=indent, default=str)

    @classmethod
    def load(cls, filepath: str) -> "Simulation":
        """Load a Simulation from a JSON file.

        Args:
            filepath: Path to the JSON file

        Returns:
            Simulation: A new Simulation instance loaded from the file
        """
        with open(filepath, "r") as f:
            data = json.load(f)
        return cls.from_dict(data)


class ArchivedSimulation:
    """Archive of a Simulation"""

    def list_files(self):
        raise NotImplementedError

    def open_file(self, filename):
        raise NotImplementedError

    def status(self):
        raise NotImplementedError


class SplitTarballArchivedSimulation(ArchivedSimulation):
    def __init__(self, part_files, fs=None):
        self.part_files = sorted(part_files)  # Ensure parts are in order
        self.fs = fs or fsspec.filesystem("file")  # Local or remote FS

    @classmethod
    def from_fs_path(cls, fs, path):
        # List all parts under path, filter and sort if needed
        listing = fs.ls(path)
        # Filter parts matching the base archive pattern (optional)
        # For now, assume all files in path are parts
        return cls(part_files=listing, fs=fs)

    def status(self):
        print("Split Tarball Archive Status:")
        for part in self.part_files:
            size = self.fs.size(part) if self.fs.exists(part) else "Unknown"
            staged_status = "unknown"
            print(f" - file: {part}")
            print(f"   size: {size}")
            print(f"   staged: {staged_status}")

    @classmethod
    def from_location(cls, location):
        fs = location.fs
        path = location.config["path"]
        return cls.from_fs_path(fs, path)

    def _assemble_tar_stream(self):
        """
        Open all parts as file-like objects and concatenate them into a single stream.
        This stream can be passed to tarfile for reading.
        """
        # Open each part with fsspec in binary read mode
        streams = [self.fs.open(part, "rb") for part in self.part_files]

        # Define a generator that yields chunks from each stream sequentially
        def stream_generator():
            for stream in streams:
                while True:
                    chunk = stream.read(1024 * 1024)  # 1 MiB chunks
                    if not chunk:
                        break
                    yield chunk
                stream.close()

        # Wrap the generator into a file-like object using io.BytesIO is not feasible for large files,
        # so use io.BufferedReader over io.RawIOBase or use a custom file-like class.
        # Here, we use io.BufferedReader over a generator-based raw stream.

        class StreamWrapper(io.RawIOBase):
            def __init__(self, gen):
                self._gen = gen
                self._buffer = b""
                self._exhausted = False

            def readable(self):
                return True

            def readinto(self, b):
                # Fill buffer if empty
                while len(self._buffer) < len(b) and not self._exhausted:
                    try:
                        self._buffer += next(self._gen)
                    except StopIteration:
                        self._exhausted = True
                        break
                # Copy data to b
                n = min(len(b), len(self._buffer))
                b[:n] = self._buffer[:n]
                self._buffer = self._buffer[n:]
                return n

        raw_stream = StreamWrapper(stream_generator())
        buffered_stream = io.BufferedReader(raw_stream)
        return buffered_stream

    def list_files(self):
        """
        List all files inside the assembled tarball.
        """
        with tarfile.open(fileobj=self._assemble_tar_stream(), mode="r:gz") as tar:
            members = tar.getmembers()
            print(f"Archive contains {len(members)} files/directories:")
            for member in members:
                print(f" - {member.name}")

    def open_file(self, filename):
        """
        Extract a single file from the tarball and return a BytesIO object.
        """
        with tarfile.open(fileobj=self._assemble_tar_stream(), mode="r:gz") as tar:
            try:
                member = tar.getmember(filename)
            except KeyError:
                raise FileNotFoundError(f"File {filename} not found in archive")

            extracted_file = tar.extractfile(member)
            if extracted_file is None:
                raise IsADirectoryError(f"{filename} is a directory in the archive")

            # Read contents into memory (consider streaming for large files)
            data = extracted_file.read()
            return io.BytesIO(data)


class OrganizedTarballArchivedSimulation(SplitTarballArchivedSimulation):
    """
    Example subclass for archives that contain multiple tarballs or have internal organization.
    Override methods as needed.
    """

    def list_files(self):
        # For example, list all tarballs inside the main archive or list files inside each tarball
        raise NotImplementedError("Implement based on specific archive organization")

    def open_file(self, filename):
        # Implement extraction logic depending on archive structure
        raise NotImplementedError("Implement based on specific archive organization")
