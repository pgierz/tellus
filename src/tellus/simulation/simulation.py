#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Simulation objects for Earth System Models"""

import io
import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

# import itertools
import tarfile
import uuid

import fsspec

from ..location import Location, LocationExistsError
from .context import LocationContext

# from snakemake.workflow import Rules, Workflow


class SimulationExistsError(Exception):
    pass


class Simulation:
    """A Earth System Model Simulation

    This class represents a simulation in the Tellus system. A simulation can have
    multiple locations associated with it, which are used to store and retrieve data.

    Class Attributes:
        _simulations: Class variable to store all simulation instances
        _simulations_file: Path to the JSON file for persistence
    """

    _simulations: Dict[str, "Simulation"] = {}
    _simulations_file: Path = (
        Path(__file__).parent.parent.parent.parent / "simulations.json"
    )

    def __init__(
        self,
        simulation_id: str | None = None,
        path: str | None = None,
        model_id: str | None = None,
    ):
        """Initialize a new simulation.

        Args:
            simulation_id: Optional unique identifier for the simulation.
                         If not provided, a UUID will be generated.
            path: Optional filesystem path for the simulation data.
            model_id: Optional identifier for the model.
        """
        _uid = str(uuid.uuid4())
        if simulation_id:
            if simulation_id in Simulation._simulations:
                raise SimulationExistsError(
                    f"Simulation with ID '{simulation_id}' already exists"
                )
            self.simulation_id = simulation_id
        else:
            self.simulation_id = _uid

        self._uid = _uid
        self.path = path
        self.model_id = model_id
        self.attrs = {}
        self.data = None
        self.namelists = {}
        self.locations: dict[str, dict[str, object]] = {}
        self.snakemakes: Dict[str, Any] = {}
        """dict: A collection of snakemake rules this simulation knows about"""

        # Add to class registry
        Simulation._simulations[self.simulation_id] = self

    @classmethod
    def save_simulations(cls):
        """Save all simulations to disk."""
        cls._simulations_file.parent.mkdir(parents=True, exist_ok=True)
        data = {}
        for sim_id, sim in cls._simulations.items():
            # Convert locations to serializable format
            locations_data = {}
            for name, loc_data in sim.locations.items():
                loc_dict = {
                    "location": loc_data["location"].to_dict(),
                }
                if "context" in loc_data and loc_data["context"] is not None:
                    loc_dict["context"] = loc_data["context"].to_dict()
                locations_data[name] = loc_dict

            data[sim_id] = {
                "simulation_id": sim.simulation_id,
                "path": sim.path,
                "model_id": sim.model_id,
                "attrs": sim.attrs,
                "locations": locations_data,
                "namelists": sim.namelists,
                "snakemakes": sim.snakemakes,
            }

        # Write to file atomically
        temp_file = cls._simulations_file.with_suffix(".tmp")
        with open(temp_file, "w") as f:
            json.dump(data, f, indent=2, default=str)
        temp_file.replace(cls._simulations_file)

    @classmethod
    def load_simulations(cls):
        """Load simulations from disk."""
        if not cls._simulations_file.exists():
            return

        with open(cls._simulations_file, "r") as f:
            data = json.load(f)

        cls._simulations = {}
        for sim_id, sim_data in data.items():
            sim = Simulation(
                simulation_id=sim_data["simulation_id"],
                path=sim_data.get("path"),
                model_id=sim_data.get("model_id"),
            )

            # Set basic attributes
            sim.attrs = sim_data.get("attrs", {})
            sim.namelists = sim_data.get("namelists", {})
            sim.snakemakes = sim_data.get("snakemakes", {})

            # Handle locations
            sim.locations = {}
            for name, loc_data in sim_data.get("locations", {}).items():
                if not isinstance(loc_data, dict):
                    continue

                # Get location data (support both old and new format)
                location_data = loc_data.get("location", loc_data)
                if not isinstance(location_data, dict):
                    continue

                # Create location and handlers
                location = Location.from_dict(location_data)

                # Create location entry
                entry = {
                    "location": location,
                }

                # Add context if present
                if "context" in loc_data and isinstance(loc_data["context"], dict):
                    entry["context"] = LocationContext.from_dict(loc_data["context"])

                sim.locations[name] = entry

    @classmethod
    def get_simulation(cls, simulation_id: str) -> Optional["Simulation"]:
        """Get a simulation by ID."""
        return cls._simulations.get(simulation_id)

    @classmethod
    def list_simulations(cls) -> List["Simulation"]:
        """List all simulations."""
        return list(cls._simulations.values())

    @classmethod
    def delete_simulation(cls, simulation_id: str) -> bool:
        """Delete a simulation.

        Returns:
            bool: True if the simulation was deleted, False if it didn't exist.
        """
        if simulation_id in cls._simulations:
            del cls._simulations[simulation_id]
            return True
        return False

    def __del__(self):
        """Clean up when a simulation is deleted."""
        if self.simulation_id in Simulation._simulations:
            del Simulation._simulations[self.simulation_id]

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

    def add_location(
        self,
        location: Union[Location, dict],
        name: str = None,
        override: bool = False,
        context: Optional[Union[LocationContext, dict]] = None,
    ) -> None:
        """Add a location to the simulation with optional context.

        Args:
            location: The Location instance or dict to create a Location from
            name: Optional name for the location (defaults to location.name)
            override: If True, overwrite existing location with the same name
            context: Optional LocationContext or dict to initialize the context with
        """
        if isinstance(location, dict):
            location = Location.from_dict(location)

        if name is None:
            name = location.name

        handlers = create_location_handlers(location)

        if name in self.locations and not override:
            raise LocationExistsError(
                f"Location '{name}' already exists. Use override=True to replace it."
            )

        # Initialize context
        if context is None:
            context = LocationContext()
        elif isinstance(context, dict):
            context = LocationContext.from_dict(context)

        self.locations[name] = {
            "location": location,
            "handlers": handlers,
            "context": context,
        }

        # Save changes if this is an existing simulation
        if self.simulation_id in Simulation._simulations:
            self.save_simulations()

    set_location = add_location

    def get_location(self, name: str) -> Optional[Location]:
        """Get a location by name.

        Args:
            name: Name of the location to retrieve

        Returns:
            The Location instance or None if not found
        """
        entry = self.locations.get(name)
        return entry["location"] if entry else None

    def get_location_context(self, name: str) -> Optional[LocationContext]:
        """Get the context for a location.

        Args:
            name: Name of the location

        Returns:
            The LocationContext for the location, or None if not found
        """
        entry = self.locations.get(name)
        return entry.get("context") if entry else None

    def set_location_context(
        self, name: str, context: Union[LocationContext, dict], merge: bool = True
    ) -> None:
        """Set the context for a location.

        Args:
            name: Name of the location
            context: LocationContext or dict to set as context
            merge: If True, merge with existing context (default: True)

        Raises:
            ValueError: If the location doesn't exist
        """
        if name not in self.locations:
            raise ValueError(f"Location '{name}' not found in simulation")

        if isinstance(context, dict):
            context = LocationContext.from_dict(context)

        if merge and "context" in self.locations[name]:
            # Merge with existing context
            existing = self.locations[name]["context"]
            if existing.path_prefix and not context.path_prefix:
                context.path_prefix = existing.path_prefix
            existing.overrides.update(context.overrides)
            existing.metadata.update(context.metadata)
            context = existing

        self.locations[name]["context"] = context
        self.save_simulations()

    def get_location_path(self, name: str, *path_parts: str) -> str:
        """Get the full path for a location, applying any context.

        Args:
            name: Name of the location
            *path_parts: Additional path components to append

        Returns:
            The full path with context applied

        Raises:
            ValueError: If the location doesn't exist
        """
        if name not in self.locations:
            raise ValueError(f"Location '{name}' not in simulation")

        loc_data = self.locations[name]
        base_path = loc_data["location"].config.get("path", "/")  # Default to root...
        context = loc_data.get("context", LocationContext())

        # Start with the base path
        full_path = base_path

        # Apply path prefix if set
        if context and context.path_prefix:
            # Render template variables in path_prefix
            template = context.path_prefix
            if template:
                # Replace template variables
                template = template.replace("{{model_id}}", str(self.model_id or ""))
                template = template.replace(
                    "{{simulation_id}}", str(self.simulation_id or "")
                )
                # Join the template with the base path, handling absolute/relative paths
                full_path = str(Path(template.rstrip("/")) / base_path.lstrip("/"))

        # Add any additional path parts
        if path_parts:
            full_path = str(Path(full_path).joinpath(*path_parts))

        return full_path

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
        sim.attrs = data.get("attrs", {})
        sim.data = data.get("data")
        sim.namelists = data.get("namelists", {})
        sim.snakemakes = data.get("snakemakes", {})

        # Handle locations - convert dicts back to Location objects
        sim.locations = {}
        for name, loc_data in data.get("locations", {}).items():
            if isinstance(loc_data, dict):
                # Extract location data and context if present
                location_data = loc_data.get(
                    "location", loc_data
                )  # Backward compatible
                location = Location.from_dict(location_data)

                # Create location entry
                entry = {
                    "location": location,
                    "handlers": create_location_handlers(location),
                }

                # Add context if present
                if "context" in loc_data and loc_data["context"] is not None:
                    entry["context"] = LocationContext.from_dict(loc_data["context"])

                sim.locations[name] = entry

        # Set data last as it might depend on other attributes
        sim.data = data.get("data")

        return sim

    def to_dict(self) -> Dict[str, Any]:
        """Convert the Simulation instance to a dictionary.

        Returns:
            Dict containing all serializable attributes of the Simulation
        """
        locations_dict = {}
        for name, loc in self.locations.items():
            loc_dict = {
                "location": loc["location"].to_dict(),
            }
            if "context" in loc and loc["context"] is not None:
                loc_dict["context"] = loc["context"].to_dict()
            locations_dict[name] = loc_dict

        return {
            "simulation_id": self.simulation_id,
            "uid": self.uid,
            "path": self.path,
            "attrs": self.attrs,
            "data": self.data,
            "namelists": self.namelists,
            "locations": locations_dict,
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
