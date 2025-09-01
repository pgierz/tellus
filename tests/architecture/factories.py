"""
Test factories and builders for domain objects.

This module provides factories and builders for creating test data
following the Builder pattern and Factory pattern, ensuring
consistent and maintainable test data creation.
"""

import random
import string
import tempfile
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Dict, Generic, List, Optional, Type, TypeVar

# Import domain objects (these would normally be imported from the actual domain)
# For now, we'll define minimal interfaces
from .interfaces import TestDataProvider

T = TypeVar('T')


class Builder(ABC, Generic[T]):
    """Abstract base class for builders following the Builder pattern."""
    
    @abstractmethod
    def build(self) -> T:
        """Build the object."""
        pass
    
    @abstractmethod
    def reset(self) -> 'Builder[T]':
        """Reset builder to initial state."""
        pass


class Factory(ABC, Generic[T]):
    """Abstract base class for factories following the Factory pattern."""
    
    @abstractmethod
    def create(self, **kwargs) -> T:
        """Create an object with optional parameters."""
        pass
    
    def create_batch(self, count: int, **kwargs) -> List[T]:
        """Create multiple objects."""
        return [self.create(**kwargs) for _ in range(count)]


# Sample domain object representations for testing
@dataclass
class TestLocation:
    """Test representation of Location domain object."""
    name: str
    kinds: List[str]
    config: Dict[str, Any]
    optional: bool = False


@dataclass
class TestSimulation:
    """Test representation of Simulation domain object."""
    simulation_id: str
    path: Optional[str] = None
    model_id: Optional[str] = None
    attrs: Dict[str, Any] = field(default_factory=dict)
    locations: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    namelists: Dict[str, str] = field(default_factory=dict)
    snakemakes: Dict[str, str] = field(default_factory=dict)


@dataclass
class TestArchive:
    """Test representation of Archive domain object."""
    archive_id: str
    location: str
    checksum: str
    size: int
    files: List[str] = field(default_factory=list)
    tags: Dict[str, List[str]] = field(default_factory=dict)


class LocationBuilder(Builder[TestLocation]):
    """Builder for creating test Location objects."""
    
    def __init__(self):
        """Initialize location builder."""
        self.reset()
    
    def reset(self) -> 'LocationBuilder':
        """Reset builder to default state."""
        self._name = f"test_location_{uuid.uuid4().hex[:8]}"
        self._kinds = ["DISK"]
        self._config = {"protocol": "file", "path": "/test/path"}
        self._optional = False
        return self
    
    def with_name(self, name: str) -> 'LocationBuilder':
        """Set location name."""
        self._name = name
        return self
    
    def with_kinds(self, *kinds: str) -> 'LocationBuilder':
        """Set location kinds."""
        self._kinds = list(kinds)
        return self
    
    def with_config(self, config: Dict[str, Any]) -> 'LocationBuilder':
        """Set location configuration."""
        self._config = config
        return self
    
    def with_protocol(self, protocol: str) -> 'LocationBuilder':
        """Set protocol in configuration."""
        self._config["protocol"] = protocol
        return self
    
    def with_path(self, path: str) -> 'LocationBuilder':
        """Set path in configuration."""
        self._config["path"] = path
        return self
    
    def with_storage_options(self, **options) -> 'LocationBuilder':
        """Add storage options to configuration."""
        if "storage_options" not in self._config:
            self._config["storage_options"] = {}
        self._config["storage_options"].update(options)
        return self
    
    def as_optional(self, optional: bool = True) -> 'LocationBuilder':
        """Set location as optional."""
        self._optional = optional
        return self
    
    def as_tape_location(self) -> 'LocationBuilder':
        """Configure as tape location."""
        return self.with_kinds("TAPE").with_protocol("tape")
    
    def as_compute_location(self) -> 'LocationBuilder':
        """Configure as compute location."""
        return self.with_kinds("COMPUTE").with_protocol("ssh")
    
    def as_s3_location(self, bucket: str, region: str = "us-east-1") -> 'LocationBuilder':
        """Configure as S3 location."""
        return (self.with_kinds("DISK")
                .with_protocol("s3")
                .with_path(f"s3://{bucket}")
                .with_storage_options(region=region))
    
    def build(self) -> TestLocation:
        """Build the location object."""
        return TestLocation(
            name=self._name,
            kinds=self._kinds.copy(),
            config=self._config.copy(),
            optional=self._optional
        )


class SimulationBuilder(Builder[TestSimulation]):
    """Builder for creating test Simulation objects."""
    
    def __init__(self):
        """Initialize simulation builder."""
        self.reset()
    
    def reset(self) -> 'SimulationBuilder':
        """Reset builder to default state."""
        self._simulation_id = f"test_sim_{uuid.uuid4().hex[:8]}"
        self._path = None
        self._model_id = None
        self._attrs = {}
        self._locations = {}
        self._namelists = {}
        self._snakemakes = {}
        return self
    
    def with_id(self, simulation_id: str) -> 'SimulationBuilder':
        """Set simulation ID."""
        self._simulation_id = simulation_id
        return self
    
    def with_path(self, path: str) -> 'SimulationBuilder':
        """Set simulation path."""
        self._path = path
        return self
    
    def with_model(self, model_id: str) -> 'SimulationBuilder':
        """Set model ID."""
        self._model_id = model_id
        return self
    
    def with_attr(self, key: str, value: Any) -> 'SimulationBuilder':
        """Add an attribute."""
        self._attrs[key] = value
        return self
    
    def with_attrs(self, **attrs) -> 'SimulationBuilder':
        """Add multiple attributes."""
        self._attrs.update(attrs)
        return self
    
    def with_location(self, name: str, location: TestLocation, context: Optional[Dict[str, Any]] = None) -> 'SimulationBuilder':
        """Add a location."""
        location_data = {
            "location": location,
        }
        if context:
            location_data["context"] = context
        self._locations[name] = location_data
        return self
    
    def with_namelist(self, name: str, content: str) -> 'SimulationBuilder':
        """Add a namelist."""
        self._namelists[name] = content
        return self
    
    def with_snakemake(self, rule_name: str, file_path: str) -> 'SimulationBuilder':
        """Add a snakemake rule."""
        self._snakemakes[rule_name] = file_path
        return self
    
    def as_awiesm_simulation(self) -> 'SimulationBuilder':
        """Configure as AWI-ESM simulation."""
        return (self.with_model("awiesm")
                .with_attr("model_version", "2.1")
                .with_attr("resolution", "T63")
                .with_namelist("namelist.echam", "# ECHAM namelist")
                .with_namelist("namelist.fesom", "# FESOM namelist"))
    
    def as_icon_simulation(self) -> 'SimulationBuilder':
        """Configure as ICON simulation."""
        return (self.with_model("icon")
                .with_attr("model_version", "2.6.4")
                .with_attr("grid", "R2B4")
                .with_namelist("icon_master.namelist", "# ICON master namelist"))
    
    def build(self) -> TestSimulation:
        """Build the simulation object."""
        return TestSimulation(
            simulation_id=self._simulation_id,
            path=self._path,
            model_id=self._model_id,
            attrs=self._attrs.copy(),
            locations=self._locations.copy(),
            namelists=self._namelists.copy(),
            snakemakes=self._snakemakes.copy()
        )


class ArchiveBuilder(Builder[TestArchive]):
    """Builder for creating test Archive objects."""
    
    def __init__(self):
        """Initialize archive builder."""
        self.reset()
    
    def reset(self) -> 'ArchiveBuilder':
        """Reset builder to default state."""
        self._archive_id = f"test_archive_{uuid.uuid4().hex[:8]}"
        self._location = "/test/archive.tar.gz"
        self._checksum = self._generate_checksum()
        self._size = 1024 * 1024  # 1MB default
        self._files = []
        self._tags = {}
        return self
    
    def with_id(self, archive_id: str) -> 'ArchiveBuilder':
        """Set archive ID."""
        self._archive_id = archive_id
        return self
    
    def with_location(self, location: str) -> 'ArchiveBuilder':
        """Set archive location."""
        self._location = location
        return self
    
    def with_checksum(self, checksum: str) -> 'ArchiveBuilder':
        """Set archive checksum."""
        self._checksum = checksum
        return self
    
    def with_size(self, size: int) -> 'ArchiveBuilder':
        """Set archive size."""
        self._size = size
        return self
    
    def with_file(self, file_path: str) -> 'ArchiveBuilder':
        """Add a file to the archive."""
        self._files.append(file_path)
        return self
    
    def with_files(self, *file_paths: str) -> 'ArchiveBuilder':
        """Add multiple files to the archive."""
        self._files.extend(file_paths)
        return self
    
    def with_tag(self, tag: str, *file_paths: str) -> 'ArchiveBuilder':
        """Add files with a specific tag."""
        if tag not in self._tags:
            self._tags[tag] = []
        self._tags[tag].extend(file_paths)
        return self
    
    def with_input_files(self, *file_paths: str) -> 'ArchiveBuilder':
        """Add input files."""
        return self.with_tag("input", *file_paths)
    
    def with_output_files(self, *file_paths: str) -> 'ArchiveBuilder':
        """Add output files."""
        return self.with_tag("output", *file_paths)
    
    def with_config_files(self, *file_paths: str) -> 'ArchiveBuilder':
        """Add configuration files."""
        return self.with_tag("config", *file_paths)
    
    def as_simulation_archive(self, simulation_id: str) -> 'ArchiveBuilder':
        """Configure as simulation archive with typical structure."""
        return (self.with_id(f"{simulation_id}_archive")
                .with_input_files("input/forcing.nc", "input/initial.nc")
                .with_output_files("output/data_001.nc", "output/data_002.nc")
                .with_config_files("config/namelist.echam", "config/namelist.fesom")
                .with_files("run_script.sh", "README.txt"))
    
    def build(self) -> TestArchive:
        """Build the archive object."""
        # Ensure all tagged files are in the files list
        all_tagged_files = []
        for files in self._tags.values():
            all_tagged_files.extend(files)
        
        # Add unique tagged files to the main files list
        for file_path in all_tagged_files:
            if file_path not in self._files:
                self._files.append(file_path)
        
        return TestArchive(
            archive_id=self._archive_id,
            location=self._location,
            checksum=self._checksum,
            size=self._size,
            files=self._files.copy(),
            tags=self._tags.copy()
        )
    
    def _generate_checksum(self) -> str:
        """Generate a random checksum for testing."""
        return ''.join(random.choices(string.hexdigits.lower(), k=32))


class LocationFactory(Factory[TestLocation]):
    """Factory for creating test Location objects."""
    
    def create(self, **kwargs) -> TestLocation:
        """Create a location with optional parameters."""
        builder = LocationBuilder()
        
        # Apply common parameters
        if 'name' in kwargs:
            builder.with_name(kwargs['name'])
        if 'kinds' in kwargs:
            builder.with_kinds(*kwargs['kinds'])
        if 'config' in kwargs:
            builder.with_config(kwargs['config'])
        if 'protocol' in kwargs:
            builder.with_protocol(kwargs['protocol'])
        if 'path' in kwargs:
            builder.with_path(kwargs['path'])
        if 'optional' in kwargs:
            builder.as_optional(kwargs['optional'])
        
        return builder.build()
    
    def create_disk_location(self, name: Optional[str] = None, path: Optional[str] = None) -> TestLocation:
        """Create a disk location."""
        builder = LocationBuilder().with_kinds("DISK").with_protocol("file")
        
        if name:
            builder.with_name(name)
        if path:
            builder.with_path(path)
        
        return builder.build()
    
    def create_tape_location(self, name: Optional[str] = None) -> TestLocation:
        """Create a tape location."""
        builder = LocationBuilder().as_tape_location()
        
        if name:
            builder.with_name(name)
        
        return builder.build()
    
    def create_compute_location(self, name: Optional[str] = None, host: Optional[str] = None) -> TestLocation:
        """Create a compute location."""
        builder = LocationBuilder().as_compute_location()
        
        if name:
            builder.with_name(name)
        if host:
            builder.with_storage_options(host=host)
        
        return builder.build()
    
    def create_s3_location(self, name: Optional[str] = None, bucket: Optional[str] = None) -> TestLocation:
        """Create an S3 location."""
        if not bucket:
            bucket = f"test-bucket-{uuid.uuid4().hex[:8]}"
        
        builder = LocationBuilder().as_s3_location(bucket)
        
        if name:
            builder.with_name(name)
        
        return builder.build()


class SimulationFactory(Factory[TestSimulation]):
    """Factory for creating test Simulation objects."""
    
    def create(self, **kwargs) -> TestSimulation:
        """Create a simulation with optional parameters."""
        builder = SimulationBuilder()
        
        # Apply common parameters
        if 'simulation_id' in kwargs:
            builder.with_id(kwargs['simulation_id'])
        if 'path' in kwargs:
            builder.with_path(kwargs['path'])
        if 'model_id' in kwargs:
            builder.with_model(kwargs['model_id'])
        if 'attrs' in kwargs:
            builder.with_attrs(**kwargs['attrs'])
        
        return builder.build()
    
    def create_awiesm_simulation(self, simulation_id: Optional[str] = None) -> TestSimulation:
        """Create an AWI-ESM simulation."""
        builder = SimulationBuilder().as_awiesm_simulation()
        
        if simulation_id:
            builder.with_id(simulation_id)
        
        return builder.build()
    
    def create_icon_simulation(self, simulation_id: Optional[str] = None) -> TestSimulation:
        """Create an ICON simulation."""
        builder = SimulationBuilder().as_icon_simulation()
        
        if simulation_id:
            builder.with_id(simulation_id)
        
        return builder.build()
    
    def create_with_locations(self, simulation_id: Optional[str] = None, 
                             location_names: Optional[List[str]] = None) -> TestSimulation:
        """Create a simulation with predefined locations."""
        builder = SimulationBuilder()
        
        if simulation_id:
            builder.with_id(simulation_id)
        
        # Create default locations
        location_factory = LocationFactory()
        
        if not location_names:
            location_names = ["local_disk", "remote_tape", "compute_cluster"]
        
        for location_name in location_names:
            if "disk" in location_name.lower():
                location = location_factory.create_disk_location(location_name)
            elif "tape" in location_name.lower():
                location = location_factory.create_tape_location(location_name)
            elif "compute" in location_name.lower():
                location = location_factory.create_compute_location(location_name)
            else:
                location = location_factory.create_disk_location(location_name)
            
            builder.with_location(location_name, location)
        
        return builder.build()


class ArchiveFactory(Factory[TestArchive]):
    """Factory for creating test Archive objects."""
    
    def create(self, **kwargs) -> TestArchive:
        """Create an archive with optional parameters."""
        builder = ArchiveBuilder()
        
        # Apply common parameters
        if 'archive_id' in kwargs:
            builder.with_id(kwargs['archive_id'])
        if 'location' in kwargs:
            builder.with_location(kwargs['location'])
        if 'size' in kwargs:
            builder.with_size(kwargs['size'])
        if 'files' in kwargs:
            builder.with_files(*kwargs['files'])
        
        return builder.build()
    
    def create_simulation_archive(self, simulation_id: str, 
                                 num_output_files: int = 10) -> TestArchive:
        """Create an archive for a simulation."""
        builder = ArchiveBuilder().as_simulation_archive(simulation_id)
        
        # Add additional output files
        for i in range(3, num_output_files + 1):
            builder.with_output_files(f"output/data_{i:03d}.nc")
        
        return builder.build()
    
    def create_large_archive(self, archive_id: Optional[str] = None,
                           size_mb: int = 100) -> TestArchive:
        """Create a large archive for testing."""
        builder = ArchiveBuilder()
        
        if archive_id:
            builder.with_id(archive_id)
        
        builder.with_size(size_mb * 1024 * 1024)
        
        # Add many files to simulate large archive
        for i in range(100):
            builder.with_file(f"data/file_{i:03d}.dat")
        
        return builder.build()


class TestDataFactory(TestDataProvider):
    """Factory for creating test data files and structures."""
    
    def __init__(self, temp_dir: Optional[Path] = None):
        """Initialize with optional temporary directory."""
        self._temp_dir = temp_dir or Path(tempfile.mkdtemp(prefix="tellus_test_data_"))
        self._created_files: List[Path] = []
    
    def get_sample_location_data(self) -> Dict[str, Any]:
        """Get sample location data."""
        return LocationFactory().create().to_dict() if hasattr(LocationFactory().create(), 'to_dict') else {
            "name": "sample_location",
            "kinds": ["DISK"],
            "config": {"protocol": "file", "path": "/sample/path"},
            "optional": False
        }
    
    def get_sample_simulation_data(self) -> Dict[str, Any]:
        """Get sample simulation data."""
        return SimulationFactory().create().to_dict() if hasattr(SimulationFactory().create(), 'to_dict') else {
            "simulation_id": "sample_simulation",
            "path": "/sample/simulation/path",
            "model_id": "sample_model",
            "attrs": {},
            "locations": {},
            "namelists": {},
            "snakemakes": {}
        }
    
    def get_sample_archive_data(self) -> bytes:
        """Get sample archive data."""
        # Create a simple tar.gz content for testing
        import io
        import tarfile
        
        buffer = io.BytesIO()
        with tarfile.open(fileobj=buffer, mode='w:gz') as tar:
            # Add a simple text file
            info = tarfile.TarInfo(name="sample.txt")
            content = b"Sample archive content"
            info.size = len(content)
            tar.addfile(info, io.BytesIO(content))
        
        return buffer.getvalue()
    
    def create_temp_file(self, content: str = "", suffix: str = "") -> Path:
        """Create temporary file with content."""
        temp_file = self._temp_dir / f"temp_file_{len(self._created_files)}{suffix}"
        temp_file.parent.mkdir(parents=True, exist_ok=True)
        temp_file.write_text(content)
        self._created_files.append(temp_file)
        return temp_file
    
    def create_temp_archive(self, files: Dict[str, str]) -> Path:
        """Create temporary archive with files."""
        import tarfile
        
        archive_path = self._temp_dir / f"temp_archive_{len(self._created_files)}.tar.gz"
        archive_path.parent.mkdir(parents=True, exist_ok=True)
        
        with tarfile.open(archive_path, 'w:gz') as tar:
            for filename, content in files.items():
                info = tarfile.TarInfo(name=filename)
                content_bytes = content.encode('utf-8')
                info.size = len(content_bytes)
                tar.addfile(info, io.BytesIO(content_bytes))
        
        self._created_files.append(archive_path)
        return archive_path
    
    def cleanup(self) -> None:
        """Clean up all created files."""
        import shutil
        for file_path in self._created_files:
            if file_path.exists():
                if file_path.is_file():
                    file_path.unlink()
                else:
                    shutil.rmtree(file_path)
        
        if self._temp_dir.exists():
            shutil.rmtree(self._temp_dir, ignore_errors=True)


# Convenience functions for quick object creation
def location(name: Optional[str] = None, **kwargs) -> TestLocation:
    """Create a test location with optional parameters."""
    factory = LocationFactory()
    if name:
        kwargs['name'] = name
    return factory.create(**kwargs)


def simulation(simulation_id: Optional[str] = None, **kwargs) -> TestSimulation:
    """Create a test simulation with optional parameters."""
    factory = SimulationFactory()
    if simulation_id:
        kwargs['simulation_id'] = simulation_id
    return factory.create(**kwargs)


def archive(archive_id: Optional[str] = None, **kwargs) -> TestArchive:
    """Create a test archive with optional parameters."""
    factory = ArchiveFactory()
    if archive_id:
        kwargs['archive_id'] = archive_id
    return factory.create(**kwargs)