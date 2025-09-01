"""
Abstract interfaces for testable components following Clean Architecture principles.

This module defines the contracts that enable dependency inversion and 
proper separation of concerns in the test suite architecture.
"""

import io
from abc import ABC, abstractmethod
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, List, Optional, Protocol, runtime_checkable


@runtime_checkable
class FileSystemInterface(Protocol):
    """Interface for filesystem operations to enable mocking and testing."""
    
    def exists(self, path: str) -> bool:
        """Check if a path exists."""
        ...
    
    def read_text(self, path: str) -> str:
        """Read text content from a file."""
        ...
    
    def write_text(self, path: str, content: str) -> None:
        """Write text content to a file."""
        ...
    
    def read_bytes(self, path: str) -> bytes:
        """Read binary content from a file."""
        ...
    
    def write_bytes(self, path: str, content: bytes) -> None:
        """Write binary content to a file."""
        ...
    
    def mkdir(self, path: str, parents: bool = False, exist_ok: bool = False) -> None:
        """Create a directory."""
        ...
    
    def rmtree(self, path: str, ignore_errors: bool = False) -> None:
        """Remove directory tree."""
        ...
    
    def list_dir(self, path: str) -> List[str]:
        """List directory contents."""
        ...
    
    def size(self, path: str) -> int:
        """Get file size."""
        ...
    
    @contextmanager
    def open(self, path: str, mode: str = 'r'):
        """Open a file with context manager."""
        ...


@runtime_checkable
class NetworkInterface(Protocol):
    """Interface for network operations to enable mocking and testing."""
    
    def download(self, url: str, destination: str, progress_callback: Optional[Any] = None) -> str:
        """Download a file from a remote location."""
        ...
    
    def upload(self, local_path: str, remote_url: str, progress_callback: Optional[Any] = None) -> bool:
        """Upload a file to a remote location."""
        ...
    
    def list_remote(self, url: str) -> List[str]:
        """List contents of remote directory."""
        ...
    
    def remote_exists(self, url: str) -> bool:
        """Check if remote resource exists."""
        ...
    
    def remote_size(self, url: str) -> int:
        """Get size of remote resource."""
        ...


@runtime_checkable
class CacheInterface(Protocol):
    """Interface for cache operations to enable dependency injection."""
    
    def get(self, key: str) -> Optional[bytes]:
        """Get cached data by key."""
        ...
    
    def put(self, key: str, data: bytes, ttl: Optional[int] = None) -> None:
        """Put data in cache with optional TTL."""
        ...
    
    def delete(self, key: str) -> bool:
        """Delete cached data."""
        ...
    
    def clear(self) -> None:
        """Clear all cached data."""
        ...
    
    def size(self) -> int:
        """Get cache size in bytes."""
        ...
    
    def stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        ...


@runtime_checkable
class ConfigurationInterface(Protocol):
    """Interface for configuration management."""
    
    def get(self, key: str, default: Any = None) -> Any:
        """Get configuration value."""
        ...
    
    def set(self, key: str, value: Any) -> None:
        """Set configuration value."""
        ...
    
    def has(self, key: str) -> bool:
        """Check if configuration key exists."""
        ...
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary."""
        ...
    
    def update(self, config: Dict[str, Any]) -> None:
        """Update configuration with dictionary."""
        ...


class LocationRepository(ABC):
    """Abstract repository for Location entities."""
    
    @abstractmethod
    def save(self, location: Any) -> None:
        """Save a location entity."""
        pass
    
    @abstractmethod
    def find_by_name(self, name: str) -> Optional[Any]:
        """Find location by name."""
        pass
    
    @abstractmethod
    def find_all(self) -> List[Any]:
        """Find all locations."""
        pass
    
    @abstractmethod
    def delete(self, name: str) -> bool:
        """Delete location by name."""
        pass
    
    @abstractmethod
    def exists(self, name: str) -> bool:
        """Check if location exists."""
        pass
    
    @abstractmethod
    def clear(self) -> None:
        """Clear all locations (for testing)."""
        pass


class SimulationRepository(ABC):
    """Abstract repository for Simulation entities."""
    
    @abstractmethod
    def save(self, simulation: Any) -> None:
        """Save a simulation entity."""
        pass
    
    @abstractmethod
    def find_by_id(self, simulation_id: str) -> Optional[Any]:
        """Find simulation by ID."""
        pass
    
    @abstractmethod
    def find_all(self) -> List[Any]:
        """Find all simulations."""
        pass
    
    @abstractmethod
    def delete(self, simulation_id: str) -> bool:
        """Delete simulation by ID."""
        pass
    
    @abstractmethod
    def exists(self, simulation_id: str) -> bool:
        """Check if simulation exists."""
        pass
    
    @abstractmethod
    def clear(self) -> None:
        """Clear all simulations (for testing)."""
        pass


class ProgressTracker(ABC):
    """Abstract interface for progress tracking."""
    
    @abstractmethod
    def start(self, operation: str, total: Optional[int] = None) -> None:
        """Start tracking an operation."""
        pass
    
    @abstractmethod
    def update(self, amount: int = 1, message: Optional[str] = None) -> None:
        """Update progress."""
        pass
    
    @abstractmethod
    def finish(self, message: Optional[str] = None) -> None:
        """Finish tracking."""
        pass
    
    @abstractmethod
    def set_total(self, total: int) -> None:
        """Set total progress amount."""
        pass


class LoggerInterface(ABC):
    """Abstract interface for logging."""
    
    @abstractmethod
    def debug(self, message: str, **kwargs) -> None:
        """Log debug message."""
        pass
    
    @abstractmethod
    def info(self, message: str, **kwargs) -> None:
        """Log info message."""
        pass
    
    @abstractmethod
    def warning(self, message: str, **kwargs) -> None:
        """Log warning message."""
        pass
    
    @abstractmethod
    def error(self, message: str, **kwargs) -> None:
        """Log error message."""
        pass


class EventPublisher(ABC):
    """Abstract interface for event publishing."""
    
    @abstractmethod
    def publish(self, event_type: str, data: Dict[str, Any]) -> None:
        """Publish an event."""
        pass
    
    @abstractmethod
    def subscribe(self, event_type: str, callback: Any) -> None:
        """Subscribe to an event type."""
        pass
    
    @abstractmethod
    def unsubscribe(self, event_type: str, callback: Any) -> None:
        """Unsubscribe from an event type."""
        pass


class ArchiveHandler(ABC):
    """Abstract interface for archive handling operations."""
    
    @abstractmethod
    def create_archive(self, source_path: str, archive_path: str) -> bool:
        """Create an archive from source path."""
        pass
    
    @abstractmethod
    def extract_archive(self, archive_path: str, destination: str) -> bool:
        """Extract archive to destination."""
        pass
    
    @abstractmethod
    def list_archive_contents(self, archive_path: str) -> List[str]:
        """List contents of archive."""
        pass
    
    @abstractmethod
    def extract_file(self, archive_path: str, file_path: str, destination: str) -> str:
        """Extract single file from archive."""
        pass
    
    @abstractmethod
    def validate_archive(self, archive_path: str) -> bool:
        """Validate archive integrity."""
        pass


class TestEnvironment(ABC):
    """Abstract interface for test environment management."""
    
    @abstractmethod
    def setup(self) -> None:
        """Setup test environment."""
        pass
    
    @abstractmethod
    def teardown(self) -> None:
        """Teardown test environment."""
        pass
    
    @abstractmethod
    def reset(self) -> None:
        """Reset environment to clean state."""
        pass
    
    @abstractmethod
    def get_temp_dir(self) -> Path:
        """Get temporary directory for test."""
        pass
    
    @abstractmethod
    def get_config(self) -> ConfigurationInterface:
        """Get test configuration."""
        pass


class TestDataProvider(ABC):
    """Abstract interface for test data provision."""
    
    @abstractmethod
    def get_sample_location_data(self) -> Dict[str, Any]:
        """Get sample location data."""
        pass
    
    @abstractmethod
    def get_sample_simulation_data(self) -> Dict[str, Any]:
        """Get sample simulation data."""
        pass
    
    @abstractmethod
    def get_sample_archive_data(self) -> bytes:
        """Get sample archive data."""
        pass
    
    @abstractmethod
    def create_temp_file(self, content: str = "", suffix: str = "") -> Path:
        """Create temporary file with content."""
        pass
    
    @abstractmethod
    def create_temp_archive(self, files: Dict[str, str]) -> Path:
        """Create temporary archive with files."""
        pass