"""
Network abstraction implementations for testing.

This module provides concrete implementations of network interfaces
for both real network operations and test doubles.
"""

import time
import urllib.parse
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional
from unittest.mock import MagicMock

from .interfaces import NetworkInterface


class RealNetwork:
    """Real network implementation for integration tests."""
    
    def __init__(self, timeout: int = 30):
        """Initialize real network with timeout."""
        self.timeout = timeout
    
    def download(self, url: str, destination: str, progress_callback: Optional[Any] = None) -> str:
        """Download a file from a remote location."""
        # This would use actual network libraries like requests, fsspec, etc.
        # For now, we'll raise NotImplementedError to indicate this needs real implementation
        raise NotImplementedError("Real network implementation requires integration with actual networking libraries")
    
    def upload(self, local_path: str, remote_url: str, progress_callback: Optional[Any] = None) -> bool:
        """Upload a file to a remote location."""
        raise NotImplementedError("Real network implementation requires integration with actual networking libraries")
    
    def list_remote(self, url: str) -> List[str]:
        """List contents of remote directory."""
        raise NotImplementedError("Real network implementation requires integration with actual networking libraries")
    
    def remote_exists(self, url: str) -> bool:
        """Check if remote resource exists."""
        raise NotImplementedError("Real network implementation requires integration with actual networking libraries")
    
    def remote_size(self, url: str) -> int:
        """Get size of remote resource."""
        raise NotImplementedError("Real network implementation requires integration with actual networking libraries")


class FakeNetwork:
    """Fake network implementation that simulates network behavior."""
    
    def __init__(self):
        """Initialize fake network with configurable responses."""
        self._remote_files: Dict[str, bytes] = {}
        self._remote_directories: Dict[str, List[str]] = {}
        self._download_delays: Dict[str, float] = {}
        self._upload_delays: Dict[str, float] = {}
        self._failure_conditions: Dict[str, Exception] = {}
        self._call_log: List[Dict[str, Any]] = []
    
    def add_remote_file(self, url: str, content: bytes, download_delay: float = 0.0) -> None:
        """Add a fake remote file."""
        self._remote_files[url] = content
        if download_delay > 0:
            self._download_delays[url] = download_delay
    
    def add_remote_directory(self, url: str, contents: List[str]) -> None:
        """Add a fake remote directory."""
        self._remote_directories[url] = contents
    
    def set_failure_condition(self, url: str, exception: Exception) -> None:
        """Set a failure condition for a specific URL."""
        self._failure_conditions[url] = exception
    
    def download(self, url: str, destination: str, progress_callback: Optional[Any] = None) -> str:
        """Download a file from a remote location."""
        self._log_call("download", {"url": url, "destination": destination})
        
        # Check for failure conditions
        if url in self._failure_conditions:
            raise self._failure_conditions[url]
        
        # Check if file exists
        if url not in self._remote_files:
            raise FileNotFoundError(f"Remote file not found: {url}")
        
        content = self._remote_files[url]
        
        # Simulate download delay
        if url in self._download_delays:
            time.sleep(self._download_delays[url])
        
        # Simulate progress updates
        if progress_callback:
            total_size = len(content)
            chunk_size = max(1, total_size // 10)  # 10 progress updates
            
            for i in range(0, total_size, chunk_size):
                chunk = content[i:i + chunk_size]
                if hasattr(progress_callback, 'update'):
                    progress_callback.update(len(chunk))
                elif callable(progress_callback):
                    progress_callback(len(chunk))
        
        # Write to destination
        dest_path = Path(destination)
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        dest_path.write_bytes(content)
        
        return destination
    
    def upload(self, local_path: str, remote_url: str, progress_callback: Optional[Any] = None) -> bool:
        """Upload a file to a remote location."""
        self._log_call("upload", {"local_path": local_path, "remote_url": remote_url})
        
        # Check for failure conditions
        if remote_url in self._failure_conditions:
            raise self._failure_conditions[remote_url]
        
        # Check if local file exists
        local_file = Path(local_path)
        if not local_file.exists():
            raise FileNotFoundError(f"Local file not found: {local_path}")
        
        content = local_file.read_bytes()
        
        # Simulate upload delay
        if remote_url in self._upload_delays:
            time.sleep(self._upload_delays[remote_url])
        
        # Simulate progress updates
        if progress_callback:
            total_size = len(content)
            chunk_size = max(1, total_size // 10)  # 10 progress updates
            
            for i in range(0, total_size, chunk_size):
                chunk_size_actual = min(chunk_size, total_size - i)
                if hasattr(progress_callback, 'update'):
                    progress_callback.update(chunk_size_actual)
                elif callable(progress_callback):
                    progress_callback(chunk_size_actual)
        
        # Store uploaded content
        self._remote_files[remote_url] = content
        
        return True
    
    def list_remote(self, url: str) -> List[str]:
        """List contents of remote directory."""
        self._log_call("list_remote", {"url": url})
        
        # Check for failure conditions
        if url in self._failure_conditions:
            raise self._failure_conditions[url]
        
        if url in self._remote_directories:
            return self._remote_directories[url].copy()
        
        # Try to infer directory contents from file URLs
        prefix = url.rstrip('/') + '/'
        contents = []
        for file_url in self._remote_files:
            if file_url.startswith(prefix):
                relative_path = file_url[len(prefix):]
                # Only include direct children (no subdirectories)
                if '/' not in relative_path:
                    contents.append(relative_path)
        
        return contents
    
    def remote_exists(self, url: str) -> bool:
        """Check if remote resource exists."""
        self._log_call("remote_exists", {"url": url})
        
        # Check for failure conditions
        if url in self._failure_conditions:
            raise self._failure_conditions[url]
        
        return (url in self._remote_files or 
                url in self._remote_directories or
                any(file_url.startswith(url.rstrip('/') + '/') for file_url in self._remote_files))
    
    def remote_size(self, url: str) -> int:
        """Get size of remote resource."""
        self._log_call("remote_size", {"url": url})
        
        # Check for failure conditions
        if url in self._failure_conditions:
            raise self._failure_conditions[url]
        
        if url not in self._remote_files:
            raise FileNotFoundError(f"Remote file not found: {url}")
        
        return len(self._remote_files[url])
    
    def get_call_log(self) -> List[Dict[str, Any]]:
        """Get log of all method calls."""
        return self._call_log.copy()
    
    def clear_call_log(self) -> None:
        """Clear the call log."""
        self._call_log.clear()
    
    def clear_remote_data(self) -> None:
        """Clear all remote data."""
        self._remote_files.clear()
        self._remote_directories.clear()
        self._download_delays.clear()
        self._upload_delays.clear()
        self._failure_conditions.clear()
    
    def _log_call(self, method: str, args: Dict[str, Any]) -> None:
        """Log a method call."""
        self._call_log.append({
            "method": method,
            "args": args,
            "timestamp": time.time()
        })


class MockNetwork:
    """Mock network for isolated unit tests."""
    
    def __init__(self):
        """Initialize mock network."""
        self.download = MagicMock(return_value="/mock/downloaded/file.txt")
        self.upload = MagicMock(return_value=True)
        self.list_remote = MagicMock(return_value=["file1.txt", "file2.txt"])
        self.remote_exists = MagicMock(return_value=True)
        self.remote_size = MagicMock(return_value=1024)
    
    def configure_download(self, url_destination_mapping: Dict[str, str]) -> None:
        """Configure download behavior for specific URLs."""
        def side_effect(url, destination, progress_callback=None):
            return url_destination_mapping.get(url, destination)
        self.download.side_effect = side_effect
    
    def configure_remote_exists(self, url_exists_mapping: Dict[str, bool]) -> None:
        """Configure remote_exists behavior for specific URLs."""
        self.remote_exists.side_effect = lambda url: url_exists_mapping.get(url, True)
    
    def configure_list_remote(self, url_contents_mapping: Dict[str, List[str]]) -> None:
        """Configure list_remote behavior for specific URLs."""
        self.list_remote.side_effect = lambda url: url_contents_mapping.get(url, [])
    
    def configure_remote_size(self, url_size_mapping: Dict[str, int]) -> None:
        """Configure remote_size behavior for specific URLs."""
        self.remote_size.side_effect = lambda url: url_size_mapping.get(url, 1024)
    
    def configure_failure(self, method_name: str, exception: Exception) -> None:
        """Configure a method to raise an exception."""
        getattr(self, method_name).side_effect = exception


class SlowNetwork:
    """Network implementation that simulates slow connections for testing."""
    
    def __init__(self, base_network: NetworkInterface, delay_per_byte: float = 0.001):
        """Initialize slow network wrapper."""
        self._base_network = base_network
        self._delay_per_byte = delay_per_byte
    
    def download(self, url: str, destination: str, progress_callback: Optional[Any] = None) -> str:
        """Download with simulated network delay."""
        # Get file size first to calculate delay
        try:
            file_size = self._base_network.remote_size(url)
            delay = file_size * self._delay_per_byte
            time.sleep(delay)
        except Exception:
            # If can't get size, use default delay
            time.sleep(1.0)
        
        return self._base_network.download(url, destination, progress_callback)
    
    def upload(self, local_path: str, remote_url: str, progress_callback: Optional[Any] = None) -> bool:
        """Upload with simulated network delay."""
        # Calculate delay based on local file size
        try:
            file_size = Path(local_path).stat().st_size
            delay = file_size * self._delay_per_byte
            time.sleep(delay)
        except Exception:
            # If can't get size, use default delay
            time.sleep(1.0)
        
        return self._base_network.upload(local_path, remote_url, progress_callback)
    
    def list_remote(self, url: str) -> List[str]:
        """List remote contents with network delay."""
        time.sleep(0.5)  # Fixed delay for directory listings
        return self._base_network.list_remote(url)
    
    def remote_exists(self, url: str) -> bool:
        """Check remote existence with network delay."""
        time.sleep(0.1)  # Small delay for existence checks
        return self._base_network.remote_exists(url)
    
    def remote_size(self, url: str) -> int:
        """Get remote size with network delay."""
        time.sleep(0.1)  # Small delay for size queries
        return self._base_network.remote_size(url)


class NetworkRecorder:
    """Network implementation that records all operations for testing."""
    
    def __init__(self, base_network: NetworkInterface):
        """Initialize network recorder."""
        self._base_network = base_network
        self._operations: List[Dict[str, Any]] = []
    
    def download(self, url: str, destination: str, progress_callback: Optional[Any] = None) -> str:
        """Download and record the operation."""
        start_time = time.time()
        result = self._base_network.download(url, destination, progress_callback)
        end_time = time.time()
        
        self._record_operation("download", {
            "url": url,
            "destination": destination,
            "result": result,
            "duration": end_time - start_time
        })
        
        return result
    
    def upload(self, local_path: str, remote_url: str, progress_callback: Optional[Any] = None) -> bool:
        """Upload and record the operation."""
        start_time = time.time()
        result = self._base_network.upload(local_path, remote_url, progress_callback)
        end_time = time.time()
        
        self._record_operation("upload", {
            "local_path": local_path,
            "remote_url": remote_url,
            "result": result,
            "duration": end_time - start_time
        })
        
        return result
    
    def list_remote(self, url: str) -> List[str]:
        """List remote and record the operation."""
        start_time = time.time()
        result = self._base_network.list_remote(url)
        end_time = time.time()
        
        self._record_operation("list_remote", {
            "url": url,
            "result": result,
            "duration": end_time - start_time
        })
        
        return result
    
    def remote_exists(self, url: str) -> bool:
        """Check existence and record the operation."""
        start_time = time.time()
        result = self._base_network.remote_exists(url)
        end_time = time.time()
        
        self._record_operation("remote_exists", {
            "url": url,
            "result": result,
            "duration": end_time - start_time
        })
        
        return result
    
    def remote_size(self, url: str) -> int:
        """Get size and record the operation."""
        start_time = time.time()
        result = self._base_network.remote_size(url)
        end_time = time.time()
        
        self._record_operation("remote_size", {
            "url": url,
            "result": result,
            "duration": end_time - start_time
        })
        
        return result
    
    def get_operations(self) -> List[Dict[str, Any]]:
        """Get recorded operations."""
        return self._operations.copy()
    
    def clear_operations(self) -> None:
        """Clear recorded operations."""
        self._operations.clear()
    
    def _record_operation(self, operation: str, data: Dict[str, Any]) -> None:
        """Record an operation."""
        self._operations.append({
            "operation": operation,
            "timestamp": time.time(),
            **data
        })


class NetworkFactory:
    """Factory for creating network implementations."""
    
    @staticmethod
    def create_real(timeout: int = 30) -> RealNetwork:
        """Create real network implementation."""
        return RealNetwork(timeout)
    
    @staticmethod
    def create_fake() -> FakeNetwork:
        """Create fake network implementation."""
        return FakeNetwork()
    
    @staticmethod
    def create_mock() -> MockNetwork:
        """Create mock network implementation."""
        return MockNetwork()
    
    @staticmethod
    def create_slow(base_network: NetworkInterface, delay_per_byte: float = 0.001) -> SlowNetwork:
        """Create slow network wrapper."""
        return SlowNetwork(base_network, delay_per_byte)
    
    @staticmethod
    def create_recorder(base_network: NetworkInterface) -> NetworkRecorder:
        """Create network recorder wrapper."""
        return NetworkRecorder(base_network)