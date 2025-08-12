"""
Integration test configuration and fixtures for the tellus system.

This module provides comprehensive fixtures and utilities for testing cross-system
integration, concurrency, error recovery, and performance scenarios in the tellus
Earth science data archive system.
"""

import asyncio
import contextlib
import hashlib
import os
import shutil
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional, Tuple
from unittest.mock import MagicMock, Mock, patch

import pytest
import fsspec
from rich.console import Console

from tellus.location import Location, LocationKind
from tellus.simulation import (
    Simulation, 
    CacheConfig, 
    CacheManager,
    CompressedArchive,
    ArchiveRegistry,
    PathMapper,
    PathMapping,
    TagSystem
)
from tellus.progress import FSSpecProgressCallback, ProgressConfig, set_progress_config


# Test environment configuration
@pytest.fixture(scope="session")
def test_env_config():
    """Configure test environment settings."""
    return {
        "max_workers": 4,
        "timeout_seconds": 30,
        "retry_attempts": 3,
        "network_delay_ms": 100,
        "cache_size_mb": 100,
        "concurrent_operations": 8
    }


@pytest.fixture(autouse=True)
def clean_registries():
    """Clean all registries before and after each test."""
    # Store original state
    original_sims = Simulation._simulations.copy()
    original_locs = Location._locations.copy()
    
    # Clear registries
    Simulation._simulations.clear()
    Location._locations.clear()
    
    yield
    
    # Restore original state
    Simulation._simulations = original_sims
    Location._locations = original_locs


@pytest.fixture
def temp_workspace():
    """Create temporary workspace for integration tests."""
    with tempfile.TemporaryDirectory(prefix="tellus_integration_") as temp_dir:
        workspace = Path(temp_dir)
        
        # Create standard directory structure
        dirs = [
            "cache", "archives", "local_storage", "remote_storage",
            "ssh_storage", "s3_storage", "tape_storage", "manifests"
        ]
        for dir_name in dirs:
            (workspace / dir_name).mkdir(exist_ok=True)
        
        yield workspace


@pytest.fixture
def cache_config(temp_workspace):
    """Create cache configuration for testing."""
    return CacheConfig(
        cache_dir=temp_workspace / "cache",
        archive_cache_size_limit=50 * 1024 * 1024,  # 50 MB for testing
        file_cache_size_limit=10 * 1024 * 1024,     # 10 MB for testing
        unified_cache=False
    )


@pytest.fixture
def cache_manager(cache_config):
    """Create cache manager for testing."""
    return CacheManager(cache_config)


@pytest.fixture
def sample_archive_data():
    """Generate sample archive data for testing."""
    import tarfile
    import io
    
    # Create in-memory tar.gz archive
    archive_buffer = io.BytesIO()
    
    with tarfile.open(fileobj=archive_buffer, mode='w:gz') as tar:
        # Add various file types
        files = {
            'input/forcing.nc': b'netcdf_data_placeholder' * 1000,
            'output/results.nc': b'output_netcdf_data' * 2000,
            'scripts/run.sh': b'#!/bin/bash\necho "Running simulation"',
            'namelists/config.nml': b'&namelist\n  param1 = true\n/',
            'logs/output.log': b'Simulation started\nProcessing...\nComplete',
            'restart/checkpoint.dat': b'binary_checkpoint_data' * 500
        }
        
        for filename, content in files.items():
            info = tarfile.TarInfo(name=filename)
            info.size = len(content)
            tar.addfile(info, io.BytesIO(content))
    
    archive_buffer.seek(0)
    return archive_buffer.getvalue()


@pytest.fixture
def test_locations(temp_workspace):
    """Create test locations for various storage backends."""
    locations = {}
    
    # Local filesystem location
    with patch('tellus.location.location.Location._save_locations'):
        locations['local'] = Location(
            name="local",
            kinds=[LocationKind.DISK],
            config={
                "protocol": "file",
                "path": str(temp_workspace / "local_storage")
            }
        )
        
        # Create SSH location but mock its filesystem to avoid network calls
        ssh_location = Location(
            name="ssh_remote",
            kinds=[LocationKind.COMPUTE],
            config={
                "protocol": "file",  # Use file protocol to avoid SSH connection
                "path": str(temp_workspace / "ssh_storage")
            }
        )
        locations['ssh'] = ssh_location
        
        # Create S3 location but use local filesystem
        s3_location = Location(
            name="s3_archive",
            kinds=[LocationKind.DISK],
            config={
                "protocol": "file",  # Use file protocol to avoid S3 connection
                "path": str(temp_workspace / "s3_storage")
            }
        )
        locations['s3'] = s3_location
        
        # Create Tape location but use local filesystem
        tape_location = Location(
            name="tape_backup",
            kinds=[LocationKind.TAPE],
            config={
                "protocol": "file",  # Use file protocol to avoid tape system
                "path": str(temp_workspace / "tape_storage")
            }
        )
        locations['tape'] = tape_location
    
    return locations


@pytest.fixture
def mock_network_conditions():
    """Mock various network conditions for testing."""
    class NetworkConditionMocker:
        def __init__(self):
            self.latency_ms = 0
            self.bandwidth_mbps = 100
            self.packet_loss = 0.0
            self.connection_failures = 0
            self.intermittent_failures = False
        
        @contextlib.contextmanager
        def simulate_conditions(self, latency_ms=0, bandwidth_mbps=100, 
                              packet_loss=0.0, connection_failures=0,
                              intermittent_failures=False):
            """Simulate specific network conditions."""
            old_values = (
                self.latency_ms, self.bandwidth_mbps, self.packet_loss,
                self.connection_failures, self.intermittent_failures
            )
            
            self.latency_ms = latency_ms
            self.bandwidth_mbps = bandwidth_mbps
            self.packet_loss = packet_loss
            self.connection_failures = connection_failures
            self.intermittent_failures = intermittent_failures
            
            try:
                yield self
            finally:
                (self.latency_ms, self.bandwidth_mbps, self.packet_loss,
                 self.connection_failures, self.intermittent_failures) = old_values
        
        def add_delay(self):
            """Add network latency delay."""
            if self.latency_ms > 0:
                time.sleep(self.latency_ms / 1000.0)
        
        def should_fail_connection(self):
            """Determine if connection should fail."""
            if self.connection_failures > 0:
                self.connection_failures -= 1
                return True
            return False
        
        def should_fail_intermittent(self):
            """Determine if operation should fail intermittently."""
            if self.intermittent_failures:
                import random
                return random.random() < 0.1  # 10% chance of failure
            return False
    
    return NetworkConditionMocker()


@pytest.fixture
def concurrent_executor(test_env_config):
    """Create thread pool executor for concurrent testing."""
    with ThreadPoolExecutor(max_workers=test_env_config["max_workers"]) as executor:
        yield executor


@pytest.fixture
def progress_tracker():
    """Create progress tracking utilities for testing."""
    class ProgressTracker:
        def __init__(self):
            self.operations = []
            self.events = []
            self.lock = threading.Lock()
        
        def track_operation(self, operation_id: str, operation_type: str, 
                          start_time: float = None):
            """Track a new operation."""
            with self.lock:
                self.operations.append({
                    'id': operation_id,
                    'type': operation_type,
                    'start_time': start_time or time.time(),
                    'end_time': None,
                    'success': None,
                    'error': None
                })
        
        def complete_operation(self, operation_id: str, success: bool = True, 
                             error: str = None):
            """Mark operation as complete."""
            with self.lock:
                for op in self.operations:
                    if op['id'] == operation_id:
                        op['end_time'] = time.time()
                        op['success'] = success
                        op['error'] = error
                        break
        
        def add_event(self, event_type: str, details: Dict[str, Any]):
            """Add a tracking event."""
            with self.lock:
                self.events.append({
                    'type': event_type,
                    'timestamp': time.time(),
                    'details': details
                })
        
        def get_stats(self) -> Dict[str, Any]:
            """Get operation statistics."""
            with self.lock:
                completed = [op for op in self.operations if op['end_time'] is not None]
                successful = [op for op in completed if op['success']]
                failed = [op for op in completed if not op['success']]
                
                return {
                    'total_operations': len(self.operations),
                    'completed_operations': len(completed),
                    'successful_operations': len(successful),
                    'failed_operations': len(failed),
                    'events': len(self.events),
                    'average_duration': sum(
                        op['end_time'] - op['start_time'] 
                        for op in completed
                    ) / len(completed) if completed else 0
                }
    
    return ProgressTracker()


@pytest.fixture
def filesystem_mocks():
    """Create filesystem mocks for different protocols."""
    class FileSystemMocks:
        def __init__(self):
            self.mocks = {}
            self.call_counts = {}
            self.failure_modes = {}
        
        def get_mock_fs(self, protocol: str):
            """Get mock filesystem for protocol."""
            if protocol not in self.mocks:
                mock_fs = Mock()
                mock_fs.protocol = protocol
                mock_fs.exists = Mock(return_value=True)
                mock_fs.size = Mock(return_value=1024 * 1024)  # 1MB default
                mock_fs.ls = Mock(return_value=[])
                mock_fs.open = Mock()
                mock_fs.get_file = Mock()
                mock_fs.put_file = Mock()
                
                self.mocks[protocol] = mock_fs
                self.call_counts[protocol] = {}
        
            return self.mocks[protocol]
        
        def set_failure_mode(self, protocol: str, operation: str, 
                           failure_type: str = "exception"):
            """Configure failure mode for specific operation."""
            if protocol not in self.failure_modes:
                self.failure_modes[protocol] = {}
            self.failure_modes[protocol][operation] = failure_type
        
        def track_call(self, protocol: str, operation: str):
            """Track filesystem operation calls."""
            if protocol not in self.call_counts:
                self.call_counts[protocol] = {}
            if operation not in self.call_counts[protocol]:
                self.call_counts[protocol][operation] = 0
            self.call_counts[protocol][operation] += 1
    
    return FileSystemMocks()


@pytest.fixture
def simulation_factory(test_locations, cache_manager):
    """Factory for creating test simulations."""
    def create_simulation(simulation_id: str = None, 
                         add_locations: List[str] = None) -> Simulation:
        """Create a test simulation with optional locations."""
        sim = Simulation(
            simulation_id=simulation_id or f"test_sim_{int(time.time())}",
            path="/test/simulation/path",
            model_id="test_model"
        )
        
        if add_locations:
            for loc_name in add_locations:
                if loc_name in test_locations:
                    with patch('tellus.simulation.simulation.Simulation.save_simulations'):
                        sim.add_location(test_locations[loc_name], loc_name)
        
        return sim
    
    return create_simulation


@pytest.fixture
def archive_factory(temp_workspace, sample_archive_data, cache_manager):
    """Factory for creating test archives."""
    def create_archive(archive_id: str = None, location_name: str = None,
                      archive_type: str = "compressed") -> CompressedArchive:
        """Create a test archive."""
        if not archive_id:
            archive_id = f"test_archive_{int(time.time())}"
        
        # Write sample archive to temp location
        archive_path = temp_workspace / "archives" / f"{archive_id}.tar.gz"
        archive_path.write_bytes(sample_archive_data)
        
        location = None
        if location_name == "local":
            # Create local location for the archive
            with patch('tellus.location.location.Location._save_locations'):
                location = Location(
                    name=f"archive_loc_{archive_id}",
                    kinds=[LocationKind.DISK],
                    config={
                        "protocol": "file",
                        "path": str(archive_path.parent)
                    }
                )
        
        archive = CompressedArchive(
            archive_id=archive_id,
            archive_location=str(archive_path),
            location=location,
            cache_manager=cache_manager
        )
        
        return archive
    
    return create_archive


@pytest.fixture
def error_injection():
    """Utilities for injecting errors during testing."""
    class ErrorInjector:
        def __init__(self):
            self.injection_points = {}
            self.call_counts = {}
        
        def inject_at(self, target: str, error_type: Exception = Exception,
                     error_message: str = "Injected error",
                     after_calls: int = 0):
            """Configure error injection at specific points."""
            self.injection_points[target] = {
                'error_type': error_type,
                'error_message': error_message,
                'after_calls': after_calls
            }
            self.call_counts[target] = 0
        
        def should_inject(self, target: str) -> bool:
            """Check if error should be injected."""
            if target not in self.injection_points:
                return False
            
            self.call_counts[target] += 1
            config = self.injection_points[target]
            
            return self.call_counts[target] > config['after_calls']
        
        def get_error(self, target: str) -> Exception:
            """Get error to inject."""
            config = self.injection_points[target]
            return config['error_type'](config['error_message'])
        
        def clear(self):
            """Clear all injection points."""
            self.injection_points.clear()
            self.call_counts.clear()
    
    return ErrorInjector()


@pytest.fixture
def performance_monitor():
    """Monitor performance metrics during tests."""
    class PerformanceMonitor:
        def __init__(self):
            self.metrics = {}
            self.start_times = {}
        
        def start_timing(self, operation: str):
            """Start timing an operation."""
            self.start_times[operation] = time.perf_counter()
        
        def end_timing(self, operation: str) -> float:
            """End timing and return duration."""
            if operation not in self.start_times:
                return 0.0
            
            duration = time.perf_counter() - self.start_times[operation]
            del self.start_times[operation]
            
            if operation not in self.metrics:
                self.metrics[operation] = []
            self.metrics[operation].append(duration)
            
            return duration
        
        @contextlib.contextmanager
        def time_operation(self, operation: str):
            """Context manager for timing operations."""
            self.start_timing(operation)
            try:
                yield
            finally:
                self.end_timing(operation)
        
        def get_stats(self, operation: str = None) -> Dict[str, Any]:
            """Get performance statistics."""
            if operation:
                if operation not in self.metrics:
                    return {}
                
                durations = self.metrics[operation]
                return {
                    'count': len(durations),
                    'total_time': sum(durations),
                    'avg_time': sum(durations) / len(durations),
                    'min_time': min(durations),
                    'max_time': max(durations)
                }
            else:
                return {op: self.get_stats(op) for op in self.metrics.keys()}
    
    return PerformanceMonitor()


@pytest.fixture
def resource_monitor():
    """Monitor system resource usage during tests."""
    import psutil
    
    class ResourceMonitor:
        def __init__(self):
            self.process = psutil.Process()
            self.snapshots = []
        
        def take_snapshot(self, label: str = ""):
            """Take a resource usage snapshot."""
            memory_info = self.process.memory_info()
            cpu_percent = self.process.cpu_percent()
            
            snapshot = {
                'timestamp': time.time(),
                'label': label,
                'memory_rss': memory_info.rss,
                'memory_vms': memory_info.vms,
                'cpu_percent': cpu_percent,
                'threads': self.process.num_threads()
            }
            
            self.snapshots.append(snapshot)
            return snapshot
        
        def get_memory_usage_mb(self) -> float:
            """Get current memory usage in MB."""
            return self.process.memory_info().rss / (1024 * 1024)
        
        def check_memory_leak(self, threshold_mb: float = 50) -> bool:
            """Check for memory leaks between snapshots."""
            if len(self.snapshots) < 2:
                return False
            
            first = self.snapshots[0]['memory_rss'] / (1024 * 1024)
            last = self.snapshots[-1]['memory_rss'] / (1024 * 1024)
            
            return (last - first) > threshold_mb
    
    return ResourceMonitor()


# Async testing support
@pytest.fixture
def async_test_runner():
    """Runner for async operations in integration tests."""
    class AsyncTestRunner:
        def __init__(self):
            self.loop = None
        
        def run_async(self, coro):
            """Run async coroutine in test."""
            return asyncio.run(coro)
        
        async def run_concurrent_operations(self, operations: List, max_concurrent: int = 5):
            """Run multiple async operations concurrently."""
            semaphore = asyncio.Semaphore(max_concurrent)
            
            async def run_with_semaphore(op):
                async with semaphore:
                    return await op
            
            tasks = [run_with_semaphore(op) for op in operations]
            return await asyncio.gather(*tasks, return_exceptions=True)
    
    return AsyncTestRunner()


# Configuration for integration tests
def pytest_configure(config):
    """Configure pytest for integration testing."""
    # Disable progress bars during testing
    set_progress_config(enabled=False)
    
    # Add custom markers
    config.addinivalue_line(
        "markers", "integration: mark test as integration test"
    )
    config.addinivalue_line(
        "markers", "concurrency: mark test as concurrency test"
    )
    config.addinivalue_line(
        "markers", "performance: mark test as performance test"
    )
    config.addinivalue_line(
        "markers", "slow: mark test as slow running"
    )
    config.addinivalue_line(
        "markers", "network: mark test as requiring network simulation"
    )


def pytest_collection_modifyitems(config, items):
    """Modify test collection for integration tests."""
    for item in items:
        # Add integration marker to all tests in integration directory
        if "integration" in str(item.fspath):
            item.add_marker(pytest.mark.integration)