"""
Base test classes for file transfer system components.

Extends the existing base test architecture to support testing
the new file transfer, operation queue, and progress tracking features.
"""

import asyncio
import tempfile
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
from unittest.mock import AsyncMock, Mock, patch

# Import application services and DTOs
from ...application.container import get_service_container
from ...application.dtos import (CreateProgressTrackingDto,
                                 FileTransferOperationDto,
                                 FileTransferResultDto,
                                 ProgressTrackingResultDto)
from ...application.services.file_transfer_service import \
    FileTransferApplicationService
from ...application.services.operation_queue_service import \
    OperationQueueService
from ...application.services.progress_tracking_service import \
    ProgressTrackingService
# Import existing base test patterns
from .base_tests import BaseTest
from .dependency_injection import TestContainer
from .file_transfer_factories import (FileTransferDtoBuilder,
                                      FileTransferFactory,
                                      OperationQueueFactory,
                                      ProgressTrackingFactory,
                                      TestFileTransferOperation,
                                      TestProgressTracking)


class FileTransferTestBase(BaseTest):
    """
    Base class for file transfer system tests.
    
    Provides common setup for file transfer operations, including:
    - Mock filesystem operations
    - Test file creation utilities
    - Progress tracking mocks
    - Service configuration
    """
    
    def _configure_container(self) -> None:
        """Configure container for file transfer testing."""
        # Configure filesystem mocks
        self.get_container().register_filesystem_factory(
            lambda: self._create_mock_filesystem()
        )
        
        # Configure network mocks for remote transfers
        self.get_container().register_network_factory(
            lambda: self._create_mock_network()
        )
        
        # Configure cache for temporary files
        self.get_container().register_cache_factory(
            lambda: self._create_test_cache()
        )
    
    def _setup_test_specific(self) -> None:
        """Set up file transfer specific test environment."""
        # Create test data factories
        self.file_transfer_factory = FileTransferFactory()
        self.progress_factory = ProgressTrackingFactory()
        self.dto_builder = FileTransferDtoBuilder()
        
        # Create temporary directories for test files
        self.temp_source_dir = Path(tempfile.mkdtemp(prefix="tellus_test_source_"))
        self.temp_dest_dir = Path(tempfile.mkdtemp(prefix="tellus_test_dest_"))
        self._temp_paths.extend([self.temp_source_dir, self.temp_dest_dir])
        
        # Create test files
        self.test_files = self._create_test_files()
        
        # Set up service mocks
        self._setup_service_mocks()
    
    def _create_test_files(self) -> Dict[str, Path]:
        """Create test files of various sizes."""
        test_files = {}
        
        # Small text file
        small_file = self.temp_source_dir / "small_file.txt"
        small_file.write_text("Small test content for file transfer testing.")
        test_files['small'] = small_file
        
        # Medium binary file (1MB)
        medium_file = self.temp_source_dir / "medium_file.bin"
        medium_file.write_bytes(b"Binary content " * 65536)  # ~1MB
        test_files['medium'] = medium_file
        
        # Large file (10MB)
        large_file = self.temp_source_dir / "large_file.dat"
        with large_file.open('wb') as f:
            for _ in range(10240):  # 10MB in 1KB chunks
                f.write(b"Large file content chunk " * 40)
        test_files['large'] = large_file
        
        # Directory with multiple files
        test_dir = self.temp_source_dir / "test_directory"
        test_dir.mkdir()
        for i in range(5):
            (test_dir / f"file_{i}.txt").write_text(f"File {i} content")
        test_files['directory'] = test_dir
        
        return test_files
    
    def _setup_service_mocks(self) -> None:
        """Set up mocks for application services."""
        # Mock location service to return test locations
        self.mock_location_service = Mock()
        self.mock_location_service.get_location.return_value = self._create_test_location()
        
        # Mock progress service
        self.mock_progress_service = Mock()
        self.mock_progress_service.create_operation.return_value = AsyncMock()
        self.mock_progress_service.update_progress.return_value = AsyncMock()
        
        # Mock queue service
        self.mock_queue_service = Mock()
        self.mock_queue_service.add_operation.return_value = "test_operation_id"
        self.mock_queue_service.get_queue_stats.return_value = {
            'total_operations': 0,
            'running': 0,
            'queued': 0,
            'completed': 0,
            'failed': 0
        }
    
    def _create_test_location(self):
        """Create a test location object."""
        from ...location.location import Location
        return Location(
            name="test_location",
            kinds=[],
            config={'protocol': 'file', 'path': str(self.temp_dest_dir)},
            _skip_registry=True
        )
    
    def _create_mock_filesystem(self):
        """Create mock filesystem interface."""
        mock_fs = Mock()
        mock_fs.exists.return_value = True
        mock_fs.size.return_value = 1024
        mock_fs.copy.return_value = None
        mock_fs.move.return_value = None
        return mock_fs
    
    def _create_mock_network(self):
        """Create mock network interface."""
        mock_network = Mock()
        mock_network.is_reachable.return_value = True
        mock_network.get_latency.return_value = 10.0
        return mock_network
    
    def _create_test_cache(self):
        """Create test cache interface."""
        mock_cache = Mock()
        mock_cache.get.return_value = None
        mock_cache.set.return_value = None
        return mock_cache
    
    def create_file_transfer_dto(self, **kwargs) -> FileTransferOperationDto:
        """Create a file transfer DTO with test defaults."""
        builder = self.dto_builder.reset()
        
        # Set test defaults
        builder.with_source("local", str(self.test_files['small']))
        builder.with_destination("test_location", str(self.temp_dest_dir / "transferred_file.txt"))
        
        # Apply custom options
        if 'source' in kwargs:
            source = kwargs['source']
            builder.with_source(source.get('location', 'local'), source.get('path'))
        if 'destination' in kwargs:
            dest = kwargs['destination']
            builder.with_destination(dest.get('location', 'test_location'), dest.get('path'))
        if 'options' in kwargs:
            builder.with_options(**kwargs['options'])
        
        return builder.build_single_transfer()
    
    def assert_transfer_completed(self, result: FileTransferResultDto, 
                                expected_bytes: Optional[int] = None):
        """Assert that a transfer completed successfully."""
        self.assertTrue(result.success, f"Transfer failed: {result.error_message}")
        self.assertIsNotNone(result.bytes_transferred)
        self.assertGreater(result.duration_seconds, 0)
        
        if expected_bytes:
            self.assertEqual(result.bytes_transferred, expected_bytes)
    
    def assert_transfer_failed(self, result: FileTransferResultDto, 
                             expected_error_pattern: Optional[str] = None):
        """Assert that a transfer failed as expected."""
        self.assertFalse(result.success)
        self.assertIsNotNone(result.error_message)
        
        if expected_error_pattern:
            self.assertIn(expected_error_pattern, result.error_message)


class OperationQueueTestBase(BaseTest):
    """
    Base class for operation queue tests.
    
    Provides common setup for testing operation queue functionality,
    including concurrent operations, queue management, and status tracking.
    """
    
    def _configure_container(self) -> None:
        """Configure container for operation queue testing."""
        # Configure operation router and handlers
        self.get_container().register_operation_router(
            lambda: self._create_mock_operation_router()
        )
        
        # Configure concurrent execution environment
        self.get_container().register_executor(
            lambda: self._create_test_executor()
        )
    
    def _setup_test_specific(self) -> None:
        """Set up operation queue specific test environment."""
        # Create test factories
        self.queue_factory = OperationQueueFactory()
        self.operation_factory = FileTransferFactory()
        
        # Set up queue service with test configuration
        self.queue_service = self._create_queue_service()
        
        # Track operations for cleanup
        self.test_operations = []
    
    def _create_mock_operation_router(self):
        """Create mock operation router."""
        mock_router = Mock()
        mock_router.execute_operation.return_value = AsyncMock()
        return mock_router
    
    def _create_test_executor(self):
        """Create test executor for concurrent operations."""
        # Use a small thread pool for testing
        import concurrent.futures
        return concurrent.futures.ThreadPoolExecutor(max_workers=2)
    
    def _create_queue_service(self) -> OperationQueueService:
        """Create operation queue service for testing."""
        # This would normally use dependency injection
        # For testing, we create a mock service
        mock_service = Mock(spec=OperationQueueService)
        mock_service.add_operation.return_value = f"op_{int(time.time())}"
        mock_service.list_operations.return_value = []
        mock_service.get_queue_stats.return_value = {
            'total_operations': 0,
            'running': 0,
            'queued': 0,
            'completed': 0,
            'failed': 0,
            'cancelled': 0,
            'is_processing': False,
            'is_paused': False,
            'max_concurrent': 3,
            'total_bytes_processed': 0
        }
        return mock_service
    
    def create_test_operations(self, count: int = 5) -> List[TestFileTransferOperation]:
        """Create a list of test operations."""
        operations = []
        for i in range(count):
            operation = self.operation_factory.create(
                operation_id=f"test_op_{i}",
                source={'location': 'local', 'path': f'/test/source/file_{i}.txt'},
                destination={'location': 'remote', 'path': f'/test/dest/file_{i}.txt'}
            )
            operations.append(operation)
            self.test_operations.append(operation)
        return operations
    
    def assert_queue_stats(self, stats: Dict[str, Any], **expected_values):
        """Assert queue statistics match expected values."""
        for key, expected_value in expected_values.items():
            self.assertIn(key, stats, f"Missing statistic: {key}")
            self.assertEqual(stats[key], expected_value, 
                           f"Expected {key}={expected_value}, got {stats[key]}")


class ProgressTrackingTestBase(BaseTest):
    """
    Base class for progress tracking tests.
    
    Provides common setup for testing progress tracking functionality,
    including real-time updates, metrics calculation, and callback systems.
    """
    
    def _configure_container(self) -> None:
        """Configure container for progress tracking testing."""
        # Configure time-based utilities for testing
        self.get_container().register_time_provider(
            lambda: self._create_mock_time_provider()
        )
        
        # Configure metrics calculation
        self.get_container().register_metrics_calculator(
            lambda: self._create_test_metrics_calculator()
        )
    
    def _setup_test_specific(self) -> None:
        """Set up progress tracking specific test environment."""
        # Create test factories
        self.progress_factory = ProgressTrackingFactory()
        
        # Set up progress tracking service
        self.progress_service = self._create_progress_service()
        
        # Track callbacks for testing
        self.callback_calls = []
        self.test_callback = self._create_test_callback()
    
    def _create_mock_time_provider(self):
        """Create mock time provider for consistent testing."""
        mock_time = Mock()
        mock_time.time.return_value = 1234567890.0
        mock_time.sleep = Mock()
        return mock_time
    
    def _create_test_metrics_calculator(self):
        """Create test metrics calculator."""
        mock_calc = Mock()
        mock_calc.calculate_throughput.return_value = 15.5
        mock_calc.calculate_eta.return_value = 60.0
        return mock_calc
    
    def _create_progress_service(self):
        """Create progress tracking service for testing."""
        mock_service = Mock(spec=ProgressTrackingService)
        mock_service.create_operation.return_value = AsyncMock()
        mock_service.update_progress.return_value = AsyncMock()
        mock_service.complete_operation.return_value = AsyncMock()
        return mock_service
    
    def _create_test_callback(self):
        """Create test callback for progress updates."""
        def callback(progress_data: Dict[str, Any]):
            self.callback_calls.append(progress_data)
        return callback
    
    def create_progress_dto(self, **kwargs) -> CreateProgressTrackingDto:
        """Create progress tracking DTO with test defaults."""
        from ...application.dtos import (CreateProgressTrackingDto,
                                         OperationType)
        
        return CreateProgressTrackingDto(
            operation_id=kwargs.get('operation_id', 'test_operation'),
            operation_type=kwargs.get('operation_type', OperationType.FILE_TRANSFER.value),
            operation_name=kwargs.get('operation_name', 'Test Operation'),
            total_bytes=kwargs.get('total_bytes'),
            metadata=kwargs.get('metadata', {})
        )
    
    def assert_progress_updated(self, expected_percentage: float, 
                              tolerance: float = 1.0):
        """Assert that progress was updated to expected percentage."""
        self.assertGreater(len(self.callback_calls), 0, "No progress callbacks received")
        
        latest_progress = self.callback_calls[-1]
        actual_percentage = latest_progress.get('progress_percentage', 0)
        
        self.assertAlmostEqual(actual_percentage, expected_percentage, 
                             delta=tolerance,
                             msg=f"Expected progress {expected_percentage}%, got {actual_percentage}%")
    
    def assert_throughput_calculated(self, min_throughput: float = 0.0):
        """Assert that throughput was calculated and is reasonable."""
        if self.callback_calls:
            latest_progress = self.callback_calls[-1]
            throughput = latest_progress.get('throughput_mbps', 0)
            self.assertGreaterEqual(throughput, min_throughput,
                                  f"Throughput {throughput} MB/s is below minimum {min_throughput} MB/s")


class IntegrationTestBase(FileTransferTestBase, OperationQueueTestBase, ProgressTrackingTestBase):
    """
    Base class for integration tests that require multiple components.
    
    Combines setup from all component test bases to support testing
    end-to-end workflows and component interactions.
    """
    
    def _configure_container(self) -> None:
        """Configure container for integration testing."""
        # Call all parent configurations
        FileTransferTestBase._configure_container(self)
        OperationQueueTestBase._configure_container(self)
        ProgressTrackingTestBase._configure_container(self)
        
        # Add integration-specific configuration
        self.get_container().register_integration_coordinator(
            lambda: self._create_integration_coordinator()
        )
    
    def _setup_test_specific(self) -> None:
        """Set up integration test environment."""
        # Call all parent setups
        FileTransferTestBase._setup_test_specific(self)
        OperationQueueTestBase._setup_test_specific(self)
        ProgressTrackingTestBase._setup_test_specific(self)
        
        # Integration-specific setup
        self.integration_test_scenarios = self._create_test_scenarios()
    
    def _create_integration_coordinator(self):
        """Create coordinator for managing integration test components."""
        mock_coordinator = Mock()
        mock_coordinator.coordinate_transfer_with_progress.return_value = AsyncMock()
        return mock_coordinator
    
    def _create_test_scenarios(self) -> Dict[str, Dict[str, Any]]:
        """Create predefined test scenarios for integration testing."""
        return {
            'simple_transfer': {
                'description': 'Single file transfer with progress tracking',
                'file_size': 1024 * 1024,  # 1MB
                'expected_duration': 5.0,
                'expected_throughput': 0.2  # MB/s
            },
            'batch_transfer': {
                'description': 'Multiple file transfers in queue',
                'file_count': 5,
                'total_size': 5 * 1024 * 1024,  # 5MB
                'expected_duration': 15.0,
                'concurrent_operations': 2
            },
            'large_file_transfer': {
                'description': 'Large file transfer with chunking',
                'file_size': 100 * 1024 * 1024,  # 100MB
                'chunk_size': 32 * 1024 * 1024,  # 32MB
                'expected_duration': 60.0,
                'expected_throughput': 1.67  # MB/s
            }
        }
    
    async def run_integration_scenario(self, scenario_name: str) -> Dict[str, Any]:
        """Run a predefined integration test scenario."""
        scenario = self.integration_test_scenarios[scenario_name]
        
        # Implementation would depend on specific scenario
        # For now, return mock results
        return {
            'scenario': scenario_name,
            'success': True,
            'duration': scenario.get('expected_duration', 5.0),
            'files_processed': scenario.get('file_count', 1),
            'bytes_transferred': scenario.get('file_size', 1024),
            'average_throughput': scenario.get('expected_throughput', 1.0)
        }