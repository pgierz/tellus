"""
Comprehensive tests for FileTransferApplicationService.

Tests cover all aspects of file transfer functionality including:
- Single file transfers with progress tracking
- Chunked transfer for large files
- Error handling and retry mechanisms
- Progress callbacks and metrics calculation
- Integration with location and progress services
"""

import asyncio
import tempfile
import time
from pathlib import Path
from typing import Any, Dict
from unittest.mock import AsyncMock, Mock, call, patch

import pytest

from .....application.dtos import (BatchFileTransferOperationDto,
                                   BatchFileTransferResultDto,
                                   CreateProgressTrackingDto,
                                   DirectoryTransferOperationDto,
                                   DirectoryTransferResultDto,
                                   FileTransferOperationDto,
                                   FileTransferResultDto, OperationType)
# Import application services and DTOs
from .....application.services.file_transfer_service import \
    FileTransferApplicationService
from ....architecture.file_transfer_base_tests import FileTransferTestBase
from ....architecture.file_transfer_factories import FileTransferDtoBuilder


class TestFileTransferApplicationService(FileTransferTestBase):
    """Test FileTransferApplicationService functionality."""
    
    def _setup_test_specific(self) -> None:
        """Set up file transfer service specific testing."""
        super()._setup_test_specific()
        
        # Create file transfer service with mocked dependencies
        self.file_transfer_service = self._create_file_transfer_service()
        
        # Track async tasks for cleanup
        self.async_tasks = []
    
    def _cleanup_test_specific(self) -> None:
        """Clean up async tasks and resources."""
        # Cancel any running async tasks
        for task in self.async_tasks:
            if not task.done():
                task.cancel()
        
        super()._cleanup_test_specific()
    
    def _create_file_transfer_service(self) -> FileTransferApplicationService:
        """Create file transfer service with mocked dependencies."""
        # Mock dependencies
        mock_location_service = self.mock_location_service
        mock_progress_service = self.mock_progress_service
        
        # Create service
        service = FileTransferApplicationService(
            location_repo=mock_location_service,
            progress_service=mock_progress_service
        )
        
        return service
    
    def test_single_file_transfer_success(self):
        """Test successful single file transfer."""
        # Arrange
        dto = self.create_file_transfer_dto(
            source={'location': 'local', 'path': str(self.test_files['small'])},
            destination={'location': 'test_location', 'path': str(self.temp_dest_dir / 'transferred.txt')}
        )
        
        # Act
        result = asyncio.run(self.file_transfer_service.transfer_file(dto))
        
        # Assert
        self.assert_transfer_completed(result)
        self.assertGreater(result.bytes_transferred, 0)
        self.assertIsNotNone(result.operation_id)
        
        # Verify progress service was called
        self.mock_progress_service.create_operation.assert_called()
        self.mock_progress_service.update_progress.assert_called()
    
    def test_large_file_chunked_transfer(self):
        """Test large file transfer with chunking."""
        # Arrange
        large_file_size = self.test_files['large'].stat().st_size
        dto = self.create_file_transfer_dto(
            source={'location': 'local', 'path': str(self.test_files['large'])},
            options={'chunk_size': 1024 * 1024}  # 1MB chunks
        )
        
        # Act
        result = asyncio.run(self.file_transfer_service.transfer_file(dto))
        
        # Assert
        self.assert_transfer_completed(result, large_file_size)
        self.assertGreater(result.throughput_mbps, 0)
        
        # Verify multiple progress updates for chunked transfer
        update_calls = self.mock_progress_service.update_progress.call_args_list
        self.assertGreater(len(update_calls), 1, "Expected multiple progress updates for chunked transfer")
    
    def test_file_transfer_with_verification(self):
        """Test file transfer with checksum verification."""
        # Arrange
        dto = self.create_file_transfer_dto(
            source={'location': 'local', 'path': str(self.test_files['medium'])},
            options={'verify_checksum': True}
        )
        
        # Act
        result = asyncio.run(self.file_transfer_service.transfer_file(dto))
        
        # Assert
        self.assert_transfer_completed(result)
        self.assertTrue(result.checksum_verified, "Checksum should be verified")
        self.assertIsNotNone(result.source_checksum)
        self.assertIsNotNone(result.dest_checksum)
        self.assertEqual(result.source_checksum, result.dest_checksum)
    
    def test_file_transfer_overwrite_existing(self):
        """Test file transfer with overwrite option."""
        # Arrange - create existing file at destination
        dest_file = self.temp_dest_dir / 'existing_file.txt'
        dest_file.write_text("Existing content")
        
        dto = self.create_file_transfer_dto(
            destination={'location': 'test_location', 'path': str(dest_file)},
            options={'overwrite': True}
        )
        
        # Act
        result = asyncio.run(self.file_transfer_service.transfer_file(dto))
        
        # Assert
        self.assert_transfer_completed(result)
        self.assertTrue(result.overwrite_performed, "File should have been overwritten")
        
        # Verify file content was replaced
        self.assertNotEqual(dest_file.read_text(), "Existing content")
    
    def test_file_transfer_no_overwrite_fails(self):
        """Test file transfer fails when destination exists and overwrite is False."""
        # Arrange - create existing file at destination
        dest_file = self.temp_dest_dir / 'existing_file.txt'
        dest_file.write_text("Existing content")
        
        dto = self.create_file_transfer_dto(
            destination={'location': 'test_location', 'path': str(dest_file)},
            options={'overwrite': False}
        )
        
        # Act
        result = asyncio.run(self.file_transfer_service.transfer_file(dto))
        
        # Assert
        self.assert_transfer_failed(result, "already exists")
    
    @patch('tellus.application.services.file_transfer_service.time.sleep')
    def test_file_transfer_retry_on_failure(self, mock_sleep):
        """Test retry mechanism on transfer failure."""
        # Arrange
        dto = self.create_file_transfer_dto()
        
        # Mock filesystem to fail first attempts, then succeed
        mock_fs = self.get_filesystem()
        mock_fs.copy.side_effect = [
            Exception("Network error"),
            Exception("Temporary failure"),
            None  # Success on third attempt
        ]
        
        # Act
        result = asyncio.run(self.file_transfer_service.transfer_file(dto))
        
        # Assert
        self.assert_transfer_completed(result)
        self.assertEqual(mock_fs.copy.call_count, 3, "Should have retried twice")
        self.assertEqual(mock_sleep.call_count, 2, "Should have waited between retries")
        
        # Verify exponential backoff
        sleep_calls = [call.args[0] for call in mock_sleep.call_args_list]
        self.assertEqual(sleep_calls[0], 1.0)  # First retry delay
        self.assertEqual(sleep_calls[1], 2.0)  # Second retry delay (exponential)
    
    def test_file_transfer_max_retries_exceeded(self):
        """Test transfer fails after maximum retries exceeded."""
        # Arrange
        dto = self.create_file_transfer_dto()
        
        # Mock filesystem to always fail
        mock_fs = self.get_filesystem()
        mock_fs.copy.side_effect = Exception("Persistent network error")
        
        # Act
        result = asyncio.run(self.file_transfer_service.transfer_file(dto))
        
        # Assert
        self.assert_transfer_failed(result, "Persistent network error")
        self.assertEqual(mock_fs.copy.call_count, 4, "Should have tried 4 times (initial + 3 retries)")
    
    def test_source_file_not_found(self):
        """Test transfer fails when source file doesn't exist."""
        # Arrange
        dto = self.create_file_transfer_dto(
            source={'location': 'local', 'path': '/nonexistent/file.txt'}
        )
        
        # Mock filesystem to return file not found
        mock_fs = self.get_filesystem()
        mock_fs.exists.return_value = False
        
        # Act
        result = asyncio.run(self.file_transfer_service.transfer_file(dto))
        
        # Assert
        self.assert_transfer_failed(result, "not found")
    
    def test_destination_location_not_found(self):
        """Test transfer fails when destination location doesn't exist."""
        # Arrange
        dto = self.create_file_transfer_dto(
            destination={'location': 'nonexistent_location', 'path': '/test/dest.txt'}
        )
        
        # Mock location service to return None
        self.mock_location_service.get_location.return_value = None
        
        # Act
        result = asyncio.run(self.file_transfer_service.transfer_file(dto))
        
        # Assert
        self.assert_transfer_failed(result, "Location not found")
    
    def test_progress_tracking_integration(self):
        """Test integration with progress tracking service."""
        # Arrange
        dto = self.create_file_transfer_dto()
        
        # Track progress service calls
        progress_calls = []
        
        async def mock_create_operation(progress_dto):
            progress_calls.append(('create', progress_dto))
            return {'operation_id': 'test_progress_id'}
        
        async def mock_update_progress(operation_id, update_dto):
            progress_calls.append(('update', operation_id, update_dto))
        
        self.mock_progress_service.create_operation.side_effect = mock_create_operation
        self.mock_progress_service.update_progress.side_effect = mock_update_progress
        
        # Act
        result = asyncio.run(self.file_transfer_service.transfer_file(dto))
        
        # Assert
        self.assert_transfer_completed(result)
        
        # Verify progress tracking calls
        self.assertGreater(len(progress_calls), 0, "Progress tracking should be called")
        
        # Verify create operation was called
        create_calls = [call for call in progress_calls if call[0] == 'create']
        self.assertEqual(len(create_calls), 1)
        
        create_dto = create_calls[0][1]
        self.assertEqual(create_dto.operation_type, OperationType.FILE_TRANSFER.value)
        
        # Verify update calls were made
        update_calls = [call for call in progress_calls if call[0] == 'update']
        self.assertGreater(len(update_calls), 0, "Progress should be updated during transfer")
    
    def test_directory_transfer_recursive(self):
        """Test recursive directory transfer."""
        # Arrange
        dto = DirectoryTransferOperationDto(
            source_location='local',
            source_path=str(self.test_files['directory']),
            dest_location='test_location',
            dest_path=str(self.temp_dest_dir / 'transferred_directory'),
            recursive=True,
            overwrite=False,
            verify_checksums=True
        )
        
        # Act
        result = asyncio.run(self.file_transfer_service.transfer_directory(dto))
        
        # Assert
        self.assertIsInstance(result, DirectoryTransferResultDto)
        self.assertTrue(result.success)
        self.assertEqual(result.files_transferred, 5)  # 5 files in test directory
        self.assertGreater(result.total_bytes_transferred, 0)
    
    def test_directory_transfer_with_patterns(self):
        """Test directory transfer with include/exclude patterns."""
        # Arrange
        dto = DirectoryTransferOperationDto(
            source_location='local',
            source_path=str(self.test_files['directory']),
            dest_location='test_location',
            dest_path=str(self.temp_dest_dir / 'filtered_directory'),
            recursive=True,
            include_patterns=['*.txt'],
            exclude_patterns=['file_0.txt'],  # Exclude specific file
            overwrite=False,
            verify_checksums=True
        )
        
        # Act
        result = asyncio.run(self.file_transfer_service.transfer_directory(dto))
        
        # Assert
        self.assertTrue(result.success)
        self.assertEqual(result.files_transferred, 4)  # 5 files - 1 excluded
        self.assertEqual(result.files_skipped, 1)
    
    def test_batch_file_transfer(self):
        """Test batch transfer of multiple files."""
        # Arrange
        file_transfers = [
            FileTransferOperationDto(
                source_location='local',
                source_path=str(self.test_files['small']),
                dest_location='test_location',
                dest_path=str(self.temp_dest_dir / 'batch_small.txt')
            ),
            FileTransferOperationDto(
                source_location='local',
                source_path=str(self.test_files['medium']),
                dest_location='test_location',
                dest_path=str(self.temp_dest_dir / 'batch_medium.bin')
            )
        ]
        
        dto = BatchFileTransferOperationDto(
            transfers=file_transfers,
            max_concurrent=2,
            stop_on_first_error=False
        )
        
        # Act
        result = asyncio.run(self.file_transfer_service.batch_transfer_files(dto))
        
        # Assert
        self.assertIsInstance(result, BatchFileTransferResultDto)
        self.assertTrue(result.success)
        self.assertEqual(len(result.successful_transfers), 2)
        self.assertEqual(len(result.failed_transfers), 0)
        self.assertGreater(result.total_bytes_transferred, 0)
    
    def test_batch_transfer_partial_failure(self):
        """Test batch transfer with some failures."""
        # Arrange
        file_transfers = [
            FileTransferOperationDto(
                source_location='local',
                source_path=str(self.test_files['small']),
                dest_location='test_location',
                dest_path=str(self.temp_dest_dir / 'batch_small.txt')
            ),
            FileTransferOperationDto(
                source_location='local',
                source_path='/nonexistent/file.txt',  # This will fail
                dest_location='test_location',
                dest_path=str(self.temp_dest_dir / 'batch_fail.txt')
            )
        ]
        
        dto = BatchFileTransferOperationDto(
            transfers=file_transfers,
            max_concurrent=1,
            stop_on_first_error=False
        )
        
        # Mock filesystem to fail for nonexistent file
        mock_fs = self.get_filesystem()
        def mock_exists(path):
            return not str(path).endswith('nonexistent/file.txt')
        mock_fs.exists.side_effect = mock_exists
        
        # Act
        result = asyncio.run(self.file_transfer_service.batch_transfer_files(dto))
        
        # Assert
        self.assertFalse(result.success)  # Overall failure due to partial failures
        self.assertEqual(len(result.successful_transfers), 1)
        self.assertEqual(len(result.failed_transfers), 1)
    
    def test_batch_transfer_stop_on_first_error(self):
        """Test batch transfer stops on first error when configured."""
        # Arrange
        file_transfers = [
            FileTransferOperationDto(
                source_location='local',
                source_path='/nonexistent/file1.txt',  # This will fail first
                dest_location='test_location',
                dest_path=str(self.temp_dest_dir / 'batch_fail1.txt')
            ),
            FileTransferOperationDto(
                source_location='local',
                source_path=str(self.test_files['small']),  # This shouldn't be processed
                dest_location='test_location',
                dest_path=str(self.temp_dest_dir / 'batch_small.txt')
            )
        ]
        
        dto = BatchFileTransferOperationDto(
            transfers=file_transfers,
            max_concurrent=1,
            stop_on_first_error=True
        )
        
        # Mock filesystem
        mock_fs = self.get_filesystem()
        mock_fs.exists.return_value = False  # All files fail
        
        # Act
        result = asyncio.run(self.file_transfer_service.batch_transfer_files(dto))
        
        # Assert
        self.assertFalse(result.success)
        self.assertEqual(len(result.failed_transfers), 1)  # Only first failure
        self.assertEqual(len(result.successful_transfers), 0)
    
    @pytest.mark.asyncio
    async def test_concurrent_transfer_limit(self):
        """Test that concurrent transfers are limited correctly."""
        # This test would need a more sophisticated setup to verify
        # concurrency limits are respected. For now, we test the interface.
        
        # Arrange
        file_transfers = [
            FileTransferOperationDto(
                source_location='local',
                source_path=str(self.test_files['small']),
                dest_location='test_location',
                dest_path=str(self.temp_dest_dir / f'concurrent_{i}.txt')
            )
            for i in range(5)
        ]
        
        dto = BatchFileTransferOperationDto(
            transfers=file_transfers,
            max_concurrent=2  # Limit to 2 concurrent
        )
        
        # Act
        result = await self.file_transfer_service.batch_transfer_files(dto)
        
        # Assert
        self.assertTrue(result.success)
        self.assertEqual(len(result.successful_transfers), 5)
    
    def test_transfer_with_custom_chunk_size(self):
        """Test transfer with custom chunk size."""
        # Arrange
        dto = self.create_file_transfer_dto(
            options={'chunk_size': 512}  # Very small chunks for testing
        )
        
        # Act
        result = asyncio.run(self.file_transfer_service.transfer_file(dto))
        
        # Assert
        self.assert_transfer_completed(result)
        
        # Verify many progress updates due to small chunks
        update_calls = self.mock_progress_service.update_progress.call_args_list
        self.assertGreater(len(update_calls), 5, "Small chunks should generate many progress updates")
    
    def test_transfer_metrics_calculation(self):
        """Test that transfer metrics are calculated correctly."""
        # Arrange
        dto = self.create_file_transfer_dto(
            source={'location': 'local', 'path': str(self.test_files['medium'])}
        )
        
        # Act
        start_time = time.time()
        result = asyncio.run(self.file_transfer_service.transfer_file(dto))
        end_time = time.time()
        
        # Assert
        self.assert_transfer_completed(result)
        
        # Verify metrics are reasonable
        self.assertGreater(result.duration_seconds, 0)
        self.assertLess(result.duration_seconds, end_time - start_time + 1)  # Allow some tolerance
        
        expected_throughput = result.bytes_transferred / (1024 * 1024) / result.duration_seconds
        self.assertAlmostEqual(result.throughput_mbps, expected_throughput, places=2)
    
    def _run_async(self, coro):
        """Helper to run async coroutines in sync tests."""
        task = asyncio.create_task(coro)
        self.async_tasks.append(task)
        return asyncio.run(coro)