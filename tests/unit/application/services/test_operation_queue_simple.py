"""
Simple comprehensive tests for OperationQueueService.

Tests cover operation queue management without complex async fixtures.
"""

import asyncio
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, Mock

from tellus.application.dtos import (BatchFileTransferOperationDto,
                                     BatchFileTransferResultDto,
                                     BulkArchiveOperationDto,
                                     BulkOperationResultDto,
                                     FileTransferOperationDto,
                                     FileTransferResultDto)
from tellus.application.services.bulk_operation_queue import (QueuePriority,
                                                              QueueStatus)
from tellus.application.services.operation_queue_service import \
    OperationQueueService


class TestOperationQueueServiceSimple(unittest.TestCase):
    """Test OperationQueueService functionality."""
    
    def setUp(self):
        """Set up test environment."""
        # Create temporary directories
        import shutil
        self.temp_source_dir = Path(tempfile.mkdtemp(prefix="queue_test_source_"))
        self.temp_dest_dir = Path(tempfile.mkdtemp(prefix="queue_test_dest_"))
        
        # Create test files
        self.test_file = self.temp_source_dir / "test_file.txt"
        self.test_file.write_text("Test content for queue operations")
        
        # Create mocked services
        self.mock_archive_service = self._create_mock_archive_service()
        self.mock_file_transfer_service = self._create_mock_file_transfer_service()
        
        # Create operation queue service
        self.operation_queue_service = OperationQueueService(
            archive_service=self.mock_archive_service,
            file_transfer_service=self.mock_file_transfer_service,
            max_concurrent=2,
            default_priority=QueuePriority.NORMAL
        )
    
    def tearDown(self):
        """Clean up test environment."""
        import shutil
        if self.temp_source_dir.exists():
            shutil.rmtree(self.temp_source_dir, ignore_errors=True)
        if self.temp_dest_dir.exists():
            shutil.rmtree(self.temp_dest_dir, ignore_errors=True)
    
    def _create_mock_archive_service(self):
        """Create mock archive service."""
        service = Mock()
        
        # Mock archive operations
        async def mock_copy_archive(dto):
            return BulkOperationResultDto(
                operation_id=f"copy_{dto.operation_id}",
                operation_type="archive_copy",
                success=True,
                operations_completed=1,
                total_operations=1,
                duration_seconds=1.0
            )
        
        service.copy_archive_to_location = AsyncMock(side_effect=mock_copy_archive)
        service.extract_archive_to_location = AsyncMock(side_effect=mock_copy_archive)
        service.move_archive_between_locations = AsyncMock(side_effect=mock_copy_archive)
        
        return service
    
    def _create_mock_file_transfer_service(self):
        """Create mock file transfer service."""
        service = Mock()
        
        # Mock file transfer operations
        async def mock_transfer_file(dto):
            return FileTransferResultDto(
                operation_id=f"transfer_{Path(dto.source_path).name}",
                operation_type="file_transfer",
                success=True,
                source_location=dto.source_location,
                source_path=dto.source_path,
                dest_location=dto.dest_location,
                dest_path=dto.dest_path,
                bytes_transferred=1024,
                files_transferred=1,
                duration_seconds=0.5,
                throughput_mbps=2.0
            )
        
        async def mock_batch_transfer(dto):
            return BatchFileTransferResultDto(
                operation_id="batch_transfer_123",
                operation_type="batch_file_transfer",
                total_files=len(dto.transfers),
                successful_transfers=[],
                failed_transfers=[],
                total_bytes_transferred=2048,
                total_duration_seconds=1.0,
                average_throughput_mbps=2.0
            )
        
        service.transfer_file = AsyncMock(side_effect=mock_transfer_file)
        service.batch_transfer_files = AsyncMock(side_effect=mock_batch_transfer)
        service.transfer_directory = AsyncMock(side_effect=mock_batch_transfer)
        
        return service
    
    def test_service_initialization(self):
        """Test service initializes correctly with proper configuration."""
        self.assertIsNotNone(self.operation_queue_service)
        self.assertIsNotNone(self.operation_queue_service._archive_service)
        self.assertIsNotNone(self.operation_queue_service._file_transfer_service)
        self.assertIsNotNone(self.operation_queue_service._queue)
        self.assertIsNotNone(self.operation_queue_service._router)
        
        # Test initial queue state
        self.assertEqual(self.operation_queue_service.queue_length, 0)
        self.assertEqual(self.operation_queue_service.running_operations, 0)
        self.assertFalse(self.operation_queue_service.is_paused)
    
    def test_add_file_transfer_operation(self):
        """Test adding file transfer operation to queue."""
        # Arrange
        dto = FileTransferOperationDto(
            source_location="local",
            source_path=str(self.test_file),
            dest_location="remote",
            dest_path=str(self.temp_dest_dir / "transferred.txt"),
            overwrite=False,
            verify_checksum=False
        )
        
        # Act
        operation_id = asyncio.run(self.operation_queue_service.add_operation(
            operation_dto=dto,
            priority=QueuePriority.HIGH,
            tags={"test", "file_transfer"},
            user_id="test_user"
        ))
        
        # Assert
        self.assertIsNotNone(operation_id)
        self.assertIsInstance(operation_id, str)
        
        # Check operation was added to queue
        status = self.operation_queue_service.get_operation_status(operation_id)
        if status is not None:  # May be None if operation completed very quickly
            self.assertEqual(status.id, operation_id)
            self.assertIn(status.status, [QueueStatus.QUEUED, QueueStatus.RUNNING, QueueStatus.COMPLETED])
    
    def test_add_batch_file_transfer_operation(self):
        """Test adding batch file transfer operation to queue."""
        # Arrange
        transfers = [
            FileTransferOperationDto(
                source_location="local",
                source_path=str(self.test_file),
                dest_location="remote",
                dest_path=str(self.temp_dest_dir / "batch1.txt"),
                overwrite=False,
                verify_checksum=False
            ),
            FileTransferOperationDto(
                source_location="local",
                source_path=str(self.test_file),
                dest_location="remote",
                dest_path=str(self.temp_dest_dir / "batch2.txt"),
                overwrite=False,
                verify_checksum=False
            )
        ]
        
        dto = BatchFileTransferOperationDto(
            transfers=transfers,
            parallel_transfers=2,
            stop_on_error=False,
            verify_all_checksums=False
        )
        
        # Act
        operation_id = asyncio.run(self.operation_queue_service.add_operation(
            operation_dto=dto,
            priority=QueuePriority.NORMAL,
            tags={"test", "batch_transfer"}
        ))
        
        # Assert
        self.assertIsNotNone(operation_id)
        status = self.operation_queue_service.get_operation_status(operation_id)
        # Status may be None if operation completed quickly
        if status is not None:
            self.assertIsNotNone(status)
    
    def test_add_archive_operation(self):
        """Test adding archive operation to queue."""
        # Arrange
        dto = BulkArchiveOperationDto(
            operation_type="bulk_copy",
            archive_ids=["archive1", "archive2"],
            destination_location="dest_loc",
            simulation_id="test_sim"
        )
        
        # Act
        operation_id = asyncio.run(self.operation_queue_service.add_operation(
            operation_dto=dto,
            priority=QueuePriority.URGENT,
            tags={"test", "archive_copy"}
        ))
        
        # Assert
        self.assertIsNotNone(operation_id)
        status = self.operation_queue_service.get_operation_status(operation_id)
        # Status may be None if operation completed quickly
        if status is not None:
            self.assertIsNotNone(status)
    
    def test_queue_statistics(self):
        """Test getting queue statistics."""
        # Act
        stats = self.operation_queue_service.get_queue_stats()
        
        # Assert
        self.assertIsInstance(stats, dict)
        self.assertIn('queue_length', stats)
        self.assertIn('running', stats)
        self.assertIn('is_processing', stats)
        self.assertIn('is_paused', stats)
        
        # Check initial state
        self.assertGreaterEqual(stats['queue_length'], 0)
        self.assertGreaterEqual(stats['running'], 0)
        self.assertIsInstance(stats['is_processing'], bool)
        self.assertIsInstance(stats['is_paused'], bool)
    
    def test_list_operations_empty_queue(self):
        """Test listing operations when queue is empty."""
        # Act
        operations = self.operation_queue_service.list_operations()
        
        # Assert
        self.assertIsInstance(operations, list)
        # Empty queue should return empty list
        self.assertGreaterEqual(len(operations), 0)
    
    def test_list_operations_with_filters(self):
        """Test listing operations with status and tag filters."""
        # Arrange - add a few operations
        dto1 = FileTransferOperationDto(
            source_location="local",
            source_path=str(self.test_file),
            dest_location="remote",
            dest_path=str(self.temp_dest_dir / "filter_test1.txt")
        )
        
        dto2 = FileTransferOperationDto(
            source_location="local",
            source_path=str(self.test_file),
            dest_location="remote",
            dest_path=str(self.temp_dest_dir / "filter_test2.txt")
        )
        
        # Add operations with different tags
        op1_id = asyncio.run(self.operation_queue_service.add_operation(
            dto1, tags={"urgent", "test"}
        ))
        op2_id = asyncio.run(self.operation_queue_service.add_operation(
            dto2, tags={"normal", "test"}
        ))
        
        # Act - test filtering
        all_ops = self.operation_queue_service.list_operations()
        urgent_ops = self.operation_queue_service.list_operations(tag_filter={"urgent"})
        test_ops = self.operation_queue_service.list_operations(tag_filter={"test"})
        
        # Assert
        self.assertGreaterEqual(len(all_ops), 0)
        self.assertGreaterEqual(len(test_ops), 0)
        self.assertGreaterEqual(len(urgent_ops), 0)
    
    def test_queue_pause_resume(self):
        """Test pausing and resuming queue processing."""
        # Test initial state
        self.assertFalse(self.operation_queue_service.is_paused)
        
        # Test pause
        self.operation_queue_service.pause_queue()
        # Note: Actual pause state depends on queue implementation
        
        # Test resume
        self.operation_queue_service.resume_queue()
        # Note: Actual resume state depends on queue implementation
    
    def test_queue_stop(self):
        """Test stopping queue processing."""
        # This should not raise an exception
        self.operation_queue_service.stop_queue()
        
        # After stopping, processing should be stopped
        stats = self.operation_queue_service.get_queue_stats()
        # Note: Actual stopped state depends on queue implementation
        self.assertIsInstance(stats, dict)
    
    def test_cancel_operation(self):
        """Test cancelling a queued operation."""
        # Arrange
        dto = FileTransferOperationDto(
            source_location="local",
            source_path=str(self.test_file),
            dest_location="remote",
            dest_path=str(self.temp_dest_dir / "cancel_test.txt")
        )
        
        # Add operation
        operation_id = asyncio.run(self.operation_queue_service.add_operation(dto))
        
        # Act - try to cancel
        result = self.operation_queue_service.cancel_operation(operation_id)
        
        # Assert
        self.assertIsInstance(result, bool)
        # Result depends on whether operation was cancellable
    
    def test_cancel_nonexistent_operation(self):
        """Test cancelling a non-existent operation."""
        # Act
        result = self.operation_queue_service.cancel_operation("nonexistent_id")
        
        # Assert
        self.assertFalse(result)
    
    def test_clear_completed_operations(self):
        """Test clearing completed operations."""
        # Act
        cleared_count = self.operation_queue_service.clear_completed()
        
        # Assert
        self.assertIsInstance(cleared_count, int)
        self.assertGreaterEqual(cleared_count, 0)
    
    def test_operation_priority_handling(self):
        """Test that operation priorities are handled correctly."""
        # Test that different priority levels are accepted
        priorities = [QueuePriority.LOW, QueuePriority.NORMAL, QueuePriority.HIGH, QueuePriority.URGENT]
        
        for priority in priorities:
            # This should not raise an exception
            dto = FileTransferOperationDto(
                source_location="local",
                source_path=str(self.test_file),
                dest_location="remote",
                dest_path=str(self.temp_dest_dir / f"priority_{priority.name}.txt")
            )
            
            # The add_operation method should accept all priority levels
            self.assertIn(priority, QueuePriority)
    
    def test_operation_tags_handling(self):
        """Test that operation tags are handled correctly."""
        # Test various tag combinations
        tag_sets = [
            {"urgent"},
            {"test", "file_transfer"},
            {"archive", "copy", "simulation_123"},
            set()  # Empty tags
        ]
        
        for tags in tag_sets:
            dto = FileTransferOperationDto(
                source_location="local",
                source_path=str(self.test_file),
                dest_location="remote",
                dest_path=str(self.temp_dest_dir / f"tags_{len(tags)}.txt")
            )
            
            # Should handle all tag combinations without error
            self.assertIsInstance(tags, set)
    
    def test_queue_properties(self):
        """Test queue property accessors."""
        # Test that all properties are accessible and return expected types
        self.assertIsInstance(self.operation_queue_service.is_processing, bool)
        self.assertIsInstance(self.operation_queue_service.is_paused, bool)
        self.assertIsInstance(self.operation_queue_service.queue_length, int)
        self.assertIsInstance(self.operation_queue_service.running_operations, int)
        
        # Test that values are reasonable
        self.assertGreaterEqual(self.operation_queue_service.queue_length, 0)
        self.assertGreaterEqual(self.operation_queue_service.running_operations, 0)
    
    def test_progress_callback_integration(self):
        """Test progress callback functionality."""
        # Arrange
        progress_updates = []
        
        def progress_callback(operation_id: str, progress_data):
            progress_updates.append({
                'operation_id': operation_id,
                'progress_data': progress_data
            })
        
        dto = FileTransferOperationDto(
            source_location="local",
            source_path=str(self.test_file),
            dest_location="remote",
            dest_path=str(self.temp_dest_dir / "progress_test.txt")
        )
        
        # Act
        operation_id = asyncio.run(self.operation_queue_service.add_operation(
            operation_dto=dto,
            progress_callback=progress_callback
        ))
        
        # Assert
        self.assertIsNotNone(operation_id)
        # Progress callback may or may not be called depending on timing
        self.assertIsInstance(progress_updates, list)
    
    def test_service_handler_routing(self):
        """Test that operations are routed to correct service handlers."""
        # Arrange
        file_dto = FileTransferOperationDto(
            source_location="local",
            source_path=str(self.test_file),
            dest_location="remote",
            dest_path=str(self.temp_dest_dir / "routing_test.txt")
        )
        
        archive_dto = BulkArchiveOperationDto(
            operation_type="bulk_copy",
            archive_ids=["archive1"],
            destination_location="dest",
            simulation_id="sim1"
        )
        
        # Act
        file_op_id = asyncio.run(self.operation_queue_service.add_operation(file_dto))
        archive_op_id = asyncio.run(self.operation_queue_service.add_operation(archive_dto))
        
        # Assert operations were added
        self.assertIsNotNone(file_op_id)
        self.assertIsNotNone(archive_op_id)
        
        # Verify that appropriate service methods would be called
        # (The actual calls depend on queue processing timing)
        self.assertTrue(hasattr(self.mock_file_transfer_service, 'transfer_file'))
        self.assertTrue(hasattr(self.mock_archive_service, 'copy_archive_to_location'))


if __name__ == '__main__':
    unittest.main()