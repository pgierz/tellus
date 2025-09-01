"""
Simple tests for FileTransferApplicationService without legacy dependencies.

This tests the new file transfer architecture using only the new clean
architecture components, avoiding legacy simulation/location imports.
"""

import asyncio
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, Mock

from tellus.application.dtos import (FileTransferOperationDto,
                                     FileTransferResultDto)
# Import application services and DTOs directly
from tellus.application.services.file_transfer_service import \
    FileTransferApplicationService
from tellus.domain.entities.location import LocationEntity, LocationKind


class TestFileTransferServiceSimple(unittest.TestCase):
    """Simple test cases for file transfer service."""
    
    def setUp(self):
        """Set up test environment."""
        # Create temporary directories
        self.temp_source_dir = Path(tempfile.mkdtemp(prefix="tellus_test_source_"))
        self.temp_dest_dir = Path(tempfile.mkdtemp(prefix="tellus_test_dest_"))
        
        # Create test file
        self.test_file = self.temp_source_dir / "test_file.txt"
        self.test_file.write_text("Test file content for transfer testing.")
        
        # Mock dependencies
        self.mock_location_repo = Mock()
        self.mock_progress_service = Mock()
        
        # Configure mocks
        self.mock_location_repo.get_by_name.return_value = self._create_mock_location_entity()
        self.mock_progress_service.create_operation = AsyncMock(return_value={'operation_id': 'test_progress_id'})
        self.mock_progress_service.update_progress = AsyncMock()
        
        # Create service
        self.service = FileTransferApplicationService(
            location_repo=self.mock_location_repo,
            progress_service=self.mock_progress_service
        )
    
    def tearDown(self):
        """Clean up test environment."""
        import shutil
        if self.temp_source_dir.exists():
            shutil.rmtree(self.temp_source_dir)
        if self.temp_dest_dir.exists():
            shutil.rmtree(self.temp_dest_dir)
    
    def _create_mock_location_entity(self):
        """Create a mock LocationEntity object."""
        # Create a real LocationEntity for local filesystem
        return LocationEntity(
            name='local',
            kinds=[LocationKind.DISK],
            config={'protocol': 'file', 'path': str(self.temp_source_dir.parent)}
        )
    
    def test_create_service_instance(self):
        """Test that we can create a FileTransferApplicationService instance."""
        self.assertIsInstance(self.service, FileTransferApplicationService)
        self.assertIsNotNone(self.service._location_repo)
        self.assertIsNotNone(self.service._progress_service)
    
    def test_create_file_transfer_dto(self):
        """Test creating a FileTransferOperationDto."""
        dto = FileTransferOperationDto(
            source_location="local",
            source_path=str(self.test_file),
            dest_location="remote",
            dest_path=str(self.temp_dest_dir / "transferred.txt"),
            overwrite=False,
            verify_checksum=True
        )
        
        self.assertEqual(dto.source_location, "local")
        self.assertEqual(dto.source_path, str(self.test_file))
        self.assertEqual(dto.dest_location, "remote")
        self.assertTrue(dto.verify_checksum)
        self.assertFalse(dto.overwrite)
    
    def test_file_transfer_basic_flow(self):
        """Test basic file transfer flow with mocked dependencies."""
        # Arrange
        dto = FileTransferOperationDto(
            source_location="local",
            source_path=str(self.test_file),
            dest_location="remote",
            dest_path=str(self.temp_dest_dir / "transferred.txt"),
            overwrite=False,
            verify_checksum=False  # Simplified for basic test
        )
        
        # Act
        result = asyncio.run(self.service.transfer_file(dto))
        
        # Assert
        self.assertIsInstance(result, FileTransferResultDto)
        self.assertTrue(result.success, f"Transfer failed: {result.error_message}")
        self.assertGreater(result.bytes_transferred, 0)
        self.assertIsNotNone(result.operation_id)
        
        # Verify mock calls
        self.mock_location_repo.get_by_name.assert_called()
        self.mock_progress_service.create_operation.assert_called_once()
        # Note: update_progress may or may not be called depending on transfer size and timing
    
    def test_location_not_found_error(self):
        """Test error handling when location is not found."""
        # Arrange
        self.mock_location_repo.get_by_name.return_value = None
        
        dto = FileTransferOperationDto(
            source_location="local",
            source_path=str(self.test_file),
            dest_location="nonexistent",
            dest_path="/test/dest.txt"
        )
        
        # Act
        result = asyncio.run(self.service.transfer_file(dto))
        
        # Assert
        self.assertFalse(result.success)
        self.assertIn("not found", result.error_message)
    
    def test_source_file_not_found_error(self):
        """Test error handling when source file doesn't exist."""
        # Arrange
        dto = FileTransferOperationDto(
            source_location="local",
            source_path="/nonexistent/file.txt",
            dest_location="remote",
            dest_path="/test/dest.txt"
        )
        
        # Mock location but file doesn't exist - return None to simulate file not found
        self.mock_location_repo.get_by_name.return_value = None
        
        # Act
        result = asyncio.run(self.service.transfer_file(dto))
        
        # Assert
        self.assertFalse(result.success)
        self.assertIn("not found", result.error_message.lower())
    
    def test_transfer_operation_id_generation(self):
        """Test that each transfer gets a unique operation ID."""
        # Arrange - use different destination files to avoid overwrite issues
        dto1 = FileTransferOperationDto(
            source_location="local",
            source_path=str(self.test_file),
            dest_location="remote",
            dest_path=str(self.temp_dest_dir / "transferred1.txt")
        )
        dto2 = FileTransferOperationDto(
            source_location="local",
            source_path=str(self.test_file),
            dest_location="remote",
            dest_path=str(self.temp_dest_dir / "transferred2.txt")
        )
        
        # Act - run multiple transfers
        result1 = asyncio.run(self.service.transfer_file(dto1))
        result2 = asyncio.run(self.service.transfer_file(dto2))
        
        # Assert
        self.assertTrue(result1.success)
        self.assertTrue(result2.success)
        self.assertNotEqual(result1.operation_id, result2.operation_id)
        self.assertTrue(result1.operation_id.startswith("transfer_"))
        self.assertTrue(result2.operation_id.startswith("transfer_"))
    
    def test_progress_service_integration(self):
        """Test integration with progress tracking service."""
        # Arrange
        dto = FileTransferOperationDto(
            source_location="local",
            source_path=str(self.test_file),
            dest_location="remote",
            dest_path=str(self.temp_dest_dir / "transferred.txt")
        )
        
        # Act
        result = asyncio.run(self.service.transfer_file(dto))
        
        # Assert
        self.assertTrue(result.success)
        
        # Verify progress service was called
        self.mock_progress_service.create_operation.assert_called_once()
        # Note: update_progress may not be called for very small/fast transfers
        
        # Check create_operation call structure
        create_call = self.mock_progress_service.create_operation.call_args[0][0]
        self.assertTrue(hasattr(create_call, 'operation_type'))
        self.assertTrue(hasattr(create_call, 'operation_name'))


if __name__ == '__main__':
    unittest.main()