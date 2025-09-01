"""
Unit tests for ArchiveApplicationService.

Tests the application service layer for archive management,
including CRUD operations, file operations, caching, and progress tracking.
"""

import asyncio
from typing import Any, Dict, List
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest

from tellus.application.dtos import (ArchiveCopyOperationDto, ArchiveDto,
                                     ArchiveExtractionDto, ArchiveFileListDto,
                                     ArchiveListDto, ArchiveMoveOperationDto,
                                     ArchiveOperationDto,
                                     ArchiveOperationResultDto,
                                     BulkArchiveOperationDto,
                                     BulkOperationResultDto,
                                     CacheConfigurationDto,
                                     CacheOperationResult, CacheStatusDto,
                                     CreateArchiveDto, FileInventoryDto,
                                     FileMetadataDto, FilterOptions,
                                     PaginationInfo, SimulationFileDto,
                                     UpdateArchiveDto)
from tellus.application.exceptions import (ArchiveOperationError,
                                           CacheOperationError,
                                           DataIntegrityError,
                                           EntityAlreadyExistsError,
                                           EntityNotFoundError,
                                           ValidationError)
from tellus.application.services.archive_service import \
    ArchiveApplicationService
from tellus.domain.entities.archive import (ArchiveId, ArchiveMetadata,
                                            ArchiveType, CacheConfiguration)
from tellus.domain.entities.location import LocationEntity, LocationKind
from tellus.domain.entities.simulation_file import (FileContentType,
                                                    FileImportance,
                                                    SimulationFile)
from tellus.domain.repositories.exceptions import (LocationNotFoundError,
                                                   RepositoryError)


@pytest.fixture
def mock_location_repo():
    """Mock location repository."""
    return Mock()


@pytest.fixture
def mock_archive_repo():
    """Mock archive repository."""
    return Mock()


@pytest.fixture
def mock_progress_service():
    """Mock progress tracking service."""
    mock = Mock()
    # Make async methods return coroutines
    mock.create_operation = AsyncMock()
    mock.update_progress = AsyncMock()
    mock.complete_operation = AsyncMock()
    return mock


@pytest.fixture
def cache_config():
    """Sample cache configuration."""
    return CacheConfigurationDto(
        cache_directory="/tmp/test-cache",
        archive_size_limit=10 * 1024**3,  # 10 GB
        file_size_limit=5 * 1024**3,  # 5 GB
        cleanup_policy="lru"
    )


@pytest.fixture
def service(mock_location_repo, mock_archive_repo, cache_config, mock_progress_service):
    """Create service instance with mocked dependencies."""
    return ArchiveApplicationService(
        location_repository=mock_location_repo,
        archive_repository=mock_archive_repo,
        cache_config=cache_config,
        progress_tracking_service=mock_progress_service
    )


@pytest.fixture
def sample_location_entity():
    """Create a sample location entity for testing."""
    return LocationEntity(
        name="test-location",
        kinds=[LocationKind.DISK],
        config={"protocol": "file", "path": "/test/archive"},
        optional=False
    )


@pytest.fixture
def sample_archive_metadata():
    """Create a sample archive metadata for testing."""
    return ArchiveMetadata(
        archive_id=ArchiveId("test-archive"),
        location="test-location",
        archive_type=ArchiveType.COMPRESSED,
        simulation_id="test-sim",
        version="1.0",
        description="Test archive"
    )


class TestArchiveApplicationService:
    """Test suite for ArchiveApplicationService."""


class TestCreateArchiveMetadata:
    """Test archive metadata creation operations."""
    
    def test_create_archive_metadata_success(self, service, mock_location_repo, mock_archive_repo, sample_location_entity):
        """Test successful archive metadata creation."""
        # Arrange
        dto = CreateArchiveDto(
            archive_id="test-archive",
            location_name="test-location",
            archive_type="compressed",
            simulation_id="test-sim",
            version="1.0",
            description="Test archive",
            tags={"climate", "output"}
        )
        
        mock_location_repo.get_by_name.return_value = sample_location_entity
        mock_archive_repo.save.return_value = None
        
        # Act
        result = service.create_archive_metadata(dto)
        
        # Assert
        assert isinstance(result, ArchiveDto)
        assert result.archive_id == "test-archive"
        assert result.location == "test-location"
        assert result.archive_type == "compressed"
        assert result.simulation_id == "test-sim"
        assert result.version == "1.0"
        assert result.description == "Test archive"
        
        mock_location_repo.get_by_name.assert_called_once_with("test-location")
        mock_archive_repo.save.assert_called_once()
    
    def test_create_archive_metadata_location_not_found(self, service, mock_location_repo):
        """Test creating archive metadata with non-existent location."""
        # Arrange
        dto = CreateArchiveDto(
            archive_id="test-archive",
            location_name="nonexistent-location"
        )
        mock_location_repo.get_by_name.return_value = None
        
        # Act & Assert
        with pytest.raises(EntityNotFoundError) as exc_info:
            service.create_archive_metadata(dto)
        
        assert "nonexistent-location" in str(exc_info.value)
        mock_archive_repo.save.assert_not_called()
    
    def test_create_archive_metadata_invalid_archive_type(self, service, mock_location_repo, sample_location_entity):
        """Test creating archive metadata with invalid archive type."""
        # Arrange
        dto = CreateArchiveDto(
            archive_id="test-archive",
            location_name="test-location",
            archive_type="invalid_type"
        )
        mock_location_repo.get_by_name.return_value = sample_location_entity
        
        # Act & Assert
        with pytest.raises(ValidationError) as exc_info:
            service.create_archive_metadata(dto)
        
        assert "Invalid archive type" in str(exc_info.value)
    
    def test_create_archive_metadata_repository_error(self, service, mock_location_repo, mock_archive_repo, sample_location_entity):
        """Test archive metadata creation with repository error."""
        # Arrange
        dto = CreateArchiveDto(
            archive_id="test-archive",
            location_name="test-location"
        )
        mock_location_repo.get_by_name.return_value = sample_location_entity
        mock_archive_repo.save.side_effect = RepositoryError("Database error")
        
        # Act & Assert
        with pytest.raises(RepositoryError):
            service.create_archive_metadata(dto)


class TestGetArchiveMetadata:
    """Test archive metadata retrieval operations."""
    
    def test_get_archive_metadata_success(self, service, mock_archive_repo, sample_archive_metadata):
        """Test successful archive metadata retrieval."""
        # Arrange
        mock_archive_repo.get_by_id.return_value = sample_archive_metadata
        
        # Act
        result = service.get_archive_metadata("test-archive")
        
        # Assert
        assert isinstance(result, ArchiveDto)
        assert result.archive_id == "test-archive"
        assert result.location == "test-location"
        assert result.simulation_id == "test-sim"
        mock_archive_repo.get_by_id.assert_called_once_with(ArchiveId("test-archive"))
    
    def test_get_archive_metadata_not_found(self, service, mock_archive_repo):
        """Test retrieving non-existent archive metadata."""
        # Arrange
        mock_archive_repo.get_by_id.return_value = None
        
        # Act & Assert
        with pytest.raises(EntityNotFoundError) as exc_info:
            service.get_archive_metadata("nonexistent")
        
        assert "nonexistent" in str(exc_info.value)


class TestListArchives:
    """Test archive listing operations."""
    
    def test_list_archives_success(self, service, mock_archive_repo, sample_archive_metadata):
        """Test successful archive listing."""
        # Arrange
        archives = [sample_archive_metadata]
        mock_archive_repo.list_all.return_value = archives
        
        # Act
        result = service.list_archives()
        
        # Assert
        assert isinstance(result, ArchiveListDto)
        assert len(result.archives) == 1
        assert result.archives[0].archive_id == "test-archive"
        assert isinstance(result.pagination, PaginationInfo)
        assert result.pagination.total_count == 1
    
    def test_list_archives_with_pagination(self, service, mock_archive_repo):
        """Test archive listing with pagination."""
        # Arrange
        archives = [
            ArchiveMetadata(
                archive_id=ArchiveId(f"archive-{i}"),
                location="test-location",
                archive_type=ArchiveType.COMPRESSED
            )
            for i in range(10)
        ]
        mock_archive_repo.list_all.return_value = archives
        
        # Act
        result = service.list_archives(page=2, page_size=3)
        
        # Assert
        assert len(result.archives) == 3
        assert result.pagination.page == 2
        assert result.pagination.page_size == 3
        assert result.pagination.total_count == 10
        assert result.pagination.has_next is True
        assert result.pagination.has_previous is True
    
    def test_list_archives_with_filters(self, service, mock_archive_repo):
        """Test archive listing with search filters."""
        # Arrange
        archives = [
            ArchiveMetadata(
                archive_id=ArchiveId("test-archive-1"),
                location="test-location",
                archive_type=ArchiveType.COMPRESSED,
                simulation_id="test-sim"
            ),
            ArchiveMetadata(
                archive_id=ArchiveId("prod-archive-2"),
                location="test-location",
                archive_type=ArchiveType.COMPRESSED,
                simulation_id="prod-sim"
            )
        ]
        mock_archive_repo.list_all.return_value = archives
        
        filters = FilterOptions(search_term="test")
        
        # Act
        result = service.list_archives(filters=filters)
        
        # Assert
        assert len(result.archives) == 1  # Only test-archive-1
        assert "test" in result.archives[0].archive_id


class TestArchiveOperations:
    """Test archive operation methods."""
    
    @pytest.mark.asyncio
    async def test_create_archive_success(self, service, mock_location_repo, mock_archive_repo, sample_location_entity):
        """Test successful archive creation."""
        # Arrange
        dto = CreateArchiveDto(
            archive_id="test-archive",
            location_name="test-location",
            source_path="/test/source"
        )
        
        mock_location_repo.get_by_name.return_value = sample_location_entity
        mock_archive_repo.save.return_value = None
        
        # Mock file operations
        with patch.object(service, '_calculate_source_size', return_value=1000000), \
             patch.object(service, '_count_source_files', return_value=10), \
             patch.object(service, '_create_archive_file_with_progress') as mock_create:
            
            mock_create.return_value = "/test/archive/test-archive.tar.gz"
            
            # Act
            result = await service.create_archive(dto)
        
        # Assert
        assert isinstance(result, ArchiveOperationResultDto)
        assert result.success is True
        assert result.archive_id == "test-archive"
        assert result.files_processed == 10
    
    def test_extract_archive_success(self, service, mock_archive_repo, sample_archive_metadata):
        """Test successful archive extraction."""
        # Arrange
        dto = ArchiveOperationDto(
            archive_id="test-archive",
            operation="extract",
            destination_path="/test/extract"
        )
        
        mock_archive_repo.get_by_id.return_value = sample_archive_metadata
        
        # Mock extraction process
        with patch.object(service, '_extract_tar_files') as mock_extract:
            mock_extract.return_value = [
                SimulationFile(
                    relative_path="file1.nc",
                    size=1000,
                    content_type=FileContentType.OUTPUT
                )
            ]
            
            # Act
            result = service.extract_archive(dto)
        
        # Assert
        assert isinstance(result, str)  # Returns extraction path
        mock_extract.assert_called_once()
    
    def test_compress_archive_success(self, service, mock_archive_repo, sample_archive_metadata):
        """Test successful archive compression."""
        # Arrange
        dto = ArchiveOperationDto(
            archive_id="test-archive",
            operation="compress",
            source_path="/test/source"
        )
        
        mock_archive_repo.get_by_id.return_value = sample_archive_metadata
        
        # Mock compression process
        with patch('tarfile.open') as mock_tarfile:
            mock_tar = MagicMock()
            mock_tarfile.return_value.__enter__.return_value = mock_tar
            
            # Act
            result = service.compress_archive(dto)
        
        # Assert
        assert isinstance(result, str)  # Returns compressed file path


class TestArchiveCopyAndMove:
    """Test archive copy and move operations."""
    
    @pytest.mark.asyncio
    async def test_copy_archive_with_progress_success(self, service, mock_archive_repo, mock_location_repo, sample_archive_metadata, sample_location_entity):
        """Test successful archive copy with progress tracking."""
        # Arrange
        copy_dto = ArchiveCopyOperationDto(
            archive_id="test-archive",
            source_location="source-location",
            destination_location="dest-location",
            simulation_id="test-sim"
        )
        
        mock_archive_repo.get_by_id.return_value = sample_archive_metadata
        mock_location_repo.get_by_name.return_value = sample_location_entity
        
        # Mock the copy operation
        with patch.object(service, '_verify_copy_integrity', return_value=True):
            # Act
            result = await service.copy_archive_with_progress(copy_dto)
        
        # Assert
        assert isinstance(result, ArchiveOperationResultDto)
        assert result.success is True
        assert result.archive_id == "test-archive"
    
    @pytest.mark.asyncio
    async def test_move_archive_with_progress_success(self, service, mock_archive_repo, mock_location_repo, sample_archive_metadata, sample_location_entity):
        """Test successful archive move with progress tracking."""
        # Arrange
        move_dto = ArchiveMoveOperationDto(
            archive_id="test-archive",
            source_location="source-location",
            destination_location="dest-location",
            simulation_id="test-sim"
        )
        
        mock_archive_repo.get_by_id.return_value = sample_archive_metadata
        mock_location_repo.get_by_name.return_value = sample_location_entity
        
        # Mock the move operation
        with patch.object(service, '_verify_copy_integrity', return_value=True):
            # Act
            result = await service.move_archive_with_progress(move_dto)
        
        # Assert
        assert isinstance(result, ArchiveOperationResultDto)
        assert result.success is True
        assert result.archive_id == "test-archive"


class TestArchiveExtraction:
    """Test archive extraction operations."""
    
    def test_extract_archive_to_location_success(self, service, mock_archive_repo, mock_location_repo, sample_archive_metadata, sample_location_entity):
        """Test successful archive extraction to location."""
        # Arrange
        extract_dto = ArchiveExtractionDto(
            archive_id="test-archive",
            destination_location="dest-location",
            simulation_id="test-sim",
            content_type_filter="output"
        )
        
        mock_archive_repo.get_by_id.return_value = sample_archive_metadata
        mock_location_repo.get_by_name.return_value = sample_location_entity
        
        # Mock file extraction
        mock_files = [
            SimulationFile(
                relative_path="output.nc",
                size=1000,
                content_type=FileContentType.OUTPUT
            )
        ]
        
        with patch.object(service, '_extract_file_list_from_archive', return_value=mock_files), \
             patch.object(service, '_extract_tar_files', return_value=mock_files):
            
            # Act
            result = service.extract_archive_to_location(extract_dto)
        
        # Assert
        assert isinstance(result, ArchiveOperationResultDto)
        assert result.success is True
        assert result.files_processed == 1
    
    def test_extract_archive_to_location_with_pattern_filter(self, service, mock_archive_repo, mock_location_repo, sample_archive_metadata, sample_location_entity):
        """Test archive extraction with pattern filtering."""
        # Arrange
        extract_dto = ArchiveExtractionDto(
            archive_id="test-archive",
            destination_location="dest-location",
            pattern_filter="*.nc"
        )
        
        mock_archive_repo.get_by_id.return_value = sample_archive_metadata
        mock_location_repo.get_by_name.return_value = sample_location_entity
        
        # Mock file extraction with multiple files
        mock_files = [
            SimulationFile(relative_path="data.nc", size=1000, content_type=FileContentType.OUTPUT),
            SimulationFile(relative_path="config.txt", size=100, content_type=FileContentType.INPUT),
            SimulationFile(relative_path="output.nc", size=2000, content_type=FileContentType.OUTPUT)
        ]
        
        with patch.object(service, '_extract_file_list_from_archive', return_value=mock_files), \
             patch.object(service, '_extract_tar_files', return_value=mock_files[:1] + mock_files[2:]):  # Only .nc files
            
            # Act
            result = service.extract_archive_to_location(extract_dto)
        
        # Assert
        assert result.files_processed == 2  # Only .nc files


class TestCacheOperations:
    """Test cache management operations."""
    
    def test_add_to_cache_success(self, service):
        """Test successfully adding an archive to cache."""
        # Arrange
        archive_id = "test-archive"
        file_path = "/cache/test-archive.tar.gz"
        
        with patch('os.path.exists', return_value=True), \
             patch('os.path.getsize', return_value=1000000), \
             patch('shutil.copy2'):
            
            # Act
            result = service.add_to_cache(archive_id, file_path)
        
        # Assert
        assert result is True
    
    def test_get_cache_status_success(self, service):
        """Test getting cache status."""
        # Mock cache directory and files
        with patch.object(service, '_get_cache_size', return_value=5000000000):  # 5GB
            # Act
            result = service.get_cache_status()
        
        # Assert
        assert isinstance(result, CacheStatusDto)
        assert result.archive_cache_size_gb == 5.0
        assert result.archive_limit_gb == 10.0
    
    def test_cleanup_cache_success(self, service):
        """Test cache cleanup operation."""
        # Mock cleanup process
        with patch.object(service, '_cleanup_lru', return_value=(3, 2000000000)):  # 3 files, 2GB cleaned
            # Act
            result = service.cleanup_cache()
        
        # Assert
        assert isinstance(result, CacheOperationResult)
        assert result.success is True
        assert result.files_removed == 3
        assert result.space_freed_bytes == 2000000000


class TestFileOperations:
    """Test file-related operations."""
    
    def test_list_archive_files_success(self, service, mock_archive_repo, sample_archive_metadata):
        """Test listing files in an archive."""
        # Arrange
        mock_archive_repo.get_by_id.return_value = sample_archive_metadata
        
        mock_files = [
            SimulationFile(
                relative_path="data/output.nc",
                size=1000000,
                content_type=FileContentType.OUTPUT,
                importance=FileImportance.CRITICAL
            ),
            SimulationFile(
                relative_path="logs/debug.log",
                size=50000,
                content_type=FileContentType.LOG,
                importance=FileImportance.LOW
            )
        ]
        
        with patch.object(service, '_extract_file_list_from_archive', return_value=mock_files):
            # Act
            result = service.list_archive_files("test-archive")
        
        # Assert
        assert isinstance(result, ArchiveFileListDto)
        assert len(result.files) == 2
        assert result.files[0].relative_path == "data/output.nc"
        assert result.files[1].relative_path == "logs/debug.log"
    
    def test_list_archive_files_with_content_type_filter(self, service, mock_archive_repo, sample_archive_metadata):
        """Test listing archive files with content type filter."""
        # Arrange
        mock_archive_repo.get_by_id.return_value = sample_archive_metadata
        
        mock_files = [
            SimulationFile(relative_path="output.nc", size=1000, content_type=FileContentType.OUTPUT),
            SimulationFile(relative_path="input.nc", size=500, content_type=FileContentType.INPUT),
            SimulationFile(relative_path="debug.log", size=100, content_type=FileContentType.LOG)
        ]
        
        with patch.object(service, '_extract_file_list_from_archive', return_value=mock_files):
            # Act
            result = service.list_archive_files("test-archive", content_type="output")
        
        # Assert
        assert len(result.files) == 1
        assert result.files[0].content_type == "output"
    
    def test_list_archive_files_with_pattern_filter(self, service, mock_archive_repo, sample_archive_metadata):
        """Test listing archive files with pattern filter."""
        # Arrange
        mock_archive_repo.get_by_id.return_value = sample_archive_metadata
        
        mock_files = [
            SimulationFile(relative_path="data.nc", size=1000, content_type=FileContentType.OUTPUT),
            SimulationFile(relative_path="config.txt", size=100, content_type=FileContentType.INPUT),
            SimulationFile(relative_path="results.nc", size=2000, content_type=FileContentType.OUTPUT)
        ]
        
        with patch.object(service, '_extract_file_list_from_archive', return_value=mock_files):
            # Act
            result = service.list_archive_files("test-archive", pattern="*.nc")
        
        # Assert
        assert len(result.files) == 2
        assert all(file.relative_path.endswith('.nc') for file in result.files)


class TestVerificationOperations:
    """Test archive verification operations."""
    
    def test_verify_archive_integrity_success(self, service, mock_archive_repo, sample_archive_metadata):
        """Test successful archive integrity verification."""
        # Arrange
        mock_archive_repo.get_by_id.return_value = sample_archive_metadata
        
        # Mock tarfile verification
        with patch('tarfile.open') as mock_tarfile:
            mock_tar = MagicMock()
            mock_tarfile.return_value.__enter__.return_value = mock_tar
            mock_tar.getmembers.return_value = []  # No corruption
            
            # Act
            result = service.verify_archive_integrity("test-archive")
        
        # Assert
        assert result is True
    
    def test_verify_archive_integrity_failure(self, service, mock_archive_repo, sample_archive_metadata):
        """Test archive integrity verification failure."""
        # Arrange
        mock_archive_repo.get_by_id.return_value = sample_archive_metadata
        
        # Mock tarfile verification with corruption
        with patch('tarfile.open', side_effect=Exception("Corrupted archive")):
            # Act
            result = service.verify_archive_integrity("test-archive")
        
        # Assert
        assert result is False


class TestBulkOperations:
    """Test bulk archive operations."""
    
    @pytest.mark.asyncio
    async def test_execute_bulk_operation_copy_success(self, service, mock_archive_repo, mock_location_repo, sample_archive_metadata, sample_location_entity):
        """Test successful bulk copy operation."""
        # Arrange
        bulk_dto = BulkArchiveOperationDto(
            operation_type="bulk_copy",
            archive_ids=["archive-1", "archive-2"],
            destination_location="dest-location",
            simulation_id="test-sim"
        )
        
        mock_archive_repo.get_by_id.return_value = sample_archive_metadata
        mock_location_repo.get_by_name.return_value = sample_location_entity
        
        # Mock individual copy operations
        with patch.object(service, 'copy_archive_to_location_async') as mock_copy:
            mock_copy.return_value = ArchiveOperationResultDto(
                success=True,
                archive_id="test-archive",
                files_processed=5
            )
            
            # Act
            result = await service.execute_bulk_operation(bulk_dto)
        
        # Assert
        assert isinstance(result, BulkOperationResultDto)
        assert result.success is True
        assert result.total_operations == 2
        assert result.successful_operations == 2
        assert result.failed_operations == 0
    
    @pytest.mark.asyncio
    async def test_execute_bulk_operation_mixed_results(self, service, mock_archive_repo, mock_location_repo, sample_archive_metadata, sample_location_entity):
        """Test bulk operation with mixed success/failure results."""
        # Arrange
        bulk_dto = BulkArchiveOperationDto(
            operation_type="bulk_extract",
            archive_ids=["archive-1", "archive-2", "archive-3"],
            destination_location="dest-location"
        )
        
        mock_archive_repo.get_by_id.return_value = sample_archive_metadata
        mock_location_repo.get_by_name.return_value = sample_location_entity
        
        # Mock mixed results
        with patch.object(service, 'extract_archive_to_location_async') as mock_extract:
            def side_effect(dto):
                if "archive-2" in dto.archive_id:
                    return ArchiveOperationResultDto(
                        success=False,
                        archive_id=dto.archive_id,
                        error_message="Extraction failed"
                    )
                return ArchiveOperationResultDto(
                    success=True,
                    archive_id=dto.archive_id,
                    files_processed=10
                )
            
            mock_extract.side_effect = side_effect
            
            # Act
            result = await service.execute_bulk_operation(bulk_dto)
        
        # Assert
        assert result.total_operations == 3
        assert result.successful_operations == 2
        assert result.failed_operations == 1
        assert len(result.operation_results) == 3


class TestPrivateHelperMethods:
    """Test private helper methods."""
    
    def test_metadata_to_dto_conversion(self, service, sample_archive_metadata):
        """Test archive metadata to DTO conversion."""
        # Act
        result = service._metadata_to_dto(sample_archive_metadata)
        
        # Assert
        assert isinstance(result, ArchiveDto)
        assert result.archive_id == "test-archive"
        assert result.location == "test-location"
        assert result.archive_type == "compressed"
        assert result.simulation_id == "test-sim"
        assert result.version == "1.0"
    
    def test_classify_file_content_type(self, service):
        """Test file content type classification."""
        # Act & Assert
        assert service._classify_file_content_type("output.nc") == FileContentType.OUTPUT
        assert service._classify_file_content_type("input.txt") == FileContentType.INPUT
        assert service._classify_file_content_type("debug.log") == FileContentType.LOG
        assert service._classify_file_content_type("unknown.xyz") == FileContentType.DATA
    
    def test_should_extract_file_content_type_filter(self, service):
        """Test file extraction filtering by content type."""
        # Arrange
        file = SimulationFile(
            relative_path="output.nc",
            size=1000,
            content_type=FileContentType.OUTPUT
        )
        
        # Act & Assert
        assert service._should_extract_file(file, content_type="output") is True
        assert service._should_extract_file(file, content_type="input") is False
    
    def test_should_extract_file_pattern_filter(self, service):
        """Test file extraction filtering by pattern."""
        # Arrange
        file = SimulationFile(
            relative_path="data/results.nc",
            size=1000,
            content_type=FileContentType.OUTPUT
        )
        
        # Act & Assert
        assert service._should_extract_file(file, pattern="*.nc") is True
        assert service._should_extract_file(file, pattern="*.txt") is False
        assert service._should_extract_file(file, pattern="data/*") is True
    
    def test_get_archive_path_success(self, service, sample_archive_metadata, sample_location_entity):
        """Test getting archive file path."""
        # Mock location filesystem
        with patch.object(service, '_location_repo') as mock_repo:
            mock_repo.get_by_name.return_value = sample_location_entity
            
            # Act
            result = service._get_archive_path(sample_archive_metadata)
        
        # Assert
        assert isinstance(result, str)
        assert "test-archive" in result


class TestErrorHandling:
    """Test error handling scenarios."""
    
    def test_repository_error_propagation(self, service, mock_archive_repo):
        """Test that repository errors are properly propagated."""
        # Arrange
        mock_archive_repo.list_all.side_effect = RepositoryError("Database connection failed")
        
        # Act & Assert
        with pytest.raises(RepositoryError) as exc_info:
            service.list_archives()
        
        assert "Database connection failed" in str(exc_info.value)
    
    @pytest.mark.asyncio
    async def test_progress_tracking_failure_graceful(self, service, mock_progress_service):
        """Test that progress tracking failures don't break operations."""
        # Arrange
        mock_progress_service.create_operation.side_effect = Exception("Progress service error")
        
        # Act - Should not raise exception
        operation_id = await service._create_operation_tracker("test", "ARCHIVE_CREATION")
        
        # Assert
        assert operation_id is None  # Failed gracefully
    
    def test_cache_operation_with_disabled_cache(self, service):
        """Test cache operations when cache is disabled."""
        # Arrange - Modify service to have disabled cache by setting size limit to 0
        service._cache_config.archive_size_limit = 0
        
        # Act
        result = service.add_to_cache("test-archive", "/test/path")
        
        # Assert
        assert result is False  # Cache effectively disabled
    
    def test_archive_extraction_not_found(self, service, mock_archive_repo):
        """Test archive extraction with non-existent archive."""
        # Arrange
        extract_dto = ArchiveExtractionDto(
            archive_id="nonexistent",
            destination_location="dest-location"
        )
        mock_archive_repo.get_by_id.return_value = None
        
        # Act & Assert
        with pytest.raises(EntityNotFoundError):
            service.extract_archive_to_location(extract_dto)