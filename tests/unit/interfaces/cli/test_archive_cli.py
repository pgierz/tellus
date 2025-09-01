"""
Unit tests for archive CLI commands.

These tests verify the archive management CLI interface layer,
including command parsing, service integration, error handling,
and output formatting.
"""

import asyncio
import io
import json
import sys
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest
from click.testing import CliRunner
from rich.console import Console

from tellus.application.dtos import (ArchiveCopyOperationDto, ArchiveDto,
                                     ArchiveExtractionDto, ArchiveListDto,
                                     ArchiveOperationResultDto,
                                     CreateArchiveDto, FilterOptions,
                                     PaginationInfo)
from tellus.application.exceptions import (EntityAlreadyExistsError,
                                           EntityNotFoundError,
                                           ValidationError)
from tellus.application.services.archive_service import \
    ArchiveApplicationService
from tellus.domain.entities.archive import ArchiveType
from tellus.interfaces.cli.archive import archive


class TestArchiveListCommand:
    """Test cases for archive list command."""
    
    @pytest.fixture
    def mock_service(self):
        """Mock archive application service."""
        service = Mock(spec=ArchiveApplicationService)
        return service
    
    @pytest.fixture
    def sample_archives(self):
        """Sample archive DTOs for testing."""
        return [
            ArchiveDto(
                archive_id="test-archive-1",
                location="localhost",
                archive_type="COMPRESSED",
                simulation_id="test-sim-1",
                size=1024,
                created_time=1672574400.0,  # 2023-01-01T12:00:00Z in epoch
                description="Test archive 1"
            ),
            ArchiveDto(
                archive_id="test-archive-2",
                location="hpc-cluster",
                archive_type="UNCOMPRESSED",
                simulation_id="test-sim-2",
                size=2048,
                created_time=1672660200.0,  # 2023-01-02T14:30:00Z in epoch
                description="Test archive 2"
            ),
            ArchiveDto(
                archive_id="orphan-archive",
                location="cloud-storage",
                archive_type="COMPRESSED",
                simulation_id=None,
                size=512,
                created_time=1672747200.0,  # 2023-01-03T10:00:00Z in epoch
                description="Orphaned archive"
            )
        ]
    
    @patch('tellus.interfaces.cli.archive._get_archive_service')
    def test_list_all_archives_success(self, mock_get_service, mock_service, sample_archives):
        """Test successful listing of all archives."""
        mock_get_service.return_value = mock_service
        list_result = ArchiveListDto(
            archives=sample_archives,
            pagination=PaginationInfo(),
            filters_applied=FilterOptions()
        )
        mock_service.list_archives.return_value = list_result
        
        runner = CliRunner()
        result = runner.invoke(archive, ['list'])
        
        assert result.exit_code == 0
        assert "test-archive-1" in result.output
        assert "test-archive-2" in result.output
        assert "orphan-archive" in result.output
        assert "localhost" in result.output
        assert "hpc-cluster" in result.output
        assert "COMPRESSED" in result.output
        assert "test-sim-1" in result.output
        mock_service.list_archives.assert_called_once()
    
    @patch('tellus.interfaces.cli.archive._get_archive_service')
    def test_list_archives_empty(self, mock_get_service, mock_service):
        """Test listing when no archives exist."""
        mock_get_service.return_value = mock_service
        empty_result = ArchiveListDto(
            archives=[],
            pagination=PaginationInfo(),
            filters_applied=FilterOptions()
        )
        mock_service.list_archives.return_value = empty_result
        
        runner = CliRunner()
        result = runner.invoke(archive, ['list'])
        
        assert result.exit_code == 0
        assert "No archives found" in result.output
        mock_service.list_archives.assert_called_once()
    
    @patch('tellus.interfaces.cli.archive._get_archive_service')
    def test_list_archives_with_orphaned(self, mock_get_service, mock_service, sample_archives):
        """Test listing archives including orphaned ones (no simulation)."""
        mock_get_service.return_value = mock_service
        list_result = ArchiveListDto(
            archives=sample_archives,
            pagination=PaginationInfo(),
            filters_applied=FilterOptions()
        )
        mock_service.list_archives.return_value = list_result
        
        runner = CliRunner()
        result = runner.invoke(archive, ['list'])
        
        assert result.exit_code == 0
        # Should show "-" for orphaned archive simulation
        assert "orphan-archive" in result.output
        assert "-" in result.output  # For the simulation column
    
    @patch('tellus.interfaces.cli.archive._get_archive_service')
    def test_list_archives_service_error(self, mock_get_service, mock_service):
        """Test handling service errors during listing."""
        mock_get_service.return_value = mock_service
        mock_service.list_archives.side_effect = Exception("Service error")
        
        runner = CliRunner()
        result = runner.invoke(archive, ['list'])
        
        assert result.exit_code == 0  # CLI handles errors gracefully
        assert "Error:" in result.output


class TestArchiveShowCommand:
    """Test cases for archive show command."""
    
    @pytest.fixture
    def mock_service(self):
        """Mock archive application service."""
        service = Mock(spec=ArchiveApplicationService)
        return service
    
    @pytest.fixture
    def sample_archive(self):
        """Sample archive DTO for testing."""
        return ArchiveDto(
            archive_id="test-archive",
            location="test-location",
            archive_type="COMPRESSED",
            simulation_id="test-simulation",
            created_at="2023-01-01T12:00:00Z",
            size_bytes=2048,
            file_count=15,
            metadata={"description": "Test archive", "version": "1.0"}
        )
    
    @patch('tellus.interfaces.cli.archive._get_archive_service')
    def test_show_archive_success(self, mock_get_service, mock_service, sample_archive):
        """Test successful display of archive details."""
        mock_get_service.return_value = mock_service
        mock_service.get_archive.return_value = sample_archive
        
        runner = CliRunner()
        result = runner.invoke(archive, ['show', 'test-archive'])
        
        assert result.exit_code == 0
        assert "test-archive" in result.output
        assert "test-location" in result.output
        assert "COMPRESSED" in result.output
        assert "test-simulation" in result.output
        mock_service.get_archive.assert_called_once_with("test-archive")
    
    @patch('tellus.interfaces.cli.archive._get_archive_service')
    def test_show_archive_orphaned(self, mock_get_service, mock_service):
        """Test showing orphaned archive (no simulation)."""
        mock_get_service.return_value = mock_service
        orphaned_archive = ArchiveDto(
            archive_id="orphaned-archive",
            location="test-location",
            archive_type="COMPRESSED",
            simulation_id=None,
            created_at="2023-01-01T12:00:00Z",
            size_bytes=1024,
            file_count=8
        )
        mock_service.get_archive.return_value = orphaned_archive
        
        runner = CliRunner()
        result = runner.invoke(archive, ['show', 'orphaned-archive'])
        
        assert result.exit_code == 0
        assert "orphaned-archive" in result.output
        assert "-" in result.output  # For the simulation field
    
    @patch('tellus.interfaces.cli.archive._get_archive_service')
    def test_show_archive_not_found(self, mock_get_service, mock_service):
        """Test showing non-existent archive."""
        mock_get_service.return_value = mock_service
        mock_service.get_archive.side_effect = EntityNotFoundError("Archive", "nonexistent")
        
        runner = CliRunner()
        result = runner.invoke(archive, ['show', 'nonexistent'])
        
        assert result.exit_code == 0  # CLI handles errors gracefully
        assert "Error:" in result.output
        mock_service.get_archive.assert_called_once_with("nonexistent")


class TestArchiveCreateCommand:
    """Test cases for archive create command."""
    
    @pytest.fixture
    def mock_service(self):
        """Mock archive application service.""" 
        service = Mock(spec=ArchiveApplicationService)
        return service
    
    @pytest.fixture
    def create_result(self):
        """Sample archive creation result."""
        return ArchiveOperationResultDto(
            success=True,
            archive_id="new-archive",
            operation_id="op-12345",
            destination_path="/storage/new-archive.tar.gz",
            files_processed=25,
            bytes_processed=4096,
            error_message=None
        )
    
    @patch('tellus.interfaces.cli.archive._get_archive_service')
    @patch('os.path.exists')
    def test_create_archive_success(self, mock_exists, mock_get_service, mock_service, create_result):
        """Test successful archive creation."""
        mock_exists.return_value = True  # Mock path exists
        mock_get_service.return_value = mock_service
        
        # Mock the async method
        async def mock_create_archive(dto):
            return create_result
        
        mock_service.create_archive = AsyncMock(return_value=create_result)
        
        runner = CliRunner()
        result = runner.invoke(archive, [
            'create', 'new-archive', '/tmp/test-data',
            '--location', 'test-location',
            '--simulation', 'test-sim'
        ])
        
        assert result.exit_code == 0
        assert "Created archive: new-archive" in result.output
        assert "Files processed: 25" in result.output
        assert "Bytes processed: 4,096" in result.output
        
        # Verify correct DTO was passed
        mock_service.create_archive.assert_called_once()
        call_args = mock_service.create_archive.call_args[0][0]
        assert isinstance(call_args, CreateArchiveDto)
        assert call_args.archive_id == "new-archive"
        assert call_args.location_name == "test-location"
        assert call_args.simulation_id == "test-sim"
        assert call_args.source_path == "/tmp/test-data"
    
    @patch('tellus.interfaces.cli.archive._get_archive_service')
    @patch('os.path.exists')
    def test_create_archive_minimal_params(self, mock_exists, mock_get_service, mock_service):
        """Test archive creation with minimal parameters."""
        mock_exists.return_value = True
        mock_get_service.return_value = mock_service
        
        minimal_result = ArchiveOperationResultDto(
            success=True,
            archive_id="minimal-archive",
            operation_id="op-67890",
            destination_path="/storage/minimal-archive.tar.gz",
            files_processed=10,
            bytes_processed=1024,
            error_message=None
        )
        mock_service.create_archive = AsyncMock(return_value=minimal_result)
        
        runner = CliRunner()
        result = runner.invoke(archive, [
            'create', 'minimal-archive', '/tmp/minimal-data',
            '--location', 'test-location'
        ])
        
        assert result.exit_code == 0
        assert "Created archive: minimal-archive" in result.output
        
        # Verify DTO with minimal parameters
        call_args = mock_service.create_archive.call_args[0][0]
        assert call_args.simulation_id is None
        assert call_args.archive_type == "compressed"  # Default value
    
    @patch('tellus.interfaces.cli.archive._get_archive_service')
    @patch('os.path.exists')
    def test_create_archive_with_type(self, mock_exists, mock_get_service, mock_service):
        """Test archive creation with specific archive type."""
        mock_exists.return_value = True
        mock_get_service.return_value = mock_service
        
        uncompressed_result = ArchiveOperationResultDto(
            success=True,
            archive_id="uncompressed-archive",
            operation_id="op-uncompressed",
            destination_path="/storage/uncompressed-archive",
            files_processed=15,
            bytes_processed=2048,
            error_message=None
        )
        mock_service.create_archive = AsyncMock(return_value=uncompressed_result)
        
        runner = CliRunner()
        result = runner.invoke(archive, [
            'create', 'uncompressed-archive', '/tmp/data',
            '--location', 'test-location',
            '--type', 'uncompressed'
        ])
        
        assert result.exit_code == 0
        
        # Verify archive type was set correctly
        call_args = mock_service.create_archive.call_args[0][0]
        assert call_args.archive_type == "uncompressed"
    
    @patch('os.path.exists')
    def test_create_archive_nonexistent_source(self, mock_exists):
        """Test archive creation with nonexistent source path."""
        mock_exists.return_value = False
        
        runner = CliRunner()
        result = runner.invoke(archive, [
            'create', 'test-archive', '/nonexistent/path',
            '--location', 'test-location'
        ])
        
        assert result.exit_code != 0
        assert "does not exist" in result.output or "No such file" in result.output
    
    @patch('tellus.interfaces.cli.archive._get_archive_service')
    @patch('os.path.exists')
    def test_create_archive_failure(self, mock_exists, mock_get_service, mock_service):
        """Test handling archive creation failure."""
        mock_exists.return_value = True
        mock_get_service.return_value = mock_service
        
        failure_result = ArchiveOperationResultDto(
            success=False,
            archive_id="failed-archive",
            operation_id="op-failed",
            destination_path=None,
            files_processed=0,
            bytes_processed=0,
            error_message="Storage location not accessible"
        )
        mock_service.create_archive = AsyncMock(return_value=failure_result)
        
        runner = CliRunner()
        result = runner.invoke(archive, [
            'create', 'failed-archive', '/tmp/data',
            '--location', 'inaccessible-location'
        ])
        
        assert result.exit_code == 0  # CLI handles errors gracefully
        assert "Failed to create archive" in result.output
    
    @patch('tellus.interfaces.cli.archive._get_archive_service')
    @patch('os.path.exists')
    def test_create_archive_keyboard_interrupt(self, mock_exists, mock_get_service, mock_service):
        """Test handling keyboard interrupt during creation."""
        mock_exists.return_value = True
        mock_get_service.return_value = mock_service
        
        # Mock KeyboardInterrupt during async execution
        mock_service.create_archive = AsyncMock(side_effect=KeyboardInterrupt())
        
        runner = CliRunner()
        result = runner.invoke(archive, [
            'create', 'interrupted-archive', '/tmp/data',
            '--location', 'test-location'
        ])
        
        assert result.exit_code == 0
        assert "cancelled by user" in result.output.lower()


class TestArchiveFilesCommand:
    """Test cases for archive files command."""
    
    @pytest.fixture
    def mock_service(self):
        """Mock archive application service."""
        service = Mock(spec=ArchiveApplicationService)
        return service
    
    @pytest.fixture
    def sample_files(self):
        """Sample archive files for testing."""
        return [
            {
                "relative_path": "output/results.nc",
                "size": 1024,
                "content_type": "netcdf",
                "file_role": "output"
            },
            {
                "relative_path": "logs/simulation.log",
                "size": 256,
                "content_type": "log",
                "file_role": "log"
            },
            {
                "relative_path": "config/settings.yaml",
                "size": 128,
                "content_type": "config",
                "file_role": "config"
            }
        ]
    
    @patch('tellus.interfaces.cli.archive._get_archive_service')
    def test_files_command_success(self, mock_get_service, mock_service, sample_files):
        """Test successful listing of archive files."""
        mock_get_service.return_value = mock_service
        mock_service.list_archive_files.return_value = sample_files
        
        runner = CliRunner()
        result = runner.invoke(archive, ['files', 'test-archive'])
        
        assert result.exit_code == 0
        assert "results.nc" in result.output
        assert "simulation.log" in result.output
        assert "settings.yaml" in result.output
        assert "netcdf" in result.output
        assert "output" in result.output
        mock_service.list_archive_files.assert_called_once_with(
            archive_id="test-archive",
            content_type_filter=None,
            pattern_filter=None
        )
    
    @patch('tellus.interfaces.cli.archive._get_archive_service')
    def test_files_command_with_content_type_filter(self, mock_get_service, mock_service):
        """Test listing files with content type filter."""
        mock_get_service.return_value = mock_service
        log_files = [
            {
                "relative_path": "logs/simulation.log",
                "size": 256,
                "content_type": "log",
                "file_role": "log"
            }
        ]
        mock_service.list_archive_files.return_value = log_files
        
        runner = CliRunner()
        result = runner.invoke(archive, ['files', 'test-archive', '--content-type', 'log'])
        
        assert result.exit_code == 0
        assert "simulation.log" in result.output
        mock_service.list_archive_files.assert_called_once_with(
            archive_id="test-archive",
            content_type_filter="log",
            pattern_filter=None
        )
    
    @patch('tellus.interfaces.cli.archive._get_archive_service')
    def test_files_command_with_pattern_filter(self, mock_get_service, mock_service):
        """Test listing files with filename pattern filter."""
        mock_get_service.return_value = mock_service
        nc_files = [
            {
                "relative_path": "output/results.nc",
                "size": 1024,
                "content_type": "netcdf",
                "file_role": "output"
            }
        ]
        mock_service.list_archive_files.return_value = nc_files
        
        runner = CliRunner()
        result = runner.invoke(archive, ['files', 'test-archive', '--pattern', '*.nc'])
        
        assert result.exit_code == 0
        assert "results.nc" in result.output
        mock_service.list_archive_files.assert_called_once_with(
            archive_id="test-archive",
            content_type_filter=None,
            pattern_filter="*.nc"
        )
    
    @patch('tellus.interfaces.cli.archive._get_archive_service')
    def test_files_command_empty_archive(self, mock_get_service, mock_service):
        """Test listing files in empty archive."""
        mock_get_service.return_value = mock_service
        mock_service.list_archive_files.return_value = []
        
        runner = CliRunner()
        result = runner.invoke(archive, ['files', 'empty-archive'])
        
        assert result.exit_code == 0
        assert "No files found in archive" in result.output
    
    @patch('tellus.interfaces.cli.archive._get_archive_service')
    def test_files_command_archive_not_found(self, mock_get_service, mock_service):
        """Test listing files for non-existent archive."""
        mock_get_service.return_value = mock_service
        mock_service.list_archive_files.side_effect = EntityNotFoundError("Archive", "nonexistent")
        
        runner = CliRunner()
        result = runner.invoke(archive, ['files', 'nonexistent'])
        
        assert result.exit_code == 0  # CLI handles errors gracefully
        assert "Error:" in result.output


class TestArchiveCopyCommand:
    """Test cases for archive copy command."""
    
    @pytest.fixture
    def mock_service(self):
        """Mock archive application service."""
        service = Mock(spec=ArchiveApplicationService)
        return service
    
    @pytest.fixture
    def copy_result(self):
        """Sample archive copy result."""
        return ArchiveOperationResultDto(
            success=True,
            archive_id="test-archive",
            operation_id="copy-op-12345",
            destination_path="/dest/storage/test-archive.tar.gz",
            files_processed=0,  # Copy operation doesn't process individual files
            bytes_processed=0,
            error_message=None
        )
    
    @patch('tellus.interfaces.cli.archive._get_archive_service')
    def test_copy_archive_success(self, mock_get_service, mock_service, copy_result):
        """Test successful archive copy operation."""
        mock_get_service.return_value = mock_service
        mock_service.copy_archive_to_location.return_value = copy_result
        
        runner = CliRunner()
        result = runner.invoke(archive, [
            'copy', 'test-archive', 'source-location', 'dest-location',
            '--simulation', 'test-sim'
        ])
        
        assert result.exit_code == 0
        assert "Archive copy initiated: copy-op-12345" in result.output
        
        # Verify correct DTO was passed
        mock_service.copy_archive_to_location.assert_called_once()
        call_args = mock_service.copy_archive_to_location.call_args[0][0]
        assert isinstance(call_args, ArchiveCopyOperationDto)
        assert call_args.archive_id == "test-archive"
        assert call_args.source_location == "source-location"
        assert call_args.destination_location == "dest-location"
        assert call_args.simulation_id == "test-sim"
    
    @patch('tellus.interfaces.cli.archive._get_archive_service')
    def test_copy_archive_minimal_params(self, mock_get_service, mock_service, copy_result):
        """Test archive copy with minimal parameters."""
        mock_get_service.return_value = mock_service
        mock_service.copy_archive_to_location.return_value = copy_result
        
        runner = CliRunner()
        result = runner.invoke(archive, [
            'copy', 'test-archive', 'source-location', 'dest-location'
        ])
        
        assert result.exit_code == 0
        
        # Verify simulation_id is None when not provided
        call_args = mock_service.copy_archive_to_location.call_args[0][0]
        assert call_args.simulation_id is None
    
    @patch('tellus.interfaces.cli.archive._get_archive_service')
    def test_copy_archive_service_error(self, mock_get_service, mock_service):
        """Test handling service errors during copy."""
        mock_get_service.return_value = mock_service
        mock_service.copy_archive_to_location.side_effect = Exception("Copy service error")
        
        runner = CliRunner()
        result = runner.invoke(archive, [
            'copy', 'test-archive', 'source-location', 'dest-location'
        ])
        
        assert result.exit_code == 0  # CLI handles errors gracefully
        assert "Error:" in result.output


class TestArchiveExtractCommand:
    """Test cases for archive extract command."""
    
    @pytest.fixture
    def mock_service(self):
        """Mock archive application service."""
        service = Mock(spec=ArchiveApplicationService)
        return service
    
    @pytest.fixture
    def extract_result_success(self):
        """Sample successful extraction result."""
        return ArchiveOperationResultDto(
            success=True,
            archive_id="test-archive",
            operation_id="extract-op-12345",
            destination_path="/extracted/test-archive",
            files_processed=15,
            bytes_processed=4096,
            error_message=None
        )
    
    @pytest.fixture
    def extract_result_failure(self):
        """Sample failed extraction result."""
        return ArchiveOperationResultDto(
            success=False,
            archive_id="test-archive",
            operation_id="extract-op-failed",
            destination_path=None,
            files_processed=0,
            bytes_processed=0,
            error_message="Destination location not accessible"
        )
    
    @patch('tellus.interfaces.cli.archive._get_archive_service')
    def test_extract_archive_success(self, mock_get_service, mock_service, extract_result_success):
        """Test successful archive extraction."""
        mock_get_service.return_value = mock_service
        mock_service.extract_archive_to_location.return_value = extract_result_success
        
        runner = CliRunner()
        result = runner.invoke(archive, [
            'extract', 'test-archive', 'source-location', 'dest-location',
            '--simulation', 'test-sim',
            '--content-type', 'output'
        ])
        
        assert result.exit_code == 0
        assert "Extracted 15 files to /extracted/test-archive" in result.output
        
        # Verify correct DTO was passed
        mock_service.extract_archive_to_location.assert_called_once()
        call_args = mock_service.extract_archive_to_location.call_args[0][0]
        assert isinstance(call_args, ArchiveExtractionDto)
        assert call_args.archive_id == "test-archive"
        assert call_args.source_location == "source-location"
        assert call_args.destination_location == "dest-location"
        assert call_args.simulation_id == "test-sim"
        assert call_args.content_type_filter == "output"
    
    @patch('tellus.interfaces.cli.archive._get_archive_service')
    def test_extract_archive_minimal_params(self, mock_get_service, mock_service, extract_result_success):
        """Test archive extraction with minimal parameters."""
        mock_get_service.return_value = mock_service
        mock_service.extract_archive_to_location.return_value = extract_result_success
        
        runner = CliRunner()
        result = runner.invoke(archive, [
            'extract', 'test-archive', 'source-location', 'dest-location'
        ])
        
        assert result.exit_code == 0
        
        # Verify optional parameters are None when not provided
        call_args = mock_service.extract_archive_to_location.call_args[0][0]
        assert call_args.simulation_id is None
        assert call_args.content_type_filter is None
    
    @patch('tellus.interfaces.cli.archive._get_archive_service')
    def test_extract_archive_failure(self, mock_get_service, mock_service, extract_result_failure):
        """Test handling extraction failure."""
        mock_get_service.return_value = mock_service
        mock_service.extract_archive_to_location.return_value = extract_result_failure
        
        runner = CliRunner()
        result = runner.invoke(archive, [
            'extract', 'test-archive', 'source-location', 'dest-location'
        ])
        
        assert result.exit_code == 0  # CLI handles errors gracefully
        assert "Extraction failed: Destination location not accessible" in result.output
    
    @patch('tellus.interfaces.cli.archive._get_archive_service')
    def test_extract_archive_service_error(self, mock_get_service, mock_service):
        """Test handling service errors during extraction."""
        mock_get_service.return_value = mock_service
        mock_service.extract_archive_to_location.side_effect = Exception("Extraction service error")
        
        runner = CliRunner()
        result = runner.invoke(archive, [
            'extract', 'test-archive', 'source-location', 'dest-location'
        ])
        
        assert result.exit_code == 0  # CLI handles errors gracefully
        assert "Error:" in result.output


class TestArchiveCLIIntegration:
    """Integration tests for archive CLI commands."""
    
    @patch('tellus.interfaces.cli.archive._get_archive_service')
    def test_cli_error_handling_service_unavailable(self, mock_get_service):
        """Test CLI behavior when service is unavailable."""
        mock_get_service.side_effect = Exception("Service initialization failed")
        
        runner = CliRunner()
        result = runner.invoke(archive, ['list'])
        
        assert result.exit_code == 0  # CLI handles errors gracefully
        assert "Error:" in result.output
    
    def test_cli_help_commands(self):
        """Test help output for CLI commands."""
        runner = CliRunner()
        
        # Test main help
        result = runner.invoke(archive, ['--help'])
        assert result.exit_code == 0
        assert "archive" in result.output.lower()
        
        # Test subcommand help
        result = runner.invoke(archive, ['create', '--help'])
        assert result.exit_code == 0
        assert "create" in result.output.lower()
        assert "location" in result.output.lower()
        
        result = runner.invoke(archive, ['extract', '--help'])
        assert result.exit_code == 0
        assert "extract" in result.output.lower()
        assert "content-type" in result.output.lower()
    
    @patch('tellus.interfaces.cli.archive._get_archive_service')
    def test_cli_output_formatting_consistency(self, mock_get_service):
        """Test that CLI output formatting is consistent across commands."""
        mock_service = Mock(spec=ArchiveApplicationService)
        mock_get_service.return_value = mock_service
        
        # Test that all commands handle rich formatting consistently
        sample_archive = ArchiveDto(
            archive_id="test-archive",
            location="test-location",
            archive_type="COMPRESSED",
            simulation_id="test-sim",
            created_at="2023-01-01T12:00:00Z",
            size_bytes=1024,
            file_count=10
        )
        
        list_result = ArchiveListDto(
            archives=[sample_archive],
            pagination=PaginationInfo(),
            filters_applied=FilterOptions()
        )
        mock_service.list_archives.return_value = list_result
        mock_service.get_archive.return_value = sample_archive
        mock_service.list_archive_files.return_value = []
        
        runner = CliRunner()
        
        # All commands should succeed and use consistent formatting
        commands_to_test = [
            ['list'],
            ['show', 'test-archive'],
            ['files', 'test-archive']
        ]
        
        for cmd in commands_to_test:
            result = runner.invoke(archive, cmd)
            assert result.exit_code == 0
            # Verify no malformed output or exceptions in formatting
            assert "Traceback" not in result.output


class TestArchiveCLIArgumentParsing:
    """Test CLI argument parsing and validation."""
    
    def test_create_command_argument_parsing(self):
        """Test that create command properly parses arguments."""
        runner = CliRunner()
        
        # Test missing required arguments
        result = runner.invoke(archive, ['create'])
        assert result.exit_code != 0
        assert "Missing argument" in result.output or "Usage:" in result.output
        
        # Test missing required location option
        result = runner.invoke(archive, ['create', 'test-archive', '/tmp/data'])
        assert result.exit_code != 0
        assert "Missing option" in result.output or "--location" in result.output
    
    def test_show_command_argument_parsing(self):
        """Test that show command properly parses arguments."""
        runner = CliRunner()
        
        # Test missing required argument
        result = runner.invoke(archive, ['show'])
        assert result.exit_code != 0
        assert "Missing argument" in result.output or "Usage:" in result.output
    
    def test_copy_command_argument_parsing(self):
        """Test that copy command properly parses arguments."""
        runner = CliRunner()
        
        # Test missing required arguments
        result = runner.invoke(archive, ['copy', 'test-archive'])
        assert result.exit_code != 0
        assert "Missing argument" in result.output or "Usage:" in result.output
        
        result = runner.invoke(archive, ['copy', 'test-archive', 'source'])
        assert result.exit_code != 0
        assert "Missing argument" in result.output or "Usage:" in result.output
    
    def test_extract_command_argument_parsing(self):
        """Test that extract command properly parses arguments."""
        runner = CliRunner()
        
        # Test missing required arguments
        result = runner.invoke(archive, ['extract', 'test-archive'])
        assert result.exit_code != 0
        assert "Missing argument" in result.output or "Usage:" in result.output
    
    def test_invalid_command(self):
        """Test handling of invalid subcommands."""
        runner = CliRunner()
        result = runner.invoke(archive, ['invalid-command'])
        assert result.exit_code != 0
        assert "No such command" in result.output


class TestArchiveCLIErrorScenarios:
    """Test CLI error handling scenarios."""
    
    @patch('tellus.interfaces.cli.archive._get_archive_service')
    def test_service_timeout_handling(self, mock_get_service):
        """Test CLI handling of service timeouts."""
        mock_service = Mock(spec=ArchiveApplicationService)
        mock_get_service.return_value = mock_service
        mock_service.list_archives.side_effect = TimeoutError("Service timeout")
        
        runner = CliRunner()
        result = runner.invoke(archive, ['list'])
        
        assert result.exit_code == 0  # CLI should handle gracefully
        assert "Error:" in result.output
    
    @patch('tellus.interfaces.cli.archive._get_archive_service')
    def test_permission_error_handling(self, mock_get_service):
        """Test CLI handling of permission errors."""
        mock_service = Mock(spec=ArchiveApplicationService)
        mock_get_service.return_value = mock_service
        mock_service.get_archive.side_effect = PermissionError("Access denied")
        
        runner = CliRunner()
        result = runner.invoke(archive, ['show', 'restricted-archive'])
        
        assert result.exit_code == 0  # CLI should handle gracefully
        assert "Error:" in result.output


class TestArchiveCLIRichFormatting:
    """Test CLI rich output formatting behaviors."""
    
    @patch('tellus.interfaces.cli.archive._get_archive_service')
    def test_table_formatting_with_various_data(self, mock_get_service):
        """Test that table formatting handles various data types correctly."""
        mock_service = Mock(spec=ArchiveApplicationService)
        mock_get_service.return_value = mock_service
        
        # Create archives with various combinations of data
        archives_with_varied_data = [
            ArchiveDto(
                archive_id="simple",
                location="local",
                archive_type="COMPRESSED",
                simulation_id=None,  # Orphaned
                created_at="2023-01-01T12:00:00Z",
                size_bytes=512,
                file_count=3
            ),
            ArchiveDto(
                archive_id="complex-archive-with-very-long-name",
                location="very-long-location-name-that-might-wrap",
                archive_type="UNCOMPRESSED",
                simulation_id="very-long-simulation-id-name",
                created_at="2023-01-02T14:30:00Z",
                size_bytes=1048576,  # 1MB
                file_count=1000
            )
        ]
        
        list_result = ArchiveListDto(
            archives=archives_with_varied_data,
            pagination=PaginationInfo(),
            filters_applied=FilterOptions()
        )
        mock_service.list_archives.return_value = list_result
        
        runner = CliRunner()
        result = runner.invoke(archive, ['list'])
        
        assert result.exit_code == 0
        # Verify table formatting handles empty/None values with "-"
        assert "-" in result.output
        # Verify complex data is displayed (might be truncated)
        assert "complex-archive" in result.output
        assert "very-long-location" in result.output
        assert "UNCOMPRESSED" in result.output
    
    @patch('tellus.interfaces.cli.archive._get_archive_service')
    def test_files_table_formatting(self, mock_get_service):
        """Test that files command formats file information correctly."""
        mock_service = Mock(spec=ArchiveApplicationService)
        mock_get_service.return_value = mock_service
        
        varied_files = [
            {
                "relative_path": "very/deep/nested/path/file.nc",
                "size": 1048576,  # 1MB
                "content_type": "netcdf",
                "file_role": "output"
            },
            {
                "relative_path": "file_without_size.txt",
                "size": None,
                "content_type": None,
                "file_role": None
            }
        ]
        mock_service.list_archive_files.return_value = varied_files
        
        runner = CliRunner()
        result = runner.invoke(archive, ['files', 'test-archive'])
        
        assert result.exit_code == 0
        # Verify nested paths are displayed
        assert "very/deep/nested" in result.output
        # Verify missing values show as "-"
        assert "-" in result.output
        # Verify file sizes and types are displayed
        assert "netcdf" in result.output
        assert "output" in result.output