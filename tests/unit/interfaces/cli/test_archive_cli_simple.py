"""
Simplified unit tests for archive CLI commands.

These tests verify the core archive management CLI interface functionality.
"""

from unittest.mock import AsyncMock, Mock, patch

import pytest
from click.testing import CliRunner

from tellus.application.dtos import (ArchiveDto, ArchiveListDto, FilterOptions,
                                     PaginationInfo)
from tellus.application.exceptions import EntityNotFoundError
from tellus.application.services.archive_service import \
    ArchiveApplicationService
from tellus.interfaces.cli.archive import archive


class TestArchiveListCommandSimple:
    """Test cases for archive list command."""
    
    @patch('tellus.interfaces.cli.archive._get_archive_service')
    def test_list_archives_empty(self, mock_get_service):
        """Test listing when no archives exist."""
        mock_service = Mock()
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
    
    @patch('tellus.interfaces.cli.archive._get_archive_service')
    def test_list_archives_with_data(self, mock_get_service):
        """Test listing archives with sample data."""
        mock_service = Mock()
        mock_get_service.return_value = mock_service
        
        sample_archives = [
            ArchiveDto(
                archive_id="test-archive-1",
                location="localhost",
                archive_type="COMPRESSED",
                simulation_id="test-sim",
                size=1024,
                created_time=1672574400.0
            )
        ]
        
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
        assert "localhost" in result.output
        assert "COMPRESSED" in result.output
    
    @patch('tellus.interfaces.cli.archive._get_archive_service')
    def test_list_archives_service_error(self, mock_get_service):
        """Test handling service errors during listing."""
        mock_service = Mock()
        mock_get_service.return_value = mock_service
        mock_service.list_archives.side_effect = Exception("Service error")
        
        runner = CliRunner()
        result = runner.invoke(archive, ['list'])
        
        assert result.exit_code == 0  # CLI handles errors gracefully
        assert "Error:" in result.output


class TestArchiveShowCommandSimple:
    """Test cases for archive show command."""
    
    @patch('tellus.interfaces.cli.archive._get_archive_service')
    def test_show_archive_success(self, mock_get_service):
        """Test successful display of archive details."""
        mock_service = Mock()
        mock_get_service.return_value = mock_service
        
        sample_archive = ArchiveDto(
            archive_id="test-archive",
            location="test-location",
            archive_type="COMPRESSED",
            simulation_id="test-simulation",
            size=2048,
            created_time=1672574400.0
        )
        mock_service.get_archive.return_value = sample_archive
        
        runner = CliRunner()
        result = runner.invoke(archive, ['show', 'test-archive'])
        
        assert result.exit_code == 0
        assert "test-archive" in result.output
        assert "test-location" in result.output
        assert "COMPRESSED" in result.output
        assert "test-simulation" in result.output
    
    @patch('tellus.interfaces.cli.archive._get_archive_service')
    def test_show_archive_not_found(self, mock_get_service):
        """Test showing non-existent archive."""
        mock_service = Mock()
        mock_get_service.return_value = mock_service
        mock_service.get_archive.side_effect = EntityNotFoundError("Archive", "nonexistent")
        
        runner = CliRunner()
        result = runner.invoke(archive, ['show', 'nonexistent'])
        
        assert result.exit_code == 0  # CLI handles errors gracefully
        assert "Error:" in result.output


class TestArchiveFilesCommandSimple:
    """Test cases for archive files command."""
    
    @patch('tellus.interfaces.cli.archive._get_archive_service')
    def test_files_command_empty_archive(self, mock_get_service):
        """Test listing files in empty archive."""
        mock_service = Mock()
        mock_get_service.return_value = mock_service
        mock_service.list_archive_files.return_value = []
        
        runner = CliRunner()
        result = runner.invoke(archive, ['files', 'empty-archive'])
        
        assert result.exit_code == 0
        assert "No files found in archive" in result.output
    
    @patch('tellus.interfaces.cli.archive._get_archive_service')
    def test_files_command_with_files(self, mock_get_service):
        """Test listing files with sample data."""
        mock_service = Mock()
        mock_get_service.return_value = mock_service
        
        # Create proper file objects instead of dicts
        class MockFile:
            def __init__(self, path, size=None, content_type=None, file_role=None):
                self.relative_path = path
                self.size = size
                self.content_type = content_type
                self.file_role = file_role
        
        sample_files = [
            MockFile("output/results.nc", 1024, "netcdf", "output"),
            MockFile("logs/simulation.log", 256, "log", "log")
        ]
        
        mock_service.list_archive_files.return_value = sample_files
        
        runner = CliRunner()
        result = runner.invoke(archive, ['files', 'test-archive'])
        
        assert result.exit_code == 0
        assert "results.nc" in result.output
        assert "simulation.log" in result.output
    
    @patch('tellus.interfaces.cli.archive._get_archive_service')
    def test_files_command_archive_not_found(self, mock_get_service):
        """Test listing files for non-existent archive."""
        mock_service = Mock()
        mock_get_service.return_value = mock_service
        mock_service.list_archive_files.side_effect = EntityNotFoundError("Archive", "nonexistent")
        
        runner = CliRunner()
        result = runner.invoke(archive, ['files', 'nonexistent'])
        
        assert result.exit_code == 0  # CLI handles errors gracefully
        assert "Error:" in result.output


class TestArchiveCLIHelpSimple:
    """Test CLI help and argument parsing."""
    
    def test_cli_help_commands(self):
        """Test help output for CLI commands."""
        runner = CliRunner()
        
        # Test main help
        result = runner.invoke(archive, ['--help'])
        assert result.exit_code == 0
        assert "archive" in result.output.lower()
        
        # Test subcommand help
        result = runner.invoke(archive, ['list', '--help'])
        assert result.exit_code == 0
        assert "list" in result.output.lower()
    
    def test_show_command_argument_parsing(self):
        """Test that show command properly parses arguments."""
        runner = CliRunner()
        
        # Test missing required argument
        result = runner.invoke(archive, ['show'])
        assert result.exit_code != 0
        assert "Missing argument" in result.output or "Usage:" in result.output
    
    def test_invalid_command(self):
        """Test handling of invalid subcommands."""
        runner = CliRunner()
        result = runner.invoke(archive, ['invalid-command'])
        assert result.exit_code != 0
        assert "No such command" in result.output


class TestArchiveCLIErrorHandling:
    """Test CLI error handling."""
    
    @patch('tellus.interfaces.cli.archive._get_archive_service')
    def test_service_unavailable(self, mock_get_service):
        """Test CLI behavior when service is unavailable."""
        mock_get_service.side_effect = Exception("Service initialization failed")
        
        runner = CliRunner()
        result = runner.invoke(archive, ['list'])
        
        assert result.exit_code == 0  # CLI handles errors gracefully
        assert "Error:" in result.output
    
    @patch('tellus.interfaces.cli.archive._get_archive_service')
    def test_timeout_handling(self, mock_get_service):
        """Test CLI handling of service timeouts."""
        mock_service = Mock()
        mock_get_service.return_value = mock_service
        mock_service.list_archives.side_effect = TimeoutError("Service timeout")
        
        runner = CliRunner()
        result = runner.invoke(archive, ['list'])
        
        assert result.exit_code == 0  # CLI should handle gracefully
        assert "Error:" in result.output