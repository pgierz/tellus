"""
Unit tests for location CLI commands.

These tests verify the location management CLI interface layer,
including command parsing, service integration, error handling,
and output formatting.
"""

import io
import json
import sys
from datetime import datetime
from unittest.mock import MagicMock, Mock, patch

import pytest
from click.testing import CliRunner
from rich.console import Console

from tellus.application.dtos import (CreateLocationDto, FilterOptions,
                                     LocationDto, LocationListDto,
                                     PaginationInfo, UpdateLocationDto)
from tellus.application.exceptions import (EntityAlreadyExistsError,
                                           EntityNotFoundError,
                                           ValidationError)
from tellus.application.services.location_service import \
    LocationApplicationService
from tellus.domain.entities.location import LocationKind
from tellus.interfaces.cli.location import location


class TestLocationListCommand:
    """Test cases for location list command."""
    
    @pytest.fixture
    def mock_service(self):
        """Mock location application service."""
        service = Mock(spec=LocationApplicationService)
        return service
    
    @pytest.fixture
    def sample_locations(self):
        """Sample location DTOs for testing."""
        return [
            LocationDto(
                name="localhost",
                kinds=["DISK"],
                protocol="file",
                path="/home/user/data",
                storage_options={},
                optional=False,
                additional_config={}
            ),
            LocationDto(
                name="hpc-cluster",
                kinds=["COMPUTE", "DISK"],
                protocol="ssh",
                path="/scratch/project",
                storage_options={"host": "cluster.domain.com", "port": 22},
                optional=False,
                additional_config={"partition": "compute"}
            ),
            LocationDto(
                name="cloud-storage",
                kinds=["FILESERVER"],
                protocol="s3",
                path="/bucket/data",
                storage_options={"endpoint_url": "https://s3.amazonaws.com"},
                optional=True,
                additional_config={}
            )
        ]
    
    @patch('tellus.interfaces.cli.location._get_location_service')
    def test_list_all_locations_success(self, mock_get_service, mock_service, sample_locations):
        """Test successful listing of all locations."""
        mock_get_service.return_value = mock_service
        list_result = LocationListDto(
            locations=sample_locations,
            pagination=PaginationInfo(),
            filters_applied=FilterOptions()
        )
        mock_service.list_locations.return_value = list_result
        
        runner = CliRunner()
        result = runner.invoke(location, ['list'])
        
        assert result.exit_code == 0
        assert "localhost" in result.output
        assert "hpc-cluster" in result.output
        assert "cloud-storage" in result.output
        assert "file" in result.output
        assert "ssh" in result.output
        mock_service.list_locations.assert_called_once()
    
    @patch('tellus.interfaces.cli.location._get_location_service')
    def test_list_locations_empty(self, mock_get_service, mock_service):
        """Test listing when no locations exist."""
        mock_get_service.return_value = mock_service
        empty_result = LocationListDto(
            locations=[],
            pagination=PaginationInfo(),
            filters_applied=FilterOptions()
        )
        mock_service.list_locations.return_value = empty_result
        
        runner = CliRunner()
        result = runner.invoke(location, ['list'])
        
        assert result.exit_code == 0
        assert "No locations found" in result.output
        mock_service.list_locations.assert_called_once()
    
    @patch('tellus.interfaces.cli.location._get_location_service')
    def test_list_locations_with_different_kinds(self, mock_get_service, mock_service):
        """Test listing locations with different kinds displayed correctly."""
        mock_get_service.return_value = mock_service
        locations_with_kinds = [
            LocationDto(
                name="tape-storage",
                kinds=["TAPE"],
                protocol="scoutfs",
                path="/tape/archive",
                storage_options={"host": "tape.domain.com"},
                optional=False,
                additional_config={}
            ),
            LocationDto(
                name="multi-purpose",
                kinds=["COMPUTE", "DISK", "FILESERVER"],
                protocol="ssh",
                path="/multi",
                storage_options={"host": "multi.domain.com"},
                optional=False,
                additional_config={}
            )
        ]
        list_result = LocationListDto(
            locations=locations_with_kinds,
            pagination=PaginationInfo(),
            filters_applied=FilterOptions()
        )
        mock_service.list_locations.return_value = list_result
        
        runner = CliRunner()
        result = runner.invoke(location, ['list'])
        
        assert result.exit_code == 0
        assert "tape" in result.output.lower()
        assert "compute" in result.output.lower()
        assert "disk" in result.output.lower()
        assert "fileserver" in result.output.lower()
    
    @patch('tellus.interfaces.cli.location._get_location_service')
    def test_list_locations_service_error(self, mock_get_service, mock_service):
        """Test handling service errors during listing."""
        mock_get_service.return_value = mock_service
        mock_service.list_locations.side_effect = Exception("Service error")
        
        runner = CliRunner()
        result = runner.invoke(location, ['list'])
        
        assert result.exit_code == 0  # CLI handles errors gracefully
        assert "Error:" in result.output


class TestLocationShowCommand:
    """Test cases for location show command."""
    
    @pytest.fixture
    def mock_service(self):
        """Mock location application service."""
        service = Mock(spec=LocationApplicationService)
        return service
    
    @pytest.fixture
    def sample_location(self):
        """Sample location DTO for testing."""
        return LocationDto(
            name="test-location",
            kinds=["COMPUTE", "DISK"],
            protocol="ssh",
            path="/scratch/user",
            storage_options={"host": "compute.domain.com", "port": 22, "username": "testuser"},
            optional=False,
            additional_config={
                "partition": "compute",
                "max_jobs": 100,
                "walltime": "24:00:00"
            }
        )
    
    @patch('tellus.interfaces.cli.location._get_location_service')
    def test_show_location_success(self, mock_get_service, mock_service, sample_location):
        """Test successful display of location details."""
        mock_get_service.return_value = mock_service
        mock_service.get_location.return_value = sample_location
        
        runner = CliRunner()
        result = runner.invoke(location, ['show', 'test-location'])
        
        assert result.exit_code == 0
        assert "test-location" in result.output
        assert "ssh" in result.output
        assert "/scratch/user" in result.output
        assert "compute.domain.com" in result.output
        assert "Storage.host" in result.output
        assert "Config.partition" in result.output
        assert "compute" in result.output.lower()
        assert "disk" in result.output.lower()
        mock_service.get_location.assert_called_once_with("test-location")
    
    @patch('tellus.interfaces.cli.location._get_location_service')
    def test_show_location_minimal_config(self, mock_get_service, mock_service):
        """Test showing location with minimal configuration."""
        mock_get_service.return_value = mock_service
        minimal_location = LocationDto(
            name="minimal-loc",
            kinds=[],
            protocol="file",
            path=None,
            storage_options={},
            optional=True,
            additional_config={}
        )
        mock_service.get_location.return_value = minimal_location
        
        runner = CliRunner()
        result = runner.invoke(location, ['show', 'minimal-loc'])
        
        assert result.exit_code == 0
        assert "minimal-loc" in result.output
        assert "file" in result.output
        # Should show "-" for empty/None values
        assert "-" in result.output
    
    @patch('tellus.interfaces.cli.location._get_location_service')
    def test_show_location_not_found(self, mock_get_service, mock_service):
        """Test showing non-existent location."""
        mock_get_service.return_value = mock_service
        mock_service.get_location.side_effect = EntityNotFoundError("Location", "nonexistent")
        
        runner = CliRunner()
        result = runner.invoke(location, ['show', 'nonexistent'])
        
        assert result.exit_code == 0  # CLI handles errors gracefully
        assert "Error:" in result.output
        mock_service.get_location.assert_called_once_with("nonexistent")


class TestLocationCreateCommand:
    """Test cases for location create command."""
    
    @pytest.fixture
    def mock_service(self):
        """Mock location application service."""
        service = Mock(spec=LocationApplicationService)
        return service
    
    @pytest.fixture
    def created_location(self):
        """Sample created location DTO."""
        return LocationDto(
            name="new-location",
            kinds=["DISK"],
            protocol="file",
            path="/data/new",
            storage_options={},
            optional=False,
            additional_config={}
        )
    
    @patch('tellus.interfaces.cli.location._get_location_service')
    def test_create_location_success(self, mock_get_service, mock_service, created_location):
        """Test successful location creation."""
        mock_get_service.return_value = mock_service
        mock_service.create_location.return_value = created_location
        
        runner = CliRunner()
        result = runner.invoke(location, [
            'create', 'new-location',
            '--protocol', 'file',
            '--kind', 'disk',
            '--path', '/data/new'
        ])
        
        assert result.exit_code == 0
        assert "Created location: new-location" in result.output
        
        # Verify correct DTO was passed
        mock_service.create_location.assert_called_once()
        call_args = mock_service.create_location.call_args[0][0]
        assert isinstance(call_args, CreateLocationDto)
        assert call_args.name == "new-location"
        assert call_args.protocol == "file"
        assert call_args.kinds == ["DISK"]
        assert call_args.path == "/data/new"
    
    @patch('tellus.interfaces.cli.location._get_location_service')
    def test_create_location_with_multiple_kinds(self, mock_get_service, mock_service):
        """Test location creation with multiple kinds."""
        mock_get_service.return_value = mock_service
        multi_kind_location = LocationDto(
            name="multi-loc",
            kinds=["COMPUTE", "DISK"],
            protocol="ssh",
            path="/multi",
            storage_options={"host": "multi.domain.com"},
            optional=False,
            additional_config={}
        )
        mock_service.create_location.return_value = multi_kind_location
        
        runner = CliRunner()
        result = runner.invoke(location, [
            'create', 'multi-loc',
            '--protocol', 'ssh',
            '--kind', 'compute',
            '--kind', 'disk',
            '--host', 'multi.domain.com',
            '--path', '/multi'
        ])
        
        assert result.exit_code == 0
        assert "Created location: multi-loc" in result.output
        
        # Verify correct DTO was passed
        call_args = mock_service.create_location.call_args[0][0]
        assert set(call_args.kinds) == {"COMPUTE", "DISK"}
        assert call_args.storage_options == {"host": "multi.domain.com"}
    
    @patch('tellus.interfaces.cli.location._get_location_service')
    def test_create_location_minimal_params(self, mock_get_service, mock_service):
        """Test location creation with minimal parameters."""
        mock_get_service.return_value = mock_service
        minimal_location = LocationDto(
            name="minimal-loc",
            kinds=[],
            protocol="file",
            path=None,
            storage_options={},
            optional=False,
            additional_config={}
        )
        mock_service.create_location.return_value = minimal_location
        
        runner = CliRunner()
        result = runner.invoke(location, [
            'create', 'minimal-loc',
            '--protocol', 'file'
        ])
        
        assert result.exit_code == 0
        assert "Created location: minimal-loc" in result.output
        
        # Verify correct DTO was passed
        call_args = mock_service.create_location.call_args[0][0]
        assert call_args.name == "minimal-loc"
        assert call_args.protocol == "file"
        assert call_args.kinds == []
        assert call_args.path is None
        assert call_args.storage_options == {}
    
    @patch('tellus.interfaces.cli.location._get_location_service')
    def test_create_location_with_remote_host(self, mock_get_service, mock_service):
        """Test location creation with remote host configuration."""
        mock_get_service.return_value = mock_service
        remote_location = LocationDto(
            name="remote-loc",
            kinds=["COMPUTE"],
            protocol="ssh",
            path="/remote/path",
            storage_options={"host": "remote.domain.com"},
            optional=False,
            additional_config={}
        )
        mock_service.create_location.return_value = remote_location
        
        runner = CliRunner()
        result = runner.invoke(location, [
            'create', 'remote-loc',
            '--protocol', 'ssh',
            '--kind', 'compute',
            '--host', 'remote.domain.com',
            '--path', '/remote/path'
        ])
        
        assert result.exit_code == 0
        
        # Verify host is added to storage_options
        call_args = mock_service.create_location.call_args[0][0]
        assert call_args.storage_options["host"] == "remote.domain.com"
    
    @patch('tellus.interfaces.cli.location._get_location_service')
    def test_create_location_already_exists(self, mock_get_service, mock_service):
        """Test creating location that already exists."""
        mock_get_service.return_value = mock_service
        mock_service.create_location.side_effect = EntityAlreadyExistsError(
            "Location", "existing-loc"
        )
        
        runner = CliRunner()
        result = runner.invoke(location, [
            'create', 'existing-loc',
            '--protocol', 'file'
        ])
        
        assert result.exit_code == 0  # CLI handles errors gracefully
        assert "Error:" in result.output
    
    @patch('tellus.interfaces.cli.location._get_location_service')
    def test_create_location_validation_error(self, mock_get_service, mock_service):
        """Test location creation with validation errors."""
        mock_get_service.return_value = mock_service
        mock_service.create_location.side_effect = ValidationError("Invalid protocol")
        
        runner = CliRunner()
        result = runner.invoke(location, [
            'create', 'invalid-loc',
            '--protocol', 'invalid-protocol'
        ])
        
        assert result.exit_code == 0  # CLI handles errors gracefully
        assert "Error:" in result.output
    


class TestLocationCLIIntegration:
    """Integration tests for location CLI commands."""
    
    @patch('tellus.interfaces.cli.location._get_location_service')
    def test_cli_error_handling_service_unavailable(self, mock_get_service):
        """Test CLI behavior when service is unavailable."""
        mock_get_service.side_effect = Exception("Service initialization failed")
        
        runner = CliRunner()
        result = runner.invoke(location, ['list'])
        
        assert result.exit_code == 0  # CLI handles errors gracefully
        assert "Error:" in result.output
    
    def test_cli_help_commands(self):
        """Test help output for CLI commands."""
        runner = CliRunner()
        
        # Test main help
        result = runner.invoke(location, ['--help'])
        assert result.exit_code == 0
        assert "location" in result.output.lower()
        
        # Test subcommand help
        result = runner.invoke(location, ['create', '--help'])
        assert result.exit_code == 0
        assert "create" in result.output.lower()
        assert "protocol" in result.output.lower()
        assert "kind" in result.output.lower()
    
    @patch('tellus.interfaces.cli.location._get_location_service')
    def test_cli_output_formatting_consistency(self, mock_get_service):
        """Test that CLI output formatting is consistent across commands."""
        mock_service = Mock(spec=LocationApplicationService)
        mock_get_service.return_value = mock_service
        
        # Test that all commands handle rich formatting consistently
        sample_location = LocationDto(
            name="test-loc",
            kinds=["DISK"],
            protocol="file",
            path="/test/path",
            storage_options={},
            optional=False,
            additional_config={}
        )
        
        list_result = LocationListDto(
            locations=[sample_location],
            pagination=PaginationInfo(),
            filters_applied=FilterOptions()
        )
        mock_service.list_locations.return_value = list_result
        mock_service.get_location.return_value = sample_location
        
        runner = CliRunner()
        
        # All commands should succeed and use consistent formatting
        commands_to_test = [
            ['list'],
            ['show', 'test-loc'],
        ]
        
        for cmd in commands_to_test:
            result = runner.invoke(location, cmd)
            assert result.exit_code == 0
            # Verify no malformed output or exceptions in formatting
            assert "Traceback" not in result.output


class TestLocationCLIArgumentParsing:
    """Test CLI argument parsing and validation."""
    
    def test_create_command_argument_parsing(self):
        """Test that create command properly parses arguments."""
        runner = CliRunner()
        
        # Test missing required argument (name)
        result = runner.invoke(location, ['create'])
        assert result.exit_code != 0
        assert "Missing argument" in result.output or "Usage:" in result.output
        
        # Test missing required option (protocol)
        result = runner.invoke(location, ['create', 'test-loc'])
        assert result.exit_code != 0
        assert "Missing option" in result.output or "--protocol" in result.output
    
    def test_show_command_argument_parsing(self):
        """Test that show command properly parses arguments."""
        runner = CliRunner()
        
        # Test missing required argument
        result = runner.invoke(location, ['show'])
        assert result.exit_code != 0
        assert "Missing argument" in result.output or "Usage:" in result.output
    
    def test_invalid_command(self):
        """Test handling of invalid subcommands."""
        runner = CliRunner()
        result = runner.invoke(location, ['invalid-command'])
        assert result.exit_code != 0
        assert "No such command" in result.output
    
    def test_kind_option_validation(self):
        """Test that kind option accepts valid location kind values."""
        runner = CliRunner()
        
        # Test invalid kind option
        result = runner.invoke(location, [
            'create', 'test-loc',
            '--protocol', 'file',
            '--kind', 'invalid-kind'
        ])
        assert result.exit_code != 0
        assert "Invalid value" in result.output or "Choice" in result.output
    
    def test_valid_kind_options(self):
        """Test that valid kind options are accepted."""
        runner = CliRunner()
        
        # Test each valid kind option (should fail due to mocking, but argument parsing should succeed)
        valid_kinds = ['disk', 'compute', 'tape', 'fileserver']
        
        for kind in valid_kinds:
            result = runner.invoke(location, [
                'create', 'test-loc',
                '--protocol', 'file',
                '--kind', kind
            ])
            # Should not fail on argument parsing (will fail on service call due to no mocking)
            assert "Invalid value" not in result.output
            assert "Choice" not in result.output


# Additional focused unit tests for specific CLI behaviors

class TestLocationCLIErrorScenarios:
    """Test CLI error handling scenarios."""
    
    @patch('tellus.interfaces.cli.location._get_location_service')
    def test_service_timeout_handling(self, mock_get_service):
        """Test CLI handling of service timeouts."""
        mock_service = Mock(spec=LocationApplicationService)
        mock_get_service.return_value = mock_service
        mock_service.list_locations.side_effect = TimeoutError("Service timeout")
        
        runner = CliRunner()
        result = runner.invoke(location, ['list'])
        
        assert result.exit_code == 0  # CLI should handle gracefully
        assert "Error:" in result.output
    
    @patch('tellus.interfaces.cli.location._get_location_service')
    def test_permission_error_handling(self, mock_get_service):
        """Test CLI handling of permission errors."""
        mock_service = Mock(spec=LocationApplicationService)
        mock_get_service.return_value = mock_service
        mock_service.get_location.side_effect = PermissionError("Access denied")
        
        runner = CliRunner()
        result = runner.invoke(location, ['show', 'restricted-loc'])
        
        assert result.exit_code == 0  # CLI should handle gracefully
        assert "Error:" in result.output


class TestLocationCLIRichFormatting:
    """Test CLI rich output formatting behaviors."""
    
    @patch('tellus.interfaces.cli.location._get_location_service')
    def test_table_formatting_with_various_data(self, mock_get_service):
        """Test that table formatting handles various data types correctly."""
        mock_service = Mock(spec=LocationApplicationService)
        mock_get_service.return_value = mock_service
        
        # Create locations with various combinations of data
        locations_with_varied_data = [
            LocationDto(
                name="simple",
                kinds=[],
                protocol=None,
                path=None,
                storage_options={},
                optional=True,
                additional_config={}
            ),
            LocationDto(
                name="complex",
                kinds=["COMPUTE", "DISK", "TAPE"],
                protocol="multi-protocol",
                path="/very/long/path/that/might/wrap",
                storage_options={"host": "very-long-hostname.domain.com", "port": 22},
                optional=False,
                additional_config={"setting1": "value1", "setting2": "value2"}
            )
        ]
        
        list_result = LocationListDto(
            locations=locations_with_varied_data,
            pagination=PaginationInfo(),
            filters_applied=FilterOptions()
        )
        mock_service.list_locations.return_value = list_result
        
        runner = CliRunner()
        result = runner.invoke(location, ['list'])
        
        assert result.exit_code == 0
        # Verify table formatting handles empty/None values with "-"
        assert "-" in result.output
        # Verify complex data is displayed
        assert "compute, disk, tape" in result.output.lower()
        # Hostname might be truncated in table display
        assert "very-long-hostname.dom" in result.output or "very-long-hostname.domain.com" in result.output
    
    @patch('tellus.interfaces.cli.location._get_location_service')
    def test_panel_formatting_in_show_command(self, mock_get_service):
        """Test that show command uses proper panel formatting."""
        mock_service = Mock(spec=LocationApplicationService)
        mock_get_service.return_value = mock_service
        
        detailed_location = LocationDto(
            name="detailed-loc",
            kinds=["COMPUTE"],
            protocol="ssh",
            path="/detailed/path",
            storage_options={
                "host": "detailed.domain.com",
                "port": 2222,
                "username": "testuser"
            },
            optional=False,
            additional_config={
                "max_jobs": 50,
                "walltime": "12:00:00",
                "partition": "gpu"
            }
        )
        mock_service.get_location.return_value = detailed_location
        
        runner = CliRunner()
        result = runner.invoke(location, ['show', 'detailed-loc'])
        
        assert result.exit_code == 0
        # Verify panel formatting with rich tables
        assert "detailed-loc" in result.output
        assert "Storage." in result.output
        assert "Config." in result.output
        # Verify all storage options are displayed with Storage. prefix
        assert "Storage.host" in result.output
        assert "Storage.port" in result.output
        assert "Storage.username" in result.output
        # Verify all config options are displayed with Config. prefix  
        assert "Config.max_jobs" in result.output
        assert "Config.walltime" in result.output
        assert "Config.partition" in result.output