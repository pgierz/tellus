"""
Unit tests for simulation CLI commands.

These tests verify the simulation management CLI interface layer,
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

from tellus.application.dtos import (CreateSimulationDto, FilterOptions,
                                     PaginationInfo, SimulationDto,
                                     SimulationListDto,
                                     SimulationLocationAssociationDto,
                                     UpdateSimulationDto)
from tellus.application.exceptions import (EntityAlreadyExistsError,
                                           EntityNotFoundError,
                                           ValidationError)
from tellus.application.services.simulation_service import \
    SimulationApplicationService
from tellus.domain.entities.simulation import SimulationEntity
from tellus.interfaces.cli.simulation import simulation


class TestSimulationListCommand:
    """Test cases for simulation list command."""
    
    @pytest.fixture
    def mock_service(self):
        """Mock simulation application service."""
        service = Mock(spec=SimulationApplicationService)
        return service
    
    @pytest.fixture
    def sample_simulations(self):
        """Sample simulation entities for testing."""
        return [
            SimulationEntity(
                simulation_id="test-sim-1",
                model_id="test-model",
                path="/path/to/sim1",
                attrs={"experiment": "Test Experiment 1"},
                associated_locations={"localhost", "hpc-cluster"},
                location_contexts={"localhost": {}, "hpc-cluster": {}}
            ),
            SimulationEntity(
                simulation_id="test-sim-2", 
                model_id="another-model",
                path="/path/to/sim2",
                attrs={"experiment": "Test Experiment 2"},
                associated_locations={"cloud-storage"},
                location_contexts={"cloud-storage": {}}
            )
        ]
    
    @patch('tellus.interfaces.cli.simulation._get_simulation_service')
    def test_list_all_simulations_success(self, mock_get_service, mock_service, sample_simulations):
        """Test successful listing of all simulations."""
        mock_get_service.return_value = mock_service
        list_result = SimulationListDto(
            simulations=sample_simulations,
            pagination=PaginationInfo(),
            filters_applied=FilterOptions()
        )
        mock_service.list_simulations.return_value = list_result
        
        runner = CliRunner()
        result = runner.invoke(simulation, ['list'])
        
        assert result.exit_code == 0
        assert "test-sim-1" in result.output
        assert "test-sim-2" in result.output
        mock_service.list_simulations.assert_called_once()
    
    @patch('tellus.interfaces.cli.simulation._get_simulation_service')
    def test_list_simulations_empty(self, mock_get_service, mock_service):
        """Test listing when no simulations exist."""
        mock_get_service.return_value = mock_service
        empty_result = SimulationListDto(
            simulations=[],
            pagination=PaginationInfo(),
            filters_applied=FilterOptions()
        )
        mock_service.list_simulations.return_value = empty_result
        
        runner = CliRunner()
        result = runner.invoke(simulation, ['list'])
        
        assert result.exit_code == 0
        assert "No simulations found" in result.output
        mock_service.list_simulations.assert_called_once()
    
    @patch('tellus.interfaces.cli.simulation._get_simulation_service')
    def test_list_simulations_service_error(self, mock_get_service, mock_service):
        """Test handling service errors during listing."""
        mock_get_service.return_value = mock_service
        mock_service.list_simulations.side_effect = Exception("Service error")
        
        runner = CliRunner()
        result = runner.invoke(simulation, ['list'])
        
        assert result.exit_code == 0  # CLI handles errors gracefully
        assert "Error:" in result.output


class TestSimulationShowCommand:
    """Test cases for simulation show command."""
    
    @pytest.fixture
    def mock_service(self):
        """Mock simulation application service."""
        service = Mock(spec=SimulationApplicationService)
        return service
    
    @pytest.fixture
    def sample_simulation(self):
        """Sample simulation entity for testing."""
        return SimulationEntity(
            simulation_id="test-sim-1",
            model_id="test-model",
            path="/path/to/test/sim",
            attrs={"experiment": "Test Experiment", "version": "1.0"},
            associated_locations={"localhost", "hpc-cluster"},
            location_contexts={"localhost": {}, "hpc-cluster": {}}
        )
    
    @patch('tellus.interfaces.cli.simulation._get_simulation_service')
    def test_show_simulation_success(self, mock_get_service, mock_service, sample_simulation):
        """Test successful display of simulation details."""
        mock_get_service.return_value = mock_service
        mock_service.get_simulation.return_value = sample_simulation
        
        runner = CliRunner()
        result = runner.invoke(simulation, ['show', 'test-sim-1'])
        
        assert result.exit_code == 0
        assert "test-sim-1" in result.output
        assert "test-model" in result.output
        assert "/path/to/test/sim" in result.output
        mock_service.get_simulation.assert_called_once_with("test-sim-1")
    
    @patch('tellus.interfaces.cli.simulation._get_simulation_service')
    def test_show_simulation_not_found(self, mock_get_service, mock_service):
        """Test showing non-existent simulation."""
        mock_get_service.return_value = mock_service
        mock_service.get_simulation.side_effect = EntityNotFoundError("Simulation", "nonexistent")
        
        runner = CliRunner()
        result = runner.invoke(simulation, ['show', 'nonexistent'])
        
        assert result.exit_code == 0  # CLI handles errors gracefully
        assert "Error:" in result.output
        mock_service.get_simulation.assert_called_once_with("nonexistent")


class TestSimulationCreateCommand:
    """Test cases for simulation create command."""
    
    @pytest.fixture
    def mock_service(self):
        """Mock simulation application service."""
        service = Mock(spec=SimulationApplicationService)
        return service
    
    @pytest.fixture
    def created_simulation(self):
        """Sample created simulation entity."""
        return SimulationEntity(
            simulation_id="new-sim",
            model_id="test-model",
            path="/path/to/new/sim",
            attrs={}
        )
    
    @patch('tellus.interfaces.cli.simulation._get_simulation_service')
    def test_create_simulation_success(self, mock_get_service, mock_service, created_simulation):
        """Test successful simulation creation."""
        mock_get_service.return_value = mock_service
        mock_service.create_simulation.return_value = self._entity_to_dto(created_simulation)
        
        runner = CliRunner()
        result = runner.invoke(simulation, [
            'create', 'new-sim', 
            '--model-id', 'test-model',
            '--path', '/path/to/new/sim'
        ])
        
        assert result.exit_code == 0
        assert "Created simulation: new-sim" in result.output
        
        # Verify correct DTO was passed
        mock_service.create_simulation.assert_called_once()
        call_args = mock_service.create_simulation.call_args[0][0]
        assert isinstance(call_args, CreateSimulationDto)
        assert call_args.simulation_id == "new-sim"
        assert call_args.model_id == "test-model"
        assert call_args.path == "/path/to/new/sim"
    
    @patch('tellus.interfaces.cli.simulation._get_simulation_service')
    def test_create_simulation_minimal_params(self, mock_get_service, mock_service):
        """Test simulation creation with minimal parameters."""
        mock_get_service.return_value = mock_service
        minimal_sim = SimulationEntity(
            simulation_id="minimal-sim",
            model_id=None,
            path=None,
            attrs={}
        )
        mock_service.create_simulation.return_value = self._entity_to_dto(minimal_sim)
        
        runner = CliRunner()
        result = runner.invoke(simulation, ['create', 'minimal-sim'])
        
        assert result.exit_code == 0
        assert "Created simulation: minimal-sim" in result.output
        
        # Verify correct DTO was passed
        call_args = mock_service.create_simulation.call_args[0][0]
        assert call_args.simulation_id == "minimal-sim"
        assert call_args.model_id is None
        assert call_args.path is None
    
    @patch('tellus.interfaces.cli.simulation._get_simulation_service')
    def test_create_simulation_already_exists(self, mock_get_service, mock_service):
        """Test creating simulation that already exists."""
        mock_get_service.return_value = mock_service
        mock_service.create_simulation.side_effect = EntityAlreadyExistsError(
            "Simulation", "existing-sim"
        )
        
        runner = CliRunner()
        result = runner.invoke(simulation, ['create', 'existing-sim'])
        
        assert result.exit_code == 0  # CLI handles errors gracefully
        assert "Error:" in result.output
    
    @patch('tellus.interfaces.cli.simulation._get_simulation_service')
    def test_create_simulation_validation_error(self, mock_get_service, mock_service):
        """Test simulation creation with validation errors."""
        mock_get_service.return_value = mock_service
        mock_service.create_simulation.side_effect = ValidationError("Invalid simulation_id")
        
        runner = CliRunner()
        result = runner.invoke(simulation, ['create', ''])
        
        assert result.exit_code == 0  # CLI handles errors gracefully
        assert "Error:" in result.output
    
    def _entity_to_dto(self, entity: SimulationEntity) -> SimulationDto:
        """Helper method to convert entity to DTO."""
        return SimulationDto(
            simulation_id=entity.simulation_id,
            uid=entity._uid,
            model_id=entity.model_id,
            path=entity.path,
            attrs=entity.attrs.copy(),
            namelists=entity.namelists.copy(),
            snakemakes=entity.snakemakes.copy(),
            contexts={
                "LocationContext": entity.location_contexts.copy()
            } if entity.location_contexts else {}
        )


class TestSimulationCLIIntegration:
    """Integration tests for simulation CLI commands."""
    
    @patch('tellus.interfaces.cli.simulation._get_simulation_service')
    def test_cli_error_handling_service_unavailable(self, mock_get_service):
        """Test CLI behavior when service is unavailable."""
        mock_get_service.side_effect = Exception("Service initialization failed")
        
        runner = CliRunner()
        result = runner.invoke(simulation, ['list'])
        
        assert result.exit_code == 0  # CLI handles errors gracefully
        assert "Error:" in result.output
    
    def test_cli_help_commands(self):
        """Test help output for CLI commands."""
        runner = CliRunner()
        
        # Test main help
        result = runner.invoke(simulation, ['--help'])
        assert result.exit_code == 0
        assert "simulation" in result.output.lower()
        
        # Test subcommand help
        result = runner.invoke(simulation, ['create', '--help'])
        assert result.exit_code == 0
        assert "create" in result.output.lower()
        assert "sim_id" in result.output.lower() or "sim-id" in result.output.lower()
    
    @patch('tellus.interfaces.cli.simulation._get_simulation_service')
    def test_cli_output_formatting_consistency(self, mock_get_service):
        """Test that CLI output formatting is consistent across commands."""
        mock_service = Mock(spec=SimulationApplicationService)
        mock_get_service.return_value = mock_service
        
        # Test that all commands handle rich formatting consistently
        sample_simulation = SimulationEntity(
            simulation_id="test-sim",
            model_id="test-model",
            path="/test/path",
            attrs={}
        )
        
        list_result = SimulationListDto(
            simulations=[sample_simulation],
            pagination=PaginationInfo(),
            filters_applied=FilterOptions()
        )
        mock_service.list_simulations.return_value = list_result
        mock_service.get_simulation.return_value = sample_simulation
        
        runner = CliRunner()
        
        # All commands should succeed and use consistent formatting
        commands_to_test = [
            ['list'],
            ['show', 'test-sim'],
        ]
        
        for cmd in commands_to_test:
            result = runner.invoke(simulation, cmd)
            assert result.exit_code == 0
            # Verify no malformed output or exceptions in formatting
            assert "Traceback" not in result.output


class TestSimulationCLIArgumentParsing:
    """Test CLI argument parsing and validation."""
    
    def test_create_command_argument_parsing(self):
        """Test that create command properly parses arguments."""
        runner = CliRunner()
        
        # Test missing required argument
        result = runner.invoke(simulation, ['create'])
        assert result.exit_code != 0
        assert "Missing argument" in result.output or "Usage:" in result.output
    
    def test_show_command_argument_parsing(self):
        """Test that show command properly parses arguments."""
        runner = CliRunner()
        
        # Test missing required argument  
        result = runner.invoke(simulation, ['show'])
        assert result.exit_code != 0
        assert "Missing argument" in result.output or "Usage:" in result.output
    
    def test_invalid_command(self):
        """Test handling of invalid subcommands."""
        runner = CliRunner()
        result = runner.invoke(simulation, ['invalid-command'])
        assert result.exit_code != 0
        assert "No such command" in result.output


# Additional focused unit tests for specific CLI behaviors

class TestSimulationCLIErrorScenarios:
    """Test CLI error handling scenarios."""
    
    @patch('tellus.interfaces.cli.simulation._get_simulation_service')
    def test_service_timeout_handling(self, mock_get_service):
        """Test CLI handling of service timeouts."""
        mock_service = Mock(spec=SimulationApplicationService)
        mock_get_service.return_value = mock_service
        mock_service.list_simulations.side_effect = TimeoutError("Service timeout")
        
        runner = CliRunner()
        result = runner.invoke(simulation, ['list'])
        
        assert result.exit_code == 0  # CLI should handle gracefully
        assert "Error:" in result.output
    
    @patch('tellus.interfaces.cli.simulation._get_simulation_service')
    def test_permission_error_handling(self, mock_get_service):
        """Test CLI handling of permission errors."""
        mock_service = Mock(spec=SimulationApplicationService)
        mock_get_service.return_value = mock_service
        mock_service.get_simulation.side_effect = PermissionError("Access denied")
        
        runner = CliRunner()
        result = runner.invoke(simulation, ['show', 'restricted-sim'])
        
        assert result.exit_code == 0  # CLI should handle gracefully
        assert "Error:" in result.output