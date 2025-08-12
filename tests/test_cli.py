import json
from unittest.mock import patch, MagicMock

import pytest
from click.testing import CliRunner

from tellus.simulation import Simulation

# Import the CLI app
import sys
from pathlib import Path

from tellus.location.cli import location
from tellus.core.cli import cli


class TestCLI:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.runner = CliRunner()
        # Patch the simulations dictionary to isolate tests
        self.simulations_patch = patch("tellus.cli.simulations", {})
        self.mock_simulations = self.simulations_patch.start()
        
        # Also patch the Simulation class registry to isolate tests
        self.class_simulations_patch = patch("tellus.simulation.Simulation._simulations", {})
        self.mock_class_simulations = self.class_simulations_patch.start()
        
        # Also patch the Location class registry to isolate tests
        self.locations_patch = patch("tellus.location.Location._locations", {})
        self.mock_locations = self.locations_patch.start()
        
        yield
        self.simulations_patch.stop()
        self.class_simulations_patch.stop()
        self.locations_patch.stop()

    def test_create_simulation(self):
        # Test creating a simulation with a custom ID
        result = self.runner.invoke(cli, ["simulation", "create", "test-sim"])
        assert result.exit_code == 0
        assert "Created simulation with ID 'test-sim'" in result.output
        assert "test-sim" in self.mock_simulations
        assert isinstance(self.mock_simulations["test-sim"], Simulation)

    def test_create_simulation_auto_id(self):
        # Test creating a simulation with auto-generated ID
        result = self.runner.invoke(cli, ["simulation", "create"])
        assert result.exit_code == 0
        assert "Created simulation with ID '" in result.output
        assert len(self.mock_simulations) == 1
        sim_id = list(self.mock_simulations.keys())[0]
        assert len(sim_id) > 0  # Should have some ID

    def test_create_duplicate_simulation(self):
        # Test creating a duplicate simulation
        self.runner.invoke(cli, ["simulation", "create", "test-sim"])
        result = self.runner.invoke(cli, ["simulation", "create", "test-sim"])
        assert result.exit_code == 0
        assert "already exists" in result.output
        assert len(self.mock_simulations) == 1

    def test_show_simulation(self):
        # Test showing simulation details
        self.runner.invoke(cli, ["simulation", "create", "test-sim"])
        result = self.runner.invoke(cli, ["simulation", "show", "test-sim"])
        assert result.exit_code == 0
        assert "test-sim" in result.output
        assert "Locations" in result.output

    def test_show_nonexistent_simulation(self):
        # Test showing a non-existent simulation
        result = self.runner.invoke(cli, ["simulation", "show", "nonexistent"])
        assert result.exit_code != 0
        assert "not found" in result.output

    def test_remove_simulation(self):
        # Test removing a simulation
        self.runner.invoke(cli, ["simulation", "create", "test-sim"])
        result = self.runner.invoke(
            cli,
            ["simulation", "remove-simulation", "test-sim"],
            input="y\n",  # Confirm deletion
        )
        assert result.exit_code == 0
        assert "Removed simulation with ID" in result.output
        assert "test-sim" not in self.mock_simulations

    def test_add_location(self):
        # Test adding a location to a simulation
        self.runner.invoke(cli, ["simulation", "create", "test-sim"])
        result = self.runner.invoke(
            cli,
            [
                "location",
                "add",
                "test-sim",
                "test-loc",
                "--kind",
                "fileserver",
                "--config",
                '{"path":"/tmp"}',
            ],
        )
        assert result.exit_code == 0
        assert "Added location 'test-loc'" in result.output

        # Verify the location was added
        result = self.runner.invoke(cli, ["location", "list", "test-sim"])
        assert "test-loc" in result.output

    def test_post_and_fetch_data(self):
        # Test posting and fetching data from a location
        # Setup
        self.runner.invoke(cli, ["simulation", "create", "test-sim"])
        self.runner.invoke(
            cli,
            [
                "location",
                "add",
                "test-sim",
                "test-loc",
                "--kind",
                "fileserver",  # Using fileserver for testing
            ],
        )

        # Post data
        test_data = '{"key": "value"}'
        result = self.runner.invoke(
            cli, ["location", "post", "test-sim", "test-loc", test_data]
        )
        assert result.exit_code == 0
        assert "Posted data to location 'test-loc'" in result.output

        # Fetch data (note: this is a simple test, actual fetch would need proper mocking)
        result = self.runner.invoke(
            cli, ["location", "fetch", "test-sim", "test-loc", "some-id"]
        )
        # Just verify the command runs, actual data testing would need proper mocking
        assert result.exit_code == 0

    def test_remove_location(self):
        # Test removing a location
        self.runner.invoke(cli, ["simulation", "create", "test-sim"])
        self.runner.invoke(
            cli, ["location", "add", "test-sim", "test-loc", "--kind", "fileserver"]
        )

        # Remove the location
        result = self.runner.invoke(
            cli,
            ["location", "remove", "test-sim", "test-loc"],
            input="y\n",  # Confirm deletion
        )
        assert result.exit_code == 0
        assert "Removed location 'test-loc'" in result.output

        # Verify the location was removed
        result = self.runner.invoke(cli, ["location", "list", "test-sim"])
        assert "No locations found" in result.output

    def test_location_list_command(self):
        """Test the standalone location list command."""
        from tellus.location.location import Location, LocationKind
        
        # Create a test location
        with patch("tellus.location.location.Location.load_locations"):
            with patch("tellus.location.location.Location.list_locations") as mock_list:
                # Create a mock location that might cause errors
                mock_loc = MagicMock()
                mock_loc.name = "test-loc"
                mock_loc.kinds = [LocationKind.FILESERVER]
                mock_loc.config = {
                    "protocol": "sftp",
                    "storage_options": {"hostname": "test.example.com", "username": "testuser"}
                }
                mock_loc.fs.to_json.side_effect = Exception("SSH connection failed")
                
                mock_list.return_value = [mock_loc]
                
                # Test normal mode (should show error)
                result = self.runner.invoke(cli, ["location", "list"])
                assert result.exit_code == 0
                assert "SSH connection failed" in result.output
                
    def test_location_list_fixed_flag(self):
        """Test the location list command with --fixed flag."""
        from tellus.location.location import Location, LocationKind
        
        # Create a test location
        with patch("tellus.location.location.Location.load_locations"):
            with patch("tellus.location.location.Location.list_locations") as mock_list:
                # Create a mock location
                mock_loc = MagicMock()
                mock_loc.name = "test-loc"
                mock_loc.kinds = [LocationKind.FILESERVER]
                mock_loc.config = {
                    "protocol": "file",
                    "storage_options": {"path": "/tmp/test"}
                }
                
                mock_list.return_value = [mock_loc]
                
                # Test fixed mode (should show improved representation)
                result = self.runner.invoke(cli, ["location", "list", "--fixed"])
                assert result.exit_code == 0
                assert "Local filesystem" in result.output
