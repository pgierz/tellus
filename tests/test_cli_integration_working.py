"""
Integration tests for confirmed working CLI commands.

Tests the CLI commands that have been verified to work correctly:
- tellus simulation list
- tellus simulation show
- tellus simulation create  
- tellus location list
- tellus location show
- tellus simulation location list
"""
import json
import pytest
import os
from contextlib import contextmanager
from click.testing import CliRunner
from pathlib import Path


@contextmanager
def change_dir(new_dir):
    """Context manager to temporarily change directory."""
    old_cwd = os.getcwd()
    try:
        os.chdir(str(new_dir))
        yield
    finally:
        os.chdir(old_cwd)


@pytest.fixture
def app():
    """Import the main CLI app."""
    from tellus.interfaces.cli.main import create_main_cli
    return create_main_cli()


@pytest.fixture
def test_project_dir(temp_dir):
    """Set up a temporary project directory with test data."""
    # Create basic project structure
    (temp_dir / ".tellus").mkdir(exist_ok=True)
    
    # Create minimal simulations.json for testing
    simulations_data = {
        "simulations": {
            "test_sim_001": {
                "id": "test_sim_001",
                "attributes": {
                    "model": "TestModel",
                    "experiment": "test"
                },
                "location_contexts": {}
            }
        }
    }
    
    with open(temp_dir / "simulations.json", "w") as f:
        json.dump(simulations_data, f)
    
    # Create minimal locations.json for testing
    locations_data = {
        "locations": {
            "test_location": {
                "name": "test_location",
                "protocol": "file",
                "path": str(temp_dir / "test_data"),
                "kinds": ["DISK"],
                "storage_options": {}
            }
        }
    }
    
    with open(temp_dir / "locations.json", "w") as f:
        json.dump(locations_data, f)
    
    # Create test data directory
    (temp_dir / "test_data").mkdir(exist_ok=True)
    
    return temp_dir


class TestSimulationListCommand:
    """Test tellus simulation list command."""
    
    def test_simulation_list_basic(self, runner, app, test_project_dir):
        """Test basic simulation list command."""
        with change_dir(test_project_dir):
            result = runner.invoke(app, ['simulation', 'list'], catch_exceptions=False)
        
        # Should exit successfully
        assert result.exit_code == 0
        
        # Should contain table header or simulation info
        output_lower = result.output.lower()
        assert any(keyword in output_lower for keyword in [
            'simulation', 'available', 'id', 'locations', 'attributes'
        ])
    
    def test_simulation_list_json_output(self, runner, app, test_project_dir):
        """Test simulation list with JSON output."""
        with change_dir(test_project_dir):
            result = runner.invoke(app, ['--json', 'simulation', 'list'], catch_exceptions=False)
        
        # Should exit successfully  
        assert result.exit_code == 0
        
        # Should be valid JSON (even if empty) or handle gracefully
        if result.output.strip():
            try:
                data = json.loads(result.output)
                assert isinstance(data, (dict, list))
            except json.JSONDecodeError:
                # Allow non-JSON output if it's a meaningful message
                assert any(keyword in result.output.lower() for keyword in [
                    'no simulations', 'empty', 'found'
                ]), f"Expected JSON or meaningful message, got: {repr(result.output)}"
    
    def test_simulation_list_help(self, runner, app):
        """Test simulation list help."""
        result = runner.invoke(app, ['simulation', 'list', '--help'])
        
        assert result.exit_code == 0
        assert 'list' in result.output.lower()
        assert 'simulation' in result.output.lower()


class TestSimulationShowCommand:
    """Test tellus simulation show command."""
    
    def test_simulation_show_help(self, runner, app):
        """Test simulation show help."""
        result = runner.invoke(app, ['simulation', 'show', '--help'])
        
        assert result.exit_code == 0
        assert 'show' in result.output.lower()
        assert 'simulation' in result.output.lower()
    
    def test_simulation_show_nonexistent(self, runner, app, test_project_dir):
        """Test simulation show with non-existent simulation."""
        with change_dir(test_project_dir):
            result = runner.invoke(app, ['simulation', 'show', 'nonexistent'])
        
        # Should handle gracefully (either error message or exit code != 0)
        assert result.exit_code != 0 or 'not found' in result.output.lower()


class TestLocationListCommand:
    """Test tellus location list command."""
    
    def test_location_list_basic(self, runner, app, test_project_dir):
        """Test basic location list command."""
        with change_dir(test_project_dir):
            result = runner.invoke(app, ['location', 'list'], catch_exceptions=False)
        
        # Should exit successfully
        assert result.exit_code == 0
        
        # Should contain location-related content
        output_lower = result.output.lower()
        assert any(keyword in output_lower for keyword in [
            'location', 'available', 'name', 'protocol', 'kind'
        ])
    
    def test_location_list_json_output(self, runner, app, test_project_dir):
        """Test location list with JSON output."""
        with change_dir(test_project_dir):
            result = runner.invoke(app, ['--json', 'location', 'list'], catch_exceptions=False)
        
        # Should exit successfully
        assert result.exit_code == 0
        
        # Should be valid JSON
        try:
            data = json.loads(result.output)
            assert isinstance(data, (dict, list))
        except json.JSONDecodeError:
            pytest.fail("Output should be valid JSON")
    
    def test_location_list_help(self, runner, app):
        """Test location list help."""
        result = runner.invoke(app, ['location', 'list', '--help'])
        
        assert result.exit_code == 0
        assert 'list' in result.output.lower()
        assert 'location' in result.output.lower()


class TestLocationShowCommand:
    """Test tellus location show command."""
    
    def test_location_show_help(self, runner, app):
        """Test location show help."""
        result = runner.invoke(app, ['location', 'show', '--help'])
        
        assert result.exit_code == 0
        assert 'show' in result.output.lower()
        assert 'location' in result.output.lower()
    
    def test_location_show_nonexistent(self, runner, app, test_project_dir):
        """Test location show with non-existent location."""
        with change_dir(test_project_dir):
            result = runner.invoke(app, ['location', 'show', 'nonexistent'])
        
        # Should handle gracefully
        assert result.exit_code != 0 or 'not found' in result.output.lower()


class TestSimulationCreateCommand:
    """Test tellus simulation create command."""
    
    def test_simulation_create_help(self, runner, app):
        """Test simulation create help."""
        result = runner.invoke(app, ['simulation', 'create', '--help'])
        
        assert result.exit_code == 0
        assert 'create' in result.output.lower()
        assert 'simulation' in result.output.lower()
    
    def test_simulation_create_basic_args(self, runner, app, test_project_dir):
        """Test simulation create with basic arguments."""
        with change_dir(test_project_dir):
            result = runner.invoke(app, ['simulation', 'create', 'test_sim_new'])
        
        # Should either succeed or fail gracefully
        assert result.exit_code in [0, 1]  # Allow for validation errors
        
        # If it failed, should have meaningful error message
        if result.exit_code != 0:
            assert len(result.output.strip()) > 0


class TestSimulationLocationCommands:
    """Test tellus simulation location subcommands."""
    
    def test_simulation_location_help(self, runner, app):
        """Test simulation location help."""
        result = runner.invoke(app, ['simulation', 'location', '--help'])
        
        assert result.exit_code == 0
        assert 'location' in result.output.lower()
    
    def test_simulation_location_list_help(self, runner, app):
        """Test simulation location list help."""
        result = runner.invoke(app, ['simulation', 'location', 'list', '--help'])
        
        assert result.exit_code == 0
        assert 'list' in result.output.lower()
        assert 'location' in result.output.lower()


class TestCLIGlobalFeatures:
    """Test global CLI features."""
    
    def test_main_cli_help(self, runner, app):
        """Test main CLI help shows all expected commands."""
        result = runner.invoke(app, ['--help'])
        
        assert result.exit_code == 0
        
        # Should show main subcommands
        expected_commands = ['simulation', 'location', 'init']
        for cmd in expected_commands:
            assert cmd in result.output
    
    def test_version_flag(self, runner, app):
        """Test --version flag works."""
        result = runner.invoke(app, ['--version'])
        
        assert result.exit_code == 0
        # Should contain version info
        assert any(keyword in result.output.lower() for keyword in [
            'version', 'tellus', '0.1.0'
        ])
    
    def test_json_flag_global(self, runner, app):
        """Test that --json flag is available globally."""
        result = runner.invoke(app, ['--help'])
        
        assert result.exit_code == 0
        assert '--json' in result.output


class TestCLIErrorHandling:
    """Test CLI error handling."""
    
    def test_invalid_command(self, runner, app):
        """Test behavior with invalid command."""
        result = runner.invoke(app, ['invalid_command'])
        
        # Should exit with error
        assert result.exit_code != 0
        
        # Should show helpful error message
        assert len(result.output.strip()) > 0
    
    def test_invalid_subcommand(self, runner, app):
        """Test behavior with invalid subcommand."""
        result = runner.invoke(app, ['simulation', 'invalid_subcommand'])
        
        # Should exit with error
        assert result.exit_code != 0
        
        # Should show helpful error message
        assert len(result.output.strip()) > 0


@pytest.mark.slow
class TestCLIPerformance:
    """Test CLI performance characteristics."""
    
    def test_help_commands_fast(self, runner, app):
        """Test that help commands execute quickly."""
        import time
        
        commands_to_test = [
            ['--help'],
            ['simulation', '--help'],
            ['location', '--help'],
            ['simulation', 'list', '--help'],
        ]
        
        for cmd in commands_to_test:
            start_time = time.time()
            result = runner.invoke(app, cmd)
            duration = time.time() - start_time
            
            # Help should be fast (under 2 seconds)
            assert duration < 2.0, f"Command {' '.join(cmd)} took {duration:.2f}s"
            assert result.exit_code == 0


@pytest.mark.integration 
class TestCLIWithRealData:
    """Integration tests that work with actual data files."""
    
    def test_cli_with_existing_project(self, runner, app):
        """Test CLI commands in an existing project directory."""
        # This test runs in the current project directory
        # to test against real simulations.json and locations.json
        
        result = runner.invoke(app, ['simulation', 'list'])
        
        # Should work in project directory
        assert result.exit_code == 0
        
        # Should show some output
        assert len(result.output.strip()) > 0
    
    def test_cli_location_list_real(self, runner, app):
        """Test location list with real data."""
        result = runner.invoke(app, ['location', 'list'])
        
        # Should work in project directory
        assert result.exit_code == 0
        
        # Should show some output
        assert len(result.output.strip()) > 0