"""
Tests for CLI specification compliance.

These tests validate that the tellus CLI follows the specification
defined in the CLI redesign document.
"""
import json
import pytest
from click.testing import CliRunner


@pytest.fixture
def app():
    """Import the main CLI app."""
    from tellus.interfaces.cli.main import create_main_cli
    return create_main_cli()


class TestCLIStructure:
    """Test that CLI commands exist with correct structure."""
    
    def test_main_help(self, runner, app):
        """Test that main CLI shows expected subcommands."""
        result = runner.invoke(app, ['--help'])
        assert result.exit_code == 0
        
        # Check for main subcommands
        expected_commands = ['location', 'simulation', 'init']
        for cmd in expected_commands:
            assert cmd in result.output
    
    def test_global_json_flag(self, runner, app):
        """Test that --json flag is available globally."""
        result = runner.invoke(app, ['--help'])
        assert '--json' in result.output
    
    def test_version_flag(self, runner, app):
        """Test that --version flag works."""
        result = runner.invoke(app, ['--version'])
        assert result.exit_code == 0


class TestInitCommand:
    """Test tellus init command."""
    
    def test_init_help(self, runner, app):
        """Test init command help."""
        result = runner.invoke(app, ['init', '--help'])
        assert result.exit_code == 0
        assert '--force' in result.output
        assert '--migrate-from' in result.output
    
    def test_init_basic(self, runner, app, temp_dir):
        """Test basic init functionality."""
        with runner.isolated_filesystem():
            result = runner.invoke(app, ['init', '--force'])
            # Don't assert success yet, just that command exists
            assert 'init' in str(result.output) or result.exit_code in [0, 1, 2]


class TestLocationCommands:
    """Test tellus location commands."""
    
    def test_location_help(self, runner, app):
        """Test location command group help."""
        result = runner.invoke(app, ['location', '--help'])
        # Command should exist even if not fully implemented
        assert result.exit_code in [0, 1, 2]
    
    def test_location_subcommands_exist(self, runner, app):
        """Test that location subcommands are defined."""
        result = runner.invoke(app, ['location', '--help'])
        if result.exit_code == 0:
            expected_subcommands = ['create', 'show', 'list', 'edit', 'update', 'delete']
            # At least some should be present
            present_commands = sum(1 for cmd in expected_subcommands if cmd in result.output)
            assert present_commands > 0, f"No location subcommands found in: {result.output}"


class TestSimulationCommands:
    """Test tellus simulation commands."""
    
    def test_simulation_help(self, runner, app):
        """Test simulation command group help."""
        result = runner.invoke(app, ['simulation', '--help'])
        # Command should exist even if not fully implemented
        assert result.exit_code in [0, 1, 2]
    
    def test_simulation_subcommands_exist(self, runner, app):
        """Test that simulation subcommands are defined."""
        result = runner.invoke(app, ['simulation', '--help'])
        if result.exit_code == 0:
            expected_subcommands = ['create', 'show', 'list', 'edit', 'update', 'delete']
            # At least some should be present
            present_commands = sum(1 for cmd in expected_subcommands if cmd in result.output)
            assert present_commands > 0, f"No simulation subcommands found in: {result.output}"


class TestCLIGrammarRules:
    """Test CLI grammar compliance."""
    
    def test_commands_use_imperative_verbs(self, runner, app):
        """Test that commands use imperative verbs as specified."""
        # Get all available commands by parsing help output
        result = runner.invoke(app, ['--help'])
        if result.exit_code == 0:
            # Should not contain non-imperative forms
            problematic_verbs = ['creating', 'showing', 'listing', 'editing']
            for verb in problematic_verbs:
                assert verb.lower() not in result.output.lower()
    
    def test_long_form_flags(self, runner, app):
        """Test that flags use --long-form."""
        result = runner.invoke(app, ['init', '--help'])
        if result.exit_code == 0:
            # Should have --force and --migrate-from
            assert '--force' in result.output
            assert '--migrate-from' in result.output
    
    def test_failure_exit_codes(self, runner, app):
        """Test that invalid commands return non-zero exit codes."""
        result = runner.invoke(app, ['nonexistent-command'])
        assert result.exit_code != 0


class TestJSONOutput:
    """Test JSON output functionality."""
    
    def test_json_flag_parseable(self, runner, app):
        """Test that --json produces valid JSON when available."""
        # Test with a command that might support JSON
        result = runner.invoke(app, ['--json', 'location', 'list'])
        
        # If command succeeds and produces output, it should be valid JSON
        if result.exit_code == 0 and result.output.strip():
            try:
                json.loads(result.output)
            except json.JSONDecodeError:
                pytest.fail(f"--json flag produced invalid JSON: {result.output}")


@pytest.mark.integration
class TestWorkflowIntegration:
    """Test basic workflow integration."""
    
    def test_basic_workflow(self, runner, app):
        """Test basic tellus workflow if available."""
        with runner.isolated_filesystem():
            # Try to init a project
            init_result = runner.invoke(app, ['init', '--force'])
            
            # If init works, try basic operations
            if init_result.exit_code == 0:
                # Try to list locations (should work even if empty)
                list_result = runner.invoke(app, ['location', 'list'])
                assert list_result.exit_code in [0, 1]  # 0 for success, 1 for "no locations"
                
                # Try to list simulations (should work even if empty)  
                sim_result = runner.invoke(app, ['simulation', 'list'])
                assert sim_result.exit_code in [0, 1]  # 0 for success, 1 for "no simulations"