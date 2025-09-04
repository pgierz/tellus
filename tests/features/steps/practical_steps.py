"""
Step implementations for practical usage scenarios.
"""
import json
import os
from pathlib import Path
from behave import given, when, then


@given('I have a clean tellus project')
def step_clean_tellus_project(context):
    """Set up a clean tellus project environment."""
    # This is already handled by the cli_steps clean working directory
    pass


@given('I have initialized tellus')
def step_initialized_tellus(context):
    """Ensure tellus is initialized."""
    if not hasattr(context, 'initialized'):
        # Try to initialize
        import subprocess
        result = subprocess.run(
            ['python', '-m', 'tellus', 'init', '--force'],
            capture_output=True,
            text=True,
            cwd=context.temp_dir
        )
        context.initialized = (result.returncode == 0)


@given('I have created a location')  
def step_created_location(context):
    """Ensure a test location exists."""
    if not hasattr(context, 'location_created'):
        import subprocess
        result = subprocess.run(
            ['python', '-m', 'tellus', 'location', 'create', '/tmp/test_location'],
            capture_output=True,
            text=True,
            cwd=context.temp_dir
        )
        context.location_created = (result.returncode == 0)


@given('I have a simulation with FESOM data')
def step_simulation_with_fesom_data(context):
    """Set up a simulation with FESOM data for testing."""
    # This would require actual test data setup
    # For now, just mark as pending
    context.scenario.skip(reason="Requires FESOM test data setup")


@then('I should have a .tellus directory')
def step_should_have_tellus_directory(context):
    """Check that .tellus directory was created."""
    tellus_dir = context.temp_dir / '.tellus'
    assert tellus_dir.exists(), f"Expected .tellus directory at {tellus_dir}"


@then('if successful, the output should be valid JSON')
def step_output_should_be_valid_json(context):
    """Check that output is valid JSON if command succeeded."""
    if context.exit_code == 0 and context.stdout.strip():
        try:
            json.loads(context.stdout)
        except json.JSONDecodeError:
            assert False, f"Expected valid JSON output, got: {context.stdout}"


@then('I should see temperature files')
def step_should_see_temperature_files(context):
    """Check for temperature files in output."""
    if context.exit_code == 0:
        # Look for indicators of temperature data
        temp_indicators = ['temp', 'temperature', 'T.', '_T_']
        found = any(indicator in context.output.lower() for indicator in temp_indicators)
        assert found, f"Expected temperature file indicators in: {context.output}"


@then('I should see salinity files')  
def step_should_see_salinity_files(context):
    """Check for salinity files in output."""
    if context.exit_code == 0:
        # Look for indicators of salinity data
        salt_indicators = ['salt', 'salinity', 'S.', '_S_']
        found = any(indicator in context.output.lower() for indicator in salt_indicators)
        assert found, f"Expected salinity file indicators in: {context.output}"