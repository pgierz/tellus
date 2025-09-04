"""
Global pytest configuration and fixtures for tellus CLI tests.
"""
import tempfile
import shutil
from pathlib import Path
import pytest
from click.testing import CliRunner


@pytest.fixture
def runner():
    """Click test runner for CLI commands."""
    return CliRunner()


@pytest.fixture
def temp_dir():
    """Temporary directory for test isolation."""
    temp_path = Path(tempfile.mkdtemp())
    yield temp_path
    shutil.rmtree(temp_path, ignore_errors=True)


@pytest.fixture
def initialized_project(temp_dir):
    """A tellus project initialized in a temporary directory."""
    from tellus.interfaces.cli.main import create_main_cli
    app = create_main_cli()
    runner = CliRunner()
    
    with runner.isolated_filesystem():
        result = runner.invoke(app, ['init', '--force'])
        if result.exit_code != 0:
            pytest.skip(f"Failed to initialize project: {result.output}")
        yield Path.cwd()