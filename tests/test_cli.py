"""Tests for CLI functionality."""
import pytest
from click.testing import CliRunner
from tellus.core.cli import main


def test_location_ls_command():
    """Test that location ls command runs without crashing."""
    runner = CliRunner()
    result = runner.invoke(main, ['location', 'ls'])
    
    # Should not crash
    assert result.exit_code == 0
    
    # Should contain the expected locations
    assert "albedo" in result.output
    assert "hsm.dmawi.de" in result.output  
    assert "localhost" in result.output
    
    # Should contain the error messages from the issue
    assert "unexpected keyword argument 'path'" in result.output
    assert "Authentication failed" in result.output


def test_location_ls_fixed_command():
    """Test that location ls --fixed command works."""
    runner = CliRunner()
    result = runner.invoke(main, ['location', 'ls', '--fixed'])
    
    # Should not crash
    assert result.exit_code == 0
    
    # Should contain the expected locations
    assert "albedo" in result.output
    assert "hsm.dmawi.de" in result.output  
    assert "localhost" in result.output
    
    # Should NOT contain the original error messages
    assert "unexpected keyword argument 'path'" not in result.output
    
    # Should contain better error handling
    assert "Connection error" in result.output or "Authentication failed" in result.output