# Testing Guide

This guide covers testing practices, tools, and strategies for Tellus development. Comprehensive testing ensures reliability and helps prevent regressions as the codebase evolves.

## Testing Philosophy

Tellus follows a comprehensive testing strategy:

- **Unit Tests**: Test individual components in isolation
- **Integration Tests**: Test component interactions and CLI workflows  
- **Documentation Tests**: Ensure examples and documentation work correctly
- **Manual Testing**: Verify real-world usage scenarios

## Test Structure

```
tests/
├── conftest.py              # Shared pytest fixtures
├── fixtures.py              # Test data and utilities
├── test_cli.py             # CLI command testing
├── test_simulation.py      # Simulation management tests
├── test_location.py        # Location and storage tests
├── test_enduser_example.py # End-to-end workflow tests
└── integration/            # Integration test suite
    ├── test_workflows.py   # Complete workflow testing
    └── test_storage.py     # Real storage backend tests
```

## Running Tests

### Basic Test Execution

```bash
# Run all tests
pixi run -e test test

# Run specific test file
pixi run -e test pytest tests/test_simulation.py

# Run specific test function
pixi run -e test pytest tests/test_simulation.py::test_simulation_creation

# Run tests with verbose output
pixi run -e test pytest -v

# Run tests with coverage
pixi run -e test pytest --cov=tellus --cov-report=html
```

### Test Environments

Tellus provides multiple test environments:

```bash
# Python 3.11 testing
pixi run -e test-py311 test

# Python 3.12 testing  
pixi run -e test-py312 test

# All supported Python versions
pixi run -e test-py311 test && pixi run -e test-py312 test
```

### Continuous Integration

Tests run automatically on:
- **Every pull request**: Full test suite
- **Main branch commits**: Full test suite + additional checks
- **Release tags**: Full test suite + deployment tests

## Writing Tests

### Test Organization

Follow these conventions for organizing tests:

```python
# test_simulation.py
import pytest
from tellus.simulation import Simulation, SimulationExistsError

class TestSimulationCreation:
    \"\"\"Tests for simulation creation functionality.\"\"\"
    
    def test_create_with_id_only(self):
        \"\"\"Test creating simulation with just an ID.\"\"\"
        sim = Simulation(\"test-sim\")
        assert sim.simulation_id == \"test-sim\"
        assert sim.path is None
        
    def test_create_with_path(self):
        \"\"\"Test creating simulation with ID and path.\"\"\"
        sim = Simulation(\"test-sim\", path=\"/data/test\")
        assert sim.simulation_id == \"test-sim\"
        assert sim.path == \"/data/test\"
        
    def test_create_duplicate_id_raises_error(self):
        \"\"\"Test that duplicate simulation IDs raise an error.\"\"\"
        Simulation(\"duplicate-id\")
        with pytest.raises(SimulationExistsError):
            Simulation(\"duplicate-id\")

class TestSimulationMetadata:
    \"\"\"Tests for simulation metadata management.\"\"\"
    
    def test_add_attributes(self):
        \"\"\"Test adding metadata attributes.\"\"\"
        sim = Simulation(\"test-sim\")
        sim.attrs[\"model\"] = \"CESM2\"
        sim.attrs[\"experiment\"] = \"historical\"
        
        assert sim.attrs[\"model\"] == \"CESM2\"
        assert sim.attrs[\"experiment\"] == \"historical\"
```

### Using Fixtures

Leverage pytest fixtures for common test setup:

```python
# conftest.py
import pytest
import tempfile
from pathlib import Path
from tellus.simulation import Simulation
from tellus.location import Location, LocationKind

@pytest.fixture
def temp_dir():
    \"\"\"Provide a temporary directory for tests.\"\"\"
    with tempfile.TemporaryDirectory() as tmp_dir:
        yield Path(tmp_dir)

@pytest.fixture
def sample_simulation():
    \"\"\"Provide a sample simulation for testing.\"\"\"
    sim = Simulation(\"test-simulation\")
    sim.attrs.update({
        \"model\": \"CESM2\",
        \"experiment\": \"ssp585\",
        \"resolution\": \"1deg\"
    })
    yield sim
    # Cleanup if needed
    Simulation.delete_simulation(\"test-simulation\")

@pytest.fixture
def local_location(temp_dir):
    \"\"\"Provide a local storage location for testing.\"\"\"
    location = Location(
        name=\"test-local\",
        kinds=[LocationKind.DISK],
        config={
            \"protocol\": \"file\",
            \"storage_options\": {\"path\": str(temp_dir)}
        }
    )
    yield location
    # Cleanup
    Location.remove_location(\"test-local\")
```

### Testing CLI Commands

Use Click's testing utilities for CLI tests:

```python
# test_cli.py
import pytest
from click.testing import CliRunner
from tellus.core.cli import cli

class TestSimulationCLI:
    \"\"\"Tests for simulation CLI commands.\"\"\"
    
    def test_simulation_create_command(self):
        \"\"\"Test simulation create CLI command.\"\"\"
        runner = CliRunner()
        result = runner.invoke(cli, [
            'simulation', 'create', 'test-sim', 
            '--path', '/data/test'
        ])
        
        assert result.exit_code == 0
        assert \"Created simulation: test-sim\" in result.output
        
    def test_simulation_list_command(self):
        \"\"\"Test simulation list CLI command.\"\"\"
        # Create test simulation first
        runner = CliRunner()
        runner.invoke(cli, ['simulation', 'create', 'test-sim'])
        
        # Test list command
        result = runner.invoke(cli, ['simulation', 'list'])
        assert result.exit_code == 0
        assert \"test-sim\" in result.output
        
    def test_invalid_simulation_id_error(self):
        \"\"\"Test that invalid simulation IDs show proper errors.\"\"\"
        runner = CliRunner()
        result = runner.invoke(cli, ['simulation', 'show', 'nonexistent'])
        
        assert result.exit_code != 0
        assert \"not found\" in result.output
```

### Testing File Operations

Mock filesystem operations for reliable tests:

```python
# test_location.py
import pytest
from unittest.mock import Mock, patch
from tellus.location import Location

class TestLocationFileOperations:
    \"\"\"Tests for location file operations.\"\"\"
    
    @patch('fsspec.filesystem')
    def test_list_files(self, mock_filesystem):
        \"\"\"Test listing files in a location.\"\"\"
        # Setup mock
        mock_fs = Mock()
        mock_fs.ls.return_value = ['file1.nc', 'file2.nc', 'data/']
        mock_filesystem.return_value = mock_fs
        
        # Create location and test
        location = Location(
            name=\"test-location\",
            kinds=[LocationKind.DISK],
            config={\"protocol\": \"file\", \"storage_options\": {}}
        )
        
        files = location.fs.ls(\"/test/path\")
        assert \"file1.nc\" in files
        assert \"file2.nc\" in files
        
    @patch('shutil.copyfileobj')
    @patch('builtins.open')
    def test_file_download(self, mock_open, mock_copy):
        \"\"\"Test file download with progress tracking.\"\"\"
        # Setup mocks
        mock_file = Mock()
        mock_open.return_value.__enter__.return_value = mock_file
        
        # Test download logic
        # ... implementation
```

### Testing Path Resolution

Test template path resolution thoroughly:

```python
# test_context.py
import pytest
from tellus.simulation.context import LocationContext
from tellus.simulation import Simulation

class TestPathTemplating:
    \"\"\"Tests for path template resolution.\"\"\"
    
    def test_simple_template_resolution(self):
        \"\"\"Test basic path template resolution.\"\"\"
        context = LocationContext(path_prefix=\"{model}/{experiment}\")
        sim = Simulation(\"test-sim\")
        sim.attrs = {\"model\": \"CESM2\", \"experiment\": \"ssp585\"}
        
        resolved = context.resolve_path(sim)
        assert resolved == \"CESM2/ssp585\"
        
    def test_nested_template_resolution(self):
        \"\"\"Test complex nested path templates.\"\"\"
        context = LocationContext(
            path_prefix=\"{model}/{experiment}/resolution_{resolution}/run_{run_id}\"
        )
        sim = Simulation(\"test-sim\")
        sim.attrs = {
            \"model\": \"CESM2\",
            \"experiment\": \"ssp585\", 
            \"resolution\": \"1deg\",
            \"run_id\": \"001\"
        }
        
        resolved = context.resolve_path(sim)
        assert resolved == \"CESM2/ssp585/resolution_1deg/run_001\"
        
    def test_missing_template_variable_error(self):
        \"\"\"Test that missing template variables raise appropriate errors.\"\"\"
        context = LocationContext(path_prefix=\"{model}/{missing_var}\")
        sim = Simulation(\"test-sim\")
        sim.attrs = {\"model\": \"CESM2\"}
        
        with pytest.raises(KeyError):
            context.resolve_path(sim)
```

## Integration Testing

### End-to-End Workflow Tests

Test complete user workflows:

```python
# test_enduser_example.py
import pytest
import tempfile
from pathlib import Path
from click.testing import CliRunner
from tellus.core.cli import cli

class TestEndToEndWorkflows:
    \"\"\"Test complete user workflows.\"\"\"
    
    def test_complete_simulation_workflow(self):
        \"\"\"Test a complete simulation management workflow.\"\"\"
        runner = CliRunner()
        
        with tempfile.TemporaryDirectory() as tmp_dir:
            # 1. Create simulation
            result = runner.invoke(cli, [
                'simulation', 'create', 'workflow-test',
                '--path', tmp_dir,
                '--attr', 'model', 'CESM2',
                '--attr', 'experiment', 'ssp585'
            ])
            assert result.exit_code == 0
            
            # 2. Create location
            result = runner.invoke(cli, [
                'location', 'create', 'test-storage',
                '--protocol', 'file',
                '--path', tmp_dir
            ])
            assert result.exit_code == 0
            
            # 3. Add location to simulation
            result = runner.invoke(cli, [
                'simulation', 'location', 'add',
                'workflow-test', 'test-storage',
                '--path-prefix', '{model}/{experiment}'
            ])
            assert result.exit_code == 0
            
            # 4. Verify simulation state
            result = runner.invoke(cli, [
                'simulation', 'show', 'workflow-test'
            ])
            assert result.exit_code == 0
            assert \"CESM2\" in result.output
            assert \"ssp585\" in result.output
```

### Storage Backend Testing

Test with real storage backends in controlled environments:

```python
# integration/test_storage.py
import pytest
import os
from tellus.location import Location, LocationKind

@pytest.mark.integration
@pytest.mark.skipif(not os.getenv(\"TEST_SSH_HOST\"), 
                   reason=\"SSH integration tests require TEST_SSH_HOST environment variable\")
class TestSSHIntegration:
    \"\"\"Integration tests for SSH storage backends.\"\"\"
    
    def test_ssh_connection(self):
        \"\"\"Test SSH connection and basic operations.\"\"\"
        location = Location(
            name=\"test-ssh\",
            kinds=[LocationKind.REMOTE],
            config={
                \"protocol\": \"ssh\",
                \"storage_options\": {
                    \"host\": os.getenv(\"TEST_SSH_HOST\"),
                    \"username\": os.getenv(\"TEST_SSH_USER\"),
                    \"password\": os.getenv(\"TEST_SSH_PASSWORD\")
                }
            }
        )
        
        # Test basic filesystem operations
        files = location.fs.ls(\"/tmp\")
        assert isinstance(files, list)
        
        # Test file existence check
        exists = location.fs.exists(\"/tmp\")
        assert exists is True
```

## Documentation Testing

### Testing Jupyter Notebooks

Ensure notebook examples execute correctly:

```python
# test_notebooks.py
import pytest
import nbformat
from nbconvert.preprocessors import ExecutePreprocessor
from pathlib import Path

@pytest.mark.docs
class TestNotebookExecution:
    \"\"\"Tests for Jupyter notebook examples.\"\"\"
    
    def test_basic_usage_notebook(self):
        \"\"\"Test that basic usage notebook executes without errors.\"\"\"
        notebook_path = Path(\"docs/examples/basic-usage.ipynb\")
        
        with open(notebook_path) as f:
            nb = nbformat.read(f, as_version=4)
            
        # Execute notebook
        ep = ExecutePreprocessor(timeout=600, kernel_name='python3')
        ep.preprocess(nb, {'metadata': {'path': notebook_path.parent}})
        
        # Check for execution errors
        for cell in nb.cells:
            if cell.cell_type == 'code':
                assert 'outputs' in cell
                for output in cell.outputs:
                    assert output.output_type != 'error'
```

### Testing Code Examples in Documentation

Validate code examples in markdown files:

```python
# test_doc_examples.py
import pytest
import re
import subprocess
from pathlib import Path

@pytest.mark.docs
class TestDocumentationExamples:
    \"\"\"Test code examples in documentation.\"\"\"
    
    def test_cli_examples_in_docs(self):
        \"\"\"Test CLI examples found in documentation.\"\"\"
        docs_dir = Path(\"docs\")
        
        for md_file in docs_dir.rglob(\"*.md\"):
            content = md_file.read_text()
            
            # Find CLI examples
            cli_examples = re.findall(r'```bash\\n(tellus .+?)\\n```', content, re.MULTILINE)
            
            for example in cli_examples:
                if example.startswith('tellus --help'):
                    # Test help commands
                    result = subprocess.run(
                        ['pixi', 'run', 'tellus', '--help'],
                        capture_output=True, text=True
                    )
                    assert result.returncode == 0
```

## Performance Testing

### Benchmarking Critical Operations

```python
# test_performance.py
import pytest
import time
from tellus.simulation import Simulation

@pytest.mark.performance
class TestPerformance:
    \"\"\"Performance tests for critical operations.\"\"\"
    
    def test_simulation_creation_performance(self):
        \"\"\"Test simulation creation performance.\"\"\"
        start_time = time.time()
        
        for i in range(100):
            sim = Simulation(f\"perf-test-{i}\")
            
        end_time = time.time()
        
        # Should create 100 simulations in under 1 second
        assert (end_time - start_time) < 1.0
        
    def test_large_file_list_performance(self):
        \"\"\"Test performance with large file lists.\"\"\"
        # Create mock location with many files
        # ... implementation
```

## Test Configuration

### pytest Configuration

Configure pytest in `pyproject.toml`:

```toml
[tool.pytest.ini_options]
python_files = \"test_*.py\"
testpaths = [\"tests\"]
pythonpath = [\"src\"]
addopts = \"--strict-markers --strict-config\"
markers = [
    \"integration: integration tests (may require external services)\",
    \"performance: performance tests\", 
    \"docs: documentation tests\",
    \"slow: slow tests\",
]
```

### Test Markers

Use markers to categorize tests:

```python
@pytest.mark.integration
def test_ssh_backend():
    \"\"\"Integration test requiring SSH server.\"\"\"

@pytest.mark.performance  
def test_large_transfer():
    \"\"\"Performance test for large file transfers.\"\"\"

@pytest.mark.slow
def test_comprehensive_workflow():
    \"\"\"Slow test covering comprehensive workflow.\"\"\"
```

### Running Specific Test Categories

```bash
# Run only unit tests (exclude integration)
pixi run -e test pytest -m \"not integration\"

# Run only integration tests
pixi run -e test pytest -m integration

# Run performance tests
pixi run -e test pytest -m performance

# Skip slow tests for quick feedback
pixi run -e test pytest -m \"not slow\"
```

## Continuous Integration

### GitHub Actions Configuration

```yaml
# .github/workflows/test.yml
name: Test Suite

on: [push, pull_request]

jobs:
  test:
    runs-on: ${{ matrix.os }}
    strategy:
      matrix:
        os: [ubuntu-latest, macos-latest]
        python-version: [\"3.11\", \"3.12\"]
        
    steps:
    - uses: actions/checkout@v3
    
    - name: Install Pixi
      uses: prefix-dev/setup-pixi@v0.4.1
      
    - name: Run tests
      run: pixi run -e test-py${{ matrix.python-version }} test
      
    - name: Upload coverage
      uses: codecov/codecov-action@v3
```

## Best Practices

### Test Design Principles

1. **Fast**: Unit tests should run quickly for rapid feedback
2. **Independent**: Tests should not depend on each other
3. **Repeatable**: Tests should produce consistent results
4. **Self-validating**: Tests should clearly pass or fail
5. **Timely**: Write tests as you develop features

### Coverage Guidelines

- **Aim for 80%+ coverage**: Focus on critical paths
- **Test edge cases**: Error conditions, boundary values
- **Mock external dependencies**: Network, filesystem, etc.
- **Test user interfaces**: CLI commands and workflows

### Test Maintenance

- **Keep tests simple**: One concept per test
- **Use descriptive names**: Tests should document behavior
- **Update tests with code changes**: Keep tests current
- **Remove obsolete tests**: Clean up when features change

This comprehensive testing approach ensures Tellus remains reliable and maintainable as it evolves.