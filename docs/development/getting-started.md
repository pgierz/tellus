# Development Getting Started

This guide will help you set up a complete development environment for Tellus, including documentation building, testing, and contributing workflow.

## Prerequisites

Before you begin, ensure you have:

- **Python 3.11+**: Tellus requires Python 3.11 or higher
- **Git**: For version control and contributing
- **Pixi**: Package manager for managing dependencies ([installation guide](https://pixi.sh/latest/))

```bash
# Verify your Python version
python --version  # Should be 3.11+

# Install Pixi (if not already installed)
curl -fsSL https://pixi.sh/install.sh | bash
```

## Setting Up Your Development Environment

### 1. Clone the Repository

```bash
git clone https://github.com/pgierz/tellus.git
cd tellus
```

### 2. Install Development Dependencies

Tellus uses Pixi for dependency management with multiple environments:

```bash
# Install all dependencies for the default environment
pixi install

# Activate the development shell
pixi shell
```

### 3. Verify Installation

Test that everything is working:

```bash
# Run Tellus CLI
pixi run tellus --version

# Run tests
pixi run -e test test

# Check available tasks
pixi task list
```

## 📚 Documentation Development

Tellus uses **Jupyter-Book** with the **Pydata Sphinx Theme** for documentation. Here's how to work with the documentation:

### Building Documentation

```bash
# Install documentation dependencies
pixi install -e docs

# Build the documentation
pixi run -e docs docs-build

# Clean previous builds (if needed)
pixi run -e docs docs-clean
```

### Viewing Documentation Locally

```bash
# Serve documentation locally at http://localhost:8000
pixi run -e docs docs-serve
```

The documentation will be available at [http://localhost:8000](http://localhost:8000).

### Alternative: Using Make

If you prefer traditional Makefiles:

```bash
cd docs

# Install dependencies
pip install -r requirements.txt

# Build documentation  
make build

# Serve locally
make serve
```

### Documentation Structure

```
docs/
├── _config.yml              # Jupyter-Book configuration
├── _toc.yml                 # Table of contents
├── index.md                 # Homepage
├── installation.md          # Installation instructions
├── quickstart.md           # Quick start guide
├── user-guide/             # User documentation
├── examples/               # Interactive notebooks
│   ├── basic-usage.ipynb   # Tutorial notebook
│   └── ...
├── api/                    # API documentation
├── development/            # This development guide
└── _static/               # Static assets (images, etc.)
```

### Key Documentation Features

- **🎯 Pydata Sphinx Theme**: Modern, responsive design with GitHub integration
- **📓 Interactive Notebooks**: Jupyter notebooks that execute during build
- **🎨 Rich Components**: Cards, grids, admonitions, and more via sphinx-design
- **📋 Code Copying**: One-click code copying with sphinx-copybutton
- **🔗 Cross-references**: Easy linking between pages and sections

### Adding New Documentation

#### Creating New Pages

1. **Markdown files**: Add `.md` files for standard documentation
2. **Jupyter notebooks**: Add `.ipynb` files for interactive examples
3. **Update `_toc.yml`**: Add your new pages to the table of contents

#### Writing Style Guidelines

- Use **MyST Markdown** syntax for enhanced features
- Include code examples with proper syntax highlighting
- Use admonitions for important notes:

```markdown
```{note}
This is an important note for users.
```

```{warning}  
This warns about potential issues.
```
```

#### Adding Interactive Examples

Create Jupyter notebooks in the `examples/` directory:

```python
# Example notebook cell
from tellus.simulation import Simulation

# Create a simulation
sim = Simulation("my-example")
print(f"Created: {sim.simulation_id}")
```

### Documentation Configuration

The main configuration is in `docs/_config.yml`:

```yaml
# Jupyter Book Configuration
title: Tellus Documentation
author: Paul Gierz

# Execution settings
execute:
  execute_notebooks: force  # Re-run notebooks on each build
  timeout: 120

# Theme configuration  
html:
  theme: pydata_sphinx_theme
  use_issues_button: true
  use_repository_button: true
```

## 🧪 Testing Your Changes

### Running Tests

```bash
# Run all tests
pixi run -e test test

# Run specific test file
pixi run -e test pytest tests/test_simulation.py

# Run with coverage
pixi run -e test pytest --cov=tellus
```

### Documentation Tests

Test that documentation builds correctly:

```bash
# Test documentation build
pixi run -e docs docs-build

# Test that notebooks execute without errors
jupyter nbconvert --execute --to notebook docs/examples/*.ipynb
```

## 🔧 Development Tools

### Available Pixi Tasks

```bash
# View all available tasks
pixi task list

# Common development tasks
pixi run tellus              # Run Tellus CLI
pixi run sandbox            # Run sandbox examples
pixi run -e test test       # Run tests
pixi run -e docs docs-build # Build documentation
```

### Project Structure for Developers

```
tellus/
├── src/tellus/             # Main source code
│   ├── core/              # CLI and application core
│   ├── simulation/        # Simulation management
│   ├── location/          # Storage locations
│   └── progress.py        # Progress tracking
├── tests/                 # Test suite
├── docs/                  # Documentation source
├── sandbox/               # Development examples
├── pyproject.toml         # Project configuration
└── pixi.toml             # Pixi environment config
```

### IDE Setup

For the best development experience:

#### VS Code
```json
// .vscode/settings.json
{
    "python.pythonPath": ".pixi/envs/default/bin/python",
    "python.testing.pytestEnabled": true,
    "python.testing.pytestArgs": ["tests"]
}
```

#### PyCharm
- Set interpreter to `.pixi/envs/default/bin/python`
- Mark `src` as Sources Root
- Configure pytest as the test runner

## 🚀 Development Workflow

### 1. Make Your Changes
- Write code following the existing patterns
- Add tests for new functionality  
- Update documentation as needed

### 2. Test Everything
```bash
# Run tests
pixi run -e test test

# Build and test documentation
pixi run -e docs docs-build

# Test CLI functionality
pixi run tellus simulation list
```

### 3. Submit Changes
```bash
# Create a feature branch
git checkout -b feature/my-new-feature

# Commit your changes
git add .
git commit -m "Add new feature: description"

# Push and create pull request
git push origin feature/my-new-feature
```

## 📝 Next Steps

- {doc}`contributing`: Learn about our contribution guidelines
- {doc}`architecture`: Understand Tellus's internal design
- {doc}`testing`: Deep dive into testing practices
- {doc}`../examples/index`: Explore interactive examples

## 🆘 Getting Help

If you encounter issues during development:

1. **Check the tests**: `pixi run -e test test`
2. **Verify dependencies**: `pixi install`
3. **Clean and rebuild**: `pixi run -e docs docs-clean && pixi run -e docs docs-build`
4. **Ask for help**: Create an issue on GitHub

The development environment is designed to be reliable and reproducible. Most issues can be resolved by ensuring you're using the correct Pixi environment and that all dependencies are properly installed.