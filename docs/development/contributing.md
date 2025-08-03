# Contributing to Tellus

Thank you for your interest in contributing to Tellus! This guide will help you get started with contributing code, documentation, examples, and bug reports.

## 🚀 Quick Start for Contributors

1. **Fork and clone** the repository
2. **Set up development environment** (see {doc}`getting-started`)
3. **Create a feature branch** for your changes
4. **Make your changes** following our guidelines
5. **Test thoroughly** and update documentation
6. **Submit a pull request** with a clear description

## Types of Contributions

We welcome various types of contributions:

::::{grid} 2
:::{grid-item-card} 🐛 Bug Reports
Report issues you encounter with clear reproduction steps
:::

:::{grid-item-card} ✨ Feature Requests  
Suggest new features or improvements to existing functionality
:::

:::{grid-item-card} 💻 Code Contributions
Fix bugs, implement features, or improve performance
:::

:::{grid-item-card} 📚 Documentation
Improve guides, add examples, or fix documentation issues
:::
::::

## Code Contribution Guidelines

### Before You Start

1. **Check existing issues** to see if your idea is already being discussed
2. **Create an issue** for new features to discuss the approach
3. **Start small** with bug fixes or minor improvements if you're new

### Development Workflow

#### 1. Set Up Your Environment

```bash
# Fork the repository on GitHub, then clone your fork
git clone https://github.com/YOUR_USERNAME/tellus.git
cd tellus

# Add upstream remote
git remote add upstream https://github.com/pgierz/tellus.git

# Set up development environment
pixi install
pixi shell
```

#### 2. Create a Feature Branch

```bash
# Update your main branch
git checkout master
git pull upstream master

# Create a new feature branch
git checkout -b feature/your-feature-name
```

Use descriptive branch names:
- `feature/add-s3-authentication`
- `bugfix/fix-progress-bar-crash`
- `docs/improve-installation-guide`

#### 3. Make Your Changes

Follow these coding standards:

**Python Code Style:**
- Follow [PEP 8](https://pep8.org/) style guidelines
- Use type hints for function parameters and return values
- Write docstrings for public functions and classes
- Keep functions focused and modular

**Example:**
```python
def create_simulation(
    simulation_id: str, 
    path: Optional[str] = None,
    attrs: Optional[Dict[str, Any]] = None
) -> Simulation:
    """Create a new simulation with the given parameters.
    
    Args:
        simulation_id: Unique identifier for the simulation
        path: Optional filesystem path for simulation data  
        attrs: Optional metadata attributes
        
    Returns:
        The created Simulation instance
        
    Raises:
        SimulationExistsError: If simulation_id already exists
    """
    # Implementation here
```

**CLI Code Guidelines:**
- Use Rich/rich-click for consistent UI
- Provide helpful error messages
- Include progress bars for long operations
- Support both interactive and non-interactive modes

#### 4. Add Tests

All code changes should include tests:

```bash
# Run existing tests to ensure nothing breaks
pixi run -e test test

# Add new tests in the tests/ directory
# Test file names should match: test_*.py
```

**Test Guidelines:**
- Write unit tests for individual functions
- Write integration tests for CLI commands
- Use fixtures for common test data
- Mock external dependencies (network, filesystem)

**Example test:**
```python
def test_simulation_creation():
    \"\"\"Test that simulations can be created with valid parameters.\"\"\"
    sim = Simulation(\"test-sim\", path=\"/tmp/test\")
    assert sim.simulation_id == \"test-sim\"
    assert sim.path == \"/tmp/test\"
```

#### 5. Update Documentation

- **Docstrings**: Add/update docstrings for any new or modified functions
- **User docs**: Update user guides if your changes affect user workflows  
- **Examples**: Add notebook examples for significant new features
- **Changelog**: Add an entry describing your changes

#### 6. Test Your Changes

```bash
# Run the full test suite
pixi run -e test test

# Test CLI functionality manually
pixi run tellus --help
pixi run tellus simulation list

# Build and review documentation
pixi run -e docs docs-build
pixi run -e docs docs-serve
```

#### 7. Commit Your Changes

Write clear, descriptive commit messages:

```bash
# Good commit messages
git commit -m \"Add S3 authentication support for locations\"
git commit -m \"Fix progress bar crash when file size is unknown\"
git commit -m \"Update installation docs for Windows users\"

# Poor commit messages (avoid these)
git commit -m \"fix bug\"
git commit -m \"updates\"
git commit -m \"wip\"
```

### Pull Request Process

#### 1. Push Your Branch

```bash
git push origin feature/your-feature-name
```

#### 2. Create Pull Request

1. Go to the [Tellus GitHub repository](https://github.com/pgierz/tellus)
2. Click \"New Pull Request\"
3. Select your feature branch
4. Fill out the pull request template

#### 3. PR Description Template

```markdown
## Description
Brief description of what this PR does.

## Type of Change
- [ ] Bug fix (non-breaking change which fixes an issue)
- [ ] New feature (non-breaking change which adds functionality)  
- [ ] Breaking change (fix or feature that would cause existing functionality to not work as expected)
- [ ] Documentation update

## Testing
- [ ] Tests pass locally
- [ ] Added new tests for new functionality
- [ ] Documentation has been updated
- [ ] Manual testing completed

## Related Issues
Closes #123

## Screenshots (if applicable)
Add screenshots for UI changes
```

#### 4. Review Process

- **Automated checks**: CI will run tests and build documentation
- **Code review**: Maintainers will review your code
- **Feedback**: Address any requested changes
- **Approval**: Once approved, your PR will be merged

## Documentation Contributions

### Types of Documentation

- **User guides**: Help users accomplish tasks
- **API reference**: Document functions and classes
- **Examples**: Interactive Jupyter notebooks
- **Developer docs**: Help contributors understand the codebase

### Writing Guidelines

**Style:**
- Use clear, concise language
- Include code examples
- Test all code examples
- Use consistent terminology

**Structure:**
- Start with what the user wants to accomplish
- Provide step-by-step instructions
- Include troubleshooting for common issues
- Link to related documentation

### Adding Examples

Create Jupyter notebooks in `docs/examples/`:

1. **Clear narrative**: Explain what the example demonstrates
2. **Working code**: All cells should execute without errors
3. **Real use cases**: Show practical applications
4. **Clean output**: Remove unnecessary output before committing

## Bug Reports

When reporting bugs, please include:

### Required Information

```markdown
**Describe the bug**
A clear description of what the bug is.

**To Reproduce**
Steps to reproduce the behavior:
1. Run command '...'
2. See error

**Expected behavior**
What you expected to happen.

**Environment:**
- OS: [e.g. macOS 14.0]
- Python version: [e.g. 3.11.5]
- Tellus version: [e.g. 0.1.0]
- Installation method: [e.g. pixi, pip]

**Additional context**
Add any other context about the problem here.
```

### Helpful Debugging

```bash
# Include output from these commands
tellus --version
python --version
pixi info

# Include full error messages and tracebacks
tellus simulation create test-sim --debug
```

## Feature Requests

When requesting features:

1. **Check existing issues** to avoid duplicates
2. **Describe the use case** - what problem does this solve?
3. **Propose a solution** - how should it work?
4. **Consider alternatives** - are there other approaches?

### Feature Request Template

```markdown
**Is your feature request related to a problem?**
A clear description of what the problem is.

**Describe the solution you'd like**
A clear description of what you want to happen.

**Describe alternatives you've considered**
Other approaches you've thought about.

**Use case**
Specific examples of how this would be used.
```

## Community Guidelines

### Code of Conduct

- **Be respectful** and inclusive
- **Be constructive** in feedback and discussions
- **Be patient** with new contributors
- **Focus on the code**, not the person

### Communication

- **GitHub Issues**: For bug reports and feature requests
- **Pull Requests**: For code reviews and discussions
- **Discussions**: For general questions and ideas

## Recognition

Contributors will be:
- **Listed** in the project documentation
- **Mentioned** in release notes for significant contributions
- **Invited** to be maintainers for sustained contributions

## Getting Help

If you need help with contributing:

1. **Read this guide** and the {doc}`getting-started` guide
2. **Check existing issues** and pull requests
3. **Create an issue** with your question
4. **Be specific** about what you're trying to do

Thank you for helping make Tellus better! 🚀