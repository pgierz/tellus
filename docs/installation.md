# Installation

Tellus is a Python package that can be installed using various package managers. This guide covers the different installation methods and requirements.

## Requirements

- Python 3.11 or higher
- Unix-like operating system (Linux, macOS)
- Git (for development installation)

## Installation Methods

### Using Pixi (Recommended for Development)

Tellus uses [Pixi](https://pixi.sh) for dependency management and development environments:

```bash
# Clone the repository
git clone https://github.com/pgierz/tellus.git
cd tellus

# Install dependencies and activate environment
pixi install
pixi shell
```

This will install all dependencies including development tools and testing frameworks.

### Using pip (Coming Soon)

```{note}
Tellus is not yet published to PyPI. This section will be updated when the package is available.
```

```bash
pip install tellus
```

### Development Installation

For development, clone the repository and install in editable mode:

```bash
git clone https://github.com/pgierz/tellus.git
cd tellus
pip install -e .
```

## Verifying Installation

After installation, verify that Tellus is working correctly:

```bash
tellus --version
```

You should see the version number printed to the console.

## Optional Dependencies

Tellus has several optional dependencies for specific storage backends:

- **S3 Support**: Already included via `fsspec[s3]`
- **Google Cloud Support**: Already included via `fsspec[gcs]`
- **SSH/SFTP Support**: Already included via `paramiko`

## Configuration

After installation, you may want to create a configuration file. Tellus will look for configuration in:

- `~/.config/tellus/config.yaml`
- `./config/tellus.yaml` (project-specific)

See the {doc}`quickstart` guide for configuration examples.

## Troubleshooting

### Common Issues

**ImportError**: If you encounter import errors, ensure you're using Python 3.11 or higher:

```bash
python --version
```

**Permission Issues**: On some systems, you may need to use `--user` flag with pip:

```bash
pip install --user tellus
```

**Development Dependencies**: If using the development installation, make sure you have all required development tools:

```bash
pixi install
```

### Getting Help

If you encounter issues during installation:

1. Check the [GitHub Issues](https://github.com/pgierz/tellus/issues) page
2. Create a new issue with your system information and error messages
3. Join our community discussions