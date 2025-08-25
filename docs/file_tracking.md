# Tellus File Tracking

The Tellus File Tracking system provides Git-like version control for simulation files, allowing you to track changes, manage versions, and integrate with the Tellus archive system.

## Getting Started

### Initialize a Repository

To start tracking files in a directory, initialize a new Tellus repository:

```bash
tellus files init
```

This creates a `.tellus` directory with the necessary configuration files.

### Track Files

Add files to be tracked:

```bash
tellus files add file1.txt file2.txt
# Or add all files in the current directory
tellus files add .
```

### Check Status

View the status of your files:

```bash
tellus files status
```

### List Tracked Files

List all files being tracked:

```bash
tellus files list
```

## Ignoring Files

Create a `.tellusignore` file in your repository root to specify files and directories to ignore. For example:

```
# Ignore Python cache and compiled files
__pycache__/
*.py[cod]
*$py.class

# Ignore Git files
.git/
.gitignore

# Ignore macOS system files
.DS_Store
```

## How It Works

The file tracking system:

1. **Tracks file contents** using SHA-256 hashes
2. **Monitors file modifications** using modification timestamps
3. **Stores metadata** in the `.tellus` directory
4. **Integrates with the Tellus archive system** for versioning and backup

## Next Steps

- Archive tracked files to the Tellus archive system
- Restore previous versions of files
- Compare file versions
- Manage branches for different simulation scenarios

## Troubleshooting

### Common Issues

1. **File not found errors**: Ensure the file exists and you have read permissions
2. **Permission denied**: Run commands with appropriate permissions
3. **Repository not found**: Make sure you're in a directory with a `.tellus` subdirectory

For more information, run `tellus files --help` or refer to the full documentation.
