# Tellus Location List Bug Fix

This implements a fix for Issue #10: "Bug: Python FS Representation broken in location listing"

## Problem Description

The issue reported that `tellus location list` was showing broken FS representations with these specific errors:

1. **SSH connection errors**: `SSHClient.connect()` got an unexpected keyword argument 'path'`
2. **Authentication failures**: `Authentication failed.` for SSH connections
3. **Raw JSON output**: Filesystem representations showing raw JSON instead of user-friendly display

## Root Cause Analysis

The existing `tellus location list` command in `src/tellus/location/cli.py` had issues with:
- Calling `fs.to_json()` which could fail for various reasons
- Poor error handling for SSH/SFTP connections
- No user-friendly representation for filesystem objects

## Implementation

### Files Modified:
- `src/tellus/location/cli.py` - Enhanced existing location list command with improved filesystem representations
- `tests/test_cli.py` - Updated tests for the enhanced functionality

### Dependencies Added (to pixi):
- `click>=8.0.0` - CLI framework (already used)
- `rich>=13.0.0` - Rich terminal output (already used)
- `fsspec>=2023.0.0` - Filesystem abstraction (already used)
- `paramiko>=3.0.0` - SSH connections

## Fixes Applied

### 1. Improved Default Behavior
Enhanced the existing `tellus location list` command to always provide improved filesystem representations instead of raw JSON or error messages.

### 2. Improved SSH Authentication
**Problem**: SSH connections lacked proper authentication handling
```python
# FIXED: Support for multiple authentication methods
connect_kwargs = {
    "hostname": hostname,
    "username": username,
    "timeout": 5,
}
if password:
    connect_kwargs["password"] = password
elif key_filename:
    connect_kwargs["key_filename"] = key_filename
# Falls back to passwordless (common in HPC environments)
```

### 3. Better Error Messages
**Problem**: Raw exceptions and JSON were shown to users
```python
# ORIGINAL: Raw exceptions displayed
try:
    fs = loc.fs.to_json()
except Exception as e:
    fs = str(e)  # Raw exception message

# FIXED: User-friendly messages
fs = _get_improved_fs_representation(loc)  # Always use friendly descriptions
```

### 4. SFTP Protocol Usage
**Problem**: Using shell commands instead of proper SFTP protocol
```python
# FIXED: Use SFTP protocol methods
sftp = client.open_sftp()
current_path = sftp.getcwd()  # Instead of exec_command('pwd')
```

## Usage

### List Locations (now with improved filesystem representations):
```bash
tellus location list
```

### Run Tests:
```bash
pixi run test tests/test_cli.py
```

## Benefits

1. **Clean Design**: No confusing flags - the command just works correctly now
2. **Proper Integration**: Works with existing location management system
3. **Better Security**: Proper SSH authentication handling
4. **User-Friendly**: Clear error messages and descriptions instead of raw JSON/exceptions
5. **Simplified Experience**: Users don't need to remember which flag gives good output