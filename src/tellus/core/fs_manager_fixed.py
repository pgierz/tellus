"""Fixed filesystem representation manager."""
import json
from typing import Any, Dict
import fsspec
import paramiko
from .config import LocationConfig


def get_fs_representation_fixed(location: LocationConfig) -> str:
    """Get the FIXED filesystem representation for a location."""
    try:
        if location.protocol == "sftp":
            return _get_sftp_representation_fixed(location)
        elif location.protocol == "file":
            return _get_file_representation_fixed(location)
        else:
            return f"Unsupported protocol: {location.protocol}"
    except Exception as e:
        return f"Error: {str(e)}"


def _get_sftp_representation_fixed(location: LocationConfig) -> str:
    """Get FIXED SFTP filesystem representation."""
    try:
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        
        # FIX 1: Remove the invalid 'path' parameter from connect()
        client.connect(
            hostname=location.config["hostname"],
            username=location.config["username"],
            timeout=5
            # Removed the invalid 'path' parameter that was causing the SSH error
        )
        
        # Test the connection
        stdin, stdout, stderr = client.exec_command('pwd')
        current_path = stdout.read().decode().strip()
        client.close()
        
        return f"Connected successfully. Current path: {current_path}"
        
    except paramiko.AuthenticationException:
        # FIX 2: Better error handling for authentication failures
        return "Authentication failed. Please check credentials."
    except paramiko.SSHException as e:
        return f"SSH connection error: {str(e)}"
    except Exception as e:
        return f"Connection error: {str(e)}"


def _get_file_representation_fixed(location: LocationConfig) -> str:
    """Get FIXED file filesystem representation."""
    try:
        # Create the filesystem
        fs = fsspec.filesystem(location.protocol, **location.config)
        
        # FIX 3: Return a more user-friendly representation instead of raw JSON
        return f"Local filesystem on {location.config.get('host', 'localhost')}"
        
    except Exception as e:
        return f"Error creating filesystem: {str(e)}"