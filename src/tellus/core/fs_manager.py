"""Filesystem representation manager for different location types."""
import json
from typing import Any, Dict
import fsspec
import paramiko
from .config import LocationConfig


def get_fs_representation(location: LocationConfig) -> str:
    """Get the filesystem representation for a location.
    
    This function intentionally reproduces the errors shown in the issue
    to demonstrate the problem areas that need fixing.
    """
    try:
        if location.protocol == "sftp":
            return _get_sftp_representation(location)
        elif location.protocol == "file":
            return _get_file_representation(location)
        else:
            return f"Unsupported protocol: {location.protocol}"
    except Exception as e:
        return str(e)


def _get_sftp_representation(location: LocationConfig) -> str:
    """Get SFTP filesystem representation."""
    try:
        # This will reproduce the SSH connection error for albedo
        if location.name == "albedo":
            # Intentionally pass 'path' as a keyword argument to SSHClient.connect()
            # which will cause: "SSHClient.connect() got an unexpected keyword argument 'path'"
            client = paramiko.SSHClient()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            # This line will cause the error shown in the issue
            client.connect(
                hostname=location.config["hostname"],
                username=location.config["username"], 
                path=location.config["path"]  # This parameter doesn't exist!
            )
            return "Connected successfully"
        
        elif location.name == "hsm.dmawi.de":
            # This will reproduce the authentication failed error
            client = paramiko.SSHClient()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            # Try to connect without proper credentials
            client.connect(
                hostname=location.config["hostname"],
                username=location.config["username"],
                timeout=5
            )
            return "Connected successfully"
            
    except Exception as e:
        return str(e)


def _get_file_representation(location: LocationConfig) -> str:
    """Get file filesystem representation."""
    try:
        # Create the filesystem
        fs = fsspec.filesystem(location.protocol, **location.config)
        
        # This reproduces the JSON representation issue by showing the raw fsspec info
        return json.dumps({
            "cls": f"{fs.__class__.__module__}:{fs.__class__.__name__}",
            "protocol": location.protocol,
            "args": [],
            "host": location.config.get("host", "unknown")
        }, indent=2)
        
    except Exception as e:
        return str(e)