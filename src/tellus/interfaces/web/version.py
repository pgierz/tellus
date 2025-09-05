"""Version utilities for the Tellus REST API."""

import subprocess
import re
from typing import Dict, Any
from functools import lru_cache


@lru_cache(maxsize=1)
def get_tellus_version() -> str:
    """
    Get the current Tellus version using commitizen.
    
    Returns:
        Version string (e.g., "0.1.0a3")
    """
    try:
        result = subprocess.run(
            ["cz", "version", "--project"],
            capture_output=True,
            text=True,
            timeout=10
        )
        if result.returncode == 0:
            return result.stdout.strip()
    except (subprocess.SubprocessError, FileNotFoundError):
        pass
    
    # Fallback to parsing git tags if commitizen fails
    try:
        result = subprocess.run(
            ["git", "describe", "--tags", "--abbrev=0"],
            capture_output=True,
            text=True,
            timeout=10
        )
        if result.returncode == 0:
            tag = result.stdout.strip()
            # Remove 'v' prefix if present
            if tag.startswith('v'):
                tag = tag[1:]
            return tag
    except (subprocess.SubprocessError, FileNotFoundError):
        pass
    
    # Final fallback
    return "0.1.0a3"


def get_api_version() -> str:
    """
    Get the API version string formatted for REST API paths.
    
    Converts version like "0.1.0a3" to "v1a3" for use in API paths.
    
    Returns:
        API version string (e.g., "v1a3")
    """
    version = get_tellus_version()
    
    # Extract major version and any alpha/beta/rc suffix
    # Pattern: major.minor.patch[alpha/beta/rc suffix]
    match = re.match(r'^(\d+)\.(\d+)\.(\d+)([a-z]+\d*)?', version)
    
    if match:
        major, minor, patch, suffix = match.groups()
        
        # For now, we'll use v{major} + suffix
        # E.g., "0.1.0a3" -> "v0a3", "1.2.3" -> "v1"
        api_version = f"v{major}"
        if suffix:
            api_version += suffix
        
        return api_version
    
    # Fallback for unparseable versions
    return "v1a3"


def get_version_info() -> Dict[str, Any]:
    """
    Get comprehensive version information.
    
    Returns:
        Dictionary containing version details
    """
    tellus_version = get_tellus_version()
    api_version = get_api_version()
    
    # Parse semantic version components
    match = re.match(r'^(\d+)\.(\d+)\.(\d+)([a-z]+)?(\d+)?', tellus_version)
    
    version_info = {
        "tellus_version": tellus_version,
        "api_version": api_version,
        "api_path": f"/api/{api_version}",
    }
    
    if match:
        major, minor, patch, pre_type, pre_num = match.groups()
        version_info.update({
            "major": int(major),
            "minor": int(minor), 
            "patch": int(patch),
            "is_prerelease": bool(pre_type),
            "prerelease_type": pre_type,
            "prerelease_number": int(pre_num) if pre_num else None,
        })
    
    return version_info


if __name__ == "__main__":
    # For testing
    import json
    print(json.dumps(get_version_info(), indent=2))