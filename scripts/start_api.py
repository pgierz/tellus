#!/usr/bin/env python3
"""
Start the Tellus API server with automatic port detection.

This script finds an available port (starting with 1968) and starts the API server.
"""

import sys
import os
import subprocess
from pathlib import Path

# Add src to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src"))

from tellus.interfaces.web.port_utils import find_available_port, get_api_url


def main():
    """Start the API server with automatic port detection."""
    try:
        # Find available port
        port = find_available_port(preferred_port=1968)
        api_url = get_api_url(port)
        
        print(f"🚀 Starting Tellus API server on {api_url}")
        
        if port != 1968:
            print(f"📍 Note: Using port {port} (preferred port 1968 was unavailable)")
        
        # Set environment variable for clients
        os.environ['TELLUS_API_URL'] = api_url
        
        # Start uvicorn server
        cmd = [
            "uvicorn", 
            "src.tellus.interfaces.web.main:app",
            "--reload",
            "--host", "0.0.0.0", 
            "--port", str(port)
        ]
        
        subprocess.run(cmd, cwd=project_root)
        
    except RuntimeError as e:
        print(f"❌ Error: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        print("\n👋 API server stopped")
        sys.exit(0)


if __name__ == "__main__":
    main()