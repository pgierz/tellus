#!/usr/bin/env python3
"""
Startup script for the Tellus Web UI.

This script initializes and runs the Reflex web application.
"""

import sys
import os

# Add the tellus package to the path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../..')))

try:
    import reflex as rx
    from app import app
    
    if __name__ == "__main__":
        print("🌍 Starting Tellus Web UI...")
        print("📊 Climate Data Management Interface")
        print("🔗 http://localhost:3000")
        
        # Initialize and run the app
        app.compile()
        app.run(
            host="0.0.0.0",
            port=3000,
            dev_mode=True
        )
        
except ImportError as e:
    print("❌ Error: Missing dependencies")
    print(f"   {str(e)}")
    print()
    print("📋 To install dependencies:")
    print("   pip install -r requirements.txt")
    print()
    print("🛠  Or using pixi:")
    print("   pixi add reflex httpx")
    sys.exit(1)
except Exception as e:
    print(f"❌ Error starting web UI: {str(e)}")
    sys.exit(1)