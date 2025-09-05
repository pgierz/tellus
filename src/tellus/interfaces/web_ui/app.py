"""
Main Tellus Web UI Application

This is the main Reflex application for the Tellus web interface.
To run this app, first install Reflex: `pixi add reflex`
Then run: `reflex run src/tellus/interfaces/web_ui/app.py`

Features:
- Simulation management dashboard
- Location and storage management
- File browser and discovery
- AI-powered chat interface
- Real-time progress tracking
"""

# Uncomment when reflex is available
# import reflex as rx
# from typing import List, Dict, Any, Optional
# from .state.simulation_state import SimulationState
# from .state.location_state import LocationState
# from .state.chat_state import ChatState
# from .pages.index import index_page
# from .pages.simulations import simulations_page
# from .pages.locations import locations_page
# from .pages.chat import chat_page
# from .components.layout.navbar import navbar
# from .components.layout.sidebar import sidebar


class TellusWebApp:
    """
    Main Tellus Web Application class.
    
    When Reflex is available, this will be a proper Reflex app.
    For now, this serves as a blueprint and documentation.
    """
    
    def __init__(self):
        self.name = "tellus-web-ui"
        self.title = "Tellus - Climate Data Management"
        self.description = "Modern web interface for Tellus climate simulations"
        
    def get_app_config(self) -> dict:
        """Get Reflex app configuration."""
        return {
            "app_name": self.name,
            "title": self.title,
            "description": self.description,
            "theme": {
                "primary_color": "#2563EB",  # Blue
                "secondary_color": "#10B981", # Green
                "accent_color": "#F59E0B",    # Amber
            },
            "port": 3000,
            "cors_origins": ["http://localhost:8000"],  # For chat API integration
        }


# When Reflex is available, uncomment this section:
"""
# Configure the app
app_config = TellusWebApp().get_app_config()
app = rx.App(
    style={
        "font_family": "Inter, -apple-system, BlinkMacSystemFont, sans-serif",
    }
)

# Add pages
app.add_page(index_page, route="/")
app.add_page(simulations_page, route="/simulations")
app.add_page(locations_page, route="/locations")
app.add_page(chat_page, route="/chat")

# Enable client-side routing
app.api.include_router(
    prefix="/api/v1",
    tags=["tellus-ui-api"]
)

if __name__ == "__main__":
    app.run(
        host="0.0.0.0",
        port=app_config["port"],
        debug=True
    )
"""

# For now, provide a simple demonstration
def main():
    """Main entry point when Reflex is not available."""
    print("Tellus Web UI Prototype")
    print("=======================")
    print()
    print("This is a prototype web frontend for Tellus.")
    print("To run the full application:")
    print("1. Add Reflex dependency: pixi add reflex")
    print("2. Uncomment the Reflex code in this file")
    print("3. Run: reflex run src/tellus/interfaces/web_ui/app.py")
    print()
    print("Features implemented:")
    print("- 📊 Simulation management dashboard")
    print("- 📍 Location and storage management")
    print("- 📁 File browser and discovery")
    print("- 💬 AI-powered chat integration")
    print("- ⏱️  Real-time progress tracking")
    print("- 🎨 Modern, responsive UI design")


if __name__ == "__main__":
    main()