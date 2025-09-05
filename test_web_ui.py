#!/usr/bin/env python3
"""
Test script for the Tellus Web UI prototype.

This script tests that the web UI structure is valid and demonstrates
the features that have been implemented.
"""

import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

def test_imports():
    """Test that all modules can be imported."""
    try:
        from tellus.interfaces.web_ui.app import TellusWebApp
        from tellus.interfaces.web_ui.state.simulation_state import SimulationState
        from tellus.interfaces.web_ui.state.location_state import LocationState
        from tellus.interfaces.web_ui.state.chat_state import ChatState
        from tellus.interfaces.web_ui.services.api_client import TellusApiClient
        print("✅ All imports successful")
        return True
    except ImportError as e:
        print(f"❌ Import error: {e}")
        return False

def test_app_creation():
    """Test that the app can be created."""
    try:
        from tellus.interfaces.web_ui.app import TellusWebApp
        app = TellusWebApp()
        config = app.get_app_config()
        print(f"✅ App created successfully: {config['app_name']}")
        return True
    except Exception as e:
        print(f"❌ App creation error: {e}")
        return False

def test_state_classes():
    """Test that state classes can be instantiated."""
    try:
        from tellus.interfaces.web_ui.state.simulation_state import SimulationState
        from tellus.interfaces.web_ui.state.location_state import LocationState
        from tellus.interfaces.web_ui.state.chat_state import ChatState
        
        sim_state = SimulationState()
        loc_state = LocationState()
        chat_state = ChatState()
        
        print("✅ State classes created successfully")
        print(f"  - Simulation state: {len(sim_state.simulations)} simulations loaded")
        print(f"  - Location state: {len(loc_state.locations)} locations loaded")
        print(f"  - Chat state: {len(chat_state.messages)} messages")
        
        return True
    except Exception as e:
        print(f"❌ State creation error: {e}")
        return False

def test_api_client():
    """Test that the API client can be created."""
    try:
        from tellus.interfaces.web_ui.services.api_client import TellusApiClient, api_client
        
        # Test singleton instance
        print(f"✅ API client created: {api_client.base_url}")
        print(f"  - Chat API: {api_client.chat_api_url}")
        print(f"  - Direct services: {api_client.use_direct_services}")
        
        return True
    except Exception as e:
        print(f"❌ API client error: {e}")
        return False

def demonstrate_features():
    """Demonstrate the implemented features."""
    print("\n🌟 Tellus Web UI Features")
    print("=" * 40)
    
    # Simulation management
    print("\n📊 Simulation Management:")
    print("  - Dashboard with simulation overview")
    print("  - Card and list views for simulations")
    print("  - Create, edit, and delete operations")
    print("  - Status tracking and filtering")
    print("  - Integration with Tellus simulation services")
    
    # Location management  
    print("\n📍 Location Management:")
    print("  - Multi-protocol storage locations (SFTP, S3, local)")
    print("  - Connectivity testing and verification")
    print("  - Path template configuration")
    print("  - Storage usage monitoring")
    
    # File management
    print("\n📁 File Management:")
    print("  - File discovery across distributed storage")
    print("  - Archive extraction and management")
    print("  - Content type classification")
    print("  - Bulk operations and transfers")
    
    # AI integration
    print("\n💬 AI Integration:")
    print("  - Chat interface with tellus_chat API")
    print("  - Context-aware assistance")
    print("  - Natural language queries")
    print("  - Conversation history management")
    
    # UI/UX
    print("\n🎨 Modern UI:")
    print("  - Responsive design for desktop and mobile")
    print("  - Clean, scientific interface")
    print("  - Dark/light mode support")
    print("  - Real-time progress tracking")
    print("  - Component-based architecture")

def show_next_steps():
    """Show next steps for implementation."""
    print("\n🚀 Next Steps")
    print("=" * 40)
    
    print("\n1. Install Reflex:")
    print("   pixi add reflex")
    
    print("\n2. Uncomment Reflex code in:")
    print("   - src/tellus/interfaces/web_ui/app.py")
    print("   - Component files in components/")
    print("   - State files in state/")
    print("   - Page files in pages/")
    
    print("\n3. Start development server:")
    print("   pixi run reflex run src/tellus/interfaces/web_ui/app.py")
    
    print("\n4. Open browser:")
    print("   http://localhost:3000")
    
    print("\n5. Optional - Start chat API:")
    print("   pixi run tellus-chat serve")

def main():
    """Main test function."""
    print("🧪 Testing Tellus Web UI Prototype")
    print("=" * 50)
    
    success = True
    
    # Run tests
    success &= test_imports()
    success &= test_app_creation()
    success &= test_state_classes()
    success &= test_api_client()
    
    if success:
        print("\n✅ All tests passed!")
        demonstrate_features()
        show_next_steps()
    else:
        print("\n❌ Some tests failed. Please check the errors above.")
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())