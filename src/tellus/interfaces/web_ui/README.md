# Tellus Web UI

A modern, responsive web frontend for the Tellus climate data management system built with [Reflex](https://reflex.dev).

## 🌟 Features

- **📊 Simulation Dashboard**: Manage and monitor climate model simulations
- **📍 Location Management**: Configure and test storage locations across distributed systems
- **📁 File Browser**: Discover and manage files across multiple storage backends
- **💬 AI Assistant**: Integrated chat interface powered by the tellus_chat API
- **⚡ Real-time Updates**: Live progress tracking and notifications
- **🎨 Modern UI**: Clean, responsive design with dark/light mode support
- **🔒 Secure**: Built-in authentication and authorization

## 🚀 Quick Start

### Prerequisites

- Python 3.11+
- Pixi package manager (already configured in this project)
- Running tellus_chat API (optional, for AI features)

### Installation

1. **Add Reflex dependency** (run from project root):
   ```bash
   pixi add reflex
   ```

2. **Enable the Reflex components** by uncommenting the Reflex code in:
   - `src/tellus/interfaces/web_ui/app.py`
   - Component files in `components/`
   - State files in `state/`
   - Page files in `pages/`

3. **Start the development server**:
   ```bash
   pixi run reflex run src/tellus/interfaces/web_ui/app.py
   ```

4. **Open your browser** and navigate to `http://localhost:3000`

### Alternative: Preview Mode

To see the structure without installing Reflex:

```bash
pixi run python src/tellus/interfaces/web_ui/app.py
```

This will show you what features are implemented and provide setup instructions.

## 📁 Project Structure

```
src/tellus/interfaces/web_ui/
├── app.py                 # Main Reflex application
├── components/            # Reusable UI components
│   ├── layout/           # Navigation, sidebar, headers
│   ├── simulation/       # Simulation-specific components
│   ├── location/         # Location management components
│   ├── common/           # Shared utility components
│   └── chat/             # AI chat interface components
├── pages/                # Route pages
│   ├── index.py          # Dashboard page
│   ├── simulations.py    # Simulation management
│   ├── locations.py      # Location management
│   └── chat.py           # AI chat interface
├── state/                # Reflex state management
│   ├── simulation_state.py
│   ├── location_state.py
│   └── chat_state.py
├── services/             # API client and services
│   └── api_client.py     # Unified API client
└── styles/               # Custom styling
```

## 🔧 Configuration

### API Endpoints

The web UI can work with:

1. **Direct Service Calls** (default): Uses the existing Tellus service layer directly
2. **HTTP API**: When the main Tellus REST API is implemented
3. **Chat API**: Connects to the tellus_chat FastAPI server

Configure in `services/api_client.py`:

```python
api_client = TellusApiClient(
    base_url="http://localhost:8001",      # Future main API
    chat_api_url="http://localhost:8000",  # tellus_chat API
    use_direct_services=True               # Use direct service calls
)
```

### Environment Variables

```bash
# Optional: Override default API URLs
TELLUS_API_URL=http://localhost:8001
TELLUS_CHAT_API_URL=http://localhost:8000

# Optional: Enable features
TELLUS_ENABLE_CHAT=true
TELLUS_ENABLE_REAL_TIME=true
```

## 🎨 UI Components

### Layout Components

- **Navbar**: Main navigation with search and user actions
- **Sidebar**: Contextual navigation for different sections
- **Breadcrumbs**: Hierarchical navigation

### Simulation Components

- **SimulationCard**: Card view for simulation overview
- **SimulationList**: List view for simulations
- **SimulationDetail**: Detailed simulation view with metadata
- **SimulationForm**: Create/edit simulation forms

### Location Components

- **LocationCard**: Storage location overview
- **LocationTest**: Connectivity testing interface
- **LocationForm**: Add/edit location forms

### Common Components

- **DataTable**: Sortable, filterable data tables
- **StatusBadge**: Status indicators with color coding
- **ProgressBar**: Progress tracking for long operations
- **Modal**: Modal dialogs for forms and confirmations

## 🔌 API Integration

### Simulation API

```python
# Get simulations with filtering
simulations = await api_client.get_simulations(
    filters=FilterOptions(search_term="CESM2"),
    pagination=PaginationInfo(page=1, page_size=20)
)

# Create new simulation
new_sim = await api_client.create_simulation(
    CreateSimulationDto(
        simulation_id="my_simulation",
        model_id="CESM2",
        attrs={"experiment": "historical"}
    )
)
```

### Location API

```python
# Get locations
locations = await api_client.get_locations()

# Test location connectivity
test_result = await api_client.test_location("hpc_storage")
```

### Chat API

```python
# Send message to AI assistant
response = await api_client.send_chat_message(
    message="How many simulations are running?",
    conversation_id="conv_123"
)

# Get conversation history
history = await api_client.get_conversation_history("conv_123")
```

## 🎯 Usage Examples

### Dashboard Page

The dashboard provides an overview of:
- Total simulations and their status
- Recent activity and running jobs
- System status and health checks
- Quick actions for common tasks

### Simulation Management

- **View simulations** in card or list format
- **Filter and search** by model, experiment, status
- **Create new simulations** with form wizards
- **Monitor progress** with real-time updates

### Location Management

- **Add storage locations** with different protocols (SFTP, S3, etc.)
- **Test connectivity** and verify access
- **Configure path templates** for data organization
- **Monitor storage usage** and availability

### AI Assistant

- **Ask questions** about simulations and data
- **Get help** with configuration and troubleshooting
- **Natural language queries** for data discovery
- **Context-aware assistance** based on current page

## 🚀 Deployment

### Development

```bash
pixi add reflex
pixi run reflex run src/tellus/interfaces/web_ui/app.py --env dev
```

### Production

```bash
# Build for production
pixi run reflex export src/tellus/interfaces/web_ui/app.py

# Deploy with Docker
docker build -t tellus-web-ui .
docker run -p 3000:3000 tellus-web-ui
```

### Environment-specific Config

Create `.env` files for different environments:

```bash
# .env.development
TELLUS_API_URL=http://localhost:8001
TELLUS_CHAT_API_URL=http://localhost:8000
DEBUG=true

# .env.production
TELLUS_API_URL=https://api.tellus.example.com
TELLUS_CHAT_API_URL=https://chat.tellus.example.com
DEBUG=false
```

## 🛠 Development

### Adding New Components

1. Create component in appropriate directory (`components/`)
2. Follow the existing pattern with blueprint structure
3. Add Reflex implementation in comments
4. Export from `components/__init__.py`

### Adding New Pages

1. Create page in `pages/`
2. Define route structure and layout
3. Add to main app routing in `app.py`
4. Update navigation components

### State Management

1. Create state class in `state/`
2. Define reactive variables and methods
3. Use in components with proper decorators
4. Handle async operations with background tasks

## 🐛 Troubleshooting

### Common Issues

**Reflex not found**: Run `pixi add reflex` to install the dependency

**API connection errors**: 
- Check that tellus_chat is running on port 8000
- Verify API URLs in configuration
- Check network connectivity

**Import errors**: 
- Ensure you're running from the project root
- Check that the tellus package is installed in development mode

**State not updating**: 
- Verify state variables are properly decorated
- Check that state mutations trigger re-renders
- Use background tasks for async operations

### Getting Help

1. Check the [Reflex documentation](https://reflex.dev/docs)
2. Review the Tellus architecture documentation
3. Ask the AI assistant for help with specific issues
4. Create an issue in the project repository

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Add your changes following the existing patterns
4. Test with both direct services and mock data
5. Submit a pull request

## 📄 License

This project is part of the Tellus climate data management system.