# Tellus Web UI

A modern, responsive web interface for the Tellus climate data management system built with [Reflex](https://reflex.dev/).

## Features

- **Dashboard**: Overview of system status, recent activity, and quick actions
- **Simulations**: Browse, search, and manage climate simulations
- **Locations**: Configure and monitor storage locations (local, SSH, cloud)
- **Files**: Discover and browse files across distributed storage
- **Responsive Design**: Works on desktop and mobile devices
- **Dark/Light Theme**: Automatic theme switching

## Architecture

The web UI follows a clean architecture pattern:

```
src/tellus/interfaces/web_ui/
├── app.py              # Main Reflex application
├── state.py            # Global state management
├── components/         # Reusable UI components
│   ├── layout.py       # Navigation, sidebar, page layout
│   ├── simulation_card.py
│   └── location_card.py
├── pages/              # Page components
│   ├── index.py        # Dashboard
│   ├── simulations.py  # Simulation management
│   ├── locations.py    # Location management
│   └── files.py        # File browser
├── services/           # Backend integration
│   └── api_client.py   # HTTP client for APIs
└── run.py             # Startup script
```

## Integration Points

### Existing APIs
- **Chat API**: `/chat` endpoint for conversational interface
- **Conversation API**: `/conversations/{id}` for chat history

### Future REST API Integration
The web UI is designed to integrate with the planned Tellus REST API:

- `GET /api/v1/simulations` - List simulations
- `GET /api/v1/locations` - List storage locations  
- `GET /api/v1/archives` - List archives
- `POST /api/v1/workflows/{id}/run` - Execute workflows

Currently uses mock data that matches the DTO structures defined in `src/tellus/application/dtos.py`.

## Running the Web UI

### Development Mode

1. Install dependencies:
   ```bash
   # Using pip
   pip install -r requirements.txt
   
   # Or using pixi
   pixi add reflex httpx
   ```

2. Start the development server:
   ```bash
   python run.py
   ```

3. Open your browser to http://localhost:3000

### Production Deployment

For production deployment, the Reflex app can be:

1. **Compiled to static files**: `reflex export`
2. **Deployed as a container**: Include in Docker image
3. **Served with reverse proxy**: nginx, Caddy, etc.

## State Management

The web UI uses Reflex's built-in reactive state management:

- `AppState` - Main application state class
- Async background tasks for API calls
- Reactive properties for derived data
- Client-server state synchronization

## Theming and Styling

- **Radix UI Theme**: Modern, accessible component system
- **Dark Mode**: Automatic system preference detection
- **Responsive Design**: Mobile-first responsive grid layouts
- **Icons**: Lucide icons for consistent iconography

## Integration with Tellus Core

The web UI integrates with the existing Tellus architecture:

- **DTOs**: Uses existing data transfer objects from `application/dtos.py`
- **Services**: Can call application services directly in Python
- **Locations**: Integrates with location management system
- **Chat**: Embeds existing chat functionality

## Future Enhancements

- **Real-time Updates**: WebSocket integration for live status updates
- **Workflow Visualization**: Interactive workflow diagrams
- **Performance Monitoring**: Resource usage charts and metrics
- **File Previews**: NetCDF data visualization
- **Advanced Search**: Full-text search across metadata
- **User Management**: Authentication and authorization
- **Mobile App**: Progressive Web App capabilities

## Development Notes

### Adding New Pages

1. Create page component in `pages/`
2. Add route to `app.py`
3. Update navigation in `components/layout.py`

### Adding New Components  

1. Create component in `components/`
2. Import and use in page components
3. Follow existing patterns for styling

### State Management

- Use async background tasks for API calls
- Update state using `async with self:` context
- Implement loading states and error handling
- Use reactive properties for computed values

### API Integration

- Extend `api_client.py` with new endpoints
- Update DTOs if backend schema changes
- Handle authentication and error responses
- Implement proper timeout and retry logic

## Contributing

When contributing to the web UI:

1. Follow the existing code style
2. Add proper type hints
3. Update this README for new features
4. Test on multiple screen sizes
5. Ensure accessibility compliance