# Web Frontend Implementation for Tellus REST API

## 🎯 Objective
Build a modern, responsive web frontend for the Tellus climate data management system that interfaces with the existing FastAPI REST backend.

## 📊 Current State Analysis

### Existing Infrastructure
- **Backend**: FastAPI REST API with versioned endpoints (`/api/v1/`)
- **Domain Architecture**: Clean DDD with simulations, locations, and file management
- **API Features**: 
  - Simulation CRUD operations
  - Location management
  - File discovery across distributed storage
  - Health monitoring endpoints
  - OpenAPI/Swagger documentation at `/docs`
- **Chat Interface**: Separate `tellus_chat` module with conversational API

### Key Entities to Visualize
- **SimulationEntity**: Climate model runs with metadata, provenance
- **LocationEntity**: Multi-protocol storage locations (SSH, SFTP, local, cloud)
- **SimulationFile**: Semantic file classification with content types

## 🚀 Recommended Technology Stack

### Option 1: **Python-Based with Modern Web Framework** (Recommended)
Stay within the Python ecosystem for easier maintenance and team expertise.

#### Frontend Framework: **Reflex** (formerly Pynecone)
- Pure Python web apps with React under the hood
- No JavaScript/TypeScript required
- Hot reload development
- Built-in state management
- Easy integration with existing Python codebase

```python
# Example Reflex component
import reflex as rx
from tellus.application.dtos import SimulationDto

class SimulationState(rx.State):
    simulations: list[SimulationDto] = []
    
    async def load_simulations(self):
        # Direct Python API calls
        self.simulations = await api_client.get_simulations()

def simulation_card(sim: SimulationDto):
    return rx.card(
        rx.heading(sim.simulation_id),
        rx.text(f"Model: {sim.model_id}"),
        rx.badge(sim.status)
    )
```

#### Alternative: **Streamlit**
- Rapid prototyping
- Built-in data visualization
- Simple deployment
- Less flexibility for complex UIs

#### Alternative: **Dash/Plotly**
- Excellent for data visualization
- Interactive plots out of the box
- Good for scientific applications

### Option 2: **TypeScript with Modern Framework**
For a more scalable, industry-standard solution.

#### Framework: **Next.js 14+ with App Router**
- Server Components for better performance
- Built-in API route handling
- TypeScript for type safety
- Excellent developer experience

```typescript
// Example component
export async function SimulationList() {
  const simulations = await fetch(`${API_URL}/api/v1/simulations`)
  
  return (
    <div className="grid gap-4">
      {simulations.map(sim => (
        <SimulationCard key={sim.uid} simulation={sim} />
      ))}
    </div>
  )
}
```

#### UI Library: **shadcn/ui** + **Tailwind CSS**
- Modern, accessible components
- Highly customizable
- Copy-paste component model
- Perfect for scientific/data-heavy interfaces

## 📋 Core Features to Implement

### Phase 1: Foundation (Weeks 1-2)
- [ ] Project setup and configuration
- [ ] Authentication/authorization layer
- [ ] Basic routing and navigation
- [ ] API client service layer
- [ ] Error handling and loading states

### Phase 2: Core Functionality (Weeks 3-4)
- [ ] **Simulation Dashboard**
  - Grid/list view toggle
  - Search and filter capabilities
  - Status indicators (running, completed, failed)
  - Quick actions (view, edit, delete)
  
- [ ] **Simulation Detail View**
  - Metadata display and editing
  - File browser across locations
  - Provenance tracking (Git history)
  - Run configuration viewer

- [ ] **Location Manager**
  - Add/edit storage locations
  - Test connectivity
  - Path template configuration
  - Protocol-specific settings

### Phase 3: Advanced Features (Weeks 5-6)
- [ ] **File Discovery Interface**
  - Multi-location search
  - File type filtering
  - Bulk operations
  - Archive extraction UI
  
- [ ] **Interactive Visualizations**
  - Simulation timeline view
  - Storage usage charts
  - Performance metrics dashboard
  - Network topology of locations

- [ ] **Workflow Integration**
  - Snakemake workflow triggers
  - Job monitoring
  - Log streaming

### Phase 4: AI Integration (Weeks 7-8)
- [ ] **Chat Interface Integration**
  - Embed tellus_chat conversational UI
  - Context-aware assistance
  - Natural language queries
  
- [ ] **Smart Recommendations**
  - Similar simulation suggestions
  - Optimization recommendations
  - Anomaly detection alerts

## 🎨 Design Principles

### Visual Design
- **Clean Scientific Interface**: Data-first, minimal chrome
- **Dark/Light Mode**: Essential for long analysis sessions
- **Responsive Design**: Desktop-first but mobile-capable
- **Accessibility**: WCAG 2.1 AA compliance

### User Experience
- **Progressive Disclosure**: Show complexity only when needed
- **Keyboard Navigation**: Power user shortcuts
- **Contextual Help**: Inline documentation
- **Performance**: Virtual scrolling for large datasets

## 📁 Proposed Project Structure

### If Python (Reflex):
```
src/tellus/interfaces/web_ui/
├── __init__.py
├── app.py              # Main Reflex app
├── state/              # State management
│   ├── simulation.py
│   ├── location.py
│   └── auth.py
├── components/         # Reusable components
│   ├── layout/
│   ├── simulation/
│   └── common/
├── pages/              # Route pages
│   ├── index.py
│   ├── simulations.py
│   └── locations.py
├── services/           # API client services
│   └── api_client.py
└── styles/            # Custom styling
```

### If TypeScript (Next.js):
```
tellus-web/
├── src/
│   ├── app/           # App router pages
│   │   ├── layout.tsx
│   │   ├── page.tsx
│   │   ├── simulations/
│   │   └── locations/
│   ├── components/    # React components
│   ├── lib/          # Utilities and services
│   │   ├── api-client.ts
│   │   └── types.ts
│   ├── hooks/        # Custom React hooks
│   └── styles/       # Global styles
├── public/           # Static assets
├── package.json
└── tsconfig.json
```

## 🔧 Implementation Recommendations

### 1. **Start with Python/Reflex**
Given the team's Python expertise and existing codebase:
- Faster initial development
- Easier maintenance
- Direct integration with backend services
- Can migrate to TypeScript later if needed

### 2. **API Client Generation**
- Use OpenAPI schema from `/api/v1/openapi.json`
- Generate typed clients automatically
- Python: `openapi-python-client`
- TypeScript: `openapi-typescript-codegen`

### 3. **State Management**
- Python/Reflex: Built-in reactive state
- TypeScript: Zustand or TanStack Query

### 4. **Testing Strategy**
- Component testing with Playwright
- E2E tests for critical workflows
- Visual regression with Percy/Chromatic

### 5. **Deployment**
- Containerize with Docker
- Deploy alongside API or separately
- Consider CDN for static assets
- Environment-specific configurations

## 📊 Success Metrics
- Page load time < 2 seconds
- Time to first meaningful paint < 1 second
- 90% of API calls < 500ms
- Accessibility score > 90
- User task completion rate > 80%

## 🚦 Next Steps
1. Team decision on technology stack (Python vs TypeScript)
2. Setup development environment
3. Create initial project structure
4. Implement authentication flow
5. Build first simulation list view
6. Iterate based on user feedback

## 💬 Discussion Points
- Should we prioritize mobile responsiveness?
- Do we need offline capabilities?
- What's the expected concurrent user load?
- Should we implement real-time updates via WebSockets?
- Integration timeline with TerraAI chat features?

## 🔗 References
- [Reflex Documentation](https://reflex.dev)
- [Next.js Documentation](https://nextjs.org)
- [FastAPI Frontend Integration](https://fastapi.tiangolo.com/tutorial/cors/)
- [shadcn/ui Components](https://ui.shadcn.com)

---

cc: @pgierz 
Labels: enhancement, frontend, architecture, discussion