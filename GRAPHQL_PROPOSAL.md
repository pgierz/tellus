# GraphQL API Proposal for Tellus

## Executive Summary

Proposal to add GraphQL API alongside existing REST API to better support complex queries, nested data relationships, and real-time operations in Tellus. This document provides architectural analysis, implementation sketch, and phased rollout plan.

## Current REST API Summary

### Core Endpoints

**Simulations** (`/api/v0/simulations`)
- `GET /simulations` - List all simulations
- `POST /simulations` - Create simulation
- `GET /simulations/{id}` - Get simulation details
- `PUT /simulations/{id}` - Update simulation
- `DELETE /simulations/{id}` - Delete simulation
- `GET /simulations/{id}/attributes` - Get attributes
- `POST /simulations/{id}/attributes` - Add/update attribute
- `POST /simulations/{id}/locations` - Associate locations

**Locations** (`/api/v0/locations`)
- `GET /locations` - List all locations
- `POST /locations` - Create location
- `GET /locations/{name}` - Get location details
- `PUT /locations/{name}` - Update location
- `DELETE /locations/{name}` - Delete location

**Files** (`/api/v0/simulations/{id}/files`)
- `GET /simulations/{id}/files` - List simulation files
- `GET /simulations/{id}/files/{file_id}` - Get file details
- `POST /simulations/{id}/files` - Register file
- `DELETE /simulations/{id}/files/{file_id}` - Remove file

**Archives** (`/api/v0/simulations/{id}/archives`)
- `GET /simulations/{id}/archives` - List archives
- `POST /simulations/{id}/archives` - Create archive
- `GET /simulations/{id}/archives/{archive_id}` - Get archive details
- `DELETE /simulations/{id}/archives/{archive_id}` - Delete archive
- `GET /simulations/{id}/archives/{archive_id}/contents` - List archive contents
- `POST /simulations/{id}/archives/{archive_id}/index` - Index archive
- `POST /simulations/{id}/archives/{archive_id}/extract` - Extract files

**Workflows** (`/api/v0/workflows`)
- `GET /workflows` - List workflows
- `POST /workflows` - Create workflow
- `GET /workflows/{id}` - Get workflow details
- `POST /workflows/{id}/execute` - Execute workflow

### REST API Limitations

1. **Over-fetching**: CLI `simulation list` doesn't need full details, but REST returns everything or requires multiple endpoints
2. **Under-fetching**: `simulation show` needs data from multiple endpoints (locations, files, workflows) - N+1 problem
3. **No Real-time**: Long operations (archive staging, workflow execution) lack progress tracking
4. **Rigid Structure**: Adding new fields requires API versioning or breaking changes
5. **Complex Filtering**: Archive content queries with multiple filters become unwieldy

## Why GraphQL for Tellus?

### 1. Natural Fit for Climate Data Structures

Tellus domain has deep relationships:

```graphql
query GetSimulationWithEverything {
  simulation(id: "Eem125-S2") {
    id
    modelId
    attributes {
      experiment
      resolution
    }
    locations {
      name
      kind
      protocol
      pathTemplates {
        pattern
        description
      }
    }
    files(contentType: OUTPUT, location: "hsm.dmawi.de") {
      relativePath
      fileType
      importance
      sizeBytes
    }
    archives {
      archiveId
      splitParts
      contents(filter: "*.nc", grep: "mpiom") {
        path
        contentType
      }
    }
    workflows {
      name
      status
      steps {
        name
        dependencies
      }
    }
  }
}
```

Compare to REST requiring 6+ round trips.

### 2. Flexible CLI Queries Without Over-fetching

```graphql
# For 'tellus simulation list' - just summary
query SimulationList {
  simulations {
    id
    locationCount
    attributeCount
    fileCount
  }
}

# For 'tellus simulation show' - detailed view
query SimulationDetail($id: ID!) {
  simulation(id: $id) {
    id
    modelId
    path
    attributes
    locations {
      name
      kind
    }
    workflowSummary {
      total
      completed
      failed
    }
  }
}
```

### 3. Archive Content Discovery

```graphql
query ArchiveDiscovery {
  simulation(id: "Eem125-S2") {
    archives(location: "hsm.dmawi.de") {
      id
      splitParts
      indexed
      contents(
        filter: "*.nc"
        grep: "mpiom"
        contentType: OUTPUT
        first: 20
      ) {
        edges {
          node {
            path
            sizeBytes
            contentType
          }
        }
        pageInfo {
          hasNextPage
          endCursor
        }
      }
      contentSummary {
        netcdfFiles
        gribFiles
        totalSize
      }
    }
  }
}
```

Relay-style pagination built-in for huge archives.

### 4. Real-time Subscriptions

GraphQL subscriptions for long operations:

```graphql
subscription ArchiveStageProgress($simulationId: ID!) {
  archiveStageProgress(simulationId: $simulationId) {
    status
    bytesTransferred
    totalBytes
    transferRate
    estimatedTimeRemaining
    currentFile
  }
}

subscription WorkflowExecution($workflowId: ID!) {
  workflowProgress(workflowId: $workflowId) {
    currentStep
    completedSteps
    totalSteps
    output
    errors
  }
}
```

### 5. Type Safety and Auto-documentation

- Auto-generated TypeScript types for web UI (Issue #53)
- Self-documenting API (GraphiQL playground)
- CLI validation at query construction time

## GraphQL Disadvantages

### 1. Increased Complexity
- Learning curve for team
- Query complexity management (depth limiting, query cost analysis)
- HTTP caching doesn't work the same way

### 2. Overhead for Simple Operations
Simple CRUD is more verbose in GraphQL than REST.

### 3. Existing REST Investment
Significant REST infrastructure already built:
- REST endpoints (`src/tellus/interfaces/web/routers/`)
- REST client (`src/tellus/interfaces/cli/rest_client.py`)
- Docker Compose with REST API
- CLI integration with `TELLUS_CLI_USE_REST_API` flag

### 4. Network Transfer Bottlenecks
Large data transfers (staging archives, extracting data) still need traditional HTTP with range requests and streaming.

## Recommended Approach: Hybrid API

### Keep Both APIs

- **GraphQL** (`/graphql`): For web UI, complex queries, subscriptions
- **REST** (`/api/v0/*`): For CLI, file operations, backwards compatibility

This is the GitHub model - they maintain both REST and GraphQL APIs.

### Division of Responsibilities

**GraphQL handles:**
- Complex nested queries (simulation + locations + files + workflows)
- Flexible field selection (avoid over-fetching)
- Real-time subscriptions (progress tracking)
- Web UI data fetching

**REST handles:**
- File uploads/downloads
- Archive staging (HTTP range requests, resume capability)
- Streaming responses (large file extraction)
- Simple CRUD operations
- CLI backwards compatibility

## Proof-of-Concept Implementation

### Technology Stack

- **Strawberry GraphQL**: Modern Python GraphQL library, integrates perfectly with FastAPI
- **WebSockets**: For subscriptions (Strawberry supports ASGI WebSockets)
- **Existing Service Layer**: No changes needed to domain/application layers

### Schema Definition

```python
# src/tellus/interfaces/web/graphql/schema.py
import strawberry
from typing import Optional, List, AsyncGenerator
from datetime import datetime

@strawberry.type
class Simulation:
    id: str
    model_id: Optional[str]
    path: Optional[str]
    attrs: strawberry.scalars.JSON
    namelists: strawberry.scalars.JSON

    @strawberry.field
    def locations(self) -> List["Location"]:
        """Resolve locations for this simulation."""
        from ...application.container import get_service_container
        service = get_service_container().service_factory.simulation_service
        sim = service.get_simulation(self.id)
        return [
            Location(
                name=loc_name,
                # ... resolve location details
            )
            for loc_name in sim.associated_locations
        ]

    @strawberry.field
    def files(
        self,
        content_type: Optional[str] = None,
        location: Optional[str] = None,
        pattern: Optional[str] = None
    ) -> List["SimulationFile"]:
        """Resolve files with optional filtering."""
        from ...application.container import get_service_container
        file_service = get_service_container().service_factory.unified_file_service
        files = file_service.get_simulation_files(self.id)

        # Apply filters
        if content_type:
            files = [f for f in files if f.content_type.value == content_type]
        if location:
            files = [f for f in files if f.location_name == location]
        if pattern:
            import fnmatch
            files = [f for f in files if fnmatch.fnmatch(f.relative_path, pattern)]

        return [SimulationFile.from_entity(f) for f in files]

    @strawberry.field
    def total_size(self) -> int:
        """Computed field - expensive, opt-in only."""
        return sum(f.size_bytes or 0 for f in self.files())

    @strawberry.field
    def file_count(self) -> int:
        """Quick count without fetching full file details."""
        from ...application.container import get_service_container
        file_service = get_service_container().service_factory.unified_file_service
        files = file_service.get_simulation_files(self.id)
        return len(files)


@strawberry.type
class Location:
    name: str
    protocol: str
    path: str
    kinds: List[str]
    is_remote: bool
    is_accessible: bool

    @strawberry.field
    def path_templates(self) -> List["PathTemplate"]:
        """Resolve path templates for this location."""
        from ...application.container import get_service_container
        service = get_service_container().service_factory.location_service
        location = service.get_location(self.name)
        return [
            PathTemplate(
                name=tpl.name,
                pattern=tpl.pattern,
                description=tpl.description,
                required_attributes=tpl.required_attributes
            )
            for tpl in location.path_templates
        ]


@strawberry.type
class PathTemplate:
    name: str
    pattern: str
    description: Optional[str]
    required_attributes: List[str]


@strawberry.type
class SimulationFile:
    relative_path: str
    file_type: str
    content_type: str
    importance: str
    size_bytes: Optional[int]
    location_name: Optional[str]

    @classmethod
    def from_entity(cls, entity):
        """Convert domain entity to GraphQL type."""
        return cls(
            relative_path=entity.relative_path,
            file_type=entity.file_type.value,
            content_type=entity.content_type.value,
            importance=entity.importance.value,
            size_bytes=entity.size_bytes,
            location_name=entity.location_name
        )


@strawberry.type
class Archive:
    id: str
    location: Location
    pattern: str
    split_parts: Optional[int]
    archive_type: str
    indexed: bool

    @strawberry.field
    def contents(
        self,
        filter: Optional[str] = None,
        grep: Optional[str] = None,
        first: int = 100,
        after: Optional[str] = None
    ) -> "FileConnection":
        """Paginated archive contents."""
        # Implementation would use cursor-based pagination
        # and return Relay-style connection
        pass

    @strawberry.field
    def content_summary(self) -> "ContentSummary":
        """Aggregated statistics about archive contents."""
        pass


@strawberry.type
class ContentSummary:
    netcdf_files: int
    grib_files: int
    text_files: int
    total_files: int
    total_size: int


@strawberry.type
class FileConnection:
    """Relay-style connection for pagination."""
    edges: List["FileEdge"]
    page_info: "PageInfo"


@strawberry.type
class FileEdge:
    node: SimulationFile
    cursor: str


@strawberry.type
class PageInfo:
    has_next_page: bool
    has_previous_page: bool
    start_cursor: Optional[str]
    end_cursor: Optional[str]


@strawberry.type
class Query:
    @strawberry.field
    def simulation(self, id: str) -> Optional[Simulation]:
        """Get a single simulation by ID."""
        from ...application.container import get_service_container
        service = get_service_container().service_factory.simulation_service
        sim = service.get_simulation(id)

        if not sim:
            return None

        return Simulation(
            id=sim.simulation_id,
            model_id=sim.model_id,
            path=sim.path,
            attrs=sim.attrs,
            namelists=sim.namelists
        )

    @strawberry.field
    def simulations(
        self,
        location: Optional[str] = None,
        model_id: Optional[str] = None,
        first: int = 50,
        after: Optional[str] = None
    ) -> List[Simulation]:
        """List simulations with optional filtering."""
        from ...application.container import get_service_container
        service = get_service_container().service_factory.simulation_service
        result = service.list_simulations()

        # Apply filters
        simulations = result.simulations
        if location:
            simulations = [
                s for s in simulations
                if location in s.associated_locations
            ]
        if model_id:
            simulations = [
                s for s in simulations
                if s.model_id == model_id
            ]

        # TODO: Implement proper cursor pagination
        return [
            Simulation(
                id=s.simulation_id,
                model_id=s.model_id,
                path=s.path,
                attrs=s.attrs,
                namelists=s.namelists
            )
            for s in simulations[:first]
        ]

    @strawberry.field
    def location(self, name: str) -> Optional[Location]:
        """Get a single location by name."""
        from ...application.container import get_service_container
        service = get_service_container().service_factory.location_service
        loc = service.get_location(name)

        if not loc:
            return None

        return Location(
            name=loc.name,
            protocol=loc.config.get('protocol', ''),
            path=loc.config.get('path', ''),
            kinds=[kind.name for kind in loc.kinds],
            is_remote=loc.config.get('is_remote', True),
            is_accessible=loc.config.get('is_accessible', True)
        )

    @strawberry.field
    def locations(self) -> List[Location]:
        """List all locations."""
        from ...application.container import get_service_container
        service = get_service_container().service_factory.location_service
        result = service.list_locations()

        return [
            Location(
                name=loc.name,
                protocol=loc.config.get('protocol', ''),
                path=loc.config.get('path', ''),
                kinds=[kind.name for kind in loc.kinds],
                is_remote=loc.config.get('is_remote', True),
                is_accessible=loc.config.get('is_accessible', True)
            )
            for loc in result.locations
        ]


@strawberry.input
class SimulationCreateInput:
    simulation_id: str
    model_id: Optional[str] = None
    path: Optional[str] = None
    attrs: Optional[strawberry.scalars.JSON] = None


@strawberry.input
class ArchiveCreateInput:
    location: str
    pattern: str
    split_parts: Optional[int] = None
    archive_type: str = "single"


@strawberry.type
class Mutation:
    @strawberry.mutation
    def create_simulation(self, input: SimulationCreateInput) -> Simulation:
        """Create a new simulation."""
        from ...application.container import get_service_container
        from ...application.dtos import CreateSimulationDto

        service = get_service_container().service_factory.simulation_service
        dto = CreateSimulationDto(
            simulation_id=input.simulation_id,
            model_id=input.model_id,
            path=input.path,
            attrs=input.attrs or {}
        )

        result = service.create_simulation(dto)
        return Simulation(
            id=result.simulation_id,
            model_id=result.model_id,
            path=result.path,
            attrs=result.attrs,
            namelists=result.namelists
        )

    @strawberry.mutation
    def delete_simulation(self, id: str) -> bool:
        """Delete a simulation."""
        from ...application.container import get_service_container
        service = get_service_container().service_factory.simulation_service
        service.delete_simulation(id)
        return True

    @strawberry.mutation
    def add_archive(
        self,
        simulation_id: str,
        archive_input: ArchiveCreateInput
    ) -> Archive:
        """Add an archive to a simulation."""
        # Implementation would trigger async archive registration
        pass


@strawberry.type
class StageProgress:
    status: str
    bytes_transferred: int
    total_bytes: int
    transfer_rate: float
    estimated_time_remaining: Optional[int]
    current_file: Optional[str]


@strawberry.type
class WorkflowProgress:
    current_step: str
    completed_steps: int
    total_steps: int
    status: str
    output: Optional[str]
    errors: List[str]


@strawberry.type
class Subscription:
    @strawberry.subscription
    async def archive_stage_progress(
        self,
        simulation_id: str,
        archive_id: str
    ) -> AsyncGenerator[StageProgress, None]:
        """Subscribe to archive staging progress."""
        # WebSocket subscription for real-time progress
        # This would integrate with the existing progress tracking
        # in archive staging operations

        # Placeholder implementation
        import asyncio
        for i in range(10):
            await asyncio.sleep(1)
            yield StageProgress(
                status="transferring",
                bytes_transferred=i * 1000000,
                total_bytes=10000000,
                transfer_rate=1000000.0,
                estimated_time_remaining=10 - i,
                current_file=f"part_{i:04d}.tar.gz"
            )

        yield StageProgress(
            status="completed",
            bytes_transferred=10000000,
            total_bytes=10000000,
            transfer_rate=1000000.0,
            estimated_time_remaining=0,
            current_file=None
        )

    @strawberry.subscription
    async def workflow_progress(
        self,
        workflow_id: str
    ) -> AsyncGenerator[WorkflowProgress, None]:
        """Subscribe to workflow execution progress."""
        # Similar pattern for workflow monitoring
        pass


# Create the schema
schema = strawberry.Schema(
    query=Query,
    mutation=Mutation,
    subscription=Subscription
)
```

### FastAPI Integration

```python
# src/tellus/interfaces/web/main.py
from fastapi import FastAPI
from strawberry.fastapi import GraphQLRouter

from .routers import simulation_router, location_router, workflow_router
from .graphql.schema import schema

app = FastAPI(
    title="Tellus API",
    description="Earth System Model Data Management API",
    version="0.2.0"
)

# Existing REST routes
app.include_router(simulation_router, prefix="/api/v0", tags=["simulations"])
app.include_router(location_router, prefix="/api/v0", tags=["locations"])
app.include_router(workflow_router, prefix="/api/v0", tags=["workflows"])

# Add GraphQL endpoint
graphql_app = GraphQLRouter(
    schema,
    graphiql=True,  # Enable GraphiQL playground in development
    subscription_protocols=[
        "graphql-transport-ws",  # Modern WebSocket protocol
        "graphql-ws"             # Legacy protocol for compatibility
    ]
)
app.include_router(graphql_app, prefix="/graphql")

@app.get("/")
async def root():
    return {
        "message": "Tellus API",
        "rest_api": "/api/v0",
        "graphql": "/graphql",
        "docs": "/docs"
    }
```

### Example Queries

**Simple Simulation List (CLI-style)**
```graphql
query SimulationList {
  simulations {
    id
    modelId
    fileCount
    locationCount: locations {
      name
    }
  }
}
```

**Complex Nested Query**
```graphql
query ComplexSimulationQuery($id: ID!) {
  simulation(id: $id) {
    id
    modelId
    path
    attributes: attrs

    locations {
      name
      kind
      isAccessible
      pathTemplates {
        pattern
        description
      }
    }

    files(contentType: "OUTPUT", location: "hsm.dmawi.de") {
      relativePath
      sizeBytes
      contentType
    }

    totalSize
  }
}
```

**Archive Discovery**
```graphql
query ArchiveContents($simId: ID!, $filter: String) {
  simulation(id: $simId) {
    archives {
      id
      splitParts
      indexed
      contents(filter: $filter, first: 20) {
        edges {
          node {
            relativePath
            sizeBytes
            contentType
          }
        }
        pageInfo {
          hasNextPage
          endCursor
        }
      }
    }
  }
}
```

**Mutation Example**
```graphql
mutation CreateSimulation($input: SimulationCreateInput!) {
  createSimulation(input: $input) {
    id
    modelId
    path
  }
}

# Variables:
{
  "input": {
    "simulation_id": "Eem130-S2",
    "model_id": "AWICM",
    "path": "/work/ab0246/a270077/Eem130-S2",
    "attrs": {
      "experiment": "paleoclimate",
      "period": "Eem130ka"
    }
  }
}
```

**Subscription Example**
```graphql
subscription WatchStaging($simId: ID!, $archiveId: ID!) {
  archiveStageProgress(simulationId: $simId, archiveId: $archiveId) {
    status
    bytesTransferred
    totalBytes
    transferRate
    estimatedTimeRemaining
    currentFile
  }
}
```

## Phased Implementation Plan

### Phase 1: GraphQL Read Queries (v0.2.0)
**Timeline**: 2-3 weeks

**Scope**:
- Add Strawberry GraphQL to dependencies
- Implement basic schema (Simulation, Location, File queries)
- Add `/graphql` endpoint to FastAPI
- Enable GraphiQL playground for development
- Keep REST API unchanged (full backwards compatibility)

**Deliverables**:
- Working GraphQL endpoint
- Documentation for GraphQL queries
- Example queries for common use cases

**Testing**:
- Unit tests for GraphQL resolvers
- Integration tests for complex queries
- Performance comparison vs REST (N+1 queries)

### Phase 2: GraphQL Mutations (v0.3.0)
**Timeline**: 2-3 weeks

**Scope**:
- Implement mutations (create, update, delete)
- Add input validation
- Error handling and custom error types
- Keep REST for file operations

**Deliverables**:
- Full CRUD operations via GraphQL
- Mutation documentation
- CLI can optionally use GraphQL

**Testing**:
- Mutation tests
- Transaction handling tests
- Error case coverage

### Phase 3: GraphQL Subscriptions (v0.4.0)
**Timeline**: 3-4 weeks

**Scope**:
- WebSocket support for subscriptions
- Archive staging progress tracking
- Workflow execution monitoring
- Real-time file system events (optional)

**Deliverables**:
- Real-time progress updates
- WebSocket connection management
- Subscription documentation

**Testing**:
- WebSocket connection tests
- Concurrent subscription handling
- Connection stability under load

### Phase 4: Web UI Integration (v0.5.0+)
**Timeline**: Depends on web UI development (Issue #53)

**Scope**:
- TypeScript type generation from schema
- React/Vue components using GraphQL
- Apollo Client or urql integration
- Optimistic updates and caching

## Performance Considerations

### Query Complexity Analysis

Implement query cost analysis to prevent expensive queries:

```python
from strawberry.extensions import QueryDepthLimiter, ValidationCache

schema = strawberry.Schema(
    query=Query,
    extensions=[
        QueryDepthLimiter(max_depth=10),  # Prevent deeply nested queries
        ValidationCache(),                 # Cache query validation
    ]
)
```

### DataLoader Pattern

Use DataLoader to solve N+1 query problems:

```python
from strawberry.dataloader import DataLoader

async def load_locations(keys: List[str]) -> List[Location]:
    """Batch load locations to avoid N+1 queries."""
    service = get_service_container().service_factory.location_service
    # Batch load all locations at once
    return [service.get_location(key) for key in keys]

location_loader = DataLoader(load_fn=load_locations)

@strawberry.field
async def locations(self, info) -> List[Location]:
    """Use DataLoader for efficient batch loading."""
    return await info.context["location_loader"].load_many(
        self.associated_locations
    )
```

### Caching Strategy

- **Query-level caching**: Cache entire query results for common queries
- **Field-level caching**: Cache expensive computed fields (totalSize, fileCount)
- **CDN caching**: For public read-only queries

### Monitoring

Add query performance monitoring:

```python
from strawberry.extensions import SchemaExtension

class PerformanceMonitoring(SchemaExtension):
    def on_operation(self):
        yield
        # Log slow queries
        if self.execution_context.result.duration > 1.0:
            logger.warning(f"Slow query: {self.execution_context.query}")
```

## Migration Path

### For CLI Users
1. **v0.2.0**: REST remains default, GraphQL available via `TELLUS_CLI_USE_GRAPHQL=true`
2. **v0.3.0**: CLI can use either API, GraphQL recommended for complex queries
3. **v0.4.0+**: GraphQL becomes default for read operations, REST for file ops

### For API Consumers
1. Both APIs available indefinitely
2. REST API maintained for backwards compatibility
3. New features may be GraphQL-first

### For Developers
1. All service layer code remains unchanged
2. GraphQL resolvers are thin wrappers around existing services
3. Can add GraphQL incrementally (endpoint by endpoint)

## Security Considerations

### Authentication
Use existing authentication middleware:

```python
from fastapi import Depends
from .auth import get_current_user

graphql_app = GraphQLRouter(
    schema,
    context_getter=lambda request: {
        "user": get_current_user(request)
    }
)

@strawberry.field
def simulation(self, info, id: str) -> Optional[Simulation]:
    user = info.context["user"]
    # Authorize based on user permissions
    if not user.can_view_simulation(id):
        raise PermissionError("Access denied")
    # ...
```

### Rate Limiting
Apply rate limiting to prevent abuse:

```python
from slowapi import Limiter
from slowapi.util import get_remote_address

limiter = Limiter(key_func=get_remote_address)

@app.post("/graphql")
@limiter.limit("100/minute")
async def graphql_endpoint():
    # ...
```

### Query Complexity Limits
Prevent expensive queries:

```python
from strawberry.extensions import QueryDepthLimiter, MaxTokensLimiter

schema = strawberry.Schema(
    query=Query,
    extensions=[
        QueryDepthLimiter(max_depth=10),
        MaxTokensLimiter(max_token_count=1000),
    ]
)
```

## Documentation

### Auto-generated Documentation
GraphQL provides introspection out of the box:
- GraphiQL playground at `/graphql` (development)
- Schema documentation via introspection
- Apollo Studio integration for production docs

### Migration Guide
Provide REST → GraphQL translation guide:

```
REST: GET /api/v0/simulations/{id}
GraphQL: query { simulation(id: "...") { id modelId path } }

REST: GET /api/v0/simulations
GraphQL: query { simulations { id modelId } }

REST: POST /api/v0/simulations
GraphQL: mutation { createSimulation(input: {...}) { id } }
```

## Success Metrics

### Performance
- Query response time < 200ms for simple queries
- Complex nested queries < 1s
- Subscription latency < 100ms

### Developer Experience
- Time to implement new query: < 30 minutes
- Generated TypeScript types for web UI
- GraphiQL playground usage

### Adoption
- 50% of CLI operations use GraphQL by v0.4.0
- Web UI uses GraphQL exclusively
- Third-party integrations prefer GraphQL

## Risks and Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| Query complexity explosion | High | Query depth/cost limits, monitoring |
| N+1 query problems | High | DataLoader pattern, batch loading |
| Learning curve | Medium | Documentation, examples, workshops |
| Breaking changes | Low | REST remains available, versioning |
| Performance regression | Medium | Comprehensive benchmarking, caching |

## Alternatives Considered

### 1. REST Only with Better Design
**Pros**: Simpler, well-understood
**Cons**: Doesn't solve over/under-fetching, N+1 problems persist

### 2. gRPC
**Pros**: Type-safe, efficient binary protocol
**Cons**: Not browser-friendly, harder to debug, steeper learning curve

### 3. JSON:API Specification
**Pros**: Standardized REST, includes/sparse fieldsets
**Cons**: Still multiple round trips, no subscriptions, less flexible than GraphQL

### 4. Custom Query Language
**Pros**: Tailored to Tellus needs
**Cons**: Reinventing the wheel, no tooling ecosystem

## Conclusion

GraphQL is a strong fit for Tellus due to:
1. Complex nested data structures (simulations → locations → files → archives)
2. Varying data needs across CLI commands and web UI
3. Need for real-time operations (archive staging, workflow execution)
4. Benefit from type safety and auto-generated documentation

**Recommendation**: Implement GraphQL alongside REST in a phased approach, maintaining REST for file operations and backwards compatibility.

## Next Steps

1. **Spike/POC** (1 week): Implement basic GraphQL endpoint with Simulation queries
2. **Team Review**: Evaluate POC, gather feedback
3. **Decision**: Go/no-go on full implementation
4. **Phase 1 Implementation**: If approved, begin v0.2.0 development

## References

- [Strawberry GraphQL Documentation](https://strawberry.rocks/)
- [GraphQL Best Practices](https://graphql.org/learn/best-practices/)
- [GitHub GraphQL API](https://docs.github.com/en/graphql) (reference implementation)
- [DataLoader Pattern](https://github.com/graphql/dataloader)
- [Relay Cursor Connections](https://relay.dev/graphql/connections.htm)
