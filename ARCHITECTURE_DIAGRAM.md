# Snakemake Workflow Integration - Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          TELLUS WORKFLOW SYSTEM                              │
└─────────────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────────────┐
│                           EXTERNAL SYSTEMS                                    │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                               │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐             │
│  │  ESM-Tools      │  │  Autosubmit     │  │  Custom         │             │
│  │  Workflow       │  │  Workflow       │  │  Workflows      │             │
│  │                 │  │                 │  │                 │             │
│  │  (Running on    │  │  (Running on    │  │  (User-defined) │             │
│  │   HPC cluster)  │  │   HPC cluster)  │  │                 │             │
│  └────────┬────────┘  └────────┬────────┘  └────────┬────────┘             │
│           │                     │                     │                      │
│           └─────────────────────┴─────────────────────┘                      │
│                                 │                                            │
│                   Observed by   │                                            │
│                                 ▼                                            │
│  ┌──────────────────────────────────────────────────────────────────┐       │
│  │  snakemake-workflow-tellus-run-fingerprint                        │       │
│  │  (Deployed per-simulation via snakedeploy)                        │       │
│  │                                                                    │       │
│  │  - Non-invasive observation                                       │       │
│  │  - Discovers phases, files, metadata                              │       │
│  │  - Generates RunFingerprint JSON                                  │       │
│  │                                                                    │       │
│  │  Location: /work/experiments/PI_control/.fingerprinting/          │       │
│  └──────────────────────────────┬───────────────────────────────────┘       │
│                                  │                                           │
└──────────────────────────────────┼───────────────────────────────────────────┘
                                   │
                                   │ RunFingerprint JSON
                                   │ (phases, files, status)
                                   │
                                   ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                         POLLING DAEMON (External Service)                     │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                               │
│  - Queries Tellus for workflows with polling_enabled=true                    │
│  - Triggers Snakemake fingerprinting at configured intervals                 │
│  - POSTs RunFingerprint data back to Tellus API                              │
│                                                                               │
│  Example polling intervals: 30s, 1m, 5m, 15m, 30m, 1h                        │
└───────────────────────────────────┬──────────────────────────────────────────┘
                                    │
                                    │ HTTP API Calls
                                    │
                                    ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                       TELLUS REST API (FastAPI)                               │
│                       Port: 8000, Path: /api/v0a3                             │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                               │
│  Workflow Endpoints:                                                          │
│  ┌────────────────────────────────────────────────────────────────┐          │
│  │ GET    /workflows/                  - List workflows            │          │
│  │ POST   /workflows/                  - Create workflow           │          │
│  │ GET    /workflows/{id}              - Get workflow details      │          │
│  │ PUT    /workflows/{id}              - Update workflow           │          │
│  │ DELETE /workflows/{id}              - Delete workflow           │          │
│  │                                                                  │          │
│  │ GET    /simulations/{id}/workflows  - List for simulation       │          │
│  │ POST   /simulations/{id}/workflows  - Create with simulation    │          │
│  │                                                                  │          │
│  │ PUT    /workflows/{id}/fingerprint  - Update RunFingerprint     │          │
│  │ GET    /workflows/{id}/fingerprint  - Get current fingerprint   │          │
│  │ GET    /workflows/{id}/fingerprint/history - Fingerprint history│          │
│  │                                                                  │          │
│  │ POST   /workflows/{id}/locations    - Associate location        │          │
│  │ DELETE /workflows/{id}/locations/{name} - Dissociate location   │          │
│  │                                                                  │          │
│  │ POST   /workflows/{id}/polling/enable  - Enable polling         │          │
│  │ POST   /workflows/{id}/polling/disable - Disable polling        │          │
│  └────────────────────────────────────────────────────────────────┘          │
│                                                                               │
└───────────────────────────────────┬──────────────────────────────────────────┘
                                    │
                                    ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                     SERVICE LAYER (Business Logic)                            │
│                     WorkflowApplicationService                                │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                               │
│  - Workflow CRUD operations                                                   │
│  - Simulation association logic                                               │
│  - Location management                                                        │
│  - RunFingerprint processing and validation                                   │
│  - Polling control and validation (1-1440 min range)                          │
│  - Fingerprint history archiving                                              │
│  - Phase extraction to WorkflowPhase objects                                  │
│  - Status determination from phases                                           │
│                                                                               │
└───────────────────────────────────┬──────────────────────────────────────────┘
                                    │
                                    ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                     REPOSITORY LAYER (Data Access)                            │
│                     PostgresWorkflowRepository                                │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                               │
│  - Entity ↔ Model conversion                                                  │
│  - Set ↔ List conversion for JSON storage                                     │
│  - Enum serialization/deserialization                                         │
│  - WorkflowPhase serialization/deserialization                                │
│  - Datetime timezone handling                                                 │
│  - Timedelta string conversion                                                │
│                                                                               │
└───────────────────────────────────┬──────────────────────────────────────────┘
                                    │
                                    ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                         DATABASE (PostgreSQL)                                 │
│                         Table: workflows                                      │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                               │
│  Primary Key: workflow_id                                                     │
│  Foreign Key: simulation_id → simulations(simulation_id)                      │
│                                                                               │
│  Hybrid Storage:                                                              │
│  ┌──────────────────────────┬────────────────────────────────────┐           │
│  │ Structured (Queryable)   │ JSON (Flexible)                    │           │
│  ├──────────────────────────┼────────────────────────────────────┤           │
│  │ - current_status         │ - current_phases                   │           │
│  │ - observed_file_count    │ - current_fingerprint              │           │
│  │ - latest_observation_time│ - fingerprint_history              │           │
│  │ - polling_enabled        │ - deployment_config                │           │
│  │ - polling_interval       │ - steps                            │           │
│  │ - workflow_system        │ - associated_locations             │           │
│  └──────────────────────────┴────────────────────────────────────┘           │
│                                                                               │
│  Indexes: simulation_id, workflow_system, current_status,                     │
│           observed_file_count, latest_observation_time                        │
│                                                                               │
└───────────────────────────────────┬──────────────────────────────────────────┘
                                    │
                                    │ Queried by
                                    │
                                    ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                       TELLUS WEB UI (Reflex)                                  │
│                       Port: 3000                                              │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                               │
│  State Management: WorkflowState                                              │
│  ┌────────────────────────────────────────────────────────────────┐          │
│  │ - Load workflows for simulation                                │          │
│  │ - Fetch workflow details and RunFingerprint                    │          │
│  │ - Manage workflow selection                                    │          │
│  │ - Enable/disable polling                                       │          │
│  │ - Compute file statistics                                      │          │
│  └────────────────────────────────────────────────────────────────┘          │
│                                                                               │
│  Components:                                                                  │
│  ┌────────────────────────────────────────────────────────────────┐          │
│  │ workflow_list                                                  │          │
│  │ ├─ Workflow cards with status badges                           │          │
│  │ ├─ Polling indicators (pulse animation)                        │          │
│  │ ├─ File counts and observation times                           │          │
│  │ └─ "View Details" navigation                                   │          │
│  │                                                                 │          │
│  │ workflow_detail                                                │          │
│  │ ├─ Phase Timeline (visual progression)                         │          │
│  │ │  ├─ Phase status badges                                      │          │
│  │ │  ├─ Start/end times and durations                            │          │
│  │ │  └─ Log file indicators                                      │          │
│  │ ├─ File Statistics Cards                                       │          │
│  │ │  ├─ Total count and size                                     │          │
│  │ │  ├─ By classification (OUTPUT, LOG, etc.)                    │          │
│  │ │  ├─ By location                                              │          │
│  │ │  └─ By component (echam6, fesom, etc.)                       │          │
│  │ └─ Polling Controls                                            │          │
│  │    ├─ Enable/disable buttons                                   │          │
│  │    └─ Interval selector (30s, 1m, 5m, 15m, 30m, 1h)           │          │
│  └────────────────────────────────────────────────────────────────┘          │
│                                                                               │
│  Pages:                                                                       │
│  └─ simulation_detail.py                                                      │
│     └─ "Workflows" tab (integrated with existing tabs)                        │
│                                                                               │
└──────────────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────────────┐
│                           DOMAIN MODEL                                        │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                               │
│  WorkflowEntity                                                               │
│  ├─ Core: workflow_id, name, workflow_type, steps, parameters                │
│  ├─ System: workflow_system (esm-tools, autosubmit, custom, snakemake)       │
│  ├─ Deployment: deployment_path, deployment_config                            │
│  ├─ Polling: polling_enabled, polling_interval_minutes                        │
│  ├─ Locations: associated_locations, location_contexts                        │
│  ├─ Simulation: simulation_id, simulation_context                             │
│  └─ Fingerprint: current_phases, observed_file_count,                         │
│                  latest_observation_time, current_status,                     │
│                  current_fingerprint, fingerprint_history                     │
│                                                                               │
│  WorkflowPhase (Value Object)                                                 │
│  └─ phase_id, name, status, start_time, end_time, log_files, metadata        │
│                                                                               │
│  ObservedFile (Value Object)                                                  │
│  └─ path, classification, component, size_bytes, temporal_info,               │
│     location_name, metadata                                                   │
│                                                                               │
└──────────────────────────────────────────────────────────────────────────────┘

═══════════════════════════════════════════════════════════════════════════════
                              DATA FLOW EXAMPLE
═══════════════════════════════════════════════════════════════════════════════

1. ESM-Tools workflow runs on HPC cluster
   ↓
2. Snakemake observation workflow detects new files and phase changes
   ↓
3. Generates RunFingerprint JSON:
   {
     "phases": [
       {"phase_id": "prepare", "status": "COMPLETED", ...},
       {"phase_id": "compute", "status": "RUNNING", ...}
     ],
     "observed_files": [
       {"path": "output.nc", "classification": "OUTPUT", "size_bytes": 1048576000}
     ]
   }
   ↓
4. Polling daemon reads fingerprint and POSTs to Tellus API:
   PUT /api/v0a3/workflows/esm-pi-control/fingerprint
   ↓
5. Service layer processes RunFingerprint:
   - Extracts phases → WorkflowPhase objects
   - Determines status from phases (running)
   - Archives to fingerprint_history
   ↓
6. Repository saves to database:
   - Structured fields: current_status="running", observed_file_count=1
   - JSON blobs: current_fingerprint, fingerprint_history
   ↓
7. Web UI queries API and displays:
   - Workflow card shows "running" status (blue)
   - Phase timeline shows prepare=✓ (green), compute=⟳ (blue)
   - File statistics show 1 file, ~1GB
   - Polling indicator pulses (active)
   ↓
8. User can enable/disable polling or view full fingerprint history

═══════════════════════════════════════════════════════════════════════════════
                              TEST COVERAGE
═══════════════════════════════════════════════════════════════════════════════

Domain Entities (54 tests)
  ├─ WorkflowPhase creation and validation
  ├─ ObservedFile creation and validation
  ├─ WorkflowEntity fingerprint updates
  ├─ Status determination logic
  ├─ Fingerprint history tracking
  └─ Field validation

Repository Layer (55 tests)
  ├─ CRUD operations
  ├─ Hybrid storage serialization
  ├─ Set↔List conversion
  ├─ Enum serialization
  ├─ Datetime/timezone handling
  ├─ Error handling
  └─ Round-trip conversions

Service Layer (41 tests)
  ├─ Simulation association
  ├─ Location management
  ├─ Fingerprint updates
  ├─ Polling control
  ├─ History tracking
  └─ Business rule validation

REST API (54 tests)
  ├─ All endpoints (GET/POST/PUT/DELETE)
  ├─ HTTP status codes
  ├─ DTO validation
  ├─ Error response format
  └─ Integration scenarios

Total: 204 tests, 100% passing, 1.67s execution time

═══════════════════════════════════════════════════════════════════════════════
