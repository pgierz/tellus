# Tellus REST API Reference

## Overview

The Tellus REST API provides comprehensive HTTP endpoints for managing climate simulation data, workflows, storage locations, and file archives in distributed environments.

### Base URL

```
http://localhost:1968/api/prep-release/
```

For production deployments:
```
https://api.your-domain.com/api/prep-release/
```

### Response Format

All endpoints return JSON responses following this structure:

**Success Response:**
```json
{
  "data": { ... },
  "status": "success"
}
```

**Error Response:**
```json
{
  "detail": "Error message",
  "status": "error"
}
```

### Pagination

List endpoints return paginated results:

```json
{
  "items": [...],
  "pagination": {
    "page": 1,
    "page_size": 50,
    "total_count": 250,
    "has_next": true,
    "has_previous": false
  }
}
```

Query parameters:
- `page`: Page number (1-based, default: 1)
- `page_size`: Items per page (1-100, default: 50)

### Rate Limiting

**Nginx Configuration (Production):**
- API endpoints: 100 requests/minute per IP
- Health checks: 300 requests/minute per IP

Rate limit headers in responses:
```
X-RateLimit-Limit: 100
X-RateLimit-Remaining: 95
X-RateLimit-Reset: 1633024800
```

### Authentication

**Current Status:** No authentication required
**Future:** JWT-based authentication planned

### Error Codes

| Code | Meaning |
|------|---------|
| 200 | OK - Request succeeded |
| 201 | Created - Resource created successfully |
| 204 | No Content - Request succeeded, no response body |
| 400 | Bad Request - Invalid request data |
| 404 | Not Found - Resource does not exist |
| 409 | Conflict - Resource already exists |
| 422 | Unprocessable Entity - Validation failed |
| 500 | Internal Server Error - Server error |

---

## Health & Status Endpoints

### GET /health

Basic health check endpoint for monitoring and load balancers.

**Response:**
```json
{
  "status": "healthy",
  "timestamp": "2025-10-13T14:30:00Z",
  "uptime_seconds": 3600.5,
  "version": "0.1.0",
  "environment": "production"
}
```

**Example:**
```bash
curl http://localhost:1968/api/prep-release/health
```

```python
import requests

response = requests.get("http://localhost:1968/api/prep-release/health")
print(response.json())
```

---

### GET /health/detailed

Detailed health check with service status information.

**Response:**
```json
{
  "status": "healthy",
  "timestamp": "2025-10-13T14:30:00Z",
  "uptime_seconds": 3600.5,
  "version": "0.1.0",
  "services": {
    "simulation_service": "available",
    "location_service": "available"
  },
  "memory_usage": {
    "note": "Memory usage monitoring not yet implemented"
  },
  "system_info": {
    "api_framework": "FastAPI",
    "python_version": "3.12+",
    "tellus_version": "0.1.0",
    "api_version": "prep-release"
  }
}
```

**Example:**
```bash
curl http://localhost:1968/api/prep-release/health/detailed
```

---

### GET /version

Get detailed version information.

**Response:**
```json
{
  "tellus_version": "0.1.0",
  "api_version": "prep-release",
  "build_date": "2025-10-13",
  "git_commit": "779749b"
}
```

---

## Simulation Management

### GET /simulations

List all simulations with pagination and filtering.

**Query Parameters:**
- `page` (integer): Page number (default: 1)
- `page_size` (integer): Items per page (1-100, default: 50)
- `search` (string): Search term for simulation IDs

**Response:**
```json
{
  "simulations": [
    {
      "simulation_id": "PI_CTRL_01",
      "uid": "550e8400-e29b-41d4-a716-446655440000",
      "attributes": {
        "model": "ECHAM6",
        "experiment": "PI_CTRL",
        "resolution": "T63"
      },
      "locations": {
        "hpc_scratch": {
          "path_prefix": "/scratch/model/PI_CTRL_01"
        }
      },
      "namelists": {},
      "workflows": {}
    }
  ],
  "pagination": {
    "page": 1,
    "page_size": 50,
    "total_count": 100,
    "has_next": true,
    "has_previous": false
  },
  "filters_applied": {
    "search_term": null
  }
}
```

**Examples:**

```bash
# List all simulations
curl http://localhost:1968/api/prep-release/simulations

# Search for simulations
curl "http://localhost:1968/api/prep-release/simulations?search=CTRL&page=1&page_size=20"
```

```python
import requests

# List simulations with pagination
response = requests.get(
    "http://localhost:1968/api/prep-release/simulations",
    params={"page": 1, "page_size": 20}
)
simulations = response.json()

# Search simulations
response = requests.get(
    "http://localhost:1968/api/prep-release/simulations",
    params={"search": "CTRL"}
)
filtered = response.json()
```

---

### POST /simulations

Create a new simulation.

**Request Body:**
```json
{
  "simulation_id": "PI_CTRL_01",
  "model_id": "ECHAM6",
  "path": "/work/simulations/PI_CTRL_01",
  "attrs": {
    "experiment": "PI_CTRL",
    "resolution": "T63",
    "start_year": 1850
  },
  "namelists": {},
  "snakemakes": {}
}
```

**Response (201 Created):**
```json
{
  "simulation_id": "PI_CTRL_01",
  "uid": "550e8400-e29b-41d4-a716-446655440000",
  "attributes": {
    "model": "ECHAM6",
    "experiment": "PI_CTRL",
    "resolution": "T63"
  },
  "locations": {},
  "namelists": {},
  "workflows": {}
}
```

**Examples:**

```bash
curl -X POST http://localhost:1968/api/prep-release/simulations \
  -H "Content-Type: application/json" \
  -d '{
    "simulation_id": "PI_CTRL_01",
    "model_id": "ECHAM6",
    "attrs": {
      "experiment": "PI_CTRL",
      "resolution": "T63"
    }
  }'
```

```python
import requests

simulation_data = {
    "simulation_id": "PI_CTRL_01",
    "model_id": "ECHAM6",
    "attrs": {
        "experiment": "PI_CTRL",
        "resolution": "T63"
    }
}

response = requests.post(
    "http://localhost:1968/api/prep-release/simulations",
    json=simulation_data
)
created = response.json()
```

---

### GET /simulations/{simulation_id}

Get details of a specific simulation.

**Path Parameters:**
- `simulation_id` (string): The simulation identifier

**Response (200 OK):**
```json
{
  "simulation_id": "PI_CTRL_01",
  "uid": "550e8400-e29b-41d4-a716-446655440000",
  "attributes": {
    "model": "ECHAM6",
    "experiment": "PI_CTRL",
    "resolution": "T63"
  },
  "locations": {
    "hpc_scratch": {
      "path_prefix": "/scratch/model/PI_CTRL_01"
    }
  },
  "namelists": {},
  "workflows": {}
}
```

**Examples:**

```bash
curl http://localhost:1968/api/prep-release/simulations/PI_CTRL_01
```

```python
import requests

response = requests.get(
    "http://localhost:1968/api/prep-release/simulations/PI_CTRL_01"
)
simulation = response.json()
```

---

### PUT /simulations/{simulation_id}

Update an existing simulation.

**Path Parameters:**
- `simulation_id` (string): The simulation identifier

**Request Body:**
```json
{
  "model_id": "ECHAM6.3",
  "attrs": {
    "resolution": "T127"
  }
}
```

**Response (200 OK):**
```json
{
  "simulation_id": "PI_CTRL_01",
  "uid": "550e8400-e29b-41d4-a716-446655440000",
  "attributes": {
    "model": "ECHAM6.3",
    "resolution": "T127"
  },
  "locations": {},
  "namelists": {},
  "workflows": {}
}
```

**Examples:**

```bash
curl -X PUT http://localhost:1968/api/prep-release/simulations/PI_CTRL_01 \
  -H "Content-Type: application/json" \
  -d '{
    "attrs": {
      "resolution": "T127"
    }
  }'
```

```python
import requests

update_data = {
    "attrs": {
        "resolution": "T127"
    }
}

response = requests.put(
    "http://localhost:1968/api/prep-release/simulations/PI_CTRL_01",
    json=update_data
)
updated = response.json()
```

---

### DELETE /simulations/{simulation_id}

Delete a simulation.

**Path Parameters:**
- `simulation_id` (string): The simulation identifier

**Response (204 No Content):**
No response body.

**Examples:**

```bash
curl -X DELETE http://localhost:1968/api/prep-release/simulations/PI_CTRL_01
```

```python
import requests

response = requests.delete(
    "http://localhost:1968/api/prep-release/simulations/PI_CTRL_01"
)
# Check response.status_code == 204
```

---

### GET /simulations/{simulation_id}/attributes

Get all attributes of a simulation.

**Path Parameters:**
- `simulation_id` (string): The simulation identifier

**Response (200 OK):**
```json
{
  "simulation_id": "PI_CTRL_01",
  "attributes": {
    "model": "ECHAM6",
    "experiment": "PI_CTRL",
    "resolution": "T63",
    "start_year": 1850,
    "end_year": 2100
  }
}
```

**Examples:**

```bash
curl http://localhost:1968/api/prep-release/simulations/PI_CTRL_01/attributes
```

```python
import requests

response = requests.get(
    "http://localhost:1968/api/prep-release/simulations/PI_CTRL_01/attributes"
)
attributes = response.json()
```

---

### GET /simulations/{simulation_id}/attributes/{attribute_key}

Get a specific attribute value.

**Path Parameters:**
- `simulation_id` (string): The simulation identifier
- `attribute_key` (string): The attribute key

**Response (200 OK):**
```json
{
  "key": "resolution",
  "value": "T63"
}
```

**Examples:**

```bash
curl http://localhost:1968/api/prep-release/simulations/PI_CTRL_01/attributes/resolution
```

```python
import requests

response = requests.get(
    "http://localhost:1968/api/prep-release/simulations/PI_CTRL_01/attributes/resolution"
)
attribute = response.json()
```

---

### PUT /simulations/{simulation_id}/attributes/{attribute_key}

Set or update a specific attribute.

**Path Parameters:**
- `simulation_id` (string): The simulation identifier
- `attribute_key` (string): The attribute key

**Request Body:**
```json
{
  "key": "resolution",
  "value": "T127"
}
```

**Response (200 OK):**
```json
{
  "key": "resolution",
  "value": "T127"
}
```

**Examples:**

```bash
curl -X PUT http://localhost:1968/api/prep-release/simulations/PI_CTRL_01/attributes/resolution \
  -H "Content-Type: application/json" \
  -d '{
    "key": "resolution",
    "value": "T127"
  }'
```

```python
import requests

attribute_data = {
    "key": "resolution",
    "value": "T127"
}

response = requests.put(
    "http://localhost:1968/api/prep-release/simulations/PI_CTRL_01/attributes/resolution",
    json=attribute_data
)
updated_attr = response.json()
```

---

### POST /simulations/{simulation_id}/attributes

Add a new attribute to a simulation.

**Path Parameters:**
- `simulation_id` (string): The simulation identifier

**Request Body:**
```json
{
  "key": "ensemble_member",
  "value": "r1i1p1"
}
```

**Response (201 Created):**
```json
{
  "key": "ensemble_member",
  "value": "r1i1p1"
}
```

**Examples:**

```bash
curl -X POST http://localhost:1968/api/prep-release/simulations/PI_CTRL_01/attributes \
  -H "Content-Type: application/json" \
  -d '{
    "key": "ensemble_member",
    "value": "r1i1p1"
  }'
```

```python
import requests

attribute_data = {
    "key": "ensemble_member",
    "value": "r1i1p1"
}

response = requests.post(
    "http://localhost:1968/api/prep-release/simulations/PI_CTRL_01/attributes",
    json=attribute_data
)
new_attr = response.json()
```

---

## Location Associations

### POST /simulations/{simulation_id}/locations

Associate a simulation with one or more storage locations.

**Path Parameters:**
- `simulation_id` (string): The simulation identifier

**Request Body:**
```json
{
  "simulation_id": "PI_CTRL_01",
  "location_names": ["hpc_scratch", "archive_tape"],
  "context_overrides": {
    "hpc_scratch": {
      "path_prefix": "/scratch/model/{experiment}"
    }
  }
}
```

**Response (200 OK):**
```json
{
  "simulation_id": "PI_CTRL_01",
  "uid": "550e8400-e29b-41d4-a716-446655440000",
  "attributes": {},
  "locations": {
    "hpc_scratch": {
      "path_prefix": "/scratch/model/PI_CTRL"
    },
    "archive_tape": {}
  },
  "namelists": {},
  "workflows": {}
}
```

**Examples:**

```bash
curl -X POST http://localhost:1968/api/prep-release/simulations/PI_CTRL_01/locations \
  -H "Content-Type: application/json" \
  -d '{
    "simulation_id": "PI_CTRL_01",
    "location_names": ["hpc_scratch"]
  }'
```

```python
import requests

association_data = {
    "simulation_id": "PI_CTRL_01",
    "location_names": ["hpc_scratch", "archive_tape"]
}

response = requests.post(
    "http://localhost:1968/api/prep-release/simulations/PI_CTRL_01/locations",
    json=association_data
)
updated = response.json()
```

---

### DELETE /simulations/{simulation_id}/locations/{location_name}

Remove a location association from a simulation.

**Path Parameters:**
- `simulation_id` (string): The simulation identifier
- `location_name` (string): The location name to disassociate

**Response (200 OK):**
```json
{
  "simulation_id": "PI_CTRL_01",
  "uid": "550e8400-e29b-41d4-a716-446655440000",
  "attributes": {},
  "locations": {},
  "namelists": {},
  "workflows": {}
}
```

**Examples:**

```bash
curl -X DELETE http://localhost:1968/api/prep-release/simulations/PI_CTRL_01/locations/hpc_scratch
```

```python
import requests

response = requests.delete(
    "http://localhost:1968/api/prep-release/simulations/PI_CTRL_01/locations/hpc_scratch"
)
updated = response.json()
```

---

### PUT /simulations/{simulation_id}/locations/{location_name}/context

Update the context for a specific location association.

**Path Parameters:**
- `simulation_id` (string): The simulation identifier
- `location_name` (string): The location name

**Request Body:**
```json
{
  "context_overrides": {
    "path_prefix": "/new/path/{model}/{experiment}",
    "storage_tier": "fast"
  }
}
```

**Response (200 OK):**
```json
{
  "simulation_id": "PI_CTRL_01",
  "uid": "550e8400-e29b-41d4-a716-446655440000",
  "attributes": {},
  "locations": {
    "hpc_scratch": {
      "path_prefix": "/new/path/ECHAM6/PI_CTRL",
      "storage_tier": "fast"
    }
  },
  "namelists": {},
  "workflows": {}
}
```

**Examples:**

```bash
curl -X PUT http://localhost:1968/api/prep-release/simulations/PI_CTRL_01/locations/hpc_scratch/context \
  -H "Content-Type: application/json" \
  -d '{
    "context_overrides": {
      "path_prefix": "/new/path/{model}/{experiment}"
    }
  }'
```

```python
import requests

context_data = {
    "context_overrides": {
        "path_prefix": "/new/path/{model}/{experiment}",
        "storage_tier": "fast"
    }
}

response = requests.put(
    "http://localhost:1968/api/prep-release/simulations/PI_CTRL_01/locations/hpc_scratch/context",
    json=context_data
)
updated = response.json()
```

---

## File Management

### GET /simulations/{simulation_id}/files

List files associated with a simulation.

**Path Parameters:**
- `simulation_id` (string): The simulation identifier

**Query Parameters:**
- `location` (string): Filter by location name
- `content_type` (string): Filter by content type (output, restart, config, log)
- `file_type` (string): Filter by file type (regular, archive, directory)

**Response (200 OK):**
```json
{
  "simulation_id": "PI_CTRL_01",
  "files": [
    {
      "file_path": "output/atm/echam6_output_2025.nc",
      "location": "hpc_scratch",
      "size_bytes": 1073741824,
      "content_type": "output",
      "file_type": "regular",
      "created_at": "2025-10-13T10:00:00Z",
      "parent_file": "archive_2025.tar",
      "attributes": {
        "component": "echam6",
        "variable": "tas"
      }
    }
  ],
  "total_files": 1
}
```

**Examples:**

```bash
# List all files
curl http://localhost:1968/api/prep-release/simulations/PI_CTRL_01/files

# Filter by content type
curl "http://localhost:1968/api/prep-release/simulations/PI_CTRL_01/files?content_type=output"

# Filter by location
curl "http://localhost:1968/api/prep-release/simulations/PI_CTRL_01/files?location=hpc_scratch"
```

```python
import requests

# List all files
response = requests.get(
    "http://localhost:1968/api/prep-release/simulations/PI_CTRL_01/files"
)
files = response.json()

# Filter by content type
response = requests.get(
    "http://localhost:1968/api/prep-release/simulations/PI_CTRL_01/files",
    params={"content_type": "output"}
)
output_files = response.json()
```

---

### POST /simulations/{simulation_id}/files/register

Register files from an archive to a simulation.

**Path Parameters:**
- `simulation_id` (string): The simulation identifier

**Request Body:**
```json
{
  "archive_id": "archive_2025.tar",
  "content_type_filter": "output",
  "pattern_filter": "*.nc",
  "overwrite_existing": false
}
```

**Response (201 Created):**
```json
{
  "simulation_id": "PI_CTRL_01",
  "archive_id": "archive_2025.tar",
  "registered_count": 45,
  "updated_count": 0,
  "skipped_count": 3,
  "status": "completed"
}
```

**Examples:**

```bash
curl -X POST http://localhost:1968/api/prep-release/simulations/PI_CTRL_01/files/register \
  -H "Content-Type: application/json" \
  -d '{
    "archive_id": "archive_2025.tar",
    "content_type_filter": "output"
  }'
```

```python
import requests

registration_data = {
    "archive_id": "archive_2025.tar",
    "content_type_filter": "output",
    "pattern_filter": "*.nc"
}

response = requests.post(
    "http://localhost:1968/api/prep-release/simulations/PI_CTRL_01/files/register",
    json=registration_data
)
result = response.json()
```

---

### DELETE /simulations/{simulation_id}/files/unregister

Unregister files from a simulation.

**Path Parameters:**
- `simulation_id` (string): The simulation identifier

**Request Body:**
```json
{
  "archive_id": "archive_2025.tar",
  "content_type_filter": "output",
  "pattern_filter": "*.nc"
}
```

**Response (200 OK):**
```json
{
  "simulation_id": "PI_CTRL_01",
  "archive_id": "archive_2025.tar",
  "unregistered_count": 45,
  "status": "completed"
}
```

**Examples:**

```bash
curl -X DELETE http://localhost:1968/api/prep-release/simulations/PI_CTRL_01/files/unregister \
  -H "Content-Type: application/json" \
  -d '{
    "archive_id": "archive_2025.tar"
  }'
```

```python
import requests

unregistration_data = {
    "archive_id": "archive_2025.tar"
}

response = requests.delete(
    "http://localhost:1968/api/prep-release/simulations/PI_CTRL_01/files/unregister",
    json=unregistration_data
)
result = response.json()
```

---

### GET /simulations/{simulation_id}/files/status

Get file status summary for a simulation (similar to `git status`).

**Path Parameters:**
- `simulation_id` (string): The simulation identifier

**Response (200 OK):**
```json
{
  "simulation_id": "PI_CTRL_01",
  "total_files": 150,
  "files_by_archive": {
    "archive_2025.tar": 45,
    "archive_2024.tar": 50,
    "no_archive": 55
  },
  "files_by_content_type": {
    "output": 100,
    "restart": 30,
    "config": 20
  },
  "files_by_location": {
    "hpc_scratch": 120,
    "archive_tape": 30
  }
}
```

**Examples:**

```bash
curl http://localhost:1968/api/prep-release/simulations/PI_CTRL_01/files/status
```

```python
import requests

response = requests.get(
    "http://localhost:1968/api/prep-release/simulations/PI_CTRL_01/files/status"
)
status = response.json()
```

---

## Archive Management

### POST /simulations/{simulation_id}/archives

Create a new archive for a simulation.

**Path Parameters:**
- `simulation_id` (string): The simulation identifier

**Request Body:**
```json
{
  "archive_name": "output_2025",
  "description": "Model output for year 2025",
  "location": "hpc_scratch",
  "pattern": "output_2025*.nc",
  "split_parts": 1,
  "archive_type": "single"
}
```

**Response (201 Created):**
```json
{
  "archive_id": "output_2025.tar",
  "archive_name": "output_2025",
  "simulation_id": "PI_CTRL_01",
  "location": "hpc_scratch",
  "pattern": "output_2025*.nc",
  "split_parts": 1,
  "archive_type": "single",
  "description": "Model output for year 2025",
  "created_at": "2025-10-13T14:30:00Z"
}
```

**Examples:**

```bash
curl -X POST http://localhost:1968/api/prep-release/simulations/PI_CTRL_01/archives \
  -H "Content-Type: application/json" \
  -d '{
    "archive_name": "output_2025",
    "location": "hpc_scratch",
    "pattern": "output_2025*.nc"
  }'
```

```python
import requests

archive_data = {
    "archive_name": "output_2025",
    "description": "Model output for year 2025",
    "location": "hpc_scratch"
}

response = requests.post(
    "http://localhost:1968/api/prep-release/simulations/PI_CTRL_01/archives",
    json=archive_data
)
archive = response.json()
```

---

### GET /simulations/{simulation_id}/archives

List all archives for a simulation.

**Path Parameters:**
- `simulation_id` (string): The simulation identifier

**Response (200 OK):**
```json
{
  "simulation_id": "PI_CTRL_01",
  "archives": [
    {
      "archive_id": "output_2025.tar",
      "archive_name": "output_2025",
      "simulation_id": "PI_CTRL_01",
      "location": "hpc_scratch",
      "pattern": "output_2025*.nc",
      "split_parts": 1,
      "archive_type": "single",
      "description": "Model output for year 2025",
      "created_at": "2025-10-13T14:30:00Z"
    }
  ]
}
```

**Examples:**

```bash
curl http://localhost:1968/api/prep-release/simulations/PI_CTRL_01/archives
```

```python
import requests

response = requests.get(
    "http://localhost:1968/api/prep-release/simulations/PI_CTRL_01/archives"
)
archives = response.json()
```

---

### GET /simulations/{simulation_id}/archives/{archive_id}

Get details of a specific archive.

**Path Parameters:**
- `simulation_id` (string): The simulation identifier
- `archive_id` (string): The archive identifier

**Response (200 OK):**
```json
{
  "archive_id": "output_2025.tar",
  "archive_name": "output_2025",
  "simulation_id": "PI_CTRL_01",
  "location": "hpc_scratch",
  "pattern": "output_2025*.nc",
  "split_parts": 1,
  "archive_type": "single",
  "description": "Model output for year 2025",
  "created_at": "2025-10-13T14:30:00Z"
}
```

**Examples:**

```bash
curl http://localhost:1968/api/prep-release/simulations/PI_CTRL_01/archives/output_2025.tar
```

```python
import requests

response = requests.get(
    "http://localhost:1968/api/prep-release/simulations/PI_CTRL_01/archives/output_2025.tar"
)
archive = response.json()
```

---

### DELETE /simulations/{simulation_id}/archives/{archive_id}

Delete an archive.

**Path Parameters:**
- `simulation_id` (string): The simulation identifier
- `archive_id` (string): The archive identifier

**Response (200 OK):**
```json
{
  "archive_id": "output_2025.tar",
  "status": "deleted"
}
```

**Examples:**

```bash
curl -X DELETE http://localhost:1968/api/prep-release/simulations/PI_CTRL_01/archives/output_2025.tar
```

```python
import requests

response = requests.delete(
    "http://localhost:1968/api/prep-release/simulations/PI_CTRL_01/archives/output_2025.tar"
)
result = response.json()
```

---

### GET /simulations/{simulation_id}/archives/{archive_id}/contents

List contents of an archive without extraction.

**Path Parameters:**
- `simulation_id` (string): The simulation identifier
- `archive_id` (string): The archive identifier

**Query Parameters:**
- `file_filter` (string): Filter files by pattern (e.g., '*.nc')
- `content_type_filter` (string): Filter by content type

**Response (200 OK):**
```json
{
  "archive_id": "output_2025.tar",
  "files": [
    {
      "file_path": "output/atm/echam6_output_2025.nc",
      "size_bytes": 1073741824,
      "content_type": "output",
      "file_type": "regular",
      "created_at": "2025-10-13T10:00:00Z",
      "attributes": {}
    }
  ],
  "total_files": 1
}
```

**Examples:**

```bash
# List all contents
curl http://localhost:1968/api/prep-release/simulations/PI_CTRL_01/archives/output_2025.tar/contents

# Filter by pattern
curl "http://localhost:1968/api/prep-release/simulations/PI_CTRL_01/archives/output_2025.tar/contents?file_filter=*.nc"
```

```python
import requests

# List all contents
response = requests.get(
    "http://localhost:1968/api/prep-release/simulations/PI_CTRL_01/archives/output_2025.tar/contents"
)
contents = response.json()

# Filter by content type
response = requests.get(
    "http://localhost:1968/api/prep-release/simulations/PI_CTRL_01/archives/output_2025.tar/contents",
    params={"content_type_filter": "output"}
)
filtered_contents = response.json()
```

---

### POST /simulations/{simulation_id}/archives/{archive_id}/index

Create content index for an archive.

**Path Parameters:**
- `simulation_id` (string): The simulation identifier
- `archive_id` (string): The archive identifier

**Request Body:**
```json
{
  "force": false
}
```

**Response (200 OK):**
```json
{
  "archive_id": "output_2025.tar",
  "status": "indexed",
  "files_indexed": 45
}
```

**Examples:**

```bash
curl -X POST http://localhost:1968/api/prep-release/simulations/PI_CTRL_01/archives/output_2025.tar/index \
  -H "Content-Type: application/json" \
  -d '{"force": false}'
```

```python
import requests

response = requests.post(
    "http://localhost:1968/api/prep-release/simulations/PI_CTRL_01/archives/output_2025.tar/index",
    json={"force": False}
)
result = response.json()
```

---

## Location Management

### GET /locations

List all storage locations with pagination and filtering.

**Query Parameters:**
- `page` (integer): Page number (default: 1)
- `page_size` (integer): Items per page (1-100, default: 50)
- `search` (string): Search term for location names
- `kind` (string): Filter by location kind (DISK, COMPUTE, TAPE, FILESERVER)

**Response (200 OK):**
```json
{
  "locations": [
    {
      "name": "hpc_scratch",
      "kinds": ["COMPUTE", "DISK"],
      "protocol": "file",
      "path": "/scratch/model",
      "storage_options": {},
      "additional_config": {},
      "is_remote": false,
      "is_accessible": true,
      "last_verified": "2025-10-13T14:00:00Z"
    }
  ],
  "pagination": {
    "page": 1,
    "page_size": 50,
    "total_count": 10,
    "has_next": false,
    "has_previous": false
  },
  "filters_applied": {
    "search_term": null
  }
}
```

**Examples:**

```bash
# List all locations
curl http://localhost:1968/api/prep-release/locations

# Filter by kind
curl "http://localhost:1968/api/prep-release/locations?kind=DISK"

# Search locations
curl "http://localhost:1968/api/prep-release/locations?search=scratch"
```

```python
import requests

# List all locations
response = requests.get("http://localhost:1968/api/prep-release/locations")
locations = response.json()

# Filter by kind
response = requests.get(
    "http://localhost:1968/api/prep-release/locations",
    params={"kind": "DISK"}
)
disk_locations = response.json()
```

---

### POST /locations

Create a new storage location.

**Request Body:**
```json
{
  "name": "hpc_scratch",
  "kinds": ["COMPUTE", "DISK"],
  "protocol": "file",
  "path": "/scratch/model",
  "storage_options": {},
  "additional_config": {
    "quota_gb": 1000
  }
}
```

**Response (201 Created):**
```json
{
  "name": "hpc_scratch",
  "kinds": ["COMPUTE", "DISK"],
  "protocol": "file",
  "path": "/scratch/model",
  "storage_options": {},
  "additional_config": {
    "quota_gb": 1000
  },
  "is_remote": false,
  "is_accessible": null,
  "last_verified": null
}
```

**Examples:**

```bash
curl -X POST http://localhost:1968/api/prep-release/locations \
  -H "Content-Type: application/json" \
  -d '{
    "name": "hpc_scratch",
    "kinds": ["COMPUTE", "DISK"],
    "protocol": "file",
    "path": "/scratch/model"
  }'
```

```python
import requests

location_data = {
    "name": "hpc_scratch",
    "kinds": ["COMPUTE", "DISK"],
    "protocol": "file",
    "path": "/scratch/model"
}

response = requests.post(
    "http://localhost:1968/api/prep-release/locations",
    json=location_data
)
created = response.json()
```

---

### GET /locations/{location_name}

Get details of a specific location.

**Path Parameters:**
- `location_name` (string): The location name

**Response (200 OK):**
```json
{
  "name": "hpc_scratch",
  "kinds": ["COMPUTE", "DISK"],
  "protocol": "file",
  "path": "/scratch/model",
  "storage_options": {},
  "additional_config": {
    "quota_gb": 1000
  },
  "is_remote": false,
  "is_accessible": true,
  "last_verified": "2025-10-13T14:00:00Z"
}
```

**Examples:**

```bash
curl http://localhost:1968/api/prep-release/locations/hpc_scratch
```

```python
import requests

response = requests.get(
    "http://localhost:1968/api/prep-release/locations/hpc_scratch"
)
location = response.json()
```

---

### PUT /locations/{location_name}

Update an existing location.

**Path Parameters:**
- `location_name` (string): The location name

**Request Body:**
```json
{
  "path": "/new/scratch/path",
  "config": {
    "quota_gb": 2000
  }
}
```

**Response (200 OK):**
```json
{
  "name": "hpc_scratch",
  "kinds": ["COMPUTE", "DISK"],
  "protocol": "file",
  "path": "/new/scratch/path",
  "storage_options": {},
  "additional_config": {
    "quota_gb": 2000
  },
  "is_remote": false,
  "is_accessible": true,
  "last_verified": "2025-10-13T14:00:00Z"
}
```

**Examples:**

```bash
curl -X PUT http://localhost:1968/api/prep-release/locations/hpc_scratch \
  -H "Content-Type: application/json" \
  -d '{
    "path": "/new/scratch/path"
  }'
```

```python
import requests

update_data = {
    "path": "/new/scratch/path",
    "config": {"quota_gb": 2000}
}

response = requests.put(
    "http://localhost:1968/api/prep-release/locations/hpc_scratch",
    json=update_data
)
updated = response.json()
```

---

### DELETE /locations/{location_name}

Delete a storage location.

**Path Parameters:**
- `location_name` (string): The location name

**Response (204 No Content):**
No response body.

**Examples:**

```bash
curl -X DELETE http://localhost:1968/api/prep-release/locations/hpc_scratch
```

```python
import requests

response = requests.delete(
    "http://localhost:1968/api/prep-release/locations/hpc_scratch"
)
# Check response.status_code == 204
```

---

### POST /locations/{location_name}/test

Test connectivity to a storage location.

**Path Parameters:**
- `location_name` (string): The location name to test

**Response (200 OK):**
```json
{
  "location_name": "hpc_scratch",
  "success": true,
  "error_message": null,
  "latency_ms": 45.2,
  "available_space": 1099511627776,
  "protocol_specific_info": {
    "protocol": "file",
    "test_performed": "basic_connectivity",
    "timestamp": "2025-10-13T14:30:00Z"
  }
}
```

**Examples:**

```bash
curl -X POST http://localhost:1968/api/prep-release/locations/hpc_scratch/test
```

```python
import requests

response = requests.post(
    "http://localhost:1968/api/prep-release/locations/hpc_scratch/test"
)
test_result = response.json()
```

---

## Workflow Management

### GET /workflows

List all workflows with pagination and filtering.

**Query Parameters:**
- `page` (integer): Page number (default: 1)
- `page_size` (integer): Items per page (1-100, default: 50)
- `search` (string): Search term for workflow names
- `simulation_id` (string): Filter by simulation ID
- `workflow_status` (string): Filter by status (DRAFT, READY, DEPRECATED, ARCHIVED)

**Response (200 OK):**
```json
{
  "workflows": [
    {
      "workflow_id": "esm_postproc_v1",
      "uid": "660e8400-e29b-41d4-a716-446655440000",
      "name": "ESM Post-Processing Pipeline",
      "description": "Standard post-processing for Earth System Model output",
      "engine": "snakemake",
      "workflow_file": "/path/to/Snakefile",
      "steps": [],
      "global_parameters": {
        "resolution": "T63"
      },
      "input_schema": {},
      "output_schema": {},
      "tags": ["production", "esm"],
      "version": "1.0",
      "author": null,
      "created_at": "2025-10-13T14:00:00Z"
    }
  ],
  "pagination": {
    "page": 1,
    "page_size": 50,
    "total_count": 5,
    "has_next": false,
    "has_previous": false
  },
  "filters_applied": {
    "search_term": null
  }
}
```

**Examples:**

```bash
# List all workflows
curl http://localhost:1968/api/prep-release/workflows

# Filter by simulation
curl "http://localhost:1968/api/prep-release/workflows?simulation_id=PI_CTRL_01"

# Search workflows
curl "http://localhost:1968/api/prep-release/workflows?search=postproc"
```

```python
import requests

# List all workflows
response = requests.get("http://localhost:1968/api/prep-release/workflows")
workflows = response.json()

# Filter by simulation
response = requests.get(
    "http://localhost:1968/api/prep-release/workflows",
    params={"simulation_id": "PI_CTRL_01"}
)
filtered = response.json()
```

---

### POST /workflows

Create a new workflow.

**Request Body:**
```json
{
  "workflow_id": "esm_postproc_v1",
  "name": "ESM Post-Processing Pipeline",
  "description": "Standard post-processing for Earth System Model output",
  "engine": "snakemake",
  "workflow_file": "/path/to/Snakefile",
  "global_parameters": {
    "resolution": "T63",
    "output_format": "netcdf4"
  },
  "tags": ["production", "esm", "post-processing"]
}
```

**Response (201 Created):**
```json
{
  "workflow_id": "esm_postproc_v1",
  "uid": "660e8400-e29b-41d4-a716-446655440000",
  "name": "ESM Post-Processing Pipeline",
  "description": "Standard post-processing for Earth System Model output",
  "engine": "snakemake",
  "workflow_file": "/path/to/Snakefile",
  "steps": [],
  "global_parameters": {
    "resolution": "T63",
    "output_format": "netcdf4"
  },
  "input_schema": {},
  "output_schema": {},
  "tags": ["production", "esm", "post-processing"],
  "version": "1.0",
  "author": null,
  "created_at": "2025-10-13T14:30:00Z"
}
```

**Examples:**

```bash
curl -X POST http://localhost:1968/api/prep-release/workflows \
  -H "Content-Type: application/json" \
  -d '{
    "workflow_id": "esm_postproc_v1",
    "name": "ESM Post-Processing Pipeline",
    "engine": "snakemake"
  }'
```

```python
import requests

workflow_data = {
    "workflow_id": "esm_postproc_v1",
    "name": "ESM Post-Processing Pipeline",
    "engine": "snakemake",
    "global_parameters": {
        "resolution": "T63"
    }
}

response = requests.post(
    "http://localhost:1968/api/prep-release/workflows",
    json=workflow_data
)
created = response.json()
```

---

### GET /workflows/{workflow_id}

Get details of a specific workflow.

**Path Parameters:**
- `workflow_id` (string): The workflow identifier

**Response (200 OK):**
```json
{
  "workflow_id": "esm_postproc_v1",
  "uid": "660e8400-e29b-41d4-a716-446655440000",
  "name": "ESM Post-Processing Pipeline",
  "description": "Standard post-processing for Earth System Model output",
  "engine": "snakemake",
  "workflow_file": "/path/to/Snakefile",
  "steps": [],
  "global_parameters": {
    "resolution": "T63"
  },
  "input_schema": {},
  "output_schema": {},
  "tags": ["production", "esm"],
  "version": "1.0",
  "author": null,
  "created_at": "2025-10-13T14:00:00Z"
}
```

**Examples:**

```bash
curl http://localhost:1968/api/prep-release/workflows/esm_postproc_v1
```

```python
import requests

response = requests.get(
    "http://localhost:1968/api/prep-release/workflows/esm_postproc_v1"
)
workflow = response.json()
```

---

### PUT /workflows/{workflow_id}

Update an existing workflow.

**Path Parameters:**
- `workflow_id` (string): The workflow identifier

**Request Body:**
```json
{
  "name": "Updated Workflow Name",
  "description": "Updated description",
  "global_parameters": {
    "new_param": "value"
  }
}
```

**Response (200 OK):**
```json
{
  "workflow_id": "esm_postproc_v1",
  "uid": "660e8400-e29b-41d4-a716-446655440000",
  "name": "Updated Workflow Name",
  "description": "Updated description",
  "engine": "snakemake",
  "workflow_file": "/path/to/Snakefile",
  "steps": [],
  "global_parameters": {
    "new_param": "value"
  },
  "input_schema": {},
  "output_schema": {},
  "tags": [],
  "version": "1.0",
  "author": null,
  "created_at": "2025-10-13T14:00:00Z"
}
```

**Examples:**

```bash
curl -X PUT http://localhost:1968/api/prep-release/workflows/esm_postproc_v1 \
  -H "Content-Type: application/json" \
  -d '{
    "description": "Updated description"
  }'
```

```python
import requests

update_data = {
    "description": "Updated description",
    "global_parameters": {"new_param": "value"}
}

response = requests.put(
    "http://localhost:1968/api/prep-release/workflows/esm_postproc_v1",
    json=update_data
)
updated = response.json()
```

---

### DELETE /workflows/{workflow_id}

Delete a workflow.

**Path Parameters:**
- `workflow_id` (string): The workflow identifier

**Response (204 No Content):**
No response body.

**Examples:**

```bash
curl -X DELETE http://localhost:1968/api/prep-release/workflows/esm_postproc_v1
```

```python
import requests

response = requests.delete(
    "http://localhost:1968/api/prep-release/workflows/esm_postproc_v1"
)
# Check response.status_code == 204
```

---

## Common Usage Patterns

### Creating a Complete Simulation Workflow

```python
import requests

base_url = "http://localhost:1968/api/prep-release"

# 1. Create a simulation
simulation_data = {
    "simulation_id": "PI_CTRL_01",
    "model_id": "ECHAM6",
    "attrs": {
        "experiment": "PI_CTRL",
        "resolution": "T63"
    }
}
sim_response = requests.post(f"{base_url}/simulations", json=simulation_data)
simulation = sim_response.json()

# 2. Create locations
location_data = {
    "name": "hpc_scratch",
    "kinds": ["COMPUTE", "DISK"],
    "protocol": "file",
    "path": "/scratch/model"
}
loc_response = requests.post(f"{base_url}/locations", json=location_data)

# 3. Associate simulation with location
association_data = {
    "simulation_id": "PI_CTRL_01",
    "location_names": ["hpc_scratch"]
}
requests.post(
    f"{base_url}/simulations/PI_CTRL_01/locations",
    json=association_data
)

# 4. Create workflow
workflow_data = {
    "workflow_id": "postproc_v1",
    "name": "Post-Processing",
    "engine": "snakemake"
}
wf_response = requests.post(f"{base_url}/workflows", json=workflow_data)

# 5. Create archive
archive_data = {
    "archive_name": "output_2025",
    "location": "hpc_scratch"
}
arc_response = requests.post(
    f"{base_url}/simulations/PI_CTRL_01/archives",
    json=archive_data
)

print("Setup complete!")
```

### Searching and Filtering

```python
import requests

base_url = "http://localhost:1968/api/prep-release"

# Search simulations
response = requests.get(
    f"{base_url}/simulations",
    params={"search": "CTRL", "page": 1, "page_size": 20}
)
simulations = response.json()

# Filter locations by kind
response = requests.get(
    f"{base_url}/locations",
    params={"kind": "DISK"}
)
disk_locations = response.json()

# Get simulation files by content type
response = requests.get(
    f"{base_url}/simulations/PI_CTRL_01/files",
    params={"content_type": "output"}
)
output_files = response.json()
```

### Error Handling

```python
import requests

base_url = "http://localhost:1968/api/prep-release"

try:
    response = requests.post(
        f"{base_url}/simulations",
        json={"simulation_id": "TEST_01"}
    )
    response.raise_for_status()
    simulation = response.json()

except requests.exceptions.HTTPError as e:
    if response.status_code == 400:
        print(f"Bad request: {response.json()['detail']}")
    elif response.status_code == 404:
        print(f"Not found: {response.json()['detail']}")
    elif response.status_code == 409:
        print(f"Conflict: {response.json()['detail']}")
    else:
        print(f"Error: {e}")

except requests.exceptions.RequestException as e:
    print(f"Connection error: {e}")
```

---

## Next Steps

- See [DEPLOYMENT.md](/Users/pgierz/work/Code/worktree-checkouts/github.com/pgierz/tellus/prep-release/DEPLOYMENT.md) for production deployment guide
- Visit the interactive API documentation at http://localhost:1968/docs
- Explore the ReDoc documentation at http://localhost:1968/redoc
- View the OpenAPI schema at http://localhost:1968/openapi.json

---

## Support and Resources

- **GitHub Repository**: https://github.com/pgierz/tellus
- **Issue Tracker**: https://github.com/pgierz/tellus/issues
- **Health Check**: http://localhost:1968/api/prep-release/health
- **Interactive Docs**: http://localhost:1968/docs
