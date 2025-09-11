"""
FastAPI test fixtures and configuration.

Provides shared fixtures for testing the Tellus REST API including
test client setup, mock services, and test data.
"""

import pytest
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch
from fastapi.testclient import TestClient
from typing import Generator, Dict, Any, List

from tellus.interfaces.web.main import create_app
from tellus.application.container import ServiceContainer
from tellus.application.dtos import (
    SimulationDto, LocationDto, CreateSimulationDto, CreateLocationDto,
    UpdateSimulationDto, UpdateLocationDto, LocationTestResult,
    SimulationListDto, LocationListDto, PaginationInfo, FilterOptions
)


@pytest.fixture
def mock_simulation_service():
    """Mock simulation service for testing."""
    service = MagicMock()
    
    # Mock data
    mock_simulations = [
        SimulationDto(
            simulation_id="test-sim-1",
            uid="123e4567-e89b-12d3-a456-426614174001",
            attributes={"model": "FESOM2", "experiment": "PI"},
            locations={"local": {}},
            namelists={"ocean": {"dt": 3600}},
            workflows={"postprocess": "snakemake/postprocess.smk"}
        ),
        SimulationDto(
            simulation_id="test-sim-2", 
            uid="123e4567-e89b-12d3-a456-426614174002",
            attributes={"model": "ICON", "experiment": "RCP85"},
            locations={"cluster": {}},
            namelists={},
            workflows={}
        ),
        SimulationDto(
            simulation_id="sim-001",
            uid="123e4567-e89b-12d3-a456-426614174003",
            attributes={"model": "FESOM", "resolution": "T127", "experiment": "Historical"},
            locations={"local": {}},
            namelists={},
            workflows={}
        )
    ]
    
    # Configure mock methods - handle parameters properly
    def mock_list_simulations(page=1, page_size=50, filters=None):
        # Apply search filter if provided
        filtered_sims = mock_simulations
        if filters and filters.search_term:
            filtered_sims = [
                s for s in mock_simulations 
                if filters.search_term.lower() in s.simulation_id.lower()
            ]
        
        # Apply pagination
        start_idx = (page - 1) * page_size
        end_idx = start_idx + page_size
        paginated_sims = filtered_sims[start_idx:end_idx]
        
        total_count = len(filtered_sims)
        has_next = end_idx < total_count
        has_previous = page > 1
        
        return SimulationListDto(
            simulations=paginated_sims,
            pagination=PaginationInfo(
                page=page,
                page_size=page_size,
                total_count=total_count,
                has_next=has_next,
                has_previous=has_previous
            ),
            filters_applied=filters or FilterOptions()
        )
    
    service.list_simulations.side_effect = mock_list_simulations
    
    def mock_create_simulation(dto):
        # Check for duplicate simulation_id
        existing_ids = [s.simulation_id for s in mock_simulations]
        if dto.simulation_id in existing_ids:
            raise Exception(f"Simulation '{dto.simulation_id}' already exists")
        
        # Validate required fields (simulation_id is required)
        if not dto.simulation_id or dto.simulation_id.strip() == "":
            raise ValueError("simulation_id is required")
        
        return SimulationDto(
            simulation_id=dto.simulation_id,
            uid="new-uid-123",
            attributes=dto.attrs if hasattr(dto, 'attrs') else {},
            locations={},
            namelists=dto.namelists if hasattr(dto, 'namelists') else {},
            workflows=dto.snakemakes if hasattr(dto, 'snakemakes') else {}
        )
    
    service.create_simulation.side_effect = mock_create_simulation
    
    def mock_get_simulation(sim_id):
        sim = next((s for s in mock_simulations if s.simulation_id == sim_id), None)
        if sim is None:
            raise Exception(f"Simulation '{sim_id}' not found")
        return sim
    
    service.get_simulation.side_effect = mock_get_simulation
    
    def mock_update_simulation(sim_id, dto):
        sim = next((s for s in mock_simulations if s.simulation_id == sim_id), None)
        if sim is None:
            raise Exception(f"Simulation '{sim_id}' not found")
        # Return the existing simulation (in reality this would be updated)
        return sim
    
    service.update_simulation.side_effect = mock_update_simulation
    
    def mock_delete_simulation(sim_id):
        sim = next((s for s in mock_simulations if s.simulation_id == sim_id), None)
        if sim is None:
            raise Exception(f"Simulation '{sim_id}' not found")
        # In reality, we'd remove from the list, but for tests just return None
        return None
    
    service.delete_simulation.side_effect = mock_delete_simulation
    
    # Mock attributes methods for new REST API endpoints
    def mock_get_simulation_attributes(sim_id):
        sim = next((s for s in mock_simulations if s.simulation_id == sim_id), None)
        if sim is None:
            raise Exception(f"Simulation '{sim_id}' not found")
        return {
            "simulation_id": sim_id,
            "attributes": sim.attributes
        }
    
    def mock_get_simulation_attribute(sim_id, attribute_key):
        sim = next((s for s in mock_simulations if s.simulation_id == sim_id), None)
        if sim is None:
            raise Exception(f"Simulation '{sim_id}' not found")
        if attribute_key not in sim.attributes:
            raise Exception(f"Attribute '{attribute_key}' not found")
        return {
            "key": attribute_key,
            "value": sim.attributes[attribute_key]
        }
    
    def mock_set_simulation_attribute(sim_id, attribute_key, value):
        sim = next((s for s in mock_simulations if s.simulation_id == sim_id), None)
        if sim is None:
            raise Exception(f"Simulation '{sim_id}' not found")
        # In reality we'd update the simulation's attributes
        return {
            "key": attribute_key,
            "value": value
        }
    
    def mock_add_simulation_attribute(sim_id, attribute_key, value):
        sim = next((s for s in mock_simulations if s.simulation_id == sim_id), None)
        if sim is None:
            raise Exception(f"Simulation '{sim_id}' not found")
        # In reality we'd add to the simulation's attributes
        return {
            "key": attribute_key,
            "value": value
        }
    
    # Mock location association methods
    def mock_associate_simulation_locations(sim_id, location_names, context_overrides=None):
        sim = next((s for s in mock_simulations if s.simulation_id == sim_id), None)
        if sim is None:
            raise Exception(f"Simulation '{sim_id}' not found")
        # Return the simulation (in reality we'd update location associations)
        return sim
    
    def mock_disassociate_simulation_from_location(sim_id, location_name):
        sim = next((s for s in mock_simulations if s.simulation_id == sim_id), None)
        if sim is None:
            raise Exception(f"Simulation '{sim_id}' not found")
        return sim
    
    def mock_update_simulation_location_context(simulation_id, location_name, context_overrides):
        sim = next((s for s in mock_simulations if s.simulation_id == simulation_id), None)
        if sim is None:
            raise Exception(f"Simulation '{simulation_id}' not found")
        return sim
    
    # Mock files methods
    def mock_get_simulation_files(sim_id):
        sim = next((s for s in mock_simulations if s.simulation_id == sim_id), None)
        if sim is None:
            raise Exception(f"Simulation '{sim_id}' not found")
        return []  # Mock empty file list (router expects list, not dict)
    
    # Add these methods to the mock service
    service.get_simulation_attributes.side_effect = mock_get_simulation_attributes
    service.get_simulation_attribute.side_effect = mock_get_simulation_attribute
    service.set_simulation_attribute.side_effect = mock_set_simulation_attribute
    service.add_simulation_attribute.side_effect = mock_add_simulation_attribute
    service.associate_simulation_locations.side_effect = mock_associate_simulation_locations
    service.disassociate_simulation_from_location.side_effect = mock_disassociate_simulation_from_location
    service.update_simulation_location_context.side_effect = mock_update_simulation_location_context
    service.get_simulation_files.side_effect = mock_get_simulation_files
    
    return service


@pytest.fixture 
def mock_location_service():
    """Mock location service for testing."""
    service = MagicMock()
    
    # Mock data
    mock_locations = [
        LocationDto(
            name="local-storage",
            kinds=["DISK"],
            protocol="file",
            path="/tmp/tellus",
            storage_options={},
            additional_config={},
            is_remote=False,
            is_accessible=True
        ),
        LocationDto(
            name="cluster-storage",
            kinds=["COMPUTE", "DISK"],
            protocol="sftp",
            path="/work/user/data",
            storage_options={"host": "cluster.example.com", "username": "user"},
            additional_config={},
            is_remote=True,
            is_accessible=True
        )
    ]
    
    # Configure mock methods - handle parameters properly
    def mock_list_locations(page=1, page_size=50, filters=None):
        # Apply search filter if provided
        filtered_locs = mock_locations
        if filters and filters.search_term:
            filtered_locs = [
                loc for loc in mock_locations 
                if filters.search_term.lower() in loc.name.lower()
            ]
        
        # Apply pagination
        start_idx = (page - 1) * page_size
        end_idx = start_idx + page_size
        paginated_locs = filtered_locs[start_idx:end_idx]
        
        total_count = len(filtered_locs)
        has_next = end_idx < total_count
        has_previous = page > 1
        
        return LocationListDto(
            locations=paginated_locs,
            pagination=PaginationInfo(
                page=page,
                page_size=page_size,
                total_count=total_count,
                has_next=has_next,
                has_previous=has_previous
            ),
            filters_applied=filters or FilterOptions()
        )
    
    service.list_locations.side_effect = mock_list_locations
    
    def mock_create_location(dto):
        # Check for duplicate location name
        existing_names = [loc.name for loc in mock_locations]
        if dto.name in existing_names:
            raise Exception(f"Location '{dto.name}' already exists")
        
        # Validate required fields
        if not dto.name or dto.name.strip() == "":
            raise ValueError("name is required")
        
        return LocationDto(
            name=dto.name,
            kinds=dto.kinds if hasattr(dto, 'kinds') else ["DISK"],
            protocol=dto.protocol if hasattr(dto, 'protocol') else "file",
            path=dto.path if hasattr(dto, 'path') else "/tmp",
            storage_options=dto.storage_options if hasattr(dto, 'storage_options') else {},
            additional_config=dto.additional_config if hasattr(dto, 'additional_config') else {},
            is_remote=(dto.protocol if hasattr(dto, 'protocol') else "file") != "file",
            is_accessible=True
        )
    
    service.create_location.side_effect = mock_create_location
    
    def mock_get_location(name):
        loc = next((loc for loc in mock_locations if loc.name == name), None)
        if loc is None:
            raise Exception(f"Location '{name}' not found")
        return loc
    
    service.get_location.side_effect = mock_get_location
    
    def mock_update_location(name, dto):
        loc = next((loc for loc in mock_locations if loc.name == name), None)
        if loc is None:
            raise Exception(f"Location '{name}' not found")
        # Return the existing location (in reality this would be updated)
        return loc
    
    service.update_location.side_effect = mock_update_location
    
    def mock_delete_location(name):
        loc = next((loc for loc in mock_locations if loc.name == name), None)
        if loc is None:
            raise Exception(f"Location '{name}' not found")
        # In reality, we'd remove from the list, but for tests just return None
        return None
    
    service.delete_location.side_effect = mock_delete_location
    
    def mock_test_location_connectivity(name):
        # First check if location exists
        loc = next((loc for loc in mock_locations if loc.name == name), None)
        if loc is None:
            raise Exception(f"Location '{name}' not found")
        
        # Return mock test result
        from tellus.application.dtos import LocationTestResult
        return LocationTestResult(
            location_name=name,
            success=True,
            latency_ms=45.2,
            protocol_specific_info={
                "protocol": loc.protocol,
                "test_performed": "basic_connectivity",
                "timestamp": "2025-01-04T10:00:00Z"
            }
        )
    
    # Add this if the service has a test method
    if hasattr(service, 'test_location_connectivity'):
        service.test_location_connectivity.side_effect = mock_test_location_connectivity
    
    return service


@pytest.fixture
def mock_file_service():
    """Mock unified file service for testing."""
    service = MagicMock()
    
    # Default mock implementations that can be overridden in specific tests
    def mock_create_archive(dto):
        # Default implementation - tests can override this
        from tellus.domain.entities.simulation_file import SimulationFile, FileType
        from datetime import datetime
        return SimulationFile(
            relative_path="default_archive",
            file_type=FileType.ARCHIVE,
            created_time=datetime.now().timestamp()
        )
    
    def mock_list_simulation_archives(sim_id):
        # Default empty list - tests can override
        return []
    
    def mock_get_archive(archive_id):
        # Default None - tests can override
        return None
    
    def mock_remove_file(file_id):
        # Default success - tests can override
        return None
    
    def mock_get_file_children(parent_id):
        # Default empty list - tests can override
        return []
    
    def mock_get_simulation_files(sim_id):
        # Default empty list - tests can override
        return []
    
    def mock_register_files_to_simulation(dto):
        # Default success result - tests can override
        from tellus.application.dtos import FileRegistrationResultDto
        return FileRegistrationResultDto(
            registered_count=0,
            updated_count=0,
            skipped_count=0
        )
    
    # Set up default mock behavior
    service.create_archive.side_effect = mock_create_archive
    service.list_simulation_archives.side_effect = mock_list_simulation_archives
    service.get_archive.side_effect = mock_get_archive
    service.remove_file.side_effect = mock_remove_file
    service.get_file_children.side_effect = mock_get_file_children
    service.get_simulation_files.side_effect = mock_get_simulation_files
    service.register_files_to_simulation.side_effect = mock_register_files_to_simulation
    
    return service


@pytest.fixture
def mock_service_container(mock_simulation_service, mock_location_service, mock_file_service):
    """Mock service container for dependency injection."""
    container = MagicMock(spec=ServiceContainer)
    
    # Mock the service factory
    mock_factory = MagicMock()
    mock_factory.simulation_service = mock_simulation_service
    mock_factory.location_service = mock_location_service
    mock_factory.unified_file_service = mock_file_service
    container.service_factory = mock_factory
    
    return container


@pytest.fixture
def test_app(mock_service_container):
    """FastAPI test application with mocked services."""
    # Create app without lifespan to avoid real container initialization
    from fastapi import FastAPI
    from tellus.interfaces.web.routers import health, simulations, locations
    from fastapi.middleware.cors import CORSMiddleware
    
    # Get dynamic version information to match production
    from tellus.interfaces.web.version import get_version_info
    version_info = get_version_info()
    api_version = version_info["api_version"]
    api_path = f"/api/{api_version}"
    
    app = FastAPI(
        title="Tellus Climate Data API",
        version=version_info["tellus_version"],
        description="REST API for Tellus - the distributed data management system for Earth System Model simulations.",
        docs_url=f"{api_path}/docs",
        redoc_url=f"{api_path}/redoc",
        openapi_url=f"{api_path}/openapi.json"
    )
    
    # Add CORS middleware
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    
    # Include routers with versioned API prefix to match production
    app.include_router(health.router, prefix=api_path, tags=["Health"])
    app.include_router(simulations.router, prefix=f"{api_path}/simulations", tags=["simulations"])
    app.include_router(locations.router, prefix=f"{api_path}/locations", tags=["locations"])
    
    # Set the mock container BEFORE any lifespan events
    app.state.container = mock_service_container
    
    return app


@pytest.fixture
def client(test_app) -> Generator[TestClient, None, None]:
    """FastAPI test client."""
    with TestClient(test_app) as test_client:
        yield test_client


@pytest.fixture
def sample_simulation_data():
    """Sample simulation data for testing."""
    return {
        "simulation_id": "new-test-sim",
        "model_id": "FESOM2",
        "path": "/data/simulations/new-test-sim",
        "attrs": {
            "experiment": "Historical",
            "resolution": "T63",
            "years": "1850-2014"
        },
        "namelists": {
            "ocean": {"dt": 1800, "mixing": "pp"},
            "atmos": {"timestep": 600}
        },
        "snakemakes": {
            "postprocess": "workflows/post.smk"
        }
    }


@pytest.fixture
def sample_location_data():
    """Sample location data for testing."""
    return {
        "name": "new-test-location",
        "kinds": ["DISK", "COMPUTE"],
        "protocol": "sftp",
        "path": "/scratch/climate/data",
        "storage_options": {
            "host": "hpc.example.com",
            "username": "climate_user",
            "port": 22
        },
        "additional_config": {
            "max_connections": 5,
            "timeout": 30
        }
    }


@pytest.fixture
def temp_project_dir():
    """Temporary directory set up as a tellus project."""
    with tempfile.TemporaryDirectory() as temp_dir:
        project_path = Path(temp_dir)
        
        # Create basic project structure
        (project_path / "simulations.json").write_text("[]")
        (project_path / "locations.json").write_text("[]")
        
        yield project_path


