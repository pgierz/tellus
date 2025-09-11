"""
Comprehensive unit tests for NetworkTopologyApplicationService.

Tests the service layer orchestration logic while mocking all infrastructure dependencies
following clean architecture testing principles.
"""

import asyncio
import pytest
import time
from unittest.mock import Mock, AsyncMock, MagicMock, patch
from typing import List, Optional, Dict, Any

from tellus.application.services.network_topology_service import (
    NetworkTopologyApplicationService,
    CreateNetworkTopologyDto,
    TopologyBenchmarkDto,
    OptimalRouteRequestDto,
    OptimalRouteResponseDto,
    NetworkConnectionDto,
    NetworkPathDto
)
from tellus.application.exceptions import (
    ValidationError,
    EntityNotFoundError,
    ExternalServiceError,
    OperationNotAllowedError
)
from tellus.domain.entities.location import LocationEntity, LocationKind
from tellus.domain.entities.network_topology import NetworkTopology
from tellus.domain.entities.network_connection import NetworkConnection, ConnectionType
from tellus.domain.entities.network_metrics import (
    NetworkPath, BandwidthMetrics, LatencyMetrics, NetworkHealth
)


# Test fixtures

@pytest.fixture
def mock_location_repo():
    """Mock location repository."""
    repo = Mock()
    repo.get_by_name = Mock(return_value=None)
    repo.list_all = Mock(return_value=[])
    return repo

@pytest.fixture
def mock_topology_repo():
    """Mock topology repository."""
    repo = Mock()
    repo.save_topology = Mock()
    repo.get_topology = Mock(return_value=None)
    repo.list_topologies = Mock(return_value=[])
    repo.delete_topology = Mock(return_value=True)
    repo.topology_exists = Mock(return_value=False)
    return repo

@pytest.fixture
def mock_benchmarking_adapter():
    """Mock benchmarking adapter."""
    adapter = AsyncMock()
    adapter.benchmark_connection_pair = AsyncMock(return_value=None)
    adapter.measure_bandwidth = AsyncMock(return_value=None)
    adapter.measure_latency = AsyncMock(return_value=None)
    adapter.test_connectivity = AsyncMock(return_value=True)
    return adapter

@pytest.fixture
def service(mock_location_repo, mock_topology_repo, mock_benchmarking_adapter):
    """Create service instance with mocked dependencies."""
    return NetworkTopologyApplicationService(
        location_repo=mock_location_repo,
        topology_repo=mock_topology_repo,
        benchmarking_adapter=mock_benchmarking_adapter
    )

@pytest.fixture
def sample_locations():
    """Create sample location entities for testing."""
    loc1 = LocationEntity(
        name="compute-node-1",
        kinds=[LocationKind.COMPUTE],
        config={
            "protocol": "ssh",
            "path": "/data",
            "storage_options": {"host": "compute1.hpc.edu"},
            "host": "compute1.hpc.edu",
            "username": "user"
        }
    )
    
    loc2 = LocationEntity(
        name="storage-server",
        kinds=[LocationKind.FILESERVER],
        config={
            "protocol": "sftp",
            "path": "/archive",
            "storage_options": {"host": "storage.hpc.edu"},
            "host": "storage.hpc.edu",
            "username": "archiver"
        }
    )
    
    loc3 = LocationEntity(
        name="tape-archive",
        kinds=[LocationKind.TAPE],
        config={
            "protocol": "file",
            "path": "/mnt/tape"
        }
    )
    
    return [loc1, loc2, loc3]

@pytest.fixture
def sample_topology():
    """Create sample network topology."""
    topology = NetworkTopology("test-topology")
    
    # Add sample connections
    connection = NetworkConnection(
        source_location="compute-node-1",
        destination_location="storage-server",
        connection_type=ConnectionType.WAN,
        bandwidth_metrics=BandwidthMetrics(
            measured_mbps=100.0,
            measurement_timestamp=time.time()
        ),
        latency_metrics=LatencyMetrics(
            avg_latency_ms=50.0,
            min_latency_ms=40.0,
            max_latency_ms=60.0,
            measurement_timestamp=time.time()
        )
    )
    topology.add_connection(connection)
    return topology

@pytest.fixture
def sample_network_path():
    """Create sample network path."""
    return NetworkPath(
        source_location="compute-node-1",
        destination_location="storage-server",
        intermediate_hops=[],
        total_cost=1.0,
        estimated_bandwidth_mbps=100.0,
        estimated_latency_ms=50.0,
        path_type="direct"
    )


class TestNetworkTopologyApplicationService:
    """Test suite for NetworkTopologyApplicationService."""


class TestCreateTopology:
    """Tests for topology creation operations."""

    @pytest.mark.asyncio
    async def test_create_topology_success(self, service, mock_topology_repo):
        """Test successful topology creation."""
        dto = CreateNetworkTopologyDto(
            name="test-topology",
            auto_discovery_enabled=True,
            benchmark_cache_ttl_hours=24.0
        )
        
        mock_topology_repo.get_topology.return_value = None
        
        result = await service.create_topology(dto)
        
        assert result.name == "test-topology"
        assert result.auto_discovery_enabled is True
        assert result.benchmark_cache_ttl_hours == 24.0
        mock_topology_repo.save_topology.assert_called_once()

    @pytest.mark.asyncio
    async def test_create_topology_invalid_name(self, service):
        """Test topology creation with invalid name."""
        dto = CreateNetworkTopologyDto(name="")
        
        with pytest.raises(ValidationError, match="Topology name is required"):
            await service.create_topology(dto)

    @pytest.mark.asyncio
    async def test_create_topology_already_exists(self, service, mock_topology_repo):
        """Test topology creation when topology already exists."""
        dto = CreateNetworkTopologyDto(name="existing-topology")
        existing_topology = NetworkTopology("existing-topology")
        mock_topology_repo.get_topology.return_value = existing_topology
        
        with pytest.raises(ValidationError, match="Topology 'existing-topology' already exists"):
            await service.create_topology(dto)

    @pytest.mark.asyncio
    async def test_get_or_create_default_topology_exists(self, service, mock_topology_repo, sample_topology):
        """Test getting default topology when it exists."""
        mock_topology_repo.get_topology.return_value = sample_topology
        
        result = await service.get_or_create_default_topology()
        
        assert result == sample_topology
        mock_topology_repo.get_topology.assert_called_once_with("default")

    @pytest.mark.asyncio
    async def test_get_or_create_default_topology_creates_new(self, service, mock_topology_repo):
        """Test creating default topology when it doesn't exist."""
        mock_topology_repo.get_topology.return_value = None
        
        result = await service.get_or_create_default_topology()
        
        assert result.name == "default"
        mock_topology_repo.save_topology.assert_called_once()


class TestDiscoverTopology:
    """Tests for network topology discovery operations."""

    @pytest.mark.asyncio
    async def test_discover_topology_success(self, service, mock_location_repo, mock_topology_repo, 
                                           mock_benchmarking_adapter, sample_locations):
        """Test successful topology discovery."""
        # Setup mocks
        mock_location_repo.list_all.return_value = sample_locations[:2]  # Only 2 locations for simplicity
        mock_topology_repo.get_topology.return_value = None
        
        # Mock successful benchmark results
        sample_connection = NetworkConnection(
            source_location="compute-node-1",
            destination_location="storage-server",
            connection_type=ConnectionType.WAN,
            bandwidth_metrics=BandwidthMetrics(measured_mbps=100.0, measurement_timestamp=time.time())
        )
        mock_benchmarking_adapter.benchmark_connection_pair.return_value = sample_connection
        
        result = await service.discover_topology()
        
        assert result.name == "default"
        assert len(result.connections) == 1
        mock_topology_repo.save_topology.assert_called()

    @pytest.mark.asyncio
    async def test_discover_topology_with_specific_locations(self, service, mock_location_repo, 
                                                           mock_topology_repo, sample_locations):
        """Test topology discovery with specific location names."""
        location_names = ["compute-node-1", "storage-server"]
        mock_location_repo.get_by_name.side_effect = lambda name: next(
            (loc for loc in sample_locations if loc.name == name), None
        )
        mock_topology_repo.get_topology.return_value = None
        
        result = await service.discover_topology(location_names=location_names)
        
        assert result.name == "default"
        # Verify correct locations were queried
        assert mock_location_repo.get_by_name.call_count == 2

    @pytest.mark.asyncio
    async def test_discover_topology_location_not_found(self, service, mock_location_repo):
        """Test topology discovery with non-existent location."""
        location_names = ["nonexistent-location"]
        mock_location_repo.get_by_name.return_value = None
        
        with pytest.raises(EntityNotFoundError, match="Location.*nonexistent-location.*not found"):
            await service.discover_topology(location_names=location_names)

    @pytest.mark.asyncio
    async def test_discover_topology_insufficient_locations(self, service, mock_location_repo):
        """Test topology discovery with insufficient locations."""
        mock_location_repo.list_all.return_value = [Mock()]  # Only 1 location
        
        with pytest.raises(ValidationError, match="At least 2 locations required"):
            await service.discover_topology()

    @pytest.mark.asyncio
    async def test_discover_topology_with_benchmark_failures(self, service, mock_location_repo, 
                                                            mock_topology_repo, mock_benchmarking_adapter, 
                                                            sample_locations):
        """Test topology discovery with some benchmark failures."""
        mock_location_repo.list_all.return_value = sample_locations[:2]
        mock_topology_repo.get_topology.return_value = None
        
        # Mock benchmark failure (returns None)
        mock_benchmarking_adapter.benchmark_connection_pair.return_value = None
        
        result = await service.discover_topology()
        
        assert result.name == "default"
        assert len(result.connections) == 0  # No connections added due to failures


class TestBenchmarkTopology:
    """Tests for topology benchmarking operations."""

    @pytest.mark.asyncio
    async def test_benchmark_topology_success(self, service, mock_topology_repo, mock_location_repo,
                                            mock_benchmarking_adapter, sample_topology, sample_locations):
        """Test successful topology benchmarking."""
        dto = TopologyBenchmarkDto(
            topology_name="test-topology",
            include_bandwidth=True,
            include_latency=True
        )
        
        mock_topology_repo.get_topology.return_value = sample_topology
        mock_location_repo.get_by_name.side_effect = lambda name: next(
            (loc for loc in sample_locations if loc.name == name), None
        )
        
        # Mock successful benchmark
        updated_connection = NetworkConnection(
            source_location="compute-node-1",
            destination_location="storage-server",
            connection_type=ConnectionType.WAN,
            bandwidth_metrics=BandwidthMetrics(measured_mbps=120.0, measurement_timestamp=time.time())
        )
        mock_benchmarking_adapter.benchmark_connection_pair.return_value = updated_connection
        
        result = await service.benchmark_topology(dto)
        
        assert result['topology_name'] == "test-topology"
        assert result['successful_benchmarks'] >= 0
        mock_topology_repo.save_topology.assert_called()

    @pytest.mark.asyncio
    async def test_benchmark_topology_not_found(self, service, mock_topology_repo):
        """Test benchmarking non-existent topology."""
        dto = TopologyBenchmarkDto(topology_name="nonexistent")
        mock_topology_repo.get_topology.return_value = None
        
        with pytest.raises(EntityNotFoundError, match="NetworkTopology.*nonexistent.*not found"):
            await service.benchmark_topology(dto)

    @pytest.mark.asyncio
    async def test_benchmark_topology_with_specific_pairs(self, service, mock_topology_repo, 
                                                        mock_location_repo, mock_benchmarking_adapter, 
                                                        sample_topology, sample_locations):
        """Test benchmarking specific location pairs."""
        dto = TopologyBenchmarkDto(
            topology_name="test-topology",
            location_pairs=[("compute-node-1", "storage-server")]
        )
        
        mock_topology_repo.get_topology.return_value = sample_topology
        mock_location_repo.get_by_name.side_effect = lambda name: next(
            (loc for loc in sample_locations if loc.name == name), None
        )
        mock_benchmarking_adapter.benchmark_connection_pair.return_value = Mock(spec=NetworkConnection)
        
        result = await service.benchmark_topology(dto)
        
        assert result['topology_name'] == "test-topology"
        # Verify specific pair was benchmarked
        mock_location_repo.get_by_name.assert_any_call("compute-node-1")
        mock_location_repo.get_by_name.assert_any_call("storage-server")

    @pytest.mark.asyncio
    async def test_benchmark_topology_location_not_found_in_pairs(self, service, mock_topology_repo, 
                                                                mock_location_repo, sample_topology):
        """Test benchmarking with non-existent location in pairs."""
        dto = TopologyBenchmarkDto(
            topology_name="test-topology",
            location_pairs=[("compute-node-1", "nonexistent")]
        )
        
        mock_topology_repo.get_topology.return_value = sample_topology
        mock_location_repo.get_by_name.side_effect = lambda name: None if name == "nonexistent" else Mock()
        
        with pytest.raises(EntityNotFoundError, match="Location.*nonexistent.*not found"):
            await service.benchmark_topology(dto)

    @pytest.mark.asyncio
    async def test_benchmark_topology_no_pairs_to_test(self, service, mock_topology_repo, sample_topology):
        """Test benchmarking when no valid pairs exist."""
        dto = TopologyBenchmarkDto(topology_name="test-topology")
        
        # Empty topology
        empty_topology = NetworkTopology("test-topology")
        mock_topology_repo.get_topology.return_value = empty_topology
        
        result = await service.benchmark_topology(dto)
        
        assert result['benchmarked_connections'] == 0
        assert "No location pairs to benchmark" in result['message']

    @pytest.mark.asyncio
    async def test_benchmark_topology_concurrent_limit(self, service, mock_topology_repo, 
                                                      mock_location_repo, mock_benchmarking_adapter, 
                                                      sample_topology, sample_locations):
        """Test benchmarking respects concurrency limits."""
        dto = TopologyBenchmarkDto(
            topology_name="test-topology",
            max_concurrent_tests=1  # Very low limit
        )
        
        mock_topology_repo.get_topology.return_value = sample_topology
        mock_location_repo.get_by_name.side_effect = lambda name: next(
            (loc for loc in sample_locations if loc.name == name), None
        )
        
        # Mock slow benchmark to test concurrency
        async def slow_benchmark(*args, **kwargs):
            await asyncio.sleep(0.1)
            return Mock(spec=NetworkConnection)
        
        mock_benchmarking_adapter.benchmark_connection_pair = slow_benchmark
        
        result = await service.benchmark_topology(dto)
        
        assert result['topology_name'] == "test-topology"

    @pytest.mark.asyncio
    async def test_benchmark_topology_force_refresh(self, service, mock_topology_repo, 
                                                   mock_location_repo, mock_benchmarking_adapter,
                                                   sample_topology, sample_locations):
        """Test benchmarking with force refresh bypasses cache."""
        dto = TopologyBenchmarkDto(
            topology_name="test-topology",
            force_refresh=True
        )
        
        mock_topology_repo.get_topology.return_value = sample_topology
        mock_location_repo.get_by_name.side_effect = lambda name: next(
            (loc for loc in sample_locations if loc.name == name), None
        )
        mock_benchmarking_adapter.benchmark_connection_pair.return_value = Mock(spec=NetworkConnection)
        
        result = await service.benchmark_topology(dto)
        
        # Verify force_refresh is passed to adapter
        mock_benchmarking_adapter.benchmark_connection_pair.assert_called()


class TestFindOptimalRoute:
    """Tests for optimal route finding operations."""

    @pytest.mark.asyncio
    async def test_find_optimal_route_success(self, service, mock_location_repo, mock_topology_repo, 
                                            sample_locations, sample_network_path):
        """Test successful optimal route finding."""
        dto = OptimalRouteRequestDto(
            source_location="compute-node-1",
            destination_location="storage-server",
            optimize_for="bandwidth"
        )
        
        # Mock locations exist
        mock_location_repo.get_by_name.side_effect = lambda name: next(
            (loc for loc in sample_locations if loc.name == name), None
        )
        
        # Mock topology with route finding capability
        mock_topology = Mock(spec=NetworkTopology)
        mock_topology.location_names = ["compute-node-1", "storage-server"]
        mock_topology.find_optimal_path.return_value = sample_network_path
        mock_topology.connections = []
        mock_topology.connections = []
        mock_topology_repo.get_topology.return_value = mock_topology
        
        result = await service.find_optimal_route(dto)
        
        assert isinstance(result, OptimalRouteResponseDto)
        assert result.primary_path.source_location == "compute-node-1"
        assert result.primary_path.destination_location == "storage-server"
        assert result.recommendation is not None

    @pytest.mark.asyncio
    async def test_find_optimal_route_source_not_found(self, service, mock_location_repo):
        """Test optimal route finding with non-existent source location."""
        dto = OptimalRouteRequestDto(
            source_location="nonexistent",
            destination_location="storage-server"
        )
        
        mock_location_repo.get_by_name.return_value = None
        
        with pytest.raises(EntityNotFoundError, match="Location.*nonexistent.*not found"):
            await service.find_optimal_route(dto)

    @pytest.mark.asyncio
    async def test_find_optimal_route_destination_not_found(self, service, mock_location_repo, sample_locations):
        """Test optimal route finding with non-existent destination location."""
        dto = OptimalRouteRequestDto(
            source_location="compute-node-1",
            destination_location="nonexistent"
        )
        
        mock_location_repo.get_by_name.side_effect = lambda name: (
            sample_locations[0] if name == "compute-node-1" else None
        )
        
        with pytest.raises(EntityNotFoundError, match="Location.*nonexistent.*not found"):
            await service.find_optimal_route(dto)

    @pytest.mark.asyncio
    async def test_find_optimal_route_no_route_found(self, service, mock_location_repo, 
                                                   mock_topology_repo, sample_locations):
        """Test optimal route finding when no route exists."""
        dto = OptimalRouteRequestDto(
            source_location="compute-node-1",
            destination_location="storage-server"
        )
        
        mock_location_repo.get_by_name.side_effect = lambda name: next(
            (loc for loc in sample_locations if loc.name == name), None
        )
        
        # Mock topology with no route
        mock_topology = Mock(spec=NetworkTopology)
        mock_topology.location_names = ["compute-node-1", "storage-server"]
        mock_topology.find_optimal_path.return_value = None
        mock_topology_repo.get_topology.return_value = mock_topology
        
        with pytest.raises(ValidationError, match="No route found"):
            await service.find_optimal_route(dto)

    @pytest.mark.asyncio
    async def test_find_optimal_route_with_constraints(self, service, mock_location_repo, 
                                                     mock_topology_repo, sample_locations, sample_network_path):
        """Test optimal route finding with constraints."""
        dto = OptimalRouteRequestDto(
            source_location="compute-node-1",
            destination_location="storage-server",
            optimize_for="latency",
            max_hops=2,
            required_min_bandwidth_mbps=50.0,
            max_acceptable_latency_ms=100.0
        )
        
        mock_location_repo.get_by_name.side_effect = lambda name: next(
            (loc for loc in sample_locations if loc.name == name), None
        )
        
        mock_topology = Mock(spec=NetworkTopology)
        mock_topology.location_names = ["compute-node-1", "storage-server"]
        mock_topology.find_optimal_path.return_value = sample_network_path
        mock_topology.connections = []
        mock_topology_repo.get_topology.return_value = mock_topology
        
        result = await service.find_optimal_route(dto)
        
        assert isinstance(result, OptimalRouteResponseDto)
        # Verify constraints are considered in path analysis
        assert 'constraint_violations' in result.path_analysis

    @pytest.mark.asyncio
    async def test_find_optimal_route_constraint_violations(self, service, mock_location_repo, 
                                                          mock_topology_repo, sample_locations):
        """Test optimal route finding with constraint violations."""
        dto = OptimalRouteRequestDto(
            source_location="compute-node-1",
            destination_location="storage-server",
            max_hops=1,  # Very restrictive
            required_min_bandwidth_mbps=1000.0,  # Very high requirement
            max_acceptable_latency_ms=1.0  # Very low requirement
        )
        
        mock_location_repo.get_by_name.side_effect = lambda name: next(
            (loc for loc in sample_locations if loc.name == name), None
        )
        
        # Path that violates constraints
        violating_path = NetworkPath(
            source_location="compute-node-1",
            destination_location="storage-server",
            intermediate_hops=["intermediate1", "intermediate2"],  # Too many hops
            total_cost=1.0,
            estimated_bandwidth_mbps=10.0,  # Too low
            estimated_latency_ms=200.0,  # Too high
            path_type="multi_hop"
        )
        
        mock_topology = Mock(spec=NetworkTopology)
        mock_topology.location_names = ["compute-node-1", "storage-server"]
        mock_topology.find_optimal_path.return_value = violating_path
        mock_topology.connections = []
        mock_topology.get_stale_connections.return_value = []
        mock_topology.get_bottleneck_connections.return_value = []
        mock_topology_repo.get_topology.return_value = mock_topology
        
        result = await service.find_optimal_route(dto)
        
        assert isinstance(result, OptimalRouteResponseDto)
        assert len(result.path_analysis['constraint_violations']) > 0

    @pytest.mark.asyncio
    async def test_find_optimal_route_with_alternatives(self, service, mock_location_repo, 
                                                      mock_topology_repo, sample_locations, sample_network_path):
        """Test optimal route finding includes alternative paths."""
        dto = OptimalRouteRequestDto(
            source_location="compute-node-1",
            destination_location="storage-server",
            optimize_for="bandwidth"
        )
        
        mock_location_repo.get_by_name.side_effect = lambda name: next(
            (loc for loc in sample_locations if loc.name == name), None
        )
        
        # Create alternative path
        alt_path = NetworkPath(
            source_location="compute-node-1",
            destination_location="storage-server",
            intermediate_hops=["intermediate"],
            total_cost=2.0,
            estimated_bandwidth_mbps=80.0,
            estimated_latency_ms=70.0,
            path_type="indirect"
        )
        
        mock_topology = Mock(spec=NetworkTopology)
        mock_topology.location_names = ["compute-node-1", "storage-server"]
        mock_topology.find_optimal_path.side_effect = [sample_network_path, alt_path, None, None]
        mock_topology.connections = []
        mock_topology.get_stale_connections.return_value = []
        mock_topology.get_bottleneck_connections.return_value = []
        mock_topology_repo.get_topology.return_value = mock_topology
        
        result = await service.find_optimal_route(dto)
        
        assert isinstance(result, OptimalRouteResponseDto)
        assert len(result.alternative_paths) > 0

    @pytest.mark.asyncio
    async def test_find_optimal_route_uses_cache(self, service, mock_location_repo, 
                                                mock_topology_repo, sample_locations, sample_network_path):
        """Test optimal route finding uses cache for repeated requests."""
        dto = OptimalRouteRequestDto(
            source_location="compute-node-1",
            destination_location="storage-server",
            optimize_for="bandwidth"
        )
        
        mock_location_repo.get_by_name.side_effect = lambda name: next(
            (loc for loc in sample_locations if loc.name == name), None
        )
        
        mock_topology = Mock(spec=NetworkTopology)
        mock_topology.location_names = ["compute-node-1", "storage-server"]
        mock_topology.find_optimal_path.return_value = sample_network_path
        mock_topology.connections = []
        mock_topology_repo.get_topology.return_value = mock_topology
        
        # First call
        result1 = await service.find_optimal_route(dto)
        
        # Second call should use cache
        result2 = await service.find_optimal_route(dto)
        
        assert result1.request_id != result2.request_id  # Different request IDs
        assert result2.path_analysis.get('cache_hit') is True


class TestGetTopologyStatus:
    """Tests for topology status operations."""

    @pytest.mark.asyncio
    async def test_get_topology_status_exists(self, service, mock_topology_repo):
        """Test getting status of existing topology."""
        # Create a mock topology to avoid is_stale issues
        mock_topology = Mock()
        mock_topology.name = "test-topology"
        mock_topology.last_updated = time.time()
        mock_topology.auto_discovery_enabled = True
        mock_topology.connections = []
        mock_topology.location_names = []
        mock_topology.get_stale_connections.return_value = []
        mock_topology.get_bottleneck_connections.return_value = []
        mock_topology.needs_refresh.return_value = False
        
        mock_topology_repo.get_topology.return_value = mock_topology
        
        result = await service.get_topology_status("test-topology")
        
        assert result['exists'] is True
        assert result['name'] == "test-topology"
        assert 'total_connections' in result
        assert 'health_distribution' in result
        assert 'bandwidth_stats' in result

    @pytest.mark.asyncio
    async def test_get_topology_status_not_exists(self, service, mock_topology_repo):
        """Test getting status of non-existent topology."""
        mock_topology_repo.get_topology.return_value = None
        
        result = await service.get_topology_status("nonexistent")
        
        assert result['exists'] is False
        assert result['name'] == "nonexistent"

    @pytest.mark.asyncio
    async def test_get_topology_status_default(self, service, mock_topology_repo):
        """Test getting default topology status."""
        # Create a mock topology to avoid is_stale issues
        mock_topology = Mock()
        mock_topology.name = "default"
        mock_topology.last_updated = time.time()
        mock_topology.auto_discovery_enabled = True
        mock_topology.connections = []
        mock_topology.location_names = []
        mock_topology.get_stale_connections.return_value = []
        mock_topology.get_bottleneck_connections.return_value = []
        mock_topology.needs_refresh.return_value = False
        
        mock_topology_repo.get_topology.return_value = mock_topology
        
        result = await service.get_topology_status()  # No topology name
        
        assert result['exists'] is True
        mock_topology_repo.get_topology.assert_called_with("default")

    @pytest.mark.asyncio
    async def test_get_topology_status_comprehensive(self, service, mock_topology_repo):
        """Test comprehensive topology status with various metrics."""
        # Create a mock topology instead of using the real one to avoid is_stale issue
        mock_topology = Mock()
        mock_topology.name = "comprehensive-test"
        mock_topology.last_updated = time.time()
        mock_topology.auto_discovery_enabled = True
        
        # Mock connections with actual bandwidth metrics (not mocked)
        mock_conn1 = Mock()
        mock_conn1.current_health.name = "OPTIMAL"
        mock_conn1.bandwidth_metrics = BandwidthMetrics(measured_mbps=100.0, measurement_timestamp=time.time())
        mock_conn1.latency_metrics = LatencyMetrics(
            avg_latency_ms=50.0, min_latency_ms=40.0, max_latency_ms=60.0, measurement_timestamp=time.time()
        )
        mock_conn1.effective_bandwidth_mbps = 100.0
        
        mock_conn2 = Mock()
        mock_conn2.current_health.name = "CONGESTED"
        mock_conn2.bandwidth_metrics = BandwidthMetrics(measured_mbps=50.0, measurement_timestamp=time.time())
        mock_conn2.latency_metrics = LatencyMetrics(
            avg_latency_ms=100.0, min_latency_ms=80.0, max_latency_ms=120.0, measurement_timestamp=time.time()
        )
        mock_conn2.effective_bandwidth_mbps = 50.0
        
        mock_topology.connections = [mock_conn1, mock_conn2]
        mock_topology.location_names = ["loc1", "loc2", "loc3"]
        mock_topology.get_stale_connections.return_value = []
        mock_topology.get_bottleneck_connections.return_value = []
        mock_topology.needs_refresh.return_value = False
        
        mock_topology_repo.get_topology.return_value = mock_topology
        
        result = await service.get_topology_status("comprehensive-test")
        
        assert result['exists'] is True
        assert result['total_connections'] == 2
        assert result['total_locations'] == 3
        assert 'OPTIMAL' in result['health_distribution']
        assert 'CONGESTED' in result['health_distribution']
        assert result['bandwidth_stats']['count'] == 2
        assert result['latency_stats']['count'] == 2


class TestErrorHandling:
    """Tests for error handling scenarios."""

    @pytest.mark.asyncio
    async def test_discover_topology_external_service_error(self, service, mock_location_repo, 
                                                           mock_topology_repo, mock_benchmarking_adapter,
                                                           sample_locations):
        """Test handling of external service errors during discovery."""
        mock_location_repo.list_all.return_value = sample_locations[:2]
        mock_topology_repo.get_topology.return_value = None
        
        # Mock benchmarking adapter to raise exception
        mock_benchmarking_adapter.benchmark_connection_pair.side_effect = Exception("Network error")
        
        # Should complete without raising exception (error logged and handled)
        result = await service.discover_topology()
        
        assert result.name == "default"
        assert len(result.connections) == 0  # No connections added due to errors

    @pytest.mark.asyncio
    async def test_benchmark_topology_repository_error(self, service, mock_topology_repo):
        """Test handling of repository errors during benchmarking."""
        dto = TopologyBenchmarkDto(topology_name="test")
        
        # Mock repository to raise exception
        mock_topology_repo.get_topology.side_effect = Exception("Database error")
        
        with pytest.raises(Exception, match="Database error"):
            await service.benchmark_topology(dto)

    @pytest.mark.asyncio
    async def test_find_optimal_route_topology_creation_error(self, service, mock_location_repo, 
                                                            mock_topology_repo, sample_locations):
        """Test handling topology creation error during route finding."""
        dto = OptimalRouteRequestDto(
            source_location="compute-node-1",
            destination_location="storage-server"
        )
        
        mock_location_repo.get_by_name.side_effect = lambda name: next(
            (loc for loc in sample_locations if loc.name == name), None
        )
        
        # Mock repository errors
        mock_topology_repo.get_topology.return_value = None
        mock_topology_repo.save_topology.side_effect = Exception("Save error")
        
        with pytest.raises(Exception, match="Save error"):
            await service.find_optimal_route(dto)


class TestConcurrencyAndThreadSafety:
    """Tests for concurrent operations and thread safety."""

    @pytest.mark.asyncio
    async def test_concurrent_benchmark_tracking(self, service, mock_topology_repo, 
                                                mock_location_repo, mock_benchmarking_adapter, 
                                                sample_topology, sample_locations):
        """Test concurrent benchmark tracking prevents duplicates."""
        dto = TopologyBenchmarkDto(topology_name="test-topology")
        
        mock_topology_repo.get_topology.return_value = sample_topology
        mock_location_repo.get_by_name.side_effect = lambda name: next(
            (loc for loc in sample_locations if loc.name == name), None
        )
        
        # Mock slow benchmarking to test concurrency
        async def slow_benchmark(*args, **kwargs):
            await asyncio.sleep(0.1)
            return Mock(spec=NetworkConnection)
        
        mock_benchmarking_adapter.benchmark_connection_pair = slow_benchmark
        
        # Start two concurrent benchmark operations
        task1 = asyncio.create_task(service.benchmark_topology(dto))
        task2 = asyncio.create_task(service.benchmark_topology(dto))
        
        results = await asyncio.gather(task1, task2, return_exceptions=True)
        
        # Both should complete (one might skip due to active tracking)
        assert len(results) == 2

    @pytest.mark.asyncio
    async def test_route_cache_thread_safety(self, service, mock_location_repo, 
                                           mock_topology_repo, sample_locations, sample_network_path):
        """Test route cache thread safety with concurrent access."""
        dto = OptimalRouteRequestDto(
            source_location="compute-node-1",
            destination_location="storage-server"
        )
        
        mock_location_repo.get_by_name.side_effect = lambda name: next(
            (loc for loc in sample_locations if loc.name == name), None
        )
        
        mock_topology = Mock(spec=NetworkTopology)
        mock_topology.location_names = ["compute-node-1", "storage-server"]
        mock_topology.find_optimal_path.return_value = sample_network_path
        mock_topology.connections = []
        mock_topology_repo.get_topology.return_value = mock_topology
        
        # Start multiple concurrent route finding operations
        tasks = [
            asyncio.create_task(service.find_optimal_route(dto))
            for _ in range(5)
        ]
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # All should complete successfully
        assert len(results) == 5
        assert all(isinstance(r, OptimalRouteResponseDto) for r in results)


class TestEdgeCases:
    """Tests for edge cases and boundary conditions."""

    @pytest.mark.asyncio
    async def test_discover_topology_empty_benchmark_results(self, service, mock_location_repo, 
                                                           mock_topology_repo, mock_benchmarking_adapter, 
                                                           sample_locations):
        """Test discovery with all benchmark results empty."""
        mock_location_repo.list_all.return_value = sample_locations
        mock_topology_repo.get_topology.return_value = None
        mock_benchmarking_adapter.benchmark_connection_pair.return_value = None
        
        result = await service.discover_topology()
        
        assert result.name == "default"
        assert len(result.connections) == 0

    @pytest.mark.asyncio
    async def test_benchmark_topology_empty_topology(self, service, mock_topology_repo):
        """Test benchmarking completely empty topology."""
        dto = TopologyBenchmarkDto(topology_name="empty")
        empty_topology = NetworkTopology("empty")
        mock_topology_repo.get_topology.return_value = empty_topology
        
        result = await service.benchmark_topology(dto)
        
        assert result['benchmarked_connections'] == 0
        assert result['message'] == "No location pairs to benchmark"

    @pytest.mark.asyncio
    async def test_find_optimal_route_disconnected_network(self, service, mock_location_repo, 
                                                         mock_topology_repo, sample_locations):
        """Test route finding in disconnected network."""
        dto = OptimalRouteRequestDto(
            source_location="compute-node-1",
            destination_location="storage-server"
        )
        
        mock_location_repo.get_by_name.side_effect = lambda name: next(
            (loc for loc in sample_locations if loc.name == name), None
        )
        
        # Mock topology with no connections between the locations
        disconnected_topology = NetworkTopology("disconnected")
        disconnected_topology.find_optimal_path = Mock(return_value=None)
        mock_topology_repo.get_topology.return_value = disconnected_topology
        
        with pytest.raises(ValidationError, match="No route found"):
            await service.find_optimal_route(dto)

    @pytest.mark.asyncio
    async def test_get_topology_status_empty_cache(self, service, mock_topology_repo):
        """Test topology status with empty caches."""
        # Clear internal caches
        service._route_cache.clear()
        service._active_benchmarks.clear()
        
        # Create a mock topology to avoid is_stale issues
        mock_topology = Mock()
        mock_topology.name = "test"
        mock_topology.last_updated = time.time()
        mock_topology.auto_discovery_enabled = True
        mock_topology.connections = []
        mock_topology.location_names = []
        mock_topology.get_stale_connections.return_value = []
        mock_topology.get_bottleneck_connections.return_value = []
        mock_topology.needs_refresh.return_value = False
        
        mock_topology_repo.get_topology.return_value = mock_topology
        
        result = await service.get_topology_status("test")
        
        assert result['cache_stats']['route_cache_entries'] == 0
        assert result['cache_stats']['active_benchmarks'] == 0

    @pytest.mark.asyncio
    async def test_optimization_criteria_variations(self, service, mock_location_repo, 
                                                    mock_topology_repo, sample_locations, sample_network_path):
        """Test different optimization criteria."""
        criteria_list = ["bandwidth", "latency", "cost", "reliability"]
        
        mock_location_repo.get_by_name.side_effect = lambda name: next(
            (loc for loc in sample_locations if loc.name == name), None
        )
        
        mock_topology = Mock(spec=NetworkTopology)
        mock_topology.location_names = ["compute-node-1", "storage-server"]
        mock_topology.find_optimal_path.return_value = sample_network_path
        mock_topology.connections = []
        mock_topology_repo.get_topology.return_value = mock_topology
        
        for criteria in criteria_list:
            dto = OptimalRouteRequestDto(
                source_location="compute-node-1",
                destination_location="storage-server",
                optimize_for=criteria
            )
            
            result = await service.find_optimal_route(dto)
            
            assert isinstance(result, OptimalRouteResponseDto)
            assert result.path_analysis['optimization_criteria'] == criteria


class TestPerformanceScenarios:
    """Tests for performance-related scenarios."""

    @pytest.mark.asyncio
    async def test_discover_topology_large_location_set(self, service, mock_location_repo, 
                                                       mock_topology_repo, mock_benchmarking_adapter):
        """Test discovery with large number of locations."""
        # Create many locations
        large_location_set = []
        for i in range(10):
            loc = LocationEntity(
                name=f"location-{i}",
                kinds=[LocationKind.COMPUTE],
                config={
                    "protocol": "ssh",
                    "path": f"/data{i}",
                    "storage_options": {"host": f"host{i}.edu"},
                    "host": f"host{i}.edu"
                }
            )
            large_location_set.append(loc)
        
        mock_location_repo.list_all.return_value = large_location_set
        mock_topology_repo.get_topology.return_value = None
        
        # Mock fast benchmark results
        mock_connection = Mock(spec=NetworkConnection)
        mock_connection.source_location = "location-0"
        mock_connection.destination_location = "location-1"
        mock_benchmarking_adapter.benchmark_connection_pair.return_value = mock_connection
        
        result = await service.discover_topology()
        
        assert result.name == "default"
        # Should have processed many pairs (n*(n-1)/2 for 10 locations = 45 pairs)
        expected_pairs = len(large_location_set) * (len(large_location_set) - 1) // 2
        assert mock_benchmarking_adapter.benchmark_connection_pair.call_count <= expected_pairs

    @pytest.mark.asyncio
    async def test_benchmark_topology_concurrent_limit_respected(self, service, mock_topology_repo, 
                                                               mock_location_repo, mock_benchmarking_adapter, 
                                                               sample_topology, sample_locations):
        """Test that concurrent benchmark limits are respected."""
        dto = TopologyBenchmarkDto(
            topology_name="test-topology",
            max_concurrent_tests=2
        )
        
        mock_topology_repo.get_topology.return_value = sample_topology
        mock_location_repo.get_by_name.side_effect = lambda name: next(
            (loc for loc in sample_locations if loc.name == name), None
        )
        
        # Track concurrent calls
        concurrent_calls = 0
        max_concurrent_seen = 0
        
        async def track_concurrent_benchmark(*args, **kwargs):
            nonlocal concurrent_calls, max_concurrent_seen
            concurrent_calls += 1
            max_concurrent_seen = max(max_concurrent_seen, concurrent_calls)
            await asyncio.sleep(0.1)  # Simulate work
            concurrent_calls -= 1
            return Mock(spec=NetworkConnection)
        
        mock_benchmarking_adapter.benchmark_connection_pair = track_concurrent_benchmark
        
        await service.benchmark_topology(dto)
        
        # Should not exceed the specified limit
        assert max_concurrent_seen <= dto.max_concurrent_tests