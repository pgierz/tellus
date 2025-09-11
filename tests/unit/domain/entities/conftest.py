"""
Test fixtures and factories for network topology domain entity tests.

Provides reusable fixtures and factory methods for creating test data
following clean architecture principles and avoiding duplication.
"""

import time
import pytest
from typing import List, Optional, Dict

from tellus.domain.entities.network_metrics import (
    BandwidthMetrics, LatencyMetrics, NetworkPath, NetworkHealth
)
from tellus.domain.entities.network_connection import NetworkConnection, ConnectionType
from tellus.domain.entities.network_topology import NetworkTopology


# Network Metrics Factories
class BandwidthMetricsFactory:
    """Factory for creating BandwidthMetrics instances with sensible defaults."""
    
    @staticmethod
    def create_optimal(
        measured_mbps: float = 1000.0,
        theoretical_max_mbps: Optional[float] = 10000.0,
        **kwargs
    ) -> BandwidthMetrics:
        """Create optimal bandwidth metrics."""
        defaults = {
            'sample_count': 10,
            'variance_mbps': 5.0
        }
        defaults.update(kwargs)
        
        return BandwidthMetrics(
            measured_mbps=measured_mbps,
            theoretical_max_mbps=theoretical_max_mbps,
            **defaults
        )
    
    @staticmethod
    def create_low_bandwidth(measured_mbps: float = 5.0, **kwargs) -> BandwidthMetrics:
        """Create low bandwidth metrics (bottleneck scenario)."""
        return BandwidthMetricsFactory.create_optimal(
            measured_mbps=measured_mbps,
            theoretical_max_mbps=100.0,
            **kwargs
        )
    
    @staticmethod
    def create_stale(
        measured_mbps: float = 100.0,
        hours_old: float = 25.0,
        **kwargs
    ) -> BandwidthMetrics:
        """Create stale bandwidth metrics."""
        stale_timestamp = time.time() - (hours_old * 3600)
        defaults = {'measurement_timestamp': stale_timestamp}
        defaults.update(kwargs)
        
        return BandwidthMetricsFactory.create_optimal(
            measured_mbps=measured_mbps,
            **defaults
        )


class LatencyMetricsFactory:
    """Factory for creating LatencyMetrics instances with sensible defaults."""
    
    @staticmethod
    def create_optimal(
        avg_latency_ms: float = 5.0,
        jitter_ms: float = 1.0,
        packet_loss_percentage: float = 0.0,
        **kwargs
    ) -> LatencyMetrics:
        """Create optimal latency metrics."""
        defaults = {
            'min_latency_ms': avg_latency_ms * 0.8,
            'max_latency_ms': avg_latency_ms * 1.2,
            'sample_count': 100
        }
        defaults.update(kwargs)
        
        return LatencyMetrics(
            avg_latency_ms=avg_latency_ms,
            jitter_ms=jitter_ms,
            packet_loss_percentage=packet_loss_percentage,
            **defaults
        )
    
    @staticmethod
    def create_congested(
        avg_latency_ms: float = 250.0,
        jitter_ms: float = 60.0,
        **kwargs
    ) -> LatencyMetrics:
        """Create congested latency metrics."""
        return LatencyMetricsFactory.create_optimal(
            avg_latency_ms=avg_latency_ms,
            jitter_ms=jitter_ms,
            **kwargs
        )
    
    @staticmethod
    def create_degraded(
        avg_latency_ms: float = 600.0,
        packet_loss_percentage: float = 2.0,
        **kwargs
    ) -> LatencyMetrics:
        """Create degraded latency metrics."""
        return LatencyMetricsFactory.create_optimal(
            avg_latency_ms=avg_latency_ms,
            packet_loss_percentage=packet_loss_percentage,
            **kwargs
        )
    
    @staticmethod
    def create_unstable(
        avg_latency_ms: float = 100.0,
        packet_loss_percentage: float = 8.0,
        **kwargs
    ) -> LatencyMetrics:
        """Create unstable latency metrics."""
        return LatencyMetricsFactory.create_optimal(
            avg_latency_ms=avg_latency_ms,
            packet_loss_percentage=packet_loss_percentage,
            **kwargs
        )
    
    @staticmethod
    def create_stale(
        avg_latency_ms: float = 50.0,
        hours_old: float = 25.0,
        **kwargs
    ) -> LatencyMetrics:
        """Create stale latency metrics."""
        stale_timestamp = time.time() - (hours_old * 3600)
        defaults = {'measurement_timestamp': stale_timestamp}
        defaults.update(kwargs)
        
        return LatencyMetricsFactory.create_optimal(
            avg_latency_ms=avg_latency_ms,
            **defaults
        )


class NetworkPathFactory:
    """Factory for creating NetworkPath instances with sensible defaults."""
    
    @staticmethod
    def create_direct(
        source: str = "A",
        destination: str = "B",
        bandwidth_mbps: float = 1000.0,
        latency_ms: float = 5.0,
        **kwargs
    ) -> NetworkPath:
        """Create a direct network path."""
        defaults = {
            'total_cost': 1.0,
            'path_type': 'direct'
        }
        defaults.update(kwargs)
        
        return NetworkPath(
            source_location=source,
            destination_location=destination,
            intermediate_hops=[],
            estimated_bandwidth_mbps=bandwidth_mbps,
            estimated_latency_ms=latency_ms,
            **defaults
        )
    
    @staticmethod
    def create_multi_hop(
        source: str = "A",
        destination: str = "C",
        intermediate_hops: List[str] = None,
        **kwargs
    ) -> NetworkPath:
        """Create a multi-hop network path."""
        if intermediate_hops is None:
            intermediate_hops = ["B"]
        
        defaults = {
            'total_cost': len(intermediate_hops) + 1.0,
            'estimated_bandwidth_mbps': 500.0,
            'estimated_latency_ms': 15.0,
            'path_type': 'multi_hop'
        }
        defaults.update(kwargs)
        
        return NetworkPath(
            source_location=source,
            destination_location=destination,
            intermediate_hops=intermediate_hops,
            **defaults
        )
    
    @staticmethod
    def create_with_bottleneck(
        source: str = "A",
        destination: str = "B",
        bottleneck_location: str = "A",
        **kwargs
    ) -> NetworkPath:
        """Create a path with a bottleneck."""
        defaults = {
            'estimated_bandwidth_mbps': 10.0,  # Low bandwidth due to bottleneck
            'estimated_latency_ms': 100.0
        }
        defaults.update(kwargs)
        
        return NetworkPathFactory.create_direct(
            source=source,
            destination=destination,
            bottleneck_location=bottleneck_location,
            **defaults
        )


# Network Connection Factories
class NetworkConnectionFactory:
    """Factory for creating NetworkConnection instances with sensible defaults."""
    
    @staticmethod
    def create_high_performance(
        source: str = "compute-node-01",
        destination: str = "storage-array-01",
        connection_type: ConnectionType = ConnectionType.INFINIBAND,
        **kwargs
    ) -> NetworkConnection:
        """Create a high-performance network connection."""
        defaults = {
            'bandwidth_metrics': BandwidthMetricsFactory.create_optimal(10000.0),
            'latency_metrics': LatencyMetricsFactory.create_optimal(1.0),
            'connection_cost': 1.0,
            'metadata': {'interface': 'ib0', 'speed': '100Gbps'}
        }
        defaults.update(kwargs)
        
        return NetworkConnection(
            source_location=source,
            destination_location=destination,
            connection_type=connection_type,
            **defaults
        )
    
    @staticmethod
    def create_bottleneck(
        source: str = "laptop",
        destination: str = "server",
        connection_type: ConnectionType = ConnectionType.BOTTLENECK,
        **kwargs
    ) -> NetworkConnection:
        """Create a bottleneck network connection."""
        defaults = {
            'bandwidth_metrics': BandwidthMetricsFactory.create_low_bandwidth(2.0),
            'latency_metrics': LatencyMetricsFactory.create_degraded(),
            'connection_cost': 5.0,
            'metadata': {'interface': 'wifi0', 'speed': '54Mbps'}
        }
        defaults.update(kwargs)
        
        return NetworkConnection(
            source_location=source,
            destination_location=destination,
            connection_type=connection_type,
            **defaults
        )
    
    @staticmethod
    def create_wan_connection(
        source: str = "site-a",
        destination: str = "site-b",
        **kwargs
    ) -> NetworkConnection:
        """Create a WAN connection."""
        defaults = {
            'connection_type': ConnectionType.WAN,
            'bandwidth_metrics': BandwidthMetricsFactory.create_optimal(100.0),
            'latency_metrics': LatencyMetricsFactory.create_congested(50.0),
            'connection_cost': 3.0,
            'metadata': {'provider': 'ISP-A', 'circuit_id': 'WAN-123'}
        }
        defaults.update(kwargs)
        
        return NetworkConnection(
            source_location=source,
            destination_location=destination,
            **defaults
        )
    
    @staticmethod
    def create_vpn_connection(
        source: str = "remote-office",
        destination: str = "headquarters",
        **kwargs
    ) -> NetworkConnection:
        """Create a VPN connection."""
        defaults = {
            'connection_type': ConnectionType.VPN,
            'bandwidth_metrics': BandwidthMetricsFactory.create_optimal(50.0),
            'latency_metrics': LatencyMetricsFactory.create_optimal(25.0),
            'connection_cost': 2.0,
            'metadata': {'vpn_type': 'ipsec', 'encryption': 'aes256'}
        }
        defaults.update(kwargs)
        
        return NetworkConnection(
            source_location=source,
            destination_location=destination,
            **defaults
        )
    
    @staticmethod
    def create_with_stale_metrics(
        source: str = "server-01",
        destination: str = "server-02",
        **kwargs
    ) -> NetworkConnection:
        """Create connection with stale metrics."""
        defaults = {
            'connection_type': ConnectionType.DIRECT,
            'bandwidth_metrics': BandwidthMetricsFactory.create_stale(),
            'latency_metrics': LatencyMetricsFactory.create_stale()
        }
        defaults.update(kwargs)
        
        return NetworkConnection(
            source_location=source,
            destination_location=destination,
            **defaults
        )


# Network Topology Factories
class NetworkTopologyFactory:
    """Factory for creating NetworkTopology instances with various configurations."""
    
    @staticmethod
    def create_empty(name: str = "Test Network", **kwargs) -> NetworkTopology:
        """Create an empty network topology."""
        defaults = {
            'auto_discovery_enabled': True,
            'benchmark_cache_ttl_hours': 24.0
        }
        defaults.update(kwargs)
        
        return NetworkTopology(name=name, **defaults)
    
    @staticmethod
    def create_simple_linear(
        locations: List[str] = None,
        name: str = "Linear Network",
        **kwargs
    ) -> NetworkTopology:
        """Create a simple linear network topology (A-B-C-D)."""
        if locations is None:
            locations = ["A", "B", "C", "D"]
        
        topology = NetworkTopologyFactory.create_empty(name, **kwargs)
        
        # Create linear connections
        for i in range(len(locations) - 1):
            connection = NetworkConnectionFactory.create_high_performance(
                source=locations[i],
                destination=locations[i + 1]
            )
            topology.add_connection(connection)
        
        return topology
    
    @staticmethod
    def create_star_network(
        center: str = "hub",
        spokes: List[str] = None,
        name: str = "Star Network",
        **kwargs
    ) -> NetworkTopology:
        """Create a star network topology."""
        if spokes is None:
            spokes = ["node-1", "node-2", "node-3", "node-4"]
        
        topology = NetworkTopologyFactory.create_empty(name, **kwargs)
        
        # Create connections from center to each spoke
        for spoke in spokes:
            connection = NetworkConnectionFactory.create_high_performance(
                source=center,
                destination=spoke
            )
            topology.add_connection(connection)
        
        return topology
    
    @staticmethod
    def create_diamond_network(name: str = "Diamond Network", **kwargs) -> NetworkTopology:
        """Create a diamond-shaped network (A->B->D, A->C->D)."""
        topology = NetworkTopologyFactory.create_empty(name, **kwargs)
        
        # Create diamond connections with different characteristics
        connections = [
            NetworkConnectionFactory.create_high_performance("A", "B"),
            NetworkConnectionFactory.create_high_performance("A", "C", 
                bandwidth_metrics=BandwidthMetricsFactory.create_optimal(5000.0)),
            NetworkConnectionFactory.create_high_performance("B", "D",
                latency_metrics=LatencyMetricsFactory.create_optimal(2.0)),
            NetworkConnectionFactory.create_high_performance("C", "D")
        ]
        
        for conn in connections:
            topology.add_connection(conn)
        
        return topology
    
    @staticmethod
    def create_with_bottlenecks(
        name: str = "Bottlenecked Network",
        **kwargs
    ) -> NetworkTopology:
        """Create a network with bottleneck connections."""
        topology = NetworkTopologyFactory.create_empty(name, **kwargs)
        
        connections = [
            NetworkConnectionFactory.create_bottleneck("laptop", "gateway"),
            NetworkConnectionFactory.create_high_performance("gateway", "server-farm"),
            NetworkConnectionFactory.create_wan_connection("server-farm", "remote-site"),
            NetworkConnectionFactory.create_bottleneck("remote-site", "remote-storage")
        ]
        
        for conn in connections:
            topology.add_connection(conn)
        
        return topology
    
    @staticmethod
    def create_disconnected_components(
        name: str = "Disconnected Network",
        **kwargs
    ) -> NetworkTopology:
        """Create a network with multiple disconnected components."""
        topology = NetworkTopologyFactory.create_empty(name, **kwargs)
        
        # Component 1: A-B
        topology.add_connection(
            NetworkConnectionFactory.create_high_performance("A", "B")
        )
        
        # Component 2: C-D-E
        topology.add_connection(
            NetworkConnectionFactory.create_high_performance("C", "D")
        )
        topology.add_connection(
            NetworkConnectionFactory.create_high_performance("D", "E")
        )
        
        # Component 3: F (isolated)
        # No connections for F, making it isolated
        
        return topology
    
    @staticmethod
    def create_with_stale_metrics(
        name: str = "Stale Metrics Network",
        **kwargs
    ) -> NetworkTopology:
        """Create a network with mixed fresh and stale metrics."""
        topology = NetworkTopologyFactory.create_empty(name, **kwargs)
        
        connections = [
            NetworkConnectionFactory.create_high_performance("A", "B"),  # Fresh
            NetworkConnectionFactory.create_with_stale_metrics("B", "C"),  # Stale
            NetworkConnectionFactory.create_high_performance("C", "D"),  # Fresh
            NetworkConnection("D", "E", ConnectionType.DIRECT)  # No metrics
        ]
        
        for conn in connections:
            topology.add_connection(conn)
        
        return topology


# Pytest Fixtures
@pytest.fixture
def optimal_bandwidth():
    """Provide optimal bandwidth metrics for testing."""
    return BandwidthMetricsFactory.create_optimal()


@pytest.fixture
def low_bandwidth():
    """Provide low bandwidth metrics for testing."""
    return BandwidthMetricsFactory.create_low_bandwidth()


@pytest.fixture
def stale_bandwidth():
    """Provide stale bandwidth metrics for testing."""
    return BandwidthMetricsFactory.create_stale()


@pytest.fixture
def optimal_latency():
    """Provide optimal latency metrics for testing."""
    return LatencyMetricsFactory.create_optimal()


@pytest.fixture
def degraded_latency():
    """Provide degraded latency metrics for testing."""
    return LatencyMetricsFactory.create_degraded()


@pytest.fixture
def unstable_latency():
    """Provide unstable latency metrics for testing."""
    return LatencyMetricsFactory.create_unstable()


@pytest.fixture
def direct_path():
    """Provide a direct network path for testing."""
    return NetworkPathFactory.create_direct()


@pytest.fixture
def multi_hop_path():
    """Provide a multi-hop network path for testing."""
    return NetworkPathFactory.create_multi_hop()


@pytest.fixture
def high_performance_connection():
    """Provide a high-performance network connection for testing."""
    return NetworkConnectionFactory.create_high_performance()


@pytest.fixture
def bottleneck_connection():
    """Provide a bottleneck network connection for testing."""
    return NetworkConnectionFactory.create_bottleneck()


@pytest.fixture
def vpn_connection():
    """Provide a VPN connection for testing."""
    return NetworkConnectionFactory.create_vpn_connection()


@pytest.fixture
def empty_topology():
    """Provide an empty network topology for testing."""
    return NetworkTopologyFactory.create_empty()


@pytest.fixture
def linear_topology():
    """Provide a linear network topology for testing."""
    return NetworkTopologyFactory.create_simple_linear()


@pytest.fixture
def star_topology():
    """Provide a star network topology for testing."""
    return NetworkTopologyFactory.create_star_network()


@pytest.fixture
def diamond_topology():
    """Provide a diamond network topology for testing."""
    return NetworkTopologyFactory.create_diamond_network()


@pytest.fixture
def bottlenecked_topology():
    """Provide a network topology with bottlenecks for testing."""
    return NetworkTopologyFactory.create_with_bottlenecks()


@pytest.fixture
def disconnected_topology():
    """Provide a disconnected network topology for testing."""
    return NetworkTopologyFactory.create_disconnected_components()


# Test Data Sets for Parameterized Tests
CONNECTION_TYPES_AND_COSTS = [
    (ConnectionType.DIRECT, 1.0),
    (ConnectionType.INFINIBAND, 1.0),
    (ConnectionType.LAN, 2.0),
    (ConnectionType.VPN, 3.0),
    (ConnectionType.WAN, 4.0),
    (ConnectionType.INTERNET, 5.0),
    (ConnectionType.BOTTLENECK, 10.0),
]


HEALTH_SCENARIOS = [
    (NetworkHealth.OPTIMAL, BandwidthMetricsFactory.create_optimal(), LatencyMetricsFactory.create_optimal()),
    (NetworkHealth.CONGESTED, None, LatencyMetricsFactory.create_congested()),
    (NetworkHealth.DEGRADED, None, LatencyMetricsFactory.create_degraded()),
    (NetworkHealth.UNSTABLE, None, LatencyMetricsFactory.create_unstable()),
    (NetworkHealth.UNAVAILABLE, None, None),
]


LOCATION_PAIRS = [
    ("compute-node-01", "storage-array-01"),
    ("server-primary", "server-backup"),
    ("datacenter-east", "datacenter-west"),
    ("hpc-cluster", "data-lake"),
    ("edge-device", "cloud-gateway"),
]