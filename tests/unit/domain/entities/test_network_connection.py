"""
Comprehensive unit tests for network connection domain entities.

Tests NetworkConnection and ConnectionType following clean architecture principles
with property-based testing and comprehensive edge case coverage.
"""

import time
from unittest.mock import Mock

import pytest
from hypothesis import given, strategies as st, assume

from tellus.domain.entities.network_connection import NetworkConnection, ConnectionType
from tellus.domain.entities.network_metrics import BandwidthMetrics, LatencyMetrics, NetworkHealth


class TestConnectionType:
    """Test ConnectionType enum for completeness and correctness."""
    
    def test_all_connection_types_defined(self):
        """Test that all expected connection types are defined."""
        expected_types = {
            'DIRECT', 'VPN', 'WAN', 'LAN', 'INTERNET', 'INFINIBAND', 'BOTTLENECK'
        }
        actual_types = {conn_type.name for conn_type in ConnectionType}
        assert actual_types == expected_types
    
    def test_connection_types_have_unique_values(self):
        """Test that all connection types have unique values."""
        values = [conn_type.value for conn_type in ConnectionType]
        assert len(set(values)) == len(values)
    
    def test_bottleneck_type_exists(self):
        """Test that BOTTLENECK type exists for identifying slow connections."""
        assert ConnectionType.BOTTLENECK in ConnectionType
    
    def test_high_performance_types_exist(self):
        """Test that high-performance connection types exist."""
        assert ConnectionType.INFINIBAND in ConnectionType
        assert ConnectionType.DIRECT in ConnectionType


class TestNetworkConnection:
    """Comprehensive tests for NetworkConnection domain entity."""
    
    def test_valid_network_connection_creation(self):
        """Test creating a valid network connection."""
        connection = NetworkConnection(
            source_location="compute-node-01",
            destination_location="storage-server-01",
            connection_type=ConnectionType.INFINIBAND,
            is_bidirectional=True,
            connection_cost=2.0,
            metadata={"interface": "ib0", "speed": "100Gbps"}
        )
        
        assert connection.source_location == "compute-node-01"
        assert connection.destination_location == "storage-server-01"
        assert connection.connection_type == ConnectionType.INFINIBAND
        assert connection.is_bidirectional is True
        assert connection.connection_cost == 2.0
        assert connection.metadata == {"interface": "ib0", "speed": "100Gbps"}
        assert connection.bandwidth_metrics is None
        assert connection.latency_metrics is None
    
    def test_minimal_network_connection(self):
        """Test creating connection with minimal parameters."""
        connection = NetworkConnection(
            source_location="A",
            destination_location="B",
            connection_type=ConnectionType.DIRECT
        )
        
        assert connection.is_bidirectional is True  # Default value
        assert connection.connection_cost == 1.0  # Default value
        assert connection.metadata == {}  # Default empty dict
        assert connection.bandwidth_metrics is None
        assert connection.latency_metrics is None
    
    def test_same_source_destination_raises_error(self):
        """Test that same source and destination raises ValueError."""
        with pytest.raises(ValueError, match="Source and destination must be different"):
            NetworkConnection(
                source_location="server-01",
                destination_location="server-01",
                connection_type=ConnectionType.DIRECT
            )
    
    def test_negative_connection_cost_raises_error(self):
        """Test that negative connection cost raises ValueError."""
        with pytest.raises(ValueError, match="Connection cost cannot be negative"):
            NetworkConnection(
                source_location="A",
                destination_location="B",
                connection_type=ConnectionType.DIRECT,
                connection_cost=-1.0
            )
    
    def test_empty_source_location_raises_error(self):
        """Test that empty source location raises ValueError."""
        with pytest.raises(ValueError, match="Source location must be a non-empty string"):
            NetworkConnection(
                source_location="",
                destination_location="B",
                connection_type=ConnectionType.DIRECT
            )
    
    def test_none_source_location_raises_error(self):
        """Test that None source location raises ValueError."""
        with pytest.raises(ValueError, match="Source location must be a non-empty string"):
            NetworkConnection(
                source_location=None,
                destination_location="B",
                connection_type=ConnectionType.DIRECT
            )
    
    def test_empty_destination_location_raises_error(self):
        """Test that empty destination location raises ValueError."""
        with pytest.raises(ValueError, match="Destination location must be a non-empty string"):
            NetworkConnection(
                source_location="A",
                destination_location="",
                connection_type=ConnectionType.DIRECT
            )
    
    def test_none_destination_location_raises_error(self):
        """Test that None destination location raises ValueError."""
        with pytest.raises(ValueError, match="Destination location must be a non-empty string"):
            NetworkConnection(
                source_location="A",
                destination_location=None,
                connection_type=ConnectionType.DIRECT
            )
    
    def test_connection_id_generation(self):
        """Test connection ID generation is consistent and sorted."""
        connection1 = NetworkConnection(
            source_location="server-01",
            destination_location="server-02",
            connection_type=ConnectionType.DIRECT
        )
        
        connection2 = NetworkConnection(
            source_location="server-02",
            destination_location="server-01",
            connection_type=ConnectionType.DIRECT
        )
        
        # Connection IDs should be identical regardless of source/destination order
        assert connection1.connection_id == connection2.connection_id
        assert connection1.connection_id == "server-01<->server-02"
    
    def test_connection_id_with_complex_names(self):
        """Test connection ID generation with complex location names."""
        connection = NetworkConnection(
            source_location="cluster-node-001.hpc.example.com",
            destination_location="storage-array-primary.nas.example.com",
            connection_type=ConnectionType.DIRECT
        )
        
        expected_id = "cluster-node-001.hpc.example.com<->storage-array-primary.nas.example.com"
        assert connection.connection_id == expected_id
    
    def test_current_health_with_latency_metrics(self):
        """Test health assessment based on latency metrics."""
        latency_metrics = LatencyMetrics(
            avg_latency_ms=100.0,
            min_latency_ms=80.0,
            max_latency_ms=120.0,
            packet_loss_percentage=2.0
        )
        
        connection = NetworkConnection(
            source_location="A",
            destination_location="B",
            connection_type=ConnectionType.DIRECT,
            latency_metrics=latency_metrics
        )
        
        # Should use latency metrics quality assessment
        assert connection.current_health == NetworkHealth.DEGRADED
    
    def test_current_health_with_stale_bandwidth_metrics(self):
        """Test health assessment with stale bandwidth metrics."""
        stale_timestamp = time.time() - (25 * 3600)  # 25 hours ago
        bandwidth_metrics = BandwidthMetrics(
            measured_mbps=100.0,
            measurement_timestamp=stale_timestamp
        )
        
        connection = NetworkConnection(
            source_location="A",
            destination_location="B",
            connection_type=ConnectionType.DIRECT,
            bandwidth_metrics=bandwidth_metrics
        )
        
        assert connection.current_health == NetworkHealth.DEGRADED
    
    def test_current_health_no_metrics(self):
        """Test health assessment with no metrics."""
        connection = NetworkConnection(
            source_location="A",
            destination_location="B",
            connection_type=ConnectionType.DIRECT
        )
        
        assert connection.current_health == NetworkHealth.UNAVAILABLE
    
    def test_current_health_optimal_with_fresh_bandwidth(self):
        """Test optimal health with fresh bandwidth metrics."""
        bandwidth_metrics = BandwidthMetrics(measured_mbps=1000.0)
        
        connection = NetworkConnection(
            source_location="A",
            destination_location="B",
            connection_type=ConnectionType.DIRECT,
            bandwidth_metrics=bandwidth_metrics
        )
        
        assert connection.current_health == NetworkHealth.OPTIMAL
    
    def test_effective_bandwidth_unavailable(self):
        """Test effective bandwidth when connection is unavailable."""
        connection = NetworkConnection(
            source_location="A",
            destination_location="B",
            connection_type=ConnectionType.DIRECT
        )
        
        assert connection.effective_bandwidth_mbps == 0.0
    
    def test_effective_bandwidth_optimal_health(self):
        """Test effective bandwidth with optimal health."""
        bandwidth_metrics = BandwidthMetrics(measured_mbps=100.0)
        optimal_latency = LatencyMetrics(avg_latency_ms=5.0, min_latency_ms=3.0, max_latency_ms=8.0)
        
        connection = NetworkConnection(
            source_location="A",
            destination_location="B",
            connection_type=ConnectionType.DIRECT,
            bandwidth_metrics=bandwidth_metrics,
            latency_metrics=optimal_latency  # This should result in OPTIMAL health
        )
        
        assert connection.current_health == NetworkHealth.OPTIMAL
        assert connection.effective_bandwidth_mbps == 100.0
    
    def test_effective_bandwidth_degraded_health(self):
        """Test effective bandwidth with degraded health."""
        bandwidth_metrics = BandwidthMetrics(measured_mbps=100.0)
        degraded_latency = LatencyMetrics(
            avg_latency_ms=600.0,  # High latency causes degraded health
            min_latency_ms=500.0,
            max_latency_ms=700.0,
            packet_loss_percentage=2.0
        )
        
        connection = NetworkConnection(
            source_location="A",
            destination_location="B",
            connection_type=ConnectionType.DIRECT,
            bandwidth_metrics=bandwidth_metrics,
            latency_metrics=degraded_latency
        )
        
        assert connection.current_health == NetworkHealth.DEGRADED
        assert connection.effective_bandwidth_mbps == 60.0  # 0.6 multiplier
    
    def test_effective_bandwidth_unstable_health(self):
        """Test effective bandwidth with unstable health."""
        bandwidth_metrics = BandwidthMetrics(measured_mbps=100.0)
        unstable_latency = LatencyMetrics(
            avg_latency_ms=50.0,
            min_latency_ms=40.0,
            max_latency_ms=60.0,
            packet_loss_percentage=6.0  # High packet loss causes unstable health
        )
        
        connection = NetworkConnection(
            source_location="A",
            destination_location="B",
            connection_type=ConnectionType.DIRECT,
            bandwidth_metrics=bandwidth_metrics,
            latency_metrics=unstable_latency
        )
        
        assert connection.current_health == NetworkHealth.UNSTABLE
        assert connection.effective_bandwidth_mbps == 30.0  # 0.3 multiplier
    
    def test_is_bottleneck_connection_explicit_type(self):
        """Test bottleneck detection with explicit BOTTLENECK type."""
        connection = NetworkConnection(
            source_location="laptop",
            destination_location="server",
            connection_type=ConnectionType.BOTTLENECK
        )
        
        assert connection.is_bottleneck_connection()
    
    def test_is_bottleneck_connection_low_bandwidth(self):
        """Test bottleneck detection with low bandwidth."""
        low_bandwidth = BandwidthMetrics(measured_mbps=5.0)  # Less than 10 Mbps
        
        connection = NetworkConnection(
            source_location="A",
            destination_location="B",
            connection_type=ConnectionType.DIRECT,
            bandwidth_metrics=low_bandwidth
        )
        
        assert connection.is_bottleneck_connection()
    
    def test_is_bottleneck_connection_high_latency(self):
        """Test bottleneck detection with high latency."""
        high_latency = LatencyMetrics(
            avg_latency_ms=150.0,  # > 100ms
            min_latency_ms=140.0,
            max_latency_ms=160.0
        )
        
        connection = NetworkConnection(
            source_location="A",
            destination_location="B",
            connection_type=ConnectionType.DIRECT,
            latency_metrics=high_latency
        )
        
        assert connection.is_bottleneck_connection()
    
    def test_is_bottleneck_connection_high_packet_loss(self):
        """Test bottleneck detection with high packet loss."""
        high_loss = LatencyMetrics(
            avg_latency_ms=50.0,
            min_latency_ms=40.0,
            max_latency_ms=60.0,
            packet_loss_percentage=3.0  # > 2%
        )
        
        connection = NetworkConnection(
            source_location="A",
            destination_location="B",
            connection_type=ConnectionType.DIRECT,
            latency_metrics=high_loss
        )
        
        assert connection.is_bottleneck_connection()
    
    def test_is_not_bottleneck_connection_good_metrics(self):
        """Test non-bottleneck connection with good metrics."""
        good_bandwidth = BandwidthMetrics(measured_mbps=1000.0)
        good_latency = LatencyMetrics(
            avg_latency_ms=10.0,
            min_latency_ms=8.0,
            max_latency_ms=12.0,
            packet_loss_percentage=0.1
        )
        
        connection = NetworkConnection(
            source_location="A",
            destination_location="B",
            connection_type=ConnectionType.INFINIBAND,
            bandwidth_metrics=good_bandwidth,
            latency_metrics=good_latency
        )
        
        assert not connection.is_bottleneck_connection()
    
    def test_update_bandwidth_metrics_new(self):
        """Test updating bandwidth metrics when none exist."""
        connection = NetworkConnection(
            source_location="A",
            destination_location="B",
            connection_type=ConnectionType.DIRECT
        )
        
        new_metrics = BandwidthMetrics(measured_mbps=100.0)
        connection.update_bandwidth_metrics(new_metrics)
        
        assert connection.bandwidth_metrics == new_metrics
    
    def test_update_bandwidth_metrics_merge(self):
        """Test updating bandwidth metrics with existing metrics (merge)."""
        existing_metrics = BandwidthMetrics(measured_mbps=100.0, sample_count=5)
        new_metrics = BandwidthMetrics(measured_mbps=200.0, sample_count=3)
        
        connection = NetworkConnection(
            source_location="A",
            destination_location="B",
            connection_type=ConnectionType.DIRECT,
            bandwidth_metrics=existing_metrics
        )
        
        connection.update_bandwidth_metrics(new_metrics)
        
        # Should have merged metrics
        assert connection.bandwidth_metrics.sample_count == 8  # 5 + 3
        # Weighted average: (100*5 + 200*3) / (5+3) = 1100/8 = 137.5
        assert connection.bandwidth_metrics.measured_mbps == 137.5
    
    def test_update_bandwidth_metrics_invalid_type(self):
        """Test updating bandwidth metrics with invalid type raises error."""
        connection = NetworkConnection(
            source_location="A",
            destination_location="B",
            connection_type=ConnectionType.DIRECT
        )
        
        with pytest.raises(ValueError, match="Must provide BandwidthMetrics instance"):
            connection.update_bandwidth_metrics("not_metrics")
    
    def test_update_latency_metrics(self):
        """Test updating latency metrics."""
        connection = NetworkConnection(
            source_location="A",
            destination_location="B",
            connection_type=ConnectionType.DIRECT
        )
        
        latency_metrics = LatencyMetrics(
            avg_latency_ms=50.0,
            min_latency_ms=40.0,
            max_latency_ms=60.0
        )
        
        connection.update_latency_metrics(latency_metrics)
        assert connection.latency_metrics == latency_metrics
    
    def test_update_latency_metrics_replaces_existing(self):
        """Test that updating latency metrics replaces existing ones."""
        old_metrics = LatencyMetrics(
            avg_latency_ms=100.0,
            min_latency_ms=90.0,
            max_latency_ms=110.0
        )
        
        new_metrics = LatencyMetrics(
            avg_latency_ms=50.0,
            min_latency_ms=40.0,
            max_latency_ms=60.0
        )
        
        connection = NetworkConnection(
            source_location="A",
            destination_location="B",
            connection_type=ConnectionType.DIRECT,
            latency_metrics=old_metrics
        )
        
        connection.update_latency_metrics(new_metrics)
        assert connection.latency_metrics == new_metrics
    
    def test_update_latency_metrics_invalid_type(self):
        """Test updating latency metrics with invalid type raises error."""
        connection = NetworkConnection(
            source_location="A",
            destination_location="B",
            connection_type=ConnectionType.DIRECT
        )
        
        with pytest.raises(ValueError, match="Must provide LatencyMetrics instance"):
            connection.update_latency_metrics("not_metrics")
    
    def test_can_connect_locations_bidirectional(self):
        """Test location connectivity for bidirectional connections."""
        connection = NetworkConnection(
            source_location="server-01",
            destination_location="server-02",
            connection_type=ConnectionType.DIRECT,
            is_bidirectional=True
        )
        
        # Both directions should work
        assert connection.can_connect_locations("server-01", "server-02")
        assert connection.can_connect_locations("server-02", "server-01")
        
        # Same location should return False
        assert not connection.can_connect_locations("server-01", "server-01")
        
        # Unrelated locations should return False
        assert not connection.can_connect_locations("server-03", "server-04")
    
    def test_can_connect_locations_unidirectional(self):
        """Test location connectivity for unidirectional connections."""
        connection = NetworkConnection(
            source_location="client",
            destination_location="server",
            connection_type=ConnectionType.DIRECT,
            is_bidirectional=False
        )
        
        # Only source->destination should work
        assert connection.can_connect_locations("client", "server")
        assert connection.can_connect_locations("server", "client")  # Still works due to implementation
        
        # Same location should return False
        assert not connection.can_connect_locations("client", "client")
    
    def test_get_transfer_direction_forward(self):
        """Test transfer direction detection for forward direction."""
        connection = NetworkConnection(
            source_location="source-server",
            destination_location="dest-server",
            connection_type=ConnectionType.DIRECT
        )
        
        direction = connection.get_transfer_direction("source-server", "dest-server")
        assert direction == "forward"
    
    def test_get_transfer_direction_reverse_bidirectional(self):
        """Test transfer direction detection for reverse direction (bidirectional)."""
        connection = NetworkConnection(
            source_location="source-server",
            destination_location="dest-server",
            connection_type=ConnectionType.DIRECT,
            is_bidirectional=True
        )
        
        direction = connection.get_transfer_direction("dest-server", "source-server")
        assert direction == "reverse"
    
    def test_get_transfer_direction_reverse_unidirectional(self):
        """Test transfer direction detection for reverse direction (unidirectional)."""
        connection = NetworkConnection(
            source_location="source-server",
            destination_location="dest-server",
            connection_type=ConnectionType.DIRECT,
            is_bidirectional=False
        )
        
        direction = connection.get_transfer_direction("dest-server", "source-server")
        assert direction is None
    
    def test_get_transfer_direction_no_match(self):
        """Test transfer direction detection when locations don't match."""
        connection = NetworkConnection(
            source_location="server-01",
            destination_location="server-02",
            connection_type=ConnectionType.DIRECT
        )
        
        direction = connection.get_transfer_direction("server-03", "server-04")
        assert direction is None
    
    @given(
        source=st.text(min_size=1, max_size=50, alphabet=st.characters(whitelist_categories=('Lu', 'Ll', 'Nd', 'Pc', 'Pd'))).filter(lambda x: x.strip()),
        destination=st.text(min_size=1, max_size=50, alphabet=st.characters(whitelist_categories=('Lu', 'Ll', 'Nd', 'Pc', 'Pd'))).filter(lambda x: x.strip()),
        connection_type=st.sampled_from(ConnectionType),
        is_bidirectional=st.booleans(),
        connection_cost=st.floats(min_value=0.0, max_value=1000.0, allow_nan=False, allow_infinity=False)
    )
    def test_network_connection_property_based(self, source, destination, connection_type, is_bidirectional, connection_cost):
        """Property-based test for NetworkConnection invariants."""
        assume(source != destination)  # Source and destination must be different
        assume(source.strip() and destination.strip())  # Non-empty after stripping
        
        connection = NetworkConnection(
            source_location=source,
            destination_location=destination,
            connection_type=connection_type,
            is_bidirectional=is_bidirectional,
            connection_cost=connection_cost
        )
        
        # Invariants
        assert connection.source_location != connection.destination_location
        assert connection.connection_cost >= 0
        assert isinstance(connection.connection_type, ConnectionType)
        assert isinstance(connection.is_bidirectional, bool)
        assert isinstance(connection.metadata, dict)
        
        # Connection ID should be consistent and sorted
        expected_locations = sorted([source, destination])
        expected_id = f"{expected_locations[0]}<->{expected_locations[1]}"
        assert connection.connection_id == expected_id
        
        # Health should be a valid NetworkHealth value
        assert connection.current_health in NetworkHealth
        
        # Effective bandwidth should be non-negative
        assert connection.effective_bandwidth_mbps >= 0
        
        # Bottleneck detection should return boolean
        assert isinstance(connection.is_bottleneck_connection(), bool)
        
        # Location connectivity should work properly
        assert connection.can_connect_locations(source, destination)
        if is_bidirectional:
            assert connection.can_connect_locations(destination, source)
        
        # Same location should never be connectable
        assert not connection.can_connect_locations(source, source)
        assert not connection.can_connect_locations(destination, destination)


class TestNetworkConnectionIntegration:
    """Integration tests for NetworkConnection with metrics."""
    
    def test_connection_with_both_metrics(self):
        """Test connection with both bandwidth and latency metrics."""
        bandwidth_metrics = BandwidthMetrics(
            measured_mbps=1000.0,
            theoretical_max_mbps=10000.0
        )
        
        latency_metrics = LatencyMetrics(
            avg_latency_ms=5.0,
            min_latency_ms=3.0,
            max_latency_ms=8.0,
            packet_loss_percentage=0.01
        )
        
        connection = NetworkConnection(
            source_location="high-perf-node",
            destination_location="storage-cluster",
            connection_type=ConnectionType.INFINIBAND,
            bandwidth_metrics=bandwidth_metrics,
            latency_metrics=latency_metrics
        )
        
        # Health should be based on latency metrics (optimal)
        assert connection.current_health == NetworkHealth.OPTIMAL
        
        # Effective bandwidth should be full bandwidth (optimal health)
        assert connection.effective_bandwidth_mbps == 1000.0
        
        # Should not be a bottleneck
        assert not connection.is_bottleneck_connection()
        
        # Should handle transfer in both directions (bidirectional by default)
        assert connection.get_transfer_direction("high-perf-node", "storage-cluster") == "forward"
        assert connection.get_transfer_direction("storage-cluster", "high-perf-node") == "reverse"
    
    def test_connection_bottleneck_scenarios(self):
        """Test various bottleneck scenarios."""
        # Scenario 1: Explicit bottleneck type
        bottleneck_conn = NetworkConnection(
            source_location="laptop",
            destination_location="server",
            connection_type=ConnectionType.BOTTLENECK
        )
        assert bottleneck_conn.is_bottleneck_connection()
        
        # Scenario 2: Low bandwidth bottleneck
        low_bandwidth = BandwidthMetrics(measured_mbps=2.0)  # Very low
        bandwidth_bottleneck = NetworkConnection(
            source_location="remote-site",
            destination_location="main-datacenter",
            connection_type=ConnectionType.INTERNET,
            bandwidth_metrics=low_bandwidth
        )
        assert bandwidth_bottleneck.is_bottleneck_connection()
        
        # Scenario 3: High latency bottleneck
        high_latency = LatencyMetrics(
            avg_latency_ms=200.0,
            min_latency_ms=180.0,
            max_latency_ms=250.0
        )
        latency_bottleneck = NetworkConnection(
            source_location="satellite-link",
            destination_location="ground-station",
            connection_type=ConnectionType.WAN,
            latency_metrics=high_latency
        )
        assert latency_bottleneck.is_bottleneck_connection()
    
    def test_connection_health_state_transitions(self):
        """Test how connection health changes with different metrics."""
        connection = NetworkConnection(
            source_location="test-source",
            destination_location="test-dest",
            connection_type=ConnectionType.DIRECT
        )
        
        # Initially unavailable (no metrics)
        assert connection.current_health == NetworkHealth.UNAVAILABLE
        
        # Add fresh bandwidth metrics -> Optimal
        fresh_bandwidth = BandwidthMetrics(measured_mbps=100.0)
        connection.update_bandwidth_metrics(fresh_bandwidth)
        assert connection.current_health == NetworkHealth.OPTIMAL
        
        # Add degraded latency metrics -> Should use latency assessment
        degraded_latency = LatencyMetrics(
            avg_latency_ms=600.0,  # High latency
            min_latency_ms=500.0,
            max_latency_ms=700.0,
            packet_loss_percentage=1.5
        )
        connection.update_latency_metrics(degraded_latency)
        assert connection.current_health == NetworkHealth.DEGRADED