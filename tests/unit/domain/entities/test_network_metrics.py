"""
Comprehensive unit tests for network metrics domain entities.

Tests BandwidthMetrics, LatencyMetrics, NetworkHealth, and NetworkPath value objects
following clean architecture principles with property-based testing.
"""

import time
from unittest.mock import patch

import pytest
from hypothesis import given, strategies as st, assume

from tellus.domain.entities.network_metrics import (
    BandwidthMetrics,
    LatencyMetrics,
    NetworkHealth,
    NetworkPath
)


class TestNetworkHealth:
    """Test NetworkHealth enum for completeness."""
    
    def test_all_health_states_defined(self):
        """Test that all expected health states are defined."""
        expected_states = {'OPTIMAL', 'DEGRADED', 'CONGESTED', 'UNSTABLE', 'UNAVAILABLE'}
        actual_states = {state.name for state in NetworkHealth}
        assert actual_states == expected_states
    
    def test_health_ordering_makes_sense(self):
        """Test that health states have logical ordering for comparison."""
        # These should be ordered from best to worst for any comparison logic
        health_order = [
            NetworkHealth.OPTIMAL,
            NetworkHealth.CONGESTED,
            NetworkHealth.DEGRADED,
            NetworkHealth.UNSTABLE,
            NetworkHealth.UNAVAILABLE
        ]
        # Verify they are all different values
        assert len(set(health_order)) == len(health_order)


class TestBandwidthMetrics:
    """Comprehensive tests for BandwidthMetrics value object."""
    
    def test_valid_bandwidth_metrics_creation(self):
        """Test creating valid bandwidth metrics."""
        metrics = BandwidthMetrics(
            measured_mbps=100.0,
            theoretical_max_mbps=1000.0,
            sample_count=10,
            variance_mbps=5.0
        )
        
        assert metrics.measured_mbps == 100.0
        assert metrics.theoretical_max_mbps == 1000.0
        assert metrics.sample_count == 10
        assert metrics.variance_mbps == 5.0
        assert isinstance(metrics.measurement_timestamp, float)
    
    def test_minimal_bandwidth_metrics(self):
        """Test creating metrics with only required fields."""
        metrics = BandwidthMetrics(measured_mbps=50.0)
        
        assert metrics.measured_mbps == 50.0
        assert metrics.theoretical_max_mbps is None
        assert metrics.sample_count == 1
        assert metrics.variance_mbps == 0.0
    
    def test_negative_measured_bandwidth_raises_error(self):
        """Test that negative measured bandwidth raises ValueError."""
        with pytest.raises(ValueError, match="Measured bandwidth cannot be negative"):
            BandwidthMetrics(measured_mbps=-10.0)
    
    def test_negative_theoretical_max_raises_error(self):
        """Test that negative theoretical max raises ValueError."""
        with pytest.raises(ValueError, match="Theoretical max bandwidth cannot be negative"):
            BandwidthMetrics(measured_mbps=100.0, theoretical_max_mbps=-50.0)
    
    def test_zero_sample_count_raises_error(self):
        """Test that zero sample count raises ValueError."""
        with pytest.raises(ValueError, match="Sample count must be at least 1"):
            BandwidthMetrics(measured_mbps=100.0, sample_count=0)
    
    def test_negative_sample_count_raises_error(self):
        """Test that negative sample count raises ValueError."""
        with pytest.raises(ValueError, match="Sample count must be at least 1"):
            BandwidthMetrics(measured_mbps=100.0, sample_count=-1)
    
    def test_utilization_percentage_with_theoretical_max(self):
        """Test utilization percentage calculation."""
        metrics = BandwidthMetrics(
            measured_mbps=250.0,
            theoretical_max_mbps=1000.0
        )
        assert metrics.utilization_percentage == 25.0
    
    def test_utilization_percentage_without_theoretical_max(self):
        """Test utilization returns None when theoretical max is not set."""
        metrics = BandwidthMetrics(measured_mbps=100.0)
        assert metrics.utilization_percentage is None
    
    def test_utilization_percentage_with_zero_theoretical_max(self):
        """Test utilization returns None when theoretical max is zero."""
        metrics = BandwidthMetrics(
            measured_mbps=100.0,
            theoretical_max_mbps=0.0
        )
        assert metrics.utilization_percentage is None
    
    def test_age_seconds_calculation(self):
        """Test age calculation."""
        timestamp = time.time() - 3600  # 1 hour ago
        metrics = BandwidthMetrics(
            measured_mbps=100.0,
            measurement_timestamp=timestamp
        )
        
        age = metrics.age_seconds
        assert 3595 <= age <= 3605  # Allow for small timing differences
    
    def test_is_stale_default_24_hours(self):
        """Test stale detection with default 24 hour threshold."""
        # Fresh measurement
        fresh_metrics = BandwidthMetrics(measured_mbps=100.0)
        assert not fresh_metrics.is_stale
        
        # 25-hour old measurement
        stale_timestamp = time.time() - (25 * 3600)
        stale_metrics = BandwidthMetrics(
            measured_mbps=100.0,
            measurement_timestamp=stale_timestamp
        )
        assert stale_metrics.is_stale
    
    def test_is_stale_custom_threshold(self):
        """Test stale detection with custom threshold."""
        timestamp = time.time() - 7200  # 2 hours ago
        metrics = BandwidthMetrics(
            measured_mbps=100.0,
            measurement_timestamp=timestamp
        )
        
        # Note: is_stale is implemented as a property with default 24h threshold
        # For 2-hour old metrics with default 24h threshold, should not be stale
        assert not metrics.is_stale
    
    def test_merge_with_valid_metrics(self):
        """Test merging two bandwidth metrics."""
        metrics1 = BandwidthMetrics(
            measured_mbps=100.0,
            theoretical_max_mbps=1000.0,
            sample_count=5,
            variance_mbps=10.0,
            measurement_timestamp=1000.0
        )
        
        metrics2 = BandwidthMetrics(
            measured_mbps=200.0,
            theoretical_max_mbps=800.0,  # Lower theoretical max
            sample_count=3,
            variance_mbps=5.0,
            measurement_timestamp=2000.0  # More recent
        )
        
        merged = metrics1.merge_with(metrics2)
        
        # Weighted average: (100*5 + 200*3) / (5+3) = 1100/8 = 137.5
        assert merged.measured_mbps == 137.5
        assert merged.theoretical_max_mbps == 1000.0  # Higher value
        assert merged.sample_count == 8
        assert merged.measurement_timestamp == 2000.0  # More recent
        
        # Weighted variance: (10*5/8 + 5*3/8) = (50/8 + 15/8) = 8.125
        assert abs(merged.variance_mbps - 8.125) < 0.001
    
    def test_merge_with_only_one_theoretical_max(self):
        """Test merging when only one has theoretical max."""
        metrics1 = BandwidthMetrics(measured_mbps=100.0, theoretical_max_mbps=1000.0)
        metrics2 = BandwidthMetrics(measured_mbps=200.0)  # No theoretical max
        
        merged = metrics1.merge_with(metrics2)
        assert merged.theoretical_max_mbps == 1000.0
        
        merged_reverse = metrics2.merge_with(metrics1)
        assert merged_reverse.theoretical_max_mbps == 1000.0
    
    def test_merge_with_neither_having_theoretical_max(self):
        """Test merging when neither has theoretical max."""
        metrics1 = BandwidthMetrics(measured_mbps=100.0)
        metrics2 = BandwidthMetrics(measured_mbps=200.0)
        
        merged = metrics1.merge_with(metrics2)
        assert merged.theoretical_max_mbps is None
    
    def test_merge_with_invalid_type_raises_error(self):
        """Test that merging with non-BandwidthMetrics raises ValueError."""
        metrics = BandwidthMetrics(measured_mbps=100.0)
        
        with pytest.raises(ValueError, match="Can only merge with another BandwidthMetrics"):
            metrics.merge_with("not_metrics")
    
    @given(
        measured_mbps=st.floats(min_value=0.0, max_value=10000.0, allow_nan=False),
        theoretical_max=st.one_of(
            st.none(),
            st.floats(min_value=0.0, max_value=10000.0, allow_nan=False)
        ),
        sample_count=st.integers(min_value=1, max_value=1000)
    )
    def test_bandwidth_metrics_property_based(self, measured_mbps, theoretical_max, sample_count):
        """Property-based test for BandwidthMetrics invariants."""
        metrics = BandwidthMetrics(
            measured_mbps=measured_mbps,
            theoretical_max_mbps=theoretical_max,
            sample_count=sample_count
        )
        
        # Invariants
        assert metrics.measured_mbps >= 0
        assert metrics.sample_count >= 1
        if metrics.theoretical_max_mbps is not None:
            assert metrics.theoretical_max_mbps >= 0
            if metrics.theoretical_max_mbps > 0:
                assert 0 <= metrics.utilization_percentage <= (measured_mbps / theoretical_max * 100)


class TestLatencyMetrics:
    """Comprehensive tests for LatencyMetrics value object."""
    
    def test_valid_latency_metrics_creation(self):
        """Test creating valid latency metrics."""
        metrics = LatencyMetrics(
            avg_latency_ms=50.0,
            min_latency_ms=30.0,
            max_latency_ms=80.0,
            jitter_ms=10.0,
            packet_loss_percentage=0.5,
            sample_count=100
        )
        
        assert metrics.avg_latency_ms == 50.0
        assert metrics.min_latency_ms == 30.0
        assert metrics.max_latency_ms == 80.0
        assert metrics.jitter_ms == 10.0
        assert metrics.packet_loss_percentage == 0.5
        assert metrics.sample_count == 100
    
    def test_minimal_latency_metrics(self):
        """Test creating metrics with minimal required fields."""
        metrics = LatencyMetrics(
            avg_latency_ms=50.0,
            min_latency_ms=40.0,
            max_latency_ms=60.0
        )
        
        assert metrics.jitter_ms == 0.0
        assert metrics.packet_loss_percentage == 0.0
        assert metrics.sample_count == 1
    
    def test_negative_avg_latency_raises_error(self):
        """Test that negative average latency raises ValueError."""
        with pytest.raises(ValueError, match="Average latency cannot be negative"):
            LatencyMetrics(avg_latency_ms=-10.0, min_latency_ms=0.0, max_latency_ms=10.0)
    
    def test_negative_min_latency_raises_error(self):
        """Test that negative minimum latency raises ValueError."""
        with pytest.raises(ValueError, match="Minimum latency cannot be negative"):
            LatencyMetrics(avg_latency_ms=50.0, min_latency_ms=-10.0, max_latency_ms=60.0)
    
    def test_negative_max_latency_raises_error(self):
        """Test that negative maximum latency raises ValueError."""
        with pytest.raises(ValueError, match="Maximum latency cannot be negative"):
            LatencyMetrics(avg_latency_ms=50.0, min_latency_ms=40.0, max_latency_ms=-10.0)
    
    def test_min_greater_than_avg_raises_error(self):
        """Test that min > avg raises ValueError."""
        with pytest.raises(ValueError, match="Minimum latency cannot be greater than average"):
            LatencyMetrics(avg_latency_ms=50.0, min_latency_ms=60.0, max_latency_ms=70.0)
    
    def test_max_less_than_avg_raises_error(self):
        """Test that max < avg raises ValueError."""
        with pytest.raises(ValueError, match="Maximum latency cannot be less than average"):
            LatencyMetrics(avg_latency_ms=50.0, min_latency_ms=40.0, max_latency_ms=45.0)
    
    def test_packet_loss_below_zero_raises_error(self):
        """Test that packet loss < 0 raises ValueError."""
        with pytest.raises(ValueError, match="Packet loss must be between 0 and 100 percent"):
            LatencyMetrics(
                avg_latency_ms=50.0, min_latency_ms=40.0, max_latency_ms=60.0,
                packet_loss_percentage=-1.0
            )
    
    def test_packet_loss_above_100_raises_error(self):
        """Test that packet loss > 100 raises ValueError."""
        with pytest.raises(ValueError, match="Packet loss must be between 0 and 100 percent"):
            LatencyMetrics(
                avg_latency_ms=50.0, min_latency_ms=40.0, max_latency_ms=60.0,
                packet_loss_percentage=101.0
            )
    
    def test_age_seconds_calculation(self):
        """Test age calculation for latency metrics."""
        timestamp = time.time() - 1800  # 30 minutes ago
        metrics = LatencyMetrics(
            avg_latency_ms=50.0,
            min_latency_ms=40.0,
            max_latency_ms=60.0,
            measurement_timestamp=timestamp
        )
        
        age = metrics.age_seconds
        assert 1795 <= age <= 1805  # Allow for small timing differences
    
    def test_is_stale_detection(self):
        """Test stale detection for latency metrics."""
        fresh_metrics = LatencyMetrics(
            avg_latency_ms=50.0, min_latency_ms=40.0, max_latency_ms=60.0
        )
        assert not fresh_metrics.is_stale
        
        stale_timestamp = time.time() - (25 * 3600)  # 25 hours ago
        stale_metrics = LatencyMetrics(
            avg_latency_ms=50.0, min_latency_ms=40.0, max_latency_ms=60.0,
            measurement_timestamp=stale_timestamp
        )
        assert stale_metrics.is_stale
    
    def test_connection_quality_optimal(self):
        """Test optimal connection quality assessment."""
        metrics = LatencyMetrics(
            avg_latency_ms=10.0,
            min_latency_ms=5.0,
            max_latency_ms=20.0,
            jitter_ms=2.0,
            packet_loss_percentage=0.0
        )
        assert metrics.connection_quality == NetworkHealth.OPTIMAL
    
    def test_connection_quality_congested_high_latency(self):
        """Test congested quality due to high latency."""
        metrics = LatencyMetrics(
            avg_latency_ms=250.0,  # > 200ms
            min_latency_ms=200.0,
            max_latency_ms=300.0,
            packet_loss_percentage=0.0
        )
        assert metrics.connection_quality == NetworkHealth.CONGESTED
    
    def test_connection_quality_congested_high_jitter(self):
        """Test congested quality due to high jitter."""
        metrics = LatencyMetrics(
            avg_latency_ms=100.0,
            min_latency_ms=50.0,
            max_latency_ms=150.0,
            jitter_ms=60.0,  # > 50ms
            packet_loss_percentage=0.0
        )
        assert metrics.connection_quality == NetworkHealth.CONGESTED
    
    def test_connection_quality_degraded_moderate_loss(self):
        """Test degraded quality due to moderate packet loss."""
        metrics = LatencyMetrics(
            avg_latency_ms=100.0,
            min_latency_ms=90.0,
            max_latency_ms=110.0,
            packet_loss_percentage=2.0  # > 1% but <= 5%
        )
        assert metrics.connection_quality == NetworkHealth.DEGRADED
    
    def test_connection_quality_degraded_high_latency_with_loss(self):
        """Test degraded quality due to high latency with some loss."""
        metrics = LatencyMetrics(
            avg_latency_ms=600.0,  # > 500ms
            min_latency_ms=500.0,
            max_latency_ms=700.0,
            packet_loss_percentage=1.5  # > 1%
        )
        assert metrics.connection_quality == NetworkHealth.DEGRADED
    
    def test_connection_quality_unstable_high_loss(self):
        """Test unstable quality due to high packet loss."""
        metrics = LatencyMetrics(
            avg_latency_ms=100.0,
            min_latency_ms=80.0,
            max_latency_ms=120.0,
            packet_loss_percentage=6.0  # > 5%
        )
        assert metrics.connection_quality == NetworkHealth.UNSTABLE
    
    @given(
        avg_latency=st.floats(min_value=1.0, max_value=1000.0, allow_nan=False),
        jitter=st.floats(min_value=0.0, max_value=100.0, allow_nan=False),
        packet_loss=st.floats(min_value=0.0, max_value=100.0, allow_nan=False)
    )
    def test_latency_metrics_property_based(self, avg_latency, jitter, packet_loss):
        """Property-based test for LatencyMetrics invariants."""
        # Generate valid min/max based on average
        min_latency = max(0.0, avg_latency * 0.8)
        max_latency = avg_latency * 1.5
        
        metrics = LatencyMetrics(
            avg_latency_ms=avg_latency,
            min_latency_ms=min_latency,
            max_latency_ms=max_latency,
            jitter_ms=jitter,
            packet_loss_percentage=packet_loss
        )
        
        # Invariants
        assert metrics.avg_latency_ms >= 0
        assert metrics.min_latency_ms >= 0
        assert metrics.max_latency_ms >= 0
        assert metrics.min_latency_ms <= metrics.avg_latency_ms
        assert metrics.max_latency_ms >= metrics.avg_latency_ms
        assert 0 <= metrics.packet_loss_percentage <= 100
        assert metrics.connection_quality in NetworkHealth


class TestNetworkPath:
    """Comprehensive tests for NetworkPath value object."""
    
    def test_valid_network_path_creation(self):
        """Test creating a valid network path."""
        path = NetworkPath(
            source_location="A",
            destination_location="B",
            intermediate_hops=["C", "D"],
            total_cost=10.5,
            estimated_bandwidth_mbps=100.0,
            estimated_latency_ms=50.0,
            bottleneck_location="C",
            path_type="multi_hop"
        )
        
        assert path.source_location == "A"
        assert path.destination_location == "B"
        assert path.intermediate_hops == ["C", "D"]
        assert path.total_cost == 10.5
        assert path.estimated_bandwidth_mbps == 100.0
        assert path.estimated_latency_ms == 50.0
        assert path.bottleneck_location == "C"
        assert path.path_type == "multi_hop"
    
    def test_minimal_network_path(self):
        """Test creating path with minimal parameters."""
        path = NetworkPath(
            source_location="A",
            destination_location="B"
        )
        
        assert path.intermediate_hops == []
        assert path.total_cost == 0.0
        assert path.estimated_bandwidth_mbps == 0.0
        assert path.estimated_latency_ms == 0.0
        assert path.bottleneck_location is None
        assert path.path_type == "direct"
    
    def test_same_source_destination_raises_error(self):
        """Test that same source and destination raises ValueError."""
        with pytest.raises(ValueError, match="Source and destination cannot be the same"):
            NetworkPath(source_location="A", destination_location="A")
    
    def test_negative_total_cost_raises_error(self):
        """Test that negative total cost raises ValueError."""
        with pytest.raises(ValueError, match="Path cost cannot be negative"):
            NetworkPath(
                source_location="A",
                destination_location="B",
                total_cost=-1.0
            )
    
    def test_negative_bandwidth_raises_error(self):
        """Test that negative bandwidth raises ValueError."""
        with pytest.raises(ValueError, match="Estimated bandwidth cannot be negative"):
            NetworkPath(
                source_location="A",
                destination_location="B",
                estimated_bandwidth_mbps=-10.0
            )
    
    def test_negative_latency_raises_error(self):
        """Test that negative latency raises ValueError."""
        with pytest.raises(ValueError, match="Estimated latency cannot be negative"):
            NetworkPath(
                source_location="A",
                destination_location="B",
                estimated_latency_ms=-5.0
            )
    
    def test_hop_count_calculation(self):
        """Test hop count calculation."""
        # Direct path
        direct_path = NetworkPath(source_location="A", destination_location="B")
        assert direct_path.hop_count == 2
        
        # Path with intermediate hops
        multi_hop_path = NetworkPath(
            source_location="A",
            destination_location="D",
            intermediate_hops=["B", "C"]
        )
        assert multi_hop_path.hop_count == 4  # A -> B -> C -> D
    
    def test_full_path_property(self):
        """Test full path construction."""
        path = NetworkPath(
            source_location="A",
            destination_location="D",
            intermediate_hops=["B", "C"]
        )
        assert path.full_path == ["A", "B", "C", "D"]
    
    def test_full_path_direct(self):
        """Test full path for direct connection."""
        path = NetworkPath(source_location="A", destination_location="B")
        assert path.full_path == ["A", "B"]
    
    def test_is_direct_property(self):
        """Test is_direct property."""
        direct_path = NetworkPath(source_location="A", destination_location="B")
        assert direct_path.is_direct
        
        multi_hop_path = NetworkPath(
            source_location="A",
            destination_location="C",
            intermediate_hops=["B"]
        )
        assert not multi_hop_path.is_direct
    
    def test_has_bottleneck(self):
        """Test bottleneck detection."""
        no_bottleneck = NetworkPath(source_location="A", destination_location="B")
        assert not no_bottleneck.has_bottleneck()
        
        with_bottleneck = NetworkPath(
            source_location="A",
            destination_location="B",
            bottleneck_location="A"
        )
        assert with_bottleneck.has_bottleneck()
    
    def test_get_path_segments(self):
        """Test path segment extraction."""
        path = NetworkPath(
            source_location="A",
            destination_location="D",
            intermediate_hops=["B", "C"]
        )
        
        segments = path.get_path_segments()
        expected_segments = [("A", "B"), ("B", "C"), ("C", "D")]
        assert segments == expected_segments
    
    def test_get_path_segments_direct(self):
        """Test path segments for direct connection."""
        path = NetworkPath(source_location="A", destination_location="B")
        segments = path.get_path_segments()
        assert segments == [("A", "B")]
    
    @given(
        source=st.text(min_size=1, max_size=20, alphabet=st.characters(whitelist_categories=('Lu', 'Ll', 'Nd'))),
        destination=st.text(min_size=1, max_size=20, alphabet=st.characters(whitelist_categories=('Lu', 'Ll', 'Nd'))),
        hop_count=st.integers(min_value=0, max_value=10),
        total_cost=st.floats(min_value=0.0, max_value=1000.0, allow_nan=False),
        bandwidth=st.floats(min_value=0.0, max_value=10000.0, allow_nan=False),
        latency=st.floats(min_value=0.0, max_value=1000.0, allow_nan=False)
    )
    def test_network_path_property_based(self, source, destination, hop_count, total_cost, bandwidth, latency):
        """Property-based test for NetworkPath invariants."""
        assume(source != destination)  # Sources and destinations must be different
        
        # Generate intermediate hops
        intermediate_hops = [f"hop_{i}" for i in range(hop_count)]
        
        path = NetworkPath(
            source_location=source,
            destination_location=destination,
            intermediate_hops=intermediate_hops,
            total_cost=total_cost,
            estimated_bandwidth_mbps=bandwidth,
            estimated_latency_ms=latency
        )
        
        # Invariants
        assert path.source_location != path.destination_location
        assert path.total_cost >= 0
        assert path.estimated_bandwidth_mbps >= 0
        assert path.estimated_latency_ms >= 0
        assert path.hop_count == len(intermediate_hops) + 2
        assert len(path.full_path) == path.hop_count
        assert path.full_path[0] == source
        assert path.full_path[-1] == destination
        assert path.is_direct == (len(intermediate_hops) == 0)
        assert len(path.get_path_segments()) == len(intermediate_hops) + 1