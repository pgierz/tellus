"""
Network connection value objects for network topology management.
"""

from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Dict, Optional

from .network_metrics import BandwidthMetrics, LatencyMetrics, NetworkHealth


class ConnectionType(Enum):
    """Types of network connections between locations."""
    DIRECT = auto()  # Direct network connection
    VPN = auto()     # VPN tunnel
    WAN = auto()     # Wide Area Network
    LAN = auto()     # Local Area Network
    INTERNET = auto() # Internet connection
    INFINIBAND = auto() # High-performance InfiniBand
    BOTTLENECK = auto() # Known bottleneck connection (e.g., laptop WiFi)


@dataclass
class NetworkConnection:
    """
    Domain entity representing a network connection between two locations.
    """
    source_location: str
    destination_location: str
    connection_type: ConnectionType
    bandwidth_metrics: Optional[BandwidthMetrics] = None
    latency_metrics: Optional[LatencyMetrics] = None
    is_bidirectional: bool = True
    connection_cost: float = 1.0  # Lower cost = preferred route
    metadata: Dict[str, str] = field(default_factory=dict)
    
    def __post_init__(self):
        """Validate network connection."""
        if self.source_location == self.destination_location:
            raise ValueError("Source and destination must be different")
        if self.connection_cost < 0:
            raise ValueError("Connection cost cannot be negative")
        if not isinstance(self.source_location, str) or not self.source_location:
            raise ValueError("Source location must be a non-empty string")
        if not isinstance(self.destination_location, str) or not self.destination_location:
            raise ValueError("Destination location must be a non-empty string")
    
    @property
    def connection_id(self) -> str:
        """Generate unique identifier for this connection."""
        locations = sorted([self.source_location, self.destination_location])
        return f"{locations[0]}<->{locations[1]}"
    
    @property
    def current_health(self) -> NetworkHealth:
        """Assess current health based on available metrics."""
        if self.latency_metrics:
            return self.latency_metrics.connection_quality
        elif self.bandwidth_metrics and self.bandwidth_metrics.is_stale:
            return NetworkHealth.DEGRADED
        elif not self.bandwidth_metrics and not self.latency_metrics:
            return NetworkHealth.UNAVAILABLE
        else:
            return NetworkHealth.OPTIMAL
    
    @property
    def effective_bandwidth_mbps(self) -> float:
        """Get effective bandwidth considering connection health."""
        if not self.bandwidth_metrics:
            return 0.0
        
        health = self.current_health
        if health == NetworkHealth.UNAVAILABLE:
            return 0.0
        elif health == NetworkHealth.UNSTABLE:
            return self.bandwidth_metrics.measured_mbps * 0.3
        elif health == NetworkHealth.DEGRADED:
            return self.bandwidth_metrics.measured_mbps * 0.6
        elif health == NetworkHealth.CONGESTED:
            return self.bandwidth_metrics.measured_mbps * 0.8
        else:
            return self.bandwidth_metrics.measured_mbps
    
    def is_bottleneck_connection(self) -> bool:
        """
        Check if this connection is likely to be a bottleneck.
        
        A connection is considered a bottleneck if:
        - It's explicitly marked as BOTTLENECK type
        - Bandwidth is significantly lower than typical values
        - High latency or packet loss indicates congestion
        """
        if self.connection_type == ConnectionType.BOTTLENECK:
            return True
        
        if self.bandwidth_metrics and self.bandwidth_metrics.measured_mbps < 10.0:  # < 10 Mbps
            return True
        
        if (self.latency_metrics and 
            (self.latency_metrics.avg_latency_ms > 100 or 
             self.latency_metrics.packet_loss_percentage > 2.0)):
            return True
        
        return False
    
    def update_bandwidth_metrics(self, new_metrics: BandwidthMetrics) -> None:
        """Update bandwidth metrics, merging with existing if available."""
        if not isinstance(new_metrics, BandwidthMetrics):
            raise ValueError("Must provide BandwidthMetrics instance")
        
        if self.bandwidth_metrics:
            self.bandwidth_metrics = self.bandwidth_metrics.merge_with(new_metrics)
        else:
            self.bandwidth_metrics = new_metrics
    
    def update_latency_metrics(self, new_metrics: LatencyMetrics) -> None:
        """Update latency metrics."""
        if not isinstance(new_metrics, LatencyMetrics):
            raise ValueError("Must provide LatencyMetrics instance")
        
        self.latency_metrics = new_metrics
    
    def can_connect_locations(self, loc1: str, loc2: str) -> bool:
        """Check if this connection can be used to connect two locations."""
        if not self.is_bidirectional:
            return ((self.source_location == loc1 and self.destination_location == loc2) or
                   (self.source_location == loc2 and self.destination_location == loc1))
        else:
            return ((self.source_location in (loc1, loc2)) and 
                   (self.destination_location in (loc1, loc2)) and
                   loc1 != loc2)
    
    def get_transfer_direction(self, from_location: str, to_location: str) -> Optional[str]:
        """
        Get transfer direction for this connection.
        
        Returns 'forward', 'reverse', or None if connection cannot handle transfer.
        """
        if self.source_location == from_location and self.destination_location == to_location:
            return 'forward'
        elif self.is_bidirectional and self.source_location == to_location and self.destination_location == from_location:
            return 'reverse'
        else:
            return None