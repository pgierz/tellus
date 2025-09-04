"""
Performance measurement value objects for network topology management.
"""

import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import List, Optional, Tuple


class NetworkHealth(Enum):
    """Health status of network connections."""
    OPTIMAL = auto()    # Connection performing optimally
    DEGRADED = auto()   # Reduced performance
    CONGESTED = auto()  # High congestion
    UNSTABLE = auto()   # Intermittent failures
    UNAVAILABLE = auto() # Connection down


@dataclass
class BandwidthMetrics:
    """
    Value object representing bandwidth measurements and statistics.
    """
    measured_mbps: float
    theoretical_max_mbps: Optional[float] = None
    measurement_timestamp: float = field(default_factory=time.time)
    sample_count: int = 1
    variance_mbps: float = 0.0
    
    def __post_init__(self):
        """Validate bandwidth metrics."""
        if self.measured_mbps < 0:
            raise ValueError("Measured bandwidth cannot be negative")
        if self.theoretical_max_mbps is not None and self.theoretical_max_mbps < 0:
            raise ValueError("Theoretical max bandwidth cannot be negative")
        if self.sample_count < 1:
            raise ValueError("Sample count must be at least 1")
    
    @property
    def utilization_percentage(self) -> Optional[float]:
        """Calculate bandwidth utilization as percentage of theoretical max."""
        if self.theoretical_max_mbps is None or self.theoretical_max_mbps == 0:
            return None
        return (self.measured_mbps / self.theoretical_max_mbps) * 100
    
    @property
    def age_seconds(self) -> float:
        """Get age of measurement in seconds."""
        return time.time() - self.measurement_timestamp
    
    @property
    def is_stale(self, max_age_hours: float = 24.0) -> bool:
        """Check if measurement is stale (older than max_age_hours)."""
        return self.age_seconds > (max_age_hours * 3600)
    
    def merge_with(self, other: 'BandwidthMetrics') -> 'BandwidthMetrics':
        """
        Merge this measurement with another to create aggregated statistics.
        
        Uses exponential moving average for bandwidth and variance calculations.
        """
        if not isinstance(other, BandwidthMetrics):
            raise ValueError("Can only merge with another BandwidthMetrics")
        
        total_samples = self.sample_count + other.sample_count
        
        # Weighted average based on sample counts
        weight_self = self.sample_count / total_samples
        weight_other = other.sample_count / total_samples
        
        merged_bandwidth = (self.measured_mbps * weight_self + 
                          other.measured_mbps * weight_other)
        
        # Use more recent timestamp
        merged_timestamp = max(self.measurement_timestamp, other.measurement_timestamp)
        
        # Combine theoretical max (use higher value if both available)
        merged_theoretical = None
        if self.theoretical_max_mbps and other.theoretical_max_mbps:
            merged_theoretical = max(self.theoretical_max_mbps, other.theoretical_max_mbps)
        elif self.theoretical_max_mbps:
            merged_theoretical = self.theoretical_max_mbps
        elif other.theoretical_max_mbps:
            merged_theoretical = other.theoretical_max_mbps
        
        # Combine variance (simplified approach)
        merged_variance = (self.variance_mbps * weight_self + 
                         other.variance_mbps * weight_other)
        
        return BandwidthMetrics(
            measured_mbps=merged_bandwidth,
            theoretical_max_mbps=merged_theoretical,
            measurement_timestamp=merged_timestamp,
            sample_count=total_samples,
            variance_mbps=merged_variance
        )


@dataclass
class LatencyMetrics:
    """
    Value object representing network latency measurements.
    """
    avg_latency_ms: float
    min_latency_ms: float
    max_latency_ms: float
    jitter_ms: float = 0.0
    packet_loss_percentage: float = 0.0
    measurement_timestamp: float = field(default_factory=time.time)
    sample_count: int = 1
    
    def __post_init__(self):
        """Validate latency metrics."""
        if self.avg_latency_ms < 0:
            raise ValueError("Average latency cannot be negative")
        if self.min_latency_ms < 0:
            raise ValueError("Minimum latency cannot be negative")
        if self.max_latency_ms < 0:
            raise ValueError("Maximum latency cannot be negative")
        if self.min_latency_ms > self.avg_latency_ms:
            raise ValueError("Minimum latency cannot be greater than average")
        if self.max_latency_ms < self.avg_latency_ms:
            raise ValueError("Maximum latency cannot be less than average")
        if not 0 <= self.packet_loss_percentage <= 100:
            raise ValueError("Packet loss must be between 0 and 100 percent")
    
    @property
    def age_seconds(self) -> float:
        """Get age of measurement in seconds."""
        return time.time() - self.measurement_timestamp
    
    @property
    def is_stale(self, max_age_hours: float = 24.0) -> bool:
        """Check if measurement is stale."""
        return self.age_seconds > (max_age_hours * 3600)
    
    @property
    def connection_quality(self) -> NetworkHealth:
        """Assess connection quality based on latency and packet loss."""
        if self.packet_loss_percentage > 5.0:
            return NetworkHealth.UNSTABLE
        elif self.packet_loss_percentage > 1.0 or self.avg_latency_ms > 500:
            return NetworkHealth.DEGRADED
        elif self.avg_latency_ms > 200 or self.jitter_ms > 50:
            return NetworkHealth.CONGESTED
        else:
            return NetworkHealth.OPTIMAL


@dataclass
class NetworkPath:
    """
    Value object representing a path through the network topology.
    """
    source_location: str
    destination_location: str
    intermediate_hops: List[str] = field(default_factory=list)
    total_cost: float = 0.0
    estimated_bandwidth_mbps: float = 0.0
    estimated_latency_ms: float = 0.0
    bottleneck_location: Optional[str] = None
    path_type: str = "direct"  # direct, multi_hop, optimized
    
    def __post_init__(self):
        """Validate network path."""
        if self.source_location == self.destination_location:
            raise ValueError("Source and destination cannot be the same")
        if self.total_cost < 0:
            raise ValueError("Path cost cannot be negative")
        if self.estimated_bandwidth_mbps < 0:
            raise ValueError("Estimated bandwidth cannot be negative")
        if self.estimated_latency_ms < 0:
            raise ValueError("Estimated latency cannot be negative")
    
    @property
    def hop_count(self) -> int:
        """Get total number of hops including source and destination."""
        return len(self.intermediate_hops) + 2
    
    @property
    def full_path(self) -> List[str]:
        """Get complete path including source, hops, and destination."""
        return [self.source_location] + self.intermediate_hops + [self.destination_location]
    
    @property
    def is_direct(self) -> bool:
        """Check if this is a direct connection without hops."""
        return len(self.intermediate_hops) == 0
    
    def has_bottleneck(self) -> bool:
        """Check if path has a known bottleneck."""
        return self.bottleneck_location is not None
    
    def get_path_segments(self) -> List[Tuple[str, str]]:
        """Get path broken down into individual segments (connection pairs)."""
        full_path = self.full_path
        return [(full_path[i], full_path[i+1]) for i in range(len(full_path) - 1)]