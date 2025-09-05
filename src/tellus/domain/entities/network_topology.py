"""
Core network topology domain entities for optimal data transfer routing in distributed systems.
"""

import time
from dataclasses import dataclass, field
from typing import List, Optional, Set

from .network_connection import NetworkConnection
from .network_metrics import NetworkPath, NetworkHealth


@dataclass
class NetworkTopology:
    """
    Domain entity representing the complete network topology between locations.
    """
    name: str
    connections: List[NetworkConnection] = field(default_factory=list)
    last_updated: float = field(default_factory=time.time)
    auto_discovery_enabled: bool = True
    benchmark_cache_ttl_hours: float = 24.0
    
    def __post_init__(self):
        """Validate network topology."""
        if not self.name:
            raise ValueError("Topology name is required")
        if self.benchmark_cache_ttl_hours <= 0:
            raise ValueError("Cache TTL must be positive")
    
    @property
    def location_names(self) -> Set[str]:
        """Get all unique location names in the topology."""
        locations = set()
        for connection in self.connections:
            locations.add(connection.source_location)
            locations.add(connection.destination_location)
        return locations
    
    @property
    def connection_count(self) -> int:
        """Get total number of connections."""
        return len(self.connections)
    
    @property
    def average_bandwidth_mbps(self) -> float:
        """Calculate average bandwidth across all connections."""
        bandwidths = [conn.effective_bandwidth_mbps for conn in self.connections 
                     if conn.bandwidth_metrics]
        return sum(bandwidths) / len(bandwidths) if bandwidths else 0.0
    
    def add_connection(self, connection: NetworkConnection) -> None:
        """
        Add a network connection to the topology.
        
        Raises ValueError if connection already exists.
        """
        if not isinstance(connection, NetworkConnection):
            raise ValueError("Must provide NetworkConnection instance")
        
        # Check for duplicate connections
        existing_connection = self.get_connection(
            connection.source_location, connection.destination_location
        )
        if existing_connection:
            raise ValueError(f"Connection already exists: {connection.connection_id}")
        
        self.connections.append(connection)
        self.last_updated = time.time()
    
    def remove_connection(self, source: str, destination: str) -> bool:
        """
        Remove a connection from the topology.
        
        Returns True if connection was removed, False if not found.
        """
        for i, connection in enumerate(self.connections):
            if connection.can_connect_locations(source, destination):
                del self.connections[i]
                self.last_updated = time.time()
                return True
        return False
    
    def get_connection(self, source: str, destination: str) -> Optional[NetworkConnection]:
        """Get connection between two locations."""
        for connection in self.connections:
            if connection.can_connect_locations(source, destination):
                return connection
        return None
    
    def get_connections_from_location(self, location: str) -> List[NetworkConnection]:
        """Get all connections originating from or connected to a location."""
        return [conn for conn in self.connections 
                if conn.source_location == location or 
                (conn.is_bidirectional and conn.destination_location == location)]
    
    def find_direct_path(self, source: str, destination: str) -> Optional[NetworkPath]:
        """Find direct path between two locations if it exists."""
        connection = self.get_connection(source, destination)
        if not connection:
            return None
        
        return NetworkPath(
            source_location=source,
            destination_location=destination,
            intermediate_hops=[],
            total_cost=connection.connection_cost,
            estimated_bandwidth_mbps=connection.effective_bandwidth_mbps,
            estimated_latency_ms=(connection.latency_metrics.avg_latency_ms 
                                if connection.latency_metrics else 0.0),
            bottleneck_location=(source if connection.is_bottleneck_connection() else None),
            path_type="direct"
        )
    
    def find_shortest_path(self, source: str, destination: str, 
                          avoid_bottlenecks: bool = True) -> Optional[NetworkPath]:
        """
        Find shortest path between locations using Dijkstra's algorithm.
        
        Considers connection costs and optionally avoids bottleneck connections.
        """
        if source == destination:
            return None
        
        if source not in self.location_names or destination not in self.location_names:
            return None
        
        # Dijkstra's algorithm implementation
        distances = {location: float('inf') for location in self.location_names}
        distances[source] = 0.0
        previous = {}
        unvisited = set(self.location_names)
        
        while unvisited:
            # Find unvisited node with minimum distance
            current = min(unvisited, key=lambda x: distances[x])
            
            if distances[current] == float('inf'):
                break  # No path exists
            
            if current == destination:
                break  # Reached destination
            
            unvisited.remove(current)
            
            # Check all connections from current location
            for connection in self.get_connections_from_location(current):
                neighbor = (connection.destination_location if connection.source_location == current 
                           else connection.source_location)
                
                if neighbor not in unvisited:
                    continue
                
                # Skip bottleneck connections if requested
                if avoid_bottlenecks and connection.is_bottleneck_connection():
                    continue
                
                # Calculate distance through this connection
                edge_cost = connection.connection_cost
                alt_distance = distances[current] + edge_cost
                
                if alt_distance < distances[neighbor]:
                    distances[neighbor] = alt_distance
                    previous[neighbor] = current
        
        # Reconstruct path
        if destination not in previous and source != destination:
            return None  # No path found
        
        path = []
        current = destination
        while current in previous:
            path.append(current)
            current = previous[current]
        path.append(source)
        path.reverse()
        
        # Calculate path metrics
        intermediate_hops = path[1:-1]
        total_cost = distances[destination]
        
        # Calculate bandwidth (limited by bottleneck) and latency (sum of segments)
        min_bandwidth = float('inf')
        total_latency = 0.0
        bottleneck_location = None
        
        for i in range(len(path) - 1):
            connection = self.get_connection(path[i], path[i + 1])
            if connection:
                bandwidth = connection.effective_bandwidth_mbps
                if bandwidth < min_bandwidth:
                    min_bandwidth = bandwidth
                    if connection.is_bottleneck_connection():
                        bottleneck_location = path[i]
                
                if connection.latency_metrics:
                    total_latency += connection.latency_metrics.avg_latency_ms
        
        return NetworkPath(
            source_location=source,
            destination_location=destination,
            intermediate_hops=intermediate_hops,
            total_cost=total_cost,
            estimated_bandwidth_mbps=min_bandwidth if min_bandwidth != float('inf') else 0.0,
            estimated_latency_ms=total_latency,
            bottleneck_location=bottleneck_location,
            path_type="multi_hop" if intermediate_hops else "direct"
        )
    
    def find_optimal_path(self, source: str, destination: str, 
                         optimize_for: str = "bandwidth") -> Optional[NetworkPath]:
        """
        Find optimal path optimizing for specified criteria.
        
        Args:
            optimize_for: "bandwidth", "latency", "cost", or "reliability"
        """
        if optimize_for == "bandwidth":
            return self._find_max_bandwidth_path(source, destination)
        elif optimize_for == "latency":
            return self._find_min_latency_path(source, destination)
        elif optimize_for == "cost":
            return self.find_shortest_path(source, destination)
        elif optimize_for == "reliability":
            return self.find_shortest_path(source, destination, avoid_bottlenecks=True)
        else:
            raise ValueError(f"Unknown optimization criteria: {optimize_for}")
    
    def _find_max_bandwidth_path(self, source: str, destination: str) -> Optional[NetworkPath]:
        """Find path with maximum bandwidth (widest path algorithm)."""
        if source == destination:
            return None
        
        bandwidths = {location: 0.0 for location in self.location_names}
        bandwidths[source] = float('inf')
        previous = {}
        unvisited = set(self.location_names)
        
        while unvisited:
            # Find unvisited node with maximum bandwidth
            current = max(unvisited, key=lambda x: bandwidths[x])
            
            if bandwidths[current] == 0.0:
                break  # No path exists
            
            if current == destination:
                break  # Reached destination
            
            unvisited.remove(current)
            
            # Check all connections from current location
            for connection in self.get_connections_from_location(current):
                neighbor = (connection.destination_location if connection.source_location == current 
                           else connection.source_location)
                
                if neighbor not in unvisited:
                    continue
                
                # Path bandwidth limited by minimum connection bandwidth
                path_bandwidth = min(bandwidths[current], connection.effective_bandwidth_mbps)
                
                if path_bandwidth > bandwidths[neighbor]:
                    bandwidths[neighbor] = path_bandwidth
                    previous[neighbor] = current
        
        # Reconstruct path (similar to shortest path)
        if destination not in previous and source != destination:
            return None
        
        path = []
        current = destination
        while current in previous:
            path.append(current)
            current = previous[current]
        path.append(source)
        path.reverse()
        
        # Build NetworkPath object
        return NetworkPath(
            source_location=source,
            destination_location=destination,
            intermediate_hops=path[1:-1],
            total_cost=len(path) - 1,  # Use hop count as cost
            estimated_bandwidth_mbps=bandwidths[destination],
            estimated_latency_ms=0.0,  # Would need to calculate
            path_type="optimized"
        )
    
    def _find_min_latency_path(self, source: str, destination: str) -> Optional[NetworkPath]:
        """Find path with minimum latency."""
        # Similar to shortest path but using latency as cost
        latencies = {location: float('inf') for location in self.location_names}
        latencies[source] = 0.0
        previous = {}
        unvisited = set(self.location_names)
        
        while unvisited:
            current = min(unvisited, key=lambda x: latencies[x])
            
            if latencies[current] == float('inf'):
                break
            
            if current == destination:
                break
            
            unvisited.remove(current)
            
            for connection in self.get_connections_from_location(current):
                neighbor = (connection.destination_location if connection.source_location == current 
                           else connection.source_location)
                
                if neighbor not in unvisited:
                    continue
                
                edge_latency = (connection.latency_metrics.avg_latency_ms 
                              if connection.latency_metrics else 50.0)  # Default estimate
                alt_latency = latencies[current] + edge_latency
                
                if alt_latency < latencies[neighbor]:
                    latencies[neighbor] = alt_latency
                    previous[neighbor] = current
        
        # Reconstruct path
        if destination not in previous and source != destination:
            return None
        
        path = []
        current = destination
        while current in previous:
            path.append(current)
            current = previous[current]
        path.append(source)
        path.reverse()
        
        return NetworkPath(
            source_location=source,
            destination_location=destination,
            intermediate_hops=path[1:-1],
            total_cost=latencies[destination],
            estimated_bandwidth_mbps=0.0,  # Would need to calculate
            estimated_latency_ms=latencies[destination],
            path_type="optimized"
        )
    
    def get_bottleneck_connections(self) -> List[NetworkConnection]:
        """Get all connections identified as bottlenecks."""
        return [conn for conn in self.connections if conn.is_bottleneck_connection()]
    
    def get_stale_connections(self, max_age_hours: float = None) -> List[NetworkConnection]:
        """Get connections with stale metrics."""
        if max_age_hours is None:
            max_age_hours = self.benchmark_cache_ttl_hours
        
        stale = []
        for connection in self.connections:
            if connection.bandwidth_metrics and connection.bandwidth_metrics.is_stale(max_age_hours):
                stale.append(connection)
            elif connection.latency_metrics and connection.latency_metrics.is_stale(max_age_hours):
                stale.append(connection)
        
        return stale
    
    def needs_refresh(self) -> bool:
        """Check if topology needs refresh based on stale metrics."""
        return len(self.get_stale_connections()) > 0
    
    def to_networkx_graph(self) -> 'networkx.Graph':
        """
        Convert topology to NetworkX graph for advanced algorithms.
        
        Requires networkx to be installed.
        """
        try:
            import networkx as nx
        except ImportError:
            raise ImportError("NetworkX is required for graph conversion")
        
        G = nx.Graph()
        
        # Add all locations as nodes
        for location in self.location_names:
            G.add_node(location)
        
        # Add connections as edges with attributes
        for connection in self.connections:
            edge_attrs = {
                'weight': connection.connection_cost,
                'bandwidth': connection.effective_bandwidth_mbps,
                'latency': (connection.latency_metrics.avg_latency_ms 
                          if connection.latency_metrics else 0),
                'connection_type': connection.connection_type.name,
                'is_bottleneck': connection.is_bottleneck_connection(),
                'health': connection.current_health.name
            }
            
            G.add_edge(connection.source_location, connection.destination_location, **edge_attrs)
        
        return G