"""
JSON-based network topology repository implementation.

Provides persistent storage for NetworkTopology entities using JSON files
compatible with the existing tellus storage patterns.
"""

import json
import logging
import time
from pathlib import Path
from typing import Dict, List, Optional, Any

from ...domain.entities.network_topology import NetworkTopology
from ...domain.entities.network_connection import NetworkConnection, ConnectionType
from ...domain.entities.network_metrics import BandwidthMetrics, LatencyMetrics, NetworkHealth
from ...domain.repositories.network_topology_repository import INetworkTopologyRepository
from ...domain.repositories.exceptions import RepositoryError


logger = logging.getLogger(__name__)


class JsonNetworkTopologyRepository(INetworkTopologyRepository):
    """
    JSON file-based implementation of network topology repository.
    
    Stores network topologies in JSON format following the tellus
    data storage conventions for consistency with existing repositories.
    """
    
    def __init__(self, storage_file: Path):
        """
        Initialize repository with JSON storage file.
        
        Args:
            storage_file: Path to JSON file for topology storage
        """
        self._storage_file = Path(storage_file)
        self._logger = logging.getLogger(__name__)
        
        # Ensure parent directory exists
        self._storage_file.parent.mkdir(parents=True, exist_ok=True)
        
        # Initialize empty storage if file doesn't exist
        if not self._storage_file.exists():
            self._save_data({})
            self._logger.info(f"Initialized empty topology storage: {self._storage_file}")
    
    def save_topology(self, topology: NetworkTopology) -> None:
        """Save network topology to JSON storage."""
        try:
            data = self._load_data()
            
            # Convert topology to serializable format
            topology_data = self._topology_to_dict(topology)
            
            # Store with timestamp
            topology_data['saved_at'] = time.time()
            
            data[topology.name] = topology_data
            self._save_data(data)
            
            self._logger.info(f"Saved topology: {topology.name}")
            
        except Exception as e:
            raise RepositoryError(f"Failed to save topology '{topology.name}': {e}")
    
    def get_topology(self, name: str) -> Optional[NetworkTopology]:
        """Retrieve topology by name from JSON storage."""
        try:
            data = self._load_data()
            
            if name not in data:
                return None
            
            topology_data = data[name]
            return self._dict_to_topology(topology_data)
            
        except Exception as e:
            raise RepositoryError(f"Failed to retrieve topology '{name}': {e}")
    
    def list_topologies(self) -> List[NetworkTopology]:
        """List all topologies from JSON storage."""
        try:
            data = self._load_data()
            
            topologies = []
            for topology_data in data.values():
                topology = self._dict_to_topology(topology_data)
                topologies.append(topology)
            
            return topologies
            
        except Exception as e:
            raise RepositoryError(f"Failed to list topologies: {e}")
    
    def delete_topology(self, name: str) -> bool:
        """Delete topology from JSON storage."""
        try:
            data = self._load_data()
            
            if name not in data:
                return False
            
            del data[name]
            self._save_data(data)
            
            self._logger.info(f"Deleted topology: {name}")
            return True
            
        except Exception as e:
            raise RepositoryError(f"Failed to delete topology '{name}': {e}")
    
    def topology_exists(self, name: str) -> bool:
        """Check if topology exists in JSON storage."""
        try:
            data = self._load_data()
            return name in data
            
        except Exception as e:
            raise RepositoryError(f"Failed to check topology existence '{name}': {e}")
    
    def _load_data(self) -> Dict[str, Any]:
        """Load data from JSON file."""
        try:
            with open(self._storage_file, 'r') as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError) as e:
            self._logger.warning(f"Failed to load topology data: {e}")
            return {}
    
    def _save_data(self, data: Dict[str, Any]) -> None:
        """Save data to JSON file."""
        try:
            # Atomic write using temporary file
            temp_file = self._storage_file.with_suffix('.tmp')
            
            with open(temp_file, 'w') as f:
                json.dump(data, f, indent=2, sort_keys=True)
            
            # Atomic replace
            temp_file.replace(self._storage_file)
            
        except Exception as e:
            # Clean up temp file on error
            if temp_file.exists():
                temp_file.unlink()
            raise RepositoryError(f"Failed to save topology data: {e}")
    
    def _topology_to_dict(self, topology: NetworkTopology) -> Dict[str, Any]:
        """Convert NetworkTopology entity to dictionary."""
        return {
            'name': topology.name,
            'connections': [self._connection_to_dict(conn) for conn in topology.connections],
            'last_updated': topology.last_updated,
            'auto_discovery_enabled': topology.auto_discovery_enabled,
            'benchmark_cache_ttl_hours': topology.benchmark_cache_ttl_hours,
            'version': '1.0'  # For future schema evolution
        }
    
    def _dict_to_topology(self, data: Dict[str, Any]) -> NetworkTopology:
        """Convert dictionary to NetworkTopology entity."""
        connections = []
        
        for conn_data in data.get('connections', []):
            connection = self._dict_to_connection(conn_data)
            connections.append(connection)
        
        topology = NetworkTopology(
            name=data['name'],
            connections=connections,
            last_updated=data.get('last_updated', time.time()),
            auto_discovery_enabled=data.get('auto_discovery_enabled', True),
            benchmark_cache_ttl_hours=data.get('benchmark_cache_ttl_hours', 24.0)
        )
        
        return topology
    
    def _connection_to_dict(self, connection: NetworkConnection) -> Dict[str, Any]:
        """Convert NetworkConnection to dictionary."""
        conn_data = {
            'source_location': connection.source_location,
            'destination_location': connection.destination_location,
            'connection_type': connection.connection_type.name,
            'is_bidirectional': connection.is_bidirectional,
            'connection_cost': connection.connection_cost,
            'metadata': connection.metadata.copy()
        }
        
        # Add bandwidth metrics if available
        if connection.bandwidth_metrics:
            conn_data['bandwidth_metrics'] = self._bandwidth_metrics_to_dict(
                connection.bandwidth_metrics
            )
        
        # Add latency metrics if available
        if connection.latency_metrics:
            conn_data['latency_metrics'] = self._latency_metrics_to_dict(
                connection.latency_metrics
            )
        
        return conn_data
    
    def _dict_to_connection(self, data: Dict[str, Any]) -> NetworkConnection:
        """Convert dictionary to NetworkConnection."""
        # Parse connection type
        connection_type = ConnectionType[data['connection_type']]
        
        # Parse metrics if available
        bandwidth_metrics = None
        if 'bandwidth_metrics' in data:
            bandwidth_metrics = self._dict_to_bandwidth_metrics(data['bandwidth_metrics'])
        
        latency_metrics = None
        if 'latency_metrics' in data:
            latency_metrics = self._dict_to_latency_metrics(data['latency_metrics'])
        
        connection = NetworkConnection(
            source_location=data['source_location'],
            destination_location=data['destination_location'],
            connection_type=connection_type,
            bandwidth_metrics=bandwidth_metrics,
            latency_metrics=latency_metrics,
            is_bidirectional=data.get('is_bidirectional', True),
            connection_cost=data.get('connection_cost', 1.0),
            metadata=data.get('metadata', {})
        )
        
        return connection
    
    def _bandwidth_metrics_to_dict(self, metrics: BandwidthMetrics) -> Dict[str, Any]:
        """Convert BandwidthMetrics to dictionary."""
        return {
            'measured_mbps': metrics.measured_mbps,
            'theoretical_max_mbps': metrics.theoretical_max_mbps,
            'measurement_timestamp': metrics.measurement_timestamp,
            'sample_count': metrics.sample_count,
            'variance_mbps': metrics.variance_mbps
        }
    
    def _dict_to_bandwidth_metrics(self, data: Dict[str, Any]) -> BandwidthMetrics:
        """Convert dictionary to BandwidthMetrics."""
        return BandwidthMetrics(
            measured_mbps=data['measured_mbps'],
            theoretical_max_mbps=data.get('theoretical_max_mbps'),
            measurement_timestamp=data.get('measurement_timestamp', time.time()),
            sample_count=data.get('sample_count', 1),
            variance_mbps=data.get('variance_mbps', 0.0)
        )
    
    def _latency_metrics_to_dict(self, metrics: LatencyMetrics) -> Dict[str, Any]:
        """Convert LatencyMetrics to dictionary."""
        return {
            'avg_latency_ms': metrics.avg_latency_ms,
            'min_latency_ms': metrics.min_latency_ms,
            'max_latency_ms': metrics.max_latency_ms,
            'jitter_ms': metrics.jitter_ms,
            'packet_loss_percentage': metrics.packet_loss_percentage,
            'measurement_timestamp': metrics.measurement_timestamp,
            'sample_count': metrics.sample_count
        }
    
    def _dict_to_latency_metrics(self, data: Dict[str, Any]) -> LatencyMetrics:
        """Convert dictionary to LatencyMetrics."""
        return LatencyMetrics(
            avg_latency_ms=data['avg_latency_ms'],
            min_latency_ms=data['min_latency_ms'],
            max_latency_ms=data['max_latency_ms'],
            jitter_ms=data.get('jitter_ms', 0.0),
            packet_loss_percentage=data.get('packet_loss_percentage', 0.0),
            measurement_timestamp=data.get('measurement_timestamp', time.time()),
            sample_count=data.get('sample_count', 1)
        )
    
    def get_storage_info(self) -> Dict[str, Any]:
        """Get information about the storage backend."""
        storage_info = {
            'storage_type': 'json_file',
            'storage_file': str(self._storage_file),
            'file_exists': self._storage_file.exists(),
            'file_size_bytes': 0,
            'topology_count': 0
        }
        
        try:
            if self._storage_file.exists():
                storage_info['file_size_bytes'] = self._storage_file.stat().st_size
                data = self._load_data()
                storage_info['topology_count'] = len(data)
                storage_info['topology_names'] = list(data.keys())
        except Exception as e:
            storage_info['error'] = str(e)
        
        return storage_info