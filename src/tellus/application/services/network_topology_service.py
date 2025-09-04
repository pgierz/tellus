"""
Network topology application service for optimal transfer routing.

This service orchestrates network topology management, route optimization,
and integration with file transfer operations for distributed climate modeling systems.
"""

import asyncio
import logging
import time
from typing import Dict, List, Optional, Set, Tuple, Any, Protocol
from dataclasses import asdict

from ...domain.entities.location import LocationEntity
from ...domain.entities.network_topology import NetworkTopology
from ...domain.entities.network_connection import NetworkConnection, ConnectionType
from ...domain.entities.network_metrics import NetworkPath, BandwidthMetrics, LatencyMetrics, NetworkHealth
from ...domain.repositories.location_repository import ILocationRepository
from ...infrastructure.adapters.network_benchmarking_adapter import IBenchmarkingAdapter
from ..dtos import (
    CreateNetworkTopologyDto, NetworkConnectionDto, NetworkPathDto,
    BandwidthMeasurementDto, LatencyMeasurementDto, TopologyBenchmarkDto,
    OptimalRouteRequestDto, OptimalRouteResponseDto
)
from ..exceptions import (
    EntityNotFoundError, ValidationError, ExternalServiceError,
    OperationNotAllowedError
)


logger = logging.getLogger(__name__)


# DTOs for network topology operations
class CreateNetworkTopologyDto:
    """DTO for creating network topology."""
    def __init__(
        self,
        name: str,
        auto_discovery_enabled: bool = True,
        benchmark_cache_ttl_hours: float = 24.0
    ):
        self.name = name
        self.auto_discovery_enabled = auto_discovery_enabled
        self.benchmark_cache_ttl_hours = benchmark_cache_ttl_hours


class NetworkConnectionDto:
    """DTO for network connection data."""
    def __init__(
        self,
        source_location: str,
        destination_location: str,
        connection_type: str,
        bandwidth_mbps: Optional[float] = None,
        latency_ms: Optional[float] = None,
        is_bidirectional: bool = True,
        connection_cost: float = 1.0
    ):
        self.source_location = source_location
        self.destination_location = destination_location
        self.connection_type = connection_type
        self.bandwidth_mbps = bandwidth_mbps
        self.latency_ms = latency_ms
        self.is_bidirectional = is_bidirectional
        self.connection_cost = connection_cost


class NetworkPathDto:
    """DTO for network path information."""
    def __init__(
        self,
        source_location: str,
        destination_location: str,
        intermediate_hops: List[str],
        total_cost: float,
        estimated_bandwidth_mbps: float,
        estimated_latency_ms: float,
        bottleneck_location: Optional[str] = None,
        path_type: str = "direct"
    ):
        self.source_location = source_location
        self.destination_location = destination_location
        self.intermediate_hops = intermediate_hops
        self.total_cost = total_cost
        self.estimated_bandwidth_mbps = estimated_bandwidth_mbps
        self.estimated_latency_ms = estimated_latency_ms
        self.bottleneck_location = bottleneck_location
        self.path_type = path_type


class OptimalRouteRequestDto:
    """DTO for optimal route requests."""
    def __init__(
        self,
        source_location: str,
        destination_location: str,
        optimize_for: str = "bandwidth",  # bandwidth, latency, cost, reliability
        avoid_bottlenecks: bool = True,
        max_hops: Optional[int] = None,
        required_min_bandwidth_mbps: Optional[float] = None,
        max_acceptable_latency_ms: Optional[float] = None
    ):
        self.source_location = source_location
        self.destination_location = destination_location
        self.optimize_for = optimize_for
        self.avoid_bottlenecks = avoid_bottlenecks
        self.max_hops = max_hops
        self.required_min_bandwidth_mbps = required_min_bandwidth_mbps
        self.max_acceptable_latency_ms = max_acceptable_latency_ms


class OptimalRouteResponseDto:
    """DTO for optimal route responses."""
    def __init__(
        self,
        request_id: str,
        primary_path: NetworkPathDto,
        alternative_paths: List[NetworkPathDto],
        path_analysis: Dict[str, Any],
        recommendation: str
    ):
        self.request_id = request_id
        self.primary_path = primary_path
        self.alternative_paths = alternative_paths
        self.path_analysis = path_analysis
        self.recommendation = recommendation


class TopologyBenchmarkDto:
    """DTO for topology benchmarking requests."""
    def __init__(
        self,
        topology_name: str,
        location_pairs: Optional[List[Tuple[str, str]]] = None,
        include_bandwidth: bool = True,
        include_latency: bool = True,
        force_refresh: bool = False,
        max_concurrent_tests: int = 3
    ):
        self.topology_name = topology_name
        self.location_pairs = location_pairs or []
        self.include_bandwidth = include_bandwidth
        self.include_latency = include_latency
        self.force_refresh = force_refresh
        self.max_concurrent_tests = max_concurrent_tests


# Repository interface for network topology persistence
class INetworkTopologyRepository(Protocol):
    """Repository interface for network topology persistence."""
    
    def save_topology(self, topology: NetworkTopology) -> None:
        """Save network topology."""
        ...
    
    def get_topology(self, name: str) -> Optional[NetworkTopology]:
        """Get topology by name."""
        ...
    
    def list_topologies(self) -> List[NetworkTopology]:
        """List all topologies."""
        ...
    
    def delete_topology(self, name: str) -> bool:
        """Delete topology by name."""
        ...


class NetworkTopologyApplicationService:
    """
    Application service for network topology management and route optimization.
    
    Orchestrates network topology discovery, benchmarking, and optimal route
    calculation for distributed Earth System Model data management.
    
    This service acts as the facade between the CLI/UI layer and the domain
    entities, coordinating complex operations across multiple components.
    """
    
    def __init__(
        self,
        location_repo: ILocationRepository,
        topology_repo: INetworkTopologyRepository,
        benchmarking_adapter: IBenchmarkingAdapter
    ):
        """
        Initialize the network topology application service.
        
        Args:
            location_repo: Repository for location entities
            topology_repo: Repository for network topology persistence
            benchmarking_adapter: Infrastructure adapter for network benchmarking
        """
        self._location_repo = location_repo
        self._topology_repo = topology_repo
        self._benchmarking_adapter = benchmarking_adapter
        self._logger = logging.getLogger(__name__)
        
        # Service configuration
        self.default_topology_name = "default"
        self.max_concurrent_benchmarks = 5
        self.route_cache_ttl_hours = 6.0
        
        # Internal caches
        self._route_cache: Dict[str, Tuple[NetworkPath, float]] = {}
        self._active_benchmarks: Set[str] = set()
    
    async def create_topology(self, dto: CreateNetworkTopologyDto) -> NetworkTopology:
        """
        Create a new network topology.
        
        Args:
            dto: Topology creation parameters
            
        Returns:
            Created NetworkTopology entity
            
        Raises:
            ValidationError: If topology name is invalid or already exists
        """
        self._logger.info(f"Creating network topology: {dto.name}")
        
        # Validate topology name
        if not dto.name or not dto.name.strip():
            raise ValidationError("Topology name is required")
        
        # Check if topology already exists
        existing_topology = self._topology_repo.get_topology(dto.name)
        if existing_topology:
            raise ValidationError(f"Topology '{dto.name}' already exists")
        
        # Create topology entity
        topology = NetworkTopology(
            name=dto.name,
            auto_discovery_enabled=dto.auto_discovery_enabled,
            benchmark_cache_ttl_hours=dto.benchmark_cache_ttl_hours
        )
        
        # Save topology
        self._topology_repo.save_topology(topology)
        
        self._logger.info(f"Created topology: {dto.name}")
        return topology
    
    async def get_or_create_default_topology(self) -> NetworkTopology:
        """
        Get the default topology, creating it if it doesn't exist.
        
        Returns:
            Default NetworkTopology entity
        """
        topology = self._topology_repo.get_topology(self.default_topology_name)
        
        if topology is None:
            self._logger.info("Creating default network topology")
            dto = CreateNetworkTopologyDto(
                name=self.default_topology_name,
                auto_discovery_enabled=True
            )
            topology = await self.create_topology(dto)
        
        return topology
    
    async def discover_topology(
        self,
        topology_name: Optional[str] = None,
        location_names: Optional[List[str]] = None
    ) -> NetworkTopology:
        """
        Discover network topology by testing all location pairs.
        
        Args:
            topology_name: Name of topology to update (uses default if None)
            location_names: Specific locations to include (uses all if None)
            
        Returns:
            Updated NetworkTopology entity
            
        Raises:
            EntityNotFoundError: If specified locations don't exist
            ExternalServiceError: If network discovery fails
        """
        if topology_name is None:
            topology_name = self.default_topology_name
        
        self._logger.info(f"Discovering network topology: {topology_name}")
        
        # Get or create topology
        topology = self._topology_repo.get_topology(topology_name)
        if topology is None:
            dto = CreateNetworkTopologyDto(topology_name)
            topology = await self.create_topology(dto)
        
        # Get locations to test
        if location_names:
            locations = []
            for name in location_names:
                location = self._location_repo.get_by_name(name)
                if location is None:
                    raise EntityNotFoundError("Location", name)
                locations.append(location)
        else:
            locations = self._location_repo.list_all()
        
        if len(locations) < 2:
            raise ValidationError("At least 2 locations required for topology discovery")
        
        # Generate all location pairs
        location_pairs = []
        for i in range(len(locations)):
            for j in range(i + 1, len(locations)):
                location_pairs.append((locations[i], locations[j]))
        
        # Benchmark all pairs concurrently (with limits)
        semaphore = asyncio.Semaphore(self.max_concurrent_benchmarks)
        
        async def benchmark_pair(source_loc: LocationEntity, dest_loc: LocationEntity) -> Optional[NetworkConnection]:
            async with semaphore:
                try:
                    return await self._benchmarking_adapter.benchmark_connection_pair(
                        source_loc, dest_loc
                    )
                except Exception as e:
                    self._logger.warning(f"Failed to benchmark {source_loc.name} <-> {dest_loc.name}: {e}")
                    return None
        
        # Execute benchmarking tasks
        benchmark_tasks = [
            benchmark_pair(source, dest) for source, dest in location_pairs
        ]
        
        results = await asyncio.gather(*benchmark_tasks, return_exceptions=True)
        
        # Add successful connections to topology
        connections_added = 0
        for result in results:
            if isinstance(result, NetworkConnection):
                try:
                    topology.add_connection(result)
                    connections_added += 1
                except ValueError as e:
                    # Connection might already exist
                    self._logger.debug(f"Skipping duplicate connection: {e}")
        
        # Save updated topology
        self._topology_repo.save_topology(topology)
        
        self._logger.info(f"Discovery complete: added {connections_added} connections to {topology_name}")
        return topology
    
    async def benchmark_topology(self, dto: TopologyBenchmarkDto) -> Dict[str, Any]:
        """
        Benchmark specific connections in a topology.
        
        Args:
            dto: Benchmarking parameters and options
            
        Returns:
            Benchmarking results summary
            
        Raises:
            EntityNotFoundError: If topology or locations don't exist
        """
        self._logger.info(f"Benchmarking topology: {dto.topology_name}")
        
        # Get topology
        topology = self._topology_repo.get_topology(dto.topology_name)
        if topology is None:
            raise EntityNotFoundError("NetworkTopology", dto.topology_name)
        
        # Determine location pairs to benchmark
        if dto.location_pairs:
            # Use specific pairs
            location_pairs = []
            for source_name, dest_name in dto.location_pairs:
                source_loc = self._location_repo.get_by_name(source_name)
                dest_loc = self._location_repo.get_by_name(dest_name)
                
                if source_loc is None:
                    raise EntityNotFoundError("Location", source_name)
                if dest_loc is None:
                    raise EntityNotFoundError("Location", dest_name)
                
                location_pairs.append((source_loc, dest_loc))
        else:
            # Use all connections in topology
            location_pairs = []
            for connection in topology.connections:
                source_loc = self._location_repo.get_by_name(connection.source_location)
                dest_loc = self._location_repo.get_by_name(connection.destination_location)
                
                if source_loc and dest_loc:
                    location_pairs.append((source_loc, dest_loc))
        
        if not location_pairs:
            return {"benchmarked_connections": 0, "message": "No location pairs to benchmark"}
        
        # Track active benchmarks to prevent duplicates
        active_pairs = set()
        for source, dest in location_pairs:
            pair_key = f"{source.name}::{dest.name}"
            if pair_key in self._active_benchmarks:
                self._logger.warning(f"Benchmark already in progress: {pair_key}")
                continue
            self._active_benchmarks.add(pair_key)
            active_pairs.add(pair_key)
        
        try:
            # Perform benchmarking with concurrency control
            semaphore = asyncio.Semaphore(dto.max_concurrent_tests)
            
            async def benchmark_connection_pair(source_loc: LocationEntity, dest_loc: LocationEntity):
                async with semaphore:
                    try:
                        # Check if we should use cached results
                        existing_connection = topology.get_connection(source_loc.name, dest_loc.name)
                        
                        if (not dto.force_refresh and existing_connection and 
                            existing_connection.bandwidth_metrics and 
                            not existing_connection.bandwidth_metrics.is_stale(topology.benchmark_cache_ttl_hours)):
                            
                            self._logger.debug(f"Using cached metrics for {source_loc.name} <-> {dest_loc.name}")
                            return existing_connection
                        
                        # Perform fresh benchmarking
                        new_connection = await self._benchmarking_adapter.benchmark_connection_pair(
                            source_loc, dest_loc, dto.include_latency, dto.include_bandwidth
                        )
                        
                        if new_connection:
                            # Update or add connection to topology
                            if existing_connection:
                                # Update existing connection metrics
                                if new_connection.bandwidth_metrics:
                                    existing_connection.update_bandwidth_metrics(new_connection.bandwidth_metrics)
                                if new_connection.latency_metrics:
                                    existing_connection.update_latency_metrics(new_connection.latency_metrics)
                                return existing_connection
                            else:
                                # Add new connection
                                topology.add_connection(new_connection)
                                return new_connection
                        
                        return None
                        
                    except Exception as e:
                        self._logger.error(f"Benchmark failed for {source_loc.name} <-> {dest_loc.name}: {e}")
                        return None
            
            # Execute benchmark tasks
            benchmark_tasks = [
                benchmark_connection_pair(source, dest) for source, dest in location_pairs
            ]
            
            results = await asyncio.gather(*benchmark_tasks, return_exceptions=True)
            
            # Process results
            successful_benchmarks = 0
            failed_benchmarks = 0
            updated_connections = []
            
            for result in results:
                if isinstance(result, NetworkConnection):
                    successful_benchmarks += 1
                    updated_connections.append({
                        'source': result.source_location,
                        'destination': result.destination_location,
                        'bandwidth_mbps': result.effective_bandwidth_mbps,
                        'health': result.current_health.name
                    })
                elif isinstance(result, Exception):
                    failed_benchmarks += 1
                    self._logger.error(f"Benchmark exception: {result}")
                else:
                    failed_benchmarks += 1
            
            # Save updated topology
            self._topology_repo.save_topology(topology)
            
            # Clear route cache since topology changed
            self._route_cache.clear()
            
            benchmark_summary = {
                'topology_name': dto.topology_name,
                'total_pairs_tested': len(location_pairs),
                'successful_benchmarks': successful_benchmarks,
                'failed_benchmarks': failed_benchmarks,
                'updated_connections': updated_connections,
                'topology_stats': {
                    'total_connections': topology.connection_count,
                    'average_bandwidth_mbps': topology.average_bandwidth_mbps,
                    'bottleneck_connections': len(topology.get_bottleneck_connections())
                }
            }
            
            self._logger.info(f"Benchmarking complete: {successful_benchmarks}/{len(location_pairs)} successful")
            return benchmark_summary
            
        finally:
            # Clean up active benchmark tracking
            for pair_key in active_pairs:
                self._active_benchmarks.discard(pair_key)
    
    async def find_optimal_route(self, dto: OptimalRouteRequestDto) -> OptimalRouteResponseDto:
        """
        Find optimal route between locations based on specified criteria.
        
        Args:
            dto: Route optimization parameters
            
        Returns:
            Optimal route response with primary and alternative paths
            
        Raises:
            EntityNotFoundError: If source/destination locations don't exist
            ValidationError: If route constraints cannot be satisfied
        """
        import uuid
        request_id = f"route_{int(time.time())}_{uuid.uuid4().hex[:8]}"
        
        self._logger.info(f"Finding optimal route: {dto.source_location} -> {dto.destination_location} "
                         f"(optimize for: {dto.optimize_for})")
        
        # Check cache first
        cache_key = f"{dto.source_location}::{dto.destination_location}::{dto.optimize_for}"
        if cache_key in self._route_cache:
            cached_path, timestamp = self._route_cache[cache_key]
            if (time.time() - timestamp) < (self.route_cache_ttl_hours * 3600):
                self._logger.debug(f"Using cached route for {cache_key}")
                return OptimalRouteResponseDto(
                    request_id=request_id,
                    primary_path=self._network_path_to_dto(cached_path),
                    alternative_paths=[],
                    path_analysis={'cache_hit': True},
                    recommendation="Using cached optimal route"
                )
        
        # Validate locations exist
        source_location = self._location_repo.get_by_name(dto.source_location)
        if source_location is None:
            raise EntityNotFoundError("Location", dto.source_location)
        
        dest_location = self._location_repo.get_by_name(dto.destination_location)
        if dest_location is None:
            raise EntityNotFoundError("Location", dto.destination_location)
        
        # Get topology
        topology = await self.get_or_create_default_topology()
        
        # Ensure locations are in topology
        if dto.source_location not in topology.location_names:
            self._logger.warning(f"Source location not in topology: {dto.source_location}")
            # Could trigger auto-discovery here
        
        if dto.destination_location not in topology.location_names:
            self._logger.warning(f"Destination location not in topology: {dto.destination_location}")
            # Could trigger auto-discovery here
        
        # Find optimal path using domain logic
        primary_path = topology.find_optimal_path(
            dto.source_location, dto.destination_location, dto.optimize_for
        )
        
        if primary_path is None:
            raise ValidationError(f"No route found between {dto.source_location} and {dto.destination_location}")
        
        # Find alternative paths
        alternative_paths = []
        
        # Try different optimization criteria for alternatives
        alt_criteria = ['bandwidth', 'latency', 'cost', 'reliability']
        for criteria in alt_criteria:
            if criteria != dto.optimize_for:
                alt_path = topology.find_optimal_path(
                    dto.source_location, dto.destination_location, criteria
                )
                if alt_path and alt_path.full_path != primary_path.full_path:
                    alternative_paths.append(alt_path)
        
        # Apply constraints validation
        constraint_violations = []
        
        if dto.max_hops and primary_path.hop_count > dto.max_hops:
            constraint_violations.append(f"Path exceeds max hops: {primary_path.hop_count} > {dto.max_hops}")
        
        if (dto.required_min_bandwidth_mbps and 
            primary_path.estimated_bandwidth_mbps < dto.required_min_bandwidth_mbps):
            constraint_violations.append(
                f"Bandwidth below requirement: {primary_path.estimated_bandwidth_mbps:.1f} < {dto.required_min_bandwidth_mbps}"
            )
        
        if (dto.max_acceptable_latency_ms and 
            primary_path.estimated_latency_ms > dto.max_acceptable_latency_ms):
            constraint_violations.append(
                f"Latency exceeds limit: {primary_path.estimated_latency_ms:.1f} > {dto.max_acceptable_latency_ms}"
            )
        
        # Generate path analysis
        path_analysis = {
            'optimization_criteria': dto.optimize_for,
            'constraint_violations': constraint_violations,
            'path_quality': self._assess_path_quality(primary_path),
            'bottleneck_analysis': self._analyze_bottlenecks(topology, primary_path),
            'alternative_count': len(alternative_paths),
            'topology_health': self._assess_topology_health(topology)
        }
        
        # Generate recommendation
        recommendation = self._generate_route_recommendation(
            primary_path, constraint_violations, path_analysis
        )
        
        # Cache result
        self._route_cache[cache_key] = (primary_path, time.time())
        
        response = OptimalRouteResponseDto(
            request_id=request_id,
            primary_path=self._network_path_to_dto(primary_path),
            alternative_paths=[self._network_path_to_dto(path) for path in alternative_paths],
            path_analysis=path_analysis,
            recommendation=recommendation
        )
        
        self._logger.info(f"Optimal route found: {len(primary_path.full_path)} hops, "
                         f"{primary_path.estimated_bandwidth_mbps:.1f} Mbps")
        
        return response
    
    async def get_topology_status(self, topology_name: Optional[str] = None) -> Dict[str, Any]:
        """
        Get comprehensive status of network topology.
        
        Args:
            topology_name: Name of topology (uses default if None)
            
        Returns:
            Topology status information
        """
        if topology_name is None:
            topology_name = self.default_topology_name
        
        topology = self._topology_repo.get_topology(topology_name)
        if topology is None:
            return {'exists': False, 'name': topology_name}
        
        # Calculate topology statistics
        connections = topology.connections
        total_connections = len(connections)
        
        # Health distribution
        health_counts = {}
        for connection in connections:
            health = connection.current_health.name
            health_counts[health] = health_counts.get(health, 0) + 1
        
        # Performance statistics
        bandwidths = [conn.effective_bandwidth_mbps for conn in connections if conn.bandwidth_metrics]
        latencies = [conn.latency_metrics.avg_latency_ms for conn in connections if conn.latency_metrics]
        
        bandwidth_stats = {}
        if bandwidths:
            bandwidth_stats = {
                'min': min(bandwidths),
                'max': max(bandwidths),
                'avg': sum(bandwidths) / len(bandwidths),
                'count': len(bandwidths)
            }
        
        latency_stats = {}
        if latencies:
            latency_stats = {
                'min': min(latencies),
                'max': max(latencies),
                'avg': sum(latencies) / len(latencies),
                'count': len(latencies)
            }
        
        # Stale connections
        stale_connections = topology.get_stale_connections()
        bottleneck_connections = topology.get_bottleneck_connections()
        
        return {
            'exists': True,
            'name': topology.name,
            'last_updated': topology.last_updated,
            'auto_discovery_enabled': topology.auto_discovery_enabled,
            'total_locations': len(topology.location_names),
            'total_connections': total_connections,
            'health_distribution': health_counts,
            'bandwidth_stats': bandwidth_stats,
            'latency_stats': latency_stats,
            'stale_connections': len(stale_connections),
            'bottleneck_connections': len(bottleneck_connections),
            'needs_refresh': topology.needs_refresh(),
            'cache_stats': {
                'route_cache_entries': len(self._route_cache),
                'active_benchmarks': len(self._active_benchmarks)
            }
        }
    
    # Private helper methods
    
    def _network_path_to_dto(self, path: NetworkPath) -> NetworkPathDto:
        """Convert NetworkPath entity to DTO."""
        return NetworkPathDto(
            source_location=path.source_location,
            destination_location=path.destination_location,
            intermediate_hops=path.intermediate_hops,
            total_cost=path.total_cost,
            estimated_bandwidth_mbps=path.estimated_bandwidth_mbps,
            estimated_latency_ms=path.estimated_latency_ms,
            bottleneck_location=path.bottleneck_location,
            path_type=path.path_type
        )
    
    def _assess_path_quality(self, path: NetworkPath) -> str:
        """Assess overall quality of a network path."""
        if path.estimated_bandwidth_mbps > 100 and path.estimated_latency_ms < 50:
            return "excellent"
        elif path.estimated_bandwidth_mbps > 50 and path.estimated_latency_ms < 100:
            return "good"
        elif path.estimated_bandwidth_mbps > 10 and path.estimated_latency_ms < 200:
            return "fair"
        else:
            return "poor"
    
    def _analyze_bottlenecks(self, topology: NetworkTopology, path: NetworkPath) -> Dict[str, Any]:
        """Analyze bottlenecks in the path."""
        bottleneck_analysis = {
            'has_bottleneck': path.has_bottleneck(),
            'bottleneck_location': path.bottleneck_location,
            'limiting_factor': None,
            'improvement_suggestions': []
        }
        
        if path.estimated_bandwidth_mbps < 10:
            bottleneck_analysis['limiting_factor'] = 'bandwidth'
            bottleneck_analysis['improvement_suggestions'].append(
                'Consider alternative routes with higher bandwidth'
            )
        
        if path.estimated_latency_ms > 200:
            bottleneck_analysis['limiting_factor'] = 'latency'
            bottleneck_analysis['improvement_suggestions'].append(
                'Route has high latency - check for geographic distance or network congestion'
            )
        
        if path.hop_count > 3:
            bottleneck_analysis['improvement_suggestions'].append(
                'Path has many hops - direct connection may be more efficient'
            )
        
        return bottleneck_analysis
    
    def _assess_topology_health(self, topology: NetworkTopology) -> Dict[str, Any]:
        """Assess overall health of the topology."""
        connections = topology.connections
        if not connections:
            return {'status': 'empty', 'health_score': 0.0}
        
        # Calculate health score based on connection health
        health_scores = {
            NetworkHealth.OPTIMAL: 1.0,
            NetworkHealth.CONGESTED: 0.8,
            NetworkHealth.DEGRADED: 0.6,
            NetworkHealth.UNSTABLE: 0.3,
            NetworkHealth.UNAVAILABLE: 0.0
        }
        
        total_score = 0.0
        for connection in connections:
            total_score += health_scores.get(connection.current_health, 0.5)
        
        avg_health_score = total_score / len(connections)
        
        if avg_health_score > 0.8:
            status = 'healthy'
        elif avg_health_score > 0.6:
            status = 'fair'
        elif avg_health_score > 0.3:
            status = 'degraded'
        else:
            status = 'critical'
        
        return {
            'status': status,
            'health_score': avg_health_score,
            'stale_connections': len(topology.get_stale_connections()),
            'bottleneck_connections': len(topology.get_bottleneck_connections())
        }
    
    def _generate_route_recommendation(
        self,
        path: NetworkPath,
        constraint_violations: List[str],
        path_analysis: Dict[str, Any]
    ) -> str:
        """Generate human-readable recommendation for the route."""
        if constraint_violations:
            return f"Route has constraint violations: {'; '.join(constraint_violations)}"
        
        quality = path_analysis.get('path_quality', 'unknown')
        
        if quality == 'excellent':
            return "Excellent route with high performance - recommended for large transfers"
        elif quality == 'good':
            return "Good route suitable for most transfer operations"
        elif quality == 'fair':
            return "Fair route - acceptable but consider alternatives for critical transfers"
        else:
            return "Poor route performance - strongly recommend finding alternatives"