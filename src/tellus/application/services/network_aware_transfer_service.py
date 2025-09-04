"""
Network-aware file transfer service that integrates topology optimization.

This service extends the existing file transfer capabilities with network
topology awareness, providing optimal routing for data transfers in
distributed climate modeling systems.
"""

import asyncio
import logging
import time
import warnings
from typing import Dict, List, Optional, Any
from dataclasses import asdict

from .file_transfer_service import FileTransferApplicationService
from .network_topology_service import (
    NetworkTopologyApplicationService, OptimalRouteRequestDto
)
from ..dtos import (
    FileTransferOperationDto, FileTransferResultDto,
    BatchFileTransferOperationDto, BatchFileTransferResultDto,
    DirectoryTransferOperationDto
)
from ..exceptions import (
    ValidationError, ExternalServiceError, OperationNotAllowedError
)
from ...domain.entities.location import LocationEntity


logger = logging.getLogger(__name__)


class NetworkAwareFileTransferService:
    """
    File transfer service with network topology optimization.
    
    Enhances the base file transfer service with network topology awareness,
    allowing for optimal routing decisions and multi-hop transfers through
    intermediary locations when beneficial.
    """
    
    def __init__(
        self,
        base_transfer_service: FileTransferApplicationService,
        network_topology_service: NetworkTopologyApplicationService,
        enable_multi_hop_transfers: bool = True,
        auto_discover_topology: bool = True
    ):
        """
        Initialize network-aware transfer service.
        
        Args:
            base_transfer_service: Underlying file transfer service
            network_topology_service: Network topology management service
            enable_multi_hop_transfers: Allow transfers through intermediary locations
            auto_discover_topology: Automatically discover topology for unknown routes
        """
        self._base_service = base_transfer_service
        self._network_service = network_topology_service
        self._enable_multi_hop = enable_multi_hop_transfers
        self._auto_discover = auto_discover_topology
        self._logger = logging.getLogger(__name__)
        
        # Performance settings
        self.route_optimization_timeout = 30  # seconds
        self.topology_refresh_threshold = 0.7  # Refresh if >70% connections stale
        
    async def transfer_file_optimized(
        self, 
        dto: FileTransferOperationDto,
        optimize_for: str = "bandwidth",
        avoid_bottlenecks: bool = True,
        force_route_calculation: bool = False
    ) -> FileTransferResultDto:
        """
        Transfer file with network topology optimization.
        
        Args:
            dto: File transfer operation details
            optimize_for: Route optimization criteria ("bandwidth", "latency", "cost", "reliability")
            avoid_bottlenecks: Avoid connections identified as bottlenecks
            force_route_calculation: Force route calculation even for direct transfers
            
        Returns:
            Enhanced file transfer result with routing information
        """
        start_time = time.time()
        
        self._logger.info(f"Starting optimized file transfer: {dto.source_location} -> {dto.dest_location}")
        
        try:
            # Check if optimization is beneficial
            if not force_route_calculation and not self._should_optimize_route(dto):
                self._logger.debug("Route optimization not beneficial, using direct transfer")
                return await self._base_service.transfer_file(dto)
            
            # Find optimal route
            route_response = await self._find_optimal_route(
                dto.source_location, dto.dest_location, optimize_for, avoid_bottlenecks
            )
            
            if not route_response:
                self._logger.warning("No optimal route found, falling back to direct transfer")
                return await self._base_service.transfer_file(dto)
            
            primary_path = route_response.primary_path
            
            # Check if multi-hop transfer is needed and enabled
            if len(primary_path.intermediate_hops) > 0 and self._enable_multi_hop:
                self._logger.info(f"Using multi-hop transfer via: {' -> '.join(primary_path.intermediate_hops)}")
                return await self._execute_multi_hop_transfer(dto, primary_path)
            
            # Direct transfer with route metadata
            result = await self._base_service.transfer_file(dto)
            
            # Enhance result with routing information
            if result.success:
                result = self._enhance_transfer_result(result, route_response, primary_path)
            
            return result
            
        except Exception as e:
            self._logger.error(f"Optimized transfer failed: {e}")
            # Fallback to basic transfer
            warnings.warn(f"Network optimization failed, using basic transfer: {e}")
            return await self._base_service.transfer_file(dto)
    
    async def batch_transfer_optimized(
        self,
        dto: BatchFileTransferOperationDto,
        optimize_for: str = "bandwidth",
        group_by_route: bool = True
    ) -> BatchFileTransferResultDto:
        """
        Batch transfer with route optimization and grouping.
        
        Groups transfers by optimal routes to maximize efficiency and
        minimize network topology discovery overhead.
        
        Args:
            dto: Batch transfer operation details
            optimize_for: Route optimization criteria
            group_by_route: Group transfers by optimal route for efficiency
            
        Returns:
            Batch transfer result with routing optimization data
        """
        self._logger.info(f"Starting optimized batch transfer of {len(dto.transfers)} files")
        
        if not group_by_route:
            # Process transfers individually with optimization
            optimized_transfers = []
            for transfer_dto in dto.transfers:
                try:
                    result = await self.transfer_file_optimized(
                        transfer_dto, optimize_for=optimize_for
                    )
                    optimized_transfers.append(result)
                except Exception as e:
                    self._logger.error(f"Transfer failed: {e}")
                    # Continue with other transfers
                    
            # Convert individual results to batch result
            return self._convert_to_batch_result(optimized_transfers, dto)
        
        # Group transfers by source-destination pairs for route optimization
        route_groups = self._group_transfers_by_route(dto.transfers)
        
        # Optimize routes for each group
        optimized_groups = []
        for (source_loc, dest_loc), group_transfers in route_groups.items():
            try:
                route_response = await self._find_optimal_route(
                    source_loc, dest_loc, optimize_for
                )
                optimized_groups.append((group_transfers, route_response))
            except Exception as e:
                self._logger.warning(f"Route optimization failed for {source_loc} -> {dest_loc}: {e}")
                # Use transfers without optimization
                optimized_groups.append((group_transfers, None))
        
        # Execute transfers by optimized groups
        all_results = []
        for group_transfers, route_response in optimized_groups:
            group_results = await self._execute_group_transfers(
                group_transfers, route_response, optimize_for
            )
            all_results.extend(group_results)
        
        return self._convert_to_batch_result(all_results, dto)
    
    async def get_transfer_recommendation(
        self,
        source_location: str,
        dest_location: str,
        file_size_mb: Optional[float] = None,
        optimize_for: str = "bandwidth"
    ) -> Dict[str, Any]:
        """
        Get transfer recommendation based on network topology.
        
        Args:
            source_location: Source location name
            dest_location: Destination location name
            file_size_mb: Estimated file size in MB for time estimation
            optimize_for: Optimization criteria
            
        Returns:
            Transfer recommendation with route analysis and estimates
        """
        self._logger.info(f"Generating transfer recommendation: {source_location} -> {dest_location}")
        
        try:
            # Get optimal route
            route_response = await self._find_optimal_route(
                source_location, dest_location, optimize_for
            )
            
            if not route_response:
                return {
                    'recommendation': 'direct_transfer',
                    'reason': 'No network topology data available',
                    'estimated_time_minutes': None,
                    'route_analysis': None
                }
            
            primary_path = route_response.primary_path
            
            # Calculate transfer time estimate
            estimated_time = None
            if file_size_mb and primary_path.estimated_bandwidth_mbps > 0:
                # Convert MB to Mb (megabits), add overhead
                file_size_mb_bits = file_size_mb * 8
                transfer_time_seconds = file_size_mb_bits / primary_path.estimated_bandwidth_mbps
                # Add 20% overhead for protocol overhead, setup time, etc.
                estimated_time = (transfer_time_seconds * 1.2) / 60  # Convert to minutes
            
            # Generate recommendation
            recommendation = self._generate_transfer_recommendation(
                route_response, estimated_time, file_size_mb
            )
            
            return {
                'recommendation': recommendation['type'],
                'reason': recommendation['reason'],
                'estimated_time_minutes': estimated_time,
                'route_analysis': {
                    'path': primary_path.full_path,
                    'hop_count': primary_path.hop_count,
                    'estimated_bandwidth_mbps': primary_path.estimated_bandwidth_mbps,
                    'estimated_latency_ms': primary_path.estimated_latency_ms,
                    'bottleneck_location': primary_path.bottleneck_location,
                    'path_quality': route_response.path_analysis.get('path_quality'),
                    'alternative_count': len(route_response.alternative_paths)
                },
                'optimization_notes': recommendation.get('notes', [])
            }
            
        except Exception as e:
            self._logger.error(f"Failed to generate transfer recommendation: {e}")
            return {
                'recommendation': 'basic_transfer',
                'reason': f'Recommendation failed: {e}',
                'estimated_time_minutes': None,
                'route_analysis': None
            }
    
    async def refresh_topology_for_locations(
        self, 
        location_names: List[str]
    ) -> Dict[str, Any]:
        """
        Refresh network topology for specific locations.
        
        Args:
            location_names: List of location names to refresh
            
        Returns:
            Refresh operation summary
        """
        from .network_topology_service import TopologyBenchmarkDto
        
        self._logger.info(f"Refreshing topology for locations: {location_names}")
        
        # Create location pairs for all combinations
        location_pairs = []
        for i, source in enumerate(location_names):
            for dest in location_names[i+1:]:
                location_pairs.append((source, dest))
        
        if not location_pairs:
            return {'refreshed_connections': 0, 'message': 'No location pairs to refresh'}
        
        # Create benchmark DTO
        benchmark_dto = TopologyBenchmarkDto(
            topology_name='default',
            location_pairs=location_pairs,
            force_refresh=True,
            max_concurrent_tests=3
        )
        
        # Execute refresh
        results = await self._network_service.benchmark_topology(benchmark_dto)
        
        return {
            'refreshed_connections': results.get('successful_benchmarks', 0),
            'failed_refreshes': results.get('failed_benchmarks', 0),
            'topology_stats': results.get('topology_stats', {})
        }
    
    # Private helper methods
    
    def _should_optimize_route(self, dto: FileTransferOperationDto) -> bool:
        """Determine if route optimization would be beneficial."""
        # Simple heuristics for when to optimize
        
        # Always optimize for remote transfers
        if 'remote' in dto.source_location.lower() or 'remote' in dto.dest_location.lower():
            return True
        
        # Optimize if locations are different types (compute vs storage)
        if ('compute' in dto.source_location.lower()) != ('compute' in dto.dest_location.lower()):
            return True
        
        # Optimize if locations suggest geographic separation
        geographic_indicators = ['hpc', 'cluster', 'cloud', 'aws', 'azure', 'gcp']
        source_geo = any(ind in dto.source_location.lower() for ind in geographic_indicators)
        dest_geo = any(ind in dto.dest_location.lower() for ind in geographic_indicators)
        
        if source_geo or dest_geo:
            return True
        
        return False
    
    async def _find_optimal_route(
        self,
        source_location: str,
        dest_location: str,
        optimize_for: str,
        avoid_bottlenecks: bool = True
    ) -> Optional[Any]:
        """Find optimal route with timeout and error handling."""
        try:
            route_request = OptimalRouteRequestDto(
                source_location=source_location,
                destination_location=dest_location,
                optimize_for=optimize_for,
                avoid_bottlenecks=avoid_bottlenecks
            )
            
            # Use timeout to prevent hanging
            route_response = await asyncio.wait_for(
                self._network_service.find_optimal_route(route_request),
                timeout=self.route_optimization_timeout
            )
            
            return route_response
            
        except asyncio.TimeoutError:
            self._logger.warning("Route optimization timed out")
            return None
        except Exception as e:
            self._logger.warning(f"Route optimization failed: {e}")
            if self._auto_discover:
                # Trigger topology discovery for these locations
                await self._trigger_topology_discovery([source_location, dest_location])
            return None
    
    async def _execute_multi_hop_transfer(
        self,
        dto: FileTransferOperationDto,
        path: Any
    ) -> FileTransferResultDto:
        """Execute multi-hop file transfer through intermediate locations."""
        self._logger.info(f"Executing multi-hop transfer via {len(path.intermediate_hops)} hops")
        
        # Multi-hop transfers are complex and would require:
        # 1. Transfer to first intermediate location
        # 2. Transfer between intermediate locations
        # 3. Transfer to final destination
        # 4. Cleanup of intermediate copies
        
        # For now, fall back to direct transfer with a warning
        self._logger.warning("Multi-hop transfers not fully implemented, using direct transfer")
        warnings.warn("Multi-hop transfers are not yet fully implemented")
        
        return await self._base_service.transfer_file(dto)
    
    def _enhance_transfer_result(
        self,
        result: FileTransferResultDto,
        route_response: Any,
        path: Any
    ) -> FileTransferResultDto:
        """Enhance transfer result with routing information."""
        # Add network optimization metadata to the result
        if not hasattr(result, 'metadata'):
            result.metadata = {}
        
        result.metadata.update({
            'network_optimized': True,
            'optimal_path': path.full_path,
            'path_bandwidth_mbps': path.estimated_bandwidth_mbps,
            'path_latency_ms': path.estimated_latency_ms,
            'path_quality': route_response.path_analysis.get('path_quality', 'unknown'),
            'route_recommendation': route_response.recommendation
        })
        
        return result
    
    def _group_transfers_by_route(
        self,
        transfers: List[FileTransferOperationDto]
    ) -> Dict[tuple, List[FileTransferOperationDto]]:
        """Group transfers by source-destination location pairs."""
        groups = {}
        
        for transfer in transfers:
            key = (transfer.source_location, transfer.dest_location)
            if key not in groups:
                groups[key] = []
            groups[key].append(transfer)
        
        return groups
    
    async def _execute_group_transfers(
        self,
        transfers: List[FileTransferOperationDto],
        route_response: Optional[Any],
        optimize_for: str
    ) -> List[FileTransferResultDto]:
        """Execute a group of transfers with shared route optimization."""
        results = []
        
        for transfer_dto in transfers:
            try:
                if route_response:
                    # Use the pre-calculated route
                    result = await self._base_service.transfer_file(transfer_dto)
                    if result.success:
                        result = self._enhance_transfer_result(
                            result, route_response, route_response.primary_path
                        )
                else:
                    # Direct transfer without optimization
                    result = await self._base_service.transfer_file(transfer_dto)
                
                results.append(result)
                
            except Exception as e:
                self._logger.error(f"Group transfer failed: {e}")
                # Create error result
                error_result = FileTransferResultDto(
                    operation_id=f"error_{int(time.time())}",
                    operation_type="file_transfer",
                    success=False,
                    source_location=transfer_dto.source_location,
                    source_path=transfer_dto.source_path,
                    dest_location=transfer_dto.dest_location,
                    dest_path=transfer_dto.dest_path,
                    error_message=str(e)
                )
                results.append(error_result)
        
        return results
    
    def _convert_to_batch_result(
        self,
        individual_results: List[FileTransferResultDto],
        original_dto: BatchFileTransferOperationDto
    ) -> BatchFileTransferResultDto:
        """Convert individual transfer results to batch result."""
        successful_transfers = [r for r in individual_results if r.success]
        failed_transfers = [r for r in individual_results if not r.success]
        
        total_bytes = sum(r.bytes_transferred for r in successful_transfers)
        total_duration = max((r.duration_seconds for r in individual_results), default=0.0)
        avg_throughput = (total_bytes / (1024 * 1024)) / total_duration if total_duration > 0 else 0
        
        return BatchFileTransferResultDto(
            operation_id=f"batch_{int(time.time())}",
            operation_type="batch_file_transfer",
            total_files=len(individual_results),
            successful_transfers=successful_transfers,
            failed_transfers=failed_transfers,
            total_bytes_transferred=total_bytes,
            total_duration_seconds=total_duration,
            average_throughput_mbps=avg_throughput
        )
    
    def _generate_transfer_recommendation(
        self,
        route_response: Any,
        estimated_time: Optional[float],
        file_size_mb: Optional[float]
    ) -> Dict[str, Any]:
        """Generate transfer recommendation based on route analysis."""
        primary_path = route_response.primary_path
        path_analysis = route_response.path_analysis
        
        # Base recommendation on path quality and characteristics
        path_quality = path_analysis.get('path_quality', 'unknown')
        has_bottleneck = primary_path.bottleneck_location is not None
        is_multi_hop = len(primary_path.intermediate_hops) > 0
        
        if path_quality == 'excellent' and not has_bottleneck:
            return {
                'type': 'optimal_direct',
                'reason': 'Excellent network path with high performance',
                'notes': ['Transfer should complete quickly', 'Low latency route']
            }
        elif path_quality in ['good', 'fair'] and not is_multi_hop:
            return {
                'type': 'direct_transfer',
                'reason': 'Good direct connection available',
                'notes': ['Acceptable performance expected']
            }
        elif has_bottleneck:
            notes = ['Path contains bottleneck - transfer may be slow']
            if estimated_time and estimated_time > 60:  # > 1 hour
                notes.append('Consider splitting large files or transferring during off-peak hours')
            
            return {
                'type': 'bottleneck_warning',
                'reason': f'Bottleneck detected at {primary_path.bottleneck_location}',
                'notes': notes
            }
        elif is_multi_hop:
            return {
                'type': 'multi_hop_available',
                'reason': 'Multi-hop path may offer better performance',
                'notes': ['Consider enabling multi-hop transfers for better performance']
            }
        else:
            return {
                'type': 'standard_transfer',
                'reason': 'Standard transfer path available',
                'notes': []
            }
    
    async def _trigger_topology_discovery(self, location_names: List[str]) -> None:
        """Trigger background topology discovery for locations."""
        try:
            self._logger.info(f"Triggering topology discovery for: {location_names}")
            
            # Run discovery in background without waiting
            asyncio.create_task(
                self._network_service.discover_topology(
                    location_names=location_names
                )
            )
        except Exception as e:
            self._logger.warning(f"Failed to trigger topology discovery: {e}")