"""
CLI commands for network topology management and optimal routing.

This module provides CLI interfaces for:
- Network topology discovery and management
- Connection benchmarking and monitoring  
- Optimal route calculation and analysis
- Integration with file transfer operations
"""

import asyncio
import json
from typing import List, Optional, Tuple

import rich_click as click
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.progress import track
from rich.text import Text
import questionary

from ...application.services.network_topology_service import (
    NetworkTopologyApplicationService,
    CreateNetworkTopologyDto,
    OptimalRouteRequestDto, 
    TopologyBenchmarkDto
)
from ...application.container import get_service_container
from ...domain.entities.network_connection import ConnectionType
from ...domain.entities.network_metrics import NetworkHealth
from .main import console


@click.group(name='network')
def network_commands():
    """Network topology and routing management commands."""
    pass


@network_commands.command('discover')
@click.option('--topology-name', '-t', 
              help='Name of topology to create/update', 
              default='default')
@click.option('--location', '-l', 'locations', multiple=True,
              help='Specific locations to include (default: all locations)')
@click.option('--parallel-tests', '-p', type=int, default=3,
              help='Maximum parallel benchmark tests')
@click.option('--quiet', '-q', is_flag=True,
              help='Suppress progress output')
def discover_topology(topology_name: str, locations: Tuple[str], 
                     parallel_tests: int, quiet: bool):
    """
    Discover network topology by benchmarking connections between locations.
    
    This command tests connectivity, bandwidth, and latency between all location
    pairs (or specified locations) to build a comprehensive network topology map.
    
    Examples:
        tellus network discover
        tellus network discover -t production-topology  
        tellus network discover -l hpc-storage -l local-workspace
        tellus network discover --parallel-tests 5
    """
    if not quiet:
        console.print(f"[green]✨ Discovering network topology: {topology_name}[/green]")
        if locations:
            console.print(f"[dim]Including locations: {', '.join(locations)}[/dim]")
        else:
            console.print("[dim]Testing all available locations[/dim]")
    
    async def run_discovery():
        container = get_service_container()
        service: NetworkTopologyApplicationService = container.get_network_topology_service()
        
        try:
            # Configure service for parallel testing
            service.max_concurrent_benchmarks = parallel_tests
            
            # Run topology discovery
            topology = await service.discover_topology(
                topology_name=topology_name,
                location_names=list(locations) if locations else None
            )
            
            # Display results
            if not quiet:
                console.print(f"[green]✅ Discovery completed[/green]")
                console.print(f"[dim]Found {topology.connection_count} connections between "
                            f"{len(topology.location_names)} locations[/dim]")
            
            # Show topology summary
            await _display_topology_summary(topology_name, service)
            
        except Exception as e:
            console.print(f"[red]❌ Discovery failed: {e}[/red]")
            raise click.Abort()
    
    try:
        asyncio.run(run_discovery())
    except KeyboardInterrupt:
        console.print("\n[yellow]⚠️ Discovery cancelled by user[/yellow]")
        raise click.Abort()


@network_commands.command('benchmark')
@click.option('--topology-name', '-t',
              help='Name of topology to benchmark',
              default='default')
@click.option('--source', '-s', 
              help='Source location name')
@click.option('--destination', '-d',
              help='Destination location name')  
@click.option('--all-connections', is_flag=True,
              help='Benchmark all connections in topology')
@click.option('--include-bandwidth/--no-bandwidth', default=True,
              help='Include bandwidth measurements')
@click.option('--include-latency/--no-latency', default=True,
              help='Include latency measurements')
@click.option('--force-refresh', is_flag=True,
              help='Force refresh of cached measurements')
@click.option('--parallel-tests', '-p', type=int, default=3,
              help='Maximum parallel benchmark tests')
def benchmark_connections(topology_name: str, source: Optional[str], 
                         destination: Optional[str], all_connections: bool,
                         include_bandwidth: bool, include_latency: bool,
                         force_refresh: bool, parallel_tests: int):
    """
    Benchmark network connections to measure performance.
    
    Measures bandwidth, latency, and connection quality between locations.
    Results are cached and used for optimal routing decisions.
    
    Examples:
        tellus network benchmark --all-connections
        tellus network benchmark -s hpc-storage -d local-workspace
        tellus network benchmark --force-refresh --parallel-tests 5
    """
    console.print(f"[green]✨ Benchmarking network connections[/green]")
    
    async def run_benchmark():
        container = get_service_container()
        service: NetworkTopologyApplicationService = container.get_network_topology_service()
        
        try:
            # Determine location pairs to benchmark
            location_pairs = []
            
            if source and destination:
                location_pairs = [(source, destination)]
                console.print(f"[dim]Testing connection: {source} ↔ {destination}[/dim]")
            elif all_connections:
                console.print(f"[dim]Testing all connections in topology: {topology_name}[/dim]")
            else:
                # Interactive selection
                topology_status = await service.get_topology_status(topology_name)
                if not topology_status.get('exists'):
                    console.print(f"[red]❌ Topology '{topology_name}' not found[/red]")
                    raise click.Abort()
                
                # Get available locations for interactive selection
                container = get_service_container()
                location_repo = container.get_location_repository()
                locations = [loc.name for loc in location_repo.list_all()]
                
                if len(locations) < 2:
                    console.print("[red]❌ At least 2 locations required for benchmarking[/red]")
                    raise click.Abort()
                
                # Interactive location selection
                console.print("\n[blue]Select locations to benchmark:[/blue]")
                
                source_loc = questionary.select(
                    "Source location:",
                    choices=locations
                ).ask()
                
                dest_choices = [loc for loc in locations if loc != source_loc]
                dest_loc = questionary.select(
                    "Destination location:",
                    choices=dest_choices
                ).ask()
                
                if source_loc and dest_loc:
                    location_pairs = [(source_loc, dest_loc)]
            
            # Create benchmark DTO
            benchmark_dto = TopologyBenchmarkDto(
                topology_name=topology_name,
                location_pairs=location_pairs,
                include_bandwidth=include_bandwidth,
                include_latency=include_latency,
                force_refresh=force_refresh,
                max_concurrent_tests=parallel_tests
            )
            
            # Run benchmarking
            results = await service.benchmark_topology(benchmark_dto)
            
            # Display results
            _display_benchmark_results(results)
            
        except Exception as e:
            console.print(f"[red]❌ Benchmarking failed: {e}[/red]")
            raise click.Abort()
    
    try:
        asyncio.run(run_benchmark())
    except KeyboardInterrupt:
        console.print("\n[yellow]⚠️ Benchmarking cancelled by user[/yellow]")
        raise click.Abort()


@network_commands.command('route')
@click.argument('source')
@click.argument('destination') 
@click.option('--optimize-for', '-o',
              type=click.Choice(['bandwidth', 'latency', 'cost', 'reliability']),
              default='bandwidth',
              help='Optimization criteria for route selection')
@click.option('--avoid-bottlenecks/--allow-bottlenecks', default=True,
              help='Avoid connections identified as bottlenecks')
@click.option('--max-hops', type=int,
              help='Maximum number of hops allowed')
@click.option('--min-bandwidth', type=float,
              help='Minimum required bandwidth (Mbps)')
@click.option('--max-latency', type=float, 
              help='Maximum acceptable latency (ms)')
@click.option('--show-alternatives', is_flag=True,
              help='Show alternative routes')
@click.option('--json-output', is_flag=True,
              help='Output results in JSON format')
def find_optimal_route(source: str, destination: str, optimize_for: str,
                      avoid_bottlenecks: bool, max_hops: Optional[int],
                      min_bandwidth: Optional[float], max_latency: Optional[float],
                      show_alternatives: bool, json_output: bool):
    """
    Find optimal route between two locations.
    
    Calculates the best path for data transfer based on network topology
    and specified optimization criteria.
    
    Examples:
        tellus network route hpc-storage local-workspace
        tellus network route source dest --optimize-for latency
        tellus network route source dest --min-bandwidth 100 --max-latency 50
        tellus network route source dest --show-alternatives --json-output
    """
    if not json_output:
        console.print(f"[green]✨ Finding optimal route: {source} → {destination}[/green]")
        console.print(f"[dim]Optimizing for: {optimize_for}[/dim]")
    
    async def find_route():
        container = get_service_container()
        service: NetworkTopologyApplicationService = container.get_network_topology_service()
        
        try:
            # Create route request
            route_request = OptimalRouteRequestDto(
                source_location=source,
                destination_location=destination,
                optimize_for=optimize_for,
                avoid_bottlenecks=avoid_bottlenecks,
                max_hops=max_hops,
                required_min_bandwidth_mbps=min_bandwidth,
                max_acceptable_latency_ms=max_latency
            )
            
            # Find optimal route
            route_response = await service.find_optimal_route(route_request)
            
            # Output results
            if json_output:
                route_data = {
                    'request_id': route_response.request_id,
                    'primary_path': _path_dto_to_dict(route_response.primary_path),
                    'alternative_paths': [_path_dto_to_dict(path) for path in route_response.alternative_paths],
                    'path_analysis': route_response.path_analysis,
                    'recommendation': route_response.recommendation
                }
                console.print(json.dumps(route_data, indent=2))
            else:
                _display_route_results(route_response, show_alternatives)
            
        except Exception as e:
            if json_output:
                error_data = {'error': str(e), 'success': False}
                console.print(json.dumps(error_data))
            else:
                console.print(f"[red]❌ Route calculation failed: {e}[/red]")
            raise click.Abort()
    
    try:
        asyncio.run(find_route())
    except KeyboardInterrupt:
        if not json_output:
            console.print("\n[yellow]⚠️ Route calculation cancelled[/yellow]")
        raise click.Abort()


@network_commands.command('status')
@click.option('--topology-name', '-t',
              help='Name of topology to check',
              default='default')
@click.option('--detailed', is_flag=True,
              help='Show detailed connection information')
@click.option('--json-output', is_flag=True,
              help='Output results in JSON format')
def topology_status(topology_name: str, detailed: bool, json_output: bool):
    """
    Show network topology status and health.
    
    Displays comprehensive information about network topology including
    connection health, performance statistics, and refresh requirements.
    
    Examples:
        tellus network status
        tellus network status --detailed
        tellus network status -t production-topology --json-output
    """
    async def get_status():
        container = get_service_container()
        service: NetworkTopologyApplicationService = container.get_network_topology_service()
        
        try:
            status = await service.get_topology_status(topology_name)
            
            if json_output:
                console.print(json.dumps(status, indent=2))
            else:
                _display_topology_status(status, detailed)
                
        except Exception as e:
            if json_output:
                error_data = {'error': str(e), 'success': False}
                console.print(json.dumps(error_data))
            else:
                console.print(f"[red]❌ Failed to get topology status: {e}[/red]")
            raise click.Abort()
    
    try:
        asyncio.run(get_status())
    except KeyboardInterrupt:
        if not json_output:
            console.print("\n[yellow]⚠️ Status check cancelled[/yellow]")
        raise click.Abort()


@network_commands.command('refresh')
@click.option('--topology-name', '-t',
              help='Name of topology to refresh',
              default='default')
@click.option('--force', is_flag=True,
              help='Force refresh even if not needed')
def refresh_topology(topology_name: str, force: bool):
    """
    Refresh stale network topology data.
    
    Updates outdated connection measurements and recalculates
    optimal routes based on current network performance.
    
    Examples:
        tellus network refresh
        tellus network refresh --force
        tellus network refresh -t production-topology
    """
    console.print(f"[green]✨ Refreshing topology: {topology_name}[/green]")
    
    async def refresh():
        container = get_service_container()
        service: NetworkTopologyApplicationService = container.get_network_topology_service()
        
        try:
            # Check if refresh is needed
            status = await service.get_topology_status(topology_name)
            
            if not status.get('exists'):
                console.print(f"[red]❌ Topology '{topology_name}' not found[/red]")
                raise click.Abort()
            
            if not force and not status.get('needs_refresh', False):
                console.print("[yellow]⚠️ Topology does not need refresh[/yellow]")
                console.print("[dim]Use --force to refresh anyway[/dim]")
                return
            
            # Refresh stale connections
            benchmark_dto = TopologyBenchmarkDto(
                topology_name=topology_name,
                force_refresh=force,
                max_concurrent_tests=3
            )
            
            results = await service.benchmark_topology(benchmark_dto)
            
            console.print(f"[green]✅ Refresh completed[/green]")
            console.print(f"[dim]Updated {results['successful_benchmarks']} connections[/dim]")
            
        except Exception as e:
            console.print(f"[red]❌ Refresh failed: {e}[/red]")
            raise click.Abort()
    
    try:
        asyncio.run(refresh())
    except KeyboardInterrupt:
        console.print("\n[yellow]⚠️ Refresh cancelled by user[/yellow]")
        raise click.Abort()


# Helper functions for display formatting

async def _display_topology_summary(topology_name: str, service: NetworkTopologyApplicationService):
    """Display topology discovery summary."""
    status = await service.get_topology_status(topology_name)
    
    # Create summary table
    table = Table(title=f"Network Topology: {topology_name}")
    table.add_column("Metric", style="cyan", no_wrap=True)
    table.add_column("Value", style="magenta")
    
    table.add_row("Total Locations", str(status.get('total_locations', 0)))
    table.add_row("Total Connections", str(status.get('total_connections', 0)))
    table.add_row("Bottleneck Connections", str(status.get('bottleneck_connections', 0)))
    table.add_row("Stale Connections", str(status.get('stale_connections', 0)))
    
    bandwidth_stats = status.get('bandwidth_stats', {})
    if bandwidth_stats:
        table.add_row("Avg Bandwidth", f"{bandwidth_stats['avg']:.1f} Mbps")
        table.add_row("Max Bandwidth", f"{bandwidth_stats['max']:.1f} Mbps")
    
    latency_stats = status.get('latency_stats', {})
    if latency_stats:
        table.add_row("Avg Latency", f"{latency_stats['avg']:.1f} ms")
        table.add_row("Min Latency", f"{latency_stats['min']:.1f} ms")
    
    console.print(table)


def _display_benchmark_results(results: dict):
    """Display benchmarking results."""
    console.print(f"\n[green]✅ Benchmarking Results[/green]")
    
    # Summary table
    table = Table(title="Benchmark Summary")
    table.add_column("Metric", style="cyan")
    table.add_column("Value", style="magenta")
    
    table.add_row("Total Pairs Tested", str(results.get('total_pairs_tested', 0)))
    table.add_row("Successful Tests", str(results.get('successful_benchmarks', 0)))
    table.add_row("Failed Tests", str(results.get('failed_benchmarks', 0)))
    
    topology_stats = results.get('topology_stats', {})
    if topology_stats:
        table.add_row("Total Connections", str(topology_stats.get('total_connections', 0)))
        table.add_row("Avg Bandwidth", f"{topology_stats.get('average_bandwidth_mbps', 0):.1f} Mbps")
        table.add_row("Bottlenecks", str(topology_stats.get('bottleneck_connections', 0)))
    
    console.print(table)
    
    # Connection details
    updated_connections = results.get('updated_connections', [])
    if updated_connections:
        conn_table = Table(title="Updated Connections")
        conn_table.add_column("Source", style="cyan")
        conn_table.add_column("Destination", style="cyan") 
        conn_table.add_column("Bandwidth", style="green")
        conn_table.add_column("Health", style="yellow")
        
        for conn in updated_connections:
            health_color = _get_health_color(conn.get('health', 'UNKNOWN'))
            conn_table.add_row(
                conn['source'],
                conn['destination'],
                f"{conn['bandwidth_mbps']:.1f} Mbps",
                f"[{health_color}]{conn['health']}[/{health_color}]"
            )
        
        console.print(conn_table)


def _display_route_results(route_response, show_alternatives: bool):
    """Display optimal route results."""
    primary_path = route_response.primary_path
    
    # Primary route panel
    path_display = " → ".join(primary_path.full_path)
    
    route_info = [
        f"[green]Path:[/green] {path_display}",
        f"[green]Hops:[/green] {primary_path.hop_count}",
        f"[green]Bandwidth:[/green] {primary_path.estimated_bandwidth_mbps:.1f} Mbps",
        f"[green]Latency:[/green] {primary_path.estimated_latency_ms:.1f} ms",
        f"[green]Type:[/green] {primary_path.path_type}"
    ]
    
    if primary_path.bottleneck_location:
        route_info.append(f"[yellow]Bottleneck:[/yellow] {primary_path.bottleneck_location}")
    
    primary_panel = Panel(
        "\n".join(route_info),
        title="[bold blue]Optimal Route[/bold blue]",
        border_style="blue"
    )
    console.print(primary_panel)
    
    # Path analysis
    analysis = route_response.path_analysis
    quality = analysis.get('path_quality', 'unknown')
    quality_color = {
        'excellent': 'green',
        'good': 'blue', 
        'fair': 'yellow',
        'poor': 'red'
    }.get(quality, 'white')
    
    analysis_info = [
        f"[green]Quality:[/green] [{quality_color}]{quality.upper()}[/{quality_color}]",
        f"[green]Optimization:[/green] {analysis.get('optimization_criteria', 'unknown')}"
    ]
    
    violations = analysis.get('constraint_violations', [])
    if violations:
        analysis_info.append(f"[red]Violations:[/red] {'; '.join(violations)}")
    
    bottleneck_analysis = analysis.get('bottleneck_analysis', {})
    if bottleneck_analysis.get('has_bottleneck'):
        limiting_factor = bottleneck_analysis.get('limiting_factor', 'unknown')
        analysis_info.append(f"[yellow]Limiting Factor:[/yellow] {limiting_factor}")
    
    analysis_panel = Panel(
        "\n".join(analysis_info),
        title="[bold yellow]Path Analysis[/bold yellow]",
        border_style="yellow"
    )
    console.print(analysis_panel)
    
    # Recommendation
    recommendation_panel = Panel(
        route_response.recommendation,
        title="[bold green]Recommendation[/bold green]",
        border_style="green"
    )
    console.print(recommendation_panel)
    
    # Alternative routes
    if show_alternatives and route_response.alternative_paths:
        console.print("\n[bold]Alternative Routes:[/bold]")
        
        alt_table = Table()
        alt_table.add_column("Path", style="cyan")
        alt_table.add_column("Hops", style="magenta")
        alt_table.add_column("Bandwidth", style="green")
        alt_table.add_column("Latency", style="yellow")
        alt_table.add_column("Type", style="blue")
        
        for alt_path in route_response.alternative_paths:
            alt_path_display = " → ".join(alt_path.full_path)
            alt_table.add_row(
                alt_path_display,
                str(alt_path.hop_count),
                f"{alt_path.estimated_bandwidth_mbps:.1f} Mbps",
                f"{alt_path.estimated_latency_ms:.1f} ms",
                alt_path.path_type
            )
        
        console.print(alt_table)


def _display_topology_status(status: dict, detailed: bool):
    """Display topology status information."""
    if not status.get('exists'):
        console.print(f"[red]❌ Topology '{status['name']}' does not exist[/red]")
        return
    
    # Main status panel
    status_info = [
        f"[green]Name:[/green] {status['name']}",
        f"[green]Locations:[/green] {status.get('total_locations', 0)}",
        f"[green]Connections:[/green] {status.get('total_connections', 0)}",
        f"[green]Auto Discovery:[/green] {'✅' if status.get('auto_discovery_enabled') else '❌'}"
    ]
    
    if status.get('needs_refresh'):
        status_info.append("[yellow]Status:[/yellow] [red]Needs Refresh[/red]")
    else:
        status_info.append("[yellow]Status:[/yellow] [green]Up to Date[/green]")
    
    main_panel = Panel(
        "\n".join(status_info),
        title="[bold blue]Topology Status[/bold blue]",
        border_style="blue"
    )
    console.print(main_panel)
    
    # Health distribution
    health_dist = status.get('health_distribution', {})
    if health_dist:
        health_table = Table(title="Connection Health Distribution")
        health_table.add_column("Health Status", style="cyan")
        health_table.add_column("Count", style="magenta")
        health_table.add_column("Percentage", style="green")
        
        total_connections = sum(health_dist.values())
        for health, count in health_dist.items():
            percentage = (count / total_connections) * 100 if total_connections > 0 else 0
            health_color = _get_health_color(health)
            health_table.add_row(
                f"[{health_color}]{health}[/{health_color}]",
                str(count),
                f"{percentage:.1f}%"
            )
        
        console.print(health_table)
    
    # Performance statistics
    if detailed:
        bandwidth_stats = status.get('bandwidth_stats', {})
        latency_stats = status.get('latency_stats', {})
        
        if bandwidth_stats or latency_stats:
            perf_table = Table(title="Performance Statistics")
            perf_table.add_column("Metric", style="cyan")
            perf_table.add_column("Value", style="magenta")
            
            if bandwidth_stats:
                perf_table.add_row("Min Bandwidth", f"{bandwidth_stats['min']:.1f} Mbps")
                perf_table.add_row("Avg Bandwidth", f"{bandwidth_stats['avg']:.1f} Mbps") 
                perf_table.add_row("Max Bandwidth", f"{bandwidth_stats['max']:.1f} Mbps")
            
            if latency_stats:
                perf_table.add_row("Min Latency", f"{latency_stats['min']:.1f} ms")
                perf_table.add_row("Avg Latency", f"{latency_stats['avg']:.1f} ms")
                perf_table.add_row("Max Latency", f"{latency_stats['max']:.1f} ms")
            
            console.print(perf_table)


def _get_health_color(health: str) -> str:
    """Get color for health status display."""
    health_colors = {
        'OPTIMAL': 'green',
        'CONGESTED': 'yellow',
        'DEGRADED': 'orange',
        'UNSTABLE': 'red',
        'UNAVAILABLE': 'bright_red'
    }
    return health_colors.get(health.upper(), 'white')


def _path_dto_to_dict(path_dto) -> dict:
    """Convert NetworkPathDto to dictionary for JSON output."""
    return {
        'source_location': path_dto.source_location,
        'destination_location': path_dto.destination_location,
        'intermediate_hops': path_dto.intermediate_hops,
        'full_path': [path_dto.source_location] + path_dto.intermediate_hops + [path_dto.destination_location],
        'hop_count': len(path_dto.intermediate_hops) + 2,
        'total_cost': path_dto.total_cost,
        'estimated_bandwidth_mbps': path_dto.estimated_bandwidth_mbps,
        'estimated_latency_ms': path_dto.estimated_latency_ms,
        'bottleneck_location': path_dto.bottleneck_location,
        'path_type': path_dto.path_type
    }


# Integration with existing archive commands

@click.command()
@click.argument('source_location')
@click.argument('dest_location') 
@click.option('--optimize-route', is_flag=True,
              help='Use network topology for optimal routing')
@click.option('--avoid-bottlenecks', is_flag=True, default=True,
              help='Avoid known bottleneck connections')
@click.option('--show-route', is_flag=True,
              help='Display planned transfer route')
def stage_with_routing(source_location: str, dest_location: str,
                      optimize_route: bool, avoid_bottlenecks: bool,
                      show_route: bool):
    """
    Stage archive with network-optimized routing.
    
    Enhanced version of archive staging that uses network topology
    to determine optimal transfer routes and avoid bottlenecks.
    """
    console.print(f"[green]✨ Staging archive with network optimization[/green]")
    
    async def stage_with_optimization():
        container = get_service_container()
        network_service: NetworkTopologyApplicationService = container.get_network_topology_service()
        
        try:
            if optimize_route or show_route:
                # Find optimal route
                route_request = OptimalRouteRequestDto(
                    source_location=source_location,
                    destination_location=dest_location,
                    optimize_for='bandwidth',
                    avoid_bottlenecks=avoid_bottlenecks
                )
                
                route_response = await network_service.find_optimal_route(route_request)
                
                if show_route:
                    console.print("\n[bold blue]Planned Transfer Route:[/bold blue]")
                    _display_route_results(route_response, show_alternatives=False)
                
                # Check for performance warnings
                primary_path = route_response.primary_path
                if primary_path.bottleneck_location:
                    console.print(f"[yellow]⚠️ Route contains bottleneck at: {primary_path.bottleneck_location}[/yellow]")
                
                if primary_path.estimated_bandwidth_mbps < 10:
                    console.print("[yellow]⚠️ Low bandwidth route - transfer may be slow[/yellow]")
            
            # Here would integrate with existing archive staging logic
            console.print(f"[green]✅ Archive staging would proceed with optimized route[/green]")
            console.print(f"[dim]Integration with existing archive service needed[/dim]")
            
        except Exception as e:
            console.print(f"[red]❌ Archive staging failed: {e}[/red]")
            raise click.Abort()
    
    try:
        asyncio.run(stage_with_optimization())
    except KeyboardInterrupt:
        console.print("\n[yellow]⚠️ Archive staging cancelled[/yellow]")
        raise click.Abort()


# Export the main command group
__all__ = ['network_commands', 'stage_with_routing']