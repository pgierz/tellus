"""
CLI commands for progress tracking operations.

This module provides comprehensive command-line interface for managing and monitoring
progress of long-running operations in the Tellus system.
"""

import asyncio
import json
import time
from typing import Optional, List
import uuid

import rich_click as click
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn, TimeElapsedColumn
from rich.live import Live
import signal
import sys

from ...application.container import get_service_container
from ...application.dtos import (
    CreateProgressTrackingDto,
    UpdateProgressDto,
    ProgressMetricsDto,
    ThroughputMetricsDto,
    OperationContextDto,
    OperationControlDto,
    ProgressCallbackRegistrationDto,
    ProgressUpdateNotificationDto,
    FilterOptions,
    PaginationInfo
)

console = Console()


@click.group()
def progress():
    """Manage and monitor progress of long-running operations"""
    pass


@progress.command()
@click.option('--status', type=click.Choice(['pending', 'running', 'completed', 'failed', 'cancelled']), 
              help='Filter by operation status')
@click.option('--operation-type', help='Filter by operation type')
@click.option('--user-id', help='Filter by user ID')
@click.option('--limit', type=int, default=20, help='Maximum number of operations to show')
@click.option('--page', type=int, default=1, help='Page number for pagination')
@click.option('--json-output', is_flag=True, help='Output in JSON format')
def list_operations(status: Optional[str], operation_type: Optional[str], user_id: Optional[str], 
                   limit: int, page: int, json_output: bool):
    """List progress tracking operations"""
    
    async def _list_operations():
        service_container = get_service_container()
        progress_service = service_container.progress_tracking_service
        
        filters = FilterOptions(
            search_term=operation_type
        )
        
        pagination = PaginationInfo(
            page=page,
            page_size=limit,
            total_count=0,
            has_next=False,
            has_previous=page > 1
        )
        
        try:
            result = await progress_service.list_operations(filters, pagination)
            
            if json_output:
                # Convert to JSON-serializable format
                operations_data = []
                for op in result.operations:
                    operations_data.append({
                        'operation_id': op.operation_id,
                        'operation_type': op.operation_type,
                        'operation_name': op.operation_name,
                        'status': op.status,
                        'priority': op.priority,
                        'progress_percentage': op.current_metrics.percentage,
                        'created_time': op.created_time,
                        'last_update_time': op.last_update_time,
                        'duration_seconds': op.duration_seconds
                    })
                
                console.print(json.dumps(operations_data, indent=2))
                return
            
            # Create rich table
            table = Table(title=f"Progress Tracking Operations (Page {page})")
            table.add_column("ID", style="cyan", no_wrap=True)
            table.add_column("Name", style="green")
            table.add_column("Type", style="blue")
            table.add_column("Status", style="yellow")
            table.add_column("Progress", style="magenta")
            table.add_column("Duration", style="white")
            
            for op in result.operations:
                # Format duration
                duration_str = "N/A"
                if op.duration_seconds:
                    if op.duration_seconds < 60:
                        duration_str = f"{op.duration_seconds:.1f}s"
                    elif op.duration_seconds < 3600:
                        duration_str = f"{op.duration_seconds/60:.1f}m"
                    else:
                        duration_str = f"{op.duration_seconds/3600:.1f}h"
                
                # Status styling
                status_style = {
                    'pending': '⏳ Pending',
                    'running': '🔄 Running',
                    'completed': '✅ Completed',
                    'failed': '❌ Failed',
                    'cancelled': '⏹️ Cancelled'
                }.get(op.status, f"❓ {op.status.title()}")
                
                table.add_row(
                    op.operation_id[:8] + "...",
                    op.operation_name,
                    op.operation_type,
                    status_style,
                    f"{op.current_metrics.percentage:.1f}%",
                    duration_str
                )
            
            console.print(table)
            
            # Show pagination info
            if result.pagination.has_next or result.pagination.has_previous:
                pagination_info = f"Page {result.pagination.page}"
                if result.pagination.has_previous:
                    pagination_info += " | Previous: --page " + str(result.pagination.page - 1)
                if result.pagination.has_next:
                    pagination_info += " | Next: --page " + str(result.pagination.page + 1)
                console.print(f"\n[dim]{pagination_info}[/dim]")
            
        except Exception as e:
            console.print(f"[red]Error listing operations:[/red] {e}")
            sys.exit(1)
    
    asyncio.run(_list_operations())


@progress.command()
@click.argument('operation_id')
@click.option('--json-output', is_flag=True, help='Output in JSON format')
@click.option('--show-logs', is_flag=True, help='Show recent log entries')
@click.option('--log-limit', type=int, default=20, help='Number of log entries to show')
def show(operation_id: str, json_output: bool, show_logs: bool, log_limit: int):
    """Show detailed information about a progress tracking operation"""
    
    async def _show_operation():
        service_container = get_service_container()
        progress_service = service_container.progress_tracking_service
        
        try:
            operation = await progress_service.get_operation(operation_id)
            if not operation:
                console.print(f"[red]Operation {operation_id} not found[/red]")
                sys.exit(1)
            
            if json_output:
                operation_data = {
                    'operation_id': operation.operation_id,
                    'operation_type': operation.operation_type,
                    'operation_name': operation.operation_name,
                    'status': operation.status,
                    'priority': operation.priority,
                    'created_time': operation.created_time,
                    'started_time': operation.started_time,
                    'completed_time': operation.completed_time,
                    'last_update_time': operation.last_update_time,
                    'duration_seconds': operation.duration_seconds,
                    'current_metrics': {
                        'percentage': operation.current_metrics.percentage,
                        'current_value': operation.current_metrics.current_value,
                        'total_value': operation.current_metrics.total_value,
                        'bytes_processed': operation.current_metrics.bytes_processed,
                        'total_bytes': operation.current_metrics.total_bytes,
                        'files_processed': operation.current_metrics.files_processed,
                        'total_files': operation.current_metrics.total_files
                    },
                    'error_message': operation.error_message,
                    'warnings': operation.warnings,
                    'context': {
                        'user_id': operation.context.user_id,
                        'session_id': operation.context.session_id,
                        'simulation_id': operation.context.simulation_id,
                        'location_name': operation.context.location_name,
                        'tags': list(operation.context.tags)
                    }
                }
                console.print(json.dumps(operation_data, indent=2))
                return
            
            # Create detailed display
            panel_content = []
            
            # Basic information
            panel_content.append(f"[bold]Operation ID:[/bold] {operation.operation_id}")
            panel_content.append(f"[bold]Name:[/bold] {operation.operation_name}")
            panel_content.append(f"[bold]Type:[/bold] {operation.operation_type}")
            
            # Status with icon
            status_icon = {
                'pending': '⏳', 'running': '🔄', 'completed': '✅',
                'failed': '❌', 'cancelled': '⏹️'
            }.get(operation.status, '❓')
            panel_content.append(f"[bold]Status:[/bold] {status_icon} {operation.status.title()}")
            panel_content.append(f"[bold]Priority:[/bold] {operation.priority.title()}")
            
            # Timestamps
            from datetime import datetime
            created = datetime.fromtimestamp(operation.created_time).strftime("%Y-%m-%d %H:%M:%S")
            panel_content.append(f"[bold]Created:[/bold] {created}")
            
            if operation.started_time:
                started = datetime.fromtimestamp(operation.started_time).strftime("%Y-%m-%d %H:%M:%S")
                panel_content.append(f"[bold]Started:[/bold] {started}")
            
            if operation.completed_time:
                completed = datetime.fromtimestamp(operation.completed_time).strftime("%Y-%m-%d %H:%M:%S")
                panel_content.append(f"[bold]Completed:[/bold] {completed}")
            
            # Duration
            if operation.duration_seconds:
                if operation.duration_seconds < 60:
                    duration = f"{operation.duration_seconds:.1f} seconds"
                elif operation.duration_seconds < 3600:
                    duration = f"{operation.duration_seconds/60:.1f} minutes"
                else:
                    duration = f"{operation.duration_seconds/3600:.1f} hours"
                panel_content.append(f"[bold]Duration:[/bold] {duration}")
            
            # Progress metrics
            panel_content.append("")
            panel_content.append("[bold]Progress Metrics:[/bold]")
            panel_content.append(f"  Progress: {operation.current_metrics.percentage:.1f}%")
            
            if operation.current_metrics.total_value:
                panel_content.append(f"  Items: {operation.current_metrics.current_value}/{operation.current_metrics.total_value}")
            
            if operation.current_metrics.total_bytes:
                bytes_mb = operation.current_metrics.bytes_processed / (1024 * 1024)
                total_mb = operation.current_metrics.total_bytes / (1024 * 1024)
                panel_content.append(f"  Data: {bytes_mb:.1f}/{total_mb:.1f} MB")
            
            if operation.current_metrics.total_files:
                panel_content.append(f"  Files: {operation.current_metrics.files_processed}/{operation.current_metrics.total_files}")
            
            # Throughput
            if operation.current_throughput:
                panel_content.append("")
                panel_content.append("[bold]Throughput:[/bold]")
                if operation.current_throughput.bytes_per_second > 0:
                    rate_mb = operation.current_throughput.bytes_per_second / (1024 * 1024)
                    panel_content.append(f"  Data Rate: {rate_mb:.1f} MB/s")
                if operation.current_throughput.files_per_second > 0:
                    panel_content.append(f"  File Rate: {operation.current_throughput.files_per_second:.1f} files/s")
            
            # Context
            if any([operation.context.user_id, operation.context.simulation_id, operation.context.location_name]):
                panel_content.append("")
                panel_content.append("[bold]Context:[/bold]")
                if operation.context.user_id:
                    panel_content.append(f"  User: {operation.context.user_id}")
                if operation.context.simulation_id:
                    panel_content.append(f"  Simulation: {operation.context.simulation_id}")
                if operation.context.location_name:
                    panel_content.append(f"  Location: {operation.context.location_name}")
                if operation.context.tags:
                    panel_content.append(f"  Tags: {', '.join(operation.context.tags)}")
            
            # Warnings and errors
            if operation.warnings:
                panel_content.append("")
                panel_content.append("[bold]Warnings:[/bold]")
                for warning in operation.warnings:
                    panel_content.append(f"  ⚠️ {warning}")
            
            if operation.error_message:
                panel_content.append("")
                panel_content.append(f"[bold red]Error:[/bold red] {operation.error_message}")
            
            # Display the panel
            panel = Panel("\n".join(panel_content), title=f"Operation Details: {operation.operation_name}")
            console.print(panel)
            
            # Show logs if requested
            if show_logs:
                try:
                    from ...domain.repositories.progress_tracking_repository import IProgressTrackingRepository
                    # Access repository directly to get logs
                    repo = service_container.service_factory._progress_tracking_service._repository
                    log_entries = await repo.get_recent_log_entries(operation_id, log_limit)
                    
                    if log_entries:
                        console.print(f"\n[bold]Recent Log Entries (last {len(log_entries)}):[/bold]")
                        for entry in log_entries[-log_limit:]:
                            timestamp = datetime.fromtimestamp(entry['timestamp']).strftime("%H:%M:%S")
                            level_color = {'INFO': 'blue', 'WARN': 'yellow', 'ERROR': 'red', 'DEBUG': 'dim'}.get(entry['level'], 'white')
                            console.print(f"[{level_color}]{timestamp} [{entry['level']}][/{level_color}] {entry['message']}")
                    else:
                        console.print("\n[dim]No log entries found[/dim]")
                        
                except Exception as e:
                    console.print(f"\n[yellow]Could not load log entries: {e}[/yellow]")
        
        except Exception as e:
            console.print(f"[red]Error getting operation details:[/red] {e}")
            sys.exit(1)
    
    asyncio.run(_show_operation())


@progress.command()
@click.argument('operation_id')
@click.argument('command', type=click.Choice(['start', 'pause', 'resume', 'cancel']))
@click.option('--reason', help='Reason for the command (for cancel)')
def control(operation_id: str, command: str, reason: Optional[str]):
    """Control an operation (start, pause, resume, cancel)"""
    
    async def _control_operation():
        service_container = get_service_container()
        progress_service = service_container.progress_tracking_service
        
        try:
            control_dto = OperationControlDto(
                operation_id=operation_id,
                command=command,
                reason=reason
            )
            
            result = await progress_service.control_operation(control_dto)
            
            if result.success:
                console.print(f"[green]✅ {result.message}[/green]")
                console.print(f"Status changed from [yellow]{result.previous_status}[/yellow] to [green]{result.new_status}[/green]")
            else:
                console.print(f"[red]❌ {result.message}[/red]")
                sys.exit(1)
                
        except Exception as e:
            console.print(f"[red]Error controlling operation:[/red] {e}")
            sys.exit(1)
    
    asyncio.run(_control_operation())


@progress.command()
@click.argument('operation_id')
@click.option('--follow', '-f', is_flag=True, help='Follow progress updates in real-time')
@click.option('--interval', type=float, default=1.0, help='Update interval in seconds')
def monitor(operation_id: str, follow: bool, interval: float):
    """Monitor an operation's progress in real-time"""
    
    async def _monitor_operation():
        service_container = get_service_container()
        progress_service = service_container.progress_tracking_service
        
        if not follow:
            # Single status check
            try:
                operation = await progress_service.get_operation(operation_id)
                if not operation:
                    console.print(f"[red]Operation {operation_id} not found[/red]")
                    sys.exit(1)
                
                console.print(f"Operation: {operation.operation_name}")
                console.print(f"Status: {operation.status}")
                console.print(f"Progress: {operation.current_metrics.percentage:.1f}%")
                
            except Exception as e:
                console.print(f"[red]Error monitoring operation:[/red] {e}")
                sys.exit(1)
        else:
            # Real-time monitoring
            last_update = 0
            
            def signal_handler(sig, frame):
                console.print("\n[yellow]Monitoring stopped[/yellow]")
                sys.exit(0)
            
            signal.signal(signal.SIGINT, signal_handler)
            
            with Progress(
                SpinnerColumn(),
                TextColumn("[bold blue]{task.description}"),
                BarColumn(),
                TaskProgressColumn(),
                TimeElapsedColumn(),
                console=console
            ) as progress_bar:
                
                task = progress_bar.add_task("Loading...", total=100)
                
                while True:
                    try:
                        operation = await progress_service.get_operation(operation_id)
                        if not operation:
                            console.print(f"[red]Operation {operation_id} not found[/red]")
                            break
                        
                        # Update progress bar
                        progress_bar.update(
                            task,
                            description=f"{operation.operation_name} ({operation.status})",
                            completed=operation.current_metrics.percentage
                        )
                        
                        # Check if operation is complete
                        if operation.status in ['completed', 'failed', 'cancelled']:
                            break
                        
                        # Check for updates
                        if operation.last_update_time > last_update:
                            last_update = operation.last_update_time
                        
                        await asyncio.sleep(interval)
                        
                    except KeyboardInterrupt:
                        console.print("\n[yellow]Monitoring stopped[/yellow]")
                        break
                    except Exception as e:
                        console.print(f"\n[red]Error monitoring operation:[/red] {e}")
                        break
    
    asyncio.run(_monitor_operation())


@progress.command()
@click.option('--json-output', is_flag=True, help='Output in JSON format')
def summary(json_output: bool):
    """Show progress tracking summary statistics"""
    
    async def _show_summary():
        service_container = get_service_container()
        progress_service = service_container.progress_tracking_service
        
        try:
            summary_data = await progress_service.get_summary()
            
            if json_output:
                console.print(json.dumps({
                    'total_operations': summary_data.total_operations,
                    'active_operations': summary_data.active_operations,
                    'completed_operations': summary_data.completed_operations,
                    'failed_operations': summary_data.failed_operations,
                    'cancelled_operations': summary_data.cancelled_operations,
                    'operations_by_type': summary_data.operations_by_type,
                    'operations_by_status': summary_data.operations_by_status,
                    'total_bytes_processed': summary_data.total_bytes_processed,
                    'average_completion_time': summary_data.average_completion_time
                }, indent=2))
                return
            
            # Create summary display
            table = Table(title="Progress Tracking Summary")
            table.add_column("Metric", style="cyan")
            table.add_column("Value", style="green")
            
            table.add_row("Total Operations", str(summary_data.total_operations))
            table.add_row("Active Operations", str(summary_data.active_operations))
            table.add_row("Completed Operations", str(summary_data.completed_operations))
            table.add_row("Failed Operations", str(summary_data.failed_operations))
            table.add_row("Cancelled Operations", str(summary_data.cancelled_operations))
            
            # Format bytes processed
            bytes_mb = summary_data.total_bytes_processed / (1024 * 1024)
            if bytes_mb > 1024:
                bytes_str = f"{bytes_mb / 1024:.1f} GB"
            else:
                bytes_str = f"{bytes_mb:.1f} MB"
            table.add_row("Total Data Processed", bytes_str)
            
            # Format average completion time
            if summary_data.average_completion_time:
                if summary_data.average_completion_time < 60:
                    avg_time = f"{summary_data.average_completion_time:.1f} seconds"
                elif summary_data.average_completion_time < 3600:
                    avg_time = f"{summary_data.average_completion_time/60:.1f} minutes"
                else:
                    avg_time = f"{summary_data.average_completion_time/3600:.1f} hours"
                table.add_row("Average Completion Time", avg_time)
            
            console.print(table)
            
            # Show operations by type
            if summary_data.operations_by_type:
                console.print("\n[bold]Operations by Type:[/bold]")
                for op_type, count in summary_data.operations_by_type.items():
                    console.print(f"  {op_type}: {count}")
                    
        except Exception as e:
            console.print(f"[red]Error getting summary:[/red] {e}")
            sys.exit(1)
    
    asyncio.run(_show_summary())


@progress.command()
@click.option('--older-than-hours', type=float, default=24.0, help='Clean up operations older than this many hours')
@click.option('--preserve-failed', is_flag=True, default=True, help='Preserve failed operations from cleanup')
@click.option('--dry-run', is_flag=True, help='Show what would be cleaned up without actually doing it')
def cleanup(older_than_hours: float, preserve_failed: bool, dry_run: bool):
    """Clean up completed operations older than specified time"""
    
    async def _cleanup_operations():
        service_container = get_service_container()
        progress_service = service_container.progress_tracking_service
        
        try:
            older_than_seconds = older_than_hours * 3600
            
            # Access repository directly for cleanup
            repo = service_container.service_factory._progress_tracking_service._repository
            
            if dry_run:
                console.print(f"[yellow]Dry run: would clean up operations older than {older_than_hours} hours[/yellow]")
                # For dry run, we'd need to implement a preview method
                console.print("[dim]Dry run preview not implemented - use with caution[/dim]")
                return
            
            count = await repo.cleanup_completed_operations(older_than_seconds, preserve_failed)
            
            if count > 0:
                console.print(f"[green]✅ Cleaned up {count} completed operations[/green]")
            else:
                console.print("[blue]No operations needed cleanup[/blue]")
                
        except Exception as e:
            console.print(f"[red]Error cleaning up operations:[/red] {e}")
            sys.exit(1)
    
    asyncio.run(_cleanup_operations())


if __name__ == "__main__":
    progress()