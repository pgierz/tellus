"""File transfer CLI commands with progress tracking and queue integration."""

import asyncio
import rich_click as click
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn
from pathlib import Path
from typing import Optional, List

from .main import cli, console
from .progress_display import (
    progress_display,
    batch_progress_display,
    OperationProgressDisplay,
    BatchProgressDisplay
)
from ...application.container import get_service_container
from ...application.dtos import (
    FileTransferOperationDto,
    BatchFileTransferOperationDto,
    DirectoryTransferOperationDto,
)
from ...application.services.bulk_operation_queue import QueuePriority


def _get_transfer_service():
    """Get file transfer service from the service container."""
    service_container = get_service_container()
    return service_container.service_factory.file_transfer_service


def _get_queue_service():
    """Get operation queue service from the service container."""
    service_container = get_service_container()
    return service_container.service_factory.operation_queue_service


@cli.group()
def transfer():
    """Transfer files between locations with progress tracking."""
    pass


@transfer.command(name="file")
@click.argument("source_path")
@click.argument("dest_path")
@click.option("--source-location", default="local", help="Source location name")
@click.option("--dest-location", required=True, help="Destination location name")
@click.option("--overwrite", is_flag=True, help="Overwrite destination if exists")
@click.option("--no-verify", is_flag=True, help="Skip checksum verification")
@click.option("--chunk-size", default=8388608, help="Transfer chunk size in bytes (default: 8MB)")
def transfer_file(
    source_path: str, 
    dest_path: str,
    source_location: str,
    dest_location: str,
    overwrite: bool,
    no_verify: bool,
    chunk_size: int
):
    """Transfer a single file between locations."""
    
    async def _transfer():
        try:
            service = _get_transfer_service()
            
            console.print(f"📁 Transferring file: [cyan]{source_location}:{source_path}[/cyan] → [green]{dest_location}:{dest_path}[/green]")
            
            dto = FileTransferOperationDto(
                source_location=source_location,
                source_path=source_path,
                dest_location=dest_location,
                dest_path=dest_path,
                overwrite=overwrite,
                verify_checksum=not no_verify,
                chunk_size=chunk_size
            )
            
            # Show enhanced progress while transferring
            async with progress_display(console) as display:
                operation_id = f"transfer_{int(__import__('time').time())}"
                task_id = display.create_task(
                    operation_id,
                    f"Transferring {Path(source_path).name}",
                    total=None
                )
                
                result = await service.transfer_file(dto)
                
                if result.success:
                    display.complete_task(operation_id, f"✓ Transfer completed ({result.bytes_transferred:,} bytes)")
                else:
                    display.fail_task(operation_id, f"Transfer failed: {result.error_message}")
            
            if result.success:
                console.print(f"[green]✓[/green] Transfer completed successfully!")
                console.print(f"   Bytes transferred: [yellow]{result.bytes_transferred:,}[/yellow]")
                console.print(f"   Duration: [yellow]{result.duration_seconds:.2f}s[/yellow]")
                console.print(f"   Throughput: [yellow]{result.throughput_mbps:.2f} MB/s[/yellow]")
                if result.checksum_verified:
                    console.print(f"   [green]✓[/green] Checksum verified")
            else:
                console.print(f"[red]✗[/red] Transfer failed: {result.error_message}")
                
        except Exception as e:
            console.print(f"[red]Error:[/red] {str(e)}")
    
    try:
        asyncio.run(_transfer())
    except KeyboardInterrupt:
        console.print("\n[yellow]Transfer cancelled by user[/yellow]")


@transfer.command(name="batch")
@click.argument("file_list", nargs=-1, required=True)
@click.option("--source-location", default="local", help="Source location name")
@click.option("--dest-location", required=True, help="Destination location name")
@click.option("--dest-dir", help="Destination directory (files keep same names)")
@click.option("--parallel", default=3, help="Number of parallel transfers")
@click.option("--overwrite", is_flag=True, help="Overwrite destination files if they exist")
@click.option("--no-verify", is_flag=True, help="Skip checksum verification")
@click.option("--stop-on-error", is_flag=True, help="Stop batch if any transfer fails")
def transfer_batch(
    file_list: tuple,
    source_location: str,
    dest_location: str,
    dest_dir: Optional[str],
    parallel: int,
    overwrite: bool,
    no_verify: bool,
    stop_on_error: bool
):
    """Transfer multiple files in a batch with parallel processing."""
    
    async def _transfer_batch():
        try:
            service = _get_transfer_service()
            
            console.print(f"📦 Starting batch transfer of {len(file_list)} files")
            console.print(f"   Source: [cyan]{source_location}[/cyan]")
            console.print(f"   Destination: [green]{dest_location}[/green]")
            console.print(f"   Parallel transfers: [yellow]{parallel}[/yellow]")
            
            # Build transfer list
            transfers = []
            for source_file in file_list:
                if dest_dir:
                    # Files go to dest_dir with same names
                    dest_file = f"{dest_dir}/{Path(source_file).name}"
                else:
                    # Assume file_list has pairs: source1 dest1 source2 dest2 ...
                    if len(file_list) % 2 != 0:
                        console.print("[red]Error:[/red] For batch without --dest-dir, provide source dest pairs")
                        return
                    # This is a simplified approach - could be enhanced
                    dest_file = source_file
                
                transfer_dto = FileTransferOperationDto(
                    source_location=source_location,
                    source_path=source_file,
                    dest_location=dest_location,
                    dest_path=dest_file,
                    overwrite=overwrite,
                    verify_checksum=not no_verify
                )
                transfers.append(transfer_dto)
            
            batch_dto = BatchFileTransferOperationDto(
                transfers=transfers,
                parallel_transfers=parallel,
                stop_on_error=stop_on_error,
                verify_all_checksums=not no_verify
            )
            
            # Execute batch transfer with enhanced progress
            async with batch_progress_display(console) as display:
                display.start_batch(len(transfers), total_bytes=0)  # Could estimate total bytes
                
                # Monitor individual file progress during batch transfer
                # This is a simplified version - full integration would require
                # callback support in the batch transfer service
                for i, transfer in enumerate(transfers):
                    display.start_file(Path(transfer.source_path).name, file_size=0)
                    
                result = await service.batch_transfer_files(batch_dto)
                
                # Update final stats
                for _ in range(len(result.successful_transfers)):
                    display.complete_file(success=True)
                for _ in range(len(result.failed_transfers)):
                    display.complete_file(success=False)
            
            # Show results
            console.print(f"\n[green]✓[/green] Batch transfer completed!")
            console.print(f"   Successful: [green]{len(result.successful_transfers)}[/green]")
            console.print(f"   Failed: [red]{len(result.failed_transfers)}[/red]")
            console.print(f"   Total bytes: [yellow]{result.total_bytes_transferred:,}[/yellow]")
            console.print(f"   Duration: [yellow]{result.total_duration_seconds:.2f}s[/yellow]")
            console.print(f"   Average throughput: [yellow]{result.average_throughput_mbps:.2f} MB/s[/yellow]")
            
            if result.failed_transfers:
                console.print("\n[red]Failed transfers:[/red]")
                for failed in result.failed_transfers:
                    console.print(f"   [red]✗[/red] {failed.source_path} → {failed.dest_path}: {failed.error_message}")
                    
        except Exception as e:
            console.print(f"[red]Error:[/red] {str(e)}")
    
    try:
        asyncio.run(_transfer_batch())
    except KeyboardInterrupt:
        console.print("\n[yellow]Batch transfer cancelled by user[/yellow]")


@transfer.command(name="directory") 
@click.argument("source_dir")
@click.argument("dest_dir")
@click.option("--source-location", default="local", help="Source location name")
@click.option("--dest-location", required=True, help="Destination location name")
@click.option("--include", multiple=True, help="Include patterns (glob)")
@click.option("--exclude", multiple=True, help="Exclude patterns (glob)")
@click.option("--overwrite", is_flag=True, help="Overwrite destination files")
@click.option("--no-verify", is_flag=True, help="Skip checksum verification")
def transfer_directory(
    source_dir: str,
    dest_dir: str,
    source_location: str,
    dest_location: str,
    include: tuple,
    exclude: tuple,
    overwrite: bool,
    no_verify: bool
):
    """Transfer a directory recursively with optional filtering."""
    
    async def _transfer_dir():
        try:
            service = _get_transfer_service()
            
            console.print(f"📂 Transferring directory: [cyan]{source_location}:{source_dir}[/cyan] → [green]{dest_location}:{dest_dir}[/green]")
            if include:
                console.print(f"   Include patterns: [blue]{', '.join(include)}[/blue]")
            if exclude:
                console.print(f"   Exclude patterns: [red]{', '.join(exclude)}[/red]")
            
            dto = DirectoryTransferOperationDto(
                source_location=source_location,
                source_path=source_dir,
                dest_location=dest_location,
                dest_path=dest_dir,
                recursive=True,
                overwrite=overwrite,
                verify_checksums=not no_verify,
                include_patterns=list(include),
                exclude_patterns=list(exclude)
            )
            
            # Use enhanced batch progress for directory transfers
            async with batch_progress_display(console) as display:
                # Directory transfers are essentially batch transfers
                display.start_batch(1, total_bytes=0)  # Will be updated when files are discovered
                
                result = await service.transfer_directory(dto)
                
                # Update progress based on results
                if hasattr(result, 'successful_transfers') and hasattr(result, 'failed_transfers'):
                    total_files = len(result.successful_transfers) + len(result.failed_transfers)
                    display.start_batch(total_files, total_bytes=result.total_bytes_transferred)
                    
                    for _ in range(len(result.successful_transfers)):
                        display.complete_file(success=True)
                    for _ in range(len(result.failed_transfers)):
                        display.complete_file(success=False)
            
            console.print(f"\n[green]✓[/green] Directory transfer completed!")
            console.print(f"   Files transferred: [green]{len(result.successful_transfers)}[/green]")
            console.print(f"   Failed transfers: [red]{len(result.failed_transfers)}[/red]")
            console.print(f"   Total bytes: [yellow]{result.total_bytes_transferred:,}[/yellow]")
            console.print(f"   Duration: [yellow]{result.total_duration_seconds:.2f}s[/yellow]")
            
            if result.failed_transfers:
                console.print(f"\n[red]Failed files:[/red]")
                for failed in result.failed_transfers[:5]:  # Show first 5
                    console.print(f"   [red]✗[/red] {Path(failed.source_path).name}: {failed.error_message}")
                if len(result.failed_transfers) > 5:
                    console.print(f"   ... and {len(result.failed_transfers) - 5} more")
                    
        except Exception as e:
            console.print(f"[red]Error:[/red] {str(e)}")
    
    try:
        asyncio.run(_transfer_dir())
    except KeyboardInterrupt:
        console.print("\n[yellow]Directory transfer cancelled by user[/yellow]")


@cli.group(name="queue")
def queue_group():
    """Manage the operation queue for file transfers and archives."""
    pass


@queue_group.command(name="list")
@click.option("--status", type=click.Choice(['queued', 'running', 'completed', 'failed', 'cancelled']), help="Filter by status")
@click.option("--user", help="Filter by user ID")
@click.option("--limit", default=20, help="Maximum number of operations to show")
def list_queue(status, user, limit):
    """List operations in the queue."""
    try:
        service = _get_queue_service()
        
        # Convert status string to enum if provided
        status_filter = None
        if status:
            from ...application.services.bulk_operation_queue import QueueStatus
            status_filter = QueueStatus(status.upper())
        
        operations = service.list_operations(
            status_filter=status_filter,
            user_filter=user
        )
        
        if not operations:
            console.print("No operations found.")
            return
        
        # Show up to limit operations
        operations = operations[:limit]
        
        table = Table(title="Operation Queue", show_header=True, header_style="bold magenta")
        table.add_column("ID", style="cyan", width=12)
        table.add_column("Type", style="blue")
        table.add_column("Status", style="green")
        table.add_column("Created", style="dim")
        table.add_column("Duration", style="yellow")
        table.add_column("Progress", style="white")
        
        for op in operations:
            operation_type = getattr(op.operation_dto, 'operation_type', 'unknown')
            
            # Format duration
            duration_str = ""
            if op.duration:
                duration_str = f"{op.duration:.1f}s"
            
            # Format status with color
            status_str = op.status.value
            if op.status.value == "completed":
                status_str = f"[green]{status_str}[/green]"
            elif op.status.value == "failed":
                status_str = f"[red]{status_str}[/red]"
            elif op.status.value == "running":
                status_str = f"[yellow]{status_str}[/yellow]"
            
            # Basic progress indication
            progress_str = ""
            if op.result:
                if hasattr(op.result, 'bytes_transferred'):
                    progress_str = f"{op.result.bytes_transferred:,} bytes"
                elif hasattr(op.result, 'total_bytes_transferred'):
                    progress_str = f"{op.result.total_bytes_transferred:,} bytes"
            
            table.add_row(
                op.id[:12],
                operation_type,
                status_str,
                f"{op.created_time:.0f}",
                duration_str,
                progress_str
            )
        
        console.print(table)
        
        # Show queue statistics
        stats = service.get_queue_stats()
        stats_panel = Panel(
            f"Queue: {stats['queue_length']} pending, {stats['running']} running, {stats['completed']} completed, {stats['failed']} failed",
            title="Queue Statistics"
        )
        console.print(stats_panel)
        
    except Exception as e:
        console.print(f"[red]Error:[/red] {str(e)}")


@queue_group.command(name="status")
@click.argument("operation_id")
def queue_status(operation_id: str):
    """Show detailed status of a specific operation."""
    try:
        service = _get_queue_service()
        operation = service.get_operation_status(operation_id)
        
        if not operation:
            console.print(f"[red]Operation not found:[/red] {operation_id}")
            return
        
        # Create detailed status display
        status_info = []
        status_info.append(f"ID: {operation.id}")
        status_info.append(f"Status: {operation.status.value}")
        status_info.append(f"Priority: {operation.priority.name}")
        status_info.append(f"Created: {operation.created_time}")
        
        if operation.started_time:
            status_info.append(f"Started: {operation.started_time}")
        if operation.completed_time:
            status_info.append(f"Completed: {operation.completed_time}")
        if operation.duration:
            status_info.append(f"Duration: {operation.duration:.2f}s")
        if operation.error_message:
            status_info.append(f"Error: {operation.error_message}")
        
        # Show operation details
        operation_type = getattr(operation.operation_dto, 'operation_type', 'unknown')
        status_info.append(f"Type: {operation_type}")
        
        if hasattr(operation.operation_dto, 'source_path'):
            status_info.append(f"Source: {operation.operation_dto.source_location}:{operation.operation_dto.source_path}")
            status_info.append(f"Destination: {operation.operation_dto.dest_location}:{operation.operation_dto.dest_path}")
        
        panel = Panel(
            "\n".join(status_info),
            title=f"Operation {operation_id[:12]}",
            border_style="cyan"
        )
        console.print(panel)
        
        # Show result details if available
        if operation.result:
            result_info = []
            if hasattr(operation.result, 'success'):
                result_info.append(f"Success: {operation.result.success}")
            if hasattr(operation.result, 'bytes_transferred'):
                result_info.append(f"Bytes transferred: {operation.result.bytes_transferred:,}")
            if hasattr(operation.result, 'throughput_mbps'):
                result_info.append(f"Throughput: {operation.result.throughput_mbps:.2f} MB/s")
            
            if result_info:
                result_panel = Panel(
                    "\n".join(result_info),
                    title="Result Details",
                    border_style="green"
                )
                console.print(result_panel)
        
    except Exception as e:
        console.print(f"[red]Error:[/red] {str(e)}")


@queue_group.command(name="cancel")
@click.argument("operation_id")
def cancel_operation(operation_id: str):
    """Cancel a queued or running operation."""
    try:
        service = _get_queue_service()
        
        if service.cancel_operation(operation_id):
            console.print(f"[green]✓[/green] Cancelled operation: {operation_id}")
        else:
            console.print(f"[red]✗[/red] Failed to cancel operation: {operation_id}")
            
    except Exception as e:
        console.print(f"[red]Error:[/red] {str(e)}")


@queue_group.command(name="pause")
def pause_queue():
    """Pause queue processing."""
    try:
        service = _get_queue_service()
        service.pause_queue()
        console.print("[yellow]⏸[/yellow] Queue processing paused")
        
    except Exception as e:
        console.print(f"[red]Error:[/red] {str(e)}")


@queue_group.command(name="resume")
def resume_queue():
    """Resume queue processing."""
    try:
        service = _get_queue_service()
        service.resume_queue()
        console.print("[green]▶[/green] Queue processing resumed")
        
    except Exception as e:
        console.print(f"[red]Error:[/red] {str(e)}")


@queue_group.command(name="clear")
def clear_completed():
    """Clear completed and failed operations from queue."""
    try:
        service = _get_queue_service()
        cleared = service.clear_completed()
        console.print(f"[green]✓[/green] Cleared {cleared} completed operations")
        
    except Exception as e:
        console.print(f"[red]Error:[/red] {str(e)}")


@queue_group.command(name="stats")
def show_stats():
    """Show detailed queue statistics."""
    try:
        service = _get_queue_service()
        stats = service.get_queue_stats()
        
        stats_info = []
        stats_info.append(f"Total operations: {stats['total_operations']}")
        stats_info.append(f"Queued: {stats['queued']}")
        stats_info.append(f"Running: {stats['running']}")
        stats_info.append(f"Completed: {stats['completed']}")
        stats_info.append(f"Failed: {stats['failed']}")
        stats_info.append(f"Cancelled: {stats['cancelled']}")
        stats_info.append(f"Processing: {'Yes' if stats['is_processing'] else 'No'}")
        stats_info.append(f"Paused: {'Yes' if stats['is_paused'] else 'No'}")
        stats_info.append(f"Max concurrent: {stats['max_concurrent']}")
        stats_info.append(f"Total bytes processed: {stats['total_bytes_processed']:,}")
        
        panel = Panel(
            "\n".join(stats_info),
            title="Queue Statistics",
            border_style="blue"
        )
        console.print(panel)
        
    except Exception as e:
        console.print(f"[red]Error:[/red] {str(e)}")


@queue_group.command(name="monitor")
@click.option("--duration", default=30, help="Monitor duration in seconds")
@click.option("--refresh", default=2, help="Refresh rate in seconds")
def monitor_queue(duration: int, refresh: int):
    """Monitor queue operations in real-time with enhanced displays."""
    async def _monitor():
        try:
            from .progress_display import monitor_queue_realtime
            console.print(f"[cyan]Monitoring queue for {duration} seconds (Ctrl+C to stop early)[/cyan]")
            await monitor_queue_realtime(console, duration)
        except KeyboardInterrupt:
            console.print("\n[yellow]Monitoring stopped by user[/yellow]")
        except Exception as e:
            console.print(f"[red]Error:[/red] {str(e)}")
    
    try:
        asyncio.run(_monitor())
    except KeyboardInterrupt:
        console.print("\n[yellow]Monitoring cancelled[/yellow]")