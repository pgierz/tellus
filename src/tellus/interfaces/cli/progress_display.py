"""
Enhanced progress display utilities for CLI operations.

Provides real-time progress bars, throughput displays, and operation monitoring
for file transfers, archive operations, and other long-running tasks.
"""

import asyncio
import time
from contextlib import asynccontextmanager
from typing import Dict, Any, Optional, Callable, AsyncGenerator
from rich.progress import (
    Progress, 
    SpinnerColumn, 
    TextColumn, 
    BarColumn, 
    TaskProgressColumn,
    TimeRemainingColumn,
    TransferSpeedColumn,
    ProgressColumn,
    Task
)
from rich.console import Console
from rich.live import Live
from rich.table import Table
from rich.panel import Panel
from rich.layout import Layout
from rich.text import Text

from ...application.container import get_service_container


class OperationProgressDisplay:
    """
    Enhanced progress display for long-running operations.
    
    Provides real-time progress tracking with throughput metrics,
    ETA calculations, and operation status updates.
    """
    
    def __init__(self, console: Console):
        self.console = console
        self.progress = Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(complete_style="green", finished_style="bright_green"),
            TaskProgressColumn(),
            TransferSpeedColumn(),
            TimeRemainingColumn(),
            console=console,
            expand=True
        )
        self._tasks: Dict[str, int] = {}
        self._operation_data: Dict[str, Dict[str, Any]] = {}
        
    def create_task(
        self, 
        operation_id: str, 
        description: str, 
        total: Optional[int] = None,
        **kwargs
    ) -> int:
        """Create a new progress task."""
        task_id = self.progress.add_task(description, total=total, **kwargs)
        self._tasks[operation_id] = task_id
        self._operation_data[operation_id] = {
            'start_time': time.time(),
            'description': description,
            'total': total
        }
        return task_id
    
    def update_task(
        self, 
        operation_id: str, 
        completed: Optional[int] = None,
        description: Optional[str] = None,
        **kwargs
    ):
        """Update progress task."""
        if operation_id in self._tasks:
            task_id = self._tasks[operation_id]
            
            # Update operation data
            if completed is not None:
                self._operation_data[operation_id]['completed'] = completed
            if description is not None:
                self._operation_data[operation_id]['description'] = description
            
            self.progress.update(task_id, completed=completed, description=description, **kwargs)
    
    def complete_task(self, operation_id: str, final_message: Optional[str] = None):
        """Mark task as completed."""
        if operation_id in self._tasks:
            task_id = self._tasks[operation_id]
            if final_message:
                self.progress.update(task_id, description=final_message)
            self.progress.update(task_id, completed=True)
    
    def fail_task(self, operation_id: str, error_message: str):
        """Mark task as failed."""
        if operation_id in self._tasks:
            task_id = self._tasks[operation_id]
            self.progress.update(
                task_id, 
                description=f"[red]✗[/red] {error_message}",
                completed=True
            )
    
    def get_operation_stats(self, operation_id: str) -> Dict[str, Any]:
        """Get operation statistics."""
        if operation_id not in self._operation_data:
            return {}
        
        data = self._operation_data[operation_id]
        current_time = time.time()
        elapsed = current_time - data['start_time']
        
        stats = {
            'elapsed_seconds': elapsed,
            'start_time': data['start_time'],
        }
        
        if 'completed' in data and 'total' in data and data['total']:
            progress_pct = (data['completed'] / data['total']) * 100
            stats['progress_percentage'] = progress_pct
            
            if progress_pct > 0:
                eta = (elapsed / progress_pct) * (100 - progress_pct)
                stats['eta_seconds'] = eta
        
        return stats


class BatchProgressDisplay:
    """Enhanced progress display for batch operations with multiple files/archives."""
    
    def __init__(self, console: Console):
        self.console = console
        self.layout = Layout()
        self._setup_layout()
        
        self.overall_progress = Progress(
            TextColumn("[bold blue]Overall Progress"),
            BarColumn(complete_style="green"),
            TaskProgressColumn(),
            console=console
        )
        
        self.file_progress = Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            TransferSpeedColumn(),
            console=console
        )
        
        self._overall_task = None
        self._current_file_task = None
        self._stats = {
            'total_files': 0,
            'completed_files': 0,
            'failed_files': 0,
            'total_bytes': 0,
            'transferred_bytes': 0,
            'start_time': time.time()
        }
    
    def _setup_layout(self):
        """Setup the layout for batch progress display."""
        self.layout.split_column(
            Layout(name="overall", size=3),
            Layout(name="current", size=3),
            Layout(name="stats", size=6)
        )
    
    def start_batch(self, total_files: int, total_bytes: int = 0):
        """Start batch operation tracking."""
        self._stats.update({
            'total_files': total_files,
            'total_bytes': total_bytes,
            'start_time': time.time()
        })
        
        self._overall_task = self.overall_progress.add_task(
            f"Processing {total_files} files", 
            total=total_files
        )
    
    def start_file(self, filename: str, file_size: int = 0):
        """Start tracking individual file transfer."""
        if self._current_file_task is not None:
            self.file_progress.remove_task(self._current_file_task)
        
        description = f"Transferring {filename}"
        if file_size > 0:
            description += f" ({self._format_bytes(file_size)})"
        
        self._current_file_task = self.file_progress.add_task(
            description,
            total=file_size if file_size > 0 else None
        )
    
    def update_file_progress(self, bytes_transferred: int):
        """Update current file transfer progress."""
        if self._current_file_task is not None:
            self.file_progress.update(self._current_file_task, completed=bytes_transferred)
        
        # Update overall bytes transferred
        self._stats['transferred_bytes'] += bytes_transferred
    
    def complete_file(self, success: bool = True):
        """Mark current file as completed."""
        if self._current_file_task is not None:
            if success:
                self._stats['completed_files'] += 1
                self.file_progress.update(
                    self._current_file_task, 
                    description="[green]✓[/green] Transfer completed"
                )
            else:
                self._stats['failed_files'] += 1
                self.file_progress.update(
                    self._current_file_task,
                    description="[red]✗[/red] Transfer failed"
                )
        
        # Update overall progress
        if self._overall_task is not None:
            self.overall_progress.update(
                self._overall_task, 
                completed=self._stats['completed_files'] + self._stats['failed_files']
            )
    
    def get_stats_table(self) -> Table:
        """Generate statistics table for display."""
        table = Table(show_header=False, box=None)
        table.add_column("Metric", style="cyan")
        table.add_column("Value", style="white")
        
        elapsed = time.time() - self._stats['start_time']
        
        table.add_row("Files completed", f"{self._stats['completed_files']}")
        table.add_row("Files failed", f"{self._stats['failed_files']}")
        table.add_row("Total files", f"{self._stats['total_files']}")
        
        if self._stats['transferred_bytes'] > 0:
            table.add_row("Data transferred", self._format_bytes(self._stats['transferred_bytes']))
            if elapsed > 0:
                throughput = self._stats['transferred_bytes'] / elapsed
                table.add_row("Average speed", f"{self._format_bytes(throughput)}/s")
        
        table.add_row("Elapsed time", f"{elapsed:.1f}s")
        
        return table
    
    def _format_bytes(self, bytes_count: int) -> str:
        """Format bytes in human-readable format."""
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if bytes_count < 1024.0:
                return f"{bytes_count:.1f} {unit}"
            bytes_count /= 1024.0
        return f"{bytes_count:.1f} PB"
    
    def render(self):
        """Render the complete batch progress display."""
        self.layout["overall"].update(Panel(self.overall_progress, title="Overall Progress"))
        self.layout["current"].update(Panel(self.file_progress, title="Current File"))
        self.layout["stats"].update(Panel(self.get_stats_table(), title="Statistics"))
        return self.layout


class QueueMonitorDisplay:
    """Real-time queue monitoring display."""
    
    def __init__(self, console: Console):
        self.console = console
        self._queue_service = None
    
    def _get_queue_service(self):
        """Get queue service lazily."""
        if self._queue_service is None:
            service_container = get_service_container()
            self._queue_service = service_container.service_factory.operation_queue_service
        return self._queue_service
    
    def create_queue_table(self) -> Table:
        """Create real-time queue status table."""
        table = Table(title="Operation Queue Status", show_header=True)
        table.add_column("ID", style="cyan", width=12)
        table.add_column("Type", style="blue")
        table.add_column("Status", style="green") 
        table.add_column("Progress", style="white")
        table.add_column("Speed", style="yellow")
        
        try:
            queue_service = self._get_queue_service()
            operations = queue_service.list_operations()
            
            for op in operations[:10]:  # Show latest 10 operations
                operation_type = getattr(op.operation_dto, 'operation_type', 'unknown')
                
                # Format status with colors
                status_text = op.status.value
                if op.status.value == "completed":
                    status_text = f"[green]{status_text}[/green]"
                elif op.status.value == "failed":
                    status_text = f"[red]{status_text}[/red]"
                elif op.status.value == "running":
                    status_text = f"[yellow]{status_text}[/yellow]"
                
                # Format progress
                progress_text = ""
                speed_text = ""
                if op.result:
                    if hasattr(op.result, 'bytes_transferred') and op.result.bytes_transferred > 0:
                        progress_text = self._format_bytes(op.result.bytes_transferred)
                        if hasattr(op.result, 'throughput_mbps'):
                            speed_text = f"{op.result.throughput_mbps:.1f} MB/s"
                    elif hasattr(op.result, 'total_bytes_transferred'):
                        progress_text = self._format_bytes(op.result.total_bytes_transferred)
                
                table.add_row(
                    op.id[:12],
                    operation_type,
                    status_text,
                    progress_text,
                    speed_text
                )
        
        except Exception as e:
            table.add_row("Error", str(e), "", "", "")
        
        return table
    
    def create_stats_panel(self) -> Panel:
        """Create queue statistics panel."""
        try:
            queue_service = self._get_queue_service()
            stats = queue_service.get_queue_stats()
            
            stats_text = Text()
            stats_text.append(f"Total: {stats['total_operations']} | ", style="white")
            stats_text.append(f"Running: {stats['running']} | ", style="yellow")
            stats_text.append(f"Queued: {stats['queued']} | ", style="blue")
            stats_text.append(f"Completed: {stats['completed']} | ", style="green")
            stats_text.append(f"Failed: {stats['failed']}", style="red")
            
            if stats['total_bytes_processed'] > 0:
                stats_text.append(f"\nTotal processed: {self._format_bytes(stats['total_bytes_processed'])}")
            
            return Panel(stats_text, title="Queue Statistics")
            
        except Exception as e:
            return Panel(f"Error: {e}", title="Queue Statistics")
    
    def _format_bytes(self, bytes_count: int) -> str:
        """Format bytes in human-readable format."""
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if bytes_count < 1024.0:
                return f"{bytes_count:.1f} {unit}"
            bytes_count /= 1024.0
        return f"{bytes_count:.1f} PB"


@asynccontextmanager
async def progress_display(console: Console) -> AsyncGenerator[OperationProgressDisplay, None]:
    """Context manager for progress display."""
    display = OperationProgressDisplay(console)
    try:
        with display.progress:
            yield display
    finally:
        # Cleanup if needed
        pass


@asynccontextmanager 
async def batch_progress_display(console: Console) -> AsyncGenerator[BatchProgressDisplay, None]:
    """Context manager for batch progress display."""
    display = BatchProgressDisplay(console)
    try:
        with Live(display.render(), console=console, refresh_per_second=4) as live:
            display._live = live
            yield display
    finally:
        # Cleanup if needed
        pass


async def monitor_queue_realtime(console: Console, duration: int = 30):
    """Monitor queue in real-time for specified duration."""
    monitor = QueueMonitorDisplay(console)
    
    with Live(console=console, refresh_per_second=2) as live:
        start_time = time.time()
        
        while time.time() - start_time < duration:
            layout = Layout()
            layout.split_column(
                Layout(monitor.create_stats_panel(), size=4),
                Layout(Panel(monitor.create_queue_table(), title="Active Operations"))
            )
            
            live.update(layout)
            await asyncio.sleep(0.5)
    
    console.print(f"[dim]Queue monitoring completed after {duration}s[/dim]")


# Progress callback function for queue operations
def create_progress_callback(display: OperationProgressDisplay, operation_id: str) -> Callable[[str, Dict[str, Any]], None]:
    """Create a progress callback function for queue operations."""
    
    def callback(callback_operation_id: str, data: Dict[str, Any]):
        """Progress callback function."""
        if callback_operation_id != operation_id:
            return
        
        status = data.get('status', '')
        if status == 'started':
            operation_type = data.get('operation_type', 'operation')
            count = data.get('archive_count', data.get('transfer_count', 1))
            display.create_task(
                operation_id, 
                f"Starting {operation_type} ({count} items)",
                total=count
            )
        elif status in ['completed', 'failed']:
            if status == 'completed':
                display.complete_task(operation_id, "✓ Operation completed")
            else:
                error = data.get('error', 'Unknown error')
                display.fail_task(operation_id, f"Operation failed: {error}")
    
    return callback