"""
Enhanced progress display utilities for CLI operations.

Provides real-time progress bars, throughput displays, and operation monitoring
for file transfers, archive operations, and other long-running tasks.
"""

import asyncio
import time
from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator, Callable, Dict, Optional

from rich.console import Console
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.progress import (BarColumn, Progress, ProgressColumn, SpinnerColumn,
                           Task, TaskProgressColumn, TextColumn,
                           TimeRemainingColumn, TransferSpeedColumn)
from rich.table import Table
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


