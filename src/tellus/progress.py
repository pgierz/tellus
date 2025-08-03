"""Progress tracking utilities for Tellus.

This module provides utilities for tracking and displaying progress of file operations
using rich progress bars and fsspec callbacks.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, Optional, Union

from fsspec.callbacks import Callback
from rich.console import Console
from rich.progress import (
    BarColumn,
    DownloadColumn,
    Progress,
    TaskID,
    TextColumn,
    TimeRemainingColumn,
    TransferSpeedColumn,
)
from rich.text import Text


class FSSpecProgressCallback(Callback):
    """A callback class that bridges fsspec's callback system with rich progress bars.

    This class can be used with any fsspec filesystem to display progress bars for
    file operations like uploads and downloads.
    """

    def __init__(
        self,
        description: str = "Processing",
        size: Optional[int] = None,
        value: int = 0,
        enable: bool = True,
        progress: Optional[Progress] = None,
        **kwargs: Any,
    ) -> None:
        """Initialize the callback.

        Args:
            description: Description of the operation
            size: Total size in bytes (if known)
            value: Initial value (bytes processed)
            enable: Whether to enable progress reporting
            progress: Optional rich Progress instance to use
            **kwargs: Additional arguments passed to the base class
        """
        super().__init__(size=size, value=value, enable=enable, **kwargs)
        self.description = description
        self._progress = progress or get_default_progress()
        self._task_id: Optional[TaskID] = None
        self._start_time = time.monotonic()

        # Start the progress bar if we have a size
        if self.size is not None and self.size > 0:
            self._task_id = self._progress.add_task(
                description,
                total=self.size,
                start=value > 0,
                completed=value,
            )

        # Start the progress display if we're the root callback
        if progress is None:
            self._progress.start()

    def __enter__(self) -> "FSSpecProgressCallback":
        """Context manager entry."""
        return self

    def __exit__(self, *args: Any) -> None:
        """Context manager exit - ensure progress is stopped."""
        self.close()

    def close(self) -> None:
        """Close the progress bar and clean up."""
        if self._task_id is not None and self._progress is not None:
            self._progress.update(self._task_id, visible=False)
            self._progress.stop_task(self._task_id)
            self._task_id = None

    def set_description(self, description: str) -> None:
        """Update the description of the progress bar.

        Args:
            description: New description text
        """
        self.description = description
        if self._task_id is not None and self._progress is not None:
            self._progress.update(self._task_id, description=description)

    def relative_update(self, inc: int = 1) -> None:
        """Increment the progress by a relative amount.

        Args:
            inc: Amount to increment by
        """
        if self._task_id is not None and self._progress is not None:
            self._progress.update(self._task_id, advance=inc)

    def branch(
        self, path_1: str, path_2: str, kwargs: Dict[str, Any]
    ) -> "FSSpecProgressCallback":
        """Create a new callback for a branch operation.

        This is used for operations that involve multiple files or chunks.

        Args:
            path_1: Source path
            path_2: Destination path
            kwargs: Additional arguments for the new callback

        Returns:
            A new callback instance
        """
        desc = f"{Path(path_1).name} → {Path(path_2).name}"
        return FSSpecProgressCallback(
            description=desc, progress=self._progress, **kwargs
        )

    def call(self, *args: Any, **kwargs: Any) -> None:
        """Handle a callback from fsspec.

        This is called by fsspec with information about the current operation.
        """
        super().call(*args, **kwargs)

        # Update the progress bar if we have one
        if self._task_id is not None and self._progress is not None:
            self._progress.update(
                self._task_id,
                completed=self.value,
                total=self.size or 0,
                refresh=True,
            )


@dataclass
class ProgressConfig:
    """Configuration for progress display."""

    # Whether to show progress bars
    enabled: bool = True

    # Whether to show transfer speed
    show_speed: bool = True

    # Whether to show time remaining
    show_eta: bool = True

    # Whether to show a progress bar
    show_bar: bool = True

    # Whether to show the current value
    show_value: bool = True

    # Whether to show the total
    show_total: bool = True


# Global console instance
_console = Console()

# Global progress configuration
_progress_config = ProgressConfig()


def get_default_progress() -> Progress:
    """Get a default Progress instance with standard columns.

    Returns:
        A configured Progress instance
    """
    columns = [
        TextColumn("[progress.description]{task.description}"),
    ]

    if _progress_config.show_bar:
        columns.append(BarColumn(bar_width=None))

    if _progress_config.show_value:
        columns.append(TextColumn("[progress.percentage]{task.percentage:>4.0f}%"))

    if _progress_config.show_speed:
        columns.append(TransferSpeedColumn())

    if _progress_config.show_eta:
        columns.append(TimeRemainingColumn())

    if _progress_config.show_total:
        columns.append(DownloadColumn())

    return Progress(
        *columns,
        console=_console,
        refresh_per_second=10,
        expand=True,
    )


def set_progress_config(**kwargs: Any) -> None:
    """Update the progress display configuration.

    Args:
        **kwargs: Configuration options to update
    """
    global _progress_config
    _progress_config = ProgressConfig(**{**vars(_progress_config), **kwargs})


def get_progress_callback(
    description: str = "Processing",
    size: Optional[int] = None,
    **kwargs: Any,
) -> FSSpecProgressCallback:
    """Create a new progress callback.

    Args:
        description: Description of the operation
        size: Total size in bytes (if known)
        **kwargs: Additional arguments for FSSpecProgressCallback

    Returns:
        A new progress callback instance
    """
    return FSSpecProgressCallback(description=description, size=size, **kwargs)


# For backward compatibility
ProgressCallback = FSSpecProgressCallback
