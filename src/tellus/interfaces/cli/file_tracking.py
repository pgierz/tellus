"""CLI commands for file tracking .."""

from pathlib import Path
from typing import Optional

import rich_click as click
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from ...application.dtos import (AddFilesDto, CreateFileTrackingRepositoryDto,
                                 CreateSnapshotDto)
from ...application.exceptions import (ApplicationError,
                                       EntityAlreadyExistsError,
                                       EntityNotFoundError)
from ...application.services.file_tracking_service import \
    FileTrackingApplicationService
from ...infrastructure.repositories.json_file_tracking_repository import \
    JsonFileTrackingRepository
from ...infrastructure.services.dvc_service import DVCService
from ...infrastructure.services.filesystem_service import FileSystemService

console = Console()


def get_file_tracking_service() -> FileTrackingApplicationService:
    """Get configured file tracking service."""
    repo = JsonFileTrackingRepository()
    fs_service = FileSystemService()
    dvc_service = DVCService() if DVCService().is_dvc_available() else None
    
    return FileTrackingApplicationService(repo, fs_service, dvc_service)


def find_repository_root() -> Optional[Path]:
    """Find the root of the current Tellus file tracking repository."""
    current = Path.cwd().resolve()
    
    while True:
        tellus_dir = current / ".tellus"
        if tellus_dir.is_dir():
            return current
        
        parent = current.parent
        if parent == current:  # Reached filesystem root
            return None
        current = parent


@click.group()
@click.pass_context
def files(ctx):
    """Git-like file tracking for simulation data with DVC integration.
    
    Examples:
        tellus files init                    # Initialize file tracking
        tellus files add *.nc               # Add NetCDF files
        tellus files status                 # Check repository status
        tellus files snapshot "Added data"  # Create snapshot
        tellus files log                    # View history
        
    Use --json flag for machine-readable output:
        tellus --json files status          # JSON status output
    """
    # Get JSON flag from parent context if available
    if ctx.parent and ctx.parent.obj and ctx.parent.obj.get('output_json'):
        ctx.ensure_object(dict)
        ctx.obj['output_json'] = True


@files.command()
@click.option(
    "--path", 
    "-p",
    type=click.Path(exists=True, file_okay=False, dir_okay=True, path_type=Path),
    default=".",
    help="Directory to initialize (default: current directory)"
)
@click.option(
    "--enable-dvc",
    is_flag=True,
    help="Enable DVC integration for large files"
)
@click.option(
    "--dvc-remote",
    help="DVC remote storage URL (e.g., s3://bucket/path, gs://bucket/path)"
)
@click.option(
    "--dvc-remote-name",
    default="storage",
    help="Name for the DVC remote (default: storage)"
)
@click.option(
    "--large-file-threshold",
    type=int,
    default=100 * 1024 * 1024,  # 100MB
    help="Size threshold for using DVC (bytes, default: 100MB)"
)
def init(
    path: Path, 
    enable_dvc: bool, 
    dvc_remote: Optional[str], 
    dvc_remote_name: str,
    large_file_threshold: int
):
    """Initialize a new file tracking repository with optional DVC integration."""
    try:
        service = get_file_tracking_service()
        
        dto = CreateFileTrackingRepositoryDto(
            root_path=str(path.resolve()),
            enable_dvc=enable_dvc,
            dvc_remote_name=dvc_remote_name if dvc_remote else None,
            dvc_remote_url=dvc_remote,
            large_file_threshold=large_file_threshold
        )
        
        repo_info = service.initialize_repository(dto)
        
        # Create success panel
        content = f"[green]✅ Initialized Tellus file tracking repository[/]\n"
        content += f"[bold]Location:[/] {repo_info.root_path}\n"
        
        if repo_info.dvc_enabled:
            content += f"[bold]DVC Integration:[/] [green]Enabled[/green]\n"
            if dvc_remote:
                content += f"[bold]DVC Remote:[/] {dvc_remote_name} → {dvc_remote}\n"
            content += f"[bold]Large File Threshold:[/] {large_file_threshold // (1024*1024)}MB\n"
        else:
            content += f"[bold]DVC Integration:[/] [yellow]Disabled[/yellow]\n"
        
        content += "\n[cyan]Next steps:[/]\n"
        content += "• [code]tellus files add <file>[/code] - Track files\n"
        content += "• [code]tellus files status[/code] - Check status\n"
        content += "• [code]tellus files snapshot[/code] - Create snapshots\n"
        
        console.print(Panel(
            content,
            title="[bold green]Repository Initialized[/]",
            border_style="green"
        ))
        
    except EntityAlreadyExistsError:
        console.print("[red]Error:[/red] Repository already exists in this directory")
        raise click.Abort()
    except ApplicationError as e:
        console.print(f"[red]Error:[/red] {e}")
        raise click.Abort()


@files.command()
@click.argument("files_to_add", nargs=-1, required=True)
@click.option(
    "--force", 
    "-f",
    is_flag=True,
    help="Force add files (ignore .tellusignore)"
)
@click.option(
    "--no-dvc",
    is_flag=True,
    help="Disable DVC for large files in this operation"
)
@click.pass_context
def add(ctx, files_to_add: tuple, force: bool, no_dvc: bool):
    """Add files to tracking.
    
    Stages files for the next snapshot, similar to 'git add'. Large files 
    (>50MB) are automatically handled with DVC if enabled.
    
    Examples:
        tellus files add data.nc                    # Add single file
        tellus files add *.nc *.txt                # Add multiple patterns  
        tellus files add output/ --force            # Force add directory
        tellus files add data.nc --no-dvc          # Skip DVC for large file
        
    See Also:
        tellus files status      # Check what will be included
        tellus files snapshot    # Create snapshot of staged files
    """
    output_json = ctx.obj.get('output_json', False) if ctx.obj else False
    repo_root = find_repository_root()
    if not repo_root:
        if output_json:
            import json
            console.print(json.dumps({"error": "Not in a Tellus repository", "suggestion": "Run 'tellus files init' to initialize"}, indent=2))
        else:
            console.print("[red]Error:[/red] Not in a Tellus repository")
            console.print("[dim]Tip:[/dim] Run [code]tellus files init[/code] to initialize")
        raise click.Abort()
    
    try:
        service = get_file_tracking_service()
        
        dto = AddFilesDto(
            file_paths=list(files_to_add),
            force_add=force,
            use_dvc_for_large_files=not no_dvc
        )
        
        status = service.add_files(str(repo_root), dto)
        
        # Display results
        if output_json:
            import json
            result = {
                "staged_files": list(status.staged_files),
                "untracked_files": list(status.untracked_files),
                "added_count": len(status.staged_files),
                "status": "success"
            }
            console.print(json.dumps(result, indent=2))
        else:
            if status.staged_files:
                console.print(f"[green]Added {len(status.staged_files)} files to tracking:[/]")
                for file_path in status.staged_files:
                    console.print(f"  [green]✓[/green] {file_path}")
            
            if status.untracked_files:
                console.print(f"\n[yellow]Untracked files ({len(status.untracked_files)} found):[/]")
                for file_path in status.untracked_files[:5]:  # Show first 5
                    console.print(f"  [yellow]?[/yellow] {file_path}")
                if len(status.untracked_files) > 5:
                    console.print(f"  ... and {len(status.untracked_files) - 5} more")
            
            console.print(f"\n[dim]Next step:[/dim] Run [code]tellus files snapshot \"Your message\"[/code] to create snapshot")
        
    except EntityNotFoundError:
        console.print("[red]Error:[/red] Repository not found")
        raise click.Abort()
    except ApplicationError as e:
        console.print(f"[red]Error:[/red] {e}")
        raise click.Abort()


@files.command()
@click.pass_context
def status(ctx):
    """Show repository status.
    
    Displays the current state of tracked files, similar to 'git status'.
    Shows staged files ready for snapshot, modified files, and untracked files.
    
    Examples:
        tellus files status                     # Show current status
        tellus --json files status             # JSON output for scripts
        
    Output shows:
        • Staged files (ready to snapshot)     
        • Modified files (changes detected)
        • Untracked files (not yet tracked)
        • Deleted files (removed from disk)
        
    See Also:
        tellus files add        # Stage files for snapshot
        tellus files snapshot   # Create snapshot of staged files
    """
    output_json = ctx.obj.get('output_json', False) if ctx.obj else False
    repo_root = find_repository_root()
    if not repo_root:
        console.print("[red]Error:[/red] Not in a Tellus repository")
        raise click.Abort()
    
    try:
        service = get_file_tracking_service()
        status = service.get_status(str(repo_root))
        
        # Repository info
        repo_info = service.get_repository_info(str(repo_root))
        
        console.print(f"[bold]Repository:[/] {repo_root}")
        if repo_info.dvc_enabled:
            console.print(f"[bold]DVC:[/] [green]Enabled[/green]")
        
        # Create status table
        table = Table(show_header=True, header_style="bold cyan")
        table.add_column("Status", width=12)
        table.add_column("Files", width=8, justify="right")
        table.add_column("Examples")
        
        # Add status rows
        if status.staged_files:
            examples = ", ".join(status.staged_files[:3])
            if len(status.staged_files) > 3:
                examples += f", ... (+{len(status.staged_files) - 3})"
            table.add_row(
                Text("Staged", style="green"),
                str(len(status.staged_files)),
                examples
            )
        
        if status.modified_files:
            examples = ", ".join(status.modified_files[:3])
            if len(status.modified_files) > 3:
                examples += f", ... (+{len(status.modified_files) - 3})"
            table.add_row(
                Text("Modified", style="yellow"),
                str(len(status.modified_files)),
                examples
            )
        
        if status.untracked_files:
            examples = ", ".join(status.untracked_files[:3])
            if len(status.untracked_files) > 3:
                examples += f", ... (+{len(status.untracked_files) - 3})"
            table.add_row(
                Text("Untracked", style="cyan"),
                str(len(status.untracked_files)),
                examples
            )
        
        if status.deleted_files:
            examples = ", ".join(status.deleted_files[:3])
            if len(status.deleted_files) > 3:
                examples += f", ... (+{len(status.deleted_files) - 3})"
            table.add_row(
                Text("Deleted", style="red"),
                str(len(status.deleted_files)),
                examples
            )
        
        if table.rows:
            console.print("\n")
            console.print(table)
        else:
            console.print("\n[green]✨ Repository is clean - no changes detected[/]")
        
        # Show suggestions
        if status.staged_files:
            console.print(f"\n[dim]Ready to snapshot {len(status.staged_files)} staged files[/]")
            console.print(f"[dim]Run: [code]tellus files snapshot -m \"Your message\"[/code][/]")
        elif status.modified_files or status.untracked_files:
            console.print(f"\n[dim]Add files to track changes:[/]")
            console.print(f"[dim]Run: [code]tellus files add <files>[/code][/]")
        
    except EntityNotFoundError:
        console.print("[red]Error:[/red] Repository not found")
        raise click.Abort()
    except ApplicationError as e:
        console.print(f"[red]Error:[/red] {e}")
        raise click.Abort()


@files.command()
@click.option(
    "--message", 
    "-m",
    required=True,
    help="Snapshot message"
)
@click.option(
    "--author",
    help="Author name (default: from git config or system user)"
)
def snapshot(message: str, author: Optional[str]):
    """Create a snapshot of staged files."""
    repo_root = find_repository_root()
    if not repo_root:
        console.print("[red]Error:[/red] Not in a Tellus repository")
        raise click.Abort()
    
    try:
        service = get_file_tracking_service()
        
        # Get author if not provided
        if not author:
            import getpass
            import subprocess
            
            try:
                # Try to get from git config
                result = subprocess.run(
                    ["git", "config", "user.name"],
                    capture_output=True,
                    text=True,
                    timeout=5
                )
                if result.returncode == 0:
                    author = result.stdout.strip()
                else:
                    author = getpass.getuser()
            except (subprocess.SubprocessError, FileNotFoundError):
                author = getpass.getuser()
        
        dto = CreateSnapshotDto(
            message=message,
            author=author
        )
        
        snapshot = service.create_snapshot(str(repo_root), dto)
        
        console.print(f"[green]✓[/green] Created snapshot [cyan]{snapshot.short_id}[/cyan]: {message}")
        console.print(f"[dim]Author: {author}[/]")
        console.print(f"[dim]Files: {len(snapshot.changed_files)} changed[/]")
        
        if snapshot.changed_files:
            console.print(f"\n[bold]Changed files:[/]")
            for file_path in snapshot.changed_files:
                change_type = snapshot.change_types.get(file_path, "unknown")
                if change_type == "added":
                    console.print(f"  [green]A[/green] {file_path}")
                elif change_type == "modified":
                    console.print(f"  [yellow]M[/yellow] {file_path}")
                elif change_type == "deleted":
                    console.print(f"  [red]D[/red] {file_path}")
                else:
                    console.print(f"  [dim]?[/dim] {file_path}")
        
    except EntityNotFoundError:
        console.print("[red]Error:[/red] Repository not found")
        raise click.Abort()
    except ApplicationError as e:
        console.print(f"[red]Error:[/red] {e}")
        raise click.Abort()


@files.command()
@click.option(
    "--limit",
    type=int,
    default=10,
    help="Maximum number of snapshots to show (default: 10)"
)
def log(limit: int):
    """Show snapshot history."""
    repo_root = find_repository_root()
    if not repo_root:
        console.print("[red]Error:[/red] Not in a Tellus repository")
        raise click.Abort()
    
    try:
        service = get_file_tracking_service()
        snapshots = service.list_snapshots(str(repo_root))
        
        if not snapshots:
            console.print("[yellow]No snapshots found[/]")
            return
        
        # Show most recent first
        recent_snapshots = list(reversed(snapshots))[:limit]
        
        console.print(f"[bold]Snapshot History[/] (showing {len(recent_snapshots)} of {len(snapshots)})\n")
        
        for snapshot in recent_snapshots:
            # Parse timestamp
            from datetime import datetime
            timestamp = datetime.fromisoformat(snapshot.timestamp)
            time_str = timestamp.strftime("%Y-%m-%d %H:%M")
            
            console.print(f"[cyan]{snapshot.short_id}[/cyan] - {snapshot.message}")
            console.print(f"[dim]  {snapshot.author} • {time_str} • {len(snapshot.changed_files)} files[/]")
            
            # Show changed files (abbreviated)
            if snapshot.changed_files:
                file_list = ", ".join(snapshot.changed_files[:3])
                if len(snapshot.changed_files) > 3:
                    file_list += f" ... (+{len(snapshot.changed_files) - 3})"
                console.print(f"[dim]  {file_list}[/]")
            
            console.print()
        
    except EntityNotFoundError:
        console.print("[red]Error:[/red] Repository not found")
        raise click.Abort()
    except ApplicationError as e:
        console.print(f"[red]Error:[/red] {e}")
        raise click.Abort()


@files.command()
def dvc():
    """Show DVC status and configuration."""
    repo_root = find_repository_root()
    if not repo_root:
        console.print("[red]Error:[/red] Not in a Tellus repository")
        raise click.Abort()
    
    try:
        service = get_file_tracking_service()
        dvc_status = service.get_dvc_status(str(repo_root))
        
        # Create DVC info panel
        if dvc_status.is_available:
            content = "[green]✓ DVC is available[/]\n"
            
            if dvc_status.repository_initialized:
                content += "[green]✓ DVC initialized in this repository[/]\n"
                
                if dvc_status.configured_remotes:
                    content += f"[bold]Remotes:[/] {', '.join(dvc_status.configured_remotes)}\n"
                
                if dvc_status.tracked_files:
                    content += f"[bold]DVC-tracked files:[/] {len(dvc_status.tracked_files)}\n"
                    for file_path in dvc_status.tracked_files[:5]:
                        content += f"  • {file_path}\n"
                    if len(dvc_status.tracked_files) > 5:
                        content += f"  ... and {len(dvc_status.tracked_files) - 5} more\n"
                else:
                    content += "[yellow]No files currently tracked by DVC[/]\n"
            else:
                content += "[yellow]DVC not initialized in this repository[/]\n"
                content += "[dim]Use --enable-dvc when running 'tellus files init'[/]\n"
        else:
            content = "[red]✗ DVC not available[/]\n"
            content += "[dim]Install DVC: pip install dvc[/]\n"
        
        console.print(Panel(
            content,
            title="[bold cyan]DVC Status[/]",
            border_style="cyan"
        ))
        
    except EntityNotFoundError:
        console.print("[red]Error:[/red] Repository not found")
        raise click.Abort()
    except ApplicationError as e:
        console.print(f"[red]Error:[/red] {e}")
        raise click.Abort()


