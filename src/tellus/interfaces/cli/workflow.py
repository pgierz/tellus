"""Command-line interface for Tellus workflow management.

Note: Workflow CLI uses the clean architecture.
to fall back to, so USE_NEW_WORKFLOW_SERVICE feature flag is not checked here.
"""

# import asyncio - not needed as services are not async
import json
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

import questionary
import rich_click as click
from rich import print as rprint
from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress, TaskID
from rich.prompt import Confirm, FloatPrompt, IntPrompt, Prompt
from rich.table import Table
from rich.text import Text

from ...application.services.workflow_execution_service import \
    WorkflowExecutionService
from ...application.services.workflow_service import WorkflowApplicationService
# Import workflow system components
from ...domain.entities.workflow import (ResourceRequirement, RunStatus,
                                         WorkflowEngine, WorkflowEntity,
                                         WorkflowRunEntity, WorkflowStatus,
                                         WorkflowStep, WorkflowType)
from ...infrastructure.adapters.progress_tracking import ProgressTracker
from ...infrastructure.adapters.workflow_engines import (
    PythonWorkflowEngine, SnakemakeWorkflowEngine)
from ...infrastructure.repositories.json_location_repository import \
    JsonLocationRepository
from ...infrastructure.repositories.json_workflow_repository import (
    JsonWorkflowRepository, JsonWorkflowRunRepository,
    JsonWorkflowTemplateRepository)

# Initialize console
console = Console()

# Initialize services using container
def _get_workflow_services():
    """Get workflow services using the application container."""
    from ...application.container import get_service_container
    
    container = get_service_container()
    
    # Create workflow-specific repositories using container paths
    workflow_repo = JsonWorkflowRepository(
        workflows_file=str(container.project_data_path / "workflows.json")
    )
    run_repo = JsonWorkflowRunRepository(
        runs_file=str(container.project_data_path / "workflow_runs.json")
    )
    template_repo = JsonWorkflowTemplateRepository(
        templates_file=str(container.project_data_path / "workflow_templates.json")
    )
    
    # Use location service from container
    location_service = container.service_factory.location_service
    
    workflow_service = WorkflowApplicationService(
        workflow_repository=workflow_repo,
        template_repository=template_repo,
        location_repository=location_service._location_repo  # Access underlying repo
    )
    
    return workflow_service, run_repo

def _get_execution_service():
    """Get workflow execution service using container."""
    workflow_service, run_repo = _get_workflow_services()
    
    execution_engines = {
        WorkflowEngine.SNAKEMAKE: SnakemakeWorkflowEngine(),
        WorkflowEngine.PYTHON: PythonWorkflowEngine()
    }
    
    progress_tracker = ProgressTracker()
    
    # Get container for location repository
    from ...application.container import get_service_container
    container = get_service_container()
    location_service = container.service_factory.location_service
    
    workflow_service, run_repo = _get_workflow_services()
    execution_service = WorkflowExecutionService(
        workflow_repository=workflow_service._workflow_repository,
        run_repository=run_repo,
        location_repository=location_service._location_repo,
        workflow_engines=execution_engines,
        progress_tracker=progress_tracker
    )
    
    return execution_service


# Helper functions
def get_workflow_or_exit(workflow_id: str) -> WorkflowEntity:
    """Helper to get a workflow or exit with error."""
    try:
        workflow_service, _ = _get_workflow_services()
        workflow = workflow_service.get_workflow(workflow_id)
        if not workflow:
            console.print(f"[red]Error:[/red] Workflow with ID '{workflow_id}' not found")
            raise click.Abort()
        return workflow
    except Exception as e:
        console.print(f"[red]Error:[/red] Failed to retrieve workflow: {e}")
        raise click.Abort()


def format_duration(td: Optional[timedelta]) -> str:
    """Format timedelta for display."""
    if not td:
        return "N/A"
    
    total_seconds = int(td.total_seconds())
    hours = total_seconds // 3600
    minutes = (total_seconds % 3600) // 60
    seconds = total_seconds % 60
    
    if hours > 0:
        return f"{hours}h {minutes}m {seconds}s"
    elif minutes > 0:
        return f"{minutes}m {seconds}s"
    else:
        return f"{seconds}s"


def format_workflow_status(status: WorkflowStatus) -> Text:
    """Format workflow status with color."""
    color_map = {
        WorkflowStatus.DRAFT: "yellow",
        WorkflowStatus.READY: "green",
        WorkflowStatus.DEPRECATED: "orange1",
        WorkflowStatus.ARCHIVED: "dim"
    }
    return Text(status.name, style=color_map.get(status, "white"))


def format_run_status(status: RunStatus) -> Text:
    """Format run status with color."""
    color_map = {
        RunStatus.QUEUED: "blue",
        RunStatus.RUNNING: "yellow",
        RunStatus.COMPLETED: "green",
        RunStatus.FAILED: "red",
        RunStatus.CANCELLED: "orange1",
        RunStatus.PAUSED: "purple"
    }
    return Text(status.name, style=color_map.get(status, "white"))


# Main workflow command group
@click.group(name="workflow")
def workflow_cli():
    """Manage Earth science workflows."""
    pass


# Workflow management commands
@workflow_cli.group(name="create")
def create_workflow():
    """Create new workflows."""
    pass


@create_workflow.command(name="interactive")
def create_workflow_interactive():
    """Create a new workflow interactively."""
    console.print("[bold blue]Creating New Workflow[/bold blue]")
    console.print()
    
    # Basic workflow information
    workflow_id = questionary.text(
        "Workflow ID:",
        validate=lambda x: len(x) > 0 or "Workflow ID cannot be empty"
    ).ask()
    
    name = questionary.text(
        "Workflow name:",
        validate=lambda x: len(x) > 0 or "Workflow name cannot be empty"
    ).ask()
    
    description = questionary.text("Description (optional):").ask()
    
    workflow_type = questionary.select(
        "Workflow type:",
        choices=[
            questionary.Choice("Data Preprocessing", WorkflowType.DATA_PREPROCESSING),
            questionary.Choice("Model Execution", WorkflowType.MODEL_EXECUTION),
            questionary.Choice("Post Processing", WorkflowType.POST_PROCESSING),
            questionary.Choice("Data Analysis", WorkflowType.DATA_ANALYSIS),
            questionary.Choice("Quality Control", WorkflowType.QUALITY_CONTROL),
            questionary.Choice("Archival", WorkflowType.ARCHIVAL),
            questionary.Choice("Visualization", WorkflowType.VISUALIZATION),
            questionary.Choice("Custom", WorkflowType.CUSTOM),
        ]
    ).ask()
    
    # Create workflow steps
    steps = []
    console.print("\n[bold]Adding workflow steps:[/bold]")
    
    while True:
        step_id = questionary.text("Step ID:").ask()
        if not step_id:
            break
            
        step_name = questionary.text("Step name:").ask()
        command = questionary.text("Command to execute:").ask()
        
        # Dependencies
        if steps:
            available_deps = [step.step_id for step in steps]
            dependencies = questionary.checkbox(
                "Dependencies (select steps this depends on):",
                choices=available_deps
            ).ask()
        else:
            dependencies = []
        
        # Resource requirements
        add_resources = questionary.confirm("Add resource requirements?", default=False).ask()
        resource_req = None
        
        if add_resources:
            cpu_cores = questionary.text("CPU cores:", default="1").ask()
            memory_gb = questionary.text("Memory (GB):", default="1.0").ask()
            disk_space_gb = questionary.text("Disk space (GB):", default="1.0").ask()
            gpu_count = questionary.text("GPU count:", default="0").ask()
            
            resource_req = ResourceRequirement(
                cpu_cores=int(cpu_cores),
                memory_gb=float(memory_gb),
                disk_space_gb=float(disk_space_gb),
                gpu_count=int(gpu_count)
            )
        
        step = WorkflowStep(
            step_id=step_id,
            name=step_name,
            command=command,
            dependencies=dependencies,
            resource_requirements=resource_req
        )
        
        steps.append(step)
        
        if not questionary.confirm("Add another step?", default=True).ask():
            break
    
    # Create workflow
    try:
        workflow = WorkflowEntity(
            workflow_id=workflow_id,
            name=name,
            description=description or "",
            workflow_type=workflow_type,
            steps=steps,
            status=WorkflowStatus.DRAFT
        )
        
        # Validate workflow
        errors = workflow.validate()
        if errors:
            console.print("[red]Workflow validation failed:[/red]")
            for error in errors:
                console.print(f"  • {error}")
            return
        
        # Save workflow
        workflow_service, _ = _get_workflow_services()
        workflow_service.create_workflow(workflow)
        console.print(f"[green]✓[/green] Workflow '{workflow_id}' created successfully!")
        
    except Exception as e:
        console.print(f"[red]Error creating workflow:[/red] {e}")


@create_workflow.command(name="from-template")
@click.argument("template_id")
@click.argument("workflow_id")
@click.option("--parameters", "-p", help="JSON string of parameters")
def create_from_template(template_id: str, workflow_id: str, parameters: Optional[str]):
    """Create workflow from template."""
    try:
        params = {}
        if parameters:
            params = json.loads(parameters)
        
        workflow_service, _ = _get_workflow_services()
        from ...application.dtos import WorkflowInstantiationDto
        
        instantiation_dto = WorkflowInstantiationDto(
            template_id=template_id,
            workflow_id=workflow_id,
            parameters=params
        )
        workflow = workflow_service.instantiate_workflow_from_template(instantiation_dto)
        
        console.print(f"[green]✓[/green] Workflow '{workflow_id}' created from template '{template_id}'")
        
    except Exception as e:
        console.print(f"[red]Error creating workflow from template:[/red] {e}")


@workflow_cli.command(name="list")
@click.option("--type", "workflow_type", help="Filter by workflow type")
@click.option("--status", help="Filter by status")
@click.option("--verbose", "-v", is_flag=True, help="Show detailed information")
def list_workflows(workflow_type: Optional[str], status: Optional[str], verbose: bool):
    """List all workflows."""
    try:
        workflow_service, _ = _get_workflow_services()
        workflow_list_dto = workflow_service.list_workflows()
        workflows = workflow_list_dto.workflows
        
        if not workflows:
            console.print("No workflows found.")
            return
        
        # Apply filters
        if workflow_type:
            workflows = [w for w in workflows if w.engine.lower() == workflow_type.lower()]
        
        # Note: Status filtering needs to be implemented in service layer
        # if status:
        #     workflows = [w for w in workflows if w.status.name.lower() == status.lower()]
        
        if verbose:
            # Detailed table
            table = Table(title="Workflows", show_header=True, header_style="bold magenta")
            table.add_column("ID", style="cyan")
            table.add_column("Name", style="white")
            table.add_column("Engine", style="yellow")
            table.add_column("Version", justify="center")
            table.add_column("Steps", justify="center")
            table.add_column("Created", style="dim")
            table.add_column("Author", style="dim")
            
            for workflow in workflows:
                created_str = workflow.created_at.split('T')[0] if workflow.created_at else "N/A"
                table.add_row(
                    workflow.workflow_id,
                    workflow.name,
                    workflow.engine,
                    workflow.version,
                    str(len(workflow.steps)),
                    created_str,
                    workflow.author or "N/A"
                )
        else:
            # Simple table
            table = Table(show_header=True, header_style="bold magenta")
            table.add_column("ID", style="cyan")
            table.add_column("Name", style="white") 
            table.add_column("Engine", style="yellow")
            table.add_column("Version", justify="center")
            table.add_column("Steps", justify="center")
            
            for workflow in workflows:
                table.add_row(
                    workflow.workflow_id,
                    workflow.name,
                    workflow.engine,
                    workflow.version,
                    str(len(workflow.steps))
                )
        
        console.print(table)
        
    except Exception as e:
        console.print(f"[red]Error listing workflows:[/red] {e}")


@workflow_cli.command(name="show")
@click.argument("workflow_id")
def show_workflow(workflow_id: str):
    """Show detailed information about a workflow."""
    workflow = get_workflow_or_exit(workflow_id)
    
    # Main workflow panel
    info_text = f"""[bold]Name:[/bold] {workflow.name}
[bold]Type:[/bold] {workflow.workflow_type.name}
[bold]Status:[/bold] {workflow.status.name}
[bold]Version:[/bold] {workflow.version}
[bold]Author:[/bold] {workflow.author or 'Unknown'}
[bold]Created:[/bold] {workflow.created_at.strftime('%Y-%m-%d %H:%M:%S')}
[bold]Updated:[/bold] {workflow.updated_at.strftime('%Y-%m-%d %H:%M:%S')}
[bold]Description:[/bold] {workflow.description or 'No description'}"""
    
    if workflow.tags:
        tags_str = ", ".join(workflow.tags)
        info_text += f"\n[bold]Tags:[/bold] {tags_str}"
    
    panel = Panel(info_text, title=f"Workflow: {workflow_id}", border_style="blue")
    console.print(panel)
    
    # Steps table
    if workflow.steps:
        console.print("\n[bold]Workflow Steps:[/bold]")
        steps_table = Table(show_header=True, header_style="bold green")
        steps_table.add_column("Step ID", style="cyan")
        steps_table.add_column("Name", style="white")
        steps_table.add_column("Dependencies", style="yellow")
        steps_table.add_column("Resources", style="magenta")
        
        for step in workflow.steps:
            deps_str = ", ".join(step.dependencies) if step.dependencies else "None"
            
            if step.resource_requirements:
                req = step.resource_requirements
                resources_str = f"CPU:{req.cpu_cores} MEM:{req.memory_gb}GB DISK:{req.disk_space_gb}GB"
                if req.gpu_count > 0:
                    resources_str += f" GPU:{req.gpu_count}"
            else:
                resources_str = "Default"
            
            steps_table.add_row(
                step.step_id,
                step.name,
                deps_str,
                resources_str
            )
        
        console.print(steps_table)
    
    # Parameters
    if workflow.parameters:
        console.print("\n[bold]Parameters:[/bold]")
        for key, value in workflow.parameters.items():
            console.print(f"  [cyan]{key}:[/cyan] {value}")


@workflow_cli.command(name="validate")
@click.argument("workflow_id")
def validate_workflow(workflow_id: str):
    """Validate a workflow definition."""
    workflow = get_workflow_or_exit(workflow_id)
    
    console.print(f"Validating workflow '{workflow_id}'...")
    
    errors = workflow.validate()
    
    if not errors:
        console.print("[green]✓ Workflow validation passed![/green]")
    else:
        console.print("[red]✗ Workflow validation failed:[/red]")
        for error in errors:
            console.print(f"  • {error}")


@workflow_cli.command(name="delete")
@click.argument("workflow_id")
@click.option("--force", is_flag=True, help="Skip confirmation")
def delete_workflow(workflow_id: str, force: bool):
    """Delete a workflow."""
    workflow = get_workflow_or_exit(workflow_id)
    
    if not force:
        if not Confirm.ask(f"Are you sure you want to delete workflow '{workflow_id}'?"):
            console.print("Deletion cancelled.")
            return
    
    try:
        workflow_service, _ = _get_workflow_services()
        workflow_service.delete_workflow(workflow_id)
        console.print(f"[green]✓[/green] Workflow '{workflow_id}' deleted successfully!")
        
    except Exception as e:
        console.print(f"[red]Error deleting workflow:[/red] {e}")


# Workflow execution commands
@workflow_cli.group(name="run")
def run_workflow():
    """Execute workflows."""
    pass


@run_workflow.command(name="start")
@click.argument("workflow_id")
@click.option("--engine", default="python", help="Execution engine (python, snakemake)")
@click.option("--parameters", "-p", help="JSON string of runtime parameters")
@click.option("--wait", "-w", is_flag=True, help="Wait for completion")
@click.option("--follow", "-f", is_flag=True, help="Follow execution progress")
def start_workflow(workflow_id: str, engine: str, parameters: Optional[str], wait: bool, follow: bool):
    """Start workflow execution."""
    try:
        # Parse parameters
        params = {}
        if parameters:
            params = json.loads(parameters)
        
        # Start execution
        # TODO: Implement proper workflow execution service integration
        console.print("[yellow]Note: Workflow execution functionality needs implementation[/yellow]")
        run_id = f"{workflow_id}-{int(time.time())}"
        
        console.print(f"[green]✓[/green] Workflow execution started with ID: {run_id}")
        
        if follow or wait:
            console.print("[dim]Note: Progress monitoring not yet implemented[/dim]")
        
    except Exception as e:
        console.print(f"[red]Error starting workflow:[/red] {e}")


@run_workflow.command(name="status")
@click.argument("run_id")
def workflow_status(run_id: str):
    """Show workflow execution status."""
    try:
        console.print(f"[yellow]Note: Workflow status functionality needs implementation[/yellow]")
        console.print(f"Requested status for run: {run_id}")
        return
        
        # Main status panel
        status_text = f"""[bold]Workflow:[/bold] {run_status.workflow_id}
[bold]Status:[/bold] {run_status.status.name}
[bold]Progress:[/bold] {run_status.progress:.1f}%
[bold]Current Step:[/bold] {run_status.current_step or 'N/A'}"""
        
        if run_status.start_time:
            status_text += f"\n[bold]Started:[/bold] {run_status.start_time.strftime('%Y-%m-%d %H:%M:%S')}"
        
        if run_status.end_time:
            status_text += f"\n[bold]Ended:[/bold] {run_status.end_time.strftime('%Y-%m-%d %H:%M:%S')}"
        
        duration = run_status.get_duration()
        if duration:
            status_text += f"\n[bold]Duration:[/bold] {format_duration(duration)}"
        
        if run_status.error_message:
            status_text += f"\n[bold red]Error:[/bold red] {run_status.error_message}"
        
        panel = Panel(status_text, title=f"Execution Status: {run_id[:8]}", border_style="blue")
        console.print(panel)
        
        # Resource usage
        if run_status.resource_usage:
            console.print("\n[bold]Resource Usage:[/bold]")
            for key, value in run_status.resource_usage.items():
                console.print(f"  [cyan]{key}:[/cyan] {value}")
        
        # Step results
        if run_status.step_results:
            console.print("\n[bold]Step Results:[/bold]")
            steps_table = Table(show_header=True, header_style="bold green")
            steps_table.add_column("Step", style="cyan")
            steps_table.add_column("Status", justify="center")
            steps_table.add_column("Duration", style="yellow")
            steps_table.add_column("Exit Code", justify="center")
            
            for result in run_status.step_results:
                duration_str = "N/A"
                if result.get('duration'):
                    duration_str = format_duration(timedelta(seconds=result['duration']))
                
                status_color = "green" if result.get('success') else "red"
                status_text = "SUCCESS" if result.get('success') else "FAILED"
                
                steps_table.add_row(
                    result.get('step_id', 'Unknown'),
                    Text(status_text, style=status_color),
                    duration_str,
                    str(result.get('exit_code', 'N/A'))
                )
            
            console.print(steps_table)
        
        # Recent logs
        if run_status.logs:
            console.print("\n[bold]Recent Logs:[/bold]")
            for log_entry in run_status.logs[-10:]:  # Last 10 entries
                console.print(f"  {log_entry}")
        
    except Exception as e:
        console.print(f"[red]Error getting workflow status:[/red] {e}")


@run_workflow.command(name="list")
@click.option("--status", help="Filter by run status")
@click.option("--workflow", help="Filter by workflow ID")
@click.option("--limit", type=int, default=20, help="Maximum number of runs to show")
def list_runs(status: Optional[str], workflow: Optional[str], limit: int):
    """List workflow execution runs."""
    try:
        # This would need to be implemented in the execution service
        console.print("Listing workflow runs...")
        
        # For now, show placeholder table
        table = Table(title="Workflow Runs", show_header=True, header_style="bold magenta")
        table.add_column("Run ID", style="cyan")
        table.add_column("Workflow", style="white")
        table.add_column("Status", justify="center")
        table.add_column("Progress", justify="center")
        table.add_column("Started", style="dim")
        table.add_column("Duration", style="yellow")
        
        console.print(table)
        console.print(f"[dim]Note: Run listing functionality needs to be implemented[/dim]")
        
    except Exception as e:
        console.print(f"[red]Error listing runs:[/red] {e}")


@run_workflow.command(name="cancel")
@click.argument("run_id")
@click.option("--force", is_flag=True, help="Force cancellation")
def cancel_run(run_id: str, force: bool):
    """Cancel a running workflow."""
    try:
        console.print(f"[yellow]Note: Workflow cancellation functionality needs implementation[/yellow]")
        console.print(f"Requested cancellation for run: {run_id}")
            
    except Exception as e:
        console.print(f"[red]Error cancelling workflow run:[/red] {e}")


# Template management commands
@workflow_cli.group(name="template")
def template_commands():
    """Manage workflow templates."""
    pass


@template_commands.command(name="list")
def list_templates():
    """List available workflow templates."""
    try:
        workflow_service, _ = _get_workflow_services()
        templates = workflow_service.list_templates()
        
        if not templates:
            console.print("No workflow templates found.")
            return
        
        table = Table(title="Workflow Templates", show_header=True, header_style="bold magenta")
        table.add_column("ID", style="cyan")
        table.add_column("Name", style="white")
        table.add_column("Type", style="yellow")
        table.add_column("Version", style="green")
        table.add_column("Author", style="dim")
        
        for template in templates:
            table.add_row(
                template.template_id,
                template.name,
                template.workflow_type.name,
                template.version,
                template.author or "Unknown"
            )
        
        console.print(table)
        
    except Exception as e:
        console.print(f"[red]Error listing templates:[/red] {e}")


@template_commands.command(name="show")
@click.argument("template_id")
def show_template(template_id: str):
    """Show template details."""
    try:
        workflow_service, _ = _get_workflow_services()
        template = workflow_service.get_template(template_id)
        
        if not template:
            console.print(f"[red]Template '{template_id}' not found[/red]")
            return
        
        # Template info panel
        info_text = f"""[bold]Name:[/bold] {template.name}
[bold]Type:[/bold] {template.workflow_type.name}
[bold]Version:[/bold] {template.version}
[bold]Author:[/bold] {template.author or 'Unknown'}
[bold]Created:[/bold] {template.created_at.strftime('%Y-%m-%d %H:%M:%S')}
[bold]Description:[/bold] {template.description}"""
        
        if template.tags:
            tags_str = ", ".join(template.tags)
            info_text += f"\n[bold]Tags:[/bold] {tags_str}"
        
        panel = Panel(info_text, title=f"Template: {template_id}", border_style="green")
        console.print(panel)
        
        # Parameters
        if template.parameter_schema:
            console.print("\n[bold]Parameters:[/bold]")
            params_table = Table(show_header=True, header_style="bold green")
            params_table.add_column("Parameter", style="cyan")
            params_table.add_column("Type", style="yellow")
            params_table.add_column("Required", justify="center")
            params_table.add_column("Default", style="dim")
            
            for param, schema in template.parameter_schema.items():
                required = "Yes" if schema.get('required', False) else "No"
                default = str(template.default_parameters.get(param, 'N/A'))
                param_type = schema.get('type', 'Any')
                
                params_table.add_row(param, param_type, required, default)
            
            console.print(params_table)
        
    except Exception as e:
        console.print(f"[red]Error showing template:[/red] {e}")


# Add workflow CLI to main CLI
def register_workflow_cli(main_cli):
    """Register workflow commands with main CLI."""
    main_cli.add_command(workflow_cli)