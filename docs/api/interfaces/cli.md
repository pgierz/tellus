# Command Line Interface (CLI)

The CLI provides command-line access to all Tellus functionality with rich output formatting, interactive wizards, and comprehensive help systems.

## Core CLI Commands

### Main CLI Entry Point

```{eval-rst}
.. currentmodule:: tellus.interfaces.cli.main

.. autofunction:: cli
.. autofunction:: version
```

### Simulation Management

```{eval-rst}
.. currentmodule:: tellus.interfaces.cli.simulation

.. autofunction:: simulation
.. autofunction:: create_simulation
.. autofunction:: list_simulations
.. autofunction:: show_simulation
.. autofunction:: delete_simulation
.. autofunction:: location_commands
.. autofunction:: associate_location
.. autofunction:: disassociate_location
.. autofunction:: list_locations
```

### Location Management

```{eval-rst}
.. currentmodule:: tellus.interfaces.cli.location

.. autofunction:: location
.. autofunction:: create_location
.. autofunction:: list_locations
.. autofunction:: show_location
.. autofunction:: update_location
.. autofunction:: delete_location
.. autofunction:: test_location
```

### Archive Management  

```{eval-rst}
.. currentmodule:: tellus.interfaces.cli.archive

.. autofunction:: archive
.. autofunction:: list_archives
.. autofunction:: show_archive
.. autofunction:: files_command
.. autofunction:: create_archive
.. autofunction:: copy_archive
.. autofunction:: move_archive
.. autofunction:: extract_archive
.. autofunction:: associate_files
```

### File Transfer Operations

```{eval-rst}
.. currentmodule:: tellus.interfaces.cli.transfer

.. autofunction:: transfer
.. autofunction:: single_file
.. autofunction:: batch_files
.. autofunction:: sync_directories
.. autofunction:: queue
.. autofunction:: queue_list
.. autofunction:: queue_status
.. autofunction:: queue_cancel
.. autofunction:: queue_pause
.. autofunction:: queue_resume
.. autofunction:: queue_clear
.. autofunction:: queue_stats
```

### Progress Tracking

```{eval-rst}
.. currentmodule:: tellus.interfaces.cli.progress

.. autofunction:: progress
.. autofunction:: list_operations
.. autofunction:: show_operation
.. autofunction:: cancel_operation
.. autofunction:: pause_operation
.. autofunction:: resume_operation
.. autofunction:: monitor_operation
.. autofunction:: cleanup_operations
.. autofunction:: progress_stats
```

## Interactive Wizards

### Workflow Wizards

```{eval-rst}
.. currentmodule:: tellus.interfaces.cli.workflow_wizards

.. autofunction:: workflow_wizard
.. autofunction:: simulation_setup_wizard
.. autofunction:: data_transfer_wizard
.. autofunction:: archive_creation_wizard
.. autofunction:: location_setup_wizard
```

## CLI Configuration

### Rich Output Configuration

```{eval-rst}
.. currentmodule:: tellus.interfaces.cli

.. autofunction:: configure_console
.. autofunction:: setup_logging
.. autofunction:: get_console
```

### Progress Display

```{eval-rst}
.. currentmodule:: tellus.interfaces.cli.progress_display

.. autoclass:: ProgressDisplay
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__

.. autoclass:: RichProgressDisplay  
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__

.. autoclass:: SimpleProgressDisplay
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__
```

## Usage Examples

### Basic CLI Operations

```bash
# Create a new simulation
tellus simulation create cesm2-historical \
    --model CESM2.1 \
    --attrs experiment=historical \
    --attrs time_period=1850-2014 \
    --attrs resolution=f09_g17

# List all simulations
tellus simulation list

# Show detailed simulation information
tellus simulation show cesm2-historical

# Create a storage location
tellus location create hpc-scratch \
    --kind COMPUTE \
    --protocol ssh \
    --config host=hpc.edu \
    --config username=researcher \
    --config path=/scratch

# Test location connectivity
tellus location test hpc-scratch

# Associate simulation with location
tellus simulation location associate cesm2-historical hpc-scratch \
    --context path_prefix="/scratch/cesm2/{simulation_id}" \
    --context file_pattern="*.nc"
```

### Advanced Archive Operations

```bash
# Create archive from simulation data
tellus archive create cesm2-output-v1 \
    "/scratch/cesm2/output/*.nc" \
    --location long-term-storage \
    --simulation cesm2-historical \
    --compression lz4 \
    --description "CESM2 model output files"

# List archive contents
tellus archive files cesm2-output-v1 \
    --content-type MODEL_OUTPUT \
    --pattern "*.nc"

# Extract specific files from archive
tellus archive extract cesm2-output-v1 \
    --destination hpc-scratch \
    --simulation cesm2-historical \
    --pattern "**/tas_*.nc" \
    --content-type MODEL_OUTPUT

# Copy archive between locations
tellus archive copy cesm2-output-v1 \
    --source long-term-storage \
    --dest backup-storage \
    --simulation cesm2-historical \
    --verify
```

### File Transfer Workflows

```bash
# Single file transfer with progress
tellus transfer single-file \
    --source-location hpc-scratch \
    --source-path "/scratch/cesm2/tas_monthly.nc" \
    --dest-location local-workspace \
    --dest-path "data/tas_monthly.nc" \
    --verify-checksum

# Batch file transfer
tellus transfer batch-files \
    --source-location hpc-scratch \
    --dest-location local-workspace \
    --file-pairs "/scratch/cesm2/tas_*.nc:data/temperature/" \
    --file-pairs "/scratch/cesm2/pr_*.nc:data/precipitation/" \
    --max-concurrent 3 \
    --verify-checksums

# Directory synchronization
tellus transfer sync-directories \
    --source-location hpc-scratch \
    --source-path "/scratch/cesm2/output/" \
    --dest-location cloud-archive \
    --dest-path "cesm2-historical-v1/" \
    --exclude "*.log" \
    --exclude "*.tmp" \
    --compression

# Monitor transfer queue
tellus transfer queue list --status running
tellus transfer queue stats
```

### Progress Monitoring

```bash
# List all operations
tellus progress list --limit 10

# Show operation details
tellus progress show data-processing-001

# Monitor operation in real-time
tellus progress monitor data-processing-001 \
    --refresh-interval 2 \
    --show-details

# Cancel running operation
tellus progress cancel data-processing-001

# Clean up completed operations
tellus progress cleanup --older-than 7d
```

### Interactive Wizards

```bash
# Launch simulation setup wizard
tellus wizard simulation-setup

# Data transfer planning wizard
tellus wizard data-transfer

# Archive creation wizard
tellus wizard archive-creation

# Location configuration wizard  
tellus wizard location-setup
```

## CLI Output Formats

### Rich Table Output

```python
from tellus.interfaces.cli import get_console
from rich.table import Table

console = get_console()

# Create formatted table for simulation listing
def display_simulations(simulations):
    table = Table(title="🌍 Climate Simulations")
    
    table.add_column("Simulation ID", style="bold blue")
    table.add_column("Model", style="green") 
    table.add_column("Experiment", style="yellow")
    table.add_column("Status", style="red")
    table.add_column("Data Size", justify="right")
    
    for sim in simulations:
        table.add_row(
            sim.simulation_id,
            sim.model_id,
            sim.attrs.get('experiment', 'unknown'),
            sim.attrs.get('status', 'unknown'),
            sim.attrs.get('data_size', 'unknown')
        )
    
    console.print(table)
```

### JSON Output Mode

```bash
# Get JSON output for programmatic processing
tellus simulation list --output json | jq '.simulations[0]'

# JSON output for all commands
tellus location list --output json
tellus archive list --output json  
tellus progress list --output json

# Pipe to external tools
tellus simulation list --output json | \
    jq -r '.simulations[] | select(.attrs.experiment == "historical") | .simulation_id'
```

### Progress Bars and Status

```python
from tellus.interfaces.cli.progress_display import RichProgressDisplay
from tellus.domain.entities.progress_tracking import ProgressOperation

# Rich progress display with multiple progress bars
progress_display = RichProgressDisplay()

# Add multiple operations
operations = [
    ProgressOperation("download-001", "download", "Downloading CESM2 data"),
    ProgressOperation("process-001", "processing", "Processing NetCDF files"),
    ProgressOperation("archive-001", "archiving", "Creating archive")
]

for op in operations:
    progress_display.add_operation(op)

# Update progress for each operation
progress_display.update_progress("download-001", {
    "current_step": 50,
    "total_steps": 100,
    "status_message": "Downloading temperature data"
})

progress_display.update_progress("process-001", {
    "current_step": 25, 
    "total_steps": 50,
    "status_message": "Regridding precipitation data"
})
```

## CLI Extension and Customization

### Custom Commands

```python
import click
from tellus.interfaces.cli.main import cli
from tellus.application.container import ServiceContainer

@cli.group()
def custom():
    """Custom climate analysis commands."""
    pass

@custom.command()
@click.argument('simulation_id')
@click.option('--variables', multiple=True, help='Variables to analyze')
@click.option('--time-period', help='Time period for analysis')
def climate_analysis(simulation_id, variables, time_period):
    """Perform custom climate analysis on simulation data."""
    
    console = get_console()
    container = ServiceContainer()
    
    console.print(f"🔬 Running climate analysis for [bold]{simulation_id}[/bold]")
    
    if variables:
        console.print(f"Variables: {', '.join(variables)}")
    if time_period:
        console.print(f"Time period: {time_period}")
    
    # Custom analysis logic here
    with console.status("Performing analysis..."):
        # Simulate analysis work
        time.sleep(2)
        
    console.print("✅ Analysis completed successfully!")

# Usage: tellus custom climate-analysis cesm2-historical --variables tas pr
```

### CLI Configuration Files

```yaml
# ~/.tellus/cli_config.yaml
output:
  format: table  # table, json, yaml
  style: rich    # rich, simple, minimal
  
progress:
  refresh_rate: 2.0
  show_eta: true
  show_rate: true
  
colors:
  theme: dark    # dark, light, auto
  
logging:
  level: INFO
  file: ~/.tellus/cli.log
  
locations:
  default_timeout: 30
  max_retries: 3
  
transfers:
  default_chunk_size: 10MB
  max_concurrent: 4
  verify_checksums: true
```

### Shell Completion

```bash
# Enable shell completion for bash
eval "$(_TELLUS_COMPLETE=bash_source tellus)"

# Enable shell completion for zsh  
eval "$(_TELLUS_COMPLETE=zsh_source tellus)"

# Enable shell completion for fish
eval (env _TELLUS_COMPLETE=fish_source tellus)

# Generate completion script
tellus --completion bash > /etc/bash_completion.d/tellus
```

### Custom Output Formatters

```python
from tellus.interfaces.cli import register_formatter
from rich.panel import Panel

@register_formatter('simulation', 'detailed')
def format_simulation_detailed(simulation, console):
    """Custom detailed formatter for simulations."""
    
    # Create detailed panel with simulation information
    content = f"""
    [bold]Simulation ID:[/bold] {simulation.simulation_id}
    [bold]Model:[/bold] {simulation.model_id}
    
    [bold]Attributes:[/bold]
    """
    
    for key, value in simulation.attrs.items():
        content += f"  • {key}: {value}\n"
    
    if simulation.associated_locations:
        content += f"\n[bold]Associated Locations:[/bold]\n"
        for location in simulation.associated_locations:
            content += f"  • {location}\n"
    
    panel = Panel(
        content.strip(),
        title=f"🌍 Simulation Details",
        border_style="blue",
        expand=False
    )
    
    console.print(panel)

# Usage: tellus simulation show cesm2-historical --format detailed
```

## Error Handling and User Experience

### Helpful Error Messages

```python
from tellus.interfaces.cli import handle_cli_error
from rich.panel import Panel

def handle_simulation_not_found(simulation_id: str):
    """Provide helpful error message for missing simulation."""
    
    console = get_console()
    
    error_panel = Panel(
        f"[red]Simulation '{simulation_id}' not found.[/red]\n\n"
        f"💡 [bold]Suggestions:[/bold]\n"
        f"  • Check simulation ID spelling\n"
        f"  • List available simulations: [code]tellus simulation list[/code]\n"
        f"  • Create new simulation: [code]tellus simulation create {simulation_id}[/code]",
        title="❌ Error",
        border_style="red"
    )
    
    console.print(error_panel)
```

### Interactive Confirmation

```python
from rich.prompt import Confirm, Prompt

def confirm_destructive_operation(operation: str, target: str) -> bool:
    """Confirm destructive operations with user."""
    
    console = get_console()
    
    console.print(f"⚠️  [bold red]Warning:[/bold red] This will {operation} [bold]{target}[/bold]")
    
    return Confirm.ask(
        "Are you sure you want to continue?",
        default=False,
        console=console
    )

# Usage in CLI commands
if confirm_destructive_operation("delete", simulation_id):
    # Proceed with deletion
    pass
else:
    console.print("Operation cancelled.")
```

### Context-Aware Help

```python
@click.command()
@click.pass_context
def smart_help_command(ctx):
    """Provide context-aware help based on user's current state."""
    
    container = ServiceContainer()
    simulation_service = container.get_simulation_service()
    
    simulations = simulation_service.list_simulations()
    
    if not simulations.simulations:
        # No simulations - show getting started help
        console.print("""
        👋 Welcome to Tellus! Let's get you started:
        
        1. Create your first simulation:
           [code]tellus simulation create my-first-sim --model CESM2.1[/code]
           
        2. Add a storage location:
           [code]tellus location create local-data --kind DISK --protocol file[/code]
           
        3. Associate simulation with location:
           [code]tellus simulation location associate my-first-sim local-data[/code]
        """)
    else:
        # Show contextual help based on current state
        console.print(f"""
        📊 You have {len(simulations.simulations)} simulations configured.
        
        Common next steps:
        • Transfer data: [code]tellus transfer single-file --help[/code]
        • Create archives: [code]tellus archive create --help[/code]
        • Monitor progress: [code]tellus progress list[/code]
        """)
```

## Performance and Responsiveness

### Lazy Loading

```python
# CLI commands use lazy loading for better responsiveness
@click.command()
@click.option('--limit', default=10, help='Limit number of results')
def list_simulations_optimized(limit):
    """List simulations with optimized loading."""
    
    container = ServiceContainer()
    simulation_service = container.get_simulation_service()
    
    # Only load requested number of simulations
    simulations = simulation_service.list_simulations(limit=limit)
    
    # Stream results for large datasets
    for simulation in simulations.simulations:
        display_simulation_summary(simulation)
```

### Progress Feedback for Long Operations

```python
@click.command()
def long_running_command():
    """Command with progress feedback."""
    
    console = get_console()
    
    with console.status("🔄 Processing large dataset...") as status:
        # Long-running operation with periodic status updates
        for step in range(100):
            status.update(f"Processing item {step + 1}/100...")
            time.sleep(0.1)
    
    console.print("✅ Processing completed!")
```