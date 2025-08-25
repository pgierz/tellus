# Advanced Interfaces Tutorial

## Introduction

Welcome to the advanced interfaces tutorial for Tellus! This guide covers the Text User Interface (TUI), advanced CLI features, programmatic API usage, and integration with other Earth System Model tools. You'll learn to efficiently manage large-scale climate data operations and integrate Tellus into your existing workflows.

## What You'll Learn

By the end of this tutorial, you'll master:
- The interactive Text User Interface (TUI) for visual data management
- Advanced CLI features and automation techniques
- Programmatic API usage for custom integrations
- Integration with HPC job schedulers and workflow engines
- Configuration management and customization
- Troubleshooting and debugging advanced scenarios

## Prerequisites

- Completed the [Quickstart](01-quickstart.md), [Storage Setup](02-storage-setup.md), and [Automation Workflows](03-automation-workflows.md) tutorials
- Familiarity with Python programming for API usage
- Access to HPC systems for advanced examples (optional)

## Part 1: The Text User Interface (TUI)

The Tellus TUI provides an interactive, visual way to manage your climate data with vim-style navigation and a ranger-like file browser interface.

### Launching the TUI

```bash
# Launch the full TUI application
pixi run tellus tui app

# Or using the shortcut
pixi run tellus tui
```

### TUI Navigation and Layout

The TUI uses a three-panel layout inspired by ranger and vim:

```
┌─────────────────┬─────────────────┬─────────────────┐
│   Navigation    │   Main View     │   Details       │
│   (Left Panel)  │  (Center Panel) │  (Right Panel)  │
├─────────────────┼─────────────────┼─────────────────┤
│ • Simulations   │ File Browser    │ Metadata &      │
│ • Locations     │ or Data View    │ Properties      │
│ • Archives      │                 │                 │
│ • Operations    │                 │                 │
│ • Workflows     │                 │                 │
└─────────────────┴─────────────────┴─────────────────┘
```

### Vim-Style Navigation

The TUI supports familiar vim keybindings:

**Movement:**
```
h, j, k, l    - Move left, down, up, right
gg            - Go to top
G             - Go to bottom  
/             - Search
n/N           - Next/previous search result
```

**Actions:**
```
Enter         - Select/expand item
Tab           - Switch between panels
q             - Quit current view
r             - Refresh current view
t             - Test connection (for locations)
```

**File Operations:**
```
y             - Yank (copy) selection
p             - Paste to current location
d             - Delete/remove item
c             - Create new item
e             - Edit properties
```

### Working with Simulations in the TUI

Navigate to the Simulations panel and explore your data:

```python
# Example: Managing simulations through the TUI
# The TUI automatically loads your registered simulations

# Key features:
# - Visual representation of simulation metadata
# - File browser showing data files and structure
# - Real-time status updates and progress tracking
# - Batch operations on multiple simulations
```

**Simulation Management Workflow:**
1. Press `h` to focus the navigation panel
2. Navigate to "Simulations" with `j/k`
3. Press `l` or `Enter` to open simulations view
4. Use `j/k` to browse simulations
5. Press `l` to view simulation details and files
6. Press `Tab` to see detailed metadata in the right panel

### Location Management in the TUI

Test and manage your storage locations interactively:

```python
# Location testing and management features:
# - Real-time connection testing with 't' key
# - Storage usage visualization
# - Configuration validation
# - Batch location operations
```

**Location Testing Workflow:**
1. Navigate to "Locations" in the left panel
2. Select a location with `j/k`
3. Press `t` to test the connection
4. View connection status in the right panel
5. Press `e` to edit location configuration
6. Press `r` to refresh location status

### Archive Browser

Browse and manage archives visually:

```python
# Archive management features:
# - Visual file browser for archive contents
# - Extraction preview and planning
# - Batch archive operations
# - Archive integrity checking
```

**Archive Operations Workflow:**
1. Navigate to "Archives" 
2. Select an archive and press `Enter`
3. Browse files with `j/k/l/h`
4. Press `e` to extract selected files
5. Use `y/p` to copy files between locations

### Operation Queue Management

Monitor and control long-running operations:

```python
# Queue management features:
# - Real-time progress visualization
# - Operation cancellation and retry
# - Batch queue operations
# - Performance monitoring
```

## Part 2: Advanced CLI Features

### Rich Output and Formatting

Tellus CLI provides rich, formatted output for better readability:

```python
from tellus.application.container import ServiceContainer
from rich.console import Console
from rich.table import Table
from rich.panel import Panel

# The CLI automatically uses rich formatting
# But you can access it programmatically too

console = Console()

# Example: Custom formatted output
def display_simulation_summary():
    container = ServiceContainer()
    simulation_service = container.get_simulation_service()
    
    simulations = simulation_service.list_simulations()
    
    # Create a rich table
    table = Table(title="🌍 Climate Simulations Dashboard")
    table.add_column("Simulation ID", style="bold blue")
    table.add_column("Model", style="green")
    table.add_column("Experiment", style="yellow")
    table.add_column("Status", style="red")
    table.add_column("Data Size", justify="right")
    
    for sim in simulations.simulations:
        table.add_row(
            sim.simulation_id,
            sim.attrs.get('atmospheric_model', sim.model_id),
            sim.attrs.get('experiment', 'unknown'),
            sim.attrs.get('status', 'unknown'),
            sim.attrs.get('data_size', 'unknown')
        )
    
    console.print(table)
    
    # Add summary panel
    summary = Panel(
        f"Total simulations: {len(simulations.simulations)}",
        title="📊 Summary",
        expand=False
    )
    console.print(summary)

# Run the example
display_simulation_summary()
```

### Advanced CLI Scripting

Create powerful CLI scripts for automation:

```bash
#!/bin/bash
# advanced-climate-data-management.sh

# Set up error handling
set -euo pipefail

# Configuration
WORKSPACE="climate-analysis-2024"
ARCHIVE_LOCATION="institutional-tape"
SCRATCH_LOCATION="hpc-scratch"

# Function: Process new simulation data
process_new_simulation() {
    local simulation_id="$1"
    local model_id="$2"
    
    echo "🔄 Processing simulation: $simulation_id"
    
    # Create simulation record
    pixi run tellus simulation create "$simulation_id" \
        --model "$model_id" \
        --attrs experiment=historical \
        --attrs status=processing \
        --attrs start_date="$(date -Iseconds)"
    
    # Associate with scratch location for processing
    pixi run tellus simulation location associate "$simulation_id" \
        "$SCRATCH_LOCATION" \
        --context path_prefix="/scratch/climate/$simulation_id" \
        --context stage=processing
    
    # Start monitoring
    pixi run tellus progress create-tracker "$simulation_id-processing" \
        --description "Processing $simulation_id simulation data" \
        --estimated-duration 7200  # 2 hours
    
    echo "✅ Simulation $simulation_id ready for processing"
}

# Function: Archive processed data
archive_processed_data() {
    local simulation_id="$1"
    
    echo "📦 Archiving processed data for: $simulation_id"
    
    # Create archive
    local archive_id="${simulation_id}-processed-$(date +%Y%m%d)"
    
    pixi run tellus archive create "$archive_id" \
        "/scratch/climate/$simulation_id/processed/*.nc" \
        --location "$ARCHIVE_LOCATION" \
        --simulation "$simulation_id" \
        --compression lz4 \
        --description "Processed climate data for $simulation_id"
    
    # Update simulation status
    pixi run tellus simulation update "$simulation_id" \
        --attrs status=archived \
        --attrs archive_date="$(date -Iseconds)" \
        --attrs archive_id="$archive_id"
    
    echo "✅ Data archived successfully"
}

# Function: Batch process multiple simulations
batch_process() {
    local config_file="$1"
    
    echo "🔄 Starting batch processing from: $config_file"
    
    # Read simulation list from JSON config
    while IFS= read -r line; do
        simulation_id=$(echo "$line" | jq -r '.simulation_id')
        model_id=$(echo "$line" | jq -r '.model_id')
        
        if [[ "$simulation_id" != "null" && "$model_id" != "null" ]]; then
            process_new_simulation "$simulation_id" "$model_id"
            
            # Add to processing queue
            pixi run tellus file-transfer queue add \
                --source-location "$SCRATCH_LOCATION" \
                --source-path "/scratch/climate/$simulation_id" \
                --dest-location "$WORKSPACE" \
                --dest-path "/analysis/$simulation_id" \
                --operation-type directory_sync \
                --priority high
        fi
    done < <(jq -c '.simulations[]' "$config_file")
    
    echo "📊 Batch processing queued. Monitor with: tellus progress list"
}

# Usage examples
case "${1:-help}" in
    process)
        process_new_simulation "$2" "$3"
        ;;
    archive) 
        archive_processed_data "$2"
        ;;
    batch)
        batch_process "$2"
        ;;
    *)
        echo "Usage: $0 {process|archive|batch} [args...]"
        echo "  process <simulation_id> <model_id>"
        echo "  archive <simulation_id>" 
        echo "  batch <config_file.json>"
        ;;
esac
```

### Custom CLI Extensions

Create custom CLI commands for your specific workflows:

```python
# custom_climate_commands.py
import click
from tellus.application.container import ServiceContainer
from tellus.interfaces.cli.console import console
from tellus.application.dtos import CreateSimulationDto
from rich.progress import Progress, SpinnerColumn, TextColumn

@click.group()
def climate():
    """Custom climate data management commands."""
    pass

@climate.command()
@click.argument('experiment_name')
@click.option('--models', multiple=True, required=True,
              help='Climate models to include (CESM2, GFDL-CM4, etc.)')
@click.option('--period', default='1850-2100',
              help='Simulation time period')
@click.option('--ensemble-size', type=int, default=5,
              help='Number of ensemble members')
def setup_cmip_experiment(experiment_name, models, period, ensemble_size):
    """Set up a CMIP-style multi-model experiment."""
    
    container = ServiceContainer()
    simulation_service = container.get_simulation_service()
    
    console.print(f"🌍 Setting up CMIP experiment: [bold]{experiment_name}[/bold]")
    console.print(f"Models: {', '.join(models)}")
    console.print(f"Period: {period}")
    console.print(f"Ensemble size: {ensemble_size}")
    
    created_simulations = []
    
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console
    ) as progress:
        
        task = progress.add_task("Creating simulations...", total=len(models) * ensemble_size)
        
        for model in models:
            for ensemble_idx in range(1, ensemble_size + 1):
                simulation_id = f"{experiment_name}-{model.lower()}-r{ensemble_idx}i1p1f1"
                
                sim_dto = CreateSimulationDto(
                    simulation_id=simulation_id,
                    model_id=model,
                    attrs={
                        'experiment': experiment_name,
                        'time_period': period,
                        'ensemble_member': f'r{ensemble_idx}i1p1f1',
                        'model': model,
                        'variant_label': f'r{ensemble_idx}i1p1f1',
                        'grid_label': 'gn',
                        'status': 'planned',
                        'experiment_type': 'cmip'
                    }
                )
                
                simulation = simulation_service.create_simulation(sim_dto)
                created_simulations.append(simulation)
                
                progress.advance(task)
    
    console.print(f"✅ Created {len(created_simulations)} simulations")
    
    # Create experiment summary
    from rich.table import Table
    table = Table(title=f"CMIP Experiment: {experiment_name}")
    table.add_column("Simulation ID", style="blue")
    table.add_column("Model", style="green")
    table.add_column("Ensemble", style="yellow")
    
    for sim in created_simulations:
        table.add_row(
            sim.simulation_id,
            sim.attrs['model'],
            sim.attrs['ensemble_member']
        )
    
    console.print(table)

@climate.command()
@click.argument('experiment_name')
@click.option('--target-location', required=True,
              help='Location to synchronize data to')
@click.option('--file-pattern', default='*.nc',
              help='File pattern to match')
def sync_experiment_data(experiment_name, target_location, file_pattern):
    """Synchronize all data for a CMIP experiment."""
    
    container = ServiceContainer()
    simulation_service = container.get_simulation_service()
    transfer_service = container.get_file_transfer_service()
    
    # Find all simulations for this experiment
    all_sims = simulation_service.list_simulations()
    experiment_sims = [
        sim for sim in all_sims.simulations
        if sim.attrs.get('experiment') == experiment_name
    ]
    
    console.print(f"📊 Found {len(experiment_sims)} simulations for experiment: {experiment_name}")
    
    # Queue transfers for all simulations
    with Progress(console=console) as progress:
        task = progress.add_task("Queueing transfers...", total=len(experiment_sims))
        
        for sim in experiment_sims:
            # This would queue actual file transfers
            # Implementation depends on your specific data organization
            console.print(f"  Queued: {sim.simulation_id}")
            progress.advance(task)
    
    console.print(f"✅ Data synchronization queued for {experiment_name}")

# Register the commands
if __name__ == '__main__':
    climate()
```

To use these custom commands:

```bash
# Make the script executable and use it
chmod +x custom_climate_commands.py

# Set up a CMIP experiment
python custom_climate_commands.py setup-cmip-experiment "historical" \
    --models CESM2 --models GFDL-CM4 --models IPSL-CM6A \
    --period "1850-2014" \
    --ensemble-size 3

# Sync experiment data
python custom_climate_commands.py sync-experiment-data "historical" \
    --target-location "analysis-cluster" \
    --file-pattern "*.nc"
```

## Part 3: Programmatic API Usage

### Core Service API

Access Tellus functionality programmatically for custom applications:

```python
# advanced_api_usage.py
from tellus.application.container import ServiceContainer
from tellus.application.dtos import (
    CreateSimulationDto, CreateLocationDto, CreateArchiveDto,
    FileTransferOperationDto, ArchiveExtractionDto
)
from tellus.domain.entities.location import LocationKind
from tellus.domain.entities.file_type_config import FileContentType
import asyncio
from pathlib import Path
from datetime import datetime, timedelta
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ClimateDataManager:
    """High-level API for climate data management operations."""
    
    def __init__(self):
        """Initialize the climate data manager."""
        self.container = ServiceContainer()
        self.simulation_service = self.container.get_simulation_service()
        self.location_service = self.container.get_location_service()
        self.archive_service = self.container.get_archive_service()
        self.transfer_service = self.container.get_file_transfer_service()
        self.progress_service = self.container.get_progress_tracking_service()
        
    def create_research_project(self, project_name: str, 
                              models: list[str], 
                              experiments: list[str],
                              base_path: str) -> dict:
        """
        Create a complete research project with multiple models and experiments.
        
        Parameters
        ----------
        project_name : str
            Name of the research project
        models : list of str
            Climate models to include (e.g., ['CESM2', 'GFDL-CM4'])
        experiments : list of str
            Experiments to run (e.g., ['historical', 'ssp585'])
        base_path : str
            Base filesystem path for project data
            
        Returns
        -------
        dict
            Project creation summary with simulation and location IDs
        """
        logger.info(f"Creating research project: {project_name}")
        
        # Create project workspace location
        workspace_dto = CreateLocationDto(
            name=f"{project_name}-workspace",
            kinds=[LocationKind.DISK],
            protocol="file",
            path=f"{base_path}/{project_name}",
            description=f"Workspace for {project_name} research project",
            metadata={
                "project": project_name,
                "purpose": "research_workspace",
                "created": datetime.now().isoformat()
            }
        )
        
        workspace = self.location_service.create_location(workspace_dto)
        logger.info(f"Created workspace: {workspace.name}")
        
        # Create simulations for all model-experiment combinations
        simulations = []
        for model in models:
            for experiment in experiments:
                sim_id = f"{project_name}-{model.lower()}-{experiment}-r1i1p1f1"
                
                sim_dto = CreateSimulationDto(
                    simulation_id=sim_id,
                    model_id=model,
                    attrs={
                        'project': project_name,
                        'experiment': experiment,
                        'model': model,
                        'ensemble_member': 'r1i1p1f1',
                        'status': 'planned',
                        'created': datetime.now().isoformat(),
                        'priority': 'high' if experiment == 'historical' else 'medium'
                    }
                )
                
                simulation = self.simulation_service.create_simulation(sim_dto)
                simulations.append(simulation)
                logger.info(f"Created simulation: {sim_id}")
        
        return {
            'project_name': project_name,
            'workspace_location': workspace.name,
            'workspace_path': workspace.path,
            'simulations_created': len(simulations),
            'simulation_ids': [s.simulation_id for s in simulations],
            'models': models,
            'experiments': experiments
        }
    
    async def intelligent_data_transfer(self, source_pattern: str, 
                                      dest_location: str,
                                      content_filters: list[FileContentType] = None,
                                      max_concurrent: int = 3) -> dict:
        """
        Perform intelligent data transfer with content-based filtering.
        
        Parameters
        ----------
        source_pattern : str
            Glob pattern for source files
        dest_location : str
            Destination location name
        content_filters : list of FileContentType, optional
            Filter files by content type (e.g., [FileContentType.MODEL_OUTPUT])
        max_concurrent : int, default 3
            Maximum concurrent transfers
            
        Returns
        -------
        dict
            Transfer operation summary and results
        """
        logger.info(f"Starting intelligent transfer: {source_pattern} -> {dest_location}")
        
        # This would implement smart file discovery and filtering
        # Based on content type, size, and other metadata
        
        results = {
            'files_discovered': 0,
            'files_transferred': 0,
            'files_skipped': 0,
            'total_size_gb': 0.0,
            'transfer_time_seconds': 0.0,
            'average_speed_mbps': 0.0
        }
        
        # Implementation would go here
        # This is a simplified example
        logger.info(f"Transfer completed: {results}")
        
        return results
    
    def setup_automated_archival(self, simulation_id: str,
                                source_location: str,
                                archive_location: str,
                                schedule: str = "weekly") -> str:
        """
        Set up automated archival for simulation data.
        
        Parameters
        ----------
        simulation_id : str
            Simulation to set up archival for
        source_location : str
            Source location with active data
        archive_location : str
            Archive location for long-term storage
        schedule : str, default "weekly"
            Archival schedule (daily, weekly, monthly)
            
        Returns
        -------
        str
            Archival workflow ID
        """
        logger.info(f"Setting up automated archival for: {simulation_id}")
        
        # Create archival configuration
        archive_config = {
            'simulation_id': simulation_id,
            'source_location': source_location,
            'archive_location': archive_location,
            'schedule': schedule,
            'compression': 'lz4',
            'verification': True,
            'cleanup_after_archive': False,
            'retention_policy': '10_years'
        }
        
        # This would integrate with workflow scheduling system
        workflow_id = f"archive-{simulation_id}-{datetime.now().strftime('%Y%m%d')}"
        
        logger.info(f"Archival workflow created: {workflow_id}")
        return workflow_id
    
    def generate_data_report(self, project_name: str = None) -> dict:
        """
        Generate comprehensive data usage and status report.
        
        Parameters
        ----------
        project_name : str, optional
            Limit report to specific project
            
        Returns
        -------
        dict
            Comprehensive data report
        """
        logger.info("Generating data usage report")
        
        # Get all simulations
        all_sims = self.simulation_service.list_simulations()
        if project_name:
            simulations = [
                s for s in all_sims.simulations 
                if s.attrs.get('project') == project_name
            ]
        else:
            simulations = all_sims.simulations
        
        # Get all locations
        all_locations = self.location_service.list_locations()
        
        # Generate report
        report = {
            'generated': datetime.now().isoformat(),
            'project_filter': project_name,
            'summary': {
                'total_simulations': len(simulations),
                'total_locations': len(all_locations.locations),
                'simulations_by_status': {},
                'models_used': set(),
                'experiments_run': set()
            },
            'storage_usage': {},
            'recent_activity': [],
            'recommendations': []
        }
        
        # Analyze simulations
        for sim in simulations:
            status = sim.attrs.get('status', 'unknown')
            report['summary']['simulations_by_status'][status] = \
                report['summary']['simulations_by_status'].get(status, 0) + 1
            
            report['summary']['models_used'].add(sim.model_id)
            if 'experiment' in sim.attrs:
                report['summary']['experiments_run'].add(sim.attrs['experiment'])
        
        # Convert sets to lists for JSON serialization
        report['summary']['models_used'] = list(report['summary']['models_used'])
        report['summary']['experiments_run'] = list(report['summary']['experiments_run'])
        
        # Add recommendations based on analysis
        if len(simulations) > 50:
            report['recommendations'].append(
                "Consider implementing automated cleanup policies for old simulations"
            )
        
        if len(all_locations.locations) > 10:
            report['recommendations'].append(
                "Review location usage patterns and consider consolidating underused locations"
            )
        
        logger.info(f"Report generated for {len(simulations)} simulations")
        return report

# Example usage
async def main():
    """Example usage of the advanced API."""
    
    # Initialize the manager
    manager = ClimateDataManager()
    
    # Create a research project
    project = manager.create_research_project(
        project_name="arctic-warming-study",
        models=["CESM2", "GFDL-CM4"],
        experiments=["historical", "ssp245", "ssp585"],
        base_path="/research/climate-projects"
    )
    
    print(f"Created project with {project['simulations_created']} simulations")
    
    # Set up automated archival for key simulations
    for sim_id in project['simulation_ids'][:2]:  # First two simulations
        workflow_id = manager.setup_automated_archival(
            simulation_id=sim_id,
            source_location=project['workspace_location'],
            archive_location="institutional-tape",
            schedule="monthly"
        )
        print(f"Archival workflow {workflow_id} created for {sim_id}")
    
    # Generate project report
    report = manager.generate_data_report(project_name="arctic-warming-study")
    print(f"Project report: {report['summary']['total_simulations']} simulations, "
          f"{len(report['summary']['models_used'])} models")

if __name__ == "__main__":
    asyncio.run(main())
```

## Part 4: HPC Integration

### Job Scheduler Integration

Integrate Tellus with HPC job schedulers for automated data management:

```python
# hpc_integration.py
from tellus.application.container import ServiceContainer
from tellus.application.dtos import CreateSimulationDto
import subprocess
import os
from pathlib import Path
import tempfile

class HPCIntegration:
    """Integration with HPC job schedulers and batch systems."""
    
    def __init__(self, scheduler_type="slurm"):
        """
        Initialize HPC integration.
        
        Parameters
        ----------
        scheduler_type : str, default "slurm"
            Type of job scheduler (slurm, pbs, sge)
        """
        self.scheduler_type = scheduler_type
        self.container = ServiceContainer()
        self.simulation_service = self.container.get_simulation_service()
    
    def generate_job_script(self, simulation_id: str, 
                          model_executable: str,
                          nodes: int = 4,
                          tasks_per_node: int = 32,
                          walltime: str = "24:00:00",
                          partition: str = "compute") -> str:
        """
        Generate HPC job script with Tellus integration.
        
        Parameters
        ----------
        simulation_id : str
            Tellus simulation ID
        model_executable : str
            Path to climate model executable
        nodes : int, default 4
            Number of compute nodes
        tasks_per_node : int, default 32
            MPI tasks per node
        walltime : str, default "24:00:00"
            Job wall clock time limit
        partition : str, default "compute"
            HPC partition/queue name
            
        Returns
        -------
        str
            Generated job script content
        """
        
        total_tasks = nodes * tasks_per_node
        
        if self.scheduler_type == "slurm":
            script = f"""#!/bin/bash
#SBATCH --job-name={simulation_id}
#SBATCH --nodes={nodes}
#SBATCH --ntasks-per-node={tasks_per_node}
#SBATCH --time={walltime}
#SBATCH --partition={partition}
#SBATCH --output={simulation_id}_%j.out
#SBATCH --error={simulation_id}_%j.err

# Load required modules
module load intel-mpi netcdf-fortran

# Set up Tellus environment
export TELLUS_SIMULATION_ID="{simulation_id}"
export TELLUS_JOB_ID=$SLURM_JOB_ID
export TELLUS_SCRATCH_DIR=$SLURM_SUBMIT_DIR

# Initialize Tellus tracking
echo "Starting simulation tracking..."
pixi run tellus simulation update "$TELLUS_SIMULATION_ID" \\
    --attrs status=running \\
    --attrs job_id=$SLURM_JOB_ID \\
    --attrs start_time="$(date -Iseconds)" \\
    --attrs allocated_nodes={nodes} \\
    --attrs allocated_tasks={total_tasks}

# Set up progress tracking
pixi run tellus progress create-tracker "${{TELLUS_SIMULATION_ID}}-run" \\
    --description "Running $TELLUS_SIMULATION_ID on $SLURM_JOB_NUM_NODES nodes" \\
    --estimated-duration 86400  # 24 hours

# Pre-simulation data staging
echo "Staging input data..."
pixi run tellus file-transfer queue add \\
    --source-location "input-data-archive" \\
    --source-path "/archive/inputs/$TELLUS_SIMULATION_ID" \\
    --dest-location "scratch-space" \\
    --dest-path "$SLURM_SUBMIT_DIR/input" \\
    --operation-type directory_sync \\
    --priority critical

# Wait for staging to complete
pixi run tellus file-transfer queue wait-for-completion

# Run the climate model
echo "Starting model execution..."
srun --mpi=pmi2 {model_executable} namelist.input

# Check model completion status
MODEL_EXIT_CODE=$?

if [ $MODEL_EXIT_CODE -eq 0 ]; then
    echo "Model completed successfully"
    
    # Update simulation status
    pixi run tellus simulation update "$TELLUS_SIMULATION_ID" \\
        --attrs status=completed \\
        --attrs end_time="$(date -Iseconds)" \\
        --attrs exit_code=$MODEL_EXIT_CODE
    
    # Start output data archival
    echo "Starting output archival..."
    pixi run tellus archive create "${{TELLUS_SIMULATION_ID}}-output-$(date +%Y%m%d)" \\
        "$SLURM_SUBMIT_DIR/output/*.nc" \\
        --location "long-term-archive" \\
        --simulation "$TELLUS_SIMULATION_ID" \\
        --compression lz4 \\
        --description "Model output from job $SLURM_JOB_ID"
    
    # Queue cleanup of scratch data
    pixi run tellus file-transfer queue add \\
        --source-location "scratch-space" \\
        --source-path "$SLURM_SUBMIT_DIR" \\
        --dest-location "/dev/null" \\
        --operation-type cleanup \\
        --delay-hours 48
        
else
    echo "Model failed with exit code $MODEL_EXIT_CODE"
    
    # Update simulation status
    pixi run tellus simulation update "$TELLUS_SIMULATION_ID" \\
        --attrs status=failed \\
        --attrs end_time="$(date -Iseconds)" \\
        --attrs exit_code=$MODEL_EXIT_CODE \\
        --attrs error_details="Model execution failed"
    
    # Archive error logs
    pixi run tellus archive create "${{TELLUS_SIMULATION_ID}}-error-$(date +%Y%m%d)" \\
        "$SLURM_SUBMIT_DIR/*.err" "$SLURM_SUBMIT_DIR/*.log" \\
        --location "error-archive" \\
        --simulation "$TELLUS_SIMULATION_ID" \\
        --description "Error logs from failed job $SLURM_JOB_ID"
fi

# Complete progress tracking
pixi run tellus progress complete "${{TELLUS_SIMULATION_ID}}-run"

echo "Job completed. Check simulation status with: tellus simulation show $TELLUS_SIMULATION_ID"
"""
        
        elif self.scheduler_type == "pbs":
            script = f"""#!/bin/bash
#PBS -N {simulation_id}
#PBS -l select={nodes}:ncpus={tasks_per_node}:mpiprocs={tasks_per_node}
#PBS -l walltime={walltime}
#PBS -q {partition}
#PBS -o {simulation_id}.out
#PBS -e {simulation_id}.err

cd $PBS_O_WORKDIR

# Set up Tellus environment
export TELLUS_SIMULATION_ID="{simulation_id}"
export TELLUS_JOB_ID=$PBS_JOBID

# Similar Tellus integration as SLURM version...
# (Implementation details similar to above)
"""
        
        return script
    
    def submit_simulation_job(self, simulation_id: str, 
                            script_content: str) -> str:
        """
        Submit a simulation job to the scheduler.
        
        Parameters
        ----------
        simulation_id : str
            Tellus simulation ID
        script_content : str
            Job script content
            
        Returns
        -------
        str
            Job ID returned by scheduler
        """
        
        # Write script to temporary file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.sh', delete=False) as f:
            f.write(script_content)
            script_path = f.name
        
        try:
            # Make script executable
            os.chmod(script_path, 0o755)
            
            # Submit job based on scheduler type
            if self.scheduler_type == "slurm":
                result = subprocess.run(
                    ["sbatch", script_path],
                    capture_output=True, text=True, check=True
                )
                # Parse job ID from SLURM output: "Submitted batch job 12345"
                job_id = result.stdout.strip().split()[-1]
                
            elif self.scheduler_type == "pbs":
                result = subprocess.run(
                    ["qsub", script_path],
                    capture_output=True, text=True, check=True
                )
                job_id = result.stdout.strip()
            
            # Update simulation with job information
            self.simulation_service.update_simulation_attrs(
                simulation_id, 
                {
                    'job_id': job_id,
                    'submission_time': subprocess.run(
                        ['date', '-Iseconds'], 
                        capture_output=True, text=True
                    ).stdout.strip(),
                    'job_script_path': script_path,
                    'status': 'queued'
                }
            )
            
            return job_id
            
        finally:
            # Clean up temporary script file
            if os.path.exists(script_path):
                os.unlink(script_path)

# Example usage
def submit_cesm_ensemble():
    """Example: Submit CESM ensemble simulations."""
    
    hpc = HPCIntegration(scheduler_type="slurm")
    container = ServiceContainer()
    simulation_service = container.get_simulation_service()
    
    # Create ensemble simulations
    ensemble_members = 5
    base_simulation = "cesm2-ssp585-ensemble"
    
    for member in range(1, ensemble_members + 1):
        simulation_id = f"{base_simulation}-r{member}i1p1f1"
        
        # Register simulation
        sim_dto = CreateSimulationDto(
            simulation_id=simulation_id,
            model_id="CESM2.1",
            attrs={
                'experiment': 'ssp585',
                'ensemble_member': f'r{member}i1p1f1',
                'time_period': '2015-2100',
                'resolution': 'f09_g17',
                'status': 'planned'
            }
        )
        
        simulation = simulation_service.create_simulation(sim_dto)
        
        # Generate and submit job
        script = hpc.generate_job_script(
            simulation_id=simulation_id,
            model_executable="/opt/cesm/cesm.exe",
            nodes=8,  # Larger allocation for CESM
            tasks_per_node=36,
            walltime="48:00:00",  # 48 hours
            partition="climate"
        )
        
        job_id = hpc.submit_simulation_job(simulation_id, script)
        print(f"Submitted {simulation_id} as job {job_id}")

if __name__ == "__main__":
    submit_cesm_ensemble()
```

## Part 5: Configuration and Customization

### Advanced Configuration Management

```python
# advanced_config.py
from pathlib import Path
import yaml
import os
from typing import Dict, Any
import logging

class TellusConfiguration:
    """Advanced configuration management for Tellus."""
    
    DEFAULT_CONFIG = {
        'storage': {
            'default_compression': 'lz4',
            'checksum_algorithm': 'sha256',
            'transfer_chunk_size_mb': 64,
            'max_concurrent_transfers': 5,
            'retry_attempts': 3,
            'retry_backoff_seconds': [1, 5, 15]
        },
        'archives': {
            'default_format': 'tar.gz',
            'compression_level': 6,
            'verification_enabled': True,
            'cleanup_temp_files': True,
            'max_archive_size_gb': 100
        },
        'performance': {
            'database_connection_pool_size': 20,
            'async_task_concurrency': 10,
            'progress_update_interval_seconds': 5,
            'memory_limit_gb': 8
        },
        'hpc': {
            'default_scheduler': 'slurm',
            'job_submission_delay_seconds': 1,
            'max_queued_jobs': 50,
            'job_monitoring_interval_seconds': 60
        },
        'ui': {
            'default_interface': 'cli',
            'rich_output': True,
            'progress_bars': True,
            'color_scheme': 'auto',
            'tui_refresh_rate_hz': 10
        },
        'logging': {
            'level': 'INFO',
            'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            'file_rotation': True,
            'max_log_size_mb': 100,
            'backup_count': 5
        }
    }
    
    def __init__(self, config_dir: Path = None):
        """Initialize configuration manager."""
        self.config_dir = config_dir or Path.home() / '.config' / 'tellus'
        self.config_file = self.config_dir / 'config.yaml'
        self.user_config = {}
        self._load_config()
    
    def _load_config(self):
        """Load configuration from file."""
        if self.config_file.exists():
            with open(self.config_file, 'r') as f:
                self.user_config = yaml.safe_load(f) or {}
        else:
            self.create_default_config()
    
    def create_default_config(self):
        """Create default configuration file."""
        self.config_dir.mkdir(parents=True, exist_ok=True)
        
        with open(self.config_file, 'w') as f:
            yaml.dump(self.DEFAULT_CONFIG, f, default_flow_style=False, indent=2)
        
        print(f"Created default configuration: {self.config_file}")
    
    def get(self, key_path: str, default=None) -> Any:
        """
        Get configuration value using dot notation.
        
        Parameters
        ----------
        key_path : str
            Configuration key path (e.g., 'storage.max_concurrent_transfers')
        default : Any, optional
            Default value if key not found
            
        Returns
        -------
        Any
            Configuration value
        """
        keys = key_path.split('.')
        
        # Try user config first
        config = self.user_config
        for key in keys:
            if isinstance(config, dict) and key in config:
                config = config[key]
            else:
                config = None
                break
        
        # Fall back to default config
        if config is None:
            config = self.DEFAULT_CONFIG
            for key in keys:
                if isinstance(config, dict) and key in config:
                    config = config[key]
                else:
                    return default
        
        return config
    
    def set(self, key_path: str, value: Any):
        """
        Set configuration value using dot notation.
        
        Parameters
        ----------
        key_path : str
            Configuration key path
        value : Any
            Value to set
        """
        keys = key_path.split('.')
        config = self.user_config
        
        # Navigate/create nested structure
        for key in keys[:-1]:
            if key not in config:
                config[key] = {}
            config = config[key]
        
        # Set final value
        config[keys[-1]] = value
        
        # Save to file
        self.save_config()
    
    def save_config(self):
        """Save current configuration to file."""
        with open(self.config_file, 'w') as f:
            yaml.dump(self.user_config, f, default_flow_style=False, indent=2)
    
    def get_environment_overrides(self) -> Dict[str, Any]:
        """Get configuration overrides from environment variables."""
        overrides = {}
        
        # Map environment variables to config keys
        env_mappings = {
            'TELLUS_MAX_CONCURRENT_TRANSFERS': 'storage.max_concurrent_transfers',
            'TELLUS_COMPRESSION': 'storage.default_compression',
            'TELLUS_LOG_LEVEL': 'logging.level',
            'TELLUS_SCHEDULER': 'hpc.default_scheduler',
            'TELLUS_UI_MODE': 'ui.default_interface'
        }
        
        for env_var, config_key in env_mappings.items():
            if env_var in os.environ:
                value = os.environ[env_var]
                
                # Type conversion based on default values
                default_value = self.get(config_key)
                if isinstance(default_value, int):
                    value = int(value)
                elif isinstance(default_value, bool):
                    value = value.lower() in ('true', '1', 'yes', 'on')
                elif isinstance(default_value, float):
                    value = float(value)
                
                overrides[config_key] = value
        
        return overrides

# Global configuration instance
config = TellusConfiguration()

# Example configuration usage
def configure_tellus_for_hpc():
    """Example: Configure Tellus for HPC environments."""
    
    # Set HPC-specific configurations
    config.set('storage.max_concurrent_transfers', 20)
    config.set('storage.transfer_chunk_size_mb', 256)
    config.set('archives.max_archive_size_gb', 500)
    config.set('hpc.default_scheduler', 'slurm')
    config.set('hpc.max_queued_jobs', 100)
    config.set('performance.memory_limit_gb', 32)
    config.set('logging.level', 'DEBUG')
    
    print("Configured Tellus for HPC environment")

def configure_tellus_for_laptop():
    """Example: Configure Tellus for laptop/desktop usage."""
    
    # Set resource-constrained configurations
    config.set('storage.max_concurrent_transfers', 2)
    config.set('storage.transfer_chunk_size_mb', 16)
    config.set('archives.max_archive_size_gb', 10)
    config.set('performance.memory_limit_gb', 2)
    config.set('ui.tui_refresh_rate_hz', 5)
    config.set('logging.level', 'WARNING')
    
    print("Configured Tellus for laptop/desktop usage")
```

## Part 6: Troubleshooting and Debugging

### Advanced Debugging Tools

```python
# debugging_tools.py
from tellus.application.container import ServiceContainer
from tellus.application.services.progress_tracking_service import ProgressTrackingApplicationService
import logging
import json
from pathlib import Path
from datetime import datetime, timedelta
from rich.console import Console
from rich.table import Table
from rich.panel import Panel

console = Console()

class TellusDebugger:
    """Advanced debugging and troubleshooting tools."""
    
    def __init__(self):
        self.container = ServiceContainer()
        self.progress_service = self.container.get_progress_tracking_service()
        
    def diagnose_system_health(self) -> dict:
        """Comprehensive system health check."""
        console.print("🔍 Running Tellus system diagnostics...")
        
        health_report = {
            'timestamp': datetime.now().isoformat(),
            'overall_status': 'healthy',
            'services': {},
            'locations': {},
            'recent_operations': {},
            'resource_usage': {},
            'recommendations': []
        }
        
        try:
            # Test service availability
            services_to_test = [
                ('simulation_service', self.container.get_simulation_service()),
                ('location_service', self.container.get_location_service()),
                ('archive_service', self.container.get_archive_service()),
                ('transfer_service', self.container.get_file_transfer_service()),
                ('progress_service', self.progress_service)
            ]
            
            for service_name, service in services_to_test:
                try:
                    # Test basic functionality
                    if hasattr(service, 'list_simulations'):
                        service.list_simulations()
                    elif hasattr(service, 'list_locations'):
                        service.list_locations()
                    elif hasattr(service, 'list_archives'):
                        service.list_archives()
                    elif hasattr(service, 'list_operations'):
                        service.list_operations()
                    
                    health_report['services'][service_name] = 'healthy'
                    
                except Exception as e:
                    health_report['services'][service_name] = f'error: {str(e)}'
                    health_report['overall_status'] = 'degraded'
        
        except Exception as e:
            health_report['overall_status'] = 'critical'
            health_report['services']['container'] = f'error: {str(e)}'
        
        # Test location connectivity
        try:
            location_service = self.container.get_location_service()
            locations = location_service.list_locations()
            
            for location in locations.locations:
                try:
                    # This would test actual connectivity
                    health_report['locations'][location.name] = 'accessible'
                except Exception as e:
                    health_report['locations'][location.name] = f'error: {str(e)}'
                    if health_report['overall_status'] == 'healthy':
                        health_report['overall_status'] = 'degraded'
        
        except Exception as e:
            health_report['locations']['_error'] = str(e)
        
        # Check recent operations
        try:
            recent_ops = self.progress_service.list_operations(limit=10)
            
            failed_ops = [
                op for op in recent_ops.operations 
                if op.status in ['failed', 'error']
            ]
            
            health_report['recent_operations'] = {
                'total_recent': len(recent_ops.operations),
                'failed_count': len(failed_ops),
                'failure_rate': len(failed_ops) / max(len(recent_ops.operations), 1)
            }
            
            if len(failed_ops) > 3:
                health_report['recommendations'].append(
                    f"High failure rate detected: {len(failed_ops)} recent failures"
                )
        
        except Exception as e:
            health_report['recent_operations']['_error'] = str(e)
        
        # Generate recommendations
        if health_report['overall_status'] != 'healthy':
            health_report['recommendations'].append(
                "System is not fully healthy - check service and location status"
            )
        
        return health_report
    
    def debug_transfer_issues(self, operation_id: str = None) -> dict:
        """Debug file transfer issues."""
        console.print(f"🔧 Debugging transfer issues for operation: {operation_id or 'recent'}")
        
        debug_info = {
            'operation_details': {},
            'location_status': {},
            'network_diagnostics': {},
            'suggested_fixes': []
        }
        
        # Get operation details
        if operation_id:
            try:
                operation = self.progress_service.get_operation(operation_id)
                debug_info['operation_details'] = {
                    'id': operation.operation_id,
                    'status': operation.status,
                    'progress': operation.progress_percentage,
                    'error_message': operation.error_message,
                    'started': operation.started_at.isoformat() if operation.started_at else None,
                    'updated': operation.updated_at.isoformat() if operation.updated_at else None
                }
                
                # Add specific diagnostics based on error patterns
                if operation.error_message:
                    if 'permission denied' in operation.error_message.lower():
                        debug_info['suggested_fixes'].append(
                            "Check file permissions on source and destination"
                        )
                    elif 'no such file' in operation.error_message.lower():
                        debug_info['suggested_fixes'].append(
                            "Verify source file paths exist"
                        )
                    elif 'connection' in operation.error_message.lower():
                        debug_info['suggested_fixes'].append(
                            "Check network connectivity and location configuration"
                        )
                        
            except Exception as e:
                debug_info['operation_details']['_error'] = str(e)
        
        return debug_info
    
    def analyze_performance_bottlenecks(self) -> dict:
        """Analyze system performance and identify bottlenecks."""
        console.print("📊 Analyzing performance bottlenecks...")
        
        analysis = {
            'timestamp': datetime.now().isoformat(),
            'queue_analysis': {},
            'resource_utilization': {},
            'slow_operations': [],
            'recommendations': []
        }
        
        try:
            # Analyze operation queue
            recent_ops = self.progress_service.list_operations(limit=100)
            
            if recent_ops.operations:
                durations = []
                for op in recent_ops.operations:
                    if op.started_at and op.completed_at:
                        duration = (op.completed_at - op.started_at).total_seconds()
                        durations.append(duration)
                        
                        if duration > 3600:  # More than 1 hour
                            analysis['slow_operations'].append({
                                'operation_id': op.operation_id,
                                'duration_hours': duration / 3600,
                                'description': op.description
                            })
                
                if durations:
                    analysis['queue_analysis'] = {
                        'average_duration_minutes': sum(durations) / len(durations) / 60,
                        'max_duration_minutes': max(durations) / 60,
                        'operations_over_1hour': len([d for d in durations if d > 3600])
                    }
                    
                    # Generate performance recommendations
                    if analysis['queue_analysis']['average_duration_minutes'] > 30:
                        analysis['recommendations'].append(
                            "Average operation time is high - consider optimizing transfer chunk sizes"
                        )
                    
                    if len(analysis['slow_operations']) > 5:
                        analysis['recommendations'].append(
                            "Multiple slow operations detected - review network and storage performance"
                        )
        
        except Exception as e:
            analysis['_error'] = str(e)
        
        return analysis
    
    def generate_debug_report(self, output_file: Path = None) -> str:
        """Generate comprehensive debug report."""
        console.print("📋 Generating comprehensive debug report...")
        
        report = {
            'generated': datetime.now().isoformat(),
            'system_health': self.diagnose_system_health(),
            'performance_analysis': self.analyze_performance_bottlenecks(),
            'configuration': {
                # Would include current configuration
                'note': 'Configuration details would be included here'
            },
            'environment_info': {
                'python_version': '3.x.x',  # Would get actual version
                'tellus_version': '1.0.0',   # Would get actual version
                'platform': 'linux'          # Would get actual platform
            }
        }
        
        report_json = json.dumps(report, indent=2, default=str)
        
        if output_file:
            output_file.write_text(report_json)
            console.print(f"Debug report saved to: {output_file}")
        
        return report_json

# Example usage and debugging scenarios
def debug_common_issues():
    """Examples of debugging common Tellus issues."""
    
    debugger = TellusDebugger()
    
    # 1. System health check
    console.print("\n=== System Health Check ===")
    health = debugger.diagnose_system_health()
    
    if health['overall_status'] == 'healthy':
        console.print("✅ System is healthy")
    else:
        console.print(f"⚠️ System status: {health['overall_status']}")
        
        # Show specific issues
        for service, status in health['services'].items():
            if 'error' in status:
                console.print(f"   {service}: [red]{status}[/red]")
    
    # 2. Performance analysis
    console.print("\n=== Performance Analysis ===")
    perf = debugger.analyze_performance_bottlenecks()
    
    if perf.get('slow_operations'):
        console.print(f"🐌 Found {len(perf['slow_operations'])} slow operations")
        for op in perf['slow_operations'][:3]:  # Show top 3
            console.print(f"   {op['operation_id']}: {op['duration_hours']:.1f} hours")
    
    # 3. Generate full debug report
    console.print("\n=== Debug Report ===")
    report_file = Path("/tmp/tellus-debug-report.json")
    debugger.generate_debug_report(report_file)

if __name__ == "__main__":
    debug_common_issues()
```

## Summary and Next Steps

Congratulations! You've completed the Advanced Interfaces tutorial and mastered the most sophisticated features of Tellus:

### Key Skills Learned

✅ **Text User Interface (TUI)** - Interactive data management with vim-style navigation  
✅ **Advanced CLI Features** - Rich output, scripting, and automation  
✅ **Programmatic API** - Custom integrations and advanced workflows  
✅ **HPC Integration** - Job scheduler integration and batch processing  
✅ **Configuration Management** - Advanced customization and optimization  
✅ **Debugging and Troubleshooting** - System diagnostics and performance analysis

### Advanced Patterns Mastered

- **Multi-modal interfaces**: CLI, TUI, and programmatic access
- **HPC workflow integration**: Job submission and monitoring
- **Performance optimization**: Configuration tuning and bottleneck analysis
- **System monitoring**: Health checks and automated diagnostics
- **Custom extensions**: Building domain-specific commands and workflows

### Production Deployment Readiness

You're now equipped to deploy Tellus in production environments:

1. **Research Groups**: Set up collaborative data management workflows
2. **Data Centers**: Implement automated archival and quality control
3. **HPC Centers**: Integrate with job schedulers and compute workflows
4. **Multi-institution Projects**: Coordinate distributed data operations

### Further Learning

- **Integration Examples**: Explore `docs/user-stories/` for real-world scenarios
- **API Documentation**: Comprehensive API reference in `docs/api/`
- **Performance Tuning**: HPC-specific optimization guides
- **Community Resources**: Join Earth System Model data management discussions

### Getting Support

- **Documentation**: Complete guides at `docs/`
- **Examples**: Production patterns in `docs/user-stories/`
- **Issues**: Report problems and request features
- **Community**: Connect with other climate data managers

You're now ready to tackle the most complex Earth System Model data management challenges with confidence! 🌍🚀