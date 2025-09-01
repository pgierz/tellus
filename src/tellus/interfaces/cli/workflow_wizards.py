"""
Interactive workflow creation wizards for common Earth science patterns.
"""

from typing import Any, Dict, List, Optional

import questionary
from rich.console import Console
from rich.panel import Panel

from ...domain.entities.workflow import (ResourceRequirement, WorkflowEntity,
                                         WorkflowStatus, WorkflowStep,
                                         WorkflowType)

console = Console()


class WorkflowWizard:
    """Base class for workflow creation wizards."""
    
    def __init__(self):
        self.workflow_type = WorkflowType.CUSTOM
        self.workflow_name = ""
        self.workflow_description = ""
    
    def run(self) -> Optional[WorkflowEntity]:
        """Run the wizard and return a configured workflow."""
        raise NotImplementedError


class ClimateDataPreprocessingWizard(WorkflowWizard):
    """Wizard for creating climate data preprocessing workflows."""
    
    def __init__(self):
        super().__init__()
        self.workflow_type = WorkflowType.DATA_PREPROCESSING
    
    def run(self) -> Optional[WorkflowEntity]:
        console.print(Panel(
            "[bold blue]Climate Data Preprocessing Wizard[/bold blue]\n"
            "This wizard will help you create a workflow for preprocessing climate data\n"
            "including validation, subsetting, regridding, and format conversion.",
            title="Workflow Wizard",
            border_style="blue"
        ))
        
        # Basic information
        workflow_id = questionary.text(
            "Workflow ID (e.g., 'preprocess-cmip6-data'):",
            validate=lambda x: len(x) > 0 or "Workflow ID cannot be empty"
        ).ask()
        
        if not workflow_id:
            return None
        
        workflow_name = questionary.text(
            "Workflow name:",
            default=f"Climate Data Preprocessing - {workflow_id}"
        ).ask()
        
        # Data source configuration
        input_format = questionary.select(
            "Input data format:",
            choices=["NetCDF", "GRIB", "HDF5", "Zarr", "Mixed formats"]
        ).ask()
        
        output_format = questionary.select(
            "Output data format:",
            choices=["NetCDF", "Zarr", "HDF5", "Same as input"]
        ).ask()
        
        # Processing steps selection
        processing_steps = questionary.checkbox(
            "Select preprocessing steps:",
            choices=[
                questionary.Choice("Data validation and QC", "validate"),
                questionary.Choice("Temporal subsetting", "temporal_subset"),
                questionary.Choice("Spatial subsetting", "spatial_subset"),
                questionary.Choice("Variable selection", "var_select"),
                questionary.Choice("Unit conversion", "unit_convert"),
                questionary.Choice("Regridding/interpolation", "regrid"),
                questionary.Choice("Data compression", "compress"),
                questionary.Choice("Metadata standardization", "metadata"),
                questionary.Choice("Format conversion", "format_convert"),
            ]
        ).ask()
        
        # Location configuration
        input_location = questionary.text(
            "Input data location (path or location name):",
            default="/data/input"
        ).ask()
        
        output_location = questionary.text(
            "Output data location:",
            default="/data/output"
        ).ask()
        
        # Resource requirements
        resource_intensive = questionary.confirm(
            "Is this a resource-intensive workflow (large datasets)?",
            default=True
        ).ask()
        
        if resource_intensive:
            cpu_cores = questionary.text("CPU cores:", default="8").ask()
            memory_gb = questionary.text("Memory (GB):", default="32").ask()
            disk_space_gb = questionary.text("Temporary disk space (GB):", default="100").ask()
        else:
            cpu_cores = "2"
            memory_gb = "8"
            disk_space_gb = "20"
        
        # Build workflow steps
        steps = []
        
        # Input validation step
        if "validate" in processing_steps:
            steps.append(WorkflowStep(
                step_id="validate_input",
                name="Validate Input Data",
                command=f"python validate_climate_data.py --input {input_location} --format {input_format}",
                dependencies=[],
                resource_requirements=ResourceRequirement(
                    cpu_cores=int(cpu_cores) // 4,
                    memory_gb=float(memory_gb) // 4,
                    disk_space_gb=10
                )
            ))
        
        # Subsetting steps
        prev_step = "validate_input" if "validate" in processing_steps else None
        
        if "temporal_subset" in processing_steps:
            start_date = questionary.text("Start date (YYYY-MM-DD):", default="2000-01-01").ask()
            end_date = questionary.text("End date (YYYY-MM-DD):", default="2020-12-31").ask()
            
            steps.append(WorkflowStep(
                step_id="temporal_subset",
                name="Temporal Subsetting",
                command=f"cdo seldate,{start_date},{end_date} {{input_file}} {{output_file}}",
                dependencies=[prev_step] if prev_step else [],
                resource_requirements=ResourceRequirement(
                    cpu_cores=int(cpu_cores) // 2,
                    memory_gb=float(memory_gb) // 2,
                    disk_space_gb=float(disk_space_gb) // 2
                )
            ))
            prev_step = "temporal_subset"
        
        if "spatial_subset" in processing_steps:
            bbox = questionary.text(
                "Bounding box (west,south,east,north):",
                default="-180,-90,180,90"
            ).ask()
            
            steps.append(WorkflowStep(
                step_id="spatial_subset",
                name="Spatial Subsetting",
                command=f"cdo sellonlatbox,{bbox} {{input_file}} {{output_file}}",
                dependencies=[prev_step] if prev_step else [],
                resource_requirements=ResourceRequirement(
                    cpu_cores=int(cpu_cores) // 2,
                    memory_gb=float(memory_gb) // 2,
                    disk_space_gb=float(disk_space_gb) // 2
                )
            ))
            prev_step = "spatial_subset"
        
        if "var_select" in processing_steps:
            variables = questionary.text(
                "Variables to select (comma-separated):",
                default="tas,pr,psl"
            ).ask()
            
            steps.append(WorkflowStep(
                step_id="select_vars",
                name="Variable Selection",
                command=f"cdo select,name={variables} {{input_file}} {{output_file}}",
                dependencies=[prev_step] if prev_step else [],
                resource_requirements=ResourceRequirement(
                    cpu_cores=2,
                    memory_gb=8,
                    disk_space_gb=20
                )
            ))
            prev_step = "select_vars"
        
        if "regrid" in processing_steps:
            target_grid = questionary.select(
                "Target grid:",
                choices=["1x1 degree", "2x2 degree", "0.5x0.5 degree", "Custom grid file"]
            ).ask()
            
            if target_grid == "Custom grid file":
                grid_file = questionary.text("Grid file path:").ask()
                grid_spec = f"-gridfile {grid_file}"
            else:
                grid_spec = f"-grid {target_grid.replace(' ', '')}"
            
            steps.append(WorkflowStep(
                step_id="regrid",
                name="Regridding",
                command=f"cdo remapbil,{grid_spec} {{input_file}} {{output_file}}",
                dependencies=[prev_step] if prev_step else [],
                resource_requirements=ResourceRequirement(
                    cpu_cores=int(cpu_cores),
                    memory_gb=float(memory_gb),
                    disk_space_gb=float(disk_space_gb)
                )
            ))
            prev_step = "regrid"
        
        # Format conversion (final step)
        if output_format != "Same as input" and "format_convert" in processing_steps:
            steps.append(WorkflowStep(
                step_id="format_convert",
                name="Format Conversion",
                command=f"python convert_format.py --input {{input_file}} --output {{output_file}} --format {output_format}",
                dependencies=[prev_step] if prev_step else [],
                resource_requirements=ResourceRequirement(
                    cpu_cores=int(cpu_cores) // 2,
                    memory_gb=float(memory_gb) // 2,
                    disk_space_gb=float(disk_space_gb)
                )
            ))
        
        # Create workflow
        parameters = {
            "input_location": input_location,
            "output_location": output_location,
            "input_format": input_format,
            "output_format": output_format
        }
        
        workflow = WorkflowEntity(
            workflow_id=workflow_id,
            name=workflow_name,
            description=f"Climate data preprocessing workflow for {input_format} data",
            workflow_type=WorkflowType.DATA_PREPROCESSING,
            steps=steps,
            parameters=parameters,
            tags={"climate", "preprocessing", input_format.lower()},
            status=WorkflowStatus.DRAFT
        )
        
        return workflow


class ESMModelExecutionWizard(WorkflowWizard):
    """Wizard for creating Earth System Model execution workflows."""
    
    def __init__(self):
        super().__init__()
        self.workflow_type = WorkflowType.MODEL_EXECUTION
    
    def run(self) -> Optional[WorkflowEntity]:
        console.print(Panel(
            "[bold green]Earth System Model Execution Wizard[/bold green]\n"
            "This wizard will help you create a workflow for running Earth System Models\n"
            "including environment setup, model compilation, execution, and output verification.",
            title="ESM Workflow Wizard",
            border_style="green"
        ))
        
        # Basic information
        workflow_id = questionary.text(
            "Workflow ID (e.g., 'fesom2-pi-control'):",
            validate=lambda x: len(x) > 0 or "Workflow ID cannot be empty"
        ).ask()
        
        if not workflow_id:
            return None
        
        # Model configuration
        model_name = questionary.select(
            "Earth System Model:",
            choices=["FESOM2", "AWI-CM", "ICON", "CESM", "MOM6", "Custom"]
        ).ask()
        
        if model_name == "Custom":
            model_name = questionary.text("Custom model name:").ask()
        
        experiment_type = questionary.select(
            "Experiment type:",
            choices=[
                "Control run", 
                "Historical simulation",
                "Climate projection",
                "Sensitivity experiment",
                "Idealized experiment"
            ]
        ).ask()
        
        # Execution environment
        execution_env = questionary.select(
            "Execution environment:",
            choices=["Local workstation", "HPC cluster", "Cloud computing", "Container"]
        ).ask()
        
        # Resource requirements
        if execution_env in ["HPC cluster", "Cloud computing"]:
            cpu_cores = questionary.text("CPU cores:", default="128").ask()
            memory_gb = questionary.text("Memory (GB):", default="256").ask()
            walltime = questionary.text("Walltime (hours):", default="24").ask()
            
            use_gpu = questionary.confirm("Use GPU acceleration?", default=False).ask()
            gpu_count = "0"
            if use_gpu:
                gpu_count = questionary.text("Number of GPUs:", default="4").ask()
        else:
            cpu_cores = "4"
            memory_gb = "16"
            walltime = "8"
            gpu_count = "0"
        
        # Model paths and configuration
        model_source = questionary.text(
            "Model source code location:",
            default="/models/" + model_name.lower()
        ).ask()
        
        run_directory = questionary.text(
            "Run directory:",
            default="/runs/" + workflow_id
        ).ask()
        
        # Build workflow steps
        steps = []
        
        # Environment setup
        steps.append(WorkflowStep(
            step_id="setup_environment",
            name="Setup Model Environment",
            command=f"module load {model_name.lower()} && setup_model_env.sh {run_directory}",
            dependencies=[],
            resource_requirements=ResourceRequirement(
                cpu_cores=1,
                memory_gb=4,
                disk_space_gb=10
            )
        ))
        
        # Model compilation (if needed)
        compile_model = questionary.confirm("Compile model from source?", default=True).ask()
        
        if compile_model:
            steps.append(WorkflowStep(
                step_id="compile_model",
                name="Compile Model",
                command=f"cd {model_source} && make clean && make -j{int(cpu_cores)//4} {model_name.lower()}",
                dependencies=["setup_environment"],
                resource_requirements=ResourceRequirement(
                    cpu_cores=int(cpu_cores) // 2,
                    memory_gb=float(memory_gb) // 4,
                    disk_space_gb=50
                )
            ))
        
        # Model execution
        prev_step = "compile_model" if compile_model else "setup_environment"
        
        steps.append(WorkflowStep(
            step_id="run_model",
            name="Execute Model",
            command=f"cd {run_directory} && mpirun -n {cpu_cores} {model_name.lower()} namelist.{model_name.lower()}",
            dependencies=[prev_step],
            resource_requirements=ResourceRequirement(
                cpu_cores=int(cpu_cores),
                memory_gb=float(memory_gb),
                gpu_count=int(gpu_count),
                disk_space_gb=1000  # Large for model output
            )
        ))
        
        # Output verification
        steps.append(WorkflowStep(
            step_id="verify_output",
            name="Verify Model Output",
            command=f"python verify_esm_output.py --run_dir {run_directory} --model {model_name}",
            dependencies=["run_model"],
            resource_requirements=ResourceRequirement(
                cpu_cores=2,
                memory_gb=8,
                disk_space_gb=20
            )
        ))
        
        # Archive results
        archive_results = questionary.confirm("Archive results after completion?", default=True).ask()
        
        if archive_results:
            archive_location = questionary.text(
                "Archive location:",
                default="/archive/" + workflow_id
            ).ask()
            
            steps.append(WorkflowStep(
                step_id="archive_results",
                name="Archive Results",
                command=f"python archive_esm_results.py --source {run_directory} --target {archive_location}",
                dependencies=["verify_output"],
                resource_requirements=ResourceRequirement(
                    cpu_cores=2,
                    memory_gb=8,
                    disk_space_gb=100
                )
            ))
        
        # Create workflow
        parameters = {
            "model_name": model_name,
            "model_source": model_source,
            "run_directory": run_directory,
            "experiment_type": experiment_type,
            "execution_environment": execution_env,
            "cpu_cores": int(cpu_cores),
            "memory_gb": float(memory_gb),
            "walltime_hours": float(walltime)
        }
        
        workflow = WorkflowEntity(
            workflow_id=workflow_id,
            name=f"{model_name} {experiment_type} - {workflow_id}",
            description=f"{experiment_type} using {model_name} on {execution_env}",
            workflow_type=WorkflowType.MODEL_EXECUTION,
            steps=steps,
            parameters=parameters,
            tags={"esm", model_name.lower(), experiment_type.lower().replace(" ", "_")},
            status=WorkflowStatus.DRAFT
        )
        
        return workflow


class ClimateAnalysisWizard(WorkflowWizard):
    """Wizard for creating climate analysis workflows."""
    
    def __init__(self):
        super().__init__()
        self.workflow_type = WorkflowType.DATA_ANALYSIS
    
    def run(self) -> Optional[WorkflowEntity]:
        console.print(Panel(
            "[bold cyan]Climate Analysis Wizard[/bold cyan]\n"
            "This wizard will help you create a workflow for climate data analysis\n"
            "including statistical analysis, visualization, and model evaluation.",
            title="Analysis Workflow Wizard",
            border_style="cyan"
        ))
        
        # Basic information
        workflow_id = questionary.text(
            "Workflow ID (e.g., 'cmip6-temperature-trends'):",
            validate=lambda x: len(x) > 0 or "Workflow ID cannot be empty"
        ).ask()
        
        if not workflow_id:
            return None
        
        # Analysis type
        analysis_types = questionary.checkbox(
            "Select analysis types:",
            choices=[
                questionary.Choice("Statistical analysis (trends, correlations)", "statistics"),
                questionary.Choice("Climate indices calculation", "indices"),
                questionary.Choice("Model evaluation and comparison", "evaluation"),
                questionary.Choice("Time series analysis", "timeseries"),
                questionary.Choice("Spatial pattern analysis", "spatial"),
                questionary.Choice("Extreme events analysis", "extremes"),
                questionary.Choice("Data visualization", "visualization"),
            ]
        ).ask()
        
        # Data configuration
        input_data = questionary.text(
            "Input data location:",
            default="/data/climate"
        ).ask()
        
        variables = questionary.text(
            "Variables to analyze (comma-separated):",
            default="tas,pr"
        ).ask()
        
        time_period = questionary.text(
            "Time period (YYYY-YYYY):",
            default="1980-2020"
        ).ask()
        
        # Build workflow steps
        steps = []
        
        # Data loading and preprocessing
        steps.append(WorkflowStep(
            step_id="load_data",
            name="Load and Preprocess Data",
            command=f"python load_climate_data.py --input {input_data} --vars {variables} --period {time_period}",
            dependencies=[],
            resource_requirements=ResourceRequirement(
                cpu_cores=4,
                memory_gb=16,
                disk_space_gb=50
            )
        ))
        
        prev_step = "load_data"
        
        # Analysis steps based on selection
        if "statistics" in analysis_types:
            steps.append(WorkflowStep(
                step_id="statistical_analysis",
                name="Statistical Analysis",
                command="python climate_statistics.py --input {{preprocessed_data}} --output {{stats_results}}",
                dependencies=[prev_step],
                resource_requirements=ResourceRequirement(
                    cpu_cores=8,
                    memory_gb=32,
                    disk_space_gb=20
                )
            ))
        
        if "indices" in analysis_types:
            indices_list = questionary.text(
                "Climate indices to calculate (e.g., ENSO, NAO, IOD):",
                default="ENSO,NAO"
            ).ask()
            
            steps.append(WorkflowStep(
                step_id="calculate_indices",
                name="Calculate Climate Indices",
                command=f"python climate_indices.py --indices {indices_list} --input {{data}} --output {{indices_output}}",
                dependencies=[prev_step],
                resource_requirements=ResourceRequirement(
                    cpu_cores=4,
                    memory_gb=16,
                    disk_space_gb=10
                )
            ))
        
        if "evaluation" in analysis_types:
            reference_data = questionary.text(
                "Reference dataset for evaluation:",
                default="/data/observations"
            ).ask()
            
            steps.append(WorkflowStep(
                step_id="model_evaluation",
                name="Model Evaluation",
                command=f"python model_evaluation.py --model {{data}} --obs {reference_data} --output {{eval_results}}",
                dependencies=[prev_step],
                resource_requirements=ResourceRequirement(
                    cpu_cores=6,
                    memory_gb=24,
                    disk_space_gb=30
                )
            ))
        
        if "visualization" in analysis_types:
            plot_types = questionary.checkbox(
                "Select plot types:",
                choices=["Time series", "Spatial maps", "Correlation plots", "Box plots", "Scatter plots"]
            ).ask()
            
            steps.append(WorkflowStep(
                step_id="create_visualizations",
                name="Create Visualizations",
                command=f"python climate_visualization.py --plots {','.join(plot_types)} --input {{analysis_results}} --output {{plots_dir}}",
                dependencies=[prev_step],
                resource_requirements=ResourceRequirement(
                    cpu_cores=2,
                    memory_gb=8,
                    disk_space_gb=15
                )
            ))
        
        # Final report generation
        steps.append(WorkflowStep(
            step_id="generate_report",
            name="Generate Analysis Report",
            command="python generate_climate_report.py --results {{all_results}} --output {{final_report}}",
            dependencies=[step.step_id for step in steps],  # Depends on all analysis steps
            resource_requirements=ResourceRequirement(
                cpu_cores=2,
                memory_gb=4,
                disk_space_gb=5
            )
        ))
        
        # Create workflow
        parameters = {
            "input_data": input_data,
            "variables": variables,
            "time_period": time_period,
            "analysis_types": analysis_types
        }
        
        workflow = WorkflowEntity(
            workflow_id=workflow_id,
            name=f"Climate Analysis - {workflow_id}",
            description=f"Climate analysis workflow for {variables} over {time_period}",
            workflow_type=WorkflowType.DATA_ANALYSIS,
            steps=steps,
            parameters=parameters,
            tags={"climate", "analysis"} | set(analysis_types),
            status=WorkflowStatus.DRAFT
        )
        
        return workflow


# Wizard registry
WORKFLOW_WIZARDS = {
    "climate-preprocessing": ClimateDataPreprocessingWizard,
    "esm-execution": ESMModelExecutionWizard,
    "climate-analysis": ClimateAnalysisWizard,
}


def get_available_wizards() -> Dict[str, str]:
    """Get available workflow wizards."""
    return {
        "climate-preprocessing": "Climate Data Preprocessing",
        "esm-execution": "Earth System Model Execution", 
        "climate-analysis": "Climate Data Analysis"
    }


def run_wizard(wizard_type: str) -> Optional[WorkflowEntity]:
    """Run a specific workflow wizard."""
    if wizard_type not in WORKFLOW_WIZARDS:
        return None
    
    wizard_class = WORKFLOW_WIZARDS[wizard_type]
    wizard = wizard_class()
    return wizard.run()