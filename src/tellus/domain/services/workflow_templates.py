"""
Earth Science workflow templates for common computational patterns.

This module provides pre-defined workflow templates for typical Earth System Model
data processing workflows, including data preprocessing, model execution,
postprocessing, and analysis pipelines.
"""

from typing import Any, Dict, Set

from ..entities.workflow import WorkflowTemplateEntity


def create_climate_data_preprocessing_template() -> WorkflowTemplateEntity:
    """
    Create a template for climate data preprocessing workflows.
    
    This template handles common preprocessing tasks for Earth System Model data:
    - Data validation and quality control
    - Spatial and temporal subsetting
    - Unit conversions and variable standardization
    - Bias correction and downscaling
    - Output to analysis-ready formats
    """
    template_parameters = {
        "input_dataset": {
            "type": "string",
            "required": True,
            "description": "Path to input climate dataset (NetCDF format)"
        },
        "output_directory": {
            "type": "string", 
            "required": True,
            "description": "Directory for processed output files"
        },
        "variables": {
            "type": "list",
            "required": True,
            "description": "List of variables to process",
            "example": ["tas", "pr", "psl"]
        },
        "spatial_bounds": {
            "type": "dict",
            "required": False,
            "description": "Spatial bounds for subsetting (lon_min, lon_max, lat_min, lat_max)",
            "default": {"global": True}
        },
        "time_range": {
            "type": "dict",
            "required": False,
            "description": "Time range for temporal subsetting (start_year, end_year)",
            "default": {"full_period": True}
        },
        "regridding_target": {
            "type": "string",
            "required": False,
            "description": "Target grid specification for regridding",
            "default": "native"
        },
        "bias_correction_reference": {
            "type": "string", 
            "required": False,
            "description": "Reference dataset for bias correction",
            "default": null
        },
        "output_format": {
            "type": "string",
            "required": False,
            "description": "Output file format",
            "default": "netcdf",
            "choices": ["netcdf", "zarr", "csv"]
        },
        "compression_level": {
            "type": "integer",
            "required": False,
            "description": "Compression level (0-9)",
            "default": 6
        }
    }
    
    workflow_template = {
        "engine": "snakemake",
        "steps": [
            {
                "step_id": "validate_input",
                "name": "Validate Input Data",
                "script_path": "scripts/validate_climate_data.py",
                "input_files": ["{input_dataset}"],
                "output_files": ["{output_directory}/validation_report.json"],
                "parameters": {
                    "variables": "{variables}",
                    "check_completeness": True,
                    "check_metadata": True
                },
                "resource_requirements": {
                    "cores": 1,
                    "memory_gb": 4,
                    "walltime_hours": 0.5
                }
            },
            {
                "step_id": "spatial_subset", 
                "name": "Spatial Subsetting",
                "script_path": "scripts/spatial_subset.py",
                "input_files": ["{input_dataset}"],
                "output_files": ["{output_directory}/spatially_subsetted.nc"],
                "parameters": {
                    "variables": "{variables}",
                    "bounds": "{spatial_bounds}"
                },
                "dependencies": ["validate_input"],
                "resource_requirements": {
                    "cores": 2,
                    "memory_gb": 8,
                    "walltime_hours": 2.0
                }
            },
            {
                "step_id": "temporal_subset",
                "name": "Temporal Subsetting", 
                "script_path": "scripts/temporal_subset.py",
                "input_files": ["{output_directory}/spatially_subsetted.nc"],
                "output_files": ["{output_directory}/temporally_subsetted.nc"],
                "parameters": {
                    "time_range": "{time_range}",
                    "calendar_handling": "standard"
                },
                "dependencies": ["spatial_subset"],
                "resource_requirements": {
                    "cores": 2,
                    "memory_gb": 6,
                    "walltime_hours": 1.5
                }
            },
            {
                "step_id": "regridding",
                "name": "Grid Transformation",
                "script_path": "scripts/regrid_data.py", 
                "input_files": ["{output_directory}/temporally_subsetted.nc"],
                "output_files": ["{output_directory}/regridded.nc"],
                "parameters": {
                    "target_grid": "{regridding_target}",
                    "method": "conservative",
                    "preserve_metadata": True
                },
                "dependencies": ["temporal_subset"],
                "resource_requirements": {
                    "cores": 4,
                    "memory_gb": 16,
                    "walltime_hours": 4.0
                }
            },
            {
                "step_id": "bias_correction",
                "name": "Bias Correction",
                "script_path": "scripts/bias_correct.py",
                "input_files": [
                    "{output_directory}/regridded.nc",
                    "{bias_correction_reference}"
                ],
                "output_files": ["{output_directory}/bias_corrected.nc"],
                "parameters": {
                    "method": "quantile_mapping",
                    "window_days": 31
                },
                "dependencies": ["regridding"],
                "resource_requirements": {
                    "cores": 8,
                    "memory_gb": 32,
                    "walltime_hours": 8.0
                },
                "conditional": "{bias_correction_reference} is not None"
            },
            {
                "step_id": "format_conversion",
                "name": "Output Format Conversion",
                "script_path": "scripts/convert_format.py",
                "input_files": ["{output_directory}/bias_corrected.nc"],
                "output_files": ["{output_directory}/final_output.{output_format}"],
                "parameters": {
                    "format": "{output_format}",
                    "compression": "{compression_level}",
                    "metadata_update": True
                },
                "dependencies": ["bias_correction"],
                "resource_requirements": {
                    "cores": 2,
                    "memory_gb": 8,
                    "walltime_hours": 2.0
                }
            }
        ]
    }
    
    return WorkflowTemplateEntity(
        template_id="climate_data_preprocessing",
        name="Climate Data Preprocessing Pipeline",
        description="Comprehensive preprocessing workflow for Earth System Model climate data",
        category="preprocessing",
        template_parameters=template_parameters,
        workflow_template=workflow_template,
        version="1.0",
        author="Tellus Earth Science Team",
        tags={"climate", "preprocessing", "netcdf", "quality-control", "regridding"}
    )


def create_esm_model_run_template() -> WorkflowTemplateEntity:
    """
    Create a template for Earth System Model execution workflows.
    
    This template handles typical ESM model runs:
    - Environment setup and dependency checks
    - Initial conditions preparation
    - Model configuration and namelist generation
    - Model execution with monitoring
    - Output verification and archival
    """
    template_parameters = {
        "model_name": {
            "type": "string",
            "required": True,
            "description": "Name of the Earth System Model",
            "choices": ["FESOM2", "ICON", "ECHAM6", "MPI-ESM", "CESM"]
        },
        "experiment_id": {
            "type": "string",
            "required": True,
            "description": "Unique identifier for the experiment"
        },
        "initial_conditions": {
            "type": "string",
            "required": True,
            "description": "Path to initial conditions directory or file"
        },
        "forcing_data": {
            "type": "string",
            "required": True,
            "description": "Path to forcing data directory"
        },
        "run_duration": {
            "type": "dict",
            "required": True,
            "description": "Simulation duration specification",
            "example": {"years": 10, "months": 0, "days": 0}
        },
        "resolution": {
            "type": "string",
            "required": True,
            "description": "Model resolution specification",
            "example": "T127L47"
        },
        "compute_nodes": {
            "type": "integer",
            "required": True,
            "description": "Number of compute nodes to request"
        },
        "walltime_hours": {
            "type": "integer",
            "required": True,
            "description": "Maximum walltime in hours"
        },
        "output_directory": {
            "type": "string",
            "required": True,
            "description": "Directory for model output"
        },
        "restart_frequency": {
            "type": "string",
            "required": False,
            "description": "Restart file output frequency",
            "default": "monthly"
        },
        "output_variables": {
            "type": "list",
            "required": False,
            "description": "List of variables to output",
            "default": ["standard"]
        },
        "debug_level": {
            "type": "integer",
            "required": False,
            "description": "Debug output level (0-3)",
            "default": 1
        }
    }
    
    workflow_template = {
        "engine": "snakemake",
        "steps": [
            {
                "step_id": "setup_environment",
                "name": "Setup Model Environment",
                "script_path": "scripts/setup_esm_environment.py",
                "output_files": [
                    "{output_directory}/environment_setup.log",
                    "{output_directory}/module_list.txt"
                ],
                "parameters": {
                    "model_name": "{model_name}",
                    "check_dependencies": True,
                    "setup_paths": True
                },
                "resource_requirements": {
                    "cores": 1,
                    "memory_gb": 2,
                    "walltime_hours": 0.5
                }
            },
            {
                "step_id": "prepare_initial_conditions",
                "name": "Prepare Initial Conditions",
                "script_path": "scripts/prepare_initial_conditions.py",
                "input_files": ["{initial_conditions}"],
                "output_files": ["{output_directory}/input/initial_conditions_prepared.nc"],
                "parameters": {
                    "model_name": "{model_name}",
                    "resolution": "{resolution}",
                    "target_date": "auto"
                },
                "dependencies": ["setup_environment"],
                "resource_requirements": {
                    "cores": 4,
                    "memory_gb": 16,
                    "walltime_hours": 2.0
                }
            },
            {
                "step_id": "prepare_forcing",
                "name": "Prepare Forcing Data", 
                "script_path": "scripts/prepare_forcing_data.py",
                "input_files": ["{forcing_data}"],
                "output_files": ["{output_directory}/input/forcing_data_prepared/"],
                "parameters": {
                    "model_name": "{model_name}",
                    "resolution": "{resolution}",
                    "run_duration": "{run_duration}"
                },
                "dependencies": ["setup_environment"],
                "resource_requirements": {
                    "cores": 8,
                    "memory_gb": 32,
                    "walltime_hours": 4.0
                }
            },
            {
                "step_id": "generate_namelists",
                "name": "Generate Model Configuration",
                "script_path": "scripts/generate_namelists.py",
                "output_files": [
                    "{output_directory}/config/namelist.{model_name}",
                    "{output_directory}/config/run_script.sh"
                ],
                "parameters": {
                    "model_name": "{model_name}",
                    "experiment_id": "{experiment_id}",
                    "resolution": "{resolution}",
                    "run_duration": "{run_duration}",
                    "compute_nodes": "{compute_nodes}",
                    "output_variables": "{output_variables}",
                    "restart_frequency": "{restart_frequency}",
                    "debug_level": "{debug_level}"
                },
                "dependencies": ["prepare_initial_conditions", "prepare_forcing"],
                "resource_requirements": {
                    "cores": 1,
                    "memory_gb": 4,
                    "walltime_hours": 0.5
                }
            },
            {
                "step_id": "run_model",
                "name": "Execute Earth System Model",
                "script_path": "scripts/run_esm_model.py",
                "input_files": [
                    "{output_directory}/config/run_script.sh",
                    "{output_directory}/input/initial_conditions_prepared.nc"
                ],
                "output_files": [
                    "{output_directory}/output/",
                    "{output_directory}/restart/",
                    "{output_directory}/logs/model_output.log"
                ],
                "parameters": {
                    "model_name": "{model_name}",
                    "experiment_id": "{experiment_id}",
                    "monitor_progress": True,
                    "save_intermediate": True
                },
                "dependencies": ["generate_namelists"],
                "resource_requirements": {
                    "cores": "{compute_nodes} * 48",  # Assuming 48 cores per node
                    "memory_gb": "{compute_nodes} * 192",  # Assuming 192GB per node
                    "walltime_hours": "{walltime_hours}",
                    "queue_name": "compute"
                }
            },
            {
                "step_id": "verify_output",
                "name": "Verify Model Output",
                "script_path": "scripts/verify_esm_output.py",
                "input_files": ["{output_directory}/output/"],
                "output_files": [
                    "{output_directory}/verification_report.json",
                    "{output_directory}/output_summary.txt"
                ],
                "parameters": {
                    "model_name": "{model_name}",
                    "expected_variables": "{output_variables}",
                    "check_completeness": True,
                    "check_physical_realism": True
                },
                "dependencies": ["run_model"],
                "resource_requirements": {
                    "cores": 4,
                    "memory_gb": 16,
                    "walltime_hours": 2.0
                }
            },
            {
                "step_id": "archive_output",
                "name": "Archive Model Output",
                "script_path": "scripts/archive_esm_output.py",
                "input_files": ["{output_directory}/output/"],
                "output_files": [
                    "{output_directory}/archived/{experiment_id}_output.tar.gz",
                    "{output_directory}/archived/metadata.json"
                ],
                "parameters": {
                    "experiment_id": "{experiment_id}",
                    "compression_level": 6,
                    "include_restarts": False,
                    "generate_metadata": True
                },
                "dependencies": ["verify_output"],
                "resource_requirements": {
                    "cores": 2,
                    "memory_gb": 8,
                    "walltime_hours": 4.0
                }
            }
        ]
    }
    
    return WorkflowTemplateEntity(
        template_id="esm_model_run",
        name="Earth System Model Execution Pipeline",
        description="Complete workflow for executing Earth System Model simulations",
        category="modeling",
        template_parameters=template_parameters,
        workflow_template=workflow_template,
        version="1.0",
        author="Tellus Earth Science Team",
        tags={"esm", "modeling", "simulation", "climate", "hpc"}
    )


def create_climate_analysis_template() -> WorkflowTemplateEntity:
    """
    Create a template for climate data analysis workflows.
    
    This template handles post-processing analysis of climate model output:
    - Statistical analysis and trend detection
    - Spatial and temporal aggregations
    - Climate indices calculation
    - Visualization and reporting
    - Comparison with observations
    """
    template_parameters = {
        "model_output": {
            "type": "string",
            "required": True,
            "description": "Path to model output data directory"
        },
        "analysis_variables": {
            "type": "list",
            "required": True,
            "description": "Variables to analyze",
            "example": ["tas", "pr", "psl"]
        },
        "analysis_period": {
            "type": "dict",
            "required": True,
            "description": "Time period for analysis",
            "example": {"start_year": 2000, "end_year": 2020}
        },
        "reference_dataset": {
            "type": "string",
            "required": False,
            "description": "Path to observational reference dataset",
            "default": None
        },
        "analysis_regions": {
            "type": "list",
            "required": False,
            "description": "Named regions for regional analysis",
            "default": ["global", "arctic", "tropics"],
            "choices": ["global", "arctic", "tropics", "nhemisphere", "shemisphere", "custom"]
        },
        "climate_indices": {
            "type": "list",
            "required": False,
            "description": "Climate indices to calculate",
            "default": ["temp_anomaly", "precip_anomaly"],
            "choices": ["temp_anomaly", "precip_anomaly", "heat_days", "frost_days", "dry_days", "wet_days"]
        },
        "statistical_tests": {
            "type": "list",
            "required": False,
            "description": "Statistical tests to perform",
            "default": ["trend_analysis", "mean_bias"],
            "choices": ["trend_analysis", "mean_bias", "correlation", "rmse", "pattern_correlation"]
        },
        "output_directory": {
            "type": "string",
            "required": True,
            "description": "Directory for analysis results"
        },
        "generate_plots": {
            "type": "boolean",
            "required": False,
            "description": "Generate visualization plots",
            "default": True
        },
        "plot_formats": {
            "type": "list",
            "required": False,
            "description": "Output formats for plots",
            "default": ["png", "pdf"],
            "choices": ["png", "pdf", "svg", "eps"]
        }
    }
    
    workflow_template = {
        "engine": "snakemake",
        "steps": [
            {
                "step_id": "load_and_validate",
                "name": "Load and Validate Data",
                "script_path": "scripts/load_climate_data.py",
                "input_files": ["{model_output}"],
                "output_files": [
                    "{output_directory}/data_inventory.json",
                    "{output_directory}/validation_report.json"
                ],
                "parameters": {
                    "variables": "{analysis_variables}",
                    "period": "{analysis_period}",
                    "quality_checks": True
                },
                "resource_requirements": {
                    "cores": 4,
                    "memory_gb": 16,
                    "walltime_hours": 2.0
                }
            },
            {
                "step_id": "temporal_statistics",
                "name": "Calculate Temporal Statistics",
                "script_path": "scripts/temporal_statistics.py",
                "input_files": ["{model_output}"],
                "output_files": [
                    "{output_directory}/temporal_means.nc",
                    "{output_directory}/temporal_trends.nc",
                    "{output_directory}/temporal_variability.nc"
                ],
                "parameters": {
                    "variables": "{analysis_variables}",
                    "period": "{analysis_period}",
                    "aggregations": ["annual", "seasonal", "monthly"]
                },
                "dependencies": ["load_and_validate"],
                "resource_requirements": {
                    "cores": 8,
                    "memory_gb": 32,
                    "walltime_hours": 4.0
                }
            },
            {
                "step_id": "spatial_statistics", 
                "name": "Calculate Spatial Statistics",
                "script_path": "scripts/spatial_statistics.py",
                "input_files": ["{output_directory}/temporal_means.nc"],
                "output_files": [
                    "{output_directory}/regional_means.nc",
                    "{output_directory}/spatial_patterns.nc"
                ],
                "parameters": {
                    "variables": "{analysis_variables}",
                    "regions": "{analysis_regions}",
                    "include_zonal_means": True
                },
                "dependencies": ["temporal_statistics"],
                "resource_requirements": {
                    "cores": 6,
                    "memory_gb": 24,
                    "walltime_hours": 3.0
                }
            },
            {
                "step_id": "climate_indices",
                "name": "Calculate Climate Indices", 
                "script_path": "scripts/climate_indices.py",
                "input_files": ["{model_output}"],
                "output_files": ["{output_directory}/climate_indices.nc"],
                "parameters": {
                    "variables": "{analysis_variables}",
                    "indices": "{climate_indices}",
                    "period": "{analysis_period}"
                },
                "dependencies": ["load_and_validate"],
                "resource_requirements": {
                    "cores": 4,
                    "memory_gb": 16,
                    "walltime_hours": 3.0
                }
            },
            {
                "step_id": "model_evaluation",
                "name": "Model-Observation Comparison",
                "script_path": "scripts/model_evaluation.py",
                "input_files": [
                    "{output_directory}/temporal_means.nc",
                    "{reference_dataset}"
                ],
                "output_files": [
                    "{output_directory}/evaluation_metrics.json",
                    "{output_directory}/bias_patterns.nc"
                ],
                "parameters": {
                    "variables": "{analysis_variables}",
                    "statistical_tests": "{statistical_tests}",
                    "regions": "{analysis_regions}"
                },
                "dependencies": ["temporal_statistics"],
                "resource_requirements": {
                    "cores": 6,
                    "memory_gb": 24,
                    "walltime_hours": 4.0
                },
                "conditional": "{reference_dataset} is not None"
            },
            {
                "step_id": "generate_visualizations",
                "name": "Generate Analysis Plots",
                "script_path": "scripts/generate_analysis_plots.py", 
                "input_files": [
                    "{output_directory}/temporal_means.nc",
                    "{output_directory}/regional_means.nc",
                    "{output_directory}/climate_indices.nc"
                ],
                "output_files": ["{output_directory}/plots/"],
                "parameters": {
                    "variables": "{analysis_variables}",
                    "plot_types": ["timeseries", "maps", "regional_comparison"],
                    "formats": "{plot_formats}",
                    "include_trends": True
                },
                "dependencies": ["temporal_statistics", "spatial_statistics", "climate_indices"],
                "resource_requirements": {
                    "cores": 2,
                    "memory_gb": 8,
                    "walltime_hours": 2.0
                },
                "conditional": "{generate_plots} == True"
            },
            {
                "step_id": "generate_report",
                "name": "Generate Analysis Report",
                "script_path": "scripts/generate_analysis_report.py",
                "input_files": [
                    "{output_directory}/evaluation_metrics.json",
                    "{output_directory}/data_inventory.json",
                    "{output_directory}/plots/"
                ],
                "output_files": [
                    "{output_directory}/analysis_report.html",
                    "{output_directory}/analysis_summary.json"
                ],
                "parameters": {
                    "variables": "{analysis_variables}",
                    "include_plots": "{generate_plots}",
                    "template": "standard_climate_analysis"
                },
                "dependencies": ["model_evaluation", "generate_visualizations"],
                "resource_requirements": {
                    "cores": 1,
                    "memory_gb": 4,
                    "walltime_hours": 1.0
                }
            }
        ]
    }
    
    return WorkflowTemplateEntity(
        template_id="climate_analysis",
        name="Climate Data Analysis Pipeline", 
        description="Comprehensive analysis workflow for climate model output and observations",
        category="analysis",
        template_parameters=template_parameters,
        workflow_template=workflow_template,
        version="1.0", 
        author="Tellus Earth Science Team",
        tags={"climate", "analysis", "statistics", "visualization", "evaluation"}
    )


def get_all_earth_science_templates() -> Dict[str, WorkflowTemplateEntity]:
    """
    Get all available Earth science workflow templates.
    
    Returns:
        Dictionary mapping template IDs to template entities
    """
    templates = {
        "climate_data_preprocessing": create_climate_data_preprocessing_template(),
        "esm_model_run": create_esm_model_run_template(),
        "climate_analysis": create_climate_analysis_template()
    }
    
    return templates