#!/usr/bin/env python3
"""
Example usage of the Tellus Workflow Runner System.

This example demonstrates how to use the comprehensive workflow system
for Earth science computing, including:
- Setting up workflow services
- Creating and executing workflows from templates
- Managing workflow runs and monitoring progress
- Integrating with location and archive systems
"""

import logging
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from tellus.application.service_factory import ApplicationServiceFactory, WorkflowCoordinator
from tellus.application.services.workflow_service import WorkflowApplicationService
from tellus.application.services.workflow_execution_service import WorkflowExecutionService
from tellus.infrastructure.repositories.json_workflow_repository import (
    JsonWorkflowRepository, JsonWorkflowRunRepository, JsonWorkflowTemplateRepository
)
from tellus.infrastructure.repositories.json_simulation_repository import JsonSimulationRepository
from tellus.infrastructure.repositories.json_location_repository import JsonLocationRepository
from tellus.infrastructure.adapters.workflow_engines import SnakemakeWorkflowEngine, PythonWorkflowEngine
from tellus.domain.entities.workflow import WorkflowEngine
from tellus.progress import ProgressTracker
from tellus.workflow_templates import get_all_earth_science_templates

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_workflow_system():
    """Create and configure the complete workflow system."""
    
    # Create repositories
    simulation_repo = JsonSimulationRepository("simulations.json")
    location_repo = JsonLocationRepository("locations.json")
    workflow_repo = JsonWorkflowRepository("workflows.json")
    workflow_run_repo = JsonWorkflowRunRepository("workflow_runs.json")
    workflow_template_repo = JsonWorkflowTemplateRepository("workflow_templates.json")
    
    # Create workflow engines
    workflow_engines = {
        WorkflowEngine.SNAKEMAKE: SnakemakeWorkflowEngine(),
        WorkflowEngine.PYTHON: PythonWorkflowEngine()
    }
    
    # Create progress tracker
    progress_tracker = ProgressTracker()
    
    # Create thread pool for workflow execution
    workflow_executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="workflow-")
    
    # Create service factory
    factory = ApplicationServiceFactory(
        simulation_repository=simulation_repo,
        location_repository=location_repo,
        workflow_repository=workflow_repo,
        workflow_run_repository=workflow_run_repo,
        workflow_template_repository=workflow_template_repo,
        workflow_engines=workflow_engines,
        progress_tracker=progress_tracker,
        workflow_executor=workflow_executor
    )
    
    return factory


def setup_earth_science_templates(factory):
    """Set up pre-defined Earth science workflow templates."""
    
    logger.info("Setting up Earth science workflow templates")
    
    template_service = factory.workflow_service
    templates = get_all_earth_science_templates()
    
    created_templates = []
    
    for template_id, template_entity in templates.items():
        try:
            # Check if template already exists
            if not template_service._template_repo.exists(template_id):
                template_service._template_repo.save(template_entity)
                created_templates.append(template_id)
                logger.info(f"Created template: {template_id}")
            else:
                logger.info(f"Template already exists: {template_id}")
                
        except Exception as e:
            logger.error(f"Failed to create template {template_id}: {e}")
    
    logger.info(f"Template setup complete. Created {len(created_templates)} templates.")
    return created_templates


def example_climate_preprocessing_workflow():
    """Example: Climate data preprocessing workflow."""
    
    logger.info("=" * 60)
    logger.info("EXAMPLE: Climate Data Preprocessing Workflow")
    logger.info("=" * 60)
    
    # Create workflow system
    factory = create_workflow_system()
    setup_earth_science_templates(factory)
    
    # Create workflow coordinator
    coordinator = factory.create_workflow_coordinator()
    
    # Define workflow parameters
    workflow_parameters = {
        "input_dataset": "/data/climate/raw_model_output.nc",
        "output_directory": "/data/climate/processed",
        "variables": ["tas", "pr", "psl"],
        "spatial_bounds": {"lon_min": -180, "lon_max": 180, "lat_min": -90, "lat_max": 90},
        "time_range": {"start_year": 2000, "end_year": 2020},
        "regridding_target": "1x1_degree",
        "output_format": "netcdf",
        "compression_level": 6
    }
    
    # Define location context (where data processing should occur)
    location_context = {
        "input_data_location": "hpc-scratch",
        "output_data_location": "archive-storage", 
        "temporary_location": "local-temp"
    }
    
    # Submit workflow
    try:
        results = coordinator.submit_earth_science_workflow(
            template_id="climate_data_preprocessing",
            workflow_id="climate_preproc_2000_2020",
            parameters=workflow_parameters,
            location_context=location_context,
            execution_environment="hpc_cluster"
        )
        
        if results["execution_submitted"]:
            run_id = results["run_id"]
            logger.info(f"Workflow submitted successfully! Run ID: {run_id}")
            logger.info(f"Estimated duration: {results.get('estimated_duration_hours', 'unknown')} hours")
            
            # Monitor workflow progress
            monitor_results = coordinator.monitor_workflow_pipeline(run_id)
            logger.info(f"Monitoring status: {monitor_results}")
            
        else:
            logger.error("Workflow submission failed:")
            for error in results.get("errors", []):
                logger.error(f"  - {error}")
                
    except Exception as e:
        logger.error(f"Workflow execution failed: {e}")


def example_esm_model_run_workflow():
    """Example: Earth System Model execution workflow."""
    
    logger.info("=" * 60)
    logger.info("EXAMPLE: Earth System Model Run Workflow")
    logger.info("=" * 60)
    
    # Create workflow system
    factory = create_workflow_system()
    setup_earth_science_templates(factory)
    
    # Create workflow coordinator
    coordinator = factory.create_workflow_coordinator()
    
    # Define model run parameters
    workflow_parameters = {
        "model_name": "FESOM2",
        "experiment_id": "historical_r1i1p1f1",
        "initial_conditions": "/data/initial_conditions/FESOM2_init.nc",
        "forcing_data": "/data/forcing/historical_forcing/",
        "run_duration": {"years": 5, "months": 0, "days": 0},
        "resolution": "T127L47",
        "compute_nodes": 16,
        "walltime_hours": 48,
        "output_directory": "/data/experiments/FESOM2_historical",
        "restart_frequency": "yearly",
        "output_variables": ["temperature", "salinity", "velocity"],
        "debug_level": 1
    }
    
    # Define location context
    location_context = {
        "model_input_location": "hpc-storage",
        "model_output_location": "hpc-scratch",
        "archive_location": "tape-storage",
        "execution_location": "hpc-compute"
    }
    
    # Submit workflow
    try:
        results = coordinator.submit_earth_science_workflow(
            template_id="esm_model_run",
            workflow_id="FESOM2_historical_5yr",
            parameters=workflow_parameters,
            location_context=location_context,
            execution_environment="hpc_cluster"
        )
        
        if results["execution_submitted"]:
            run_id = results["run_id"]
            logger.info(f"ESM model run submitted! Run ID: {run_id}")
            logger.info(f"Estimated duration: {results.get('estimated_duration_hours', 48)} hours")
            
            # Set up monitoring with output archival
            monitor_results = coordinator.monitor_workflow_pipeline(
                run_id, 
                archive_outputs=True
            )
            logger.info(f"Model run monitoring: {monitor_results}")
            
        else:
            logger.error("ESM model run submission failed:")
            for error in results.get("errors", []):
                logger.error(f"  - {error}")
                
    except Exception as e:
        logger.error(f"ESM model workflow failed: {e}")


def example_climate_analysis_workflow():
    """Example: Climate analysis workflow."""
    
    logger.info("=" * 60)
    logger.info("EXAMPLE: Climate Analysis Workflow")  
    logger.info("=" * 60)
    
    # Create workflow system
    factory = create_workflow_system()
    setup_earth_science_templates(factory)
    
    # Create workflow coordinator
    coordinator = factory.create_workflow_coordinator()
    
    # Define analysis parameters
    workflow_parameters = {
        "model_output": "/data/experiments/FESOM2_historical/output/",
        "analysis_variables": ["temperature", "salinity"],
        "analysis_period": {"start_year": 2000, "end_year": 2020},
        "reference_dataset": "/data/observations/WOA2018.nc",
        "analysis_regions": ["global", "arctic", "tropics"],
        "climate_indices": ["temp_anomaly", "precip_anomaly"],
        "statistical_tests": ["trend_analysis", "mean_bias", "correlation"],
        "output_directory": "/data/analysis/FESOM2_historical_analysis",
        "generate_plots": True,
        "plot_formats": ["png", "pdf"]
    }
    
    # Define location context
    location_context = {
        "model_data_location": "hpc-scratch",
        "reference_data_location": "archive-storage",
        "analysis_output_location": "local-results"
    }
    
    # Submit workflow
    try:
        results = coordinator.submit_earth_science_workflow(
            template_id="climate_analysis",
            workflow_id="FESOM2_historical_analysis_2000_2020",
            parameters=workflow_parameters,
            location_context=location_context,
            execution_environment="local"  # Analysis can run locally
        )
        
        if results["execution_submitted"]:
            run_id = results["run_id"]
            logger.info(f"Climate analysis submitted! Run ID: {run_id}")
            
            # Monitor analysis progress
            monitor_results = coordinator.monitor_workflow_pipeline(run_id)
            logger.info(f"Analysis monitoring: {monitor_results}")
            
        else:
            logger.error("Climate analysis submission failed:")
            for error in results.get("errors", []):
                logger.error(f"  - {error}")
                
    except Exception as e:
        logger.error(f"Climate analysis workflow failed: {e}")


def example_workflow_from_simulation():
    """Example: Create workflow from existing simulation."""
    
    logger.info("=" * 60)
    logger.info("EXAMPLE: Workflow from Existing Simulation")
    logger.info("=" * 60)
    
    # Create workflow system
    factory = create_workflow_system()
    setup_earth_science_templates(factory)
    
    # Create workflow coordinator
    coordinator = factory.create_workflow_coordinator()
    
    # This would create a workflow based on an existing simulation
    try:
        results = coordinator.create_workflow_from_simulation(
            simulation_id="FESOM2_historical_001",
            workflow_type="analysis",
            custom_parameters={"analysis_type": "detailed"}
        )
        
        logger.info(f"Workflow created from simulation: {results}")
        
    except Exception as e:
        logger.error(f"Failed to create workflow from simulation: {e}")


def example_workflow_monitoring():
    """Example: Monitor running workflows."""
    
    logger.info("=" * 60)
    logger.info("EXAMPLE: Workflow Monitoring")
    logger.info("=" * 60)
    
    # Create workflow system
    factory = create_workflow_system()
    
    # Get workflow execution service
    execution_service = factory.workflow_execution_service
    
    try:
        # List all running workflows
        running_runs = execution_service.list_workflow_runs(
            status="running" if hasattr(execution_service._run_repo, 'WorkflowStatus') else None,
            page=1,
            page_size=10
        )
        
        logger.info(f"Found {len(running_runs.runs)} running workflows")
        
        # Monitor each running workflow
        for run_dto in running_runs.runs:
            progress = execution_service.get_workflow_progress(run_dto.run_id)
            resource_usage = execution_service.get_resource_usage(run_dto.run_id)
            
            logger.info(f"Workflow {run_dto.run_id}:")
            logger.info(f"  Status: {progress.status}")
            logger.info(f"  Progress: {progress.progress:.1%}")
            logger.info(f"  Current Step: {progress.current_step}")
            logger.info(f"  Steps Completed: {progress.completed_steps}/{progress.total_steps}")
            logger.info(f"  Resource Usage: {resource_usage.cores_used} cores, {resource_usage.memory_gb_used}GB memory")
        
    except Exception as e:
        logger.error(f"Workflow monitoring failed: {e}")


def main():
    """Run workflow system examples."""
    
    logger.info("Starting Tellus Workflow System Examples")
    logger.info("="*60)
    
    try:
        # Run examples
        example_climate_preprocessing_workflow()
        example_esm_model_run_workflow()
        example_climate_analysis_workflow()
        example_workflow_from_simulation()
        example_workflow_monitoring()
        
        logger.info("="*60)
        logger.info("All workflow examples completed successfully!")
        
    except Exception as e:
        logger.error(f"Example execution failed: {e}")
    finally:
        logger.info("Shutting down workflow system")


if __name__ == "__main__":
    main()