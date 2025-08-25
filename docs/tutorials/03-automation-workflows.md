# Automation Workflows Tutorial

## Introduction

This tutorial covers setting up automated workflows in Tellus for Earth System Model data processing. You'll learn to create sophisticated automation pipelines that handle quality control, data processing, file transfers, and monitoring with minimal manual intervention.

## Prerequisites

- Completed [Quickstart Tutorial](01-quickstart.md) and [Storage Setup Guide](02-storage-setup.md)
- Understanding of Earth System Model output data formats
- Basic familiarity with workflow automation concepts

## What You'll Learn

- Design automated quality control workflows
- Set up data processing pipelines with error handling
- Implement progress tracking and monitoring systems
- Create conditional workflows with decision logic
- Configure alerts and notifications for workflow events
- Build complex multi-stage automation workflows

## Workflow Architecture Overview

Tellus automation workflows follow a pipeline architecture:

```
┌─────────────┐    ┌──────────────┐    ┌─────────────┐    ┌──────────────┐
│   Trigger   │ -> │  Processing  │ -> │ Validation  │ -> │  Completion  │
│ (Data/Time) │    │  Pipeline    │    │  & QC       │    │  Actions     │
└─────────────┘    └──────────────┘    └─────────────┘    └──────────────┘
                           |                   |                   |
                           v                   v                   v
                    ┌─────────────┐    ┌──────────────┐    ┌──────────────┐
                    │   Error     │    │   Retry      │    │ Notification │
                    │  Handling   │    │  Logic       │    │   System     │
                    └─────────────┘    └──────────────┘    └──────────────┘
```

## Step 1: Setting Up the Workflow Environment

First, let's establish the foundation for our automation workflows.

```python
from tellus.application.container import ServiceContainer
from tellus.application.dtos import (
    CreateSimulationDto, CreateProgressTrackingDto, 
    BatchFileTransferOperationDto, CreateArchiveDto
)
from tellus.domain.entities.progress_tracking import OperationType
import json
from datetime import datetime, timedelta

# Initialize services
container = ServiceContainer()
simulation_service = container.get_simulation_service()
archive_service = container.get_archive_service() 
transfer_service = container.get_file_transfer_service()
progress_service = container.get_progress_tracking_service()

print("🔧 Tellus Automation Workflow System")
print("Initializing workflow management capabilities...")

# Define workflow configuration
workflow_config = {
    "max_concurrent_workflows": 10,
    "default_retry_attempts": 3,
    "progress_update_interval_seconds": 30,
    "workflow_timeout_hours": 24,
    "notification_channels": ["email", "slack"],
    "error_escalation_levels": ["warning", "error", "critical"],
    "supported_triggers": ["file_arrival", "schedule", "simulation_completion", "manual"]
}

print(f"✅ Workflow system configured:")
print(f"   Max concurrent workflows: {workflow_config['max_concurrent_workflows']}")
print(f"   Retry attempts: {workflow_config['default_retry_attempts']}")
print(f"   Supported triggers: {', '.join(workflow_config['supported_triggers'])}")
```

## Step 2: Quality Control Workflow

Create an automated quality control workflow that validates Earth System Model output.

```python
def create_qc_workflow():
    """Create automated quality control workflow for model output."""
    
    qc_workflow = {
        "workflow_id": "esm_quality_control_v1",
        "description": "Automated QC for Earth System Model output files",
        "trigger": {
            "type": "file_arrival",
            "pattern": "*.nc",
            "location": "hpc-scratch",
            "min_age_minutes": 5  # Wait 5 minutes to ensure file is complete
        },
        "stages": [
            {
                "stage_id": "file_validation",
                "description": "Basic file integrity and format validation",
                "timeout_minutes": 10,
                "tasks": [
                    {
                        "task": "check_file_integrity",
                        "tool": "ncdump",
                        "parameters": {
                            "header_only": True,
                            "validate_structure": True
                        },
                        "success_criteria": "valid_netcdf_structure"
                    },
                    {
                        "task": "verify_file_size",
                        "parameters": {
                            "minimum_size_mb": 1,
                            "maximum_size_gb": 50,
                            "expected_size_tolerance": 0.1
                        }
                    },
                    {
                        "task": "check_cf_compliance",
                        "tool": "cf-checker",
                        "parameters": {
                            "version": "1.8",
                            "severity_threshold": "warning"
                        }
                    }
                ]
            },
            {
                "stage_id": "metadata_validation", 
                "description": "Validate metadata completeness and accuracy",
                "timeout_minutes": 15,
                "tasks": [
                    {
                        "task": "required_attributes_check",
                        "required_global_attrs": [
                            "title", "institution", "source", "contact",
                            "creation_date", "experiment", "frequency"
                        ]
                    },
                    {
                        "task": "variable_metadata_check", 
                        "required_var_attrs": [
                            "standard_name", "long_name", "units"
                        ]
                    },
                    {
                        "task": "time_coordinate_validation",
                        "parameters": {
                            "check_calendar": True,
                            "validate_time_bounds": True,
                            "check_temporal_consistency": True
                        }
                    }
                ]
            },
            {
                "stage_id": "scientific_validation",
                "description": "Scientific content and physical consistency checks",
                "timeout_minutes": 30,
                "tasks": [
                    {
                        "task": "range_validation",
                        "parameters": {
                            "temperature_range_k": [180, 350],
                            "pressure_range_pa": [1000, 110000],
                            "precipitation_range_mm_day": [0, 500]
                        }
                    },
                    {
                        "task": "statistical_analysis",
                        "parameters": {
                            "compute_basic_stats": True,
                            "detect_outliers": True,
                            "outlier_threshold_sigma": 5
                        }
                    },
                    {
                        "task": "conservation_checks",
                        "parameters": {
                            "energy_balance_tolerance": 0.1,
                            "water_balance_tolerance": 0.01,
                            "mass_conservation_tolerance": 1e-10
                        }
                    }
                ]
            },
            {
                "stage_id": "completion_actions",
                "description": "Actions to take upon successful QC completion",
                "tasks": [
                    {
                        "task": "tag_file",
                        "parameters": {
                            "qc_status": "passed",
                            "qc_version": "v1.0",
                            "qc_timestamp": "auto_generated"
                        }
                    },
                    {
                        "task": "move_to_validated",
                        "destination_location": "hpc-work",
                        "destination_path": "validated/{simulation_id}/{filename}"
                    },
                    {
                        "task": "update_simulation_status",
                        "status_field": "qc_status",
                        "status_value": "passed"
                    }
                ]
            }
        ],
        "error_handling": {
            "retry_policy": "exponential_backoff",
            "max_retries": 3,
            "retry_delay_seconds": [60, 300, 900],  # 1min, 5min, 15min
            "failure_actions": [
                {
                    "action": "quarantine_file",
                    "location": "hpc-work/quarantine"
                },
                {
                    "action": "notify_administrators",
                    "channels": ["email", "slack"],
                    "severity": "warning"
                },
                {
                    "action": "create_failure_report",
                    "include_logs": True,
                    "include_file_metadata": True
                }
            ]
        },
        "notifications": {
            "on_success": {
                "enabled": True,
                "frequency": "daily_summary",
                "recipients": ["data_managers", "scientists"]
            },
            "on_failure": {
                "enabled": True,
                "frequency": "immediate",
                "recipients": ["data_managers", "system_admins"]
            }
        }
    }
    
    return qc_workflow

qc_workflow = create_qc_workflow()
print(f"\n🔍 Quality Control Workflow Created:")
print(f"   Workflow ID: {qc_workflow['workflow_id']}")
print(f"   Stages: {len(qc_workflow['stages'])}")
print(f"   Trigger: {qc_workflow['trigger']['type']} for {qc_workflow['trigger']['pattern']}")
print(f"   Max retries: {qc_workflow['error_handling']['max_retries']}")

# Display workflow stages
print(f"\n   📋 Workflow Stages:")
for i, stage in enumerate(qc_workflow['stages'], 1):
    print(f"      {i}. {stage['stage_id']}: {len(stage['tasks'])} tasks")
    print(f"         {stage['description']}")
```

## Step 3: Data Processing Pipeline

Create a comprehensive data processing pipeline for model output.

```python
def create_processing_pipeline():
    """Create automated data processing pipeline."""
    
    processing_pipeline = {
        "workflow_id": "esm_data_processing_v2",
        "description": "Automated processing of ESM output data",
        "trigger": {
            "type": "simulation_completion",
            "conditions": {
                "qc_status": "passed",
                "processing_status": "pending"
            }
        },
        "parallel_processing": {
            "enabled": True,
            "max_parallel_jobs": 4,
            "job_scheduler": "slurm",
            "resource_requirements": {
                "nodes": 1,
                "cores_per_node": 8,
                "memory_gb": 32,
                "walltime_hours": 6
            }
        },
        "stages": [
            {
                "stage_id": "data_preparation",
                "description": "Prepare data for processing",
                "parallelizable": False,
                "tasks": [
                    {
                        "task": "inventory_files",
                        "description": "Create inventory of files to process",
                        "parameters": {
                            "file_pattern": "*.nc",
                            "min_size_mb": 1,
                            "exclude_temp_files": True
                        }
                    },
                    {
                        "task": "check_dependencies",
                        "description": "Verify all required input files are available",
                        "parameters": {
                            "required_components": ["atm", "ocn", "lnd", "ice"],
                            "required_frequencies": ["mon", "day"],
                            "completeness_threshold": 0.95
                        }
                    },
                    {
                        "task": "allocate_resources",
                        "description": "Reserve compute resources for processing",
                        "parameters": {
                            "resource_type": "compute_nodes",
                            "duration_estimate": "4_hours"
                        }
                    }
                ]
            },
            {
                "stage_id": "regridding_and_remapping",
                "description": "Regrid data to standard grids",
                "parallelizable": True,
                "tasks": [
                    {
                        "task": "atmospheric_regridding",
                        "tool": "cdo",
                        "parameters": {
                            "target_grid": "r360x180",  # 1-degree grid
                            "method": "bilinear",
                            "preserve_area": True
                        },
                        "input_pattern": "atm_*.nc",
                        "output_pattern": "atm_regrid_*.nc"
                    },
                    {
                        "task": "ocean_regridding", 
                        "tool": "nco",
                        "parameters": {
                            "target_grid": "regular_1deg",
                            "method": "conservative",
                            "mask_handling": "preserve_land_sea"
                        },
                        "input_pattern": "ocn_*.nc",
                        "output_pattern": "ocn_regrid_*.nc"
                    },
                    {
                        "task": "land_processing",
                        "tool": "ncks",
                        "parameters": {
                            "variable_selection": ["TSOI", "QSOIL", "GPP", "NPP"],
                            "level_selection": "surface_only"
                        },
                        "input_pattern": "lnd_*.nc",
                        "output_pattern": "lnd_surface_*.nc"
                    }
                ]
            },
            {
                "stage_id": "temporal_processing",
                "description": "Compute temporal statistics and diagnostics",
                "parallelizable": True,
                "tasks": [
                    {
                        "task": "climatology_computation",
                        "description": "Compute 30-year climatological means",
                        "parameters": {
                            "time_period": "30_years",
                            "seasons": ["DJF", "MAM", "JJA", "SON"],
                            "include_annual": True
                        },
                        "output_suffix": "_clim.nc"
                    },
                    {
                        "task": "trend_analysis",
                        "description": "Compute linear trends",
                        "parameters": {
                            "method": "least_squares",
                            "confidence_level": 0.95,
                            "detrend_seasonal_cycle": True
                        },
                        "output_suffix": "_trend.nc"
                    },
                    {
                        "task": "extreme_indices",
                        "description": "Compute climate extreme indices",
                        "tool": "climdex",
                        "parameters": {
                            "temperature_indices": ["tx90p", "tn10p", "wsdi", "csdi"],
                            "precipitation_indices": ["rx1day", "rx5day", "cdd", "cwd"],
                            "base_period": "1961-1990"
                        },
                        "output_suffix": "_extremes.nc"
                    }
                ]
            },
            {
                "stage_id": "visualization_and_diagnostics",
                "description": "Create standard plots and diagnostic figures",
                "parallelizable": True,
                "tasks": [
                    {
                        "task": "standard_plots",
                        "tool": "python",
                        "script": "create_standard_plots.py",
                        "parameters": {
                            "plot_types": ["maps", "time_series", "seasonal_cycle"],
                            "variables": ["temperature", "precipitation", "pressure"],
                            "output_format": ["png", "pdf"],
                            "dpi": 300
                        }
                    },
                    {
                        "task": "model_evaluation",
                        "tool": "python",
                        "script": "model_evaluation.py",
                        "parameters": {
                            "reference_datasets": ["ERA5", "GPCP", "HadISST"],
                            "metrics": ["bias", "rmse", "correlation", "taylor_diagram"],
                            "regions": ["global", "tropics", "arctic"]
                        }
                    }
                ]
            },
            {
                "stage_id": "output_packaging",
                "description": "Package and organize processed outputs",
                "parallelizable": False,
                "tasks": [
                    {
                        "task": "organize_outputs",
                        "description": "Organize files into standard directory structure",
                        "parameters": {
                            "structure": "variable/frequency/processing_level",
                            "create_index": True,
                            "generate_catalog": True
                        }
                    },
                    {
                        "task": "compression_optimization",
                        "tool": "nccopy",
                        "parameters": {
                            "compression": "lz4",
                            "shuffle": True,
                            "chunk_strategy": "auto",
                            "compression_level": 4
                        }
                    },
                    {
                        "task": "metadata_finalization",
                        "description": "Add processing provenance metadata",
                        "parameters": {
                            "processing_version": "v2.0",
                            "processing_date": "auto",
                            "input_data_version": "auto_detect",
                            "software_versions": "auto_collect"
                        }
                    }
                ]
            }
        ],
        "progress_tracking": {
            "enabled": True,
            "granularity": "task_level",
            "metrics": ["files_processed", "data_volume_gb", "compute_hours_used"],
            "reporting_frequency": "hourly"
        },
        "quality_assurance": {
            "validation_after_each_stage": True,
            "output_validation": {
                "file_integrity_check": True,
                "metadata_validation": True,
                "statistical_validation": True
            },
            "rollback_on_failure": True
        }
    }
    
    return processing_pipeline

processing_pipeline = create_processing_pipeline()
print(f"\n⚙️  Data Processing Pipeline Created:")
print(f"   Pipeline ID: {processing_pipeline['workflow_id']}")
print(f"   Stages: {len(processing_pipeline['stages'])}")
print(f"   Parallel processing: {processing_pipeline['parallel_processing']['enabled']}")
print(f"   Max parallel jobs: {processing_pipeline['parallel_processing']['max_parallel_jobs']}")

# Calculate total tasks
total_tasks = sum(len(stage['tasks']) for stage in processing_pipeline['stages'])
print(f"   Total tasks: {total_tasks}")

print(f"\n   🔄 Pipeline Stages:")
for i, stage in enumerate(processing_pipeline['stages'], 1):
    parallel = "✓" if stage.get('parallelizable', False) else "✗"
    print(f"      {i}. {stage['stage_id']} ({len(stage['tasks'])} tasks, parallel: {parallel})")
```

## Step 4: Automated File Transfer Workflows

Create intelligent file transfer workflows with error handling and optimization.

```python
def create_transfer_workflow():
    """Create automated file transfer workflow."""
    
    transfer_workflow = {
        "workflow_id": "intelligent_file_transfer_v1",
        "description": "Smart file transfer with optimization and error handling",
        "trigger": {
            "type": "schedule",
            "schedule": "daily_at_02:00_utc"
        },
        "transfer_policies": {
            "bandwidth_management": {
                "max_bandwidth_mbps": 1000,
                "priority_queuing": True,
                "traffic_shaping": True,
                "off_peak_hours": ["22:00", "06:00"]
            },
            "file_selection": {
                "criteria": [
                    {"age": "older_than_24_hours"},
                    {"size": "larger_than_100_mb"},
                    {"status": "qc_passed"},
                    {"priority": "high_or_medium"}
                ],
                "exclusions": [
                    {"pattern": "*.tmp"},
                    {"pattern": "*_temp_*"},
                    {"status": "processing"}
                ]
            },
            "optimization": {
                "compression_on_the_fly": True,
                "parallel_streams": 4,
                "chunk_size_mb": 64,
                "adaptive_chunk_size": True,
                "resume_interrupted_transfers": True
            }
        },
        "transfer_routes": [
            {
                "route_id": "scratch_to_work_daily",
                "source": "hpc-scratch",
                "destination": "hpc-work",
                "priority": "high",
                "file_pattern": "*.nc",
                "processing": {
                    "verify_integrity": True,
                    "update_permissions": True,
                    "create_symlinks": False
                }
            },
            {
                "route_id": "work_to_campaign_weekly",
                "source": "hpc-work", 
                "destination": "hpc-campaign",
                "priority": "medium",
                "schedule_override": "weekly_sunday",
                "file_pattern": "processed_*.nc",
                "processing": {
                    "create_documentation": True,
                    "generate_thumbnails": True,
                    "update_catalog": True
                }
            },
            {
                "route_id": "critical_to_cloud_backup",
                "source": "hpc-work",
                "destination": "aws-s3-backup", 
                "priority": "critical",
                "file_pattern": "*_critical_*",
                "processing": {
                    "encryption": "aes256",
                    "redundancy": "cross_region",
                    "notification_on_completion": True
                }
            }
        ],
        "error_handling": {
            "retry_policy": {
                "max_attempts": 5,
                "backoff_strategy": "exponential",
                "base_delay_seconds": 30,
                "max_delay_seconds": 3600
            },
            "failure_classification": {
                "transient": ["network_timeout", "temporary_unavailable"],
                "permanent": ["permission_denied", "file_not_found"],
                "critical": ["data_corruption", "security_violation"]
            },
            "recovery_actions": {
                "transient": "retry_with_backoff",
                "permanent": "notify_admin_and_skip", 
                "critical": "halt_workflow_and_alert"
            }
        },
        "monitoring": {
            "metrics": [
                "transfer_rate_mbps",
                "queue_depth",
                "success_rate_percent",
                "error_rate_percent",
                "bandwidth_utilization"
            ],
            "alerts": {
                "slow_transfer": "below_50_percent_expected_rate",
                "high_error_rate": "above_5_percent_failures",
                "queue_backup": "more_than_100_pending_transfers"
            }
        }
    }
    
    return transfer_workflow

transfer_workflow = create_transfer_workflow()
print(f"\n📁 File Transfer Workflow Created:")
print(f"   Workflow ID: {transfer_workflow['workflow_id']}")
print(f"   Transfer routes: {len(transfer_workflow['transfer_routes'])}")
print(f"   Max bandwidth: {transfer_workflow['transfer_policies']['bandwidth_management']['max_bandwidth_mbps']} Mbps")
print(f"   Parallel streams: {transfer_workflow['transfer_policies']['optimization']['parallel_streams']}")

print(f"\n   🛤️  Transfer Routes:")
for route in transfer_workflow['transfer_routes']:
    print(f"      • {route['route_id']}: {route['source']} → {route['destination']} ({route['priority']} priority)")

print(f"\n   📊 Monitoring:")
print(f"      Metrics tracked: {len(transfer_workflow['monitoring']['metrics'])}")
print(f"      Alert conditions: {len(transfer_workflow['monitoring']['alerts'])}")
```

## Step 5: Progress Tracking and Monitoring

Implement comprehensive progress tracking for all workflows.

```python
async def setup_progress_tracking():
    """Set up comprehensive progress tracking for workflows."""
    
    # Create progress tracking for QC workflow
    qc_progress = CreateProgressTrackingDto(
        operation_id="qc_workflow_daily_run",
        operation_type=OperationType.DATA_VALIDATION.value,
        operation_name="Daily Quality Control Workflow",
        priority="high",
        context={
            "workflow_type": "quality_control",
            "trigger": "scheduled",
            "expected_duration_minutes": 120,
            "success_criteria": "all_files_validated"
        }
    )
    
    # Create progress tracking for processing pipeline
    processing_progress = CreateProgressTrackingDto(
        operation_id="processing_pipeline_run",
        operation_type=OperationType.DATA_PROCESSING.value,
        operation_name="ESM Data Processing Pipeline", 
        priority="medium",
        context={
            "workflow_type": "data_processing",
            "parallel_jobs": 4,
            "estimated_compute_hours": 16,
            "output_size_estimate_gb": 50
        }
    )
    
    # Create progress tracking for transfer workflow
    transfer_progress = CreateProgressTrackingDto(
        operation_id="transfer_workflow_daily",
        operation_type=OperationType.FILE_TRANSFER.value,
        operation_name="Daily File Transfer Workflow",
        priority="medium",
        context={
            "workflow_type": "file_transfer",
            "transfer_routes": 3,
            "estimated_transfer_gb": 100,
            "bandwidth_limit_mbps": 1000
        }
    )
    
    # Note: In real implementation, these would create actual progress trackers
    progress_operations = [qc_progress, processing_progress, transfer_progress]
    
    print("📊 Progress Tracking Configuration:")
    for op in progress_operations:
        print(f"   • {op.operation_name}")
        print(f"     Type: {op.operation_type}, Priority: {op.priority}")
        print(f"     Operation ID: {op.operation_id}")
    
    return progress_operations

# Set up progress tracking
import asyncio
progress_operations = asyncio.run(setup_progress_tracking())

# Configure monitoring dashboard
monitoring_config = {
    "dashboard_name": "Tellus Workflow Monitoring",
    "refresh_interval_seconds": 30,
    "data_retention_days": 90,
    "widgets": [
        {
            "widget_type": "workflow_status",
            "title": "Active Workflows",
            "data_source": "workflow_manager",
            "display": "status_grid"
        },
        {
            "widget_type": "performance_metrics",
            "title": "System Performance", 
            "metrics": ["cpu_usage", "memory_usage", "storage_io", "network_throughput"],
            "time_range": "last_24_hours"
        },
        {
            "widget_type": "error_summary",
            "title": "Recent Errors",
            "severity_levels": ["warning", "error", "critical"],
            "max_items": 20
        },
        {
            "widget_type": "throughput_chart",
            "title": "Data Processing Throughput",
            "metrics": ["files_per_hour", "gb_per_hour"],
            "chart_type": "line_graph"
        }
    ],
    "alerts": {
        "workflow_failure": {
            "condition": "any_workflow_fails",
            "notification": "immediate",
            "channels": ["email", "slack"]
        },
        "performance_degradation": {
            "condition": "throughput_below_50_percent",
            "notification": "after_15_minutes",
            "channels": ["email"]
        },
        "storage_full": {
            "condition": "any_storage_above_90_percent",
            "notification": "immediate",
            "channels": ["email", "slack", "pagerduty"]
        }
    }
}

print(f"\n📈 Monitoring Dashboard Configuration:")
print(f"   Dashboard: {monitoring_config['dashboard_name']}")
print(f"   Refresh rate: {monitoring_config['refresh_interval_seconds']} seconds")
print(f"   Widgets: {len(monitoring_config['widgets'])}")
print(f"   Alert conditions: {len(monitoring_config['alerts'])}")
```

## Step 6: Conditional Workflow Logic

Implement workflows with conditional logic and decision points.

```python
def create_conditional_workflow():
    """Create workflow with conditional logic and branching."""
    
    conditional_workflow = {
        "workflow_id": "adaptive_processing_workflow_v1",
        "description": "Adaptive workflow that adjusts based on data characteristics",
        "trigger": {
            "type": "file_arrival",
            "pattern": "*.nc"
        },
        "decision_points": [
            {
                "decision_id": "file_size_routing",
                "description": "Route files based on size for optimal processing",
                "condition": "file_size_gb",
                "branches": {
                    "small_files": {
                        "condition": "< 1",
                        "action": "batch_processing",
                        "parameters": {
                            "batch_size": 50,
                            "processing_mode": "fast_track",
                            "resources": "1_node_8_cores"
                        }
                    },
                    "medium_files": {
                        "condition": ">= 1 and < 10",
                        "action": "standard_processing",
                        "parameters": {
                            "processing_mode": "standard",
                            "resources": "2_nodes_16_cores"
                        }
                    },
                    "large_files": {
                        "condition": ">= 10",
                        "action": "high_memory_processing",
                        "parameters": {
                            "processing_mode": "memory_optimized",
                            "resources": "4_nodes_64_cores_high_memory"
                        }
                    }
                }
            },
            {
                "decision_id": "data_type_processing",
                "description": "Choose processing pipeline based on data type",
                "condition": "data_type",
                "detection_method": "metadata_analysis",
                "branches": {
                    "atmospheric_data": {
                        "condition": "component == 'atm'",
                        "workflow": "atmospheric_processing_pipeline",
                        "specialized_tools": ["cdo", "ncl", "python"]
                    },
                    "ocean_data": {
                        "condition": "component == 'ocn'",
                        "workflow": "ocean_processing_pipeline",
                        "specialized_tools": ["ferret", "xarray", "python"]
                    },
                    "land_data": {
                        "condition": "component == 'lnd'",
                        "workflow": "land_processing_pipeline", 
                        "specialized_tools": ["ncks", "cdo", "python"]
                    },
                    "unknown_data": {
                        "condition": "component == 'unknown'",
                        "workflow": "generic_processing_pipeline",
                        "fallback": True
                    }
                }
            },
            {
                "decision_id": "quality_based_routing",
                "description": "Route based on initial quality assessment",
                "condition": "initial_qc_score",
                "branches": {
                    "high_quality": {
                        "condition": "> 0.95",
                        "action": "fast_track_to_production",
                        "skip_stages": ["detailed_validation"]
                    },
                    "medium_quality": {
                        "condition": "> 0.80 and <= 0.95", 
                        "action": "standard_processing_with_validation"
                    },
                    "low_quality": {
                        "condition": "> 0.60 and <= 0.80",
                        "action": "intensive_quality_improvement",
                        "additional_stages": ["data_cleaning", "outlier_removal"]
                    },
                    "poor_quality": {
                        "condition": "<= 0.60",
                        "action": "quarantine_and_manual_review",
                        "notification": "immediate_admin_alert"
                    }
                }
            }
        ],
        "adaptive_parameters": {
            "resource_scaling": {
                "enabled": True,
                "scale_up_threshold": "queue_depth_over_10",
                "scale_down_threshold": "idle_for_30_minutes",
                "max_scale_factor": 3,
                "min_resources": "1_node"
            },
            "priority_adjustment": {
                "enabled": True,
                "factors": ["user_priority", "data_age", "downstream_dependencies"],
                "recompute_frequency": "hourly"
            }
        },
        "learning_system": {
            "enabled": True,
            "track_performance": True,
            "adjust_thresholds": True,
            "learning_period_days": 30,
            "performance_metrics": ["processing_time", "resource_efficiency", "quality_score"]
        }
    }
    
    return conditional_workflow

conditional_workflow = create_conditional_workflow()
print(f"\n🧠 Conditional Workflow Created:")
print(f"   Workflow ID: {conditional_workflow['workflow_id']}")
print(f"   Decision points: {len(conditional_workflow['decision_points'])}")
print(f"   Adaptive features: Resource scaling, Priority adjustment")
print(f"   Learning system: {'Enabled' if conditional_workflow['learning_system']['enabled'] else 'Disabled'}")

print(f"\n   🔀 Decision Points:")
for decision in conditional_workflow['decision_points']:
    branches = len(decision['branches'])
    print(f"      • {decision['decision_id']}: {branches} branches")
    print(f"        {decision['description']}")
```

## Step 7: Notification and Alert System

Set up comprehensive notifications for workflow events.

```python
def create_notification_system():
    """Create comprehensive notification and alert system."""
    
    notification_system = {
        "system_id": "tellus_notification_center",
        "channels": {
            "email": {
                "enabled": True,
                "smtp_server": "mail.institution.edu",
                "default_sender": "tellus-system@institution.edu",
                "templates": {
                    "workflow_success": "templates/workflow_success.html",
                    "workflow_failure": "templates/workflow_failure.html",
                    "system_alert": "templates/system_alert.html",
                    "daily_summary": "templates/daily_summary.html"
                }
            },
            "slack": {
                "enabled": True,
                "webhook_url": "https://hooks.slack.com/...",
                "channels": {
                    "general": "#tellus-general",
                    "alerts": "#tellus-alerts",
                    "development": "#tellus-dev"
                },
                "mention_groups": {
                    "critical": "@here",
                    "urgent": "@channel",
                    "info": ""
                }
            },
            "webhooks": {
                "enabled": True,
                "endpoints": [
                    {
                        "name": "monitoring_system",
                        "url": "https://monitoring.institution.edu/webhook",
                        "events": ["workflow_completion", "system_alerts"]
                    }
                ]
            }
        },
        "notification_rules": [
            {
                "rule_id": "workflow_completion",
                "trigger": "workflow_completed",
                "conditions": {
                    "success": {
                        "channels": ["email"],
                        "recipients": ["workflow_owner"],
                        "frequency": "immediate",
                        "template": "workflow_success"
                    },
                    "failure": {
                        "channels": ["email", "slack"],
                        "recipients": ["workflow_owner", "system_admins"],
                        "frequency": "immediate",
                        "template": "workflow_failure",
                        "escalation": {
                            "after_minutes": 30,
                            "additional_recipients": ["manager"]
                        }
                    }
                }
            },
            {
                "rule_id": "system_performance",
                "trigger": "performance_threshold",
                "conditions": {
                    "degraded": {
                        "threshold": "below_70_percent_normal",
                        "channels": ["slack"],
                        "frequency": "every_15_minutes_while_active"
                    },
                    "critical": {
                        "threshold": "below_50_percent_normal",
                        "channels": ["email", "slack"],
                        "recipients": ["system_admins", "oncall_engineer"],
                        "frequency": "immediate"
                    }
                }
            },
            {
                "rule_id": "storage_alerts",
                "trigger": "storage_usage",
                "conditions": {
                    "warning": {
                        "threshold": "above_80_percent",
                        "channels": ["email"],
                        "frequency": "daily_if_persistent"
                    },
                    "critical": {
                        "threshold": "above_95_percent",
                        "channels": ["email", "slack"],
                        "frequency": "immediate_and_repeat_hourly"
                    }
                }
            }
        ],
        "digest_reports": {
            "daily_summary": {
                "enabled": True,
                "schedule": "08:00_utc_daily",
                "recipients": ["project_team", "data_managers"],
                "content": [
                    "workflows_completed",
                    "data_processed_gb",
                    "system_performance_summary",
                    "error_summary",
                    "upcoming_scheduled_tasks"
                ]
            },
            "weekly_report": {
                "enabled": True,
                "schedule": "monday_09:00_utc",
                "recipients": ["project_leads", "stakeholders"],
                "content": [
                    "workflow_statistics",
                    "performance_trends",
                    "cost_analysis",
                    "capacity_planning",
                    "recommended_actions"
                ]
            }
        }
    }
    
    return notification_system

notification_system = create_notification_system()
print(f"\n📢 Notification System Configuration:")
print(f"   System ID: {notification_system['system_id']}")
print(f"   Channels: {', '.join(notification_system['channels'].keys())}")
print(f"   Notification rules: {len(notification_system['notification_rules'])}")
print(f"   Digest reports: {len(notification_system['digest_reports'])}")

print(f"\n   📨 Notification Channels:")
for channel, config in notification_system['channels'].items():
    status = "✅" if config['enabled'] else "❌"
    print(f"      {status} {channel.title()}")

print(f"\n   📊 Digest Reports:")
for report_name, config in notification_system['digest_reports'].items():
    status = "✅" if config['enabled'] else "❌"
    print(f"      {status} {report_name.replace('_', ' ').title()}: {config['schedule']}")
```

## Summary and Best Practices

Congratulations! You've built a comprehensive automation system for Earth System Model workflows.

### 🎯 **Key Accomplishments**

✅ **Quality Control Automation** - Automated validation of model output files  
✅ **Data Processing Pipeline** - Multi-stage processing with parallel execution  
✅ **Intelligent File Transfers** - Smart routing with error handling and optimization  
✅ **Progress Tracking** - Real-time monitoring of all workflow operations  
✅ **Conditional Logic** - Adaptive workflows that adjust based on data characteristics  
✅ **Notification System** - Comprehensive alerting and reporting capabilities

### 📊 **Workflow System Statistics**

- **Total workflow types**: 4 (QC, Processing, Transfer, Conditional)
- **Processing stages**: 15+ configurable stages across all workflows
- **Decision points**: 3 major conditional logic branches
- **Notification rules**: 3 categories with escalation policies
- **Monitoring metrics**: 10+ performance and health indicators

### 🚀 **Advanced Features Implemented**

- **Parallel Processing**: Multi-job execution with resource management
- **Error Recovery**: Automatic retry with exponential backoff
- **Adaptive Scaling**: Dynamic resource allocation based on workload
- **Learning System**: Performance-based threshold adjustment
- **Quality Assurance**: Multi-level validation with rollback capabilities

### 💡 **Best Practices for Workflow Automation**

1. **Design for Failure**: Implement comprehensive error handling and recovery
2. **Monitor Everything**: Track performance, errors, and resource usage
3. **Make it Adaptive**: Use conditional logic to handle different data types
4. **Optimize Resources**: Scale processing power based on workload
5. **Communicate Status**: Keep stakeholders informed with smart notifications

### 🔄 **Next Steps**

1. **Deploy Workflows**: Test and deploy your automation workflows
2. **Monitor Performance**: Use dashboards to track workflow efficiency
3. **Optimize and Tune**: Adjust parameters based on real-world performance
4. **Expand Coverage**: Add more specialized workflows for your specific needs

Ready to master the advanced interfaces? Continue to [**Advanced Interfaces Tutorial**](04-advanced-interfaces.md) to learn about the TUI, advanced CLI features, and integration with other scientific tools.

### 🛠️ **Troubleshooting Common Issues**

**Workflow Failures**
- Check resource availability and quotas
- Verify file permissions and accessibility
- Review error logs for specific failure points

**Performance Issues**
- Monitor CPU, memory, and I/O usage
- Adjust parallel processing parameters
- Consider storage I/O optimization

**Notification Problems**
- Verify email/Slack configuration
- Test webhook endpoints
- Check notification rule conditions

Need help with advanced features? The [Advanced Interfaces Tutorial](04-advanced-interfaces.md) covers debugging tools, performance profiling, and integration techniques.