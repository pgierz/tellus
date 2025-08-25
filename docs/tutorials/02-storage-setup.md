# Storage Setup Guide

## Introduction

This tutorial covers advanced storage configuration in Tellus, including multi-tier storage hierarchies, remote systems, cloud storage, and integration with High-Performance Computing (HPC) environments. You'll learn to set up complex storage architectures that match real-world Earth System Model workflows.

## Prerequisites

- Completed the [Quickstart Tutorial](01-quickstart.md)
- Basic understanding of storage systems (local, network, cloud)
- Access to one or more storage systems (local, SSH, or cloud)

## What You'll Learn

- Configure multi-tier storage hierarchies (scratch, work, archive)
- Set up remote storage access (SSH, SFTP, cloud)
- Implement automated data movement between storage tiers
- Configure HPC-specific storage optimizations
- Set up monitoring and alerting for storage systems

## Storage Architecture Overview

Tellus supports sophisticated storage architectures commonly used in Earth System Model research:

```
┌─────────────┐    ┌──────────────┐    ┌─────────────┐    ┌──────────────┐
│   Scratch   │ -> │     Work     │ -> │  Campaign   │ -> │   Archive    │
│ (Fast I/O)  │    │ (Project)    │    │ (Shared)    │    │ (Long-term)  │
│ Days        │    │ Months       │    │ Years       │    │ Decades      │
└─────────────┘    └──────────────┘    └─────────────┘    └──────────────┘
```

## Step 1: Understanding Location Types and Protocols

First, let's understand the different types of storage locations Tellus supports:

```python
from tellus.application.container import ServiceContainer
from tellus.application.dtos import CreateLocationDto
from tellus.domain.entities.location import LocationKind

container = ServiceContainer()
location_service = container.get_location_service()

# Location kinds define the purpose and characteristics
location_kinds = {
    LocationKind.COMPUTE: "High-performance computing nodes with fast local storage",
    LocationKind.DISK: "General-purpose disk storage, good for active data",  
    LocationKind.FILESERVER: "Network-attached storage, shared across systems",
    LocationKind.TAPE: "Tape archive systems, high capacity but slower access"
}

print("🏗️  Tellus Storage Location Types:")
for kind, description in location_kinds.items():
    print(f"   {kind.value.upper()}: {description}")

# Supported protocols
protocols = {
    "file": "Local filesystem access",
    "ssh": "Remote access via SSH/SFTP", 
    "s3": "Amazon S3 compatible object storage",
    "gcs": "Google Cloud Storage",
    "azure": "Microsoft Azure Blob Storage",
    "hsi": "HPSS (High Performance Storage System)",
    "ftp": "File Transfer Protocol",
    "http": "HTTP/HTTPS file access"
}

print(f"\n🌐 Supported Storage Protocols:")
for protocol, description in protocols.items():
    print(f"   {protocol.upper()}: {description}")
```

## Step 2: Setting Up HPC Storage Hierarchy

Let's configure a realistic HPC storage hierarchy similar to what you'd find at NCAR, NERSC, or other major computing centers.

```python
# Step 2a: Scratch Storage (High-performance, temporary)
scratch_storage = CreateLocationDto(
    name="hpc-scratch",
    kinds=[LocationKind.COMPUTE],
    protocol="file",
    path="/scratch/username",  # Update with your username
    description="High-performance scratch storage for active simulations",
    metadata={
        "filesystem_type": "lustre",  # Common HPC filesystem
        "performance_tier": "high",
        "retention_policy": "90_days",
        "quota_tb": 20,
        "stripe_count": 8,  # Lustre-specific optimization
        "stripe_size": "1MB",
        "purge_policy": "automatic",
        "backup": False,
        "typical_usage": "active_simulations"
    }
)

scratch = location_service.create_location(scratch_storage)
print(f"🚀 Created scratch storage: {scratch.name}")
print(f"   Path: {scratch.path}")
print(f"   Performance: {scratch.metadata['performance_tier']}")
print(f"   Retention: {scratch.metadata['retention_policy']}")

# Step 2b: Work Storage (Project data, persistent)
work_storage = CreateLocationDto(
    name="hpc-work", 
    kinds=[LocationKind.DISK, LocationKind.FILESERVER],
    protocol="file",
    path="/work/projectname",  # Update with your project
    description="Project work storage for analysis and processed data",
    metadata={
        "filesystem_type": "gpfs",  # Another common HPC filesystem
        "performance_tier": "medium",
        "retention_policy": "project_lifetime",
        "quota_tb": 5,
        "backup_schedule": "weekly",
        "snapshot_retention": "30_days", 
        "shared_access": True,
        "typical_usage": "processed_data_analysis"
    }
)

work = location_service.create_location(work_storage)
print(f"💼 Created work storage: {work.name}")
print(f"   Shared access: {work.metadata['shared_access']}")
print(f"   Backup schedule: {work.metadata['backup_schedule']}")

# Step 2c: Campaign Storage (Collaborative, shared datasets)
campaign_storage = CreateLocationDto(
    name="hpc-campaign",
    kinds=[LocationKind.FILESERVER],
    protocol="file", 
    path="/glade/campaign/project/shared",
    description="Campaign storage for collaborative datasets and results",
    metadata={
        "filesystem_type": "gpfs",
        "performance_tier": "medium",
        "retention_policy": "long_term",
        "quota_tb": 50,
        "access_control": "group_based",
        "collaboration_enabled": True,
        "data_sharing_policy": "project_team",
        "typical_usage": "collaborative_datasets"
    }
)

campaign = location_service.create_location(campaign_storage)
print(f"👥 Created campaign storage: {campaign.name}")
print(f"   Collaboration: {campaign.metadata['collaboration_enabled']}")
print(f"   Access control: {campaign.metadata['access_control']}")

# Step 2d: Archive Storage (HPSS tape system)
archive_storage = CreateLocationDto(
    name="hpc-hpss-archive",
    kinds=[LocationKind.TAPE],
    protocol="hsi",  # HPSS Storage Interface
    host="hpss.center.edu",
    path="/CLIMATE/project/archives",
    description="HPSS tape archive for long-term data preservation",
    metadata={
        "storage_system": "hpss",
        "tape_technology": "LTO-9",
        "capacity_pb": 10,  # 10 petabytes
        "retention_policy": "permanent",
        "retrieval_time_hours": "0.5_to_4",
        "cost_per_tb_year": 5.0,
        "migration_policy": "automatic",
        "compression_enabled": True,
        "typical_usage": "long_term_preservation"
    }
)

archive = location_service.create_location(archive_storage)
print(f"📼 Created HPSS archive: {archive.name}")
print(f"   Capacity: {archive.metadata['capacity_pb']} PB")
print(f"   Retrieval time: {archive.metadata['retrieval_time_hours']} hours")
```

## Step 3: Remote Storage Configuration

Configure remote storage locations accessible via SSH or cloud APIs.

```python
# Step 3a: Remote analysis server via SSH
remote_analysis = CreateLocationDto(
    name="remote-analysis-server",
    kinds=[LocationKind.COMPUTE, LocationKind.FILESERVER],
    protocol="ssh",
    host="analysis.university.edu",
    username="your_username",  # Update with your username
    path="/home/your_username/climate_analysis",
    description="Remote analysis server for post-processing and visualization",
    metadata={
        "ssh_key_path": "~/.ssh/id_rsa_analysis",  # SSH key for authentication
        "port": 22,
        "connection_timeout": 30,
        "keepalive_interval": 60,
        "compression": True,
        "cpu_cores": 64,
        "memory_gb": 512,
        "gpu_count": 2,
        "software_stack": ["python", "ncl", "cdo", "nco", "jupyter"],
        "typical_usage": "analysis_and_visualization"
    }
)

remote = location_service.create_location(remote_analysis)
print(f"🌐 Created remote analysis server: {remote.name}")
print(f"   Host: {remote.host}")
print(f"   Compute: {remote.metadata['cpu_cores']} cores, {remote.metadata['gpu_count']} GPUs")

# Step 3b: Cloud storage (AWS S3)
cloud_storage = CreateLocationDto(
    name="aws-s3-backup",
    kinds=[LocationKind.FILESERVER],
    protocol="s3",
    host="s3.us-west-2.amazonaws.com",
    bucket="climate-data-backup",  # Update with your bucket
    path="/processed-datasets",
    description="AWS S3 bucket for cloud backup and sharing",
    metadata={
        "aws_region": "us-west-2",
        "storage_class": "STANDARD_IA",  # Infrequent Access
        "encryption": "AES256",
        "versioning": True,
        "lifecycle_policy": "transition_to_glacier_after_90_days",
        "access_policy": "project_team",
        "cost_optimization": True,
        "typical_usage": "backup_and_sharing"
    }
)

cloud = location_service.create_location(cloud_storage)
print(f"☁️  Created cloud storage: {cloud.name}")
print(f"   Region: {cloud.metadata['aws_region']}")
print(f"   Storage class: {cloud.metadata['storage_class']}")

# Step 3c: Institutional data repository
institutional_repo = CreateLocationDto(
    name="institutional-repository",
    kinds=[LocationKind.FILESERVER],
    protocol="ssh",  # Could also be "http" for web-based access
    host="data.institution.edu",
    username="repo_user",
    path="/repository/climate/project",
    description="Institutional data repository for published datasets",
    metadata={
        "repository_type": "institutional",
        "doi_assignment": True,
        "metadata_schema": "datacite",
        "access_level": "public",
        "embargo_period_months": 12,
        "preservation_level": "bit_level",
        "citation_required": True,
        "typical_usage": "data_publication"
    }
)

repo = location_service.create_location(institutional_repo)
print(f"🏛️  Created institutional repository: {repo.name}")
print(f"   DOI assignment: {repo.metadata['doi_assignment']}")
print(f"   Access level: {repo.metadata['access_level']}")
```

## Step 4: Automated Data Movement Workflows

Set up automated workflows to move data through your storage hierarchy.

```python
from tellus.application.dtos import BatchFileTransferOperationDto

# Get the transfer service
transfer_service = container.get_file_transfer_service()

def create_data_lifecycle_workflow():
    """Create automated data movement workflows."""
    
    workflows = {
        "scratch_to_work": {
            "description": "Daily movement of completed simulation data",
            "schedule": "daily_at_02:00_utc",
            "source_location": "hpc-scratch",
            "dest_location": "hpc-work",
            "file_pattern": "*.nc",
            "age_criteria": "older_than_24_hours",
            "size_criteria": "larger_than_100_mb",
            "verification": "checksum_and_size"
        },
        "work_to_campaign": {
            "description": "Weekly movement of analyzed data to shared storage", 
            "schedule": "weekly_sunday_01:00_utc",
            "source_location": "hpc-work",
            "dest_location": "hpc-campaign",
            "file_pattern": "processed_*.nc",
            "quality_check": "metadata_validation",
            "documentation_required": True
        },
        "campaign_to_archive": {
            "description": "Monthly archival of stable datasets",
            "schedule": "monthly_first_saturday",
            "source_location": "hpc-campaign", 
            "dest_location": "hpc-hpss-archive",
            "compression": "gzip_level_6",
            "bundling": "tar_by_simulation",
            "metadata_preservation": True
        },
        "selective_cloud_backup": {
            "description": "Cloud backup of critical processed data",
            "schedule": "weekly_saturday_03:00_utc",
            "source_location": "hpc-work",
            "dest_location": "aws-s3-backup", 
            "criteria": "tagged_as_critical",
            "encryption": "client_side_aes256",
            "cost_optimization": True
        }
    }
    
    return workflows

workflows = create_data_lifecycle_workflow()
print("🔄 Data Lifecycle Workflows Configuration:")
for workflow_id, config in workflows.items():
    print(f"\n   {workflow_id.upper().replace('_', ' ')}:")
    print(f"      {config['description']}")
    print(f"      Schedule: {config['schedule']}")
    print(f"      Route: {config['source_location']} → {config['dest_location']}")
    if 'verification' in config:
        print(f"      Verification: {config['verification']}")
```

## Step 5: Storage Performance Optimization

Configure performance optimizations for different storage systems.

```python
def configure_storage_optimizations():
    """Configure storage-specific performance optimizations."""
    
    optimizations = {
        "lustre_optimizations": {
            "description": "Lustre filesystem optimizations for HPC scratch",
            "stripe_count": 8,  # Spread files across 8 storage targets
            "stripe_size": "1MB",  # 1MB stripe size for large files
            "directory_striping": True,
            "progressive_file_layout": True,
            "client_cache": "write_through",
            "lock_ahead": True,
            "io_pattern": "sequential_optimized"
        },
        "gpfs_optimizations": {
            "description": "GPFS optimizations for work and campaign storage",
            "block_size": "1MB",
            "compression": "lz4",  # Fast compression for space savings
            "prefetch": "aggressive",
            "write_behind": True,
            "metadata_replication": 2,
            "data_replication": 1
        },
        "s3_optimizations": {
            "description": "S3 optimizations for cloud storage",
            "multipart_threshold": "64MB",
            "multipart_chunk_size": "16MB",
            "max_concurrent_requests": 10,
            "storage_class_transitions": {
                "to_ia": "30_days",
                "to_glacier": "90_days", 
                "to_deep_archive": "365_days"
            },
            "intelligent_tiering": True
        },
        "hpss_optimizations": {
            "description": "HPSS tape archive optimizations",
            "cos_level": 2,  # Class of Service for performance
            "migration_policy": "auto_migrate_after_30_days",
            "bundling_strategy": "by_simulation_and_date",
            "compression": "hardware_assisted",
            "duplicate_detection": True,
            "metadata_indexing": "full_content"
        }
    }
    
    return optimizations

optimizations = configure_storage_optimizations()
print("\n⚡ Storage Performance Optimizations:")
for opt_type, config in optimizations.items():
    print(f"\n   {opt_type.upper().replace('_', ' ')}:")
    print(f"      {config['description']}")
    # Show key optimization settings
    key_settings = [k for k in config.keys() if k != 'description'][:3]
    for setting in key_settings:
        if isinstance(config[setting], dict):
            print(f"      {setting}: {len(config[setting])} configurations")
        else:
            print(f"      {setting}: {config[setting]}")
```

## Step 6: Storage Monitoring and Alerting

Set up monitoring for your storage systems.

```python
def configure_storage_monitoring():
    """Configure comprehensive storage monitoring."""
    
    monitoring_config = {
        "metrics_collection": {
            "update_frequency_seconds": 60,
            "retention_days": 365,
            "aggregation_levels": ["hourly", "daily", "weekly"],
            "metrics": {
                "space_usage": {
                    "total_capacity": "bytes",
                    "used_space": "bytes", 
                    "available_space": "bytes",
                    "usage_percentage": "percent",
                    "growth_rate": "bytes_per_day"
                },
                "performance": {
                    "read_throughput": "mb_per_second",
                    "write_throughput": "mb_per_second",
                    "iops": "operations_per_second",
                    "latency": "milliseconds",
                    "queue_depth": "count"
                },
                "health": {
                    "availability": "percent",
                    "error_rate": "percent",
                    "failed_operations": "count",
                    "maintenance_windows": "scheduled"
                }
            }
        },
        "alert_thresholds": {
            "space_critical": "95_percent_full",
            "space_warning": "85_percent_full",
            "performance_degraded": "50_percent_below_baseline",
            "availability_critical": "less_than_99_percent",
            "error_rate_high": "more_than_5_percent"
        },
        "notification_channels": {
            "email": "storage-admins@institution.edu",
            "slack": "#storage-alerts",
            "pagerduty": "storage_oncall_team",
            "dashboard": "grafana_storage_overview"
        }
    }
    
    return monitoring_config

monitoring = configure_storage_monitoring()
print("\n📊 Storage Monitoring Configuration:")
print(f"   Update frequency: {monitoring['metrics_collection']['update_frequency_seconds']} seconds")
print(f"   Metrics tracked: {len(monitoring['metrics_collection']['metrics'])} categories")
print(f"   Alert thresholds: {len(monitoring['alert_thresholds'])} conditions")
print(f"   Notification channels: {', '.join(monitoring['notification_channels'].keys())}")

# Display key alert thresholds
print("\n   🚨 Key Alert Thresholds:")
for alert, threshold in monitoring['alert_thresholds'].items():
    print(f"      {alert.replace('_', ' ').title()}: {threshold}")
```

## Step 7: Storage Location Validation

Implement validation checks for your storage locations.

```python
def validate_storage_locations():
    """Validate all configured storage locations."""
    
    # Get all locations
    all_locations = location_service.list_locations()
    
    validation_results = {
        "total_locations": len(all_locations.locations),
        "by_protocol": {},
        "by_kind": {},
        "issues_found": []
    }
    
    print("🔍 Validating Storage Locations:")
    
    for location in all_locations.locations:
        # Count by protocol
        protocol = location.protocol or "unknown"
        validation_results["by_protocol"][protocol] = validation_results["by_protocol"].get(protocol, 0) + 1
        
        # Count by kind
        for kind in location.kinds:
            kind_str = kind.value
            validation_results["by_kind"][kind_str] = validation_results["by_kind"].get(kind_str, 0) + 1
        
        # Validation checks
        issues = []
        
        # Check for required metadata
        if not location.description:
            issues.append("Missing description")
        
        # Check protocol-specific requirements
        if location.protocol == "ssh" and not location.host:
            issues.append("SSH location missing host")
        
        if location.protocol == "s3" and not location.bucket:
            issues.append("S3 location missing bucket")
        
        # Check path validity
        if not location.path:
            issues.append("Missing path specification")
        
        if issues:
            validation_results["issues_found"].append({
                "location": location.name,
                "issues": issues
            })
        
        # Display status
        status = "❌" if issues else "✅"
        print(f"   {status} {location.name} ({location.protocol})")
        if issues:
            for issue in issues:
                print(f"      • {issue}")
    
    # Summary
    print(f"\n📋 Validation Summary:")
    print(f"   Total locations: {validation_results['total_locations']}")
    print(f"   Issues found: {len(validation_results['issues_found'])}")
    
    print(f"\n   📊 By Protocol:")
    for protocol, count in validation_results["by_protocol"].items():
        print(f"      {protocol.upper()}: {count}")
    
    print(f"\n   📊 By Kind:")
    for kind, count in validation_results["by_kind"].items():
        print(f"      {kind.upper()}: {count}")
    
    return validation_results

validation_results = validate_storage_locations()
```

## Step 8: Advanced Storage Features

Configure advanced features for sophisticated storage management.

```python
def configure_advanced_features():
    """Configure advanced storage management features."""
    
    advanced_features = {
        "data_deduplication": {
            "enabled": True,
            "hash_algorithm": "sha256",
            "minimum_file_size": "1MB",
            "savings_estimate": "15_to_30_percent",
            "applicable_locations": ["hpc-work", "hpc-campaign", "aws-s3-backup"]
        },
        "automated_cleanup": {
            "enabled": True,
            "policies": {
                "scratch_cleanup": {
                    "location": "hpc-scratch",
                    "criteria": "older_than_90_days",
                    "dry_run_first": True,
                    "notification_required": True
                },
                "temp_file_cleanup": {
                    "pattern": "*.tmp",
                    "criteria": "older_than_7_days",
                    "all_locations": True
                }
            }
        },
        "data_integrity_checking": {
            "enabled": True,
            "checksum_algorithm": "xxhash64",
            "verification_schedule": "weekly",
            "error_handling": "quarantine_and_alert",
            "repair_attempts": 3
        },
        "intelligent_caching": {
            "enabled": True,
            "cache_locations": ["hpc-work"],
            "eviction_policy": "lru_with_access_patterns",
            "size_limit_gb": 100,
            "prefetch_predictions": True
        },
        "cross_location_replication": {
            "enabled": True,
            "critical_data_copies": 3,
            "geographic_distribution": True,
            "async_replication": True,
            "conflict_resolution": "timestamp_based"
        }
    }
    
    return advanced_features

advanced = configure_advanced_features()
print("\n🚀 Advanced Storage Features:")
for feature_name, config in advanced.items():
    enabled = config.get("enabled", False)
    status = "✅" if enabled else "❌"
    print(f"   {status} {feature_name.replace('_', ' ').title()}")
    
    if enabled and feature_name == "data_deduplication":
        print(f"      Savings estimate: {config['savings_estimate']}")
        print(f"      Locations: {len(config['applicable_locations'])}")
    elif enabled and feature_name == "automated_cleanup":
        print(f"      Cleanup policies: {len(config['policies'])}")
    elif enabled and feature_name == "intelligent_caching":
        print(f"      Cache size: {config['size_limit_gb']} GB")
```

## Summary and Best Practices

You've successfully configured a sophisticated storage hierarchy! Here are key best practices:

### 🎯 **Storage Hierarchy Best Practices**

1. **Match Performance to Usage**:
   - Use high-performance storage (scratch) for active simulations
   - Use shared storage (campaign) for collaborative data
   - Use archive storage for long-term preservation

2. **Implement Automated Workflows**:
   - Set up regular data movement between tiers
   - Use checksums for data integrity verification
   - Implement cleanup policies to prevent storage overflow

3. **Monitor and Alert**:
   - Track storage usage and performance metrics
   - Set up alerts before storage becomes full
   - Monitor data transfer success rates

4. **Optimize for Your Workload**:
   - Configure filesystem parameters for your I/O patterns
   - Use compression where appropriate
   - Implement intelligent caching strategies

### 📊 **What You've Accomplished**

✅ **Multi-tier storage hierarchy** with scratch, work, campaign, and archive tiers  
✅ **Remote access** via SSH and cloud storage integration  
✅ **Automated data movement** workflows with scheduling and validation  
✅ **Performance optimizations** tailored to different storage systems  
✅ **Comprehensive monitoring** with alerts and health checks  
✅ **Advanced features** including deduplication and intelligent caching

### 🔄 **Next Steps**

1. **Test Your Configuration**: Validate all storage locations and test data transfers
2. **Set Up Monitoring**: Deploy monitoring dashboards and configure alerts
3. **Create Automation**: Implement your data lifecycle workflows
4. **Optimize Performance**: Monitor and tune storage performance parameters

Continue to [**Automation Workflows Tutorial**](03-automation-workflows.md) to learn how to automate complex data processing pipelines using your storage infrastructure.

## Troubleshooting Common Issues

### Connection Issues
- **SSH authentication**: Verify SSH keys and host connectivity
- **Cloud credentials**: Check AWS/GCS/Azure credentials and permissions
- **Network connectivity**: Test network access to remote systems

### Performance Issues  
- **Slow transfers**: Check network bandwidth and storage I/O capabilities
- **High latency**: Consider caching frequently accessed data
- **Quota limits**: Monitor storage quotas and implement cleanup policies

### Configuration Issues
- **Path errors**: Verify all paths exist and have correct permissions
- **Protocol mismatches**: Ensure protocol matches storage system capabilities
- **Metadata inconsistencies**: Validate metadata completeness and accuracy

Need help? Check the [Advanced Interfaces Tutorial](04-advanced-interfaces.md) for debugging tools and advanced configuration options.