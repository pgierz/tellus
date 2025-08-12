# Workflow Integration

This example demonstrates how to integrate Tellus with computational workflows, particularly Snakemake, for automated data management in scientific pipelines.

```{note}
This example focuses on workflow integration patterns. For a complete interactive notebook version, see the planned `workflow-integration.ipynb` file.
```

## Overview

Tellus seamlessly integrates with workflow engines to provide:

- Automatic data staging and retrieval
- Dependency-aware file transfers  
- Progress tracking for long-running workflows
- Centralized data location management
- Reproducible data access patterns

## Snakemake Integration

### Basic Setup

```python
# Snakefile with Tellus integration
import tellus

# Global simulation setup
SIM = tellus.Simulation("climate-workflow")
REMOTE_DATA = SIM.add_location("hpc-storage", type="ssh", ...)
LOCAL_CACHE = SIM.add_location("cache", type="local", path="./cache/")

# Workflow configuration
configfile: "config.yaml"

rule all:
    input:
        "results/final_analysis.pdf"

rule download_input_data:
    output:
        "data/raw/temperature_{year}.nc"
    run:
        # Download specific year's data
        REMOTE_DATA.get(f"temperature_{wildcards.year}.nc", "data/raw/")

rule process_data:
    input:
        "data/raw/temperature_{year}.nc"
    output:
        "data/processed/temperature_{year}_processed.nc"
    shell:
        "python scripts/process_temperature.py {input} {output}"

rule combine_years:
    input:
        expand("data/processed/temperature_{year}_processed.nc", year=range(2000, 2021))
    output:
        "results/combined_temperature.nc"
    shell:
        "python scripts/combine_data.py {input} {output}"
```

### Advanced Patterns

#### Data Staging Rule

```python
# Create a reusable data staging rule
rule stage_remote_data:
    output:
        "data/staged/{dataset}.nc"
    params:
        remote_path=lambda wildcards: config["datasets"][wildcards.dataset]["path"]
    run:
        # Stage data with caching
        REMOTE_DATA.get(
            params.remote_path,
            f"data/staged/{wildcards.dataset}.nc",
            cache=True,
            cache_dir="./workflow_cache/"
        )

# Use staged data in analysis rules
rule analyze_dataset:
    input:
        "data/staged/{dataset}.nc"
    output:
        "results/{dataset}_analysis.json"
    shell:
        "python scripts/analyze.py {input} {output}"
```

#### Conditional Data Downloads

```python
def get_input_files(wildcards):
    """Dynamically determine required input files."""
    experiment = wildcards.experiment
    required_vars = config["experiments"][experiment]["variables"]
    
    files = []
    for var in required_vars:
        # Check if data exists locally first
        local_file = f"data/{var}_{experiment}.nc"
        if not os.path.exists(local_file):
            # Will trigger download rule
            files.append(local_file)
    
    return files

rule run_experiment:
    input:
        get_input_files
    output:
        "results/{experiment}/output.nc"
    shell:
        "python scripts/experiment.py --experiment {wildcards.experiment} --output {output}"
```

### Progress Tracking in Workflows

```python
# Custom rule with progress tracking
rule download_large_dataset:
    output:
        "data/large/climate_model_output.tar.gz"
    run:
        import tellus
        from rich.progress import Progress
        
        with Progress() as progress:
            task = progress.add_task("Downloading climate data...", total=100)
            
            def update_progress(bytes_transferred, total_bytes):
                percent = (bytes_transferred / total_bytes) * 100
                progress.update(task, completed=percent)
            
            REMOTE_DATA.get(
                "climate_model_output.tar.gz",
                "data/large/",
                progress_callback=update_progress
            )
```

## Workflow Managers

### Nextflow Integration

```nextflow
// nextflow.config
params {
    simulation_name = "genomics-pipeline"
    remote_host = "data.server.edu"
    remote_path = "/shared/genomics/data"
}

// main.nf
#!/usr/bin/env nextflow

process downloadData {
    publishDir 'data/raw'
    
    output:
    path "*.fastq.gz" into raw_reads
    
    script:
    """
    python -c "
    import tellus
    sim = tellus.Simulation('${params.simulation_name}')
    remote = sim.add_location('genomics-data', type='ssh', 
                             host='${params.remote_host}', 
                             path='${params.remote_path}')
    remote.get('*.fastq.gz', './')
    "
    """
}

process qualityControl {
    input:
    path reads from raw_reads
    
    output:
    path "*_qc.fastq.gz" into clean_reads
    
    script:
    """
    fastqc ${reads}
    # ... quality control steps
    """
}
```

### Apache Airflow Integration

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import tellus

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'climate_data_pipeline',
    default_args=default_args,
    description='Climate data processing pipeline',
    schedule_interval=timedelta(days=1),
    catchup=False
)

def download_daily_data(**context):
    """Download daily climate data."""
    sim = tellus.Simulation("climate-daily")
    remote = sim.add_location("noaa-server", type="ssh", ...)
    
    # Download data for execution date
    date_str = context['ds']  # YYYY-MM-DD format
    remote.get(f"climate_{date_str}.nc", "./data/daily/")

def process_data(**context):
    """Process downloaded data."""
    date_str = context['ds']
    # Processing logic here
    pass

def upload_results(**context):
    """Upload results to archive."""
    sim = tellus.Simulation("climate-daily")
    archive = sim.add_location("results-archive", type="s3", ...)
    
    date_str = context['ds']
    archive.put(f"./results/processed_{date_str}.nc", f"daily/{date_str}/")

# Define tasks
download_task = PythonOperator(
    task_id='download_data',
    python_callable=download_daily_data,
    dag=dag
)

process_task = PythonOperator(
    task_id='process_data', 
    python_callable=process_data,
    dag=dag
)

upload_task = PythonOperator(
    task_id='upload_results',
    python_callable=upload_results,
    dag=dag
)

# Set dependencies
download_task >> process_task >> upload_task
```

## Custom Workflow Scripts

### Data Pipeline Script

```python
#!/usr/bin/env python3
"""
Custom data processing pipeline with Tellus integration.
"""

import tellus
import argparse
import logging
from pathlib import Path

class DataPipeline:
    def __init__(self, config_file):
        self.config = tellus.load_config(config_file)
        self.sim = tellus.Simulation(self.config["simulation_name"])
        
        # Setup locations
        self.setup_locations()
        
    def setup_locations(self):
        """Initialize data locations."""
        self.raw_data = self.sim.add_location(
            "raw-data",
            **self.config["locations"]["raw_data"]
        )
        
        self.processed_data = self.sim.add_location(
            "processed-data", 
            **self.config["locations"]["processed_data"]
        )
        
        self.results = self.sim.add_location(
            "results",
            **self.config["locations"]["results"]
        )
    
    def stage_data(self, pattern="*.nc"):
        """Download and stage input data."""
        logging.info(f"Staging data matching: {pattern}")
        
        staging_dir = Path("./staging")
        staging_dir.mkdir(exist_ok=True)
        
        files = self.raw_data.list(pattern)
        logging.info(f"Found {len(files)} files to download")
        
        self.raw_data.get(files, staging_dir, parallel=True)
        return list(staging_dir.glob(pattern))
    
    def process_files(self, input_files):
        """Process staged files."""
        processed_files = []
        
        for input_file in input_files:
            output_file = Path("./processed") / f"processed_{input_file.name}"
            
            # Your processing logic here
            self.run_processing_step(input_file, output_file)
            processed_files.append(output_file)
        
        return processed_files
    
    def upload_results(self, result_files):
        """Upload results to final location."""
        logging.info(f"Uploading {len(result_files)} result files")
        
        for result_file in result_files:
            self.results.put(
                result_file, 
                f"batch_{self.config['batch_id']}/{result_file.name}"
            )
    
    def run(self, data_pattern="*.nc"):
        """Execute the full pipeline."""
        try:
            # Stage input data
            input_files = self.stage_data(data_pattern)
            
            # Process data
            result_files = self.process_files(input_files)
            
            # Upload results
            self.upload_results(result_files)
            
            logging.info("Pipeline completed successfully!")
            
        except Exception as e:
            logging.error(f"Pipeline failed: {e}")
            raise

def main():
    parser = argparse.ArgumentParser(description="Data processing pipeline")
    parser.add_argument("--config", required=True, help="Configuration file")
    parser.add_argument("--pattern", default="*.nc", help="Data file pattern")
    parser.add_argument("--verbose", action="store_true", help="Verbose logging")
    
    args = parser.parse_args()
    
    # Setup logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(level=log_level, format='%(asctime)s - %(levelname)s - %(message)s')
    
    # Run pipeline
    pipeline = DataPipeline(args.config)
    pipeline.run(args.pattern)

if __name__ == "__main__":
    main()
```

### Configuration File

```yaml
# pipeline_config.yaml
simulation_name: "climate-processing-v2"
batch_id: "2024-01-15"

locations:
  raw_data:
    type: "ssh"
    host: "hpc.university.edu"
    username: "researcher"
    path: "/scratch/climate/raw"
    key_file: "~/.ssh/hpc_key"
  
  processed_data:
    type: "local"
    path: "./processed_data"
  
  results:
    type: "s3"
    bucket: "research-results"
    prefix: "climate/processed/"
    aws_profile: "research"

processing:
  chunk_size: "100MB"
  parallel_jobs: 4
  temp_dir: "./temp"
```

## Best Practices

### 1. Separation of Concerns

```python
# Keep data management separate from processing logic
class DataManager:
    def __init__(self, simulation):
        self.sim = simulation
        
    def get_input_data(self, experiment_id):
        """Handle all data retrieval."""
        pass
        
    def store_results(self, results, experiment_id):
        """Handle all data storage."""
        pass

class DataProcessor:
    def __init__(self):
        pass
        
    def run_analysis(self, input_data):
        """Pure processing logic."""
        pass

# Workflow orchestration
data_mgr = DataManager(sim)
processor = DataProcessor()

input_data = data_mgr.get_input_data("exp_001")
results = processor.run_analysis(input_data)
data_mgr.store_results(results, "exp_001")
```

### 2. Error Handling and Recovery

```python
def robust_data_transfer(location, files, destination, max_retries=3):
    """Transfer files with automatic retry and partial recovery."""
    failed_files = []
    
    for file in files:
        for attempt in range(max_retries):
            try:
                location.get(file, destination)
                break
            except tellus.TransferError as e:
                if attempt == max_retries - 1:
                    failed_files.append(file)
                    logging.error(f"Failed to transfer {file}: {e}")
                else:
                    logging.warning(f"Transfer attempt {attempt + 1} failed for {file}, retrying...")
                    time.sleep(2 ** attempt)  # Exponential backoff
    
    if failed_files:
        raise tellus.PartialTransferError(f"Failed to transfer: {failed_files}")
```

### 3. Resource Management

```python
# Use context managers for temporary resources
from contextlib import contextmanager

@contextmanager
def temporary_staging_area(base_path="./temp"):
    """Create and cleanup temporary staging directory.""" 
    staging_dir = Path(base_path) / f"staging_{uuid.uuid4()}"
    staging_dir.mkdir(parents=True)
    
    try:
        yield staging_dir
    finally:
        shutil.rmtree(staging_dir)

# Usage in workflow
with temporary_staging_area() as staging:
    remote.get("*.nc", staging)
    process_files(staging.glob("*.nc"))
    # Automatic cleanup
```

## Next Steps

- See {doc}`../user-guide/index` for comprehensive workflow patterns
- Explore {doc}`../api/index` for complete API reference  
- Check {doc}`../development/index` for custom integrations