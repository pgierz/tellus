# CI/CD Integration Testing Strategies for Tellus

This document outlines comprehensive CI/CD pipeline integration strategies for testing the tellus Earth science data archive system in research environments.

## Overview

The tellus system requires specialized testing approaches due to its use in HPC/research environments with:
- Intermittent network connectivity
- Large data files
- Multiple storage backends (local, SSH, S3, tape)
- Concurrent scientific workflows
- Resource constraints

## CI/CD Pipeline Architecture

### Multi-Environment Testing Strategy

```yaml
# .github/workflows/integration-tests.yml
name: Integration Tests

on:
  push:
    branches: [ main, develop ]
  pull_request:
    branches: [ main ]
  schedule:
    # Run nightly at 2 AM UTC
    - cron: '0 2 * * *'

jobs:
  unit-tests:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        python-version: [3.11, 3.12]
    steps:
      - uses: actions/checkout@v4
      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: ${{ matrix.python-version }}
      - name: Install dependencies
        run: |
          pip install -e .
          pip install pytest pytest-xdist pytest-cov
      - name: Run unit tests
        run: pytest tests/ -m "not integration" --cov=tellus

  integration-tests-basic:
    runs-on: ubuntu-latest
    needs: unit-tests
    steps:
      - uses: actions/checkout@v4
      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.11'
      - name: Install dependencies
        run: |
          pip install -e .
          pip install pytest pytest-xdist
      - name: Run basic integration tests
        run: |
          pytest tests/integration/ -m "not slow and not network" \
            --maxfail=5 --tb=short
        timeout-minutes: 15

  integration-tests-extended:
    runs-on: ubuntu-latest
    needs: integration-tests-basic
    if: github.event_name == 'schedule' || contains(github.event.head_commit.message, '[extended-tests]')
    steps:
      - uses: actions/checkout@v4
      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.11'
      - name: Install dependencies
        run: |
          pip install -e .
          pip install pytest pytest-xdist pytest-timeout
      - name: Run extended integration tests
        run: |
          pytest tests/integration/ -m "slow or performance" \
            --timeout=300 --maxfail=3
        timeout-minutes: 45

  hpc-simulation:
    runs-on: ubuntu-latest
    needs: unit-tests
    if: github.event_name == 'schedule'
    steps:
      - uses: actions/checkout@v4
      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.11'
      - name: Install dependencies
        run: |
          pip install -e .
          pip install pytest docker
      - name: Start HPC simulation environment
        run: |
          docker run -d --name ssh-server \
            -p 2222:22 \
            -v $PWD/test-data:/data \
            rastasheep/ubuntu-sshd:18.04
          
          # Wait for SSH server to be ready
          sleep 10
      - name: Run HPC-specific tests
        run: |
          pytest tests/integration/ -m "network" \
            --ssh-host=localhost --ssh-port=2222 \
            --ssh-user=root --ssh-password=root
        timeout-minutes: 20
      - name: Cleanup
        run: docker stop ssh-server && docker rm ssh-server
```

### Research Environment Specific Configurations

```yaml
# .github/workflows/research-env-tests.yml
name: Research Environment Tests

on:
  workflow_dispatch:
    inputs:
      environment:
        description: 'Target research environment'
        required: true
        default: 'local'
        type: choice
        options:
        - local
        - hpc-cluster
        - cloud-hpc

jobs:
  test-research-environment:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Configure environment-specific settings
        run: |
          case "${{ github.event.inputs.environment }}" in
            "hpc-cluster")
              echo "TELLUS_TEST_CONFIG=hpc_cluster" >> $GITHUB_ENV
              echo "TELLUS_TIMEOUT_MULTIPLIER=3" >> $GITHUB_ENV
              echo "TELLUS_CONCURRENT_LIMIT=2" >> $GITHUB_ENV
              ;;
            "cloud-hpc")
              echo "TELLUS_TEST_CONFIG=cloud_hpc" >> $GITHUB_ENV
              echo "TELLUS_TIMEOUT_MULTIPLIER=5" >> $GITHUB_ENV
              echo "TELLUS_CONCURRENT_LIMIT=1" >> $GITHUB_ENV
              ;;
            *)
              echo "TELLUS_TEST_CONFIG=local" >> $GITHUB_ENV
              echo "TELLUS_TIMEOUT_MULTIPLIER=1" >> $GITHUB_ENV
              echo "TELLUS_CONCURRENT_LIMIT=4" >> $GITHUB_ENV
              ;;
          esac
      - name: Run environment-specific tests
        run: |
          pytest tests/integration/ \
            --env-config=$TELLUS_TEST_CONFIG \
            --timeout-multiplier=$TELLUS_TIMEOUT_MULTIPLIER \
            --max-concurrent=$TELLUS_CONCURRENT_LIMIT
```

## Test Data Management

### Large File Testing Strategy

```python
# tests/integration/test_data_manager.py
import os
import tempfile
from pathlib import Path

class TestDataManager:
    """Manage test data for integration tests in CI/CD."""
    
    def __init__(self):
        self.data_cache = Path.home() / ".tellus_test_cache"
        self.data_cache.mkdir(exist_ok=True)
    
    def get_test_archive(self, size_mb=10, file_count=100):
        """Get or create a test archive of specified size."""
        cache_key = f"archive_{size_mb}mb_{file_count}files.tar.gz"
        cache_path = self.data_cache / cache_key
        
        if not cache_path.exists():
            self._create_test_archive(cache_path, size_mb, file_count)
        
        return cache_path
    
    def _create_test_archive(self, output_path, size_mb, file_count):
        """Create a test archive with specified characteristics."""
        import tarfile
        import io
        
        file_size = (size_mb * 1024 * 1024) // file_count
        
        with tarfile.open(output_path, 'w:gz') as tar:
            for i in range(file_count):
                # Create synthetic scientific data patterns
                if i % 10 == 0:
                    # NetCDF-like files
                    filename = f"data/output_{i:04d}.nc"
                    content = self._generate_netcdf_like_data(file_size)
                elif i % 5 == 0:
                    # Log files
                    filename = f"logs/run_{i:04d}.log"
                    content = self._generate_log_data(file_size)
                else:
                    # General data files
                    filename = f"results/file_{i:04d}.dat"
                    content = self._generate_binary_data(file_size)
                
                info = tarfile.TarInfo(name=filename)
                info.size = len(content)
                tar.addfile(info, io.BytesIO(content))
    
    def _generate_netcdf_like_data(self, size):
        """Generate NetCDF-like test data."""
        # Simulate structured scientific data
        header = b"CDF\x01" + b"\x00" * 100  # Mock NetCDF header
        data_pattern = b"scientific_data_pattern" * (size // 20)
        return header + data_pattern[:size - len(header)]
    
    def _generate_log_data(self, size):
        """Generate log-like test data."""
        log_lines = [
            "2024-01-01 00:00:00 INFO: Simulation started",
            "2024-01-01 00:01:00 DEBUG: Processing timestep 1",
            "2024-01-01 00:02:00 WARNING: High memory usage detected",
            "2024-01-01 00:03:00 INFO: Checkpoint saved",
        ]
        
        content = "\n".join(log_lines * (size // 200)).encode()
        return content[:size]
    
    def _generate_binary_data(self, size):
        """Generate binary test data."""
        import random
        return bytes([random.randint(0, 255) for _ in range(size)])
```

### Test Configuration Management

```python
# tests/integration/config.py
import os
from dataclasses import dataclass
from typing import Dict, List, Optional

@dataclass
class TestEnvironmentConfig:
    """Configuration for different test environments."""
    name: str
    timeout_multiplier: float
    max_concurrent_operations: int
    network_simulation: bool
    large_file_tests: bool
    storage_backends: List[str]
    resource_limits: Dict[str, int]

class TestConfigManager:
    """Manage test configurations for different environments."""
    
    CONFIGS = {
        'local': TestEnvironmentConfig(
            name='local',
            timeout_multiplier=1.0,
            max_concurrent_operations=8,
            network_simulation=False,
            large_file_tests=True,
            storage_backends=['file'],
            resource_limits={'memory_mb': 512, 'disk_mb': 1024}
        ),
        'ci': TestEnvironmentConfig(
            name='ci',
            timeout_multiplier=2.0,
            max_concurrent_operations=4,
            network_simulation=True,
            large_file_tests=False,  # Skip large files in CI
            storage_backends=['file', 'mock_ssh'],
            resource_limits={'memory_mb': 256, 'disk_mb': 512}
        ),
        'hpc_cluster': TestEnvironmentConfig(
            name='hpc_cluster',
            timeout_multiplier=5.0,
            max_concurrent_operations=2,
            network_simulation=True,
            large_file_tests=True,
            storage_backends=['file', 'ssh', 'mock_tape'],
            resource_limits={'memory_mb': 128, 'disk_mb': 256}
        )
    }
    
    @classmethod
    def get_config(cls, env_name: Optional[str] = None) -> TestEnvironmentConfig:
        """Get configuration for specified environment."""
        if env_name is None:
            env_name = os.environ.get('TELLUS_TEST_CONFIG', 'local')
        
        return cls.CONFIGS.get(env_name, cls.CONFIGS['local'])
```

## Specialized Test Execution

### Distributed Testing for HPC Environments

```python
# tests/integration/distributed_runner.py
import asyncio
import json
import subprocess
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from typing import List, Dict, Any

class DistributedTestRunner:
    """Run integration tests across multiple processes/nodes."""
    
    def __init__(self, config: TestEnvironmentConfig):
        self.config = config
        self.results = []
        
    async def run_distributed_tests(self, test_modules: List[str]) -> Dict[str, Any]:
        """Run tests distributed across available resources."""
        
        # Split tests based on available resources
        test_groups = self._distribute_tests(test_modules)
        
        # Run test groups in parallel
        tasks = []
        for group_id, tests in test_groups.items():
            task = asyncio.create_task(
                self._run_test_group(group_id, tests)
            )
            tasks.append(task)
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Aggregate results
        return self._aggregate_results(results)
    
    def _distribute_tests(self, test_modules: List[str]) -> Dict[str, List[str]]:
        """Distribute tests across available resources."""
        max_groups = self.config.max_concurrent_operations
        
        groups = {}
        for i, test_module in enumerate(test_modules):
            group_id = f"group_{i % max_groups}"
            if group_id not in groups:
                groups[group_id] = []
            groups[group_id].append(test_module)
        
        return groups
    
    async def _run_test_group(self, group_id: str, tests: List[str]) -> Dict[str, Any]:
        """Run a group of tests."""
        cmd = [
            'pytest',
            *tests,
            '--json-report',
            f'--json-report-file=/tmp/test_results_{group_id}.json',
            '--tb=short',
            f'--timeout={30 * self.config.timeout_multiplier}',
        ]
        
        if not self.config.large_file_tests:
            cmd.extend(['-m', 'not slow'])
        
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        
        stdout, stderr = await process.communicate()
        
        # Load test results
        results_file = Path(f'/tmp/test_results_{group_id}.json')
        if results_file.exists():
            with open(results_file) as f:
                results = json.load(f)
        else:
            results = {'tests': [], 'summary': {'failed': 1}}
        
        return {
            'group_id': group_id,
            'return_code': process.returncode,
            'stdout': stdout.decode(),
            'stderr': stderr.decode(),
            'results': results
        }
    
    def _aggregate_results(self, results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Aggregate results from all test groups."""
        total_tests = 0
        passed_tests = 0
        failed_tests = 0
        skipped_tests = 0
        
        all_failures = []
        
        for result in results:
            if isinstance(result, Exception):
                failed_tests += 1
                all_failures.append(str(result))
                continue
            
            test_results = result.get('results', {})
            summary = test_results.get('summary', {})
            
            total_tests += summary.get('total', 0)
            passed_tests += summary.get('passed', 0)
            failed_tests += summary.get('failed', 0)
            skipped_tests += summary.get('skipped', 0)
            
            # Collect failure details
            for test in test_results.get('tests', []):
                if test.get('outcome') == 'failed':
                    all_failures.append({
                        'test': test.get('nodeid'),
                        'message': test.get('call', {}).get('longrepr')
                    })
        
        return {
            'total_tests': total_tests,
            'passed': passed_tests,
            'failed': failed_tests,
            'skipped': skipped_tests,
            'success_rate': passed_tests / max(total_tests, 1),
            'failures': all_failures
        }
```

### Test Isolation and Cleanup

```python
# tests/integration/isolation.py
import atexit
import os
import shutil
import tempfile
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Generator

class TestIsolationManager:
    """Manage test isolation and cleanup."""
    
    def __init__(self):
        self.cleanup_paths = set()
        self.cleanup_processes = set()
        self.lock = threading.Lock()
        
        # Register cleanup on exit
        atexit.register(self.cleanup_all)
    
    @contextmanager
    def isolated_workspace(self) -> Generator[Path, None, None]:
        """Create isolated workspace for tests."""
        workspace = Path(tempfile.mkdtemp(prefix="tellus_test_"))
        
        with self.lock:
            self.cleanup_paths.add(workspace)
        
        try:
            # Set up isolated environment
            os.environ['TELLUS_CACHE_DIR'] = str(workspace / 'cache')
            os.environ['TELLUS_CONFIG_DIR'] = str(workspace / 'config')
            
            yield workspace
        finally:
            # Clean up environment variables
            for var in ['TELLUS_CACHE_DIR', 'TELLUS_CONFIG_DIR']:
                os.environ.pop(var, None)
    
    def register_cleanup_path(self, path: Path):
        """Register a path for cleanup."""
        with self.lock:
            self.cleanup_paths.add(path)
    
    def cleanup_all(self):
        """Clean up all registered resources."""
        with self.lock:
            # Clean up paths
            for path in self.cleanup_paths:
                try:
                    if path.exists():
                        if path.is_dir():
                            shutil.rmtree(path)
                        else:
                            path.unlink()
                except Exception as e:
                    print(f"Failed to cleanup {path}: {e}")
            
            self.cleanup_paths.clear()
```

## Performance Monitoring in CI/CD

### Continuous Performance Tracking

```python
# tests/integration/performance_tracking.py
import json
import time
from pathlib import Path
from typing import Dict, List, Optional

class CIPerformanceTracker:
    """Track performance metrics across CI/CD runs."""
    
    def __init__(self, results_dir: Path):
        self.results_dir = results_dir
        self.results_dir.mkdir(exist_ok=True)
        self.current_run = {
            'timestamp': time.time(),
            'git_commit': self._get_git_commit(),
            'metrics': {}
        }
    
    def record_metric(self, test_name: str, metric_name: str, value: float):
        """Record a performance metric."""
        if test_name not in self.current_run['metrics']:
            self.current_run['metrics'][test_name] = {}
        
        self.current_run['metrics'][test_name][metric_name] = value
    
    def save_results(self):
        """Save current run results."""
        filename = f"perf_results_{int(self.current_run['timestamp'])}.json"
        results_file = self.results_dir / filename
        
        with open(results_file, 'w') as f:
            json.dump(self.current_run, f, indent=2)
    
    def analyze_trends(self, days_back: int = 30) -> Dict[str, List[Dict]]:
        """Analyze performance trends over time."""
        cutoff_time = time.time() - (days_back * 24 * 3600)
        
        trend_data = {}
        
        for results_file in self.results_dir.glob("perf_results_*.json"):
            try:
                with open(results_file) as f:
                    data = json.load(f)
                
                if data['timestamp'] < cutoff_time:
                    continue
                
                for test_name, metrics in data['metrics'].items():
                    if test_name not in trend_data:
                        trend_data[test_name] = []
                    
                    trend_data[test_name].append({
                        'timestamp': data['timestamp'],
                        'commit': data['git_commit'],
                        'metrics': metrics
                    })
            except Exception:
                continue
        
        return trend_data
    
    def detect_regressions(self, threshold_percent: float = 20) -> List[Dict]:
        """Detect performance regressions."""
        trends = self.analyze_trends()
        regressions = []
        
        for test_name, history in trends.items():
            if len(history) < 2:
                continue
            
            # Sort by timestamp
            history.sort(key=lambda x: x['timestamp'])
            
            latest = history[-1]
            baseline = history[-2] if len(history) >= 2 else history[0]
            
            for metric_name in latest['metrics']:
                if metric_name not in baseline['metrics']:
                    continue
                
                latest_value = latest['metrics'][metric_name]
                baseline_value = baseline['metrics'][metric_name]
                
                if baseline_value == 0:
                    continue
                
                change_percent = ((latest_value - baseline_value) / baseline_value) * 100
                
                if change_percent > threshold_percent:
                    regressions.append({
                        'test': test_name,
                        'metric': metric_name,
                        'baseline_value': baseline_value,
                        'current_value': latest_value,
                        'change_percent': change_percent,
                        'baseline_commit': baseline['commit'],
                        'current_commit': latest['commit']
                    })
        
        return regressions
    
    def _get_git_commit(self) -> Optional[str]:
        """Get current git commit hash."""
        try:
            import subprocess
            result = subprocess.run(
                ['git', 'rev-parse', 'HEAD'],
                capture_output=True,
                text=True,
                timeout=5
            )
            return result.stdout.strip() if result.returncode == 0 else None
        except Exception:
            return None
```

## Environment-Specific Test Configurations

### HPC Cluster Testing

```bash
#!/bin/bash
# scripts/run_hpc_tests.sh
# Script for running tests on HPC clusters

set -e

# Configuration
SLURM_JOB_NAME="tellus-integration-tests"
SLURM_PARTITION="test"
SLURM_TIME="02:00:00"
SLURM_NODES=2
SLURM_TASKS_PER_NODE=4

# Create SLURM job script
cat > hpc_test_job.slurm << EOF
#!/bin/bash
#SBATCH --job-name=$SLURM_JOB_NAME
#SBATCH --partition=$SLURM_PARTITION
#SBATCH --time=$SLURM_TIME
#SBATCH --nodes=$SLURM_NODES
#SBATCH --ntasks-per-node=$SLURM_TASKS_PER_NODE
#SBATCH --output=tellus_test_%j.out
#SBATCH --error=tellus_test_%j.err

module load python/3.11
module load mpi/openmpi

# Set up environment
export TELLUS_TEST_CONFIG=hpc_cluster
export TELLUS_TIMEOUT_MULTIPLIER=5
export TELLUS_HPC_NODES=\$SLURM_JOB_NUM_NODES
export TELLUS_HPC_TASKS=\$SLURM_NTASKS

# Run tests with MPI if needed
if [ "\$SLURM_NTASKS" -gt 1 ]; then
    mpirun python -m pytest tests/integration/ \\
        --env-config=hpc_cluster \\
        --distributed \\
        --maxfail=5
else
    python -m pytest tests/integration/ \\
        --env-config=hpc_cluster \\
        --maxfail=5
fi
EOF

# Submit job
sbatch hpc_test_job.slurm

echo "HPC integration tests submitted. Check job status with 'squeue -u \$USER'"
```

### Cloud HPC Testing

```yaml
# .github/workflows/cloud-hpc-tests.yml
name: Cloud HPC Integration Tests

on:
  workflow_dispatch:
  schedule:
    - cron: '0 6 * * 1'  # Weekly on Monday

jobs:
  cloud-hpc-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      
      - name: Configure AWS credentials
        uses: aws-actions/configure-aws-credentials@v2
        with:
          aws-access-key-id: ${{ secrets.AWS_ACCESS_KEY_ID }}
          aws-secret-access-key: ${{ secrets.AWS_SECRET_ACCESS_KEY }}
          aws-region: us-west-2
      
      - name: Launch HPC cluster
        run: |
          # Use AWS ParallelCluster or similar
          pcluster create-cluster \
            --cluster-name tellus-test-cluster \
            --cluster-configuration hpc-config.yaml
          
          # Wait for cluster to be ready
          pcluster describe-cluster \
            --cluster-name tellus-test-cluster \
            --query "clusterStatus" \
            --output text
      
      - name: Run distributed tests
        run: |
          # SSH into head node and run tests
          pcluster ssh \
            --cluster-name tellus-test-cluster \
            -i ~/.ssh/tellus-test-key.pem \
            -c "cd /shared/tellus && ./scripts/run_distributed_tests.sh"
      
      - name: Collect results
        run: |
          # Copy test results back
          pcluster ssh \
            --cluster-name tellus-test-cluster \
            -i ~/.ssh/tellus-test-key.pem \
            -c "tar -czf test-results.tar.gz /shared/tellus/test-results/"
          
          scp -i ~/.ssh/tellus-test-key.pem \
            ec2-user@$(pcluster describe-cluster --cluster-name tellus-test-cluster --query "headNode.publicIpAddress" --output text):/shared/test-results.tar.gz .
      
      - name: Upload test results
        uses: actions/upload-artifact@v3
        with:
          name: cloud-hpc-test-results
          path: test-results.tar.gz
      
      - name: Cleanup cluster
        if: always()
        run: |
          pcluster delete-cluster \
            --cluster-name tellus-test-cluster
```

## Monitoring and Alerting

### Test Failure Alerting

```python
# scripts/test_monitor.py
import json
import requests
from typing import Dict, List

class TestMonitor:
    """Monitor test results and send alerts."""
    
    def __init__(self, webhook_url: str, alert_threshold: float = 0.8):
        self.webhook_url = webhook_url
        self.alert_threshold = alert_threshold
    
    def analyze_test_results(self, results_file: str) -> Dict:
        """Analyze test results and determine if alerts are needed."""
        with open(results_file) as f:
            results = json.load(f)
        
        total_tests = results.get('total', 0)
        passed_tests = results.get('passed', 0)
        failed_tests = results.get('failed', 0)
        
        success_rate = passed_tests / max(total_tests, 1)
        
        analysis = {
            'success_rate': success_rate,
            'total_tests': total_tests,
            'passed_tests': passed_tests,
            'failed_tests': failed_tests,
            'needs_alert': success_rate < self.alert_threshold,
            'severity': self._determine_severity(success_rate)
        }
        
        return analysis
    
    def send_alert(self, analysis: Dict, additional_info: Dict = None):
        """Send alert about test failures."""
        if not analysis['needs_alert']:
            return
        
        message = self._format_alert_message(analysis, additional_info)
        
        payload = {
            'text': message,
            'color': 'danger' if analysis['severity'] == 'critical' else 'warning'
        }
        
        try:
            response = requests.post(self.webhook_url, json=payload, timeout=10)
            response.raise_for_status()
        except Exception as e:
            print(f"Failed to send alert: {e}")
    
    def _determine_severity(self, success_rate: float) -> str:
        """Determine alert severity based on success rate."""
        if success_rate < 0.5:
            return 'critical'
        elif success_rate < 0.8:
            return 'warning'
        else:
            return 'info'
    
    def _format_alert_message(self, analysis: Dict, additional_info: Dict = None) -> str:
        """Format alert message."""
        severity = analysis['severity'].upper()
        success_rate = analysis['success_rate'] * 100
        
        message = f"""
🚨 {severity}: Tellus Integration Test Failure

Success Rate: {success_rate:.1f}%
Total Tests: {analysis['total_tests']}
Failed Tests: {analysis['failed_tests']}
"""
        
        if additional_info:
            message += f"\nEnvironment: {additional_info.get('environment', 'unknown')}"
            message += f"\nCommit: {additional_info.get('commit', 'unknown')}"
            message += f"\nBranch: {additional_info.get('branch', 'unknown')}"
        
        return message
```

This comprehensive CI/CD integration strategy provides:

1. **Multi-environment testing** with different configurations for local, CI, and HPC environments
2. **Distributed test execution** for parallel processing across multiple nodes
3. **Test data management** for handling large scientific datasets
4. **Performance monitoring** with regression detection
5. **Environment-specific configurations** for different research computing environments
6. **Monitoring and alerting** for test failures and performance issues

The strategy is designed to work reliably in research environments with intermittent connectivity and varying resource constraints while providing comprehensive coverage of the tellus system's capabilities.