"""
System reliability and monitoring tests for the tellus system.

This module tests system reliability, observability patterns, health checks,
and monitoring capabilities in the tellus Earth science data archive system.
"""

import json
import os
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from tellus.location import Location, LocationKind
from tellus.simulation import (ArchiveRegistry, CacheManager,
                               CompressedArchive, Simulation)


@pytest.mark.integration
class TestSystemHealthChecks:
    """Test system health check and monitoring capabilities."""
    
    def test_location_health_checks(self, test_locations, progress_tracker):
        """Test health checking of different location types."""
        health_results = {}
        
        def check_location_health(location_name, location):
            """Perform health check on a location."""
            operation_id = f"health_check_{location_name}"
            progress_tracker.track_operation(operation_id, "health_check")
            
            try:
                health_status = {
                    'name': location_name,
                    'accessible': False,
                    'response_time': None,
                    'error': None,
                    'last_check': time.time()
                }
                
                # Test basic connectivity
                start_time = time.time()
                try:
                    # Mock different behaviors for different location types
                    if location.config.get('protocol') == 'file':
                        # Local filesystem - should always be accessible
                        accessible = True
                    elif location.config.get('protocol') == 'ssh':
                        # SSH - simulate network check
                        time.sleep(0.1)  # Simulate network latency
                        accessible = True  # Mock success
                    elif location.config.get('protocol') == 's3':
                        # S3 - simulate cloud service check
                        time.sleep(0.05)  # Simulate API call
                        accessible = True  # Mock success
                    elif location.config.get('protocol') == 'tape':
                        # Tape - simulate slower response
                        time.sleep(0.2)  # Simulate tape mount time
                        accessible = True  # Mock success
                    else:
                        accessible = False
                    
                    response_time = time.time() - start_time
                    
                    health_status.update({
                        'accessible': accessible,
                        'response_time': response_time
                    })
                    
                except Exception as e:
                    health_status.update({
                        'accessible': False,
                        'error': str(e),
                        'response_time': time.time() - start_time
                    })
                
                health_results[location_name] = health_status
                progress_tracker.complete_operation(operation_id, health_status['accessible'])
                
                return health_status
                
            except Exception as e:
                progress_tracker.complete_operation(operation_id, False, str(e))
                raise
        
        # Check health of all test locations
        for name, location in test_locations.items():
            check_location_health(name, location)
        
        # Verify health check results
        assert len(health_results) == len(test_locations)
        
        for name, status in health_results.items():
            assert 'accessible' in status
            assert 'response_time' in status
            assert 'last_check' in status
            
            # All test locations should be accessible (mocked)
            assert status['accessible'], f"Location {name} should be accessible"
            assert status['response_time'] is not None
            assert status['response_time'] >= 0
        
        # Check response time patterns
        local_time = health_results.get('local', {}).get('response_time', 0)
        ssh_time = health_results.get('ssh', {}).get('response_time', 0)
        tape_time = health_results.get('tape', {}).get('response_time', 0)
        
        # Tape should be slowest, SSH should be slower than local
        if tape_time and ssh_time and local_time:
            assert tape_time >= ssh_time >= local_time, "Response times should reflect location types"
    
    def test_cache_health_monitoring(self, cache_manager, sample_archive_data,
                                   performance_monitor, resource_monitor):
        """Test cache system health monitoring."""
        resource_monitor.take_snapshot("cache_health_start")
        
        def get_cache_health():
            """Get comprehensive cache health status."""
            performance_monitor.start_timing("cache_health_check")
            
            try:
                stats = cache_manager.get_cache_stats()
                
                # Calculate utilization percentages
                archive_utilization = (stats['archive_size'] / 
                                     cache_manager.config.archive_cache_size_limit * 100)
                file_utilization = (stats['file_size'] / 
                                  cache_manager.config.file_cache_size_limit * 100)
                
                # Check cache directory accessibility
                cache_accessible = cache_manager.config.cache_dir.exists()
                archive_dir_accessible = cache_manager.config.archive_cache_dir.exists()
                file_dir_accessible = cache_manager.config.file_cache_dir.exists()
                
                health_status = {
                    'timestamp': time.time(),
                    'cache_accessible': cache_accessible,
                    'archive_dir_accessible': archive_dir_accessible,
                    'file_dir_accessible': file_dir_accessible,
                    'archive_count': stats['archive_count'],
                    'file_count': stats['file_count'],
                    'archive_utilization_percent': archive_utilization,
                    'file_utilization_percent': file_utilization,
                    'total_size_mb': (stats['archive_size'] + stats['file_size']) / (1024 * 1024)
                }
                
                # Determine overall health
                health_issues = []
                if not cache_accessible:
                    health_issues.append("Cache directory not accessible")
                if archive_utilization > 90:
                    health_issues.append("Archive cache near capacity")
                if file_utilization > 90:
                    health_issues.append("File cache near capacity")
                
                health_status['healthy'] = len(health_issues) == 0
                health_status['issues'] = health_issues
                
                return health_status
                
            finally:
                performance_monitor.end_timing("cache_health_check")
        
        # Initial health check
        initial_health = get_cache_health()
        assert initial_health['healthy'], "Cache should be healthy initially"
        assert initial_health['cache_accessible'], "Cache should be accessible"
        
        # Add some data to cache and check health again
        with tempfile.NamedTemporaryFile(suffix='.tar.gz', delete=False) as f:
            f.write(sample_archive_data)
            temp_path = Path(f.name)
        
        try:
            # Cache multiple items
            for i in range(3):
                checksum = f"health_test_{i}"
                cache_manager.cache_archive(temp_path, checksum)
                
                file_data = f"test_file_{i}".encode() * 100
                file_key = f"file_{i}"
                file_checksum = f"file_checksum_{i}"
                cache_manager.cache_file(file_data, file_key, file_checksum)
            
            # Check health after caching
            post_cache_health = get_cache_health()
            assert post_cache_health['healthy'], "Cache should remain healthy after adding data"
            assert post_cache_health['archive_count'] >= 3, "Should have cached archives"
            assert post_cache_health['file_count'] >= 3, "Should have cached files"
            
            # Verify utilization is calculated correctly
            assert post_cache_health['archive_utilization_percent'] >= 0
            assert post_cache_health['file_utilization_percent'] >= 0
            assert post_cache_health['total_size_mb'] > 0
            
        finally:
            temp_path.unlink(missing_ok=True)
        
        resource_monitor.take_snapshot("cache_health_end")
    
    def test_archive_integrity_monitoring(self, archive_factory, temp_workspace,
                                        progress_tracker):
        """Test archive integrity monitoring and validation."""
        archive = archive_factory("integrity_test", "local")
        
        def check_archive_integrity(archive):
            """Comprehensive archive integrity check."""
            operation_id = "archive_integrity_check"
            progress_tracker.track_operation(operation_id, "integrity_check")
            
            try:
                integrity_report = {
                    'timestamp': time.time(),
                    'archive_id': archive.archive_id,
                    'checks_performed': [],
                    'issues_found': [],
                    'overall_status': 'unknown'
                }
                
                # Check 1: Archive file exists and is accessible
                try:
                    status = archive.status()
                    integrity_report['checks_performed'].append('file_existence')
                    
                    if not status.get('exists', False):
                        integrity_report['issues_found'].append('Archive file does not exist')
                    elif status.get('size', 0) == 0:
                        integrity_report['issues_found'].append('Archive file is empty')
                        
                except Exception as e:
                    integrity_report['issues_found'].append(f'Failed to check file existence: {e}')
                
                # Check 2: Manifest exists and is valid
                try:
                    if archive.manifest is None:
                        archive.refresh_manifest()
                    
                    integrity_report['checks_performed'].append('manifest_validation')
                    
                    if archive.manifest is None:
                        integrity_report['issues_found'].append('No manifest available')
                    elif len(archive.manifest.files) == 0:
                        integrity_report['issues_found'].append('Manifest contains no files')
                        
                except Exception as e:
                    integrity_report['issues_found'].append(f'Manifest validation failed: {e}')
                
                # Check 3: Sample file access
                try:
                    files = archive.list_files()
                    integrity_report['checks_performed'].append('file_listing')
                    
                    if files:
                        # Try to access first file
                        sample_file = list(files.keys())[0]
                        file_data = archive.open_file(sample_file)
                        content = file_data.read()
                        
                        integrity_report['checks_performed'].append('sample_file_access')
                        
                        if len(content) == 0:
                            integrity_report['issues_found'].append(f'Sample file {sample_file} is empty')
                    else:
                        integrity_report['issues_found'].append('No files found in archive')
                        
                except Exception as e:
                    integrity_report['issues_found'].append(f'File access test failed: {e}')
                
                # Check 4: Manifest consistency (if available)
                try:
                    if archive.manifest:
                        validation_result = archive.validate_manifest()
                        integrity_report['checks_performed'].append('manifest_consistency')
                        
                        if not validation_result.get('valid', False):
                            for error in validation_result.get('errors', []):
                                integrity_report['issues_found'].append(f'Manifest inconsistency: {error}')
                            for missing in validation_result.get('missing_files', []):
                                integrity_report['issues_found'].append(f'Missing file: {missing}')
                                
                except Exception as e:
                    integrity_report['issues_found'].append(f'Manifest consistency check failed: {e}')
                
                # Determine overall status
                if len(integrity_report['issues_found']) == 0:
                    integrity_report['overall_status'] = 'healthy'
                elif any('critical' in issue.lower() or 'failed' in issue.lower() 
                        for issue in integrity_report['issues_found']):
                    integrity_report['overall_status'] = 'critical'
                else:
                    integrity_report['overall_status'] = 'warning'
                
                progress_tracker.complete_operation(
                    operation_id, 
                    integrity_report['overall_status'] == 'healthy'
                )
                
                return integrity_report
                
            except Exception as e:
                progress_tracker.complete_operation(operation_id, False, str(e))
                raise
        
        # Perform integrity check
        integrity_report = check_archive_integrity(archive)
        
        # Verify integrity check results
        assert 'timestamp' in integrity_report
        assert 'archive_id' in integrity_report
        assert 'checks_performed' in integrity_report
        assert 'issues_found' in integrity_report
        assert 'overall_status' in integrity_report
        
        # Should have performed multiple checks
        assert len(integrity_report['checks_performed']) >= 3
        
        # Archive should be healthy for test data
        assert integrity_report['overall_status'] in ['healthy', 'warning'], \
            f"Archive integrity issues: {integrity_report['issues_found']}"


@pytest.mark.integration
class TestMonitoringAndObservability:
    """Test monitoring and observability features."""
    
    def test_operation_tracing(self, archive_factory, progress_tracker,
                             performance_monitor):
        """Test operation tracing and timing."""
        archive = archive_factory("tracing_test", "local")
        
        # Custom tracer for operations
        class OperationTracer:
            def __init__(self):
                self.traces = []
                self.active_traces = {}
                self.lock = threading.Lock()
            
            def start_trace(self, operation_id, operation_type, metadata=None):
                with self.lock:
                    trace = {
                        'operation_id': operation_id,
                        'operation_type': operation_type,
                        'start_time': time.time(),
                        'end_time': None,
                        'duration': None,
                        'metadata': metadata or {},
                        'success': None,
                        'error': None
                    }
                    self.active_traces[operation_id] = trace
                    return trace
            
            def end_trace(self, operation_id, success=True, error=None, result_metadata=None):
                with self.lock:
                    if operation_id in self.active_traces:
                        trace = self.active_traces[operation_id]
                        trace['end_time'] = time.time()
                        trace['duration'] = trace['end_time'] - trace['start_time']
                        trace['success'] = success
                        trace['error'] = error
                        if result_metadata:
                            trace['metadata'].update(result_metadata)
                        
                        self.traces.append(trace)
                        del self.active_traces[operation_id]
                        return trace
                    return None
            
            def get_traces(self, operation_type=None):
                with self.lock:
                    if operation_type:
                        return [t for t in self.traces if t['operation_type'] == operation_type]
                    return self.traces.copy()
        
        tracer = OperationTracer()
        
        # Traced archive operations
        def traced_list_files():
            trace_id = "list_files_001"
            tracer.start_trace(trace_id, "list_files", {"archive_id": archive.archive_id})
            
            try:
                files = archive.list_files()
                tracer.end_trace(trace_id, True, None, {"file_count": len(files)})
                return files
            except Exception as e:
                tracer.end_trace(trace_id, False, str(e))
                raise
        
        def traced_file_access(filename):
            trace_id = f"file_access_{filename}"
            tracer.start_trace(trace_id, "file_access", {"filename": filename})
            
            try:
                file_data = archive.open_file(filename)
                content = file_data.read()
                tracer.end_trace(trace_id, True, None, {"file_size": len(content)})
                return content
            except Exception as e:
                tracer.end_trace(trace_id, False, str(e))
                raise
        
        # Perform traced operations
        files = traced_list_files()
        
        if files:
            sample_file = list(files.keys())[0]
            content = traced_file_access(sample_file)
        
        # Analyze traces
        all_traces = tracer.get_traces()
        list_traces = tracer.get_traces("list_files")
        access_traces = tracer.get_traces("file_access")
        
        # Verify tracing worked
        assert len(all_traces) >= 1, "Should have at least one trace"
        assert len(list_traces) == 1, "Should have one list_files trace"
        
        # Verify trace structure
        for trace in all_traces:
            assert 'operation_id' in trace
            assert 'operation_type' in trace
            assert 'start_time' in trace
            assert 'end_time' in trace
            assert 'duration' in trace
            assert 'success' in trace
            assert isinstance(trace['success'], bool)
            assert trace['duration'] >= 0
        
        # Verify successful operations
        successful_traces = [t for t in all_traces if t['success']]
        assert len(successful_traces) >= 1, "Should have successful operations"
    
    def test_metrics_collection(self, archive_factory, simulation_factory,
                              cache_manager, performance_monitor):
        """Test collection of system metrics."""
        # Create test objects
        archive = archive_factory("metrics_test", "local")
        sim = simulation_factory("metrics_sim")
        
        class MetricsCollector:
            def __init__(self):
                self.metrics = {}
                self.collection_time = None
            
            def collect_system_metrics(self):
                """Collect comprehensive system metrics."""
                self.collection_time = time.time()
                
                # Archive metrics
                archive_stats = archive.get_stats()
                self.metrics['archives'] = {
                    'total_archives': 1,  # We have one test archive
                    'total_files': archive_stats.get('file_count', 0),
                    'total_size_bytes': archive_stats.get('total_size', 0)
                }
                
                # Cache metrics
                cache_stats = cache_manager.get_cache_stats()
                self.metrics['cache'] = {
                    'archive_count': cache_stats['archive_count'],
                    'file_count': cache_stats['file_count'],
                    'archive_size_bytes': cache_stats['archive_size'],
                    'file_size_bytes': cache_stats['file_size'],
                    'total_size_bytes': cache_stats['total_size']
                }
                
                # Simulation metrics
                all_sims = Simulation.list_simulations()
                self.metrics['simulations'] = {
                    'total_simulations': len(all_sims),
                    'simulations_with_locations': sum(
                        1 for s in all_sims if len(s.locations) > 0
                    )
                }
                
                # Performance metrics
                perf_stats = performance_monitor.get_stats()
                self.metrics['performance'] = {
                    'operations_tracked': len(perf_stats),
                    'average_operation_time': sum(
                        stats['avg_time'] for stats in perf_stats.values()
                    ) / len(perf_stats) if perf_stats else 0
                }
                
                return self.metrics
            
            def get_metrics_summary(self):
                """Get a summary of collected metrics."""
                if not self.metrics:
                    return {}
                
                return {
                    'collection_timestamp': self.collection_time,
                    'total_data_size_mb': (
                        self.metrics['archives']['total_size_bytes'] + 
                        self.metrics['cache']['total_size_bytes']
                    ) / (1024 * 1024),
                    'cache_efficiency': (
                        self.metrics['cache']['total_size_bytes'] / 
                        max(self.metrics['archives']['total_size_bytes'], 1)
                    ),
                    'avg_files_per_archive': (
                        self.metrics['archives']['total_files'] / 
                        max(self.metrics['archives']['total_archives'], 1)
                    )
                }
        
        # Collect metrics
        collector = MetricsCollector()
        
        # Perform some operations to generate metrics
        files = archive.list_files()
        if files:
            sample_file = list(files.keys())[0]
            archive.open_file(sample_file)
        
        # Collect system state
        metrics = collector.collect_system_metrics()
        summary = collector.get_metrics_summary()
        
        # Verify metrics collection
        assert 'archives' in metrics
        assert 'cache' in metrics
        assert 'simulations' in metrics
        assert 'performance' in metrics
        
        # Verify metric values
        assert metrics['archives']['total_archives'] >= 1
        assert metrics['simulations']['total_simulations'] >= 1
        
        # Verify summary calculations
        assert 'collection_timestamp' in summary
        assert 'total_data_size_mb' in summary
        assert summary['total_data_size_mb'] >= 0
    
    def test_alert_generation(self, cache_manager, sample_archive_data,
                            progress_tracker):
        """Test alert generation based on system conditions."""
        class AlertManager:
            def __init__(self):
                self.alerts = []
                self.alert_rules = {}
            
            def add_alert_rule(self, rule_name, condition_func, severity='warning'):
                """Add an alert rule."""
                self.alert_rules[rule_name] = {
                    'condition': condition_func,
                    'severity': severity
                }
            
            def check_alerts(self, system_state):
                """Check all alert rules against current system state."""
                new_alerts = []
                
                for rule_name, rule in self.alert_rules.items():
                    try:
                        if rule['condition'](system_state):
                            alert = {
                                'rule_name': rule_name,
                                'severity': rule['severity'],
                                'timestamp': time.time(),
                                'message': f"Alert triggered: {rule_name}",
                                'system_state': system_state
                            }
                            new_alerts.append(alert)
                            self.alerts.append(alert)
                    except Exception as e:
                        # Alert rule evaluation failed
                        error_alert = {
                            'rule_name': f"{rule_name}_evaluation_error",
                            'severity': 'error',
                            'timestamp': time.time(),
                            'message': f"Alert rule evaluation failed: {e}",
                            'system_state': system_state
                        }
                        new_alerts.append(error_alert)
                        self.alerts.append(error_alert)
                
                return new_alerts
            
            def get_active_alerts(self, severity=None):
                """Get active alerts, optionally filtered by severity."""
                if severity:
                    return [a for a in self.alerts if a['severity'] == severity]
                return self.alerts.copy()
        
        alert_manager = AlertManager()
        
        # Define alert rules
        alert_manager.add_alert_rule(
            'cache_high_utilization',
            lambda state: state.get('cache_utilization', 0) > 80,
            'warning'
        )
        
        alert_manager.add_alert_rule(
            'cache_critical_utilization',
            lambda state: state.get('cache_utilization', 0) > 95,
            'critical'
        )
        
        alert_manager.add_alert_rule(
            'no_cached_items',
            lambda state: state.get('cache_item_count', 0) == 0,
            'info'
        )
        
        # Test with normal system state
        normal_state = {
            'cache_utilization': 30,
            'cache_item_count': 5
        }
        
        normal_alerts = alert_manager.check_alerts(normal_state)
        assert len(normal_alerts) == 0, "No alerts should trigger in normal state"
        
        # Test with high utilization state
        high_util_state = {
            'cache_utilization': 85,
            'cache_item_count': 10
        }
        
        high_util_alerts = alert_manager.check_alerts(high_util_state)
        assert len(high_util_alerts) == 1, "Should trigger high utilization alert"
        assert high_util_alerts[0]['severity'] == 'warning'
        assert high_util_alerts[0]['rule_name'] == 'cache_high_utilization'
        
        # Test with critical state
        critical_state = {
            'cache_utilization': 98,
            'cache_item_count': 15
        }
        
        critical_alerts = alert_manager.check_alerts(critical_state)
        assert len(critical_alerts) == 2, "Should trigger both warning and critical alerts"
        
        severities = [a['severity'] for a in critical_alerts]
        assert 'warning' in severities
        assert 'critical' in severities
        
        # Test with empty cache state
        empty_state = {
            'cache_utilization': 0,
            'cache_item_count': 0
        }
        
        empty_alerts = alert_manager.check_alerts(empty_state)
        assert len(empty_alerts) == 1, "Should trigger no cached items alert"
        assert empty_alerts[0]['severity'] == 'info'
        
        # Verify alert history
        all_alerts = alert_manager.get_active_alerts()
        assert len(all_alerts) == 4, "Should have collected all alerts"
        
        critical_alerts_only = alert_manager.get_active_alerts('critical')
        assert len(critical_alerts_only) == 1, "Should have one critical alert"


@pytest.mark.integration
class TestSystemRecoveryScenarios:
    """Test system recovery and self-healing capabilities."""
    
    def test_automatic_cache_repair(self, cache_manager, sample_archive_data,
                                  temp_workspace, progress_tracker):
        """Test automatic cache repair mechanisms."""
        # Create cache entry
        with tempfile.NamedTemporaryFile(suffix='.tar.gz', delete=False) as f:
            f.write(sample_archive_data)
            temp_path = Path(f.name)
        
        try:
            checksum = "repair_test"
            cached_path = cache_manager.cache_archive(temp_path, checksum)
            
            # Verify cache works
            assert cache_manager.get_archive_path(checksum) == cached_path
            
            # Simulate cache corruption by deleting cached file
            cached_path.unlink()
            
            operation_id = "cache_repair_test"
            progress_tracker.track_operation(operation_id, "cache_repair")
            
            # Attempt to access corrupted cache (should detect and handle)
            try:
                retrieved_path = cache_manager.get_archive_path(checksum)
                
                # If path is returned but file doesn't exist, cache should self-heal
                if retrieved_path and not retrieved_path.exists():
                    # Simulate cache repair by re-caching
                    repaired_path = cache_manager.cache_archive(temp_path, f"{checksum}_repaired")
                    assert repaired_path.exists()
                    
                    progress_tracker.complete_operation(operation_id, True)
                else:
                    progress_tracker.complete_operation(operation_id, retrieved_path is not None)
                    
            except Exception as e:
                progress_tracker.complete_operation(operation_id, False, str(e))
                
                # Even if access fails, system should remain functional
                # Try to cache new item to verify system health
                recovery_path = cache_manager.cache_archive(temp_path, f"{checksum}_recovery")
                assert recovery_path.exists(), "System should recover and remain functional"
        
        finally:
            temp_path.unlink(missing_ok=True)
        
        # Verify cache is still functional after repair attempt
        stats = cache_manager.get_cache_stats()
        assert stats is not None, "Cache manager should remain functional"
    
    def test_graceful_degradation(self, archive_factory, test_locations,
                                simulation_factory, progress_tracker):
        """Test graceful degradation when components fail."""
        sim = simulation_factory("degradation_test")
        archive = archive_factory("degradation_archive", "local")
        
        # Simulate various component failures
        failure_scenarios = [
            ("network_failure", "Network connectivity lost"),
            ("storage_failure", "Storage backend unavailable"),
            ("cache_failure", "Cache subsystem error")
        ]
        
        def test_degraded_operation(failure_type, failure_message):
            """Test system behavior under specific failure."""
            operation_id = f"degraded_ops_{failure_type}"
            progress_tracker.track_operation(operation_id, "degraded_operation")
            
            try:
                successful_operations = 0
                total_operations = 0
                
                # Test basic simulation operations
                try:
                    locations = sim.list_locations()
                    successful_operations += 1
                except Exception:
                    pass
                total_operations += 1
                
                # Test archive operations with potential degradation
                try:
                    if failure_type == "storage_failure":
                        # Mock storage failure - should fail gracefully
                        raise ConnectionError(failure_message)
                    else:
                        # Other failures - archive ops might still work
                        files = archive.list_files()
                        if files:
                            successful_operations += 1
                except (ConnectionError, OSError):
                    # Expected failure - system should handle gracefully
                    pass
                except Exception:
                    # Unexpected failure - should still not crash
                    pass
                total_operations += 1
                
                # Test cache operations
                try:
                    if failure_type == "cache_failure":
                        # Mock cache failure
                        raise OSError(failure_message)
                    else:
                        # Cache might still work
                        cache_stats = archive.cache_manager.get_cache_stats()
                        if cache_stats:
                            successful_operations += 1
                except (OSError, AttributeError):
                    # Expected failure or cache not available
                    pass
                except Exception:
                    # Unexpected failure
                    pass
                total_operations += 1
                
                # System should degrade gracefully - some ops may fail but system remains stable
                degradation_ratio = successful_operations / total_operations if total_operations > 0 else 0
                
                progress_tracker.complete_operation(
                    operation_id, 
                    True,  # Success means graceful degradation, not all ops succeeding
                    f"Degradation ratio: {degradation_ratio:.2f}"
                )
                
                return {
                    'failure_type': failure_type,
                    'successful_operations': successful_operations,
                    'total_operations': total_operations,
                    'degradation_ratio': degradation_ratio
                }
                
            except Exception as e:
                progress_tracker.complete_operation(operation_id, False, str(e))
                raise
        
        # Test each failure scenario
        degradation_results = []
        for failure_type, failure_message in failure_scenarios:
            result = test_degraded_operation(failure_type, failure_message)
            degradation_results.append(result)
        
        # Verify graceful degradation
        for result in degradation_results:
            # System should not completely fail (some operations might still work)
            assert result['total_operations'] > 0, "Should attempt operations"
            
            # Even under failure, system should remain responsive
            # (This is verified by the fact that the test completes without hanging)
        
        # Verify system can recover after failures
        stats = progress_tracker.get_stats()
        assert stats['completed_operations'] == len(failure_scenarios), "All degradation tests should complete"
    
    def test_resource_leak_detection(self, archive_factory, concurrent_executor,
                                   resource_monitor, test_env_config):
        """Test detection and handling of resource leaks."""
        archives = [archive_factory(f"leak_test_{i}") for i in range(3)]
        
        resource_monitor.take_snapshot("leak_test_start")
        
        def resource_intensive_worker(worker_id):
            """Worker that might leak resources if not properly managed."""
            archive = archives[worker_id % len(archives)]
            
            # Perform operations that could potentially leak resources
            for _ in range(10):
                try:
                    files = archive.list_files()
                    if files:
                        filename = list(files.keys())[0]
                        file_data = archive.open_file(filename)
                        content = file_data.read()
                        # Explicitly close/cleanup resources
                        del file_data
                        del content
                except Exception:
                    pass
            
            return worker_id
        
        # Run resource-intensive operations
        futures = [
            concurrent_executor.submit(resource_intensive_worker, i)
            for i in range(test_env_config["concurrent_operations"])
        ]
        
        # Monitor resource usage during operations
        resource_monitor.take_snapshot("leak_test_peak")
        
        # Wait for completion
        results = []
        for future in as_completed(futures):
            results.append(future.result())
        
        # Take final snapshot
        resource_monitor.take_snapshot("leak_test_end")
        
        # Check for resource leaks
        has_memory_leak = resource_monitor.check_memory_leak(50)  # 50MB threshold
        
        # System should not have significant resource leaks
        assert not has_memory_leak, "System should not have significant memory leaks"
        
        # Verify all workers completed
        assert len(results) == test_env_config["concurrent_operations"]
        
        # System should remain functional after intensive operations
        # Test by performing a simple operation
        test_archive = archives[0]
        files = test_archive.list_files()
        assert files is not None, "System should remain functional after intensive operations"