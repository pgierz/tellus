"""
Performance Optimization Recommendations for PathSandboxedFileSystem.

This module analyzes performance test results and provides specific,
actionable optimization recommendations for improving PathSandboxedFileSystem
performance in HPC climate science environments.

Optimization Categories:
- Path resolution and validation optimizations
- Memory management and allocation improvements  
- CPU instruction efficiency enhancements
- Parallel access pattern optimizations
- Network filesystem adaptations
- Cache-aware algorithms
- NUMA-aware memory allocation strategies

Recommendation Types:
- Immediate fixes (low-hanging fruit)
- Short-term improvements (weeks)
- Long-term architectural changes (months)
- System-level optimizations (environment-specific)
"""

import json
import math
import os
import re
import statistics
import tempfile
import time
import warnings
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Dict, List, NamedTuple, Optional, Set, Tuple
from unittest.mock import Mock, patch

import fsspec
import psutil
import pytest
# Import components from other performance test modules
from test_hpc_climate_performance import (ClimateDataGenerator,
                                          HPC_Performance_Profiler,
                                          PerformanceProfile,
                                          generate_hpc_performance_report)
from test_memory_cpu_profiling import (AdvancedCPUProfiler,
                                       AdvancedMemoryProfiler, CombinedProfile,
                                       generate_profiling_report)
from test_parallel_stress_performance import (ParallelStressTester,
                                              StressTestResult,
                                              generate_stress_test_report)
from test_performance_regression_framework import (BaselineManager,
                                                   RegressionTestFramework,
                                                   RegressionTestResult,
                                                   generate_regression_report)

from tellus.location.sandboxed_filesystem import (PathSandboxedFileSystem,
                                                  PathValidationError)


class OptimizationOpportunity(NamedTuple):
    """Represents a specific optimization opportunity."""
    category: str
    priority: str  # "critical", "high", "medium", "low"
    description: str
    impact_estimate: str  # Expected performance improvement
    implementation_effort: str  # "low", "medium", "high"
    code_changes_required: List[str]
    test_validation_strategy: str
    performance_metric_targets: Dict[str, float]


@dataclass
class PerformanceAnalysisResult:
    """Results from comprehensive performance analysis."""
    bottlenecks_identified: List[str]
    performance_patterns: Dict[str, Any]
    optimization_opportunities: List[OptimizationOpportunity]
    recommended_immediate_actions: List[str]
    long_term_architectural_suggestions: List[str]
    system_level_recommendations: List[str]
    performance_budget_analysis: Dict[str, float]
    risk_assessment: Dict[str, str]


class PerformanceAnalyzer:
    """Comprehensive performance analyzer and optimization recommender."""
    
    def __init__(self):
        self.analysis_results = []
        self.pattern_cache = {}
    
    def analyze_path_resolution_performance(self, filesystem, test_paths: List[str], 
                                          iterations: int = 100) -> Dict[str, Any]:
        """Analyze path resolution performance patterns."""
        profiler = HPC_Performance_Profiler()
        
        # Test different path patterns
        path_patterns = self._categorize_paths(test_paths)
        pattern_performance = {}
        
        for pattern_type, paths in path_patterns.items():
            if not paths:
                continue
            
            sample_paths = paths[:min(20, len(paths))]  # Sample for performance
            
            profiler.start_profiling(f"path_resolution_{pattern_type}", 
                                   filesystem_type="sandboxed")
            
            for _ in range(iterations // len(sample_paths)):
                for path in sample_paths:
                    try:
                        resolved = filesystem._resolve_path(path)
                        filesystem._is_within_base_path(resolved)
                    except Exception:
                        pass
            
            profile = profiler.end_profiling()
            
            pattern_performance[pattern_type] = {
                'execution_time': profile.execution_time,
                'operations_per_second': profile.operations_per_second,
                'path_count': len(sample_paths),
                'avg_time_per_path': profile.execution_time / (iterations if iterations > 0 else 1)
            }
        
        return {
            'pattern_performance': pattern_performance,
            'total_patterns': len(path_patterns),
            'bottleneck_patterns': self._identify_slow_patterns(pattern_performance)
        }
    
    def _categorize_paths(self, paths: List[str]) -> Dict[str, List[str]]:
        """Categorize paths by common patterns."""
        categories = {
            'simple': [],          # No subdirectories
            'shallow': [],         # 1-2 levels deep
            'deep': [],           # 3+ levels deep  
            'relative': [],       # Contains ".." or "."
            'long_names': [],     # Long file/directory names
            'special_chars': [],  # Contains special characters
            'numbers': [],        # Contains numeric patterns
            'patterns': []        # Contains glob-like patterns
        }
        
        for path in paths:
            path_parts = Path(path).parts
            
            # Depth categorization
            if len(path_parts) == 1:
                categories['simple'].append(path)
            elif len(path_parts) <= 2:
                categories['shallow'].append(path)
            else:
                categories['deep'].append(path)
            
            # Pattern analysis
            if '..' in path or '.' in path:
                categories['relative'].append(path)
            
            if any(len(part) > 20 for part in path_parts):
                categories['long_names'].append(path)
            
            if re.search(r'[^\w/.-]', path):
                categories['special_chars'].append(path)
            
            if re.search(r'\d+', path):
                categories['numbers'].append(path)
            
            if any(char in path for char in ['*', '?', '[', ']']):
                categories['patterns'].append(path)
        
        return categories
    
    def _identify_slow_patterns(self, pattern_performance: Dict[str, Dict]) -> List[str]:
        """Identify path patterns that are performance bottlenecks."""
        if not pattern_performance:
            return []
        
        # Calculate average performance across all patterns
        all_times = [perf['avg_time_per_path'] for perf in pattern_performance.values()]
        if not all_times:
            return []
        
        avg_time = statistics.mean(all_times)
        std_time = statistics.stdev(all_times) if len(all_times) > 1 else 0
        
        # Identify patterns that are significantly slower than average
        slow_threshold = avg_time + std_time
        slow_patterns = [
            pattern for pattern, perf in pattern_performance.items()
            if perf['avg_time_per_path'] > slow_threshold
        ]
        
        return slow_patterns
    
    def analyze_memory_allocation_patterns(self, profiles: List[CombinedProfile]) -> Dict[str, Any]:
        """Analyze memory allocation patterns for optimization opportunities."""
        if not profiles:
            return {'status': 'no_data'}
        
        # Extract memory metrics
        allocation_counts = [p.memory_profile.allocation_count for p in profiles]
        memory_deltas = [p.memory_profile.net_memory_growth_mb for p in profiles]
        fragmentation_indices = [p.memory_profile.fragmentation_index for p in profiles]
        gc_collections = [p.memory_profile.gc_collections for p in profiles]
        
        # Identify patterns
        analysis = {
            'allocation_stats': {
                'mean_allocations': statistics.mean(allocation_counts) if allocation_counts else 0,
                'max_allocations': max(allocation_counts) if allocation_counts else 0,
                'allocation_variance': statistics.variance(allocation_counts) if len(allocation_counts) > 1 else 0
            },
            'memory_growth_stats': {
                'mean_growth_mb': statistics.mean(memory_deltas) if memory_deltas else 0,
                'max_growth_mb': max(memory_deltas) if memory_deltas else 0,
                'negative_growth_count': sum(1 for delta in memory_deltas if delta < 0)
            },
            'fragmentation_stats': {
                'mean_fragmentation': statistics.mean(fragmentation_indices) if fragmentation_indices else 0,
                'high_fragmentation_count': sum(1 for frag in fragmentation_indices if frag > 0.5)
            },
            'gc_pressure_stats': {
                'mean_gc_collections': statistics.mean(gc_collections) if gc_collections else 0,
                'high_gc_operations': sum(1 for gc in gc_collections if gc > 10)
            }
        }
        
        # Identify optimization opportunities
        opportunities = []
        
        if analysis['allocation_stats']['mean_allocations'] > 1000:
            opportunities.append('high_allocation_count')
        
        if analysis['memory_growth_stats']['mean_growth_mb'] > 50:
            opportunities.append('excessive_memory_growth')
        
        if analysis['fragmentation_stats']['mean_fragmentation'] > 0.3:
            opportunities.append('memory_fragmentation')
        
        if analysis['gc_pressure_stats']['mean_gc_collections'] > 5:
            opportunities.append('gc_pressure')
        
        analysis['optimization_opportunities'] = opportunities
        return analysis
    
    def analyze_concurrency_bottlenecks(self, stress_results: List[StressTestResult]) -> Dict[str, Any]:
        """Analyze concurrency performance for bottleneck identification."""
        if not stress_results:
            return {'status': 'no_data'}
        
        # Group results by concurrency level
        concurrency_performance = defaultdict(list)
        
        for result in stress_results:
            concurrency_performance[result.concurrency_level].append({
                'throughput': result.throughput_ops_per_sec,
                'error_rate': result.error_rate_percent,
                'test_name': result.test_name
            })
        
        # Analyze scaling efficiency
        scaling_analysis = {}
        
        if len(concurrency_performance) > 1:
            sorted_levels = sorted(concurrency_performance.keys())
            base_level = sorted_levels[0]
            base_throughput = statistics.mean([r['throughput'] for r in concurrency_performance[base_level]])
            
            for level in sorted_levels[1:]:
                level_throughput = statistics.mean([r['throughput'] for r in concurrency_performance[level]])
                
                # Calculate scaling efficiency
                expected_throughput = base_throughput * level
                actual_efficiency = level_throughput / expected_throughput if expected_throughput > 0 else 0
                
                scaling_analysis[level] = {
                    'throughput': level_throughput,
                    'expected_throughput': expected_throughput,
                    'efficiency': actual_efficiency,
                    'scaling_factor': level_throughput / base_throughput if base_throughput > 0 else 0
                }
        
        # Identify bottlenecks
        bottlenecks = []
        
        for level, analysis in scaling_analysis.items():
            if analysis['efficiency'] < 0.5:  # Less than 50% efficiency
                bottlenecks.append(f'poor_scaling_at_{level}_threads')
            
            if analysis['scaling_factor'] < level * 0.3:  # Very poor scaling
                bottlenecks.append(f'severe_contention_at_{level}_threads')
        
        # Error rate analysis
        error_patterns = []
        for level, results in concurrency_performance.items():
            avg_error_rate = statistics.mean([r['error_rate'] for r in results])
            if avg_error_rate > 10:  # More than 10% errors
                error_patterns.append(f'high_error_rate_at_{level}_threads')
        
        return {
            'concurrency_performance': dict(concurrency_performance),
            'scaling_analysis': scaling_analysis,
            'bottlenecks': bottlenecks,
            'error_patterns': error_patterns,
            'max_efficient_concurrency': self._find_max_efficient_concurrency(scaling_analysis)
        }
    
    def _find_max_efficient_concurrency(self, scaling_analysis: Dict[int, Dict]) -> int:
        """Find the maximum concurrency level with good efficiency."""
        efficient_levels = [
            level for level, analysis in scaling_analysis.items()
            if analysis['efficiency'] >= 0.7  # 70% efficiency threshold
        ]
        
        return max(efficient_levels) if efficient_levels else 1
    
    def generate_optimization_recommendations(self, 
                                            path_analysis: Dict[str, Any],
                                            memory_analysis: Dict[str, Any],
                                            concurrency_analysis: Dict[str, Any],
                                            regression_results: List[RegressionTestResult]) -> PerformanceAnalysisResult:
        """Generate comprehensive optimization recommendations."""
        
        # Identify bottlenecks
        bottlenecks = []
        
        # Path resolution bottlenecks
        if 'bottleneck_patterns' in path_analysis:
            bottlenecks.extend([f"Path pattern '{pattern}' is slow" for pattern in path_analysis['bottleneck_patterns']])
        
        # Memory bottlenecks
        if 'optimization_opportunities' in memory_analysis:
            bottlenecks.extend(memory_analysis['optimization_opportunities'])
        
        # Concurrency bottlenecks
        if 'bottlenecks' in concurrency_analysis:
            bottlenecks.extend(concurrency_analysis['bottlenecks'])
        
        # Generate optimization opportunities
        opportunities = []
        
        # Path resolution optimizations
        opportunities.extend(self._generate_path_optimization_opportunities(path_analysis))
        
        # Memory optimization opportunities  
        opportunities.extend(self._generate_memory_optimization_opportunities(memory_analysis))
        
        # Concurrency optimization opportunities
        opportunities.extend(self._generate_concurrency_optimization_opportunities(concurrency_analysis))
        
        # Generate immediate actions
        immediate_actions = self._generate_immediate_actions(opportunities)
        
        # Long-term architectural suggestions
        architectural_suggestions = self._generate_architectural_suggestions(opportunities, regression_results)
        
        # System-level recommendations
        system_recommendations = self._generate_system_recommendations()
        
        # Performance budget analysis
        performance_budget = self._analyze_performance_budget(opportunities)
        
        # Risk assessment
        risk_assessment = self._assess_optimization_risks(opportunities)
        
        return PerformanceAnalysisResult(
            bottlenecks_identified=bottlenecks,
            performance_patterns={
                'path_patterns': path_analysis,
                'memory_patterns': memory_analysis, 
                'concurrency_patterns': concurrency_analysis
            },
            optimization_opportunities=opportunities,
            recommended_immediate_actions=immediate_actions,
            long_term_architectural_suggestions=architectural_suggestions,
            system_level_recommendations=system_recommendations,
            performance_budget_analysis=performance_budget,
            risk_assessment=risk_assessment
        )
    
    def _generate_path_optimization_opportunities(self, path_analysis: Dict[str, Any]) -> List[OptimizationOpportunity]:
        """Generate path resolution optimization opportunities."""
        opportunities = []
        
        if 'bottleneck_patterns' in path_analysis:
            slow_patterns = path_analysis['bottleneck_patterns']
            
            if 'deep' in slow_patterns:
                opportunities.append(OptimizationOpportunity(
                    category="path_resolution",
                    priority="high", 
                    description="Deep directory paths cause performance degradation in path resolution",
                    impact_estimate="15-25% improvement for deep path operations",
                    implementation_effort="medium",
                    code_changes_required=[
                        "Implement path component caching in _resolve_path()",
                        "Add fast path for already-resolved paths",
                        "Optimize Path object creation and manipulation"
                    ],
                    test_validation_strategy="Compare before/after performance on deep directory structures",
                    performance_metric_targets={"path_resolution_time_ms": 2.0}
                ))
            
            if 'relative' in slow_patterns:
                opportunities.append(OptimizationOpportunity(
                    category="path_resolution",
                    priority="medium",
                    description="Relative paths with '..' and '.' components are inefficient",
                    impact_estimate="10-15% improvement for relative path operations",
                    implementation_effort="low",
                    code_changes_required=[
                        "Pre-compile regex patterns for common path components",
                        "Implement fast path for paths without relative components",
                        "Cache normalized path results"
                    ],
                    test_validation_strategy="Measure performance on mixed relative/absolute path workloads",
                    performance_metric_targets={"relative_path_resolution_ms": 1.5}
                ))
            
            if 'special_chars' in slow_patterns:
                opportunities.append(OptimizationOpportunity(
                    category="path_validation",
                    priority="medium",
                    description="Special character validation adds overhead",
                    impact_estimate="5-10% improvement for paths with special characters",
                    implementation_effort="low", 
                    code_changes_required=[
                        "Optimize character validation loops",
                        "Use set-based character checking instead of regex where possible",
                        "Implement early termination for common safe paths"
                    ],
                    test_validation_strategy="Test with paths containing various special character patterns",
                    performance_metric_targets={"special_char_validation_us": 100}
                ))
        
        return opportunities
    
    def _generate_memory_optimization_opportunities(self, memory_analysis: Dict[str, Any]) -> List[OptimizationOpportunity]:
        """Generate memory optimization opportunities."""
        opportunities = []
        
        if 'optimization_opportunities' not in memory_analysis:
            return opportunities
        
        memory_opportunities = memory_analysis['optimization_opportunities']
        
        if 'high_allocation_count' in memory_opportunities:
            opportunities.append(OptimizationOpportunity(
                category="memory_management",
                priority="high",
                description="High allocation count indicates excessive object creation",
                impact_estimate="20-30% reduction in memory allocations",
                implementation_effort="medium",
                code_changes_required=[
                    "Implement object pooling for Path objects",
                    "Add string interning for common path components",
                    "Use flyweight pattern for repeated path operations",
                    "Optimize string concatenation in path building"
                ],
                test_validation_strategy="Monitor allocation count and memory usage in sustained operations",
                performance_metric_targets={"allocations_per_operation": 10}
            ))
        
        if 'memory_fragmentation' in memory_opportunities:
            opportunities.append(OptimizationOpportunity(
                category="memory_management", 
                priority="medium",
                description="Memory fragmentation reduces cache efficiency",
                impact_estimate="10-15% improvement in memory locality",
                implementation_effort="medium",
                code_changes_required=[
                    "Implement custom memory allocator for path objects",
                    "Use memory pools for frequently allocated objects", 
                    "Reduce object lifetime variance",
                    "Implement explicit memory compaction strategies"
                ],
                test_validation_strategy="Measure memory fragmentation over long-running operations",
                performance_metric_targets={"fragmentation_index": 0.2}
            ))
        
        if 'gc_pressure' in memory_opportunities:
            opportunities.append(OptimizationOpportunity(
                category="garbage_collection",
                priority="medium", 
                description="High GC pressure indicates short-lived object creation",
                impact_estimate="15-20% reduction in GC overhead",
                implementation_effort="low",
                code_changes_required=[
                    "Reuse objects where possible instead of creating new ones",
                    "Use weak references for cache entries",
                    "Implement manual memory management for critical paths",
                    "Tune GC parameters for workload characteristics"
                ],
                test_validation_strategy="Monitor GC frequency and duration during typical operations",
                performance_metric_targets={"gc_collections_per_100_ops": 1}
            ))
        
        return opportunities
    
    def _generate_concurrency_optimization_opportunities(self, concurrency_analysis: Dict[str, Any]) -> List[OptimizationOpportunity]:
        """Generate concurrency optimization opportunities."""
        opportunities = []
        
        if 'bottlenecks' not in concurrency_analysis:
            return opportunities
        
        bottlenecks = concurrency_analysis['bottlenecks']
        
        # Check for scaling issues
        scaling_issues = [b for b in bottlenecks if 'scaling' in b or 'contention' in b]
        if scaling_issues:
            opportunities.append(OptimizationOpportunity(
                category="concurrency",
                priority="high",
                description="Poor scaling indicates contention or synchronization overhead", 
                impact_estimate="30-50% improvement in concurrent throughput",
                implementation_effort="high",
                code_changes_required=[
                    "Implement lock-free path resolution caching",
                    "Use thread-local storage for frequently accessed data",
                    "Implement fine-grained locking instead of coarse locks",
                    "Add async/await support for I/O operations",
                    "Use lock-free data structures where appropriate"
                ],
                test_validation_strategy="Measure scaling efficiency up to 16+ concurrent threads",
                performance_metric_targets={"scaling_efficiency_8_threads": 0.7}
            ))
        
        # Check for high error rates under concurrency
        error_issues = [b for b in bottlenecks if 'error_rate' in b]
        if error_issues:
            opportunities.append(OptimizationOpportunity(
                category="error_handling",
                priority="medium",
                description="High error rates under concurrency indicate race conditions",
                impact_estimate="10-15% reduction in operation failures",
                implementation_effort="medium",
                code_changes_required=[
                    "Implement proper synchronization around shared state",
                    "Add retry mechanisms for transient failures",
                    "Improve error handling and recovery",
                    "Add circuit breaker pattern for failing operations"
                ],
                test_validation_strategy="Measure error rates under various concurrency levels",
                performance_metric_targets={"error_rate_at_8_threads": 5.0}
            ))
        
        return opportunities
    
    def _generate_immediate_actions(self, opportunities: List[OptimizationOpportunity]) -> List[str]:
        """Generate immediate actions based on optimization opportunities."""
        immediate_actions = []
        
        # High priority, low effort opportunities
        high_priority_low_effort = [
            opp for opp in opportunities 
            if opp.priority in ['critical', 'high'] and opp.implementation_effort == 'low'
        ]
        
        for opp in high_priority_low_effort:
            immediate_actions.append(f"IMMEDIATE: {opp.description} - {opp.impact_estimate}")
        
        # Critical issues regardless of effort
        critical_opportunities = [opp for opp in opportunities if opp.priority == 'critical']
        for opp in critical_opportunities:
            if f"IMMEDIATE: {opp.description}" not in immediate_actions:
                immediate_actions.append(f"CRITICAL: {opp.description} - requires immediate attention")
        
        # Add general immediate actions
        immediate_actions.extend([
            "Profile production workloads to identify real-world bottlenecks",
            "Implement performance monitoring and alerting",
            "Set up automated performance regression testing",
            "Document performance characteristics and known limitations"
        ])
        
        return immediate_actions
    
    def _generate_architectural_suggestions(self, opportunities: List[OptimizationOpportunity],
                                          regression_results: List[RegressionTestResult]) -> List[str]:
        """Generate long-term architectural suggestions."""
        suggestions = []
        
        # Analyze categories of opportunities
        categories = Counter(opp.category for opp in opportunities)
        
        if categories.get('path_resolution', 0) >= 2:
            suggestions.append(
                "Consider redesigning path resolution architecture with caching layer and "
                "pre-compiled validation rules"
            )
        
        if categories.get('memory_management', 0) >= 2:
            suggestions.append(
                "Implement custom memory management system optimized for filesystem operations "
                "with object pooling and arena allocation"
            )
        
        if categories.get('concurrency', 0) >= 1:
            suggestions.append(
                "Redesign for async/await patterns to improve I/O concurrency and reduce "
                "thread contention"
            )
        
        # Analysis based on regression patterns
        if regression_results:
            critical_regressions = [r for r in regression_results if r.regression_severity == 'critical']
            if critical_regressions:
                suggestions.append(
                    "Implement comprehensive performance monitoring and automatic rollback "
                    "mechanisms for critical performance regressions"
                )
        
        # Add general architectural suggestions
        suggestions.extend([
            "Consider plugin architecture for different filesystem backends with optimized implementations",
            "Implement performance-oriented configuration profiles for different use cases",
            "Design modular validation system that can be customized based on security requirements",
            "Consider implementing filesystem operations as microservices for better scalability"
        ])
        
        return suggestions
    
    def _generate_system_recommendations(self) -> List[str]:
        """Generate system-level performance recommendations."""
        return [
            "NUMA Configuration: Pin filesystem processes to specific NUMA nodes for better memory locality",
            "CPU Affinity: Set CPU affinity to reduce context switching overhead",
            "Memory Management: Increase system memory for filesystem caching, tune vm.swappiness",
            "I/O Scheduler: Use appropriate I/O scheduler (deadline/mq-deadline) for HPC workloads",
            "Network Tuning: For network filesystems, tune TCP window sizes and buffer sizes",
            "Filesystem Mount Options: Use appropriate mount options (noatime, etc.) for performance",
            "Kernel Parameters: Tune kernel parameters for high-concurrency filesystem access",
            "Monitoring: Deploy system-level monitoring (iotop, sar, perf) for ongoing optimization"
        ]
    
    def _analyze_performance_budget(self, opportunities: List[OptimizationOpportunity]) -> Dict[str, float]:
        """Analyze performance budget and expected improvements."""
        budget = {
            'total_expected_improvement_percent': 0.0,
            'high_confidence_improvement_percent': 0.0,
            'implementation_effort_score': 0.0,
            'risk_adjusted_improvement_percent': 0.0
        }
        
        effort_weights = {'low': 1.0, 'medium': 2.0, 'high': 3.0}
        priority_weights = {'low': 0.5, 'medium': 1.0, 'high': 1.5, 'critical': 2.0}
        
        for opp in opportunities:
            # Extract numeric improvement estimate
            impact_text = opp.impact_estimate.lower()
            improvement_match = re.search(r'(\d+)-(\d+)%', impact_text)
            if improvement_match:
                min_improvement = float(improvement_match.group(1))
                max_improvement = float(improvement_match.group(2))
                avg_improvement = (min_improvement + max_improvement) / 2
            else:
                improvement_match = re.search(r'(\d+)%', impact_text)
                if improvement_match:
                    avg_improvement = float(improvement_match.group(1))
                else:
                    avg_improvement = 5.0  # Default assumption
            
            # Weight by priority and effort
            priority_weight = priority_weights.get(opp.priority, 1.0)
            effort_weight = effort_weights.get(opp.implementation_effort, 2.0)
            
            # Add to budget calculations
            budget['total_expected_improvement_percent'] += avg_improvement * priority_weight
            
            # High confidence improvements (low effort, high priority)
            if opp.implementation_effort == 'low' and opp.priority in ['high', 'critical']:
                budget['high_confidence_improvement_percent'] += avg_improvement
            
            # Implementation effort score
            budget['implementation_effort_score'] += effort_weight
            
            # Risk-adjusted (conservative estimate)
            risk_factor = 0.7 if opp.implementation_effort == 'high' else 0.8 if opp.implementation_effort == 'medium' else 0.9
            budget['risk_adjusted_improvement_percent'] += avg_improvement * priority_weight * risk_factor
        
        return budget
    
    def _assess_optimization_risks(self, opportunities: List[OptimizationOpportunity]) -> Dict[str, str]:
        """Assess risks associated with proposed optimizations."""
        risks = {
            'implementation_complexity': 'low',
            'regression_risk': 'low', 
            'maintenance_burden': 'low',
            'compatibility_risk': 'low'
        }
        
        high_effort_count = sum(1 for opp in opportunities if opp.implementation_effort == 'high')
        concurrency_changes = sum(1 for opp in opportunities if 'concurrency' in opp.category)
        memory_changes = sum(1 for opp in opportunities if 'memory' in opp.category)
        
        if high_effort_count >= 3:
            risks['implementation_complexity'] = 'high'
        elif high_effort_count >= 1:
            risks['implementation_complexity'] = 'medium'
        
        if concurrency_changes >= 2:
            risks['regression_risk'] = 'high'
        elif concurrency_changes >= 1:
            risks['regression_risk'] = 'medium'
        
        if memory_changes >= 2 or concurrency_changes >= 2:
            risks['maintenance_burden'] = 'medium'
        
        # Check for changes that might affect compatibility
        breaking_patterns = ['async', 'redesign', 'architecture', 'microservices']
        breaking_changes = any(
            any(pattern in code_change.lower() for pattern in breaking_patterns)
            for opp in opportunities
            for code_change in opp.code_changes_required
        )
        
        if breaking_changes:
            risks['compatibility_risk'] = 'high'
        
        return risks


# Test Fixtures
@pytest.fixture
def performance_analyzer():
    """Performance analyzer instance."""
    return PerformanceAnalyzer()


@pytest.fixture
def comprehensive_test_data(tmp_path):
    """Comprehensive test data for optimization analysis."""
    generator = ClimateDataGenerator()
    
    # Create diverse test data
    cmip6_files = generator.create_cmip6_structure(tmp_path / "cmip6", size_constraint_mb=150)
    ensemble_files = generator.create_ensemble_structure(tmp_path / "ensemble", 
                                                       ensemble_size=15, size_constraint_mb=100)
    
    all_files = cmip6_files + ensemble_files
    return tmp_path, all_files


# Performance Optimization Tests
@pytest.mark.performance
@pytest.mark.benchmark
class TestPerformanceOptimizationAnalysis:
    """Test performance optimization analysis and recommendation generation."""
    
    def test_path_resolution_analysis(self, performance_analyzer, comprehensive_test_data):
        """Test path resolution performance analysis."""
        base_path, files = comprehensive_test_data
        
        direct_fs = fsspec.filesystem('file')
        sandboxed_fs = PathSandboxedFileSystem(direct_fs, str(base_path))
        
        test_paths = [str(f.relative_to(base_path)) for f in files[:30]]
        
        # Analyze path resolution performance
        path_analysis = performance_analyzer.analyze_path_resolution_performance(
            sandboxed_fs, test_paths, iterations=50
        )
        
        print(f"\nPath Resolution Analysis:")
        print(f"  Pattern types analyzed: {path_analysis['total_patterns']}")
        print(f"  Bottleneck patterns: {path_analysis['bottleneck_patterns']}")
        
        # Verify analysis structure
        assert 'pattern_performance' in path_analysis
        assert 'bottleneck_patterns' in path_analysis
        assert isinstance(path_analysis['bottleneck_patterns'], list)
        
        # Check performance data
        pattern_perf = path_analysis['pattern_performance']
        for pattern_type, perf_data in pattern_perf.items():
            assert 'execution_time' in perf_data
            assert 'operations_per_second' in perf_data
            assert 'avg_time_per_path' in perf_data
            
            # Performance should be reasonable
            assert perf_data['avg_time_per_path'] < 0.1, f"Path resolution too slow for {pattern_type}"
    
    def test_memory_analysis(self, performance_analyzer):
        """Test memory allocation pattern analysis."""
        # Create mock memory profiles
        mock_profiles = []
        
        for i in range(10):
            from test_memory_cpu_profiling import (CombinedProfile, CPUProfile,
                                                   MemoryProfile)
            
            memory_profile = MemoryProfile(
                operation=f"test_op_{i}",
                peak_memory_mb=50 + i * 5,
                current_memory_mb=45 + i * 4,
                allocated_memory_mb=60 + i * 6,
                deallocated_memory_mb=55 + i * 5,
                net_memory_growth_mb=5 + i,
                allocation_count=100 + i * 50,
                deallocation_count=90 + i * 45,
                fragmentation_index=0.1 + i * 0.05,
                gc_collections=i,
                gc_time_ms=i * 2,
                memory_efficiency=0.8 - i * 0.02,
                numa_locality_score=0.9 - i * 0.01
            )
            
            cpu_profile = CPUProfile(
                operation=f"test_op_{i}",
                cpu_time_seconds=0.1 + i * 0.01,
                wall_time_seconds=0.2 + i * 0.02,
                cpu_efficiency=0.5,
                user_time_seconds=0.08 + i * 0.008,
                system_time_seconds=0.02 + i * 0.002,
                context_switches=i * 2,
                page_faults=i,
                instructions_per_operation=1000 + i * 100,
                cache_misses=i * 10,
                branch_misses=i * 5,
                cpu_cycles=10000 + i * 1000,
                ipc_ratio=2.0 + i * 0.1
            )
            
            combined_profile = CombinedProfile(
                memory_profile=memory_profile,
                cpu_profile=cpu_profile,
                operation_count=10,
                data_processed_mb=1.0,
                operations_per_second=50.0,
                mb_per_second=5.0,
                memory_bandwidth_mbps=100.0,
                efficiency_score=0.7
            )
            
            mock_profiles.append(combined_profile)
        
        # Analyze memory patterns
        memory_analysis = performance_analyzer.analyze_memory_allocation_patterns(mock_profiles)
        
        print(f"\nMemory Analysis Results:")
        print(f"  Mean allocations: {memory_analysis['allocation_stats']['mean_allocations']:.0f}")
        print(f"  Mean memory growth: {memory_analysis['memory_growth_stats']['mean_growth_mb']:.1f}MB")
        print(f"  Mean fragmentation: {memory_analysis['fragmentation_stats']['mean_fragmentation']:.3f}")
        print(f"  Optimization opportunities: {memory_analysis['optimization_opportunities']}")
        
        # Verify analysis structure
        assert 'allocation_stats' in memory_analysis
        assert 'memory_growth_stats' in memory_analysis
        assert 'fragmentation_stats' in memory_analysis
        assert 'optimization_opportunities' in memory_analysis
        
        # Check for reasonable analysis results
        assert isinstance(memory_analysis['optimization_opportunities'], list)
    
    def test_concurrency_analysis(self, performance_analyzer):
        """Test concurrency bottleneck analysis."""
        # Create mock stress test results
        mock_results = []
        
        for concurrency in [1, 2, 4, 8]:
            # Simulate decreasing efficiency with higher concurrency
            base_throughput = 100.0
            efficiency = 1.0 / (1.0 + (concurrency - 1) * 0.3)  # Decreasing efficiency
            throughput = base_throughput * concurrency * efficiency
            
            result = StressTestResult(
                test_name=f"stress_test_{concurrency}",
                duration=10.0,
                operations_completed=int(throughput * 10),
                operations_failed=int(throughput * 10 * 0.05),  # 5% failure rate
                peak_memory_mb=50 + concurrency * 10,
                avg_cpu_percent=20 + concurrency * 15,
                concurrency_level=concurrency,
                throughput_ops_per_sec=throughput,
                error_rate_percent=5.0,
                additional_metrics={}
            )
            
            mock_results.append(result)
        
        # Analyze concurrency patterns
        concurrency_analysis = performance_analyzer.analyze_concurrency_bottlenecks(mock_results)
        
        print(f"\nConcurrency Analysis Results:")
        print(f"  Scaling analysis: {list(concurrency_analysis['scaling_analysis'].keys())}")
        print(f"  Bottlenecks: {concurrency_analysis['bottlenecks']}")
        print(f"  Max efficient concurrency: {concurrency_analysis['max_efficient_concurrency']}")
        
        # Verify analysis structure
        assert 'scaling_analysis' in concurrency_analysis
        assert 'bottlenecks' in concurrency_analysis
        assert 'max_efficient_concurrency' in concurrency_analysis
        
        # Check for scaling analysis
        scaling_analysis = concurrency_analysis['scaling_analysis']
        assert isinstance(scaling_analysis, dict)
        
        if scaling_analysis:
            for level, analysis in scaling_analysis.items():
                assert 'efficiency' in analysis
                assert 'scaling_factor' in analysis
                assert 0 <= analysis['efficiency'] <= 2.0  # Reasonable efficiency range
    
    def test_comprehensive_optimization_recommendations(self, performance_analyzer):
        """Test comprehensive optimization recommendation generation."""
        # Create mock analysis data
        path_analysis = {
            'pattern_performance': {
                'deep': {'avg_time_per_path': 0.05},
                'shallow': {'avg_time_per_path': 0.01},
                'relative': {'avg_time_per_path': 0.03}
            },
            'bottleneck_patterns': ['deep', 'relative']
        }
        
        memory_analysis = {
            'optimization_opportunities': ['high_allocation_count', 'memory_fragmentation']
        }
        
        concurrency_analysis = {
            'bottlenecks': ['poor_scaling_at_8_threads', 'high_error_rate_at_4_threads']
        }
        
        # Mock regression results
        regression_results = []
        
        # Generate recommendations
        analysis_result = performance_analyzer.generate_optimization_recommendations(
            path_analysis, memory_analysis, concurrency_analysis, regression_results
        )
        
        print(f"\nOptimization Recommendations Summary:")
        print(f"  Bottlenecks identified: {len(analysis_result.bottlenecks_identified)}")
        print(f"  Optimization opportunities: {len(analysis_result.optimization_opportunities)}")
        print(f"  Immediate actions: {len(analysis_result.recommended_immediate_actions)}")
        print(f"  Architectural suggestions: {len(analysis_result.long_term_architectural_suggestions)}")
        
        # Verify comprehensive analysis
        assert len(analysis_result.bottlenecks_identified) > 0
        assert len(analysis_result.optimization_opportunities) > 0
        assert len(analysis_result.recommended_immediate_actions) > 0
        assert len(analysis_result.long_term_architectural_suggestions) > 0
        assert len(analysis_result.system_level_recommendations) > 0
        
        # Check performance budget analysis
        budget = analysis_result.performance_budget_analysis
        assert 'total_expected_improvement_percent' in budget
        assert 'implementation_effort_score' in budget
        assert budget['total_expected_improvement_percent'] > 0
        
        # Check risk assessment
        risks = analysis_result.risk_assessment
        assert 'implementation_complexity' in risks
        assert 'regression_risk' in risks
        assert all(risk in ['low', 'medium', 'high'] for risk in risks.values())
        
        # Print detailed recommendations
        print(f"\nDetailed Optimization Opportunities:")
        for i, opp in enumerate(analysis_result.optimization_opportunities[:3]):  # Show first 3
            print(f"  {i+1}. {opp.category.upper()}: {opp.description}")
            print(f"     Priority: {opp.priority}, Effort: {opp.implementation_effort}")
            print(f"     Expected Impact: {opp.impact_estimate}")
    
    def test_optimization_report_generation(self, performance_analyzer):
        """Test optimization report generation."""
        # Create a sample analysis result
        opportunities = [
            OptimizationOpportunity(
                category="path_resolution",
                priority="high",
                description="Deep directory paths cause performance degradation",
                impact_estimate="15-25% improvement",
                implementation_effort="medium",
                code_changes_required=["Implement path caching", "Optimize Path operations"],
                test_validation_strategy="Compare before/after on deep paths",
                performance_metric_targets={"path_time_ms": 2.0}
            )
        ]
        
        analysis_result = PerformanceAnalysisResult(
            bottlenecks_identified=["Deep path resolution", "High memory allocations"],
            performance_patterns={},
            optimization_opportunities=opportunities,
            recommended_immediate_actions=["Implement path caching"],
            long_term_architectural_suggestions=["Redesign path resolution"],
            system_level_recommendations=["Tune NUMA settings"],
            performance_budget_analysis={"total_expected_improvement_percent": 20.0},
            risk_assessment={"implementation_complexity": "medium"}
        )
        
        # Generate report
        report = generate_optimization_report(analysis_result)
        
        print(f"\nOptimization Report Preview:")
        print(report[:500] + "...")  # Show first 500 characters
        
        # Verify report structure
        assert "Performance Optimization Report" in report
        assert "BOTTLENECKS IDENTIFIED" in report
        assert "OPTIMIZATION OPPORTUNITIES" in report
        assert "IMMEDIATE ACTIONS" in report
        assert "PERFORMANCE BUDGET" in report


# Utility Functions
def generate_optimization_report(analysis_result: PerformanceAnalysisResult) -> str:
    """Generate comprehensive optimization report."""
    report_lines = [
        "PathSandboxedFileSystem Performance Optimization Report",
        "=" * 65,
        f"Generated: {time.strftime('%Y-%m-%d %H:%M:%S')}",
        "",
        "EXECUTIVE SUMMARY:",
        f"  Bottlenecks identified: {len(analysis_result.bottlenecks_identified)}",
        f"  Optimization opportunities: {len(analysis_result.optimization_opportunities)}",
        f"  Expected total improvement: {analysis_result.performance_budget_analysis.get('total_expected_improvement_percent', 0):.1f}%",
        f"  Implementation complexity: {analysis_result.risk_assessment.get('implementation_complexity', 'unknown')}",
        "",
    ]
    
    # Performance Budget Summary
    budget = analysis_result.performance_budget_analysis
    report_lines.extend([
        "PERFORMANCE BUDGET ANALYSIS:",
        f"  Total expected improvement: {budget.get('total_expected_improvement_percent', 0):.1f}%",
        f"  High confidence improvements: {budget.get('high_confidence_improvement_percent', 0):.1f}%",
        f"  Risk-adjusted improvement: {budget.get('risk_adjusted_improvement_percent', 0):.1f}%",
        f"  Implementation effort score: {budget.get('implementation_effort_score', 0):.1f}",
        "",
    ])
    
    # Bottlenecks
    report_lines.extend([
        "BOTTLENECKS IDENTIFIED:",
    ])
    
    for bottleneck in analysis_result.bottlenecks_identified:
        report_lines.append(f"  • {bottleneck}")
    
    report_lines.append("")
    
    # Optimization Opportunities
    report_lines.extend([
        "OPTIMIZATION OPPORTUNITIES (by priority):",
        "",
    ])
    
    # Sort opportunities by priority
    priority_order = {'critical': 0, 'high': 1, 'medium': 2, 'low': 3}
    sorted_opportunities = sorted(
        analysis_result.optimization_opportunities, 
        key=lambda x: priority_order.get(x.priority, 4)
    )
    
    for i, opp in enumerate(sorted_opportunities, 1):
        priority_symbol = "🔴" if opp.priority == "critical" else \
                         "🟠" if opp.priority == "high" else \
                         "🟡" if opp.priority == "medium" else "🟢"
        
        report_lines.extend([
            f"{i}. {priority_symbol} {opp.category.upper()} - {opp.priority.upper()} PRIORITY",
            f"   Description: {opp.description}",
            f"   Expected Impact: {opp.impact_estimate}",
            f"   Implementation Effort: {opp.implementation_effort}",
            f"   Code Changes Required:",
        ])
        
        for change in opp.code_changes_required:
            report_lines.append(f"     - {change}")
        
        report_lines.extend([
            f"   Validation Strategy: {opp.test_validation_strategy}",
            f"   Performance Targets: {opp.performance_metric_targets}",
            "",
        ])
    
    # Immediate Actions
    report_lines.extend([
        "RECOMMENDED IMMEDIATE ACTIONS:",
    ])
    
    for action in analysis_result.recommended_immediate_actions:
        report_lines.append(f"  ⚡ {action}")
    
    report_lines.append("")
    
    # Long-term Architectural Suggestions
    report_lines.extend([
        "LONG-TERM ARCHITECTURAL SUGGESTIONS:",
    ])
    
    for suggestion in analysis_result.long_term_architectural_suggestions:
        report_lines.append(f"  🏗️  {suggestion}")
    
    report_lines.append("")
    
    # System-level Recommendations
    report_lines.extend([
        "SYSTEM-LEVEL RECOMMENDATIONS:",
    ])
    
    for recommendation in analysis_result.system_level_recommendations:
        report_lines.append(f"  ⚙️  {recommendation}")
    
    report_lines.append("")
    
    # Risk Assessment
    risks = analysis_result.risk_assessment
    report_lines.extend([
        "RISK ASSESSMENT:",
        f"  Implementation Complexity: {risks.get('implementation_complexity', 'unknown').upper()}",
        f"  Regression Risk: {risks.get('regression_risk', 'unknown').upper()}",
        f"  Maintenance Burden: {risks.get('maintenance_burden', 'unknown').upper()}",
        f"  Compatibility Risk: {risks.get('compatibility_risk', 'unknown').upper()}",
        "",
    ])
    
    # Implementation Timeline Suggestion
    report_lines.extend([
        "SUGGESTED IMPLEMENTATION TIMELINE:",
        "  Phase 1 (Weeks 1-2): Implement high-priority, low-effort optimizations",
        "  Phase 2 (Weeks 3-6): Address medium-effort improvements with high impact",
        "  Phase 3 (Months 2-3): Implement architectural changes and system-level optimizations",
        "  Phase 4 (Ongoing): Monitor performance and iterate based on production metrics",
        "",
        "MONITORING AND VALIDATION:",
        "  - Set up continuous performance monitoring with alerting",
        "  - Implement A/B testing for major optimizations",
        "  - Create performance regression test suite",
        "  - Document baseline performance characteristics",
        "  - Establish performance SLAs and budgets",
    ])
    
    return "\n".join(report_lines)


if __name__ == "__main__":
    print("Performance Optimization Analysis Suite for PathSandboxedFileSystem")
    print("Run with: pixi run -e test pytest -m 'performance and benchmark' tests/test_optimization_recommendations.py")