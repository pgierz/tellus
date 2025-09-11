"""
Unit tests for network topology domain entities.

This package contains comprehensive unit tests for the network topology domain entities
following clean architecture principles, testing pure domain logic without infrastructure dependencies.

Test modules:
- test_network_metrics.py: Tests for BandwidthMetrics, LatencyMetrics, NetworkHealth, NetworkPath
- test_network_connection.py: Tests for NetworkConnection and ConnectionType
- test_network_topology.py: Tests for NetworkTopology aggregate root
- conftest.py: Test fixtures, factories, and shared test data

The tests include:
- Complete business logic validation
- Edge case handling
- Property-based testing with Hypothesis
- Comprehensive error validation
- Path-finding algorithm testing
- Network health assessment testing
- Integration scenarios with complex topologies

Coverage includes:
- Value object validation and behavior
- Domain entity invariants and business rules
- Aggregate root coordination and consistency
- Path optimization algorithms (shortest, bandwidth, latency)
- Network topology analysis and metrics
- Clean separation between domain and infrastructure concerns
"""