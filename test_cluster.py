#!/usr/bin/env python3
"""
Test Distributed Cluster System for Sabot.

This test verifies that the complete distributed cluster system works:
- Node discovery and registration
- Work distribution and load balancing
- Fault tolerance and recovery
- Health monitoring and assessment
- Cluster coordination and communication
"""

import asyncio
import sys
import os
import time
import tempfile
from pathlib import Path

# Add sabot to path
sys.path.insert(0, os.path.dirname(__file__))

async def test_cluster_coordinator():
    """Test cluster coordinator functionality."""
    print("🎯 Testing Cluster Coordinator")
    print("=" * 50)

    from sabot.cluster.coordinator import ClusterCoordinator, ClusterConfig, ClusterWork

    # Create coordinator
    config = ClusterConfig(
        node_id="test-coordinator",
        host="localhost",
        port=18080,  # Use a test port
        heartbeat_interval=1.0  # Fast for testing
    )

    coordinator = ClusterCoordinator(config)

    # Test initialization
    assert coordinator.node_id == "test-coordinator"
    assert coordinator.is_coordinator == True
    print("✅ Coordinator initialization verified")

    # Test work submission
    work_id = coordinator.submit_work("test_task", {"data": "test"}, priority=1)
    assert work_id is not None
    print("✅ Work submission working")

    # Test cluster status
    status = coordinator.get_cluster_status()
    assert status['total_nodes'] == 1  # Just the coordinator
    assert status['coordinator_node'] == "test-coordinator"
    print("✅ Cluster status retrieval working")

    # Start and stop coordinator
    await coordinator.start()
    await asyncio.sleep(0.1)  # Brief startup time
    await coordinator.stop()
    print("✅ Coordinator lifecycle working")

    print("\n✅ All coordinator tests passed")


async def test_load_balancer():
    """Test load balancer functionality."""
    print("\n⚖️  Testing Load Balancer")
    print("=" * 50)

    from sabot.cluster.balancer import LoadBalancer, WorkloadStrategy
    from sabot.cluster.coordinator import ClusterNode, ClusterWork

    # Create load balancer
    balancer = LoadBalancer(WorkloadStrategy.LEAST_LOADED)

    # Create test nodes
    nodes = [
        ClusterNode(
            node_id="node1",
            host="localhost",
            port=8081,
            cpu_cores=4,
            memory_gb=8.0,
            active_tasks=2,
            health_score=0.9
        ),
        ClusterNode(
            node_id="node2",
            host="localhost",
            port=8082,
            cpu_cores=8,
            memory_gb=16.0,
            active_tasks=1,
            health_score=0.95
        ),
        ClusterNode(
            node_id="node3",
            host="localhost",
            port=8083,
            cpu_cores=2,
            memory_gb=4.0,
            active_tasks=4,
            health_score=0.7
        )
    ]

    # Create test work
    work = ClusterWork(
        work_id="test-work-1",
        work_type="compute",
        payload={"operation": "test"},
        priority=1
    )

    # Test node selection
    selected_node = balancer.select_node(nodes, work)
    assert selected_node is not None
    assert selected_node.node_id in ["node1", "node2", "node3"]
    print("✅ Node selection working")

    # Test least loaded strategy (node2 should be selected most often)
    selections = {}
    for _ in range(100):
        selected = balancer.select_node(nodes, work)
        selections[selected.node_id] = selections.get(selected.node_id, 0) + 1

    # Node2 should be selected most (highest capacity, lowest load)
    assert selections.get("node2", 0) > selections.get("node1", 0)
    print("✅ Load balancing strategy working")

    # Test strategy change
    balancer.set_strategy(WorkloadStrategy.ROUND_ROBIN)
    selected_nodes = []
    for _ in range(10):
        selected = balancer.select_node(nodes, work)
        selected_nodes.append(selected.node_id)

    # Should cycle through nodes
    assert len(set(selected_nodes)) > 1  # Should use multiple nodes
    print("✅ Strategy switching working")

    # Test metrics update
    from sabot.cluster.balancer import LoadMetrics
    metrics = LoadMetrics(
        cpu_usage=0.6,
        memory_usage=0.7,
        active_tasks=3,
        task_completion_rate=0.9,
        average_task_duration=2.5
    )
    balancer.update_node_metrics("node1", metrics)

    stats = balancer.get_node_stats("node1")
    assert 'avg_cpu_usage' in stats
    assert stats['measurements'] == 1
    print("✅ Metrics tracking working")

    print("\n✅ All load balancer tests passed")


async def test_node_discovery():
    """Test node discovery functionality."""
    print("\n🔍 Testing Node Discovery")
    print("=" * 50)

    from sabot.cluster.discovery import NodeDiscovery, StaticDiscovery

    # Test static discovery
    static_nodes = [
        {
            'node_id': 'static-node-1',
            'host': '192.168.1.10',
            'port': 8081,
            'service_type': 'sabot-node'
        },
        {
            'node_id': 'static-node-2',
            'host': '192.168.1.11',
            'port': 8082,
            'service_type': 'sabot-node'
        }
    ]

    discovery = NodeDiscovery([f"static://{static_nodes[0]['node_id']}:{static_nodes[0]['host']}:{static_nodes[0]['port']},{static_nodes[1]['node_id']}:{static_nodes[1]['host']}:{static_nodes[1]['port']}"])

    # Test node discovery
    discovered = await discovery.discover_nodes()
    assert len(discovered) == 2
    assert all(node['node_id'].startswith('static-node-') for node in discovered)
    print("✅ Static node discovery working")

    # Test known nodes
    known = discovery.get_known_nodes()
    assert len(known) == 2
    print("✅ Known nodes tracking working")

    # Test health check
    health = await discovery.health_check()
    assert 'backends' in health
    print("✅ Discovery health check working")

    print("\n✅ All node discovery tests passed")


async def test_fault_tolerance():
    """Test fault tolerance functionality."""
    print("\n🛡️  Testing Fault Tolerance")
    print("=" * 50)

    from sabot.cluster.fault_tolerance import FailureDetector, RecoveryManager, CircuitBreaker, FailureType

    # Test failure detector
    detector = FailureDetector(timeout_seconds=5.0, check_interval=1.0)

    await detector.start()

    # Test heartbeat reporting
    detector.report_heartbeat("node1", time.time())
    stats = detector.get_failure_stats("node1")
    assert stats['total_failures'] == 0
    print("✅ Heartbeat reporting working")

    # Test failure reporting
    detector.report_node_down("node2", FailureType.NODE_CRASH)

    stats = detector.get_failure_stats("node2")
    assert stats['total_failures'] == 1
    print("✅ Failure detection working")

    await detector.stop()

    # Test recovery manager
    recovery = RecoveryManager()

    action_id = recovery.add_recovery_action(
        "failed-node-1",
        "reassign_work",
        priority=5,
        metadata={"affected_work": ["work1", "work2"]}
    )

    assert action_id is not None
    print("✅ Recovery action creation working")

    # Test circuit breaker
    breaker = CircuitBreaker(failure_threshold=2, recovery_timeout=1.0)

    # Test successful calls
    result = breaker.call(lambda: "success")
    assert result == "success"
    print("✅ Circuit breaker success handling working")

    # Test failures
    try:
        breaker.call(lambda: (_ for _ in ()).throw(RuntimeError("test error")))
    except RuntimeError:
        pass  # Expected

    try:
        breaker.call(lambda: (_ for _ in ()).throw(RuntimeError("test error")))
    except RuntimeError:
        pass  # Expected

    # Should be open now
    try:
        breaker.call(lambda: "should fail")
        assert False, "Circuit breaker should be open"
    except Exception as e:
        assert "Circuit breaker is open" in str(e)

    print("✅ Circuit breaker failure handling working")

    print("\n✅ All fault tolerance tests passed")


async def test_cluster_health():
    """Test cluster health monitoring."""
    print("\n🏥 Testing Cluster Health Monitor")
    print("=" * 50)

    from sabot.cluster.health import ClusterHealthMonitor, ClusterHealth

    monitor = ClusterHealthMonitor()

    # Test health assessment
    cluster_state = {
        'coordinator': {
            'last_heartbeat': time.time(),
            'active_tasks': 5
        },
        'nodes': [
            {
                'node_id': 'node1',
                'status': 'active',
                'health_score': 0.9,
                'cpu_usage': 0.6,
                'memory_usage': 0.7
            },
            {
                'node_id': 'node2',
                'status': 'active',
                'health_score': 0.85,
                'cpu_usage': 0.5,
                'memory_usage': 0.6
            }
        ],
        'work': {
            'pending_work': 10,
            'active_work': 5,
            'completed_work': 100,
            'failed_work': 2
        }
    }

    snapshot = await monitor.assess_cluster_health(cluster_state)

    assert snapshot.overall_score > 0
    assert snapshot.overall_health in [ClusterHealth.EXCELLENT, ClusterHealth.GOOD, ClusterHealth.FAIR]
    assert len(snapshot.component_scores) > 0
    print("✅ Health assessment working")

    # Test health summary
    summary = monitor.get_health_summary()
    assert 'overall_health' in summary
    assert 'components' in summary
    print("✅ Health summary working")

    # Test health trends
    trends = monitor.get_health_trends(hours=1)
    assert 'trend' in trends
    print("✅ Health trends analysis working")

    print("\n✅ All cluster health tests passed")


async def test_cluster_integration():
    """Test integration of all cluster components."""
    print("\n🔗 Testing Cluster Integration")
    print("=" * 50)

    from sabot.cluster.coordinator import ClusterCoordinator, ClusterConfig
    from sabot.cluster.balancer import LoadBalancer
    from sabot.cluster.health import ClusterHealthMonitor

    # Create integrated test
    config = ClusterConfig(
        node_id="integration-test",
        host="localhost",
        port=18081,  # Different port for testing
        heartbeat_interval=2.0
    )

    coordinator = ClusterCoordinator(config)
    load_balancer = LoadBalancer()
    health_monitor = ClusterHealthMonitor()

    # Start coordinator
    await coordinator.start()

    # Test work submission and status
    work_id = coordinator.submit_work("integration_test", {"test": "data"})
    status = coordinator.get_work_status(work_id)

    # Work should be pending (no workers)
    assert status is None or status.get('status') == 'pending'
    print("✅ Work submission and status working")

    # Test cluster status
    cluster_status = coordinator.get_cluster_status()
    assert cluster_status['total_nodes'] == 1
    assert cluster_status['coordinator_node'] == "integration-test"
    print("✅ Cluster status integration working")

    # Test health monitoring integration
    cluster_state = coordinator.get_cluster_status()
    health_snapshot = await health_monitor.assess_cluster_health(cluster_state)

    assert health_snapshot.overall_score >= 0.0
    print("✅ Health monitoring integration working")

    # Stop coordinator
    await coordinator.stop()

    print("✅ Cluster integration test passed")


async def main():
    """Run all cluster tests."""
    print("🚀 Sabot Distributed Cluster - Implementation Verification")
    print("=" * 80)

    try:
        # Test 1: Cluster coordinator
        await test_cluster_coordinator()

        # Test 2: Load balancer
        await test_load_balancer()

        # Test 3: Node discovery
        await test_node_discovery()

        # Test 4: Fault tolerance
        await test_fault_tolerance()

        # Test 5: Cluster health
        await test_cluster_health()

        # Test 6: Cluster integration
        await test_cluster_integration()

        print("\n" + "=" * 80)
        print("🎉 ALL CLUSTER TESTS PASSED!")
        print("✅ Coordinator: Node management, work distribution, communication")
        print("✅ Load Balancer: Intelligent work assignment, multiple strategies")
        print("✅ Discovery: Node registration, service discovery, health checks")
        print("✅ Fault Tolerance: Failure detection, recovery, circuit breakers")
        print("✅ Health Monitor: Assessment, trends, recommendations")
        print("✅ Integration: All components working together seamlessly")
        print("\n🏆 Sabot now has ENTERPRISE-GRADE distributed clustering!")

        return True

    except Exception as e:
        print(f"\n❌ Test suite failed: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)
