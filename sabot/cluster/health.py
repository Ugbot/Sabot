#!/usr/bin/env python3
"""
Cluster Health Monitor for Sabot distributed deployments.

Provides comprehensive cluster health monitoring with:
- Real-time health assessment
- Performance metrics aggregation
- Alert generation and escalation
- Health trend analysis
- Predictive failure detection
"""

import asyncio
import time
import logging
import statistics
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, field
from enum import Enum
from collections import deque

logger = logging.getLogger(__name__)


class ClusterHealth(Enum):
    """Overall cluster health status."""
    EXCELLENT = "excellent"
    GOOD = "good"
    FAIR = "fair"
    POOR = "poor"
    CRITICAL = "critical"


@dataclass
class HealthMetric:
    """Individual health metric measurement."""
    name: str
    value: float
    timestamp: float
    unit: str = ""
    labels: Dict[str, str] = field(default_factory=dict)


@dataclass
class HealthScore:
    """Health score for a component or node."""
    component: str
    score: float  # 0.0 to 1.0 (1.0 = perfect health)
    weight: float = 1.0
    metrics: List[HealthMetric] = field(default_factory=list)
    issues: List[str] = field(default_factory=list)
    timestamp: float = field(default_factory=time.time)


@dataclass
class ClusterHealthSnapshot:
    """Snapshot of cluster health at a point in time."""
    timestamp: float
    overall_health: ClusterHealth
    overall_score: float
    component_scores: Dict[str, HealthScore]
    alerts: List[Dict[str, Any]] = field(default_factory=list)
    recommendations: List[str] = field(default_factory=list)


class ClusterHealthMonitor:
    """
    Monitors and assesses the health of a Sabot cluster.

    Tracks various health indicators and provides actionable insights
    for maintaining cluster reliability and performance.
    """

    def __init__(self, history_size: int = 1000):
        self.history_size = history_size
        self.health_history: deque = deque(maxlen=history_size)
        self.current_snapshot: Optional[ClusterHealthSnapshot] = None

        # Health check functions
        self.health_checks: Dict[str, callable] = {}
        self._initialize_builtin_checks()

        # Alert thresholds
        self.alert_thresholds = {
            'node_failure_rate': 0.1,  # 10% nodes failing
            'avg_response_time': 5.0,  # 5 seconds
            'error_rate': 0.05,  # 5% error rate
            'cpu_usage': 0.85,  # 85% CPU usage
            'memory_usage': 0.90,  # 90% memory usage
            'disk_usage': 0.95  # 95% disk usage
        }

        # Component weights for overall score
        self.component_weights = {
            'nodes': 0.4,
            'coordinator': 0.3,
            'work_queue': 0.15,
            'network': 0.15
        }

    def _initialize_builtin_checks(self):
        """Initialize built-in health checks."""
        self.health_checks = {
            'node_health': self._check_node_health,
            'coordinator_health': self._check_coordinator_health,
            'work_queue_health': self._check_work_queue_health,
            'network_health': self._check_network_health,
            'resource_health': self._check_resource_health
        }

    async def assess_cluster_health(self, cluster_state: Dict[str, Any]) -> ClusterHealthSnapshot:
        """
        Assess overall cluster health based on current state.

        Args:
            cluster_state: Current cluster state information

        Returns:
            Health snapshot with assessment and recommendations
        """
        component_scores = {}

        # Assess each component
        for component_name, check_func in self.health_checks.items():
            try:
                score = await check_func(cluster_state)
                component_scores[component_name] = score
            except Exception as e:
                logger.error(f"Health check failed for {component_name}: {e}")
                # Create degraded score for failed checks
                component_scores[component_name] = HealthScore(
                    component=component_name,
                    score=0.3,
                    issues=[f"Health check failed: {str(e)}"]
                )

        # Calculate overall score
        overall_score = self._calculate_overall_score(component_scores)

        # Determine overall health
        overall_health = self._score_to_health(overall_score)

        # Generate alerts
        alerts = self._generate_alerts(component_scores, cluster_state)

        # Generate recommendations
        recommendations = self._generate_recommendations(component_scores, cluster_state)

        # Create snapshot
        snapshot = ClusterHealthSnapshot(
            timestamp=time.time(),
            overall_health=overall_health,
            overall_score=overall_score,
            component_scores=component_scores,
            alerts=alerts,
            recommendations=recommendations
        )

        # Store in history
        self.health_history.append(snapshot)
        self.current_snapshot = snapshot

        return snapshot

    async def _check_node_health(self, cluster_state: Dict[str, Any]) -> HealthScore:
        """Check health of cluster nodes."""
        nodes = cluster_state.get('nodes', [])
        if not nodes:
            return HealthScore(
                component='nodes',
                score=0.0,
                issues=['No nodes found in cluster']
            )

        active_nodes = [n for n in nodes if n.get('status') == 'active']
        total_nodes = len(nodes)

        if total_nodes == 0:
            return HealthScore(
                component='nodes',
                score=0.0,
                issues=['No nodes configured']
            )

        # Calculate health metrics
        active_ratio = len(active_nodes) / total_nodes
        avg_health_score = sum(n.get('health_score', 0.5) for n in active_nodes) / len(active_nodes) if active_nodes else 0

        # Assess issues
        issues = []
        if active_ratio < 0.8:
            issues.append(f"Only {active_ratio:.1%} of nodes are active")
        if avg_health_score < 0.7:
            issues.append(f"Average node health score is {avg_health_score:.2f}")

        # Calculate overall score
        score = (active_ratio * 0.6) + (avg_health_score * 0.4)

        return HealthScore(
            component='nodes',
            score=min(score, 1.0),
            metrics=[
                HealthMetric('active_nodes', len(active_nodes), time.time(), 'count'),
                HealthMetric('total_nodes', total_nodes, time.time(), 'count'),
                HealthMetric('active_ratio', active_ratio, time.time(), ''),
                HealthMetric('avg_health_score', avg_health_score, time.time(), '')
            ],
            issues=issues
        )

    async def _check_coordinator_health(self, cluster_state: Dict[str, Any]) -> HealthScore:
        """Check health of cluster coordinator."""
        coordinator_info = cluster_state.get('coordinator', {})

        score = 1.0
        issues = []
        metrics = []

        # Check if coordinator is responding
        last_heartbeat = coordinator_info.get('last_heartbeat', 0)
        time_since_heartbeat = time.time() - last_heartbeat

        if time_since_heartbeat > 60:  # 1 minute
            score *= 0.5
            issues.append(f"Coordinator heartbeat is {time_since_heartbeat:.0f}s old")

        metrics.append(HealthMetric(
            'coordinator_heartbeat_age',
            time_since_heartbeat,
            time.time(),
            'seconds'
        ))

        # Check coordinator load
        active_tasks = coordinator_info.get('active_tasks', 0)
        if active_tasks > 100:  # Arbitrary threshold
            score *= 0.8
            issues.append(f"Coordinator has {active_tasks} active tasks")

        metrics.append(HealthMetric(
            'coordinator_active_tasks',
            active_tasks,
            time.time(),
            'count'
        ))

        return HealthScore(
            component='coordinator',
            score=score,
            metrics=metrics,
            issues=issues
        )

    async def _check_work_queue_health(self, cluster_state: Dict[str, Any]) -> HealthScore:
        """Check health of work queue."""
        work_info = cluster_state.get('work', {})

        pending_work = work_info.get('pending_work', 0)
        active_work = work_info.get('active_work', 0)
        completed_work = work_info.get('completed_work', 0)
        failed_work = work_info.get('failed_work', 0)

        total_work = pending_work + active_work + completed_work + failed_work

        score = 1.0
        issues = []
        metrics = []

        # Check queue backlog
        if pending_work > 100:  # Arbitrary threshold
            score *= 0.7
            issues.append(f"Work queue has {pending_work} pending items")

        # Check failure rate
        if total_work > 0:
            failure_rate = failed_work / total_work
            if failure_rate > 0.1:  # 10% failure rate
                score *= 0.8
                issues.append(f"Work failure rate is {failure_rate:.1%}")

            metrics.append(HealthMetric(
                'work_failure_rate',
                failure_rate,
                time.time(),
                ''
            ))

        metrics.extend([
            HealthMetric('pending_work', pending_work, time.time(), 'count'),
            HealthMetric('active_work', active_work, time.time(), 'count'),
            HealthMetric('completed_work', completed_work, time.time(), 'count'),
            HealthMetric('failed_work', failed_work, time.time(), 'count')
        ])

        return HealthScore(
            component='work_queue',
            score=score,
            metrics=metrics,
            issues=issues
        )

    async def _check_network_health(self, cluster_state: Dict[str, Any]) -> HealthScore:
        """Check network health between nodes."""
        # This would perform network connectivity tests
        # For now, use a simplified check

        nodes = cluster_state.get('nodes', [])
        score = 1.0
        issues = []
        metrics = []

        # Check for network partitions (simplified)
        active_nodes = [n for n in nodes if n.get('status') == 'active']
        isolated_nodes = [n for n in nodes if n.get('status') == 'isolated']

        if isolated_nodes:
            score *= 0.6
            issues.append(f"{len(isolated_nodes)} nodes are isolated")

        # Network latency (placeholder)
        avg_latency = cluster_state.get('avg_network_latency', 0.01)
        if avg_latency > 0.1:  # 100ms
            score *= 0.9
            issues.append(f"Average network latency is {avg_latency*1000:.0f}ms")

        metrics.extend([
            HealthMetric('active_nodes', len(active_nodes), time.time(), 'count'),
            HealthMetric('isolated_nodes', len(isolated_nodes), time.time(), 'count'),
            HealthMetric('avg_network_latency', avg_latency, time.time(), 'seconds')
        ])

        return HealthScore(
            component='network',
            score=score,
            metrics=metrics,
            issues=issues
        )

    async def _check_resource_health(self, cluster_state: Dict[str, Any]) -> HealthScore:
        """Check resource utilization across cluster."""
        nodes = cluster_state.get('nodes', [])

        if not nodes:
            return HealthScore(
                component='resources',
                score=0.0,
                issues=['No nodes available for resource check']
            )

        # Aggregate resource metrics
        total_cpu = sum(n.get('cpu_usage', 0) for n in nodes)
        total_memory = sum(n.get('memory_usage', 0) for n in nodes)
        total_disk = sum(n.get('disk_usage', 0) for n in nodes)

        avg_cpu = total_cpu / len(nodes)
        avg_memory = total_memory / len(nodes)
        avg_disk = total_disk / len(nodes)

        score = 1.0
        issues = []
        metrics = []

        # Check resource thresholds
        if avg_cpu > 0.8:
            score *= 0.8
            issues.append(f"Average CPU usage is {avg_cpu:.1%}")

        if avg_memory > 0.85:
            score *= 0.8
            issues.append(f"Average memory usage is {avg_memory:.1%}")

        if avg_disk > 0.9:
            score *= 0.7
            issues.append(f"Average disk usage is {avg_disk:.1%}")

        metrics.extend([
            HealthMetric('avg_cpu_usage', avg_cpu, time.time(), ''),
            HealthMetric('avg_memory_usage', avg_memory, time.time(), ''),
            HealthMetric('avg_disk_usage', avg_disk, time.time(), '')
        ])

        return HealthScore(
            component='resources',
            score=score,
            metrics=metrics,
            issues=issues
        )

    def _calculate_overall_score(self, component_scores: Dict[str, HealthScore]) -> float:
        """Calculate overall cluster health score."""
        if not component_scores:
            return 0.0

        weighted_sum = 0.0
        total_weight = 0.0

        for component_name, score in component_scores.items():
            weight = self.component_weights.get(component_name, 1.0)
            weighted_sum += score.score * weight
            total_weight += weight

        return weighted_sum / total_weight if total_weight > 0 else 0.0

    def _score_to_health(self, score: float) -> ClusterHealth:
        """Convert numeric score to health enum."""
        if score >= 0.9:
            return ClusterHealth.EXCELLENT
        elif score >= 0.8:
            return ClusterHealth.GOOD
        elif score >= 0.7:
            return ClusterHealth.FAIR
        elif score >= 0.6:
            return ClusterHealth.POOR
        else:
            return ClusterHealth.CRITICAL

    def _generate_alerts(self, component_scores: Dict[str, HealthScore],
                        cluster_state: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Generate alerts based on health assessment."""
        alerts = []

        # Check component scores
        for component_name, score in component_scores.items():
            if score.score < 0.7:  # Below fair health
                severity = 'critical' if score.score < 0.5 else 'warning'

                alerts.append({
                    'type': 'component_health',
                    'severity': severity,
                    'component': component_name,
                    'message': f"{component_name} health is {score.score:.2f}",
                    'issues': score.issues,
                    'timestamp': time.time()
                })

        # Check resource thresholds
        nodes = cluster_state.get('nodes', [])
        for node in nodes:
            cpu_usage = node.get('cpu_usage', 0)
            if cpu_usage > self.alert_thresholds['cpu_usage']:
                alerts.append({
                    'type': 'resource_usage',
                    'severity': 'warning',
                    'component': f"node_{node['node_id']}",
                    'message': f"High CPU usage: {cpu_usage:.1%}",
                    'resource': 'cpu',
                    'current': cpu_usage,
                    'threshold': self.alert_thresholds['cpu_usage'],
                    'timestamp': time.time()
                })

        return alerts

    def _generate_recommendations(self, component_scores: Dict[str, HealthScore],
                                cluster_state: Dict[str, Any]) -> List[str]:
        """Generate health improvement recommendations."""
        recommendations = []

        # Check node health
        node_score = component_scores.get('nodes')
        if node_score and node_score.score < 0.8:
            recommendations.append("Consider adding more nodes to improve cluster resilience")
            recommendations.append("Investigate failing nodes and replace if necessary")

        # Check resource usage
        resource_score = component_scores.get('resources')
        if resource_score and resource_score.score < 0.8:
            recommendations.append("Monitor resource usage and scale nodes as needed")
            recommendations.append("Consider optimizing workloads to reduce resource consumption")

        # Check work queue
        work_score = component_scores.get('work_queue')
        if work_score and work_score.score < 0.8:
            pending_work = cluster_state.get('work', {}).get('pending_work', 0)
            if pending_work > 50:
                recommendations.append("Work queue is backing up - consider adding more worker nodes")

        # Check network
        network_score = component_scores.get('network')
        if network_score and network_score.score < 0.8:
            recommendations.append("Network issues detected - check connectivity between nodes")

        return recommendations

    def get_health_trends(self, hours: int = 24) -> Dict[str, Any]:
        """Analyze health trends over time."""
        if not self.health_history:
            return {'error': 'No health history available'}

        # Get snapshots from the last N hours
        cutoff_time = time.time() - (hours * 3600)
        recent_snapshots = [
            s for s in self.health_history
            if s.timestamp >= cutoff_time
        ]

        if not recent_snapshots:
            return {'error': 'No recent health data'}

        # Analyze trends
        scores = [s.overall_score for s in recent_snapshots]
        health_trend = 'stable'

        if len(scores) >= 2:
            recent_avg = statistics.mean(scores[-10:]) if len(scores) >= 10 else statistics.mean(scores)
            older_avg = statistics.mean(scores[:-10]) if len(scores) >= 20 else statistics.mean(scores[:len(scores)//2])

            if recent_avg > older_avg + 0.05:
                health_trend = 'improving'
            elif recent_avg < older_avg - 0.05:
                health_trend = 'degrading'

        return {
            'trend': health_trend,
            'avg_score': statistics.mean(scores),
            'min_score': min(scores),
            'max_score': max(scores),
            'score_volatility': statistics.stdev(scores) if len(scores) > 1 else 0,
            'measurements': len(scores),
            'time_range_hours': hours
        }

    def get_health_summary(self) -> Dict[str, Any]:
        """Get current health summary."""
        if not self.current_snapshot:
            return {'status': 'unknown', 'message': 'No health assessment performed'}

        return {
            'overall_health': self.current_snapshot.overall_health.value,
            'overall_score': self.current_snapshot.overall_score,
            'timestamp': self.current_snapshot.timestamp,
            'components': {
                name: {
                    'score': score.score,
                    'issues': score.issues
                }
                for name, score in self.current_snapshot.component_scores.items()
            },
            'active_alerts': len(self.current_snapshot.alerts),
            'recommendations': self.current_snapshot.recommendations
        }
