#!/usr/bin/env python3
"""
Load Balancer for distributed work distribution in Sabot clusters.

Provides intelligent load balancing strategies for optimal work distribution
across cluster nodes based on resource availability, performance metrics,
and workload characteristics.
"""

import time
import random
import logging
from typing import List, Dict, Any, Optional, Callable
from enum import Enum
from dataclasses import dataclass

from .types import ClusterNode, ClusterWork, LoadMetrics

logger = logging.getLogger(__name__)


class WorkloadStrategy(Enum):
    """Load balancing strategies for work distribution."""
    ROUND_ROBIN = "round_robin"
    LEAST_LOADED = "least_loaded"
    WEIGHTED_RANDOM = "weighted_random"
    RESOURCE_AWARE = "resource_aware"
    PERFORMANCE_BASED = "performance_based"




class LoadBalancer:
    """
    Intelligent load balancer for distributing work across cluster nodes.

    Uses various strategies to optimize work distribution based on node
    capabilities, current load, and work requirements.
    """

    def __init__(self, strategy: WorkloadStrategy = WorkloadStrategy.LEAST_LOADED):
        self.strategy = strategy
        self.round_robin_index = 0
        self.node_load_history: Dict[str, List[LoadMetrics]] = {}
        self.strategy_functions: Dict[WorkloadStrategy, Callable] = {
            WorkloadStrategy.ROUND_ROBIN: self._round_robin_select,
            WorkloadStrategy.LEAST_LOADED: self._least_loaded_select,
            WorkloadStrategy.WEIGHTED_RANDOM: self._weighted_random_select,
            WorkloadStrategy.RESOURCE_AWARE: self._resource_aware_select,
            WorkloadStrategy.PERFORMANCE_BASED: self._performance_based_select
        }

    def select_node(self, available_nodes: List[ClusterNode],
                   work: ClusterWork) -> Optional[ClusterNode]:
        """
        Select the best node for the given work from available nodes.

        Args:
            available_nodes: List of nodes that can accept work
            work: Work to be assigned

        Returns:
            Selected node or None if no suitable node found
        """
        if not available_nodes:
            return None

        if len(available_nodes) == 1:
            return available_nodes[0]

        # Use the configured strategy
        strategy_func = self.strategy_functions.get(self.strategy, self._least_loaded_select)
        return strategy_func(available_nodes, work)

    def _round_robin_select(self, nodes: List[ClusterNode], work: ClusterWork) -> Optional[ClusterNode]:
        """Round-robin node selection."""
        if not nodes:
            return None

        # Select next node in round-robin fashion
        selected_node = nodes[self.round_robin_index % len(nodes)]
        self.round_robin_index += 1

        return selected_node

    def _least_loaded_select(self, nodes: List[ClusterNode], work: ClusterWork) -> Optional[ClusterNode]:
        """Select the least loaded node."""
        if not nodes:
            return None

        # Sort nodes by load score (lower is better)
        sorted_nodes = sorted(nodes, key=lambda n: self._calculate_node_load(n))

        # Return the least loaded node
        return sorted_nodes[0]

    def _weighted_random_select(self, nodes: List[ClusterNode], work: ClusterWork) -> Optional[ClusterNode]:
        """Select node using weighted random based on available capacity."""
        if not nodes:
            return None

        # Calculate weights based on available capacity (inverse of load)
        weights = []
        for node in nodes:
            load_score = self._calculate_node_load(node)
            # Weight is inverse of load (higher capacity = higher weight)
            weight = max(0.1, 1.0 - load_score)  # Minimum weight of 0.1
            weights.append(weight)

        # Normalize weights
        total_weight = sum(weights)
        if total_weight == 0:
            return random.choice(nodes)

        normalized_weights = [w / total_weight for w in weights]

        # Weighted random selection
        r = random.random()
        cumulative = 0.0
        for i, weight in enumerate(normalized_weights):
            cumulative += weight
            if r <= cumulative:
                return nodes[i]

        # Fallback
        return nodes[-1]

    def _resource_aware_select(self, nodes: List[ClusterNode], work: ClusterWork) -> Optional[ClusterNode]:
        """Select node based on resource requirements of the work."""
        if not nodes:
            return None

        # Analyze work requirements
        work_requirements = self._analyze_work_requirements(work)

        # Score nodes based on how well they match requirements
        node_scores = []
        for node in nodes:
            score = self._score_node_for_work(node, work_requirements)
            node_scores.append((node, score))

        # Return highest scoring node
        if node_scores:
            return max(node_scores, key=lambda x: x[1])[0]

        return None

    def _performance_based_select(self, nodes: List[ClusterNode], work: ClusterWork) -> Optional[ClusterNode]:
        """Select node based on historical performance."""
        if not nodes:
            return None

        # Get performance metrics for each node
        node_scores = []
        for node in nodes:
            performance_score = self._calculate_performance_score(node, work.work_type)
            load_penalty = self._calculate_node_load(node) * 0.3  # 30% penalty for load
            final_score = performance_score * (1.0 - load_penalty)
            node_scores.append((node, final_score))

        # Return highest scoring node
        if node_scores:
            return max(node_scores, key=lambda x: x[1])[0]

        return None

    def _calculate_node_load(self, node: ClusterNode) -> float:
        """Calculate current load score for a node (0.0 to 1.0)."""
        # Simple load calculation based on active tasks and health
        task_load = min(node.active_tasks / max(1, node.cpu_cores * 2), 1.0)  # Assume 2 tasks per core max
        health_penalty = 1.0 - node.health_score

        return min(task_load + health_penalty, 1.0)

    def _analyze_work_requirements(self, work: ClusterWork) -> Dict[str, Any]:
        """Analyze work requirements for optimal node selection."""
        requirements = {
            'cpu_intensive': False,
            'memory_intensive': False,
            'io_intensive': False,
            'min_cpu_cores': 1,
            'min_memory_gb': 0.5,
            'priority': work.priority
        }

        # Analyze work type for resource requirements
        work_type = work.work_type.lower()

        if any(keyword in work_type for keyword in ['compute', 'math', 'ml', 'ai', 'training']):
            requirements['cpu_intensive'] = True
            requirements['min_cpu_cores'] = 2

        if any(keyword in work_type for keyword in ['stream', 'process', 'transform']):
            requirements['memory_intensive'] = True
            requirements['min_memory_gb'] = 2.0

        if any(keyword in work_type for keyword in ['io', 'file', 'network', 'database']):
            requirements['io_intensive'] = True

        # Analyze payload size for memory requirements
        payload_size = len(str(work.payload).encode())
        if payload_size > 10 * 1024 * 1024:  # 10MB
            requirements['memory_intensive'] = True
            requirements['min_memory_gb'] = max(requirements['min_memory_gb'], 4.0)

        return requirements

    def _score_node_for_work(self, node: ClusterNode, requirements: Dict[str, Any]) -> float:
        """Score how well a node matches work requirements."""
        score = 1.0  # Base score

        # Resource availability scoring
        if requirements['min_cpu_cores'] > node.cpu_cores:
            return 0.0  # Node doesn't meet minimum requirements

        if requirements['min_memory_gb'] > node.memory_gb:
            return 0.0  # Node doesn't meet minimum requirements

        # CPU scoring
        cpu_capacity = node.cpu_cores / requirements['min_cpu_cores']
        score *= min(cpu_capacity, 2.0)  # Cap at 2x requirements

        # Memory scoring
        memory_capacity = node.memory_gb / requirements['min_memory_gb']
        score *= min(memory_capacity, 2.0)  # Cap at 2x requirements

        # Load penalty
        load_penalty = self._calculate_node_load(node)
        score *= (1.0 - load_penalty)

        # Health bonus
        health_bonus = node.health_score - 0.5  # Bonus for health above 0.5
        score *= (1.0 + health_bonus)

        # Priority bonus (higher priority gets better nodes)
        if requirements['priority'] > 1:
            score *= (1.0 + (requirements['priority'] - 1) * 0.1)

        return max(0.0, score)

    def _calculate_performance_score(self, node: ClusterNode, work_type: str) -> float:
        """Calculate performance score for a node based on historical data."""
        # Get recent performance history for this work type
        history = self.node_load_history.get(node.node_id, [])

        if not history:
            return 0.5  # Neutral score if no history

        # Calculate average performance metrics
        recent_history = history[-10:]  # Last 10 measurements
        avg_completion_rate = sum(h.task_completion_rate for h in recent_history) / len(recent_history)
        avg_duration = sum(h.average_task_duration for h in recent_history) / len(recent_history)

        # Higher completion rate and lower duration = better score
        completion_score = min(avg_completion_rate, 1.0)
        duration_score = max(0, 1.0 - (avg_duration / 60.0))  # Penalize >60s average

        return (completion_score + duration_score) / 2.0

    def update_node_metrics(self, node_id: str, metrics: LoadMetrics) -> None:
        """Update performance metrics for a node."""
        if node_id not in self.node_load_history:
            self.node_load_history[node_id] = []

        self.node_load_history[node_id].append(metrics)

        # Keep only recent history (last 100 measurements)
        if len(self.node_load_history[node_id]) > 100:
            self.node_load_history[node_id] = self.node_load_history[node_id][-100:]

    def get_node_stats(self, node_id: str) -> Dict[str, Any]:
        """Get statistics for a specific node."""
        history = self.node_load_history.get(node_id, [])

        if not history:
            return {}

        recent = history[-10:] if len(history) >= 10 else history

        return {
            'avg_cpu_usage': sum(h.cpu_usage for h in recent) / len(recent),
            'avg_memory_usage': sum(h.memory_usage for h in recent) / len(recent),
            'avg_active_tasks': sum(h.active_tasks for h in recent) / len(recent),
            'avg_completion_rate': sum(h.task_completion_rate for h in recent) / len(recent),
            'avg_task_duration': sum(h.average_task_duration for h in recent) / len(recent),
            'measurements': len(history)
        }

    def set_strategy(self, strategy: WorkloadStrategy) -> None:
        """Change the load balancing strategy."""
        self.strategy = strategy
        logger.info(f"Load balancer strategy changed to: {strategy.value}")

    def get_strategy_stats(self) -> Dict[str, Any]:
        """Get statistics about the current strategy performance."""
        total_nodes = len(self.node_load_history)
        total_measurements = sum(len(history) for history in self.node_load_history.values())

        avg_load_scores = []
        for node_id, history in self.node_load_history.items():
            if history:
                avg_load = sum(h.load_score for h in history) / len(history)
                avg_load_scores.append(avg_load)

        return {
            'strategy': self.strategy.value,
            'total_nodes': total_nodes,
            'total_measurements': total_measurements,
            'avg_load_balance': sum(avg_load_scores) / len(avg_load_scores) if avg_load_scores else 0.0,
            'load_distribution_stddev': statistics.stdev(avg_load_scores) if len(avg_load_scores) > 1 else 0.0
        }


# Utility functions for work analysis

def analyze_work_complexity(work: ClusterWork) -> Dict[str, Any]:
    """Analyze the computational complexity of work."""
    complexity = {
        'estimated_duration': 1.0,  # seconds
        'cpu_intensity': 0.5,  # 0.0 to 1.0
        'memory_intensity': 0.5,  # 0.0 to 1.0
        'io_intensity': 0.5,  # 0.0 to 1.0
        'parallelizable': False
    }

    # Analyze based on work type
    work_type = work.work_type.lower()

    if 'stream' in work_type or 'process' in work_type:
        complexity['cpu_intensity'] = 0.7
        complexity['memory_intensity'] = 0.8
        complexity['parallelizable'] = True
    elif 'compute' in work_type or 'math' in work_type:
        complexity['cpu_intensity'] = 0.9
        complexity['estimated_duration'] = 5.0
        complexity['parallelizable'] = True
    elif 'io' in work_type or 'file' in work_type:
        complexity['io_intensity'] = 0.8
        complexity['estimated_duration'] = 2.0
    elif 'network' in work_type:
        complexity['io_intensity'] = 0.9
        complexity['estimated_duration'] = 1.0

    # Adjust based on payload size
    payload_size = len(str(work.payload).encode())
    if payload_size > 100 * 1024 * 1024:  # 100MB
        complexity['memory_intensity'] = min(1.0, complexity['memory_intensity'] + 0.3)
        complexity['estimated_duration'] *= 1.5

    # Adjust based on priority
    if work.priority > 1:
        complexity['estimated_duration'] *= 0.8  # Higher priority = faster execution

    return complexity


def calculate_node_capacity_score(node: ClusterNode, work_complexity: Dict[str, Any]) -> float:
    """Calculate how well a node can handle specific work complexity."""
    # Resource availability scoring
    cpu_score = min(node.cpu_cores / max(1, work_complexity.get('cpu_intensity', 0.5) * 4), 1.0)
    memory_score = min(node.memory_gb / max(1, work_complexity.get('memory_intensity', 0.5) * 4), 1.0)

    # Health and load scoring
    health_score = node.health_score
    load_score = 1.0 - min(node.active_tasks / max(1, node.cpu_cores * 2), 1.0)

    # Combined score
    return (cpu_score * 0.3 + memory_score * 0.3 + health_score * 0.2 + load_score * 0.2)
