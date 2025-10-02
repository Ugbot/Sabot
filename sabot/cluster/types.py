#!/usr/bin/env python3
"""
Shared types and data structures for Sabot cluster components.

This module contains shared classes and enums used across cluster components
to avoid circular import issues.
"""

import time
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, field
from enum import Enum


class NodeStatus(Enum):
    """Node status in the cluster."""
    UNKNOWN = "unknown"
    JOINING = "joining"
    ACTIVE = "active"
    LEAVING = "leaving"
    FAILED = "failed"
    ISOLATED = "isolated"


class NodeRole(Enum):
    """Node roles in the cluster."""
    COORDINATOR = "coordinator"
    WORKER = "worker"
    GATEWAY = "gateway"


@dataclass
class ClusterNode:
    """Represents a node in the cluster."""
    node_id: str
    host: str
    port: int
    role: NodeRole = NodeRole.WORKER
    status: NodeStatus = NodeStatus.UNKNOWN
    joined_at: float = field(default_factory=time.time)
    last_heartbeat: float = field(default_factory=time.time)
    capabilities: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)

    # Resource information
    cpu_cores: int = 0
    memory_gb: float = 0.0
    active_tasks: int = 0
    completed_tasks: int = 0

    # Health metrics
    health_score: float = 1.0
    consecutive_failures: int = 0

    @property
    def is_alive(self) -> bool:
        """Check if node is alive based on heartbeat."""
        return (time.time() - self.last_heartbeat) < 30.0  # 30 second timeout

    @property
    def address(self) -> str:
        """Get node address as host:port."""
        return f"{self.host}:{self.port}"

    def to_dict(self) -> Dict[str, Any]:
        """Convert node to dictionary for serialization."""
        return {
            'node_id': self.node_id,
            'host': self.host,
            'port': self.port,
            'role': self.role.value,
            'status': self.status.value,
            'joined_at': self.joined_at,
            'last_heartbeat': self.last_heartbeat,
            'capabilities': self.capabilities,
            'metadata': self.metadata,
            'cpu_cores': self.cpu_cores,
            'memory_gb': self.memory_gb,
            'active_tasks': self.active_tasks,
            'completed_tasks': self.completed_tasks,
            'health_score': self.health_score,
            'consecutive_failures': self.consecutive_failures
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ClusterNode':
        """Create node from dictionary."""
        return cls(
            node_id=data['node_id'],
            host=data['host'],
            port=data['port'],
            role=NodeRole(data['role']),
            status=NodeStatus(data['status']),
            joined_at=data.get('joined_at', time.time()),
            last_heartbeat=data.get('last_heartbeat', time.time()),
            capabilities=data.get('capabilities', {}),
            metadata=data.get('metadata', {}),
            cpu_cores=data.get('cpu_cores', 0),
            memory_gb=data.get('memory_gb', 0.0),
            active_tasks=data.get('active_tasks', 0),
            completed_tasks=data.get('completed_tasks', 0),
            health_score=data.get('health_score', 1.0),
            consecutive_failures=data.get('consecutive_failures', 0)
        )


@dataclass
class ClusterWork:
    """Represents a unit of work to be distributed."""
    work_id: str
    work_type: str
    payload: Dict[str, Any]
    priority: int = 1
    created_at: float = field(default_factory=time.time)
    assigned_to: Optional[str] = None
    assigned_at: Optional[float] = None
    completed_at: Optional[float] = None
    status: str = "pending"  # pending, assigned, completed, failed
    result: Optional[Any] = None
    retries: int = 0
    max_retries: int = 3


@dataclass
class ClusterConfig:
    """Configuration for the cluster coordinator."""
    node_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    host: str = "0.0.0.0"
    port: int = 8081
    role: NodeRole = NodeRole.COORDINATOR
    discovery_endpoints: List[str] = field(default_factory=list)
    heartbeat_interval: float = 10.0
    node_timeout: float = 30.0
    max_workers_per_node: int = 10
    work_queue_size: int = 1000
    enable_auto_scaling: bool = True
    min_nodes: int = 1
    max_nodes: int = 10


@dataclass
class LoadMetrics:
    """Load metrics for a node."""
    cpu_usage: float = 0.0
    memory_usage: float = 0.0
    active_tasks: int = 0
    task_completion_rate: float = 0.0
    average_task_duration: float = 0.0
    network_latency: float = 0.0

    @property
    def load_score(self) -> float:
        """Calculate overall load score (0.0 to 1.0, higher = more loaded)."""
        # Weighted combination of metrics
        cpu_weight = 0.3
        memory_weight = 0.3
        task_weight = 0.4

        cpu_score = min(self.cpu_usage / 100.0, 1.0)
        memory_score = min(self.memory_usage / 100.0, 1.0)
        task_score = min(self.active_tasks / 10.0, 1.0)  # Assume 10 max concurrent tasks

        return (cpu_score * cpu_weight +
                memory_score * memory_weight +
                task_score * task_weight)


# Import uuid here to avoid circular imports
import uuid
