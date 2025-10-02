# -*- coding: utf-8 -*-
"""DBOS Parallel Controller - Manages morsel-driven parallelism decisions."""

import asyncio
import time
import psutil
import threading
from typing import Dict, List, Any, Optional, Callable, Tuple
from dataclasses import dataclass, field
from collections import defaultdict, deque
import logging
import statistics

logger = logging.getLogger(__name__)

@dataclass
class WorkerMetrics:
    """Metrics for a worker's performance."""
    worker_id: int
    processed_count: int = 0
    processing_time: float = 0.0
    queue_depth: int = 0
    error_count: int = 0
    last_heartbeat: float = field(default_factory=time.time)
    current_load: float = 0.0  # 0.0 to 1.0

    @property
    def throughput(self) -> float:
        """Items processed per second."""
        if self.processing_time > 0:
            return self.processed_count / self.processing_time
        return 0.0

    @property
    def is_healthy(self) -> bool:
        """Check if worker is healthy."""
        return (time.time() - self.last_heartbeat) < 10.0 and self.error_count < 10

@dataclass
class MorselMetrics:
    """Metrics for morsel processing."""
    morsel_id: int
    size_bytes: int
    processing_time: float = 0.0
    worker_id: Optional[int] = None
    completed_at: Optional[float] = None
    success: bool = False

class AdaptiveLoadBalancer:
    """Adaptive load balancer that learns from performance patterns."""

    def __init__(self, num_workers: int):
        self.num_workers = num_workers
        self.worker_history: Dict[int, List[float]] = defaultdict(list)
        self.morsel_history: List[MorselMetrics] = []
        self.performance_weights: Dict[int, float] = {}
        self._lock = threading.RLock()

        # Initialize equal weights
        for i in range(num_workers):
            self.performance_weights[i] = 1.0

    def record_worker_performance(self, worker_id: int, throughput: float):
        """Record worker performance for learning."""
        with self._lock:
            self.worker_history[worker_id].append(throughput)

            # Keep only recent history
            if len(self.worker_history[worker_id]) > 100:
                self.worker_history[worker_id] = self.worker_history[worker_id][-50:]

            # Update weights based on relative performance
            if self.worker_history[worker_id]:
                avg_throughput = statistics.mean(self.worker_history[worker_id])
                max_throughput = max(
                    statistics.mean(history) for history in self.worker_history.values()
                    if history
                )

                if max_throughput > 0:
                    self.performance_weights[worker_id] = avg_throughput / max_throughput

    def select_worker_for_morsel(self, morsel_size: int, current_queue_depths: Dict[int, int]) -> int:
        """Select best worker for a morsel based on current state."""
        with self._lock:
            # Calculate scores for each worker
            scores = {}
            for worker_id in range(self.num_workers):
                queue_depth = current_queue_depths.get(worker_id, 0)
                performance_weight = self.performance_weights.get(worker_id, 1.0)

                # Score considers: performance weight, current load, and morsel size fit
                load_factor = min(queue_depth / 10.0, 1.0)  # Normalize queue depth
                score = performance_weight * (1.0 - load_factor)
                scores[worker_id] = score

            # Select worker with highest score
            return max(scores.items(), key=lambda x: x[1])[0]

    def get_load_distribution_plan(self, morsels: List[Any]) -> Dict[int, List[Any]]:
        """Create optimal load distribution plan."""
        with self._lock:
            # Get current queue depths (simplified - would come from real monitoring)
            current_queue_depths = {i: 0 for i in range(self.num_workers)}

            distribution = defaultdict(list)

            for morsel in morsels:
                # Get morsel size
                if hasattr(morsel, 'size_bytes'):
                    size = morsel.size_bytes()
                else:
                    size = len(str(morsel).encode('utf-8'))

                # Select worker
                worker_id = self.select_worker_for_morsel(size, current_queue_depths)
                distribution[worker_id].append(morsel)

                # Update simulated queue depth
                current_queue_depths[worker_id] += 1

            return dict(distribution)

class DBOSParallelController:
    """DBOS-controlled parallel processing coordinator.

    This is the "brain" that makes intelligent decisions about:
    - How many workers to use
    - How to distribute work
    - When to scale up/down
    - How to handle failures
    - Performance optimization
    """

    def __init__(self, max_workers: int = 64, target_utilization: float = 0.8):
        self.max_workers = max_workers
        self.target_utilization = target_utilization

        # State tracking
        self.active_workers: Dict[int, WorkerMetrics] = {}
        self.pending_morsels: deque = deque()
        self.completed_morsels: List[MorselMetrics] = []
        self.system_metrics = self._get_system_metrics()

        # Adaptive components
        self.load_balancer = AdaptiveLoadBalancer(max_workers)
        self.scaling_policy = AdaptiveScalingPolicy()

        # Performance monitoring
        self.monitoring_thread: Optional[threading.Thread] = None
        self._monitoring_active = False
        self._lock = threading.RLock()

        logger.info(f"Initialized DBOS Parallel Controller (max_workers={max_workers})")

    def _get_system_metrics(self) -> Dict[str, Any]:
        """Get current system resource metrics."""
        return {
            'cpu_percent': psutil.cpu_percent(interval=0.1),
            'memory_percent': psutil.virtual_memory().percent,
            'available_memory_gb': psutil.virtual_memory().available / (1024**3),
            'cpu_count': psutil.cpu_count(),
            'cpu_logical_count': psutil.cpu_count(logical=True),
            'load_avg': psutil.getloadavg() if hasattr(psutil, 'getloadavg') else (0, 0, 0),
        }

    async def processor_started(self, initial_worker_count: int):
        """Called when parallel processor starts."""
        logger.info(f"Parallel processor started with {initial_worker_count} workers")

        # Initialize worker metrics
        for i in range(initial_worker_count):
            self.active_workers[i] = WorkerMetrics(worker_id=i)

        # Start monitoring
        self._start_monitoring()

    async def processor_stopped(self):
        """Called when parallel processor stops."""
        logger.info("Parallel processor stopped")

        # Stop monitoring
        self._stop_monitoring()

    async def plan_work_distribution(self, morsels: List[Any], available_workers: int) -> Dict[int, List[Any]]:
        """Plan how to distribute work across workers."""

        # Update system metrics
        self.system_metrics = self._get_system_metrics()

        # Get optimal worker count based on current conditions
        optimal_workers = self._calculate_optimal_worker_count(len(morsels))

        # Adjust available workers if needed
        actual_workers = min(available_workers, optimal_workers, self.max_workers)

        # Use load balancer to create distribution plan
        distribution = self.load_balancer.get_load_distribution_plan(morsels)

        # Ensure we don't exceed optimal worker count
        if len(distribution) > actual_workers:
            # Redistribute excess work
            excess_workers = list(distribution.keys())[actual_workers:]
            for worker_id in excess_workers:
                morsels_to_redistribute = distribution.pop(worker_id)
                # Redistribute to existing workers
                for i, morsel in enumerate(morsels_to_redistribute):
                    target_worker = i % actual_workers
                    distribution[target_worker].append(morsel)

        logger.debug(f"Planned work distribution: {len(distribution)} workers, "
                    f"{sum(len(m) for m in distribution.values())} total morsels")

        return distribution

    def _calculate_optimal_worker_count(self, morsel_count: int) -> int:
        """Calculate optimal number of workers based on current conditions."""

        # Factors to consider:
        # 1. CPU utilization target
        # 2. Memory availability
        # 3. Morsel count
        # 4. System load

        cpu_percent = self.system_metrics['cpu_percent']
        memory_percent = self.system_metrics['memory_percent']
        cpu_count = self.system_metrics['cpu_count']

        # Base calculation on CPU cores
        base_workers = cpu_count

        # Adjust based on current utilization
        if cpu_percent > 80:
            # System is busy, reduce workers
            base_workers = max(1, int(base_workers * 0.7))
        elif cpu_percent < 30:
            # System has capacity, can use more workers
            base_workers = min(base_workers * 2, cpu_count * 2)

        # Adjust based on memory
        if memory_percent > 85:
            base_workers = max(1, int(base_workers * 0.8))

        # Adjust based on morsel count
        if morsel_count < base_workers:
            # Fewer morsels than workers, don't over-subscribe
            base_workers = max(1, morsel_count)

        # Apply scaling policy
        scaled_workers = self.scaling_policy.adjust_worker_count(
            base_workers, self.system_metrics, len(self.active_workers)
        )

        return min(scaled_workers, self.max_workers)

    async def assign_work_to_worker(self, worker_id: int) -> Optional[Any]:
        """Assign work to a worker (called during work stealing)."""
        with self._lock:
            if not self.pending_morsels:
                return None

            # Find best morsel for this worker
            best_morsel = None
            best_score = -1

            for morsel in self.pending_morsels:
                if hasattr(morsel, 'size_bytes'):
                    size = morsel.size_bytes()
                else:
                    size = len(str(morsel).encode('utf-8'))

                # Score based on worker's historical performance with similar sizes
                score = self.load_balancer.performance_weights.get(worker_id, 1.0)
                if size < 1024:  # Small morsels
                    score *= 1.2  # Prefer for small items

                if score > best_score:
                    best_score = score
                    best_morsel = morsel

            if best_morsel:
                self.pending_morsels.remove(best_morsel)
                return best_morsel

        return None

    async def report_work_completion(self, worker_id: int, morsel_id: int, result: Any):
        """Report work completion."""
        with self._lock:
            if worker_id in self.active_workers:
                worker = self.active_workers[worker_id]
                worker.processed_count += 1
                worker.last_heartbeat = time.time()

                # Record performance metrics
                if hasattr(result, '__len__'):
                    throughput = len(result) / max(worker.processing_time, 0.001)
                else:
                    throughput = 1.0 / max(worker.processing_time, 0.001)

                self.load_balancer.record_worker_performance(worker_id, throughput)

    async def report_work_failure(self, worker_id: int, morsel_id: int, error: Exception):
        """Report work failure."""
        with self._lock:
            if worker_id in self.active_workers:
                worker = self.active_workers[worker_id]
                worker.error_count += 1
                worker.last_heartbeat = time.time()

                logger.warning(f"Worker {worker_id} failed to process morsel {morsel_id}: {error}")

    def _start_monitoring(self):
        """Start background monitoring thread."""
        if self._monitoring_active:
            return

        self._monitoring_active = True
        self.monitoring_thread = threading.Thread(
            target=self._monitoring_loop,
            name="dbos-parallel-monitor",
            daemon=True
        )
        self.monitoring_thread.start()

    def _stop_monitoring(self):
        """Stop background monitoring."""
        self._monitoring_active = False
        if self.monitoring_thread and self.monitoring_thread.is_alive():
            self.monitoring_thread.join(timeout=1.0)

    def _monitoring_loop(self):
        """Background monitoring loop."""
        while self._monitoring_active:
            try:
                # Update system metrics
                self.system_metrics = self._get_system_metrics()

                # Check worker health
                stale_workers = []
                for worker_id, worker in self.active_workers.items():
                    if not worker.is_healthy:
                        stale_workers.append(worker_id)
                        logger.warning(f"Worker {worker_id} is unhealthy")

                # Remove stale workers
                for worker_id in stale_workers:
                    del self.active_workers[worker_id]

                # Adaptive scaling decisions
                current_workers = len(self.active_workers)
                optimal_workers = self._calculate_optimal_worker_count(
                    len(self.pending_morsels)
                )

                if abs(current_workers - optimal_workers) > 1:
                    logger.info(f"Scaling workers: {current_workers} -> {optimal_workers}")

            except Exception as e:
                logger.error(f"Monitoring error: {e}")

            time.sleep(2.0)  # Monitor every 2 seconds

    def get_performance_stats(self) -> Dict[str, Any]:
        """Get comprehensive performance statistics."""
        with self._lock:
            total_processed = sum(w.processed_count for w in self.active_workers.values())
            total_errors = sum(w.error_count for w in self.active_workers.values())

            worker_stats = {}
            for worker_id, worker in self.active_workers.items():
                worker_stats[worker_id] = {
                    'processed_count': worker.processed_count,
                    'error_count': worker.error_count,
                    'throughput': worker.throughput,
                    'queue_depth': worker.queue_depth,
                    'is_healthy': worker.is_healthy,
                    'load': worker.current_load,
                }

            return {
                'active_workers': len(self.active_workers),
                'max_workers': self.max_workers,
                'total_processed': total_processed,
                'total_errors': total_errors,
                'pending_morsels': len(self.pending_morsels),
                'system_metrics': self.system_metrics,
                'performance_weights': dict(self.load_balancer.performance_weights),
                'worker_stats': worker_stats,
                'target_utilization': self.target_utilization,
            }

class AdaptiveScalingPolicy:
    """Policy for adaptive worker scaling."""

    def __init__(self, min_workers: int = 1, scale_up_threshold: float = 0.7,
                 scale_down_threshold: float = 0.3):
        self.min_workers = min_workers
        self.scale_up_threshold = scale_up_threshold
        self.scale_down_threshold = scale_down_threshold
        self.scaling_history: List[Tuple[int, float]] = []

    def adjust_worker_count(self, base_count: int, system_metrics: Dict[str, Any],
                           current_workers: int) -> int:
        """Adjust worker count based on system conditions."""

        cpu_percent = system_metrics['cpu_percent']
        memory_percent = system_metrics['memory_percent']

        # Scale up if CPU utilization is high
        if cpu_percent > self.scale_up_threshold * 100:
            adjusted = min(base_count + 2, current_workers + 4)
        # Scale down if CPU utilization is low
        elif cpu_percent < self.scale_down_threshold * 100:
            adjusted = max(self.min_workers, current_workers - 1)
        else:
            adjusted = base_count

        # Don't exceed memory limits
        if memory_percent > 90:
            adjusted = max(self.min_workers, adjusted - 2)

        # Record scaling decision
        self.scaling_history.append((adjusted, time.time()))

        # Keep only recent history
        if len(self.scaling_history) > 100:
            self.scaling_history = self.scaling_history[-50:]

        return adjusted

# Convenience functions
def create_dbos_parallel_controller(max_workers: Optional[int] = None,
                                  target_utilization: float = 0.8) -> DBOSParallelController:
    """Create a DBOS parallel controller."""
    if max_workers is None:
        max_workers = min(64, psutil.cpu_count(logical=True) or 8)

    return DBOSParallelController(max_workers, target_utilization)

def get_system_resource_summary() -> Dict[str, Any]:
    """Get a summary of system resources."""
    return {
        'cpu_count': psutil.cpu_count(),
        'cpu_logical_count': psutil.cpu_count(logical=True),
        'memory_total_gb': psutil.virtual_memory().total / (1024**3),
        'memory_available_gb': psutil.virtual_memory().available / (1024**3),
        'cpu_percent': psutil.cpu_percent(interval=0.1),
        'memory_percent': psutil.virtual_memory().percent,
        'load_average': psutil.getloadavg() if hasattr(psutil, 'getloadavg') else None,
    }
