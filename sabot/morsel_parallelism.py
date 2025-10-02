# -*- coding: utf-8 -*-
"""Morsel-Driven Parallelism for Sabot - NUMA-aware query evaluation framework."""

import asyncio
import os
import psutil
from typing import List, Dict, Any, Optional, Callable, TypeVar, Generic, Set, Tuple
from dataclasses import dataclass, field
from enum import Enum
import threading
import time
import heapq
from collections import deque
import logging

logger = logging.getLogger(__name__)

T = TypeVar('T')

class NUMANode(Enum):
    """NUMA node identifiers."""
    NODE_0 = 0
    NODE_1 = 1
    NODE_2 = 2
    NODE_3 = 3

@dataclass
class Morsel(Generic[T]):
    """A morsel - small chunk of data for parallel processing.

    Based on Morsel-Driven Parallelism paper:
    - Small size (fits in L2/L3 cache)
    - Self-contained processing unit
    - NUMA-aware placement
    """

    data: T
    morsel_id: int
    partition_id: int = 0
    numa_node: NUMANode = NUMANode.NODE_0
    priority: int = 0
    created_at: float = field(default_factory=time.time)
    processed_at: Optional[float] = None
    processing_time: Optional[float] = None

    def size_bytes(self) -> int:
        """Estimate memory size of morsel data."""
        if hasattr(self.data, 'nbytes'):
            # Arrow/NumPy array
            return self.data.nbytes
        elif hasattr(self.data, '__sizeof__'):
            return self.data.__sizeof__()
        else:
            return len(str(self.data).encode('utf-8'))

    def mark_processed(self):
        """Mark morsel as processed."""
        self.processed_at = time.time()
        if self.created_at:
            self.processing_time = self.processed_at - self.created_at

@dataclass
class WorkerState:
    """State of a worker in the parallel processing system."""

    worker_id: int
    numa_node: NUMANode
    current_morsel: Optional[Morsel] = None
    processed_count: int = 0
    queue_size: int = 0
    is_idle: bool = True
    last_heartbeat: float = field(default_factory=time.time)

    def update_heartbeat(self):
        """Update worker heartbeat timestamp."""
        self.last_heartbeat = time.time()

    def is_stale(self, timeout: float = 5.0) -> bool:
        """Check if worker is stale (no heartbeat)."""
        return time.time() - self.last_heartbeat > timeout

class WorkStealingQueue:
    """Work-stealing queue for morsel distribution.

    Implements the work-stealing algorithm from the Morsel-Driven paper:
    - Local queue per worker
    - Steal from other workers when local queue empty
    - NUMA-aware stealing (prefer same NUMA node)
    """

    def __init__(self, worker_id: int, numa_node: NUMANode, capacity: int = 1024):
        self.worker_id = worker_id
        self.numa_node = numa_node
        self.capacity = capacity
        self._queue: deque[Morsel] = deque(maxlen=capacity)
        self._lock = asyncio.Lock()

    async def push(self, morsel: Morsel) -> bool:
        """Push morsel to local queue."""
        async with self._lock:
            if len(self._queue) >= self.capacity:
                return False
            self._queue.append(morsel)
            return True

    async def pop(self) -> Optional[Morsel]:
        """Pop morsel from local queue."""
        async with self._lock:
            return self._queue.pop() if self._queue else None

    async def steal(self) -> Optional[Morsel]:
        """Attempt to steal work from this queue (called by other workers)."""
        async with self._lock:
            # Only allow stealing from the opposite end for work-stealing
            if len(self._queue) > 1:
                return self._queue.popleft()
            return None

    def size(self) -> int:
        """Get current queue size."""
        return len(self._queue)

    def is_empty(self) -> bool:
        """Check if queue is empty."""
        return len(self._queue) == 0

class ParallelProcessor:
    """Morsel-driven parallel processor with NUMA awareness.

    Implements the core parallelism framework from the Morsel-Driven paper:
    - Morsel creation from input data
    - NUMA-aware work distribution
    - Work-stealing for load balancing
    - Fan-out/fan-in coordination
    """

    def __init__(self, num_workers: Optional[int] = None, morsel_size_kb: int = 64):
        self.num_workers = num_workers or self._detect_optimal_worker_count()
        self.morsel_size_kb = morsel_size_kb
        self.morsel_size_bytes = morsel_size_kb * 1024

        # Worker management
        self.workers: Dict[int, WorkerState] = {}
        self.worker_queues: Dict[int, WorkStealingQueue] = {}
        self.numa_mapping = self._detect_numa_mapping()

        # Statistics
        self.total_morsels_created = 0
        self.total_morsels_processed = 0
        self.processing_start_time: Optional[float] = None

        # Control
        self._running = False
        self._shutdown_event = asyncio.Event()

        logger.info(f"Initialized ParallelProcessor with {self.num_workers} workers, "
                   f"morsel size: {morsel_size_kb}KB")

    def _detect_optimal_worker_count(self) -> int:
        """Detect optimal worker count based on system resources."""
        cpu_count = os.cpu_count() or 4

        # Use 80% of available CPUs to leave headroom
        optimal = max(1, int(cpu_count * 0.8))

        # Cap at reasonable maximum
        return min(optimal, 64)

    def _detect_numa_mapping(self) -> Dict[int, NUMANode]:
        """Detect NUMA node mapping for workers."""
        mapping = {}

        try:
            # Try to detect NUMA nodes (simplified)
            numa_nodes = len(psutil.cpu_count(logical=False) or 1)

            for worker_id in range(self.num_workers):
                # Simple round-robin NUMA assignment
                numa_node = NUMANode(min(worker_id % numa_nodes, 3))  # Max 4 NUMA nodes
                mapping[worker_id] = numa_node

        except Exception:
            # Fallback: all workers on node 0
            for worker_id in range(self.num_workers):
                mapping[worker_id] = NUMANode.NODE_0

        return mapping

    async def start(self):
        """Start the parallel processor."""
        if self._running:
            return

        self._running = True
        self.processing_start_time = time.time()

        # Initialize workers and queues
        for worker_id in range(self.num_workers):
            numa_node = self.numa_mapping[worker_id]
            self.workers[worker_id] = WorkerState(worker_id, numa_node)
            self.worker_queues[worker_id] = WorkStealingQueue(worker_id, numa_node)

        logger.info(f"Started parallel processor with {self.num_workers} workers")

    async def stop(self):
        """Stop the parallel processor."""
        if not self._running:
            return

        self._running = False
        self._shutdown_event.set()

        # Wait for workers to finish
        await asyncio.sleep(0.1)

        logger.info("Stopped parallel processor")

    def create_morsels(self, data: Any, partition_id: int = 0) -> List[Morsel]:
        """Create morsels from input data.

        Splits large datasets into small, cache-friendly morsels.
        """
        morsels = []

        if hasattr(data, 'to_batches'):  # Arrow Table
            # Split Arrow table into smaller batches
            for batch in data.to_batches(max_chunksize=self.morsel_size_bytes // 1024):
                morsel = Morsel(
                    data=batch,
                    morsel_id=self.total_morsels_created,
                    partition_id=partition_id,
                    numa_node=self._get_preferred_numa_node()
                )
                morsels.append(morsel)
                self.total_morsels_created += 1

        elif hasattr(data, '__iter__') and not isinstance(data, (str, bytes)):
            # Split iterable data
            chunk = []
            current_size = 0

            for item in data:
                item_size = len(str(item).encode('utf-8'))
                if current_size + item_size > self.morsel_size_bytes and chunk:
                    # Create morsel from current chunk
                    morsel = Morsel(
                        data=chunk,
                        morsel_id=self.total_morsels_created,
                        partition_id=partition_id,
                        numa_node=self._get_preferred_numa_node()
                    )
                    morsels.append(morsel)
                    self.total_morsels_created += 1

                    chunk = []
                    current_size = 0

                chunk.append(item)
                current_size += item_size

            # Add remaining chunk
            if chunk:
                morsel = Morsel(
                    data=chunk,
                    morsel_id=self.total_morsels_created,
                    partition_id=partition_id,
                    numa_node=self._get_preferred_numa_node()
                )
                morsels.append(morsel)
                self.total_morsels_created += 1

        else:
            # Single item as one morsel
            morsel = Morsel(
                data=data,
                morsel_id=self.total_morsels_created,
                partition_id=partition_id,
                numa_node=self._get_preferred_numa_node()
            )
            morsels.append(morsel)
            self.total_morsels_created += 1

        return morsels

    def _get_preferred_numa_node(self) -> NUMANode:
        """Get preferred NUMA node for morsel placement."""
        # Simple strategy: round-robin across available nodes
        available_nodes = set(self.numa_mapping.values())
        if not available_nodes:
            return NUMANode.NODE_0

        # Use current morsel count to distribute
        node_index = self.total_morsels_created % len(available_nodes)
        return list(available_nodes)[node_index]

    async def submit_morsels(self, morsels: List[Morsel]) -> List[asyncio.Task]:
        """Submit morsels for parallel processing."""
        if not self._running:
            await self.start()

        tasks = []

        # Distribute morsels to workers using work-stealing queue placement
        for morsel in morsels:
            # Find best worker for this morsel (NUMA-aware)
            worker_id = self._select_worker_for_morsel(morsel)

            # Submit to worker's queue
            queue = self.worker_queues[worker_id]
            success = await queue.push(morsel)

            if success:
                self.workers[worker_id].queue_size += 1
            else:
                # Queue full, find another worker
                logger.warning(f"Worker {worker_id} queue full, redistributing morsel")

        return tasks

    def _select_worker_for_morsel(self, morsel: Morsel) -> int:
        """Select best worker for morsel (NUMA-aware scheduling)."""
        # Prefer workers on same NUMA node
        same_node_workers = [
            wid for wid, node in self.numa_mapping.items()
            if node == morsel.numa_node
        ]

        if same_node_workers:
            # Choose least loaded worker on same NUMA node
            return min(same_node_workers,
                      key=lambda wid: self.workers[wid].queue_size)

        # Fallback: choose least loaded worker overall
        return min(range(self.num_workers),
                  key=lambda wid: self.workers[wid].queue_size)

    async def process_with_function(
        self,
        data: Any,
        processor_func: Callable[[Morsel], Any],
        partition_id: int = 0
    ) -> List[Any]:
        """Process data using morsel-driven parallelism."""
        if not self._running:
            await self.start()

        # Create morsels from input data
        morsels = self.create_morsels(data, partition_id)

        if not morsels:
            return []

        # Submit morsels for processing
        await self.submit_morsels(morsels)

        # Process morsels in parallel
        results = []
        semaphore = asyncio.Semaphore(self.num_workers)

        async def process_single_morsel(morsel: Morsel):
            async with semaphore:
                try:
                    result = await processor_func(morsel)
                    morsel.mark_processed()
                    self.total_morsels_processed += 1
                    return result
                except Exception as e:
                    logger.error(f"Error processing morsel {morsel.morsel_id}: {e}")
                    return None

        # Create tasks for all morsels
        tasks = [process_single_morsel(morsel) for morsel in morsels]

        # Wait for all to complete
        completed_results = await asyncio.gather(*tasks, return_exceptions=True)

        # Filter out exceptions and None results
        results = [r for r in completed_results if r is not None and not isinstance(r, Exception)]

        return results

    def get_stats(self) -> Dict[str, Any]:
        """Get processing statistics."""
        total_time = time.time() - (self.processing_start_time or time.time())

        return {
            'num_workers': self.num_workers,
            'morsel_size_kb': self.morsel_size_kb,
            'total_morsels_created': self.total_morsels_created,
            'total_morsels_processed': self.total_morsels_processed,
            'processing_time_seconds': total_time,
            'morsels_per_second': self.total_morsels_processed / max(total_time, 0.001),
            'worker_stats': {
                wid: {
                    'numa_node': worker.numa_node.value,
                    'processed_count': worker.processed_count,
                    'queue_size': worker.queue_size,
                    'is_idle': worker.is_idle
                }
                for wid, worker in self.workers.items()
            },
            'numa_mapping': {wid: node.value for wid, node in self.numa_mapping.items()}
        }

class FanOutOperator:
    """Fan-out operator for parallel stream processing.

    Splits a single input stream into multiple parallel processing branches.
    Each branch processes morsels independently.
    """

    def __init__(self, num_branches: int, processor: ParallelProcessor):
        self.num_branches = num_branches
        self.processor = processor
        self.output_queues: List[asyncio.Queue] = []

        # Create output queues for each branch
        for _ in range(num_branches):
            self.output_queues.append(asyncio.Queue())

    async def process_stream(self, input_stream: asyncio.Queue) -> List[asyncio.Queue]:
        """Process input stream and fan out to multiple branches."""

        async def fan_out_worker(branch_id: int, output_queue: asyncio.Queue):
            """Worker for a single branch."""
            while True:
                try:
                    # Get item from input stream
                    item = await input_stream.get()

                    # Create morsels from item
                    morsels = self.processor.create_morsels(item, branch_id)

                    # Distribute morsels to this branch
                    for morsel in morsels:
                        await output_queue.put(morsel)

                    input_stream.task_done()

                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error(f"Fan-out worker {branch_id} error: {e}")

        # Start fan-out workers
        workers = []
        for branch_id in range(self.num_branches):
            worker = asyncio.create_task(
                fan_out_worker(branch_id, self.output_queues[branch_id])
            )
            workers.append(worker)

        # Return output queues for downstream processing
        return self.output_queues

class FanInOperator:
    """Fan-in operator for merging parallel processing results.

    Merges multiple parallel processing branches back into a single output stream.
    """

    def __init__(self, num_inputs: int):
        self.num_inputs = num_inputs
        self.output_queue = asyncio.Queue()

    async def merge_streams(self, input_queues: List[asyncio.Queue]) -> asyncio.Queue:
        """Merge multiple input streams into single output stream."""

        async def merge_worker(input_queue: asyncio.Queue):
            """Worker to drain one input queue."""
            while True:
                try:
                    item = await input_queue.get()
                    await self.output_queue.put(item)
                    input_queue.task_done()
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error(f"Fan-in merge error: {e}")

        # Start merge workers
        workers = []
        for input_queue in input_queues:
            worker = asyncio.create_task(merge_worker(input_queue))
            workers.append(worker)

        return self.output_queue

# Convenience functions for easy usage

def create_parallel_processor(num_workers: Optional[int] = None,
                            morsel_size_kb: int = 64) -> ParallelProcessor:
    """Create a parallel processor with optimal settings."""
    return ParallelProcessor(num_workers, morsel_size_kb)

async def parallel_process_data(data: Any,
                               processor_func: Callable[[Morsel], Any],
                               num_workers: Optional[int] = None,
                               morsel_size_kb: int = 64) -> List[Any]:
    """Convenience function for parallel data processing."""
    processor = ParallelProcessor(num_workers, morsel_size_kb)
    await processor.start()

    try:
        results = await processor.process_with_function(data, processor_func)
        return results
    finally:
        await processor.stop()

async def create_fan_out_fan_in_pipeline(
    input_stream: asyncio.Queue,
    processor_func: Callable[[Morsel], Any],
    num_branches: int = 4,
    num_workers: Optional[int] = None
) -> asyncio.Queue:
    """Create a complete fan-out/fan-in processing pipeline."""

    # Create parallel processor
    processor = ParallelProcessor(num_workers)

    # Create fan-out operator
    fan_out = FanOutOperator(num_branches, processor)

    # Create fan-in operator
    fan_in = FanInOperator(num_branches)

    # Connect the pipeline
    parallel_queues = await fan_out.process_stream(input_stream)
    output_stream = await fan_in.merge_streams(parallel_queues)

    return output_stream
