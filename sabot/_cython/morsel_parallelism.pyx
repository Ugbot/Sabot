# -*- coding: utf-8 -*-
"""Cython-optimized Morsel-Driven Parallelism with DBOS control."""

import asyncio
import time
import threading
from typing import Optional, Dict, Any, List, Callable
from libc.stdlib cimport malloc, free, realloc
from libc.string cimport memcpy, memset
from cpython.ref cimport Py_INCREF, Py_DECREF
from cpython cimport PyObject
from cython.parallel import prange, parallel
from concurrent.futures import ThreadPoolExecutor
import logging

logger = logging.getLogger(__name__)

# Constants for performance
DEF DEFAULT_MORSEL_SIZE = 65536  # 64KB
DEF MAX_WORKERS = 256
DEF WORK_STEALING_THRESHOLD = 4
DEF BATCH_SIZE = 64

# Thread-safe counters
cdef class AtomicCounter:
    """Thread-safe counter using atomic operations."""

    cdef:
        long long _value
        object _lock

    def __cinit__(self):
        self._value = 0
        self._lock = threading.Lock()

    cdef inline void increment(self) nogil:
        """Atomically increment counter."""
        with gil:
            with self._lock:
                self._value += 1

    cdef inline void decrement(self) nogil:
        """Atomically decrement counter."""
        with gil:
            with self._lock:
                self._value -= 1

    cdef inline long long get_value(self) nogil:
        """Get current value."""
        return self._value

    cdef inline void set_value(self, long long value) nogil:
        """Set value."""
        with gil:
            with self._lock:
                self._value = value

cdef class FastMorsel:
    """Cython-optimized morsel with zero-copy operations."""

    cdef:
        object _data
        long long _morsel_id
        int _partition_id
        int _priority
        double _created_at
        double _processed_at
        double _processing_time
        bint _processed
        object _metadata

    def __cinit__(self, object data, long long morsel_id, int partition_id=0, int priority=0):
        self._data = data
        self._morsel_id = morsel_id
        self._partition_id = partition_id
        self._priority = priority
        self._created_at = time.time()
        self._processed_at = 0.0
        self._processing_time = 0.0
        self._processed = False
        self._metadata = {}

        # Keep reference to data
        Py_INCREF(data)

    def __dealloc__(self):
        if self._data is not None:
            Py_DECREF(self._data)

    cdef inline void mark_processed(self) nogil:
        """Mark morsel as processed."""
        with gil:
            self._processed_at = time.time()
            self._processed = True
            if self._created_at > 0:
                self._processing_time = self._processed_at - self._created_at

    cdef inline bint is_processed(self) nogil:
        """Check if morsel is processed."""
        return self._processed

    cdef inline long long size_bytes(self) nogil:
        """Estimate memory size."""
        with gil:
            if hasattr(self._data, 'nbytes'):
                return (<object>self._data).nbytes
            elif hasattr(self._data, '__sizeof__'):
                return (<object>self._data).__sizeof__()
            else:
                return len(str(self._data).encode('utf-8'))

    # Property accessors
    @property
    def data(self):
        return self._data

    @property
    def morsel_id(self):
        return self._morsel_id

    @property
    def partition_id(self):
        return self._partition_id

    @property
    def priority(self):
        return self._priority

    @property
    def created_at(self):
        return self._created_at

    @property
    def processed_at(self):
        return self._processed_at

    @property
    def processing_time(self):
        return self._processing_time

    @property
    def processed(self):
        return self._processed

    @property
    def metadata(self):
        return self._metadata

cdef class FastWorkStealingQueue:
    """Cython-optimized work-stealing queue."""

    cdef:
        FastMorsel** _buffer
        long long _capacity
        long long _size
        long long _head
        long long _tail
        object _lock

    def __cinit__(self, long long capacity=1024):
        self._capacity = capacity
        self._size = 0
        self._head = 0
        self._tail = 0
        self._lock = threading.RLock()

        # Allocate buffer
        self._buffer = <FastMorsel**>malloc(capacity * sizeof(FastMorsel*))
        if self._buffer == NULL:
            raise MemoryError("Failed to allocate work queue buffer")

        # Initialize to NULL
        cdef long long i
        for i in range(capacity):
            self._buffer[i] = NULL

    def __dealloc__(self):
        if self._buffer != NULL:
            free(self._buffer)

    cdef inline bint _ensure_capacity(self, long long required_capacity) nogil:
        """Ensure queue has enough capacity."""
        if required_capacity <= self._capacity:
            return True

        cdef long long new_capacity = max(required_capacity, self._capacity * 2)
        cdef FastMorsel** new_buffer = <FastMorsel**>realloc(
            self._buffer, new_capacity * sizeof(FastMorsel*)
        )

        if new_buffer == NULL:
            return False

        # Initialize new slots
        cdef long long i
        for i in range(self._capacity, new_capacity):
            new_buffer[i] = NULL

        self._buffer = new_buffer
        self._capacity = new_capacity
        return True

    cdef inline bint push(self, FastMorsel* morsel) nogil:
        """Push morsel to queue."""
        with gil:
            with self._lock:
                if not self._ensure_capacity(self._size + 1):
                    return False

                self._buffer[self._tail] = morsel
                self._tail = (self._tail + 1) % self._capacity
                self._size += 1
                Py_INCREF(morsel)  # Keep reference
                return True

    cdef inline FastMorsel* pop(self) nogil:
        """Pop morsel from queue."""
        cdef FastMorsel* result = NULL

        with gil:
            with self._lock:
                if self._size == 0:
                    return NULL

                result = self._buffer[self._head]
                self._buffer[self._head] = NULL
                self._head = (self._head + 1) % self._capacity
                self._size -= 1

        return result

    cdef inline FastMorsel* steal(self) nogil:
        """Attempt to steal work from opposite end."""
        cdef FastMorsel* result = NULL

        with gil:
            with self._lock:
                if self._size <= 1:  # Don't steal if queue is too small
                    return NULL

                # Steal from head (opposite of pop)
                result = self._buffer[self._head]
                self._buffer[self._head] = NULL
                self._head = (self._head + 1) % self._capacity
                self._size -= 1

        return result

    cdef inline long long size(self) nogil:
        """Get current queue size."""
        return self._size

    cdef inline bint is_empty(self) nogil:
        """Check if queue is empty."""
        return self._size == 0

cdef class FastWorker:
    """Cython-optimized worker with DBOS-controlled execution."""

    cdef:
        int _worker_id
        FastWorkStealingQueue* _local_queue
        object _dbos_controller
        AtomicCounter* _processed_count
        double _last_heartbeat
        bint _is_running
        object _executor
        object _loop

    def __cinit__(self, int worker_id, object dbos_controller, object loop=None):
        self._worker_id = worker_id
        self._dbos_controller = dbos_controller
        self._local_queue = new FastWorkStealingQueue(1024)
        self._processed_count = new AtomicCounter()
        self._last_heartbeat = time.time()
        self._is_running = False
        self._loop = loop or asyncio.get_event_loop()
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix=f"worker-{worker_id}")

    def __dealloc__(self):
        if self._local_queue != NULL:
            del self._local_queue
        if self._processed_count != NULL:
            del self._processed_count

    cdef inline void update_heartbeat(self):
        """Update worker heartbeat."""
        self._last_heartbeat = time.time()

    cdef inline bint is_stale(self, double timeout=5.0):
        """Check if worker is stale."""
        return (time.time() - self._last_heartbeat) > timeout

    cdef inline long long get_processed_count(self):
        """Get number of items processed."""
        return self._processed_count.get_value()

    async def submit_morsel(self, FastMorsel morsel):
        """Submit morsel for processing."""
        if not self._local_queue.push(&morsel):
            logger.warning(f"Worker {self._worker_id} queue full, morsel dropped")

    async def process_queue(self, object processor_func):
        """Process items in queue using DBOS-controlled execution."""
        cdef FastMorsel* morsel

        if self._is_running:
            return

        self._is_running = True

        try:
            while self._is_running:
                # Get work from local queue
                morsel = self._local_queue.pop()

                if morsel == NULL:
                    # Try work stealing from DBOS coordinator
                    morsel = await self._steal_work_from_dbos()

                if morsel != NULL:
                    # Process morsel
                    await self._process_morsel(morsel, processor_func)
                    self._processed_count.increment()
                    morsel.mark_processed()
                else:
                    # No work available, wait a bit
                    await asyncio.sleep(0.001)

        except Exception as e:
            logger.error(f"Worker {self._worker_id} error: {e}")
        finally:
            self._is_running = False

    async def _steal_work_from_dbos(self):
        """Request work from DBOS coordinator."""
        try:
            # Ask DBOS for work assignment
            if hasattr(self._dbos_controller, 'assign_work_to_worker'):
                return await self._dbos_controller.assign_work_to_worker(self._worker_id)
        except Exception as e:
            logger.debug(f"DBOS work stealing failed: {e}")
        return None

    async def _process_morsel(self, FastMorsel* morsel, object processor_func):
        """Process a single morsel."""
        try:
            # Call processing function
            result = await processor_func(morsel)

            # Notify DBOS of completion
            if hasattr(self._dbos_controller, 'report_work_completion'):
                await self._dbos_controller.report_work_completion(
                    self._worker_id, morsel.morsel_id, result
                )

        except Exception as e:
            # Notify DBOS of failure
            if hasattr(self._dbos_controller, 'report_work_failure'):
                await self._dbos_controller.report_work_failure(
                    self._worker_id, morsel.morsel_id, e
                )
            logger.error(f"Morsel processing error: {e}")

    async def stop(self):
        """Stop the worker."""
        self._is_running = False
        self._executor.shutdown(wait=True)

cdef class FastParallelProcessor:
    """Cython-optimized parallel processor controlled by DBOS."""

    cdef:
        int _num_workers
        long long _morsel_size_bytes
        object _dbos_controller
        dict _workers
        AtomicCounter* _total_processed
        double _start_time
        bint _is_running
        object _morsel_factory

    def __cinit__(self, int num_workers, long long morsel_size_kb, object dbos_controller):
        self._num_workers = num_workers
        self._morsel_size_bytes = morsel_size_kb * 1024
        self._dbos_controller = dbos_controller
        self._workers = {}
        self._total_processed = new AtomicCounter()
        self._start_time = time.time()
        self._is_running = False

    def __dealloc__(self):
        if self._total_processed != NULL:
            del self._total_processed

    async def start(self):
        """Start the parallel processor with DBOS coordination."""
        if self._is_running:
            return

        self._is_running = True
        self._start_time = time.time()

        # Initialize workers under DBOS control
        for worker_id in range(self._num_workers):
            worker = FastWorker(worker_id, self._dbos_controller)
            self._workers[worker_id] = worker

        # Notify DBOS that processor is ready
        if hasattr(self._dbos_controller, 'processor_started'):
            await self._dbos_controller.processor_started(self._num_workers)

        logger.info(f"Started Cython parallel processor with {self._num_workers} workers")

    async def stop(self):
        """Stop the parallel processor."""
        if not self._is_running:
            return

        self._is_running = False

        # Stop all workers
        stop_tasks = []
        for worker in self._workers.values():
            stop_tasks.append(worker.stop())

        await asyncio.gather(*stop_tasks, return_exceptions=True)

        # Notify DBOS
        if hasattr(self._dbos_controller, 'processor_stopped'):
            await self._dbos_controller.processor_stopped()

        logger.info("Stopped Cython parallel processor")

    cdef list _create_morsels(self, object data, int partition_id):
        """Create optimized morsels from input data."""
        cdef list morsels = []
        cdef long long total_size = 0
        cdef long long morsel_count = 0
        cdef list current_chunk = []

        # Handle different data types
        if hasattr(data, 'to_batches'):  # Arrow Table
            # Process Arrow table in batches
            for batch in data.to_batches(max_chunksize=self._morsel_size_bytes):
                morsel = FastMorsel(batch, morsel_count, partition_id)
                morsels.append(morsel)
                morsel_count += 1

        elif hasattr(data, '__iter__') and not isinstance(data, (str, bytes)):
            # Process iterable data
            for item in data:
                item_size = len(str(item).encode('utf-8'))
                current_chunk.append(item)
                total_size += item_size

                if total_size >= self._morsel_size_bytes:
                    # Create morsel from current chunk
                    morsel = FastMorsel(current_chunk, morsel_count, partition_id)
                    morsels.append(morsel)
                    morsel_count += 1
                    current_chunk = []
                    total_size = 0

            # Add remaining chunk
            if current_chunk:
                morsel = FastMorsel(current_chunk, morsel_count, partition_id)
                morsels.append(morsel)

        else:
            # Single item
            morsel = FastMorsel(data, morsel_count, partition_id)
            morsels.append(morsel)

        return morsels

    async def process_data(self, object data, object processor_func, int partition_id=0):
        """Process data using DBOS-controlled parallel execution."""
        if not self._is_running:
            await self.start()

        # Create morsels
        cdef list morsels = self._create_morsels(data, partition_id)

        # Ask DBOS how to distribute work
        work_distribution = await self._get_dbos_work_distribution(morsels)

        # Submit morsels to workers according to DBOS plan
        submit_tasks = []
        for worker_id, worker_morsels in work_distribution.items():
            if worker_id in self._workers:
                worker = self._workers[worker_id]
                for morsel in worker_morsels:
                    submit_tasks.append(worker.submit_morsel(morsel))

        # Start processing
        process_tasks = []
        for worker in self._workers.values():
            process_tasks.append(worker.process_queue(processor_func))

        # Wait for all processing to complete
        await asyncio.gather(*submit_tasks, return_exceptions=True)
        await asyncio.gather(*process_tasks, return_exceptions=True)

        return morsels

    async def _get_dbos_work_distribution(self, list morsels):
        """Get work distribution plan from DBOS."""
        try:
            if hasattr(self._dbos_controller, 'plan_work_distribution'):
                return await self._dbos_controller.plan_work_distribution(
                    morsels, self._num_workers
                )
        except Exception as e:
            logger.debug(f"DBOS work distribution failed: {e}")

        # Fallback: round-robin distribution
        distribution = {}
        for i, worker_id in enumerate(self._workers.keys()):
            distribution[worker_id] = []

        for i, morsel in enumerate(morsels):
            worker_id = list(self._workers.keys())[i % self._num_workers]
            distribution[worker_id].append(morsel)

        return distribution

    def get_stats(self):
        """Get processing statistics."""
        cdef double total_time
        cdef long long total_processed

        total_time = time.time() - self._start_time
        total_processed = self._total_processed.get_value()

        worker_stats = {}
        for worker_id, worker in self._workers.items():
            worker_stats[worker_id] = {
                'processed_count': worker.get_processed_count(),
                'is_stale': worker.is_stale(),
                'last_heartbeat': worker._last_heartbeat,
            }

        return {
            'num_workers': self._num_workers,
            'morsel_size_kb': self._morsel_size_bytes // 1024,
            'total_processed': total_processed,
            'processing_time_seconds': total_time,
            'throughput_items_per_second': total_processed / max(total_time, 0.001),
            'is_running': self._is_running,
            'worker_stats': worker_stats,
        }

# Python interface functions
def create_fast_parallel_processor(int num_workers, int morsel_size_kb, object dbos_controller):
    """Create a FastParallelProcessor instance."""
    return FastParallelProcessor(num_workers, morsel_size_kb, dbos_controller)

def create_fast_morsel(object data, long long morsel_id, int partition_id=0, int priority=0):
    """Create a FastMorsel instance."""
    return FastMorsel(data, morsel_id, partition_id, priority)

# Performance monitoring
def get_parallel_processor_stats(object processor):
    """Get detailed statistics for a FastParallelProcessor."""
    if isinstance(processor, FastParallelProcessor):
        return processor.get_stats()
    else:
        return {'type': 'python_processor', 'stats_unavailable': True}
