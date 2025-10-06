# cython: language_level=3, boundscheck=False, wraparound=False
"""
Morsel-Driven Shuffle Implementation

Pipelined shuffle using work-stealing morsel parallelism.
No barriers - streaming shuffle with low latency.
"""

import cython
from libc.stdint cimport int32_t, int64_t
from libcpp cimport bool as cbool
from libcpp.vector cimport vector
from libcpp.string cimport string
import threading
import asyncio
import logging

from sabot._cython.shuffle.lock_free_queue cimport MPSCQueue
from sabot._cython.shuffle.shuffle_transport cimport ShuffleClient
from sabot.morsel_parallelism import Morsel

logger = logging.getLogger(__name__)


cdef class MorselShuffleManager:
    """
    Manages morsel-driven shuffle execution.

    Architecture:
    - One MPSC queue per destination partition
    - Pool of worker threads pulling from queues
    - Each worker sends morsels via ShuffleClient
    - Work-stealing for load balancing
    """

    def __cinit__(self, int32_t num_partitions, int32_t num_workers=4,
                  int64_t morsel_size_kb=64):
        """
        Initialize morsel shuffle manager.

        Args:
            num_partitions: Number of downstream partitions
            num_workers: Number of worker threads
            morsel_size_kb: Size of each morsel in KB
        """
        self._num_partitions = num_partitions
        self._num_workers = num_workers
        self._morsel_size_kb = morsel_size_kb
        self._running = False
        self._client = None

        # Initialize queues
        self._partition_queues.reserve(num_partitions)
        for i in range(num_partitions):
            queue = MPSCQueue(capacity=1024)  # Configurable capacity
            self._partition_queues.push_back(queue)

        # Worker threads (will be started later)
        self._worker_threads = []

    def __dealloc__(self):
        """Clean up resources."""
        self.stop()

    cpdef void start(self):
        """
        Start worker threads.

        Creates worker threads that pull from partition queues
        and send morsels via ShuffleClient.
        """
        if self._running:
            return

        self._running = True
        logger.info(f"Starting {self._num_workers} morsel shuffle workers")

        # Initialize ShuffleClient if not already done
        if self._client is None:
            self._client = ShuffleClient()

        # Start worker threads
        self._worker_threads = []
        for i in range(self._num_workers):
            thread = threading.Thread(
                target=self._worker_loop,
                args=(i,),
                name=f"MorselShuffleWorker-{i}",
                daemon=True
            )
            thread.start()
            self._worker_threads.append(thread)

        logger.info(f"Morsel shuffle manager started with {self._num_workers} workers")

    cpdef void stop(self):
        """
        Stop worker threads and clean up resources.
        """
        if not self._running:
            return

        self._running = False
        logger.info("Stopping morsel shuffle workers")

        # Signal workers to stop (they check self._running)
        # Wait for threads to finish
        for thread in self._worker_threads:
            if thread.is_alive():
                thread.join(timeout=5.0)

        # Clean up queues
        self._partition_queues.clear()
        self._worker_threads.clear()

        # Clean up client
        if self._client is not None:
            self._client.close()
            self._client = None

        logger.info("Morsel shuffle manager stopped")

    cpdef void enqueue_morsel(self, int32_t partition_id, object morsel):
        """
        Enqueue morsel for shuffling.

        Args:
            partition_id: Target partition (0 to num_partitions-1)
            morsel: Morsel to shuffle (RecordBatch)
        """
        if partition_id < 0 or partition_id >= self._num_partitions:
            raise ValueError(f"Invalid partition_id: {partition_id}")

        # Get the queue for this partition
        cdef MPSCQueue queue = self._partition_queues[partition_id]

        # Wrap morsel in shuffle envelope
        shuffle_envelope = {
            'partition_id': partition_id,
            'morsel': morsel,
            'shuffle_id': None  # Will be set by transport
        }

        # Enqueue (lock-free MPSC)
        # TODO: Convert to C++ types for zero-copy
        # TODO: Fix push_mpsc call - needs shuffle_id_hash, partition_id, batch (not envelope)
        # queue.push_mpsc(shuffle_envelope)
        pass  # Placeholder until proper conversion is implemented

    def _worker_loop(self, int32_t worker_id):
        """
        Worker thread main loop.

        Pulls morsels from queues and sends via ShuffleClient.
        Uses work-stealing for load balancing.

        Args:
            worker_id: ID of this worker thread
        """
        cdef int32_t partition_id
        cdef MPSCQueue queue
        cdef object envelope
        cdef object morsel

        logger.debug(f"Worker {worker_id} starting")

        while self._running:
            # Work-stealing: try queues in round-robin order
            # Start from worker_id to distribute load
            found_work = False

            for offset in range(self._num_partitions):
                partition_id = (worker_id + offset) % self._num_partitions
                queue = self._partition_queues[partition_id]

                # Try to pop from this queue (non-blocking)
                envelope = queue.try_pop_mpsc()
                if envelope is not None:
                    # Got work! Process it
                    found_work = True
                    try:
                        self._send_morsel(envelope)
                    except Exception as e:
                        logger.error(f"Worker {worker_id} failed to send morsel: {e}")
                    break

            if not found_work:
                # No work available, sleep briefly to avoid busy-waiting
                import time
                time.sleep(0.001)  # 1ms

        logger.debug(f"Worker {worker_id} stopping")

    def _send_morsel(self, object envelope):
        """
        Send a morsel via ShuffleClient.

        Args:
            envelope: Shuffle envelope with partition_id, morsel, shuffle_id
        """
        cdef int32_t partition_id = envelope['partition_id']
        cdef object morsel = envelope['morsel']
        cdef bytes shuffle_id = envelope.get('shuffle_id')

        if shuffle_id is None:
            raise ValueError("Shuffle ID not set in envelope")

        # Convert morsel to Arrow Flight format
        # TODO: Zero-copy conversion to Flight data
        flight_data = self._morsel_to_flight_data(morsel)

        # Send via ShuffleClient
        # TODO: Implement ShuffleClient.send_morsel()
        # self._client.send_morsel(shuffle_id, partition_id, flight_data)

    def _morsel_to_flight_data(self, object morsel):
        """
        Convert RecordBatch morsel to Arrow Flight data.

        Args:
            morsel: RecordBatch or Morsel object

        Returns:
            Arrow Flight data structure
        """
        # TODO: Implement conversion to Flight format
        # For now, return the morsel as-is
        return morsel

    cpdef object get_stats(self):
        """
        Get shuffle statistics.

        Returns:
            Dict with queue depths, throughput, etc.
        """
        stats = {
            'num_partitions': self._num_partitions,
            'num_workers': self._num_workers,
            'running': self._running,
            'morsel_size_kb': self._morsel_size_kb,
            'queue_depths': [],
            'total_enqueued': 0,
            'total_sent': 0
        }

        # Get queue depths
        for i in range(self._num_partitions):
            queue = self._partition_queues[i]
            # TODO: Add queue.size() method
            stats['queue_depths'].append(0)  # Placeholder

        return stats
