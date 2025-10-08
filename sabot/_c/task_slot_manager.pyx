# cython: language_level=3, boundscheck=False, wraparound=False, cdivision=True, nonecheck=False
"""
Cython Wrapper for C++ TaskSlotManager

Provides Python interface to dynamic task slot management with work stealing.
"""

from libc.stdint cimport int32_t, int64_t
from libcpp cimport bool as cbool
from libcpp.vector cimport vector
from libcpp.memory cimport shared_ptr
from libcpp.functional cimport function

# Import Arrow C++ types
from pyarrow.includes.libarrow cimport CRecordBatch as PCRecordBatch
from pyarrow.lib cimport pyarrow_unwrap_batch, pyarrow_wrap_batch
cimport pyarrow.lib as pa

cimport cython


# ============================================================================
# C++ Declarations
# ============================================================================

cdef extern from "task_slot_manager.hpp" namespace "sabot" nogil:
    # Morsel source enum
    cdef enum class CMorselSource "sabot::MorselSource":
        LOCAL
        NETWORK

    # Morsel struct
    cdef cppclass CMorsel "sabot::Morsel":
        shared_ptr[PCRecordBatch] batch
        int64_t start_row
        int64_t num_rows
        int partition_id
        CMorselSource source

    # Morsel result
    cdef cppclass CMorselResult "sabot::MorselResult":
        shared_ptr[PCRecordBatch] batch
        int morsel_id
        cbool success

    # Slot statistics
    cdef cppclass CSlotStats "sabot::SlotStats":
        int slot_id
        cbool busy
        int64_t morsels_processed
        int64_t total_processing_time_us
        int64_t local_morsels
        int64_t network_morsels

    # C++ callback type (workaround for Cython's std::function limitation)
    ctypedef shared_ptr[PCRecordBatch] (*CMorselProcessorFunc)(shared_ptr[PCRecordBatch]) noexcept nogil

    # TaskSlotManager
    cdef cppclass CTaskSlotManager "sabot::TaskSlotManager":
        CTaskSlotManager(int num_slots) except +

        vector[CMorselResult] ExecuteMorselsWithCallback(
            vector[CMorsel] morsels,
            CMorselProcessorFunc processor
        ) nogil

        cbool EnqueueMorsel(const CMorsel& morsel) nogil

        void AddSlots(int count) nogil
        void RemoveSlots(int count) nogil
        int GetNumSlots() nogil
        int GetAvailableSlots() nogil
        int64_t GetQueueDepth() nogil
        vector[CSlotStats] GetSlotStats() nogil
        void Shutdown() nogil


# ============================================================================
# Callback Bridge (Python → C++)
# ============================================================================

# Global callback storage
cdef object _current_python_callback = None

cdef shared_ptr[PCRecordBatch] cpp_callback_wrapper(shared_ptr[PCRecordBatch] batch_cpp) noexcept nogil:
    """Bridge C++ → Python callback."""
    with gil:
        py_batch = pyarrow_wrap_batch(batch_cpp)

        if _current_python_callback is not None:
            try:
                result_py = _current_python_callback(py_batch)
                if result_py is not None:
                    return pyarrow_unwrap_batch(result_py)
            except:
                pass

        return shared_ptr[PCRecordBatch]()


# ============================================================================
# Python Wrapper Classes
# ============================================================================

cdef class TaskSlotManager:
    """
    Dynamic Task Slot Manager - Elastic worker pool for morsel execution.

    Features:
    - Work-stealing: Idle slots steal from global queue
    - Elastic scaling: Add/remove slots dynamically
    - Unified processing: Local + network morsels use same slots
    - Lock-free: All operations use atomic queues

    Usage:
        # Create manager with 8 slots
        manager = TaskSlotManager(num_slots=8)

        # Execute morsels
        results = manager.execute_morsels(morsels, processor_func)

        # Elastically scale
        manager.add_slots(4)  # Now have 12 slots

        # Enqueue network morsel asynchronously
        manager.enqueue_morsel(network_morsel)

        # Get stats
        stats = manager.get_slot_stats()
    """

    cdef CTaskSlotManager* _manager

    def __cinit__(self, int num_slots=0):
        """
        Create task slot manager.

        Args:
            num_slots: Number of worker slots (0 = auto-detect)
        """
        if num_slots == 0:
            import os
            num_slots = os.cpu_count() or 4

        self._manager = new CTaskSlotManager(num_slots)

    def __dealloc__(self):
        """Clean up C++ manager."""
        if self._manager != NULL:
            self._manager.Shutdown()
            del self._manager

    def execute_morsels(self, list morsels, object processor_func):
        """
        Execute morsels using task slots.

        Process:
        1. Enqueue all morsels to global queue (lock-free)
        2. Worker slots pull morsels and process in parallel
        3. Results collected and returned in order

        Args:
            morsels: List of (batch, start_row, num_rows, partition_id) tuples
            processor_func: Python callback (batch → batch)

        Returns:
            List of result batches (in morsel order)
        """
        global _current_python_callback

        # Convert Python morsels to C++ morsels
        cdef vector[CMorsel] cpp_morsels
        cdef CMorsel cpp_morsel

        for i, morsel_data in enumerate(morsels):
            batch, start_row, num_rows, partition_id = morsel_data

            cpp_morsel.batch = pyarrow_unwrap_batch(batch)
            cpp_morsel.start_row = start_row
            cpp_morsel.num_rows = num_rows
            cpp_morsel.partition_id = partition_id
            cpp_morsel.source = CMorselSource.LOCAL

            cpp_morsels.push_back(cpp_morsel)

        # Store callback
        _current_python_callback = processor_func

        # Execute in C++ (releases GIL!)
        cdef vector[CMorselResult] results_cpp
        with nogil:
            results_cpp = self._manager.ExecuteMorselsWithCallback(cpp_morsels, cpp_callback_wrapper)

        # Clear callback
        _current_python_callback = None

        # Convert C++ results to Python
        results = []
        for i in range(results_cpp.size()):
            if results_cpp[i].success and results_cpp[i].batch:
                py_batch = pyarrow_wrap_batch(results_cpp[i].batch)
                results.append(py_batch)

        return results

    def enqueue_morsel(self, object batch, int64_t start_row, int64_t num_rows,
                       int partition_id, str source='local'):
        """
        Enqueue single morsel (for network morsels arriving async).

        Args:
            batch: RecordBatch
            start_row: Start row index
            num_rows: Number of rows in morsel
            partition_id: Partition ID
            source: 'local' or 'network'

        Returns:
            True if enqueued successfully
        """
        cdef CMorsel cpp_morsel
        cpp_morsel.batch = pyarrow_unwrap_batch(batch)
        cpp_morsel.start_row = start_row
        cpp_morsel.num_rows = num_rows
        cpp_morsel.partition_id = partition_id
        cpp_morsel.source = CMorselSource.NETWORK if source == 'network' else CMorselSource.LOCAL

        cdef cbool success
        with nogil:
            success = self._manager.EnqueueMorsel(cpp_morsel)

        return success

    def add_slots(self, int count):
        """
        Add worker slots (elastic scale up).

        Args:
            count: Number of slots to add
        """
        with nogil:
            self._manager.AddSlots(count)

    def remove_slots(self, int count):
        """
        Remove worker slots (elastic scale down).

        Slots are removed gracefully after completing current morsels.

        Args:
            count: Number of slots to remove
        """
        with nogil:
            self._manager.RemoveSlots(count)

    @property
    def num_slots(self):
        """Get current number of slots."""
        cdef int n
        with nogil:
            n = self._manager.GetNumSlots()
        return n

    @property
    def available_slots(self):
        """Get number of idle (available) slots."""
        cdef int n
        with nogil:
            n = self._manager.GetAvailableSlots()
        return n

    @property
    def queue_depth(self):
        """Get current queue depth (enqueued - processed)."""
        cdef int64_t depth
        with nogil:
            depth = self._manager.GetQueueDepth()
        return depth

    def get_slot_stats(self):
        """
        Get per-slot statistics.

        Returns:
            List of dicts with slot stats:
                - slot_id
                - busy (bool)
                - morsels_processed
                - total_processing_time_us
                - local_morsels
                - network_morsels
        """
        cdef vector[CSlotStats] cpp_stats
        with nogil:
            cpp_stats = self._manager.GetSlotStats()

        stats = []
        for i in range(cpp_stats.size()):
            stats.append({
                'slot_id': cpp_stats[i].slot_id,
                'busy': cpp_stats[i].busy,
                'morsels_processed': cpp_stats[i].morsels_processed,
                'total_processing_time_us': cpp_stats[i].total_processing_time_us,
                'local_morsels': cpp_stats[i].local_morsels,
                'network_morsels': cpp_stats[i].network_morsels,
            })

        return stats

    def shutdown(self):
        """Shutdown all slots and wait for completion."""
        if self._manager != NULL:
            self._manager.Shutdown()
