#!/usr/bin/env python3
"""
Arrow-Native Windowing Engine for Sabot

Implements high-performance, zero-copy windowing operations using Apache Arrow:
- Tumbling, Sliding, Hopping, and Session windows
- Vectorized aggregations using Arrow compute
- Memory pool management for zero-copy operations
- Integration with Arrow RecordBatches and Tables
"""

import asyncio
import logging
from typing import Dict, List, Optional, Any, AsyncIterator, Tuple
from dataclasses import dataclass, field
from collections import defaultdict
import time

# Try to import internal Arrow implementation first
try:
    from .arrow import Table, RecordBatch, Array, Schema, Field, compute as pc, USING_INTERNAL, USING_EXTERNAL
    # Create pa-like namespace for compatibility
    class _ArrowCompat:
        Table = Table
        RecordBatch = RecordBatch
        Array = Array
        Schema = Schema
        Field = Field
    pa = _ArrowCompat()
    ARROW_AVAILABLE = USING_INTERNAL or USING_EXTERNAL
except ImportError:
    ARROW_AVAILABLE = False
    pa = None
    pc = None

from .windows import WindowType, WindowSpec
from ..core.metrics import MetricsCollector

logger = logging.getLogger(__name__)


@dataclass
class ArrowWindowState:
    """State for an individual Arrow window."""
    window_key: str
    start_time: float
    end_time: float
    record_count: int = 0
    memory_pool: Optional[Any] = None
    record_batches: List[pa.RecordBatch] = field(default_factory=list)
    aggregated_data: Optional[pa.RecordBatch] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    @property
    def is_complete(self) -> bool:
        """Check if window is complete and ready for emission."""
        return time.time() >= self.end_time

    @property
    def total_memory_usage(self) -> int:
        """Get total memory usage of this window."""
        if not ARROW_AVAILABLE:
            return 0

        total = 0
        for batch in self.record_batches:
            if hasattr(batch, 'nbytes'):
                total += batch.nbytes
        if self.aggregated_data and hasattr(self.aggregated_data, 'nbytes'):
            total += self.aggregated_data.nbytes
        return total


class ArrowWindowProcessor:
    """
    Arrow-Native Window Processor

    Processes streaming data through time-based windows using Apache Arrow
    for zero-copy operations and vectorized computations.
    """

    def __init__(self, spec: WindowSpec, enable_metrics: bool = True):
        """
        Initialize Arrow window processor.

        Args:
            spec: Window specification
            enable_metrics: Whether to enable metrics collection
        """
        if not ARROW_AVAILABLE:
            raise RuntimeError("Arrow windowing requires PyArrow. Install with: pip install pyarrow")

        self.spec = spec
        self.active_windows: Dict[str, ArrowWindowState] = {}
        self.completed_windows: List[ArrowWindowState] = []
        self.memory_pool = pa.default_memory_pool()

        # Metrics
        self.metrics: Optional[MetricsCollector] = None
        if enable_metrics:
            self.metrics = MetricsCollector()

        # Window configuration
        self.key_field = spec.key_field
        self.timestamp_field = spec.timestamp_field
        self.aggregations = spec.aggregations or {}

        # Window timing parameters
        self.window_size = spec.size_seconds
        self.slide_size = spec.slide_seconds or spec.size_seconds  # Default to tumbling
        self.hop_size = spec.hop_seconds or spec.size_seconds
        self.session_timeout = spec.timeout_seconds or (spec.size_seconds * 2)

        logger.info(f"ArrowWindowProcessor initialized for {spec.window_type.value} windows")

    async def process_record_batch(self, record_batch: pa.RecordBatch) -> None:
        """
        Process a batch of records through windows.

        Args:
            record_batch: Arrow RecordBatch to process
        """
        if not isinstance(record_batch, pa.RecordBatch):
            logger.error(f"Expected Arrow RecordBatch, got {type(record_batch)}")
            return

        start_time = time.time()

        # Extract timestamps and keys for window assignment
        timestamps = self._extract_timestamps(record_batch)
        keys = self._extract_keys(record_batch)

        if not timestamps or not keys:
            logger.warning("No valid timestamps or keys found in batch")
            return

        # Process each record
        for i, (timestamp, key) in enumerate(zip(timestamps, keys)):
            window_keys = self._assign_to_windows(timestamp, key)

            for window_key in window_keys:
                await self._add_record_to_window(window_key, record_batch, i, timestamp)

        # Clean up completed windows
        await self._cleanup_completed_windows()

        # Record metrics
        if self.metrics:
            processing_time = time.time() - start_time
            await self.metrics.record_message_processed(
                "window_processor",
                message_count=record_batch.num_rows
            )

    def _extract_timestamps(self, batch: pa.RecordBatch) -> List[float]:
        """Extract timestamps from record batch."""
        if self.timestamp_field not in batch.schema.names:
            # Use current time if no timestamp field
            return [time.time()] * batch.num_rows

        timestamp_col = batch.column(self.timestamp_field)
        return pc.to_list(timestamp_col).to_pylist()

    def _extract_keys(self, batch: pa.RecordBatch) -> List[str]:
        """Extract keys from record batch."""
        if self.key_field not in batch.schema.names:
            # Use default key if no key field
            return ["default"] * batch.num_rows

        key_col = batch.column(self.key_field)
        return [str(k) for k in pc.to_list(key_col).to_pylist()]

    def _assign_to_windows(self, timestamp: float, key: str) -> List[str]:
        """
        Assign a record to appropriate windows based on window type.

        Returns:
            List of window keys this record belongs to
        """
        window_keys = []

        if self.spec.window_type == WindowType.TUMBLING:
            # Tumbling windows: non-overlapping, fixed size
            window_start = int(timestamp / self.window_size) * self.window_size
            window_key = f"{key}:{window_start}"
            window_keys.append(window_key)

        elif self.spec.window_type == WindowType.SLIDING:
            # Sliding windows: overlapping, slide by slide_size
            window_starts = []
            max_start = int(timestamp / self.slide_size) * self.slide_size

            # Calculate which windows this timestamp belongs to
            for start_time in range(int(max_start), int(timestamp - self.window_size) - 1, -int(self.slide_size)):
                if start_time <= timestamp < start_time + self.window_size:
                    window_starts.append(start_time)

            window_keys = [f"{key}:{start}" for start in window_starts]

        elif self.spec.window_type == WindowType.HOPPING:
            # Hopping windows: overlapping, fixed hop size
            window_start = int(timestamp / self.hop_size) * self.hop_size
            window_key = f"{key}:{window_start}"
            window_keys.append(window_key)

        elif self.spec.window_type == WindowType.SESSION:
            # Session windows: dynamic based on activity gaps
            # This is more complex - for now, use simplified session logic
            window_start = int(timestamp / self.session_timeout) * self.session_timeout
            window_key = f"{key}:{window_start}"
            window_keys.append(window_key)

        return window_keys

    async def _add_record_to_window(self, window_key: str, batch: pa.RecordBatch,
                                  record_index: int, timestamp: float) -> None:
        """
        Add a record to a window.

        Args:
            window_key: Window identifier
            batch: Source record batch
            record_index: Index of record in batch
            timestamp: Record timestamp
        """
        # Create or get window state
        if window_key not in self.active_windows:
            window_start = float(window_key.split(':')[-1])
            self.active_windows[window_key] = ArrowWindowState(
                window_key=window_key,
                start_time=window_start,
                end_time=window_start + self.window_size,
                memory_pool=self.memory_pool
            )

        window_state = self.active_windows[window_key]

        # Extract single record as new batch
        single_record_batch = self._extract_single_record(batch, record_index)

        # Store record batch (zero-copy if possible)
        window_state.record_batches.append(single_record_batch)
        window_state.record_count += 1

    def _extract_single_record(self, batch: pa.RecordBatch, index: int) -> pa.RecordBatch:
        """Extract a single record from a batch as a new batch."""
        # Create new batch with single row
        arrays = []
        for i, field in enumerate(batch.schema):
            array = batch.column(i)
            single_value_array = pc.take(array, [index])
            arrays.append(single_value_array)

        return pa.RecordBatch.from_arrays(arrays, names=batch.schema.names)

    async def _cleanup_completed_windows(self) -> None:
        """Move completed windows to completed list and trigger aggregation."""
        current_time = time.time()
        completed_keys = []

        for window_key, window_state in self.active_windows.items():
            if window_state.is_complete:
                # Aggregate window data
                await self._aggregate_window(window_state)

                # Move to completed
                self.completed_windows.append(window_state)
                completed_keys.append(window_key)

        # Remove from active
        for key in completed_keys:
            del self.active_windows[key]

    async def _aggregate_window(self, window_state: ArrowWindowState) -> None:
        """
        Aggregate data in a completed window.

        Args:
            window_state: Window state to aggregate
        """
        if not window_state.record_batches:
            return

        try:
            # Combine all record batches in window
            if len(window_state.record_batches) == 1:
                combined_batch = window_state.record_batches[0]
            else:
                combined_batch = pa.Table.from_batches(window_state.record_batches).to_batches()[0]

            # Apply aggregations
            if self.aggregations:
                aggregated_data = await self._compute_arrow_aggregations(combined_batch)
                window_state.aggregated_data = aggregated_data
            else:
                window_state.aggregated_data = combined_batch

        except Exception as e:
            logger.error(f"Failed to aggregate window {window_state.window_key}: {e}")
            # Keep original data if aggregation fails
            window_state.aggregated_data = window_state.record_batches[0] if window_state.record_batches else None

    async def _compute_arrow_aggregations(self, batch: pa.RecordBatch) -> pa.RecordBatch:
        """
        Compute aggregations using Arrow compute functions.

        Args:
            batch: Input record batch

        Returns:
            Aggregated record batch
        """
        results = {}

        for field_name, agg_func in self.aggregations.items():
            if field_name not in batch.schema.names:
                continue

            column = batch.column(field_name)

            try:
                if agg_func == 'sum':
                    result = pc.sum(column)
                elif agg_func == 'mean' or agg_func == 'avg':
                    result = pc.mean(column)
                elif agg_func == 'count':
                    result = pc.count(column)
                elif agg_func == 'min':
                    result = pc.min(column)
                elif agg_func == 'max':
                    result = pc.max(column)
                elif agg_func == 'std' or agg_func == 'stddev':
                    result = pc.stddev(column)
                else:
                    logger.warning(f"Unsupported aggregation function: {agg_func}")
                    continue

                results[field_name] = result

            except Exception as e:
                logger.error(f"Failed to compute {agg_func} for {field_name}: {e}")

        # Create result batch
        if results:
            arrays = [pa.array([value.as_py()]) for value in results.values()]
            names = list(results.keys())
            return pa.RecordBatch.from_arrays(arrays, names=names)
        else:
            # Return empty batch if no aggregations computed
            return pa.record_batch([])

    async def emit_completed_windows(self) -> AsyncIterator[Dict[str, Any]]:
        """
        Emit completed windows.

        Yields:
            Window data as dictionaries
        """
        # Process completed windows
        for window_state in self.completed_windows:
            window_data = {
                'window_key': window_state.window_key,
                'start_time': window_state.start_time,
                'end_time': window_state.end_time,
                'record_count': window_state.record_count,
                'memory_usage': window_state.total_memory_usage,
                'key': window_state.window_key.split(':')[0] if ':' in window_state.window_key else 'unknown'
            }

            # Include aggregated data if available
            if window_state.aggregated_data:
                # Convert to Python dict for easier consumption
                if hasattr(window_state.aggregated_data, 'to_pydict'):
                    window_data['aggregations'] = window_state.aggregated_data.to_pydict()
                else:
                    window_data['aggregations'] = {}

            yield window_data

        # Clear emitted windows
        self.completed_windows.clear()

    def get_window_stats(self) -> Dict[str, Any]:
        """Get window processor statistics."""
        total_memory = sum(w.total_memory_usage for w in self.active_windows.values())
        total_records = sum(w.record_count for w in self.active_windows.values())

        return {
            'active_windows': len(self.active_windows),
            'completed_windows': len(self.completed_windows),
            'total_memory_usage': total_memory,
            'total_records_processed': total_records,
            'window_type': self.spec.window_type.value,
            'window_size': self.window_size
        }

    async def flush_all_windows(self) -> AsyncIterator[Dict[str, Any]]:
        """
        Force flush all active windows (for shutdown/cleanup).

        Yields:
            All window data
        """
        # Aggregate all active windows
        for window_state in self.active_windows.values():
            await self._aggregate_window(window_state)
            self.completed_windows.append(window_state)

        # Clear active windows
        self.active_windows.clear()

        # Emit all completed windows
        async for window_data in self.emit_completed_windows():
            yield window_data
