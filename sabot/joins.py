# -*- coding: utf-8 -*-
"""Arrow-focused join operations for Sabot streams and tables."""

import asyncio
from typing import Any, Dict, List, Optional, Callable, AsyncIterator, Union
from enum import Enum

from .observability import get_observability

# Try to import Cython implementations
try:
    from ._cython.joins import (
        StreamTableJoinProcessor as CythonStreamTableJoin,
        StreamStreamJoinProcessor as CythonStreamStreamJoin,
        IntervalJoinProcessor as CythonIntervalJoin,
        TemporalJoinProcessor as CythonTemporalJoin,
        LookupJoinProcessor as CythonLookupJoin,
        WindowJoinProcessor as CythonWindowJoin,
        TableTableJoinProcessor as CythonTableTableJoin,
        ArrowTableJoinProcessor as CythonArrowTableJoin,
        ArrowDatasetJoinProcessor as CythonArrowDatasetJoin,
        ArrowAsofJoinProcessor as CythonArrowAsofJoin,
        create_stream_table_join,
        create_stream_stream_join,
        create_interval_join,
        create_temporal_join,
        create_lookup_join,
        create_window_join,
        create_table_table_join,
        create_arrow_table_join,
        create_arrow_dataset_join,
        create_arrow_asof_join,
        JoinType as CythonJoinType,
        JoinMode as CythonJoinMode,
        JoinCondition,
    )
    CYTHON_AVAILABLE = True
except ImportError:
    CYTHON_AVAILABLE = False

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

# Python fallback implementations when Cython is not available
if not CYTHON_AVAILABLE and ARROW_AVAILABLE:
    # Provide Python implementations using PyArrow directly

    def create_arrow_table_join(join_type, conditions, **kwargs):
        return PythonArrowTableJoinProcessor(join_type, conditions, **kwargs)

    def create_arrow_dataset_join(join_type, conditions, **kwargs):
        return PythonArrowDatasetJoinProcessor(join_type, conditions, **kwargs)

    def create_arrow_asof_join(left_key, right_key, left_by_key=None, right_by_key=None, tolerance=0.0):
        return PythonArrowAsofJoinProcessor(left_key, right_key, left_by_key, right_by_key, tolerance)

    # Python implementations
    class PythonArrowTableJoinProcessor:
        def __init__(self, join_type, conditions, **kwargs):
            self.join_type = join_type
            self.conditions = conditions
            self.left_table = None
            self.right_table = None

        async def load_left_table(self, table):
            self.left_table = table

        async def load_right_table(self, table):
            self.right_table = table

        async def execute_join(self):
            if not self.left_table or not self.right_table:
                raise ValueError("Both left and right tables must be loaded")

            # Convert conditions to Arrow join keys
            left_keys = []
            right_keys = []

            for condition in self.conditions:
                left_keys.append(condition.get('left_field', 'id'))
                right_keys.append(condition.get('right_field', 'id'))

            # Map join types
            join_type_map = {
                'inner': 'inner outer',
                'left_outer': 'left outer',
                'right_outer': 'right outer',
                'full_outer': 'full outer'
            }

            arrow_join_type = join_type_map.get(self.join_type, 'inner outer')

            # Perform the join
            result = self.left_table.join(
                self.right_table,
                keys=left_keys + right_keys,
                right_keys=right_keys,
                join_type=arrow_join_type
            )

            return result

    class PythonArrowDatasetJoinProcessor:
        def __init__(self, join_type, conditions, **kwargs):
            self.join_type = join_type
            self.conditions = conditions
            self.left_dataset = None
            self.right_dataset = None

        async def load_left_dataset(self, dataset):
            self.left_dataset = dataset

        async def load_right_dataset(self, dataset):
            self.right_dataset = dataset

        async def execute_join(self):
            if not self.left_dataset or not self.right_dataset:
                raise ValueError("Both left and right datasets must be loaded")

            # Convert conditions to join keys
            left_keys = []
            right_keys = []

            for condition in self.conditions:
                left_keys.append(condition.get('left_field', 'id'))
                right_keys.append(condition.get('right_field', 'id'))

            # Perform dataset join
            result = self.left_dataset.join(
                self.right_dataset,
                keys=left_keys,
                right_keys=right_keys,
                join_type=self.join_type
            )

            return result

    class PythonArrowAsofJoinProcessor:
        def __init__(self, left_key, right_key, left_by_key=None, right_by_key=None, tolerance=0.0):
            self.left_key = left_key
            self.right_key = right_key
            self.left_by_key = left_by_key
            self.right_by_key = right_by_key
            self.tolerance = tolerance

        async def execute_join(self, left_table, right_table):
            """Execute as-of join using PyArrow."""
            try:
                # Try to use Arrow's native asof join if available (Arrow 10.0+)
                result = left_table.join_asof(
                    right_table,
                    on=self.left_key,
                    by=self.left_by_key,
                    right_on=self.right_key,
                    right_by=self.right_by_key,
                    tolerance=self.tolerance
                )
                return result
            except AttributeError:
                # Fallback to pandas for asof join
                try:
                    import pandas as pd

                    left_df = left_table.to_pandas()
                    right_df = right_table.to_pandas()

                    # Perform asof join
                    result_df = pd.merge_asof(
                        left_df.sort_values(self.left_key),
                        right_df.sort_values(self.right_key),
                        left_on=self.left_key,
                        right_on=self.right_key,
                        by=self.left_by_key,
                        tolerance=self.tolerance,
                        direction='backward'
                    )

                    # Convert back to Arrow
                    return pa.Table.from_pandas(result_df)

                except ImportError:
                    raise RuntimeError("Asof joins require pandas fallback when Arrow native asof is unavailable")

elif not CYTHON_AVAILABLE and not ARROW_AVAILABLE:
    # Neither Cython nor PyArrow available
    def create_arrow_table_join(join_type, conditions, **kwargs):
        raise ImportError("Arrow joins require PyArrow. Install with: pip install pyarrow")

    def create_arrow_dataset_join(join_type, conditions, **kwargs):
        raise ImportError("Arrow joins require PyArrow. Install with: pip install pyarrow")

    def create_arrow_asof_join(left_key, right_key, left_by_key=None, right_by_key=None, tolerance=0.0):
        raise ImportError("Arrow joins require PyArrow. Install with: pip install pyarrow")

    # Stub classes for when neither Cython nor PyArrow is available
    class ArrowTableJoinProcessor:
        pass

    class ArrowDatasetJoinProcessor:
        pass

    class ArrowAsofJoinProcessor:
        pass


class JoinType(Enum):
    """Join types supported by Sabot (Flink-compatible)."""
    INNER = "inner"
    LEFT_OUTER = "left_outer"
    RIGHT_OUTER = "right_outer"
    FULL_OUTER = "full_outer"


class JoinMode(Enum):
    """Join execution modes (Flink-style)."""
    BATCH = "batch"           # Traditional batch joins
    STREAMING = "streaming"   # Streaming joins
    INTERVAL = "interval"     # Time-interval based joins
    TEMPORAL = "temporal"     # Temporal table joins
    WINDOW = "window"         # Window-based joins
    LOOKUP = "lookup"         # External system lookups


class StreamTableJoin:
    """Stream-table join operation."""

    def __init__(
        self,
        stream,
        table,
        join_type: JoinType = JoinType.INNER,
        conditions: Optional[List[Dict[str, str]]] = None,
        **kwargs
    ):
        self.stream = stream
        self.table = table
        self.join_type = join_type
        self.conditions = conditions or [{"left_field": "id", "right_field": "id"}]

        self._cython_processor = None
        if CYTHON_AVAILABLE:
            try:
                self._cython_processor = create_stream_table_join(
                    join_type.value,
                    self.conditions,
                    **kwargs
                )
            except Exception as e:
                print(f"Cython stream-table join failed: {e}")

    async def __aenter__(self):
        """Initialize the join operation."""
        if self._cython_processor:
            # Load table data
            table_data = {}
            if hasattr(self.table, 'items'):
                # If table has items method (like our Table class)
                async for key, value in self.table.aitems():
                    table_data[key] = value
            elif hasattr(self.table, 'query'):
                # If it's a materialized view
                results = await self.table.query()
                for result in results:
                    key = result.get('id', str(id(result)))
                    table_data[key] = result

            await self._cython_processor.load_table_data(table_data)

        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Clean up the join operation."""
        pass

    def __aiter__(self):
        return self._create_join_stream()

    async def _create_join_stream(self) -> AsyncIterator[Any]:
        """Create an async iterator that yields joined results."""
        async for record_batch in self.stream:
            # Extract records from batch
            records = self._extract_records(record_batch)

            for record in records:
                if self._cython_processor:
                    joined_results = await self._cython_processor.process_stream_record(record)
                    for result in joined_results:
                        yield result
                else:
                    # Fallback: simple lookup join
                    yield await self._fallback_join(record)

    def _extract_records(self, record_batch: Any) -> List[Dict[str, Any]]:
        """Extract records from various input formats."""
        if hasattr(record_batch, 'to_pylist'):
            # Arrow RecordBatch
            return record_batch.to_pylist()
        elif isinstance(record_batch, list):
            return record_batch
        elif isinstance(record_batch, dict):
            return [record_batch]
        else:
            return []

    async def _fallback_join(self, stream_record: Dict[str, Any]) -> Dict[str, Any]:
        """Fallback join implementation."""
        # Simple lookup in table
        join_key = None
        if self.conditions:
            key_field = self.conditions[0].get('right_field', 'id')
            join_key = stream_record.get(key_field)

        if join_key is not None:
            # Try to get from table
            try:
                if hasattr(self.table, 'aget'):
                    table_record = await self.table.aget(join_key)
                    if table_record:
                        # Merge records
                        merged = dict(stream_record)
                        for key, value in table_record.items():
                            merged[f"table_{key}"] = value
                        return merged
            except Exception:
                pass

        # Return original record if join fails
        return stream_record


class StreamStreamJoin:
    """Stream-stream join operation with windowing."""

    def __init__(
        self,
        left_stream,
        right_stream,
        join_type: JoinType = JoinType.INNER,
        conditions: Optional[List[Dict[str, str]]] = None,
        window_size_seconds: float = 300.0,
        **kwargs
    ):
        self.left_stream = left_stream
        self.right_stream = right_stream
        self.join_type = join_type
        self.conditions = conditions or [{"left_field": "id", "right_field": "id"}]
        self.window_size_seconds = window_size_seconds

        self._cython_processor = None
        if CYTHON_AVAILABLE:
            try:
                self._cython_processor = create_stream_stream_join(
                    join_type.value,
                    self.conditions,
                    window_size_seconds=window_size_seconds,
                    **kwargs
                )
            except Exception as e:
                print(f"Cython stream-stream join failed: {e}")

    async def __aenter__(self):
        """Initialize the join operation."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Clean up the join operation."""
        pass

    def __aiter__(self):
        return self._create_join_stream()

    async def _create_join_stream(self) -> AsyncIterator[Any]:
        """Create an async iterator that yields joined results."""
        # Create tasks for both streams
        left_task = asyncio.create_task(self._process_left_stream())
        right_task = asyncio.create_task(self._process_right_stream())

        # Wait for both to complete
        await asyncio.gather(left_task, right_task)

    async def _process_left_stream(self) -> None:
        """Process the left stream."""
        async for record_batch in self.left_stream:
            records = self._extract_records(record_batch)
            for record in records:
                if self._cython_processor:
                    joined_results = await self._cython_processor.process_left_stream(record)
                    for result in joined_results:
                        yield result
                else:
                    # Fallback processing
                    pass

    async def _process_right_stream(self) -> None:
        """Process the right stream."""
        async for record_batch in self.right_stream:
            records = self._extract_records(record_batch)
            for record in records:
                if self._cython_processor:
                    joined_results = await self._cython_processor.process_right_stream(record)
                    for result in joined_results:
                        yield result
                else:
                    # Fallback processing
                    pass

    def _extract_records(self, record_batch: Any) -> List[Dict[str, Any]]:
        """Extract records from various input formats."""
        if hasattr(record_batch, 'to_pylist'):
            # Arrow RecordBatch
            return record_batch.to_pylist()
        elif isinstance(record_batch, list):
            return record_batch
        elif isinstance(record_batch, dict):
            return [record_batch]
        else:
            return []


class TableTableJoin:
    """Table-table join operation."""

    def __init__(
        self,
        left_table,
        right_table,
        join_type: JoinType = JoinType.INNER,
        conditions: Optional[List[Dict[str, str]]] = None,
        **kwargs
    ):
        self.left_table = left_table
        self.right_table = right_table
        self.join_type = join_type
        self.conditions = conditions or [{"left_field": "id", "right_field": "id"}]

        self._cython_processor = None
        if CYTHON_AVAILABLE:
            try:
                self._cython_processor = create_table_table_join(
                    join_type.value,
                    self.conditions,
                    **kwargs
                )
            except Exception as e:
                print(f"Cython table-table join failed: {e}")

    async def execute(self) -> List[Dict[str, Any]]:
        """Execute the table-table join."""
        if self._cython_processor:
            # Load table data
            left_data = {}
            right_data = {}

            # Load left table data
            if hasattr(self.left_table, 'aitems'):
                async for key, value in self.left_table.aitems():
                    left_data[key] = value
            elif hasattr(self.left_table, 'query'):
                results = await self.left_table.query()
                for result in results:
                    key = result.get('id', str(id(result)))
                    left_data[key] = result

            # Load right table data
            if hasattr(self.right_table, 'aitems'):
                async for key, value in self.right_table.aitems():
                    right_data[key] = value
            elif hasattr(self.right_table, 'query'):
                results = await self.right_table.query()
                for result in results:
                    key = result.get('id', str(id(result)))
                    right_data[key] = result

            await self._cython_processor.load_left_table(left_data)
            await self._cython_processor.load_right_table(right_data)

            return await self._cython_processor.execute_join()
        else:
            # Fallback implementation
            return await self._fallback_join()

    async def _fallback_join(self) -> List[Dict[str, Any]]:
        """Fallback table-table join implementation."""
        results = []

        # Simple nested loop join
        left_data = {}
        right_data = {}

        # Load data
        if hasattr(self.left_table, 'aitems'):
            async for key, value in self.left_table.aitems():
                left_data[key] = value
        if hasattr(self.right_table, 'aitems'):
            async for key, value in self.right_table.aitems():
                right_data[key] = value

        # Perform join
        for left_key, left_record in left_data.items():
            for right_key, right_record in right_data.items():
                # Simple equality join on first condition
                if self.conditions:
                    left_field = self.conditions[0].get('left_field', 'id')
                    right_field = self.conditions[0].get('right_field', 'id')

                    if left_record.get(left_field) == right_record.get(right_field):
                        # Merge records
                        merged = dict(left_record)
                        for key, value in right_record.items():
                            if key not in merged:
                                merged[key] = value
                            else:
                                merged[f"right_{key}"] = value
                        results.append(merged)

        return results


class IntervalJoin:
    """Interval join operation (Flink-style time-interval joins)."""

    def __init__(
        self,
        left_stream,
        right_stream,
        join_type: JoinType = JoinType.INNER,
        conditions: Optional[List[Dict[str, str]]] = None,
        left_lower_bound: float = -5.0,
        left_upper_bound: float = 5.0,
        right_lower_bound: float = -5.0,
        right_upper_bound: float = 5.0,
        **kwargs
    ):
        self.left_stream = left_stream
        self.right_stream = right_stream
        self.join_type = join_type
        self.conditions = conditions or [{"left_field": "id", "right_field": "id"}]
        self.left_lower_bound = left_lower_bound
        self.left_upper_bound = left_upper_bound
        self.right_lower_bound = right_lower_bound
        self.right_upper_bound = right_upper_bound

        self._cython_processor = None
        if CYTHON_AVAILABLE:
            try:
                self._cython_processor = create_interval_join(
                    join_type.value,
                    self.conditions,
                    left_lower_bound,
                    left_upper_bound,
                    right_lower_bound,
                    right_upper_bound,
                    **kwargs
                )
            except Exception as e:
                print(f"Cython interval join failed: {e}")

    async def __aenter__(self):
        """Initialize the join operation."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Clean up the join operation."""
        pass

    def __aiter__(self):
        return self._create_join_stream()

    async def _create_join_stream(self) -> AsyncIterator[Any]:
        """Create an async iterator that yields joined results."""
        # Create tasks for both streams
        left_task = asyncio.create_task(self._process_left_stream())
        right_task = asyncio.create_task(self._process_right_stream())

        # Wait for both to complete
        await asyncio.gather(left_task, right_task)

    async def _process_left_stream(self) -> None:
        """Process the left stream."""
        async for record_batch in self.left_stream:
            records = self._extract_records(record_batch)
            for record in records:
                if self._cython_processor:
                    joined_results = await self._cython_processor.process_left_record(record)
                    for result in joined_results:
                        yield result

    async def _process_right_stream(self) -> None:
        """Process the right stream."""
        async for record_batch in self.right_stream:
            records = self._extract_records(record_batch)
            for record in records:
                if self._cython_processor:
                    joined_results = await self._cython_processor.process_right_record(record)
                    for result in joined_results:
                        yield result

    def _extract_records(self, record_batch: Any) -> List[Dict[str, Any]]:
        """Extract records from various input formats."""
        if hasattr(record_batch, 'to_pylist'):
            # Arrow RecordBatch
            return record_batch.to_pylist()
        elif isinstance(record_batch, list):
            return record_batch
        elif isinstance(record_batch, dict):
            return [record_batch]
        else:
            return []


class TemporalJoin:
    """Temporal table join operation (Flink-style temporal joins)."""

    def __init__(
        self,
        stream,
        temporal_table,
        join_type: JoinType = JoinType.INNER,
        conditions: Optional[List[Dict[str, str]]] = None,
        **kwargs
    ):
        self.stream = stream
        self.temporal_table = temporal_table
        self.join_type = join_type
        self.conditions = conditions or [{"left_field": "id", "right_field": "id"}]

        self._cython_processor = None
        if CYTHON_AVAILABLE:
            try:
                self._cython_processor = create_temporal_join(
                    join_type.value,
                    self.conditions,
                    **kwargs
                )
            except Exception as e:
                print(f"Cython temporal join failed: {e}")

    async def __aenter__(self):
        """Initialize the join operation."""
        if self._cython_processor:
            # Load initial temporal table data
            table_data = {}
            if hasattr(self.temporal_table, 'aitems'):
                async for key, value in self.temporal_table.aitems():
                    table_data[key] = value

            await self._cython_processor.update_temporal_table(table_data, 1)

        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Clean up the join operation."""
        pass

    def __aiter__(self):
        return self._create_join_stream()

    async def _create_join_stream(self) -> AsyncIterator[Any]:
        """Create an async iterator that yields joined results."""
        async for record_batch in self.stream:
            records = self._extract_records(record_batch)
            for record in records:
                if self._cython_processor:
                    joined_results = await self._cython_processor.process_stream_record(record, 1)
                    for result in joined_results:
                        yield result
                else:
                    # Fallback processing
                    yield record

    def _extract_records(self, record_batch: Any) -> List[Dict[str, Any]]:
        """Extract records from various input formats."""
        if hasattr(record_batch, 'to_pylist'):
            # Arrow RecordBatch
            return record_batch.to_pylist()
        elif isinstance(record_batch, list):
            return record_batch
        elif isinstance(record_batch, dict):
            return [record_batch]
        else:
            return []


class LookupJoin:
    """Lookup join operation (Flink-style external system joins)."""

    def __init__(
        self,
        stream,
        lookup_func: Callable[[str], Any],
        join_type: JoinType = JoinType.LEFT_OUTER,
        conditions: Optional[List[Dict[str, str]]] = None,
        cache_size: int = 1000,
        **kwargs
    ):
        self.stream = stream
        self.lookup_func = lookup_func
        self.join_type = join_type
        self.conditions = conditions or [{"left_field": "id", "right_field": "id"}]
        self.cache_size = cache_size

        self._cython_processor = None
        if CYTHON_AVAILABLE:
            try:
                self._cython_processor = create_lookup_join(
                    join_type.value,
                    self.conditions,
                    lookup_func,
                    cache_size=cache_size,
                    **kwargs
                )
            except Exception as e:
                print(f"Cython lookup join failed: {e}")

    async def __aenter__(self):
        """Initialize the join operation."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Clean up the join operation."""
        pass

    def __aiter__(self):
        return self._create_join_stream()

    async def _create_join_stream(self) -> AsyncIterator[Any]:
        """Create an async iterator that yields joined results."""
        async for record_batch in self.stream:
            records = self._extract_records(record_batch)
            for record in records:
                if self._cython_processor:
                    joined_results = await self._cython_processor.process_stream_record(record)
                    for result in joined_results:
                        yield result
                else:
                    # Fallback processing
                    yield record

    def _extract_records(self, record_batch: Any) -> List[Dict[str, Any]]:
        """Extract records from various input formats."""
        if hasattr(record_batch, 'to_pylist'):
            # Arrow RecordBatch
            return record_batch.to_pylist()
        elif isinstance(record_batch, list):
            return record_batch
        elif isinstance(record_batch, dict):
            return [record_batch]
        else:
            return []


class WindowJoin:
    """Window join operation (Flink-style window joins)."""

    def __init__(
        self,
        left_stream,
        right_stream,
        join_type: JoinType = JoinType.INNER,
        conditions: Optional[List[Dict[str, str]]] = None,
        window_size: float = 300.0,
        window_slide: float = 0.0,
        **kwargs
    ):
        self.left_stream = left_stream
        self.right_stream = right_stream
        self.join_type = join_type
        self.conditions = conditions or [{"left_field": "id", "right_field": "id"}]
        self.window_size = window_size
        self.window_slide = window_slide

        self._cython_processor = None
        if CYTHON_AVAILABLE:
            try:
                self._cython_processor = create_window_join(
                    join_type.value,
                    self.conditions,
                    window_size,
                    window_slide=window_slide,
                    **kwargs
                )
            except Exception as e:
                print(f"Cython window join failed: {e}")

    async def __aenter__(self):
        """Initialize the join operation."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Clean up the join operation."""
        pass

    def __aiter__(self):
        return self._create_join_stream()

    async def _create_join_stream(self) -> AsyncIterator[Any]:
        """Create an async iterator that yields joined results."""
        # Create tasks for both streams
        left_task = asyncio.create_task(self._process_left_stream())
        right_task = asyncio.create_task(self._process_right_stream())

        # Wait for both to complete
        await asyncio.gather(left_task, right_task)

    async def _process_left_stream(self) -> None:
        """Process the left stream."""
        async for record_batch in self.left_stream:
            records = self._extract_records(record_batch)
            for record in records:
                if self._cython_processor:
                    joined_results = await self._cython_processor.process_left_record(record)
                    for result in joined_results:
                        yield result

    async def _process_right_stream(self) -> None:
        """Process the right stream."""
        async for record_batch in self.right_stream:
            records = self._extract_records(record_batch)
            for record in records:
                if self._cython_processor:
                    joined_results = await self._cython_processor.process_right_record(record)
                    for result in joined_results:
                        yield result

    def _extract_records(self, record_batch: Any) -> List[Dict[str, Any]]:
        """Extract records from various input formats."""
        if hasattr(record_batch, 'to_pylist'):
            # Arrow RecordBatch
            return record_batch.to_pylist()
        elif isinstance(record_batch, list):
            return record_batch
        elif isinstance(record_batch, dict):
            return [record_batch]
        else:
            return []


class ArrowTableJoin:
    """Arrow-native table-table join operation."""

    def __init__(
        self,
        left_table,
        right_table,
        join_type: JoinType = JoinType.INNER,
        conditions: Optional[List[Dict[str, str]]] = None,
        **kwargs
    ):
        self.left_table = left_table
        self.right_table = right_table
        self.join_type = join_type
        self.conditions = conditions or [{"left_field": "id", "right_field": "id"}]

        # Initialize observability
        self.observability = get_observability()

        self._cython_processor = None
        if CYTHON_AVAILABLE:
            try:
                self._cython_processor = create_arrow_table_join(
                    join_type.value,
                    self.conditions,
                    **kwargs
                )
            except Exception as e:
                print(f"Cython Arrow table join failed: {e}")

    async def execute(self):
        """Execute Arrow-native table join."""
        with self.observability.trace_operation(
            "arrow_table_join",
            {
                "join_type": self.join_type.value,
                "conditions_count": len(self.conditions),
                "left_table_type": type(self.left_table).__name__,
                "right_table_type": type(self.right_table).__name__
            }
        ):
            if self._cython_processor:
                await self._cython_processor.load_left_table(self.left_table)
                await self._cython_processor.load_right_table(self.right_table)
                return await self._cython_processor.execute_join()
            else:
                # Fallback: direct Arrow join using Python implementation
                if not ARROW_AVAILABLE:
                    raise ImportError("Arrow table joins require PyArrow. Install with: pip install pyarrow")

                # Use Python implementation
                processor = create_arrow_table_join(
                    self.join_type.value,
                    self.conditions
                )
                await processor.load_left_table(self.left_table)
                await processor.load_right_table(self.right_table)
                return await processor.execute_join()


class ArrowDatasetJoin:
    """Arrow-native dataset join operation."""

    def __init__(
        self,
        left_dataset,
        right_dataset,
        join_type: JoinType = JoinType.INNER,
        conditions: Optional[List[Dict[str, str]]] = None,
        **kwargs
    ):
        self.left_dataset = left_dataset
        self.right_dataset = right_dataset
        self.join_type = join_type
        self.conditions = conditions or [{"left_field": "id", "right_field": "id"}]

        self._cython_processor = None
        if CYTHON_AVAILABLE:
            try:
                self._cython_processor = create_arrow_dataset_join(
                    join_type.value,
                    self.conditions,
                    **kwargs
                )
            except Exception as e:
                print(f"Cython Arrow dataset join failed: {e}")

    async def execute(self):
        """Execute Arrow-native dataset join."""
        if self._cython_processor:
            await self._cython_processor.load_left_dataset(self.left_dataset)
            await self._cython_processor.load_right_dataset(self.right_dataset)
            return await self._cython_processor.execute_join()
        else:
            # Fallback: direct Arrow dataset join using Python implementation
            if not ARROW_AVAILABLE:
                raise ImportError("Arrow dataset joins require PyArrow. Install with: pip install pyarrow")

            # Use Python implementation
            processor = create_arrow_dataset_join(
                self.join_type.value,
                self.conditions
            )
            await processor.load_left_dataset(self.left_dataset)
            await processor.load_right_dataset(self.right_dataset)
            return await processor.execute_join()


class ArrowAsofJoin:
    """Arrow-native as-of join operation (temporal)."""

    def __init__(
        self,
        left_dataset,
        right_dataset,
        left_key: str,
        right_key: str,
        left_by_key: Optional[str] = None,
        right_by_key: Optional[str] = None,
        tolerance: float = 0.0,
        **kwargs
    ):
        self.left_dataset = left_dataset
        self.right_dataset = right_dataset
        self.left_key = left_key
        self.right_key = right_key
        self.left_by_key = left_by_key
        self.right_by_key = right_by_key
        self.tolerance = tolerance

        self._cython_processor = None
        if CYTHON_AVAILABLE:
            try:
                self._cython_processor = create_arrow_asof_join(
                    left_key,
                    right_key,
                    left_by_key,
                    right_by_key,
                    tolerance
                )
            except Exception as e:
                print(f"Cython Arrow as-of join failed: {e}")

    async def execute(self):
        """Execute Arrow-native as-of join."""
        if self._cython_processor:
            await self._cython_processor.load_left_dataset(self.left_dataset)
            await self._cython_processor.load_right_dataset(self.right_dataset)
            return await self._cython_processor.execute_join()
        else:
            # Fallback: direct Arrow as-of join using Python implementation
            if not ARROW_AVAILABLE:
                raise ImportError("Arrow as-of joins require PyArrow. Install with: pip install pyarrow")

            # Use Python implementation
            processor = create_arrow_asof_join(
                self.left_key,
                self.right_key,
                self.left_by_key,
                self.right_by_key,
                self.tolerance
            )
            return await processor.execute_join(self.left_table, self.right_table)


class JoinBuilder:
    """Fluent API for building joins (Flink-compatible)."""

    def __init__(self, app):
        self.app = app

    def stream_table(
        self,
        stream,
        table,
        join_type: JoinType = JoinType.INNER
    ) -> 'StreamTableJoinBuilder':
        """Start building a stream-table join (Flink-style)."""
        return StreamTableJoinBuilder(self.app, stream, table, join_type)

    def stream_stream(
        self,
        left_stream,
        right_stream,
        join_type: JoinType = JoinType.INNER
    ) -> 'StreamStreamJoinBuilder':
        """Start building a stream-stream join (Flink-style)."""
        return StreamStreamJoinBuilder(self.app, left_stream, right_stream, join_type)

    def interval_join(
        self,
        left_stream,
        right_stream,
        join_type: JoinType = JoinType.INNER
    ) -> 'IntervalJoinBuilder':
        """Start building an interval join (Flink-style)."""
        return IntervalJoinBuilder(self.app, left_stream, right_stream, join_type)

    def temporal_join(
        self,
        stream,
        temporal_table,
        join_type: JoinType = JoinType.INNER
    ) -> 'TemporalJoinBuilder':
        """Start building a temporal table join (Flink-style)."""
        return TemporalJoinBuilder(self.app, stream, temporal_table, join_type)

    def lookup_join(
        self,
        stream,
        lookup_func: Callable[[str], Any],
        join_type: JoinType = JoinType.LEFT_OUTER
    ) -> 'LookupJoinBuilder':
        """Start building a lookup join (Flink-style)."""
        return LookupJoinBuilder(self.app, stream, lookup_func, join_type)

    def window_join(
        self,
        left_stream,
        right_stream,
        join_type: JoinType = JoinType.INNER
    ) -> 'WindowJoinBuilder':
        """Start building a window join (Flink-style)."""
        return WindowJoinBuilder(self.app, left_stream, right_stream, join_type)

    def table_table(
        self,
        left_table,
        right_table,
        join_type: JoinType = JoinType.INNER
    ) -> 'TableTableJoinBuilder':
        """Start building a table-table join (Flink-style)."""
        return TableTableJoinBuilder(self.app, left_table, right_table, join_type)

    def arrow_table_join(
        self,
        left_table,
        right_table,
        join_type: JoinType = JoinType.INNER
    ) -> 'ArrowTableJoinBuilder':
        """Start building an Arrow-native table join."""
        return ArrowTableJoinBuilder(self.app, left_table, right_table, join_type)

    def arrow_dataset_join(
        self,
        left_dataset,
        right_dataset,
        join_type: JoinType = JoinType.INNER
    ) -> 'ArrowDatasetJoinBuilder':
        """Start building an Arrow-native dataset join."""
        return ArrowDatasetJoinBuilder(self.app, left_dataset, right_dataset, join_type)

    def arrow_asof_join(
        self,
        left_dataset,
        right_dataset,
        left_key: str,
        right_key: str
    ) -> 'ArrowAsofJoinBuilder':
        """Start building an Arrow-native as-of join."""
        return ArrowAsofJoinBuilder(self.app, left_dataset, right_dataset, left_key, right_key)


class StreamTableJoinBuilder:
    """Builder for stream-table joins."""

    def __init__(self, app, stream, table, join_type):
        self.app = app
        self.stream = stream
        self.table = table
        self.join_type = join_type
        self.conditions = []

    def on(self, left_field: str, right_field: str) -> 'StreamTableJoinBuilder':
        """Add a join condition."""
        self.conditions.append({
            'left_field': left_field,
            'right_field': right_field
        })
        return self

    def build(self) -> StreamTableJoin:
        """Build the join operation."""
        return StreamTableJoin(
            self.stream,
            self.table,
            self.join_type,
            self.conditions
        )


class StreamStreamJoinBuilder:
    """Builder for stream-stream joins."""

    def __init__(self, app, left_stream, right_stream, join_type):
        self.app = app
        self.left_stream = left_stream
        self.right_stream = right_stream
        self.join_type = join_type
        self.conditions = []
        self.window_size_seconds = 300.0

    def on(self, left_field: str, right_field: str) -> 'StreamStreamJoinBuilder':
        """Add a join condition."""
        self.conditions.append({
            'left_field': left_field,
            'right_field': right_field
        })
        return self

    def within(self, window_size_seconds: float) -> 'StreamStreamJoinBuilder':
        """Set the join window size."""
        self.window_size_seconds = window_size_seconds
        return self

    def build(self) -> StreamStreamJoin:
        """Build the join operation."""
        return StreamStreamJoin(
            self.left_stream,
            self.right_stream,
            self.join_type,
            self.conditions,
            self.window_size_seconds
        )


class IntervalJoinBuilder:
    """Builder for interval joins."""

    def __init__(self, app, left_stream, right_stream, join_type):
        self.app = app
        self.left_stream = left_stream
        self.right_stream = right_stream
        self.join_type = join_type
        self.conditions = []
        self.left_lower_bound = -5.0
        self.left_upper_bound = 5.0
        self.right_lower_bound = -5.0
        self.right_upper_bound = 5.0

    def on(self, left_field: str, right_field: str) -> 'IntervalJoinBuilder':
        """Add a join condition."""
        self.conditions.append({
            'left_field': left_field,
            'right_field': right_field
        })
        return self

    def between(
        self,
        left_lower: float,
        left_upper: float,
        right_lower: float,
        right_upper: float
    ) -> 'IntervalJoinBuilder':
        """Set time interval bounds for the join."""
        self.left_lower_bound = left_lower
        self.left_upper_bound = left_upper
        self.right_lower_bound = right_lower
        self.right_upper_bound = right_upper
        return self

    def build(self) -> IntervalJoin:
        """Build the join operation."""
        return IntervalJoin(
            self.left_stream,
            self.right_stream,
            self.join_type,
            self.conditions,
            self.left_lower_bound,
            self.left_upper_bound,
            self.right_lower_bound,
            self.right_upper_bound
        )


class TemporalJoinBuilder:
    """Builder for temporal table joins."""

    def __init__(self, app, stream, temporal_table, join_type):
        self.app = app
        self.stream = stream
        self.temporal_table = temporal_table
        self.join_type = join_type
        self.conditions = []

    def on(self, left_field: str, right_field: str) -> 'TemporalJoinBuilder':
        """Add a join condition."""
        self.conditions.append({
            'left_field': left_field,
            'right_field': right_field
        })
        return self

    def build(self) -> TemporalJoin:
        """Build the join operation."""
        return TemporalJoin(
            self.stream,
            self.temporal_table,
            self.join_type,
            self.conditions
        )


class LookupJoinBuilder:
    """Builder for lookup joins."""

    def __init__(self, app, stream, lookup_func, join_type):
        self.app = app
        self.stream = stream
        self.lookup_func = lookup_func
        self.join_type = join_type
        self.conditions = []
        self.cache_size = 1000

    def on(self, left_field: str, right_field: str) -> 'LookupJoinBuilder':
        """Add a join condition."""
        self.conditions.append({
            'left_field': left_field,
            'right_field': right_field
        })
        return self

    def cache(self, size: int) -> 'LookupJoinBuilder':
        """Set cache size for lookup results."""
        self.cache_size = size
        return self

    def build(self) -> LookupJoin:
        """Build the join operation."""
        return LookupJoin(
            self.stream,
            self.lookup_func,
            self.join_type,
            self.conditions,
            self.cache_size
        )


class WindowJoinBuilder:
    """Builder for window joins."""

    def __init__(self, app, left_stream, right_stream, join_type):
        self.app = app
        self.left_stream = left_stream
        self.right_stream = right_stream
        self.join_type = join_type
        self.conditions = []
        self.window_size = 300.0
        self.window_slide = 0.0

    def on(self, left_field: str, right_field: str) -> 'WindowJoinBuilder':
        """Add a join condition."""
        self.conditions.append({
            'left_field': left_field,
            'right_field': right_field
        })
        return self

    def tumbling_window(self, size_seconds: float) -> 'WindowJoinBuilder':
        """Set tumbling window size."""
        self.window_size = size_seconds
        self.window_slide = size_seconds  # Same as window size for tumbling
        return self

    def sliding_window(self, size_seconds: float, slide_seconds: float) -> 'WindowJoinBuilder':
        """Set sliding window parameters."""
        self.window_size = size_seconds
        self.window_slide = slide_seconds
        return self

    def build(self) -> WindowJoin:
        """Build the join operation."""
        return WindowJoin(
            self.left_stream,
            self.right_stream,
            self.join_type,
            self.conditions,
            self.window_size,
            self.window_slide
        )


class TableTableJoinBuilder:
    """Builder for table-table joins."""

    def __init__(self, app, left_table, right_table, join_type):
        self.app = app
        self.left_table = left_table
        self.right_table = right_table
        self.join_type = join_type
        self.conditions = []

    def on(self, left_field: str, right_field: str) -> 'TableTableJoinBuilder':
        """Add a join condition."""
        self.conditions.append({
            'left_field': left_field,
            'right_field': right_field
        })
        return self

    def build(self) -> TableTableJoin:
        """Build the join operation."""
        return TableTableJoin(
            self.left_table,
            self.right_table,
            self.join_type,
            self.conditions
        )


class ArrowTableJoinBuilder:
    """Builder for Arrow-native table joins."""

    def __init__(self, app, left_table, right_table, join_type):
        self.app = app
        self.left_table = left_table
        self.right_table = right_table
        self.join_type = join_type
        self.conditions = []

    def on(self, left_field: str, right_field: str) -> 'ArrowTableJoinBuilder':
        """Add a join condition."""
        self.conditions.append({
            'left_field': left_field,
            'right_field': right_field
        })
        return self

    def build(self) -> ArrowTableJoin:
        """Build the Arrow table join operation."""
        return ArrowTableJoin(
            self.left_table,
            self.right_table,
            self.join_type,
            self.conditions
        )


class ArrowDatasetJoinBuilder:
    """Builder for Arrow-native dataset joins."""

    def __init__(self, app, left_dataset, right_dataset, join_type):
        self.app = app
        self.left_dataset = left_dataset
        self.right_dataset = right_dataset
        self.join_type = join_type
        self.conditions = []

    def on(self, left_field: str, right_field: str) -> 'ArrowDatasetJoinBuilder':
        """Add a join condition."""
        self.conditions.append({
            'left_field': left_field,
            'right_field': right_field
        })
        return self

    def build(self) -> ArrowDatasetJoin:
        """Build the Arrow dataset join operation."""
        return ArrowDatasetJoin(
            self.left_dataset,
            self.right_dataset,
            self.join_type,
            self.conditions
        )


class ArrowAsofJoinBuilder:
    """Builder for Arrow-native as-of joins."""

    def __init__(self, app, left_dataset, right_dataset, left_key, right_key):
        self.app = app
        self.left_dataset = left_dataset
        self.right_dataset = right_dataset
        self.left_key = left_key
        self.right_key = right_key
        self.left_by_key = None
        self.right_by_key = None
        self.tolerance = 0.0

    def by(self, left_by_key: str, right_by_key: str = None) -> 'ArrowAsofJoinBuilder':
        """Set the 'by' keys for grouping as-of joins."""
        self.left_by_key = left_by_key
        self.right_by_key = right_by_key or left_by_key
        return self

    def within(self, tolerance: float) -> 'ArrowAsofJoinBuilder':
        """Set tolerance for as-of join matching."""
        self.tolerance = tolerance
        return self

    def build(self) -> ArrowAsofJoin:
        """Build the Arrow as-of join operation."""
        return ArrowAsofJoin(
            self.left_dataset,
            self.right_dataset,
            self.left_key,
            self.right_key,
            self.left_by_key,
            self.right_by_key,
            self.tolerance
        )


# Integration with Sabot App
def create_join_builder(app) -> JoinBuilder:
    """Create a join builder for the given app."""
    return JoinBuilder(app)
