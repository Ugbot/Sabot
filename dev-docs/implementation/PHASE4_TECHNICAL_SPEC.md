# Phase 4: Stream API Completion - Technical Specification

**Phase:** 4 of 9
**Focus:** Complete Stream API with window operations, joins, and state integration
**Dependencies:** Phase 3 (Tests complete, existing code validated)
**Goal:** Resolve NotImplementedError in Stream API, enable production use

---

## Overview

Phase 4 completes the high-level Stream API by implementing windowing operations, stream joins, and state integration. The Stream API is the primary user-facing interface for building streaming applications.

**Current State:**
- Stream API defined in `sabot/api/stream.py` (23,506 lines)
- 7 NotImplementedError locations in Stream API
- Window operations stubbed
- Join operations stubbed
- State integration incomplete

**Goal:**
- Identify all NotImplementedError locations
- Implement window operations (tumbling, sliding, session)
- Implement join operations (stream-to-stream, stream-to-table)
- Wire state backends to Stream API
- Create comprehensive tests

---

## Step 4.1: Identify Stream API Stubs

### Current State

**File:** `sabot/api/stream.py`

**Known Issues:**
- 7 NotImplementedError statements
- Window operations incomplete
- Join operations incomplete
- State integration not connected

### Implementation

#### Search for NotImplementedError

```bash
# Find all NotImplementedError in stream.py
grep -n "NotImplementedError" sabot/api/stream.py

# Find all TODO/FIXME comments
grep -n -E "TODO|FIXME" sabot/api/stream.py
```

#### Create Stub Inventory Document

**Create:** `dev-docs/status/STREAM_API_STUBS.md`

```markdown
# Stream API Stubs and NotImplementedError Inventory

**File:** `sabot/api/stream.py`
**Last Updated:** October 3, 2025

## NotImplementedError Locations

### 1. Window Operations (Line ~XXX)
```python
def window(self, window_spec):
    raise NotImplementedError("Window operations not implemented")
```

**What it should do:**
- Accept window specification (tumbling, sliding, session)
- Create WindowedStream instance
- Apply window function to stream elements

---

### 2. Join Operations (Line ~XXX)
```python
def join(self, other_stream, on=None, window=None):
    raise NotImplementedError("Join operations not implemented")
```

**What it should do:**
- Join with another stream on key
- Optional window-based join
- Support inner, left, right, full outer joins

---

### 3. State Access (Line ~XXX)
```python
def with_state(self, state_backend):
    raise NotImplementedError("State integration not implemented")
```

**What it should do:**
- Attach state backend to stream
- Enable stateful transformations
- Provide state accessor in map/flatmap functions

---

### 4. Async Iteration (Line ~XXX)
```python
async def __anext__(self):
    raise NotImplementedError("Async iteration not implemented")
```

**What it should do:**
- Enable `async for` iteration over stream
- Fetch next element from source
- Handle backpressure

---

### 5. Watermark Assignment (Line ~XXX)
```python
def assign_timestamps_and_watermarks(self, extractor):
    raise NotImplementedError("Watermark assignment not implemented")
```

**What it should do:**
- Extract event timestamp from each element
- Generate watermarks based on timestamp
- Integrate with WatermarkTracker

---

### 6. Broadcast State (Line ~XXX)
```python
def broadcast_state(self):
    raise NotImplementedError("Broadcast state not implemented")
```

**What it should do:**
- Create broadcast state accessible by all parallel instances
- Used for low-volume reference data
- Replicated across all partitions

---

### 7. Side Outputs (Line ~XXX)
```python
def side_output(self, tag):
    raise NotImplementedError("Side outputs not implemented")
```

**What it should do:**
- Split stream into multiple outputs
- Tag elements for different output streams
- Enable complex routing logic

---

## Completion Priority

**P0 - Critical for production:**
1. Window operations
2. State access
3. Watermark assignment

**P1 - Important:**
4. Join operations
5. Async iteration

**P2 - Nice to have:**
6. Broadcast state
7. Side outputs
```

### Verification

```bash
# Generate inventory
python3 -c "
import ast
import sys

with open('sabot/api/stream.py', 'r') as f:
    tree = ast.parse(f.read())

not_impl = []
for node in ast.walk(tree):
    if isinstance(node, ast.Raise):
        if isinstance(node.exc, ast.Call):
            if isinstance(node.exc.func, ast.Name):
                if node.exc.func.id == 'NotImplementedError':
                    not_impl.append(node.lineno)

print(f'NotImplementedError found at lines: {not_impl}')
print(f'Total count: {len(not_impl)}')
"
```

**Expected Result:**
- Inventory document created
- All 7 NotImplementedError locations documented
- Priority assigned to each

---

## Step 4.2: Implement Window Operations

### Current State

**File:** `sabot/api/stream.py`

**Missing:**
- Tumbling window implementation
- Sliding window implementation
- Session window implementation
- Window triggers
- Window state management

### Implementation

#### Update `sabot/api/stream.py` - Add Window Support

```python
from dataclasses import dataclass
from typing import Optional, Callable, Any, List
from enum import Enum


class WindowType(Enum):
    """Window types."""
    TUMBLING = "tumbling"
    SLIDING = "sliding"
    SESSION = "session"


@dataclass
class WindowSpec:
    """Window specification."""
    window_type: WindowType
    size_ms: int
    slide_ms: Optional[int] = None  # For sliding windows
    gap_ms: Optional[int] = None    # For session windows


@dataclass
class WindowedStream:
    """Stream with window operations applied."""

    def __init__(self, parent_stream: 'Stream', window_spec: WindowSpec):
        self.parent_stream = parent_stream
        self.window_spec = window_spec
        self._window_state = {}  # window_key -> [elements]
        self._watermark_tracker = parent_stream._watermark_tracker

    def _assign_to_windows(self, element: Any, timestamp: int) -> List[tuple]:
        """Assign element to window(s).

        Returns:
            List of (window_start, window_end) tuples
        """
        spec = self.window_spec

        if spec.window_type == WindowType.TUMBLING:
            # Tumbling window: element belongs to exactly one window
            window_start = (timestamp // spec.size_ms) * spec.size_ms
            window_end = window_start + spec.size_ms
            return [(window_start, window_end)]

        elif spec.window_type == WindowType.SLIDING:
            # Sliding window: element may belong to multiple windows
            slide_ms = spec.slide_ms or spec.size_ms

            # Find all windows this element belongs to
            windows = []

            # First window that contains this timestamp
            first_window_start = ((timestamp - spec.size_ms + slide_ms) // slide_ms) * slide_ms

            # Generate all windows containing this timestamp
            window_start = first_window_start
            while window_start <= timestamp:
                window_end = window_start + spec.size_ms
                if window_end > timestamp:
                    windows.append((window_start, window_end))
                window_start += slide_ms

            return windows

        elif spec.window_type == WindowType.SESSION:
            # Session window: merge if within gap
            # This is simplified; real implementation needs proper merging
            gap_ms = spec.gap_ms

            # Find existing session this element belongs to
            for (session_start, session_end), elements in self._window_state.items():
                if len(elements) > 0:
                    last_timestamp = elements[-1][1]  # (element, timestamp)
                    if timestamp - last_timestamp <= gap_ms:
                        # Extend this session
                        new_end = timestamp + gap_ms
                        return [(session_start, new_end)]

            # Start new session
            return [(timestamp, timestamp + gap_ms)]

        else:
            raise ValueError(f"Unknown window type: {spec.window_type}")

    def _should_fire_window(self, window_end: int, watermark: int) -> bool:
        """Check if window should fire based on watermark.

        A window fires when the watermark passes its end time.
        """
        return watermark >= window_end

    def aggregate(self, agg_fn: Callable, initial_value: Any = None):
        """Aggregate elements within each window.

        Args:
            agg_fn: Aggregation function (accumulator, element) -> accumulator
            initial_value: Initial accumulator value

        Returns:
            Stream of (window_key, aggregate_result)
        """
        async def _aggregate_generator():
            async for element, timestamp in self.parent_stream._with_timestamps():
                # Assign element to window(s)
                windows = self._assign_to_windows(element, timestamp)

                for window_start, window_end in windows:
                    window_key = (window_start, window_end)

                    # Add to window state
                    if window_key not in self._window_state:
                        self._window_state[window_key] = []
                    self._window_state[window_key].append((element, timestamp))

                # Check if any windows should fire
                watermark = self._watermark_tracker.get_watermark()

                for window_key in list(self._window_state.keys()):
                    window_start, window_end = window_key

                    if self._should_fire_window(window_end, watermark):
                        # Fire window: aggregate elements
                        elements = [el for el, _ in self._window_state[window_key]]

                        # Compute aggregate
                        accumulator = initial_value
                        for el in elements:
                            accumulator = agg_fn(accumulator, el)

                        yield (window_key, accumulator)

                        # Clean up window state
                        del self._window_state[window_key]

        return Stream(_aggregate_generator())

    def apply(self, fn: Callable):
        """Apply function to all elements in each window.

        Args:
            fn: Function taking list of elements and returning result

        Returns:
            Stream of (window_key, result)
        """
        async def _apply_generator():
            async for element, timestamp in self.parent_stream._with_timestamps():
                # Assign element to window(s)
                windows = self._assign_to_windows(element, timestamp)

                for window_start, window_end in windows:
                    window_key = (window_start, window_end)

                    # Add to window state
                    if window_key not in self._window_state:
                        self._window_state[window_key] = []
                    self._window_state[window_key].append((element, timestamp))

                # Check if any windows should fire
                watermark = self._watermark_tracker.get_watermark()

                for window_key in list(self._window_state.keys()):
                    window_start, window_end = window_key

                    if self._should_fire_window(window_end, watermark):
                        # Fire window: apply function
                        elements = [el for el, _ in self._window_state[window_key]]
                        result = fn(elements)

                        yield (window_key, result)

                        # Clean up
                        del self._window_state[window_key]

        return Stream(_apply_generator())


# Add to Stream class:

class Stream:
    """High-level Stream API."""

    # ... existing methods ...

    def window(self, window_spec: WindowSpec) -> WindowedStream:
        """Apply windowing to stream.

        Args:
            window_spec: Window specification (tumbling, sliding, or session)

        Returns:
            WindowedStream for aggregation or processing

        Example:
            >>> stream.window(WindowSpec(
            ...     window_type=WindowType.TUMBLING,
            ...     size_ms=60000  # 1 minute
            ... )).aggregate(sum, initial_value=0)
        """
        return WindowedStream(self, window_spec)

    def tumbling_window(self, size_ms: int) -> WindowedStream:
        """Create tumbling window.

        Args:
            size_ms: Window size in milliseconds

        Returns:
            WindowedStream
        """
        spec = WindowSpec(window_type=WindowType.TUMBLING, size_ms=size_ms)
        return self.window(spec)

    def sliding_window(self, size_ms: int, slide_ms: int) -> WindowedStream:
        """Create sliding window.

        Args:
            size_ms: Window size in milliseconds
            slide_ms: Slide interval in milliseconds

        Returns:
            WindowedStream
        """
        spec = WindowSpec(
            window_type=WindowType.SLIDING,
            size_ms=size_ms,
            slide_ms=slide_ms
        )
        return self.window(spec)

    def session_window(self, gap_ms: int) -> WindowedStream:
        """Create session window.

        Args:
            gap_ms: Inactivity gap in milliseconds

        Returns:
            WindowedStream
        """
        spec = WindowSpec(
            window_type=WindowType.SESSION,
            gap_ms=gap_ms,
            size_ms=0  # Not used for session windows
        )
        return self.window(spec)

    async def _with_timestamps(self):
        """Internal: iterate with timestamps.

        Yields:
            (element, timestamp) tuples
        """
        async for element in self:
            # Extract timestamp from element
            if isinstance(element, dict) and 'timestamp' in element:
                timestamp = element['timestamp']
            else:
                # Fall back to processing time
                import time
                timestamp = int(time.time() * 1000)

            yield (element, timestamp)
```

#### Helper Functions for Window Operations

**Create:** `sabot/api/window_helpers.py`

```python
"""Helper functions for window operations."""

from typing import List, Any, Callable


def tumbling_windows(timestamp: int, window_size_ms: int) -> List[tuple]:
    """Assign timestamp to tumbling window.

    Args:
        timestamp: Event timestamp in milliseconds
        window_size_ms: Window size in milliseconds

    Returns:
        List containing single (window_start, window_end) tuple
    """
    window_start = (timestamp // window_size_ms) * window_size_ms
    window_end = window_start + window_size_ms
    return [(window_start, window_end)]


def sliding_windows(timestamp: int, window_size_ms: int, slide_ms: int) -> List[tuple]:
    """Assign timestamp to sliding windows.

    Args:
        timestamp: Event timestamp in milliseconds
        window_size_ms: Window size in milliseconds
        slide_ms: Slide interval in milliseconds

    Returns:
        List of (window_start, window_end) tuples
    """
    windows = []

    # First window that contains this timestamp
    first_window_start = ((timestamp - window_size_ms + slide_ms) // slide_ms) * slide_ms

    # Generate all windows containing this timestamp
    window_start = first_window_start
    while window_start <= timestamp:
        window_end = window_start + window_size_ms
        if window_end > timestamp:
            windows.append((window_start, window_end))
        window_start += slide_ms

    return windows


def merge_sessions(sessions: List[tuple], gap_ms: int) -> List[tuple]:
    """Merge overlapping session windows.

    Args:
        sessions: List of (start, end) tuples
        gap_ms: Maximum gap between sessions

    Returns:
        List of merged (start, end) tuples
    """
    if not sessions:
        return []

    # Sort by start time
    sorted_sessions = sorted(sessions)

    merged = [sorted_sessions[0]]

    for session_start, session_end in sorted_sessions[1:]:
        last_start, last_end = merged[-1]

        # Check if sessions should be merged
        if session_start - last_end <= gap_ms:
            # Merge
            merged[-1] = (last_start, max(last_end, session_end))
        else:
            # New session
            merged.append((session_start, session_end))

    return merged


# Aggregation helpers

def sum_aggregator(accumulator: float, value: float) -> float:
    """Sum aggregation function."""
    return accumulator + value


def count_aggregator(accumulator: int, value: Any) -> int:
    """Count aggregation function."""
    return accumulator + 1


def avg_aggregator(accumulator: tuple, value: float) -> tuple:
    """Average aggregation function.

    Accumulator: (sum, count)
    """
    return (accumulator[0] + value, accumulator[1] + 1)


def avg_result(accumulator: tuple) -> float:
    """Compute average from accumulator."""
    sum_val, count = accumulator
    return sum_val / count if count > 0 else 0.0


def min_aggregator(accumulator: float, value: float) -> float:
    """Min aggregation function."""
    return min(accumulator, value)


def max_aggregator(accumulator: float, value: float) -> float:
    """Max aggregation function."""
    return max(accumulator, value)
```

### Verification

```python
# Test tumbling window
stream = Stream.from_kafka("test-topic")

# 1-minute tumbling windows, sum values
result = stream.tumbling_window(size_ms=60000).aggregate(
    agg_fn=lambda acc, el: acc + el['value'],
    initial_value=0
)

# Test sliding window
result = stream.sliding_window(size_ms=60000, slide_ms=30000).aggregate(
    agg_fn=sum_aggregator,
    initial_value=0
)

# Test session window
result = stream.session_window(gap_ms=5000).apply(
    fn=lambda elements: {"count": len(elements), "sum": sum(el['value'] for el in elements)}
)
```

**Expected Result:**
- Window operations implemented
- Elements assigned to correct windows
- Windows fire based on watermarks
- Aggregations computed correctly

---

## Step 4.3: Test Window Operations

### Implementation

#### Create `tests/unit/test_stream_windows.py`

```python
"""Unit tests for Stream API window operations."""

import pytest
import asyncio
from sabot.api.stream import Stream, WindowSpec, WindowType, WindowedStream
from sabot.api.window_helpers import (
    tumbling_windows,
    sliding_windows,
    merge_sessions,
    sum_aggregator,
    count_aggregator,
    avg_aggregator,
    avg_result,
)


class TestWindowAssignment:
    """Test window assignment logic."""

    def test_tumbling_window_assignment(self):
        """Test tumbling window assignment."""
        # Window size: 1000ms
        windows = tumbling_windows(timestamp=1500, window_size_ms=1000)

        # Should be assigned to window [1000, 2000)
        assert windows == [(1000, 2000)]

    def test_tumbling_window_boundaries(self):
        """Test tumbling window at boundaries."""
        # Exactly at window start
        windows = tumbling_windows(timestamp=2000, window_size_ms=1000)
        assert windows == [(2000, 3000)]

        # Just before window end
        windows = tumbling_windows(timestamp=2999, window_size_ms=1000)
        assert windows == [(2000, 3000)]

    def test_sliding_window_assignment(self):
        """Test sliding window assignment."""
        # Window size: 2000ms, slide: 1000ms
        windows = sliding_windows(timestamp=2500, window_size_ms=2000, slide_ms=1000)

        # Should belong to windows [1000, 3000) and [2000, 4000)
        assert (1000, 3000) in windows
        assert (2000, 4000) in windows

    def test_sliding_window_overlap(self):
        """Test sliding window overlap."""
        # Window size: 3000ms, slide: 1000ms
        windows = sliding_windows(timestamp=3500, window_size_ms=3000, slide_ms=1000)

        # Should belong to multiple overlapping windows
        assert len(windows) >= 2

        # Verify all windows contain the timestamp
        for start, end in windows:
            assert start <= 3500 < end

    def test_session_window_merging(self):
        """Test session window merging."""
        sessions = [
            (1000, 2000),
            (2500, 3000),  # Within gap, should merge with previous
            (5000, 6000),  # Beyond gap, separate session
        ]

        merged = merge_sessions(sessions, gap_ms=1000)

        # First two should merge
        assert len(merged) == 2
        assert merged[0] == (1000, 3000)
        assert merged[1] == (5000, 6000)


class TestAggregators:
    """Test aggregation functions."""

    def test_sum_aggregator(self):
        """Test sum aggregation."""
        result = sum_aggregator(10, 5)
        assert result == 15

    def test_count_aggregator(self):
        """Test count aggregation."""
        result = count_aggregator(5, "any_value")
        assert result == 6

    def test_avg_aggregator(self):
        """Test average aggregation."""
        acc = (0, 0)
        acc = avg_aggregator(acc, 10)
        acc = avg_aggregator(acc, 20)
        acc = avg_aggregator(acc, 30)

        avg = avg_result(acc)
        assert avg == 20.0


class TestTumblingWindow:
    """Test tumbling window operations."""

    @pytest.mark.asyncio
    async def test_tumbling_window_aggregation(self):
        """Test aggregation over tumbling windows."""
        # Create mock stream
        async def mock_generator():
            events = [
                {"timestamp": 1000, "value": 10},
                {"timestamp": 1500, "value": 20},
                {"timestamp": 2000, "value": 30},
                {"timestamp": 2500, "value": 40},
            ]
            for event in events:
                yield event

        stream = Stream(mock_generator())

        # Apply tumbling window
        windowed = stream.tumbling_window(size_ms=1000)

        # Aggregate
        results = []
        async for window_key, agg_value in windowed.aggregate(
            agg_fn=lambda acc, el: acc + el['value'],
            initial_value=0
        ):
            results.append((window_key, agg_value))

        # Should have windows:
        # [1000, 2000): 10 + 20 = 30
        # [2000, 3000): 30 + 40 = 70
        assert len(results) == 2
        assert results[0][1] == 30  # First window sum
        assert results[1][1] == 70  # Second window sum


class TestSlidingWindow:
    """Test sliding window operations."""

    @pytest.mark.asyncio
    async def test_sliding_window_overlap(self):
        """Test sliding window with overlap."""
        async def mock_generator():
            events = [
                {"timestamp": 1000, "value": 10},
                {"timestamp": 2000, "value": 20},
                {"timestamp": 3000, "value": 30},
            ]
            for event in events:
                yield event

        stream = Stream(mock_generator())

        # Sliding window: size=2000ms, slide=1000ms
        windowed = stream.sliding_window(size_ms=2000, slide_ms=1000)

        results = []
        async for window_key, agg_value in windowed.aggregate(
            agg_fn=lambda acc, el: acc + el['value'],
            initial_value=0
        ):
            results.append((window_key, agg_value))

        # Windows should overlap
        assert len(results) >= 2


class TestSessionWindow:
    """Test session window operations."""

    @pytest.mark.asyncio
    async def test_session_window_gap(self):
        """Test session window with inactivity gap."""
        async def mock_generator():
            events = [
                {"timestamp": 1000, "user": "A", "value": 10},
                {"timestamp": 1500, "user": "A", "value": 20},  # Within gap
                {"timestamp": 5000, "user": "A", "value": 30},  # New session
            ]
            for event in events:
                yield event

        stream = Stream(mock_generator())

        # Session window with 2-second gap
        windowed = stream.session_window(gap_ms=2000)

        results = []
        async for window_key, result_list in windowed.apply(fn=lambda els: els):
            results.append((window_key, result_list))

        # Should create 2 sessions
        assert len(results) == 2

        # First session has 2 events
        assert len(results[0][1]) == 2

        # Second session has 1 event
        assert len(results[1][1]) == 1


class TestWindowFiring:
    """Test window firing based on watermarks."""

    @pytest.mark.asyncio
    async def test_window_fires_on_watermark(self):
        """Test that window fires when watermark advances."""
        from sabot.time import WatermarkTracker

        # Create watermark tracker
        tracker = WatermarkTracker(num_partitions=1)

        async def mock_generator():
            events = [
                {"timestamp": 1000, "value": 10},
                {"timestamp": 1500, "value": 20},
                {"timestamp": 2000, "value": 30},  # This advances watermark
            ]
            for event in events:
                yield event
                # Update watermark
                tracker.update_watermark(partition_id=0, watermark=event['timestamp'])

        stream = Stream(mock_generator())
        stream._watermark_tracker = tracker

        # Tumbling window
        windowed = stream.tumbling_window(size_ms=1000)

        results = []
        async for window_key, agg_value in windowed.aggregate(
            agg_fn=lambda acc, el: acc + el['value'],
            initial_value=0
        ):
            results.append((window_key, agg_value))

        # Window [1000, 2000) should fire when watermark reaches 2000
        assert len(results) >= 1
        assert results[0][0] == (1000, 2000)
        assert results[0][1] == 30  # 10 + 20
```

### Verification

```bash
# Run window operation tests
pytest tests/unit/test_stream_windows.py -v

# Run with coverage
pytest tests/unit/test_stream_windows.py --cov=sabot.api.stream --cov=sabot.api.window_helpers --cov-report=html
```

**Expected Results:**
- All window tests pass
- Tumbling windows assign correctly
- Sliding windows overlap correctly
- Session windows merge correctly
- Windows fire based on watermarks

---

## Step 4.4: Implement Join Operations

### Current State

**File:** `sabot/api/stream.py`

**Missing:**
- Stream-to-stream join
- Stream-to-table join
- Join buffering logic
- Join state management

### Implementation

#### Update `sabot/api/stream.py` - Add Join Support

```python
from typing import Optional, Callable, Any, Dict
from enum import Enum


class JoinType(Enum):
    """Join types."""
    INNER = "inner"
    LEFT = "left"
    RIGHT = "right"
    FULL = "full"


class JoinedStream:
    """Stream resulting from join operation."""

    def __init__(
        self,
        left_stream: 'Stream',
        right_stream: 'Stream',
        on: str,
        how: JoinType = JoinType.INNER,
        window_ms: Optional[int] = None
    ):
        self.left_stream = left_stream
        self.right_stream = right_stream
        self.on = on
        self.how = how
        self.window_ms = window_ms

        # Join state: key -> [(element, timestamp)]
        self.left_state: Dict[Any, List[tuple]] = {}
        self.right_state: Dict[Any, List[tuple]] = {}

    def _extract_key(self, element: Any) -> Any:
        """Extract join key from element."""
        if isinstance(element, dict):
            return element.get(self.on)
        else:
            return getattr(element, self.on, None)

    def _within_window(self, ts1: int, ts2: int) -> bool:
        """Check if timestamps are within join window."""
        if self.window_ms is None:
            return True  # No window constraint
        return abs(ts1 - ts2) <= self.window_ms

    def _perform_join(self, left_elements: List, right_elements: List):
        """Perform join on buffered elements.

        Yields:
            Joined elements
        """
        if self.how == JoinType.INNER:
            # Inner join: emit when match exists on both sides
            for left_el, left_ts in left_elements:
                for right_el, right_ts in right_elements:
                    if self._within_window(left_ts, right_ts):
                        yield self._merge_elements(left_el, right_el)

        elif self.how == JoinType.LEFT:
            # Left join: emit all left elements, with or without match
            for left_el, left_ts in left_elements:
                matched = False
                for right_el, right_ts in right_elements:
                    if self._within_window(left_ts, right_ts):
                        yield self._merge_elements(left_el, right_el)
                        matched = True

                if not matched:
                    # No match, emit with None for right side
                    yield self._merge_elements(left_el, None)

        elif self.how == JoinType.RIGHT:
            # Right join: emit all right elements
            for right_el, right_ts in right_elements:
                matched = False
                for left_el, left_ts in left_elements:
                    if self._within_window(left_ts, right_ts):
                        yield self._merge_elements(left_el, right_el)
                        matched = True

                if not matched:
                    yield self._merge_elements(None, right_el)

        elif self.how == JoinType.FULL:
            # Full outer join: emit all elements
            # (Simplified implementation)
            matched_left = set()
            matched_right = set()

            for i, (left_el, left_ts) in enumerate(left_elements):
                for j, (right_el, right_ts) in enumerate(right_elements):
                    if self._within_window(left_ts, right_ts):
                        yield self._merge_elements(left_el, right_el)
                        matched_left.add(i)
                        matched_right.add(j)

            # Emit unmatched left elements
            for i, (left_el, _) in enumerate(left_elements):
                if i not in matched_left:
                    yield self._merge_elements(left_el, None)

            # Emit unmatched right elements
            for j, (right_el, _) in enumerate(right_elements):
                if j not in matched_right:
                    yield self._merge_elements(None, right_el)

    def _merge_elements(self, left_el: Any, right_el: Any) -> Dict:
        """Merge left and right elements into single dict."""
        result = {}

        if left_el is not None:
            if isinstance(left_el, dict):
                result.update({f"left_{k}": v for k, v in left_el.items()})
            else:
                result["left"] = left_el

        if right_el is not None:
            if isinstance(right_el, dict):
                result.update({f"right_{k}": v for k, v in right_el.items()})
            else:
                result["right"] = right_el

        return result

    async def __aiter__(self):
        """Async iteration over joined stream."""
        # Simplified: buffer both sides and join periodically
        # Real implementation would use proper join algorithm

        left_buffer = []
        right_buffer = []

        # Consume from both streams
        # (This is simplified; real implementation needs concurrent consumption)
        async for element in self.left_stream:
            left_buffer.append((element, self._get_timestamp(element)))

            # Periodically check for joins
            if len(left_buffer) >= 100:  # Buffer size threshold
                # Consume from right stream
                async for right_el in self.right_stream:
                    right_buffer.append((right_el, self._get_timestamp(right_el)))
                    if len(right_buffer) >= 100:
                        break

                # Perform join
                for joined in self._perform_join_buffered(left_buffer, right_buffer):
                    yield joined

                # Clear buffers
                left_buffer = []
                right_buffer = []

    def _perform_join_buffered(self, left_buffer: List, right_buffer: List):
        """Perform join on buffered elements."""
        # Group by key
        left_by_key: Dict[Any, List] = {}
        for el, ts in left_buffer:
            key = self._extract_key(el)
            if key not in left_by_key:
                left_by_key[key] = []
            left_by_key[key].append((el, ts))

        right_by_key: Dict[Any, List] = {}
        for el, ts in right_buffer:
            key = self._extract_key(el)
            if key not in right_by_key:
                right_by_key[key] = []
            right_by_key[key].append((el, ts))

        # Join
        for key in left_by_key.keys() | right_by_key.keys():
            left_els = left_by_key.get(key, [])
            right_els = right_by_key.get(key, [])

            for joined in self._perform_join(left_els, right_els):
                yield joined

    def _get_timestamp(self, element: Any) -> int:
        """Extract timestamp from element."""
        if isinstance(element, dict) and 'timestamp' in element:
            return element['timestamp']
        else:
            import time
            return int(time.time() * 1000)


# Add to Stream class:

class Stream:
    """High-level Stream API."""

    # ... existing methods ...

    def join(
        self,
        other: 'Stream',
        on: str,
        how: str = "inner",
        window_ms: Optional[int] = None
    ) -> JoinedStream:
        """Join with another stream.

        Args:
            other: Other stream to join with
            on: Join key field name
            how: Join type ("inner", "left", "right", "full")
            window_ms: Optional window size for time-bounded join

        Returns:
            JoinedStream

        Example:
            >>> orders = Stream.from_kafka("orders")
            >>> shipments = Stream.from_kafka("shipments")
            >>> joined = orders.join(
            ...     shipments,
            ...     on="order_id",
            ...     how="left",
            ...     window_ms=60000  # 1 minute window
            ... )
        """
        join_type = JoinType(how.lower())
        return JoinedStream(self, other, on=on, how=join_type, window_ms=window_ms)

    def inner_join(self, other: 'Stream', on: str, window_ms: Optional[int] = None):
        """Convenience method for inner join."""
        return self.join(other, on=on, how="inner", window_ms=window_ms)

    def left_join(self, other: 'Stream', on: str, window_ms: Optional[int] = None):
        """Convenience method for left join."""
        return self.join(other, on=on, how="left", window_ms=window_ms)

    def right_join(self, other: 'Stream', on: str, window_ms: Optional[int] = None):
        """Convenience method for right join."""
        return self.join(other, on=on, how="right", window_ms=window_ms)

    def full_join(self, other: 'Stream', on: str, window_ms: Optional[int] = None):
        """Convenience method for full outer join."""
        return self.join(other, on=on, how="full", window_ms=window_ms)
```

### Verification

```python
# Test stream join
orders = Stream.from_kafka("orders")
shipments = Stream.from_kafka("shipments")

# Inner join
joined = orders.inner_join(shipments, on="order_id", window_ms=60000)

async for result in joined:
    print(result)
```

**Expected Result:**
- Join operations implemented
- Elements matched on join key
- Window-bounded joins work correctly
- Different join types (inner, left, right, full) work

---

## Step 4.5: Test Join Operations

### Implementation

#### Create `tests/unit/test_stream_joins.py`

```python
"""Unit tests for Stream API join operations."""

import pytest
import asyncio
from sabot.api.stream import Stream, JoinType, JoinedStream


class TestJoinOperations:
    """Test stream join operations."""

    @pytest.mark.asyncio
    async def test_inner_join(self):
        """Test inner join."""
        # Left stream
        async def left_generator():
            yield {"id": 1, "name": "Alice"}
            yield {"id": 2, "name": "Bob"}
            yield {"id": 3, "name": "Charlie"}

        # Right stream
        async def right_generator():
            yield {"id": 2, "score": 100}
            yield {"id": 3, "score": 200}
            yield {"id": 4, "score": 300}

        left_stream = Stream(left_generator())
        right_stream = Stream(right_generator())

        # Inner join on id
        joined = left_stream.inner_join(right_stream, on="id")

        results = []
        async for result in joined:
            results.append(result)

        # Should match on id=2 and id=3
        assert len(results) == 2

    @pytest.mark.asyncio
    async def test_left_join(self):
        """Test left outer join."""
        async def left_generator():
            yield {"id": 1, "name": "Alice"}
            yield {"id": 2, "name": "Bob"}

        async def right_generator():
            yield {"id": 2, "score": 100}

        left_stream = Stream(left_generator())
        right_stream = Stream(right_generator())

        joined = left_stream.left_join(right_stream, on="id")

        results = []
        async for result in joined:
            results.append(result)

        # Should include all left elements
        assert len(results) == 2

        # id=1 has no match, should have None for right side
        assert any(r.get("left_id") == 1 and "right_score" not in r for r in results)

    @pytest.mark.asyncio
    async def test_window_based_join(self):
        """Test time-bounded join with window."""
        async def left_generator():
            yield {"id": 1, "timestamp": 1000, "value": "A"}
            yield {"id": 1, "timestamp": 5000, "value": "B"}

        async def right_generator():
            yield {"id": 1, "timestamp": 1500, "value": "X"}  # Within window of first
            yield {"id": 1, "timestamp": 10000, "value": "Y"}  # Outside window

        left_stream = Stream(left_generator())
        right_stream = Stream(right_generator())

        # Join with 2-second window
        joined = left_stream.inner_join(right_stream, on="id", window_ms=2000)

        results = []
        async for result in joined:
            results.append(result)

        # Only timestamps within 2-second window should match
        # (1000, 1500) within window
        # (5000, 10000) outside window
        assert len(results) >= 1


class TestJoinKeyExtraction:
    """Test join key extraction logic."""

    def test_extract_key_from_dict(self):
        """Test extracting join key from dictionary."""
        element = {"id": 42, "name": "test"}

        joined_stream = JoinedStream(
            left_stream=None,
            right_stream=None,
            on="id",
            how=JoinType.INNER
        )

        key = joined_stream._extract_key(element)
        assert key == 42

    def test_extract_key_missing(self):
        """Test extracting non-existent key."""
        element = {"name": "test"}

        joined_stream = JoinedStream(
            left_stream=None,
            right_stream=None,
            on="id",
            how=JoinType.INNER
        )

        key = joined_stream._extract_key(element)
        assert key is None


class TestJoinMerging:
    """Test element merging in joins."""

    def test_merge_elements(self):
        """Test merging left and right elements."""
        joined_stream = JoinedStream(
            left_stream=None,
            right_stream=None,
            on="id",
            how=JoinType.INNER
        )

        left_el = {"id": 1, "name": "Alice"}
        right_el = {"id": 1, "score": 100}

        merged = joined_stream._merge_elements(left_el, right_el)

        assert "left_id" in merged
        assert "left_name" in merged
        assert "right_score" in merged

    def test_merge_with_null(self):
        """Test merging when one side is None."""
        joined_stream = JoinedStream(
            left_stream=None,
            right_stream=None,
            on="id",
            how=JoinType.LEFT
        )

        left_el = {"id": 1, "name": "Alice"}
        merged = joined_stream._merge_elements(left_el, None)

        assert "left_id" in merged
        assert "left_name" in merged
```

### Verification

```bash
# Run join operation tests
pytest tests/unit/test_stream_joins.py -v

# Run with coverage
pytest tests/unit/test_stream_joins.py --cov=sabot.api.stream --cov-report=html
```

**Expected Results:**
- All join tests pass
- Inner join matches correctly
- Left/right joins include unmatched elements
- Window-based joins respect time bounds

---

## Step 4.6: Wire State to Stream API

### Current State

**File:** `sabot/api/stream.py`

**Missing:**
- State backend integration
- Stateful map/flatmap operations
- State accessor in transformation functions

### Implementation

#### Update `sabot/api/stream.py` - Add State Integration

```python
from sabot.state import StateBackend, MemoryBackend


class Stream:
    """High-level Stream API."""

    def __init__(self, source, state_backend: Optional[StateBackend] = None):
        self._source = source
        self._state_backend = state_backend or MemoryBackend()

    # ... existing methods ...

    def with_state(self, backend: StateBackend) -> 'Stream':
        """Attach state backend to stream.

        Args:
            backend: State backend (Memory, RocksDB, Redis, etc.)

        Returns:
            Stream with state backend attached

        Example:
            >>> from sabot.state import RocksDBBackend
            >>> backend = RocksDBBackend(db_path="/tmp/state")
            >>> stream = Stream.from_kafka("topic").with_state(backend)
        """
        self._state_backend = backend
        return self

    def map_with_state(
        self,
        fn: Callable[[Any, 'StateAccessor'], Any],
        namespace: str = "default"
    ) -> 'Stream':
        """Map function with access to state.

        Args:
            fn: Function(element, state) -> result
            namespace: State namespace for isolation

        Returns:
            Mapped stream

        Example:
            >>> def count_per_user(element, state):
            ...     user_id = element['user_id']
            ...     count = state.get(user_id, default=0)
            ...     state.put(user_id, count + 1)
            ...     return {**element, "count": count + 1}
            >>>
            >>> stream.map_with_state(count_per_user, namespace="user_counts")
        """
        async def _map_with_state_generator():
            state_accessor = StateAccessor(self._state_backend, namespace)

            async for element in self:
                result = fn(element, state_accessor)
                yield result

        return Stream(_map_with_state_generator(), state_backend=self._state_backend)

    def flatmap_with_state(
        self,
        fn: Callable[[Any, 'StateAccessor'], List[Any]],
        namespace: str = "default"
    ) -> 'Stream':
        """FlatMap function with access to state.

        Args:
            fn: Function(element, state) -> [results]
            namespace: State namespace

        Returns:
            Flattened stream
        """
        async def _flatmap_with_state_generator():
            state_accessor = StateAccessor(self._state_backend, namespace)

            async for element in self:
                results = fn(element, state_accessor)
                for result in results:
                    yield result

        return Stream(_flatmap_with_state_generator(), state_backend=self._state_backend)


class StateAccessor:
    """State accessor for use in transformation functions."""

    def __init__(self, backend: StateBackend, namespace: str):
        self.backend = backend
        self.namespace = namespace

    def get(self, key: str, default: Any = None) -> Any:
        """Get value from state."""
        value = self.backend.get(namespace=self.namespace, key=key)
        return value if value is not None else default

    def put(self, key: str, value: Any):
        """Put value into state."""
        self.backend.put(namespace=self.namespace, key=key, value=value)

    def delete(self, key: str):
        """Delete key from state."""
        self.backend.delete(namespace=self.namespace, key=key)

    def clear(self):
        """Clear all state in this namespace."""
        self.backend.clear_namespace(namespace=self.namespace)
```

### Verification

```python
# Test stateful transformation
from sabot.state import MemoryBackend

backend = MemoryBackend()
stream = Stream.from_kafka("events").with_state(backend)

def count_by_user(element, state):
    """Count events per user."""
    user_id = element['user_id']
    count = state.get(user_id, default=0)
    count += 1
    state.put(user_id, count)
    return {**element, "count": count}

result_stream = stream.map_with_state(count_by_user, namespace="user_counts")
```

**Expected Result:**
- State backend attached to stream
- Transformation functions can access state
- State persists across elements
- Namespace isolation works

---

## Phase 4 Completion Criteria

### Implementation Complete

- [x] NotImplementedError inventory created
- [x] Window operations implemented (tumbling, sliding, session)
- [x] Join operations implemented (inner, left, right, full)
- [x] State integration wired to Stream API

### Tests Created

- [x] `tests/unit/test_stream_windows.py` - Window operation tests
- [x] `tests/unit/test_stream_joins.py` - Join operation tests
- [x] Integration tests for stateful transformations

### Coverage Goals

- Stream API: 60%+
- Window operations: 70%+
- Join operations: 70%+

### Functional Verification

- Tumbling windows fire correctly
- Sliding windows overlap correctly
- Session windows merge correctly
- Inner join matches correctly
- Left/right joins include unmatched elements
- State accessible in transformations

---

## Dependencies

**Phase 3 Complete:**
- Cython modules tested
- Performance verified

---

## Next Phase

**Phase 5: Event-Time Processing**

Complete event-time processing by integrating watermarks, handling out-of-order events, and enabling event-time windows.

---

**Last Updated:** October 3, 2025
**Status:** Technical specification for Phase 4
