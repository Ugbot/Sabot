# -*- coding: utf-8 -*-
"""Arrow-focused windowing system for Sabot streams."""

import asyncio
from typing import Any, Dict, List, Optional, Union, Callable, AsyncIterator
from dataclasses import dataclass
from enum import Enum

# Try to import Cython implementations
try:
    from ._cython.windows import (
        WindowManager,
        get_window_manager,
        create_window_processor,
        WindowType as CythonWindowType,
        WindowConfig
    )
    CYTHON_AVAILABLE = True
except ImportError:
    CYTHON_AVAILABLE = False
    WindowManager = None
    get_window_manager = None
    create_window_processor = None
    CythonWindowType = None
    WindowConfig = None

# Try to import Arrow implementations
try:
    from .windows_arrow import ArrowWindowProcessor
    ARROW_AVAILABLE = True
except ImportError:
    ARROW_AVAILABLE = False
    ArrowWindowProcessor = None


class WindowType(Enum):
    """Window types supported by Sabot."""
    TUMBLING = "tumbling"
    SLIDING = "sliding"
    HOPPING = "hopping"
    SESSION = "session"


@dataclass
class WindowSpec:
    """Specification for a window operation."""
    name: str
    window_type: WindowType
    size_seconds: float
    slide_seconds: Optional[float] = None
    hop_seconds: Optional[float] = None
    timeout_seconds: Optional[float] = None
    key_field: str = "key"
    timestamp_field: str = "timestamp"
    aggregations: Optional[Dict[str, str]] = None
    emit_empty_windows: bool = False


class WindowProcessor:
    """Window processor with Arrow integration and multiple backends."""

    def __init__(self, spec: WindowSpec):
        self.spec = spec
        self._cython_processor = None
        self._arrow_processor = None
        self._python_fallback = None

        # Priority: Cython > Arrow > Python
        if CYTHON_AVAILABLE:
            try:
                # Convert to Cython config
                config_kwargs = {
                    'key_field': spec.key_field,
                    'timestamp_field': spec.timestamp_field,
                    'emit_empty_windows': spec.emit_empty_windows,
                }

                if spec.slide_seconds is not None:
                    config_kwargs['slide_seconds'] = spec.slide_seconds
                if spec.hop_seconds is not None:
                    config_kwargs['hop_seconds'] = spec.hop_seconds
                if spec.timeout_seconds is not None:
                    config_kwargs['timeout_seconds'] = spec.timeout_seconds
                if spec.aggregations is not None:
                    config_kwargs['aggregations'] = spec.aggregations

                self._cython_processor = create_window_processor(
                    spec.window_type.value,
                    spec.size_seconds,
                    **config_kwargs
                )
            except Exception as e:
                print(f"Cython window processor failed, trying Arrow: {e}")
                self._cython_processor = None

        if self._cython_processor is None and ARROW_AVAILABLE:
            try:
                self._arrow_processor = ArrowWindowProcessor(spec)
            except Exception as e:
                print(f"Arrow window processor failed, falling back to Python: {e}")
                self._arrow_processor = None

        if self._cython_processor is None and self._arrow_processor is None:
            # Python fallback implementation
            self._python_fallback = PythonWindowProcessor(spec)

    async def process_record(self, record_batch: Any) -> None:
        """Process a batch of records through the window."""
        if self._cython_processor:
            await self._cython_processor.process_record(record_batch)
        elif self._arrow_processor:
            await self._arrow_processor.process_record_batch(record_batch)
        elif self._python_fallback:
            await self._python_fallback.process_record(record_batch)

    async def emit_windows(self) -> AsyncIterator[Dict[str, Any]]:
        """Emit completed windows."""
        if self._cython_processor:
            # Cython processor handles emission internally
            await self._cython_processor.emit_windows()
        elif self._arrow_processor:
            async for window in self._arrow_processor.emit_completed_windows():
                yield window
        elif self._python_fallback:
            async for window in self._python_fallback.emit_windows():
                yield window

    async def get_stats(self) -> Dict[str, Any]:
        """Get window statistics."""
        if self._cython_processor:
            return await self._cython_processor.get_window_stats()
        elif self._arrow_processor:
            return self._arrow_processor.get_window_stats()
        elif self._python_fallback:
            return await self._python_fallback.get_stats()
        else:
            return {}


class PythonWindowProcessor:
    """Pure Python window processor as fallback."""

    def __init__(self, spec: WindowSpec):
        self.spec = spec
        self.windows: Dict[str, Dict[str, Any]] = {}
        self.window_times: Dict[str, float] = {}

    async def process_record(self, record_batch: Any) -> None:
        """Process records through Python-based windows."""
        current_time = asyncio.get_event_loop().time()

        # Extract records
        records = self._extract_records(record_batch)

        for record in records:
            key = record.get(self.spec.key_field, 'default')
            window_key = self._get_window_key(key, current_time)

            if window_key not in self.windows:
                self.windows[window_key] = {
                    'start_time': current_time,
                    'end_time': current_time + self.spec.size_seconds,
                    'records': [],
                    'key': key
                }
                self.window_times[window_key] = current_time

            self.windows[window_key]['records'].append(record)

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

    def _get_window_key(self, record_key: str, current_time: float) -> str:
        """Generate window key based on window type."""
        if self.spec.window_type == WindowType.TUMBLING:
            window_start = int(current_time / self.spec.size_seconds) * self.spec.size_seconds
            return f"{record_key}:{window_start}"
        elif self.spec.window_type == WindowType.SLIDING:
            # Simplified sliding window logic
            slide_size = self.spec.slide_seconds or (self.spec.size_seconds / 2)
            window_start = int(current_time / slide_size) * slide_size
            return f"{record_key}:{window_start}"
        else:
            # Default to tumbling
            window_start = int(current_time / self.spec.size_seconds) * self.spec.size_seconds
            return f"{record_key}:{window_start}"

    async def emit_windows(self) -> AsyncIterator[Dict[str, Any]]:
        """Emit completed windows."""
        current_time = asyncio.get_event_loop().time()
        completed_windows = []

        for window_key, window_data in self.windows.items():
            if window_data['end_time'] <= current_time:
                # Apply aggregations
                if self.spec.aggregations:
                    window_data['aggregations'] = self._compute_aggregations(window_data['records'])

                completed_windows.append((window_key, window_data))

        # Emit and clean up completed windows
        for window_key, window_data in completed_windows:
            yield window_data
            del self.windows[window_key]
            if window_key in self.window_times:
                del self.window_times[window_key]

    def _compute_aggregations(self, records: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Compute aggregations on window records."""
        if not records or not self.spec.aggregations:
            return {}

        result = {}
        for field, agg_func in self.spec.aggregations.items():
            values = [r.get(field) for r in records if field in r and r[field] is not None]

            if not values:
                continue

            if agg_func == 'sum':
                result[field] = sum(values)
            elif agg_func == 'mean':
                result[field] = sum(values) / len(values)
            elif agg_func == 'count':
                result[field] = len(values)
            elif agg_func == 'min':
                result[field] = min(values)
            elif agg_func == 'max':
                result[field] = max(values)

        return result

    async def get_stats(self) -> Dict[str, Any]:
        """Get window statistics."""
        return {
            'active_windows': len(self.windows),
            'window_type': self.spec.window_type.value,
            'size_seconds': self.spec.size_seconds,
            'total_records': sum(len(w['records']) for w in self.windows.values())
        }


class StreamWindow:
    """Windowed stream that applies windowing operations to an input stream."""

    def __init__(self, stream, window_spec: WindowSpec):
        self.stream = stream
        self.window_spec = window_spec
        self.processor = WindowProcessor(window_spec)

    def __aiter__(self):
        return self._create_windowed_stream()

    async def _create_windowed_stream(self) -> AsyncIterator[Any]:
        """Create an async iterator that yields windowed results."""
        async for record_batch in self.stream:
            # Process records through window
            await self.processor.process_record(record_batch)

            # Emit any completed windows
            async for window in self.processor.emit_windows():
                yield window

        # Emit any remaining windows at end of stream
        async for window in self.processor.emit_windows():
            yield window


class WindowedStream:
    """Factory for creating windowed streams."""

    def __init__(self, app):
        self.app = app
        self._window_manager = get_window_manager() if CYTHON_AVAILABLE else None

    def tumbling(
        self,
        stream,
        size_seconds: float,
        key_field: str = "key",
        aggregations: Optional[Dict[str, str]] = None
    ) -> StreamWindow:
        """Create a tumbling window on a stream."""
        spec = WindowSpec(
            name=f"tumbling_{id(stream)}",
            window_type=WindowType.TUMBLING,
            size_seconds=size_seconds,
            key_field=key_field,
            aggregations=aggregations
        )
        return StreamWindow(stream, spec)

    def sliding(
        self,
        stream,
        size_seconds: float,
        slide_seconds: Optional[float] = None,
        key_field: str = "key",
        aggregations: Optional[Dict[str, str]] = None
    ) -> StreamWindow:
        """Create a sliding window on a stream."""
        spec = WindowSpec(
            name=f"sliding_{id(stream)}",
            window_type=WindowType.SLIDING,
            size_seconds=size_seconds,
            slide_seconds=slide_seconds,
            key_field=key_field,
            aggregations=aggregations
        )
        return StreamWindow(stream, spec)

    def hopping(
        self,
        stream,
        size_seconds: float,
        hop_seconds: Optional[float] = None,
        key_field: str = "key",
        aggregations: Optional[Dict[str, str]] = None
    ) -> StreamWindow:
        """Create a hopping window on a stream."""
        spec = WindowSpec(
            name=f"hopping_{id(stream)}",
            window_type=WindowType.HOPPING,
            size_seconds=size_seconds,
            hop_seconds=hop_seconds,
            key_field=key_field,
            aggregations=aggregations
        )
        return StreamWindow(stream, spec)

    def session(
        self,
        stream,
        timeout_seconds: float,
        key_field: str = "key",
        aggregations: Optional[Dict[str, str]] = None
    ) -> StreamWindow:
        """Create a session window on a stream."""
        spec = WindowSpec(
            name=f"session_{id(stream)}",
            window_type=WindowType.SESSION,
            size_seconds=timeout_seconds,  # For session windows, size is timeout
            timeout_seconds=timeout_seconds,
            key_field=key_field,
            aggregations=aggregations
        )
        return StreamWindow(stream, spec)

    async def get_stats(self) -> Dict[str, Any]:
        """Get statistics for all managed windows."""
        if self._window_manager:
            return await self._window_manager.get_window_stats()
        else:
            return {'status': 'Python fallback - no global stats available'}


# Integration with Sabot App
def create_windowed_stream(app) -> WindowedStream:
    """Create a windowed stream factory for the given app."""
    return WindowedStream(app)
