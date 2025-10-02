"""
High-Level Window API

Provides window specifications for time-based aggregations.
Supports tumbling, sliding, and session windows.
"""

from typing import Optional
from dataclasses import dataclass
from datetime import timedelta


@dataclass
class WindowSpec:
    """Base class for window specifications."""
    pass


@dataclass
class TumblingWindow(WindowSpec):
    """
    Tumbling (fixed) window.

    Non-overlapping windows of fixed size.
    Each element belongs to exactly one window.

    Example:
        window = tumbling(seconds=60)  # 1-minute windows
        [0-60), [60-120), [120-180), ...
    """
    size: timedelta

    def __repr__(self):
        return f"TumblingWindow(size={self.size})"


@dataclass
class SlidingWindow(WindowSpec):
    """
    Sliding window.

    Overlapping windows with fixed size and slide interval.
    Each element can belong to multiple windows.

    Example:
        window = sliding(size=60, slide=30)  # 60s windows, 30s slide
        [0-60), [30-90), [60-120), ...
    """
    size: timedelta
    slide: timedelta

    def __repr__(self):
        return f"SlidingWindow(size={self.size}, slide={self.slide})"


@dataclass
class SessionWindow(WindowSpec):
    """
    Session window.

    Dynamic windows based on activity gaps.
    Window closes after gap of inactivity.

    Example:
        window = session(gap=300)  # 5-minute inactivity gap
    """
    gap: timedelta

    def __repr__(self):
        return f"SessionWindow(gap={self.gap})"


@dataclass
class CountWindow(WindowSpec):
    """
    Count-based window.

    Windows based on number of elements rather than time.

    Example:
        window = count(size=100)  # Windows of 100 elements
    """
    size: int
    slide: Optional[int] = None

    def __repr__(self):
        return f"CountWindow(size={self.size}, slide={self.slide})"


# Convenience functions for creating windows

def tumbling(seconds: Optional[int] = None,
             minutes: Optional[int] = None,
             hours: Optional[int] = None,
             days: Optional[int] = None) -> TumblingWindow:
    """
    Create tumbling window.

    Args:
        seconds: Window size in seconds
        minutes: Window size in minutes
        hours: Window size in hours
        days: Window size in days

    Returns:
        TumblingWindow specification

    Example:
        tumbling(seconds=60)  # 1-minute tumbling windows
        tumbling(minutes=5)   # 5-minute tumbling windows
        tumbling(hours=1)     # 1-hour tumbling windows
    """
    size = timedelta(
        seconds=seconds or 0,
        minutes=minutes or 0,
        hours=hours or 0,
        days=days or 0
    )

    if size.total_seconds() <= 0:
        raise ValueError("Window size must be positive")

    return TumblingWindow(size=size)


def sliding(size_seconds: Optional[int] = None,
            slide_seconds: Optional[int] = None,
            size_minutes: Optional[int] = None,
            slide_minutes: Optional[int] = None,
            size_hours: Optional[int] = None,
            slide_hours: Optional[int] = None) -> SlidingWindow:
    """
    Create sliding window.

    Args:
        size_seconds: Window size in seconds
        slide_seconds: Slide interval in seconds
        size_minutes: Window size in minutes
        slide_minutes: Slide interval in minutes
        size_hours: Window size in hours
        slide_hours: Slide interval in hours

    Returns:
        SlidingWindow specification

    Example:
        sliding(size_seconds=60, slide_seconds=30)  # 60s window, 30s slide
        sliding(size_minutes=5, slide_minutes=1)    # 5min window, 1min slide
    """
    size = timedelta(
        seconds=size_seconds or 0,
        minutes=size_minutes or 0,
        hours=size_hours or 0
    )

    slide = timedelta(
        seconds=slide_seconds or 0,
        minutes=slide_minutes or 0,
        hours=slide_hours or 0
    )

    if size.total_seconds() <= 0:
        raise ValueError("Window size must be positive")

    if slide.total_seconds() <= 0:
        raise ValueError("Slide interval must be positive")

    if slide > size:
        raise ValueError("Slide interval cannot be larger than window size")

    return SlidingWindow(size=size, slide=slide)


def session(gap_seconds: Optional[int] = None,
            gap_minutes: Optional[int] = None,
            gap_hours: Optional[int] = None) -> SessionWindow:
    """
    Create session window.

    Args:
        gap_seconds: Inactivity gap in seconds
        gap_minutes: Inactivity gap in minutes
        gap_hours: Inactivity gap in hours

    Returns:
        SessionWindow specification

    Example:
        session(gap_seconds=300)  # 5-minute inactivity gap
        session(gap_minutes=10)   # 10-minute inactivity gap
    """
    gap = timedelta(
        seconds=gap_seconds or 0,
        minutes=gap_minutes or 0,
        hours=gap_hours or 0
    )

    if gap.total_seconds() <= 0:
        raise ValueError("Gap must be positive")

    return SessionWindow(gap=gap)


def count(size: int, slide: Optional[int] = None) -> CountWindow:
    """
    Create count-based window.

    Args:
        size: Number of elements per window
        slide: Slide interval in elements (default: same as size for tumbling)

    Returns:
        CountWindow specification

    Example:
        count(100)           # Tumbling window of 100 elements
        count(100, slide=50) # Sliding window of 100 elements, slide 50
    """
    if size <= 0:
        raise ValueError("Window size must be positive")

    if slide is not None and slide <= 0:
        raise ValueError("Slide must be positive")

    if slide is not None and slide > size:
        raise ValueError("Slide cannot be larger than window size")

    return CountWindow(size=size, slide=slide)


class WindowAssigner:
    """
    Assigns elements to windows.

    Used internally by windowed streams to determine which
    window(s) each element belongs to.
    """

    def __init__(self, window_spec: WindowSpec):
        self.window_spec = window_spec

    def assign_windows(self, timestamp: int) -> list:
        """
        Assign element to window(s) based on timestamp.

        Args:
            timestamp: Element timestamp (milliseconds since epoch)

        Returns:
            List of (window_start, window_end) tuples
        """
        if isinstance(self.window_spec, TumblingWindow):
            return self._assign_tumbling(timestamp)
        elif isinstance(self.window_spec, SlidingWindow):
            return self._assign_sliding(timestamp)
        elif isinstance(self.window_spec, SessionWindow):
            # Session windows require stateful assignment
            raise NotImplementedError("Session window assignment requires state")
        else:
            raise ValueError(f"Unknown window type: {type(self.window_spec)}")

    def _assign_tumbling(self, timestamp: int) -> list:
        """Assign to tumbling window."""
        size_ms = int(self.window_spec.size.total_seconds() * 1000)
        window_start = (timestamp // size_ms) * size_ms
        window_end = window_start + size_ms
        return [(window_start, window_end)]

    def _assign_sliding(self, timestamp: int) -> list:
        """Assign to sliding windows."""
        size_ms = int(self.window_spec.size.total_seconds() * 1000)
        slide_ms = int(self.window_spec.slide.total_seconds() * 1000)

        # Find all windows that contain this timestamp
        windows = []

        # Start from the first window that could contain this timestamp
        last_window_start = (timestamp // slide_ms) * slide_ms

        # Check windows going backward
        window_start = last_window_start
        while window_start > timestamp - size_ms:
            window_end = window_start + size_ms
            if window_start <= timestamp < window_end:
                windows.append((window_start, window_end))
            window_start -= slide_ms

        # Check windows going forward
        window_start = last_window_start + slide_ms
        while window_start <= timestamp:
            window_end = window_start + size_ms
            if timestamp < window_end:
                windows.append((window_start, window_end))
            window_start += slide_ms

        return sorted(windows)


class WindowTrigger:
    """
    Determines when to emit window results.

    Supports event-time and processing-time triggers,
    early/late firing, and custom trigger logic.
    """

    def __init__(self, trigger_type: str = "event_time", early_fire: bool = False):
        """
        Initialize window trigger.

        Args:
            trigger_type: "event_time" or "processing_time"
            early_fire: Whether to fire early results before window closes
        """
        self.trigger_type = trigger_type
        self.early_fire = early_fire

    def should_fire(self, window_end: int, current_watermark: int) -> bool:
        """
        Check if window should fire.

        Args:
            window_end: Window end timestamp
            current_watermark: Current watermark timestamp

        Returns:
            True if window should fire
        """
        if self.trigger_type == "event_time":
            return current_watermark >= window_end
        else:
            # Processing time - fire immediately
            return True


class WindowEvictor:
    """
    Evicts elements from windows after trigger fires.

    Can keep elements for multiple firings or remove after first.
    """

    def __init__(self, keep_all: bool = False):
        """
        Initialize window evictor.

        Args:
            keep_all: If True, keep elements after firing. If False, evict.
        """
        self.keep_all = keep_all

    def should_evict(self) -> bool:
        """Check if elements should be evicted after firing."""
        return not self.keep_all
