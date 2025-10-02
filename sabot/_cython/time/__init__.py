# -*- coding: utf-8 -*-
"""
Cython Time Management

Provides high-performance timer services and watermark tracking
for Flink-compatible event-time processing.
"""

__all__ = [
    'TimerService',
    'WatermarkTracker',
    'TimeService',
    'EventTimeService',
]

# Import Cython extensions (will be available after compilation)
try:
    from .timers import TimerService
    from .watermark_tracker import WatermarkTracker
    from .time_service import TimeService
    from .event_time import EventTimeService

    CYTHON_TIME_AVAILABLE = True

except ImportError:
    CYTHON_TIME_AVAILABLE = False

    # Provide fallback interfaces
    class TimerService:
        """Fallback TimerService interface."""
        pass

    class WatermarkTracker:
        """Fallback WatermarkTracker interface."""
        pass

    class TimeService:
        """Fallback TimeService interface."""
        pass

    class EventTimeService:
        """Fallback EventTimeService interface."""
        pass
