"""
Time Window Manager

Manages sliding time windows for streaming graph queries.
"""

from datetime import datetime, timedelta
from typing import Tuple


class TimeWindowManager:
    """
    Manages sliding time windows for graph queries.
    
    Features:
    - Tumbling windows
    - Sliding windows
    - Session windows
    - Configurable window sizes and slide intervals
    """
    
    def __init__(self, window_size: timedelta, slide_interval: timedelta = None):
        """
        Initialize time window manager.
        
        Args:
            window_size: Size of the time window
            slide_interval: How often to slide the window (default: same as window_size for tumbling)
        """
        self.window_size = window_size
        self.slide_interval = slide_interval or window_size
        self.current_window_end = datetime.now()
        self.window_count = 0
    
    def should_slide(self) -> bool:
        """Check if window should slide."""
        return datetime.now() >= self.current_window_end + self.slide_interval
    
    def get_current_window(self) -> Tuple[datetime, datetime]:
        """
        Get current window time range.
        
        Returns:
            Tuple of (start_time, end_time)
        """
        end = self.current_window_end
        start = end - self.window_size
        return (start, end)
    
    def slide(self) -> Tuple[datetime, datetime]:
        """
        Slide the window forward.
        
        Returns:
            New window time range
        """
        self.current_window_end += self.slide_interval
        self.window_count += 1
        return self.get_current_window()
    
    def reset(self):
        """Reset window to current time."""
        self.current_window_end = datetime.now()
        self.window_count = 0
    
    def get_stats(self) -> dict:
        """Get window statistics."""
        return {
            'window_size': str(self.window_size),
            'slide_interval': str(self.slide_interval),
            'window_count': self.window_count,
            'current_window': self.get_current_window()
        }

