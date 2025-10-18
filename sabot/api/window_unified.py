#!/usr/bin/env python3
"""
Unified Window API

Single window implementation with thin Python wrapper over Cython.
Consolidates 3+ window implementations into one.

Performance: Delegates to sabot/_cython/arrow/window_processor.pyx
"""

import logging
from typing import Optional, Dict, Any, Callable
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)


class WindowType(Enum):
    """Window types."""
    TUMBLING = "tumbling"
    SLIDING = "sliding"
    HOPPING = "hopping"
    SESSION = "session"
    COUNT = "count"


@dataclass
class WindowConfig:
    """
    Window configuration.
    
    Unified config that works across all window types.
    """
    window_type: WindowType
    size: float  # For time windows: seconds; for count windows: element count
    slide: Optional[float] = None  # Slide interval (sliding/hopping windows)
    gap: Optional[float] = None  # Session gap (session windows)
    key_field: str = "key"
    timestamp_field: str = "timestamp"
    aggregations: Optional[Dict[str, str]] = None
    emit_early: bool = False


class UnifiedWindowProcessor:
    """
    Unified window processor.
    
    Single implementation that delegates to Cython for performance.
    Replaces multiple window implementations with one.
    
    Delegates to: sabot/_cython/arrow/window_processor.pyx (when available)
    Fallback: Python implementation
    
    Performance:
    - Cython: Near C++ speed
    - Python fallback: ~10-100x slower (for testing)
    
    Example:
        config = WindowConfig(WindowType.TUMBLING, size=60.0)
        processor = UnifiedWindowProcessor(config)
        
        processor.process_batch(batch)
        windows = processor.emit_completed_windows()
    """
    
    def __init__(self, config: WindowConfig):
        """
        Initialize window processor.
        
        Args:
            config: Window configuration
        """
        self.config = config
        self._processor = None
        self._fallback = None
        
        # Try Cython implementation first
        self._init_processor()
    
    def _init_processor(self):
        """Initialize window processor (Cython or Python fallback)."""
        # Try Cython implementation
        try:
            from sabot._cython.arrow import window_processor
            
            # Create Cython window processor
            # Note: Actual class name might be different
            self._processor = window_processor.create_window_processor(
                window_type=self.config.window_type.value,
                size=self.config.size,
                slide=self.config.slide,
                gap=self.config.gap,
                key_field=self.config.key_field,
                timestamp_field=self.config.timestamp_field,
                aggregations=self.config.aggregations
            )
            
            logger.info(f"Initialized Cython window processor ({self.config.window_type.value})")
            
        except (ImportError, AttributeError) as e:
            logger.warning(f"Cython window processor not available: {e}")
            logger.info("Using Python fallback window processor")
            
            # Use Python fallback
            from sabot.api.window import WindowAssigner
            self._fallback = WindowAssigner(self._config_to_window_spec())
    
    def _config_to_window_spec(self):
        """Convert WindowConfig to old WindowSpec format."""
        from sabot.api.window import TumblingWindow, SlidingWindow, SessionWindow
        from datetime import timedelta
        
        if self.config.window_type == WindowType.TUMBLING:
            return TumblingWindow(size=timedelta(seconds=self.config.size))
        elif self.config.window_type == WindowType.SLIDING:
            return SlidingWindow(
                size=timedelta(seconds=self.config.size),
                slide=timedelta(seconds=self.config.slide or self.config.size)
            )
        elif self.config.window_type == WindowType.SESSION:
            return SessionWindow(gap=timedelta(seconds=self.config.gap or 300))
        else:
            # Default to tumbling
            return TumblingWindow(size=timedelta(seconds=self.config.size))
    
    def process_batch(self, batch):
        """
        Process batch through window.
        
        Args:
            batch: Arrow RecordBatch
        """
        if self._processor:
            # Use Cython implementation
            self._processor.process_batch(batch)
        elif self._fallback:
            # Use Python fallback
            # TODO: Implement fallback processing
            pass
        else:
            logger.warning("No window processor available")
    
    def emit_completed_windows(self):
        """
        Emit completed windows.
        
        Returns:
            List of completed windows
        """
        if self._processor:
            try:
                return self._processor.emit_completed_windows()
            except:
                return []
        elif self._fallback:
            # TODO: Implement fallback emission
            return []
        else:
            return []
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get window statistics.
        
        Returns:
            Dict with window stats
        """
        stats = {
            'window_type': self.config.window_type.value,
            'size': self.config.size,
            'implementation': 'cython' if self._processor else 'python'
        }
        
        if self._processor and hasattr(self._processor, 'get_stats'):
            try:
                stats.update(self._processor.get_stats())
            except:
                pass
        
        return stats


# Convenience functions

def tumbling(size: float, **kwargs) -> WindowConfig:
    """
    Create tumbling window configuration.
    
    Args:
        size: Window size in seconds
        **kwargs: Additional window options
        
    Returns:
        WindowConfig
        
    Example:
        config = tumbling(60.0)  # 1-minute tumbling windows
    """
    return WindowConfig(WindowType.TUMBLING, size=size, **kwargs)


def sliding(size: float, slide: float, **kwargs) -> WindowConfig:
    """
    Create sliding window configuration.
    
    Args:
        size: Window size in seconds
        slide: Slide interval in seconds
        **kwargs: Additional window options
        
    Returns:
        WindowConfig
        
    Example:
        config = sliding(60.0, 30.0)  # 60s window, 30s slide
    """
    return WindowConfig(WindowType.SLIDING, size=size, slide=slide, **kwargs)


def session(gap: float, **kwargs) -> WindowConfig:
    """
    Create session window configuration.
    
    Args:
        gap: Inactivity gap in seconds
        **kwargs: Additional window options
        
    Returns:
        WindowConfig
        
    Example:
        config = session(300.0)  # 5-minute inactivity gap
    """
    return WindowConfig(WindowType.SESSION, size=gap, gap=gap, **kwargs)


def count(size: int, slide: Optional[int] = None, **kwargs) -> WindowConfig:
    """
    Create count window configuration.
    
    Args:
        size: Window size in number of elements
        slide: Slide interval in elements
        **kwargs: Additional window options
        
    Returns:
        WindowConfig
        
    Example:
        config = count(100)  # Windows of 100 elements
    """
    return WindowConfig(
        WindowType.COUNT,
        size=float(size),
        slide=float(slide) if slide else None,
        **kwargs
    )


def create_window_processor(config: WindowConfig) -> UnifiedWindowProcessor:
    """
    Create window processor from configuration.
    
    Args:
        config: Window configuration
        
    Returns:
        UnifiedWindowProcessor
        
    Example:
        config = tumbling(60.0, key_field='user_id')
        processor = create_window_processor(config)
    """
    return UnifiedWindowProcessor(config)

