# -*- coding: utf-8 -*-
"""Window state management for Sabot."""

try:
    from sabot._cython.windows.marbledb_window_state import MarbleDBWindowState
    MARBLEDB_WINDOWS_AVAILABLE = True
except ImportError:
    MARBLEDB_WINDOWS_AVAILABLE = False
    MarbleDBWindowState = None

# Stubs for missing functions (backward compatibility)
def create_windowed_stream(*args, **kwargs):
    """Deprecated: Use Stream.window() from sabot.api.stream instead."""
    raise NotImplementedError("Use Stream.window() instead")

class WindowedStream:
    """Deprecated: Use Stream.window() from sabot.api.stream instead."""
    def __init__(self, *args, **kwargs):
        raise NotImplementedError("Use Stream.window() instead")

__all__ = ['MarbleDBWindowState', 'MARBLEDB_WINDOWS_AVAILABLE', 'create_windowed_stream', 'WindowedStream']

