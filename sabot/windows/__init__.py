# -*- coding: utf-8 -*-
"""Window state management for Sabot."""

try:
    from sabot._cython.windows.marbledb_window_state import MarbleDBWindowState
    MARBLEDB_WINDOWS_AVAILABLE = True
except ImportError:
    MARBLEDB_WINDOWS_AVAILABLE = False
    MarbleDBWindowState = None

__all__ = ['MarbleDBWindowState', 'MARBLEDB_WINDOWS_AVAILABLE']

