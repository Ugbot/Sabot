# -*- coding: utf-8 -*-
"""Join buffer management for Sabot stream-stream joins."""

try:
    from sabot._cython.joins.marbledb_join_buffer import MarbleDBJoinBuffer
    MARBLEDB_JOINS_AVAILABLE = True
except ImportError:
    MARBLEDB_JOINS_AVAILABLE = False
    MarbleDBJoinBuffer = None

__all__ = ['MarbleDBJoinBuffer', 'MARBLEDB_JOINS_AVAILABLE']

