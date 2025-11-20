# -*- coding: utf-8 -*-
"""Join operations for Sabot - zero-copy hash join and stream-stream joins."""

# Zero-copy hash join using sabot_ql C++ implementation
try:
    from sabot._cython.joins_ql.hash_join import hash_join
    HASH_JOIN_AVAILABLE = True
except ImportError:
    HASH_JOIN_AVAILABLE = False
    hash_join = None

# Stream-stream join buffer using MarbleDB
try:
    from sabot._cython.joins.marbledb_join_buffer import MarbleDBJoinBuffer
    MARBLEDB_JOINS_AVAILABLE = True
except ImportError:
    MARBLEDB_JOINS_AVAILABLE = False
    MarbleDBJoinBuffer = None

__all__ = ['hash_join', 'HASH_JOIN_AVAILABLE', 'MarbleDBJoinBuffer', 'MARBLEDB_JOINS_AVAILABLE']

