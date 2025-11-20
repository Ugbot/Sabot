# -*- coding: utf-8 -*-
"""Zero-copy hash join using sabot_ql C++ implementation."""

try:
    from sabot._cython.joins_ql.hash_join import hash_join
    HASH_JOIN_AVAILABLE = True
except ImportError as e:
    HASH_JOIN_AVAILABLE = False
    hash_join = None
    import warnings
    warnings.warn(f"hash_join not available: {e}")

__all__ = ['hash_join', 'HASH_JOIN_AVAILABLE']
