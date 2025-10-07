# -*- coding: utf-8 -*-
"""
Shuffle types module - Python-accessible types for shuffle operations.

Since cpdef enums in .pxd files are not directly importable from Python,
we provide Python equivalents here.
"""

from enum import IntEnum


class ShuffleEdgeType(IntEnum):
    """Shuffle edge types for execution graph."""
    FORWARD = 0      # 1:1 mapping, no shuffle (operator chaining)
    HASH = 1         # Hash-based repartitioning by key
    BROADCAST = 2    # Replicate to all downstream tasks
    REBALANCE = 3    # Round-robin redistribution
    RANGE = 4        # Range-based partitioning for sorted data


__all__ = ['ShuffleEdgeType']
