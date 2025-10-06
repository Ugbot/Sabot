# -*- coding: utf-8 -*-
"""
Shuffle Module - High-Performance Data Redistribution for Operator Parallelism

Implements shuffle operations for redistributing Arrow RecordBatches across
parallel agent instances based on partitioning keys.

All components implemented in Cython/C++ for zero-copy Arrow IPC performance.

Key Components:
- Partitioner: Hash/Range/RoundRobin key-based partitioning
- ShuffleBuffer: Memory management with spill-to-disk
- ShuffleTransport: Arrow Flight-based network transport
- ShuffleManager: Coordination of N×M task shuffles

Performance Targets:
- Partitioning: >1M rows/sec
- Network: >100K rows/sec per partition
- Serialization: <1μs per batch (zero-copy)
- Memory: Bounded by buffer pool
"""

__all__ = [
    # Partitioners
    "HashPartitioner",
    "RangePartitioner",
    "RoundRobinPartitioner",
    # Buffers
    "ShuffleBuffer",
    "NetworkBufferPool",
    # Transport
    "ShuffleServer",
    "ShuffleClient",
    # Manager
    "ShuffleManager",
]

try:
    from .partitioner import (
        HashPartitioner,
        RangePartitioner,
        RoundRobinPartitioner,
    )
except ImportError:
    pass

try:
    from .shuffle_buffer import (
        ShuffleBuffer,
        NetworkBufferPool,
    )
except ImportError:
    pass

try:
    from .shuffle_transport import (
        ShuffleServer,
        ShuffleClient,
    )
except ImportError:
    pass

try:
    from .shuffle_manager import ShuffleManager
except ImportError:
    pass
