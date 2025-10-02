# -*- coding: utf-8 -*-
"""
Cython Arrow Processing

Zero-copy Arrow operations with SIMD acceleration for Flink-level performance.
"""

__all__ = [
    'ArrowBatchProcessor',
    'ArrowComputeEngine',
    'ArrowWindowProcessor',
    'ArrowJoinProcessor',
    'ArrowFlightClient',
]

# Import Cython extensions (will be available after compilation)
try:
    from .batch_processor import ArrowBatchProcessor, ArrowComputeEngine
    from .window_processor import ArrowWindowProcessor
    from .join_processor import ArrowJoinProcessor
    from .flight_client import ArrowFlightClient

    ARROW_PROCESSING_AVAILABLE = True

except ImportError:
    ARROW_PROCESSING_AVAILABLE = False

    # Provide fallback interfaces
    class ArrowBatchProcessor:
        """Fallback ArrowBatchProcessor interface."""
        pass

    class ArrowComputeEngine:
        """Fallback ArrowComputeEngine interface."""
        pass

    class ArrowWindowProcessor:
        """Fallback ArrowWindowProcessor interface."""
        pass

    class ArrowJoinProcessor:
        """Fallback ArrowJoinProcessor interface."""
        pass

    class ArrowFlightClient:
        """Fallback ArrowFlightClient interface."""
        pass
