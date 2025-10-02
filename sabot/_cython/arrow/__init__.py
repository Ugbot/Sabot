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
    from sabot._cython.arrow.batch_processor import ArrowBatchProcessor, ArrowComputeEngine
    from sabot._cython.arrow.window_processor import ArrowWindowProcessor
    from sabot._cython.arrow.join_processor import ArrowJoinProcessor
    # from sabot._cython.arrow.flight_client import ArrowFlightClient  # Not available

    ARROW_PROCESSING_AVAILABLE = True

except ImportError as e:
    print(f"Warning: Arrow processing not available: {e}")
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
