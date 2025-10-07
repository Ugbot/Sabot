"""
Sabot Feature Engineering Pipeline

High-performance feature engineering for streaming and batch processing.
Leverages CyRedis for fast feature storage/retrieval and Cython for compute.

Key Components:
- Feature extractors: Vectorized feature computation on Arrow batches
- Feature store: CyRedis-backed caching with TTL support
- Feature pipeline: Stream operator for feature enrichment

Example:
    stream = Stream.from_kafka(...)
    result = stream.extract_features([
        'price_rolling_mean_5m',
        'volume_std_1h',
        'spread_percentile_95'
    ]).cache_features(redis_url='localhost:6379')
"""

from typing import List, Optional, Dict, Any, Callable

# Public API exports
__all__ = [
    'FeatureRegistry',
    'FeatureStore',
    'extract_features',
    'create_feature_map',
    'to_feature_store',
]

# Import Python modules
from .registry import FeatureRegistry
from .store import FeatureStore
from .sink import to_feature_store

# Import Cython modules when available
try:
    from .extractors import (
        RollingMeanExtractor,
        RollingStdExtractor,
        PercentileExtractor,
        TimeBasedExtractor,
    )
    from .pipeline import FeaturePipeline
    CYTHON_AVAILABLE = True
except ImportError:
    CYTHON_AVAILABLE = False
    RollingMeanExtractor = None
    RollingStdExtractor = None
    PercentileExtractor = None
    TimeBasedExtractor = None
    FeaturePipeline = None


def create_feature_map(extractors: List) -> Callable:
    """
    Convert feature extractors to a map function for use with Stream.map().

    Args:
        extractors: List of BaseFeatureExtractor instances

    Returns:
        Function that applies extractors to Arrow batches

    Example:
        >>> from sabot.features import FeatureRegistry, create_feature_map
        >>> registry = FeatureRegistry()
        >>> extractors = [
        ...     registry.create_extractor('price_rolling_mean_5m'),
        ...     registry.create_extractor('volume_std_1h')
        ... ]
        >>> feature_func = create_feature_map(extractors)
        >>> stream.map(feature_func)
    """
    def apply_extractors(batch):
        """Apply all extractors sequentially to the batch."""
        for extractor in extractors:
            batch = extractor.extract(batch)
        return batch

    return apply_extractors


def extract_features(feature_names: List[str],
                     redis_url: Optional[str] = None,
                     **kwargs) -> 'FeaturePipeline':
    """
    Create a feature extraction pipeline (deprecated - use create_feature_map + .map()).

    Args:
        feature_names: List of feature names to extract
        redis_url: Optional Redis URL for feature caching
        **kwargs: Additional configuration for feature extractors

    Returns:
        FeaturePipeline operator for use in Stream API

    Example:
        >>> stream = Stream.from_kafka(...)
        >>> features = extract_features([
        ...     'price_rolling_mean_5m',
        ...     'volume_std_1h'
        ... ], redis_url='localhost:6379')
        >>> result = stream.map(features)
    """
    if not CYTHON_AVAILABLE:
        raise ImportError(
            "Feature extraction requires Cython modules. "
            "Please run: python setup.py build_ext --inplace"
        )

    return FeaturePipeline(feature_names, redis_url=redis_url, **kwargs)
