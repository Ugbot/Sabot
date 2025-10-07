"""
Feature Registry - Metadata and configuration for feature extractors.
"""

from typing import Dict, List, Optional, Callable, Any
from dataclasses import dataclass
from enum import Enum


class FeatureType(Enum):
    """Types of features supported."""
    NUMERICAL = "numerical"
    CATEGORICAL = "categorical"
    TEMPORAL = "temporal"
    DERIVED = "derived"


@dataclass
class FeatureMetadata:
    """Metadata for a single feature."""
    name: str
    feature_type: FeatureType
    source_columns: List[str]
    window_size: Optional[str] = None  # e.g., "5m", "1h", "1d"
    aggregation: Optional[str] = None  # e.g., "mean", "std", "percentile"
    percentile: Optional[float] = None  # For percentile aggregations
    description: Optional[str] = None
    ttl: Optional[int] = None  # Cache TTL in seconds


class FeatureRegistry:
    """
    Central registry for feature definitions.

    Stores metadata about all available features and provides
    lookup/validation functionality.
    """

    def __init__(self):
        self._features: Dict[str, FeatureMetadata] = {}
        self._register_builtin_features()

    def register(self, feature: FeatureMetadata) -> None:
        """Register a new feature."""
        self._features[feature.name] = feature

    def get(self, name: str) -> Optional[FeatureMetadata]:
        """Get feature metadata by name."""
        return self._features.get(name)

    def list_features(self, feature_type: Optional[FeatureType] = None) -> List[str]:
        """List all registered features, optionally filtered by type."""
        if feature_type is None:
            return list(self._features.keys())
        return [
            name for name, meta in self._features.items()
            if meta.feature_type == feature_type
        ]

    def validate(self, feature_names: List[str]) -> tuple[List[str], List[str]]:
        """
        Validate a list of feature names.

        Returns:
            Tuple of (valid_features, invalid_features)
        """
        valid = []
        invalid = []
        for name in feature_names:
            if name in self._features:
                valid.append(name)
            else:
                invalid.append(name)
        return valid, invalid

    def _register_builtin_features(self) -> None:
        """Register built-in features for common use cases."""

        # Price-based features
        self.register(FeatureMetadata(
            name="price_rolling_mean_5m",
            feature_type=FeatureType.NUMERICAL,
            source_columns=["price"],
            window_size="5m",
            aggregation="mean",
            description="5-minute rolling mean of price",
            ttl=300  # 5 minutes
        ))

        self.register(FeatureMetadata(
            name="price_rolling_mean_1h",
            feature_type=FeatureType.NUMERICAL,
            source_columns=["price"],
            window_size="1h",
            aggregation="mean",
            description="1-hour rolling mean of price",
            ttl=3600  # 1 hour
        ))

        # Volume-based features
        self.register(FeatureMetadata(
            name="volume_std_1h",
            feature_type=FeatureType.NUMERICAL,
            source_columns=["volume_24_h"],
            window_size="1h",
            aggregation="std",
            description="1-hour rolling std of volume",
            ttl=3600
        ))

        # Spread features
        self.register(FeatureMetadata(
            name="spread_percentile_95",
            feature_type=FeatureType.NUMERICAL,
            source_columns=["best_bid", "best_ask"],
            aggregation="percentile",
            percentile=0.95,
            description="95th percentile of spread",
            ttl=3600
        ))

        self.register(FeatureMetadata(
            name="spread_bps",
            feature_type=FeatureType.DERIVED,
            source_columns=["best_bid", "best_ask"],
            description="Spread in basis points",
            ttl=60
        ))

        # Temporal features
        self.register(FeatureMetadata(
            name="hour_of_day",
            feature_type=FeatureType.TEMPORAL,
            source_columns=["timestamp"],
            description="Hour of day (0-23)",
            ttl=3600
        ))

        self.register(FeatureMetadata(
            name="day_of_week",
            feature_type=FeatureType.TEMPORAL,
            source_columns=["timestamp"],
            description="Day of week (0=Monday, 6=Sunday)",
            ttl=86400  # 1 day
        ))

        # Volatility features
        self.register(FeatureMetadata(
            name="price_volatility_1h",
            feature_type=FeatureType.NUMERICAL,
            source_columns=["price"],
            window_size="1h",
            aggregation="std",
            description="1-hour price volatility (std dev)",
            ttl=3600
        ))


# Global registry instance
_global_registry = FeatureRegistry()


def get_registry() -> FeatureRegistry:
    """Get the global feature registry."""
    return _global_registry
