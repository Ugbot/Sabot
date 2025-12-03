# -*- coding: utf-8 -*-
"""
Sabot Memory Configuration

Configuration for bigger-than-memory streaming operations.
Allows setting per-operator memory limits for joins, groupby, and windows.

Usage:
    from sabot.config import MemoryConfig

    # Configure memory limits
    MemoryConfig.set_join_memory_limit(512 * 1024 * 1024)  # 512MB for joins
    MemoryConfig.set_groupby_memory_limit(256 * 1024 * 1024)  # 256MB for groupby
    MemoryConfig.set_window_memory_limit(64 * 1024 * 1024)  # 64MB per window

    # Or set all at once
    MemoryConfig.configure(
        join_threshold=512 * 1024 * 1024,
        groupby_threshold=256 * 1024 * 1024,
        window_per_operator=64 * 1024 * 1024,
        window_total=256 * 1024 * 1024,
        spill_path="/tmp/sabot_spill"
    )

    # Check current config
    print(MemoryConfig.get_config())
"""

import os
from dataclasses import dataclass, field
from typing import Optional


# Default memory limits (in bytes)
DEFAULT_JOIN_MEMORY_THRESHOLD = 256 * 1024 * 1024      # 256MB
DEFAULT_GROUPBY_FLUSH_THRESHOLD = 64 * 1024 * 1024     # 64MB
DEFAULT_GROUPBY_MEMORY_THRESHOLD = 256 * 1024 * 1024   # 256MB total
DEFAULT_WINDOW_PER_OPERATOR = 64 * 1024 * 1024         # 64MB per window
DEFAULT_WINDOW_TOTAL = 256 * 1024 * 1024               # 256MB total for windows
DEFAULT_SPILL_PATH = "/tmp/sabot_spill"


@dataclass
class MemorySettings:
    """Memory settings for streaming operators."""

    # Join operator settings
    join_memory_threshold: int = DEFAULT_JOIN_MEMORY_THRESHOLD

    # GroupBy operator settings
    groupby_flush_threshold: int = DEFAULT_GROUPBY_FLUSH_THRESHOLD
    groupby_memory_threshold: int = DEFAULT_GROUPBY_MEMORY_THRESHOLD

    # Window operator settings
    window_per_operator_threshold: int = DEFAULT_WINDOW_PER_OPERATOR
    window_total_threshold: int = DEFAULT_WINDOW_TOTAL

    # Spill storage settings
    spill_path: str = DEFAULT_SPILL_PATH

    # Feature flags
    enable_spill: bool = True
    auto_detect_memory: bool = False  # TODO: Implement auto-detection


class MemoryConfig:
    """
    Global memory configuration for Sabot streaming operators.

    Thread-safe singleton pattern for consistent configuration across operators.
    """

    _settings: MemorySettings = MemorySettings()

    @classmethod
    def configure(
        cls,
        join_threshold: Optional[int] = None,
        groupby_flush_threshold: Optional[int] = None,
        groupby_threshold: Optional[int] = None,
        window_per_operator: Optional[int] = None,
        window_total: Optional[int] = None,
        spill_path: Optional[str] = None,
        enable_spill: Optional[bool] = None
    ) -> None:
        """
        Configure memory settings for all operators.

        Args:
            join_threshold: Memory threshold for hash join (bytes)
            groupby_flush_threshold: Flush threshold for groupby partials (bytes)
            groupby_threshold: Total memory threshold for groupby (bytes)
            window_per_operator: Memory per window (bytes)
            window_total: Total memory for all windows (bytes)
            spill_path: Directory for spill storage
            enable_spill: Enable/disable spilling to disk
        """
        if join_threshold is not None:
            cls._settings.join_memory_threshold = join_threshold
        if groupby_flush_threshold is not None:
            cls._settings.groupby_flush_threshold = groupby_flush_threshold
        if groupby_threshold is not None:
            cls._settings.groupby_memory_threshold = groupby_threshold
        if window_per_operator is not None:
            cls._settings.window_per_operator_threshold = window_per_operator
        if window_total is not None:
            cls._settings.window_total_threshold = window_total
        if spill_path is not None:
            cls._settings.spill_path = spill_path
        if enable_spill is not None:
            cls._settings.enable_spill = enable_spill

    @classmethod
    def set_join_memory_limit(cls, threshold_bytes: int) -> None:
        """Set memory limit for join operations."""
        cls._settings.join_memory_threshold = threshold_bytes

    @classmethod
    def set_groupby_memory_limit(cls, threshold_bytes: int, flush_threshold: Optional[int] = None) -> None:
        """Set memory limit for groupby operations."""
        cls._settings.groupby_memory_threshold = threshold_bytes
        if flush_threshold is not None:
            cls._settings.groupby_flush_threshold = flush_threshold

    @classmethod
    def set_window_memory_limit(cls, per_window: int, total: Optional[int] = None) -> None:
        """Set memory limit for window operations."""
        cls._settings.window_per_operator_threshold = per_window
        if total is not None:
            cls._settings.window_total_threshold = total

    @classmethod
    def set_spill_path(cls, path: str) -> None:
        """Set directory for spill storage."""
        cls._settings.spill_path = path
        os.makedirs(path, exist_ok=True)

    @classmethod
    def enable_spilling(cls, enable: bool = True) -> None:
        """Enable or disable spilling to disk."""
        cls._settings.enable_spill = enable

    @classmethod
    def get_settings(cls) -> MemorySettings:
        """Get current memory settings."""
        return cls._settings

    @classmethod
    def get_config(cls) -> dict:
        """Get current configuration as dictionary."""
        return {
            'join_memory_threshold_mb': cls._settings.join_memory_threshold / (1024 * 1024),
            'groupby_flush_threshold_mb': cls._settings.groupby_flush_threshold / (1024 * 1024),
            'groupby_memory_threshold_mb': cls._settings.groupby_memory_threshold / (1024 * 1024),
            'window_per_operator_mb': cls._settings.window_per_operator_threshold / (1024 * 1024),
            'window_total_mb': cls._settings.window_total_threshold / (1024 * 1024),
            'spill_path': cls._settings.spill_path,
            'enable_spill': cls._settings.enable_spill,
        }

    @classmethod
    def reset(cls) -> None:
        """Reset to default settings."""
        cls._settings = MemorySettings()


# Convenience functions
def configure_memory(**kwargs):
    """Configure memory settings (shortcut for MemoryConfig.configure)."""
    MemoryConfig.configure(**kwargs)


def get_memory_config() -> dict:
    """Get current memory configuration."""
    return MemoryConfig.get_config()
