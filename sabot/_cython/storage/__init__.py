# -*- coding: utf-8 -*-
"""
Sabot Storage Cython Module

Provides Python bindings for the C++ storage shim layer.
"""

import logging
import warnings

logger = logging.getLogger(__name__)

# Try to import the storage shim
try:
    from .storage_shim import SabotStateBackend, SabotStoreBackend
    STORAGE_SHIM_AVAILABLE = True
    logger.debug("Storage shim loaded successfully")
except ImportError as e:
    STORAGE_SHIM_AVAILABLE = False
    SabotStateBackend = None
    SabotStoreBackend = None
    warnings.warn(f"Storage shim not available: {e}")

__all__ = [
    'STORAGE_SHIM_AVAILABLE',
    'SabotStateBackend',
    'SabotStoreBackend',
]
