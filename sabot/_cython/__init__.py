# -*- coding: utf-8 -*-
"""Sabot Cython extensions - High-performance internal implementations."""

import logging

logger = logging.getLogger(__name__)

# Track which modules are available
AVAILABLE_MODULES = []

# Core Arrow implementation (internal)
try:
    from .arrow_core_simple import *
    AVAILABLE_MODULES.append('arrow_core_simple')
    logger.debug("Loaded internal Arrow implementation")
except ImportError as e:
    logger.debug(f"arrow_core_simple not available: {e}")

# Agents
try:
    from .agents import *
    AVAILABLE_MODULES.append('agents')
except ImportError as e:
    logger.debug(f"agents not available: {e}")

# Windows
try:
    from .windows import *
    AVAILABLE_MODULES.append('windows')
except ImportError as e:
    logger.debug(f"windows not available: {e}")

# Joins
try:
    from .joins import *
    AVAILABLE_MODULES.append('joins')
except ImportError as e:
    logger.debug(f"joins not available: {e}")

# Channels
try:
    from .channels import *
    AVAILABLE_MODULES.append('channels')
except ImportError as e:
    logger.debug(f"channels not available: {e}")

# Morsel parallelism
try:
    from .morsel_parallelism import *
    AVAILABLE_MODULES.append('morsel_parallelism')
except ImportError as e:
    logger.debug(f"morsel_parallelism not available: {e}")

# Materialized views
try:
    from .materialized_views import *
    AVAILABLE_MODULES.append('materialized_views')
except ImportError as e:
    logger.debug(f"materialized_views not available: {e}")

# Tonbo wrapper
try:
    from .tonbo_wrapper import *
    AVAILABLE_MODULES.append('tonbo_wrapper')
except ImportError as e:
    logger.debug(f"tonbo_wrapper not available: {e}")

# Store backends
try:
    from .stores_base import *
    AVAILABLE_MODULES.append('stores_base')
except ImportError as e:
    logger.debug(f"stores_base not available: {e}")

try:
    from .stores_memory import *
    AVAILABLE_MODULES.append('stores_memory')
except ImportError as e:
    logger.debug(f"stores_memory not available: {e}")

# Spillable buffers (bigger-than-memory streaming)
try:
    from .buffers import SpillableBuffer, SPILLABLE_BUFFER_AVAILABLE
    AVAILABLE_MODULES.append('buffers')
except ImportError as e:
    logger.debug(f"buffers not available: {e}")
    SpillableBuffer = None
    SPILLABLE_BUFFER_AVAILABLE = False

# State management
try:
    from .state import state_backend, value_state, list_state, map_state, reducing_state, aggregating_state, rocksdb_state
    AVAILABLE_MODULES.extend(['state.state_backend', 'state.value_state', 'state.list_state',
                               'state.map_state', 'state.reducing_state', 'state.aggregating_state',
                               'state.rocksdb_state'])
except ImportError as e:
    logger.debug(f"state modules not available: {e}")

# Time management
try:
    from .time import timers, watermark_tracker, time_service, event_time
    AVAILABLE_MODULES.extend(['time.timers', 'time.watermark_tracker', 'time.time_service', 'time.event_time'])
except ImportError as e:
    logger.debug(f"time modules not available: {e}")

# Checkpoint coordination
try:
    from .checkpoint import barrier, barrier_tracker, coordinator, storage, recovery
    AVAILABLE_MODULES.extend(['checkpoint.barrier', 'checkpoint.barrier_tracker', 'checkpoint.coordinator',
                              'checkpoint.storage', 'checkpoint.recovery'])
except ImportError as e:
    logger.debug(f"checkpoint modules not available: {e}")

logger.info(f"Loaded {len(AVAILABLE_MODULES)} Cython modules: {', '.join(AVAILABLE_MODULES)}")

__all__ = ['AVAILABLE_MODULES']
