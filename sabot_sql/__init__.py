"""
SabotSQL Python Module

This module provides Python bindings for SabotSQL, enabling agent-based
distributed SQL execution through Sabot's orchestrator.
"""

# Import Cython implementation (real SQL execution)
try:
    from .sabot_sql import SabotSQLBridge, create_sabot_sql_bridge
    SQL_BACKEND = "cython"
except ImportError as e:
    # Use DuckDB directly for real SQL execution (temporary until Cython builds)
    from .sabot_sql_duckdb_direct import (
        SabotSQLBridge,
        create_sabot_sql_bridge
    )
    SQL_BACKEND = "duckdb_direct"

# Streaming SQL (optional import)
try:
    from .sabot_sql_streaming import (
        StreamingSQLExecutor,
        create_streaming_executor
    )
    STREAMING_AVAILABLE = True
except ImportError:
    STREAMING_AVAILABLE = False

__version__ = "0.1.0"
__all__ = [
    "SabotSQLBridge",
    "create_sabot_sql_bridge",
    "STREAMING_AVAILABLE"
]

# Add streaming exports if available
if STREAMING_AVAILABLE:
    __all__.extend([
        "StreamingSQLExecutor",
        "create_streaming_executor"
    ])
