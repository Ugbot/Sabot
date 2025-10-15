"""
SabotSQL Python Module

This module provides Python bindings for SabotSQL, enabling agent-based
distributed SQL execution through Sabot's orchestrator.
"""

from .sabot_sql_python import (
    SabotSQLBridge,
    SabotOperatorTranslator,
    SabotSQLOrchestrator,
    create_sabot_sql_bridge,
    create_operator_translator,
    execute_sql_on_agent,
    distribute_sql_query
)

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
    "SabotOperatorTranslator", 
    "SabotSQLOrchestrator",
    "create_sabot_sql_bridge",
    "create_operator_translator",
    "execute_sql_on_agent",
    "distribute_sql_query",
    "STREAMING_AVAILABLE"
]

# Add streaming exports if available
if STREAMING_AVAILABLE:
    __all__.extend([
        "StreamingSQLExecutor",
        "create_streaming_executor"
    ])
