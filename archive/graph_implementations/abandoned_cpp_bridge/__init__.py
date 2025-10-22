"""
SabotGraph Module

Graph query execution (Cypher + SPARQL) for Sabot streaming pipelines.
MarbleDB state backend (like RocksDB to Flink).

Pattern: Mirrors sabot_sql module structure.
"""

from .sabot_graph_python import (
    SabotGraphBridge,
    SabotGraphOrchestrator,
    create_sabot_graph_bridge,
    distribute_graph_query
)

# Streaming (optional import)
try:
    from .sabot_graph_streaming import (
        StreamingGraphExecutor,
        create_streaming_graph_executor
    )
    STREAMING_AVAILABLE = True
except ImportError:
    STREAMING_AVAILABLE = False

__version__ = "0.1.0"
__all__ = [
    "SabotGraphBridge",
    "SabotGraphOrchestrator",
    "create_sabot_graph_bridge",
    "distribute_graph_query",
    "STREAMING_AVAILABLE"
]

# Add streaming exports if available
if STREAMING_AVAILABLE:
    __all__.extend([
        "StreamingGraphExecutor",
        "create_streaming_graph_executor"
    ])

