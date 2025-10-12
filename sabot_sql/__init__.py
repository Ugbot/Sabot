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

__version__ = "0.1.0"
__all__ = [
    "SabotSQLBridge",
    "SabotOperatorTranslator", 
    "SabotSQLOrchestrator",
    "create_sabot_sql_bridge",
    "create_operator_translator",
    "execute_sql_on_agent",
    "distribute_sql_query"
]
