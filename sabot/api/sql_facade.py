#!/usr/bin/env python3
"""
SQL API Facade

Unified SQL API that wraps existing sabot_sql implementation.
Provides integration with unified Sabot engine.
"""

import logging
from typing import Optional, Any, Dict

logger = logging.getLogger(__name__)


class SQLAPI:
    """
    SQL processing API facade.
    
    Wraps sabot_sql (DuckDB fork) and integrates with unified engine.
    Provides SQL execution with streaming semantics.
    
    Example:
        engine = Sabot()
        result = engine.sql("SELECT * FROM table WHERE x > 10")
        
        # Or call execute directly
        result = engine.sql.execute("SELECT customer_id, SUM(amount) FROM orders GROUP BY customer_id")
    """
    
    def __init__(self, engine):
        """
        Initialize SQL API.
        
        Args:
            engine: Parent Sabot engine instance
        """
        self.engine = engine
        self._sql_bridge = None
    
    def _get_bridge(self):
        """Lazy-load SQL bridge."""
        if self._sql_bridge is None:
            try:
                from sabot_sql import create_sabot_sql_bridge
                self._sql_bridge = create_sabot_sql_bridge()
                logger.info("Initialized sabot_sql bridge")
            except ImportError as e:
                logger.error(f"Failed to load sabot_sql: {e}")
                raise ImportError(
                    "sabot_sql not available. Ensure it's built and installed."
                ) from e
        
        return self._sql_bridge
    
    def execute(self, query: str, **options) -> Any:
        """
        Execute SQL query.
        
        Args:
            query: SQL query string
            **options: Execution options
            
        Returns:
            Query result (Arrow table or stream)
            
        Example:
            result = engine.sql.execute("SELECT * FROM table WHERE x > 10")
            
            # With options
            result = engine.sql.execute(
                "SELECT * FROM large_table",
                streaming=True,
                batch_size=10000
            )
        """
        bridge = self._get_bridge()
        
        try:
            # Execute via sabot_sql
            result = bridge.execute_sql(query)
            
            # Wrap result to integrate with engine
            return self._wrap_result(result, options)
            
        except Exception as e:
            logger.error(f"SQL execution failed: {e}")
            raise
    
    def _wrap_result(self, result, options):
        """Wrap SQL result for unified API."""
        # If streaming mode, return as Stream
        if options.get('streaming', False):
            from sabot.api.stream import Stream
            return Stream.from_table(result, batch_size=options.get('batch_size', 100000))
        
        # Otherwise return raw table
        return result
    
    def register_table(self, name: str, table: Any):
        """
        Register table for SQL queries.
        
        Args:
            name: Table name
            table: Arrow table or compatible object
            
        Example:
            engine.sql.register_table('events', events_table)
            result = engine.sql.execute("SELECT * FROM events WHERE status = 'active'")
        """
        bridge = self._get_bridge()
        
        try:
            bridge.register_table(name, table)
            logger.info(f"Registered SQL table: {name}")
        except Exception as e:
            logger.error(f"Failed to register table {name}: {e}")
            raise
    
    def unregister_table(self, name: str):
        """
        Unregister table.
        
        Args:
            name: Table name to unregister
        """
        bridge = self._get_bridge()
        
        try:
            bridge.unregister_table(name)
            logger.info(f"Unregistered SQL table: {name}")
        except Exception as e:
            logger.error(f"Failed to unregister table {name}: {e}")
            raise
    
    def list_tables(self) -> list:
        """
        List registered tables.
        
        Returns:
            List of table names
        """
        bridge = self._get_bridge()
        
        try:
            return bridge.list_tables()
        except Exception as e:
            logger.error(f"Failed to list tables: {e}")
            return []
    
    def explain(self, query: str) -> str:
        """
        Get query execution plan.
        
        Args:
            query: SQL query
            
        Returns:
            Execution plan as string
            
        Example:
            plan = engine.sql.explain("SELECT * FROM table WHERE x > 10")
            print(plan)
        """
        bridge = self._get_bridge()
        
        try:
            return bridge.explain(query)
        except Exception as e:
            logger.error(f"Failed to explain query: {e}")
            raise
    
    def __call__(self, query: str, **options) -> Any:
        """
        Convenience: Call SQL API directly.
        
        Args:
            query: SQL query
            **options: Execution options
            
        Returns:
            Query result
            
        Example:
            result = engine.sql("SELECT * FROM table")
        """
        return self.execute(query, **options)
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get SQL engine statistics.
        
        Returns:
            Dict with SQL stats
        """
        stats = {'api': 'sql'}
        
        if self._sql_bridge:
            try:
                stats['tables_registered'] = len(self.list_tables())
                stats['bridge_loaded'] = True
            except:
                stats['bridge_loaded'] = False
        else:
            stats['bridge_loaded'] = False
        
        return stats

