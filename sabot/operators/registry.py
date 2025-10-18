#!/usr/bin/env python3
"""
Operator Registry

Central registry for all physical operators in Sabot.
Provides single source of truth for operator implementations.
"""

import logging
from typing import Dict, Type, Any, List, Optional, Callable

logger = logging.getLogger(__name__)


class OperatorRegistry:
    """
    Central registry for physical operators.
    
    All engines (Stream, SQL, Graph) register their operators here,
    ensuring single implementation and consistent behavior.
    
    Example:
        registry = OperatorRegistry()
        registry.register('group_by', CythonGroupByOperator)
        
        # Create operator instance
        op = registry.create('group_by', source, ['key'])
    """
    
    def __init__(self):
        self._operators: Dict[str, Type] = {}
        self._factories: Dict[str, Callable] = {}
        self._metadata: Dict[str, Dict[str, Any]] = {}
    
    def register(
        self,
        name: str,
        operator_class: Type,
        factory: Optional[Callable] = None,
        metadata: Optional[Dict[str, Any]] = None
    ):
        """
        Register an operator.
        
        Args:
            name: Operator name (e.g., 'group_by', 'hash_join')
            operator_class: Operator class
            factory: Optional factory function for custom creation
            metadata: Optional metadata (description, performance characteristics)
        """
        if name in self._operators:
            logger.warning(f"Overwriting existing operator: {name}")
        
        self._operators[name] = operator_class
        
        if factory:
            self._factories[name] = factory
        
        if metadata:
            self._metadata[name] = metadata
        
        logger.debug(f"Registered operator: {name} ({operator_class.__name__})")
    
    def get(self, name: str) -> Optional[Type]:
        """
        Get operator class by name.
        
        Args:
            name: Operator name
            
        Returns:
            Operator class or None if not found
        """
        return self._operators.get(name)
    
    def create(self, name: str, *args, **kwargs) -> Any:
        """
        Create operator instance.
        
        Args:
            name: Operator name
            *args: Positional arguments for operator constructor
            **kwargs: Keyword arguments for operator constructor
            
        Returns:
            Operator instance
            
        Raises:
            ValueError: If operator not found
        """
        if name not in self._operators:
            raise ValueError(
                f"Unknown operator: {name}. "
                f"Available: {', '.join(self.list_operators())}"
            )
        
        # Use factory if available
        if name in self._factories:
            return self._factories[name](*args, **kwargs)
        
        # Otherwise use constructor
        return self._operators[name](*args, **kwargs)
    
    def list_operators(self) -> List[str]:
        """
        List all registered operators.
        
        Returns:
            List of operator names
        """
        return list(self._operators.keys())
    
    def get_metadata(self, name: str) -> Optional[Dict[str, Any]]:
        """
        Get metadata for an operator.
        
        Args:
            name: Operator name
            
        Returns:
            Metadata dict or None
        """
        return self._metadata.get(name)
    
    def has_operator(self, name: str) -> bool:
        """
        Check if operator is registered.
        
        Args:
            name: Operator name
            
        Returns:
            True if operator exists
        """
        return name in self._operators
    
    def unregister(self, name: str):
        """
        Remove an operator from registry.
        
        Args:
            name: Operator name
        """
        if name in self._operators:
            del self._operators[name]
            logger.debug(f"Unregistered operator: {name}")
        
        if name in self._factories:
            del self._factories[name]
        
        if name in self._metadata:
            del self._metadata[name]
    
    def clear(self):
        """Clear all registered operators."""
        self._operators.clear()
        self._factories.clear()
        self._metadata.clear()
        logger.debug("Cleared operator registry")


def create_default_registry() -> OperatorRegistry:
    """
    Create operator registry with all default operators.
    
    Registers operators from:
    - sabot._cython.operators (Cython implementations)
    - sabot_sql operators (SQL-specific)
    - sabot_cypher operators (Graph-specific)
    
    Returns:
        OperatorRegistry: Registry with default operators
    """
    registry = OperatorRegistry()
    
    # Register transform operators
    try:
        from sabot._cython.operators.transform import (
            CythonFilterOperator,
            CythonMapOperator,
            CythonSelectOperator,
            CythonFlatMapOperator,
            CythonUnionOperator
        )
        
        registry.register('filter', CythonFilterOperator, metadata={
            'type': 'transform',
            'description': 'Filter rows based on predicate',
            'performance': '10-500M rows/sec (SIMD)'
        })
        
        registry.register('map', CythonMapOperator, metadata={
            'type': 'transform',
            'description': 'Transform rows with function',
            'performance': '10-100M rows/sec'
        })
        
        registry.register('select', CythonSelectOperator, metadata={
            'type': 'transform',
            'description': 'Project columns (zero-copy)',
            'performance': '50-1000M rows/sec'
        })
        
        registry.register('flat_map', CythonFlatMapOperator, metadata={
            'type': 'transform',
            'description': '1-to-N row expansion',
            'performance': '5-50M rows/sec'
        })
        
        registry.register('union', CythonUnionOperator, metadata={
            'type': 'transform',
            'description': 'Merge multiple streams',
            'performance': '5-50M rows/sec'
        })
        
        logger.info("Registered transform operators")
        
    except ImportError as e:
        logger.warning(f"Failed to import transform operators: {e}")
    
    # Register aggregation operators
    try:
        from sabot._cython.operators.aggregations import (
            CythonGroupByOperator,
            CythonAggregateOperator,
            CythonReduceOperator,
            CythonDistinctOperator
        )
        
        registry.register('group_by', CythonGroupByOperator, metadata={
            'type': 'aggregation',
            'description': 'Group by keys and aggregate',
            'performance': '5-100M rows/sec'
        })
        
        registry.register('aggregate', CythonAggregateOperator, metadata={
            'type': 'aggregation',
            'description': 'Global aggregation (sum, mean, etc.)',
            'performance': '2-50M rows/sec'
        })
        
        registry.register('reduce', CythonReduceOperator, metadata={
            'type': 'aggregation',
            'description': 'Custom reduction function',
            'performance': '1-10M rows/sec'
        })
        
        registry.register('distinct', CythonDistinctOperator, metadata={
            'type': 'aggregation',
            'description': 'Deduplication',
            'performance': '1-10M rows/sec'
        })
        
        logger.info("Registered aggregation operators")
        
    except ImportError as e:
        logger.warning(f"Failed to import aggregation operators: {e}")
    
    # Register join operators
    try:
        from sabot._cython.operators.joins import (
            CythonHashJoinOperator,
            CythonIntervalJoinOperator,
            CythonAsofJoinOperator
        )
        
        registry.register('hash_join', CythonHashJoinOperator, metadata={
            'type': 'join',
            'description': 'Hash join on keys',
            'performance': '0.15-0.17M rows/sec (build+probe)'
        })
        
        registry.register('interval_join', CythonIntervalJoinOperator, metadata={
            'type': 'join',
            'description': 'Time-based interval join',
            'performance': '1500M rows/sec'
        })
        
        registry.register('asof_join', CythonAsofJoinOperator, metadata={
            'type': 'join',
            'description': 'Point-in-time join (backward/forward)',
            'performance': '22,221M rows/sec'
        })
        
        logger.info("Registered join operators")
        
    except ImportError as e:
        logger.warning(f"Failed to import join operators: {e}")
    
    # Register window operators
    try:
        from sabot._cython.arrow.window_processor import WindowProcessor
        
        registry.register('window', WindowProcessor, metadata={
            'type': 'window',
            'description': 'Windowed aggregations (tumbling, sliding, session)',
            'performance': 'Variable based on window type'
        })
        
        logger.info("Registered window operators")
        
    except ImportError as e:
        logger.warning(f"Failed to import window operators: {e}")
    
    # Register SQL operators (if available)
    try:
        from sabot.engines.sql.operators import register_sql_operators
        register_sql_operators(registry)
        logger.info("Registered SQL operators")
    except ImportError:
        logger.debug("SQL operators not available")
    
    # Register Graph operators (if available)
    try:
        from sabot.engines.graph.operators import register_graph_operators
        register_graph_operators(registry)
        logger.info("Registered graph operators")
    except ImportError:
        logger.debug("Graph operators not available")
    
    return registry


# Global registry instance (singleton pattern)
_global_registry: Optional[OperatorRegistry] = None


def get_global_registry() -> OperatorRegistry:
    """
    Get the global operator registry.
    
    Creates registry on first access (lazy initialization).
    
    Returns:
        OperatorRegistry: Global registry instance
    """
    global _global_registry
    
    if _global_registry is None:
        _global_registry = create_default_registry()
    
    return _global_registry


def reset_global_registry():
    """Reset global registry (mainly for testing)."""
    global _global_registry
    _global_registry = None

