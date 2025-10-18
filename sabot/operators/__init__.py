"""
Sabot Operators

Unified operator implementations and registry.
"""

from .registry import (
    OperatorRegistry,
    create_default_registry,
    get_global_registry,
    reset_global_registry
)

__all__ = [
    'OperatorRegistry',
    'create_default_registry',
    'get_global_registry',
    'reset_global_registry',
]


# Convenience function
def create_operator(name: str, *args, **kwargs):
    """
    Create an operator instance from global registry.
    
    Args:
        name: Operator name
        *args: Positional arguments
        **kwargs: Keyword arguments
        
    Returns:
        Operator instance
        
    Example:
        op = create_operator('filter', source, predicate_func)
        op = create_operator('group_by', source, ['key1', 'key2'])
    """
    registry = get_global_registry()
    return registry.create(name, *args, **kwargs)

