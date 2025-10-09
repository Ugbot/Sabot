"""
Graph Query Module

Pattern matching for graph queries.
"""

try:
    from .pattern_match import (
        match_2hop,
        match_3hop,
        match_variable_length_path,
        PyPatternMatchResult,
        OptimizeJoinOrder
    )
    __all__ = [
        'match_2hop',
        'match_3hop',
        'match_variable_length_path',
        'PyPatternMatchResult',
        'OptimizeJoinOrder'
    ]
except ImportError as e:
    print(f"Warning: Could not import pattern_match: {e}")
    __all__ = []
