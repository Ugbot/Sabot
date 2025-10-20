"""
Query optimizer Cython bindings

Exposes C++ query optimizer to Python with minimal overhead.
"""

from .optimizer_bridge import QueryOptimizer, OptimizerStats, list_optimizers

__all__ = ['QueryOptimizer', 'OptimizerStats', 'list_optimizers']

