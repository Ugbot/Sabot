"""
Graph algorithms and representations for property path queries.

This module provides efficient CSR (Compressed Sparse Row) graph structures
and transitive closure algorithms for SPARQL property paths.
"""

try:
    # Import from Cython extension module
    from sabot_ql.bindings.python.graph import (
        build_csr_graph,
        build_reverse_csr_graph,
        transitive_closure,
        sequence_path,
        alternative_path,
        filter_by_predicate,
        PyCSRGraph as CSRGraph
    )
    __all__ = [
        'build_csr_graph',
        'build_reverse_csr_graph',
        'transitive_closure',
        'sequence_path',
        'alternative_path',
        'filter_by_predicate',
        'CSRGraph'
    ]

except ImportError as e:
    # Fallback for when Cython module isn't built yet
    import warnings
    warnings.warn(
        f"Could not import CSR graph module: {e}\n"
        "Please build sabot_ql with: cd sabot_ql/build && cmake .. && make",
        ImportWarning
    )

    def build_csr_graph(*args, **kwargs):
        raise NotImplementedError(
            "CSR graph module not built. "
            "Run: cd sabot_ql/build && cmake .. && make"
        )

    def build_reverse_csr_graph(*args, **kwargs):
        raise NotImplementedError(
            "CSR graph module not built. "
            "Run: cd sabot_ql/build && cmake .. && make"
        )

    def transitive_closure(*args, **kwargs):
        raise NotImplementedError(
            "Transitive closure module not built. "
            "Run: cd sabot_ql/build && cmake .. && make"
        )

    def sequence_path(*args, **kwargs):
        raise NotImplementedError(
            "Property path module not built. "
            "Run: cd sabot_ql/build && cmake .. && make"
        )

    def alternative_path(*args, **kwargs):
        raise NotImplementedError(
            "Property path module not built. "
            "Run: cd sabot_ql/build && cmake .. && make"
        )

    def filter_by_predicate(*args, **kwargs):
        raise NotImplementedError(
            "Property path module not built. "
            "Run: cd sabot_ql/build && cmake .. && make"
        )

    class CSRGraph:
        """Placeholder for CSR graph (module not built)."""
        def __init__(self):
            raise NotImplementedError(
                "CSR graph module not built. "
                "Run: cd sabot_ql/build && cmake .. && make"
            )

    __all__ = [
        'build_csr_graph',
        'build_reverse_csr_graph',
        'transitive_closure',
        'sequence_path',
        'alternative_path',
        'filter_by_predicate',
        'CSRGraph'
    ]
