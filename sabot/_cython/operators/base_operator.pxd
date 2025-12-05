# cython: language_level=3

from sabot._c.arrow_core cimport ca


cdef class BaseOperator:
    """Base class for all Cython streaming operators."""

    cdef:
        object _source              # Upstream operator or iterable
        object _schema              # Arrow schema (optional)
        bint _stateful              # Does this op have keyed state?
        list _key_columns           # Key columns for partitioning
        int _parallelism_hint       # Suggested parallelism

    cpdef object process_batch(self, object batch)
    cpdef object process_morsel(self, object morsel)
    cpdef bint requires_shuffle(self)
    cpdef list get_partition_keys(self)
    cpdef int get_parallelism_hint(self)
    cpdef object get_schema(self)
    cpdef bint is_stateful(self)
    cpdef str get_operator_name(self)
    cpdef object get_source(self)
