# cython: language_level=3

from libcpp.memory cimport shared_ptr
from libcpp.string cimport string
from libcpp.vector cimport vector
from libc.stdint cimport int64_t

from pyarrow.lib cimport CTable, CResult

cdef extern from "pattern_match.h" namespace "sabot::graph::query":
    cdef struct CPatternMatchResult "sabot::graph::query::PatternMatchResult":
        shared_ptr[CTable] result_table
        int64_t num_matches
        vector[string] binding_names

    CResult[CPatternMatchResult] Match2Hop(
        const shared_ptr[CTable]& edges1,
        const shared_ptr[CTable]& edges2,
        const string& source_name,
        const string& intermediate_name,
        const string& target_name
    ) nogil

    CResult[CPatternMatchResult] Match3Hop(
        const shared_ptr[CTable]& edges1,
        const shared_ptr[CTable]& edges2,
        const shared_ptr[CTable]& edges3
    ) nogil

    CResult[CPatternMatchResult] MatchVariableLengthPath(
        const shared_ptr[CTable]& edges,
        int64_t source_vertex,
        const int64_t* target_vertex,
        int64_t min_hops,
        int64_t max_hops
    ) nogil

    vector[int] OptimizeJoinOrder(
        const vector[shared_ptr[CTable]]& edges
    ) nogil
