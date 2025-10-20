# cython: language_level=3
"""
Cython declarations for C++ operator registry
"""

from libc.stdint cimport uint32_t, uint64_t
from libcpp.string cimport string
from libcpp.vector cimport vector
from libcpp.memory cimport unique_ptr
from libcpp cimport bool as cbool

cdef extern from "sabot/operators/registry.h" namespace "sabot::operators":
    # Use simple int for enum (Cython compatibility)
    ctypedef int OperatorType
    
    cdef cppclass CppOperatorMetadata "sabot::operators::OperatorMetadata":
        string name
        OperatorType type
        string description
        string performance_hint
        cbool is_stateful
        cbool requires_shuffle
    
    cdef cppclass OperatorRegistry:
        OperatorRegistry() except +
        
        void Register(const string& name, OperatorType type, const CppOperatorMetadata& metadata)
        OperatorType Lookup(const string& name) const
        cbool HasOperator(const string& name) const
        const CppOperatorMetadata* GetMetadata(const string& name) const
        vector[string] ListOperators() const
        
        cppclass Stats:
            size_t num_operators
            size_t num_lookups
            double avg_lookup_ns
        
        Stats GetStats() const
        void Clear()
    
    unique_ptr[OperatorRegistry] CreateDefaultRegistry()
    OperatorRegistry& GetGlobalRegistry()

