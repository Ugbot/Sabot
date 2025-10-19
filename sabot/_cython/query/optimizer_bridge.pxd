# cython: language_level=3
"""
Cython declarations for C++ query optimizer

Provides Python bindings with <10ns overhead for optimizer access.
"""

from libcpp.string cimport string
from libcpp.memory cimport shared_ptr, unique_ptr
from libcpp.vector cimport vector
from libcpp cimport bool as cbool

cdef extern from "sabot/query/logical_plan.h" namespace "sabot::query":
    cdef enum class LogicalOperatorType:
        SCAN
        FILTER
        PROJECT
        MAP
        AGGREGATE
        GROUP_BY
        JOIN
        WINDOW
        SORT
        LIMIT
        UNION
        DISTINCT
    
    cdef cppclass LogicalPlan:
        LogicalOperatorType GetType() const
        vector[shared_ptr[LogicalPlan]] GetChildren() const
        cbool Validate() const
        int64_t EstimateCardinality() const
        shared_ptr[LogicalPlan] Clone() const

cdef extern from "sabot/query/optimizer.h" namespace "sabot::query":
    cdef cppclass QueryOptimizer:
        QueryOptimizer() except +
        
        shared_ptr[LogicalPlan] Optimize(const shared_ptr[LogicalPlan]& plan)
        
        cppclass OptimizerStats:
            int64_t plans_optimized
            int64_t rules_applied
            double total_time_ms
            double avg_time_ms
        
        OptimizerStats GetStats() const
        void EnableRule(const string& rule_name, cbool enabled)
    
    unique_ptr[QueryOptimizer] CreateDefaultOptimizer()

cdef extern from "sabot/query/optimizer_type.h" namespace "sabot::query":
    cdef enum class OptimizerType:
        FILTER_PUSHDOWN
        PROJECTION_PUSHDOWN
        JOIN_ORDER
        CONSTANT_FOLDING
        # ... add more as needed
    
    string OptimizerTypeToString(OptimizerType type)
    OptimizerType OptimizerTypeFromString(const string& str)
    vector[string] ListAllOptimizers()

