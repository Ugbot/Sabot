# cython: language_level=3
"""
SabotCypher - Cypher Query Engine with Arrow Execution

Cython wrapper for sabot_cypher C++ library.
Provides zero-copy integration with PyArrow.
"""

from libc.stdint cimport int64_t, uint64_t
from libc.stdlib cimport malloc, free
from libcpp.string cimport string
from libcpp.vector cimport vector
from libcpp.map cimport map
from libcpp.memory cimport shared_ptr
from libcpp cimport bool as cbool

# Import PyArrow
cimport pyarrow.lib as pa
from pyarrow.lib cimport CTable, CRecordBatch, CSchema, CArray


# Need to include logical_plan_translator.h for ArrowPlan
cdef extern from "sabot_cypher/cypher/logical_plan_translator.h" namespace "sabot_cypher::cypher":
    
    cdef cppclass CArrowOperatorDesc "sabot_cypher::cypher::ArrowOperatorDesc":
        string type
        map[string, string] params
        cbool is_stateful
        cbool is_broadcast
        
        CArrowOperatorDesc() except +
    
    cdef cppclass CArrowPlan "sabot_cypher::cypher::ArrowPlan":
        vector[CArrowOperatorDesc] operators
        shared_ptr[CSchema] output_schema
        cbool has_joins
        cbool has_aggregates
        cbool has_filters
        vector[string] join_keys
        vector[string] group_by_keys
        
        CArrowPlan() except +


cdef extern from "sabot_cypher/cypher/sabot_cypher_bridge.h" namespace "sabot_cypher::cypher":
    
    cdef cppclass CCypherResult "sabot_cypher::cypher::CypherResult":
        shared_ptr[CTable] table
        string query
        double execution_time_ms
        size_t num_rows
    
    cdef cppclass CSabotCypherBridge "sabot_cypher::cypher::SabotCypherBridge":
        # Note: Create() returns arrow::Result, we'll handle it differently
        pass


# Helper function to create bridge (calls C++ directly)
cdef extern from "sabot_cypher/cypher/sabot_cypher_bridge.h" namespace "sabot_cypher::cypher::SabotCypherBridge":
    cdef shared_ptr[CSabotCypherBridge] Create "sabot_cypher::cypher::SabotCypherBridge::Create"() except +


cdef extern from "sabot_cypher/cypher/sabot_cypher_bridge.h" namespace "sabot_cypher::cypher":
    # Member functions need object instance
    cdef void RegisterGraph "RegisterGraph"(CSabotCypherBridge* self, shared_ptr[CTable] vertices, shared_ptr[CTable] edges) except +
    cdef CCypherResult ExecutePlan "ExecutePlan"(CSabotCypherBridge* self, const CArrowPlan& plan) except +


# Python wrapper class
cdef class SabotCypherBridge:
    """
    SabotCypher bridge for executing Cypher queries with Arrow.
    
    Examples:
        >>> bridge = SabotCypherBridge()
        >>> bridge.register_graph(vertices_table, edges_table)
        >>> result = bridge.execute_plan(plan_dict)
        >>> print(result['table'])
    """
    
    cdef shared_ptr[CSabotCypherBridge] _bridge
    
    def __cinit__(self):
        """Initialize the bridge."""
        self._bridge = CSabotCypherBridge.Create()
        if not self._bridge:
            raise RuntimeError("Failed to create SabotCypherBridge")
    
    def register_graph(self, object vertices_table, object edges_table):
        """
        Register graph data for querying.
        
        Args:
            vertices_table: PyArrow table with vertices
            edges_table: PyArrow table with edges
        """
        cdef shared_ptr[CTable] c_vertices
        cdef shared_ptr[CTable] c_edges
        
        # Convert PyArrow tables to C++ tables
        c_vertices = pa.pyarrow_unwrap_table(vertices_table)
        c_edges = pa.pyarrow_unwrap_table(edges_table)
        
        # Register with bridge
        self._bridge.get().RegisterGraph(c_vertices, c_edges)
    
    def execute_plan(self, dict plan_dict):
        """
        Execute an ArrowPlan.
        
        Args:
            plan_dict: Dictionary with 'operators' list
        
        Returns:
            dict with 'table', 'query', 'execution_time_ms', 'num_rows'
        """
        cdef CArrowPlan plan
        cdef CArrowOperatorDesc op_desc
        
        # Build ArrowPlan from Python dict
        if 'operators' in plan_dict:
            for op in plan_dict['operators']:
                op_desc.type = op['type'].encode('utf-8')
                
                if 'params' in op:
                    for key, val in op['params'].items():
                        op_desc.params[key.encode('utf-8')] = str(val).encode('utf-8')
                
                plan.operators.push_back(op_desc)
                op_desc.params.clear()  # Reset for next operator
        
        # Set flags
        plan.has_joins = plan_dict.get('has_joins', False)
        plan.has_aggregates = plan_dict.get('has_aggregates', False)
        plan.has_filters = plan_dict.get('has_filters', False)
        
        # Execute plan
        cdef CCypherResult result = self._bridge.get().ExecutePlan(plan)
        
        # Convert result to Python
        return {
            'table': pa.pyarrow_wrap_table(result.table),
            'query': result.query.decode('utf-8'),
            'execution_time_ms': result.execution_time_ms,
            'num_rows': result.num_rows,
        }


# Module-level convenience function
def create_bridge():
    """Create a new SabotCypherBridge instance."""
    return SabotCypherBridge()


# Version info
__version__ = "0.1.0"
__kuzu_version__ = "0.11.2 (vendored frontend only)"

