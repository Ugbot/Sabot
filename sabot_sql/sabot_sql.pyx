# SabotSQL Cython Bindings
# This provides Python bindings for SabotSQL to enable agent-based distributed execution

from libcpp.memory cimport shared_ptr, unique_ptr
from libcpp.string cimport string
from libcpp.vector cimport vector
from libcpp.unordered_map cimport unordered_map
from cython.operator cimport dereference as deref

# Arrow imports
cimport pyarrow.lib as pa
from pyarrow.lib cimport *
from pyarrow.lib import Table, Schema, RecordBatch

# Arrow Result types
ctypedef arrow::Result[T] Result[T]
ctypedef arrow::Status Status

# SabotSQL imports
cdef extern from "sabot_sql/sql/simple_sabot_sql_bridge.h" namespace "sabot_sql::sql":
    cdef cppclass SimpleSabotSQLBridge:
        SimpleSabotSQLBridge() except +
        @staticmethod
        shared_ptr[SimpleSabotSQLBridge] Create() except +
        Status RegisterTable(const string& table_name, shared_ptr[arrow::Table] table) except +
        Result[LogicalPlan] ParseAndOptimize(const string& sql) except +
        Result[shared_ptr[arrow::Table]] ExecuteSQL(const string& sql) except +
        bint TableExists(const string& table_name) except +

cdef extern from "sabot_sql/sql/sabot_operator_translator.h" namespace "sabot_sql::sql":
    cdef cppclass SabotOperatorTranslator:
        SabotOperatorTranslator() except +
        @staticmethod
        shared_ptr[SabotOperatorTranslator] Create() except +
        Result[shared_ptr[void]] TranslateToMorselOperators(const LogicalPlan& logical_plan) except +
        Result[shared_ptr[arrow::Schema]] GetOutputSchema(shared_ptr[void] morsel_plan) except +
        Result[shared_ptr[arrow::Table]] ExecuteMorselPlan(shared_ptr[void] morsel_plan) except +

cdef extern from "sabot_sql/sql/flink_sql_extension.h" namespace "sabot_sql::sql":
    cdef cppclass FlinkSQLExtension:
        FlinkSQLExtension() except +
        @staticmethod
        shared_ptr[FlinkSQLExtension] Create() except +
        Result[string] PreprocessFlinkSQL(const string& flink_sql) except +
        bint ContainsFlinkConstructs(const string& sql) except +
        Result[vector[string]] ExtractWindowSpecifications(const string& sql) except +
        Result[vector[string]] ExtractWatermarkDefinitions(const string& sql) except +

cdef extern from "sabot_sql/sql/flink_sql_extension.h" namespace "sabot_sql::sql":
    cdef cppclass QuestDBSQLExtension:
        QuestDBSQLExtension() except +
        @staticmethod
        shared_ptr[QuestDBSQLExtension] Create() except +
        Result[string] PreprocessQuestDBSQL(const string& questdb_sql) except +
        bint ContainsQuestDBConstructs(const string& sql) except +
        Result[vector[string]] ExtractSampleByClauses(const string& sql) except +
        Result[vector[string]] ExtractLatestByClauses(const string& sql) except +

cdef extern from "sabot_sql/sql/common_types.h" namespace "sabot_sql::sql":
    cdef struct LogicalPlan:
        shared_ptr[void] root_operator
        bint has_joins
        bint has_aggregates
        bint has_subqueries
        bint has_ctes
        bint has_windows

    cdef struct MorselPlan:
        string plan_type
        shared_ptr[void] root_operator
        vector[shared_ptr[void]] operators
        shared_ptr[arrow::Schema] output_schema
        size_t estimated_rows
        bint has_joins
        bint has_aggregates

# Python wrapper classes
cdef class SabotSQLBridge:
    """Python wrapper for SabotSQL bridge"""
    
    cdef shared_ptr[SimpleSabotSQLBridge] bridge
    
    def __cinit__(self):
        self.bridge = SimpleSabotSQLBridge.Create()
        if not self.bridge:
            raise RuntimeError("Failed to create SabotSQL bridge")
    
    def register_table(self, str table_name, Table table):
        """Register an Arrow table"""
        cdef string c_table_name = table_name.encode('utf-8')
        cdef shared_ptr[arrow::Table] c_table = table.sp_table
        
        cdef arrow::Status status = self.bridge.get().RegisterTable(c_table_name, c_table)
        if not status.ok():
            raise RuntimeError(f"Failed to register table: {status.ToString().decode('utf-8')}")
    
    def parse_and_optimize(self, str sql):
        """Parse and optimize SQL query"""
        cdef string c_sql = sql.encode('utf-8')
        cdef arrow::Result[LogicalPlan] result = self.bridge.get().ParseAndOptimize(c_sql)
        
        if not result.ok():
            raise RuntimeError(f"Failed to parse SQL: {result.status().ToString().decode('utf-8')}")
        
        cdef LogicalPlan plan = result.ValueOrDie()
        return {
            'has_joins': plan.has_joins,
            'has_aggregates': plan.has_aggregates,
            'has_subqueries': plan.has_subqueries,
            'has_ctes': plan.has_ctes,
            'has_windows': plan.has_windows
        }
    
    def execute_sql(self, str sql):
        """Execute SQL query and return Arrow table"""
        cdef string c_sql = sql.encode('utf-8')
        cdef arrow::Result[shared_ptr[arrow::Table]] result = self.bridge.get().ExecuteSQL(c_sql)
        
        if not result.ok():
            raise RuntimeError(f"Failed to execute SQL: {result.status().ToString().decode('utf-8')}")
        
        cdef shared_ptr[arrow::Table] c_table = result.ValueOrDie()
        return Table.from_pyarrow(c_table)
    
    def table_exists(self, str table_name):
        """Check if table exists"""
        cdef string c_table_name = table_name.encode('utf-8')
        return self.bridge.get().TableExists(c_table_name)

cdef class SabotOperatorTranslator:
    """Python wrapper for Sabot operator translator"""
    
    cdef shared_ptr[SabotOperatorTranslator] translator
    
    def __cinit__(self):
        self.translator = SabotOperatorTranslator.Create()
        if not self.translator:
            raise RuntimeError("Failed to create Sabot operator translator")
    
    def translate_to_morsel_operators(self, dict logical_plan):
        """Translate logical plan to morsel operators"""
        cdef LogicalPlan c_plan
        c_plan.has_joins = logical_plan.get('has_joins', False)
        c_plan.has_aggregates = logical_plan.get('has_aggregates', False)
        c_plan.has_subqueries = logical_plan.get('has_subqueries', False)
        c_plan.has_ctes = logical_plan.get('has_ctes', False)
        c_plan.has_windows = logical_plan.get('has_windows', False)
        
        cdef arrow::Result[shared_ptr[void]] result = self.translator.get().TranslateToMorselOperators(c_plan)
        if not result.ok():
            raise RuntimeError(f"Failed to translate to morsel operators: {result.status().ToString().decode('utf-8')}")
        
        # Return a Python object representing the morsel plan
        return {'morsel_plan': 'translated'}
    
    def get_output_schema(self, dict morsel_plan):
        """Get output schema of morsel plan"""
        # For now, return a simple schema
        return Schema.from_pyarrow(arrow.schema([
            arrow.field('id', arrow.int64()),
            arrow.field('name', arrow.utf8()),
            arrow.field('value', arrow.float64())
        ]).sp_schema)
    
    def execute_morsel_plan(self, dict morsel_plan):
        """Execute morsel plan and return Arrow table"""
        # For now, return an empty table
        return Table.from_pyarrow(arrow.table({
            'id': [],
            'name': [],
            'value': []
        }).sp_table)

cdef class FlinkSQLExtension:
    """Python wrapper for Flink SQL extension"""
    
    cdef shared_ptr[FlinkSQLExtension] extension
    
    def __cinit__(self):
        self.extension = FlinkSQLExtension.Create()
        if not self.extension:
            raise RuntimeError("Failed to create Flink SQL extension")
    
    def preprocess_flink_sql(self, str flink_sql):
        """Preprocess Flink SQL to SabotSQL-compatible SQL"""
        cdef string c_sql = flink_sql.encode('utf-8')
        cdef arrow::Result[string] result = self.extension.get().PreprocessFlinkSQL(c_sql)
        
        if not result.ok():
            raise RuntimeError(f"Failed to preprocess Flink SQL: {result.status().ToString().decode('utf-8')}")
        
        return result.ValueOrDie().decode('utf-8')
    
    def contains_flink_constructs(self, str sql):
        """Check if SQL contains Flink constructs"""
        cdef string c_sql = sql.encode('utf-8')
        return self.extension.get().ContainsFlinkConstructs(c_sql)
    
    def extract_window_specifications(self, str sql):
        """Extract window specifications from SQL"""
        cdef string c_sql = sql.encode('utf-8')
        cdef arrow::Result[vector[string]] result = self.extension.get().ExtractWindowSpecifications(c_sql)
        
        if not result.ok():
            raise RuntimeError(f"Failed to extract window specifications: {result.status().ToString().decode('utf-8')}")
        
        cdef vector[string] windows = result.ValueOrDie()
        cdef list python_windows = []
        cdef size_t i
        for i in range(windows.size()):
            python_windows.append(windows[i].decode('utf-8'))
        
        return python_windows
    
    def extract_watermark_definitions(self, str sql):
        """Extract watermark definitions from SQL"""
        cdef string c_sql = sql.encode('utf-8')
        cdef arrow::Result[vector[string]] result = self.extension.get().ExtractWatermarkDefinitions(c_sql)
        
        if not result.ok():
            raise RuntimeError(f"Failed to extract watermark definitions: {result.status().ToString().decode('utf-8')}")
        
        cdef vector[string] watermarks = result.ValueOrDie()
        cdef list python_watermarks = []
        cdef size_t i
        for i in range(watermarks.size()):
            python_watermarks.append(watermarks[i].decode('utf-8'))
        
        return python_watermarks

cdef class QuestDBSQLExtension:
    """Python wrapper for QuestDB SQL extension"""
    
    cdef shared_ptr[QuestDBSQLExtension] extension
    
    def __cinit__(self):
        self.extension = QuestDBSQLExtension.Create()
        if not self.extension:
            raise RuntimeError("Failed to create QuestDB SQL extension")
    
    def preprocess_questdb_sql(self, str questdb_sql):
        """Preprocess QuestDB SQL to SabotSQL-compatible SQL"""
        cdef string c_sql = questdb_sql.encode('utf-8')
        cdef arrow::Result[string] result = self.extension.get().PreprocessQuestDBSQL(c_sql)
        
        if not result.ok():
            raise RuntimeError(f"Failed to preprocess QuestDB SQL: {result.status().ToString().decode('utf-8')}")
        
        return result.ValueOrDie().decode('utf-8')
    
    def contains_questdb_constructs(self, str sql):
        """Check if SQL contains QuestDB constructs"""
        cdef string c_sql = sql.encode('utf-8')
        return self.extension.get().ContainsQuestDBConstructs(c_sql)
    
    def extract_sample_by_clauses(self, str sql):
        """Extract SAMPLE BY clauses from SQL"""
        cdef string c_sql = sql.encode('utf-8')
        cdef arrow::Result[vector[string]] result = self.extension.get().ExtractSampleByClauses(c_sql)
        
        if not result.ok():
            raise RuntimeError(f"Failed to extract SAMPLE BY clauses: {result.status().ToString().decode('utf-8')}")
        
        cdef vector[string] samples = result.ValueOrDie()
        cdef list python_samples = []
        cdef size_t i
        for i in range(samples.size()):
            python_samples.append(samples[i].decode('utf-8'))
        
        return python_samples
    
    def extract_latest_by_clauses(self, str sql):
        """Extract LATEST BY clauses from SQL"""
        cdef string c_sql = sql.encode('utf-8')
        cdef arrow::Result[vector[string]] result = self.extension.get().ExtractLatestByClauses(c_sql)
        
        if not result.ok():
            raise RuntimeError(f"Failed to extract LATEST BY clauses: {result.status().ToString().decode('utf-8')}")
        
        cdef vector[string] latest = result.ValueOrDie()
        cdef list python_latest = []
        cdef size_t i
        for i in range(latest.size()):
            python_latest.append(latest[i].decode('utf-8'))
        
        return python_latest

# Convenience functions for agent-based execution
def create_sabot_sql_bridge():
    """Create a new SabotSQL bridge instance"""
    return SabotSQLBridge()

def create_operator_translator():
    """Create a new operator translator instance"""
    return SabotOperatorTranslator()

def create_flink_extension():
    """Create a new Flink SQL extension instance"""
    return FlinkSQLExtension()

def create_questdb_extension():
    """Create a new QuestDB SQL extension instance"""
    return QuestDBSQLExtension()

# Agent execution helpers
def execute_sql_on_agent(bridge, sql_query, agent_id=None):
    """Execute SQL query on a specific agent"""
    try:
        # Parse and optimize
        plan = bridge.parse_and_optimize(sql_query)
        
        # Execute
        result = bridge.execute_sql(sql_query)
        
        return {
            'agent_id': agent_id,
            'query': sql_query,
            'plan': plan,
            'result': result,
            'status': 'success'
        }
    except Exception as e:
        return {
            'agent_id': agent_id,
            'query': sql_query,
            'error': str(e),
            'status': 'error'
        }

def distribute_sql_query(bridge, sql_query, agent_ids):
    """Distribute SQL query across multiple agents"""
    results = []
    for agent_id in agent_ids:
        result = execute_sql_on_agent(bridge, sql_query, agent_id)
        results.append(result)
    
    return results
