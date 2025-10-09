#!/usr/bin/env python3
"""
MarbleDB Python API Example

Demonstrates how to use the MarbleDB C API from Python using ctypes.
This shows how the C API enables seamless integration with Python
and other dynamic languages.
"""

import ctypes
import json
import os
import sys
from typing import Optional, List, Dict, Any

# Load the MarbleDB C library
try:
    # Try to load from current directory first (for development)
    lib_path = os.path.join(os.path.dirname(__file__), '..', 'build', 'libmarble.so')
    if not os.path.exists(lib_path):
        # Try system installation
        lib_path = 'libmarble.so'
    marble_lib = ctypes.CDLL(lib_path)
except OSError as e:
    print(f"Error loading MarbleDB library: {e}")
    print("Make sure the library is built and accessible.")
    sys.exit(1)

# Define C types and structures
class MarbleDB_Status(ctypes.Structure):
    _fields_ = [
        ('code', ctypes.c_int),
        ('message', ctypes.c_char_p)
    ]

class MarbleDB_Handle(ctypes.Structure):
    pass  # Opaque handle

class MarbleDB_QueryResult_Handle(ctypes.Structure):
    pass  # Opaque handle

# Status codes
MARBLEDB_OK = 0
MARBLEDB_INVALID_ARGUMENT = 1
MARBLEDB_NOT_FOUND = 2
MARBLEDB_INTERNAL_ERROR = 6

# Function signatures
marble_lib.marble_init.restype = MarbleDB_Status
marble_lib.marble_cleanup.argtypes = []

marble_lib.marble_db_open.argtypes = [ctypes.c_char_p, ctypes.POINTER(ctypes.POINTER(MarbleDB_Handle))]
marble_lib.marble_db_open.restype = MarbleDB_Status

marble_lib.marble_db_close.argtypes = [ctypes.POINTER(ctypes.POINTER(MarbleDB_Handle))]

marble_lib.marble_db_create_table.argtypes = [ctypes.POINTER(MarbleDB_Handle), ctypes.c_char_p, ctypes.c_char_p]
marble_lib.marble_db_create_table.restype = MarbleDB_Status

marble_lib.marble_db_insert_record.argtypes = [ctypes.POINTER(MarbleDB_Handle), ctypes.c_char_p, ctypes.c_char_p]
marble_lib.marble_db_insert_record.restype = MarbleDB_Status

marble_lib.marble_db_execute_query.argtypes = [
    ctypes.POINTER(MarbleDB_Handle),
    ctypes.c_char_p,
    ctypes.POINTER(ctypes.POINTER(MarbleDB_QueryResult_Handle))
]
marble_lib.marble_db_execute_query.restype = MarbleDB_Status

marble_lib.marble_db_query_result_has_next.argtypes = [ctypes.POINTER(MarbleDB_QueryResult_Handle), ctypes.POINTER(ctypes.c_bool)]
marble_lib.marble_db_query_result_has_next.restype = MarbleDB_Status

marble_lib.marble_db_query_result_next_json.argtypes = [ctypes.POINTER(MarbleDB_QueryResult_Handle), ctypes.POINTER(ctypes.c_char_p)]
marble_lib.marble_db_query_result_next_json.restype = MarbleDB_Status

marble_lib.marble_db_close_query_result.argtypes = [ctypes.POINTER(ctypes.POINTER(MarbleDB_QueryResult_Handle))]

marble_lib.marble_free_string.argtypes = [ctypes.c_char_p]

marble_lib.marble_db_get_version.argtypes = [ctypes.POINTER(ctypes.c_char_p)]
marble_lib.marble_db_get_version.restype = MarbleDB_Status

# Helper functions
def check_status(status: MarbleDB_Status, operation: str) -> None:
    """Check if a status indicates success, raise exception if not."""
    if status.code != MARBLEDB_OK:
        message = status.message.decode('utf-8') if status.message else "Unknown error"
        raise RuntimeError(f"Error in {operation}: {message}")

def to_c_string(s: str) -> bytes:
    """Convert Python string to C string (bytes)."""
    return s.encode('utf-8')

def from_c_string(c_str: ctypes.c_char_p) -> str:
    """Convert C string to Python string."""
    return c_str.decode('utf-8') if c_str else ""

class MarbleDB:
    """Python wrapper for MarbleDB C API."""

    def __init__(self, path: str):
        """Open a MarbleDB database."""
        self._handle = ctypes.POINTER(MarbleDB_Handle)()

        status = marble_lib.marble_db_open(to_c_string(path), ctypes.byref(self._handle))
        check_status(status, "database open")

    def __del__(self):
        """Close the database."""
        if hasattr(self, '_handle') and self._handle:
            marble_lib.marble_db_close(ctypes.byref(self._handle))

    def create_table(self, table_name: str, schema: Dict[str, Any]) -> None:
        """Create a table with the given schema."""
        schema_json = json.dumps(schema)
        status = marble_lib.marble_db_create_table(
            self._handle,
            to_c_string(table_name),
            to_c_string(schema_json)
        )
        check_status(status, "table creation")

    def insert(self, table_name: str, record: Dict[str, Any]) -> None:
        """Insert a record into a table."""
        record_json = json.dumps(record)
        status = marble_lib.marble_db_insert_record(
            self._handle,
            to_c_string(table_name),
            to_c_string(record_json)
        )
        check_status(status, "record insertion")

    def query(self, sql: str) -> 'QueryResult':
        """Execute a SQL query."""
        result_handle = ctypes.POINTER(MarbleDB_QueryResult_Handle)()
        status = marble_lib.marble_db_execute_query(
            self._handle,
            to_c_string(sql),
            ctypes.byref(result_handle)
        )
        check_status(status, "query execution")
        return QueryResult(result_handle)

class QueryResult:
    """Python wrapper for query results."""

    def __init__(self, handle: ctypes.POINTER(MarbleDB_QueryResult_Handle)):
        self._handle = handle

    def __iter__(self):
        """Iterate over result batches."""
        return self

    def __next__(self) -> List[Dict[str, Any]]:
        """Get next batch of results."""
        has_next = ctypes.c_bool()
        status = marble_lib.marble_db_query_result_has_next(self._handle, ctypes.byref(has_next))
        check_status(status, "checking result availability")

        if not has_next.value:
            raise StopIteration

        batch_json_ptr = ctypes.c_char_p()
        status = marble_lib.marble_db_query_result_next_json(self._handle, ctypes.byref(batch_json_ptr))
        check_status(status, "getting next batch")

        try:
            batch_json = from_c_string(batch_json_ptr)
            return json.loads(batch_json)
        finally:
            marble_lib.marble_free_string(batch_json_ptr)

    def __del__(self):
        """Clean up result handle."""
        if hasattr(self, '_handle') and self._handle:
            marble_lib.marble_db_close_query_result(ctypes.byref(self._handle))

def main():
    """Main example demonstrating MarbleDB Python API usage."""
    print("=========================================")
    print("🐍 MarbleDB Python API Example")
    print("=========================================\n")

    try:
        # Initialize library
        status = marble_lib.marble_init()
        check_status(status, "library initialization")
        print("✅ Library initialized")

        # Open database
        db_path = "/tmp/marble_python_example"
        db = MarbleDB(db_path)
        print("✅ Database opened")

        # Create table
        schema = {
            "fields": [
                {"name": "id", "type": "int64"},
                {"name": "name", "type": "string"},
                {"name": "department", "type": "string"},
                {"name": "salary", "type": "float64"},
                {"name": "hire_year", "type": "int64"}
            ]
        }

        db.create_table("employees", schema)
        print("✅ Table 'employees' created")

        # Insert sample data
        employees = [
            {"id": 1, "name": "Alice Johnson", "department": "Engineering", "salary": 95000.0, "hire_year": 2020},
            {"id": 2, "name": "Bob Smith", "department": "Sales", "salary": 75000.0, "hire_year": 2019},
            {"id": 3, "name": "Carol Williams", "department": "Engineering", "salary": 105000.0, "hire_year": 2018},
            {"id": 4, "name": "David Brown", "department": "Marketing", "salary": 65000.0, "hire_year": 2021},
            {"id": 5, "name": "Eve Davis", "department": "Engineering", "salary": 115000.0, "hire_year": 2017}
        ]

        for emp in employees:
            db.insert("employees", emp)

        print(f"✅ Inserted {len(employees)} employee records")

        # Query high earners in Engineering
        query = "SELECT * FROM employees WHERE department = 'Engineering' AND salary > 100000"
        results = list(db.query(query))

        print(f"\n🔍 Query: {query}")
        print(f"📊 Results: {len(results)} high-earning engineers found")

        for result in results:
            print(f"  👤 {result['name']}: ${result['salary']:,.0f} ({result['hire_year']})")

        # Query department summary
        dept_query = "SELECT * FROM employees WHERE department = 'Engineering'"
        engineering_staff = list(db.query(dept_query))

        print(f"\n🏢 Engineering Department:")
        print(f"  👥 Total staff: {len(engineering_staff)}")
        if engineering_staff:
            salaries = [emp['salary'] for emp in engineering_staff]
            avg_salary = sum(salaries) / len(salaries)
            print(".0f")
            print(f"  📈 Salary range: ${min(salaries):,.0f} - ${max(salaries):,.0f}")

        # Get version info
        version_ptr = ctypes.c_char_p()
        status = marble_lib.marble_db_get_version(ctypes.byref(version_ptr))
        check_status(status, "getting version")

        try:
            version = from_c_string(version_ptr)
            print(f"\n📋 MarbleDB version: {version}")
        finally:
            marble_lib.marble_free_string(version_ptr)

        print("\n✅ All operations completed successfully!")
        print("=========================================")

    except Exception as e:
        print(f"❌ Error: {e}")
        return 1

    finally:
        # Cleanup
        marble_lib.marble_cleanup()
        print("🧹 Library cleanup completed")

    return 0

if __name__ == "__main__":
    sys.exit(main())
