# MarbleDB API Surface Documentation

## Overview

MarbleDB provides a clean, stable API surface that can be easily wrapped for use from multiple programming languages. The API is designed around these key principles:

- **Simple and Intuitive**: Easy to understand and use operations
- **Memory Safe**: Clear ownership semantics and resource management
- **Language Agnostic**: C-compatible interface that can be wrapped in any language
- **Extensible**: JSON-based schemas and flexible query options
- **Performance Focused**: Zero-copy operations where possible

## API Layers

### 1. C++ API (`marble/api.h`)
The primary C++ interface providing type safety and RAII resource management.

```cpp
#include <marble/api.h>

// Clean, modern C++ interface
std::unique_ptr<MarbleDB> db = OpenDatabase("/path/to/db");
db->CreateTable(table_schema);
db->InsertBatch(table_name, record_batch);
auto results = db->ScanTable(table_name);
```

### 2. C API (`marble/c_api.h`)
C-compatible extern "C" interface for language interop.

```c
#include <marble/c_api.h>

// C interface with opaque pointers
MarbleDB_Handle* db;
marble_db_open("/path/to/db", &db);
marble_db_create_table(db, "table", schema_json);
marble_db_insert_record(db, "table", record_json);
marble_db_close(&db);
```

### 3. Language Bindings
Generated wrappers for specific languages (Python, Java, Rust, etc.).

```python
# Python binding
import marbledb

db = marbledb.open("/path/to/db")
db.create_table("users", schema)
db.insert("users", {"id": 1, "name": "Alice"})
results = db.query("SELECT * FROM users WHERE age > 25")
```

## Core Concepts

### Database Handle
An opaque handle representing a database connection. All operations require a valid handle.

### Tables and Schemas
Tables are defined with JSON schemas supporting multiple data types:
- `int64`, `int32` - Integer types
- `float64`, `float32` - Floating point types
- `string`/`utf8` - String types
- `bool`/`boolean` - Boolean type
- `timestamp` - Timestamp with microsecond precision

### Records and Batches
Data is inserted as individual JSON records or Arrow RecordBatches for bulk operations.

### Queries and Results
Queries use SQL-like syntax with automatic pushdown optimization. Results are streamed as JSON or Arrow data.

## API Reference

### Database Management

#### Open Database
```cpp
Status OpenDatabase(const std::string& path, std::unique_ptr<MarbleDB>* db);
```
```c
MarbleDB_Status marble_db_open(const char* path, MarbleDB_Handle** db_handle);
```

Opens or creates a MarbleDB database at the specified path.

**Parameters:**
- `path`: Filesystem path to database directory
- `db`/`db_handle`: Output parameter for database handle

**Returns:** Status indicating success or failure

#### Close Database
```cpp
void CloseDatabase(std::unique_ptr<MarbleDB>* db);
```
```c
void marble_db_close(MarbleDB_Handle** db_handle);
```

Closes a database and frees all associated resources.

**Parameters:**
- `db`/`db_handle`: Database handle to close (set to nullptr)

### Table Management

#### Create Table
```cpp
Status CreateTable(MarbleDB* db, const std::string& table_name, const std::string& schema_json);
```
```c
MarbleDB_Status marble_db_create_table(MarbleDB_Handle* db_handle, const char* table_name, const char* schema_json);
```

Creates a new table with the specified schema.

**Parameters:**
- `db`/`db_handle`: Database handle
- `table_name`: Name of table to create
- `schema_json`: JSON schema definition

**Schema Format:**
```json
{
  "fields": [
    {"name": "id", "type": "int64", "nullable": false},
    {"name": "name", "type": "string"},
    {"name": "age", "type": "int64"},
    {"name": "salary", "type": "float64"}
  ]
}
```

### Data Ingestion

#### Insert Single Record
```cpp
Status InsertRecord(MarbleDB* db, const std::string& table_name, const std::string& record_json);
```
```c
MarbleDB_Status marble_db_insert_record(MarbleDB_Handle* db_handle, const char* table_name, const char* record_json);
```

Inserts a single record into a table.

**Parameters:**
- `db`/`db_handle`: Database handle
- `table_name`: Target table name
- `record_json`: JSON representation of record

**Record Format:**
```json
{"id": 1, "name": "Alice", "age": 30, "salary": 75000.0}
```

#### Insert Record Batch
```cpp
Status InsertRecordsBatch(MarbleDB* db, const std::string& table_name, const std::string& records_json);
```
```c
MarbleDB_Status marble_db_insert_records_batch(MarbleDB_Handle* db_handle, const char* table_name, const char* records_json);
```

Inserts multiple records as a batch.

**Parameters:**
- `db`/`db_handle`: Database handle
- `table_name`: Target table name
- `records_json`: JSON array of records

**Batch Format:**
```json
[
  {"id": 1, "name": "Alice", "age": 30},
  {"id": 2, "name": "Bob", "age": 25}
]
```

#### Insert Arrow Batch
```cpp
Status InsertArrowBatch(MarbleDB* db, const std::string& table_name, const void* data, size_t size);
```
```c
MarbleDB_Status marble_db_insert_arrow_batch(MarbleDB_Handle* db_handle, const char* table_name, const void* arrow_data, size_t arrow_size);
```

Inserts records from a serialized Arrow RecordBatch (highest performance).

### Query Execution

#### Execute Query
```cpp
Status ExecuteQuery(MarbleDB* db, const std::string& query_sql, std::unique_ptr<QueryResult>* result);
```
```c
MarbleDB_Status marble_db_execute_query(MarbleDB_Handle* db_handle, const char* query_sql, MarbleDB_QueryResult_Handle** result_handle);
```

Executes a SQL-like query and returns results.

**Parameters:**
- `db`/`db_handle`: Database handle
- `query_sql`: SQL query string
- `result`/`result_handle`: Output parameter for query results

**Supported Query Syntax:**
- `SELECT * FROM table` - Scan entire table
- `SELECT * FROM table WHERE condition` - Filtered scan
- Basic WHERE clauses with comparison operators (=, >, <, >=, <=)
- AND/OR logical operators
- String and numeric literals

**Examples:**
```sql
SELECT * FROM employees WHERE age > 30
SELECT * FROM products WHERE category = 'Electronics' AND price < 1000
```

### Result Processing

#### Check for More Results
```cpp
Status QueryResultHasNext(QueryResult* result, bool* has_next);
```
```c
MarbleDB_Status marble_db_query_result_has_next(MarbleDB_QueryResult_Handle* result_handle, bool* has_next);
```

Checks if more result batches are available.

#### Get Next JSON Batch
```cpp
Status QueryResultNextJson(QueryResult* result, std::string* batch_json);
```
```c
MarbleDB_Status marble_db_query_result_next_json(MarbleDB_QueryResult_Handle* result_handle, char** batch_json);
```

Retrieves the next batch of results as JSON.

#### Get Next Arrow Batch
```cpp
Status QueryResultNextArrow(QueryResult* result, void** arrow_data, size_t* arrow_size);
```
```c
MarbleDB_Status marble_db_query_result_next_arrow(MarbleDB_QueryResult_Handle* result_handle, void** arrow_data, size_t* arrow_size);
```

Retrieves the next batch of results as serialized Arrow data.

#### Get Result Schema
```cpp
Status QueryResultGetSchema(QueryResult* result, std::string* schema_json);
```
```c
MarbleDB_Status marble_db_query_result_get_schema(MarbleDB_QueryResult_Handle* result_handle, char** schema_json);
```

Returns the schema of query results as JSON.

#### Get Row Count
```cpp
Status QueryResultGetRowCount(QueryResult* result, int64_t* row_count);
```
```c
MarbleDB_Status marble_db_query_result_get_row_count(MarbleDB_QueryResult_Handle* result_handle, int64_t* row_count);
```

Returns the total number of rows in the result set.

#### Close Results
```cpp
void CloseQueryResult(std::unique_ptr<QueryResult>* result);
```
```c
void marble_db_close_query_result(MarbleDB_QueryResult_Handle** result_handle);
```

Frees query result resources.

## Usage Patterns

### Basic CRUD Operations

```cpp
// 1. Open database
auto db = OpenDatabase("/tmp/my_database");

// 2. Create table
std::string schema = R"({
  "fields": [
    {"name": "id", "type": "int64"},
    {"name": "name", "type": "string"},
    {"name": "email", "type": "string"}
  ]
})";
db->CreateTable("users", schema);

// 3. Insert data
db->InsertRecord("users", R"({"id": 1, "name": "Alice", "email": "alice@example.com"})");
db->InsertRecord("users", R"({"id": 2, "name": "Bob", "email": "bob@example.com"})");

// 4. Query data
auto results = db->ScanTable("users");
std::string json_result;
while (results->Next(&json_result)) {
    std::cout << "Result: " << json_result << std::endl;
}

// 5. Close database
CloseDatabase(&db);
```

### Batch Operations

```cpp
// Bulk insert using JSON array
std::string batch_data = R"([
  {"id": 1, "name": "Alice", "department": "Engineering"},
  {"id": 2, "name": "Bob", "department": "Sales"},
  {"id": 3, "name": "Carol", "department": "Marketing"}
])";
db->InsertRecordsBatch("employees", batch_data);

// Query with filtering
auto filtered_results = db->ExecuteQuery("SELECT * FROM employees WHERE department = 'Engineering'");
```

### Error Handling

```cpp
Status status = db->InsertRecord("nonexistent_table", "{}");
if (!status.ok()) {
    std::cerr << "Error: " << status.message() << std::endl;
    // Handle error appropriately
}
```

### Resource Management

```cpp
// RAII ensures proper cleanup
{
    auto db = OpenDatabase("/tmp/db");
    auto results = db->ScanTable("table");

    // Use results...
    // Automatic cleanup when variables go out of scope
}
```

## Language Bindings

### Python (ctypes)

```python
import ctypes
import json

# Load library
marble = ctypes.CDLL('./libmarble.so')

# Define structures
class Status(ctypes.Structure):
    _fields_ = [('code', ctypes.c_int), ('message', ctypes.c_char_p)]

class DBHandle(ctypes.Structure):
    pass

# Function signatures
marble.marble_db_open.argtypes = [ctypes.c_char_p, ctypes.POINTER(ctypes.POINTER(DBHandle))]
marble.marble_db_open.restype = Status

# Usage
db = ctypes.POINTER(DBHandle)()
status = marble.marble_db_open(b"/tmp/db", ctypes.byref(db))
if status.code != 0:
    raise RuntimeError(status.message.decode())

# Insert and query...
```

### Java (JNI)

```java
public class MarbleDB {
    static {
        System.loadLibrary("marble");
    }

    // Native method declarations
    public native long openDatabase(String path);
    public native void closeDatabase(long handle);
    public native void createTable(long handle, String tableName, String schemaJson);
    public native void insertRecord(long handle, String tableName, String recordJson);
    public native String executeQuery(long handle, String querySql);

    // Usage
    public static void main(String[] args) {
        MarbleDB db = new MarbleDB();
        long handle = db.openDatabase("/tmp/db");

        String schema = "{\"fields\": [{\"name\": \"id\", \"type\": \"int64\"}]}";
        db.createTable(handle, "test", schema);

        db.insertRecord(handle, "test", "{\"id\": 42}");
        String results = db.executeQuery(handle, "SELECT * FROM test");

        db.closeDatabase(handle);
    }
}
```

### Rust (bindgen)

```rust
#[link(name = "marble")]
extern "C" {
    fn marble_db_open(path: *const c_char, handle: *mut *mut MarbleDB_Handle) -> MarbleDB_Status;
    fn marble_db_close(handle: *mut *mut MarbleDB_Handle);
    fn marble_db_execute_query(handle: *mut MarbleDB_Handle, query: *const c_char, result: *mut *mut QueryResult_Handle) -> MarbleDB_Status;
}

// Safe wrapper
pub struct Database {
    handle: *mut MarbleDB_Handle,
}

impl Database {
    pub fn open(path: &str) -> Result<Self, String> {
        let mut handle: *mut MarbleDB_Handle = ptr::null_mut();
        let c_path = CString::new(path).unwrap();

        let status = unsafe { marble_db_open(c_path.as_ptr(), &mut handle) };
        if status.code != 0 {
            return Err(unsafe { CStr::from_ptr(status.message) }.to_string_lossy().into_owned());
        }

        Ok(Database { handle })
    }

    pub fn execute_query(&self, query: &str) -> Result<QueryResult, String> {
        // Implementation...
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        unsafe { marble_db_close(&mut self.handle) };
    }
}
```

## Performance Considerations

### Pushdown Optimization
MarbleDB automatically pushes down projections and predicates to minimize data transfer:

```cpp
// This query only reads 'name' and 'salary' columns from disk
auto results = db->ExecuteQuery("SELECT name, salary FROM employees WHERE age > 30");
```

### Batch Processing
Use batch operations for better performance:

```cpp
// Preferred: batch insert
db->InsertRecordsBatch("table", large_json_array);

// Less efficient: individual inserts
for (const auto& record : records) {
    db->InsertRecord("table", record);
}
```

### Memory Management
- JSON results require parsing overhead
- Arrow results provide zero-copy access
- Result handles should be closed promptly to free memory

## Error Codes

- `MARBLEDB_OK` (0): Success
- `MARBLEDB_INVALID_ARGUMENT` (1): Invalid function arguments
- `MARBLEDB_NOT_FOUND` (2): Resource not found
- `MARBLEDB_ALREADY_EXISTS` (3): Resource already exists
- `MARBLEDB_PERMISSION_DENIED` (4): Permission denied
- `MARBLEDB_UNAVAILABLE` (5): Service unavailable
- `MARBLEDB_INTERNAL_ERROR` (6): Internal error
- `MARBLEDB_NOT_IMPLEMENTED` (7): Feature not implemented
- `MARBLEDB_IO_ERROR` (8): I/O error
- `MARBLEDB_CORRUPTION` (9): Data corruption
- `MARBLEDB_TIMEOUT` (10): Operation timeout

## Thread Safety

- Database handles are not thread-safe
- Multiple databases can be used from different threads
- Query results are bound to their creating thread
- Library initialization is not thread-safe (call once at startup)

## Version Compatibility

The API follows semantic versioning:
- **Major**: Breaking changes to API signatures
- **Minor**: New features and functions
- **Patch**: Bug fixes and optimizations

All functions are designed to be forward-compatible within major versions.
