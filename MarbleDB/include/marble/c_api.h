/**
 * MarbleDB C API - Extern "C" Interface
 *
 * C-compatible interface that can be called from any language
 * that supports C interop (Python, Java, Rust, Go, etc.).
 *
 * This wraps the C++ API with opaque pointers and C function signatures.
 */

#pragma once

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

//==============================================================================
// Opaque Types (Forward Declarations)
//==============================================================================

/**
 * @brief Opaque handle to a MarbleDB database instance
 */
typedef struct MarbleDB_Handle MarbleDB_Handle;

/**
 * @brief Opaque handle to a query result iterator
 */
typedef struct MarbleDB_QueryResult_Handle MarbleDB_QueryResult_Handle;

/**
 * @brief Status codes for API operations
 */
typedef enum MarbleDB_StatusCode {
    MARBLEDB_OK = 0,
    MARBLEDB_INVALID_ARGUMENT = 1,
    MARBLEDB_NOT_FOUND = 2,
    MARBLEDB_ALREADY_EXISTS = 3,
    MARBLEDB_PERMISSION_DENIED = 4,
    MARBLEDB_UNAVAILABLE = 5,
    MARBLEDB_INTERNAL_ERROR = 6,
    MARBLEDB_NOT_IMPLEMENTED = 7,
    MARBLEDB_IO_ERROR = 8,
    MARBLEDB_CORRUPTION = 9,
    MARBLEDB_TIMEOUT = 10
} MarbleDB_StatusCode;

/**
 * @brief Status structure for API operations
 */
typedef struct MarbleDB_Status {
    MarbleDB_StatusCode code;
    const char* message;  // Owned by the status, don't free
} MarbleDB_Status;

//==============================================================================
// Database Management API
//==============================================================================

/**
 * @brief Open or create a MarbleDB database
 *
 * @param path Path to the database directory (UTF-8 encoded)
 * @param db_handle Output parameter for database handle
 * @return Status of the operation
 */
MarbleDB_Status marble_db_open(const char* path, MarbleDB_Handle** db_handle);

/**
 * @brief Close a MarbleDB database
 *
 * @param db_handle Database handle to close (will be invalidated)
 */
void marble_db_close(MarbleDB_Handle** db_handle);

//==============================================================================
// Table Management API
//==============================================================================

/**
 * @brief Create a table in the database
 *
 * @param db_handle Database handle
 * @param table_name Name of the table to create (UTF-8 encoded)
 * @param schema_json JSON schema definition (UTF-8 encoded)
 * @return Status of the operation
 */
MarbleDB_Status marble_db_create_table(MarbleDB_Handle* db_handle,
                                      const char* table_name,
                                      const char* schema_json);

/**
 * @brief Drop a table from the database
 *
 * @param db_handle Database handle
 * @param table_name Name of the table to drop (UTF-8 encoded)
 * @return Status of the operation
 */
MarbleDB_Status marble_db_drop_table(MarbleDB_Handle* db_handle,
                                    const char* table_name);

/**
 * @brief List all tables in the database
 *
 * @param db_handle Database handle
 * @param table_names Output array of table names (caller must free with marble_free_string_array)
 * @param count Output parameter for number of tables
 * @return Status of the operation
 */
MarbleDB_Status marble_db_list_tables(MarbleDB_Handle* db_handle,
                                     const char*** table_names,
                                     size_t* count);

/**
 * @brief Get table schema as JSON
 *
 * @param db_handle Database handle
 * @param table_name Name of the table (UTF-8 encoded)
 * @param schema_json Output parameter for JSON schema (caller must free with marble_free_string)
 * @return Status of the operation
 */
MarbleDB_Status marble_db_get_table_schema(MarbleDB_Handle* db_handle,
                                          const char* table_name,
                                          char** schema_json);

//==============================================================================
// Data Ingestion API
//==============================================================================

/**
 * @brief Insert a single record into a table
 *
 * @param db_handle Database handle
 * @param table_name Target table name (UTF-8 encoded)
 * @param record_json JSON representation of the record (UTF-8 encoded)
 * @return Status of the operation
 */
MarbleDB_Status marble_db_insert_record(MarbleDB_Handle* db_handle,
                                       const char* table_name,
                                       const char* record_json);

/**
 * @brief Insert multiple records into a table (batch operation)
 *
 * @param db_handle Database handle
 * @param table_name Target table name (UTF-8 encoded)
 * @param records_json JSON array of records (UTF-8 encoded)
 * @return Status of the operation
 */
MarbleDB_Status marble_db_insert_records_batch(MarbleDB_Handle* db_handle,
                                              const char* table_name,
                                              const char* records_json);

/**
 * @brief Insert records from Arrow RecordBatch
 *
 * @param db_handle Database handle
 * @param table_name Target table name (UTF-8 encoded)
 * @param arrow_data Serialized Arrow RecordBatch data
 * @param arrow_size Size of the serialized data
 * @return Status of the operation
 */
MarbleDB_Status marble_db_insert_arrow_batch(MarbleDB_Handle* db_handle,
                                            const char* table_name,
                                            const void* arrow_data,
                                            size_t arrow_size);

//==============================================================================
// Query API
//==============================================================================

/**
 * @brief Execute a SQL-like query
 *
 * @param db_handle Database handle
 * @param query_sql SQL query string (UTF-8 encoded)
 * @param result_handle Output parameter for query result handle
 * @return Status of the operation
 */
MarbleDB_Status marble_db_execute_query(MarbleDB_Handle* db_handle,
                                       const char* query_sql,
                                       MarbleDB_QueryResult_Handle** result_handle);

/**
 * @brief Execute a query with options
 *
 * @param db_handle Database handle
 * @param query_sql SQL query string (UTF-8 encoded)
 * @param options_json JSON options for query execution (UTF-8 encoded)
 * @param result_handle Output parameter for query result handle
 * @return Status of the operation
 */
MarbleDB_Status marble_db_execute_query_with_options(MarbleDB_Handle* db_handle,
                                                    const char* query_sql,
                                                    const char* options_json,
                                                    MarbleDB_QueryResult_Handle** result_handle);

//==============================================================================
// Query Result API
//==============================================================================

/**
 * @brief Check if query result has more data
 *
 * @param result_handle Query result handle
 * @param has_next Output boolean indicating if more data is available
 * @return Status of the operation
 */
MarbleDB_Status marble_db_query_result_has_next(MarbleDB_QueryResult_Handle* result_handle,
                                               bool* has_next);

/**
 * @brief Get next batch of results as JSON
 *
 * @param result_handle Query result handle
 * @param batch_json Output parameter for JSON representation of result batch (caller must free)
 * @return Status of the operation
 */
MarbleDB_Status marble_db_query_result_next_json(MarbleDB_QueryResult_Handle* result_handle,
                                                char** batch_json);

/**
 * @brief Get next batch of results as Arrow data
 *
 * @param result_handle Query result handle
 * @param arrow_data Output buffer for serialized Arrow data (caller must free with marble_free_arrow_data)
 * @param arrow_size Output size of the serialized data
 * @return Status of the operation
 */
MarbleDB_Status marble_db_query_result_next_arrow(MarbleDB_QueryResult_Handle* result_handle,
                                                 void** arrow_data,
                                                 size_t* arrow_size);

/**
 * @brief Get result schema as JSON
 *
 * @param result_handle Query result handle
 * @param schema_json Output parameter for JSON schema (caller must free with marble_free_string)
 * @return Status of the operation
 */
MarbleDB_Status marble_db_query_result_get_schema(MarbleDB_QueryResult_Handle* result_handle,
                                                 char** schema_json);

/**
 * @brief Get approximate number of rows in result
 *
 * @param result_handle Query result handle
 * @param row_count Output parameter for row count
 * @return Status of the operation
 */
MarbleDB_Status marble_db_query_result_get_row_count(MarbleDB_QueryResult_Handle* result_handle,
                                                    int64_t* row_count);

/**
 * @brief Close and free query result
 *
 * @param result_handle Query result handle (will be invalidated)
 */
void marble_db_close_query_result(MarbleDB_QueryResult_Handle** result_handle);

//==============================================================================
// Transaction API
//==============================================================================

/**
 * @brief Begin a transaction
 *
 * @param db_handle Database handle
 * @param transaction_id Output parameter for transaction ID
 * @return Status of the operation
 */
MarbleDB_Status marble_db_begin_transaction(MarbleDB_Handle* db_handle,
                                           uint64_t* transaction_id);

/**
 * @brief Commit a transaction
 *
 * @param db_handle Database handle
 * @param transaction_id Transaction ID to commit
 * @return Status of the operation
 */
MarbleDB_Status marble_db_commit_transaction(MarbleDB_Handle* db_handle,
                                            uint64_t transaction_id);

/**
 * @brief Rollback a transaction
 *
 * @param db_handle Database handle
 * @param transaction_id Transaction ID to rollback
 * @return Status of the operation
 */
MarbleDB_Status marble_db_rollback_transaction(MarbleDB_Handle* db_handle,
                                              uint64_t transaction_id);

//==============================================================================
// Administrative API
//==============================================================================

/**
 * @brief Get database statistics
 *
 * @param db_handle Database handle
 * @param stats_json Output parameter for JSON statistics (caller must free)
 * @return Status of the operation
 */
MarbleDB_Status marble_db_get_stats(MarbleDB_Handle* db_handle,
                                   char** stats_json);

/**
 * @brief Optimize database (compaction, etc.)
 *
 * @param db_handle Database handle
 * @param options_json JSON options for optimization (UTF-8 encoded)
 * @return Status of the operation
 */
MarbleDB_Status marble_db_optimize(MarbleDB_Handle* db_handle,
                                  const char* options_json);

/**
 * @brief Backup database to a path
 *
 * @param db_handle Database handle
 * @param backup_path Path to create backup (UTF-8 encoded)
 * @return Status of the operation
 */
MarbleDB_Status marble_db_backup(MarbleDB_Handle* db_handle,
                                const char* backup_path);

/**
 * @brief Restore database from backup
 *
 * @param db_handle Database handle
 * @param backup_path Path to backup to restore from (UTF-8 encoded)
 * @return Status of the operation
 */
MarbleDB_Status marble_db_restore(MarbleDB_Handle* db_handle,
                                 const char* backup_path);

//==============================================================================
// Configuration API
//==============================================================================

/**
 * @brief Set database configuration option
 *
 * @param db_handle Database handle
 * @param key Configuration key (UTF-8 encoded)
 * @param value Configuration value (UTF-8 encoded)
 * @return Status of the operation
 */
MarbleDB_Status marble_db_set_config(MarbleDB_Handle* db_handle,
                                    const char* key,
                                    const char* value);

/**
 * @brief Get database configuration option
 *
 * @param db_handle Database handle
 * @param key Configuration key (UTF-8 encoded)
 * @param value Output parameter for configuration value (caller must free)
 * @return Status of the operation
 */
MarbleDB_Status marble_db_get_config(MarbleDB_Handle* db_handle,
                                    const char* key,
                                    char** value);

//==============================================================================
// Error Handling
//==============================================================================

/**
 * @brief Get last error message
 *
 * @param db_handle Database handle (can be NULL for global errors)
 * @param error_msg Output parameter for error message (caller must free)
 * @return Status of the operation
 */
MarbleDB_Status marble_db_get_last_error(MarbleDB_Handle* db_handle,
                                        char** error_msg);

/**
 * @brief Clear last error
 *
 * @param db_handle Database handle
 */
void marble_db_clear_last_error(MarbleDB_Handle* db_handle);

//==============================================================================
// Version and Information
//==============================================================================

/**
 * @brief Get MarbleDB version
 *
 * @param version Output parameter for version string (caller must free)
 * @return Status of the operation
 */
MarbleDB_Status marble_db_get_version(char** version);

/**
 * @brief Get MarbleDB build information
 *
 * @param build_info Output parameter for build information JSON (caller must free)
 * @return Status of the operation
 */
MarbleDB_Status marble_db_get_build_info(char** build_info);

//==============================================================================
// Memory Management Helpers
//==============================================================================

/**
 * @brief Free a string allocated by the API
 *
 * @param str String to free (can be NULL)
 */
void marble_free_string(char* str);

/**
 * @brief Free a string array allocated by the API
 *
 * @param array Array to free (can be NULL)
 * @param count Number of elements in the array
 */
void marble_free_string_array(const char** array, size_t count);

/**
 * @brief Free Arrow data allocated by the API
 *
 * @param data Data to free (can be NULL)
 */
void marble_free_arrow_data(void* data);

//==============================================================================
// Utility Functions
//==============================================================================

/**
 * @brief Get status message for a status code
 *
 * @param code Status code
 * @return Human-readable status message (statically allocated, don't free)
 */
const char* marble_status_message(MarbleDB_StatusCode code);

/**
 * @brief Check if status indicates success
 *
 * @param status Status to check
 * @return true if status indicates success, false otherwise
 */
bool marble_status_ok(const MarbleDB_Status* status);

//==============================================================================
// Library Initialization/Cleanup
//==============================================================================

/**
 * @brief Initialize the MarbleDB library
 *
 * Must be called before using any other API functions.
 * Not thread-safe - call once at application startup.
 *
 * @return Status of initialization
 */
MarbleDB_Status marble_init(void);

/**
 * @brief Cleanup the MarbleDB library
 *
 * Should be called at application shutdown.
 * Not thread-safe - call once at application shutdown.
 */
void marble_cleanup(void);

#ifdef __cplusplus
} // extern "C"
#endif
