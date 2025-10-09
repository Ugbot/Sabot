/**
 * MarbleDB C API Implementation
 *
 * Implementation of the extern "C" interface that wraps the C++ API
 * for use from other programming languages.
 */

#include "marble/c_api.h"
#include "marble/api.h"
#include "marble/db.h"
#include "marble/query.h"
#include <cstring>
#include <memory>
#include <vector>
#include <string>

//==============================================================================
// Opaque Handle Structures (PIMPL Pattern)
//==============================================================================

struct MarbleDB_Handle {
    std::unique_ptr<marble::MarbleDB> db;
};

struct MarbleDB_QueryResult_Handle {
    std::unique_ptr<marble::QueryResult> result;
};

//==============================================================================
// Internal Helper Functions
//==============================================================================

namespace {

// Convert C++ Status to C Status
MarbleDB_Status ConvertStatus(const marble::Status& cpp_status) {
    MarbleDB_Status c_status;

    switch (cpp_status.code()) {
        case marble::StatusCode::kOk:
            c_status.code = MARBLEDB_OK;
            break;
        case marble::StatusCode::kInvalidArgument:
            c_status.code = MARBLEDB_INVALID_ARGUMENT;
            break;
        case marble::StatusCode::kNotFound:
            c_status.code = MARBLEDB_NOT_FOUND;
            break;
        case marble::StatusCode::kAlreadyExists:
            c_status.code = MARBLEDB_ALREADY_EXISTS;
            break;
        case marble::StatusCode::kPermissionDenied:
            c_status.code = MARBLEDB_PERMISSION_DENIED;
            break;
        case marble::StatusCode::kUnavailable:
            c_status.code = MARBLEDB_UNAVAILABLE;
            break;
        case marble::StatusCode::kInternalError:
            c_status.code = MARBLEDB_INTERNAL_ERROR;
            break;
        case marble::StatusCode::kNotImplemented:
            c_status.code = MARBLEDB_NOT_IMPLEMENTED;
            break;
        case marble::StatusCode::kIOError:
            c_status.code = MARBLEDB_IO_ERROR;
            break;
        case marble::StatusCode::kCorruption:
            c_status.code = MARBLEDB_CORRUPTION;
            break;
        default:
            c_status.code = MARBLEDB_INTERNAL_ERROR;
            break;
    }

    // Store message in a thread-local or static buffer
    static thread_local std::string last_message;
    last_message = cpp_status.message();
    c_status.message = last_message.c_str();

    return c_status;
}

// Create a C-compatible status
MarbleDB_Status MakeStatus(MarbleDB_StatusCode code, const char* message = nullptr) {
    MarbleDB_Status status;
    status.code = code;

    static thread_local std::string stored_message;
    if (message) {
        stored_message = message;
        status.message = stored_message.c_str();
    } else {
        status.message = marble_status_message(code);
    }

    return status;
}

// Safely copy string for output (caller must free)
char* CopyString(const std::string& str) {
    if (str.empty()) {
        return nullptr;
    }

    char* result = static_cast<char*>(malloc(str.size() + 1));
    if (!result) {
        return nullptr;
    }

    memcpy(result, str.c_str(), str.size() + 1);
    return result;
}

// Safely copy string array for output (caller must free)
const char** CopyStringArray(const std::vector<std::string>& strings, size_t* count) {
    if (strings.empty()) {
        *count = 0;
        return nullptr;
    }

    *count = strings.size();

    // Allocate array of pointers
    const char** result = static_cast<const char**>(malloc(strings.size() * sizeof(const char*)));
    if (!result) {
        return nullptr;
    }

    // Copy each string
    for (size_t i = 0; i < strings.size(); ++i) {
        result[i] = CopyString(strings[i]);
        if (!result[i]) {
            // Free previously allocated strings on failure
            for (size_t j = 0; j < i; ++j) {
                free(const_cast<char*>(result[j]));
            }
            free(result);
            return nullptr;
        }
    }

    return result;
}

} // anonymous namespace

//==============================================================================
// Database Management API Implementation
//==============================================================================

MarbleDB_Status marble_db_open(const char* path, MarbleDB_Handle** db_handle) {
    if (!path || !db_handle) {
        return MakeStatus(MARBLEDB_INVALID_ARGUMENT, "Invalid path or handle pointer");
    }

    try {
        std::unique_ptr<marble::MarbleDB> db;
        auto status = marble::OpenDatabase(path, &db);

        if (!status.ok()) {
            return ConvertStatus(status);
        }

        // Create opaque handle
        *db_handle = new MarbleDB_Handle{std::move(db)};
        return MakeStatus(MARBLEDB_OK);
    } catch (const std::exception& e) {
        return MakeStatus(MARBLEDB_INTERNAL_ERROR, e.what());
    }
}

void marble_db_close(MarbleDB_Handle** db_handle) {
    if (db_handle && *db_handle) {
        delete *db_handle;
        *db_handle = nullptr;
    }
}

//==============================================================================
// Table Management API Implementation
//==============================================================================

MarbleDB_Status marble_db_create_table(MarbleDB_Handle* db_handle,
                                      const char* table_name,
                                      const char* schema_json) {
    if (!db_handle || !table_name || !schema_json) {
        return MakeStatus(MARBLEDB_INVALID_ARGUMENT, "Invalid parameters");
    }

    try {
        auto status = marble::CreateTable(db_handle->db.get(), table_name, schema_json);
        return ConvertStatus(status);
    } catch (const std::exception& e) {
        return MakeStatus(MARBLEDB_INTERNAL_ERROR, e.what());
    }
}

MarbleDB_Status marble_db_drop_table(MarbleDB_Handle* db_handle,
                                    const char* table_name) {
    if (!db_handle || !table_name) {
        return MakeStatus(MARBLEDB_INVALID_ARGUMENT, "Invalid parameters");
    }

    try {
        auto status = marble::DropTable(db_handle->db.get(), table_name);
        return ConvertStatus(status);
    } catch (const std::exception& e) {
        return MakeStatus(MARBLEDB_INTERNAL_ERROR, e.what());
    }
}

MarbleDB_Status marble_db_list_tables(MarbleDB_Handle* db_handle,
                                     const char*** table_names,
                                     size_t* count) {
    if (!db_handle || !table_names || !count) {
        return MakeStatus(MARBLEDB_INVALID_ARGUMENT, "Invalid parameters");
    }

    try {
        std::vector<std::string> tables;
        auto status = marble::ListTables(db_handle->db.get(), &tables);

        if (!status.ok()) {
            return ConvertStatus(status);
        }

        *table_names = CopyStringArray(tables, count);
        if (!*table_names && !tables.empty()) {
            return MakeStatus(MARBLEDB_INTERNAL_ERROR, "Failed to allocate string array");
        }

        return MakeStatus(MARBLEDB_OK);
    } catch (const std::exception& e) {
        return MakeStatus(MARBLEDB_INTERNAL_ERROR, e.what());
    }
}

MarbleDB_Status marble_db_get_table_schema(MarbleDB_Handle* db_handle,
                                          const char* table_name,
                                          char** schema_json) {
    if (!db_handle || !table_name || !schema_json) {
        return MakeStatus(MARBLEDB_INVALID_ARGUMENT, "Invalid parameters");
    }

    try {
        std::string schema;
        auto status = marble::GetTableSchema(db_handle->db.get(), table_name, &schema);

        if (!status.ok()) {
            return ConvertStatus(status);
        }

        *schema_json = CopyString(schema);
        if (!*schema_json && !schema.empty()) {
            return MakeStatus(MARBLEDB_INTERNAL_ERROR, "Failed to allocate string");
        }

        return MakeStatus(MARBLEDB_OK);
    } catch (const std::exception& e) {
        return MakeStatus(MARBLEDB_INTERNAL_ERROR, e.what());
    }
}

//==============================================================================
// Data Ingestion API Implementation
//==============================================================================

MarbleDB_Status marble_db_insert_record(MarbleDB_Handle* db_handle,
                                       const char* table_name,
                                       const char* record_json) {
    if (!db_handle || !table_name || !record_json) {
        return MakeStatus(MARBLEDB_INVALID_ARGUMENT, "Invalid parameters");
    }

    try {
        auto status = marble::InsertRecord(db_handle->db.get(), table_name, record_json);
        return ConvertStatus(status);
    } catch (const std::exception& e) {
        return MakeStatus(MARBLEDB_INTERNAL_ERROR, e.what());
    }
}

MarbleDB_Status marble_db_insert_records_batch(MarbleDB_Handle* db_handle,
                                              const char* table_name,
                                              const char* records_json) {
    if (!db_handle || !table_name || !records_json) {
        return MakeStatus(MARBLEDB_INVALID_ARGUMENT, "Invalid parameters");
    }

    try {
        auto status = marble::InsertRecordsBatch(db_handle->db.get(), table_name, records_json);
        return ConvertStatus(status);
    } catch (const std::exception& e) {
        return MakeStatus(MARBLEDB_INTERNAL_ERROR, e.what());
    }
}

MarbleDB_Status marble_db_insert_arrow_batch(MarbleDB_Handle* db_handle,
                                            const char* table_name,
                                            const void* arrow_data,
                                            size_t arrow_size) {
    if (!db_handle || !table_name || !arrow_data || arrow_size == 0) {
        return MakeStatus(MARBLEDB_INVALID_ARGUMENT, "Invalid parameters");
    }

    try {
        auto status = marble::InsertArrowBatch(db_handle->db.get(), table_name, arrow_data, arrow_size);
        return ConvertStatus(status);
    } catch (const std::exception& e) {
        return MakeStatus(MARBLEDB_INTERNAL_ERROR, e.what());
    }
}

//==============================================================================
// Query API Implementation
//==============================================================================

MarbleDB_Status marble_db_execute_query(MarbleDB_Handle* db_handle,
                                       const char* query_sql,
                                       MarbleDB_QueryResult_Handle** result_handle) {
    if (!db_handle || !query_sql || !result_handle) {
        return MakeStatus(MARBLEDB_INVALID_ARGUMENT, "Invalid parameters");
    }

    try {
        std::unique_ptr<marble::QueryResult> result;
        auto status = marble::ExecuteQuery(db_handle->db.get(), query_sql, &result);

        if (!status.ok()) {
            return ConvertStatus(status);
        }

        // Create opaque handle
        *result_handle = new MarbleDB_QueryResult_Handle{std::move(result)};
        return MakeStatus(MARBLEDB_OK);
    } catch (const std::exception& e) {
        return MakeStatus(MARBLEDB_INTERNAL_ERROR, e.what());
    }
}

MarbleDB_Status marble_db_execute_query_with_options(MarbleDB_Handle* db_handle,
                                                    const char* query_sql,
                                                    const char* options_json,
                                                    MarbleDB_QueryResult_Handle** result_handle) {
    if (!db_handle || !query_sql || !options_json || !result_handle) {
        return MakeStatus(MARBLEDB_INVALID_ARGUMENT, "Invalid parameters");
    }

    try {
        std::unique_ptr<marble::QueryResult> result;
        auto status = marble::ExecuteQueryWithOptions(db_handle->db.get(), query_sql, options_json, &result);

        if (!status.ok()) {
            return ConvertStatus(status);
        }

        // Create opaque handle
        *result_handle = new MarbleDB_QueryResult_Handle{std::move(result)};
        return MakeStatus(MARBLEDB_OK);
    } catch (const std::exception& e) {
        return MakeStatus(MARBLEDB_INTERNAL_ERROR, e.what());
    }
}

//==============================================================================
// Query Result API Implementation
//==============================================================================

MarbleDB_Status marble_db_query_result_has_next(MarbleDB_QueryResult_Handle* result_handle,
                                               bool* has_next) {
    if (!result_handle || !has_next) {
        return MakeStatus(MARBLEDB_INVALID_ARGUMENT, "Invalid parameters");
    }

    try {
        auto status = marble::QueryResultHasNext(result_handle->result.get(), has_next);
        return ConvertStatus(status);
    } catch (const std::exception& e) {
        return MakeStatus(MARBLEDB_INTERNAL_ERROR, e.what());
    }
}

MarbleDB_Status marble_db_query_result_next_json(MarbleDB_QueryResult_Handle* result_handle,
                                                char** batch_json) {
    if (!result_handle || !batch_json) {
        return MakeStatus(MARBLEDB_INVALID_ARGUMENT, "Invalid parameters");
    }

    try {
        std::string json;
        auto status = marble::QueryResultNextJson(result_handle->result.get(), &json);

        if (!status.ok()) {
            return ConvertStatus(status);
        }

        *batch_json = CopyString(json);
        if (!*batch_json && !json.empty()) {
            return MakeStatus(MARBLEDB_INTERNAL_ERROR, "Failed to allocate string");
        }

        return MakeStatus(MARBLEDB_OK);
    } catch (const std::exception& e) {
        return MakeStatus(MARBLEDB_INTERNAL_ERROR, e.what());
    }
}

MarbleDB_Status marble_db_query_result_next_arrow(MarbleDB_QueryResult_Handle* result_handle,
                                                 void** arrow_data,
                                                 size_t* arrow_size) {
    if (!result_handle || !arrow_data || !arrow_size) {
        return MakeStatus(MARBLEDB_INVALID_ARGUMENT, "Invalid parameters");
    }

    try {
        auto status = marble::QueryResultNextArrow(result_handle->result.get(), arrow_data, arrow_size);
        return ConvertStatus(status);
    } catch (const std::exception& e) {
        return MakeStatus(MARBLEDB_INTERNAL_ERROR, e.what());
    }
}

MarbleDB_Status marble_db_query_result_get_schema(MarbleDB_QueryResult_Handle* result_handle,
                                                 char** schema_json) {
    if (!result_handle || !schema_json) {
        return MakeStatus(MARBLEDB_INVALID_ARGUMENT, "Invalid parameters");
    }

    try {
        std::string schema;
        auto status = marble::QueryResultGetSchema(result_handle->result.get(), &schema);

        if (!status.ok()) {
            return ConvertStatus(status);
        }

        *schema_json = CopyString(schema);
        if (!*schema_json && !schema.empty()) {
            return MakeStatus(MARBLEDB_INTERNAL_ERROR, "Failed to allocate string");
        }

        return MakeStatus(MARBLEDB_OK);
    } catch (const std::exception& e) {
        return MakeStatus(MARBLEDB_INTERNAL_ERROR, e.what());
    }
}

MarbleDB_Status marble_db_query_result_get_row_count(MarbleDB_QueryResult_Handle* result_handle,
                                                    int64_t* row_count) {
    if (!result_handle || !row_count) {
        return MakeStatus(MARBLEDB_INVALID_ARGUMENT, "Invalid parameters");
    }

    try {
        auto status = marble::QueryResultGetRowCount(result_handle->result.get(), row_count);
        return ConvertStatus(status);
    } catch (const std::exception& e) {
        return MakeStatus(MARBLEDB_INTERNAL_ERROR, e.what());
    }
}

void marble_db_close_query_result(MarbleDB_QueryResult_Handle** result_handle) {
    if (result_handle && *result_handle) {
        delete *result_handle;
        *result_handle = nullptr;
    }
}

//==============================================================================
// Transaction API Implementation
//==============================================================================

MarbleDB_Status marble_db_begin_transaction(MarbleDB_Handle* db_handle,
                                           uint64_t* transaction_id) {
    if (!db_handle || !transaction_id) {
        return MakeStatus(MARBLEDB_INVALID_ARGUMENT, "Invalid parameters");
    }

    try {
        auto status = marble::BeginTransaction(db_handle->db.get(), transaction_id);
        return ConvertStatus(status);
    } catch (const std::exception& e) {
        return MakeStatus(MARBLEDB_INTERNAL_ERROR, e.what());
    }
}

MarbleDB_Status marble_db_commit_transaction(MarbleDB_Handle* db_handle,
                                            uint64_t transaction_id) {
    if (!db_handle) {
        return MakeStatus(MARBLEDB_INVALID_ARGUMENT, "Invalid database handle");
    }

    try {
        auto status = marble::CommitTransaction(db_handle->db.get(), transaction_id);
        return ConvertStatus(status);
    } catch (const std::exception& e) {
        return MakeStatus(MARBLEDB_INTERNAL_ERROR, e.what());
    }
}

MarbleDB_Status marble_db_rollback_transaction(MarbleDB_Handle* db_handle,
                                              uint64_t transaction_id) {
    if (!db_handle) {
        return MakeStatus(MARBLEDB_INVALID_ARGUMENT, "Invalid database handle");
    }

    try {
        auto status = marble::RollbackTransaction(db_handle->db.get(), transaction_id);
        return ConvertStatus(status);
    } catch (const std::exception& e) {
        return MakeStatus(MARBLEDB_INTERNAL_ERROR, e.what());
    }
}

//==============================================================================
// Administrative API Implementation
//==============================================================================

MarbleDB_Status marble_db_get_stats(MarbleDB_Handle* db_handle,
                                   char** stats_json) {
    if (!db_handle || !stats_json) {
        return MakeStatus(MARBLEDB_INVALID_ARGUMENT, "Invalid parameters");
    }

    try {
        std::string stats;
        auto status = marble::GetDatabaseStats(db_handle->db.get(), &stats);

        if (!status.ok()) {
            return ConvertStatus(status);
        }

        *stats_json = CopyString(stats);
        if (!*stats_json && !stats.empty()) {
            return MakeStatus(MARBLEDB_INTERNAL_ERROR, "Failed to allocate string");
        }

        return MakeStatus(MARBLEDB_OK);
    } catch (const std::exception& e) {
        return MakeStatus(MARBLEDB_INTERNAL_ERROR, e.what());
    }
}

MarbleDB_Status marble_db_optimize(MarbleDB_Handle* db_handle,
                                  const char* options_json) {
    if (!db_handle) {
        return MakeStatus(MARBLEDB_INVALID_ARGUMENT, "Invalid database handle");
    }

    try {
        std::string options = options_json ? options_json : "{}";
        auto status = marble::OptimizeDatabase(db_handle->db.get(), options);
        return ConvertStatus(status);
    } catch (const std::exception& e) {
        return MakeStatus(MARBLEDB_INTERNAL_ERROR, e.what());
    }
}

MarbleDB_Status marble_db_backup(MarbleDB_Handle* db_handle,
                                const char* backup_path) {
    if (!db_handle || !backup_path) {
        return MakeStatus(MARBLEDB_INVALID_ARGUMENT, "Invalid parameters");
    }

    try {
        auto status = marble::BackupDatabase(db_handle->db.get(), backup_path);
        return ConvertStatus(status);
    } catch (const std::exception& e) {
        return MakeStatus(MARBLEDB_INTERNAL_ERROR, e.what());
    }
}

MarbleDB_Status marble_db_restore(MarbleDB_Handle* db_handle,
                                 const char* backup_path) {
    if (!db_handle || !backup_path) {
        return MakeStatus(MARBLEDB_INVALID_ARGUMENT, "Invalid parameters");
    }

    try {
        auto status = marble::RestoreDatabase(db_handle->db.get(), backup_path);
        return ConvertStatus(status);
    } catch (const std::exception& e) {
        return MakeStatus(MARBLEDB_INTERNAL_ERROR, e.what());
    }
}

//==============================================================================
// Configuration API Implementation
//==============================================================================

MarbleDB_Status marble_db_set_config(MarbleDB_Handle* db_handle,
                                    const char* key,
                                    const char* value) {
    if (!db_handle || !key) {
        return MakeStatus(MARBLEDB_INVALID_ARGUMENT, "Invalid parameters");
    }

    try {
        std::string val = value ? value : "";
        auto status = marble::SetConfig(db_handle->db.get(), key, val);
        return ConvertStatus(status);
    } catch (const std::exception& e) {
        return MakeStatus(MARBLEDB_INTERNAL_ERROR, e.what());
    }
}

MarbleDB_Status marble_db_get_config(MarbleDB_Handle* db_handle,
                                    const char* key,
                                    char** value) {
    if (!db_handle || !key || !value) {
        return MakeStatus(MARBLEDB_INVALID_ARGUMENT, "Invalid parameters");
    }

    try {
        std::string val;
        auto status = marble::GetConfig(db_handle->db.get(), key, &val);

        if (!status.ok()) {
            return ConvertStatus(status);
        }

        *value = CopyString(val);
        if (!*value && !val.empty()) {
            return MakeStatus(MARBLEDB_INTERNAL_ERROR, "Failed to allocate string");
        }

        return MakeStatus(MARBLEDB_OK);
    } catch (const std::exception& e) {
        return MakeStatus(MARBLEDB_INTERNAL_ERROR, e.what());
    }
}

//==============================================================================
// Error Handling Implementation
//==============================================================================

MarbleDB_Status marble_db_get_last_error(MarbleDB_Handle* db_handle,
                                        char** error_msg) {
    if (!error_msg) {
        return MakeStatus(MARBLEDB_INVALID_ARGUMENT, "Invalid output parameter");
    }

    try {
        std::string error;
        auto status = marble::GetLastError(db_handle ? db_handle->db.get() : nullptr, &error);

        if (!status.ok()) {
            return ConvertStatus(status);
        }

        *error_msg = CopyString(error);
        if (!*error_msg && !error.empty()) {
            return MakeStatus(MARBLEDB_INTERNAL_ERROR, "Failed to allocate string");
        }

        return MakeStatus(MARBLEDB_OK);
    } catch (const std::exception& e) {
        return MakeStatus(MARBLEDB_INTERNAL_ERROR, e.what());
    }
}

void marble_db_clear_last_error(MarbleDB_Handle* db_handle) {
    try {
        marble::ClearLastError(db_handle ? db_handle->db.get() : nullptr);
    } catch (...) {
        // Ignore errors in cleanup
    }
}

//==============================================================================
// Version and Information Implementation
//==============================================================================

MarbleDB_Status marble_db_get_version(char** version) {
    if (!version) {
        return MakeStatus(MARBLEDB_INVALID_ARGUMENT, "Invalid output parameter");
    }

    try {
        std::string ver;
        auto status = marble::GetVersion(&ver);

        if (!status.ok()) {
            return ConvertStatus(status);
        }

        *version = CopyString(ver);
        if (!*version && !ver.empty()) {
            return MakeStatus(MARBLEDB_INTERNAL_ERROR, "Failed to allocate string");
        }

        return MakeStatus(MARBLEDB_OK);
    } catch (const std::exception& e) {
        return MakeStatus(MARBLEDB_INTERNAL_ERROR, e.what());
    }
}

MarbleDB_Status marble_db_get_build_info(char** build_info) {
    if (!build_info) {
        return MakeStatus(MARBLEDB_INVALID_ARGUMENT, "Invalid output parameter");
    }

    try {
        std::string info;
        auto status = marble::GetBuildInfo(&info);

        if (!status.ok()) {
            return ConvertStatus(status);
        }

        *build_info = CopyString(info);
        if (!*build_info && !info.empty()) {
            return MakeStatus(MARBLEDB_INTERNAL_ERROR, "Failed to allocate string");
        }

        return MakeStatus(MARBLEDB_OK);
    } catch (const std::exception& e) {
        return MakeStatus(MARBLEDB_INTERNAL_ERROR, e.what());
    }
}

//==============================================================================
// Memory Management Helpers Implementation
//==============================================================================

void marble_free_string(char* str) {
    free(str);
}

void marble_free_string_array(const char** array, size_t count) {
    if (array) {
        for (size_t i = 0; i < count; ++i) {
            free(const_cast<char*>(array[i]));
        }
        free(const_cast<char**>(array));
    }
}

void marble_free_arrow_data(void* data) {
    marble::FreeArrowData(data);
}

//==============================================================================
// Utility Functions Implementation
//==============================================================================

const char* marble_status_message(MarbleDB_StatusCode code) {
    switch (code) {
        case MARBLEDB_OK: return "OK";
        case MARBLEDB_INVALID_ARGUMENT: return "Invalid argument";
        case MARBLEDB_NOT_FOUND: return "Not found";
        case MARBLEDB_ALREADY_EXISTS: return "Already exists";
        case MARBLEDB_PERMISSION_DENIED: return "Permission denied";
        case MARBLEDB_UNAVAILABLE: return "Unavailable";
        case MARBLEDB_INTERNAL_ERROR: return "Internal error";
        case MARBLEDB_NOT_IMPLEMENTED: return "Not implemented";
        case MARBLEDB_IO_ERROR: return "I/O error";
        case MARBLEDB_CORRUPTION: return "Data corruption";
        case MARBLEDB_TIMEOUT: return "Timeout";
        default: return "Unknown error";
    }
}

bool marble_status_ok(const MarbleDB_Status* status) {
    return status && status->code == MARBLEDB_OK;
}

//==============================================================================
// Library Initialization/Cleanup Implementation
//==============================================================================

MarbleDB_Status marble_init(void) {
    try {
        // Initialize any global state here
        // For now, just return success
        return MakeStatus(MARBLEDB_OK);
    } catch (const std::exception& e) {
        return MakeStatus(MARBLEDB_INTERNAL_ERROR, e.what());
    }
}

void marble_cleanup(void) {
    // Cleanup any global state here
    // For now, do nothing
}
