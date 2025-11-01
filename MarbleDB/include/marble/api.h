/**
 * MarbleDB Public API
 *
 * Clean, stable API surface for MarbleDB that can be easily wrapped
 * in extern "C" for use from other programming languages.
 */

#pragma once

#include <memory>
#include <string>
#include <vector>
#include <cstdint>
#include <arrow/api.h>
#include <arrow/table.h>
#include <marble/status.h>

namespace marble {

//==============================================================================
// Forward Declarations
//==============================================================================

class MarbleDB;
class Query;
class QueryResult {
public:
    virtual ~QueryResult() = default;

    // Get the result as a table
    virtual arrow::Result<std::shared_ptr<arrow::Table>> GetTable() = 0;

    // Check if there are more results
    virtual bool HasNext() const = 0;

    // Get next batch
    virtual Status Next(std::shared_ptr<arrow::RecordBatch>* batch) = 0;

    // Get schema
    virtual std::shared_ptr<arrow::Schema> schema() const = 0;

    // Get total row count
    virtual int64_t num_rows() const = 0;
};
struct QueryOptions {
    bool optimize = true;
    size_t batch_size = 1000;
    std::string query_plan;
};

// Forward declarations for WAL and predicates
class WalManager;
struct ColumnPredicate {
    std::string column_name;
    enum class PredicateType {
        kEqual,
        kGreaterThan,
        kLessThan,
        kGreaterThanOrEqual,
        kLessThanOrEqual,
        kNotEqual,
        kLike,
        kIn
    } predicate_type;
    std::shared_ptr<arrow::Scalar> value;
};

class TableQueryResult : public QueryResult {
public:
    explicit TableQueryResult(std::shared_ptr<arrow::Table> table)
        : table_(std::move(table)), current_batch_(0) {}

    static Status Create(std::shared_ptr<arrow::Table> table,
                        std::unique_ptr<QueryResult>* result) {
        *result = std::make_unique<TableQueryResult>(std::move(table));
        return Status::OK();
    }

    arrow::Result<std::shared_ptr<arrow::Table>> GetTable() override {
        return table_;
    }

    bool HasNext() const override {
        return current_batch_ < static_cast<size_t>(table_->num_columns() > 0 ? 1 : 0);
    }

    Status Next(std::shared_ptr<arrow::RecordBatch>* batch) override {
        if (!HasNext()) {
            return Status::OK(); // End of results
        }

        // Convert table to record batch using TableBatchReader
        if (!batch_reader_) {
            batch_reader_ = std::make_unique<arrow::TableBatchReader>(*table_);
        }

        return batch_reader_->ReadNext(batch);

        return Status::OK(); // End of results
    }

    std::shared_ptr<arrow::Schema> schema() const override {
        return table_->schema();
    }

    int64_t num_rows() const override {
        return table_->num_rows();
    }

private:
    std::shared_ptr<arrow::Table> table_;
    mutable size_t current_batch_;
    std::unique_ptr<arrow::TableBatchReader> batch_reader_;
};

//==============================================================================
// Database Management API
//==============================================================================

/**
 * @brief Open or create a MarbleDB database
 *
 * @param path Path to the database directory
 * @param db Output parameter for the database handle
 * @return Status indicating success or failure
 */
Status OpenDatabase(const std::string& path, std::unique_ptr<MarbleDB>* db);

/**
 * @brief Close a MarbleDB database
 *
 * @param db Database handle to close (will be set to nullptr)
 */
void CloseDatabase(std::unique_ptr<MarbleDB>* db);

//==============================================================================
// Table Management API
//==============================================================================

/**
 * @brief Create a table in the database
 *
 * @param db Database handle
 * @param table_name Name of the table to create
 * @param schema_json JSON schema definition for the table
 * @return Status indicating success or failure
 */
Status CreateTable(MarbleDB* db, const std::string& table_name, const std::string& schema_json);

/**
 * @brief Drop a table from the database
 *
 * @param db Database handle
 * @param table_name Name of the table to drop
 * @return Status indicating success or failure
 */
Status DropTable(MarbleDB* db, const std::string& table_name);

/**
 * @brief List all tables in the database
 *
 * @param db Database handle
 * @param table_names Output vector for table names
 * @return Status indicating success or failure
 */
Status ListTables(MarbleDB* db, std::vector<std::string>* table_names);

/**
 * @brief Get table schema as JSON
 *
 * @param db Database handle
 * @param table_name Name of the table
 * @param schema_json Output parameter for JSON schema
 * @return Status indicating success or failure
 */
Status GetTableSchema(MarbleDB* db, const std::string& table_name, std::string* schema_json);

//==============================================================================
// Data Ingestion API
//==============================================================================

/**
 * @brief Insert a single record into a table
 *
 * @param db Database handle
 * @param table_name Target table name
 * @param record_json JSON representation of the record
 * @return Status indicating success or failure
 */
Status InsertRecord(MarbleDB* db, const std::string& table_name, const std::string& record_json);

/**
 * @brief Insert multiple records into a table (batch operation)
 *
 * @param db Database handle
 * @param table_name Target table name
 * @param records_json JSON array of records
 * @return Status indicating success or failure
 */
Status InsertRecordsBatch(MarbleDB* db, const std::string& table_name, const std::string& records_json);

/**
 * @brief Insert records from Arrow RecordBatch
 *
 * @param db Database handle
 * @param table_name Target table name
 * @param record_batch_data Serialized Arrow RecordBatch data
 * @param record_batch_size Size of the serialized data
 * @return Status indicating success or failure
 */
Status InsertArrowBatch(MarbleDB* db, const std::string& table_name,
                       const void* record_batch_data, size_t record_batch_size);

//==============================================================================
// Query API
//==============================================================================

/**
 * @brief Execute a SQL-like query
 *
 * @param db Database handle
 * @param query_sql SQL query string
 * @param result Output parameter for query results
 * @return Status indicating success or failure
 */
Status ExecuteQuery(MarbleDB* db, const std::string& query_sql, std::unique_ptr<QueryResult>* result);

/**
 * @brief Execute a query with options
 *
 * @param db Database handle
 * @param query_sql SQL query string
 * @param options_json JSON options for query execution
 * @param result Output parameter for query results
 * @return Status indicating success or failure
 */
Status ExecuteQueryWithOptions(MarbleDB* db, const std::string& query_sql,
                              const std::string& options_json, std::unique_ptr<QueryResult>* result);

//==============================================================================
// Query Result API
//==============================================================================

/**
 * @brief Check if query result has more data
 *
 * @param result Query result handle
 * @param has_next Output boolean indicating if more data is available
 * @return Status indicating success or failure
 */
Status QueryResultHasNext(QueryResult* result, bool* has_next);

/**
 * @brief Get next batch of results as JSON
 *
 * @param result Query result handle
 * @param batch_json Output parameter for JSON representation of result batch
 * @return Status indicating success or failure
 */
Status QueryResultNextJson(QueryResult* result, std::string* batch_json);

/**
 * @brief Get next batch of results as Arrow data
 *
 * @param result Query result handle
 * @param arrow_data Output buffer for serialized Arrow data
 * @param arrow_size Output size of the serialized data
 * @return Status indicating success or failure
 */
Status QueryResultNextArrow(QueryResult* result, void** arrow_data, size_t* arrow_size);

/**
 * @brief Free memory allocated for Arrow data
 *
 * @param arrow_data Pointer to the data to free
 */
void FreeArrowData(void* arrow_data);

/**
 * @brief Get result schema as JSON
 *
 * @param result Query result handle
 * @param schema_json Output parameter for JSON schema
 * @return Status indicating success or failure
 */
Status QueryResultGetSchema(QueryResult* result, std::string* schema_json);

/**
 * @brief Get approximate number of rows in result
 *
 * @param result Query result handle
 * @param row_count Output parameter for row count
 * @return Status indicating success or failure
 */
Status QueryResultGetRowCount(QueryResult* result, int64_t* row_count);

/**
 * @brief Close and free query result
 *
 * @param result Query result handle (will be set to nullptr)
 */
void CloseQueryResult(std::unique_ptr<QueryResult>* result);

//==============================================================================
// Transaction API
//==============================================================================

/**
 * @brief Begin a transaction
 *
 * @param db Database handle
 * @param transaction_id Output parameter for transaction ID
 * @return Status indicating success or failure
 */
Status BeginTransaction(MarbleDB* db, uint64_t* transaction_id);

/**
 * @brief Commit a transaction
 *
 * @param db Database handle
 * @param transaction_id Transaction ID to commit
 * @return Status indicating success or failure
 */
Status CommitTransaction(MarbleDB* db, uint64_t transaction_id);

/**
 * @brief Rollback a transaction
 *
 * @param db Database handle
 * @param transaction_id Transaction ID to rollback
 * @return Status indicating success or failure
 */
Status RollbackTransaction(MarbleDB* db, uint64_t transaction_id);

//==============================================================================
// Administrative API
//==============================================================================

/**
 * @brief Get database statistics
 *
 * @param db Database handle
 * @param stats_json Output parameter for JSON statistics
 * @return Status indicating success or failure
 */
Status GetDatabaseStats(MarbleDB* db, std::string* stats_json);

/**
 * @brief Optimize database (compaction, etc.)
 *
 * @param db Database handle
 * @param options_json JSON options for optimization
 * @return Status indicating success or failure
 */
Status OptimizeDatabase(MarbleDB* db, const std::string& options_json);

/**
 * @brief Backup database to a path
 *
 * @param db Database handle
 * @param backup_path Path to create backup
 * @return Status indicating success or failure
 */
Status BackupDatabase(MarbleDB* db, const std::string& backup_path);

/**
 * @brief Restore database from backup
 *
 * @param db Database handle
 * @param backup_path Path to backup to restore from
 * @return Status indicating success or failure
 */
Status RestoreDatabase(MarbleDB* db, const std::string& backup_path);

//==============================================================================
// Configuration API
//==============================================================================

/**
 * @brief Set database configuration option
 *
 * @param db Database handle
 * @param key Configuration key
 * @param value Configuration value
 * @return Status indicating success or failure
 */
Status SetConfig(MarbleDB* db, const std::string& key, const std::string& value);

/**
 * @brief Get database configuration option
 *
 * @param db Database handle
 * @param key Configuration key
 * @param value Output parameter for configuration value
 * @return Status indicating success or failure
 */
Status GetConfig(MarbleDB* db, const std::string& key, std::string* value);

//==============================================================================
// Error Handling
//==============================================================================

/**
 * @brief Get last error message
 *
 * @param db Database handle (can be nullptr for global errors)
 * @param error_msg Output parameter for error message
 * @return Status indicating success or failure
 */
Status GetLastError(MarbleDB* db, std::string* error_msg);

/**
 * @brief Clear last error
 *
 * @param db Database handle
 */
void ClearLastError(MarbleDB* db);

//==============================================================================
// Version and Information
//==============================================================================

/**
 * @brief Get MarbleDB version
 *
 * @param version Output parameter for version string
 * @return Status indicating success or failure
 */
Status GetVersion(std::string* version);

/**
 * @brief Get MarbleDB build information
 *
 * @param build_info Output parameter for build information JSON
 * @return Status indicating success or failure
 */
Status GetBuildInfo(std::string* build_info);

// Arrow serialization utilities
Status SerializeArrowBatch(const std::shared_ptr<arrow::RecordBatch>& batch,
                          std::string* serialized_data);
Status DeserializeArrowBatch(const void* data, size_t size,
                           std::shared_ptr<arrow::RecordBatch>* batch);
Status DeserializeArrowBatch(const std::string& data,
                           std::shared_ptr<arrow::RecordBatch>* batch);

} // namespace marble
