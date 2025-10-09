/************************************************************************
Copyright 2024 MarbleDB Contributors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
**************************************************************************/

#pragma once

#include <memory>
#include <string>
#include <vector>
#include <unordered_map>
#include <arrow/api.h>
#include <marble/status.h>
#include <marble/query.h>

namespace marble {

class Table;
class Partition;

/**
 * @brief Time partition configuration
 */
enum class TimePartition {
    kHourly,    // Partition by hour
    kDaily,     // Partition by day
    kMonthly,   // Partition by month
    kNone       // No time partitioning
};

/**
 * @brief Table schema definition
 */
struct TableSchema {
    std::string table_name;
    std::shared_ptr<arrow::Schema> arrow_schema;
    TimePartition time_partition = TimePartition::kDaily;
    std::string time_column = "timestamp";  // Name of timestamp column for partitioning

    TableSchema() = default;
    TableSchema(const std::string& name, std::shared_ptr<arrow::Schema> schema)
        : table_name(name), arrow_schema(schema) {}
};

/**
 * @brief Scan specification for queries
 */
struct ScanSpec {
    std::vector<std::string> columns;           // Columns to project (empty = all)
    std::unordered_map<std::string, std::string> filters;  // Column -> filter expression
    std::string time_filter;                   // Time range filter (optional)
    int64_t limit = -1;                        // Max rows to return (-1 = unlimited)

    // Future: Add sorting, aggregation, etc.
};

/**
 * @brief Query result containing Arrow data
 */

/**
 * @brief Partition represents a time-based data partition
 */
class Partition {
public:
    virtual ~Partition() = default;

    /**
     * @brief Get partition identifier (e.g., "2024-01-01" for daily partitions)
     */
    virtual std::string GetPartitionId() const = 0;

    /**
     * @brief Append an Arrow RecordBatch to this partition
     */
    virtual Status Append(const arrow::RecordBatch& batch) = 0;

    /**
     * @brief Scan this partition with the given specification
     */
    virtual Status Scan(const ScanSpec& spec, std::unique_ptr<QueryResult>* result) = 0;

    /**
     * @brief Get approximate row count
     */
    virtual int64_t GetRowCount() const = 0;

    /**
     * @brief Get data size in bytes
     */
    virtual int64_t GetSizeBytes() const = 0;

    /**
     * @brief Get time range covered by this partition
     */
    virtual Status GetTimeRange(int64_t* min_time, int64_t* max_time) const = 0;
};

/**
 * @brief Table represents a logical table with time-based partitions
 */
class Table {
public:
    virtual ~Table() = default;

    /**
     * @brief Get table name
     */
    virtual std::string GetName() const = 0;

    /**
     * @brief Get table schema
     */
    virtual const TableSchema& GetSchema() const = 0;

    /**
     * @brief Append an Arrow RecordBatch to the appropriate partition
     */
    virtual Status Append(const arrow::RecordBatch& batch) = 0;

    /**
     * @brief Scan the table with the given specification
     */
    virtual Status Scan(const ScanSpec& spec, std::unique_ptr<QueryResult>* result) = 0;

    /**
     * @brief Get list of partition IDs
     */
    virtual Status ListPartitions(std::vector<std::string>* partitions) const = 0;

    /**
     * @brief Get partition by ID
     */
    virtual Status GetPartition(const std::string& partition_id,
                               std::shared_ptr<Partition>* partition) const = 0;

    /**
     * @brief Get table statistics
     */
    virtual Status GetStats(std::unordered_map<std::string, std::string>* stats) const = 0;
};

/**
 * @brief Database interface for managing tables
 */
class Database {
public:
    virtual ~Database() = default;

    /**
     * @brief Create a new table
     */
    virtual Status CreateTable(const TableSchema& schema) = 0;

    /**
     * @brief Drop a table
     */
    virtual Status DropTable(const std::string& table_name) = 0;

    /**
     * @brief Get table by name
     */
    virtual Status GetTable(const std::string& table_name,
                           std::shared_ptr<Table>* table) = 0;

    /**
     * @brief List all tables
     */
    virtual Status ListTables(std::vector<std::string>* tables) const = 0;

    /**
     * @brief Get database statistics
     */
    virtual Status GetStats(std::unordered_map<std::string, std::string>* stats) const = 0;
};

// Factory functions
std::shared_ptr<Database> CreateDatabase(const std::string& base_path);
std::shared_ptr<Table> CreateTable(const std::string& base_path,
                                  const TableSchema& schema);

} // namespace marble
