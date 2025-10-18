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
#include <marble/table.h>

namespace marble {

/**
 * @brief Temporal model types
 */
enum class TemporalModel {
    kUnitemporal,      // No temporal versioning
    kSystemTime,       // Only system time versioning (MVCC)
    kValidTime,        // Only valid time versioning
    kBitemporal        // Both system time and valid time
};

/**
 * @brief Temporal snapshot identifier
 */
struct SnapshotId {
    uint64_t timestamp;  // System time when snapshot was created
    uint64_t version;    // Incremental version number

    SnapshotId() : timestamp(0), version(0) {}
    SnapshotId(uint64_t ts, uint64_t ver) : timestamp(ts), version(ver) {}

    std::string ToString() const;
    static SnapshotId FromString(const std::string& str);
    static SnapshotId Latest();  // Special value for latest snapshot

    bool operator==(const SnapshotId& other) const {
        return timestamp == other.timestamp && version == other.version;
    }

    bool operator!=(const SnapshotId& other) const {
        return !(*this == other);
    }
};

/**
 * @brief Temporal query specification
 */
struct TemporalQuerySpec {
    // System time constraints (MVCC)
    SnapshotId as_of_snapshot;      // AS OF <snapshot>
    uint64_t system_time_start;     // SYSTEM_TIME BETWEEN start AND end
    uint64_t system_time_end;

    // Valid time constraints (temporal validity)
    uint64_t valid_time_start;      // VALID_TIME BETWEEN start AND end
    uint64_t valid_time_end;

    // Query behavior
    bool include_deleted = false;   // Include soft-deleted records
    bool temporal_reconstruction = true;  // Enable temporal reconstruction

    TemporalQuerySpec() : system_time_start(0), system_time_end(UINT64_MAX),
                         valid_time_start(0), valid_time_end(UINT64_MAX) {}
};

/**
 * @brief Temporal metadata for a record
 */
struct TemporalMetadata {
    SnapshotId created_snapshot;    // When record was created
    SnapshotId deleted_snapshot;    // When record was deleted (if soft delete)
    uint64_t valid_from;           // Valid time start
    uint64_t valid_to;             // Valid time end
    uint64_t system_time = 0;      // System time (transaction timestamp)

    bool is_deleted = false;       // Soft delete flag

    TemporalMetadata() : valid_from(0), valid_to(UINT64_MAX), system_time(0) {}
};

/**
 * @brief Temporal table with bitemporal capabilities
 */
class TemporalTable {
public:
    virtual ~TemporalTable() = default;

    /**
     * @brief Get temporal model supported by this table
     */
    virtual TemporalModel GetTemporalModel() const = 0;

    /**
     * @brief Create a new snapshot
     */
    virtual Status CreateSnapshot(SnapshotId* snapshot_id) = 0;

    /**
     * @brief List available snapshots
     */
    virtual Status ListSnapshots(std::vector<SnapshotId>* snapshots) const = 0;

    /**
     * @brief Delete a snapshot
     */
    virtual Status DeleteSnapshot(const SnapshotId& snapshot_id) = 0;

    /**
     * @brief Temporal query with time travel
     */
    virtual Status TemporalScan(const TemporalQuerySpec& temporal_spec,
                               const ScanSpec& scan_spec,
                               std::unique_ptr<QueryResult>* result) = 0;

    /**
     * @brief Insert/update with temporal versioning
     */
    virtual Status TemporalInsert(const arrow::RecordBatch& batch,
                                 const TemporalMetadata& metadata = TemporalMetadata()) = 0;

    /**
     * @brief Soft delete records
     */
    virtual Status TemporalDelete(const std::string& predicate,
                                 const TemporalMetadata& metadata = TemporalMetadata()) = 0;

    /**
     * @brief Update records with temporal versioning
     */
    virtual Status TemporalUpdate(const std::string& predicate,
                                 const std::unordered_map<std::string, std::string>& updates,
                                 const TemporalMetadata& metadata = TemporalMetadata()) = 0;

    /**
     * @brief Get temporal statistics
     */
    virtual Status GetTemporalStats(std::unordered_map<std::string, std::string>* stats) const = 0;
};

/**
 * @brief Temporal database with snapshot management
 */
class TemporalDatabase {
public:
    virtual ~TemporalDatabase() = default;

    /**
     * @brief Create a temporal table
     */
    virtual Status CreateTemporalTable(const std::string& table_name,
                                      const TableSchema& schema,
                                      TemporalModel model = TemporalModel::kBitemporal) = 0;

    /**
     * @brief Get temporal table
     */
    virtual Status GetTemporalTable(const std::string& table_name,
                                   std::shared_ptr<TemporalTable>* table) = 0;

    /**
     * @brief Create database-wide snapshot
     */
    virtual Status CreateDatabaseSnapshot(SnapshotId* snapshot_id) = 0;

    /**
     * @brief Restore database to snapshot
     */
    virtual Status RestoreDatabaseSnapshot(const SnapshotId& snapshot_id) = 0;
};

/**
 * @brief Temporal query builder for fluent API
 */
class TemporalQueryBuilder {
public:
    TemporalQueryBuilder();

    // System time constraints
    TemporalQueryBuilder& AsOf(const SnapshotId& snapshot);
    TemporalQueryBuilder& SystemTimeBetween(uint64_t start, uint64_t end);
    TemporalQueryBuilder& SystemTimeFrom(uint64_t start);
    TemporalQueryBuilder& SystemTimeTo(uint64_t end);

    // Valid time constraints
    TemporalQueryBuilder& ValidTimeBetween(uint64_t start, uint64_t end);
    TemporalQueryBuilder& ValidTimeFrom(uint64_t start);
    TemporalQueryBuilder& ValidTimeTo(uint64_t end);

    // Query options
    TemporalQueryBuilder& IncludeDeleted(bool include = true);

    // Build the spec
    TemporalQuerySpec Build() const;

private:
    TemporalQuerySpec spec_;
};

/**
 * @brief Version information for temporal history queries
 */
struct VersionInfo {
    SnapshotId snapshot_id;              // When this version was created
    uint64_t valid_from;                 // Business validity start
    uint64_t valid_to;                   // Business validity end
    bool is_deleted;                     // Whether this version represents deletion
    std::shared_ptr<arrow::RecordBatch> data;  // The actual data for this version
};

/**
 * @brief Time travel utilities
 */
class TimeTravel {
public:
    /**
     * @brief Get current timestamp in microseconds
     */
    static uint64_t Now();

    /**
     * @brief Parse timestamp from string (ISO 8601 format)
     */
    static uint64_t ParseTimestamp(const std::string& timestamp_str);

    /**
     * @brief Format timestamp to string
     */
    static std::string FormatTimestamp(uint64_t timestamp);

    /**
     * @brief Generate unique snapshot ID
     */
    static SnapshotId GenerateSnapshotId();
};

// Factory functions
std::shared_ptr<TemporalDatabase> CreateTemporalDatabase(const std::string& base_path);
std::shared_ptr<TemporalTable> CreateTemporalTable(const std::string& base_path,
                                                  const TableSchema& schema,
                                                  TemporalModel model = TemporalModel::kBitemporal);

// Forward declaration for TableCapabilities integration
struct TableCapabilities;

// Helper function to configure temporal features from TableCapabilities
Status ConfigureTemporalFromCapabilities(
    const std::string& table_name,
    const TableCapabilities& capabilities);

} // namespace marble
