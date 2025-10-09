#pragma once

#include <memory>
#include <string>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <marble/status.h>
#include <marble/file_system.h>
#include <marble/query.h>
#include <marble/sstable.h>

namespace marble {

// Forward declarations
class ArrowRecordBatchIterator;
struct ColumnStatistics;

/**
 * @brief Arrow-based SSTable interface with pushdown capabilities
 *
 * This extends the basic SSTable interface to support:
 * - Column projection pushdown (reading only selected columns)
 * - Predicate pushdown (filtering at storage level)
 * - Arrow RecordBatch-based operations
 * - Statistics for query optimization
 */
class ArrowSSTable {
public:
    virtual ~ArrowSSTable() = default;

    /**
     * @brief Get the SSTable metadata
     */
    virtual const SSTableMetadata& GetMetadata() const = 0;

    /**
     * @brief Get the Arrow schema of this SSTable
     */
    virtual std::shared_ptr<arrow::Schema> GetArrowSchema() const = 0;

    /**
     * @brief Scan with projection and predicate pushdown
     *
     * @param projection_columns Columns to read (empty = all columns)
     * @param predicates Predicates to evaluate at storage level
     * @param batch_size Size of Arrow RecordBatches to return
     * @return Iterator over matching RecordBatches
     */
    virtual Status ScanWithPushdown(
        const std::vector<std::string>& projection_columns,
        const std::vector<ColumnPredicate>& predicates,
        int64_t batch_size,
        std::unique_ptr<ArrowRecordBatchIterator>* iterator) = 0;

    /**
     * @brief Count rows matching predicates (without full scan)
     *
     * Uses metadata and statistics to estimate/calculate row count
     * @param predicates Predicates to apply
     * @param count Output parameter for row count
     * @return Status
     */
    virtual Status CountRows(
        const std::vector<ColumnPredicate>& predicates,
        int64_t* count) = 0;

    /**
     * @brief Get column statistics for query optimization
     *
     * @param column_name Name of column to get stats for
     * @param stats Output parameter for column statistics
     * @return Status
     */
    virtual Status GetColumnStats(
        const std::string& column_name,
        ColumnStatistics* stats) = 0;

    /**
     * @brief Check if this SSTable can satisfy the given predicates
     *
     * Uses bloom filters, zone maps, and other metadata to quickly
     * determine if the SSTable contains matching data.
     *
     * @param predicates Predicates to check
     * @param can_satisfy Output parameter indicating if SSTable can satisfy predicates
     * @return Status
     */
    virtual Status CanSatisfyPredicates(
        const std::vector<ColumnPredicate>& predicates,
        bool* can_satisfy) = 0;

    /**
     * @brief Get file path of this SSTable
     */
    virtual std::string GetFilePath() const = 0;

    /**
     * @brief Get file size in bytes
     */
    virtual uint64_t GetFileSize() const = 0;

    /**
     * @brief Get memory usage of this SSTable instance
     */
    virtual size_t GetMemoryUsage() const = 0;

    /**
     * @brief Validate SSTable integrity
     */
    virtual Status Validate() const = 0;
};

/**
 * @brief Column statistics for query optimization
 */
struct ColumnStatistics {
    std::string column_name;
    bool has_nulls = false;
    int64_t null_count = 0;
    int64_t distinct_count = -1;  // -1 if unknown

    // Min/max values for different types
    std::shared_ptr<arrow::Scalar> min_value;
    std::shared_ptr<arrow::Scalar> max_value;

    // For numeric columns
    double mean = 0.0;
    double std_dev = 0.0;

    // For string columns
    int64_t min_length = 0;
    int64_t max_length = 0;
    int64_t avg_length = 0;

    ColumnStatistics() : column_name("") {}
    ColumnStatistics(const std::string& name) : column_name(name) {}

    /**
     * @brief Check if a predicate can be satisfied based on column stats
     */
    bool CanSatisfyPredicate(const ColumnPredicate& predicate) const;
};

/**
 * @brief Iterator interface for Arrow RecordBatches from SSTable
 */
class ArrowRecordBatchIterator {
public:
    virtual ~ArrowRecordBatchIterator() = default;

    /**
     * @brief Check if there are more batches
     */
    virtual bool HasNext() const = 0;

    /**
     * @brief Get next RecordBatch
     */
    virtual Status Next(std::shared_ptr<arrow::RecordBatch>* batch) = 0;

    /**
     * @brief Get the schema of returned batches
     */
    virtual std::shared_ptr<arrow::Schema> schema() const = 0;

    /**
     * @brief Get estimated number of remaining rows
     */
    virtual int64_t EstimatedRemainingRows() const = 0;

    /**
     * @brief Skip to a specific row offset (for LIMIT/OFFSET)
     */
    virtual Status SkipRows(int64_t num_rows) = 0;
};

/**
 * @brief Arrow-based SSTable Writer with pushdown support
 */
class ArrowSSTableWriter {
public:
    virtual ~ArrowSSTableWriter() = default;

    /**
     * @brief Add a RecordBatch to the SSTable
     *
     * Records must be added in sorted order by primary key
     */
    virtual Status AddRecordBatch(
        const std::shared_ptr<arrow::RecordBatch>& batch) = 0;

    /**
     * @brief Finalize the SSTable and write to disk
     */
    virtual Status Finish(std::unique_ptr<ArrowSSTable>* sstable) = 0;

    /**
     * @brief Get current number of record batches written
     */
    virtual size_t GetBatchCount() const = 0;

    /**
     * @brief Get current number of records written
     */
    virtual size_t GetRecordCount() const = 0;

    /**
     * @brief Get estimated file size
     */
    virtual size_t GetEstimatedSize() const = 0;
};

/**
 * @brief Arrow-based SSTable Reader
 */
class ArrowSSTableReader {
public:
    virtual ~ArrowSSTableReader() = default;

    /**
     * @brief Open an existing Arrow SSTable file
     */
    virtual Status Open(
        const std::string& filepath,
        std::unique_ptr<ArrowSSTable>* sstable) = 0;

    /**
     * @brief Create SSTable from existing file with known metadata
     */
    virtual Status CreateFromFile(
        const std::string& filepath,
        const SSTableMetadata& metadata,
        std::unique_ptr<ArrowSSTable>* sstable) = 0;
};

/**
 * @brief Arrow SSTable Manager - factory and utility functions
 */
class ArrowSSTableManager {
public:
    virtual ~ArrowSSTableManager() = default;

    /**
     * @brief Create a new Arrow SSTable writer
     */
    virtual Status CreateWriter(
        const std::string& filepath,
        uint64_t level,
        std::shared_ptr<arrow::Schema> schema,
        std::unique_ptr<ArrowSSTableWriter>* writer) = 0;

    /**
     * @brief Open an existing Arrow SSTable
     */
    virtual Status OpenSSTable(
        const std::string& filepath,
        std::unique_ptr<ArrowSSTable>* sstable) = 0;

    /**
     * @brief List all Arrow SSTable files in a directory
     */
    virtual Status ListSSTables(
        const std::string& directory,
        std::vector<std::string>* files) = 0;

    /**
     * @brief Delete an Arrow SSTable file
     */
    virtual Status DeleteSSTable(const std::string& filepath) = 0;

    /**
     * @brief Compact multiple SSTables into one
     */
    virtual Status CompactSSTables(
        const std::vector<std::string>& input_files,
        const std::string& output_file,
        uint64_t target_level) = 0;
};

/**
 * @brief Predicate evaluator for Arrow data
 */
class ArrowPredicateEvaluator {
public:
    explicit ArrowPredicateEvaluator(const std::vector<ColumnPredicate>& predicates);

    /**
     * @brief Evaluate predicates against an Arrow RecordBatch
     *
     * @param batch Input RecordBatch
     * @param mask Output boolean mask (true = row matches predicates)
     * @return Status
     */
    Status EvaluateBatch(
        const std::shared_ptr<arrow::RecordBatch>& batch,
        std::shared_ptr<arrow::BooleanArray>* mask) const;

    /**
     * @brief Filter RecordBatch using predicates
     *
     * @param batch Input RecordBatch
     * @param filtered_batch Output filtered RecordBatch
     * @return Status
     */
    Status FilterBatch(
        const std::shared_ptr<arrow::RecordBatch>& batch,
        std::shared_ptr<arrow::RecordBatch>* filtered_batch) const;

    /**
     * @brief Check if any predicates can be evaluated at this level
     */
    bool HasPredicates() const { return !predicates_.empty(); }

private:
    std::vector<ColumnPredicate> predicates_;

    // Cache compiled expressions for performance
    struct CompiledPredicate {
        std::string column_name;
        arrow::compute::CompareOperator op;
        std::shared_ptr<arrow::Scalar> value;
    };
    std::vector<CompiledPredicate> compiled_predicates_;
};

/**
 * @brief Column projector for Arrow data
 */
class ArrowColumnProjector {
public:
    explicit ArrowColumnProjector(const std::vector<std::string>& columns);

    /**
     * @brief Project columns from RecordBatch
     */
    Status ProjectBatch(
        const std::shared_ptr<arrow::RecordBatch>& batch,
        std::shared_ptr<arrow::RecordBatch>* projected_batch) const;

    /**
     * @brief Get projected schema
     */
    std::shared_ptr<arrow::Schema> GetProjectedSchema(
        const std::shared_ptr<arrow::Schema>& input_schema) const;

    /**
     * @brief Check if projection is needed (not all columns)
     */
    bool NeedsProjection() const;

    /**
     * @brief Get projection indices for the input schema
     */
    Status GetProjectionIndices(
        const std::shared_ptr<arrow::Schema>& schema,
        std::vector<int>* indices) const;

private:
    std::vector<std::string> projected_columns_;
    bool needs_projection_;
};

// Factory functions
std::unique_ptr<ArrowSSTableManager> CreateArrowSSTableManager(
    std::shared_ptr<FileSystem> fs = nullptr);

std::unique_ptr<ArrowPredicateEvaluator> CreateArrowPredicateEvaluator(
    const std::vector<ColumnPredicate>& predicates);

std::unique_ptr<ArrowColumnProjector> CreateArrowColumnProjector(
    const std::vector<std::string>& columns);

// Utility functions
Status BuildColumnStatistics(
    const std::shared_ptr<arrow::ChunkedArray>& column,
    const std::string& column_name,
    ColumnStatistics* stats);

} // namespace marble
