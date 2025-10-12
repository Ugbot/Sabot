#pragma once

#include <memory>
#include <string>
#include <vector>
#include <functional>
#include <arrow/api.h>
#include <arrow/result.h>
#include "sabot_sql/operators/operator.h"

namespace sabot_sql {

/**
 * @brief Table scan operator - reads from Arrow tables, CSV, Parquet, or other sources
 * 
 * This is the SQL equivalent of TripleScanOperator for SPARQL.
 * It's a leaf operator that produces data from external sources.
 * 
 * Features:
 * - Predicate pushdown (filter early)
 * - Projection pushdown (read only needed columns)
 * - Support for multiple formats (Arrow, CSV, Parquet)
 * - Batch-based streaming (memory efficient)
 */
class TableScanOperator : public Operator {
public:
    /**
     * @brief Scan from an in-memory Arrow table
     */
    TableScanOperator(std::shared_ptr<arrow::Table> table,
                      size_t batch_size = 10000);
    
    /**
     * @brief Scan from a file (format auto-detected from extension)
     */
    static arrow::Result<std::shared_ptr<TableScanOperator>> 
        FromFile(const std::string& file_path,
                size_t batch_size = 10000);
    
    /**
     * @brief Scan from Parquet file
     */
    static arrow::Result<std::shared_ptr<TableScanOperator>> 
        FromParquet(const std::string& file_path,
                   size_t batch_size = 10000);
    
    /**
     * @brief Scan from CSV file
     */
    static arrow::Result<std::shared_ptr<TableScanOperator>> 
        FromCSV(const std::string& file_path,
               size_t batch_size = 10000);
    
    ~TableScanOperator() override = default;
    
    // Operator interface
    arrow::Result<std::shared_ptr<arrow::Schema>> GetOutputSchema() const override;
    arrow::Result<std::shared_ptr<arrow::RecordBatch>> GetNextBatch() override;
    bool HasNextBatch() const override;
    std::string ToString() const override;
    size_t EstimateCardinality() const override;
    
    /**
     * @brief Add predicate pushdown filter
     * Filters are applied during scanning for better performance
     */
    void AddPredicatePushdown(std::function<bool(const arrow::RecordBatch&)> predicate);
    
    /**
     * @brief Add projection pushdown (select specific columns)
     * Only these columns will be read and returned
     */
    void AddProjectionPushdown(const std::vector<std::string>& column_names);
    
    /**
     * @brief Set batch size for scanning
     */
    void SetBatchSize(size_t batch_size) { batch_size_ = batch_size; }
    
private:
    std::shared_ptr<arrow::Table> table_;
    size_t batch_size_;
    size_t current_batch_;
    bool exhausted_;
    
    // Pushdown filters/projections
    std::vector<std::function<bool(const arrow::RecordBatch&)>> predicates_;
    std::vector<std::string> projected_columns_;
    bool has_projection_pushdown_;
};

} // namespace sabot_sql

