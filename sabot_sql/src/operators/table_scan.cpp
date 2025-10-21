#include "sabot_sql/operators/table_scan.h"
#include <arrow/io/file.h>
#include <arrow/ipc/reader.h>
#include <arrow/csv/reader.h>
#include <parquet/arrow/reader.h>
#include <algorithm>

namespace sabot_sql {

TableScanOperator::TableScanOperator(
    std::shared_ptr<arrow::Table> table,
    size_t batch_size)
    : table_(std::move(table))
    , batch_size_(batch_size)
    , current_batch_(0)
    , exhausted_(false)
    , has_projection_pushdown_(false) {
}

arrow::Result<std::shared_ptr<TableScanOperator>> 
TableScanOperator::FromFile(
    const std::string& file_path,
    size_t batch_size) {
    
    // Auto-detect format from extension
    if (file_path.ends_with(".parquet")) {
        return FromParquet(file_path, batch_size);
    } else if (file_path.ends_with(".csv")) {
        return FromCSV(file_path, batch_size);
    } else if (file_path.ends_with(".arrow") || file_path.ends_with(".ipc")) {
        // Arrow IPC format
        ARROW_ASSIGN_OR_RAISE(auto input_file, arrow::io::ReadableFile::Open(file_path));
        ARROW_ASSIGN_OR_RAISE(auto reader, arrow::ipc::RecordBatchFileReader::Open(input_file));
        
        // Read all batches into a table
        std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
        for (int i = 0; i < reader->num_record_batches(); i++) {
            ARROW_ASSIGN_OR_RAISE(auto batch, reader->ReadRecordBatch(i));
            batches.push_back(batch);
        }
        
        ARROW_ASSIGN_OR_RAISE(auto table, arrow::Table::FromRecordBatches(batches));
        return std::shared_ptr<TableScanOperator>(
            new TableScanOperator(table, batch_size));
    } else {
        return arrow::Status::NotImplemented(
            "Unsupported file format: " + file_path);
    }
}

arrow::Result<std::shared_ptr<TableScanOperator>> 
TableScanOperator::FromParquet(
    const std::string& file_path,
    size_t batch_size) {
    
    ARROW_ASSIGN_OR_RAISE(auto input_file, arrow::io::ReadableFile::Open(file_path));
    
    std::unique_ptr<parquet::arrow::FileReader> reader;
    ARROW_RETURN_NOT_OK(
        parquet::arrow::OpenFile(input_file, arrow::default_memory_pool()));
    
    std::shared_ptr<arrow::Table> table;
    ARROW_RETURN_NOT_OK(reader->ReadTable(&table));
    
    return std::shared_ptr<TableScanOperator>(
        new TableScanOperator(table, batch_size));
}

arrow::Result<std::shared_ptr<TableScanOperator>> 
TableScanOperator::FromCSV(
    const std::string& file_path,
    size_t batch_size) {
    
    ARROW_ASSIGN_OR_RAISE(auto input_file, arrow::io::ReadableFile::Open(file_path));
    
    auto read_options = arrow::csv::ReadOptions::Defaults();
    auto parse_options = arrow::csv::ParseOptions::Defaults();
    auto convert_options = arrow::csv::ConvertOptions::Defaults();
    
    ARROW_ASSIGN_OR_RAISE(
        auto reader,
        arrow::csv::TableReader::Make(
            arrow::io::default_io_context(),
            input_file,
            read_options,
            parse_options,
            convert_options));
    
    ARROW_ASSIGN_OR_RAISE(auto table, reader->Read());
    
    return std::shared_ptr<TableScanOperator>(
        new TableScanOperator(table, batch_size));
}

arrow::Result<std::shared_ptr<arrow::Schema>> 
TableScanOperator::GetOutputSchema() const {
    if (!table_) {
        return arrow::Status::Invalid("No table");
    }
    
    if (has_projection_pushdown_ && !projected_columns_.empty()) {
        // Return schema with only projected columns
        std::vector<std::shared_ptr<arrow::Field>> fields;
        for (const auto& col_name : projected_columns_) {
            auto field = table_->schema()->GetFieldByName(col_name);
            if (field) {
                fields.push_back(field);
            }
        }
        return arrow::schema(fields);
    }
    
    return table_->schema();
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> 
TableScanOperator::GetNextBatch() {
    if (!table_ || exhausted_) {
        return nullptr;
    }
    
    // Calculate batch range
    int64_t start_row = current_batch_ * batch_size_;
    int64_t num_rows = std::min(
        static_cast<int64_t>(batch_size_),
        table_->num_rows() - start_row);
    
    if (num_rows <= 0) {
        exhausted_ = true;
        return nullptr;
    }
    
    // Slice the table
    auto sliced_table = table_->Slice(start_row, num_rows);
    
    // Apply projection pushdown if specified
    if (has_projection_pushdown_ && !projected_columns_.empty()) {
        std::vector<std::shared_ptr<arrow::Array>> columns;
        std::vector<std::shared_ptr<arrow::Field>> fields;
        
        for (const auto& col_name : projected_columns_) {
            auto chunked_col = sliced_table->GetColumnByName(col_name);
            if (chunked_col) {
                // Combine chunks to single array
                if (chunked_col->num_chunks() == 1) {
                    columns.push_back(chunked_col->chunk(0));
                } else {
                    // Concatenate chunks
                    ARROW_ASSIGN_OR_RAISE(
                        auto combined,
                        arrow::Concatenate(chunked_col->chunks())
                    );
                    columns.push_back(combined);
                }
                
                auto field = sliced_table->schema()->GetFieldByName(col_name);
                fields.push_back(field);
            }
        }
        
        auto schema = arrow::schema(fields);
        auto batch = arrow::RecordBatch::Make(schema, num_rows, columns);
        
        // Apply predicates
        bool passes = true;
        for (const auto& predicate : predicates_) {
            if (!predicate(*batch)) {
                passes = false;
                break;
            }
        }
        
        current_batch_++;
        
        if (passes) {
            stats_.rows_processed += batch->num_rows();
            stats_.batches_processed++;
            return batch;
        } else {
            // Skip this batch, get next one
            return GetNextBatch();
        }
    }
    
    // No projection pushdown - convert table to batch
    ARROW_ASSIGN_OR_RAISE(auto batch, sliced_table->CombineChunksToBatch());
    
    // Apply predicates
    bool passes = true;
    for (const auto& predicate : predicates_) {
        if (!predicate(*batch)) {
            passes = false;
            break;
        }
    }
    
    current_batch_++;
    
    if (passes) {
        stats_.rows_processed += batch->num_rows();
        stats_.batches_processed++;
        return batch;
    } else {
        // Skip this batch, get next one
        return GetNextBatch();
    }
}

bool TableScanOperator::HasNextBatch() const {
    if (!table_ || exhausted_) {
        return false;
    }
    
    int64_t start_row = current_batch_ * batch_size_;
    return start_row < table_->num_rows();
}

std::string TableScanOperator::ToString() const {
    std::string result = "TableScan(";
    if (table_) {
        result += std::to_string(table_->num_rows()) + " rows, ";
        result += std::to_string(table_->num_columns()) + " columns";
    }
    
    if (has_projection_pushdown_) {
        result += ", projected: [";
        for (size_t i = 0; i < projected_columns_.size(); i++) {
            if (i > 0) result += ", ";
            result += projected_columns_[i];
        }
        result += "]";
    }
    
    result += ")";
    return result;
}

size_t TableScanOperator::EstimateCardinality() const {
    if (!table_) {
        return 0;
    }
    
    // If we have predicates, estimate selectivity
    if (!predicates_.empty()) {
        // Conservative estimate: each predicate has 10% selectivity
        double selectivity = 1.0;
        for (size_t i = 0; i < predicates_.size(); i++) {
            selectivity *= 0.1;
        }
        return static_cast<size_t>(table_->num_rows() * selectivity);
    }
    
    return table_->num_rows();
}

void TableScanOperator::AddPredicatePushdown(
    std::function<bool(const arrow::RecordBatch&)> predicate) {
    predicates_.push_back(predicate);
}

void TableScanOperator::AddProjectionPushdown(
    const std::vector<std::string>& column_names) {
    projected_columns_ = column_names;
    has_projection_pushdown_ = true;
}

arrow::Result<std::shared_ptr<arrow::Table>> 
TableScanOperator::GetAllResults() {
    // Reset state
    current_batch_ = 0;
    exhausted_ = false;
    
    // Collect all batches
    std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
    
    while (HasNextBatch()) {
        ARROW_ASSIGN_OR_RAISE(auto batch, GetNextBatch());
        if (batch) {
            batches.push_back(batch);
        }
    }
    
    // Combine batches into a single table
    if (batches.empty()) {
        std::vector<std::shared_ptr<arrow::Array>> empty_arrays;
        return arrow::Table::Make(table_->schema(), empty_arrays);
    }
    
    ARROW_ASSIGN_OR_RAISE(auto table, arrow::Table::FromRecordBatches(batches));
    return table;
}

} // namespace sabot_sql

