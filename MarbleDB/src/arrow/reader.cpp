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

#include "marble/arrow/reader.h"

#include <arrow/api.h>
#include <arrow/io/api.h>

#include "marble/api.h"
#include "marble/arrow/lsm_iterator.h"
#include "marble/table.h"

namespace marble {
namespace arrow_api {

//==============================================================================
// MarbleRecordBatchReader Implementation
//==============================================================================

MarbleRecordBatchReader::MarbleRecordBatchReader(
    std::shared_ptr<MarbleDB> db,
    const std::string& table_name,
    const std::vector<std::string>& projection,
    const std::vector<ColumnPredicate>& predicates)
    : db_(std::move(db))
    , table_name_(table_name)
    , projection_(projection)
    , predicates_(predicates)
    , finished_(false) {

    // Initialize will be called lazily on first ReadNext()
}

MarbleRecordBatchReader::~MarbleRecordBatchReader() {
    // Cleanup handled by smart pointers
}

std::shared_ptr<::arrow::Schema> MarbleRecordBatchReader::schema() const {
    if (!schema_) {
        // Lazy initialization - load schema on first access
        const_cast<MarbleRecordBatchReader*>(this)->LoadSchema();
    }
    return schema_;
}

::arrow::Status MarbleRecordBatchReader::ReadNext(
    std::shared_ptr<::arrow::RecordBatch>* batch) {

    // Lazy initialization on first call
    if (!iterator_ && !finished_) {
        auto status = Initialize();
        if (!status.ok()) {
            finished_ = true;
            *batch = nullptr;
            return status;
        }
    }

    // End of stream
    if (finished_ || !iterator_) {
        *batch = nullptr;
        return ::arrow::Status::OK();
    }

    // Check if iterator has more data
    if (!iterator_->HasNext()) {
        finished_ = true;
        *batch = nullptr;

        // Propagate iterator stats before returning (critical for when all SSTables are skipped)
        if (iterator_) {
            auto lsm_iter = dynamic_cast<LSMBatchIterator*>(iterator_.get());
            if (lsm_iter) {
                auto iter_stats = lsm_iter->GetStats();
                stats_.bloom_filter_skips = iter_stats.sstables_skipped;
                stats_.zone_map_skips = iter_stats.batches_skipped;
                stats_.total_sstables = iter_stats.sstables_opened;
            }
        }

        return ::arrow::Status::OK();
    }

    // Read next batch from iterator
    Status marble_status = iterator_->Next(batch);
    if (!marble_status.ok()) {
        finished_ = true;
        *batch = nullptr;
        return ::arrow::Status::IOError("Failed to read next batch: " + marble_status.ToString());
    }

    // Update statistics
    if (*batch) {
        stats_.batches_read++;
        stats_.rows_read += (*batch)->num_rows();

        // Estimate bytes read (sum of all array buffers)
        for (int i = 0; i < (*batch)->num_columns(); ++i) {
            auto column = (*batch)->column(i);
            for (const auto& buffer : column->data()->buffers) {
                if (buffer) {
                    stats_.bytes_read += buffer->size();
                }
            }
        }
    }

    // Propagate iterator stats (predicate pushdown metrics)
    // This must happen OUTSIDE the if (*batch) block so stats are propagated
    // even when all SSTables are skipped and batch is nullptr
    if (iterator_) {
        auto lsm_iter = dynamic_cast<LSMBatchIterator*>(iterator_.get());
        if (lsm_iter) {
            auto iter_stats = lsm_iter->GetStats();
            stats_.bloom_filter_skips = iter_stats.sstables_skipped;
            stats_.zone_map_skips = iter_stats.batches_skipped;
            stats_.total_sstables = iter_stats.sstables_opened;
            // Don't overwrite batches_returned/rows_read - reader tracks these separately
        }
    }

    return ::arrow::Status::OK();
}

::arrow::Status MarbleRecordBatchReader::Initialize() {
    // Load schema first
    auto status = LoadSchema();
    if (!status.ok()) {
        return status;
    }

    // Create iterator
    status = CreateIterator();
    if (!status.ok()) {
        return status;
    }

    return ::arrow::Status::OK();
}

::arrow::Status MarbleRecordBatchReader::CreateIterator() {
    // Create LSMBatchIterator for multi-level merge with deduplication
    //
    // Architecture:
    // 1. Get all SSTables for the table from LSM tree
    // 2. Create LSMBatchIterator with K-way merge
    // 3. Apply predicate pushdown (bloom filters, zone maps)
    // 4. Stream batches incrementally with constant memory

    // Get SSTables from DB (organized by LSM level)
    // NOTE: We have friend access to protected GetSSTablesInternal()
    std::vector<std::vector<std::shared_ptr<SSTable>>> sstables;
    Status marble_status = db_->GetSSTablesInternal(table_name_, &sstables);

    if (!marble_status.ok()) {
        return ::arrow::Status::IOError(
            "Failed to get SSTables for table '" + table_name_ + "': " +
            marble_status.ToString());
    }

    // Create LSMBatchIterator with K-way merge
    std::unique_ptr<LSMBatchIterator> lsm_iterator;
    marble_status = LSMBatchIterator::Create(
        sstables,
        schema_,
        projection_,
        predicates_,
        10000,  // batch_size (10K rows per batch)
        &lsm_iterator);

    if (!marble_status.ok()) {
        return ::arrow::Status::IOError(
            "Failed to create LSMBatchIterator: " + marble_status.ToString());
    }

    // Success - move iterator into place
    iterator_ = std::move(lsm_iterator);
    return ::arrow::Status::OK();
}

::arrow::Status MarbleRecordBatchReader::LoadSchema() {
    // Get actual table schema from MarbleDB
    std::shared_ptr<::arrow::Schema> table_schema;
    Status marble_status = db_->GetTableSchemaInternal(table_name_, &table_schema);

    if (!marble_status.ok()) {
        return ::arrow::Status::IOError(
            "Failed to get schema for table '" + table_name_ + "': " +
            marble_status.ToString());
    }

    if (!table_schema) {
        return ::arrow::Status::IOError(
            "Table '" + table_name_ + "' has null schema");
    }

    // Apply column projection if specified
    if (projection_.empty()) {
        // No projection - use all columns from table schema
        schema_ = table_schema;
    } else {
        // Apply projection: select only specified columns
        std::vector<std::shared_ptr<::arrow::Field>> projected_fields;
        projected_fields.reserve(projection_.size());

        for (const auto& col_name : projection_) {
            // Find field in table schema
            auto field = table_schema->GetFieldByName(col_name);
            if (!field) {
                return ::arrow::Status::Invalid(
                    "Column '" + col_name + "' not found in table '" +
                    table_name_ + "' schema");
            }
            projected_fields.push_back(field);
        }

        // Create projected schema
        schema_ = ::arrow::schema(projected_fields);
    }

    return ::arrow::Status::OK();
}

//==============================================================================
// Factory Functions
//==============================================================================

::arrow::Result<std::shared_ptr<::arrow::RecordBatchReader>> OpenTable(
    std::shared_ptr<MarbleDB> db,
    const std::string& table_name,
    const std::vector<std::string>& projection,
    const std::vector<ColumnPredicate>& predicates) {

    if (!db) {
        return ::arrow::Status::Invalid("DB pointer is null");
    }

    if (table_name.empty()) {
        return ::arrow::Status::Invalid("Table name is empty");
    }

    // Call protected CreateRecordBatchReader() method (we have friend access)
    std::shared_ptr<::arrow::RecordBatchReader> reader;
    Status status = db->CreateRecordBatchReader(table_name, projection, predicates, &reader);

    if (!status.ok()) {
        return ::arrow::Status::IOError(
            "Failed to create reader for table '" + table_name + "': " + status.ToString());
    }

    // Eagerly initialize to validate projection columns
    auto marble_reader = std::static_pointer_cast<MarbleRecordBatchReader>(reader);
    auto init_status = marble_reader->Initialize();
    if (!init_status.ok()) {
        return ::arrow::Status::Invalid(
            "Invalid column projection for table '" + table_name + "': " + init_status.ToString());
    }

    return reader;
}

::arrow::Result<std::shared_ptr<::arrow::RecordBatchReader>> OpenTableWithOptions(
    std::shared_ptr<MarbleDB> db,
    const std::string& table_name,
    const ScanOptions& options) {

    return OpenTable(db, table_name, options.columns, options.predicates);
}

}  // namespace arrow_api
}  // namespace marble
