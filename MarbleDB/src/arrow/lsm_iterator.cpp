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

#include "marble/arrow/lsm_iterator.h"

#include <cassert>
#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/compute/api.h>
#include <arrow/type.h>
#include <iostream>

#include "marble/sstable_arrow.h"
#include "marble/api.h"  // For DeserializeArrowBatch

namespace marble {
namespace arrow_api {

//==============================================================================
// VectorBatchIterator Implementation
//==============================================================================

VectorBatchIterator::VectorBatchIterator(
    std::vector<std::shared_ptr<::arrow::RecordBatch>> batches,
    std::shared_ptr<::arrow::Schema> schema)
    : batches_(std::move(batches))
    , schema_(std::move(schema))
    , current_idx_(0) {
}

bool VectorBatchIterator::HasNext() const {
    return current_idx_ < batches_.size();
}

Status VectorBatchIterator::Next(std::shared_ptr<::arrow::RecordBatch>* batch) {
    if (!HasNext()) {
        *batch = nullptr;
        return Status::OK();
    }

    *batch = batches_[current_idx_++];
    return Status::OK();
}

std::shared_ptr<::arrow::Schema> VectorBatchIterator::schema() const {
    return schema_;
}

int64_t VectorBatchIterator::EstimatedRemainingRows() const {
    int64_t total = 0;
    for (size_t i = current_idx_; i < batches_.size(); ++i) {
        if (batches_[i]) {
            total += batches_[i]->num_rows();
        }
    }
    return total;
}

Status VectorBatchIterator::SkipRows(int64_t num_rows) {
    int64_t skipped = 0;
    while (skipped < num_rows && HasNext()) {
        std::shared_ptr<::arrow::RecordBatch> batch;
        ARROW_RETURN_NOT_OK(Next(&batch));
        if (batch) {
            skipped += batch->num_rows();
        }
    }
    return Status::OK();
}

//==============================================================================
// LSMBatchIterator Implementation
//==============================================================================

LSMBatchIterator::LSMBatchIterator(
    std::shared_ptr<::arrow::Schema> schema,
    const std::vector<std::string>& projection,
    const std::vector<ColumnPredicate>& predicates,
    int64_t batch_size)
    : schema_(std::move(schema))
    , projection_(projection)
    , predicates_(predicates)
    , batch_size_(batch_size)
    , output_row_count_(0)
    , initialized_(false)
    , finished_(false)
    , last_emitted_key_(0) {
}

LSMBatchIterator::~LSMBatchIterator() {
    // Cleanup handled by smart pointers
}

Status LSMBatchIterator::Create(
    const std::vector<std::vector<std::shared_ptr<SSTable>>>& sstables,
    std::shared_ptr<::arrow::Schema> schema,
    const std::vector<std::string>& projection,
    const std::vector<ColumnPredicate>& predicates,
    int64_t batch_size,
    std::unique_ptr<LSMBatchIterator>* iterator) {

    auto iter = std::unique_ptr<LSMBatchIterator>(
        new LSMBatchIterator(schema, projection, predicates, batch_size));

    ARROW_RETURN_NOT_OK(iter->Initialize(sstables));

    *iterator = std::move(iter);
    return Status::OK();
}

Status LSMBatchIterator::Initialize(
    const std::vector<std::vector<std::shared_ptr<SSTable>>>& sstables) {

    sstables_ = sstables;

    // Initialize output column builders
    for (int i = 0; i < schema_->num_fields(); ++i) {
        output_columns_.push_back(nullptr);  // Will be created on demand
    }

    // Open iterators for all SSTables across all levels
    for (size_t level = 0; level < sstables_.size(); ++level) {
        for (const auto& sstable : sstables_[level]) {
            // Check bloom filter pruning
            if (ShouldSkipSSTable(sstable)) {
                stats_.sstables_skipped++;
                continue;
            }

            // Open SSTable iterator
            std::shared_ptr<SSTableIteratorState> state;
            ARROW_RETURN_NOT_OK(OpenSSTable(sstable, level, &state));

            if (state && state->HasNext()) {
                // Validate state before adding to heap (defensive programming)
                assert(state->current_batch != nullptr &&
                       "State with null current_batch should not pass HasNext()");
                assert(state->row_idx < state->current_batch->num_rows() &&
                       "row_idx out of bounds in initial state");

                active_iterators_.push(state);
                stats_.sstables_opened++;
            }
        }
    }

    initialized_ = true;

    // Check if we have any data
    if (active_iterators_.empty()) {
        finished_ = true;
    }

    return Status::OK();
}

bool LSMBatchIterator::HasNext() const {
    return initialized_ && !finished_;
}

Status LSMBatchIterator::Next(std::shared_ptr<::arrow::RecordBatch>* batch) {
    if (!initialized_) {
        return Status::InvalidArgument("Iterator not initialized");
    }

    if (finished_) {
        *batch = nullptr;
        return Status::OK();
    }

    // Fill output batch using K-way merge
    ARROW_RETURN_NOT_OK(FillOutputBatch(batch));

    if (!*batch) {
        finished_ = true;
    }

    return Status::OK();
}

std::shared_ptr<::arrow::Schema> LSMBatchIterator::schema() const {
    return schema_;
}

int64_t LSMBatchIterator::EstimatedRemainingRows() const {
    // Conservative estimate: sum of all remaining rows
    // (doesn't account for deduplication)
    return -1;  // Unknown
}

Status LSMBatchIterator::SkipRows(int64_t num_rows) {
    // Simple implementation: read and discard
    int64_t skipped = 0;
    while (skipped < num_rows && HasNext()) {
        std::shared_ptr<::arrow::RecordBatch> batch;
        ARROW_RETURN_NOT_OK(Next(&batch));
        if (batch) {
            skipped += batch->num_rows();
        }
    }
    return Status::OK();
}

//==============================================================================
// SSTableIteratorState Implementation
//==============================================================================

Status LSMBatchIterator::SSTableIteratorState::Advance() {
    if (!HasNext()) {
        return Status::OK();
    }

    row_idx++;

    // Check if we need to load next batch
    if (!current_batch || row_idx >= current_batch->num_rows()) {
        if (!iterator->HasNext()) {
            current_batch = nullptr;
            current_key = 0;      // Reset to sentinel value to prevent stale data
            row_idx = 0;          // Reset index to prevent out-of-bounds access
            return Status::OK();
        }

        ARROW_RETURN_NOT_OK(iterator->Next(&current_batch));
        row_idx = 0;

        if (!current_batch || current_batch->num_rows() == 0) {
            current_batch = nullptr;
            current_key = 0;      // Reset to sentinel value
            row_idx = 0;          // Reset index
            return Status::OK();
        }
    }

    // Extract current key (assume first column is key)
    if (current_batch) {
        auto key_array = std::static_pointer_cast<::arrow::UInt64Array>(
            current_batch->column(0));
        current_key = key_array->Value(row_idx);
    }

    return Status::OK();
}

bool LSMBatchIterator::SSTableIteratorState::HasNext() const {
    return current_batch != nullptr;
}

//==============================================================================
// Iterator Management
//==============================================================================

Status LSMBatchIterator::OpenSSTable(
    std::shared_ptr<SSTable> sstable,
    int level,
    std::shared_ptr<SSTableIteratorState>* state) {

    auto iter_state = std::make_shared<SSTableIteratorState>();
    iter_state->sstable = sstable;
    iter_state->level = level;
    iter_state->row_idx = 0;

    // Try to cast to ArrowSSTable for native Arrow format
    auto arrow_sstable = std::dynamic_pointer_cast<ArrowSSTable>(sstable);
    if (arrow_sstable) {
        // Native Arrow SSTable - use pushdown interface
        std::unique_ptr<ArrowRecordBatchIterator> iterator;
        ARROW_RETURN_NOT_OK(arrow_sstable->ScanWithPushdown(
            projection_, predicates_, batch_size_, &iterator));

        iter_state->iterator = std::move(iterator);

        // Load first batch
        if (iter_state->iterator->HasNext()) {
            ARROW_RETURN_NOT_OK(iter_state->iterator->Next(&iter_state->current_batch));
            stats_.batches_read++;

            if (iter_state->current_batch && iter_state->current_batch->num_rows() > 0) {
                // Extract first key
                auto key_array = std::static_pointer_cast<::arrow::UInt64Array>(
                    iter_state->current_batch->column(0));
                iter_state->current_key = key_array->Value(0);

                *state = iter_state;
                return Status::OK();
            }
        }
    } else {
        // Old-format SSTable - contains serialized Arrow batches as values
        // Read all key-value pairs and deserialize all batches

        const auto& metadata = sstable->GetMetadata();

        // Scan entire SSTable to get all entries
        std::vector<std::pair<uint64_t, std::string>> entries;
        ARROW_RETURN_NOT_OK(sstable->Scan(metadata.min_key, metadata.max_key, &entries));

        if (entries.empty()) {
            *state = nullptr;
            return Status::OK();
        }

        // Deserialize ALL batches from this SSTable
        std::vector<std::shared_ptr<::arrow::RecordBatch>> all_batches;
        all_batches.reserve(entries.size());

        for (const auto& entry : entries) {
            const std::string& serialized_batch = entry.second;

            std::shared_ptr<::arrow::RecordBatch> batch;
            ARROW_RETURN_NOT_OK(DeserializeArrowBatch(serialized_batch, &batch));

            if (batch && batch->num_rows() > 0) {
                all_batches.push_back(batch);
                stats_.batches_read++;
            }
        }

        if (all_batches.empty()) {
            *state = nullptr;
            return Status::OK();
        }

        // Create VectorBatchIterator to iterate through all batches
        auto vector_iterator = std::make_unique<VectorBatchIterator>(
            std::move(all_batches), schema_);

        iter_state->iterator = std::move(vector_iterator);

        // Load first batch from iterator to initialize state
        if (iter_state->iterator->HasNext()) {
            ARROW_RETURN_NOT_OK(iter_state->iterator->Next(&iter_state->current_batch));

            if (iter_state->current_batch && iter_state->current_batch->num_rows() > 0) {
                iter_state->row_idx = 0;

                // Extract first key from batch
                auto key_array = std::static_pointer_cast<::arrow::UInt64Array>(
                    iter_state->current_batch->column(0));
                iter_state->current_key = key_array->Value(0);

                *state = iter_state;
                return Status::OK();
            }
        }
    }

    // No data in this SSTable
    *state = nullptr;
    return Status::OK();
}

bool LSMBatchIterator::ShouldSkipSSTable(std::shared_ptr<SSTable> sstable) {
    if (!sstable) {
        return true;  // Skip null SSTables
    }

    // No predicates - can't skip
    if (predicates_.empty()) {
        return false;
    }

    // Get SSTable metadata
    const auto& metadata = sstable->GetMetadata();

    // Check bloom filter if available (serialized bloom filter data)
    if (!metadata.bloom_filter.empty()) {
        // For each equality predicate, check bloom filter
        for (const auto& pred : predicates_) {
            if (pred.predicate_type == ColumnPredicate::PredicateType::kEqual) {
                // Bloom filter returns:
                // - false: definitely not present (safe to skip)
                // - true: might be present (must read)

                // Extract key from predicate value (Arrow Scalar)
                if (pred.value && pred.value->type->id() == ::arrow::Type::UINT64) {
                    auto uint_scalar = std::static_pointer_cast<::arrow::UInt64Scalar>(pred.value);
                    uint64_t key_to_check = uint_scalar->value;

                    // Deserialize and check bloom filter
                    // For now, skip bloom filter check since it's serialized
                    // TODO: Deserialize bloom filter and check
                    // In practice, would cache deserialized bloom filters in memory
                }
            }
        }
    }

    // Check min/max key range for key-based predicates
    // SSTableMetadata has TWO ranges:
    // 1. min_key/max_key: LSM storage keys (EncodeBatchKey for RecordBatch API)
    // 2. data_min_key/data_max_key: First data column values (for predicate pushdown)
    for (const auto& pred : predicates_) {
        // For data column predicates (column_name set), use data_min_key/data_max_key
        // For LSM storage key predicates (column_name empty), use min_key/max_key
        if (!pred.column_name.empty() && metadata.has_data_range) {
            // Data column predicate with available data ranges
            if (pred.value && pred.value->type->id() == ::arrow::Type::UINT64) {
                auto uint_scalar = std::static_pointer_cast<::arrow::UInt64Scalar>(pred.value);
                uint64_t pred_val = uint_scalar->value;

                // Use data column ranges for pruning
                if (pred.predicate_type == ColumnPredicate::PredicateType::kEqual) {
                    if (pred_val < metadata.data_min_key || pred_val > metadata.data_max_key) {
                        stats_.sstables_skipped++;
                        return true;  // Skip this SSTable
                    }
                }
                else if (pred.predicate_type == ColumnPredicate::PredicateType::kGreaterThan) {
                    if (metadata.data_max_key <= pred_val) {
                        stats_.sstables_skipped++;
                        return true;
                    }
                }
                else if (pred.predicate_type == ColumnPredicate::PredicateType::kGreaterThanOrEqual) {
                    if (metadata.data_max_key < pred_val) {
                        stats_.sstables_skipped++;
                        return true;
                    }
                }
                else if (pred.predicate_type == ColumnPredicate::PredicateType::kLessThan) {
                    if (metadata.data_min_key >= pred_val) {
                        stats_.sstables_skipped++;
                        return true;
                    }
                }
                else if (pred.predicate_type == ColumnPredicate::PredicateType::kLessThanOrEqual) {
                    if (metadata.data_min_key > pred_val) {
                        stats_.sstables_skipped++;
                        return true;
                    }
                }
            }
        }
        else if (pred.column_name.empty()) {
            // LSM storage key predicate (legacy/internal use)
            if (pred.value && pred.value->type->id() == ::arrow::Type::UINT64) {
                auto uint_scalar = std::static_pointer_cast<::arrow::UInt64Scalar>(pred.value);
                uint64_t pred_val = uint_scalar->value;

                // For equality predicates: value must be in [min_key, max_key]
                if (pred.predicate_type == ColumnPredicate::PredicateType::kEqual) {
                    // If value outside [min_key, max_key], skip entire SSTable
                    if (pred_val < metadata.min_key || pred_val > metadata.max_key) {
                        return true;
                    }
                }
                // For range predicates: check if ranges overlap
                else if (pred.predicate_type == ColumnPredicate::PredicateType::kGreaterThan) {
                    // If all values in SSTable are <= pred_val, skip
                    if (metadata.max_key <= pred_val) {
                        return true;
                    }
                }
                else if (pred.predicate_type == ColumnPredicate::PredicateType::kGreaterThanOrEqual) {
                    // If all values in SSTable are < pred_val, skip
                    if (metadata.max_key < pred_val) {
                        return true;
                    }
                }
                else if (pred.predicate_type == ColumnPredicate::PredicateType::kLessThan) {
                    // If all values in SSTable are >= pred_val, skip
                    if (metadata.min_key >= pred_val) {
                        return true;
                    }
                }
                else if (pred.predicate_type == ColumnPredicate::PredicateType::kLessThanOrEqual) {
                    // If all values in SSTable are > pred_val, skip
                    if (metadata.min_key > pred_val) {
                        return true;
                    }
                }
            }
        }
    }

    return false;  // Can't definitively skip
}

bool LSMBatchIterator::ShouldSkipBatch(
    const std::shared_ptr<::arrow::RecordBatch>& batch) {

    if (!batch || batch->num_rows() == 0) {
        return true;  // Skip empty batches
    }

    // No predicates - can't skip
    if (predicates_.empty()) {
        return false;
    }

    // For each predicate, check if batch can be skipped using zone maps
    // Zone maps track min/max values per column per batch
    // If predicate is outside [min, max], batch can be skipped
    for (const auto& pred : predicates_) {
        // Find column in batch schema
        auto field = batch->schema()->GetFieldByName(pred.column_name);
        if (!field) {
            continue;  // Column not in batch
        }

        int col_idx = batch->schema()->GetFieldIndex(pred.column_name);
        if (col_idx < 0) {
            continue;
        }

        auto column = batch->column(col_idx);
        if (!column) {
            continue;
        }

        // Compute min/max for this column using Arrow compute functions
        ::arrow::compute::ExecContext ctx;

        // Min value
        auto min_result = ::arrow::compute::CallFunction(
            "min", {column}, &ctx);
        if (!min_result.ok()) {
            continue;  // Can't skip if can't compute min
        }

        // Max value
        auto max_result = ::arrow::compute::CallFunction(
            "max", {column}, &ctx);
        if (!max_result.ok()) {
            continue;  // Can't skip if can't compute max
        }

        auto min_scalar = min_result->scalar();
        auto max_scalar = max_result->scalar();

        // Type-specific comparison
        // Handle different Arrow types
        if (field->type()->id() == ::arrow::Type::UINT64 &&
            pred.value && pred.value->type->id() == ::arrow::Type::UINT64) {

            auto pred_uint = std::static_pointer_cast<::arrow::UInt64Scalar>(pred.value);
            auto min_uint = std::static_pointer_cast<::arrow::UInt64Scalar>(min_scalar);
            auto max_uint = std::static_pointer_cast<::arrow::UInt64Scalar>(max_scalar);

            uint64_t pred_val = pred_uint->value;
            uint64_t min_val = min_uint->value;
            uint64_t max_val = max_uint->value;

            // Check predicate against [min, max] range
            if (pred.predicate_type == ColumnPredicate::PredicateType::kEqual) {
                // If value outside [min, max], skip batch
                if (pred_val < min_val || pred_val > max_val) {
                    return true;
                }
            }
            else if (pred.predicate_type == ColumnPredicate::PredicateType::kGreaterThan) {
                // If all values <= pred_val, skip batch
                if (max_val <= pred_val) {
                    return true;
                }
            }
            else if (pred.predicate_type == ColumnPredicate::PredicateType::kGreaterThanOrEqual) {
                // If all values < pred_val, skip batch
                if (max_val < pred_val) {
                    return true;
                }
            }
            else if (pred.predicate_type == ColumnPredicate::PredicateType::kLessThan) {
                // If all values >= pred_val, skip batch
                if (min_val >= pred_val) {
                    return true;
                }
            }
            else if (pred.predicate_type == ColumnPredicate::PredicateType::kLessThanOrEqual) {
                // If all values > pred_val, skip batch
                if (min_val > pred_val) {
                    return true;
                }
            }
        }
        else if (field->type()->id() == ::arrow::Type::INT64 &&
                 pred.value && pred.value->type->id() == ::arrow::Type::INT64) {

            auto pred_int = std::static_pointer_cast<::arrow::Int64Scalar>(pred.value);
            auto min_int = std::static_pointer_cast<::arrow::Int64Scalar>(min_scalar);
            auto max_int = std::static_pointer_cast<::arrow::Int64Scalar>(max_scalar);

            int64_t pred_val = pred_int->value;
            int64_t min_val = min_int->value;
            int64_t max_val = max_int->value;

            // Same logic as uint64
            if (pred.predicate_type == ColumnPredicate::PredicateType::kEqual) {
                if (pred_val < min_val || pred_val > max_val) {
                    return true;
                }
            }
            else if (pred.predicate_type == ColumnPredicate::PredicateType::kGreaterThan) {
                if (max_val <= pred_val) {
                    return true;
                }
            }
            else if (pred.predicate_type == ColumnPredicate::PredicateType::kGreaterThanOrEqual) {
                if (max_val < pred_val) {
                    return true;
                }
            }
            else if (pred.predicate_type == ColumnPredicate::PredicateType::kLessThan) {
                if (min_val >= pred_val) {
                    return true;
                }
            }
            else if (pred.predicate_type == ColumnPredicate::PredicateType::kLessThanOrEqual) {
                if (min_val > pred_val) {
                    return true;
                }
            }
        }
        // TODO: Add support for other Arrow types (string, double, timestamp, etc.)
    }

    return false;  // Can't definitively skip
}

//==============================================================================
// K-way Merge
//==============================================================================

Status LSMBatchIterator::FillOutputBatch(
    std::shared_ptr<::arrow::RecordBatch>* batch) {

    output_row_count_ = 0;

    // Build arrays for each column
    std::vector<std::unique_ptr<::arrow::ArrayBuilder>> builders;
    for (int i = 0; i < schema_->num_fields(); ++i) {
        auto field = schema_->field(i);

        // Create builder for this column type
        if (field->type()->id() == ::arrow::Type::UINT64) {
            builders.push_back(std::make_unique<::arrow::UInt64Builder>());
        } else if (field->type()->id() == ::arrow::Type::INT64) {
            builders.push_back(std::make_unique<::arrow::Int64Builder>());
        } else if (field->type()->id() == ::arrow::Type::BINARY) {
            builders.push_back(std::make_unique<::arrow::BinaryBuilder>());
        } else if (field->type()->id() == ::arrow::Type::STRING) {
            builders.push_back(std::make_unique<::arrow::StringBuilder>());
        } else {
            return Status::NotImplemented(
                "Column type not yet supported in LSMBatchIterator: " +
                field->type()->ToString());
        }
    }

    // Perform K-way merge until we fill a batch
    while (!active_iterators_.empty() && output_row_count_ < batch_size_) {
        auto top = active_iterators_.top();
        active_iterators_.pop();

        stats_.merge_operations++;

        // Check for deduplication
        if (output_row_count_ > 0 && top->current_key == last_emitted_key_) {
            // Duplicate key - skip this row (we already emitted it from higher level)
            stats_.rows_deduplicated++;

            // Advance this iterator
            ARROW_RETURN_NOT_OK(top->Advance());
            if (top->HasNext()) {
                active_iterators_.push(top);
            }
            continue;
        }

        // Defensive check: Ensure iterator state is valid before accessing
        if (!top || !top->current_batch || top->row_idx >= top->current_batch->num_rows()) {
            return Status::InternalError("Iterator state corrupted: current_batch is null or row_idx out of bounds");
        }

        // Append row to output
        for (int col_idx = 0; col_idx < schema_->num_fields(); ++col_idx) {
            auto array = top->current_batch->column(col_idx);

            if (array->type()->id() == ::arrow::Type::UINT64) {
                auto typed_array = std::static_pointer_cast<::arrow::UInt64Array>(array);
                auto builder = static_cast<::arrow::UInt64Builder*>(builders[col_idx].get());
                ARROW_RETURN_NOT_OK(builder->Append(typed_array->Value(top->row_idx)));
            } else if (array->type()->id() == ::arrow::Type::INT64) {
                auto typed_array = std::static_pointer_cast<::arrow::Int64Array>(array);
                auto builder = static_cast<::arrow::Int64Builder*>(builders[col_idx].get());
                ARROW_RETURN_NOT_OK(builder->Append(typed_array->Value(top->row_idx)));
            } else if (array->type()->id() == ::arrow::Type::BINARY) {
                auto typed_array = std::static_pointer_cast<::arrow::BinaryArray>(array);
                auto builder = static_cast<::arrow::BinaryBuilder*>(builders[col_idx].get());
                ARROW_RETURN_NOT_OK(builder->Append(typed_array->GetView(top->row_idx)));
            } else if (array->type()->id() == ::arrow::Type::STRING) {
                auto typed_array = std::static_pointer_cast<::arrow::StringArray>(array);
                auto builder = static_cast<::arrow::StringBuilder*>(builders[col_idx].get());
                ARROW_RETURN_NOT_OK(builder->Append(typed_array->GetView(top->row_idx)));
            }
        }

        output_row_count_++;
        last_emitted_key_ = top->current_key;
        stats_.rows_read++;

        // Advance this iterator
        ARROW_RETURN_NOT_OK(top->Advance());
        if (top->HasNext()) {
            active_iterators_.push(top);
        }
    }

    // Build output batch
    if (output_row_count_ == 0) {
        *batch = nullptr;
        return Status::OK();
    }

    // Finish all builders
    std::vector<std::shared_ptr<::arrow::Array>> arrays;
    for (auto& builder : builders) {
        std::shared_ptr<::arrow::Array> array;
        ARROW_RETURN_NOT_OK(builder->Finish(&array));
        arrays.push_back(array);
    }

    *batch = ::arrow::RecordBatch::Make(schema_, output_row_count_, arrays);
    return Status::OK();
}

bool LSMBatchIterator::ShouldDeduplicateRow(uint64_t key) {
    return (output_row_count_ > 0 && key == last_emitted_key_);
}

}  // namespace arrow_api
}  // namespace marble
