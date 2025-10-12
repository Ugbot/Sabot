#include <sabot_ql/operators/join.h>
#include <arrow/compute/api.h>
#include <sstream>

namespace sabot_ql {

// JoinOperator base class implementation
arrow::Result<std::shared_ptr<arrow::Schema>> JoinOperator::GetOutputSchema() const {
    ARROW_ASSIGN_OR_RAISE(auto left_schema, left_->GetOutputSchema());
    ARROW_ASSIGN_OR_RAISE(auto right_schema, right_->GetOutputSchema());

    // Combine schemas (left fields + right fields)
    // Rename duplicate column names
    std::vector<std::shared_ptr<arrow::Field>> fields;

    // Add left fields
    for (int i = 0; i < left_schema->num_fields(); ++i) {
        fields.push_back(left_schema->field(i));
    }

    // Add right fields (rename if conflicts)
    for (int i = 0; i < right_schema->num_fields(); ++i) {
        auto field = right_schema->field(i);
        std::string name = field->name();

        // Check for name conflict
        if (left_schema->GetFieldIndex(name) >= 0) {
            // Check if it's a join key
            bool is_join_key = false;
            for (size_t j = 0; j < right_keys_.size(); ++j) {
                if (right_keys_[j] == name) {
                    // This is a join key, skip adding it (already in left schema)
                    is_join_key = true;
                    break;
                }
            }

            if (!is_join_key) {
                // Rename with suffix
                name = name + "_right";
                field = arrow::field(name, field->type());
            } else {
                // Skip join key columns from right side
                continue;
            }
        }

        fields.push_back(field);
    }

    return arrow::schema(fields);
}

size_t JoinOperator::EstimateCardinality() const {
    size_t left_card = left_->EstimateCardinality();
    size_t right_card = right_->EstimateCardinality();

    // Simple heuristic: assume 10% selectivity
    // In practice, this should use statistics
    switch (join_type_) {
        case JoinType::Inner:
            return std::min(left_card, right_card) / 10;
        case JoinType::LeftOuter:
            return left_card;
        case JoinType::FullOuter:
            return left_card + right_card;
    }

    return 0;
}

std::string JoinOperator::BuildJoinKey(
    const std::shared_ptr<arrow::RecordBatch>& batch,
    int64_t row_idx,
    const std::vector<int>& key_indices) const {

    std::ostringstream oss;
    for (size_t i = 0; i < key_indices.size(); ++i) {
        if (i > 0) oss << "|";

        auto array = batch->column(key_indices[i]);
        if (array->IsNull(row_idx)) {
            oss << "NULL";
        } else {
            auto scalar_result = array->GetScalar(row_idx);
            if (scalar_result.ok()) {
                oss << scalar_result.ValueOrDie()->ToString();
            }
        }
    }
    return oss.str();
}

// HashJoinOperator implementation
arrow::Status HashJoinOperator::BuildHashTable() {
    if (hash_table_built_) {
        return arrow::Status::OK();
    }

    auto start = std::chrono::high_resolution_clock::now();

    // Materialize build side (smaller relation)
    ARROW_ASSIGN_OR_RAISE(build_table_, left_->GetAllResults());

    // Resolve key indices
    std::vector<int> key_indices;
    for (const auto& key : left_keys_) {
        int idx = build_table_->schema()->GetFieldIndex(key);
        if (idx < 0) {
            return arrow::Status::Invalid("Join key not found in left relation: " + key);
        }
        key_indices.push_back(idx);
    }

    // Build hash table - iterate over table using TableBatchReader
    arrow::TableBatchReader reader(*build_table_);
    std::shared_ptr<arrow::RecordBatch> batch;
    int64_t global_row_offset = 0;

    while (true) {
        auto batch_result = reader.Next();
        if (!batch_result.ok() || !batch_result.ValueOrDie()) {
            break;
        }

        batch = batch_result.ValueOrDie();

        for (int64_t i = 0; i < batch->num_rows(); ++i) {
            std::string join_key = BuildJoinKey(batch, i, key_indices);

            // Store row index (global index across all batches)
            int64_t global_row_idx = global_row_offset + i;
            hash_table_[join_key].push_back(global_row_idx);
        }

        global_row_offset += batch->num_rows();
    }

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
    stats_.execution_time_ms += duration.count() / 1000.0;

    hash_table_built_ = true;
    return arrow::Status::OK();
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> HashJoinOperator::ProbeNextBatch() {
    // Resolve right key indices
    ARROW_ASSIGN_OR_RAISE(auto right_schema, right_->GetOutputSchema());
    std::vector<int> right_key_indices;
    for (const auto& key : right_keys_) {
        int idx = right_schema->GetFieldIndex(key);
        if (idx < 0) {
            return arrow::Status::Invalid("Join key not found in right relation: " + key);
        }
        right_key_indices.push_back(idx);
    }

    // Process current probe batch
    while (true) {
        // Load next probe batch if needed
        if (!current_probe_batch_ || probe_row_idx_ >= current_probe_batch_->num_rows()) {
            if (!right_->HasNextBatch()) {
                return nullptr;  // No more data
            }

            ARROW_ASSIGN_OR_RAISE(current_probe_batch_, right_->GetNextBatch());
            probe_row_idx_ = 0;

            if (!current_probe_batch_) {
                return nullptr;
            }
        }

        // Probe hash table for matches
        std::vector<int64_t> left_indices;
        std::vector<int64_t> right_indices;

        // For LEFT OUTER JOIN: track which probe rows have no matches
        std::vector<int64_t> unmatched_right_indices;

        for (; probe_row_idx_ < current_probe_batch_->num_rows(); ++probe_row_idx_) {
            std::string join_key = BuildJoinKey(
                current_probe_batch_,
                probe_row_idx_,
                right_key_indices
            );

            auto it = hash_table_.find(join_key);
            if (it != hash_table_.end()) {
                // Found matches
                for (int64_t left_idx : it->second) {
                    left_indices.push_back(left_idx);
                    right_indices.push_back(probe_row_idx_);
                }
            } else if (join_type_ == JoinType::LeftOuter) {
                // No match found - for LEFT OUTER JOIN, keep this row
                unmatched_right_indices.push_back(probe_row_idx_);
            }
        }

        // Build output batch
        if (!left_indices.empty() || !unmatched_right_indices.empty()) {
            ARROW_ASSIGN_OR_RAISE(auto output_schema, GetOutputSchema());
            ARROW_ASSIGN_OR_RAISE(auto left_schema, left_->GetOutputSchema());

            std::vector<std::shared_ptr<arrow::Array>> output_columns;

            if (!left_indices.empty()) {
                // Build matched rows output

                // Take rows from left side
                arrow::Int64Builder left_idx_builder;
                ARROW_RETURN_NOT_OK(left_idx_builder.AppendValues(left_indices));
                ARROW_ASSIGN_OR_RAISE(auto left_idx_array, left_idx_builder.Finish());

                arrow::compute::ExecContext ctx;
                ARROW_ASSIGN_OR_RAISE(
                    auto left_taken,
                    arrow::compute::Take(build_table_, left_idx_array, arrow::compute::TakeOptions::Defaults(), &ctx)
                );

                // Take rows from right side
                arrow::Int64Builder right_idx_builder;
                ARROW_RETURN_NOT_OK(right_idx_builder.AppendValues(right_indices));
                ARROW_ASSIGN_OR_RAISE(auto right_idx_array, right_idx_builder.Finish());

                ARROW_ASSIGN_OR_RAISE(
                    auto right_taken,
                    arrow::compute::Take(current_probe_batch_, right_idx_array, arrow::compute::TakeOptions::Defaults(), &ctx)
                );

                // Combine left and right batches
                auto left_batch = left_taken.record_batch();
                auto right_batch = right_taken.record_batch();

                // Add left columns
                for (int i = 0; i < left_batch->num_columns(); ++i) {
                    output_columns.push_back(left_batch->column(i));
                }

                // Add right columns (skip join keys)
                for (int i = 0; i < right_batch->num_columns(); ++i) {
                    std::string col_name = right_schema->field(i)->name();

                    // Check if this is a join key
                    bool is_join_key = false;
                    for (const auto& key : right_keys_) {
                        if (key == col_name) {
                            is_join_key = true;
                            break;
                        }
                    }

                    if (!is_join_key) {
                        output_columns.push_back(right_batch->column(i));
                    }
                }

                stats_.rows_processed += left_indices.size();
                stats_.batches_processed++;

                return arrow::RecordBatch::Make(
                    output_schema,
                    left_indices.size(),
                    output_columns
                );
            } else if (!unmatched_right_indices.empty()) {
                // Build unmatched rows output (LEFT OUTER JOIN only)
                // These rows have NULL values for the build (left) side

                // Take unmatched rows from right side
                arrow::Int64Builder right_idx_builder;
                ARROW_RETURN_NOT_OK(right_idx_builder.AppendValues(unmatched_right_indices));
                ARROW_ASSIGN_OR_RAISE(auto right_idx_array, right_idx_builder.Finish());

                arrow::compute::ExecContext ctx;
                ARROW_ASSIGN_OR_RAISE(
                    auto right_taken,
                    arrow::compute::Take(current_probe_batch_, right_idx_array, arrow::compute::TakeOptions::Defaults(), &ctx)
                );
                auto right_batch = right_taken.record_batch();

                // Add left columns (all NULLs)
                for (int i = 0; i < left_schema->num_fields(); ++i) {
                    auto field = left_schema->field(i);
                    ARROW_ASSIGN_OR_RAISE(
                        auto null_array,
                        arrow::MakeArrayOfNull(field->type(), unmatched_right_indices.size())
                    );
                    output_columns.push_back(null_array);
                }

                // Add right columns (skip join keys)
                for (int i = 0; i < right_batch->num_columns(); ++i) {
                    std::string col_name = right_schema->field(i)->name();

                    // Check if this is a join key
                    bool is_join_key = false;
                    for (const auto& key : right_keys_) {
                        if (key == col_name) {
                            is_join_key = true;
                            break;
                        }
                    }

                    if (!is_join_key) {
                        output_columns.push_back(right_batch->column(i));
                    }
                }

                stats_.rows_processed += unmatched_right_indices.size();
                stats_.batches_processed++;

                return arrow::RecordBatch::Make(
                    output_schema,
                    unmatched_right_indices.size(),
                    output_columns
                );
            }
        }
    }
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> HashJoinOperator::GetNextBatch() {
    // Build hash table on first call
    if (!hash_table_built_) {
        ARROW_RETURN_NOT_OK(BuildHashTable());
    }

    // Probe hash table
    return ProbeNextBatch();
}

bool HashJoinOperator::HasNextBatch() const {
    if (!hash_table_built_) {
        return true;  // Haven't started yet
    }

    // Check if probe side has more data
    if (current_probe_batch_ && probe_row_idx_ < current_probe_batch_->num_rows()) {
        return true;
    }

    return right_->HasNextBatch();
}

std::string HashJoinOperator::ToString() const {
    std::ostringstream oss;
    oss << "HashJoin(";

    for (size_t i = 0; i < left_keys_.size(); ++i) {
        if (i > 0) oss << ", ";
        oss << left_keys_[i] << "=" << right_keys_[i];
    }

    oss << ")\n";
    oss << "  ├─ " << left_->ToString() << "\n";
    oss << "  └─ " << right_->ToString();

    return oss.str();
}

// MergeJoinOperator implementation
arrow::Status MergeJoinOperator::LoadNextBatches() {
    if (!left_batch_ && left_->HasNextBatch()) {
        ARROW_ASSIGN_OR_RAISE(left_batch_, left_->GetNextBatch());
        left_idx_ = 0;
    }

    if (!right_batch_ && right_->HasNextBatch()) {
        ARROW_ASSIGN_OR_RAISE(right_batch_, right_->GetNextBatch());
        right_idx_ = 0;
    }

    return arrow::Status::OK();
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> MergeJoinOperator::MergeStep() {
    // TODO: Implement merge join algorithm
    // This is a simplified placeholder - full implementation requires:
    // 1. Compare join keys from both sides
    // 2. Advance the side with smaller key
    // 3. When keys match, emit all combinations
    // 4. Handle UNDEF values (QLever's zipperJoinWithUndef)

    return arrow::Status::NotImplemented("Merge join not yet implemented");
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> MergeJoinOperator::GetNextBatch() {
    ARROW_RETURN_NOT_OK(LoadNextBatches());
    return MergeStep();
}

bool MergeJoinOperator::HasNextBatch() const {
    return !left_exhausted_ && !right_exhausted_;
}

std::string MergeJoinOperator::ToString() const {
    std::ostringstream oss;
    oss << "MergeJoin(";

    for (size_t i = 0; i < left_keys_.size(); ++i) {
        if (i > 0) oss << ", ";
        oss << left_keys_[i] << "=" << right_keys_[i];
    }

    oss << ")\n";
    oss << "  ├─ " << left_->ToString() << "\n";
    oss << "  └─ " << right_->ToString();

    return oss.str();
}

// NestedLoopJoinOperator implementation
arrow::Status NestedLoopJoinOperator::MaterializeRightSide() {
    if (right_materialized_) {
        return arrow::Status::OK();
    }

    ARROW_ASSIGN_OR_RAISE(right_table_, right_->GetAllResults());
    right_materialized_ = true;

    return arrow::Status::OK();
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> NestedLoopJoinOperator::GetNextBatch() {
    // Materialize right side
    if (!right_materialized_) {
        ARROW_RETURN_NOT_OK(MaterializeRightSide());
    }

    // Load left batch if needed
    if (!current_left_batch_ || left_idx_ >= current_left_batch_->num_rows()) {
        if (!left_->HasNextBatch()) {
            return nullptr;
        }

        ARROW_ASSIGN_OR_RAISE(current_left_batch_, left_->GetNextBatch());
        left_idx_ = 0;
        right_idx_ = 0;
    }

    // TODO: Implement nested loop join
    // For now, return not implemented

    return arrow::Status::NotImplemented("Nested loop join not yet implemented");
}

bool NestedLoopJoinOperator::HasNextBatch() const {
    if (!right_materialized_) {
        return true;
    }

    return left_->HasNextBatch() || (current_left_batch_ && left_idx_ < current_left_batch_->num_rows());
}

std::string NestedLoopJoinOperator::ToString() const {
    std::ostringstream oss;
    oss << "NestedLoopJoin(";

    for (size_t i = 0; i < left_keys_.size(); ++i) {
        if (i > 0) oss << ", ";
        oss << left_keys_[i] << "=" << right_keys_[i];
    }

    oss << ")\n";
    oss << "  ├─ " << left_->ToString() << "\n";
    oss << "  └─ " << right_->ToString();

    return oss.str();
}

// Factory function
std::shared_ptr<JoinOperator> CreateJoin(
    std::shared_ptr<Operator> left,
    std::shared_ptr<Operator> right,
    const std::vector<std::string>& left_keys,
    const std::vector<std::string>& right_keys,
    JoinType join_type,
    JoinAlgorithm algorithm) {

    switch (algorithm) {
        case JoinAlgorithm::Hash:
            return std::make_shared<HashJoinOperator>(
                std::move(left), std::move(right),
                left_keys, right_keys, join_type
            );

        case JoinAlgorithm::Merge:
            return std::make_shared<MergeJoinOperator>(
                std::move(left), std::move(right),
                left_keys, right_keys, join_type
            );

        case JoinAlgorithm::Nested:
            return std::make_shared<NestedLoopJoinOperator>(
                std::move(left), std::move(right),
                left_keys, right_keys, join_type
            );
    }

    // Default: Hash join
    return std::make_shared<HashJoinOperator>(
        std::move(left), std::move(right),
        left_keys, right_keys, join_type
    );
}

} // namespace sabot_ql
