#include "sabot_sql/operators/join.h"
#include <arrow/compute/api_vector.h>

namespace sabot_sql {
namespace operators {

JoinOperator::JoinOperator(
    std::shared_ptr<Operator> left,
    std::shared_ptr<Operator> right,
    const std::vector<std::string>& left_keys,
    const std::vector<std::string>& right_keys,
    JoinType join_type)
    : left_(left)
    , right_(right)
    , left_keys_(left_keys)
    , right_keys_(right_keys)
    , join_type_(join_type)
    , result_computed_(false) {
}

arrow::Result<std::shared_ptr<arrow::Schema>> 
JoinOperator::GetOutputSchema() const {
    ARROW_ASSIGN_OR_RAISE(auto left_schema, left_->GetOutputSchema());
    ARROW_ASSIGN_OR_RAISE(auto right_schema, right_->GetOutputSchema());
    
    // Combine schemas (left columns + right columns)
    std::vector<std::shared_ptr<arrow::Field>> fields;
    
    for (const auto& field : left_schema->fields()) {
        fields.push_back(field);
    }
    
    for (const auto& field : right_schema->fields()) {
        // Avoid duplicate key columns
        bool is_key = false;
        for (const auto& key : right_keys_) {
            if (field->name() == key) {
                is_key = true;
                break;
            }
        }
        
        if (!is_key) {
            fields.push_back(field);
        }
    }
    
    return arrow::schema(fields);
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> 
JoinOperator::GetNextBatch() {
    if (result_computed_) {
        return nullptr;
    }
    
    // Perform join
    ARROW_ASSIGN_OR_RAISE(result_, PerformJoin());
    result_computed_ = true;
    
    // Return first batch
    if (result_->num_rows() > 0) {
        ARROW_ASSIGN_OR_RAISE(auto batch, result_->CombineChunksToBatch());
        return batch;
    }
    
    return nullptr;
}

bool JoinOperator::HasNextBatch() const {
    return !result_computed_;
}

std::string JoinOperator::ToString() const {
    std::string join_type_str;
    switch (join_type_) {
        case JoinType::INNER: join_type_str = "INNER"; break;
        case JoinType::LEFT: join_type_str = "LEFT"; break;
        case JoinType::RIGHT: join_type_str = "RIGHT"; break;
        case JoinType::FULL: join_type_str = "FULL"; break;
        case JoinType::SEMI: join_type_str = "SEMI"; break;
        case JoinType::ANTI: join_type_str = "ANTI"; break;
    }
    
    std::string keys;
    for (size_t i = 0; i < left_keys_.size(); ++i) {
        if (i > 0) keys += ", ";
        keys += left_keys_[i] + "=" + right_keys_[i];
    }
    
    return join_type_str + " Join(" + keys + ")";
}

size_t JoinOperator::EstimateCardinality() const {
    // Estimate join cardinality (simplified)
    auto left_card = left_->EstimateCardinality();
    auto right_card = right_->EstimateCardinality();
    
    // Inner join: estimate as geometric mean
    return static_cast<size_t>(std::sqrt(left_card * right_card));
}

arrow::Result<std::shared_ptr<arrow::Table>> 
JoinOperator::GetAllResults() {
    if (!result_computed_) {
        ARROW_ASSIGN_OR_RAISE(result_, PerformJoin());
        result_computed_ = true;
    }
    
    return result_;
}

arrow::Result<std::shared_ptr<arrow::Table>>
JoinOperator::PerformJoin() {
    // Get left and right tables
    ARROW_ASSIGN_OR_RAISE(auto left_table, left_->GetAllResults());
    ARROW_ASSIGN_OR_RAISE(auto right_table, right_->GetAllResults());
    
    if (left_table->num_rows() == 0 || right_table->num_rows() == 0) {
        // Empty result for inner join
        if (join_type_ == JoinType::INNER) {
            ARROW_ASSIGN_OR_RAISE(auto schema, GetOutputSchema());
            std::vector<std::shared_ptr<arrow::Array>> empty_arrays;
            return arrow::Table::Make(schema, empty_arrays);
        }
        // For outer joins, return appropriate table
        if (join_type_ == JoinType::LEFT) {
            return left_table;
        }
    }
    
    // Simple nested loop join (inefficient but correct)
    // TODO: Implement hash join for better performance
    
    if (left_keys_.size() != right_keys_.size()) {
        return arrow::Status::Invalid("Join key count mismatch");
    }
    
    // Get join key columns
    auto left_key_col = left_table->GetColumnByName(left_keys_[0]);
    auto right_key_col = right_table->GetColumnByName(right_keys_[0]);
    
    if (!left_key_col || !right_key_col) {
        return arrow::Status::Invalid("Join key column not found");
    }
    
    // Build hash table from right side
    std::unordered_multimap<std::string, int64_t> hash_table;
    
    auto right_key_array = right_key_col->chunk(0);
    for (int64_t i = 0; i < right_key_array->length(); ++i) {
        std::string key;
        
        // Convert to string for hashing
        if (right_key_array->type()->id() == arrow::Type::STRING) {
            auto str_array = std::static_pointer_cast<arrow::StringArray>(right_key_array);
            key = str_array->GetString(i);
        } else if (right_key_array->type()->id() == arrow::Type::INT64) {
            auto int_array = std::static_pointer_cast<arrow::Int64Array>(right_key_array);
            key = std::to_string(int_array->Value(i));
        } else if (right_key_array->type()->id() == arrow::Type::INT32) {
            auto int_array = std::static_pointer_cast<arrow::Int32Array>(right_key_array);
            key = std::to_string(int_array->Value(i));
        }
        
        hash_table.insert({key, i});
    }
    
    // Probe with left side
    std::vector<int64_t> left_indices;
    std::vector<int64_t> right_indices;
    
    auto left_key_array = left_key_col->chunk(0);
    for (int64_t i = 0; i < left_key_array->length(); ++i) {
        std::string key;
        
        // Convert to string
        if (left_key_array->type()->id() == arrow::Type::STRING) {
            auto str_array = std::static_pointer_cast<arrow::StringArray>(left_key_array);
            key = str_array->GetString(i);
        } else if (left_key_array->type()->id() == arrow::Type::INT64) {
            auto int_array = std::static_pointer_cast<arrow::Int64Array>(left_key_array);
            key = std::to_string(int_array->Value(i));
        } else if (left_key_array->type()->id() == arrow::Type::INT32) {
            auto int_array = std::static_pointer_cast<arrow::Int32Array>(left_key_array);
            key = std::to_string(int_array->Value(i));
        }
        
        // Find matches in hash table
        auto range = hash_table.equal_range(key);
        
        if (range.first != range.second) {
            // Found matches
            for (auto it = range.first; it != range.second; ++it) {
                left_indices.push_back(i);
                right_indices.push_back(it->second);
            }
        } else if (join_type_ == JoinType::LEFT) {
            // Left outer join - include unmatched left rows
            left_indices.push_back(i);
            right_indices.push_back(-1);  // Null marker
        }
    }
    
    // Build result using Take on indices
    arrow::Int64Builder left_idx_builder, right_idx_builder;
    ARROW_RETURN_NOT_OK(left_idx_builder.AppendValues(left_indices));
    ARROW_RETURN_NOT_OK(right_idx_builder.AppendValues(right_indices));
    
    std::shared_ptr<arrow::Array> left_idx_array, right_idx_array;
    ARROW_RETURN_NOT_OK(left_idx_builder.Finish(&left_idx_array));
    ARROW_RETURN_NOT_OK(right_idx_builder.Finish(&right_idx_array));
    
    // Take from left table
    std::vector<std::shared_ptr<arrow::ChunkedArray>> result_columns;
    
    for (int i = 0; i < left_table->num_columns(); ++i) {
        auto col = left_table->column(i);
        ARROW_ASSIGN_OR_RAISE(
            auto taken_datum,
            arrow::compute::Take(col->chunk(0), left_idx_array)
        );
        result_columns.push_back(std::make_shared<arrow::ChunkedArray>(taken_datum.make_array()));
    }
    
    // Take from right table (excluding join keys)
    for (int i = 0; i < right_table->num_columns(); ++i) {
        auto field = right_table->schema()->field(i);
        
        // Skip if it's a join key
        bool is_join_key = false;
        for (const auto& key : right_keys_) {
            if (field->name() == key) {
                is_join_key = true;
                break;
            }
        }
        
        if (!is_join_key) {
            auto col = right_table->column(i);
            ARROW_ASSIGN_OR_RAISE(
                auto taken_datum,
                arrow::compute::Take(col->chunk(0), right_idx_array)
            );
            result_columns.push_back(std::make_shared<arrow::ChunkedArray>(taken_datum.make_array()));
        }
    }
    
    // Build result table
    ARROW_ASSIGN_OR_RAISE(auto schema, GetOutputSchema());
    return arrow::Table::Make(schema, result_columns);
}

} // namespace operators
} // namespace sabot_sql

