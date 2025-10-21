#include "sabot_sql/operators/aggregate.h"
#include "sabot_sql/sql/string_operations.h"
#include <arrow/compute/cast.h>
#include <unordered_map>

namespace sabot_sql {
namespace operators {

AggregateOperator::AggregateOperator(
    std::shared_ptr<Operator> child,
    const std::vector<AggregationSpec>& aggregations)
    : child_(child)
    , aggregations_(aggregations)
    , has_group_by_(false)
    , result_computed_(false) {
}

AggregateOperator::AggregateOperator(
    std::shared_ptr<Operator> child,
    const std::vector<std::string>& group_by_columns,
    const std::vector<AggregationSpec>& aggregations)
    : child_(child)
    , group_by_columns_(group_by_columns)
    , aggregations_(aggregations)
    , has_group_by_(true)
    , result_computed_(false) {
}

arrow::Result<std::shared_ptr<arrow::Schema>> 
AggregateOperator::GetOutputSchema() const {
    std::vector<std::shared_ptr<arrow::Field>> fields;
    
    // Add group by columns
    if (has_group_by_) {
        ARROW_ASSIGN_OR_RAISE(auto child_schema, child_->GetOutputSchema());
        for (const auto& col_name : group_by_columns_) {
            auto field = child_schema->GetFieldByName(col_name);
            if (field) {
                fields.push_back(field);
            }
        }
    }
    
    // Add aggregation result columns
    for (const auto& agg : aggregations_) {
        // Determine output type based on aggregation
        std::shared_ptr<arrow::DataType> type;
        switch (agg.type) {
            case AggregationType::COUNT:
            case AggregationType::COUNT_DISTINCT:
                type = arrow::int64();
                break;
            case AggregationType::SUM:
            case AggregationType::AVG:
                type = arrow::float64();
                break;
            case AggregationType::MIN:
            case AggregationType::MAX:
                type = arrow::float64();  // Default
                break;
        }
        
        fields.push_back(arrow::field(agg.output_name, type));
    }
    
    return arrow::schema(fields);
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> 
AggregateOperator::GetNextBatch() {
    if (result_computed_) {
        return nullptr;  // Already returned result
    }
    
    // Compute aggregations
    ARROW_ASSIGN_OR_RAISE(result_, ComputeAggregations());
    result_computed_ = true;
    
    // Convert first batch to RecordBatch
    if (result_->num_rows() > 0) {
        ARROW_ASSIGN_OR_RAISE(auto batch, result_->CombineChunksToBatch());
        return batch;
    }
    
    return nullptr;
}

bool AggregateOperator::HasNextBatch() const {
    return !result_computed_;
}

std::string AggregateOperator::ToString() const {
    std::string aggs;
    for (size_t i = 0; i < aggregations_.size(); ++i) {
        if (i > 0) aggs += ", ";
        
        switch (aggregations_[i].type) {
            case AggregationType::COUNT: aggs += "COUNT"; break;
            case AggregationType::COUNT_DISTINCT: aggs += "COUNT DISTINCT"; break;
            case AggregationType::SUM: aggs += "SUM"; break;
            case AggregationType::AVG: aggs += "AVG"; break;
            case AggregationType::MIN: aggs += "MIN"; break;
            case AggregationType::MAX: aggs += "MAX"; break;
        }
        
        if (!aggregations_[i].column_name.empty()) {
            aggs += "(" + aggregations_[i].column_name + ")";
        } else {
            aggs += "(*)";
        }
    }
    
    if (has_group_by_) {
        std::string groups;
        for (size_t i = 0; i < group_by_columns_.size(); ++i) {
            if (i > 0) groups += ", ";
            groups += group_by_columns_[i];
        }
        return "Aggregate(" + aggs + " GROUP BY " + groups + ")";
    }
    
    return "Aggregate(" + aggs + ")";
}

size_t AggregateOperator::EstimateCardinality() const {
    if (has_group_by_) {
        return child_->EstimateCardinality() / 10;
    }
    return 1;
}

arrow::Result<std::shared_ptr<arrow::Table>> 
AggregateOperator::GetAllResults() {
    if (!result_computed_) {
        ARROW_ASSIGN_OR_RAISE(result_, ComputeAggregations());
        result_computed_ = true;
    }
    
    return result_;
}

arrow::Result<std::shared_ptr<arrow::Table>>
AggregateOperator::ComputeAggregations() {
    // Get all data from child
    ARROW_ASSIGN_OR_RAISE(auto input_table, child_->GetAllResults());
    
    if (input_table->num_rows() == 0) {
        ARROW_ASSIGN_OR_RAISE(auto schema, GetOutputSchema());
        std::vector<std::shared_ptr<arrow::Array>> empty_arrays;
        return arrow::Table::Make(schema, empty_arrays);
    }
    
    if (!has_group_by_) {
        // Simple aggregation without GROUP BY
        std::vector<std::shared_ptr<arrow::Array>> result_arrays;
        
        for (const auto& agg : aggregations_) {
            ARROW_ASSIGN_OR_RAISE(auto scalar, ComputeSingleAggregation(input_table, agg));
            
            // Convert scalar to array with one element
            ARROW_ASSIGN_OR_RAISE(auto array, arrow::MakeArrayFromScalar(*scalar, 1));
            result_arrays.push_back(array);
        }
        
        ARROW_ASSIGN_OR_RAISE(auto schema, GetOutputSchema());
        return arrow::Table::Make(schema, result_arrays, 1);
        
    } else {
        // GROUP BY aggregation using manual hash table
        // Build hash map: key -> {sum, count, min, max}
        
        struct GroupState {
            double sum = 0.0;
            int64_t count = 0;
            double min = std::numeric_limits<double>::max();
            double max = std::numeric_limits<double>::lowest();
        };
        
        std::unordered_map<std::string, GroupState> groups;
        
        // Get group by column
        if (group_by_columns_.empty()) {
            return arrow::Status::Invalid("GROUP BY requires group columns");
        }
        
        auto group_column = input_table->GetColumnByName(group_by_columns_[0]);
        if (!group_column) {
            return arrow::Status::Invalid("Group column not found: " + group_by_columns_[0]);
        }
        
        // Get value column for aggregations
        std::shared_ptr<arrow::ChunkedArray> value_column;
        if (!aggregations_.empty() && !aggregations_[0].column_name.empty()) {
            value_column = input_table->GetColumnByName(aggregations_[0].column_name);
        }
        
        // Build groups
        auto group_array = group_column->chunk(0);
        
        for (int64_t i = 0; i < group_array->length(); ++i) {
            // Get group key
            std::string key;
            if (group_array->type()->id() == arrow::Type::STRING) {
                auto str_array = std::static_pointer_cast<arrow::StringArray>(group_array);
                key = str_array->GetString(i);
            } else if (group_array->type()->id() == arrow::Type::INT32) {
                auto int_array = std::static_pointer_cast<arrow::Int32Array>(group_array);
                key = std::to_string(int_array->Value(i));
            } else if (group_array->type()->id() == arrow::Type::INT64) {
                auto int_array = std::static_pointer_cast<arrow::Int64Array>(group_array);
                key = std::to_string(int_array->Value(i));
            } else {
                return arrow::Status::NotImplemented("Group by type not supported");
            }
            
            // Get value
            double value = 0.0;
            if (value_column) {
                auto value_array = value_column->chunk(0);
                if (value_array->type()->id() == arrow::Type::DOUBLE) {
                    auto dbl_array = std::static_pointer_cast<arrow::DoubleArray>(value_array);
                    value = dbl_array->Value(i);
                } else if (value_array->type()->id() == arrow::Type::INT32) {
                    auto int_array = std::static_pointer_cast<arrow::Int32Array>(value_array);
                    value = static_cast<double>(int_array->Value(i));
                } else if (value_array->type()->id() == arrow::Type::INT64) {
                    auto int_array = std::static_pointer_cast<arrow::Int64Array>(value_array);
                    value = static_cast<double>(int_array->Value(i));
                }
            }
            
            // Update group state
            auto& state = groups[key];
            state.sum += value;
            state.count++;
            state.min = std::min(state.min, value);
            state.max = std::max(state.max, value);
        }
        
        // Build result arrays
        arrow::StringBuilder key_builder;
        std::vector<arrow::DoubleBuilder> agg_builders(aggregations_.size());
        std::vector<arrow::Int64Builder> count_builders;
        
        // Reserve count builders for COUNT aggregations
        for (const auto& agg : aggregations_) {
            if (agg.type == AggregationType::COUNT || agg.type == AggregationType::COUNT_DISTINCT) {
                count_builders.emplace_back();
            }
        }
        
        // Fill arrays
        for (const auto& [key, state] : groups) {
            ARROW_RETURN_NOT_OK(key_builder.Append(key));
            
            size_t count_idx = 0;
            for (size_t i = 0; i < aggregations_.size(); ++i) {
                const auto& agg = aggregations_[i];
                
                switch (agg.type) {
                    case AggregationType::COUNT:
                    case AggregationType::COUNT_DISTINCT:
                        if (count_idx < count_builders.size()) {
                            ARROW_RETURN_NOT_OK(count_builders[count_idx].Append(state.count));
                            count_idx++;
                        }
                        break;
                    case AggregationType::SUM:
                        ARROW_RETURN_NOT_OK(agg_builders[i].Append(state.sum));
                        break;
                    case AggregationType::AVG:
                        ARROW_RETURN_NOT_OK(agg_builders[i].Append(state.sum / state.count));
                        break;
                    case AggregationType::MIN:
                        ARROW_RETURN_NOT_OK(agg_builders[i].Append(state.min));
                        break;
                    case AggregationType::MAX:
                        ARROW_RETURN_NOT_OK(agg_builders[i].Append(state.max));
                        break;
                }
            }
        }
        
        // Finish arrays
        std::vector<std::shared_ptr<arrow::Array>> result_arrays;
        
        std::shared_ptr<arrow::Array> key_array;
        ARROW_RETURN_NOT_OK(key_builder.Finish(&key_array));
        result_arrays.push_back(key_array);
        
        size_t count_idx2 = 0;
        for (size_t i = 0; i < aggregations_.size(); ++i) {
            const auto& agg = aggregations_[i];
            
            if (agg.type == AggregationType::COUNT || agg.type == AggregationType::COUNT_DISTINCT) {
                if (count_idx2 < count_builders.size()) {
                    std::shared_ptr<arrow::Array> count_array;
                    ARROW_RETURN_NOT_OK(count_builders[count_idx2].Finish(&count_array));
                    result_arrays.push_back(count_array);
                    count_idx2++;
                }
            } else {
                std::shared_ptr<arrow::Array> agg_array;
                ARROW_RETURN_NOT_OK(agg_builders[i].Finish(&agg_array));
                result_arrays.push_back(agg_array);
            }
        }
        
        // Create result table
        ARROW_ASSIGN_OR_RAISE(auto schema, GetOutputSchema());
        return arrow::Table::Make(schema, result_arrays);
    }
}

arrow::Result<std::shared_ptr<arrow::Scalar>>
AggregateOperator::ComputeSingleAggregation(
    const std::shared_ptr<arrow::Table>& table,
    const AggregationSpec& agg) {
    
    switch (agg.type) {
        case AggregationType::COUNT: {
            // COUNT(*) - just return row count
            int64_t count = table->num_rows();
            return arrow::MakeScalar(count);
        }
        
        case AggregationType::COUNT_DISTINCT: {
            // COUNT(DISTINCT column)
            if (agg.column_name.empty()) {
                return arrow::Status::Invalid("COUNT DISTINCT requires column name");
            }
            
            auto column = table->GetColumnByName(agg.column_name);
            if (!column) {
                return arrow::Status::Invalid("Column not found: " + agg.column_name);
            }
            
            // Use our string operations for strings, Arrow compute for others
            if (column->type()->id() == arrow::Type::STRING) {
                ARROW_ASSIGN_OR_RAISE(
                    int64_t count,
                    sql::StringOperations::CountDistinct(column->chunk(0))
                );
                return arrow::MakeScalar(count);
            } else {
                ARROW_ASSIGN_OR_RAISE(
                    auto unique,
                    arrow::compute::Unique(*column->chunk(0))
                );
                return arrow::MakeScalar(static_cast<int64_t>(unique->length()));
            }
        }
        
        case AggregationType::SUM: {
            if (agg.column_name.empty()) {
                return arrow::Status::Invalid("SUM requires column name");
            }
            
            auto column = table->GetColumnByName(agg.column_name);
            if (!column) {
                return arrow::Status::Invalid("Column not found: " + agg.column_name);
            }
            
            ARROW_ASSIGN_OR_RAISE(
                auto sum_datum,
                arrow::compute::Sum(*column->chunk(0))
            );
            return sum_datum.scalar();
        }
        
        case AggregationType::AVG: {
            if (agg.column_name.empty()) {
                return arrow::Status::Invalid("AVG requires column name");
            }
            
            auto column = table->GetColumnByName(agg.column_name);
            if (!column) {
                return arrow::Status::Invalid("Column not found: " + agg.column_name);
            }
            
            ARROW_ASSIGN_OR_RAISE(
                auto mean_datum,
                arrow::compute::Mean(*column->chunk(0))
            );
            return mean_datum.scalar();
        }
        
        case AggregationType::MIN: {
            if (agg.column_name.empty()) {
                return arrow::Status::Invalid("MIN requires column name");
            }
            
            auto column = table->GetColumnByName(agg.column_name);
            if (!column) {
                return arrow::Status::Invalid("Column not found: " + agg.column_name);
            }
            
            ARROW_ASSIGN_OR_RAISE(
                auto min_datum,
                arrow::compute::MinMax(*column->chunk(0))
            );
            auto minmax_scalar = std::static_pointer_cast<arrow::StructScalar>(min_datum.scalar());
            return minmax_scalar->value[0];  // min is first element
        }
        
        case AggregationType::MAX: {
            if (agg.column_name.empty()) {
                return arrow::Status::Invalid("MAX requires column name");
            }
            
            auto column = table->GetColumnByName(agg.column_name);
            if (!column) {
                return arrow::Status::Invalid("Column not found: " + agg.column_name);
            }
            
            ARROW_ASSIGN_OR_RAISE(
                auto minmax_datum,
                arrow::compute::MinMax(*column->chunk(0))
            );
            auto minmax_scalar = std::static_pointer_cast<arrow::StructScalar>(minmax_datum.scalar());
            return minmax_scalar->value[1];  // max is second element
        }
        
        default:
            return arrow::Status::NotImplemented("Aggregation type not supported");
    }
}

} // namespace operators
} // namespace sabot_sql

