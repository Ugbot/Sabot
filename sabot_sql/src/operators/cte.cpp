#include "sabot_sql/operators/cte.h"

namespace sabot_sql {

CTEOperator::CTEOperator(
    const std::string& name,
    std::shared_ptr<Operator> source,
    bool materialize_immediately)
    : name_(name)
    , source_(std::move(source))
    , materialize_immediately_(materialize_immediately)
    , materialized_(false)
    , current_batch_(0)
    , batch_size_(10000) {
    
    if (materialize_immediately_) {
        auto status = Materialize();
        if (!status.ok()) {
            // Log error but don't fail construction
        }
    }
}

arrow::Result<std::shared_ptr<arrow::Schema>> 
CTEOperator::GetOutputSchema() const {
    if (materialized_ && materialized_table_) {
        return materialized_table_->schema();
    }
    return source_->GetOutputSchema();
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> 
CTEOperator::GetNextBatch() {
    // Materialize if not already done
    if (!materialized_) {
        ARROW_RETURN_NOT_OK(Materialize());
    }
    
    if (!materialized_table_) {
        return nullptr;
    }
    
    // Return batches from materialized table
    int64_t start_row = current_batch_ * batch_size_;
    int64_t num_rows = std::min(
        static_cast<int64_t>(batch_size_),
        materialized_table_->num_rows() - start_row);
    
    if (num_rows <= 0) {
        return nullptr;
    }
    
    auto sliced_table = materialized_table_->Slice(start_row, num_rows);
    ARROW_ASSIGN_OR_RAISE(auto batch, sliced_table->CombineChunksToBatch());
    
    current_batch_++;
    stats_.rows_processed += batch->num_rows();
    stats_.batches_processed++;
    
    return batch;
}

bool CTEOperator::HasNextBatch() const {
    if (!materialized_ || !materialized_table_) {
        return true; // Will materialize on first GetNextBatch()
    }
    
    int64_t start_row = current_batch_ * batch_size_;
    return start_row < materialized_table_->num_rows();
}

std::string CTEOperator::ToString() const {
    std::string result = "CTE(name=" + name_;
    if (materialized_) {
        result += ", materialized";
        if (materialized_table_) {
            result += ", " + std::to_string(materialized_table_->num_rows()) + " rows";
        }
    } else {
        result += ", not materialized";
    }
    result += ")";
    return result;
}

size_t CTEOperator::EstimateCardinality() const {
    if (materialized_ && materialized_table_) {
        return materialized_table_->num_rows();
    }
    return source_->EstimateCardinality();
}

arrow::Status CTEOperator::Materialize() {
    if (materialized_) {
        return arrow::Status::OK();
    }
    
    // Execute source and collect all results
    ARROW_ASSIGN_OR_RAISE(auto table, source_->GetAllResults());
    materialized_table_ = table;
    materialized_ = true;
    
    return arrow::Status::OK();
}

void CTEOperator::Reset() {
    current_batch_ = 0;
}

// CTERegistry implementation
void CTERegistry::RegisterCTE(
    const std::string& name, 
    std::shared_ptr<CTEOperator> cte_op) {
    ctes_[name] = cte_op;
}

std::shared_ptr<CTEOperator> CTERegistry::GetCTE(const std::string& name) {
    auto it = ctes_.find(name);
    if (it != ctes_.end()) {
        return it->second;
    }
    return nullptr;
}

bool CTERegistry::HasCTE(const std::string& name) const {
    return ctes_.find(name) != ctes_.end();
}

arrow::Status CTERegistry::MaterializeAll() {
    for (auto& [name, cte_op] : ctes_) {
        ARROW_RETURN_NOT_OK(cte_op->Materialize());
    }
    return arrow::Status::OK();
}

void CTERegistry::Clear() {
    ctes_.clear();
}

} // namespace sabot_sql

