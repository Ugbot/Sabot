#include "sabot/query/logical_plan.h"

namespace sabot {
namespace query {

// ScanPlan implementation

ScanPlan::ScanPlan(const std::string& source_type, const std::string& source_name)
    : source_type_(source_type), source_name_(source_name), estimated_rows_(-1) {}

bool ScanPlan::Validate() const {
    return !source_name_.empty();
}

arrow::Result<std::shared_ptr<arrow::Schema>> ScanPlan::GetSchema() const {
    if (schema_) {
        return schema_;
    }
    
    // Schema unknown for now
    return arrow::Status::NotImplemented("Schema inference not implemented");
}

int64_t ScanPlan::EstimateCardinality() const {
    return estimated_rows_ > 0 ? estimated_rows_ : 1000000;  // Default estimate
}

std::unordered_map<std::string, std::string> ScanPlan::GetMetadata() const {
    return {
        {"operator", "scan"},
        {"source_type", source_type_},
        {"source_name", source_name_}
    };
}

std::shared_ptr<LogicalPlan> ScanPlan::Clone() const {
    auto clone = std::make_shared<ScanPlan>(source_type_, source_name_);
    clone->schema_ = schema_;
    clone->estimated_rows_ = estimated_rows_;
    return clone;
}

// FilterPlan implementation

FilterPlan::FilterPlan(std::shared_ptr<LogicalPlan> input, const std::string& predicate)
    : input_(input), predicate_(predicate), selectivity_(0.5) {}

bool FilterPlan::Validate() const {
    return input_ && input_->Validate() && !predicate_.empty();
}

arrow::Result<std::shared_ptr<arrow::Schema>> FilterPlan::GetSchema() const {
    // Filter doesn't change schema
    return input_->GetSchema();
}

int64_t FilterPlan::EstimateCardinality() const {
    // Estimate based on selectivity
    int64_t input_rows = input_->EstimateCardinality();
    return static_cast<int64_t>(input_rows * selectivity_);
}

std::unordered_map<std::string, std::string> FilterPlan::GetMetadata() const {
    return {
        {"operator", "filter"},
        {"predicate", predicate_},
        {"selectivity", std::to_string(selectivity_)}
    };
}

std::shared_ptr<LogicalPlan> FilterPlan::Clone() const {
    auto clone = std::make_shared<FilterPlan>(input_->Clone(), predicate_);
    clone->selectivity_ = selectivity_;
    return clone;
}

// ProjectPlan implementation

ProjectPlan::ProjectPlan(
    std::shared_ptr<LogicalPlan> input,
    const std::vector<std::string>& columns
) : input_(input), columns_(columns) {}

bool ProjectPlan::Validate() const {
    return input_ && input_->Validate() && !columns_.empty();
}

arrow::Result<std::shared_ptr<arrow::Schema>> ProjectPlan::GetSchema() const {
    // TODO: Build schema from selected columns
    return arrow::Status::NotImplemented("Project schema not implemented");
}

int64_t ProjectPlan::EstimateCardinality() const {
    // Projection doesn't change cardinality
    return input_->EstimateCardinality();
}

std::unordered_map<std::string, std::string> ProjectPlan::GetMetadata() const {
    std::string cols;
    for (size_t i = 0; i < columns_.size(); ++i) {
        if (i > 0) cols += ",";
        cols += columns_[i];
    }
    
    return {
        {"operator", "project"},
        {"columns", cols}
    };
}

std::shared_ptr<LogicalPlan> ProjectPlan::Clone() const {
    return std::make_shared<ProjectPlan>(input_->Clone(), columns_);
}

// JoinPlan implementation

JoinPlan::JoinPlan(
    std::shared_ptr<LogicalPlan> left,
    std::shared_ptr<LogicalPlan> right,
    const std::vector<std::string>& left_keys,
    const std::vector<std::string>& right_keys,
    JoinType join_type
) : left_(left), right_(right), left_keys_(left_keys), 
    right_keys_(right_keys), join_type_(join_type) {}

bool JoinPlan::Validate() const {
    return left_ && right_ && 
           left_->Validate() && right_->Validate() &&
           left_keys_.size() == right_keys_.size() &&
           !left_keys_.empty();
}

arrow::Result<std::shared_ptr<arrow::Schema>> JoinPlan::GetSchema() const {
    // TODO: Combine left and right schemas
    return arrow::Status::NotImplemented("Join schema not implemented");
}

int64_t JoinPlan::EstimateCardinality() const {
    // Simple estimate: multiply cardinalities and apply selectivity
    int64_t left_rows = left_->EstimateCardinality();
    int64_t right_rows = right_->EstimateCardinality();
    
    // Assume 10% join selectivity
    return static_cast<int64_t>(left_rows * right_rows * 0.1);
}

std::unordered_map<std::string, std::string> JoinPlan::GetMetadata() const {
    return {
        {"operator", "join"},
        {"join_type", std::to_string(static_cast<int>(join_type_))},
        {"left_keys", std::to_string(left_keys_.size())},
        {"right_keys", std::to_string(right_keys_.size())}
    };
}

std::shared_ptr<LogicalPlan> JoinPlan::Clone() const {
    return std::make_shared<JoinPlan>(
        left_->Clone(),
        right_->Clone(),
        left_keys_,
        right_keys_,
        join_type_
    );
}

// GroupByPlan implementation

GroupByPlan::GroupByPlan(
    std::shared_ptr<LogicalPlan> input,
    const std::vector<std::string>& group_keys,
    const std::unordered_map<std::string, std::string>& aggregations
) : input_(input), group_keys_(group_keys), aggregations_(aggregations) {}

bool GroupByPlan::Validate() const {
    return input_ && input_->Validate() && 
           !group_keys_.empty() && !aggregations_.empty();
}

arrow::Result<std::shared_ptr<arrow::Schema>> GroupByPlan::GetSchema() const {
    // TODO: Build schema from group keys + aggregations
    return arrow::Status::NotImplemented("GroupBy schema not implemented");
}

int64_t GroupByPlan::EstimateCardinality() const {
    // Estimate based on number of unique groups
    // Assume 10% unique groups
    int64_t input_rows = input_->EstimateCardinality();
    return static_cast<int64_t>(input_rows * 0.1);
}

std::unordered_map<std::string, std::string> GroupByPlan::GetMetadata() const {
    return {
        {"operator", "group_by"},
        {"num_keys", std::to_string(group_keys_.size())},
        {"num_aggs", std::to_string(aggregations_.size())}
    };
}

std::shared_ptr<LogicalPlan> GroupByPlan::Clone() const {
    return std::make_shared<GroupByPlan>(input_->Clone(), group_keys_, aggregations_);
}

} // namespace query
} // namespace sabot

