#pragma once

#include "sabot_sql/operators/operator.h"
#include <unordered_map>

namespace sabot_sql {
namespace operators {

/**
 * Join types
 */
enum class JoinType {
    INNER,
    LEFT,
    RIGHT,
    FULL,
    SEMI,
    ANTI
};

/**
 * Join operator - hash join using Arrow
 * 
 * Implements hash join algorithm:
 * 1. Build phase: Create hash table from right side
 * 2. Probe phase: Probe with left side using SIMD key comparison
 */
class JoinOperator : public Operator {
public:
    /**
     * Create join operator
     * 
     * @param left Left input operator
     * @param right Right input operator  
     * @param left_keys Left join key columns
     * @param right_keys Right join key columns
     * @param join_type Type of join
     */
    JoinOperator(
        std::shared_ptr<Operator> left,
        std::shared_ptr<Operator> right,
        const std::vector<std::string>& left_keys,
        const std::vector<std::string>& right_keys,
        JoinType join_type = JoinType::INNER
    );
    
    ~JoinOperator() override = default;
    
    // Operator interface
    arrow::Result<std::shared_ptr<arrow::Schema>> GetOutputSchema() const override;
    arrow::Result<std::shared_ptr<arrow::RecordBatch>> GetNextBatch() override;
    bool HasNextBatch() const override;
    std::string ToString() const override;
    size_t EstimateCardinality() const override;
    arrow::Result<std::shared_ptr<arrow::Table>> GetAllResults() override;

private:
    std::shared_ptr<Operator> left_;
    std::shared_ptr<Operator> right_;
    std::vector<std::string> left_keys_;
    std::vector<std::string> right_keys_;
    JoinType join_type_;
    bool result_computed_;
    std::shared_ptr<arrow::Table> result_;
    
    // Perform hash join
    arrow::Result<std::shared_ptr<arrow::Table>> PerformJoin();
};

} // namespace operators
} // namespace sabot_sql

