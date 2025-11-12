/**
 * Property Path Operator
 *
 * Wraps pre-computed property path results as an Operator.
 * This allows property path query results to integrate seamlessly with the operator tree.
 */

#pragma once

#include <sabot_ql/operators/operator.h>
#include <arrow/api.h>
#include <memory>
#include <string>

namespace sabot_ql {

/**
 * Operator that wraps pre-computed property path results
 *
 * Property paths are executed eagerly during planning, producing a RecordBatch.
 * This operator wraps that batch so it can be used in joins, filters, etc.
 */
class PropertyPathOperator : public Operator {
public:
    /**
     * Construct operator with property path results
     *
     * @param batch Pre-computed results (columns: subject, object)
     * @param description Human-readable description of the property path
     */
    PropertyPathOperator(
        std::shared_ptr<arrow::RecordBatch> batch,
        const std::string& description
    ) : batch_(std::move(batch)),
        description_(description),
        executed_(false) {}

    // Operator interface
    arrow::Result<std::shared_ptr<arrow::Schema>> GetOutputSchema() const override {
        if (!batch_) {
            return arrow::Status::Invalid("Property path batch is null");
        }
        return batch_->schema();
    }

    arrow::Result<std::shared_ptr<arrow::RecordBatch>> GetNextBatch() override {
        if (executed_) {
            return nullptr;  // No more batches
        }
        executed_ = true;
        return batch_;
    }

    bool HasNextBatch() const override {
        return !executed_;
    }

    std::string ToString() const override {
        return "PropertyPath(" + description_ + ")";
    }

    size_t EstimateCardinality() const override {
        return batch_ ? batch_->num_rows() : 0;
    }

    OrderingProperty GetOutputOrdering() const override {
        // Property path results are unordered
        return OrderingProperty{};
    }

private:
    std::shared_ptr<arrow::RecordBatch> batch_;
    std::string description_;
    bool executed_;
};

} // namespace sabot_ql
