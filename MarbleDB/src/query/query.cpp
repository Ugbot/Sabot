/**
 * Arrow-First Query API Implementation
 * 
 * Provides C++ native QueryBuilder with:
 * - Column projection pushdown
 * - Predicate filtering using Arrow compute
 * - Streaming RecordBatchReader results
 * - Zero-copy where possible
 */

#include "marble/query.h"
#include "marble/api.h"
#include <arrow/compute/cast.h>
#include <arrow/compute/api.h>

namespace marble {

// Forward declaration
class SimpleMarbleDB;

//=============================================================================
// Predicate Implementation
//=============================================================================

arrow::Result<arrow::compute::Expression> Predicate::ToArrowExpression() const {
    auto field = arrow::compute::field_ref(column);
    auto literal = arrow::compute::literal(value);
    
    switch (op) {
        case Op::EQ:
            return arrow::compute::equal(field, literal);
        case Op::NE:
            return arrow::compute::not_equal(field, literal);
        case Op::LT:
            return arrow::compute::less(field, literal);
        case Op::LE:
            return arrow::compute::less_equal(field, literal);
        case Op::GT:
            return arrow::compute::greater(field, literal);
        case Op::GE:
            return arrow::compute::greater_equal(field, literal);
        case Op::IN:
            return arrow::compute::is_in(field, literal);
        case Op::LIKE:
            return arrow::Status::NotImplemented("LIKE not yet implemented");
    }
    return arrow::Status::Invalid("Unknown operator");
}

//=============================================================================
// QueryRecordBatchReader - Streaming reader with query operations
//=============================================================================

class QueryRecordBatchReader : public arrow::RecordBatchReader {
public:
    QueryRecordBatchReader(
        std::shared_ptr<arrow::Schema> schema,
        std::vector<std::shared_ptr<arrow::RecordBatch>> batches,
        const std::vector<std::string>& projection,
        const std::vector<Predicate>& predicates,
        size_t limit,
        bool reverse
    ) : schema_(schema),
        batches_(std::move(batches)),
        projection_(projection),
        predicates_(predicates),
        limit_(limit),
        reverse_(reverse),
        batch_index_(reverse ? batches_.size() : 0),
        rows_emitted_(0) {}
    
    std::shared_ptr<arrow::Schema> schema() const override {
        return schema_;
    }
    
    arrow::Status ReadNext(std::shared_ptr<arrow::RecordBatch>* out) override {
        *out = nullptr;
        
        // Check limit
        if (rows_emitted_ >= limit_) {
            return arrow::Status::OK();
        }
        
        // Get next batch
        if (reverse_) {
            if (batch_index_ == 0) return arrow::Status::OK();
            batch_index_--;
        } else {
            if (batch_index_ >= batches_.size()) return arrow::Status::OK();
        }
        
        auto batch = batches_[batch_index_];
        if (!reverse_) batch_index_++;
        
        // Apply predicates (filter)
        if (!predicates_.empty()) {
            ARROW_ASSIGN_OR_RAISE(batch, ApplyPredicates(batch));
            if (batch->num_rows() == 0) {
                return ReadNext(out);  // Skip empty batch, continue to next
            }
        }
        
        // Apply projection (select columns)
        if (!projection_.empty()) {
            ARROW_ASSIGN_OR_RAISE(batch, ApplyProjection(batch));
        }
        
        // Apply limit
        if (rows_emitted_ + batch->num_rows() > limit_) {
            size_t remaining = limit_ - rows_emitted_;
            batch = batch->Slice(0, remaining);
        }
        
        rows_emitted_ += batch->num_rows();
        *out = batch;
        return arrow::Status::OK();
    }

private:
    arrow::Result<std::shared_ptr<arrow::RecordBatch>> ApplyPredicates(
        const std::shared_ptr<arrow::RecordBatch>& batch
    ) {
        if (predicates_.empty()) return batch;
        
        // Build combined filter expression (AND all predicates)
        auto expr_result = predicates_[0].ToArrowExpression();
        if (!expr_result.ok()) return expr_result.status();
        auto expr = *expr_result;
        
        for (size_t i = 1; i < predicates_.size(); ++i) {
            auto next_expr_result = predicates_[i].ToArrowExpression();
            if (!next_expr_result.ok()) return next_expr_result.status();
            expr = arrow::compute::and_(expr, *next_expr_result);
        }
        
        // Execute filter on batch
        arrow::compute::ExecContext ctx;
        ARROW_ASSIGN_OR_RAISE(
            auto filter_datum,
            arrow::compute::ExecuteScalarExpression(expr, batch, &ctx)
        );
        
        // Filter batch using boolean mask
        ARROW_ASSIGN_OR_RAISE(
            auto filtered_datum,
            arrow::compute::Filter(batch, filter_datum.make_array())
        );
        
        return filtered_datum.record_batch();
    }
    
    arrow::Result<std::shared_ptr<arrow::RecordBatch>> ApplyProjection(
        const std::shared_ptr<arrow::RecordBatch>& batch
    ) {
        if (projection_.empty()) return batch;
        
        // Get column indices
        std::vector<int> indices;
        for (const auto& col_name : projection_) {
            int idx = batch->schema()->GetFieldIndex(col_name);
            if (idx < 0) {
                return arrow::Status::Invalid("Column not found: " + col_name);
            }
            indices.push_back(idx);
        }
        
        // Select columns
        return batch->SelectColumns(indices);
    }
    
    std::shared_ptr<arrow::Schema> schema_;
    std::vector<std::shared_ptr<arrow::RecordBatch>> batches_;
    std::vector<std::string> projection_;
    std::vector<Predicate> predicates_;
    size_t limit_;
    bool reverse_;
    size_t batch_index_;
    size_t rows_emitted_;
};

//=============================================================================
// QueryBuilder Implementation
//=============================================================================

QueryBuilder::QueryBuilder(MarbleDB* db, const std::string& table_name)
    : db_(db), table_name_(table_name) {}

QueryBuilder& QueryBuilder::Scan(uint64_t start_key, uint64_t end_key) {
    start_key_ = start_key;
    end_key_ = end_key;
    return *this;
}

QueryBuilder& QueryBuilder::Project(const std::vector<std::string>& columns) {
    columns_ = columns;
    return *this;
}

QueryBuilder& QueryBuilder::Filter(
    const std::string& column,
    Predicate::Op op,
    const std::shared_ptr<arrow::Scalar>& value
) {
    predicates_.push_back(Predicate{column, op, value});
    return *this;
}

QueryBuilder& QueryBuilder::Limit(size_t n) {
    limit_ = n;
    return *this;
}

QueryBuilder& QueryBuilder::Reverse() {
    reverse_ = true;
    return *this;
}

arrow::Result<std::shared_ptr<arrow::RecordBatchReader>> QueryBuilder::Execute() {
    // TODO: This needs to be wired up properly
    // The issue is that SimpleMarbleDB is defined in api.cpp (not in a header)
    // and MarbleDB base class (in db.h) doesn't have ReadBatchesFromLSM method
    //
    // Solutions:
    // 1. Add virtual ReadBatchesFromLSM() to MarbleDB base class in db.h
    // 2. Move SimpleMarbleDB to a header file
    // 3. Use type erasure / std::function callback
    //
    // For now, returning NotImplemented until architecture is decided

    return arrow::Status::NotImplemented(
        "QueryBuilder::Execute() needs wiring to SimpleMarbleDB::ReadBatchesFromLSM(). "
        "This requires either: (1) adding virtual method to MarbleDB base class, or "
        "(2) exposing SimpleMarbleDB in a header file.");
}

arrow::Result<std::shared_ptr<arrow::Table>> QueryBuilder::ExecuteToTable() {
    ARROW_ASSIGN_OR_RAISE(auto reader, Execute());
    return reader->ToTable();
}

} // namespace marble
