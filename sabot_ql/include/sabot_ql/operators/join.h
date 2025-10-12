#pragma once

#include <sabot_ql/operators/operator.h>
#include <sabot_ql/util/hash_map.h>
#include <memory>
#include <string>
#include <vector>

namespace sabot_ql {

// Join type for SPARQL
enum class JoinType {
    Inner,      // Standard join (both sides must match)
    LeftOuter,  // Left outer join (OPTIONAL in SPARQL)
    FullOuter   // Full outer join (rare in SPARQL)
};

// Join algorithm selection
enum class JoinAlgorithm {
    Hash,       // Hash join (best for unsorted inputs)
    Merge,      // Merge join (requires sorted inputs)
    Nested      // Nested loop join (fallback for small inputs)
};

// Base class for all join operators
class JoinOperator : public BinaryOperator {
public:
    JoinOperator(std::shared_ptr<Operator> left,
                 std::shared_ptr<Operator> right,
                 const std::vector<std::string>& left_keys,
                 const std::vector<std::string>& right_keys,
                 JoinType join_type = JoinType::Inner)
        : BinaryOperator(std::move(left), std::move(right)),
          left_keys_(left_keys),
          right_keys_(right_keys),
          join_type_(join_type) {

        if (left_keys.size() != right_keys.size()) {
            throw std::invalid_argument("Left and right key counts must match");
        }
    }

    arrow::Result<std::shared_ptr<arrow::Schema>> GetOutputSchema() const override;
    size_t EstimateCardinality() const override;

protected:
    std::vector<std::string> left_keys_;
    std::vector<std::string> right_keys_;
    JoinType join_type_;

    // Helper: Build join key from row
    std::string BuildJoinKey(const std::shared_ptr<arrow::RecordBatch>& batch,
                             int64_t row_idx,
                             const std::vector<int>& key_indices) const;
};

// Hash join: Build hash table from smaller input, probe with larger input
// Best for: Unsorted inputs, medium to large datasets
// Performance: O(n + m) time, O(min(n, m)) space
class HashJoinOperator : public JoinOperator {
public:
    HashJoinOperator(std::shared_ptr<Operator> left,
                     std::shared_ptr<Operator> right,
                     const std::vector<std::string>& left_keys,
                     const std::vector<std::string>& right_keys,
                     JoinType join_type = JoinType::Inner)
        : JoinOperator(std::move(left), std::move(right),
                       left_keys, right_keys, join_type) {}

    arrow::Result<std::shared_ptr<arrow::RecordBatch>> GetNextBatch() override;
    bool HasNextBatch() const override;
    std::string ToString() const override;

private:
    // Build phase: Load smaller relation into hash table
    arrow::Status BuildHashTable();

    // Probe phase: Scan larger relation and produce matches
    arrow::Result<std::shared_ptr<arrow::RecordBatch>> ProbeNextBatch();

    // Hash table: join key -> list of row indices in build side
    HashMap<std::string, std::vector<int64_t>> hash_table_;

    // Build side data (materialized)
    std::shared_ptr<arrow::Table> build_table_;

    // Probe side iterator
    bool hash_table_built_ = false;
    std::shared_ptr<arrow::RecordBatch> current_probe_batch_;
    int64_t probe_row_idx_ = 0;

    // Output buffer
    std::vector<std::shared_ptr<arrow::RecordBatch>> output_buffer_;
    size_t output_buffer_idx_ = 0;
};

// Merge join: Assumes both inputs are sorted by join keys
// Best for: Sorted inputs (e.g., index scans), large datasets
// Performance: O(n + m) time, O(1) space
// Inspired by QLever's zipperJoinWithUndef
class MergeJoinOperator : public JoinOperator {
public:
    MergeJoinOperator(std::shared_ptr<Operator> left,
                      std::shared_ptr<Operator> right,
                      const std::vector<std::string>& left_keys,
                      const std::vector<std::string>& right_keys,
                      JoinType join_type = JoinType::Inner)
        : JoinOperator(std::move(left), std::move(right),
                       left_keys, right_keys, join_type) {}

    arrow::Result<std::shared_ptr<arrow::RecordBatch>> GetNextBatch() override;
    bool HasNextBatch() const override;
    std::string ToString() const override;

private:
    // Load next batches from both sides
    arrow::Status LoadNextBatches();

    // Perform merge step
    arrow::Result<std::shared_ptr<arrow::RecordBatch>> MergeStep();

    // Current batches
    std::shared_ptr<arrow::RecordBatch> left_batch_;
    std::shared_ptr<arrow::RecordBatch> right_batch_;

    // Current positions within batches
    int64_t left_idx_ = 0;
    int64_t right_idx_ = 0;

    bool left_exhausted_ = false;
    bool right_exhausted_ = false;
};

// Nested loop join: Simple but slow, used for small inputs or no join keys
// Best for: Very small inputs, cross products
// Performance: O(n * m) time, O(1) space
class NestedLoopJoinOperator : public JoinOperator {
public:
    NestedLoopJoinOperator(std::shared_ptr<Operator> left,
                           std::shared_ptr<Operator> right,
                           const std::vector<std::string>& left_keys,
                           const std::vector<std::string>& right_keys,
                           JoinType join_type = JoinType::Inner)
        : JoinOperator(std::move(left), std::move(right),
                       left_keys, right_keys, join_type) {}

    arrow::Result<std::shared_ptr<arrow::RecordBatch>> GetNextBatch() override;
    bool HasNextBatch() const override;
    std::string ToString() const override;

private:
    // Materialize right side
    arrow::Status MaterializeRightSide();

    std::shared_ptr<arrow::Table> right_table_;
    bool right_materialized_ = false;

    std::shared_ptr<arrow::RecordBatch> current_left_batch_;
    int64_t left_idx_ = 0;
    int64_t right_idx_ = 0;
};

// Factory function: Select best join algorithm based on inputs
std::shared_ptr<JoinOperator> CreateJoin(
    std::shared_ptr<Operator> left,
    std::shared_ptr<Operator> right,
    const std::vector<std::string>& left_keys,
    const std::vector<std::string>& right_keys,
    JoinType join_type = JoinType::Inner,
    JoinAlgorithm algorithm = JoinAlgorithm::Hash);

} // namespace sabot_ql
