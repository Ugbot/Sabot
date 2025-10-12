#pragma once

#include <sabot_ql/operators/operator.h>
#include <sabot_ql/util/hash_map.h>
#include <arrow/compute/api.h>
#include <memory>
#include <string>
#include <vector>
#include <functional>

namespace sabot_ql {

// Aggregate function types (SPARQL 1.1 aggregates)
enum class AggregateFunction {
    Count,      // COUNT(?x)
    Sum,        // SUM(?x)
    Avg,        // AVG(?x)
    Min,        // MIN(?x)
    Max,        // MAX(?x)
    GroupConcat,// GROUP_CONCAT(?x)
    Sample      // SAMPLE(?x) - arbitrary value from group
};

// Aggregate specification
struct AggregateSpec {
    AggregateFunction function;
    std::string input_column;   // Column to aggregate
    std::string output_column;  // Output column name
    bool distinct = false;      // COUNT(DISTINCT ?x)

    // For GROUP_CONCAT
    std::string separator = " ";

    AggregateSpec(AggregateFunction func,
                  const std::string& input,
                  const std::string& output,
                  bool distinct_flag = false)
        : function(func),
          input_column(input),
          output_column(output),
          distinct(distinct_flag) {}
};

// GroupBy operator: Groups rows by keys and computes aggregates
// Implements SPARQL GROUP BY with aggregate functions
//
// Example SPARQL:
//   SELECT ?person (COUNT(?item) AS ?count) WHERE {
//       ?person hasItem ?item
//   } GROUP BY ?person
//
// Becomes:
//   GroupByOperator(
//       input = TripleScan(...),
//       group_keys = {"person"},
//       aggregates = {
//           AggregateSpec(Count, "item", "count")
//       }
//   )
class GroupByOperator : public UnaryOperator {
public:
    GroupByOperator(std::shared_ptr<Operator> input,
                    const std::vector<std::string>& group_keys,
                    const std::vector<AggregateSpec>& aggregates)
        : UnaryOperator(std::move(input)),
          group_keys_(group_keys),
          aggregates_(aggregates) {

        if (aggregates.empty()) {
            throw std::invalid_argument("At least one aggregate function required");
        }
    }

    arrow::Result<std::shared_ptr<arrow::Schema>> GetOutputSchema() const override;
    arrow::Result<std::shared_ptr<arrow::RecordBatch>> GetNextBatch() override;
    bool HasNextBatch() const override;
    std::string ToString() const override;
    size_t EstimateCardinality() const override;

private:
    // Compute aggregates using Arrow compute kernels
    arrow::Status ComputeAggregates();

    // Build group key from row
    std::string BuildGroupKey(const std::shared_ptr<arrow::RecordBatch>& batch,
                              int64_t row_idx,
                              const std::vector<int>& key_indices) const;

    std::vector<std::string> group_keys_;
    std::vector<AggregateSpec> aggregates_;

    // Group hash table: group key -> list of row indices
    HashMap<std::string, std::vector<int64_t>> groups_;

    // Materialized input (for aggregation)
    std::shared_ptr<arrow::Table> input_table_;

    // Result table (computed once)
    std::shared_ptr<arrow::Table> result_table_;

    bool computed_ = false;
    bool exhausted_ = false;
};

// Aggregate without grouping: compute aggregates over entire input
// Example SPARQL:
//   SELECT (COUNT(?person) AS ?count) WHERE {
//       ?person a Person
//   }
//
// This is equivalent to GROUP BY with empty group keys
class AggregateOperator : public UnaryOperator {
public:
    AggregateOperator(std::shared_ptr<Operator> input,
                      const std::vector<AggregateSpec>& aggregates)
        : UnaryOperator(std::move(input)),
          aggregates_(aggregates) {

        if (aggregates.empty()) {
            throw std::invalid_argument("At least one aggregate function required");
        }
    }

    arrow::Result<std::shared_ptr<arrow::Schema>> GetOutputSchema() const override;
    arrow::Result<std::shared_ptr<arrow::RecordBatch>> GetNextBatch() override;
    bool HasNextBatch() const override;
    std::string ToString() const override;
    size_t EstimateCardinality() const override { return 1; }  // Always returns 1 row

private:
    arrow::Status ComputeAggregates();

    std::vector<AggregateSpec> aggregates_;

    std::shared_ptr<arrow::Table> input_table_;
    std::shared_ptr<arrow::RecordBatch> result_batch_;

    bool computed_ = false;
    bool exhausted_ = false;
};

// Helper functions for computing aggregates using Arrow compute kernels
namespace aggregate_helpers {

// Compute COUNT aggregate
arrow::Result<std::shared_ptr<arrow::Scalar>> ComputeCount(
    const std::shared_ptr<arrow::Array>& array,
    bool count_nulls = true);

// Compute SUM aggregate
arrow::Result<std::shared_ptr<arrow::Scalar>> ComputeSum(
    const std::shared_ptr<arrow::Array>& array);

// Compute AVG aggregate
arrow::Result<std::shared_ptr<arrow::Scalar>> ComputeAvg(
    const std::shared_ptr<arrow::Array>& array);

// Compute MIN aggregate
arrow::Result<std::shared_ptr<arrow::Scalar>> ComputeMin(
    const std::shared_ptr<arrow::Array>& array);

// Compute MAX aggregate
arrow::Result<std::shared_ptr<arrow::Scalar>> ComputeMax(
    const std::shared_ptr<arrow::Array>& array);

// Compute GROUP_CONCAT aggregate
arrow::Result<std::shared_ptr<arrow::Scalar>> ComputeGroupConcat(
    const std::shared_ptr<arrow::Array>& array,
    const std::string& separator);

// Compute SAMPLE aggregate (arbitrary value)
arrow::Result<std::shared_ptr<arrow::Scalar>> ComputeSample(
    const std::shared_ptr<arrow::Array>& array);

} // namespace aggregate_helpers

} // namespace sabot_ql
