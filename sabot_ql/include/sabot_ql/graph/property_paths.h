/**
 * Property Path Operators
 *
 * SPARQL 1.1 property path operators implemented as Arrow kernels.
 */

#pragma once

#include <arrow/api.h>
#include <arrow/compute/api.h>

namespace sabot {
namespace graph {

/**
 * Sequence Path (p/q): Follows path p then path q
 *
 * Algorithm: Hash join on path_p.end = path_q.start
 * Returns: RecordBatch with columns (start, end) where
 *   - start is from path_p
 *   - end is from path_q
 *   - Connected via intermediate node
 *
 * Example:
 *   path_p: (0, 1), (0, 2)  # 0 reaches 1 and 2
 *   path_q: (1, 3), (2, 4)  # 1 reaches 3, 2 reaches 4
 *   Result: (0, 3), (0, 4)  # 0 reaches 3 and 4 via intermediate nodes
 *
 * Complexity: O(|p| + |q|) with hash join
 */
arrow::Result<std::shared_ptr<arrow::RecordBatch>> SequencePath(
    const std::shared_ptr<arrow::RecordBatch>& path_p,
    const std::shared_ptr<arrow::RecordBatch>& path_q,
    arrow::MemoryPool* pool = arrow::default_memory_pool()
);

/**
 * Alternative Path (p|q): Union of two paths with deduplication
 *
 * Algorithm: Concatenate paths and deduplicate (start, end) pairs
 * Returns: RecordBatch with columns (start, end) containing all unique paths
 *
 * Example:
 *   path_p: (0, 1), (0, 2)
 *   path_q: (0, 2), (0, 3)  # Note: (0, 2) is duplicate
 *   Result: (0, 1), (0, 2), (0, 3)  # Deduplicated
 *
 * Complexity: O(|p| + |q|) with hash-based deduplication
 */
arrow::Result<std::shared_ptr<arrow::RecordBatch>> AlternativePath(
    const std::shared_ptr<arrow::RecordBatch>& path_p,
    const std::shared_ptr<arrow::RecordBatch>& path_q,
    arrow::MemoryPool* pool = arrow::default_memory_pool()
);

/**
 * Negated Property Set (!p): Filter edges excluding specific predicates
 *
 * Algorithm: Arrow filter where predicate NOT IN excluded_predicates
 * Returns: RecordBatch with rows where predicate is not in exclusion list
 *
 * Example:
 *   edges: (0, 10, 1), (0, 20, 2), (0, 30, 3)  # (start, predicate, end)
 *   excluded_predicates: [20]
 *   Result: (0, 10, 1), (0, 30, 3)  # Excludes predicate 20
 *
 * Complexity: O(E) single pass filter
 */
arrow::Result<std::shared_ptr<arrow::RecordBatch>> FilterByPredicate(
    const std::shared_ptr<arrow::RecordBatch>& edges,
    const std::vector<int64_t>& excluded_predicates,
    const std::string& predicate_col = "predicate",
    arrow::MemoryPool* pool = arrow::default_memory_pool()
);

}  // namespace graph
}  // namespace sabot
