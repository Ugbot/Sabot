#pragma once

#include <arrow/api.h>
#include <arrow/result.h>
#include <memory>
#include <cstdint>
#include <string>
#include <vector>

namespace sabot {
namespace graph {
namespace query {

/**
 * Pattern matching for graph queries.
 *
 * Implements efficient pattern matching using hash joins on Arrow tables.
 * Supports:
 * - 2-hop patterns: (A)-[R1]->(B)-[R2]->(C)
 * - 3-hop patterns: (A)-[R1]->(B)-[R2]->(C)-[R3]->(D)
 * - Variable-length paths: (A)-[R*min..max]->(B)
 * - Property predicates on vertices and edges
 */

/**
 * Result of a pattern match query.
 */
struct PatternMatchResult {
    std::shared_ptr<arrow::Table> result_table;  // Result with all matched patterns
    int64_t num_matches;                          // Total number of matches
    std::vector<std::string> binding_names;       // Names of bound vertices
};

/**
 * 2-hop pattern: (A)-[R1]->(B)-[R2]->(C)
 *
 * Finds all paths A->B->C where:
 * - R1 is an edge from A to B
 * - R2 is an edge from B to C
 *
 * Implementation:
 * 1. Hash join edges on target_id = source_id
 * 2. Filter by optional predicates
 * 3. Return table with columns: [a_id, r1_id, b_id, r2_id, c_id]
 *
 * Time complexity: O(E1 + E2) where E1, E2 are edge counts
 * Space complexity: O(M) where M is number of matches
 *
 * @param edges1 First edge table (must have source, target columns)
 * @param edges2 Second edge table (must have source, target columns)
 * @param source_name Name for source vertex binding (default: "a")
 * @param intermediate_name Name for intermediate vertex binding (default: "b")
 * @param target_name Name for target vertex binding (default: "c")
 * @return PatternMatchResult with matched patterns
 */
arrow::Result<PatternMatchResult> Match2Hop(
    const std::shared_ptr<arrow::Table>& edges1,
    const std::shared_ptr<arrow::Table>& edges2,
    const std::string& source_name = "a",
    const std::string& intermediate_name = "b",
    const std::string& target_name = "c"
);

/**
 * 2-hop pattern with edge type filtering.
 *
 * Same as Match2Hop but filters edges by type before joining.
 *
 * @param edges Edge table with type column
 * @param type1 Edge type for R1 (nullptr = any type)
 * @param type2 Edge type for R2 (nullptr = any type)
 * @param source_name Name for source vertex
 * @param intermediate_name Name for intermediate vertex
 * @param target_name Name for target vertex
 * @return PatternMatchResult with matched patterns
 */
arrow::Result<PatternMatchResult> Match2HopWithTypes(
    const std::shared_ptr<arrow::Table>& edges,
    const std::string* type1,
    const std::string* type2,
    const std::string& source_name = "a",
    const std::string& intermediate_name = "b",
    const std::string& target_name = "c"
);

/**
 * 3-hop pattern: (A)-[R1]->(B)-[R2]->(C)-[R3]->(D)
 *
 * Finds all paths A->B->C->D.
 *
 * Implementation:
 * 1. Hash join edges1 and edges2 on target_id = source_id
 * 2. Hash join result with edges3
 * 3. Return table with columns: [a_id, r1_id, b_id, r2_id, c_id, r3_id, d_id]
 *
 * @param edges1 First edge table
 * @param edges2 Second edge table
 * @param edges3 Third edge table
 * @return PatternMatchResult with matched patterns
 */
arrow::Result<PatternMatchResult> Match3Hop(
    const std::shared_ptr<arrow::Table>& edges1,
    const std::shared_ptr<arrow::Table>& edges2,
    const std::shared_ptr<arrow::Table>& edges3
);

/**
 * Variable-length path: (A)-[R*min..max]->(B)
 *
 * Finds all paths from A to B with length between min and max hops.
 *
 * Implementation:
 * 1. For k = min to max:
 *    - Compute k-hop reachability using repeated hash joins
 *    - Collect intermediate paths
 * 2. Deduplicate and return
 *
 * Note: This can be expensive for large max values. Consider caching.
 *
 * @param edges Edge table
 * @param source_vertex Source vertex ID
 * @param target_vertex Target vertex ID (nullptr = all reachable)
 * @param min_hops Minimum path length (inclusive)
 * @param max_hops Maximum path length (inclusive)
 * @return PatternMatchResult with all paths
 */
arrow::Result<PatternMatchResult> MatchVariableLengthPath(
    const std::shared_ptr<arrow::Table>& edges,
    int64_t source_vertex,
    const int64_t* target_vertex,
    int64_t min_hops,
    int64_t max_hops
);

/**
 * Pattern with vertex property predicates.
 *
 * Filters matched patterns by vertex properties.
 *
 * Example: Find patterns where A.age > 30 and C.city = "NYC"
 *
 * @param pattern_result Input pattern match result
 * @param vertices Vertex table with properties
 * @param predicates Map of binding_name -> predicate expression
 * @return Filtered PatternMatchResult
 */
arrow::Result<PatternMatchResult> ApplyVertexPredicates(
    const PatternMatchResult& pattern_result,
    const std::shared_ptr<arrow::Table>& vertices,
    const std::vector<std::pair<std::string, std::string>>& predicates
);

/**
 * Pattern with edge property predicates.
 *
 * Filters matched patterns by edge properties.
 *
 * Example: Find patterns where R1.weight > 0.5
 *
 * @param pattern_result Input pattern match result
 * @param edges Edge table with properties
 * @param predicates Map of edge_name -> predicate expression
 * @return Filtered PatternMatchResult
 */
arrow::Result<PatternMatchResult> ApplyEdgePredicates(
    const PatternMatchResult& pattern_result,
    const std::shared_ptr<arrow::Table>& edges,
    const std::vector<std::pair<std::string, std::string>>& predicates
);

/**
 * Optimize pattern join order.
 *
 * Reorders joins to minimize intermediate result sizes.
 * Uses edge cardinality statistics to estimate costs.
 *
 * @param edges Vector of edge tables
 * @return Optimized join order (indices into edges vector)
 */
std::vector<int> OptimizeJoinOrder(
    const std::vector<std::shared_ptr<arrow::Table>>& edges
);

/**
 * Triangle pattern: (A)-[R1]->(B)-[R2]->(C)-[R3]->(A)
 *
 * Specialized pattern matcher for triangles (3-cycles).
 * More efficient than general 3-hop matcher.
 *
 * @param edges Edge table
 * @return PatternMatchResult with all triangles
 */
arrow::Result<PatternMatchResult> MatchTriangle(
    const std::shared_ptr<arrow::Table>& edges
);

/**
 * Diamond pattern: (A)-[R1]->(B), (A)-[R2]->(C), (B)-[R3]->(D), (C)-[R4]->(D)
 *
 * Specialized pattern matcher for diamond shapes.
 *
 * @param edges Edge table
 * @return PatternMatchResult with all diamonds
 */
arrow::Result<PatternMatchResult> MatchDiamond(
    const std::shared_ptr<arrow::Table>& edges
);

}  // namespace query
}  // namespace graph
}  // namespace sabot
