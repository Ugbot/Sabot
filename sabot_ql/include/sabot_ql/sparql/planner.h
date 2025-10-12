#pragma once

#include <sabot_ql/sparql/ast.h>
#include <sabot_ql/operators/operator.h>
#include <sabot_ql/operators/aggregate.h>
#include <sabot_ql/storage/triple_store.h>
#include <sabot_ql/storage/vocabulary.h>
#include <arrow/result.h>
#include <memory>
#include <string>
#include <unordered_map>

namespace sabot_ql {
namespace sparql {

// Query planning context
struct PlanningContext {
    std::shared_ptr<TripleStore> store;
    std::shared_ptr<Vocabulary> vocab;

    // Variable mappings (SPARQL variable name -> column name in operator output)
    std::unordered_map<std::string, std::string> var_to_column;

    PlanningContext(std::shared_ptr<TripleStore> s, std::shared_ptr<Vocabulary> v)
        : store(std::move(s)), vocab(std::move(v)) {}
};

// Physical query plan
struct PhysicalPlan {
    std::shared_ptr<Operator> root_operator;
    std::vector<std::string> output_columns;  // Final output column names
    double estimated_cost;

    PhysicalPlan() : estimated_cost(0.0) {}

    std::string ToString() const {
        if (root_operator) {
            return root_operator->ToString();
        }
        return "Empty plan";
    }
};

// Query planner: Converts SPARQL AST to physical operator tree
// This is the main entry point for query planning
class QueryPlanner {
public:
    QueryPlanner(std::shared_ptr<TripleStore> store,
                 std::shared_ptr<Vocabulary> vocab)
        : store_(std::move(store)), vocab_(std::move(vocab)) {}

    // Plan a SELECT query
    arrow::Result<PhysicalPlan> PlanSelectQuery(const SelectQuery& query);

    // Plan a triple pattern (returns scan operator)
    arrow::Result<std::shared_ptr<Operator>> PlanTriplePattern(
        const TriplePattern& pattern,
        PlanningContext& ctx);

    // Plan a basic graph pattern (multiple triple patterns with joins)
    arrow::Result<std::shared_ptr<Operator>> PlanBasicGraphPattern(
        const BasicGraphPattern& bgp,
        PlanningContext& ctx);

    // Plan a FILTER clause
    arrow::Result<std::shared_ptr<Operator>> PlanFilter(
        std::shared_ptr<Operator> input,
        const FilterClause& filter,
        PlanningContext& ctx);

    // Plan an OPTIONAL clause
    arrow::Result<std::shared_ptr<Operator>> PlanOptional(
        std::shared_ptr<Operator> input,
        const OptionalPattern& optional,
        PlanningContext& ctx);

    // Plan a UNION clause
    arrow::Result<std::shared_ptr<Operator>> PlanUnion(
        const UnionPattern& union_pat,
        PlanningContext& ctx);

    // Plan ORDER BY
    arrow::Result<std::shared_ptr<Operator>> PlanOrderBy(
        std::shared_ptr<Operator> input,
        const std::vector<OrderBy>& order_by,
        PlanningContext& ctx);

    // Plan GROUP BY with aggregates
    arrow::Result<std::shared_ptr<Operator>> PlanGroupBy(
        std::shared_ptr<Operator> input,
        const GroupByClause& group_by,
        const std::vector<AggregateExpression>& aggregates,
        PlanningContext& ctx);

    // Plan aggregates without GROUP BY
    arrow::Result<std::shared_ptr<Operator>> PlanAggregateOnly(
        std::shared_ptr<Operator> input,
        const std::vector<AggregateExpression>& aggregates,
        PlanningContext& ctx);

    // Convert SPARQL RDFTerm to storage-level ValueId
    arrow::Result<std::optional<ValueId>> TermToValueId(
        const RDFTerm& term,
        PlanningContext& ctx);

private:
    std::shared_ptr<TripleStore> store_;
    std::shared_ptr<Vocabulary> vocab_;

    // Helper: Find join variables between two triple patterns
    std::vector<std::string> FindJoinVariables(
        const TriplePattern& left,
        const TriplePattern& right);

    // Helper: Convert SPARQL expression to Arrow filter predicate
    arrow::Result<FilterOperator::PredicateFn> ExpressionToPredicate(
        const Expression& expr,
        PlanningContext& ctx);

    // Helper: Get variable name for a triple pattern position
    std::string GetColumnNameForPosition(
        const TriplePattern& pattern,
        const std::string& position);  // "subject", "predicate", or "object"

    // Helper: Convert SPARQL ExprOperator to AggregateFunction
    arrow::Result<AggregateFunction> ExprOperatorToAggregateFunction(
        ExprOperator op) const;

    // Helper: Extract aggregate expressions from SelectClause
    std::vector<AggregateExpression> ExtractAggregates(
        const SelectClause& select) const;
};

// Simple query optimizer: Reorders joins for better performance
// This is a basic optimizer that uses heuristics
class QueryOptimizer {
public:
    QueryOptimizer(std::shared_ptr<TripleStore> store)
        : store_(std::move(store)) {}

    // Optimize a basic graph pattern
    // Returns reordered triple patterns for better join performance
    std::vector<TriplePattern> OptimizeBasicGraphPattern(
        const BasicGraphPattern& bgp);

    // Estimate cardinality for a triple pattern
    arrow::Result<size_t> EstimateCardinality(
        const TriplePattern& pattern,
        std::shared_ptr<Vocabulary> vocab);

    // Select best join order for a set of triple patterns
    // Uses greedy algorithm: start with smallest cardinality
    std::vector<size_t> SelectJoinOrder(
        const std::vector<TriplePattern>& patterns,
        std::shared_ptr<Vocabulary> vocab);

private:
    std::shared_ptr<TripleStore> store_;

    // Cost model for joins
    double EstimateJoinCost(
        const TriplePattern& left,
        const TriplePattern& right,
        std::shared_ptr<Vocabulary> vocab);
};

// Helper functions for planning
namespace planning {

// Convert SPARQL Variable to column name
std::string VariableToColumnName(const Variable& var);

// Check if two triple patterns have join variables
bool HasJoinVariables(const TriplePattern& left, const TriplePattern& right);

// Get all variables in a triple pattern
std::vector<std::string> GetVariables(const TriplePattern& pattern);

// Get all variables in a basic graph pattern
std::vector<std::string> GetVariables(const BasicGraphPattern& bgp);

} // namespace planning

} // namespace sparql
} // namespace sabot_ql
