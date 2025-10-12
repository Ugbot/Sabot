#include <sabot_ql/sparql/query_engine.h>
#include <sstream>

namespace sabot_ql {
namespace sparql {

// QueryEngine implementation
arrow::Result<std::shared_ptr<arrow::Table>> QueryEngine::ExecuteSelect(
    const SelectQuery& query) {

    // 1. Plan the query
    ARROW_ASSIGN_OR_RAISE(auto plan, planner_.PlanSelectQuery(query));

    // 2. Execute the plan
    return executor_.Execute(plan.root_operator);
}

std::string QueryEngine::Explain(const SelectQuery& query) {
    auto plan_result = planner_.PlanSelectQuery(query);

    if (!plan_result.ok()) {
        return "Failed to plan query: " + plan_result.status().ToString();
    }

    auto plan = plan_result.ValueOrDie();

    std::ostringstream oss;
    oss << "Physical Plan:\n";
    oss << plan.ToString() << "\n";
    oss << "\nEstimated cost: " << plan.estimated_cost << "\n";
    oss << "Estimated cardinality: " << plan.root_operator->EstimateCardinality() << " rows";

    return oss.str();
}

arrow::Result<std::string> QueryEngine::ExplainAnalyze(const SelectQuery& query) {
    // Execute the query to collect statistics
    ARROW_ASSIGN_OR_RAISE(auto result, ExecuteSelect(query));

    std::ostringstream oss;
    oss << "Physical Plan with Execution Statistics:\n";
    oss << "\n" << executor_.GetStats().ToString() << "\n";
    oss << "Result: " << result->num_rows() << " rows, "
        << result->num_columns() << " columns";

    return oss.str();
}

// SPARQLBuilder implementation
SPARQLBuilder& SPARQLBuilder::Select(const std::vector<std::string>& variables) {
    query_.select.distinct = false;
    query_.select.items.clear();

    for (const auto& var : variables) {
        query_.select.items.push_back(Variable(var));
    }

    return *this;
}

SPARQLBuilder& SPARQLBuilder::SelectAll() {
    query_.select.distinct = false;
    query_.select.items.clear();
    return *this;
}

SPARQLBuilder& SPARQLBuilder::SelectDistinct(const std::vector<std::string>& variables) {
    query_.select.distinct = true;
    query_.select.items.clear();

    for (const auto& var : variables) {
        query_.select.items.push_back(Variable(var));
    }

    return *this;
}

SPARQLBuilder& SPARQLBuilder::Where() {
    current_pattern_ = &query_.where;

    // Initialize BGP if not already present
    if (!current_pattern_->bgp.has_value()) {
        current_pattern_->bgp = BasicGraphPattern();
    }

    return *this;
}

SPARQLBuilder& SPARQLBuilder::Triple(const RDFTerm& subject,
                                     const RDFTerm& predicate,
                                     const RDFTerm& object) {
    if (!current_pattern_) {
        throw std::runtime_error("Cannot add triple: WHERE clause not started");
    }

    if (!current_pattern_->bgp.has_value()) {
        current_pattern_->bgp = BasicGraphPattern();
    }

    current_pattern_->bgp->triples.emplace_back(subject, predicate, object);

    return *this;
}

SPARQLBuilder& SPARQLBuilder::Filter(std::shared_ptr<Expression> expr) {
    if (!current_pattern_) {
        throw std::runtime_error("Cannot add filter: WHERE clause not started");
    }

    current_pattern_->filters.emplace_back(expr);

    return *this;
}

SPARQLBuilder& SPARQLBuilder::Optional(std::shared_ptr<QueryPattern> pattern) {
    if (!current_pattern_) {
        throw std::runtime_error("Cannot add optional: WHERE clause not started");
    }

    current_pattern_->optionals.emplace_back(pattern);

    return *this;
}

SPARQLBuilder& SPARQLBuilder::EndWhere() {
    current_pattern_ = nullptr;
    return *this;
}

SPARQLBuilder& SPARQLBuilder::OrderBy(const std::string& variable,
                                      OrderDirection direction) {
    query_.order_by.emplace_back(Variable(variable), direction);
    return *this;
}

SPARQLBuilder& SPARQLBuilder::Limit(size_t limit) {
    query_.limit = limit;
    return *this;
}

SPARQLBuilder& SPARQLBuilder::Offset(size_t offset) {
    query_.offset = offset;
    return *this;
}

SelectQuery SPARQLBuilder::Build() const {
    return query_;
}

} // namespace sparql
} // namespace sabot_ql
