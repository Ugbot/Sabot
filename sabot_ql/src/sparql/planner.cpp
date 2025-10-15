#include <sabot_ql/sparql/planner.h>
#include <sabot_ql/sparql/expression_evaluator.h>
#include <sabot_ql/operators/join.h>
#include <sabot_ql/operators/aggregate.h>
#include <sabot_ql/operators/sort.h>
#include <sabot_ql/operators/union.h>
#include <sstream>
#include <algorithm>

namespace sabot_ql {
namespace sparql {

// Helper functions
namespace planning {

std::string VariableToColumnName(const Variable& var) {
    return var.name;  // For now, just use the variable name without '?'
}

bool HasJoinVariables(const TriplePattern& left, const TriplePattern& right) {
    auto left_vars = GetVariables(left);
    auto right_vars = GetVariables(right);

    for (const auto& lv : left_vars) {
        for (const auto& rv : right_vars) {
            if (lv == rv) {
                return true;
            }
        }
    }

    return false;
}

std::vector<std::string> GetVariables(const TriplePattern& pattern) {
    std::vector<std::string> vars;

    if (auto s = pattern.GetSubjectVar()) {
        vars.push_back(*s);
    }
    if (auto p = pattern.GetPredicateVar()) {
        vars.push_back(*p);
    }
    if (auto o = pattern.GetObjectVar()) {
        vars.push_back(*o);
    }

    return vars;
}

std::vector<std::string> GetVariables(const BasicGraphPattern& bgp) {
    std::vector<std::string> vars;

    for (const auto& triple : bgp.triples) {
        auto triple_vars = GetVariables(triple);
        vars.insert(vars.end(), triple_vars.begin(), triple_vars.end());
    }

    // Remove duplicates
    std::sort(vars.begin(), vars.end());
    vars.erase(std::unique(vars.begin(), vars.end()), vars.end());

    return vars;
}

} // namespace planning

// QueryPlanner implementation
arrow::Result<PhysicalPlan> QueryPlanner::PlanSelectQuery(const SelectQuery& query) {
    PhysicalPlan plan;
    PlanningContext ctx(store_, vocab_);

    // 1. Plan WHERE clause (basic graph pattern)
    if (!query.where.bgp.has_value()) {
        return arrow::Status::Invalid("SELECT query must have WHERE clause with basic graph pattern");
    }

    ARROW_ASSIGN_OR_RAISE(
        plan.root_operator,
        PlanBasicGraphPattern(*query.where.bgp, ctx)
    );

    // 2. Plan FILTER clauses
    for (const auto& filter : query.where.filters) {
        ARROW_ASSIGN_OR_RAISE(
            plan.root_operator,
            PlanFilter(plan.root_operator, filter, ctx)
        );
    }

    // 3. Plan OPTIONAL clauses
    for (const auto& optional : query.where.optionals) {
        ARROW_ASSIGN_OR_RAISE(
            plan.root_operator,
            PlanOptional(plan.root_operator, optional, ctx)
        );
    }

    // 4. Plan UNION clauses
    for (const auto& union_pat : query.where.unions) {
        ARROW_ASSIGN_OR_RAISE(
            plan.root_operator,
            PlanUnion(union_pat, ctx)
        );
    }

    // 5. Plan GROUP BY and aggregates
    if (query.HasAggregates()) {
        // Extract aggregate expressions from SELECT clause
        auto aggregates = ExtractAggregates(query.select);

        if (query.HasGroupBy()) {
            // GROUP BY with aggregates
            ARROW_ASSIGN_OR_RAISE(
                plan.root_operator,
                PlanGroupBy(plan.root_operator, *query.group_by, aggregates, ctx)
            );
        } else {
            // Aggregates without GROUP BY (aggregate over entire input)
            ARROW_ASSIGN_OR_RAISE(
                plan.root_operator,
                PlanAggregateOnly(plan.root_operator, aggregates, ctx)
            );
        }
    } else if (query.HasGroupBy()) {
        // GROUP BY without aggregates - this is technically invalid SPARQL
        // but we'll handle it by treating it as SELECT DISTINCT on the group keys
        std::vector<std::string> group_cols;
        for (const auto& var : query.group_by->variables) {
            group_cols.push_back(planning::VariableToColumnName(var));
        }
        plan.root_operator = std::make_shared<ProjectOperator>(
            plan.root_operator,
            group_cols
        );
        plan.root_operator = std::make_shared<DistinctOperator>(plan.root_operator);
    }

    // 6. Plan ORDER BY
    if (!query.order_by.empty()) {
        ARROW_ASSIGN_OR_RAISE(
            plan.root_operator,
            PlanOrderBy(plan.root_operator, query.order_by, ctx)
        );
    }

    // 7. Plan projection (SELECT clause) - only if not aggregating
    if (!query.HasAggregates() && !query.HasGroupBy() && !query.select.IsSelectAll()) {
        std::vector<std::string> select_cols;
        for (const auto& item : query.select.items) {
            if (auto* var = std::get_if<Variable>(&item)) {
                select_cols.push_back(planning::VariableToColumnName(*var));
            }
        }

        plan.root_operator = std::make_shared<ProjectOperator>(
            plan.root_operator,
            select_cols
        );
    }

    // 8. Plan DISTINCT (only if not already handled by GROUP BY)
    if (query.select.distinct && !query.HasGroupBy()) {
        plan.root_operator = std::make_shared<DistinctOperator>(plan.root_operator);
    }

    // 9. Plan LIMIT
    if (query.limit.has_value()) {
        plan.root_operator = std::make_shared<LimitOperator>(
            plan.root_operator,
            *query.limit
        );
    }

    // 9. Estimate cost
    plan.estimated_cost = plan.root_operator->EstimateCardinality();

    return plan;
}

arrow::Result<std::shared_ptr<Operator>> QueryPlanner::PlanTriplePattern(
    const TriplePattern& pattern,
    PlanningContext& ctx) {

    // Convert SPARQL triple pattern to storage triple pattern
    sabot_ql::TriplePattern storage_pattern;

    // Convert subject
    ARROW_ASSIGN_OR_RAISE(auto subject_vid, TermToValueId(pattern.subject, ctx));
    storage_pattern.subject = subject_vid.has_value() ?
        std::optional<uint64_t>(subject_vid->getBits()) : std::nullopt;

    // Convert predicate
    ARROW_ASSIGN_OR_RAISE(auto predicate_vid, TermToValueId(pattern.predicate, ctx));
    storage_pattern.predicate = predicate_vid.has_value() ?
        std::optional<uint64_t>(predicate_vid->getBits()) : std::nullopt;

    // Convert object
    ARROW_ASSIGN_OR_RAISE(auto object_vid, TermToValueId(pattern.object, ctx));
    storage_pattern.object = object_vid.has_value() ?
        std::optional<uint64_t>(object_vid->getBits()) : std::nullopt;

    // Create scan operator
    auto scan_op = std::make_shared<TripleScanOperator>(
        store_,
        vocab_,
        storage_pattern
    );

    // Update variable-to-column mappings
    if (auto var = pattern.GetSubjectVar()) {
        ctx.var_to_column[*var] = "subject";
    }
    if (auto var = pattern.GetPredicateVar()) {
        ctx.var_to_column[*var] = "predicate";
    }
    if (auto var = pattern.GetObjectVar()) {
        ctx.var_to_column[*var] = "object";
    }

    return scan_op;
}

arrow::Result<std::shared_ptr<Operator>> QueryPlanner::PlanBasicGraphPattern(
    const BasicGraphPattern& bgp,
    PlanningContext& ctx) {

    if (bgp.triples.empty()) {
        return arrow::Status::Invalid("Empty basic graph pattern");
    }

    // Use optimizer to reorder triple patterns
    QueryOptimizer optimizer(store_);
    auto optimized_triples = optimizer.OptimizeBasicGraphPattern(bgp);

    // Plan first triple pattern
    ARROW_ASSIGN_OR_RAISE(
        auto current_op,
        PlanTriplePattern(optimized_triples[0], ctx)
    );

    // Join remaining triple patterns
    for (size_t i = 1; i < optimized_triples.size(); ++i) {
        const auto& pattern = optimized_triples[i];

        // Plan this triple pattern
        ARROW_ASSIGN_OR_RAISE(
            auto right_op,
            PlanTriplePattern(pattern, ctx)
        );

        // Find join variables
        auto join_vars = FindJoinVariables(optimized_triples[i-1], pattern);

        if (join_vars.empty()) {
            // Cross product (no join variables)
            // Use nested loop join or hash join with no keys
            // For now, just use hash join with dummy keys
            // TODO: Implement proper cross product operator
            return arrow::Status::NotImplemented("Cross product not yet supported");
        }

        // Build join keys
        std::vector<std::string> left_keys;
        std::vector<std::string> right_keys;

        for (const auto& var : join_vars) {
            // Map variable to column name
            auto it = ctx.var_to_column.find(var);
            if (it != ctx.var_to_column.end()) {
                left_keys.push_back(it->second);
                right_keys.push_back(it->second);
            }
        }

        // Create join operator
        current_op = CreateJoin(
            current_op,
            right_op,
            left_keys,
            right_keys,
            JoinType::Inner,
            JoinAlgorithm::Hash
        );
    }

    return current_op;
}

arrow::Result<std::shared_ptr<Operator>> QueryPlanner::PlanFilter(
    std::shared_ptr<Operator> input,
    const FilterClause& filter,
    PlanningContext& ctx) {

    // Convert SPARQL expression to Arrow predicate
    ARROW_ASSIGN_OR_RAISE(
        auto predicate,
        ExpressionToPredicate(*filter.expr, ctx)
    );

    // Get human-readable description
    std::string description = GetExpressionDescription(filter.expr);

    return std::make_shared<FilterOperator>(
        input,
        predicate,
        description
    );
}

arrow::Result<std::shared_ptr<Operator>> QueryPlanner::PlanOptional(
    std::shared_ptr<Operator> input,
    const OptionalPattern& optional,
    PlanningContext& ctx) {

    // Plan the optional pattern
    if (!optional.pattern->bgp.has_value()) {
        return arrow::Status::Invalid("OPTIONAL must contain basic graph pattern");
    }

    ARROW_ASSIGN_OR_RAISE(
        auto optional_op,
        PlanBasicGraphPattern(*optional.pattern->bgp, ctx)
    );

    // Apply FILTER clauses to optional pattern
    for (const auto& filter : optional.pattern->filters) {
        ARROW_ASSIGN_OR_RAISE(
            optional_op,
            PlanFilter(optional_op, filter, ctx)
        );
    }

    // Find join variables between input and optional pattern
    // Extract variables from input schema
    ARROW_ASSIGN_OR_RAISE(auto input_schema, input->GetOutputSchema());
    ARROW_ASSIGN_OR_RAISE(auto optional_schema, optional_op->GetOutputSchema());

    std::vector<std::string> join_vars;
    for (int i = 0; i < input_schema->num_fields(); ++i) {
        std::string field_name = input_schema->field(i)->name();
        if (optional_schema->GetFieldIndex(field_name) >= 0) {
            join_vars.push_back(field_name);
        }
    }

    if (join_vars.empty()) {
        // No join variables - this is a cross product with LEFT OUTER JOIN semantics
        // For simplicity, we'll use nested loop join with empty keys
        // This means every input row is combined with every optional row (or kept with NULLs if optional is empty)
        return std::make_shared<HashJoinOperator>(
            input,
            optional_op,
            std::vector<std::string>{},  // Empty keys = cross product
            std::vector<std::string>{},
            JoinType::LeftOuter
        );
    }

    // Create LEFT OUTER JOIN with the discovered join variables
    return std::make_shared<HashJoinOperator>(
        input,
        optional_op,
        join_vars,  // left keys
        join_vars,  // right keys (same variable names)
        JoinType::LeftOuter
    );
}

arrow::Result<std::shared_ptr<Operator>> QueryPlanner::PlanUnion(
    const UnionPattern& union_pat,
    PlanningContext& ctx) {

    if (union_pat.patterns.empty()) {
        return arrow::Status::Invalid("UNION must have at least one pattern");
    }

    // Plan each branch of the UNION
    std::vector<std::shared_ptr<Operator>> union_inputs;

    for (const auto& pattern : union_pat.patterns) {
        std::shared_ptr<Operator> pattern_op;

        // 1. Plan BGP if present
        if (pattern->bgp.has_value()) {
            ARROW_ASSIGN_OR_RAISE(
                pattern_op,
                PlanBasicGraphPattern(*pattern->bgp, ctx)
            );
        } else {
            // Empty pattern - skip
            continue;
        }

        // 2. Apply FILTER clauses
        for (const auto& filter : pattern->filters) {
            ARROW_ASSIGN_OR_RAISE(
                pattern_op,
                PlanFilter(pattern_op, filter, ctx)
            );
        }

        // 3. Apply OPTIONAL clauses
        for (const auto& optional : pattern->optionals) {
            ARROW_ASSIGN_OR_RAISE(
                pattern_op,
                PlanOptional(pattern_op, optional, ctx)
            );
        }

        // 4. Recursively handle nested UNIONs
        for (const auto& nested_union : pattern->unions) {
            ARROW_ASSIGN_OR_RAISE(
                pattern_op,
                PlanUnion(nested_union, ctx)
            );
        }

        union_inputs.push_back(pattern_op);
    }

    if (union_inputs.empty()) {
        return arrow::Status::Invalid("UNION has no valid patterns");
    }

    if (union_inputs.size() == 1) {
        // Only one branch - no union needed
        return union_inputs[0];
    }

    // Create UNION operator (with deduplication by default)
    return std::make_shared<UnionOperator>(union_inputs, true /* deduplicate */);
}

arrow::Result<std::shared_ptr<Operator>> QueryPlanner::PlanOrderBy(
    std::shared_ptr<Operator> input,
    const std::vector<OrderBy>& order_by,
    PlanningContext& ctx) {

    if (order_by.empty()) {
        return input;  // No sorting needed
    }

    // Convert SPARQL OrderBy to SortKeys
    std::vector<SortKey> sort_keys;

    for (const auto& order : order_by) {
        // Map SPARQL variable to column name
        std::string column_name = planning::VariableToColumnName(order.var);

        // Convert SPARQL OrderDirection to SortDirection
        SortDirection direction = (order.direction == OrderDirection::Ascending)
                                      ? SortDirection::Ascending
                                      : SortDirection::Descending;

        sort_keys.emplace_back(column_name, direction);
    }

    // Create SortOperator
    return std::make_shared<SortOperator>(input, sort_keys);
}

arrow::Result<std::optional<ValueId>> QueryPlanner::TermToValueId(
    const RDFTerm& term,
    PlanningContext& ctx) {

    if (std::holds_alternative<Variable>(term)) {
        // Variables are unbound - return nullopt
        return std::nullopt;
    }

    if (auto* iri = std::get_if<IRI>(&term)) {
        Term storage_term = Term::IRI(iri->iri);
        ARROW_ASSIGN_OR_RAISE(auto maybe_id, ctx.vocab->GetValueId(storage_term));
        return maybe_id;
    }

    if (auto* literal = std::get_if<Literal>(&term)) {
        Term storage_term = Term::Literal(
            literal->value,
            literal->language,
            literal->datatype
        );
        ARROW_ASSIGN_OR_RAISE(auto maybe_id, ctx.vocab->GetValueId(storage_term));
        return maybe_id;
    }

    if (auto* blank = std::get_if<BlankNode>(&term)) {
        Term storage_term = Term::BlankNode(blank->id);
        ARROW_ASSIGN_OR_RAISE(auto maybe_id, ctx.vocab->GetValueId(storage_term));
        return maybe_id;
    }

    return arrow::Status::Invalid("Unknown term type");
}

std::vector<std::string> QueryPlanner::FindJoinVariables(
    const TriplePattern& left,
    const TriplePattern& right) {

    auto left_vars = planning::GetVariables(left);
    auto right_vars = planning::GetVariables(right);

    std::vector<std::string> join_vars;

    for (const auto& lv : left_vars) {
        for (const auto& rv : right_vars) {
            if (lv == rv) {
                join_vars.push_back(lv);
            }
        }
    }

    // Remove duplicates
    std::sort(join_vars.begin(), join_vars.end());
    join_vars.erase(std::unique(join_vars.begin(), join_vars.end()), join_vars.end());

    return join_vars;
}

arrow::Result<FilterOperator::PredicateFn> QueryPlanner::ExpressionToPredicate(
    const Expression& expr,
    PlanningContext& ctx) {

    // Create evaluation context
    EvaluationContext eval_ctx(ctx.vocab, ctx.var_to_column);

    // Convert expression to shared_ptr for evaluator
    auto expr_ptr = std::make_shared<Expression>(expr);

    // Use expression evaluator to create predicate function
    return CreatePredicateFunction(expr_ptr, eval_ctx);
}

std::string QueryPlanner::GetColumnNameForPosition(
    const TriplePattern& pattern,
    const std::string& position) {

    // Map position to column name
    if (position == "subject") {
        if (auto var = pattern.GetSubjectVar()) {
            return planning::VariableToColumnName(Variable(*var));
        }
        return "subject";
    } else if (position == "predicate") {
        if (auto var = pattern.GetPredicateVar()) {
            return planning::VariableToColumnName(Variable(*var));
        }
        return "predicate";
    } else if (position == "object") {
        if (auto var = pattern.GetObjectVar()) {
            return planning::VariableToColumnName(Variable(*var));
        }
        return "object";
    }

    return position;
}

arrow::Result<std::shared_ptr<Operator>> QueryPlanner::PlanGroupBy(
    std::shared_ptr<Operator> input,
    const GroupByClause& group_by,
    const std::vector<AggregateExpression>& aggregates,
    PlanningContext& ctx) {

    // Convert GROUP BY variables to column names
    std::vector<std::string> group_keys;
    for (const auto& var : group_by.variables) {
        group_keys.push_back(planning::VariableToColumnName(var));
    }

    // Convert SPARQL aggregates to AggregateSpec
    std::vector<AggregateSpec> aggregate_specs;
    for (const auto& agg : aggregates) {
        // Convert aggregate operator to function
        ARROW_ASSIGN_OR_RAISE(
            auto agg_func,
            ExprOperatorToAggregateFunction(agg.expr->op)
        );

        // Get input column name from aggregate expression
        std::string input_column;
        if (agg_func == AggregateFunction::Count && agg.expr->arguments.empty()) {
            // COUNT(*) - no input column needed
            input_column = "*";
        } else if (!agg.expr->arguments.empty()) {
            // Extract variable from first argument
            auto& arg_expr = agg.expr->arguments[0];
            if (arg_expr->IsConstant() && arg_expr->constant.has_value()) {
                if (auto* var = std::get_if<Variable>(&*arg_expr->constant)) {
                    input_column = planning::VariableToColumnName(*var);
                } else {
                    return arrow::Status::Invalid("Aggregate argument must be a variable");
                }
            } else {
                return arrow::Status::Invalid("Aggregate argument must be a simple variable");
            }
        } else {
            return arrow::Status::Invalid("Aggregate function requires an argument");
        }

        // Get output column name (alias)
        std::string output_column = planning::VariableToColumnName(agg.alias);

        // Create AggregateSpec
        aggregate_specs.emplace_back(
            agg_func,
            input_column,
            output_column,
            agg.distinct
        );
    }

    // Create GroupByOperator
    return std::make_shared<GroupByOperator>(
        input,
        group_keys,
        aggregate_specs
    );
}

arrow::Result<std::shared_ptr<Operator>> QueryPlanner::PlanAggregateOnly(
    std::shared_ptr<Operator> input,
    const std::vector<AggregateExpression>& aggregates,
    PlanningContext& ctx) {

    // Convert SPARQL aggregates to AggregateSpec
    std::vector<AggregateSpec> aggregate_specs;
    for (const auto& agg : aggregates) {
        // Convert aggregate operator to function
        ARROW_ASSIGN_OR_RAISE(
            auto agg_func,
            ExprOperatorToAggregateFunction(agg.expr->op)
        );

        // Get input column name from aggregate expression
        std::string input_column;
        if (agg_func == AggregateFunction::Count && agg.expr->arguments.empty()) {
            // COUNT(*) - no input column needed
            input_column = "*";
        } else if (!agg.expr->arguments.empty()) {
            // Extract variable from first argument
            auto& arg_expr = agg.expr->arguments[0];
            if (arg_expr->IsConstant() && arg_expr->constant.has_value()) {
                if (auto* var = std::get_if<Variable>(&*arg_expr->constant)) {
                    input_column = planning::VariableToColumnName(*var);
                } else {
                    return arrow::Status::Invalid("Aggregate argument must be a variable");
                }
            } else {
                return arrow::Status::Invalid("Aggregate argument must be a simple variable");
            }
        } else {
            return arrow::Status::Invalid("Aggregate function requires an argument");
        }

        // Get output column name (alias)
        std::string output_column = planning::VariableToColumnName(agg.alias);

        // Create AggregateSpec
        aggregate_specs.emplace_back(
            agg_func,
            input_column,
            output_column,
            agg.distinct
        );
    }

    // Create AggregateOperator (no grouping)
    return std::make_shared<AggregateOperator>(
        input,
        aggregate_specs
    );
}

arrow::Result<AggregateFunction> QueryPlanner::ExprOperatorToAggregateFunction(
    ExprOperator op) const {

    switch (op) {
        case ExprOperator::Count:
            return AggregateFunction::Count;
        case ExprOperator::Sum:
            return AggregateFunction::Sum;
        case ExprOperator::Avg:
            return AggregateFunction::Avg;
        case ExprOperator::Min:
            return AggregateFunction::Min;
        case ExprOperator::Max:
            return AggregateFunction::Max;
        case ExprOperator::GroupConcat:
            return AggregateFunction::GroupConcat;
        case ExprOperator::Sample:
            return AggregateFunction::Sample;
        default:
            return arrow::Status::Invalid("Not an aggregate function operator");
    }
}

std::vector<AggregateExpression> QueryPlanner::ExtractAggregates(
    const SelectClause& select) const {

    std::vector<AggregateExpression> aggregates;

    for (const auto& item : select.items) {
        if (auto* agg = std::get_if<AggregateExpression>(&item)) {
            aggregates.push_back(*agg);
        }
    }

    return aggregates;
}

// QueryOptimizer implementation
std::vector<TriplePattern> QueryOptimizer::OptimizeBasicGraphPattern(
    const BasicGraphPattern& bgp) {

    // Simple greedy optimization:
    // 1. Estimate cardinality for each triple pattern
    // 2. Sort by cardinality (smallest first)
    // 3. Build join tree left-to-right

    if (bgp.triples.empty()) {
        return {};
    }

    if (bgp.triples.size() == 1) {
        // Only one pattern - no optimization needed
        return bgp.triples;
    }

    // Use SelectJoinOrder to get optimal ordering
    auto order = SelectJoinOrder(bgp.triples, nullptr);

    // Reorder patterns according to optimal order
    std::vector<TriplePattern> optimized;
    optimized.reserve(bgp.triples.size());

    for (size_t idx : order) {
        if (idx < bgp.triples.size()) {
            optimized.push_back(bgp.triples[idx]);
        }
    }

    // If optimization failed to include all patterns, fall back to original order
    if (optimized.size() != bgp.triples.size()) {
        return bgp.triples;
    }

    return optimized;
}

arrow::Result<size_t> QueryOptimizer::EstimateCardinality(
    const TriplePattern& pattern,
    std::shared_ptr<Vocabulary> vocab) {

    // Convert SPARQL triple pattern to storage triple pattern
    sabot_ql::TriplePattern storage_pattern;

    // Convert subject
    if (auto var = pattern.GetSubjectVar()) {
        // Variable - unbound
        storage_pattern.subject = std::nullopt;
    } else if (auto* iri = std::get_if<IRI>(&pattern.subject)) {
        // Bound IRI - look up ValueId
        Term term = Term::IRI(iri->iri);
        ARROW_ASSIGN_OR_RAISE(auto maybe_id, vocab->GetValueId(term));
        if (maybe_id.has_value()) {
            storage_pattern.subject = maybe_id->getBits();
        } else {
            // IRI not in vocabulary - pattern matches nothing
            return 0;
        }
    } else if (auto* literal = std::get_if<Literal>(&pattern.subject)) {
        // Literals cannot be subjects in RDF - pattern matches nothing
        return 0;
    }

    // Convert predicate
    if (auto var = pattern.GetPredicateVar()) {
        // Variable - unbound
        storage_pattern.predicate = std::nullopt;
    } else if (auto* iri = std::get_if<IRI>(&pattern.predicate)) {
        // Bound IRI - look up ValueId
        Term term = Term::IRI(iri->iri);
        ARROW_ASSIGN_OR_RAISE(auto maybe_id, vocab->GetValueId(term));
        if (maybe_id.has_value()) {
            storage_pattern.predicate = maybe_id->getBits();
        } else {
            // IRI not in vocabulary - pattern matches nothing
            return 0;
        }
    } else if (auto* literal = std::get_if<Literal>(&pattern.predicate)) {
        // Literals cannot be predicates in RDF - pattern matches nothing
        return 0;
    }

    // Convert object
    if (auto var = pattern.GetObjectVar()) {
        // Variable - unbound
        storage_pattern.object = std::nullopt;
    } else if (auto* iri = std::get_if<IRI>(&pattern.object)) {
        // Bound IRI - look up ValueId
        Term term = Term::IRI(iri->iri);
        ARROW_ASSIGN_OR_RAISE(auto maybe_id, vocab->GetValueId(term));
        if (maybe_id.has_value()) {
            storage_pattern.object = maybe_id->getBits();
        } else {
            // IRI not in vocabulary - pattern matches nothing
            return 0;
        }
    } else if (auto* literal = std::get_if<Literal>(&pattern.object)) {
        // Bound literal - look up ValueId
        Term term = Term::Literal(
            literal->value,
            literal->language,
            literal->datatype
        );
        ARROW_ASSIGN_OR_RAISE(auto maybe_id, vocab->GetValueId(term));
        if (maybe_id.has_value()) {
            storage_pattern.object = maybe_id->getBits();
        } else {
            // Literal not in vocabulary - pattern matches nothing
            return 0;
        }
    }

    // Use triple store's cardinality estimation
    return store_->EstimateCardinality(storage_pattern);
}

std::vector<size_t> QueryOptimizer::SelectJoinOrder(
    const std::vector<TriplePattern>& patterns,
    std::shared_ptr<Vocabulary> vocab) {

    // Greedy algorithm: always pick the pattern with smallest cardinality
    // that has join variables with already-selected patterns

    std::vector<size_t> order;
    std::vector<bool> selected(patterns.size(), false);

    // Start with pattern that has smallest cardinality
    size_t best_idx = 0;
    size_t best_card = std::numeric_limits<size_t>::max();

    for (size_t i = 0; i < patterns.size(); ++i) {
        auto card_result = EstimateCardinality(patterns[i], vocab);
        if (card_result.ok()) {
            size_t card = card_result.ValueOrDie();
            if (card < best_card) {
                best_card = card;
                best_idx = i;
            }
        }
    }

    order.push_back(best_idx);
    selected[best_idx] = true;

    // Select remaining patterns
    while (order.size() < patterns.size()) {
        best_idx = 0;
        best_card = std::numeric_limits<size_t>::max();
        bool found = false;

        for (size_t i = 0; i < patterns.size(); ++i) {
            if (selected[i]) continue;

            // Check if this pattern has join variables with selected patterns
            bool has_join = false;
            for (size_t j : order) {
                if (planning::HasJoinVariables(patterns[j], patterns[i])) {
                    has_join = true;
                    break;
                }
            }

            if (!has_join) continue;

            auto card_result = EstimateCardinality(patterns[i], vocab);
            if (card_result.ok()) {
                size_t card = card_result.ValueOrDie();
                if (card < best_card) {
                    best_card = card;
                    best_idx = i;
                    found = true;
                }
            }
        }

        if (!found) {
            // No pattern with join variables found - pick any remaining
            for (size_t i = 0; i < patterns.size(); ++i) {
                if (!selected[i]) {
                    best_idx = i;
                    found = true;
                    break;
                }
            }
        }

        if (found) {
            order.push_back(best_idx);
            selected[best_idx] = true;
        } else {
            break;
        }
    }

    return order;
}

double QueryOptimizer::EstimateJoinCost(
    const TriplePattern& left,
    const TriplePattern& right,
    std::shared_ptr<Vocabulary> vocab) {

    // Simple cost model: cardinality(left) * cardinality(right)
    auto left_card = EstimateCardinality(left, vocab);
    auto right_card = EstimateCardinality(right, vocab);

    if (left_card.ok() && right_card.ok()) {
        return static_cast<double>(left_card.ValueOrDie()) *
               static_cast<double>(right_card.ValueOrDie());
    }

    return 1000000.0;  // Large default cost
}

} // namespace sparql
} // namespace sabot_ql
