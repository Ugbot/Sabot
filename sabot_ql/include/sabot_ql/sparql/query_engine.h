#pragma once

#include <sabot_ql/sparql/ast.h>
#include <sabot_ql/sparql/planner.h>
#include <sabot_ql/execution/executor.h>
#include <sabot_ql/storage/triple_store.h>
#include <sabot_ql/storage/vocabulary.h>
#include <arrow/api.h>
#include <memory>
#include <string>

namespace sabot_ql {
namespace sparql {

// SPARQL query execution engine
// This is the main entry point for executing SPARQL queries
class QueryEngine {
public:
    QueryEngine(std::shared_ptr<TripleStore> store,
                std::shared_ptr<Vocabulary> vocab)
        : planner_(store, vocab),
          executor_(store, vocab),
          store_(std::move(store)),
          vocab_(std::move(vocab)) {}

    // Execute a SPARQL SELECT query (from AST)
    arrow::Result<std::shared_ptr<arrow::Table>> ExecuteSelect(const SelectQuery& query);

    // Get execution statistics from last query
    const QueryStats& GetStats() const { return executor_.GetStats(); }

    // Generate EXPLAIN plan for a query
    std::string Explain(const SelectQuery& query);

    // Generate EXPLAIN ANALYZE for a query (execute and show stats)
    arrow::Result<std::string> ExplainAnalyze(const SelectQuery& query);

private:
    QueryPlanner planner_;
    QueryExecutor executor_;
    std::shared_ptr<TripleStore> store_;
    std::shared_ptr<Vocabulary> vocab_;
};

// Fluent API for building SPARQL queries programmatically
// This provides a convenient way to build queries without parsing text
//
// Example usage:
//   SPARQLBuilder builder;
//
//   auto query = builder
//       .Select({"person", "name", "age"})
//       .Where()
//           .Triple(Var("person"), IRI("hasName"), Var("name"))
//           .Triple(Var("person"), IRI("hasAge"), Var("age"))
//           .Filter(/* ... */)
//       .EndWhere()
//       .Limit(10)
//       .Build();
//
class SPARQLBuilder {
public:
    SPARQLBuilder() = default;

    // Start SELECT clause
    SPARQLBuilder& Select(const std::vector<std::string>& variables);
    SPARQLBuilder& SelectAll();
    SPARQLBuilder& SelectDistinct(const std::vector<std::string>& variables);

    // Start WHERE clause
    SPARQLBuilder& Where();

    // Add triple pattern to WHERE clause
    SPARQLBuilder& Triple(const RDFTerm& subject,
                          const RDFTerm& predicate,
                          const RDFTerm& object);

    // Add FILTER to WHERE clause
    SPARQLBuilder& Filter(std::shared_ptr<Expression> expr);

    // Add OPTIONAL to WHERE clause
    SPARQLBuilder& Optional(std::shared_ptr<QueryPattern> pattern);

    // End WHERE clause
    SPARQLBuilder& EndWhere();

    // Add ORDER BY (ascending by default)
    SPARQLBuilder& OrderBy(const std::string& variable,
                           OrderDirection direction = OrderDirection::Ascending);

    // Add ORDER BY ascending (convenience method)
    SPARQLBuilder& OrderByAsc(const std::string& variable) {
        return OrderBy(variable, OrderDirection::Ascending);
    }

    // Add ORDER BY descending (convenience method)
    SPARQLBuilder& OrderByDesc(const std::string& variable) {
        return OrderBy(variable, OrderDirection::Descending);
    }

    // Add LIMIT
    SPARQLBuilder& Limit(size_t limit);

    // Add OFFSET
    SPARQLBuilder& Offset(size_t offset);

    // Build the query
    SelectQuery Build() const;

    // Get current query (for inspection)
    const SelectQuery& GetQuery() const { return query_; }

private:
    SelectQuery query_;
    QueryPattern* current_pattern_ = nullptr;  // Points to query_.where during WHERE clause
};

// Helper functions for building SPARQL terms
inline Variable Var(const std::string& name) {
    return Variable(name);
}

inline IRI Iri(const std::string& iri) {
    return IRI(iri);
}

inline Literal Lit(const std::string& value) {
    return Literal(value);
}

inline Literal LitLang(const std::string& value, const std::string& language) {
    return Literal(value, language);
}

inline Literal LitType(const std::string& value, const std::string& datatype) {
    return Literal(value, "", datatype);
}

inline BlankNode Blank(const std::string& id) {
    return BlankNode(id);
}

// Helper functions for building SPARQL expressions
namespace expr {

// Comparison operators
inline std::shared_ptr<Expression> Equal(std::shared_ptr<Expression> left,
                                         std::shared_ptr<Expression> right) {
    auto expr = std::make_shared<Expression>(ExprOperator::Equal);
    expr->arguments = {left, right};
    return expr;
}

inline std::shared_ptr<Expression> GreaterThan(std::shared_ptr<Expression> left,
                                               std::shared_ptr<Expression> right) {
    auto expr = std::make_shared<Expression>(ExprOperator::GreaterThan);
    expr->arguments = {left, right};
    return expr;
}

inline std::shared_ptr<Expression> LessThan(std::shared_ptr<Expression> left,
                                            std::shared_ptr<Expression> right) {
    auto expr = std::make_shared<Expression>(ExprOperator::LessThan);
    expr->arguments = {left, right};
    return expr;
}

inline std::shared_ptr<Expression> NotEqual(std::shared_ptr<Expression> left,
                                            std::shared_ptr<Expression> right) {
    auto expr = std::make_shared<Expression>(ExprOperator::NotEqual);
    expr->arguments = {left, right};
    return expr;
}

inline std::shared_ptr<Expression> GreaterThanEqual(std::shared_ptr<Expression> left,
                                                    std::shared_ptr<Expression> right) {
    auto expr = std::make_shared<Expression>(ExprOperator::GreaterThanEqual);
    expr->arguments = {left, right};
    return expr;
}

inline std::shared_ptr<Expression> LessThanEqual(std::shared_ptr<Expression> left,
                                                 std::shared_ptr<Expression> right) {
    auto expr = std::make_shared<Expression>(ExprOperator::LessThanEqual);
    expr->arguments = {left, right};
    return expr;
}

// Logical operators
inline std::shared_ptr<Expression> And(std::shared_ptr<Expression> left,
                                       std::shared_ptr<Expression> right) {
    auto expr = std::make_shared<Expression>(ExprOperator::And);
    expr->arguments = {left, right};
    return expr;
}

inline std::shared_ptr<Expression> Or(std::shared_ptr<Expression> left,
                                      std::shared_ptr<Expression> right) {
    auto expr = std::make_shared<Expression>(ExprOperator::Or);
    expr->arguments = {left, right};
    return expr;
}

inline std::shared_ptr<Expression> Not(std::shared_ptr<Expression> arg) {
    auto expr = std::make_shared<Expression>(ExprOperator::Not);
    expr->arguments = {arg};
    return expr;
}

// Create expression from term (leaf node)
inline std::shared_ptr<Expression> Term(const RDFTerm& term) {
    return std::make_shared<Expression>(term);
}

// Create expression from variable
inline std::shared_ptr<Expression> Var(const std::string& name) {
    return std::make_shared<Expression>(Variable(name));
}

// Create expression from literal
inline std::shared_ptr<Expression> Lit(const std::string& value) {
    return std::make_shared<Expression>(Literal(value));
}

} // namespace expr

} // namespace sparql
} // namespace sabot_ql
