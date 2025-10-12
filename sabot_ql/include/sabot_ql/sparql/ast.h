#pragma once

#include <sabot_ql/types/value_id.h>
#include <string>
#include <vector>
#include <memory>
#include <variant>
#include <optional>

namespace sabot_ql {
namespace sparql {

// Forward declarations
struct QueryPattern;
struct Expression;

// SPARQL variable (e.g., ?person, ?name)
struct Variable {
    std::string name;  // Without the '?' prefix

    Variable() = default;
    explicit Variable(std::string n) : name(std::move(n)) {}

    std::string ToString() const { return "?" + name; }
    bool operator==(const Variable& other) const { return name == other.name; }
};

// SPARQL IRI (e.g., <http://example.org/Person>)
struct IRI {
    std::string iri;

    IRI() = default;
    explicit IRI(std::string i) : iri(std::move(i)) {}

    std::string ToString() const { return "<" + iri + ">"; }
    bool operator==(const IRI& other) const { return iri == other.iri; }
};

// SPARQL Literal (e.g., "Alice", "42"^^xsd:integer, "Hello"@en)
struct Literal {
    std::string value;
    std::string language;  // Optional language tag
    std::string datatype;  // Optional datatype IRI

    Literal() = default;
    explicit Literal(std::string v, std::string lang = "", std::string dt = "")
        : value(std::move(v)), language(std::move(lang)), datatype(std::move(dt)) {}

    std::string ToString() const;
    bool operator==(const Literal& other) const {
        return value == other.value && language == other.language && datatype == other.datatype;
    }
};

// SPARQL Blank Node (e.g., _:b1)
struct BlankNode {
    std::string id;

    BlankNode() = default;
    explicit BlankNode(std::string i) : id(std::move(i)) {}

    std::string ToString() const { return "_:" + id; }
    bool operator==(const BlankNode& other) const { return id == other.id; }
};

// RDF Term: Variable, IRI, Literal, or BlankNode
using RDFTerm = std::variant<Variable, IRI, Literal, BlankNode>;

std::string ToString(const RDFTerm& term);

// Triple pattern: (subject, predicate, object)
// Each component can be a variable or a concrete term
struct TriplePattern {
    RDFTerm subject;
    RDFTerm predicate;
    RDFTerm object;

    TriplePattern() = default;
    TriplePattern(RDFTerm s, RDFTerm p, RDFTerm o)
        : subject(std::move(s)), predicate(std::move(p)), object(std::move(o)) {}

    std::string ToString() const;

    // Check if a position is a variable
    bool IsSubjectVariable() const { return std::holds_alternative<Variable>(subject); }
    bool IsPredicateVariable() const { return std::holds_alternative<Variable>(predicate); }
    bool IsObjectVariable() const { return std::holds_alternative<Variable>(object); }

    // Get variable name if it's a variable
    std::optional<std::string> GetSubjectVar() const;
    std::optional<std::string> GetPredicateVar() const;
    std::optional<std::string> GetObjectVar() const;
};

// Basic Graph Pattern (BGP): a set of triple patterns
struct BasicGraphPattern {
    std::vector<TriplePattern> triples;

    BasicGraphPattern() = default;
    explicit BasicGraphPattern(std::vector<TriplePattern> t) : triples(std::move(t)) {}

    std::string ToString() const;
};

// FILTER expression operators
enum class ExprOperator {
    // Comparison
    Equal,           // =
    NotEqual,        // !=
    LessThan,        // <
    LessThanEqual,   // <=
    GreaterThan,     // >
    GreaterThanEqual,// >=

    // Logical
    And,             // &&
    Or,              // ||
    Not,             // !

    // Arithmetic
    Plus,            // +
    Minus,           // -
    Multiply,        // *
    Divide,          // /

    // Built-in functions
    Bound,           // BOUND(?var)
    IsIRI,           // isIRI(?var)
    IsLiteral,       // isLiteral(?var)
    IsBlank,         // isBlank(?var)
    Str,             // STR(?var)
    Lang,            // LANG(?var)
    Datatype,        // DATATYPE(?var)
    Regex            // REGEX(?var, "pattern")
};

// FILTER expression
struct Expression {
    ExprOperator op;
    std::vector<std::shared_ptr<Expression>> arguments;
    std::optional<RDFTerm> constant;  // For leaf nodes (constants)

    Expression() : op(ExprOperator::Equal) {}

    explicit Expression(ExprOperator o) : op(o) {}

    explicit Expression(RDFTerm term)
        : op(ExprOperator::Equal), constant(std::move(term)) {}

    std::string ToString() const;

    // Check if this is a leaf node (constant)
    bool IsConstant() const { return constant.has_value(); }
};

// FILTER clause
struct FilterClause {
    std::shared_ptr<Expression> expr;

    FilterClause() = default;
    explicit FilterClause(std::shared_ptr<Expression> e) : expr(std::move(e)) {}

    std::string ToString() const;
};

// OPTIONAL clause
struct OptionalPattern {
    std::shared_ptr<QueryPattern> pattern;

    OptionalPattern() = default;
    explicit OptionalPattern(std::shared_ptr<QueryPattern> p) : pattern(std::move(p)) {}

    std::string ToString() const;
};

// UNION clause
struct UnionPattern {
    std::vector<std::shared_ptr<QueryPattern>> patterns;

    UnionPattern() = default;
    explicit UnionPattern(std::vector<std::shared_ptr<QueryPattern>> p)
        : patterns(std::move(p)) {}

    std::string ToString() const;
};

// Graph pattern: BGP, FILTER, OPTIONAL, or UNION
struct QueryPattern {
    std::optional<BasicGraphPattern> bgp;
    std::vector<FilterClause> filters;
    std::vector<OptionalPattern> optionals;
    std::vector<UnionPattern> unions;

    QueryPattern() = default;

    std::string ToString() const;
};

// ORDER BY clause
enum class OrderDirection {
    Ascending,
    Descending
};

struct OrderBy {
    Variable var;
    OrderDirection direction;

    OrderBy() : direction(OrderDirection::Ascending) {}
    OrderBy(Variable v, OrderDirection d) : var(std::move(v)), direction(d) {}

    std::string ToString() const;
};

// SELECT variables
struct SelectClause {
    bool distinct = false;
    std::vector<Variable> variables;  // Empty means SELECT *

    SelectClause() = default;

    bool IsSelectAll() const { return variables.empty(); }

    std::string ToString() const;
};

// SPARQL SELECT query
struct SelectQuery {
    SelectClause select;
    QueryPattern where;
    std::vector<OrderBy> order_by;
    std::optional<size_t> limit;
    std::optional<size_t> offset;

    SelectQuery() = default;

    std::string ToString() const;
};

// Top-level query (for now, only SELECT is supported)
struct Query {
    SelectQuery select_query;

    Query() = default;
    explicit Query(SelectQuery sq) : select_query(std::move(sq)) {}

    std::string ToString() const { return select_query.ToString(); }
};

} // namespace sparql
} // namespace sabot_ql
