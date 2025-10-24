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

// Forward declaration to break circular dependency
struct RDFTermList;

// SPARQL Blank Node (e.g., _:b1)
struct BlankNode {
    std::string id;
    std::shared_ptr<RDFTermList> collection_items;  // For RDF collections: (item1 item2 ...)

    BlankNode() = default;
    explicit BlankNode(std::string i) : id(std::move(i)) {}

    // Constructor for RDF collection (defined after RDFTerm)
    BlankNode(std::string i, std::shared_ptr<RDFTermList> items);

    std::string ToString() const { return "_:" + id; }
    bool operator==(const BlankNode& other) const { return id == other.id; }
};

// RDF Term: Variable, IRI, Literal, or BlankNode
using RDFTerm = std::variant<Variable, IRI, Literal, BlankNode>;

// RDF collection items (defined after RDFTerm to break circular dependency)
struct RDFTermList {
    std::vector<RDFTerm> items;
};

std::string ToString(const RDFTerm& term);

// Property Path modifiers for combining paths
enum class PropertyPathModifier {
    Sequence,      // p1 / p2 (follow p1 then p2)
    Alternative,   // p1 | p2 (follow either p1 or p2)
    Inverse,       // ^p (follow p in reverse direction)
    Negated        // !p (any predicate except p)
};

// Property Path quantifiers for repetition
enum class PropertyPathQuantifier {
    None,          // No quantifier (just the path)
    ZeroOrMore,    // p* (zero or more occurrences)
    OneOrMore,     // p+ (one or more occurrences)
    ZeroOrOne,     // p? (optional, zero or one occurrence)
    ExactCount,    // p{n} (exactly n occurrences)
    MinCount,      // p{n,} (n or more occurrences)
    RangeCount     // p{n,m} (between n and m occurrences)
};

// Forward declaration for recursive structure
struct PropertyPath;

// Property path element - can be a simple IRI/variable or a complex path
struct PropertyPathElement {
    // Either a simple term or a complex path
    std::variant<RDFTerm, std::shared_ptr<PropertyPath>> element;
    PropertyPathQuantifier quantifier = PropertyPathQuantifier::None;
    int min_count = 0;  // For ExactCount, MinCount, RangeCount quantifiers
    int max_count = 0;  // For RangeCount quantifier (-1 means unbounded)

    PropertyPathElement() = default;
    explicit PropertyPathElement(RDFTerm term) : element(std::move(term)) {}
    explicit PropertyPathElement(std::shared_ptr<PropertyPath> path) : element(std::move(path)) {}

    std::string ToString() const;
};

// Property path - combines multiple elements with modifiers
struct PropertyPath {
    std::vector<PropertyPathElement> elements;
    PropertyPathModifier modifier = PropertyPathModifier::Sequence;

    PropertyPath() = default;
    explicit PropertyPath(PropertyPathElement elem) {
        elements.push_back(std::move(elem));
    }

    std::string ToString() const;
};

// Predicate position - can be a simple term or a property path
using PredicatePosition = std::variant<RDFTerm, PropertyPath>;

// Triple pattern: (subject, predicate, object)
// Subject and object are RDFTerms, predicate can be a term or property path
struct TriplePattern {
    RDFTerm subject;
    PredicatePosition predicate;
    RDFTerm object;

    TriplePattern() = default;
    TriplePattern(RDFTerm s, RDFTerm p, RDFTerm o)
        : subject(std::move(s)), predicate(std::move(p)), object(std::move(o)) {}
    TriplePattern(RDFTerm s, PropertyPath p, RDFTerm o)
        : subject(std::move(s)), predicate(std::move(p)), object(std::move(o)) {}

    std::string ToString() const;

    // Check if a position is a variable
    bool IsSubjectVariable() const { return std::holds_alternative<Variable>(subject); }
    bool IsPredicateVariable() const {
        if (auto* term = std::get_if<RDFTerm>(&predicate)) {
            return std::holds_alternative<Variable>(*term);
        }
        return false;
    }
    bool IsObjectVariable() const { return std::holds_alternative<Variable>(object); }
    bool IsPredicatePropertyPath() const { return std::holds_alternative<PropertyPath>(predicate); }

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
    In,              // IN
    NotIn,           // NOT IN

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
    Regex,           // REGEX(?var, "pattern")

    // String functions (SPARQL 1.1)
    StrLen,          // STRLEN(?str)
    SubStr,          // SUBSTR(?str, start, length)
    UCase,           // UCASE(?str)
    LCase,           // LCASE(?str)
    StrStarts,       // STRSTARTS(?str, ?prefix)
    StrEnds,         // STRENDS(?str, ?suffix)
    Contains,        // CONTAINS(?str, ?substring)
    StrBefore,       // STRBEFORE(?str, ?search)
    StrAfter,        // STRAFTER(?str, ?search)
    Concat,          // CONCAT(?str1, ?str2, ...)
    LangMatches,     // LANGMATCHES(?tag, ?range)
    Replace,         // REPLACE(?str, ?pattern, ?replacement)
    EncodeForURI,    // ENCODE_FOR_URI(?str)

    // Math functions (SPARQL 1.1)
    Abs,             // ABS(?num)
    Round,           // ROUND(?num)
    Ceil,            // CEIL(?num)
    Floor,           // FLOOR(?num)

    // Date/Time functions (SPARQL 1.1)
    Now,             // NOW()
    Year,            // YEAR(?datetime)
    Month,           // MONTH(?datetime)
    Day,             // DAY(?datetime)
    Hours,           // HOURS(?datetime)
    Minutes,         // MINUTES(?datetime)
    Seconds,         // SECONDS(?datetime)
    Timezone,        // TIMEZONE(?datetime)
    Tz,              // TZ(?datetime)

    // Type conversion functions
    StrDt,           // STRDT(?str, ?datatype)
    StrLang,         // STRLANG(?str, ?lang)

    // Hash functions (SPARQL 1.1)
    MD5,             // MD5(?str)
    SHA1,            // SHA1(?str)
    SHA256,          // SHA256(?str)
    SHA384,          // SHA384(?str)
    SHA512,          // SHA512(?str)

    // Special/Control functions (SPARQL 1.1)
    If,              // IF(condition, trueExpr, falseExpr)
    Coalesce,        // COALESCE(expr1, expr2, ...)
    BNode,           // BNODE() or BNODE(?label)
    UUID,            // UUID()
    StrUUID,         // STRUUID()
    IRI,             // IRI(?str)
    IsNumeric,       // isNumeric(?term)
    Rand,            // RAND()

    // Aggregate functions (SPARQL 1.1)
    Count,           // COUNT(?var) or COUNT(*)
    Sum,             // SUM(?var)
    Avg,             // AVG(?var)
    Min,             // MIN(?var)
    Max,             // MAX(?var)
    GroupConcat,     // GROUP_CONCAT(?var; separator="sep")
    Sample,          // SAMPLE(?var)

    // Graph pattern tests
    Exists,          // EXISTS { pattern }
    NotExists,       // NOT EXISTS { pattern }

    // Generic function call (for custom functions like xsd:double)
    FunctionCall     // Custom function call with IRI
};

// Forward declaration
struct QueryPattern;

// FILTER expression
struct Expression {
    ExprOperator op;
    std::vector<std::shared_ptr<Expression>> arguments;
    std::optional<RDFTerm> constant;  // For leaf nodes (constants)
    std::shared_ptr<QueryPattern> exists_pattern;  // For EXISTS/NOT EXISTS
    std::optional<std::string> function_iri;  // For FunctionCall (custom functions like xsd:double)

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

// BIND clause: BIND(expression AS ?var)
// Creates a new variable by evaluating an expression
// Example: BIND(?age + 1 AS ?ageNextYear)
struct BindClause {
    std::shared_ptr<Expression> expr;
    Variable alias;

    BindClause() = default;
    BindClause(std::shared_ptr<Expression> e, Variable a)
        : expr(std::move(e)), alias(std::move(a)) {}

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

// VALUES clause - inline data table
// Example: VALUES (?x ?y) { ("Alice" 30) ("Bob" 25) }
struct ValuesClause {
    std::vector<Variable> variables;           // Variables in the table
    std::vector<std::vector<RDFTerm>> rows;    // Data rows

    ValuesClause() = default;
    ValuesClause(std::vector<Variable> vars, std::vector<std::vector<RDFTerm>> data)
        : variables(std::move(vars)), rows(std::move(data)) {}

    std::string ToString() const;
};

// MINUS clause - anti-join pattern (remove matching rows)
// Example: { ?s ?p ?o } MINUS { ?s <http://example.org/exclude> ?o }
struct MinusPattern {
    std::shared_ptr<QueryPattern> pattern;

    MinusPattern() = default;
    explicit MinusPattern(std::shared_ptr<QueryPattern> p) : pattern(std::move(p)) {}

    std::string ToString() const;
};

// GRAPH pattern - named graph access
// Example: GRAPH ?g { triple patterns } or GRAPH <iri> { triple patterns }
struct GraphPattern {
    RDFTerm graph;  // Variable or IRI identifying the graph
    std::shared_ptr<QueryPattern> pattern;

    GraphPattern() = default;
    GraphPattern(RDFTerm g, std::shared_ptr<QueryPattern> p)
        : graph(std::move(g)), pattern(std::move(p)) {}

    std::string ToString() const;
};

// EXISTS/NOT EXISTS filter - semi-join and anti-semi-join
// Example: FILTER EXISTS { ?s <http://example.org/knows> ?o }
struct ExistsPattern {
    std::shared_ptr<QueryPattern> pattern;
    bool is_negated;  // true for NOT EXISTS

    ExistsPattern() : is_negated(false) {}
    ExistsPattern(std::shared_ptr<QueryPattern> p, bool negated = false)
        : pattern(std::move(p)), is_negated(negated) {}

    std::string ToString() const;
};

// Forward declaration for SubqueryPattern (defined after SelectQuery)
struct SubqueryPattern;

// Graph pattern: BGP, FILTER, BIND, OPTIONAL, UNION, VALUES, MINUS, EXISTS, Subquery
struct QueryPattern {
    std::optional<BasicGraphPattern> bgp;
    std::vector<FilterClause> filters;
    std::vector<BindClause> binds;
    std::vector<OptionalPattern> optionals;
    std::vector<UnionPattern> unions;
    std::vector<ValuesClause> values;          // VALUES clauses
    std::vector<MinusPattern> minus_patterns;  // MINUS clauses
    std::vector<GraphPattern> graph_patterns;  // GRAPH clauses
    std::vector<ExistsPattern> exists_patterns; // EXISTS/NOT EXISTS
    std::vector<SubqueryPattern> subqueries;   // Nested SELECT queries

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

// Aggregate expression: COUNT(?var) AS ?count
// Represents aggregate function call with optional alias
struct AggregateExpression {
    std::shared_ptr<Expression> expr;  // The aggregate function expression
    Variable alias;                     // AS ?alias (output variable name)
    bool distinct = false;              // COUNT(DISTINCT ?var)

    AggregateExpression() = default;
    AggregateExpression(std::shared_ptr<Expression> e, Variable a, bool d = false)
        : expr(std::move(e)), alias(std::move(a)), distinct(d) {}

    std::string ToString() const;
};

// Projection expression - any expression with an alias in SELECT clause
struct ProjectionExpression {
    std::shared_ptr<Expression> expr;  // The expression to project
    Variable alias;                     // The alias variable

    ProjectionExpression() = default;
    ProjectionExpression(std::shared_ptr<Expression> e, Variable a)
        : expr(std::move(e)), alias(std::move(a)) {}

    std::string ToString() const;
};

// SELECT clause item: variable, aggregate, or general expression
using SelectItem = std::variant<Variable, AggregateExpression, ProjectionExpression>;

// SELECT variables
struct SelectClause {
    bool distinct = false;
    std::vector<SelectItem> items;  // Empty means SELECT *

    SelectClause() = default;

    bool IsSelectAll() const { return items.empty(); }
    bool HasAggregates() const;

    std::string ToString() const;
};

// GROUP BY clause
// GROUP BY item - can be a variable or an expression with alias
struct GroupByItem {
    std::optional<Variable> variable;  // Simple variable: GROUP BY ?x
    std::shared_ptr<Expression> expression;  // Expression: GROUP BY (expr AS ?alias)
    std::optional<Variable> alias;  // Alias for expression

    GroupByItem() = default;
    explicit GroupByItem(Variable var) : variable(std::move(var)) {}
    GroupByItem(std::shared_ptr<Expression> expr, Variable a)
        : expression(std::move(expr)), alias(std::move(a)) {}

    bool IsVariable() const { return variable.has_value(); }
    std::string ToString() const;
};

struct GroupByClause {
    std::vector<Variable> variables;  // Variables to group by (legacy)
    std::vector<GroupByItem> items;   // Variables or expressions with aliases

    GroupByClause() = default;
    explicit GroupByClause(std::vector<Variable> vars) : variables(std::move(vars)) {}

    bool IsEmpty() const { return variables.empty() && items.empty(); }
    std::string ToString() const;
};

// SPARQL SELECT query
struct SelectQuery {
    SelectClause select;
    QueryPattern where;
    std::optional<GroupByClause> group_by;  // GROUP BY clause
    std::vector<OrderBy> order_by;
    std::optional<size_t> limit;
    std::optional<size_t> offset;

    SelectQuery() = default;

    bool HasGroupBy() const { return group_by.has_value() && !group_by->IsEmpty(); }
    bool HasAggregates() const { return select.HasAggregates(); }

    std::string ToString() const;
};

// Subquery - nested SELECT query inside WHERE clause
// Example: { SELECT ?x WHERE { ?x ?p ?o } }
struct SubqueryPattern {
    SelectQuery query;

    SubqueryPattern() = default;
    explicit SubqueryPattern(SelectQuery q) : query(std::move(q)) {}

    std::string ToString() const;
};

// SPARQL ASK query - returns boolean (does pattern match?)
struct AskQuery {
    QueryPattern where;

    AskQuery() = default;
    explicit AskQuery(QueryPattern w) : where(std::move(w)) {}

    std::string ToString() const;
};

// CONSTRUCT template - pattern for building result graph
struct ConstructTemplate {
    std::vector<TriplePattern> triples;

    ConstructTemplate() = default;
    explicit ConstructTemplate(std::vector<TriplePattern> t) : triples(std::move(t)) {}

    std::string ToString() const;
};

// SPARQL CONSTRUCT query - builds RDF graph from results
struct ConstructQuery {
    ConstructTemplate construct_template;
    QueryPattern where;

    ConstructQuery() = default;
    ConstructQuery(ConstructTemplate ct, QueryPattern w)
        : construct_template(std::move(ct)), where(std::move(w)) {}

    std::string ToString() const;
};

// SPARQL DESCRIBE query - returns all triples about resources
struct DescribeQuery {
    std::vector<RDFTerm> resources;  // IRIs or variables to describe
    std::optional<QueryPattern> where;  // Optional WHERE clause

    DescribeQuery() = default;

    std::string ToString() const;
};

// Query variant - can be SELECT, ASK, CONSTRUCT, or DESCRIBE
using QueryBody = std::variant<SelectQuery, AskQuery, ConstructQuery, DescribeQuery>;

// Top-level query
struct Query {
    std::optional<std::string> base_iri;  // BASE <iri>
    QueryBody query_body;

    Query() : query_body(SelectQuery()) {}
    explicit Query(SelectQuery sq) : query_body(std::move(sq)) {}
    explicit Query(AskQuery aq) : query_body(std::move(aq)) {}
    explicit Query(ConstructQuery cq) : query_body(std::move(cq)) {}
    explicit Query(DescribeQuery dq) : query_body(std::move(dq)) {}

    std::string ToString() const;

    // Type checking helpers
    bool IsSelect() const { return std::holds_alternative<SelectQuery>(query_body); }
    bool IsAsk() const { return std::holds_alternative<AskQuery>(query_body); }
    bool IsConstruct() const { return std::holds_alternative<ConstructQuery>(query_body); }
    bool IsDescribe() const { return std::holds_alternative<DescribeQuery>(query_body); }
};

} // namespace sparql
} // namespace sabot_ql
