#pragma once

#include <sabot_ql/sparql/ast.h>
#include <string>
#include <vector>
#include <optional>
#include <unordered_map>
#include <arrow/status.h>
#include <arrow/result.h>

namespace sabot_ql {
namespace sparql {

// Token types for SPARQL lexical analysis
enum class TokenType {
    // Keywords
    PREFIX, BASE,
    SELECT, ASK, CONSTRUCT, DESCRIBE,
    WHERE, FILTER, BIND, OPTIONAL, UNION, ORDER, BY, ASC, DESC,
    DISTINCT, LIMIT, OFFSET, AS, GROUP, GRAPH, FROM,
    VALUES, MINUS_KEYWORD, EXISTS, NOT_KEYWORD, IN, SEPARATOR,

    // Built-in functions
    BOUND, ISIRI, ISLITERAL, ISBLANK, STR, LANG, DATATYPE, REGEX,

    // String functions
    STRLEN, SUBSTR, UCASE, LCASE, STRSTARTS, STRENDS, CONTAINS,
    STRBEFORE, STRAFTER, CONCAT, LANGMATCHES, REPLACE, ENCODE_FOR_URI,

    // Math functions
    ABS, ROUND, CEIL, FLOOR,

    // Date/Time functions
    NOW, YEAR, MONTH, DAY, HOURS, MINUTES, SECONDS, TIMEZONE, TZ,

    // Type conversion functions
    STRDT, STRLANG,

    // Hash functions
    MD5, SHA1, SHA256, SHA384, SHA512,

    // Special/Control functions
    IF, COALESCE, BNODE, UUID, STRUUID, IRI, URI, ISNUMERIC, RAND,

    // Aggregate functions
    COUNT, SUM, AVG, MIN, MAX, GROUP_CONCAT, SAMPLE,

    // Operators
    LPAREN,          // (
    RPAREN,          // )
    LBRACE,          // {
    RBRACE,          // }
    LBRACKET,        // [
    RBRACKET,        // ]
    DOT,             // .
    SEMICOLON,       // ;
    COMMA,           // ,
    PIPE,            // | (for property path alternative, different from OR)
    CARET,           // ^ (for property path inverse)
    QUESTION,        // ? (for property path zero-or-one)

    EQUAL,           // =
    NOT_EQUAL,       // !=
    LESS_THAN,       // <
    LESS_EQUAL,      // <=
    GREATER_THAN,    // >
    GREATER_EQUAL,   // >=

    AND,             // &&
    OR,              // ||
    NOT,             // !

    PLUS,            // +
    MINUS,           // -
    MULTIPLY,        // *
    DIVIDE,          // /

    // Literals
    VARIABLE,        // ?name or $name
    IRI_REF,         // <http://example.org/...>
    STRING_LITERAL,  // "string" or 'string'
    INTEGER,         // 42
    DECIMAL,         // 3.14
    BOOLEAN,         // true or false

    // Special
    PREFIX_LABEL,    // prefix:localName
    BLANK_NODE,      // _:label
    DATATYPE_MARKER, // ^^
    LANG_TAG,        // @en

    // End of input
    END_OF_INPUT,

    // Error token
    ERROR
};

// Token representation
struct Token {
    TokenType type;
    std::string text;
    size_t line;
    size_t column;

    Token(TokenType t, std::string txt, size_t l, size_t c)
        : type(t), text(std::move(txt)), line(l), column(c) {}

    std::string ToString() const;
};

// Tokenizer for SPARQL queries
class SPARQLTokenizer {
public:
    explicit SPARQLTokenizer(std::string input);

    // Tokenize the entire input
    arrow::Result<std::vector<Token>> Tokenize();

private:
    std::string input_;
    size_t pos_ = 0;
    size_t line_ = 1;
    size_t column_ = 1;

    // Character inspection
    char CurrentChar() const;
    char PeekChar(size_t offset = 1) const;
    bool IsAtEnd() const;

    // Character consumption
    void Advance();
    void SkipWhitespace();
    void SkipComment();

    // Token recognition
    Token ReadVariable();
    Token ReadIRI();
    Token ReadStringLiteral(char quote);
    Token ReadNumber();
    Token ReadKeywordOrPrefixedName();
    Token ReadOperator();

    // Helper functions
    bool IsWhitespace(char c) const;
    bool IsDigit(char c) const;
    bool IsAlpha(char c) const;
    bool IsAlphaNumeric(char c) const;
    bool IsKeywordStart(char c) const;

    Token MakeToken(TokenType type, std::string text);
    Token MakeError(std::string message);

    // Keyword lookup
    std::optional<TokenType> LookupKeyword(const std::string& text) const;
};

// Forward declarations for property path parsing helpers
struct PathToken;
arrow::Result<std::vector<PathToken>> TokenizePropertyPathTokens(SPARQLParser& parser);

// SPARQL query parser (recursive descent)
class SPARQLParser {
public:
    explicit SPARQLParser(std::vector<Token> tokens);

    // Parse a complete SPARQL query
    arrow::Result<Query> Parse();

    // Friend declarations for property path helpers
    friend arrow::Result<std::vector<PathToken>> TokenizePropertyPathTokens(SPARQLParser& parser);

private:
    std::vector<Token> tokens_;
    size_t pos_ = 0;
    std::string base_iri_;  // BASE IRI for expanding relative IRIs
    std::unordered_map<std::string, std::string> prefixes_;  // prefix -> IRI mapping
    size_t blank_node_counter_ = 0;  // Counter for generating unique blank node IDs

    // Token navigation
    const Token& CurrentToken() const;
    const Token& PeekToken(size_t offset = 1) const;
    bool IsAtEnd() const;
    void Advance();

    // Token matching
    bool Match(TokenType type);
    bool Check(TokenType type) const;
    arrow::Status Expect(TokenType type, const std::string& message);

    // Error handling
    arrow::Status Error(const std::string& message) const;

    // Parsing methods (top-down)
    arrow::Result<std::string> ParseBaseDeclaration();
    arrow::Status ParsePrefixDeclaration();
    arrow::Result<SelectQuery> ParseSelectQuery();
    arrow::Result<AskQuery> ParseAskQuery();
    arrow::Result<ConstructQuery> ParseConstructQuery();
    arrow::Result<DescribeQuery> ParseDescribeQuery();
    arrow::Result<SelectClause> ParseSelectClause();
    arrow::Result<ConstructTemplate> ParseConstructTemplate();
    arrow::Result<QueryPattern> ParseWhereClause();
    arrow::Result<BasicGraphPattern> ParseBasicGraphPattern();
    arrow::Result<TriplePattern> ParseTriplePattern();
    arrow::Result<RDFTerm> ParseRDFTerm();
    arrow::Result<BlankNode> ParseBlankNodePropertyList();
    arrow::Result<FilterClause> ParseFilterClause();
    arrow::Result<BindClause> ParseBindClause();
    arrow::Result<std::shared_ptr<Expression>> ParseExpression();
    arrow::Result<std::shared_ptr<Expression>> ParseOrExpression();
    arrow::Result<std::shared_ptr<Expression>> ParseAndExpression();
    arrow::Result<std::shared_ptr<Expression>> ParseComparisonExpression();
    arrow::Result<std::shared_ptr<Expression>> ParseAdditiveExpression();
    arrow::Result<std::shared_ptr<Expression>> ParseMultiplicativeExpression();
    arrow::Result<std::shared_ptr<Expression>> ParseUnaryExpression();
    arrow::Result<std::shared_ptr<Expression>> ParsePrimaryExpression();
    arrow::Result<std::shared_ptr<Expression>> ParseBuiltInCall();
    arrow::Result<OptionalPattern> ParseOptionalClause();
    arrow::Result<UnionPattern> ParseUnionClause();
    arrow::Result<ValuesClause> ParseValuesClause();
    arrow::Result<MinusPattern> ParseMinusClause();
    arrow::Result<GraphPattern> ParseGraphClause();
    arrow::Result<ExistsPattern> ParseExistsClause(bool negated = false);
    arrow::Result<SubqueryPattern> ParseSubqueryPattern();
    arrow::Result<GroupByClause> ParseGroupByClause();
    arrow::Result<std::vector<OrderBy>> ParseOrderByClause();

    // Helper methods
    arrow::Result<Variable> ParseVariable();
    arrow::Result<IRI> ParseIRI();
    arrow::Result<Literal> ParseLiteral();
    arrow::Result<PropertyPath> ParsePropertyPath();
    arrow::Result<PropertyPathElement> ParsePropertyPathElement();  // Deprecated - no longer used
    arrow::Result<PredicatePosition> ParsePredicatePosition();
    arrow::Result<ExprOperator> TokenTypeToOperator(TokenType type) const;
    arrow::Result<std::string> ExpandPrefixedName(const std::string& prefixed_name);
    std::string ExpandRelativeIRI(const std::string& iri) const;
};

// Convenience function for parsing SPARQL queries from text
arrow::Result<Query> ParseSPARQL(const std::string& query_text);

} // namespace sparql
} // namespace sabot_ql
