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
    SELECT, WHERE, FILTER, OPTIONAL, UNION, ORDER, BY, ASC, DESC,
    DISTINCT, LIMIT, OFFSET, AS, GROUP,

    // Built-in functions
    BOUND, ISIRI, ISLITERAL, ISBLANK, STR, LANG, DATATYPE, REGEX,

    // Aggregate functions
    COUNT, SUM, AVG, MIN, MAX, GROUP_CONCAT, SAMPLE,

    // Operators
    LPAREN,          // (
    RPAREN,          // )
    LBRACE,          // {
    RBRACE,          // }
    DOT,             // .
    SEMICOLON,       // ;
    COMMA,           // ,

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

// SPARQL query parser (recursive descent)
class SPARQLParser {
public:
    explicit SPARQLParser(std::vector<Token> tokens);

    // Parse a complete SPARQL query
    arrow::Result<Query> Parse();

private:
    std::vector<Token> tokens_;
    size_t pos_ = 0;
    std::unordered_map<std::string, std::string> prefixes_;  // prefix -> IRI mapping

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
    arrow::Status ParsePrefixDeclaration();
    arrow::Result<SelectQuery> ParseSelectQuery();
    arrow::Result<SelectClause> ParseSelectClause();
    arrow::Result<QueryPattern> ParseWhereClause();
    arrow::Result<BasicGraphPattern> ParseBasicGraphPattern();
    arrow::Result<TriplePattern> ParseTriplePattern();
    arrow::Result<RDFTerm> ParseRDFTerm();
    arrow::Result<FilterClause> ParseFilterClause();
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
    arrow::Result<GroupByClause> ParseGroupByClause();
    arrow::Result<std::vector<OrderBy>> ParseOrderByClause();

    // Helper methods
    arrow::Result<Variable> ParseVariable();
    arrow::Result<IRI> ParseIRI();
    arrow::Result<Literal> ParseLiteral();
    arrow::Result<ExprOperator> TokenTypeToOperator(TokenType type) const;
    arrow::Result<std::string> ExpandPrefixedName(const std::string& prefixed_name);
};

// Convenience function for parsing SPARQL queries from text
arrow::Result<Query> ParseSPARQL(const std::string& query_text);

} // namespace sparql
} // namespace sabot_ql
