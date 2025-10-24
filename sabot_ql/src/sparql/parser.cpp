#include <sabot_ql/sparql/parser.h>
#include <cctype>
#include <unordered_map>
#include <sstream>
#include <iostream>

namespace sabot_ql {
namespace sparql {

// ============================================================================
// Token Implementation
// ============================================================================

std::string Token::ToString() const {
    std::ostringstream oss;
    oss << "Token(" << static_cast<int>(type) << ", \"" << text
        << "\", line=" << line << ", col=" << column << ")";
    return oss.str();
}

// ============================================================================
// SPARQLTokenizer Implementation
// ============================================================================

SPARQLTokenizer::SPARQLTokenizer(std::string input)
    : input_(std::move(input)) {}

arrow::Result<std::vector<Token>> SPARQLTokenizer::Tokenize() {
    std::vector<Token> tokens;

    while (!IsAtEnd()) {
        SkipWhitespace();
        if (IsAtEnd()) break;

        // Skip comments
        if (CurrentChar() == '#') {
            SkipComment();
            continue;
        }

        char c = CurrentChar();

        // Variable: ?name or $name (or ? quantifier in property paths)
        if (c == '?' || c == '$') {
            // Check if this is a variable or a quantifier
            // Variables must be followed by alphanumeric or underscore
            // Quantifiers (?) appear after ), IRI, etc.
            if (c == '?' && !IsAlphaNumeric(PeekChar()) && PeekChar() != '_') {
                // It's a quantifier, not a variable
                tokens.push_back(MakeToken(TokenType::QUESTION, "?"));
                Advance();
            } else {
                // It's a variable
                tokens.push_back(ReadVariable());
            }
            continue;
        }

        // IRI: <...> (QLever approach: always IRI if has closing >)
        if (c == '<') {
            // Could be IRI or < operator
            if (PeekChar() == '=') {
                tokens.push_back(MakeToken(TokenType::LESS_EQUAL, "<="));
                Advance();
                Advance();
            } else {
                // Look ahead to see if there's a closing > (making it an IRI)
                size_t saved_pos = pos_;
                Advance();  // Skip '<'
                bool found_close = false;
                // Scan until we hit >, whitespace, newline, or EOF
                while (!IsAtEnd()) {
                    char ch = CurrentChar();
                    if (ch == '>') {
                        found_close = true;
                        break;
                    }
                    // Stop at whitespace or newline (not an IRI)
                    if (ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r') {
                        break;
                    }
                    Advance();
                }
                // Restore position
                pos_ = saved_pos;

                if (found_close) {
                    // It's an IRI: <...>
                    tokens.push_back(ReadIRI());
                } else {
                    // It's a less-than operator: <
                    tokens.push_back(MakeToken(TokenType::LESS_THAN, "<"));
                    Advance();
                }
            }
            continue;
        }

        // String literal: "..." or '...'
        if (c == '"' || c == '\'') {
            tokens.push_back(ReadStringLiteral(c));
            continue;
        }

        // Number: 42 or 3.14
        if (IsDigit(c) || (c == '-' && IsDigit(PeekChar()))) {
            tokens.push_back(ReadNumber());
            continue;
        }

        // Blank node: _:label
        if (c == '_' && PeekChar() == ':') {
            size_t start_line = line_;
            size_t start_column = column_;
            Advance(); // _
            Advance(); // :
            std::string label;
            while (!IsAtEnd() && (IsAlphaNumeric(CurrentChar()) || CurrentChar() == '_')) {
                label += CurrentChar();
                Advance();
            }
            tokens.push_back(Token(TokenType::BLANK_NODE, "_:" + label, start_line, start_column));
            continue;
        }

        // Keyword or prefixed name
        if (IsAlpha(c) || c == '_') {
            tokens.push_back(ReadKeywordOrPrefixedName());
            continue;
        }

        // Colon: either empty PREFIX or prefixed name with empty prefix
        if (c == ':') {
            size_t start_line = line_;
            size_t start_column = column_;
            std::string text = ":";
            Advance();

            // Check if followed by an identifier (for :localName syntax)
            if (!IsAtEnd() && (IsAlpha(CurrentChar()) || CurrentChar() == '_')) {
                // Read the local name part
                while (!IsAtEnd() && (IsAlphaNumeric(CurrentChar()) || CurrentChar() == '_')) {
                    text += CurrentChar();
                    Advance();
                }
            }

            // Token is either ":" (empty prefix) or ":localName" (prefixed name)
            tokens.push_back(Token(TokenType::PREFIX_LABEL, text, start_line, start_column));
            continue;
        }

        // Operators
        Token op_token = ReadOperator();
        if (op_token.type != TokenType::ERROR) {
            tokens.push_back(op_token);
            continue;
        }

        // Unknown character
        std::ostringstream oss;
        oss << "Unknown character '" << c << "' at line " << line_
            << ", column " << column_;
        return arrow::Status::Invalid(oss.str());
    }

    tokens.push_back(MakeToken(TokenType::END_OF_INPUT, ""));
    return tokens;
}

// Character inspection

char SPARQLTokenizer::CurrentChar() const {
    if (IsAtEnd()) return '\0';
    return input_[pos_];
}

char SPARQLTokenizer::PeekChar(size_t offset) const {
    if (pos_ + offset >= input_.size()) return '\0';
    return input_[pos_ + offset];
}

bool SPARQLTokenizer::IsAtEnd() const {
    return pos_ >= input_.size();
}

// Character consumption

void SPARQLTokenizer::Advance() {
    if (IsAtEnd()) return;
    if (input_[pos_] == '\n') {
        line_++;
        column_ = 1;
    } else {
        column_++;
    }
    pos_++;
}

void SPARQLTokenizer::SkipWhitespace() {
    while (!IsAtEnd() && IsWhitespace(CurrentChar())) {
        Advance();
    }
}

void SPARQLTokenizer::SkipComment() {
    // Skip until end of line
    while (!IsAtEnd() && CurrentChar() != '\n') {
        Advance();
    }
    if (!IsAtEnd()) Advance(); // Skip the newline
}

// Token recognition

Token SPARQLTokenizer::ReadVariable() {
    size_t start_line = line_;
    size_t start_column = column_;
    char prefix = CurrentChar(); // ? or $
    Advance();

    std::string name;
    while (!IsAtEnd() && (IsAlphaNumeric(CurrentChar()) || CurrentChar() == '_')) {
        name += CurrentChar();
        Advance();
    }

    if (name.empty()) {
        return MakeError("Variable name expected after " + std::string(1, prefix));
    }

    return Token(TokenType::VARIABLE, std::string(1, prefix) + name, start_line, start_column);
}

Token SPARQLTokenizer::ReadIRI() {
    size_t start_line = line_;
    size_t start_column = column_;

    Advance(); // Skip '<'

    std::string iri;
    while (!IsAtEnd() && CurrentChar() != '>') {
        iri += CurrentChar();
        Advance();
    }

    if (IsAtEnd() || CurrentChar() != '>') {
        return MakeError("Unterminated IRI");
    }

    Advance(); // Skip '>'

    return Token(TokenType::IRI_REF, "<" + iri + ">", start_line, start_column);
}

Token SPARQLTokenizer::ReadStringLiteral(char quote) {
    size_t start_line = line_;
    size_t start_column = column_;

    Advance(); // Skip opening quote

    std::string value;
    while (!IsAtEnd() && CurrentChar() != quote) {
        if (CurrentChar() == '\\') {
            Advance();
            if (IsAtEnd()) {
                return MakeError("Unterminated string literal");
            }
            // Handle escape sequences
            char escaped = CurrentChar();
            switch (escaped) {
                case 'n': value += '\n'; break;
                case 't': value += '\t'; break;
                case 'r': value += '\r'; break;
                case '\\': value += '\\'; break;
                case '"': value += '"'; break;
                case '\'': value += '\''; break;
                default: value += escaped; break;
            }
            Advance();
        } else {
            value += CurrentChar();
            Advance();
        }
    }

    if (IsAtEnd() || CurrentChar() != quote) {
        return MakeError("Unterminated string literal");
    }

    Advance(); // Skip closing quote

    return Token(TokenType::STRING_LITERAL, value, start_line, start_column);
}

Token SPARQLTokenizer::ReadNumber() {
    size_t start_line = line_;
    size_t start_column = column_;

    std::string number;
    bool is_decimal = false;

    // Handle negative sign
    if (CurrentChar() == '-') {
        number += CurrentChar();
        Advance();
    }

    // Read digits
    while (!IsAtEnd() && IsDigit(CurrentChar())) {
        number += CurrentChar();
        Advance();
    }

    // Check for decimal point
    if (!IsAtEnd() && CurrentChar() == '.') {
        is_decimal = true;
        number += CurrentChar();
        Advance();

        // Read fractional digits
        while (!IsAtEnd() && IsDigit(CurrentChar())) {
            number += CurrentChar();
            Advance();
        }
    }

    TokenType type = is_decimal ? TokenType::DECIMAL : TokenType::INTEGER;
    return Token(type, number, start_line, start_column);
}

Token SPARQLTokenizer::ReadKeywordOrPrefixedName() {
    size_t start_line = line_;
    size_t start_column = column_;

    std::string text;
    while (!IsAtEnd() && (IsAlphaNumeric(CurrentChar()) || CurrentChar() == '_')) {
        text += CurrentChar();
        Advance();
    }

    // Check for prefixed name (prefix:localName)
    if (!IsAtEnd() && CurrentChar() == ':') {
        text += ':';
        Advance();
        while (!IsAtEnd() && (IsAlphaNumeric(CurrentChar()) || CurrentChar() == '_')) {
            text += CurrentChar();
            Advance();
        }
        return Token(TokenType::PREFIX_LABEL, text, start_line, start_column);
    }

    // Check if it's a keyword (case-insensitive)
    std::string uppercase_text = text;
    for (char& c : uppercase_text) {
        c = std::toupper(static_cast<unsigned char>(c));
    }
    auto keyword_type = LookupKeyword(uppercase_text);
    if (keyword_type.has_value()) {
        return Token(*keyword_type, text, start_line, start_column);
    }

    // Otherwise, it's a prefixed name without ':'
    return Token(TokenType::PREFIX_LABEL, text, start_line, start_column);
}

Token SPARQLTokenizer::ReadOperator() {
    size_t start_line = line_;
    size_t start_column = column_;
    char c = CurrentChar();

    switch (c) {
        case '(': Advance(); return Token(TokenType::LPAREN, "(", start_line, start_column);
        case ')': Advance(); return Token(TokenType::RPAREN, ")", start_line, start_column);
        case '{': Advance(); return Token(TokenType::LBRACE, "{", start_line, start_column);
        case '}': Advance(); return Token(TokenType::RBRACE, "}", start_line, start_column);
        case '[': Advance(); return Token(TokenType::LBRACKET, "[", start_line, start_column);
        case ']': Advance(); return Token(TokenType::RBRACKET, "]", start_line, start_column);
        case '.': Advance(); return Token(TokenType::DOT, ".", start_line, start_column);
        case ';': Advance(); return Token(TokenType::SEMICOLON, ";", start_line, start_column);
        case ',': Advance(); return Token(TokenType::COMMA, ",", start_line, start_column);
        case '+': Advance(); return Token(TokenType::PLUS, "+", start_line, start_column);
        case '-': Advance(); return Token(TokenType::MINUS, "-", start_line, start_column);
        case '*': Advance(); return Token(TokenType::MULTIPLY, "*", start_line, start_column);
        case '/': Advance(); return Token(TokenType::DIVIDE, "/", start_line, start_column);

        case '=':
            Advance();
            return Token(TokenType::EQUAL, "=", start_line, start_column);

        case '!':
            Advance();
            if (!IsAtEnd() && CurrentChar() == '=') {
                Advance();
                return Token(TokenType::NOT_EQUAL, "!=", start_line, start_column);
            }
            return Token(TokenType::NOT, "!", start_line, start_column);

        case '>':
            Advance();
            if (!IsAtEnd() && CurrentChar() == '=') {
                Advance();
                return Token(TokenType::GREATER_EQUAL, ">=", start_line, start_column);
            }
            return Token(TokenType::GREATER_THAN, ">", start_line, start_column);

        case '&':
            Advance();
            if (!IsAtEnd() && CurrentChar() == '&') {
                Advance();
                return Token(TokenType::AND, "&&", start_line, start_column);
            }
            return MakeError("Expected '&&' for AND operator");

        case '|':
            Advance();
            if (!IsAtEnd() && CurrentChar() == '|') {
                Advance();
                return Token(TokenType::OR, "||", start_line, start_column);
            }
            // Single | is property path alternative
            return Token(TokenType::PIPE, "|", start_line, start_column);

        case '?':
            // Could be start of variable or property path quantifier
            // Let the parser context decide
            Advance();
            return Token(TokenType::QUESTION, "?", start_line, start_column);

        case '@':
            Advance();
            return Token(TokenType::LANG_TAG, "@", start_line, start_column);

        case '^':
            Advance();
            if (!IsAtEnd() && CurrentChar() == '^') {
                Advance();
                return Token(TokenType::DATATYPE_MARKER, "^^", start_line, start_column);
            }
            // Single ^ is property path inverse
            return Token(TokenType::CARET, "^", start_line, start_column);

        default:
            return MakeError("Unknown operator: " + std::string(1, c));
    }
}

// Helper functions

bool SPARQLTokenizer::IsWhitespace(char c) const {
    return c == ' ' || c == '\t' || c == '\n' || c == '\r';
}

bool SPARQLTokenizer::IsDigit(char c) const {
    return c >= '0' && c <= '9';
}

bool SPARQLTokenizer::IsAlpha(char c) const {
    return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z');
}

bool SPARQLTokenizer::IsAlphaNumeric(char c) const {
    return IsAlpha(c) || IsDigit(c);
}

bool SPARQLTokenizer::IsKeywordStart(char c) const {
    return IsAlpha(c);
}

Token SPARQLTokenizer::MakeToken(TokenType type, std::string text) {
    return Token(type, std::move(text), line_, column_);
}

Token SPARQLTokenizer::MakeError(std::string message) {
    return Token(TokenType::ERROR, std::move(message), line_, column_);
}

std::optional<TokenType> SPARQLTokenizer::LookupKeyword(const std::string& text) const {
    static const std::unordered_map<std::string, TokenType> keywords = {
        {"PREFIX", TokenType::PREFIX},
        {"BASE", TokenType::BASE},
        {"SELECT", TokenType::SELECT},
        {"ASK", TokenType::ASK},
        {"CONSTRUCT", TokenType::CONSTRUCT},
        {"DESCRIBE", TokenType::DESCRIBE},
        {"WHERE", TokenType::WHERE},
        {"FILTER", TokenType::FILTER},
        {"BIND", TokenType::BIND},
        {"OPTIONAL", TokenType::OPTIONAL},
        {"UNION", TokenType::UNION},
        {"ORDER", TokenType::ORDER},
        {"BY", TokenType::BY},
        {"ASC", TokenType::ASC},
        {"DESC", TokenType::DESC},
        {"DISTINCT", TokenType::DISTINCT},
        {"LIMIT", TokenType::LIMIT},
        {"OFFSET", TokenType::OFFSET},
        {"AS", TokenType::AS},
        {"GROUP", TokenType::GROUP},
        {"GRAPH", TokenType::GRAPH},
        {"FROM", TokenType::FROM},
        {"VALUES", TokenType::VALUES},
        {"MINUS", TokenType::MINUS_KEYWORD},
        {"EXISTS", TokenType::EXISTS},
        {"NOT", TokenType::NOT_KEYWORD},
        {"IN", TokenType::IN},
        {"SEPARATOR", TokenType::SEPARATOR},

        // Built-in functions
        {"BOUND", TokenType::BOUND},
        {"ISIRI", TokenType::ISIRI},
        {"ISLITERAL", TokenType::ISLITERAL},
        {"ISBLANK", TokenType::ISBLANK},
        {"STR", TokenType::STR},
        {"LANG", TokenType::LANG},
        {"DATATYPE", TokenType::DATATYPE},
        {"REGEX", TokenType::REGEX},

        // String functions
        {"STRLEN", TokenType::STRLEN},
        {"SUBSTR", TokenType::SUBSTR},
        {"UCASE", TokenType::UCASE},
        {"LCASE", TokenType::LCASE},
        {"STRSTARTS", TokenType::STRSTARTS},
        {"STRENDS", TokenType::STRENDS},
        {"CONTAINS", TokenType::CONTAINS},
        {"STRBEFORE", TokenType::STRBEFORE},
        {"STRAFTER", TokenType::STRAFTER},
        {"CONCAT", TokenType::CONCAT},
        {"LANGMATCHES", TokenType::LANGMATCHES},
        {"REPLACE", TokenType::REPLACE},
        {"ENCODE_FOR_URI", TokenType::ENCODE_FOR_URI},

        // Math functions
        {"ABS", TokenType::ABS},
        {"ROUND", TokenType::ROUND},
        {"CEIL", TokenType::CEIL},
        {"FLOOR", TokenType::FLOOR},

        // Date/Time functions
        {"NOW", TokenType::NOW},
        {"YEAR", TokenType::YEAR},
        {"MONTH", TokenType::MONTH},
        {"DAY", TokenType::DAY},
        {"HOURS", TokenType::HOURS},
        {"MINUTES", TokenType::MINUTES},
        {"SECONDS", TokenType::SECONDS},
        {"TIMEZONE", TokenType::TIMEZONE},
        {"TZ", TokenType::TZ},

        // Type conversion functions
        {"STRDT", TokenType::STRDT},
        {"STRLANG", TokenType::STRLANG},

        // Hash functions
        {"MD5", TokenType::MD5},
        {"SHA1", TokenType::SHA1},
        {"SHA256", TokenType::SHA256},
        {"SHA384", TokenType::SHA384},
        {"SHA512", TokenType::SHA512},

        // Special/Control functions
        {"IF", TokenType::IF},
        {"COALESCE", TokenType::COALESCE},
        {"BNODE", TokenType::BNODE},
        {"UUID", TokenType::UUID},
        {"STRUUID", TokenType::STRUUID},
        {"IRI", TokenType::IRI},
        {"URI", TokenType::URI},
        {"ISNUMERIC", TokenType::ISNUMERIC},
        {"RAND", TokenType::RAND},

        // Aggregate functions
        {"COUNT", TokenType::COUNT},
        {"SUM", TokenType::SUM},
        {"AVG", TokenType::AVG},
        {"MIN", TokenType::MIN},
        {"MAX", TokenType::MAX},
        {"GROUP_CONCAT", TokenType::GROUP_CONCAT},
        {"SAMPLE", TokenType::SAMPLE},

        // Boolean literals
        {"TRUE", TokenType::BOOLEAN},
        {"FALSE", TokenType::BOOLEAN},
    };

    auto it = keywords.find(text);
    if (it != keywords.end()) {
        return it->second;
    }

    // Try uppercase version
    std::string upper_text = text;
    for (char& c : upper_text) {
        c = std::toupper(c);
    }
    it = keywords.find(upper_text);
    if (it != keywords.end()) {
        return it->second;
    }

    return std::nullopt;
}

// ============================================================================
// SPARQLParser Implementation (to be continued)
// ============================================================================

SPARQLParser::SPARQLParser(std::vector<Token> tokens)
    : tokens_(std::move(tokens)) {}

arrow::Result<Query> SPARQLParser::Parse() {
    Query query;

    // Parse BASE declaration (optional, must come before PREFIX)
    if (Check(TokenType::BASE)) {
        ARROW_ASSIGN_OR_RAISE(query.base_iri, ParseBaseDeclaration());
    }

    // Parse PREFIX declarations (optional, can be multiple)
    while (Check(TokenType::PREFIX)) {
        ARROW_RETURN_NOT_OK(ParsePrefixDeclaration());
    }

    // Parse query based on query type
    if (Check(TokenType::SELECT)) {
        ARROW_ASSIGN_OR_RAISE(auto select_query, ParseSelectQuery());
        query.query_body = std::move(select_query);
    } else if (Check(TokenType::ASK)) {
        ARROW_ASSIGN_OR_RAISE(auto ask_query, ParseAskQuery());
        query.query_body = std::move(ask_query);
    } else if (Check(TokenType::CONSTRUCT)) {
        ARROW_ASSIGN_OR_RAISE(auto construct_query, ParseConstructQuery());
        query.query_body = std::move(construct_query);
    } else if (Check(TokenType::DESCRIBE)) {
        ARROW_ASSIGN_OR_RAISE(auto describe_query, ParseDescribeQuery());
        query.query_body = std::move(describe_query);
    } else {
        return Error("Expected SELECT, ASK, CONSTRUCT, or DESCRIBE");
    }

    return query;
}

// Token navigation

const Token& SPARQLParser::CurrentToken() const {
    if (IsAtEnd()) {
        return tokens_.back(); // Should be END_OF_INPUT
    }
    return tokens_[pos_];
}

const Token& SPARQLParser::PeekToken(size_t offset) const {
    if (pos_ + offset >= tokens_.size()) {
        return tokens_.back();
    }
    return tokens_[pos_ + offset];
}

bool SPARQLParser::IsAtEnd() const {
    if (pos_ >= tokens_.size()) return true;
    // Check END_OF_INPUT token without calling CurrentToken() (avoid recursion)
    if (pos_ < tokens_.size() && tokens_[pos_].type == TokenType::END_OF_INPUT) return true;
    return false;
}

void SPARQLParser::Advance() {
    if (!IsAtEnd()) {
        pos_++;
    }
}

// Token matching

bool SPARQLParser::Match(TokenType type) {
    if (Check(type)) {
        Advance();
        return true;
    }
    return false;
}

bool SPARQLParser::Check(TokenType type) const {
    if (IsAtEnd()) return false;
    return CurrentToken().type == type;
}

arrow::Status SPARQLParser::Expect(TokenType type, const std::string& message) {
    if (!Check(type)) {
        return Error(message);
    }
    Advance();
    return arrow::Status::OK();
}

// Error handling

arrow::Status SPARQLParser::Error(const std::string& message) const {
    const Token& token = CurrentToken();
    std::ostringstream oss;
    oss << "Parse error at line " << token.line << ", column " << token.column
        << ": " << message << " (found: '" << token.text << "')";
    return arrow::Status::Invalid(oss.str());
}

// ============================================================================
// Parser methods for SPARQL grammar
// ============================================================================

arrow::Result<std::string> SPARQLParser::ParseBaseDeclaration() {
    ARROW_RETURN_NOT_OK(Expect(TokenType::BASE, "Expected BASE keyword"));

    // Parse IRI
    if (CurrentToken().type != TokenType::IRI_REF) {
        return Error("Expected IRI after BASE");
    }

    std::string iri = CurrentToken().text;
    // Remove '<' and '>'
    if (iri.size() >= 2 && iri.front() == '<' && iri.back() == '>') {
        iri = iri.substr(1, iri.size() - 2);
    }

    Advance();

    // Store BASE IRI for later expansion
    base_iri_ = iri;

    return iri;
}

arrow::Status SPARQLParser::ParsePrefixDeclaration() {
    ARROW_RETURN_NOT_OK(Expect(TokenType::PREFIX, "Expected PREFIX keyword"));

    // Parse prefix label (e.g., "schema:" or just ":" for empty prefix)
    std::string prefix_label;

    if (CurrentToken().type == TokenType::PREFIX_LABEL) {
        prefix_label = CurrentToken().text;

        // Check if it's just ":" (empty prefix)
        if (prefix_label == ":") {
            prefix_label = "";
        } else {
            // Remove trailing ':' if present
            if (!prefix_label.empty() && prefix_label.back() == ':') {
                prefix_label = prefix_label.substr(0, prefix_label.size() - 1);
            }
        }

        Advance();
    } else if (CurrentToken().type == TokenType::IRI_REF) {
        // Empty prefix without explicit colon (rare case)
        prefix_label = "";
        // Don't advance - IRI parsing will happen next
    } else {
        return Error("Expected prefix label or ':' after PREFIX");
    }


    // Parse IRI
    if (CurrentToken().type != TokenType::IRI_REF) {
        return Error("Expected IRI after prefix label");
    }

    std::string iri = CurrentToken().text;
    // Remove '<' and '>'
    if (iri.size() >= 2 && iri.front() == '<' && iri.back() == '>') {
        iri = iri.substr(1, iri.size() - 2);
    }

    Advance();

    // Store prefix mapping
    prefixes_[prefix_label] = iri;

    return arrow::Status::OK();
}

arrow::Result<std::string> SPARQLParser::ExpandPrefixedName(const std::string& prefixed_name) {
    // Check if it contains ':'
    size_t colon_pos = prefixed_name.find(':');
    if (colon_pos == std::string::npos) {
        // No prefix, return as-is
        return prefixed_name;
    }

    // Split into prefix and local name
    std::string prefix = prefixed_name.substr(0, colon_pos);
    std::string local_name = prefixed_name.substr(colon_pos + 1);

    // Look up prefix
    auto it = prefixes_.find(prefix);
    if (it == prefixes_.end()) {
        std::ostringstream oss;
        oss << "Undefined prefix '" << prefix << "'";
        return arrow::Status::Invalid(oss.str());
    }

    // Expand to full IRI
    return it->second + local_name;
}

std::string SPARQLParser::ExpandRelativeIRI(const std::string& iri) const {
    // If BASE is not defined, return IRI as-is
    if (base_iri_.empty()) {
        return iri;
    }

    // Check if IRI is absolute (contains scheme like "http://")
    // Absolute IRI patterns: scheme:// or scheme:
    size_t colon_pos = iri.find(':');
    if (colon_pos != std::string::npos) {
        // Check if this looks like a scheme (letters before colon)
        bool is_scheme = true;
        for (size_t i = 0; i < colon_pos; ++i) {
            char c = iri[i];
            if (!std::isalnum(c) && c != '+' && c != '-' && c != '.') {
                is_scheme = false;
                break;
            }
        }
        if (is_scheme) {
            // Absolute IRI, return as-is
            return iri;
        }
    }

    // Relative IRI - expand using BASE
    // Simple concatenation (proper IRI resolution would be more complex)
    // Handle BASE ending with / or #
    if (!base_iri_.empty() && (base_iri_.back() == '/' || base_iri_.back() == '#')) {
        return base_iri_ + iri;
    } else {
        // Add separator if needed
        if (!iri.empty() && iri[0] != '/' && iri[0] != '#') {
            return base_iri_ + "/" + iri;
        }
        return base_iri_ + iri;
    }
}

arrow::Result<SelectQuery> SPARQLParser::ParseSelectQuery() {
    SelectQuery query;

    // Parse SELECT clause
    ARROW_ASSIGN_OR_RAISE(query.select, ParseSelectClause());

    // Parse WHERE clause (WHERE keyword is optional if { follows directly)
    if (Match(TokenType::WHERE)) {
        // WHERE keyword present
        ARROW_ASSIGN_OR_RAISE(query.where, ParseWhereClause());
    } else if (Check(TokenType::LBRACE)) {
        // No WHERE keyword, but graph pattern in braces
        ARROW_ASSIGN_OR_RAISE(query.where, ParseWhereClause());
    } else {
        return Error("Expected WHERE keyword or '{' for graph pattern");
    }

    // Parse optional GROUP BY clause
    if (Check(TokenType::GROUP)) {
        ARROW_ASSIGN_OR_RAISE(auto group_by, ParseGroupByClause());
        query.group_by = group_by;
    }

    // Parse optional ORDER BY clause
    if (Check(TokenType::ORDER)) {
        ARROW_ASSIGN_OR_RAISE(query.order_by, ParseOrderByClause());
    }

    // Parse optional LIMIT clause
    if (Match(TokenType::LIMIT)) {
        if (CurrentToken().type != TokenType::INTEGER) {
            return Error("Expected integer after LIMIT");
        }
        query.limit = std::stoull(CurrentToken().text);
        Advance();
    }

    // Parse optional OFFSET clause
    if (Match(TokenType::OFFSET)) {
        if (CurrentToken().type != TokenType::INTEGER) {
            return Error("Expected integer after OFFSET");
        }
        query.offset = std::stoull(CurrentToken().text);
        Advance();
    }

    return query;
}

arrow::Result<AskQuery> SPARQLParser::ParseAskQuery() {
    AskQuery query;

    // Consume ASK keyword
    ARROW_RETURN_NOT_OK(Expect(TokenType::ASK, "Expected ASK keyword"));

    // Parse WHERE clause (WHERE keyword is optional if { follows directly)
    if (Match(TokenType::WHERE)) {
        ARROW_ASSIGN_OR_RAISE(query.where, ParseWhereClause());
    } else if (Check(TokenType::LBRACE)) {
        ARROW_ASSIGN_OR_RAISE(query.where, ParseWhereClause());
    } else {
        return Error("Expected WHERE keyword or '{' for graph pattern");
    }

    return query;
}

arrow::Result<ConstructQuery> SPARQLParser::ParseConstructQuery() {
    ConstructQuery query;

    // Consume CONSTRUCT keyword
    ARROW_RETURN_NOT_OK(Expect(TokenType::CONSTRUCT, "Expected CONSTRUCT keyword"));

    // Skip FROM clauses (we parse them but don't use them yet)
    while (Match(TokenType::FROM)) {
        // Expect IRI for FROM clause
        if (!Check(TokenType::IRI_REF) && !Check(TokenType::PREFIX_LABEL)) {
            return Error("Expected IRI after FROM");
        }
        Advance();  // Consume the IRI
    }

    // Check for CONSTRUCT WHERE shorthand
    if (Check(TokenType::WHERE)) {
        Advance();  // Consume WHERE
        // Parse the pattern
        ARROW_ASSIGN_OR_RAISE(auto pattern, ParseWhereClause());

        // Use pattern as both template and WHERE clause
        query.where = pattern;

        // Convert pattern to construct template (simplified - assumes pattern has triples)
        if (pattern.bgp.has_value()) {
            query.construct_template.triples = pattern.bgp->triples;
        }

        return query;
    }

    // Parse CONSTRUCT template
    ARROW_ASSIGN_OR_RAISE(query.construct_template, ParseConstructTemplate());

    // Parse WHERE clause (WHERE keyword is optional if { follows directly)
    if (Match(TokenType::WHERE)) {
        ARROW_ASSIGN_OR_RAISE(query.where, ParseWhereClause());
    } else if (Check(TokenType::LBRACE)) {
        ARROW_ASSIGN_OR_RAISE(query.where, ParseWhereClause());
    } else {
        return Error("Expected WHERE keyword or '{' for graph pattern");
    }

    return query;
}

arrow::Result<DescribeQuery> SPARQLParser::ParseDescribeQuery() {
    DescribeQuery query;

    // Consume DESCRIBE keyword
    ARROW_RETURN_NOT_OK(Expect(TokenType::DESCRIBE, "Expected DESCRIBE keyword"));

    // Parse one or more resources (IRIs or variables)
    while (!Check(TokenType::WHERE) && !IsAtEnd()) {
        ARROW_ASSIGN_OR_RAISE(auto resource, ParseRDFTerm());
        query.resources.push_back(std::move(resource));

        // Check for more resources (space-separated)
        if (Check(TokenType::WHERE) || IsAtEnd()) {
            break;
        }
    }

    if (query.resources.empty()) {
        return Error("DESCRIBE requires at least one resource");
    }

    // Parse optional WHERE clause
    if (Match(TokenType::WHERE)) {
        ARROW_ASSIGN_OR_RAISE(auto where, ParseWhereClause());
        query.where = std::move(where);
    }

    return query;
}

arrow::Result<ConstructTemplate> SPARQLParser::ParseConstructTemplate() {
    ConstructTemplate tmpl;

    // Expect opening brace
    ARROW_RETURN_NOT_OK(Expect(TokenType::LBRACE, "Expected '{' to start CONSTRUCT template"));

    // Parse triple patterns (same as basic graph pattern)
    while (!Check(TokenType::RBRACE) && !IsAtEnd()) {
        ARROW_ASSIGN_OR_RAISE(auto triple, ParseTriplePattern());
        tmpl.triples.push_back(triple);

        // Store subject and predicate for potential abbreviations
        RDFTerm subject = triple.subject;
        auto predicate = triple.predicate;

        // Handle comma abbreviations: ?S :p ?O1, ?O2 (reuse subject+predicate)
        while (Match(TokenType::COMMA)) {
            ARROW_ASSIGN_OR_RAISE(auto object, ParseRDFTerm());
            if (auto* term = std::get_if<RDFTerm>(&predicate)) {
                tmpl.triples.push_back(TriplePattern(subject, *term, object));
            } else if (auto* path = std::get_if<PropertyPath>(&predicate)) {
                tmpl.triples.push_back(TriplePattern(subject, *path, object));
            }
        }

        // Handle semicolon abbreviations: ?S :p1 ?O1 ; :p2 ?O2 (reuse subject)
        while (Match(TokenType::SEMICOLON)) {
            // Parse additional predicate-object pairs with same subject
            ARROW_ASSIGN_OR_RAISE(auto new_predicate, ParsePredicatePosition());
            ARROW_ASSIGN_OR_RAISE(auto object, ParseRDFTerm());

            // Create new triple with same subject
            if (auto* term = std::get_if<RDFTerm>(&new_predicate)) {
                tmpl.triples.push_back(TriplePattern(subject, *term, object));
            } else if (auto* path = std::get_if<PropertyPath>(&new_predicate)) {
                tmpl.triples.push_back(TriplePattern(subject, *path, object));
            }
        }

        // Consume optional dot
        Match(TokenType::DOT);
    }

    // Expect closing brace
    ARROW_RETURN_NOT_OK(Expect(TokenType::RBRACE, "Expected '}' to end CONSTRUCT template"));

    return tmpl;
}

arrow::Result<SelectClause> SPARQLParser::ParseSelectClause() {
    SelectClause clause;

    ARROW_RETURN_NOT_OK(Expect(TokenType::SELECT, "Expected SELECT keyword"));

    // Check for DISTINCT
    if (Match(TokenType::DISTINCT)) {
        clause.distinct = true;
    }

    // Check for SELECT *
    if (Match(TokenType::MULTIPLY)) {
        // SELECT * - no variables specified
        return clause;
    }

    // Parse variable list or expressions (aggregates or general expressions)
    while (Check(TokenType::VARIABLE) || Check(TokenType::LPAREN)) {
        if (Check(TokenType::LPAREN)) {
            // Parse expression with alias: (expr AS ?alias)
            Advance(); // Consume '('

            // Check if it's an aggregate function
            bool is_aggregate = Check(TokenType::COUNT) || Check(TokenType::SUM) || Check(TokenType::AVG) ||
                                Check(TokenType::MIN) || Check(TokenType::MAX) || Check(TokenType::GROUP_CONCAT) ||
                                Check(TokenType::SAMPLE);

            if (is_aggregate) {
                // Parse aggregate expression: (COUNT(?x) AS ?count)
                ExprOperator agg_op;
                TokenType func_type = CurrentToken().type;
                if (func_type == TokenType::COUNT) {
                    agg_op = ExprOperator::Count;
                } else if (func_type == TokenType::SUM) {
                    agg_op = ExprOperator::Sum;
                } else if (func_type == TokenType::AVG) {
                    agg_op = ExprOperator::Avg;
                } else if (func_type == TokenType::MIN) {
                    agg_op = ExprOperator::Min;
                } else if (func_type == TokenType::MAX) {
                    agg_op = ExprOperator::Max;
                } else if (func_type == TokenType::GROUP_CONCAT) {
                    agg_op = ExprOperator::GroupConcat;
                } else if (func_type == TokenType::SAMPLE) {
                    agg_op = ExprOperator::Sample;
                }

                Advance(); // Consume aggregate function name

                ARROW_RETURN_NOT_OK(Expect(TokenType::LPAREN, "Expected '(' after aggregate function"));

                // Check for DISTINCT modifier (only valid for COUNT)
                bool distinct = false;
                if (agg_op == ExprOperator::Count && Match(TokenType::DISTINCT)) {
                    distinct = true;
                }

                // Parse argument (expression, variable, or * for COUNT)
                auto agg_expr = std::make_shared<Expression>(agg_op);
                if (agg_op == ExprOperator::Count && Check(TokenType::MULTIPLY)) {
                    // COUNT(*) - no argument
                    Advance();
                } else if (!Check(TokenType::RPAREN)) {
                    // Accept full expressions as arguments (not just variables)
                    ARROW_ASSIGN_OR_RAISE(auto arg_expr, ParseExpression());
                    agg_expr->arguments.push_back(arg_expr);
                } else {
                    return Error("Expected expression or * as aggregate function argument");
                }

                // Handle GROUP_CONCAT separator: GROUP_CONCAT(?var ; SEPARATOR="str")
                if (agg_op == ExprOperator::GroupConcat && Match(TokenType::SEMICOLON)) {
                    ARROW_RETURN_NOT_OK(Expect(TokenType::SEPARATOR, "Expected SEPARATOR keyword after semicolon"));
                    ARROW_RETURN_NOT_OK(Expect(TokenType::EQUAL, "Expected '=' after SEPARATOR"));

                    if (!Check(TokenType::STRING_LITERAL)) {
                        return Error("Expected string literal after SEPARATOR=");
                    }
                    ARROW_ASSIGN_OR_RAISE(auto separator_lit, ParseLiteral());
                    auto separator_expr = std::make_shared<Expression>(RDFTerm(separator_lit));
                    agg_expr->arguments.push_back(separator_expr);
                }

                ARROW_RETURN_NOT_OK(Expect(TokenType::RPAREN, "Expected ')' after aggregate function argument"));

                // Parse AS alias
                ARROW_RETURN_NOT_OK(Expect(TokenType::AS, "Expected AS keyword after aggregate function"));

                ARROW_ASSIGN_OR_RAISE(auto alias, ParseVariable());

                // Expect closing ')' for the alias expression: (COUNT(?x) AS ?count)
                ARROW_RETURN_NOT_OK(Expect(TokenType::RPAREN, "Expected ')' to close alias expression"));

                // Create AggregateExpression
                AggregateExpression agg(agg_expr, alias, distinct);
                clause.items.push_back(SelectItem(agg));
            } else {
                // Parse general expression: (expr AS ?alias)
                ARROW_ASSIGN_OR_RAISE(auto expr, ParseExpression());

                // Parse AS alias
                ARROW_RETURN_NOT_OK(Expect(TokenType::AS, "Expected AS keyword after expression"));

                ARROW_ASSIGN_OR_RAISE(auto alias, ParseVariable());

                // Expect closing ')' for the alias expression
                ARROW_RETURN_NOT_OK(Expect(TokenType::RPAREN, "Expected ')' to close projection expression"));

                // Create ProjectionExpression
                ProjectionExpression proj(expr, alias);
                clause.items.push_back(SelectItem(proj));
            }
        } else {
            // Parse simple variable
            ARROW_ASSIGN_OR_RAISE(auto var, ParseVariable());
            clause.items.push_back(SelectItem(var));
        }

        // Commas are optional in SPARQL (space-separated is valid)
        // Continue if there's a comma, or if another variable/aggregate follows
        Match(TokenType::COMMA);  // Optional comma
        // Loop continues if Check(VARIABLE) || Check(LPAREN) at line 701
    }

    if (clause.items.empty()) {
        return Error("Expected variable list or aggregate expressions after SELECT");
    }

    return clause;
}

arrow::Result<QueryPattern> SPARQLParser::ParseWhereClause() {
    QueryPattern pattern;

    ARROW_RETURN_NOT_OK(Expect(TokenType::LBRACE, "Expected '{' to start WHERE clause"));

    // Parse basic graph pattern and optional clauses
    while (!Check(TokenType::RBRACE) && !IsAtEnd()) {
        if (Check(TokenType::FILTER)) {
            // Check for FILTER EXISTS or FILTER NOT EXISTS
            Advance();  // Consume FILTER
            if (Check(TokenType::EXISTS)) {
                ARROW_ASSIGN_OR_RAISE(auto exists, ParseExistsClause(false));
                pattern.exists_patterns.push_back(exists);
            } else if (Check(TokenType::NOT_KEYWORD)) {
                Advance();  // Consume NOT
                ARROW_ASSIGN_OR_RAISE(auto exists, ParseExistsClause(true));
                pattern.exists_patterns.push_back(exists);
            } else {
                // Regular FILTER expression
                ARROW_ASSIGN_OR_RAISE(auto filter_expr, ParseExpression());
                FilterClause filter(filter_expr);
                pattern.filters.push_back(filter);
            }
        } else if (Check(TokenType::BIND)) {
            // Parse BIND clause
            ARROW_ASSIGN_OR_RAISE(auto bind, ParseBindClause());
            pattern.binds.push_back(bind);
        } else if (Check(TokenType::OPTIONAL)) {
            // Parse OPTIONAL clause
            ARROW_ASSIGN_OR_RAISE(auto optional, ParseOptionalClause());
            pattern.optionals.push_back(optional);
        } else if (Check(TokenType::UNION)) {
            // Parse UNION clause
            ARROW_ASSIGN_OR_RAISE(auto union_pattern, ParseUnionClause());
            pattern.unions.push_back(union_pattern);
        } else if (Check(TokenType::VALUES)) {
            // Parse VALUES clause
            ARROW_ASSIGN_OR_RAISE(auto values, ParseValuesClause());
            pattern.values.push_back(values);
        } else if (Check(TokenType::MINUS_KEYWORD)) {
            // Parse MINUS clause
            ARROW_ASSIGN_OR_RAISE(auto minus, ParseMinusClause());
            pattern.minus_patterns.push_back(minus);
        } else if (Check(TokenType::GRAPH)) {
            // Parse GRAPH clause
            ARROW_ASSIGN_OR_RAISE(auto graph, ParseGraphClause());
            pattern.graph_patterns.push_back(graph);
        } else if (Check(TokenType::SELECT)) {
            // Direct subquery without braces: SELECT ... WHERE { ... }
            ARROW_ASSIGN_OR_RAISE(auto subquery_query, ParseSelectQuery());
            SubqueryPattern subquery;
            subquery.query = subquery_query;
            pattern.subqueries.push_back(subquery);
        } else if (Check(TokenType::LBRACE)) {
            // Check if this is a subquery: { SELECT ... }
            // Peek ahead to see if next token is SELECT
            if (PeekToken().type == TokenType::SELECT) {
                ARROW_ASSIGN_OR_RAISE(auto subquery, ParseSubqueryPattern());
                pattern.subqueries.push_back(subquery);
            } else {
                // Group graph pattern: { ... }
                // This creates a nested scope, parse it recursively
                // Don't consume '{' - ParseWhereClause will consume it
                ARROW_ASSIGN_OR_RAISE(auto nested_pattern, ParseWhereClause());

                // Check if this is part of a UNION: { ... } UNION { ... }
                if (Check(TokenType::UNION)) {
                    // This group pattern is the first part of a UNION
                    UnionPattern union_pattern;
                    union_pattern.patterns.push_back(std::make_shared<QueryPattern>(nested_pattern));

                    // Parse all UNION branches
                    while (Match(TokenType::UNION)) {
                        // ParseWhereClause will consume the '{'
                        ARROW_ASSIGN_OR_RAISE(auto union_branch, ParseWhereClause());
                        union_pattern.patterns.push_back(std::make_shared<QueryPattern>(union_branch));
                    }

                    pattern.unions.push_back(union_pattern);
                    continue;
                }

                // Not a UNION, just merge the nested pattern into the current pattern
                // For now, we'll treat it as a separate scope by storing in a Union-like structure
                // But since we don't have a dedicated GroupPattern, we'll inline it
                if (nested_pattern.bgp.has_value()) {
                    if (!pattern.bgp.has_value()) {
                        pattern.bgp = BasicGraphPattern();
                    }
                    for (const auto& triple : nested_pattern.bgp->triples) {
                        pattern.bgp->triples.push_back(triple);
                    }
                }
                // Merge other components
                pattern.filters.insert(pattern.filters.end(), nested_pattern.filters.begin(), nested_pattern.filters.end());
                pattern.binds.insert(pattern.binds.end(), nested_pattern.binds.begin(), nested_pattern.binds.end());
                pattern.optionals.insert(pattern.optionals.end(), nested_pattern.optionals.begin(), nested_pattern.optionals.end());
                pattern.unions.insert(pattern.unions.end(), nested_pattern.unions.begin(), nested_pattern.unions.end());
                pattern.values.insert(pattern.values.end(), nested_pattern.values.begin(), nested_pattern.values.end());
                pattern.minus_patterns.insert(pattern.minus_patterns.end(), nested_pattern.minus_patterns.begin(), nested_pattern.minus_patterns.end());
                pattern.graph_patterns.insert(pattern.graph_patterns.end(), nested_pattern.graph_patterns.begin(), nested_pattern.graph_patterns.end());
                pattern.exists_patterns.insert(pattern.exists_patterns.end(), nested_pattern.exists_patterns.begin(), nested_pattern.exists_patterns.end());
                pattern.subqueries.insert(pattern.subqueries.end(), nested_pattern.subqueries.begin(), nested_pattern.subqueries.end());

                // Group pattern parsed and merged, continue to next statement
            }
        } else {
            // Parse triple pattern
            if (!pattern.bgp.has_value()) {
                pattern.bgp = BasicGraphPattern();
            }

            ARROW_ASSIGN_OR_RAISE(auto triple, ParseTriplePattern());
            pattern.bgp->triples.push_back(triple);

            // Store subject and predicate for potential abbreviations
            RDFTerm subject = triple.subject;
            auto predicate = triple.predicate;

            // Handle comma abbreviations: ?S :p ?O1, ?O2 (reuse subject+predicate)
            while (Match(TokenType::COMMA)) {
                ARROW_ASSIGN_OR_RAISE(auto object, ParseRDFTerm());
                if (auto* term = std::get_if<RDFTerm>(&predicate)) {
                    pattern.bgp->triples.push_back(TriplePattern(subject, *term, object));
                } else if (auto* path = std::get_if<PropertyPath>(&predicate)) {
                    pattern.bgp->triples.push_back(TriplePattern(subject, *path, object));
                }
            }

            // Handle semicolon abbreviations: ?S :p1 ?O1 ; :p2 ?O2 (reuse subject)
            while (Match(TokenType::SEMICOLON)) {
                // Parse additional predicate-object pairs with same subject
                ARROW_ASSIGN_OR_RAISE(auto new_predicate, ParsePredicatePosition());
                ARROW_ASSIGN_OR_RAISE(auto object, ParseRDFTerm());

                // Create new triple with same subject
                if (auto* term = std::get_if<RDFTerm>(&new_predicate)) {
                    pattern.bgp->triples.push_back(TriplePattern(subject, *term, object));
                } else if (auto* path = std::get_if<PropertyPath>(&new_predicate)) {
                    pattern.bgp->triples.push_back(TriplePattern(subject, *path, object));
                }
            }

            // Consume optional '.'
            Match(TokenType::DOT);

            // Check if next token can start a triple pattern
            // If not, continue loop to check for keywords or end of pattern
            if (!Check(TokenType::VARIABLE) && !Check(TokenType::IRI_REF) &&
                !Check(TokenType::PREFIX_LABEL) && !Check(TokenType::BLANK_NODE) &&
                !Check(TokenType::LBRACE)) {
                continue;
            }
        }
    }

    ARROW_RETURN_NOT_OK(Expect(TokenType::RBRACE, "Expected '}' to end WHERE clause"));

    return pattern;
}

arrow::Result<BasicGraphPattern> SPARQLParser::ParseBasicGraphPattern() {
    BasicGraphPattern bgp;

    while (!Check(TokenType::RBRACE) && !Check(TokenType::FILTER) &&
           !Check(TokenType::OPTIONAL) && !Check(TokenType::UNION) &&
           !Check(TokenType::VALUES) && !Check(TokenType::MINUS_KEYWORD) && !IsAtEnd()) {
        ARROW_ASSIGN_OR_RAISE(auto triple, ParseTriplePattern());
        bgp.triples.push_back(triple);

        // Consume optional '.'
        Match(TokenType::DOT);
    }

    return bgp;
}

arrow::Result<TriplePattern> SPARQLParser::ParseTriplePattern() {
    // Parse subject
    ARROW_ASSIGN_OR_RAISE(auto subject, ParseRDFTerm());

    // Parse predicate (can be simple term or property path)
    ARROW_ASSIGN_OR_RAISE(auto predicate, ParsePredicatePosition());

    // Parse object
    ARROW_ASSIGN_OR_RAISE(auto object, ParseRDFTerm());

    // Construct TriplePattern based on predicate type
    if (auto* term = std::get_if<RDFTerm>(&predicate)) {
        return TriplePattern(subject, *term, object);
    } else if (auto* path = std::get_if<PropertyPath>(&predicate)) {
        return TriplePattern(subject, *path, object);
    }

    return Error("Invalid predicate type");
}

arrow::Result<RDFTerm> SPARQLParser::ParseRDFTerm() {
    const Token& token = CurrentToken();

    if (token.type == TokenType::VARIABLE) {
        ARROW_ASSIGN_OR_RAISE(auto var, ParseVariable());
        return RDFTerm(var);
    } else if (token.type == TokenType::IRI_REF) {
        ARROW_ASSIGN_OR_RAISE(auto iri, ParseIRI());
        return RDFTerm(iri);
    } else if (token.type == TokenType::STRING_LITERAL) {
        ARROW_ASSIGN_OR_RAISE(auto lit, ParseLiteral());
        return RDFTerm(lit);
    } else if (token.type == TokenType::INTEGER || token.type == TokenType::DECIMAL) {
        ARROW_ASSIGN_OR_RAISE(auto lit, ParseLiteral());
        return RDFTerm(lit);
    } else if (token.type == TokenType::BOOLEAN) {
        ARROW_ASSIGN_OR_RAISE(auto lit, ParseLiteral());
        return RDFTerm(lit);
    } else if (token.type == TokenType::BLANK_NODE) {
        std::string id = token.text.substr(2); // Remove "_:"
        Advance();
        return RDFTerm(BlankNode(id));
    } else if (token.type == TokenType::LBRACKET) {
        // Blank node with property list: [] or [ pred obj ]
        ARROW_ASSIGN_OR_RAISE(auto bnode, ParseBlankNodePropertyList());
        return RDFTerm(bnode);
    } else if (token.type == TokenType::LPAREN) {
        // RDF collection: (item1 item2 ...)
        Advance();  // Consume '('

        // Generate unique blank node ID for the collection
        std::string collection_id = "c" + std::to_string(blank_node_counter_++);

        // Parse collection items
        auto term_list = std::make_shared<RDFTermList>();
        while (!Check(TokenType::RPAREN) && !IsAtEnd()) {
            ARROW_ASSIGN_OR_RAISE(auto item, ParseRDFTerm());
            term_list->items.push_back(std::move(item));
        }

        ARROW_RETURN_NOT_OK(Expect(TokenType::RPAREN, "Expected ')' to end RDF collection"));

        // Return blank node representing the collection
        return RDFTerm(BlankNode(collection_id, term_list));
    } else if (token.type == TokenType::PREFIX_LABEL) {
        // Expand prefixed name to full IRI
        std::string prefixed_name = token.text;
        Advance();

        ARROW_ASSIGN_OR_RAISE(auto full_iri, ExpandPrefixedName(prefixed_name));
        return RDFTerm(IRI(full_iri));
    }

    return Error("Expected RDF term (variable, IRI, literal, or blank node)");
}

arrow::Result<BlankNode> SPARQLParser::ParseBlankNodePropertyList() {
    // Parse '['
    ARROW_RETURN_NOT_OK(Expect(TokenType::LBRACKET, "Expected '[' to start blank node"));

    // Generate unique blank node ID
    std::string bnode_id = "b" + std::to_string(blank_node_counter_++);
    BlankNode bnode(bnode_id);

    // Check if empty blank node: []
    if (Check(TokenType::RBRACKET)) {
        Advance();  // Consume ']'
        return bnode;
    }

    // Parse property-object pairs: [ pred obj ] or [ pred obj ; pred2 obj2 ]
    // For now, we parse but don't store the triples (TODO: add triples to context)
    while (!Check(TokenType::RBRACKET) && !IsAtEnd()) {
        // Parse predicate
        ARROW_ASSIGN_OR_RAISE(auto predicate, ParsePredicatePosition());

        // Parse object
        ARROW_ASSIGN_OR_RAISE(auto object, ParseRDFTerm());

        // Check for semicolon (multiple property-object pairs)
        if (Match(TokenType::SEMICOLON)) {
            // Continue to parse next property-object pair
            continue;
        }

        // Check for dot (optional separator)
        Match(TokenType::DOT);

        // If next is not semicolon or ']', break
        if (!Check(TokenType::SEMICOLON) && !Check(TokenType::RBRACKET)) {
            break;
        }
    }

    ARROW_RETURN_NOT_OK(Expect(TokenType::RBRACKET, "Expected ']' to end blank node"));

    return bnode;
}

arrow::Result<FilterClause> SPARQLParser::ParseFilterClause() {
    ARROW_RETURN_NOT_OK(Expect(TokenType::FILTER, "Expected FILTER keyword"));

    ARROW_ASSIGN_OR_RAISE(auto expr, ParseExpression());

    return FilterClause(expr);
}

arrow::Result<BindClause> SPARQLParser::ParseBindClause() {
    ARROW_RETURN_NOT_OK(Expect(TokenType::BIND, "Expected BIND keyword"));
    ARROW_RETURN_NOT_OK(Expect(TokenType::LPAREN, "Expected '(' after BIND"));

    // Parse expression
    ARROW_ASSIGN_OR_RAISE(auto expr, ParseExpression());

    // Expect AS keyword
    ARROW_RETURN_NOT_OK(Expect(TokenType::AS, "Expected AS in BIND clause"));

    // Parse variable
    ARROW_ASSIGN_OR_RAISE(auto alias, ParseVariable());

    ARROW_RETURN_NOT_OK(Expect(TokenType::RPAREN, "Expected ')' to close BIND"));

    return BindClause(expr, alias);
}

arrow::Result<std::shared_ptr<Expression>> SPARQLParser::ParseExpression() {
    return ParseOrExpression();
}

arrow::Result<std::shared_ptr<Expression>> SPARQLParser::ParseOrExpression() {
    ARROW_ASSIGN_OR_RAISE(auto left, ParseAndExpression());

    while (Match(TokenType::OR)) {
        ARROW_ASSIGN_OR_RAISE(auto right, ParseAndExpression());

        auto or_expr = std::make_shared<Expression>(ExprOperator::Or);
        or_expr->arguments.push_back(left);
        or_expr->arguments.push_back(right);
        left = or_expr;
    }

    return left;
}

arrow::Result<std::shared_ptr<Expression>> SPARQLParser::ParseAndExpression() {
    ARROW_ASSIGN_OR_RAISE(auto left, ParseComparisonExpression());

    while (Match(TokenType::AND)) {
        ARROW_ASSIGN_OR_RAISE(auto right, ParseComparisonExpression());

        auto and_expr = std::make_shared<Expression>(ExprOperator::And);
        and_expr->arguments.push_back(left);
        and_expr->arguments.push_back(right);
        left = and_expr;
    }

    return left;
}

arrow::Result<std::shared_ptr<Expression>> SPARQLParser::ParseComparisonExpression() {
    ARROW_ASSIGN_OR_RAISE(auto left, ParseAdditiveExpression());

    // Check for comparison operators
    if (Match(TokenType::EQUAL)) {
        ARROW_ASSIGN_OR_RAISE(auto right, ParseAdditiveExpression());
        auto expr = std::make_shared<Expression>(ExprOperator::Equal);
        expr->arguments.push_back(left);
        expr->arguments.push_back(right);
        return expr;
    } else if (Match(TokenType::NOT_EQUAL)) {
        ARROW_ASSIGN_OR_RAISE(auto right, ParseAdditiveExpression());
        auto expr = std::make_shared<Expression>(ExprOperator::NotEqual);
        expr->arguments.push_back(left);
        expr->arguments.push_back(right);
        return expr;
    } else if (Match(TokenType::LESS_THAN)) {
        ARROW_ASSIGN_OR_RAISE(auto right, ParseAdditiveExpression());
        auto expr = std::make_shared<Expression>(ExprOperator::LessThan);
        expr->arguments.push_back(left);
        expr->arguments.push_back(right);
        return expr;
    } else if (Match(TokenType::LESS_EQUAL)) {
        ARROW_ASSIGN_OR_RAISE(auto right, ParseAdditiveExpression());
        auto expr = std::make_shared<Expression>(ExprOperator::LessThanEqual);
        expr->arguments.push_back(left);
        expr->arguments.push_back(right);
        return expr;
    } else if (Match(TokenType::GREATER_THAN)) {
        ARROW_ASSIGN_OR_RAISE(auto right, ParseAdditiveExpression());
        auto expr = std::make_shared<Expression>(ExprOperator::GreaterThan);
        expr->arguments.push_back(left);
        expr->arguments.push_back(right);
        return expr;
    } else if (Match(TokenType::GREATER_EQUAL)) {
        ARROW_ASSIGN_OR_RAISE(auto right, ParseAdditiveExpression());
        auto expr = std::make_shared<Expression>(ExprOperator::GreaterThanEqual);
        expr->arguments.push_back(left);
        expr->arguments.push_back(right);
        return expr;
    } else if (Check(TokenType::NOT_KEYWORD) && PeekToken().type == TokenType::IN) {
        // NOT IN operator
        Advance();  // Consume NOT
        Advance();  // Consume IN
        ARROW_RETURN_NOT_OK(Expect(TokenType::LPAREN, "Expected '(' after NOT IN"));

        auto expr = std::make_shared<Expression>(ExprOperator::NotIn);
        expr->arguments.push_back(left);

        // Parse list of values
        if (!Check(TokenType::RPAREN)) {
            while (true) {
                ARROW_ASSIGN_OR_RAISE(auto value, ParseAdditiveExpression());
                expr->arguments.push_back(value);
                if (!Match(TokenType::COMMA)) {
                    break;
                }
            }
        }

        ARROW_RETURN_NOT_OK(Expect(TokenType::RPAREN, "Expected ')' after IN list"));
        return expr;
    } else if (Match(TokenType::IN)) {
        // IN operator
        ARROW_RETURN_NOT_OK(Expect(TokenType::LPAREN, "Expected '(' after IN"));

        auto expr = std::make_shared<Expression>(ExprOperator::In);
        expr->arguments.push_back(left);

        // Parse list of values
        if (!Check(TokenType::RPAREN)) {
            while (true) {
                ARROW_ASSIGN_OR_RAISE(auto value, ParseAdditiveExpression());
                expr->arguments.push_back(value);
                if (!Match(TokenType::COMMA)) {
                    break;
                }
            }
        }

        ARROW_RETURN_NOT_OK(Expect(TokenType::RPAREN, "Expected ')' after IN list"));
        return expr;
    }

    return left;
}

arrow::Result<std::shared_ptr<Expression>> SPARQLParser::ParseAdditiveExpression() {
    ARROW_ASSIGN_OR_RAISE(auto left, ParseMultiplicativeExpression());

    while (Match(TokenType::PLUS) || Match(TokenType::MINUS)) {
        TokenType op = tokens_[pos_ - 1].type;
        ARROW_ASSIGN_OR_RAISE(auto right, ParseMultiplicativeExpression());

        auto expr = std::make_shared<Expression>(
            op == TokenType::PLUS ? ExprOperator::Plus : ExprOperator::Minus
        );
        expr->arguments.push_back(left);
        expr->arguments.push_back(right);
        left = expr;
    }

    return left;
}

arrow::Result<std::shared_ptr<Expression>> SPARQLParser::ParseMultiplicativeExpression() {
    ARROW_ASSIGN_OR_RAISE(auto left, ParseUnaryExpression());

    while (Match(TokenType::MULTIPLY) || Match(TokenType::DIVIDE)) {
        TokenType op = tokens_[pos_ - 1].type;
        ARROW_ASSIGN_OR_RAISE(auto right, ParseUnaryExpression());

        auto expr = std::make_shared<Expression>(
            op == TokenType::MULTIPLY ? ExprOperator::Multiply : ExprOperator::Divide
        );
        expr->arguments.push_back(left);
        expr->arguments.push_back(right);
        left = expr;
    }

    return left;
}

arrow::Result<std::shared_ptr<Expression>> SPARQLParser::ParseUnaryExpression() {
    if (Match(TokenType::NOT)) {
        ARROW_ASSIGN_OR_RAISE(auto operand, ParseUnaryExpression());
        auto expr = std::make_shared<Expression>(ExprOperator::Not);
        expr->arguments.push_back(operand);
        return expr;
    }

    return ParsePrimaryExpression();
}

arrow::Result<std::shared_ptr<Expression>> SPARQLParser::ParsePrimaryExpression() {
    // Parenthesized expression
    if (Match(TokenType::LPAREN)) {
        ARROW_ASSIGN_OR_RAISE(auto expr, ParseExpression());
        ARROW_RETURN_NOT_OK(Expect(TokenType::RPAREN, "Expected ')' after expression"));
        return expr;
    }

    // EXISTS or NOT EXISTS expression (special case - takes graph pattern, not args)
    if (Check(TokenType::EXISTS)) {
        ARROW_ASSIGN_OR_RAISE(auto exists_pattern, ParseExistsClause(false));
        auto expr = std::make_shared<Expression>(ExprOperator::Exists);
        expr->exists_pattern = exists_pattern.pattern;
        return expr;
    }
    if (Check(TokenType::NOT_KEYWORD) && PeekToken().type == TokenType::EXISTS) {
        Advance();  // Consume NOT
        ARROW_ASSIGN_OR_RAISE(auto exists_pattern, ParseExistsClause(true));
        auto expr = std::make_shared<Expression>(ExprOperator::NotExists);
        expr->exists_pattern = exists_pattern.pattern;
        return expr;
    }

    // Built-in function call
    if (Check(TokenType::BOUND) || Check(TokenType::ISIRI) || Check(TokenType::ISLITERAL) ||
        Check(TokenType::ISBLANK) || Check(TokenType::STR) || Check(TokenType::LANG) ||
        Check(TokenType::DATATYPE) || Check(TokenType::REGEX) ||
        // String functions
        Check(TokenType::STRLEN) || Check(TokenType::SUBSTR) || Check(TokenType::UCASE) ||
        Check(TokenType::LCASE) || Check(TokenType::STRSTARTS) || Check(TokenType::STRENDS) ||
        Check(TokenType::CONTAINS) || Check(TokenType::STRBEFORE) || Check(TokenType::STRAFTER) ||
        Check(TokenType::CONCAT) || Check(TokenType::LANGMATCHES) || Check(TokenType::REPLACE) ||
        Check(TokenType::ENCODE_FOR_URI) ||
        // Math functions
        Check(TokenType::ABS) || Check(TokenType::ROUND) || Check(TokenType::CEIL) ||
        Check(TokenType::FLOOR) ||
        // Date/Time functions
        Check(TokenType::NOW) || Check(TokenType::YEAR) || Check(TokenType::MONTH) ||
        Check(TokenType::DAY) || Check(TokenType::HOURS) || Check(TokenType::MINUTES) ||
        Check(TokenType::SECONDS) || Check(TokenType::TIMEZONE) || Check(TokenType::TZ) ||
        // Type conversion functions
        Check(TokenType::STRDT) || Check(TokenType::STRLANG) ||
        // Hash functions
        Check(TokenType::MD5) || Check(TokenType::SHA1) || Check(TokenType::SHA256) ||
        Check(TokenType::SHA384) || Check(TokenType::SHA512) ||
        // Special/Control functions
        Check(TokenType::IF) || Check(TokenType::COALESCE) || Check(TokenType::BNODE) ||
        Check(TokenType::UUID) || Check(TokenType::STRUUID) || Check(TokenType::IRI) ||
        Check(TokenType::URI) || Check(TokenType::ISNUMERIC) || Check(TokenType::RAND) ||
        // Aggregate functions (can be used in expressions)
        Check(TokenType::COUNT) || Check(TokenType::SUM) || Check(TokenType::AVG) ||
        Check(TokenType::MIN) || Check(TokenType::MAX) || Check(TokenType::GROUP_CONCAT) ||
        Check(TokenType::SAMPLE)) {
        return ParseBuiltInCall();
    }

    // Type conversion function call: xsd:double(?x), ex:myFunction(?y)
    // This is PREFIX_LABEL followed by LPAREN
    if (Check(TokenType::PREFIX_LABEL) && PeekToken().type == TokenType::LPAREN) {
        std::string prefixed_name = CurrentToken().text;
        Advance();  // Consume PREFIX_LABEL

        // Expand to full IRI
        ARROW_ASSIGN_OR_RAISE(auto full_iri, ExpandPrefixedName(prefixed_name));

        // Parse function call with IRI as the function
        ARROW_RETURN_NOT_OK(Expect(TokenType::LPAREN, "Expected '(' after IRI"));

        auto expr = std::make_shared<Expression>(ExprOperator::FunctionCall);
        expr->function_iri = full_iri;

        // Parse arguments
        if (!Check(TokenType::RPAREN)) {
            while (true) {
                ARROW_ASSIGN_OR_RAISE(auto arg, ParseExpression());
                expr->arguments.push_back(arg);

                if (!Match(TokenType::COMMA)) {
                    break;
                }
            }
        }

        ARROW_RETURN_NOT_OK(Expect(TokenType::RPAREN, "Expected ')' after function arguments"));
        return expr;
    }

    // Literal value
    ARROW_ASSIGN_OR_RAISE(auto term, ParseRDFTerm());
    return std::make_shared<Expression>(term);
}

arrow::Result<std::shared_ptr<Expression>> SPARQLParser::ParseBuiltInCall() {
    const Token& func_token = CurrentToken();
    ExprOperator op;

    // Original built-in functions
    if (func_token.type == TokenType::BOUND) {
        op = ExprOperator::Bound;
    } else if (func_token.type == TokenType::ISIRI) {
        op = ExprOperator::IsIRI;
    } else if (func_token.type == TokenType::ISLITERAL) {
        op = ExprOperator::IsLiteral;
    } else if (func_token.type == TokenType::ISBLANK) {
        op = ExprOperator::IsBlank;
    } else if (func_token.type == TokenType::STR) {
        op = ExprOperator::Str;
    } else if (func_token.type == TokenType::LANG) {
        op = ExprOperator::Lang;
    } else if (func_token.type == TokenType::DATATYPE) {
        op = ExprOperator::Datatype;
    } else if (func_token.type == TokenType::REGEX) {
        op = ExprOperator::Regex;

    // String functions
    } else if (func_token.type == TokenType::STRLEN) {
        op = ExprOperator::StrLen;
    } else if (func_token.type == TokenType::SUBSTR) {
        op = ExprOperator::SubStr;
    } else if (func_token.type == TokenType::UCASE) {
        op = ExprOperator::UCase;
    } else if (func_token.type == TokenType::LCASE) {
        op = ExprOperator::LCase;
    } else if (func_token.type == TokenType::STRSTARTS) {
        op = ExprOperator::StrStarts;
    } else if (func_token.type == TokenType::STRENDS) {
        op = ExprOperator::StrEnds;
    } else if (func_token.type == TokenType::CONTAINS) {
        op = ExprOperator::Contains;
    } else if (func_token.type == TokenType::STRBEFORE) {
        op = ExprOperator::StrBefore;
    } else if (func_token.type == TokenType::STRAFTER) {
        op = ExprOperator::StrAfter;
    } else if (func_token.type == TokenType::CONCAT) {
        op = ExprOperator::Concat;
    } else if (func_token.type == TokenType::LANGMATCHES) {
        op = ExprOperator::LangMatches;
    } else if (func_token.type == TokenType::REPLACE) {
        op = ExprOperator::Replace;
    } else if (func_token.type == TokenType::ENCODE_FOR_URI) {
        op = ExprOperator::EncodeForURI;

    // Math functions
    } else if (func_token.type == TokenType::ABS) {
        op = ExprOperator::Abs;
    } else if (func_token.type == TokenType::ROUND) {
        op = ExprOperator::Round;
    } else if (func_token.type == TokenType::CEIL) {
        op = ExprOperator::Ceil;
    } else if (func_token.type == TokenType::FLOOR) {
        op = ExprOperator::Floor;

    // Date/Time functions
    } else if (func_token.type == TokenType::NOW) {
        op = ExprOperator::Now;
    } else if (func_token.type == TokenType::YEAR) {
        op = ExprOperator::Year;
    } else if (func_token.type == TokenType::MONTH) {
        op = ExprOperator::Month;
    } else if (func_token.type == TokenType::DAY) {
        op = ExprOperator::Day;
    } else if (func_token.type == TokenType::HOURS) {
        op = ExprOperator::Hours;
    } else if (func_token.type == TokenType::MINUTES) {
        op = ExprOperator::Minutes;
    } else if (func_token.type == TokenType::SECONDS) {
        op = ExprOperator::Seconds;
    } else if (func_token.type == TokenType::TIMEZONE) {
        op = ExprOperator::Timezone;
    } else if (func_token.type == TokenType::TZ) {
        op = ExprOperator::Tz;

    // Type conversion functions
    } else if (func_token.type == TokenType::STRDT) {
        op = ExprOperator::StrDt;
    } else if (func_token.type == TokenType::STRLANG) {
        op = ExprOperator::StrLang;

    // Hash functions
    } else if (func_token.type == TokenType::MD5) {
        op = ExprOperator::MD5;
    } else if (func_token.type == TokenType::SHA1) {
        op = ExprOperator::SHA1;
    } else if (func_token.type == TokenType::SHA256) {
        op = ExprOperator::SHA256;
    } else if (func_token.type == TokenType::SHA384) {
        op = ExprOperator::SHA384;
    } else if (func_token.type == TokenType::SHA512) {
        op = ExprOperator::SHA512;

    // Special/Control functions
    } else if (func_token.type == TokenType::IF) {
        op = ExprOperator::If;
    } else if (func_token.type == TokenType::COALESCE) {
        op = ExprOperator::Coalesce;
    } else if (func_token.type == TokenType::BNODE) {
        op = ExprOperator::BNode;
    } else if (func_token.type == TokenType::UUID) {
        op = ExprOperator::UUID;
    } else if (func_token.type == TokenType::STRUUID) {
        op = ExprOperator::StrUUID;
    } else if (func_token.type == TokenType::IRI) {
        op = ExprOperator::IRI;
    } else if (func_token.type == TokenType::URI) {
        op = ExprOperator::IRI;  // URI is an alias for IRI
    } else if (func_token.type == TokenType::ISNUMERIC) {
        op = ExprOperator::IsNumeric;
    } else if (func_token.type == TokenType::RAND) {
        op = ExprOperator::Rand;

    // Aggregate functions
    } else if (func_token.type == TokenType::COUNT) {
        op = ExprOperator::Count;
    } else if (func_token.type == TokenType::SUM) {
        op = ExprOperator::Sum;
    } else if (func_token.type == TokenType::AVG) {
        op = ExprOperator::Avg;
    } else if (func_token.type == TokenType::MIN) {
        op = ExprOperator::Min;
    } else if (func_token.type == TokenType::MAX) {
        op = ExprOperator::Max;
    } else if (func_token.type == TokenType::GROUP_CONCAT) {
        op = ExprOperator::GroupConcat;
    } else if (func_token.type == TokenType::SAMPLE) {
        op = ExprOperator::Sample;

    } else {
        return Error("Unknown built-in function");
    }

    Advance(); // Consume function name

    ARROW_RETURN_NOT_OK(Expect(TokenType::LPAREN, "Expected '(' after function name"));

    auto expr = std::make_shared<Expression>(op);

    // Handle DISTINCT modifier for aggregates
    if (op == ExprOperator::Count || op == ExprOperator::Sum || op == ExprOperator::Avg ||
        op == ExprOperator::Min || op == ExprOperator::Max || op == ExprOperator::GroupConcat ||
        op == ExprOperator::Sample) {
        Match(TokenType::DISTINCT);  // Optional DISTINCT, ignore for now
    }

    // Handle COUNT(*) special case
    if (op == ExprOperator::Count && Check(TokenType::MULTIPLY)) {
        Advance();  // Consume *
        // COUNT(*) has no arguments
    } else if (!Check(TokenType::RPAREN)) {
        // Parse function arguments (now accepts full expressions!)
        while (true) {
            ARROW_ASSIGN_OR_RAISE(auto arg, ParseExpression());
            expr->arguments.push_back(arg);

            if (!Match(TokenType::COMMA)) {
                break;
            }
        }

        // Handle GROUP_CONCAT SEPARATOR
        if (op == ExprOperator::GroupConcat && Match(TokenType::SEMICOLON)) {
            ARROW_RETURN_NOT_OK(Expect(TokenType::SEPARATOR, "Expected SEPARATOR after semicolon"));
            ARROW_RETURN_NOT_OK(Expect(TokenType::EQUAL, "Expected '=' after SEPARATOR"));
            if (!Check(TokenType::STRING_LITERAL)) {
                return Error("Expected string literal after SEPARATOR=");
            }
            ARROW_ASSIGN_OR_RAISE(auto separator_lit, ParseLiteral());
            auto separator_expr = std::make_shared<Expression>(RDFTerm(separator_lit));
            expr->arguments.push_back(separator_expr);
        }
    }

    ARROW_RETURN_NOT_OK(Expect(TokenType::RPAREN, "Expected ')' after function arguments"));

    return expr;
}

arrow::Result<OptionalPattern> SPARQLParser::ParseOptionalClause() {
    ARROW_RETURN_NOT_OK(Expect(TokenType::OPTIONAL, "Expected OPTIONAL keyword"));

    ARROW_RETURN_NOT_OK(Expect(TokenType::LBRACE, "Expected '{' after OPTIONAL"));

    auto pattern = std::make_shared<QueryPattern>();

    // Parse BGP and filters inside OPTIONAL
    while (!Check(TokenType::RBRACE) && !IsAtEnd()) {
        if (Check(TokenType::FILTER)) {
            ARROW_ASSIGN_OR_RAISE(auto filter, ParseFilterClause());
            pattern->filters.push_back(filter);
        } else {
            if (!pattern->bgp.has_value()) {
                pattern->bgp = BasicGraphPattern();
            }

            ARROW_ASSIGN_OR_RAISE(auto triple, ParseTriplePattern());
            pattern->bgp->triples.push_back(triple);

            Match(TokenType::DOT);
        }
    }

    ARROW_RETURN_NOT_OK(Expect(TokenType::RBRACE, "Expected '}' to end OPTIONAL"));

    return OptionalPattern(pattern);
}

arrow::Result<UnionPattern> SPARQLParser::ParseUnionClause() {
    // Parse first pattern
    ARROW_RETURN_NOT_OK(Expect(TokenType::LBRACE, "Expected '{' for first UNION pattern"));
    ARROW_ASSIGN_OR_RAISE(auto pattern1_where, ParseWhereClause());
    auto pattern1 = std::make_shared<QueryPattern>(pattern1_where);

    ARROW_RETURN_NOT_OK(Expect(TokenType::UNION, "Expected UNION keyword"));

    // Parse second pattern
    ARROW_RETURN_NOT_OK(Expect(TokenType::LBRACE, "Expected '{' for second UNION pattern"));
    ARROW_ASSIGN_OR_RAISE(auto pattern2_where, ParseWhereClause());
    auto pattern2 = std::make_shared<QueryPattern>(pattern2_where);

    UnionPattern union_pattern;
    union_pattern.patterns.push_back(pattern1);
    union_pattern.patterns.push_back(pattern2);

    // Handle additional UNION patterns
    while (Match(TokenType::UNION)) {
        ARROW_RETURN_NOT_OK(Expect(TokenType::LBRACE, "Expected '{' for additional UNION pattern"));
        ARROW_ASSIGN_OR_RAISE(auto pattern_where, ParseWhereClause());
        auto pattern = std::make_shared<QueryPattern>(pattern_where);
        union_pattern.patterns.push_back(pattern);
    }

    return union_pattern;
}

arrow::Result<ValuesClause> SPARQLParser::ParseValuesClause() {
    ValuesClause values;

    ARROW_RETURN_NOT_OK(Expect(TokenType::VALUES, "Expected VALUES keyword"));

    // Check for inline syntax: VALUES ?var { val1 val2 }
    bool inline_syntax = Check(TokenType::VARIABLE);

    if (inline_syntax) {
        // Inline syntax: single variable without parentheses
        ARROW_ASSIGN_OR_RAISE(auto var, ParseVariable());
        values.variables.push_back(std::move(var));

        // Parse data values in braces (without parentheses around each value)
        ARROW_RETURN_NOT_OK(Expect(TokenType::LBRACE, "Expected '{' for VALUES data"));

        while (!Check(TokenType::RBRACE) && !IsAtEnd()) {
            ARROW_ASSIGN_OR_RAISE(auto term, ParseRDFTerm());
            values.rows.push_back({std::move(term)});
        }

        ARROW_RETURN_NOT_OK(Expect(TokenType::RBRACE, "Expected '}' after VALUES data"));
    } else {
        // Standard syntax: VALUES (?var1 ?var2) { (val1 val2) }
        ARROW_RETURN_NOT_OK(Expect(TokenType::LPAREN, "Expected '(' after VALUES"));

        while (!Check(TokenType::RPAREN) && !IsAtEnd()) {
            ARROW_ASSIGN_OR_RAISE(auto var, ParseVariable());
            values.variables.push_back(std::move(var));
        }

        ARROW_RETURN_NOT_OK(Expect(TokenType::RPAREN, "Expected ')' after variable list"));

        // Parse data rows in braces
        ARROW_RETURN_NOT_OK(Expect(TokenType::LBRACE, "Expected '{' for VALUES data"));

        while (!Check(TokenType::RBRACE) && !IsAtEnd()) {
            // Each row is in parentheses
            ARROW_RETURN_NOT_OK(Expect(TokenType::LPAREN, "Expected '(' for data row"));

            std::vector<RDFTerm> row;
            while (!Check(TokenType::RPAREN) && !IsAtEnd()) {
                ARROW_ASSIGN_OR_RAISE(auto term, ParseRDFTerm());
                row.push_back(std::move(term));
            }

            ARROW_RETURN_NOT_OK(Expect(TokenType::RPAREN, "Expected ')' after data row"));

            if (row.size() != values.variables.size()) {
                return Error("VALUES data row size does not match variable count");
            }

            values.rows.push_back(std::move(row));
        }

        ARROW_RETURN_NOT_OK(Expect(TokenType::RBRACE, "Expected '}' after VALUES data"));
    }

    return values;
}

arrow::Result<MinusPattern> SPARQLParser::ParseMinusClause() {
    MinusPattern minus;

    ARROW_RETURN_NOT_OK(Expect(TokenType::MINUS_KEYWORD, "Expected MINUS keyword"));

    // Parse pattern in braces (ParseWhereClause expects and consumes braces)
    ARROW_ASSIGN_OR_RAISE(auto pattern_where, ParseWhereClause());
    minus.pattern = std::make_shared<QueryPattern>(pattern_where);

    return minus;
}

arrow::Result<GraphPattern> SPARQLParser::ParseGraphClause() {
    ARROW_RETURN_NOT_OK(Expect(TokenType::GRAPH, "Expected GRAPH keyword"));

    // Parse graph term (variable or IRI)
    ARROW_ASSIGN_OR_RAISE(auto graph_term, ParseRDFTerm());

    // Parse pattern in braces (ParseWhereClause expects and consumes braces)
    ARROW_ASSIGN_OR_RAISE(auto pattern_where, ParseWhereClause());

    return GraphPattern(graph_term, std::make_shared<QueryPattern>(pattern_where));
}

arrow::Result<ExistsPattern> SPARQLParser::ParseExistsClause(bool negated) {
    ExistsPattern exists;
    exists.is_negated = negated;

    // Expect FILTER keyword (already consumed by caller)
    // Expect EXISTS keyword (or already consumed NOT)
    ARROW_RETURN_NOT_OK(Expect(TokenType::EXISTS, "Expected EXISTS keyword"));

    // Parse pattern in braces (ParseWhereClause expects and consumes braces)
    ARROW_ASSIGN_OR_RAISE(auto pattern_where, ParseWhereClause());
    exists.pattern = std::make_shared<QueryPattern>(pattern_where);

    return exists;
}

arrow::Result<SubqueryPattern> SPARQLParser::ParseSubqueryPattern() {
    SubqueryPattern subquery;

    // Consume opening brace
    ARROW_RETURN_NOT_OK(Expect(TokenType::LBRACE, "Expected '{' to start subquery"));

    // Parse SELECT query
    ARROW_ASSIGN_OR_RAISE(subquery.query, ParseSelectQuery());

    // Consume closing brace
    ARROW_RETURN_NOT_OK(Expect(TokenType::RBRACE, "Expected '}' to end subquery"));

    return subquery;
}

arrow::Result<std::vector<OrderBy>> SPARQLParser::ParseOrderByClause() {
    std::vector<OrderBy> order_by_list;

    ARROW_RETURN_NOT_OK(Expect(TokenType::ORDER, "Expected ORDER keyword"));
    ARROW_RETURN_NOT_OK(Expect(TokenType::BY, "Expected BY keyword after ORDER"));

    while (Check(TokenType::ASC) || Check(TokenType::DESC) || Check(TokenType::VARIABLE)) {
        OrderDirection direction = OrderDirection::Ascending;

        if (Match(TokenType::ASC)) {
            ARROW_RETURN_NOT_OK(Expect(TokenType::LPAREN, "Expected '(' after ASC"));
            direction = OrderDirection::Ascending;
        } else if (Match(TokenType::DESC)) {
            ARROW_RETURN_NOT_OK(Expect(TokenType::LPAREN, "Expected '(' after DESC"));
            direction = OrderDirection::Descending;
        }

        ARROW_ASSIGN_OR_RAISE(auto var, ParseVariable());

        if (direction != OrderDirection::Ascending || tokens_[pos_ - 2].type == TokenType::ASC) {
            ARROW_RETURN_NOT_OK(Expect(TokenType::RPAREN, "Expected ')' after variable"));
        }

        order_by_list.push_back(OrderBy(var, direction));

        // Continue if there's more order clauses
        if (!Check(TokenType::ASC) && !Check(TokenType::DESC) && !Check(TokenType::VARIABLE)) {
            break;
        }
    }

    if (order_by_list.empty()) {
        return Error("Expected at least one ORDER BY clause");
    }

    return order_by_list;
}

arrow::Result<GroupByClause> SPARQLParser::ParseGroupByClause() {
    GroupByClause clause;

    ARROW_RETURN_NOT_OK(Expect(TokenType::GROUP, "Expected GROUP keyword"));
    ARROW_RETURN_NOT_OK(Expect(TokenType::BY, "Expected BY keyword after GROUP"));

    // Parse comma-separated list of variables or expressions
    while (Check(TokenType::VARIABLE) || Check(TokenType::LPAREN)) {
        if (Check(TokenType::LPAREN)) {
            // Parse (expression) or (expression AS ?alias)
            Advance();  // Consume '('
            ARROW_ASSIGN_OR_RAISE(auto expr, ParseExpression());

            // AS clause is optional
            if (Match(TokenType::AS)) {
                ARROW_ASSIGN_OR_RAISE(auto alias, ParseVariable());
                ARROW_RETURN_NOT_OK(Expect(TokenType::RPAREN, "Expected ')' after GROUP BY alias"));
                clause.items.push_back(GroupByItem(expr, alias));
            } else {
                ARROW_RETURN_NOT_OK(Expect(TokenType::RPAREN, "Expected ')' after GROUP BY expression"));
                // Expression without alias - create GroupByItem with just the expression
                GroupByItem item;
                item.expression = expr;
                clause.items.push_back(item);
            }
        } else {
            // Parse simple variable
            ARROW_ASSIGN_OR_RAISE(auto var, ParseVariable());
            clause.variables.push_back(var);
            clause.items.push_back(GroupByItem(var));
        }

        // Continue if there's a comma
        if (!Match(TokenType::COMMA)) {
            break;
        }
    }

    if (clause.variables.empty() && clause.items.empty()) {
        return Error("Expected at least one variable or expression in GROUP BY clause");
    }

    return clause;
}

// Helper methods

arrow::Result<Variable> SPARQLParser::ParseVariable() {
    const Token& token = CurrentToken();
    if (token.type != TokenType::VARIABLE) {
        return Error("Expected variable");
    }

    // Remove '?' or '$' prefix
    std::string name = token.text.substr(1);
    Advance();

    return Variable(name);
}

arrow::Result<IRI> SPARQLParser::ParseIRI() {
    const Token& token = CurrentToken();
    if (token.type != TokenType::IRI_REF) {
        return Error("Expected IRI");
    }

    // Remove '<' and '>'
    std::string iri = token.text.substr(1, token.text.size() - 2);

    // Expand relative IRI if BASE is defined
    iri = ExpandRelativeIRI(iri);

    Advance();

    return IRI(iri);
}

arrow::Result<Literal> SPARQLParser::ParseLiteral() {
    const Token& token = CurrentToken();

    if (token.type == TokenType::STRING_LITERAL) {
        std::string value = token.text;
        Advance();

        // Check for language tag or datatype
        std::string language;
        std::string datatype;

        if (Match(TokenType::LANG_TAG)) {
            // Language tag: @en
            if (CurrentToken().type != TokenType::PREFIX_LABEL) {
                return Error("Expected language tag after '@'");
            }
            language = CurrentToken().text;
            Advance();
        } else if (Match(TokenType::DATATYPE_MARKER)) {
            // Datatype: ^^<http://www.w3.org/2001/XMLSchema#integer> or ^^xsd:integer
            if (CurrentToken().type == TokenType::IRI_REF) {
                ARROW_ASSIGN_OR_RAISE(auto dt_iri, ParseIRI());
                datatype = dt_iri.iri;
            } else if (CurrentToken().type == TokenType::PREFIX_LABEL) {
                std::string prefixed_name = CurrentToken().text;
                Advance();
                // Expand prefixed name to full IRI
                ARROW_ASSIGN_OR_RAISE(datatype, ExpandPrefixedName(prefixed_name));
            } else {
                return Error("Expected IRI after '^^'");
            }
        }

        return Literal(value, language, datatype);
    } else if (token.type == TokenType::INTEGER) {
        std::string value = token.text;
        Advance();
        return Literal(value, "", "http://www.w3.org/2001/XMLSchema#integer");
    } else if (token.type == TokenType::DECIMAL) {
        std::string value = token.text;
        Advance();
        return Literal(value, "", "http://www.w3.org/2001/XMLSchema#decimal");
    } else if (token.type == TokenType::BOOLEAN) {
        std::string value = token.text;
        Advance();
        return Literal(value, "", "http://www.w3.org/2001/XMLSchema#boolean");
    }

    return Error("Expected literal value");
}

arrow::Result<PredicatePosition> SPARQLParser::ParsePredicatePosition() {
    // Check if this is a property path or simple term
    // Property paths can start with:
    // - ^ (inverse)
    // - ! (negated)
    // - ( (grouped path)
    // - Regular IRI/variable followed by path operators (/, |, *, +, ?)

    const Token& token = CurrentToken();

    // Check for property path indicators
    if (token.type == TokenType::CARET || token.type == TokenType::NOT ||
        token.type == TokenType::LPAREN) {
        // Definitely a property path
        ARROW_ASSIGN_OR_RAISE(auto path, ParsePropertyPath());
        return PredicatePosition(path);
    }

    // Try to parse as simple term first
    size_t saved_pos = pos_;
    auto term_result = ParseRDFTerm();
    if (!term_result.ok()) {
        return term_result.status();
    }
    auto term = *term_result;

    // Check if followed by property path operators
    if (Check(TokenType::DIVIDE) || Check(TokenType::PIPE) ||
        Check(TokenType::MULTIPLY) || Check(TokenType::PLUS) ||
        Check(TokenType::QUESTION) || Check(TokenType::LBRACE)) {
        // It's a property path - backtrack and parse as path
        pos_ = saved_pos;
        ARROW_ASSIGN_OR_RAISE(auto path, ParsePropertyPath());
        return PredicatePosition(path);
    }

    // It's just a simple term
    return PredicatePosition(term);
}

// ============================================================================
// Property Path Parsing with Shunting Yard Algorithm
// ============================================================================

// Internal token type for property path parsing with operator precedence
struct PathToken {
    enum Type {
        OPERAND,        // IRI, variable, or literal
        BINARY_OP,      // / (sequence) or | (alternative)
        UNARY_PREFIX,   // ^ (inverse) or ! (negation)
        UNARY_POSTFIX,  // * + ? {n,m} (quantifiers)
        LPAREN,         // (
        RPAREN          // )
    };

    Type type;
    std::string value;
    int precedence;  // Higher number = tighter binding

    // For OPERAND type
    RDFTerm term;

    // For UNARY_POSTFIX type (quantifiers)
    PropertyPathQuantifier quantifier_type = PropertyPathQuantifier::None;
    int min_count = 0;
    int max_count = 0;

    PathToken(Type t, std::string v, int prec)
        : type(t), value(std::move(v)), precedence(prec) {}

    PathToken(RDFTerm t)
        : type(OPERAND), precedence(0), term(std::move(t)) {}

    bool IsOperator() const {
        return type == BINARY_OP || type == UNARY_PREFIX || type == UNARY_POSTFIX;
    }
};

// Friend function to tokenize property path - needs parser context
// This is in the .cpp to keep PathToken internal
arrow::Result<std::vector<PathToken>> TokenizePropertyPathTokens(SPARQLParser& parser) {
    std::vector<PathToken> path_tokens;
    bool expecting_operand = true;  // Start by expecting an operand

    while (!parser.IsAtEnd()) {
        const Token& token = parser.CurrentToken();

        // Stop conditions: tokens that can't be part of a property path
        if (token.type == TokenType::DOT || token.type == TokenType::SEMICOLON ||
            token.type == TokenType::RBRACE || token.type == TokenType::WHERE ||
            token.type == TokenType::FILTER || token.type == TokenType::BIND ||
            token.type == TokenType::OPTIONAL || token.type == TokenType::UNION ||
            token.type == TokenType::VALUES || token.type == TokenType::ORDER ||
            token.type == TokenType::GROUP || token.type == TokenType::LIMIT ||
            token.type == TokenType::OFFSET || token.type == TokenType::SELECT) {
            break;
        }

        // Operands: IRI, variable, prefixed name
        if (token.type == TokenType::IRI_REF || token.type == TokenType::VARIABLE ||
            token.type == TokenType::PREFIX_LABEL) {
            // If we're not expecting an operand, this must be the object of the triple pattern
            if (!expecting_operand && !path_tokens.empty()) {
                break;  // Stop here, let this token be parsed as the object
            }
            ARROW_ASSIGN_OR_RAISE(auto term, parser.ParseRDFTerm());
            path_tokens.emplace_back(term);
            expecting_operand = false;  // After operand, expect operator or quantifier
            continue;
        }

        // Binary operators
        if (token.type == TokenType::DIVIDE) {
            path_tokens.emplace_back(PathToken::BINARY_OP, "/", 3);  // Sequence: precedence 3
            parser.Advance();
            expecting_operand = true;  // After operator, expect operand
            continue;
        }
        if (token.type == TokenType::PIPE) {
            path_tokens.emplace_back(PathToken::BINARY_OP, "|", 2);  // Alternative: precedence 2
            parser.Advance();
            expecting_operand = true;  // After operator, expect operand
            continue;
        }

        // Unary prefix operators
        if (token.type == TokenType::CARET) {
            path_tokens.emplace_back(PathToken::UNARY_PREFIX, "^", 4);  // Inverse: precedence 4
            parser.Advance();
            expecting_operand = true;  // After prefix operator, expect operand
            continue;
        }
        if (token.type == TokenType::NOT) {
            path_tokens.emplace_back(PathToken::UNARY_PREFIX, "!", 4);  // Negation: precedence 4
            parser.Advance();
            expecting_operand = true;  // After prefix operator, expect operand
            continue;
        }

        // Unary postfix operators (quantifiers)
        if (token.type == TokenType::MULTIPLY) {
            PathToken qt(PathToken::UNARY_POSTFIX, "*", 5);
            qt.quantifier_type = PropertyPathQuantifier::ZeroOrMore;
            path_tokens.push_back(qt);
            parser.Advance();
            continue;
        }
        if (token.type == TokenType::PLUS) {
            PathToken qt(PathToken::UNARY_POSTFIX, "+", 5);
            qt.quantifier_type = PropertyPathQuantifier::OneOrMore;
            path_tokens.push_back(qt);
            parser.Advance();
            continue;
        }
        if (token.type == TokenType::QUESTION) {
            PathToken qt(PathToken::UNARY_POSTFIX, "?", 5);
            qt.quantifier_type = PropertyPathQuantifier::ZeroOrOne;
            path_tokens.push_back(qt);
            parser.Advance();
            continue;
        }
        if (token.type == TokenType::LBRACE) {
            // Counted quantifier: {n}, {n,}, {n,m}
            parser.Advance();  // Skip {

            if (!parser.Check(TokenType::INTEGER) && !parser.Check(TokenType::COMMA)) {
                return parser.Error("Expected integer or comma in counted quantifier");
            }

            int min_val = 0;
            int max_val = -1;

            if (parser.Check(TokenType::INTEGER)) {
                min_val = std::stoi(parser.CurrentToken().text);
                parser.Advance();
            }

            PathToken qt(PathToken::UNARY_POSTFIX, "{}", 5);
            if (parser.Match(TokenType::COMMA)) {
                if (parser.Check(TokenType::INTEGER)) {
                    max_val = std::stoi(parser.CurrentToken().text);
                    parser.Advance();
                    qt.quantifier_type = PropertyPathQuantifier::RangeCount;
                } else {
                    qt.quantifier_type = PropertyPathQuantifier::MinCount;
                    max_val = -1;
                }
            } else {
                qt.quantifier_type = PropertyPathQuantifier::ExactCount;
                max_val = min_val;
            }

            qt.min_count = min_val;
            qt.max_count = max_val;
            path_tokens.push_back(qt);

            ARROW_RETURN_NOT_OK(parser.Expect(TokenType::RBRACE, "Expected '}' after counted quantifier"));
            continue;
        }

        // Parentheses
        if (token.type == TokenType::LPAREN) {
            path_tokens.emplace_back(PathToken::LPAREN, "(", 0);
            parser.Advance();
            expecting_operand = true;  // After '(', expect operand
            continue;
        }
        if (token.type == TokenType::RPAREN) {
            path_tokens.emplace_back(PathToken::RPAREN, ")", 0);
            parser.Advance();
            expecting_operand = false;  // After ')', expect operator or quantifier
            continue;
        }

        // If we hit something we don't recognize, stop
        break;
    }

    if (path_tokens.empty()) {
        return parser.Error("Empty property path");
    }

    return path_tokens;
}

// Convert infix notation to postfix (RPN) using Shunting Yard algorithm
// This handles operator precedence automatically
std::vector<PathToken> InfixToPostfix(const std::vector<PathToken>& infix) {
    std::vector<PathToken> output;
    std::vector<PathToken> operators;

    for (const auto& token : infix) {
        if (token.type == PathToken::OPERAND) {
            // Operands go directly to output
            output.push_back(token);
        }
        else if (token.IsOperator()) {
            // Pop operators with higher or equal precedence (for left-associative ops)
            // For right-associative, would use strictly higher (>)
            while (!operators.empty() &&
                   operators.back().precedence >= token.precedence &&
                   operators.back().type != PathToken::LPAREN) {
                output.push_back(operators.back());
                operators.pop_back();
            }
            operators.push_back(token);
        }
        else if (token.type == PathToken::LPAREN) {
            operators.push_back(token);
        }
        else if (token.type == PathToken::RPAREN) {
            // Pop until matching LPAREN
            while (!operators.empty() && operators.back().type != PathToken::LPAREN) {
                output.push_back(operators.back());
                operators.pop_back();
            }
            if (!operators.empty()) {
                operators.pop_back();  // Remove the LPAREN
            }
        }
    }

    // Pop remaining operators
    while (!operators.empty()) {
        output.push_back(operators.back());
        operators.pop_back();
    }

    return output;
}

// Build PropertyPath AST from postfix (RPN) notation using stack-based evaluation
PropertyPath BuildPathFromPostfix(const std::vector<PathToken>& postfix) {
    std::vector<PropertyPath> stack;

    for (const auto& token : postfix) {
        if (token.type == PathToken::OPERAND) {
            // Create a simple path with one element
            PropertyPath path;
            PropertyPathElement elem(token.term);
            path.elements.push_back(elem);
            stack.push_back(path);
        }
        else if (token.type == PathToken::BINARY_OP) {
            // Pop two operands and combine them
            if (stack.size() < 2) {
                // Error - not enough operands
                PropertyPath error_path;
                return error_path;
            }

            auto right = std::move(stack.back());
            stack.pop_back();
            auto left = std::move(stack.back());
            stack.pop_back();

            PropertyPath combined;
            combined.modifier = (token.value == "/")
                ? PropertyPathModifier::Sequence
                : PropertyPathModifier::Alternative;

            // If left has the same modifier, we can flatten it
            if (left.modifier == combined.modifier && left.elements.size() > 0) {
                // Flatten: just copy elements
                combined.elements = std::move(left.elements);
            } else {
                // Wrap left in a nested path
                PropertyPathElement nested_elem(std::make_shared<PropertyPath>(std::move(left)));
                combined.elements.push_back(nested_elem);
            }

            // If right has the same modifier, we can flatten it
            if (right.modifier == combined.modifier && right.elements.size() > 0) {
                // Flatten: append elements
                combined.elements.insert(combined.elements.end(),
                                       std::make_move_iterator(right.elements.begin()),
                                       std::make_move_iterator(right.elements.end()));
            } else {
                // Wrap right in a nested path
                PropertyPathElement nested_elem(std::make_shared<PropertyPath>(std::move(right)));
                combined.elements.push_back(nested_elem);
            }

            stack.push_back(std::move(combined));
        }
        else if (token.type == PathToken::UNARY_PREFIX) {
            // Pop one operand and apply prefix operator
            if (stack.empty()) {
                PropertyPath error_path;
                return error_path;
            }

            auto child = std::move(stack.back());
            stack.pop_back();

            PropertyPath wrapped;
            wrapped.modifier = (token.value == "^")
                ? PropertyPathModifier::Inverse
                : PropertyPathModifier::Negated;

            // Wrap the child path
            PropertyPathElement nested_elem(std::make_shared<PropertyPath>(std::move(child)));
            wrapped.elements.push_back(nested_elem);

            stack.push_back(std::move(wrapped));
        }
        else if (token.type == PathToken::UNARY_POSTFIX) {
            // Pop one operand and apply quantifier to its last element
            if (stack.empty()) {
                PropertyPath error_path;
                return error_path;
            }

            auto child = std::move(stack.back());
            stack.pop_back();

            // Apply quantifier to the last element
            if (!child.elements.empty()) {
                child.elements.back().quantifier = token.quantifier_type;
                child.elements.back().min_count = token.min_count;
                child.elements.back().max_count = token.max_count;
            }

            stack.push_back(std::move(child));
        }
    }

    // Final result should be single item on stack
    if (stack.size() == 1) {
        return std::move(stack.back());
    }

    // Error: should have exactly one result
    PropertyPath error_path;
    return error_path;
}

arrow::Result<PropertyPath> SPARQLParser::ParsePropertyPath() {
    // NEW: Shunting Yard algorithm for operator precedence
    // Step 1: Tokenize property path from current position
    ARROW_ASSIGN_OR_RAISE(auto path_tokens, TokenizePropertyPathTokens(*this));

    // Step 2: Convert infix to postfix using Shunting Yard algorithm
    auto postfix = InfixToPostfix(path_tokens);

    // Step 3: Build AST from postfix notation
    auto path = BuildPathFromPostfix(postfix);

    return path;
}

// OLD: This function is no longer used with Shunting Yard approach
// Keeping it for backward compatibility if needed
arrow::Result<PropertyPathElement> SPARQLParser::ParsePropertyPathElement() {
    PropertyPathElement elem;

    // Check for inverse modifier on this element: ^p
    if (Check(TokenType::CARET)) {
        // Parse as a nested path with inverse modifier
        ARROW_ASSIGN_OR_RAISE(auto nested_path, ParsePropertyPath());
        elem.element = std::make_shared<PropertyPath>(nested_path);
    }
    // Check for grouped path: (path)
    else if (Match(TokenType::LPAREN)) {
        // Parse nested path
        ARROW_ASSIGN_OR_RAISE(auto nested_path, ParsePropertyPath());
        ARROW_RETURN_NOT_OK(Expect(TokenType::RPAREN, "Expected ')' after grouped path"));
        elem.element = std::make_shared<PropertyPath>(nested_path);
    }
    // Check for negated path set: !p or !(p1|p2|...)
    else if (Check(TokenType::NOT)) {
        // Parse as a nested path with negation modifier
        ARROW_ASSIGN_OR_RAISE(auto nested_path, ParsePropertyPath());
        elem.element = std::make_shared<PropertyPath>(nested_path);
    }
    else {
        // Parse simple term (IRI or variable)
        ARROW_ASSIGN_OR_RAISE(auto term, ParseRDFTerm());
        elem.element = term;
    }

    // Check for quantifier
    if (Match(TokenType::MULTIPLY)) {
        elem.quantifier = PropertyPathQuantifier::ZeroOrMore;
    } else if (Match(TokenType::PLUS)) {
        elem.quantifier = PropertyPathQuantifier::OneOrMore;
    } else if (Match(TokenType::QUESTION)) {
        elem.quantifier = PropertyPathQuantifier::ZeroOrOne;
    } else if (Match(TokenType::LBRACE)) {
        // Counted quantifier: {n}, {n,}, {n,m}, {,m}
        // Parse first number (or empty for {,m})
        if (!Check(TokenType::INTEGER) && !Check(TokenType::COMMA)) {
            return Error("Expected integer or comma in counted quantifier");
        }

        int min_val = 0;
        int max_val = -1;  // -1 means unbounded

        if (Check(TokenType::INTEGER)) {
            min_val = std::stoi(CurrentToken().text);
            Advance();
        }

        // Check for comma (indicates range)
        if (Match(TokenType::COMMA)) {
            // {n,} or {n,m} or {,m}
            if (Check(TokenType::INTEGER)) {
                max_val = std::stoi(CurrentToken().text);
                Advance();
                elem.quantifier = PropertyPathQuantifier::RangeCount;
            } else {
                // {n,} - n or more
                elem.quantifier = PropertyPathQuantifier::MinCount;
                max_val = -1;
            }
        } else {
            // {n} - exactly n
            elem.quantifier = PropertyPathQuantifier::ExactCount;
            max_val = min_val;
        }

        elem.min_count = min_val;
        elem.max_count = max_val;

        ARROW_RETURN_NOT_OK(Expect(TokenType::RBRACE, "Expected '}' after counted quantifier"));
    }

    return elem;
}

arrow::Result<ExprOperator> SPARQLParser::TokenTypeToOperator(TokenType type) const {
    switch (type) {
        case TokenType::EQUAL: return ExprOperator::Equal;
        case TokenType::NOT_EQUAL: return ExprOperator::NotEqual;
        case TokenType::LESS_THAN: return ExprOperator::LessThan;
        case TokenType::LESS_EQUAL: return ExprOperator::LessThanEqual;
        case TokenType::GREATER_THAN: return ExprOperator::GreaterThan;
        case TokenType::GREATER_EQUAL: return ExprOperator::GreaterThanEqual;
        case TokenType::AND: return ExprOperator::And;
        case TokenType::OR: return ExprOperator::Or;
        case TokenType::NOT: return ExprOperator::Not;
        case TokenType::PLUS: return ExprOperator::Plus;
        case TokenType::MINUS: return ExprOperator::Minus;
        case TokenType::MULTIPLY: return ExprOperator::Multiply;
        case TokenType::DIVIDE: return ExprOperator::Divide;
        default:
            return arrow::Status::Invalid("Not an operator token");
    }
}

// ============================================================================
// Convenience function
// ============================================================================

arrow::Result<Query> ParseSPARQL(const std::string& query_text) {
    SPARQLTokenizer tokenizer(query_text);
    ARROW_ASSIGN_OR_RAISE(auto tokens, tokenizer.Tokenize());

    SPARQLParser parser(std::move(tokens));
    return parser.Parse();
}

} // namespace sparql
} // namespace sabot_ql
