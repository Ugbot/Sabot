#include <sabot_ql/sparql/parser.h>
#include <cctype>
#include <unordered_map>
#include <sstream>

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

        // Variable: ?name or $name
        if (c == '?' || c == '$') {
            tokens.push_back(ReadVariable());
            continue;
        }

        // IRI: <http://...>
        if (c == '<') {
            // Could be IRI or < operator
            if (PeekChar() == '=') {
                tokens.push_back(MakeToken(TokenType::LESS_EQUAL, "<="));
                Advance();
                Advance();
            } else {
                // Check if it looks like an IRI (has :// or contains /)
                size_t saved_pos = pos_;
                size_t saved_line = line_;
                size_t saved_column = column_;
                Advance();
                bool looks_like_iri = false;
                while (!IsAtEnd() && CurrentChar() != '>') {
                    if (CurrentChar() == ':' || CurrentChar() == '/') {
                        looks_like_iri = true;
                        break;
                    }
                    Advance();
                }
                // Restore position
                pos_ = saved_pos;
                line_ = saved_line;
                column_ = saved_column;

                if (looks_like_iri) {
                    tokens.push_back(ReadIRI());
                } else {
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

        // Operators
        Token op_token = ReadOperator();
        if (op_token.type != TokenType::ERROR) {
            tokens.push_back(op_token);
            continue;
        }

        // Unknown character
        return arrow::Status::Invalid("Unknown character '", std::string(1, c),
                                     "' at line ", line_, ", column ", column_);
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

    // Check if it's a keyword
    auto keyword_type = LookupKeyword(text);
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
            return MakeError("Expected '||' for OR operator");

        case '@':
            Advance();
            return Token(TokenType::LANG_TAG, "@", start_line, start_column);

        case '^':
            Advance();
            if (!IsAtEnd() && CurrentChar() == '^') {
                Advance();
                return Token(TokenType::DATATYPE_MARKER, "^^", start_line, start_column);
            }
            return MakeError("Expected '^^' for datatype marker");

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
        {"WHERE", TokenType::WHERE},
        {"FILTER", TokenType::FILTER},
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

        // Built-in functions
        {"BOUND", TokenType::BOUND},
        {"isIRI", TokenType::ISIRI},
        {"isLiteral", TokenType::ISLITERAL},
        {"isBlank", TokenType::ISBLANK},
        {"STR", TokenType::STR},
        {"LANG", TokenType::LANG},
        {"DATATYPE", TokenType::DATATYPE},
        {"REGEX", TokenType::REGEX},

        // Aggregate functions
        {"COUNT", TokenType::COUNT},
        {"SUM", TokenType::SUM},
        {"AVG", TokenType::AVG},
        {"MIN", TokenType::MIN},
        {"MAX", TokenType::MAX},
        {"GROUP_CONCAT", TokenType::GROUP_CONCAT},
        {"SAMPLE", TokenType::SAMPLE},

        // Boolean literals
        {"true", TokenType::BOOLEAN},
        {"false", TokenType::BOOLEAN},
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
    // Parse PREFIX declarations (optional, can be multiple)
    while (Check(TokenType::PREFIX)) {
        ARROW_RETURN_NOT_OK(ParsePrefixDeclaration());
    }

    // Parse BASE declaration (optional)
    if (Match(TokenType::BASE)) {
        // BASE support not yet implemented, skip for now
        if (CurrentToken().type == TokenType::IRI_REF) {
            Advance();
        }
    }

    // Parse query
    ARROW_ASSIGN_OR_RAISE(auto select_query, ParseSelectQuery());
    return Query(std::move(select_query));
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
    return pos_ >= tokens_.size() || CurrentToken().type == TokenType::END_OF_INPUT;
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

arrow::Status SPARQLParser::ParsePrefixDeclaration() {
    ARROW_RETURN_NOT_OK(Expect(TokenType::PREFIX, "Expected PREFIX keyword"));

    // Parse prefix label (e.g., "schema:")
    if (CurrentToken().type != TokenType::PREFIX_LABEL) {
        return Error("Expected prefix label after PREFIX");
    }

    std::string prefix_label = CurrentToken().text;

    // Remove trailing ':' if present
    if (!prefix_label.empty() && prefix_label.back() == ':') {
        prefix_label = prefix_label.substr(0, prefix_label.size() - 1);
    }

    Advance();

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
        return arrow::Status::Invalid("Undefined prefix '", prefix, "'");
    }

    // Expand to full IRI
    return it->second + local_name;
}

arrow::Result<SelectQuery> SPARQLParser::ParseSelectQuery() {
    SelectQuery query;

    // Parse SELECT clause
    ARROW_ASSIGN_OR_RAISE(query.select, ParseSelectClause());

    // Parse WHERE clause
    ARROW_RETURN_NOT_OK(Expect(TokenType::WHERE, "Expected WHERE keyword"));
    ARROW_ASSIGN_OR_RAISE(query.where, ParseWhereClause());

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

    // Parse variable list or aggregate expressions
    while (Check(TokenType::VARIABLE) || Check(TokenType::LPAREN)) {
        if (Check(TokenType::LPAREN)) {
            // Parse aggregate expression: (COUNT(?x) AS ?count)
            Advance(); // Consume '('

            // Parse aggregate function (COUNT, SUM, AVG, MIN, MAX, GROUP_CONCAT, SAMPLE)
            if (!Check(TokenType::COUNT) && !Check(TokenType::SUM) && !Check(TokenType::AVG) &&
                !Check(TokenType::MIN) && !Check(TokenType::MAX) && !Check(TokenType::GROUP_CONCAT) &&
                !Check(TokenType::SAMPLE)) {
                return Error("Expected aggregate function (COUNT, SUM, AVG, MIN, MAX, GROUP_CONCAT, SAMPLE)");
            }

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

            // Parse argument (variable or * for COUNT)
            auto agg_expr = std::make_shared<Expression>(agg_op);
            if (agg_op == ExprOperator::Count && Check(TokenType::MULTIPLY)) {
                // COUNT(*) - no argument
                Advance();
            } else if (Check(TokenType::VARIABLE)) {
                ARROW_ASSIGN_OR_RAISE(auto arg_var, ParseVariable());
                auto arg_expr = std::make_shared<Expression>(RDFTerm(arg_var));
                agg_expr->arguments.push_back(arg_expr);
            } else {
                return Error("Expected variable or * as aggregate function argument");
            }

            ARROW_RETURN_NOT_OK(Expect(TokenType::RPAREN, "Expected ')' after aggregate function argument"));

            ARROW_RETURN_NOT_OK(Expect(TokenType::RPAREN, "Expected ')' to close aggregate expression"));

            // Parse AS alias
            ARROW_RETURN_NOT_OK(Expect(TokenType::AS, "Expected AS keyword after aggregate expression"));

            ARROW_ASSIGN_OR_RAISE(auto alias, ParseVariable());

            // Create AggregateExpression
            AggregateExpression agg(agg_expr, alias, distinct);
            clause.items.push_back(SelectItem(agg));

        } else {
            // Parse simple variable
            ARROW_ASSIGN_OR_RAISE(auto var, ParseVariable());
            clause.items.push_back(SelectItem(var));
        }

        // Continue if there's a comma
        if (!Match(TokenType::COMMA)) {
            break;
        }
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
            // Parse FILTER clause
            ARROW_ASSIGN_OR_RAISE(auto filter, ParseFilterClause());
            pattern.filters.push_back(filter);
        } else if (Check(TokenType::OPTIONAL)) {
            // Parse OPTIONAL clause
            ARROW_ASSIGN_OR_RAISE(auto optional, ParseOptionalClause());
            pattern.optionals.push_back(optional);
        } else if (Check(TokenType::UNION)) {
            // Parse UNION clause
            ARROW_ASSIGN_OR_RAISE(auto union_pattern, ParseUnionClause());
            pattern.unions.push_back(union_pattern);
        } else {
            // Parse triple pattern
            if (!pattern.bgp.has_value()) {
                pattern.bgp = BasicGraphPattern();
            }

            ARROW_ASSIGN_OR_RAISE(auto triple, ParseTriplePattern());
            pattern.bgp->triples.push_back(triple);

            // Consume optional '.'
            Match(TokenType::DOT);
        }
    }

    ARROW_RETURN_NOT_OK(Expect(TokenType::RBRACE, "Expected '}' to end WHERE clause"));

    return pattern;
}

arrow::Result<BasicGraphPattern> SPARQLParser::ParseBasicGraphPattern() {
    BasicGraphPattern bgp;

    while (!Check(TokenType::RBRACE) && !Check(TokenType::FILTER) &&
           !Check(TokenType::OPTIONAL) && !Check(TokenType::UNION) && !IsAtEnd()) {
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

    // Parse predicate
    ARROW_ASSIGN_OR_RAISE(auto predicate, ParseRDFTerm());

    // Parse object
    ARROW_ASSIGN_OR_RAISE(auto object, ParseRDFTerm());

    return TriplePattern(subject, predicate, object);
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
    } else if (token.type == TokenType::PREFIX_LABEL) {
        // Expand prefixed name to full IRI
        std::string prefixed_name = token.text;
        Advance();

        ARROW_ASSIGN_OR_RAISE(auto full_iri, ExpandPrefixedName(prefixed_name));
        return RDFTerm(IRI(full_iri));
    }

    return Error("Expected RDF term (variable, IRI, literal, or blank node)");
}

arrow::Result<FilterClause> SPARQLParser::ParseFilterClause() {
    ARROW_RETURN_NOT_OK(Expect(TokenType::FILTER, "Expected FILTER keyword"));

    ARROW_ASSIGN_OR_RAISE(auto expr, ParseExpression());

    return FilterClause(expr);
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

    // Built-in function call
    if (Check(TokenType::BOUND) || Check(TokenType::ISIRI) || Check(TokenType::ISLITERAL) ||
        Check(TokenType::ISBLANK) || Check(TokenType::STR) || Check(TokenType::LANG) ||
        Check(TokenType::DATATYPE) || Check(TokenType::REGEX)) {
        return ParseBuiltInCall();
    }

    // Literal value
    ARROW_ASSIGN_OR_RAISE(auto term, ParseRDFTerm());
    return std::make_shared<Expression>(term);
}

arrow::Result<std::shared_ptr<Expression>> SPARQLParser::ParseBuiltInCall() {
    const Token& func_token = CurrentToken();
    ExprOperator op;

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
    } else {
        return Error("Unknown built-in function");
    }

    Advance(); // Consume function name

    ARROW_RETURN_NOT_OK(Expect(TokenType::LPAREN, "Expected '(' after function name"));

    auto expr = std::make_shared<Expression>(op);

    // Parse function arguments
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

    // Parse comma-separated list of variables
    while (Check(TokenType::VARIABLE)) {
        ARROW_ASSIGN_OR_RAISE(auto var, ParseVariable());
        clause.variables.push_back(var);

        // Continue if there's a comma
        if (!Match(TokenType::COMMA)) {
            break;
        }
    }

    if (clause.variables.empty()) {
        return Error("Expected at least one variable in GROUP BY clause");
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
