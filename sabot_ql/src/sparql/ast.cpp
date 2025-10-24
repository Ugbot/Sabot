#include <sabot_ql/sparql/ast.h>
#include <sstream>

namespace sabot_ql {
namespace sparql {

std::string Literal::ToString() const {
    std::ostringstream oss;
    oss << "\"" << value << "\"";
    if (!language.empty()) {
        oss << "@" << language;
    }
    if (!datatype.empty()) {
        oss << "^^<" << datatype << ">";
    }
    return oss.str();
}

std::string ToString(const RDFTerm& term) {
    return std::visit([](const auto& t) { return t.ToString(); }, term);
}

// BlankNode constructor for RDF collections
BlankNode::BlankNode(std::string i, std::shared_ptr<RDFTermList> items)
    : id(std::move(i)), collection_items(std::move(items)) {
}

std::optional<std::string> TriplePattern::GetSubjectVar() const {
    if (auto* var = std::get_if<Variable>(&subject)) {
        return var->name;
    }
    return std::nullopt;
}

std::optional<std::string> TriplePattern::GetPredicateVar() const {
    if (auto* term = std::get_if<RDFTerm>(&predicate)) {
        if (auto* var = std::get_if<Variable>(term)) {
            return var->name;
        }
    }
    return std::nullopt;
}

std::optional<std::string> TriplePattern::GetObjectVar() const {
    if (auto* var = std::get_if<Variable>(&object)) {
        return var->name;
    }
    return std::nullopt;
}

std::string PropertyPathElement::ToString() const {
    std::ostringstream oss;

    // Print the element (term or nested path)
    if (auto* term = std::get_if<RDFTerm>(&element)) {
        oss << sparql::ToString(*term);
    } else if (auto* path = std::get_if<std::shared_ptr<PropertyPath>>(&element)) {
        oss << "(" << (*path)->ToString() << ")";
    }

    // Add quantifier if present
    switch (quantifier) {
        case PropertyPathQuantifier::ZeroOrMore:
            oss << "*";
            break;
        case PropertyPathQuantifier::OneOrMore:
            oss << "+";
            break;
        case PropertyPathQuantifier::ZeroOrOne:
            oss << "?";
            break;
        case PropertyPathQuantifier::None:
            // No quantifier
            break;
    }

    return oss.str();
}

std::string PropertyPath::ToString() const {
    if (elements.empty()) {
        return "";
    }

    if (elements.size() == 1) {
        // Single element, check for inverse
        if (modifier == PropertyPathModifier::Inverse) {
            return "^" + elements[0].ToString();
        } else if (modifier == PropertyPathModifier::Negated) {
            return "!" + elements[0].ToString();
        }
        return elements[0].ToString();
    }

    // Multiple elements - use modifier
    std::ostringstream oss;
    for (size_t i = 0; i < elements.size(); ++i) {
        if (i > 0) {
            switch (modifier) {
                case PropertyPathModifier::Sequence:
                    oss << "/";
                    break;
                case PropertyPathModifier::Alternative:
                    oss << "|";
                    break;
                default:
                    oss << " ";  // Fallback
                    break;
            }
        }
        oss << elements[i].ToString();
    }

    return oss.str();
}

std::string TriplePattern::ToString() const {
    std::ostringstream oss;
    oss << sparql::ToString(subject) << " ";

    // Handle predicate (can be RDFTerm or PropertyPath)
    if (auto* term = std::get_if<RDFTerm>(&predicate)) {
        oss << sparql::ToString(*term);
    } else if (auto* path = std::get_if<PropertyPath>(&predicate)) {
        oss << path->ToString();
    }

    oss << " " << sparql::ToString(object) << " .";
    return oss.str();
}

std::string BasicGraphPattern::ToString() const {
    std::ostringstream oss;
    for (const auto& triple : triples) {
        oss << "  " << triple.ToString() << "\n";
    }
    return oss.str();
}

std::string Expression::ToString() const {
    if (IsConstant()) {
        return sparql::ToString(*constant);
    }

    std::ostringstream oss;

    switch (op) {
        case ExprOperator::Equal:
            oss << "(" << arguments[0]->ToString() << " = " << arguments[1]->ToString() << ")";
            break;
        case ExprOperator::NotEqual:
            oss << "(" << arguments[0]->ToString() << " != " << arguments[1]->ToString() << ")";
            break;
        case ExprOperator::LessThan:
            oss << "(" << arguments[0]->ToString() << " < " << arguments[1]->ToString() << ")";
            break;
        case ExprOperator::LessThanEqual:
            oss << "(" << arguments[0]->ToString() << " <= " << arguments[1]->ToString() << ")";
            break;
        case ExprOperator::GreaterThan:
            oss << "(" << arguments[0]->ToString() << " > " << arguments[1]->ToString() << ")";
            break;
        case ExprOperator::GreaterThanEqual:
            oss << "(" << arguments[0]->ToString() << " >= " << arguments[1]->ToString() << ")";
            break;
        case ExprOperator::And:
            oss << "(" << arguments[0]->ToString() << " && " << arguments[1]->ToString() << ")";
            break;
        case ExprOperator::Or:
            oss << "(" << arguments[0]->ToString() << " || " << arguments[1]->ToString() << ")";
            break;
        case ExprOperator::Not:
            oss << "!(" << arguments[0]->ToString() << ")";
            break;
        case ExprOperator::Plus:
            oss << "(" << arguments[0]->ToString() << " + " << arguments[1]->ToString() << ")";
            break;
        case ExprOperator::Minus:
            oss << "(" << arguments[0]->ToString() << " - " << arguments[1]->ToString() << ")";
            break;
        case ExprOperator::Multiply:
            oss << "(" << arguments[0]->ToString() << " * " << arguments[1]->ToString() << ")";
            break;
        case ExprOperator::Divide:
            oss << "(" << arguments[0]->ToString() << " / " << arguments[1]->ToString() << ")";
            break;
        case ExprOperator::Bound:
            oss << "BOUND(" << arguments[0]->ToString() << ")";
            break;
        case ExprOperator::IsIRI:
            oss << "isIRI(" << arguments[0]->ToString() << ")";
            break;
        case ExprOperator::IsLiteral:
            oss << "isLiteral(" << arguments[0]->ToString() << ")";
            break;
        case ExprOperator::IsBlank:
            oss << "isBlank(" << arguments[0]->ToString() << ")";
            break;
        case ExprOperator::Str:
            oss << "STR(" << arguments[0]->ToString() << ")";
            break;
        case ExprOperator::Lang:
            oss << "LANG(" << arguments[0]->ToString() << ")";
            break;
        case ExprOperator::Datatype:
            oss << "DATATYPE(" << arguments[0]->ToString() << ")";
            break;
        case ExprOperator::Regex:
            oss << "REGEX(" << arguments[0]->ToString() << ", " << arguments[1]->ToString() << ")";
            break;

        // String functions
        case ExprOperator::StrLen:
            oss << "STRLEN(" << arguments[0]->ToString() << ")";
            break;
        case ExprOperator::SubStr:
            oss << "SUBSTR(" << arguments[0]->ToString();
            if (arguments.size() > 1) oss << ", " << arguments[1]->ToString();
            if (arguments.size() > 2) oss << ", " << arguments[2]->ToString();
            oss << ")";
            break;
        case ExprOperator::UCase:
            oss << "UCASE(" << arguments[0]->ToString() << ")";
            break;
        case ExprOperator::LCase:
            oss << "LCASE(" << arguments[0]->ToString() << ")";
            break;
        case ExprOperator::StrStarts:
            oss << "STRSTARTS(" << arguments[0]->ToString() << ", " << arguments[1]->ToString() << ")";
            break;
        case ExprOperator::StrEnds:
            oss << "STRENDS(" << arguments[0]->ToString() << ", " << arguments[1]->ToString() << ")";
            break;
        case ExprOperator::Contains:
            oss << "CONTAINS(" << arguments[0]->ToString() << ", " << arguments[1]->ToString() << ")";
            break;
        case ExprOperator::StrBefore:
            oss << "STRBEFORE(" << arguments[0]->ToString() << ", " << arguments[1]->ToString() << ")";
            break;
        case ExprOperator::StrAfter:
            oss << "STRAFTER(" << arguments[0]->ToString() << ", " << arguments[1]->ToString() << ")";
            break;
        case ExprOperator::Concat:
            oss << "CONCAT(";
            for (size_t i = 0; i < arguments.size(); ++i) {
                if (i > 0) oss << ", ";
                oss << arguments[i]->ToString();
            }
            oss << ")";
            break;
        case ExprOperator::LangMatches:
            oss << "LANGMATCHES(" << arguments[0]->ToString() << ", " << arguments[1]->ToString() << ")";
            break;
        case ExprOperator::Replace:
            oss << "REPLACE(" << arguments[0]->ToString() << ", " << arguments[1]->ToString();
            if (arguments.size() > 2) oss << ", " << arguments[2]->ToString();
            oss << ")";
            break;
        case ExprOperator::EncodeForURI:
            oss << "ENCODE_FOR_URI(" << arguments[0]->ToString() << ")";
            break;

        // Math functions
        case ExprOperator::Abs:
            oss << "ABS(" << arguments[0]->ToString() << ")";
            break;
        case ExprOperator::Round:
            oss << "ROUND(" << arguments[0]->ToString() << ")";
            break;
        case ExprOperator::Ceil:
            oss << "CEIL(" << arguments[0]->ToString() << ")";
            break;
        case ExprOperator::Floor:
            oss << "FLOOR(" << arguments[0]->ToString() << ")";
            break;

        // Date/Time functions
        case ExprOperator::Now:
            oss << "NOW()";
            break;
        case ExprOperator::Year:
            oss << "YEAR(" << arguments[0]->ToString() << ")";
            break;
        case ExprOperator::Month:
            oss << "MONTH(" << arguments[0]->ToString() << ")";
            break;
        case ExprOperator::Day:
            oss << "DAY(" << arguments[0]->ToString() << ")";
            break;
        case ExprOperator::Hours:
            oss << "HOURS(" << arguments[0]->ToString() << ")";
            break;
        case ExprOperator::Minutes:
            oss << "MINUTES(" << arguments[0]->ToString() << ")";
            break;
        case ExprOperator::Seconds:
            oss << "SECONDS(" << arguments[0]->ToString() << ")";
            break;
        case ExprOperator::Timezone:
            oss << "TIMEZONE(" << arguments[0]->ToString() << ")";
            break;
        case ExprOperator::Tz:
            oss << "TZ(" << arguments[0]->ToString() << ")";
            break;

        // Type conversion functions
        case ExprOperator::StrDt:
            oss << "STRDT(" << arguments[0]->ToString() << ", " << arguments[1]->ToString() << ")";
            break;
        case ExprOperator::StrLang:
            oss << "STRLANG(" << arguments[0]->ToString() << ", " << arguments[1]->ToString() << ")";
            break;

        // Hash functions
        case ExprOperator::MD5:
            oss << "MD5(" << arguments[0]->ToString() << ")";
            break;
        case ExprOperator::SHA1:
            oss << "SHA1(" << arguments[0]->ToString() << ")";
            break;
        case ExprOperator::SHA256:
            oss << "SHA256(" << arguments[0]->ToString() << ")";
            break;
        case ExprOperator::SHA384:
            oss << "SHA384(" << arguments[0]->ToString() << ")";
            break;
        case ExprOperator::SHA512:
            oss << "SHA512(" << arguments[0]->ToString() << ")";
            break;

        case ExprOperator::Count:
            oss << "COUNT(" << (arguments.empty() ? "*" : arguments[0]->ToString()) << ")";
            break;
        case ExprOperator::Sum:
            oss << "SUM(" << arguments[0]->ToString() << ")";
            break;
        case ExprOperator::Avg:
            oss << "AVG(" << arguments[0]->ToString() << ")";
            break;
        case ExprOperator::Min:
            oss << "MIN(" << arguments[0]->ToString() << ")";
            break;
        case ExprOperator::Max:
            oss << "MAX(" << arguments[0]->ToString() << ")";
            break;
        case ExprOperator::GroupConcat:
            oss << "GROUP_CONCAT(" << arguments[0]->ToString() << ")";
            break;
        case ExprOperator::Sample:
            oss << "SAMPLE(" << arguments[0]->ToString() << ")";
            break;
    }

    return oss.str();
}

std::string FilterClause::ToString() const {
    return "FILTER " + expr->ToString();
}

std::string BindClause::ToString() const {
    return "BIND(" + expr->ToString() + " AS " + alias.ToString() + ")";
}

std::string OptionalPattern::ToString() const {
    return "OPTIONAL { " + pattern->ToString() + " }";
}

std::string UnionPattern::ToString() const {
    std::ostringstream oss;
    oss << "{ ";
    for (size_t i = 0; i < patterns.size(); ++i) {
        if (i > 0) oss << " UNION ";
        oss << patterns[i]->ToString();
    }
    oss << " }";
    return oss.str();
}

std::string ValuesClause::ToString() const {
    std::ostringstream oss;
    oss << "VALUES (";
    for (size_t i = 0; i < variables.size(); ++i) {
        if (i > 0) oss << " ";
        oss << variables[i].ToString();
    }
    oss << ") {\n";
    for (const auto& row : rows) {
        oss << "  (";
        for (size_t i = 0; i < row.size(); ++i) {
            if (i > 0) oss << " ";
            oss << sparql::ToString(row[i]);
        }
        oss << ")\n";
    }
    oss << "}";
    return oss.str();
}

std::string MinusPattern::ToString() const {
    return "MINUS { " + pattern->ToString() + " }";
}

std::string ExistsPattern::ToString() const {
    std::ostringstream oss;
    if (is_negated) {
        oss << "FILTER NOT EXISTS { ";
    } else {
        oss << "FILTER EXISTS { ";
    }
    oss << pattern->ToString();
    oss << " }";
    return oss.str();
}

std::string SubqueryPattern::ToString() const {
    return "{ " + query.ToString() + " }";
}

std::string QueryPattern::ToString() const {
    std::ostringstream oss;

    if (bgp.has_value()) {
        oss << bgp->ToString();
    }

    for (const auto& filter : filters) {
        oss << "  " << filter.ToString() << "\n";
    }

    for (const auto& bind : binds) {
        oss << "  " << bind.ToString() << "\n";
    }

    for (const auto& optional : optionals) {
        oss << "  " << optional.ToString() << "\n";
    }

    for (const auto& union_pat : unions) {
        oss << "  " << union_pat.ToString() << "\n";
    }

    for (const auto& values_clause : values) {
        oss << "  " << values_clause.ToString() << "\n";
    }

    for (const auto& minus_pat : minus_patterns) {
        oss << "  " << minus_pat.ToString() << "\n";
    }

    for (const auto& exists_pat : exists_patterns) {
        oss << "  " << exists_pat.ToString() << "\n";
    }

    for (const auto& subquery : subqueries) {
        oss << "  " << subquery.ToString() << "\n";
    }

    return oss.str();
}

std::string OrderBy::ToString() const {
    std::ostringstream oss;
    if (direction == OrderDirection::Descending) {
        oss << "DESC(";
    } else {
        oss << "ASC(";
    }
    oss << var.ToString() << ")";
    return oss.str();
}

std::string AggregateExpression::ToString() const {
    std::ostringstream oss;
    oss << "(";
    if (distinct) {
        // Insert DISTINCT into the aggregate function string
        std::string expr_str = expr->ToString();
        size_t paren_pos = expr_str.find('(');
        if (paren_pos != std::string::npos) {
            oss << expr_str.substr(0, paren_pos + 1) << "DISTINCT " << expr_str.substr(paren_pos + 1);
        } else {
            oss << expr_str;
        }
    } else {
        oss << expr->ToString();
    }
    oss << " AS " << alias.ToString() << ")";
    return oss.str();
}

std::string ProjectionExpression::ToString() const {
    std::ostringstream oss;
    oss << "(" << expr->ToString() << " AS " << alias.ToString() << ")";
    return oss.str();
}

std::string GroupByClause::ToString() const {
    std::ostringstream oss;
    oss << "GROUP BY";
    for (size_t i = 0; i < variables.size(); ++i) {
        if (i > 0) oss << ",";
        oss << " " << variables[i].ToString();
    }
    return oss.str();
}

bool SelectClause::HasAggregates() const {
    for (const auto& item : items) {
        if (std::holds_alternative<AggregateExpression>(item)) {
            return true;
        }
    }
    return false;
}

std::string SelectClause::ToString() const {
    std::ostringstream oss;
    oss << "SELECT ";
    if (distinct) {
        oss << "DISTINCT ";
    }

    if (IsSelectAll()) {
        oss << "*";
    } else {
        for (size_t i = 0; i < items.size(); ++i) {
            if (i > 0) oss << " ";

            if (auto* var = std::get_if<Variable>(&items[i])) {
                oss << var->ToString();
            } else if (auto* agg = std::get_if<AggregateExpression>(&items[i])) {
                oss << agg->ToString();
            } else if (auto* proj = std::get_if<ProjectionExpression>(&items[i])) {
                oss << proj->ToString();
            }
        }
    }

    return oss.str();
}

std::string SelectQuery::ToString() const {
    std::ostringstream oss;

    oss << select.ToString() << "\n";
    oss << "WHERE {\n";
    oss << where.ToString();
    oss << "}\n";

    if (group_by.has_value() && !group_by->IsEmpty()) {
        oss << group_by->ToString() << "\n";
    }

    if (!order_by.empty()) {
        oss << "ORDER BY ";
        for (size_t i = 0; i < order_by.size(); ++i) {
            if (i > 0) oss << " ";
            oss << order_by[i].ToString();
        }
        oss << "\n";
    }

    if (limit.has_value()) {
        oss << "LIMIT " << *limit << "\n";
    }

    if (offset.has_value()) {
        oss << "OFFSET " << *offset << "\n";
    }

    return oss.str();
}

std::string AskQuery::ToString() const {
    std::ostringstream oss;
    oss << "ASK\n";
    oss << "WHERE {\n";
    oss << where.ToString();
    oss << "}\n";
    return oss.str();
}

std::string ConstructTemplate::ToString() const {
    std::ostringstream oss;
    for (const auto& triple : triples) {
        oss << "  " << triple.ToString() << "\n";
    }
    return oss.str();
}

std::string ConstructQuery::ToString() const {
    std::ostringstream oss;
    oss << "CONSTRUCT {\n";
    oss << construct_template.ToString();
    oss << "}\n";
    oss << "WHERE {\n";
    oss << where.ToString();
    oss << "}\n";
    return oss.str();
}

std::string DescribeQuery::ToString() const {
    std::ostringstream oss;
    oss << "DESCRIBE ";
    for (size_t i = 0; i < resources.size(); ++i) {
        if (i > 0) oss << " ";
        oss << sparql::ToString(resources[i]);
    }
    oss << "\n";
    if (where.has_value()) {
        oss << "WHERE {\n";
        oss << where->ToString();
        oss << "}\n";
    }
    return oss.str();
}

std::string Query::ToString() const {
    return std::visit([](const auto& q) { return q.ToString(); }, query_body);
}

} // namespace sparql
} // namespace sabot_ql
