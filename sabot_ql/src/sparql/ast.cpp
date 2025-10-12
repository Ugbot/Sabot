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

std::optional<std::string> TriplePattern::GetSubjectVar() const {
    if (auto* var = std::get_if<Variable>(&subject)) {
        return var->name;
    }
    return std::nullopt;
}

std::optional<std::string> TriplePattern::GetPredicateVar() const {
    if (auto* var = std::get_if<Variable>(&predicate)) {
        return var->name;
    }
    return std::nullopt;
}

std::optional<std::string> TriplePattern::GetObjectVar() const {
    if (auto* var = std::get_if<Variable>(&object)) {
        return var->name;
    }
    return std::nullopt;
}

std::string TriplePattern::ToString() const {
    std::ostringstream oss;
    oss << sparql::ToString(subject) << " "
        << sparql::ToString(predicate) << " "
        << sparql::ToString(object) << " .";
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
    }

    return oss.str();
}

std::string FilterClause::ToString() const {
    return "FILTER " + expr->ToString();
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

std::string QueryPattern::ToString() const {
    std::ostringstream oss;

    if (bgp.has_value()) {
        oss << bgp->ToString();
    }

    for (const auto& filter : filters) {
        oss << "  " << filter.ToString() << "\n";
    }

    for (const auto& optional : optionals) {
        oss << "  " << optional.ToString() << "\n";
    }

    for (const auto& union_pat : unions) {
        oss << "  " << union_pat.ToString() << "\n";
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

std::string SelectClause::ToString() const {
    std::ostringstream oss;
    oss << "SELECT ";
    if (distinct) {
        oss << "DISTINCT ";
    }

    if (IsSelectAll()) {
        oss << "*";
    } else {
        for (size_t i = 0; i < variables.size(); ++i) {
            if (i > 0) oss << " ";
            oss << variables[i].ToString();
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

} // namespace sparql
} // namespace sabot_ql
