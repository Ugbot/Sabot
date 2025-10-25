#include <sabot_ql/operators/operator_metadata.h>
#include <sstream>

namespace sabot_ql {

bool OrderingProperty::matches(const std::vector<std::string>& required_columns) const {
    // Empty ordering matches nothing (except empty requirement)
    if (columns.empty()) {
        return required_columns.empty();
    }

    // Required columns must be a prefix of our ordering
    if (required_columns.size() > columns.size()) {
        return false;
    }

    // Check if each required column matches in order
    for (size_t i = 0; i < required_columns.size(); ++i) {
        if (columns[i] != required_columns[i]) {
            return false;
        }
    }

    return true;
}

bool OrderingProperty::equals(const OrderingProperty& other) const {
    if (columns.size() != other.columns.size()) {
        return false;
    }

    for (size_t i = 0; i < columns.size(); ++i) {
        if (columns[i] != other.columns[i] ||
            directions[i] != other.directions[i]) {
            return false;
        }
    }

    return true;
}

OrderingProperty OrderingProperty::prefix(size_t n) const {
    if (n >= columns.size()) {
        return *this;
    }

    OrderingProperty result;
    result.columns.assign(columns.begin(), columns.begin() + n);
    result.directions.assign(directions.begin(), directions.begin() + n);
    return result;
}

std::string OrderingProperty::ToString() const {
    if (columns.empty()) {
        return "[]";
    }

    std::ostringstream oss;
    oss << "[";
    for (size_t i = 0; i < columns.size(); ++i) {
        if (i > 0) oss << ", ";
        oss << columns[i];
        oss << (directions[i] == SortDirection::Ascending ? " ASC" : " DESC");
    }
    oss << "]";
    return oss.str();
}

} // namespace sabot_ql
