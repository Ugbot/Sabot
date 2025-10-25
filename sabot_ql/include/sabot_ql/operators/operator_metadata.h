#pragma once

#include <vector>
#include <string>
#include <optional>

namespace sabot_ql {

/**
 * @brief Sort direction for ordering properties
 */
enum class SortDirection {
    Ascending,
    Descending
};

/**
 * @brief Sort key: column name and direction
 * Used by sort operators and ordering properties
 */
struct SortKey {
    std::string column_name;
    SortDirection direction;

    SortKey(std::string col, SortDirection dir)
        : column_name(std::move(col)), direction(dir) {}

    std::string ToString() const {
        return column_name + (direction == SortDirection::Ascending ? " ASC" : " DESC");
    }
};

/**
 * @brief Ordering property tracks how output data is sorted
 *
 * Used for query optimization - if data is already sorted,
 * we can skip redundant sort operations.
 */
struct OrderingProperty {
    std::vector<std::string> columns;      // Columns in sort order
    std::vector<SortDirection> directions; // Sort direction for each column

    /**
     * @brief Check if this ordering matches required columns
     *
     * Matches if this ordering provides the required columns as a prefix.
     * Example: ordering by (a, b, c) matches requirement for (a, b).
     *
     * @param required_columns Columns that must be ordered
     * @return true if ordering satisfies requirement
     */
    bool matches(const std::vector<std::string>& required_columns) const;

    /**
     * @brief Check if this ordering exactly matches another ordering
     */
    bool equals(const OrderingProperty& other) const;

    /**
     * @brief Get prefix of ordering (first N columns)
     */
    OrderingProperty prefix(size_t n) const;

    /**
     * @brief Check if empty (no ordering)
     */
    bool empty() const { return columns.empty(); }

    /**
     * @brief String representation for debugging
     */
    std::string ToString() const;
};

/**
 * @brief Metadata about operator output
 *
 * Tracks properties like ordering and cardinality that can be used
 * for query optimization.
 */
struct OperatorMetadata {
    // Output ordering (if known)
    std::optional<OrderingProperty> output_ordering;

    // Estimated number of output rows
    size_t cardinality_estimate = 0;

    /**
     * @brief Check if output has known ordering
     */
    bool has_ordering() const { return output_ordering.has_value(); }

    /**
     * @brief Get output ordering (or empty if unknown)
     */
    OrderingProperty get_ordering() const {
        return output_ordering.value_or(OrderingProperty{});
    }

    /**
     * @brief Set output ordering
     */
    void set_ordering(const OrderingProperty& ordering) {
        output_ordering = ordering;
    }

    /**
     * @brief Clear output ordering
     */
    void clear_ordering() {
        output_ordering = std::nullopt;
    }
};

} // namespace sabot_ql
