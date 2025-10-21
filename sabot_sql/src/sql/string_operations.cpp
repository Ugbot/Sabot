#include "sabot_sql/sql/string_operations.h"

namespace sabot_sql {
namespace sql {

// ========== Comparison Operations ==========

arrow::Result<std::shared_ptr<arrow::BooleanArray>>
StringOperations::Equal(const std::shared_ptr<arrow::Array>& arr, const std::string& value) {
    auto scalar = arrow::MakeScalar(value);
    ARROW_ASSIGN_OR_RAISE(
        auto result,
        arrow::compute::CallFunction("equal", {arr, scalar})
    );
    return std::static_pointer_cast<arrow::BooleanArray>(result.make_array());
}

arrow::Result<std::shared_ptr<arrow::BooleanArray>>
StringOperations::NotEqual(const std::shared_ptr<arrow::Array>& arr, const std::string& value) {
    auto scalar = arrow::MakeScalar(value);
    ARROW_ASSIGN_OR_RAISE(
        auto result,
        arrow::compute::CallFunction("not_equal", {arr, scalar})
    );
    return std::static_pointer_cast<arrow::BooleanArray>(result.make_array());
}

arrow::Result<std::shared_ptr<arrow::BooleanArray>>
StringOperations::IsEmpty(const std::shared_ptr<arrow::Array>& arr) {
    return Equal(arr, "");
}

arrow::Result<std::shared_ptr<arrow::BooleanArray>>
StringOperations::IsNotEmpty(const std::shared_ptr<arrow::Array>& arr) {
    return NotEqual(arr, "");
}

// ========== Pattern Matching ==========

arrow::Result<std::shared_ptr<arrow::BooleanArray>>
StringOperations::Contains(const std::shared_ptr<arrow::Array>& arr, const std::string& pattern) {
    auto scalar = arrow::MakeScalar(pattern);
    ARROW_ASSIGN_OR_RAISE(
        auto result,
        arrow::compute::CallFunction("match_substring", {arr, scalar})
    );
    return std::static_pointer_cast<arrow::BooleanArray>(result.make_array());
}

arrow::Result<std::shared_ptr<arrow::BooleanArray>>
StringOperations::MatchLike(const std::shared_ptr<arrow::Array>& arr, const std::string& pattern) {
    // Convert SQL LIKE pattern to regex
    // % → .*
    // _ → .
    std::string regex_pattern = pattern;
    
    // Escape regex special chars except % and _
    // Then convert % and _ to regex equivalents
    // Simplified implementation - full version would handle all cases
    
    // For now, use match_like if available, otherwise match_substring_regex
    auto scalar = arrow::MakeScalar(pattern);
    auto result_or = arrow::compute::CallFunction("match_like", {arr, scalar});
    if (result_or.ok()) {
        return std::static_pointer_cast<arrow::BooleanArray>(result_or.ValueOrDie().make_array());
    } else {
        // Fallback to regex if match_like not available
        return MatchRegex(arr, regex_pattern);
    }
}

arrow::Result<std::shared_ptr<arrow::BooleanArray>>
StringOperations::MatchRegex(const std::shared_ptr<arrow::Array>& arr, const std::string& pattern) {
    auto scalar = arrow::MakeScalar(pattern);
    ARROW_ASSIGN_OR_RAISE(
        auto result,
        arrow::compute::CallFunction("match_substring_regex", {arr, scalar})
    );
    return std::static_pointer_cast<arrow::BooleanArray>(result.make_array());
}

// ========== String Functions ==========

arrow::Result<std::shared_ptr<arrow::Int32Array>>
StringOperations::Length(const std::shared_ptr<arrow::Array>& arr) {
    ARROW_ASSIGN_OR_RAISE(
        auto result,
        arrow::compute::CallFunction("utf8_length", {arr})
    );
    return std::static_pointer_cast<arrow::Int32Array>(result.make_array());
}

arrow::Result<std::shared_ptr<arrow::StringArray>>
StringOperations::Replace(const std::shared_ptr<arrow::Array>& arr,
                          const std::string& pattern,
                          const std::string& replacement) {
    auto pattern_scalar = arrow::MakeScalar(pattern);
    auto replacement_scalar = arrow::MakeScalar(replacement);
    
    ARROW_ASSIGN_OR_RAISE(
        auto result,
        arrow::compute::CallFunction("replace_substring", 
                                     {arr, pattern_scalar, replacement_scalar})
    );
    return std::static_pointer_cast<arrow::StringArray>(result.make_array());
}

arrow::Result<std::shared_ptr<arrow::StringArray>>
StringOperations::ReplaceRegex(const std::shared_ptr<arrow::Array>& arr,
                               const std::string& pattern,
                               const std::string& replacement) {
    auto pattern_scalar = arrow::MakeScalar(pattern);
    auto replacement_scalar = arrow::MakeScalar(replacement);
    
    ARROW_ASSIGN_OR_RAISE(
        auto result,
        arrow::compute::CallFunction("replace_substring_regex",
                                     {arr, pattern_scalar, replacement_scalar})
    );
    return std::static_pointer_cast<arrow::StringArray>(result.make_array());
}

arrow::Result<std::shared_ptr<arrow::StringArray>>
StringOperations::Upper(const std::shared_ptr<arrow::Array>& arr) {
    ARROW_ASSIGN_OR_RAISE(
        auto result,
        arrow::compute::CallFunction("utf8_upper", {arr})
    );
    return std::static_pointer_cast<arrow::StringArray>(result.make_array());
}

arrow::Result<std::shared_ptr<arrow::StringArray>>
StringOperations::Lower(const std::shared_ptr<arrow::Array>& arr) {
    ARROW_ASSIGN_OR_RAISE(
        auto result,
        arrow::compute::CallFunction("utf8_lower", {arr})
    );
    return std::static_pointer_cast<arrow::StringArray>(result.make_array());
}

// ========== Aggregations ==========

arrow::Result<std::string>
StringOperations::Min(const std::shared_ptr<arrow::Array>& arr) {
    ARROW_ASSIGN_OR_RAISE(
        auto result,
        arrow::compute::CallFunction("min", {arr})
    );
    auto scalar = result.scalar();
    return scalar->ToString();
}

arrow::Result<std::string>
StringOperations::Max(const std::shared_ptr<arrow::Array>& arr) {
    ARROW_ASSIGN_OR_RAISE(
        auto result,
        arrow::compute::CallFunction("max", {arr})
    );
    auto scalar = result.scalar();
    return scalar->ToString();
}

arrow::Result<int64_t>
StringOperations::CountDistinct(const std::shared_ptr<arrow::Array>& arr) {
    // Get unique values
    ARROW_ASSIGN_OR_RAISE(
        auto unique_result,
        arrow::compute::CallFunction("unique", {arr})
    );
    
    // Count them
    auto unique_array = unique_result.make_array();
    return unique_array->length();
}

} // namespace sql
} // namespace sabot_sql

