#pragma once

#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <string>
#include <memory>

namespace sabot_sql {
namespace sql {

/**
 * Arrow-based string operations using SIMD-optimized compute kernels
 * 
 * Provides high-performance string operations by wrapping Arrow's
 * compute functions which are already SIMD-optimized.
 */
class StringOperations {
public:
    // ========== Comparison Operations ==========
    
    /**
     * String equality: arr == value
     * Uses Arrow's SIMD-optimized equal kernel
     */
    static arrow::Result<std::shared_ptr<arrow::BooleanArray>>
    Equal(const std::shared_ptr<arrow::Array>& arr, const std::string& value);
    
    /**
     * String inequality: arr != value
     * Uses Arrow's SIMD-optimized not_equal kernel
     */
    static arrow::Result<std::shared_ptr<arrow::BooleanArray>>
    NotEqual(const std::shared_ptr<arrow::Array>& arr, const std::string& value);
    
    /**
     * Is empty string: arr == ""
     * Optimized for common empty string checks
     */
    static arrow::Result<std::shared_ptr<arrow::BooleanArray>>
    IsEmpty(const std::shared_ptr<arrow::Array>& arr);
    
    /**
     * Is not empty string: arr != ""
     * Optimized for common non-empty checks
     */
    static arrow::Result<std::shared_ptr<arrow::BooleanArray>>
    IsNotEmpty(const std::shared_ptr<arrow::Array>& arr);
    
    // ========== Pattern Matching ==========
    
    /**
     * Substring match: arr LIKE '%pattern%'
     * Uses Arrow's match_substring kernel
     */
    static arrow::Result<std::shared_ptr<arrow::BooleanArray>>
    Contains(const std::shared_ptr<arrow::Array>& arr, const std::string& pattern);
    
    /**
     * SQL LIKE: arr LIKE pattern
     * Supports % and _ wildcards
     */
    static arrow::Result<std::shared_ptr<arrow::BooleanArray>>
    MatchLike(const std::shared_ptr<arrow::Array>& arr, const std::string& pattern);
    
    /**
     * Regex match: arr ~ pattern
     * Uses Arrow's match_substring_regex kernel
     */
    static arrow::Result<std::shared_ptr<arrow::BooleanArray>>
    MatchRegex(const std::shared_ptr<arrow::Array>& arr, const std::string& pattern);
    
    // ========== String Functions ==========
    
    /**
     * String length: STRLEN(arr) or LENGTH(arr)
     * Uses Arrow's utf8_length kernel
     */
    static arrow::Result<std::shared_ptr<arrow::Int32Array>>
    Length(const std::shared_ptr<arrow::Array>& arr);
    
    /**
     * String replace: REPLACE(arr, pattern, replacement)
     * Uses Arrow's replace_substring kernel
     */
    static arrow::Result<std::shared_ptr<arrow::StringArray>>
    Replace(const std::shared_ptr<arrow::Array>& arr, 
            const std::string& pattern, 
            const std::string& replacement);
    
    /**
     * Regex replace: REGEXP_REPLACE(arr, pattern, replacement)
     * Uses Arrow's replace_substring_regex kernel
     */
    static arrow::Result<std::shared_ptr<arrow::StringArray>>
    ReplaceRegex(const std::shared_ptr<arrow::Array>& arr,
                 const std::string& pattern,
                 const std::string& replacement);
    
    /**
     * Uppercase: UPPER(arr)
     * Uses Arrow's utf8_upper kernel
     */
    static arrow::Result<std::shared_ptr<arrow::StringArray>>
    Upper(const std::shared_ptr<arrow::Array>& arr);
    
    /**
     * Lowercase: LOWER(arr)
     * Uses Arrow's utf8_lower kernel
     */
    static arrow::Result<std::shared_ptr<arrow::StringArray>>
    Lower(const std::shared_ptr<arrow::Array>& arr);
    
    // ========== Aggregations ==========
    
    /**
     * Minimum string: MIN(arr)
     * Uses Arrow's min_element_wise kernel
     */
    static arrow::Result<std::string>
    Min(const std::shared_ptr<arrow::Array>& arr);
    
    /**
     * Maximum string: MAX(arr)
     * Uses Arrow's max_element_wise kernel
     */
    static arrow::Result<std::string>
    Max(const std::shared_ptr<arrow::Array>& arr);
    
    /**
     * Count distinct strings: COUNT(DISTINCT arr)
     * Uses Arrow's unique kernel + count
     */
    static arrow::Result<int64_t>
    CountDistinct(const std::shared_ptr<arrow::Array>& arr);
};

} // namespace sql
} // namespace sabot_sql

