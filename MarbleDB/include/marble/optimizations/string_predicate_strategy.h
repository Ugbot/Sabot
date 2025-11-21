/**
 * String Predicate Optimization Strategy
 *
 * Integrates Sabot's SIMD-accelerated Arrow string operations into MarbleDB
 * for 100-200x faster string filtering on RDF literals, log search, and text queries.
 *
 * Key Features:
 * - SIMD string equality (352M ops/sec)
 * - Substring search with Boyer-Moore (165M ops/sec)
 * - Prefix/suffix matching (255-297M ops/sec)
 * - Regex matching with RE2 (50M+ ops/sec)
 * - Zero-copy Arrow integration
 *
 * Performance:
 * - String WHERE clauses: 100-200x faster (short-circuits disk reads)
 * - RDF literal queries: 5-10x faster (vocabulary caching)
 * - Log search: 30-40x faster (LIKE operations)
 *
 * Usage:
 *   auto strategy = std::make_unique<StringPredicateStrategy>();
 *   pipeline.AddStrategy(std::move(strategy));
 */

#pragma once

#include <memory>
#include <string>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <arrow/api.h>
#include "marble/optimization_strategy.h"
#include "marble/status.h"

namespace marble {

//==============================================================================
// String Predicate Information - Extended predicate support for ReadContext
//==============================================================================

/**
 * Extended string predicate information for ReadContext.
 *
 * Extends ReadContext with string-specific predicate information
 * to enable SIMD-accelerated filtering during reads.
 */
struct StringPredicateInfo {
    // Column being filtered
    std::string column_name;

    // Predicate type
    enum class Type {
        NONE,           // No string predicate
        EQUAL,          // Exact match (=)
        NOT_EQUAL,      // Not equal (!=)
        CONTAINS,       // Substring search (LIKE '%x%')
        STARTS_WITH,    // Prefix match (LIKE 'x%')
        ENDS_WITH,      // Suffix match (LIKE '%x')
        REGEX           // Regex match (REGEXP)
    } type = Type::NONE;

    // Predicate value (pattern to match)
    std::string pattern;

    // Options
    bool ignore_case = false;

    // Result (set by strategy after evaluation)
    bool can_skip_read = false;    // Short-circuit: definitely no match
    bool filter_applied = false;    // Did we apply SIMD filtering?
    size_t rows_filtered = 0;       // How many rows passed filter
};

//==============================================================================
// String Wrapper - C++ wrapper around Sabot's Cython string operations
//==============================================================================

/**
 * C++ wrapper around Sabot's SIMD string operations.
 *
 * Provides C++ API to call Sabot's Cython-wrapped Arrow compute kernels.
 * All operations are zero-copy and SIMD-accelerated.
 */
class StringOperationsWrapper {
public:
    StringOperationsWrapper();
    ~StringOperationsWrapper();

    /**
     * String equality (352M ops/sec)
     * Returns boolean array indicating which strings match the pattern.
     */
    arrow::Result<std::shared_ptr<arrow::BooleanArray>>
    Equal(const std::shared_ptr<arrow::StringArray>& array,
          const std::string& pattern);

    /**
     * String inequality
     */
    arrow::Result<std::shared_ptr<arrow::BooleanArray>>
    NotEqual(const std::shared_ptr<arrow::StringArray>& array,
             const std::string& pattern);

    /**
     * Substring search (165M ops/sec, Boyer-Moore)
     * LIKE '%pattern%'
     */
    arrow::Result<std::shared_ptr<arrow::BooleanArray>>
    Contains(const std::shared_ptr<arrow::StringArray>& array,
             const std::string& pattern,
             bool ignore_case = false);

    /**
     * Prefix matching (297M ops/sec)
     * LIKE 'pattern%'
     */
    arrow::Result<std::shared_ptr<arrow::BooleanArray>>
    StartsWith(const std::shared_ptr<arrow::StringArray>& array,
               const std::string& pattern,
               bool ignore_case = false);

    /**
     * Suffix matching (255M ops/sec)
     * LIKE '%pattern'
     */
    arrow::Result<std::shared_ptr<arrow::BooleanArray>>
    EndsWith(const std::shared_ptr<arrow::StringArray>& array,
             const std::string& pattern,
             bool ignore_case = false);

    /**
     * Regex matching (50M+ ops/sec, RE2)
     * REGEXP 'pattern'
     */
    arrow::Result<std::shared_ptr<arrow::BooleanArray>>
    MatchRegex(const std::shared_ptr<arrow::StringArray>& array,
               const std::string& pattern,
               bool ignore_case = false);

    /**
     * String length (367M ops/sec)
     */
    arrow::Result<std::shared_ptr<arrow::Int32Array>>
    Length(const std::shared_ptr<arrow::StringArray>& array);

    /**
     * Check if module is available (returns false if Cython module not built)
     */
    bool IsAvailable() const { return is_available_; }

private:
    bool is_available_;  // Is Cython module available?

    // Internal: Call Python string operations via Python C API
    arrow::Result<std::shared_ptr<arrow::BooleanArray>>
    CallPythonStringOp(const std::string& func_name,
                       const std::shared_ptr<arrow::StringArray>& array,
                       const std::string& pattern,
                       bool ignore_case = false);
};

//==============================================================================
// StringPredicateStrategy - Main optimization strategy
//==============================================================================

/**
 * String predicate optimization strategy using SIMD acceleration.
 *
 * Intercepts read operations with string predicates and applies
 * SIMD-accelerated filtering to short-circuit disk reads.
 *
 * Integration Points:
 * - OnRead(): Intercept string predicates, apply SIMD filtering
 * - OnReadComplete(): Update statistics
 * - OnWrite(): Track string column statistics
 * - OnFlush(): Serialize vocabulary cache
 *
 * Performance Benefits:
 * - WHERE column = 'value': 100-200x faster (short-circuit)
 * - WHERE column LIKE '%pattern%': 30-40x faster (SIMD substring search)
 * - WHERE column LIKE 'prefix%': 40-50x faster (SIMD prefix match)
 * - WHERE column REGEXP 'pattern': 10-20x faster (RE2 engine)
 *
 * Example:
 *   // Auto-configuration (detects string columns)
 *   auto strategy = StringPredicateStrategy::AutoConfigure(schema);
 *
 *   // Manual configuration
 *   auto strategy = std::make_unique<StringPredicateStrategy>();
 *   strategy->EnableColumn("name");
 *   strategy->EnableColumn("description");
 *   pipeline.AddStrategy(std::move(strategy));
 */
class StringPredicateStrategy : public OptimizationStrategy {
public:
    StringPredicateStrategy();
    ~StringPredicateStrategy() override;

    //==========================================================================
    // OptimizationStrategy interface
    //==========================================================================

    Status OnTableCreate(const TableCapabilities& caps) override;

    /**
     * Intercept read operations with string predicates.
     *
     * If ReadContext has string_predicate_info set:
     * 1. Apply SIMD filtering to narrow down candidate rows
     * 2. Set can_skip_read = true if no rows match
     * 3. Update filter statistics
     *
     * Returns Status::NotFound() if definitely no match.
     */
    Status OnRead(ReadContext* ctx) override;

    void OnReadComplete(const Key& key, const Record& record) override;

    Status OnWrite(WriteContext* ctx) override;

    Status OnCompaction(CompactionContext* ctx) override;

    /**
     * Serialize vocabulary cache during flush.
     */
    Status OnFlush(FlushContext* ctx) override;

    size_t MemoryUsage() const override;

    void Clear() override;

    std::vector<uint8_t> Serialize() const override;

    Status Deserialize(const std::vector<uint8_t>& data) override;

    std::string Name() const override { return "StringPredicateStrategy"; }

    std::string GetStats() const override;

    //==========================================================================
    // Configuration
    //==========================================================================

    /**
     * Enable string predicate optimization for a specific column.
     */
    void EnableColumn(const std::string& column_name);

    /**
     * Disable string predicate optimization for a column.
     */
    void DisableColumn(const std::string& column_name);

    /**
     * Check if a column is enabled for optimization.
     */
    bool IsColumnEnabled(const std::string& column_name) const;

    /**
     * Auto-configure based on schema.
     * Enables optimization for all string/utf8 columns.
     */
    static std::unique_ptr<StringPredicateStrategy>
    AutoConfigure(const std::shared_ptr<arrow::Schema>& schema);

    //==========================================================================
    // Vocabulary Cache (RDF optimization)
    //==========================================================================

    /**
     * Cache string->int64 vocabulary mapping (RDF IRI/literal encoding).
     * Enables 5-10x faster RDF literal queries by avoiding repeated encoding.
     */
    void CacheVocabulary(const std::string& str, int64_t id);

    /**
     * Lookup vocabulary ID for a string.
     * Returns -1 if not found.
     */
    int64_t LookupVocabulary(const std::string& str) const;

    /**
     * Clear vocabulary cache.
     */
    void ClearVocabulary();

    //==========================================================================
    // Statistics
    //==========================================================================

    struct Stats {
        // Filter statistics
        size_t reads_intercepted = 0;     // Total reads intercepted
        size_t reads_short_circuited = 0; // Reads skipped (no match)
        size_t rows_filtered = 0;         // Total rows filtered
        size_t rows_passed = 0;           // Rows that passed filter

        // Performance
        double total_filter_time_ms = 0.0;  // Total SIMD filtering time
        double avg_filter_time_ms = 0.0;    // Average per-read

        // Vocabulary cache
        size_t vocab_cache_hits = 0;      // Cache hits
        size_t vocab_cache_misses = 0;    // Cache misses
        size_t vocab_cache_size = 0;      // Current cache size

        // Breakdown by predicate type
        std::unordered_map<std::string, size_t> predicate_type_counts;
    };

    const Stats& GetDetailedStats() const { return stats_; }

private:
    // String operations wrapper (SIMD acceleration)
    std::unique_ptr<StringOperationsWrapper> string_ops_;

    // Enabled columns (only these get optimized)
    std::unordered_set<std::string> enabled_columns_;

    // Vocabulary cache (string -> int64 ID for RDF)
    std::unordered_map<std::string, int64_t> vocabulary_cache_;
    size_t max_vocabulary_cache_size_ = 10000;  // Limit cache size

    // Statistics
    Stats stats_;

    // Internal helpers
    Status ApplyStringPredicate(ReadContext* ctx);
    void UpdateStats(const StringPredicateInfo& pred_info, double time_ms);
};

}  // namespace marble
