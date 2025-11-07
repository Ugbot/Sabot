/**
 * OptimizationFactory - Auto-configures optimization strategies
 *
 * Detects schema type and workload patterns to automatically select
 * appropriate optimization strategies for each table.
 */

#pragma once

#include "marble/optimization_strategy.h"
#include "marble/table_capabilities.h"
#include <memory>
#include <arrow/api.h>

namespace marble {

//==============================================================================
// Schema Type Detection
//==============================================================================

/**
 * Detected schema type from Arrow schema inspection.
 */
enum class SchemaType {
    UNKNOWN = 0,
    RDF_TRIPLE,       // 3 int64 columns (subject, predicate, object)
    KEY_VALUE,        // 2 columns (key, value)
    TIME_SERIES,      // Has timestamp column + numeric columns
    PROPERTY_GRAPH,   // Complex nested schema (vertices/edges)
    DOCUMENT          // Flexible schema with string/binary columns
};

/**
 * Convert SchemaType to human-readable string.
 */
inline const char* SchemaTypeToString(SchemaType type) {
    switch (type) {
        case SchemaType::UNKNOWN: return "Unknown";
        case SchemaType::RDF_TRIPLE: return "RDF Triple";
        case SchemaType::KEY_VALUE: return "Key-Value";
        case SchemaType::TIME_SERIES: return "Time-Series";
        case SchemaType::PROPERTY_GRAPH: return "Property Graph";
        case SchemaType::DOCUMENT: return "Document";
        default: return "Invalid";
    }
}

//==============================================================================
// OptimizationFactory
//==============================================================================

/**
 * Factory for creating optimization pipelines.
 *
 * Two main use cases:
 * 1. Auto-configuration: Inspect schema and create appropriate strategies
 * 2. Manual configuration: Create specific strategies on demand
 *
 * Example auto-configuration:
 *   auto pipeline = OptimizationFactory::CreateForSchema(schema, capabilities, hints);
 *
 * Example manual configuration:
 *   OptimizationPipeline pipeline;
 *   pipeline.AddStrategy(OptimizationFactory::CreateBloomFilter(...));
 *   pipeline.AddStrategy(OptimizationFactory::CreateCache(...));
 */
class OptimizationFactory {
public:
    //==========================================================================
    // Auto-configuration
    //==========================================================================

    /**
     * Create optimization pipeline for a table.
     *
     * Automatically detects schema type and selects appropriate strategies
     * based on schema, capabilities, and workload hints.
     *
     * @param schema Arrow schema of the table
     * @param caps Table capabilities (has hot key cache, etc.)
     * @param hints Optional workload hints for tuning
     * @return Configured optimization pipeline
     */
    static std::unique_ptr<OptimizationPipeline> CreateForSchema(
        const std::shared_ptr<arrow::Schema>& schema,
        const TableCapabilities& caps,
        const WorkloadHints& hints = WorkloadHints());

    /**
     * Detect schema type from Arrow schema.
     *
     * Heuristics:
     * - RDF_TRIPLE: Exactly 3 int64 columns with names like (S,P,O) or (subject,predicate,object)
     * - KEY_VALUE: Exactly 2 columns
     * - TIME_SERIES: Has timestamp column (name contains "time", "timestamp", "ts", "date")
     * - PROPERTY_GRAPH: Nested struct/list columns
     * - DOCUMENT: Mostly string/binary columns
     *
     * @param schema Arrow schema to inspect
     * @return Detected schema type
     */
    static SchemaType DetectSchemaType(const std::shared_ptr<arrow::Schema>& schema);

    /**
     * Create optimization pipeline for a specific schema type.
     *
     * Uses default configuration for each schema type.
     */
    static std::unique_ptr<OptimizationPipeline> CreateForSchemaType(
        SchemaType type,
        const TableCapabilities& caps,
        const WorkloadHints& hints = WorkloadHints());

    //==========================================================================
    // Manual strategy creation
    //==========================================================================

    /**
     * Create BloomFilterStrategy.
     *
     * @param expected_keys Expected number of keys (for sizing bloom filter)
     * @param false_positive_rate Target false positive rate (default: 0.01 = 1%)
     * @return BloomFilterStrategy instance
     */
    static std::unique_ptr<OptimizationStrategy> CreateBloomFilter(
        size_t expected_keys = 1000000,
        double false_positive_rate = 0.01);

    /**
     * Create CacheStrategy.
     *
     * @param hot_key_capacity Maximum number of hot keys to cache
     * @param negative_cache_capacity Maximum number of negative entries to cache
     * @return CacheStrategy instance
     */
    static std::unique_ptr<OptimizationStrategy> CreateCache(
        size_t hot_key_capacity = 10000,
        size_t negative_cache_capacity = 1000);

    /**
     * Create SkippingIndexStrategy.
     *
     * @param block_size Number of rows per block for statistics
     * @return SkippingIndexStrategy instance
     */
    static std::unique_ptr<OptimizationStrategy> CreateSkippingIndex(
        size_t block_size = 8192);

    /**
     * Create TripleStoreStrategy (RDF-specific optimizations).
     *
     * @param enable_predicate_bloom Enable per-predicate bloom filters
     * @param enable_join_hints Enable join order hints
     * @return TripleStoreStrategy instance
     */
    static std::unique_ptr<OptimizationStrategy> CreateTripleStore(
        bool enable_predicate_bloom = true,
        bool enable_join_hints = true);

    //==========================================================================
    // Configuration helpers
    //==========================================================================

    /**
     * Estimate optimal bloom filter parameters.
     *
     * @param expected_keys Expected number of keys
     * @param memory_budget_bytes Maximum memory for bloom filter
     * @return (num_bits, num_hash_functions, expected_fpr)
     */
    static std::tuple<size_t, size_t, double> EstimateBloomFilterParams(
        size_t expected_keys,
        size_t memory_budget_bytes);

    /**
     * Estimate optimal cache capacity.
     *
     * @param total_keys Total number of keys in dataset
     * @param access_skewness Skewness of access pattern (0.0 = uniform, 1.0 = highly skewed)
     * @param memory_budget_bytes Maximum memory for cache
     * @param avg_value_size Average value size in bytes
     * @return Recommended cache capacity
     */
    static size_t EstimateCacheCapacity(
        size_t total_keys,
        double access_skewness,
        size_t memory_budget_bytes,
        size_t avg_value_size);

    /**
     * Estimate optimal skipping index block size.
     *
     * @param avg_record_size Average record size in bytes
     * @param typical_selectivity Typical query selectivity (0.0-1.0)
     * @return Recommended block size (number of rows)
     */
    static size_t EstimateBlockSize(
        size_t avg_record_size,
        double typical_selectivity);

private:
    //==========================================================================
    // Internal helpers
    //==========================================================================

    /**
     * Check if schema matches RDF triple pattern.
     */
    static bool IsRDFTripleSchema(const std::shared_ptr<arrow::Schema>& schema);

    /**
     * Check if schema matches key-value pattern.
     */
    static bool IsKeyValueSchema(const std::shared_ptr<arrow::Schema>& schema);

    /**
     * Check if schema has timestamp column (time-series).
     */
    static bool HasTimestampColumn(const std::shared_ptr<arrow::Schema>& schema);

    /**
     * Check if schema has nested structures (property graph).
     */
    static bool HasNestedStructures(const std::shared_ptr<arrow::Schema>& schema);

    /**
     * Check if schema is mostly strings/binary (document store).
     */
    static bool IsDocumentSchema(const std::shared_ptr<arrow::Schema>& schema);

    /**
     * Apply workload hints to refine auto-configuration.
     */
    static void ApplyWorkloadHints(
        OptimizationPipeline* pipeline,
        SchemaType detected_type,
        const WorkloadHints& hints,
        const TableCapabilities& caps);
};

}  // namespace marble
