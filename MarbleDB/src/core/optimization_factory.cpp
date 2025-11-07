/**
 * OptimizationFactory implementation
 */

#include "marble/optimization_factory.h"
#include "marble/bloom_filter_strategy.h"
#include "marble/cache_strategy.h"
#include "marble/skipping_index_strategy.h"
#include <algorithm>
#include <cmath>
#include <cctype>

namespace marble {

//==============================================================================
// Schema Type Detection
//==============================================================================

SchemaType OptimizationFactory::DetectSchemaType(const std::shared_ptr<arrow::Schema>& schema) {
    if (!schema) {
        return SchemaType::UNKNOWN;
    }

    // Check RDF triple pattern first (most specific)
    if (IsRDFTripleSchema(schema)) {
        return SchemaType::RDF_TRIPLE;
    }

    // Check key-value pattern (2 columns)
    if (IsKeyValueSchema(schema)) {
        return SchemaType::KEY_VALUE;
    }

    // Check time-series pattern (has timestamp)
    if (HasTimestampColumn(schema)) {
        return SchemaType::TIME_SERIES;
    }

    // Check property graph pattern (nested structures)
    if (HasNestedStructures(schema)) {
        return SchemaType::PROPERTY_GRAPH;
    }

    // Check document store pattern (mostly strings/binary)
    if (IsDocumentSchema(schema)) {
        return SchemaType::DOCUMENT;
    }

    return SchemaType::UNKNOWN;
}

bool OptimizationFactory::IsRDFTripleSchema(const std::shared_ptr<arrow::Schema>& schema) {
    // RDF triple: Exactly 3 int64 columns
    if (schema->num_fields() != 3) {
        return false;
    }

    // All columns must be int64
    for (int i = 0; i < 3; ++i) {
        auto field = schema->field(i);
        if (field->type()->id() != arrow::Type::INT64) {
            return false;
        }
    }

    // Check for common RDF triple column names
    auto f0 = schema->field(0)->name();
    auto f1 = schema->field(1)->name();
    auto f2 = schema->field(2)->name();

    // Convert to lowercase for comparison
    std::string n0 = f0, n1 = f1, n2 = f2;
    std::transform(n0.begin(), n0.end(), n0.begin(), ::tolower);
    std::transform(n1.begin(), n1.end(), n1.begin(), ::tolower);
    std::transform(n2.begin(), n2.end(), n2.begin(), ::tolower);

    // Common patterns:
    // (s, p, o), (subject, predicate, object), (sub, pred, obj)
    bool has_subject = (n0 == "s" || n0 == "subject" || n0 == "sub");
    bool has_predicate = (n1 == "p" || n1 == "predicate" || n1 == "pred");
    bool has_object = (n2 == "o" || n2 == "object" || n2 == "obj");

    return has_subject && has_predicate && has_object;
}

bool OptimizationFactory::IsKeyValueSchema(const std::shared_ptr<arrow::Schema>& schema) {
    // Key-value: Exactly 2 columns
    return schema->num_fields() == 2;
}

bool OptimizationFactory::HasTimestampColumn(const std::shared_ptr<arrow::Schema>& schema) {
    // Look for timestamp column by name or type
    for (int i = 0; i < schema->num_fields(); ++i) {
        auto field = schema->field(i);
        auto type_id = field->type()->id();

        // Check for timestamp types
        if (type_id == arrow::Type::TIMESTAMP ||
            type_id == arrow::Type::DATE32 ||
            type_id == arrow::Type::DATE64 ||
            type_id == arrow::Type::TIME32 ||
            type_id == arrow::Type::TIME64) {
            return true;
        }

        // Check for common timestamp column names
        std::string name = field->name();
        std::transform(name.begin(), name.end(), name.begin(), ::tolower);

        if (name.find("time") != std::string::npos ||
            name.find("timestamp") != std::string::npos ||
            name.find("ts") != std::string::npos ||
            name.find("date") != std::string::npos ||
            name.find("created") != std::string::npos ||
            name.find("updated") != std::string::npos) {
            return true;
        }
    }

    return false;
}

bool OptimizationFactory::HasNestedStructures(const std::shared_ptr<arrow::Schema>& schema) {
    // Check for struct, list, or map columns
    for (int i = 0; i < schema->num_fields(); ++i) {
        auto field = schema->field(i);
        auto type_id = field->type()->id();

        if (type_id == arrow::Type::STRUCT ||
            type_id == arrow::Type::LIST ||
            type_id == arrow::Type::LARGE_LIST ||
            type_id == arrow::Type::FIXED_SIZE_LIST ||
            type_id == arrow::Type::MAP) {
            return true;
        }
    }

    return false;
}

bool OptimizationFactory::IsDocumentSchema(const std::shared_ptr<arrow::Schema>& schema) {
    // Document store: Mostly string/binary columns
    int string_binary_count = 0;
    int total_fields = schema->num_fields();

    for (int i = 0; i < total_fields; ++i) {
        auto field = schema->field(i);
        auto type_id = field->type()->id();

        if (type_id == arrow::Type::STRING ||
            type_id == arrow::Type::LARGE_STRING ||
            type_id == arrow::Type::BINARY ||
            type_id == arrow::Type::LARGE_BINARY) {
            string_binary_count++;
        }
    }

    // Consider it a document schema if >50% columns are string/binary
    return total_fields > 2 && string_binary_count > total_fields / 2;
}

//==============================================================================
// Auto-configuration
//==============================================================================

std::unique_ptr<OptimizationPipeline> OptimizationFactory::CreateForSchema(
    const std::shared_ptr<arrow::Schema>& schema,
    const TableCapabilities& caps,
    const WorkloadHints& hints) {

    // Detect schema type
    SchemaType type = DetectSchemaType(schema);

    // Create pipeline for detected type
    return CreateForSchemaType(type, caps, hints);
}

std::unique_ptr<OptimizationPipeline> OptimizationFactory::CreateForSchemaType(
    SchemaType type,
    const TableCapabilities& caps,
    const WorkloadHints& hints) {

    auto pipeline = std::make_unique<OptimizationPipeline>();

    // Apply workload hints to customize configuration
    ApplyWorkloadHints(pipeline.get(), type, hints, caps);

    return pipeline;
}

void OptimizationFactory::ApplyWorkloadHints(
    OptimizationPipeline* pipeline,
    SchemaType detected_type,
    const WorkloadHints& hints,
    const TableCapabilities& caps) {

    // Manual overrides (highest priority)
    if (hints.force_bloom_filter) {
        // Will add BloomFilterStrategy when implemented
        // pipeline->AddStrategy(CreateBloomFilter(...));
    }

    if (hints.force_cache) {
        // Will add CacheStrategy when implemented
        // pipeline->AddStrategy(CreateCache(...));
    }

    if (hints.force_skipping_index) {
        pipeline->AddStrategy(CreateSkippingIndex(8192));
    }

    if (hints.force_triple_store) {
        // Will add TripleStoreStrategy when implemented
        // pipeline->AddStrategy(CreateTripleStore(...));
    }

    // If manual overrides present, skip auto-configuration
    if (hints.force_bloom_filter || hints.force_cache ||
        hints.force_skipping_index || hints.force_triple_store) {
        return;
    }

    // Auto-configuration based on detected type and hints
    switch (detected_type) {
        case SchemaType::RDF_TRIPLE:
            // RDF: Bloom filter + TripleStore optimizations
            pipeline->AddStrategy(CreateBloomFilter(1000000, 0.01));
            // TODO: Add TripleStore strategy when implemented
            // pipeline->AddStrategy(CreateTripleStore(true, true));
            break;

        case SchemaType::KEY_VALUE:
            // Key-value: Bloom filter for existence checks
            if (hints.access_pattern == WorkloadHints::POINT_LOOKUP ||
                hints.access_pattern == WorkloadHints::MIXED ||
                hints.access_pattern == WorkloadHints::UNKNOWN) {
                pipeline->AddStrategy(CreateBloomFilter(1000000, 0.01));

                // Add cache if hot keys detected
                if (hints.has_hot_keys || caps.enable_hot_key_cache) {
                    pipeline->AddStrategy(CreateCache(10000, 1000));
                }
            }
            break;

        case SchemaType::TIME_SERIES:
            // Time-series: Skipping index for range queries
            if (hints.access_pattern == WorkloadHints::RANGE_SCAN ||
                hints.access_pattern == WorkloadHints::MIXED) {
                pipeline->AddStrategy(CreateSkippingIndex(8192));
            }
            break;

        case SchemaType::PROPERTY_GRAPH:
            // Property graph: Similar to RDF but without triple-specific opts
            pipeline->AddStrategy(CreateBloomFilter(1000000, 0.01));
            if (hints.has_hot_keys || caps.enable_hot_key_cache) {
                pipeline->AddStrategy(CreateCache(10000, 1000));
            }
            break;

        case SchemaType::DOCUMENT:
            // Document store: Bloom filter + cache for lookups
            if (hints.access_pattern != WorkloadHints::RANGE_SCAN) {
                pipeline->AddStrategy(CreateBloomFilter(1000000, 0.01));
                if (hints.has_hot_keys) {
                    pipeline->AddStrategy(CreateCache(10000, 1000));
                }
            }
            break;

        case SchemaType::UNKNOWN:
        default:
            // Unknown: Use conservative defaults based on hints
            if (hints.access_pattern == WorkloadHints::POINT_LOOKUP) {
                pipeline->AddStrategy(CreateBloomFilter(1000000, 0.01));
            } else if (hints.access_pattern == WorkloadHints::RANGE_SCAN) {
                pipeline->AddStrategy(CreateSkippingIndex(8192));
            }
            break;
    }
}

//==============================================================================
// Manual strategy creation (skeleton implementations)
//==============================================================================

std::unique_ptr<OptimizationStrategy> OptimizationFactory::CreateBloomFilter(
    size_t expected_keys,
    double false_positive_rate) {
    return std::make_unique<BloomFilterStrategy>(expected_keys, false_positive_rate);
}

std::unique_ptr<OptimizationStrategy> OptimizationFactory::CreateCache(
    size_t hot_key_capacity,
    size_t negative_cache_capacity) {
    return std::make_unique<CacheStrategy>(hot_key_capacity, negative_cache_capacity);
}

std::unique_ptr<OptimizationStrategy> OptimizationFactory::CreateSkippingIndex(
    size_t block_size) {
    return std::make_unique<SkippingIndexStrategy>(static_cast<int64_t>(block_size));
}

std::unique_ptr<OptimizationStrategy> OptimizationFactory::CreateTripleStore(
    bool enable_predicate_bloom,
    bool enable_join_hints) {
    // TODO: Implement when TripleStoreStrategy is available
    // return std::make_unique<TripleStoreStrategy>(enable_predicate_bloom, enable_join_hints);
    return nullptr;
}

//==============================================================================
// Configuration helpers
//==============================================================================

std::tuple<size_t, size_t, double> OptimizationFactory::EstimateBloomFilterParams(
    size_t expected_keys,
    size_t memory_budget_bytes) {

    if (expected_keys == 0) {
        return {0, 0, 1.0};
    }

    size_t num_bits = memory_budget_bytes * 8;

    // Optimal number of hash functions: k = (m/n) * ln(2)
    // where m = num_bits, n = expected_keys
    double bits_per_key = static_cast<double>(num_bits) / expected_keys;
    size_t num_hash_functions = static_cast<size_t>(std::ceil(bits_per_key * std::log(2)));

    // Clamp to reasonable range
    num_hash_functions = std::max<size_t>(1, std::min<size_t>(num_hash_functions, 10));

    // Calculate expected false positive rate: (1 - e^(-kn/m))^k
    double exponent = -static_cast<double>(num_hash_functions * expected_keys) / num_bits;
    double fpr = std::pow(1.0 - std::exp(exponent), num_hash_functions);

    return {num_bits, num_hash_functions, fpr};
}

size_t OptimizationFactory::EstimateCacheCapacity(
    size_t total_keys,
    double access_skewness,
    size_t memory_budget_bytes,
    size_t avg_value_size) {

    if (avg_value_size == 0 || memory_budget_bytes == 0) {
        return 0;
    }

    // Simple capacity calculation
    size_t max_capacity = memory_budget_bytes / avg_value_size;

    // Adjust based on skewness
    // High skewness (0.8-1.0): Cache 10-20% of keys (captures most access)
    // Medium skewness (0.4-0.8): Cache 30-50% of keys
    // Low skewness (0.0-0.4): Cache 50-100% of keys (more uniform access)

    double capacity_ratio;
    if (access_skewness > 0.8) {
        capacity_ratio = 0.15;  // 15%
    } else if (access_skewness > 0.4) {
        capacity_ratio = 0.40;  // 40%
    } else {
        capacity_ratio = 0.70;  // 70%
    }

    size_t desired_capacity = static_cast<size_t>(total_keys * capacity_ratio);

    return std::min(desired_capacity, max_capacity);
}

size_t OptimizationFactory::EstimateBlockSize(
    size_t avg_record_size,
    double typical_selectivity) {

    // Target block size: 64KB - 256KB (good for I/O and compression)
    const size_t target_block_bytes = 128 * 1024;  // 128KB

    // Calculate number of rows per block
    size_t rows_per_block = target_block_bytes / std::max<size_t>(avg_record_size, 1);

    // Adjust based on selectivity
    // High selectivity (0.8-1.0): Small blocks (better pruning)
    // Low selectivity (0.0-0.2): Large blocks (fewer metadata overhead)

    if (typical_selectivity > 0.8) {
        rows_per_block /= 2;  // Smaller blocks for high selectivity
    } else if (typical_selectivity < 0.2) {
        rows_per_block *= 2;  // Larger blocks for low selectivity
    }

    // Clamp to reasonable range
    return std::max<size_t>(1024, std::min<size_t>(rows_per_block, 65536));
}

}  // namespace marble
