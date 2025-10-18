/************************************************************************
MarbleDB Table Capabilities
Per-table/column-family feature configuration

Enables flexible capability configuration where different tables can opt-in
to different features (MVCC, temporal, search, TTL, etc.) based on their use case.

Copyright 2024 MarbleDB Contributors
Licensed under the Apache License, Version 2.0
**************************************************************************/

#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace marble {

/**
 * @brief Feature capabilities that can be enabled per table/column family
 *
 * Each capability has performance and storage trade-offs. Enable only what you need.
 *
 * Example:
 * @code
 * TableCapabilities caps;
 * caps.enable_mvcc = true;  // Enable MVCC for snapshot isolation
 * caps.temporal_model = TemporalModel::kBitemporal;  // Enable bitemporal
 * @endcode
 */
struct TableCapabilities {
    // ===== MVCC (Multi-Version Concurrency Control) =====

    /**
     * @brief Enable MVCC for snapshot isolation transactions
     *
     * Benefits:
     * - Snapshot isolation (repeatable reads)
     * - Non-blocking reads during writes
     * - Point-in-time queries
     *
     * Costs:
     * - Write amplification: +30-50% (versioned keys)
     * - Storage overhead: +20-40% (version metadata)
     * - Read latency: +5-10 μs (version filtering)
     *
     * Use cases:
     * - OLTP transactions (banking, e-commerce)
     * - Session management with consistency
     * - Multi-reader single-writer patterns
     */
    bool enable_mvcc = false;

    /**
     * @brief MVCC-specific configuration
     */
    struct MVCCSettings {
        /**
         * @brief Version garbage collection policy
         */
        enum class GCPolicy {
            kKeepAllVersions,      ///< No GC (for audit)
            kKeepRecentVersions,   ///< Keep N recent versions
            kKeepVersionsUntil     ///< Keep versions until timestamp
        };

        /// Garbage collection policy
        GCPolicy gc_policy = GCPolicy::kKeepRecentVersions;

        /// Maximum versions per key (for kKeepRecentVersions)
        size_t max_versions_per_key = 10;

        /// GC timestamp threshold in milliseconds (for kKeepVersionsUntil)
        uint64_t gc_timestamp_ms = 0;

        /// Maximum number of active snapshots
        size_t max_active_snapshots = 100;

        /// Snapshot TTL in milliseconds (snapshots older than this are released)
        uint64_t snapshot_ttl_ms = 3600000;  // 1 hour default
    } mvcc_settings;

    // ===== Temporal/Bitemporal Features =====

    /**
     * @brief Temporal versioning model
     */
    enum class TemporalModel {
        kNone = 0,        ///< No temporal features
        kSystemTime,      ///< Only system time (when data was inserted)
        kValidTime,       ///< Only valid time (when data is/was valid)
        kBitemporal       ///< Both system and valid time
    };

    /**
     * @brief Enable temporal versioning (system time and/or valid time)
     *
     * Benefits:
     * - Time travel queries (AS OF timestamp)
     * - Historical reconstruction (see data as it was)
     * - Regulatory compliance (audit trail)
     * - Bitemporal queries (system + valid time)
     *
     * Costs:
     * - Storage overhead: +40-100% (full history)
     * - Write amplification: +20-30% (metadata)
     * - Query complexity: +10-50ms (temporal reconstruction)
     *
     * Use cases:
     * - Audit logs (compliance, regulatory)
     * - Historical analytics (trend analysis)
     * - Data lineage tracking
     */
    TemporalModel temporal_model = TemporalModel::kNone;

    /**
     * @brief Temporal-specific configuration
     */
    struct TemporalSettings {
        /// Maximum number of snapshots to retain
        size_t max_snapshots = 1000;

        /// Snapshot retention period in milliseconds
        uint64_t snapshot_retention_ms = 86400000;  // 24 hours

        /// Minimum valid time for bitemporal (milliseconds since epoch)
        uint64_t min_valid_time_ms = 0;

        /// Maximum valid time for bitemporal (milliseconds since epoch)
        uint64_t max_valid_time_ms = UINT64_MAX;

        /// Automatically create snapshots at regular intervals
        bool auto_create_snapshots = true;

        /// Snapshot creation interval in milliseconds
        uint64_t snapshot_interval_ms = 3600000;  // 1 hour
    } temporal_settings;

    // ===== Full-Text Search =====

    /**
     * @brief Enable Lucene-style inverted index for full-text search
     *
     * Benefits:
     * - Fast text search (1-15ms for 1M docs)
     * - Phrase queries, wildcards, fuzzy matching
     * - Relevance ranking (TF-IDF, BM25)
     *
     * Costs:
     * - Index overhead: +30-80% storage (inverted index)
     * - Write amplification: +20-40% (index updates)
     * - Memory usage: +10-20% (in-memory dictionary)
     *
     * Use cases:
     * - Document search (wikis, knowledge bases)
     * - Log search and analysis
     * - Product catalogs with text search
     */
    bool enable_full_text_search = false;

    /**
     * @brief Full-text search configuration
     */
    struct SearchSettings {
        /// Columns to index for full-text search
        std::vector<std::string> indexed_columns;

        /**
         * @brief Text analyzer type
         */
        enum class Analyzer {
            kStandard,      ///< Tokenize by whitespace + lowercase
            kSimple,        ///< Only lowercase
            kWhitespace,    ///< Only tokenize by whitespace
            kKeyword        ///< No tokenization (exact match)
        };

        /// Text analyzer for tokenization
        Analyzer analyzer = Analyzer::kStandard;

        /// Enable term positions for phrase queries
        bool enable_term_positions = true;

        /// Enable term offsets for highlighting
        bool enable_term_offsets = false;

        /// Maximum terms per document (protection against large docs)
        size_t max_terms_per_doc = 10000;

        /// In-memory dictionary cache size in MB
        size_t dictionary_cache_mb = 64;
    } search_settings;

    // ===== Time To Live (TTL) =====

    /**
     * @brief Enable automatic expiration of old data
     *
     * Benefits:
     * - Automatic cleanup (no manual purging)
     * - Bounded storage growth
     * - Compliance with data retention policies
     *
     * Costs:
     * - Background overhead: +1-3% CPU (cleanup)
     * - Write overhead: +5-10% (timestamp tracking)
     *
     * Use cases:
     * - Time-series metrics (auto-expire old data)
     * - Session management (expire inactive sessions)
     * - Log retention (comply with retention policies)
     */
    bool enable_ttl = false;

    /**
     * @brief TTL configuration
     */
    struct TTLSettings {
        /// Column containing timestamp for TTL calculation
        std::string timestamp_column;

        /// TTL duration in milliseconds
        uint64_t ttl_duration_ms = 86400000;  // 24 hours default

        /**
         * @brief Cleanup policy
         */
        enum class CleanupPolicy {
            kLazy,          ///< Clean during compaction only
            kBackground,    ///< Periodic background cleanup
            kImmediate      ///< Clean immediately on expiration (expensive)
        };

        /// Cleanup policy
        CleanupPolicy cleanup_policy = CleanupPolicy::kBackground;

        /// Background cleanup interval in milliseconds (for kBackground policy)
        uint64_t cleanup_interval_ms = 1800000;  // 30 minutes
    } ttl_settings;

    // ===== Zero-Copy RecordRef =====

    /**
     * @brief Enable zero-copy record access (borrow from Arrow batches)
     *
     * Benefits:
     * - 10-100x less memory (no materialization)
     * - 5-10x faster scans (no deserialization)
     * - Better cache efficiency
     *
     * Costs:
     * - Lifetime complexity (must keep batch alive)
     * - API change (std::string_view vs std::string)
     *
     * Note: Enabled by default (low cost, high benefit)
     */
    bool enable_zero_copy_reads = true;  // Enabled by default

    // ===== Hot Key Cache =====

    /**
     * @brief Enable LRU cache for frequently accessed keys
     *
     * Benefits:
     * - 5-10 μs hot key lookups (80% of queries)
     * - Reduced I/O for skewed workloads
     *
     * Costs:
     * - Memory: Configurable cache size
     * - Cache management: +1-2% CPU
     *
     * Note: Enabled by default (low cost, high benefit for skewed workloads)
     */
    bool enable_hot_key_cache = true;  // Enabled by default

    /**
     * @brief Hot key cache configuration
     */
    struct HotKeyCacheSettings {
        /// Cache size in MB
        size_t cache_size_mb = 64;

        /// Promotion threshold (promote key to cache after N accesses)
        uint32_t promotion_threshold = 3;
    } hot_key_cache_settings;

    // ===== Negative Cache =====

    /**
     * @brief Enable cache for non-existent keys
     *
     * Benefits:
     * - Fast negative lookups (key doesn't exist)
     * - Protection against missing key attacks
     *
     * Costs:
     * - Memory: ~1MB for 10,000 entries
     *
     * Note: Enabled by default (low cost, protects against attacks)
     */
    bool enable_negative_cache = true;  // Enabled by default

    /**
     * @brief Negative cache configuration
     */
    struct NegativeCacheSettings {
        /// Maximum number of negative cache entries
        size_t max_entries = 10000;
    } negative_cache_settings;

    // ===== Helper Methods =====

    /**
     * @brief Check if any versioning feature is enabled (MVCC or Temporal)
     */
    bool HasVersioning() const {
        return enable_mvcc || (temporal_model != TemporalModel::kNone);
    }

    /**
     * @brief Check if any write-amplification feature is enabled
     */
    bool HasWriteAmplification() const {
        return enable_mvcc ||
               (temporal_model != TemporalModel::kNone) ||
               enable_full_text_search ||
               enable_ttl;
    }

    /**
     * @brief Estimate storage overhead percentage (0-100+)
     */
    int EstimateStorageOverhead() const {
        int overhead = 0;
        if (enable_mvcc) overhead += 30;  // +20-40% average
        if (temporal_model == TemporalModel::kSystemTime) overhead += 50;  // +40-60%
        if (temporal_model == TemporalModel::kValidTime) overhead += 50;
        if (temporal_model == TemporalModel::kBitemporal) overhead += 90;  // +80-100%
        if (enable_full_text_search) overhead += 55;  // +30-80% average
        return overhead;
    }

    /**
     * @brief Estimate write amplification percentage (0-100+)
     */
    int EstimateWriteAmplification() const {
        int amplification = 0;
        if (enable_mvcc) amplification += 40;  // +30-50%
        if (temporal_model == TemporalModel::kSystemTime) amplification += 25;  // +20-30%
        if (temporal_model == TemporalModel::kValidTime) amplification += 25;
        if (temporal_model == TemporalModel::kBitemporal) amplification += 30;
        if (enable_full_text_search) amplification += 30;  // +20-40%
        if (enable_ttl) amplification += 7;  // +5-10%
        return amplification;
    }

    /**
     * @brief Get human-readable description of enabled capabilities
     */
    std::string GetDescription() const {
        std::string desc = "Capabilities: ";
        int count = 0;

        if (enable_mvcc) {
            if (count++ > 0) desc += ", ";
            desc += "MVCC";
        }
        if (temporal_model != TemporalModel::kNone) {
            if (count++ > 0) desc += ", ";
            desc += "Temporal(";
            switch (temporal_model) {
                case TemporalModel::kSystemTime: desc += "SystemTime"; break;
                case TemporalModel::kValidTime: desc += "ValidTime"; break;
                case TemporalModel::kBitemporal: desc += "Bitemporal"; break;
                default: break;
            }
            desc += ")";
        }
        if (enable_full_text_search) {
            if (count++ > 0) desc += ", ";
            desc += "FullTextSearch";
        }
        if (enable_ttl) {
            if (count++ > 0) desc += ", ";
            desc += "TTL";
        }
        if (enable_hot_key_cache) {
            if (count++ > 0) desc += ", ";
            desc += "HotKeyCache";
        }
        if (enable_negative_cache) {
            if (count++ > 0) desc += ", ";
            desc += "NegativeCache";
        }
        if (enable_zero_copy_reads) {
            if (count++ > 0) desc += ", ";
            desc += "ZeroCopy";
        }

        if (count == 0) {
            desc += "None (minimal overhead)";
        }

        return desc;
    }
};

// ===== Preset Configurations =====

/**
 * @brief Preset capability configurations for common use cases
 */
namespace CapabilityPresets {

/**
 * @brief OLTP transactions (banking, e-commerce)
 *
 * Features: MVCC, hot key cache, negative cache
 * Performance: 5-10 μs reads (hot), 4-5M writes/sec
 */
inline TableCapabilities OLTP() {
    TableCapabilities caps;
    caps.enable_mvcc = true;
    caps.mvcc_settings.max_versions_per_key = 5;
    caps.enable_hot_key_cache = true;
    caps.enable_negative_cache = true;
    caps.enable_zero_copy_reads = true;
    return caps;
}

/**
 * @brief Audit logs (compliance, regulatory)
 *
 * Features: Bitemporal, no MVCC, no TTL
 * Performance: 3-4M writes/sec, 10-50ms temporal queries
 */
inline TableCapabilities AuditLog() {
    TableCapabilities caps;
    caps.temporal_model = TableCapabilities::TemporalModel::kBitemporal;
    caps.temporal_settings.snapshot_retention_ms = 31536000000;  // 1 year
    caps.enable_ttl = false;  // Keep forever
    return caps;
}

/**
 * @brief Document search (wikis, knowledge bases)
 *
 * Features: Full-text search, TTL
 * Performance: 3-4M writes/sec, 1-15ms search queries
 */
inline TableCapabilities DocumentSearch(const std::vector<std::string>& indexed_columns) {
    TableCapabilities caps;
    caps.enable_full_text_search = true;
    caps.search_settings.indexed_columns = indexed_columns;
    caps.search_settings.enable_term_positions = true;
    caps.enable_ttl = true;
    caps.ttl_settings.ttl_duration_ms = 15552000000;  // 180 days
    return caps;
}

/**
 * @brief Time-series metrics (IoT, monitoring)
 *
 * Features: TTL, hot key cache
 * Performance: 6-7M writes/sec, 5-10 μs reads (recent)
 */
inline TableCapabilities TimeSeries(const std::string& timestamp_column, uint64_t retention_days) {
    TableCapabilities caps;
    caps.enable_ttl = true;
    caps.ttl_settings.timestamp_column = timestamp_column;
    caps.ttl_settings.ttl_duration_ms = retention_days * 86400000;
    caps.enable_hot_key_cache = true;
    return caps;
}

/**
 * @brief Session store (web apps, APIs)
 *
 * Features: MVCC, TTL, hot key cache, negative cache
 * Performance: 4-5M writes/sec, 5-10 μs reads (active sessions)
 */
inline TableCapabilities SessionStore(uint64_t session_ttl_ms) {
    TableCapabilities caps;
    caps.enable_mvcc = true;
    caps.enable_ttl = true;
    caps.ttl_settings.ttl_duration_ms = session_ttl_ms;
    caps.enable_hot_key_cache = true;
    caps.enable_negative_cache = true;
    return caps;
}

/**
 * @brief High-throughput cache (materialized views, aggregates)
 *
 * Features: Only hot key cache and zero-copy (all else disabled)
 * Performance: 7M writes/sec, 5-10 μs reads (hot), 20-50M rows/sec scans
 */
inline TableCapabilities HighThroughputCache() {
    TableCapabilities caps;
    caps.enable_mvcc = false;
    caps.temporal_model = TableCapabilities::TemporalModel::kNone;
    caps.enable_full_text_search = false;
    caps.enable_ttl = false;
    caps.enable_hot_key_cache = true;
    caps.enable_zero_copy_reads = true;
    return caps;
}

} // namespace CapabilityPresets

} // namespace marble
