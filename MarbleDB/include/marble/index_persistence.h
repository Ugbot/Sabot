/**
 * Index Persistence Manager
 *
 * Manages persistent storage of optimization indexes (bloom filters, skipping indexes, etc.)
 * separately from the main LSM tree data.
 *
 * Directory structure:
 *   {db_path}/indexes/
 *     cf_{id}/           # Per-column-family indexes
 *       bloom/           # Bloom filter files
 *         batch_{id}.bloom
 *       skip/            # Skipping indexes (future)
 *         batch_{id}.skip.arrow
 *     manifest.json      # Index registry
 */

#pragma once

#include <marble/status.h>
#include <string>
#include <vector>
#include <unordered_map>
#include <memory>
#include <mutex>

namespace marble {

/**
 * Index manifest entry - tracks which indexes exist for each batch
 */
struct IndexManifestEntry {
    uint32_t cf_id;
    uint64_t batch_id;
    std::string bloom_filter_path;  // Relative path to .bloom file
    std::string skipping_index_path; // Relative path to .skip.arrow file (future)

    IndexManifestEntry() : cf_id(0), batch_id(0) {}
    IndexManifestEntry(uint32_t cf, uint64_t batch)
        : cf_id(cf), batch_id(batch) {}
};

/**
 * Column family index metadata
 */
struct ColumnFamilyIndexMeta {
    uint32_t cf_id;
    std::string cf_name;
    std::vector<std::string> bloom_filter_files;
    std::vector<std::string> skipping_index_files;

    ColumnFamilyIndexMeta() : cf_id(0) {}
    ColumnFamilyIndexMeta(uint32_t id, const std::string& name)
        : cf_id(id), cf_name(name) {}
};

/**
 * Index manifest - tracks all persisted indexes
 */
struct IndexManifest {
    uint32_t version = 1;
    std::unordered_map<uint32_t, ColumnFamilyIndexMeta> column_families;
    std::unordered_map<std::string, IndexManifestEntry> batch_index_map;  // "cf_id:batch_id" -> entry

    // Serialize to JSON
    std::string ToJSON() const;

    // Deserialize from JSON
    static Status FromJSON(const std::string& json, IndexManifest* manifest);
};

/**
 * IndexPersistenceManager - Handles saving and loading optimization indexes
 */
class IndexPersistenceManager {
public:
    /**
     * Create index persistence manager
     *
     * @param base_path Database base path (indexes will be in {base_path}/indexes/)
     */
    explicit IndexPersistenceManager(const std::string& base_path);

    ~IndexPersistenceManager() = default;

    /**
     * Initialize index directories and load existing manifest
     *
     * @return Status OK on success
     */
    Status Initialize();

    /**
     * Save a bloom filter to disk
     *
     * @param cf_id Column family ID
     * @param batch_id Batch ID
     * @param serialized_data Serialized bloom filter data
     * @return Status OK on success
     */
    Status SaveBloomFilter(uint32_t cf_id, uint64_t batch_id,
                           const std::vector<uint8_t>& serialized_data);

    /**
     * Load a bloom filter from disk
     *
     * @param cf_id Column family ID
     * @param batch_id Batch ID
     * @param data Output buffer for serialized bloom filter
     * @return Status OK on success, NotFound if file doesn't exist
     */
    Status LoadBloomFilter(uint32_t cf_id, uint64_t batch_id,
                           std::vector<uint8_t>* data);

    /**
     * Load all bloom filters for a column family
     *
     * @param cf_id Column family ID
     * @param filters Output map of batch_id -> serialized bloom filter
     * @return Status OK on success
     */
    Status LoadAllBloomFilters(uint32_t cf_id,
                                std::unordered_map<uint64_t, std::vector<uint8_t>>* filters);

    /**
     * Delete a bloom filter file
     *
     * @param cf_id Column family ID
     * @param batch_id Batch ID
     * @return Status OK on success
     */
    Status DeleteBloomFilter(uint32_t cf_id, uint64_t batch_id);

    /**
     * Save a skipping index to disk
     *
     * @param cf_id Column family ID
     * @param batch_id Batch ID
     * @param serialized_data Serialized skipping index data
     * @return Status OK on success
     */
    Status SaveSkippingIndex(uint32_t cf_id, uint64_t batch_id,
                             const std::vector<uint8_t>& serialized_data);

    /**
     * Load a skipping index from disk
     *
     * @param cf_id Column family ID
     * @param batch_id Batch ID
     * @param data Output buffer for serialized skipping index
     * @return Status OK on success, NotFound if file doesn't exist
     */
    Status LoadSkippingIndex(uint32_t cf_id, uint64_t batch_id,
                             std::vector<uint8_t>* data);

    /**
     * Load all skipping indexes for a column family
     *
     * @param cf_id Column family ID
     * @param indexes Output map of batch_id -> serialized skipping index
     * @return Status OK on success
     */
    Status LoadAllSkippingIndexes(uint32_t cf_id,
                                   std::unordered_map<uint64_t, std::vector<uint8_t>>* indexes);

    /**
     * Delete a skipping index file
     *
     * @param cf_id Column family ID
     * @param batch_id Batch ID
     * @return Status OK on success
     */
    Status DeleteSkippingIndex(uint32_t cf_id, uint64_t batch_id);

    /**
     * Register a column family in the manifest
     *
     * @param cf_id Column family ID
     * @param cf_name Column family name
     * @return Status OK on success
     */
    Status RegisterColumnFamily(uint32_t cf_id, const std::string& cf_name);

    /**
     * Save the current manifest to disk
     *
     * @return Status OK on success
     */
    Status SaveManifest();

    /**
     * Load manifest from disk
     *
     * @return Status OK on success
     */
    Status LoadManifest();

    /**
     * Get index directory path for a column family
     *
     * @param cf_id Column family ID
     * @return Full path to column family index directory
     */
    std::string GetColumnFamilyIndexPath(uint32_t cf_id) const;

    /**
     * Get bloom filter file path
     *
     * @param cf_id Column family ID
     * @param batch_id Batch ID
     * @return Full path to bloom filter file
     */
    std::string GetBloomFilterPath(uint32_t cf_id, uint64_t batch_id) const;

    /**
     * Get skipping index file path
     *
     * @param cf_id Column family ID
     * @param batch_id Batch ID
     * @return Full path to skipping index file
     */
    std::string GetSkippingIndexPath(uint32_t cf_id, uint64_t batch_id) const;

    /**
     * Get statistics about persisted indexes
     *
     * @return JSON string with statistics
     */
    std::string GetStats() const;

private:
    std::string base_path_;
    std::string indexes_path_;
    std::string manifest_path_;
    IndexManifest manifest_;
    mutable std::mutex mutex_;

    // Create directory if it doesn't exist
    Status CreateDirectory(const std::string& path);

    // Create all necessary index directories for a column family
    Status CreateColumnFamilyDirectories(uint32_t cf_id);
};

}  // namespace marble
