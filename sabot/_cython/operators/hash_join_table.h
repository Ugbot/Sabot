// Simple hash table for streaming hash join
// Optimized for int64 keys -> vector of row indices
#pragma once

#include <cstdint>
#include <vector>
#include <unordered_map>

namespace sabot {
namespace hash_join {

/**
 * Lightweight hash table for hash join operations.
 * Maps hash values to lists of row indices.
 *
 * Thread-safe for concurrent reads after build phase completes.
 * NOT thread-safe for concurrent writes (use single thread for build).
 */
class HashJoinTable {
 public:
  HashJoinTable() : num_entries_(0) {}

  /**
   * Reserve space for expected number of entries.
   * Call this before inserting to avoid rehashing.
   */
  void reserve(size_t expected_entries) {
    table_.reserve(expected_entries);
  }

  /**
   * Insert a hash value with its row index.
   * Not thread-safe.
   *
   * @param hash Hash value (32-bit)
   * @param row_index Row index in build table
   */
  void insert(uint32_t hash, uint32_t row_index) {
    table_[hash].push_back(row_index);
    num_entries_++;
  }

  /**
   * Batch insert: add multiple hash-index pairs.
   * More efficient than multiple individual inserts.
   *
   * @param hashes Array of hash values
   * @param row_indices Array of row indices (parallel to hashes)
   * @param count Number of entries to insert
   */
  void insert_batch(const uint32_t* hashes, const uint32_t* row_indices, int count) {
    for (int i = 0; i < count; ++i) {
      table_[hashes[i]].push_back(row_indices[i]);
    }
    num_entries_ += count;
  }

  /**
   * Lookup matching row indices for a hash value.
   * Thread-safe for reads.
   *
   * @param hash Hash value to lookup
   * @param out_indices Output buffer for matching row indices
   * @param max_matches Maximum number of matches to return
   * @return Number of matches found
   */
  int lookup(uint32_t hash, uint32_t* out_indices, int max_matches) const {
    auto it = table_.find(hash);
    if (it == table_.end()) {
      return 0;
    }

    const auto& indices = it->second;
    int num_matches = static_cast<int>(indices.size());
    if (num_matches > max_matches) {
      num_matches = max_matches;
    }

    for (int i = 0; i < num_matches; ++i) {
      out_indices[i] = indices[i];
    }

    return num_matches;
  }

  /**
   * Get all matching indices for a hash value (no limit).
   * Returns pointer to internal vector - valid until next insert/clear.
   *
   * @param hash Hash value to lookup
   * @param out_count Output: number of matches
   * @return Pointer to array of matching indices, or nullptr if no matches
   */
  const uint32_t* lookup_all(uint32_t hash, int* out_count) const {
    auto it = table_.find(hash);
    if (it == table_.end()) {
      *out_count = 0;
      return nullptr;
    }

    *out_count = static_cast<int>(it->second.size());
    return it->second.data();
  }

  /**
   * Check if a hash value exists in the table.
   *
   * @param hash Hash value to check
   * @return 1 if exists, 0 otherwise
   */
  int contains(uint32_t hash) const {
    return table_.find(hash) != table_.end() ? 1 : 0;
  }

  /**
   * Get number of unique hash values in table.
   */
  size_t num_buckets() const {
    return table_.size();
  }

  /**
   * Get total number of entries (including duplicates).
   */
  size_t num_entries() const {
    return num_entries_;
  }

  /**
   * Clear the hash table.
   */
  void clear() {
    table_.clear();
    num_entries_ = 0;
  }

  /**
   * Get memory usage in bytes (approximate).
   */
  size_t memory_usage() const {
    size_t total = sizeof(*this);
    total += table_.bucket_count() * sizeof(void*);  // Bucket pointers
    total += table_.size() * (sizeof(uint32_t) + sizeof(std::vector<uint32_t>));  // Map nodes

    // Add vector capacities
    for (const auto& entry : table_) {
      total += entry.second.capacity() * sizeof(uint32_t);
    }

    return total;
  }

 private:
  // Hash table: hash value -> list of row indices
  std::unordered_map<uint32_t, std::vector<uint32_t>> table_;

  // Total number of entries (including duplicates)
  size_t num_entries_;
};

}  // namespace hash_join
}  // namespace sabot
