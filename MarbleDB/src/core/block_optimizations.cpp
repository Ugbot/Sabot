#include "marble/block_optimizations.h"
#include <algorithm>

namespace marble {

//==============================================================================
// BlockBloomFilterManager Implementation
//==============================================================================

BlockBloomFilterManager::BlockBloomFilterManager(size_t block_size, size_t bits_per_key)
    : block_size_(block_size), bits_per_key_(bits_per_key) {}

void BlockBloomFilterManager::AddKeyToBlock(size_t block_id, const Key& key) {
    // Get or create bloom filter for this block
    auto it = block_blooms_.find(block_id);
    if (it == block_blooms_.end()) {
        // Create new bloom filter for this block
        // Estimate expected items based on bits per key
        size_t expected_items = bits_per_key_ > 0 ? (block_size_ * 8) / bits_per_key_ : 1000;
        // auto bloom = std::make_shared<BloomFilter>(expected_items, 0.01); // TODO: Re-enable
        // block_blooms_[block_id] = bloom; // TODO: Re-enable
        it = block_blooms_.find(block_id);
    }

    // Add key to block's bloom filter
    // it->second->Add(key); // TODO: Re-enable
}

bool BlockBloomFilterManager::BlockMayContainKey(size_t block_id, const Key& key) const {
    auto it = block_blooms_.find(block_id);
    if (it == block_blooms_.end()) {
        return true;  // Conservative: assume block might contain key
    }

    // return it->second->MayContain(key); // TODO: Re-enable
    return true; // Conservative: assume block might contain key
}

std::shared_ptr<BloomFilter> BlockBloomFilterManager::GetBlockBloomFilter(size_t block_id) const {
    auto it = block_blooms_.find(block_id);
    if (it != block_blooms_.end()) {
        // return it->second; // TODO: Re-enable
        return nullptr; // TODO: Re-enable
    }
    return nullptr;
}

void BlockBloomFilterManager::Finalize() {
    // Bloom filters are finalized when keys are added
    // No additional work needed
}

//==============================================================================
// OptimizedSSTableReader Implementation
//==============================================================================

OptimizedSSTableReader::OptimizedSSTableReader(
    std::shared_ptr<NegativeCache> negative_cache)
    : negative_cache_(negative_cache) {}

Status OptimizedSSTableReader::GetOptimized(
    const std::vector<std::pair<uint64_t, std::string>>& sorted_entries,
    const std::vector<std::shared_ptr<BloomFilter>>& block_blooms,
    uint64_t key,
    std::string* value,
    size_t block_size) {

    stats_.total_lookups++;

    // Check negative cache first (if enabled)
    if (negative_cache_) {
        Int64Key key_obj(key);
        if (negative_cache_->DefinitelyNotExists(key_obj)) {
            stats_.negative_cache_hits++;
            return Status::NotFound("Key in negative cache");
        }
    }

    if (sorted_entries.empty()) {
        return Status::NotFound("Empty SSTable");
    }

    // Calculate which blocks might contain the key
    size_t num_blocks = (sorted_entries.size() + block_size - 1) / block_size;

    for (size_t block_id = 0; block_id < num_blocks; ++block_id) {
        // Check block bloom filter first
        // TODO: Re-enable bloom filter
        /*
        if (block_id < block_blooms.size()) {
            Int64Key key_obj(key);
            if (!block_blooms[block_id]->MayContain(key_obj)) {
                // Block definitely doesn't contain key - skip it
                stats_.blocks_skipped++;
                continue;
            }
        }
        */

        // Block might contain key - do binary search within block
        stats_.blocks_scanned++;

        size_t block_start = block_id * block_size;
        size_t block_end = std::min(block_start + block_size, sorted_entries.size());

        auto status = BinarySearchInBlock(sorted_entries, block_start, block_end, key, value);
        if (status.ok()) {
            return Status::OK();
        }
    }

    // Key not found - add to negative cache
    if (negative_cache_) {
        Int64Key key_obj(key);
        negative_cache_->RecordMiss(key_obj);
    }

    return Status::NotFound("Key not found in any block");
}

Status OptimizedSSTableReader::BinarySearchInBlock(
    const std::vector<std::pair<uint64_t, std::string>>& entries,
    size_t block_start,
    size_t block_end,
    uint64_t key,
    std::string* value) const {

    // Binary search within the block
    auto it = std::lower_bound(
        entries.begin() + block_start,
        entries.begin() + block_end,
        key,
        [](const std::pair<uint64_t, std::string>& entry, uint64_t k) {
            return entry.first < k;
        });

    if (it != entries.begin() + block_end && it->first == key) {
        *value = it->second;
        return Status::OK();
    }

    return Status::NotFound("Key not found in block");
}

//==============================================================================
// BlockBloomFilterBuilder Implementation
//==============================================================================

BlockBloomFilterBuilder::BlockBloomFilterBuilder(size_t block_size, size_t bits_per_key)
    : block_size_(block_size), bits_per_key_(bits_per_key) {}

std::vector<std::shared_ptr<BloomFilter>> BlockBloomFilterBuilder::BuildFromEntries(
    const std::vector<std::pair<uint64_t, std::string>>& sorted_entries) {

    std::vector<std::shared_ptr<BloomFilter>> block_blooms;

    if (sorted_entries.empty()) {
        return block_blooms;
    }

    // Calculate number of blocks
    size_t num_blocks = (sorted_entries.size() + block_size_ - 1) / block_size_;
    block_blooms.reserve(num_blocks);

    // Create bloom filter for each block
    for (size_t block_id = 0; block_id < num_blocks; ++block_id) {
        size_t block_start = block_id * block_size_;
        size_t block_end = std::min(block_start + block_size_, sorted_entries.size());
        size_t block_key_count = block_end - block_start;

        // Create bloom filter for this block
        // auto bloom = std::make_shared<BloomFilter>(bits_per_key_, block_key_count); // TODO: Re-enable

        // Add all keys in this block to the bloom filter
        // TODO: Re-enable bloom filter
        /*
        for (size_t i = block_start; i < block_end; ++i) {
            Int64Key key_obj(sorted_entries[i].first);
            bloom->Add(key_obj);
        }

        block_blooms.push_back(bloom);
        */
    }

    return block_blooms;
}

} // namespace marble
