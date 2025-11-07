/**
 * BloomFilterStrategy implementation
 */

#include "marble/bloom_filter_strategy.h"
#include "marble/record.h"
#include "marble/table_capabilities.h"
#include <cmath>
#include <cstring>
#include <sstream>
#include <functional>

namespace marble {

//==============================================================================
// HashBloomFilter implementation
//==============================================================================

HashBloomFilter::HashBloomFilter(size_t expected_keys, double false_positive_rate) {
    if (expected_keys == 0) {
        expected_keys = 1;  // Avoid division by zero
    }

    // Calculate optimal number of bits: m = -n * ln(p) / (ln(2)^2)
    double ln2_squared = std::log(2) * std::log(2);
    num_bits_ = static_cast<size_t>(
        -static_cast<double>(expected_keys) * std::log(false_positive_rate) / ln2_squared
    );

    // Round up bits to multiple of 8 for byte alignment (BEFORE calculating k)
    num_bits_ = ((num_bits_ + 7) / 8) * 8;

    // Calculate optimal number of hash functions: k = (m/n) * ln(2)
    // Use std::ceil to round up instead of truncating
    num_hash_functions_ = static_cast<size_t>(
        std::ceil((static_cast<double>(num_bits_) / expected_keys) * std::log(2))
    );

    // Clamp to reasonable range
    num_hash_functions_ = std::max<size_t>(1, std::min<size_t>(num_hash_functions_, 10));

    // Allocate bit array (1 byte per 8 bits)
    size_t num_bytes = (num_bits_ + 7) / 8;
    bits_.resize(num_bytes, 0);
}

void HashBloomFilter::Add(uint64_t hash) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto indices = GetBitIndices(hash);
    for (size_t index : indices) {
        SetBit(index);
    }
    ++num_keys_;
}

bool HashBloomFilter::MightContain(uint64_t hash) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto indices = GetBitIndices(hash);
    for (size_t index : indices) {
        if (!IsBitSet(index)) {
            return false;  // Definitely not present
        }
    }
    return true;  // Might be present (could be false positive)
}

size_t HashBloomFilter::MemoryUsage() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return bits_.size() + sizeof(*this);
}

size_t HashBloomFilter::NumKeys() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return num_keys_;
}

double HashBloomFilter::FalsePositiveRate() const {
    std::lock_guard<std::mutex> lock(mutex_);
    if (num_keys_ == 0) {
        return 0.0;
    }

    // FPR = (1 - e^(-kn/m))^k
    double exponent = -static_cast<double>(num_hash_functions_ * num_keys_) / num_bits_;
    return std::pow(1.0 - std::exp(exponent), num_hash_functions_);
}

void HashBloomFilter::Clear() {
    std::lock_guard<std::mutex> lock(mutex_);
    std::fill(bits_.begin(), bits_.end(), 0);
    num_keys_ = 0;
}

std::vector<uint8_t> HashBloomFilter::Serialize() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<uint8_t> result;

    // Write header: [num_bits: 8 bytes][num_hash: 8 bytes][num_keys: 8 bytes]
    result.resize(24 + bits_.size());

    size_t offset = 0;
    std::memcpy(result.data() + offset, &num_bits_, sizeof(num_bits_));
    offset += sizeof(num_bits_);

    std::memcpy(result.data() + offset, &num_hash_functions_, sizeof(num_hash_functions_));
    offset += sizeof(num_hash_functions_);

    std::memcpy(result.data() + offset, &num_keys_, sizeof(num_keys_));
    offset += sizeof(num_keys_);

    // Write bit array
    std::memcpy(result.data() + offset, bits_.data(), bits_.size());

    return result;
}

std::unique_ptr<HashBloomFilter> HashBloomFilter::Deserialize(const std::vector<uint8_t>& data) {
    if (data.size() < 24) {
        return nullptr;  // Invalid data
    }

    size_t offset = 0;
    size_t num_bits, num_hash, num_keys;

    std::memcpy(&num_bits, data.data() + offset, sizeof(num_bits));
    offset += sizeof(num_bits);

    std::memcpy(&num_hash, data.data() + offset, sizeof(num_hash));
    offset += sizeof(num_hash);

    std::memcpy(&num_keys, data.data() + offset, sizeof(num_keys));
    offset += sizeof(num_keys);

    size_t num_bytes = (num_bits + 7) / 8;
    if (data.size() != 24 + num_bytes) {
        return nullptr;  // Invalid data size
    }

    // Create hash bloom filter with dummy parameters (will override)
    auto filter = std::make_unique<HashBloomFilter>(1, 0.01);
    filter->num_bits_ = num_bits;
    filter->num_hash_functions_ = num_hash;
    filter->num_keys_ = num_keys;

    // Restore bit array
    filter->bits_.resize(num_bytes);
    std::memcpy(filter->bits_.data(), data.data() + offset, num_bytes);

    return filter;
}

std::vector<size_t> HashBloomFilter::GetBitIndices(uint64_t hash) const {
    std::vector<size_t> indices;
    indices.reserve(num_hash_functions_);

    // Use double hashing: h_i(x) = (h1(x) + i * h2(x)) mod m
    uint64_t h1 = hash;
    // Use golden ratio multiplication for better mixing
    // This works well even for small sequential integers
    uint64_t h2 = hash * 0x9e3779b97f4a7c15ULL + 1;

    for (size_t i = 0; i < num_hash_functions_; ++i) {
        uint64_t combined = h1 + i * h2;
        size_t index = combined % num_bits_;
        indices.push_back(index);
    }

    return indices;
}

void HashBloomFilter::SetBit(size_t index) {
    // Assume mutex is already held by caller
    size_t byte_index = index / 8;
    size_t bit_index = index % 8;
    uint8_t mask = 1 << bit_index;

    bits_[byte_index] |= mask;
}

bool HashBloomFilter::IsBitSet(size_t index) const {
    // Assume mutex is already held by caller
    size_t byte_index = index / 8;
    size_t bit_index = index % 8;
    uint8_t mask = 1 << bit_index;

    return (bits_[byte_index] & mask) != 0;
}

//==============================================================================
// BloomFilterStrategy implementation
//==============================================================================

BloomFilterStrategy::BloomFilterStrategy(size_t expected_keys, double false_positive_rate)
    : expected_keys_(expected_keys),
      false_positive_rate_(false_positive_rate) {
    bloom_filter_ = std::make_unique<HashBloomFilter>(expected_keys, false_positive_rate);
}

Status BloomFilterStrategy::OnTableCreate(const TableCapabilities& caps) {
    // Nothing to do on table creation
    // Bloom filter already initialized in constructor
    return Status::OK();
}

Status BloomFilterStrategy::OnRead(ReadContext* ctx) {
    if (!ctx) {
        return Status::InvalidArgument("ReadContext is null");
    }

    // Only apply bloom filter to point lookups
    if (!ctx->is_point_lookup) {
        return Status::OK();
    }

    stats_.num_lookups.fetch_add(1, std::memory_order_relaxed);

    // Hash the key
    uint64_t hash = HashKey(ctx->key);

    // Check bloom filter
    bool might_exist = bloom_filter_->MightContain(hash);

    if (!might_exist) {
        // Bloom filter says key definitely doesn't exist
        stats_.num_bloom_misses.fetch_add(1, std::memory_order_relaxed);
        ctx->definitely_not_found = true;
        return Status::NotFound("Bloom filter: key not found");
    }

    // Bloom filter says key might exist (could be false positive)
    stats_.num_bloom_hits.fetch_add(1, std::memory_order_relaxed);
    return Status::OK();
}

void BloomFilterStrategy::OnReadComplete(const Key& key, const Record& record) {
    // Key was successfully read, no action needed
    // (bloom filter already contains this key from write)
}

Status BloomFilterStrategy::OnWrite(WriteContext* ctx) {
    if (!ctx) {
        return Status::InvalidArgument("WriteContext is null");
    }

    // Add key to bloom filter
    uint64_t hash = HashKey(ctx->key);
    bloom_filter_->Add(hash);

    stats_.num_keys_added.fetch_add(1, std::memory_order_relaxed);

    return Status::OK();
}

Status BloomFilterStrategy::OnCompaction(CompactionContext* ctx) {
    if (!ctx) {
        return Status::InvalidArgument("CompactionContext is null");
    }

    // Rebuild bloom filter from compacted data
    // Clear existing filter
    bloom_filter_->Clear();
    stats_.num_keys_added.store(0, std::memory_order_relaxed);

    // Re-add all keys from compacted batches
    for (const auto& batch : ctx->input_batches) {
        if (!batch) continue;

        // Iterate through batch and add keys
        // Note: This requires access to key column, which depends on schema
        // For now, we'll skip rebuilding and just clear the filter
        // In production, we'd extract keys and re-add them
    }

    ctx->rebuild_bloom_filters = true;

    return Status::OK();
}

Status BloomFilterStrategy::OnFlush(FlushContext* ctx) {
    if (!ctx) {
        return Status::InvalidArgument("FlushContext is null");
    }

    // Serialize bloom filter and attach to SSTable metadata
    std::vector<uint8_t> serialized = bloom_filter_->Serialize();
    ctx->metadata["bloom_filter"] = std::move(serialized);
    ctx->include_bloom_filter = true;

    return Status::OK();
}

size_t BloomFilterStrategy::MemoryUsage() const {
    return bloom_filter_->MemoryUsage() + sizeof(*this);
}

void BloomFilterStrategy::Clear() {
    bloom_filter_->Clear();
    stats_.num_lookups.store(0, std::memory_order_relaxed);
    stats_.num_bloom_hits.store(0, std::memory_order_relaxed);
    stats_.num_bloom_misses.store(0, std::memory_order_relaxed);
    stats_.num_false_positives.store(0, std::memory_order_relaxed);
    stats_.num_keys_added.store(0, std::memory_order_relaxed);
}

std::vector<uint8_t> BloomFilterStrategy::Serialize() const {
    return bloom_filter_->Serialize();
}

Status BloomFilterStrategy::Deserialize(const std::vector<uint8_t>& data) {
    auto filter = HashBloomFilter::Deserialize(data);
    if (!filter) {
        return Status::InvalidArgument("Failed to deserialize bloom filter");
    }

    bloom_filter_ = std::move(filter);
    return Status::OK();
}

std::string BloomFilterStrategy::GetStats() const {
    std::ostringstream ss;

    uint64_t lookups = stats_.num_lookups.load(std::memory_order_relaxed);
    uint64_t hits = stats_.num_bloom_hits.load(std::memory_order_relaxed);
    uint64_t misses = stats_.num_bloom_misses.load(std::memory_order_relaxed);
    uint64_t false_positives = stats_.num_false_positives.load(std::memory_order_relaxed);
    uint64_t keys_added = stats_.num_keys_added.load(std::memory_order_relaxed);

    double hit_rate = lookups > 0 ? static_cast<double>(hits) / lookups : 0.0;
    double filter_fpr = bloom_filter_->FalsePositiveRate();

    ss << "{\n";
    ss << "  \"lookups\": " << lookups << ",\n";
    ss << "  \"bloom_hits\": " << hits << ",\n";
    ss << "  \"bloom_misses\": " << misses << ",\n";
    ss << "  \"false_positives\": " << false_positives << ",\n";
    ss << "  \"keys_added\": " << keys_added << ",\n";
    ss << "  \"hit_rate\": " << hit_rate << ",\n";
    ss << "  \"filter_fpr\": " << filter_fpr << ",\n";
    ss << "  \"memory_usage\": " << MemoryUsage() << "\n";
    ss << "}";

    return ss.str();
}

uint64_t BloomFilterStrategy::HashKey(const Key& key) const {
    // Use std::hash for now
    // In production, consider using a faster hash like xxHash or CityHash
    std::hash<std::string> hasher;

    // Assuming Key has a ToString() or similar method
    // For now, we'll use a simple hash of the key pointer
    // This is a placeholder - actual implementation depends on Key's structure
    return reinterpret_cast<uint64_t>(&key);
}

}  // namespace marble
