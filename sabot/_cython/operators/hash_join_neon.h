// ARM NEON SIMD functions for streaming hash join
#pragma once

#include <cstdint>

namespace sabot {
namespace hash_join {

/**
 * Compute 32-bit hash values using ARM NEON (4 values in parallel).
 *
 * @param keys Input key values (int64_t[num_keys])
 * @param num_keys Number of keys (must be >= 0)
 * @param hashes Output hash values (uint32_t[num_keys])
 * @return Number of keys processed
 */
int hash_int64_neon(const int64_t* keys, int num_keys, uint32_t* hashes);

/**
 * Probe Bloom filter using ARM NEON SIMD (4 keys in parallel).
 *
 * @param hashes Hash values for probe keys (uint32_t[num_keys])
 * @param num_keys Number of probe keys
 * @param bloom_filter Bloom filter bit array
 * @param bloom_size Bloom filter size in bytes
 * @param match_bitvector Output: 1 if key may match, 0 if definitely no match (uint8_t[num_keys])
 * @return Number of keys that passed Bloom filter
 */
int probe_bloom_filter_neon(const uint32_t* hashes, int num_keys,
                             const uint8_t* bloom_filter, int bloom_size,
                             uint8_t* match_bitvector);

/**
 * Build Bloom filter from hash values using ARM NEON SIMD.
 *
 * @param hashes Hash values (uint32_t[num_keys])
 * @param num_keys Number of keys
 * @param bloom_filter Output Bloom filter bit array
 * @param bloom_size Bloom filter size in bytes
 */
void build_bloom_filter_neon(const uint32_t* hashes, int num_keys,
                              uint8_t* bloom_filter, int bloom_size);

/**
 * Compact match indices using ARM NEON SIMD.
 *
 * @param match_bitvector Bitvector indicating matches (uint8_t[num_keys])
 * @param num_keys Number of keys
 * @param left_indices Input left indices (uint32_t[num_keys])
 * @param right_indices Input right indices (uint32_t[num_keys])
 * @param out_left Output compacted left indices (uint32_t[num_matches])
 * @param out_right Output compacted right indices (uint32_t[num_matches])
 * @return Number of matches compacted
 */
int compact_match_indices_neon(const uint8_t* match_bitvector, int num_keys,
                                const uint32_t* left_indices, const uint32_t* right_indices,
                                uint32_t* out_left, uint32_t* out_right);

}  // namespace hash_join
}  // namespace sabot
