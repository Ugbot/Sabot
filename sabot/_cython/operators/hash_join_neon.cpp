// ARM NEON SIMD implementation for streaming hash join
// Compiled only on ARM64 platforms with -march=armv8-a

#if defined(ARROW_HAVE_NEON)

#include <arm_neon.h>
#include <cstdint>
#include <cstring>

namespace sabot {
namespace hash_join {

// NEON SIMD constants
constexpr int NEON_BATCH_SIZE = 4;  // Process 4x uint32_t per iteration

/**
 * Compute 32-bit hash values using ARM NEON (4 values in parallel).
 *
 * Uses MurmurHash3-like mixing for good distribution.
 *
 * @param keys Input key values (int64_t[num_keys])
 * @param num_keys Number of keys (must be >= 0)
 * @param hashes Output hash values (uint32_t[num_keys])
 * @return Number of keys processed
 */
int hash_int64_neon(const int64_t* keys, int num_keys, uint32_t* hashes) {
    int i = 0;

    // NEON constants for MurmurHash3 mixing
    const uint32x4_t c1 = vdupq_n_u32(0xcc9e2d51);
    const uint32x4_t c2 = vdupq_n_u32(0x1b873593);
    const uint32x4_t seed = vdupq_n_u32(0x9747b28c);

    // Process 4 keys at a time with NEON
    for (; i + NEON_BATCH_SIZE <= num_keys; i += NEON_BATCH_SIZE) {
        // Load 4 int64_t keys (need two loads)
        int64x2_t k0_k1 = vld1q_s64(keys + i);
        int64x2_t k2_k3 = vld1q_s64(keys + i + 2);

        // Extract low 32 bits from each int64_t (simple hash)
        uint32x2_t low0 = vmovn_u64(vreinterpretq_u64_s64(k0_k1));
        uint32x2_t low1 = vmovn_u64(vreinterpretq_u64_s64(k2_k3));
        uint32x4_t k = vcombine_u32(low0, low1);

        // MurmurHash3 mixing (simplified for NEON)
        // h = k * c1
        k = vmulq_u32(k, c1);

        // h = rotl(h, 15)
        uint32x4_t k_rot = vshlq_n_u32(k, 15);
        k = vorrq_u32(k_rot, vshrq_n_u32(k, 17));

        // h = h * c2
        k = vmulq_u32(k, c2);

        // h = h ^ seed
        k = veorq_u32(k, seed);

        // Final avalanche mixing
        k = veorq_u32(k, vshrq_n_u32(k, 16));
        k = vmulq_u32(k, vdupq_n_u32(0x85ebca6b));
        k = veorq_u32(k, vshrq_n_u32(k, 13));
        k = vmulq_u32(k, vdupq_n_u32(0xc2b2ae35));
        k = veorq_u32(k, vshrq_n_u32(k, 16));

        // Store 4 hash values
        vst1q_u32(hashes + i, k);
    }

    // Scalar fallback for remaining keys
    for (; i < num_keys; ++i) {
        uint32_t k = static_cast<uint32_t>(keys[i]);
        k *= 0xcc9e2d51;
        k = (k << 15) | (k >> 17);
        k *= 0x1b873593;
        k ^= 0x9747b28c;
        k ^= k >> 16;
        k *= 0x85ebca6b;
        k ^= k >> 13;
        k *= 0xc2b2ae35;
        k ^= k >> 16;
        hashes[i] = k;
    }

    return num_keys;
}


/**
 * Probe hash table using NEON SIMD.
 *
 * Checks Bloom filter first (NEON-accelerated), then probes hash table.
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
                             uint8_t* match_bitvector) {
    int num_matches = 0;
    int i = 0;

    const uint32_t bloom_mask = (bloom_size * 8) - 1;  // Bloom filter bit mask

    // Process 4 hashes at a time with NEON
    for (; i + NEON_BATCH_SIZE <= num_keys; i += NEON_BATCH_SIZE) {
        // Load 4 hash values
        uint32x4_t hash_vec = vld1q_u32(hashes + i);

        // Compute 2 Bloom filter indices per hash (h1 = hash, h2 = hash >> 16)
        uint32x4_t h1 = vandq_u32(hash_vec, vdupq_n_u32(bloom_mask));
        uint32x4_t h2 = vandq_u32(vshrq_n_u32(hash_vec, 16), vdupq_n_u32(bloom_mask));

        // Check Bloom filter bits (scalar - NEON gather is complex)
        uint32_t h1_vals[4], h2_vals[4];
        vst1q_u32(h1_vals, h1);
        vst1q_u32(h2_vals, h2);

        for (int j = 0; j < 4; ++j) {
            uint32_t bit1 = h1_vals[j];
            uint32_t bit2 = h2_vals[j];

            uint8_t byte1 = bloom_filter[bit1 / 8];
            uint8_t byte2 = bloom_filter[bit2 / 8];

            uint8_t mask1 = 1 << (bit1 % 8);
            uint8_t mask2 = 1 << (bit2 % 8);

            // Both bits must be set for match
            uint8_t match = ((byte1 & mask1) && (byte2 & mask2)) ? 1 : 0;
            match_bitvector[i + j] = match;
            num_matches += match;
        }
    }

    // Scalar fallback for remaining keys
    for (; i < num_keys; ++i) {
        uint32_t hash = hashes[i];
        uint32_t bit1 = hash & bloom_mask;
        uint32_t bit2 = (hash >> 16) & bloom_mask;

        uint8_t byte1 = bloom_filter[bit1 / 8];
        uint8_t byte2 = bloom_filter[bit2 / 8];

        uint8_t mask1 = 1 << (bit1 % 8);
        uint8_t mask2 = 1 << (bit2 % 8);

        uint8_t match = ((byte1 & mask1) && (byte2 & mask2)) ? 1 : 0;
        match_bitvector[i] = match;
        num_matches += match;
    }

    return num_matches;
}


/**
 * Build Bloom filter from hash values using NEON SIMD.
 *
 * Sets 2 bits per hash value for ~1% false positive rate.
 *
 * @param hashes Hash values (uint32_t[num_keys])
 * @param num_keys Number of keys
 * @param bloom_filter Output Bloom filter bit array
 * @param bloom_size Bloom filter size in bytes
 */
void build_bloom_filter_neon(const uint32_t* hashes, int num_keys,
                              uint8_t* bloom_filter, int bloom_size) {
    const uint32_t bloom_mask = (bloom_size * 8) - 1;

    // Zero out bloom filter
    memset(bloom_filter, 0, bloom_size);

    // Process all hashes (scalar for now - NEON scatter is complex)
    for (int i = 0; i < num_keys; ++i) {
        uint32_t hash = hashes[i];

        // Set 2 bits per hash
        uint32_t bit1 = hash & bloom_mask;
        uint32_t bit2 = (hash >> 16) & bloom_mask;

        bloom_filter[bit1 / 8] |= (1 << (bit1 % 8));
        bloom_filter[bit2 / 8] |= (1 << (bit2 % 8));
    }
}


/**
 * Compact match indices using NEON SIMD.
 *
 * Takes sparse match_bitvector and compacts matching indices into dense array.
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
                                uint32_t* out_left, uint32_t* out_right) {
    int out_idx = 0;

    // Scalar compaction (NEON compress is complex)
    for (int i = 0; i < num_keys; ++i) {
        if (match_bitvector[i]) {
            out_left[out_idx] = left_indices[i];
            out_right[out_idx] = right_indices[i];
            ++out_idx;
        }
    }

    return out_idx;
}

}  // namespace hash_join
}  // namespace sabot

#endif  // ARROW_HAVE_NEON
