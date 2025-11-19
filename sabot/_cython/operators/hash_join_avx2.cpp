// x86 AVX2 SIMD implementation for streaming hash join
// Compiled only on x86_64 platforms with -march=haswell -mavx2

#if defined(ARROW_HAVE_RUNTIME_AVX2)

#include <immintrin.h>
#include <cstdint>
#include <cstring>

namespace sabot {
namespace hash_join {

// AVX2 SIMD constants
constexpr int AVX2_BATCH_SIZE = 8;  // Process 8x uint32_t per iteration

/**
 * Compute 32-bit hash values using AVX2 (8 values in parallel).
 *
 * Uses MurmurHash3-like mixing for good distribution.
 *
 * @param keys Input key values (int64_t[num_keys])
 * @param num_keys Number of keys (must be >= 0)
 * @param hashes Output hash values (uint32_t[num_keys])
 * @return Number of keys processed
 */
int hash_int64_avx2(const int64_t* keys, int num_keys, uint32_t* hashes) {
    int i = 0;

    // AVX2 constants for MurmurHash3 mixing
    const __m256i c1 = _mm256_set1_epi32(0xcc9e2d51);
    const __m256i c2 = _mm256_set1_epi32(0x1b873593);
    const __m256i seed = _mm256_set1_epi32(0x9747b28c);

    // Process 8 keys at a time with AVX2
    for (; i + AVX2_BATCH_SIZE <= num_keys; i += AVX2_BATCH_SIZE) {
        // Load 8 int64_t keys (need two loads)
        __m256i k0_k3 = _mm256_loadu_si256((__m256i*)(keys + i));
        __m256i k4_k7 = _mm256_loadu_si256((__m256i*)(keys + i + 4));

        // Extract low 32 bits from each int64_t
        __m256i k_low0 = _mm256_shuffle_epi32(k0_k3, _MM_SHUFFLE(2, 0, 2, 0));
        __m256i k_low1 = _mm256_shuffle_epi32(k4_k7, _MM_SHUFFLE(2, 0, 2, 0));
        __m256i k = _mm256_permute2x128_si256(k_low0, k_low1, 0x20);

        // MurmurHash3 mixing
        // h = k * c1
        k = _mm256_mullo_epi32(k, c1);

        // h = rotl(h, 15)
        __m256i k_rot = _mm256_slli_epi32(k, 15);
        k = _mm256_or_si256(k_rot, _mm256_srli_epi32(k, 17));

        // h = h * c2
        k = _mm256_mullo_epi32(k, c2);

        // h = h ^ seed
        k = _mm256_xor_si256(k, seed);

        // Final avalanche mixing
        k = _mm256_xor_si256(k, _mm256_srli_epi32(k, 16));
        k = _mm256_mullo_epi32(k, _mm256_set1_epi32(0x85ebca6b));
        k = _mm256_xor_si256(k, _mm256_srli_epi32(k, 13));
        k = _mm256_mullo_epi32(k, _mm256_set1_epi32(0xc2b2ae35));
        k = _mm256_xor_si256(k, _mm256_srli_epi32(k, 16));

        // Store 8 hash values
        _mm256_storeu_si256((__m256i*)(hashes + i), k);
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
 * Probe Bloom filter using AVX2 SIMD (8 keys in parallel).
 *
 * @param hashes Hash values for probe keys (uint32_t[num_keys])
 * @param num_keys Number of probe keys
 * @param bloom_filter Bloom filter bit array
 * @param bloom_size Bloom filter size in bytes
 * @param match_bitvector Output: 1 if key may match, 0 if definitely no match (uint8_t[num_keys])
 * @return Number of keys that passed Bloom filter
 */
int probe_bloom_filter_avx2(const uint32_t* hashes, int num_keys,
                             const uint8_t* bloom_filter, int bloom_size,
                             uint8_t* match_bitvector) {
    int num_matches = 0;
    int i = 0;

    const uint32_t bloom_mask = (bloom_size * 8) - 1;
    const __m256i mask_vec = _mm256_set1_epi32(bloom_mask);

    // Process 8 hashes at a time with AVX2
    for (; i + AVX2_BATCH_SIZE <= num_keys; i += AVX2_BATCH_SIZE) {
        // Load 8 hash values
        __m256i hash_vec = _mm256_loadu_si256((__m256i*)(hashes + i));

        // Compute Bloom filter indices (h1 = hash, h2 = hash >> 16)
        __m256i h1 = _mm256_and_si256(hash_vec, mask_vec);
        __m256i h2 = _mm256_and_si256(_mm256_srli_epi32(hash_vec, 16), mask_vec);

        // Check Bloom filter bits (scalar - AVX2 gather is available but complex)
        uint32_t h1_vals[8], h2_vals[8];
        _mm256_storeu_si256((__m256i*)h1_vals, h1);
        _mm256_storeu_si256((__m256i*)h2_vals, h2);

        for (int j = 0; j < 8; ++j) {
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
 * Build Bloom filter from hash values using AVX2 SIMD.
 *
 * @param hashes Hash values (uint32_t[num_keys])
 * @param num_keys Number of keys
 * @param bloom_filter Output Bloom filter bit array
 * @param bloom_size Bloom filter size in bytes
 */
void build_bloom_filter_avx2(const uint32_t* hashes, int num_keys,
                              uint8_t* bloom_filter, int bloom_size) {
    const uint32_t bloom_mask = (bloom_size * 8) - 1;

    // NOTE: Bloom filter is already zeroed in __init__
    // Do NOT zero here as this function may be called multiple times during chunked build

    // Process all hashes (scalar for now - AVX2 scatter is complex)
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
 * Compact match indices using AVX2 SIMD.
 *
 * @param match_bitvector Bitvector indicating matches (uint8_t[num_keys])
 * @param num_keys Number of keys
 * @param left_indices Input left indices (uint32_t[num_keys])
 * @param right_indices Input right indices (uint32_t[num_keys])
 * @param out_left Output compacted left indices (uint32_t[num_matches])
 * @param out_right Output compacted right indices (uint32_t[num_matches])
 * @return Number of matches compacted
 */
int compact_match_indices_avx2(const uint8_t* match_bitvector, int num_keys,
                                const uint32_t* left_indices, const uint32_t* right_indices,
                                uint32_t* out_left, uint32_t* out_right) {
    int out_idx = 0;

    // Scalar compaction (AVX2 compress is available via _mm256_maskstore but complex)
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

#endif  // ARROW_HAVE_RUNTIME_AVX2
