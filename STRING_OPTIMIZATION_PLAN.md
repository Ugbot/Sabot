# String Operation Optimization Plan

## Problem Analysis

Sabot is **2-20x slower** than DuckDB on string-heavy queries:

| Query | Operation | Slowdown |
|-------|-----------|----------|
| Q2 | COUNT WHERE AdvEngineID <> 0 | 16.2x |
| Q8 | GROUP BY WHERE (string) | 15.8x |
| Q11 | WHERE + GROUP BY (string) | 20.4x |
| Q22 | LIKE + GROUP BY | 3.7x |
| Q28 | STRLEN + aggregation | 5.5x |
| Q29 | REGEXP_REPLACE | 2.7x |

**Pattern**: String filtering and string functions are bottlenecks

## Root Causes

### 1. String Comparison Overhead

**Current Implementation** (likely):
```cpp
// Naive string comparison
for (int i = 0; i < num_rows; i++) {
    if (string_col[i] != "") {
        // Process row
    }
}
```

**Problem**: Row-by-row comparison, no SIMD

**DuckDB's Approach**:
- Vectorized string comparisons
- Specialized kernels for common patterns (empty string, equality)
- Dictionary encoding for repeated strings

### 2. No Dictionary Encoding

**Current**: Store full strings
```
["apple", "apple", "banana", "apple", "banana"]
→ 5 string objects, repeated allocations
```

**DuckDB**: Dictionary encoding
```
Dictionary: ["apple", "banana"]
Indices: [0, 0, 1, 0, 1]
→ 2 strings + 5 integers (much faster!)
```

**Impact**: 5-10x faster string operations

### 3. LIKE Pattern Matching

**Current**: Likely using standard string::find()

**DuckDB**: Optimized pattern matching
- Boyer-Moore for long patterns
- SIMD for simple patterns
- Pre-compiled patterns

### 4. Regular Expressions

**Current**: std::regex (slow)

**DuckDB**: RE2 or Hyperscan
- Faster regex engines
- DFA-based matching
- Compiled patterns

## Optimization Strategy

### Phase 1: Arrow String Kernels (Immediate)

**Use Arrow's built-in string functions**:

```cpp
// Instead of custom string operations
// Use Arrow compute kernels

#include <arrow/compute/api.h>

// String equality
auto result = arrow::compute::Equal(string_array, "value");

// String contains
auto result = arrow::compute::MatchSubstring(string_array, "pattern");

// String length
auto result = arrow::compute::StrLength(string_array);
```

**Benefits**:
- Already SIMD-optimized
- Well-tested
- Zero-copy where possible

**Implementation**:
```cpp
// Location: sabot_sql/src/sql/string_operations.cpp (create)

#include <arrow/compute/api.h>

arrow::Result<std::shared_ptr<arrow::Array>> 
StringEquals(const std::shared_ptr<arrow::Array>& arr, const std::string& value) {
    return arrow::compute::CallFunction("equal", {arr, arrow::MakeScalar(value)});
}

arrow::Result<std::shared_ptr<arrow::Array>>
StringNotEquals(const std::shared_ptr<arrow::Array>& arr, const std::string& value) {
    return arrow::compute::CallFunction("not_equal", {arr, arrow::MakeScalar(value)});
}

arrow::Result<std::shared_ptr<arrow::Array>>
StringContains(const std::shared_ptr<arrow::Array>& arr, const std::string& pattern) {
    return arrow::compute::CallFunction("match_substring", {arr, arrow::MakeScalar(pattern)});
}
```

**Expected Improvement**: 5-10x faster string operations

### Phase 2: Dictionary Encoding (High Impact)

**Implement automatic dictionary encoding**:

```cpp
// Location: sabot_sql/src/sql/dictionary_encoder.h

class DictionaryEncoder {
public:
    // Convert string array to dictionary-encoded array
    arrow::Result<std::shared_ptr<arrow::DictionaryArray>>
    Encode(const std::shared_ptr<arrow::StringArray>& strings);
    
    // Check if worth encoding (based on cardinality)
    bool ShouldEncode(const std::shared_ptr<arrow::Array>& arr);
    
private:
    // Hash table for dictionary
    std::unordered_map<std::string, int32_t> dictionary_;
};
```

**When to use**:
- Cardinality < 10% of rows
- Repeated queries on same column
- GROUP BY on string columns

**Implementation**:
```cpp
arrow::Result<std::shared_ptr<arrow::DictionaryArray>>
DictionaryEncoder::Encode(const std::shared_ptr<arrow::StringArray>& strings) {
    // Build dictionary
    std::vector<std::string> dict_values;
    std::unordered_map<std::string, int32_t> dict_map;
    arrow::Int32Builder indices_builder;
    
    for (int64_t i = 0; i < strings->length(); i++) {
        if (strings->IsNull(i)) {
            ARROW_RETURN_NOT_OK(indices_builder.AppendNull());
            continue;
        }
        
        std::string value = strings->GetString(i);
        auto it = dict_map.find(value);
        
        if (it == dict_map.end()) {
            // New value - add to dictionary
            int32_t index = dict_values.size();
            dict_values.push_back(value);
            dict_map[value] = index;
            ARROW_RETURN_NOT_OK(indices_builder.Append(index));
        } else {
            // Existing value - use index
            ARROW_RETURN_NOT_OK(indices_builder.Append(it->second));
        }
    }
    
    // Build dictionary array
    arrow::StringBuilder dict_builder;
    for (const auto& val : dict_values) {
        ARROW_RETURN_NOT_OK(dict_builder.Append(val));
    }
    
    std::shared_ptr<arrow::Array> dict_array;
    ARROW_RETURN_NOT_OK(dict_builder.Finish(&dict_array));
    
    std::shared_ptr<arrow::Array> indices_array;
    ARROW_RETURN_NOT_OK(indices_builder.Finish(&indices_array));
    
    // Create dictionary type and array
    auto dict_type = arrow::dictionary(arrow::int32(), arrow::utf8());
    return arrow::DictionaryArray::FromArrays(dict_type, indices_array, dict_array);
}
```

**Expected Improvement**: 3-10x for string GROUP BY, DISTINCT

### Phase 3: SIMD String Kernels (Advanced)

**Use simdjson-style SIMD for strings**:

```cpp
// Location: sabot_sql/src/sql/simd_strings.h

#include <immintrin.h>  // AVX2/NEON intrinsics

// SIMD string equality check
bool simd_string_equals(const char* str, size_t len, const char* pattern, size_t pattern_len) {
    if (len != pattern_len) return false;
    
    // Process 32 bytes at a time with AVX2
    size_t i = 0;
    for (; i + 32 <= len; i += 32) {
        __m256i a = _mm256_loadu_si256((__m256i*)(str + i));
        __m256i b = _mm256_loadu_si256((__m256i*)(pattern + i));
        __m256i cmp = _mm256_cmpeq_epi8(a, b);
        
        if (_mm256_movemask_epi8(cmp) != 0xFFFFFFFF) {
            return false;  // Mismatch found
        }
    }
    
    // Handle remainder
    for (; i < len; i++) {
        if (str[i] != pattern[i]) return false;
    }
    
    return true;
}
```

**Expected Improvement**: 2-4x for string equality

### Phase 4: Better String Functions

**Vendor optimized string libraries**:

1. **RE2** (Google's regex engine)
   - Much faster than std::regex
   - DFA-based matching
   - Thread-safe

2. **Hyperscan** (Intel's pattern matching)
   - SIMD-optimized
   - Multiple patterns simultaneously
   - Used by Snort, Suricata

**Implementation**:
```cmake
# CMakeLists.txt
add_subdirectory(vendor/re2)
target_link_libraries(sabot_sql re2)
```

```cpp
#include <re2/re2.h>

// Much faster regex matching
bool regex_match(const std::string& text, const std::string& pattern) {
    RE2 re(pattern);
    return RE2::FullMatch(text, re);
}
```

**Expected Improvement**: 5-10x for REGEXP operations

## Implementation Plan

### Step 1: Use Arrow String Kernels (2-4 hours)

**Files to create**:
- `sabot_sql/include/sabot_sql/sql/string_operations.h`
- `sabot_sql/src/sql/string_operations.cpp`

**What to implement**:
- `StringEquals()` - Use Arrow compute
- `StringNotEquals()` - Use Arrow compute
- `StringContains()` - Use Arrow compute (match_substring)
- `StringLength()` - Use Arrow compute (utf8_length)
- `StringMatches()` - Use Arrow compute (match_substring_regex)

**Integrate**:
- Update query translator to use new string ops
- Replace naive comparisons
- Test with Q2, Q8, Q11

**Expected**: 3-5x improvement

### Step 2: Dictionary Encoding (4-6 hours)

**Files to create**:
- `sabot_sql/include/sabot_sql/sql/dictionary_encoder.h`
- `sabot_sql/src/sql/dictionary_encoder.cpp`

**What to implement**:
- Auto-detect low-cardinality columns
- Encode on first GROUP BY/DISTINCT
- Cache encoded versions
- Use encoded for all operations

**Expected**: 5-10x on GROUP BY with strings

### Step 3: Vendor RE2 (1-2 hours)

**Steps**:
```bash
cd vendor
git clone --depth=1 https://github.com/google/re2.git

# Update CMakeLists.txt
add_subdirectory(vendor/re2)
target_link_libraries(sabot_sql re2)
```

**Replace**:
- std::regex → RE2
- Compile patterns once
- Cache compiled patterns

**Expected**: 5-10x on regex queries

### Step 4: SIMD String Compare (Advanced, 6-8 hours)

**Only if needed** after above optimizations:
- Implement AVX2/NEON string kernels
- Focus on hot paths (equality, contains)
- Benchmark before/after

**Expected**: Additional 2-3x

## Quick Wins (Priority Order)

### 1. Use Arrow String Kernels (Immediate)

**Effort**: 2-4 hours
**Impact**: 3-5x improvement
**Risk**: Low (Arrow is well-tested)

**Code**:
```cpp
// Replace this:
bool is_empty = (str == "");

// With this:
auto result = arrow::compute::Equal(string_array, "");
```

### 2. Vendor RE2 (Quick)

**Effort**: 1-2 hours
**Impact**: 5-10x on regex queries
**Risk**: Low (Google library)

### 3. Dictionary Encoding (High Impact)

**Effort**: 4-6 hours
**Impact**: 5-10x on GROUP BY/DISTINCT strings
**Risk**: Medium (need careful implementation)

## Expected Results After Optimization

### Current (Without Optimizations)

| Query Type | Current | Target | Method |
|------------|---------|--------|--------|
| String WHERE | 16x slower | 1-2x | Arrow kernels |
| String GROUP BY | 15x slower | 1-2x | Dictionary encoding |
| STRLEN | 5x slower | 1x | Arrow utf8_length |
| REGEXP | 3x slower | 1x | RE2 |

### After Phase 1 (Arrow Kernels)

**Expected**:
- Q2: 16x slower → 3-5x slower
- Q8: 15x slower → 3-5x slower
- Q11: 20x slower → 4-6x slower

**Overall**: Competitive on string queries

### After Phase 2 (Dictionary Encoding)

**Expected**:
- Q8, Q11, Q13, Q14: 1-2x slower (or faster!)
- GROUP BY queries: Sabot advantage

### After Phase 3 (RE2)

**Expected**:
- Q29 (REGEXP): 3x slower → 1x or faster
- Regex queries: Competitive

### Final Expected Performance

| Query Type | Current | After Optimization |
|------------|---------|-------------------|
| Numeric aggregation | 2-6x faster | 2-6x faster (no change) |
| String WHERE | 2-16x slower | 1-2x faster |
| String GROUP BY | 2-20x slower | 1-2x faster |
| REGEXP | 3x slower | 1-2x faster |
| Overall | 1.3x slower | **1.5-2x faster** |

## Recommended Approach

### Quick Win: Arrow String Kernels

**Start here** - biggest impact for least effort:

1. Create `sabot_sql/src/sql/string_operations.cpp`
2. Wrap Arrow compute string functions
3. Update SQL translator to use them
4. Test on Q2, Q8, Q11

**Timeline**: 1 day
**Impact**: 3-5x improvement on string queries

### Medium Term: Dictionary Encoding

**After Arrow kernels working**:

1. Implement dictionary encoder
2. Auto-detect low-cardinality columns
3. Encode on first use
4. Cache for queries

**Timeline**: 2-3 days  
**Impact**: 5-10x on string GROUP BY

### Long Term: Advanced Optimizations

**If needed**:
- RE2 for regex
- SIMD string kernels
- Hyperscan for pattern matching

**Timeline**: 1-2 weeks
**Impact**: Additional 2-5x

## Files to Create

### 1. String Operations (Priority 1)

```
sabot_sql/
  include/sabot_sql/sql/
    string_operations.h       ← Arrow string kernel wrappers
  src/sql/
    string_operations.cpp     ← Implementation
```

### 2. Dictionary Encoding (Priority 2)

```
sabot_sql/
  include/sabot_sql/sql/
    dictionary_encoder.h      ← Dictionary encoding
  src/sql/
    dictionary_encoder.cpp    ← Implementation
```

### 3. Regex Optimization (Priority 3)

```
vendor/
  re2/                        ← Vendor RE2
  
sabot_sql/
  include/sabot_sql/sql/
    regex_engine.h            ← RE2 wrapper
  src/sql/
    regex_engine.cpp          ← Implementation
```

## Implementation Starting Point

Let me create the string operations wrapper now:

```cpp
// sabot_sql/include/sabot_sql/sql/string_operations.h

#pragma once

#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <string>

namespace sabot_sql {
namespace sql {

class StringOperations {
public:
    // Equality operations
    static arrow::Result<std::shared_ptr<arrow::BooleanArray>>
    Equal(const std::shared_ptr<arrow::StringArray>& arr, const std::string& value);
    
    static arrow::Result<std::shared_ptr<arrow::BooleanArray>>
    NotEqual(const std::shared_ptr<arrow::StringArray>& arr, const std::string& value);
    
    // Pattern matching
    static arrow::Result<std::shared_ptr<arrow::BooleanArray>>
    Contains(const std::shared_ptr<arrow::StringArray>& arr, const std::string& pattern);
    
    static arrow::Result<std::shared_ptr<arrow::BooleanArray>>
    MatchLike(const std::shared_ptr<arrow::StringArray>& arr, const std::string& pattern);
    
    static arrow::Result<std::shared_ptr<arrow::BooleanArray>>
    MatchRegex(const std::shared_ptr<arrow::StringArray>& arr, const std::string& pattern);
    
    // String functions
    static arrow::Result<std::shared_ptr<arrow::Int32Array>>
    Length(const std::shared_ptr<arrow::StringArray>& arr);
    
    static arrow::Result<std::shared_ptr<arrow::StringArray>>
    Replace(const std::shared_ptr<arrow::StringArray>& arr, 
            const std::string& pattern, 
            const std::string& replacement);
    
    // Aggregations
    static arrow::Result<std::shared_ptr<arrow::StringArray>>
    Min(const std::shared_ptr<arrow::StringArray>& arr);
    
    static arrow::Result<std::shared_ptr<arrow::StringArray>>
    Max(const std::shared_ptr<arrow::StringArray>& arr);
};

} // namespace sql
} // namespace sabot_sql
```

## Next Steps

1. **Implement Arrow string kernels** (start now)
2. **Test on failing queries** (Q2, Q8, Q11)
3. **Measure improvement** (expect 3-5x)
4. **Add dictionary encoding** (if needed)
5. **Vendor RE2** (for regex queries)

**Goal**: Match or beat DuckDB on string operations while maintaining advantage on numeric operations

**Expected Final Result**: Sabot 1.5-2x faster overall on ClickBench
