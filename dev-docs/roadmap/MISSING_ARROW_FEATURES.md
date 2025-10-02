# Missing Arrow Features in sabot.arrow

Based on the inventory TopN demo, here are Arrow features we need to implement for better performance:

## High Priority (Performance Critical)

### 1. `concat_tables()` - Table Concatenation
**Current Status:** Not implemented
**Workaround:** Using `import pyarrow as _pa` directly
**Impact:** Critical for combining multiple batches
**Usage:**
```python
tables = [pa.Table.from_batches([b]) for b in batches]
combined = pa.concat_tables(tables)
```

### 2. `sort_indices()` - Efficient Sorting
**Current Status:** Available but slow (955 rows/sec on TopN)
**Impact:** Major performance bottleneck in TopN ranking
**Usage:**
```python
# Current (slow):
indices = pc.sort_indices(column, sort_keys=[("price", "descending")])

# Need optimized version with SIMD
```

### 3. `take()` - Array Indexing
**Current Status:** Available via pc.take()
**Performance:** Could be optimized
**Usage:**
```python
sorted_data = pc.take(table, indices)
```

## Medium Priority (Convenience)

### 4. `Table.sort_by()` - Direct Table Sorting
**Current Status:** Not implemented
**Workaround:** Using sort_indices + take
**Impact:** Code simplicity
**Usage:**
```python
sorted_table = table.sort_by([('price', 'descending')])
```

### 5. `group_by()` - Grouping Operations
**Current Status:** Not implemented (manual Python loops)
**Impact:** Performance on group-by-key operations
**Current Approach:**
```python
# Manual grouping:
for key in keys:
    mask = pc.equal(table.column('key'), key)
    group = table.filter(mask)
    # process group
```

**Desired:**
```python
grouped = table.group_by(['instrumentId', 'side']).aggregate({
    'price': 'max',
    'size': 'sum'
})
```

### 6. `row_number()` / Window Functions
**Current Status:** Manual ranking with enumerate
**Impact:** TopN queries, analytics
**Desired:**
```python
ranked = table.add_column('rank',
    pc.row_number().over(partition_by='instrumentId', order_by='price DESC')
)
```

## Low Priority (Nice to Have)

### 7. `join()` - Table Joins
**Current Status:** Not implemented
**Workaround:** Manual dictionary lookup
**Impact:** Code clarity
**Usage:**
```python
# Currently doing:
for row in left_table:
    key = row['key']
    right_row = lookup_dict.get(key)
    # merge

# Desired:
result = left_table.join(right_table, on='key', how='left')
```

### 8. Aggregate Functions
**Current Status:** Basic ones available (sum, mean, max, min)
**Missing:**
- `stddev()` - Standard deviation
- `variance()` - Variance
- `median()` - Median
- `percentile()` - Percentiles
- `first()` / `last()` - First/last in group

### 9. String Functions
**Current Status:** Basic string ops
**Missing:**
- `substring()`
- `concat()`
- `upper()` / `lower()`
- `trim()`
- `regexp_match()`

### 10. Date/Time Functions
**Current Status:** Minimal
**Missing:**
- `date_diff()`
- `date_add()`
- `extract()` (year, month, day)
- `timestamp()` parsing

## Performance Bottlenecks Found

### TopN Ranking (Current: 955 rows/sec)
**Bottleneck:** Python loop over 84K instruments × 2 sides
```python
for instrument in instruments:  # 84K iterations
    for side in sides:  # 2 iterations
        mask = pc.and_(...)
        group_data = table.filter(mask)
        indices = pc.sort_indices(...)  # Slow
        sorted_data = pc.take(...)
```

**Solution Needed:**
1. Vectorized group-by with sorting
2. SIMD-optimized sort_indices
3. Parallel processing of groups

### Expected Performance:
- **Target:** 50K-100K rows/sec (50-100x improvement)
- **With Cython:** Could reach 500K+ rows/sec

## Implementation Priority

### Phase 1 (Immediate):
1. ✅ `concat_tables()` - Already using workaround
2. 🔥 Optimize `sort_indices()` - Major bottleneck
3. 🔥 `group_by()` with aggregation

### Phase 2 (Short-term):
4. `Table.sort_by()` - Convenience
5. Window functions (`row_number()`, `rank()`)
6. `join()` operations

### Phase 3 (Long-term):
7. Advanced aggregations
8. String functions
9. Date/time functions

## Recommendations

### For TopN Demo:
- **Option A:** Implement fast group_by + sort in Cython
- **Option B:** Use DuckDB for complex queries (embedded)
- **Option C:** Parallelize the Python loop with multiprocessing

### For Production:
- Focus on Phase 1 features (concat_tables, fast sorting, group_by)
- These cover 80% of stream processing use cases
- Can achieve 10-100x performance gains
