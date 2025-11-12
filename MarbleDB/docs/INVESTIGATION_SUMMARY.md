# InsertBatch Iterator Investigation - Summary

## Key Findings

### 1. InsertBatch Storage (WORKING)
- **Stores in**: `arrow_active_memtable` (in-memory)
- **Path**: InsertBatchInternal → lsm_tree_->PutBatch() → arrow_active_memtable_->PutBatch()
- **Status**: Data is successfully stored in memory
- **Metadata**: cf_id, batch_id, cf_name attached to RecordBatch schema

### 2. Iterator Reads (BROKEN)
- **Source**: LSMBatchIterator constructor
- **Calls**: ScanSSTablesBatches() which includes memtable scan
- **Status**: Returns invalid immediately (empty batches_)

### 3. ScanSSTablesBatches Implementation (CORRECT DESIGN)
- **Already includes memtable scans**: arrow_active_memtable + arrow_immutable_memtables
- **Should return data**: Code structure is correct
- **Likely issue**: Memtable scan returns empty or batches are filtered out

---

## Root Cause Analysis

### The Critical Path

```
LSMBatchIterator Constructor:
  ├─ Call: lsm_->ScanSSTablesBatches(start_key, end_key, &scan_batches)
  │  └─ Should include: arrow_active_memtable_->ScanBatches()
  │
  ├─ Receive: scan_batches vector
  │
  └─ Filter by metadata:
     for (each batch in scan_batches) {
         if (batch->schema()->metadata()->FindKey("cf_id") matches expected_cf_id) {
             batches_.push_back(batch);  ← Only if matches
         }
     }
  
  └─ Set: valid_ = (batches_.empty() ? false : true)
```

### Two Possible Failure Points

**Point A**: ScanSSTablesBatches returns empty
- Memtable scan broken
- Key range encoding mismatch
- Memtable not initialized

**Point B**: ScanSSTablesBatches returns data but filtered out
- Metadata not attached in InsertBatchInternal
- Metadata lost during storage/retrieval
- cf_id format mismatch (string vs number)

---

## Code Locations

| Component | File | Lines | Status |
|-----------|------|-------|--------|
| InsertBatch | api.cpp | 1092-1120 | Works |
| InsertBatchInternal | api.cpp | 1126-1288 | Works |
| PutBatch | lsm_storage.cpp | 1186-1228 | Works |
| ScanSSTablesBatches | lsm_storage.cpp | 981-1111 | Design correct |
| LSMBatchIterator | api.cpp | 477-670 | Broken |
| ScanTable (for reference) | api.cpp | 1310-1342 | Works |

---

## Exact Code Showing the Issue

### InsertBatchInternal - Metadata Attachment

```cpp
// File: api.cpp:1147-1166
auto new_metadata = std::make_shared<arrow::KeyValueMetadata>();

// Copy existing metadata if present
auto existing_metadata = batch->schema()->metadata();
if (existing_metadata) {
    for (int64_t i = 0; i < existing_metadata->size(); ++i) {
        new_metadata->Append(existing_metadata->key(i), existing_metadata->value(i));
    }
}

// Add column family metadata
new_metadata->Append("cf_id", std::to_string(cf_info->id));
new_metadata->Append("batch_id", std::to_string(batch_id));
new_metadata->Append("cf_name", table_name);

auto schema_with_metadata = batch->schema()->WithMetadata(new_metadata);
auto batch_with_metadata = arrow::RecordBatch::Make(
    schema_with_metadata,
    batch->num_rows(),
    batch->columns());

// Write to LSM tree using zero-copy Arrow-native path
auto put_status = lsm_tree_->PutBatch(batch_with_metadata);
```

**Question**: Does batch_with_metadata preserve metadata through memtable storage?

### LSMBatchIterator - Metadata Filtering

```cpp
// File: api.cpp:493-519
if (status.ok()) {
    // Filter batches by cf_id metadata (ScanBatches returns all tables)
    std::string expected_cf_id = std::to_string(table_id);

    for (const auto& batch : scan_batches) {
        if (!batch || batch->num_rows() == 0) continue;

        // Check cf_id in metadata
        auto metadata = batch->schema()->metadata();
        if (metadata) {
            auto cf_id_index = metadata->FindKey("cf_id");
            if (cf_id_index != -1) {
                std::string batch_cf_id = metadata->value(cf_id_index);
                if (batch_cf_id == expected_cf_id) {
                    batches_.push_back(batch);  // ← Accepted
                }
                // If cf_id doesn't match, silently skipped!
            }
            // If metadata->FindKey("cf_id") returns -1, silently skipped!
        }
        // If metadata is null, silently skipped!
    }

    // Position at first row if we have data
    if (!batches_.empty() && batches_[0]->num_rows() > 0) {
        valid_ = true;
        current_batch_idx_ = 0;
        current_row_idx_ = 0;
    }
    // If batches is empty, valid_ remains false!
}
```

**Problem**: Multiple silent failures possible:
1. scan_batches is empty → batches_ is empty → valid_ = false
2. Metadata is null → batches_ stays empty → valid_ = false
3. cf_id key not found → batches_ stays empty → valid_ = false
4. cf_id value mismatch → batches_ stays empty → valid_ = false

---

## Why ScanTable Works

ScanTable doesn't use metadata filtering:

```cpp
// File: api.cpp:1310-1342
Status ScanTable(const std::string& table_name, std::unique_ptr<QueryResult>* result) override {
    std::lock_guard<std::mutex> lock(cf_mutex_);

    auto it = column_families_.find(table_name);
    if (it == column_families_.end()) {
        return Status::InvalidArgument("Column family does not exist");
    }

    auto* cf_info = it->second.get();

    // Scan all batches from LSM tree using zero-copy Arrow-native path
    std::vector<std::shared_ptr<arrow::RecordBatch>> all_batches;
    // ... implementation continues ...
    
    // Creates TableQueryResult which wraps the batches DIRECTLY
    // NO metadata filtering!
    *result = std::make_unique<TableQueryResult>(combined_table);
    return Status::OK();
}
```

ScanTable gets the same batches but doesn't filter by metadata, so it works.

---

## Diagnosis Tests

Add these to identify the exact failure point:

### Test 1: Verify Memtable Has Data
```cpp
// After InsertBatch, before NewIterator
auto mt = lsm_tree_->arrow_active_memtable_.get();
assert(mt != nullptr && "Memtable should be initialized");
// Check if batches are actually in the memtable
```

### Test 2: Verify ScanSSTablesBatches Returns Data
```cpp
std::vector<std::shared_ptr<arrow::RecordBatch>> scan_results;
uint64_t start_key = EncodeBatchKey(table_id, 0);
uint64_t end_key = EncodeBatchKey(table_id + 1, 0);

auto status = lsm_tree_->ScanSSTablesBatches(start_key, end_key, &scan_results);
assert(status.ok());
assert(!scan_results.empty() && "ScanSSTablesBatches should find data in memtable");

// If this assertion passes, problem is in metadata filtering
// If this assertion fails, problem is in memtable scan
```

### Test 3: Verify Metadata Is Present
```cpp
if (!scan_results.empty()) {
    auto batch = scan_results[0];
    auto metadata = batch->schema()->metadata();
    
    if (!metadata) {
        std::cout << "ERROR: Metadata is null!" << std::endl;
        return;  // Problem: Metadata not attached
    }
    
    auto cf_id_index = metadata->FindKey("cf_id");
    if (cf_id_index == -1) {
        std::cout << "ERROR: cf_id key not found in metadata!" << std::endl;
        return;  // Problem: Metadata key missing
    }
    
    std::string actual_cf_id = metadata->value(cf_id_index);
    std::string expected_cf_id = std::to_string(table_id);
    
    if (actual_cf_id != expected_cf_id) {
        std::cout << "ERROR: cf_id mismatch! Expected=" << expected_cf_id 
                  << " Actual=" << actual_cf_id << std::endl;
        return;  // Problem: cf_id value mismatch
    }
    
    std::cout << "SUCCESS: Metadata is correct" << std::endl;
}
```

---

## Recommended Fixes

### Fix 1: Remove Metadata Filtering (Simple, Fast)
If table isolation isn't needed, just accept all batches from ScanSSTablesBatches:

```cpp
// In LSMBatchIterator constructor, replace metadata filtering loop with:
for (const auto& batch : scan_batches) {
    if (!batch || batch->num_rows() == 0) continue;
    batches_.push_back(batch);  // Accept all batches
}
```

**Pros**: 
- Matches what ScanTable does
- Removes dependency on metadata preservation
- Fixes iterator immediately

**Cons**: 
- Loses per-table metadata isolation
- Works only if each table uses separate key ranges

### Fix 2: Debug and Fix Metadata Preservation
Verify metadata is properly preserved through memtable storage/retrieval.

**Investigation steps**:
1. Check if `arrow::RecordBatch::Make()` preserves metadata
2. Check if `ArrowBatchMemTable::PutBatch()` preserves metadata
3. Check if `ArrowBatchMemTable::ScanBatches()` returns batches with metadata intact

### Fix 3: Flush After Insert (Workaround)
```cpp
db->InsertBatch("table", batch);
db->Flush();  // Forces memtable to SSTable
db->NewIterator("table", ...)->Valid();  // Now true
```

This works because SSTables are on disk and metadata might be preserved there.

---

## Next Steps

1. **Run Test 2** above to determine if ScanSSTablesBatches returns data
   - If YES: Metadata filtering is broken (use Fix 1 or Fix 2)
   - If NO: Memtable scan is broken (requires deeper investigation)

2. **If ScanSSTablesBatches returns data**, run Test 3 to verify metadata

3. **Apply appropriate fix** based on diagnosis

4. **Verify with**: `assert(iterator->Valid())` after NewIterator()

---

## Files to Examine Next

If problems persist:

1. **ArrowBatchMemTable implementation**
   - Does `ScanBatches()` preserve RecordBatch metadata?
   - Does it return all batches or filter some out?

2. **EncodeBatchKey function**
   - Is the key range encoding consistent?
   - Do start_key/end_key properly cover all batch_ids?

3. **Metadata preservation through Arrow**
   - Does `RecordBatch::Make(schema_with_metadata, ...)` preserve metadata?
   - Does it work with memtable storage?

---

## Documents Generated

1. **INSERTBATCH_ITERATOR_INVESTIGATION.md** - Detailed technical analysis with code excerpts
2. **INSERTBATCH_ITERATOR_DATA_FLOW.md** - Visual diagrams of data flow and failure scenarios
3. **INVESTIGATION_SUMMARY.md** - This document
