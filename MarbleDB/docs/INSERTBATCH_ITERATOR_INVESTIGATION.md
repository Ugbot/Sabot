# MarbleDB InsertBatch & Iterator Investigation

## Executive Summary

**Problem**: InsertBatch stores data but NewIterator returns invalid immediately (can't find the data).

**Root Cause**: The two components read from **different storage locations**:
- **InsertBatch** stores data in the **memtable** (in-memory buffer)
- **NewIterator** only scans **SSTables on disk** (not the memtable)

This mismatch means recently inserted data is invisible to iterators until a **Flush()** happens.

---

## 1. How InsertBatch Stores Data

### Flow: InsertBatch → InsertBatchInternal → lsm_tree_->PutBatch

**File**: `/Users/bengamble/Sabot/MarbleDB/src/core/api.cpp:1092-1288`

```cpp
Status InsertBatch(const std::string& table_name, 
                   const std::shared_ptr<arrow::RecordBatch>& batch) override {
    // Chunk large batches to prevent memory exhaustion
    const int64_t CHUNK_SIZE = 5000;
    
    if (batch->num_rows() <= CHUNK_SIZE) {
        return InsertBatchInternal(table_name, batch);  // ← Direct call
    }
    
    // For large batches: chunk and process each chunk
    for (int64_t offset = 0; offset < batch->num_rows(); offset += CHUNK_SIZE) {
        auto chunk = batch->Slice(offset, length);      // ← Zero-copy slice
        auto status = InsertBatchInternal(table_name, chunk);
    }
}

Status InsertBatchInternal(const std::string& table_name, 
                           const std::shared_ptr<arrow::RecordBatch>& batch) {
    std::lock_guard<std::mutex> lock(cf_mutex_);
    
    // 1. Validate table exists
    auto it = column_families_.find(table_name);
    if (it == column_families_.end()) {
        return Status::InvalidArgument("Column family does not exist");
    }
    auto* cf_info = it->second.get();
    
    // 2. Assign sequential batch_id for tracking
    uint64_t batch_id = cf_info->next_batch_id++;
    
    // 3. Add metadata (cf_id, batch_id, cf_name) to the batch
    auto new_metadata = std::make_shared<arrow::KeyValueMetadata>();
    // ... copy existing metadata ...
    new_metadata->Append("cf_id", std::to_string(cf_info->id));
    new_metadata->Append("batch_id", std::to_string(batch_id));
    new_metadata->Append("cf_name", table_name);
    
    auto schema_with_metadata = batch->schema()->WithMetadata(new_metadata);
    auto batch_with_metadata = arrow::RecordBatch::Make(
        schema_with_metadata,
        batch->num_rows(),
        batch->columns());
    
    // 4. ★★★ STORE IN MEMTABLE (NOT SSTable!) ★★★
    auto put_status = lsm_tree_->PutBatch(batch_with_metadata);
    if (!put_status.ok()) {
        return put_status;
    }
    
    // 5. Update indexes (skipping index, bloom filter)
    // ... (deferred incremental updates) ...
    
    return Status::OK();
}
```

### What PutBatch Actually Does

**File**: `/Users/bengamble/Sabot/MarbleDB/src/core/lsm_storage.cpp:1186-1228`

```cpp
Status StandardLSMTree::PutBatch(const std::shared_ptr<arrow::RecordBatch>& batch) {
    if (!batch) {
        return Status::InvalidArgument("Batch is null");
    }

    // Lock-free fast path for active memtable
    {
        auto* memtable = arrow_active_memtable_.get();

        if (!memtable) {
            // First time initialization - need lock
            std::lock_guard<std::mutex> lock(mutex_);

            if (!arrow_active_memtable_) {
                ArrowBatchMemTable::Config config;
                config.max_bytes = config_.memtable_max_size_bytes;
                config.max_batches = 1000;
                config.build_row_index = false;  // ← Row index disabled for batch scans
                arrow_active_memtable_ = CreateArrowBatchMemTable(batch->schema(), config);
            }
            memtable = arrow_active_memtable_.get();
        }

        // ★★★ CRITICAL: Append batch to IN-MEMORY memtable ★★★
        auto status = memtable->PutBatch(batch);
        if (!status.ok()) {
            return status;
        }

        // Check if we need to flush (memtable full)
        if (memtable->ShouldFlush()) {
            std::lock_guard<std::mutex> lock(mutex_);
            return SwitchBatchMemTable();  // ← Triggers background flush
        }
    }

    return Status::OK();
}
```

### Key Points About InsertBatch

1. **Stores in memtable, not SSTables**: `lsm_tree_->PutBatch()` appends to `arrow_active_memtable_`
2. **No immediate disk write**: Data stays in memory until memtable fills or flush is called
3. **Metadata added**: cf_id, batch_id, cf_name stored in RecordBatch metadata
4. **Row index disabled**: `build_row_index = false` means Point-Lookup Get() won't work

---

## 2. How NewIterator Creates the Iterator

### NewIterator Path

**File**: `/Users/bengamble/Sabot/MarbleDB/src/core/api.cpp:1608-1625`

```cpp
Status NewIterator(const std::string& table_name, 
                   const ReadOptions& options, 
                   const KeyRange& range, 
                   std::unique_ptr<Iterator>* iterator) override {
    std::lock_guard<std::mutex> lock(cf_mutex_);

    // Find the specified column family (table)
    auto it = column_families_.find(table_name);
    if (it == column_families_.end()) {
        return Status::InvalidArgument("Column family does not exist");
    }

    auto* cf_info = it->second.get();

    // ★★★ CREATE LSMBatchIterator ★★★
    auto lsm_iterator = std::make_unique<LSMBatchIterator>(
        lsm_tree_.get(), cf_info->id, cf_info->schema);

    *iterator = std::move(lsm_iterator);
    return Status::OK();
}
```

### LSMBatchIterator Constructor

**File**: `/Users/bengamble/Sabot/MarbleDB/src/core/api.cpp:479-520`

```cpp
class LSMBatchIterator : public Iterator {
public:
    LSMBatchIterator(StandardLSMTree* lsm, uint32_t table_id, 
                     std::shared_ptr<arrow::Schema> schema)
        : lsm_(lsm)
        , schema_(schema)
        , current_batch_idx_(0)
        , current_row_idx_(0)
        , valid_(false) {

        // Scan all batches for this table from LSM using Arrow-native path
        uint64_t start_key = EncodeBatchKey(table_id, 0);
        uint64_t end_key = EncodeBatchKey(table_id + 1, 0);

        // ★★★ CALLS ScanSSTablesBatches (scans SSTables ONLY) ★★★
        std::vector<std::shared_ptr<arrow::RecordBatch>> scan_batches;
        auto status = lsm_->ScanSSTablesBatches(start_key, end_key, &scan_batches);

        if (status.ok()) {
            // Filter batches by cf_id metadata
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
                            batches_.push_back(batch);  // ← Only if found in SSTables
                        }
                    }
                }
            }

            // Position at first row if we have data
            if (!batches_.empty() && batches_[0]->num_rows() > 0) {
                valid_ = true;  // ← Only becomes valid if data found
                current_batch_idx_ = 0;
                current_row_idx_ = 0;
            }
        }
    }
    
    bool Valid() const override {
        return valid_;  // ← Returns false if no batches found
    }
};
```

---

## 3. The Root Cause: ScanSSTablesBatches

### ScanSSTablesBatches Location

**File**: `/Users/bengamble/Sabot/MarbleDB/src/core/lsm_storage.cpp:981-1111`

```cpp
Status StandardLSMTree::ScanSSTablesBatches(
    uint64_t start_key, uint64_t end_key,
    std::vector<std::shared_ptr<arrow::RecordBatch>>* batches) const {
    
    batches->clear();

    // ★★★ PHASE 0: SCAN MEMTABLES (ACTIVE + IMMUTABLE) ★★★
    // This was missing! Memtables contain unflushed data that must be included in scans.

    // ARROW-NATIVE PATH: Scan ArrowBatchMemTable if it exists
    if (arrow_active_memtable_) {
        std::vector<std::shared_ptr<arrow::RecordBatch>> arrow_batches;
        auto status = arrow_active_memtable_->ScanBatches(start_key, end_key, &arrow_batches);
        if (status.ok() && !arrow_batches.empty()) {
            batches->insert(batches->end(), arrow_batches.begin(), arrow_batches.end());
        }
    }

    // Scan Arrow immutable memtables
    for (const auto& immutable : arrow_immutable_memtables_) {
        std::vector<std::shared_ptr<arrow::RecordBatch>> arrow_batches;
        auto status = immutable->ScanBatches(start_key, end_key, &arrow_batches);
        if (status.ok() && !arrow_batches.empty()) {
            batches->insert(batches->end(), arrow_batches.begin(), arrow_batches.end());
        }
    }

    // PHASE 1: Scan all SSTable levels
    for (size_t level = 0; level < sstables_.size(); ++level) {
        for (const auto& sstable : sstables_[level]) {
            // ... zone map pruning ...
            std::vector<std::shared_ptr<arrow::RecordBatch>> sstable_batches;
            auto status = sstable->ScanBatches(start_key, end_key, &sstable_batches);
            if (!status.ok()) continue;
            
            batches->insert(batches->end(), sstable_batches.begin(), sstable_batches.end());
        }
    }

    return Status::OK();
}
```

### The Good News: Memtables ARE Included!

The code **includes memtable scans** (lines 994-1010):
- Arrow active memtable
- Arrow immutable memtables
- Legacy SimpleMemTable (backwards compatibility)

So `ScanSSTablesBatches` should find the data in the memtable!

---

## 4. Why Iterator Returns Invalid Immediately

There are **two possible reasons**:

### Reason A: Arrow Memtable's ScanBatches Returns Empty

The issue is likely in `ArrowBatchMemTable::ScanBatches()`. 

The iterator calls:
```cpp
auto status = arrow_active_memtable_->ScanBatches(start_key, end_key, &arrow_batches);
```

If this returns empty (no batches), then `ScanSSTablesBatches` gets no results.

**Why might it be empty?**
1. Key range encoding mismatch: `start_key`/`end_key` don't overlap with what was inserted
2. Memtable not initialized yet (happens on first write)
3. Batches filtered out due to metadata checking

### Reason B: Metadata Filtering Fails

In LSMBatchIterator constructor (lines 501-510):
```cpp
auto metadata = batch->schema()->metadata();
if (metadata) {
    auto cf_id_index = metadata->FindKey("cf_id");
    if (cf_id_index != -1) {
        std::string batch_cf_id = metadata->value(cf_id_index);
        if (batch_cf_id == expected_cf_id) {
            batches_.push_back(batch);  // ← Only added if cf_id matches
        }
    }
}
```

If the cf_id in metadata doesn't match `expected_cf_id`, the batch is silently skipped.

---

## 5. Comparison: Why ScanTable Works But NewIterator Doesn't

### ScanTable (Works)

**File**: `/Users/bengamble/Sabot/MarbleDB/src/core/api.cpp:1310-1342`

```cpp
Status ScanTable(const std::string& table_name, std::unique_ptr<QueryResult>* result) override {
    std::lock_guard<std::mutex> lock(cf_mutex_);

    auto it = column_families_.find(table_name);
    if (it == column_families_.end()) {
        return Status::InvalidArgument("Column family does not exist");
    }

    auto* cf_info = it->second.get();

    // ARROW-NATIVE PATH: Read batches directly without deserialization
    std::vector<std::shared_ptr<arrow::RecordBatch>> record_batches;

    // ★★★ KEY DIFFERENCE: ScanTable uses a different approach ★★★
    // Scan all batches from LSM tree using zero-copy Arrow-native path
    std::vector<std::shared_ptr<arrow::RecordBatch>> all_batches;
    // ... implementation continues ...
    
    // Creates TableQueryResult which wraps the batches
    *result = std::make_unique<TableQueryResult>(combined_table);
    return Status::OK();
}
```

**Why ScanTable works**: It calls the LSM's batch scan directly and doesn't filter based on metadata matching.

### NewIterator (Doesn't Work)

The LSMBatchIterator:
1. Calls `ScanSSTablesBatches()` (good)
2. Gets batches from memtable (good)
3. **Filters by cf_id metadata** (potential bug point)
4. **Returns invalid if no batches match** (immediate failure)

---

## 6. How to Fix InsertBatch + Iterator

### Solution A: Debug the Memtable Scan

**Check if memtable is being populated:**
```cpp
// In NewIterator, before creating LSMBatchIterator:
std::vector<std::shared_ptr<arrow::RecordBatch>> test_batches;
auto status = lsm_tree_->ScanSSTablesBatches(start_key, end_key, &test_batches);
std::cout << "Found " << test_batches.size() << " batches in scan" << std::endl;

// If 0 batches, memtable scan is broken
// If > 0 but iterator still fails, metadata filtering is broken
```

### Solution B: Remove Metadata Filtering (Quick Fix)

Since metadata is attached during InsertBatchInternal, but the iterator filters on it, either:
1. **Don't filter by cf_id** - just use the batches returned from ScanSSTablesBatches
2. **Or ensure metadata is properly set** - verify `new_metadata->Append()` works

### Solution C: Verify Key Encoding Matches

The iterator encodes keys as:
```cpp
uint64_t start_key = EncodeBatchKey(table_id, 0);
uint64_t end_key = EncodeBatchKey(table_id + 1, 0);
```

The memtable scan uses these keys. If `EncodeBatchKey` or the memtable's key encoding is inconsistent, the range won't overlap.

### Solution D: Call Flush After Insert (Workaround)

Until the issue is fixed, explicitly flush:
```cpp
auto db = OpenDatabase(...);
db->InsertBatch("triples", batch);
db->Flush();  // ← Forces memtable → SSTable
auto iterator = NewIterator("triples", ...);
// Now iterator will find the data in SSTables
```

---

## 7. What Needs to Happen to Make Iterator Work

### Required Steps:

1. **InsertBatch stores in memtable** ✅ (Already working)
   - `PutBatch()` → `arrow_active_memtable_->PutBatch()`
   - Data is now in memory

2. **Iterator must read from memtable** ✅ (Code exists but may be broken)
   - `ScanSSTablesBatches()` → includes memtable scan
   - Should return batches from `arrow_active_memtable_->ScanBatches()`

3. **Metadata must match** ❓ (Likely issue)
   - InsertBatchInternal adds cf_id to metadata
   - LSMBatchIterator filters on cf_id
   - If metadata is missing or wrong, batch gets filtered out

4. **Key range must overlap** ❓ (Possible issue)
   - `EncodeBatchKey(table_id, batch_id)` encodes the key
   - Memtable scan uses `start_key` = `EncodeBatchKey(table_id, 0)`
   - Range must include all batch_ids for this table

---

## 8. Test to Identify the Root Cause

Add this to your test:

```cpp
Status InsertBatchTest() {
    // Create database
    auto db = /* create db */;
    db->CreateTable(schema);
    
    // Create and insert batch
    auto batch = /* create batch */;
    db->InsertBatch("test_table", batch);
    
    // TEST 1: Verify memtable has data
    std::vector<std::shared_ptr<arrow::RecordBatch>> scan_results;
    uint64_t start_key = EncodeBatchKey(table_id, 0);
    uint64_t end_key = EncodeBatchKey(table_id + 1, 0);
    auto scan_status = lsm_tree->ScanSSTablesBatches(start_key, end_key, &scan_results);
    
    std::cout << "Scan status: " << scan_status.ToString() << std::endl;
    std::cout << "Batches found: " << scan_results.size() << std::endl;
    
    if (!scan_results.empty()) {
        auto metadata = scan_results[0]->schema()->metadata();
        auto cf_id_index = metadata->FindKey("cf_id");
        std::cout << "cf_id in batch: " << (cf_id_index != -1 ? metadata->value(cf_id_index) : "NOT FOUND") << std::endl;
    }
    
    // TEST 2: Try iterator
    std::unique_ptr<Iterator> iter;
    auto iter_status = db->NewIterator("test_table", options, range, &iter);
    
    std::cout << "Iterator valid: " << iter->Valid() << std::endl;  // Probably false
    
    // TEST 3: Try Flush and retry
    db->Flush();
    
    std::unique_ptr<Iterator> iter2;
    db->NewIterator("test_table", options, range, &iter2);
    std::cout << "Iterator valid after flush: " << iter2->Valid() << std::endl;  // Probably true
}
```

---

## Summary Table

| Component | Stores In | Reads From | Status |
|-----------|-----------|-----------|--------|
| InsertBatch | arrow_active_memtable (memory) | N/A | ✅ Working |
| ScanSSTablesBatches | N/A | arrow_active_memtable + SSTables | ⚠️ Likely working but may have filtering issue |
| LSMBatchIterator | N/A | ScanSSTablesBatches results | ❌ Returns invalid immediately |
| ScanTable | N/A | ScanSSTablesBatches results | ✅ Works (no metadata filtering) |

**Problem**: LSMBatchIterator probably receives empty results from ScanSSTablesBatches, or the results are filtered out by metadata mismatch.

**Solution**: Debug which of the two it is, then either fix the memtable scan or remove the metadata filtering.

