# MarbleDB InsertBatch vs Iterator - Exact Code Snippets

## 1. How InsertBatch Stores Data

### Entry Point: InsertBatch
**File**: `/Users/bengamble/Sabot/MarbleDB/src/core/api.cpp:1092-1120`

```cpp
Status InsertBatch(const std::string& table_name, 
                   const std::shared_ptr<arrow::RecordBatch>& batch) override {
    // Chunk large batches to prevent memory exhaustion
    // Use zero-copy slicing for efficient processing
    const int64_t CHUNK_SIZE = 5000;  // Process 5K rows at a time

    if (batch->num_rows() <= CHUNK_SIZE) {
        // Small batch, process directly
        return InsertBatchInternal(table_name, batch);
    }

    // Large batch, chunk and process
    for (int64_t offset = 0; offset < batch->num_rows(); offset += CHUNK_SIZE) {
        int64_t length = std::min(CHUNK_SIZE, batch->num_rows() - offset);

        // Safety check
        if (offset + length > batch->num_rows()) {
            return Status::InvalidArgument("Slice bounds exceed batch size");
        }

        auto chunk = batch->Slice(offset, length);  // Zero-copy slice
        if (!chunk) {
            return Status::InvalidArgument("Failed to slice batch");
        }

        auto status = InsertBatchInternal(table_name, chunk);
        if (!status.ok()) {
            return status;
        }
    }

    return Status::OK();
}
```

### Core Storage: InsertBatchInternal
**File**: `/Users/bengamble/Sabot/MarbleDB/src/core/api.cpp:1126-1288`

```cpp
Status InsertBatchInternal(const std::string& table_name, 
                           const std::shared_ptr<arrow::RecordBatch>& batch) {
    std::lock_guard<std::mutex> lock(cf_mutex_);

    // Find column family
    auto it = column_families_.find(table_name);
    if (it == column_families_.end()) {
        return Status::InvalidArgument("Column family '" + table_name + "' does not exist");
    }

    auto* cf_info = it->second.get();

    // Validate batch schema matches column family schema
    if (!batch->schema()->Equals(cf_info->schema)) {
        return Status::InvalidArgument("Batch schema does not match column family schema for '" + table_name + "'");
    }

    // Assign sequential batch ID
    uint64_t batch_id = cf_info->next_batch_id++;

    // ARROW-NATIVE PATH: Store RecordBatch directly without serialization
    // Add column family metadata to the batch for filtering during scans
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

    // ★★★ STORE IN MEMTABLE - THIS IS WHERE DATA GOES ★★★
    auto put_status = lsm_tree_->PutBatch(batch_with_metadata);
    if (!put_status.ok()) {
        return put_status;
    }

    // Incrementally update indexes (O(m) where m = batch size, not O(n) where n = total data)
    // Initialize indexes on first insert
    if (cf_info->next_batch_id == 1) {
        cf_info->skipping_index = std::make_shared<InMemorySkippingIndex>();
        cf_info->bloom_filter = std::make_shared<BloomFilter>(100000, 0.01);
    }

    // Update skipping index with new batch only
    if (cf_info->skipping_index) {
        auto single_batch_table_result = arrow::Table::FromRecordBatches(cf_info->schema, {batch});
        if (single_batch_table_result.ok()) {
            std::shared_ptr<arrow::Table> single_batch_table = single_batch_table_result.ValueOrDie();
            cf_info->skipping_index->BuildFromTable(single_batch_table, 8192);
        }
    }

    // Update bloom filter with new batch only (incremental)
    if (cf_info->bloom_filter && batch->num_columns() >= 3) {
        auto subject_col = std::static_pointer_cast<arrow::Int64Array>(batch->column(0));
        auto predicate_col = std::static_pointer_cast<arrow::Int64Array>(batch->column(1));
        auto object_col = std::static_pointer_cast<arrow::Int64Array>(batch->column(2));

        for (int64_t i = 0; i < batch->num_rows(); ++i) {
            std::string key_str = std::to_string(subject_col->Value(i)) + "," +
                                std::to_string(predicate_col->Value(i)) + "," +
                                std::to_string(object_col->Value(i));
            cf_info->bloom_filter->Add(key_str);
        }
    }

    return Status::OK();
}
```

### Storage Layer: PutBatch
**File**: `/Users/bengamble/Sabot/MarbleDB/src/core/lsm_storage.cpp:1186-1228`

```cpp
Status StandardLSMTree::PutBatch(const std::shared_ptr<arrow::RecordBatch>& batch) {
    if (!batch) {
        return Status::InvalidArgument("Batch is null");
    }

    // Lock-free fast path: try to append to active memtable
    // Only lock if we need to switch memtables
    {
        // Read active memtable atomically
        auto* memtable = arrow_active_memtable_.get();

        if (!memtable) {
            // First time initialization - need lock
            std::lock_guard<std::mutex> lock(mutex_);

            // Double-check after acquiring lock
            if (!arrow_active_memtable_) {
                ArrowBatchMemTable::Config config;
                config.max_bytes = config_.memtable_max_size_bytes;
                config.max_batches = 1000;
                config.build_row_index = false;  // ← Row index disabled for batch-scan workloads
                arrow_active_memtable_ = CreateArrowBatchMemTable(batch->schema(), config);
            }
            memtable = arrow_active_memtable_.get();
        }

        // ★★★ CRITICAL: Append batch to IN-MEMORY memtable ★★★
        auto status = memtable->PutBatch(batch);
        if (!status.ok()) {
            return status;
        }

        // Check if we need to switch (lock-free check)
        if (memtable->ShouldFlush()) {
            // Need to switch - acquire lock for memtable rotation
            std::lock_guard<std::mutex> lock(mutex_);
            return SwitchBatchMemTable();
        }
    }

    return Status::OK();
}
```

---

## 2. How NewIterator Creates the Iterator

### Entry Point: NewIterator
**File**: `/Users/bengamble/Sabot/MarbleDB/src/core/api.cpp:1608-1625`

```cpp
Status NewIterator(const std::string& table_name, 
                   const ReadOptions& options, 
                   const KeyRange& range, 
                   std::unique_ptr<Iterator>* iterator) override {
    std::lock_guard<std::mutex> lock(cf_mutex_);

    // Find the specified column family
    auto it = column_families_.find(table_name);
    if (it == column_families_.end()) {
        return Status::InvalidArgument("Column family '" + table_name + "' does not exist");
    }

    auto* cf_info = it->second.get();

    // ★★★ CREATE LSMBatchIterator ★★★
    auto lsm_iterator = std::make_unique<LSMBatchIterator>(
        lsm_tree_.get(), cf_info->id, cf_info->schema);

    *iterator = std::move(lsm_iterator);
    return Status::OK();
}
```

### Iterator Constructor: LSMBatchIterator
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
        uint64_t end_key = EncodeBatchKey(table_id + 1, 0);  // Next table's range

        // ★★★ CALLS ScanSSTablesBatches ★★★
        std::vector<std::shared_ptr<arrow::RecordBatch>> scan_batches;
        auto status = lsm_->ScanSSTablesBatches(start_key, end_key, &scan_batches);

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
                            batches_.push_back(batch);
                        }
                    }
                }
            }

            // Position at first row if we have data
            if (!batches_.empty() && batches_[0]->num_rows() > 0) {
                valid_ = true;
                current_batch_idx_ = 0;
                current_row_idx_ = 0;
            }
        }
    }

    bool Valid() const override {
        return valid_;  // ← THIS RETURNS FALSE WHEN batches_ IS EMPTY
    }

    // ... rest of iterator implementation ...
};
```

---

## 3. ScanSSTablesBatches - How It Reads

### The Method That Should Find Data
**File**: `/Users/bengamble/Sabot/MarbleDB/src/core/lsm_storage.cpp:981-1111`

```cpp
Status StandardLSMTree::ScanSSTablesBatches(
    uint64_t start_key, uint64_t end_key,
    std::vector<std::shared_ptr<arrow::RecordBatch>>* batches) const {
    
    batches->clear();

    // ★★★ PHASE 0: SCAN MEMTABLES (ACTIVE + IMMUTABLE) ★★★
    // This was missing! Memtables contain unflushed data that must be included in scans.

    // ARROW-NATIVE PATH (zero-copy): Scan ArrowBatchMemTable if it exists
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

    // LEGACY PATH: Scan SimpleMemTable (keep for backwards compatibility)
    std::vector<std::pair<uint64_t, std::string>> memtable_results;

    // Scan active memtable
    if (active_memtable_) {
        std::vector<SimpleMemTableEntry> active_entries;
        active_memtable_->Scan(start_key, end_key, &active_entries);
        for (const auto& entry : active_entries) {
            if (entry.op == SimpleMemTableEntry::kPut) {
                memtable_results.emplace_back(entry.key, entry.value);
            }
        }
    }

    // Scan immutable memtables
    for (const auto& memtable : immutable_memtables_) {
        std::vector<SimpleMemTableEntry> entries;
        memtable->Scan(start_key, end_key, &entries);

        for (const auto& entry : entries) {
            if (entry.op == SimpleMemTableEntry::kPut) {
                memtable_results.emplace_back(entry.key, entry.value);
            }
        }
    }

    // Convert memtable results to RecordBatch if any data found
    if (!memtable_results.empty()) {
        arrow::UInt64Builder key_builder;
        arrow::BinaryBuilder value_builder;

        for (const auto& [key, value] : memtable_results) {
            auto append_key_status = key_builder.Append(key);
            if (!append_key_status.ok()) {
                return Status::InternalError("Failed to build key array from memtable");
            }
            auto append_value_status = value_builder.Append(value);
            if (!append_value_status.ok()) {
                return Status::InternalError("Failed to build value array from memtable");
            }
        }

        std::shared_ptr<arrow::Array> key_array, value_array;
        auto key_finish_status = key_builder.Finish(&key_array);
        if (!key_finish_status.ok()) {
            return Status::InternalError("Failed to finish key array from memtable");
        }
        auto value_finish_status = value_builder.Finish(&value_array);
        if (!value_finish_status.ok()) {
            return Status::InternalError("Failed to finish value array from memtable");
        }

        // Create RecordBatch from memtable data
        auto schema = arrow::schema({
            arrow::field("key", arrow::uint64()),
            arrow::field("value", arrow::binary())
        });

        auto memtable_batch = arrow::RecordBatch::Make(
            schema, memtable_results.size(), {key_array, value_array});
        batches->push_back(memtable_batch);
    }

    // PHASE 1: Scan all SSTable levels
    for (size_t level = 0; level < sstables_.size(); ++level) {
        for (const auto& sstable : sstables_[level]) {
            const auto& metadata = sstable->GetMetadata();

            // Phase 1: SSTable-level zone map pruning (min_key/max_key)
            if (metadata.max_key < start_key || metadata.min_key > end_key) {
                continue;  // SSTable doesn't overlap with range
            }

            // Phase 3: Get batches from SSTable
            std::vector<std::shared_ptr<arrow::RecordBatch>> sstable_batches;
            auto status = sstable->ScanBatches(start_key, end_key, &sstable_batches);
            if (!status.ok()) continue;

            // Collect batches from this SSTable
            batches->insert(batches->end(), sstable_batches.begin(), sstable_batches.end());
        }
    }

    return Status::OK();
}
```

---

## 4. Why ScanTable Works (No Metadata Filtering)

**File**: `/Users/bengamble/Sabot/MarbleDB/src/core/api.cpp:1310-1342`

```cpp
Status ScanTable(const std::string& table_name, std::unique_ptr<QueryResult>* result) override {
    std::lock_guard<std::mutex> lock(cf_mutex_);

    // Find column family
    auto it = column_families_.find(table_name);
    if (it == column_families_.end()) {
        return Status::InvalidArgument("Column family '" + table_name + "' does not exist");
    }

    auto* cf_info = it->second.get();

    // ARROW-NATIVE PATH: Read batches directly without deserialization
    std::vector<std::shared_ptr<arrow::RecordBatch>> record_batches;

    // ★★★ KEY DIFFERENCE: ScanTable uses a different approach ★★★
    // Scan all batches from LSM tree using zero-copy Arrow-native path
    std::vector<std::shared_ptr<arrow::RecordBatch>> all_batches;
    // ... implementation continues ...
    
    // Creates TableQueryResult which wraps the batches
    // ★★★ NO METADATA FILTERING ★★★
    *result = std::make_unique<TableQueryResult>(combined_table);
    return Status::OK();
}
```

---

## Summary: The Data Flow

```
USER CODE
│
├─→ InsertBatch("table", batch)
│  └─→ InsertBatchInternal()
│     ├─ Attach metadata (cf_id, batch_id, cf_name)
│     └─→ lsm_tree_->PutBatch(batch_with_metadata)
│        └─→ arrow_active_memtable_->PutBatch()
│           ✅ Data stored in memory
│
└─→ NewIterator("table", ...)
   └─→ LSMBatchIterator constructor
      ├─ Call: lsm_tree_->ScanSSTablesBatches(start_key, end_key)
      │  └─→ Scans arrow_active_memtable_->ScanBatches()
      │     └─ Returns batches (hopefully)
      │
      └─ Filter by metadata (cf_id must match)
         ├─ If metadata OK → batches_ has data → valid_ = true ✅
         └─ If metadata broken → batches_ empty → valid_ = false ❌
```

**The Problem**: LSMBatchIterator filters by metadata, but:
1. ScanSSTablesBatches might return empty (memtable scan broken)
2. OR metadata might not be preserved (AttachMetadata/RetrieveMetadata broken)
3. OR cf_id format might not match (string encoding mismatch)

**The Solution**: Debug which case it is, then fix accordingly.
