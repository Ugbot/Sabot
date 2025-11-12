# MarbleDB InsertBatch vs Iterator Data Flow

## High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         SimpleMarbleDB                          │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  column_families_ (map: name → ColumnFamilyInfo)              │
│  - schema                                                        │
│  - cf_id                                                         │
│  - next_batch_id                                               │
│                                                                 │
│  StandardLSMTree (lsm_tree_)                                   │
│  ├─ arrow_active_memtable (in-memory, current)                │
│  ├─ arrow_immutable_memtables[] (in-memory, flushing)         │
│  └─ sstables_[][] (SSTable files on disk, by level)           │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

## InsertBatch Data Flow

```
User Code
   │
   ├─→ db->InsertBatch("table_name", batch)
   │
   └─→ SimpleMarbleDB::InsertBatch()
       │
       ├─ Chunk if large (>5000 rows)
       │
       └─→ InsertBatchInternal() for each chunk
           │
           ├─ Find ColumnFamilyInfo for table_name
           ├─ Assign batch_id = cf_info->next_batch_id++
           │
           ├─ Add metadata to RecordBatch:
           │  ├─ "cf_id" = std::to_string(cf_info->id)
           │  ├─ "batch_id" = std::to_string(batch_id)
           │  └─ "cf_name" = table_name
           │
           └─→ lsm_tree_->PutBatch(batch_with_metadata)
               │
               └─→ StandardLSMTree::PutBatch()
                   │
                   ├─ Get arrow_active_memtable (create if null)
                   │
                   └─→ arrow_active_memtable_->PutBatch(batch)
                       │
                       ✅ DATA NOW IN MEMORY

                       Check if memtable should flush (size > threshold)
                       │
                       └─→ If full: SwitchBatchMemTable()
                           ├─ Move to arrow_immutable_memtables[]
                           ├─ Create new active memtable
                           └─→ Trigger background flush (writes to SSTables)
```

## Iterator Data Flow

```
User Code
   │
   ├─→ db->NewIterator("table_name", options, range, &iterator)
   │
   └─→ SimpleMarbleDB::NewIterator()
       │
       ├─ Find ColumnFamilyInfo for table_name
       │
       └─→ new LSMBatchIterator(lsm_tree, cf_info->id, cf_info->schema)
           │
           ├─ Encode key range:
           │  ├─ start_key = EncodeBatchKey(table_id, 0)
           │  └─ end_key = EncodeBatchKey(table_id + 1, 0)
           │
           └─→ lsm_tree_->ScanSSTablesBatches(start_key, end_key, &scan_batches)
               │
               └─→ StandardLSMTree::ScanSSTablesBatches()
                   │
                   ├─ PHASE 0: Scan memtables
                   │  ├─ if (arrow_active_memtable_)
                   │  │  └─→ arrow_active_memtable_->ScanBatches(start_key, end_key)
                   │  │      └─→ Returns [] if empty or filtered
                   │  │
                   │  └─ for (auto& immutable in arrow_immutable_memtables_)
                   │     └─→ immutable->ScanBatches(start_key, end_key)
                   │
                   └─ PHASE 1: Scan SSTables
                      └─→ For each level 0..max:
                          └─→ For each sstable in level:
                              └─→ sstable->ScanBatches(start_key, end_key)
                   
                   ✅ Returns vector of RecordBatches from memtables + SSTables
           
           Back in LSMBatchIterator constructor:
           │
           └─→ Filter batches by metadata
               │
               ├─ for (const auto& batch : scan_batches) {
               │  ├─ Get batch->schema()->metadata()
               │  ├─ Find "cf_id" in metadata
               │  ├─ If cf_id == expected_cf_id:
               │  │  └─→ batches_.push_back(batch)  ✅
               │  └─ Else:
               │     └─→ Skip batch silently ❌
               │
               └─ if (!batches_.empty())
                  ├─ valid_ = true
                  └─ Position at first row
                  
                  else
                  └─ valid_ = false  ❌ Returns invalid immediately!
```

## The Problem: Why Iterator Returns Invalid

### Scenario 1: Memtable Scan Returns Empty

```
InsertBatch("triples", batch)  ✅ Stores in arrow_active_memtable_

Later:
NewIterator()
  ├─ ScanSSTablesBatches(start_key, end_key)
  │  └─ arrow_active_memtable_->ScanBatches(start_key, end_key)
  │     └─ Returns [] (empty!)  ❌
  │
  └─ LSMBatchIterator constructor
     └─ scan_batches is empty
     └─ batches_ is empty
     └─ valid_ = false  ❌
```

**Why memtable might return empty:**
1. Key range doesn't match (EncodeBatchKey mismatch)
2. Memtable's ScanBatches implementation is broken
3. Batches were filtered out before returning

### Scenario 2: Memtable Scan Returns Data But Filtered Out

```
InsertBatch("triples", batch)
  └─ batch metadata: cf_id="1", batch_id="0", cf_name="triples"
  └─ Stores in arrow_active_memtable_  ✅

Later:
NewIterator()
  ├─ ScanSSTablesBatches(start_key, end_key)
  │  └─ arrow_active_memtable_->ScanBatches(start_key, end_key)
  │     └─ Returns [batch with cf_id="1"] ✅
  │
  └─ LSMBatchIterator constructor
     │
     ├─ expected_cf_id = "1"
     │
     ├─ for (batch : scan_batches) {
     │  ├─ metadata = batch->schema()->metadata()
     │  ├─ cf_id_index = metadata->FindKey("cf_id")
     │  │
     │  ├─ If cf_id_index == -1:
     │  │  └─ Metadata missing!  ❌ Skip batch
     │  │
     │  └─ If metadata->value(cf_id_index) != "1":
     │     └─ cf_id mismatch!  ❌ Skip batch
     │
     └─ batches_ is empty
     └─ valid_ = false  ❌
```

**Why metadata filtering might fail:**
1. Metadata not properly attached by InsertBatchInternal
2. Metadata lost during memtable storage/retrieval
3. cf_id string format mismatch (e.g., "1" vs 1)

---

## Key Difference: Why ScanTable Works

```
ScanTable("triples")
│
└─ ScanSSTablesBatches(start_key, end_key)
   │
   └─ arrow_active_memtable_->ScanBatches(start_key, end_key)
      └─ Returns [batch]  ✅
   
   └─ Returns [batch]  ✅

└─ Does NOT filter by cf_id metadata
   └─ Directly creates TableQueryResult([batch])
   └─ Returns [batch]  ✅ Works!


NewIterator("triples")
│
└─ ScanSSTablesBatches(start_key, end_key)
   │
   └─ arrow_active_memtable_->ScanBatches(start_key, end_key)
      └─ Returns [batch]  ✅
   
   └─ Returns [batch]  ✅

└─ FILTERS by cf_id metadata  ❌
   ├─ If cf_id doesn't match → skip
   └─ Result: empty batches, invalid iterator
```

---

## The Fix Options

### Option A: Remove Metadata Filtering
```cpp
// In LSMBatchIterator constructor, instead of filtering:

for (const auto& batch : scan_batches) {
    if (!batch || batch->num_rows() == 0) continue;
    batches_.push_back(batch);  // ← Accept all batches
}
```

**Pros**: Simple, matches what ScanTable does
**Cons**: Loses table isolation if multiple tables stored together

### Option B: Fix Metadata Attachment
Ensure InsertBatchInternal properly sets metadata so filtering works.

**Current code**:
```cpp
auto new_metadata = std::make_shared<arrow::KeyValueMetadata>();
new_metadata->Append("cf_id", std::to_string(cf_info->id));
new_metadata->Append("batch_id", std::to_string(batch_id));
new_metadata->Append("cf_name", table_name);

auto schema_with_metadata = batch->schema()->WithMetadata(new_metadata);
auto batch_with_metadata = arrow::RecordBatch::Make(
    schema_with_metadata,
    batch->num_rows(),
    batch->columns());

auto put_status = lsm_tree_->PutBatch(batch_with_metadata);
```

**Verify**: Does batch_with_metadata preserve metadata through memtable storage?

### Option C: Fix Memtable Key Range
Ensure EncodeBatchKey produces consistent values:
- Encoding: `EncodeBatchKey(table_id, batch_id)`
- Decoding: `start_key = EncodeBatchKey(table_id, 0)` to `end_key = EncodeBatchKey(table_id + 1, 0)`

This determines whether ScanBatches includes the data.

### Option D: Workaround - Flush Before Reading
```cpp
db->InsertBatch("table", batch);
db->Flush();  // Force memtable → SSTables
auto iterator = db->NewIterator("table", ...);
// Now iterator will find data in SSTables
```

---

## Testing the Data Flow

### Test 1: Verify Memtable Storage
```cpp
// After InsertBatch
auto mt = lsm_tree_->arrow_active_memtable_.get();
if (mt) {
    std::cout << "Memtable has data" << std::endl;
}
```

### Test 2: Verify Memtable Scan
```cpp
// After InsertBatch
std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
uint64_t start = EncodeBatchKey(table_id, 0);
uint64_t end = EncodeBatchKey(table_id + 1, 0);
auto status = lsm_tree_->ScanSSTablesBatches(start, end, &batches);
std::cout << "Found " << batches.size() << " batches" << std::endl;

if (!batches.empty()) {
    auto meta = batches[0]->schema()->metadata();
    auto idx = meta->FindKey("cf_id");
    std::cout << "cf_id = " << (idx != -1 ? meta->value(idx) : "NOT FOUND") << std::endl;
}
```

### Test 3: Verify Iterator Creation
```cpp
std::unique_ptr<Iterator> iter;
db->NewIterator("table_name", options, range, &iter);
std::cout << "Iterator valid: " << iter->Valid() << std::endl;
```

### Test 4: Verify Flush Works Around
```cpp
db->InsertBatch("table", batch);
db->Flush();

std::unique_ptr<Iterator> iter;
db->NewIterator("table_name", options, range, &iter);
std::cout << "Iterator valid after flush: " << iter->Valid() << std::endl;
```

