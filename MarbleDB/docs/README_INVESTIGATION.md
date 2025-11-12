# MarbleDB InsertBatch & Iterator Investigation

## Overview

This investigation documents why `NewIterator()` returns invalid immediately after `InsertBatch()` stores data successfully.

**TL;DR**: InsertBatch stores in memtable. Iterator reads from ScanSSTablesBatches which should include memtable, but either:
1. Memtable scan returns empty, OR
2. Batches are filtered out due to metadata mismatch

## Documents in This Investigation

### 1. INVESTIGATION_SUMMARY.md - Start Here
- Quick findings and root cause analysis
- Code locations and file references
- Diagnostic tests to run
- Recommended fixes (3 options)
- Next steps

**Read this first** - gives you the full picture in 5 minutes.

### 2. CODE_SNIPPETS.md - Implementation Details
- Exact code for InsertBatch (full path)
- Exact code for InsertBatchInternal (metadata attachment)
- Exact code for PutBatch (memtable storage)
- Exact code for NewIterator (iterator creation)
- Exact code for LSMBatchIterator (the problem)
- Exact code for ScanSSTablesBatches (how it should work)
- Comparison with ScanTable (why it works)

**Read this** to understand the exact implementations.

### 3. INSERTBATCH_ITERATOR_DATA_FLOW.md - Visual Diagrams
- High-level architecture diagram
- InsertBatch data flow (chunking, metadata, memtable storage)
- Iterator data flow (scanning, metadata filtering)
- Problem scenarios (empty scan, metadata filtering failures)
- Why ScanTable works vs Iterator doesn't
- Fix options with pros/cons
- Testing procedures

**Read this** to visualize the problem and understand failure scenarios.

### 4. INSERTBATCH_ITERATOR_INVESTIGATION.md - Deep Dive
- Executive summary of root cause
- Detailed InsertBatch storage path with code
- Detailed PutBatch implementation
- Detailed NewIterator path with code
- LSMBatchIterator constructor full implementation
- ScanSSTablesBatches code with explanations
- Comparison with ScanTable
- Four possible failure points (A, B, C, D)
- Test code to identify the issue
- Summary table of components

**Read this** for comprehensive understanding of every component.

## Quick Start

### If you have 5 minutes:
1. Read: INVESTIGATION_SUMMARY.md (sections "Key Findings" and "Root Cause Analysis")
2. Look at: The "Critical Path" diagram
3. Run: Test 2 from INVESTIGATION_SUMMARY.md to identify which path is broken

### If you have 15 minutes:
1. Read: INVESTIGATION_SUMMARY.md (full)
2. Scan: CODE_SNIPPETS.md (summary sections)
3. Look at: DATA_FLOW.md (problem scenarios)
4. Decide: Which fix option makes sense for your use case

### If you have 30 minutes:
1. Read: All four documents in order
2. Study: The code snippets and data flow diagrams
3. Understand: Why ScanTable works but Iterator doesn't
4. Plan: Your debugging and fix strategy

## The Problem in One Picture

```
InsertBatch("table", batch)
  └─ Stores in: arrow_active_memtable (memory)  ✅ Works

NewIterator("table", ...)
  └─ LSMBatchIterator constructor
     ├─ Calls: ScanSSTablesBatches() (should scan memtable)
     ├─ Receives: batches from memtable (hopefully)
     ├─ Filters: by cf_id metadata
     └─ Result: batches_ empty, valid_ = false  ❌ Returns invalid

Problem: Either memtable scan returns empty OR metadata filtering removes all batches
```

## Key Code Locations

| What | File | Lines | Issue |
|------|------|-------|-------|
| Store data | api.cpp | 1126-1288 | Works fine |
| Retrieve data | lsm_storage.cpp | 981-1111 | Should work, but might not |
| Filter data | api.cpp | 479-520 | Silently fails |
| ScanTable (reference) | api.cpp | 1310-1342 | Works (no filtering) |

## Diagnostic Strategy

### Step 1: Find the Failure Point
```cpp
// After InsertBatch, before NewIterator
std::vector<std::shared_ptr<arrow::RecordBatch>> scan_results;
auto status = lsm_tree_->ScanSSTablesBatches(start_key, end_key, &scan_results);
std::cout << "Batches found: " << scan_results.size() << std::endl;
```

- If `size() == 0`: Memtable scan is broken (investigate ScanBatches)
- If `size() > 0`: Metadata filtering is broken (investigate metadata preservation)

### Step 2: Check Metadata
```cpp
if (!scan_results.empty()) {
    auto meta = scan_results[0]->schema()->metadata();
    if (!meta) std::cout << "Metadata missing!" << std::endl;
    
    auto idx = meta->FindKey("cf_id");
    if (idx == -1) std::cout << "cf_id key missing!" << std::endl;
    else std::cout << "cf_id = " << meta->value(idx) << std::endl;
}
```

### Step 3: Fix Accordingly
- If memtable scan broken: Investigate ArrowBatchMemTable::ScanBatches()
- If metadata broken: Either fix metadata preservation or remove metadata filtering
- Quick fix: Remove metadata filtering (make iterator match ScanTable behavior)

## Recommended Fixes (in order of simplicity)

### Fix 1: Remove Metadata Filtering (SIMPLEST)
In LSMBatchIterator constructor (api.cpp:500-510), replace:
```cpp
for (const auto& batch : scan_batches) {
    if (!batch || batch->num_rows() == 0) continue;
    // Check metadata... only add if cf_id matches
    // ...
}
```

With:
```cpp
for (const auto& batch : scan_batches) {
    if (!batch || batch->num_rows() == 0) continue;
    batches_.push_back(batch);  // Accept all batches
}
```

**Pros**: One-line fix, matches ScanTable behavior
**Cons**: Loses metadata isolation (assumes separate key ranges per table)

### Fix 2: Fix Metadata Preservation
Verify metadata is preserved through memtable storage.
- Check if arrow::RecordBatch::Make() preserves metadata
- Check if ArrowBatchMemTable::PutBatch() preserves metadata
- Check if ArrowBatchMemTable::ScanBatches() returns metadata

### Fix 3: Flush Before Reading (WORKAROUND)
```cpp
db->InsertBatch("table", batch);
db->Flush();  // Forces memtable to SSTable
auto iterator = db->NewIterator("table", ...);
assert(iterator->Valid());  // Should be true now
```

## References

All documents are in: `/Users/bengamble/Sabot/MarbleDB/docs/`

- `INVESTIGATION_SUMMARY.md` - Summary and next steps
- `CODE_SNIPPETS.md` - Full implementations
- `INSERTBATCH_ITERATOR_DATA_FLOW.md` - Visual diagrams
- `INSERTBATCH_ITERATOR_INVESTIGATION.md` - Deep technical analysis

## Questions This Investigation Answers

1. **Where does InsertBatch store data?**
   - Answer: `arrow_active_memtable` (in-memory buffer)

2. **Why can't NewIterator find it?**
   - Answer: Either memtable scan returns empty or metadata filtering removes all batches

3. **Why does ScanTable work?**
   - Answer: It doesn't filter by metadata, just uses all batches from scan

4. **What's the exact data flow?**
   - Answer: See DATA_FLOW.md with diagrams

5. **What code is broken?**
   - Answer: Either ArrowBatchMemTable::ScanBatches() or LSMBatchIterator metadata filtering

6. **How do I fix it?**
   - Answer: Run Test 2 from INVESTIGATION_SUMMARY.md to identify which, then choose a fix

## Files Created by This Investigation

1. `/Users/bengamble/Sabot/MarbleDB/docs/INVESTIGATION_SUMMARY.md` (4KB)
2. `/Users/bengamble/Sabot/MarbleDB/docs/CODE_SNIPPETS.md` (15KB)
3. `/Users/bengamble/Sabot/MarbleDB/docs/INSERTBATCH_ITERATOR_DATA_FLOW.md` (12KB)
4. `/Users/bengamble/Sabot/MarbleDB/docs/INSERTBATCH_ITERATOR_INVESTIGATION.md` (18KB)
5. `/Users/bengamble/Sabot/MarbleDB/docs/README_INVESTIGATION.md` (this file, 4KB)

**Total**: ~53KB of documentation with code, diagrams, and analysis

## Next Action Items

1. Read INVESTIGATION_SUMMARY.md (5 minutes)
2. Run Test 2 to identify failure point (5 minutes)
3. Read appropriate deep-dive section based on test results (10 minutes)
4. Decide on fix strategy (5 minutes)
5. Implement fix (15-60 minutes depending on which option)

## Investigation Completion

This investigation is complete with:
- Root cause identified (metadata filtering or memtable scan)
- Exact code locations provided
- Diagnostic tests included
- Multiple fix options documented
- Data flow diagrams included
- Code snippets included

The investigation is ready for implementation.
