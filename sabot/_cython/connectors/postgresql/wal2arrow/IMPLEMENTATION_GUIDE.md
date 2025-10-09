# wal2arrow Implementation Guide

## Overview

This guide describes how to implement the wal2arrow PostgreSQL output plugin that converts logical decoding changes directly to Apache Arrow RecordBatches.

## Architecture Summary

```
PostgreSQL Logical Decoding
         ↓
  pg_decode_change() callback
         ↓
  Extract tuple data (Datum values)
         ↓
  Arrow RecordBatch Builder
    - Add row to batch
    - Type conversion (PG → Arrow)
    - Batching (accumulate rows)
         ↓
  Flush batch (when full or on commit)
         ↓
  Serialize to Arrow IPC Stream format
         ↓
  Output via OutputPluginWrite()
```

## File Structure

### Core Files

1. **wal2arrow.c** - Main plugin logic (~800 lines)
   - Plugin initialization (`_PG_output_plugin_init`)
   - Decoding callbacks
   - Batch management
   - IPC serialization

2. **arrow_builder.h/.cpp** - Arrow RecordBatch builder (~400 lines)
   - Schema definition
   - Row accumulation
   - Batch finalization

3. **type_mapping.h/.c** - PostgreSQL → Arrow type mapping (~300 lines)
   - Type conversion functions
   - Datum extraction

## Implementation Steps

### Step 1: Plugin Initialization

**File:** `wal2arrow.c`

```c
#include "postgres.h"
#include "replication/output_plugin.h"
#include "replication/logical.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"

#include <arrow/c/abi.h>
#include <arrow/c/bridge.h>

PG_MODULE_MAGIC;

// Plugin data structure
typedef struct {
    MemoryContext context;
    ArrowSchema schema;
    ArrowArray array;
    int batch_size;
    int row_count;
    bool include_xids;
    bool include_timestamp;
    // ... other options
} Wal2ArrowData;

extern void _PG_output_plugin_init(OutputPluginCallbacks *cb) {
    AssertVariableIsOfType(&_PG_output_plugin_init, LogicalOutputPluginInit);

    cb->startup_cb = wal2arrow_startup;
    cb->begin_cb = wal2arrow_begin_txn;
    cb->change_cb = wal2arrow_change;
    cb->commit_cb = wal2arrow_commit_txn;
    cb->shutdown_cb = wal2arrow_shutdown;
}
```

### Step 2: Decoding Callbacks

**Startup:**
```c
static void wal2arrow_startup(
    LogicalDecodingContext *ctx,
    OutputPluginOptions *opt,
    bool is_init
) {
    Wal2ArrowData *data = palloc0(sizeof(Wal2ArrowData));

    // Parse plugin options
    data->batch_size = get_plugin_option_int(opt, "batch-size", 100);
    data->include_xids = get_plugin_option_bool(opt, "include-xids", true);

    // Initialize Arrow schema
    init_arrow_schema(&data->schema);

    // Initialize Arrow array builders
    init_arrow_builders(&data->array, &data->schema);

    ctx->output_plugin_private = data;
}
```

**Change Callback:**
```c
static void wal2arrow_change(
    LogicalDecodingContext *ctx,
    ReorderBufferTXN *txn,
    Relation relation,
    ReorderBufferChange *change
) {
    Wal2ArrowData *data = ctx->output_plugin_private;

    // Extract change metadata
    const char *action = NULL;
    switch (change->action) {
        case REORDER_BUFFER_CHANGE_INSERT:
            action = "I";
            break;
        case REORDER_BUFFER_CHANGE_UPDATE:
            action = "U";
            break;
        case REORDER_BUFFER_CHANGE_DELETE:
            action = "D";
            break;
        default:
            return;
    }

    // Build Arrow row
    arrow_builder_add_row(
        &data->array,
        action,
        get_namespace_name(RelationGetNamespace(relation)),
        RelationGetRelationName(relation),
        txn->xid,
        txn->commit_time,
        change->data.tp.newtuple  // or oldtuple for DELETE
    );

    data->row_count++;

    // Flush batch if full
    if (data->row_count >= data->batch_size) {
        flush_arrow_batch(ctx, data);
    }
}
```

**Commit Callback:**
```c
static void wal2arrow_commit_txn(
    LogicalDecodingContext *ctx,
    ReorderBufferTXN *txn,
    XLogRecPtr commit_lsn
) {
    Wal2ArrowData *data = ctx->output_plugin_private;

    // Flush remaining rows
    if (data->row_count > 0) {
        flush_arrow_batch(ctx, data);
    }
}
```

### Step 3: Arrow RecordBatch Builder

**File:** `arrow_builder.cpp`

```cpp
#include "arrow_builder.h"
#include <arrow/api.h>
#include <arrow/ipc/api.h>
#include <arrow/c/bridge.h>

// Schema for CDC events
std::shared_ptr<arrow::Schema> create_cdc_schema() {
    return arrow::schema({
        arrow::field("action", arrow::utf8()),
        arrow::field("schema", arrow::utf8()),
        arrow::field("table", arrow::utf8()),
        arrow::field("lsn", arrow::int64()),
        arrow::field("timestamp", arrow::timestamp(arrow::TimeUnit::MICRO)),
        arrow::field("xid", arrow::int32()),
        arrow::field("columns", arrow::struct_({
            // Dynamic columns based on table schema
        }))
    });
}

// Add row to batch
void arrow_builder_add_row(
    ArrowArray *array,
    const char *action,
    const char *schema_name,
    const char *table_name,
    uint32_t xid,
    int64_t timestamp_us,
    HeapTuple tuple
) {
    // Append to Arrow array builders
    // This is simplified - real implementation would use
    // arrow::StringBuilder, arrow::Int64Builder, etc.

    // 1. action
    append_string(array, 0, action);

    // 2. schema
    append_string(array, 1, schema_name);

    // 3. table
    append_string(array, 2, table_name);

    // 4. xid
    append_int32(array, 5, xid);

    // 5. timestamp
    append_timestamp(array, 4, timestamp_us);

    // 6. columns (struct with table columns)
    append_tuple_struct(array, 6, tuple);
}

// Finalize batch
std::shared_ptr<arrow::RecordBatch> arrow_builder_finish(
    ArrowArray *array,
    ArrowSchema *schema
) {
    // Convert C Data Interface to C++ RecordBatch
    auto maybe_batch = arrow::ImportRecordBatch(array, schema);
    if (!maybe_batch.ok()) {
        // Error handling
        return nullptr;
    }
    return *maybe_batch;
}

// Serialize to IPC stream format
std::shared_ptr<arrow::Buffer> serialize_to_ipc(
    std::shared_ptr<arrow::RecordBatch> batch
) {
    auto output_stream = arrow::io::BufferOutputStream::Create().ValueOrDie();
    auto writer = arrow::ipc::MakeStreamWriter(output_stream, batch->schema()).ValueOrDie();

    writer->WriteRecordBatch(*batch).ok();
    writer->Close().ok();

    return output_stream->Finish().ValueOrDie();
}
```

### Step 4: Type Mapping

**File:** `type_mapping.c`

```c
#include "type_mapping.h"

// Map PostgreSQL type OID to Arrow type
ArrowType pg_type_to_arrow_type(Oid typid) {
    switch (typid) {
        case BOOLOID:
            return ARROW_BOOL;
        case INT2OID:
            return ARROW_INT16;
        case INT4OID:
            return ARROW_INT32;
        case INT8OID:
            return ARROW_INT64;
        case FLOAT4OID:
            return ARROW_FLOAT32;
        case FLOAT8OID:
            return ARROW_FLOAT64;
        case TEXTOID:
        case VARCHAROID:
            return ARROW_UTF8;
        case BYTEAOID:
            return ARROW_BINARY;
        case TIMESTAMPOID:
            return ARROW_TIMESTAMP_MICRO;
        case DATEOID:
            return ARROW_DATE32;
        case NUMERICOID:
            return ARROW_DECIMAL128;
        case UUIDOID:
            return ARROW_FIXED_SIZE_BINARY_16;
        default:
            // Unknown type - fallback to string
            return ARROW_UTF8;
    }
}

// Extract Datum value and convert to Arrow value
void datum_to_arrow_value(
    ArrowArray *array,
    int column_idx,
    Datum value,
    Oid typid,
    bool isnull
) {
    if (isnull) {
        arrow_array_append_null(array, column_idx);
        return;
    }

    switch (typid) {
        case BOOLOID:
            arrow_array_append_bool(array, column_idx, DatumGetBool(value));
            break;

        case INT2OID:
            arrow_array_append_int16(array, column_idx, DatumGetInt16(value));
            break;

        case INT4OID:
            arrow_array_append_int32(array, column_idx, DatumGetInt32(value));
            break;

        case INT8OID:
            arrow_array_append_int64(array, column_idx, DatumGetInt64(value));
            break;

        case TEXTOID:
        case VARCHAROID:
            arrow_array_append_string(array, column_idx, TextDatumGetCString(value));
            break;

        // ... other types

        default:
            // Convert to string as fallback
            char *str = OidOutputFunctionCall(typid, value);
            arrow_array_append_string(array, column_idx, str);
            pfree(str);
    }
}
```

### Step 5: IPC Serialization

**Flush batch to output:**
```c
static void flush_arrow_batch(
    LogicalDecodingContext *ctx,
    Wal2ArrowData *data
) {
    if (data->row_count == 0) {
        return;
    }

    // Finalize Arrow RecordBatch
    std::shared_ptr<arrow::RecordBatch> batch = arrow_builder_finish(
        &data->array,
        &data->schema
    );

    // Serialize to Arrow IPC Stream format
    std::shared_ptr<arrow::Buffer> buffer = serialize_to_ipc(batch);

    // Write to PostgreSQL output stream
    OutputPluginPrepareWrite(ctx, true);
    appendBinaryStringInfo(ctx->out, (char*)buffer->data(), buffer->size());
    OutputPluginWrite(ctx, true);

    // Reset builders for next batch
    arrow_builder_reset(&data->array);
    data->row_count = 0;
}
```

## Build Integration

The full C++ implementation requires:

1. **PostgreSQL headers** - `postgres.h`, `replication/output_plugin.h`
2. **Arrow C++ library** - `arrow/api.h`, `arrow/ipc/api.h`
3. **Arrow C Data Interface** - `arrow/c/abi.h`, `arrow/c/bridge.h`

All linked together via the Makefile we created.

## Testing

```sql
-- Setup
CREATE TABLE test_cdc (id INT PRIMARY KEY, name TEXT, value INT);

-- Create slot
SELECT pg_create_logical_replication_slot('test_slot', 'wal2arrow');

-- Make changes
INSERT INTO test_cdc VALUES (1, 'Alice', 100);
UPDATE test_cdc SET value = 200 WHERE id = 1;
DELETE FROM test_cdc WHERE id = 1;

-- Read changes (will be Arrow IPC format)
SELECT * FROM pg_logical_slot_get_binary_changes('test_slot', NULL, NULL);
```

## Next Steps

The actual C/C++ implementation of wal2arrow.c, arrow_builder.cpp, and type_mapping.c requires:

1. Detailed knowledge of PostgreSQL logical decoding API
2. Proficiency with Apache Arrow C++ API
3. Understanding of Arrow C Data Interface
4. Memory management (PostgreSQL palloc vs C++ new/delete)

This is a significant C++ development effort (~1500 lines of code total). The framework and structure are provided above - the detailed implementation can be completed by a C++ developer familiar with both PostgreSQL and Arrow.

For Sabot integration, the **Arrow-native CDC reader** (Phase 3) can be implemented immediately using the libpq wrapper we've created, and tested with the existing wal2json plugin until wal2arrow is complete.
