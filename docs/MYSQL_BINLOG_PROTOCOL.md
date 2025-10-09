# MySQL Binlog Protocol Implementation Notes

**Status:** Phase 1 complete (Python), Phase 2 planned (C++/Cython)

This document contains technical notes for implementing a MySQL binlog CDC connector from scratch in C++/Cython for Phase 2.

## Protocol Flow (From Scratch)

1. **TCP + TLS** to MySQL (port 3306)
2. **Handshake** (Protocol 10/41):
   - Read server handshake
   - Negotiate auth plugin (`caching_sha2_password` on 8.0; fallback to `mysql_native_password`)
   - Send auth response
   - **Easiest path:** Use TLS so `caching_sha2_password` works without RSA exchange complexity

3. **Set Session** (optional):
   - `SET @master_binlog_checksum='CRC32'` (not required—modern servers advertise checksum in FormatDescriptionEvent)

4. **Start Streaming**:
   - `COM_BINLOG_DUMP` (file+pos), or
   - `COM_BINLOG_DUMP_GTID` (preferred for failover)

5. **Event Loop**:
   - Parse EventHeader → dispatch by type:
     - `Format_description_event` → note checksum_alg
     - `Table_map_event` → cache table id → {db, table, column types, metadata, nullability}
     - `Write_rows_v2` / `Update_rows_v2` / `Delete_rows_v2` → decode using cached schema
     - `Query_event` (DDL), `GTID_EVENT`, `Xid_event` (txn commit), `HEARTBEAT_EVENT`

6. **Offsets**: Persist (log_file, log_pos) or GTID set as you progress

7. **Resume**: Reconnect → re-issue dump from last offset/GTIDs

## Wire Essentials

### Packet Framing (Client/Server Protocol)

- **Packet header (4 bytes):** 3-byte little-endian length + 1-byte sequence id
- **Payload:** command or reply
- **Commands:**
  - `0x03` COM_QUERY (if you need to run SQL, optional)
  - `0x12` COM_BINLOG_DUMP
  - `0x1e` COM_BINLOG_DUMP_GTID

### Binlog Event Header (Server → Client)

```
+----------------+----------+-------------------+
| timestamp (4)  | type (1) | server_id (4)     |
+----------------+----------+-------------------+
| event_size (4) | log_pos (4) | flags (2)     |
+----------------+--------------+---------------+
```

Followed by event-specific "post-header" then payload, and (optionally) CRC32.

**Checksum:** If Format_description_event says CRC32 is enabled, the last 4 bytes of every subsequent event are checksum—strip before parsing.

### Key Events

#### Format_description_event
- Version, header lengths, checksum algorithm
- Detect CRC32 enabled

#### Table_map_event
```
table_id(6) | flags(2) | db_name_len(1) + db_name | 0x00 |
table_name_len(1) + table_name | 0x00 | column_count (lenenc) |
column_types[column_count] | metadata_len (lenenc) + metadata |
null_bitmap (ceil(ncols/8))
```

Cache as `TableMap[table_id]`.

#### Write_rows_event_v2 / Update_rows_event_v2 / Delete_rows_event_v2
```
table_id(6) | flags(2) | extra_data_len(2)? + extra_data |
column_count (lenenc) | column_present_bitmap (ceil(ncols/8)) |
[for UPDATE: also "columns_present_before" bitmap] |
rows...  (values encoded column-by-column by type)
```

Type decoding uses MySQL type codes plus metadata from Table_map_event.

## Type Decoding

### MySQL Type Codes

```c
MYSQL_TYPE_DECIMAL=0, MYSQL_TYPE_TINY=1, MYSQL_TYPE_SHORT=2, MYSQL_TYPE_LONG=3,
MYSQL_TYPE_FLOAT=4, MYSQL_TYPE_DOUBLE=5, MYSQL_TYPE_NULL=6, MYSQL_TYPE_TIMESTAMP=7,
MYSQL_TYPE_LONGLONG=8, MYSQL_TYPE_INT24=9, MYSQL_TYPE_DATE=10, MYSQL_TYPE_TIME=11,
MYSQL_TYPE_DATETIME=12, MYSQL_TYPE_YEAR=13, MYSQL_TYPE_VARCHAR=15, MYSQL_TYPE_BIT=16,
MYSQL_TYPE_NEWDECIMAL=246, MYSQL_TYPE_ENUM=247, MYSQL_TYPE_SET=248,
MYSQL_TYPE_TINY_BLOB=249, MYSQL_TYPE_MEDIUM_BLOB=250, MYSQL_TYPE_LONG_BLOB=251,
MYSQL_TYPE_BLOB=252, MYSQL_TYPE_VAR_STRING=253, MYSQL_TYPE_STRING=254,
MYSQL_TYPE_GEOMETRY=255, MYSQL_TYPE_JSON=245,
MYSQL_TYPE_TIME2=0x0f, MYSQL_TYPE_DATETIME2=0x12, MYSQL_TYPE_TIMESTAMP2=0x11
```

### Decoding Examples

**MYSQL_TYPE_VARCHAR:**
```c
uint16_t maxlen = col_meta;  // 2-byte declared length
uint16_t len = (maxlen > 255) ? read_u16(cur) : read_u8(cur);
cur += (maxlen > 255) ? 2 : 1;
std::string value((const char*)cur, len);
cur += len;
```

**MYSQL_TYPE_BLOB:**
```c
uint8_t length_bytes = col_meta;  // 1, 2, 3, or 4
uint32_t len = read_uint(cur, length_bytes);
cur += length_bytes;
// Binary data: cur[0..len-1]
cur += len;
```

**MYSQL_TYPE_NEWDECIMAL:**
```c
// packed-decimal format
// precision = (col_meta >> 8), scale = (col_meta & 0xFF)
// TODO: decode to Decimal128
```

**MYSQL_TYPE_DATETIME2:**
```c
// 5 bytes for int part + fractional bytes (0..3) depending on fsp
// TODO: convert to microseconds since epoch
```

**MYSQL_TYPE_JSON:**
```c
// Binary JSON format (first 1/2/4 len then type-tagged tree)
// TODO: parse or pass-through as bytes
```

## Arrow Mapping

At the row decode point:

1. **Build Arrow Schema** from `TableMap.cols` (once per table id)
2. **Maintain `arrow::ArrayBuilders`** per column
3. **For each decoded row value:**
   - Append to matching builder (`AppendNull()` vs `Append(value)`)
   - Convert:
     - `NEWDECIMAL` → `Decimal128` (using precision/scale)
     - `DATETIME2/TIMESTAMP2` → `Timestamp(us)`
     - `JSON` → `Utf8` (if converting to text) or `Binary` (raw binjson)
4. **Flush RecordBatch** on size/time/txn boundaries
5. **Tag with position** (file, pos) or GTID for exactly-once semantics

## C++ Skeleton (Phase 2)

### Packet I/O

```cpp
class Socket {
public:
  explicit Socket(const std::string& host, uint16_t port);
  void connect();
  void sendAll(const uint8_t* buf, size_t n);
  void recvAll(uint8_t* buf, size_t n);
private:
  int fd{-1};
};

class PacketIO {
public:
  explicit PacketIO(Socket& s) : sock(s) {}
  Packet readPacket();
  void writePacket(const std::vector<uint8_t>& payload, uint8_t seq);
private:
  Socket& sock;
};
```

### Binlog Client

```cpp
class BinlogClient {
public:
  BinlogClient(const std::string& host, uint16_t port,
               const std::string& user, const std::string& pass);
  void runDump(const std::string& binlog_file, uint32_t pos);
  void runDumpGtid(const std::string& gtid_set);

private:
  void handshake();
  void auth_native41(const std::string& plugin, const std::string& salt);
  void send_cmd_binlog_dump(const std::string& file, uint32_t pos);
  void event_loop();

  void on_format_desc(const uint8_t* p, size_t n);
  void on_table_map(const uint8_t* p, size_t n);
  void on_write_rows_v2(const uint8_t* p, size_t n);
  // TODO: update/delete/gtid/xid/heartbeat…

  Socket sock;
  PacketIO io;
  bool crc32_enabled{false};
  SchemaCache cache;
};
```

### Table Map Cache

```cpp
struct ColumnMeta {
  uint8_t type;
  uint16_t meta;  // meaning depends on type
  bool nullable;
};

struct TableMap {
  uint64_t table_id;
  std::string db;
  std::string table;
  std::vector<ColumnMeta> cols;
};

struct SchemaCache {
  std::unordered_map<uint64_t, TableMap> byId;
};
```

## Performance Tips

1. **Zero-copy:** Keep a scratch buffer, parse in-place; only allocate for strings/blob/json when emitting
2. **TableMap cache:** Evict on Rotate_event rollovers if you need to bound memory; usually safe to keep active maps
3. **Parallel decode:** One network thread → pass event payloads to a worker pool (but preserve txn order if you output transactional batches)
4. **Backpressure:** If downstream (Arrow writer) is slower, buffer rows and periodically fsync your offset

## Gotchas

1. **Auth plugin:** MySQL 8.0 defaults to `caching_sha2_password`. Use TLS or create the CDC user with `IDENTIFIED WITH mysql_native_password BY '...'`
2. **Row image:** Ensure server `binlog_row_image=FULL`
3. **Retention:** Set `binlog_expire_logs_seconds` long enough to tolerate consumer downtime
4. **DDL:** On `Query_event` with DDL, invalidate/rebuild TableMap-derived Arrow schema
5. **Heartbeat:** `HEARTBEAT_EVENT` has no data—use to detect liveness
6. **Rotate_event:** Switch to next binlog file; update your state

## Implementation Plan (Phase 2)

### Step 1: Packet Layer (1 day)
- Socket wrapper with TLS support (OpenSSL)
- PacketIO class
- Unit tests for packet framing

### Step 2: Authentication (1 day)
- Handshake parsing
- `mysql_native_password` auth
- `caching_sha2_password` with TLS
- Unit tests for auth flow

### Step 3: Event Parsing (3 days)
- Format_description_event (CRC32 detection)
- Table_map_event (schema cache)
- Write_rows_v2, Update_rows_v2, Delete_rows_v2
- Unit tests with sample binlog events

### Step 4: Type Decoding (2 days)
- All MySQL types → Arrow types
- DECIMAL, DATETIME2, JSON parsing
- Unit tests for each type

### Step 5: Arrow Integration (2 days)
- Arrow schema generation from TableMap
- Arrow builders per column
- RecordBatch emission
- Zero-copy optimizations

### Step 6: Cython Wrapper (1 day)
- `.pxd` / `.pyx` files
- Python-friendly API
- Integration with MySQLCDCConnector

### Step 7: Testing & Benchmarking (2 days)
- Integration tests with live MySQL
- Performance benchmarks
- Verify 10-100x speedup

**Total: ~12 days (2 weeks)**

## References

- [MySQL Replication Protocol](https://dev.mysql.com/doc/internals/en/replication-protocol.html)
- [Binlog Event Format](https://dev.mysql.com/doc/internals/en/binlog-event.html)
- [python-mysql-replication](https://github.com/julien-duponchelle/python-mysql-replication) - Reference implementation
- [Arrow C Data Interface](https://arrow.apache.org/docs/format/CDataInterface.html)

## Current Status

**Phase 1 (Complete):**
- ✅ Python implementation using `python-mysql-replication`
- ✅ Arrow RecordBatch output
- ✅ Configuration and filtering
- ✅ GTID support
- ✅ Documentation and tests
- ✅ Demo example

**Phase 2 (Next):**
- ⏳ C++/Cython binlog parser
- ⏳ Zero-copy Arrow integration
- ⏳ 10-100x performance improvement

**Phase 3 (Future):**
- ⏳ Sabot checkpoint integration
- ⏳ Schema registry
- ⏳ Multi-table parallel streams
- ⏳ Production hardening
