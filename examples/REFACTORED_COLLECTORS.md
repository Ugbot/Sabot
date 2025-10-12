# Refactored Exchange Data Collectors

**Clean architecture with shared infrastructure**

---

## New Structure

### Before (Monolithic)

```
examples/
├── coinbase2parquet.py  (800 lines - storage + WebSocket + parsing)
├── kraken2parquet.py    (750 lines - duplicated code)
└── binance2parquet.py   (850 lines - duplicated code)
```

**Problems**: Lots of code duplication, hard to maintain

### After (Modular)

```
examples/
├── exchange_common/           # Shared infrastructure
│   ├── __init__.py
│   ├── storage.py             # Kafka, Parquet, SQLite, JSON (300 lines)
│   └── websocket.py           # WebSocket client (200 lines)
│
├── exchange_configs/          # Small exchange-specific files
│   ├── __init__.py
│   ├── coinbase_config.py     # Coinbase: URL, schema, parsing (200 lines)
│   ├── kraken_config.py       # Kraken: URL, schema, parsing (180 lines)
│   └── binance_config.py      # Binance: URL, schema, parsing (190 lines)
│
└── exchange2storage.py        # Unified runner (150 lines)
```

**Benefits**: 
- ✅ No code duplication
- ✅ Easy to add new exchanges (just add config file)
- ✅ Easy to add new storage backends (modify storage.py)
- ✅ Clean separation of concerns

---

## Usage

### Same Commands, New Structure

```bash
# Coinbase → Kafka
python examples/exchange2storage.py coinbase -k

# Kraken → Parquet
python examples/exchange2storage.py kraken -F -o ./data/kraken.parquet

# Binance → SQLite
python examples/exchange2storage.py binance -S -o ./data/binance.db

# Read Parquet → Kafka
python examples/exchange2storage.py coinbase -FK -o ./data/coinbase.parquet
```

**Backwards compatible** - same flags, same behavior!

---

## Architecture

### Shared Infrastructure (`exchange_common/`)

**storage.py** - All storage backends:
```python
class KafkaStorage:
    """Kafka with JSON/Avro/Protobuf serialization."""
    
class ParquetStorage:
    """Parquet with Polars and row-group streaming."""
    
class JSONLinesStorage:
    """JSON Lines file storage."""
    
class SQLiteStorage:
    """SQLite database storage."""
    
class StorageManager:
    """Unified interface - write to multiple backends simultaneously."""
```

**websocket.py** - WebSocket infrastructure:
```python
class ExchangeWebSocket:
    """WebSocket client with automatic reconnection, exponential backoff."""
    
class MessageRouter:
    """Route messages to type-specific handlers."""
```

### Exchange Configs (`exchange_configs/`)

**Each config file provides**:
- `EXCHANGE_NAME`: "coinbase", "kraken", "binance"
- `WS_URL`: WebSocket endpoint
- `KAFKA_TOPIC`: Kafka topic name
- `PRODUCTS/SYMBOLS`: Products to subscribe to
- `POLARS_SCHEMA`: Schema for Parquet
- `AVRO_SCHEMA`: Schema for Avro
- `SQLITE_SCHEMA`: SQL for table creation
- `create_subscribe_message()`: Build subscription message
- `parse_message(frame)`: Parse WebSocket frame → records

**Example**: `coinbase_config.py`
```python
EXCHANGE_NAME = "coinbase"
WS_URL = "wss://advanced-trade-ws.coinbase.com"
PRODUCTS = ["BTC-USD", "ETH-USD", ...]

def create_subscribe_message():
    return json.dumps({
        "type": "subscribe",
        "product_ids": PRODUCTS,
        "channel": "ticker",
    })

def parse_message(frame):
    # Parse Coinbase format
    # Return list of ticker dicts
    ...
```

### Unified Runner (`exchange2storage.py`)

**Simple orchestration**:
1. Load exchange config (`import exchange_configs.{exchange}_config`)
2. Create storage backends based on mode
3. Create WebSocket client
4. Stream: `config.parse_message(frame)` → `storage.write(records)`

**~150 lines** vs 800+ in monolithic files!

---

## Adding New Exchange

### Step 1: Create Config File

```python
# exchange_configs/ftx_config.py

EXCHANGE_NAME = "ftx"
WS_URL = "wss://ftx.com/ws/"
KAFKA_TOPIC = "ftx-ticker"
PRODUCTS = ["BTC-PERP", "ETH-PERP", ...]

POLARS_SCHEMA = {
    "symbol": pl.String,
    "bid": pl.Float64,
    "ask": pl.Float64,
    # ... FTX-specific fields
}

def create_subscribe_message():
    return json.dumps({
        "op": "subscribe",
        "channel": "ticker",
        "market": "BTC-PERP"
    })

def parse_message(frame):
    msg = json.loads(frame)
    if msg.get('channel') == 'ticker':
        return [msg['data']]  # FTX format
    return None
```

### Step 2: Run It!

```bash
python examples/exchange2storage.py ftx -k
```

**That's it!** No need to duplicate storage/WebSocket code.

---

## Migration from Old Files

### Old Way

```bash
python examples/coinbase2parquet.py -k
python examples/kraken2parquet.py -k
python examples/binance2parquet.py -k
```

### New Way

```bash
python examples/exchange2storage.py coinbase -k
python examples/exchange2storage.py kraken -k
python examples/exchange2storage.py binance -k
```

**Same functionality, cleaner architecture!**

---

## Benefits

### 1. No Code Duplication

**Before**: 2400 lines across 3 files (80% duplicated)  
**After**: ~1200 lines total (all unique)

**Maintenance**: Fix bug once in `storage.py`, all exchanges benefit!

### 2. Easy to Extend

**Add new storage backend**:
```python
# In exchange_common/storage.py

class ClickHouseStorage:
    """ClickHouse database storage."""
    def write(self, record): ...

# Use with any exchange:
python exchange2storage.py coinbase --clickhouse
```

**Add new exchange**: Just 200-line config file!

### 3. Testable

```python
# Test storage independently
from exchange_common import KafkaStorage

storage = KafkaStorage(topic="test", format="json")
storage.write({"test": "data"})

# Test parsing independently
from exchange_configs import coinbase_config

records = coinbase_config.parse_message(sample_frame)
assert len(records) == 1
```

### 4. Composable

```python
# Write to multiple backends simultaneously
from exchange_common import StorageManager, KafkaStorage, ParquetStorage

mgr = StorageManager()
mgr.add_backend(KafkaStorage(...))
mgr.add_backend(ParquetStorage(...))

mgr.write(record)  # Goes to both Kafka AND Parquet!
```

---

## File Sizes

| File | Lines | Purpose |
|------|-------|---------|
| **exchange_common/storage.py** | ~300 | All storage backends |
| **exchange_common/websocket.py** | ~200 | WebSocket infrastructure |
| **exchange_configs/coinbase_config.py** | ~200 | Coinbase specifics |
| **exchange_configs/kraken_config.py** | ~180 | Kraken specifics |
| **exchange_configs/binance_config.py** | ~190 | Binance specifics |
| **exchange2storage.py** | ~150 | Unified runner |
| **Total** | ~1,220 | vs 2,400 before |

**50% code reduction!**

---

## Testing

### Test Individual Components

```bash
# Test Coinbase collector
python exchange2storage.py coinbase -k

# Test Kraken collector
python exchange2storage.py kraken -k

# Test Binance collector
python exchange2storage.py binance -k
```

### Test Storage Backends

```bash
# Test Parquet storage
python exchange2storage.py coinbase -F -o ./test.parquet
# Ctrl+C after a few seconds
python exchange2storage.py coinbase -PF -o ./test.parquet

# Test SQLite storage
python exchange2storage.py kraken -S -o ./test.db
sqlite3 ./test.db "SELECT COUNT(*) FROM kraken_ticker;"
```

### Test Multi-Backend

```python
# Modify exchange2storage.py to add multiple backends:
storage_mgr.add_backend(KafkaStorage(...))
storage_mgr.add_backend(ParquetStorage(...))
# Now writes to BOTH simultaneously!
```

---

## Next Steps

1. **Use new structure**:
   ```bash
   python exchange2storage.py coinbase -k &
   python exchange2storage.py kraken -k &
   python exchange2storage.py binance -k &
   ```

2. **Keep old files** (for compatibility):
   - Old files still work
   - Gradually migrate to new structure
   - Eventually deprecate old files

3. **Add more exchanges**:
   - Just create config file in `exchange_configs/`
   - ~200 lines per exchange
   - Reuse all infrastructure

4. **Extend functionality**:
   - Add new storage backends to `storage.py`
   - Improve WebSocket handling in `websocket.py`
   - All exchanges benefit automatically!

---

**Clean, maintainable, extensible architecture!** 🚀

