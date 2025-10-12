#!/usr/bin/env python3
"""
Stream Kraken Spot WebSocket v2 tickers through asyncio websocket.
Depending on flags, either pipe raw JSON to Kafka, append to a Parquet file,
or read from the Parquet file to Kafka.

Modes:
  -k   : Stream Kraken → Kafka (default)
  -F   : Stream Kraken → Parquet file (row-group streaming)
  -FK  : Read Parquet file → Kafka
  -J   : Stream Kraken → JSON Lines file
  -JK  : Read JSON Lines file → Kafka
  -S   : Stream Kraken → SQLite database
  -SK  : Read SQLite database → Kafka
  -PF  : Print Parquet path + first 100 rows, then exit

Serialization formats for Kafka:
  --format json      : JSON serialization (default)
  --format avro      : Avro serialization with Schema Registry
  --format protobuf  : Protobuf serialization with Schema Registry

Based on Kraken WebSocket v2 API:
https://docs.kraken.com/api/docs/guides/spot-ws-intro
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import ssl
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, NoReturn, Optional
from decimal import Decimal

import certifi
import polars as pl
import websockets
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.schema_registry.protobuf import ProtobufSerializer
from confluent_kafka.serialization import SerializationContext, MessageField
from pydantic import BaseModel, Field, field_validator
import sqlite3

# ───────────────────────────────────────── Configuration ──
# Kraken WebSocket v2 endpoints
WS_URL = "wss://ws.kraken.com/v2"
WS_AUTH_URL = "wss://ws-auth.kraken.com/v2"

KAFKA_TOPIC = "kraken-ticker"
PARQUET_TOPIC = "kraken-ticker"
PARQUET_BATCH_SIZE = 50  # rows per row-group

# Default filenames
DEFAULT_PARQUET_FILE = Path("./kraken_ticker_data.parquet")
DEFAULT_JSONL_FILE = Path("./kraken_ticker_data.jsonl")
DEFAULT_SQLITE_FILE = Path("./kraken_ticker_data.db")
DEFAULT_SCHEMA_REGISTRY_URL = "http://localhost:18081"

# Kraken uses BTC/USD format (slash separator)
PRODUCT_IDS = [
    "BTC/USD", "ETH/USD", "XRP/USD", "LTC/USD",
    "BCH/USD", "ADA/USD", "SOL/USD", "DOT/USD",
    "LINK/USD", "UNI/USD", "ALGO/USD", "MATIC/USD",
    "DOGE/USD", "XLM/USD",
]

# ─────────────────────────────────── Pydantic models ──
class KrakenTicker(BaseModel):
    """
    Kraken ticker data model (WebSocket v2 format).
    
    Based on: https://docs.kraken.com/api/docs/websocket-v2/ticker
    """
    symbol: str  # e.g., "BTC/USD"
    ask: float
    ask_qty: float
    bid: float
    bid_qty: float
    last: float  # Last trade price
    volume: float  # 24h volume
    vwap: float  # 24h VWAP
    low: float  # 24h low
    high: float  # 24h high
    change: float  # 24h price change
    change_pct: float  # 24h price change %
    
    # Metadata
    timestamp: str  # RFC3339 format
    channel: str = "ticker"
    type: str = "update"
    
    @field_validator(
        "ask", "ask_qty", "bid", "bid_qty", "last",
        "volume", "vwap", "low", "high", "change", "change_pct",
        mode="before"
    )
    def _num(cls, v):
        if v in (None, ""):
            return None
        try:
            return float(v)
        except (TypeError, ValueError):
            logging.warning("Could not convert %r to float; setting to None", v)
            return None


class KrakenTickerUpdate(BaseModel):
    """Wrapper for ticker updates."""
    channel: str
    type: str
    data: List[KrakenTicker]
    timestamp: Optional[str] = None


# ─────────────────────────────────────────────── Avro schema ──
AVRO_SCHEMA = {
    "type": "record",
    "name": "KrakenTicker",
    "namespace": "com.kraken.ticker",
    "fields": [
        {"name": "symbol", "type": "string"},
        {"name": "ask", "type": "double"},
        {"name": "ask_qty", "type": "double"},
        {"name": "bid", "type": "double"},
        {"name": "bid_qty", "type": "double"},
        {"name": "last", "type": "double"},
        {"name": "volume", "type": "double"},
        {"name": "vwap", "type": "double"},
        {"name": "low", "type": "double"},
        {"name": "high", "type": "double"},
        {"name": "change", "type": "double"},
        {"name": "change_pct", "type": "double"},
        {"name": "timestamp", "type": "string"},
        {"name": "channel", "type": "string"},
        {"name": "type", "type": "string"},
    ]
}

# ─────────────────────────────────────────────── Polars schema ──
POLARS_SCHEMA = {
    "symbol": pl.String,
    "ask": pl.Float64,
    "ask_qty": pl.Float64,
    "bid": pl.Float64,
    "bid_qty": pl.Float64,
    "last": pl.Float64,
    "volume": pl.Float64,
    "vwap": pl.Float64,
    "low": pl.Float64,
    "high": pl.Float64,
    "change": pl.Float64,
    "change_pct": pl.Float64,
    "timestamp": pl.String,
    "channel": pl.String,
    "type": pl.String,
}

# ────────────────────────────────────────── SQLite setup ──
_SQLITE_TABLE_NAME = "kraken_ticker"
_SQLITE_SCHEMA = f"""
CREATE TABLE IF NOT EXISTS {_SQLITE_TABLE_NAME} (
    symbol TEXT,
    ask REAL,
    ask_qty REAL,
    bid REAL,
    bid_qty REAL,
    last REAL,
    volume REAL,
    vwap REAL,
    low REAL,
    high REAL,
    change REAL,
    change_pct REAL,
    timestamp TEXT,
    channel TEXT,
    type TEXT,
    id INTEGER PRIMARY KEY AUTOINCREMENT
);
"""

_SQLITE_INSERT_SQL = f"""INSERT INTO {_SQLITE_TABLE_NAME} 
(symbol, ask, ask_qty, bid, bid_qty, last, volume, vwap, low, high, 
 change, change_pct, timestamp, channel, type) 
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""


def _get_sqlite_conn(db_path: Path) -> sqlite3.Connection:
    """Get SQLite connection and create table if needed."""
    conn = sqlite3.connect(db_path, check_same_thread=False)
    try:
        conn.execute(_SQLITE_SCHEMA)
        conn.commit()
        logging.debug("Ensured table %s exists in %s", _SQLITE_TABLE_NAME, db_path)
    except sqlite3.Error as e:
        logging.error("SQLite error during table creation: %s", e)
        conn.close()
        raise
    return conn


# Global configuration
_output_file_path: Path | None = None
_serialization_format: str = "json"
_schema_registry_client: SchemaRegistryClient | None = None
_avro_serializer: AvroSerializer | None = None
_protobuf_serializer: ProtobufSerializer | None = None

# ───────────────────────────────────────── SSL + logging ──
ssl_ctx = ssl.create_default_context(cafile=certifi.where())
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

# ───────────────────────────────────────── WebSocket subscription ──
def create_subscribe_message() -> str:
    """
    Create Kraken WebSocket v2 subscription message.
    
    Format: https://docs.kraken.com/api/docs/websocket-v2/ticker
    """
    return json.dumps({
        "method": "subscribe",
        "params": {
            "channel": "ticker",
            "symbol": PRODUCT_IDS,
        }
    })


# ───────────────────────────────────────── Serialization helpers ──
def _init_schema_registry(url: str) -> None:
    """Initialize Schema Registry client and serializers."""
    global _schema_registry_client, _avro_serializer, _protobuf_serializer
    
    if _schema_registry_client is None:
        logging.info("Connecting to Schema Registry at %s", url)
        _schema_registry_client = SchemaRegistryClient({"url": url})
    
    if _serialization_format == "avro" and _avro_serializer is None:
        logging.info("Initializing Avro serializer")
        _avro_serializer = AvroSerializer(
            _schema_registry_client,
            json.dumps(AVRO_SCHEMA),
            lambda obj, ctx: obj
        )
    
    if _serialization_format == "protobuf" and _protobuf_serializer is None:
        logging.info("Initializing Protobuf serializer")
        try:
            import kraken_ticker_pb2
            _protobuf_serializer = ProtobufSerializer(
                kraken_ticker_pb2.KrakenTicker,
                _schema_registry_client,
                conf={"use.deprecated.format": False}
            )
        except ImportError:
            logging.error("Protobuf module not found. Run: protoc --python_out=. kraken_ticker.proto")
            raise


def _serialize_message(data: dict) -> bytes:
    """Serialize message based on configured format."""
    if _serialization_format == "json":
        return json.dumps(data).encode()
    elif _serialization_format == "avro":
        if _avro_serializer is None:
            raise RuntimeError("Avro serializer not initialized")
        ctx = SerializationContext(KAFKA_TOPIC, MessageField.VALUE)
        return _avro_serializer(data, ctx)
    elif _serialization_format == "protobuf":
        if _protobuf_serializer is None:
            raise RuntimeError("Protobuf serializer not initialized")
        try:
            import kraken_ticker_pb2
            msg = kraken_ticker_pb2.KrakenTicker(**data)
            ctx = SerializationContext(KAFKA_TOPIC, MessageField.VALUE)
            return _protobuf_serializer(msg, ctx)
        except ImportError:
            logging.error("Protobuf module not found")
            raise
    else:
        raise ValueError(f"Unknown serialization format: {_serialization_format}")


# ───────────────────────────────────────── Kafka producer ──
producer: Producer | None = None


def _get_producer() -> Producer:
    global producer
    if producer is None:
        logging.info("Initializing Kafka producer...")
        producer = Producer({"bootstrap.servers": "localhost:19092"})
    return producer


# ────────────────────────────────────────── Mode 1: Kraken → Kafka ──
async def stream_to_kafka() -> None:
    """Stream Kraken WebSocket data to Kafka."""
    kafka = _get_producer()
    
    async with websockets.connect(WS_URL, ssl=ssl_ctx, ping_interval=20) as ws:
        # Subscribe to ticker channel
        subscribe_msg = create_subscribe_message()
        await ws.send(subscribe_msg)
        logging.info("Subscribed to Kraken ticker; streaming to Kafka topic '%s' (format: %s)", 
                    KAFKA_TOPIC, _serialization_format)
        
        async for frame in ws:
            try:
                msg = json.loads(frame)
                
                # Handle subscription confirmation
                if msg.get("method") == "subscribe" and msg.get("success"):
                    logging.info("✅ Subscription confirmed: %s", msg.get("result"))
                    continue
                
                # Handle heartbeat
                if msg.get("channel") == "heartbeat":
                    logging.debug("💓 Heartbeat received")
                    continue
                
                # Handle ticker updates
                if msg.get("channel") == "ticker" and msg.get("type") == "update":
                    if _serialization_format == "json":
                        # Send raw frame
                        kafka.produce(KAFKA_TOPIC, value=frame.encode())
                    else:
                        # Parse and serialize
                        data_list = msg.get("data", [])
                        for ticker_data in data_list:
                            try:
                                # Add metadata
                                ticker_data['timestamp'] = msg.get('timestamp', ticker_data.get('timestamp', ''))
                                ticker_data['channel'] = 'ticker'
                                ticker_data['type'] = 'update'
                                
                                serialized = _serialize_message(ticker_data)
                                kafka.produce(KAFKA_TOPIC, value=serialized)
                            except Exception as e:
                                logging.error("Serialization error: %s", e)
                                continue
                    
                    kafka.poll(0)
            
            except json.JSONDecodeError as e:
                logging.warning("JSON decode error: %s", e)
                continue
            except Exception as e:
                logging.error("Error processing frame: %s", e)
                continue
        
        kafka.flush()


# ────────────────────────────────────────── Mode 2: Kraken → Parquet ──
async def stream_to_parquet() -> None:
    """Stream Kraken WebSocket data to Parquet file."""
    batch: list[KrakenTicker] = []
    last_write = time.monotonic()
    first_write = True
    
    if _output_file_path.exists():
        logging.warning("%s already exists and will be **overwritten**", _output_file_path)
    
    async with websockets.connect(WS_URL, ssl=ssl_ctx, ping_interval=20) as ws:
        subscribe_msg = create_subscribe_message()
        await ws.send(subscribe_msg)
        logging.info("Subscribed to Kraken ticker; streaming to Parquet file %s", _output_file_path)
        
        try:
            async for frame in ws:
                try:
                    msg = json.loads(frame)
                    
                    # Skip non-ticker messages
                    if msg.get("channel") != "ticker" or msg.get("type") != "update":
                        continue
                    
                    # Process ticker data
                    data_list = msg.get("data", [])
                    parent_timestamp = msg.get('timestamp', '')
                    
                    for ticker_data in data_list:
                        try:
                            # Add parent timestamp if not present
                            if 'timestamp' not in ticker_data and parent_timestamp:
                                ticker_data['timestamp'] = parent_timestamp
                            
                            ticker_data['channel'] = 'ticker'
                            ticker_data['type'] = 'update'
                            
                            ticker = KrakenTicker.model_validate(ticker_data)
                            batch.append(ticker)
                        except Exception as err:
                            logging.debug("Ticker parse error: %s", err)
                            continue
                    
                    now = time.monotonic()
                    need_flush = len(batch) >= PARQUET_BATCH_SIZE or (batch and now - last_write > 5)
                    
                    if need_flush:
                        _flush_batch_polars(batch, _output_file_path, first_write)
                        first_write = False
                        last_write = now
                
                except json.JSONDecodeError:
                    continue
                except Exception as e:
                    logging.error("Error processing message: %s", e)
                    continue
        
        finally:
            if batch:
                _flush_batch_polars(batch, _output_file_path, first_write)
            logging.info("Closed Parquet file %s", _output_file_path)


def _flush_batch_polars(batch: list[KrakenTicker], file_path: Path, first_write: bool) -> None:
    """Write the current batch using Polars and clear it."""
    if not batch:
        return
    
    records = [t.model_dump() for t in batch]
    batch.clear()
    
    # Create Polars DataFrame
    df = pl.DataFrame(records, schema=POLARS_SCHEMA)
    
    # Write or append to Parquet
    if first_write:
        df.write_parquet(file_path, compression="snappy", use_pyarrow=False)
        logging.info("Created %s with %d rows", file_path, len(df))
    else:
        # Append mode
        existing_df = pl.read_parquet(file_path)
        combined_df = pl.concat([existing_df, df])
        combined_df.write_parquet(file_path, compression="snappy", use_pyarrow=False)
        logging.info("Appended %d rows (total: %d rows, %.1f MiB)",
                    len(df), len(combined_df),
                    file_path.stat().st_size / 2**20 if file_path.exists() else 0)


# ────────────────────────────────────────── Mode 3: Kraken → JSON Lines ──
async def stream_to_json() -> None:
    """Stream Kraken WebSocket data to JSON Lines file."""
    if _output_file_path.exists():
        logging.warning("%s already exists and will be **overwritten**", _output_file_path)
    
    try:
        with open(_output_file_path, "a+", encoding="utf-8", buffering=1) as f:
            logging.info("Opened %s for writing JSON lines", _output_file_path)
            
            async with websockets.connect(WS_URL, ssl=ssl_ctx, ping_interval=20) as ws:
                subscribe_msg = create_subscribe_message()
                await ws.send(subscribe_msg)
                logging.info("Subscribed to Kraken ticker; streaming to %s", _output_file_path)
                
                async for frame in ws:
                    try:
                        msg = json.loads(frame)
                        
                        if msg.get("channel") != "ticker" or msg.get("type") != "update":
                            continue
                        
                        data_list = msg.get("data", [])
                        parent_timestamp = msg.get('timestamp', '')
                        
                        for ticker_data in data_list:
                            ticker_data['timestamp'] = ticker_data.get('timestamp', parent_timestamp)
                            ticker_data['channel'] = 'ticker'
                            ticker_data['type'] = 'update'
                            
                            # Write as JSON line
                            f.write(json.dumps(ticker_data) + '\n')
                    
                    except json.JSONDecodeError:
                        continue
                    except Exception as e:
                        logging.error("Error writing JSON line: %s", e)
                        continue
    
    except OSError as e:
        logging.error("Error opening or writing to %s: %s", _output_file_path, e)
    finally:
        logging.info("Stopped writing JSON Lines to %s", _output_file_path)


# ────────────────────────────────────────── Mode 4: Parquet → Kafka ──
async def read_parquet_to_kafka() -> None:
    """Read Parquet file and produce to Kafka."""
    if not _output_file_path.exists() or not _output_file_path.is_file():
        logging.error("Parquet file %s not found", _output_file_path)
        return
    
    kafka = _get_producer()
    df = pl.read_parquet(_output_file_path)
    logging.info("Read %d rows; producing to Kafka (format: %s)...", len(df), _serialization_format)
    
    produced = 0
    for record in df.iter_rows(named=True):
        record_clean = {k: (None if v is None else v) for k, v in record.items()}
        
        try:
            serialized = _serialize_message(record_clean)
            kafka.produce(KAFKA_TOPIC, value=serialized)
            produced += 1
            
            if produced % 1000 == 0:
                kafka.poll(0)
                logging.info("Produced %d records...", produced)
        except Exception as e:
            logging.error("Error producing record: %s", e)
            continue
    
    kafka.flush()
    logging.info("Finished sending %d records", produced)


# ────────────────────────────────────────── Mode 5: JSON Lines → Kafka ──
async def read_json_to_kafka() -> None:
    """Read JSON Lines file and produce to Kafka."""
    if not _output_file_path.exists() or not _output_file_path.is_file():
        logging.error("JSON file %s not found", _output_file_path)
        return
    
    kafka = _get_producer()
    logging.info("Reading from %s and producing to Kafka topic '%s'...", 
                _output_file_path, KAFKA_TOPIC)
    
    produced = 0
    try:
        with open(_output_file_path, "r", encoding="utf-8") as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                
                try:
                    kafka.produce(KAFKA_TOPIC, value=line.encode())
                    produced += 1
                    
                    if produced % 1000 == 0:
                        kafka.poll(0)
                        logging.info("Produced %d records...", produced)
                except Exception as e:
                    logging.error("Error producing line %d: %s", line_num, e)
                    continue
    
    except OSError as e:
        logging.error("Error reading file %s: %s", _output_file_path, e)
        return
    
    kafka.flush()
    logging.info("Finished sending %d records from %s", produced, _output_file_path)


# ────────────────────────────────────────── Mode 6: Kraken → SQLite ──
async def stream_to_sqlite() -> None:
    """Stream Kraken WebSocket data to SQLite database."""
    batch: list[tuple] = []
    conn: sqlite3.Connection | None = None
    cursor: sqlite3.Cursor | None = None
    last_commit = time.monotonic()
    
    try:
        conn = _get_sqlite_conn(_output_file_path)
        cursor = conn.cursor()
        logging.info("Opened SQLite DB %s and ensured table '%s' exists", 
                    _output_file_path, _SQLITE_TABLE_NAME)
        
        async with websockets.connect(WS_URL, ssl=ssl_ctx, ping_interval=20) as ws:
            subscribe_msg = create_subscribe_message()
            await ws.send(subscribe_msg)
            logging.info("Subscribed to Kraken ticker; streaming to %s", _output_file_path)
            
            async for frame in ws:
                try:
                    msg = json.loads(frame)
                    
                    if msg.get("channel") != "ticker" or msg.get("type") != "update":
                        continue
                    
                    data_list = msg.get("data", [])
                    parent_timestamp = msg.get('timestamp', '')
                    
                    for ticker_data in data_list:
                        try:
                            ticker = KrakenTicker.model_validate(ticker_data)
                            ticker.timestamp = ticker.timestamp or parent_timestamp
                            
                            # Convert to tuple for insertion
                            record_tuple = (
                                ticker.symbol,
                                ticker.ask,
                                ticker.ask_qty,
                                ticker.bid,
                                ticker.bid_qty,
                                ticker.last,
                                ticker.volume,
                                ticker.vwap,
                                ticker.low,
                                ticker.high,
                                ticker.change,
                                ticker.change_pct,
                                ticker.timestamp,
                                ticker.channel,
                                ticker.type,
                            )
                            batch.append(record_tuple)
                        except Exception as err:
                            logging.debug("Ticker parse error: %s", err)
                            continue
                    
                    now = time.monotonic()
                    need_commit = len(batch) >= 100 or (batch and now - last_commit > 5)
                    
                    if need_commit and cursor and conn:
                        try:
                            cursor.executemany(_SQLITE_INSERT_SQL, batch)
                            conn.commit()
                            logging.info("Committed %d records to SQLite", len(batch))
                            batch.clear()
                            last_commit = now
                        except sqlite3.Error as e:
                            logging.error("SQLite insert/commit error: %s", e)
                            batch.clear()
                
                except json.JSONDecodeError:
                    continue
                except Exception as e:
                    logging.error("Error processing message: %s", e)
                    continue
    
    except sqlite3.Error as e:
        logging.error("SQLite connection error: %s", e)
    except OSError as e:
        logging.error("Error accessing SQLite file %s: %s", _output_file_path, e)
    finally:
        # Final commit
        if batch and cursor and conn:
            try:
                cursor.executemany(_SQLITE_INSERT_SQL, batch)
                conn.commit()
                logging.info("Committed final %d records to SQLite", len(batch))
            except sqlite3.Error as e:
                logging.error("SQLite final commit error: %s", e)
        
        if conn:
            conn.close()
            logging.info("Closed SQLite DB %s", _output_file_path)


# ────────────────────────────────────────── Mode 7: SQLite → Kafka ──
async def read_sqlite_to_kafka() -> None:
    """Read SQLite database and produce to Kafka."""
    if not _output_file_path.exists() or not _output_file_path.is_file():
        logging.error("SQLite file %s not found", _output_file_path)
        return
    
    kafka = _get_producer()
    logging.info("Reading from SQLite DB %s (table: %s) and producing to Kafka topic '%s'...",
                _output_file_path, _SQLITE_TABLE_NAME, KAFKA_TOPIC)
    
    produced = 0
    conn: sqlite3.Connection | None = None
    
    try:
        conn = sqlite3.connect(_output_file_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        # Select all columns except id
        columns = list(POLARS_SCHEMA.keys())
        query = f"SELECT {', '.join(columns)} FROM {_SQLITE_TABLE_NAME}"
        cursor.execute(query)
        
        # Fetch in chunks
        fetch_size = 1000
        while True:
            rows = cursor.fetchmany(fetch_size)
            if not rows:
                break
            
            for row in rows:
                try:
                    record_dict = dict(row)
                    kafka.produce(KAFKA_TOPIC, value=json.dumps(record_dict).encode())
                    produced += 1
                    
                    if produced % 1000 == 0:
                        kafka.poll(0)
                        logging.info("Produced %d records...", produced)
                except Exception as e:
                    logging.error("Error processing/producing row: %s", e)
                    continue
    
    except sqlite3.Error as e:
        logging.error("SQLite error while reading: %s", e)
    except OSError as e:
        logging.error("Error accessing SQLite file %s: %s", _output_file_path, e)
    finally:
        if conn:
            conn.close()
    
    kafka.flush()
    logging.info("Finished sending %d records from SQLite DB %s", produced, _output_file_path)


# ────────────────────────────────────────── Utility: print parquet ──
async def print_parquet_contents() -> None:
    """Print Parquet file contents."""
    if not _output_file_path.exists() or not _output_file_path.is_file():
        logging.error("Parquet file not found: %s", _output_file_path)
        return
    
    df = pl.read_parquet(_output_file_path)
    head = df.head(100)
    print(head.write_json(row_oriented=True))
    logging.info("Shown 100 / %d rows from %s", len(df), _output_file_path)


# ────────────────────────────────────────── Main ──
async def main() -> None:
    """Main entry point with mode selection."""
    p = argparse.ArgumentParser(description="Kraken ticker streaming utility")
    p.add_argument("-PF", "--print-file", action="store_true",
                  help="Print Parquet path + first 100 rows, then exit")
    
    g = p.add_mutually_exclusive_group()
    g.add_argument("-k", "--kafka", action="store_true", 
                  help="Stream Kraken → Kafka (default)")
    g.add_argument("-F", "--file", action="store_true", 
                  help="Stream Kraken → Parquet file")
    g.add_argument("-FK", "--file-to-kafka", action="store_true", 
                  help="Read Parquet → Kafka")
    g.add_argument("-J", "--json", action="store_true", 
                  help="Stream Kraken → JSON Lines file")
    g.add_argument("-JK", "--json-to-kafka", action="store_true", 
                  help="Read JSON Lines file → Kafka")
    g.add_argument("-S", "--sqlite", action="store_true", 
                  help="Stream Kraken → SQLite database")
    g.add_argument("-SK", "--sqlite-to-kafka", action="store_true", 
                  help="Read SQLite database → Kafka")
    
    p.add_argument("-o", "--output-file", type=Path, default=None,
                  help="Input/Output file path (default depends on mode)")
    p.add_argument("--format", choices=["json", "avro", "protobuf"], default="json",
                  help="Serialization format for Kafka (default: json)")
    p.add_argument("--schema-registry-url", type=str, default=DEFAULT_SCHEMA_REGISTRY_URL,
                  help=f"Schema Registry URL (default: {DEFAULT_SCHEMA_REGISTRY_URL})")
    p.add_argument("--products", type=str, nargs='+', default=None,
                  help="Product IDs to subscribe to (default: BTC/USD, ETH/USD, etc.)")
    
    args = p.parse_args()
    
    # Override products if specified
    if args.products:
        global PRODUCT_IDS
        PRODUCT_IDS = args.products
        logging.info("Using custom products: %s", PRODUCT_IDS)
    
    # Set global serialization format
    global _serialization_format
    _serialization_format = args.format
    
    # Initialize Schema Registry if using Avro or Protobuf
    if _serialization_format in ["avro", "protobuf"]:
        _init_schema_registry(args.schema_registry_url)
    
    # Determine mode
    mode = (
        "file" if args.file else
        "file_to_kafka" if args.file_to_kafka else
        "json" if args.json else
        "json_to_kafka" if args.json_to_kafka else
        "sqlite" if args.sqlite else
        "sqlite_to_kafka" if args.sqlite_to_kafka else
        "kafka"  # Default
    )
    
    # Set output file path
    global _output_file_path
    
    requires_file_arg = mode in ["file", "file_to_kafka", "json", "json_to_kafka", 
                                  "sqlite", "sqlite_to_kafka"] or args.print_file
    
    if requires_file_arg:
        if args.output_file:
            _output_file_path = args.output_file
        else:
            # Assign default based on mode
            if mode == "file":
                _output_file_path = DEFAULT_PARQUET_FILE
            elif mode == "json":
                _output_file_path = DEFAULT_JSONL_FILE
            elif mode == "sqlite":
                _output_file_path = DEFAULT_SQLITE_FILE
            else:
                logging.error(f"Mode '{mode}' requires an input file path specified with -o")
                return
    elif args.output_file:
        logging.warning(f"Ignoring -o/--output-file argument ('{args.output_file}') "
                       f"as it's not used in mode '{mode}'")
    
    if args.print_file:
        await print_parquet_contents()
        return
    
    logging.info("Selected mode: %s", mode)
    logging.info("Products: %s", ', '.join(PRODUCT_IDS))
    
    # Run appropriate mode
    if mode == "file_to_kafka":
        await read_parquet_to_kafka()
        return
    if mode == "json_to_kafka":
        await read_json_to_kafka()
        return
    if mode == "sqlite_to_kafka":
        await read_sqlite_to_kafka()
        return
    
    # Select target function for streaming modes
    if mode == "file":
        target = stream_to_parquet
    elif mode == "json":
        target = stream_to_json
    elif mode == "sqlite":
        target = stream_to_sqlite
    else:  # mode == "kafka"
        target = stream_to_kafka
    
    # Run with reconnection logic
    backoff = 1
    
    while True:
        try:
            await target()
            logging.warning("Stream ended unexpectedly; restarting in %ds", backoff)
        except (asyncio.CancelledError, KeyboardInterrupt):
            break
        except (websockets.ConnectionClosedError, OSError) as exc:
            logging.warning("Connection error: %s; reconnecting in %ds", exc, backoff)
        except Exception as exc:
            logging.error("Unexpected error: %s; retrying in %ds", exc, backoff)
        
        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, 60)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Interrupted by user — shutting down...")

