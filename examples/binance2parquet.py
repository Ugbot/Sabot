#!/usr/bin/env python3
"""
Stream Binance Spot WebSocket data through asyncio websocket.
Depending on flags, either pipe raw JSON to Kafka, append to a Parquet file,
or read from the Parquet file to Kafka.

Modes:
  -k   : Stream Binance → Kafka (default)
  -F   : Stream Binance → Parquet file (row-group streaming)
  -FK  : Read Parquet file → Kafka
  -J   : Stream Binance → JSON Lines file
  -JK  : Read JSON Lines file → Kafka
  -S   : Stream Binance → SQLite database
  -SK  : Read SQLite database → Kafka
  -PF  : Print Parquet path + first 100 rows, then exit

Serialization formats for Kafka:
  --format json      : JSON serialization (default)
  --format avro      : Avro serialization with Schema Registry
  --format protobuf  : Protobuf serialization with Schema Registry

Based on Binance WebSocket Streams API:
https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams
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
# Binance WebSocket endpoints
WS_URL_BASE = "wss://stream.binance.com:9443"
WS_URL_ALT = "wss://stream.binance.com:443"
WS_DATA_ONLY = "wss://data-stream.binance.vision"  # Market data only

KAFKA_TOPIC = "binance-ticker"
PARQUET_TOPIC = "binance-ticker"
PARQUET_BATCH_SIZE = 50  # rows per row-group

# Default filenames
DEFAULT_PARQUET_FILE = Path("./binance_ticker_data.parquet")
DEFAULT_JSONL_FILE = Path("./binance_ticker_data.jsonl")
DEFAULT_SQLITE_FILE = Path("./binance_ticker_data.db")
DEFAULT_SCHEMA_REGISTRY_URL = "http://localhost:18081"

# Binance uses lowercase symbols without separators (btcusdt, ethusdt)
SYMBOLS = [
    "btcusdt", "ethusdt", "xrpusdt", "ltcusdt",
    "bchusdt", "adausdt", "solusdt", "dotusdt",
    "linkusdt", "uniusdt", "algousdt", "maticusdt",
    "dogeusdt", "xlmusdt",
]

# ─────────────────────────────────── Pydantic models ──
class BinanceBookTicker(BaseModel):
    """
    Binance bookTicker stream data (real-time best bid/ask).
    
    Based on: https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams#individual-symbol-book-ticker-streams
    """
    u: int  # order book updateId
    s: str  # symbol (e.g., "BTCUSDT")
    b: float  # best bid price
    B: float  # best bid qty
    a: float  # best ask price
    A: float  # best ask qty
    
    # Add for consistency
    timestamp: Optional[int] = None  # Will be added when received
    
    @field_validator("b", "B", "a", "A", mode="before")
    def _num(cls, v):
        if v in (None, ""):
            return None
        try:
            return float(v)
        except (TypeError, ValueError):
            logging.warning("Could not convert %r to float; setting to None", v)
            return None


class BinanceTicker(BaseModel):
    """
    Binance 24hr ticker statistics.
    
    Based on: https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams#individual-symbol-ticker-streams
    """
    e: str  # Event type ("24hrTicker")
    E: int  # Event time (ms)
    s: str  # Symbol
    p: float  # Price change
    P: float  # Price change percent
    w: float  # Weighted average price
    x: Optional[float] = None  # First trade(F)-1 price (first trade before the 24hr rolling window)
    c: float  # Last price
    Q: Optional[float] = None  # Last quantity
    b: float  # Best bid price
    B: float  # Best bid quantity
    a: float  # Best ask price
    A: float  # Best ask quantity
    o: float  # Open price
    h: float  # High price
    l: float  # Low price (lowercase L)
    v: float  # Total traded base asset volume
    q: float  # Total traded quote asset volume
    O: int  # Statistics open time
    C: int  # Statistics close time
    F: int  # First trade ID
    L: int  # Last trade ID
    n: int  # Total number of trades
    
    @field_validator(
        "p", "P", "w", "x", "c", "Q", "b", "B", "a", "A", 
        "o", "h", "l", "v", "q",
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


class BinanceCombinedTicker(BaseModel):
    """
    Combined ticker with both 24hr stats and book ticker.
    
    This is what we'll actually store - combines best bid/ask with statistics.
    """
    # Symbol and timing
    symbol: str
    timestamp: int  # Event time (ms)
    
    # Best bid/ask (from bookTicker)
    best_bid: float
    best_bid_qty: float
    best_ask: float
    best_ask_qty: float
    
    # 24hr statistics (from ticker)
    last_price: float
    open_price: float
    high_price: float
    low_price: float
    volume: float  # Base asset
    quote_volume: float  # Quote asset
    price_change: float
    price_change_pct: float
    weighted_avg_price: float
    num_trades: int
    
    # Metadata
    channel: str = "combined"
    
    @field_validator(
        "best_bid", "best_bid_qty", "best_ask", "best_ask_qty",
        "last_price", "open_price", "high_price", "low_price",
        "volume", "quote_volume", "price_change", "price_change_pct",
        "weighted_avg_price",
        mode="before"
    )
    def _num(cls, v):
        if v in (None, ""):
            return None
        try:
            return float(v)
        except (TypeError, ValueError):
            return None


# ─────────────────────────────────────────────── Avro schema ──
AVRO_SCHEMA = {
    "type": "record",
    "name": "BinanceTicker",
    "namespace": "com.binance.ticker",
    "fields": [
        {"name": "symbol", "type": "string"},
        {"name": "timestamp", "type": "long"},
        {"name": "best_bid", "type": "double"},
        {"name": "best_bid_qty", "type": "double"},
        {"name": "best_ask", "type": "double"},
        {"name": "best_ask_qty", "type": "double"},
        {"name": "last_price", "type": "double"},
        {"name": "open_price", "type": "double"},
        {"name": "high_price", "type": "double"},
        {"name": "low_price", "type": "double"},
        {"name": "volume", "type": "double"},
        {"name": "quote_volume", "type": "double"},
        {"name": "price_change", "type": "double"},
        {"name": "price_change_pct", "type": "double"},
        {"name": "weighted_avg_price", "type": "double"},
        {"name": "num_trades", "type": "long"},
        {"name": "channel", "type": "string"},
    ]
}

# ─────────────────────────────────────────────── Polars schema ──
POLARS_SCHEMA = {
    "symbol": pl.String,
    "timestamp": pl.Int64,
    "best_bid": pl.Float64,
    "best_bid_qty": pl.Float64,
    "best_ask": pl.Float64,
    "best_ask_qty": pl.Float64,
    "last_price": pl.Float64,
    "open_price": pl.Float64,
    "high_price": pl.Float64,
    "low_price": pl.Float64,
    "volume": pl.Float64,
    "quote_volume": pl.Float64,
    "price_change": pl.Float64,
    "price_change_pct": pl.Float64,
    "weighted_avg_price": pl.Float64,
    "num_trades": pl.Int64,
    "channel": pl.String,
}

# ────────────────────────────────────────── SQLite setup ──
_SQLITE_TABLE_NAME = "binance_ticker"
_SQLITE_SCHEMA = f"""
CREATE TABLE IF NOT EXISTS {_SQLITE_TABLE_NAME} (
    symbol TEXT,
    timestamp INTEGER,
    best_bid REAL,
    best_bid_qty REAL,
    best_ask REAL,
    best_ask_qty REAL,
    last_price REAL,
    open_price REAL,
    high_price REAL,
    low_price REAL,
    volume REAL,
    quote_volume REAL,
    price_change REAL,
    price_change_pct REAL,
    weighted_avg_price REAL,
    num_trades INTEGER,
    channel TEXT,
    id INTEGER PRIMARY KEY AUTOINCREMENT
);
"""

_SQLITE_INSERT_SQL = f"""INSERT INTO {_SQLITE_TABLE_NAME} 
(symbol, timestamp, best_bid, best_bid_qty, best_ask, best_ask_qty,
 last_price, open_price, high_price, low_price, volume, quote_volume,
 price_change, price_change_pct, weighted_avg_price, num_trades, channel) 
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""


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
_use_combined_stream: bool = True  # Use combined stream for multiple symbols

# ───────────────────────────────────────── SSL + logging ──
ssl_ctx = ssl.create_default_context(cafile=certifi.where())
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

# ───────────────────────────────────────── WebSocket URL construction ──
def create_websocket_url() -> str:
    """
    Create Binance WebSocket URL with combined streams.
    
    Format: /stream?streams=btcusdt@bookTicker/btcusdt@ticker/ethusdt@bookTicker/...
    
    We subscribe to both @bookTicker (real-time bid/ask) and @ticker (24hr stats).
    """
    if _use_combined_stream:
        # Combined stream with both bookTicker and ticker for each symbol
        streams = []
        for symbol in SYMBOLS:
            streams.append(f"{symbol}@bookTicker")  # Real-time best bid/ask
            streams.append(f"{symbol}@ticker")      # 24hr rolling stats
        
        stream_params = "/".join(streams)
        url = f"{WS_URL_BASE}/stream?streams={stream_params}"
        return url
    else:
        # Single symbol stream (for testing)
        return f"{WS_URL_BASE}/ws/{SYMBOLS[0]}@bookTicker"


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
            import binance_ticker_pb2
            _protobuf_serializer = ProtobufSerializer(
                binance_ticker_pb2.BinanceTicker,
                _schema_registry_client,
                conf={"use.deprecated.format": False}
            )
        except ImportError:
            logging.error("Protobuf module not found. Run: protoc --python_out=. binance_ticker.proto")
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
            import binance_ticker_pb2
            msg = binance_ticker_pb2.BinanceTicker(**data)
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


# ───────────────────────────────────────── State management for combining streams ──
class StreamCombiner:
    """
    Combines bookTicker and ticker streams into unified records.
    
    Binance sends separate messages for:
    - bookTicker: real-time best bid/ask updates
    - ticker: 24hr rolling statistics (every 1000ms)
    
    We combine them into single records with both.
    """
    
    def __init__(self):
        self.book_state = {}  # symbol -> latest bookTicker
        self.ticker_state = {}  # symbol -> latest ticker
    
    def update_book(self, book: BinanceBookTicker):
        """Update book ticker state."""
        self.book_state[book.s] = book
    
    def update_ticker(self, ticker: BinanceTicker):
        """Update 24hr ticker state."""
        self.ticker_state[ticker.s] = ticker
    
    def get_combined(self, symbol: str) -> Optional[BinanceCombinedTicker]:
        """Get combined ticker for symbol (if both book and ticker available)."""
        book = self.book_state.get(symbol)
        ticker = self.ticker_state.get(symbol)
        
        if not book or not ticker:
            return None
        
        return BinanceCombinedTicker(
            symbol=symbol,
            timestamp=ticker.E,
            # Book data
            best_bid=book.b,
            best_bid_qty=book.B,
            best_ask=book.a,
            best_ask_qty=book.A,
            # Ticker data
            last_price=ticker.c,
            open_price=ticker.o,
            high_price=ticker.h,
            low_price=ticker.l,
            volume=ticker.v,
            quote_volume=ticker.q,
            price_change=ticker.p,
            price_change_pct=ticker.P,
            weighted_avg_price=ticker.w,
            num_trades=ticker.n,
        )


# ────────────────────────────────────────── Mode 1: Binance → Kafka ──
async def stream_to_kafka() -> None:
    """Stream Binance WebSocket data to Kafka."""
    kafka = _get_producer()
    ws_url = create_websocket_url()
    
    logging.info("Connecting to Binance: %s", ws_url[:100] + "...")
    
    async with websockets.connect(ws_url, ssl=ssl_ctx, ping_interval=20, ping_timeout=60) as ws:
        logging.info("Connected! Streaming to Kafka topic '%s' (format: %s)", 
                    KAFKA_TOPIC, _serialization_format)
        
        combiner = StreamCombiner()
        last_flush = time.monotonic()
        
        async for frame in ws:
            try:
                msg = json.loads(frame)
                
                # Handle combined stream format
                if "stream" in msg and "data" in msg:
                    stream_name = msg["stream"]
                    data = msg["data"]
                    
                    # Process based on stream type
                    if "@bookTicker" in stream_name:
                        # Real-time best bid/ask
                        try:
                            data['timestamp'] = int(time.time() * 1000)
                            book = BinanceBookTicker.model_validate(data)
                            combiner.update_book(book)
                        except Exception as e:
                            logging.debug("BookTicker parse error: %s", e)
                    
                    elif "@ticker" in stream_name:
                        # 24hr statistics
                        try:
                            ticker = BinanceTicker.model_validate(data)
                            combiner.update_ticker(ticker)
                            
                            # Try to get combined record
                            combined = combiner.get_combined(ticker.s)
                            if combined:
                                combined_dict = combined.model_dump()
                                
                                if _serialization_format == "json":
                                    kafka.produce(KAFKA_TOPIC, value=json.dumps(combined_dict).encode())
                                else:
                                    serialized = _serialize_message(combined_dict)
                                    kafka.produce(KAFKA_TOPIC, value=serialized)
                        
                        except Exception as e:
                            logging.debug("Ticker parse error: %s", e)
                
                else:
                    # Non-combined stream format (single symbol)
                    # Send raw
                    if _serialization_format == "json":
                        kafka.produce(KAFKA_TOPIC, value=frame.encode())
                
                # Periodic flush
                now = time.monotonic()
                if now - last_flush > 1.0:
                    kafka.poll(0)
                    kafka.flush()
                    last_flush = now
            
            except json.JSONDecodeError as e:
                logging.warning("JSON decode error: %s", e)
                continue
            except Exception as e:
                logging.error("Error processing frame: %s", e)
                continue


# ────────────────────────────────────────── Mode 2: Binance → Parquet ──
async def stream_to_parquet() -> None:
    """Stream Binance WebSocket data to Parquet file."""
    batch: list[BinanceCombinedTicker] = []
    last_write = time.monotonic()
    first_write = True
    
    if _output_file_path.exists():
        logging.warning("%s already exists and will be **overwritten**", _output_file_path)
    
    ws_url = create_websocket_url()
    logging.info("Connecting to Binance: %s", ws_url[:100] + "...")
    
    async with websockets.connect(ws_url, ssl=ssl_ctx, ping_interval=20, ping_timeout=60) as ws:
        logging.info("Connected! Streaming to Parquet file %s", _output_file_path)
        
        combiner = StreamCombiner()
        
        try:
            async for frame in ws:
                try:
                    msg = json.loads(frame)
                    
                    # Handle combined stream format
                    if "stream" in msg and "data" in msg:
                        stream_name = msg["stream"]
                        data = msg["data"]
                        
                        if "@bookTicker" in stream_name:
                            data['timestamp'] = int(time.time() * 1000)
                            book = BinanceBookTicker.model_validate(data)
                            combiner.update_book(book)
                        
                        elif "@ticker" in stream_name:
                            ticker = BinanceTicker.model_validate(data)
                            combiner.update_ticker(ticker)
                            
                            # Get combined record
                            combined = combiner.get_combined(ticker.s)
                            if combined:
                                batch.append(combined)
                    
                    now = time.monotonic()
                    need_flush = len(batch) >= PARQUET_BATCH_SIZE or (batch and now - last_write > 5)
                    
                    if need_flush:
                        _flush_batch_polars(batch, _output_file_path, first_write)
                        first_write = False
                        last_write = now
                
                except json.JSONDecodeError:
                    continue
                except Exception as e:
                    logging.debug("Error processing message: %s", e)
                    continue
        
        finally:
            if batch:
                _flush_batch_polars(batch, _output_file_path, first_write)
            logging.info("Closed Parquet file %s", _output_file_path)


def _flush_batch_polars(batch: list[BinanceCombinedTicker], file_path: Path, first_write: bool) -> None:
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


# ────────────────────────────────────────── Mode 3: Binance → JSON Lines ──
async def stream_to_json() -> None:
    """Stream Binance WebSocket data to JSON Lines file."""
    if _output_file_path.exists():
        logging.warning("%s already exists and will be **overwritten**", _output_file_path)
    
    try:
        with open(_output_file_path, "a+", encoding="utf-8", buffering=1) as f:
            logging.info("Opened %s for writing JSON lines", _output_file_path)
            
            ws_url = create_websocket_url()
            async with websockets.connect(ws_url, ssl=ssl_ctx, ping_interval=20, ping_timeout=60) as ws:
                logging.info("Connected! Streaming to %s", _output_file_path)
                
                combiner = StreamCombiner()
                
                async for frame in ws:
                    try:
                        msg = json.loads(frame)
                        
                        if "stream" in msg and "data" in msg:
                            stream_name = msg["stream"]
                            data = msg["data"]
                            
                            if "@bookTicker" in stream_name:
                                data['timestamp'] = int(time.time() * 1000)
                                book = BinanceBookTicker.model_validate(data)
                                combiner.update_book(book)
                            
                            elif "@ticker" in stream_name:
                                ticker = BinanceTicker.model_validate(data)
                                combiner.update_ticker(ticker)
                                
                                combined = combiner.get_combined(ticker.s)
                                if combined:
                                    f.write(combined.model_dump_json() + '\n')
                    
                    except json.JSONDecodeError:
                        continue
                    except Exception as e:
                        logging.debug("Error writing JSON line: %s", e)
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


# ────────────────────────────────────────── Mode 6: Binance → SQLite ──
async def stream_to_sqlite() -> None:
    """Stream Binance WebSocket data to SQLite database."""
    batch: list[tuple] = []
    conn: sqlite3.Connection | None = None
    cursor: sqlite3.Cursor | None = None
    last_commit = time.monotonic()
    
    try:
        conn = _get_sqlite_conn(_output_file_path)
        cursor = conn.cursor()
        logging.info("Opened SQLite DB %s and ensured table '%s' exists", 
                    _output_file_path, _SQLITE_TABLE_NAME)
        
        ws_url = create_websocket_url()
        async with websockets.connect(ws_url, ssl=ssl_ctx, ping_interval=20, ping_timeout=60) as ws:
            logging.info("Connected! Streaming to %s", _output_file_path)
            
            combiner = StreamCombiner()
            
            async for frame in ws:
                try:
                    msg = json.loads(frame)
                    
                    if "stream" in msg and "data" in msg:
                        stream_name = msg["stream"]
                        data = msg["data"]
                        
                        if "@bookTicker" in stream_name:
                            data['timestamp'] = int(time.time() * 1000)
                            book = BinanceBookTicker.model_validate(data)
                            combiner.update_book(book)
                        
                        elif "@ticker" in stream_name:
                            ticker = BinanceTicker.model_validate(data)
                            combiner.update_ticker(ticker)
                            
                            combined = combiner.get_combined(ticker.s)
                            if combined:
                                # Convert to tuple for insertion
                                record_tuple = (
                                    combined.symbol,
                                    combined.timestamp,
                                    combined.best_bid,
                                    combined.best_bid_qty,
                                    combined.best_ask,
                                    combined.best_ask_qty,
                                    combined.last_price,
                                    combined.open_price,
                                    combined.high_price,
                                    combined.low_price,
                                    combined.volume,
                                    combined.quote_volume,
                                    combined.price_change,
                                    combined.price_change_pct,
                                    combined.weighted_avg_price,
                                    combined.num_trades,
                                    combined.channel,
                                )
                                batch.append(record_tuple)
                    
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
                    logging.debug("Error processing message: %s", e)
                    continue
    
    except sqlite3.Error as e:
        logging.error("SQLite connection error: %s", e)
    except OSError as e:
        logging.error("Error accessing SQLite file %s: %s", _output_file_path, e)
    finally:
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
        
        columns = list(POLARS_SCHEMA.keys())
        query = f"SELECT {', '.join(columns)} FROM {_SQLITE_TABLE_NAME}"
        cursor.execute(query)
        
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
    p = argparse.ArgumentParser(description="Binance ticker streaming utility")
    p.add_argument("-PF", "--print-file", action="store_true",
                  help="Print Parquet path + first 100 rows, then exit")
    
    g = p.add_mutually_exclusive_group()
    g.add_argument("-k", "--kafka", action="store_true", 
                  help="Stream Binance → Kafka (default)")
    g.add_argument("-F", "--file", action="store_true", 
                  help="Stream Binance → Parquet file")
    g.add_argument("-FK", "--file-to-kafka", action="store_true", 
                  help="Read Parquet → Kafka")
    g.add_argument("-J", "--json", action="store_true", 
                  help="Stream Binance → JSON Lines file")
    g.add_argument("-JK", "--json-to-kafka", action="store_true", 
                  help="Read JSON Lines file → Kafka")
    g.add_argument("-S", "--sqlite", action="store_true", 
                  help="Stream Binance → SQLite database")
    g.add_argument("-SK", "--sqlite-to-kafka", action="store_true", 
                  help="Read SQLite database → Kafka")
    
    p.add_argument("-o", "--output-file", type=Path, default=None,
                  help="Input/Output file path (default depends on mode)")
    p.add_argument("--format", choices=["json", "avro", "protobuf"], default="json",
                  help="Serialization format for Kafka (default: json)")
    p.add_argument("--schema-registry-url", type=str, default=DEFAULT_SCHEMA_REGISTRY_URL,
                  help=f"Schema Registry URL (default: {DEFAULT_SCHEMA_REGISTRY_URL})")
    p.add_argument("--symbols", type=str, nargs='+', default=None,
                  help="Symbols to subscribe (default: btcusdt, ethusdt, etc.)")
    
    args = p.parse_args()
    
    # Override symbols if specified
    if args.symbols:
        global SYMBOLS
        SYMBOLS = [s.lower() for s in args.symbols]  # Binance requires lowercase
        logging.info("Using custom symbols: %s", SYMBOLS)
    
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
    logging.info("Symbols: %s", ', '.join(SYMBOLS))
    
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
    
    # Run with reconnection logic (Binance connection valid for 24hrs)
    backoff = 1
    
    while True:
        try:
            await target()
            logging.warning("Stream ended; reconnecting in %ds", backoff)
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

