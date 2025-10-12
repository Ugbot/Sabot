#!/usr/bin/env python3
"""
Unified Exchange Data Collector - Refactored Architecture

Streams data from any supported exchange (Coinbase, Kraken, Binance) to
multiple storage backends (Kafka, Parquet, JSON Lines, SQLite).

Usage:
    python exchange2storage.py coinbase -k              # Coinbase → Kafka
    python exchange2storage.py kraken -F                # Kraken → Parquet
    python exchange2storage.py binance -S               # Binance → SQLite
    python exchange2storage.py coinbase -FK             # Parquet → Kafka

Architecture:
    exchange_configs/          # Small exchange-specific files
    ├── coinbase_config.py     # Coinbase: URL, schema, parsing
    ├── kraken_config.py       # Kraken: URL, schema, parsing  
    └── binance_config.py      # Binance: URL, schema, parsing
    
    exchange_common/           # Shared infrastructure
    ├── storage.py             # Kafka, Parquet, SQLite, JSON
    └── websocket.py           # WebSocket client + reconnection
    
    exchange2storage.py        # This file (unified runner)
"""

import argparse
import asyncio
import logging
import sys
import time
from pathlib import Path
from typing import Optional
import importlib

# Import common infrastructure
from exchange_common import (
    KafkaStorage,
    ParquetStorage,
    JSONLinesStorage,
    SQLiteStorage,
    StorageManager,
    ExchangeWebSocket,
)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


# ============================================================================
# Main Streaming Logic
# ============================================================================

async def stream_to_storage(
    exchange_config,
    storage_manager: StorageManager,
    use_json_mode: bool = False
):
    """
    Stream from exchange WebSocket to storage backends.
    
    Args:
        exchange_config: Exchange-specific configuration module
        storage_manager: Storage manager with configured backends
        use_json_mode: If True, send raw JSON; if False, parse and extract records
    """
    # Create WebSocket client
    ws_url = exchange_config.create_websocket_url() if hasattr(exchange_config, 'create_websocket_url') else exchange_config.WS_URL
    ws = ExchangeWebSocket(
        url=ws_url,
        name=exchange_config.EXCHANGE_NAME.capitalize()
    )
    
    # State for message parsing (e.g., Binance combiner)
    parser_state = {}
    if hasattr(exchange_config, 'StreamCombiner'):
        parser_state['combiner'] = exchange_config.StreamCombiner()
    
    # Message counter
    msg_count = 0
    record_count = 0
    last_log = time.monotonic()
    
    async def on_connect():
        """Called after WebSocket connection established."""
        # Send subscription message if needed
        subscribe_msg = exchange_config.create_subscribe_message()
        if subscribe_msg:
            await ws.send(subscribe_msg)
            logger.info(f"Sent subscription request")
    
    async def on_message(frame: str):
        """Process each WebSocket message."""
        nonlocal msg_count, record_count, last_log
        
        msg_count += 1
        
        if use_json_mode:
            # Raw JSON mode - send frame as-is
            storage_manager.write({'raw': frame})
        else:
            # Parse and extract records
            records = exchange_config.parse_message(frame, **parser_state)
            
            if records:
                for record in records:
                    storage_manager.write(record)
                    record_count += 1
        
        # Periodic logging and flush
        now = time.monotonic()
        if now - last_log > 10:
            logger.info(f"Processed {msg_count} messages, {record_count} records")
            storage_manager.flush()
            last_log = now
    
    # Stream messages
    try:
        await ws.stream_messages(on_message, on_connect)
    finally:
        storage_manager.flush()
        storage_manager.close()


# ============================================================================
# Read from Storage and Re-publish
# ============================================================================

async def read_parquet_to_kafka(file_path: Path, kafka_storage: KafkaStorage):
    """Read Parquet file and produce to Kafka."""
    import polars as pl
    
    if not file_path.exists():
        logger.error(f"Parquet file not found: {file_path}")
        return
    
    logger.info(f"Reading from {file_path}...")
    df = pl.read_parquet(file_path)
    logger.info(f"Loaded {len(df)} rows")
    
    for i, record in enumerate(df.iter_rows(named=True)):
        kafka_storage.write(record)
        
        if (i + 1) % 1000 == 0:
            kafka_storage.flush()
            logger.info(f"Produced {i + 1} records...")
    
    kafka_storage.flush()
    logger.info(f"✅ Finished producing {len(df)} records to Kafka")


async def read_jsonl_to_kafka(file_path: Path, kafka_storage: KafkaStorage):
    """Read JSON Lines file and produce to Kafka."""
    import json
    
    if not file_path.exists():
        logger.error(f"JSON file not found: {file_path}")
        return
    
    logger.info(f"Reading from {file_path}...")
    
    count = 0
    with open(file_path, 'r') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            
            try:
                record = json.loads(line)
                kafka_storage.write(record)
                count += 1
                
                if count % 1000 == 0:
                    kafka_storage.flush()
                    logger.info(f"Produced {count} records...")
            except json.JSONDecodeError:
                continue
    
    kafka_storage.flush()
    logger.info(f"✅ Finished producing {count} records to Kafka")


async def read_sqlite_to_kafka(file_path: Path, kafka_storage: KafkaStorage):
    """Read SQLite database and produce to Kafka."""
    import sqlite3
    
    if not file_path.exists():
        logger.error(f"SQLite file not found: {file_path}")
        return
    
    logger.info(f"Reading from SQLite: {file_path}...")
    
    conn = sqlite3.connect(file_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    # Determine table name (exchange-specific)
    tables = cursor.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    if not tables:
        logger.error("No tables found in SQLite database")
        return
    
    table_name = tables[0][0]
    logger.info(f"Reading from table: {table_name}")
    
    cursor.execute(f"SELECT * FROM {table_name}")
    
    count = 0
    while True:
        rows = cursor.fetchmany(1000)
        if not rows:
            break
        
        for row in rows:
            kafka_storage.write(dict(row))
            count += 1
        
        kafka_storage.flush()
        logger.info(f"Produced {count} records...")
    
    conn.close()
    logger.info(f"✅ Finished producing {count} records to Kafka")


# ============================================================================
# Main Entry Point
# ============================================================================

async def main():
    """Main entry point with mode and exchange selection."""
    parser = argparse.ArgumentParser(
        description="Unified exchange data collector (Coinbase, Kraken, Binance)"
    )
    
    parser.add_argument(
        "exchange",
        choices=["coinbase", "kraken", "binance"],
        help="Exchange to collect from"
    )
    
    # Modes (same as original)
    parser.add_argument("-PF", "--print-file", action="store_true",
                       help="Print Parquet file contents and exit")
    
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument("-k", "--kafka", action="store_true",
                           help="Stream exchange → Kafka (default)")
    mode_group.add_argument("-F", "--file", action="store_true",
                           help="Stream exchange → Parquet file")
    mode_group.add_argument("-FK", "--file-to-kafka", action="store_true",
                           help="Read Parquet → Kafka")
    mode_group.add_argument("-J", "--json", action="store_true",
                           help="Stream exchange → JSON Lines")
    mode_group.add_argument("-JK", "--json-to-kafka", action="store_true",
                           help="Read JSON Lines → Kafka")
    mode_group.add_argument("-S", "--sqlite", action="store_true",
                           help="Stream exchange → SQLite")
    mode_group.add_argument("-SK", "--sqlite-to-kafka", action="store_true",
                           help="Read SQLite → Kafka")
    
    parser.add_argument("-o", "--output-file", type=Path, default=None,
                       help="Output file path")
    parser.add_argument("--format", choices=["json", "avro", "protobuf"], default="json",
                       help="Kafka serialization format")
    parser.add_argument("--schema-registry-url", type=str, default="http://localhost:18081",
                       help="Schema Registry URL")
    
    args = parser.parse_args()
    
    # Load exchange configuration
    logger.info(f"Loading {args.exchange} configuration...")
    exchange_config = importlib.import_module(f"exchange_configs.{args.exchange}_config")
    logger.info(f"✅ Loaded {exchange_config.EXCHANGE_NAME} config")
    
    # Determine mode
    mode = (
        "file" if args.file else
        "file_to_kafka" if args.file_to_kafka else
        "json" if args.json else
        "json_to_kafka" if args.json_to_kafka else
        "sqlite" if args.sqlite else
        "sqlite_to_kafka" if args.sqlite_to_kafka else
        "kafka"
    )
    
    logger.info(f"Mode: {mode}")
    
    # Set output file
    if mode in ["file", "json", "sqlite"]:
        if args.output_file:
            output_file = args.output_file
        else:
            # Default files
            defaults = {
                "file": f"./{args.exchange}_ticker_data.parquet",
                "json": f"./{args.exchange}_ticker_data.jsonl",
                "sqlite": f"./{args.exchange}_ticker_data.db",
            }
            output_file = Path(defaults.get(mode, f"./{args.exchange}_data"))
        
        logger.info(f"Output file: {output_file}")
    
    # Handle print mode
    if args.print_file:
        if not args.output_file:
            output_file = Path(f"./{args.exchange}_ticker_data.parquet")
        else:
            output_file = args.output_file
        
        if output_file.exists():
            import polars as pl
            df = pl.read_parquet(output_file)
            print(df.head(100))
            logger.info(f"Shown 100 / {len(df)} rows from {output_file}")
        else:
            logger.error(f"File not found: {output_file}")
        return
    
    # Handle read modes (file → Kafka)
    if mode in ["file_to_kafka", "json_to_kafka", "sqlite_to_kafka"]:
        if not args.output_file:
            logger.error("Input file required with -o for read modes")
            return
        
        # Create Kafka storage
        kafka = KafkaStorage(
            topic=exchange_config.KAFKA_TOPIC,
            format=args.format,
            schema_registry_url=args.schema_registry_url if args.format != "json" else None,
            avro_schema=exchange_config.AVRO_SCHEMA if hasattr(exchange_config, 'AVRO_SCHEMA') else None
        )
        
        # Read and republish
        if mode == "file_to_kafka":
            await read_parquet_to_kafka(args.output_file, kafka)
        elif mode == "json_to_kafka":
            await read_jsonl_to_kafka(args.output_file, kafka)
        elif mode == "sqlite_to_kafka":
            await read_sqlite_to_kafka(args.output_file, kafka)
        
        return
    
    # Stream modes - create storage backends
    storage_mgr = StorageManager()
    
    if mode == "kafka" or mode == "file" or mode == "json" or mode == "sqlite":
        # Add Kafka if in kafka mode or multi-output
        if mode == "kafka":
            kafka = KafkaStorage(
                topic=exchange_config.KAFKA_TOPIC,
                format=args.format,
                schema_registry_url=args.schema_registry_url if args.format != "json" else None,
                avro_schema=exchange_config.AVRO_SCHEMA if hasattr(exchange_config, 'AVRO_SCHEMA') else None
            )
            storage_mgr.add_backend(kafka)
        
        # Add Parquet if in file mode
        if mode == "file":
            parquet = ParquetStorage(
                file_path=output_file,
                schema=exchange_config.POLARS_SCHEMA,
                batch_size=50
            )
            storage_mgr.add_backend(parquet)
        
        # Add JSON Lines if in json mode
        if mode == "json":
            jsonl = JSONLinesStorage(file_path=output_file)
            jsonl.open()
            storage_mgr.add_backend(jsonl)
        
        # Add SQLite if in sqlite mode
        if mode == "sqlite":
            sqlite = SQLiteStorage(
                file_path=output_file,
                table_name=f"{args.exchange}_ticker",
                schema_sql=exchange_config.SQLITE_SCHEMA,
                insert_sql=exchange_config.SQLITE_INSERT,
                batch_size=100
            )
            sqlite.open()
            storage_mgr.add_backend(sqlite)
    
    # Stream data
    logger.info(f"🚀 Starting {exchange_config.EXCHANGE_NAME.upper()} → {mode.upper()}")
    
    try:
        await stream_to_storage(
            exchange_config=exchange_config,
            storage_manager=storage_mgr,
            use_json_mode=(args.format == "json" and mode == "kafka")
        )
    except KeyboardInterrupt:
        logger.info("⏹️  Interrupted by user")
    finally:
        storage_mgr.close()
        logger.info("✅ Shutdown complete")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Interrupted by user — shutting down...")

