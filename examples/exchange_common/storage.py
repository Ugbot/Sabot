"""
Common storage backends for exchange data collectors.

Supports:
- Kafka (with JSON/Avro/Protobuf serialization)
- Parquet (with Polars)
- JSON Lines
- SQLite
"""

import json
import logging
import sqlite3
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import polars as pl
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.schema_registry.protobuf import ProtobufSerializer
from confluent_kafka.serialization import SerializationContext, MessageField


logger = logging.getLogger(__name__)


# ============================================================================
# Kafka Storage
# ============================================================================

class KafkaStorage:
    """Kafka producer with multiple serialization formats."""
    
    def __init__(
        self,
        topic: str,
        bootstrap_servers: str = "localhost:19092",
        format: str = "json",
        schema_registry_url: Optional[str] = None,
        avro_schema: Optional[dict] = None,
        protobuf_class: Optional[type] = None
    ):
        self.topic = topic
        self.format = format
        self.producer = Producer({"bootstrap.servers": bootstrap_servers})
        
        # Initialize serializers if needed
        self.avro_serializer = None
        self.protobuf_serializer = None
        
        if format in ["avro", "protobuf"] and schema_registry_url:
            self.schema_registry = SchemaRegistryClient({"url": schema_registry_url})
            
            if format == "avro" and avro_schema:
                self.avro_serializer = AvroSerializer(
                    self.schema_registry,
                    json.dumps(avro_schema),
                    lambda obj, ctx: obj
                )
            
            if format == "protobuf" and protobuf_class:
                self.protobuf_serializer = ProtobufSerializer(
                    protobuf_class,
                    self.schema_registry,
                    conf={"use.deprecated.format": False}
                )
        
        logger.info(f"Kafka storage initialized: topic={topic}, format={format}")
    
    def write(self, data: dict) -> None:
        """Write record to Kafka."""
        try:
            serialized = self._serialize(data)
            self.producer.produce(self.topic, value=serialized)
        except Exception as e:
            logger.error(f"Failed to produce to Kafka: {e}")
            raise
    
    def write_batch(self, records: List[dict]) -> None:
        """Write multiple records to Kafka."""
        for record in records:
            self.write(record)
        self.flush()
    
    def flush(self) -> None:
        """Flush pending messages."""
        self.producer.poll(0)
        self.producer.flush()
    
    def _serialize(self, data: dict) -> bytes:
        """Serialize data based on configured format."""
        if self.format == "json":
            return json.dumps(data).encode()
        
        elif self.format == "avro":
            if not self.avro_serializer:
                raise RuntimeError("Avro serializer not initialized")
            ctx = SerializationContext(self.topic, MessageField.VALUE)
            return self.avro_serializer(data, ctx)
        
        elif self.format == "protobuf":
            if not self.protobuf_serializer:
                raise RuntimeError("Protobuf serializer not initialized")
            ctx = SerializationContext(self.topic, MessageField.VALUE)
            return self.protobuf_serializer(data, ctx)
        
        else:
            raise ValueError(f"Unknown format: {self.format}")


# ============================================================================
# Parquet Storage
# ============================================================================

class ParquetStorage:
    """Parquet file storage with row-group streaming."""
    
    def __init__(
        self,
        file_path: Path,
        schema: dict,
        batch_size: int = 50,
        compression: str = "snappy"
    ):
        self.file_path = Path(file_path)
        self.schema = schema
        self.batch_size = batch_size
        self.compression = compression
        self.batch = []
        self.first_write = True
        self.last_write = time.monotonic()
        
        logger.info(f"Parquet storage initialized: {file_path}")
    
    def write(self, record: dict) -> None:
        """Add record to batch (auto-flushes when batch_size reached)."""
        self.batch.append(record)
        
        now = time.monotonic()
        need_flush = len(self.batch) >= self.batch_size or (self.batch and now - self.last_write > 5)
        
        if need_flush:
            self.flush()
    
    def write_batch(self, records: List[dict]) -> None:
        """Write multiple records."""
        for record in records:
            self.batch.append(record)
        self.flush()
    
    def flush(self) -> None:
        """Flush batch to Parquet file."""
        if not self.batch:
            return
        
        # Create DataFrame
        df = pl.DataFrame(self.batch, schema=self.schema)
        self.batch.clear()
        
        # Write or append
        if self.first_write:
            df.write_parquet(self.file_path, compression=self.compression, use_pyarrow=False)
            logger.info(f"Created {self.file_path} with {len(df)} rows")
            self.first_write = False
        else:
            existing = pl.read_parquet(self.file_path)
            combined = pl.concat([existing, df])
            combined.write_parquet(self.file_path, compression=self.compression, use_pyarrow=False)
            logger.info(f"Appended {len(df)} rows (total: {len(combined)} rows, "
                       f"{self.file_path.stat().st_size / 2**20:.1f} MiB)")
        
        self.last_write = time.monotonic()


# ============================================================================
# JSON Lines Storage
# ============================================================================

class JSONLinesStorage:
    """JSON Lines file storage."""
    
    def __init__(self, file_path: Path):
        self.file_path = Path(file_path)
        self.file = None
        logger.info(f"JSON Lines storage initialized: {file_path}")
    
    def open(self) -> None:
        """Open file for writing."""
        self.file = open(self.file_path, "a+", encoding="utf-8", buffering=1)
    
    def write(self, record: dict) -> None:
        """Write record as JSON line."""
        if self.file is None:
            self.open()
        
        try:
            self.file.write(json.dumps(record) + '\n')
        except Exception as e:
            logger.error(f"Failed to write JSON line: {e}")
    
    def write_batch(self, records: List[dict]) -> None:
        """Write multiple records."""
        for record in records:
            self.write(record)
    
    def flush(self) -> None:
        """Flush file buffer."""
        if self.file:
            self.file.flush()
    
    def close(self) -> None:
        """Close file."""
        if self.file:
            self.file.close()
            self.file = None
            logger.info(f"Closed JSON Lines file {self.file_path}")


# ============================================================================
# SQLite Storage
# ============================================================================

class SQLiteStorage:
    """SQLite database storage."""
    
    def __init__(
        self,
        file_path: Path,
        table_name: str,
        schema_sql: str,
        insert_sql: str,
        batch_size: int = 100
    ):
        self.file_path = Path(file_path)
        self.table_name = table_name
        self.schema_sql = schema_sql
        self.insert_sql = insert_sql
        self.batch_size = batch_size
        
        self.conn = None
        self.cursor = None
        self.batch = []
        self.last_commit = time.monotonic()
        
        logger.info(f"SQLite storage initialized: {file_path}, table={table_name}")
    
    def open(self) -> None:
        """Open database connection."""
        self.conn = sqlite3.connect(self.file_path, check_same_thread=False)
        self.conn.execute(self.schema_sql)
        self.conn.commit()
        self.cursor = self.conn.cursor()
        logger.info(f"Opened SQLite DB and ensured table '{self.table_name}' exists")
    
    def write(self, record_tuple: tuple) -> None:
        """Add record to batch (auto-commits when batch_size reached)."""
        if self.conn is None:
            self.open()
        
        self.batch.append(record_tuple)
        
        now = time.monotonic()
        need_commit = len(self.batch) >= self.batch_size or (self.batch and now - self.last_commit > 5)
        
        if need_commit:
            self.flush()
    
    def write_batch(self, records: List[tuple]) -> None:
        """Write multiple records."""
        if self.conn is None:
            self.open()
        
        try:
            self.cursor.executemany(self.insert_sql, records)
            self.conn.commit()
            logger.info(f"Committed {len(records)} records to SQLite")
        except sqlite3.Error as e:
            logger.error(f"SQLite batch insert error: {e}")
    
    def flush(self) -> None:
        """Commit pending batch."""
        if not self.batch or not self.cursor or not self.conn:
            return
        
        try:
            self.cursor.executemany(self.insert_sql, self.batch)
            self.conn.commit()
            logger.info(f"Committed {len(self.batch)} records to SQLite")
            self.batch.clear()
            self.last_commit = time.monotonic()
        except sqlite3.Error as e:
            logger.error(f"SQLite commit error: {e}")
            self.batch.clear()
    
    def close(self) -> None:
        """Close database connection."""
        if self.batch:
            self.flush()
        
        if self.conn:
            self.conn.close()
            self.conn = None
            logger.info(f"Closed SQLite DB {self.file_path}")


# ============================================================================
# Storage Manager (Unified Interface)
# ============================================================================

class StorageManager:
    """
    Unified storage interface for exchange collectors.
    
    Supports multiple storage backends simultaneously.
    """
    
    def __init__(self):
        self.backends = []
    
    def add_backend(self, backend) -> None:
        """Add a storage backend."""
        self.backends.append(backend)
    
    def write(self, record: Any) -> None:
        """Write to all backends."""
        for backend in self.backends:
            try:
                backend.write(record)
            except Exception as e:
                logger.error(f"Backend write failed ({type(backend).__name__}): {e}")
    
    def write_batch(self, records: List[Any]) -> None:
        """Write batch to all backends."""
        for backend in self.backends:
            try:
                backend.write_batch(records)
            except Exception as e:
                logger.error(f"Backend batch write failed ({type(backend).__name__}): {e}")
    
    def flush(self) -> None:
        """Flush all backends."""
        for backend in self.backends:
            try:
                backend.flush()
            except Exception as e:
                logger.error(f"Backend flush failed ({type(backend).__name__}): {e}")
    
    def close(self) -> None:
        """Close all backends."""
        for backend in self.backends:
            try:
                if hasattr(backend, 'close'):
                    backend.close()
            except Exception as e:
                logger.error(f"Backend close failed ({type(backend).__name__}): {e}")

