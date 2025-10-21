#!/usr/bin/env python3
"""
Morsel Streaming Operator

Demonstrates how morsels access embedded MarbleDB directly for streaming SQL state.
"""

import asyncio
import logging
from typing import Optional, Dict, Any, List
import pyarrow as pa

logger = logging.getLogger(__name__)


class MorselStreamingOperator:
    """
    Example streaming operator that uses embedded MarbleDB for state management.
    
    This shows how morsels can access MarbleDB directly without client overhead.
    """
    
    def __init__(self, operator_id: str, marbledb=None):
        """
        Initialize streaming operator.
        
        Args:
            operator_id: Unique identifier for this operator
            marbledb: Embedded MarbleDB instance from agent
        """
        self.operator_id = operator_id
        self.marbledb = marbledb
        self.running = False
        
        # State management
        self.state_table_name = f"operator_state_{operator_id}"
        self.watermark_table_name = f"watermarks_{operator_id}"
        
        logger.info(f"MorselStreamingOperator initialized: {operator_id}")
    
    async def initialize(self):
        """Initialize operator with embedded MarbleDB."""
        if not self.marbledb:
            logger.warning("No embedded MarbleDB available, using mock state")
            return
        
        try:
            # Create state table schema
            state_schema = pa.schema([
                ('key', pa.string()),
                ('window_start', pa.int64()),
                ('count', pa.int64()),
                ('sum', pa.float64()),
                ('avg', pa.float64()),
                ('last_update', pa.int64())
            ])
            
            # Create state table (local, not RAFT replicated)
            status = self.marbledb.CreateTable(self.state_table_name, state_schema, False)
            if status.ok():
                logger.info(f"Created state table: {self.state_table_name}")
            else:
                logger.error(f"Failed to create state table: {status.message()}")
            
            # Create watermark table schema
            watermark_schema = pa.schema([
                ('partition_id', pa.int32()),
                ('watermark', pa.int64()),
                ('last_update', pa.int64())
            ])
            
            # Create watermark table (local, not RAFT replicated)
            status = self.marbledb.CreateTable(self.watermark_table_name, watermark_schema, False)
            if status.ok():
                logger.info(f"Created watermark table: {self.watermark_table_name}")
            else:
                logger.error(f"Failed to create watermark table: {status.message()}")
                
        except Exception as e:
            logger.error(f"Failed to initialize operator: {e}")
    
    async def process_batch(self, batch: pa.RecordBatch) -> Optional[pa.RecordBatch]:
        """
        Process a batch of data using embedded MarbleDB state.
        
        Args:
            batch: Input data batch
            
        Returns:
            Processed batch or None if no output
        """
        if not self.running:
            return None
        
        try:
            # Example: Window aggregation with state management
            # This demonstrates direct MarbleDB access from morsels
            
            # 1. Extract watermark from batch
            watermark = self._extract_watermark(batch)
            if watermark:
                await self._update_watermark(0, watermark)
            
            # 2. Process each row in the batch
            results = []
            for i in range(batch.num_rows):
                row_data = self._extract_row_data(batch, i)
                if row_data:
                    # 3. Update state in embedded MarbleDB
                    updated_state = await self._update_state(row_data)
                    if updated_state:
                        results.append(updated_state)
            
            # 4. Return results if any windows are complete
            if results:
                return self._create_result_batch(results)
            
            return None
            
        except Exception as e:
            logger.error(f"Error processing batch: {e}")
            return None
    
    def _extract_watermark(self, batch: pa.RecordBatch) -> Optional[int]:
        """Extract watermark from batch."""
        # Look for timestamp column
        if 'timestamp' in batch.schema.names:
            timestamp_col = batch.column('timestamp')
            if timestamp_col.length > 0:
                # Get max timestamp as watermark
                max_ts = 0
                for i in range(timestamp_col.length):
                    if not timestamp_col.is_null(i):
                        ts = timestamp_col[i].as_py()
                        if isinstance(ts, int):
                            max_ts = max(max_ts, ts)
                return max_ts
        return None
    
    def _extract_row_data(self, batch: pa.RecordBatch, row_idx: int) -> Optional[Dict[str, Any]]:
        """Extract data from a single row."""
        try:
            data = {}
            for i, field in enumerate(batch.schema):
                col = batch.column(i)
                if not col.is_null(row_idx):
                    data[field.name] = col[row_idx].as_py()
                else:
                    data[field.name] = None
            return data
        except Exception as e:
            logger.error(f"Error extracting row data: {e}")
            return None
    
    async def _update_watermark(self, partition_id: int, watermark: int):
        """Update watermark in embedded MarbleDB."""
        if not self.marbledb:
            return
        
        try:
            # Store watermark state
            key = f"watermark_{partition_id}"
            value = str(watermark)
            status = self.marbledb.WriteState(key, value)
            if not status.ok():
                logger.error(f"Failed to update watermark: {status.message()}")
        except Exception as e:
            logger.error(f"Error updating watermark: {e}")
    
    async def _update_state(self, row_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Update operator state in embedded MarbleDB."""
        if not self.marbledb:
            return row_data
        
        try:
            # Example: Simple window aggregation
            key = row_data.get('symbol', 'default')
            timestamp = row_data.get('timestamp', 0)
            value = row_data.get('price', 0.0)
            
            # Calculate window start (1-hour windows)
            window_start = (timestamp // 3600000) * 3600000
            
            # Read current state
            state_key = f"state_{key}_{window_start}"
            result = self.marbledb.ReadState(state_key)
            
            if result.ok():
                # Update existing state
                current_state = result.ValueOrDie()
                if current_state:
                    # Parse existing state (simplified)
                    try:
                        count = int(current_state.get('count', 0))
                        total = float(current_state.get('sum', 0.0))
                    except (ValueError, TypeError):
                        count = 0
                        total = 0.0
                else:
                    count = 0
                    total = 0.0
            else:
                # New state
                count = 0
                total = 0.0
            
            # Update state
            count += 1
            total += value
            avg = total / count if count > 0 else 0.0
            
            new_state = {
                'count': count,
                'sum': total,
                'avg': avg,
                'last_update': timestamp
            }
            
            # Write updated state
            status = self.marbledb.WriteState(state_key, str(new_state))
            if not status.ok():
                logger.error(f"Failed to update state: {status.message()}")
                return None
            
            # Return aggregated result
            return {
                'key': key,
                'window_start': window_start,
                'count': count,
                'sum': total,
                'avg': avg,
                'timestamp': timestamp
            }
            
        except Exception as e:
            logger.error(f"Error updating state: {e}")
            return None
    
    def _create_result_batch(self, results: List[Dict[str, Any]]) -> pa.RecordBatch:
        """Create result batch from aggregated results."""
        try:
            # Convert results to Arrow arrays
            keys = [r['key'] for r in results]
            window_starts = [r['window_start'] for r in results]
            counts = [r['count'] for r in results]
            sums = [r['sum'] for r in results]
            avgs = [r['avg'] for r in results]
            timestamps = [r['timestamp'] for r in results]
            
            # Create schema
            schema = pa.schema([
                ('key', pa.string()),
                ('window_start', pa.int64()),
                ('count', pa.int64()),
                ('sum', pa.float64()),
                ('avg', pa.float64()),
                ('timestamp', pa.int64())
            ])
            
            # Create arrays
            key_array = pa.array(keys)
            window_start_array = pa.array(window_starts)
            count_array = pa.array(counts)
            sum_array = pa.array(sums)
            avg_array = pa.array(avgs)
            timestamp_array = pa.array(timestamps)
            
            # Create batch
            batch = pa.RecordBatch.from_arrays(
                [key_array, window_start_array, count_array, sum_array, avg_array, timestamp_array],
                schema=schema
            )
            
            return batch
            
        except Exception as e:
            logger.error(f"Error creating result batch: {e}")
            return None
    
    async def start(self):
        """Start the operator."""
        self.running = True
        await self.initialize()
        logger.info(f"Operator started: {self.operator_id}")
    
    async def stop(self):
        """Stop the operator."""
        self.running = False
        logger.info(f"Operator stopped: {self.operator_id}")


class MorselStreamingSource:
    """
    Example streaming source that uses embedded MarbleDB for offset management.
    
    This shows how morsels can manage connector state directly.
    """
    
    def __init__(self, source_id: str, marbledb=None):
        """
        Initialize streaming source.
        
        Args:
            source_id: Unique identifier for this source
            marbledb: Embedded MarbleDB instance from agent
        """
        self.source_id = source_id
        self.marbledb = marbledb
        self.running = False
        
        # Offset management
        self.offset_table_name = f"offsets_{source_id}"
        
        logger.info(f"MorselStreamingSource initialized: {source_id}")
    
    async def initialize(self):
        """Initialize source with embedded MarbleDB."""
        if not self.marbledb:
            logger.warning("No embedded MarbleDB available, using mock offsets")
            return
        
        try:
            # Create offset table schema
            offset_schema = pa.schema([
                ('partition', pa.int32()),
                ('offset', pa.int64()),
                ('timestamp', pa.int64()),
                ('last_update', pa.int64())
            ])
            
            # Create offset table (RAFT replicated for fault tolerance)
            status = self.marbledb.CreateTable(self.offset_table_name, offset_schema, True)
            if status.ok():
                logger.info(f"Created offset table: {self.offset_table_name}")
            else:
                logger.error(f"Failed to create offset table: {status.message()}")
                
        except Exception as e:
            logger.error(f"Failed to initialize source: {e}")
    
    async def commit_offset(self, partition: int, offset: int, timestamp: int):
        """Commit offset to embedded MarbleDB."""
        if not self.marbledb:
            return
        
        try:
            # Store offset state
            key = f"offset_{self.source_id}_{partition}"
            value = f"{offset}:{timestamp}"
            status = self.marbledb.WriteState(key, value)
            if not status.ok():
                logger.error(f"Failed to commit offset: {status.message()}")
        except Exception as e:
            logger.error(f"Error committing offset: {e}")
    
    async def get_last_offset(self, partition: int) -> Optional[int]:
        """Get last committed offset from embedded MarbleDB."""
        if not self.marbledb:
            return None
        
        try:
            key = f"offset_{self.source_id}_{partition}"
            result = self.marbledb.ReadState(key)
            if result.ok():
                value = result.ValueOrDie()
                if value:
                    # Parse offset:timestamp
                    parts = value.split(':')
                    if len(parts) >= 1:
                        return int(parts[0])
            return None
        except Exception as e:
            logger.error(f"Error getting last offset: {e}")
            return None
    
    async def start(self):
        """Start the source."""
        self.running = True
        await self.initialize()
        logger.info(f"Source started: {self.source_id}")
    
    async def stop(self):
        """Stop the source."""
        self.running = False
        logger.info(f"Source stopped: {self.source_id}")
