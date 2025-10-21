#!/usr/bin/env python3
"""
SabotSQL Pipeline Step 1: Base Enrichment

Equivalent to Flink SQL 1_base_enrichment.sql
Enriches inventory quotes with security master data using SabotSQL.

Input: inventory CSV, security CSV
Output: Enriched inventory table
"""

import os
import sys
from pathlib import Path

sys.path.insert(0, '/Users/bengamble/Sabot')
os.environ['DYLD_LIBRARY_PATH'] = '/Users/bengamble/Sabot/sabot_sql/build:/Users/bengamble/Sabot/vendor/arrow/cpp/build/install/lib:/Users/bengamble/Sabot/vendor/tonbo/tonbo-ffi/target/release:' + os.environ.get('DYLD_LIBRARY_PATH', '')

from sabot_sql import create_sabot_sql_bridge
import pyarrow as pa
import pyarrow.ipc as ipc
import time


def load_arrow_table(arrow_path: Path, limit=None):
    """Load Arrow IPC file"""
    print(f"Loading {arrow_path.name}...")
    start = time.time()
    
    with pa.memory_map(str(arrow_path), 'r') as source:
        with ipc.open_file(source) as reader:
            batches = []
            rows_read = 0
            
            for i in range(reader.num_record_batches):
                if limit and rows_read >= limit:
                    break
                    
                batch = reader.get_batch(i)
                
                if limit:
                    rows_to_take = min(batch.num_rows, limit - rows_read)
                    if rows_to_take < batch.num_rows:
                        batch = batch.slice(0, rows_to_take)
                
                batches.append(batch)
                rows_read += batch.num_rows
            
            table = pa.Table.from_batches(batches)
    
    elapsed = time.time() - start
    print(f"✅ Loaded {table.num_rows:,} rows in {elapsed:.2f}s")
    return table


def main():
    """
    Base enrichment pipeline using SabotSQL.
    
    Equivalent Flink SQL:
        SELECT i.*, s.ID, s.ISIN, s.NAME, s.ISSUER, s.SECTOR, s.COUPON, ...
        FROM inventory_source i
        LEFT JOIN security_source s ON MOD(i.instrumentId, 50000) = s.ID
        WHERE i.action <> 'DELETE' AND i.price > 0
    """
    print("="*70)
    print("SabotSQL Pipeline Step 1: Base Enrichment")
    print("="*70)
    
    data_dir = Path(__file__).parent.parent
    
    # Load data
    inventory = load_arrow_table(data_dir / "synthetic_inventory.arrow", limit=100000)
    securities = load_arrow_table(data_dir / "master_security_10m.arrow", limit=1000000)
    
    # Rename ID to instrumentId in securities for join compatibility
    if 'ID' in securities.column_names:
        securities = securities.rename_columns(
            ['instrumentId' if c == 'ID' else c for c in securities.column_names]
        )
    
    # Create SabotSQL bridge
    bridge = create_sabot_sql_bridge()
    bridge.register_table("inventory_source", inventory)
    bridge.register_table("security_source", securities)
    
    # Enrichment query using standard LEFT JOIN
    # Note: MOD(i.instrumentId, 50000) = s.instrumentId for synthetic join
    sql = """
    SELECT 
        i.instrumentId,
        i.companyShortName,
        i.companyName,
        i.side,
        i.quoteType,
        i.spread,
        i.tier,
        i.price,
        i.size,
        i.level,
        i.marketSegment,
        i.processedTimestamp,
        i.kafkaCreateTimestamp,
        s.instrumentId as securityId,
        s.ISIN as isin,
        s.NAME as securityName,
        s.ISSUER as issuer,
        s.SECTOR as sector,
        s.COUPON as coupon,
        s.MATURITY as maturity,
        s.FITCHRATING as fitchRating,
        s.SNPRATING as snpRating,
        s.MOODYRATING as moodyRating,
        s.ISINVESTMENTGRADE as isInvestmentGrade
    FROM inventory_source i
    LEFT JOIN security_source s 
        ON i.instrumentId = s.instrumentId
    WHERE i.action <> 'DELETE' AND i.price > 0
    LIMIT 100000
    """
    
    print("\n🔍 Executing enrichment query...")
    start = time.time()
    result = bridge.execute_sql(sql)
    elapsed = time.time() - start
    
    print(f"✅ Enrichment complete in {elapsed:.3f}s")
    print(f"   Input: {inventory.num_rows:,} inventory rows")
    print(f"   Security master: {securities.num_rows:,} rows")
    print(f"   Output: {result.num_rows:,} enriched rows")
    print(f"   Columns: {result.num_columns}")
    
    # Save enriched data for next step
    output_path = data_dir / "enriched_inventory.arrow"
    print(f"\n💾 Saving to {output_path.name}...")
    
    with pa.OSFile(str(output_path), 'wb') as sink:
        with ipc.new_file(sink, result.schema) as writer:
            writer.write_table(result)
    
    print(f"✅ Saved {result.num_rows:,} enriched rows")
    
    # Show sample
    print("\n📊 Sample enriched records:")
    # Arrow-native display without pandas dependency
    sample = result.slice(0, 3)
    sample_dict = sample.to_pydict()
    print("Sample enriched records:")
    for i in range(min(3, sample.num_rows)):
        row = sample.slice(i, 1)
        row_dict = row.to_pydict()
        print(f"Row {i}: instrumentId={row_dict.get('instrumentId', ['N/A'])[0]}, "
              f"side={row_dict.get('side', ['N/A'])[0]}, "
              f"price={row_dict.get('price', ['N/A'])[0]}, "
              f"size={row_dict.get('size', ['N/A'])[0]}, "
              f"securityName={row_dict.get('securityName', ['N/A'])[0]}, "
              f"sector={row_dict.get('sector', ['N/A'])[0]}")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())

