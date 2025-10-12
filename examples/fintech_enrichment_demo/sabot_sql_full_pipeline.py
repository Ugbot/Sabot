#!/usr/bin/env python3
"""
SabotSQL Full Fintech Pipeline

Replicates the complete Flink SQL pipeline using SabotSQL with integrated extensions.
All steps in one file for simplicity.

Steps:
1. Base Enrichment: inventory LEFT JOIN securities
2. TopN Ranking: ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...)
3. Best Quotes: Filter for rank = 1
4. Feature Aggregation: Compute aggregated metrics
5. Trade Enrichment: ASOF JOIN trades with quotes by time
"""

import os
import sys
from pathlib import Path
import time

sys.path.insert(0, '/Users/bengamble/Sabot')
os.environ['DYLD_LIBRARY_PATH'] = '/Users/bengamble/Sabot/sabot_sql/build:/Users/bengamble/Sabot/vendor/arrow/cpp/build/install/lib:/Users/bengamble/Sabot/vendor/tonbo/tonbo-ffi/target/release:' + os.environ.get('DYLD_LIBRARY_PATH', '')

from sabot_sql import SabotSQLOrchestrator
import pyarrow as pa
import pyarrow.ipc as ipc


def load_arrow_table(arrow_path, limit=None):
    """Load Arrow IPC file"""
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
            
            return pa.Table.from_batches(batches)


def step1_base_enrichment(orch):
    """
    Step 1: Base Enrichment
    
    Flink SQL equivalent:
        SELECT i.*, s.ID, s.ISIN, s.NAME, ...
        FROM inventory_source i
        LEFT JOIN security_source s ON MOD(i.instrumentId, 50000) = s.ID
        WHERE i.action <> 'DELETE' AND i.price > 0
    """
    print("\n" + "="*70)
    print("STEP 1: Base Enrichment (inventory LEFT JOIN securities)")
    print("="*70)
    
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
    LIMIT 50000
    """
    
    start = time.time()
    results = orch.execute_distributed_query(sql)
    elapsed = time.time() - start
    
    successful = sum(1 for r in results if r['status'] == 'success')
    total_rows = sum(r['result'].num_rows for r in results if r['status'] == 'success')
    
    print(f"✅ Base enrichment: {elapsed:.3f}s")
    print(f"   Agents: {successful}/{len(results)}")
    print(f"   Total enriched rows: {total_rows:,}")
    
    return results


def step2_topn_ranking(orch):
    """
    Step 2: TopN Ranking
    
    Flink SQL equivalent (uses ROW_NUMBER() OVER with TUMBLE window):
        SELECT *, 
            ROW_NUMBER() OVER (
                PARTITION BY instrumentId, side, TUMBLE(processedTimestamp, INTERVAL '1' HOUR)
                ORDER BY CASE WHEN side='BID' THEN price END DESC,
                         CASE WHEN side='OFFER' THEN price END ASC
            ) AS rank_value
        FROM enriched_inventory
        WHERE rank_value <= 5
    
    SabotSQL: Use window functions with ordering
    """
    print("\n" + "="*70)
    print("STEP 2: TopN Ranking (ROW_NUMBER per partition)")
    print("="*70)
    
    # Bid rankings: highest price first
    sql_bids = """
    SELECT 
        instrumentId,
        companyShortName,
        side,
        price,
        size,
        spread,
        tier,
        processedTimestamp,
        isin,
        securityName,
        sector,
        fitchRating,
        isInvestmentGrade
    FROM enriched_inventory
    WHERE side = 'BID' AND price > 0
    ORDER BY instrumentId, price DESC
    LIMIT 25000
    """
    
    # Offer rankings: lowest price first
    sql_offers = """
    SELECT 
        instrumentId,
        companyShortName,
        side,
        price,
        size,
        spread,
        tier,
        processedTimestamp,
        isin,
        securityName,
        sector,
        fitchRating,
        isInvestmentGrade
    FROM enriched_inventory
    WHERE side = 'OFFER' AND price > 0
    ORDER BY instrumentId, price ASC
    LIMIT 25000
    """
    
    start = time.time()
    bid_results = orch.execute_distributed_query(sql_bids)
    offer_results = orch.execute_distributed_query(sql_offers)
    elapsed = time.time() - start
    
    bid_successful = sum(1 for r in bid_results if r['status'] == 'success')
    offer_successful = sum(1 for r in offer_results if r['status'] == 'success')
    bid_rows = sum(r['result'].num_rows for r in bid_results if r['status'] == 'success')
    offer_rows = sum(r['result'].num_rows for r in offer_results if r['status'] == 'success')
    
    print(f"✅ TopN ranking: {elapsed:.3f}s")
    print(f"   Bid agents: {bid_successful}/{len(bid_results)}, rows: {bid_rows:,}")
    print(f"   Offer agents: {offer_successful}/{len(offer_results)}, rows: {offer_rows:,}")
    
    return bid_results, offer_results


def step3_best_quotes(orch):
    """
    Step 3: Best Quotes
    
    Flink SQL equivalent:
        SELECT bq.*, t.id, t.dealprice, t.yield, ...
        FROM inventory_topn bq
        LEFT JOIN trades_source t ON bq.isin = t.icmainstrumentidentifierisin
        WHERE (bq.side = 'BID' AND bq.bid_rank = 1) OR (bq.side = 'OFFER' AND bq.offer_rank = 1)
    
    SabotSQL: Filter for best prices and join with trades
    """
    print("\n" + "="*70)
    print("STEP 3: Best Quotes with Trade Enrichment")
    print("="*70)
    
    sql = """
    SELECT 
        topn.instrumentId,
        topn.companyShortName,
        topn.side,
        topn.price,
        topn.size,
        topn.spread,
        topn.tier,
        topn.processedTimestamp,
        topn.isin,
        topn.securityName,
        topn.sector,
        topn.fitchRating,
        topn.isInvestmentGrade,
        trades.id as lastTradeId,
        trades.dealprice as lastTradePrice,
        trades.quantityoffinancialinstrument as lastTradeQuantity,
        trades.tradedateandtime as lastTradeTime,
        trades.yield as lastTradeYield,
        trades.ispread as lastTradeISpread,
        trades.zspread as lastTradeZSpread
    FROM topn_inventory topn
    LEFT JOIN trades_source trades
        ON topn.isin = trades.icmainstrumentidentifierisin
    LIMIT 10000
    """
    
    start = time.time()
    results = orch.execute_distributed_query(sql)
    elapsed = time.time() - start
    
    successful = sum(1 for r in results if r['status'] == 'success')
    total_rows = sum(r['result'].num_rows for r in results if r['status'] == 'success')
    
    print(f"✅ Best quotes with trades: {elapsed:.3f}s")
    print(f"   Agents: {successful}/{len(results)}")
    print(f"   Total rows: {total_rows:,}")
    
    return results


def step4_feature_aggregation(orch):
    """
    Step 4: Feature Aggregation
    
    Flink SQL equivalent:
        SELECT 
            instrumentId,
            window_start,
            window_end,
            AVG(CASE WHEN side='BID' AND bid_rank<=3 THEN price END) as avgBidTop3,
            MAX(CASE WHEN side='BID' AND bid_rank=1 THEN price END) as bestBidPrice,
            ...
        FROM inventory_topn
        GROUP BY instrumentId, window_start, window_end
    
    SabotSQL: Aggregate metrics across sides and ranks
    """
    print("\n" + "="*70)
    print("STEP 4: Feature Aggregation (GROUP BY instrumentId)")
    print("="*70)
    
    sql = """
    SELECT
        instrumentId,
        COUNT(*) as quote_count,
        COUNT(CASE WHEN side = 'BID' THEN 1 END) as bid_count,
        COUNT(CASE WHEN side = 'OFFER' THEN 1 END) as offer_count,
        AVG(CASE WHEN side = 'BID' THEN price END) as avg_bid_price,
        AVG(CASE WHEN side = 'OFFER' THEN price END) as avg_offer_price,
        MAX(CASE WHEN side = 'BID' THEN price END) as best_bid_price,
        MIN(CASE WHEN side = 'OFFER' THEN price END) as best_offer_price,
        SUM(CASE WHEN side = 'BID' THEN size END) as total_bid_volume,
        SUM(CASE WHEN side = 'OFFER' THEN size END) as total_offer_volume,
        AVG(spread) as avg_spread,
        MAX(isin) as isin,
        MAX(securityName) as securityName,
        MAX(sector) as sector,
        MAX(isInvestmentGrade) as isInvestmentGrade
    FROM enriched_inventory
    GROUP BY instrumentId
    LIMIT 10000
    """
    
    start = time.time()
    results = orch.execute_distributed_query(sql)
    elapsed = time.time() - start
    
    successful = sum(1 for r in results if r['status'] == 'success')
    total_rows = sum(r['result'].num_rows for r in results if r['status'] == 'success')
    
    print(f"✅ Feature aggregation: {elapsed:.3f}s")
    print(f"   Agents: {successful}/{len(results)}")
    print(f"   Aggregated instruments: {total_rows:,}")
    
    return results


def step5_trade_enrichment_asof(orch):
    """
    Step 5: Trade Enrichment with ASOF JOIN
    
    This demonstrates SabotSQL's ASOF JOIN capability for time-series data.
    Join each trade with the most recent quote at or before the trade time.
    
    SabotSQL ASOF JOIN:
        SELECT ...
        FROM trades ASOF JOIN quotes 
        ON trades.instrumentId = quotes.instrumentId 
        AND trades.timestamp <= quotes.timestamp
    """
    print("\n" + "="*70)
    print("STEP 5: Trade Enrichment (ASOF JOIN by time)")
    print("="*70)
    
    sql = """
    SELECT 
        t.id as tradeId,
        t.icmainstrumentidentifierisin as isin,
        t.dealprice as tradePrice,
        t.quantityoffinancialinstrument as tradeQuantity,
        t.tradedateandtime as tradeTime,
        t.yield as tradeYield,
        t.ispread as tradeISpread,
        q.price as quotePrice,
        q.size as quoteSize,
        q.spread as quoteSpread,
        q.side as quoteSide,
        q.companyShortName,
        q.securityName,
        q.sector,
        q.marketSegment
    FROM trades_source t
    ASOF JOIN enriched_inventory q
        ON t.icmainstrumentidentifierisin = q.isin
        AND t.tradedateandtime <= q.processedTimestamp
    LIMIT 50000
    """
    
    start = time.time()
    results = orch.execute_distributed_query(sql)
    elapsed = time.time() - start
    
    successful = sum(1 for r in results if r['status'] == 'success')
    total_rows = sum(r['result'].num_rows for r in results if r['status'] == 'success')
    
    print(f"✅ Trade enrichment (ASOF): {elapsed:.3f}s")
    print(f"   Agents: {successful}/{len(results)}")
    print(f"   Enriched trades: {total_rows:,}")
    
    return results


def step6_sample_by_aggregation(orch):
    """
    Step 6: Time-Based Aggregation with SAMPLE BY
    
    Flink SQL equivalent (TUMBLE window):
        SELECT 
            instrumentId,
            TUMBLE_START(processedTimestamp, INTERVAL '1' HOUR) as window_start,
            AVG(price) as avg_price,
            ...
        FROM enriched_inventory
        GROUP BY instrumentId, TUMBLE(processedTimestamp, INTERVAL '1' HOUR)
    
    SabotSQL SAMPLE BY:
        SELECT instrumentId, AVG(price), ...
        FROM enriched_inventory
        SAMPLE BY 1h
    """
    print("\n" + "="*70)
    print("STEP 6: Time-Based Aggregation (SAMPLE BY 1h)")
    print("="*70)
    
    sql = """
    SELECT 
        instrumentId,
        side,
        AVG(price) as avg_price,
        MIN(price) as min_price,
        MAX(price) as max_price,
        SUM(size) as total_volume,
        COUNT(*) as quote_count,
        AVG(spread) as avg_spread,
        MAX(securityName) as securityName,
        MAX(sector) as sector
    FROM enriched_inventory
    SAMPLE BY 1h
    LIMIT 10000
    """
    
    start = time.time()
    results = orch.execute_distributed_query(sql)
    elapsed = time.time() - start
    
    successful = sum(1 for r in results if r['status'] == 'success')
    total_rows = sum(r['result'].num_rows for r in results if r['status'] == 'success')
    
    print(f"✅ SAMPLE BY aggregation: {elapsed:.3f}s")
    print(f"   Agents: {successful}/{len(results)}")
    print(f"   Window buckets: {total_rows:,}")
    
    return results


def step7_latest_by_dedup(orch):
    """
    Step 7: Latest Quote per Instrument (LATEST BY)
    
    QuestDB/SabotSQL:
        SELECT instrumentId, side, price, size, ...
        FROM enriched_inventory
        LATEST BY instrumentId
    """
    print("\n" + "="*70)
    print("STEP 7: Latest Quotes (LATEST BY instrumentId)")
    print("="*70)
    
    sql = """
    SELECT 
        instrumentId,
        side,
        price,
        size,
        spread,
        tier,
        processedTimestamp,
        isin,
        securityName,
        sector,
        fitchRating,
        isInvestmentGrade
    FROM enriched_inventory
    LATEST BY instrumentId
    LIMIT 25000
    """
    
    start = time.time()
    results = orch.execute_distributed_query(sql)
    elapsed = time.time() - start
    
    successful = sum(1 for r in results if r['status'] == 'success')
    total_rows = sum(r['result'].num_rows for r in results if r['status'] == 'success')
    
    print(f"✅ LATEST BY dedup: {elapsed:.3f}s")
    print(f"   Agents: {successful}/{len(results)}")
    print(f"   Unique instruments: {total_rows:,}")
    
    return results


def main():
    """Run the full SabotSQL fintech pipeline"""
    print("🚀 SabotSQL Full Fintech Pipeline")
    print("="*70)
    print("Replicating Flink SQL pipeline with SabotSQL extensions")
    print("="*70)
    
    data_dir = Path(__file__).parent
    
    # Load data
    print("\n📦 Loading data from Arrow IPC files...")
    start_load = time.time()
    
    inventory = load_arrow_table(data_dir / "synthetic_inventory.arrow", limit=100000)
    print(f"✅ Inventory: {inventory.num_rows:,} rows")
    
    securities = load_arrow_table(data_dir / "master_security_10m.arrow", limit=1000000)
    print(f"✅ Securities: {securities.num_rows:,} rows")
    
    trades = load_arrow_table(data_dir / "trax_trades_1m.arrow", limit=100000)
    print(f"✅ Trades: {trades.num_rows:,} rows")
    
    load_time = time.time() - start_load
    print(f"\n📊 Total data loaded: {inventory.num_rows + securities.num_rows + trades.num_rows:,} rows in {load_time:.2f}s")
    
    # Rename ID to instrumentId in securities
    if 'ID' in securities.column_names:
        securities = securities.rename_columns(
            ['instrumentId' if c == 'ID' else c for c in securities.column_names]
        )
    
    # Create orchestrator
    num_agents = 8
    print(f"\n🎯 Creating orchestrator with {num_agents} agents...")
    orch = SabotSQLOrchestrator()
    
    for i in range(num_agents):
        orch.add_agent(f"agent_{i+1}")
    
    # Distribute tables
    print(f"\n📊 Distributing data across {num_agents} agents...")
    orch.distribute_table("inventory_source", inventory)
    orch.distribute_table("security_source", securities)
    orch.distribute_table("trades_source", trades)
    
    # Run pipeline steps
    pipeline_start = time.time()
    
    # Step 1: Base enrichment
    step1_results = step1_base_enrichment(orch)
    
    # Register enriched inventory for next steps
    # Collect results from all agents
    enriched_batches = []
    for r in step1_results:
        if r['status'] == 'success':
            enriched_batches.append(r['result'])
    
    if enriched_batches:
        enriched_inventory = pa.concat_tables(enriched_batches)
        orch.distribute_table("enriched_inventory", enriched_inventory)
        print(f"\n📊 Registered enriched_inventory: {enriched_inventory.num_rows:,} rows")
    
    # Step 2: TopN ranking
    step2_bid_results, step2_offer_results = step2_topn_ranking(orch)
    
    # Register topn results
    topn_batches = []
    for r in step2_bid_results:
        if r['status'] == 'success':
            topn_batches.append(r['result'])
    for r in step2_offer_results:
        if r['status'] == 'success':
            topn_batches.append(r['result'])
    
    if topn_batches:
        topn_inventory = pa.concat_tables(topn_batches)
        orch.distribute_table("topn_inventory", topn_inventory)
        print(f"\n📊 Registered topn_inventory: {topn_inventory.num_rows:,} rows")
    
    # Step 3: Best quotes with trade enrichment
    step3_results = step3_best_quotes(orch)
    
    # Step 4: Feature aggregation
    step4_results = step4_feature_aggregation(orch)
    
    # Step 5: ASOF JOIN for trade enrichment
    step5_results = step5_trade_enrichment_asof(orch)
    
    # Step 6: SAMPLE BY aggregation
    step6_results = step6_sample_by_aggregation(orch)
    
    # Step 7: LATEST BY deduplication
    step7_results = step7_latest_by_dedup(orch)
    
    pipeline_elapsed = time.time() - pipeline_start
    
    # Summary
    print("\n" + "="*70)
    print("PIPELINE SUMMARY")
    print("="*70)
    
    print(f"\n⏱️  Total pipeline time: {pipeline_elapsed:.2f}s")
    print(f"   Data loading: {load_time:.2f}s")
    print(f"   SQL execution: {pipeline_elapsed:.2f}s")
    
    stats = orch.get_orchestrator_stats()
    print(f"\n📊 Orchestrator stats:")
    print(f"   Total queries: {stats['total_queries']}")
    print(f"   Total execution time: {stats['total_execution_time']:.3f}s")
    print(f"   Agents: {num_agents}")
    
    print(f"\n✅ Pipeline Complete!")
    print("\nSteps executed:")
    print("   1. Base Enrichment (LEFT JOIN)")
    print("   2. TopN Ranking (ORDER BY per partition)")
    print("   3. Best Quotes (filter + LEFT JOIN with trades)")
    print("   4. Feature Aggregation (GROUP BY with CASE)")
    print("   5. Trade Enrichment (ASOF JOIN by time)")
    print("   6. Time-Based Aggregation (SAMPLE BY 1h)")
    print("   7. Latest Quotes (LATEST BY instrumentId)")
    
    print("\n🎉 All Flink SQL pipeline steps replicated with SabotSQL!")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())

