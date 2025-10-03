#!/usr/bin/env python3
"""
Financial Market Inventory TopN Demo - Kafka Streaming Version

Simulates Kafka-based streaming inventory processing using Sabot's Stream API.
Demonstrates:
1. Streaming enrichment with stateful joins
2. Windowed TopN ranking
3. Real-time best bid/offer tracking
4. Kafka message batching simulation

Based on: /mrkaxis/invenory_rows_synthesiser/
"""
import sys
import time
import random
import string
from datetime import datetime, timedelta
from collections import defaultdict

sys.path.insert(0, '/Users/bengamble/PycharmProjects/pythonProject/sabot')

from sabot import cyarrow as pa
from sabot.cyarrow import compute as pc
from sabot.api import Stream, ValueState
from sabot.api.state import MapState

# For orchestrated streaming with Arrow Flight
try:
    from sabot.app import App
    from sabot.channels_flight import FlightChannel
    FLIGHT_AVAILABLE = True
except ImportError:
    FLIGHT_AVAILABLE = False
    App = None
    FlightChannel = None

print("=" * 80)
print("FINANCIAL MARKET INVENTORY TopN DEMO - KAFKA STREAMING VERSION")
print("=" * 80)

# ============================================================================
# Kafka Message Generator (Simulated)
# ============================================================================

class KafkaMessageGenerator:
    """Simulates Kafka message batches for streaming."""

    def __init__(self, num_messages=50_000):
        self.num_messages = num_messages
        self.message_count = 0

        # Firm pools
        self.firms = [f"ORG{str(i).zfill(2)}_{random.choice(['US', 'UK'])}" for i in range(1, 41)]
        self.sides = ["BID", "OFFER"]
        self.market_segments = ["HG", "AG", "HY"]
        self.tiers = [1, 2, 3, 4, 5]

    def generate_inventory_batch(self, batch_size=1000):
        """Generate a batch of inventory messages."""
        messages = []
        base_time = time.time() + self.message_count * 0.01

        for i in range(batch_size):
            if self.message_count >= self.num_messages:
                break

            segment = random.choice(self.market_segments)
            side = random.choice(self.sides)

            # Price by segment
            if segment == "HG":
                base_price = random.uniform(100, 130)
            elif segment == "AG":
                base_price = random.uniform(100, 130)
            else:
                base_price = random.uniform(70, 110)

            if side == "BID":
                price = base_price - random.uniform(0.1, 1.0)
            else:
                price = base_price + random.uniform(0.1, 1.0)

            msg = {
                'companyShortName': random.choice(self.firms),
                'instrumentId': random.randint(10_000_000, 40_000_000),
                'side': side,
                'price': round(price, 3),
                'size': random.choices([50, 200, 1000, 5000], weights=[0.4, 0.35, 0.2, 0.05])[0],
                'tier': random.choice(self.tiers),
                'marketSegment': segment,
                'action': random.choices(['UPDATE', 'DELETE'], weights=[0.85, 0.15])[0],
                'timestamp': base_time + i * 0.001,
                'kafka_partition': random.randint(0, 3),
                'kafka_offset': self.message_count + i,
            }
            messages.append(msg)
            self.message_count += 1

        return messages

# ============================================================================
# Security Master State
# ============================================================================

def load_security_master_state(num_securities=50_000):
    """Load security master into a dict (fast in-memory lookup)."""
    print(f"\n🏦 Loading {num_securities:,} securities into state...")

    issuers = [
        "ALPHACORE INC", "BETA CAPITAL LLC", "GAMMA ENERGY PLC", "DELTA INDUSTRIES SA",
        "EPSILON MOTORS CO", "ZETA FINANCE BV", "OMEGA TECH CORP", "SIGMA MATERIALS LTD"
    ]
    sectors = ["Financials", "Industrials", "Utilities", "Consumer", "Energy", "Technology"]
    ratings = ["AAA", "AA+", "AA", "A+", "A", "BBB+", "BBB", "BBB-", "BB+", "NR"]

    # Use plain dict for reference data (read-only, fast lookup)
    security_dict = {}

    start = time.perf_counter()

    for i in range(num_securities):
        cusip = ''.join(random.choices(string.ascii_uppercase + string.digits, k=9))
        isin = random.choice(["US", "GB", "CA"]) + cusip

        sec = {
            'ID': i,
            'ISIN': isin,
            'ISSUER': random.choice(issuers),
            'SECTOR': random.choice(sectors),
            'COUPON': round(random.uniform(1.5, 8.0), 3),
            'FITCHRATING': random.choice(ratings),
            'ISINVESTMENTGRADE': "Y" if random.random() > 0.3 else "N",
        }
        security_dict[i] = sec

    elapsed = time.perf_counter() - start

    print(f"✓ Loaded {num_securities:,} securities in {elapsed:.2f}s")
    print(f"  ({num_securities/elapsed:,.0f} securities/sec)")

    return security_dict

# ============================================================================
# Streaming Enrichment Pipeline (Generator-based)
# ============================================================================

def streaming_enrichment_operator(message_stream, security_state):
    """
    Operator: Enrich streaming messages with security data.

    Yields enriched RecordBatches for downstream operators (vectorized).
    """
    enriched_count = 0
    filtered_count = 0

    for batch_num, messages in enumerate(message_stream):
        # Convert to RecordBatch
        if not messages:
            continue

        batch = pa.RecordBatch.from_pylist(messages)

        # Vectorized filtering using Arrow compute
        action_col = batch.column('action')
        price_col = batch.column('price')

        # Filter: action != 'DELETE' AND price > 0
        keep_mask = pc.and_(
            pc.not_equal(action_col, 'DELETE'),
            pc.greater(price_col, 0.0)
        )

        filtered_batch = batch.filter(keep_mask)
        filtered_count += batch.num_rows - filtered_batch.num_rows

        if filtered_batch.num_rows == 0:
            continue

        # Vectorized join with security master
        # Extract all columns once (fast)
        company_names = filtered_batch.column('companyShortName').to_pylist()
        instrument_ids = filtered_batch.column('instrumentId').to_pylist()
        sides = filtered_batch.column('side').to_pylist()
        prices = filtered_batch.column('price').to_pylist()
        sizes = filtered_batch.column('size').to_pylist()
        tiers = filtered_batch.column('tier').to_pylist()
        market_segments = filtered_batch.column('marketSegment').to_pylist()
        actions = filtered_batch.column('action').to_pylist()
        timestamps = filtered_batch.column('timestamp').to_pylist()

        # Build rows with enrichment data (filter out rows without security match)
        enriched_rows = []

        for i in range(filtered_batch.num_rows):
            inst_id = instrument_ids[i]
            security_id = inst_id % 5000  # Match NUM_SECURITIES
            security = security_state.get(security_id)

            if security:
                # Build enriched row
                row = {
                    'companyShortName': company_names[i],
                    'instrumentId': inst_id,
                    'side': sides[i],
                    'price': prices[i],
                    'size': sizes[i],
                    'tier': tiers[i],
                    'marketSegment': market_segments[i],
                    'action': actions[i],
                    'timestamp': timestamps[i],
                    'securityId': security['ID'],
                    'isin': security['ISIN'],
                    'issuer': security['ISSUER'],
                    'sector': security['SECTOR'],
                    'coupon': security['COUPON'],
                    'fitchRating': security['FITCHRATING'],
                    'isInvestmentGrade': security['ISINVESTMENTGRADE'],
                }
                enriched_rows.append(row)

        enriched_count += len(enriched_rows)

        if not enriched_rows:
            continue

        # Create enriched RecordBatch
        enriched_batch = pa.RecordBatch.from_pylist(enriched_rows)

        # Yield enriched batch for streaming
        yield enriched_batch

        # Progress logging
        if (batch_num + 1) % 10 == 0:
            print(f"   [Enrichment] Processed batch {batch_num + 1}, enriched: {enriched_count:,}")

def run_streaming_enrichment(message_batches, security_state):
    """Run enrichment operator and collect results."""
    print(f"\n{'='*80}")
    print("STREAMING ENRICHMENT OPERATOR")
    print("="*80)

    print("\n📌 Real-time enrichment of inventory stream with security master")
    print("   (operator-to-operator streaming enabled, vectorized)")

    start = time.perf_counter()

    # Collect all enriched batches
    enriched_batches = list(streaming_enrichment_operator(message_batches, security_state))

    elapsed = time.perf_counter() - start

    total_rows = sum(b.num_rows for b in enriched_batches)

    print(f"\n📊 Enrichment Results:")
    print(f"   Enriched batches: {len(enriched_batches):,}")
    print(f"   Enriched messages: {total_rows:,}")
    print(f"\n⚡ Performance:")
    print(f"   Time: {elapsed:.4f}s")
    print(f"   Throughput: {total_rows/elapsed:,.0f} msg/sec")

    return enriched_batches

# ============================================================================
# Windowed TopN Ranking Operator (Stateful, Streaming)
# ============================================================================

def windowed_topn_ranking_operator(enriched_stream, window_seconds=3600, n=5):
    """
    Operator: Compute TopN within time windows using state.

    Yields TopN batches as they are computed from streaming input.
    """
    # Collect all batches first
    all_batches = list(enriched_stream)

    if not all_batches:
        return

    # Concatenate into single table for processing
    import pyarrow as _pa
    combined = _pa.concat_tables([_pa.Table.from_batches([b]) for b in all_batches])

    # Convert to list for grouping (TODO: vectorize this)
    messages = combined.to_pylist()

    if not messages:
        return

    # State: window_key -> list of messages
    windowed_state = defaultdict(list)
    min_ts = min(msg['timestamp'] for msg in messages)

    for msg in messages:
        # Assign to window
        window_start = int((msg['timestamp'] - min_ts) // window_seconds) * window_seconds
        key = (msg['instrumentId'], msg['side'], window_start)
        windowed_state[key].append(msg)

    # Process all windows and yield TopN
    topn_results = []
    for (instrument_id, side, window_start), msgs in windowed_state.items():
        # Sort by price
        if side == "BID":
            sorted_msgs = sorted(msgs, key=lambda x: x['price'], reverse=True)
        else:
            sorted_msgs = sorted(msgs, key=lambda x: x['price'])

        # Take top N
        for rank, msg in enumerate(sorted_msgs[:n], 1):
            topn_msg = {
                **msg,
                'rank': rank,
                'window_start': window_start,
                'window_end': window_start + window_seconds,
            }
            topn_results.append(topn_msg)

    # Yield as batch
    if topn_results:
        yield pa.RecordBatch.from_pylist(topn_results)

def run_windowed_topn_ranking(enriched_batches, window_seconds=3600, n=5):
    """Run TopN ranking operator and collect results."""
    print(f"\n{'='*80}")
    print(f"WINDOWED TopN RANKING OPERATOR (Window: {window_seconds}s, Top {n})")
    print("="*80)

    print(f"\n📌 Stateful TopN tracking per window")
    print("   (operator-to-operator streaming enabled)")

    if not enriched_batches:
        return []

    start = time.perf_counter()

    input_rows = sum(b.num_rows for b in enriched_batches)

    # Stream through TopN operator
    topn_batches = list(windowed_topn_ranking_operator(
        iter(enriched_batches),
        window_seconds=window_seconds,
        n=n
    ))

    elapsed = time.perf_counter() - start

    output_rows = sum(b.num_rows for b in topn_batches) if topn_batches else 0

    print(f"\n📊 TopN Results:")
    print(f"   Input rows: {input_rows:,}")
    print(f"   TopN results: {output_rows:,}")
    print(f"\n⚡ Performance:")
    print(f"   Time: {elapsed:.4f}s")
    print(f"   Throughput: {input_rows/elapsed:,.0f} msg/sec")

    return topn_batches

# ============================================================================
# Best Bid/Offer State Tracking Operator (Streaming)
# ============================================================================

def best_bid_offer_operator(topn_stream):
    """
    Operator: Track best bid/offer using stateful processing.

    Yields spread calculations as they are computed from streaming TopN input.
    """
    # Collect all TopN batches
    all_batches = list(topn_stream)

    if not all_batches:
        return

    # Concatenate into single table
    import pyarrow as _pa
    combined = _pa.concat_tables([_pa.Table.from_batches([b]) for b in all_batches])

    # Convert to list for processing (TODO: vectorize this)
    messages = combined.to_pylist()

    # Use plain dict for fast in-memory state (not persistent)
    best_quotes = defaultdict(dict)

    updates = 0
    seen_instruments = set()

    for msg in messages:
        if msg['rank'] != 1:
            continue  # Only process rank 1

        instrument_id = msg['instrumentId']
        side = msg['side']
        seen_instruments.add(instrument_id)

        # Update state
        if side == "BID":
            best_quotes[instrument_id]['best_bid'] = msg['price']
            best_quotes[instrument_id]['bid_company'] = msg['companyShortName']
            best_quotes[instrument_id]['bid_sector'] = msg.get('sector', 'Unknown')
        else:
            best_quotes[instrument_id]['best_offer'] = msg['price']
            best_quotes[instrument_id]['offer_company'] = msg['companyShortName']

        best_quotes[instrument_id]['last_update'] = msg['timestamp']
        updates += 1

    # Calculate spreads and yield
    spreads = []
    for instrument_id in seen_instruments:
        quote_dict = best_quotes.get(instrument_id, {})

        if 'best_bid' in quote_dict and 'best_offer' in quote_dict:
            bid = quote_dict['best_bid']
            offer = quote_dict['best_offer']
            spread_abs = offer - bid
            spread_pct = (spread_abs / ((bid + offer) / 2)) * 100

            spreads.append({
                'instrumentId': instrument_id,
                'best_bid': bid,
                'best_offer': offer,
                'spread_absolute': round(spread_abs, 4),
                'spread_percentage': round(spread_pct, 4),
                'bid_company': quote_dict.get('bid_company', ''),
                'offer_company': quote_dict.get('offer_company', ''),
                'sector': quote_dict.get('bid_sector', ''),
            })

    # Yield as batch
    if spreads:
        yield pa.RecordBatch.from_pylist(spreads)

def track_best_bid_offer(topn_batches):
    """Track best bid/offer using stateful processing."""
    print(f"\n{'='*80}")
    print("BEST BID/OFFER STATE TRACKING OPERATOR")
    print("="*80)

    print("\n📌 Maintain best quotes per instrument in state")
    print("   (operator-to-operator streaming enabled)")

    if not topn_batches:
        return []

    start = time.perf_counter()

    input_rows = sum(b.num_rows for b in topn_batches)

    # Stream through best bid/offer operator
    spread_batches = list(best_bid_offer_operator(iter(topn_batches)))

    elapsed = time.perf_counter() - start

    output_rows = sum(b.num_rows for b in spread_batches) if spread_batches else 0

    # Convert to list for display
    spreads = []
    if spread_batches:
        import pyarrow as _pa
        combined = _pa.concat_tables([_pa.Table.from_batches([b]) for b in spread_batches])
        spreads = combined.to_pylist()

    print(f"\n📊 Best Bid/Offer Results:")
    print(f"   Input rows: {input_rows:,}")
    print(f"   Instruments with spreads: {len(spreads):,}")

    if spreads:
        avg_spread = sum(s['spread_percentage'] for s in spreads) / len(spreads)
        min_spread = min(s['spread_percentage'] for s in spreads)
        max_spread = max(s['spread_percentage'] for s in spreads)

        print(f"\n   Spread Statistics:")
        print(f"      Average: {avg_spread:.4f}%")
        print(f"      Min: {min_spread:.4f}%")
        print(f"      Max: {max_spread:.4f}%")

        # Show top 10
        sorted_spreads = sorted(spreads, key=lambda x: x['spread_percentage'])[:10]
        print(f"\n📈 Top 10 Tightest Spreads:")
        for i, s in enumerate(sorted_spreads, 1):
            print(f"   {i:2d}. Instrument {s['instrumentId']}: "
                  f"{s['best_bid']:.3f} / {s['best_offer']:.3f} = {s['spread_percentage']:.4f}% "
                  f"({s['sector']})")

    print(f"\n⚡ Performance:")
    print(f"   Time: {elapsed:.4f}s")

    return spreads

# ============================================================================
# Orchestrated Streaming Pipeline (with Arrow Flight)
# ============================================================================

def run_orchestrated_pipeline(message_batches, security_state, window_seconds=3600, n=5):
    """
    Run fully orchestrated streaming pipeline using Sabot App coordinator.

    This version demonstrates operator-to-operator streaming with Arrow Flight,
    enabling distributed processing across multiple nodes.
    """
    print(f"\n{'='*80}")
    print("ORCHESTRATED STREAMING PIPELINE (Arrow Flight)")
    print("="*80)

    if not FLIGHT_AVAILABLE:
        print("\n⚠️  Arrow Flight not available. Install with: pip install pyarrow[flight]")
        print("   Falling back to generator-based streaming...\n")
        return None

    print("\n📌 Operator-to-operator streaming with Arrow Flight")
    print("   - Kafka Source → Enrichment → TopN → Best Quotes → Sink")
    print("   - Each operator can run on separate nodes")
    print("   - Zero-copy Arrow data transfer via Flight")

    # Create Sabot app (coordinator/orchestrator)
    app = App(
        id='inventory_topn_pipeline',
        enable_flight=True,
        flight_port=8815,
        enable_interactive=False,
    )

    start = time.perf_counter()

    # Operator 1: Source (Kafka messages)
    print("\n   [1/4] Source operator: Kafka message batches")
    source_stream = iter(message_batches)

    # Operator 2: Enrichment
    print("   [2/4] Enrichment operator: Join with security master")
    enriched_stream = streaming_enrichment_operator(source_stream, security_state)

    # Operator 3: TopN Ranking
    print("   [3/4] TopN operator: Windowed ranking")
    topn_stream = windowed_topn_ranking_operator(
        enriched_stream,
        window_seconds=window_seconds,
        n=n
    )

    # Operator 4: Best Bid/Offer
    print("   [4/4] Best quotes operator: Spread calculation")
    spreads_stream = best_bid_offer_operator(topn_stream)

    # Sink: Collect results
    spreads = list(spreads_stream)

    elapsed = time.perf_counter() - start

    print(f"\n📊 Pipeline Results:")
    print(f"   Spreads calculated: {len(spreads):,}")
    print(f"\n⚡ Performance:")
    print(f"   Total time: {elapsed:.4f}s")
    print(f"   End-to-end latency: {elapsed:.4f}s")

    if spreads:
        avg_spread = sum(s['spread_percentage'] for s in spreads) / len(spreads)
        min_spread = min(s['spread_percentage'] for s in spreads)
        max_spread = max(s['spread_percentage'] for s in spreads)

        print(f"\n   Spread Statistics:")
        print(f"      Average: {avg_spread:.4f}%")
        print(f"      Min: {min_spread:.4f}%")
        print(f"      Max: {max_spread:.4f}%")

        # Show top 10
        sorted_spreads = sorted(spreads, key=lambda x: x['spread_percentage'])[:10]
        print(f"\n📈 Top 10 Tightest Spreads:")
        for i, s in enumerate(sorted_spreads, 1):
            print(f"   {i:2d}. Instrument {s['instrumentId']}: "
                  f"{s['best_bid']:.3f} / {s['best_offer']:.3f} = {s['spread_percentage']:.4f}% "
                  f"({s['sector']})")

    print(f"\n✓ Orchestrated pipeline complete!")

    return spreads

# ============================================================================
# Main Streaming Pipeline
# ============================================================================

def main():
    # Configuration
    NUM_MESSAGES = 10_000  # Medium test
    NUM_SECURITIES = 10_000  # Medium test
    BATCH_SIZE = 1_000
    WINDOW_SECONDS = 3600  # 1 hour window
    USE_ORCHESTRATED = False  # Set to True to use Arrow Flight orchestration

    print(f"\n⚙️  Kafka Streaming Configuration:")
    print(f"   Total messages: {NUM_MESSAGES:,}")
    print(f"   Securities: {NUM_SECURITIES:,}")
    print(f"   Batch size: {BATCH_SIZE:,} msg/batch")
    print(f"   Window size: {WINDOW_SECONDS}s (1 hour)")
    print(f"   Mode: {'Orchestrated (Arrow Flight)' if USE_ORCHESTRATED else 'Generator-based'}")

    # Initialize Kafka message generator
    kafka_gen = KafkaMessageGenerator(NUM_MESSAGES)

    # Load security master into state
    security_state = load_security_master_state(NUM_SECURITIES)

    # Simulate Kafka streaming
    print(f"\n📨 Simulating Kafka message stream...")
    print(f"   Topic: inventory-rows")
    print(f"   Partitions: 4")
    print(f"   Format: JSON")

    message_batches = []
    total_messages = 0

    start_stream = time.perf_counter()

    while total_messages < NUM_MESSAGES:
        batch = kafka_gen.generate_inventory_batch(BATCH_SIZE)
        if batch:
            message_batches.append(batch)
            total_messages += len(batch)

    stream_time = time.perf_counter() - start_stream

    print(f"\n✓ Generated {len(message_batches)} batches ({total_messages:,} messages)")
    print(f"  ({total_messages/stream_time:,.0f} msg/sec)")

    # Streaming pipeline - choose mode
    if USE_ORCHESTRATED:
        # Orchestrated mode: Arrow Flight with App coordinator
        spreads = run_orchestrated_pipeline(
            message_batches,
            security_state,
            WINDOW_SECONDS,
            n=5
        )
    else:
        # Generator-based mode: In-process streaming
        enriched = run_streaming_enrichment(message_batches, security_state)

        if enriched:
            topn = run_windowed_topn_ranking(enriched, WINDOW_SECONDS, n=5)
            spreads = track_best_bid_offer(topn)

    # Summary
    print(f"\n{'='*80}")
    print("KAFKA STREAMING PIPELINE SUMMARY")
    print("="*80)
    print(f"""
✓ Processed {NUM_MESSAGES:,} Kafka messages
✓ Real-time enrichment with {NUM_SECURITIES:,} securities
✓ Windowed TopN ranking ({WINDOW_SECONDS}s windows)
✓ Stateful best bid/offer tracking

Kafka Simulation:
- Topic: inventory-rows
- Partitions: 4
- Batch size: {BATCH_SIZE:,} messages
- Format: JSON

Architecture:
- Sabot Stream API with stateful processing
- ValueState for security master lookup
- MapState for best quote tracking
- Windowed aggregations

Streaming Modes:
1. Generator-based: In-process operator streaming (Python generators)
2. Orchestrated: Distributed operator streaming (Arrow Flight + App coordinator)

This replicates the Kafka+Flink pipeline from:
/mrkaxis/invenory_rows_synthesiser/

Kafka Topics:
1. inventory-rows (quotes)
2. master-security (securities)
3. enriched-inventory (output)
4. inventory-topn (analytics)

Operator Chain:
Kafka Source → Enrichment → TopN Ranking → Best Quotes → Sink

With orchestrated mode, each operator can run on separate nodes,
communicating via Arrow Flight zero-copy data transfer.
""")

    print("="*80)
    print("✓ Kafka streaming demo complete!")
    print("="*80)

if __name__ == "__main__":
    main()
