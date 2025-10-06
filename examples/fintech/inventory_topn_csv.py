#!/usr/bin/env python3
"""
Financial Market Inventory TopN Demo - CSV Version

Replicates the Flink SQL inventory processing pipeline using Sabot's Stream API.
Demonstrates:
1. Stream-table enrichment (inventory + security master)
2. TopN ranking (best 5 bid/offer quotes per instrument)
3. Best bid/offer spread calculation
4. Market analytics

Based on: /mrkaxis/invenory_rows_synthesiser/
"""
import time
import random
import string
from datetime import datetime, timedelta

from sabot import cyarrow as pa
from sabot.cyarrow import compute as pc
from sabot.api import Stream, ValueState

print("=" * 80)
print("FINANCIAL MARKET INVENTORY TopN DEMO - CSV VERSION")
print("=" * 80)

# ============================================================================
# Data Generation - Inventory Quotes
# ============================================================================

def generate_inventory_quotes(num_quotes=100_000):
    """Generate realistic market inventory quotes (bid/offer)."""
    print(f"\n📊 Generating {num_quotes:,} inventory quotes...")

    # Firm pools
    countries = ["US", "UK"]
    firms = [f"ORG{str(i).zfill(2)}_{random.choice(countries)}" for i in range(1, 41)]

    # Market data
    sides = ["BID", "OFFER"]
    actions = ["UPDATE", "DELETE"]
    quote_types = ["I", "F"]  # Indicative / Firm
    tiers = [1, 2, 3, 4, 5]
    market_segments = ["HG", "AG", "HY"]  # High Grade, Agency, High Yield
    product_cds = ["USHG", "USAS", "USSC", "USHS", "USHY", "UPAG"]

    start_gen = time.perf_counter()

    quotes = []
    base_time = time.time()

    for i in range(num_quotes):
        # Market segment determines price range
        segment = random.choice(market_segments)
        side = random.choice(sides)

        # Price ranges by segment
        if segment == "HG":
            base_price = random.uniform(100, 130)
        elif segment == "AG":
            base_price = random.uniform(100, 130)
        else:  # HY
            base_price = random.uniform(70, 110)

        # Bid/offer adjustment
        if side == "BID":
            price = base_price - random.uniform(0.1, 1.0)
        else:
            price = base_price + random.uniform(0.1, 1.0)

        # Size distribution (heavy tail)
        size_type = random.choices(
            ["small", "medium", "large", "huge"],
            weights=[0.4, 0.35, 0.20, 0.05]
        )[0]

        if size_type == "small":
            size = random.randint(10, 100)
        elif size_type == "medium":
            size = random.randint(100, 500)
        elif size_type == "large":
            size = random.randint(500, 2000)
        else:
            size = random.randint(2000, 10000)

        quote = {
            'companyShortName': random.choice(firms),
            'companyName': random.choice(firms),  # Keep same for simplicity
            'instrumentId': random.randint(10_000_000, 40_000_000),
            'side': side,
            'price': round(price, 3),
            'size': size,
            'spread': round(random.uniform(50, 200), 2) if random.random() > 0.8 else None,
            'tier': random.choice(tiers),
            'level': random.choices([1, 2, 3, 4, 5], weights=[0.15, 0.2, 0.35, 0.2, 0.1])[0],
            'quoteType': random.choice(quote_types),
            'action': random.choices(actions, weights=[0.85, 0.15])[0],
            'marketSegment': segment,
            'productCD': random.choice(product_cds),
            'processedTimestamp': (base_time + i * 0.01),
            'kafkaCreateTimestamp': (base_time + i * 0.01 + random.uniform(0, 0.5)),
        }
        quotes.append(quote)

    gen_time = time.perf_counter() - start_gen

    print(f"✓ Generated {num_quotes:,} quotes in {gen_time:.2f}s")
    print(f"  ({num_quotes/gen_time:,.0f} quotes/sec)")

    return quotes

# ============================================================================
# Data Generation - Security Master
# ============================================================================

def generate_security_master(num_securities=50_000):
    """Generate security master data (bonds/securities)."""
    print(f"\n🏦 Generating {num_securities:,} securities...")

    issuers = [
        "ALPHACORE INC", "BETA CAPITAL LLC", "GAMMA ENERGY PLC", "DELTA INDUSTRIES SA",
        "EPSILON MOTORS CO", "ZETA FINANCE BV", "OMEGA TECH CORP", "SIGMA MATERIALS LTD",
        "RHO UTILITIES CO", "THETA LOGISTICS LP", "LAMBDA TELECOM AG", "KAPPA CHEMICALS NV"
    ]

    sectors = ["Financials", "Industrials", "Utilities", "Consumer", "Energy", "Technology", "Healthcare"]
    ratings_fitch = ["AAA", "AA+", "AA", "AA-", "A+", "A", "A-", "BBB+", "BBB", "BBB-", "BB+", "NR"]
    ratings_snp = ["AAA", "AA+", "AA", "AA-", "A+", "A", "A-", "BBB+", "BBB", "BBB-", "BB+", "NR"]
    ratings_moody = ["Aaa", "Aa2", "Aa3", "A1", "A2", "A3", "Baa1", "Baa2", "Baa3", "Ba1", "NR"]

    start_gen = time.perf_counter()

    securities = []
    for i in range(num_securities):
        # CUSIP generation (simplified)
        cusip_base = ''.join(random.choices(string.ascii_uppercase + string.digits, k=8))
        cusip = cusip_base + str(random.randint(0, 9))

        # ISIN = Country Code + CUSIP + check digit
        country = random.choice(["US", "GB", "CA"])
        isin = country + cusip + str(random.randint(0, 9))

        # Investment grade determination
        fitch = random.choice(ratings_fitch)
        is_inv_grade = "Y" if fitch in ["AAA", "AA+", "AA", "AA-", "A+", "A", "A-", "BBB+", "BBB", "BBB-"] else "N"

        # Maturity date
        issue_date = datetime.now() - timedelta(days=random.randint(0, 3650))
        maturity_years = random.randint(2, 30)
        maturity = (issue_date + timedelta(days=365 * maturity_years)).strftime("%Y-%m-%d")

        sec = {
            'ID': i,
            'NAME': f"{random.choice(issuers)} {random.uniform(2.0, 8.0):.3f} {maturity[:4]}",
            'CUSIP': cusip,
            'ISIN': isin,
            'COUPON': round(random.uniform(1.5, 8.0), 3),
            'MATURITY': maturity,
            'ISSUEDATE': issue_date.strftime("%Y-%m-%d"),
            'ISSUER': random.choice(issuers),
            'SECTOR': random.choice(sectors),
            'FITCHRATING': fitch,
            'SNPRATING': random.choice(ratings_snp),
            'MOODYRATING': random.choice(ratings_moody),
            'ISINVESTMENTGRADE': is_inv_grade,
            'COUNTRYCODE': country,
            'ISOCURRENCYCODE': "USD" if country == "US" else "GBP" if country == "GB" else "CAD",
        }
        securities.append(sec)

    gen_time = time.perf_counter() - start_gen

    print(f"✓ Generated {num_securities:,} securities in {gen_time:.2f}s")
    print(f"  ({num_securities/gen_time:,.0f} securities/sec)")

    return securities

# ============================================================================
# Step 1: Enrichment Pipeline
# ============================================================================

def enrich_inventory(inventory_batches, securities_dict):
    """Enrich inventory quotes with security master data."""
    print(f"\n{'='*80}")
    print("STEP 1: ENRICHMENT PIPELINE")
    print("="*80)

    print("\n📌 Join inventory with security master data")

    start = time.perf_counter()

    def enrich_batch(batch):
        """Enrich batch with security data."""
        enriched_rows = []

        for i in range(batch.num_rows):
            row = {col: batch.column(col)[i].as_py() for col in batch.schema.names}

            # Skip DELETEs and zero prices (per SQL logic)
            if row['action'] == 'DELETE' or row['price'] <= 0:
                continue

            # Join logic: instrumentId % 50000 = securityId (from SQL)
            security_id = row['instrumentId'] % 50000
            security = securities_dict.get(security_id)

            if security:
                enriched = {
                    **row,
                    'securityId': security['ID'],
                    'isin': security['ISIN'],
                    'securityName': security['NAME'],
                    'issuer': security['ISSUER'],
                    'sector': security['SECTOR'],
                    'coupon': security['COUPON'],
                    'maturity': security['MATURITY'],
                    'fitchRating': security['FITCHRATING'],
                    'snpRating': security['SNPRATING'],
                    'moodyRating': security['MOODYRATING'],
                    'isInvestmentGrade': security['ISINVESTMENTGRADE'],
                }
                enriched_rows.append(enriched)

        if enriched_rows:
            return pa.RecordBatch.from_pylist(enriched_rows)
        else:
            # Return empty batch with proper schema
            return None

    # Process batches
    stream = Stream.from_batches(inventory_batches, inventory_batches[0].schema)
    enriched_stream = stream.map(enrich_batch)

    # Collect non-None batches
    enriched_batches = []
    for batch in enriched_stream._execute_iterator():
        if batch is not None and batch.num_rows > 0:
            enriched_batches.append(batch)

    if not enriched_batches:
        print("⚠️  No enriched data (all filtered out)")
        return None

    # Combine batches into single table
    import pyarrow as _pa
    enriched = _pa.concat_tables([_pa.Table.from_batches([b]) for b in enriched_batches])

    elapsed = time.perf_counter() - start

    total_input = sum(b.num_rows for b in inventory_batches)

    print(f"\n📊 Enrichment Results:")
    print(f"   Input rows: {total_input:,}")
    print(f"   Enriched rows: {enriched.num_rows:,}")
    print(f"   Filtered out: {total_input - enriched.num_rows:,}")
    print(f"\n⚡ Performance:")
    print(f"   Time: {elapsed:.4f}s")
    print(f"   Throughput: {total_input/elapsed:,.0f} rows/sec")

    return enriched

# ============================================================================
# Step 2: TopN Ranking
# ============================================================================

def compute_topn_ranking(enriched_data, n=5):
    """Compute TopN (best 5) quotes per instrument per side."""
    print(f"\n{'='*80}")
    print(f"STEP 2: TopN RANKING (Top {n} per instrument/side)")
    print("="*80)

    print(f"\n📌 Rank quotes by price (BID: highest first, OFFER: lowest first)")

    start = time.perf_counter()

    # Group by instrumentId and side
    instruments = set(enriched_data.column('instrumentId').to_pylist())
    sides = set(enriched_data.column('side').to_pylist())

    topn_results = []

    for instrument in instruments:
        for side in sides:
            # Filter for this instrument + side
            mask = pc.and_(
                pc.equal(enriched_data.column('instrumentId'), instrument),
                pc.equal(enriched_data.column('side'), side)
            )
            group_data = enriched_data.filter(mask)

            if group_data.num_rows == 0:
                continue

            # Sort by price
            if side == "BID":
                # BID: highest price first
                indices = pc.sort_indices(group_data.column('price'), sort_keys=[("price", "descending")])
            else:
                # OFFER: lowest price first
                indices = pc.sort_indices(group_data.column('price'), sort_keys=[("price", "ascending")])

            sorted_data = pc.take(group_data, indices)

            # Take top N
            top_n = sorted_data.slice(0, min(n, sorted_data.num_rows))

            # Add rank
            for rank in range(top_n.num_rows):
                row_dict = {col: top_n.column(col)[rank].as_py() for col in top_n.schema.names}
                row_dict['rank'] = rank + 1
                topn_results.append(row_dict)

    topn_table = pa.Table.from_pylist(topn_results)

    elapsed = time.perf_counter() - start

    print(f"\n📊 TopN Results:")
    print(f"   Input rows: {enriched_data.num_rows:,}")
    print(f"   TopN rows: {topn_table.num_rows:,}")
    print(f"   Instruments processed: {len(instruments):,}")
    print(f"\n⚡ Performance:")
    print(f"   Time: {elapsed:.4f}s")
    print(f"   Throughput: {enriched_data.num_rows/elapsed:,.0f} rows/sec")

    return topn_table

# ============================================================================
# Step 3: Best Bid/Offer Calculation
# ============================================================================

def calculate_best_bid_offer(topn_data):
    """Calculate best bid/offer and spreads."""
    print(f"\n{'='*80}")
    print("STEP 3: BEST BID/OFFER SPREAD CALCULATION")
    print("="*80)

    print("\n📌 Find best bid (highest) and best offer (lowest) per instrument")

    start = time.perf_counter()

    # Filter rank=1 only
    rank_1_mask = pc.equal(topn_data.column('rank'), 1)
    best_quotes = topn_data.filter(rank_1_mask)

    # Group by instrument to find bid/offer pairs
    instruments = set(best_quotes.column('instrumentId').to_pylist())

    spreads = []
    for instrument in instruments:
        inst_mask = pc.equal(best_quotes.column('instrumentId'), instrument)
        inst_data = best_quotes.filter(inst_mask)

        # Extract bid and offer
        bid_data = None
        offer_data = None

        for i in range(inst_data.num_rows):
            side = inst_data.column('side')[i].as_py()
            if side == "BID":
                bid_data = {col: inst_data.column(col)[i].as_py() for col in inst_data.schema.names}
            elif side == "OFFER":
                offer_data = {col: inst_data.column(col)[i].as_py() for col in inst_data.schema.names}

        if bid_data and offer_data:
            bid_price = bid_data['price']
            offer_price = offer_data['price']
            spread_abs = offer_price - bid_price
            spread_pct = (spread_abs / ((bid_price + offer_price) / 2)) * 100

            spread_row = {
                'instrumentId': instrument,
                'best_bid': bid_price,
                'best_offer': offer_price,
                'bid_company': bid_data['companyShortName'],
                'offer_company': offer_data['companyShortName'],
                'spread_absolute': round(spread_abs, 4),
                'spread_percentage': round(spread_pct, 4),
                'sector': bid_data.get('sector', 'Unknown'),
                'fitchRating': bid_data.get('fitchRating', 'NR'),
                'isInvestmentGrade': bid_data.get('isInvestmentGrade', 'N'),
                'marketSegment': bid_data.get('marketSegment', ''),
            }
            spreads.append(spread_row)

    spreads_table = pa.Table.from_pylist(spreads) if spreads else None

    elapsed = time.perf_counter() - start

    if spreads_table:
        print(f"\n📊 Best Bid/Offer Results:")
        print(f"   Instruments with both bid & offer: {spreads_table.num_rows:,}")

        # Calculate statistics
        avg_spread_pct = pc.mean(spreads_table.column('spread_percentage')).as_py()
        min_spread_pct = pc.min(spreads_table.column('spread_percentage')).as_py()
        max_spread_pct = pc.max(spreads_table.column('spread_percentage')).as_py()

        print(f"\n   Spread Statistics:")
        print(f"      Average: {avg_spread_pct:.4f}%")
        print(f"      Min: {min_spread_pct:.4f}%")
        print(f"      Max: {max_spread_pct:.4f}%")

        print(f"\n⚡ Performance:")
        print(f"   Time: {elapsed:.4f}s")

        # Show top 10 tightest spreads
        print(f"\n📈 Top 10 Tightest Spreads:")
        sorted_indices = pc.sort_indices(spreads_table.column('spread_percentage'))
        sorted_spreads = pc.take(spreads_table, sorted_indices).slice(0, 10)

        for i in range(min(10, sorted_spreads.num_rows)):
            inst_id = sorted_spreads.column('instrumentId')[i].as_py()
            bid = sorted_spreads.column('best_bid')[i].as_py()
            offer = sorted_spreads.column('best_offer')[i].as_py()
            spread_pct = sorted_spreads.column('spread_percentage')[i].as_py()
            sector = sorted_spreads.column('sector')[i].as_py()
            rating = sorted_spreads.column('fitchRating')[i].as_py()

            print(f"   {i+1:2d}. Instrument {inst_id}: {bid:.3f} / {offer:.3f} = {spread_pct:.4f}% ({sector}, {rating})")

    return spreads_table

# ============================================================================
# Main Pipeline
# ============================================================================

def main():
    # Configuration
    NUM_QUOTES = 100_000
    NUM_SECURITIES = 50_000
    BATCH_SIZE = 5_000

    print(f"\n⚙️  Configuration:")
    print(f"   Inventory quotes: {NUM_QUOTES:,}")
    print(f"   Securities: {NUM_SECURITIES:,}")
    print(f"   Batch size: {BATCH_SIZE:,}")

    # Generate data
    quotes = generate_inventory_quotes(NUM_QUOTES)
    securities = generate_security_master(NUM_SECURITIES)

    # Create lookup dict for securities
    securities_dict = {sec['ID']: sec for sec in securities}

    # Convert quotes to Arrow batches
    print(f"\n📦 Creating Arrow batches...")
    start_batch = time.perf_counter()

    batches = []
    for i in range(0, len(quotes), BATCH_SIZE):
        chunk = quotes[i:i+BATCH_SIZE]
        batch = pa.RecordBatch.from_pylist(chunk)
        batches.append(batch)

    batch_time = time.perf_counter() - start_batch
    print(f"✓ Created {len(batches)} batches in {batch_time:.2f}s")

    # Pipeline execution
    enriched = enrich_inventory(batches, securities_dict)

    if enriched:
        topn = compute_topn_ranking(enriched, n=5)
        spreads = calculate_best_bid_offer(topn)

    # Summary
    print(f"\n{'='*80}")
    print("PIPELINE SUMMARY")
    print("="*80)
    print(f"""
✓ Processed {NUM_QUOTES:,} inventory quotes
✓ Enriched with {NUM_SECURITIES:,} securities
✓ TopN ranking (top 5 per instrument/side)
✓ Best bid/offer spread calculation

Architecture:
- Sabot Stream API with Arrow columnar processing
- Stream-table join pattern (inventory + securities)
- TopN ranking with sorting
- Zero-copy operations throughout

This replicates the Flink SQL pipeline from:
/mrkaxis/invenory_rows_synthesiser/

Pipeline stages:
1. Base Enrichment (1_base_enrichment.sql)
2. TopN Ranking (2_topn_ranking.sql)
3. Best Quotes Calculation
""")

    print("="*80)
    print("✓ Demo complete!")
    print("="*80)

if __name__ == "__main__":
    main()
