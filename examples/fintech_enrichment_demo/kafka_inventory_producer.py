#!/usr/bin/env python3
"""
Kafka producer for inventory rows synthesizer
Sends synthetic inventory data to Kafka as fast as possible
"""

import numpy as np
import pandas as pd
from datetime import datetime, timedelta, timezone
import math
import json
from confluent_kafka import Producer
import time
import sys
from kafka_config import get_producer_config, TOPIC_INVENTORY, KAFKA_BOOTSTRAP_SERVERS

# -----------------------------
# Data Generation Config
# -----------------------------
BATCH_SIZE = 1000  # Number of records to generate per batch
SEED = 42

rng = np.random.default_rng(SEED)

# -----------------------------
# Categorical pools (synthetic)
# -----------------------------
countries = ["US", "UK"]
num_orgs = 40
org_codes = [f"ORG{str(i).zfill(2)}_{rng.choice(countries)}" for i in range(1, num_orgs + 1)]
company_pool = np.array(org_codes)

sides = np.array(["BID", "OFFER"])
actions = np.array(["UPDATE", "DELETE"])
quote_types = np.array(["I", "F"])
item_types = np.array(["P", "S"])
tiers = np.array([1, 2, 3, 4, 5])
market_segments = np.array(["HG", "AG", "HY"])
product_cds = np.array(["USHG", "USAS", "USSC", "USHS", "USHY", "UPAG"])

level_probs = np.array([0.15, 0.2, 0.35, 0.2, 0.1])

# -----------------------------
# Kafka Producer Setup
# -----------------------------
def create_producer():
    """Create Kafka producer with Aiven configuration"""
    return Producer(get_producer_config())

# -----------------------------
# Data Generation Functions
# -----------------------------
def gen_sizes(n):
    buckets = rng.choice(
        ["zero", "tiny", "small", "med", "big", "huge"],
        size=n,
        p=[0.08, 0.25, 0.30, 0.22, 0.12, 0.03],
    )
    out = np.empty(n, dtype=np.int64)
    out[buckets == "zero"] = 0
    out[buckets == "tiny"] = rng.integers(1, 20, size=(buckets == "tiny").sum())
    out[buckets == "small"] = rng.integers(20, 250, size=(buckets == "small").sum())
    out[buckets == "med"] = rng.integers(250, 1200, size=(buckets == "med").sum())
    out[buckets == "big"] = rng.choice([1000, 2000, 5000], size=(buckets == "big").sum(), p=[0.6, 0.25, 0.15])
    out[buckets == "huge"] = rng.choice([10000, 15000], size=(buckets == "huge").sum(), p=[0.8, 0.2])
    return out

def gen_prices(n, market_seg, side):
    base = np.zeros(n, dtype=float)
    seg_anchor = {
        "HG": (110, 8),
        "AG": (112, 7),
        "HY": (92, 10),
    }
    for seg in np.unique(market_seg):
        idx = np.where(market_seg == seg)[0]
        mean, std = seg_anchor[seg]
        nudges = np.where(side[idx] == "BID", -0.6, 0.6)
        base[idx] = rng.normal(mean + nudges, std)
    
    bounds = {
        "HG": (85, 140),
        "AG": (90, 140),
        "HY": (60, 120),
    }
    lo = np.vectorize(lambda s: bounds[s][0])(market_seg)
    hi = np.vectorize(lambda s: bounds[s][1])(market_seg)
    base = np.clip(base, lo, hi)
    
    zeros_mask = rng.random(n) < 0.02
    null_mask = rng.random(n) < 0.03
    base[zeros_mask] = 0.0
    base[null_mask] = np.nan
    
    prec_pick = rng.random(n)
    p = base.copy()
    mask3 = (prec_pick < 0.6)
    mask2 = (prec_pick >= 0.6) & (prec_pick < 0.9)
    
    p[mask3] = np.round(p[mask3], 3)
    p[mask2] = np.round(p[mask2], 2)
    
    return p

def gen_spreads(n):
    out = np.full(n, np.nan)
    base_non_null = rng.random(n) < 0.15
    out[base_non_null] = rng.normal(120, 30, size=base_non_null.sum())
    out = np.where(np.isnan(out), out, np.maximum(out, 0.0))
    return out

def gen_instruments(n):
    return rng.integers(10_000_000, 40_000_000, size=n, dtype=np.int64)

def generate_batch(n):
    """Generate a batch of inventory rows"""
    # Company data
    comp = rng.choice(company_pool, size=n)
    
    # Market and product
    marketSegment = rng.choice(market_segments, size=n, p=[0.55, 0.20, 0.25])
    productCD = rng.choice(product_cds, size=n, p=[0.40, 0.20, 0.20, 0.10, 0.07, 0.03])
    
    # Side/action
    side = rng.choice(sides, size=n, p=[0.55, 0.45])
    action = rng.choice(actions, size=n, p=[0.85, 0.15])
    
    # Quote/item types
    quoteType = rng.choice(quote_types, size=n, p=[0.85, 0.15])
    itemType = rng.choice(item_types, size=n, p=[0.85, 0.15])
    
    # Levels/tiers
    level = rng.choice(tiers, size=n, p=level_probs)
    tier = rng.integers(1, 5, size=n)
    bump3 = rng.random(n) < 0.3
    tier[bump3] = 3
    
    # Instruments
    instrumentId = gen_instruments(n)
    bench_mask = rng.random(n) < 0.25
    benchmarkInstrumentId = gen_instruments(n)
    
    # Spreads and prices
    spread = gen_spreads(n)
    price = gen_prices(n, marketSegment, side)
    size_vals = gen_sizes(n)
    
    # Timestamps
    now = datetime.now(timezone.utc)
    processed = now
    
    # Create records
    records = []
    for i in range(n):
        record = {
            "companyShortName": comp[i],
            "companyName": comp[i],
            "onBehalfOf": None,
            "contributionTimestamp": (processed - timedelta(seconds=int(rng.integers(0, 900)))).isoformat(),
            "processedTimestamp": processed.isoformat(),
            "kafkaCreateTimestamp": now.isoformat(),
            "singlestoreTimestamp": (now + timedelta(seconds=int(rng.integers(0, 10)))).isoformat(),
            "instrumentId": int(instrumentId[i]),
            "benchmarkInstrumentId": int(benchmarkInstrumentId[i]) if bench_mask[i] else None,
            "spread": float(spread[i]) if not np.isnan(spread[i]) else None,
            "price": float(price[i]) if not np.isnan(price[i]) else None,
            "size": int(size_vals[i]),
            "side": side[i],
            "action": action[i],
            "level": int(level[i]),
            "quoteType": quoteType[i],
            "itemType": itemType[i],
            "tier": int(tier[i]),
            "marketSegment": marketSegment[i],
            "productCD": productCD[i],
            "actualFirmness": "INVENTORY"
        }
        records.append(record)
    
    return records

# -----------------------------
# Delivery callback
# -----------------------------
def delivery_report(err, msg):
    """Called once for each message produced to indicate delivery result."""
    if err is not None:
        print(f'Message delivery failed: {err}')
    # In fast mode, we don't print successful deliveries to maximize speed

# -----------------------------
# Main Producer Loop
# -----------------------------
def main():
    print(f"Starting Kafka producer for topic: {TOPIC_INVENTORY}")
    print(f"Connecting to: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Batch size: {BATCH_SIZE}")
    print("Press Ctrl+C to stop")
    
    producer = create_producer()
    
    total_sent = 0
    start_time = time.time()
    
    try:
        while True:
            # Generate batch
            batch = generate_batch(BATCH_SIZE)
            
            # Send to Kafka with queue management
            for record in batch:
                # Keep trying to produce until successful
                while True:
                    try:
                        producer.produce(
                            TOPIC_INVENTORY,
                            key=str(record['instrumentId']).encode('utf-8'),
                            value=json.dumps(record).encode('utf-8'),
                            callback=delivery_report
                        )
                        break  # Success, exit retry loop
                    except BufferError:
                        # Queue is full, wait for messages to be delivered
                        producer.poll(0.1)  # Process delivery reports
                        continue
            
            # Poll for delivery reports more frequently
            producer.poll(0)
            
            total_sent += BATCH_SIZE
            
            # Print stats and flush every 10,000 records
            if total_sent % 10000 == 0:
                # Flush to ensure delivery
                producer.flush(timeout=1)  # Quick flush
                
                elapsed = time.time() - start_time
                rate = total_sent / elapsed
                print(f"Sent {total_sent:,} records | Rate: {rate:.0f} records/sec")
            
    except KeyboardInterrupt:
        print("\nShutting down...")
    finally:
        # Wait for any outstanding messages to be delivered
        producer.flush()
        elapsed = time.time() - start_time
        print(f"\nTotal records sent: {total_sent:,}")
        print(f"Total time: {elapsed:.2f} seconds")
        print(f"Average rate: {total_sent/elapsed:.0f} records/sec")

if __name__ == "__main__":
    main()