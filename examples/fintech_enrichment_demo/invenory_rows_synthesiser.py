import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import math

# -----------------------------
# Config
# -----------------------------
N_ROWS = 1_200_000
SEED = 42
OUTFILE = "synthetic_inventory.csv"
CHUNK_SIZE = 100_000  # write in chunks to keep memory in check

rng = np.random.default_rng(SEED)

# -----------------------------
# Categorical pools (synthetic)
# -----------------------------
# Replace real firms with synthetic org codes while preserving US/UK flavor
countries = ["US", "UK"]
num_orgs = 40
org_codes = [f"ORG{str(i).zfill(2)}_{rng.choice(countries)}" for i in range(1, num_orgs + 1)]

# To keep companyShortName == companyName look consistent with sample, we’ll use the same synthetic value for both.
company_pool = np.array(org_codes)

sides = np.array(["BID", "OFFER"])
actions = np.array(["UPDATE", "DELETE"])  # Mostly UPDATE with some DELETEs
quote_types = np.array(["I", "F"])        # I ~ indicative, F ~ firm (name inspired by sample)
item_types = np.array(["P", "S"])         # P and S appear in sample
tiers = np.array([1, 2, 3, 4, 5])
market_segments = np.array(["HG", "AG", "HY"])            # high grade / agency / high yield
product_cds = np.array(["USHG", "USAS", "USSC", "USHS", "USHY", "UPAG"])

# level is often 1-5, with more 3s in the sample feel
level_probs = np.array([0.15, 0.2, 0.35, 0.2, 0.1])

# -----------------------------
# Helpers
# -----------------------------
def weighted_choice(values, probs, size):
    return rng.choice(values, size=size, p=probs)

def gen_sizes(n):
    # Heavy-tail: many small, some 100/1000, rare 5000/10000; zeros allowed (seen in sample)
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
    """
    Price ranges loosely tied to market segment and side.
    HG ~ 95-130, AG ~ 100-130, HY ~ 70-110 (all with noise).
    We'll occasionally drop NULLs and zeros to mimic sample.
    """
    base = np.zeros(n, dtype=float)

    # Segment anchors
    seg_anchor = {
        "HG": (110, 8),   # mean, std
        "AG": (112, 7),
        "HY": (92, 10),
    }
    for seg in np.unique(market_seg):
        idx = np.where(market_seg == seg)[0]
        mean, std = seg_anchor[seg]
        # Side can nudge a bit
        nudges = np.where(side[idx] == "BID", -0.6, 0.6)
        base[idx] = rng.normal(mean + nudges, std)

    # Clamp to reasonable bounds per segment
    bounds = {
        "HG": (85, 140),
        "AG": (90, 140),
        "HY": (60, 120),
    }
    lo = np.vectorize(lambda s: bounds[s][0])(market_seg)
    hi = np.vectorize(lambda s: bounds[s][1])(market_seg)
    base = np.clip(base, lo, hi)

    # Inject some zeros and NULLs similar to sample (a few %)
    zeros_mask = rng.random(n) < 0.02
    null_mask = rng.random(n) < 0.03
    base[zeros_mask] = 0.0
    # We'll return as strings later; mark nulls as np.nan for now
    base[null_mask] = np.nan

    # Round like market quotes (3 decimals, but keep some with more)
    # Randomly choose precision: 60% 3dp, 30% 2dp, 10% many dp
    prec_pick = rng.random(n)
    p = base.copy()
    p[np.isnan(p)] = np.nan
    mask3 = (prec_pick < 0.6)
    mask2 = (prec_pick >= 0.6) & (prec_pick < 0.9)
    maskN = (prec_pick >= 0.9)

    p[mask3] = np.round(p[mask3], 3)
    p[mask2] = np.round(p[mask2], 2)
    # maskN: leave as-is (already noisy)

    return p

def gen_spreads(n, quote_type):
    """
    Spreads mostly NULL in your sample; when present as numbers, keep them plausible.
    For quote_type 'S' (seen in sample), allow non-NULL more often.
    """
    out = np.full(n, np.nan)
    # 85% NULL overall, but if itemType == 'S', reduce null rate
    # We'll choose a base null mask now and let caller override
    base_non_null = rng.random(n) < 0.15
    out[base_non_null] = rng.normal(120, 30, size=base_non_null.sum())  # around 120 with dispersion
    # Negative spreads don’t make sense here; clamp
    out = np.where(np.isnan(out), out, np.maximum(out, 0.0))
    return out

def gen_levels(n):
    return rng.choice(tiers, size=n, p=level_probs)

def gen_instruments(n):
    """
    Synthetic instrument IDs with realistic width.
    """
    # 10M–40M range to look like your samples
    instr = rng.integers(10_000_000, 40_000_000, size=n, dtype=np.int64)
    # ~5% NULLs allowed for benchmarkInstrumentId later
    return instr

def random_datetime_array(n, start_dt, end_dt):
    delta = (end_dt - start_dt).total_seconds()
    secs = rng.uniform(0, delta, size=n)
    return np.array([start_dt + timedelta(seconds=float(s)) for s in secs])

def strftime_or_null(arr, null_mask):
    out = np.empty(arr.shape[0], dtype=object)
    for i, ts in enumerate(arr):
        out[i] = "NULL" if null_mask[i] else ts.strftime("%Y-%m-%d %H:%M:%S")
    return out

def number_or_null(arr, null_mask):
    out = np.empty(arr.shape[0], dtype=object)
    for i, v in enumerate(arr):
        out[i] = "NULL" if (null_mask[i] or (isinstance(v, float) and math.isnan(v))) else (str(v) if not isinstance(v, (int, np.integer)) else int(v))
    return out

# -----------------------------
# Main generator (chunked)
# -----------------------------
def main():
    columns = [
        "companyShortName","companyName","onBehalfOf",
        "contributionTimestamp","processedTimestamp","kafkaCreateTimestamp","singlestoreTimestamp",
        "instrumentId","benchmarkInstrumentId","spread","price","size","side","action",
        "level","quoteType","itemType","tier","marketSegment","productCD","actualFirmness"
    ]

    # Write header once
    first_chunk = True
    total = 0

    # Time window around the day in your sample (privacy-skewed to ±14 days)
    end_dt = datetime(2025, 9, 10, 23, 59, 59)
    start_dt = datetime(2025, 8, 20, 0, 0, 0)

    while total < N_ROWS:
        n = min(CHUNK_SIZE, N_ROWS - total)

        # Firm fields
        comp = rng.choice(company_pool, size=n)
        companyShortName = comp
        companyName = comp  # keep equal like sample
        onBehalfOf = np.full(n, "NULL", dtype=object)  # matches the sample; still “skewed”

        # Market and product
        marketSegment = rng.choice(market_segments, size=n, p=[0.55, 0.20, 0.25])
        productCD = rng.choice(product_cds, size=n, p=[0.40, 0.20, 0.20, 0.10, 0.07, 0.03])

        # Side/action distributions with more UPDATEs
        side = rng.choice(sides, size=n, p=[0.55, 0.45])
        action = rng.choice(actions, size=n, p=[0.85, 0.15])

        # Quote/item types
        quoteType = rng.choice(quote_types, size=n, p=[0.85, 0.15])
        itemType = rng.choice(item_types, size=n, p=[0.85, 0.15])

        # Levels/tiers
        level = gen_levels(n)
        tier = rng.integers(1, 5, size=n)  # keep 1-4 like sample often shows; allow 5 sparsely
        # Slightly bias toward 3 for tier:
        bump3 = rng.random(n) < 0.3
        tier[bump3] = 3

        # Instruments
        instrumentId = gen_instruments(n)
        # Benchmarks: ~25% present
        bench_mask = rng.random(n) < 0.25
        benchmarkInstrumentId = gen_instruments(n)
        benchmarkInstrumentId[~bench_mask] = -1  # will turn into NULL later

        # Spreads (mostly NULL)
        spread = gen_spreads(n, quoteType)

        # Prices depend on segment and side
        price = gen_prices(n, marketSegment, side)

        # Sizes heavy-tail
        size_vals = gen_sizes(n)

        # Timestamps with realistic ordering and random lags:
        # contributionTimestamp (sometimes NULL), processedTimestamp >= contribution,
        # kafkaCreateTimestamp around processed, singlestoreTimestamp >= kafka.
        processed = random_datetime_array(n, start_dt, end_dt)

        # contribution is sometimes NULL (~35% NULL); otherwise within -15m..+15m of processed
        contrib_null = rng.random(n) < 0.35
        contrib_offsets = rng.integers(-15*60, 15*60, size=n)
        contribution = np.array([processed[i] + timedelta(seconds=int(contrib_offsets[i])) for i in range(n)])
        # kafkaCreate within 0..+120s
        kafka_offsets = rng.integers(0, 120, size=n)
        kafka_ts = np.array([processed[i] + timedelta(seconds=int(kafka_offsets[i])) for i in range(n)])
        # singlestore within 0..+300s from kafka
        sstore_offsets = rng.integers(0, 300, size=n)
        sstore_ts = np.array([kafka_ts[i] + timedelta(seconds=int(sstore_offsets[i])) for i in range(n)])

        # Some fields get forced NULL:
        # price NULL already handled; spread NULL already handled.
        # instrumentId occasionally NULL (~2%)
        instr_null = rng.random(n) < 0.02

        # benchmark NULL where we set -1
        bench_null = (benchmarkInstrumentId == -1)

        # contribution NULL according to contrib_null
        # Build final string columns
        df = pd.DataFrame({
            "companyShortName": companyShortName,
            "companyName": companyName,
            "onBehalfOf": onBehalfOf,  # already "NULL"
            "contributionTimestamp": strftime_or_null(contribution, contrib_null),
            "processedTimestamp": [dt.strftime("%Y-%m-%d %H:%M:%S") for dt in processed],
            "kafkaCreateTimestamp": [dt.strftime("%Y-%m-%d %H:%M:%S") for dt in kafka_ts],
            "singlestoreTimestamp": [dt.strftime("%Y-%m-%d %H:%M:%S") for dt in sstore_ts],
            "instrumentId": number_or_null(instrumentId, instr_null),
            "benchmarkInstrumentId": number_or_null(benchmarkInstrumentId, bench_null),
            "spread": number_or_null(np.round(spread, 9), np.isnan(spread)),
            "price": number_or_null(price, np.isnan(price)),
            "size": number_or_null(size_vals, np.zeros(n, dtype=bool)),
            "side": side,
            "action": action,
            "level": number_or_null(level, np.zeros(n, dtype=bool)),
            "quoteType": quoteType,
            "itemType": itemType,
            "tier": number_or_null(tier, np.zeros(n, dtype=bool)),
            "marketSegment": marketSegment,
            "productCD": productCD,
            "actualFirmness": np.full(n, "INVENTORY", dtype=object),
        }, columns=columns)

        # Ensure text "NULL" for numeric NaNs already handled by number_or_null
        # Write chunk
        df.to_csv(
            OUTFILE,
            mode="wt" if first_chunk else "at",
            index=False,
            header=first_chunk,
        )
        first_chunk = False
        total += n
        print(f"Wrote {total:,} rows...")

    print(f"Done. File written: {OUTFILE}")

if __name__ == "__main__":
    main()
