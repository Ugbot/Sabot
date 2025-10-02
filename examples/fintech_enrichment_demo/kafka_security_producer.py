#!/usr/bin/env python3
"""
Kafka producer for master security synthesizer
Sends synthetic security master data to Kafka as fast as possible
"""

import numpy as np
import pandas as pd
from datetime import datetime, timedelta, date, timezone
import string
import json
from confluent_kafka import Producer
import time
import sys
from kafka_config import get_producer_config, TOPIC_SECURITY, KAFKA_BOOTSTRAP_SERVERS

# -----------------------------
# Data Generation Config
# -----------------------------
BATCH_SIZE = 500  # Smaller batch for complex security records
SEED = 7

rng = np.random.default_rng(SEED)

# -----------------------------
# Reference pools (synthetic)
# -----------------------------
issuer_roots = [
    "ALPHACORE INC","BETA CAPITAL LLC","GAMMA ENERGY PLC","DELTA INDUSTRIES SA",
    "EPSILON MOTORS CO","ZETA FINANCE BV","OMEGA TECH CORP","SIGMA MATERIALS LTD",
    "RHO UTILITIES CO","THETA LOGISTICS LP","LAMBDA TELECOM AG","KAPPA CHEMICALS NV",
    "XI AEROSPACE INC","TAU HEALTHCARE PLC","OMICRON FOODS CO","PI REALTY TRUST",
    "CHI INSURANCE CO","PHI TRANSPORT SA","UPSILON METALS PLC","NU POWER CO",
    "ETA BANK NA","IOTA LENDING LLC","MU RETAIL INC","PSI PAPER CO",
    "OMEGA GLOBAL HOLDINGS","VEGA SERVICES LTD","ORION BRANDS INC","LYRA TECH LLC",
    "CYGNUS GROUP SA","DRACO FUNDING BV","POLARIS ENERGY INC","HYDRA PIPELINE CO",
    "PHOENIX MATERIALS PLC","PAVO AUTO CORP","CETUS SOFTWARE INC","AQUILA MEDIA LTD",
    "VOLANS PACKAGING INC","MIRA FINANCIAL CORP","SIRENUS SHIPPING PLC","ARCTURUS CAPITAL LLC",
]
SUFFIX_POOL = np.array([" HOLDINGS", " GROUP", " LTD", " PLC"], dtype=object)

countries = np.array(["US","CA","GB","DE","FR","NL","LU","IE","CH"])
currencies = np.array(["USD","EUR","GBP","CAD","CHF"])
sectors = np.array(["Financials","Industrials","Utilities","Consumer","Energy","Technology","Healthcare"])
industries_by_sector = {
    "Financials": ["Banks","Insurance","Other Financial","Asset Management"],
    "Industrials": ["Machinery","Aerospace","Transportation","Manufacturing","Vehicle Parts"],
    "Utilities": ["Electric Power","Gas","Water","Utility - Other"],
    "Consumer": ["Consumer Goods","Food and Beverage","Media","Retail"],
    "Energy": ["Oil and Gas","Midstream","Energy Company"],
    "Technology": ["Software","Semiconductors","Hardware","IT Services"],
    "Healthcare": ["Pharma","Biotech","Medical Devices"],
}
payment_types = np.array(list("NFF"))
interest_rate_types = np.array(["F","N"])
bond_types = np.array(["Q","S"])
accrual_methods = np.array(["2","3"])
payment_freqs = np.array(["Q","S","A"])
redemptions = np.array(["Sr","Sr Secured","Unsecrd Nt","Sub Note","Sr Note","Sr Deb"])
security_types = np.array(["Financials","Industrials","Utilities","Technology","Consumer","Energy"])
flt_formula_bases = np.array(["US3MLIB","US6MLIB","SOFR3M","EUR3M","GBP3M"])
vendor_names = np.array(["EJV","BBG","IDC"])
asset_status = np.array(["MAT","NAC","ISS","SB","SU","SS"])
market_segments = np.array(["1","2","3","4"])
ratings_fitch = np.array(["AAA","AA+","AA","AA-","A+","A","A-","BBB+","BBB","BBB-","BB+","NR","NA"])
ratings_snp = np.array(["AAA","AA+","AA","AA-","A+","A","A-","BBB+","BBB","BBB-","BB+","NR","NA"])
ratings_mood = np.array(["Aaa","Aa2","Aa3","A1","A2","A3","Baa1","Baa2","Baa3","Ba1","Ba2","NR","NA"])

# -----------------------------
# Kafka Producer Setup
# -----------------------------
def create_producer():
    """Create Kafka producer with Aiven configuration"""
    return Producer(get_producer_config())

# -----------------------------
# CUSIP/ISIN Generation
# -----------------------------
CUSIP_CHARS = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ*@#"
CUSIP_MAP = {c: i for i, c in enumerate(CUSIP_CHARS)}

def cusip_check_digit(base8: str) -> str:
    s = 0
    for i, ch in enumerate(base8):
        v = CUSIP_MAP[ch]
        if i % 2 == 1:
            v *= 2
        s += v // 10 + v % 10
    return str((10 - (s % 10)) % 10)

def random_cusip(n):
    letters = np.array(list(string.ascii_uppercase + string.digits))
    first = rng.choice(list("XZABCDEFGHJKLMNPQRSTUVWY"), size=n)
    body = ["".join(rng.choice(letters, size=7)) for _ in range(n)]
    base8 = np.array([first[i] + body[i] for i in range(n)])
    check = np.array([cusip_check_digit(b) for b in base8])
    return np.char.add(base8, check)

def isin_check_digit(isin12: str) -> str:
    def expand(s):
        out = ""
        for ch in s:
            if ch.isalpha():
                out += str(ord(ch) - 55)
            else:
                out += ch
        return out
    digits = expand(isin12).replace(" ", "")
    total, dbl = 0, (len(digits) % 2 == 0)
    for d in digits:
        val = int(d)
        if dbl:
            val *= 2
            if val > 9:
                val -= 9
        total += val
        dbl = not dbl
    return str((10 - (total % 10)) % 10)

def build_isin(country2: np.ndarray, cusip9: np.ndarray) -> np.ndarray:
    base = np.char.add(country2, cusip9)
    chk = np.array([isin_check_digit(b) for b in base])
    return np.char.add(base, chk)

# -----------------------------
# Data Generation
# -----------------------------
def random_dates(n):
    start = datetime(1996, 1, 1)
    end = datetime(2025, 9, 1)
    span = (end - start).days
    issue = np.array([start + timedelta(days=int(rng.integers(0, span))) for _ in range(n)])
    
    mat_years = rng.integers(2, 41, size=n)
    maturity = np.array([date(issue[i].year + int(mat_years[i]), max(1, issue[i].month), 1) for i in range(n)])
    
    return issue, maturity

def random_coupons(n):
    fixed = rng.random(n) < 0.8
    coup = np.empty(n, dtype=float)
    coup[fixed] = rng.uniform(0.02, 0.09, size=fixed.sum())
    coup[~fixed] = rng.uniform(0.005, 0.03, size=(~fixed).sum())
    return np.round(coup, 6), fixed

def random_ratings(n):
    f = rng.choice(ratings_fitch, size=n)
    s = rng.choice(ratings_snp, size=n)
    m = rng.choice(ratings_mood, size=n)
    ig_map = {"AAA","AA+","AA","AA-","A+","A","A-","BBB+","BBB","Baa1","Baa2","Baa3"}
    ig = np.array(["Y" if (f[i] in ig_map or s[i] in ig_map or m[i] in ig_map) else "N" for i in range(n)], dtype=object)
    return f, s, m, ig

def generate_batch(n, next_id):
    """Generate a batch of security master records"""
    ids = np.arange(next_id, next_id + n, dtype=np.int64)
    
    # Issuer names
    issuer = rng.choice(np.array(issuer_roots, dtype=object), size=n)
    suffix_mask = rng.random(n) < 0.25
    if np.any(suffix_mask):
        suffix_choices = rng.choice(SUFFIX_POOL, size=int(suffix_mask.sum()))
        base = issuer[suffix_mask].tolist()
        issuer[suffix_mask] = [a + b for a, b in zip(base, suffix_choices.tolist())]
    
    # Dates
    issue_dates, maturity_dates = random_dates(n)
    
    # Coupons
    coupon, is_fixed = random_coupons(n)
    
    # Geography
    country = rng.choice(countries, size=n, p=[0.55,0.06,0.12,0.06,0.05,0.05,0.04,0.04,0.03])
    currency = rng.choice(currencies, size=n, p=[0.7,0.12,0.1,0.05,0.03])
    
    # Identifiers
    cusip9 = random_cusip(n)
    isin = build_isin(country, cusip9)
    
    # Sectors
    sector = rng.choice(sectors, size=n)
    industry = np.empty(n, dtype=object)
    for s in np.unique(sector):
        idx = np.where(sector == s)[0]
        industry[idx] = rng.choice(np.array(industries_by_sector[s], dtype=object), size=idx.size)
    
    # Bond characteristics
    payment_type = rng.choice(payment_types, size=n, p=[0.25,0.75,0.0])
    ir_type = np.where(is_fixed, "F", "N")
    bond_type = rng.choice(bond_types, size=n, p=[0.35,0.65])
    defaulted = rng.choice(np.array(["N","Y"]), size=n, p=[0.995, 0.005])
    accrual_method = rng.choice(accrual_methods, size=n, p=[0.6,0.4])
    pay_freq = rng.choice(payment_freqs, size=n, p=[0.25,0.6,0.15])
    redemption = rng.choice(redemptions, size=n, p=[0.35,0.12,0.18,0.15,0.15,0.05])
    
    # Ratings
    fr, sr, mr, ig = random_ratings(n)
    
    # Optional features
    callable_f = rng.choice(["Y","N"], size=n, p=[0.1,0.9])
    putable_f = rng.choice(["Y","N"], size=n, p=[0.03,0.97])
    sinkable_f = rng.choice(["Y","N"], size=n, p=[0.06,0.94])
    makewhole = rng.choice(["Y","N"], size=n, p=[0.08,0.92])
    mtn = rng.choice(["Y","N"], size=n, p=[0.15,0.85])
    
    # Market data
    mkt_seg = rng.choice(market_segments, size=n, p=[0.4,0.3,0.2,0.1])
    sec_type = rng.choice(security_types, size=n)
    vendor = rng.choice(vendor_names, size=n)
    asset_status_cd = rng.choice(asset_status, size=n, p=[0.25,0.15,0.15,0.02,0.35,0.08])
    
    # Amounts
    parvalue = rng.choice(np.array([1000, 25000, 100, 97, 99], dtype=object), size=n, p=[0.7, 0.05, 0.05, 0.1, 0.1])
    issue_amt = rng.integers(25_000_000, 1_000_000_000, size=n, dtype=np.int64)
    amt_out = issue_amt.copy()
    zero_mask = rng.random(n) < 0.4
    amt_out[zero_mask] = 0
    
    # Create records
    records = []
    for i in range(n):
        record = {
            "ID": int(ids[i]),
            "NAME": issuer[i],
            "CUSIP": cusip9[i],
            "ISIN": isin[i],
            "COUPON": float(coupon[i]),
            "MATURITY": maturity_dates[i].isoformat(),
            "ISSUEDATE": issue_dates[i].date().isoformat(),
            "ISSUER": issuer[i],
            "COUNTRYCODE": country[i],
            "ISOCURRENCYCODE": currency[i],
            "SECTOR": sector[i],
            "INDUSTRY": industry[i],
            "PAYMENTTYPE": payment_type[i],
            "INTERESTRATETYPE": ir_type[i],
            "BONDTYPE": bond_type[i],
            "DEFAULTED": defaulted[i],
            "ACCRUALMETHOD": accrual_method[i],
            "PAYMENTFREQUENCY": pay_freq[i],
            "REDEMPTION": redemption[i],
            "FITCHRATING": fr[i],
            "SNPRATING": sr[i],
            "MOODYRATING": mr[i],
            "ISINVESTMENTGRADE": ig[i],
            "CALLABLE": callable_f[i],
            "PUTABLE": putable_f[i],
            "SINKABLE": sinkable_f[i],
            "MAKEWHOLE": makewhole[i],
            "MTN": mtn[i],
            "MARKETSEGMENT": mkt_seg[i],
            "SECURITYTYPE": sec_type[i],
            "VENDORNAME": vendor[i],
            "ASSET_STATUS_CD": asset_status_cd[i],
            "PARVALUE": int(parvalue[i]),
            "ISSUEAMOUNT": int(issue_amt[i]),
            "AMOUNTOUTSTANDING": int(amt_out[i]),
            "TIMESTAMP": datetime.now(timezone.utc).isoformat()
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

# -----------------------------
# Main Producer Loop
# -----------------------------
def main():
    print(f"Starting Kafka producer for topic: {TOPIC_SECURITY}")
    print(f"Connecting to: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Batch size: {BATCH_SIZE}")
    print("Press Ctrl+C to stop")
    
    producer = create_producer()
    
    total_sent = 0
    start_time = time.time()
    next_id = 100_000_00
    
    try:
        while True:
            # Generate batch
            batch = generate_batch(BATCH_SIZE, next_id)
            next_id += BATCH_SIZE
            
            # Send to Kafka with queue management
            for record in batch:
                # Keep trying to produce until successful
                while True:
                    try:
                        producer.produce(
                            TOPIC_SECURITY,
                            key=record['ISIN'].encode('utf-8'),
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
            
            # Print stats and flush every 5,000 records
            if total_sent % 5000 == 0:
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