#!/usr/bin/env python3
"""
Kafka producer for TRAX trades synthesizer
Sends synthetic TRAX trade data to Kafka as fast as possible
"""

import numpy as np
import string
from datetime import datetime, timedelta, timezone
import json
from confluent_kafka import Producer
import time
from kafka_config import get_producer_config, TOPIC_TRADES, KAFKA_BOOTSTRAP_SERVERS

# -----------------------------
# Data Generation Config
# -----------------------------
BATCH_SIZE = 2000  # Larger batch for simpler trade records
SEED = 20250904

rng = np.random.default_rng(SEED)

# -----------------------------
# Instrument universe
# -----------------------------
N_INSTR = 50_000
INSTR_CCYS = np.array(["EUR","USD","GBP","AUD","ILS","CHF"])
CCY_TO_EUR = {"EUR": 1.00, "USD": 0.86, "GBP": 1.17, "AUD": 0.62, "ILS": 0.28, "CHF": 1.05}
CCY_TO_USD = {"EUR": 1.17, "USD": 1.00, "GBP": 1.36, "AUD": 0.72, "ILS": 0.33, "CHF": 1.10}

# Pre-generate instrument universe
_COUNTRIES = np.array(["US","GB","DE","FR","IT","ES","NL","IE","FI","AT","LU","SE","NO","CH","IL","AU"])
DEALERS = np.array(["JPMG","CITI","BOFA","MSCO","GSIL","BARX","STNX","UBSW","RBC","BNP"])

# -----------------------------
# Kafka Producer Setup
# -----------------------------
def create_producer():
    """Create Kafka producer with Aiven configuration"""
    return Producer(get_producer_config())

# -----------------------------
# ISIN/CUSIP helpers
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

def random_cusip9(n):
    letters = np.array(list(string.ascii_uppercase + string.digits))
    first = rng.choice(list("XZABCDEFGHJKLMNPQRSTUVWY"), size=n)
    body = ["".join(rng.choice(letters, size=7)) for _ in range(n)]
    base8 = np.array([first[i] + body[i] for i in range(n)])
    chk = np.array([cusip_check_digit(b) for b in base8])
    return np.char.add(base8, chk)

def isin_check_digit(isin12: str) -> str:
    def expand(s):
        out = ""
        for ch in s:
            out += str(ord(ch) - 55) if ch.isalpha() else ch
        return out
    digits = expand(isin12)
    total, dbl = 0, (len(digits) % 2 == 0)
    for d in digits:
        v = int(d)
        if dbl:
            v *= 2
            if v > 9:
                v -= 9
        total += v
        dbl = not dbl
    return str((10 - (total % 10)) % 10)

def random_isin(n):
    country = rng.choice(_COUNTRIES, size=n)
    cusip9 = random_cusip9(n)
    base = np.char.add(country, cusip9)
    chk = np.array([isin_check_digit(b) for b in base])
    return np.char.add(base, chk), country

# LEI-like generator
_LEI_ALPHANUM = np.array(list(string.ascii_uppercase + string.digits))
def rand_lei(n: int) -> np.ndarray:
    return np.array(["".join(rng.choice(_LEI_ALPHANUM, size=20)) for _ in range(n)], dtype=object)

# Pre-generate instrument pool
print("Pre-generating instrument universe...")
instr_isin, instr_country = random_isin(N_INSTR)
instr_ccy = rng.choice(INSTR_CCYS, size=N_INSTR, p=[0.55,0.25,0.07,0.05,0.04,0.04])
instr_name = np.array([
    f"{rng.choice(['CORP','SOVN','MTGE','COVER','AGCY'])} {rng.integers(2026, 2046)} {ccy}"
    for ccy in instr_ccy
], dtype=object)

# -----------------------------
# Data Generation
# -----------------------------
def generate_batch(n, id_seq):
    """Generate a batch of TRAX trade records"""
    
    # Time windows (trades from last 7 days)
    t_now = datetime.now(timezone.utc)
    start = t_now - timedelta(days=7)
    tradedt = np.array([
        start + timedelta(seconds=int(rng.integers(0, 7*24*3600)))
        for _ in range(n)
    ])
    reported = tradedt + np.array([timedelta(seconds=int(rng.integers(3, 7200))) for _ in range(n)])
    
    # Instruments & economics
    instr_idx = rng.integers(0, N_INSTR, size=n)
    isin = instr_isin[instr_idx]
    ccy = instr_ccy[instr_idx]
    name = instr_name[instr_idx]
    
    eur_fx = np.array([max(0.0001, CCY_TO_EUR[c]) for c in ccy]) * rng.uniform(0.99, 1.01, size=n)
    usd_fx = np.array([max(0.0001, CCY_TO_USD[c]) for c in ccy]) * rng.uniform(0.99, 1.01, size=n)
    
    # Pricing
    dealpricetype = rng.choice(np.array(["PERC","YIEL"], dtype=object), size=n, p=[0.92,0.08])
    price = np.where(dealpricetype == "PERC",
                     rng.uniform(70.0, 110.0, size=n),
                     rng.uniform(1.0, 8.0, size=n))
    
    yld = np.where(dealpricetype == "PERC", np.maximum(0.5, 8.5 - (price - 70.0)*0.15), price)
    asky = yld + rng.uniform(0.00, 0.08, size=n)
    bidy = yld - rng.uniform(0.00, 0.08, size=n)
    isprd = rng.uniform(-120.0, 320.0, size=n)
    zsprd = isprd + rng.uniform(-5.0, 5.0, size=n)
    isw = isprd + rng.uniform(-10.0, 10.0, size=n)
    asw = isprd + rng.uniform(-10.0, 10.0, size=n)
    mktsp = isprd + rng.uniform(-5.0, 30.0, size=n)
    
    # Quantity
    qty = np.round(rng.choice(
        np.array([1_000, 5_000, 10_000, 100_000, 1_000_000, 2_000_000, 6_000_000], dtype=float),
        size=n,
        p=[0.25,0.15,0.25,0.18,0.12,0.03,0.02]
    ), 3)
    
    qty_eur = np.round(qty * (ccy == "EUR") + qty * (ccy != "EUR") * eur_fx, 6)
    qty_usd = np.round(qty_eur * np.divide(usd_fx, np.maximum(eur_fx, 1e-6)), 6)
    
    # Side & pairing
    side = rng.choice(np.array(["BUYR","SELL"], dtype=object), size=n)
    
    # Simple pairing logic (30% get pairs)
    pair_flags = rng.random(n) < 0.30
    pair_id = np.full(n, None, dtype=object)
    pair_ids = rng.integers(10**11, 10**12, size=int(pair_flags.sum()/2)+5)
    idxs = np.where(pair_flags)[0]
    rng.shuffle(idxs)
    for i in range(0, len(idxs)-1, 2):
        pid = int(pair_ids[i//2])
        a, b = idxs[i], idxs[i+1]
        pair_id[a] = pid
        pair_id[b] = pid
        side[b] = "SELL" if side[a] == "BUYR" else "BUYR"
    
    # Counterparties
    cpty_code = rand_lei(n)
    principal_code = rand_lei(n)
    firm_id_principal = rng.choice(DEALERS, size=n)
    firm_id_cpty = rng.choice(DEALERS, size=n)
    
    # IDs
    ids = id_seq + rng.integers(0, 9_999_999, size=n)
    tradedetailsid = 10**11 + rng.integers(0, 9_999_999, size=n)
    record_id = 10**11 + rng.integers(0, 9_999_999, size=n)
    
    global_trade_key = np.array([
        f"{tradedt[i].strftime('%Y%m%d%H%M%S')}{i:021d}" for i in range(n)
    ], dtype=object)
    
    # Settlement
    settle_days = rng.integers(1, 3, size=n)
    settlementdate = np.array([
        tradedt[i] + timedelta(days=int(settle_days[i]))
        for i in range(n)
    ])
    
    # Trading venue
    place_of_trade = rng.choice(np.array(["XOFF","SINT","OFF"], dtype=object), size=n, p=[0.75,0.20,0.05])
    
    # APA fields for EUR trades
    apa_active = (ccy == "EUR")
    
    # Direction / flags
    direction = np.where(side == "BUYR", "P", "Y")
    canbepublished = np.where(ccy == "EUR", "F", "N")
    sector = rng.choice(np.array(["CORP","SOVN","COVER","AGCY"], dtype=object), size=n, p=[0.6,0.32,0.05,0.03])
    
    # Create records
    records = []
    for i in range(n):
        record = {
            "id": int(ids[i]),
            "currdate": tradedt[i].date().isoformat(),
            "counterpartycode": cpty_code[i],
            "icmainstrumentidentifierisin": isin[i],
            "principalcodeidtrax": isin[i],
            "purchasesellindicator": side[i],
            "dealprice": float(price[i]),
            "quantityoffinancialinstrument": float(qty[i]),
            "tradedateandtime": tradedt[i].isoformat(),
            "reporteddatetime": reported[i].isoformat(),
            "quantityinusd": float(qty_usd[i]),
            "quantityineuro": float(qty_eur[i]),
            "ispread": float(isprd[i]),
            "zspread": float(zsprd[i]),
            "ispreadswaps": float(isw[i]),
            "aswspread": float(asw[i]),
            "marketspread": float(mktsp[i]),
            "yield": float(yld[i]),
            "askyield": float(asky[i]),
            "bidyield": float(bidy[i]),
            "is_block": rng.choice(["P","N","Y"], p=[0.15,0.65,0.20]),
            "counterpartytrax": rng.choice(DEALERS) if rng.random() > 0.3 else None,
            "dealpricetype": dealpricetype[i],
            "denomination": ccy[i],
            "icmafinancialinstrumentname": name[i],
            "settlementdate": settlementdate[i].isoformat(),
            "sp_condition": "SP" if rng.random() < 0.25 else "",
            "principalcodevaluetrax": rand_lei(1)[0] if rng.random() < 0.5 else None,
            "ccyeurfxrate": float(eur_fx[i]),
            "ccyusdfxrate": float(usd_fx[i]),
            "tradedetailsid": int(tradedetailsid[i]),
            "trade_quality": rng.choice(["NPM","PMT","APA-NON-PUB",""], p=[0.6,0.25,0.05,0.10]),
            "record_id": int(record_id[i]),
            "trade_pair_id": pair_id[i],
            "firm_id_principal": firm_id_principal[i],
            "firm_role_principal": rng.choice(["BRKR-DLR","BUY-SIDE","BANK","IDB"], p=[0.7,0.15,0.1,0.05]),
            "firm_id_cpty": firm_id_cpty[i],
            "firm_role_cpty": rng.choice(["BRKR-DLR","BUY-SIDE","BANK","IDB"], p=[0.4,0.4,0.15,0.05]),
            "placeoftrade": place_of_trade[i],
            "global_trade_key": global_trade_key[i],
            "source": rng.choice(["ARM_REPORT","TRAX_APA","TRAX_MATCH"], p=[0.45,0.35,0.20]),
            "apa_active": "NEW" if apa_active[i] else None,
            "canbepublished": canbepublished[i],
            "direction": direction[i],
            "sector": sector[i],
            "trddate": tradedt[i].date().isoformat()
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
    print(f"Starting Kafka producer for topic: {TOPIC_TRADES}")
    print(f"Connecting to: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Batch size: {BATCH_SIZE}")
    print("Press Ctrl+C to stop")
    
    producer = create_producer()
    
    total_sent = 0
    start_time = time.time()
    id_seq = 2_100_000_000
    
    try:
        while True:
            # Generate batch
            batch = generate_batch(BATCH_SIZE, id_seq)
            id_seq += BATCH_SIZE * 10  # Leave gaps in IDs
            
            # Send to Kafka with queue management
            for record in batch:
                # Keep trying to produce until successful
                while True:
                    try:
                        producer.produce(
                            TOPIC_TRADES,
                            key=record['icmainstrumentidentifierisin'].encode('utf-8'),
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
            
            # Print stats and flush every 20,000 records
            if total_sent % 20000 == 0:
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