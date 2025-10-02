#!/usr/bin/env python3
"""
Generate 1,000,000 synthetic TRAX-like trades (last 7 days), CSV output.
- Produces realistic pair trades (BUYR/SELL) for ~30% of rows
- Populates key economics: price, quantity, EUR/USD conversions, yields/spreads
- Uses LEI-like party codes and valid ISINs
- Fills unmapped columns with 'NULL' to match your wide schema

Requires: numpy, pandas
"""

import math
import string
from datetime import datetime, timedelta
import numpy as np
import pandas as pd

# ------------------------------
# Config
# ------------------------------
N_ROWS       = 1_000_000
CHUNK_SIZE   = 200_000         # tune to your RAM/IO
SEED         = 20250904
OUTFILE      = "trax_trades_1m.csv"
NULL         = "NULL"

rng = np.random.default_rng(SEED)

# ------------------------------
# Schema (order preserved from your sample)
# ------------------------------
COLUMNS = [
    "#", "replication_ts", "id", "currdate", "counterpartycode",
    "icmainstrumentidentifierisin", "principalcodeidtrax", "purchasesellindicator",
    "dealprice", "quantityoffinancialinstrument", "tradedateandtime",
    "reporteddatetime", "quantityinusd", "quantityineuro", "quantityineuroest",
    "ispread", "zspread", "ispreadswaps", "aswspread", "marketspread",
    "yield", "askyield", "bidyield", "quantityinccyest", "totalchildtrades",
    "canbesummarized", "lastchildtradeid", "is_block", "tradetimerank",
    "create_date", "update_date", "counterpartybic", "counterpartycodetype_numeric",
    "createddatetime", "dealpricetype", "denomination", "icmafinancialinstrumentname",
    "linkedreferencerela", "linkedreferencevers", "linkedreferenceprev",
    "modifieddatetime", "qtyoffinancialinstrumenttype", "sendersreference",
    "settlementdate", "tradesubaccount", "tradetransactiontype", "transactionstatus",
    "sp_condition", "notsummarizedreason", "icmainstrumentidentifiericma",
    "principalcodevaluetrax", "ccyeurfxrate", "ccyusdfxrate", "tradedetailsid",
    "trade_quality", "block_key", "matched_block_key", "record_id", "trade_pair_id",
    "passive_match_status", "sp_condition_principal", "sp_condition_cpty",
    "firm_id_principal", "firm_role_principal", "firm_id_cpty", "firm_role_cpty",
    "counterpartytrax", "counterpartynarrative", "createduser", "exerciseprice",
    "exercisepricetype", "expirydate", "icmafinancialinstrumenttype",
    "linkedreferencepool", "modifieduser", "narrative", "optionputorcall",
    "pfinancialinstrumentname", "pfinancialinstrumenttype", "participantinstrumentcode",
    "participantinstrumentcodetype", "punderlyinginstrumentcode",
    "punderlyinginstrumentcodetype", "punderlyinginstrumenttype", "placeoftrade",
    "placeoftradetype", "pricemultiplier", "rebuilddatetime", "repoendlegstatus",
    "settlementcode", "sinstructiongenerationind", "settlementinstructionstatus",
    "sinstructionstatustype", "settlementmsgfunction", "settlementreasoncode",
    "settlementreasoncodetype", "settlementreasonnarrative", "statusdatetime",
    "archivedate", "fundid", "thirdpartyreference", "delete_flag", "global_trade_key",
    "original_record_id", "source", "duplicate_flag", "duplicate_key_straight",
    "duplicate_key_reversed", "bmkisin", "dup_check_id", "dup_record_id",
    "benchmark_inst_code", "benchmark_inst_code_type", "benchmark_price",
    "benchmark_price_type", "apa_active", "apa_transaction_type", "apa_deferral_type",
    "apa_publish_datetime", "apa_publish_type", "apa_publish_flags",
    "apa_publish_status", "apa_full_details", "apa_deferral_level",
    "apa_deferral_reason", "principal_mktx_dealer", "counterparty_mktx_dealer",
    "public_trade_volume_eur", "public_trade_volume_ccy", "public_bond_volume_eur",
    "public_bond_volume_ccy", "trax_benchmark_isin", "trax_benchmark_yield",
    "trax_benchmark_spread", "price", "counterpartycodetype", "mktx_benchmark_yield",
    "apa_aggregate_count", "canbepublished", "direction", "apa_nonpub_ovrd",
    "canbesummarized_apa_ovrd", "trade_profile", "sector", "cappingapplied",
    "threshold_volume", "dms_seq_no", "trddate"
]

# ------------------------------
# Helpers
# ------------------------------

# LEI-like (20 upper alnum chars; not a real checksum)
_LEI_ALPHANUM = np.array(list(string.ascii_uppercase + string.digits))
def rand_lei(n: int) -> np.ndarray:
    return np.array(["".join(rng.choice(_LEI_ALPHANUM, size=20)) for _ in range(n)], dtype=object)

# ISIN helpers (with valid check digit)
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
    first = rng.choice(list("XZABCDEFGHJKLMNPQRSTUVWY"), size=n)  # avoid I/O confusions
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

_COUNTRIES = np.array(["US","GB","DE","FR","IT","ES","NL","IE","FI","AT","LU","SE","NO","CH","IL","AU"])
def random_isin(n, ccy_codes):
    country = rng.choice(_COUNTRIES, size=n)
    cusip9 = random_cusip9(n)
    base = np.char.add(country, cusip9)
    chk = np.array([isin_check_digit(b) for b in base])
    return np.char.add(base, chk), country

def str_or_null(arr, p_null=0.0):
    out = arr.astype(object)
    if p_null > 0:
        mask = rng.random(out.shape[0]) < p_null
        out[mask] = NULL
    return out

def now_utc():
    return datetime(2025, 9, 4, 12, 0, 0)

# ------------------------------
# Instrument universe
# ------------------------------
N_INSTR = 50_000
INSTR_CCYS = np.array(["EUR","USD","GBP","AUD","ILS","CHF"])
CCY_TO_EUR = {"EUR": 1.00, "USD": 0.86, "GBP": 1.17, "AUD": 0.62, "ILS": 0.28, "CHF": 1.05}
CCY_TO_USD = {"EUR": 1.17, "USD": 1.00, "GBP": 1.36, "AUD": 0.72, "ILS": 0.33, "CHF": 1.10}

instr_isin, instr_country = random_isin(N_INSTR, INSTR_CCYS)
instr_ccy = rng.choice(INSTR_CCYS, size=N_INSTR, p=[0.55,0.25,0.07,0.05,0.04,0.04])
instr_name = np.array([
    f"{rng.choice(['CORP','SOVN','MTGE','COVER','AGCY'])} {rng.integers(2026, 2046)} {ccy}"
    for ccy in instr_ccy
], dtype=object)

# Benchmarks
bench_isin, _ = random_isin(8_000, INSTR_CCYS)
bench_yield = rng.uniform(1.0, 6.0, size=bench_isin.size)
bench_spread = rng.uniform(10.0, 350.0, size=bench_isin.size)

# Dealers / BIC-ish
DEALERS = np.array(["JPMG","CITI","BOFA","MSCO","GSIL","BARX","STNX","UBSW","RBC","BNP"])
DEALER_BICS = np.array(["JPMAGB2LXXX","CITIGB2LXXX","BOFAGB2LXXX","MSCOGB2LXXX","GSILGB2LXXX",
                        "BARXGB2LXXX","STONXLON","UBSWGB2LXXX","RBOSGB2LXXX","BNPAFRPPXXX"])

# ------------------------------
# Main
# ------------------------------
def main():
    first = True
    written = 0
    row_seq = 1
    id_seq = 2_100_000_000  # arbitrary base

    while written < N_ROWS:
        n = min(CHUNK_SIZE, N_ROWS - written)

        # base fill with NULL
        d = {col: np.full(n, NULL, dtype=object) for col in COLUMNS}

        # time windows (last 7 days)
        t_now = now_utc()
        start = t_now - timedelta(days=7)
        tradedt = np.array([
            start + timedelta(seconds=int(rng.integers(0, 7*24*3600)))
            for _ in range(n)
        ])
        reported = tradedt + np.array([timedelta(seconds=int(rng.integers(3, 7200))) for _ in range(n)])

        currdate = np.array([dt.date().strftime("%Y-%m-%d 00:00:00.000") for dt in tradedt])
        trddate = np.array([dt.date().strftime("%Y-%m-%d") for dt in tradedt])

        # instruments & economics
        instr_idx = rng.integers(0, N_INSTR, size=n)
        isin = instr_isin[instr_idx]
        ccy = instr_ccy[instr_idx]
        name = instr_name[instr_idx]

        eur_fx = np.array([max(0.0001, CCY_TO_EUR[c]) for c in ccy]) * rng.uniform(0.99, 1.01, size=n)
        usd_fx = np.array([max(0.0001, CCY_TO_USD[c]) for c in ccy]) * rng.uniform(0.99, 1.01, size=n)

        dealpricetype = rng.choice(np.array(["PERC","YIEL"], dtype=object), size=n, p=[0.92,0.08])
        price = np.where(dealpricetype == "PERC",
                         rng.uniform(70.0, 110.0, size=n),
                         rng.uniform(1.0, 8.0, size=n))

        yld = np.where(dealpricetype == "PERC", np.maximum(0.5, 8.5 - (price - 70.0)*0.15), price)
        asky = yld + rng.uniform(0.00, 0.08, size=n)
        bidy = yld - rng.uniform(0.00, 0.08, size=n)
        isprd = rng.uniform(-120.0, 320.0, size=n)
        zsprd = isprd + rng.uniform(-5.0, 5.0, size=n)
        isw   = isprd + rng.uniform(-10.0, 10.0, size=n)
        asw   = isprd + rng.uniform(-10.0, 10.0, size=n)
        mktsp = isprd + rng.uniform( -5.0, 30.0, size=n)

        qty = np.round(rng.choice(
            np.array([1_000, 5_000, 10_000, 100_000, 1_000_000, 2_000_000, 6_000_000], dtype=float),
            size=n,
            p=[0.25,0.15,0.25,0.18,0.12,0.03,0.02]
        ), 3)

        qty_eur  = np.round(qty * (ccy == "EUR") + qty * (ccy != "EUR") * eur_fx, 6)
        qty_usd  = np.round(qty_eur * np.divide(usd_fx, np.maximum(eur_fx, 1e-6)), 6)

        side = rng.choice(np.array(["BUYR","SELL"], dtype=object), size=n)

        # Pairing
        pair_flags = rng.random(n) < 0.30
        pair_id = np.full(n, NULL, dtype=object)
        pair_ids = rng.integers(10**11, 10**12, size=int(pair_flags.sum()/2)+5)
        idxs = np.where(pair_flags)[0]
        rng.shuffle(idxs)
        for i in range(0, len(idxs)-1, 2):
            pid = f"{int(pair_ids[i//2])}"
            a, b = idxs[i], idxs[i+1]
            pair_id[a] = pid
            pair_id[b] = pid
            side[b] = "SELL" if side[a] == "BUYR" else "BUYR"

        # counterparties & firms
        cpty_code = rand_lei(n)
        principal_code = rand_lei(n)
        DEALERS = np.array(["JPMG","CITI","BOFA","MSCO","GSIL","BARX","STNX","UBSW","RBC","BNP"])
        firm_id_principal = rng.choice(DEALERS, size=n)
        firm_id_cpty = rng.choice(DEALERS, size=n)
        cpty_bic = rng.choice(np.append(DEALER_BICS, np.array([NULL]*5)), size=n)

        # IDs & keys
        idx_rows = np.arange(row_seq, row_seq + n, dtype=np.int64)
        row_seq += n

        ids = id_seq + rng.integers(0, 9_999_999, size=n)
        id_seq += n

        tradedetailsid = 10**11 + rng.integers(0, 9_999_999, size=n)
        record_id = 10**11 + rng.integers(0, 9_999_999, size=n)
        block_key = str_or_null((10**11 + rng.integers(0, 9_999_999, size=n)).astype(object), 0.85)
        matched_block_key = str_or_null((10**11 + rng.integers(0, 9_999_999, size=n)).astype(object), 0.92)

        global_trade_key = np.array([
            f"{tradedt[i].strftime('%Y%m%d%H%M%S')}{i:021d}" for i in range(n)
        ], dtype=object)

        # --- FIX: cast NumPy integers to Python int in timedelta args ---
        hours_off = rng.integers(0, 48, size=n)
        replication_ts = np.array([
            (tradedt[i] + timedelta(hours=int(hours_off[i]))).strftime("%Y-%m-%d %H:%M:%S.%f")
            for i in range(n)
        ], dtype=object)

        mins_off_create = rng.integers(0, 120, size=n)
        create_dt = np.array([
            (reported[i] + timedelta(minutes=int(mins_off_create[i]))).strftime("%Y-%m-%d %H:%M:%S.%f")
            for i in range(n)
        ], dtype=object)

        mins_off_update = rng.integers(0, 180, size=n)
        update_dt = np.array([
            (reported[i] + timedelta(minutes=int(mins_off_update[i]))).strftime("%Y-%m-%d %H:%M:%S.%f")
            for i in range(n)
        ], dtype=object)
        # ----------------------------------------------------------------

        # APA-ish fields for EUR trades
        apa_active = np.where(ccy == "EUR", "NEW", NULL)
        apa_publish_type = np.where(ccy == "EUR", "PUB_FULL", NULL)
        apa_publish_dt = np.where(
            ccy == "EUR",
            np.array([reported[i].strftime("%Y-%m-%d %H:%M:%S.000") for i in range(n)], dtype=object),
            NULL
        )

        # settlement / venue
        place_of_trade = rng.choice(np.array(["XOFF","SINT","OFF"], dtype=object), size=n, p=[0.75,0.20,0.05])
        place_type     = np.where(place_of_trade == "SINT", "EXCH", "OTCO")
        tradetx_type   = np.full(n, "TRAD", dtype=object)
        trans_status   = np.full(n, "NMT", dtype=object)
        sp_cond        = rng.choice(np.array(["SP", ""], dtype=object), size=n, p=[0.25,0.75])
        not_sum_reason = np.where(sp_cond=="SP", "Special price is indicated in the trade", NULL)

        denom = ccy.copy()
        qty_type = np.full(n, "FAMT", dtype=object)
        trad_subacct = np.full(n, "NMT", dtype=object)
        cpty_code_type = np.where(rng.random(n) < 0.6, "LEI", NULL)
        cpty_code_type_num = np.where(cpty_code_type == "LEI", "2.0", NULL)

        icma_instr_isin = isin
        icma_instr_name = name

        # Benchmarks (optional)
        use_bmk = rng.random(n) < 0.1
        bmk_idx = rng.integers(0, bench_isin.size, size=n)
        bmk_isin = np.where(use_bmk, bench_isin[bmk_idx], NULL)
        bmk_yld  = np.where(use_bmk, np.round(bench_yield[bmk_idx], 10).astype(object), NULL)
        bmk_spd  = np.where(use_bmk, np.round(bench_spread[bmk_idx], 10).astype(object), NULL)

        # Direction / publish flags
        direction = np.where(side == "BUYR", "P", "Y")
        canbepublished = np.where(ccy == "EUR", "F", "N")
        trade_profile = np.where(
            rng.random(n) < 0.2, "Client Buy",
            np.where(rng.random(n) < 0.4, "Client Sell", "Unidentified")
        )
        sector = rng.choice(np.array(["CORP","SOVN","COVER","AGCY"], dtype=object), size=n, p=[0.6,0.32,0.05,0.03])

        duplicate_flag = np.where(pair_id != NULL, "DUP-OPPOSITE", NULL)
        duplicate_key_straight = str_or_null(record_id.astype(object), 0.85)
        duplicate_key_reversed = str_or_null(record_id.astype(object), 0.90)

        tradetimerank = str_or_null(rng.integers(1, 10, size=n).astype(object), 0.7)

        # Assign columns
        d["#"] = idx_rows.astype(object)
        d["replication_ts"] = replication_ts
        d["id"] = np.array([f"{float(x):.10f}" for x in ids], dtype=object)
        d["currdate"] = currdate
        d["counterpartycode"] = cpty_code
        d["icmainstrumentidentifierisin"] = isin
        d["principalcodeidtrax"] = isin
        d["purchasesellindicator"] = side
        d["dealprice"] = np.array([f"{p:.10f}" for p in price], dtype=object)
        d["quantityoffinancialinstrument"] = np.array([f"{q:.10f}" for q in qty], dtype=object)
        d["tradedateandtime"] = np.array([dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] for dt in tradedt], dtype=object)
        d["reporteddatetime"] = np.array([dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] for dt in reported], dtype=object)
        d["quantityinusd"] = np.array([f"{v:.10f}" for v in qty_usd], dtype=object)
        d["quantityineuro"] = np.array([f"{v:.10f}" for v in qty_eur], dtype=object)
        d["quantityineuroest"] = d["quantityineuro"]

        d["ispread"] = np.array([f"{v:.10f}" for v in isprd], dtype=object)
        d["zspread"] = np.array([f"{v:.10f}" for v in zsprd], dtype=object)
        d["ispreadswaps"] = np.array([f"{v:.10f}" for v in isw], dtype=object)
        d["aswspread"] = np.array([f"{v:.10f}" for v in asw], dtype=object)
        d["marketspread"] = np.array([f"{v:.10f}" for v in mktsp], dtype=object)
        d["yield"] = np.array([f"{v:.10f}" for v in yld], dtype=object)
        d["askyield"] = np.array([f"{v:.10f}" for v in asky], dtype=object)
        d["bidyield"] = np.array([f"{v:.10f}" for v in bidy], dtype=object)
        d["quantityinccyest"] = d["quantityoffinancialinstrument"]
        d["is_block"] = rng.choice(np.array(["P","N","Y",""], dtype=object), size=n, p=[0.15,0.65,0.05,0.15])

        d["tradetimerank"] = tradetimerank
        d["create_date"] = create_dt
        d["update_date"] = update_dt
        d["counterpartybic"] = cpty_bic
        d["counterpartycodetype_numeric"] = cpty_code_type_num
        d["createddatetime"] = create_dt
        d["dealpricetype"] = dealpricetype
        d["denomination"] = ccy
        d["icmafinancialinstrumentname"] = icma_instr_name

        d["modifieddatetime"] = update_dt
        d["qtyoffinancialinstrumenttype"] = qty_type

        # settlementdate: FIX cast to int for timedelta days
        settle_days = rng.integers(1, 3, size=n)
        d["settlementdate"] = np.array([
            (tradedt[i] + timedelta(days=int(settle_days[i]))).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
            for i in range(n)
        ], dtype=object)

        d["tradesubaccount"] = trad_subacct
        d["tradetransactiontype"] = tradetx_type
        d["transactionstatus"] = trans_status
        d["sp_condition"] = sp_cond
        d["notsummarizedreason"] = not_sum_reason

        d["icmainstrumentidentifiericma"] = np.where(rng.random(n) < 0.9, isin, NULL)
        d["principalcodevaluetrax"] = np.where(rng.random(n) < 0.5, rand_lei(n), NULL)

        d["ccyeurfxrate"] = np.array([f"{v:.10f}" for v in eur_fx], dtype=object)
        d["ccyusdfxrate"] = np.array([f"{v:.10f}" for v in usd_fx], dtype=object)
        d["tradedetailsid"] = np.array([f"{float(x):.10f}" for x in tradedetailsid], dtype=object)
        d["trade_quality"] = rng.choice(np.array(["NPM","PMT","APA-NON-PUB",""], dtype=object), size=n, p=[0.6,0.25,0.05,0.10])

        d["block_key"] = block_key
        d["matched_block_key"] = matched_block_key
        d["record_id"] = np.array([f"{float(x):.10f}" for x in record_id], dtype=object)
        d["trade_pair_id"] = pair_id
        d["passive_match_status"] = str_or_null(rng.choice(np.array(["", "Y", "N", "P"], dtype=object), size=n, p=[0.7,0.1,0.15,0.05]), 0.4)

        d["sp_condition_principal"] = rng.choice(np.array(["SP",""], dtype=object), size=n, p=[0.05,0.95])
        d["sp_condition_cpty"] = rng.choice(np.array(["SP",""], dtype=object), size=n, p=[0.05,0.95])

        d["firm_id_principal"] = firm_id_principal
        d["firm_role_principal"] = rng.choice(np.array(["BRKR-DLR","BUY-SIDE","BANK","IDB"], dtype=object), size=n, p=[0.7,0.15,0.1,0.05])
        d["firm_id_cpty"] = firm_id_cpty
        d["firm_role_cpty"] = rng.choice(np.array(["BRKR-DLR","BUY-SIDE","BANK","IDB"], dtype=object), size=n, p=[0.4,0.4,0.15,0.05])

        d["counterpartytrax"] = rng.choice(np.append(DEALERS, np.array([NULL]*3)), size=n)
        d["counterpartynarrative"] = rng.choice(np.array(["", "Client Buy", "Client Sell", "Inter-Dealer", "Unidentified"], dtype=object), size=n, p=[0.4,0.2,0.2,0.1,0.1])

        d["placeoftrade"] = place_of_trade
        d["placeoftradetype"] = place_type

        d["statusdatetime"] = d["reporteddatetime"]
        d["archivedate"] = np.array([reported[i].strftime("%Y-%m-%d") for i in range(n)], dtype=object)

        d["delete_flag"] = np.full(n, "N", dtype=object)
        d["global_trade_key"] = global_trade_key
        d["source"] = rng.choice(np.array(["ARM_REPORT","TRAX_APA","TRAX_MATCH"], dtype=object), size=n, p=[0.45,0.35,0.20])

        d["duplicate_flag"] = duplicate_flag
        d["duplicate_key_straight"] = duplicate_key_straight
        d["duplicate_key_reversed"] = duplicate_key_reversed

        d["bmkisin"] = bmk_isin
        d["trax_benchmark_isin"] = bmk_isin
        d["trax_benchmark_yield"] = bmk_yld
        d["trax_benchmark_spread"] = bmk_spd

        d["benchmark_inst_code"] = str_or_null(bmk_isin.copy(), 0.6)
        d["benchmark_inst_code_type"] = np.where(d["benchmark_inst_code"] != NULL, "ISIN", NULL)
        d["benchmark_price"] = str_or_null(np.array([f"{v:.10f}" for v in rng.uniform(80, 105, size=n)], dtype=object), 0.8)
        d["benchmark_price_type"] = np.where(d["benchmark_price"] != NULL, "PERC", NULL)

        d["apa_active"] = apa_active
        d["apa_publish_type"] = apa_publish_type
        d["apa_publish_datetime"] = apa_publish_dt
        d["apa_publish_flags"] = np.where(apa_active == "NEW", "F", NULL)
        d["apa_publish_status"] = np.where(apa_active == "NEW", "", NULL)
        d["apa_full_details"] = np.where(apa_active == "NEW", "", NULL)

        d["price"] = d["dealprice"]
        d["counterpartycodetype"] = cpty_code_type
        d["mktx_benchmark_yield"] = str_or_null(np.array([f"{v:.10f}" for v in rng.uniform(1.0, 7.0, size=n)], dtype=object), 0.9)
        d["apa_aggregate_count"] = np.where(apa_active == "NEW", "8.0000000000", NULL)
        d["canbepublished"] = canbepublished
        d["direction"] = direction
        d["trade_profile"] = trade_profile
        d["sector"] = sector
        d["cappingapplied"] = np.where((apa_active == "NEW") & (qty_eur > 100_000), "N", NULL)

        d["threshold_volume"] = np.where((apa_active == "NEW"), "", NULL)
        d["dms_seq_no"] = np.array([
            f"{tradedt[i].strftime('%Y%m%d%H%M%S')}{int(rng.integers(0, 10**11)):011d}"
            for i in range(n)
        ], dtype=object)
        d["trddate"] = trddate

        df = pd.DataFrame({col: d[col] for col in COLUMNS})

        df.to_csv(
            OUTFILE,
            mode="w" if first else "a",
            index=False,
            header=first
        )
        written += n
        first = False
        print(f"Wrote {written:,} / {N_ROWS:,}")

    print(f"Done → {OUTFILE}")

if __name__ == "__main__":
    main()
