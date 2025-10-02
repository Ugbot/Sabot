import numpy as np
import pandas as pd
from datetime import datetime, timedelta, date
import math
import string
import random

# -----------------------------
# Config
# -----------------------------
N_ROWS = 10_000_000
SEED = 7
OUTFILE = "master_security_10m.csv"   # plain CSV (no gzip)
CHUNK_SIZE = 500_000                  # tune per machine
NULL = "NULL"

rng = np.random.default_rng(SEED)
random.seed(SEED)

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
payment_types = np.array(list("NFF"))  # N or F, bias F
interest_rate_types = np.array(["F","N"])  # Fixed / FRN-like
bond_types = np.array(["Q","S"])
accrual_methods = np.array(["2","3"])
payment_freqs = np.array(["Q","S","A"])  # Quarterly/Semi/Annual
redemptions = np.array(["Sr","Sr Secured","Unsecrd Nt","Sub Note","Sr Note","Sr Deb"])
trailer_vals = np.array(["", "AMR", "DMTC", "GLOB", "FRGN", "NAC", "ISS", "EVL"])
security_types = np.array(["Financials","Industrials","Utilities","Technology","Consumer","Energy"])
flt_formula_bases = np.array(["US3MLIB","US6MLIB","SOFR3M","EUR3M","GBP3M"])
vendor_names = np.array(["EJV","BBG","IDC"])
asset_status = np.array(["MAT","NAC","ISS","SB","SU","SS"])
market_segments = np.array(["1","2","3","4"])
market_category = np.array(["1"])
market_subcat = np.array(["","FRGN","GLOB","DMTC","AMR","NAC"])
ratings_fitch = np.array(["AAA","AA+","AA","AA-","A+","A","A-","BBB+","BBB","BBB-","BB+","NR","NA"])
ratings_snp   = np.array(["AAA","AA+","AA","AA-","A+","A","A-","BBB+","BBB","BBB-","BB+","NR","NA"])
ratings_mood  = np.array(["Aaa","Aa2","Aa3","A1","A2","A3","Baa1","Baa2","Baa3","Ba1","Ba2","NR","NA"])
boolYN = np.array(["Y","N"])
sector_ids = np.arange(10000, 10099)

# -----------------------------
# Helpers: identifiers & checksums
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
# Value generators
# -----------------------------
def random_dates(n):
    start = datetime(1996, 1, 1)
    end = datetime(2025, 9, 1)
    span = (end - start).days
    issue = np.array([start + timedelta(days=int(rng.integers(0, span))) for _ in range(n)])
    accrual = issue.copy()

    fp_offset = rng.integers(60, 200, size=n)
    sp_offset = fp_offset + rng.integers(30, 180, size=n)
    first_pay = np.array([issue[i] + timedelta(days=int(fp_offset[i])) for i in range(n)])
    second_pay = np.array([issue[i] + timedelta(days=int(sp_offset[i])) for i in range(n)])

    fp_null = rng.random(n) < 0.15
    sp_null = rng.random(n) < 0.25

    mat_years = rng.integers(2, 41, size=n)
    maturity = np.array([date(issue[i].year + int(mat_years[i]), max(1, issue[i].month), 1) for i in range(n)])

    today = datetime(2025, 9, 4)
    last_pay = np.minimum(first_pay, np.full(n, today))
    last_pay_null = rng.random(n) < 0.35

    eff_delta = rng.integers(-15, 30, size=n)
    effective_date = np.array([issue[i] + timedelta(days=int(eff_delta[i])) for i in range(n)])
    first_settle = np.array([issue[i] + timedelta(days=int(rng.integers(2, 11))) for i in range(n)])

    def fmt_dt(dt_arr, null_mask=None, date_only=False):
        out = np.empty(n, dtype=object)
        for i, dt in enumerate(dt_arr):
            if null_mask is not None and null_mask[i]:
                out[i] = NULL
            else:
                if date_only:
                    if isinstance(dt, datetime):
                        dt = dt.date()
                    out[i] = dt.strftime("%Y-%m-%d")
                else:
                    if isinstance(dt, date):
                        dt = datetime(dt.year, dt.month, dt.day)
                    out[i] = dt.strftime("%Y-%m-%d %H:%M:%S")
        return out

    return {
        "ISSUEDATE": fmt_dt(issue, date_only=True),
        "ACCRUALSTART": fmt_dt(accrual, date_only=True),
        "FIRSTPAYMENT": fmt_dt(first_pay, fp_null, date_only=True),
        "SECONDPAYMENT": fmt_dt(second_pay, sp_null, date_only=True),
        "MATURITY": fmt_dt(maturity, date_only=True),
        "LASTPAYMENT": fmt_dt(last_pay, last_pay_null, date_only=True),
        "TIMESTAMP": fmt_dt(np.array([issue[i] + timedelta(days=int(rng.integers(0, 3000))) for i in range(n)])),
        "FIRSTSETTLEMENTDATE": fmt_dt(first_settle, date_only=True),
        "EFFECTIVE_DATE": fmt_dt(effective_date, date_only=True),
    }

def random_ratings(n):
    f = rng.choice(ratings_fitch, size=n)
    s = rng.choice(ratings_snp, size=n)
    m = rng.choice(ratings_mood, size=n)
    ig_map = {"AAA","AA+","AA","AA-","A+","A","A-","BBB+","BBB","Baa1","Baa2","Baa3"}
    ig = np.array(["Y" if (f[i] in ig_map or s[i] in ig_map or m[i] in ig_map) else "N" for i in range(n)], dtype=object)
    ig_nasd = ig.copy()
    flip = rng.random(n) < 0.03
    ig_nasd[flip] = np.where(ig_nasd[flip] == "Y", "N", "Y")
    return f, s, m, f.copy(), s.copy(), m.copy(), ig, ig_nasd

def random_coupons(n):
    fixed = rng.random(n) < 0.8
    coup = np.empty(n, dtype=float)
    coup[fixed] = rng.uniform(0.02, 0.09, size=fixed.sum())
    coup[~fixed] = rng.uniform(0.005, 0.03, size=(~fixed).sum())
    return np.round(coup, 6), fixed

def random_frn_fields(n):
    have = rng.random(n) < 0.15
    idx = np.full(n, NULL, dtype=object)
    basis = np.full(n, NULL, dtype=object)
    idx[have] = rng.choice(flt_formula_bases, size=have.sum())
    basis[have] = np.round(rng.uniform(0.0, 2.0, size=have.sum()), 2).astype(str)
    return idx, basis

def str_or_null(arr, null_p=0.0):
    mask = rng.random(arr.shape[0]) < null_p
    out = arr.astype(object)
    out[mask] = NULL
    return out

def random_money(n, low=25_000_000, high=1_000_000_000, allow_zero_rate=0.4):
    amt = rng.integers(low, high, size=n, dtype=np.int64)
    out = amt.copy()
    zero_mask = rng.random(n) < allow_zero_rate
    out[zero_mask] = 0
    return amt.astype(object), out.astype(object)

def make_shortname(issue_name, coupon, maturity):
    return f"{issue_name.split()[0][:4].upper():<4} {coupon:.3f} {maturity[5:7]}/{maturity[2:4]}"

def rand_alnum(n, k):
    letters = string.ascii_uppercase + string.digits
    return np.array(["".join(rng.choice(list(letters), size=k)) for _ in range(n)])

# -----------------------------
# Columns
# -----------------------------
columns = [
"ID","NAME","COUPON","MATURITY","CUSIP","ISSUEDATE","ACCRUALSTART","FIRSTPAYMENT","SECONDPAYMENT",
"PAYMENTTYPE","INTERESTRATETYPE","BONDTYPE","DEFAULTED","ACCRUALMETHOD","PAYMENTFREQUENCY","REDEMPTION",
"TRAILER","DESCRIPTION","ISDELETED","ISMULTICOUPON","TIMESTAMP","LASTPAYMENT","ISIN","COUNTRYCODE",
"CLEARINGHOUSE","MINLOTSIZE","MARKETSEGMENT","MARKETCATEGORY","MARKETSUBCATEGORY","COMMISSIONRATE",
"FRNINDEX","FRNBASIS","SHORTNAME","ISSUER","ISCVTPREFSTOCK","PARVALUE","FITCHRATING","SNPRATING",
"MOODYRATING","ISOCURRENCYCODE","SECTORID","NOUPDATEFROMFEED","ISSUEAMOUNT","AMOUNTOUTSTANDING",
"FIRSTSETTLEMENTDATE","CALLABLE","PUTABLE","SINKABLE","MAKEWHOLE","NEXTCALLDATE","NEXTPUTDATE",
"NEXTSINKDATE","SECURITYTYPE","FLTFORMULA","FLTMULT","FLTCAP","FLTFLOOR","VENDORNAME","TRACECODE",
"INDUSTRY","SECTOR","DEFAULTTICKER","ASSET_STATUS_CD","ASSET_SUBTYPE_CD","PARTY_ID","ASSET_ID",
"FITCHRATINGDISP","SNPRATINGDISP","MOODYRATINGDISP","CURRENCYCODE","ISINVESTMENTGRADE",
"IS_INVESTMENTGRADE_NASD","PARENT_PARTY_ID","TRADING_SECTOR","BMKATISSUEID","ISFDICBOND","WKN",
"MINDENOMINATION","DENOMINATIONINCR","LEADUNDERWRITERCD","DV01WORST","MODDURWORST","MTN",
"MDY_DEBT_CLASS_CD","CALL_OPT_CD","EFFECTIVE_DATE","SPREADATISSUE","BLOOMBERGSYMBOL",
"SUBPRODUCTTYPE","NASDSYMBOL","PRODUCTID","CINS","SM_CUSIP","VENDORID","GREEN_BOND_FL"
]

# -----------------------------
# Main
# -----------------------------
def main():
    first = True
    written = 0
    next_id = 100_000_00  # arbitrary starting ID

    while written < N_ROWS:
        n = min(CHUNK_SIZE, N_ROWS - written)

        ids = np.arange(next_id, next_id + n, dtype=np.int64)
        next_id += n

        # Issuer names with robust suffix handling
        issuer = rng.choice(np.array(issuer_roots, dtype=object), size=n).astype(object)
        suffix_mask = rng.random(n) < 0.25
        if np.any(suffix_mask):
            suffix_choices = rng.choice(SUFFIX_POOL, size=int(suffix_mask.sum()))
            base = issuer[suffix_mask].tolist()
            issuer[suffix_mask] = [a + b for a, b in zip(base, suffix_choices.tolist())]

        dts = random_dates(n)
        coupon, is_fixed = random_coupons(n)

        country = rng.choice(countries, size=n, p=[0.55,0.06,0.12,0.06,0.05,0.05,0.04,0.04,0.03])
        currency = rng.choice(currencies, size=n, p=[0.7,0.12,0.1,0.05,0.03])

        cusip9 = random_cusip(n)
        isin = build_isin(country, cusip9)

        sector = rng.choice(sectors, size=n)
        industry = np.empty(n, dtype=object)
        for s in np.unique(sector):
            idx = np.where(sector == s)[0]
            industry[idx] = rng.choice(np.array(industries_by_sector[s], dtype=object), size=idx.size)

        sector_id = rng.choice(sector_ids, size=n)

        payment_type = rng.choice(payment_types, size=n, p=[0.25,0.75,0.0])
        ir_type = np.where(is_fixed, "F", "N")
        bond_type = rng.choice(bond_types, size=n, p=[0.35,0.65])
        defaulted = rng.choice(np.array(["N","Y"]), size=n, p=[0.995, 0.005])
        accrual_method = rng.choice(accrual_methods, size=n, p=[0.6,0.4])
        pay_freq = rng.choice(payment_freqs, size=n, p=[0.25,0.6,0.15])
        redemption = rng.choice(redemptions, size=n, p=[0.35,0.12,0.18,0.15,0.15,0.05])
        trailer = str_or_null(rng.choice(trailer_vals, size=n, p=[0.25,0.12,0.2,0.18,0.08,0.07,0.05,0.05]), null_p=0.25)
        isdeleted = rng.choice(np.array(["N","Y"]), size=n, p=[0.98,0.02])
        ismulticoupon = rng.choice(np.array(["N","Y"]), size=n, p=[0.96,0.04])

        clearing = str_or_null(rand_alnum(n, 3), 0.92)
        minlotsize = rng.choice(np.array([1, 7, 4], dtype=object), size=n, p=[0.6, 0.2, 0.2])
        mkt_seg = rng.choice(market_segments, size=n, p=[0.4,0.3,0.2,0.1])
        mkt_cat = rng.choice(market_category, size=n)
        mkt_sub = rng.choice(market_subcat, size=n, p=[0.55,0.05,0.1,0.1,0.1,0.1])

        commission_rate = np.where(rng.random(n) < 0.15,
                                   np.round(rng.uniform(0.5, 1.5, size=n), 2).astype(object),
                                   np.array([""], dtype=object))

        frn_index, frn_basis = random_frn_fields(n)

        maturity_str = dts["MATURITY"]
        shortname = np.array([make_shortname(issuer[i], coupon[i], maturity_str[i]) for i in range(n)], dtype=object)
        name = issuer
        description = shortname
        default_ticker = np.array([sn.split()[0].replace(" ", "")[:4] for sn in shortname], dtype=object)

        parvalue = rng.choice(np.array([1000, 25000, 100, 97, 99], dtype=object), size=n, p=[0.7, 0.05, 0.05, 0.1, 0.1])
        issue_amt, amt_out = random_money(n)
        min_denom = rng.choice(np.array([1000, 500, 2000, 25000], dtype=object), size=n, p=[0.7,0.05,0.2,0.05])
        denom_incr = rng.choice(np.array([1_000, 2_000, 5_000, 1], dtype=object), size=n, p=[0.65,0.2,0.1,0.05])

        fr, sr, mr, frd, srd, mrd, ig, ig_nasd = random_ratings(n)

        callable_f = rng.choice(boolYN, size=n, p=[0.1,0.9])
        putable_f  = rng.choice(boolYN, size=n, p=[0.03,0.97])
        sinkable_f = rng.choice(boolYN, size=n, p=[0.06,0.94])
        makewhole  = rng.choice(boolYN, size=n, p=[0.08,0.92])
        mtn        = rng.choice(boolYN, size=n, p=[0.15,0.85])

        next_call = str_or_null(dts["MATURITY"], 0.92)
        next_put  = str_or_null(dts["FIRSTPAYMENT"], 0.95)
        next_sink = str_or_null(dts["SECONDPAYMENT"], 0.95)

        sec_type = rng.choice(security_types, size=n)
        flt_mult = str_or_null(np.round(rng.uniform(0.5, 1.5, size=n), 2).astype(str), 0.85)
        flt_cap  = str_or_null(np.round(rng.uniform(4.0, 12.0, size=n), 2).astype(str), 0.92)
        flt_floor= str_or_null(np.round(rng.uniform(0.0, 2.0, size=n), 2).astype(str), 0.88)
        flt_formula = np.full(n, NULL, dtype=object)
        set_flt = (ir_type == "N") & (rng.random(n) < 0.6)
        if np.any(set_flt):
            ff = []
            for i in range(n):
                if set_flt[i]:
                    base = frn_index[i]
                    basis = frn_basis[i] if frn_basis[i] != NULL else "0.25"
                    if base == NULL:
                        base = rng.choice(flt_formula_bases)
                    ff.append(f"1.00*#{base}<2d+{basis}")
                else:
                    ff.append(NULL)
            flt_formula = np.array(ff, dtype=object)

        vendor = rng.choice(vendor_names, size=n)
        trace  = rng.choice(np.array(["D","S","F",""], dtype=object), size=n, p=[0.5,0.2,0.05,0.25])

        asset_status_cd = rng.choice(asset_status, size=n, p=[0.25,0.15,0.15,0.02,0.35,0.08])
        asset_subtype_cd = rng.choice(np.array(["DMTC","AMR","GLOB","FRGN","MAT",""], dtype=object), size=n, p=[0.25,0.15,0.15,0.05,0.2,0.2])

        party_id = np.array([f"0x{rng.integers(0, 2**48-1, dtype=np.uint64):012X}"], dtype=object)
        party_id = np.repeat(party_id, n)
        asset_id = np.array([f"0x{rng.integers(0, 2**60-1, dtype=np.uint64):015X}"], dtype=object)
        asset_id = np.repeat(asset_id, n)
        parent_party_id = np.array([f"0x{rng.integers(0, 2**48-1, dtype=np.uint64):012X}"], dtype=object)
        parent_party_id = np.repeat(parent_party_id, n)

        trading_sector = sector
        bmk_at_issue_id = str_or_null(np.array([f"0x{rng.integers(0, 2**48-1, dtype=np.uint64):012X}" for _ in range(n)], dtype=object), 0.8)

        is_fdic = rng.choice(np.array(["Y","N"]), size=n, p=[0.01,0.99])
        wkn = str_or_null(rand_alnum(n, 6), 0.85)

        lead_uw = rng.choice(np.array(["JPM","BAC","CITI","DBK","BAR","MER","LEH","GS","MS","RBC","ANV","WCG","CIT"], dtype=object), size=n)
        dv01w = str_or_null(np.round(rng.uniform(0.0, 0.15, size=n), 5).astype(str), 0.85)
        modd_worst = str_or_null(np.round(rng.uniform(0.0, 12.0, size=n), 6).astype(str), 0.8)

        mdy_debt_cls = rng.choice(np.array(["CORP","GOVT","MUNI","AGCY"], dtype=object), size=n, p=[0.85,0.05,0.05,0.05])
        call_opt_cd = rng.choice(np.array(["", "C", "P", "CP"], dtype=object), size=n, p=[0.8,0.12,0.06,0.02])

        spread_at_issue = str_or_null(np.round(rng.uniform(0.0, 300.0, size=n), 6).astype(str), 0.9)
        bbg_sym = str_or_null(np.array([f"{default_ticker[i]}.{rng.choice(['UQ','GA','IC','RU','GE','HF'])}" for i in range(n)], dtype=object), 0.6)

        subproduct = rng.choice(np.array(["CORP","MTN","PERP","PFD",""], dtype=object), size=n, p=[0.6,0.2,0.05,0.05,0.1])
        nasd_sym = str_or_null(np.array([default_ticker[i] for i in range(n)], dtype=object), 0.7)
        product_id = rng.integers(300, 900, size=n).astype(object)
        cins = str_or_null(rand_alnum(n, 9), 0.9)
        sm_cusip = cusip9
        vendor_id = rng.integers(100_000, 999_999, size=n).astype(object)
        green_flag = rng.choice(np.array(["Y","N"], dtype=object), size=n, p=[0.03,0.97])

        fr_disp, sr_disp, mr_disp = fr.copy(), sr.copy(), mr.copy()
        commission_rate = np.where(commission_rate == "", "", commission_rate)

        df = pd.DataFrame({
            "ID": ids,
            "NAME": name,
            "COUPON": np.round(coupon, 6),
            "MATURITY": dts["MATURITY"],
            "CUSIP": cusip9,
            "ISSUEDATE": dts["ISSUEDATE"],
            "ACCRUALSTART": dts["ACCRUALSTART"],
            "FIRSTPAYMENT": dts["FIRSTPAYMENT"],
            "SECONDPAYMENT": dts["SECONDPAYMENT"],
            "PAYMENTTYPE": payment_type,
            "INTERESTRATETYPE": ir_type,
            "BONDTYPE": bond_type,
            "DEFAULTED": defaulted,
            "ACCRUALMETHOD": accrual_method,
            "PAYMENTFREQUENCY": pay_freq,
            "REDEMPTION": redemption,
            "TRAILER": trailer,
            "DESCRIPTION": description,
            "ISDELETED": isdeleted,
            "ISMULTICOUPON": ismulticoupon,
            "TIMESTAMP": dts["TIMESTAMP"],
            "LASTPAYMENT": dts["LASTPAYMENT"],
            "ISIN": isin,
            "COUNTRYCODE": country,
            "CLEARINGHOUSE": clearing,
            "MINLOTSIZE": minlotsize,
            "MARKETSEGMENT": mkt_seg,
            "MARKETCATEGORY": mkt_cat,
            "MARKETSUBCATEGORY": mkt_sub,
            "COMMISSIONRATE": commission_rate,
            "FRNINDEX": frn_index,
            "FRNBASIS": frn_basis,
            "SHORTNAME": shortname,
            "ISSUER": issuer,
            "ISCVTPREFSTOCK": rng.choice(np.array(["N","Y"]), size=n, p=[0.97,0.03]),
            "PARVALUE": parvalue,
            "FITCHRATING": fr,
            "SNPRATING": sr,
            "MOODYRATING": mr,
            "ISOCURRENCYCODE": currency,
            "SECTORID": sector_id,
            "NOUPDATEFROMFEED": rng.choice(np.array(["N","Y"]), size=n, p=[0.97,0.03]),
            "ISSUEAMOUNT": issue_amt,
            "AMOUNTOUTSTANDING": amt_out,
            "FIRSTSETTLEMENTDATE": dts["FIRSTSETTLEMENTDATE"],
            "CALLABLE": callable_f,
            "PUTABLE": putable_f,
            "SINKABLE": sinkable_f,
            "MAKEWHOLE": makewhole,
            "NEXTCALLDATE": next_call,
            "NEXTPUTDATE": next_put,
            "NEXTSINKDATE": next_sink,
            "SECURITYTYPE": sec_type,
            "FLTFORMULA": flt_formula,
            "FLTMULT": flt_mult,
            "FLTCAP": flt_cap,
            "FLTFLOOR": flt_floor,
            "VENDORNAME": vendor,
            "TRACECODE": trace,
            "INDUSTRY": industry,
            "SECTOR": sector,
            "DEFAULTTICKER": default_ticker,
            "ASSET_STATUS_CD": asset_status_cd,
            "ASSET_SUBTYPE_CD": asset_subtype_cd,
            "PARTY_ID": party_id,
            "ASSET_ID": asset_id,
            "FITCHRATINGDISP": fr_disp,
            "SNPRATINGDISP": sr_disp,
            "MOODYRATINGDISP": mr_disp,
            "CURRENCYCODE": currency,
            "ISINVESTMENTGRADE": ig,
            "IS_INVESTMENTGRADE_NASD": ig_nasd,
            "PARENT_PARTY_ID": parent_party_id,
            "TRADING_SECTOR": trading_sector,
            "BMKATISSUEID": bmk_at_issue_id,
            "ISFDICBOND": is_fdic,
            "WKN": wkn,
            "MINDENOMINATION": min_denom,
            "DENOMINATIONINCR": denom_incr,
            "LEADUNDERWRITERCD": lead_uw,
            "DV01WORST": dv01w,
            "MODDURWORST": modd_worst,
            "MTN": mtn,
            "MDY_DEBT_CLASS_CD": mdy_debt_cls,
            "CALL_OPT_CD": call_opt_cd,
            "EFFECTIVE_DATE": dts["EFFECTIVE_DATE"],
            "SPREADATISSUE": spread_at_issue,
            "BLOOMBERGSYMBOL": bbg_sym,
            "SUBPRODUCTTYPE": subproduct,
            "NASDSYMBOL": nasd_sym,
            "PRODUCTID": product_id,
            "CINS": cins,
            "SM_CUSIP": sm_cusip,
            "VENDORID": vendor_id,
            "GREEN_BOND_FL": green_flag,
        }, columns=columns)

        df = df.where(pd.notnull(df), NULL)

        df.to_csv(
            OUTFILE,
            mode="w" if first else "a",
            index=False,
            header=first,
        )
        written += n
        first = False
        print(f"Wrote {written:,} / {N_ROWS:,}")

    print(f"Done → {OUTFILE}")

if __name__ == "__main__":
    main()
