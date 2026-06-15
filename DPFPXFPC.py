
from __future__ import annotations

from pathlib import Path
from datetime import datetime, date
import pyarrow as pa
import pyarrow.parquet as pq
import duckdb
import polars as pl

# ============================================
# PATHS (adjust to your environment)
# ============================================
BASE_INPUT = Path("parquet_input")     # root for input Parquet
BASE_OUT   = Path("parquet_output")    # for optional intermediate Parquet outputs
BASE_TXT   = Path("text_output")       # final text output folder
BASE_OUT.mkdir(parents=True, exist_ok=True)
BASE_TXT.mkdir(parents=True, exist_ok=True)

# Inputs corresponding to DD names:
# - BNM1.REPTDATE  -> one-row parquet with column REPTDATE (date/datetime or yyyymmdd int/str)
# - BNM1.IBTRAD&REPTMON&NOWK -> naming pattern; see below
REPTDATE_PARQUET = BASE_INPUT / "BNM1_REPTDATE.parquet"

# Output “host dataset” mirror
NLFBT_TXT = BASE_TXT / "SAP.PIBB.ALM.NLFBT.TEXT.txt"

# ============================================
# HELPERS: date handling & formats/macros
# ============================================

def _coerce_date(x) -> date | None:
    if x is None:
        return None
    if isinstance(x, date) and not isinstance(x, datetime):
        return x
    if isinstance(x, datetime):
        return x.date()
    s = str(x).strip()
    if not s:
        return None
    # accept yyyymmdd
    if s.isdigit() and len(s) == 8:
        return date(int(s[:4]), int(s[4:6]), int(s[6:]))
    # try ISO
    try:
        return datetime.fromisoformat(s).date()
    except Exception:
        return None

def is_leap_sas_style(y: int) -> bool:
    # SAS macro uses MOD(year,4)=0 in this job context
    return (y % 4) == 0

def LDAY_for(m: int, y: int) -> int:
    # Mirrors RETAIN D1-D12 31; D4 D6 D9 D11 30; February special
    if m == 2:
        return 29 if is_leap_sas_style(y) else 28
    return 30 if m in (4, 6, 9, 11) else 31

# PROC FORMAT: REMFMT
def REMFMT(x: float) -> str:
    if x <= 0.1:  return "01"
    if x <= 1:    return "02"
    if x <= 3:    return "03"
    if x <= 6:    return "04"
    if x <= 12:   return "05"
    if x <= 36:   return "06"
    if x <= 60:   return "07"
    return "08"

# PROC FORMAT: PRDFMT (defined in SAS but not used by this program’s logic)
HL_SET = {4,5,6,7,31,32,100,101,102,103,110,111,112,113,114,115,
          116,170,200,201,204,205,209,210,211,212,214,215,219,220,
          225,226,227,228,229,230,231,232,233,234}
RC_SET = {350,910,925}
def PRDFMT(product: int) -> str:
    if product in HL_SET: return "HL"
    if product in RC_SET: return "RC"
    return "FL"

def NXTBLDT(BLDATE: date, PAYFREQ: str, FREQ: int, ISSDTE: date) -> date:
    # SAS macro NXTBLDT
    if PAYFREQ == "6":  # biweekly branch (kept though PAYFREQ='3' later)
        dd = BLDATE.day + 14
        mm = BLDATE.month
        yy = BLDATE.year
        if dd > LDAY_for(mm, yy):
            dd -= LDAY_for(mm, yy)
            mm += 1
            if mm > 12:
                mm -= 12
                yy += 1
    else:
        dd = ISSDTE.day
        mm = BLDATE.month + FREQ
        yy = BLDATE.year
        if mm > 12:
            mm -= 12
            yy += 1
    if dd > LDAY_for(mm, yy):
        dd = LDAY_for(mm, yy)
    return date(yy, mm, dd)

def REMMTH_fn(MATDT: date, RPYR: int, RPMTH: int, RPDAY: int) -> float:
    # SAS macro REMMTH
    MDYR, MDMTH, MDDAY = MATDT.year, MATDT.month, MATDT.day
    rpdays_month_len = 29 if (RPMTH == 2 and is_leap_sas_style(RPYR)) else (30 if RPMTH in (4,6,9,11) else 31)
    if MDDAY > rpdays_month_len:
        MDDAY = rpdays_month_len
    REMY = MDYR  - RPYR
    REMM = MDMTH - RPMTH
    REMD = MDDAY - RPDAY
    return REMY*12 + REMM + (REMD / rpdays_month_len)

# ============================================
# 1) READ REPTDATE + derive macros
# ============================================
REPTDATE_DF = pl.read_parquet(REPTDATE_PARQUET)
if REPTDATE_DF.height != 1:
    raise RuntimeError("BNM1.REPTDATE must have exactly one row.")

REPTDATE_VAL = REPTDATE_DF["REPTDATE"][0]
repdt = _coerce_date(REPTDATE_VAL)
if repdt is None:
    raise RuntimeError("REPTDATE could not be parsed as a date.")

REPTDAY = f"{repdt.day:02d}"
REPTMON = f"{repdt.month:02d}"
REPTYEAR = f"{repdt.year:04d}"
if   repdt.day == 8:  NOWK = "1"
elif repdt.day == 15: NOWK = "2"
elif repdt.day == 22: NOWK = "3"
else:                 NOWK = "4"
RDATE = repdt.strftime("%d/%m/%y")  # DDMMYY8. equivalent

# ============================================
# 2) READ IBTRAD for the period (Parquet)
#    Adjust path pattern to your partition layout
# ============================================
IBTRAD_PATH = BASE_INPUT / f"BNM1_IBTRAD_{REPTMON}_{NOWK}.parquet"
IBTRAD = pl.read_parquet(IBTRAD_PATH)

# ============================================
# 3) DATA NOTE ... PROC SUMMARY NWAY
#    We build NOTE rows in Python (SAS emits multiple outputs per record)
# ============================================
rows: list[dict] = []

RPYR, RPMTH, RPDAY = repdt.year, repdt.month, repdt.day

for rec in IBTRAD.iter_rows(named=True):
    # SAS variable names preserved
    PRODCD   = str(rec.get("PRODCD", "") or "")
    PRODUCT  = int(rec.get("PRODUCT", 0) or 0)
    CUSTCD   = str(rec.get("CUSTCD", "") or "")
    BLDATE   = _coerce_date(rec.get("BLDATE", None))
    EXPRDATE = _coerce_date(rec.get("EXPRDATE", None))
    ISSDTE   = _coerce_date(rec.get("ISSDTE", None))
    PAYAMT   = float(rec.get("PAYAMT", 0) or 0.0)
    BALANCE  = float(rec.get("BALANCE", 0) or 0.0)

    # IF SUBSTR(PRODCD,1,2)='34' OR PRODUCT IN (225,226);
    if not (PRODCD[:2] == "34" or PRODUCT in (225, 226)):
        continue

    # CUST
    if CUSTCD in {"77", "78", "95", "96"}:
        CUST = "08"
    else:
        CUST = "09"

    # PROD set to 'BT' per SAS (do not derive from PRDFMT)
    PROD = "BT"

    # ITEM selection mirrors SAS branches
    if CUSTCD in {"77", "78", "95", "96"}:
        if PROD == "HL":
            ITEM = "214"
        else:
            ITEM = "219"
    else:
        if   PROD == "FL": ITEM = "211"
        elif PROD == "RC": ITEM = "212"
        else:              ITEM = "219"

    DAYS = None
    if BLDATE is not None:
        DAYS = (repdt - BLDATE).days

    # --- SAS-faithful initialization to avoid undefined REMMTH when EXPRDATE is missing ---
    REMMTH_val = 0.1  # safe default bucket, consistent with earliest range

    # If expiry within <8 days, set REMMTH=0.1, else compute via loop/macro
    if EXPRDATE is not None and (EXPRDATE - repdt).days < 8:
        REMMTH_val = 0.1
    else:
        PAYFREQ = "3"
        if   PAYFREQ == "1": FREQ = 1
        elif PAYFREQ == "2": FREQ = 3
        elif PAYFREQ == "3": FREQ = 6
        elif PAYFREQ == "4": FREQ = 12
        else:                FREQ = 6  # conservative default

        if PRODUCT in (350, 910, 925):
            BLDATE = EXPRDATE
        elif BLDATE is None or BLDATE <= date(1600, 1, 1):  # emulate BLDATE<=0
            BLDATE = ISSDTE
            while BLDATE is not None and BLDATE <= repdt:
                BLDATE = NXTBLDT(BLDATE, PAYFREQ, FREQ, ISSDTE)

        if PAYAMT < 0:
            PAYAMT = 0.0

        if BLDATE is not None and EXPRDATE is not None:
            if (BLDATE > EXPRDATE) or (BALANCE <= PAYAMT):
                BLDATE = EXPRDATE

        # DO WHILE (BLDATE <= EXPRDATE)
        while BLDATE is not None and EXPRDATE is not None and BLDATE <= EXPRDATE:
            MATDT = BLDATE
            REMMTH_val = REMMTH_fn(MATDT, RPYR, RPMTH, RPDAY)
            if (REMMTH_val > 60) or (BLDATE == EXPRDATE):
                break

            AMOUNT = PAYAMT
            BALANCE = BALANCE - PAYAMT
            BNMCODE = "95" + ITEM + CUST + REMFMT(REMMTH_val) + "0000Y"
            rows.append({"BNMCODE": BNMCODE, "AMOUNT": AMOUNT})

            if (DAYS is not None) and (DAYS > 89):
                REMMTH_tmp = 13
            else:
                REMMTH_tmp = REMMTH_val
            BNMCODE = "93" + ITEM + CUST + REMFMT(REMMTH_tmp) + "0000Y"
            rows.append({"BNMCODE": BNMCODE, "AMOUNT": AMOUNT})

            BLDATE = NXTBLDT(BLDATE, PAYFREQ, FREQ, ISSDTE)

            if (BLDATE > EXPRDATE) or (BALANCE <= PAYAMT):
                BLDATE = EXPRDATE

    # Final two OUTPUTs after the loop (exactly as SAS)
    AMOUNT = BALANCE
    BNMCODE = "95" + ITEM + CUST + REMFMT(REMMTH_val) + "0000Y"
    rows.append({"BNMCODE": BNMCODE, "AMOUNT": AMOUNT})

    if (DAYS is not None) and (DAYS > 89):
        REMMTH_final = 13
    else:
        REMMTH_final = REMMTH_val
    BNMCODE = "93" + ITEM + CUST + REMFMT(REMMTH_final) + "0000Y"
    rows.append({"BNMCODE": BNMCODE, "AMOUNT": AMOUNT})

# Build NOTE (detail) as Arrow, then summarize with DuckDB to mirror PROC SUMMARY NWAY
NOTE_arrow = pa.Table.from_pylist(rows, schema=pa.schema([
    pa.field("BNMCODE", pa.string()),
    pa.field("AMOUNT",  pa.float64()),
]))

con = duckdb.connect()
con.register("note", NOTE_arrow)
NOTE_SUM_arrow = con.execute("""
    SELECT BNMCODE, SUM(AMOUNT) AS AMOUNT
    FROM note
    GROUP BY BNMCODE
    ORDER BY BNMCODE
""").arrow()

# Save the summary as Parquet (optional)
NOTE_SUM_PARQUET = BASE_OUT / "NOTE_SUM.parquet"
pq.write_table(NOTE_SUM_arrow, NOTE_SUM_PARQUET)

# Also hold as Polars if you want to reuse downstream
NOTE_SUM = pl.from_arrow(NOTE_SUM_arrow)

# ============================================
# 4) Emit text file (header + BNMCODE;AMOUNT;)
#    Faithful to SAS: no forced decimals / separators
# ============================================
with NLFBT_TXT.open("w", encoding="utf-8", newline="") as f:
    # Header: INLFBT REPTDAY REPTMON REPTYEAR (spaces between, like SAS line)
    f.write(f"INLFBT {REPTDAY} {REPTMON} {REPTYEAR}\n")
    for r in NOTE_SUM.iter_rows(named=True):
        BNMCODE = r["BNMCODE"]
        AMOUNT  = r["AMOUNT"]
        # SAS-like numeric output: no thousands sep, no forced decimals.
        # If whole number, print as integer; otherwise print as plain float string.
        amt_str = str(int(AMOUNT)) if float(AMOUNT).is_integer() else str(AMOUNT)
        f.write(f"{BNMCODE};{amt_str};\n")

print(f"Wrote summary parquet: {NOTE_SUM_PARQUET}")
print(f"Wrote text file     : {NLFBT_TXT}")
