from __future__ import annotations

from pathlib import Path
from datetime import datetime, date, timedelta
import pyarrow as pa
import pyarrow.parquet as pq
import duckdb
import polars as pl
import sys

# ============================================
# PATHS (adjust to your environment)
# ============================================
BASE_INPUT = Path("parquet_input")     # root for input Parquet
BASE_OUT   = Path("parquet_output")    # for optional intermediate Parquet outputs
BASE_TXT   = Path("text_output")       # final text output folder
BASE_OUT.mkdir(parents=True, exist_ok=True)
BASE_TXT.mkdir(parents=True, exist_ok=True)

# Input: BNM1.IBTRAD&REPTMON&NOWK -> naming pattern; see below
# Output "host dataset" mirror
NLFBT_TXT = BASE_TXT / "SAP.PIBB.ALM.NLFBT.TEXT.txt"

# ============================================
# HELPERS: date handling & formats/macros
# ============================================

def is_leap_sas_style(y: int) -> bool:
    """SAS macro uses MOD(year,4)=0 in this job context"""
    return (y % 4) == 0

def LDAY_for(m: int, y: int) -> int:
    """Mirrors RETAIN D1-D12 31; D4 D6 D9 D11 30; February special"""
    if m == 2:
        return 29 if is_leap_sas_style(y) else 28
    return 30 if m in (4, 6, 9, 11) else 31

# PROC FORMAT: REMFMT (from EIIMABTL - same as EIBMABTL)
def REMFMT(x: float) -> str:
    if x is None:
        return "07"
    if x <= 0.1:  return "01"
    if x <= 1:    return "02"
    if x <= 3:    return "03"
    if x <= 6:    return "04"
    if x <= 12:   return "05"
    if x <= 36:   return "06"
    if x <= 60:   return "07"
    return "08"

def NXTBLDT(BLDATE: date, PAYFREQ: str, FREQ: int, ISSDTE: date) -> date:
    """SAS macro NXTBLDT"""
    if PAYFREQ == "6":
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
    """SAS macro REMMTH"""
    MDYR, MDMTH, MDDAY = MATDT.year, MATDT.month, MATDT.day
    rpdays_month_len = 29 if (RPMTH == 2 and is_leap_sas_style(RPYR)) else (30 if RPMTH in (4,6,9,11) else 31)
    if MDDAY > rpdays_month_len:
        MDDAY = rpdays_month_len
    REMY = MDYR - RPYR
    REMM = MDMTH - RPMTH
    REMD = MDDAY - RPDAY
    return REMY * 12 + REMM + (REMD / rpdays_month_len)

def to_date(value):
    """Convert SAS numeric date to Python date."""
    if value is None:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, (int, float)):
        if value <= 0:
            return None
        return date(1960, 1, 1) + timedelta(days=int(value))
    return None


# ============================================
# 1) SET REPORT DATE (using datetime directly)
# ============================================

# ====================================================================
# TESTING MODE - Use today's date minus 1 day
# ====================================================================
repdt = date.today() - timedelta(days=1)

# ====================================================================
# PRODUCTION MODE - Uncomment below for production use with specific date
# ====================================================================
# repdt = date(2026, 6, 8)  # Example: June 8, 2026

# ====================================================================
# End of date selection
# ====================================================================

REPTDAY = f"{repdt.day:02d}"
REPTMON = f"{repdt.month:02d}"
REPTYEAR = f"{repdt.year:04d}"
if repdt.day == 8:
    NOWK = "1"
elif repdt.day == 15:
    NOWK = "2"
elif repdt.day == 22:
    NOWK = "3"
else:
    NOWK = "4"
RDATE = repdt.strftime("%d/%m/%y")

print(f"\n{'='*70}")
print(f"EIIMABTL - ISLAMIC LOAN MATURITY PROFILE PROCESSOR")
print(f"{'='*70}")
print(f"\nReport Date: {repdt.strftime('%d/%m/%Y')}")
print(f"Week Number: {NOWK}")
print(f"Report Month: {REPTMON}")
print(f"Report Year: {REPTYEAR}")

# ============================================
# 2) READ IBTRAD for the period
# ============================================
IBTRAD_PATH = BASE_INPUT / f"BNM1_IBTRAD_{REPTMON}_{NOWK}.parquet"
print(f"\nLooking for IBTRAD file: {IBTRAD_PATH.name}")

if not IBTRAD_PATH.exists():
    raise FileNotFoundError(f"IBTRAD file not found: {IBTRAD_PATH}")

# Read SAS file if it's .sas7bdat, otherwise read parquet
if IBTRAD_PATH.suffix == '.sas7bdat':
    import pyreadstat
    df, meta = pyreadstat.read_sas7bdat(str(IBTRAD_PATH))
    IBTRAD = pl.from_pandas(df)
else:
    IBTRAD = pl.read_parquet(IBTRAD_PATH)

print(f"Total records read: {len(IBTRAD)}")

# ============================================
# 3) DATA NOTE ... PROC SUMMARY NWAY
#    Matches SAS DATA NOTE step exactly
# ============================================
rows: list[dict] = []

RPYR, RPMTH, RPDAY = repdt.year, repdt.month, repdt.day
processed = 0

for rec in IBTRAD.iter_rows(named=True):
    # SAS variable names preserved
    PRODCD = str(rec.get("PRODCD", "") or "")
    PRODUCT = int(rec.get("PRODUCT", 0) or 0)
    CUSTCD = str(rec.get("CUSTCD", "") or "")
    
    # Convert dates from SAS numeric if needed
    BLDATE = to_date(rec.get("BLDATE", None))
    EXPRDATE = to_date(rec.get("EXPRDATE", None))
    ISSDTE = to_date(rec.get("ISSDTE", None))
    
    PAYAMT = float(rec.get("PAYAMT", 0) or 0.0)
    BALANCE = float(rec.get("BALANCE", 0) or 0.0)

    # IF SUBSTR(PRODCD,1,2)='34' OR PRODUCT IN (225,226)
    if not (PRODCD[:2] == "34" or PRODUCT in (225, 226)):
        continue

    # Determine CUST (matches SAS)
    if CUSTCD in {"77", "78", "95", "96"}:
        CUST = "08"
    else:
        CUST = "09"

    # PROD set to 'BT' (matches SAS)
    PROD = "BT"

    # ITEM selection (matches SAS SELECT logic)
    if CUSTCD in {"77", "78", "95", "96"}:
        if PROD == "HL":
            ITEM = "214"
        else:
            ITEM = "219"
    else:
        if PROD == "FL":
            ITEM = "211"
        elif PROD == "RC":
            ITEM = "212"
        else:
            ITEM = "219"

    # Calculate DAYS past due (only if BLDATE > 0)
    DAYS = None
    if BLDATE is not None and BLDATE > date(1900, 1, 1):
        DAYS = (repdt - BLDATE).days

    # Initialize REMMTH (matches SAS logic)
    REMMTH_val = None

    # Process maturity profile (matches SAS IF-ELSE logic)
    if EXPRDATE is not None and (EXPRDATE - repdt).days < 8:
        REMMTH_val = 0.1
    elif EXPRDATE is not None:
        PAYFREQ = "3"
        if PAYFREQ == "1":
            FREQ = 1
        elif PAYFREQ == "2":
            FREQ = 3
        elif PAYFREQ == "3":
            FREQ = 6
        elif PAYFREQ == "4":
            FREQ = 12
        else:
            FREQ = 6

        # RC products use expiry date as billing date
        if PRODUCT in (350, 910, 925):
            BLDATE = EXPRDATE
        elif BLDATE is None or BLDATE <= date(1900, 1, 1):
            BLDATE = ISSDTE
            if BLDATE is not None:
                while BLDATE <= repdt:
                    BLDATE = NXTBLDT(BLDATE, PAYFREQ, FREQ, ISSDTE)

        if PAYAMT < 0:
            PAYAMT = 0.0

        if BLDATE is not None:
            if BLDATE > EXPRDATE or BALANCE <= PAYAMT:
                BLDATE = EXPRDATE

        current_balance = BALANCE
        current_bldate = BLDATE

        # DO WHILE (BLDATE <= EXPRDATE)
        while current_bldate is not None and current_bldate <= EXPRDATE:
            MATDT = current_bldate
            REMMTH_val = REMMTH_fn(MATDT, RPYR, RPMTH, RPDAY)
            
            # SAS uses > 60 for EIIMABTL (different from EIBMABTL which uses > 12)
            if REMMTH_val > 60 or current_bldate == EXPRDATE:
                break

            if PAYAMT > 0:
                AMOUNT = PAYAMT
                current_balance -= PAYAMT
                
                # Part 2-RM (95)
                BNMCODE = "95" + ITEM + CUST + REMFMT(REMMTH_val) + "0000Y"
                rows.append({"BNMCODE": BNMCODE, "AMOUNT": AMOUNT})
                
                # Part 1-RM (93) - NPL if days > 89
                if DAYS is not None and DAYS > 89:
                    REMMTH_tmp = 13
                else:
                    REMMTH_tmp = REMMTH_val
                BNMCODE = "93" + ITEM + CUST + REMFMT(REMMTH_tmp) + "0000Y"
                rows.append({"BNMCODE": BNMCODE, "AMOUNT": AMOUNT})

            # Calculate next billing date
            current_bldate = NXTBLDT(current_bldate, PAYFREQ, FREQ, ISSDTE)
            
            if current_bldate > EXPRDATE or current_balance <= PAYAMT:
                current_bldate = EXPRDATE
        
        # Update BALANCE and BLDATE after loop
        BALANCE = current_balance
        BLDATE = current_bldate

    # Final two OUTPUTs after the loop (matches SAS final OUTPUT statements)
    AMOUNT = BALANCE
    BNMCODE = "95" + ITEM + CUST + REMFMT(REMMTH_val) + "0000Y"
    rows.append({"BNMCODE": BNMCODE, "AMOUNT": AMOUNT})

    if DAYS is not None and DAYS > 89:
        REMMTH_final = 13
    else:
        REMMTH_final = REMMTH_val
    BNMCODE = "93" + ITEM + CUST + REMFMT(REMMTH_final) + "0000Y"
    rows.append({"BNMCODE": BNMCODE, "AMOUNT": AMOUNT})

    processed += 1
    if processed % 1000 == 0:
        print(f"  Processed {processed} records...")

print(f"\n  Total records processed: {processed}")
print(f"  Output records created: {len(rows)}")

if len(rows) == 0:
    print("  No output records generated.")
    sys.exit(0)

# ============================================
# 4) PROC SUMMARY NWAY (aggregation)
# ============================================
NOTE_arrow = pa.Table.from_pylist(rows, schema=pa.schema([
    pa.field("BNMCODE", pa.string()),
    pa.field("AMOUNT", pa.float64()),
]))

con = duckdb.connect()
con.register("note", NOTE_arrow)
NOTE_SUM_arrow = con.execute("""
    SELECT BNMCODE, SUM(AMOUNT) AS AMOUNT
    FROM note
    GROUP BY BNMCODE
    ORDER BY BNMCODE
""").arrow()

# Filter out missing remmth (code '07') - matches SAS where '07' is filtered out
NOTE_SUM_arrow = con.execute("""
    SELECT BNMCODE, AMOUNT
    FROM NOTE_SUM_arrow
    WHERE SUBSTR(BNMCODE, 8, 2) != '07'
""").arrow() if len(NOTE_SUM_arrow) > 0 else NOTE_SUM_arrow

# Save the summary as Parquet (optional)
NOTE_SUM_PARQUET = BASE_OUT / "NOTE_SUM_EIIMABTL.parquet"
pq.write_table(NOTE_SUM_arrow, NOTE_SUM_PARQUET)

# Convert to Polars for easier handling
NOTE_SUM = pl.from_arrow(NOTE_SUM_arrow)

# ============================================
# 5) DATA _NULL_ step - write output file
#    Matches SAS PUT statement exactly
# ============================================
print(f"\nWriting output to: {NLFBT_TXT}")
with NLFBT_TXT.open("w", encoding="utf-8", newline="") as f:
    # Header: INLFBT REPTDAY REPTMON REPTYEAR (no spaces in SAS? Check original)
    # Original SAS: PUT @1 'INLFBT' "&REPTDAY" "&REPTMON" "&REPTYEAR";
    # This puts them concatenated: INLFBTDDMMYYYY
    f.write(f"INLFBT{REPTDAY}{REPTMON}{REPTYEAR}\n")
    
    # Data rows: BNMCODE;AMOUNT;
    for r in NOTE_SUM.iter_rows(named=True):
        BNMCODE = r["BNMCODE"]
        AMOUNT = r["AMOUNT"]
        # Format amount without thousand separators, matching SAS output
        if float(AMOUNT).is_integer():
            amt_str = str(int(AMOUNT))
        else:
            amt_str = f"{AMOUNT:.2f}".rstrip('0').rstrip('.') if AMOUNT != 0 else "0"
        f.write(f"{BNMCODE};{amt_str};\n")

# Calculate totals for summary
total_amount = NOTE_SUM['AMOUNT'].sum() if len(NOTE_SUM) > 0 else 0
missing_count = len(rows) - len(NOTE_SUM)

print("\n" + "=" * 70)
print("PROCESSING COMPLETED SUCCESSFULLY")
print("=" * 70)
print(f"\nOutput file: {NLFBT_TXT}")
print(f"Summary Parquet: {NOTE_SUM_PARQUET}")
print(f"Total BNM codes: {len(NOTE_SUM)}")
print(f"Total amount: {total_amount:,.2f}")
if missing_count > 0:
    print(f"Records with missing remmth (code '07'): {missing_count}")

# Close DuckDB connection
con.close()
