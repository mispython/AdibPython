#!/usr/bin/env python3
"""
EIIFTXT4 - ESMR 2013-705 (SEND FILE TO LNS) - 2ND EXTRACTION CAP
Merge WOFFTXT with ICAP data, write fixed-width WOFFTEXT + PROC PRINT listing.

Text-only output. REPTDATE from system clock - 1 day.
"""

import os
import sys
import time
from datetime import datetime, timedelta, date
from pathlib import Path

import polars as pl
import pyreadstat

# ============================================================================
# CONFIG
# ============================================================================
EXPECTED_LEN = 797           # @789 AKPK_STATUS $9. -> positions 789..797

# --- Input paths (each from its own source, no shared ROOT) -----------------
PATH_WOFFTXT  = Path('/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIFTXT4/WOFFTXT.sas7bdat')

# --- Output paths ----------------------------------------------------------
OUTPUT_DIR    = Path('/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIFTXT4')
WOFFTEXT_PATH = OUTPUT_DIR / 'EIIFTXT4_wofftxt.txt'
REPORT_PATH   = OUTPUT_DIR / 'EIIFTXT4_report.txt'

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ============================================================================
# Report date & week logic (system clock - 1 day)
# ============================================================================
reptdate = datetime.now().date() - timedelta(days=1)

day = reptdate.day
if   day == 8:  wk, wk1 = '1', '4'
elif day == 15: wk, wk1 = '2', '1'
elif day == 22: wk, wk1 = '3', '2'
else:           wk, wk1 = '4', '3'

mm, mm1 = reptdate.month, (reptdate.month - 1 if reptdate.month > 1 else 12)
nowk, nowks, nowk1 = wk, '4', wk1
reptmon, reptmon1  = f'{mm:02d}', f'{mm1:02d}'
reptyear = f'{reptdate.year % 100:02d}'
rdate    = reptdate.strftime('%d/%m/%y')

# Dynamic ICAP path (date-dependent)
PATH_ICAP = Path(
    f'/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIFTXT4/'
    f'ICAP{reptmon}{reptyear}.sas7bdat'
)

print(f"\n{'='*60}\nESMR 2013-705 (SEND FILE TO LNS) - 2ND EXTRACTION CAP\n{'='*60}")
print(f"Report Date: {reptdate.strftime('%d/%m/%Y')}  "
      f"Week: {nowk}  Prev Month: {reptmon1}")
print(f"Output Dir : {OUTPUT_DIR}")
print(f"{'='*60}\n")

start_total = time.time()

# ============================================================================
# Helpers
# ============================================================================
def read_full(path, columns=None):
    path = Path(path)
    if not path.exists():
        print(f"  WARNING: missing {path}")
        return None
    t0 = time.time()
    print(f"  Reading: {path.name}", end="", flush=True)
    try:
        df, _ = pyreadstat.read_sas7bdat(str(path), usecols=columns or None)
        print(f" - {len(df):,} rows, {time.time()-t0:.1f}s")
        return pl.from_pandas(df) if len(df) else None
    except Exception as e:
        print(f"\n  ERROR: {e}")
        return None

def standardize_schema(df, schema):
    if df is None or df.height == 0:
        return df
    exprs = [pl.col(c).cast(dt) for c, dt in schema.items() if c in df.columns]
    return df.with_columns(exprs) if exprs else df

# ============================================================================
# STEP 1: Load WOFFTXT (sorted by ACCTNO NOTENO)
# ============================================================================
print("STEP 1: WOFFTXT...")
df_wofftxt = read_full(PATH_WOFFTXT)
if df_wofftxt is None or df_wofftxt.height == 0:
    print("WARNING: WOFFTXT empty or missing. Exiting.")
    sys.exit(0)

if {'ACCTNO','NOTENO'} <= set(df_wofftxt.columns):
    df_wofftxt = df_wofftxt.sort(['ACCTNO','NOTENO'])
df_wofftxt = standardize_schema(df_wofftxt, {
    'ACCTNO':pl.Float64,'NOTENO':pl.Float64,
    'IIS':pl.Float64,'OI':pl.Float64,'TOTIIS':pl.Float64,'SP':pl.Float64,
    'TOTAL':pl.Float64,'CURBAL':pl.Float64,'PREVBAL':pl.Float64,
    'PAYMENT':pl.Float64,'ECSRRSRV':pl.Float64,'POSTAMT':pl.Float64,
    'OTHERAMT':pl.Float64,'INTAMT':pl.Float64,'MARKETVL':pl.Float64,
    'INTEARN4':pl.Float64,'DAYS':pl.Float64,'CUSTCODE':pl.Float64,
    'OIFEEAMT':pl.Float64,'LSTTRNCD':pl.Float64,'MTHPDUE':pl.Float64,
    'BALANCE':pl.Float64,'NETPROC':pl.Float64,'COLLYEAR':pl.Float64,
    'BILPAID':pl.Float64,'MARGINFI':pl.Float64,'NOTETERM':pl.Float64,
    'PAYAMT':pl.Float64,'LOANTYPE':pl.Float64,'CAP':pl.Float64,
})
print(f"  WOFFTXT: {df_wofftxt.height:,} rows\n")

# ============================================================================
# STEP 2: Load ICAP (NODUPKEY by ACCTNO NOTENO)
# ============================================================================
print("STEP 2: ICAP...")
df_icap = read_full(PATH_ICAP, columns=['ACCTNO','NOTENO','CAP'])
if df_icap is not None and df_icap.height:
    df_icap = standardize_schema(df_icap, {
        'ACCTNO':pl.Float64,'NOTENO':pl.Float64,'CAP':pl.Float64,
    }).unique(subset=['ACCTNO','NOTENO'], keep='first').sort(['ACCTNO','NOTENO'])
    print(f"  ICAP: {df_icap.height:,} rows")
else:
    print("  ICAP: empty (CAP will default to 0 where not overridden)")
    df_icap = pl.DataFrame(schema={
        'ACCTNO':pl.Float64, 'NOTENO':pl.Float64, 'CAP':pl.Float64})
print()

# ============================================================================
# STEP 3: MERGE WOFFTXT(IN=A DROP=CAP) ICAP(IN=B KEEP=ACCTNO NOTENO CAP)
#         IF BORSTAT='A' THEN CAP = TOTAL
# ============================================================================
print("STEP 3: MERGE WOFFTXT (drop CAP) + ICAP...")
if 'CAP' in df_wofftxt.columns:
    df_wofftxt = df_wofftxt.drop('CAP')

df_text = df_wofftxt.join(df_icap, on=['ACCTNO','NOTENO'], how='left')

df_text = df_text.with_columns(
    pl.when(pl.col('BORSTAT') == 'A')
      .then(pl.col('TOTAL'))
      .otherwise(pl.col('CAP'))
      .alias('CAP')
)
print(f"  TEXT: {df_text.height:,} rows\n")

# ============================================================================
# STEP 4: Fixed-width writer (SAS @n positions, LRECL=797)
# ============================================================================
def _s(v, n):
    s = '' if v is None else str(v)
    s = s.replace('\n', ' ').replace('\r', ' ')
    return s[:n].ljust(n)

def _n(v, n, d=2):
    if v is None:
        return ' ' * n
    try:
        s = f"{float(v):.{d}f}"
        return s.rjust(n) if len(s) <= n else s[:n]
    except Exception:
        return ' ' * n

def _i(v, n):
    if v is None:
        return ' ' * n
    try:
        s = str(int(float(v)))
        return s.rjust(n) if len(s) <= n else s[:n]
    except Exception:
        return ' ' * n

def _fd(v):
    """MMDDYY10. -> 'MM/DD/YYYY'. Missing -> 10 spaces."""
    if v is None:
        return ' ' * 10
    if isinstance(v, date):
        return v.strftime('%m/%d/%Y')
    if isinstance(v, str):
        return v[:10].ljust(10)
    try:
        d = date.fromordinal(int(v))
        return d.strftime('%m/%d/%Y')
    except Exception:
        return ' ' * 10

def build_woff_record(r):
    buf = [' '] * EXPECTED_LEN

    def put(pos, s, n):
        s = ('' if s is None else str(s)).replace('\n', ' ').replace('\r', ' ')[:n].ljust(n)
        start = pos - 1
        buf[start:start+n] = list(s)

    def put_num(pos, v, n, d=2):
        put(pos, _n(v, n, d), n)

    def put_int(pos, v, n):
        put(pos, _i(v, n), n)

    put(1,   r.get('BRANCH'), 7)
    put(9,   r.get('NAME'), 40)
    put_int(50, r.get('ACCTNO'), 10)
    put_int(61, r.get('NOTENO'), 5)
    put(67,  r.get('BORSTAT'), 1)
    put_num(69,  r.get('IIS'), 16)
    put_num(85,  r.get('OI'), 16)
    put_num(101, r.get('TOTIIS'), 16)
    put_num(117, r.get('SP'), 16)
    put_num(133, r.get('TOTAL'), 16)
    put_num(149, r.get('CURBAL'), 16)
    put_num(165, r.get('PREVBAL'), 16)
    put_num(181, r.get('PAYMENT'), 16)
    put_num(197, r.get('ECSRRSRV'), 16)
    put_num(213, r.get('POSTAMT'), 16)
    put_num(229, r.get('OTHERAMT'), 16)
    put(245, r.get('MATDATE'), 10)
    put_int(255, r.get('LOANTYPE'), 3)
    put_num(258, r.get('INTAMT'), 16)
    put(274, r.get('POSTNTRN'), 1)
    put_num(278, r.get('MARKETVL'), 16)
    put_num(294, r.get('INTEARN4'), 16)
    put_int(310, r.get('DAYS'), 6)
    put_int(317, r.get('CUSTCODE'), 3)
    put(321, r.get('RIND'), 1)
    put_num(322, r.get('OIFEEAMT'), 16)
    put(339, r.get('LASTTRA1'), 10)
    put_int(350, r.get('LSTTRNCD'), 3)
    put_int(354, r.get('MTHPDUE'), 3)
    put_num(357, r.get('BALANCE'), 16)
    put_num(374, r.get('CAP'), 16)
    put_num(391, r.get('LATECHG'), 16)
    put(408, r.get('GUAREND'), 20)
    put(429, r.get('GUARNAM1'), 40)
    put(470, r.get('GUARNAM2'), 40)
    put(511, _fd(r.get('ISSXDTE')), 10)
    put_num(522, r.get('NETPROC'), 16)
    put(539, r.get('COLLDESC'), 70)
    put_int(610, r.get('COLLYEAR'), 4)
    put_int(615, r.get('BILPAID'), 3)
    put(619, r.get('CRRGRADE'), 5)
    put_num(625, r.get('MARGINFI'), 16)
    put_int(642, r.get('NOTETERM'), 3)
    put_num(646, r.get('PAYAMT'), 16)
    put(663, _fd(r.get('DOBMNI')), 10)
    put(674, r.get('ECSRIND'), 1)
    put(677, r.get('DELQCD'), 2)
    put(680, r.get('DELQDES'), 30)
    put(711, r.get('BIZTYPE'), 1)
    put(713, r.get('OCCUPAT'), 3)
    put(717, r.get('OCCUPDES'), 25)
    put(743, r.get('BGC'), 2)
    put(746, r.get('BGCDES'), 20)
    put(767, r.get('PAY75PCT'), 1)
    put(769, r.get('NACODATE'), 10)
    put(780, r.get('CP'), 1)
    put(782, r.get('MODELDES'), 6)
    put(789, r.get('AKPK_STATUS'), 9)

    return ''.join(buf)

# ============================================================================
# STEP 5: Write WOFFTEXT (delete stale first)
# ============================================================================
print("STEP 5: Writing WOFFTEXT...")
if WOFFTEXT_PATH.exists():
    WOFFTEXT_PATH.unlink()

with open(WOFFTEXT_PATH, 'w', encoding='utf-8') as fout:
    for r in df_text.iter_rows(named=True):
        fout.write(build_woff_record(r) + '\n')
print(f"  {WOFFTEXT_PATH.name}: {WOFFTEXT_PATH.stat().st_size:,} bytes")

with open(WOFFTEXT_PATH) as f:
    lengths = [len(line.rstrip('\n')) for line in f]
if lengths:
    print(f"  line lengths: min={min(lengths)} max={max(lengths)} expected={EXPECTED_LEN}")
    if min(lengths) != EXPECTED_LEN:
        print(f"  WARNING: line length mismatch")

# ============================================================================
# STEP 6: PROC PRINT listing
# ============================================================================
print("STEP 6: PROC PRINT listing...")

PRINT_VARS = ["BRANCH","NAME","ACCTNO","NOTENO","BORSTAT",
              "IIS","OI","TOTIIS","SP","TOTAL","CURBAL",
              "PREVBAL","PAYMENT","MATDATE","LOANTYPE","INTAMT","POSTNTRN",
              "MARKETVL","INTEARN4","DAYS","LASTTRA1","LSTTRNCD","MTHPDUE","BALANCE"]
SUM_VARS = {"IIS","OI","TOTIIS","SP","TOTAL","CURBAL","PREVBAL","PAYMENT","BALANCE"}
NUMERIC_VARS = {"ACCTNO","NOTENO","IIS","OI","TOTIIS","SP","TOTAL","CURBAL",
                "PREVBAL","PAYMENT","LOANTYPE","INTAMT","MARKETVL","INTEARN4",
                "DAYS","LSTTRNCD","MTHPDUE","BALANCE"}

COL_WIDTHS = {
    "BRANCH":10,"NAME":30,"ACCTNO":12,"NOTENO":8,"BORSTAT":8,
    "IIS":15,"OI":15,"TOTIIS":15,"SP":15,"TOTAL":15,
    "CURBAL":15,"PREVBAL":15,"PAYMENT":15,"MATDATE":12,
    "LOANTYPE":9,"INTAMT":15,"POSTNTRN":9,"MARKETVL":15,
    "INTEARN4":15,"DAYS":7,"LASTTRA1":12,"LSTTRNCD":9,
    "MTHPDUE":8,"BALANCE":15,
}

def fmt_report_val(val, col):
    w = COL_WIDTHS.get(col, 10)
    if val is None:
        return ' ' * w
    if col in NUMERIC_VARS:
        try:
            if col in SUM_VARS or col in ("MARKETVL","INTEARN4","INTAMT"):
                return f"{float(val):>{w},.2f}"
            return f"{int(float(val)):>{w},d}"
        except Exception:
            return str(val)[:w].rjust(w)
    return str(val)[:w].ljust(w)

PAGE_LEN = 60
output_lines = []
line_count = [0]

def emit(cc, content=''):
    output_lines.append(cc + content)
    line_count[0] += 1

def emit_page_header():
    line_count[0] = 0
    emit('1', 'LISTING OF ACCOUNTS FOR BAD DEBT WRITING-OFF EXERCISE')
    emit(' ', f'AS AT {rdate}')
    emit(' ', '')
    hdr = ' '.join(c.center(COL_WIDTHS.get(c, 10)) for c in PRINT_VARS)
    emit(' ', hdr)
    emit(' ', '-' * len(hdr))
    line_count[0] = 5

emit_page_header()

sums = {v: 0.0 for v in SUM_VARS}

for r in df_text.iter_rows(named=True):
    if line_count[0] >= PAGE_LEN - 2:
        emit_page_header()
    parts = []
    for col in PRINT_VARS:
        val = r.get(col)
        parts.append(fmt_report_val(val, col))
    emit(' ', ' '.join(parts))
    for sv in SUM_VARS:
        v = r.get(sv)
        if v is not None:
            try: sums[sv] += float(v)
            except Exception: pass

sep = '-' * sum(COL_WIDTHS.get(c, 10) + 1 for c in PRINT_VARS)
emit(' ', sep)
sum_parts = []
for col in PRINT_VARS:
    w = COL_WIDTHS.get(col, 10)
    if col in SUM_VARS:
        sum_parts.append(f"{sums[col]:>{w},.2f}")
    else:
        sum_parts.append(' ' * w)
emit(' ', ' '.join(sum_parts))

with open(REPORT_PATH, 'w', encoding='utf-8') as frpt:
    for line in output_lines:
        frpt.write(line + '\n')
print(f"  {REPORT_PATH.name}: {REPORT_PATH.stat().st_size:,} bytes")

total_elapsed = time.time() - start_total
print(f"\n{'='*60}\nCOMPLETED IN {total_elapsed:.1f}s\n{'='*60}")
print(f"  {WOFFTEXT_PATH}")
print(f"  {REPORT_PATH}")
print(f"  Records: {df_text.height:,}")
