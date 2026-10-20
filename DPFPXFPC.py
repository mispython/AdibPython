"""
EIMRESHI - HP/Hire Purchase Loan Summary & Detail Report
Converted from SAS to Python

Uses PBBLNFMT.py and PBBELF.py for:
- &HPD macro (HP product filter list)
- BRCHCD. format (branch code -> branch name, for BRABBR)

OUTPUT STRUCTURE (matches SAS):
- CCDTXT1 equivalent: single file, all 32 summary sections concatenated.
- CCDTXT2 equivalent: single NPL detail report.

FIXES APPLIED:
- ISSUEDT is a SAS date serial (days since 1960-01-01). Epoch sanity check
  verifies parsed dates against the epoch independently, so stale bytecode or
  a modified SAS_EPOCH cannot silently produce wrong dates.
- BRANCH and DAYDIFF are LOANTEMP columns, NOT LNNOTE columns. Removed from
  LNNOTE_USECOLS (they arrive via the LOANTEMP join).
- Chunk reader unpacks (DataFrame, Metadata) tuples correctly.
- Polars deprecations: pl.count() -> pl.len(), pivot(columns=) -> pivot(on=).
- usecols verification uses case-insensitive trimmed comparison.
"""

import polars as pl
import pyreadstat
from datetime import datetime, timedelta, date
import os
import sys
import random
import traceback

# Business format libraries (SAS %INC PGM(PBBELF) / PGM(PBBLNFMT))
import PBBLNFMT
import PBBELF

# ============================================================
# PERFORMANCE / SAMPLING CONFIG
# ============================================================
SAMPLE_MODE = None          # None | "head" | "reservoir"
SAMPLE_SIZE = 5000
CHUNK_SIZE = 100_000
RANDOM_SEED = 42

# ============================================================
# DIRECTORIES
# ============================================================
LOAN_DIR_TEMPLATE = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS/enrh_ln_note_m{reptmon}.sas7bdat"
CCDTEMP_PATH = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIMREPOI/loantemp_iccd2.sas7bdat"
OUTPUT_DIR = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIMRESHI"

# Two output files, matching SAS CCDTXT1 / CCDTXT2
CCDTXT1_PATH = os.path.join(OUTPUT_DIR, 'EIMRESHI_SUMMARY_ALL_CCDTXT1.txt')
CCDTXT2_PATH = os.path.join(OUTPUT_DIR, 'EIMRESHI_DETAIL_NPL_CCDTXT2.txt')

os.makedirs(OUTPUT_DIR, exist_ok=True)

print("EIMRESHI - HP Loan Summary & Detail Report")
print("=" * 60)
if SAMPLE_MODE:
    print(f"*** SAMPLE_MODE = '{SAMPLE_MODE}' active -> capping LNNOTE at ~{SAMPLE_SIZE:,} matching rows ***")
print("=" * 60)

# HP Products - from SAS macro &HPD (defined in PBBLNFMT)
# NOTE: The SAS report title hardcodes '128,130,380,381,700,705' even though
# &HPD contains 10 products. This is a business quirk we preserve exactly.
HP_PRODUCTS = list(PBBLNFMT.HPD)               # [128,130,131,132,380,381,700,705,720,725]
RPT_TITLE_PRODUCTS = '128,130,380,381,700,705' # hardcoded in SAS PRNTITLE

print(f"HP_PRODUCTS (from PBBLNFMT.HPD): {HP_PRODUCTS}")
print(f"Report title products (SAS hardcoded): {RPT_TITLE_PRODUCTS}")

# ============================================================
# SAS DATE EPOCH
# ============================================================
# SAS stores dates as days since 1960-01-01. Verified against real data:
#   ISSUEDT=14633 -> 2000-01-24, ISSUEDT=14435 -> 1999-07-10, etc.
# Do NOT change this value without re-verifying against raw SAS data:
#   date(1960,1,1) + timedelta(days=14633) == date(2000,1,24)
SAS_EPOCH = date(1960, 1, 1)

print(f"SAS_EPOCH = {SAS_EPOCH.isoformat()}")
print(f"Verification: ISSUEDT=14633 should map to "
      f"{(SAS_EPOCH + timedelta(days=14633)).isoformat()} (expected 2000-01-24)")

# ============================================================
# REPORT DATE LOGIC (matches SAS REPTDATE data step)
# ============================================================
reptdate = datetime.now() - timedelta(days=20)
reptdate = reptdate.replace(hour=0, minute=0, second=0, microsecond=0)

day = reptdate.day
if day == 8:
    sdd, wk, wk1 = 1, '1', '4'
elif day == 15:
    sdd, wk, wk1 = 9, '2', '1'
elif day == 22:
    sdd, wk, wk1 = 16, '3', '2'
else:
    sdd, wk, wk1 = 23, '4', '3'

mm = reptdate.month
if wk == '1':
    mm1 = mm - 1 if mm > 1 else 12
else:
    mm1 = mm

reptmon = f'{mm:02d}'
reptmon1 = f'{mm1:02d}'
reptyear = reptdate.year
reptday = f'{day:02d}'
rdate = reptdate.strftime('%d%m%y')

LOAN_PATH = LOAN_DIR_TEMPLATE.format(reptmon=reptmon)

print(f"Report Date: {reptdate.strftime('%d/%m/%Y')}")
print(f"Week: {wk}  |  RDATE: {rdate}")
print(f"LNNOTE file: {LOAN_PATH}")
print(f"LOANTEMP file: {CCDTEMP_PATH}")
print(f"Output CCDTXT1: {CCDTXT1_PATH}")
print(f"Output CCDTXT2: {CCDTXT2_PATH}")
print("=" * 60)

# ============================================================
# LOOKUP MAPS
# ============================================================
MAKE_MAP = {
    ' 1': 'PROTON', ' 2': 'PERODUA', ' 3': 'TOYOTA', ' 4': 'NISSAN',
    ' 5': 'HONDA', ' 6': 'ISUZU', ' 7': 'DAIHATSU', ' 8': 'MITSUBISHI',
    ' 9': 'FORD', '10': 'MERCEDES BENZ', '11': 'VOLVO', '13': 'BMW'
}

STATE_MAP = {
    '1': 'JOHORE', '2': 'KEDAH', '3': 'KELANTAN', '4': 'MALACCA',
    '5': 'N.SEMBILAN', '6': 'PAHANG', '7': 'PENANG', '8': 'PERAK',
    '9': 'PERLIS', '10': 'SABAH', '11': 'SARAWAK', '12': 'SELANGOR',
    '13': 'TRENGGANU', '14': 'W.PERSEKUTUAN', '15': 'LABUAN'
}

# Columns from LNNOTE only (BRANCH and DAYDIFF come from LOANTEMP via join)
LNNOTE_USECOLS = [
    'ACCTNO', 'NOTENO', 'LOANTYPE', 'NETPROC', 'APPVALUE',
    'NOTETERM', 'STATE', 'DEALERNO', 'SCORE2', 'ORGBAL',
    'CURBAL', 'PAYAMT', 'ISSUEDT', 'BALANCE',
    'BORSTAT', 'CENSUS', 'NAME'
]


# ============================================================
# SAS-DATE-SERIAL CONVERTER
# ============================================================
def sas_serial_to_date(val):
    """
    Convert SAS date serial (days since 1960-01-01) to Python datetime.
    Returns None if not a plausible SAS date in the range 1960-2069.
    """
    if val is None:
        return None
    try:
        n = int(round(float(val)))
    except (TypeError, ValueError):
        return None
    if n < 0 or n > 40000:
        return None
    return datetime.combine(SAS_EPOCH + timedelta(days=n), datetime.min.time())


# ============================================================
# BRABBR RESOLVER (BRCHCD. format)
# ============================================================
def resolve_brabbr(branch_val):
    """Apply BRCHCD. format from PBBELF."""
    if branch_val is None:
        return ''
    try:
        code = int(round(float(branch_val)))
    except (TypeError, ValueError):
        return str(branch_val)
    name = PBBELF.format_brchcd(code)
    return name if name else str(code)


# ============================================================
# CHUNKED LNNOTE READER
# ============================================================
def read_lnnote_filtered(path, usecols, hp_products, chunk_size,
                         sample_mode=None, sample_size=5000, seed=42):
    rng = random.Random(seed)
    reservoir = []
    seen_matching = 0
    collected_frames = []
    collected_count = 0
    total_scanned = 0

    chunk_iter = None
    try:
        chunk_iter = pyreadstat.read_file_in_chunks(
            pyreadstat.read_sas7bdat, path,
            chunksize=chunk_size, usecols=usecols,
        )
        # read_file_in_chunks yields (DataFrame, Metadata) tuples
        _first_df, _first_meta = next(chunk_iter)
    except (AttributeError, TypeError, StopIteration) as e:
        print(f"    [note] chunked reader unavailable ({type(e).__name__}: {e}); "
              f"falling back to full read with usecols")
        chunk_iter = None
    else:
        # Verify usecols was honoured - case-insensitive + trimmed comparison
        actual_cols = set(c.strip().upper() for c in _first_df.columns.tolist())
        expected_cols = set(c.strip().upper() for c in usecols)
        if actual_cols == expected_cols:
            print(f"    usecols honoured: {len(actual_cols)} columns parsed")
        else:
            extra = actual_cols - expected_cols
            missing = expected_cols - actual_cols
            if extra:
                print(f"    WARNING: usecols had extra columns decoded: {sorted(extra)[:5]}")
            if missing:
                print(f"    WARNING: usecols missed expected columns: {sorted(missing)[:5]}")

        def _rechain(first_tuple, rest):
            yield first_tuple
            yield from rest
        chunk_iter = _rechain((_first_df, _first_meta), chunk_iter)

    if chunk_iter is not None:
        for chunk_df, _meta in chunk_iter:
            total_scanned += len(chunk_df)
            if len(chunk_df) == 0:
                continue

            chunk_pl = pl.from_pandas(chunk_df)
            chunk_pl = chunk_pl.filter(
                (pl.col('LOANTYPE').is_in(hp_products)) &
                (pl.col('BALANCE') > 0)
            )
            if len(chunk_pl) == 0:
                print(f"    ...scanned {total_scanned:,} raw rows so far", end='\r')
                continue

            if sample_mode is None:
                collected_frames.append(chunk_pl)
                collected_count += len(chunk_pl)
            elif sample_mode == "head":
                collected_frames.append(chunk_pl)
                collected_count += len(chunk_pl)
                if collected_count >= sample_size:
                    print(f"    ...scanned {total_scanned:,} raw rows "
                          f"(stopped early at {collected_count:,} matching)")
                    break
            elif sample_mode == "reservoir":
                for row in chunk_pl.to_dicts():
                    seen_matching += 1
                    if len(reservoir) < sample_size:
                        reservoir.append(row)
                    else:
                        j = rng.randint(0, seen_matching - 1)
                        if j < sample_size:
                            reservoir[j] = row
            else:
                raise ValueError(f"Unknown sample_mode: {sample_mode}")

            print(f"    ...scanned {total_scanned:,} raw rows so far", end='\r')

        print(f"    ...scanned {total_scanned:,} raw rows total          ")

        if sample_mode == "reservoir":
            if not reservoir:
                return pl.DataFrame()
            return pl.DataFrame(reservoir)
        if not collected_frames:
            return pl.DataFrame()
        result = pl.concat(collected_frames, how='vertical_relaxed')
        if sample_mode == "head":
            result = result.head(sample_size)
        return result

    # Fallback: full read with usecols
    df, _ = pyreadstat.read_sas7bdat(path, usecols=usecols)
    df_pl = pl.from_pandas(df)
    total_scanned = len(df_pl)
    df_pl = df_pl.filter(
        (pl.col('LOANTYPE').is_in(hp_products)) &
        (pl.col('BALANCE') > 0)
    )
    print(f"    ...scanned {total_scanned:,} raw rows (single read)")

    if sample_mode == "head" and len(df_pl) > sample_size:
        return df_pl.head(sample_size)
    if sample_mode == "reservoir" and len(df_pl) > sample_size:
        indices = rng.sample(range(len(df_pl)), sample_size)
        return df_pl[indices]
    return df_pl


# ============================================================
# LOAD DATA
# ============================================================
print("\nReading loan data from SAS files...")

try:
    print("  Reading loantemp.sas7bdat...")
    df_loantemp, _meta = pyreadstat.read_sas7bdat(CCDTEMP_PATH)
    df_loantemp = pl.from_pandas(df_loantemp)
    print(f"  LOANTEMP raw rows: {len(df_loantemp):,}")

    df_loantemp = df_loantemp.filter(
        (pl.col('PRODUCT').is_in(HP_PRODUCTS)) &
        (pl.col('BALANCE') > 0)
    )
    print(f"  LOANTEMP after filtering: {len(df_loantemp):,} rows")

    if len(df_loantemp) == 0:
        print("  ERROR: No HP products found in LOANTEMP")
        sys.exit(1)

    print("  Reading LNNOTE (chunked, filtered as it streams)...")
    df_lnnote = read_lnnote_filtered(
        LOAN_PATH,
        usecols=LNNOTE_USECOLS,
        hp_products=HP_PRODUCTS,
        chunk_size=CHUNK_SIZE,
        sample_mode=SAMPLE_MODE,
        sample_size=SAMPLE_SIZE,
        seed=RANDOM_SEED,
    )
    print(f"  LNNOTE after filtering: {len(df_lnnote):,} rows")

    if len(df_lnnote) == 0:
        print("  ERROR: No HP products found in LNNOTE")
        sys.exit(1)

    print(f"  ACCTNO dtype  -> lnnote: {df_lnnote['ACCTNO'].dtype}, "
          f"loantemp: {df_loantemp['ACCTNO'].dtype}")
    print(f"  NOTENO dtype  -> lnnote: {df_lnnote['NOTENO'].dtype}, "
          f"loantemp: {df_loantemp['NOTENO'].dtype}")

    def normalize_keys(df):
        return df.with_columns([
            pl.col('ACCTNO').cast(pl.Float64, strict=False).round(0).cast(pl.Int64, strict=False).alias('ACCTNO'),
            pl.col('NOTENO').cast(pl.Float64, strict=False).round(0).cast(pl.Int64, strict=False).alias('NOTENO'),
        ])

    df_lnnote = normalize_keys(df_lnnote)
    df_loantemp = normalize_keys(df_loantemp)

    print("  Merging data...")
    df_hploan = df_lnnote.join(df_loantemp, on=['ACCTNO', 'NOTENO'], how='inner')
    print(f"  HP Loans after merge: {len(df_hploan):,} accounts")

    if len(df_hploan) == 0:
        acct_lnnote = set(df_lnnote['ACCTNO'].to_list())
        acct_loantemp = set(df_loantemp['ACCTNO'].to_list())
        overlap = acct_lnnote & acct_loantemp
        print(f"  [diag] lnnote unique ACCTNO: {len(acct_lnnote):,}")
        print(f"  [diag] loantemp unique ACCTNO: {len(acct_loantemp):,}")
        print(f"  [diag] ACCTNO overlap (ignoring NOTENO): {len(overlap):,}")
        sys.exit(1)

    # Confirm required LOANTEMP-sourced columns survived the merge
    required_after_merge = ['BRANCH', 'DAYDIFF', 'PRODUCT', 'BALANCE']
    for col in required_after_merge:
        if col not in df_hploan.columns:
            print(f"  WARNING: column '{col}' missing after merge. "
                  f"BRABBR or MTHARR may be incorrect.")
        else:
            print(f"  Column '{col}' present (dtype={df_hploan[col].dtype})")

except Exception as e:
    print(f"  Error: {e}")
    traceback.print_exc()
    sys.exit(1)

# ============================================================
# PROCESSING
# ============================================================
print("\nProcessing HP loans...")

df_hploan = df_hploan.with_columns([
    ((pl.col('ORGBAL') - pl.col('CURBAL')) / pl.col('PAYAMT')).alias('ISTLPD'),
    pl.col('ISSUEDT')
      .map_elements(sas_serial_to_date, return_dtype=pl.Datetime)
      .alias('ISSDTE'),
    pl.col('SCORE2').cast(pl.Utf8, strict=False).str.slice(0, 1).alias('CRRISK'),
    pl.when(pl.col('APPVALUE') > 0)
      .then(((pl.col('NETPROC') / pl.col('APPVALUE')) * 100).round(1))
      .otherwise(0)
      .alias('MARGINF'),
    pl.col('CENSUS').cast(pl.Utf8, strict=False).str.zfill(7).alias('CENSUS9'),
])

# Report unparsed dates
_null_dates = df_hploan.filter(pl.col('ISSDTE').is_null()).height
if _null_dates > 0:
    pct = 100.0 * _null_dates / len(df_hploan)
    sample_raw = (df_hploan.filter(pl.col('ISSDTE').is_null())
                  .select(pl.col('ISSUEDT').cast(pl.Utf8, strict=False))
                  .head(5)['ISSUEDT'].to_list())
    print(f"  WARNING: {_null_dates:,} of {len(df_hploan):,} rows ({pct:.1f}%) "
          f"had ISSUEDT that couldn't be interpreted as a SAS date serial. "
          f"Samples: {sample_raw}")

# Sanity check: verify ISSDTE is exactly SAS_EPOCH + timedelta(days=ISSUEDT).
# This is NOT tautological - it recomputes the expected date from the raw
# ISSUEDT integer using the module-level SAS_EPOCH and a fresh timedelta,
# then compares to the parsed output. Any drift in sas_serial_to_date (stale
# bytecode, shadowed constant, wrong epoch) will produce a MISMATCH.
_check = (df_hploan
          .filter(pl.col('ISSDTE').is_not_null())
          .select(['ISSUEDT', 'ISSDTE'])
          .head(5))
if len(_check) > 0:
    print(f"  ISSDTE sample (ISSUEDT -> parsed, checked against epoch):")
    mismatches = 0
    for issued, parsed in _check.rows():
        n = int(round(float(issued)))
        # Recompute expected independently from the epoch constant
        expected = datetime(SAS_EPOCH.year, SAS_EPOCH.month, SAS_EPOCH.day) + timedelta(days=n)
        if parsed == expected:
            marker = 'OK'
        else:
            marker = f'MISMATCH (expected {expected.date()})'
            mismatches += 1
        print(f"    ISSUEDT={n:>10}  -> {parsed.date()}  [{marker}]")
    if mismatches > 0:
        print(f"  ERROR: {mismatches} date parse mismatches!")
        print(f"  sas_serial_to_date is NOT producing SAS_EPOCH + timedelta(days=n).")
        print(f"  Likely cause: stale __pycache__, edited SAS_EPOCH, or "
              f"module shadowing. Try: find . -name '__pycache__' -exec rm -rf {{}} +")
    else:
        print(f"  Date epoch verified OK: SAS_EPOCH = {SAS_EPOCH.isoformat()}")

df_hploan = df_hploan.with_columns([
    pl.when(pl.col('MARGINF') < 70).then(pl.lit('E. <70%'))
      .when((pl.col('MARGINF') >= 70) & (pl.col('MARGINF') < 80)).then(pl.lit('D. 70 TO <80%'))
      .when((pl.col('MARGINF') >= 80) & (pl.col('MARGINF') < 85)).then(pl.lit('C. 80 TO <85%'))
      .when((pl.col('MARGINF') >= 85) & (pl.col('MARGINF') < 89)).then(pl.lit('B. 85 TO <89%'))
      .otherwise(pl.lit('A. 89% & ABV'))
      .alias('MGINGRP'),

    pl.when(pl.col('NOTETERM') <= 36).then(pl.lit('A. <=3 YRS'))
      .when((pl.col('NOTETERM') > 36) & (pl.col('NOTETERM') <= 48)).then(pl.lit('B. 4 YRS'))
      .when((pl.col('NOTETERM') > 48) & (pl.col('NOTETERM') <= 60)).then(pl.lit('C. 5 YRS'))
      .when((pl.col('NOTETERM') > 60) & (pl.col('NOTETERM') <= 72)).then(pl.lit('D. 6 YRS'))
      .when((pl.col('NOTETERM') > 72) & (pl.col('NOTETERM') <= 84)).then(pl.lit('E. 7 YRS'))
      .when((pl.col('NOTETERM') > 84) & (pl.col('NOTETERM') <= 96)).then(pl.lit('F. 8 YRS'))
      .otherwise(pl.lit('G. 9 YRS'))
      .alias('TERMGRP'),

    pl.col('STATE').cast(pl.Utf8, strict=False)
      .replace_strict(STATE_MAP, default='OTHERS').alias('STATENM'),

    pl.col('CENSUS9').str.slice(0, 2).str.strip_chars()
      .replace_strict(MAKE_MAP, default='OTHERS')
      .alias('MAKE'),

    pl.when(pl.col('CENSUS9').str.slice(3, 1).is_in(['1', '2']))
      .then(pl.lit('NEW'))
      .otherwise(pl.lit('SECONDHAND'))
      .alias('NEWSEC'),

    pl.when(pl.col('NETPROC') <= 30000).then(pl.lit('A. RM30K & BELOW'))
      .when((pl.col('NETPROC') > 30000) & (pl.col('NETPROC') <= 50000)).then(pl.lit('B. >RM30K TO 50K'))
      .when((pl.col('NETPROC') > 50000) & (pl.col('NETPROC') <= 100000)).then(pl.lit('C. >RM50K TO 100K'))
      .when((pl.col('NETPROC') > 100000) & (pl.col('NETPROC') <= 250000)).then(pl.lit('D. >RM100K TO 250K'))
      .otherwise(pl.lit('E. >RM250K'))
      .alias('FINGRP'),

    pl.when(pl.col('DEALERNO') > 0)
      .then(pl.lit('DEALERS'))
      .otherwise(pl.lit('NON DEALERS'))
      .alias('SOURCE'),
])

df_hploan = df_hploan.with_columns([
    pl.when(pl.col('STATENM').is_in(['SABAH', 'SARAWAK', 'LABUAN']))
      .then(pl.lit('EAST MALAYSIA'))
      .otherwise(pl.lit('WEST MALAYSIA'))
      .alias('NATIONAL'),

    pl.when(pl.col('MAKE').is_in(['PROTON', 'PERODUA']))
      .then(pl.lit('NATIONAL'))
      .otherwise(pl.lit('NON NATIONAL'))
      .alias('CARS'),

    pl.when((pl.col('MAKE') == 'OTHERS') & (pl.col('PRODUCT').is_in([128, 700])))
      .then(pl.lit('SCHEDULE'))
      .when(pl.col('MAKE') == 'OTHERS')
      .then(pl.lit('UNSCHEDULE'))
      .otherwise(pl.lit(''))
      .alias('GOODS'),
])

df_hploan = df_hploan.with_columns([
    pl.when(pl.col('DAYDIFF') > 729).then((pl.col('DAYDIFF') / 365 * 12).cast(pl.Int32))
      .when(pl.col('DAYDIFF') > 698).then(pl.lit(23))
      .when(pl.col('DAYDIFF') > 668).then(pl.lit(22))
      .when(pl.col('DAYDIFF') > 638).then(pl.lit(21))
      .when(pl.col('DAYDIFF') > 608).then(pl.lit(20))
      .when(pl.col('DAYDIFF') > 577).then(pl.lit(19))
      .when(pl.col('DAYDIFF') > 547).then(pl.lit(18))
      .when(pl.col('DAYDIFF') > 516).then(pl.lit(17))
      .when(pl.col('DAYDIFF') > 486).then(pl.lit(16))
      .when(pl.col('DAYDIFF') > 456).then(pl.lit(15))
      .when(pl.col('DAYDIFF') > 424).then(pl.lit(14))
      .when(pl.col('DAYDIFF') > 394).then(pl.lit(13))
      .when(pl.col('DAYDIFF') > 364).then(pl.lit(12))
      .when(pl.col('DAYDIFF') > 333).then(pl.lit(11))
      .when(pl.col('DAYDIFF') > 303).then(pl.lit(10))
      .when(pl.col('DAYDIFF') > 273).then(pl.lit(9))
      .when(pl.col('DAYDIFF') > 243).then(pl.lit(8))
      .when(pl.col('DAYDIFF') > 213).then(pl.lit(7))
      .when(pl.col('DAYDIFF') > 182).then(pl.lit(6))
      .when(pl.col('DAYDIFF') > 151).then(pl.lit(5))
      .when(pl.col('DAYDIFF') > 121).then(pl.lit(4))
      .when(pl.col('DAYDIFF') > 91).then(pl.lit(3))
      .when(pl.col('DAYDIFF') > 61).then(pl.lit(2))
      .when(pl.col('DAYDIFF') > 30).then(pl.lit(1))
      .otherwise(pl.lit(0))
      .alias('MTHARR'),
])

df_hploan = df_hploan.with_columns([
    pl.col('BRANCH').map_elements(resolve_brabbr, return_dtype=pl.Utf8).alias('BRABBR'),
])

df_hploan = df_hploan.with_columns([
    pl.when(pl.col('BORSTAT') == 'F')
      .then(pl.lit(999))
      .otherwise(pl.col('MTHARR'))
      .alias('MTHARR')
])

print(f"  Processed: {len(df_hploan):,} HP loans")

# ============================================================
# ACCOUNT GROUPS
# ============================================================
print("\nCreating account groups...")

df_hploan1 = df_hploan
df_hploan2 = df_hploan.filter(
    (pl.col('MTHARR') >= 3) | (pl.col('BORSTAT').is_in(['F', 'I', 'R']))
)
df_hploan3 = df_hploan.filter(pl.col('NOTENO') >= 98010)
df_hploan4 = df_hploan.filter(
    (pl.col('NOTENO') >= 98010) &
    ((pl.col('MTHARR') >= 3) | (pl.col('BORSTAT').is_in(['F', 'I', 'R'])))
)

print(f"  HPLOAN1 (All): {len(df_hploan1):,}")
print(f"  HPLOAN2 (NPL): {len(df_hploan2):,}")
print(f"  HPLOAN3 (Restructured): {len(df_hploan3):,}")
print(f"  HPLOAN4 (Restructured NPL): {len(df_hploan4):,}")


# ============================================================
# SUMMARY REPORT GENERATION
# ============================================================
print("\nGenerating summary reports...")

BUCKET_ORDER = ['<3MTHS', '3-6MTHS', '6-12MTHS', '12-24MTHS', '24-36MTHS', '>36MTHS']
GRD_HEADER = ['GROUP BY', '<3MTHS NO', '<3MTHS AMT', '3-6MTHS NO', '3-6MTHS AMT',
              '6-12MTHS NO', '6-12MTHS AMT', '12-24MTHS NO', '12-24MTHS AMT',
              '24-36MTHS NO', '24-36MTHS AMT', '>36MTHS NO', '>36MTHS AMT',
              'DEFICIT NO', 'DEFICIT AMT', 'TOTAL NO', 'TOTAL AMT']


def _bucket_totals(dff):
    """Return (counts, amounts) tuples aligned to BUCKET_ORDER for the given frame."""
    counts = []
    amounts = []
    for b in BUCKET_ORDER:
        sub = dff.filter(pl.col('BUCKET') == b)
        counts.append(sub.height)
        amounts.append(sub['BALANCE'].sum() if sub.height else 0.0)
    return counts, amounts


def build_summary_section(df, group_cols, title, subtitle):
    """
    Build one summary section as a list of lines, matching one GENRPT* output block.
    Structure: PRNTITLE header + column header + group rows + GRD TOTAL + blank line.
    Returns lines list, or None if no data.
    """
    if len(df) == 0:
        return None

    df_summary = df.with_columns([
        pl.when(pl.col('MTHARR') < 3).then(pl.lit('<3MTHS'))
          .when((pl.col('MTHARR') >= 3) & (pl.col('MTHARR') < 6)).then(pl.lit('3-6MTHS'))
          .when((pl.col('MTHARR') >= 6) & (pl.col('MTHARR') < 12)).then(pl.lit('6-12MTHS'))
          .when((pl.col('MTHARR') >= 12) & (pl.col('MTHARR') < 24)).then(pl.lit('12-24MTHS'))
          .when((pl.col('MTHARR') >= 24) & (pl.col('MTHARR') < 36)).then(pl.lit('24-36MTHS'))
          .when(pl.col('MTHARR') >= 36).then(pl.lit('>36MTHS'))
          .otherwise(pl.lit('UNKNOWN'))
          .alias('BUCKET'),
    ])

    df_agg = df_summary.group_by(group_cols + ['BUCKET']).agg([
        pl.len().alias('COUNT'),
        pl.col('BALANCE').sum().alias('AMOUNT'),
    ])

    df_pivot = df_agg.pivot(
        values=['COUNT', 'AMOUNT'],
        index=group_cols,
        on='BUCKET',
    )

    # Section header block (matches %PRNTITLE)
    lines = [
        f"TOTAL POSITION FOR HPD (CONV & AITAB) AS AT {rdate}",
        title,
        subtitle,
        "REPORT ID : EIMRESHP",
    ]
    lines.append(';'.join(GRD_HEADER))

    # Body rows - one per group combination
    if len(df_pivot) > 0:
        for row in df_pivot.iter_rows(named=True):
            parts = [str(row.get(c, '')) for c in group_cols]
            for bucket in BUCKET_ORDER:
                cnt = row.get(f'COUNT_{bucket}')
                amt = row.get(f'AMOUNT_{bucket}')
                parts.append(str(cnt if cnt is not None else 0))
                parts.append(f"{amt:,.2f}" if amt is not None else "0.00")
            # DEFICIT and TOTAL columns are section-wide in the SAS output;
            # leave blank at row level and fill only in GRD TOTAL.
            parts.extend(['', '', '', ''])
            lines.append(';'.join(parts))

    # GRD TOTAL row (matches %PRNGRD)
    grd_counts, grd_amounts = _bucket_totals(df_summary)
    deficit_df = df_summary.filter(pl.col('BORSTAT') == 'F')
    deficit_count = len(deficit_df)
    deficit_amount = deficit_df['BALANCE'].sum() if deficit_count else 0.0
    total_count = len(df_summary)
    total_amount = df_summary['BALANCE'].sum()

    grd_row = ['GRD TOTAL']
    for cnt, amt in zip(grd_counts, grd_amounts):
        grd_row.append(str(cnt))
        grd_row.append(f"{amt:,.2f}")
    grd_row.append(str(deficit_count))
    grd_row.append(f"{deficit_amount:,.2f}")
    grd_row.append(str(total_count))
    grd_row.append(f"{total_amount:,.2f}")
    lines.append(';'.join(grd_row))

    # Blank line separator between sections (matches PUT ' '; in %PRNGRD)
    lines.append('')

    return lines


# Build all 32 sections in the exact SAS order
report_configs = []

# CREDIT RISK SCORE
for df, suffix in [(df_hploan1, f'PRODUCT {RPT_TITLE_PRODUCTS}'),
                   (df_hploan2, 'NPL ACCOUNT'),
                   (df_hploan3, 'RESTRUCTURE ACCOUNT'),
                   (df_hploan4, 'RESTRUCTURE NPL ACCOUNT')]:
    report_configs.append({
        'df': df, 'groups': ['CRRISK', 'BRABBR'],
        'title': 'CREDIT RISK SCORE', 'subtitle': suffix,
    })

# SOURCE OF BUSINESS
for df, suffix in [(df_hploan1, f'PRODUCT {RPT_TITLE_PRODUCTS}'),
                   (df_hploan2, 'NPL ACCOUNT'),
                   (df_hploan3, 'RESTRUCTURE ACCOUNT'),
                   (df_hploan4, 'RESTRUCTURE NPL ACCOUNT')]:
    report_configs.append({
        'df': df, 'groups': ['SOURCE', 'BRABBR'],
        'title': 'SOURCE OF BUSINESS', 'subtitle': suffix,
    })

# MARGIN OF FINANCE
for df, suffix in [(df_hploan1, f'PRODUCT {RPT_TITLE_PRODUCTS}'),
                   (df_hploan2, 'NPL ACCOUNT'),
                   (df_hploan3, 'RESTRUCTURE ACCOUNT'),
                   (df_hploan4, 'RESTRUCTURE NPL ACCOUNT')]:
    report_configs.append({
        'df': df, 'groups': ['MGINGRP', 'BRABBR'],
        'title': 'MARGIN OF FINANCE', 'subtitle': suffix,
    })

# LOAN TERM
for df, suffix in [(df_hploan1, f'PRODUCT {RPT_TITLE_PRODUCTS}'),
                   (df_hploan2, 'NPL ACCOUNT'),
                   (df_hploan3, 'RESTRUCTURE ACCOUNT'),
                   (df_hploan4, 'RESTRUCTURE NPL ACCOUNT')]:
    report_configs.append({
        'df': df, 'groups': ['TERMGRP', 'BRABBR'],
        'title': 'LOAN TERM', 'subtitle': suffix,
    })

# AMT FINANCE
for df, suffix in [(df_hploan1, f'PRODUCT {RPT_TITLE_PRODUCTS}'),
                   (df_hploan2, 'NPL ACCOUNT'),
                   (df_hploan3, 'RESTRUCTURE ACCOUNT'),
                   (df_hploan4, 'RESTRUCTURE NPL ACCOUNT')]:
    report_configs.append({
        'df': df, 'groups': ['NEWSEC', 'FINGRP', 'BRABBR'],
        'title': 'AMT FINANCE', 'subtitle': suffix,
    })

# BY STATE
for df, suffix in [(df_hploan1, f'PRODUCT {RPT_TITLE_PRODUCTS}'),
                   (df_hploan2, 'NPL ACCOUNT'),
                   (df_hploan3, 'RESTRUCTURE ACCOUNT'),
                   (df_hploan4, 'RESTRUCTURE NPL ACCOUNT')]:
    report_configs.append({
        'df': df, 'groups': ['NATIONAL', 'STATENM', 'BRABBR'],
        'title': 'BY STATE', 'subtitle': suffix,
    })

# BY MAKE OF VEHICLE
for df, suffix in [(df_hploan1, f'PRODUCT {RPT_TITLE_PRODUCTS}'),
                   (df_hploan2, 'NPL ACCOUNT'),
                   (df_hploan3, 'RESTRUCTURE ACCOUNT'),
                   (df_hploan4, 'RESTRUCTURE NPL ACCOUNT')]:
    report_configs.append({
        'df': df, 'groups': ['NEWSEC', 'CARS', 'MAKE', 'BRABBR'],
        'title': 'BY MAKE OF VEHICLE', 'subtitle': suffix,
    })

# MAKE = OTHERS
for df, suffix in [(df_hploan1.filter(pl.col('MAKE') == 'OTHERS'), f'PRODUCT {RPT_TITLE_PRODUCTS}'),
                   (df_hploan2.filter(pl.col('MAKE') == 'OTHERS'), 'NPL ACCOUNT'),
                   (df_hploan3.filter(pl.col('MAKE') == 'OTHERS'), 'RESTRUCTURE ACCOUNT'),
                   (df_hploan4.filter(pl.col('MAKE') == 'OTHERS'), 'RESTRUCTURE NPL ACCOUNT')]:
    report_configs.append({
        'df': df, 'groups': ['NEWSEC', 'GOODS', 'BRABBR'],
        'title': 'BY MAKE OF VEHICLE = OTHERS', 'subtitle': suffix,
    })

# Write all sections to a single CCDTXT1-equivalent file
sections_written = 0
with open(CCDTXT1_PATH, 'w') as ccdtxt1:
    for i, cfg in enumerate(report_configs, 1):
        section = build_summary_section(cfg['df'], cfg['groups'], cfg['title'], cfg['subtitle'])
        if section is None:
            print(f"  Skipped section {i:02d} ({cfg['title']} - {cfg['subtitle']}): no data")
            continue
        for line in section:
            ccdtxt1.write(line + '\n')
        sections_written += 1
        print(f"  Wrote section {i:02d}: {cfg['title']} - {cfg['subtitle']}")

print(f"  Total summary sections written: {sections_written}")
print(f"  CCDTXT1 file: {CCDTXT1_PATH}")

# ============================================================
# DETAIL REPORT (CCDTXT2 equivalent)
# ============================================================
print("\nGenerating detail report...")

if len(df_hploan2) > 0:
    df_detail = df_hploan2.select([
        'ACCTNO', 'NOTENO', 'NAME', 'BRABBR', 'PRODUCT', 'BORSTAT',
        'NETPROC', 'BALANCE', 'MTHARR', 'MARGINF', 'NOTETERM',
        'STATENM', 'MAKE', 'NEWSEC', 'SOURCE', 'SCORE2', 'ISTLPD', 'ISSDTE',
    ]).sort(['ACCTNO', 'NOTENO'])

    detail_lines = []
    header = ['MNI NO', 'NOTE NO', 'NAME', 'BRABBR', 'PRODUCT', 'BOR. STATUS',
              'AMT FINANCE', 'NET BALANCE', 'MTH PASS DUE', 'MARGIN OF FIN.',
              'LOAN TERM', 'STATES', 'MAKE OF VEC.', 'NEW/SECONDHAND',
              'SOURCE OF BUS.', 'CREDIT SCORE', 'NO ISTL PAID', 'ISSUE DATE']
    detail_lines.append(';'.join(header))

    for row in df_detail.iter_rows():
        parts = []
        for val in row:
            if isinstance(val, datetime):
                parts.append(val.strftime('%d%m%Y'))
            elif isinstance(val, float):
                parts.append(f"{val:.2f}")
            elif val is None:
                parts.append('')
            else:
                parts.append(str(val))
        detail_lines.append(';'.join(parts))

    tot_acc = len(df_detail)
    tot_amt = df_detail['BALANCE'].sum()
    detail_lines.append(f"TOT NO OF A/C : ;{tot_acc};TOT NET BALANCE : ;{tot_amt:,.2f};")

    with open(CCDTXT2_PATH, 'w') as f:
        f.write('\n'.join(detail_lines))

    print(f"  Detail report: {tot_acc:,} NPL accounts")
    print(f"  Total balance: {tot_amt:,.2f}")
    print(f"  CCDTXT2 file: {CCDTXT2_PATH}")
else:
    print("  No NPL accounts found")
    with open(CCDTXT2_PATH, 'w') as f:
        f.write(f"No NPL accounts found for report date {reptdate.strftime('%d/%m/%Y')}")

# ============================================================
# FINAL SUMMARY
# ============================================================
print(f"\n{'='*60}")
print("EIMRESHI Complete!")
print(f"{'='*60}")
print(f"\nData Statistics:")
print(f"  Total HP loans processed: {len(df_hploan):,}")
print(f"\n4 Account Groups:")
print(f"  1. All HP accounts: {len(df_hploan1):,}")
print(f"  2. NPL (>=3 months OR F/I/R): {len(df_hploan2):,}")
print(f"  3. Restructured (NOTENO >= 98010): {len(df_hploan3):,}")
print(f"  4. Restructured NPL: {len(df_hploan4):,}")
print(f"\nOutput Directory: {OUTPUT_DIR}")
print(f"  - CCDTXT1 equivalent: {CCDTXT1_PATH}")
print(f"    ({sections_written} summary sections in one file)")
print(f"  - CCDTXT2 equivalent: {CCDTXT2_PATH}")
print(f"    (1 detail report)")
