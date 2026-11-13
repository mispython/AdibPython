#!/usr/bin/env python3
"""
EIBDAWSA - Average Savings Account Analysis
Reads DPTRBLGS parquet, writes ONLY a SAS dataset (.sas7bdat) via saspy.
Report date is set to (today - 1 day) to match SAS DPTRBL report date logic.
"""

import duckdb
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
import gc
import warnings
warnings.filterwarnings('ignore')

# Format libraries (must be on PYTHONPATH)
import PBMISFMT
import PBBDPFMT

# ---------------------------------------------------------------------------
# Paths (absolute)
# ---------------------------------------------------------------------------
INPUT_FILE = Path('/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDAWSA/DPTRBLGS.parquet')
OUTPUT_DIR = Path('/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDAWSA')
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# SAS macro vars
AGELIMIT = 12
MAXAGE   = 18
AGEBELOW = 11

# ---------------------------------------------------------------------------
# SAS format wrappers
# ---------------------------------------------------------------------------
f_sacustcd = PBBDPFMT.sacustcd_format
f_statecd  = PBBDPFMT.statecd_format
f_saprod   = PBBDPFMT.saprod_format
f_sadenom  = PBBDPFMT.sadenom_format


def apply_format(func, value):
    if value is None or (isinstance(value, float) and value != value):
        try:
            return func(None)
        except Exception:
            return '0'
    return func(value)


# ---------------------------------------------------------------------------
# Age calculation (SAS parity)
# ---------------------------------------------------------------------------
def calculate_age(bdate_int, reptdate):
    if bdate_int is None or bdate_int == 0:
        return 0
    try:
        s = str(int(bdate_int)).zfill(6)
        bday   = int(s[4:6])
        bmonth = int(s[2:4])
        byear  = int(s[0:2])
        byear  = 1900 + byear if byear > 30 else 2000 + byear

        age = reptdate.year - byear
        if (bmonth == reptdate.month and bday > reptdate.day) or bmonth > reptdate.month:
            age -= 1

        if age < AGELIMIT:
            return AGEBELOW
        if age >= MAXAGE:
            return MAXAGE
        return AGELIMIT
    except Exception:
        return 0


# ---------------------------------------------------------------------------
# Range CASE generator
# ---------------------------------------------------------------------------
def _range_case(col, bounds, default_label):
    parts = [f"WHEN {col} < {b} THEN '{b}'" for b in bounds]
    return f"CASE {' '.join(parts)} ELSE '{default_label}' END"


DDRANGE_BOUNDS  = [1000, 2000, 2500, 3000, 5000, 10000, 20000, 30000, 40000,
                   50000, 75000, 100000, 150000, 200000, 250000, 500000,
                   1000000, 2000000, 3000000, 4000000, 5000000]
ISARANGE_BOUNDS = [1000, 5000, 25000, 50000]
IBWRNGE_BOUNDS  = [10000, 30000, 50000, 75000, 100000]
CARANGE_BOUNDS  = [2000, 3000, 5000, 10000, 30000, 50000, 75000, 100000,
                   150000, 200000]


# ---------------------------------------------------------------------------
# Main processing
# ---------------------------------------------------------------------------
def process_dptrblgs(input_file: Path, reptdate: datetime) -> pd.DataFrame:
    reptyear = reptdate.year

    print("Initializing DuckDB ...")
    con = duckdb.connect(':memory:')
    con.execute("PRAGMA memory_limit='12GB'")
    con.execute("PRAGMA threads=8")
    tmp = Path('/tmp/duckdb_tmp'); tmp.mkdir(parents=True, exist_ok=True)
    con.execute(f"PRAGMA temp_directory='{tmp}'")

    print(f"Registering parquet: {input_file}")
    con.execute(f"CREATE OR REPLACE VIEW dptrblgs AS SELECT * FROM parquet_scan('{input_file}')")

    cols = con.execute("DESCRIBE dptrblgs").fetchdf()
    col_map = {c.upper(): c for c in cols['column_name'].tolist()}
    print("Parquet columns:", ', '.join(cols['column_name'].tolist()))

    def col(name):
        return f'"{col_map.get(name.upper(), name)}"'

    dd_range  = _range_case(col('AVGAMT'), DDRANGE_BOUNDS,  '5000001')
    isa_range = _range_case(col('CURBAL'), ISARANGE_BOUNDS, '50001')
    ibw_range = _range_case(col('CURBAL'), IBWRNGE_BOUNDS,  '100001')
    ca_range  = _range_case(col('CURBAL'), CARANGE_BOUNDS,  '200001')

    sql = f"""
    WITH filtered AS (
        SELECT
            {col('BANKNO')}   AS BANKNO,
            {col('REPTNO')}   AS REPTNO,
            {col('FMTCODE')}  AS FMTCODE,
            {col('BRANCH')}   AS BRANCH,
            {col('ACCTNO')}   AS ACCTNO,
            {col('CLOSEDT')}  AS CLOSEDT,
            {col('OPENDT')}   AS OPENDT,
            {col('CUSTCODE')} AS CUSTCODE,
            {col('PURPOSE')}  AS PURPOSE,
            {col('OPENIND')}  AS OPENIND,
            {col('AVGAMT')}   AS AVGAMT,
            {col('PRODUCT')}  AS PRODUCT,
            {col('RACE')}     AS RACE,
            {col('CURBAL')}   AS CURBAL,
            {col('BDATE')}    AS BDATE
        FROM dptrblgs
        WHERE {col('BANKNO')}  = 33
          AND {col('REPTNO')}  = 1001
          AND {col('FMTCODE')} = 1
          AND CAST({col('OPENIND')} AS VARCHAR) NOT IN ('B','C','P')
          AND {col('PRODUCT')} NOT IN (297, 298)
    )
    SELECT
        PRODUCT,
        BRANCH,
        ACCTNO,
        COALESCE(NULLIF(CAST(PURPOSE AS VARCHAR), ''), '0') AS PURPOSE,
        COALESCE(NULLIF(CAST(RACE    AS VARCHAR), ''), '0') AS RACE,
        CUSTCODE,
        AVGAMT,
        CURBAL,
        OPENDT,
        CLOSEDT,
        BDATE,
        CASE
            WHEN OPENDT <> 0 AND CLOSEDT = 0
                 AND CAST(SUBSTR(LPAD(CAST(OPENDT AS VARCHAR), 6, '0'), 1, 2) AS INTEGER)
                     = {reptyear - 2000}
            THEN 1 ELSE 0
        END AS ACCYTD,
        {dd_range}  AS AVGRNGE,
        CASE
            WHEN PRODUCT = 214 THEN {isa_range}
            WHEN PRODUCT = 207 THEN {ibw_range}
            ELSE {ca_range}
        END AS DEPRANGE
    FROM filtered
    WHERE PRODUCT IN (204, 207, 215)
    """

    print("Running main query ...")
    df = con.execute(sql).fetchdf()
    print(f"Extracted {len(df):,} savings rows")

    if df.empty:
        con.close()
        return df

    print("Applying SAS formats ...")
    df['CUSTCD']  = df['CUSTCODE'].map(lambda v: apply_format(f_sacustcd, v))
    df['STATECD'] = df['BRANCH'].map(lambda v: apply_format(f_statecd, v))
    df['PRODCD']  = df['PRODUCT'].map(lambda v: apply_format(f_saprod, v))
    df['DENOM']   = df['PRODUCT'].map(lambda v: apply_format(f_sadenom, v))
    df['AGE']     = df['BDATE'].map(lambda v: calculate_age(v, reptdate))

    print("Aggregating ...")
    agg = (
        df.groupby(['PURPOSE', 'RACE', 'CUSTCD', 'AVGRNGE', 'DEPRANGE', 'PRODUCT'],
                   dropna=False)
        .agg(
            NOACCT  = ('ACCTNO', 'count'),
            CURBAL  = ('CURBAL', 'sum'),
            AVGACCT = ('AVGAMT', 'count'),
            AVGAMT  = ('AVGAMT', 'sum'),
            ACCYTD  = ('ACCYTD', 'sum'),
        )
        .reset_index()
    )

    # REPTDATE stored as SAS date numeric (YYYYMMDD-like string used historically)
    agg['REPTDATE'] = int(reptdate.strftime('%Y%m%d'))
    agg['DENOM']    = agg['PRODUCT'].map(lambda v: apply_format(f_sadenom, v))
    agg['PRODCD']   = agg['PRODUCT'].map(lambda v: apply_format(f_saprod, v))

    # SAS-friendly dtypes
    agg['NOACCT']  = agg['NOACCT'].astype('int64')
    agg['AVGACCT'] = agg['AVGACCT'].astype('int64')
    agg['ACCYTD']  = agg['ACCYTD'].astype('int64')
    agg['REPTDATE'] = agg['REPTDATE'].astype('int64')

    con.close()
    gc.collect()
    return agg


# ---------------------------------------------------------------------------
# Write ONLY SAS7BDAT via saspy
# ---------------------------------------------------------------------------
def write_sas_only(df: pd.DataFrame, table_name: str, out_dir: Path, libref='MISOUT'):
    import saspy

    print("Connecting to SAS via saspy ...")
    sas = saspy.SASsession(cfgname='default')

    sas.saslib(libref, path=str(out_dir))

    sas.submit(f"""
        proc datasets lib={libref} nolist;
            delete {table_name};
        quit;
    """)

    print(f"Writing SAS dataset {libref}.{table_name} ...")
    sas.df2sd(df, table=table_name, libref=libref)

    target = out_dir / f"{table_name}.sas7bdat"
    if target.exists():
        print(f"SAS dataset written: {target}  ({target.stat().st_size:,} bytes)")
    else:
        print(f"WARNING: expected file not found: {target}")

    sas.endsas()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
print("=" * 80)
print("EIBDAWSA - Average Savings Account Analysis")
print("=" * 80)

# Report date = yesterday (T-1) — matches SAS DPTRBL @106 report date
reptdate = (datetime.now() - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
day = reptdate.day
week = '1' if 1 <= day <= 8 else '2' if 9 <= day <= 15 else '3' if 16 <= day <= 22 else '4'

print(f"Report Date : {reptdate.strftime('%d/%m/%Y')}  (today - 1)")
print(f"Week        : {week}")
print(f"Input       : {INPUT_FILE}")
print(f"Output dir  : {OUTPUT_DIR}")
print(f"Input size  : {INPUT_FILE.stat().st_size / (1024**3):.2f} GB")

if not INPUT_FILE.exists():
    raise SystemExit(f"ERROR: input parquet not found: {INPUT_FILE}")

try:
    result = process_dptrblgs(INPUT_FILE, reptdate)

    if result.empty:
        print("No savings rows produced — nothing to write.")
    else:
        table_name = f"AWSA{reptdate.month:02d}"
        write_sas_only(result, table_name, OUTPUT_DIR, libref='MISOUT')

        print("\n" + "=" * 80)
        print("SUMMARY")
        print("=" * 80)
        print(f"Total accounts     : {int(result['NOACCT'].sum()):,}")
        print(f"Total balance      : {result['CURBAL'].sum():,.2f}")
        print(f"Total average amt  : {result['AVGAMT'].sum():,.2f}")
        print(f"New accts this year: {int(result['ACCYTD'].sum()):,}")
        print("\nAccounts by product:")
        for prod, cnt in result.groupby('PRODUCT')['NOACCT'].sum().items():
            print(f"  Product {int(prod):>3}: {int(cnt):>10,}")
        print("\nSample rows:")
        print(result.head(5).to_string(index=False))

except MemoryError as e:
    print(f"ERROR: out of memory — {e}")
except Exception as e:
    import traceback
    print(f"ERROR: {e}")
    traceback.print_exc()

print(f"\nCompleted: {OUTPUT_DIR}")
