"""
EIBDDEPE - Daily Deposit Position Extract
DuckDB does everything; SAS gets small summaries via SASPy,
plus DYDP via DATA step + INFILE (fast, no PROC IMPORT guessing).

Clean output: no HTML noise from SAS ODS.
"""

import duckdb
import polars as pl
import saspy
from datetime import datetime, timedelta
import os
import re
import time
import warnings

import PBBDPFMT
import PBBLNFMT

# Suppress SASPy's false-alarm "Noticed 'ERROR:' in LOG" warning
warnings.filterwarnings("ignore", message=".*Noticed 'ERROR:'.*")

# ============================================================================
# CONFIGURATION
# ============================================================================

INPUT_FILE = '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIDMELW/DPTRBLGS.parquet'

BASE_OUT  = '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDDEPE'
MIS_PATH  = f'{BASE_OUT}/MIS'
MISQ_PATH = f'{BASE_OUT}/MISQ'
TEMP_PATH = f'{BASE_OUT}/TEMP'

DYDP_CSV = f'{TEMP_PATH}/DYDP_import.csv'

for p in (MIS_PATH, MISQ_PATH, TEMP_PATH):
    os.makedirs(p, exist_ok=True)

SAS_LIB_MIS  = 'MIS'
SAS_LIB_MISQ = 'MISQ'
SAS_LIB_TEMP = 'TEMP'

ACE_PRODUCTS = sorted(PBBDPFMT.ACE)
AGELIMIT, MAXAGE, AGEBELOW = 12, 18, 11

# ============================================================================
# SAS SESSION
# ============================================================================

print("Connecting to SAS...")
sas = saspy.SASsession(cfgname='default')
print(f"SAS session: {sas.accessMethod}")   # Access Method only, no full repr

# Silence ODS HTML output so LST returns plain text
sas.submit("""
    ods html close;
    ods listing;
    options nonumber nodate;
""")

for lib, path in [(SAS_LIB_MIS, MIS_PATH),
                  (SAS_LIB_MISQ, MISQ_PATH),
                  (SAS_LIB_TEMP, TEMP_PATH)]:
    sas.saslib(lib, engine=f'"{path}"')
    print(f"  libname {lib:<5} -> {path}")

# Verify libnames (using TABLE, not LST)
rc = sas.submit("""
    proc sql;
      select libname, path from dictionary.libnames
      where libname in ('MIS','MISQ','TEMP');
    quit;
""")
tbl = rc.get('TABLE')
if tbl is not None and len(tbl) > 0:
    for _, row in tbl.iterrows():
        print(f"  ✓ {row['libname']:<5} -> {row['path']}")

# ============================================================================
# HELPERS
# ============================================================================

def parse_date(val):
    if not val: return None
    s = re.sub(r'[^0-9]', '', str(int(val)) if isinstance(val, (int, float)) else str(val))
    if len(s) >= 8:
        try: return datetime.strptime(s[:8], '%Y%m%d').date()
        except: pass
    return None

def days_in_month(year, month):
    if month == 2: return 29 if year % 4 == 0 else 28
    elif month in [4, 6, 9, 11]: return 30
    else: return 31

def sas_scalar(sql):
    """Submit SQL and return the first cell of first row (no HTML printed)."""
    rc = sas.submit(sql.rstrip(';') + '; quit;')
    tbl = rc.get('TABLE')
    if tbl is not None and len(tbl) > 0:
        return tbl.iloc[0, 0]
    return None

# ============================================================================
# LOOKUP TABLES
# ============================================================================

def _rules_to_pairs(format_name, library):
    rules = library.FORMATS[format_name]['rules']
    return [(lo, label) for lo, hi, *_rest, label, _opts in rules
            if isinstance(lo, int) and isinstance(hi, int) and lo == hi]

SAPROD_DF  = pl.DataFrame(_rules_to_pairs('SAPROD',  PBBDPFMT),
                          schema={'PRODUCT': pl.Int64, 'SAPROD':  pl.String}, orient='row')
CAPROD_DF  = pl.DataFrame(_rules_to_pairs('CAPROD',  PBBDPFMT),
                          schema={'PRODUCT': pl.Int64, 'CAPROD':  pl.String}, orient='row')
FDPRODD_DF = pl.DataFrame(_rules_to_pairs('FDPRODD', PBBDPFMT),
                          schema={'PRODUCT': pl.Int64, 'FDPRODD': pl.String}, orient='row')
ODPROD_DF  = pl.DataFrame(_rules_to_pairs('ODPROD',  PBBLNFMT),
                          schema={'PRODUCT': pl.Int64, 'ODPROD':  pl.String}, orient='row')

# ============================================================================
# PHASE 1: DuckDB
# ============================================================================

print("\n" + "=" * 70)
print("PHASE 1: DuckDB reads and enriches")
print("=" * 70)

t0 = time.time()
con = duckdb.connect()
con.execute("PRAGMA threads=8;")
con.execute("PRAGMA memory_limit='8GB';")
con.execute(f"PRAGMA temp_directory='{TEMP_PATH}';")

con.register('saprod_lkp',  SAPROD_DF.to_arrow())
con.register('caprod_lkp',  CAPROD_DF.to_arrow())
con.register('fdprodd_lkp', FDPRODD_DF.to_arrow())
con.register('odprod_lkp',  ODPROD_DF.to_arrow())

rd = con.execute(f"SELECT * FROM read_parquet('{INPUT_FILE}') LIMIT 1").pl()
reptdate = None
for col in ['TBDATE', 'REPTDATE', 'REPTDT']:
    if col in rd.columns:
        reptdate = parse_date(rd.select(col).row(0)[0])
        if reptdate: break
if not reptdate: reptdate = datetime.now().date()

reptyear, reptmon, reptday = reptdate.year, reptdate.month, reptdate.day
nowk = str((reptday - 1) // 8 + 1) if reptday <= 24 else '4'
reptdat3 = reptdate - timedelta(days=31)
mth = days_in_month(reptyear, reptmon)
is_weekly_or_monthend = (reptday in [8, 15, 22]) or (reptday == mth)

print(f"  Report date: {reptdate.strftime('%d/%m/%Y')} (Week {nowk})")
print(f"  Weekly/Month-end: {is_weekly_or_monthend}")

ACE_LIST = ','.join(str(p) for p in ACE_PRODUCTS)

enrich_sql = f"""
CREATE OR REPLACE TEMP VIEW enriched AS
WITH base AS (
    SELECT *,
        (CREDIT - DEBIT) AS MOVEMENT,
        CURBAL AS DYDPBAL,
        DATE '{reptdate.isoformat()}' AS REPTDATE,
        CASE WHEN BRANCH = 132 THEN 168 ELSE BRANCH END AS BRANCH2
    FROM read_parquet('{INPUT_FILE}')
    WHERE BANKNO = 33 AND REPTNO = 1001 AND FMTCODE = 1
      AND OPENIND NOT IN ('B','C','P')
      AND PRODUCT NOT IN (297, 298)
),
with_year AS (
    SELECT *,
        CASE
            WHEN OPENDT IS NULL OR OPENDT = 0 THEN NULL
            ELSE CAST(SUBSTR(REGEXP_REPLACE(CAST(OPENDT AS VARCHAR), '[^0-9]', '', 'g'), 1, 8) AS INTEGER) / 10000
        END AS OPEN_YEAR
    FROM base
),
with_acc AS (
    SELECT *,
        CASE WHEN OPEN_YEAR = {reptyear} AND CLOSEDT = 0 THEN 1 ELSE 0 END AS ACCYTD
    FROM with_year
),
with_prod AS (
    SELECT b.*, s.SAPROD, c.CAPROD, f.FDPRODD, o.ODPROD
    FROM with_acc b
    LEFT JOIN saprod_lkp  s ON b.PRODUCT = s.PRODUCT
    LEFT JOIN caprod_lkp  c ON b.PRODUCT = c.PRODUCT
    LEFT JOIN fdprodd_lkp f ON b.PRODUCT = f.PRODUCT
    LEFT JOIN odprod_lkp  o ON b.PRODUCT = o.PRODUCT
),
with_prodcd AS (
    SELECT *,
        CASE
            WHEN DEPTYPE = 'S' THEN COALESCE(SAPROD, '42120')
            WHEN DEPTYPE IN ('D','N') AND CURBAL >= 0 THEN COALESCE(CAPROD, '42110')
            WHEN DEPTYPE IN ('D','N') AND CURBAL <  0 THEN COALESCE(ODPROD, '34180')
            WHEN DEPTYPE = 'C' THEN COALESCE(FDPRODD, '42130')
            ELSE ''
        END AS PRODCD,
        CASE WHEN DEPTYPE = 'S' THEN 'S' ELSE 'D' END AS DPTYPE
    FROM with_prod
),
with_mvrange AS (
    SELECT *,
        CASE
            WHEN PRODUCT IN ({ACE_LIST}) THEN
                CASE
                    WHEN ABS(MOVEMENT) <= 5000 THEN 5000
                    WHEN ABS(MOVEMENT) <= 10000 THEN 10000
                    WHEN ABS(MOVEMENT) <= 30000 THEN 30000
                    WHEN ABS(MOVEMENT) <= 50000 THEN 50000
                    WHEN ABS(MOVEMENT) <= 75000 THEN 75000
                    WHEN ABS(MOVEMENT) <= 100000 THEN 100000
                    ELSE 200000
                END
            ELSE
                CASE
                    WHEN ABS(MOVEMENT) <= 5000 THEN 5000
                    WHEN ABS(MOVEMENT) <= 10000 THEN 10000
                    WHEN ABS(MOVEMENT) <= 30000 THEN 30000
                    WHEN ABS(MOVEMENT) <= 50000 THEN 50000
                    WHEN ABS(MOVEMENT) <= 75000 THEN 75000
                    ELSE 80000
                END
        END AS MVRANGE
    FROM with_prodcd
)
SELECT
    REPTDATE, BRANCH2 AS BRANCH, ACCTNO, NAME, DEBIT, CREDIT, DEPTYPE,
    DPTYPE, PRODUCT, PRODCD, CAPROD, ODPROD,
    CURBAL, DYDPBAL, APPRLIMT, ACCYTD, OPENDT,
    MOVEMENT, MVRANGE, CUSTCODE, SECOND
FROM with_mvrange
"""
con.execute(enrich_sql)
print(f"  Enriched view created  ({time.time()-t0:.1f}s)")

# ---- DYDP CSV ----
print("\n" + "=" * 70)
print("PHASE 1A: Export DYDP to CSV via DuckDB")
print("=" * 70)
t0 = time.time()
con.execute(f"""
COPY (
    SELECT REPTDATE, BRANCH, ACCTNO, NAME, DEBIT, CREDIT, DEPTYPE,
           DPTYPE, PRODUCT, PRODCD, CURBAL, DYDPBAL, APPRLIMT,
           ACCYTD, OPENDT, MOVEMENT, MVRANGE, CUSTCODE, SECOND
    FROM enriched
    WHERE (DEPTYPE = 'S' AND CURBAL >= 0 AND PRODCD IN ('42120','42320'))
       OR (DEPTYPE IN ('D','N') AND CURBAL >= 0 AND PRODCD IN ('42110','42180','42310'))
) TO '{DYDP_CSV}' (HEADER, DELIMITER ',')
""")
print(f"  ✓ CSV: {DYDP_CSV}  ({time.time()-t0:.1f}s, {os.path.getsize(DYDP_CSV)/1e6:.1f} MB)")

# ---- Summaries ----
print("\n" + "=" * 70)
print("PHASE 1B: Summary aggregates in DuckDB")
print("=" * 70)
t0 = time.time()

ddmv_pl = con.execute("""
    SELECT REPTDATE, BRANCH, ACCTNO, NAME, DEBIT, CREDIT, DEPTYPE,
           CAPROD AS PRODCD, PRODUCT, CURBAL
    FROM enriched
    WHERE DEPTYPE IN ('D','N') AND CURBAL < 0 AND CAPROD IN ('42110','42310')
""").pl()

dyposn_pl = con.execute(f"""
    SELECT DATE '{reptdate.isoformat()}' AS REPTDATE,
        COALESCE(SUM(CASE WHEN DEPTYPE='S' AND PRODCD='42120' AND CURBAL>=0 THEN CURBAL END), 0) AS TOTSAVG,
        COALESCE(SUM(CASE WHEN DEPTYPE='S' AND PRODCD='42320' AND CURBAL>=0 THEN CURBAL END), 0) AS TOTSAVGI,
        COALESCE(SUM(CASE WHEN DEPTYPE IN ('D','N') AND CURBAL>=0 AND PRODCD IN ('42110','42180') THEN CURBAL END), 0) AS TOTDMND,
        COALESCE(SUM(CASE WHEN DEPTYPE IN ('D','N') AND CURBAL>=0 AND PRODCD='42310' THEN CURBAL END), 0) AS TOTDMNDI,
        COALESCE(SUM(CASE WHEN DEPTYPE IN ('D','N') AND CURBAL>=0 AND PRODUCT=105 THEN CURBAL END), 0) AS TOTVOSF,
        COALESCE(SUM(CASE WHEN DEPTYPE IN ('D','N') AND CURBAL>=0 AND PRODUCT=104 THEN CURBAL END), 0) AS TOTVOSC,
        COALESCE(SUM(CASE WHEN DEPTYPE IN ('D','N') AND CURBAL<0 AND PRODUCT=105 THEN CURBAL END), 0) AS OVDVOSF,
        COALESCE(SUM(CASE WHEN DEPTYPE IN ('D','N') AND CURBAL<0 AND PRODUCT=104 THEN CURBAL END), 0) AS OVDVOSC,
        COALESCE(SUM(CASE WHEN DEPTYPE IN ('D','N') AND CURBAL<0 AND ODPROD IN ('34180','34240','34380') THEN ABS(CURBAL) END), 0) AS TOTOVFT,
        COALESCE(SUM(CASE WHEN DEPTYPE='C' AND PRODCD='42130' THEN CURBAL END), 0) AS TOTFD,
        COALESCE(SUM(CASE WHEN DEPTYPE='C' AND PRODCD='42132' THEN CURBAL END), 0) AS TOTFDI,
        COALESCE(SUM(CASE WHEN DEPTYPE='S' AND CURBAL>=0 AND PRODUCT IN ({ACE_LIST}) THEN CURBAL END), 0) AS ACESA,
        COALESCE(SUM(CASE WHEN DEPTYPE IN ('D','N') AND CURBAL>=0 AND PRODUCT IN ({ACE_LIST}) THEN CURBAL END), 0) AS ACECA,
        COALESCE(SUM(CASE WHEN DEPTYPE='S' AND PRODCD='42320' AND PRODUCT=214 AND CURBAL>=0 THEN CURBAL END), 0) AS TOTMBSA
    FROM enriched
""").pl()

dyibu_pl = con.execute("""
    SELECT REPTDATE, BRANCH,
        COALESCE(SUM(CASE WHEN DEPTYPE='S' AND PRODCD='42320' THEN CURBAL END), 0) AS SAI,
        COALESCE(SUM(CASE WHEN DEPTYPE IN ('D','N') AND PRODCD='42310' THEN CURBAL END), 0) AS CAI,
        COALESCE(SUM(CASE WHEN DEPTYPE IN ('D','N') AND PRODCD='42310' AND PRODUCT IN (61,161) THEN CURBAL END), 0) AS CAIG,
        COALESCE(SUM(CASE WHEN DEPTYPE IN ('D','N') AND PRODCD='42180' AND PRODUCT IN (63,163) THEN CURBAL END), 0) AS CAIH,
        COALESCE(SUM(CASE WHEN DEPTYPE='S' AND PRODCD='42320' AND PRODUCT=214 THEN CURBAL END), 0) AS MBS,
        SUM(CASE WHEN DEPTYPE='S' AND PRODCD='42320' THEN 1 ELSE 0 END) AS SNO,
        SUM(CASE WHEN DEPTYPE IN ('D','N') AND PRODCD='42310' THEN 1 ELSE 0 END) AS CNO,
        SUM(CASE WHEN DEPTYPE IN ('D','N') AND PRODCD='42310' AND PRODUCT IN (61,161) THEN 1 ELSE 0 END) AS CGNO,
        SUM(CASE WHEN DEPTYPE IN ('D','N') AND PRODCD='42180' AND PRODUCT IN (63,163) THEN 1 ELSE 0 END) AS CHNO,
        SUM(CASE WHEN DEPTYPE='S' AND PRODCD='42320' AND PRODUCT=214 THEN 1 ELSE 0 END) AS MBSNO
    FROM enriched
    GROUP BY REPTDATE, BRANCH
""").pl()

dymvnt_pl = con.execute(f"""
    SELECT REPTDATE, BRANCH, ACCTNO, NAME, DEBIT, CREDIT, DEPTYPE,
           PRODUCT, CURBAL, APPRLIMT, CUSTCODE, SECOND
    FROM enriched
    WHERE (DEPTYPE = 'S' AND ABS(MOVEMENT) >= 50000)
       OR (DEPTYPE IN ('D','N') AND DPTYPE = 'D' AND (
              (ABS(MOVEMENT) >= 100000 AND PRODUCT IN ({ACE_LIST}))
           OR (ABS(MOVEMENT) >= 1000000)))
""").pl()

dybrdp_pl = con.execute("""
    SELECT BRANCH, DPTYPE, REPTDATE, SUM(DYDPBAL) AS BALANCE
    FROM enriched
    WHERE PRODCD NOT IN ('42320','42310') AND PRODUCT NOT IN (104, 105)
    GROUP BY BRANCH, DPTYPE, REPTDATE
""").pl()

dyddcr_pl = con.execute(f"""
    WITH src AS (
        SELECT REPTDATE,
            CASE WHEN CURBAL - (CREDIT - DEBIT) < 0 THEN CURBAL
                 ELSE CURBAL - (CURBAL - (CREDIT - DEBIT))
            END AS MOVEMENT_ADJ
        FROM enriched
        WHERE DPTYPE = 'D'
          AND PRODUCT NOT IN ({ACE_LIST})
          AND PRODCD <> '42310' AND PRODUCT <> 72 AND CURBAL >= 0
    )
    SELECT REPTDATE,
        CASE
            WHEN ABS(MOVEMENT_ADJ) < 300000 THEN 300000
            WHEN ABS(MOVEMENT_ADJ) < 500000 THEN 500000
            WHEN ABS(MOVEMENT_ADJ) < 1000000 THEN 1000000
            WHEN ABS(MOVEMENT_ADJ) < 1500000 THEN 1500000
            WHEN ABS(MOVEMENT_ADJ) < 2000000 THEN 2000000
            WHEN ABS(MOVEMENT_ADJ) < 3000000 THEN 3000000
            WHEN ABS(MOVEMENT_ADJ) < 4000000 THEN 4000000
            WHEN ABS(MOVEMENT_ADJ) < 5000000 THEN 5000000
            WHEN ABS(MOVEMENT_ADJ) < 10000000 THEN 10000000
            ELSE 10000001
        END AS RANGE,
        SUM(MOVEMENT_ADJ) AS MOVEMENT
    FROM src GROUP BY REPTDATE, RANGE
""").pl()

dydps_pl = con.execute(f"""
    SELECT REPTDATE, MVRANGE, SUM(MOVEMENT) AS MOVEMENT
    FROM enriched
    WHERE DPTYPE='S' AND PRODUCT NOT IN ({ACE_LIST}) AND PRODCD='42120'
    GROUP BY REPTDATE, MVRANGE
""").pl()

dyace_pl = con.execute(f"""
    SELECT REPTDATE, MVRANGE, SUM(MOVEMENT) AS MOVEMENT
    FROM enriched
    WHERE DPTYPE='D' AND PRODUCT IN ({ACE_LIST})
    GROUP BY REPTDATE, MVRANGE
""").pl()

if is_weekly_or_monthend:
    sdrnge_pl = con.execute(f"""
        WITH savg AS (
            SELECT BRANCH, PRODUCT, PRODCD,
                CASE WHEN RACE IN ('0','1','2','3') THEN RACE ELSE '0' END AS RACE,
                CASE
                    WHEN BDATE IS NULL OR BDATE = 0 THEN 0
                    ELSE LEAST({MAXAGE}, GREATEST({AGEBELOW},
                        {reptyear} - CAST(SUBSTR(REGEXP_REPLACE(CAST(BDATE AS VARCHAR), '[^0-9]', '', 'g'), 1, 4) AS INTEGER)))
                END AS AGE,
                CASE
                    WHEN CURBAL < 500 THEN 500 WHEN CURBAL < 1000 THEN 1000
                    WHEN CURBAL < 5000 THEN 5000 WHEN CURBAL < 10000 THEN 10000
                    WHEN CURBAL < 20000 THEN 20000 WHEN CURBAL < 50000 THEN 50000
                    WHEN CURBAL < 100000 THEN 100000 WHEN CURBAL < 200000 THEN 200000
                    ELSE 200001
                END AS RANGE,
                CASE
                    WHEN CURBAL < 1000 THEN 1000 WHEN CURBAL < 5000 THEN 5000
                    WHEN CURBAL < 10000 THEN 10000 WHEN CURBAL < 50000 THEN 50000
                    WHEN CURBAL < 100000 THEN 100000 ELSE 100001
                END AS R2NGE,
                CURBAL, ACCYTD
            FROM enriched WHERE DEPTYPE = 'S' AND CURBAL >= 0
        )
        SELECT BRANCH, RACE, PRODCD, PRODUCT, RANGE, AGE, R2NGE,
               COUNT(*) AS NOACCT, SUM(CURBAL) AS CURBAL, SUM(ACCYTD) AS ACCYTD
        FROM savg GROUP BY BRANCH, RACE, PRODCD, PRODUCT, RANGE, AGE, R2NGE
    """).pl()
else:
    sdrnge_pl = pl.DataFrame([])

con.close()
print(f"  ✓ Summaries computed  ({time.time()-t0:.1f}s)")
print(f"    DDMV={len(ddmv_pl):,}  DYIBU={len(dyibu_pl):,}  DYMVNT={len(dymvnt_pl):,}  "
      f"DYBRDP={len(dybrdp_pl):,}  DYDDCR={len(dyddcr_pl):,}  "
      f"DYDPS={len(dydps_pl):,}  DYACE={len(dyace_pl):,}")

# ============================================================================
# PHASE 2: Save
# ============================================================================

print("\n" + "=" * 70)
print("PHASE 2: Writing SAS datasets")
print("=" * 70)

def save_sas(df_polars, libref, table):
    if len(df_polars) == 0:
        print(f"  ⚠ {libref}.{table}: No data"); return
    sas.df2sd(df_polars.to_pandas(), table=table, libref=libref, replace=True)
    print(f"  ✓ {libref}.{table}  ({len(df_polars):,} rows)")

def upsert_sas(df_new, libref, table):
    if len(df_new) == 0:
        print(f"  ⚠ {libref}.{table}: No data"); return
    pdf = df_new.to_pandas()
    if reptday == 1:
        sas.df2sd(pdf, table=table, libref=libref, replace=True)
        print(f"  ✓ {libref}.{table} (new)"); return
    if not sas.exist(table, libref=libref):
        sas.df2sd(pdf, table=table, libref=libref)
        print(f"  ✓ {libref}.{table} (created)"); return
    dt_str = reptdate.strftime('%d%b%Y').upper()
    sas.submit(f"""
        data {libref}.{table}; set {libref}.{table};
          if REPTDATE = '{dt_str}'d then delete; run;
    """)
    sas.df2sd(pdf, table=table, libref=libref, replace=False)
    print(f"  ✓ {libref}.{table} (appended)")

def append_ddmv(df_new):
    if len(df_new) == 0:
        print("  ⚠ MISQ.DDMV: No data"); return
    pdf = df_new.to_pandas()
    if not sas.exist('DDMV', libref=SAS_LIB_MISQ):
        sas.df2sd(pdf, table='DDMV', libref=SAS_LIB_MISQ)
        print(f"  ✓ MISQ.DDMV (created, {len(df_new):,})"); return
    dt_str = reptdat3.strftime('%d%b%Y').upper()
    sas.submit(f"""
        data {SAS_LIB_MISQ}.DDMV; set {SAS_LIB_MISQ}.DDMV;
          if REPTDATE = '{dt_str}'d then delete; run;
    """)
    sas.df2sd(pdf, table='DDMV', libref=SAS_LIB_MISQ, replace=False)
    print(f"  ✓ MISQ.DDMV (appended, {len(df_new):,})")

# ---------------------------------------------------------------------------
# DYDP import via DATA step + INFILE (fast, explicit types)
# ---------------------------------------------------------------------------
def import_dydp_via_datastep(csv_path, libref, table):
    print(f"  Reading {csv_path}")
    print(f"  Size: {os.path.getsize(csv_path)/1e6:.1f} MB")

    t0 = time.time()
    rc = sas.submit(f"""
        data {libref}.{table};
          length REPTDATE 8 BRANCH 8 ACCTNO 8 NAME $15 DEBIT 8 CREDIT 8
                 DEPTYPE $1 DPTYPE $1 PRODUCT 8 PRODCD $8 CURBAL 8
                 DYDPBAL 8 APPRLIMT 8 ACCYTD 8 OPENDT 8 MOVEMENT 8
                 MVRANGE 8 CUSTCODE 8 SECOND 8;
          infile "{csv_path}" dlm=',' dsd firstobs=2 lrecl=1000 missover;
          input REPTDATE BRANCH ACCTNO NAME $ DEBIT CREDIT DEPTYPE $
                DPTYPE $ PRODUCT PRODCD $ CURBAL DYDPBAL APPRLIMT
                ACCYTD OPENDT MOVEMENT MVRANGE CUSTCODE SECOND;
          format REPTDATE yymmdd10.;
        run;
    """)
    elapsed = time.time() - t0
    print(f"  ✓ DATA step import: {elapsed:.1f}s")

    # Show only error lines (no HTML)
    log = rc.get('LOG', '')
    for line in log.splitlines():
        if 'ERROR' in line.upper() or 'WARNING' in line.upper():
            print(f"  ⚠ {line.strip()}")

    # Row count
    n = sas_scalar(f"proc sql; select count(*) as N from {libref}.{table}")
    if n is not None:
        print(f"  ✓ {libref}.{table} has {int(n):,} rows")
    return elapsed

print(f"\nImporting DYDP from {DYDP_CSV} via DATA step...")
sas.submit(f"proc datasets lib={SAS_LIB_TEMP} nolist; delete DYDP; run;")
import_dydp_via_datastep(DYDP_CSV, SAS_LIB_TEMP, 'DYDP')

try:
    os.remove(DYDP_CSV)
    print(f"  ✓ removed scratch CSV")
except OSError as e:
    print(f"  ⚠ {e}")

# All summaries
save_sas(dyposn_pl, SAS_LIB_MIS, f'DYPOSN{reptmon:02d}')
upsert_sas(dymvnt_pl, SAS_LIB_MIS, f'DYMVNT{reptmon:02d}')
upsert_sas(dyibu_pl,  SAS_LIB_MIS, f'DYIBU{reptmon:02d}')
upsert_sas(dyddcr_pl, SAS_LIB_MIS, f'DYDDCR{reptmon:02d}')
upsert_sas(dybrdp_pl, SAS_LIB_MIS, f'DYBRDP{reptmon:02d}')
upsert_sas(dydps_pl,  SAS_LIB_MIS, f'DYDPS{reptmon:02d}')
upsert_sas(dyace_pl,  SAS_LIB_MIS, f'DYACE{reptmon:02d}')

if is_weekly_or_monthend and len(sdrnge_pl) > 0:
    save_sas(sdrnge_pl, SAS_LIB_MIS, f'SDRNGE{reptmon:02d}')

append_ddmv(ddmv_pl)

# ============================================================================
# SUMMARY
# ============================================================================

print("\n" + "=" * 70)
print("✓ EIBDDEPE Complete")
print("=" * 70)
print(f"Report Date : {reptdate.strftime('%d/%m/%Y')} (Week {nowk})")
print(f"DDMV dedupe : {reptdat3.strftime('%d/%m/%Y')}")
print(f"Output dirs : MIS={MIS_PATH}")
print(f"              MISQ={MISQ_PATH}")
print(f"              TEMP={TEMP_PATH}")
print(f"Records     : DYMVNT={len(dymvnt_pl):,}  DYIBU={len(dyibu_pl):,}  "
      f"DYBRDP={len(dybrdp_pl):,}  DYDDCR={len(dyddcr_pl):,}  "
      f"DYDPS={len(dydps_pl):,}  DYACE={len(dyace_pl):,}  "
      f"DDMV={len(ddmv_pl):,}"
      + (f"  SDRNGE={len(sdrnge_pl):,}" if is_weekly_or_monthend else ""))
print("=" * 70)

sas.endsas()
