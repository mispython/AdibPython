#!/usr/bin/env python3
"""
EIIDLOAN - Islamic Daily Loan Movement Report
"""

import duckdb
import re
from pathlib import Path
from datetime import datetime, timedelta
import pyreadstat
import pandas as pd
import tempfile
import saspy


# ---------------------------------------------------------------------------
# TESTING CONTROLS
# ---------------------------------------------------------------------------
TESTING  = True
MAX_ROWS = 1_000_000


# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
DATEFILE_PATH  = Path('/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIDLOAN/DATEFILE')
BRANCH_PATH    = Path('/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIDLOAN/LKP_BRANCH')
LNNOTE_DIR     = Path('/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS')
OUTPUT_DIR     = Path('/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIDLOAN')

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

CHUNK_SIZE = 500_000

TMP_DB_DIR = Path(tempfile.gettempdir()) / 'eiidloan_duckdb'
TMP_DB_DIR.mkdir(parents=True, exist_ok=True)

con = duckdb.connect()
con.execute(f"SET temp_directory = '{TMP_DB_DIR}'")
con.execute("SET preserve_insertion_order = false")


# ---------------------------------------------------------------------------
# 1. Read DATEFILE (flat file)
# ---------------------------------------------------------------------------
print(f"Reading DATEFILE: {DATEFILE_PATH}")
with open(DATEFILE_PATH, 'r') as f:
    first_line = f.readline().rstrip('\n')

extdate      = int(first_line[0:11].strip())
extdate_z11  = f"{extdate:011d}"
reptdate_str = extdate_z11[0:8]
reptdate     = datetime.strptime(reptdate_str, '%m%d%Y')

prevdate = reptdate - timedelta(days=1)
dletdate = reptdate - timedelta(days=3)

reptday = reptdate.day
prevday = prevdate.day
dletday = dletdate.day

yy       = reptdate.year - 1 if (reptdate.month == 1 and reptdate.day == 1) else reptdate.year
reptyear = reptdate.year
prevyear = yy
reptmon  = reptdate.month
rdate    = reptdate.strftime('%d/%m/%Y')
reptdate_num = int(reptdate.strftime('%y%m%d'))

LNDLY_PREVYEAR = Path(
    f'/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIDLOAN/lndly{prevday}.sas7bdat'
)

print(f"Islamic Daily Loan Movement - {rdate}")
print(f"  EXTDATE  : {extdate}")
print(f"  REPTDATE : {reptdate.date()}  (num: {reptdate_num})")
print(f"  PREVDATE : {prevdate.date()}")
print(f"  DLETDATE : {dletdate.date()}")
print(f"  REPTMON  : {reptmon:02d}")
print(f"  REPTDAY  : {reptday:02d}")
print(f"  LNDLY_PREVYEAR : {LNDLY_PREVYEAR}")
if TESTING:
    print(f"  *** TESTING MODE: row cap = {MAX_ROWS:,} per SAS7BDAT input ***")


# ---------------------------------------------------------------------------
# SAS session + libref writability probe
# ---------------------------------------------------------------------------
sas = saspy.SASsession()

_PROBE_LIB = '_probe'   # SAS libref: <= 8 chars, letters/digits/underscore
_probe_path = str(OUTPUT_DIR).replace("'", "''")
_probe = sas.submit(f"libname {_PROBE_LIB} '{_probe_path}';")
_probe_log = _probe.get('LOG', '') if isinstance(_probe, dict) else str(_probe)
if 'ERROR' in _probe_log.upper():
    print("---- LIBREF PROBE FAILED — SAS cannot write to OUTPUT_DIR ----")
    print(_probe_log)
    raise RuntimeError(
        f"SAS process cannot create libref at {OUTPUT_DIR}. "
        f"Check mount/permissions between the Python and SAS subprocesses."
    )
sas.submit(f"libname {_PROBE_LIB} clear;")
print(f"SAS libref probe OK for {OUTPUT_DIR}")


# ---------------------------------------------------------------------------
# Helper: SAS-safe names
# ---------------------------------------------------------------------------
_SAS_MEMBER_RE = re.compile(r'[^A-Za-z0-9_]')

def _sas_member_name(stem: str) -> str:
    name = _SAS_MEMBER_RE.sub('_', stem)
    if not name or not (name[0].isalpha() or name[0] == '_'):
        name = 'ds_' + name
    return name[:32]

def _sanitize_columns(df: pd.DataFrame) -> pd.DataFrame:
    new_cols = []
    for c in df.columns:
        c2 = _SAS_MEMBER_RE.sub('_', str(c))
        if not c2 or not (c2[0].isalpha() or c2[0] == '_'):
            c2 = 'c_' + c2
        new_cols.append(c2[:32])
    df = df.copy()
    df.columns = new_cols
    return df


# ---------------------------------------------------------------------------
# Helper: write a DuckDB table to a SAS7BDAT file via saspy
# ---------------------------------------------------------------------------
def write_sas7bdat(table_name: str, out_path: Path):
    df = _sanitize_columns(con.execute(f"SELECT * FROM {table_name}").df())

    desired_stem = out_path.stem
    sas_ds_name  = _sas_member_name(desired_stem)

    sas_lib      = 'eiidout'
    sas_lib_path = str(out_path.parent).replace("'", "''")
    sas.submit(f"libname {sas_lib} '{sas_lib_path}';")

    try:
        sas.df2sd(df, table=sas_ds_name, libref=sas_lib, replace=True)
    except Exception:
        print(f"---- SAS LOG for {table_name} ----")
        print(sas.lastlog())
        raise
    finally:
        sas.submit(f"libname {sas_lib} clear;")

    if sas_ds_name != desired_stem:
        sanitised_path = out_path.parent / f"{sas_ds_name}.sas7bdat"
        if sanitised_path.exists():
            sanitised_path.replace(out_path)
        else:
            print(f"  WARNING: expected sanitised file {sanitised_path} not found "
                  f"after SAS write of {desired_stem}")

    print(f"  Wrote {out_path} ({len(df):,} rows)")


# ---------------------------------------------------------------------------
# Helper: stream a SAS7BDAT file into a DuckDB table.
# ---------------------------------------------------------------------------
def ingest_sas7bdat_to_table(sas_file: Path, table_name: str, select_cols: list):
    if not sas_file.exists():
        raise FileNotFoundError(f"SAS7BDAT not found: {sas_file}")

    _, meta = pyreadstat.read_sas7bdat(str(sas_file), metadataonly=True)
    total = meta.number_rows
    cols_upper = [c.upper() for c in meta.column_names]
    miss = [c for c in select_cols if c not in cols_upper]
    if miss:
        raise ValueError(f"{sas_file.name}: missing columns {miss}")

    limit = total
    if TESTING and MAX_ROWS is not None:
        limit = min(total, MAX_ROWS)
        print(f"  Ingesting {sas_file.name}: {total:,} rows available "
              f"(TESTING cap -> reading {limit:,})")
    else:
        print(f"  Ingesting {sas_file.name}: {total:,} rows")

    con.execute(f"""
        CREATE OR REPLACE TABLE {table_name} (
            acctno   BIGINT,
            noteno   BIGINT,
            name     VARCHAR,
            balance  DOUBLE,
            loantype BIGINT,
            curbal   DOUBLE,
            branch   BIGINT
        )
    """)

    offset = 0
    chunk_idx = 0
    while offset < limit:
        chunk_idx += 1
        read_n = min(CHUNK_SIZE, limit - offset)
        df_chunk, _ = pyreadstat.read_sas7bdat(
            str(sas_file), row_offset=offset, row_limit=read_n
        )
        if df_chunk.empty:
            break

        df_chunk.columns = [c.upper() for c in df_chunk.columns]
        df_chunk = df_chunk[select_cols].copy()

        if 'PENDBRH' in df_chunk.columns:
            df_chunk = df_chunk.rename(columns={'PENDBRH': 'BRANCH'})

        for c in ['ACCTNO', 'NOTENO', 'BALANCE', 'LOANTYPE', 'CURBAL', 'BRANCH']:
            if c in df_chunk.columns:
                df_chunk[c] = pd.to_numeric(df_chunk[c], errors='coerce')

        df_chunk['ACCTNO']   = df_chunk['ACCTNO'].astype('Int64')
        df_chunk['NOTENO']   = df_chunk['NOTENO'].astype('Int64')
        df_chunk['LOANTYPE'] = df_chunk['LOANTYPE'].astype('Int64')
        df_chunk['BRANCH']   = df_chunk['BRANCH'].astype('Int64')
        df_chunk['BALANCE']  = df_chunk['BALANCE'].astype('float64')
        df_chunk['CURBAL']   = df_chunk['CURBAL'].astype('float64')

        df_chunk = df_chunk.rename(columns={
            'ACCTNO':   'acctno',
            'NOTENO':   'noteno',
            'NAME':     'name',
            'BALANCE':  'balance',
            'LOANTYPE': 'loantype',
            'CURBAL':   'curbal',
            'BRANCH':   'branch',
        })

        con.register(f'_chunk_{table_name}_{chunk_idx}', df_chunk)
        con.execute(f"INSERT INTO {table_name} SELECT * FROM _chunk_{table_name}_{chunk_idx}")
        con.unregister(f'_chunk_{table_name}_{chunk_idx}')

        offset += len(df_chunk)
        print(f"    chunk {chunk_idx}: rows {offset:,} / {limit:,}")
        del df_chunk

    print(f"  Finished {table_name}: {offset:,} rows (cap {limit:,})")


# ---------------------------------------------------------------------------
# 2. Read LNNOTE (today's feed) -> loan_raw  (filename uses REPTDAY)
# ---------------------------------------------------------------------------
lnnote_file = LNNOTE_DIR / f"enrh_ln_note_d{reptday:02d}.sas7bdat"
print(f"Reading LNNOTE (chunked): {lnnote_file}")

REQUIRED_COLS = ['ACCTNO', 'NOTENO', 'NAME', 'BALANCE', 'LOANTYPE', 'CURBAL', 'PENDBRH']
ingest_sas7bdat_to_table(lnnote_file, 'loan_raw', REQUIRED_COLS)


# ---------------------------------------------------------------------------
# 3. Persist LNDLY{reptday}.sas7bdat
# ---------------------------------------------------------------------------
lndly_today_sas = OUTPUT_DIR / f"lndly{reptday:02d}.sas7bdat"
write_sas7bdat('loan_raw', lndly_today_sas)

con.execute("CREATE OR REPLACE VIEW loan AS SELECT * FROM loan_raw")


# ---------------------------------------------------------------------------
# 4. Split
# ---------------------------------------------------------------------------
REVCRE_TYPES = (302, 350, 364, 365, 506, 902, 903, 910, 925, 951)
HPLOAN_TYPES = (128, 130, 380, 381, 700, 705)
revcre_in = ",".join(str(x) for x in REVCRE_TYPES)
hploan_in = ",".join(str(x) for x in HPLOAN_TYPES)

con.execute(f"CREATE OR REPLACE TABLE lnnote AS SELECT * FROM loan WHERE loantype NOT IN ({revcre_in}, {hploan_in})")
con.execute(f"CREATE OR REPLACE TABLE revcre AS SELECT * FROM loan WHERE loantype IN ({revcre_in})")
con.execute(f"CREATE OR REPLACE TABLE hploan AS SELECT * FROM loan WHERE loantype IN ({hploan_in})")


# ---------------------------------------------------------------------------
# 5. Current-day branch summaries
# ---------------------------------------------------------------------------
con.execute("CREATE OR REPLACE TABLE loansum AS SELECT branch, COUNT(*) AS noacct, SUM(balance) AS brlnamt FROM lnnote GROUP BY branch")
con.execute("CREATE OR REPLACE TABLE revsumm AS SELECT branch, COUNT(*) AS revacc, SUM(balance) AS brrvamt FROM revcre GROUP BY branch")
con.execute("CREATE OR REPLACE TABLE hpsumm  AS SELECT branch, COUNT(*) AS hpacc,  SUM(balance) AS brhpamt FROM hploan GROUP BY branch")


# ---------------------------------------------------------------------------
# 6. Previous-day input
# ---------------------------------------------------------------------------
print(f"Reading previous day (SAS7BDAT): {LNDLY_PREVYEAR}")

HAVE_PREV = False
prev_source = None

if LNDLY_PREVYEAR.exists():
    ingest_sas7bdat_to_table(
        LNDLY_PREVYEAR, 'prev_loan',
        ['ACCTNO', 'NOTENO', 'NAME', 'BALANCE', 'LOANTYPE', 'CURBAL', 'PENDBRH']
    )
    HAVE_PREV = True
    prev_source = str(LNDLY_PREVYEAR)
else:
    print("  WARNING: no previous-day SAS7BDAT found — previous-day tables will be empty.")

if HAVE_PREV:
    print(f"  Using previous-day source: {prev_source}")
    con.execute(f"CREATE OR REPLACE TABLE plnnote AS SELECT * FROM prev_loan WHERE loantype NOT IN ({revcre_in}, {hploan_in})")
    con.execute(f"CREATE OR REPLACE TABLE prevcre AS SELECT * FROM prev_loan WHERE loantype IN ({revcre_in})")
    con.execute(f"CREATE OR REPLACE TABLE phploan AS SELECT * FROM prev_loan WHERE loantype IN ({hploan_in})")

    con.execute("CREATE OR REPLACE TABLE ploansum AS SELECT branch, COUNT(*) AS pnoacct, SUM(balance) AS pbrlnamt FROM plnnote GROUP BY branch")
    con.execute("CREATE OR REPLACE TABLE prevsumm AS SELECT branch, COUNT(*) AS prevacc, SUM(balance) AS pbrrvamt FROM prevcre GROUP BY branch")
    con.execute("CREATE OR REPLACE TABLE phpsumm  AS SELECT branch, COUNT(*) AS phpacc,  SUM(balance) AS pbrhpamt FROM phploan GROUP BY branch")
else:
    con.execute("CREATE OR REPLACE TABLE ploansum AS SELECT NULL::BIGINT AS branch, 0::BIGINT AS pnoacct, 0::DOUBLE AS pbrlnamt WHERE FALSE")
    con.execute("CREATE OR REPLACE TABLE prevsumm AS SELECT NULL::BIGINT AS branch, 0::BIGINT AS prevacc, 0::DOUBLE AS pbrrvamt WHERE FALSE")
    con.execute("CREATE OR REPLACE TABLE phpsumm  AS SELECT NULL::BIGINT AS branch, 0::BIGINT AS phpacc,  0::DOUBLE AS pbrhpamt WHERE FALSE")


# ---------------------------------------------------------------------------
# 7. Read LKP_BRANCH
# ---------------------------------------------------------------------------
print(f"Reading LKP_BRANCH: {BRANCH_PATH}")

branch_rows = []
with open(BRANCH_PATH, 'r') as f:
    for line in f:
        line = line.rstrip('\n')
        if not line.strip():
            continue
        branch_s = line[1:4].strip()
        abbrev   = line[5:8].strip()
        brchname = line[11:41].strip()
        if not branch_s:
            continue
        try:
            branch_val = int(branch_s)
        except ValueError:
            continue
        branch_rows.append({'branch': branch_val, 'abbrev': abbrev, 'brchname': brchname})

branch_df = pd.DataFrame(branch_rows, columns=['branch', 'abbrev', 'brchname'])
print(f"  Loaded {len(branch_df)} branch records")

con.register('branch_df', branch_df)
con.execute("CREATE OR REPLACE TABLE branch AS SELECT branch, abbrev, brchname FROM branch_df")


# ---------------------------------------------------------------------------
# 8. MLOAN / MCRED / MHP
# ---------------------------------------------------------------------------
con.execute("""
    CREATE OR REPLACE TABLE mloan AS
    SELECT l.branch, b.abbrev, b.brchname,
        COALESCE(l.brlnamt,  0) AS brlnamt,
        COALESCE(p.pbrlnamt, 0) AS pbrlnamt,
        COALESCE(l.noacct,   0) AS noacct,
        COALESCE(l.brlnamt,  0) - COALESCE(p.pbrlnamt, 0) AS varianln
    FROM loansum l
    LEFT JOIN ploansum p ON l.branch = p.branch
    LEFT JOIN branch   b ON l.branch = b.branch
""")
con.execute("""
    CREATE OR REPLACE TABLE mcred AS
    SELECT r.branch, b.abbrev, b.brchname,
        COALESCE(r.brrvamt,  0) AS brrvamt,
        COALESCE(p.pbrrvamt, 0) AS pbrrvamt,
        COALESCE(r.revacc,   0) AS revacc,
        COALESCE(r.brrvamt,  0) - COALESCE(p.pbrrvamt, 0) AS varianrv
    FROM revsumm r
    LEFT JOIN prevsumm p ON r.branch = p.branch
    LEFT JOIN branch   b ON r.branch = b.branch
""")
con.execute("""
    CREATE OR REPLACE TABLE mhp AS
    SELECT h.branch, b.abbrev, b.brchname,
        COALESCE(h.brhpamt,  0) AS brhpamt,
        COALESCE(p.pbrhpamt, 0) AS pbrhpamt,
        COALESCE(h.hpacc,    0) AS hpacc,
        COALESCE(h.brhpamt,  0) - COALESCE(p.pbrhpamt, 0) AS varianhp
    FROM hpsumm h
    LEFT JOIN phpsumm p ON h.branch = p.branch
    LEFT JOIN branch  b ON h.branch = b.branch
""")

rdate_file = rdate.replace('/', '-')
write_sas7bdat('mloan', OUTPUT_DIR / f"mloan_{rdate_file}.sas7bdat")
write_sas7bdat('mcred', OUTPUT_DIR / f"mcred_{rdate_file}.sas7bdat")
write_sas7bdat('mhp',   OUTPUT_DIR / f"mhp_{rdate_file}.sas7bdat")


# ---------------------------------------------------------------------------
# 9. Movement reports
# ---------------------------------------------------------------------------
if HAVE_PREV:
    con.execute("CREATE OR REPLACE TABLE dloan1  AS SELECT acctno, SUM(balance) AS dltotol FROM lnnote GROUP BY acctno")
    con.execute("CREATE OR REPLACE TABLE dcred1  AS SELECT acctno, SUM(balance) AS drtotol FROM revcre GROUP BY acctno")
    con.execute("CREATE OR REPLACE TABLE dhp1    AS SELECT acctno, SUM(balance) AS dhptotol FROM hploan GROUP BY acctno")
    con.execute("CREATE OR REPLACE TABLE pdloan1 AS SELECT acctno, SUM(balance) AS pdltotol FROM plnnote GROUP BY acctno")
    con.execute("CREATE OR REPLACE TABLE pdcred1 AS SELECT acctno, SUM(balance) AS pdrtotol FROM prevcre GROUP BY acctno")
    con.execute("CREATE OR REPLACE TABLE pdhp1   AS SELECT acctno, SUM(balance) AS pdhptoto FROM phploan GROUP BY acctno")

    con.execute("""
        CREATE OR REPLACE TABLE dmloan_check AS
        SELECT COALESCE(d.acctno, p.acctno) AS acctno,
               COALESCE(d.dltotol, 0) AS dltotol,
               COALESCE(p.pdltotol, 0) AS pdltotol
        FROM dloan1 d FULL OUTER JOIN pdloan1 p ON d.acctno = p.acctno
        WHERE ABS(COALESCE(d.dltotol, 0) - COALESCE(p.pdltotol, 0)) >= 500000
    """)
    con.execute("""
        CREATE OR REPLACE TABLE dmcred_check AS
        SELECT COALESCE(d.acctno, p.acctno) AS acctno,
               COALESCE(d.drtotol, 0) AS drtotol,
               COALESCE(p.pdrtotol, 0) AS pdrtotol
        FROM dcred1 d FULL OUTER JOIN pdcred1 p ON d.acctno = p.acctno
        WHERE ABS(COALESCE(d.drtotol, 0) - COALESCE(p.pdrtotol, 0)) >= 500000
    """)
    con.execute("""
        CREATE OR REPLACE TABLE dmhp_check AS
        SELECT COALESCE(d.acctno, p.acctno) AS acctno,
               COALESCE(d.dhptotol, 0) AS dhptotol,
               COALESCE(p.pdhptoto, 0) AS pdhptoto
        FROM dhp1 d FULL OUTER JOIN pdhp1 p ON d.acctno = p.acctno
        WHERE ABS(COALESCE(d.dhptotol, 0) - COALESCE(p.pdhptoto, 0)) >= 500000
    """)

    con.execute("""
        CREATE OR REPLACE TABLE dmloan AS
        SELECT DISTINCT ON (l.acctno)
            l.acctno, l.name, l.branch, b.abbrev,
            c.dltotol, c.pdltotol, c.dltotol - c.pdltotol AS movement
        FROM lnnote l
        JOIN dmloan_check c ON l.acctno = c.acctno
        LEFT JOIN branch  b ON l.branch = b.branch
        ORDER BY l.acctno, l.noteno
    """)
    con.execute("""
        CREATE OR REPLACE TABLE dmcred AS
        SELECT DISTINCT ON (r.acctno)
            r.acctno, r.name, r.branch, b.abbrev,
            c.drtotol, c.pdrtotol, c.drtotol - c.pdrtotol AS movement
        FROM revcre r
        JOIN dmcred_check c ON r.acctno = c.acctno
        LEFT JOIN branch   b ON r.branch = b.branch
        ORDER BY r.acctno, r.noteno
    """)
    con.execute("""
        CREATE OR REPLACE TABLE dmhp AS
        SELECT DISTINCT ON (h.acctno)
            h.acctno, h.name, h.branch, b.abbrev,
            c.dhptotol, c.pdhptoto, c.dhptotol - c.pdhptoto AS movement
        FROM hploan h
        JOIN dmhp_check c ON h.acctno = c.acctno
        LEFT JOIN branch  b ON h.branch = b.branch
        ORDER BY h.acctno, h.noteno
    """)

    write_sas7bdat('dmloan', OUTPUT_DIR / f"dmloan_{rdate_file}.sas7bdat")
    write_sas7bdat('dmcred', OUTPUT_DIR / f"dmcred_{rdate_file}.sas7bdat")
    write_sas7bdat('dmhp',   OUTPUT_DIR / f"dmhp_{rdate_file}.sas7bdat")

    loan_mvmt = con.execute("SELECT COUNT(*) FROM dmloan").fetchone()[0]
    cred_mvmt = con.execute("SELECT COUNT(*) FROM dmcred").fetchone()[0]
    hp_mvmt   = con.execute("SELECT COUNT(*) FROM dmhp").fetchone()[0]
else:
    print("  Skipping movement reports (no previous-day source found).")
    loan_mvmt = cred_mvmt = hp_mvmt = 0


# ---------------------------------------------------------------------------
# 10. Summary
# ---------------------------------------------------------------------------
print(f"""
Islamic Daily Loan Movement Report Complete
Date: {rdate}

Previous-day source used: {prev_source or 'NONE (movement reports skipped)'}

Customer Movements (>= RM 500K):
  - Term Loans       : {loan_mvmt} accounts
  - Revolving Credit : {cred_mvmt} accounts
  - HP Loans         : {hp_mvmt} accounts

Testing mode: {'ON' if TESTING else 'OFF'} (MAX_ROWS = {MAX_ROWS if TESTING else 'n/a'})
""")

con.close()
sas.endsas()
print(f"Completed: {OUTPUT_DIR}")
