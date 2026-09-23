# -*- coding: utf-8 -*-
"""
EIBDLNS2 - Branch Daily Outstanding Loan Summary Report
Fast multiprocessing version:
  - np.memmap reads (no full-file copy)
  - vectorised COMP-3 + EBCDIC decoding
  - per-worker streaming ParquetWriter (snappy)
  - RAM-aware worker & chunk sizing
  - unused columns dropped
"""

import os
import sys
import math
import duckdb
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyreadstat
from datetime import timedelta
from pathlib import Path
from multiprocessing import Pool, cpu_count
import saspy

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
OUTPUT_DIR = Path("/stgsrcsys/host/holding")
MIS_DIR    = Path("/stgsrcsys/host/uat/python/loans/")

DATEFILE = Path("/host_pq/dwh/input/LOAN/DATEFILE")
FEEFILE  = "/host_pq/dwh/input/LOAN/NFEEFILE_{reptyear}{reptmon}{reptday}"
ACCTFILE = "/host_pq/dwh/input/LOAN/ACCTFILE_{reptyear}{reptmon}{reptday}"
BRANCHF  = Path("/sasdata/rawdata/lookup/LKP_BRANCH")

FEE_LRECL  = 300
ACCT_LRECL = 4000

# ---------------------------------------------------------------------------
# Sizing — RAM-aware, safe defaults
# ---------------------------------------------------------------------------
MAX_WORKERS      = 8        # hard cap (change to taste)
MAX_CHUNK_MB     = 512      # hard cap per worker, MB
SAFETY_FACTOR    = 4        # numpy / arrow temporaries
RAM_HEADROOM     = 0.50     # use at most 50% of free RAM


def _free_ram_bytes() -> int:
    """Best-effort free RAM in bytes."""
    try:
        import psutil
        return psutil.virtual_memory().available
    except Exception:
        pass
    try:
        return os.sysconf("SC_AVPHYS_PAGES") * os.sysconf("SC_PAGE_SIZE")
    except Exception:
        # very conservative fallback: assume 8 GB free
        return 8 * 1024 ** 3


def plan_workers_and_chunk(record_len: int):
    """Return (n_workers, chunk_records) sized to free RAM."""
    free = _free_ram_bytes()
    budget = int(free * RAM_HEADROOM)

    n_workers = max(1, min(MAX_WORKERS, cpu_count()))
    # per-worker budget
    per_worker_bytes = budget // n_workers

    # chunk size in records, clamped by MAX_CHUNK_MB
    max_chunk_bytes = MAX_CHUNK_MB * 1024 * 1024
    usable = min(per_worker_bytes, max_chunk_bytes)

    chunk_records = max(10_000, usable // (record_len * SAFETY_FACTOR))
    # round to nearest 10k for tidiness
    chunk_records = int(chunk_records // 10_000 * 10_000)

    return n_workers, chunk_records


N_WORKERS_ACCT, ACCT_CHUNK = plan_workers_and_chunk(ACCT_LRECL)
N_WORKERS_FEE,  FEE_CHUNK  = plan_workers_and_chunk(FEE_LRECL)

print(f"Plan: ACCT workers={N_WORKERS_ACCT}, chunk={ACCT_CHUNK:,} records "
      f"({ACCT_CHUNK*ACCT_LRECL/1024/1024:.0f} MB/chunk)")
print(f"Plan: FEE  workers={N_WORKERS_FEE},  chunk={FEE_CHUNK:,} records "
      f"({FEE_CHUNK*FEE_LRECL/1024/1024:.0f} MB/chunk)")

# ---------------------------------------------------------------------------
# DuckDB
# ---------------------------------------------------------------------------
con = duckdb.connect(":memory:")
print("REPORT ID : EIBDLNSA")

# ===========================================================================
# STEP 1 - Report date
# ===========================================================================
with open(DATEFILE, "r") as f:
    first_line = f.readline()

EXTDATE_str = first_line[0:11].strip().zfill(11)
extdate_str = EXTDATE_str[:8]
EXTDATE     = int(EXTDATE_str)

REPTDATE = con.execute(
    f"SELECT CAST(strptime('{extdate_str}', '%m%d%Y') AS DATE)"
).fetchone()[0]

PREVDATE = REPTDATE - timedelta(days=1)
REPTDAY, PREVDAY = REPTDATE.day, PREVDATE.day
YY = REPTDATE.year - 1 if (REPTDATE.month == 1 and REPTDATE.day == 1) else REPTDATE.year

REPTYEAR     = str(REPTDATE.year)
REPTMON      = str(REPTDATE.month).zfill(2)
REPTDAY_STR  = str(REPTDAY).zfill(2)
PREVDAY_STR  = str(PREVDAY).zfill(2)
RDATE        = REPTDATE.strftime("%d%m%Y")
REPTDATE_INT = int(REPTDATE.strftime("%y%m%d"))

print(f"Report Date: {REPTDATE.strftime('%d/%m/%Y')}")
print(f"Prev   Date: {PREVDATE.strftime('%d/%m/%Y')}")

feefile_path  = FEEFILE.format(reptyear=REPTYEAR, reptmon=REPTMON, reptday=REPTDAY_STR)
acctfile_path = ACCTFILE.format(reptyear=REPTYEAR, reptmon=REPTMON, reptday=REPTDAY_STR)


# ===========================================================================
# EBCDIC -> ASCII translation table (built once, inherited by workers)
# ===========================================================================
def _build_ebc2ascii() -> np.ndarray:
    tbl = bytearray(256)
    for b in range(256):
        try:
            ch = bytes([b]).decode("cp037")
            enc = ch.encode("ascii", "replace")
            tbl[b] = enc[0] if len(enc) == 1 else 0x3F   # '?'
        except Exception:
            tbl[b] = 0x3F
    return np.frombuffer(bytes(tbl), dtype=np.uint8)


EBC2ASCII = _build_ebc2ascii()          # module-level, inherited via fork


# ===========================================================================
# Vectorised decoders
# ===========================================================================
def pd_decode(pd_bytes: np.ndarray, decimals: int = 0) -> np.ndarray:
    """COMP-3 packed decimal -> float64 array."""
    if pd_bytes.size == 0:
        return np.zeros(0, dtype=np.float64)
    high = (pd_bytes >> 4) & 0x0F
    low  = pd_bytes & 0x0F
    n = pd_bytes.shape[0]
    L = pd_bytes.shape[1]
    digits = np.empty((n, 2 * L - 1), dtype=np.uint8)
    digits[:, 0::2] = high
    digits[:, 1::2] = low[:, :-1]
    powers = (10 ** np.arange(digits.shape[1] - 1, -1, -1, dtype=np.int64))
    values = (digits.astype(np.int64) * powers).sum(axis=1)
    sign = low[:, -1]
    neg = (sign == 0x0D) | (sign == 0x0B)
    values = np.where(neg, -values, values)
    if decimals:
        values = values / (10 ** decimals)
    return values.astype(np.float64)


def ebcdic_decode(col: np.ndarray) -> np.ndarray:
    """(N, L) EBCDIC bytes -> numpy array of Python strings, rstrip'd."""
    if col.size == 0:
        return np.zeros(0, dtype=object)
    L = col.shape[1]
    ascii_bytes = EBC2ASCII[col]                # (N, L) uint8 of ASCII
    packed = ascii_bytes.tobytes()
    arr = np.frombuffer(packed, dtype=f"S{L}").astype(str)
    # remove trailing spaces
    return np.char.rstrip(arr)


# ===========================================================================
# Worker: decode one byte-range of a fixed-block file into a Parquet part
# ===========================================================================
def _decode_acct_part(args):
    path, rec_start, n_records, part_path = args
    ACCT_LRECL = 4000

    # open read-only memmap (inherited per worker)
    mm = np.memmap(path, dtype=np.uint8, mode="r")
    start_byte = rec_start * ACCT_LRECL
    nbytes = n_records * ACCT_LRECL
    arr = mm[start_byte:start_byte + nbytes].reshape(n_records, ACCT_LRECL)

    writer = None
    CHUNK = 50_000     # records per internal write; keep small
    for off in range(0, n_records, CHUNK):
        chunk = arr[off:off + CHUNK]

        acctno   = pd_decode(chunk[:, 0:6],   0).astype(np.int64)
        loantype = pd_decode(chunk[:, 84:86], 0).astype(np.int32)
        mask = (loantype == 135) | (loantype == 136)
        if not mask.any():
            continue

        a = chunk[mask]
        tbl = pa.table({
            "acctno":   pa.array(acctno[mask],                   type=pa.int64()),
            "name":     pa.array(ebcdic_decode(a[:, 6:30]),      type=pa.string()),
            "bankno":   pa.array(pd_decode(a[:, 62:64], 0).astype(np.int32)),
            "accbrch":  pa.array(pd_decode(a[:, 65:69], 0).astype(np.int32)),
            "noteno":   pa.array(pd_decode(a[:, 80:83], 0).astype(np.int64)),
            "reversed": pa.array(ebcdic_decode(a[:, 83:84]),     type=pa.string()),
            "loantype": pa.array(loantype[mask],                 type=pa.int32()),
            "ntbrch":   pa.array(pd_decode(a[:, 86:90], 0).astype(np.int32)),
            "pendbrh":  pa.array(pd_decode(a[:, 90:94], 0).astype(np.int32)),
            "lasttran": pa.array(pd_decode(a[:, 112:118], 0).astype(np.int64)),
            "curbal":   pa.array(pd_decode(a[:, 120:128], 2)),
            "intamt":   pa.array(pd_decode(a[:, 128:136], 2)),
            "paidind":  pa.array(ebcdic_decode(a[:, 260:261]),   type=pa.string()),
            "ntint":    pa.array(ebcdic_decode(a[:, 295:296]),   type=pa.string()),
            "intearn":  pa.array(pd_decode(a[:, 310:318], 2)),
            "accrual":  pa.array(pd_decode(a[:, 318:326], 7)),
            "feeamt":   pa.array(pd_decode(a[:, 456:464], 2)),
        })

        if writer is None:
            writer = pq.ParquetWriter(part_path, tbl.schema,
                                      compression="snappy")
        writer.write_table(tbl)

    if writer is not None:
        writer.close()
    del mm
    return part_path, n_records


def _decode_fee_part(args):
    path, rec_start, n_records, part_path = args
    FEE_LRECL = 300

    mm = np.memmap(path, dtype=np.uint8, mode="r")
    start_byte = rec_start * FEE_LRECL
    nbytes = n_records * FEE_LRECL
    arr = mm[start_byte:start_byte + nbytes].reshape(n_records, FEE_LRECL)

    writer = None
    CHUNK = 200_000
    for off in range(0, n_records, CHUNK):
        chunk = arr[off:off + CHUNK]

        acctno   = pd_decode(chunk[:, 0:6],  0).astype(np.int64)
        loantype = pd_decode(chunk[:, 9:11], 0).astype(np.int32)
        feepln   = ebcdic_decode(chunk[:, 21:23])

        mask = (acctno < 3000000000) & \
               ((loantype == 135) | (loantype == 136)) & \
               (feepln == "PA")
        if not mask.any():
            continue

        tbl = pa.table({
            "acctno":   pa.array(acctno[mask],                    type=pa.int64()),
            "noteno":   pa.array(pd_decode(chunk[mask][:, 6:9], 0).astype(np.int64)),
            "loantype": pa.array(loantype[mask],                  type=pa.int32()),
            "feepln":   pa.array(feepln[mask],                    type=pa.string()),
            "feeamta":  pa.array(pd_decode(chunk[mask][:, 34:42], 2)),
            "feeamtc":  pa.array(pd_decode(chunk[mask][:, 66:74], 2)),
            "feeamtb":  pa.array(pd_decode(chunk[mask][:, 74:82], 2)),
        })

        if writer is None:
            writer = pq.ParquetWriter(part_path, tbl.schema,
                                      compression="snappy")
        writer.write_table(tbl)

    if writer is not None:
        writer.close()
    del mm
    return part_path, n_records


# ===========================================================================
# Parallel driver for a fixed-block file
# ===========================================================================
def decode_fb_parallel(path: str, lrecl: int, out_parquet: Path,
                       n_workers: int, chunk_records: int, worker_fn):
    file_size = Path(path).stat().st_size
    n_records_total = file_size // lrecl
    print(f"  {path}")
    print(f"    size = {file_size:,} bytes -> {n_records_total:,} records "
          f"(lrecl={lrecl})")
    print(f"    workers = {n_workers}, chunk = {chunk_records:,} records")

    parts_dir = out_parquet.parent / (out_parquet.stem + "_parts")
    parts_dir.mkdir(parents=True, exist_ok=True)

    # build chunk jobs
    jobs = []
    rec = 0
    idx = 0
    while rec < n_records_total:
        n = min(chunk_records, n_records_total - rec)
        part = parts_dir / f"part_{idx:05d}.parquet"
        jobs.append((path, rec, n, str(part)))
        rec += n
        idx += 1

    print(f"    {len(jobs)} chunk(s) to process")

    with Pool(processes=n_workers) as pool:
        for i, (_, _) in enumerate(pool.imap_unordered(worker_fn, jobs), 1):
            if i % 5 == 0 or i == len(jobs):
                print(f"    ... {i}/{len(jobs)} chunks done")

    # combine part files into one Parquet
    print(f"    combining {len(jobs)} part files ...")
    parts = sorted(parts_dir.glob("part_*.parquet"))
    writer = None
    for p in parts:
        t = pq.read_table(p)
        if writer is None:
            writer = pq.ParquetWriter(out_parquet, t.schema,
                                      compression="snappy")
        writer.write_table(t)
    if writer is not None:
        writer.close()

    # cleanup parts
    for p in parts:
        p.unlink()
    parts_dir.rmdir()

    print(f"    -> {out_parquet}")


# ===========================================================================
# STEP 2 - NFEEFILE
# ===========================================================================
fee_parquet = OUTPUT_DIR / f"NFEEFILE_{REPTYEAR}{REPTMON}{REPTDAY_STR}.parquet"
if fee_parquet.exists():
    print(f"NFEEFILE parquet cached: {fee_parquet}")
else:
    print(f"Decoding NFEEFILE: {feefile_path}")
    decode_fb_parallel(
        path=feefile_path,
        lrecl=FEE_LRECL,
        out_parquet=fee_parquet,
        n_workers=N_WORKERS_FEE,
        chunk_records=FEE_CHUNK,
        worker_fn=_decode_fee_part,
    )


# ===========================================================================
# STEP 3 - ACCTFILE
# ===========================================================================
acct_parquet = OUTPUT_DIR / f"ACCTFILE_{REPTYEAR}{REPTMON}{REPTDAY_STR}.parquet"
if acct_parquet.exists():
    print(f"ACCTFILE parquet cached: {acct_parquet}")
else:
    print(f"Decoding ACCTFILE: {acctfile_path}")
    decode_fb_parallel(
        path=acctfile_path,
        lrecl=ACCT_LRECL,
        out_parquet=acct_parquet,
        n_workers=N_WORKERS_ACCT,
        chunk_records=ACCT_CHUNK,
        worker_fn=_decode_acct_part,
    )


# ===========================================================================
# STEP 4 - LKP_BRANCH
# ===========================================================================
branch_parquet = OUTPUT_DIR / "LKP_BRANCH.parquet"
if branch_parquet.exists():
    print(f"LKP_BRANCH parquet cached: {branch_parquet}")
else:
    branch_rows = []
    with open(BRANCHF, "r", encoding="latin-1", newline="") as f:
        for line in f:
            line = line.rstrip("\r\n")
            if not line or line.startswith("NOTE:") or "The SAS System" in line:
                continue
            bank   = line[0:1]
            branch = line[1:4].strip()
            abbrev = line[5:8].rstrip()
            name   = line[11:41].rstrip()
            if not branch.isdigit():
                continue
            branch_rows.append({"bank": bank, "branch": int(branch),
                                "abbrev": abbrev, "brchname": name})
    df_branch = pd.DataFrame(branch_rows)
    print(f"  {len(df_branch):,} branch rows -> {branch_parquet}")
    pq.write_table(pa.Table.from_pandas(df_branch), branch_parquet)


# ===========================================================================
# STEP 5 - DuckDB processing
# ===========================================================================
con.execute(f"""
    CREATE OR REPLACE TABLE feplan AS
    SELECT acctno, noteno, loantype, feepln, feeamta, feeamtc, feeamtb
    FROM read_parquet('{fee_parquet}')
""")

con.execute("""
    CREATE OR REPLACE TABLE feepo AS
    SELECT acctno, noteno,
           SUM(feeamta) AS feeamta,
           SUM(feeamtb) AS feeamtb,
           SUM(feeamtc) AS feeamtc
    FROM feplan
    GROUP BY acctno, noteno
""")

con.execute(f"""
    CREATE OR REPLACE TABLE loan_raw AS
    SELECT *,
        CASE
            WHEN COALESCE(pendbrh, 0) <> 0 THEN pendbrh
            WHEN COALESCE(ntbrch,  0) <> 0 THEN ntbrch
            ELSE accbrch
        END AS branch
    FROM read_parquet('{acct_parquet}')
    WHERE loantype IN (135, 136)
      AND (
            ( (reversed IS NULL OR reversed <> 'Y')
              AND noteno IS NOT NULL
              AND (paidind IS NULL OR paidind <> 'P') )
            OR
            ( paidind = 'P' AND lasttran = {REPTDATE_INT} )
          )
""")

con.execute("""
    CREATE OR REPLACE TABLE loan AS
    SELECT l.*,
        COALESCE(f.feeamta, 0) AS feeamta_f,
        COALESCE(f.feeamtb, 0) AS feeamtb_f,
        COALESCE(f.feeamtc, 0) AS feeamtc_f,
        CASE
            WHEN l.acctno > 8000000000 AND l.loantype IN (720, 725)
            THEN l.feeamt + COALESCE(f.feeamta, 0) + COALESCE(f.feeamtc, 0)
            ELSE l.feeamt + COALESCE(f.feeamta, 0)
        END AS feeamt_final,
        CASE
            WHEN l.ntint = 'A'
            THEN l.curbal + l.intearn - l.intamt +
                 CASE
                     WHEN l.acctno > 8000000000 AND l.loantype IN (720, 725)
                     THEN l.feeamt + COALESCE(f.feeamta,0) + COALESCE(f.feeamtc,0)
                     ELSE l.feeamt + COALESCE(f.feeamta,0)
                 END
            ELSE l.curbal + l.accrual +
                 CASE
                     WHEN l.acctno > 8000000000 AND l.loantype IN (720, 725)
                     THEN l.feeamt + COALESCE(f.feeamta,0) + COALESCE(f.feeamtc,0)
                     ELSE l.feeamt + COALESCE(f.feeamta,0)
                 END
        END AS balance
    FROM loan_raw l
    LEFT JOIN feepo f
      ON l.acctno = f.acctno AND l.noteno = f.noteno
""")

con.execute(f"""
    CREATE OR REPLACE TABLE loan_summary AS
    SELECT branch,
           {EXTDATE} AS extdate,
           COUNT(*)  AS noacct,
           SUM(balance) AS brlnamt
    FROM loan
    GROUP BY branch, extdate
    ORDER BY branch
""")


# ===========================================================================
# STEP 6 - Cross-day MIS storage
# ===========================================================================
today_df  = con.execute("SELECT * FROM loan_summary").df()
today_sas = MIS_DIR / f"LOAN{REPTDAY_STR}.sas7bdat"
today_pq  = MIS_DIR / f"LOAN{REPTDAY_STR}.parquet"

sas = saspy.SASsession(cfgname="default")
sas.df2sd(today_df, table="MISLOAN_TODAY", libref="WORK")
sas.submit(f"""
    libname misout "{MIS_DIR}";
    data misout.LOAN{REPTDAY_STR};
        set WORK.MISLOAN_TODAY;
    run;
""")
print(f"MIS today SAS7BDAT saved: {today_sas}")

pq.write_table(pa.Table.from_pandas(today_df), today_pq, compression="snappy")
print(f"MIS today Parquet  saved: {today_pq}")

prev_sas = MIS_DIR / f"LOAN{PREVDAY_STR}.sas7bdat"
prev_pq  = MIS_DIR / f"LOAN{PREVDAY_STR}.parquet"

if prev_sas.exists():
    print(f"MIS prev SAS7BDAT found: {prev_sas}")
    df_prev, _ = pyreadstat.read_sas7bdat(str(prev_sas))
    df_prev.columns = [c.lower() for c in df_prev.columns]
    con.register("prevln_src", df_prev)
    con.execute("""
        CREATE OR REPLACE TABLE prevln AS
        SELECT branch, brlnamt AS pbrlnamt FROM prevln_src
    """)
elif prev_pq.exists():
    print(f"MIS prev Parquet  found: {prev_pq}")
    con.execute(f"""
        CREATE OR REPLACE TABLE prevln AS
        SELECT branch, brlnamt AS pbrlnamt
        FROM read_parquet('{prev_pq}')
    """)
else:
    print(f"MIS prev NOT FOUND ({prev_sas} / {prev_pq}) -> using zeros")
    con.execute("""
        CREATE OR REPLACE TABLE prevln AS
        SELECT branch, CAST(0.0 AS DOUBLE) AS pbrlnamt
        FROM loan_summary WHERE 1=0
    """)

con.execute("""
    CREATE OR REPLACE TABLE loans AS
    SELECT COALESCE(l.branch, p.branch) AS branch,
           l.extdate, l.noacct, l.brlnamt,
           COALESCE(p.pbrlnamt, 0) AS pbrlnamt
    FROM loan_summary l
    FULL OUTER JOIN prevln p ON l.branch = p.branch
""")


# ===========================================================================
# STEP 7 - BRANCH + MLOAN
# ===========================================================================
con.execute(f"""
    CREATE OR REPLACE TABLE branch AS
    SELECT * FROM read_parquet('{branch_parquet}')
""")

con.execute("""
    CREATE OR REPLACE TABLE mloan AS
    SELECT COALESCE(l.branch, b.branch) AS branch,
           b.bank, b.abbrev, b.brchname,
           l.extdate,
           COALESCE(l.noacct, 0) AS noacct,
           l.pbrlnamt, l.brlnamt,
           l.brlnamt - l.pbrlnamt AS varianln
    FROM loans l
    LEFT JOIN branch b ON l.branch = b.branch
""")


# ===========================================================================
# STEP 8 - Final report
# ===========================================================================
report = con.execute("""
    SELECT branch AS code, abbrev, brchname AS name,
           noacct   AS no_of_accounts,
           pbrlnamt AS prev_amount,
           brlnamt  AS curr_amount,
           varianln AS variance
    FROM mloan
    WHERE branch IS NOT NULL
    UNION ALL
    SELECT 999999, 'TOTAL', 'TOTAL',
           SUM(noacct), SUM(pbrlnamt), SUM(brlnamt), SUM(varianln)
    FROM mloan
    ORDER BY code
""").arrow()


# ===========================================================================
# STEP 9 - Final output
# ===========================================================================
output_base = f"EIBDLNS2_Branch_Loan_Summary_{REPTDATE.strftime('%Y%m%d')}"

parquet_file = OUTPUT_DIR / f"{output_base}.parquet"
pq.write_table(report, parquet_file, compression="snappy")
print(f"Final Parquet saved: {parquet_file}")

sas.df2sd(report.to_pandas(), table="EIBDLNS2", libref="WORK")
sas.submit(f"""
    libname out "{OUTPUT_DIR}";
    data out.{output_base};
        set WORK.EIBDLNS2;
    run;
""")
sas.endsas()
print(f"Final SAS7BDAT saved: {OUTPUT_DIR / (output_base + '.sas7bdat')}")


# ===========================================================================
# STEP 10 - Display
# ===========================================================================
print("\n" + "=" * 130)
print("PUBLIC BANK BERHAD")
print("BRANCH SUMMARY ON DAILY OUTSTANDING(RM)-BAE PERSONAL")
print(f"AS AT {RDATE}")
print("=" * 130)

for row in report.to_pylist():
    code = row["code"]
    if code == 999999:
        print("-" * 130)
    print(f"{code:<8} {row['abbrev'] or '':<8} {row['name'] or '':<35} "
          f"{row['no_of_accounts']:>12,} "
          f"{row['prev_amount']:>20,.2f} "
          f"{row['curr_amount']:>20,.2f} "
          f"{row['variance']:>20,.2f}")

print("=" * 130)
con.close()
print("\nProcessing complete!")
