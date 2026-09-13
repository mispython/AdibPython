# -*- coding: utf-8 -*-
"""
EIBDLNS2 - Branch Daily Outstanding Loan Summary Report
Python version of SAS job EIBDLNS2

Pipeline:
  1. Read DATEFILE (single-line ASCII) -> derive report date
  2. Decode NFEEFILE (RECFM=FB, LRECL=300, EBCDIC + COMP-3)  -> NFEEFILE.parquet
  3. Decode ACCTFILE (RECFM=FB, LRECL=4000, EBCDIC + COMP-3) -> ACCTFILE.parquet
  4. Read LKP_BRANCH (ASCII fixed-width)                     -> LKP_BRANCH.parquet
  5. DuckDB: feplan -> feepo -> loan -> loan_summary
  6. Cross-day: read MIS_DIR/LOAN{PREVDAY}.sas7bdat (or .parquet)
                write MIS_DIR/LOAN{REPTDAY}.sas7bdat + .parquet
  7. Merge with branch, compute variance, write final report
  8. Output: Parquet + SAS7BDAT (via saspy)
"""

import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyreadstat
from datetime import timedelta
from pathlib import Path
import saspy

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
OUTPUT_DIR = Path("/stgsrcsys/host/holding")             # final report output
MIS_DIR    = Path("/stgsrcsys/host/uat/python/loans/")   # cross-day MIS storage

DATEFILE = Path("/host_pq/dwh/input/LOAN/DATEFILE")
FEEFILE  = "/host_pq/dwh/input/LOAN/NFEEFILE_{reptyear}{reptmon}{reptday}"
ACCTFILE = "/host_pq/dwh/input/LOAN/ACCTFILE_{reptyear}{reptmon}{reptday}"
BRANCHF  = Path("/sasdata/rawdata/lookup/LKP_BRANCH")

FEE_LRECL  = 300
ACCT_LRECL = 4000

# ---------------------------------------------------------------------------
# DuckDB
# ---------------------------------------------------------------------------
con = duckdb.connect(":memory:")
print("REPORT ID : EIBDLNSA")

# ===========================================================================
# STEP 1 - DATA REPTDATE
# ===========================================================================
with open(DATEFILE, "r") as f:
    first_line = f.readline()

EXTDATE_str = first_line[0:11].strip().zfill(11)
extdate_str = EXTDATE_str[:8]                      # MMDDYYYY
EXTDATE     = int(EXTDATE_str)

REPTDATE = con.execute(
    f"SELECT CAST(strptime('{extdate_str}', '%m%d%Y') AS DATE)"
).fetchone()[0]

PREVDATE = REPTDATE - timedelta(days=1)
DLETDATE = REPTDATE - timedelta(days=3)
REPTDAY  = REPTDATE.day
PREVDAY  = PREVDATE.day
DLETDAY  = DLETDATE.day
YY = REPTDATE.year - 1 if (REPTDATE.month == 1 and REPTDATE.day == 1) else REPTDATE.year

REPTYEAR     = str(REPTDATE.year)
PREVYEAR     = str(YY).zfill(4)
REPTMON      = str(REPTDATE.month).zfill(2)
REPTDAY_STR  = str(REPTDAY).zfill(2)
PREVDAY_STR  = str(PREVDAY).zfill(2)
DLETDAY_STR  = str(DLETDAY).zfill(2)
RDATE        = REPTDATE.strftime("%d%m%Y")
REPTDATE_INT = int(REPTDATE.strftime("%y%m%d"))

print(f"Report Date: {REPTDATE.strftime('%d/%m/%Y')}")
print(f"Prev   Date: {PREVDATE.strftime('%d/%m/%Y')}")

feefile_path  = FEEFILE.format(reptyear=REPTYEAR, reptmon=REPTMON, reptday=REPTDAY_STR)
acctfile_path = ACCTFILE.format(reptyear=REPTYEAR, reptmon=REPTMON, reptday=REPTDAY_STR)


# ===========================================================================
# Fixed-block + COMP-3 + EBCDIC decoders
# ===========================================================================
def _unpack_pd(raw: bytes, decimals: int = 0) -> float:
    """Unpack a COMP-3 packed-decimal byte string into a Python float."""
    if not raw:
        return 0.0
    sign_nibble = raw[-1] & 0x0F
    digits = []
    for b in raw[:-1]:
        digits.append((b >> 4) & 0x0F)
        digits.append(b & 0x0F)
    digits.append((raw[-1] >> 4) & 0x0F)
    digit_str = "".join(str(d) for d in digits)
    num = int(digit_str) if digit_str else 0
    if sign_nibble in (0x0D, 0x0B):
        num = -num
    return num / (10 ** decimals) if decimals else float(num)


def _decode_ebcdic(raw: bytes) -> str:
    return raw.decode("cp037", errors="replace").rstrip()


def _read_fb(path: Path, reclen: int):
    """Yield fixed-length records from a RECFM=FB file."""
    with open(path, "rb") as f:
        while True:
            rec = f.read(reclen)
            if len(rec) < reclen:
                if rec:
                    yield rec
                return
            yield rec


# ---------------------------------------------------------------------------
# STEP 2 - NFEEFILE -> Parquet
# ---------------------------------------------------------------------------
fee_parquet = OUTPUT_DIR / f"NFEEFILE_{REPTYEAR}{REPTMON}{REPTDAY_STR}.parquet"

if fee_parquet.exists():
    print(f"NFEEFILE parquet cached: {fee_parquet}")
else:
    print(f"Decoding NFEEFILE: {feefile_path}")
    rows = []
    n = 0
    for rec in _read_fb(Path(feefile_path), FEE_LRECL):
        n += 1
        if n % 1_000_000 == 0:
            print(f"  ... {n:,} records")
        acctno   = _unpack_pd(rec[0:6],   0)
        noteno   = _unpack_pd(rec[6:9],   0)
        loantype = _unpack_pd(rec[9:11],  0)
        feepln   = _decode_ebcdic(rec[21:23])
        if acctno >= 3000000000:
            continue
        if loantype not in (135, 136):
            continue
        if feepln != "PA":
            continue
        rows.append((
            int(acctno),
            int(noteno),
            int(loantype),
            feepln,
            _unpack_pd(rec[34:42], 2),
            _unpack_pd(rec[66:74], 2),
            _unpack_pd(rec[74:82], 2),
        ))

    df_fee = pd.DataFrame(
        rows,
        columns=["acctno", "noteno", "loantype", "feepln",
                 "feeamta", "feeamtc", "feeamtb"],
    )
    print(f"  {len(df_fee):,} rows kept -> {fee_parquet}")
    pq.write_table(pa.Table.from_pandas(df_fee), fee_parquet, compression="zstd")


# ---------------------------------------------------------------------------
# STEP 3 - ACCTFILE -> Parquet
# ---------------------------------------------------------------------------
acct_parquet = OUTPUT_DIR / f"ACCTFILE_{REPTYEAR}{REPTMON}{REPTDAY_STR}.parquet"

if acct_parquet.exists():
    print(f"ACCTFILE parquet cached: {acct_parquet}")
else:
    print(f"Decoding ACCTFILE: {acctfile_path}")
    rows = []
    n = 0
    for rec in _read_fb(Path(acctfile_path), ACCT_LRECL):
        n += 1
        if n % 500_000 == 0:
            print(f"  ... {n:,} records")
        acctno   = _unpack_pd(rec[0:6],   0)
        loantype = _unpack_pd(rec[84:86], 0)
        if loantype not in (135, 136):
            continue
        rows.append((
            int(acctno),
            _decode_ebcdic(rec[6:30]),         # NAME
            int(_unpack_pd(rec[62:64], 0)),    # BANKNO
            int(_unpack_pd(rec[65:69], 0)),    # ACCBRCH
            int(_unpack_pd(rec[80:83], 0)),    # NOTENO
            _decode_ebcdic(rec[83:84]),        # REVERSED
            int(loantype),                     # LOANTYPE
            int(_unpack_pd(rec[86:90], 0)),    # NTBRCH
            int(_unpack_pd(rec[90:94], 0)),    # PENDBRH
            int(_unpack_pd(rec[112:118], 0)),  # LASTTRAN
            _unpack_pd(rec[120:128], 2),       # CURBAL
            _unpack_pd(rec[128:136], 2),       # INTAMT
            _decode_ebcdic(rec[260:261]),      # PAIDIND
            _decode_ebcdic(rec[295:296]),      # NTINT
            _unpack_pd(rec[310:318], 2),       # INTEARN
            _unpack_pd(rec[318:326], 7),       # ACCRUAL
            _unpack_pd(rec[399:407], 2),       # INTEARN2
            _unpack_pd(rec[407:415], 2),       # INTEARN3
            _unpack_pd(rec[415:423], 2),       # INTEARN4
            _unpack_pd(rec[456:464], 2),       # FEEAMT
            _unpack_pd(rec[464:472], 2),       # FEEAMT2
            _unpack_pd(rec[553:561], 2),       # FEEAMT4
            _unpack_pd(rec[763:771], 2),       # NFEEAMT5
            _unpack_pd(rec[771:779], 2),       # NFEEAMT6
            _unpack_pd(rec[779:787], 2),       # NFEEAMT7
            _unpack_pd(rec[860:868], 2),       # FEEAMT8
            _unpack_pd(rec[868:876], 2),       # FEEAMT9
            _unpack_pd(rec[884:892], 2),       # FEEAMT13
            _unpack_pd(rec[903:911], 2),       # FEEAMT10
            _unpack_pd(rec[911:919], 2),       # FEEAMT11
            _unpack_pd(rec[919:927], 2),       # FEEAMT12
            _unpack_pd(rec[927:935], 2),       # FEEAMT14
            _unpack_pd(rec[935:943], 2),       # FEEAMT15
            _unpack_pd(rec[943:951], 2),       # FEEAMT16
        ))

    df_acct = pd.DataFrame(
        rows,
        columns=[
            "acctno", "name", "bankno", "accbrch", "noteno", "reversed",
            "loantype", "ntbrch", "pendbrh", "lasttran", "curbal", "intamt",
            "paidind", "ntint", "intearn", "accrual",
            "intearn2", "intearn3", "intearn4",
            "feeamt", "feeamt2", "feeamt4",
            "nfeeamt5", "nfeeamt6", "nfeeamt7",
            "feeamt8", "feeamt9", "feeamt13",
            "feeamt10", "feeamt11", "feeamt12",
            "feeamt14", "feeamt15", "feeamt16",
        ],
    )
    print(f"  {len(df_acct):,} rows kept -> {acct_parquet}")
    pq.write_table(pa.Table.from_pandas(df_acct), acct_parquet, compression="zstd")


# ---------------------------------------------------------------------------
# STEP 4 - LKP_BRANCH -> Parquet
# ---------------------------------------------------------------------------
branch_parquet = OUTPUT_DIR / "LKP_BRANCH.parquet"
if branch_parquet.exists():
    print(f"LKP_BRANCH parquet cached: {branch_parquet}")
else:
    branch_rows = []
    with open(BRANCHF, "r", encoding="latin-1", newline="") as f:
        for line in f:
            line = line.rstrip("\r\n")
            if not line:
                continue
            if line.startswith("NOTE:") or "The SAS System" in line:
                continue
            bank   = line[0:1]
            branch = line[1:4].strip()
            abbrev = line[5:8].rstrip()
            name   = line[11:41].rstrip()
            if not branch.isdigit():
                continue
            branch_rows.append({
                "bank": bank,
                "branch": int(branch),
                "abbrev": abbrev,
                "brchname": name,
            })
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
    SELECT
        *,
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
    SELECT
        l.*,
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
#
#   SAS:  DATA MIS.LOAN&REPTDAY; SET LOAN;       -> write today
#         DATA PREVLN;         SET MIS.LOAN&PREVDAY;
#                              RENAME BRLNAMT=PBRLNAMT;   -> read yesterday
# ===========================================================================

# --- (A) Write today's summary: BOTH .sas7bdat and .parquet ---
today_df   = con.execute("SELECT * FROM loan_summary").to_df()
today_sas  = MIS_DIR / f"LOAN{REPTDAY_STR}.sas7bdat"
today_pq   = MIS_DIR / f"LOAN{REPTDAY_STR}.parquet"

# Write SAS7BDAT via saspy (uses a temporary WORK dataset then copies out)
sas = saspy.SASsession(cfgname="default")
sas.df2sd(today_df, table="MISLOAN_TODAY", libref="WORK")
sas.submit(f"""
    libname misout "{MIS_DIR}";
    data misout.LOAN{REPTDAY_STR};
        set WORK.MISLOAN_TODAY;
    run;
""")
print(f"MIS today SAS7BDAT saved: {today_sas}")

# Also keep a Parquet copy for fast future reads
pq.write_table(pa.Table.from_pandas(today_df), today_pq, compression="zstd")
print(f"MIS today Parquet  saved: {today_pq}")


# --- (B) Read yesterday's summary: prefer .sas7bdat, fall back to .parquet ---
prev_sas = MIS_DIR / f"LOAN{PREVDAY_STR}.sas7bdat"
prev_pq  = MIS_DIR / f"LOAN{PREVDAY_STR}.parquet"

if prev_sas.exists():
    print(f"MIS prev SAS7BDAT found: {prev_sas}")
    df_prev, meta = pyreadstat.read_sas7bdat(str(prev_sas))
    df_prev.columns = [c.lower() for c in df_prev.columns]
    con.register("prevln_src", df_prev)
    con.execute("""
        CREATE OR REPLACE TABLE prevln AS
        SELECT branch, brlnamt AS pbrlnamt
        FROM prevln_src
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

# DATA LOANS
con.execute("""
    CREATE OR REPLACE TABLE loans AS
    SELECT COALESCE(l.branch, p.branch) AS branch,
           l.extdate, l.noacct, l.brlnamt,
           COALESCE(p.pbrlnamt, 0) AS pbrlnamt
    FROM loan_summary l
    FULL OUTER JOIN prevln p ON l.branch = p.branch
""")


# ===========================================================================
# STEP 7 - DATA BRANCH + MLOAN
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
pq.write_table(report, parquet_file)
print(f"Final Parquet saved: {parquet_file}")

# SAS7BDAT via saspy (reuse the open SAS session)
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
# STEP 10 - Display report
# ===========================================================================
print("\n" + "=" * 130)
print("PUBLIC BANK BERHAD")
print("BRANCH SUMMARY ON DAILY OUTSTANDING(RM)-BAE PERSONAL")
print(f"AS AT {RDATE}")
print("=" * 130)
print(f"{'BRANCH':<8} {'ABBREV':<8} {'NAME':<35} {'NO OF':>12} "
      f"{'PREV.AMOUNT':>20} {'CURR.AMOUNT':>20} {'VARIANCE':>20}")
print(f"{'CODE':<8} {'':<8} {'':<35} {'ACCOUNTS':>12} "
      f"{'(RM)':>20} {'(RM)':>20} {'(RM)':>20}")
print("-" * 130)

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
