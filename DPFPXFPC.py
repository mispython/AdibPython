# -*- coding: utf-8 -*-
"""
EIBDLNS2 - Branch Daily Outstanding Loan Summary Report (optimised)
"""

import duckdb
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyreadstat
from datetime import timedelta
from pathlib import Path
import saspy

OUTPUT_DIR = Path("/stgsrcsys/host/holding")
MIS_DIR    = Path("/stgsrcsys/host/uat/python/loans/")

DATEFILE = Path("/host_pq/dwh/input/LOAN/DATEFILE")
FEEFILE  = "/host_pq/dwh/input/LOAN/NFEEFILE_{reptyear}{reptmon}{reptday}"
ACCTFILE = "/host_pq/dwh/input/LOAN/ACCTFILE_{reptyear}{reptmon}{reptday}"
BRANCHF  = Path("/sasdata/rawdata/lookup/LKP_BRANCH")

FEE_LRECL  = 300
ACCT_LRECL = 4000

# chunk size in records (tune to available RAM; 200k * 4000B = 800 MB per chunk)
FEE_CHUNK  = 500_000
ACCT_CHUNK = 100_000

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
PREVYEAR     = str(YY).zfill(4)
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
# Vectorised packed-decimal decoder
#   pd_matrix: 2-D uint8 array of shape (n_records, field_len)
#   Returns a 1-D float64 array of length n_records
# ===========================================================================
def pd_decode(pd_bytes: np.ndarray, decimals: int = 0) -> np.ndarray:
    """
    pd_bytes: uint8 array shape (N, L) where L = packed length in bytes.
    Each byte holds two BCD digits; the last nibble of the last byte is the
    sign nibble (C/F positive, D/B negative).
    """
    # high nibble of every byte, low nibble of every byte
    high = (pd_bytes >> 4) & 0x0F        # (N, L)
    low  = pd_bytes & 0x0F               # (N, L)

    # combine: for byte i, the digit order is high[i], low[i], so the full
    # digit string is [high[:,0], low[:,0], high[:,1], low[:,1], ...,
    #                  high[:,L-1]] with low[:,L-1] being the sign nibble.
    # We exclude the last low nibble (sign) from the digits.
    digits = np.empty((pd_bytes.shape[0], 2 * pd_bytes.shape[1] - 1),
                      dtype=np.uint8)
    digits[:, 0::2] = high
    digits[:, 1::2] = low[:, :-1]        # drop last low (sign)

    # Convert BCD digits to number: value = sum(digit_i * 10^(k-1-i))
    powers = 10 ** np.arange(digits.shape[1] - 1, -1, -1, dtype=np.int64)
    values = (digits.astype(np.int64) * powers).sum(axis=1)

    # apply sign from last low nibble
    sign_nibble = low[:, -1]
    negative = (sign_nibble == 0x0D) | (sign_nibble == 0x0B)
    values = np.where(negative, -values, values)

    if decimals:
        values = values / (10 ** decimals)
    return values.astype(np.float64)


def ebcdic_decode(col: np.ndarray) -> list:
    """col: uint8 array shape (N, L) -> list of N python strings."""
    # decode the whole 2-D array as EBCDIC cp037 then rstrip per row
    buf = col.tobytes()
    text = buf.decode("cp037", errors="replace")
    L = col.shape[1]
    return [text[i*L:(i+1)*L].rstrip() for i in range(col.shape[0])]


# ===========================================================================
# STEP 2 - NFEEFILE -> Parquet  (chunked, vectorised)
# ===========================================================================
fee_parquet = OUTPUT_DIR / f"NFEEFILE_{REPTYEAR}{REPTMON}{REPTDAY_STR}.parquet"

if fee_parquet.exists():
    print(f"NFEEFILE parquet cached: {fee_parquet}")
else:
    print(f"Decoding NFEEFILE: {feefile_path}")
    writer = None
    file_size = Path(feefile_path).stat().st_size
    n_records_total = file_size // FEE_LRECL
    print(f"  file size: {file_size:,} bytes  -> ~{n_records_total:,} records")

    with open(feefile_path, "rb") as f:
        processed = 0
        while processed < n_records_total:
            n_this = min(FEE_CHUNK, n_records_total - processed)
            raw = f.read(n_this * FEE_LRECL)
            if not raw:
                break
            n_this = len(raw) // FEE_LRECL
            arr = np.frombuffer(raw, dtype=np.uint8).reshape(n_this, FEE_LRECL)

            acctno   = pd_decode(arr[:, 0:6],  0).astype(np.int64)
            noteno   = pd_decode(arr[:, 6:9],  0).astype(np.int64)
            loantype = pd_decode(arr[:, 9:11], 0).astype(np.int32)
            feepln   = np.array(ebcdic_decode(arr[:, 21:23]), dtype=object)
            feeamta  = pd_decode(arr[:, 34:42], 2)
            feeamtc  = pd_decode(arr[:, 66:74], 2)
            feeamtb  = pd_decode(arr[:, 74:82], 2)

            mask = (
                (acctno < 3000000000) &
                ((loantype == 135) | (loantype == 136)) &
                (feepln == "PA")
            )

            tbl = pa.table({
                "acctno":   pa.array(acctno[mask],   type=pa.int64()),
                "noteno":   pa.array(noteno[mask],   type=pa.int64()),
                "loantype": pa.array(loantype[mask], type=pa.int32()),
                "feepln":   pa.array(feepln[mask],   type=pa.string()),
                "feeamta":  pa.array(feeamta[mask],  type=pa.float64()),
                "feeamtc":  pa.array(feeamtc[mask],  type=pa.float64()),
                "feeamtb":  pa.array(feeamtb[mask],  type=pa.float64()),
            })

            if writer is None:
                writer = pq.ParquetWriter(fee_parquet, tbl.schema,
                                          compression="zstd")
            writer.write_table(tbl)

            processed += n_this
            print(f"  ... {processed:,} / {n_records_total:,} records")

    if writer is not None:
        writer.close()
    print(f"  NFEEFILE parquet written -> {fee_parquet}")


# ===========================================================================
# STEP 3 - ACCTFILE -> Parquet  (chunked, vectorised)
# ===========================================================================
acct_parquet = OUTPUT_DIR / f"ACCTFILE_{REPTYEAR}{REPTMON}{REPTDAY_STR}.parquet"

if acct_parquet.exists():
    print(f"ACCTFILE parquet cached: {acct_parquet}")
else:
    print(f"Decoding ACCTFILE: {acctfile_path}")
    writer = None
    file_size = Path(acctfile_path).stat().st_size
    n_records_total = file_size // ACCT_LRECL
    print(f"  file size: {file_size:,} bytes  -> ~{n_records_total:,} records")

    with open(acctfile_path, "rb") as f:
        processed = 0
        while processed < n_records_total:
            n_this = min(ACCT_CHUNK, n_records_total - processed)
            raw = f.read(n_this * ACCT_LRECL)
            if not raw:
                break
            n_this = len(raw) // ACCT_LRECL
            arr = np.frombuffer(raw, dtype=np.uint8).reshape(n_this, ACCT_LRECL)

            acctno   = pd_decode(arr[:, 0:6],   0).astype(np.int64)
            loantype = pd_decode(arr[:, 84:86], 0).astype(np.int32)
            mask = (loantype == 135) | (loantype == 136)

            # Early exit if chunk has no relevant rows
            if not mask.any():
                processed += n_this
                print(f"  ... {processed:,} / {n_records_total:,} records")
                continue

            a = arr[mask]
            n_kept = a.shape[0]

            tbl = pa.table({
                "acctno":   pa.array(acctno[mask], type=pa.int64()),
                "name":     pa.array(ebcdic_decode(a[:, 6:30]),    type=pa.string()),
                "bankno":   pa.array(pd_decode(a[:, 62:64], 0).astype(np.int32)),
                "accbrch":  pa.array(pd_decode(a[:, 65:69], 0).astype(np.int32)),
                "noteno":   pa.array(pd_decode(a[:, 80:83], 0).astype(np.int64)),
                "reversed": pa.array(ebcdic_decode(a[:, 83:84]),   type=pa.string()),
                "loantype": pa.array(loantype[mask],               type=pa.int32()),
                "ntbrch":   pa.array(pd_decode(a[:, 86:90], 0).astype(np.int32)),
                "pendbrh":  pa.array(pd_decode(a[:, 90:94], 0).astype(np.int32)),
                "lasttran": pa.array(pd_decode(a[:, 112:118], 0).astype(np.int64)),
                "curbal":   pa.array(pd_decode(a[:, 120:128], 2)),
                "intamt":   pa.array(pd_decode(a[:, 128:136], 2)),
                "paidind":  pa.array(ebcdic_decode(a[:, 260:261]), type=pa.string()),
                "ntint":    pa.array(ebcdic_decode(a[:, 295:296]), type=pa.string()),
                "intearn":  pa.array(pd_decode(a[:, 310:318], 2)),
                "accrual":  pa.array(pd_decode(a[:, 318:326], 7)),
                "intearn2": pa.array(pd_decode(a[:, 399:407], 2)),
                "intearn3": pa.array(pd_decode(a[:, 407:415], 2)),
                "intearn4": pa.array(pd_decode(a[:, 415:423], 2)),
                "feeamt":   pa.array(pd_decode(a[:, 456:464], 2)),
                "feeamt2":  pa.array(pd_decode(a[:, 464:472], 2)),
                "feeamt4":  pa.array(pd_decode(a[:, 553:561], 2)),
                "nfeeamt5": pa.array(pd_decode(a[:, 763:771], 2)),
                "nfeeamt6": pa.array(pd_decode(a[:, 771:779], 2)),
                "nfeeamt7": pa.array(pd_decode(a[:, 779:787], 2)),
                "feeamt8":  pa.array(pd_decode(a[:, 860:868], 2)),
                "feeamt9":  pa.array(pd_decode(a[:, 868:876], 2)),
                "feeamt13": pa.array(pd_decode(a[:, 884:892], 2)),
                "feeamt10": pa.array(pd_decode(a[:, 903:911], 2)),
                "feeamt11": pa.array(pd_decode(a[:, 911:919], 2)),
                "feeamt12": pa.array(pd_decode(a[:, 919:927], 2)),
                "feeamt14": pa.array(pd_decode(a[:, 927:935], 2)),
                "feeamt15": pa.array(pd_decode(a[:, 935:943], 2)),
                "feeamt16": pa.array(pd_decode(a[:, 943:951], 2)),
            })

            if writer is None:
                writer = pq.ParquetWriter(acct_parquet, tbl.schema,
                                          compression="zstd")
            writer.write_table(tbl)

            processed += n_this
            print(f"  ... {processed:,} / {n_records_total:,} records "
                  f"(kept this chunk: {n_kept:,})")

    if writer is not None:
        writer.close()
    print(f"  ACCTFILE parquet written -> {acct_parquet}")


# ===========================================================================
# STEP 4 - LKP_BRANCH -> Parquet  (unchanged, tiny file)
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
            branch_rows.append({
                "bank": bank, "branch": int(branch),
                "abbrev": abbrev, "brchname": name,
            })
    df_branch = pd.DataFrame(branch_rows)
    print(f"  {len(df_branch):,} branch rows -> {branch_parquet}")
    pq.write_table(pa.Table.from_pandas(df_branch), branch_parquet)


# ===========================================================================
# STEP 5 - DuckDB processing (unchanged)
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

pq.write_table(pa.Table.from_pandas(today_df), today_pq, compression="zstd")
print(f"MIS today Parquet  saved: {today_pq}")

prev_sas = MIS_DIR / f"LOAN{PREVDAY_STR}.sas7bdat"
prev_pq  = MIS_DIR / f"LOAN{PREVDAY_STR}.parquet"

if prev_sas.exists():
    print(f"MIS prev SAS7BDAT found: {prev_sas}")
    df_prev, meta = pyreadstat.read_sas7bdat(str(prev_sas))
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
pq.write_table(report, parquet_file)
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
