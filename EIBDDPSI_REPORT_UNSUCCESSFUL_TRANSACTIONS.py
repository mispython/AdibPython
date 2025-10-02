import duckdb
import pyarrow.parquet as pq
import pyarrow.csv as pc
import pyarrow as pa
import datetime

# ------------------------------------------------------------------
# Job name: EIBDDPSI - SI REPORT + WEEKLY UNSUCCESSFUL CASES
# ------------------------------------------------------------------

con = duckdb.connect()

# ------------------------------------------------------------------
# Step 1: REPTDATE logic
# ------------------------------------------------------------------
today = datetime.date.today()
reptdate = today - datetime.timedelta(days=1)

REPTDAY  = str(reptdate.day).zfill(2)
REPTMON  = str(reptdate.month).zfill(2)
REPTYEAR = str(reptdate.year)[-2:]  # last 2 digits
DD       = str(today.day).zfill(2)
RDATE    = reptdate.strftime("%Y-%m-%d")

# ------------------------------------------------------------------
# Step 2: Load DEPOSIT input (multiple parquet files)
# ------------------------------------------------------------------
# Example: all MST*.parquet in same folder
con.execute("""
    CREATE OR REPLACE TABLE deposit AS
    SELECT * FROM read_parquet(['MST01.parquet','MST02.parquet',
                                'MST03.parquet','MST04.parquet',
                                'MST05.parquet','MST06.parquet',
                                'MST07.parquet','MST08.parquet',
                                'MST09.parquet','MST10.parquet'])
""")

# Or use glob if they are in the same folder:
# con.execute("CREATE OR REPLACE TABLE deposit AS SELECT * FROM read_parquet('MST*.parquet')")

# ------------------------------------------------------------------
# Step 3: Daily SI Report
# ------------------------------------------------------------------
si_tbl = con.execute("""
    SELECT 
        BANKNO,
        REPTNO,
        FMTCODE,
        FLSEQNUM,
        BRANCH,
        ACCTNO,
        NAME,
        CONTROL,
        TYPE,
        FUNC,
        RECSEQNUM,
        STRDATE,
        ENDDATE,
        CURRENCY,
        CYCLE,
        AMOUNT,
        TOACCT,
        NEXTDATE,
        TP,
        BENECD,
        REFNO,
        DESC,
        USERID
    FROM deposit
    WHERE REPTNO = 1220 AND FMTCODE = 1
""").arrow()

# Save SI output
out_si_parquet = f"SI{REPTDAY}{REPTMON}{REPTYEAR}.parquet"
out_si_csv     = f"SI{REPTDAY}{REPTMON}{REPTYEAR}.csv"

pq.write_table(si_tbl, out_si_parquet)
pc.write_csv(si_tbl, out_si_csv)

# ------------------------------------------------------------------
# Step 4: Weekly unsuccessful SI (only on 09,16,23,29)
# ------------------------------------------------------------------
if DD in ["09", "16", "23", "29"]:
    si_weekly = con.execute("""
        SELECT 
            BRANCH,
            ACCTNO,
            NAME,
            DEPCAT,
            TYPE,
            SEQNO,
            AMOUNT,
            TRANTYP,
            TOACCT,
            SERVCH,
            SERVCHTP,
            RSNFRM,
            RSNCD,
            BENEFC
        FROM deposit
        WHERE REPTNO = 1231 AND FMTCODE = 1
    """).arrow()

    # Reason code mapping
    reason_map = {
        1:  "ACCOUNT DOES NOT EXIST",
        2:  "ACCOUNT CLOSED",
        3:  "POST NO TRANSACTIONS",
        4:  "DEPCAT = C OR T",
        5:  "AMDB PRA SEG NOT FOUND",
        6:  "BCDB PRA SEG NOT FOUND",
        7:  "INSUFFICIENT FUNDS",
        8:  "ABA BANK TABLE ERROR",
        9:  "INVALID INSTRUCTION TYPE",
        10: "NO TYPE 2 INSTRUCT. FOUND",
        11: "INTERBANK TABLE ERROR",
        12: "POST NO DEBITS",
        13: "POST NO CREDITS",
        14: "ATS DR EXCEEDS EXCTRN LMT",
        15: "ATS SVC DR EXCEEDS TRNLMT",
        16: "NOT ALLOW DR PB SHARELINK"
    }

    rows = si_weekly.to_pylist()
    reasons = [reason_map.get(row["RSNCD"], "UNKNOWN") for row in rows]
    unsucc_dt = [RDATE for _ in rows]

    si_weekly = si_weekly.append_column("UNSUCC_DT", pa.array(unsucc_dt))
    si_weekly = si_weekly.append_column("REASON", pa.array(reasons))

    # Save weekly output
    out_unsu_parquet = f"UNSUCSI{REPTDAY}{REPTMON}{REPTYEAR}.parquet"
    out_unsu_csv     = f"UNSUCSI{REPTDAY}{REPTMON}{REPTYEAR}.csv"

    pq.write_table(si_weekly, out_unsu_parquet)
    pc.write_csv(si_weekly, out_unsu_csv)

    print(f"EIBDDPSI weekly job executed. Outputs: {out_unsu_parquet}, {out_unsu_csv}")

# ------------------------------------------------------------------
print(f"EIBDDPSI daily job executed. Outputs: {out_si_parquet}, {out_si_csv}")
