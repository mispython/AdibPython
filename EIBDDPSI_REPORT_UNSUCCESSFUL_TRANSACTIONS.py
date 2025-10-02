import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import datetime

# ------------------------------------------------------------------
# Job name: EIBDDPSI - SI REPORT + WEEKLY UNSUCCESSFUL CASES
# ------------------------------------------------------------------

# Connect DuckDB in-memory
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
# Step 2: Load DEPOSIT input (replace with actual Parquet path)
# ------------------------------------------------------------------
con.execute("CREATE OR REPLACE TABLE deposit AS SELECT * FROM 'DEPOSIT.parquet'")

# ------------------------------------------------------------------
# Step 3: Main SI Report
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

# Convert STRDATE / ENDDATE → proper dates
strdates = []
enddates = []
for row in si_tbl.to_pylist():
    try:
        strdates.append(datetime.datetime.strptime(str(row["STRDATE"]).zfill(8), "%m%d%Y").date())
    except:
        strdates.append(None)
    try:
        enddates.append(datetime.datetime.strptime(str(row["ENDDATE"]).zfill(8), "%m%d%Y").date())
    except:
        enddates.append(None)

si_tbl = si_tbl.append_column("STRDT", pa.array(strdates))
si_tbl = si_tbl.append_column("ENDDT", pa.array(enddates))

# Save SI output
output_si_parquet = f"SI{REPTDAY}{REPTMON}{REPTYEAR}.parquet"
output_si_csv     = f"SI{REPTDAY}{REPTMON}{REPTYEAR}.csv"
pq.write_table(si_tbl, output_si_parquet)
con.register("si_tbl", si_tbl)
con.execute(f"COPY si_tbl TO '{output_si_csv}' (HEADER, DELIMITER ',')")

# ------------------------------------------------------------------
# Step 4: WEEKLY logic (only run on 09,16,23,29)
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

    # Reason code mapping (PROC FORMAT equivalent)
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

    # Add UNSUCC_DT and REASON
    reasons = []
    unsucc_dt = []
    for row in si_weekly.to_pylist():
        reasons.append(reason_map.get(row["RSNCD"], "UNKNOWN"))
        unsucc_dt.append(RDATE)

    si_weekly = si_weekly.append_column("UNSUCC_DT", pa.array(unsucc_dt))
    si_weekly = si_weekly.append_column("REASON", pa.array(reasons))

    # Save UNSUCCESSFUL SI report
    output_unsu_parquet = f"UNSUCSI{REPTDAY}{REPTMON}{REPTYEAR}.parquet"
    output_unsu_csv     = f"UNSUCSI{REPTDAY}{REPTMON}{REPTYEAR}.csv"
    pq.write_table(si_weekly, output_unsu_parquet)
    con.register("si_weekly", si_weekly)
    con.execute(f"COPY si_weekly TO '{output_unsu_csv}' (HEADER, DELIMITER ',')")

    print(f"EIBDDPSI weekly job executed. Outputs: {output_unsu_parquet}, {output_unsu_csv}")

# ------------------------------------------------------------------
print(f"EIBDDPSI daily job executed. Outputs: {output_si_parquet}, {output_si_csv}")
