import duckdb
import pyarrow as pa
import polars as pl
import datetime

# === Step 1: Dates (SAS CALL SYMPUT equivalent) ===
today = datetime.date.today()
reptdate = today - datetime.timedelta(days=1)

REPTDAY = reptdate.strftime("%d")
REPTMON = reptdate.strftime("%m")
REPTYEAR = reptdate.strftime("%y")   # 2-digit year
DD = today.strftime("%d")
RDATE = reptdate

# === Step 2: DuckDB connection ===
con = duckdb.connect()

# deposit parquet files (simulate JCL DD concatenation)
deposit_files = [
    "RBP2.B033.MST01.DPDARPGS.parquet",
    "RBP2.B033.MST02.DPDARPGS.parquet",
    "RBP2.B033.MST03.DPDARPGS.parquet",
    "RBP2.B033.MST04.DPDARPGS.parquet",
    "RBP2.B033.MST05.DPDARPGS.parquet",
    "RBP2.B033.MST06.DPDARPGS.parquet",
    "RBP2.B033.MST07.DPDARPGS.parquet",
    "RBP2.B033.MST08.DPDARPGS.parquet",
    "RBP2.B033.MST09.DPDARPGS.parquet",
    "RBP2.B033.MST10.DPDARPGS.parquet"
]

# Register all deposits as one virtual table
con.execute(f"""
    CREATE OR REPLACE VIEW deposit AS
    SELECT * FROM read_parquet({deposit_files})
""")

# === Step 3: First dataset (REPTNO=1220, FMTCODE=1) ===
tbl_si = con.execute("""
    SELECT * FROM deposit
    WHERE REPTNO = 1220 AND FMTCODE = 1
""").arrow()

if tbl_si.num_rows > 0:
    df_si = pl.from_arrow(tbl_si)

    # STRDATE / ENDDATE → datetime (like SAS INPUT/PUT)
    df_si = df_si.with_columns([
        df_si["STRDATE"].cast(pl.Utf8).str.slice(0, 8).str.strptime(pl.Datetime, "%m%d%Y", strict=False).alias("STRDT"),
        df_si["ENDDATE"].cast(pl.Utf8).str.slice(0, 8).str.strptime(pl.Datetime, "%m%d%Y", strict=False).alias("ENDDT")
    ])

    out1_parquet = f"SI{REPTDAY}{REPTMON}{REPTYEAR}.parquet"
    out1_csv     = f"SI{REPTDAY}{REPTMON}{REPTYEAR}.csv"

    df_si.write_parquet(out1_parquet)
    df_si.write_csv(out1_csv)

# === Step 4: Weekly condition (only 09,16,23,29) ===
if DD in ["09", "16", "23", "29"]:
    tbl_unsu = con.execute("""
        SELECT * FROM deposit
        WHERE REPTNO = 1231 AND FMTCODE = 1
    """).arrow()

    if tbl_unsu.num_rows > 0:
        df_unsu = pl.from_arrow(tbl_unsu)

        # SAS PROC FORMAT mapping for REASONCD
        reason_map = {
            1: "ACCOUNT DOES NOT EXIST",
            2: "ACCOUNT CLOSED",
            3: "POST NO TRANSACTIONS",
            4: "DEPCAT = C OR T",
            5: "AMDB PRA SEG NOT FOUND",
            6: "BCDB PRA SEG NOT FOUND",
            7: "INSUFFICIENT FUNDS",
            8: "ABA BANK TABLE ERROR",
            9: "INVALID INSTRUCTION TYPE",
            10: "NO TYPE 2 INSTRUCT. FOUND",
            11: "INTERBANK TABLE ERROR",
            12: "POST NO DEBITS",
            13: "POST NO CREDITS",
            14: "ATS DR EXCEEDS EXCTRN LMT",
            15: "ATS SVC DR EXCEEDS TRNLMT",
            16: "NOT ALLOW DR PB SHARELINK"
        }

        # map RSNCD → REASON, add UNSUCC_DT
        df_unsu = df_unsu.with_columns([
            pl.lit(RDATE).alias("UNSUCC_DT"),
            df_unsu["RSNCD"].map_dict(reason_map, default=None).alias("REASON")
        ])

        out2_parquet = f"UNSUCSI{REPTDAY}{REPTMON}{REPTYEAR}.parquet"
        out2_csv     = f"UNSUCSI{REPTDAY}{REPTMON}{REPTYEAR}.csv"

        df_unsu.write_parquet(out2_parquet)
        df_unsu.write_csv(out2_csv)

con.close()
