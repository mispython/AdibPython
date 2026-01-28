import polars as pl
import duckdb
from datetime import datetime
from pathlib import Path

# ==================== SETUP ====================
BASE_PATH = Path("/path/to/data")
MNITB_PATH = BASE_PATH / "mnitb"
SCRD_PATH = BASE_PATH / "scrd"
OUTPUT_PATH = BASE_PATH / "output"

# File paths (assuming parquet conversion)
CARDFILE = BASE_PATH / "CARDFILE.parquet"
DEP_FILE = BASE_PATH / "DEP.parquet"
ELDS_FILE = BASE_PATH / "ELDS.parquet"
HRBR_FILE = BASE_PATH / "HRBR.parquet"
HRHO_FILE = BASE_PATH / "HRHO.parquet"
BRHFI_FILE = BASE_PATH / "BRHFI.parquet"

conn = duckdb.connect()

# ==================== REPTDATE PROCESSING ====================
def get_report_dates():
    reptdate_df = pl.read_parquet(MNITB_PATH / "REPTDATE.parquet")
    reptdate_val = reptdate_df[0, "REPTDATE"]
    
    RDATE = reptdate_val.strftime("%d/%m/%Y")
    RMM = f"{reptdate_val.month:02d}"
    RMON = reptdate_val.strftime("%b%y").upper()
    RYY = str(reptdate_val.year)[-2:]
    RYEAR = str(reptdate_val.year)
    
    return RDATE, RMM, RMON, RYY, RYEAR

RDATE, RMM, RMON, RYY, RYEAR = get_report_dates()

# ==================== BRANCH HEADER PROCESSING ====================
brh_df = pl.read_parquet(BRHFI_FILE).select(["BRANCH", "BRCHCD"])

# ==================== HR FILES PROCESSING ====================
def process_hr_files():
    # Process HRBR
    hrbr_df = pl.read_parquet(HRBR_FILE).select([
        pl.col("STAFF").cast(pl.Int32),
        pl.col("BRANCH").cast(pl.Int32),
        pl.col("BRCHCD").cast(pl.Utf8)
    ]).filter((2 <= pl.col("BRANCH")) & (pl.col("BRANCH") <= 267))
    
    # Process HRHO
    hrho_df = pl.read_parquet(HRHO_FILE).select([
        pl.col("STAFF").cast(pl.Int32),
        pl.col("HOE").cast(pl.Utf8)
    ])
    
    return hrbr_df, hrho_df

hrbr_df, hrho_df = process_hr_files()

# ==================== UCARD PROCESSING ====================
card_df = pl.read_parquet(CARDFILE)

# Filter and process UCARD data
valid_typec = [3301, 3302, 3305, 3306, 3308, 3309, 5503, 5504]

ucard_df = card_df.with_columns([
    pl.col("ACCTNO").cast(pl.Utf8).str.slice(0, 5).cast(pl.Int32).alias("TYPEC"),
    pl.col("SECO").str.strip_chars().alias("SECO_CLEAN")
]).filter(
    pl.col("TYPEC").is_in(valid_typec) &
    ((pl.col("OPNYR") == 6) & (pl.col("OPNMM") > 8) | (pl.col("OPNYR") >= 7)) &
    (pl.col("SECO_CLEAN").str.len_bytes() > 1) &
    (pl.col("SECO_CLEAN").str.len_bytes() < 6) &
    (pl.col("SECO_CLEAN").str.replace_all("[^0-9]", "").cast(pl.Int64).is_between(2, 99998))
).with_columns([
    pl.col("SECO_CLEAN").str.replace_all("[^0-9]", "").cast(pl.Int32).alias("STAFF"),
    pl.lit("CARD           ").alias("PRODUCT"),
    pl.when(pl.col("TYPEC") == 3309)
    .then(1).otherwise(0).alias("C4CNT"),
    pl.when(pl.col("TYPEC") != 3309)
    .then(1).otherwise(0).alias("C5CNT")
])

# ==================== MERGE UCARD WITH HR FILES ====================
# Merge with HRBR
ucbr_df = ucard_df.join(hrbr_df, on="STAFF", how="inner")
ucexc_df = ucard_df.join(hrbr_df, on="STAFF", how="anti")

# Merge remaining with HRHO
ucho_df = ucexc_df.join(hrho_df, on="STAFF", how="inner")
ucexc1_df = ucexc_df.join(hrho_df, on="STAFF", how="anti")

# ==================== DEP PROCESSING ====================
dep_df = pl.read_parquet(DEP_FILE).with_columns([
    pl.when((1000000000 <= pl.col("ACCTNO")) & (pl.col("ACCTNO") <= 1999999999))
    .then(pl.lit("FIXED DEPOSITS    "))
    .when((7000000000 <= pl.col("ACCTNO")) & (pl.col("ACCTNO") <= 7999999999))
    .then(pl.lit("FIXED DEPOSITS    "))
    .when((3000000000 <= pl.col("ACCTNO")) & (pl.col("ACCTNO") <= 3999999999))
    .then(pl.lit("CURRENT ACCOUNT   "))
    .when((4000000000 <= pl.col("ACCTNO")) & (pl.col("ACCTNO") <= 4999999999))
    .then(pl.lit("SAVING ACCOUNT    "))
    .when((6000000000 <= pl.col("ACCTNO")) & (pl.col("ACCTNO") <= 6999999999))
    .then(pl.lit("SAVING ACCOUNT    "))
    .otherwise(pl.lit("")).alias("PRODUCT"),
    (pl.col("YTDBALS") / pl.col("NUMMTH")).round(2).alias("YTDBAL"),
    pl.lit("DEPO").alias("TAG")
])

# Merge with branch data
dep_df = dep_df.join(brh_df, on="BRANCH", how="left")

# ==================== SPLIT DEP INTO CATEGORIES ====================
# DPC1 - Category 1
dpc1_df = dep_df.filter(
    pl.col("PRIMOFF").is_not_null() &
    (pl.col("PRIMOFF") > 0) &
    (pl.col("XCORE") == "")
).with_columns([
    pl.col("PRIMOFF").alias("STAFF"),
    pl.lit("X").alias("NCORE"),
    pl.lit("CAT.1         ").alias("CATG"),
    pl.lit(1).alias("C1CNT"),
    pl.col("YTDBAL").alias("C1BAL")
])

# DPC2 - Category 2
dpc2_df = dep_df.filter(
    pl.col("SECNOFF").is_not_null() &
    (pl.col("SECNOFF") > 0) &
    pl.col("XCORE").is_in(["C", "N"])
).with_columns([
    pl.when(pl.col("SECNOFF") > 0)
    .then(pl.col("SECNOFF"))
    .otherwise(pl.col("PRIMOFF"))
    .alias("STAFF"),
    pl.when(pl.col("XCORE") == "C")
    .then(pl.lit("C"))
    .otherwise(pl.lit("N")).alias("NCORE"),
    pl.when(pl.col("XCORE") == "C")
    .then(pl.lit("CAT.2 CORE    "))
    .otherwise(pl.lit("CAT.2 NON-CORE")).alias("CATG"),
    pl.when(pl.col("XCORE") == "C")
    .then(1).otherwise(0).alias("C2CNT"),
    pl.when(pl.col("XCORE") == "C")
    .then(pl.col("YTDBAL")).otherwise(0).alias("C2BAL"),
    pl.when(pl.col("XCORE") == "N")
    .then(1).otherwise(0).alias("C3CNT"),
    pl.when(pl.col("XCORE") == "N")
    .then(pl.col("YTDBAL")).otherwise(0).alias("C3BAL")
])

# ==================== ELDS PROCESSING ====================
elds_df = pl.read_parquet(ELDS_FILE).filter(
    pl.col("STAFX1") != "*****"
).with_columns([
    pl.when((pl.col("STAFX1") >= "00001") & (pl.col("STAFX1") <= "99999"))
    .then(pl.lit("X"))
    .when((pl.col("STAFX2") >= "00001") & (pl.col("STAFX2") <= "99999"))
    .then(pl.lit("C"))
    .when((pl.col("STAFX3") >= "00001") & (pl.col("STAFX3") <= "99999"))
    .then(pl.lit("N"))
    .otherwise(None).alias("NCORE"),
    pl.lit("ELDS").alias("TAG"),
    pl.col("AANUM").str.slice(0, 3).alias("BRCHCD")
])

# Merge with branch data
elds_df = elds_df.join(brh_df, on="BRCHCD", how="left")

# Process ELDS categories
elds_cat1 = elds_df.filter(pl.col("NCORE") == "X").with_columns([
    pl.col("STAFX1").cast(pl.Int32).alias("STAFF"),
    pl.lit("CAT.1         ").alias("CATG"),
    pl.lit(1).alias("C1CNT"),
    pl.col("YTDBAL").alias("C1BAL")
])

elds_cat2c = elds_df.filter(pl.col("NCORE") == "C").with_columns([
    pl.col("STAFX2").cast(pl.Int32).alias("STAFF"),
    pl.lit("CAT.2 CORE    ").alias("CATG"),
    pl.lit(1).alias("C2CNT"),
    pl.col("YTDBAL").alias("C2BAL")
])

elds_cat2n = elds_df.filter(pl.col("NCORE") == "N").with_columns([
    pl.col("STAFX3").cast(pl.Int32).alias("STAFF"),
    pl.lit("CAT.2 NON-CORE").alias("CATG"),
    pl.lit(1).alias("C3CNT"),
    pl.col("YTDBAL").alias("C3BAL")
])

elds_combined = pl.concat([elds_cat1, elds_cat2c, elds_cat2n])

# ==================== COMBINE ALL DATA ====================
srs_df = pl.concat([elds_combined, dpc1_df, dpc2_df])

# ==================== MERGE WITH HR FILES ====================
# Merge with HRHO
srs_rho = srs_df.join(hrho_df, on="STAFF", how="inner")
srs_no_ho = srs_df.join(hrho_df, on="STAFF", how="anti")

# Split remaining data
srs_br = srs_no_ho.filter(pl.col("BRANCH") > 0)
srs_exc = srs_no_ho.filter(pl.col("BRANCH") <= 0)

# ==================== EXCEPTION REPORT ====================
def generate_exception_report():
    """Generate exception reports"""
    report_lines = ["\x0cEXCEPTION REPORT : UNMATCHED STAFF ID AGAINST HR FILE"]
    report_lines.append("=" * 80)
    
    # ELDS exceptions
    elds_exc = srs_exc.filter(pl.col("TAG") == "ELDS")
    if len(elds_exc) > 0:
        report_lines.append("ELDS Exceptions:")
        for row in elds_exc.select(["AANUM", "STAFF", "PRODUCT", "YTDBAL"]).iter_rows(named=True):
            report_lines.append(f"{row['AANUM']} | {row['STAFF']} | {row['PRODUCT']} | {row['YTDBAL']:,.2f}")
    
    # DEPO exceptions
    depo_exc = srs_exc.filter(pl.col("TAG") == "DEPO")
    if len(depo_exc) > 0:
        report_lines.append("\nDEPO Exceptions:")
        for row in depo_exc.select(["ACCTNO", "STAFF", "PRODUCT", "YTDBAL"]).iter_rows(named=True):
            report_lines.append(f"{row['ACCTNO']} | {row['STAFF']} | {row['PRODUCT']} | {row['YTDBAL']:,.2f}")
    
    # Write exception report
    exc_file = OUTPUT_PATH / f"EXCEPTION_REPORT_{RMM}{RYY}.txt"
    with open(exc_file, 'w') as f:
        for line in report_lines:
            f.write(line + '\n')
    
    print(f"Exception report generated: {exc_file}")

generate_exception_report()

# ==================== COMBINE WITH UCARD DATA ====================
srs_br = pl.concat([srs_br, ucbr_df])
srs_ho = pl.concat([srs_ho, ucho_df])

# ==================== SUMMARIZE DATA ====================
# Summarize branch data
srsbr_summary = srs_br.group_by(["BRCHCD", "STAFF", "PRODUCT", "NCORE"]).agg([
    pl.col("C1CNT").sum(),
    pl.col("C2CNT").sum(),
    pl.col("C3CNT").sum(),
    pl.col("C4CNT").sum(),
    pl.col("C5CNT").sum(),
    pl.col("C1BAL").sum(),
    pl.col("C2BAL").sum(),
    pl.col("C3BAL").sum()
])

# Summarize HO data
srsho_summary = srs_ho.group_by(["HOE", "STAFF", "PRODUCT", "NCORE"]).agg([
    pl.col("C1CNT").sum(),
    pl.col("C2CNT").sum(),
    pl.col("C3CNT").sum(),
    pl.col("C4CNT").sum(),
    pl.col("C5CNT").sum(),
    pl.col("C1BAL").sum(),
    pl.col("C2BAL").sum(),
    pl.col("C3BAL").sum()
])

# Save summaries
srsbr_summary.write_parquet(SCRD_PATH / f"SRSBR_{RMM}{RYY}.parquet")
srsho_summary.write_parquet(SCRD_PATH / f"SRSHO_{RMM}{RYY}.parquet")

# ==================== COMBINE AND FINAL SUMMARIZE ====================
srss_df = pl.concat([srs_ho, srs_br])

# Final summary by STAFF, PRODUCT, NCORE
srss_summary = srss_df.group_by(["STAFF", "PRODUCT", "NCORE"]).agg([
    pl.col("C1CNT").sum(),
    pl.col("C2CNT").sum(),
    pl.col("C3CNT").sum(),
    pl.col("C4CNT").sum(),
    pl.col("C5CNT").sum(),
    pl.col("C1BAL").sum(),
    pl.col("C2BAL").sum(),
    pl.col("C3BAL").sum()
])

# Clean null values
srss_summary = srss_summary.with_columns([
    pl.col("C1BAL").fill_null(0),
    pl.col("C2BAL").fill_null(0),
    pl.col("C3BAL").fill_null(0),
    pl.col("C1CNT").fill_null(0),
    pl.col("C2CNT").fill_null(0),
    pl.col("C3CNT").fill_null(0),
    pl.col("C4CNT").fill_null(0),
    pl.col("C5CNT").fill_null(0)
])

# Add S-counters
srss_summary = srss_summary.with_columns([
    pl.when((pl.col("NCORE") == "X") & (pl.col("C1CNT") > 0))
    .then(1).otherwise(0).alias("S1CNT"),
    pl.when((pl.col("NCORE") == "C") & (pl.col("C2CNT") > 0))
    .then(1).otherwise(0).alias("S2CNT"),
    pl.when((pl.col("NCORE") == "N") & (pl.col("C3CNT") > 0))
    .then(1).otherwise(0).alias("S3CNT"),
    pl.when((pl.col("NCORE") == "N") & (pl.col("C4CNT") > 0))
    .then(1).otherwise(0).alias("S4CNT"),
    pl.when((pl.col("NCORE") == "N") & (pl.col("C5CNT") > 0))
    .then(1).otherwise(0).alias("S5CNT")
])

# ==================== FINAL PRODUCT SUMMARY ====================
srsp_summary = srss_summary.group_by("PRODUCT").agg([
    pl.col("S1CNT").sum(),
    pl.col("C1CNT").sum(),
    pl.col("C1BAL").sum(),
    pl.col("S2CNT").sum(),
    pl.col("C2CNT").sum(),
    pl.col("C2BAL").sum(),
    pl.col("S3CNT").sum(),
    pl.col("C3CNT").sum(),
    pl.col("C3BAL").sum(),
    pl.col("S4CNT").sum(),
    pl.col("C4CNT").sum(),
    pl.col("S5CNT").sum(),
    pl.col("C5CNT").sum()
])

# ==================== OUTPUT FINAL REPORT ====================
# Generate formatted output
output_file = OUTPUT_PATH / f"SRSP_{RMM}{RYY}.txt"
with open(output_file, 'w') as f:
    f.write(f"Report Date: {RDATE}\n")
    f.write(f"Month: {RMON}\n")
    f.write("=" * 100 + "\n")
    
    for row in srsp_summary.sort("PRODUCT").iter_rows(named=True):
        line = (f"{row['PRODUCT']};"
                f"{row['S1CNT']};{row['C1CNT']};{row['C1BAL']:.2f};"
                f"{row['S2CNT']};{row['C2CNT']};{row['C2BAL']:.2f};"
                f"{row['S3CNT']};{row['C3CNT']};{row['C3BAL']:.2f};"
                f"{row['S4CNT']};{row['C4CNT']};"
                f"{row['S5CNT']};{row['C5CNT']}")
        f.write(line + '\n')
    
    f.write("=" * 100 + "\n")
    f.write(f"Total Records Processed: {len(srss_summary)}\n")

print(f"Summary report generated: {output_file}")
conn.close()
