import polars as pl
import duckdb
from datetime import datetime
from pathlib import Path

# ==================== SETUP ====================
BASE_PATH = Path("/path/to/data")
LIMIT_PATH = BASE_PATH / "limit"
ILIMIT_PATH = BASE_PATH / "ilimit"
OUTPUT_PATH = BASE_PATH / "output"

conn = duckdb.connect()

# ==================== REPTDATE PROCESSING ====================
def process_reptdate():
    """Process report date from DPLIMIT file"""
    # Assuming DPLIMIT.parquet exists with TBDATE column
    dplimit_df = pl.read_parquet(BASE_PATH / "DPLIMIT.parquet").head(1)
    tbdate_val = dplimit_df[0, "TBDATE"]
    
    # Convert packed decimal to date
    tbdate_str = str(tbdate_val).zfill(11)[:8]
    reptdate_dt = datetime.strptime(tbdate_str[:6] + tbdate_str[6:8], "%m%d%Y").date()
    
    day_val = reptdate_dt.day
    
    # Determine week
    if 1 <= day_val <= 8:
        NOWK = "1"
    elif 9 <= day_val <= 15:
        NOWK = "2"
    elif 16 <= day_val <= 22:
        NOWK = "3"
    else:
        NOWK = "4"
    
    REPTYEAR = str(reptdate_dt.year)
    REPTMON = f"{reptdate_dt.month:02d}"
    REPTDAY = f"{day_val:02d}"
    RDATE = reptdate_dt.strftime("%d%m%Y")
    
    # Save report dates
    reptdate_data = pl.DataFrame({"REPTDATE": [reptdate_dt], "TBDATE": [tbdate_val]})
    reptdate_data.write_parquet(LIMIT_PATH / "REPTDATE.parquet")
    reptdate_data.write_parquet(ILIMIT_PATH / "REPTDATE.parquet")
    
    return NOWK, REPTYEAR, REPTMON, REPTDAY, RDATE

NOWK, REPTYEAR, REPTMON, REPTDAY, RDATE = process_reptdate()

# ==================== COLUMN DEFINITIONS ====================
LIMIT_COLS = [
    "ACCTNO", "BRANCH", "LMTID", "LMTDESC", "LMTINDEX", "LMTBASER",
    "LMTADJF", "LMTRATE", "LMTTYPE", "LMTAMT", "LMTCOLL", "NAME",
    "PRODUCT", "OPENIND", "STATE", "RISKCODE", "EXCESSDT", "TODDATE",
    "ODSTATUS", "APPRLIMT", "REVIEWDT", "LMTSTART", "LMTENDDT",
    "LMTTERM", "LMTTRMID", "LMTINCR", "ODTEMADJ", "ODFEECTD",
    "ODLMTDTE", "ODLMTDD", "ODLMTMM", "ODLMTYY", "INTERDUE",
    "COMMFERT", "LINEFEWV", "LFEEYTD", "INTBLYTD", "ODLMTDTE",
    "ODFDIND", "FDACNO", "FDRCNO", "COSTCTR", "CURODDT", "AUTOPRIC",
    "CRISPURP", "CRRINI", "CRRNOW", "ODBASERT", "ODTEMPADJ",
    "REFIN_FLG", "OLD_FI_CD", "OLD_MACC_NO", "OLD_SUBACC_NO",
    "ASCORE_PERM", "ASCORE_LTST", "ASCORE_COMM"
]

ODREV_COLS = ["ACCTNO", "INTINCGR", "AVRCRBAL"]
LMTEX_COLS = [
    "ACCTNO", "LMTID", "CLF_EXP_DT", "CLIMATE_PRIN_TAXONOMY_CLASS",
    "CLIMATE_MITIGATE_GP1_FLG", "CLIMATE_ADAPT_GP2_FLG",
    "CLIMATE_ENVIRONMT_GP3_FLG", "CLIMATE_TRANSITION_GP4_FLG",
    "CLIMATE_PROHIBIT_GP5_FLG", "CLF_WAKLH_FLG"
]

# ==================== ODREVIEW PROCESSING ====================
print("Processing ODREVIEW data...")
odrev_df = pl.read_parquet(BASE_PATH / "ODREVIEW.parquet").with_columns([
    pl.when(pl.col("DAYINMTH") > 0)
    .then(pl.col("LEDBLMTD") / pl.col("DAYINMTH"))
    .otherwise(None).alias("AVRCRBAL")
]).select(ODREV_COLS).sort("ACCTNO")

# ==================== LIMIT DATA PROCESSING ====================
print("Processing LIMIT data...")
limit_df = pl.read_parquet(BASE_PATH / "DPLIMIT.parquet").tail(-1)  # Skip first row

# Clean and transform columns
limit_df = limit_df.with_columns([
    pl.col("CRRINI").str.replace_all(r"[^\d]", "").alias("CRRINI_CLEAN"),
    pl.col("CRRNOW").str.replace_all(r"[^\d]", "").alias("CRRNOW_CLEAN"),
    pl.col("ASCORE_PERM").str.replace_all(r"[^\x00-\x7F]", "[]").alias("ASCORE_PERM_CLEAN"),
    pl.col("ASCORE_LTST").str.replace_all(r"[^\x00-\x7F]", "[]").alias("ASCORE_LTST_CLEAN"),
    pl.col("ASCORE_COMM").str.replace_all(r"[^\x00-\x7F]", "[]").alias("ASCORE_COMM_CLEAN"),
    pl.when(pl.col("FDACNO") > 0)
    .then(pl.lit("Y"))
    .otherwise(None).alias("ODFDIND"),
    pl.col("LOANPURP").cast(pl.Utf8).alias("CRISPURP")
]).filter(pl.col("BRANCH") != 0)

# Rename cleaned columns
limit_df = limit_df.rename({
    "CRRINI_CLEAN": "CRRINI",
    "CRRNOW_CLEAN": "CRRNOW", 
    "ASCORE_PERM_CLEAN": "ASCORE_PERM",
    "ASCORE_LTST_CLEAN": "ASCORE_LTST",
    "ASCORE_COMM_CLEAN": "ASCORE_COMM"
}).drop("LOANPURP")

# Select required columns
available_limit_cols = [col for col in LIMIT_COLS if col in limit_df.columns]
limit_df = limit_df.select(available_limit_cols).sort(["ACCTNO", "LMTID"])

# ==================== LIMITEX PROCESSING ====================
print("Processing LIMITEX data...")
lmtex_df = pl.read_parquet(BASE_PATH / "DPLMTEX.parquet").with_columns([
    pl.when(pl.col("CLFEXPDT").is_not_null())
    .then(pl.col("CLFEXPDT").cast(pl.Utf8).str.zfill(11).str.slice(0, 8))
    .then(pl.col("CLFEXPDT").str.slice(0, 8).str.strptime(pl.Date, "%m%d%Y"))
    .otherwise(None).alias("CLF_EXP_DT")
]).select(LMTEX_COLS).sort(["ACCTNO", "LMTID"])

# ==================== MERGE LIMIT WITH LIMITEX ====================
limit_df = limit_df.join(lmtex_df, on=["ACCTNO", "LMTID"], how="left")

# Convert LMTSTART to date
limit_df = limit_df.with_columns([
    pl.when(pl.col("LMTSTART") > 0)
    .then(pl.col("LMTSTART").cast(pl.Utf8).str.zfill(11).str.slice(0, 8))
    .then(pl.col("LMTSTART").str.slice(0, 8).str.strptime(pl.Date, "%m%d%Y"))
    .otherwise(None).alias("LMTSTDTE")
])

# ==================== CREATE OVERDFT DATASET ====================
print("Creating OVERDFT dataset...")
overdft_df = limit_df.join(odrev_df, on="ACCTNO", how="left").with_columns([
    pl.col("INTINCGR").fill_null(0)
])

# Save OVERDFT
overdft_df.write_parquet(LIMIT_PATH / "OVERDFT.parquet")

# ==================== CREATE LIMITS SUMMARY ====================
limits_summary = overdft_df.group_by("ACCTNO").agg([
    pl.col("LMTAMT").sum().alias("LMTAMT")
])
limits_summary.write_parquet(LIMIT_PATH / "LIMITS.parquet")

# ==================== GP3 PROCESSING ====================
print("Processing GP3 data...")
gp3_file = BASE_PATH / "GP3PRO.parquet"
if gp3_file.exists():
    gp3_df = pl.read_parquet(gp3_file)
    
    # Process GP3 data based on RFIELD
    gp31 = gp3_df.filter(pl.col("RFIELD") == "EXCDTE").select(["ACCTNO", "DATE"]).with_columns([
        (pl.col("DATE").cast(pl.Utf8).str.zfill(8).str.slice(2, 2) +
         pl.col("DATE").cast(pl.Utf8).str.zfill(8).str.slice(0, 2) +
         pl.col("DATE").cast(pl.Utf8).str.zfill(8).str.slice(4, 4) + "000").alias("EXCESSDT")
    ]).select(["ACCTNO", "EXCESSDT"])
    
    gp32 = gp3_df.filter(pl.col("RFIELD") == "TMPDTE").select(["ACCTNO", "TDATE"]).with_columns([
        (pl.col("TDATE").cast(pl.Utf8).str.zfill(8).str.slice(2, 2) +
         pl.col("TDATE").cast(pl.Utf8).str.zfill(8).str.slice(0, 2) +
         pl.col("TDATE").cast(pl.Utf8).str.zfill(8).str.slice(4, 4) + "000").alias("TODDATE")
    ]).select(["ACCTNO", "TODDATE"])
    
    gp33 = gp3_df.filter(pl.col("RFIELD") == "USRCD4").select(["ACCTNO", "RISKCODE"])
    gp34 = gp3_df.filter(pl.col("RFIELD") == "ODSTAT").select(["ACCTNO", "ODSTATUS"])
    
    # Merge all GP3 data
    gp3_merged = gp31.join(gp32, on="ACCTNO", how="outer")
    gp3_merged = gp3_merged.join(gp33, on="ACCTNO", how="outer")
    gp3_merged = gp3_merged.join(gp34, on="ACCTNO", how="outer")
    
    gp3_merged = gp3_merged.unique(subset=["ACCTNO"])
    gp3_merged.write_parquet(LIMIT_PATH / "GP3.parquet")
else:
    gp3_merged = pl.DataFrame()

# ==================== ISLAMIC DATA SPLITTING ====================
print("Splitting Islamic data...")
islamic_df = overdft_df.filter(
    (3000 <= pl.col("COSTCTR")) & (pl.col("COSTCTR") <= 4999)
)
conventional_df = overdft_df.filter(
    ~((3000 <= pl.col("COSTCTR")) & (pl.col("COSTCTR") <= 4999))
)

# Save Islamic data
islamic_df.write_parquet(ILIMIT_PATH / "OVERDFT.parquet")

# Create Islamic limits summary
islamic_limits = islamic_df.group_by("ACCTNO").agg([
    pl.col("LMTAMT").sum().alias("LMTAMT")
])
islamic_limits.write_parquet(ILIMIT_PATH / "LIMITS.parquet")

# ==================== GP3 FOR ISLAMIC ====================
if len(gp3_merged) > 0:
    islamic_accounts = islamic_df.select(["ACCTNO"]).unique()
    islamic_gp3 = islamic_accounts.join(gp3_merged, on="ACCTNO", how="inner").unique(subset=["ACCTNO"])
    islamic_gp3.write_parquet(ILIMIT_PATH / "GP3.parquet")

# ==================== MONTHLY UPDATE LOGIC ====================
print(f"Applying weekly logic (Week: {NOWK})...")
if NOWK == "4":
    # Week 4 - Update with GP3 data
    if len(gp3_merged) > 0:
        # Update conventional OVERDFT
        updated_overdft = overdft_df.join(
            gp3_merged.select(["ACCTNO", "RISKCODE", "ODSTATUS", "EXCESSDT", "TODDATE"]),
            on="ACCTNO",
            how="left",
            suffix="_gp3"
        ).with_columns([
            pl.when(pl.col("RISKCODE_gp3").is_not_null())
            .then(pl.col("RISKCODE_gp3"))
            .otherwise(pl.col("RISKCODE")).alias("RISKCODE"),
            pl.when(pl.col("ODSTATUS_gp3").is_not_null())
            .then(pl.col("ODSTATUS_gp3"))
            .otherwise(pl.col("ODSTATUS")).alias("ODSTATUS"),
            pl.when(pl.col("EXCESSDT_gp3").is_not_null())
            .then(pl.col("EXCESSDT_gp3"))
            .otherwise(pl.col("EXCESSDT")).alias("EXCESSDT"),
            pl.when(pl.col("TODDATE_gp3").is_not_null())
            .then(pl.col("TODDATE_gp3"))
            .otherwise(pl.col("TODDATE")).alias("TODDATE")
        ]).drop([f"{col}_gp3" for col in ["RISKCODE", "ODSTATUS", "EXCESSDT", "TODDATE"]])
        
        updated_overdft.write_parquet(LIMIT_PATH / "OVERDFT.parquet")
        
        # Update Islamic OVERDFT if GP3 data exists
        if len(islamic_gp3) > 0:
            updated_islamic = islamic_df.join(
                islamic_gp3.select(["ACCTNO", "RISKCODE", "ODSTATUS", "EXCESSDT", "TODDATE"]),
                on="ACCTNO",
                how="left",
                suffix="_gp3"
            ).with_columns([
                pl.when(pl.col("RISKCODE_gp3").is_not_null())
                .then(pl.col("RISKCODE_gp3"))
                .otherwise(pl.col("RISKCODE")).alias("RISKCODE"),
                pl.when(pl.col("ODSTATUS_gp3").is_not_null())
                .then(pl.col("ODSTATUS_gp3"))
                .otherwise(pl.col("ODSTATUS")).alias("ODSTATUS")
            ]).drop([f"{col}_gp3" for col in ["RISKCODE", "ODSTATUS", "EXCESSDT", "TODDATE"]])
            
            updated_islamic.write_parquet(ILIMIT_PATH / "OVERDFT.parquet")
else:
    # Other weeks - clear GP3 data
    if (LIMIT_PATH / "GP3.parquet").exists():
        (LIMIT_PATH / "GP3.parquet").unlink()
    if (ILIMIT_PATH / "GP3.parquet").exists():
        (ILIMIT_PATH / "GP3.parquet").unlink()

# ==================== GENERATE SAMPLE REPORTS ====================
print("Generating sample reports...")

# Sample OVERDFT report
sample_file = OUTPUT_PATH / f"OVERDFT_SAMPLE_{REPTMON}{NOWK}{REPTYEAR}.txt"
with open(sample_file, 'w') as f:
    f.write(f"Sample OVERDFT Data - First 100 Records\n")
    f.write(f"Report Date: {RDATE}\n")
    f.write("=" * 120 + "\n")
    
    sample_data = overdft_df.head(100)
    for row in sample_data.iter_rows(named=True):
        f.write(str(row) + "\n")

# Report date info
date_file = OUTPUT_PATH / f"REPDATE_INFO_{REPTMON}{NOWK}{REPTYEAR}.txt"
with open(date_file, 'w') as f:
    f.write(f"Report Date Information\n")
    f.write("=" * 50 + "\n")
    f.write(f"Week: {NOWK}\n")
    f.write(f"Month: {REPTMON}\n")
    f.write(f"Year: {REPTYEAR}\n")
    f.write(f"Output Files:\n")
    f.write(f"  Conventional: {len(overdft_df)} records\n")
    f.write(f"  Islamic: {len(islamic_df)} records\n")

print("Processing complete!")
print(f"Week: {NOWK}, Month: {REPTMON}, Year: {REPTYEAR}")
print(f"Conventional records: {len(overdft_df)}")
print(f"Islamic records: {len(islamic_df)}")

conn.close()
