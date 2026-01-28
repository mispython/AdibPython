import polars as pl
from pathlib import Path

# ==================== SETUP ====================
BASE_PATH = Path("/path/to/data")
DEPOSIT_PATH = BASE_PATH / "deposit"
LIMIT_PATH = BASE_PATH / "limit"
LOAN_PATH = BASE_PATH / "loan"
OUTPUT_PATH = BASE_PATH / "output"

# ==================== REPTDATE PROCESSING ====================
print("Processing report date...")
reptdate_df = pl.read_parquet(DEPOSIT_PATH / "REPTDATE.parquet")
reptdate_val = reptdate_df[0, "REPTDATE"]

day_val = reptdate_val.day
if 1 <= day_val <= 8:
    NOWK = "1"
elif 9 <= day_val <= 15:
    NOWK = "2"
elif 16 <= day_val <= 22:
    NOWK = "3"
else:
    NOWK = "4"

REPTYEAR = str(reptdate_val.year)
REPTMON = f"{reptdate_val.month:02d}"
REPTDAY = f"{day_val:02d}"
RDATE = reptdate_val.strftime("%d%m%Y")

print(f"Month: {REPTMON}, Week: {NOWK}, Year: {REPTYEAR}")

# ==================== PROCESS GP3 DATA ====================
print("Processing GP3 data...")
gp3_df = pl.read_parquet(LIMIT_PATH / "GP3.parquet")

# Transform GP3 data
gp3_df = gp3_df.with_columns([
    pl.col("RISKCODE").cast(pl.Int64, strict=False).alias("RISKRTE"),
    pl.col("EXCESSDT").alias("EXODDATE"),
    pl.col("TODDATE").alias("TEMPODDT"),
    pl.col("ODSTATUS").alias("ODSTAT")
])

# Apply RISKCD format (simplified)
def format_riskcd(riskcode):
    risk_mapping = {
        '1': 'NORMAL',
        '2': 'SPECIAL MENTION',
        '3': 'SUBSTANDARD',
        '4': 'DOUBTFUL',
        '5': 'BAD',
        '6': 'BAD & LITIGATION',
        '7': 'BAD & LEGAL',
        '8': 'BAD & LITIGATION',
        '9': 'BAD & LEGAL'
    }
    return risk_mapping.get(str(riskcode), str(riskcode))

gp3_df = gp3_df.with_columns([
    pl.col("RISKCODE").map_elements(format_riskcd, return_dtype=pl.Utf8).alias("RISKCD")
])

gp3_df = gp3_df.sort("ACCTNO")

# ==================== UPDATE LOAN DATA ====================
print("Updating LOAN data...")
loan_file = LOAN_PATH / f"LOAN{REPTMON}{NOWK}.parquet"
if loan_file.exists():
    loan_df = pl.read_parquet(loan_file).sort("ACCTNO")
    
    # Update with RISKRTE and RISKCD from GP3
    loan_updated = loan_df.join(
        gp3_df.select(["ACCTNO", "RISKRTE", "RISKCD"]),
        on="ACCTNO",
        how="left"
    ).with_columns([
        pl.when(pl.col("RISKRTE_right").is_not_null())
        .then(pl.col("RISKRTE_right"))
        .otherwise(pl.col("RISKRTE_left"))
        .alias("RISKRTE"),
        pl.when(pl.col("RISKCD_right").is_not_null())
        .then(pl.col("RISKCD_right"))
        .otherwise(pl.col("RISKCD_left"))
        .alias("RISKCD")
    ]).drop(["RISKRTE_left", "RISKRTE_right", "RISKCD_left", "RISKCD_right"])
    
    # Save updated loan data
    loan_updated.write_parquet(loan_file)
    print(f"Updated LOAN data saved: {loan_file}")
else:
    print(f"Warning: LOAN file not found: {loan_file}")
    loan_updated = None

# ==================== UPDATE CURRENT ACCOUNTS ====================
print("Updating CURRENT accounts...")
current_df = pl.read_parquet(DEPOSIT_PATH / "CURRENT.parquet")

# Update with GP3 fields
current_updated = current_df.join(
    gp3_df.select(["ACCTNO", "EXODDATE", "TEMPODDT", "ODSTAT", "RISKCODE"]),
    on="ACCTNO",
    how="left"
).with_columns([
    pl.when(pl.col("EXODDATE_right").is_not_null())
    .then(pl.col("EXODDATE_right"))
    .otherwise(pl.col("EXODDATE_left"))
    .alias("EXODDATE"),
    pl.when(pl.col("TEMPODDT_right").is_not_null())
    .then(pl.col("TEMPODDT_right"))
    .otherwise(pl.col("TEMPODDT_left"))
    .alias("TEMPODDT"),
    pl.when(pl.col("ODSTAT_right").is_not_null())
    .then(pl.col("ODSTAT_right"))
    .otherwise(pl.col("ODSTAT_left"))
    .alias("ODSTAT"),
    pl.when(pl.col("RISKCODE_right").is_not_null())
    .then(pl.col("RISKCODE_right"))
    .otherwise(pl.col("RISKCODE_left"))
    .alias("RISKCODE")
]).drop([f"{col}_left" for col in ["EXODDATE", "TEMPODDT", "ODSTAT", "RISKCODE"]] + 
        [f"{col}_right" for col in ["EXODDATE", "TEMPODDT", "ODSTAT", "RISKCODE"]])

# Save updated current accounts
current_updated.write_parquet(DEPOSIT_PATH / "CURRENT.parquet")
print(f"Updated CURRENT data saved")

# ==================== PROCESS OVERDRAFT DATA ====================
print("Processing overdraft data...")

# Filter current accounts with negative balance
current_negative = current_updated.filter(
    (~pl.col("OPENIND").is_in(["B", "C", "P"])) &
    (pl.col("CURBAL") < 0)
).select(["ACCTNO"]).sort("ACCTNO")

# Load OVERDFT data
overdft_file = LIMIT_PATH / "OVERDFT.parquet"
if overdft_file.exists():
    overdft_df = pl.read_parquet(overdft_file)
    
    # Get unique APPRLIMT values
    overdft_unique = overdft_df.select(["ACCTNO", "APPRLIMT"]).unique(subset=["ACCTNO"])
    overdft_unique = overdft_unique.rename({"APPRLIMT": "APPRLIM2"})
    
    # Merge with negative current accounts
    overdft_merged = current_negative.join(
        overdft_unique,
        on="ACCTNO",
        how="left"
    ).select(["ACCTNO", "APPRLIM2"])
    
    print(f"Overdraft records: {len(overdft_merged)}")
    
    # ==================== UPDATE LOAN WITH OVERDRAFT LIMITS ====================
    if loan_updated is not None:
        print("Updating LOAN with overdraft limits...")
        
        # Merge overdraft limits into loan data
        loan_final = loan_updated.join(
            overdft_merged,
            on="ACCTNO",
            how="left"
        ).with_columns([
            pl.col("APPRLIM2").fill_null(0)
        ])
        
        # Save final loan data
        loan_final.write_parquet(loan_file)
        print(f"Final LOAN data with overdraft limits saved: {loan_file}")
        
        # Save overdraft data for reference
        overdft_merged.write_parquet(OUTPUT_PATH / f"OVERDFT_{REPTMON}{NOWK}_{REPTYEAR}.parquet")
else:
    print(f"Warning: OVERDFT file not found: {overdft_file}")

# ==================== GENERATE SUMMARY REPORT ====================
print("\nGenerating summary report...")
summary_file = OUTPUT_PATH / f"FGP3_SUMMARY_{REPTMON}{NOWK}_{REPTYEAR}.txt"

with open(summary_file, 'w') as f:
    f.write("EIBMFGP3 PROCESSING SUMMARY\n")
    f.write("=" * 60 + "\n")
    f.write(f"Report Date: {reptdate_val.strftime('%d/%m/%Y')}\n")
    f.write(f"Week: {NOWK}, Month: {REPTMON}, Year: {REPTYEAR}\n")
    f.write("-" * 60 + "\n")
    
    f.write(f"GP3 Records Processed: {len(gp3_df):,}\n")
    
    if current_updated is not None:
        f.write(f"CURRENT Accounts: {len(current_updated):,}\n")
        negative_count = len(current_negative)
        f.write(f"  Negative Balance Accounts: {negative_count:,}\n")
    
    if loan_updated is not None:
        f.write(f"LOAN Records: {len(loan_updated):,}\n")
    
    # RISKCODE distribution
    if len(gp3_df) > 0 and "RISKCODE" in gp3_df.columns:
        f.write("\nRISKCODE Distribution:\n")
        risk_dist = gp3_df.group_by("RISKCODE").agg(pl.len().alias("COUNT")).sort("RISKCODE")
        for row in risk_dist.iter_rows(named=True):
            risk_desc = format_riskcd(row["RISKCODE"])
            f.write(f"  {row['RISKCODE']} ({risk_desc}): {row['COUNT']:>8,}\n")

print(f"Summary report saved: {summary_file}")
print("\nProcessing complete!")
