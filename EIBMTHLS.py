"""
SAS EIBMTHLS conversion to Python - Short version
Outputs parquet files with same logic
"""

from pathlib import Path
from datetime import date
import polars as pl
import duckdb

# Setup paths
BASE_PATH = Path("./data")
INPUT_PATH = BASE_PATH / "input"
OUTPUT_PATH = BASE_PATH / "output"

# Create directories
OUTPUT_PATH.mkdir(parents=True, exist_ok=True)

# ============================================================================
# 1. REPTDATE Calculation
# ============================================================================
today = date.today()
REPTDATE = date(today.year, today.month, 1)  # First day of current month
REPTMON = f"{REPTDATE.month:02d}"
REPTDAY = f"{REPTDATE.day:02d}"
REPTDT = f"{REPTDATE.year % 100:02d}{REPTDATE.month:02d}{REPTDATE.day:02d}"

print(f"Report Date: {REPTDATE} ({REPTDT})")

# ============================================================================
# 2. BIC List Definition
# ============================================================================
BIC_LIST = [
    '57210', '57299', '57510', '57599', '57610', '57620', '57631',
    '57632', '57633', '57639', '57711', '57712', '57720', '57730',
    '57799', '57110', '57120', '57130', '57200', '57410', '57420',
    '57430', '57500', '68504', '78504'
]

# ============================================================================
# 3. Load and Filter FOREX Data
# ============================================================================
def load_forex_data():
    """Load K1TBL and DCITBL data, filter by BIC codes"""
    conn = duckdb.connect()
    
    # Build list for SQL IN clause
    bic_sql = "('" + "','".join(BIC_LIST) + "')"
    
    # Read both files and filter
    k1_file = INPUT_PATH / f"K1TBL{REPTMON}.parquet"
    dci_file = INPUT_PATH / f"DCITBL{REPTMON}.parquet"
    
    query = f"""
    SELECT * FROM (
        SELECT * FROM read_parquet('{k1_file}')
        UNION ALL
        SELECT * FROM read_parquet('{dci_file}')
    )
    WHERE SUBSTR(BNMCODE, 1, 5) IN {bic_sql}
    """
    
    df = conn.execute(query).pl().sort("BNMCODE")
    conn.close()
    
    return df

# Load data
forex_df = load_forex_data()
print(f"Loaded {len(forex_df)} records")

# ============================================================================
# 4. Apply SAS Transformations
# ============================================================================
def apply_transformations(df):
    """Apply all SAS DATA step transformations"""
    
    # ABS(AMOUNT)
    df = df.with_columns(pl.col("AMOUNT").abs().alias("AMOUNT_ABS"))
    
    # DCIAMT logic: if missing or 0, use AMOUNT
    if "DCIAMT" in df.columns:
        df = df.with_columns(
            pl.when((pl.col("DCIAMT").is_null()) | (pl.col("DCIAMT") == 0))
            .then(pl.col("AMOUNT"))
            .otherwise(pl.col("DCIAMT"))
            .alias("DCIAMT_ADJ")
        )
    else:
        df = df.with_columns(pl.col("AMOUNT").alias("DCIAMT_ADJ"))
    
    # Add RECNO (record number within BNMCODE group)
    df = df.with_columns(
        pl.col("BNMCODE").rank("dense").over("BNMCODE").alias("RECNO")
    )
    
    # Add TOTAMT (running total within BNMCODE group)
    df = df.with_columns(
        pl.col("AMOUNT_ABS").cum_sum().over("BNMCODE").alias("TOTAMT")
    )
    
    # Add FIRST/LAST indicators
    df = df.with_columns([
        (pl.col("RECNO") == 1).alias("FIRST_BNMCODE"),
        (pl.col("RECNO") == pl.col("RECNO").max().over("BNMCODE")).alias("LAST_BNMCODE")
    ])
    
    # Add DCDLP type indicator
    df = df.with_columns(
        pl.when(pl.col("DCDLP").is_in(["DCI", "RFO", "TFO", "PFO"]))
        .then(pl.lit("DCI_TYPE"))
        .otherwise(pl.lit("OTHER_TYPE"))
        .alias("RECORD_TYPE")
    )
    
    # Add PRN logic for BNMCODE starting with 68504
    df = df.with_columns(
        pl.when(pl.col("BNMCODE").str.starts_with("68504"))
        .then(pl.lit(1))
        .otherwise(pl.lit(0))
        .alias("PRN_FLAG")
    )
    
    return df

# Apply transformations
forex_transformed = apply_transformations(forex_df)
print(f"Applied transformations")

# ============================================================================
# 5. Split Data by Record Type
# ============================================================================
dci_data = forex_transformed.filter(pl.col("RECORD_TYPE") == "DCI_TYPE")
other_data = forex_transformed.filter(pl.col("RECORD_TYPE") == "OTHER_TYPE")

print(f"DCI records: {len(dci_data)}, Other records: {len(other_data)}")

# ============================================================================
# 6. Create Output DataFrames
# ============================================================================
def create_output_df(df, record_type):
    """Create output DataFrame with required columns"""
    
    # Common columns for both types
    base_cols = {
        "BNMCODE": pl.col("BNMCODE"),
        "RECNO": pl.col("RECNO"),
        "AMOUNT_ABS": pl.col("AMOUNT_ABS"),
        "TOTAMT": pl.col("TOTAMT"),
        "FIRST_BNMCODE": pl.col("FIRST_BNMCODE"),
        "LAST_BNMCODE": pl.col("LAST_BNMCODE"),
        "RECORD_TYPE": pl.lit(record_type)
    }
    
    if record_type == "DCI_TYPE":
        # DCI-type specific columns
        output_cols = base_cols.copy()
        output_cols.update({
            "REPTDATE": pl.lit(REPTDATE),
            "DCDLP": pl.col("DCDLP"),
            "DCDLR": pl.col("DCDLR"),
            "DCCUS": pl.col("DCCUS"),
            "DCCLC": pl.col("DCCLC"),
            "DCBCCY": pl.col("DCBCCY"),
            "DCIAMT_ADJ": pl.col("DCIAMT_ADJ"),
            "C8SPT": pl.col("C8SPT"),
            "DCACCY": pl.col("DCACCY"),
            "GFCTP": pl.col("GFCTP"),
            "GFCUN": pl.col("GFCUN"),
            "GFCNAL": pl.col("GFCNAL"),
            "DCTRNT": pl.col("DCTRNT"),
            "DCBSI": pl.col("DCBSI"),
            "ODGRP": pl.col("ODGRP"),
            "STATUS": pl.col("STATUS"),
            "DCCTRD": pl.col("DCCTRD"),
            "DCMTYD": pl.col("DCMTYD"),
            "CUSTCD": pl.col("CUSTCD"),
            "OPTYPE": pl.col("OPTYPE"),
            "STKRATE": pl.col("STKRATE")
        })
    else:
        # Other-type specific columns
        output_cols = base_cols.copy()
        output_cols.update({
            "GWBALA": pl.col("GWBALA"),
            "GWSHN": pl.col("GWSHN"),
            "GWCTP": pl.col("GWCTP"),
            "GWCNAL": pl.col("GWCNAL"),
            "GWCCY": pl.col("GWCCY"),
            "GWCNAP": pl.col("GWCNAP"),
            "GWDLP": pl.col("GWDLP"),
            "GWDLR": pl.col("GWDLR"),
            "GWSDT": pl.col("GWSDT"),
            "GWMVT": pl.col("GWMVT"),
            "GWMVTS": pl.col("GWMVTS"),
            "GWC2R": pl.col("GWC2R"),
            "GWOCY": pl.col("GWOCY"),
            "GWCBD": pl.col("GWCBD")
        })
    
    # Select columns, fill missing with appropriate defaults
    result_df = df.select([
        output_cols.get(col, pl.lit(None).alias(col))
        for col in output_cols
    ])
    
    return result_df

# Create output DataFrames
dci_output = create_output_df(dci_data, "DCI_TYPE")
other_output = create_output_df(other_data, "OTHER_TYPE")

# Combined output
combined_output = pl.concat([dci_output, other_output], how="vertical")

# ============================================================================
# 7. Write Parquet Files
# ============================================================================
# Main processed data
forex_transformed.write_parquet(OUTPUT_PATH / "FOREX_PROCESSED.parquet")

# Split by type
dci_output.write_parquet(OUTPUT_PATH / "FOREX_DCI_TYPE.parquet")
other_output.write_parquet(OUTPUT_PATH / "FOREX_OTHER_TYPE.parquet")

# Combined
combined_output.write_parquet(OUTPUT_PATH / "FOREX_COMBINED.parquet")

# Summary by BNMCODE
summary = forex_transformed.group_by("BNMCODE").agg([
    pl.count().alias("RECORD_COUNT"),
    pl.sum("AMOUNT_ABS").alias("TOTAL_AMOUNT"),
    pl.first("RECORD_TYPE").alias("RECORD_TYPE")
])
summary.write_parquet(OUTPUT_PATH / "FOREX_SUMMARY.parquet")

# Metadata
metadata = pl.DataFrame({
    "VARIABLE": ["REPTDATE", "REPTMON", "REPTDAY", "REPTDT"],
    "VALUE": [str(REPTDATE), REPTMON, REPTDAY, REPTDT]
})
metadata.write_parquet(OUTPUT_PATH / "METADATA.parquet")

# ============================================================================
# 8. Summary
# ============================================================================
print("\n" + "="*50)
print("CONVERSION COMPLETE")
print("="*50)
print(f"Records processed: {len(forex_transformed)}")
print(f"DCI-type: {len(dci_data)}")
print(f"Other-type: {len(other_data)}")
print(f"Output files: 6 parquet files in {OUTPUT_PATH}")
print("="*50)
