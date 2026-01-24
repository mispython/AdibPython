"""
EIIMFGP3 SAS to Python conversion
Updates loan and deposit data with GP3 risk information
"""

from pathlib import Path
from datetime import datetime, date
import polars as pl
from typing import Dict, Optional

# Setup paths
BASE_PATH = Path("./data")
INPUT_PATH = BASE_PATH / "input"
OUTPUT_PATH = BASE_PATH / "output"
OUTPUT_PATH.mkdir(parents=True, exist_ok=True)

# ============================================================================
# 1. REPTDATE Processing
# ============================================================================

def process_repdate() -> Dict[str, str]:
    """Process REPTDATE and calculate week/month variables"""
    
    # Load REPTDATE from deposit data
    repdate_path = INPUT_PATH / "DEPOSIT/REPTDATE.parquet"
    df = pl.read_parquet(repdate_path)
    repdate = df["REPTDATE"][0]
    
    # Determine week based on day of month
    day = repdate.day
    if 1 <= day <= 8:
        nowk = '1'
    elif 9 <= day <= 15:
        nowk = '2'
    elif 16 <= day <= 22:
        nowk = '3'
    else:
        nowk = '4'
    
    # Format variables (matching SAS formats)
    variables = {
        'NOWK': nowk,
        'REPTYEAR': str(repdate.year),  # YEAR4.
        'REPTMON': f"{repdate.month:02d}",  # Z2.
        'REPTDAY': f"{repdate.day:02d}",  # Z2.
        'RDATE': repdate.strftime("%d%m%y"),  # DDMMYY8.
        'REPTDATE': repdate
    }
    
    print(f"Report Date: {repdate}")
    print(f"Week: {nowk}, Month: {variables['REPTMON']}")
    
    return variables

# ============================================================================
# 2. Load and Process GP3 Data
# ============================================================================

def load_and_process_gp3() -> pl.DataFrame:
    """Load GP3 data and apply transformations"""
    
    # Load GP3 data
    gp3_path = INPUT_PATH / "LIMIT/GP3.parquet"
    gp3_df = pl.read_parquet(gp3_path)
    
    # Apply transformations (matching SAS DATA step)
    gp3_df = gp3_df.with_columns([
        # RISKRTE = INPUT(RISKCODE,1.)
        pl.col("RISKCODE").str.slice(0, 1).cast(pl.Int64).alias("RISKRTE"),
        
        # Rename columns
        pl.col("EXCESSDT").alias("EXODDATE"),
        pl.col("TODDATE").alias("TEMPODDT"),
        pl.col("ODSTATUS").alias("ODSTAT"),
        
        # RISKCD = PUT(RISKCODE, $RISKCD.)
        # Assuming external format mapping exists
        pl.col("RISKCODE").alias("RISKCD")  # Simplified - would use actual format
    ])
    
    # Sort by ACCTNO (matching PROC SORT)
    gp3_df = gp3_df.sort("ACCTNO")
    
    print(f"Loaded {len(gp3_df)} GP3 records")
    return gp3_df

# ============================================================================
# 3. Update Loan Data
# ============================================================================

def update_loan_data(gp3_df: pl.DataFrame, variables: Dict[str, str]) -> pl.DataFrame:
    """Update loan data with GP3 risk information"""
    
    # Load loan data for current month/week
    loan_filename = f"LOAN{variables['REPTMON']}{variables['NOWK']}.parquet"
    loan_path = INPUT_PATH / f"LOAN/{loan_filename}"
    
    if not loan_path.exists():
        # Create empty loan data if it doesn't exist
        loan_df = pl.DataFrame(schema={"ACCTNO": pl.Utf8})
    else:
        loan_df = pl.read_parquet(loan_path)
    
    # Sort loan data (PROC SORT)
    loan_df = loan_df.sort("ACCTNO")
    
    # Select only needed columns from GP3
    gp3_subset = gp3_df.select(["ACCTNO", "RISKRTE", "RISKCD"])
    
    # Update loan data with GP3 info (matching SAS UPDATE)
    # UPDATE performs a left join and updates matching records
    updated_loan = loan_df.join(
        gp3_subset, 
        on="ACCTNO", 
        how="left",
        suffix="_gp3"
    )
    
    # If GP3 columns exist in original, keep them, otherwise use GP3 values
    if "RISKRTE" in updated_loan.columns:
        updated_loan = updated_loan.with_columns([
            pl.when(pl.col("RISKRTE_gp3").is_not_null())
            .then(pl.col("RISKRTE_gp3"))
            .otherwise(pl.col("RISKRTE"))
            .alias("RISKRTE"),
            
            pl.when(pl.col("RISKCD_gp3").is_not_null())
            .then(pl.col("RISKCD_gp3"))
            .otherwise(pl.col("RISKCD"))
            .alias("RISKCD")
        ]).drop(["RISKRTE_gp3", "RISKCD_gp3"])
    
    print(f"Updated {len(updated_loan)} loan records")
    return updated_loan

# ============================================================================
# 4. Update Deposit Data
# ============================================================================

def update_deposit_data(gp3_df: pl.DataFrame) -> pl.DataFrame:
    """Update deposit data with GP3 information"""
    
    # Load current deposit data
    deposit_path = INPUT_PATH / "DEPOSIT/CURRENT.parquet"
    deposit_df = pl.read_parquet(deposit_path)
    
    # Select GP3 columns for deposit update
    gp3_deposit_cols = gp3_df.select([
        "ACCTNO", "EXODDATE", "TEMPODDT", "ODSTAT", "RISKCODE"
    ])
    
    # Update deposit data (matching SAS UPDATE)
    updated_deposit = deposit_df.join(
        gp3_deposit_cols,
        on="ACCTNO",
        how="left",
        suffix="_gp3"
    )
    
    # Update existing columns with GP3 values where available
    for col in ["EXODDATE", "TEMPODDT", "ODSTAT", "RISKCODE"]:
        if f"{col}_gp3" in updated_deposit.columns:
            updated_deposit = updated_deposit.with_columns([
                pl.when(pl.col(f"{col}_gp3").is_not_null())
                .then(pl.col(f"{col}_gp3"))
                .otherwise(pl.col(col))
                .alias(col)
            ]).drop(f"{col}_gp3")
    
    print(f"Updated {len(updated_deposit)} deposit records")
    return updated_deposit

# ============================================================================
# 5. Process Negative Balances and Overdrafts
# ============================================================================

def process_overdrafts(deposit_df: pl.DataFrame) -> pl.DataFrame:
    """Process negative balances and merge with overdraft limits"""
    
    # Filter for negative balances with specific OPENIND values (matching WHERE clause)
    current_filtered = deposit_df.filter(
        (~pl.col("OPENIND").is_in(["B", "C", "P"])) &
        (pl.col("CURBAL") < 0)
    ).sort("ACCTNO")
    
    print(f"Found {len(current_filtered)} accounts with negative balances")
    
    # Load overdraft data
    od_path = INPUT_PATH / "LIMIT/OVERDFT.parquet"
    if od_path.exists():
        od_df = pl.read_parquet(od_path)
        
        # Remove duplicates by ACCTNO (NODUPKEY)
        od_df = od_df.unique(subset=["ACCTNO"], keep="first")
        
        # Rename APPRLIMT to APPRLIM2
        if "APPRLIMT" in od_df.columns:
            od_df = od_df.rename({"APPRLIMT": "APPRLIM2"})
        else:
            od_df = od_df.with_columns(pl.lit(0).alias("APPRLIM2"))
        
        # Merge with current data (matching SAS MERGE)
        overdraft_result = current_filtered.join(
            od_df.select(["ACCTNO", "APPRLIM2"]),
            on="ACCTNO",
            how="left"
        )
    else:
        # No overdraft data - create with APPRLIM2 = 0
        overdraft_result = current_filtered.with_columns(
            pl.lit(0).alias("APPRLIM2")
        )
    
    # Keep only ACCTNO and APPRLIM2
    overdraft_result = overdraft_result.select(["ACCTNO", "APPRLIM2"])
    
    return overdraft_result

# ============================================================================
# 6. Final Loan Data Update with Overdraft Limits
# ============================================================================

def update_loan_with_overdrafts(loan_df: pl.DataFrame, overdraft_df: pl.DataFrame) -> pl.DataFrame:
    """Merge loan data with overdraft limits"""
    
    # Merge loan data with overdraft limits
    final_loan = loan_df.join(
        overdraft_df,
        on="ACCTNO",
        how="left"
    )
    
    # Set APPRLIM2 to 0 if null (matching: IF APPRLIM2 EQ . THEN APPRLIM2 = 0)
    if "APPRLIM2" in final_loan.columns:
        final_loan = final_loan.with_columns([
            pl.when(pl.col("APPRLIM2").is_null())
            .then(pl.lit(0))
            .otherwise(pl.col("APPRLIM2"))
            .alias("APPRLIM2")
        ])
    
    print(f"Final loan data: {len(final_loan)} records")
    return final_loan

# ============================================================================
# 7. Main Execution
# ============================================================================

def main():
    """Main execution function"""
    print("=" * 50)
    print("EIIMFGP3 SAS to Python Conversion")
    print("=" * 50)
    
    # 1. Process REPTDATE
    print("\n1. Processing REPTDATE...")
    variables = process_repdate()
    
    # 2. Load and process GP3 data
    print("\n2. Loading GP3 data...")
    gp3_df = load_and_process_gp3()
    
    # 3. Update loan data with GP3 risk info
    print("\n3. Updating loan data...")
    updated_loan = update_loan_data(gp3_df, variables)
    
    # 4. Update deposit data with GP3 info
    print("\n4. Updating deposit data...")
    updated_deposit = update_deposit_data(gp3_df)
    
    # 5. Process negative balances and overdrafts
    print("\n5. Processing overdrafts...")
    overdraft_df = process_overdrafts(updated_deposit)
    
    # 6. Final loan update with overdraft limits
    print("\n6. Final loan data update...")
    final_loan = update_loan_with_overdrafts(updated_loan, overdraft_df)
    
    # ========================================================================
    # 7. Save Outputs
    # ========================================================================
    
    # Save updated loan data
    loan_filename = f"LOAN{variables['REPTMON']}{variables['NOWK']}.parquet"
    final_loan.write_parquet(OUTPUT_PATH / loan_filename)
    print(f"\n✓ Saved updated loan data: {loan_filename}")
    
    # Save updated deposit data
    updated_deposit.write_parquet(OUTPUT_PATH / "DEPOSIT_CURRENT.parquet")
    print(f"✓ Saved updated deposit data: DEPOSIT_CURRENT.parquet")
    
    # Save overdraft results
    overdraft_df.write_parquet(OUTPUT_PATH / "OVERDFT_PROCESSED.parquet")
    print(f"✓ Saved overdraft data: OVERDFT_PROCESSED.parquet")
    
    # Save GP3 processed data
    gp3_df.write_parquet(OUTPUT_PATH / "GP3_PROCESSED.parquet")
    print(f"✓ Saved processed GP3 data: GP3_PROCESSED.parquet")
    
    # Save variables
    variables_df = pl.DataFrame([variables])
    variables_df.write_parquet(OUTPUT_PATH / "EIIMFGP3_VARIABLES.parquet")
    print(f"✓ Saved variables: EIIMFGP3_VARIABLES.parquet")
    
    print("\n" + "=" * 50)
    print("CONVERSION COMPLETE")
    print("=" * 50)
    print(f"Total accounts processed: {len(final_loan)}")
    print(f"Negative balance accounts: {len(overdraft_df)}")
    print(f"Output saved to: {OUTPUT_PATH}")

# ============================================================================
# 8. Run the conversion
# ============================================================================

if __name__ == "__main__":
    main()
