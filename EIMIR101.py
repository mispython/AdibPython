"""
EIMIR101 SAS to Python conversion
Processes loan arrears reports with branch-level summaries
"""

from pathlib import Path
from datetime import date
import polars as pl
from typing import Dict, List, Optional

# Setup paths
BASE_PATH = Path("./data")
INPUT_PATH = BASE_PATH / "input"
OUTPUT_PATH = BASE_PATH / "output"
OUTPUT_PATH.mkdir(parents=True, exist_ok=True)

# ============================================================================
# 1. REPTDATE Processing
# ============================================================================

def process_repdate() -> Dict[str, str]:
    """Process REPTDATE and extract formatted variables"""
    repdate_path = INPUT_PATH / "BNM/REPTDATE.parquet"
    df = pl.read_parquet(repdate_path)
    repdate = df["REPTDATE"][0]
    
    return {
        'RDATE': repdate.strftime("%d%m%y"),  # DDMMYY8.
        'REPTYEAR': str(repdate.year),        # YEAR4.
        'REPTMON': f"{repdate.month:02d}",    # Z2.
        'REPTDAY': f"{repdate.day:02d}",      # Z2.
        'REPTDATE': repdate
    }

# ============================================================================
# 2. Load and Process Loan Data
# ============================================================================

def load_loan_data() -> pl.DataFrame:
    """Load and sort loan data"""
    loan_path = INPUT_PATH / "BNM/LOANTEMP.parquet"
    loan_df = pl.read_parquet(loan_path)
    
    # Sort by BRANCH, ARREAR2 (matching PROC SORT)
    return loan_df.sort(["BRANCH", "ARREAR2"])

def load_branch_data() -> pl.DataFrame:
    """Load branch header data from fixed-width file"""
    # Simulating INFILE BRHFILE LRECL=80
    # In reality, you'd parse a text file, but here we'll assume parquet
    branch_path = INPUT_PATH / "BRHFILE.parquet"
    if branch_path.exists():
        return pl.read_parquet(branch_path)
    else:
        # Create empty if not exists
        return pl.DataFrame(schema={"BRANCH": pl.Int64, "BRHCODE": pl.Utf8})

# ============================================================================
# 3. Categorize Loans
# ============================================================================

def categorize_loans(loan_df: pl.DataFrame, hpd_list: List[str]) -> pl.DataFrame:
    """Categorize loans into different types (A,B,C,D)"""
    
    # Filter: BALANCE > 0 AND BORSTAT != 'Z'
    filtered_df = loan_df.filter(
        (pl.col("BALANCE") > 0) & (pl.col("BORSTAT") != "Z")
    )
    
    # Create categorized rows (multiple rows per loan for different categories)
    categorized_rows = []
    
    # Category A: (HPD-C)
    cat_a = filtered_df.filter(pl.col("PRODUCT").is_in([380, 381, 700, 705, 720, 725]))
    if len(cat_a) > 0:
        cat_a_df = cat_a.with_columns([
            pl.lit("A").alias("CAT"),
            pl.lit("(HPD-C)").alias("TYPE")
        ])
        categorized_rows.append(cat_a_df)
    
    # Category B: (HP 380/381)
    cat_b = filtered_df.filter(pl.col("PRODUCT").is_in([380, 381]))
    if len(cat_b) > 0:
        cat_b_df = cat_b.with_columns([
            pl.lit("B").alias("CAT"),
            pl.lit("(HP 380/381)").alias("TYPE")
        ])
        categorized_rows.append(cat_b_df)
    
    # Category C: (AITAB)
    cat_c = filtered_df.filter(pl.col("PRODUCT").is_in([128, 130, 131, 132]))
    if len(cat_c) > 0:
        cat_c_df = cat_c.with_columns([
            pl.lit("C").alias("CAT"),
            pl.lit("(AITAB)").alias("TYPE")
        ])
        categorized_rows.append(cat_c_df)
    
    # Category D: (-HPD-)
    # Convert HPD list from string format like ('110','115') to numbers
    hpd_numbers = [int(x.strip("'")) for x in hpd_list]
    cat_d = filtered_df.filter(pl.col("PRODUCT").is_in(hpd_numbers))
    if len(cat_d) > 0:
        cat_d_df = cat_d.with_columns([
            pl.lit("D").alias("CAT"),
            pl.lit("(-HPD-)").alias("TYPE")
        ])
        categorized_rows.append(cat_d_df)
    
    # Combine all categories
    if categorized_rows:
        combined = pl.concat(categorized_rows, how="vertical")
        return combined.sort(["BRANCH"])
    else:
        return pl.DataFrame(schema=filtered_df.schema)

# ============================================================================
# 4. Merge with Branch Data
# ============================================================================

def merge_branch_data(loan_df: pl.DataFrame, branch_df: pl.DataFrame) -> pl.DataFrame:
    """Merge loan data with branch header data"""
    # Keep only loans with matching branch data (IF PRESENT=1)
    merged = loan_df.join(
        branch_df,
        on="BRANCH",
        how="inner"
    )
    
    return merged.sort(["CAT", "BRANCH", "ARREAR2"])

# ============================================================================
# 5. Create Branch-Level Summaries
# ============================================================================

def calculate_branch_summaries(loan_df: pl.DataFrame) -> pl.DataFrame:
    """Calculate branch-level summaries for arrears buckets"""
    
    # Group by CAT, BRANCH, ARREAR2 and calculate counts and sums
    summary = loan_df.group_by(["CAT", "BRANCH", "ARREAR2"]).agg([
        pl.col("BRHCODE").first().alias("BRHCODE"),
        pl.col("TYPE").first().alias("TYPE"),
        pl.col("BALANCE").sum().alias("BRHAMT"),
        pl.count().alias("NOACC")
    ])
    
    # Pivot to get arrears buckets (1-14) as columns
    # First, ensure ARREAR2 is between 1 and 14
    summary = summary.filter(
        (pl.col("ARREAR2") >= 1) & (pl.col("ARREAR2") <= 14)
    )
    
    # Pivot to get matrix format
    amount_pivot = summary.pivot(
        values="BRHAMT",
        index=["CAT", "BRANCH", "BRHCODE", "TYPE"],
        columns="ARREAR2",
        aggregate_function="sum"
    )
    
    count_pivot = summary.pivot(
        values="NOACC",
        index=["CAT", "BRANCH", "BRHCODE", "TYPE"],
        columns="ARREAR2",
        aggregate_function="sum"
    )
    
    # Merge amount and count pivots
    result = amount_pivot.join(
        count_pivot,
        on=["CAT", "BRANCH", "BRHCODE", "TYPE"],
        suffix="_count"
    )
    
    # Fill nulls with 0 and rename columns
    for i in range(1, 15):
        amt_col = f"{i}"
        cnt_col = f"{i}_count"
        result = result.with_columns([
            pl.col(amt_col).fill_null(0).alias(f"BRHAMT{i}"),
            pl.col(cnt_col).fill_null(0).alias(f"NOACC{i}")
        ]).drop([amt_col, cnt_col])
    
    return result.sort(["CAT", "BRANCH"])

# ============================================================================
# 6. Calculate Subtotals and Totals
# ============================================================================

def calculate_totals(branch_summary: pl.DataFrame) -> Dict:
    """Calculate various subtotals and totals"""
    
    results = {}
    
    for cat in branch_summary["CAT"].unique().to_list():
        cat_data = branch_summary.filter(pl.col("CAT") == cat)
        
        # Initialize arrays for totals
        totamt = [0.0] * 15  # 1-14 (index 0 unused)
        totacc = [0] * 15
        
        # Sum across all branches in category
        for i in range(1, 15):
            if f"BRHAMT{i}" in cat_data.columns:
                totamt[i] = cat_data[f"BRHAMT{i}"].sum()
            if f"NOACC{i}" in cat_data.columns:
                totacc[i] = cat_data[f"NOACC{i}"].sum()
        
        # Calculate subtotals (matching SAS logic)
        # SUBBRH = SUM(BRHAMT4 through BRHAMT14)
        subbrh = sum(totamt[4:15])  # 4-14 inclusive
        
        # SUBBR2 = SUM(BRHAMT7 through BRHAMT14)
        subbr2 = sum(totamt[7:15])  # 7-14 inclusive
        
        # SUBACC = SUM(NOACC4 through NOACC14)
        subacc = sum(totacc[4:15])
        
        # SUBAC2 = SUM(NOACC7 through NOACC14)
        subac2 = sum(totacc[7:15])
        
        # TOTBRH = SUBBRH + BRHAMT1-3
        totbrh = subbrh + sum(totamt[1:4])
        
        # SOTACC = SUBACC + NOACC1-3
        sotacc = subacc + sum(totacc[1:4])
        
        # Category totals (SGTOT*)
        sgtotbrh = sum(totamt[4:15])  # Same as subbrh but across all branches
        sgtotbr2 = sum(totamt[7:15])
        sgtotacc = sum(totacc[4:15])
        sgtotac2 = sum(totacc[7:15])
        gtotbrh = sgtotbrh + sum(totamt[1:4])
        gtotacc = sgtotacc + sum(totacc[1:4])
        
        results[cat] = {
            'branch_summary': cat_data,
            'totamt': totamt,
            'totacc': totacc,
            'subbrh': subbrh,
            'subbr2': subbr2,
            'subacc': subacc,
            'subac2': subac2,
            'totbrh': totbrh,
            'sotacc': sotacc,
            'sgtotbrh': sgtotbrh,
            'sgtotbr2': sgtotbr2,
            'sgtotacc': sgtotacc,
            'sgtotac2': sgtotac2,
            'gtotbrh': gtotbrh,
            'gtotacc': gtotacc
        }
    
    return results

# ============================================================================
# 7. Generate Reports
# ============================================================================

def generate_report_a(loan_data: pl.DataFrame, variables: Dict) -> pl.DataFrame:
    """Generate first report (EIMAR101-A)"""
    # All data for first report
    report_df = loan_data.with_columns(
        pl.lit("EIMAR101-A").alias("PROGID")
    )
    
    # Save for later processing
    report_df.write_parquet(OUTPUT_PATH / "PRNDATA_A.parquet")
    
    return report_df

def generate_report_b(loan_data: pl.DataFrame, variables: Dict, hpd_list: List[str]) -> pl.DataFrame:
    """Generate second report (EIMAR101-B) with exclusions"""
    
    # Filter: exclude certain types and borrower statuses
    hpd_numbers = [int(x.strip("'")) for x in hpd_list]
    
    filtered = loan_data.filter(
        (~pl.col("TYPE").is_in(["(AITAB)", "(-HPD-)"])) &
        (~pl.col("BORSTAT").is_in(["F", "I", "R"])) &
        (pl.col("PRODUCT").is_in(hpd_numbers))
    )
    
    report_df = filtered.with_columns(
        pl.lit("EIMAR101-B").alias("PROGID")
    )
    
    # Save for later processing
    report_df.write_parquet(OUTPUT_PATH / "PRNDATA_B.parquet")
    
    return report_df

def create_detailed_summary(report_df: pl.DataFrame) -> pl.DataFrame:
    """Create detailed summary with all arrears buckets (LOAN7A equivalent)"""
    
    # Calculate branch-level summaries
    branch_summary = calculate_branch_summaries(report_df)
    
    # Keep only the needed columns
    keep_cols = ["BRHCODE", "TYPE"]
    for i in range(1, 15):
        keep_cols.extend([f"NOACC{i}", f"BRHAMT{i}"])
    
    detailed = branch_summary.select(keep_cols)
    detailed.write_parquet(OUTPUT_PATH / "LOAN7A_DETAILED.parquet")
    
    return detailed

def write_csv_output(detailed_df: pl.DataFrame):
    """Write CSV output (matching SAS FILE CCDTXT7A)"""
    csv_path = OUTPUT_PATH / "EIMIR101_DETAILED.csv"
    
    # Build header row
    header_cols = ["BRHCODE", "TYPE"]
    for i in range(1, 15):
        header_cols.extend([f"NOACC{i}", f"BRHAMT{i}"])
    
    # Write to CSV
    detailed_df.write_csv(csv_path)
    print(f"✓ CSV output saved to: {csv_path}")

# ============================================================================
# 8. Main Execution
# ============================================================================

def main():
    """Main execution function"""
    print("=" * 60)
    print("EIMIR101 SAS to Python Conversion")
    print("=" * 60)
    
    # HPD list (would come from macro variable &HPD)
    # Example: ("110","115","700","705")
    HPD_LIST = ["110", "115", "700", "705"]
    
    # 1. Process REPTDATE
    print("\n1. Processing REPTDATE...")
    variables = process_repdate()
    print(f"   Report Date: {variables['RDATE']}")
    
    # 2. Load data
    print("\n2. Loading data...")
    loan_df = load_loan_data()
    branch_df = load_branch_data()
    print(f"   Loans: {len(loan_df)}, Branches: {len(branch_df)}")
    
    # 3. Categorize loans
    print("\n3. Categorizing loans...")
    categorized = categorize_loans(loan_df, HPD_LIST)
    print(f"   Categorized records: {len(categorized)}")
    
    # 4. Merge with branch data
    print("\n4. Merging with branch data...")
    merged_data = merge_branch_data(categorized, branch_df)
    print(f"   Merged records: {len(merged_data)}")
    
    # 5. Generate Report A
    print("\n5. Generating Report A (EIMAR101-A)...")
    report_a = generate_report_a(merged_data, variables)
    summary_a = calculate_branch_summaries(report_a)
    totals_a = calculate_totals(summary_a)
    print(f"   Report A: {len(report_a)} records")
    
    # 6. Generate Report B
    print("\n6. Generating Report B (EIMAR101-B)...")
    report_b = generate_report_b(merged_data, variables, HPD_LIST)
    summary_b = calculate_branch_summaries(report_b)
    totals_b = calculate_totals(summary_b)
    print(f"   Report B: {len(report_b)} records")
    
    # 7. Create detailed summary
    print("\n7. Creating detailed summary...")
    detailed_summary = create_detailed_summary(report_b)
    
    # 8. Write CSV output
    print("\n8. Writing CSV output...")
    write_csv_output(detailed_summary)
    
    # ========================================================================
    # 9. Save Outputs
    # ========================================================================
    
    # Save categorized data
    merged_data.write_parquet(OUTPUT_PATH / "LOANTEMP_CATEGORIZED.parquet")
    
    # Save summaries
    summary_a.write_parquet(OUTPUT_PATH / "REPORT_A_SUMMARY.parquet")
    summary_b.write_parquet(OUTPUT_PATH / "REPORT_B_SUMMARY.parquet")
    
    # Save variables
    variables_df = pl.DataFrame([variables])
    variables_df.write_parquet(OUTPUT_PATH / "EIMIR101_VARIABLES.parquet")
    
    # Save totals calculations
    totals_data = []
    for cat, cat_totals in totals_a.items():
        totals_data.append({
            "CAT": cat,
            "REPORT": "A",
            **{k: str(v) for k, v in cat_totals.items() if k != 'branch_summary'}
        })
    
    for cat, cat_totals in totals_b.items():
        totals_data.append({
            "CAT": cat,
            "REPORT": "B",
            **{k: str(v) for k, v in cat_totals.items() if k != 'branch_summary'}
        })
    
    if totals_data:
        totals_df = pl.DataFrame(totals_data)
        totals_df.write_parquet(OUTPUT_PATH / "TOTALS_CALCULATIONS.parquet")
    
    print("\n" + "=" * 60)
    print("CONVERSION COMPLETE")
    print("=" * 60)
    print(f"Report A records: {len(report_a)}")
    print(f"Report B records: {len(report_b)}")
    print(f"Categories processed: {len(totals_a) + len(totals_b)}")
    print(f"Output saved to: {OUTPUT_PATH}")

# ============================================================================
# 10. Run the conversion
# ============================================================================

if __name__ == "__main__":
    main()
