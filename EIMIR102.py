"""
EIMIR102 SAS to Python conversion
Processes loan arrears reports with different bucket structures
"""

from pathlib import Path
from datetime import date
import polars as pl
from typing import Dict, List

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
# 2. Load and Process Data
# ============================================================================

def load_branch_data() -> pl.DataFrame:
    """Load branch header data"""
    branch_path = INPUT_PATH / "BRHFILE.parquet"
    if branch_path.exists():
        return pl.read_parquet(branch_path)
    else:
        return pl.DataFrame(schema={"BRANCH": pl.Int64, "BRHCODE": pl.Utf8})

def categorize_loans(loan_df: pl.DataFrame, hpd_list: List[str]) -> pl.DataFrame:
    """Categorize loans into different types"""
    
    # Filter: BALANCE > 0 AND BORSTAT != 'Z'
    filtered_df = loan_df.filter(
        (pl.col("BALANCE") > 0) & (pl.col("BORSTAT") != "Z")
    )
    
    categorized_rows = []
    
    # Helper function to create category
    def create_category(df_condition, cat, type_name):
        cat_df = filtered_df.filter(df_condition)
        if len(cat_df) > 0:
            return cat_df.with_columns([
                pl.lit(cat).alias("CAT"),
                pl.lit(type_name).alias("TYPE")
            ])
        return None
    
    # Category A: (HPD-C)
    cat_a = create_category(
        pl.col("PRODUCT").is_in([380, 381, 700, 705, 720, 725]),
        "A", "(HPD-C)"
    )
    if cat_a is not None:
        categorized_rows.append(cat_a)
    
    # Category B: (HP 380/381)
    cat_b = create_category(
        pl.col("PRODUCT").is_in([380, 381]),
        "B", "(HP 380/381)"
    )
    if cat_b is not None:
        categorized_rows.append(cat_b)
    
    # Category C: (AITAB)
    cat_c = create_category(
        pl.col("PRODUCT").is_in([128, 130, 131, 132]),
        "C", "(AITAB)"
    )
    if cat_c is not None:
        categorized_rows.append(cat_c)
    
    # Category D: (-HPD-)
    hpd_numbers = [int(x.strip("'")) for x in hpd_list]
    cat_d = create_category(
        pl.col("PRODUCT").is_in(hpd_numbers),
        "D", "(-HPD-)"
    )
    if cat_d is not None:
        categorized_rows.append(cat_d)
    
    # Combine all categories
    if categorized_rows:
        return pl.concat(categorized_rows, how="vertical").sort(["BRANCH"])
    else:
        return pl.DataFrame(schema=filtered_df.schema)

# ============================================================================
# 3. Calculate 17-Bucket Summaries (Main Report)
# ============================================================================

def calculate_17_bucket_summaries(loan_df: pl.DataFrame) -> Dict:
    """Calculate 17-bucket arrears summaries"""
    
    # First, calculate branch-level summaries
    branch_summary = loan_df.group_by(["CAT", "BRANCH", "ARREAR"]).agg([
        pl.col("BRHCODE").first().alias("BRHCODE"),
        pl.col("TYPE").first().alias("TYPE"),
        pl.col("BALANCE").sum().alias("BRHAMT"),
        pl.count().alias("NOACC")
    ])
    
    # Filter ARREAR values 1-17
    branch_summary = branch_summary.filter(
        (pl.col("ARREAR") >= 1) & (pl.col("ARREAR") <= 17)
    )
    
    # Pivot to matrix format
    result_dict = {}
    for cat in branch_summary["CAT"].unique().to_list():
        cat_data = branch_summary.filter(pl.col("CAT") == cat)
        
        # Initialize arrays
        totamt = [0.0] * 18  # 1-17 (index 0 unused)
        totacc = [0] * 18
        branch_results = []
        
        # Process each branch
        for branch in cat_data["BRANCH"].unique().to_list():
            branch_data = cat_data.filter(pl.col("BRANCH") == branch)
            
            # Initialize branch arrays
            branhamt = [0.0] * 18
            noacc = [0] * 18
            
            # Fill branch arrays
            for row in branch_data.iter_rows(named=True):
                arrear = int(row["ARREAR"])
                branhamt[arrear] = row["BRHAMT"]
                noacc[arrear] = row["NOACC"]
            
            # Calculate subtotals
            subbrh = sum(branhamt[4:18])  # 4-17
            subbr2 = subbrh - sum(branhamt[4:7])  # minus buckets 4-6
            subacc = sum(noacc[4:18])
            subac2 = subacc - sum(noacc[4:7])
            totbrh = subbrh + sum(branhamt[1:4])
            sotacc = subacc + sum(noacc[1:4])
            
            # Add to totals
            for i in range(1, 18):
                totamt[i] += branhamt[i]
                totacc[i] += noacc[i]
            
            branch_results.append({
                "BRANCH": branch,
                "BRHCODE": branch_data["BRHCODE"][0],
                "NOACC": noacc,
                "BRHAMT": branhamt,
                "SUBBRH": subbrh,
                "SUBBR2": subbr2,
                "SUBACC": subacc,
                "SUBAC2": subac2,
                "TOTBRH": totbrh,
                "SOTACC": sotacc
            })
        
        # Calculate category totals
        sgtotbrh = sum(totamt[4:18])
        sgtotbr2 = sgtotbrh - sum(totamt[4:7])
        sgtotacc = sum(totacc[4:18])
        sgtotac2 = sgtotacc - sum(totacc[4:7])
        gtotbrh = sgtotbrh + sum(totamt[1:4])
        gtotacc = sgtotacc + sum(totacc[1:4])
        
        result_dict[cat] = {
            "branches": branch_results,
            "totamt": totamt,
            "totacc": totacc,
            "sgtotbrh": sgtotbrh,
            "sgtotbr2": sgtotbr2,
            "sgtotacc": sgtotacc,
            "sgtotac2": sgtotac2,
            "gtotbrh": gtotbrh,
            "gtotacc": gtotacc
        }
    
    return result_dict

# ============================================================================
# 4. Calculate 15-Bucket Summaries (Day 15 Report)
# ============================================================================

def calculate_15_bucket_summaries(loan_df: pl.DataFrame) -> Dict:
    """Calculate 15-bucket arrears summaries for day 15"""
    
    # First, calculate branch-level summaries using ARREAR2
    branch_summary = loan_df.group_by(["CAT", "BRANCH", "ARREAR2"]).agg([
        pl.col("BRHCODE").first().alias("BRHCODE"),
        pl.col("TYPE").first().alias("TYPE"),
        pl.col("BALANCE").sum().alias("BRHAMT"),
        pl.count().alias("NOACC")
    ])
    
    # Filter ARREAR2 values 1-15
    branch_summary = branch_summary.filter(
        (pl.col("ARREAR2") >= 1) & (pl.col("ARREAR2") <= 15)
    )
    
    result_dict = {}
    for cat in branch_summary["CAT"].unique().to_list():
        cat_data = branch_summary.filter(pl.col("CAT") == cat)
        
        # Initialize arrays
        totamt = [0.0] * 16  # 1-15
        totacc = [0] * 16
        branch_results = []
        
        # Process each branch
        for branch in cat_data["BRANCH"].unique().to_list():
            branch_data = cat_data.filter(pl.col("BRANCH") == branch)
            
            # Initialize branch arrays
            branhamt = [0.0] * 16
            noacc = [0] * 16
            
            # Fill branch arrays
            for row in branch_data.iter_rows(named=True):
                arrear = int(row["ARREAR2"])
                branhamt[arrear] = row["BRHAMT"]
                noacc[arrear] = row["NOACC"]
            
            # Calculate subtotals (different logic for 15 buckets)
            subbrh = sum(branhamt[4:16])  # 4-15
            subbr2 = sum(branhamt[7:16])  # 7-15
            subacc = sum(noacc[4:16])
            subac2 = sum(noacc[7:16])
            totbrh = subbrh + sum(branhamt[1:4])
            sotacc = subacc + sum(noacc[1:4])
            
            # Add to totals
            for i in range(1, 16):
                totamt[i] += branhamt[i]
                totacc[i] += noacc[i]
            
            branch_results.append({
                "BRANCH": branch,
                "BRHCODE": branch_data["BRHCODE"][0],
                "NOACC": noacc,
                "BRHAMT": branhamt,
                "SUBBRH": subbrh,
                "SUBBR2": subbr2,
                "SUBACC": subacc,
                "SUBAC2": subac2,
                "TOTBRH": totbrh,
                "SOTACC": sotacc
            })
        
        # Calculate category totals
        sgtotbrh = sum(totamt[4:16])
        sgtotbr2 = sum(totamt[7:16])
        sgtotacc = sum(totacc[4:16])
        sgtotac2 = sum(totacc[7:16])
        gtotbrh = sgtotbrh + sum(totamt[1:4])
        gtotacc = sgtotacc + sum(totacc[1:4])
        
        result_dict[cat] = {
            "branches": branch_results,
            "totamt": totamt,
            "totacc": totacc,
            "sgtotbrh": sgtotbrh,
            "sgtotbr2": sgtotbr2,
            "sgtotacc": sgtotacc,
            "sgtotac2": sgtotac2,
            "gtotbrh": gtotbrh,
            "gtotacc": gtotacc
        }
    
    return result_dict

# ============================================================================
# 5. Generate Report Outputs
# ============================================================================

def generate_17_bucket_report(results: Dict, variables: Dict):
    """Generate 17-bucket report output"""
    
    report_data = []
    for cat, cat_data in results.items():
        # Add branch data
        for branch in cat_data["branches"]:
            report_data.append({
                "REPORT_TYPE": "17_BUCKET",
                "CATEGORY": cat,
                "PROGID": "EIMAR102-A",
                "BRANCH": branch["BRANCH"],
                "BRHCODE": branch["BRHCODE"],
                **{f"NOACC{i}": branch["NOACC"][i] for i in range(1, 18)},
                **{f"BRHAMT{i}": branch["BRHAMT"][i] for i in range(1, 18)},
                "SUBBRH": branch["SUBBRH"],
                "SUBBR2": branch["SUBBR2"],
                "SUBACC": branch["SUBACC"],
                "SUBAC2": branch["SUBAC2"],
                "TOTBRH": branch["TOTBRH"],
                "SOTACC": branch["SOTACC"]
            })
        
        # Add category totals
        report_data.append({
            "REPORT_TYPE": "17_BUCKET_TOTAL",
            "CATEGORY": cat,
            "PROGID": "EIMAR102-A",
            "BRANCH": "TOTAL",
            "BRHCODE": "",
            **{f"NOACC{i}": cat_data["totacc"][i] for i in range(1, 18)},
            **{f"BRHAMT{i}": cat_data["totamt"][i] for i in range(1, 18)},
            "SUBBRH": cat_data["sgtotbrh"],
            "SUBBR2": cat_data["sgtotbr2"],
            "SUBACC": cat_data["sgtotacc"],
            "SUBAC2": cat_data["sgtotac2"],
            "TOTBRH": cat_data["gtotbrh"],
            "SOTACC": cat_data["gtotacc"]
        })
    
    if report_data:
        df = pl.DataFrame(report_data)
        df.write_parquet(OUTPUT_PATH / "17_BUCKET_REPORT.parquet")
        print(f"✓ 17-bucket report saved: {len(df)} records")

def generate_15_bucket_report(results: Dict, variables: Dict):
    """Generate 15-bucket report output (for day 15)"""
    
    report_data = []
    for cat, cat_data in results.items():
        # Add branch data
        for branch in cat_data["branches"]:
            report_data.append({
                "REPORT_TYPE": "15_BUCKET",
                "CATEGORY": cat,
                "PROGID": "EIMAR102-B",
                "BRANCH": branch["BRANCH"],
                "BRHCODE": branch["BRHCODE"],
                **{f"NOACC{i}": branch["NOACC"][i] for i in range(1, 16)},
                **{f"BRHAMT{i}": branch["BRHAMT"][i] for i in range(1, 16)},
                "SUBBRH": branch["SUBBRH"],
                "SUBBR2": branch["SUBBR2"],
                "SUBACC": branch["SUBACC"],
                "SUBAC2": branch["SUBAC2"],
                "TOTBRH": branch["TOTBRH"],
                "SOTACC": branch["SOTACC"]
            })
        
        # Add category totals
        report_data.append({
            "REPORT_TYPE": "15_BUCKET_TOTAL",
            "CATEGORY": cat,
            "PROGID": "EIMAR102-B",
            "BRANCH": "TOTAL",
            "BRHCODE": "",
            **{f"NOACC{i}": cat_data["totacc"][i] for i in range(1, 16)},
            **{f"BRHAMT{i}": cat_data["totamt"][i] for i in range(1, 16)},
            "SUBBRH": cat_data["sgtotbrh"],
            "SUBBR2": cat_data["sgtotbr2"],
            "SUBACC": cat_data["sgtotacc"],
            "SUBAC2": cat_data["sgtotac2"],
            "TOTBRH": cat_data["gtotbrh"],
            "SOTACC": cat_data["gtotacc"]
        })
    
    if report_data:
        df = pl.DataFrame(report_data)
        df.write_parquet(OUTPUT_PATH / "15_BUCKET_REPORT.parquet")
        print(f"✓ 15-bucket report saved: {len(df)} records")

# ============================================================================
# 6. Main Execution
# ============================================================================

def main():
    """Main execution function"""
    print("=" * 60)
    print("EIMIR102 SAS to Python Conversion")
    print("=" * 60)
    
    # HPD list (would come from macro variable &HPD)
    HPD_LIST = ["110", "115", "700", "705"]
    
    # 1. Process REPTDATE
    print("\n1. Processing REPTDATE...")
    variables = process_repdate()
    print(f"   Report Date: {variables['RDATE']}")
    print(f"   Day of Month: {variables['REPTDAY']}")
    
    # 2. Load data
    print("\n2. Loading data...")
    loan_path = INPUT_PATH / "BNM/LOANTEMP.parquet"
    loan_df = pl.read_parquet(loan_path)
    branch_df = load_branch_data()
    print(f"   Loans: {len(loan_df)}, Branches: {len(branch_df)}")
    
    # 3. Categorize loans
    print("\n3. Categorizing loans...")
    categorized = categorize_loans(loan_df, HPD_LIST)
    print(f"   Categorized records: {len(categorized)}")
    
    # 4. Merge with branch data
    print("\n4. Merging with branch data...")
    merged_data = categorized.join(
        branch_df,
        on="BRANCH",
        how="inner"
    ).sort(["CAT", "BRANCH"])
    print(f"   Merged records: {len(merged_data)}")
    
    # Save merged data
    merged_data.write_parquet(OUTPUT_PATH / "LOANTEMP_CATEGORIZED.parquet")
    
    # 5. Always generate 17-bucket report
    print("\n5. Generating 17-bucket report (EIMAR102-A)...")
    results_17 = calculate_17_bucket_summaries(merged_data)
    generate_17_bucket_report(results_17, variables)
    print(f"   Categories processed: {len(results_17)}")
    
    # 6. Generate 15-bucket report only on day 15
    if variables['REPTDAY'] == '15':
        print("\n6. Day 15 detected - Generating 15-bucket report (EIMAR102-B)...")
        results_15 = calculate_15_bucket_summaries(merged_data)
        generate_15_bucket_report(results_15, variables)
        print(f"   Categories processed: {len(results_15)}")
    else:
        print("\n6. Not day 15 - Skipping 15-bucket report")
    
    # 7. Create summary statistics
    print("\n7. Creating summary statistics...")
    
    summary_data = []
    for cat in sorted(merged_data["CAT"].unique().to_list()):
        cat_data = merged_data.filter(pl.col("CAT") == cat)
        
        # Basic statistics
        total_balance = cat_data["BALANCE"].sum()
        total_accounts = len(cat_data)
        avg_balance = total_balance / total_accounts if total_accounts > 0 else 0
        
        # Arrears distribution
        arrears_dist = cat_data.group_by("ARREAR").agg(pl.count().alias("COUNT"))
        
        summary_data.append({
            "CATEGORY": cat,
            "TYPE": cat_data["TYPE"][0],
            "TOTAL_ACCOUNTS": total_accounts,
            "TOTAL_BALANCE": total_balance,
            "AVERAGE_BALANCE": avg_balance,
            "MAX_ARREAR": cat_data["ARREAR"].max() if len(cat_data) > 0 else 0,
            "MIN_ARREAR": cat_data["ARREAR"].min() if len(cat_data) > 0 else 0
        })
    
    if summary_data:
        summary_df = pl.DataFrame(summary_data)
        summary_df.write_parquet(OUTPUT_PATH / "SUMMARY_STATISTICS.parquet")
        print(f"✓ Summary statistics saved: {len(summary_df)} categories")
    
    # 8. Save variables
    variables_df = pl.DataFrame([variables])
    variables_df.write_parquet(OUTPUT_PATH / "EIMIR102_VARIABLES.parquet")
    
    print("\n" + "=" * 60)
    print("CONVERSION COMPLETE")
    print("=" * 60)
    print(f"Total loan records: {len(loan_df)}")
    print(f"Categorized records: {len(merged_data)}")
    print(f"Report day: {variables['REPTDAY']}")
    print(f"Output saved to: {OUTPUT_PATH}")

# ============================================================================
# 7. Run the conversion
# ============================================================================

if __name__ == "__main__":
    main()
