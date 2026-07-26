"""
EIMIR102 SAS to Python conversion
Processes loan arrears reports with different bucket structures
"""

from pathlib import Path
from datetime import datetime, timedelta
import polars as pl
import pyreadstat
from typing import Dict, List

# Setup paths
BASE_PATH = Path(".")
INPUT_PATH = BASE_PATH / "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIMIR102"
OUTPUT_PATH = BASE_PATH / "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIMIR102"
OUTPUT_PATH.mkdir(parents=True, exist_ok=True)

# ============================================================================
# 1. REPTDATE Processing (using yesterday's date)
# ============================================================================

def process_repdate() -> Dict[str, str]:
    """Process REPTDATE using yesterday's date"""
    repdate = datetime.now().date() - timedelta(days=1)
    
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
    """Load branch header data from flatfile"""
    branch_path = INPUT_PATH / "LKP_BRANCH"
    if branch_path.exists():
        # Read the flatfile - try different delimiters
        try:
            # First try reading as fixed-width or space-delimited
            with open(branch_path, 'r') as f:
                lines = f.readlines()
            
            # Parse the file manually
            data = []
            for line in lines:
                line = line.strip()
                if line and not line.startswith('---'):  # Skip separator lines
                    # Split by multiple spaces
                    parts = line.split()
                    if len(parts) >= 2:
                        # Try to extract branch code and branch number
                        # Assuming format: BRHCODE BRANCH (or vice versa)
                        data.append(parts)
            
            if data:
                # Try to determine columns
                # Usually first column is branch code, second is branch number
                # But let's try both possibilities
                df = pl.DataFrame({
                    "BRHCODE": [row[0] for row in data],
                    "BRANCH": [row[1] if len(row) > 1 else row[0] for row in data]
                })
                # Try to convert BRANCH to integer
                try:
                    df = df.with_columns(pl.col("BRANCH").cast(pl.Int64))
                except:
                    pass
                return df
            else:
                # Fallback to read_csv with various delimiters
                for sep in ['|', '\t', ',', ' ']:
                    try:
                        df = pl.read_csv(branch_path, separator=sep, has_header=False)
                        if len(df.columns) >= 2:
                            # Rename columns
                            df = df.rename({df.columns[0]: "BRHCODE", df.columns[1]: "BRANCH"})
                            return df
                    except:
                        continue
        except Exception as e:
            print(f"Warning: Error reading branch file: {e}")
    
    print(f"Warning: Branch file not found or couldn't be parsed at {branch_path}")
    return pl.DataFrame(schema={"BRANCH": pl.Int64, "BRHCODE": pl.Utf8})

def load_loan_data() -> pl.DataFrame:
    """Load loan data from SAS dataset"""
    loan_path = INPUT_PATH / "loantemp.sas7bdat"
    if loan_path.exists():
        # Read SAS dataset using pyreadstat
        df, meta = pyreadstat.read_sas7bdat(str(loan_path))
        # Convert to polars DataFrame
        return pl.from_pandas(df)
    else:
        raise FileNotFoundError(f"Loan data file not found at {loan_path}")

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
                "BRHCODE": branch_data["BRHCODE"][0] if len(branch_data) > 0 else "",
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
                "BRHCODE": branch_data["BRHCODE"][0] if len(branch_data) > 0 else "",
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
# 5. Generate Report Outputs as Text Files
# ============================================================================

def generate_17_bucket_report_text(results: Dict, variables: Dict):
    """Generate 17-bucket report as text file"""
    
    output_file = OUTPUT_PATH / "17_BUCKET_REPORT.txt"
    
    with open(output_file, 'w') as f:
        f.write("=" * 100 + "\n")
        f.write(f"EIMIR102 - 17-Bucket Arrears Report\n")
        f.write(f"Report Date: {variables['RDATE']}\n")
        f.write(f"Program: EIMAR102-A\n")
        f.write("=" * 100 + "\n\n")
        
        for cat, cat_data in sorted(results.items()):
            f.write(f"\n{'='*50}\n")
            f.write(f"CATEGORY {cat}: {cat_data['branches'][0]['TYPE'] if cat_data['branches'] else 'Unknown'}\n")
            f.write(f"{'='*50}\n\n")
            
            # Write header
            header = "BRANCH  BRHCODE  "
            for i in range(1, 18):
                header += f"B{i:6} "
            header += "SUBBRH  SUBBR2  SUBACC  SUBAC2  TOTBRH  SOTACC"
            f.write(header + "\n")
            f.write("-" * len(header) + "\n")
            
            # Write branch data
            for branch in cat_data["branches"]:
                line = f"{branch['BRANCH']:6}  {branch['BRHCODE']:7}  "
                for i in range(1, 18):
                    line += f"{branch['BRHAMT'][i]:6.0f} "
                line += f"{branch['SUBBRH']:6.0f}  {branch['SUBBR2']:6.0f}  "
                line += f"{branch['SUBACC']:6}  {branch['SUBAC2']:6}  "
                line += f"{branch['TOTBRH']:6.0f}  {branch['SOTACC']:6}"
                f.write(line + "\n")
            
            # Write totals
            f.write("-" * len(header) + "\n")
            line = "TOTAL         "
            for i in range(1, 18):
                line += f"{cat_data['totamt'][i]:6.0f} "
            line += f"{cat_data['sgtotbrh']:6.0f}  {cat_data['sgtotbr2']:6.0f}  "
            line += f"{cat_data['sgtotacc']:6}  {cat_data['sgtotac2']:6}  "
            line += f"{cat_data['gtotbrh']:6.0f}  {cat_data['gtotacc']:6}"
            f.write(line + "\n\n")
    
    print(f"✓ 17-bucket report saved: {output_file}")

def generate_15_bucket_report_text(results: Dict, variables: Dict):
    """Generate 15-bucket report as text file"""
    
    output_file = OUTPUT_PATH / "15_BUCKET_REPORT.txt"
    
    with open(output_file, 'w') as f:
        f.write("=" * 100 + "\n")
        f.write(f"EIMIR102 - 15-Bucket Arrears Report\n")
        f.write(f"Report Date: {variables['RDATE']}\n")
        f.write(f"Program: EIMAR102-B\n")
        f.write("=" * 100 + "\n\n")
        
        for cat, cat_data in sorted(results.items()):
            f.write(f"\n{'='*50}\n")
            f.write(f"CATEGORY {cat}: {cat_data['branches'][0]['TYPE'] if cat_data['branches'] else 'Unknown'}\n")
            f.write(f"{'='*50}\n\n")
            
            # Write header
            header = "BRANCH  BRHCODE  "
            for i in range(1, 16):
                header += f"B{i:6} "
            header += "SUBBRH  SUBBR2  SUBACC  SUBAC2  TOTBRH  SOTACC"
            f.write(header + "\n")
            f.write("-" * len(header) + "\n")
            
            # Write branch data
            for branch in cat_data["branches"]:
                line = f"{branch['BRANCH']:6}  {branch['BRHCODE']:7}  "
                for i in range(1, 16):
                    line += f"{branch['BRHAMT'][i]:6.0f} "
                line += f"{branch['SUBBRH']:6.0f}  {branch['SUBBR2']:6.0f}  "
                line += f"{branch['SUBACC']:6}  {branch['SUBAC2']:6}  "
                line += f"{branch['TOTBRH']:6.0f}  {branch['SOTACC']:6}"
                f.write(line + "\n")
            
            # Write totals
            f.write("-" * len(header) + "\n")
            line = "TOTAL         "
            for i in range(1, 16):
                line += f"{cat_data['totamt'][i]:6.0f} "
            line += f"{cat_data['sgtotbrh']:6.0f}  {cat_data['sgtotbr2']:6.0f}  "
            line += f"{cat_data['sgtotacc']:6}  {cat_data['sgtotac2']:6}  "
            line += f"{cat_data['gtotbrh']:6.0f}  {cat_data['gtotacc']:6}"
            f.write(line + "\n\n")
    
    print(f"✓ 15-bucket report saved: {output_file}")

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
    
    # 1. Process REPTDATE (yesterday)
    print("\n1. Processing REPTDATE...")
    variables = process_repdate()
    print(f"   Report Date: {variables['RDATE']}")
    print(f"   Day of Month: {variables['REPTDAY']}")
    
    # 2. Load data
    print("\n2. Loading data...")
    loan_df = load_loan_data()
    branch_df = load_branch_data()
    print(f"   Loans: {len(loan_df)}, Branches: {len(branch_df)}")
    print(f"   Branch columns: {branch_df.columns}")
    
    # 3. Categorize loans
    print("\n3. Categorizing loans...")
    categorized = categorize_loans(loan_df, HPD_LIST)
    print(f"   Categorized records: {len(categorized)}")
    
    # 4. Merge with branch data
    print("\n4. Merging with branch data...")
    
    # Check if BRANCH column exists in both dataframes
    if "BRANCH" not in branch_df.columns:
        # Try to find the branch column
        for col in branch_df.columns:
            if "BRANCH" in col.upper() or "BRH" in col.upper():
                print(f"   Found potential branch column: {col}")
                branch_df = branch_df.rename({col: "BRANCH"})
                break
        
        # If still no BRANCH column, create one
        if "BRANCH" not in branch_df.columns:
            print("   No BRANCH column found in branch file. Using row number as branch.")
            branch_df = branch_df.with_columns(
                pl.Series("BRANCH", range(1, len(branch_df) + 1))
            )
    
    # Convert BRANCH to same type as categorized
    if branch_df["BRANCH"].dtype != pl.Int64:
        try:
            branch_df = branch_df.with_columns(pl.col("BRANCH").cast(pl.Int64))
        except:
            # If cannot cast, try to extract numbers
            branch_df = branch_df.with_columns(
                pl.col("BRANCH").str.extract(r"(\d+)", 1).cast(pl.Int64)
            )
    
    # Perform the join
    merged_data = categorized.join(
        branch_df,
        on="BRANCH",
        how="inner"
    ).sort(["CAT", "BRANCH"])
    print(f"   Merged records: {len(merged_data)}")
    
    # 5. Always generate 17-bucket report
    print("\n5. Generating 17-bucket report (EIMAR102-A)...")
    results_17 = calculate_17_bucket_summaries(merged_data)
    generate_17_bucket_report_text(results_17, variables)
    print(f"   Categories processed: {len(results_17)}")
    
    # 6. Generate 15-bucket report only on day 15
    if variables['REPTDAY'] == '15':
        print("\n6. Day 15 detected - Generating 15-bucket report (EIMAR102-B)...")
        results_15 = calculate_15_bucket_summaries(merged_data)
        generate_15_bucket_report_text(results_15, variables)
        print(f"   Categories processed: {len(results_15)}")
    else:
        print("\n6. Not day 15 - Skipping 15-bucket report")
    
    # 7. Create summary statistics as text
    print("\n7. Creating summary statistics...")
    
    summary_file = OUTPUT_PATH / "SUMMARY_STATISTICS.txt"
    with open(summary_file, 'w') as f:
        f.write("=" * 80 + "\n")
        f.write("EIMIR102 - Summary Statistics\n")
        f.write(f"Report Date: {variables['RDATE']}\n")
        f.write("=" * 80 + "\n\n")
        
        for cat in sorted(merged_data["CAT"].unique().to_list()):
            cat_data = merged_data.filter(pl.col("CAT") == cat)
            
            total_balance = cat_data["BALANCE"].sum()
            total_accounts = len(cat_data)
            avg_balance = total_balance / total_accounts if total_accounts > 0 else 0
            max_arrear = cat_data["ARREAR"].max() if len(cat_data) > 0 else 0
            min_arrear = cat_data["ARREAR"].min() if len(cat_data) > 0 else 0
            
            f.write(f"Category {cat}: {cat_data['TYPE'][0]}\n")
            f.write(f"  Total Accounts: {total_accounts:,}\n")
            f.write(f"  Total Balance: {total_balance:,.2f}\n")
            f.write(f"  Average Balance: {avg_balance:,.2f}\n")
            f.write(f"  Arrear Range: {min_arrear} - {max_arrear}\n\n")
    
    print(f"✓ Summary statistics saved: {summary_file}")
    
    # 8. Save variables as text
    variables_file = OUTPUT_PATH / "EIMIR102_VARIABLES.txt"
    with open(variables_file, 'w') as f:
        f.write("EIMIR102 Variables\n")
        f.write("=" * 40 + "\n")
        for key, value in variables.items():
            f.write(f"{key}: {value}\n")
    
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
