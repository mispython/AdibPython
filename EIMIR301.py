"""
EIMAR301 SAS to Python conversion
Multi-report system for HP Direct loan analysis with different criteria
"""

from pathlib import Path
from datetime import date, datetime
import polars as pl
from typing import Dict, List, Tuple

# Setup paths
BASE_PATH = Path("./data")
INPUT_PATH = BASE_PATH / "input"
OUTPUT_PATH = BASE_PATH / "output"
OUTPUT_PATH.mkdir(parents=True, exist_ok=True)

# ============================================================================
# 1. REPTDATE Processing with Previous Month Calculation
# ============================================================================

def process_repdate() -> Dict[str, str]:
    """Process REPTDATE and calculate previous month date"""
    repdate_path = INPUT_PATH / "BNM/REPTDATE.parquet"
    df = pl.read_parquet(repdate_path)
    repdate = df["REPTDATE"][0]
    
    # Calculate previous month (PMTH, PYEAR, PDATE)
    if repdate.month == 1:
        pmth = 12
        pyear = repdate.year - 1
    else:
        pmth = repdate.month - 1
        pyear = repdate.year
    
    # Create previous month date (1st day of previous month)
    pdate = date(pyear, pmth, 1)
    
    return {
        'RDATE': repdate.strftime("%d%m%y"),  # DDMMYY8.
        'REPTYEAR': str(repdate.year),        # YEAR4.
        'REPTMON': f"{repdate.month:02d}",    # Z2.
        'REPTDAY': f"{repdate.day:02d}",      # Z2.
        'REPTDATE': repdate,
        'PREPTDTE': pdate,                    # Previous month date
        'PMTH': pmth,
        'PYEAR': pyear
    }

# ============================================================================
# 2. Load and Preprocess Data
# ============================================================================

def load_branch_data() -> pl.DataFrame:
    """Load branch header data"""
    branch_path = INPUT_PATH / "BRHFILE.parquet"
    if branch_path.exists():
        return pl.read_parquet(branch_path)
    else:
        return pl.DataFrame(schema={"BRANCH": pl.Int64, "BRHCODE": pl.Utf8})

def load_and_filter_loans(hpd_list: List[str], variables: Dict) -> pl.DataFrame:
    """Load and filter HP Direct loans with basic criteria"""
    
    # Load loan data
    loan_path = INPUT_PATH / "BNM/LOANTEMP.parquet"
    loan_df = pl.read_parquet(loan_path)
    
    # Convert HPD list to numbers
    hpd_numbers = [int(x.strip("'")) for x in hpd_list]
    
    # Apply filters: BALANCE > 0 AND BORSTAT != 'Z' AND PRODUCT IN &HPD
    filtered = loan_df.filter(
        (pl.col("BALANCE") > 0) &
        (pl.col("BORSTAT") != "Z") &
        (pl.col("PRODUCT").is_in(hpd_numbers))
    )
    
    # Load and merge branch data
    branch_df = load_branch_data()
    merged = filtered.join(
        branch_df,
        on="BRANCH",
        how="inner"
    ).sort(["BRANCH"])
    
    return merged

# ============================================================================
# 3. Report A: 2+ Months Arrears and 2 Installments or Less
# ============================================================================

def create_report_a_data(loan_df: pl.DataFrame, variables: Dict) -> pl.DataFrame:
    """Create data for Report A: ARREAR2 >= 3 OR bad BORSTAT OR new loans with DAYDIFF >= 8"""
    
    # First condition: ARREAR2 >= 3 OR BORSTAT in R/I/F/Y
    condition1 = loan_df.filter(
        (pl.col("ARREAR2") >= 3) |
        (pl.col("BORSTAT").is_in(["R", "I", "F", "Y"]))
    )
    
    # Second condition: ISSDTE >= PREPTDTE AND DAYDIFF >= 8 (new loans)
    # Convert PREPTDTE to datetime for comparison
    prept_date = variables['PREPTDTE']
    condition2 = loan_df.filter(
        (pl.col("ISSDTE") >= prept_date) &
        (pl.col("DAYDIFF") >= 8)
    )
    
    # Combine both conditions
    report_a_data = pl.concat([condition1, condition2], how="diagonal").unique()
    
    # Add categories and formatting
    categorized = report_a_data.with_columns([
        # Set ARREAR2 = 15 for BORSTAT = 'F'
        pl.when(pl.col("BORSTAT") == "F")
        .then(pl.lit(15))
        .otherwise(pl.col("ARREAR2"))
        .alias("ARREAR2_ADJ"),
        
        # Create ARREARS classification (simplified)
        pl.when(pl.col("ARREAR2") < 3)
        .then(pl.lit("< 3 MTHS"))
        .when(pl.col("ARREAR2") < 6)
        .then(pl.lit("3-5 MTHS"))
        .when(pl.col("ARREAR2") < 9)
        .then(pl.lit("6-8 MTHS"))
        .when(pl.col("ARREAR2") < 12)
        .then(pl.lit("9-11 MTHS"))
        .otherwise(pl.lit("12+ MTHS"))
        .alias("ARREARS"),
        
        # CACBR classification (simplified - would need external format)
        pl.when(pl.col("BRANCH") < 100)
        .then(pl.lit("000"))
        .otherwise(pl.col("BRANCH").cast(pl.Utf8))
        .alias("CACBR"),
        
        # Categories
        pl.when(pl.col("PRODUCT").is_in([380, 381, 700, 705, 720, 725]))
        .then(pl.lit("A"))
        .when(pl.col("PRODUCT").is_in([380, 381]))
        .then(pl.lit("B"))
        .when(pl.col("PRODUCT").is_in([128, 130, 131, 132]))
        .then(pl.lit("C"))
        .otherwise(pl.lit("D"))
        .alias("CAT"),
        
        # Type descriptions
        pl.when(pl.col("PRODUCT").is_in([380, 381, 700, 705, 720, 725]))
        .then(pl.lit("HP DIRECT(CONV) "))
        .when(pl.col("PRODUCT").is_in([380, 381]))
        .then(pl.lit("HP (380,381) "))
        .when(pl.col("PRODUCT").is_in([128, 130, 131, 132]))
        .then(pl.lit("AITAB "))
        .otherwise(pl.lit("OTHER"))
        .alias("TYPE")
    ])
    
    # Sort for report
    return categorized.sort(["CAT", "BRANCH", "ARREAR2", "BALANCE"], descending=[False, False, False, True])

def generate_report_a_summary(report_a_df: pl.DataFrame, variables: Dict):
    """Generate Report A summary - Non-CAC branches only"""
    
    # Filter for non-CAC branches (CACBR = '000')
    non_cac_data = report_a_df.filter(pl.col("CACBR") == "000")
    
    if len(non_cac_data) == 0:
        print("   No data for non-CAC branches in Report A")
        return
    
    # Group and summarize
    summary_data = []
    
    for cat in sorted(non_cac_data["CAT"].unique().to_list()):
        cat_data = non_cac_data.filter(pl.col("CAT") == cat)
        
        for branch in sorted(cat_data["BRANCH"].unique().to_list()):
            branch_data = cat_data.filter(pl.col("BRANCH") == branch)
            
            # Calculate branch totals
            branch_total = branch_data["BALANCE"].sum()
            branch_accounts = len(branch_data)
            
            # By arrears bucket
            for arrear in sorted(branch_data["ARREAR2"].unique().to_list()):
                arrear_data = branch_data.filter(pl.col("ARREAR2") == arrear)
                arrear_total = arrear_data["BALANCE"].sum()
                arrear_accounts = len(arrear_data)
                
                summary_data.append({
                    "REPORT": "A",
                    "CATEGORY": cat,
                    "BRANCH": branch,
                    "BRHCODE": branch_data["BRHCODE"][0],
                    "ARREAR_BUCKET": arrear,
                    "ACCOUNT_COUNT": arrear_accounts,
                    "TOTAL_BALANCE": arrear_total,
                    "ARREARS_CLASS": arrear_data["ARREARS"][0] if len(arrear_data) > 0 else ""
                })
            
            # Add branch summary
            summary_data.append({
                "REPORT": "A_BRANCH_SUMMARY",
                "CATEGORY": cat,
                "BRANCH": branch,
                "BRHCODE": branch_data["BRHCODE"][0],
                "ARREAR_BUCKET": "ALL",
                "ACCOUNT_COUNT": branch_accounts,
                "TOTAL_BALANCE": branch_total,
                "ARREARS_CLASS": "BRANCH TOTAL"
            })
        
        # Add category summary
        cat_total = cat_data["BALANCE"].sum()
        cat_accounts = len(cat_data)
        summary_data.append({
            "REPORT": "A_CATEGORY_SUMMARY",
            "CATEGORY": cat,
            "BRANCH": "ALL",
            "BRHCODE": "",
            "ARREAR_BUCKET": "ALL",
            "ACCOUNT_COUNT": cat_accounts,
            "TOTAL_BALANCE": cat_total,
            "ARREARS_CLASS": "CATEGORY TOTAL"
        })
    
    if summary_data:
        summary_df = pl.DataFrame(summary_data)
        summary_df.write_parquet(OUTPUT_PATH / "REPORT_A_SUMMARY.parquet")
        print(f"✓ Report A summary saved: {len(summary_df)} records")

# ============================================================================
# 4. Report B: 3-8 Months Arrears (Excluding BORSTAT F/I/R)
# ============================================================================

def create_report_b_data(loan_df: pl.DataFrame, variables: Dict) -> pl.DataFrame:
    """Create data for Report B: ARREAR2 4-9 months, exclude BORSTAT F/I/R"""
    
    report_b_data = loan_df.filter(
        (pl.col("ARREAR2") >= 4) &
        (pl.col("ARREAR2") < 10) &
        (~pl.col("BORSTAT").is_in(["F", "I", "R"]))
    )
    
    # Add categories and formatting
    categorized = report_b_data.with_columns([
        # Categories (same as Report A)
        pl.when(pl.col("PRODUCT").is_in([380, 381, 700, 705, 720, 725]))
        .then(pl.lit("A"))
        .when(pl.col("PRODUCT").is_in([380, 381]))
        .then(pl.lit("B"))
        .when(pl.col("PRODUCT").is_in([128, 130, 131, 132]))
        .then(pl.lit("C"))
        .otherwise(pl.lit("D"))
        .alias("CAT"),
        
        # Type descriptions
        pl.when(pl.col("PRODUCT").is_in([380, 381, 700, 705, 720, 725]))
        .then(pl.lit("HP DIRECT(CONV) "))
        .when(pl.col("PRODUCT").is_in([380, 381]))
        .then(pl.lit("HP (380,381) "))
        .when(pl.col("PRODUCT").is_in([128, 130, 131, 132]))
        .then(pl.lit("AITAB "))
        .otherwise(pl.lit("OTHER"))
        .alias("TYPE"),
        
        # ARREARS classification for 4-9 months
        pl.when(pl.col("ARREAR2") < 6)
        .then(pl.lit("4-5 MTHS"))
        .when(pl.col("ARREAR2") < 8)
        .then(pl.lit("6-7 MTHS"))
        .otherwise(pl.lit("8-9 MTHS"))
        .alias("ARREARS_RANGE")
    ])
    
    # Sort for report
    return categorized.sort(["CAT", "BRANCH", "ARREAR2", "BALANCE"], descending=[False, False, False, True])

def generate_report_b_summary(report_b_df: pl.DataFrame, variables: Dict):
    """Generate Report B summary"""
    
    if len(report_b_df) == 0:
        print("   No data for Report B")
        return
    
    # Group and summarize
    summary_data = []
    
    for cat in sorted(report_b_df["CAT"].unique().to_list()):
        cat_data = report_b_df.filter(pl.col("CAT") == cat)
        
        for branch in sorted(cat_data["BRANCH"].unique().to_list()):
            branch_data = cat_data.filter(pl.col("BRANCH") == branch)
            
            # Calculate branch totals
            branch_total = branch_data["BALANCE"].sum()
            branch_accounts = len(branch_data)
            
            # By arrears bucket
            for arrear in sorted(branch_data["ARREAR2"].unique().to_list()):
                arrear_data = branch_data.filter(pl.col("ARREAR2") == arrear)
                arrear_total = arrear_data["BALANCE"].sum()
                arrear_accounts = len(arrear_data)
                
                summary_data.append({
                    "REPORT": "B",
                    "CATEGORY": cat,
                    "BRANCH": branch,
                    "BRHCODE": branch_data["BRHCODE"][0],
                    "ARREAR_BUCKET": arrear,
                    "ARREARS_RANGE": arrear_data["ARREARS_RANGE"][0] if len(arrear_data) > 0 else "",
                    "ACCOUNT_COUNT": arrear_accounts,
                    "TOTAL_BALANCE": arrear_total
                })
            
            # Add branch summary
            summary_data.append({
                "REPORT": "B_BRANCH_SUMMARY",
                "CATEGORY": cat,
                "BRANCH": branch,
                "BRHCODE": branch_data["BRHCODE"][0],
                "ARREAR_BUCKET": "ALL",
                "ARREARS_RANGE": "4-9 MTHS",
                "ACCOUNT_COUNT": branch_accounts,
                "TOTAL_BALANCE": branch_total
            })
        
        # Add category summary
        cat_total = cat_data["BALANCE"].sum()
        cat_accounts = len(cat_data)
        summary_data.append({
            "REPORT": "B_CATEGORY_SUMMARY",
            "CATEGORY": cat,
            "BRANCH": "ALL",
            "BRHCODE": "",
            "ARREAR_BUCKET": "ALL",
            "ARREARS_RANGE": "4-9 MTHS",
            "ACCOUNT_COUNT": cat_accounts,
            "TOTAL_BALANCE": cat_total
        })
    
    if summary_data:
        summary_df = pl.DataFrame(summary_data)
        summary_df.write_parquet(OUTPUT_PATH / "REPORT_B_SUMMARY.parquet")
        print(f"✓ Report B summary saved: {len(summary_df)} records")

# ============================================================================
# 5. Report C: New Releases Summary (2 Installments or Less)
# ============================================================================

def create_report_c_data(loan_df: pl.DataFrame, variables: Dict) -> pl.DataFrame:
    """Create data for Report C: New loans with DAYDIFF >= 8, payment summary"""
    
    # Filter: ISSDTE >= PREPTDTE AND DAYDIFF >= 8
    prept_date = variables['PREPTDTE']
    new_loans = loan_df.filter(
        (pl.col("ISSDTE") >= prept_date) &
        (pl.col("DAYDIFF") >= 8)
    )
    
    # Add payment description
    report_c_data = new_loans.with_columns([
        pl.when(pl.col("NOISTLPD") < 1)
        .then(pl.lit("NO PAYMENT"))
        .when((pl.col("NOISTLPD") >= 1) & (pl.col("NOISTLPD") < 2))
        .then(pl.lit("PAID 1 ISTL"))
        .otherwise(pl.lit("PAID 2 ISTL"))
        .alias("PAYDESC")
    ])
    
    return report_c_data

def generate_report_c_summary(report_c_df: pl.DataFrame, variables: Dict):
    """Generate Report C summary by branch and payment type"""
    
    if len(report_c_df) == 0:
        print("   No data for Report C")
        return
    
    # Group by BRHCODE and PAYDESC
    summary = report_c_df.group_by(["BRHCODE", "PAYDESC"]).agg([
        pl.count().alias("NOACCT"),
        pl.sum("BALANCE").alias("BALANCE_SUM")
    ])
    
    # Add total row for each branch
    branch_totals = report_c_df.group_by("BRHCODE").agg([
        pl.count().alias("TOTAL_NOACCT"),
        pl.sum("BALANCE").alias("TOTAL_BALANCE")
    ])
    
    # Combine summaries
    summary_data = []
    for row in summary.iter_rows(named=True):
        summary_data.append({
            "REPORT": "C",
            "BRHCODE": row["BRHCODE"],
            "PAYDESC": row["PAYDESC"],
            "NO_OF_AC": row["NOACCT"],
            "OS_BALANCE": row["BALANCE_SUM"]
        })
    
    for row in branch_totals.iter_rows(named=True):
        summary_data.append({
            "REPORT": "C_TOTAL",
            "BRHCODE": row["BRHCODE"],
            "PAYDESC": "TOTAL",
            "NO_OF_AC": row["TOTAL_NOACCT"],
            "OS_BALANCE": row["TOTAL_BALANCE"]
        })
    
    if summary_data:
        summary_df = pl.DataFrame(summary_data)
        summary_df.write_parquet(OUTPUT_PATH / "REPORT_C_SUMMARY.parquet")
        print(f"✓ Report C summary saved: {len(summary_df)} records")

# ============================================================================
# 6. Report D: Accounts with Exactly 2 Installments Paid
# ============================================================================

def create_report_d_data(loan_df: pl.DataFrame, variables: Dict) -> pl.DataFrame:
    """Create data for Report D: Accounts with exactly 2 installments paid"""
    
    report_d_data = loan_df.filter(
        (pl.col("NOISTLPD") >= 2) &
        (pl.col("NOISTLPD") < 3) &
        (pl.col("DAYDIFF") >= 8)
    )
    
    # Add payment description
    report_d_data = report_d_data.with_columns(
        pl.lit("PAID 2 ISTL").alias("PAYDESC")
    )
    
    return report_d_data

def generate_report_d_summary(report_d_df: pl.DataFrame, variables: Dict):
    """Generate Report D summary by branch"""
    
    if len(report_d_df) == 0:
        print("   No data for Report D")
        return
    
    # Group by BRHCODE
    summary = report_d_df.group_by("BRHCODE").agg([
        pl.count().alias("NOACCT"),
        pl.sum("BALANCE").alias("BALANCE_SUM")
    ])
    
    # Add grand total
    grand_total = report_d_df.agg([
        pl.count().alias("TOTAL_NOACCT"),
        pl.sum("BALANCE").alias("TOTAL_BALANCE")
    ])
    
    summary_data = []
    for row in summary.iter_rows(named=True):
        summary_data.append({
            "REPORT": "D",
            "BRHCODE": row["BRHCODE"],
            "PAYDESC": "PAID 2 ISTL",
            "NO_OF_AC": row["NOACCT"],
            "OS_BALANCE": row["BALANCE_SUM"]
        })
    
    for row in grand_total.iter_rows(named=True):
        summary_data.append({
            "REPORT": "D_TOTAL",
            "BRHCODE": "TOTAL",
            "PAYDESC": "PAID 2 ISTL",
            "NO_OF_AC": row["TOTAL_NOACCT"],
            "OS_BALANCE": row["TOTAL_BALANCE"]
        })
    
    if summary_data:
        summary_df = pl.DataFrame(summary_data)
        summary_df.write_parquet(OUTPUT_PATH / "REPORT_D_SUMMARY.parquet")
        print(f"✓ Report D summary saved: {len(summary_df)} records")

# ============================================================================
# 7. Main Execution
# ============================================================================

def main():
    """Main execution function"""
    print("=" * 60)
    print("EIMAR301 SAS to Python Conversion - Multi-Report System")
    print("=" * 60)
    
    # HPD list (would come from macro variable &HPD)
    HPD_LIST = ["110", "115", "700", "705"]
    
    # 1. Process REPTDATE with previous month calculation
    print("\n1. Processing REPTDATE with previous month...")
    variables = process_repdate()
    print(f"   Current Date: {variables['RDATE']}")
    print(f"   Previous Month Date: {variables['PREPTDTE']}")
    
    # 2. Load and filter loans
    print("\n2. Loading and filtering HP Direct loans...")
    filtered_loans = load_and_filter_loans(HPD_LIST, variables)
    print(f"   Filtered HP Direct loans: {len(filtered_loans)}")
    
    # 3. Generate Report A
    print("\n3. Generating Report A (EIMAR301-A)...")
    report_a_data = create_report_a_data(filtered_loans, variables)
    print(f"   Report A accounts: {len(report_a_data)}")
    generate_report_a_summary(report_a_data, variables)
    
    # 4. Generate Report B
    print("\n4. Generating Report B (EIMAR301-B)...")
    report_b_data = create_report_b_data(filtered_loans, variables)
    print(f"   Report B accounts: {len(report_b_data)}")
    generate_report_b_summary(report_b_data, variables)
    
    # 5. Generate Report C
    print("\n5. Generating Report C (EIMAR301-C)...")
    report_c_data = create_report_c_data(filtered_loans, variables)
    print(f"   Report C accounts: {len(report_c_data)}")
    generate_report_c_summary(report_c_data, variables)
    
    # 6. Generate Report D
    print("\n6. Generating Report D (EIMAR301-D)...")
    report_d_data = create_report_d_data(filtered_loans, variables)
    print(f"   Report D accounts: {len(report_d_data)}")
    generate_report_d_summary(report_d_data, variables)
    
    # 7. Create combined analysis
    print("\n7. Creating combined analysis...")
    
    # Overall statistics
    overall_stats = {
        "TOTAL_HP_LOANS": len(filtered_loans),
        "REPORT_A_ACCOUNTS": len(report_a_data),
        "REPORT_B_ACCOUNTS": len(report_b_data),
        "REPORT_C_ACCOUNTS": len(report_c_data),
        "REPORT_D_ACCOUNTS": len(report_d_data),
        "TOTAL_BALANCE": filtered_loans["BALANCE"].sum(),
        "AVG_BALANCE": filtered_loans["BALANCE"].mean(),
        "AVG_ARREAR": filtered_loans["ARREAR2"].mean(),
        "NEW_LOANS_COUNT": len(filtered_loans.filter(pl.col("ISSDTE") >= variables['PREPTDTE'])),
        "NPL_COUNT": len(filtered_loans.filter(
            (pl.col("ARREAR2") >= 3) |
            (pl.col("BORSTAT").is_in(["R", "I", "F", "Y"]))
        ))
    }
    
    overall_df = pl.DataFrame([overall_stats])
    overall_df.write_parquet(OUTPUT_PATH / "OVERALL_STATISTICS.parquet")
    
    # 8. Create detailed data extracts
    print("\n8. Creating detailed data extracts...")
    
    # Save detailed data for each report
    report_a_data.write_parquet(OUTPUT_PATH / "REPORT_A_DETAILED.parquet")
    report_b_data.write_parquet(OUTPUT_PATH / "REPORT_B_DETAILED.parquet")
    report_c_data.write_parquet(OUTPUT_PATH / "REPORT_C_DETAILED.parquet")
    report_d_data.write_parquet(OUTPUT_PATH / "REPORT_D_DETAILED.parquet")
    
    # 9. Create branch performance analysis
    print("\n9. Creating branch performance analysis...")
    
    # Calculate key metrics per branch
    branch_metrics = []
    for branch in filtered_loans["BRANCH"].unique().to_list():
        branch_data = filtered_loans.filter(pl.col("BRANCH") == branch)
        
        # NPL ratio
        npl_count = len(branch_data.filter(
            (pl.col("ARREAR2") >= 3) |
            (pl.col("BORSTAT").is_in(["R", "I", "F", "Y"]))
        ))
        
        # New loans ratio
        new_loans = len(branch_data.filter(pl.col("ISSDTE") >= variables['PREPTDTE']))
        
        # Average arrears
        avg_arrear = branch_data["ARREAR2"].mean()
        
        # Payment performance
        paid_2_or_less = len(branch_data.filter(pl.col("NOISTLPD") <= 2))
        
        branch_metrics.append({
            "BRANCH": branch,
            "BRHCODE": branch_data["BRHCODE"][0] if len(branch_data) > 0 else "",
            "TOTAL_ACCOUNTS": len(branch_data),
            "TOTAL_BALANCE": branch_data["BALANCE"].sum(),
            "NPL_COUNT": npl_count,
            "NPL_PERCENTAGE": (npl_count / len(branch_data) * 100) if len(branch_data) > 0 else 0,
            "NEW_LOANS_COUNT": new_loans,
            "AVG_ARREAR": avg_arrear,
            "PAID_2_OR_LESS": paid_2_or_less,
            "PAYMENT_RATIO": (paid_2_or_less / len(branch_data) * 100) if len(branch_data) > 0 else 0
        })
    
    if branch_metrics:
        branch_df = pl.DataFrame(branch_metrics)
        branch_df.write_parquet(OUTPUT_PATH / "BRANCH_PERFORMANCE.parquet")
        print(f"✓ Branch performance analysis saved: {len(branch_df)} branches")
    
    # 10. Save variables
    variables_df = pl.DataFrame([variables])
    variables_df.write_parquet(OUTPUT_PATH / "EIMAR301_VARIABLES.parquet")
    
    print("\n" + "=" * 60)
    print("CONVERSION COMPLETE")
    print("=" * 60)
    print(f"Total HP Direct loans processed: {len(filtered_loans)}")
    print(f"Report A (2+ months arrears): {len(report_a_data)}")
    print(f"Report B (3-8 months arrears): {len(report_b_data)}")
    print(f"Report C (New releases): {len(report_c_data)}")
    print(f"Report D (2 installments paid): {len(report_d_data)}")
    print(f"Previous month date: {variables['PREPTDTE']}")
    print(f"Output saved to: {OUTPUT_PATH}")

# ============================================================================
# 8. Run the conversion
# ============================================================================

if __name__ == "__main__":
    main()
