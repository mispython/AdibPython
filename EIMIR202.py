"""
EIMAR202 SAS to Python conversion
Processes NPL HP Direct loans issued from 01 Jan 1998 with stricter criteria
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

def categorize_npl_hpdirect_loans(loan_df: pl.DataFrame) -> pl.DataFrame:
    """Categorize NPL HP Direct loans with stricter criteria (ARREAR > 6 or BORSTAT R/I/F)"""
    
    # Filter: ARREAR > 6 OR BORSTAT in R/I/F (stricter than EIMAR201)
    npl_candidates = loan_df.filter(
        (pl.col("ARREAR") > 6) |
        (pl.col("BORSTAT").is_in(["R", "I", "F"]))
    )
    
    # Additional filter: CHECKDT = 1 (issued from 01 Jan 1998)
    npl_checkdt = npl_candidates.filter(pl.col("CHECKDT") == 1)
    
    categorized_rows = []
    
    # Helper function to create category
    def create_category(df_condition, cat, type_name):
        cat_df = npl_checkdt.filter(df_condition)
        if len(cat_df) > 0:
            return cat_df.with_columns([
                pl.lit(cat).alias("CAT"),
                pl.lit(type_name).alias("TYPE")
            ])
        return None
    
    # Category A: (HPD-C) - Products 380,381,700,705
    cat_a = create_category(
        pl.col("PRODUCT").is_in([380, 381, 700, 705]),
        "A", "(HPD-C)"
    )
    if cat_a is not None:
        categorized_rows.append(cat_a)
    
    # Category B: (HP 380/381) - Products 380,381
    cat_b = create_category(
        pl.col("PRODUCT").is_in([380, 381]),
        "B", "(HP 380/381)"
    )
    if cat_b is not None:
        categorized_rows.append(cat_b)
    
    # Category C: (AITAB) - Products 128,130
    cat_c = create_category(
        pl.col("PRODUCT").is_in([128, 130]),
        "C", "(AITAB)"
    )
    if cat_c is not None:
        categorized_rows.append(cat_c)
    
    # Category D: (-HPD-) - Products 128,130,380,381,700,705
    cat_d = create_category(
        pl.col("PRODUCT").is_in([128, 130, 380, 381, 700, 705]),
        "D", "(-HPD-)"
    )
    if cat_d is not None:
        categorized_rows.append(cat_d)
    
    # Combine all categories
    if categorized_rows:
        return pl.concat(categorized_rows, how="vertical").sort(["BRANCH"])
    else:
        return pl.DataFrame(schema=npl_checkdt.schema)

# ============================================================================
# 3. Calculate 17-Bucket NPL HP Direct Summaries
# ============================================================================

def calculate_17_bucket_npl_hpdirect_summaries(loan_df: pl.DataFrame) -> Dict:
    """Calculate 17-bucket summaries for NPL HP Direct loans"""
    
    # Group by CAT, BRANCH, ARREAR
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
    
    result_dict = {}
    for cat in branch_summary["CAT"].unique().to_list():
        cat_data = branch_summary.filter(pl.col("CAT") == cat)
        
        # Initialize arrays for 17 buckets
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
            
            # Calculate subtotals (same 17-bucket logic)
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
# 4. Generate NPL HP Direct Report Output
# ============================================================================

def create_npl_hpdirect_report(results: Dict, variables: Dict):
    """Create NPL HP Direct report output"""
    
    report_data = []
    for cat, cat_data in results.items():
        # Add branch data
        for branch in cat_data["branches"]:
            report_data.append({
                "REPORT_TYPE": "NPL_HP_DIRECT",
                "PROGID": "EIMAR202",
                "CATEGORY": cat,
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
            "REPORT_TYPE": "NPL_HP_DIRECT_TOTAL",
            "PROGID": "EIMAR202",
            "CATEGORY": cat,
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
        df.write_parquet(OUTPUT_PATH / "NPL_HP_DIRECT_REPORT.parquet")
        print(f"✓ NPL HP Direct report saved: {len(df)} records")
        return df
    return None

# ============================================================================
# 5. NPL HP Direct Specific Analysis
# ============================================================================

def analyze_npl_hpdirect_characteristics(loan_df: pl.DataFrame) -> Dict:
    """Analyze NPL HP Direct loan characteristics"""
    
    analysis_results = {}
    
    for cat in loan_df["CAT"].unique().to_list():
        cat_data = loan_df.filter(pl.col("CAT") == cat)
        
        # NPL reason analysis
        npl_by_reason = {
            "ARREAR_GT_6": len(cat_data.filter(pl.col("ARREAR") > 6)),
            "BORSTAT_R": len(cat_data.filter(pl.col("BORSTAT") == "R")),
            "BORSTAT_I": len(cat_data.filter(pl.col("BORSTAT") == "I")),
            "BORSTAT_F": len(cat_data.filter(pl.col("BORSTAT") == "F"))
        }
        
        # Multiple reasons (accounts may have both ARREAR > 6 AND bad BORSTAT)
        accounts_with_multiple = len(cat_data.filter(
            (pl.col("ARREAR") > 6) & 
            (pl.col("BORSTAT").is_in(["R", "I", "F"]))
        ))
        
        # Product distribution
        product_dist = cat_data.group_by("PRODUCT").agg([
            pl.count().alias("COUNT"),
            pl.sum("BALANCE").alias("TOTAL_BALANCE"),
            pl.mean("ARREAR").alias("AVG_ARREAR")
        ])
        
        # Arrears severity
        arrears_stats = {
            "total_accounts": len(cat_data),
            "total_balance": cat_data["BALANCE"].sum(),
            "avg_balance": cat_data["BALANCE"].mean() if len(cat_data) > 0 else 0,
            "avg_arrear": cat_data["ARREAR"].mean() if len(cat_data) > 0 else 0,
            "max_arrear": cat_data["ARREAR"].max() if len(cat_data) > 0 else 0,
            "min_arrear": cat_data["ARREAR"].min() if len(cat_data) > 0 else 0,
            "accounts_arrear_gt_12": len(cat_data.filter(pl.col("ARREAR") > 12)),
            "accounts_arrear_gt_24": len(cat_data.filter(pl.col("ARREAR") > 24))
        }
        
        analysis_results[cat] = {
            "arrears_stats": arrears_stats,
            "npl_by_reason": npl_by_reason,
            "accounts_with_multiple_reasons": accounts_with_multiple,
            "product_distribution": product_dist.to_dicts(),
            "checkdt_status": "ALL=1 (Issued from 01 Jan 1998)",
            "branches_count": cat_data["BRANCH"].n_unique(),
            "severity_index": (arrears_stats["avg_arrear"] * arrears_stats["total_balance"]) / 1000000 if arrears_stats["total_balance"] > 0 else 0
        }
    
    return analysis_results

# ============================================================================
# 6. Compare NPL HP Direct vs Regular HP Direct
# ============================================================================

def compare_npl_vs_regular_hp(loan_df: pl.DataFrame) -> Dict:
    """Compare NPL HP Direct loans vs regular HP Direct loans"""
    
    # HP Direct products
    hp_products = [128, 130, 380, 381, 700, 705]
    
    # All HP Direct loans with CHECKDT = 1
    all_hp = loan_df.filter(
        pl.col("PRODUCT").is_in(hp_products) & 
        (pl.col("CHECKDT") == 1)
    )
    
    # NPL HP Direct loans (ARREAR > 6 OR BORSTAT R/I/F)
    npl_hp = all_hp.filter(
        (pl.col("ARREAR") > 6) |
        (pl.col("BORSTAT").is_in(["R", "I", "F"]))
    )
    
    # Regular HP Direct loans (not NPL)
    regular_hp = all_hp.filter(
        (pl.col("ARREAR") <= 6) &
        (~pl.col("BORSTAT").is_in(["R", "I", "F"]))
    )
    
    comparison = {
        "total_hp_loans": len(all_hp),
        "npl_hp_loans": len(npl_hp),
        "regular_hp_loans": len(regular_hp),
        "npl_percentage": (len(npl_hp) / len(all_hp) * 100) if len(all_hp) > 0 else 0,
        "npl_total_balance": npl_hp["BALANCE"].sum(),
        "regular_total_balance": regular_hp["BALANCE"].sum(),
        "npl_avg_balance": npl_hp["BALANCE"].mean() if len(npl_hp) > 0 else 0,
        "regular_avg_balance": regular_hp["BALANCE"].mean() if len(regular_hp) > 0 else 0,
        "npl_avg_arrear": npl_hp["ARREAR"].mean() if len(npl_hp) > 0 else 0,
        "regular_avg_arrear": regular_hp["ARREAR"].mean() if len(regular_hp) > 0 else 0,
        "npl_max_arrear": npl_hp["ARREAR"].max() if len(npl_hp) > 0 else 0,
        "regular_max_arrear": regular_hp["ARREAR"].max() if len(regular_hp) > 0 else 0
    }
    
    return comparison

# ============================================================================
# 7. Main Execution
# ============================================================================

def main():
    """Main execution function"""
    print("=" * 60)
    print("EIMAR202 SAS to Python Conversion - NPL HP Direct Loans Report")
    print("=" * 60)
    
    # 1. Process REPTDATE
    print("\n1. Processing REPTDATE...")
    variables = process_repdate()
    print(f"   Report Date: {variables['RDATE']}")
    
    # 2. Load loan data
    print("\n2. Loading loan data...")
    loan_path = INPUT_PATH / "BNM/LOANTEMP.parquet"
    loan_df = pl.read_parquet(loan_path)
    branch_df = load_branch_data()
    print(f"   Total loans: {len(loan_df)}")
    print(f"   Total branches: {len(branch_df)}")
    
    # 3. Analyze NPL criteria distribution
    print("\n3. Analyzing NPL criteria distribution...")
    arrear_gt_6 = len(loan_df.filter(pl.col("ARREAR") > 6))
    borstat_r = len(loan_df.filter(pl.col("BORSTAT") == "R"))
    borstat_i = len(loan_df.filter(pl.col("BORSTAT") == "I"))
    borstat_f = len(loan_df.filter(pl.col("BORSTAT") == "F"))
    print(f"   ARREAR > 6: {arrear_gt_6} loans")
    print(f"   BORSTAT R: {borstat_r} loans")
    print(f"   BORSTAT I: {borstat_i} loans")
    print(f"   BORSTAT F: {borstat_f} loans")
    
    # 4. Categorize NPL HP Direct loans
    print("\n4. Categorizing NPL HP Direct loans...")
    npl_hpdirect_categorized = categorize_npl_hpdirect_loans(loan_df)
    print(f"   NPL HP Direct loans: {len(npl_hpdirect_categorized)}")
    
    # 5. Merge with branch data
    print("\n5. Merging with branch data...")
    merged_npl_hpdirect = npl_hpdirect_categorized.join(
        branch_df,
        on="BRANCH",
        how="inner"
    ).sort(["CAT", "BRANCH"])
    print(f"   Merged NPL HP Direct records: {len(merged_npl_hpdirect)}")
    
    # Save categorized data
    merged_npl_hpdirect.write_parquet(OUTPUT_PATH / "NPL_HP_DIRECT_CATEGORIZED.parquet")
    
    # 6. Calculate 17-bucket summaries
    print("\n6. Calculating 17-bucket summaries...")
    npl_hpdirect_summaries = calculate_17_bucket_npl_hpdirect_summaries(merged_npl_hpdirect)
    print(f"   Categories processed: {len(npl_hpdirect_summaries)}")
    
    # 7. Generate NPL HP Direct report
    print("\n7. Generating NPL HP Direct report (EIMAR202)...")
    report_df = create_npl_hpdirect_report(npl_hpdirect_summaries, variables)
    
    # 8. Analyze NPL HP Direct characteristics
    print("\n8. Analyzing NPL HP Direct characteristics...")
    npl_hpdirect_analysis = analyze_npl_hpdirect_characteristics(merged_npl_hpdirect)
    
    # Save analysis results
    analysis_data = []
    for cat, analysis in npl_hpdirect_analysis.items():
        stats = analysis["arrears_stats"]
        npl_reasons = analysis["npl_by_reason"]
        analysis_data.append({
            "CATEGORY": cat,
            "TOTAL_ACCOUNTS": stats["total_accounts"],
            "TOTAL_BALANCE": stats["total_balance"],
            "AVG_BALANCE": stats["avg_balance"],
            "AVG_ARREAR": stats["avg_arrear"],
            "MAX_ARREAR": stats["max_arrear"],
            "ACCOUNTS_ARREAR_GT_6": npl_reasons["ARREAR_GT_6"],
            "ACCOUNTS_BORSTAT_R": npl_reasons["BORSTAT_R"],
            "ACCOUNTS_BORSTAT_I": npl_reasons["BORSTAT_I"],
            "ACCOUNTS_BORSTAT_F": npl_reasons["BORSTAT_F"],
            "ACCOUNTS_MULTIPLE_REASONS": analysis["accounts_with_multiple_reasons"],
            "SEVERITY_INDEX": analysis["severity_index"]
        })
    
    if analysis_data:
        analysis_df = pl.DataFrame(analysis_data)
        analysis_df.write_parquet(OUTPUT_PATH / "NPL_HP_DIRECT_ANALYSIS.parquet")
        print(f"✓ NPL HP Direct analysis saved: {len(analysis_df)} categories")
    
    # 9. Compare NPL vs Regular HP Direct
    print("\n9. Comparing NPL vs Regular HP Direct loans...")
    comparison = compare_npl_vs_regular_hp(loan_df)
    
    # Save comparison
    comparison_df = pl.DataFrame([comparison])
    comparison_df.write_parquet(OUTPUT_PATH / "NPL_VS_REGULAR_HP_COMPARISON.parquet")
    print(f"✓ NPL vs Regular HP comparison saved")
    
    # 10. Create NPL severity analysis by product
    print("\n10. Creating NPL severity analysis by product...")
    
    # HP products
    hp_products = [128, 130, 380, 381, 700, 705]
    
    product_severity_data = []
    for product in hp_products:
        product_data = merged_npl_hpdirect.filter(pl.col("PRODUCT") == product)
        if len(product_data) > 0:
            # Calculate NPL severity metrics
            total_balance = product_data["BALANCE"].sum()
            avg_arrear = product_data["ARREAR"].mean()
            max_arrear = product_data["ARREAR"].max()
            
            # NPL reason breakdown
            arrear_gt_6_count = len(product_data.filter(pl.col("ARREAR") > 6))
            borstat_r_count = len(product_data.filter(pl.col("BORSTAT") == "R"))
            borstat_i_count = len(product_data.filter(pl.col("BORSTAT") == "I"))
            borstat_f_count = len(product_data.filter(pl.col("BORSTAT") == "F"))
            
            product_severity_data.append({
                "PRODUCT": product,
                "ACCOUNT_COUNT": len(product_data),
                "TOTAL_BALANCE": total_balance,
                "AVG_BALANCE": product_data["BALANCE"].mean(),
                "AVG_ARREAR": avg_arrear,
                "MAX_ARREAR": max_arrear,
                "ARREAR_GT_6_COUNT": arrear_gt_6_count,
                "BORSTAT_R_COUNT": borstat_r_count,
                "BORSTAT_I_COUNT": borstat_i_count,
                "BORSTAT_F_COUNT": borstat_f_count,
                "SEVERITY_SCORE": (avg_arrear * total_balance) / 1000000 if total_balance > 0 else 0
            })
    
    if product_severity_data:
        product_severity_df = pl.DataFrame(product_severity_data)
        product_severity_df.write_parquet(OUTPUT_PATH / "NPL_HP_PRODUCT_SEVERITY.parquet")
        print(f"✓ Product severity analysis saved: {len(product_severity_df)} HP products")
    
    # 11. Create overall summary
    print("\n11. Creating overall summary...")
    
    total_npl_hp_accounts = len(merged_npl_hpdirect)
    total_npl_hp_balance = merged_npl_hpdirect["BALANCE"].sum()
    
    overall_summary = pl.DataFrame([{
        "REPORT_DATE": variables["RDATE"],
        "PROGID": "EIMAR202",
        "TITLE": "OUTSTANDING LOANS CLASSIFIED AS NPL ISSUED FROM 1 JAN 98",
        "TOTAL_NPL_HP_DIRECT_ACCOUNTS": total_npl_hp_accounts,
        "TOTAL_NPL_HP_DIRECT_BALANCE": total_npl_hp_balance,
        "AVG_NPL_HP_DIRECT_BALANCE": total_npl_hp_balance / total_npl_hp_accounts if total_npl_hp_accounts > 0 else 0,
        "CATEGORIES_COUNT": len(npl_hpdirect_summaries),
        "BRANCHES_COUNT": merged_npl_hpdirect["BRANCH"].n_unique(),
        "AVG_ARREAR": merged_npl_hpdirect["ARREAR"].mean() if total_npl_hp_accounts > 0 else 0,
        "MAX_ARREAR": merged_npl_hpdirect["ARREAR"].max() if total_npl_hp_accounts > 0 else 0,
        "CHECKDT_STATUS": "ALL=1 (Issued from 01 Jan 1998)",
        "NPL_CRITERIA": "ARREAR > 6 OR BORSTAT R/I/F"
    }])
    overall_summary.write_parquet(OUTPUT_PATH / "NPL_HP_DIRECT_OVERALL_SUMMARY.parquet")
    
    # 12. Save variables
    variables_df = pl.DataFrame([variables])
    variables_df.write_parquet(OUTPUT_PATH / "EIMAR202_VARIABLES.parquet")
    
    print("\n" + "=" * 60)
    print("CONVERSION COMPLETE")
    print("=" * 60)
    print(f"Total loans processed: {len(loan_df)}")
    print(f"NPL HP Direct loans (CHECKDT=1): {total_npl_hp_accounts}")
    print(f"NPL HP Direct balance: {total_npl_hp_balance:,.2f}")
    print(f"Categories: {len(npl_hpdirect_summaries)}")
    print(f"NPL criteria: ARREAR > 6 OR BORSTAT R/I/F")
    print(f"17-bucket NPL HP Direct report generated: {len(report_df) if report_df is not None else 0} records")
    print(f"Output saved to: {OUTPUT_PATH}")

# ============================================================================
# 8. Run the conversion
# ============================================================================

if __name__ == "__main__":
    main()
