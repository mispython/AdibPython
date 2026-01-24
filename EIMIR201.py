"""
EIMIR104 SAS to Python conversion
Processes NPL (Non-Performing Loan) reports with 17-bucket structure
Combines EIMIR103 NPL logic with EIMIR102 17-bucket format
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

def extract_census9(census_value: float) -> str:
    """Extract 7th character from formatted census (8.2 format)"""
    formatted = f"{census_value:8.2f}"  # 8.2 format with spaces
    return formatted[6] if len(formatted) >= 7 else ' '

def categorize_npl_loans_17bucket(loan_df: pl.DataFrame, hpd_list: List[str]) -> pl.DataFrame:
    """Categorize NPL loans for 17-bucket report (uses ARREAR instead of ARREAR2)"""
    
    # Filter: BALANCE > 0 AND BORSTAT != 'Z'
    filtered_df = loan_df.filter(
        (pl.col("BALANCE") > 0) & (pl.col("BORSTAT") != "Z")
    )
    
    # Add CENSUS9 column
    filtered_df = filtered_df.with_columns(
        pl.col("CENSUS").map_elements(extract_census9, return_dtype=pl.Utf8).alias("CENSUS9")
    )
    
    # Main filter: ARREAR > 3 OR BORSTAT in R/I/F OR CENSUS9 = '9' OR USER5 = 'N'
    npl_candidates = filtered_df.filter(
        (pl.col("ARREAR") > 3) |
        (pl.col("BORSTAT").is_in(["R", "I", "F"])) |
        (pl.col("CENSUS9") == "9") |
        (pl.col("USER5") == "N")
    )
    
    categorized_rows = []
    
    # Helper function for NPL categories (same as EIMIR103)
    def create_npl_category(df_condition, cat, type_name, product_list):
        # Additional filter: BORSTAT in R/I/F OR ARREAR > 3 OR USER5 = 'N'
        final_condition = df_condition & (
            pl.col("BORSTAT").is_in(["R", "I", "F"]) |
            (pl.col("ARREAR") > 3) |
            (pl.col("USER5") == "N")
        )
        
        cat_df = npl_candidates.filter(final_condition)
        if len(cat_df) > 0:
            return cat_df.with_columns([
                pl.lit(cat).alias("CAT"),
                pl.lit(type_name).alias("TYPE")
            ])
        return None
    
    # Category A: (HPD-C)
    cat_a = create_npl_category(
        pl.col("PRODUCT").is_in([380, 381, 700, 705, 720, 725]),
        "A", "(HPD-C)", [380, 381, 700, 705, 720, 725]
    )
    if cat_a is not None:
        categorized_rows.append(cat_a)
    
    # Category B: (HP 380/381)
    cat_b = create_npl_category(
        pl.col("PRODUCT").is_in([380, 381]),
        "B", "(HP 380/381)", [380, 381]
    )
    if cat_b is not None:
        categorized_rows.append(cat_b)
    
    # Category C: (AITAB)
    cat_c = create_npl_category(
        pl.col("PRODUCT").is_in([103, 104, 107, 108, 128, 130, 131, 132]),
        "C", "(AITAB)", [103, 104, 107, 108, 128, 130, 131, 132]
    )
    if cat_c is not None:
        categorized_rows.append(cat_c)
    
    # Category D: (-HPD-)
    hpd_numbers = [int(x.strip("'")) for x in hpd_list]
    cat_d = create_npl_category(
        pl.col("PRODUCT").is_in(hpd_numbers),
        "D", "(-HPD-)", hpd_numbers
    )
    if cat_d is not None:
        categorized_rows.append(cat_d)
    
    # Combine all categories
    if categorized_rows:
        return pl.concat(categorized_rows, how="vertical").sort(["BRANCH"])
    else:
        return pl.DataFrame(schema=filtered_df.schema)

# ============================================================================
# 3. Calculate 17-Bucket NPL Summaries
# ============================================================================

def calculate_17_bucket_npl_summaries(loan_df: pl.DataFrame) -> Dict:
    """Calculate 17-bucket NPL summaries (combines NPL logic with 17 buckets)"""
    
    # Group by CAT, BRANCH, ARREAR (not ARREAR2)
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
            
            # Calculate subtotals (17-bucket logic from EIMIR102)
            # SUBBRH = SUM(buckets 4-17)
            subbrh = sum(branhamt[4:18])
            # SUBBR2 = SUBBRH - buckets 4-6
            subbr2 = subbrh - sum(branhamt[4:7])
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
# 4. Generate 17-Bucket NPL Report Output
# ============================================================================

def create_17_bucket_npl_report(results: Dict, variables: Dict):
    """Create 17-bucket NPL report output"""
    
    report_data = []
    for cat, cat_data in results.items():
        # Add branch data
        for branch in cat_data["branches"]:
            report_data.append({
                "REPORT_TYPE": "17_BUCKET_NPL",
                "PROGID": "EIMAR104",
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
            "REPORT_TYPE": "17_BUCKET_NPL_TOTAL",
            "PROGID": "EIMAR104",
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
        df.write_parquet(OUTPUT_PATH / "17_BUCKET_NPL_REPORT.parquet")
        print(f"✓ 17-bucket NPL report saved: {len(df)} records")
        return df
    return None

# ============================================================================
# 5. NPL Bucket Analysis
# ============================================================================

def analyze_npl_by_buckets(loan_df: pl.DataFrame) -> Dict:
    """Analyze NPL distribution across 17 buckets"""
    
    analysis_results = {}
    
    for cat in loan_df["CAT"].unique().to_list():
        cat_data = loan_df.filter(pl.col("CAT") == cat)
        
        # Distribution across buckets 1-17
        bucket_dist = {}
        for bucket in range(1, 18):
            bucket_data = cat_data.filter(pl.col("ARREAR") == bucket)
            if len(bucket_data) > 0:
                bucket_dist[f"BUCKET_{bucket}"] = {
                    "ACCOUNT_COUNT": len(bucket_data),
                    "TOTAL_BALANCE": bucket_data["BALANCE"].sum(),
                    "AVG_BALANCE": bucket_data["BALANCE"].mean()
                }
        
        # Focus on NPL buckets (4-17)
        npl_buckets = {}
        for bucket in range(4, 18):
            bucket_data = cat_data.filter(pl.col("ARREAR") == bucket)
            if len(bucket_data) > 0:
                npl_buckets[f"BUCKET_{bucket}"] = len(bucket_data)
        
        # Severe NPL buckets (7-17)
        severe_npl_buckets = {}
        for bucket in range(7, 18):
            bucket_data = cat_data.filter(pl.col("ARREAR") == bucket)
            if len(bucket_data) > 0:
                severe_npl_buckets[f"BUCKET_{bucket}"] = len(bucket_data)
        
        analysis_results[cat] = {
            "total_accounts": len(cat_data),
            "total_balance": cat_data["BALANCE"].sum(),
            "bucket_distribution": bucket_dist,
            "npl_accounts_buckets_4_17": sum(npl_buckets.values()),
            "severe_npl_accounts_buckets_7_17": sum(severe_npl_buckets.values()),
            "avg_arrear": cat_data["ARREAR"].mean() if len(cat_data) > 0 else 0,
            "max_arrear": cat_data["ARREAR"].max() if len(cat_data) > 0 else 0
        }
    
    return analysis_results

# ============================================================================
# 6. Main Execution
# ============================================================================

def main():
    """Main execution function"""
    print("=" * 60)
    print("EIMIR104 SAS to Python Conversion - 17-Bucket NPL Report")
    print("=" * 60)
    
    # HPD list (would come from macro variable &HPD)
    HPD_LIST = ["110", "115", "700", "705"]
    
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
    
    # 3. Categorize NPL loans for 17-bucket report
    print("\n3. Categorizing NPL loans (17-bucket)...")
    npl_categorized = categorize_npl_loans_17bucket(loan_df, HPD_LIST)
    print(f"   NPL candidates: {len(npl_categorized)}")
    
    # 4. Merge with branch data
    print("\n4. Merging with branch data...")
    merged_npl = npl_categorized.join(
        branch_df,
        on="BRANCH",
        how="inner"
    ).sort(["CAT", "BRANCH"])
    print(f"   Merged NPL records: {len(merged_npl)}")
    
    # Save merged data
    merged_npl.write_parquet(OUTPUT_PATH / "NPL_17BUCKET_CATEGORIZED.parquet")
    
    # 5. Calculate 17-bucket NPL summaries
    print("\n5. Calculating 17-bucket NPL summaries...")
    npl_summaries = calculate_17_bucket_npl_summaries(merged_npl)
    print(f"   Categories processed: {len(npl_summaries)}")
    
    # 6. Generate 17-bucket NPL report
    print("\n6. Generating 17-bucket NPL report (EIMAR104)...")
    report_df = create_17_bucket_npl_report(npl_summaries, variables)
    
    # 7. Analyze NPL by buckets
    print("\n7. Analyzing NPL distribution by buckets...")
    bucket_analysis = analyze_npl_by_buckets(merged_npl)
    
    # Save bucket analysis
    analysis_data = []
    for cat, analysis in bucket_analysis.items():
        analysis_data.append({
            "CATEGORY": cat,
            "TOTAL_ACCOUNTS": analysis["total_accounts"],
            "TOTAL_BALANCE": analysis["total_balance"],
            "AVG_ARREAR": analysis["avg_arrear"],
            "MAX_ARREAR": analysis["max_arrear"],
            "NPL_ACCOUNTS_BUCKETS_4_17": analysis["npl_accounts_buckets_4_17"],
            "SEVERE_NPL_ACCOUNTS_BUCKETS_7_17": analysis["severe_npl_accounts_buckets_7_17"],
            "NPL_PERCENTAGE": (analysis["npl_accounts_buckets_4_17"] / analysis["total_accounts"] * 100) 
                              if analysis["total_accounts"] > 0 else 0,
            "SEVERE_NPL_PERCENTAGE": (analysis["severe_npl_accounts_buckets_7_17"] / analysis["total_accounts"] * 100)
                                     if analysis["total_accounts"] > 0 else 0
        })
    
    if analysis_data:
        analysis_df = pl.DataFrame(analysis_data)
        analysis_df.write_parquet(OUTPUT_PATH / "NPL_BUCKET_ANALYSIS.parquet")
        print(f"✓ NPL bucket analysis saved: {len(analysis_df)} categories")
    
    # 8. Create detailed bucket breakdown
    print("\n8. Creating detailed bucket breakdown...")
    
    # Create bucket-level summary
    bucket_summary_data = []
    for cat in merged_npl["CAT"].unique().to_list():
        cat_data = merged_npl.filter(pl.col("CAT") == cat)
        
        for bucket in range(1, 18):
            bucket_data = cat_data.filter(pl.col("ARREAR") == bucket)
            if len(bucket_data) > 0:
                bucket_summary_data.append({
                    "CATEGORY": cat,
                    "TYPE": bucket_data["TYPE"][0],
                    "BUCKET": bucket,
                    "ACCOUNT_COUNT": len(bucket_data),
                    "TOTAL_BALANCE": bucket_data["BALANCE"].sum(),
                    "AVG_BALANCE": bucket_data["BALANCE"].mean(),
                    "BUCKET_LABEL": get_bucket_label(bucket)
                })
    
    if bucket_summary_data:
        bucket_summary_df = pl.DataFrame(bucket_summary_data)
        bucket_summary_df.write_parquet(OUTPUT_PATH / "NPL_DETAILED_BUCKET_SUMMARY.parquet")
        print(f"✓ Detailed bucket summary saved: {len(bucket_summary_df)} rows")
    
    # 9. Create overall summary
    print("\n9. Creating overall summary...")
    
    total_npl_accounts = len(merged_npl)
    total_npl_balance = merged_npl["BALANCE"].sum()
    
    # NPL by category summary
    category_summary = merged_npl.group_by(["CAT", "TYPE"]).agg([
        pl.count().alias("ACCOUNT_COUNT"),
        pl.sum("BALANCE").alias("TOTAL_BALANCE"),
        pl.mean("BALANCE").alias("AVG_BALANCE"),
        pl.mean("ARREAR").alias("AVG_ARREAR"),
        pl.max("ARREAR").alias("MAX_ARREAR")
    ])
    
    category_summary.write_parquet(OUTPUT_PATH / "NPL_CATEGORY_SUMMARY.parquet")
    
    # Overall summary
    overall_summary = pl.DataFrame([{
        "REPORT_DATE": variables["RDATE"],
        "PROGID": "EIMAR104",
        "TOTAL_NPL_ACCOUNTS": total_npl_accounts,
        "TOTAL_NPL_BALANCE": total_npl_balance,
        "AVG_NPL_BALANCE": total_npl_balance / total_npl_accounts if total_npl_accounts > 0 else 0,
        "CATEGORIES_COUNT": len(npl_summaries),
        "BRANCHES_COUNT": merged_npl["BRANCH"].n_unique(),
        "AVG_ARREAR": merged_npl["ARREAR"].mean() if total_npl_accounts > 0 else 0,
        "MAX_ARREAR": merged_npl["ARREAR"].max() if total_npl_accounts > 0 else 0
    }])
    overall_summary.write_parquet(OUTPUT_PATH / "NPL_OVERALL_SUMMARY.parquet")
    
    # 10. Save variables
    variables_df = pl.DataFrame([variables])
    variables_df.write_parquet(OUTPUT_PATH / "EIMIR104_VARIABLES.parquet")
    
    print("\n" + "=" * 60)
    print("CONVERSION COMPLETE")
    print("=" * 60)
    print(f"Total loans processed: {len(loan_df)}")
    print(f"NPL accounts identified: {total_npl_accounts}")
    print(f"NPL balance: {total_npl_balance:,.2f}")
    print(f"Categories: {len(npl_summaries)}")
    print(f"17-bucket NPL report generated: {len(report_df) if report_df is not None else 0} records")
    print(f"Output saved to: {OUTPUT_PATH}")

def get_bucket_label(bucket: int) -> str:
    """Get bucket label for 17-bucket structure"""
    labels = {
        1: "< 1 MTH",
        2: "1 TO < 2 MTH",
        3: "2 TO < 3 MTH",
        4: "3 TO < 4 MTH",
        5: "4 TO < 5 MTH",
        6: "5 TO < 6 MTH",
        7: "6 TO < 7 MTH",
        8: "7 TO < 8 MTH",
        9: "8 TO < 9 MTH",
        10: "9 TO < 10 MTH",
        11: "10 TO < 11 MTH",
        12: "11 TO < 12 MTH",
        13: "12 TO < 18 MTH",
        14: "18 TO < 24 MTH",
        15: "24 TO < 36 MTH",
        16: "> 36 MTH",
        17: "DEFICIT"
    }
    return labels.get(bucket, f"BUCKET_{bucket}")

# ============================================================================
# 7. Run the conversion
# ============================================================================

if __name__ == "__main__":
    main()
