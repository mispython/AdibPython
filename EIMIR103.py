"""
EIMIR103 SAS to Python conversion
Processes NPL (Non-Performing Loan) reports with stricter criteria
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
    # Simulating SUBSTR(PUT(CENSUS,8.2),7,1)
    formatted = f"{census_value:8.2f}"  # 8.2 format with spaces
    return formatted[6] if len(formatted) >= 7 else ' '  # 7th character (0-indexed 6)

def categorize_npl_loans(loan_df: pl.DataFrame, hpd_list: List[str]) -> pl.DataFrame:
    """Categorize NPL loans with stricter criteria"""
    
    # Filter: BALANCE > 0 AND BORSTAT != 'Z'
    filtered_df = loan_df.filter(
        (pl.col("BALANCE") > 0) & (pl.col("BORSTAT") != "Z")
    )
    
    # Add CENSUS9 column
    filtered_df = filtered_df.with_columns(
        pl.col("CENSUS").map_elements(extract_census9, return_dtype=pl.Utf8).alias("CENSUS9")
    )
    
    # Main filter: ARREAR2 > 3 OR BORSTAT in R/I/F OR CENSUS9 = '9' OR USER5 = 'N'
    npl_candidates = filtered_df.filter(
        (pl.col("ARREAR2") > 3) |
        (pl.col("BORSTAT").is_in(["R", "I", "F"])) |
        (pl.col("CENSUS9") == "9") |
        (pl.col("USER5") == "N")
    )
    
    categorized_rows = []
    
    # Helper function for NPL categories
    def create_npl_category(df_condition, cat, type_name, product_list):
        # Additional filter: BORSTAT in R/I/F OR ARREAR2 > 3 OR USER5 = 'N'
        final_condition = df_condition & (
            pl.col("BORSTAT").is_in(["R", "I", "F"]) |
            (pl.col("ARREAR2") > 3) |
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
    
    # Category C: (AITAB) - Different product list for NPL
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
# 3. Calculate 14-Bucket NPL Summaries
# ============================================================================

def calculate_npl_summaries(loan_df: pl.DataFrame) -> Dict:
    """Calculate 14-bucket NPL summaries"""
    
    # Group by CAT, BRANCH, ARREAR2
    branch_summary = loan_df.group_by(["CAT", "BRANCH", "ARREAR2"]).agg([
        pl.col("BRHCODE").first().alias("BRHCODE"),
        pl.col("TYPE").first().alias("TYPE"),
        pl.col("BALANCE").sum().alias("BRHAMT"),
        pl.count().alias("NOACC")
    ])
    
    # Filter ARREAR2 values 1-14 (but NPL focuses on >= 3 months)
    branch_summary = branch_summary.filter(
        (pl.col("ARREAR2") >= 1) & (pl.col("ARREAR2") <= 14)
    )
    
    result_dict = {}
    for cat in branch_summary["CAT"].unique().to_list():
        cat_data = branch_summary.filter(pl.col("CAT") == cat)
        
        # Initialize arrays
        totamt = [0.0] * 15  # 1-14 (index 0 unused)
        totacc = [0] * 15
        branch_results = []
        
        # Process each branch
        for branch in cat_data["BRANCH"].unique().to_list():
            branch_data = cat_data.filter(pl.col("BRANCH") == branch)
            
            # Initialize branch arrays
            branhamt = [0.0] * 15
            noacc = [0] * 15
            
            # Fill branch arrays
            for row in branch_data.iter_rows(named=True):
                arrear = int(row["ARREAR2"])
                branhamt[arrear] = row["BRHAMT"]
                noacc[arrear] = row["NOACC"]
            
            # Calculate subtotals (NPL specific)
            subbrh = sum(branhamt[4:15])  # 4-14 (>=3 months)
            subbr2 = sum(branhamt[7:15])  # 7-14 (>=6 months)
            subacc = sum(noacc[4:15])
            subac2 = sum(noacc[7:15])
            totbrh = subbrh + sum(branhamt[1:4])
            sotacc = subacc + sum(noacc[1:4])
            
            # Add to totals
            for i in range(1, 15):
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
        sgtotbrh = sum(totamt[4:15])
        sgtotbr2 = sum(totamt[7:15])
        sgtotacc = sum(totacc[4:15])
        sgtotac2 = sum(totacc[7:15])
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
# 4. Generate NPL Report Outputs
# ============================================================================

def generate_npl_report_a(loan_df: pl.DataFrame, variables: Dict) -> pl.DataFrame:
    """Generate first NPL report (EIMAR103-A) - All NPL data"""
    report_df = loan_df.with_columns(
        pl.lit("EIMAR103-A").alias("PROGID")
    )
    
    # Save for processing
    report_df.write_parquet(OUTPUT_PATH / "PRNDATA_A.parquet")
    
    return report_df

def generate_npl_report_b(loan_df: pl.DataFrame, variables: Dict, hpd_list: List[str]) -> pl.DataFrame:
    """Generate second NPL report (EIMAR103-B) with exclusions"""
    
    # Convert HPD list
    hpd_numbers = [int(x.strip("'")) for x in hpd_list]
    
    # Filter: exclude certain types AND exclude BORSTAT F/I/R
    filtered = loan_df.filter(
        (~pl.col("TYPE").is_in(["(AITAB)", "(-HPD-)"])) &
        (~pl.col("BORSTAT").is_in(["F", "I", "R"])) &
        (pl.col("PRODUCT").is_in(hpd_numbers + [103, 104, 107, 108]))
    )
    
    report_df = filtered.with_columns(
        pl.lit("EIMAR103-B").alias("PROGID")
    )
    
    # Save for processing
    report_df.write_parquet(OUTPUT_PATH / "PRNDATA_B.parquet")
    
    return report_df

def create_npl_summary_output(results: Dict, report_type: str, variables: Dict):
    """Create NPL summary output for a specific report type"""
    
    report_data = []
    for cat, cat_data in results.items():
        # Add branch data
        for branch in cat_data["branches"]:
            report_data.append({
                "REPORT_TYPE": report_type,
                "CATEGORY": cat,
                "PROGID": f"EIMAR103-{report_type[-1]}",
                "BRANCH": branch["BRANCH"],
                "BRHCODE": branch["BRHCODE"],
                **{f"NOACC{i}": branch["NOACC"][i] for i in range(1, 15)},
                **{f"BRHAMT{i}": branch["BRHAMT"][i] for i in range(1, 15)},
                "SUBBRH": branch["SUBBRH"],
                "SUBBR2": branch["SUBBR2"],
                "SUBACC": branch["SUBACC"],
                "SUBAC2": branch["SUBAC2"],
                "TOTBRH": branch["TOTBRH"],
                "SOTACC": branch["SotACC"]
            })
        
        # Add category totals
        report_data.append({
            "REPORT_TYPE": f"{report_type}_TOTAL",
            "CATEGORY": cat,
            "PROGID": f"EIMAR103-{report_type[-1]}",
            "BRANCH": "TOTAL",
            "BRHCODE": "",
            **{f"NOACC{i}": cat_data["totacc"][i] for i in range(1, 15)},
            **{f"BRHAMT{i}": cat_data["totamt"][i] for i in range(1, 15)},
            "SUBBRH": cat_data["sgtotbrh"],
            "SUBBR2": cat_data["sgtotbr2"],
            "SUBACC": cat_data["sgtotacc"],
            "SUBAC2": cat_data["sgtotac2"],
            "TOTBRH": cat_data["gtotbrh"],
            "SOTACC": cat_data["gtotacc"]
        })
    
    if report_data:
        df = pl.DataFrame(report_data)
        df.write_parquet(OUTPUT_PATH / f"NPL_{report_type}_REPORT.parquet")
        print(f"✓ NPL {report_type} report saved: {len(df)} records")

# ============================================================================
# 5. NPL Specific Analysis
# ============================================================================

def analyze_npl_characteristics(loan_df: pl.DataFrame) -> Dict:
    """Analyze NPL characteristics by category"""
    
    analysis_results = {}
    
    for cat in loan_df["CAT"].unique().to_list():
        cat_data = loan_df.filter(pl.col("CAT") == cat)
        
        # Count by NPL reason
        npl_by_reason = {
            "ARREAR2_GT_3": len(cat_data.filter(pl.col("ARREAR2") > 3)),
            "BORSTAT_RIF": len(cat_data.filter(pl.col("BORSTAT").is_in(["R", "I", "F"]))),
            "CENSUS9_9": len(cat_data.filter(pl.col("CENSUS9") == "9")),
            "USER5_N": len(cat_data.filter(pl.col("USER5") == "N"))
        }
        
        # Overlap analysis (accounts may have multiple reasons)
        total_accounts = len(cat_data)
        
        # Arrears distribution for NPL
        arrears_dist = {}
        for arrear in range(1, 15):
            count = len(cat_data.filter(pl.col("ARREAR2") == arrear))
            if count > 0:
                arrears_dist[f"ARREAR{arrear}"] = count
        
        # Borrower status distribution
        borstat_dist = cat_data.group_by("BORSTAT").agg(pl.count().alias("COUNT"))
        
        analysis_results[cat] = {
            "total_accounts": total_accounts,
            "total_balance": cat_data["BALANCE"].sum(),
            "npl_by_reason": npl_by_reason,
            "arrears_distribution": arrears_dist,
            "borrower_status": borstat_dist.to_dicts(),
            "avg_balance": cat_data["BALANCE"].mean() if total_accounts > 0 else 0
        }
    
    return analysis_results

# ============================================================================
# 6. Main Execution
# ============================================================================

def main():
    """Main execution function"""
    print("=" * 60)
    print("EIMIR103 SAS to Python Conversion - NPL Report")
    print("=" * 60)
    
    # HPD list (would come from macro variable &HPD)
    HPD_LIST = ["110", "115", "700", "705"]
    
    # 1. Process REPTDATE
    print("\n1. Processing REPTDATE...")
    variables = process_repdate()
    print(f"   Report Date: {variables['RDATE']}")
    
    # 2. Load and sort loan data
    print("\n2. Loading and sorting loan data...")
    loan_path = INPUT_PATH / "BNM/LOANTEMP.parquet"
    loan_df = pl.read_parquet(loan_path).sort(["BRANCH", "ARREAR2"])
    branch_df = load_branch_data()
    print(f"   Total loans: {len(loan_df)}")
    print(f"   Total branches: {len(branch_df)}")
    
    # 3. Categorize NPL loans (stricter criteria)
    print("\n3. Categorizing NPL loans...")
    npl_categorized = categorize_npl_loans(loan_df, HPD_LIST)
    print(f"   NPL candidates: {len(npl_categorized)}")
    
    # 4. Merge with branch data
    print("\n4. Merging with branch data...")
    merged_npl = npl_categorized.join(
        branch_df,
        on="BRANCH",
        how="inner"
    ).sort(["CAT", "BRANCH", "ARREAR2"])
    print(f"   Merged NPL records: {len(merged_npl)}")
    
    # Save NPL data
    merged_npl.write_parquet(OUTPUT_PATH / "NPL_LOANS_CATEGORIZED.parquet")
    
    # 5. Generate Report A (All NPL data)
    print("\n5. Generating Report A (EIMAR103-A)...")
    report_a = generate_npl_report_a(merged_npl, variables)
    results_a = calculate_npl_summaries(report_a)
    create_npl_summary_output(results_a, "A", variables)
    
    # 6. Generate Report B (Exclusions)
    print("\n6. Generating Report B (EIMAR103-B)...")
    report_b = generate_npl_report_b(merged_npl, variables, HPD_LIST)
    results_b = calculate_npl_summaries(report_b)
    create_npl_summary_output(results_b, "B", variables)
    
    # 7. Analyze NPL characteristics
    print("\n7. Analyzing NPL characteristics...")
    npl_analysis = analyze_npl_characteristics(merged_npl)
    
    # Save analysis results
    analysis_data = []
    for cat, analysis in npl_analysis.items():
        analysis_data.append({
            "CATEGORY": cat,
            "TOTAL_ACCOUNTS": analysis["total_accounts"],
            "TOTAL_BALANCE": analysis["total_balance"],
            "AVG_BALANCE": analysis["avg_balance"],
            "NPL_ARREAR_GT_3": analysis["npl_by_reason"]["ARREAR2_GT_3"],
            "NPL_BORSTAT_RIF": analysis["npl_by_reason"]["BORSTAT_RIF"],
            "NPL_CENSUS9_9": analysis["npl_by_reason"]["CENSUS9_9"],
            "NPL_USER5_N": analysis["npl_by_reason"]["USER5_N"]
        })
    
    if analysis_data:
        analysis_df = pl.DataFrame(analysis_data)
        analysis_df.write_parquet(OUTPUT_PATH / "NPL_ANALYSIS.parquet")
        print(f"✓ NPL analysis saved: {len(analysis_df)} categories")
    
    # 8. Create summary statistics
    print("\n8. Creating summary statistics...")
    
    # Overall NPL statistics
    total_npl_accounts = len(merged_npl)
    total_npl_balance = merged_npl["BALANCE"].sum()
    avg_npl_balance = total_npl_balance / total_npl_accounts if total_npl_accounts > 0 else 0
    
    # NPL by arrears bucket
    npl_by_arrears = merged_npl.group_by("ARREAR2").agg([
        pl.count().alias("ACCOUNT_COUNT"),
        pl.sum("BALANCE").alias("TOTAL_BALANCE")
    ]).sort("ARREAR2")
    
    npl_by_arrears.write_parquet(OUTPUT_PATH / "NPL_BY_ARREARS.parquet")
    
    # Save summary
    summary = pl.DataFrame([{
        "REPORT_DATE": variables["RDATE"],
        "TOTAL_NPL_ACCOUNTS": total_npl_accounts,
        "TOTAL_NPL_BALANCE": total_npl_balance,
        "AVG_NPL_BALANCE": avg_npl_balance,
        "NPL_CATEGORIES": len(npl_analysis),
        "MAX_ARREAR": merged_npl["ARREAR2"].max() if total_npl_accounts > 0 else 0,
        "AVG_ARREAR": merged_npl["ARREAR2"].mean() if total_npl_accounts > 0 else 0
    }])
    summary.write_parquet(OUTPUT_PATH / "NPL_SUMMARY.parquet")
    
    # 9. Save variables
    variables_df = pl.DataFrame([variables])
    variables_df.write_parquet(OUTPUT_PATH / "EIMIR103_VARIABLES.parquet")
    
    print("\n" + "=" * 60)
    print("CONVERSION COMPLETE")
    print("=" * 60)
    print(f"Total loans processed: {len(loan_df)}")
    print(f"NPL accounts identified: {total_npl_accounts}")
    print(f"NPL balance: {total_npl_balance:,.2f}")
    print(f"Categories: {len(npl_analysis)}")
    print(f"Output saved to: {OUTPUT_PATH}")

# ============================================================================
# 7. Run the conversion
# ============================================================================

if __name__ == "__main__":
    main()
