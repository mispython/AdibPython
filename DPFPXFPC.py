"""
EIMIR103 SAS to Python Conversion
Processes NPL (Non-Performing Loan) reports with stricter criteria
"""

from pathlib import Path
from datetime import datetime, timedelta
import polars as pl
import pyreadstat
from typing import Dict, List, Optional
import pandas as pd

# Setup paths
BASE_PATH = Path(".")
INPUT_PATH = BASE_PATH / "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIMIR101 - 104"
OUTPUT_PATH = BASE_PATH / "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIMIR103"
OUTPUT_PATH.mkdir(parents=True, exist_ok=True)

# ============================================================================
# 1. Date Processing (uses yesterday's date)
# ============================================================================

def get_yesterday_date() -> Dict[str, str]:
    """Get yesterday's date and extract formatted variables"""
    yesterday = datetime.now() - timedelta(days=1)
    
    return {
        'RDATE': yesterday.strftime("%d%m%y"),  # DDMMYY8.
        'REPTYEAR': str(yesterday.year),        # YEAR4.
        'REPTMON': f"{yesterday.month:02d}",    # Z2.
        'REPTDAY': f"{yesterday.day:02d}",      # Z2.
        'REPTDATE': yesterday
    }

# ============================================================================
# 2. Load and Process Data
# ============================================================================

def load_branch_data() -> pl.DataFrame:
    """Load branch header data from LKP_BRANCH flatfile"""
    branch_path = INPUT_PATH / "LKP_BRANCH"
    
    if branch_path.exists():
        # Read flatfile - assuming space/pipe delimited with header
        # Adjust delimiter and column names based on actual file format
        try:
            # Try reading as fixed width or delimited
            with open(branch_path, 'r') as f:
                first_line = f.readline().strip()
            
            # Check if it's a delimited file
            if '\t' in first_line:
                df = pl.read_csv(branch_path, separator='\t', has_header=True)
            elif '|' in first_line:
                df = pl.read_csv(branch_path, separator='|', has_header=True)
            elif ',' in first_line:
                df = pl.read_csv(branch_path, separator=',', has_header=True)
            else:
                # Assume space-delimited or fixed width
                # Try space delimiter first
                df = pl.read_csv(branch_path, separator=' ', has_header=True, ignore_errors=True)
            
            # Ensure required columns exist
            if "BRANCH" not in df.columns:
                # Try to find appropriate column names
                for col in df.columns:
                    if "BRANCH" in col.upper() or "BRH" in col.upper():
                        df = df.rename({col: "BRANCH"})
                        break
                    elif "CODE" in col.upper():
                        df = df.rename({col: "BRHCODE"})
                        break
            
            # Ensure BRHCODE column exists
            if "BRHCODE" not in df.columns:
                # Try to find appropriate column
                for col in df.columns:
                    if "CODE" in col.upper() or "BRH" in col.upper():
                        df = df.rename({col: "BRHCODE"})
                        break
                else:
                    # If no BRHCODE, create from BRANCH
                    df = df.with_columns(
                        pl.col("BRANCH").cast(pl.Utf8).alias("BRHCODE")
                    )
            
            return df
            
        except Exception as e:
            print(f"   Warning: Could not parse LKP_BRANCH: {e}")
            return pl.DataFrame(schema={"BRANCH": pl.Int64, "BRHCODE": pl.Utf8})
    else:
        print(f"   Warning: LKP_BRANCH not found at {branch_path}")
        return pl.DataFrame(schema={"BRANCH": pl.Int64, "BRHCODE": pl.Utf8})

def load_loan_data() -> pl.DataFrame:
    """Load loan data from SAS .sas7bdat file using pyreadstat"""
    loan_path = INPUT_PATH / "BNM/loantemp.sas7bdat"
    
    if loan_path.exists():
        try:
            # Read SAS file with pyreadstat
            df_pd, meta = pyreadstat.read_sas7bdat(loan_path)
            df = pl.from_pandas(df_pd)
            print(f"   Loaded {len(df)} loan records")
            print(f"   Columns: {df.columns[:10]}...")
            return df
        except Exception as e:
            print(f"   Error reading loantemp.sas7bdat: {e}")
            # Fallback to parquet if available
            parquet_path = INPUT_PATH / "BNM/LOANTEMP.parquet"
            if parquet_path.exists():
                return pl.read_parquet(parquet_path)
            else:
                raise FileNotFoundError(f"Could not find loantemp.sas7bdat or LOANTEMP.parquet")
    else:
        # Try parquet as fallback
        parquet_path = INPUT_PATH / "BNM/LOANTEMP.parquet"
        if parquet_path.exists():
            return pl.read_parquet(parquet_path)
        else:
            raise FileNotFoundError(f"Could not find loantemp.sas7bdat or LOANTEMP.parquet")

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
                "BRHCODE": branch_data["BRHCODE"][0] if len(branch_data["BRHCODE"]) > 0 else str(branch),
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
# 4. Generate Text Output with CCDTXT2 Format
# ============================================================================

def format_number(value: float, width: int = 15, decimals: int = 2) -> str:
    """Format number with specified width and decimals"""
    if value == 0:
        return f"{'0':>{width}}.{'0'*decimals}"
    return f"{value:>{width}.{decimals}f}"

def generate_text_output(results: Dict, report_type: str, variables: Dict, output_file: str):
    """Generate text output with the specified format and append to CCDTXT2"""
    
    # Determine report title based on type
    report_titles = {
        "A": "OUTSTANDING LOANS IN ARREARS (ALL NPL)",
        "B": "OUTSTANDING LOANS IN ARREARS (EXCLUDING AITAB & HPD)"
    }
    
    title = report_titles.get(report_type, "OUTSTANDING LOANS IN ARREARS")
    rdate = variables['RDATE']
    rdate_display = f"{rdate[:2]}/{rdate[2:4]}/{rdate[4:]}"
    
    # Open output file
    with open(output_file, 'w') as f:
        # Write header
        f.write("1PROGRAM-ID : EIMAR103-A                   P U B L I C   I S L A M I C   B A N K   B E R H A D                        PAGE NO.: 1    \n")
        f.write(f"                                             {title:<50}       {rdate_display}                                     \n")
        f.write("0BRH    NO          < 1 MTH      NO     1 TO < 2 MTH      NO     2 TO < 3 MTH       NO      3 TO < 4 MTH       NO      4 TO < 5 MTH  \n")
        f.write("        NO     5 TO < 6 MTH      NO     6 TO < 7 MTH      NO     7 TO < 8 MTH       NO      8 TO < 9 MTH       NO     9 TO < 12 MTH  \n")
        f.write("        NO   12 TO < 18 MTH      NO   18 TO < 24 MTH      NO   24 TO < 36 MTH       NO          > 36 MTH       NO   SUBTOTAL >=3MTH  \n")
        f.write("                                                                                    NO   SUBTOTAL >=6MTH       NO             TOTAL  \n")
        f.write(" ----------------------------------------------------------------------------------------------------------------------------------  \n")
        
        # Process each category
        for cat, cat_data in results.items():
            # Write category header
            f.write(f" CATEGORY {cat}:\n")
            
            # Write branch data
            for branch_data in cat_data["branches"]:
                branch_code = str(branch_data["BRHCODE"]).strip()
                if len(branch_code) < 3:
                    branch_code = branch_code.zfill(3)
                
                noacc = branch_data["NOACC"]
                brhamt = branch_data["BRHAMT"]
                
                # Format each line (3 lines per branch)
                # Line 1: Branch code, and first 4 buckets (1-4)
                line1 = f" {branch_code:<3}   {noacc[1]:>6}   {format_number(brhamt[1], 12)}   {noacc[2]:>6}   {format_number(brhamt[2], 12)}   {noacc[3]:>6}   {format_number(brhamt[3], 12)}   {noacc[4]:>6}   {format_number(brhamt[4], 12)}\n"
                f.write(line1)
                
                # Line 2: Buckets 5-8
                line2 = f"        {noacc[5]:>6}   {format_number(brhamt[5], 12)}   {noacc[6]:>6}   {format_number(brhamt[6], 12)}   {noacc[7]:>6}   {format_number(brhamt[7], 12)}   {noacc[8]:>6}   {format_number(brhamt[8], 12)}\n"
                f.write(line2)
                
                # Line 3: Buckets 9-14 and subtotals
                line3 = f"        {noacc[9]:>6}   {format_number(brhamt[9], 12)}   {noacc[10]:>6}   {format_number(brhamt[10], 12)}   {noacc[11]:>6}   {format_number(brhamt[11], 12)}   {noacc[12]:>6}   {format_number(brhamt[12], 12)}  \n"
                f.write(line3)
                
                # Line 4: >36 months and subtotals
                line4 = f"                                                                                     {noacc[13]:>6}   {format_number(brhamt[13], 12)}   {branch_data['SUBACC']:>6}   {format_number(branch_data['SUBBRH'], 12)}\n"
                f.write(line4)
                
                # Line 5: >=6 months and total
                line5 = f"                                                                                    {branch_data['SUBAC2']:>6}   {format_number(branch_data['SUBBR2'], 12)}   {branch_data['SOTACC']:>6}   {format_number(branch_data['TOTBRH'], 12)}\n"
                f.write(line5)
                
                # Line 6: Separator for next branch or blank line
                f.write("                                                                                     \n")
            
            # Write category total
            totacc = cat_data["totacc"]
            totamt = cat_data["totamt"]
            
            f.write("                                                                                     \n")
            f.write(" TOTAL FOR CATEGORY:\n")
            
            # Line 1: Totals for buckets 1-4
            line1 = f"     TOTAL  {totacc[1]:>6}   {format_number(totamt[1], 12)}   {totacc[2]:>6}   {format_number(totamt[2], 12)}   {totacc[3]:>6}   {format_number(totamt[3], 12)}   {totacc[4]:>6}   {format_number(totamt[4], 12)}\n"
            f.write(line1)
            
            # Line 2: Totals for buckets 5-8
            line2 = f"        {totacc[5]:>6}   {format_number(totamt[5], 12)}   {totacc[6]:>6}   {format_number(totamt[6], 12)}   {totacc[7]:>6}   {format_number(totamt[7], 12)}   {totacc[8]:>6}   {format_number(totamt[8], 12)}\n"
            f.write(line2)
            
            # Line 3: Totals for buckets 9-14
            line3 = f"        {totacc[9]:>6}   {format_number(totamt[9], 12)}   {totacc[10]:>6}   {format_number(totamt[10], 12)}   {totacc[11]:>6}   {format_number(totamt[11], 12)}   {totacc[12]:>6}   {format_number(totamt[12], 12)}\n"
            f.write(line3)
            
            # Line 4: >36 months and subtotals
            line4 = f"                                                                                     {totacc[13]:>6}   {format_number(totamt[13], 12)}   {cat_data['sgtotacc']:>6}   {format_number(cat_data['sgtotbrh'], 12)}\n"
            f.write(line4)
            
            # Line 5: >=6 months and total
            line5 = f"                                                                                    {cat_data['sgtotac2']:>6}   {format_number(cat_data['sgtotbr2'], 12)}   {cat_data['gtotacc']:>6}   {format_number(cat_data['gtotbrh'], 12)}\n"
            f.write(line5)
            
            f.write("\n")
    
    # Append to CCDTXT2
    append_to_ccdtxt2(output_file, report_type, variables)

def append_to_ccdtxt2(source_file: str, report_type: str, variables: Dict):
    """Append the generated report to CCDTXT2"""
    
    ccdtxt2_path = OUTPUT_PATH / "CCDTXT2"
    
    # Read the generated file content
    with open(source_file, 'r') as f:
        content = f.read()
    
    # Append to CCDTXT2
    with open(ccdtxt2_path, 'a') as f:
        f.write("\n" + "="*80 + "\n")
        f.write(f"Report Type: EIMAR103-{report_type}\n")
        f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write("="*80 + "\n")
        f.write(content)
        f.write("\n" + "="*80 + "\n")
    
    print(f"   Appended to CCDTXT2: {ccdtxt2_path}")

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
        
        # Arrears distribution for NPL
        arrears_dist = {}
        for arrear in range(1, 15):
            count = len(cat_data.filter(pl.col("ARREAR2") == arrear))
            if count > 0:
                arrears_dist[f"ARREAR{arrear}"] = count
        
        # Borrower status distribution
        borstat_dist = cat_data.group_by("BORSTAT").agg(pl.count().alias("COUNT"))
        
        analysis_results[cat] = {
            "total_accounts": len(cat_data),
            "total_balance": cat_data["BALANCE"].sum(),
            "npl_by_reason": npl_by_reason,
            "arrears_distribution": arrears_dist,
            "borrower_status": borstat_dist.to_dicts(),
            "avg_balance": cat_data["BALANCE"].mean() if len(cat_data) > 0 else 0
        }
    
    return analysis_results

# ============================================================================
# 6. Generate NPL Report Outputs
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

# ============================================================================
# 7. Main Execution
# ============================================================================

def main():
    """Main execution function"""
    print("=" * 60)
    print("EIMIR103 SAS to Python Conversion - NPL Report")
    print("=" * 60)
    
    # HPD list (would come from macro variable &HPD)
    HPD_LIST = ["110", "115", "700", "705"]
    
    # 1. Get yesterday's date
    print("\n1. Setting report date (yesterday)...")
    variables = get_yesterday_date()
    print(f"   Report Date: {variables['RDATE']}")
    print(f"   Report Date (display): {variables['RDATE'][:2]}/{variables['RDATE'][2:4]}/{variables['RDATE'][4:]}")
    
    # 2. Load loan data from SAS file
    print("\n2. Loading loan data from loantemp.sas7bdat...")
    loan_df = load_loan_data()
    print(f"   Total loans: {len(loan_df)}")
    
    # 3. Load branch data
    print("\n3. Loading branch data from LKP_BRANCH...")
    branch_df = load_branch_data()
    print(f"   Total branches: {len(branch_df)}")
    
    # 4. Categorize NPL loans (stricter criteria)
    print("\n4. Categorizing NPL loans...")
    npl_categorized = categorize_npl_loans(loan_df, HPD_LIST)
    print(f"   NPL candidates: {len(npl_categorized)}")
    
    if len(npl_categorized) == 0:
        print("   No NPL loans found. Exiting.")
        return
    
    # 5. Merge with branch data
    print("\n5. Merging with branch data...")
    merged_npl = npl_categorized.join(
        branch_df,
        on="BRANCH",
        how="inner"
    ).sort(["CAT", "BRANCH", "ARREAR2"])
    print(f"   Merged NPL records: {len(merged_npl)}")
    
    # Save NPL data
    merged_npl.write_parquet(OUTPUT_PATH / "NPL_LOANS_CATEGORIZED.parquet")
    
    # 6. Generate Report A (All NPL data)
    print("\n6. Generating Report A (EIMAR103-A)...")
    report_a = generate_npl_report_a(merged_npl, variables)
    results_a = calculate_npl_summaries(report_a)
    output_file_a = OUTPUT_PATH / "EIMAR103-A.txt"
    generate_text_output(results_a, "A", variables, output_file_a)
    print(f"   Report A saved to: {output_file_a}")
    
    # 7. Generate Report B (Exclusions)
    print("\n7. Generating Report B (EIMAR103-B)...")
    report_b = generate_npl_report_b(merged_npl, variables, HPD_LIST)
    if len(report_b) > 0:
        results_b = calculate_npl_summaries(report_b)
        output_file_b = OUTPUT_PATH / "EIMAR103-B.txt"
        generate_text_output(results_b, "B", variables, output_file_b)
        print(f"   Report B saved to: {output_file_b}")
    else:
        print("   No records for Report B (all excluded)")
    
    # 8. Analyze NPL characteristics
    print("\n8. Analyzing NPL characteristics...")
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
        print(f"   NPL analysis saved: {len(analysis_df)} categories")
    
    # 9. Create summary statistics
    print("\n9. Creating summary statistics...")
    
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
    
    # 10. Save variables
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
    print(f"CCDTXT2 appended at: {OUTPUT_PATH / 'CCDTXT2'}")

# ============================================================================
# 8. Run the conversion
# ============================================================================

if __name__ == "__main__":
    main()
