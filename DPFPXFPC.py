"""
EIMIR104 SAS to Python conversion
Processes NPL (Non-Performing Loan) reports with 17-bucket structure
Combines EIMIR103 NPL logic with EIMIR102 17-bucket format
Output: Text file matching CCDTXT2 format
"""

from pathlib import Path
from datetime import datetime, timedelta
import pyreadstat
import polars as pl
from typing import Dict, List, Tuple
import numpy as np

# Setup paths
BASE_PATH = Path(".")
INPUT_PATH = BASE_PATH / "sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIMIR101-104"
OUTPUT_PATH = BASE_PATH / "sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIMIR104"
CCDTXT2_PATH = BASE_PATH / "sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIMIR_CCDTXT2"
OUTPUT_PATH.mkdir(parents=True, exist_ok=True)
CCDTXT2_PATH.mkdir(parents=True, exist_ok=True)

# ============================================================================
# 1. REPTDATE Processing (using datetime - 1 day)
# ============================================================================

def process_repdate() -> Dict[str, str]:
    """Process REPTDATE using current date minus 1 day"""
    repdate = datetime.now() - timedelta(days=1)
    
    return {
        'RDATE': repdate.strftime("%d%m%y"),  # DDMMYY8.
        'REPTYEAR': str(repdate.year),        # YEAR4.
        'REPTMON': f"{repdate.month:02d}",    # Z2.
        'REPTDAY': f"{repdate.day:02d}",      # Z2.
        'REPTDATE': repdate.strftime("%d/%m/%Y"),  # DD/MM/YYYY for display
        'REPTDATE_DISPLAY': repdate.strftime("%d/%m/%Y")  # For header
    }

# ============================================================================
# 2. Load and Process Data
# ============================================================================

def load_branch_data() -> pl.DataFrame:
    """Load branch header data from LKP_BRANCH flatfile"""
    branch_path = INPUT_PATH / "LKP_BRANCH"
    
    # Read fixed-width flatfile
    # Format: BRANCH (first 3 chars), BRHCODE (next 3 chars)
    branches = []
    with open(branch_path, 'r') as f:
        for line in f:
            if len(line.strip()) >= 6:
                branch_code = line[0:3].strip()
                brhcode = line[3:6].strip()
                if branch_code and brhcode:
                    branches.append({
                        "BRANCH": int(branch_code),
                        "BRHCODE": brhcode
                    })
    
    return pl.DataFrame(branches)

def read_sas_data(filename: str) -> pl.DataFrame:
    """Read SAS dataset using pyreadstat"""
    filepath = INPUT_PATH / filename
    df, meta = pyreadstat.read_sas7bdat(str(filepath))
    return pl.from_pandas(df)

def extract_census9(census_value: float) -> str:
    """Extract 7th character from formatted census (8.2 format)"""
    if census_value is None or np.isnan(census_value):
        return ' '
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
# 3. Generate Text Report in CCDTXT2 Format
# ============================================================================

def generate_ccdtxt2_report(npl_df: pl.DataFrame, branch_df: pl.DataFrame, 
                            category_type: str, report_date: str, 
                            prog_id: str, output_file: Path):
    """Generate report in CCDTXT2 format with 17-bucket structure"""
    
    # Merge with branch data
    merged_df = npl_df.join(branch_df, on="BRANCH", how="left")
    
    # Group by BRANCH and ARREAR bucket
    bucket_data = merged_df.group_by(["BRANCH", "BRHCODE", "ARREAR"]).agg([
        pl.count().alias("NOACC"),
        pl.sum("BALANCE").alias("BALANCE")
    ]).sort(["BRANCH", "ARREAR"])
    
    # Create branch-level summary with 17 buckets
    branch_summaries = []
    for branch in sorted(merged_df["BRANCH"].unique()):
        branch_info = merged_df.filter(pl.col("BRANCH") == branch)
        brhcode = branch_info["BRHCODE"].drop_nulls().first() if len(branch_info["BRHCODE"].drop_nulls()) > 0 else ""
        
        # Initialize buckets 1-17
        buckets = {i: {"count": 0, "amount": 0.0} for i in range(1, 18)}
        
        # Fill bucket data
        for row in bucket_data.filter(pl.col("BRANCH") == branch).iter_rows(named=True):
            bucket = int(row["ARREAR"])
            if 1 <= bucket <= 17:
                buckets[bucket]["count"] = row["NOACC"]
                buckets[bucket]["amount"] = row["BALANCE"]
        
        # Calculate subtotals
        sub_ge3_count = sum(buckets[i]["count"] for i in range(4, 18))  # >= 3 months
        sub_ge3_amount = sum(buckets[i]["amount"] for i in range(4, 18))
        sub_ge6_count = sum(buckets[i]["count"] for i in range(7, 18))  # >= 6 months  
        sub_ge6_amount = sum(buckets[i]["amount"] for i in range(7, 18))
        total_count = sum(buckets[i]["count"] for i in range(1, 18))
        total_amount = sum(buckets[i]["amount"] for i in range(1, 18))
        
        branch_summaries.append({
            "BRANCH": branch,
            "BRHCODE": brhcode,
            "buckets": buckets,
            "sub_ge3_count": sub_ge3_count,
            "sub_ge3_amount": sub_ge3_amount,
            "sub_ge6_count": sub_ge6_count,
            "sub_ge6_amount": sub_ge6_amount,
            "total_count": total_count,
            "total_amount": total_amount
        })
    
    # Generate text output - append to existing file
    with open(output_file, 'a') as f:  # 'a' for append mode
        # Header page
        _write_report_header(f, prog_id, category_type, report_date)
        
        # Detail lines
        for branch_sum in branch_summaries:
            _write_branch_detail(f, branch_sum)
        
        # Grand totals
        _write_grand_totals(f, branch_summaries)
        
        f.write("\f")  # Form feed for new page

def _write_report_header(f, prog_id: str, category_type: str, report_date: str):
    """Write CCDTXT2 format header"""
    # Line 1: Program ID and title
    title_line = f"1PROGRAM-ID : {prog_id:<20} P U B L I C   I S L A M I C   B A N K   B E R H A D"
    f.write(f"{title_line:<120}PAGE NO.: 1\n")
    
    # Line 2: Report title and date (centered)
    report_title = f"OUTSTANDING LOANS IN ARREARS ({category_type})"
    date_part = report_date
    # Center title with date on right
    padding = 60 - len(report_title)
    f.write(f"{' ' * padding}{report_title}{' ' * 10}{date_part}\n")
    
    # Line 3: Bucket headers - Line 1
    header_line1 = ("0BRH    NO          < 1 MTH      NO     1 TO < 2 MTH      NO     2 TO < 3 MTH       "
                    "NO      3 TO < 4 MTH       NO      4 TO < 5 MTH")
    f.write(f"{header_line1}\n")
    
    # Line 4: Bucket headers - Line 2
    header_line2 = ("        NO     5 TO < 6 MTH      NO     6 TO < 7 MTH      NO     7 TO < 8 MTH       "
                    "NO      8 TO < 9 MTH       NO     9 TO < 12 MTH")
    f.write(f"{header_line2}\n")
    
    # Line 5: Bucket headers - Line 3
    header_line3 = ("        NO   12 TO < 18 MTH      NO   18 TO < 24 MTH      NO   24 TO < 36 MTH       "
                    "NO          > 36 MTH       NO   SUBTOTAL >=3MTH")
    f.write(f"{header_line3}\n")
    
    # Line 6: Bucket headers - Line 4
    header_line4 = ("                                                                                    "
                    "NO   SUBTOTAL >=6MTH       NO             TOTAL")
    f.write(f"{header_line4}\n")
    
    # Line 7: Separator
    f.write(" " + "-" * 98 + "\n")

def _write_branch_detail(f, branch_sum: Dict):
    """Write branch detail lines in CCDTXT2 format"""
    b = branch_sum["buckets"]
    
    # Format numbers with commas for thousands
    def fmt_num(n):
        return f"{n:>7}" if n >= 0 else f"{n:>7}"
    
    def fmt_amt(a):
        return f"{a:>12,.2f}" if a >= 0 else f"{a:>12,.2f}"
    
    # Line 1: Branch code + buckets 1-5
    line1 = (f" {branch_sum['BRANCH']:>3}   {fmt_num(b[1]['count'])}  {fmt_amt(b[1]['amount'])}   "
             f"{fmt_num(b[2]['count'])}  {fmt_amt(b[2]['amount'])}   "
             f"{fmt_num(b[3]['count'])}  {fmt_amt(b[3]['amount'])}    "
             f"{fmt_num(b[4]['count'])}  {fmt_amt(b[4]['amount'])}        "
             f"{fmt_num(b[5]['count'])}  {fmt_amt(b[5]['amount'])}")
    f.write(f"{line1}\n")
    
    # Line 2: BRHCODE + buckets 6-10
    line2 = (f" {branch_sum['BRHCODE']:<3}   {fmt_num(b[6]['count'])}  {fmt_amt(b[6]['amount'])}   "
             f"{fmt_num(b[7]['count'])}  {fmt_amt(b[7]['amount'])}   "
             f"{fmt_num(b[8]['count'])}  {fmt_amt(b[8]['amount'])}    "
             f"{fmt_num(b[9]['count'])}  {fmt_amt(b[9]['amount'])}        "
             f"{fmt_num(b[10]['count'])}  {fmt_amt(b[10]['amount'])}")
    f.write(f"{line2}\n")
    
    # Line 3: Empty BRH + buckets 11-15
    line3 = (f"            {fmt_num(b[11]['count'])}  {fmt_amt(b[11]['amount'])}   "
             f"{fmt_num(b[12]['count'])}  {fmt_amt(b[12]['amount'])}   "
             f"{fmt_num(b[13]['count'])}  {fmt_amt(b[13]['amount'])}    "
             f"{fmt_num(b[14]['count'])}  {fmt_amt(b[14]['amount'])}        "
             f"{fmt_num(b[15]['count'])}  {fmt_amt(b[15]['amount'])}")
    f.write(f"{line3}\n")
    
    # Line 4: Buckets 16-17 + subtotals
    line4 = (f"                                                                                   "
             f"{fmt_num(b[16]['count'])}  {fmt_amt(b[16]['amount'])}   "
             f"{fmt_num(b[17]['count'])}  {fmt_amt(b[17]['amount'])}")
    f.write(f"{line4}\n")
    
    # Line 5: Subtotals continued + total
    line5 = (f"                                                                                  "
             f"{fmt_num(branch_sum['sub_ge3_count'])}  {fmt_amt(branch_sum['sub_ge3_amount'])}   "
             f"{fmt_num(branch_sum['sub_ge6_count'])}  {fmt_amt(branch_sum['sub_ge6_amount'])}       "
             f"{fmt_num(branch_sum['total_count'])}  {fmt_amt(branch_sum['total_amount'])}")
    f.write(f"{line5}\n")

def _write_grand_totals(f, branch_summaries: List[Dict]):
    """Write grand totals in CCDTXT2 format"""
    # Calculate grand totals
    tot_buckets = {i: {"count": 0, "amount": 0.0} for i in range(1, 18)}
    total_sub_ge3_count = 0
    total_sub_ge3_amount = 0.0
    total_sub_ge6_count = 0
    total_sub_ge6_amount = 0.0
    total_all_count = 0
    total_all_amount = 0.0
    
    for branch_sum in branch_summaries:
        for i in range(1, 18):
            tot_buckets[i]["count"] += branch_sum["buckets"][i]["count"]
            tot_buckets[i]["amount"] += branch_sum["buckets"][i]["amount"]
        total_sub_ge3_count += branch_sum["sub_ge3_count"]
        total_sub_ge3_amount += branch_sum["sub_ge3_amount"]
        total_sub_ge6_count += branch_sum["sub_ge6_count"]
        total_sub_ge6_amount += branch_sum["sub_ge6_amount"]
        total_all_count += branch_sum["total_count"]
        total_all_amount += branch_sum["total_amount"]
    
    # Separator line
    f.write(" " + "-" * 98 + "\n")
    
    # Grand total using same format as branch detail
    grand_sum = {
        "BRANCH": "",
        "BRHCODE": "",
        "buckets": tot_buckets,
        "sub_ge3_count": total_sub_ge3_count,
        "sub_ge3_amount": total_sub_ge3_amount,
        "sub_ge6_count": total_sub_ge6_count,
        "sub_ge6_amount": total_sub_ge6_amount,
        "total_count": total_all_count,
        "total_amount": total_all_amount
    }
    
    _write_branch_detail(f, grand_sum)

# ============================================================================
# 4. Main Execution
# ============================================================================

def main():
    """Main execution function"""
    print("=" * 60)
    print("EIMIR104 SAS to Python Conversion - 17-Bucket NPL Report")
    print("CCDTXT2 Format Output")
    print("=" * 60)
    
    # HPD list (would come from macro variable &HPD)
    HPD_LIST = ["110", "115", "700", "705"]
    
    # 1. Process REPTDATE (current date - 1 day)
    print("\n1. Processing REPTDATE...")
    variables = process_repdate()
    print(f"   Report Date: {variables['RDATE']}")
    
    # 2. Load loan data from SAS file
    print("\n2. Loading loan data from SAS file...")
    try:
        loan_df = read_sas_data("loantemp.sas7bdat")
        print(f"   Total loans loaded: {len(loan_df)}")
    except Exception as e:
        print(f"   Error reading SAS file: {e}")
        return
    
    # 3. Load branch data from flatfile
    print("\n3. Loading branch data from LKP_BRANCH...")
    try:
        branch_df = load_branch_data()
        print(f"   Total branches loaded: {len(branch_df)}")
    except Exception as e:
        print(f"   Error reading branch file: {e}")
        branch_df = pl.DataFrame(schema={"BRANCH": pl.Int64, "BRHCODE": pl.Utf8})
    
    # 4. Categorize NPL loans
    print("\n4. Categorizing NPL loans (17-bucket)...")
    npl_categorized = categorize_npl_loans_17bucket(loan_df, HPD_LIST)
    print(f"   NPL candidates: {len(npl_categorized)}")
    
    # 5. Generate CCDTXT2 report - append to existing CCDTXT2.txt
    print("\n5. Generating CCDTXT2 report and appending to existing CCDTXT2.txt...")
    ccdtxt2_output = CCDTXT2_PATH / "CCDTXT2.txt"
    
    # Process each category and append to CCDTXT2.txt
    for cat in sorted(npl_categorized["CAT"].unique().to_list()):
        cat_data = npl_categorized.filter(pl.col("CAT") == cat)
        cat_type = cat_data["TYPE"].drop_nulls().first() if len(cat_data["TYPE"].drop_nulls()) > 0 else ""
        
        print(f"   Appending {cat_type} report to CCDTXT2.txt...")
        generate_ccdtxt2_report(
            npl_df=cat_data,
            branch_df=branch_df,
            category_type=cat_type,
            report_date=variables['REPTDATE_DISPLAY'],
            prog_id=f"EIMAR104-{cat}",
            output_file=ccdtxt2_output
        )
    
    print(f"   ✓ Report appended to: {ccdtxt2_output}")
    
    # 6. Also save individual category reports in EIMIR104 output for reference
    print("\n6. Saving individual category reports for reference...")
    for cat in npl_categorized["CAT"].unique().to_list():
        cat_data = npl_categorized.filter(pl.col("CAT") == cat)
        cat_type = cat_data["TYPE"].drop_nulls().first() if len(cat_data["TYPE"].drop_nulls()) > 0 else ""
        
        # Create output file for this category (overwrite mode)
        output_file = OUTPUT_PATH / f"EIMIR104_{cat}_CCDTXT2.txt"
        
        # Clear file if exists, or create new
        if output_file.exists():
            output_file.unlink()
        
        generate_ccdtxt2_report(
            npl_df=cat_data,
            branch_df=branch_df,
            category_type=cat_type,
            report_date=variables['REPTDATE_DISPLAY'],
            prog_id=f"EIMAR104-{cat}",
            output_file=output_file
        )
        print(f"   ✓ Reference report saved: {output_file}")
    
    # 7. Save supporting data files
    print("\n7. Saving supporting data files...")
    
    # Save merged data
    merged_npl = npl_categorized.join(branch_df, on="BRANCH", how="left")
    merged_npl.write_parquet(OUTPUT_PATH / "NPL_17BUCKET_CATEGORIZED.parquet")
    
    # Save variables
    variables_df = pl.DataFrame([variables])
    variables_df.write_parquet(OUTPUT_PATH / "EIMIR104_VARIABLES.parquet")
    
    print("\n" + "=" * 60)
    print("CONVERSION COMPLETE")
    print("=" * 60)
    print(f"Total loans processed: {len(loan_df)}")
    print(f"NPL accounts identified: {len(npl_categorized)}")
    print(f"Categories: {npl_categorized['CAT'].n_unique()}")
    print(f"Output appended to: {ccdtxt2_output}")
    print(f"Reference reports saved to: {OUTPUT_PATH}")

# ============================================================================
# 5. Run the conversion
# ============================================================================

if __name__ == "__main__":
    main()
