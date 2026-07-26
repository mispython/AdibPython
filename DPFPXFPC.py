"""
EIMIR102 SAS to Python Conversion
Processes loan arrears reports with different bucket structures
Matches exact SAS output format for CCDTXT2
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
        try:
            with open(branch_path, 'r') as f:
                lines = f.readlines()
            
            data = []
            for line in lines:
                line = line.strip()
                if line and not line.startswith('---') and not line.startswith('B00'):
                    parts = line.split()
                    if len(parts) >= 2:
                        data.append(parts)
            
            if data:
                df = pl.DataFrame({
                    "BRHCODE": [row[0] for row in data],
                    "BRANCH_NAME": [" ".join(row[1:]) for row in data if len(row) > 1]
                })
                df = df.with_columns(
                    pl.Series("BRANCH", range(1, len(df) + 1)).cast(pl.Int64)
                )
                return df
            else:
                for sep in ['|', '\t', ',']:
                    try:
                        df = pl.read_csv(branch_path, separator=sep, has_header=False)
                        if len(df.columns) >= 2:
                            df = df.rename({df.columns[0]: "BRHCODE", df.columns[1]: "BRANCH_NAME"})
                            df = df.with_columns(
                                pl.Series("BRANCH", range(1, len(df) + 1)).cast(pl.Int64)
                            )
                            return df
                    except:
                        continue
        except Exception as e:
            print(f"Warning: Error reading branch file: {e}")
    
    return pl.DataFrame(schema={"BRANCH": pl.Int64, "BRHCODE": pl.Utf8, "BRANCH_NAME": pl.Utf8})

def load_loan_data() -> pl.DataFrame:
    """Load loan data from SAS dataset"""
    loan_path = INPUT_PATH / "loantemp.sas7bdat"
    if loan_path.exists():
        df, meta = pyreadstat.read_sas7bdat(str(loan_path))
        return pl.from_pandas(df)
    else:
        raise FileNotFoundError(f"Loan data file not found at {loan_path}")

def categorize_loans(loan_df: pl.DataFrame, hpd_list: List[str]) -> pl.DataFrame:
    """Categorize loans into different types"""
    
    filtered_df = loan_df.filter(
        (pl.col("BALANCE") > 0) & (pl.col("BORSTAT") != "Z")
    )
    
    categorized_rows = []
    
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
    
    if categorized_rows:
        result = pl.concat(categorized_rows, how="vertical")
        if result["BRANCH"].dtype != pl.Int64:
            result = result.with_columns(pl.col("BRANCH").cast(pl.Int64))
        return result.sort(["BRANCH"])
    else:
        return pl.DataFrame(schema=filtered_df.schema)

# ============================================================================
# 3. Calculate 17-Bucket Summaries
# ============================================================================

def calculate_17_bucket_summaries(loan_df: pl.DataFrame) -> Dict:
    """Calculate 17-bucket arrears summaries"""
    
    branch_summary = loan_df.group_by(["CAT", "BRANCH", "ARREAR"]).agg([
        pl.col("BRHCODE").first().alias("BRHCODE"),
        pl.col("TYPE").first().alias("TYPE"),
        pl.col("BALANCE").sum().alias("BRHAMT"),
        pl.len().alias("NOACC")
    ])
    
    branch_summary = branch_summary.filter(
        (pl.col("ARREAR") >= 1) & (pl.col("ARREAR") <= 17)
    )
    
    result_dict = {}
    for cat in branch_summary["CAT"].unique().to_list():
        cat_data = branch_summary.filter(pl.col("CAT") == cat)
        
        totamt = [0.0] * 18
        totacc = [0] * 18
        branch_results = []
        
        for branch in cat_data["BRANCH"].unique().to_list():
            branch_data = cat_data.filter(pl.col("BRANCH") == branch)
            
            branhamt = [0.0] * 18
            noacc = [0] * 18
            
            for row in branch_data.iter_rows(named=True):
                arrear = int(row["ARREAR"])
                branhamt[arrear] = row["BRHAMT"]
                noacc[arrear] = row["NOACC"]
            
            subbrh = sum(branhamt[4:18])
            subbr2 = subbrh - sum(branhamt[4:7])
            subacc = sum(noacc[4:18])
            subac2 = subacc - sum(noacc[4:7])
            totbrh = subbrh + sum(branhamt[1:4])
            sotacc = subacc + sum(noacc[1:4])
            
            for i in range(1, 18):
                totamt[i] += branhamt[i]
                totacc[i] += noacc[i]
            
            branch_results.append({
                "BRANCH": branch,
                "BRHCODE": branch_data["BRHCODE"][0] if len(branch_data) > 0 else "",
                "TYPE": branch_data["TYPE"][0] if len(branch_data) > 0 else "",
                "NOACC": noacc,
                "BRHAMT": branhamt,
                "SUBBRH": subbrh,
                "SUBBR2": subbr2,
                "SUBACC": subacc,
                "SUBAC2": subac2,
                "TOTBRH": totbrh,
                "SOTACC": sotacc
            })
        
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
# 4. Calculate 15-Bucket Summaries
# ============================================================================

def calculate_15_bucket_summaries(loan_df: pl.DataFrame) -> Dict:
    """Calculate 15-bucket arrears summaries for day 15"""
    
    branch_summary = loan_df.group_by(["CAT", "BRANCH", "ARREAR2"]).agg([
        pl.col("BRHCODE").first().alias("BRHCODE"),
        pl.col("TYPE").first().alias("TYPE"),
        pl.col("BALANCE").sum().alias("BRHAMT"),
        pl.len().alias("NOACC")
    ])
    
    branch_summary = branch_summary.filter(
        (pl.col("ARREAR2") >= 1) & (pl.col("ARREAR2") <= 15)
    )
    
    result_dict = {}
    for cat in branch_summary["CAT"].unique().to_list():
        cat_data = branch_summary.filter(pl.col("CAT") == cat)
        
        totamt = [0.0] * 16
        totacc = [0] * 16
        branch_results = []
        
        for branch in cat_data["BRANCH"].unique().to_list():
            branch_data = cat_data.filter(pl.col("BRANCH") == branch)
            
            branhamt = [0.0] * 16
            noacc = [0] * 16
            
            for row in branch_data.iter_rows(named=True):
                arrear = int(row["ARREAR2"])
                branhamt[arrear] = row["BRHAMT"]
                noacc[arrear] = row["NOACC"]
            
            subbrh = sum(branhamt[4:16])
            subbr2 = sum(branhamt[7:16])
            subacc = sum(noacc[4:16])
            subac2 = sum(noacc[7:16])
            totbrh = subbrh + sum(branhamt[1:4])
            sotacc = subacc + sum(noacc[1:4])
            
            for i in range(1, 16):
                totamt[i] += branhamt[i]
                totacc[i] += noacc[i]
            
            branch_results.append({
                "BRANCH": branch,
                "BRHCODE": branch_data["BRHCODE"][0] if len(branch_data) > 0 else "",
                "TYPE": branch_data["TYPE"][0] if len(branch_data) > 0 else "",
                "NOACC": noacc,
                "BRHAMT": branhamt,
                "SUBBRH": subbrh,
                "SUBBR2": subbr2,
                "SUBACC": subacc,
                "SUBAC2": subac2,
                "TOTBRH": totbrh,
                "SOTACC": sotacc
            })
        
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
# 5. Generate Report Outputs in SAS Format
# ============================================================================

def format_number(num, width, decimals=2):
    """Format number with commas and specific width"""
    if num == 0:
        return f"{0:>{width}.{decimals}f}"
    return f"{num:>{width},.{decimals}f}"

def generate_17_bucket_report_sas_format(results: Dict, variables: Dict, output_file):
    """Generate 17-bucket report in exact SAS format"""
    
    pagecnt = 0
    
    for cat, cat_data in sorted(results.items()):
        # Get the type
        branch_type = cat_data['branches'][0].get('TYPE', 'Unknown') if cat_data['branches'] else 'Unknown'
        
        # New page for each category
        pagecnt += 1
        
        # Header - Page 1
        output_file.write(f"PROGRAM-ID : EIMAR102-A                   P U B L I C   I S L A M I C   B A N K   B E R H A D                        PAGE NO.: {pagecnt:>4}\n")
        output_file.write(f"                                             OUTSTANDING LOANS IN ARREARS {branch_type:>13}       {variables['RDATE']}\n")
        output_file.write(f"0BRH    NO          < 1 MTH      NO     1 TO < 2 MTH      NO     2 TO < 3 MTH       NO      3 TO < 4 MTH       NO      4 TO < 5 MTH\n")
        output_file.write(f"        NO     5 TO < 6 MTH      NO     6 TO < 7 MTH      NO     7 TO < 8 MTH       NO      8 TO < 9 MTH       NO     9 TO < 10 MTH\n")
        output_file.write(f"        NO   10 TO < 11 MTH      NO   11 TO < 12 MTH      NO   12 TO < 18 MTH       NO    18 TO < 24 MTH       NO    24 TO < 36 MTH\n")
        output_file.write(f"        NO         > 36 MTH      NO          DEFICIT      NO   SUBTOTAL >=3MTH       NO   SUBTOTAL >=6MTH       NO             TOTAL\n")
        output_file.write(f" ----------------------------------------------------------------------------------------------------------------------------------\n")
        
        # Write branch data
        for branch in cat_data["branches"]:
            # Line 1: BRANCH and first 5 buckets
            output_file.write(
                f" {branch['BRANCH']:>3}  "
                f"{branch['NOACC'][1]:>7,}  {branch['BRHAMT'][1]:>15,.2f}  "
                f"{branch['NOACC'][2]:>7,}  {branch['BRHAMT'][2]:>15,.2f}  "
                f"{branch['NOACC'][3]:>7,}  {branch['BRHAMT'][3]:>15,.2f}  "
                f"{branch['NOACC'][4]:>8,}  {branch['BRHAMT'][4]:>17,.2f}  "
                f"{branch['NOACC'][5]:>8,}  {branch['BRHAMT'][5]:>17,.2f}\n"
            )
            
            # Line 2: BRHCODE and next 5 buckets
            output_file.write(
                f" {branch['BRHCODE']:<4}  "
                f"{branch['NOACC'][6]:>7,}  {branch['BRHAMT'][6]:>15,.2f}  "
                f"{branch['NOACC'][7]:>7,}  {branch['BRHAMT'][7]:>15,.2f}  "
                f"{branch['NOACC'][8]:>7,}  {branch['BRHAMT'][8]:>15,.2f}  "
                f"{branch['NOACC'][9]:>8,}  {branch['BRHAMT'][9]:>17,.2f}  "
                f"{branch['NOACC'][10]:>8,}  {branch['BRHAMT'][10]:>17,.2f}\n"
            )
            
            # Line 3: next 5 buckets
            output_file.write(
                f"        "
                f"{branch['NOACC'][11]:>7,}  {branch['BRHAMT'][11]:>15,.2f}  "
                f"{branch['NOACC'][12]:>7,}  {branch['BRHAMT'][12]:>15,.2f}  "
                f"{branch['NOACC'][13]:>7,}  {branch['BRHAMT'][13]:>15,.2f}  "
                f"{branch['NOACC'][14]:>8,}  {branch['BRHAMT'][14]:>17,.2f}  "
                f"{branch['NOACC'][15]:>8,}  {branch['BRHAMT'][15]:>17,.2f}\n"
            )
            
            # Line 4: last 2 buckets and subtotals
            output_file.write(
                f"        "
                f"{branch['NOACC'][16]:>7,}  {branch['BRHAMT'][16]:>15,.2f}  "
                f"{branch['NOACC'][17]:>7,}  {branch['BRHAMT'][17]:>15,.2f}  "
                f"{branch['SUBACC']:>7,}  {branch['SUBBRH']:>15,.2f}  "
                f"{branch['SUBAC2']:>8,}  {branch['SUBBR2']:>17,.2f}  "
                f"{branch['SOTACC']:>8,}  {branch['TOTBRH']:>17,.2f}\n"
            )
        
        # Write category totals
        output_file.write(" ----------------------------------------------------------------------------------------------------------------------------------\n")
        output_file.write(
            f" TOT  "
            f"{cat_data['totacc'][1]:>7,}  {cat_data['totamt'][1]:>15,.2f}  "
            f"{cat_data['totacc'][2]:>7,}  {cat_data['totamt'][2]:>15,.2f}  "
            f"{cat_data['totacc'][3]:>7,}  {cat_data['totamt'][3]:>15,.2f}  "
            f"{cat_data['totacc'][4]:>8,}  {cat_data['totamt'][4]:>17,.2f}  "
            f"{cat_data['totacc'][5]:>8,}  {cat_data['totamt'][5]:>17,.2f}\n"
        )
        output_file.write(
            f"        "
            f"{cat_data['totacc'][6]:>7,}  {cat_data['totamt'][6]:>15,.2f}  "
            f"{cat_data['totacc'][7]:>7,}  {cat_data['totamt'][7]:>15,.2f}  "
            f"{cat_data['totacc'][8]:>7,}  {cat_data['totamt'][8]:>15,.2f}  "
            f"{cat_data['totacc'][9]:>8,}  {cat_data['totamt'][9]:>17,.2f}  "
            f"{cat_data['totacc'][10]:>8,}  {cat_data['totamt'][10]:>17,.2f}\n"
        )
        output_file.write(
            f"        "
            f"{cat_data['totacc'][11]:>7,}  {cat_data['totamt'][11]:>15,.2f}  "
            f"{cat_data['totacc'][12]:>7,}  {cat_data['totamt'][12]:>15,.2f}  "
            f"{cat_data['totacc'][13]:>7,}  {cat_data['totamt'][13]:>15,.2f}  "
            f"{cat_data['totacc'][14]:>8,}  {cat_data['totamt'][14]:>17,.2f}  "
            f"{cat_data['totacc'][15]:>8,}  {cat_data['totamt'][15]:>17,.2f}\n"
        )
        output_file.write(
            f"        "
            f"{cat_data['totacc'][16]:>7,}  {cat_data['totamt'][16]:>15,.2f}  "
            f"{cat_data['totacc'][17]:>7,}  {cat_data['totamt'][17]:>15,.2f}  "
            f"{cat_data['sgtotacc']:>7,}  {cat_data['sgtotbrh']:>15,.2f}  "
            f"{cat_data['sgtotac2']:>8,}  {cat_data['sgtotbr2']:>17,.2f}  "
            f"{cat_data['gtotacc']:>8,}  {cat_data['gtotbrh']:>17,.2f}\n"
        )
        output_file.write(" ----------------------------------------------------------------------------------------------------------------------------------\n\n")

def generate_15_bucket_report_sas_format(results: Dict, variables: Dict, output_file):
    """Generate 15-bucket report in exact SAS format"""
    
    pagecnt = 0
    
    for cat, cat_data in sorted(results.items()):
        branch_type = cat_data['branches'][0].get('TYPE', 'Unknown') if cat_data['branches'] else 'Unknown'
        
        pagecnt += 1
        
        # Header - Page 1 (15-bucket format)
        output_file.write(f"PROGRAM-ID : EIMAR102-B                   P U B L I C   I S L A M I C   B A N K   B E R H A D                        PAGE NO.: {pagecnt:>4}\n")
        output_file.write(f"                                             OUTSTANDING LOANS IN ARREARS {branch_type:>13}       {variables['RDATE']}\n")
        output_file.write(f"0BRH    NO          < 1 MTH      NO     1 TO < 2 MTH      NO     2 TO < 3 MTH       NO      3 TO < 4 MTH       NO      4 TO < 5 MTH\n")
        output_file.write(f"        NO     5 TO < 6 MTH      NO     6 TO < 7 MTH      NO     7 TO < 8 MTH       NO      8 TO < 9 MTH       NO     9 TO < 12 MTH\n")
        output_file.write(f"        NO   12 TO < 18 MTH      NO   18 TO < 24 MTH      NO   24 TO < 36 MTH       NO          > 36 MTH       NO           DEFICIT\n")
        output_file.write(f"        NO   SUBTOTAL >=3MTH      NO   SUBTOTAL >=6MTH      NO             TOTAL\n")
        output_file.write(f" ----------------------------------------------------------------------------------------------------------------------------------\n")
        
        # Write branch data
        for branch in cat_data["branches"]:
            # Line 1: BRANCH and first 5 buckets
            output_file.write(
                f" {branch['BRANCH']:>3}  "
                f"{branch['NOACC'][1]:>7,}  {branch['BRHAMT'][1]:>15,.2f}  "
                f"{branch['NOACC'][2]:>7,}  {branch['BRHAMT'][2]:>15,.2f}  "
                f"{branch['NOACC'][3]:>7,}  {branch['BRHAMT'][3]:>15,.2f}  "
                f"{branch['NOACC'][4]:>8,}  {branch['BRHAMT'][4]:>17,.2f}  "
                f"{branch['NOACC'][5]:>8,}  {branch['BRHAMT'][5]:>17,.2f}\n"
            )
            
            # Line 2: BRHCODE and next 5 buckets
            output_file.write(
                f" {branch['BRHCODE']:<4}  "
                f"{branch['NOACC'][6]:>7,}  {branch['BRHAMT'][6]:>15,.2f}  "
                f"{branch['NOACC'][7]:>7,}  {branch['BRHAMT'][7]:>15,.2f}  "
                f"{branch['NOACC'][8]:>7,}  {branch['BRHAMT'][8]:>15,.2f}  "
                f"{branch['NOACC'][9]:>8,}  {branch['BRHAMT'][9]:>17,.2f}  "
                f"{branch['NOACC'][10]:>8,}  {branch['BRHAMT'][10]:>17,.2f}\n"
            )
            
            # Line 3: next 5 buckets
            output_file.write(
                f"        "
                f"{branch['NOACC'][11]:>7,}  {branch['BRHAMT'][11]:>15,.2f}  "
                f"{branch['NOACC'][12]:>7,}  {branch['BRHAMT'][12]:>15,.2f}  "
                f"{branch['NOACC'][13]:>7,}  {branch['BRHAMT'][13]:>15,.2f}  "
                f"{branch['NOACC'][14]:>8,}  {branch['BRHAMT'][14]:>17,.2f}  "
                f"{branch['NOACC'][15]:>8,}  {branch['BRHAMT'][15]:>17,.2f}\n"
            )
            
            # Line 4: subtotals
            output_file.write(
                f"        "
                f"{branch['SUBACC']:>7,}  {branch['SUBBRH']:>15,.2f}  "
                f"{branch['SUBAC2']:>8,}  {branch['SUBBR2']:>17,.2f}  "
                f"{branch['SOTACC']:>8,}  {branch['TOTBRH']:>17,.2f}\n"
            )
        
        # Write category totals
        output_file.write(" ----------------------------------------------------------------------------------------------------------------------------------\n")
        output_file.write(
            f" TOT  "
            f"{cat_data['totacc'][1]:>7,}  {cat_data['totamt'][1]:>15,.2f}  "
            f"{cat_data['totacc'][2]:>7,}  {cat_data['totamt'][2]:>15,.2f}  "
            f"{cat_data['totacc'][3]:>7,}  {cat_data['totamt'][3]:>15,.2f}  "
            f"{cat_data['totacc'][4]:>8,}  {cat_data['totamt'][4]:>17,.2f}  "
            f"{cat_data['totacc'][5]:>8,}  {cat_data['totamt'][5]:>17,.2f}\n"
        )
        output_file.write(
            f"        "
            f"{cat_data['totacc'][6]:>7,}  {cat_data['totamt'][6]:>15,.2f}  "
            f"{cat_data['totacc'][7]:>7,}  {cat_data['totamt'][7]:>15,.2f}  "
            f"{cat_data['totacc'][8]:>7,}  {cat_data['totamt'][8]:>15,.2f}  "
            f"{cat_data['totacc'][9]:>8,}  {cat_data['totamt'][9]:>17,.2f}  "
            f"{cat_data['totacc'][10]:>8,}  {cat_data['totamt'][10]:>17,.2f}\n"
        )
        output_file.write(
            f"        "
            f"{cat_data['totacc'][11]:>7,}  {cat_data['totamt'][11]:>15,.2f}  "
            f"{cat_data['totacc'][12]:>7,}  {cat_data['totamt'][12]:>15,.2f}  "
            f"{cat_data['totacc'][13]:>7,}  {cat_data['totamt'][13]:>15,.2f}  "
            f"{cat_data['totacc'][14]:>8,}  {cat_data['totamt'][14]:>17,.2f}  "
            f"{cat_data['totacc'][15]:>8,}  {cat_data['totamt'][15]:>17,.2f}\n"
        )
        output_file.write(
            f"        "
            f"{cat_data['sgtotacc']:>7,}  {cat_data['sgtotbrh']:>15,.2f}  "
            f"{cat_data['sgtotac2']:>8,}  {cat_data['sgtotbr2']:>17,.2f}  "
            f"{cat_data['gtotacc']:>8,}  {cat_data['gtotbrh']:>17,.2f}\n"
        )
        output_file.write(" ----------------------------------------------------------------------------------------------------------------------------------\n\n")

# ============================================================================
# 6. Main Execution
# ============================================================================

def main():
    """Main execution function"""
    print("=" * 60)
    print("EIMIR102 SAS to Python Conversion")
    print("=" * 60)
    
    HPD_LIST = ["110", "115", "700", "705"]
    
    # 1. Process REPTDATE
    print("\n1. Processing REPTDATE...")
    variables = process_repdate()
    print(f"   Report Date: {variables['RDATE']}")
    print(f"   Day of Month: {variables['REPTDAY']}")
    
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
    
    # Create branch mapping
    if len(branch_df) > 0:
        branch_codes = branch_df["BRHCODE"].to_list()
        branch_map = {i+1: code for i, code in enumerate(branch_codes)}
        merged_data = categorized.with_columns(
            pl.col("BRANCH").map_elements(
                lambda x: branch_map.get(x, ""), 
                return_dtype=pl.Utf8
            ).alias("BRHCODE")
        )
    else:
        merged_data = categorized.with_columns(pl.lit("").alias("BRHCODE"))
    
    print(f"   Merged records: {len(merged_data)}")
    
    # 5. Generate reports in SAS format
    print("\n5. Generating reports in SAS format...")
    
    # Open output file in append mode (like SAS CCDTXT2 with DISP=MOD)
    output_file = OUTPUT_PATH / "CCDTXT2_REPORT.txt"
    
    with open(output_file, 'w') as f:
        # 17-bucket report (EIMAR102-A)
        print("   Generating 17-bucket report (EIMAR102-A)...")
        results_17 = calculate_17_bucket_summaries(merged_data)
        generate_17_bucket_report_sas_format(results_17, variables, f)
        print(f"   Categories processed: {len(results_17)}")
        
        # 15-bucket report only on day 15 (EIMAR102-B)
        if variables['REPTDAY'] == '15':
            print("   Day 15 detected - Generating 15-bucket report (EIMAR102-B)...")
            results_15 = calculate_15_bucket_summaries(merged_data)
            generate_15_bucket_report_sas_format(results_15, variables, f)
            print(f"   Categories processed: {len(results_15)}")
        else:
            print("   Not day 15 - Skipping 15-bucket report")
    
    print(f"\n✓ Report saved to: {output_file}")
    
    # 6. Summary statistics
    print("\n6. Creating summary statistics...")
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
            
            f.write(f"Category {cat}: {cat_data['TYPE'][0]}\n")
            f.write(f"  Total Accounts: {total_accounts:,}\n")
            f.write(f"  Total Balance: {total_balance:,.2f}\n")
            f.write(f"  Average Balance: {avg_balance:,.2f}\n\n")
    
    print(f"✓ Summary statistics saved: {summary_file}")
    
    print("\n" + "=" * 60)
    print("CONVERSION COMPLETE")
    print("=" * 60)
    print(f"Total loan records: {len(loan_df)}")
    print(f"Categorized records: {len(merged_data)}")
    print(f"Report day: {variables['REPTDAY']}")
    print(f"Output saved to: {OUTPUT_PATH}")

if __name__ == "__main__":
    main()
