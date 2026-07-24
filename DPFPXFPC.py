import duckdb
import polars as pl
import pandas as pd
import pyreadstat
from pathlib import Path
from datetime import datetime, timedelta
import numpy as np
import os

# Path configuration
SCRIPT_DIR = Path(os.path.dirname(os.path.abspath(__file__)))
INPUT_PATH = SCRIPT_DIR / "input" / "prod" / "EIMIR201"
OUTPUT_PATH = SCRIPT_DIR / "output"
OUTPUT_PATH.mkdir(parents=True, exist_ok=True)

# File paths
LOANTEMP_FILE = INPUT_PATH / "bnm" / "loantemp.sas7bdat"
BRHFILE_FILE = INPUT_PATH / "LKP_BRANCH"

# Output file
OUTPUT_FILE = OUTPUT_PATH / f"eimar201_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"

def read_sas7bdat_with_pandas(filepath):
    """Read SAS7BDAT file using pandas/pyreadstat"""
    print(f"Reading SAS file: {filepath}")
    if not filepath.exists():
        raise FileNotFoundError(f"SAS file not found: {filepath}")
    df, meta = pyreadstat.read_sas7bdat(str(filepath))
    return df

def format_number_commas(value, decimal_places=0):
    """Format number with commas, matching SAS COMMA format"""
    if decimal_places == 0:
        return f"{value:,.0f}"
    else:
        return f"{value:,.{decimal_places}f}"

def main():
    # Use current date minus 1 day
    current_date = datetime.now() - timedelta(days=1)
    rdate = current_date.strftime('%d%m%y')  # DDMMYY8 format
    
    print(f"Report date: {rdate}")

    # Check and find BRHFILE
    if not BRHFILE_FILE.exists():
        alt_paths = [
            Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIMIR201/LKP_BRANCH"),
            Path("input/prod/EIMIR201/LKP_BRANCH"),
            Path("../input/prod/EIMIR201/LKP_BRANCH"),
        ]
        for alt_path in alt_paths:
            if alt_path.exists():
                print(f"Found BRHFILE at: {alt_path}")
                brhfile_final = alt_path
                break
        else:
            raise FileNotFoundError(f"BRHFILE not found")
    else:
        brhfile_final = BRHFILE_FILE

    # Read BRHFILE with correct column specifications
    # SAS: INPUT @2 BRANCH 3. @6 BRHCODE $3.
    # This means BRANCH starts at position 2 (3 chars) and BRHCODE starts at position 6 (3 chars)
    brhdata_df = pd.read_fwf(
        brhfile_final,
        colspecs=[(1, 4), (5, 8)],  # 0-based in Python, so position 2 is index 1
        names=['BRANCH', 'BRHCODE'],
        dtype={'BRANCH': str, 'BRHCODE': str},
        header=None
    )
    # Clean BRANCH - remove leading zeros but keep as string for formatting
    brhdata_df['BRANCH'] = brhdata_df['BRANCH'].str.strip()
    brhdata_df['BRANCH_NUM'] = pd.to_numeric(brhdata_df['BRANCH'], errors='coerce').fillna(0).astype(int)
    print(f"Read {len(brhdata_df)} branch records")

    # Check and find LOANTEMP file
    if not LOANTEMP_FILE.exists():
        alt_loantemp_paths = [
            Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIMIR201/bnm/loantemp.sas7bdat"),
            Path("input/prod/EIMIR201/bnm/loantemp.sas7bdat"),
        ]
        for alt_path in alt_loantemp_paths:
            if alt_path.exists():
                print(f"Found LOANTEMP at: {alt_path}")
                loantemp_file_final = alt_path
                break
        else:
            raise FileNotFoundError(f"LOANTEMP file not found")
    else:
        loantemp_file_final = LOANTEMP_FILE

    # Read LOANTEMP
    loantemp_df = read_sas7bdat_with_pandas(loantemp_file_final)
    print(f"Read {len(loantemp_df)} loan records")

    # Convert to DuckDB for processing
    conn = duckdb.connect(':memory:')
    conn.register('loantemp', loantemp_df)
    conn.register('brhdata', brhdata_df)

    # Create LOANTEM2 with proper CAT ordering
    loantem2_query = """
    WITH categorized AS (
        SELECT 
            *,
            CASE 
                WHEN (PRODUCT IN (380, 381, 700, 705)) AND CHECKDT = 1 THEN 'A'
                WHEN (PRODUCT IN (380, 381)) AND CHECKDT = 1 THEN 'B'
                WHEN (PRODUCT IN (128, 130)) AND CHECKDT = 1 THEN 'C'
                WHEN (PRODUCT IN (128, 130, 380, 381, 700, 705)) AND CHECKDT = 1 THEN 'D'
            END AS CAT,
            CASE 
                WHEN (PRODUCT IN (380, 381, 700, 705)) AND CHECKDT = 1 THEN '(HPD-C)'
                WHEN (PRODUCT IN (380, 381)) AND CHECKDT = 1 THEN '(HP 380/381)'
                WHEN (PRODUCT IN (128, 130)) AND CHECKDT = 1 THEN '(AITAB)'
                WHEN (PRODUCT IN (128, 130, 380, 381, 700, 705)) AND CHECKDT = 1 THEN '(-HPD-)'
            END AS TYPE
        FROM loantemp
        WHERE (PRODUCT IN (380, 381, 700, 705) AND CHECKDT = 1)
           OR (PRODUCT IN (380, 381) AND CHECKDT = 1)
           OR (PRODUCT IN (128, 130) AND CHECKDT = 1)
           OR (PRODUCT IN (128, 130, 380, 381, 700, 705) AND CHECKDT = 1)
    )
    SELECT * FROM categorized
    WHERE CAT IS NOT NULL
    """
    
    loantem2_df = conn.execute(loantem2_query).fetchdf()
    print(f"Filtered to {len(loantem2_df)} records for reporting")
    
    # Merge with BRHDATA
    loantemp_final_df = loantem2_df.merge(brhdata_df, left_on='BRANCH', right_on='BRANCH_NUM', how='inner')
    loantemp_final_df = loantemp_final_df.sort_values(['CAT', 'BRANCH_NUM'])
    print(f"Merged data: {len(loantemp_final_df)} records")
    
    # Generate report
    with open(OUTPUT_FILE, 'w') as f:
        pagecnt = 0
        
        # Process each CAT group
        for cat, cat_group in loantemp_final_df.groupby('CAT'):
            if cat is None or pd.isna(cat):
                continue
                
            # Initialize category-level arrays and totals
            totamt = np.zeros(17)
            totacc = np.zeros(17)
            
            first_branch_in_category = True
            line_count = 0
            
            for branch, branch_group in cat_group.groupby('BRANCH_NUM'):
                brhamt = np.zeros(17)
                noacc = np.zeros(17)
                
                # Process each row in branch
                for _, row in branch_group.iterrows():
                    if row['BALANCE'] > 0:
                        arrears_idx = int(row['ARREAR']) - 1  # 1-based to 0-based
                        if 0 <= arrears_idx < 17:
                            brhamt[arrears_idx] += row['BALANCE']
                            noacc[arrears_idx] += 1
                
                # Calculate subtotals
                subbrh = np.sum(brhamt[3:])  # elements 4-17 (0-based index 3-16)
                subbr2 = subbrh - brhamt[3] - brhamt[4] - brhamt[5]
                subacc = np.sum(noacc[3:])
                subac2 = subacc - noacc[3] - noacc[4] - noacc[5]
                totbrh = subbrh + brhamt[0] + brhamt[1] + brhamt[2]
                sotacc = subacc + noacc[0] + noacc[1] + noacc[2]
                
                # Update category totals
                totamt += brhamt
                totacc += noacc
                
                # Print page header if first branch in category
                if first_branch_in_category:
                    pagecnt += 1
                    f.write(f"PROGRAM-ID : EIMAR201                     P U B L I C   I S L A M I C   B A N K   B E R H A D                        PAGE NO.: {pagecnt}\n")
                    cat_type = branch_group['TYPE'].iloc[0] if len(branch_group) > 0 else '          '
                    f.write(f"                                   OUTSTANDING LOANS IN ARREARS ISSUED FROM 01 JAN 1998  {cat_type}       {rdate}\n")
                    f.write("\n")
                    f.write("BRH    NO          < 1 MTH      NO     1 TO < 2 MTH      NO     2 TO < 3 MTH       NO      3 TO < 4 MTH       NO      4 TO < 5 MTH\n")
                    f.write("       NO     5 TO < 6 MTH      NO     6 TO < 7 MTH      NO     7 TO < 8 MTH       NO      8 TO < 9 MTH       NO     9 TO < 10 MTH\n")
                    f.write("       NO   10 TO < 11 MTH      NO   11 TO < 12 MTH      NO   12 TO < 18 MTH       NO    18 TO < 24 MTH       NO    24 TO < 36 MTH\n")
                    f.write("       NO         > 36 MTH      NO          DEFICIT      NO   SUBTOTAL >=3MTH      NO   SUBTOTAL >=6MTH       NO             TOTAL\n")
                    f.write("----------------------------------------------------------------------------------------------------------------------------------\n")
                    first_branch_in_category = False
                    line_count = 0
                
                # Format branch number with leading spaces (SAS Z3 format)
                branch_str = f"{branch:3d}"
                
                # Get BRHCODE
                brhcode = branch_group['BRHCODE'].iloc[0] if len(branch_group) > 0 else '   '
                brhcode_str = f"{brhcode:<3}"
                
                # Line 1: BRANCH, NOACC1-5, BRHAMT1-5
                f.write(f" {branch_str}    {noacc[0]:>7,.0f}       {brhamt[0]:>15,.2f}       "
                       f"{noacc[1]:>7,.0f}       {brhamt[1]:>15,.2f}       "
                       f"{noacc[2]:>7,.0f}       {brhamt[2]:>15,.2f}        "
                       f"{noacc[3]:>8,.0f}        {brhamt[3]:>17,.2f}        "
                       f"{noacc[4]:>8,.0f}        {brhamt[4]:>17,.2f}\n")
                
                # Line 2: BRHCODE, NOACC6-10, BRHAMT6-10
                f.write(f" {brhcode_str}    {noacc[5]:>7,.0f}       {brhamt[5]:>15,.2f}       "
                       f"{noacc[6]:>7,.0f}       {brhamt[6]:>15,.2f}       "
                       f"{noacc[7]:>7,.0f}       {brhamt[7]:>15,.2f}        "
                       f"{noacc[8]:>8,.0f}        {brhamt[8]:>17,.2f}        "
                       f"{noacc[9]:>8,.0f}        {brhamt[9]:>17,.2f}\n")
                
                # Line 3: NOACC11-15, BRHAMT11-15
                f.write(f"       {noacc[10]:>7,.0f}       {brhamt[10]:>15,.2f}       "
                       f"{noacc[11]:>7,.0f}       {brhamt[11]:>15,.2f}       "
                       f"{noacc[12]:>7,.0f}       {brhamt[12]:>15,.2f}        "
                       f"{noacc[13]:>8,.0f}        {brhamt[13]:>17,.2f}        "
                       f"{noacc[14]:>8,.0f}        {brhamt[14]:>17,.2f}\n")
                
                # Line 4: NOACC16-17, SUBACC, SUBBRH, SUBAC2, SUBBR2, SOTACC, TOTBRH
                f.write(f"       {noacc[15]:>7,.0f}       {brhamt[15]:>15,.2f}       "
                       f"{noacc[16]:>7,.0f}       {brhamt[16]:>15,.2f}       "
                       f"{subacc:>7,.0f}       {subbrh:>15,.2f}        "
                       f"{subac2:>8,.0f}        {subbr2:>17,.2f}       "
                       f"{sotacc:>8,.0f}      {totbrh:>16,.2f}\n")
                
                line_count += 4
            
            # Calculate grand totals for category
            sgtotbrh = np.sum(totamt[3:])
            sgtotbr2 = sgtotbrh - totamt[3] - totamt[4] - totamt[5]
            sgtotacc = np.sum(totacc[3:])
            sgtotac2 = sgtotacc - totacc[3] - totacc[4] - totacc[5]
            gtotbrh = sgtotbrh + totamt[0] + totamt[1] + totamt[2]
            gtotacc = sgtotacc + totacc[0] + totacc[1] + totacc[2]
            
            # Print category totals
            f.write("----------------------------------------------------------------------------------------------------------------------------------\n")
            f.write(f" TOT    {totacc[0]:>7,.0f}       {totamt[0]:>15,.2f}       "
                   f"{totacc[1]:>7,.0f}       {totamt[1]:>15,.2f}       "
                   f"{totacc[2]:>7,.0f}       {totamt[2]:>15,.2f}        "
                   f"{totacc[3]:>8,.0f}        {totamt[3]:>17,.2f}        "
                   f"{totacc[4]:>8,.0f}        {totamt[4]:>17,.2f}\n")
            f.write(f"       {totacc[5]:>7,.0f}       {totamt[5]:>15,.2f}       "
                   f"{totacc[6]:>7,.0f}       {totamt[6]:>15,.2f}       "
                   f"{totacc[7]:>7,.0f}       {totamt[7]:>15,.2f}        "
                   f"{totacc[8]:>8,.0f}        {totamt[8]:>17,.2f}        "
                   f"{totacc[9]:>8,.0f}        {totamt[9]:>17,.2f}\n")
            f.write(f"       {totacc[10]:>7,.0f}       {totamt[10]:>15,.2f}       "
                   f"{totacc[11]:>7,.0f}       {totamt[11]:>15,.2f}       "
                   f"{totacc[12]:>7,.0f}       {totamt[12]:>15,.2f}        "
                   f"{totacc[13]:>8,.0f}        {totamt[13]:>17,.2f}        "
                   f"{totacc[14]:>8,.0f}        {totamt[14]:>17,.2f}\n")
            f.write(f"       {totacc[15]:>7,.0f}       {totamt[15]:>15,.2f}       "
                   f"{totacc[16]:>7,.0f}       {totamt[16]:>15,.2f}       "
                   f"{sgtotacc:>7,.0f}       {sgtotbrh:>15,.2f}        "
                   f"{sgtotac2:>8,.0f}        {sgtotbr2:>17,.2f}       "
                   f"{gtotacc:>8,.0f}      {gtotbrh:>16,.2f}\n")
            f.write("----------------------------------------------------------------------------------------------------------------------------------\n")
            f.write("\n")
    
    print(f"Report generated: {OUTPUT_FILE}")
    print(f"Report date (current date - 1 day): {rdate}")

if __name__ == "__main__":
    main()
