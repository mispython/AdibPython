import duckdb
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

# Output file - only date, no timestamp
OUTPUT_FILE = OUTPUT_PATH / f"eimar201_report_{datetime.now().strftime('%Y%m%d')}.txt"

def read_sas7bdat_with_pandas(filepath):
    """Read SAS7BDAT file using pandas/pyreadstat"""
    print(f"Reading SAS file: {filepath}")
    if not filepath.exists():
        raise FileNotFoundError(f"SAS file not found: {filepath}")
    df, meta = pyreadstat.read_sas7bdat(str(filepath))
    return df

def format_line1(branch, values):
    """Format line 1: BRANCH + columns 1-5"""
    return (f"{branch:>3}     {values[0]:>7,.0f} {values[1]:>16,.2f}     {values[2]:>7,.0f} {values[3]:>15,.2f}     "
            f"{values[4]:>7,.0f} {values[5]:>15,.2f}      {values[6]:>8,.0f} {values[7]:>17,.2f}     "
            f"{values[8]:>8,.0f} {values[9]:>17,.2f}")

def format_line2(brhcode, values):
    """Format line 2: BRHCODE + columns 6-10"""
    return (f"{brhcode:<3}     {values[0]:>7,.0f} {values[1]:>16,.2f}     {values[2]:>7,.0f} {values[3]:>15,.2f}     "
            f"{values[4]:>7,.0f} {values[5]:>15,.2f}      {values[6]:>8,.0f} {values[7]:>17,.2f}     "
            f"{values[8]:>8,.0f} {values[9]:>17,.2f}")

def format_line3(values):
    """Format line 3: columns 11-15"""
    return (f"        {values[0]:>7,.0f} {values[1]:>16,.2f}     {values[2]:>7,.0f} {values[3]:>15,.2f}     "
            f"{values[4]:>7,.0f} {values[5]:>15,.2f}      {values[6]:>8,.0f} {values[7]:>17,.2f}     "
            f"{values[8]:>8,.0f} {values[9]:>17,.2f}")

def format_line4(values):
    """Format line 4: columns 16-17 + subtotals"""
    return (f"        {values[0]:>7,.0f} {values[1]:>16,.2f}     {values[2]:>7,.0f} {values[3]:>15,.2f}     "
            f"{values[4]:>7,.0f} {values[5]:>15,.2f}      {values[6]:>8,.0f} {values[7]:>17,.2f}     "
            f"{values[8]:>8,.0f} {values[9]:>17,.2f}")

def main():
    # Use current date minus 1 day
    current_date = datetime.now() - timedelta(days=1)
    # Format as DD/MM/YY like SAS DDMMYY8 format
    rdate = current_date.strftime('%d/%m/%y')
    
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
    brhdata_df = pd.read_fwf(
        brhfile_final,
        colspecs=[(1, 4), (5, 8)],
        names=['BRANCH', 'BRHCODE'],
        dtype={'BRANCH': str, 'BRHCODE': str},
        header=None
    )
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
            
            for branch, branch_group in cat_group.groupby('BRANCH_NUM'):
                brhamt = np.zeros(17)
                noacc = np.zeros(17)
                
                # Process each row in branch
                for _, row in branch_group.iterrows():
                    if row['BALANCE'] > 0:
                        arrears_idx = int(row['ARREAR']) - 1
                        if 0 <= arrears_idx < 17:
                            brhamt[arrears_idx] += row['BALANCE']
                            noacc[arrears_idx] += 1
                
                # Calculate subtotals
                subbrh = np.sum(brhamt[3:])
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
                    # Column headers with proper spacing to match data columns
                    f.write("BRH       NO          < 1 MTH         NO     1 TO < 2 MTH         NO     2 TO < 3 MTH          NO      3 TO < 4 MTH          NO      4 TO < 5 MTH\n")
                    f.write("          NO     5 TO < 6 MTH         NO     6 TO < 7 MTH         NO     7 TO < 8 MTH          NO      8 TO < 9 MTH          NO     9 TO < 10 MTH\n")
                    f.write("          NO   10 TO < 11 MTH         NO   11 TO < 12 MTH         NO   12 TO < 18 MTH          NO    18 TO < 24 MTH          NO    24 TO < 36 MTH\n")
                    f.write("          NO         > 36 MTH         NO          DEFICIT         NO   SUBTOTAL >=3MTH         NO   SUBTOTAL >=6MTH          NO             TOTAL\n")
                    f.write("-" * 145 + "\n")
                    first_branch_in_category = False
                
                # Get BRHCODE
                brhcode = branch_group['BRHCODE'].iloc[0] if len(branch_group) > 0 else '   '
                
                # Line 1: Branch number + columns 1-5
                f.write(format_line1(branch, [noacc[0], brhamt[0], noacc[1], brhamt[1], noacc[2], brhamt[2], noacc[3], brhamt[3], noacc[4], brhamt[4]]) + "\n")
                
                # Line 2: BRHCODE + columns 6-10
                f.write(format_line2(brhcode, [noacc[5], brhamt[5], noacc[6], brhamt[6], noacc[7], brhamt[7], noacc[8], brhamt[8], noacc[9], brhamt[9]]) + "\n")
                
                # Line 3: Columns 11-15
                f.write(format_line3([noacc[10], brhamt[10], noacc[11], brhamt[11], noacc[12], brhamt[12], noacc[13], brhamt[13], noacc[14], brhamt[14]]) + "\n")
                
                # Line 4: Columns 16-17 + subtotals
                f.write(format_line4([noacc[15], brhamt[15], noacc[16], brhamt[16], subacc, subbrh, subac2, subbr2, sotacc, totbrh]) + "\n")
            
            # Calculate grand totals for category
            sgtotbrh = np.sum(totamt[3:])
            sgtotbr2 = sgtotbrh - totamt[3] - totamt[4] - totamt[5]
            sgtotacc = np.sum(totacc[3:])
            sgtotac2 = sgtotacc - totacc[3] - totacc[4] - totacc[5]
            gtotbrh = sgtotbrh + totamt[0] + totamt[1] + totamt[2]
            gtotacc = sgtotacc + totacc[0] + totacc[1] + totacc[2]
            
            # Print category totals
            f.write("-" * 145 + "\n")
            f.write(format_line1("TOT", [totacc[0], totamt[0], totacc[1], totamt[1], totacc[2], totamt[2], totacc[3], totamt[3], totacc[4], totamt[4]]) + "\n")
            f.write(format_line2("", [totacc[5], totamt[5], totacc[6], totamt[6], totacc[7], totamt[7], totacc[8], totamt[8], totacc[9], totamt[9]]) + "\n")
            f.write(format_line3([totacc[10], totamt[10], totacc[11], totamt[11], totacc[12], totamt[12], totacc[13], totamt[13], totacc[14], totamt[14]]) + "\n")
            f.write(format_line4([totacc[15], totamt[15], totacc[16], totamt[16], sgtotacc, sgtotbrh, sgtotac2, sgtotbr2, gtotacc, gtotbrh]) + "\n")
            f.write("-" * 145 + "\n")
            f.write("\n")
    
    print(f"Report generated: {OUTPUT_FILE}")
    print(f"Report date (current date - 1 day): {rdate}")

if __name__ == "__main__":
    main()
