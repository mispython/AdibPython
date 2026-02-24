"""
EIMAR202 - NPL (Non-Performing Loan) Report by Arrears Buckets
Complete version with all SAS logic
"""

import polars as pl
from datetime import datetime
from pathlib import Path
import sys

# =============================================================================
# CONFIGURATION
# =============================================================================
PATHS = {
    'BNM': 'data/bnm/',
    'OUTPUT': 'output/'
}

for path in PATHS.values():
    Path(path).mkdir(parents=True, exist_ok=True)

# Arrears buckets definition (1-17 as per SAS arrays)
ARREAR_BUCKETS = list(range(1, 18))

# Product categories for HP Direct
HP_PRODUCTS = {
    'A': {'name': '(HPD-C)', 'products': [380, 381, 700, 705]},
    'B': {'name': '(HP 380/381)', 'products': [380, 381]},
    'C': {'name': '(AITAB)', 'products': [128, 130]},
    'D': {'name': '(-HPD-)', 'products': [128, 130, 380, 381, 700, 705]}
}

# Products with special handling (commented in SAS)
SPECIAL_PRODUCTS = [110, 115, 700, 705]
SPECIAL_BALANCE_THRESHOLD = 200

# =============================================================================
# REPORT DATE
# =============================================================================
def get_report_date():
    """Get report date from BNM.REPTDATE and set macro variables"""
    try:
        df = pl.read_parquet(f"{PATHS['BNM']}REPTDATE.parquet")
        reptdate = df['REPTDATE'][0]
        
        # Macro variables
        rdate = reptdate.strftime('%d%m%Y')
        reptyear = reptdate.strftime('%Y')
        reptmon = f"{reptdate.month:02d}"
        reptday = f"{reptdate.day:02d}"
        
        return {
            'rdate': rdate,
            'reptyear': reptyear,
            'reptmon': reptmon,
            'reptday': reptday,
            'date_obj': reptdate
        }
    except Exception as e:
        print(f"Warning: Could not read REPTDATE: {e}")
        today = datetime.now()
        return {
            'rdate': today.strftime('%d%m%Y'),
            'reptyear': today.strftime('%Y'),
            'reptmon': f"{today.month:02d}",
            'reptday': f"{today.day:02d}",
            'date_obj': today
        }

# =============================================================================
# BRANCH DATA
# =============================================================================
def read_branch_data():
    """Read branch data from fixed-width file BRHFILE (LRECL=80)"""
    branches = {}
    try:
        brh_file = Path("data/input/BRHFILE.txt")
        if brh_file.exists():
            with open(brh_file, 'r') as f:
                for line in f:
                    if len(line) >= 8:
                        # SAS: @2 BRANCH 3. @6 BRHCODE $3.
                        branch_str = line[1:4].strip()
                        brhcode = line[5:8].strip()
                        
                        if branch_str and branch_str.isdigit():
                            branches[int(branch_str)] = brhcode
    except Exception as e:
        print(f"Warning: Could not read branch file: {e}")
    
    return branches

# =============================================================================
# MAIN REPORT
# =============================================================================
def main():
    print("=" * 60)
    print("EIMAR202 - NPL Report by Arrears Buckets")
    print("=" * 60)
    
    # Get report date
    rep = get_report_date()
    print(f"\nReport Date: {rep['rdate']}")
    
    # Read branch data
    branch_codes = read_branch_data()
    print(f"Branch codes loaded: {len(branch_codes)}")
    
    # Read loan data
    print("\nReading loan data...")
    try:
        df = pl.read_parquet(f"{PATHS['BNM']}LOANTEMP.parquet")
        print(f"  Total loans: {len(df):,}")
    except Exception as e:
        print(f"Error reading LOANTEMP: {e}")
        sys.exit(1)
    
    # Filter for NPL based on SAS criteria
    # IF ARREAR > 6 OR BORSTAT = 'R' OR BORSTAT = 'I' OR BORSTAT = 'F'
    df = df.filter(
        (pl.col('ARREAR') > 6) |
        (pl.col('BORSTAT').is_in(['R', 'I', 'F']))
    )
    print(f"  NPL loans: {len(df):,}")
    
    # Process each product category (A, B, C, D)
    all_results = []
    
    for cat, config in HP_PRODUCTS.items():
        # Filter for this category's products with CHECKDT = 1
        cat_df = df.filter(
            pl.col('PRODUCT').is_in(config['products']) &
            (pl.col('CHECKDT') == 1)
        )
        
        if not cat_df.is_empty():
            # Add category and type
            cat_df = cat_df.with_columns([
                pl.lit(cat).alias('CAT'),
                pl.lit(config['name']).alias('TYPE')
            ])
            all_results.append(cat_df)
    
    if not all_results:
        print("No matching loans found")
        return
    
    # Combine all categories
    loan_df = pl.concat(all_results)
    
    # Merge with branch codes (equivalent to MERGE with BRHDATA)
    branch_df = pl.DataFrame({
        'BRANCH': list(branch_codes.keys()),
        'BRHCODE': list(branch_codes.values())
    })
    
    loan_df = loan_df.join(branch_df, on='BRANCH', how='left')
    
    # Sort by CAT, BRANCH (matches PROC SORT)
    loan_df = loan_df.sort(['CAT', 'BRANCH'])
    
    # Generate report
    print("\nGenerating report...")
    
    output_file = PATHS['OUTPUT'] / f"EIMAR202_{rep['rdate']}.txt"
    all_lines = []
    
    # Page tracking
    page_num = 0
    
    # Process by CAT
    for cat_idx, cat in enumerate(loan_df['CAT'].unique().sort()):
        cat_df = loan_df.filter(pl.col('CAT') == cat)
        cat_type = cat_df['TYPE'][0]
        
        # Initialize category totals (like TOTAMT and TOTACC arrays in SAS)
        cat_tot_amt = {i: 0.0 for i in ARREAR_BUCKETS}
        cat_tot_acc = {i: 0 for i in ARREAR_BUCKETS}
        
        # New page for each category (PUT _PAGE_ in SAS)
        page_num += 1
        all_lines.extend(generate_header(page_num, cat_type, rep['rdate']))
        
        # Process by BRANCH within CAT
        for branch in cat_df['BRANCH'].unique().sort():
            branch_df = cat_df.filter(pl.col('BRANCH') == branch)
            brhcode = branch_df['BRHCODE'][0] if not branch_df['BRHCODE'].is_null().all() else ''
            
            # Initialize branch arrays (BRHAMT and NOACC in SAS)
            brh_amt = {i: 0.0 for i in ARREAR_BUCKETS}
            brh_acc = {i: 0 for i in ARREAR_BUCKETS}
            
            # Aggregate by arrears bucket with special product handling
            for row in branch_df.rows(named=True):
                arrears = row.get('ARREAR', 0)
                if 1 <= arrears <= 17:
                    balance = row.get('BALANCE', 0)
                    product = row.get('PRODUCT', 0)
                    
                    if balance > 0:
                        # SAS logic: BRHAMT(ARREAR) + BALANCE;
                        brh_amt[arrears] += balance
                        
                        # SAS logic with commented special handling:
                        # IF PRODUCT IN (110,115,700,705) THEN DO;
                        #    IF BALANCE GT 200 THEN NOACC(ARREAR) + 1;
                        # END; ELSE NOACC(ARREAR) + 1;
                        if product in SPECIAL_PRODUCTS and balance <= SPECIAL_BALANCE_THRESHOLD:
                            # Skip counting for these products with low balance
                            pass
                        else:
                            brh_acc[arrears] += 1
                            cat_tot_acc[arrears] += 1
                        
                        cat_tot_amt[arrears] += balance
            
            # Calculate branch subtotals (exactly as SAS)
            # SUBBRH = SUM(BRHAMT4-BRHAMT17)
            sub_amt = sum(brh_amt[i] for i in range(4, 18))
            # SUBACC = SUM(NOACC4-NOACC17)
            sub_acc = sum(brh_acc[i] for i in range(4, 18))
            
            # SUBBR2 = SUBBRH - BRHAMT4 - BRHAMT5 - BRHAMT6
            sub_amt2 = sub_amt - brh_amt[4] - brh_amt[5] - brh_amt[6]
            # SUBAC2 = SUBACC - NOACC4 - NOACC5 - NOACC6
            sub_acc2 = sub_acc - brh_acc[4] - brh_acc[5] - brh_acc[6]
            
            # TOTBRH = SUM(SUBBRH,BRHAMT1,BRHAMT2,BRHAMT3)
            tot_amt = sub_amt + brh_amt[1] + brh_amt[2] + brh_amt[3]
            # SOTACC = SUM(SUBACC,NOACC1,NOACC2,NOACC3)
            tot_acc = sub_acc + brh_acc[1] + brh_acc[2] + brh_acc[3]
            
            # Output branch lines (exact SAS positioning)
            # First PUT statement
            all_lines.append(
                f"{branch:03d} {brh_acc[1]:>7,.0f} {brh_amt[1]:>16,.2f} "
                f"{brh_acc[2]:>7,.0f} {brh_amt[2]:>15,.2f} "
                f"{brh_acc[3]:>7,.0f} {brh_amt[3]:>15,.2f} "
                f"{brh_acc[4]:>8,.0f} {brh_amt[4]:>17,.2f} "
                f"{brh_acc[5]:>8,.0f} {brh_amt[5]:>17,.2f}"
            )
            
            # Second PUT statement with BRHCODE
            all_lines.append(
                f"{brhcode} {brh_acc[6]:>7,.0f} {brh_amt[6]:>16,.2f} "
                f"{brh_acc[7]:>7,.0f} {brh_amt[7]:>15,.2f} "
                f"{brh_acc[8]:>7,.0f} {brh_amt[8]:>15,.2f} "
                f"{brh_acc[9]:>8,.0f} {brh_amt[9]:>17,.2f} "
                f"{brh_acc[10]:>8,.0f} {brh_amt[10]:>17,.2f}"
            )
            
            # Third PUT statement
            all_lines.append(
                f"     {brh_acc[11]:>7,.0f} {brh_amt[11]:>16,.2f} "
                f"{brh_acc[12]:>7,.0f} {brh_amt[12]:>15,.2f} "
                f"{brh_acc[13]:>7,.0f} {brh_amt[13]:>15,.2f} "
                f"{brh_acc[14]:>8,.0f} {brh_amt[14]:>17,.2f} "
                f"{brh_acc[15]:>8,.0f} {brh_amt[15]:>17,.2f}"
            )
            
            # Fourth PUT statement with subtotals
            all_lines.append(
                f"     {brh_acc[16]:>7,.0f} {brh_amt[16]:>16,.2f} "
                f"{brh_acc[17]:>7,.0f} {brh_amt[17]:>15,.2f} "
                f"{sub_acc:>7,.0f} {sub_amt:>15,.2f} "
                f"{sub_acc2:>8,.0f} {sub_amt2:>17,.2f} "
                f"{tot_acc:>8,.0f} {tot_amt:>17,.2f}"
            )
            
            all_lines.append("")  # Blank line between branches
        
        # Calculate category subtotals (SGTOTBRH, SGTOTACC, etc.)
        cat_sub_amt = sum(cat_tot_amt[i] for i in range(4, 18))
        cat_sub_acc = sum(cat_tot_acc[i] for i in range(4, 18))
        
        cat_sub_amt2 = cat_sub_amt - cat_tot_amt[4] - cat_tot_amt[5] - cat_tot_amt[6]
        cat_sub_acc2 = cat_sub_acc - cat_tot_acc[4] - cat_tot_acc[5] - cat_tot_acc[6]
        
        cat_tot_amt_all = cat_sub_amt + cat_tot_amt[1] + cat_tot_amt[2] + cat_tot_amt[3]
        cat_tot_acc_all = cat_sub_acc + cat_tot_acc[1] + cat_tot_acc[2] + cat_tot_acc[3]
        
        # Category separator line
        all_lines.append("-" * 130)
        
        # Category total lines (exactly as SAS)
        all_lines.append(
            f"TOT {cat_tot_acc[1]:>7,.0f} {cat_tot_amt[1]:>16,.2f} "
            f"{cat_tot_acc[2]:>7,.0f} {cat_tot_amt[2]:>15,.2f} "
            f"{cat_tot_acc[3]:>7,.0f} {cat_tot_amt[3]:>15,.2f} "
            f"{cat_tot_acc[4]:>8,.0f} {cat_tot_amt[4]:>17,.2f} "
            f"{cat_tot_acc[5]:>8,.0f} {cat_tot_amt[5]:>17,.2f}"
        )
        
        all_lines.append(
            f"     {cat_tot_acc[6]:>7,.0f} {cat_tot_amt[6]:>16,.2f} "
            f"{cat_tot_acc[7]:>7,.0f} {cat_tot_amt[7]:>15,.2f} "
            f"{cat_tot_acc[8]:>7,.0f} {cat_tot_amt[8]:>15,.2f} "
            f"{cat_tot_acc[9]:>8,.0f} {cat_tot_amt[9]:>17,.2f} "
            f"{cat_tot_acc[10]:>8,.0f} {cat_tot_amt[10]:>17,.2f}"
        )
        
        all_lines.append(
            f"     {cat_tot_acc[11]:>7,.0f} {cat_tot_amt[11]:>16,.2f} "
            f"{cat_tot_acc[12]:>7,.0f} {cat_tot_amt[12]:>15,.2f} "
            f"{cat_tot_acc[13]:>7,.0f} {cat_tot_amt[13]:>15,.2f} "
            f"{cat_tot_acc[14]:>8,.0f} {cat_tot_amt[14]:>17,.2f} "
            f"{cat_tot_acc[15]:>8,.0f} {cat_tot_amt[15]:>17,.2f}"
        )
        
        all_lines.append(
            f"     {cat_tot_acc[16]:>7,.0f} {cat_tot_amt[16]:>16,.2f} "
            f"{cat_tot_acc[17]:>7,.0f} {cat_tot_amt[17]:>15,.2f} "
            f"{cat_sub_acc:>7,.0f} {cat_sub_amt:>15,.2f} "
            f"{cat_sub_acc2:>8,.0f} {cat_sub_amt2:>17,.2f} "
            f"{cat_tot_acc_all:>8,.0f} {cat_tot_amt_all:>17,.2f}"
        )
        
        all_lines.append("-" * 130)
        all_lines.append("")
    
    # Write report
    with open(output_file, 'w', encoding='utf-8') as f:
        for line in all_lines:
            f.write(f"{line}\n")
    
    # Clean up (equivalent to PROC DATASETS DELETE)
    print(f"\nReport written to: {output_file}")
    print(f"Total lines: {len(all_lines):,}")
    print("\n" + "=" * 60)
    print("✓ EIMAR202 Complete")

def generate_header(page_num, cat_type, rdate):
    """Generate page header (exact match to SAS NEWPAGE)"""
    return [
        f"PROGRAM-ID : EIMAR202{'':<30} P U B L I C   B A N K   B E R H A D{'':<25} PAGE NO.: {page_num}",
        f"{'OUTSTANDING LOANS CLASSIFIED AS NPL ISSUED FROM 1 JAN 98':^80} {cat_type:<13} {rdate:>10}",
        "",
        "BRH     NO         < 1 MTH        NO     1 TO < 2 MTH        NO     2 TO < 3 MTH       NO      3 TO < 4 MTH       NO      4 TO < 5 MTH",
        "        NO    5 TO < 6 MTH        NO     6 TO < 7 MTH        NO     7 TO < 8 MTH       NO      8 TO < 9 MTH       NO     9 TO < 10 MTH",
        "        NO  10 TO < 11 MTH        NO   11 TO < 12 MTH        NO   12 TO < 18 MTH       NO    18 TO < 24 MTH       NO    24 TO < 36 MTH",
        "        NO        > 36 MTH        NO          DEFICIT        NO   SUBTOTAL >=3MTH       NO   SUBTOTAL >=6MTH       NO             TOTAL",
        "-" * 130,
        ""
    ]

if __name__ == "__main__":
    main()
