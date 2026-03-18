"""
EIMAR103 - Loans in Arrears Classified as NPL
Outstanding loans classified as Non-Performing Loans (NPL) for HPCCD
"""

import polars as pl
from datetime import datetime
from calendar import monthrange
import os

BNM_DIR = 'data/bnm/'
PBB_DIR = 'data/pbb/'
OUTPUT_DIR = 'data/output/'

os.makedirs(OUTPUT_DIR, exist_ok=True)

HPD = [110, 115, 120, 125, 140, 145, 150, 155, 160, 165, 170, 175, 180, 185]

print("=" * 80)
print("EIMAR103 - OUTSTANDING LOANS CLASSIFIED AS NPL")
print("PUBLIC BANK BERHAD")
print("=" * 80)

try:
    df_reptdate = pl.read_parquet(f'{BNM_DIR}REPTDATE.parquet')
    reptdate = df_reptdate['REPTDATE'][0]
    
    if isinstance(reptdate, str):
        reptdate = datetime.strptime(reptdate, '%Y-%m-%d')
    
    rdate = reptdate.strftime('%d/%m/%y')
    reptyear = str(reptdate.year)
    reptmon = str(reptdate.month).zfill(2)
    reptday = str(reptdate.day).zfill(2)
    
    print(f"\nReport Date: {reptdate.strftime('%d %B %Y')} ({rdate})")

except Exception as e:
    print(f"✗ ERROR reading REPTDATE: {e}")
    import sys
    sys.exit(1)

print("\n" + "=" * 80)

print("\n1. Reading loan data...")
try:
    df_loan = pl.read_parquet(f'{BNM_DIR}LOANTEMP.parquet')
    df_loan = df_loan.sort(['BRANCH', 'ARREAR2'])
    print(f"  ✓ LOANTEMP: {len(df_loan):,} records")
except Exception as e:
    print(f"  ✗ LOANTEMP: {e}")
    import sys
    sys.exit(1)

print("\n2. Reading branch data...")
try:
    df_branch = pl.read_parquet(f'{PBB_DIR}BRANCH.parquet')
    df_branch = df_branch.select(['BRANCH', 'BRHCODE'])
    print(f"  ✓ BRANCH: {len(df_branch):,} records")
except Exception as e:
    print(f"  ✗ BRANCH: {e}")
    df_branch = pl.DataFrame({'BRANCH': [], 'BRHCODE': []})

print("\n3. Processing NPL classification...")

df_loan = df_loan.with_columns([
    pl.col('CENSUS').cast(pl.Utf8).str.slice(-1).alias('CENSUS9')
])

df_loan = df_loan.filter(
    (pl.col('BALANCE') > 0) &
    (pl.col('BORSTAT') != 'Z')
)

df_loan = df_loan.filter(
    (pl.col('ARREAR2') > 3) |
    (pl.col('BORSTAT') == 'R') |
    (pl.col('BORSTAT') == 'I') |
    (pl.col('BORSTAT') == 'F') |
    (pl.col('CENSUS9') == '9') |
    (pl.col('USER5') == 'N')
)

df_records = []

df_hpdc = df_loan.filter(
    (pl.col('PRODUCT').is_in([380, 381, 700, 705, 720, 725, 15, 20, 71, 72])) &
    ((pl.col('BORSTAT').is_in(['R', 'I', 'F'])) |
     (pl.col('ARREAR2') > 3) |
     (pl.col('USER5') == 'N'))
)
df_hpdc = df_hpdc.with_columns([
    pl.lit('A').alias('CAT'),
    pl.lit('(HPD-C)').alias('TYPE')
])
df_records.append(df_hpdc)

df_hp380 = df_loan.filter(
    (pl.col('PRODUCT').is_in([380, 381])) &
    ((pl.col('BORSTAT').is_in(['R', 'I', 'F'])) |
     (pl.col('ARREAR2') > 3) |
     (pl.col('USER5') == 'N'))
)
df_hp380 = df_hp380.with_columns([
    pl.lit('B').alias('CAT'),
    pl.lit('(HP 380/381)').alias('TYPE')
])
df_records.append(df_hp380)

df_aitab = df_loan.filter(
    (pl.col('PRODUCT').is_in([103, 104, 107, 108, 128, 130, 131, 132])) &
    ((pl.col('BORSTAT').is_in(['R', 'I', 'F'])) |
     (pl.col('ARREAR2') > 3) |
     (pl.col('USER5') == 'N'))
)
df_aitab = df_aitab.with_columns([
    pl.lit('C').alias('CAT'),
    pl.lit('(AITAB)').alias('TYPE')
])
df_records.append(df_aitab)

df_hpd = df_loan.filter(
    (pl.col('PRODUCT').is_in(HPD)) &
    ((pl.col('BORSTAT').is_in(['R', 'I', 'F'])) |
     (pl.col('ARREAR2') > 3) |
     (pl.col('USER5') == 'N'))
)
df_hpd = df_hpd.with_columns([
    pl.lit('D').alias('CAT'),
    pl.lit('(-HPD-)').alias('TYPE')
])
df_records.append(df_hpd)

df_loantemp = pl.concat(df_records) if df_records else pl.DataFrame()

if len(df_loantemp) == 0:
    print("  ⚠ No NPL records found")
    import sys
    sys.exit(0)

df_loantemp = df_loantemp.sort(['BRANCH'])

df_loan1 = df_loantemp.join(df_branch, on='BRANCH', how='left')
df_loan1 = df_loan1.sort(['CAT', 'BRANCH', 'ARREAR2'])

print(f"  ✓ NPL records: {len(df_loan1):,}")
print(f"    - (HPD-C): {len(df_loan1.filter(pl.col('CAT') == 'A')):,}")
print(f"    - (HP 380/381): {len(df_loan1.filter(pl.col('CAT') == 'B')):,}")
print(f"    - (AITAB): {len(df_loan1.filter(pl.col('CAT') == 'C')):,}")
print(f"    - (-HPD-): {len(df_loan1.filter(pl.col('CAT') == 'D')):,}")

print("\n4. Generating reports...")

def generate_report(df_data, progid, output_file):
    """Generate NPL report by category"""
    
    if len(df_data) == 0:
        return
    
    categories = df_data['CAT'].unique().sort().to_list()
    
    with open(output_file, 'w') as f:
        for cat in categories:
            df_cat = df_data.filter(pl.col('CAT') == cat)
            type_desc = df_cat['TYPE'][0]
            
            f.write(f"PROGRAM-ID : {progid}\n")
            f.write(f"PUBLIC BANK BERHAD\n")
            f.write(f"OUTSTANDING LOANS CLASSIFIED AS NPL {type_desc}\n")
            f.write(f"{rdate}\n\n")
            
            f.write("BRH     NO         < 1 MTH         NO     1 TO < 2 MTH         ")
            f.write("NO     2 TO < 3 MTH        NO      3 TO < 4 MTH       ")
            f.write("NO      4 TO < 5 MTH\n")
            f.write("        NO    5 TO < 6 MTH         NO     6 TO < 7 MTH         ")
            f.write("NO     7 TO < 8 MTH        NO      8 TO < 9 MTH       ")
            f.write("NO     9 TO < 12 MTH\n")
            f.write("        NO  12 TO < 18 MTH         NO   18 TO < 24 MTH         ")
            f.write("NO   24 TO < 36 MTH        NO          > 36 MTH       ")
            f.write("NO   SUBTOTAL >=3MTH\n")
            f.write("        NO   SUBTOTAL >=6MTH       NO             TOTAL\n")
            f.write("-" * 131 + "\n")
            
            branches = df_cat['BRANCH'].unique().sort().to_list()
            
            totamt = [0.0] * 14
            totacc = [0] * 14
            
            for branch in branches:
                df_branch_data = df_cat.filter(pl.col('BRANCH') == branch)
                brhcode = df_branch_data['BRHCODE'][0] if 'BRHCODE' in df_branch_data.columns else ''
                
                brhamt = [0.0] * 14
                noacc = [0] * 14
                
                for row in df_branch_data.iter_rows(named=True):
                    arrear = min(row['ARREAR2'], 14) - 1
                    if 0 <= arrear < 14:
                        brhamt[arrear] += row['BALANCE']
                        noacc[arrear] += 1
                
                for i in range(14):
                    totamt[i] += brhamt[i]
                    totacc[i] += noacc[i]
                
                subbrh = sum(brhamt[3:14])
                subbr2 = sum(brhamt[6:14])
                subacc = sum(noacc[3:14])
                subac2 = sum(noacc[6:14])
                totbrh = sum(brhamt)
                sotacc = sum(noacc)
                
                f.write(f"{branch:03d}   ")
                f.write(f"{noacc[0]:7,}  {brhamt[0]:16,.2f}   ")
                f.write(f"{noacc[1]:7,}  {brhamt[1]:15,.2f}   ")
                f.write(f"{noacc[2]:7,}  {brhamt[2]:15,.2f}   ")
                f.write(f"{noacc[3]:8,}  {brhamt[3]:17,.2f}   ")
                f.write(f"{noacc[4]:8,}  {brhamt[4]:17,.2f}\n")
                
                f.write(f"{brhcode:3s}   ")
                f.write(f"{noacc[5]:7,}  {brhamt[5]:16,.2f}   ")
                f.write(f"{noacc[6]:7,}  {brhamt[6]:15,.2f}   ")
                f.write(f"{noacc[7]:7,}  {brhamt[7]:15,.2f}   ")
                f.write(f"{noacc[8]:8,}  {brhamt[8]:17,.2f}   ")
                f.write(f"{noacc[9]:8,}  {brhamt[9]:17,.2f}\n")
                
                f.write(f"       ")
                f.write(f"{noacc[10]:7,}  {brhamt[10]:16,.2f}   ")
                f.write(f"{noacc[11]:7,}  {brhamt[11]:15,.2f}   ")
                f.write(f"{noacc[12]:7,}  {brhamt[12]:15,.2f}   ")
                f.write(f"{noacc[13]:8,}  {brhamt[13]:17,.2f}   ")
                f.write(f"{subacc:8,}  {subbrh:17,.2f}\n")
                
                f.write(f"       ")
                f.write(f"                                                              ")
                f.write(f"{subac2:8,}  {subbr2:17,.2f}   ")
                f.write(f"{sotacc:8,}  {totbrh:17,.2f}\n")
            
            sgtotbrh = sum(totamt[3:14])
            sgtotbr2 = sum(totamt[6:14])
            sgtotacc = sum(totacc[3:14])
            sgtotac2 = sum(totacc[6:14])
            gtotbrh = sum(totamt)
            gtotacc = sum(totacc)
            
            f.write("-" * 131 + "\n")
            f.write(f"TOT   ")
            f.write(f"{totacc[0]:7,}  {totamt[0]:16,.2f}   ")
            f.write(f"{totacc[1]:7,}  {totamt[1]:15,.2f}   ")
            f.write(f"{totacc[2]:7,}  {totamt[2]:15,.2f}   ")
            f.write(f"{totacc[3]:8,}  {totamt[3]:17,.2f}   ")
            f.write(f"{totacc[4]:8,}  {totamt[4]:17,.2f}\n")
            
            f.write(f"       ")
            f.write(f"{totacc[5]:7,}  {totamt[5]:16,.2f}   ")
            f.write(f"{totacc[6]:7,}  {totamt[6]:15,.2f}   ")
            f.write(f"{totacc[7]:7,}  {totamt[7]:15,.2f}   ")
            f.write(f"{totacc[8]:8,}  {totamt[8]:17,.2f}   ")
            f.write(f"{totacc[9]:8,}  {totamt[9]:17,.2f}\n")
            
            f.write(f"       ")
            f.write(f"{totacc[10]:7,}  {totamt[10]:16,.2f}   ")
            f.write(f"{totacc[11]:7,}  {totamt[11]:15,.2f}   ")
            f.write(f"{totacc[12]:7,}  {totamt[12]:15,.2f}   ")
            f.write(f"{totacc[13]:8,}  {totamt[13]:17,.2f}   ")
            f.write(f"{sgtotacc:8,}  {sgtotbrh:17,.2f}\n")
            
            f.write(f"       ")
            f.write(f"                                                              ")
            f.write(f"{sgtotac2:8,}  {sgtotbr2:17,.2f}   ")
            f.write(f"{gtotacc:8,}  {gtotbrh:17,.2f}\n")
            
            f.write("-" * 131 + "\n\n")

generate_report(df_loan1, 'EIMAR103-A', f'{OUTPUT_DIR}NPL_REPORT_A.txt')
print(f"  ✓ Generated: NPL_REPORT_A.txt")

df_exclude = df_loan1.filter(
    ~pl.col('TYPE').is_in(['(HPD-C)', '(-HPD-)']) &
    ~pl.col('BORSTAT').is_in(['F', 'I', 'R']) &
    (pl.col('PRODUCT').is_in(HPD + [15, 20, 71, 72]))
)

if len(df_exclude) > 0:
    generate_report(df_exclude, 'EIMAR103-B', f'{OUTPUT_DIR}NPL_REPORT_B.txt')
    print(f"  ✓ Generated: NPL_REPORT_B.txt")

print(f"\n{'='*80}")
print("✓ EIMAR103 Complete")
print(f"{'='*80}")
