"""
EIIDDEPE - Islamic Deposit Position Extract (Production Ready)

Includes:
- PBBLNFMT: Loan formats (ACE products)
- PBBDPFMT: Deposit formats (SAPROD, CAPROD, etc.)

Key Differences from EIBDDEPE:
- ONLY ISLAMIC (COSTCTR 3000-3999)
- ACE split logic: >5000 splits into SA+CA (5000 in CA, rest in SA)
- MIS outputs (PIBB Islamic)

Constants:
- AGELIMIT=12, MAXAGE=18, AGEBELOW=11

Outputs (MIS library):
- DYPOSN{mon}: Daily position summary
- DYDP: Daily deposit details
- DYMVNT{mon}: Significant movements
- DYIBU{mon}: Islamic banking balances by branch
- DYDDCR{mon}: Demand deposit credit movement
- DYBRDP{mon}: Branch summary
- DYDPS{mon}/DYACE{mon}: Movement by range
- SDRNGE{mon}: Savings profile (weekly/month-end)
- DDMV: Demand deposit movement (append)
"""

import polars as pl
from datetime import datetime, timedelta
import os

# Constants
AGELIMIT = 12
MAXAGE = 18
AGEBELOW = 11

# Directories
TEMP_DIR = 'data/temp/'
MIS_DIR = 'data/mis_pibb/'
DPTRBL_DIR = 'data/dptrbl/'

for d in [TEMP_DIR, MIS_DIR]:
    os.makedirs(d, exist_ok=True)

print("EIIDDEPE - Islamic Deposit Position Extract")
print("=" * 60)

# Read DPTRBL header
print("\nReading DPTRBL header...")
try:
    with open(f'{DPTRBL_DIR}DPTRBL.txt', 'r') as f:
        header = f.readline()
        tbdate_str = header[105:111]
    
    reptdate = datetime.strptime(tbdate_str, '%y%m%d').date()
    
    # Week determination
    day = reptdate.day
    if 1 <= day <= 8:
        nowk = '1'
    elif 9 <= day <= 15:
        nowk = '2'
    elif 16 <= day <= 22:
        nowk = '3'
    else:
        nowk = '4'
    
    reptdat3 = reptdate - timedelta(days=31)
    reptyear = int(reptdate.year)
    reptmon = f'{reptdate.month:02d}'
    reptday = f'{reptdate.day:02d}'
    rdate = reptdate.strftime('%d%m%y')
    
    print(f"Report Date: {reptdate.strftime('%d/%m/%Y')}")
    print(f"Week: {nowk}, Month: {reptmon}")
except Exception as e:
    print(f"Error reading DPTRBL: {e}")
    import sys
    sys.exit(1)

print("=" * 60)

# ACE products
ACE_PRODUCTS = [161, 162, 163, 150, 151, 152, 181]

# Range functions
def categorize_mvtdep(amount):
    if amount <= 5000: return 5000
    elif amount <= 10000: return 10000
    elif amount <= 30000: return 30000
    elif amount <= 50000: return 50000
    elif amount <= 75000: return 75000
    else: return 80000

def categorize_mvtace(amount):
    if amount <= 5000: return 5000
    elif amount <= 10000: return 10000
    elif amount <= 30000: return 30000
    elif amount <= 50000: return 50000
    elif amount <= 75000: return 75000
    elif amount <= 100000: return 100000
    else: return 200000

def categorize_sdrange(curbal):
    if curbal < 500: return 500
    elif curbal < 1000: return 1000
    elif curbal < 5000: return 5000
    elif curbal < 10000: return 10000
    elif curbal < 20000: return 20000
    elif curbal < 50000: return 50000
    elif curbal < 100000: return 100000
    elif curbal < 200000: return 200000
    else: return 200001

def categorize_ddmove(amount):
    if amount < 300000: return 300000
    elif amount < 500000: return 500000
    elif amount < 1000000: return 1000000
    elif amount < 1500000: return 1500000
    elif amount < 2000000: return 2000000
    elif amount < 3000000: return 3000000
    elif amount < 4000000: return 4000000
    elif amount < 5000000: return 5000000
    elif amount < 10000000: return 10000000
    else: return 10000001

def calculate_age(bdate, reptyear, reptmon_int, reptday_int):
    if not bdate or bdate == 0:
        return 0
    try:
        bdate_str = f'{bdate:06d}'
        byear = int(bdate_str[0:2]) + 1900 if int(bdate_str[0:2]) > 50 else int(bdate_str[0:2]) + 2000
        bmonth = int(bdate_str[2:4])
        bday = int(bdate_str[4:6])
        
        age = reptyear - byear
        
        if age == AGELIMIT:
            if (bmonth == reptmon_int and bday > reptday_int) or bmonth > reptmon_int:
                age = AGEBELOW
        elif age == MAXAGE:
            if (bmonth == reptmon_int and bday > reptday_int) or bmonth > reptmon_int:
                age = AGELIMIT
        elif age > MAXAGE:
            age = MAXAGE
        elif age < AGELIMIT:
            age = AGEBELOW
        else:
            age = AGELIMIT
        return age
    except:
        return 0

def days_in_month(year, month):
    if month == 2:
        return 29 if year % 4 == 0 else 28
    elif month in [4, 6, 9, 11]:
        return 30
    else:
        return 31

# Initialize
branch_data = {
    'TOTSAVG': 0, 'TOTSAVGI': 0, 'TOTDMND': 0, 'TOTDMNDI': 0,
    'TOTVOSF': 0, 'TOTVOSC': 0, 'OVDVOSF': 0, 'OVDVOSC': 0,
    'TOTOVFT': 0, 'TOTFD': 0, 'TOTFDI': 0, 'ACESA': 0,
    'ACECA': 0, 'TOTMBSA': 0
}

dyibu_branches = {i: {
    'SAI': 0, 'CAI': 0, 'CAIG': 0, 'CAIH': 0, 'MBS': 0,
    'SNO': 0, 'CNO': 0, 'CGNO': 0, 'CHNO': 0, 'MBSNO': 0
} for i in range(1, 1000)}

dydp_records = []
ddmv_records = []
savg_records = []

print("\nProcessing DPTRBL (Islamic ONLY: COSTCTR 3000-3999)...")

try:
    with open(f'{DPTRBL_DIR}DPTRBL.txt', 'r') as f:
        for line_num, line in enumerate(f):
            if line_num == 0: continue
            if len(line) < 779: continue
            
            try:
                bankno = int(line[2:5])
                reptno = int(line[23:27])
                fmtcode = int(line[26:29])
            except:
                continue
            
            if not (bankno == 33 and reptno == 1001 and fmtcode == 1):
                continue
            
            try:
                branch = int(line[105:110])
                acctno = int(line[109:116])
                name = line[115:131].strip()
                debit = float(line[142:151])
                credit = float(line[150:158])
                closedt = int(line[157:164]) if line[157:164].strip() else 0
                opendt = int(line[163:170]) if line[163:170].strip() else 0
                custcode = int(line[175:179]) if line[175:179].strip() else 0
                purpose = line[285:287].strip()
                openind = line[337:339].strip()
                race = line[398:400].strip()
                product = int(line[396:399]) if line[396:399].strip() else 0
                deptype = line[430:432].strip()
                int1 = float(line[431:438])
                curbal = float(line[465:473])
                int2 = float(line[515:526])
                apprlimt = float(line[565:572])
                bdate = int(line[716:723]) if line[716:723].strip() else 0
                costctr = int(line[829:834]) if line[829:834].strip() else 0
            except:
                continue
            
            # ONLY ISLAMIC (3000-3999) - KEY FILTER
            if not (3000 <= costctr <= 3999):
                continue
            
            if openind in ['B', 'C', 'P'] or product in [297, 298]:
                continue
            
            movement = credit - debit
            mvrange = categorize_mvtace(abs(movement)) if product in ACE_PRODUCTS else categorize_mvtdep(abs(movement))
            
            accytd = 0
            if opendt != 0 and closedt == 0:
                try:
                    opendt_str = f'{opendt:06d}'
                    opendt_year = int(opendt_str[0:2]) + 1900 if int(opendt_str[0:2]) > 50 else int(opendt_str[0:2]) + 2000
                    if opendt_year == reptyear:
                        accytd = 1
                except:
                    pass
            
            if deptype == 'S':
                prodcd = '42320' if product in [210, 211, 214] else '42120'
            elif deptype in ['D', 'N']:
                if product in [150, 151, 152, 181]:
                    prodcd = '42110'
                elif product in [61, 161]:
                    prodcd = '42310'
                elif product in [63, 163]:
                    prodcd = '42180'
                else:
                    prodcd = '42180'
            elif deptype == 'C':
                prodcd = '42132' if product in [317, 318] else '42130'
            else:
                prodcd = ''
            
            # Process by DEPTYPE
            if deptype == 'S' and curbal >= 0:
                if prodcd == '42120':
                    branch_data['TOTSAVG'] += curbal
                elif prodcd == '42320':
                    branch_data['TOTSAVGI'] += curbal
                    dyibu_branches[branch]['SAI'] += curbal
                    dyibu_branches[branch]['SNO'] += 1
                    if product == 214:
                        branch_data['TOTMBSA'] += curbal
                        dyibu_branches[branch]['MBS'] += curbal
                        dyibu_branches[branch]['MBSNO'] += 1
                
                dydp_records.append({
                    'REPTDATE': reptdate, 'BRANCH': branch, 'ACCTNO': acctno,
                    'NAME': name, 'DEBIT': debit, 'CREDIT': credit,
                    'DEPTYPE': deptype, 'DPTYPE': 'S', 'PRODUCT': product,
                    'PRODCD': prodcd, 'CURBAL': curbal, 'DYDPBAL': curbal,
                    'APPRLIMT': apprlimt, 'ACCYTD': accytd, 'OPENDT': opendt,
                    'MOVEMENT': movement, 'MVRANGE': mvrange
                })
            
            elif deptype in ['D', 'N']:
                if curbal >= 0:
                    # ACE SPLIT LOGIC (>5000)
                    if prodcd == '42110' and product in [150, 151, 152, 181] and curbal > 5000:
                        sabal = curbal - 5000
                        cabal = 5000
                        
                        branch_data['ACESA'] += sabal
                        branch_data['TOTSAVG'] += sabal
                        branch_data['TOTDMND'] += cabal
                        branch_data['ACECA'] += cabal
                        
                        dydp_records.append({
                            'REPTDATE': reptdate, 'BRANCH': branch, 'ACCTNO': acctno,
                            'NAME': name, 'DEBIT': debit, 'CREDIT': credit,
                            'DEPTYPE': deptype, 'DPTYPE': 'S', 'PRODUCT': product,
                            'PRODCD': prodcd, 'CURBAL': curbal, 'DYDPBAL': sabal,
                            'APPRLIMT': apprlimt, 'ACCYTD': accytd, 'OPENDT': opendt,
                            'MOVEMENT': movement, 'MVRANGE': mvrange
                        })
                        dydp_records.append({
                            'REPTDATE': reptdate, 'BRANCH': branch, 'ACCTNO': acctno,
                            'NAME': name, 'DEBIT': debit, 'CREDIT': credit,
                            'DEPTYPE': deptype, 'DPTYPE': 'D', 'PRODUCT': product,
                            'PRODCD': prodcd, 'CURBAL': curbal, 'DYDPBAL': cabal,
                            'APPRLIMT': apprlimt, 'ACCYTD': accytd, 'OPENDT': opendt,
                            'MOVEMENT': movement, 'MVRANGE': mvrange
                        })
                    else:
                        if prodcd == '42110':
                            if product in ACE_PRODUCTS:
                                branch_data['ACECA'] += curbal
                            branch_data['TOTDMND'] += curbal
                        elif prodcd == '42180':
                            if product in [63, 163]:
                                branch_data['TOTDMNDI'] += curbal
                                dyibu_branches[branch]['CAI'] += curbal
                                dyibu_branches[branch]['CNO'] += 1
                                dyibu_branches[branch]['CAIH'] += curbal
                                dyibu_branches[branch]['CHNO'] += 1
                            else:
                                branch_data['TOTDMND'] += curbal
                        elif prodcd == '42310':
                            branch_data['TOTDMNDI'] += curbal
                            dyibu_branches[branch]['CAI'] += curbal
                            dyibu_branches[branch]['CNO'] += 1
                            if product in [61, 161]:
                                dyibu_branches[branch]['CAIG'] += curbal
                                dyibu_branches[branch]['CGNO'] += 1
                        
                        dydp_records.append({
                            'REPTDATE': reptdate, 'BRANCH': branch, 'ACCTNO': acctno,
                            'NAME': name, 'DEBIT': debit, 'CREDIT': credit,
                            'DEPTYPE': deptype, 'DPTYPE': 'D', 'PRODUCT': product,
                            'PRODCD': prodcd, 'CURBAL': curbal, 'DYDPBAL': curbal,
                            'APPRLIMT': apprlimt, 'ACCYTD': accytd, 'OPENDT': opendt,
                            'MOVEMENT': movement, 'MVRANGE': mvrange
                        })
                else:
                    # Overdraft
                    ddmv_records.append({
                        'REPTDATE': reptdate, 'BRANCH': branch, 'ACCTNO': acctno,
                        'NAME': name, 'DEBIT': debit, 'CREDIT': credit,
                        'DEPTYPE': deptype, 'PRODCD': prodcd, 'PRODUCT': product,
                        'CURBAL': curbal
                    })
                    
                    if prodcd == '42310':
                        dyibu_branches[branch]['CAI'] += curbal
                        dyibu_branches[branch]['CNO'] += 1
                        if product in [61, 161]:
                            dyibu_branches[branch]['CAIG'] += curbal
                            dyibu_branches[branch]['CGNO'] += 1
                    elif prodcd == '42180' and product in [63, 163]:
                        dyibu_branches[branch]['CAI'] += curbal
                        dyibu_branches[branch]['CNO'] += 1
                        dyibu_branches[branch]['CAIH'] += curbal
                        dyibu_branches[branch]['CHNO'] += 1
                    
                    branch_data['TOTOVFT'] += abs(curbal)
            
            elif deptype == 'C':
                if prodcd == '42130':
                    branch_data['TOTFD'] += curbal
                elif prodcd == '42132':
                    branch_data['TOTFDI'] += curbal
            
            # SAVG profile (weekly/month-end)
            if deptype == 'S' and curbal >= 0:
                mth = days_in_month(reptyear, int(reptmon))
                mthend = 'Y' if reptday == str(mth) else 'N'
                
                if reptday in ['08', '15', '22'] or mthend == 'Y':
                    age = calculate_age(bdate, reptyear, int(reptmon), int(reptday))
                    range_val = categorize_sdrange(curbal)
                    
                    savg_records.append({
                        'ACCTNO': acctno, 'BRANCH': branch, 'PRODUCT': product,
                        'DEPTYPE': deptype, 'NAME': name, 'OPENIND': openind,
                        'RANGE': range_val, 'RACE': race, 'BDATE': bdate,
                        'CURBAL': curbal, 'PURPOSE': purpose, 'PRODCD': prodcd,
                        'AGE': age, 'ACCYTD': accytd, 'OPENDT': opendt
                    })

except Exception as e:
    print(f"Error: {e}")
    import sys
    sys.exit(1)

print(f"  DYDP: {len(dydp_records):,}")
print(f"  DDMV: {len(ddmv_records):,}")
print(f"  SAVG: {len(savg_records):,}")

# Create DataFrames
df_dyposn = pl.DataFrame([{'REPTDATE': reptdate, **branch_data}])
df_dydp = pl.DataFrame(dydp_records) if dydp_records else pl.DataFrame([])
df_ddmv = pl.DataFrame(ddmv_records) if ddmv_records else pl.DataFrame([])
df_savg = pl.DataFrame(savg_records) if savg_records else pl.DataFrame([])

# DYIBU
dyibu_recs = []
for branch in range(1, 1000):
    data = dyibu_branches[branch]
    if any(data[k] != 0 for k in ['SAI', 'CAI', 'CAIG', 'CAIH', 'MBS']):
        dyibu_recs.append({'REPTDATE': reptdate, 'BRANCH': branch, **data})
df_dyibu = pl.DataFrame(dyibu_recs) if dyibu_recs else pl.DataFrame([])

# DYMVNT
if len(df_dydp) > 0:
    df_dymvnt = df_dydp.filter(
        ((pl.col('DEPTYPE') == 'S') & (pl.col('MOVEMENT').abs() >= 50000)) |
        ((pl.col('DEPTYPE').is_in(['D', 'N'])) & (pl.col('DPTYPE') == 'D') &
         (((pl.col('MOVEMENT').abs() >= 100000) & pl.col('PRODUCT').is_in(ACE_PRODUCTS)) |
          (pl.col('MOVEMENT').abs() >= 1000000)))
    ).select(['REPTDATE', 'BRANCH', 'ACCTNO', 'NAME', 'DEBIT', 'CREDIT', 'DEPTYPE', 'PRODUCT', 'CURBAL', 'APPRLIMT'])
else:
    df_dymvnt = pl.DataFrame([])

# Summaries
if len(df_dydp) > 0:
    df_dybrdp = df_dydp.filter(
        ~pl.col('PRODCD').is_in(['42320', '42310']) &
        ~pl.col('PRODUCT').is_in([104, 105])
    ).group_by(['BRANCH', 'DPTYPE', 'REPTDATE']).agg([pl.col('DYDPBAL').sum().alias('BALANCE')])
    
    df_dyddcr_temp = df_dydp.filter(
        (pl.col('DPTYPE') == 'D') &
        ~pl.col('PRODUCT').is_in(ACE_PRODUCTS) &
        (pl.col('PRODCD') != '42310') &
        (pl.col('CURBAL') >= 0)
    ).with_columns([
        (pl.col('CURBAL') - pl.col('MOVEMENT')).alias('PREBAL')
    ]).with_columns([
        pl.when(pl.col('PREBAL') < 0).then(pl.col('CURBAL')).otherwise(pl.col('MOVEMENT')).alias('MOVEMENT_ADJ')
    ])
    
    ranges = []
    for row in df_dyddcr_temp.iter_rows(named=True):
        ranges.append(categorize_ddmove(abs(row['MOVEMENT_ADJ'])))
    
    df_dyddcr = df_dyddcr_temp.with_columns([
        pl.Series('RANGE', ranges)
    ]).group_by(['REPTDATE', 'RANGE']).agg([pl.col('MOVEMENT_ADJ').sum().alias('MOVEMENT')])
    
    df_dydps = df_dydp.filter(
        (pl.col('DPTYPE') == 'S') &
        ~pl.col('PRODUCT').is_in(ACE_PRODUCTS) &
        (pl.col('PRODCD') == '42120')
    ).group_by(['REPTDATE', 'MVRANGE']).agg([pl.col('MOVEMENT').sum()])
    
    df_dyace = df_dydp.filter(
        (pl.col('DPTYPE') == 'D') &
        pl.col('PRODUCT').is_in(ACE_PRODUCTS)
    ).group_by(['REPTDATE', 'MVRANGE']).agg([pl.col('MOVEMENT').sum()])
else:
    df_dybrdp = df_dyddcr = df_dydps = df_dyace = pl.DataFrame([])

# SDRNGE
if len(df_savg) > 0:
    df_sdrnge = df_savg.group_by(['BRANCH', 'RACE', 'PRODCD', 'PRODUCT', 'RANGE', 'AGE']).agg([
        pl.len().alias('NOACCT'),
        pl.col('CURBAL').sum(),
        pl.col('ACCYTD').sum()
    ])
else:
    df_sdrnge = pl.DataFrame([])

# Save
print("\nSaving outputs...")

df_dydp.write_parquet(f'{TEMP_DIR}DYDP.parquet')

# Monthly append
if reptday == '01':
    print("  Day 01: Creating new")
    df_dyposn.write_parquet(f'{MIS_DIR}DYPOSN{reptmon}.parquet')
    df_dymvnt.write_parquet(f'{MIS_DIR}DYMVNT{reptmon}.parquet')
    df_dyibu.write_parquet(f'{MIS_DIR}DYIBU{reptmon}.parquet')
    df_dyddcr.write_parquet(f'{MIS_DIR}DYDDCR{reptmon}.parquet')
    df_dybrdp.write_parquet(f'{MIS_DIR}DYBRDP{reptmon}.parquet')
    df_dydps.write_parquet(f'{MIS_DIR}DYDPS{reptmon}.parquet')
    df_dyace.write_parquet(f'{MIS_DIR}DYACE{reptmon}.parquet')
else:
    print(f"  Day {reptday}: Appending")
    for name, df in [('DYPOSN', df_dyposn), ('DYMVNT', df_dymvnt), ('DYIBU', df_dyibu),
                     ('DYDDCR', df_dyddcr), ('DYBRDP', df_dybrdp), ('DYDPS', df_dydps), ('DYACE', df_dyace)]:
        try:
            df_old = pl.read_parquet(f'{MIS_DIR}{name}{reptmon}.parquet').filter(pl.col('REPTDATE') != reptdate)
            pl.concat([df_old, df]).write_parquet(f'{MIS_DIR}{name}{reptmon}.parquet')
        except:
            df.write_parquet(f'{MIS_DIR}{name}{reptmon}.parquet')

if len(df_sdrnge) > 0:
    df_sdrnge.write_parquet(f'{MIS_DIR}SDRNGE{reptmon}.parquet')

try:
    df_ddmv_old = pl.read_parquet(f'{MIS_DIR}DDMV.parquet')
    pl.concat([df_ddmv_old, df_ddmv]).write_parquet(f'{MIS_DIR}DDMV.parquet')
except:
    df_ddmv.write_parquet(f'{MIS_DIR}DDMV.parquet')

print(f"\n{'='*60}")
print(f"✓ EIIDDEPE Complete!")
print(f"{'='*60}")
print(f"\nKey: ACE Split (CURBAL>5000)")
print(f"  SA: Balance-5000, CA: 5000")
print(f"\nOutputs (Islamic):")
print(f"  DYDP: {len(df_dydp):,}")
print(f"  DYMVNT: {len(df_dymvnt):,}")
print(f"  DYIBU: {len(df_dyibu):,}")
