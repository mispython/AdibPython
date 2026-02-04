#!/usr/bin/env python3
"""
EIBDISLM - Islamic Banking Statistics
Processes daily Islamic account balances and monthly summaries
"""

import duckdb
from pathlib import Path
from datetime import datetime

BASE_DIR = Path('.')
INPUT_DIR = BASE_DIR / 'data'
OUTPUT_DIR = BASE_DIR / 'output'
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

AGELIMIT = 12
MAXAGE = 18
AGEBELOW = 11

con = duckdb.connect()

reptdate = con.execute(f"SELECT reptdate FROM read_parquet('{INPUT_DIR}/deposit/reptdate.parquet')").fetchone()[0]
reptyear, reptmon, reptday = reptdate.year, reptdate.month, reptdate.day
rdate = reptdate.strftime('%d/%m/%Y')
zdate = int(reptdate.strftime('%y%m%d'))

print(f"Islamic Banking Statistics - {rdate}")

# ============================================================================
# SECTION 1: DAILY ISLAMIC BALANCE SUMMARY (DYIBU)
# ============================================================================

con.execute(f"""
    CREATE TEMP TABLE dyibu_raw AS
    SELECT branch, product, curbal, {zdate} reptdate
    FROM read_parquet('{INPUT_DIR}/deposit/current.parquet')
    WHERE openind NOT IN ('B','C','P')
    UNION ALL
    SELECT branch, product, curbal, {zdate} reptdate
    FROM read_parquet('{INPUT_DIR}/deposit/saving.parquet')
    WHERE openind NOT IN ('B','C','P')
""")

con.execute("""
    CREATE TEMP TABLE dyibu AS
    SELECT 
        branch,
        reptdate,
        SUM(CASE WHEN product IN (204,207,214,215) THEN curbal ELSE 0 END) sai,
        SUM(CASE WHEN product IN (204,207,214,215) THEN 1 ELSE 0 END) saino,
        SUM(CASE WHEN product = 214 THEN curbal ELSE 0 END) mbs,
        SUM(CASE WHEN product = 214 THEN 1 ELSE 0 END) mbsno,
        SUM(CASE WHEN product IN (60,61,63,64,70,71,93,94,160,161,162,163,164,166,169,66,67,168,167,182,183,184,73) 
                 AND curbal > 0 AND product NOT IN (96,97,61,161,63,163) THEN curbal ELSE 0 END) cai,
        SUM(CASE WHEN product IN (60,61,63,64,70,71,93,94,160,161,162,163,164,166,169,66,67,168,167,182,183,184,73) 
                 AND curbal > 0 AND product NOT IN (96,97,61,161,63,163) THEN 1 ELSE 0 END) caino,
        SUM(CASE WHEN product IN (96,97) AND curbal > 0 THEN curbal ELSE 0 END) ca96,
        SUM(CASE WHEN product IN (96,97) AND curbal > 0 THEN 1 ELSE 0 END) cai96,
        SUM(CASE WHEN product IN (61,161) AND curbal > 0 THEN curbal ELSE 0 END) caig,
        SUM(CASE WHEN product IN (61,161) AND curbal > 0 THEN 1 ELSE 0 END) caigno,
        SUM(CASE WHEN product IN (63,163) AND curbal > 0 THEN curbal ELSE 0 END) caih,
        SUM(CASE WHEN product IN (63,163) AND curbal > 0 THEN 1 ELSE 0 END) caihno
    FROM dyibu_raw
    GROUP BY branch, reptdate
""")

con.execute(f"COPY dyibu TO '{OUTPUT_DIR}/dyibu{reptmon:02d}.parquet'")
print(f"Section 1: DYIBU - {con.execute('SELECT COUNT(*) FROM dyibu').fetchone()[0]} branches")

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def calculate_age(bdate_str, reptdate, reptmon, reptday, reptyear):
    if not bdate_str or bdate_str == '0':
        return 0
    try:
        bdate = datetime.strptime(str(bdate_str)[:8], '%m%d%Y')
        age = reptyear - bdate.year
        if age == AGELIMIT:
            if (bdate.month == reptmon and bdate.day > reptday) or bdate.month > reptmon:
                age = AGEBELOW
        elif age == MAXAGE:
            if (bdate.month == reptmon and bdate.day > reptday) or bdate.month > reptmon:
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

def get_isa_range(curbal):
    if curbal < 501: return 500
    elif curbal < 2001: return 2000
    elif curbal < 5001: return 5000
    elif curbal < 10001: return 10000
    elif curbal < 30001: return 30000
    elif curbal < 50001: return 50000
    elif curbal < 75001: return 75000
    else: return 75001

def get_range_bucket(curbal, product):
    if product == 214:
        return get_isa_range(curbal)
    ranges = [(500, '< 500'), (1000, '< 1000'), (5000, '< 5000'), 
              (10000, '< 10000'), (50000, '< 50000'), (100000, '< 100000'),
              (500000, '< 500000'), (float('inf'), '>= 500000')]
    for limit, label in ranges:
        if curbal < limit:
            return label
    return '>= 500000'

# ============================================================================
# SECTION 2: PROCESS SAVINGS & CURRENT ACCOUNTS
# ============================================================================

con.execute(f"""
    CREATE TEMP TABLE accounts AS
    SELECT branch, product, curbal, avgamt, opendt, closedt, bdate,
           custcode, purpose, race, openind
    FROM read_parquet('{INPUT_DIR}/deposit/saving.parquet')
    WHERE openind NOT IN ('B','C','P') AND product NOT IN (297,298)
    UNION ALL
    SELECT branch, product, curbal, avgamt, opendt, closedt, bdate,
           custcode, purpose, race, openind
    FROM read_parquet('{INPUT_DIR}/deposit/current.parquet')
    WHERE openind NOT IN ('B','C','P') AND product NOT IN (297,298)
""")

accounts = con.execute("SELECT * FROM accounts").fetchall()
processed = []

for row in accounts:
    branch, product, curbal, avgamt, opendt, closedt, bdate, custcode, purpose, race, openind = row
    
    accytd = 0
    if opendt and opendt != 0 and (not closedt or closedt == 0):
        try:
            open_year = int(str(opendt)[:4]) if len(str(opendt)) >= 8 else 0
            if open_year == reptyear:
                accytd = 1
        except:
            pass
    
    age = calculate_age(str(bdate) if bdate else '0', reptdate, reptmon, reptday, reptyear)
    purpose = purpose if purpose and purpose != ' ' else '0'
    race = race if race and race != ' ' else '0'
    
    avgrnge = get_range_bucket(avgamt if avgamt else 0, 0)
    range_val = get_range_bucket(curbal if curbal else 0, product)
    
    processed.append({
        'product': product,
        'branch': branch,
        'curbal': curbal if curbal else 0,
        'avgamt': avgamt if avgamt else 0,
        'accytd': accytd,
        'age': age,
        'purpose': purpose,
        'race': race,
        'custcode': custcode,
        'avgrnge': avgrnge,
        'range': range_val,
        'reptdate': zdate
    })

con.execute("CREATE TEMP TABLE processed (product INT, branch INT, curbal DOUBLE, avgamt DOUBLE, accytd INT, age INT, purpose VARCHAR, race VARCHAR, custcode INT, avgrnge VARCHAR, range VARCHAR, reptdate INT)")
con.executemany("INSERT INTO processed VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", 
                [(p['product'], p['branch'], p['curbal'], p['avgamt'], p['accytd'], 
                  p['age'], p['purpose'], p['race'], p['custcode'], p['avgrnge'], 
                  p['range'], p['reptdate']) for p in processed])

# ============================================================================
# ALWSA: Products 204, 215 (Regular Savings)
# ============================================================================

con.execute("""
    CREATE TEMP TABLE alwsa AS
    SELECT 
        purpose, race, custcode, avgrnge, range deprange, product,
        COUNT(*) noacct,
        SUM(curbal) curbal,
        SUM(accytd) accytd,
        COUNT(CASE WHEN avgamt > 0 THEN 1 END) avgacct,
        SUM(avgamt) avgamt,
        reptdate
    FROM processed
    WHERE product IN (204, 215)
    GROUP BY purpose, race, custcode, avgrnge, range, product, reptdate
""")

con.execute(f"COPY alwsa TO '{OUTPUT_DIR}/awsa{reptmon:02d}.parquet'")
print(f"Section 2A: ALWSA - {con.execute('SELECT SUM(noacct) FROM alwsa').fetchone()[0]} accounts")

# ============================================================================
# ALWSB: Product 207 (Islamic Basic Savings)
# ============================================================================

con.execute("""
    CREATE TEMP TABLE alwsb AS
    SELECT 
        purpose, race, custcode, avgrnge, range deprange, product,
        COUNT(*) noacct,
        SUM(curbal) curbal,
        SUM(accytd) accytd,
        COUNT(CASE WHEN avgamt > 0 THEN 1 END) avgacct,
        SUM(avgamt) avgamt,
        reptdate
    FROM processed
    WHERE product = 207
    GROUP BY purpose, race, custcode, avgrnge, range, product, reptdate
""")

con.execute(f"COPY alwsb TO '{OUTPUT_DIR}/awsb{reptmon:02d}.parquet'")
print(f"Section 2B: ALWSB - {con.execute('SELECT SUM(noacct) FROM alwsb').fetchone()[0]} accounts")

# ============================================================================
# ALWSC: Product 214 (Mudharabah by Age/Race)
# ============================================================================

con.execute("""
    CREATE TEMP TABLE alwsc AS
    SELECT 
        product prodcd, range, race, age,
        COUNT(*) noacct,
        SUM(curbal) curbal,
        SUM(accytd) accytd,
        SUM(avgamt) avgamt,
        reptdate
    FROM processed
    WHERE product = 214
    GROUP BY product, range, race, age, reptdate
""")

con.execute(f"COPY alwsc TO '{OUTPUT_DIR}/awsc{reptmon:02d}.parquet'")
print(f"Section 2C: ALWSC - {con.execute('SELECT SUM(noacct) FROM alwsc').fetchone()[0]} accounts")

# ============================================================================
# MUDHA: Product 214 (Mudharabah by Purpose/Race/Customer)
# ============================================================================

con.execute("""
    CREATE TEMP TABLE mudha AS
    SELECT 
        purpose, race, custcode, avgrnge, range deprange, product,
        COUNT(*) noacct,
        SUM(curbal) curbal,
        SUM(accytd) accytd,
        SUM(avgamt) avgamt,
        reptdate
    FROM processed
    WHERE product = 214
    GROUP BY purpose, race, custcode, avgrnge, range, product, reptdate
""")

con.execute(f"COPY mudha TO '{OUTPUT_DIR}/mudh{reptmon:02d}.parquet'")
print(f"Section 2D: MUDHA - {con.execute('SELECT SUM(noacct) FROM mudha').fetchone()[0]} accounts")

# ============================================================================
# ALWCA: Products 93, 96 (Islamic Current Accounts)
# ============================================================================

con.execute("""
    CREATE TEMP TABLE alwca AS
    SELECT 
        purpose, race, custcode, avgrnge, range deprange, product,
        COUNT(*) noacct,
        SUM(curbal) curbal,
        SUM(accytd) accytd,
        COUNT(CASE WHEN avgamt > 0 THEN 1 END) avgacct,
        SUM(avgamt) avgamt,
        reptdate
    FROM processed
    WHERE product IN (93, 96) AND curbal > 0
    GROUP BY purpose, race, custcode, avgrnge, range, product, reptdate
""")

con.execute(f"COPY alwca TO '{OUTPUT_DIR}/awca{reptmon:02d}.parquet'")
print(f"Section 2E: ALWCA - {con.execute('SELECT SUM(noacct) FROM alwca').fetchone()[0]} accounts")

# ============================================================================
# ALWCB: Products 160,162,164,168,182,169 (Specific Purpose Current Accounts)
# ============================================================================

con.execute("""
    CREATE TEMP TABLE alwcb AS
    SELECT 
        purpose, race, custcode, avgrnge, range deprange, product,
        COUNT(*) noacct,
        SUM(curbal) curbal,
        SUM(accytd) accytd,
        COUNT(CASE WHEN avgamt > 0 THEN 1 END) avgacct,
        SUM(avgamt) avgamt,
        reptdate
    FROM processed
    WHERE product IN (160, 162, 164, 168, 182, 169)
      AND curbal > 0
      AND purpose IN ('1', '2', '4')
    GROUP BY purpose, race, custcode, avgrnge, range, product, reptdate
""")

con.execute(f"COPY alwcb TO '{OUTPUT_DIR}/awcb{reptmon:02d}.parquet'")
print(f"Section 2F: ALWCB - {con.execute('SELECT SUM(noacct) FROM alwcb').fetchone()[0]} accounts")

# ============================================================================
# SUMMARY
# ============================================================================

print("\n" + "="*80)
print("ISLAMIC BANKING STATISTICS SUMMARY")
print("="*80)
print(f"""
Date: {rdate}

Output Datasets:
1. DYIBU{reptmon:02d}  - Daily Islamic Balance Summary
   Fields: BRANCH, SAI, SAINO, MBS, MBSNO, CAI, CAINO, CA96, CAI96, CAIG, CAIGNO, CAIH, CAIHNO
   
2. AWSA{reptmon:02d}   - Products 204,215 (Regular Savings)
   Dimensions: PURPOSE × RACE × CUSTCD × AVGRNGE × DEPRANGE × PRODUCT
   
3. AWSB{reptmon:02d}   - Product 207 (Islamic Basic Savings)
   Dimensions: PURPOSE × RACE × CUSTCD × AVGRNGE × DEPRANGE × PRODUCT
   
4. AWSC{reptmon:02d}   - Product 214 (Mudharabah by Age/Race)
   Dimensions: PRODCD × RANGE × RACE × AGE
   
5. MUDH{reptmon:02d}   - Product 214 (Mudharabah by Purpose)
   Dimensions: PURPOSE × RACE × CUSTCD × AVGRNGE × DEPRANGE × PRODUCT
   
6. AWCA{reptmon:02d}   - Products 93,96 (Islamic Current Accounts)
   Dimensions: PURPOSE × RACE × CUSTCD × AVGRNGE × DEPRANGE × PRODUCT
   
7. AWCB{reptmon:02d}   - Products 160,162,164,168,182,169 (Purpose 1,2,4 only)
   Dimensions: PURPOSE × RACE × CUSTCD × AVGRNGE × DEPRANGE × PRODUCT

Product Categories:
- Savings: 204 (Regular), 207 (Basic), 214 (Mudharabah), 215 (Special)
- Current: 93,96 (Basic Islamic), 160-169,182 (Specific Purpose)

Metrics per Dataset:
- NOACCT: Number of accounts
- CURBAL: Total current balance
- ACCYTD: Accounts opened year-to-date
- AVGACCT: Count of accounts with average balance
- AVGAMT: Total average amount
""")

con.close()
print(f"\nCompleted: {OUTPUT_DIR}")
