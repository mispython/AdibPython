import duckdb
from pathlib import Path
from datetime import datetime

# ============================================================================
# CONFIGURATION
# ============================================================================

BASE_DIR = Path('.')
INPUT_DIR = BASE_DIR / 'data'
OUTPUT_DIR = BASE_DIR / 'output'
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Collateral type mapping (PBBLNFMT)
COLL_TYPE_MAP = {
    # Type 1: PBB OWN FIXED DEPOSIT (0%)
    ('007','012','013','014','021','024','048','049'): 1,
    # Type 2: DISCOUNT HOUSE/CAGAMAS (10%)
    ('017','026','029'): 2,
    # Type 3: FIN INST FIXED DEPOSIT/GUARANTEE (20%)
    ('006','011','016','030','018','027','003'): 3,
    # Type 4: STATUTORY BODIES (20%)
    ('025',): 4,
    # Type 5: FIRST CHARGE (50%)
    ('050',): 5,
    # Type 6: SHARES/UNIT TRUSTS (100%)
    ('015','008','042'): 6,
    # Type 7: OTHERS (100%)
}

TYPE_LABELS = {
    1: 'PBB OWN FIXED DEPOSIT, 30308 (0%)',
    2: 'DISCOUNT HOUSE/CAGAMAS, 30314 (10%)',
    3: 'FIN INST FIXED DEPOSIT/GUARANTEE, 30332 (20%)',
    4: 'STATUTORY BODIES, 30336 (20%)',
    5: 'FIRST CHARGE, 30342 (50%)',
    6: 'SHARES/UNIT TRUSTS, 30360 (100%)',
    7: 'OTHERS, 30360 (100%)'
}

con = duckdb.connect()

# ============================================================================
# GET REPORTING DATE
# ============================================================================

reptdate = con.execute(f"SELECT reptdate FROM read_parquet('{INPUT_DIR}/bnm/reptdate.parquet')").fetchone()[0]
day = reptdate.day
nowk = {8:'1', 15:'2', 22:'3'}.get(day, '4')

reptyear, reptmon, reptday = f"{reptdate.year % 100:02d}", f"{reptdate.month:02d}", f"{day:02d}"
rdate = reptdate.strftime('%d/%m/%Y')

print(f"Report Date: {rdate}, Week: {nowk}")

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def get_collateral_type(liabcode):
    """Map liability code to collateral type"""
    if not liabcode:
        return 7
    for codes, typ in COLL_TYPE_MAP.items():
        if liabcode in codes:
            return typ
    return 7

def calc_remaining_months(exprdate, reptdate):
    """Calculate remaining months from report date to expiry"""
    remy = exprdate.year - reptdate.year
    remm = exprdate.month - reptdate.month
    
    rpdays = 31 if reptdate.month in [1,3,5,7,8,10,12] else \
             30 if reptdate.month in [4,6,9,11] else \
             29 if reptdate.year % 4 == 0 else 28
    
    mdday = min(exprdate.day, rpdays)
    remd = (mdday - reptdate.day) / rpdays
    
    return remy * 12 + remm + remd

# ============================================================================
# LOAD BTR DAILY DETAIL DATA
# ============================================================================

con.execute(f"""
    CREATE TEMP TABLE btradix AS
    SELECT branch, acctno, liabcode, outstand balance,
           liabcode prodcd
    FROM read_parquet('{INPUT_DIR}/bnmdaily/btdtl{reptyear}{reptmon}{reptday}.parquet')
    WHERE acctno BETWEEN 2500000000 AND 2599999999
      AND dirctind = 'I'
      AND liabcode NOT IN ('LCE','SGE','BGE','SBE','FBE')
""")

# Aggregate by account and product
con.execute("""
    CREATE TEMP TABLE btradi AS
    SELECT branch, acctno, prodcd, SUM(balance) balance
    FROM btradix
    GROUP BY branch, acctno, prodcd
""")

# ============================================================================
# LOAD BTR MASTER DATA
# ============================================================================

con.execute(f"""
    CREATE TEMP TABLE btradm AS
    SELECT DISTINCT acctno, icurbal, dbalance, issdte, exprdate, origmt
    FROM read_parquet('{INPUT_DIR}/bnm/btmast{reptmon}{nowk}.parquet')
    WHERE subacct = 'OV'
""")

# Merge master and detail
con.execute("""
    CREATE TEMP TABLE btrade AS
    SELECT m.*, d.branch, d.prodcd, d.balance
    FROM btradm m
    JOIN btradi d ON m.acctno = d.acctno
""")

# ============================================================================
# LOAD COLLATERAL DATA
# ============================================================================

con.execute(f"""
    CREATE TEMP TABLE collater AS
    SELECT CAST(acctno AS BIGINT) acctno, cclassc, cdolarv
    FROM read_parquet('{INPUT_DIR}/btcoll/collater.parquet')
    WHERE CAST(acctno AS BIGINT) BETWEEN 2500000001 AND 2599999999
""")

con.execute("""
    CREATE TEMP TABLE btcoll AS
    SELECT DISTINCT acctno, cclassc liabcode, cdolarv
    FROM collater
""")

# ============================================================================
# MERGE BTR TRADE WITH COLLATERAL
# ============================================================================

con.execute("""
    CREATE TEMP TABLE btrcol AS
    SELECT b.*, c.liabcode coll_liabcode, c.cdolarv
    FROM btrade b
    LEFT JOIN btcoll c ON b.acctno = c.acctno
    ORDER BY b.acctno, b.prodcd
""")

# ============================================================================
# APPLY COLLATERAL ALLOCATION LOGIC
# ============================================================================

data = con.execute("SELECT * FROM btrcol ORDER BY acctno, prodcd").fetchall()
results = []

prev_acctno = None
xcolval = 0

for row in data:
    acctno, icurbal, dbalance, issdte, exprdate, origmt, branch, prodcd, balance, coll_liabcode, cdolarv = row
    
    # Determine collateral type
    typ = get_collateral_type(coll_liabcode)
    
    # Calculate total balance and collateral value
    totbal = (icurbal if icurbal else 0) + (dbalance if dbalance else 0)
    
    if totbal > 0 and cdolarv:
        colval = (icurbal / totbal) * cdolarv if icurbal else 0
    else:
        colval = 0
    
    # Reset xcolval for new account
    if acctno != prev_acctno:
        xcolval = colval
        prev_acctno = acctno
    
    # Allocate balance to collateral
    if xcolval > 0 and balance >= xcolval:
        # Secured portion
        xbalance = xcolval
        results.append((branch, prodcd, typ, None, None, xbalance, acctno, coll_liabcode, cdolarv))
        
        # Unsecured portion
        xbalance = balance - xcolval
        if xbalance > 0:
            results.append((branch, prodcd, 7, None, None, xbalance, acctno, coll_liabcode, cdolarv))
        xcolval = 0
    else:
        # Partially secured
        xbalance = balance
        xcolval = max(0, xcolval - xbalance)
        
        if colval > 0 and xcolval == 0:
            typ = 7  # Switch to unsecured
        
        results.append((branch, prodcd, typ, None, None, xbalance, acctno, coll_liabcode, cdolarv))

# Create LOAN dataset
con.execute("CREATE TEMP TABLE loan_raw (branch INT, bnmcode VARCHAR, typ INT, remmth DOUBLE, undrawn DOUBLE, xbalance DOUBLE, acctno BIGINT, liabcod1 VARCHAR, cdolarv DOUBLE)")
con.executemany("INSERT INTO loan_raw VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", results)

con.execute("""
    CREATE TEMP TABLE loan AS
    SELECT * FROM loan_raw
    WHERE bnmcode IS NOT NULL AND bnmcode != ''
    ORDER BY branch, bnmcode
""")

# ============================================================================
# CREATE LOAN2 DATASET WITH MATURITY
# ============================================================================

con.execute(f"""
    CREATE TEMP TABLE loan2 AS
    SELECT branch, prodcd bnmcode, 
           CASE WHEN origmt < '20' THEN 1 ELSE 13 END remmth,
           0.0 undrawn, balance, liabcode, issdte, exprdate, acctno
    FROM btradix
    WHERE prodcd IS NOT NULL AND prodcd != ''
""")

# Calculate actual remaining months for loan2
data2 = con.execute("SELECT * FROM loan2").fetchall()
results2 = []

for row in data2:
    branch, bnmcode, remmth_simple, undrawn, balance, liabcode, issdte, exprdate, acctno = row
    
    if exprdate:
        remmth = calc_remaining_months(exprdate, reptdate)
    else:
        remmth = remmth_simple
    
    results2.append((branch, bnmcode, remmth, undrawn, balance, liabcode, issdte, exprdate, acctno))

con.execute("DROP TABLE loan2")
con.execute("CREATE TEMP TABLE loan2 (branch INT, bnmcode VARCHAR, remmth DOUBLE, undrawn DOUBLE, balance DOUBLE, liabcode VARCHAR, issdte DATE, exprdate DATE, acctno BIGINT)")
con.executemany("INSERT INTO loan2 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", results2)

# ============================================================================
# GENERATE REPORTS
# ============================================================================

print("\n" + "="*100)
print("REPORT ID: EIWBTR1C")
print(f"PUBLIC BANK BERHAD            DATE: {rdate}")
print("="*100)

# Report 1: COLLATERAL BY TYPE
print("\nREPORT ON COLLATERAL")
print("-"*100)

summary = con.execute("""
    SELECT branch, bnmcode, typ, SUM(xbalance) total
    FROM loan
    GROUP BY branch, bnmcode, typ
    ORDER BY branch, bnmcode, typ
""").fetchall()

print(f"{'BRANCH':<10} {'BNMCODE':<10} {'TYPE':<50} {'AMOUNT':>20}")
print("-"*100)

for r in summary:
    branch, bnmcode, typ, total = r
    type_label = TYPE_LABELS.get(typ, 'UNKNOWN')
    print(f"{branch:<10} {bnmcode:<10} {type_label:<50} {total:>20,.2f}")

# Grand totals
totals_by_type = con.execute("""
    SELECT typ, SUM(xbalance) total
    FROM loan
    GROUP BY typ
    ORDER BY typ
""").fetchall()

print("\nGRAND TOTAL BY TYPE:")
for typ, total in totals_by_type:
    type_label = TYPE_LABELS.get(typ, 'UNKNOWN')
    print(f"{type_label:<60} {total:>20,.2f}")

# Report 2: MATURITY PROFILE (DETAILED)
print("\n" + "="*100)
print("REPORT ON REMAINING MATURITY (DETAILED)")
print("-"*100)

# Define maturity buckets
def get_maturity_bucket_detailed(remmth):
    if remmth is None or remmth <= 0.1:
        return 'UP TO 1 WK'
    elif remmth <= 1:
        return '>1 WK - 1 MTH'
    elif remmth <= 3:
        return '>1 MTH - 3 MTHS'
    elif remmth <= 6:
        return '>3 - 6 MTHS'
    elif remmth <= 12:
        return '>6 MTHS - 1 YR'
    else:
        return '>1 YEAR'

maturity_data = con.execute("SELECT bnmcode, remmth, balance FROM loan2").fetchall()

# Group by bnmcode and bucket
from collections import defaultdict
maturity_summary = defaultdict(lambda: defaultdict(float))

for bnmcode, remmth, balance in maturity_data:
    bucket = get_maturity_bucket_detailed(remmth)
    maturity_summary[bnmcode][bucket] += balance

buckets = ['UP TO 1 WK', '>1 WK - 1 MTH', '>1 MTH - 3 MTHS', '>3 - 6 MTHS', '>6 MTHS - 1 YR', '>1 YEAR']

print(f"{'BNMCODE':<10} {' '.join([f'{b:>15}' for b in buckets])}")
print("-"*100)

for bnmcode in sorted(maturity_summary.keys()):
    values = [maturity_summary[bnmcode].get(b, 0) for b in buckets]
    print(f"{bnmcode:<10} {' '.join([f'{v:>15,.2f}' for v in values])}")

# Report 3: MATURITY PROFILE (SUMMARY)
print("\n" + "="*100)
print("REPORT ON REMAINING MATURITY (SUMMARY)")
print("-"*100)

def get_maturity_bucket_summary(remmth):
    return '<1 YEAR' if remmth < 12 else '>1 YEAR'

maturity_summary2 = defaultdict(lambda: defaultdict(float))

for bnmcode, remmth, balance in maturity_data:
    bucket = get_maturity_bucket_summary(remmth)
    maturity_summary2[bnmcode][bucket] += balance

buckets2 = ['<1 YEAR', '>1 YEAR']

print(f"{'BNMCODE':<10} {' '.join([f'{b:>20}' for b in buckets2])}")
print("-"*100)

for bnmcode in sorted(maturity_summary2.keys()):
    values = [maturity_summary2[bnmcode].get(b, 0) for b in buckets2]
    print(f"{bnmcode:<10} {' '.join([f'{v:>20,.2f}' for v in values])}")

# ============================================================================
# SAVE OUTPUTS
# ============================================================================

con.execute(f"COPY loan TO '{OUTPUT_DIR}/btr_collateral.parquet'")
con.execute(f"COPY loan2 TO '{OUTPUT_DIR}/btr_maturity.parquet'")

# CSV outputs
with open(OUTPUT_DIR/'btr_collateral.csv', 'w') as f:
    f.write("BRANCH,BNMCODE,TYPE,XBALANCE\n")
    for r in con.execute("SELECT branch, bnmcode, typ, xbalance FROM loan").fetchall():
        f.write(f"{r[0]},{r[1]},{r[2]},{r[3]:.2f}\n")

with open(OUTPUT_DIR/'btr_maturity.csv', 'w') as f:
    f.write("BNMCODE,REMMTH,BALANCE\n")
    for r in con.execute("SELECT bnmcode, remmth, balance FROM loan2").fetchall():
        f.write(f"{r[0]},{r[1]:.2f},{r[2]:.2f}\n")

print(f"\n\nOutputs saved to {OUTPUT_DIR}")

con.close()
