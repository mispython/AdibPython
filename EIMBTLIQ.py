import duckdb
from pathlib import Path
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

# ============================================================================
# CONFIGURATION
# ============================================================================

BASE_DIR = Path('.')
INPUT_DIR = BASE_DIR / 'data'
OUTPUT_DIR = BASE_DIR / 'output'
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Item descriptions
ITEM_LABELS = {
    'A1.01': 'LOANS: CORP - FIXED TERM LOANS',
    'A1.02': 'LOANS: CORP - REVOLVING LOANS',
    'A1.03': 'LOANS: CORP - OVERDRAFTS',
    'A1.04': 'LOANS: CORP - OTHERS',
    'A1.05': 'LOANS: IND  - HOUSING LOANS',
    'A1.07': 'LOANS: IND  - OVERDRAFTS',
    'A1.08': 'LOANS: IND  - OTHERS',
    'A1.08A': 'LOANS: IND  - REVOLVING LOANS',
}

con = duckdb.connect()

# ============================================================================
# GET REPORTING DATE AND CALCULATE RUN-OFF DATE
# ============================================================================

reptdate = con.execute(f"SELECT reptdate FROM read_parquet('{INPUT_DIR}/bnm1/reptdate.parquet')").fetchone()[0]
day = reptdate.day
nowk = {8:'1', 15:'2', 22:'3'}.get(day, '4')

reptyear, reptmon, reptday = str(reptdate.year), f"{reptdate.month:02d}", f"{day:02d}"
rdate = reptdate.strftime('%d/%m/%Y')

# Calculate run-off date (end of reporting month)
runoffdt = reptdate.replace(day=1) + relativedelta(months=1) - timedelta(days=1)

print(f"Report Date: {rdate}")
print(f"Run-off Date: {runoffdt.strftime('%d/%m/%Y')}")

# ============================================================================
# HELPER FUNCTIONS FOR DATE CALCULATIONS
# ============================================================================

def get_days_in_month(year, month):
    """Get number of days in a month"""
    if month == 2:
        return 29 if year % 4 == 0 else 28
    elif month in [4, 6, 9, 11]:
        return 30
    return 31

def next_bldate(bldate, issdte, payfreq, freq):
    """Calculate next billing date"""
    if payfreq == '6':  # Fortnightly
        return bldate + timedelta(days=14)
    else:
        dd = issdte.day
        mm = bldate.month + freq
        yy = bldate.year
        
        if mm > 12:
            mm -= 12
            yy += 1
        
        days_in_month = get_days_in_month(yy, mm)
        dd = min(dd, days_in_month)
        
        return datetime(yy, mm, dd).date()

def calc_remaining_months(matdt, runoffdt):
    """Calculate remaining months from run-off date to maturity"""
    if matdt <= runoffdt:
        return None
    
    if (matdt - runoffdt).days < 8:
        return 0.1
    
    remy = matdt.year - runoffdt.year
    remm = matdt.month - runoffdt.month
    
    # Adjust day component
    rpdays = get_days_in_month(runoffdt.year, runoffdt.month)
    mdday = min(matdt.day, rpdays)
    remd = (mdday - runoffdt.day) / rpdays
    
    return remy * 12 + remm + remd

# ============================================================================
# LOAD BNM TRADE DATA
# ============================================================================

con.execute(f"""
    CREATE TEMP TABLE btrad AS
    SELECT acctno, prodcd, product, custcd, balance, exprdate, issdte, 
           bldate, payamt, loanstat, reptdate
    FROM read_parquet('{INPUT_DIR}/bnm1/btrad{reptmon}{nowk}.parquet')
    WHERE SUBSTRING(prodcd, 1, 2) = '34' OR product IN (225, 226)
    ORDER BY acctno
""")

# ============================================================================
# PROCESS LOANS WITH MATURITY CALCULATIONS
# ============================================================================

data = con.execute("SELECT * FROM btrad").fetchall()
results = []

for row in data:
    acctno, prodcd, product, custcd, balance, exprdate, issdte, bldate_raw, payamt, loanstat, _ = row
    
    # Determine item classification
    prod = 'BT'  # Trade Bills
    
    if custcd in ('77', '78', '95', '96'):  # Individual
        if prod == 'HL':
            item = 'A1.05'
        elif prod == 'RC':
            item = 'A1.08A'
        else:
            item = 'A1.08'
    else:  # Corporate
        if prod == 'FL':
            item = 'A1.01'
        elif prod == 'RC':
            item = 'A1.02'
        else:
            item = 'A1.04'
    
    # Hardcoded product 100 → housing loan
    if product == 100:
        item = 'A1.05'
    
    # Convert dates
    bldate = bldate_raw if bldate_raw and bldate_raw > 0 else None
    
    # Calculate days overdue
    days = (reptdate - bldate).days if bldate else 0
    
    # If expiry date <= run-off date, no remaining months
    if exprdate <= runoffdt:
        remmth = None
        amount = balance
        if days > 89:
            remmth = 13  # NPL bucket
        results.append(('1-RM', item, remmth, amount))
        continue
    
    # If expiry within 1 week of run-off
    if (exprdate - runoffdt).days < 8:
        remmth = 0.1
        amount = balance
        if days > 89:
            remmth = 13
        results.append(('1-RM', item, remmth, amount))
        continue
    
    # Calculate payment schedule
    payfreq = '3'  # Default semi-annual
    freq_map = {'1': 1, '2': 3, '3': 6, '4': 12}
    freq = freq_map.get(payfreq, 6)
    
    # Initialize billing date
    if product in (350, 910, 925):  # Specific products
        bldate = exprdate
    elif not bldate or bldate <= datetime(1900, 1, 1).date():
        bldate = issdte
        while bldate <= reptdate:
            bldate = next_bldate(bldate, issdte, payfreq, freq)
    
    # Payment amount handling
    if not payamt or payamt < 0:
        payamt = 0
    
    if bldate > exprdate or balance <= payamt:
        bldate = exprdate
    
    # Generate payment schedule
    remaining_balance = balance
    
    while bldate <= exprdate:
        # Calculate remaining months for this payment
        if bldate <= runoffdt:
            remmth = None
        elif (bldate - runoffdt).days < 8:
            remmth = 0.1
        else:
            remmth = calc_remaining_months(bldate, runoffdt)
        
        # Break if beyond 1 month or at maturity
        if remmth and remmth > 1 or bldate == exprdate:
            amount = remaining_balance
            if days > 89 or loanstat != 1:
                remmth = 13  # NPL
            results.append(('1-RM', item, remmth, amount))
            break
        
        # Output payment installment
        amount = min(payamt, remaining_balance)
        if days > 89 or loanstat != 1:
            remmth = 13
        results.append(('1-RM', item, remmth, amount))
        
        # Update for next iteration
        remaining_balance -= payamt
        bldate = next_bldate(bldate, issdte, payfreq, freq)
        
        if bldate > exprdate or remaining_balance <= payamt:
            bldate = exprdate

# ============================================================================
# CREATE OUTPUT DATASET
# ============================================================================

con.execute("CREATE TEMP TABLE note_raw (part VARCHAR, item VARCHAR, remmth DOUBLE, amount DOUBLE)")
con.executemany("INSERT INTO note_raw VALUES (?, ?, ?, ?)", results)

# Aggregate by part, item, remmth
con.execute("""
    CREATE TEMP TABLE note AS
    SELECT part, item, remmth, SUM(amount) amount
    FROM note_raw
    GROUP BY part, item, remmth
    ORDER BY item, remmth
""")

# ============================================================================
# GENERATE REPORT
# ============================================================================

print("\n" + "="*100)
print("PUBLIC BANK BERHAD")
print(f"NEW LIQUIDITY FRAMEWORK AS AT {rdate}")
print("(CONTRACTUAL RUN-OFF FOR TRADE BILLS)")
print("BREAKDOWN BY BEHAVIOURAL MATURITY PROFILE (PART 1-RM)")
print("="*100)
print("\nCORE (NON-TRADING) BANKING ACTIVITIES")
print("-"*100)

# Generate tabular report
items = con.execute("SELECT DISTINCT item FROM note WHERE part='1-RM' ORDER BY item").fetchall()

# Define buckets
buckets = [
    ('UP TO 1 WK', lambda x: x is not None and x <= 0.255),
    ('>1 WK - 1 MTH', lambda x: x is not None and 0.255 < x <= 1),
    ('>1 MTH', lambda x: x is not None and x > 1 and x != 13),
]

output_lines = []
output_lines.append(f"{'ITEM':<50} {'UP TO 1 WK':>15} {'>1 WK - 1 MTH':>15} {'>1 MTH':>15} {'TOTAL':>15}")
output_lines.append("-"*110)

for item_row in items:
    item = item_row[0]
    label = f"{item}  {ITEM_LABELS.get(item, '')}"
    
    row_data = []
    total = 0
    
    for bucket_name, bucket_fn in buckets:
        bucket_sum = con.execute(f"""
            SELECT COALESCE(SUM(amount), 0)
            FROM note
            WHERE part = '1-RM' AND item = '{item}'
        """).fetchone()[0]
        
        # Filter by bucket condition in Python (simplified)
        amounts = con.execute(f"""
            SELECT remmth, amount FROM note WHERE part='1-RM' AND item='{item}'
        """).fetchall()
        
        bucket_total = sum(amt for rem, amt in amounts if bucket_fn(rem))
        row_data.append(bucket_total)
        total += bucket_total
    
    output_lines.append(f"{label:<50} {row_data[0]:>15,.2f} {row_data[1]:>15,.2f} {row_data[2]:>15,.2f} {total:>15,.2f}")

print("\n".join(output_lines))

# Write to CSV
with open(OUTPUT_DIR/'liquidity_runoff.csv', 'w') as f:
    f.write("ITEM,UP_TO_1_WK,GT1WK_1MTH,GT1MTH,TOTAL\n")
    for item_row in items:
        item = item_row[0]
        amounts = con.execute(f"SELECT remmth, amount FROM note WHERE part='1-RM' AND item='{item}'").fetchall()
        
        bucket0 = sum(amt for rem, amt in amounts if rem is not None and rem <= 0.255)
        bucket1 = sum(amt for rem, amt in amounts if rem is not None and 0.255 < rem <= 1)
        bucket2 = sum(amt for rem, amt in amounts if rem is not None and rem > 1 and rem != 13)
        total = bucket0 + bucket1 + bucket2
        
        f.write(f"{item},{bucket0:.2f},{bucket1:.2f},{bucket2:.2f},{total:.2f}\n")

# Save to parquet
con.execute(f"COPY note TO '{OUTPUT_DIR}/note.parquet'")

print(f"\nOutput saved to {OUTPUT_DIR}")
print(f"Total records: {con.execute('SELECT COUNT(*) FROM note').fetchone()[0]}")

con.close()
