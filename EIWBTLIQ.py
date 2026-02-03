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

con = duckdb.connect()

# ============================================================================
# MATURITY BUCKET MAPPING
# ============================================================================

def get_maturity_bucket(remmth):
    """Map remaining months to BNM maturity bucket code"""
    if remmth is None or remmth <= 0.1:
        return '01'  # UP TO 1 WK
    elif remmth <= 1:
        return '02'  # >1 WK - 1 MTH
    elif remmth <= 3:
        return '03'  # >1 MTH - 3 MTHS
    elif remmth <= 6:
        return '04'  # >3 - 6 MTHS
    elif remmth <= 12:
        return '05'  # >6 MTHS - 1 YR
    else:
        return '06'  # > 1 YEAR

# ============================================================================
# GET REPORTING DATE
# ============================================================================

reptdate = con.execute(f"SELECT reptdate FROM read_parquet('{INPUT_DIR}/bnm1/reptdate.parquet')").fetchone()[0]
day = reptdate.day
nowk = {8:'1', 15:'2', 22:'3'}.get(day, '4')

reptyear, reptmon, reptday = str(reptdate.year), f"{reptdate.month:02d}", f"{day:02d}"
rdate = reptdate.strftime('%d/%m/%Y')

print(f"Report Date: {rdate}, Week: {nowk}")

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def get_days_in_month(year, month):
    """Get number of days in month"""
    if month == 2:
        return 29 if year % 4 == 0 else 28
    elif month in [4, 6, 9, 11]:
        return 30
    return 31

def next_bldate(bldate, issdte, payfreq, freq):
    """Calculate next billing date"""
    if payfreq == '6':
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

def calc_remaining_months(matdt, reptdate):
    """Calculate remaining months from report date to maturity"""
    remy = matdt.year - reptdate.year
    remm = matdt.month - reptdate.month
    
    rpdays = get_days_in_month(reptdate.year, reptdate.month)
    mdday = min(matdt.day, rpdays)
    remd = (mdday - reptdate.day) / rpdays
    
    return remy * 12 + remm + remd

# ============================================================================
# LOAD BTR TRADE DATA
# ============================================================================

con.execute(f"""
    CREATE TEMP TABLE btrad AS
    SELECT acctno, prodcd, product, custcd, balance, exprdate, issdte, 
           bldate, payamt, forcurr
    FROM read_parquet('{INPUT_DIR}/bnm1/btrad{reptmon}{nowk}.parquet')
    WHERE SUBSTRING(prodcd, 1, 2) = '34' OR product IN (225, 226)
""")

# ============================================================================
# PROCESS LOANS WITH MATURITY BUCKETS
# ============================================================================

data = con.execute("SELECT * FROM btrad").fetchall()
results = []

for row in data:
    acctno, prodcd, product, custcd, balance, exprdate, issdte, bldate_raw, payamt, forcurr = row
    
    # Determine customer type
    cust = '08' if custcd in ('77', '78', '95', '96') else '09'
    
    # Determine product type (hardcoded to 'BT' for trade bills)
    prod = 'BT'
    
    # Determine item code
    if custcd in ('77', '78', '95', '96'):  # Individual
        item = '219'  # Others (no HL classification for BT)
    else:  # Corporate
        if prod == 'FL':
            item = '211'
        elif prod == 'RC':
            item = '212'
        else:
            item = '219'
    
    # Convert dates
    bldate = bldate_raw if bldate_raw and bldate_raw > datetime(1900, 1, 1).date() else None
    
    # Calculate days overdue
    days = (reptdate - bldate).days if bldate else 0
    
    # If expiry within 1 week
    if (exprdate - reptdate).days < 8:
        remmth = 0.1
    else:
        # Calculate payment schedule
        payfreq = '3'  # Default semi-annual
        freq_map = {'1': 1, '2': 3, '3': 6, '4': 12}
        freq = freq_map.get(payfreq, 6)
        
        # Initialize billing date
        if product in (350, 910, 925):
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
            # Calculate remaining months
            remmth = calc_remaining_months(bldate, reptdate)
            
            # Break if beyond 12 months or at maturity
            if remmth > 12 or bldate == exprdate:
                break
            
            # Output payment installment
            amount = min(payamt, remaining_balance)
            bucket = get_maturity_bucket(remmth)
            
            # Determine BNM code prefix based on product and days
            if prodcd[:3] == '346':  # Foreign currency trade bills
                prefix = '96'
                # Also output NPL version if overdue
                bnmcode = f"{prefix}{item}{cust}{bucket}0000Y"
                results.append((bnmcode, amount, 
                               amount if forcurr == 'USD' else 0,
                               amount if forcurr == 'SGD' else 0))
                
                if days > 89:
                    bnmcode_npl = f"94{item}{cust}{bucket}0000Y"
                    results.append((bnmcode_npl, amount, 
                                   amount if forcurr == 'USD' else 0,
                                   amount if forcurr == 'SGD' else 0))
            else:  # MYR trade bills
                prefix = '95'
                bnmcode = f"{prefix}{item}{cust}{bucket}0000Y"
                results.append((bnmcode, amount, 0, 0))
                
                if days > 89:
                    bnmcode_npl = f"93{item}{cust}{bucket}0000Y"
                    results.append((bnmcode_npl, amount, 0, 0))
            
            # Update for next iteration
            remaining_balance -= payamt
            bldate = next_bldate(bldate, issdte, payfreq, freq)
            
            if bldate > exprdate or remaining_balance <= payamt:
                bldate = exprdate
        
        # Final payment/maturity
        remmth = calc_remaining_months(bldate, reptdate)
    
    # Output final balance
    amount = balance if 'remaining_balance' not in locals() else remaining_balance
    bucket = get_maturity_bucket(remmth)
    
    if prodcd[:3] == '346':
        bnmcode = f"96{item}{cust}{bucket}0000Y"
        results.append((bnmcode, amount,
                       amount if forcurr == 'USD' else 0,
                       amount if forcurr == 'SGD' else 0))
        
        if days > 89:
            bnmcode_npl = f"94{item}{cust}{bucket}0000Y"
            results.append((bnmcode_npl, amount,
                           amount if forcurr == 'USD' else 0,
                           amount if forcurr == 'SGD' else 0))
    else:
        bnmcode = f"95{item}{cust}{bucket}0000Y"
        results.append((bnmcode, amount, 0, 0))
        
        if days > 89:
            bnmcode_npl = f"93{item}{cust}{bucket}0000Y"
            results.append((bnmcode_npl, amount, 0, 0))

# ============================================================================
# AGGREGATE LOAN DATA
# ============================================================================

con.execute("CREATE TEMP TABLE note_raw (bnmcode VARCHAR, amount DOUBLE, amtusd DOUBLE, amtsgd DOUBLE)")
con.executemany("INSERT INTO note_raw VALUES (?, ?, ?, ?)", results)

con.execute("""
    CREATE TEMP TABLE note AS
    SELECT bnmcode, SUM(amount) amount, SUM(amtusd) amtusd, SUM(amtsgd) amtsgd
    FROM note_raw
    GROUP BY bnmcode
    ORDER BY bnmcode
""")

# Print loan summary
print("\n" + "="*80)
print("TRADE BILLS LIQUIDITY MATURITY PROFILE")
print(f"AS AT {rdate}")
print("="*80)
print(con.execute("SELECT * FROM note").df())
print("\nTotals:")
totals = con.execute("SELECT SUM(amount) amt, SUM(amtusd) usd, SUM(amtsgd) sgd FROM note").fetchone()
print(f"AMOUNT: {totals[0]:,.2f}, AMTUSD: {totals[1]:,.2f}, AMTSGD: {totals[2]:,.2f}")

# ============================================================================
# UNDRAWN PORTION
# ============================================================================

con.execute(f"""
    CREATE TEMP TABLE alm AS
    SELECT acctno, exprdate, dundrawn
    FROM read_parquet('{INPUT_DIR}/bnm1/btmast{reptmon}{nowk}.parquet')
    WHERE subacct = 'OV' AND custcd != ' ' AND dcurbal IS NOT NULL
    ORDER BY acctno
""")

# Process undrawn facilities
undrawn_data = con.execute("SELECT * FROM alm").fetchall()
undrawn_results = []

for row in undrawn_data:
    acctno, exprdate, dundrawn = row
    
    item = '429'  # Undrawn portion item code
    
    # Calculate remaining months
    if (exprdate - reptdate).days < 8:
        remmth = 0.1
    else:
        remmth = calc_remaining_months(exprdate, reptdate)
    
    bucket = get_maturity_bucket(remmth)
    bnmcode = f"95{item}00{bucket}0000Y"
    
    undrawn_results.append((bnmcode, dundrawn))

# Aggregate undrawn
con.execute("CREATE TEMP TABLE unote_raw (bnmcode VARCHAR, amount DOUBLE)")
con.executemany("INSERT INTO unote_raw VALUES (?, ?)", undrawn_results)

con.execute("""
    CREATE TEMP TABLE unote AS
    SELECT bnmcode, SUM(amount) amount
    FROM unote_raw
    GROUP BY bnmcode
    ORDER BY bnmcode
""")

# Print undrawn summary
print("\n" + "="*80)
print(f"UNDRAWN PORTION FOR TRADE BILLS AS AT {rdate}")
print("="*80)
print(con.execute("SELECT * FROM unote").df())
print("\nTotal Undrawn:")
undrawn_total = con.execute("SELECT SUM(amount) FROM unote").fetchone()[0]
print(f"{undrawn_total:,.2f}")

# ============================================================================
# SAVE OUTPUTS
# ============================================================================

con.execute(f"COPY note TO '{OUTPUT_DIR}/btliq_note.parquet'")
con.execute(f"COPY unote TO '{OUTPUT_DIR}/btliq_undrawn.parquet'")

# CSV outputs
with open(OUTPUT_DIR/'btliq_loans.csv', 'w') as f:
    f.write("BNMCODE,AMOUNT,AMTUSD,AMTSGD\n")
    for r in con.execute("SELECT * FROM note").fetchall():
        f.write(f"{r[0]},{r[1]:.2f},{r[2]:.2f},{r[3]:.2f}\n")

with open(OUTPUT_DIR/'btliq_undrawn.csv', 'w') as f:
    f.write("BNMCODE,AMOUNT\n")
    for r in con.execute("SELECT * FROM unote").fetchall():
        f.write(f"{r[0]},{r[1]:.2f}\n")

print(f"\nOutputs saved to {OUTPUT_DIR}")

con.close()
