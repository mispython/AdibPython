import polars as pl
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

BNM1_REPTDATE = 'data/bnm1/reptdate.parquet'
BNM1_DIR = 'data/bnm1/'
NLFBT = 'data/nlfbt.txt'

HL_PRODUCTS = [4, 5, 6, 7, 31, 32, 100, 101, 102, 103, 110, 111, 112, 113, 114, 115,
               116, 170, 200, 201, 204, 205, 209, 210, 211, 212, 214, 215, 219, 220,
               225, 226, 227, 228, 229, 230, 231, 232, 233, 234]
RC_PRODUCTS = [350, 910, 925]

df_reptdate = pl.read_parquet(BNM1_REPTDATE)
reptdate = df_reptdate['REPTDATE'][0]

day = reptdate.day
if day == 8:
    nowk = '1'
elif day == 15:
    nowk = '2'
elif day == 22:
    nowk = '3'
else:
    nowk = '4'

reptyear = reptdate.strftime('%Y')
reptmon = reptdate.strftime('%m')
reptday = reptdate.strftime('%d')
rdate = reptdate.strftime('%d%m%y')

rpyr = reptdate.year
rpmth = reptdate.month
rpday = reptdate.day

def get_days_in_month(year, month):
    if month == 2:
        return 29 if year % 4 == 0 else 28
    elif month in [4, 6, 9, 11]:
        return 30
    else:
        return 31

def calculate_remmth(matdt, reptdate_val):
    mdyr = matdt.year
    mdmth = matdt.month
    mdday = matdt.day
    
    rpyr_val = reptdate_val.year
    rpmth_val = reptdate_val.month
    rpday_val = reptdate_val.day
    
    rpdays_current = get_days_in_month(rpyr_val, rpmth_val)
    
    if mdday > rpday_val:
        mdday_adj = rpday_val
    else:
        mdday_adj = mdday
    
    remy = mdyr - rpyr_val
    remm = mdmth - rpmth_val
    remd = mdday_adj - rpday_val
    
    return remy * 12 + remm + remd / rpdays_current

def get_next_bldate(bldate, issdte, payfreq, product):
    freq_map = {'1': 1, '2': 3, '3': 6, '4': 12}
    freq = freq_map.get(payfreq, 6)
    
    if payfreq == '6':
        return bldate + timedelta(days=14)
    else:
        dd = issdte.day
        mm = bldate.month + freq
        yy = bldate.year
        
        while mm > 12:
            mm -= 12
            yy += 1
        
        days_in_target = get_days_in_month(yy, mm)
        dd = min(dd, days_in_target)
        
        return datetime(yy, mm, dd).date()

def categorize_remmth(remmth):
    if remmth < 0.1:
        return '01'
    elif remmth < 1:
        return '02'
    elif remmth < 3:
        return '03'
    elif remmth < 6:
        return '04'
    elif remmth < 12:
        return '05'
    elif remmth < 36:
        return '06'
    elif remmth < 60:
        return '07'
    else:
        return '08'

df_btrad = pl.read_parquet(f'{BNM1_DIR}BTRAD{reptmon}{nowk}.parquet')

df_note = df_btrad.filter(
    (pl.col('PRODCD').str.slice(0, 2) == '34') | 
    pl.col('PRODUCT').is_in([225, 226])
)

df_note = df_note.with_columns([
    pl.when(pl.col('CUSTCD').is_in(['77', '78', '95', '96']))
      .then(pl.lit('08'))
      .otherwise(pl.lit('09'))
      .alias('CUST'),
    pl.lit('BT').alias('PROD')
])

df_note = df_note.with_columns([
    pl.when(pl.col('CUSTCD').is_in(['77', '78', '95', '96']))
      .then(pl.lit('219'))
      .otherwise(
          pl.when(pl.col('PROD') == 'FL')
            .then(pl.lit('211'))
            .when(pl.col('PROD') == 'RC')
            .then(pl.lit('212'))
            .otherwise(pl.lit('219'))
      )
      .alias('ITEM')
])

df_note = df_note.with_columns([
    pl.when(pl.col('BLDATE') > 0)
      .then((pl.lit(reptdate).cast(pl.Date) - pl.col('BLDATE').cast(pl.Date)).dt.total_days())
      .otherwise(pl.lit(None))
      .alias('DAYS')
])

records = []

for row in df_note.iter_rows(named=True):
    bldate = row['BLDATE'] if row['BLDATE'] and row['BLDATE'] > 0 else row['ISSDTE']
    exprdate = row['EXPRDATE']
    balance = row['BALANCE']
    payamt = row['PAYAMT'] if row['PAYAMT'] and row['PAYAMT'] > 0 else 0
    issdte = row['ISSDTE']
    product = row['PRODUCT']
    prodcd = row['PRODCD']
    forcurr = row.get('FORCURR', '')
    days = row['DAYS'] if row['DAYS'] else 0
    
    if (exprdate - reptdate).days < 8:
        remmth = 0.1
    else:
        payfreq = '3'
        
        if product in RC_PRODUCTS:
            bldate = exprdate
        elif not bldate or bldate <= 0:
            bldate = issdte
            while bldate <= reptdate:
                bldate = get_next_bldate(bldate, issdte, payfreq, product)
        
        if payamt < 0:
            payamt = 0
        
        if bldate > exprdate or balance <= payamt:
            bldate = exprdate
        
        while bldate <= exprdate:
            matdt = bldate
            remmth = calculate_remmth(matdt, reptdate)
            
            if remmth > 12 or bldate == exprdate:
                break
            
            remmth_code = categorize_remmth(remmth)
            
            if prodcd[:3] == '346':
                records.append({
                    'BNMCODE': f"96{row['ITEM']}{row['CUST']}{remmth_code}0000Y",
                    'AMOUNT': payamt,
                    'AMTUSD': 0,
                    'AMTSGD': 0
                })
                
                remmth_overdue = 13 if days > 89 else remmth
                remmth_code_overdue = categorize_remmth(remmth_overdue)
                records.append({
                    'BNMCODE': f"94{row['ITEM']}{row['CUST']}{remmth_code_overdue}0000Y",
                    'AMOUNT': payamt,
                    'AMTUSD': 0,
                    'AMTSGD': 0
                })
            else:
                records.append({
                    'BNMCODE': f"95{row['ITEM']}{row['CUST']}{remmth_code}0000Y",
                    'AMOUNT': payamt,
                    'AMTUSD': 0,
                    'AMTSGD': 0
                })
                
                remmth_overdue = 13 if days > 89 else remmth
                remmth_code_overdue = categorize_remmth(remmth_overdue)
                records.append({
                    'BNMCODE': f"93{row['ITEM']}{row['CUST']}{remmth_code_overdue}0000Y",
                    'AMOUNT': payamt,
                    'AMTUSD': 0,
                    'AMTSGD': 0
                })
            
            balance -= payamt
            bldate = get_next_bldate(bldate, issdte, payfreq, product)
            
            if bldate > exprdate or balance <= payamt:
                bldate = exprdate
    
    remmth_final = remmth if 'remmth' in locals() else 0.1
    remmth_code_final = categorize_remmth(remmth_final)
    
    amtusd = balance if forcurr == 'USD' else 0
    amtsgd = balance if forcurr == 'SGD' else 0
    
    if prodcd[:3] == '346':
        records.append({
            'BNMCODE': f"96{row['ITEM']}{row['CUST']}{remmth_code_final}0000Y",
            'AMOUNT': balance,
            'AMTUSD': amtusd,
            'AMTSGD': amtsgd
        })
        
        remmth_overdue_final = 13 if days > 89 else remmth_final
        remmth_code_overdue_final = categorize_remmth(remmth_overdue_final)
        records.append({
            'BNMCODE': f"94{row['ITEM']}{row['CUST']}{remmth_code_overdue_final}0000Y",
            'AMOUNT': balance,
            'AMTUSD': amtusd,
            'AMTSGD': amtsgd
        })
    else:
        records.append({
            'BNMCODE': f"95{row['ITEM']}{row['CUST']}{remmth_code_final}0000Y",
            'AMOUNT': balance,
            'AMTUSD': amtusd,
            'AMTSGD': amtsgd
        })
        
        remmth_overdue_final = 13 if days > 89 else remmth_final
        remmth_code_overdue_final = categorize_remmth(remmth_overdue_final)
        records.append({
            'BNMCODE': f"93{row['ITEM']}{row['CUST']}{remmth_code_overdue_final}0000Y",
            'AMOUNT': balance,
            'AMTUSD': amtusd,
            'AMTSGD': amtsgd
        })

df_note_output = pl.DataFrame(records)

df_note_summary = df_note_output.group_by('BNMCODE').agg([
    pl.col('AMOUNT').sum(),
    pl.col('AMTUSD').sum(),
    pl.col('AMTSGD').sum()
])

df_note_summary = df_note_summary.with_columns([
    pl.col('AMTUSD').fill_null(0),
    pl.col('AMTSGD').fill_null(0)
])

with open(NLFBT, 'w') as f:
    f.write(f"NLFBT{reptday}{reptmon}{reptyear}\n")
    
    for row in df_note_summary.iter_rows(named=True):
        f.write(f"{row['BNMCODE']};{row['AMOUNT']:.2f};\n")

print(f"Report generated: {NLFBT}")
print(f"Total records: {len(df_note_summary)}")
