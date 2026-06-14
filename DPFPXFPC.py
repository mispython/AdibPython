#!/usr/bin/env python3
"""BNM Liquidity Report for Trade Finance - Processes BA and TR transactions"""

import pyreadstat
import polars as pl
from datetime import datetime, timedelta, date
from pathlib import Path
import warnings
from pbblnfmt import get_remfmt, get_days_in_month
from pbbelf import calculate_next_bldate, calculate_remmth

warnings.filterwarnings('ignore')

# ============================================================================
# CONFIGURATION
# ============================================================================
BASE_DIR = Path(__file__).resolve().parent
INPUT_DIR = BASE_DIR / "data"
OUTPUT_DIR = BASE_DIR / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

BTDTL_BASE = INPUT_DIR / "bt_btdtl"
PBA01_BASE = INPUT_DIR / "pba01"

OUTPUT_PARQUET = OUTPUT_DIR / "bt.parquet"
OUTPUT_CSV = OUTPUT_DIR / "bt.csv"

BASE_DATE = date(1960, 1, 1)


# ============================================================================
# SAS FILE READERS
# ============================================================================
def read_sas(filepath):
    print(f"  Reading: {filepath.name}")
    df, _ = pyreadstat.read_sas7bdat(str(filepath))
    return pl.from_pandas(df)

def read_sas_dated(base, y, m, d):
    patterns = [base.parent / f"{base.stem}{y}{m}{d}.sas7bdat",
                base.parent / f"{base.stem}_{y}{m}{d}.sas7bdat"]
    for p in patterns:
        if p.exists():
            return read_sas(p)
    raise FileNotFoundError(f"No file found with date {y}{m}{d}")


# ============================================================================
# MAIN PROCESSING
# ============================================================================
def main(reptdate=None):
    # Step 1: Set report date
    reptdate = reptdate or date.today()
    rpyr, rpmth, rpday = reptdate.year, reptdate.month, reptdate.day
    reptdate_sas = (reptdate - BASE_DATE).days
    print(f"\nReport Date: {reptdate.strftime('%d/%m/%Y')}")
    
    # Step 2: Calculate runoff date (last day of month)
    last_day = get_days_in_month(rpyr, rpmth)
    runoff_dt = date(rpyr, rpmth, last_day)
    runoff_sas = (runoff_dt - BASE_DATE).days
    print(f"Runoff Date: {runoff_dt.strftime('%d/%m/%Y')}")
    
    # Step 3: Read BTDTL data
    y2, m2, d2 = f"{rpyr%100:02d}", f"{rpmth:02d}", f"{rpday:02d}"
    try:
        btdtl = read_sas_dated(BTDTL_BASE, y2, m2, d2)
        btdtl = btdtl.filter((pl.col('ISSDTE') > 0) | (pl.col('EXPRDATE') > 0))
        btdtl = btdtl.select(['TRANSREF', 'ISSDTE', 'EXPRDATE', 'PAYAMT'])
        btdtl = btdtl.sort(['TRANSREF', 'ISSDTE'], descending=[False, True]).unique('TRANSREF', keep='first')
    except:
        btdtl = pl.DataFrame()
    
    # Step 4: Read PBA data and merge
    records = []
    try:
        pba = read_sas_dated(PBA01_BASE, y2, m2, d2)
        pba = pba.with_columns(pl.col('TRANSREF').cast(pl.Utf8).str.slice(1, 8).alias('TRANSREF'))
        ba_data = pba.join(btdtl, on='TRANSREF', how='left')
        
        # Process BA records
        for row in ba_data.iter_rows(named=True):
            balance = (row.get('FCVALUE',0) or 0) - (row.get('UNEARNED',0) or 0)
            if balance == 0: continue
            
            cust = '08' if str(row.get('CUSTCD','')) in ['77','78','95','96'] else '09'
            item = '212' if (row.get('PRODUCT',0) or 0) == 100 else '219'
            days = (reptdate_sas - (row.get('BLDATE',0) or 0)) if (row.get('BLDATE',0) or 0) > 0 else 0
            
            # Process maturity and create records
            remmth, cur_bal, cur_bldate, expr_sas = None, balance, row.get('BLDATE',0) or 0, row.get('EXPRDATE',0) or 0
            payamt = row.get('PAYAMT',0) or 0
            
            if expr_sas and expr_sas <= runoff_sas:
                remmth = None
            elif expr_sas and (expr_sas - runoff_sas) < 8:
                remmth = 0.1
            elif expr_sas:
                if cur_bldate <= 0 and (row.get('ISSDTE',0) or 0) > 0:
                    cur_bldate = row.get('ISSDTE',0)
                    while cur_bldate > 0 and cur_bldate <= reptdate_sas:
                        bl_date = BASE_DATE + timedelta(days=int(cur_bldate))
                        iss_date = BASE_DATE + timedelta(days=int(row.get('ISSDTE',0))) if row.get('ISSDTE',0) > 0 else bl_date
                        nxt = calculate_next_bldate(bl_date, iss_date, '3', 6)
                        cur_bldate = (nxt - BASE_DATE).days if nxt else 0
                
                if payamt < 0: payamt = 0
                if cur_bldate > 0 and expr_sas and (cur_bldate > expr_sas or cur_bal <= payamt):
                    cur_bldate = expr_sas
                
                while cur_bldate > 0 and expr_sas and cur_bldate <= expr_sas:
                    if cur_bldate <= runoff_sas: remmth = None
                    elif (cur_bldate - runoff_sas) < 8: remmth = 0.1
                    else:
                        mat_date = BASE_DATE + timedelta(days=int(cur_bldate))
                        remmth = calculate_remmth(mat_date, runoff_dt, runoff_dt.year, runoff_dt.month, runoff_dt.day)
                    
                    if (remmth and remmth > 1) or cur_bldate == expr_sas: break
                    
                    if payamt > 0 and remmth is not None:
                        cur_bal -= payamt
                        for prefix in ['95', '93']:
                            rm = 13 if (prefix == '93' and days > 89) else remmth
                            records.append({'BNMCODE': f"{prefix}{item}{cust}{get_remfmt(rm)}0000Y", 'AMOUNT': payamt})
                    
                    bl_date = BASE_DATE + timedelta(days=int(cur_bldate))
                    iss_date = BASE_DATE + timedelta(days=int(row.get('ISSDTE',0))) if row.get('ISSDTE',0) > 0 else bl_date
                    nxt = calculate_next_bldate(bl_date, iss_date, '3', 6)
                    cur_bldate = (nxt - BASE_DATE).days if nxt else 0
                    if cur_bldate > 0 and expr_sas and (cur_bldate > expr_sas or cur_bal <= payamt):
                        cur_bldate = expr_sas
                
                if cur_bldate > 0:
                    if cur_bldate <= runoff_sas: remmth = None
                    elif (cur_bldate - runoff_sas) < 8: remmth = 0.1
                    else:
                        mat_date = BASE_DATE + timedelta(days=int(cur_bldate))
                        remmth = calculate_remmth(mat_date, runoff_dt, runoff_dt.year, runoff_dt.month, runoff_dt.day)
            
            if cur_bal != 0:
                for prefix in ['95', '93']:
                    rm = 13 if (prefix == '93' and days > 89) else remmth
                    records.append({'BNMCODE': f"{prefix}{item}{cust}{get_remfmt(rm)}0000Y", 'AMOUNT': cur_bal})
    except Exception as e:
        print(f"  Warning: {e}")
    
    # Step 5: Process TR records
    try:
        tr_full = read_sas_dated(BTDTL_BASE, y2, m2, d2)
        tr_data = tr_full.filter((~pl.col('LIABCODE').cast(pl.Utf8).is_in(['BAI','BAP','BAS','BAE'])) & 
                                  (pl.col('DIRCTIND').cast(pl.Utf8) == 'D'))
        
        for row in tr_data.iter_rows(named=True):
            outstand = row.get('OUTSTAND',0) or 0
            if outstand == 0: continue
            
            cust = '08' if str(row.get('CUSTCD','')) in ['77','78','95','96'] else '09'
            item = '212' if (row.get('PRODUCT',0) or 0) == 100 else '219'
            days = (reptdate_sas - (row.get('BLDATE',0) or 0)) if (row.get('BLDATE',0) or 0) > 0 else 0
            
            remmth, cur_out, cur_bldate, expr_sas = None, outstand, row.get('BLDATE',0) or 0, row.get('EXPRDATE',0) or 0
            payamt = row.get('PAYAMT',0) or 0
            
            if expr_sas and expr_sas <= runoff_sas:
                remmth = None
            elif expr_sas and (expr_sas - runoff_sas) < 8:
                remmth = 0.1
            elif expr_sas:
                if cur_bldate <= 0 and (row.get('ISSDTE',0) or 0) > 0:
                    cur_bldate = row.get('ISSDTE',0)
                    while cur_bldate > 0 and cur_bldate <= reptdate_sas:
                        bl_date = BASE_DATE + timedelta(days=int(cur_bldate))
                        iss_date = BASE_DATE + timedelta(days=int(row.get('ISSDTE',0))) if row.get('ISSDTE',0) > 0 else bl_date
                        nxt = calculate_next_bldate(bl_date, iss_date, '3', 6)
                        cur_bldate = (nxt - BASE_DATE).days if nxt else 0
                
                if payamt < 0: payamt = 0
                if cur_bldate > 0 and expr_sas and (cur_bldate > expr_sas or cur_out <= payamt):
                    cur_bldate = expr_sas
                
                while cur_bldate > 0 and expr_sas and cur_bldate <= expr_sas:
                    if cur_bldate <= runoff_sas: remmth = None
                    elif (cur_bldate - runoff_sas) < 8: remmth = 0.1
                    else:
                        mat_date = BASE_DATE + timedelta(days=int(cur_bldate))
                        remmth = calculate_remmth(mat_date, runoff_dt, runoff_dt.year, runoff_dt.month, runoff_dt.day)
                    
                    if (remmth and remmth > 1) or cur_bldate == expr_sas: break
                    
                    if payamt > 0 and remmth is not None:
                        cur_out -= payamt
                        for prefix in ['95', '93']:
                            rm = 0.1 if (prefix == '93' and days > 89) else remmth
                            records.append({'BNMCODE': f"{prefix}{item}{cust}{get_remfmt(rm)}0000Y", 'AMOUNT': payamt})
                    
                    bl_date = BASE_DATE + timedelta(days=int(cur_bldate))
                    iss_date = BASE_DATE + timedelta(days=int(row.get('ISSDTE',0))) if row.get('ISSDTE',0) > 0 else bl_date
                    nxt = calculate_next_bldate(bl_date, iss_date, '3', 6)
                    cur_bldate = (nxt - BASE_DATE).days if nxt else 0
                    if cur_bldate > 0 and expr_sas and (cur_bldate > expr_sas or cur_out <= payamt):
                        cur_bldate = expr_sas
                
                if cur_bldate > 0:
                    if cur_bldate <= runoff_sas: remmth = None
                    elif (cur_bldate - runoff_sas) < 8: remmth = 0.1
                    else:
                        mat_date = BASE_DATE + timedelta(days=int(cur_bldate))
                        remmth = calculate_remmth(mat_date, runoff_dt, runoff_dt.year, runoff_dt.month, runoff_dt.day)
            
            if cur_out != 0:
                for prefix in ['95', '93']:
                    rm = 0.1 if (prefix == '93' and days > 89) else remmth
                    records.append({'BNMCODE': f"{prefix}{item}{cust}{get_remfmt(rm)}0000Y", 'AMOUNT': cur_out})
    except Exception as e:
        print(f"  Warning: {e}")
    
    # Step 6: Combine, filter, and output
    if records:
        df = pl.DataFrame(records)
        missing = df.filter(pl.col('BNMCODE').str.slice(7,2) == '07')
        if len(missing) > 0:
            print(f"  Missing remmth records: {len(missing)}, Amount: {missing['AMOUNT'].sum():,.2f}")
        
        df = df.filter(pl.col('BNMCODE').str.slice(7,2) != '07')
        result = df.group_by('BNMCODE').agg(pl.col('AMOUNT').sum()).sort('BNMCODE')
        
        result.write_parquet(OUTPUT_PARQUET)
        result.write_csv(OUTPUT_CSV)
        
        print(f"\nOutput written to: {OUTPUT_PARQUET} and {OUTPUT_CSV}")
        print(f"Total records: {len(result)}, Total amount: {result['AMOUNT'].sum():,.2f}")
    else:
        print("No records generated")


if __name__ == "__main__":
    # Usage: python eibdlibt.py [YYYY-MM-DD]
    import sys
    if len(sys.argv) > 1:
        reptdate = datetime.strptime(sys.argv[1], '%Y-%m-%d').date()
        main(reptdate)
    else:
        main()
