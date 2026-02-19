"""
SAS to Python Translation - EIIDFALE
Processes Islamic Fixed Deposit data with date-based classification
"""

import polars as pl
from pathlib import Path
from datetime import datetime, timedelta, date

BASE_DIR = Path("./")
INPUT_DIR = BASE_DIR / "input"
OUTPUT_DIR = BASE_DIR / "output"
MIS_DIR = OUTPUT_DIR / "mis"

for d in [MIS_DIR]:
    d.mkdir(parents=True, exist_ok=True)

DEPOSIT_DIR = INPUT_DIR / "deposit"

def sas_date_to_dt(sas_date):
    if not sas_date or sas_date <= 0: return None
    return datetime(1960, 1, 1) + timedelta(days=int(sas_date))

def dt_to_sas_date(dt):
    if not dt: return 0
    return (dt - datetime(1960, 1, 1).date()).days

def parse_mmddyyyy(s):
    if not s or len(s) != 8: return None
    return date(int(s[4:8]), int(s[0:2]), int(s[2:4]))

def setup_dates():
    rept = pl.read_parquet(DEPOSIT_DIR / "REPTDATE.parquet").select(pl.first()).item()
    rept_dt = sas_date_to_dt(rept)
    return {
        'REPTDATE': rept_dt, 'REPTDATE_SAS': rept,
        'REPTYEAR': rept_dt.strftime('%Y'), 'REPTMON': rept_dt.strftime('%m'),
        'REPTDAY': rept_dt.strftime('%d'), 'RDATE': str(rept).strip()
    }

def get_rn(dt):
    if not dt or dt == 0: return 'O'
    cutoff = [
        ('04SEP04', '15APR06', 'A'), ('16APR06', '15SEP08', 'N'),
        ('16SEP08', '15MAR09', 'Z'), ('16MAR09', '18MAR10', 'X'),
        ('19MAR10', '15JUN10', 'S'), ('16JUN10', '19JUL10', 'Y'),
        ('20JUL10', '15MAY11', 'K'), ('16MAY11', '15JAN12', 'L'),
        ('16JAN12', '15JUN13', 'M'), ('16JUN13', '15SEP16', 'O'),
        ('16SEP16', '31DEC99', 'P')
    ]
    for s, e, r in cutoff:
        sd = datetime.strptime(f"{s}2004", "%d%b%Y").date() if s == '04SEP04' else datetime.strptime(f"{s}2004", "%d%b%Y").date()
        ed = datetime.strptime(f"{e}2004", "%d%b%Y").date()
        if s == '16SEP16': ed = date(2099, 12, 31)
        if sd <= dt <= ed: return r
    return 'B' if dt < date(2004, 9, 4) else 'O'

def process_fd():
    mv = setup_dates()
    fd = pl.read_parquet(DEPOSIT_DIR / "FD.parquet").filter(pl.col('OPENIND').is_in(['D','O']))
    
    # Process maturity dates
    fd = fd.with_columns([
        pl.when(pl.col('LMATDATE') == 0).then(pl.col('ORGDATE')).otherwise(pl.col('LMATDATE')).alias('LMATDATE')
    ])
    
    fd = fd.with_columns([
        pl.when(pl.col('LMATDATE') != 0)
          .then(pl.col('LMATDATE').cast(str).str.zfill(11).str.slice(0,8).str.strptime(pl.Date, '%m%d%Y'))
          .otherwise(None).alias('LMDATES'),
        pl.when(pl.col('LMATDATE') != 0)
          .then(pl.col('LMATDATE').cast(str).str.zfill(11).str.slice(0,8).str.strptime(pl.Date, '%m%d%Y'))
          .otherwise(None).alias('LMATDT')
    ])
    
    # Add RN classification
    fd = fd.with_columns(
        pl.col('LMATDT').apply(lambda x: get_rn(x) if x else 'O').alias('RN')
    )
    
    # Split datasets
    fdw = fd.filter(pl.col('ACCTTYPE') == 315)
    fdv = fd.filter(pl.col('ACCTTYPE') == 394)
    fd_rest = fd.filter(~pl.col('ACCTTYPE').is_in([315, 394, 316, 393]))
    
    mask = pl.col('ACCTTYPE').is_in([316, 393])
    fdh = fd.filter(mask & pl.col('INTPLAN').is_between(272, 283))
    fdg = fd.filter(mask & ~pl.col('INTPLAN').is_between(272, 283))
    
    fdo = fd.filter(pl.col('ACCTTYPE') != 398)
    
    return fd, fdw, fdv, fdg, fdh, fdo, mv

def summarize_by_period(df, date_col, condition, suffix, mv):
    """Generic summarization function for each period"""
    filtered = df.filter(condition)
    
    # Count by RN, BRANCH, INTPLAN
    cnt = filtered.unique(['RN','BRANCH','INTPLAN','ACCTNO']).group_by(['RN','BRANCH','INTPLAN']).len().rename({'len':'FDINO'})
    
    # Sum by detailed keys
    sum_df = filtered.group_by(['RN','BRANCH','INTPLAN','LMATDT','ACCTNO','CUSTCD']).agg(pl.col('CURBAL').sum().alias('FDI'))
    sum_cnt = sum_df.group_by(['RN','BRANCH','INTPLAN']).len().rename({'len':'FDINO2'})
    sum_bal = sum_df.group_by(['RN','BRANCH','INTPLAN']).agg(pl.col('FDI').sum())
    
    result = cnt.join(sum_bal, on=['RN','BRANCH','INTPLAN'], how='outer').join(sum_cnt, on=['RN','BRANCH','INTPLAN'], how='outer')
    result = result.with_columns(pl.lit(mv['REPTDATE_SAS']).alias('REPTDATE'))
    
    # Save intermediate
    out_name = f"DYIBU{suffix}"
    globals()[out_name] = result
    return result

def process_all_periods(fd, fdw, fdv, fdg, fdh, fdo, mv):
    periods = [
        ('B', (pl.col('LMATDT') < date(2004,9,4)) & (pl.col('LMATDT').is_not_null())),
        ('A', pl.col('LMATDT').is_between(date(2004,9,4), date(2006,4,15))),
        ('N', pl.col('LMATDT').is_between(date(2006,4,16), date(2008,9,15))),
        ('Z', pl.col('LMATDT').is_between(date(2008,9,16), date(2009,3,15))),
        ('X', pl.col('LMATDT').is_between(date(2009,3,16), date(2010,3,18))),
        ('S', pl.col('LMATDT').is_between(date(2010,3,19), date(2010,6,15))),
        ('Y', pl.col('LMATDT').is_between(date(2010,6,16), date(2010,7,19))),
        ('K', pl.col('LMATDT').is_between(date(2010,7,20), date(2011,5,15))),
        ('L', pl.col('LMATDT').is_between(date(2011,5,16), date(2012,1,15))),
        ('M', pl.col('LMATDT').is_between(date(2012,1,16), date(2013,6,15))),
    ]
    
    # Process each period
    for suffix, cond in periods:
        summarize_by_period(fd, 'LMATDT', cond, suffix, mv)
    
    # O period (16JUN13-15SEP16)
    summarize_by_period(fdo, 'LMATDT', 
                       pl.col('LMATDT').is_between(date(2013,6,16), date(2016,9,15)), 'O', mv)
    
    # P period (16SEP16 onwards)
    summarize_by_period(fdo, 'LMATDT', pl.col('LMATDT') >= date(2016,9,16), 'P', mv)
    
    # Special product summaries
    for df, suffix in [(fdw,'W'), (fdv,'V'), (fdg,'G'), (fdh,'H')]:
        cnt = df.unique(['BRANCH','INTPLAN','ACCTNO']).group_by(['BRANCH','INTPLAN']).len().rename({'len':'FDINO'})
        sum_df = df.group_by(['BRANCH','INTPLAN','LMATDT','ACCTNO','CUSTCD']).agg(pl.col('CURBAL').sum().alias('FDI'))
        sum_cnt = sum_df.group_by(['BRANCH','INTPLAN']).len().rename({'len':'FDINO2'})
        sum_bal = sum_df.group_by(['BRANCH','INTPLAN']).agg(pl.col('FDI').sum())
        
        result = cnt.join(sum_bal, on=['BRANCH','INTPLAN'], how='outer').join(sum_cnt, on=['BRANCH','INTPLAN'], how='outer')
        result = result.with_columns([
            pl.lit(mv['REPTDATE_SAS']).alias('REPTDATE'),
            pl.lit('').alias('RN')  # RN not used for these
        ])
        
        globals()[f"DYIBU{suffix}"] = result

def mis_append(mv):
    """Handle deletion and appending logic"""
    rep_day = int(mv['REPTDAY'])
    rep_sas = mv['REPTDATE_SAS']
    rep_mon = mv['REPTMON']
    
    suffixes = ['F','B','A','N','Z','X','S','Y','K','L','M','O','P','W','V','G','H']
    
    if rep_day == 1:
        # Delete all existing files for this month
        for s in suffixes:
            f = MIS_DIR / f"DYIBU{s}{rep_mon}.parquet"
            if f.exists(): f.unlink()
    else:
        # Remove current date records
        for s in suffixes:
            f = MIS_DIR / f"DYIBU{s}{rep_mon}.parquet"
            if f.exists():
                df = pl.read_parquet(f).filter(pl.col('REPTDATE') != rep_sas)
                df.write_parquet(f)
    
    # Append new data
    for s in suffixes:
        src = globals().get(f"DYIBU{s}")
        if src is not None and len(src) > 0:
            dst = MIS_DIR / f"DYIBU{s}{rep_mon}.parquet"
            if dst.exists():
                existing = pl.read_parquet(dst)
                combined = pl.concat([existing, src]).unique()
                combined.write_parquet(dst)
            else:
                src.write_parquet(dst)
            print(f"Appended to DYIBU{s}{rep_mon}.parquet")

def main():
    print(f"Starting EIIDFALE at {datetime.now()}")
    
    fd, fdw, fdv, fdg, fdh, fdo, mv = process_fd()
    process_all_periods(fd, fdw, fdv, fdg, fdh, fdo, mv)
    mis_append(mv)
    
    print(f"Completed at {datetime.now()}")

if __name__ == "__main__":
    main()
