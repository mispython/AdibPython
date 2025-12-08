import polars as pl
from datetime import date, timedelta
from pathlib import Path
import pyreadstat

# ============================================
# CORE EIBMRNID LOGIC
# ============================================

def eibmrnid_sas(nid_file, trnch_file=None, output_dir='.', rep_date=None):
    """EIBMRNID report from SAS7BDAT - follows original SAS logic"""
    
    # 1. Set report date
    rd = rep_date or (date.today().replace(day=1) - timedelta(days=1))
    sd = date(rd.year, rd.month, 1)
    
    print(f"EIBMRNID Report: {rd.strftime('%d/%m/%Y')}")
    
    # 2. Read SAS7BDAT files
    df_nid = pl.from_pandas(pyreadstat.read_sas7bdat(nid_file)[0])
    
    # Fix column names
    col_map = {}
    for c in df_nid.columns:
        cl = c.lower()
        if 'tranche' in cl: col_map[c] = 'trancheno'
        elif 'curbal' in cl: col_map[c] = 'curbal'
        elif 'nidstat' in cl: col_map[c] = 'nidstat'
        elif 'cdstat' in cl: col_map[c] = 'cdstat'
        elif 'matdt' in cl: col_map[c] = 'matdt'
        elif 'startdt' in cl: col_map[c] = 'startdt'
        elif 'early' in cl: col_map[c] = 'early_wddt'
        elif 'bid' in cl: col_map[c] = 'intplrate_bid'
        elif 'offer' in cl: col_map[c] = 'intplrate_offer'
    
    df_nid = df_nid.rename(col_map) if col_map else df_nid
    
    # Merge with TRNCH if exists
    if trnch_file and Path(trnch_file).exists():
        df_trnch = pl.from_pandas(pyreadstat.read_sas7bdat(trnch_file)[0])
        for c in df_trnch.columns:
            cl = c.lower()
            if 'tranche' in cl: 
                df_trnch = df_trnch.rename({c: 'trancheno'})
                break
        df = df_nid.join(df_trnch, on='trancheno', how='left')
    else:
        df = df_nid
    
    # Filter positive balance
    df = df.filter(pl.col('curbal') > 0)
    
    # Convert dates (SAS numeric to Python date)
    for col in ['matdt','startdt','early_wddt']:
        if col in df.columns and df[col].dtype in [pl.Int64, pl.Float64]:
            df = df.with_columns(
                (pl.col(col) + pl.date(1960,1,1).timestamp()/86400).cast(pl.Date)
            )
    
    # Add report dates
    df = df.with_columns([
        pl.lit(rd).alias('reptdate'),
        pl.lit(sd).alias('startdte')
    ])
    
    # 3. Apply SAS logic
    
    # Filter active records
    active = df.filter(
        (pl.col('matdt') > pl.col('reptdate')) &
        (pl.col('startdt') <= pl.col('reptdate'))
    )
    
    # Calculate remaining months (SAS logic)
    def remmth_calc(matdt):
        if not matdt or matdt <= rd: return None
        if (matdt - rd).days < 8: return 0.1
        
        y1,m1,d1 = matdt.year, matdt.month, matdt.day
        y2,m2,d2 = rd.year, rd.month, rd.day
        
        month_days = [31,29 if y1%4==0 else 28,31,30,31,30,31,31,30,31,30,31]
        d1 = min(d1, month_days[m1-1])
        
        remy = y1 - y2
        remm = m1 - m2
        remd = d1 - d2
        
        if remd < 0:
            remm -= 1
            if remm < 0: remy -= 1; remm += 12
            remd += month_days[(m1-2)%12]
        
        return remy*12 + remm + remd/month_days[m1-1]
    
    active = active.with_columns(
        pl.col('matdt').map_elements(remmth_calc).alias('remmth')
    )
    
    # Format definitions (REMFMTA & REMFMTB)
    FMTA = {(-1e9,6):'1. LE  6      ',(6,12):'2. GT  6 TO 12',
            (12,24):'3. GT 12 TO 24',(24,36):'4. GT 24 TO 36',
            (36,60):'5. GT 36 TO 60','other':'              '}
    
    FMTB = {(-1e9,1):'1. LE  1      ',(1,3):'2. GT  1 TO  3',
            (3,6):'3. GT  3 TO  6',(6,9):'4. GT  6 TO  9',
            (9,12):'5. GT  9 TO 12',(12,24):'6. GT 12 TO 24',
            (24,36):'7. GT 24 TO 36',(36,60):'8. GT 36 TO 60',
            'other':'             '}
    
    def apply_fmt(val, fmt):
        if not val or val != val: return fmt['other']
        for (l,h),lab in fmt.items():
            if l != 'other' and l <= val < h: return lab
        return fmt['other']
    
    # Table 1: Outstanding NID
    tbl1 = active.filter(
        (pl.col('nidstat') == 'N') & (pl.col('cdstat') == 'A')
    ).with_columns([
        pl.lit(0).alias('heldmkt'),
        (pl.col('curbal') - pl.col('heldmkt')).alias('outstanding'),
        pl.col('remmth').map_elements(lambda x: apply_fmt(x, FMTA)).alias('remmfmt')
    ])
    
    # Table 2: Monthly Trading
    tbl2_stats = df.filter(
        (pl.col('nidstat') == 'E') &
        (pl.col('early_wddt') >= sd) &
        (pl.col('early_wddt') <= rd)
    ).select([
        pl.len().alias('nidcnt'),
        pl.sum('curbal').alias('nidvol')
    ]).row(0)
    
    # Table 3: Mid Yield
    if 'intplrate_bid' in active.columns and 'intplrate_offer' in active.columns:
        tbl3 = active.filter(
            (pl.col('nidstat') == 'N') & (pl.col('cdstat') == 'A')
        ).with_columns([
            ((pl.col('intplrate_bid') + pl.col('intplrate_offer')) / 2).alias('yield'),
            pl.col('remmth').map_elements(lambda x: apply_fmt(x, FMTB)).alias('remmfmt')
        ]).unique(subset=['remmfmt', 'trancheno'])
        
        tbl3_sum = tbl3.filter(pl.col('yield') > 0).group_by('remmfmt').agg(
            pl.mean('yield').alias('midyield')
        ).sort('remmfmt')
        overall = tbl3.filter(pl.col('yield') > 0).select(pl.mean('yield')).row(0)[0] or 0
    else:
        tbl3_sum = pl.DataFrame()
        overall = 0
    
    # Table 1 summary
    tbl1_sum = tbl1.group_by('remmfmt').agg([
        pl.sum('curbal').alias('curbal'),
        pl.sum('heldmkt').alias('heldmkt'),
        pl.sum('outstanding').alias('outstanding')
    ]).sort('remmfmt')
    
    # 4. Save Parquet files
    Path(output_dir).mkdir(exist_ok=True)
    base = f"EIBMRNID_{rd.strftime('%Y%m')}"
    
    # Save main tables
    tbl1.write_parquet(f"{output_dir}/{base}_TABLE1.parquet")
    
    # Save summaries
    tbl1_sum.write_parquet(f"{output_dir}/{base}_TABLE1_SUMMARY.parquet")
    
    if not tbl3_sum.is_empty():
        tbl3_sum.write_parquet(f"{output_dir}/{base}_TABLE3_SUMMARY.parquet")
    
    # Metadata
    meta = pl.DataFrame({
        'report_date': [rd],
        'nid_file': [Path(nid_file).name],
        'trnch_file': [Path(trnch_file).name if trnch_file else None],
        'table1_records': [len(tbl1)],
        'table2_count': [tbl2_stats['nidcnt']],
        'overall_yield': [overall]
    })
    meta.write_parquet(f"{output_dir}/{base}_METADATA.parquet")
    
    print(f"✓ Saved to: {output_dir}")
    return {'success': True, 'output_dir': output_dir}

# ============================================
# COMMAND LINE
# ============================================

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='EIBMRNID from SAS7BDAT')
    parser.add_argument('--nid', required=True, help='NID SAS7BDAT file')
    parser.add_argument('--trnch', help='TRNCH SAS7BDAT file')
    parser.add_argument('--output', default='.', help='Output directory')
    parser.add_argument('--date', help='Report date YYYY-MM-DD')
    
    args = parser.parse_args()
    
    # Check file exists
    if not Path(args.nid).exists():
        print(f"Error: File not found: {args.nid}")
        sys.exit(1)
    
    # Parse date
    rd = None
    if args.date:
        try:
            rd = date.fromisoformat(args.date)
        except:
            print("Error: Use YYYY-MM-DD format")
            sys.exit(1)
    
    # Run
    try:
        result = eibmrnid_sas(
            nid_file=args.nid,
            trnch_file=args.trnch,
            output_dir=args.output,
            rep_date=rd
        )
        if result['success']:
            sys.exit(0)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)
