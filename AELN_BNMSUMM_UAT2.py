# ============================================
# EIBMRNID for VS Code - Simple Version
# ============================================

import polars as pl
from datetime import date, timedelta
from pathlib import Path

# ============================================
# 1. CONFIGURE YOUR FILE PATHS HERE
# ============================================

# Put your SAS file paths here
NID_FILE = "rnidm09.sas7bdat"      # ← CHANGE THIS to your NID file
TRNCH_FILE = "trnchm09.sas7bdat"   # ← CHANGE THIS to your TRNCH file (optional)
OUTPUT_DIR = "eibmrnid_output"     # ← Output folder name

# Optional: Set specific report date (YYYY-MM-DD)
# REPORT_DATE = "2024-09-30"  # Uncomment and set if needed
REPORT_DATE = None  # Will use last day of previous month

# ============================================
# 2. MAIN FUNCTION - NO NEED TO CHANGE BELOW
# ============================================

def main():
    """Run EIBMRNID with your configured paths"""
    
    print("=" * 60)
    print("EIBMRNID Report Generator")
    print("=" * 60)
    
    # Set report date
    if REPORT_DATE:
        reptdate = date.fromisoformat(REPORT_DATE)
    else:
        today = date.today()
        reptdate = date(today.year, today.month, 1) - timedelta(days=1)
    
    startdte = date(reptdate.year, reptdate.month, 1)
    
    print(f"Report Date: {reptdate.strftime('%d/%m/%Y')}")
    print(f"Start Date: {startdte.strftime('%d/%m/%Y')}")
    print(f"NID File: {NID_FILE}")
    
    # Check if files exist
    if not Path(NID_FILE).exists():
        print(f"\n❌ ERROR: NID file not found: {NID_FILE}")
        print("Please check the file path in line 12")
        return
    
    if TRNCH_FILE and not Path(TRNCH_FILE).exists():
        print(f"\n⚠️  Warning: TRNCH file not found: {TRNCH_FILE}")
        print("Continuing without TRNCH file...")
        trnch_path = None
    else:
        trnch_path = TRNCH_FILE
    
    # Run the processing
    try:
        result = process_eibmrnid(NID_FILE, trnch_path, OUTPUT_DIR, reptdate)
        
        if result['success']:
            print(f"\n✅ SUCCESS! Report generated in: {OUTPUT_DIR}/")
            print(f"\nOutput files:")
            for file in result['output_files']:
                size_kb = file.stat().st_size / 1024
                print(f"  • {file.name} ({size_kb:.1f} KB)")
            
            # Show quick summary
            print(f"\n📊 Summary:")
            print(f"  Table 1 records: {result['table1_records']:,}")
            print(f"  Table 2 count: {result['table2_count']:,}")
            if result['overall_yield'] > 0:
                print(f"  Overall yield: {result['overall_yield']:.4f}%")
            
            # Open output folder
            import os
            os.startfile(OUTPUT_DIR) if os.name == 'nt' else os.system(f'open {OUTPUT_DIR}')
            
        else:
            print(f"\n❌ Processing failed: {result.get('error', 'Unknown error')}")
            
    except Exception as e:
        print(f"\n❌ ERROR: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()

# ============================================
# 3. PROCESSING FUNCTIONS
# ============================================

def process_eibmrnid(nid_path, trnch_path, output_dir, reptdate):
    """Core EIBMRNID processing logic"""
    
    print("\n📂 Reading SAS files...")
    
    # Read SAS file
    try:
        import pyreadstat
        df_nid, _ = pyreadstat.read_sas7bdat(nid_path)
        df_nid = pl.from_pandas(df_nid)
    except ImportError:
        print("Installing pyreadstat...")
        import subprocess
        subprocess.check_call([sys.executable, "-m", "pip", "install", "pyreadstat"])
        import pyreadstat
        df_nid, _ = pyreadstat.read_sas7bdat(nid_path)
        df_nid = pl.from_pandas(df_nid)
    
    # Fix column names
    for col in df_nid.columns:
        cl = col.lower()
        if 'tranche' in cl: df_nid = df_nid.rename({col: 'trancheno'})
        elif 'curbal' in cl: df_nid = df_nid.rename({col: 'curbal'})
        elif 'nidstat' in cl: df_nid = df_nid.rename({col: 'nidstat'})
        elif 'cdstat' in cl: df_nid = df_nid.rename({col: 'cdstat'})
        elif 'matdt' in cl: df_nid = df_nid.rename({col: 'matdt'})
        elif 'startdt' in cl: df_nid = df_nid.rename({col: 'startdt'})
    
    # Merge with TRNCH if exists
    if trnch_path:
        df_trnch, _ = pyreadstat.read_sas7bdat(trnch_path)
        df_trnch = pl.from_pandas(df_trnch)
        for col in df_trnch.columns:
            if 'tranche' in col.lower():
                df_trnch = df_trnch.rename({col: 'trancheno'})
                break
        df = df_nid.join(df_trnch, on='trancheno', how='left')
    else:
        df = df_nid
    
    # Basic filter
    df = df.filter(pl.col('curbal') > 0)
    
    print(f"✅ Loaded {len(df):,} records")
    
    # Calculate remaining months (SAS logic)
    def calc_remmth(matdt):
        if not matdt or matdt <= reptdate: return None
        if (matdt - reptdate).days < 8: return 0.1
        
        y1,m1,d1 = matdt.year, matdt.month, matdt.day
        y2,m2,d2 = reptdate.year, reptdate.month, reptdate.day
        
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
    
    # Add calculation
    df = df.with_columns(
        pl.col('matdt').map_elements(calc_remmth).alias('remmth')
    )
    
    # Format A (REMFMTA)
    FMTA = {(-1e9,6):'1. LE  6      ',(6,12):'2. GT  6 TO 12',
            (12,24):'3. GT 12 TO 24',(24,36):'4. GT 24 TO 36',
            (36,60):'5. GT 36 TO 60','other':'              '}
    
    def fmt(v,f):
        if not v: return f['other']
        for (l,h),lab in f.items():
            if l!='other' and l<=v<h: return lab
        return f['other']
    
    # Table 1
    tbl1 = df.filter(pl.col('nidstat')=='N').with_columns([
        pl.lit(0).alias('heldmkt'),
        (pl.col('curbal')-pl.col('heldmkt')).alias('outstanding'),
        pl.col('remmth').map_elements(lambda x: fmt(x, FMTA)).alias('remmfmt')
    ])
    
    # Table 2
    tbl2_stats = df.filter(pl.col('nidstat')=='E').select([
        pl.len().alias('nidcnt'),
        pl.sum('curbal').alias('nidvol')
    ]).row(0)
    
    # Table 3 (if rate columns exist)
    if 'intplrate_bid' in df.columns and 'intplrate_offer' in df.columns:
        tbl3 = df.filter(pl.col('nidstat')=='N').with_columns([
            ((pl.col('intplrate_bid') + pl.col('intplrate_offer')) / 2).alias('yield')
        ])
        overall_yield = tbl3.filter(pl.col('yield')>0).select(pl.mean('yield')).row(0)[0] or 0
    else:
        overall_yield = 0
    
    # Summary
    tbl1_sum = tbl1.group_by('remmfmt').agg([
        pl.sum('curbal').alias('curbal'),
        pl.sum('outstanding').alias('outstanding'),
        pl.len().alias('count')
    ]).sort('remmfmt')
    
    # Save to Parquet
    out_dir = Path(output_dir)
    out_dir.mkdir(exist_ok=True)
    
    base = f"EIBMRNID_{reptdate.strftime('%Y%m')}"
    
    # Save files
    tbl1.write_parquet(out_dir / f"{base}_TABLE1.parquet")
    tbl1_sum.write_parquet(out_dir / f"{base}_SUMMARY.parquet")
    
    # Metadata
    meta = pl.DataFrame({
        'report_date': [reptdate],
        'nid_file': [Path(nid_path).name],
        'trnch_file': [Path(trnch_path).name if trnch_path else None],
        'processed_at': [date.today()],
        'table1_records': [len(tbl1)],
        'table2_count': [tbl2_stats['nidcnt']],
        'overall_yield': [overall_yield]
    })
    meta.write_parquet(out_dir / f"{base}_METADATA.parquet")
    
    return {
        'success': True,
        'output_files': list(out_dir.glob(f"{base}*.parquet")),
        'table1_records': len(tbl1),
        'table2_count': tbl2_stats['nidcnt'],
        'overall_yield': overall_yield
    }

# ============================================
# 4. RUN IT!
# ============================================

if __name__ == "__main__":
    # Check if dependencies are installed
    try:
        import pyreadstat
        import polars as pl
    except ImportError:
        print("Installing required packages...")
        import subprocess
        import sys
        subprocess.check_call([sys.executable, "-m", "pip", "install", "pyreadstat", "polars"])
        print("Packages installed. Please run the script again.")
        sys.exit(0)
    
    # Run the main function
    main()
