# ============================================
# EIBMRNID for SAS7BDAT - Fixed Date Conversion
# ============================================

import polars as pl
import pyreadstat
from datetime import date, timedelta, datetime
from pathlib import Path

# ============================================
# CONFIGURE PATHS
# ============================================

NID_FILE = "/stgsrcsys/host/uat/rnidm09.sas7bdat"  # Your NID file
TRNCH_FILE = None                                  # TRNCH file if available
OUTPUT_DIR = "eibmrnid_output"                    # Output folder

# ============================================
# HELPER FUNCTIONS
# ============================================

def convert_sas_date(sas_num):
    """Convert SAS numeric date (days since 1960-01-01) to Python date"""
    if sas_num is None:
        return None
    try:
        return date(1960, 1, 1) + timedelta(days=int(float(sas_num)))
    except:
        return None

def calc_remmth(matdt, reptdate):
    """Calculate remaining months - SAS exact logic"""
    if matdt is None or matdt <= reptdate:
        return None
    
    if (matdt - reptdate).days < 8:
        return 0.1
    
    mdyr, mdmth, mdday = matdt.year, matdt.month, matdt.day
    rpyear, rpmonth, rpday = reptdate.year, reptdate.month, reptdate.day
    
    # Days in month with leap year
    month_days = [31, 29 if mdyr % 4 == 0 else 28, 31, 30, 31, 30, 
                  31, 31, 30, 31, 30, 31]
    
    mdday = min(mdday, month_days[mdmth - 1])
    
    remy = mdyr - rpyear
    remm = mdmth - rpmonth
    remd = mdday - rpday
    
    if remd < 0:
        remm -= 1
        if remm < 0:
            remy -= 1
            remm += 12
        remd += month_days[(mdmth - 2) % 12]
    
    return remy * 12 + remm + remd / month_days[mdmth - 1]

# ============================================
# MAIN PROCESSING
# ============================================

def main():
    print("=" * 60)
    print("EIBMRNID SAS7BDAT PROCESSOR")
    print("=" * 60)
    
    # Set report date
    today = date.today()
    reptdate = date(today.year, today.month, 1) - timedelta(days=1)
    startdte = date(reptdate.year, reptdate.month, 1)
    
    print(f"Report Date: {reptdate.strftime('%d/%m/%Y')}")
    print(f"NID File: {NID_FILE}")
    
    if not Path(NID_FILE).exists():
        print(f"❌ ERROR: File not found: {NID_FILE}")
        return
    
    # 1. READ SAS FILE
    print("\n📂 Reading SAS file...")
    df_nid, _ = pyreadstat.read_sas7bdat(NID_FILE)
    df = pl.from_pandas(df_nid)
    print(f"✅ Loaded: {len(df):,} records")
    
    # 2. STANDARDIZE COLUMNS
    col_map = {}
    for col in df.columns:
        cl = col.lower()
        if 'tranche' in cl: col_map[col] = 'trancheno'
        elif 'curbal' in cl: col_map[col] = 'curbal'
        elif 'nidstat' in cl: col_map[col] = 'nidstat'
        elif 'cdstat' in cl: col_map[col] = 'cdstat'
        elif 'matdt' in cl: col_map[col] = 'matdt'
        elif 'startdt' in cl: col_map[col] = 'startdt'
        elif 'early' in cl: col_map[col] = 'early_wddt'
        elif 'bid' in cl: col_map[col] = 'intplrate_bid'
        elif 'offer' in cl: col_map[col] = 'intplrate_offer'
    
    if col_map:
        df = df.rename(col_map)
        print(f"✅ Standardized {len(col_map)} columns")
    
    # 3. CONVERT SAS DATES
    print("\n📅 Converting SAS dates...")
    for col in ['matdt', 'startdt', 'early_wddt']:
        if col in df.columns and df[col].dtype in [pl.Int64, pl.Float64]:
            df = df.with_columns(
                pl.col(col).map_elements(
                    convert_sas_date,
                    return_dtype=pl.Date
                ).alias(col)
            )
            print(f"  Converted {col}")
    
    # 4. ADD REPORT DATES & FILTER
    df = df.with_columns([
        pl.lit(reptdate).alias('reptdate'),
        pl.lit(startdte).alias('startdte')
    ])
    
    df = df.filter(pl.col('curbal') > 0)
    print(f"✅ After positive balance filter: {len(df):,} records")
    
    # 5. CALCULATE REMAINING MONTHS
    print("\n🧮 Calculating remaining months...")
    df = df.with_columns(
        pl.col('matdt').map_elements(
            lambda x: calc_remmth(x, reptdate),
            return_dtype=pl.Float64
        ).alias('remmth')
    )
    
    # 6. APPLY SAS FORMATS
    print("\n🎯 Applying SAS formats...")
    
    # REMFMTA format
    FMTA = {
        (-float('inf'), 6): '1. LE  6      ',
        (6, 12): '2. GT  6 TO 12',
        (12, 24): '3. GT 12 TO 24',
        (24, 36): '4. GT 24 TO 36',
        (36, 60): '5. GT 36 TO 60',
        'other': '              '
    }
    
    def apply_fmt(val):
        if val is None or pl.is_nan(val):
            return FMTA['other']
        for (low, high), label in FMTA.items():
            if low == 'other':
                continue
            if low <= val < high:
                return label
        return FMTA['other']
    
    # 7. CREATE TABLE 1
    print("\n📊 Creating Table 1...")
    tbl1 = df.filter(
        (pl.col('matdt') > pl.col('reptdate')) &
        (pl.col('startdt') <= pl.col('reptdate')) &
        (pl.col('nidstat') == 'N') &
        (pl.col('cdstat') == 'A')
    ).with_columns([
        pl.lit(0).alias('heldmkt'),
        (pl.col('curbal') - pl.col('heldmkt')).alias('outstanding'),
        pl.col('remmth').map_elements(apply_fmt, return_dtype=pl.Utf8).alias('remmfmt')
    ])
    
    print(f"✅ Table 1 records: {len(tbl1):,}")
    
    # 8. CREATE TABLE 2
    print("\n📊 Creating Table 2...")
    tbl2_stats = df.filter(
        (pl.col('nidstat') == 'E') &
        (pl.col('early_wddt') >= startdte) &
        (pl.col('early_wddt') <= reptdate)
    ).select([
        pl.len().alias('nidcnt'),
        pl.sum('curbal').alias('nidvol')
    ]).row(0)
    
    print(f"✅ Table 2 count: {tbl2_stats['nidcnt']:,}")
    
    # 9. CREATE TABLE 3
    print("\n📊 Creating Table 3...")
    if 'intplrate_bid' in df.columns and 'intplrate_offer' in df.columns:
        overall_yield = df.filter(
            (pl.col('nidstat') == 'N') &
            (pl.col('cdstat') == 'A') &
            pl.col('intplrate_bid').is_not_null() &
            pl.col('intplrate_offer').is_not_null()
        ).select(
            ((pl.col('intplrate_bid') + pl.col('intplrate_offer')) / 2).mean()
        ).row(0)[0] or 0
        print(f"✅ Overall yield: {overall_yield:.4f}%")
    else:
        overall_yield = 0
        print("⚠️  Rate columns not found")
    
    # 10. CREATE SUMMARY
    print("\n📈 Creating summaries...")
    tbl1_sum = tbl1.group_by('remmfmt').agg([
        pl.sum('curbal').alias('curbal'),
        pl.sum('heldmkt').alias('heldmkt'),
        pl.sum('outstanding').alias('outstanding'),
        pl.len().alias('count')
    ]).sort('remmfmt')
    
    # 11. SAVE TO PARQUET
    print("\n💾 Saving to Parquet...")
    out_dir = Path(OUTPUT_DIR)
    out_dir.mkdir(exist_ok=True)
    
    base = f"EIBMRNID_{reptdate.strftime('%Y%m')}"
    
    # Save files
    tbl1.write_parquet(out_dir / f"{base}_TABLE1.parquet")
    tbl1_sum.write_parquet(out_dir / f"{base}_SUMMARY.parquet")
    
    # Save metadata
    meta = pl.DataFrame({
        'report_date': [reptdate],
        'nid_file': [Path(NID_FILE).name],
        'processed_at': [datetime.now()],
        'table1_records': [len(tbl1)],
        'table2_count': [tbl2_stats['nidcnt']],
        'overall_yield': [overall_yield]
    })
    meta.write_parquet(out_dir / f"{base}_METADATA.parquet")
    
    # 12. PRINT RESULTS
    print("=" * 60)
    print("✅ PROCESSING COMPLETE")
    print("=" * 60)
    
    files = list(out_dir.glob(f"{base}*.parquet"))
    for f in files:
        size_kb = f.stat().st_size / 1024
        print(f"📄 {f.name} ({size_kb:.1f} KB)")
    
    print(f"\n📊 SUMMARY:")
    print(f"  • Table 1 records: {len(tbl1):,}")
    print(f"  • Table 2 count: {tbl2_stats['nidcnt']:,}")
    print(f"  • Overall yield: {overall_yield:.4f}%")
    print(f"\n📁 Output folder: {out_dir.absolute()}")

# ============================================
# RUN
# ============================================

if __name__ == "__main__":
    main()
