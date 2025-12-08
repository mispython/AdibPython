# ============================================
# EIBMRNID for SAS7BDAT - Fixed Duplicate Columns
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
    
    # Show original columns
    print(f"\n📋 Original columns ({len(df.columns)}):")
    for i, col in enumerate(df.columns, 1):
        print(f"  {i:3}. {col}")
    
    # 2. FIX DUPLICATE COLUMNS FIRST
    print("\n🔧 Checking for duplicate columns...")
    col_counts = {}
    for col in df.columns:
        col_counts[col] = col_counts.get(col, 0) + 1
    
    duplicates = [col for col, count in col_counts.items() if count > 1]
    if duplicates:
        print(f"⚠️  Found duplicate columns: {duplicates}")
        print("  Renaming duplicates...")
        
        # Rename duplicates
        seen = {}
        new_cols = []
        for col in df.columns:
            if col in seen:
                seen[col] += 1
                new_name = f"{col}_{seen[col]}"
                new_cols.append(new_name)
            else:
                seen[col] = 1
                new_cols.append(col)
        
        df = df.rename(dict(zip(df.columns, new_cols)))
        print("✅ Duplicates renamed")
    
    # 3. STANDARDIZE COLUMNS (carefully)
    print("\n🔧 Standardizing column names...")
    
    # First, let's see what columns we actually have
    col_map = {}
    for col in df.columns:
        cl = col.lower()
        # Only rename if we haven't already mapped this standard name
        if 'tranche' in cl and 'trancheno' not in col_map.values():
            col_map[col] = 'trancheno'
        elif ('curbal' in cl or 'current' in cl) and 'curbal' not in col_map.values():
            col_map[col] = 'curbal'
        elif 'nidstat' in cl and 'nidstat' not in col_map.values():
            col_map[col] = 'nidstat'
        elif 'cdstat' in cl and 'cdstat' not in col_map.values():
            col_map[col] = 'cdstat'
        elif ('matdt' in cl or 'maturity' in cl) and 'matdt' not in col_map.values():
            col_map[col] = 'matdt'
        elif ('startdt' in cl or 'start' in cl) and 'startdt' not in col_map.values():
            col_map[col] = 'startdt'
        elif ('early' in cl and 'wddt' in cl) and 'early_wddt' not in col_map.values():
            col_map[col] = 'early_wddt'
        elif ('intplrate' in cl or 'bid' in cl) and 'intplrate_bid' not in col_map.values():
            col_map[col] = 'intplrate_bid'
        elif ('intplrate' in cl or 'offer' in cl) and 'intplrate_offer' not in col_map.values():
            col_map[col] = 'intplrate_offer'
    
    if col_map:
        print(f"✅ Will rename {len(col_map)} columns:")
        for old, new in col_map.items():
            print(f"  {old} → {new}")
        
        # Rename only unique mappings
        df = df.rename(col_map)
    else:
        print("⚠️  No standard columns found for renaming")
    
    # Show final columns
    print(f"\n📋 Final columns ({len(df.columns)}):")
    for i, col in enumerate(df.columns, 1):
        dtype = str(df[col].dtype)
        print(f"  {i:3}. {col:30} {dtype}")
    
    # 4. CHECK REQUIRED COLUMNS
    required = ['trancheno', 'curbal', 'nidstat', 'cdstat', 'matdt', 'startdt']
    missing = [col for col in required if col not in df.columns]
    
    if missing:
        print(f"\n⚠️  Missing required columns: {missing}")
        print("   Available columns:", list(df.columns))
        
        # Try to find alternatives
        for col in missing:
            alternatives = [c for c in df.columns if col in c.lower()]
            if alternatives:
                print(f"   Possible alternatives for {col}: {alternatives}")
        
        # Exit if critical columns missing
        if 'curbal' not in df.columns or 'matdt' not in df.columns:
            print("❌ Critical columns missing, cannot proceed")
            return
    
    # 5. CONVERT SAS DATES
    print("\n📅 Converting SAS dates...")
    date_cols_converted = 0
    for col in ['matdt', 'startdt', 'early_wddt']:
        if col in df.columns:
            if df[col].dtype in [pl.Int64, pl.Float64, pl.Int32, pl.Float32]:
                df = df.with_columns(
                    pl.col(col).map_elements(
                        convert_sas_date,
                        return_dtype=pl.Date
                    ).alias(col)
                )
                print(f"  Converted {col} (numeric → date)")
                date_cols_converted += 1
            elif df[col].dtype == pl.Date:
                print(f"  {col} already a date")
            else:
                print(f"  {col} has dtype {df[col].dtype}")
    
    if date_cols_converted == 0:
        print("⚠️  No date columns were converted")
        print("   Date columns might be in unexpected format")
    
    # 6. ADD REPORT DATES & FILTER
    df = df.with_columns([
        pl.lit(reptdate).alias('reptdate'),
        pl.lit(startdte).alias('startdte')
    ])
    
    # Filter positive balance
    if 'curbal' in df.columns:
        initial_count = len(df)
        df = df.filter(pl.col('curbal') > 0)
        filtered_count = len(df)
        print(f"\n💰 Positive balance filter: {initial_count:,} → {filtered_count:,} records")
    else:
        print("⚠️  curbal column not found, skipping balance filter")
    
    # 7. CALCULATE REMAINING MONTHS
    print("\n🧮 Calculating remaining months...")
    if 'matdt' in df.columns:
        df = df.with_columns(
            pl.col('matdt').map_elements(
                lambda x: calc_remmth(x, reptdate),
                return_dtype=pl.Float64
            ).alias('remmth')
        )
        print("✅ Remaining months calculated")
    else:
        print("❌ matdt column not found, cannot calculate remaining months")
        return
    
    # 8. APPLY SAS FORMATS
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
    
    # 9. CREATE TABLE 1
    print("\n📊 Creating Table 1...")
    
    # Check all required columns for Table 1
    tbl1_required = ['matdt', 'startdt', 'reptdate', 'nidstat', 'cdstat', 'remmth']
    tbl1_missing = [col for col in tbl1_required if col not in df.columns]
    
    if tbl1_missing:
        print(f"⚠️  Missing columns for Table 1: {tbl1_missing}")
        tbl1 = pl.DataFrame()
    else:
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
    
    # 10. CREATE TABLE 2
    print("\n📊 Creating Table 2...")
    
    tbl2_count = 0
    tbl2_volume = 0
    
    if all(col in df.columns for col in ['nidstat', 'early_wddt', 'curbal']):
        tbl2_df = df.filter(
            (pl.col('nidstat') == 'E') &
            (pl.col('early_wddt') >= startdte) &
            (pl.col('early_wddt') <= reptdate)
        )
        tbl2_count = len(tbl2_df)
        tbl2_volume = tbl2_df['curbal'].sum() if tbl2_count > 0 else 0
    
    print(f"✅ Table 2 count: {tbl2_count:,}, volume: {tbl2_volume:,.2f}")
    
    # 11. CREATE TABLE 3
    print("\n📊 Creating Table 3...")
    
    overall_yield = 0
    if all(col in df.columns for col in ['intplrate_bid', 'intplrate_offer', 'nidstat', 'cdstat']):
        yield_df = df.filter(
            (pl.col('nidstat') == 'N') &
            (pl.col('cdstat') == 'A') &
            pl.col('intplrate_bid').is_not_null() &
            pl.col('intplrate_offer').is_not_null()
        )
        
        if len(yield_df) > 0:
            overall_yield = yield_df.select(
                ((pl.col('intplrate_bid') + pl.col('intplrate_offer')) / 2).mean()
            ).row(0)[0] or 0
    
    print(f"✅ Overall yield: {overall_yield:.4f}%")
    
    # 12. CREATE SUMMARY
    print("\n📈 Creating summaries...")
    
    if len(tbl1) > 0:
        tbl1_sum = tbl1.group_by('remmfmt').agg([
            pl.sum('curbal').alias('curbal'),
            pl.sum('heldmkt').alias('heldmkt'),
            pl.sum('outstanding').alias('outstanding'),
            pl.len().alias('count')
        ]).sort('remmfmt')
    else:
        tbl1_sum = pl.DataFrame()
        print("⚠️  No data for Table 1 summary")
    
    # 13. SAVE TO PARQUET
    print("\n💾 Saving to Parquet...")
    out_dir = Path(OUTPUT_DIR)
    out_dir.mkdir(exist_ok=True)
    
    base = f"EIBMRNID_{reptdate.strftime('%Y%m')}"
    
    files_saved = []
    
    # Save Table 1 if we have data
    if len(tbl1) > 0:
        tbl1_path = out_dir / f"{base}_TABLE1.parquet"
        tbl1.write_parquet(tbl1_path)
        files_saved.append(tbl1_path)
        print(f"  Saved: {tbl1_path.name}")
    
    # Save summary if we have data
    if len(tbl1_sum) > 0:
        summary_path = out_dir / f"{base}_SUMMARY.parquet"
        tbl1_sum.write_parquet(summary_path)
        files_saved.append(summary_path)
        print(f"  Saved: {summary_path.name}")
    
    # Save metadata
    meta_path = out_dir / f"{base}_METADATA.parquet"
    meta = pl.DataFrame({
        'report_date': [reptdate],
        'nid_file': [Path(NID_FILE).name],
        'processed_at': [datetime.now()],
        'table1_records': [len(tbl1)],
        'table2_count': [tbl2_count],
        'table2_volume': [tbl2_volume],
        'overall_yield': [overall_yield],
        'total_records': [len(df)]
    })
    meta.write_parquet(meta_path)
    files_saved.append(meta_path)
    print(f"  Saved: {meta_path.name}")
    
    # 14. PRINT RESULTS
    print("\n" + "=" * 60)
    print("✅ PROCESSING COMPLETE")
    print("=" * 60)
    
    if files_saved:
        for f in files_saved:
            size_kb = f.stat().st_size / 1024
            print(f"📄 {f.name} ({size_kb:.1f} KB)")
        
        print(f"\n📊 SUMMARY:")
        print(f"  • Table 1 records: {len(tbl1):,}")
        print(f"  • Table 2 count: {tbl2_count:,}")
        print(f"  • Table 2 volume: {tbl2_volume:,.2f}")
        print(f"  • Overall yield: {overall_yield:.4f}%")
        print(f"  • Total processed: {len(df):,}")
        print(f"\n📁 Output folder: {out_dir.absolute()}")
    else:
        print("⚠️  No files were saved")

# ============================================
# RUN
# ============================================

if __name__ == "__main__":
    main()
