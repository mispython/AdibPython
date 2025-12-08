# ============================================
# EIBMRNID for SAS7BDAT - Final Fixed Version
# ============================================

import polars as pl
import pyreadstat
from datetime import date, timedelta, datetime
from pathlib import Path

# ============================================
# CONFIGURE PATHS
# ============================================

NID_FILE = "/stgsrcsys/host/uat/rnidm09.sas7bdat"   # Your NID file
TRNCH_FILE = "/stgsrcsys/host/uat/trnchm09.sas7bdat"  # Your TRNCH file
OUTPUT_DIR = "eibmrnid_output"                     # Output folder

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
    print(f"NID File: {Path(NID_FILE).name}")
    print(f"TRNCH File: {Path(TRNCH_FILE).name}")
    
    # Check files exist
    if not Path(NID_FILE).exists():
        print(f"❌ ERROR: NID file not found: {NID_FILE}")
        return
    
    if not Path(TRNCH_FILE).exists():
        print(f"⚠️  WARNING: TRNCH file not found: {TRNCH_FILE}")
        print("  Continuing without TRNCH data...")
        use_trnch = False
    else:
        use_trnch = True
    
    # 1. READ NID SAS FILE
    print("\n📂 Reading NID file...")
    df_nid, _ = pyreadstat.read_sas7bdat(NID_FILE)
    df_nid = pl.from_pandas(df_nid)
    print(f"✅ NID loaded: {len(df_nid):,} records")
    
    # Convert NID columns to lowercase
    df_nid = df_nid.rename({col: col.lower() for col in df_nid.columns})
    
    # 2. READ TRNCH SAS FILE
    if use_trnch:
        print("📂 Reading TRNCH file...")
        df_trnch, _ = pyreadstat.read_sas7bdat(TRNCH_FILE)
        df_trnch = pl.from_pandas(df_trnch)
        print(f"✅ TRNCH loaded: {len(df_trnch):,} records")
        
        # Convert TRNCH columns to lowercase
        df_trnch = df_trnch.rename({col: col.lower() for col in df_trnch.columns})
        
        # Merge NID with TRNCH (left join on trancheno)
        print("\n🔗 Merging NID with TRNCH...")
        df = df_nid.join(df_trnch, on='trancheno', how='left')
        print(f"✅ Merged: {len(df):,} records")
    else:
        df = df_nid
    
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
            print(f"  ✓ Converted {col} (numeric → date)")
    
    # 4. ADD REPORT DATES
    df = df.with_columns([
        pl.lit(reptdate).alias('reptdate'),
        pl.lit(startdte).alias('startdte')
    ])
    
    # 5. FILTER POSITIVE BALANCE
    if 'curbal' in df.columns:
        df = df.filter(pl.col('curbal') > 0)
        print(f"\n💰 Positive balance filter: {len(df):,} records")
    else:
        print("❌ curbal column not found")
        return
    
    # 6. CALCULATE REMAINING MONTHS
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
        print("❌ matdt column not found")
        return
    
    # 7. APPLY SAS FORMATS
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
    
    # 8. CREATE TABLE 1 (Outstanding NID)
    print("\n📊 Creating Table 1 (Outstanding NID)...")
    
    # First create the filter conditions
    tbl1_filtered = df.filter(
        (pl.col('matdt') > pl.col('reptdate')) &
        (pl.col('startdt') <= pl.col('reptdate')) &
        (pl.col('nidstat') == 'N') &
        (pl.col('cdstat') == 'A')
    )
    
    # Then add columns
    tbl1 = tbl1_filtered.with_columns([
        pl.lit(0).alias('heldmkt'),  # HELDMKT = 0
        (pl.col('curbal') - pl.lit(0)).alias('outstanding'),  # OUTSTANDING = CURBAL - 0
        pl.col('remmth').map_elements(apply_fmt, return_dtype=pl.Utf8).alias('remmfmt')
    ])
    
    print(f"✅ Table 1 records: {len(tbl1):,}")
    
    # 9. CREATE TABLE 2 (Monthly Trading)
    print("\n📊 Creating Table 2 (Monthly Trading)...")
    
    if all(col in df.columns for col in ['nidstat', 'early_wddt', 'curbal']):
        tbl2_df = df.filter(
            (pl.col('nidstat') == 'E') &
            (pl.col('early_wddt') >= startdte) &
            (pl.col('early_wddt') <= reptdate)
        )
        tbl2_count = len(tbl2_df)
        tbl2_volume = tbl2_df['curbal'].sum() if tbl2_count > 0 else 0
    else:
        tbl2_count = 0
        tbl2_volume = 0
    
    print(f"✅ Table 2 count: {tbl2_count:,}, volume: RM{tbl2_volume:,.2f}")
    
    # 10. CREATE TABLE 3 (Mid Yield)
    print("\n📊 Creating Table 3 (Mid Yield)...")
    
    # Check which rate columns we have
    overall_yield = 0
    yield_source = "None"
    
    if all(col in df.columns for col in ['intplrate_bid', 'intplrate_offer']):
        # Use bid/offer rates from TRNCH
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
            yield_source = "intplrate_bid/offer"
    
    elif 'intrate' in df.columns:
        # Use INTRATE from NID
        yield_df = df.filter(
            (pl.col('nidstat') == 'N') &
            (pl.col('cdstat') == 'A') &
            pl.col('intrate').is_not_null() &
            (pl.col('intrate') > 0)
        )
        if len(yield_df) > 0:
            overall_yield = yield_df.select(pl.mean('intrate')).row(0)[0] or 0
            yield_source = "intrate"
    
    print(f"✅ Overall yield: {overall_yield:.4f}% (Source: {yield_source})")
    
    # 11. CREATE SUMMARY
    print("\n📈 Creating summaries...")
    
    if len(tbl1) > 0:
        tbl1_sum = tbl1.group_by('remmfmt').agg([
            pl.sum('curbal').alias('curbal'),
            pl.sum('heldmkt').alias('heldmkt'),
            pl.sum('outstanding').alias('outstanding'),
            pl.len().alias('count')
        ]).sort('remmfmt')
        print(f"✅ Summary created: {len(tbl1_sum)} maturity buckets")
    else:
        tbl1_sum = pl.DataFrame()
        print("⚠️  No data for Table 1 summary")
    
    # 12. SAVE TO PARQUET
    print("\n💾 Saving to Parquet...")
    out_dir = Path(OUTPUT_DIR)
    out_dir.mkdir(exist_ok=True)
    
    base = f"EIBMRNID_{reptdate.strftime('%Y%m')}"
    files_saved = []
    
    # Save Table 1
    if len(tbl1) > 0:
        tbl1_path = out_dir / f"{base}_TABLE1.parquet"
        tbl1.write_parquet(tbl1_path)
        files_saved.append(tbl1_path)
        size_kb = tbl1_path.stat().st_size / 1024
        print(f"  📄 {tbl1_path.name} ({size_kb:.1f} KB)")
    
    # Save summary
    if len(tbl1_sum) > 0:
        summary_path = out_dir / f"{base}_SUMMARY.parquet"
        tbl1_sum.write_parquet(summary_path)
        files_saved.append(summary_path)
        size_kb = summary_path.stat().st_size / 1024
        print(f"  📄 {summary_path.name} ({size_kb:.1f} KB)")
    
    # Save metadata
    meta_path = out_dir / f"{base}_METADATA.parquet"
    meta = pl.DataFrame({
        'report_date': [reptdate],
        'nid_file': [Path(NID_FILE).name],
        'trnch_file': [Path(TRNCH_FILE).name if use_trnch else 'None'],
        'processed_at': [datetime.now()],
        'table1_records': [len(tbl1)],
        'table2_count': [tbl2_count],
        'table2_volume': [tbl2_volume],
        'overall_yield': [overall_yield],
        'yield_source': [yield_source],
        'total_records': [len(df)],
        'merged_with_trnch': [use_trnch]
    })
    meta.write_parquet(meta_path)
    files_saved.append(meta_path)
    size_kb = meta_path.stat().st_size / 1024
    print(f"  📄 {meta_path.name} ({size_kb:.1f} KB)")
    
    # 13. PRINT FINAL RESULTS
    print("\n" + "=" * 60)
    print("✅ EIBMRNID PROCESSING COMPLETE")
    print("=" * 60)
    
    print(f"\n📊 FINAL SUMMARY:")
    print(f"  • Merged with TRNCH: {'Yes' if use_trnch else 'No'}")
    print(f"  • Table 1 (Outstanding): {len(tbl1):,} records")
    print(f"  • Table 2 (Trading): {tbl2_count:,} transactions, RM {tbl2_volume:,.2f}")
    print(f"  • Table 3 (Yield): {overall_yield:.4f}%")
    print(f"  • Yield source: {yield_source}")
    print(f"  • Files saved: {len(files_saved)}")
    print(f"\n📁 Output folder: {out_dir.absolute()}")

# ============================================
# RUN
# ============================================

if __name__ == "__main__":
    main()
