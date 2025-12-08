# ============================================
# EIBMRNID - EXACT SAS OUTPUT VERSION
# ============================================

import polars as pl
import pyreadstat
from datetime import date, timedelta
from pathlib import Path
import os

# ============================================
# CONFIGURE PATHS
# ============================================

NID_FILE = "/stgsrcsys/host/uat/rnidm09.sas7bdat"
TRNCH_FILE = "/stgsrcsys/host/uat/trnchm09.sas7bdat"
OUTPUT_DIR = "/sas/python/virt_edw/Data_Warehouse/MIS/Job/Output"
OUTPUT_FILE = "EIBMRNID_REPORT.TXT"
PARQUET_FILE = "EIBMRNID_DATA.parquet"

# ============================================
# SAS-EXACT FUNCTIONS
# ============================================

def convert_sas_date(sas_num):
    """Convert SAS numeric date to Python date"""
    if sas_num is None:
        return None
    try:
        # SAS date: number of days since 1960-01-01
        sas_days = int(float(sas_num))
        return date(1960, 1, 1) + timedelta(days=sas_days)
    except:
        return None

def calc_remmth(matdt, reptdate):
    """SAS exact remaining months calculation"""
    if matdt is None or matdt <= reptdate:
        return None
    
    if (matdt - reptdate).days < 8:
        return 0.1
    
    mdyr, mdmth, mdday = matdt.year, matdt.month, matdt.day
    rpyear, rpmonth, rpday = reptdate.year, reptdate.month, reptdate.day
    
    # Days in months for reporting date year
    rp_month_days = [31, 29 if rpyear % 4 == 0 else 28, 31, 30, 31, 30,
                     31, 31, 30, 31, 30, 31]
    
    # Days in months for maturity date year
    md_month_days = [31, 29 if mdyr % 4 == 0 else 28, 31, 30, 31, 30,
                     31, 31, 30, 31, 30, 31]
    
    mdday = min(mdday, md_month_days[mdmth - 1])
    
    remy = mdyr - rpyear
    remm = mdmth - rpmonth
    remd = mdday - rpday
    
    if remd < 0:
        remm -= 1
        if remm < 0:
            remy -= 1
            remm += 12
        # Add days from previous month
        remd += rp_month_days[(rpmonth - 2) % 12]
    
    return remy * 12 + remm + remd / rp_month_days[rpmonth - 1]

# SAS FORMAT DEFINITIONS
def apply_remfmta(val):
    """Apply SAS FORMAT REMFMTA"""
    if val is None or pl.is_nan(val):
        return '              '
    
    if val <= 6:
        return '1. LE  6      '
    elif 6 < val <= 12:
        return '2. GT  6 TO 12'
    elif 12 < val <= 24:
        return '3. GT 12 TO 24'
    elif 24 < val <= 36:
        return '4. GT 24 TO 36'
    elif 36 < val <= 60:
        return '5. GT 36 TO 60'
    else:
        return '              '

def apply_remfmtb(val):
    """Apply SAS FORMAT REMFMTB"""
    if val is None or pl.is_nan(val):
        return '             '
    
    if val <= 1:
        return '1. LE  1      '
    elif 1 < val <= 3:
        return '2. GT  1 TO  3'
    elif 3 < val <= 6:
        return '3. GT  3 TO  6'
    elif 6 < val <= 9:
        return '4. GT  6 TO  9'
    elif 9 < val <= 12:
        return '5. GT  9 TO 12'
    elif 12 < val <= 24:
        return '6. GT 12 TO 24'
    elif 24 < val <= 36:
        return '7. GT 24 TO 36'
    elif 36 < val <= 60:
        return '8. GT 36 TO 60'
    else:
        return '             '

def write_sas_exact_output(filename, reptdate, tbl1_sum, tbl2_stats, tbl3_data):
    """Write output exactly like SAS (ASCII delimiter hex 05)"""
    dlm = '\x05'  # ASCII delimiter hex 05
    
    # Extract tbl2_stats values
    nidcnt = tbl2_stats[0]
    nidvol = tbl2_stats[1]
    
    with open(filename, 'w', encoding='utf-8') as f:
        # Header (EXACTLY like SAS)
        f.write("PUBLIC BANK BERHAD\n\n")
        f.write("REPORT ON RETAIL RINGGIT-DENOMINATED NEGOTIABLE ")
        f.write("INSTRUMENT OF DEPOSIT (NID)\n")
        f.write(f"REPORTING DATE : {reptdate.strftime('%d/%m/%Y')}\n")
        
        # ============================================
        # TABLE 1 - Outstanding Retail NID
        # ============================================
        f.write(f"{dlm}\n")
        f.write("Table 1 - Outstanding Retail NID\n")
        f.write("\n")
        f.write(f"{dlm}\n")
        f.write("REMAINING MATURITY (IN MONTHS)" + dlm)
        f.write("GROSS ISSUANCE" + dlm)
        f.write("HELD FOR MARKET MARKING" + dlm)
        f.write("NET OUTSTANDING" + dlm + "\n")
        f.write(dlm + dlm + "(A)" + dlm + "(B)" + dlm + "(A-B)" + dlm + "\n")
        
        total_curbal = 0
        total_heldmkt = 0
        total_outstanding = 0
        
        # Sort Table 1 data according to SAS format order
        fmt_order = [
            '1. LE  6      ',
            '2. GT  6 TO 12',
            '3. GT 12 TO 24',
            '4. GT 24 TO 36',
            '5. GT 36 TO 60',
            '              '
        ]
        
        # Create a dictionary for quick lookup
        tbl1_dict = {}
        for row in tbl1_sum.rows(named=True):
            label = row['remfmta']
            if label is not None:
                label = label.strip()
                tbl1_dict[label] = row
        
        # Write rows in SAS order
        for label in fmt_order:
            stripped_label = label.strip()
            if stripped_label in tbl1_dict:
                row = tbl1_dict[stripped_label]
                curbal = float(row['curbal'])
                heldmkt = float(row['heldmkt'])
                outstanding = float(row['outstanding'])
                
                total_curbal += curbal
                total_heldmkt += heldmkt
                total_outstanding += outstanding
                
                # Extract REMMGRP (second part after '.')
                if '.' in label:
                    remmgrp = label.split('.', 1)[1].strip()
                else:
                    remmgrp = label.strip()
                
                f.write(f"{dlm}{remmgrp:24}{dlm}")
                f.write(f"{curbal:16,.2f}{dlm}")
                f.write(f"{heldmkt:16,.2f}{dlm}")
                f.write(f"{outstanding:16,.2f}{dlm}\n")
        
        # Total row
        f.write(f"{dlm}TOTAL{24*' '}{dlm}")
        f.write(f"{total_curbal:16,.2f}{dlm}")
        f.write(f"{total_heldmkt:16,.2f}{dlm}")
        f.write(f"{total_outstanding:16,.2f}{dlm}\n")
        
        # ============================================
        # TABLE 2 - Monthly Trading Volume
        # ============================================
        f.write(f"{dlm}\n")
        f.write("Table 2 - Monthly Trading Volume\n")
        f.write("\n")
        f.write(f"{dlm}\n")
        f.write("GROSS MONTHLY PURCHASE OF RETAIL NID BY THE BANK" + dlm)
        f.write("A) NUMBER OF NID" + dlm)
        
        if nidcnt > 0:
            f.write(f"{nidcnt}{dlm}\n")
            f.write(f"{dlm}{dlm}B) VOLUME OF NID{dlm}")
            f.write(f"{nidvol:,.2f}{dlm}\n")
        else:
            f.write(f"0{dlm}\n")
            f.write(f"{dlm}{dlm}B) VOLUME OF NID{dlm}0.00{dlm}\n")
        
        # ============================================
        # TABLE 3 - Indicative Mid Yield
        # ============================================
        f.write(f"{dlm}\n")
        f.write("Table 3 - Indicative Mid Yield\n")
        f.write("\n")
        f.write(f"{dlm}\n")
        f.write("REMAINING MATURITY (IN MONTHS)" + dlm + "(%)" + dlm + "\n")
        
        # Sort Table 3 data according to SAS format order
        fmtb_order = [
            '1. LE  1      ',
            '2. GT  1 TO  3',
            '3. GT  3 TO  6',
            '4. GT  6 TO  9',
            '5. GT  9 TO 12',
            '6. GT 12 TO 24',
            '7. GT 24 TO 36',
            '8. GT 36 TO 60',
            '             '
        ]
        
        # Write yield by maturity bucket
        for label in fmtb_order:
            stripped_label = label.strip()
            if stripped_label in tbl3_data:
                midyield = tbl3_data[stripped_label]
                if midyield > 0:  # Only show if there's data
                    # Extract REMMGRP (second part after '.')
                    if '.' in label:
                        remmgrp = label.split('.', 1)[1].strip()
                    else:
                        remmgrp = label.strip()
                    
                    f.write(f"{dlm}{remmgrp:24}{dlm}")
                    f.write(f"{midyield:7.4f}{dlm}\n")
        
        # Write overall average at the end
        overall_avg = tbl3_data.get('OVERALL', 0)
        if overall_avg > 0:
            f.write(f"{dlm}AVERAGE BID-ASK SPREAD ACROSS MATURITIES{dlm}")
            f.write(f"{overall_avg:7.4f}{dlm}\n")

# ============================================
# MAIN PROCESSING
# ============================================

def main():
    print("=" * 60)
    print("EIBMRNID - EXACT SAS OUTPUT")
    print("=" * 60)
    
    # ============================================
    # TESTING: Use a specific report date that matches your data
    # ============================================
    # Based on your data (2017-2019), let's use a report date in 2017
    # This will make the data appear as "outstanding" NIDs
    
    # Option 1: Use a date in 2017 when your NIDs were active
    test_report_date = date(2017, 6, 30)  # June 30, 2017
    test_start_date = date(2017, 6, 1)    # June 1, 2017
    
    # Option 2: Or use the original logic but with a fixed date
    # today = date.today()
    # test_report_date = date(today.year, today.month, 1) - timedelta(days=1)
    # test_start_date = date(test_report_date.year, test_report_date.month, 1)
    
    print(f"TEST MODE: Using fixed report date for historical data")
    print(f"Report Date: {test_report_date.strftime('%d/%m/%Y')}")
    print(f"Start Date: {test_start_date.strftime('%d/%m/%Y')}")
    
    reptdate = test_report_date
    startdte = test_start_date
    
    # Check files
    if not Path(NID_FILE).exists():
        print(f"❌ NID file not found: {NID_FILE}")
        return
    
    # Create output directory
    output_path = Path(OUTPUT_DIR)
    output_path.mkdir(parents=True, exist_ok=True)
    final_output_file = output_path / OUTPUT_FILE
    final_parquet_file = output_path / PARQUET_FILE
    
    # 1. READ AND PROCESS DATA
    print("\n📂 Processing data...")
    
    # Read NID
    df_nid, _ = pyreadstat.read_sas7bdat(NID_FILE)
    df = pl.from_pandas(df_nid).rename({col: col.lower() for col in df_nid.columns})
    
    print(f"  Original NID records: {len(df):,}")
    
    # Read TRNCH if exists
    if Path(TRNCH_FILE).exists():
        df_trnch, _ = pyreadstat.read_sas7bdat(TRNCH_FILE)
        df_trnch = pl.from_pandas(df_trnch).rename({col: col.lower() for col in df_trnch.columns})
        df = df.join(df_trnch, on='trancheno', how='left')
        print("  Merged with TRNCH")
    else:
        print("  TRNCH file not found, using NID data only")
    
    # Debug: Check if yield columns exist
    print("\n🔍 Checking for yield columns...")
    if 'intplrate_bid' in df.columns and 'intplrate_offer' in df.columns:
        print(f"  Yield columns found: intplrate_bid, intplrate_offer")
        # Check sample values
        sample_bid = df.select('intplrate_bid').head(3).to_series().to_list()
        sample_offer = df.select('intplrate_offer').head(3).to_series().to_list()
        print(f"  Sample intplrate_bid: {sample_bid}")
        print(f"  Sample intplrate_offer: {sample_offer}")
    else:
        print(f"  Warning: Yield columns not found in data")
    
    # Convert dates
    for col in ['matdt', 'startdt', 'early_wddt']:
        if col in df.columns and df[col].dtype in [pl.Int64, pl.Float64]:
            df = df.with_columns(
                pl.col(col).map_elements(convert_sas_date, return_dtype=pl.Date).alias(col)
            )
    
    # Filter positive balance
    df = df.filter(pl.col('curbal') > 0)
    print(f"  Records with positive balance: {len(df):,}")
    
    # Calculate remaining months
    if 'matdt' in df.columns and df['matdt'].dtype == pl.Date:
        df = df.with_columns(
            pl.col('matdt').map_elements(
                lambda x: calc_remmth(x, reptdate),
                return_dtype=pl.Float64
            ).alias('remmth')
        )
        # Check remmth values
        print(f"  Sample remmth values: {df.select('remmth').head(5).to_series().to_list()}")
    
    # Save processed data to Parquet
    print(f"\n💾 Saving processed data to Parquet: {final_parquet_file}")
    df.write_parquet(final_parquet_file)
    print(f"  Saved {len(df):,} records to Parquet")
    
    # Initialize variables
    overall_yield = 0
    tbl3_data = {'OVERALL': 0}
    
    # 2. CREATE TABLE 1 DATA (Outstanding Retail NID)
    print("\n📊 Creating Table 1...")
    
    # Check if required columns exist
    required_cols = ['matdt', 'startdt', 'nidstat', 'cdstat']
    missing_cols = [col for col in required_cols if col not in df.columns]
    
    if missing_cols:
        print(f"  Warning: Missing columns for Table 1: {missing_cols}")
        tbl1_sum = pl.DataFrame({'remfmta': [], 'curbal': [], 'heldmkt': [], 'outstanding': []})
        tbl1 = pl.DataFrame()
    else:
        # Filter for Table 1 (Outstanding NID) - Use test report date
        tbl1_filtered = df.filter(
            (pl.col('matdt') > reptdate) &
            (pl.col('startdt') <= reptdate) &
            (pl.col('nidstat') == 'N') &
            (pl.col('cdstat') == 'A')
        )
        
        print(f"  Table 1 filtered records: {len(tbl1_filtered):,}")
        
        # Show what dates we're comparing
        if len(tbl1_filtered) > 0:
            sample_dates = tbl1_filtered.select(['matdt', 'startdt']).head(3)
            print(f"  Sample matdt/startdt values:")
            for row in sample_dates.rows():
                print(f"    matdt={row[0]}, startdt={row[1]}")
        
        # Apply format REMFMTA
        if len(tbl1_filtered) > 0:
            tbl1 = tbl1_filtered.with_columns([
                pl.lit(0).alias('heldmkt')
            ]).with_columns([
                (pl.col('curbal') - pl.col('heldmkt')).alias('outstanding')
            ]).with_columns([
                pl.col('remmth').map_elements(apply_remfmta, return_dtype=pl.Utf8).alias('remfmta')
            ])
            
            # Summarize Table 1
            tbl1_sum = tbl1.group_by('remfmta').agg([
                pl.sum('curbal').alias('curbal'),
                pl.sum('heldmkt').alias('heldmkt'),
                pl.sum('outstanding').alias('outstanding')
            ])
            
            print(f"  Table 1 summary: {len(tbl1_sum)} maturity buckets")
            for row in tbl1_sum.rows():
                print(f"    Bucket: '{row[0]}', Amount: {row[1]:,.2f}")
        else:
            tbl1_sum = pl.DataFrame({'remfmta': [], 'curbal': [], 'heldmkt': [], 'outstanding': []})
            tbl1 = pl.DataFrame()
            print("  Warning: No records found for Table 1")
    
    print(f"  Table 1 records: {len(tbl1):,}")
    
    # 3. CREATE TABLE 2 DATA (Monthly Trading Volume)
    print("\n📊 Creating Table 2...")
    
    if 'nidstat' in df.columns and 'early_wddt' in df.columns and df['early_wddt'].dtype == pl.Date:
        tbl2_result = df.filter(
            (pl.col('nidstat') == 'E') &
            (pl.col('early_wddt') >= startdte) &
            (pl.col('early_wddt') <= reptdate)
        ).select([
            pl.len().alias('nidcnt'),
            pl.sum('curbal').alias('nidvol')
        ])
        
        if tbl2_result.height > 0:
            tbl2_stats = tbl2_result.row(0)
            nidcnt = tbl2_stats[0] if tbl2_stats[0] is not None else 0
            nidvol = tbl2_stats[1] if tbl2_stats[1] is not None else 0.0
        else:
            nidcnt = 0
            nidvol = 0.0
    else:
        nidcnt = 0
        nidvol = 0.0
    
    tbl2_stats = (nidcnt, nidvol)
    print(f"  Table 2 - NID Count: {nidcnt:,}, Volume: {nidvol:,.2f}")
    
    # 4. CREATE TABLE 3 DATA (Indicative Mid Yield)
    print("\n📊 Creating Table 3...")
    
    # Check if required columns exist
    if 'intplrate_bid' in df.columns and 'intplrate_offer' in df.columns:
        # Filter for Table 3 (Yield calculation)
        tbl3_filtered = df.filter(
            (pl.col('matdt') > reptdate) &
            (pl.col('startdt') <= reptdate) &
            (pl.col('nidstat') == 'N') &
            (pl.col('cdstat') == 'A') &
            pl.col('intplrate_bid').is_not_null() &
            pl.col('intplrate_offer').is_not_null()
        )
        
        print(f"  Table 3 filtered records: {len(tbl3_filtered):,}")
        
        if len(tbl3_filtered) > 0:
            # Calculate yield for each record
            tbl3 = tbl3_filtered.with_columns([
                ((pl.col('intplrate_bid') + pl.col('intplrate_offer')) / 2).alias('yield'),
                pl.col('remmth').map_elements(apply_remfmtb, return_dtype=pl.Utf8).alias('remfmtb')
            ])
            
            # Check for None values in remfmtb
            null_count = tbl3.filter(pl.col('remfmtb').is_null()).height
            if null_count > 0:
                print(f"  Warning: {null_count} records have null remfmtb values")
                # Fill null values with default
                tbl3 = tbl3.with_columns(
                    pl.when(pl.col('remfmtb').is_null())
                    .then(pl.lit('             '))
                    .otherwise(pl.col('remfmtb'))
                    .alias('remfmtb')
                )
            
            # Remove duplicates by tranche
            tbl3_unique = tbl3.unique(subset=['remfmtb', 'trancheno'])
            
            # Calculate average yield by maturity bucket
            tbl3_yield = tbl3_unique.filter(pl.col('yield') > 0).group_by('remfmtb').agg([
                pl.mean('yield').alias('midyield'),
                pl.len().alias('count')
            ])
            
            print(f"  Table 3 yield buckets: {len(tbl3_yield)}")
            
            # Calculate overall average yield
            overall_yield_df = tbl3_unique.filter(pl.col('yield') > 0).select(
                pl.mean('yield').alias('overall_yield')
            )
            
            if overall_yield_df.height > 0:
                overall_yield_result = overall_yield_df.row(0)
                overall_yield = overall_yield_result[0] if overall_yield_result[0] is not None else 0
            
            # Create dictionary for Table 3 data (with None check)
            tbl3_data = {'OVERALL': overall_yield}
            for row in tbl3_yield.rows(named=True):
                label = row['remfmtb']
                if label is not None:
                    label = label.strip()
                    if label:  # Only add non-empty labels
                        tbl3_data[label] = float(row['midyield'])
            
            print(f"  Table 3 - Overall yield: {overall_yield:.4f}%, Buckets: {len(tbl3_data)-1}")
        else:
            print("  Warning: No records found for Table 3 yield calculation")
            tbl3_data = {'OVERALL': 0}
    else:
        print("  Skipping Table 3: Yield columns not available")
        tbl3_data = {'OVERALL': 0}
    
    # 5. WRITE EXACT SAS OUTPUT
    print(f"\n💾 Writing SAS-format output to: {final_output_file}")
    write_sas_exact_output(final_output_file, reptdate, tbl1_sum, tbl2_stats, tbl3_data)
    
    print(f"\n✅ SAS report generated: {final_output_file}")
    print(f"✅ Parquet data saved: {final_parquet_file}")
    print(f"\n📊 Final Summary:")
    print(f"  Total processed records: {len(df):,}")
    print(f"  Table 1 (Outstanding): {len(tbl1):,} records")
    print(f"  Table 2 (Trading): {nidcnt:,} NIDs, RM {nidvol:,.2f}")
    print(f"  Table 3 (Yield): {overall_yield:.4f}% overall")
    print(f"\n📁 Output folder: {output_path.absolute()}")
    
    # Show where output files are
    print(f"\n📄 Output files:")
    print(f"  1. Report: {final_output_file}")
    print(f"  2. Data: {final_parquet_file}")

# ============================================
# RUN
# ============================================

if __name__ == "__main__":
    main()
