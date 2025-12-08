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

# ============================================
# SAS-EXACT FUNCTIONS
# ============================================

def convert_sas_date(sas_num):
    """Convert SAS numeric date to Python date"""
    if sas_num is None:
        return None
    try:
        return date(1960, 1, 1) + timedelta(days=int(float(sas_num)))
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
            label = row['remfmta'].strip()
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
    
    # Set report date (last day of previous month)
    today = date.today()
    reptdate = date(today.year, today.month, 1) - timedelta(days=1)
    startdte = date(reptdate.year, reptdate.month, 1)
    
    print(f"Report Date: {reptdate.strftime('%d/%m/%Y')}")
    print(f"Start Date: {startdte.strftime('%d/%m/%Y')}")
    
    # Check files
    if not Path(NID_FILE).exists():
        print(f"❌ NID file not found: {NID_FILE}")
        return
    
    # Create output directory
    output_path = Path(OUTPUT_DIR)
    output_path.mkdir(parents=True, exist_ok=True)
    final_output_file = output_path / OUTPUT_FILE
    
    # 1. READ AND PROCESS DATA
    print("\n📂 Processing data...")
    
    # Read NID
    df_nid, _ = pyreadstat.read_sas7bdat(NID_FILE)
    df = pl.from_pandas(df_nid).rename({col: col.lower() for col in df_nid.columns})
    
    # Read TRNCH if exists
    if Path(TRNCH_FILE).exists():
        df_trnch, _ = pyreadstat.read_sas7bdat(TRNCH_FILE)
        df_trnch = pl.from_pandas(df_trnch).rename({col: col.lower() for col in df_trnch.columns})
        df = df.join(df_trnch, on='trancheno', how='left')
        print("  Merged with TRNCH")
    else:
        print("  TRNCH file not found, using NID data only")
    
    # Convert dates
    for col in ['matdt', 'startdt', 'early_wddt']:
        if col in df.columns and df[col].dtype in [pl.Int64, pl.Float64]:
            df = df.with_columns(
                pl.col(col).map_elements(convert_sas_date, return_dtype=pl.Date).alias(col)
            )
    
    # Filter positive balance
    df = df.filter(pl.col('curbal') > 0)
    
    # Calculate remaining months
    df = df.with_columns(
        pl.col('matdt').map_elements(
            lambda x: calc_remmth(x, reptdate),
            return_dtype=pl.Float64
        ).alias('remmth')
    )
    
    # 2. CREATE TABLE 1 DATA (Outstanding Retail NID)
    print("\n📊 Creating Table 1...")
    
    # Filter for Table 1 (Outstanding NID)
    tbl1_filtered = df.filter(
        (pl.col('matdt') > reptdate) &
        (pl.col('startdt') <= reptdate) &
        (pl.col('nidstat') == 'N') &
        (pl.col('cdstat') == 'A')
    )
    
    # Apply format REMFMTA
    tbl1 = tbl1_filtered.with_columns([
        pl.lit(0).alias('heldmkt'),
        (pl.col('curbal') - pl.col('heldmkt')).alias('outstanding'),
        pl.col('remmth').map_elements(apply_remfmta, return_dtype=pl.Utf8).alias('remfmta')
    ])
    
    # Summarize Table 1
    tbl1_sum = tbl1.group_by('remfmta').agg([
        pl.sum('curbal').alias('curbal'),
        pl.sum('heldmkt').alias('heldmkt'),
        pl.sum('outstanding').alias('outstanding')
    ])
    
    # 3. CREATE TABLE 2 DATA (Monthly Trading Volume)
    print("📊 Creating Table 2...")
    
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
    
    tbl2_stats = (nidcnt, nidvol)
    
    # 4. CREATE TABLE 3 DATA (Indicative Mid Yield)
    print("📊 Creating Table 3...")
    
    # Filter for Table 3 (Yield calculation)
    tbl3_filtered = df.filter(
        (pl.col('matdt') > reptdate) &
        (pl.col('startdt') <= reptdate) &
        (pl.col('nidstat') == 'N') &
        (pl.col('cdstat') == 'A') &
        pl.col('intplrate_bid').is_not_null() &
        pl.col('intplrate_offer').is_not_null()
    )
    
    if 'intplrate_bid' in tbl3_filtered.columns and 'intplrate_offer' in tbl3_filtered.columns:
        # Calculate yield for each record
        tbl3 = tbl3_filtered.with_columns([
            ((pl.col('intplrate_bid') + pl.col('intplrate_offer')) / 2).alias('yield'),
            pl.col('remmth').map_elements(apply_remfmtb, return_dtype=pl.Utf8).alias('remfmtb')
        ])
        
        # Remove duplicates by tranche
        tbl3_unique = tbl3.unique(subset=['remfmtb', 'trancheno'])
        
        # Calculate average yield by maturity bucket
        tbl3_yield = tbl3_unique.filter(pl.col('yield') > 0).group_by('remfmtb').agg([
            pl.mean('yield').alias('midyield'),
            pl.len().alias('count')
        ])
        
        # Calculate overall average yield
        overall_yield_df = tbl3_unique.filter(pl.col('yield') > 0).select(
            pl.mean('yield').alias('overall_yield')
        )
        
        overall_yield = 0
        if overall_yield_df.height > 0:
            overall_yield_result = overall_yield_df.row(0)
            overall_yield = overall_yield_result[0] if overall_yield_result[0] is not None else 0
        
        # Create dictionary for Table 3 data
        tbl3_data = {'OVERALL': overall_yield}
        for row in tbl3_yield.rows(named=True):
            label = row['remfmtb'].strip()
            tbl3_data[label] = float(row['midyield'])
    else:
        tbl3_data = {'OVERALL': 0}
        print("  Warning: intplrate_bid or intplrate_offer columns not found")
    
    # 5. WRITE EXACT SAS OUTPUT
    print(f"\n💾 Writing SAS-format output to: {final_output_file}")
    write_sas_exact_output(final_output_file, reptdate, tbl1_sum, tbl2_stats, tbl3_data)
    
    print(f"\n✅ SAS report generated: {final_output_file}")
    print(f"\n📊 Summary:")
    print(f"  Table 1 records: {len(tbl1):,}")
    print(f"  Table 2 count: {nidcnt:,} (volume: {nidvol:,.2f})")
    print(f"  Table 3 overall yield: {tbl3_data.get('OVERALL', 0):.4f}%")
    print(f"  Table 3 maturity buckets: {len(tbl3_data)-1}")
    print(f"\n📁 Output folder: {output_path.absolute()}")

# ============================================
# RUN
# ============================================

if __name__ == "__main__":
    main()
