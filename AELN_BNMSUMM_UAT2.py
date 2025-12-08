import polars as pl
import pyreadstat
from datetime import date, timedelta
from pathlib import Path
import os
import math

NID_FILE = "/stgsrcsys/host/uat/rinidm09.sas7bdat"  # Changed to Islamic NID file
TRNCH_FILE = "/stgsrcsys/host/uat/trnchim09.sas7bdat"  # Changed to Islamic TRNCH file
OUTPUT_DIR = "/sas/python/virt_edw/Data_Warehouse/MIS/Job/Output"
OUTPUT_FILE = "EIIMRNID_REPORT.TXT"  # Changed output filename
PARQUET_FILE = "EIIMRNID_DATA.parquet"

def convert_sas_date(sas_num):
    if sas_num is None:
        return None
    try:
        return date(1960, 1, 1) + timedelta(days=int(float(sas_num)))
    except:
        return None

def calc_remmth(matdt, reptdate):
    if matdt is None or matdt <= reptdate:
        return None
    if (matdt - reptdate).days < 8:
        return 0.1
    
    mdyr, mdmth, mdday = matdt.year, matdt.month, matdt.day
    rpyear, rpmonth, rpday = reptdate.year, reptdate.month, reptdate.day
    
    rp_month_days = [31, 29 if rpyear % 4 == 0 else 28, 31, 30, 31, 30,
                     31, 31, 30, 31, 30, 31]
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
        remd += rp_month_days[(rpmonth - 2) % 12]
    
    return remy * 12 + remm + remd / rp_month_days[rpmonth - 1]

def apply_remfmta(val):
    if val is None or (isinstance(val, float) and math.isnan(val)):
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
    if val is None or (isinstance(val, float) and math.isnan(val)):
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

def write_islamic_output(filename, reptdate, tbl1_sum, tbl2_stats, tbl3_data):
    dlm = '\x05'
    nidcnt, nidvol = tbl2_stats[0], tbl2_stats[1]
    
    with open(filename, 'w', encoding='utf-8') as f:
        f.write("PUBLIC ISLAMIC BANK BERHAD\n\n")
        f.write("REPORT ON RETAIL RINGGIT-DENOMINATED NEGOTIABLE ")
        f.write("ISLAMIC DEBT CERTIFICATE (R-NIDC)\n")
        f.write(f"REPORTING DATE : {reptdate.strftime('%d/%m/%Y')}\n")
        
        f.write(f"{dlm}\nTable 1 - Outstanding Retail R-NIDC\n\n{dlm}\n")
        f.write("REMAINING MATURITY (IN MONTHS)" + dlm)
        f.write("GROSS ISSUANCE" + dlm)
        f.write("HELD FOR MARKET MARKING" + dlm)
        f.write("NET OUTSTANDING" + dlm + "\n")
        f.write(dlm + dlm + "(A)" + dlm + "(B)" + dlm + "(A-B)" + dlm + "\n")
        
        total_curbal = total_heldmkt = total_outstanding = 0
        fmt_order = ['1. LE  6      ', '2. GT  6 TO 12', '3. GT 12 TO 24',
                    '4. GT 24 TO 36', '5. GT 36 TO 60', '              ']
        
        tbl1_dict = {}
        for row in tbl1_sum.rows(named=True):
            label = row['remfmta']
            if label: tbl1_dict[label.strip()] = row
        
        for label in fmt_order:
            stripped = label.strip()
            if stripped in tbl1_dict:
                row = tbl1_dict[stripped]
                curbal, heldmkt, outstanding = float(row['curbal']), float(row['heldmkt']), float(row['outstanding'])
                total_curbal += curbal
                total_heldmkt += heldmkt
                total_outstanding += outstanding
                remmgrp = label.split('.', 1)[1].strip() if '.' in label else label.strip()
                f.write(f"{dlm}{remmgrp:24}{dlm}{curbal:16,.2f}{dlm}{heldmkt:16,.2f}{dlm}{outstanding:16,.2f}{dlm}\n")
        
        f.write(f"{dlm}TOTAL{24*' '}{dlm}{total_curbal:16,.2f}{dlm}{total_heldmkt:16,.2f}{dlm}{total_outstanding:16,.2f}{dlm}\n")
        
        f.write(f"{dlm}\nTable 2 - Monthly Trading Volume\n\n{dlm}\n")
        f.write("GROSS MONTHLY PURCHASE OF RETAIL R-NIDC BY THE BANK" + dlm + "A) NUMBER OF R-NIDC" + dlm)
        f.write(f"{nidcnt if nidcnt > 0 else '0'}{dlm}\n{dlm}{dlm}B) VOLUME OF R-NIDC{dlm}{nidvol if nidcnt > 0 else '0.00'}{dlm}\n")
        
        f.write(f"{dlm}\nTable 3 - Indicative Mid Yield\n\n{dlm}\n")
        f.write("REMAINING MATURITY (IN MONTHS)" + dlm + "(%)" + dlm + "\n")
        
        fmtb_order = ['1. LE  1      ', '2. GT  1 TO  3', '3. GT  3 TO  6',
                     '4. GT  6 TO  9', '5. GT  9 TO 12', '6. GT 12 TO 24',
                     '7. GT 24 TO 36', '8. GT 36 TO 60', '             ']
        
        for label in fmtb_order:
            stripped = label.strip()
            if stripped in tbl3_data:
                midyield = tbl3_data[stripped]
                if midyield > 0:
                    remmgrp = label.split('.', 1)[1].strip() if '.' in label else label.strip()
                    f.write(f"{dlm}{remmgrp:24}{dlm}{midyield:7.4f}{dlm}\n")
        
        overall_avg = tbl3_data.get('OVERALL', 0)
        if overall_avg > 0:
            f.write(f"{dlm}AVERAGE BID-ASK SPREAD ACROSS MATURITIES{dlm}{overall_avg:7.4f}{dlm}\n")

def main():
    print("=" * 60)
    print("EIIMRNID - EXACT SAS OUTPUT (ISLAMIC VERSION)")
    print("=" * 60)
    
    today = date.today()
    reptdate = date(today.year, today.month, 1) - timedelta(days=1)
    startdte = date(reptdate.year, reptdate.month, 1)
    
    print(f"Report Date: {reptdate.strftime('%d/%m/%Y')}")
    print(f"Start Date: {startdte.strftime('%d/%m/%Y')}")
    
    if not Path(NID_FILE).exists():
        print(f"❌ Islamic NID file not found: {NID_FILE}")
        return
    
    output_path = Path(OUTPUT_DIR)
    output_path.mkdir(parents=True, exist_ok=True)
    final_output_file = output_path / OUTPUT_FILE
    final_parquet_file = output_path / PARQUET_FILE
    
    print("\n📂 Processing Islamic data...")
    df_nid, _ = pyreadstat.read_sas7bdat(NID_FILE)
    df = pl.from_pandas(df_nid).rename({col: col.lower() for col in df_nid.columns})
    print(f"  Original Islamic NID records: {len(df):,}")
    
    if Path(TRNCH_FILE).exists():
        df_trnch, _ = pyreadstat.read_sas7bdat(TRNCH_FILE)
        df_trnch = pl.from_pandas(df_trnch).rename({col: col.lower() for col in df_trnch.columns})
        df = df.join(df_trnch, on='trancheno', how='left')
        print("  Merged with Islamic TRNCH")
    
    for col in ['matdt', 'startdt', 'early_wddt']:
        if col in df.columns and df[col].dtype in [pl.Int64, pl.Float64]:
            df = df.with_columns(
                pl.col(col).map_elements(convert_sas_date, return_dtype=pl.Date).alias(col)
            )
    
    df = df.filter(pl.col('curbal') > 0)
    
    if 'matdt' in df.columns and df['matdt'].dtype == pl.Date:
        df = df.with_columns(
            pl.col('matdt').map_elements(
                lambda x: calc_remmth(x, reptdate),
                return_dtype=pl.Float64
            ).alias('remmth')
        )
    
    print(f"\n💾 Saving processed Islamic data to Parquet: {final_parquet_file}")
    df.write_parquet(final_parquet_file)
    print(f"  Saved {len(df):,} records to Parquet")
    
    overall_yield = 0
    tbl3_data = {'OVERALL': 0}
    
    print("\n📊 Creating Table 1...")
    required_cols = ['matdt', 'startdt', 'nidstat', 'cdstat']
    if all(col in df.columns for col in required_cols):
        tbl1_filtered = df.filter(
            (pl.col('matdt') > reptdate) &
            (pl.col('startdt') <= reptdate) &
            (pl.col('nidstat') == 'N') &
            (pl.col('cdstat') == 'A')
        )
        print(f"  Table 1 filtered records: {len(tbl1_filtered):,}")
        
        if len(tbl1_filtered) > 0:
            tbl1 = tbl1_filtered.with_columns([
                pl.lit(0).alias('heldmkt')
            ]).with_columns([
                (pl.col('curbal') - pl.col('heldmkt')).alias('outstanding')
            ]).with_columns([
                pl.col('remmth').map_elements(
                    lambda x: apply_remfmta(x) if x is not None else '              ',
                    return_dtype=pl.Utf8
                ).alias('remfmta')
            ])
            
            tbl1_sum = tbl1.group_by('remfmta').agg([
                pl.sum('curbal').alias('curbal'),
                pl.sum('heldmkt').alias('heldmkt'),
                pl.sum('outstanding').alias('outstanding')
            ])
            print(f"  Table 1 summary: {len(tbl1_sum)} maturity buckets")
        else:
            tbl1_sum = pl.DataFrame({'remfmta': [], 'curbal': [], 'heldmkt': [], 'outstanding': []})
            tbl1 = pl.DataFrame()
    else:
        tbl1_sum = pl.DataFrame({'remfmta': [], 'curbal': [], 'heldmkt': [], 'outstanding': []})
        tbl1 = pl.DataFrame()
    
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
            nidcnt, nidvol = tbl2_result.row(0)
            nidcnt = nidcnt if nidcnt is not None else 0
            nidvol = nidvol if nidvol is not None else 0.0
        else:
            nidcnt, nidvol = 0, 0.0
    else:
        nidcnt, nidvol = 0, 0.0
    
    tbl2_stats = (nidcnt, nidvol)
    print(f"  Table 2 - R-NIDC Count: {nidcnt:,}, Volume: {nidvol:,.2f}")
    
    print("\n📊 Creating Table 3...")
    if 'intplrate_bid' in df.columns and 'intplrate_offer' in df.columns:
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
            tbl3 = tbl3_filtered.with_columns([
                ((pl.col('intplrate_bid') + pl.col('intplrate_offer')) / 2).alias('yield')
            ]).with_columns([
                pl.col('remmth').map_elements(
                    lambda x: apply_remfmtb(x) if x is not None else '             ',
                    return_dtype=pl.Utf8
                ).alias('remfmtb')
            ])
            
            tbl3_unique = tbl3.unique(subset=['remfmtb', 'trancheno'])
            tbl3_yield = tbl3_unique.filter(pl.col('yield') > 0).group_by('remfmtb').agg([
                pl.mean('yield').alias('midyield'),
                pl.len().alias('count')
            ])
            
            overall_yield_df = tbl3_unique.filter(pl.col('yield') > 0).select(
                pl.mean('yield').alias('overall_yield')
            )
            
            if overall_yield_df.height > 0:
                overall_yield = overall_yield_df.row(0)[0] or 0
            
            tbl3_data = {'OVERALL': overall_yield}
            for row in tbl3_yield.rows(named=True):
                label = row['remfmtb']
                if label:
                    tbl3_data[label.strip()] = float(row['midyield'])
            
            print(f"  Table 3 - Overall yield: {overall_yield:.4f}%, Buckets: {len(tbl3_data)-1}")
    
    print(f"\n💾 Writing Islamic SAS-format output to: {final_output_file}")
    write_islamic_output(final_output_file, reptdate, tbl1_sum, tbl2_stats, tbl3_data)
    
    print(f"\n✅ Islamic SAS report generated: {final_output_file}")
    print(f"✅ Islamic Parquet data saved: {final_parquet_file}")
    print(f"\n📊 Final Summary:")
    print(f"  Total processed records: {len(df):,}")
    print(f"  Table 1 (Outstanding): {len(tbl1):,} records")
    print(f"  Table 2 (Trading): {nidcnt:,} R-NIDCs, RM {nidvol:,.2f}")
    print(f"  Table 3 (Yield): {overall_yield:.4f}% overall")
    print(f"\n📁 Output folder: {output_path.absolute()}")

if __name__ == "__main__":
    main()
