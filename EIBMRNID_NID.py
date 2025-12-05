import polars as pl
from datetime import date, timedelta
from pathlib import Path

# ============================================
# MAIN PROCESSING FUNCTION
# ============================================

def generate_nid_report(nid_path: str, trnch_path: str, output_path: str, custom_date=None):
    """Generate NID report following SAS logic"""
    
    # 1. SET REPORT DATE (like REPTDATE DATA step)
    reptdate = custom_date or (date.today().replace(day=1) - timedelta(days=1))
    startdte = date(reptdate.year, reptdate.month, 1)
    reptdt = reptdate.strftime("%d/%m/%Y")
    
    # Format dictionaries (like PROC FORMAT)
    remfmta = {(-1e9,6):'1. LE  6      ',(6,12):'2. GT  6 TO 12',
               (12,24):'3. GT 12 TO 24',(24,36):'4. GT 24 TO 36',
               (36,60):'5. GT 36 TO 60','other':'              '}
    
    remfmtb = {(-1e9,1):'1. LE  1      ',(1,3):'2. GT  1 TO  3',
               (3,6):'3. GT  3 TO  6',(6,9):'4. GT  6 TO  9',
               (9,12):'5. GT  9 TO 12',(12,24):'6. GT 12 TO 24',
               (24,36):'7. GT 24 TO 36',(36,60):'8. GT 36 TO 60',
               'other':'             '}
    
    # Helper functions
    def apply_fmt(val, fmt):
        if not val or val != val: return fmt['other']
        for (l,h),lab in fmt.items():
            if l != 'other' and l <= val < h: return lab
        return fmt['other']
    
    def remmth_calc(matdt):
        if not matdt or matdt <= reptdate: return None
        if (matdt - reptdate).days < 8: return 0.1
        
        mdyr, mdmth, mdday = matdt.year, matdt.month, matdt.day
        rpdays = 29 if reptdate.year%4==0 and mdmth==2 else [31,28,31,30,31,30,31,31,30,31,30,31][mdmth-1]
        mdday = min(mdday, rpdays)
        
        remy = mdyr - reptdate.year
        remm = mdmth - reptdate.month
        remd = mdday - reptdate.day
        
        if remd < 0:
            remm -= 1
            if remm < 0: remy -= 1; remm += 12
            remd += [31,28,31,30,31,30,31,31,30,31,30,31][(mdmth-2)%12]
        
        return remy*12 + remm + remd/rpdays
    
    # 2. LOAD AND MERGE DATA (like PROC SORT and MERGE)
    month_str = f"{reptdate.month:02d}"
    nid_file = Path(nid_path.replace('&REPTMON', month_str))
    trnch_file = Path(trnch_path.replace('&REPTMON', month_str))
    
    # Read and merge
    rnid = pl.read_csv(nid_file).filter(pl.col('curbal') > 0).sort('trancheno')
    trnch = pl.read_csv(trnch_file).sort('trancheno')
    merged = rnid.join(trnch, on='trancheno', how='inner')
    
    # Convert dates
    for col in ['matdt','startdt','early_wddt']:
        if col in merged.columns:
            merged = merged.with_columns(pl.col(col).str.strptime(pl.Date))
    
    # Add report dates
    merged = merged.with_columns([
        pl.lit(reptdate).alias('reptdate'),
        pl.lit(startdte).alias('startdte')
    ])
    
    # 3. CREATE TABLES (like DATA steps)
    
    # Filter for active records (WHERE conditions)
    filtered = merged.filter(
        (pl.col('matdt') > pl.col('reptdate')) & 
        (pl.col('startdt') <= pl.col('reptdate'))
    )
    
    # Add remmth calculation
    filtered = filtered.with_columns(
        pl.col('matdt').map_elements(remmth_calc).alias('remmth')
    )
    
    # TABLE 1: Outstanding NID (RNIDTBL1)
    tbl1 = filtered.filter(
        (pl.col('nidstat') == 'N') & (pl.col('cdstat') == 'A')
    ).with_columns([
        pl.lit(0).alias('heldmkt'),
        (pl.col('curbal') - pl.col('heldmkt')).alias('outstanding'),
        pl.col('remmth').map_elements(lambda x: apply_fmt(x, remfmta)).alias('remmfmt')
    ])
    
    # TABLE 2: Monthly Trading (RNIDTBL2)
    tbl2_stats = merged.filter(
        (pl.col('nidstat') == 'E') &
        (pl.col('early_wddt') >= startdte) &
        (pl.col('early_wddt') <= reptdate)
    ).select([
        pl.len().alias('nidcnt'),
        pl.col('curbal').sum().alias('nidvol')
    ]).row(0)
    
    # TABLE 3: Mid Yield (RNIDTBL3)
    tbl3 = filtered.filter(
        (pl.col('nidstat') == 'N') & (pl.col('cdstat') == 'A')
    ).with_columns([
        ((pl.col('intplrate_bid') + pl.col('intplrate_offer')) / 2).alias('yield'),
        pl.col('remmth').map_elements(lambda x: apply_fmt(x, remfmtb)).alias('remmfmt')
    ]).unique(subset=['remmfmt', 'trancheno'])
    
    # 4. SUMMARIZE DATA (like PROC SUMMARY)
    
    # Table 1 summary
    tbl1_summary = tbl1.group_by('remmfmt').agg([
        pl.sum('curbal').alias('curbal'),
        pl.sum('heldmkt').alias('heldmkt'),
        pl.sum('outstanding').alias('outstanding')
    ]).sort('remmfmt')
    
    # Table 3 summary (mid-yield calculation)
    tbl3_summary = tbl3.filter(pl.col('yield') > 0).group_by('remmfmt').agg(
        pl.col('yield').mean().alias('midyield')
    ).sort('remmfmt')
    overall_yield = tbl3.filter(pl.col('yield') > 0).select(pl.col('yield').mean()).row(0)[0] or 0
    
    # 5. GENERATE OUTPUT FILE
    with open(output_path, 'w') as f:
        dlm = '\x05'
        
        # Header
        f.write("PUBLIC BANK BERHAD\n\n")
        f.write("REPORT ON RETAIL RINGGIT-DENOMINATED NEGOTIABLE ")
        f.write("INSTRUMENT OF DEPOSIT (NID)\n")
        f.write(f"REPORTING DATE : {reptdt}\n")
        
        # TABLE 1
        f.write(f"{dlm}\nTable 1 - Outstanding Retail NID\n\n{dlm}\n")
        f.write("REMAINING MATURITY (IN MONTHS)" + dlm)
        f.write("GROSS ISSUANCE" + dlm)
        f.write("HELD FOR MARKET MARKING" + dlm)
        f.write("NET OUTSTANDING" + dlm + "\n")
        f.write(dlm + dlm + "(A)" + dlm + "(B)" + dlm + "(A-B)" + dlm + "\n")
        
        total_curbal = total_heldmkt = total_outstanding = 0
        for row in tbl1_summary.rows(named=True):
            grp = row['remmfmt'].split('.')[1].strip()
            curbal, heldmkt, outstanding = row['curbal'], row['heldmkt'], row['outstanding']
            
            total_curbal += curbal
            total_heldmkt += heldmkt
            total_outstanding += outstanding
            
            f.write(f"{dlm}{grp}{dlm}")
            f.write(f"{curbal:,.2f}{dlm}{heldmkt:,.2f}{dlm}{outstanding:,.2f}{dlm}\n")
        
        f.write(f"{dlm}TOTAL{dlm}{total_curbal:,.2f}{dlm}")
        f.write(f"{total_heldmkt:,.2f}{dlm}{total_outstanding:,.2f}{dlm}\n")
        
        # TABLE 2
        f.write(f"{dlm}\nTable 2 - Monthly Trading Volume\n\n{dlm}\n")
        f.write("GROSS MONTHLY PURCHASE OF RETAIL NID BY THE BANK" + dlm)
        f.write("A) NUMBER OF NID" + dlm)
        
        nidcnt, nidvol = tbl2_stats['nidcnt'], tbl2_stats['nidvol']
        if nidcnt > 0:
            f.write(f"{nidcnt}{dlm}\n{dlm}{dlm}B) VOLUME OF NID{dlm}{nidvol:,.2f}{dlm}\n")
        else:
            f.write(f"0.00{dlm}\n{dlm}{dlm}B) VOLUME OF NID{dlm}0.00{dlm}\n")
        
        # TABLE 3
        f.write(f"{dlm}\nTable 3 - Indicative Mid Yield\n\n{dlm}\n")
        f.write("REMAINING MATURITY (IN MONTHS)" + dlm + "(%)" + dlm + "\n")
        
        for row in tbl3_summary.rows(named=True):
            grp = row['remmfmt'].split('.')[1].strip()
            f.write(f"{dlm}{grp}{dlm}{row['midyield']:.4f}{dlm}\n")
        
        f.write(f"{dlm}AVERAGE BID-ASK SPREAD ACROSS MATURITIES{dlm}")
        f.write(f"{overall_yield:.4f}{dlm}\n")
    
    print(f"Report generated: {output_path}")
    return {
        'table1': tbl1_summary,
        'table2': {'nidcnt': nidcnt, 'nidvol': nidvol},
        'table3': tbl3_summary,
        'overall_yield': overall_yield
    }

# ============================================
# USAGE
# ============================================

if __name__ == "__main__":
    # Simple usage
    result = generate_nid_report(
        nid_path="data/RNIDM&REPTMON.csv",
        trnch_path="data/TRNCHM&REPTMON.csv",
        output_path="NID_REPORT.txt"
    )
    
    # With custom date
    # from datetime import date
    # result = generate_nid_report(
    #     nid_path="data/RNIDM01.csv",
    #     trnch_path="data/TRNCHM01.csv",
    #     output_path="JAN_REPORT.txt",
    #     custom_date=date(2024, 1, 31)
    # )
