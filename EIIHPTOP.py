import polars as pl
from pathlib import Path
from datetime import datetime, timedelta

def eiihptop():
    """Islamic bank version of EIMHPTOP"""
    base = Path.cwd()
    loan_path = base / "LOAN"
    cis_path = base / "CIS"
    
    # Execute external program
    try:
        from pbblnfmt import process as pbblnfmt_process
        pbblnfmt_process()
    except ImportError:
        print("Note: PGM(PBBLNFMT) not executed")
    
    # REPTDATE with week logic
    reptdate_df = pl.read_parquet(loan_path / "REPTDATE.parquet")
    reptdate = reptdate_df["REPTDATE"][0]
    reptday = reptdate.day
    
    # SAS SELECT logic for weeks
    if reptday == 8:
        wk, wk1 = '1', '4'
    elif reptday == 15:
        wk, wk1 = '2', '1'
    elif reptday == 22:
        wk, wk1 = '3', '2'
    else:
        wk, wk1 = '4', '3'
    
    mm = reptdate.month
    if wk == '1':
        mm1 = mm - 1 if mm > 1 else 12
    else:
        mm1 = mm
    
    reptmon = f"{mm:02d}"
    reptmon1 = f"{mm1:02d}"
    rdate = reptdate.strftime("%d%m%y")
    
    print(f"EIIHPTOP - Islamic Bank Top Accounts")
    print(f"NOWK: {wk}, REPTMON: {reptmon}, RDATE: {rdate}")
    
    # HPD macro variable (assuming defined elsewhere)
    hpd_products = ["'P1'", "'P2'", "'P3'"]  # Example - should come from macro
    
    # 1. CIS INFO (Islamic bank specific)
    try:
        cis_df = pl.read_parquet(cis_path / "LOAN.parquet")
        hpcis_df = cis_df.filter(
            (~pl.col("CACCCODE").is_in(["017", "021", "028"])) &
            (pl.col("SECCUST") == "901")
        )
        hpcis_df = hpcis_df.with_columns(
            pl.when(pl.col("NEWIC") != "")
            .then(pl.col("NEWIC"))
            .otherwise(pl.col("OLDIC"))
            .alias("ICNO")
        ).select(["ACCTNO", "ICNO", "CUSTNAME"])
    except:
        hpcis_df = pl.DataFrame({"ACCTNO": [], "ICNO": [], "CUSTNAME": []})
    
    # 2. EXTRACT HP A/C with MTHARR calculation (same as EIMHPTOP)
    try:
        hpacc_df = pl.read_parquet(loan_path / f"LOAN{reptmon}{wk}.parquet")
    except:
        print(f"File not found: LOAN/LOAN{reptmon}{wk}.parquet")
        return
    
    # Filter for HPD products and balance > 0
    hpacc_df = hpacc_df.filter(
        (pl.col("PRODUCT").is_in(hpd_products)) &
        (pl.col("BALANCE") > 0)
    )
    
    # Calculate MTHARR (days to months conversion)
    thisdate = datetime.strptime(rdate, "%d%m%y")
    
    def calculate_mtharr(bldate):
        if not bldate or bldate == 0:
            return 0
        
        try:
            if isinstance(bldate, (int, float)):
                # SAS date to Python date conversion
                sas_base = datetime(1960, 1, 1)
                bldate_dt = sas_base + timedelta(days=int(bldate))
            else:
                bldate_dt = bldate
            
            daydiff = (thisdate - bldate_dt).days
            
            # SAS IF-ELSE chain
            if daydiff > 729: return int((daydiff/365)*12)
            elif daydiff > 698: return 23
            elif daydiff > 668: return 22
            elif daydiff > 638: return 21
            elif daydiff > 608: return 20
            elif daydiff > 577: return 19
            elif daydiff > 547: return 18
            elif daydiff > 516: return 17
            elif daydiff > 486: return 16
            elif daydiff > 456: return 15
            elif daydiff > 424: return 14
            elif daydiff > 394: return 13
            elif daydiff > 364: return 12
            elif daydiff > 333: return 11
            elif daydiff > 303: return 10
            elif daydiff > 273: return 9
            elif daydiff > 243: return 8
            elif daydiff > 213: return 7
            elif daydiff > 182: return 6
            elif daydiff > 151: return 5
            elif daydiff > 121: return 4
            elif daydiff > 91: return 3
            elif daydiff > 61: return 2
            elif daydiff > 30: return 1
            else: return 0
        except:
            return 0
    
    hpacc_df = hpacc_df.with_columns(
        pl.col("BLDATE").map_elements(calculate_mtharr, return_dtype=pl.Int64).alias("MTHARR")
    ).select(["ACCTNO", "NOTENO", "PRODUCT", "BORSTAT", "BALANCE", 
              "BLDATE", "ISSDTE", "MTHARR", "BRANCH"])
    
    # 3. Merge with BRANCH abbreviation
    try:
        with open(base / "BRHFILE", 'r') as f:
            brh_lines = []
            for line in f:
                if len(line) >= 9:
                    branch = int(line[1:4])
                    brabbr = line[5:8].strip()
                    brh_lines.append({"BRANCH": branch, "BRABBR": brabbr})
        brhdata_df = pl.DataFrame(brh_lines)
    except:
        brhdata_df = pl.DataFrame({"BRANCH": [], "BRABBR": []})
    
    hpacc_df = hpacc_df.sort("BRANCH")
    brhdata_df = brhdata_df.sort("BRANCH")
    hpacc_df = hpacc_df.join(brhdata_df, on="BRANCH", how="left")
    
    # 4. Merge with CIS
    hpcis_df = hpcis_df.sort("ACCTNO")
    hpacc_df = hpacc_df.sort("ACCTNO")
    hpacc_df = hpacc_df.join(hpcis_df, on="ACCTNO", how="inner")
    
    # 5. Summarize by BRANCH and ICNO
    hpacc_df = hpacc_df.sort(["BRANCH", "ICNO"])
    hpic_df = hpacc_df.group_by(["BRANCH", "ICNO"]).agg(
        pl.sum("BALANCE").alias("BALANCE_SUM")
    )
    
    # 6. Top 10 customers per branch
    hpic_df = hpic_df.sort(["BRANCH", "BALANCE_SUM"], descending=[False, True])
    
    # Add rank within each branch
    hpic_df = hpic_df.with_columns(
        pl.int_range(1, pl.count() + 1).over("BRANCH").alias("N")
    ).filter(pl.col("N") <= 10)
    
    # Rename and merge back
    hpic_df = hpic_df.rename({"BALANCE_SUM": "TOTBAL"}).sort(["BRANCH", "ICNO"])
    hpacc_df = hpacc_df.sort(["BRANCH", "ICNO"])
    hpacc1_df = hpacc_df.join(hpic_df.select(["BRANCH", "ICNO", "TOTBAL"]), 
                             on=["BRANCH", "ICNO"], how="inner")
    
    # Final sort
    hpacc1_df = hpacc1_df.sort(["BRANCH", "TOTBAL", "ICNO", "ACCTNO"], 
                              descending=[False, True, False, False])
    
    # 7. Generate report with Islamic bank header
    generate_islamic_hp_report(hpacc1_df, rdate)
    
    print(f"Processing complete. Top {len(hpacc1_df)} records identified.")

def generate_islamic_hp_report(df, rdate):
    """Generate formatted report for Islamic bank"""
    if df.is_empty():
        return
    
    # Group by BRANCH for page breaks
    branches = df["BRANCH"].unique().to_list()
    
    with open("HPCOLD_ISLAMIC.txt", 'w') as f:
        page_num = 0
        
        for branch in branches:
            branch_df = df.filter(pl.col("BRANCH") == branch).sort(
                ["TOTBAL", "ICNO", "ACCTNO"], descending=[True, False, False]
            )
            
            page_num += 1
            # **CHANGED: Islamic bank header**
            f.write(f"PUBLIC ISLAMIC BANK BERHAD{' ' * 58}{rdate}\n")
            f.write(f"{' ' * 90}PAGE NO : {page_num}\n")
            f.write(f"TOP TEN LARGE ACCOUNTS FOR HPD (CONVENTIONAL & AITAB) AS AT {rdate}\n")
            f.write(f"REPORT ID: EIIHPTOP\n")  # **CHANGED: EIIHPTOP instead of EIMHPTOP**
            f.write(f"\n")
            f.write(f"BRANCH CODE= {branch:03d}\n")
            f.write(f"\n")
            f.write(f"{' ' * 12}NOTE{' ' * 30}LOAN{' ' * 5}BORROWER{' ' * 20}MONTH{' ' * 9}ISSUE\n")
            f.write(f"MNI NO{' ' * 6}NO{' ' * 6}NAME{' ' * 25}TYPE{' ' * 5}STATUS{' ' * 8}NET BALANCE{' ' * 6}PASS DUE{' ' * 9}DATE\n")
            f.write(f"{'-' * 42}{'-' * 42}{'-' * 20}\n")
            
            # Process each ICNO group within branch
            branch_total = 0
            icnos = branch_df["ICNO"].unique().to_list()
            
            for icno in icnos:
                ic_df = branch_df.filter(pl.col("ICNO") == icno)
                ic_total = 0
                
                for row in ic_df.iter_rows(named=True):
                    # Format values
                    acctno = str(row.get('ACCTNO', '')).ljust(12)
                    noteno = str(row.get('NOTENO', '')).ljust(6)
                    custname = (row.get('CUSTNAME', '')[:30]).ljust(30)
                    product = str(row.get('PRODUCT', '')).ljust(4)
                    borstat = str(row.get('BORSTAT', '')).ljust(6)
                    balance = f"{row.get('BALANCE', 0):,.2f}".rjust(16)
                    mtharr = f"{row.get('MTHARR', 0):,.0f}".rjust(6)
                    
                    # Format date
                    issdate = "        "
                    if 'ISSDTE' in row and row['ISSDTE']:
                        try:
                            if isinstance(row['ISSDTE'], (datetime, pl.Date)):
                                issdate = row['ISSDTE'].strftime("%d%b%y").upper()
                            else:
                                issdate = str(row['ISSDTE'])[:8]
                        except:
                            issdate = "        "
                    
                    f.write(f"{acctno}{noteno}{custname}{product}{borstat}{balance}{mtharr}{issdate}\n")
                    
                    ic_total += row.get('BALANCE', 0)
                    branch_total += row.get('BALANCE', 0)
                
                # ICNO total
                f.write(f"{' ' * 57}----------------\n")
                f.write(f"{' ' * 50}TOTAL: {ic_total:,.2f}\n".rjust(80))
                f.write(f"{' ' * 57}================\n\n")
            
            # Branch total
            f.write(f"{' ' * 57}----------------\n")
            f.write(f"{' ' * 37}BRANCH TOTAL: {branch_total:,.2f}\n".rjust(80))
            f.write(f"{' ' * 57}================\n\n")
            
            # Page break
            f.write("\f\n")  # Form feed for new page
    
    print(f"Islamic bank report saved to HPCOLD_ISLAMIC.txt")

# Simplified version
def eiihptop_simple():
    """Simplified Islamic bank version"""
    base = Path.cwd()
    
    # Get date
    reptdate = pl.read_parquet(base / "LOAN/REPTDATE.parquet")["REPTDATE"][0]
    
    # Week logic
    reptday = reptdate.day
    if reptday == 8:
        wk = '1'
    elif reptday == 15:
        wk = '2'
    elif reptday == 22:
        wk = '3'
    else:
        wk = '4'
    
    reptmon = f"{reptdate.month:02d}"
    rdate = reptdate.strftime("%d%m%y")
    
    print(f"EIIHPTOP - Islamic Bank HPD Accounts")
    print(f"Week: {wk}, Month: {reptmon}, Date: {rdate}")
    
    # Read data
    try:
        df = pl.read_parquet(base / f"LOAN/LOAN{reptmon}{wk}.parquet")
    except:
        print(f"No data for LOAN{reptmon}{wk}")
        return
    
    # Filter HPD products (simplified)
    hpd_products = ["HP1", "HP2", "HP3"]  # Example
    df = df.filter(
        (pl.col("PRODUCT").is_in(hpd_products)) &
        (pl.col("BALANCE") > 0)
    )
    
    if df.is_empty():
        print("No HPD accounts found")
        return
    
    # Read CIS data
    try:
        cis_df = pl.read_parquet(base / "CIS/LOAN.parquet")
        # Filter and get customer info
        cis_df = cis_df.filter(
            (~pl.col("CACCCODE").is_in(["017", "021", "028"])) &
            (pl.col("SECCUST") == "901")
        )
        cis_df = cis_df.with_columns(
            pl.coalesce(pl.col("NEWIC"), pl.col("OLDIC")).alias("ICNO")
        ).select(["ACCTNO", "ICNO", "CUSTNAME"])
        
        # Merge
        df = df.join(cis_df, on="ACCTNO", how="inner")
    except:
        print("CIS data not available")
        return
    
    # Group by BRANCH and ICNO
    summary = df.group_by(["BRANCH", "ICNO"]).agg(
        pl.sum("BALANCE").alias("TOTBAL"),
        pl.first("CUSTNAME").alias("CUSTNAME"),
        pl.count().alias("ACCOUNT_COUNT")
    )
    
    # Get top 10 per branch
    top10 = []
    for branch in summary["BRANCH"].unique().to_list():
        branch_df = summary.filter(pl.col("BRANCH") == branch)
        branch_top = branch_df.sort("TOTBAL", descending=True).head(10)
        top10.append(branch_top)
    
    if top10:
        result_df = pl.concat(top10)
        
        # Generate report
        with open("HPCOLD_ISLAMIC_SIMPLE.txt", 'w') as f:
            f.write(f"PUBLIC ISLAMIC BANK BERHAD\n")
            f.write(f"TOP TEN LARGE ACCOUNTS FOR HPD\n")
            f.write(f"AS AT {rdate}\n")
            f.write(f"=" * 60 + "\n\n")
            
            for row in result_df.iter_rows(named=True):
                f.write(f"Branch: {row['BRANCH']}, ICNO: {row['ICNO']}, ")
                f.write(f"Customer: {row['CUSTNAME'][:20]}, ")
                f.write(f"Total: {row['TOTBAL']:,.2f}, ")
                f.write(f"Accounts: {row['ACCOUNT_COUNT']}\n")
        
        print(f"Report saved: HPCOLD_ISLAMIC_SIMPLE.txt")
        print(f"Total accounts in report: {len(result_df)}")
    else:
        print("No data to report")

if __name__ == "__main__":
    eiihptop_simple()
