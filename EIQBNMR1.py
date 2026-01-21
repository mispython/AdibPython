import polars as pl
from pathlib import Path

def eiqbnmr1():
    base = Path.cwd()
    sas_path = base / "SAS"
    btsas_path = base / "BTSAS"
    
    # Define macro variables as Python lists
    unwanted_ln = [110,111,112,113,114,115,116,117,118,119,
                   128,130,131,132,135,136,138,139,140,141,142,199,
                   315,320,325,330,340,355,380,381,500,520,
                   700,705,720,725]
    
    unwanted_od = [107,126,127,128,129,130,131,132,133,134,
                   135,136,140,141,142,143,144,145,146,147,148,
                   149,150,171,172,173,549,550]
    
    corp_custcd = ['4','5','6','13','17','20','30','31','32',
                  '33','34','35','37','38','39','40','45',
                  '57','59','61','62','63','64','71','72',
                  '73','74','75','82','83','84','86','90',
                  '91','92','98']
    
    sme_custcd = ['41','42','43','44','46','47','48','49',
                 '51','52','53','54','66','67','68','69']
    
    # REPTDATE processing
    reptdate_df = pl.read_parquet(sas_path / "REPTDATE.parquet")
    reptdate = reptdate_df["REPTDATE"][0]
    
    # Week determination
    reptday = reptdate.day
    if reptday == 8:
        nowk = '1'
    elif reptday == 15:
        nowk = '2'
    elif reptday == 22:
        nowk = '3'
    else:
        nowk = '4'
    
    reptyear = str(reptdate.year)
    reptmon = f"{reptdate.month:02d}"
    reptday_str = f"{reptdate.day:02d}"
    rdate = reptdate.strftime("%d%m%y")
    
    print(f"REPORT ID: EIQBNMR1")
    print(f"Date: {rdate}, Week: {nowk}, Month: {reptmon}")
    print("=" * 60)
    
    # Helper function to read datasets
    def read_dataset(path, file_name):
        try:
            return pl.read_parquet(path / file_name)
        except:
            return pl.DataFrame()
    
    # Read loan datasets
    loan_df = read_dataset(sas_path, f"LOAN{reptmon}{nowk}.parquet")
    lnwod_df = read_dataset(sas_path, f"LNWOD{reptmon}{nowk}.parquet")
    lnwof_df = read_dataset(sas_path, f"LNWOF{reptmon}{nowk}.parquet")
    
    # Combine loan datasets
    all_loan_df = pl.concat([loan_df, lnwod_df, lnwof_df])
    
    if all_loan_df.is_empty():
        print("No loan data found")
        return
    
    # 1. Process LN accounts (Term Loans)
    if not all_loan_df.is_empty():
        # Filter LN accounts
        ln_data = all_loan_df.filter(
            (pl.col("ACCTYPE") == "LN") &
            (~pl.col("PRODUCT").is_in(unwanted_ln)) &
            (
                ((pl.col("PRODUCT") < 200) | (pl.col("PRODUCT") > 299)) &
                ((pl.col("PRODUCT") < 981) | (pl.col("PRODUCT") > 996))
            )
        )
        
        # Split into CORP and SME
        lncorp_df = ln_data.filter(pl.col("CUSTCD").is_in(corp_custcd))
        lnsme_df = ln_data.filter(pl.col("CUSTCD").is_in(sme_custcd))
    
    # 2. Process OD accounts (Overdrafts)
    if not all_loan_df.is_empty():
        od_data = all_loan_df.filter(
            (pl.col("ACCTYPE") == "OD") &
            (~pl.col("PRODUCT").is_in(unwanted_od))
        )
        
        # Split into CORP and SME
        odcorp_df = od_data.filter(pl.col("CUSTCD").is_in(corp_custcd))
        odsme_df = od_data.filter(pl.col("CUSTCD").is_in(sme_custcd))
    
    # 3. Process BT accounts (Bills/Trust Receipts)
    bt_df = read_dataset(btsas_path, f"BTRAD{reptmon}{nowk}.parquet")
    
    if not bt_df.is_empty():
        # Rename ACCTNO1 to ACCTNO
        bt_df = bt_df.rename({"ACCTNO": "ACCTNO1"})
        bt_df = bt_df.with_columns(
            pl.col("ACCTNO1").alias("ACCTNO"),
            pl.lit("BT").alias("ACCTYPE")
        )
        
        # Split into CORP and SME
        btcorp_df = bt_df.filter(pl.col("CUSTCD").is_in(corp_custcd))
        btsme_df = bt_df.filter(pl.col("CUSTCD").is_in(sme_custcd))
    
    # 4. Combine CORP datasets
    corp_dfs = []
    for df_name in ['lncorp_df', 'odcorp_df', 'btcorp_df']:
        if df_name in locals() and not locals()[df_name].is_empty():
            corp_dfs.append(locals()[df_name].with_columns(
                pl.lit("CORPORATE LOANS").alias("CATEG")
            ))
    
    corp_df = pl.concat(corp_dfs) if corp_dfs else pl.DataFrame()
    
    # 5. Combine SME datasets
    sme_dfs = []
    for df_name in ['lnsme_df', 'odsme_df', 'btsme_df']:
        if df_name in locals() and not locals()[df_name].is_empty():
            sme_dfs.append(locals()[df_name].with_columns(
                pl.lit("SME LOANS").alias("CATEG")
            ))
    
    sme_df = pl.concat(sme_dfs) if sme_dfs else pl.DataFrame()
    
    # 6. Combine all data
    totln_dfs = []
    if not corp_df.is_empty():
        totln_dfs.append(corp_df)
    if not sme_df.is_empty():
        totln_dfs.append(sme_df)
    
    if not totln_dfs:
        print("No data to process")
        return
    
    totln_df = pl.concat(totln_dfs)
    
    # 7. Summarize by CATEG and ACCTYPE
    summary_df = totln_df.group_by(["CATEG", "ACCTYPE"]).agg(
        pl.sum("BALANCE").alias("BALANCE")
    ).sort(["CATEG", "ACCTYPE"])
    
    # 8. Generate report
    generate_bnm_report(summary_df, rdate)
    
    print(f"\nProcessing complete. Summary:")
    for row in summary_df.iter_rows(named=True):
        print(f"  {row['CATEG']} - {row['ACCTYPE']}: {row['BALANCE']:,.2f}")

def generate_bnm_report(df, rdate):
    """Generate BNM report format"""
    if df.is_empty():
        return
    
    print("\n" + "=" * 60)
    print("REPORT ID : EIQBNMR1")
    print(f"PBB - BREAKDOWN OF LOAN BY OPERATING DIVISION {rdate}")
    print("=" * 60)
    
    # Header
    print(f"{'LOAN TYPE':<20} {'A/C TYPE':<10} {'BALANCE':>20}")
    print("-" * 52)
    
    total_all = 0
    
    # Group by CATEG
    categories = df["CATEG"].unique().to_list()
    for category in categories:
        cat_df = df.filter(pl.col("CATEG") == category)
        cat_total = 0
        
        # Print category rows
        for row in cat_df.iter_rows(named=True):
            print(f"{row['CATEG']:<20} {row['ACCTYPE']:<10} {row['BALANCE']:>20,.2f}")
            cat_total += row['BALANCE']
        
        # Category total line
        print(f"{' '*30} {'TOTAL:':<10} {cat_total:>20,.2f}")
        print("-" * 52)
        total_all += cat_total
    
    # Grand total
    print(f"{' '*30} {'GRAND TOTAL:':<10} {total_all:>20,.2f}")
    print("=" * 52)

# Simplified version
def eiqbnmr1_simple():
    """Simplified version"""
    base = Path.cwd()
    
    # Get date
    reptdate = pl.read_parquet(base / "SAS/REPTDATE.parquet")["REPTDATE"][0]
    reptday = reptdate.day
    nowk = '1' if reptday == 8 else '2' if reptday == 15 else '3' if reptday == 22 else '4'
    reptmon = f"{reptdate.month:02d}"
    rdate = reptdate.strftime("%d%m%y")
    
    print(f"EIQBNMR1 - Loan Breakdown by Operating Division")
    print(f"Report Date: {rdate} (Week {nowk})")
    
    # Define filter lists
    unwanted_ln = [110,111,112,113,114,115,116,117,118,119,128,130,131,132,135,136,
                   138,139,140,141,142,199,315,320,325,330,340,355,380,381,500,520,
                   700,705,720,725]
    
    unwanted_od = [107,126,127,128,129,130,131,132,133,134,135,136,140,141,142,
                   143,144,145,146,147,148,149,150,171,172,173,549,550]
    
    corp_cust = ['4','5','6','13','17','20','30','31','32','33','34','35','37','38',
                 '39','40','45','57','59','61','62','63','64','71','72','73','74',
                 '75','82','83','84','86','90','91','92','98']
    
    sme_cust = ['41','42','43','44','46','47','48','49','51','52','53','54',
                '66','67','68','69']
    
    # Read data
    datasets = []
    for suffix in ["", "WOD", "WOF"]:
        try:
            df = pl.read_parquet(base / f"SAS/LOAN{suffix}{reptmon}{nowk}.parquet")
            datasets.append(df)
        except:
            pass
    
    if not datasets:
        print("No data found")
        return
    
    loan_df = pl.concat(datasets)
    
    # Process LN and OD accounts
    results = []
    
    # LN accounts
    ln_df = loan_df.filter(
        (pl.col("ACCTYPE") == "LN") &
        (~pl.col("PRODUCT").is_in(unwanted_ln)) &
        ((pl.col("PRODUCT") < 200) | (pl.col("PRODUCT") > 299)) &
        ((pl.col("PRODUCT") < 981) | (pl.col("PRODUCT") > 996))
    )
    
    # OD accounts
    od_df = loan_df.filter(
        (pl.col("ACCTYPE") == "OD") &
        (~pl.col("PRODUCT").is_in(unwanted_od))
    )
    
    # Process each account type
    for acctype, df in [("LN", ln_df), ("OD", od_df)]:
        if not df.is_empty():
            # Corporate
            corp = df.filter(pl.col("CUSTCD").is_in(corp_cust))
            if not corp.is_empty():
                corp_bal = corp["BALANCE"].sum()
                results.append(("CORPORATE LOANS", acctype, corp_bal))
            
            # SME
            sme = df.filter(pl.col("CUSTCD").is_in(sme_cust))
            if not sme.is_empty():
                sme_bal = sme["BALANCE"].sum()
                results.append(("SME LOANS", acctype, sme_bal))
    
    # BT accounts
    try:
        bt_df = pl.read_parquet(base / f"BTSAS/BTRAD{reptmon}{nowk}.parquet")
        bt_df = bt_df.with_columns(pl.lit("BT").alias("ACCTYPE"))
        
        # Corporate BT
        bt_corp = bt_df.filter(pl.col("CUSTCD").is_in(corp_cust))
        if not bt_corp.is_empty():
            bt_corp_bal = bt_corp["BALANCE"].sum()
            results.append(("CORPORATE LOANS", "BT", bt_corp_bal))
        
        # SME BT
        bt_sme = bt_df.filter(pl.col("CUSTCD").is_in(sme_cust))
        if not bt_sme.is_empty():
            bt_sme_bal = bt_sme["BALANCE"].sum()
            results.append(("SME LOANS", "BT", bt_sme_bal))
    except:
        pass
    
    # Create summary
    if not results:
        print("No results")
        return
    
    summary_df = pl.DataFrame({
        "CATEG": [r[0] for r in results],
        "ACCTYPE": [r[1] for r in results],
        "BALANCE": [r[2] for r in results]
    })
    
    # Sort and display
    summary_df = summary_df.sort(["CATEG", "ACCTYPE"])
    
    print("\n" + "=" * 52)
    print(f"{'LOAN TYPE':<20} {'A/C TYPE':<10} {'BALANCE':>20}")
    print("-" * 52)
    
    # Group by category
    for categ in ["CORPORATE LOANS", "SME LOANS"]:
        cat_df = summary_df.filter(pl.col("CATEG") == categ)
        if not cat_df.is_empty():
            cat_total = 0
            for row in cat_df.iter_rows(named=True):
                print(f"{row['CATEG']:<20} {row['ACCTYPE']:<10} {row['BALANCE']:>20,.2f}")
                cat_total += row['BALANCE']
            print(f"{' '*30} {'TOTAL:':<10} {cat_total:>20,.2f}")
            print("-" * 52)
    
    grand_total = summary_df["BALANCE"].sum()
    print(f"{' '*30} {'GRAND TOTAL:':<10} {grand_total:>20,.2f}")
    print("=" * 52)

if __name__ == "__main__":
    eiqbnmr1_simple()
