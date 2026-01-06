import polars as pl
import pyreadstat
from pathlib import Path
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

# Configuration - Input paths
loan_staging = Path("/parquet/dwh/LOAN/staging")
deposit_staging = Path("/parquet/dwh/DEPOSIT/staging")
dptr_path = Path("/dwh/cmsbdptr")
cis_staging = Path("/parquet/dwh/CIS/staging")
lookup_path = Path("/sasdata/rawdata/lookup")
cis_mart = Path("/sas/cis/mart/enrichment")

# Output paths
output_path = Path("/parquet/output")
output_path.mkdir(parents=True, exist_ok=True)

# Report date - set to previous month's last processing date
today = datetime.now()
# Get previous month
prev_month = today - relativedelta(months=1)
# Set to last day of previous month for monthly report
report_date = datetime(prev_month.year, prev_month.month, 1) + relativedelta(months=1) - timedelta(days=1)

print(f"Today's date: {today.strftime('%d %B %Y')}")
print(f"Processing monthly report for: {report_date.strftime('%B %Y')}")

# Step 1: Process REPTDATE and set macro variables for MONTHLY report
# For monthly report, we always process all 4 weeks
nowk = 4  # Always process all 4 weeks for monthly report
stdate = report_date.replace(day=1)

macro_vars = {
    'NOWK': f"{nowk:02d}",
    'NOWK1': f"{nowk:01d}",
    'REPTYEAR': f"{report_date.year % 100:02d}",
    'REPTMON': f"{report_date.month:02d}",
    'REPTDAY': f"{report_date.day:02d}",
    'RDATE': today.strftime('%d/%m/%Y'),
    'SDATE': stdate.strftime('%d/%m/%Y'),
    'EDATE': report_date.strftime('%d/%m/%Y'),
    'RPTDT': report_date.strftime('%Y%m%d'),
    'REPTDATE': report_date,
    'REPORT_MONTH_YEAR': report_date.strftime('%B %Y')
}

print(f"Processing Islamic Banking Monthly Report for {macro_vars['REPORT_MONTH_YEAR']}")
print(f"Period: {macro_vars['SDATE']} to {macro_vars['EDATE']}")
print(f"Processing all {nowk} weeks of transaction data")

# Step 2: Load and process BRANCH lookup from flat file
brch_file = lookup_path / "LKP_BRANCH"
brch_records = []
with open(brch_file, 'r') as f:
    for line in f:
        if len(line) >= 50:
            branch = line[1:4].strip()
            brabbr = line[5:8].strip()
            brname = line[11:41].strip()
            brstat = line[49:50].strip()
            if brstat != 'C' and branch:
                try:
                    brch_records.append({
                        'BRANCH': int(branch),
                        'BRABBR': brabbr
                    })
                except ValueError:
                    continue

brch_df = pl.DataFrame(brch_records).sort("BRANCH")
print(f"Loaded {len(brch_df)} branch records")

# Create branch lookup dictionary
branch_lookup = dict(zip(brch_df["BRANCH"].to_list(), brch_df["BRABBR"].to_list()))

# Step 3: Load and process CUSTCODE from flat file to identify staff
code_file = lookup_path / "LKP_CUSTCODE"
code_records = []
with open(code_file, 'r') as f:
    for line in f:
        if len(line) >= 86:
            custno = line[0:12].strip()
            # Check CUST01 through CUST20
            has_002 = False
            for i in range(20):
                pos = 28 + (i * 3)
                cust_code = line[pos:pos+3].strip()
                if cust_code == '002':
                    has_002 = True
                    break
            
            if has_002 and custno:
                try:
                    code_records.append({'CUSTNO': int(custno)})
                except ValueError:
                    continue

code_df = pl.DataFrame(code_records).unique().sort("CUSTNO")
print(f"Loaded {len(code_df)} staff customer records")

# Step 4: Load CIS customer data
cis_sas_file = cis_mart / "enrh_exdwh_cisr1.sas7bdat"
print(f"Reading CIS data from: {cis_sas_file}")
cis_data, cis_meta = pyreadstat.read_sas7bdat(str(cis_sas_file))
cis_df = pl.from_pandas(cis_data)
cis_df = cis_df.filter(pl.col("PRISEC") == 901).select([
    "ACCTNO",
    "CUSTNAME",
    pl.col("CUSTNO").cast(pl.Int64).alias("CUSTNO")
]).sort("CUSTNO")
print(f"Loaded {len(cis_df)} CIS customer records")

# Step 5: Merge to get staff accounts
staf_df = code_df.join(cis_df, on="CUSTNO", how="inner").select("ACCTNO").sort("ACCTNO")
print(f"Identified {len(staf_df)} staff accounts")

# Step 6: Load DEPOSIT accounts
saving_df = pl.read_parquet(deposit_staging / "STG_DP_DPTRBLGS.parquet")
depo_df = saving_df.filter(
    ~((pl.col("ACCTNO") >= 3990000000) & (pl.col("ACCTNO") <= 3999999999))
).select(["ACCTNO", "PRODUCT", "BRANCH"]).sort("ACCTNO")
print(f"Loaded {len(depo_df)} deposit accounts")

# Step 7: Separate staff and customer deposit accounts
cis_df = cis_df.sort("ACCTNO")
dpstaf_df = depo_df.join(staf_df, on="ACCTNO", how="inner").join(
    cis_df, on="ACCTNO", how="left"
)
dpcust_df = depo_df.join(staf_df, on="ACCTNO", how="anti").join(
    cis_df, on="ACCTNO", how="left"
)
print(f"Staff deposit accounts: {len(dpstaf_df)}, Customer deposit accounts: {len(dpcust_df)}")

# Step 8: Load LOAN accounts
loan_acct_df = pl.read_parquet(loan_staging / "STG_LN_ACCTFILE.parquet")
loan_acc3_df = pl.read_parquet(loan_staging / "STG_LN_ACC3FILE.parquet")

loan_df = loan_acct_df.join(
    loan_acc3_df.select(["ACCTNO", "NOTENO", "NTBRCH"]),
    on=["ACCTNO", "NOTENO"],
    how="left"
).with_columns(
    pl.when(pl.col("NTBRCH") != 0)
    .then(pl.col("NTBRCH"))
    .otherwise(pl.col("ACCBRCH"))
    .alias("BRANCH")
).select([
    "ACCTNO", "NOTENO", 
    pl.col("LOANTYPE").alias("PRODUCT"),
    "BRANCH"
]).sort(["ACCTNO", "NOTENO"])
print(f"Loaded {len(loan_df)} loan accounts")

# Step 9: Separate staff and customer loan accounts
lnstaf_df = loan_df.join(staf_df, on="ACCTNO", how="inner").join(
    cis_df, on="ACCTNO", how="left"
)
lncust_df = loan_df.join(staf_df, on="ACCTNO", how="anti").join(
    cis_df, on="ACCTNO", how="left"
)
print(f"Staff loan accounts: {len(lnstaf_df)}, Customer loan accounts: {len(lncust_df)}")

# Step 10: Load ALL deposit transactions for the MONTH - read all 4 weeks
dptr_dfs = []
for week in range(1, 5):  # Always load weeks 1-4 for monthly report
    file_pattern = f"dpbtran{macro_vars['REPTYEAR']}{macro_vars['REPTMON']}{week:02d}.sas7bdat"
    file_path = dptr_path / file_pattern
    if file_path.exists():
        print(f"Reading transaction file: {file_pattern}")
        df_data, df_meta = pyreadstat.read_sas7bdat(str(file_path))
        df = pl.from_pandas(df_data)
        dptr_dfs.append(df)
        print(f"  Week {week}: {len(df)} transactions")
    else:
        print(f"Warning: Transaction file not found: {file_pattern}")

if dptr_dfs:
    dptr_df = pl.concat(dptr_dfs)
    dptr_df = dptr_df.select([
        "ACCTNO", "TRANCODE", "TRANAMT", "REPTDATE",
        "TIMECTRL", "STRAIL1", "LTRAIL1", "TRACEBR"
    ]).sort("ACCTNO")
    print(f"Loaded {len(dptr_dfs)} transaction file(s) with {len(dptr_df)} total records for the month")
else:
    dptr_df = pl.DataFrame()
    print("Warning: No transaction files found")

# Step 11: Load CIS relationship data
cisrl_file = cis_staging / f"STG_CIS_CISRLCC.parquet"
if cisrl_file.exists():
    cisrl1_df = pl.read_parquet(cisrl_file).sort(["CUSTNO", "CUSTNO2"])
    print(f"Loaded {len(cisrl1_df)} CIS relationship records")
else:
    cisrl1_df = pl.DataFrame()
    print("Warning: CIS relationship file not found")

# ==================== PART 1: Staff to Customer Transfers (Islamic) ====================

if not dptr_df.is_empty() and not dpstaf_df.is_empty():
    dpstaf_df = dpstaf_df.sort("ACCTNO")
    
    # Merge staff deposit accounts with transactions
    tran_df = dpstaf_df.join(dptr_df, on="ACCTNO", how="inner")
    
    # Parse account numbers from transaction trails
    tran_df = tran_df.with_columns([
        pl.col("LTRAIL1").str.strip_chars().alias("LTRAIL1_CLEAN"),
        pl.lit(None).cast(pl.Utf8).alias("ACCTNO2"),
        pl.lit(None).cast(pl.Int64).alias("NOTENO2")
    ])
    
    # Extract ACCTNO2 from STRAIL1 when LTRAIL1 is empty and contains 'TO'
    tran_df = tran_df.with_columns(
        pl.when(
            (pl.col("LTRAIL1_CLEAN") == "") & 
            (pl.col("STRAIL1").str.slice(2, 2) == "TO")
        )
        .then(pl.col("STRAIL1").str.slice(7, 10))
        .otherwise(pl.col("ACCTNO2"))
        .alias("ACCTNO2")
    )
    
    # Extract ACCTNO2 and NOTENO2 from LTRAIL1 when it contains '/' at position 11
    tran_df = tran_df.with_columns([
        pl.when(pl.col("LTRAIL1_CLEAN").str.contains("/").fill_null(False))
        .then(pl.col("LTRAIL1_CLEAN").str.slice(0, 10))
        .otherwise(pl.col("ACCTNO2"))
        .alias("ACCTNO2"),
        pl.when(pl.col("LTRAIL1_CLEAN").str.contains("/").fill_null(False))
        .then(pl.col("LTRAIL1_CLEAN").str.slice(11, 5).cast(pl.Int64))
        .otherwise(pl.col("NOTENO2"))
        .alias("NOTENO2")
    ]).sort(["ACCTNO2", "NOTENO2"])
    
    # Prepare customer loan accounts for matching
    accust_df = lncust_df.filter(
        (pl.col("ACCTNO") >= 8000000000) & (pl.col("ACCTNO") <= 8999999999)
    ).with_columns([
        pl.col("ACCTNO").cast(pl.Utf8).alias("ACCTNO2"),
        pl.col("NOTENO").alias("NOTENO2"),
        pl.col("CUSTNAME").alias("CUSTNAME2"),
        pl.col("CUSTNO").alias("CUSTNO2"),
        pl.col("BRANCH").alias("BRANCH2")
    ]).select([
        "ACCTNO2", "NOTENO2", "CUSTNAME2", "CUSTNO2", "BRANCH2"
    ]).sort(["ACCTNO2", "NOTENO2"])
    
    # Merge transactions with customer accounts
    tranx_df = tran_df.join(accust_df, on=["ACCTNO2", "NOTENO2"], how="inner")
    
    # Format transaction details
    tranx_df = tranx_df.with_columns([
        (pl.col("REPTDATE").dt.strftime('%d/%m/%Y') + " " + 
         pl.col("TIMECTRL").str.strip_chars()).alias("TRANDATE"),
        (pl.col("ACCTNO").cast(pl.Utf8) + " " + 
         pl.col("CUSTNAME")).str.strip_chars().alias("DEBIT"),
        (pl.col("ACCTNO2") + " " + 
         pl.col("CUSTNAME2")).str.strip_chars().alias("CREDIT")
    ])
    
    # Add branch abbreviations using map
    tranx_df = tranx_df.with_columns([
        pl.col("BRANCH").map_elements(lambda x: branch_lookup.get(x, ""), return_dtype=pl.Utf8).alias("BRABBR"),
        pl.col("BRANCH2").map_elements(lambda x: branch_lookup.get(x, ""), return_dtype=pl.Utf8).alias("BRABBR2")
    ]).sort(["CUSTNO", "CUSTNO2"])
    
    # Filter by relationship codes
    if not cisrl1_df.is_empty():
        tranx_temp = tranx_df.join(cisrl1_df, on=["CUSTNO", "CUSTNO2"], how="left")
        trany_df = tranx_temp.filter(pl.col("RELATCD2").is_null())
        tranx_df = tranx_temp.filter(pl.col("RELATCD2").is_not_null())
        
        # Swap relationship for reverse lookup
        cisrl2_df = cisrl1_df.rename({
            "RELATCD1": "RELATCD2",
            "RELATCD2": "RELATCD1",
            "CUSTNO": "CUSTNO2",
            "CUSTNO2": "CUSTNO"
        }).sort(["CUSTNO", "CUSTNO2"])
        
        trany_df = trany_df.join(cisrl2_df, on=["CUSTNO", "CUSTNO2"], how="left")
        tranx_df = pl.concat([tranx_df, trany_df])
    
    # Exclude related parties
    excluded_codes = ['001','002','003','004','005','006','007','008','009']
    tranz_sc_df = tranx_df.filter(
        ~pl.col("RELATCD2").is_in(excluded_codes)
    ).sort(["BRANCH", "PRODUCT", "REPTDATE", "TIMECTRL", "ACCTNO"])
    
    # Generate text report (Staff to Customer) - MONTHLY
    report_file_sc = output_path / f"islamic_staff_to_customer_monthly_{macro_vars['REPTYEAR']}{macro_vars['REPTMON']}.txt"
    with open(report_file_sc, 'w') as f:
        f.write("P U B L I C   I S L A M I C   B A N K   B E R H A D\n")
        f.write("MONTHLY REPORT ON ELECTRONIC FUND TRANSFER BETWEEN STAFF SA/CA TO CUSTOMER HP ACCOUNT\n")
        f.write(f"FOR THE MONTH OF {macro_vars['REPORT_MONTH_YEAR'].upper()}\n")
        f.write(f"FROM {macro_vars['SDATE']} TO {macro_vars['EDATE']}\n\n")
        f.write(f"DATE OF REPORT: {macro_vars['RDATE']}\n\n")
        f.write(f"{'ITEM':<7} {'DATE':<20} {'TRANSACTION':<17} {'AMOUNT':<15} {'TRANSACTION':<15} "
                f"{'DEBIT FROM ACCOUNT':<52} {'DEBIT BRANCH':<15} {'CREDIT FROM ACCOUNT':<52} {'CREDIT BRANCH':<15}\n")
        f.write(f"{'':<7} {'(TIME)':<20} {'CODE':<17} {'(RM)':<15} {'BRANCH':<15} "
                f"{'(CUSTOMER NAME)':<52} {'(USER)':<15} {'(CUSTOMER NAME)':<52} {'(CUSTOMER)':<15}\n\n")
        
        for idx, row in enumerate(tranz_sc_df.iter_rows(named=True), 1):
            f.write(f"{idx:<7} {row['TRANDATE']:<20} {row['TRANCODE']:<17} "
                   f"{row['TRANAMT']:>13,.2f}  {row['TRACEBR']:<15} "
                   f"{row['DEBIT']:<52} {row['BRANCH']:03d} {row['BRABBR']:<11} "
                   f"{row['CREDIT']:<52} {row['BRANCH2']:03d} {row['BRABBR2']:<11}\n")
    
    print(f"Islamic Staff to Customer MONTHLY report saved to: {report_file_sc}")
    print(f"Total transactions (Staff to Customer): {len(tranz_sc_df)}")

# ==================== PART 2: Customer to Staff Transfers (Islamic) ====================

if not dptr_df.is_empty() and not dpcust_df.is_empty():
    # Prepare staff deposit accounts for matching
    acstaf_df = dpstaf_df.with_columns([
        pl.col("ACCTNO").cast(pl.Utf8).alias("ACCTNO2"),
        pl.lit(None).cast(pl.Int64).alias("NOTENO2"),
        pl.col("CUSTNAME").alias("CUSTNAME2"),
        pl.col("CUSTNO").alias("CUSTNO2"),
        pl.col("BRANCH").alias("BRANCH2")
    ]).select([
        "ACCTNO2", "NOTENO2", "CUSTNAME2", "CUSTNO2", "BRANCH2"
    ]).sort(["ACCTNO2", "NOTENO2"])
    
    # Filter customer deposit accounts
    accust_df = dpcust_df.filter(
        (pl.col("ACCTNO") >= 3000000000) & (pl.col("ACCTNO") <= 3999999999)
    ).sort("ACCTNO")
    
    # Merge customer accounts with transactions
    tran_df = accust_df.join(dptr_df, on="ACCTNO", how="inner")
    
    # Parse account numbers from transaction trails
    tran_df = tran_df.with_columns([
        pl.col("LTRAIL1").str.strip_chars().alias("LTRAIL1_CLEAN"),
        pl.lit(None).cast(pl.Utf8).alias("ACCTNO2"),
        pl.lit(None).cast(pl.Int64).alias("NOTENO2")
    ])
    
    # Extract ACCTNO2 from STRAIL1
    tran_df = tran_df.with_columns(
        pl.when(
            (pl.col("LTRAIL1_CLEAN") == "") & 
            (pl.col("STRAIL1").str.slice(2, 2) == "TO")
        )
        .then(pl.col("STRAIL1").str.slice(7, 10))
        .otherwise(pl.col("ACCTNO2"))
        .alias("ACCTNO2")
    )
    
    # Extract ACCTNO2 and NOTENO2 from LTRAIL1
    tran_df = tran_df.with_columns([
        pl.when(pl.col("ACCTNO2").str.contains("/").fill_null(False))
        .then(pl.col("LTRAIL1_CLEAN").str.slice(0, 10))
        .otherwise(pl.col("ACCTNO2"))
        .alias("ACCTNO2"),
        pl.when(pl.col("ACCTNO2").str.contains("/").fill_null(False))
        .then(pl.col("LTRAIL1_CLEAN").str.slice(11, 5).cast(pl.Int64))
        .otherwise(pl.col("NOTENO2"))
        .alias("NOTENO2")
    ]).sort(["ACCTNO2", "NOTENO2"])
    
    # Merge with staff accounts
    tranx_df = tran_df.join(acstaf_df, on=["ACCTNO2", "NOTENO2"], how="inner")
    
    # Format transaction details
    tranx_df = tranx_df.with_columns([
        (pl.col("REPTDATE").dt.strftime('%d/%m/%Y') + " " + 
         pl.col("TIMECTRL").str.strip_chars()).alias("TRANDATE"),
        (pl.col("ACCTNO").cast(pl.Utf8) + " " + 
         pl.col("CUSTNAME")).str.strip_chars().alias("DEBIT"),
        (pl.col("ACCTNO2") + " " + 
         pl.col("CUSTNAME2")).str.strip_chars().alias("CREDIT")
    ])
    
    # Add branch abbreviations
    tranx_df = tranx_df.with_columns([
        pl.col("BRANCH").map_elements(lambda x: branch_lookup.get(x, ""), return_dtype=pl.Utf8).alias("BRABBR"),
        pl.col("BRANCH2").map_elements(lambda x: branch_lookup.get(x, ""), return_dtype=pl.Utf8).alias("BRABBR2")
    ]).sort(["CUSTNO", "CUSTNO2"])
    
    # Filter by relationship codes
    if not cisrl1_df.is_empty():
        tranx_temp = tranx_df.join(cisrl1_df, on=["CUSTNO", "CUSTNO2"], how="left")
        trany_df = tranx_temp.filter(pl.col("RELATCD2").is_null())
        tranx_df = tranx_temp.filter(pl.col("RELATCD2").is_not_null())
        
        cisrl2_df = cisrl1_df.rename({
            "RELATCD1": "RELATCD2",
            "RELATCD2": "RELATCD1",
            "CUSTNO": "CUSTNO2",
            "CUSTNO2": "CUSTNO"
        }).sort(["CUSTNO", "CUSTNO2"])
        
        trany_df = trany_df.join(cisrl2_df, on=["CUSTNO", "CUSTNO2"], how="left")
        tranx_df = pl.concat([tranx_df, trany_df])
    
    # Exclude related parties
    tranz_cs_df = tranx_df.filter(
        ~pl.col("RELATCD2").is_in(excluded_codes)
    ).sort(["BRANCH", "PRODUCT", "REPTDATE", "TIMECTRL", "ACCTNO"])
    
    # Generate text report (Customer to Staff) - MONTHLY
    report_file_cs = output_path / f"islamic_customer_to_staff_monthly_{macro_vars['REPTYEAR']}{macro_vars['REPTMON']}.txt"
    with open(report_file_cs, 'w') as f:
        f.write("P U B L I C   I S L A M I C   B A N K   B E R H A D\n")
        f.write("MONTHLY REPORT ON ELECTRONIC FUND TRANSFER BETWEEN CUSTOMER CA TO STAFF SA/CA ACCOUNT\n")
        f.write(f"FOR THE MONTH OF {macro_vars['REPORT_MONTH_YEAR'].upper()}\n")
        f.write(f"FROM {macro_vars['SDATE']} TO {macro_vars['EDATE']}\n\n")
        f.write(f"DATE OF REPORT: {macro_vars['RDATE']}\n\n")
        f.write(f"{'ITEM':<7} {'DATE':<20} {'TRANSACTION':<17} {'AMOUNT':<15} {'TRANSACTION':<15} "
                f"{'DEBIT FROM ACCOUNT':<52} {'DEBIT BRANCH':<15} {'CREDIT FROM ACCOUNT':<52} {'CREDIT BRANCH':<15}\n")
        f.write(f"{'':<7} {'(TIME)':<20} {'CODE':<17} {'(RM)':<15} {'BRANCH':<15} "
                f"{'(CUSTOMER NAME)':<52} {'(USER)':<15} {'(CUSTOMER NAME)':<52} {'(CUSTOMER)':<15}\n\n")
        
        for idx, row in enumerate(tranz_cs_df.iter_rows(named=True), 1):
            f.write(f"{idx:<7} {row['TRANDATE']:<20} {row['TRANCODE']:<17} "
                   f"{row['TRANAMT']:>13,.2f}  {row['TRACEBR']:<15} "
                   f"{row['DEBIT']:<52} {row['BRANCH']:03d} {row['BRABBR']:<11} "
                   f"{row['CREDIT']:<52} {row['BRANCH2']:03d} {row['BRABBR2']:<11}\n")
    
    print(f"Islamic Customer to Staff MONTHLY report saved to: {report_file_cs}")
    print(f"Total transactions (Customer to Staff): {len(tranz_cs_df)}")

print("\n=== Islamic Banking MONTHLY Processing Complete ===")
print(f"Report Month: {macro_vars['REPORT_MONTH_YEAR']}")
print(f"Report Period: {macro_vars['SDATE']} to {macro_vars['EDATE']}")
print(f"Report Generated: {macro_vars['RDATE']}")
print(f"All monthly text reports saved to: {output_path}")
