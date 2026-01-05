import duckdb
import polars as pl
from pathlib import Path
from datetime import datetime

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

# Report date (configurable - set to desired date)
report_date = datetime.now()  # Change this as needed

# Initialize DuckDB connection
conn = duckdb.connect()

# Step 1: Process REPTDATE and set macro variables
day = report_date.day

if day == 8:
    nowk, sday = 1, 1
elif day == 15:
    nowk, sday = 2, 9
elif day == 22:
    nowk, sday = 3, 16
else:
    nowk, sday = 4, 23

stdate = report_date.replace(day=1)

macro_vars = {
    'NOWK': f"{nowk:02d}",
    'NOWK1': f"{nowk:01d}",
    'REPTYEAR': f"{report_date.year % 100:02d}",
    'REPTMON': f"{report_date.month:02d}",
    'REPTDAY': f"{report_date.day:02d}",
    'RDATE': report_date.strftime('%d/%m/%Y'),
    'SDATE': stdate.strftime('%d/%m/%Y'),
    'EDATE': report_date.strftime('%d/%m/%Y'),
    'RPTDT': report_date.strftime('%Y%m%d'),
    'REPTDATE': report_date
}

print(f"Processing for week {macro_vars['NOWK']} of {macro_vars['REPTMON']}/{macro_vars['REPTYEAR']}")

# Step 2: Load and process BRANCH lookup
brch_df = pl.read_parquet(lookup_path / "LKP_BRANCH.parquet")
brch_df = brch_df.filter(pl.col("BRSTAT") != "C").select([
    pl.col("BRANCH").alias("START"),
    pl.col("BRABBR").alias("LABEL")
]).sort("START")

# Create branch lookup dictionary
branch_lookup = dict(zip(brch_df["START"].to_list(), brch_df["LABEL"].to_list()))

# Step 3: Load and process CUSTCODE to identify staff
code_df = pl.read_parquet(lookup_path / "LKP_CUSTCODE.parquet")

# Check if any CUST01-CUST20 contains '002'
cust_cols = [f"CUST{i:02d}" for i in range(1, 21)]
code_df = code_df.filter(
    pl.concat([pl.col(c) == "002" for c in cust_cols]).any()
).select("CUSTNO").unique().sort("CUSTNO")

# Step 4: Load CIS customer data
cis_df = conn.execute(f"""
    SELECT 
        ACCTNO,
        CUSTNAME,
        CAST(CUSTNO AS BIGINT) as CUSTNO1
    FROM read_sas('{cis_mart / "enrh_exdwh_cisr1.sas7bdat"}')
    WHERE PRISEC = 901
""").pl()

cis_df = cis_df.rename({"CUSTNO1": "CUSTNO"}).sort("CUSTNO")

# Step 5: Merge to get staff accounts
staf_df = code_df.join(cis_df, on="CUSTNO", how="inner").select("ACCTNO").sort("ACCTNO")

# Step 6: Load DEPOSIT accounts
saving_df = pl.read_parquet(deposit_staging / "STG_DP_DPTRBLGS.parquet")
# Note: Assuming current accounts are in same file or separate - adjust as needed
depo_df = saving_df.filter(
    ~((pl.col("ACCTNO") >= 3990000000) & (pl.col("ACCTNO") <= 3999999999))
).select(["ACCTNO", "PRODUCT", "BRANCH"]).sort("ACCTNO")

# Step 7: Separate staff and customer deposit accounts
cis_df = cis_df.sort("ACCTNO")
dpstaf_df = depo_df.join(staf_df, on="ACCTNO", how="inner").join(
    cis_df, on="ACCTNO", how="left"
)
dpcust_df = depo_df.join(staf_df, on="ACCTNO", how="anti").join(
    cis_df, on="ACCTNO", how="left"
)

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

# Step 9: Separate staff and customer loan accounts
lnstaf_df = loan_df.join(staf_df, on="ACCTNO", how="inner").join(
    cis_df, on="ACCTNO", how="left"
)
lncust_df = loan_df.join(staf_df, on="ACCTNO", how="anti").join(
    cis_df, on="ACCTNO", how="left"
)

# Step 10: Load deposit transactions based on week
dptr_files = []
nowk_int = int(macro_vars['NOWK'])
for week in range(1, nowk_int + 1):
    file_pattern = f"dpbtran{macro_vars['REPTYEAR']}{macro_vars['REPTMON']}{week:02d}.sas7bdat"
    file_path = dptr_path / file_pattern
    if file_path.exists():
        dptr_files.append(str(file_path))

if dptr_files:
    dptr_df = conn.execute(f"""
        SELECT 
            ACCTNO, TRANCODE, TRANAMT, REPTDATE,
            TIMECTRL, STRAIL1, LTRAIL1, TRACEBR
        FROM read_sas({dptr_files})
    """).pl().sort("ACCTNO")
else:
    dptr_df = pl.DataFrame()
    print("Warning: No transaction files found")

# Step 11: Load CIS relationship data
cisrl_file = cis_staging / f"STG_CIS_CISRLCC.parquet"
if cisrl_file.exists():
    cisrl1_df = pl.read_parquet(cisrl_file).sort(["CUSTNO", "CUSTNO2"])
else:
    cisrl1_df = pl.DataFrame()
    print("Warning: CIS relationship file not found")

# ==================== PART 1: Staff to Customer Transfers ====================

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
    
    # Save to parquet
    output_file_sc = output_path / f"staff_to_customer_transfers_{macro_vars['RPTDT']}.parquet"
    tranz_sc_df.write_parquet(output_file_sc)
    print(f"Staff to Customer transfers saved to: {output_file_sc}")
    
    # Generate text report
    report_file_sc = output_path / f"staff_to_customer_report_{macro_vars['RPTDT']}.txt"
    with open(report_file_sc, 'w') as f:
        f.write("P U B L I C   B A N K   B E R H A D\n")
        f.write("MONTHLY REPORT ON ELECTRONIC FUND TRANSFER BETWEEN STAFF SA/CA TO CUSTOMER HP ACCOUNT\n")
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
    
    print(f"Staff to Customer report saved to: {report_file_sc}")

# ==================== PART 2: Customer to Staff Transfers ====================

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
    
    # Save to parquet
    output_file_cs = output_path / f"customer_to_staff_transfers_{macro_vars['RPTDT']}.parquet"
    tranz_cs_df.write_parquet(output_file_cs)
    print(f"Customer to Staff transfers saved to: {output_file_cs}")
    
    # Generate text report
    report_file_cs = output_path / f"customer_to_staff_report_{macro_vars['RPTDT']}.txt"
    with open(report_file_cs, 'w') as f:
        f.write("P U B L I C   B A N K   B E R H A D\n")
        f.write("MONTHLY REPORT ON ELECTRONIC FUND TRANSFER BETWEEN CUSTOMER CA TO STAFF SA/CA ACCOUNT\n")
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
    
    print(f"Customer to Staff report saved to: {report_file_cs}")

# Close DuckDB connection
conn.close()

print("\n=== Processing Complete ===")
print(f"Report Date: {macro_vars['EDATE']}")
print(f"Week: {macro_vars['NOWK']}")
print(f"All outputs saved to: {output_path}")
