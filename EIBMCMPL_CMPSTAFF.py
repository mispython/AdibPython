import duckdb
import polars as pl
from pathlib import Path
from datetime import datetime
import pyreadstat  # For reading SAS7BDAT files
import warnings

# Suppress warnings if needed
warnings.filterwarnings('ignore')

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

def read_sas_file(file_path):
    """Read SAS7BDAT file using pyreadstat and return as Polars DataFrame"""
    try:
        print(f"Reading SAS file: {file_path}")
        df, meta = pyreadstat.read_sas7bdat(str(file_path))
        pl_df = pl.from_pandas(df)
        print(f"  Successfully read {len(pl_df)} rows")
        return pl_df
    except Exception as e:
        print(f"  Error reading {file_path}: {e}")
        return pl.DataFrame()

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

# Step 4: Load CIS customer data from SAS file
cis_sas_file = cis_mart / "enrh_exdwh_cisr1.sas7bdat"
print(f"\nReading CIS data from: {cis_sas_file}")
cis_df = read_sas_file(cis_sas_file)

if not cis_df.is_empty():
    # Filter for PRISEC = 901 and select required columns
    cis_df = cis_df.filter(pl.col("PRISEC") == 901).select([
        "ACCTNO",
        "CUSTNAME",
        pl.col("CUSTNO").cast(pl.Int64).alias("CUSTNO")
    ]).sort("CUSTNO")
    print(f"  Filtered to {len(cis_df)} rows with PRISEC=901")
else:
    print("  Warning: Could not load CIS data")
    cis_df = pl.DataFrame()

# Step 5: Merge to get staff accounts
if not cis_df.is_empty() and not code_df.is_empty():
    staf_df = code_df.join(cis_df, on="CUSTNO", how="inner").select("ACCTNO").sort("ACCTNO")
    print(f"  Found {len(staf_df)} staff accounts")
else:
    staf_df = pl.DataFrame()
    print("  Warning: Could not create staff accounts list")

# Step 6: Load DEPOSIT accounts
saving_df = pl.read_parquet(deposit_staging / "STG_DP_DPTRBLGS.parquet")
# Note: Assuming current accounts are in same file or separate - adjust as needed
depo_df = saving_df.filter(
    ~((pl.col("ACCTNO") >= 3990000000) & (pl.col("ACCTNO") <= 3999999999))
).select(["ACCTNO", "PRODUCT", "BRANCH"]).sort("ACCTNO")

# Step 7: Separate staff and customer deposit accounts
if not cis_df.is_empty() and not staf_df.is_empty():
    cis_df = cis_df.sort("ACCTNO")
    dpstaf_df = depo_df.join(staf_df, on="ACCTNO", how="inner").join(
        cis_df, on="ACCTNO", how="left"
    )
    dpcust_df = depo_df.join(staf_df, on="ACCTNO", how="anti").join(
        cis_df, on="ACCTNO", how="left"
    )
    print(f"  Staff deposit accounts: {len(dpstaf_df)}")
    print(f"  Customer deposit accounts: {len(dpcust_df)}")
else:
    dpstaf_df = pl.DataFrame()
    dpcust_df = pl.DataFrame()

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
if not cis_df.is_empty() and not staf_df.is_empty():
    lnstaf_df = loan_df.join(staf_df, on="ACCTNO", how="inner").join(
        cis_df, on="ACCTNO", how="left"
    )
    lncust_df = loan_df.join(staf_df, on="ACCTNO", how="anti").join(
        cis_df, on="ACCTNO", how="left"
    )
    print(f"  Staff loan accounts: {len(lnstaf_df)}")
    print(f"  Customer loan accounts: {len(lncust_df)}")
else:
    lnstaf_df = pl.DataFrame()
    lncust_df = pl.DataFrame()

# Step 10: Load ALL 4 weekly deposit transaction files
print(f"\nLooking for transaction files in: {dptr_path}")
dptr_frames = []
# Always load all 4 weeks for the current month
for week in range(1, 5):
    file_pattern = f"dpbtran{macro_vars['REPTYEAR']}{macro_vars['REPTMON']}{week:02d}.sas7bdat"
    file_path = dptr_path / file_pattern
    if file_path.exists():
        print(f"Found transaction file: {file_pattern}")
        df_week = read_sas_file(file_path)
        if not df_week.is_empty():
            # Convert REPTDATE to datetime if it's not already
            if 'REPTDATE' in df_week.columns:
                try:
                    # Try to parse date if it's in string format
                    if df_week['REPTDATE'].dtype == pl.Utf8:
                        df_week = df_week.with_columns(
                            pl.col("REPTDATE").str.strptime(pl.Date, format="%Y-%m-%d")
                        )
                except:
                    pass
            dptr_frames.append(df_week)
    else:
        print(f"Warning: Transaction file not found: {file_pattern}")

if dptr_frames:
    # Combine all transaction files
    dptr_df = pl.concat(dptr_frames, how="vertical")
    
    # Filter by date range
    try:
        dptr_df = dptr_df.filter(
            (pl.col("REPTDATE") >= stdate) & 
            (pl.col("REPTDATE") <= report_date)
        )
    except:
        # If date filtering fails, continue with all data
        pass
    
    dptr_df = dptr_df.sort("ACCTNO")
    print(f"Loaded {len(dptr_df)} transactions from {len(dptr_frames)} weekly files")
else:
    dptr_df = pl.DataFrame()
    print("Warning: No transaction files found or could not be read")

# Step 11: Load CIS relationship data
cisrl_file = cis_staging / f"STG_CIS_CISRLCC.parquet"
if cisrl_file.exists():
    cisrl1_df = pl.read_parquet(cisrl_file).sort(["CUSTNO", "CUSTNO2"])
    print(f"Loaded CIS relationship data: {len(cisrl1_df)} rows")
else:
    cisrl1_df = pl.DataFrame()
    print("Warning: CIS relationship file not found")

# ==================== PART 1: Staff to Customer Transfers ====================

if not dptr_df.is_empty() and not dpstaf_df.is_empty():
    print(f"\nProcessing Staff to Customer transfers...")
    dpstaf_df = dpstaf_df.sort("ACCTNO")
    
    # Merge staff deposit accounts with transactions
    tran_df = dpstaf_df.join(dptr_df, on="ACCTNO", how="inner")
    print(f"  Staff transactions found: {len(tran_df)}")
    
    if not tran_df.is_empty():
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
        print(f"  Transactions matched to customer loans: {len(tranx_df)}")
        
        if not tranx_df.is_empty():
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
            
            # Generate text report for Staff to Customer transfers
            report_file_sc = output_path / f"staff_to_customer_report_{macro_vars['RPTDT']}.txt"
            with open(report_file_sc, 'w') as f:
                f.write("P U B L I C   B A N K   B E R H A D\n")
                f.write("MONTHLY REPORT ON ELECTRONIC FUND TRANSFER BETWEEN STAFF SA/CA TO CUSTOMER HP ACCOUNT\n")
                f.write(f"FROM {macro_vars['SDATE']} TO {macro_vars['EDATE']}\n\n")
                f.write(f"DATE OF REPORT: {macro_vars['RDATE']}\n\n")
                f.write(f"TRANSACTION SOURCE FILES: dpbtran{macro_vars['REPTYEAR']}{macro_vars['REPTMON']}01, ")
                f.write(f"dpbtran{macro_vars['REPTYEAR']}{macro_vars['REPTMON']}02, ")
                f.write(f"dpbtran{macro_vars['REPTYEAR']}{macro_vars['REPTMON']}03, ")
                f.write(f"dpbtran{macro_vars['REPTYEAR']}{macro_vars['REPTMON']}04\n")
                f.write(f"TOTAL TRANSACTIONS FOUND: {len(tranz_sc_df)}\n\n")
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
            print(f"Total Staff to Customer transfers: {len(tranz_sc_df)}")
        else:
            print("  No transactions matched customer loan accounts")
    else:
        print("  No transactions found for staff accounts")

# ==================== PART 2: Customer to Staff Transfers ====================

if not dptr_df.is_empty() and not dpcust_df.is_empty():
    print(f"\nProcessing Customer to Staff transfers...")
    
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
    print(f"  Customer transactions found: {len(tran_df)}")
    
    if not tran_df.is_empty():
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
        print(f"  Transactions matched to staff accounts: {len(tranx_df)}")
        
        if not tranx_df.is_empty():
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
            excluded_codes = ['001','002','003','004','005','006','007','008','009']
            tranz_cs_df = tranx_df.filter(
                ~pl.col("RELATCD2").is_in(excluded_codes)
            ).sort(["BRANCH", "PRODUCT", "REPTDATE", "TIMECTRL", "ACCTNO"])
            
            # Generate text report for Customer to Staff transfers
            report_file_cs = output_path / f"customer_to_staff_report_{macro_vars['RPTDT']}.txt"
            with open(report_file_cs, 'w') as f:
                f.write("P U B L I C   B A N K   B E R H A D\n")
                f.write("MONTHLY REPORT ON ELECTRONIC FUND TRANSFER BETWEEN CUSTOMER CA TO STAFF SA/CA ACCOUNT\n")
                f.write(f"FROM {macro_vars['SDATE']} TO {macro_vars['EDATE']}\n\n")
                f.write(f"DATE OF REPORT: {macro_vars['RDATE']}\n\n")
                f.write(f"TRANSACTION SOURCE FILES: dpbtran{macro_vars['REPTYEAR']}{macro_vars['REPTMON']}01, ")
                f.write(f"dpbtran{macro_vars['REPTYEAR']}{macro_vars['REPTMON']}02, ")
                f.write(f"dpbtran{macro_vars['REPTYEAR']}{macro_vars['REPTMON']}03, ")
                f.write(f"dpbtran{macro_vars['REPTYEAR']}{macro_vars['REPTMON']}04\n")
                f.write(f"TOTAL TRANSACTIONS FOUND: {len(tranz_cs_df)}\n\n")
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
            print(f"Total Customer to Staff transfers: {len(tranz_cs_df)}")
        else:
            print("  No transactions matched staff accounts")
    else:
        print("  No transactions found for customer accounts")

# Close DuckDB connection
conn.close()

print("\n=== Processing Complete ===")
print(f"Report Date: {macro_vars['EDATE']}")
print(f"Week: {macro_vars['NOWK']}")
print(f"Transaction Files Processed: {len(dptr_frames)}")
print(f"All reports saved as text files to: {output_path}")

# Generate summary report
summary_file = output_path / f"transfer_summary_{macro_vars['RPTDT']}.txt"
with open(summary_file, 'w') as f:
    f.write("=== TRANSFER REPORT SUMMARY ===\n")
    f.write(f"Report Period: {macro_vars['SDATE']} to {macro_vars['EDATE']}\n")
    f.write(f"Report Generated: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}\n")
    f.write(f"Transaction Files: dpbtran{macro_vars['REPTYEAR']}{macro_vars['REPTMON']}01-04\n")
    f.write(f"Staff to Customer Transfers: {len(tranz_sc_df) if 'tranz_sc_df' in locals() else 0}\n")
    f.write(f"Customer to Staff Transfers: {len(tranz_cs_df) if 'tranz_cs_df' in locals() else 0}\n")
    f.write(f"Output Directory: {output_path}\n")

print(f"Summary report saved to: {summary_file}")
