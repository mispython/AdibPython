import duckdb
import polars as pl
from pathlib import Path
from datetime import datetime
import pyreadstat  # For reading SAS7BDAT files
import warnings
import os

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
        if not file_path.exists():
            print(f"  File does not exist: {file_path}")
            return pl.DataFrame()
        
        print(f"Reading SAS file: {file_path}")
        df, meta = pyreadstat.read_sas7bdat(str(file_path))
        pl_df = pl.from_pandas(df)
        print(f"  Successfully read {len(pl_df)} rows")
        return pl_df
    except Exception as e:
        print(f"  Error reading {file_path}: {e}")
        return pl.DataFrame()

def read_parquet_or_empty(file_path):
    """Read parquet file or return empty DataFrame if file doesn't exist"""
    try:
        if not file_path.exists():
            print(f"  Parquet file does not exist: {file_path}")
            return pl.DataFrame()
        
        print(f"Reading parquet file: {file_path}")
        df = pl.read_parquet(file_path)
        print(f"  Successfully read {len(df)} rows")
        return df
    except Exception as e:
        print(f"  Error reading {file_path}: {e}")
        return pl.DataFrame()

def read_flat_file(file_path, delimiter=","):
    """Read flat file (CSV, TSV, etc.) and return as Polars DataFrame"""
    try:
        if not file_path.exists():
            print(f"  Flat file does not exist: {file_path}")
            return pl.DataFrame()
        
        print(f"Reading flat file: {file_path}")
        
        # Try to determine file type and read accordingly
        if file_path.suffix.lower() in ['.csv', '.txt']:
            # Try to read with comma delimiter
            try:
                df = pl.read_csv(file_path, separator=delimiter)
                print(f"  Successfully read {len(df)} rows with delimiter '{delimiter}'")
                return df
            except Exception as e:
                # Try tab delimiter
                try:
                    df = pl.read_csv(file_path, separator="\t")
                    print(f"  Successfully read {len(df)} rows with tab delimiter")
                    return df
                except:
                    # Try pipe delimiter
                    try:
                        df = pl.read_csv(file_path, separator="|")
                        print(f"  Successfully read {len(df)} rows with pipe delimiter")
                        return df
                    except:
                        # Try with inferring schema
                        df = pl.read_csv(file_path, infer_schema_length=1000)
                        print(f"  Successfully read {len(df)} rows with inferred schema")
                        return df
        else:
            print(f"  Unsupported file format: {file_path.suffix}")
            return pl.DataFrame()
    except Exception as e:
        print(f"  Error reading flat file {file_path}: {e}")
        return pl.DataFrame()

# Step 2: Load and process BRANCH lookup from flat file
print("\nStep 2: Loading BRANCH lookup from flat file...")
branch_file = lookup_path / "LKP_BRANCH"  # Assuming no extension or .csv/.txt
brch_df = pl.DataFrame()

# Try different file extensions
possible_extensions = ['', '.csv', '.txt', '.dat', '.tsv']
for ext in possible_extensions:
    file_path = Path(str(branch_file) + ext)
    if file_path.exists():
        brch_df = read_flat_file(file_path)
        if not brch_df.is_empty():
            break

if brch_df.is_empty():
    # Try to find any file with LKP_BRANCH in the name
    for file in lookup_path.glob("*LKP_BRANCH*"):
        if file.is_file():
            brch_df = read_flat_file(file)
            if not brch_df.is_empty():
                break

if not brch_df.is_empty():
    # Print columns to help debug
    print(f"  Columns in BRANCH file: {brch_df.columns}")
    
    # Look for common column names
    branch_col = None
    label_col = None
    status_col = None
    
    # Common column name patterns
    for col in brch_df.columns:
        col_upper = col.upper()
        if 'BRANCH' in col_upper or col_upper in ['BRANCH', 'BR', 'BRNCH']:
            branch_col = col
        elif 'BRABBR' in col_upper or 'ABBR' in col_upper or 'LABEL' in col_upper or 'NAME' in col_upper:
            label_col = col
        elif 'BRSTAT' in col_upper or 'STAT' in col_upper or 'STATUS' in col_upper:
            status_col = col
    
    if branch_col and label_col:
        if status_col:
            brch_df = brch_df.filter(pl.col(status_col) != "C")
        
        brch_df = brch_df.select([
            pl.col(branch_col).alias("START"),
            pl.col(label_col).alias("LABEL")
        ]).sort("START")
        
        # Create branch lookup dictionary
        branch_lookup = dict(zip(brch_df["START"].to_list(), brch_df["LABEL"].to_list()))
        print(f"  Loaded {len(branch_lookup)} branch mappings")
    else:
        print(f"  Warning: Could not identify required columns. Available columns: {brch_df.columns}")
        branch_lookup = {}
else:
    branch_lookup = {}
    print("  Warning: Could not load branch lookup data")

# Step 3: Load and process CUSTCODE to identify staff from flat file
print("\nStep 3: Loading CUSTCODE lookup from flat file...")
custcode_file = lookup_path / "LKP_CUSTCODE"  # Assuming no extension or .csv/.txt
code_df = pl.DataFrame()

# Try different file extensions
for ext in possible_extensions:
    file_path = Path(str(custcode_file) + ext)
    if file_path.exists():
        code_df = read_flat_file(file_path)
        if not code_df.is_empty():
            break

if code_df.is_empty():
    # Try to find any file with LKP_CUSTCODE in the name
    for file in lookup_path.glob("*LKP_CUSTCODE*"):
        if file.is_file():
            code_df = read_flat_file(file)
            if not code_df.is_empty():
                break

if not code_df.is_empty():
    print(f"  Columns in CUSTCODE file: {code_df.columns}")
    
    # Look for CUSTNO column
    custno_col = None
    for col in code_df.columns:
        col_upper = col.upper()
        if 'CUSTNO' in col_upper or col_upper in ['CUSTNO', 'CUSTNUM', 'CUSTOMERNO']:
            custno_col = col
            break
    
    if custno_col:
        # Look for CUST01-CUST20 columns
        cust_cols = []
        for i in range(1, 21):
            for col in code_df.columns:
                col_upper = col.upper()
                if f'CUST{i:02d}' == col_upper or f'CUST{i}' == col_upper:
                    cust_cols.append(col)
                    break
        
        if cust_cols:
            # Check if any CUST01-CUST20 contains '002'
            code_df = code_df.filter(
                pl.concat([pl.col(c) == "002" for c in cust_cols]).any()
            ).select(pl.col(custno_col).alias("CUSTNO")).unique().sort("CUSTNO")
            print(f"  Found {len(code_df)} staff customer codes")
        else:
            print(f"  Warning: Could not find CUST01-CUST20 columns")
            code_df = pl.DataFrame()
    else:
        print(f"  Warning: Could not find CUSTNO column")
        code_df = pl.DataFrame()
else:
    print("  Warning: Could not load CUSTCODE data")
    code_df = pl.DataFrame()

# Step 4: Load CIS customer data from SAS file
print("\nStep 4: Loading CIS data...")
cis_sas_file = cis_mart / "enrh_exdwh_cisr1.sas7bdat"
cis_df = read_sas_file(cis_sas_file)

if not cis_df.is_empty():
    # Check for PRISEC column
    if "PRISEC" in cis_df.columns:
        cis_df = cis_df.filter(pl.col("PRISEC") == 901).select([
            "ACCTNO",
            "CUSTNAME",
            pl.col("CUSTNO").cast(pl.Int64).alias("CUSTNO")
        ]).sort("CUSTNO")
        print(f"  Filtered to {len(cis_df)} rows with PRISEC=901")
    else:
        print(f"  Warning: PRISEC column not found in CIS data. Using all rows.")
        cis_df = cis_df.select([
            "ACCTNO",
            "CUSTNAME",
            pl.col("CUSTNO").cast(pl.Int64).alias("CUSTNO")
        ]).sort("CUSTNO")
else:
    print("  Warning: Could not load CIS data")
    cis_df = pl.DataFrame()

# Step 5: Merge to get staff accounts
print("\nStep 5: Identifying staff accounts...")
if not cis_df.is_empty() and not code_df.is_empty():
    staf_df = code_df.join(cis_df, on="CUSTNO", how="inner").select("ACCTNO").sort("ACCTNO")
    print(f"  Found {len(staf_df)} staff accounts")
else:
    staf_df = pl.DataFrame()
    print("  Warning: Could not create staff accounts list")

# Step 6: Load DEPOSIT accounts
print("\nStep 6: Loading DEPOSIT accounts...")
deposit_file = deposit_staging / "STG_DP_DPTRBLGS.parquet"
saving_df = read_parquet_or_empty(deposit_file)

if not saving_df.is_empty():
    # Filter out current accounts (3990000000-3999999999)
    depo_df = saving_df.filter(
        ~((pl.col("ACCTNO") >= 3990000000) & (pl.col("ACCTNO") <= 3999999999))
    ).select(["ACCTNO", "PRODUCT", "BRANCH"]).sort("ACCTNO")
    print(f"  Loaded {len(depo_df)} deposit accounts")
else:
    depo_df = pl.DataFrame()
    print("  Warning: Could not load deposit accounts")

# Step 7: Separate staff and customer deposit accounts
print("\nStep 7: Separating staff and customer deposit accounts...")
if not cis_df.is_empty() and not staf_df.is_empty() and not depo_df.is_empty():
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
    print("  Warning: Missing data for deposit account separation")

# Step 8: Load LOAN accounts
print("\nStep 8: Loading LOAN accounts...")
loan_acct_file = loan_staging / "STG_LN_ACCTFILE.parquet"
loan_acc3_file = loan_staging / "STG_LN_ACC3FILE.parquet"

loan_acct_df = read_parquet_or_empty(loan_acct_file)
loan_acc3_df = read_parquet_or_empty(loan_acc3_file)

if not loan_acct_df.is_empty() and not loan_acc3_df.is_empty():
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
    print(f"  Loaded {len(loan_df)} loan accounts")
else:
    loan_df = pl.DataFrame()
    print("  Warning: Could not load loan accounts")

# Step 9: Separate staff and customer loan accounts
print("\nStep 9: Separating staff and customer loan accounts...")
if not cis_df.is_empty() and not staf_df.is_empty() and not loan_df.is_empty():
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
    print("  Warning: Missing data for loan account separation")

# Step 10: Load ALL 4 weekly deposit transaction files
print(f"\nStep 10: Loading transaction files from: {dptr_path}")
dptr_frames = []
# Always load all 4 weeks for the current month
for week in range(1, 5):
    file_pattern = f"dpbtran{macro_vars['REPTYEAR']}{macro_vars['REPTMON']}{week:02d}.sas7bdat"
    file_path = dptr_path / file_pattern
    df_week = read_sas_file(file_path)
    if not df_week.is_empty():
        dptr_frames.append(df_week)

if dptr_frames:
    # Combine all transaction files
    dptr_df = pl.concat(dptr_frames, how="vertical")
    
    # Filter by date range if REPTDATE column exists and is datetime
    if 'REPTDATE' in dptr_df.columns:
        try:
            # Ensure REPTDATE is datetime
            if dptr_df['REPTDATE'].dtype != pl.Date and dptr_df['REPTDATE'].dtype != pl.Datetime:
                dptr_df = dptr_df.with_columns(
                    pl.col("REPTDATE").str.strptime(pl.Date, format="%Y-%m-%d").alias("REPTDATE_DATE")
                )
            else:
                dptr_df = dptr_df.with_columns(pl.col("REPTDATE").alias("REPTDATE_DATE"))
            
            dptr_df = dptr_df.filter(
                (pl.col("REPTDATE_DATE") >= stdate) & 
                (pl.col("REPTDATE_DATE") <= report_date)
            )
        except Exception as e:
            print(f"  Note: Could not filter by date: {e}")
            # Continue with all data
    
    dptr_df = dptr_df.sort("ACCTNO")
    print(f"  Loaded {len(dptr_df)} transactions from {len(dptr_frames)} weekly files")
else:
    dptr_df = pl.DataFrame()
    print("  Warning: No transaction files found or could not be read")

# Step 11: Load CIS relationship data
print("\nStep 11: Loading CIS relationship data...")
cisrl_file = cis_staging / f"STG_CIS_CISRLCC.parquet"
cisrl1_df = read_parquet_or_empty(cisrl_file)

if not cisrl1_df.is_empty():
    cisrl1_df = cisrl1_df.sort(["CUSTNO", "CUSTNO2"])
    print(f"  Loaded CIS relationship data: {len(cisrl1_df)} rows")
else:
    print("  Warning: CIS relationship file not found")

# ==================== PART 1: Staff to Customer Transfers ====================

print("\n" + "="*60)
print("PART 1: Processing Staff to Customer transfers...")
print("="*60)

if dptr_df.is_empty():
    print("  No transaction data available")
elif dpstaf_df.is_empty():
    print("  No staff deposit accounts available")
elif lncust_df.is_empty():
    print("  No customer loan accounts available")
else:
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
            
            print(f"\n✓ Staff to Customer report saved to: {report_file_sc}")
            print(f"  Total Staff to Customer transfers: {len(tranz_sc_df)}")
        else:
            print("  No transactions matched customer loan accounts")
    else:
        print("  No transactions found for staff accounts")

# ==================== PART 2: Customer to Staff Transfers ====================

print("\n" + "="*60)
print("PART 2: Processing Customer to Staff transfers...")
print("="*60)

if dptr_df.is_empty():
    print("  No transaction data available")
elif dpcust_df.is_empty():
    print("  No customer deposit accounts available")
elif dpstaf_df.is_empty():
    print("  No staff deposit accounts available")
else:
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
            
            print(f"\n✓ Customer to Staff report saved to: {report_file_cs}")
            print(f"  Total Customer to Staff transfers: {len(tranz_cs_df)}")
        else:
            print("  No transactions matched staff accounts")
    else:
        print("  No transactions found for customer accounts")

# Close DuckDB connection
conn.close()

print("\n" + "="*60)
print("PROCESSING COMPLETE")
print("="*60)
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
    staff_to_customer = len(tranz_sc_df) if 'tranz_sc_df' in locals() else 0
    customer_to_staff = len(tranz_cs_df) if 'tranz_cs_df' in locals() else 0
    f.write(f"Staff to Customer Transfers: {staff_to_customer}\n")
    f.write(f"Customer to Staff Transfers: {customer_to_staff}\n")
    f.write(f"Total Transfers: {staff_to_customer + customer_to_staff}\n")
    f.write(f"Output Directory: {output_path}\n")

print(f"\n✓ Summary report saved to: {summary_file}")

# List all output files
print("\nGenerated files:")
for file in output_path.glob("*.txt"):
    print(f"  {file.name}")
