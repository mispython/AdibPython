import polars as pl
from pathlib import Path
from datetime import datetime
import pyreadstat

# Configuration - Input paths
loan_staging = Path("/parquet/dwh/LOAN/staging")
deposit_staging = Path("/parquet/dwh/DEPOSIT/staging")
dptr_path = Path("/dwh/cmsbdptr")
cis_staging = Path("/parquet/dwh/CIS/staging")
lookup_path = Path("/sasdata/rawdata/lookup")
cis_mart = Path("/sas/cis/mart/enrichment")

# Output paths
output_path = Path("/host/mis/parquet")
output_path.mkdir(parents=True, exist_ok=True)

# Report date
report_date = datetime.now()

# Step 1: Process REPTDATE and set macro variables
day = report_date.day
nowk = 4 if day > 22 else 3 if day > 15 else 2 if day > 8 else 1
stdate = report_date.replace(day=1)

# Monthly format: YYMM (e.g., 2501 for Jan 2025)
monthly_format = f"{report_date.year % 100:02d}{report_date.month:02d}"

macro_vars = {
    'NOWK': f"{nowk:02d}",
    'REPTYEAR': f"{report_date.year % 100:02d}",
    'REPTMON': f"{report_date.month:02d}",
    'RDATE': report_date.strftime('%d/%m/%Y'),
    'SDATE': stdate.strftime('%d/%m/%Y'),
    'EDATE': report_date.strftime('%d/%m/%Y'),
    'MONTHLY': monthly_format
}

print(f"Processing for week {macro_vars['NOWK']} of {macro_vars['REPTMON']}/{macro_vars['REPTYEAR']}")

def read_file(file_path):
    """Read file (parquet, csv, txt, or SAS) and return as Polars DataFrame"""
    if not file_path.exists():
        return pl.DataFrame()
    
    try:
        if file_path.suffix == '.parquet':
            return pl.read_parquet(file_path)
        elif file_path.suffix == '.sas7bdat':
            df, _ = pyreadstat.read_sas7bdat(str(file_path))
            return pl.from_pandas(df)
        else:  # CSV, TXT, etc.
            for sep in [",", "\t", "|", ";"]:
                try:
                    return pl.read_csv(file_path, separator=sep, infer_schema_length=10000)
                except:
                    continue
            return pl.read_csv(file_path, infer_schema_length=10000)
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return pl.DataFrame()

def find_file(base_name, directory):
    """Find file with base name and various extensions"""
    for ext in ['.parquet', '.csv', '.txt', '.dat', '']:
        file_path = directory / f"{base_name}{ext}"
        if file_path.exists():
            return file_path
    return None

# Step 2: Load BRANCH lookup
branch_file = find_file("LKP_BRANCH", lookup_path)
brch_df = read_file(branch_file) if branch_file else pl.DataFrame()
branch_lookup = {}

if not brch_df.is_empty():
    col_map = {col.upper(): col for col in brch_df.columns}
    
    branch_col = next((col_map[k] for k in ['BRANCH', 'BR', 'BRNCH'] if k in col_map), None)
    brabbr_col = next((col_map[k] for k in ['BRABBR', 'ABBR', 'LABEL', 'NAME'] if k in col_map), None)
    brstat_col = next((col_map[k] for k in ['BRSTAT', 'STATUS', 'STAT'] if k in col_map), None)
    
    if branch_col and brabbr_col:
        if brstat_col and brstat_col in brch_df.columns:
            brch_df = brch_df.filter(pl.col(brstat_col) != "C")
        
        brch_df = brch_df.select([
            pl.col(branch_col).alias("START"),
            pl.col(brabbr_col).alias("LABEL")
        ])
        
        branch_lookup = dict(zip(brch_df["START"].to_list(), brch_df["LABEL"].to_list()))

# Step 3: Load CUSTCODE to identify staff
custcode_file = find_file("LKP_CUSTCODE", lookup_path)
code_df = read_file(custcode_file) if custcode_file else pl.DataFrame()

if not code_df.is_empty():
    col_map = {col.upper(): col for col in code_df.columns}
    custno_col = next((col_map[k] for k in ['CUSTNO', 'CUSTNUM', 'CUSTOMERNO'] if k in col_map), None)
    
    if custno_col:
        cust_cols = []
        for i in range(1, 21):
            for pattern in [f'CUST{i:02d}', f'CUST{i}']:
                if pattern in col_map:
                    cust_cols.append(col_map[pattern])
                    break
        
        if cust_cols:
            condition = pl.col(cust_cols[0]) == "002"
            for col in cust_cols[1:]:
                condition = condition | (pl.col(col) == "002")
            
            code_df = code_df.filter(condition).select(pl.col(custno_col).alias("CUSTNO")).unique()

# Step 4: Load CIS customer data
cis_sas_file = cis_mart / "enrh_exdwh_cisr1.sas7bdat"
cis_df = read_file(cis_sas_file)

if not cis_df.is_empty() and all(col in cis_df.columns for col in ["PRISEC", "ACCTNO", "CUSTNAME", "CUSTNO"]):
    cis_df = cis_df.filter(pl.col("PRISEC") == 901).select([
        "ACCTNO", "CUSTNAME", pl.col("CUSTNO").cast(pl.Int64).alias("CUSTNO")
    ])

# Step 5: Get staff accounts
staf_df = pl.DataFrame()
if not cis_df.is_empty() and not code_df.is_empty():
    staf_df = code_df.join(cis_df, on="CUSTNO", how="inner").select("ACCTNO")

# Step 6: Load DEPOSIT accounts
deposit_file = deposit_staging / "STG_DP_DPTRBLGS.parquet"
saving_df = read_file(deposit_file)

if not saving_df.is_empty():
    depo_df = saving_df.filter(
        ~((pl.col("ACCTNO") >= 3990000000) & (pl.col("ACCTNO") <= 3999999999))
    ).select(["ACCTNO", "PRODUCT", "BRANCH"])
else:
    depo_df = pl.DataFrame()

# Step 7: Separate staff and customer deposit accounts
if not cis_df.is_empty() and not staf_df.is_empty() and not depo_df.is_empty():
    dpstaf_df = depo_df.join(staf_df, on="ACCTNO", how="inner").join(cis_df, on="ACCTNO", how="left")
    dpcust_df = depo_df.join(staf_df, on="ACCTNO", how="anti").join(cis_df, on="ACCTNO", how="left")
else:
    dpstaf_df = pl.DataFrame()
    dpcust_df = pl.DataFrame()

# Step 8: Load LOAN accounts
loan_acct_file = loan_staging / "STG_LN_ACCTFILE.parquet"
loan_acc3_file = loan_staging / "STG_LN_ACC3FILE.parquet"

loan_acct_df = read_file(loan_acct_file)
loan_acc3_df = read_file(loan_acc3_file)

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
    ).select(["ACCTNO", "NOTENO", pl.col("LOANTYPE").alias("PRODUCT"), "BRANCH"])
else:
    loan_df = pl.DataFrame()

# Step 9: Separate staff and customer loan accounts
if not cis_df.is_empty() and not staf_df.is_empty() and not loan_df.is_empty():
    lnstaf_df = loan_df.join(staf_df, on="ACCTNO", how="inner").join(cis_df, on="ACCTNO", how="left")
    lncust_df = loan_df.join(staf_df, on="ACCTNO", how="anti").join(cis_df, on="ACCTNO", how="left")
else:
    lnstaf_df = pl.DataFrame()
    lncust_df = pl.DataFrame()

# Step 10: Load ALL 4 weekly transaction files
dptr_frames = []
for week in range(1, 5):
    file_path = dptr_path / f"dpbtran{macro_vars['REPTYEAR']}{macro_vars['REPTMON']}{week:02d}.sas7bdat"
    df = read_file(file_path)
    if not df.is_empty():
        dptr_frames.append(df)

dptr_df = pl.concat(dptr_frames) if dptr_frames else pl.DataFrame()

# Step 11: Load CIS relationship data
cisrl_file = cis_staging / "STG_CIS_CISRLCC.parquet"
cisrl1_df = read_file(cisrl_file)

# Helper function to process transactions
def process_transfers(source_df, target_df, source_is_staff=True):
    """Process transfers between source and target accounts"""
    if dptr_df.is_empty() or source_df.is_empty() or target_df.is_empty():
        return pl.DataFrame()
    
    # Merge source accounts with transactions
    tran_df = source_df.join(dptr_df, on="ACCTNO", how="inner")
    if tran_df.is_empty():
        return pl.DataFrame()
    
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
        pl.when(pl.col("LTRAIL1_CLEAN").str.contains("/").fill_null(False))
        .then(pl.col("LTRAIL1_CLEAN").str.slice(0, 10))
        .otherwise(pl.col("ACCTNO2"))
        .alias("ACCTNO2"),
        pl.when(pl.col("LTRAIL1_CLEAN").str.contains("/").fill_null(False))
        .then(pl.col("LTRAIL1_CLEAN").str.slice(11, 5).cast(pl.Int64))
        .otherwise(pl.col("NOTENO2"))
        .alias("NOTENO2")
    ])
    
    # Prepare target accounts for matching
    if source_is_staff:
        # Staff to Customer: target is customer loan accounts (8000000000-8999999999)
        target_accounts = target_df.filter(
            (pl.col("ACCTNO") >= 8000000000) & (pl.col("ACCTNO") <= 8999999999)
        )
    else:
        # Customer to Staff: target is staff deposit accounts
        target_accounts = target_df.with_columns([
            pl.col("ACCTNO").cast(pl.Utf8).alias("ACCTNO2"),
            pl.lit(None).cast(pl.Int64).alias("NOTENO2"),
            pl.col("CUSTNAME").alias("CUSTNAME2"),
            pl.col("CUSTNO").alias("CUSTNO2"),
            pl.col("BRANCH").alias("BRANCH2")
        ]).select(["ACCTNO2", "NOTENO2", "CUSTNAME2", "CUSTNO2", "BRANCH2"])
    
    # Merge transactions with target accounts
    tranx_df = tran_df.join(target_accounts, on=["ACCTNO2", "NOTENO2"], how="inner")
    if tranx_df.is_empty():
        return pl.DataFrame()
    
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
    ])
    
    # Filter by relationship codes if available
    if not cisrl1_df.is_empty():
        tranx_temp = tranx_df.join(cisrl1_df, on=["CUSTNO", "CUSTNO2"], how="left")
        trany_df = tranx_temp.filter(pl.col("RELATCD2").is_null())
        tranx_df = tranx_temp.filter(pl.col("RELATCD2").is_not_null())
        
        cisrl2_df = cisrl1_df.rename({
            "RELATCD1": "RELATCD2",
            "RELATCD2": "RELATCD1",
            "CUSTNO": "CUSTNO2",
            "CUSTNO2": "CUSTNO"
        })
        
        trany_df = trany_df.join(cisrl2_df, on=["CUSTNO", "CUSTNO2"], how="left")
        tranx_df = pl.concat([tranx_df, trany_df])
    
    # Exclude related parties
    excluded_codes = ['001','002','003','004','005','006','007','008','009']
    return tranx_df.filter(~pl.col("RELATCD2").is_in(excluded_codes)).sort(
        ["BRANCH", "PRODUCT", "REPTDATE", "TIMECTRL", "ACCTNO"]
    )

# Helper function to generate report
def generate_report(transfers, report_type, report_name):
    """Generate text report for transfers"""
    if transfers.is_empty():
        print(f"  No {report_type} transfers found")
        return
    
    report_file = output_path / f"{report_name}{macro_vars['MONTHLY']}.txt"
    
    title = "STAFF SA/CA TO CUSTOMER HP ACCOUNT" if report_type == "Staff to Customer" else "CUSTOMER CA TO STAFF SA/CA ACCOUNT"
    
    with open(report_file, 'w') as f:
        f.write("P U B L I C   B A N K   B E R H A D\n")
        f.write(f"MONTHLY REPORT ON ELECTRONIC FUND TRANSFER BETWEEN {title}\n")
        f.write(f"FROM {macro_vars['SDATE']} TO {macro_vars['EDATE']}\n\n")
        f.write(f"DATE OF REPORT: {macro_vars['RDATE']}\n\n")
        f.write(f"TRANSACTION SOURCE FILES: dpbtran{macro_vars['REPTYEAR']}{macro_vars['REPTMON']}01-04\n")
        f.write(f"TOTAL TRANSACTIONS FOUND: {len(transfers)}\n\n")
        f.write(f"{'ITEM':<7} {'DATE':<20} {'TRANSACTION':<17} {'AMOUNT':<15} {'TRANSACTION':<15} "
                f"{'DEBIT FROM ACCOUNT':<52} {'DEBIT BRANCH':<15} {'CREDIT FROM ACCOUNT':<52} {'CREDIT BRANCH':<15}\n")
        f.write(f"{'':<7} {'(TIME)':<20} {'CODE':<17} {'(RM)':<15} {'BRANCH':<15} "
                f"{'(CUSTOMER NAME)':<52} {'(USER)':<15} {'(CUSTOMER NAME)':<52} {'(CUSTOMER)':<15}\n\n")
        
        for idx, row in enumerate(transfers.iter_rows(named=True), 1):
            f.write(f"{idx:<7} {row['TRANDATE']:<20} {row['TRANCODE']:<17} "
                   f"{row['TRANAMT']:>13,.2f}  {row['TRACEBR']:<15} "
                   f"{row['DEBIT']:<52} {row['BRANCH']:03d} {row['BRABBR']:<11} "
                   f"{row['CREDIT']:<52} {row['BRANCH2']:03d} {row['BRABBR2']:<11}\n")
    
    print(f"✓ {report_type} report saved to: {report_file}")
    print(f"  Total {report_type} transfers: {len(transfers)}")

# ==================== PROCESS TRANSFERS ====================

print("\n" + "="*60)

# Part 1: Staff to Customer transfers
print("PART 1: Processing Staff to Customer transfers...")
sc_transfers = process_transfers(dpstaf_df, lncust_df, source_is_staff=True)
generate_report(sc_transfers, "Staff to Customer", "SC")

print("\n" + "="*60)

# Part 2: Customer to Staff transfers  
print("PART 2: Processing Customer to Staff transfers...")
cs_transfers = process_transfers(
    dpcust_df.filter((pl.col("ACCTNO") >= 3000000000) & (pl.col("ACCTNO") <= 3999999999)),
    dpstaf_df,
    source_is_staff=False
)
generate_report(cs_transfers, "Customer to Staff", "CS")

print("\n" + "="*60)
print("PROCESSING COMPLETE")
print("="*60)
print(f"Monthly Code: {macro_vars['MONTHLY']}")
print(f"Output Directory: {output_path}")

# Generate summary
summary_file = output_path / f"SUMMARY{macro_vars['MONTHLY']}.txt"
with open(summary_file, 'w') as f:
    f.write("=== TRANSFER REPORT SUMMARY ===\n")
    f.write(f"Report Period: {macro_vars['SDATE']} to {macro_vars['EDATE']}\n")
    f.write(f"Monthly Code: {macro_vars['MONTHLY']}\n")
    f.write(f"Staff to Customer Transfers: {len(sc_transfers)}\n")
    f.write(f"Customer to Staff Transfers: {len(cs_transfers)}\n")
    f.write(f"Total Transfers: {len(sc_transfers) + len(cs_transfers)}\n")

print(f"\n✓ Summary saved to: {summary_file}")
