import polars as pl
import pyreadstat
from pathlib import Path
from datetime import datetime
import warnings

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

print(f"Processing Islamic Banking Report for week {macro_vars['NOWK']} of {macro_vars['REPTMON']}/{macro_vars['REPTYEAR']}")

def read_parquet_or_flat(file_path, delimiter=","):
    """Try to read parquet, then flat file if parquet doesn't exist"""
    # Try parquet first
    parquet_path = Path(str(file_path) + ".parquet")
    if parquet_path.exists():
        print(f"Reading parquet file: {parquet_path}")
        return pl.read_parquet(parquet_path)
    
    # Try flat file with various extensions
    for ext in ['', '.csv', '.txt', '.dat', '.tsv']:
        flat_path = Path(str(file_path) + ext)
        if flat_path.exists():
            print(f"Reading flat file: {flat_path}")
            # Try different delimiters
            for sep in [",", "\t", "|", ";"]:
                try:
                    return pl.read_csv(flat_path, separator=sep, infer_schema_length=1000)
                except:
                    continue
            # If all delimiters fail, try with default
            return pl.read_csv(flat_path, infer_schema_length=1000)
    
    print(f"Warning: File not found: {file_path}")
    return pl.DataFrame()

# Step 2: Load and process BRANCH lookup
print("\nStep 2: Loading BRANCH lookup...")
brch_df = read_parquet_or_flat(lookup_path / "LKP_BRANCH")

if not brch_df.is_empty():
    # Print columns to debug
    print(f"  Columns found: {brch_df.columns}")
    
    # Find column names (case-insensitive)
    col_map = {col.upper(): col for col in brch_df.columns}
    
    # Get actual column names
    branch_col = col_map.get('BRANCH', col_map.get('BR', None))
    brstat_col = col_map.get('BRSTAT', col_map.get('STATUS', col_map.get('STAT', None)))
    brabbr_col = col_map.get('BRABBR', col_map.get('ABBR', col_map.get('LABEL', col_map.get('NAME', None))))
    
    if branch_col and brabbr_col:
        # Filter if status column exists
        if brstat_col:
            brch_df = brch_df.filter(pl.col(brstat_col) != "C")
        
        brch_df = brch_df.select([
            pl.col(branch_col).alias("START"),
            pl.col(brabbr_col).alias("LABEL")
        ]).sort("START")
        
        # Create branch lookup dictionary
        branch_lookup = dict(zip(brch_df["START"].to_list(), brch_df["LABEL"].to_list()))
        print(f"  Loaded {len(branch_lookup)} branch mappings")
    else:
        branch_lookup = {}
        print(f"  Error: Could not find required columns. Need BRANCH and BRABBR columns.")
else:
    branch_lookup = {}
    print("  Warning: Could not load branch lookup data")

# Step 3: Load and process CUSTCODE to identify staff
print("\nStep 3: Loading CUSTCODE lookup...")
code_df = read_parquet_or_flat(lookup_path / "LKP_CUSTCODE")

if not code_df.is_empty():
    print(f"  Columns found: {code_df.columns}")
    
    # Find column names (case-insensitive)
    col_map = {col.upper(): col for col in code_df.columns}
    
    # Find CUSTNO column
    custno_col = col_map.get('CUSTNO', col_map.get('CUSTNUM', col_map.get('CUSTOMERNO', None)))
    
    if custno_col:
        # Find CUST01-CUST20 columns
        cust_cols = []
        for i in range(1, 21):
            col_name = f'CUST{i:02d}'
            if col_name in col_map:
                cust_cols.append(col_map[col_name])
            elif f'CUST{i}' in col_map:
                cust_cols.append(col_map[f'CUST{i}'])
        
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

# Step 4: Load CIS customer data
print("\nStep 4: Loading CIS customer data...")
cis_sas_file = cis_mart / "enrh_exdwh_cisr1.sas7bdat"
if cis_sas_file.exists():
    cis_data, cis_meta = pyreadstat.read_sas7bdat(str(cis_sas_file))
    cis_df = pl.from_pandas(cis_data)
    
    # Check for PRISEC column
    if "PRISEC" in cis_df.columns:
        cis_df = cis_df.filter(pl.col("PRISEC") == 901).select([
            "ACCTNO",
            "CUSTNAME",
            pl.col("CUSTNO").cast(pl.Int64).alias("CUSTNO")
        ]).sort("CUSTNO")
        print(f"  Loaded {len(cis_df)} CIS records with PRISEC=901")
    else:
        print(f"  Warning: PRISEC column not found. Using all rows.")
        cis_df = cis_df.select([
            "ACCTNO",
            "CUSTNAME",
            pl.col("CUSTNO").cast(pl.Int64).alias("CUSTNO")
        ]).sort("CUSTNO")
else:
    cis_df = pl.DataFrame()
    print("  Warning: CIS SAS file not found")

# Step 5: Merge to get staff accounts
print("\nStep 5: Identifying staff accounts...")
if not cis_df.is_empty() and not code_df.is_empty():
    staf_df = code_df.join(cis_df, on="CUSTNO", how="inner").select("ACCTNO").sort("ACCTNO")
    print(f"  Found {len(staf_df)} staff accounts")
else:
    staf_df = pl.DataFrame()
    print("  Warning: Could not create staff accounts list")

# Continue with the rest of your second code (Steps 6-11 and transfer processing)
# ... [Rest of your second code remains the same]
