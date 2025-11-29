import duckdb
import polars as pl
from pathlib import Path
from datetime import datetime
import saspy
import pandas as pd
import sys

input_path = Path("/parquet/dwh/LOAN/staging/STG_LN_BILL_MST.parquet")
output_path = Path("/parquet/dwh/LOAN/enrichment/ENRH_LN_BILL.parquet")

def parse_packed_date(packed_value: int) -> datetime.date:
    """
    Convert packed decimal date to Python date.
    Handles both 8-digit (YYYYMMDD) and 11-digit formats (with trailing zeros)
    """
    if packed_value is None or packed_value == 0:
        return None
    
    # Convert to string and take first 8 digits
    date_str = str(packed_value)[:8].zfill(8)
    
    try:
        return datetime.strptime(date_str, '%Y%m%d').date()
    except:
        return None

# Read the parquet file
df = pl.read_parquet(input_path)
print(f"✓ Loaded {len(df):,} records from {input_path.name}")

# Validate required columns
required_columns = [
    'BILL_ACCT_NO', 'BILL_NOTE_NO', 'BILL_DUE_DATE', 
    'BILL_PAID_DATE', 'BILL_DAYS_LATE', 'BILL_NOTE_TYPE',
    'BILL_COST_CENTER', 'BILL_TOT_BILLED', 'BILL_PRIN_BILLED',
    'BILL_INT_BILLED', 'BILL_ESC_BILLED', 'BILL_FEE_BILLED',
    'BILL_TOT_BNP', 'BILL_PRIN_BNP', 'BILL_INT_BNP',
    'BILL_ESC_BNP', 'BILL_FEE_BNP'
]

missing_columns = [col for col in required_columns if col not in df.columns]
if missing_columns:
    print(f"✗ ERROR: Missing required columns: {missing_columns}")
    sys.exit(1)

print("\n✓ All required columns present")

# Show actual column names
print("\nActual columns in file:")
print(df.columns)

# Filter out null records
initial_count = len(df)
stg_ln_bill = df.filter(
    pl.col('BILL_ACCT_NO').is_not_null() & 
    (pl.col('BILL_ACCT_NO') != 0)
)
filtered_count = initial_count - len(stg_ln_bill)

if filtered_count > 0:
    print(f"\n✓ Filtered out {filtered_count:,} null/zero account records")

print(f"\n✓ Processing {len(stg_ln_bill):,} valid records")

# Convert to Pandas for DuckDB
print("\nConverting to DuckDB...")
con = duckdb.connect(':memory:')
con.register('stg_ln_bill', stg_ln_bill.to_pandas())

print("\nStep 2: Mapping columns and creating SAS numeric dates...")

try:
    con.execute("""
        CREATE TABLE extracted AS
        SELECT
            BILL_ACCT_NO AS ACCTNO,
            BILL_NOTE_NO AS NOTENO,
            BILL_DUE_DATE AS BLDATE,
            BILL_PAID_DATE AS BLPDDATE,
            BILL_DAYS_LATE AS DAYSLATE,
            BILL_NOTE_TYPE AS PRODUCT,
            BILL_COST_CENTER AS COSTCTR,
            ROUND(BILL_TOT_BILLED / 100.0, 2) AS BILL_AMT,
            ROUND(BILL_PRIN_BILLED / 100.0, 2) AS BILL_AMT_PRIN,
            ROUND(BILL_INT_BILLED / 100.0, 2) AS BILL_AMT_INT,
            ROUND(BILL_ESC_BILLED / 100.0, 2) AS BILL_AMT_ESCROW,
            ROUND(BILL_FEE_BILLED / 100.0, 2) AS BILL_AMT_FEE,
            ROUND(BILL_TOT_BNP / 100.0, 2) AS BILL_NOT_PAY_AMT,
            ROUND(BILL_PRIN_BNP / 100.0, 2) AS BILL_NOT_PAY_AMT_PRIN,
            ROUND(BILL_INT_BNP / 100.0, 2) AS BILL_NOT_PAY_AMT_INT,
            ROUND(BILL_ESC_BNP / 100.0, 2) AS BILL_NOT_PAY_AMT_ESCROW,
            ROUND(BILL_FEE_BNP / 100.0, 2) AS BILL_NOT_PAY_AMT_FEE,
            
            -- BLDATE: Convert from YYYYMMDD to SAS numeric date (days since 1960-01-01)
            CASE 
                WHEN BILL_DUE_DATE > 0 THEN 
                    DATEDIFF('day', DATE '1960-01-01', 
                        TRY_CAST(SUBSTR(CAST(BILL_DUE_DATE AS VARCHAR), 1, 8) AS DATE)
                    )
                ELSE NULL
            END AS BILL_DT,
            
            -- BLPDDATE: Convert from MMDDYYYY to SAS numeric date (days since 1960-01-01)  
            CASE 
                WHEN BILL_PAID_DATE > 0 THEN
                    DATEDIFF('day', DATE '1960-01-01',
                        CASE 
                            WHEN LENGTH(CAST(BILL_PAID_DATE AS VARCHAR)) >= 8 THEN
                                TRY_CAST(
                                    CONCAT(
                                        SUBSTR(CAST(BILL_PAID_DATE AS VARCHAR), 5, 4),  -- Year
                                        SUBSTR(CAST(BILL_PAID_DATE AS VARCHAR), 1, 2),  -- Month  
                                        SUBSTR(CAST(BILL_PAID_DATE AS VARCHAR), 3, 2)   -- Day
                                    ) AS DATE
                                )
                            ELSE NULL
                        END
                    )
                ELSE NULL
            END AS BILL_PAID_DT
        FROM stg_ln_bill
    """)
    
    count = con.execute("SELECT COUNT(*) FROM extracted").fetchone()[0]
    print(f"  ✓ Transformed {count:,} records")
    
except Exception as e:
    print(f"✗ ERROR in SQL transformation: {e}")
    print("\nAvailable columns:")
    print(con.execute("DESCRIBE stg_ln_bill").fetchdf())
    raise

# Get final result
print("\nStep 3: Loading to ENRH_LN_BILL...")

result = con.execute("""
    SELECT 
        ACCTNO,
        NOTENO,
        BLDATE,
        BLPDDATE,
        DAYSLATE,
        PRODUCT,
        COSTCTR,
        BILL_AMT,
        BILL_AMT_PRIN,
        BILL_AMT_INT,
        BILL_AMT_ESCROW,
        BILL_AMT_FEE,
        BILL_NOT_PAY_AMT,
        BILL_NOT_PAY_AMT_PRIN,
        BILL_NOT_PAY_AMT_INT,
        BILL_NOT_PAY_AMT_ESCROW,
        BILL_NOT_PAY_AMT_FEE,
        BILL_DT,
        BILL_PAID_DT
    FROM extracted
""").pl()

# Write to parquet
result.write_parquet(output_path)
print(f"  ✓ Output written to: {output_path}")

# Show statistics with null-safe formatting
stats = con.execute("""
    SELECT 
        COUNT(*) as total_records,
        COUNT(DISTINCT ACCTNO) as unique_accounts,
        MIN(BILL_DT) as min_sas_date,
        MAX(BILL_DT) as max_sas_date,
        SUM(BILL_AMT) as total_bill_amt,
        COUNT(CASE WHEN BILL_DT IS NULL THEN 1 END) as null_dates,
        COUNT(CASE WHEN BILL_PAID_DT IS NULL THEN 1 END) as null_paid_dates
    FROM extracted
""").fetchone()

print("\n" + "="*70)
print("DATA STATISTICS")
print("="*70)
print(f"Total Records:        {stats[0]:>15,}")
print(f"Unique Accounts:      {stats[1]:>15,}")

# Handle None values in date ranges
min_date = stats[2] if stats[2] is not None else "N/A"
max_date = stats[3] if stats[3] is not None else "N/A"
print(f"SAS Date Range:       {min_date:>15} to {max_date:>15}")

print(f"Null BILL_DT:         {stats[5]:>15,}")
print(f"Null BILL_PAID_DT:    {stats[6]:>15,}")
print(f"Total Bill Amount:    {stats[4]:>15,.2f}")
print("="*70)

# Sample output
print("\nSample output (first 5 rows):")
print(result.head(5))

# Validate date parsing
date_check = con.execute("""
    SELECT 
        COUNT(*) as total,
        COUNT(BILL_DT) as valid_bill_dt,
        COUNT(BILL_PAID_DT) as valid_paid_dt
    FROM extracted
""").fetchone()

print(f"\nDate Parsing Results:")
print(f"  Total records: {date_check[0]:,}")
print(f"  Valid BILL_DT: {date_check[1]:,} ({100*date_check[1]/date_check[0]:.1f}%)")
print(f"  Valid BILL_PAID_DT: {date_check[2]:,} ({100*date_check[2]/date_check[0]:.1f}%)")

con.close()
print("\n✓ Processing completed successfully")

# SAS Integration
sas = saspy.SASsession()

enrh_ln_bill_ctl = "enrh_ln_bill_ctrl"
enrh_ln_bill = "enrh_ln_bill"

def assign_libname(lib_name, sas_path):
    log = sas.submit(f"libname {lib_name} '{sas_path}';")
    return log

def set_data(df, lib_name, ctrl_name, cur_data, prev_data):
    sas.df2sd(df, table=cur_data, libref='work')

    log = sas.submit(f"""
                proc sql noprint;
                   create table colmeta as 
                    select name, type, length
                from dictionary.columns
                where libname = upcase("{ctrl_name}")  
                     and memname = upcase("{prev_data}");
                quit
                """)
    
    print(log["LOG"])
    df_meta = sas.sasdata("colmeta", libref="work").to_df()
    cols = df_meta["name"].dropna().tolist()
    col_list = ", ".join(cols)

    casted_cols = []
    for _, row in df_meta.iterrows():
        col = row["name"]
        length = row['length']
        if row['type'].strip().lower() == 'char' and pd.notnull(length) and length > 0:
            casted_cols.append(f"input(trim({col}), ${int(length)}.) as {col}")
        else:
            casted_cols.append(col)

    casted_cols = ",\n ".join(casted_cols)

    log = sas.submit(f"""
                proc sql noprint;
                    create table {lib_name}.{cur_data} as
                         select {col_list} from {ctrl_name}.{prev_data}(obs=0)
                         union corr
                         select {casted_cols} from work.{cur_data};
                    quit;
                    """)
    print(f"Final table created : {log['LOG']}") 
    return log

# Assign libname and load data to SAS
assign_libname("ln", "/stgsrcsys/host/uat")
log1 = set_data(result, "ln", "ctrl", enrh_ln_bill, enrh_ln_bill_ctl)
